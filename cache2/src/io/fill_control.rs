// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded pre-timeout observation and nonblocking fill admission.

use std::io;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use crate::FillControlOptions;
use crate::FillControlSnapshot;
use crate::FillLimits;
use crate::FillPressure;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;

const TICK: Duration = Duration::from_millis(100);
const TICKS_PER_SECOND: u64 = 10;
const UNIT: u64 = 64;
const COUNT_MASK: u64 = 0xffff;
const MAX_CAS_ATTEMPTS: usize = 4;
const MIN_BYTES_PER_SECOND: u64 = 640;
const MAX_BYTES_PER_SECOND: u64 = 1 << 40;
const MIN_RECORDS_PER_SECOND: u32 = 10;
const MAX_RECORDS_PER_SECOND: u32 = 655_350;
const DECISION_WINDOW: Duration = Duration::from_millis(500);
const STALL_CAP: Duration = Duration::from_millis(500);
const MAX_DRAIN_DEADLINE: Duration = Duration::from_secs(5);
const CLEAN_DECISIONS: u32 = 3;
const RAMP_DIVISOR: u32 = 40;
const STAGING_KEEP_NUM: u64 = 7;
const STAGING_KEEP_DEN: u64 = 10;
const SERVICE_HEADROOM: f64 = 0.8;

fn packed_epoch(value: u64) -> u64 {
    value >> 48
}
fn packed_ops(value: u64) -> u64 {
    (value >> 32) & COUNT_MASK
}
fn packed_units(value: u64) -> u64 {
    value & u64::from(u32::MAX)
}
fn pack_credit(epoch: u64, ops: u64, units: u64) -> u64 {
    (epoch & COUNT_MASK) << 48 | (ops << 32) | units
}

fn encode_pressure(pressure: FillPressure) -> u8 {
    match pressure {
        FillPressure::Disabled => 0,
        FillPressure::Healthy => 1,
        FillPressure::Throttled => 2,
        FillPressure::Paused => 3,
    }
}

#[derive(Clone, Copy)]
struct InFlight {
    start: Instant,
    admitted: Option<Instant>,
    completed: Option<Instant>,
    bytes: u64,
    timeout: Duration,
}

struct State {
    slots: Box<[Option<InFlight>]>,
    snapshot: FillControlSnapshot,
    last_tick: Instant,
    last_progress: Instant,
    previous_pending: usize,
    completed_bytes: u64,
    completed_ops: u64,
    sampled_bytes: u64,
    sampled_ops: u64,
    window_start: Instant,
    service_bytes: f64,
    service_ops: f64,
    clean_ticks: u32,
    was_recovering: bool,
}

pub struct FillController {
    options: FillLimits,
    enforcing: bool,
    max_units: u64,
    max_ops: u64,
    // Epoch:16, records:16, 64-byte units:32. One CAS reserves both dimensions.
    credit: AtomicU64,
    epoch: AtomicU64,
    staging_pressure: AtomicBool,
    pressure: AtomicU8,
    stopped: AtomicBool,
    recovering: AtomicBool,
    rejections: AtomicU64,
    would_reject: AtomicU64,
    dropped_observations: AtomicU64,
    state: Mutex<State>,
    wake: Condvar,
}

pub struct FillMonitor {
    control: Arc<FillController>,
    worker: Option<JoinHandle<()>>,
}

impl Drop for FillMonitor {
    fn drop(&mut self) {
        self.control.stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl FillController {
    pub fn allocation_bytes(options: FillControlOptions, slots: usize) -> io::Result<usize> {
        let settings = match options {
            FillControlOptions::Disabled => return Ok(0),
            FillControlOptions::Observe(settings) | FillControlOptions::Adaptive(settings) => {
                settings
            }
        };
        if !(MIN_BYTES_PER_SECOND..=MAX_BYTES_PER_SECOND).contains(&settings.max_bytes_per_second)
            || !(MIN_RECORDS_PER_SECOND..=MAX_RECORDS_PER_SECOND)
                .contains(&settings.max_records_per_second)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill rate ceilings are out of range",
            ));
        }
        slots
            .checked_mul(size_of::<Option<InFlight>>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() + CACHE_THREAD_STACK_BYTES + 256))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "fill controller memory overflow",
                )
            })
    }

    pub fn new(
        options: FillControlOptions,
        slots: usize,
        max_record: u64,
    ) -> io::Result<Option<Arc<Self>>> {
        Self::allocation_bytes(options, slots)?;
        let (settings, enforcing) = match options {
            FillControlOptions::Disabled => return Ok(None),
            FillControlOptions::Observe(settings) => (settings, false),
            FillControlOptions::Adaptive(settings) => (settings, true),
        };
        let max_units = (settings.max_bytes_per_second / TICKS_PER_SECOND)
            .max(max_record)
            .div_ceil(UNIT);
        if max_units > u64::from(u32::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill burst exceeds budget representation",
            ));
        }
        let max_ops = u64::from(settings.max_records_per_second / TICKS_PER_SECOND as u32).max(1);
        let mut observations = Vec::new();
        observations.try_reserve_exact(slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate fill observations",
            )
        })?;
        observations.resize(slots, None);
        let now = Instant::now();
        Ok(Some(Arc::new(Self {
            options: settings,
            enforcing,
            max_units,
            max_ops,
            credit: AtomicU64::new(pack_credit(0, max_ops, max_units)),
            epoch: AtomicU64::new(0),
            staging_pressure: AtomicBool::new(false),
            pressure: AtomicU8::new(encode_pressure(FillPressure::Healthy)),
            stopped: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            rejections: AtomicU64::new(0),
            would_reject: AtomicU64::new(0),
            dropped_observations: AtomicU64::new(0),
            state: Mutex::new(State {
                slots: observations.into_boxed_slice(),
                snapshot: FillControlSnapshot {
                    pressure: FillPressure::Healthy,
                    enforcing,
                    bytes_per_second: settings.max_bytes_per_second,
                    records_per_second: settings.max_records_per_second,
                    ..FillControlSnapshot::default()
                },
                last_tick: now,
                last_progress: now,
                previous_pending: 0,
                completed_bytes: 0,
                completed_ops: 0,
                sampled_bytes: 0,
                sampled_ops: 0,
                window_start: now,
                service_bytes: 0.,
                service_ops: 0.,
                clean_ticks: 0,
                was_recovering: false,
            }),
            wake: Condvar::new(),
        })))
    }

    pub fn start(self: &Arc<Self>) -> io::Result<FillMonitor> {
        let control = Arc::clone(self);
        let worker = std::thread::Builder::new()
            .name("cache2-fill-control".into())
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || {
                let mut state = control.lock();
                while !control.stopped.load(Ordering::Acquire) {
                    let now = Instant::now();
                    let elapsed = now.saturating_duration_since(state.last_tick);
                    if elapsed >= TICK {
                        control.tick(&mut state, now);
                        continue;
                    }
                    state = control
                        .wake
                        .wait_timeout(state, TICK - elapsed)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                }
            })?;
        Ok(FillMonitor {
            control: Arc::clone(self),
            worker: Some(worker),
        })
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub fn stop(&self) {
        let _state = self.lock();
        self.stopped.store(true, Ordering::Release);
        self.fence();
        self.wake.notify_all();
    }

    pub fn set_recovering(&self, recovering: bool) {
        self.recovering.store(recovering, Ordering::Release);
        if recovering && self.enforcing {
            self.fence();
        }
    }

    fn fence(&self) {
        self.pressure
            .store(encode_pressure(FillPressure::Paused), Ordering::Release);
    }

    pub fn suppress_reinsertion(&self) -> bool {
        self.enforcing
            && self.pressure.load(Ordering::Acquire) != encode_pressure(FillPressure::Healthy)
    }

    pub fn note_staging_pressure(&self) {
        self.staging_pressure.store(true, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FillControlSnapshot {
        let mut snapshot = self.lock().snapshot;
        snapshot.rejections = self.rejections.load(Ordering::Relaxed);
        snapshot.would_reject = self.would_reject.load(Ordering::Relaxed);
        snapshot.dropped_observations = self.dropped_observations.load(Ordering::Relaxed);
        snapshot
    }

    pub fn try_admit(&self, bytes: u64) -> Option<FillPermit<'_>> {
        let units = bytes.div_ceil(UNIT);
        if self.stopped.load(Ordering::Acquire)
            || self.pressure.load(Ordering::Acquire) == encode_pressure(FillPressure::Paused)
        {
            return self.refuse(true);
        }
        let mut value = self.credit.load(Ordering::Relaxed);
        for _ in 0..MAX_CAS_ATTEMPTS {
            let epoch = self.epoch.load(Ordering::Acquire);
            if packed_epoch(value) != epoch & COUNT_MASK {
                value = self.credit.load(Ordering::Relaxed);
                continue;
            }
            if packed_units(value) < units || packed_ops(value) == 0 {
                return self.refuse(true);
            }
            match self.credit.compare_exchange_weak(
                value,
                value - units - (1 << 32),
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(FillPermit {
                        control: self,
                        units,
                        epoch,
                        committed: false,
                    });
                }
                Err(current) => value = current,
            }
        }
        self.refuse(false)
    }

    fn refuse(&self, policy: bool) -> Option<FillPermit<'_>> {
        if self.enforcing {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            if policy {
                self.would_reject.fetch_add(1, Ordering::Relaxed);
            }
            Some(FillPermit {
                control: self,
                units: 0,
                epoch: 0,
                committed: true,
            })
        }
    }

    pub fn observe(&self, bytes: u64, timeout: Duration) -> Option<Observation<'_>> {
        let mut state = self.lock();
        let now = Instant::now();
        let mut free = None;
        let mut occupied = 0;
        for (index, slot) in state.slots.iter().enumerate() {
            if slot.is_some() {
                occupied += 1;
            } else if free.is_none() {
                free = Some(index);
            }
        }
        let Some(index) = free else {
            self.dropped_observations.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        if occupied == 0 {
            state.last_progress = now;
        }
        state.slots[index] = Some(InFlight {
            start: now,
            admitted: None,
            completed: None,
            bytes,
            timeout,
        });
        Some(Observation {
            control: self,
            index,
            succeeded: false,
        })
    }

    fn clamp_rates(&self, bytes: u64, records: u32) -> (u64, u32) {
        (
            bytes
                .max(MIN_BYTES_PER_SECOND)
                .min(self.options.max_bytes_per_second),
            records
                .max(MIN_RECORDS_PER_SECOND)
                .min(self.options.max_records_per_second),
        )
    }

    fn throttle(&self, state: &mut State, bytes: u64, records: u32) {
        let (bytes, records) = self.clamp_rates(bytes, records);
        state.snapshot.pressure = FillPressure::Throttled;
        state.snapshot.bytes_per_second = bytes;
        state.snapshot.records_per_second = records;
        state.clean_ticks = 0;
    }

    fn tick(&self, state: &mut State, now: Instant) {
        state.last_tick = now;
        let elapsed = now
            .saturating_duration_since(state.window_start)
            .as_secs_f64();
        let decision = elapsed >= DECISION_WINDOW.as_secs_f64();
        let mut made_progress = false;
        if decision {
            let bytes = state.completed_bytes.saturating_sub(state.sampled_bytes);
            let ops = state.completed_ops.saturating_sub(state.sampled_ops);
            made_progress = ops != 0;
            state.service_bytes = bytes as f64 / elapsed;
            state.service_ops = ops as f64 / elapsed;
            state.sampled_bytes = state.completed_bytes;
            state.sampled_ops = state.completed_ops;
            state.window_start = now;
        }
        let mut pending = 0;
        let mut bytes = 0_u64;
        let mut oldest = Duration::ZERO;
        let mut deadline = MAX_DRAIN_DEADLINE;
        let mut aged = false;
        for request in state.slots.iter().flatten() {
            pending += 1;
            bytes = bytes.saturating_add(request.bytes);
            let age = now.saturating_duration_since(request.start);
            oldest = oldest.max(age);
            deadline = deadline.min(request.timeout);
            aged |= age >= request.timeout / 4;
        }
        let drain = if pending != 0 && state.service_bytes > 0. && state.service_ops > 0. {
            (bytes as f64 / state.service_bytes).max(pending as f64 / state.service_ops)
        } else {
            0.
        };
        let no_progress = pending != 0
            && now.saturating_duration_since(state.last_progress) >= (deadline / 2).min(STALL_CAP);
        let recovering = self.recovering.load(Ordering::Acquire);
        let pause = recovering || aged || no_progress || drain >= deadline.as_secs_f64() / 4.;
        let previous = state.snapshot.pressure;
        let staging_pressure = decision && self.staging_pressure.swap(false, Ordering::Relaxed);
        if pause {
            state.snapshot.pressure = FillPressure::Paused;
            state.clean_ticks = 0;
        } else if previous == FillPressure::Paused || state.was_recovering {
            if pending == 0
                || (made_progress && oldest < deadline / 10 && drain < deadline.as_secs_f64() / 10.)
            {
                let bytes = state.snapshot.bytes_per_second / 2;
                let records = state.snapshot.records_per_second / 2;
                let (bytes, records) = if state.service_bytes > 0. {
                    (
                        bytes.min((state.service_bytes * SERVICE_HEADROOM) as u64),
                        records.min((state.service_ops * SERVICE_HEADROOM) as u32),
                    )
                } else {
                    (bytes, records)
                };
                self.throttle(state, bytes, records);
            }
        } else if staging_pressure && pending >= state.previous_pending && pending != 0 {
            self.throttle(
                state,
                state.snapshot.bytes_per_second * STAGING_KEEP_NUM / STAGING_KEEP_DEN,
                u32::try_from(
                    u64::from(state.snapshot.records_per_second) * STAGING_KEEP_NUM
                        / STAGING_KEEP_DEN,
                )
                .unwrap_or(MIN_RECORDS_PER_SECOND),
            );
        } else if made_progress {
            state.clean_ticks += 1;
            if state.clean_ticks >= CLEAN_DECISIONS {
                let (bytes, records) = self.clamp_rates(
                    state.snapshot.bytes_per_second
                        + self.options.max_bytes_per_second / u64::from(RAMP_DIVISOR),
                    state.snapshot.records_per_second
                        + self.options.max_records_per_second.div_ceil(RAMP_DIVISOR),
                );
                state.snapshot.bytes_per_second = bytes;
                state.snapshot.records_per_second = records;
                if bytes == self.options.max_bytes_per_second
                    && records == self.options.max_records_per_second
                {
                    state.snapshot.pressure = FillPressure::Healthy;
                }
            }
        }
        state.was_recovering = recovering;
        if decision {
            state.previous_pending = pending;
        }
        state.snapshot.outstanding_operations = pending as u64;
        state.snapshot.outstanding_bytes = bytes;
        state.snapshot.oldest_operation_ns = nanos(oldest);
        state.snapshot.estimated_drain_ns = (drain * 1e9).min(u64::MAX as f64) as u64;
        self.pressure
            .store(encode_pressure(state.snapshot.pressure), Ordering::Release);
        let clear =
            previous != state.snapshot.pressure || state.snapshot.pressure == FillPressure::Paused;
        let add_units = state.snapshot.bytes_per_second / TICKS_PER_SECOND / UNIT;
        let add_ops = u64::from(state.snapshot.records_per_second) / TICKS_PER_SECOND;
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        let _ = self
            .credit
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                let units = if clear { 0 } else { packed_units(old) };
                let ops = if clear { 0 } else { packed_ops(old) };
                let (units, ops) = if state.snapshot.pressure == FillPressure::Paused {
                    (0, 0)
                } else {
                    (
                        (units + add_units).min(self.max_units),
                        (ops + add_ops).min(self.max_ops),
                    )
                };
                Some(pack_credit(epoch, ops, units))
            });
        if previous != state.snapshot.pressure {
            log::info!(target: "cache2::health", event = "cache_fill_pressure_changed", pressure:? = state.snapshot.pressure;
                "cache fill admission pressure changed");
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

pub struct FillPermit<'a> {
    control: &'a FillController,
    units: u64,
    epoch: u64,
    committed: bool,
}
impl FillPermit<'_> {
    pub fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for FillPermit<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut value = self.control.credit.load(Ordering::Relaxed);
        for _ in 0..MAX_CAS_ATTEMPTS {
            if self.control.epoch.load(Ordering::Acquire) != self.epoch
                || packed_epoch(value) != self.epoch & COUNT_MASK
            {
                return;
            }
            let next = pack_credit(
                packed_epoch(value),
                (packed_ops(value) + 1).min(self.control.max_ops),
                (packed_units(value) + self.units).min(self.control.max_units),
            );
            match self.control.credit.compare_exchange_weak(
                value,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => value = current,
            }
        }
    }
}

/// One worker-owned observation. Failed work is removed, never marked successful.
pub struct Observation<'a> {
    control: &'a FillController,
    index: usize,
    succeeded: bool,
}
impl Observation<'_> {
    pub fn admitted(&self) {
        self.stamp(|request, now| request.admitted = Some(now));
    }
    pub fn completed(&self) {
        self.stamp(|request, now| request.completed = Some(now));
    }
    pub fn finish(mut self) {
        self.succeeded = true;
    }
    fn stamp(&self, update: impl FnOnce(&mut InFlight, Instant)) {
        update(
            self.control.lock().slots[self.index]
                .as_mut()
                .expect("live observation slot"),
            Instant::now(),
        );
    }
}
impl Drop for Observation<'_> {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        let request = state.slots[self.index]
            .take()
            .expect("live observation slot");
        let now = Instant::now();
        if self.succeeded {
            state.completed_ops = state.completed_ops.saturating_add(1);
            state.completed_bytes = state.completed_bytes.saturating_add(request.bytes);
            state.last_progress = now;
        }
        let admission_end = request.admitted.unwrap_or(now);
        state.snapshot.admission_ns = state.snapshot.admission_ns.saturating_add(nanos(
            admission_end.saturating_duration_since(request.start),
        ));
        if let Some(admitted) = request.admitted {
            let wait_end = request.completed.unwrap_or(now);
            state.snapshot.completion_wait_ns = state
                .snapshot
                .completion_wait_ns
                .saturating_add(nanos(wait_end.saturating_duration_since(admitted)));
            if let Some(completed) = request.completed {
                state.snapshot.validation_ns = state
                    .snapshot
                    .validation_ns
                    .saturating_add(nanos(now.saturating_duration_since(completed)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control(enforcing: bool) -> Arc<FillController> {
        let settings = FillLimits::new(64_000, 100);
        FillController::new(
            if enforcing {
                FillControlOptions::Adaptive(settings)
            } else {
                FillControlOptions::Observe(settings)
            },
            4,
            4096,
        )
        .unwrap()
        .unwrap()
    }

    fn tick(control: &FillController, elapsed: Duration) {
        let mut state = control.lock();
        let now = state.last_tick + elapsed;
        control.tick(&mut state, now);
    }

    #[test]
    fn budget_reserves_both_dimensions_and_refunds_failed_staging() {
        let control = control(true);
        let initial = control.credit.load(Ordering::Relaxed);
        let permit = control.try_admit(640).unwrap();
        assert_eq!(
            control.credit.load(Ordering::Relaxed),
            initial - 10 - (1 << 32)
        );
        drop(permit);
        assert_eq!(control.credit.load(Ordering::Relaxed), initial);
        for _ in 0..10 {
            control.try_admit(64).unwrap().commit();
        }
        assert!(control.try_admit(64).is_none());
        tick(&control, TICK);
        assert!(control.try_admit(64).is_some());
    }

    #[test]
    fn staging_pressure_reduces_once_per_window_and_idle_does_not_raise_rates() {
        let control = control(true);
        let pending = control.observe(64, Duration::from_secs(30)).unwrap();
        control.note_staging_pressure();
        tick(&control, TICK);
        assert_eq!(control.snapshot().bytes_per_second, 64_000);
        {
            let mut state = control.lock();
            state.last_progress = state.last_tick + Duration::from_millis(500);
        }
        tick(&control, Duration::from_millis(500));
        assert_eq!(control.snapshot().pressure, FillPressure::Throttled);
        assert_eq!(control.snapshot().bytes_per_second, 44_800);
        assert_eq!(control.snapshot().records_per_second, 70);
        drop(pending);
        tick(&control, Duration::from_secs(10));
        assert_eq!(control.snapshot().bytes_per_second, 44_800);
        for sample in 0..3 {
            {
                let mut state = control.lock();
                state.completed_bytes += 64;
                state.completed_ops += 1;
            }
            tick(&control, Duration::from_millis(500));
            assert_eq!(
                control.snapshot().bytes_per_second,
                if sample < 2 { 44_800 } else { 46_400 }
            );
        }
    }

    #[test]
    fn old_refund_cannot_mint_new_epoch_credits_even_after_low_bits_wrap() {
        let control = control(true);
        let permit = control.try_admit(64).unwrap();
        control.epoch.store(COUNT_MASK, Ordering::Release);
        tick(&control, TICK);
        let before = control.credit.load(Ordering::Relaxed);
        drop(permit);
        assert_eq!(control.credit.load(Ordering::Relaxed), before);
    }

    #[test]
    fn old_request_is_detected_while_other_requests_progress() {
        let control = control(true);
        let old = control.observe(4096, Duration::from_secs(2)).unwrap();
        old.admitted();
        let fast = control.observe(64, Duration::from_secs(2)).unwrap();
        fast.admitted();
        fast.completed();
        fast.finish();
        {
            // Many small completions keep aggregate progress healthy. Only
            // the retained request's age should trigger the pause.
            let mut state = control.lock();
            state.last_progress = state.last_tick + Duration::from_millis(600);
            state.completed_bytes = 1_000_000;
            state.completed_ops = 1_000;
        }
        tick(&control, Duration::from_millis(600));
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(control.try_admit(64).is_none());
        old.completed();
        assert!(
            control.try_admit(64).is_none(),
            "delivery does not release the admission fence"
        );
        old.finish();
        tick(&control, TICK);
        assert_eq!(control.snapshot().pressure, FillPressure::Throttled);
        assert!(control.try_admit(64).is_some());
    }

    #[test]
    fn observe_never_rejects_or_suppresses_reinsertion() {
        let control = control(false);
        let _old = control.observe(4096, Duration::from_secs(2)).unwrap();
        tick(&control, Duration::from_millis(600));
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        for _ in 0..20 {
            control.try_admit(4096).unwrap().commit();
        }
        assert_eq!(control.snapshot().would_reject, 20);
        assert_eq!(control.snapshot().rejections, 0);
        assert!(!control.suppress_reinsertion());
    }

    #[test]
    fn idle_time_does_not_inflate_credit_or_look_like_a_stall() {
        let control = control(true);
        tick(&control, Duration::from_secs(3600));
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        let credit = control.credit.load(Ordering::Relaxed);
        assert_eq!(packed_ops(credit), control.max_ops);
        assert_eq!(packed_units(credit), control.max_units);
        let _new = control.observe(4096, Duration::from_secs(2)).unwrap();
        // Production ticks and observations use the same clock.
        let mut state = control.lock();
        control.tick(&mut state, Instant::now());
        assert_eq!(state.snapshot.pressure, FillPressure::Healthy);
    }

    #[test]
    fn large_record_remains_eligible_and_stop_cannot_reopen_admission() {
        let control = FillController::new(
            FillControlOptions::Adaptive(FillLimits::new(640, 10)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        control.try_admit(4096).unwrap().commit();
        for _ in 0..64 {
            tick(&control, TICK);
        }
        assert!(control.try_admit(4096).is_some());
        control.stop();
        tick(&control, TICK);
        assert!(control.try_admit(64).is_none());
    }

    #[test]
    fn recovering_fence_survives_empty_observation_table() {
        let control = control(true);
        control.set_recovering(true);
        tick(&control, TICK);
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(control.try_admit(64).is_none());
        control.set_recovering(false);
        tick(&control, TICK);
        assert_eq!(control.snapshot().pressure, FillPressure::Throttled);
    }

    #[test]
    fn idle_monitor_can_be_stopped_without_io() {
        let control = control(true);
        let monitor = control.start().unwrap();
        drop(monitor);
        assert!(control.stopped.load(Ordering::Acquire));
    }

    #[test]
    fn failed_observations_release_capacity_without_reporting_success() {
        let control = FillController::new(
            FillControlOptions::Observe(FillLimits::new(640, 10)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        let pending = control.observe(4096, Duration::from_secs(5)).unwrap();
        assert!(control.observe(64, Duration::from_secs(5)).is_none());
        assert_eq!(control.snapshot().dropped_observations, 1);
        // A failed admission still contributes its elapsed phase time.
        control.lock().slots[0].as_mut().unwrap().start -= Duration::from_secs(1);
        drop(pending);
        assert!(control.snapshot().admission_ns >= 1_000_000_000);
        let pending = control.observe(4096, Duration::from_secs(5)).unwrap();
        pending.admitted();
        control.lock().slots[0].as_mut().unwrap().admitted =
            Some(Instant::now() - Duration::from_secs(1));
        drop(pending);
        assert!(control.snapshot().completion_wait_ns >= 1_000_000_000);
        let state = control.lock();
        assert_eq!(state.completed_ops, 0);
        assert_eq!(state.completed_bytes, 0);
        assert!(state.slots[0].is_none());
    }

    #[test]
    fn observe_would_reject_counts_policy_not_epoch_contention() {
        let control = control(false);
        control.epoch.store(1, Ordering::Release);
        control.try_admit(64).unwrap().commit();
        assert_eq!(control.snapshot().would_reject, 0);
        tick(&control, TICK);
        for _ in 0..10 {
            control.try_admit(64).unwrap().commit();
        }
        control.try_admit(64).unwrap().commit();
        assert_eq!(control.snapshot().would_reject, 1);
        let _old = control.observe(4096, Duration::from_secs(2)).unwrap();
        tick(&control, Duration::from_millis(600));
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        control.try_admit(64).unwrap().commit();
        assert_eq!(control.snapshot().would_reject, 2);
    }

    #[test]
    fn concurrent_admission_never_exceeds_shared_budget() {
        let control = control(true);
        let accepted = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let control = &control;
                let accepted = &accepted;
                scope.spawn(move || {
                    for _ in 0..100 {
                        if let Some(permit) = control.try_admit(640) {
                            permit.commit();
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(accepted.load(Ordering::Relaxed), 10);
    }
}
