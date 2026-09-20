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
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use crate::AdaptiveFillOptions;
use crate::FillControlOptions;
use crate::FillControlSnapshot;
use crate::FillPressure;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;

const TICK: Duration = Duration::from_millis(100);
const UNIT: u64 = 64;
const COUNT_MASK: u64 = 0xffff;
const CREDIT_MASK: u64 = (1 << 48) - 1;
const MAX_CAS_ATTEMPTS: usize = 4;

#[derive(Clone, Copy)]
struct Observation {
    start: Instant,
    admitted: Option<Instant>,
    returned: Option<Instant>,
    bytes: u64,
    timeout: Duration,
}

struct State {
    slots: Box<[Option<Observation>]>,
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
    options: AdaptiveFillOptions,
    enforcing: bool,
    max_units: u64,
    max_ops: u64,
    // Epoch:16, operations:16, 64-byte units:32. One CAS reserves both dimensions.
    credit: AtomicU64,
    epoch: AtomicU64,
    staging_pressure: AtomicBool,
    paused: AtomicBool,
    stopped: AtomicBool,
    recovering: AtomicBool,
    rejections: AtomicU64,
    would_reject: AtomicU64,
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
        if !(640..=1 << 40).contains(&settings.max_bytes_per_second)
            || !(10..=655_350).contains(&settings.max_operations_per_second)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill rate ceilings are out of range",
            ));
        }
        slots
            .checked_mul(size_of::<Option<Observation>>())
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
        let max_units = (settings.max_bytes_per_second / 10)
            .max(max_record)
            .div_ceil(UNIT);
        if max_units > u64::from(u32::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill burst exceeds budget representation",
            ));
        }
        let max_ops = u64::from(settings.max_operations_per_second / 10).max(1);
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
            credit: AtomicU64::new((max_ops << 32) | max_units),
            epoch: AtomicU64::new(0),
            staging_pressure: AtomicBool::new(false),
            paused: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            recovering: AtomicBool::new(false),
            rejections: AtomicU64::new(0),
            would_reject: AtomicU64::new(0),
            state: Mutex::new(State {
                slots: observations.into_boxed_slice(),
                snapshot: FillControlSnapshot {
                    pressure: FillPressure::Healthy,
                    enforcing,
                    bytes_per_second: settings.max_bytes_per_second,
                    operations_per_second: settings.max_operations_per_second,
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
                    state = control
                        .wake
                        .wait_timeout(state, TICK)
                        .unwrap_or_else(|p| p.into_inner())
                        .0;
                    let now = Instant::now();
                    if now.saturating_duration_since(state.last_tick) >= TICK {
                        control.tick(&mut state, now);
                    }
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
        self.paused.store(true, Ordering::Release);
        self.wake.notify_all();
    }

    pub fn set_recovering(&self, recovering: bool) {
        self.recovering.store(recovering, Ordering::Release);
        if recovering && self.enforcing {
            self.paused.store(true, Ordering::Release);
        }
    }

    pub fn suppress_reinsertion(&self) -> bool {
        self.enforcing && self.lock().snapshot.pressure != FillPressure::Healthy
    }

    pub fn staging_busy(&self) {
        self.staging_pressure.store(true, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FillControlSnapshot {
        let mut snapshot = self.lock().snapshot;
        snapshot.rejections = self.rejections.load(Ordering::Relaxed);
        snapshot.would_reject = self.would_reject.load(Ordering::Relaxed);
        snapshot
    }

    pub fn try_admit(&self, bytes: u64) -> Option<FillPermit<'_>> {
        let units = bytes.div_ceil(UNIT);
        let epoch = self.epoch.load(Ordering::Acquire);
        let mut value = self.credit.load(Ordering::Relaxed);
        if !self.paused.load(Ordering::Acquire) && !self.stopped.load(Ordering::Acquire) {
            for _ in 0..MAX_CAS_ATTEMPTS {
                if value >> 48 != epoch & COUNT_MASK || self.epoch.load(Ordering::Acquire) != epoch
                {
                    break;
                }
                if value & u64::from(u32::MAX) < units || (value >> 32) & COUNT_MASK == 0 {
                    break;
                }
                let next = value - units - (1 << 32);
                match self.credit.compare_exchange_weak(
                    value,
                    next,
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
        }
        if self.enforcing {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            self.would_reject.fetch_add(1, Ordering::Relaxed);
            Some(FillPermit {
                control: self,
                units: 0,
                epoch: 0,
                committed: true,
            })
        }
    }

    pub fn observe(&self, bytes: u64, timeout: Duration) -> io::Result<Progress<'_>> {
        let mut state = self.lock();
        let now = Instant::now();
        if state.slots.iter().all(Option::is_none) {
            state.last_progress = now;
        }
        let index = state
            .slots
            .iter()
            .position(Option::is_none)
            .ok_or_else(|| io::Error::other("background observation capacity exhausted"))?;
        state.slots[index] = Some(Observation {
            start: now,
            admitted: None,
            returned: None,
            bytes,
            timeout,
        });
        self.wake.notify_one();
        Ok(Progress {
            control: self,
            index,
            succeeded: false,
        })
    }

    fn tick(&self, state: &mut State, now: Instant) {
        state.last_tick = now;
        let elapsed = now
            .saturating_duration_since(state.window_start)
            .as_secs_f64();
        let decision = elapsed >= 0.5;
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
        let mut deadline = Duration::from_secs(5);
        let mut aged = false;
        for observation in state.slots.iter().flatten() {
            pending += 1;
            bytes = bytes.saturating_add(observation.bytes);
            let age = now.saturating_duration_since(observation.start);
            oldest = oldest.max(age);
            deadline = deadline.min(observation.timeout);
            aged |= age >= observation.timeout / 4;
        }
        let drain = if pending != 0 && state.service_bytes > 0. && state.service_ops > 0. {
            (bytes as f64 / state.service_bytes).max(pending as f64 / state.service_ops)
        } else {
            0.
        };
        let no_progress = pending != 0
            && now.saturating_duration_since(state.last_progress)
                >= (deadline / 2).min(Duration::from_millis(500));
        let recovering = self.recovering.load(Ordering::Acquire);
        let pressure = recovering || aged || no_progress || drain >= deadline.as_secs_f64() / 4.;
        let previous = state.snapshot.pressure;
        let staging_busy = decision && self.staging_pressure.swap(false, Ordering::Relaxed);
        if pressure {
            state.snapshot.pressure = FillPressure::Paused;
            state.clean_ticks = 0;
        } else if previous == FillPressure::Paused || state.was_recovering {
            if pending == 0
                || (made_progress && oldest < deadline / 10 && drain < deadline.as_secs_f64() / 10.)
            {
                state.snapshot.pressure = FillPressure::Throttled;
                state.snapshot.bytes_per_second = (state.snapshot.bytes_per_second / 2).max(640);
                state.snapshot.operations_per_second =
                    (state.snapshot.operations_per_second / 2).max(10);
                if state.service_bytes > 0. {
                    state.snapshot.bytes_per_second = state
                        .snapshot
                        .bytes_per_second
                        .min((state.service_bytes * 0.8) as u64)
                        .max(640);
                    state.snapshot.operations_per_second = state
                        .snapshot
                        .operations_per_second
                        .min((state.service_ops * 0.8) as u32)
                        .max(10);
                }
                state.clean_ticks = 0;
            }
        } else if staging_busy && pending >= state.previous_pending && pending != 0 {
            state.snapshot.pressure = FillPressure::Throttled;
            state.snapshot.bytes_per_second = (state.snapshot.bytes_per_second * 7 / 10).max(640);
            state.snapshot.operations_per_second =
                (state.snapshot.operations_per_second * 7 / 10).max(10);
            state.clean_ticks = 0;
        } else if made_progress {
            state.clean_ticks += 1;
            if state.clean_ticks >= 3 {
                state.snapshot.bytes_per_second = (state.snapshot.bytes_per_second
                    + self.options.max_bytes_per_second / 40)
                    .min(self.options.max_bytes_per_second);
                state.snapshot.operations_per_second = (state.snapshot.operations_per_second
                    + self.options.max_operations_per_second.div_ceil(40))
                .min(self.options.max_operations_per_second);
                if state.snapshot.bytes_per_second == self.options.max_bytes_per_second
                    && state.snapshot.operations_per_second
                        == self.options.max_operations_per_second
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
        self.paused.store(
            state.snapshot.pressure == FillPressure::Paused,
            Ordering::Release,
        );
        // Publishing a fresh epoch bounds refunds and prevents accumulating missed ticks.
        let clear =
            previous != state.snapshot.pressure || state.snapshot.pressure == FillPressure::Paused;
        let add_units = state.snapshot.bytes_per_second / 10 / UNIT;
        let add_ops = u64::from(state.snapshot.operations_per_second / 10);
        let epoch = self.epoch.fetch_add(1, Ordering::AcqRel).wrapping_add(1);
        let _ = self
            .credit
            .try_update(Ordering::AcqRel, Ordering::Relaxed, |old| {
                let units = if clear { 0 } else { old & u64::from(u32::MAX) };
                let ops = if clear { 0 } else { (old >> 32) & COUNT_MASK };
                let next_epoch = (epoch & COUNT_MASK) << 48;
                let (units, ops) = if state.snapshot.pressure == FillPressure::Paused {
                    (0, 0)
                } else {
                    (
                        (units + add_units).min(self.max_units),
                        (ops + add_ops).min(self.max_ops),
                    )
                };
                Some(next_epoch | (ops << 32) | units)
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
                || value >> 48 != self.epoch & COUNT_MASK
            {
                return;
            }
            let units = ((value & u64::from(u32::MAX)) + self.units).min(self.control.max_units);
            let ops = (((value >> 32) & COUNT_MASK) + 1).min(self.control.max_ops);
            let next = (value & !CREDIT_MASK) | (ops << 32) | units;
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
pub struct Progress<'a> {
    control: &'a FillController,
    index: usize,
    succeeded: bool,
}
impl Progress<'_> {
    pub fn admitted(&self) {
        self.control.lock().slots[self.index]
            .as_mut()
            .unwrap()
            .admitted = Some(Instant::now());
    }
    pub fn returned(&self) {
        self.control.lock().slots[self.index]
            .as_mut()
            .unwrap()
            .returned = Some(Instant::now());
    }
    pub fn finish(mut self) {
        self.succeeded = true;
    }
}
impl Drop for Progress<'_> {
    fn drop(&mut self) {
        let mut state = self.control.lock();
        let observation = state.slots[self.index].take().expect("live progress slot");
        let now = Instant::now();
        if self.succeeded {
            state.completed_ops = state.completed_ops.saturating_add(1);
            state.completed_bytes = state.completed_bytes.saturating_add(observation.bytes);
            state.last_progress = now;
        }
        let admission_end = observation.admitted.unwrap_or(now);
        state.snapshot.admission_ns = state.snapshot.admission_ns.saturating_add(nanos(
            admission_end.saturating_duration_since(observation.start),
        ));
        if let Some(admitted) = observation.admitted {
            let wait_end = observation.returned.unwrap_or(now);
            state.snapshot.completion_wait_ns = state
                .snapshot
                .completion_wait_ns
                .saturating_add(nanos(wait_end.saturating_duration_since(admitted)));
            if let Some(returned) = observation.returned {
                state.snapshot.validation_ns = state
                    .snapshot
                    .validation_ns
                    .saturating_add(nanos(now.saturating_duration_since(returned)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control(enforcing: bool) -> Arc<FillController> {
        let settings = AdaptiveFillOptions::new(64_000, 100);
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
        control.staging_busy();
        tick(&control, TICK);
        assert_eq!(control.snapshot().bytes_per_second, 64_000);
        {
            let mut state = control.lock();
            state.last_progress = state.last_tick + Duration::from_millis(500);
        }
        tick(&control, Duration::from_millis(500));
        assert_eq!(control.snapshot().pressure, FillPressure::Throttled);
        assert_eq!(control.snapshot().bytes_per_second, 44_800);
        assert_eq!(control.snapshot().operations_per_second, 70);
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
        fast.returned();
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
        old.returned();
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
        assert_eq!(
            control.credit.load(Ordering::Relaxed) & CREDIT_MASK,
            (control.max_ops << 32) | control.max_units
        );
        let _new = control.observe(4096, Duration::from_secs(2)).unwrap();
        // Production ticks and observations use the same clock.
        let mut state = control.lock();
        control.tick(&mut state, Instant::now());
        assert_eq!(state.snapshot.pressure, FillPressure::Healthy);
    }

    #[test]
    fn large_record_remains_eligible_and_stop_cannot_reopen_admission() {
        let control = FillController::new(
            FillControlOptions::Adaptive(AdaptiveFillOptions::new(640, 10)),
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
            FillControlOptions::Observe(AdaptiveFillOptions::new(640, 10)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        let pending = control.observe(4096, Duration::from_secs(5)).unwrap();
        assert!(control.observe(64, Duration::from_secs(5)).is_err());
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
