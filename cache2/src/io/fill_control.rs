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

//! Bounded pre-timeout observation and worker-paced fill admission.

use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::FillControlOptions;
use crate::FillControlSnapshot;
use crate::FillLimits;
use crate::FillPressure;

const CREDIT_UNIT_BYTES: u64 = 64;
const MAX_CAS_ATTEMPTS: usize = 4;
const MIN_BYTES_PER_SECOND: u64 = 640;
const MAX_BYTES_PER_SECOND: u64 = 1 << 40;
const MIN_RECORDS_PER_SECOND: u32 = 10;
const MAX_RECORDS_PER_SECOND: u32 = u32::MAX;
const TICKS_PER_SECOND: u64 = 10;

/// Wake shard workers this often when a non-essential flush is waiting for budget.
pub const FLUSH_RETRY: Duration = Duration::from_millis(100);

/// Credit taken by one non-essential flush. Zero for Observe and essential work.
#[derive(Clone, Copy)]
pub struct FlushCharge {
    pub records: u64,
    pub byte_units: u64,
}

fn packed_records(value: u64) -> u64 {
    value >> 32
}
fn packed_byte_units(value: u64) -> u64 {
    value & u64::from(u32::MAX)
}
fn pack_credit(records: u64, byte_units: u64) -> u64 {
    (records << 32) | byte_units
}

fn encode_pressure(pressure: FillPressure) -> u8 {
    match pressure {
        FillPressure::Disabled => 0,
        FillPressure::Healthy => 1,
        FillPressure::Paused => 2,
    }
}

fn decode_pressure(value: u8) -> FillPressure {
    match value {
        1 => FillPressure::Healthy,
        2 => FillPressure::Paused,
        _ => FillPressure::Disabled,
    }
}

fn nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

struct Slot {
    start_ns: AtomicU64,
    bytes: AtomicU64,
}

pub struct FillController {
    limits: FillLimits,
    enforcing: bool,
    max_byte_units: u64,
    max_records: u64,
    origin: Instant,
    credit: AtomicU64,
    last_refill_ns: AtomicU64,
    pressure: AtomicU8,
    stopped: AtomicBool,
    recovering: AtomicUsize,
    pause_holders: AtomicUsize,
    rejections: AtomicU64,
    would_reject: AtomicU64,
    dropped_observations: AtomicU64,
    completed_bytes: AtomicU64,
    completed_io_operations: AtomicU64,
    slots: Box<[Slot]>,
}

impl FillController {
    pub fn allocation_bytes(options: FillControlOptions, slots: usize) -> io::Result<usize> {
        let limits = match options {
            FillControlOptions::Disabled => return Ok(0),
            FillControlOptions::Observe(limits) | FillControlOptions::Adaptive(limits) => limits,
        };
        if !(MIN_BYTES_PER_SECOND..=MAX_BYTES_PER_SECOND).contains(&limits.max_bytes_per_second)
            || !(MIN_RECORDS_PER_SECOND..=MAX_RECORDS_PER_SECOND)
                .contains(&limits.max_records_per_second)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill rate ceilings are out of range",
            ));
        }
        slots
            .checked_mul(size_of::<Slot>())
            .and_then(|bytes| bytes.checked_add(size_of::<Self>() + 256))
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
        max_flush_bytes: u64,
    ) -> io::Result<Option<Arc<Self>>> {
        Self::allocation_bytes(options, slots)?;
        let (limits, enforcing) = match options {
            FillControlOptions::Disabled => return Ok(None),
            FillControlOptions::Observe(limits) => (limits, false),
            FillControlOptions::Adaptive(limits) => (limits, true),
        };
        let max_byte_units = (limits.max_bytes_per_second / TICKS_PER_SECOND)
            .max(max_flush_bytes)
            .div_ceil(CREDIT_UNIT_BYTES);
        if max_byte_units > u64::from(u32::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fill burst exceeds budget representation",
            ));
        }
        let max_records = u64::from(limits.max_records_per_second / TICKS_PER_SECOND as u32).max(1);
        let mut table = Vec::new();
        table.try_reserve_exact(slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate fill observations",
            )
        })?;
        table.resize_with(slots, || Slot {
            start_ns: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
        });
        Ok(Some(Arc::new(Self {
            limits,
            enforcing,
            max_byte_units,
            max_records,
            origin: Instant::now(),
            credit: AtomicU64::new(pack_credit(max_records, max_byte_units)),
            last_refill_ns: AtomicU64::new(0),
            pressure: AtomicU8::new(encode_pressure(FillPressure::Healthy)),
            stopped: AtomicBool::new(false),
            recovering: AtomicUsize::new(0),
            pause_holders: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
            would_reject: AtomicU64::new(0),
            dropped_observations: AtomicU64::new(0),
            completed_bytes: AtomicU64::new(0),
            completed_io_operations: AtomicU64::new(0),
            slots: table.into_boxed_slice(),
        })))
    }

    fn elapsed_ns(&self) -> u64 {
        nanos(self.origin.elapsed())
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.publish();
    }

    pub fn set_recovering(&self, recovering: bool) {
        if recovering {
            self.recovering.fetch_add(1, Ordering::AcqRel);
        } else {
            loop {
                let holds = self.recovering.load(Ordering::Acquire);
                if holds == 0
                    || self
                        .recovering
                        .compare_exchange(holds, holds - 1, Ordering::AcqRel, Ordering::Relaxed)
                        .is_ok()
                {
                    break;
                }
            }
        }
        self.publish();
    }

    fn hold_pause(&self) {
        self.pause_holders.fetch_add(1, Ordering::AcqRel);
        self.publish();
    }

    fn release_pause(&self) {
        self.pause_holders.fetch_sub(1, Ordering::AcqRel);
        self.publish();
    }

    fn publish(&self) {
        let paused = self.stopped.load(Ordering::Acquire)
            || self.recovering.load(Ordering::Acquire) != 0
            || self.pause_holders.load(Ordering::Acquire) != 0;
        let next = if paused {
            FillPressure::Paused
        } else {
            FillPressure::Healthy
        };
        let previous =
            decode_pressure(self.pressure.swap(encode_pressure(next), Ordering::Release));
        if previous != next && previous != FillPressure::Disabled {
            log::info!(target: "cache2::health", event = "cache_fill_pressure_changed", pressure:? = next;
                "cache fill admission pressure changed");
        }
    }

    fn paused(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
            || self.recovering.load(Ordering::Acquire) != 0
            || self.pause_holders.load(Ordering::Acquire) != 0
    }

    pub fn suppress_reinsertion(&self) -> bool {
        self.enforcing && self.paused()
    }

    pub fn snapshot(&self) -> FillControlSnapshot {
        let pressure = if self.paused() {
            FillPressure::Paused
        } else {
            FillPressure::Healthy
        };
        let now = self.elapsed_ns();
        let mut outstanding_operations = 0_u64;
        let mut outstanding_bytes = 0_u64;
        let mut oldest = 0_u64;
        for slot in self.slots.iter() {
            let start = slot.start_ns.load(Ordering::Acquire);
            if start == 0 {
                continue;
            }
            outstanding_operations += 1;
            outstanding_bytes =
                outstanding_bytes.saturating_add(slot.bytes.load(Ordering::Relaxed));
            oldest = oldest.max(now.saturating_sub(start));
        }
        let completed = self.completed_bytes.load(Ordering::Relaxed);
        let drain = if outstanding_operations != 0 && completed != 0 && now != 0 {
            (outstanding_bytes as f64 / (completed as f64 / (now as f64 / 1e9)) * 1e9)
                .min(u64::MAX as f64) as u64
        } else {
            0
        };
        let (byte_rate, record_rate) = if pressure == FillPressure::Paused {
            (0, 0)
        } else {
            (
                self.limits.max_bytes_per_second,
                self.limits.max_records_per_second,
            )
        };
        FillControlSnapshot {
            pressure,
            enforcing: self.enforcing,
            bytes_per_second: byte_rate,
            records_per_second: record_rate,
            rejections: self.rejections.load(Ordering::Relaxed),
            would_reject: self.would_reject.load(Ordering::Relaxed),
            dropped_observations: self.dropped_observations.load(Ordering::Relaxed),
            outstanding_operations,
            outstanding_bytes,
            oldest_operation_ns: oldest,
            estimated_drain_ns: drain,
        }
    }

    /// Foreground admission: Adaptive rejects only while paused.
    pub fn try_admit_fill(&self) -> bool {
        if !self.enforcing {
            if self.paused() {
                self.would_reject.fetch_add(1, Ordering::Relaxed);
            }
            return true;
        }
        if self.paused() {
            self.rejections.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            true
        }
    }

    /// Background flush pacing. Essential flushes always proceed.
    pub fn try_acquire_flush_budget(
        &self,
        bytes: u64,
        records: u32,
        essential: bool,
    ) -> Option<FlushCharge> {
        if essential || !self.enforcing {
            return Some(FlushCharge {
                records: 0,
                byte_units: 0,
            });
        }
        self.refill();
        let raw_byte_units = bytes.div_ceil(CREDIT_UNIT_BYTES).max(1);
        let raw_records = u64::from(records.max(1));
        let byte_units = raw_byte_units.min(self.max_byte_units);
        let records = raw_records.min(self.max_records);
        let byte_oversized = raw_byte_units >= self.max_byte_units;
        let record_oversized = raw_records >= self.max_records;
        let mut value = self.credit.load(Ordering::Relaxed);
        for _ in 0..MAX_CAS_ATTEMPTS {
            let have_byte_units = packed_byte_units(value);
            let have_records = packed_records(value);
            if have_byte_units == 0 || have_records == 0 {
                return None;
            }
            if !byte_oversized && have_byte_units < byte_units {
                return None;
            }
            if !record_oversized && have_records < records {
                return None;
            }
            let take_byte_units = if byte_oversized {
                have_byte_units
            } else {
                byte_units
            };
            let take_records = if record_oversized {
                have_records
            } else {
                records
            };
            let next = pack_credit(
                have_records - take_records,
                have_byte_units - take_byte_units,
            );
            match self
                .credit
                .compare_exchange(value, next, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Some(FlushCharge {
                        records: take_records,
                        byte_units: take_byte_units,
                    });
                }
                Err(current) => value = current,
            }
        }
        None
    }

    pub fn refund_flush_budget(&self, charge: FlushCharge) {
        if !self.enforcing || (charge.records == 0 && charge.byte_units == 0) {
            return;
        }
        self.add_credit(charge.records, charge.byte_units);
    }

    fn refill(&self) {
        let now = self.elapsed_ns();
        let last = self.last_refill_ns.load(Ordering::Relaxed);
        let Some(dt) = now.checked_sub(last).filter(|dt| *dt != 0) else {
            return;
        };
        let add_byte_units = self
            .limits
            .max_bytes_per_second
            .saturating_mul(dt)
            .checked_div(1_000_000_000)
            .unwrap_or(0)
            / CREDIT_UNIT_BYTES;
        let add_records = u64::from(self.limits.max_records_per_second)
            .saturating_mul(dt)
            .checked_div(1_000_000_000)
            .unwrap_or(0);
        if add_byte_units == 0 && add_records == 0 {
            return;
        }
        if self
            .last_refill_ns
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.add_credit(add_records, add_byte_units);
    }

    fn add_credit(&self, records: u64, byte_units: u64) {
        if records == 0 && byte_units == 0 {
            return;
        }
        let mut value = self.credit.load(Ordering::Relaxed);
        loop {
            let next = pack_credit(
                (packed_records(value) + records).min(self.max_records),
                (packed_byte_units(value) + byte_units).min(self.max_byte_units),
            );
            match self
                .credit
                .compare_exchange(value, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return,
                Err(current) => value = current,
            }
        }
    }

    pub fn observe_io(&self, bytes: u64) -> Option<IoObservation<'_>> {
        let start = self.elapsed_ns().max(1);
        for (index, slot) in self.slots.iter().enumerate() {
            if slot
                .start_ns
                .compare_exchange(0, start, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                slot.bytes.store(bytes, Ordering::Relaxed);
                return Some(IoObservation {
                    control: self,
                    index,
                    succeeded: false,
                    slow: false,
                });
            }
        }
        self.dropped_observations.fetch_add(1, Ordering::Relaxed);
        None
    }
}

/// One worker-owned observation. Failed work is removed, never marked successful.
pub struct IoObservation<'a> {
    control: &'a FillController,
    index: usize,
    succeeded: bool,
    slow: bool,
}
impl IoObservation<'_> {
    pub fn note_slow(&mut self) {
        if self.slow {
            return;
        }
        self.slow = true;
        self.control.hold_pause();
    }

    pub fn clear_slow(&mut self) {
        if !self.slow {
            return;
        }
        self.slow = false;
        self.control.release_pause();
    }

    pub fn finish(mut self) {
        self.succeeded = true;
    }
}
impl Drop for IoObservation<'_> {
    fn drop(&mut self) {
        let slot = &self.control.slots[self.index];
        let bytes = slot.bytes.load(Ordering::Relaxed);
        slot.start_ns.store(0, Ordering::Release);
        if self.succeeded {
            self.control
                .completed_io_operations
                .fetch_add(1, Ordering::Relaxed);
            self.control
                .completed_bytes
                .fetch_add(bytes, Ordering::Relaxed);
        }
        if self.slow {
            self.control.release_pause();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn control(enforcing: bool) -> Arc<FillController> {
        let limits = FillLimits::new(64_000, 100);
        FillController::new(
            if enforcing {
                FillControlOptions::Adaptive(limits)
            } else {
                FillControlOptions::Observe(limits)
            },
            4,
            4096,
        )
        .unwrap()
        .unwrap()
    }

    fn restore_burst(control: &FillController) {
        control.credit.store(
            pack_credit(control.max_records, control.max_byte_units),
            Ordering::Release,
        );
    }

    fn freeze_refill(control: &FillController) {
        control
            .last_refill_ns
            .store(control.elapsed_ns(), Ordering::Relaxed);
    }

    #[test]
    fn foreground_admit_ignores_flush_budget() {
        let control = control(true);
        for _ in 0..10 {
            assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        }
        assert!(control.try_acquire_flush_budget(64, 1, false).is_none());
        assert!(control.try_admit_fill());
    }

    #[test]
    fn record_budget_paces_nonessential_flush() {
        let control = control(true);
        for _ in 0..10 {
            assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        }
        assert!(control.try_acquire_flush_budget(64, 1, false).is_none());
        assert!(control.try_acquire_flush_budget(64, 1, true).is_some());
        restore_burst(&control);
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
    }

    #[test]
    fn byte_budget_paces_nonessential_flush() {
        let control = control(true);
        assert!(control.try_acquire_flush_budget(4096, 1, false).is_some());
        assert!(control.try_acquire_flush_budget(4096, 1, false).is_none());
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        restore_burst(&control);
        assert!(control.try_acquire_flush_budget(4096, 1, false).is_some());
    }

    #[test]
    fn old_request_pauses_while_other_requests_progress() {
        let control = control(true);
        let mut old = control.observe_io(4096).unwrap();
        let fast = control.observe_io(64).unwrap();
        fast.finish();
        old.note_slow();
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(!control.try_admit_fill());
        drop(old);
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert!(control.try_admit_fill());
    }

    #[test]
    fn observe_never_rejects_or_suppresses_reinsertion() {
        let control = control(false);
        let mut old = control.observe_io(4096).unwrap();
        old.note_slow();
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        for _ in 0..20 {
            assert!(control.try_admit_fill());
        }
        assert_eq!(control.snapshot().would_reject, 20);
        assert_eq!(control.snapshot().rejections, 0);
        assert!(!control.suppress_reinsertion());
    }

    #[test]
    fn observe_would_reject_counts_pause_not_budget() {
        let control = control(false);
        for _ in 0..20 {
            assert!(control.try_acquire_flush_budget(4096, 1, false).is_some());
            assert!(control.try_admit_fill());
        }
        assert_eq!(control.snapshot().would_reject, 0);
        let mut old = control.observe_io(4096).unwrap();
        old.note_slow();
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(control.try_admit_fill());
        assert_eq!(control.snapshot().would_reject, 1);
        assert_eq!(control.snapshot().rejections, 0);
    }

    #[test]
    fn idle_time_does_not_inflate_credit() {
        let control = control(true);
        control.last_refill_ns.store(0, Ordering::Relaxed);
        control.refill();
        let credit = control.credit.load(Ordering::Relaxed);
        assert_eq!(packed_records(credit), control.max_records);
        assert_eq!(packed_byte_units(credit), control.max_byte_units);
    }

    #[test]
    fn large_flush_remains_eligible_and_stop_cannot_reopen_admission() {
        let control = FillController::new(
            FillControlOptions::Adaptive(FillLimits::new(640, 10)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        assert!(control.try_acquire_flush_budget(4096, 1, false).is_some());
        restore_burst(&control);
        assert!(control.try_acquire_flush_budget(4096, 1, false).is_some());
        control.stop();
        assert!(!control.try_admit_fill());
    }

    #[test]
    fn recovering_fence_does_not_need_outstanding_io() {
        let control = control(true);
        control.set_recovering(true);
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(!control.try_admit_fill());
        control.set_recovering(false);
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert!(control.try_admit_fill());
    }

    #[test]
    fn overlapping_recovery_holds_resume_when_all_release() {
        let control = control(true);
        control.set_recovering(true);
        control.set_recovering(true);
        assert!(!control.try_admit_fill());
        control.set_recovering(false);
        assert_eq!(control.snapshot().pressure, FillPressure::Paused);
        assert!(!control.try_admit_fill());
        control.set_recovering(false);
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert!(control.try_admit_fill());
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
        let pending = control.observe_io(4096).unwrap();
        assert!(control.observe_io(64).is_none());
        assert_eq!(control.snapshot().dropped_observations, 1);
        drop(pending);
        let pending = control.observe_io(4096).unwrap();
        drop(pending);
        assert_eq!(control.completed_io_operations.load(Ordering::Relaxed), 0);
        assert_eq!(control.completed_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(control.slots[0].start_ns.load(Ordering::Acquire), 0);
    }

    #[test]
    fn concurrent_flush_never_exceeds_shared_budget() {
        let control = control(true);
        freeze_refill(&control);
        let accepted = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let control = &control;
                let accepted = &accepted;
                scope.spawn(move || {
                    for _ in 0..100 {
                        if control.try_acquire_flush_budget(640, 1, false).is_some() {
                            accepted.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        assert_eq!(accepted.load(Ordering::Relaxed), 10);
    }

    #[test]
    fn io_completion_releases_pause_before_observation_drop() {
        let control = control(true);
        let mut pending = control.observe_io(4096).unwrap();
        pending.note_slow();
        assert!(!control.try_admit_fill());
        pending.clear_slow();
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert!(control.try_admit_fill());
        pending.finish();
    }

    #[test]
    fn concurrent_pause_holders_resume_when_all_release() {
        let control = control(true);
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let control = &control;
                scope.spawn(move || {
                    let mut pending = control.observe_io(64).unwrap();
                    pending.note_slow();
                    assert!(!control.try_admit_fill());
                    drop(pending);
                });
            }
        });
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert_eq!(control.pause_holders.load(Ordering::Acquire), 0);
        assert!(control.try_admit_fill());
    }

    #[test]
    fn overlapping_hold_and_release_cannot_stick_paused() {
        let control = control(true);
        for _ in 0..1_000 {
            let mut first = control.observe_io(64).unwrap();
            let mut second = control.observe_io(64).unwrap();
            first.note_slow();
            second.note_slow();
            drop(first);
            drop(second);
            assert_eq!(control.pause_holders.load(Ordering::Acquire), 0);
            assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
            assert!(control.try_admit_fill());
        }
    }

    #[test]
    fn burst_sized_flush_proceeds_on_remaining_credit() {
        let control = control(true);
        freeze_refill(&control);
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        assert!(
            control
                .try_acquire_flush_budget(1_000_000, 1, false)
                .is_some()
        );
        restore_burst(&control);
        freeze_refill(&control);
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        assert!(
            control
                .try_acquire_flush_budget(64, 1_000_000, false)
                .is_some()
        );
    }

    #[test]
    fn refund_restores_nonessential_budget() {
        let control = control(true);
        freeze_refill(&control);
        let mut last = FlushCharge {
            records: 0,
            byte_units: 0,
        };
        for _ in 0..10 {
            last = control.try_acquire_flush_budget(64, 1, false).unwrap();
        }
        assert!(control.try_acquire_flush_budget(64, 1, false).is_none());
        control.refund_flush_budget(last);
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
    }

    #[test]
    fn sub_tick_refills_do_not_burn_elapsed_time() {
        let control = FillController::new(
            FillControlOptions::Adaptive(FillLimits::new(640, 10)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        freeze_refill(&control);
        while control.try_acquire_flush_budget(64, 1, false).is_some() {}
        let one_ms_ago = control.elapsed_ns().saturating_sub(1_000_000);
        control.last_refill_ns.store(one_ms_ago, Ordering::Relaxed);
        for _ in 0..32 {
            control.refill();
        }
        assert_eq!(control.last_refill_ns.load(Ordering::Relaxed), one_ms_ago);
        std::thread::sleep(Duration::from_millis(110));
        control.refill();
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
    }

    #[test]
    fn concurrent_refunds_are_not_dropped() {
        let control = FillController::new(
            FillControlOptions::Adaptive(FillLimits::new(64_000, 10_000)),
            1,
            4096,
        )
        .unwrap()
        .unwrap();
        freeze_refill(&control);
        while control.try_acquire_flush_budget(64, 1, false).is_some() {}
        let before = control.credit.load(Ordering::Relaxed);
        std::thread::scope(|scope| {
            for _ in 0..32 {
                let control = &control;
                scope.spawn(move || {
                    control.refund_flush_budget(FlushCharge {
                        records: 1,
                        byte_units: 1,
                    });
                });
            }
        });
        let credit = control.credit.load(Ordering::Relaxed);
        assert_eq!(
            packed_records(credit),
            (packed_records(before) + 32).min(control.max_records)
        );
        assert_eq!(
            packed_byte_units(credit),
            (packed_byte_units(before) + 32).min(control.max_byte_units)
        );
    }

    #[test]
    fn oversized_refund_restores_only_consumed_remainder() {
        let control = control(true);
        freeze_refill(&control);
        for _ in 0..9 {
            assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        }
        let charge = control
            .try_acquire_flush_budget(1_000_000, 1, false)
            .unwrap();
        assert_eq!(charge.records, 1);
        assert_eq!(charge.byte_units, control.max_byte_units - 9);
        control.refund_flush_budget(charge);
        let credit = control.credit.load(Ordering::Relaxed);
        assert_eq!(packed_records(credit), 1);
        assert_eq!(packed_byte_units(credit), charge.byte_units);
        assert!(control.try_acquire_flush_budget(64, 1, false).is_some());
        assert!(control.try_acquire_flush_budget(64, 1, false).is_none());
    }
}
