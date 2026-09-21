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

//! Background I/O observation, timeout recovery, and publication boundaries.
//! Fill control and recovery are independent runtime-owned policies borrowed by each attempt.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::io::fill_control::FillController;
use crate::io::fill_control::IoObservation;

const SLOW_IO_CHECK_CAP: Duration = Duration::from_millis(500);

/// Shared timeout budget and admission fence for background I/O recovery.
/// Restart recovery and terminal Region health are separate concerns.
pub struct IoRecovery {
    timeout: Option<Duration>,
    pending: AtomicUsize,
    stopped: AtomicBool,
}

impl IoRecovery {
    pub const fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            pending: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn is_recovering(&self) -> bool {
        self.pending.load(Ordering::Acquire) != 0
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }

    fn next_deadline(&self, original: Instant) -> Option<Instant> {
        if self.stopped.load(Ordering::Acquire) {
            return None;
        }
        let now = Instant::now();
        let deadline = now.checked_add(Duration::from_secs(1))?;
        let deadline = match self.timeout {
            Some(timeout) => {
                let end = original.checked_add(timeout)?;
                if now >= end {
                    return None;
                }
                deadline.min(end)
            }
            None => deadline,
        };
        Some(deadline)
    }
}

/// Tracks one background operation from admission through validation and publication.
/// Slow I/O pauses fills until completion; timeout recovery keeps its fence until
/// successful publication. Failed recovery deliberately leaves the fence raised.
pub struct BackgroundIoAttempt<'a> {
    io_recovery: &'a IoRecovery,
    fill_control: Option<&'a FillController>,
    recovering: bool,
    slow: bool,
    started: Option<Instant>,
    io_timeout: Duration,
    observation: Option<IoObservation<'a>>,
}

impl<'a> BackgroundIoAttempt<'a> {
    pub const fn new(
        io_recovery: &'a IoRecovery,
        fill_control: Option<&'a FillController>,
    ) -> Self {
        Self {
            io_recovery,
            fill_control,
            recovering: false,
            slow: false,
            started: None,
            io_timeout: Duration::ZERO,
            observation: None,
        }
    }

    pub fn start(&mut self, bytes: u64, timeout: Duration) {
        self.started = Some(Instant::now());
        self.io_timeout = timeout;
        if let Some(fill) = self.fill_control {
            self.observation = fill.observe_io(bytes);
        }
    }

    /// First wait bound: a fill checkpoint before the normal I/O timeout.
    pub fn wait_deadline(&self, original: Instant) -> Instant {
        if self.fill_control.is_none() || self.slow {
            return original;
        }
        let Some(start) = self.started else {
            return original;
        };
        (start + (self.io_timeout / 4).min(SLOW_IO_CHECK_CAP)).min(original)
    }

    pub fn note_slow(&mut self) {
        if self.slow {
            return;
        }
        self.slow = true;
        if let Some(observation) = &mut self.observation {
            observation.note_slow();
        }
    }

    pub fn clear_slow(&mut self) {
        if !self.slow {
            return;
        }
        self.slow = false;
        if let Some(observation) = &mut self.observation {
            observation.clear_slow();
        }
    }

    /// Poll completion/admission at fixed one-second intervals, without extending
    /// a configured total budget. Shutdown also terminates unlimited recovery.
    pub fn next_deadline(&mut self, original: Instant) -> Option<Instant> {
        let deadline = self.io_recovery.next_deadline(original)?;
        if !self.recovering {
            self.recovering = true;
            if let Some(fill) = self.fill_control {
                fill.set_recovering(true);
            }
            if self.io_recovery.pending.fetch_add(1, Ordering::AcqRel) == 0 {
                log::warn!(target: "cache2::health", event = "cache_io_recovery_started";
                "background I/O timed out; pausing cache fills while retaining owned requests");
            }
        }
        Some(deadline)
    }

    /// Called only after operation-result validation and publication succeed.
    pub fn finish(mut self) {
        if let Some(observation) = self.observation.take() {
            observation.finish();
        }
        if self.recovering {
            let last = self.io_recovery.pending.fetch_sub(1, Ordering::AcqRel) == 1;
            if let Some(fill) = self.fill_control {
                fill.set_recovering(false);
            }
            if last {
                log::info!(target: "cache2::health", event = "cache_io_recovery_completed";
                "all timed-out background operations recovered and passed validation");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FillControlOptions;
    use crate::FillLimits;
    use crate::FillPressure;

    #[test]
    fn failed_publication_keeps_recovery_fenced_after_io_observation_ends() {
        let fill = FillController::new(
            FillControlOptions::Adaptive(FillLimits::new(64_000, 100)),
            2,
            4096,
        )
        .unwrap()
        .unwrap();
        let recovery = IoRecovery::new(None);
        let mut failed = BackgroundIoAttempt::new(&recovery, Some(&fill));
        failed.start(4096, Duration::from_secs(1));
        failed.note_slow();
        assert_eq!(fill.snapshot().pressure, FillPressure::Paused);
        assert!(!recovery.is_recovering());

        assert!(failed.next_deadline(Instant::now()).is_some());
        failed.clear_slow();
        // I/O completed, but validation/publication failed before finish.
        drop(failed);
        assert_eq!(fill.snapshot().outstanding_operations, 0);
        assert!(recovery.is_recovering());
        assert!(!fill.try_admit_fill());

        let mut successful = BackgroundIoAttempt::new(&recovery, Some(&fill));
        successful.start(4096, Duration::from_secs(1));
        assert!(successful.next_deadline(Instant::now()).is_some());
        successful.finish();
        assert!(recovery.is_recovering());
        assert!(!fill.try_admit_fill());
    }
}
