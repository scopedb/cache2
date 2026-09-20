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

//! Reversible background timeout recovery, separate from the health latch.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

use crate::io::fill_control::FillController;
use crate::io::fill_control::Observation;

/// Shared across background workers. Resource ownership remains with each worker.
pub struct BackgroundRecovery {
    timeout: Option<Duration>,
    pending: AtomicUsize,
    stopped: AtomicBool,
    pub fill: Option<Arc<FillController>>,
}

impl BackgroundRecovery {
    pub const fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            pending: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
            fill: None,
        }
    }

    pub fn with_fill(timeout: Option<Duration>, fill: Option<Arc<FillController>>) -> Self {
        Self {
            fill,
            ..Self::new(timeout)
        }
    }

    pub fn attempt(&self) -> RecoveryAttempt<'_> {
        RecoveryAttempt {
            recovery: self,
            entered: false,
            slow: false,
            started: None,
            io_timeout: Duration::ZERO,
            observation: None,
        }
    }

    pub fn is_recovering(&self) -> bool {
        self.pending.load(Ordering::Acquire) != 0
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
        if let Some(fill) = &self.fill {
            fill.stop();
        }
    }
}

/// Retains the admission fence until the caller validates and publishes success.
/// Failed attempts deliberately leave the fence raised until instance teardown.
pub struct RecoveryAttempt<'a> {
    recovery: &'a BackgroundRecovery,
    entered: bool,
    slow: bool,
    started: Option<Instant>,
    io_timeout: Duration,
    observation: Option<Observation<'a>>,
}

impl RecoveryAttempt<'_> {
    pub fn start(&mut self, bytes: u64, timeout: Duration) {
        self.started = Some(Instant::now());
        self.io_timeout = timeout;
        if let Some(fill) = &self.recovery.fill {
            self.observation = fill.observe(bytes, timeout);
        }
    }

    /// First wait bound: a fill checkpoint before the normal I/O timeout.
    pub fn wait_cap(&self, original: Instant) -> Instant {
        if self.recovery.fill.is_none() || self.slow {
            return original;
        }
        let Some(start) = self.started else {
            return original;
        };
        FillController::checkpoint(start, self.io_timeout).min(original)
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
        if self.recovery.stopped.load(Ordering::Acquire) {
            return None;
        }
        let now = Instant::now();
        let deadline = now.checked_add(Duration::from_secs(1))?;
        let deadline = match self.recovery.timeout {
            Some(timeout) => {
                let end = original.checked_add(timeout)?;
                if now >= end {
                    return None;
                }
                deadline.min(end)
            }
            None => deadline,
        };
        if !self.entered {
            self.entered = true;
            if let Some(fill) = &self.recovery.fill {
                fill.set_recovering(true);
            }
            if self.recovery.pending.fetch_add(1, Ordering::AcqRel) == 0 {
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
        if self.entered {
            let last = self.recovery.pending.fetch_sub(1, Ordering::AcqRel) == 1;
            if let Some(fill) = &self.recovery.fill {
                fill.set_recovering(false);
            }
            if last {
                log::info!(target: "cache2::health", event = "cache_io_recovery_completed";
                "all timed-out background operations recovered and passed validation");
            }
        }
    }
}
