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

use std::cell::Cell;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use std::time::Instant;

/// Shared across background workers. Resource ownership remains with each worker.
pub struct BackgroundRecovery {
    timeout: Option<Duration>,
    pending: AtomicUsize,
    stopped: AtomicBool,
}

impl BackgroundRecovery {
    pub const fn new(timeout: Option<Duration>) -> Self {
        Self {
            timeout,
            pending: AtomicUsize::new(0),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn attempt(&self) -> RecoveryAttempt<'_> {
        RecoveryAttempt {
            recovery: self,
            entered: Cell::new(false),
        }
    }

    pub fn is_recovering(&self) -> bool {
        self.pending.load(Ordering::Acquire) != 0
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
}

/// Retains the admission fence until the caller validates and publishes success.
/// Failed attempts deliberately leave the fence raised until instance teardown.
pub struct RecoveryAttempt<'a> {
    recovery: &'a BackgroundRecovery,
    entered: Cell<bool>,
}

impl RecoveryAttempt<'_> {
    /// Poll completion/admission at fixed one-second intervals, without extending
    /// a configured total budget. Shutdown also terminates unlimited recovery.
    pub fn next_deadline(&self, original: Instant) -> Option<Instant> {
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
        if !self.entered.replace(true) && self.recovery.pending.fetch_add(1, Ordering::AcqRel) == 0
        {
            log::warn!(target: "cache2::health", event = "cache_io_recovery_started";
                "background I/O timed out; pausing cache fills while retaining owned requests");
        }
        Some(deadline)
    }

    /// Called only after operation-result validation and publication succeed.
    pub fn finish(&self) {
        if self.entered.replace(false) && self.recovery.pending.fetch_sub(1, Ordering::AcqRel) == 1
        {
            log::info!(target: "cache2::health", event = "cache_io_recovery_completed";
                "all timed-out background operations recovered and passed validation");
        }
    }
}
