//! Test-only process crash points spanning the Hybrid coordinator and its
//! three persistent components.

use std::sync::atomic::{AtomicU64, Ordering};

const POINT_ENV: &str = "CACHE_RS_HYBRID_COMBINED_CRASH_POINT";
const OCCURRENCE_ENV: &str = "CACHE_RS_HYBRID_COMBINED_CRASH_OCCURRENCE";

static MATCHED_OCCURRENCES: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum HybridCrashPoint {
    GroupJournalRecordsWritten,
    GroupJournalSentinelWritten,
    GroupJournalSynced,
    TargetWritten,
    BeforeSourceRemove,
    AfterSourceRemove,
    AfterFirstRemove,
    AfterAllRemoves,
    GlobalCleanPublished,
}

impl HybridCrashPoint {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::GroupJournalRecordsWritten => "group-journal-records-written",
            Self::GroupJournalSentinelWritten => "group-journal-sentinel-written",
            Self::GroupJournalSynced => "group-journal-synced",
            Self::TargetWritten => "target-written",
            Self::BeforeSourceRemove => "before-source-remove",
            Self::AfterSourceRemove => "after-source-remove",
            Self::AfterFirstRemove => "after-first-remove",
            Self::AfterAllRemoves => "after-all-removes",
            Self::GlobalCleanPublished => "global-clean-published",
        }
    }
}

pub(crate) fn hit(point: HybridCrashPoint) {
    if std::env::var(POINT_ENV).ok().as_deref() != Some(point.name()) {
        return;
    }
    let expected = std::env::var(OCCURRENCE_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    let occurrence = MATCHED_OCCURRENCES.fetch_add(1, Ordering::SeqCst) + 1;
    if occurrence != expected {
        return;
    }
    kill_process();
}

#[cfg(unix)]
fn kill_process() -> ! {
    const SIGKILL: i32 = 9;
    // SAFETY: `kill` is called with this process's valid pid and SIGKILL.
    let result = unsafe { kill(std::process::id() as i32, SIGKILL) };
    if result != 0 {
        std::process::abort();
    }
    // Signal delivery may occur just after `kill(2)` returns. Do not race it
    // with an immediate `abort()`, which would make a reached crash point
    // intermittently appear as SIGABRT on macOS.
    loop {
        std::thread::park();
    }
}

#[cfg(not(unix))]
fn kill_process() -> ! {
    std::process::abort()
}

#[cfg(unix)]
unsafe extern "C" {
    fn kill(pid: i32, signal: i32) -> i32;
}
