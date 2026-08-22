//! Stable startup and health diagnostics intended for deployment tooling.

use std::path::PathBuf;

use crate::cache::{CacheStatus, IoEngineKind, IoMode, RecoveryMode};
use crate::miss_guard::OriginFillStats;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigDiagnostics {
    pub path: PathBuf,
    pub requested_capacity_bytes: u64,
    pub data_file_len_bytes: u64,
    pub region_size_bytes: u64,
    pub region_count: u32,
    pub index_slots: usize,
    pub append_lanes: usize,
    pub maximum_record_bytes: u32,
    pub memory_budget_bytes: u64,
    /// Fixed engine-owned bytes charged during validation/open. Aligned data
    /// buffers grow lazily and are independently capped by `memory_budget_bytes`.
    pub planned_memory_bytes: u64,
    pub read_submission_depth: usize,
    pub write_submission_depth: usize,
    pub io_queue_depth: usize,
    pub io_engine: IoEngineKind,
    pub io_mode: IoMode,
    pub recovery_mode: RecoveryMode,
    pub checkpoint_slot_bytes: u64,
    /// Peak open-time workspace used to accumulate per-Region and
    /// per-namespace live bytes during a single streaming checkpoint load.
    pub checkpoint_accounting_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupDiagnostics {
    pub path: PathBuf,
    pub status: CacheStatus,
    pub recovered_entries: u64,
    pub checkpoint_loaded: bool,
    pub checkpoint_fallbacks: u64,
    pub recovery_regions_scanned: u64,
    pub recovery_records_scanned: u64,
    pub recovery_elapsed_us: u64,
    pub recovery_in_progress: bool,
    pub io_uring_active: bool,
    pub direct_io_active: bool,
    pub configured_io_engine: IoEngineKind,
    pub configured_io_mode: IoMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HealthSnapshot {
    pub status: CacheStatus,
    pub ready: bool,
    pub recovery_in_progress: bool,
    pub io_errors: u64,
    pub checkpoint_errors: u64,
    pub corrupt_records: u64,
    pub reclaim_backlog_rejections: u64,
    pub nvme_health_critical: bool,
    pub origin_fills: OriginFillStats,
}

impl HealthSnapshot {
    /// Ready means normal cache traffic is available. Device-health advisory
    /// state remains separately visible and does not redefine lifecycle.
    pub const fn is_ready(self) -> bool {
        self.ready
    }

    pub const fn is_degraded(self) -> bool {
        !self.ready
            || self.recovery_in_progress
            || self.nvme_health_critical
            || self.io_errors != 0
            || self.checkpoint_errors != 0
    }
}
