use crate::region_layout::RegionSetId;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupMode {
    Fresh,
    ColdAfterUncleanShutdown,
    Warm,
    ColdAfterRejectedImage,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheHealth {
    Running,
    Draining,
    MissOnly,
    Failed,
}

/// Lock-free point-in-time operational counters and cache-owned resource
/// accounting. Counters are process-local and reset on every open.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSnapshot {
    pub health: CacheHealth,
    pub statistics_enabled: bool,
    pub puts: u64,
    pub written_bytes: u64,
    pub l1_hits: u64,
    pub l1_misses: u64,
    pub l2_hits: u64,
    pub l2_misses: u64,
    pub l2_read_memory_misses: u64,
    pub l2_read_busy_misses: u64,
    pub served_bytes: u64,
    pub l1_promotions: u64,
    pub l1_evictions: u64,
    pub l1_bypasses: u64,
    pub l1_admission_rejections: u64,
    pub write_rejections: u64,
    pub io_failures: u64,
    pub region_rotations: u64,
    pub managed_memory_bytes: usize,
    pub managed_memory_peak_bytes: usize,
    pub managed_memory_limit_bytes: usize,
    pub logical_disk_peak_bytes: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheWriteSnapshot {
    pub write_requests_in_flight: u64,
    pub write_requests_peak: u64,
    pub write_gate_rejections: u64,
    pub write_gate_wait_ns: u64,
    pub write_buffer_rejections: u64,
    pub write_buffer_wait_ns: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub cancel_requested: u64,
    pub cancelled: u64,
    pub errors: u64,
    pub requests_in_flight: u64,
    pub requests_in_flight_peak: u64,
    pub slot_wait_ns: u64,
    pub completion_ns: u64,
    pub direct_operations: u64,
    pub direct_bytes: u64,
    pub buffered_operations: u64,
    pub buffered_bytes: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionSetSnapshot {
    pub id: RegionSetId,
    pub capacity_bytes: u64,
    pub append_shard_count: u32,
    pub active_region_count: u32,
    pub free_region_count: u32,
    pub sealed_region_count: u32,
    pub live_record_count: u64,
    pub live_bytes: u64,
    pub physical_record_count: u64,
    pub physical_bytes: u64,
    pub rotations: u64,
}

/// On-demand operational detail. Producing this value briefly locks and scans
/// Region metadata.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetailedCacheSnapshot {
    pub summary: CacheSnapshot,
    pub writes: CacheWriteSnapshot,
    pub io: CacheIoSnapshot,
    pub region_sets: Box<[RegionSetSnapshot]>,
}
