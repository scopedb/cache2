#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupMode {
    Cold,
    Warm,
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
/// accounting. Counters are process-local and reset on every open. Concurrent
/// updates may appear across fields at slightly different instants.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSnapshot {
    /// Process-local identity for this set of cumulative counters. This changes
    /// whenever a cache is opened and is not intended to be exported as a
    /// metric label.
    pub metrics_epoch: u64,
    pub health: CacheHealth,
    pub statistics_enabled: bool,
    pub puts: u64,
    pub deletes: u64,
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
    pub write_rejections: u64,
    pub io_failures: u64,
    pub region_rotations: u64,
    pub reclaim: CacheReclaimSnapshot,
    pub managed_memory_bytes: usize,
    pub managed_memory_peak_bytes: usize,
    pub managed_memory_limit_bytes: usize,
    pub logical_disk_peak_bytes: u64,
    pub io: CacheIoSnapshot,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoSnapshot {
    pub read: CacheIoDirectionSnapshot,
    pub write: CacheIoDirectionSnapshot,
}

/// Cumulative engine-request and runtime-file-I/O statistics for one
/// direction. Request counters describe logical engine requests. Path counters
/// describe positive runtime file operations and are not physical device I/O.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoDirectionSnapshot {
    /// Requests accepted by the engine command queue.
    pub requests_submitted: u64,
    /// Mutually exclusive terminal outcomes for submitted requests.
    pub requests_succeeded: u64,
    pub requests_cancelled: u64,
    pub requests_failed: u64,
    pub requests_in_flight: u64,
    /// Sum of the participating engines' high-water marks. This is a
    /// conservative bound when a direction spans multiple engines, not a
    /// synchronized process-wide peak.
    pub requests_in_flight_peak: u64,
    /// Nanoseconds spent acquiring an engine slot. Pre-reserved nonwaiting
    /// reads do not contribute to this counter.
    pub slot_wait_ns: u64,
    /// Nanoseconds from slot reservation to terminal completion for submitted
    /// requests.
    pub request_time_ns: u64,
    pub buffered: CacheIoPathSnapshot,
    pub direct: CacheIoPathSnapshot,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoPathSnapshot {
    /// Positive runtime file operations. Short I/O can produce more than one
    /// operation for one engine request.
    pub operations: u64,
    pub bytes: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheReclaimSnapshot {
    pub regions: u64,
    pub bytes_read: u64,
    pub records_scanned: u64,
    pub index_entries_removed: u64,
    /// Referenced records physically rewritten during reclaim.
    pub reinsert_records: u64,
    /// Encoded record bytes rewritten, excluding batch padding.
    pub reinsert_bytes: u64,
    /// Referenced records dropped after validation failure or budget/staging pressure.
    pub reinsert_skipped: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheL1Snapshot {
    pub entry_capacity: usize,
    pub resident_entries: usize,
    pub resident_bytes: usize,
    pub retained_bytes: usize,
    pub metadata_bytes: usize,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIndexSnapshot {
    pub slot_capacity: u64,
    pub physical_value_slots: u64,
    pub empty_slots: u64,
    pub relocations: u64,
    pub overflow_evictions: u64,
    pub conditional_remove_misses: u64,
    /// Completed reinsert writes whose old index address had already changed.
    pub conditional_replace_misses: u64,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionSnapshot {
    pub capacity_bytes: u64,
    pub append_shard_count: u32,
    pub active_region_count: u32,
    pub free_region_count: u32,
    pub sealed_region_count: u32,
    pub reclaiming_region_count: u32,
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
    pub write_buffer_rejections: u64,
    pub l1: CacheL1Snapshot,
    pub index: CacheIndexSnapshot,
    pub region: RegionSnapshot,
}
