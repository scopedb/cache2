// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

/// How the current cache instance obtained its initial state.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupMode {
    /// The instance started empty.
    Cold,
    /// The instance accepted a clean recovery image from the previous open.
    Warm,
}

/// Current availability of the cache instance.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheHealth {
    /// Reads and mutations are operating normally.
    Running,
    /// New mutations are fenced while accepted work completes.
    Draining,
    /// Reads fail open as misses and mutations report the terminal failure.
    MissOnly,
    /// Lifecycle or worker failure prevents normal cache operation.
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
    /// Current cache availability.
    pub health: CacheHealth,
    /// Whether optional cumulative activity and I/O counters are enabled.
    pub statistics_enabled: bool,
    /// Accepted `put` and `put_l2` operations.
    pub puts: u64,
    /// Accepted delete operations.
    pub deletes: u64,
    /// Input value bytes accepted by `put` and `put_l2`.
    pub written_bytes: u64,
    /// Gets satisfied by an L1-resident value.
    pub l1_hits: u64,
    /// Gets not satisfied by L1. These overlap the L2 outcome counters.
    pub l1_misses: u64,
    /// Gets satisfied by a validated Region value.
    pub l2_hits: u64,
    /// Gets not satisfied by L2 after an L1 miss.
    pub l2_misses: u64,
    /// Immediate-mode L2 misses caused by unavailable managed read memory.
    pub l2_read_memory_misses: u64,
    /// Immediate-mode L2 misses caused by read-lane or engine pressure.
    pub l2_read_busy_misses: u64,
    /// L2 candidates rejected after bounded read waiting was enabled and its
    /// queue, memory, or deadline bound was exhausted.
    pub l2_read_overloads: u64,
    /// Nanoseconds spent in the optional bounded L2 read execution queue.
    pub l2_read_wait_ns: u64,
    /// Value bytes returned by successful L1 and L2 gets.
    pub served_bytes: u64,
    /// Validated L2 values successfully retained in L1 before return.
    pub l1_promotions: u64,
    /// Resident entries evicted by L1 admission.
    pub l1_evictions: u64,
    /// L1 admission attempts that did not retain the supplied value.
    pub l1_bypasses: u64,
    /// Foreground mutations rejected by bounded admission or staging.
    pub write_rejections: u64,
    /// Runtime I/O or persistent-state failures observed by this instance.
    pub io_failures: u64,
    /// Completed transitions to a new Active Region.
    pub region_rotations: u64,
    /// Cumulative Region reclaim activity.
    pub reclaim: CacheReclaimSnapshot,
    /// Bytes currently charged to the managed-memory limit.
    pub managed_memory_bytes: usize,
    /// Highest managed-memory charge observed during this open.
    pub managed_memory_peak_bytes: usize,
    /// Configured managed-memory limit in bytes.
    pub managed_memory_limit_bytes: usize,
    /// Maximum logical bytes owned by the cache's data and sidecar files.
    pub logical_disk_peak_bytes: u64,
    /// Cumulative read and write I/O statistics.
    pub io: CacheIoSnapshot,
}

/// Cumulative I/O statistics split by direction.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoSnapshot {
    /// Read-engine and runtime read-file activity.
    pub read: CacheIoDirectionSnapshot,
    /// Write-engine and runtime write-file activity.
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
    /// Submitted requests that completed through cancellation.
    pub requests_cancelled: u64,
    /// Submitted requests that completed with an error.
    pub requests_failed: u64,
    /// Requests currently holding an engine slot.
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
    /// Runtime operations issued through buffered file descriptors.
    pub buffered: CacheIoPathSnapshot,
    /// Runtime operations issued through direct-I/O file descriptors.
    pub direct: CacheIoPathSnapshot,
}

/// Cumulative runtime file activity for one I/O path.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoPathSnapshot {
    /// Positive runtime file operations. Short I/O can produce more than one
    /// operation for one engine request.
    pub operations: u64,
    /// Bytes transferred by positive runtime file operations.
    pub bytes: u64,
}

/// Cumulative work performed by Region reclamation.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheReclaimSnapshot {
    /// Source Regions whose reclaim pass completed.
    pub regions: u64,
    /// Source Region bytes read during reclaim.
    pub bytes_read: u64,
    /// Encoded records examined during reclaim.
    pub records_scanned: u64,
    /// Current index mappings removed as cold during reclaim.
    pub index_entries_removed: u64,
    /// Referenced records physically rewritten during reclaim.
    pub reinsert_records: u64,
    /// Encoded record bytes rewritten, excluding batch padding.
    pub reinsert_bytes: u64,
    /// Referenced records dropped after validation failure or budget/staging pressure.
    pub reinsert_skipped: u64,
    /// Valid referenced records dropped specifically because the byte budget was exhausted.
    pub reinsert_budget_skipped: u64,
}

/// Current L1 occupancy and fixed metadata footprint.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheL1Snapshot {
    /// Fixed number of entry slots across all L1 shards.
    pub entry_capacity: usize,
    /// Entry slots currently containing resident values.
    pub resident_entries: usize,
    /// Charged key/value bytes owned by resident entries.
    pub resident_bytes: usize,
    /// Charged bytes kept alive by returned values after resident eviction.
    pub retained_bytes: usize,
    /// Fixed bytes allocated for L1 entries, directories, and policy state.
    pub metadata_bytes: usize,
}

/// Current L2 index occupancy and cumulative mutation diagnostics.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIndexSnapshot {
    /// Fixed number of physical index slots.
    pub slot_capacity: u64,
    /// Slots currently containing a location mapping.
    pub physical_value_slots: u64,
    /// Slots currently empty.
    pub empty_slots: u64,
    /// Slot relocations performed while making room for mappings.
    pub relocations: u64,
    /// Mappings replaced after bounded relocation could not find an empty slot.
    pub overflow_evictions: u64,
    /// Conditional removals skipped because the expected mapping had changed.
    pub conditional_remove_misses: u64,
    /// Completed reinsert writes whose old index address had already changed.
    pub conditional_replace_misses: u64,
}

/// Current Region-store geometry, queue membership, and physical occupancy.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionSnapshot {
    /// Total bytes in the fixed Region extent.
    pub capacity_bytes: u64,
    /// Number of append shards configured for this open.
    pub append_shard_count: u32,
    /// Regions currently receiving writes, one per append shard.
    pub active_region_count: u32,
    /// Empty Regions immediately available for rotation.
    pub free_region_count: u32,
    /// Completed Regions waiting in reclaim order.
    pub sealed_region_count: u32,
    /// Regions currently owned by reclaim workers.
    pub reclaiming_region_count: u32,
    /// Encoded records physically present, including stale records.
    pub physical_record_count: u64,
    /// Completed encoded bytes physically present, including stale records.
    pub physical_bytes: u64,
    /// Region rotations recorded by the Region manager.
    pub rotations: u64,
}

/// On-demand operational detail. Producing this value briefly reads every L1
/// shard and index partition and scans Region metadata. It does not scan index
/// slots or Region data.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetailedCacheSnapshot {
    /// Lock-free summary sampled for this diagnostic.
    pub summary: CacheSnapshot,
    /// Mutations rejected specifically because an append buffer needed progress.
    pub write_buffer_rejections: u64,
    /// L1 occupancy and metadata detail.
    pub l1: CacheL1Snapshot,
    /// L2 index occupancy and mutation detail.
    pub index: CacheIndexSnapshot,
    /// Region geometry, queues, and physical occupancy.
    pub region: RegionSnapshot,
}
