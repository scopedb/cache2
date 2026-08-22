//! Bounded NVMe cache engines: a circular RegionLog and an inclusive Hybrid
//! composition for mixed small/large objects.
//!
//! Version 1.1 is the source-complete large-scale single-device candidate. It
//! combines bounded queues and aligned buffers, a compact index with up to
//! 268,435,456 slots and 4,096 shards, concurrent reads, sync and Linux
//! `io_uring` engines, optional `O_DIRECT`, up to eight append lanes, and a
//! compatible Format V1 checkpoint tail.
//!
//! A standalone [`DiskCache`] clean reopen validates Region Headers and restores
//! the paired checkpoint without record replay. A dirty reopen restores the previous clean baseline,
//! invalidates changed Region incarnations, and scans only changed tails.
//! [`RecoveryMode::Blocking`] completes that bounded work before `open`
//! returns; [`RecoveryMode::MissOnly`] temporarily serves misses and rejects
//! mutations, then atomically publishes normal service after recovery has
//! written a new clean checkpoint. Standalone periodic checkpoints are coalesced
//! after an adaptive admitted-byte threshold by default; a zero interval disables
//! only periodic publication. Checkpoint payload I/O is streamed in fixed
//! 256 KiB windows, and recovery workspaces remain bounded. [`CacheStats`]
//! exposes checkpoint and recovery progress counters. Checkpoint payload v4
//! persists Active Region lane identity and exact compact-index placement while
//! readers remain compatible with v1/v2/v3 payloads.
//!
//! Bounded SecondHit admission, namespace quotas, daily host-write accounting,
//! Region validity and one-shot second-chance reinsertion provide the M7 policy
//! layer. [`MetricsSnapshot`] exports fixed-cardinality OpenMetrics, request
//! latency/error classes, and structured lifecycle events. Configuration and
//! startup diagnostics, health snapshots, an origin-fill limiter, plus the
//! `cachectl` inspect/verify/diagnose/format/reset workflows provide the M8
//! operations surface.
//!
//! The Format V1 Superblock, Region Header, and base record encodings remain
//! compatible. An unusable dirty baseline is disposable cache state and is
//! safely reopened empty. Target-NVMe performance, TB-scale recovery, DWPD,
//! long-soak, canary, and real-power-loss sign-offs remain deployment tests;
//! source completion does not claim results for unrelated hardware.
//!
//! [`HybridCache`] combines a fixed-capacity sharded memory LRU with a
//! [`BucketCache`] small-object tier and the RegionLog [`DiskCache`] large-object
//! tier. Its default is bounded, memory-first [`HybridWriteMode::WriteBack`];
//! [`HybridWriteMode::WriteThrough`] remains available explicitly. Open persists
//! one dirty-session fence, after which steady-state mutations allocate versions
//! in memory without a per-key journal write or durability sync. `flush` drains
//! dirty L1 values, publishes clean lower/global checkpoints, and re-arms the
//! session fence; `close` publishes the final clean boundary. An unclean session
//! may cold-start the disposable lower tiers.
//!
//! L1 values use shared immutable storage and [`HybridCache::get_handle`] avoids
//! payload copies on L1 hits; compatibility `get`/`lookup` APIs copy only after
//! releasing the memory-shard lock. Dirty victims detach through a bounded,
//! exact-key pending directory that masks any older lower value and makes a
//! same-key mutation wait without holding the coarse ordering lock. Disposable
//! lower-absent work uses at most 75% of the executor and may be dropped to a
//! miss; lower-candidate updates use the complete bounded budget, and admission
//! failure keeps the victim resident and rejects the incoming cache put instead
//! of doing foreground device I/O. Both disk tiers provide bounded
//! sync/`io_uring` runtimes and aligned optional `O_DIRECT` without changing
//! their respective Format V1 encodings.

mod async_cache;
mod async_hybrid;
mod bucket_engine;
mod cache;
mod checkpoint;
mod checksum;
mod diagnostics;
mod format;
mod hybrid;
#[cfg(test)]
mod hybrid_crash;
mod hybrid_journal;
mod hybrid_manifest;
mod hybrid_metrics;
mod index;
mod io_backend;
mod io_engine;
mod management;
mod memory_engine;
mod metrics;
mod miss_guard;
mod pending_write;
mod policy;
mod resources;
mod write_back;
mod write_batch;

pub use async_cache::{
    AsyncCloseFuture, AsyncDiskCache, AsyncQueueStats, AsyncRequestOptions, CacheFuture,
    CancelOutcome,
};
pub use async_hybrid::{AsyncHybridCache, AsyncHybridCloseFuture, AsyncHybridCloseStats};
pub use bucket_engine::{
    BucketCache, BucketCacheConfig, BucketCacheStats, BucketConfigDiagnostics,
};
pub use cache::{
    CacheConfig, CacheError, CacheStats, CacheStatus, DiskCache, IoEngineKind, IoMode, PutOptions,
    PutOutcome, ReclaimMode, RecoveryMode, RegionStats, RejectReason, RemoveOutcome, Result,
};
pub use diagnostics::{ConfigDiagnostics, HealthSnapshot, StartupDiagnostics};
pub use hybrid::{
    CacheTier, HybridCache, HybridCacheConfig, HybridCacheStats, HybridConfigDiagnostics,
    HybridHealthSnapshot, HybridJournalGroupCommitStats, HybridLookupOutcome,
    HybridMetricsSnapshot, HybridMissKind, HybridOpenStats, HybridPolicySnapshot,
    HybridValueHandle, HybridWriteBackStats, HybridWriteMode,
};
pub use management::{
    BucketFileReport, CacheFileKind, CheckpointDirectoryState, CheckpointSlotState,
    CheckpointSlotSummary, CheckpointSummary, HybridInspectReport, HybridManifestFileReport,
    HybridVerifyReport, InspectReport, MAX_REPORTED_VERIFY_ISSUES, ManagementError,
    ManagementResult, RegionSummary, ReopenDisposition, SuperblockState, SuperblockSummary,
    VerifyComponent, VerifyIssue, VerifyReport, inspect_cache_file, inspect_hybrid_cache_files,
    verify_cache_file, verify_hybrid_cache_files,
};
pub use metrics::{
    CacheErrorClass, CacheOperation, LATENCY_BUCKET_COUNT, LATENCY_BUCKET_UPPER_US,
    LatencyHistogramSnapshot, MetricsSnapshot, OperationMetricsSnapshot, RequestResultClass,
    StateChangeReason, StateTransition,
};
pub use miss_guard::{OriginFillConfig, OriginFillPermit, OriginFillRejectReason, OriginFillStats};
pub use policy::{
    AdmissionMode, DeviceHealthPolicy, HostWriteKind, HostWriteSnapshot, NamespaceConfig,
    NamespaceId, NamespaceSnapshot, NvmeHealthSample, NvmeHealthStats,
};
pub use resources::{BackpressurePolicy, OverloadReason};
