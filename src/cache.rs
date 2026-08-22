use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::async_cache::{
    AsyncDiskCache, AsyncFailure, AsyncInner, TaskContext, async_read_worker_count,
};
use crate::checkpoint::{
    CHECKPOINT_DIRECTORY_SIZE, CHECKPOINT_INDEX_ENTRY_SIZE, CHECKPOINT_PAGE_SIZE,
    CHECKPOINT_REGION_SNAPSHOT_SIZE, CHECKPOINT_SLOT_COUNT, CHECKPOINT_SLOT_HEADER_SIZE,
    CheckpointCodecError, CheckpointDirectory, CheckpointIndexEntry, CheckpointPayloadDecoder,
    CheckpointPayloadEncoder, CheckpointRegionSnapshot, CheckpointSlotHeader,
    CheckpointSnapshotMeta, decode_checkpoint_index_entry, padded_payload_len, required_slot_size,
};
use crate::checksum::{Crc32c, crc32c};
use crate::diagnostics::{ConfigDiagnostics, HealthSnapshot, StartupDiagnostics};
use crate::format::{
    MAX_KEY_SIZE, MAX_VALUE_SIZE, RECORD_HEADER_SIZE, REGION_HEADER_SIZE, RecordCodec,
    RecordHeader, RecordKind, RegionHeader, RegionState, SUPERBLOCK_A_OFFSET, SUPERBLOCK_AREA_SIZE,
    SUPERBLOCK_B_OFFSET, SUPERBLOCK_COUNT, SUPERBLOCK_SIZE, Superblock, SuperblockProbe,
};
use crate::index::{
    ApplyResult, INDEX_FLAG_SECOND_CHANCE_PENDING, INDEX_FLAG_SECOND_CHANCE_USED, IndexEntry,
    IndexSnapshotEntry, MAX_INDEX_SLOTS, MAX_RECORD_LEN, MAX_REGION_OFFSET, PackedLocation,
    RegionGeneration, ShardedIndex,
};
use crate::io_backend::{
    DIRECT_IO_ALIGNMENT, DirectIoMode, FileBackend, IoBackend, RuntimeFileSet, SyncMode, SyncPoint,
    WritePoint, read_exact_at, write_all_at,
};
#[cfg(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use crate::io_engine::UringIoEngine;
use crate::io_engine::{
    BackendIoEngine, DEFAULT_IO_QUEUE_DEPTH, EngineKind, IoBuffer, IoEngine, MAX_IO_QUEUE_DEPTH,
    OperationKind,
};
use crate::metrics::{
    CacheErrorClass, CacheOperation, MetricsSnapshot, RequestResultClass, RequestTelemetry,
    StateChangeReason,
};
use crate::miss_guard::{
    OriginFillConfig, OriginFillLimiter, OriginFillPermit, OriginFillRejectReason, OriginFillStats,
};
use crate::policy::{
    AdmissionDecision, AdmissionMode, AdmissionPolicy, DailyWriteReservation, DeviceHealthPolicy,
    HostWriteKind, HostWriteSnapshot, HostWriteTracker, NamespaceCapacityReservation,
    NamespaceConfig, NamespaceController, NamespaceId, NamespaceRejectReason, NamespaceSnapshot,
    NamespaceUsage, NamespaceWriteReservation, NvmeHealthSample, NvmeHealthStats, PolicyController,
};
use crate::resources::{
    BackpressurePolicy, BufferLease, DEFAULT_MEMORY_BUDGET_BYTES, DEFAULT_READ_QUEUE_DEPTH,
    DEFAULT_WRITE_QUEUE_DEPTH, DataResources, OverloadReason, QueuePermit, RemoveResources,
    ResourceController, ResourceLimits, WriteBudgetReservation, aligned_buffer_capacity,
};
use crate::write_batch::{BatchPlan, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, plan_batch};

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_MAX_KEY_SIZE: usize = 64 * 1024;
const DEFAULT_MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;
const DEFAULT_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const MIN_REGIONS: u32 = 2;
const DATA_OFFSET: u64 = SUPERBLOCK_AREA_SIZE;
const RESOURCE_OVERHEAD_BYTES: usize = 64 * 1024;
const APPEND_QUEUE_SLOT_OVERHEAD_BYTES: usize = 16;
const APPEND_COMPLETION_OVERHEAD_BYTES: usize = 1024;
const KEY_ORDERING_SHARDS: usize = 256;
const IO_ENGINE_SLOT_OVERHEAD_BYTES: usize = 2048;
const ASYNC_TASK_OVERHEAD_BYTES: usize = 1024;
const ASYNC_MUTATION_WORKERS_PER_LANE: usize = 8;
const MAX_ASYNC_MUTATION_WORKERS: usize = 64;
const ASYNC_CONTROL_QUEUE_RESERVE: usize = 2;
const DEFAULT_APPEND_LANES: usize = 1;
const APPEND_COALESCE_DELAY: Duration = Duration::from_micros(200);
pub(crate) const MAX_APPEND_LANES: usize = 8;
const MAX_DATA_BUFFER_SLOTS: usize = 128;
const DEFAULT_CHECKPOINT_INTERVAL_BYTES: u64 = 256 * 1024 * 1024;
const DEFAULT_CHECKPOINT_REWRITE_RATIO: u64 = 16;
const RECLAIM_SCAN_CHUNK_BYTES: usize = 128 * 1024;
// Checkpoint entries are tiny and a 4 KiB write per page turns a 100M-entry
// snapshot into roughly one million syscalls. Keep the on-disk page/alignment
// at 4 KiB, but aggregate buffered payload I/O into a fixed 256 KiB window.
const CHECKPOINT_IO_CHUNK_BYTES: usize = 256 * 1024;
const NAMESPACE_KEY_PREFIX_SIZE: usize = std::mem::size_of::<u32>();
const NAMESPACE_HASH_DOMAIN: &[u8] = b"cache-rs/ns/v1\0";
const SECOND_CHANCE_QUEUE_DEPTH: usize = 64;
const SECOND_CHANCE_MAX_RECORD_BYTES: u32 = 128 * 1024;
const SECOND_CHANCE_REGION_FRACTION: u64 = 4;
const RECLAIM_TRIGGER_NUMERATOR: u64 = 3;
const RECLAIM_TRIGGER_DENOMINATOR: u64 = 4;
const POLICY_NAMESPACE_SLOT_OVERHEAD_BYTES: usize = 256;
const SECOND_CHANCE_QUEUE_SLOT_OVERHEAD_BYTES: usize = 128;

pub type Result<T> = std::result::Result<T, CacheError>;

#[non_exhaustive]
#[derive(Debug)]
pub enum CacheError {
    Io(io::Error),
    InvalidConfig(String),
    CorruptMetadata(&'static str),
    Locked,
    Closed,
    Poisoned,
    Cancelled,
    TimedOut,
    Overloaded(OverloadReason),
    ReclaimBacklog,
}

/// Observable lifecycle and failure state of a cache instance.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CacheStatus {
    Healthy,
    MissOnly,
    Poisoned,
    Closed,
}

/// Runtime I/O implementation used after open and recovery complete.
///
/// `Sync` remains the v0.6 default and reference implementation. `Auto` uses
/// `io_uring` when the running Linux kernel exposes the required operations and
/// otherwise falls back to `Sync`. `IoUring` requests that backend explicitly
/// and returns a configuration error when it is unavailable.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoEngineKind {
    #[default]
    Sync,
    Auto,
    IoUring,
}

/// File-I/O policy for the runtime data path.
///
/// Metadata, recovery, locking, and durability barriers always use the
/// buffered control descriptor. `Direct` requires Linux `O_DIRECT` and never
/// falls back after an aligned direct-I/O error; `Auto` disables direct I/O
/// when the filesystem rejects it. Both modes use the buffered descriptor for
/// legacy or short-completion remainders that are not 4 KiB aligned, so Format
/// V1 remains interchangeable with `Buffered` mode.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoMode {
    #[default]
    Buffered,
    Auto,
    Direct,
}

/// Startup behavior when an unclean cache has a usable index checkpoint.
///
/// `Blocking` completes the bounded incremental scan before `open` returns.
/// `MissOnly` returns immediately after the checkpoint has been validated,
/// serves reads as misses, rejects mutations, and opens normal traffic only
/// after the background recovery has published a new clean checkpoint.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum RecoveryMode {
    #[default]
    Blocking,
    MissOnly,
}

/// Region-reclaim behavior.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReclaimMode {
    /// Strict creation-order FIFO, retained as the compatibility baseline.
    #[default]
    Fifo,
    /// Give a verified hot value one bounded asynchronous reinsertion.
    SecondChance,
}

impl From<IoMode> for DirectIoMode {
    fn from(mode: IoMode) -> Self {
        match mode {
            IoMode::Buffered => Self::Buffered,
            IoMode::Auto => Self::Auto,
            IoMode::Direct => Self::Required,
        }
    }
}

impl fmt::Display for CacheError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "disk cache I/O error: {err}"),
            Self::InvalidConfig(message) => write!(f, "invalid cache config: {message}"),
            Self::CorruptMetadata(message) => write!(f, "corrupt cache metadata: {message}"),
            Self::Locked => write!(f, "cache file is already open by another instance"),
            Self::Closed => write!(f, "cache is closed"),
            Self::Poisoned => write!(f, "cache state is poisoned"),
            Self::Cancelled => write!(f, "cache request was cancelled"),
            Self::TimedOut => write!(f, "cache operation timed out while waiting for completion"),
            Self::Overloaded(reason) => write!(f, "disk cache overloaded: {reason}"),
            Self::ReclaimBacklog => f.write_str("background region reclaim is catching up"),
        }
    }
}

impl std::error::Error for CacheError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for CacheError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Static cache configuration.
///
/// The effective region count derived from `capacity`, plus `region_size` and
/// `hash_seed`, establish persistent layout and identity and must match when
/// the file is reopened. `append_lanes` determines the number of Active Region
/// headers in a clean checkpoint and must also match on reopen. Index,
/// object-size, memory, queue, backpressure, and write-budget settings are
/// per-open runtime policies; object-size limits apply only to new `put`
/// admission, never to reading or removing an older record.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    path: PathBuf,
    capacity: u64,
    region_size: u64,
    index_slots: usize,
    max_key_size: usize,
    max_value_size: usize,
    hash_seed: u64,
    memory_budget_bytes: usize,
    read_queue_depth: usize,
    write_queue_depth: usize,
    append_lanes: usize,
    io_mode: IoMode,
    io_engine: IoEngineKind,
    io_queue_depth: usize,
    backpressure: BackpressurePolicy,
    write_budget_bytes_per_second: Option<u64>,
    checkpoint_interval_bytes: u64,
    checkpoint_interval_explicit: bool,
    recovery_mode: RecoveryMode,
    admission_mode: AdmissionMode,
    reclaim_mode: ReclaimMode,
    namespace_configs: Vec<NamespaceConfig>,
    daily_host_write_budget_bytes: Option<u64>,
    daily_host_write_baseline: Option<(u64, u64)>,
    device_health_policy: DeviceHealthPolicy,
    origin_fill_config: Option<OriginFillConfig>,
}

impl CacheConfig {
    pub fn new(path: impl AsRef<Path>, capacity: u64) -> Self {
        let estimated = capacity / 4096;
        let slots = estimated
            .saturating_mul(5)
            .saturating_div(4)
            .clamp(1024, 16 * 1024 * 1024) as usize;
        Self {
            path: path.as_ref().to_path_buf(),
            capacity,
            region_size: DEFAULT_REGION_SIZE,
            index_slots: slots,
            max_key_size: DEFAULT_MAX_KEY_SIZE,
            max_value_size: DEFAULT_MAX_VALUE_SIZE,
            hash_seed: DEFAULT_HASH_SEED,
            memory_budget_bytes: DEFAULT_MEMORY_BUDGET_BYTES,
            read_queue_depth: DEFAULT_READ_QUEUE_DEPTH,
            write_queue_depth: DEFAULT_WRITE_QUEUE_DEPTH,
            append_lanes: DEFAULT_APPEND_LANES,
            io_mode: IoMode::Buffered,
            io_engine: IoEngineKind::Sync,
            io_queue_depth: DEFAULT_IO_QUEUE_DEPTH,
            backpressure: BackpressurePolicy::Reject,
            write_budget_bytes_per_second: None,
            checkpoint_interval_bytes: DEFAULT_CHECKPOINT_INTERVAL_BYTES,
            checkpoint_interval_explicit: false,
            recovery_mode: RecoveryMode::Blocking,
            admission_mode: AdmissionMode::Always,
            reclaim_mode: ReclaimMode::Fifo,
            namespace_configs: Vec::new(),
            daily_host_write_budget_bytes: None,
            daily_host_write_baseline: None,
            device_health_policy: DeviceHealthPolicy::ObserveOnly,
            origin_fill_config: None,
        }
    }

    /// Dedicated file used by this disk tier.
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn maximum_key_size(&self) -> usize {
        self.max_key_size
    }

    pub(crate) fn maximum_value_size(&self) -> usize {
        self.max_value_size
    }

    /// Return whether this Region configuration contains a policy that must be
    /// owned by a composing driver instead of silently applied a second time.
    #[allow(dead_code)]
    pub(crate) fn has_driver_policy_settings(&self) -> bool {
        self.write_budget_bytes_per_second.is_some()
            || self.admission_mode != AdmissionMode::Always
            || !self.namespace_configs.is_empty()
            || self.daily_host_write_budget_bytes.is_some()
            || self.daily_host_write_baseline.is_some()
            || self.device_health_policy != DeviceHealthPolicy::ObserveOnly
    }

    pub(crate) fn configured_recovery_mode(&self) -> RecoveryMode {
        self.recovery_mode
    }

    pub fn with_region_size(mut self, bytes: u64) -> Self {
        self.region_size = bytes;
        self
    }

    pub fn with_index_slots(mut self, slots: usize) -> Self {
        self.index_slots = slots;
        self
    }

    /// Size the fixed in-memory index for an expected live-entry population.
    ///
    /// The resulting slot count targets at most 80% occupancy (`entries *
    /// 1.25`). The memory budget must still cover 32 bytes per slot plus Region
    /// metadata; validation reports an error before allocating the index when
    /// it does not.
    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.index_slots = entries
            .saturating_mul(5)
            .saturating_add(3)
            .checked_div(4)
            .unwrap_or(usize::MAX)
            .max(8);
        self
    }

    pub fn with_max_key_size(mut self, bytes: usize) -> Self {
        self.max_key_size = bytes;
        self
    }

    pub fn with_max_value_size(mut self, bytes: usize) -> Self {
        self.max_value_size = bytes;
        self
    }

    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }

    /// Set the engine-owned logical heap budget.
    ///
    /// The budget covers the index, region/recovery metadata, aligned scratch
    /// buffers, I/O queue bookkeeping, and the bounded async executor's copied
    /// request inputs. Caller-owned inputs, returned `Vec`s, thread stacks, and
    /// allocator bookkeeping are outside this accounting scope.
    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget_bytes = bytes;
        self
    }

    /// Set independent read and write admission depths.
    ///
    /// Both depths must be in `1..=65_536`. Control operations use separate
    /// per-append-lane reserves so ordinary write pressure cannot consume them.
    pub fn with_submission_queue_depths(mut self, read: usize, write: usize) -> Self {
        self.read_queue_depth = read;
        self.write_queue_depth = write;
        self
    }

    /// Set the number of independent append lanes.
    ///
    /// Each lane owns one Active Region and is selected by key hash. The small
    /// hard limit keeps region metadata coordination and memory accounting
    /// simple while allowing up to eight NVMe writes to overlap. At least one
    /// additional non-Active region is required for rotation, and the lane
    /// count must match an existing clean cache.
    pub fn with_append_lanes(mut self, lanes: usize) -> Self {
        self.append_lanes = lanes;
        self
    }

    /// Select buffered, opportunistic direct, or required-capability direct
    /// file I/O. This setting does not change Format V1 and may change on
    /// reopen; unaligned legacy records retain a buffered compatibility path.
    pub fn with_io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    /// Select the runtime I/O implementation. This is a per-open setting and
    /// does not change Format V1 or the bytes stored on disk.
    pub fn with_io_engine(mut self, engine: IoEngineKind) -> Self {
        self.io_engine = engine;
        self
    }

    /// Set the runtime device queue depth.
    ///
    /// Values must be in `1..=4096`. The hard limit bounds driver bookkeeping
    /// even if a kernel or device advertises a larger queue.
    pub fn with_io_queue_depth(mut self, depth: usize) -> Self {
        self.io_queue_depth = depth;
        self
    }

    /// Select how callers react when an admission gate or buffer pool is full.
    /// Timeout durations are limited to 24 hours.
    pub fn with_backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure = policy;
        self
    }

    /// Limit admitted `put` traffic to bytes per second with a one-second burst.
    ///
    /// Encoded record bytes are charged before the cache is marked dirty.
    /// Removes and control operations use their reserved capacity and are not
    /// charged. A value of zero is rejected when the cache is opened.
    pub fn with_write_budget(mut self, bytes_per_second: u64) -> Self {
        self.write_budget_bytes_per_second = Some(bytes_per_second);
        self
    }

    /// Disable the optional write-rate budget.
    pub fn without_write_budget(mut self) -> Self {
        self.write_budget_bytes_per_second = None;
        self
    }

    /// Request a coalesced background checkpoint after this many admitted
    /// record bytes. A zero value disables periodic checkpoints; explicit
    /// `flush`, `clear`, and `close` still publish one.
    pub fn with_checkpoint_interval_bytes(mut self, bytes: u64) -> Self {
        self.checkpoint_interval_bytes = bytes;
        self.checkpoint_interval_explicit = true;
        self
    }

    /// Select blocking startup or temporary miss-only service while an
    /// unclean cache performs its bounded incremental scan.
    pub fn with_recovery_mode(mut self, mode: RecoveryMode) -> Self {
        self.recovery_mode = mode;
        self
    }

    /// Select the bounded admission policy. `Always` preserves v0.7 behavior.
    pub fn with_admission_mode(mut self, mode: AdmissionMode) -> Self {
        self.admission_mode = mode;
        self
    }

    /// Select strict FIFO or one asynchronous second chance for verified hot values.
    pub fn with_reclaim_mode(mut self, mode: ReclaimMode) -> Self {
        self.reclaim_mode = mode;
        self
    }

    /// Configure one namespace. Repeating an id replaces its earlier settings.
    /// Namespace zero exists implicitly without limits when omitted.
    pub fn with_namespace(mut self, namespace: NamespaceConfig) -> Self {
        if let Some(existing) = self
            .namespace_configs
            .iter_mut()
            .find(|existing| existing.namespace() == namespace.namespace())
        {
            *existing = namespace;
        } else {
            self.namespace_configs.push(namespace);
        }
        self
    }

    /// Limit submitted host-write bytes in each UTC day.
    ///
    /// The engine does not persist device-wide counters in the cache file.
    /// Supply the current day's durable external total with
    /// [`Self::with_daily_host_write_baseline`] to preserve a hard budget
    /// across process restarts; without it this is a per-open guard.
    pub fn with_daily_host_write_budget(mut self, bytes: u64) -> Self {
        self.daily_host_write_budget_bytes = Some(bytes);
        self
    }

    pub fn without_daily_host_write_budget(mut self) -> Self {
        self.daily_host_write_budget_bytes = None;
        self
    }

    /// Seed the host-write counter from a durable device-level source.
    ///
    /// `utc_day` is the number of days since the Unix epoch. A stale baseline
    /// is ignored after UTC rollover, so the caller can reuse a saved config.
    pub fn with_daily_host_write_baseline(mut self, utc_day: u64, bytes: u64) -> Self {
        self.daily_host_write_baseline = Some((utc_day, bytes));
        self
    }

    /// Keep NVMe health advisory-only or reject only new puts on a critical sample.
    pub fn with_device_health_policy(mut self, policy: DeviceHealthPolicy) -> Self {
        self.device_health_policy = policy;
        self
    }

    /// Bound calls from cache misses to an authoritative origin.
    ///
    /// The limiter is explicit: callers acquire a permit with
    /// [`DiskCache::try_begin_origin_fill`] only after observing a miss.
    pub fn with_origin_fill_protection(mut self, config: OriginFillConfig) -> Self {
        self.origin_fill_config = Some(config);
        self
    }

    pub fn without_origin_fill_protection(mut self) -> Self {
        self.origin_fill_config = None;
        self
    }

    pub fn open(self) -> Result<DiskCache> {
        DiskCache::open(self)
    }

    /// Create a new cache only when the dedicated path is missing or empty.
    ///
    /// The empty check is performed after acquiring the cache's exclusive
    /// file lock. Existing bytes, including a valid cache, are never opened or
    /// reformatted by this entry point.
    pub fn format_empty(self) -> Result<DiskCache> {
        DiskCache::format_empty(self)
    }

    /// Destructively reset one existing, recognized Format V1 cache.
    ///
    /// Validation and all fallible workspace allocation happen before the
    /// path is opened. The same descriptor then holds the exclusive lock
    /// across format recognition, durable truncation, and fresh formatting.
    pub fn reset_existing(self) -> Result<DiskCache> {
        DiskCache::reset_existing(self)
    }

    /// Open the cache and return the startup/recovery outcome captured before
    /// normal traffic begins.
    pub fn open_with_diagnostics(self) -> Result<(DiskCache, StartupDiagnostics)> {
        let cache = DiskCache::open(self)?;
        let diagnostics = cache.startup_diagnostics();
        Ok((cache, diagnostics))
    }

    /// Validate the complete resource plan without creating, locking, or
    /// modifying the configured path.
    pub fn diagnostics(&self) -> Result<ConfigDiagnostics> {
        let layout = self.validate()?;
        // Validate the exact size arithmetic and the small policy/control
        // allocations, but do not allocate and zero a multi-GiB index merely
        // to report its plan. `open` performs each large fallible allocation
        // once, before it creates or modifies the configured path.
        let resources = allocate_resources(self, &layout)?;
        let _controls = allocate_runtime_controls(self, None)?;
        let resource_snapshot = resources.snapshot();
        let maximum_record_bytes =
            RecordHeader::aligned_len(self.max_key_size, self.max_value_size)
                .ok_or_else(|| CacheError::InvalidConfig("maximum record is too large".into()))?;
        let checkpoint_slot_bytes = required_slot_size(layout.region_count, self.index_slots)
            .map_err(checkpoint_codec_error)?;
        let checkpoint_accounting_bytes = u64::try_from(checkpoint_accounting_workspace_bytes(
            self,
            layout.region_count as usize,
        )?)
        .map_err(|_| {
            CacheError::InvalidConfig(
                "checkpoint accounting workspace does not fit diagnostics".into(),
            )
        })?;
        Ok(ConfigDiagnostics {
            path: self.path.clone(),
            requested_capacity_bytes: self.capacity,
            data_file_len_bytes: layout.file_len,
            region_size_bytes: self.region_size,
            region_count: layout.region_count,
            index_slots: self.index_slots,
            append_lanes: self.append_lanes,
            maximum_record_bytes,
            memory_budget_bytes: resource_snapshot.memory_budget_bytes,
            planned_memory_bytes: resource_snapshot.memory_used_bytes,
            read_submission_depth: self.read_queue_depth,
            write_submission_depth: self.write_queue_depth,
            io_queue_depth: self.io_queue_depth,
            io_engine: self.io_engine,
            io_mode: self.io_mode,
            recovery_mode: self.recovery_mode,
            checkpoint_slot_bytes,
            checkpoint_accounting_bytes,
        })
    }

    fn validate(&self) -> Result<Layout> {
        #[cfg(not(target_os = "linux"))]
        if self.io_mode == IoMode::Direct {
            return Err(CacheError::InvalidConfig(
                "direct I/O is unavailable on this build target".into(),
            ));
        }
        #[cfg(not(all(
            feature = "io-uring",
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            )
        )))]
        if self.io_engine == IoEngineKind::IoUring {
            return Err(CacheError::InvalidConfig(
                "io_uring support is unavailable on this build target".into(),
            ));
        }
        if self.index_slots < 8 {
            return Err(CacheError::InvalidConfig(
                "index_slots must be at least 8".into(),
            ));
        }
        if self.io_queue_depth == 0 || self.io_queue_depth > MAX_IO_QUEUE_DEPTH {
            return Err(CacheError::InvalidConfig(format!(
                "io_queue_depth must be in 1..={MAX_IO_QUEUE_DEPTH}"
            )));
        }
        if !(1..=MAX_APPEND_LANES).contains(&self.append_lanes) {
            return Err(CacheError::InvalidConfig(format!(
                "append_lanes must be in 1..={MAX_APPEND_LANES}"
            )));
        }
        if let Some(origin) = self.origin_fill_config {
            if origin.fills_per_second == 0 {
                return Err(CacheError::InvalidConfig(
                    "origin fills_per_second must be greater than zero".into(),
                ));
            }
            if origin.max_in_flight == 0 {
                return Err(CacheError::InvalidConfig(
                    "origin max_in_flight must be greater than zero".into(),
                ));
            }
        }
        if self.index_slots > MAX_INDEX_SLOTS {
            return Err(CacheError::InvalidConfig(format!(
                "index_slots must be at most {MAX_INDEX_SLOTS}"
            )));
        }
        if self.max_key_size == 0 || self.max_key_size > MAX_KEY_SIZE {
            return Err(CacheError::InvalidConfig(format!(
                "max_key_size must be in 1..={MAX_KEY_SIZE}"
            )));
        }
        if self.max_value_size > MAX_VALUE_SIZE {
            return Err(CacheError::InvalidConfig(format!(
                "max_value_size must be at most {MAX_VALUE_SIZE}"
            )));
        }
        if self.region_size <= REGION_HEADER_SIZE as u64
            || self.region_size % SUPERBLOCK_SIZE as u64 != 0
        {
            return Err(CacheError::InvalidConfig(
                "region_size must be a 4096-byte multiple larger than its header".into(),
            ));
        }
        if self.region_size > u64::from(MAX_REGION_OFFSET) + 8 {
            return Err(CacheError::InvalidConfig(
                "region_size exceeds the packed-location offset range".into(),
            ));
        }
        let max_record = RecordHeader::aligned_len(self.max_key_size, self.max_value_size)
            .ok_or_else(|| CacheError::InvalidConfig("maximum record is too large".into()))?;
        if max_record > MAX_RECORD_LEN {
            return Err(CacheError::InvalidConfig(
                "maximum record exceeds the packed-location length range".into(),
            ));
        }
        if u64::from(max_record) > self.region_size - REGION_HEADER_SIZE as u64 {
            return Err(CacheError::InvalidConfig(format!(
                "maximum encoded record ({max_record} bytes) does not fit in a region"
            )));
        }
        let data_capacity = self
            .capacity
            .checked_sub(DATA_OFFSET)
            .ok_or_else(|| CacheError::InvalidConfig("capacity is too small".into()))?;
        let region_count = u32::try_from(data_capacity / self.region_size)
            .map_err(|_| CacheError::InvalidConfig("region count does not fit Format V1".into()))?;
        if region_count < MIN_REGIONS {
            return Err(CacheError::InvalidConfig(format!(
                "capacity must hold at least {MIN_REGIONS} regions"
            )));
        }
        if self.append_lanes >= region_count as usize {
            return Err(CacheError::InvalidConfig(
                "append_lanes must leave at least one non-Active region".into(),
            ));
        }
        if region_count >= (1 << 21) {
            return Err(CacheError::InvalidConfig(
                "Format V1 supports fewer than 2^21 regions".into(),
            ));
        }
        required_slot_size(region_count, self.index_slots).map_err(|_| {
            CacheError::InvalidConfig(
                "index checkpoint size exceeds the supported Format V1 tail".into(),
            )
        })?;
        let file_len = DATA_OFFSET
            .checked_add(u64::from(region_count) * self.region_size)
            .ok_or_else(|| CacheError::InvalidConfig("file length overflow".into()))?;
        Ok(Layout {
            region_count,
            file_len,
        })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PutOptions {
    /// Absolute Unix timestamp in milliseconds. `None` means no expiration;
    /// `Some(0)` and timestamps at or before validation time are rejected.
    pub expires_at_unix_ms: Option<u64>,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RejectReason {
    KeyTooLarge,
    ValueTooLarge,
    AlreadyExpired,
    RecordTooLarge,
    SubmissionFull,
    SubmissionTimeout,
    BufferUnavailable,
    WriteBudgetExceeded,
    AdmissionFiltered,
    LargeObjectCold,
    NamespaceNotConfigured,
    NamespaceCapacityExceeded,
    NamespaceWriteBudgetExceeded,
    DailyWriteBudgetExceeded,
    ReclaimBacklog,
    DeviceHealth,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PutOutcome {
    Stored,
    Rejected(RejectReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemoveOutcome {
    Removed,
    NotFound,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub removes: u64,
    pub rejected: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub write_batches: u64,
    pub records_coalesced: u64,
    pub regions_reused: u64,
    pub corrupt_records: u64,
    pub recovered_entries: u64,
    pub checkpoint_writes: u64,
    pub checkpoint_loads: u64,
    pub checkpoint_fallbacks: u64,
    pub checkpoint_errors: u64,
    pub recovery_regions_scanned: u64,
    pub recovery_records_scanned: u64,
    pub recovery_bytes_scanned: u64,
    pub recovery_elapsed_us: u64,
    pub recovery_regions_completed: u64,
    pub recovery_regions_total: u64,
    pub recovery_in_progress: bool,
    pub read_queue_depth: u64,
    pub write_queue_depth: u64,
    pub control_queue_depth: u64,
    pub read_queue_depth_peak: u64,
    pub write_queue_depth_peak: u64,
    pub control_queue_depth_peak: u64,
    pub read_buffers_in_use: u64,
    pub write_buffers_in_use: u64,
    pub control_buffers_in_use: u64,
    pub metadata_buffers_in_use: u64,
    pub read_buffers_in_use_peak: u64,
    pub write_buffers_in_use_peak: u64,
    pub control_buffers_in_use_peak: u64,
    pub metadata_buffers_in_use_peak: u64,
    pub queue_rejections: u64,
    pub buffer_rejections: u64,
    pub write_budget_rejections: u64,
    pub backpressure_wait_ns: u64,
    pub memory_budget_bytes: u64,
    pub memory_used_bytes: u64,
    pub memory_peak_bytes: u64,
    pub async_read_queued: u64,
    pub async_read_in_flight: u64,
    pub async_read_reserved: u64,
    pub async_mutation_queued: u64,
    pub async_mutation_in_flight: u64,
    pub async_mutation_reserved: u64,
    pub async_ordinary_mutation_queued: u64,
    pub async_control_mutation_queued: u64,
    pub async_read_queue_capacity: u64,
    pub async_write_queue_capacity: u64,
    pub async_control_queue_reserve: u64,
    pub async_queue_rejections: u64,
    pub io_queue_depth_configured: u64,
    pub io_in_flight: u64,
    pub io_in_flight_peak: u64,
    pub io_submitted: u64,
    pub io_completed: u64,
    pub io_cancel_requested: u64,
    pub io_cancelled: u64,
    pub io_errors: u64,
    pub io_submit_wait_ns: u64,
    pub io_completion_ns: u64,
    pub direct_io_operations: u64,
    pub direct_io_bytes: u64,
    pub buffered_io_operations: u64,
    pub buffered_io_bytes: u64,
    pub io_uring_active: bool,
    pub direct_io_active: bool,
    pub io_unfenced_mutations: bool,
    pub admission_observations: u64,
    pub admission_rejections: u64,
    pub large_object_rejections: u64,
    pub namespace_capacity_rejections: u64,
    pub namespace_write_budget_rejections: u64,
    pub host_write_operations: u64,
    pub host_write_bytes: u64,
    pub foreground_record_bytes: u64,
    pub reinsertion_bytes: u64,
    pub metadata_write_bytes: u64,
    pub checkpoint_write_bytes: u64,
    pub admitted_value_bytes: u64,
    pub write_amplification_milli: u64,
    pub daily_host_write_bytes: u64,
    pub daily_write_budget_rejections: u64,
    pub reinsert_queued: u64,
    pub reinsert_dropped: u64,
    pub reinsert_stale: u64,
    pub reinsert_completed: u64,
    pub background_regions_reclaimed: u64,
    pub reclaim_backlog_rejections: u64,
    pub reclaim_records_scanned: u64,
    pub reclaim_index_fallbacks: u64,
    pub region_used_bytes: u64,
    pub region_valid_bytes: u64,
    pub minimum_region_valid_ratio_bps: u64,
    pub nvme_health_observations: u64,
    pub nvme_health_critical: bool,
}

/// Point-in-time reclaim accounting for one Region.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RegionStats {
    pub region_id: u32,
    pub active: bool,
    pub sealed: bool,
    pub incarnation: u32,
    pub used_bytes: u64,
    pub valid_bytes: u64,
    pub valid_ratio_bps: u64,
    pub second_chance_bytes: u64,
    pub second_chance_pending_requests: u64,
}

#[derive(Clone)]
pub struct DiskCache {
    pub(crate) inner: Arc<Inner>,
}

type NamespaceRetireSink = dyn Fn(NamespaceUsage) -> bool + Send + Sync;
type OwnerDirtyFence = dyn Fn() -> Result<()> + Send + Sync;
pub(crate) type ManagedPutCommit =
    Box<dyn FnOnce(NamespaceUsage, Option<NamespaceUsage>) -> Result<()> + Send>;

struct ManagedPolicyHooks {
    retire_sink: Option<Arc<NamespaceRetireSink>>,
    delegated_namespaces: Option<Arc<NamespaceController>>,
    owner_dirty: Option<Arc<OwnerDirtyFence>>,
}

pub(crate) struct Inner {
    io: Arc<dyn IoBackend>,
    engine: Arc<dyn IoEngine>,
    config: CacheConfig,
    resources: Arc<ResourceController>,
    policy: Arc<PolicyController>,
    delegated_policy: bool,
    delegated_namespaces: Option<Arc<NamespaceController>>,
    opened_clean: bool,
    retire_sink: Option<Arc<NamespaceRetireSink>>,
    owner_dirty: Option<Arc<OwnerDirtyFence>>,
    origin_fill_limiter: Option<Arc<OriginFillLimiter>>,
    telemetry: RequestTelemetry,
    index: Arc<ShardedIndex>,
    region_valid_bytes: Vec<AtomicU64>,
    region_reinserted_bytes: Vec<AtomicU64>,
    region_reinsert_pending: Vec<AtomicU64>,
    operation_barrier: RwLock<()>,
    key_ordering: KeyOrdering,
    accepting: AtomicBool,
    lifecycle: AtomicU8,
    read_view: ReadView,
    read_stats: ReadStats,
    state: Mutex<State>,
    append_txs: Vec<SyncSender<AppendCommand>>,
    append_workers: Mutex<Vec<JoinHandle<()>>>,
    reinsert_tx: SyncSender<ReinsertCommand>,
    reinsert_worker: Mutex<Option<JoinHandle<()>>>,
    reinsert_queued: AtomicU64,
    reinsert_dropped: AtomicU64,
    reinsert_stale: AtomicU64,
    reinsert_completed: AtomicU64,
    maintenance_tx: SyncSender<MaintenanceCommand>,
    maintenance_worker: Mutex<Option<JoinHandle<()>>>,
    reclaim_eligible: AtomicBool,
    reclaim_forced: AtomicBool,
    reclaim_records_scanned: AtomicU64,
    reclaim_index_fallbacks: AtomicU64,
    checkpoint_bytes: AtomicU64,
    checkpoint_pending: AtomicBool,
    recovery_worker: Mutex<Option<JoinHandle<()>>>,
    recovery_cancel: AtomicBool,
    recovery_active: AtomicBool,
    recovery_regions_done: AtomicU64,
    recovery_regions_total: AtomicU64,
    async_handle: Mutex<Option<Arc<AsyncInner>>>,
    #[cfg(test)]
    schedule_observer: Mutex<Option<ScheduleObserver>>,
    #[cfg(test)]
    force_direct_append_padding: AtomicBool,
    #[cfg(test)]
    append_coalesce_delay_ns: AtomicU64,
}

struct KeyOrdering {
    shards: Vec<Mutex<()>>,
}

impl KeyOrdering {
    fn try_new() -> Result<Self> {
        let mut shards = Vec::new();
        shards.try_reserve_exact(KEY_ORDERING_SHARDS).map_err(|_| {
            CacheError::InvalidConfig("key-ordering table cannot be allocated".into())
        })?;
        shards.resize_with(KEY_ORDERING_SHARDS, || Mutex::new(()));
        Ok(Self { shards })
    }

    fn lock(&self, hash: u64) -> MutexGuard<'_, ()> {
        let shard = (hash as usize) & (self.shards.len() - 1);
        self.shards[shard]
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn try_lock(&self, hash: u64) -> Option<MutexGuard<'_, ()>> {
        let shard = (hash as usize) & (self.shards.len() - 1);
        match self.shards[shard].try_lock() {
            Ok(guard) => Some(guard),
            Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
}

#[derive(Clone, Copy)]
struct Layout {
    region_count: u32,
    file_len: u64,
}

struct OpenRuntime {
    index: Arc<ShardedIndex>,
    regions: Vec<RegionMeta>,
    recovery_order: Vec<u32>,
    checkpoint_regions: Vec<CheckpointRegionSnapshot>,
    background_regions: Vec<RegionMeta>,
    read_regions: Vec<RwLock<RegionMeta>>,
    key_ordering: KeyOrdering,
    resources: Arc<ResourceController>,
    policy: Arc<PolicyController>,
    delegated_policy: bool,
    origin_fill_limiter: Option<Arc<OriginFillLimiter>>,
    region_valid_bytes: Vec<AtomicU64>,
    region_reinserted_bytes: Vec<AtomicU64>,
    region_reinsert_pending: Vec<AtomicU64>,
}

struct State {
    superblock: Superblock,
    regions: Vec<RegionMeta>,
    active_regions: Vec<u32>,
    free_regions: VecDeque<u32>,
    sealed_regions: VecDeque<u32>,
    index: Arc<ShardedIndex>,
    checkpoint_slot: Option<u8>,
    reclaiming_region: Option<u32>,
    reclaim_ready_region: Option<u32>,
    stats: CacheStats,
    status: CacheStatus,
    lock_held: bool,
    /// The checkpoint loader has already rebuilt Region and namespace live
    /// byte accounting while restoring the final visible index entries.
    /// Dirty/full-scan recovery must leave this false because it mutates the
    /// index after the checkpoint payload has been consumed.
    runtime_accounting_restored: bool,
}

#[derive(Clone, Copy)]
struct RegionMeta {
    header: RegionHeader,
    used: u64,
    max_seqno: u64,
}

struct ReadView {
    superblock: RwLock<Superblock>,
    regions: Vec<RwLock<RegionMeta>>,
}

struct ReadStats {
    hits: AtomicU64,
    misses: AtomicU64,
    bytes_read: AtomicU64,
    corrupt_records: AtomicU64,
}

impl ReadStats {
    const fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
            corrupt_records: AtomicU64::new(0),
        }
    }

    fn record_miss(&self) {
        atomic_saturating_add(&self.misses, 1);
    }

    fn record_hit(&self, bytes: u64) {
        atomic_saturating_add(&self.hits, 1);
        atomic_saturating_add(&self.bytes_read, bytes);
    }

    fn record_corrupt_miss(&self) {
        atomic_saturating_add(&self.corrupt_records, 1);
        atomic_saturating_add(&self.misses, 1);
    }

    fn snapshot(&self) -> ReadStatsSnapshot {
        ReadStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            corrupt_records: self.corrupt_records.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ReadStatsSnapshot {
    hits: u64,
    misses: u64,
    bytes_read: u64,
    corrupt_records: u64,
}

struct PendingFlagGuard<'a>(&'a AtomicBool);

impl Drop for PendingFlagGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone, Copy)]
struct ReadSnapshot {
    superblock: Superblock,
    region: RegionMeta,
}

#[derive(Clone, Copy)]
struct AppendReservation {
    location: PackedLocation,
    seqno: u64,
    epoch: u32,
    region_incarnation: u32,
    absolute: u64,
}

enum LoadedRecord {
    Value { start: usize, len: usize },
    Tombstone,
    KeyMismatch,
    Expired,
    Corrupt,
    Unavailable(io::Error),
    Cancelled,
}

#[derive(Clone, Copy)]
struct ReinsertRecord {
    codec: RecordCodec,
    key_len: usize,
    value_len: usize,
    expires_at: u64,
    minimum_record_len: u32,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SchedulePoint {
    AppendCoalesceWaiting,
    ReadCompleted,
    RotateBlockedByReader,
    RotateReadersDrained,
}

#[cfg(test)]
type ScheduleObserver = Arc<dyn Fn(SchedulePoint) + Send + Sync>;

enum AppendCommand {
    Put {
        hash: u64,
        namespace_id: NamespaceId,
        codec: RecordCodec,
        key_len: usize,
        value_len: usize,
        expires_at: u64,
        record_len: u32,
        source: PutSource,
        managed_commit: Option<ManagedPutCommit>,
        resources: DataResources,
        completion: SyncSender<Result<RegionPutReceipt>>,
    },
    Remove {
        hash: u64,
        namespace_id: NamespaceId,
        codec: RecordCodec,
        key_len: usize,
        record_len: u32,
        managed: bool,
        resources: RemoveResources,
        completion: SyncSender<Result<RegionRemoveReceipt>>,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PutSource {
    Foreground,
    ManagedForeground,
    Reinsertion,
}

impl PutSource {
    const fn is_foreground(self) -> bool {
        matches!(self, Self::Foreground | Self::ManagedForeground)
    }

    const fn is_managed(self) -> bool {
        matches!(self, Self::ManagedForeground)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionPutReceipt {
    pub(crate) outcome: PutOutcome,
    pub(crate) new_usage: Option<NamespaceUsage>,
    pub(crate) previous_usage: Option<NamespaceUsage>,
}

impl RegionPutReceipt {
    const fn rejected(outcome: PutOutcome) -> Self {
        Self {
            outcome,
            new_usage: None,
            previous_usage: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRemoveReceipt {
    pub(crate) outcome: RemoveOutcome,
    pub(crate) previous_usage: Option<NamespaceUsage>,
}

enum MaintenanceCommand {
    Checkpoint,
    Reclaim,
    Shutdown,
}

enum ReinsertCommand {
    Candidate {
        hash: u64,
        entry: IndexEntry,
        region_incarnation: u32,
        reserved_bytes: u64,
    },
    Shutdown,
}

struct PendingPut {
    hash: u64,
    namespace_id: NamespaceId,
    codec: RecordCodec,
    key_len: usize,
    value_len: usize,
    expires_at: u64,
    record_len: u32,
    source: PutSource,
    managed_commit: Option<ManagedPutCommit>,
    resources: DataResources,
    completion: SyncSender<Result<RegionPutReceipt>>,
}

struct PreparedPut {
    hash: u64,
    namespace_id: NamespaceId,
    codec: RecordCodec,
    key_len: usize,
    value_len: usize,
    expires_at: u64,
    minimum_record_len: u32,
    source: PutSource,
    managed_commit: Option<ManagedPutCommit>,
    _permit: QueuePermit,
    buffer: Option<BufferLease>,
    completion: SyncSender<Result<RegionPutReceipt>>,
    receipt: Option<RegionPutReceipt>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenRequirement {
    OpenOrCreate,
    Empty,
    ResetExisting,
}

impl DiskCache {
    pub(crate) fn from_inner(inner: Arc<Inner>) -> Self {
        Self { inner }
    }

    /// Open an existing cache, or format an empty/corrupt dedicated file.
    pub fn open(config: CacheConfig) -> Result<Self> {
        Self::open_with_requirement(config, OpenRequirement::OpenOrCreate)
    }

    /// Open a Region engine whose logical policy is owned by a composing
    /// driver. Region I/O remains fully accounted in `shared_host_writes`.
    #[allow(dead_code)]
    pub(crate) fn open_managed(
        config: CacheConfig,
        shared_host_writes: Arc<HostWriteTracker>,
    ) -> Result<Self> {
        Self::open_with_requirement_and_host_writes(
            config,
            OpenRequirement::OpenOrCreate,
            Some(shared_host_writes),
            None,
            None,
            None,
        )
    }

    /// Open a managed Region engine and report every logical live-value
    /// retirement to its composing driver. Physical second-chance copies do
    /// not trigger this sink.
    #[allow(dead_code)]
    pub(crate) fn open_managed_with_retire_sink(
        config: CacheConfig,
        shared_host_writes: Arc<HostWriteTracker>,
        delegated_namespaces: Arc<NamespaceController>,
        retire_sink: Arc<dyn Fn(NamespaceUsage) -> bool + Send + Sync>,
    ) -> Result<Self> {
        Self::open_with_requirement_and_host_writes(
            config,
            OpenRequirement::OpenOrCreate,
            Some(shared_host_writes),
            Some(retire_sink),
            Some(delegated_namespaces),
            None,
        )
    }

    pub(crate) fn open_managed_with_owner_hooks(
        config: CacheConfig,
        shared_host_writes: Arc<HostWriteTracker>,
        delegated_namespaces: Arc<NamespaceController>,
        retire_sink: Arc<dyn Fn(NamespaceUsage) -> bool + Send + Sync>,
        owner_dirty: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<Self> {
        // The owner fence makes an unclean lower session disposable. Region
        // rotation may therefore defer its local durability syncs to the
        // owner's flush/close boundary.
        Self::open_with_requirement_and_host_writes(
            config,
            OpenRequirement::OpenOrCreate,
            Some(shared_host_writes),
            Some(retire_sink),
            Some(delegated_namespaces),
            Some(owner_dirty),
        )
    }

    fn format_empty(config: CacheConfig) -> Result<Self> {
        Self::open_with_requirement(config, OpenRequirement::Empty)
    }

    fn reset_existing(config: CacheConfig) -> Result<Self> {
        Self::open_with_requirement(config, OpenRequirement::ResetExisting)
    }

    fn open_with_requirement(config: CacheConfig, requirement: OpenRequirement) -> Result<Self> {
        Self::open_with_requirement_and_host_writes(config, requirement, None, None, None, None)
    }

    fn open_with_requirement_and_host_writes(
        config: CacheConfig,
        requirement: OpenRequirement,
        shared_host_writes: Option<Arc<HostWriteTracker>>,
        retire_sink: Option<Arc<NamespaceRetireSink>>,
        delegated_namespaces: Option<Arc<NamespaceController>>,
        owner_dirty: Option<Arc<OwnerDirtyFence>>,
    ) -> Result<Self> {
        let layout = config.validate()?;
        let runtime = allocate_open_runtime(&config, &layout, shared_host_writes)?;
        let backend = Arc::new(match requirement {
            OpenRequirement::OpenOrCreate | OpenRequirement::Empty => {
                FileBackend::open_with_io_mode(&config.path, config.io_mode.into())?
            }
            OpenRequirement::ResetExisting => {
                FileBackend::open_existing_with_io_mode(&config.path, config.io_mode.into())?
            }
        });
        let runtime_files = backend.try_clone_runtime_files()?;
        let io: Arc<dyn IoBackend> = backend;
        let managed_policy = ManagedPolicyHooks {
            retire_sink,
            delegated_namespaces,
            owner_dirty,
        };
        Self::open_on_backend(
            config,
            layout,
            runtime,
            io,
            Some(runtime_files),
            requirement,
            managed_policy,
        )
    }

    #[cfg(test)]
    fn open_with_backend(config: CacheConfig, io: Box<dyn IoBackend>) -> Result<Self> {
        let layout = config.validate()?;
        let runtime = allocate_open_runtime(&config, &layout, None)?;
        Self::open_on_backend(
            config,
            layout,
            runtime,
            Arc::from(io),
            None,
            OpenRequirement::OpenOrCreate,
            ManagedPolicyHooks {
                retire_sink: None,
                delegated_namespaces: None,
                owner_dirty: None,
            },
        )
    }

    #[cfg(test)]
    fn open_with_backend_and_owner_hooks(
        config: CacheConfig,
        io: Box<dyn IoBackend>,
        shared_host_writes: Arc<HostWriteTracker>,
        delegated_namespaces: Arc<NamespaceController>,
        retire_sink: Arc<NamespaceRetireSink>,
        owner_dirty: Arc<OwnerDirtyFence>,
    ) -> Result<Self> {
        let layout = config.validate()?;
        let runtime = allocate_open_runtime(&config, &layout, Some(shared_host_writes))?;
        Self::open_on_backend(
            config,
            layout,
            runtime,
            Arc::from(io),
            None,
            OpenRequirement::OpenOrCreate,
            ManagedPolicyHooks {
                retire_sink: Some(retire_sink),
                delegated_namespaces: Some(delegated_namespaces),
                owner_dirty: Some(owner_dirty),
            },
        )
    }

    fn open_on_backend(
        config: CacheConfig,
        layout: Layout,
        runtime: OpenRuntime,
        io: Arc<dyn IoBackend>,
        runtime_files: Option<RuntimeFileSet>,
        requirement: OpenRequirement,
        managed_policy: ManagedPolicyHooks,
    ) -> Result<Self> {
        let ManagedPolicyHooks {
            retire_sink,
            delegated_namespaces,
            owner_dirty,
        } = managed_policy;
        let OpenRuntime {
            index,
            mut regions,
            mut recovery_order,
            mut checkpoint_regions,
            mut background_regions,
            mut read_regions,
            key_ordering,
            resources,
            policy,
            delegated_policy,
            origin_fill_limiter,
            region_valid_bytes,
            region_reinserted_bytes,
            region_reinsert_pending,
        } = runtime;
        try_lock_exclusive(io.as_ref())?;
        let file_had_content = match io.len() {
            Ok(length) => length != 0,
            Err(error) => {
                let _ = unlock_file(io.as_ref());
                return Err(CacheError::Io(error));
            }
        };
        let prepare_result = (|| -> Result<bool> {
            match requirement {
                OpenRequirement::OpenOrCreate => Ok(false),
                OpenRequirement::Empty => {
                    if io.len()? == 0 {
                        Ok(false)
                    } else {
                        Err(CacheError::InvalidConfig(
                            "format requires a missing or empty cache path".into(),
                        ))
                    }
                }
                OpenRequirement::ResetExisting => {
                    recognize_format_v1_for_reset(io.as_ref())?;
                    io.set_len(0)?;
                    io.sync(SyncPoint::FormatTruncate, SyncMode::All)?;
                    Ok(true)
                }
            }
        })();
        let reset_existing = match prepare_result {
            Ok(reset_existing) => reset_existing,
            Err(error) => {
                let _ = unlock_file(io.as_ref());
                return Err(error);
            }
        };

        let mut pending_recovery: Option<PendingRecovery> = None;
        let mut opened_clean = true;
        let state_result = if reset_existing {
            opened_clean = false;
            format_state(
                io.as_ref(),
                policy.host_writes(),
                &config,
                &layout,
                None,
                index,
                regions,
            )
        } else {
            (|| -> Result<State> {
                match read_superblock(io.as_ref())? {
                    Some(superblock) => {
                        opened_clean = superblock.clean;
                        if superblock.region_size != config.region_size
                            || superblock.region_count != layout.region_count
                            || superblock.hash_seed != config.hash_seed
                        {
                            return Err(CacheError::InvalidConfig(
                            "existing cache layout does not match capacity/region_size/hash_seed"
                                .into(),
                        ));
                        }
                        if io.len()? < layout.file_len {
                            opened_clean = false;
                            format_state(
                                io.as_ref(),
                                policy.host_writes(),
                                &config,
                                &layout,
                                Some(superblock.generation),
                                index,
                                regions,
                            )
                        } else if superblock.clean {
                            let checkpoint = try_load_checkpoint(
                                io.as_ref(),
                                &superblock,
                                index.as_ref(),
                                &mut checkpoint_regions,
                                policy.as_ref(),
                                !delegated_policy,
                                &region_valid_bytes,
                            )?;
                            let recovered = match checkpoint {
                                Some(checkpoint) => recover_clean_checkpoint_state(
                                    io.as_ref(),
                                    superblock,
                                    checkpoint,
                                    Arc::clone(&index),
                                    &checkpoint_regions,
                                    config.append_lanes,
                                    &mut regions,
                                    &mut recovery_order,
                                ),
                                None => {
                                    opened_clean = false;
                                    index.clear();
                                    recover_state(
                                        io.as_ref(),
                                        superblock,
                                        Arc::clone(&index),
                                        &resources,
                                        config.append_lanes,
                                        &mut regions,
                                        &mut recovery_order,
                                    )
                                    .map(|mut state| {
                                        state.stats.checkpoint_fallbacks = 1;
                                        state
                                    })
                                }
                            };
                            match recovered {
                                Ok(state) => Ok(state),
                                Err(CacheError::CorruptMetadata(_)) => {
                                    opened_clean = false;
                                    index.clear();
                                    match recover_state(
                                        io.as_ref(),
                                        superblock,
                                        Arc::clone(&index),
                                        &resources,
                                        config.append_lanes,
                                        &mut regions,
                                        &mut recovery_order,
                                    ) {
                                        Ok(mut state) => {
                                            state.stats.checkpoint_fallbacks = 1;
                                            Ok(state)
                                        }
                                        Err(CacheError::CorruptMetadata(_)) => format_state(
                                            io.as_ref(),
                                            policy.host_writes(),
                                            &config,
                                            &layout,
                                            Some(superblock.generation),
                                            Arc::clone(&index),
                                            regions,
                                        ),
                                        Err(error) => Err(error),
                                    }
                                }
                                Err(error) => Err(error),
                            }
                        } else {
                            match try_load_checkpoint(
                                io.as_ref(),
                                &superblock,
                                index.as_ref(),
                                &mut checkpoint_regions,
                                policy.as_ref(),
                                !delegated_policy,
                                &region_valid_bytes,
                            )? {
                                Some(checkpoint) => {
                                    if config.recovery_mode == RecoveryMode::MissOnly {
                                        let state = prepare_miss_only_recovery_state(
                                            superblock,
                                            checkpoint,
                                            Arc::clone(&index),
                                            &checkpoint_regions,
                                            config.append_lanes,
                                            &mut regions,
                                            &mut recovery_order,
                                        )?;
                                        pending_recovery = Some(PendingRecovery {
                                            layout,
                                            superblock,
                                            checkpoint,
                                            checkpoint_regions: std::mem::take(
                                                &mut checkpoint_regions,
                                            ),
                                            regions: std::mem::take(&mut background_regions),
                                            ordered: std::mem::take(&mut recovery_order),
                                        });
                                        Ok(state)
                                    } else {
                                        match recover_dirty_checkpoint_state(
                                            io.as_ref(),
                                            policy.host_writes(),
                                            config.reclaim_mode,
                                            superblock,
                                            checkpoint,
                                            Arc::clone(&index),
                                            &resources,
                                            &checkpoint_regions,
                                            config.append_lanes,
                                            &mut regions,
                                            &mut recovery_order,
                                            None,
                                            None,
                                        ) {
                                            Ok(state) => Ok(state),
                                            Err(CacheError::CorruptMetadata(_)) => format_state(
                                                io.as_ref(),
                                                policy.host_writes(),
                                                &config,
                                                &layout,
                                                Some(superblock.generation),
                                                Arc::clone(&index),
                                                regions,
                                            ),
                                            Err(error) => Err(error),
                                        }
                                    }
                                }
                                None => format_state(
                                    io.as_ref(),
                                    policy.host_writes(),
                                    &config,
                                    &layout,
                                    Some(superblock.generation),
                                    index,
                                    regions,
                                ),
                            }
                        }
                    }
                    None => {
                        opened_clean = !file_had_content;
                        format_state(
                            io.as_ref(),
                            policy.host_writes(),
                            &config,
                            &layout,
                            None,
                            index,
                            regions,
                        )
                    }
                }
            })()
        };
        let state = match state_result {
            Ok(state) => state,
            Err(error) => {
                // Do not rely on descriptor destruction to release flock: the
                // caller may retry immediately in the same process.
                let _ = unlock_file(io.as_ref());
                return Err(error);
            }
        };
        if state.runtime_accounting_restored {
            reset_reinsertion_accounting(&region_reinserted_bytes, &region_reinsert_pending);
        } else if let Err(error) = rebuild_runtime_accounting(
            &state,
            policy.as_ref(),
            !delegated_policy,
            &region_valid_bytes,
            &region_reinserted_bytes,
            &region_reinsert_pending,
        ) {
            let _ = unlock_file(io.as_ref());
            return Err(error);
        }
        let initial_status = state.status;
        let reclaim_eligible = state.free_regions.is_empty();
        debug_assert!(read_regions.capacity() >= state.regions.len());
        read_regions.extend(state.regions.iter().copied().map(RwLock::new));
        let read_view = ReadView {
            superblock: RwLock::new(state.superblock),
            regions: read_regions,
        };
        let engine = match build_io_engine(&config, Arc::clone(&io), runtime_files) {
            Ok(engine) => engine,
            Err(error) => {
                let _ = unlock_file(io.as_ref());
                return Err(error);
            }
        };
        let append_lanes = config.append_lanes;
        let channel_capacity = config.write_queue_depth + 1;
        let mut append_txs = Vec::with_capacity(append_lanes);
        let mut append_receivers = Vec::with_capacity(append_lanes);
        for _ in 0..append_lanes {
            let (append_tx, append_rx) = mpsc::sync_channel(channel_capacity);
            append_txs.push(append_tx);
            append_receivers.push(append_rx);
        }
        let (maintenance_tx, maintenance_rx) = mpsc::sync_channel(1);
        let (reinsert_tx, reinsert_rx) = mpsc::sync_channel(SECOND_CHANCE_QUEUE_DEPTH);
        let inner = Arc::new(Inner {
            io,
            engine,
            config,
            resources,
            policy,
            delegated_policy,
            delegated_namespaces,
            opened_clean,
            retire_sink,
            owner_dirty,
            origin_fill_limiter,
            telemetry: RequestTelemetry::new(initial_status),
            index: Arc::clone(&state.index),
            region_valid_bytes,
            region_reinserted_bytes,
            region_reinsert_pending,
            operation_barrier: RwLock::new(()),
            key_ordering,
            accepting: AtomicBool::new(true),
            lifecycle: AtomicU8::new(initial_status as u8),
            read_view,
            read_stats: ReadStats::new(),
            state: Mutex::new(state),
            append_txs,
            append_workers: Mutex::new(Vec::new()),
            reinsert_tx,
            reinsert_worker: Mutex::new(None),
            reinsert_queued: AtomicU64::new(0),
            reinsert_dropped: AtomicU64::new(0),
            reinsert_stale: AtomicU64::new(0),
            reinsert_completed: AtomicU64::new(0),
            maintenance_tx,
            maintenance_worker: Mutex::new(None),
            reclaim_eligible: AtomicBool::new(reclaim_eligible),
            reclaim_forced: AtomicBool::new(false),
            reclaim_records_scanned: AtomicU64::new(0),
            reclaim_index_fallbacks: AtomicU64::new(0),
            checkpoint_bytes: AtomicU64::new(0),
            checkpoint_pending: AtomicBool::new(false),
            recovery_worker: Mutex::new(None),
            recovery_cancel: AtomicBool::new(false),
            recovery_active: AtomicBool::new(false),
            recovery_regions_done: AtomicU64::new(0),
            recovery_regions_total: AtomicU64::new(0),
            async_handle: Mutex::new(None),
            #[cfg(test)]
            schedule_observer: Mutex::new(None),
            #[cfg(test)]
            force_direct_append_padding: AtomicBool::new(false),
            #[cfg(test)]
            append_coalesce_delay_ns: AtomicU64::new(APPEND_COALESCE_DELAY.as_nanos() as u64),
        });
        let needs_recovery_baseline = {
            let state = inner
                .state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            !state.superblock.clean || state.checkpoint_slot.is_none()
        };
        // Fresh Format V1 files and legacy/full-scan fallbacks have no index
        // slot yet. Publish a baseline before accepting the first mutation so
        // a dirty restart can replay its tail instead of discarding the cache.
        if needs_recovery_baseline && pending_recovery.is_none() {
            let cache = Self {
                inner: Arc::clone(&inner),
            };
            let result = {
                let mut state = inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                cache
                    .checkpoint_clean(&mut state)
                    .map(|()| cache.publish_read_view(&state))
            };
            if let Err(error) = result {
                let _ = inner.engine.shutdown();
                let _ = unlock_file(inner.io.as_ref());
                inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .lock_held = false;
                return Err(error);
            }
        }
        let mut workers: Vec<JoinHandle<()>> = Vec::with_capacity(append_lanes);
        for (lane_id, append_rx) in append_receivers.into_iter().enumerate() {
            let weak = Arc::downgrade(&inner);
            let worker = match std::thread::Builder::new()
                .name(format!("cache-rs-append-{lane_id}"))
                .spawn(move || append_worker(weak, lane_id, append_rx))
            {
                Ok(worker) => worker,
                Err(error) => {
                    for append_tx in inner.append_txs.iter().take(workers.len()) {
                        let _ = append_tx.send(AppendCommand::Shutdown);
                    }
                    for worker in workers {
                        let _ = worker.join();
                    }
                    let mut state = inner
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let _ = inner.engine.shutdown();
                    let _ = unlock_file(inner.io.as_ref());
                    state.lock_held = false;
                    return Err(CacheError::Io(error));
                }
            };
            workers.push(worker);
        }
        *inner
            .append_workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = workers;
        if inner.config.reclaim_mode == ReclaimMode::SecondChance {
            let weak = Arc::downgrade(&inner);
            let worker = match std::thread::Builder::new()
                .name("cache-rs-reinsert".into())
                .spawn(move || reinsert_worker(weak, reinsert_rx))
            {
                Ok(worker) => worker,
                Err(error) => {
                    let cache = Self {
                        inner: Arc::clone(&inner),
                    };
                    let _ = cache.stop_and_join_append_workers();
                    let _ = inner.engine.shutdown();
                    let _ = unlock_file(inner.io.as_ref());
                    inner
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .lock_held = false;
                    return Err(CacheError::Io(error));
                }
            };
            *inner
                .reinsert_worker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(worker);
        } else {
            drop(reinsert_rx);
        }
        let weak = Arc::downgrade(&inner);
        let maintenance_worker = match std::thread::Builder::new()
            .name("cache-rs-checkpoint".into())
            .spawn(move || maintenance_worker(weak, maintenance_rx))
        {
            Ok(worker) => worker,
            Err(error) => {
                let cache = Self {
                    inner: Arc::clone(&inner),
                };
                let _ = cache.stop_and_join_reinsert_worker();
                let _ = cache.stop_and_join_append_workers();
                let _ = inner.engine.shutdown();
                let _ = unlock_file(inner.io.as_ref());
                inner
                    .state
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .lock_held = false;
                return Err(CacheError::Io(error));
            }
        };
        *inner
            .maintenance_worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(maintenance_worker);
        if let Some(pending) = pending_recovery {
            inner.recovery_active.store(true, Ordering::Release);
            inner.recovery_regions_total.store(
                u64::from(pending.superblock.region_count),
                Ordering::Release,
            );
            let weak = Arc::downgrade(&inner);
            let recovery_worker = match std::thread::Builder::new()
                .name("cache-rs-recovery".into())
                .spawn(move || background_recovery_worker(weak, pending))
            {
                Ok(worker) => worker,
                Err(error) => {
                    inner.recovery_active.store(false, Ordering::Release);
                    let cache = Self {
                        inner: Arc::clone(&inner),
                    };
                    let _ = cache.stop_and_join_maintenance_worker();
                    let _ = cache.stop_and_join_reinsert_worker();
                    let _ = cache.stop_and_join_append_workers();
                    let _ = inner.engine.shutdown();
                    let _ = unlock_file(inner.io.as_ref());
                    inner
                        .state
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .lock_held = false;
                    return Err(CacheError::Io(error));
                }
            };
            *inner
                .recovery_worker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(recovery_worker);
        }
        Ok(Self { inner })
    }

    /// Return the shared bounded asynchronous facade for this cache instance.
    ///
    /// Repeated calls and clones reuse one executor, so creating handles cannot
    /// multiply the configured queues or worker threads.
    pub fn async_handle(&self) -> Result<AsyncDiskCache> {
        let mut shared = self
            .inner
            .async_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Serialize executor construction with close. Once close observes an
        // empty registry and closes sync admission, no late facade can appear.
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(CacheError::Closed);
        }
        match self.runtime_status() {
            CacheStatus::Healthy | CacheStatus::MissOnly => {}
            CacheStatus::Poisoned => return Err(CacheError::Poisoned),
            CacheStatus::Closed => return Err(CacheError::Closed),
        }
        if let Some(inner) = shared.as_ref() {
            return Ok(AsyncDiskCache::from_inner(Arc::clone(inner)));
        }
        let handle = AsyncDiskCache::try_new(
            Arc::downgrade(&self.inner),
            self.inner.config.read_queue_depth,
            self.inner.config.write_queue_depth,
            self.inner.config.io_queue_depth,
            async_mutation_worker_count(
                self.inner.config.write_queue_depth,
                self.inner.config.append_lanes,
            ),
        )?;
        *shared = Some(Arc::clone(handle.shared_inner()));
        Ok(handle)
    }

    pub(crate) fn async_put_input_limits(&self) -> (usize, usize) {
        (
            self.inner.config.max_key_size,
            self.inner.config.max_value_size,
        )
    }

    pub(crate) fn record_async_put_rejection(&self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.status == CacheStatus::Healthy {
            state.stats.rejected = state.stats.rejected.saturating_add(1);
        }
    }

    pub(crate) fn record_async_miss(&self) {
        self.inner.read_stats.record_miss();
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_in(0, key)
    }

    /// Look up a key in one configured namespace.
    pub fn get_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_in_with_context(namespace, key, None)
    }

    /// Cheap advisory membership check for the Hybrid coordinator. A compact
    /// index collision may return `true`, but `false` means there is no current
    /// Region candidate and avoids appending an unnecessary tombstone during a
    /// small-object update.
    pub(crate) fn may_contain_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<bool> {
        if !self.ensure_readable()? {
            return Ok(false);
        }
        let _barrier = self.lock_shared_operation()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        // Membership is an advisory fast-path query. ReadView publishes the
        // clear floor independently of the Region-manager mutex, so a FIFO
        // rotation doing header sync and victim scrub cannot stall foreground
        // Hybrid admission here.
        let epoch_start_seqno = self.read_superblock()?.epoch_start_seqno;
        Ok(self.inner.index.get(hash, epoch_start_seqno).is_some())
    }

    /// Voluntarily forget the current compact-index candidate without writing
    /// a tombstone or issuing any other device I/O. This is suitable for a
    /// disposable cache whose caller prefers a conservative miss over write
    /// amplification. Because the compact index stores only hashes, a true
    /// hash collision may evict the colliding candidate; its actual namespace
    /// and physical size are still retired from the removed entry metadata.
    ///
    /// Returns `true` exactly when this call removed a currently visible index
    /// candidate. Repeated invalidations therefore do not double-retire Region
    /// valid bytes or delegated namespace usage.
    #[allow(dead_code)]
    pub(crate) fn invalidate_in_memory(&self, namespace: NamespaceId, key: &[u8]) -> Result<bool> {
        self.ensure_accepting()?;
        let _barrier = self.lock_shared_operation()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let _key_order = self.inner.key_ordering.lock(hash);
        let epoch_start_seqno = self.read_superblock()?.epoch_start_seqno;
        let Some(candidate) = self.inner.index.get(hash, epoch_start_seqno) else {
            return Ok(false);
        };
        self.remove_index_entry_accounted(hash, candidate)
    }

    /// Return a conservative upper bound for the current candidate's decoded
    /// key and value allocation without issuing I/O. Hybrid holds its same-key
    /// ordering lock while using this bound, so a foreground mutation cannot
    /// replace the candidate with a larger record before the subsequent read.
    pub(crate) fn candidate_record_bytes_in(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<Option<usize>> {
        if !self.ensure_readable()? {
            return Ok(None);
        }
        let _barrier = self.lock_shared_operation()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let epoch_start_seqno = self.read_superblock()?.epoch_start_seqno;
        Ok(self
            .inner
            .index
            .get(hash, epoch_start_seqno)
            .filter(|entry| entry.namespace_id == namespace && !entry.location.is_tombstone())
            .map(|entry| entry.location.record_len() as usize))
    }

    /// Conservatively reserve the largest physical record this runtime may
    /// publish for one logical value. Direct I/O assigns at most one 4 KiB
    /// tail-alignment pad to the final record of a coalesced batch; the
    /// managed receipt later releases any unused portion of this reservation.
    pub(crate) fn maximum_record_bytes_in(
        &self,
        namespace: NamespaceId,
        key_len: usize,
        value_len: usize,
    ) -> Option<u64> {
        let encoded_key_len = encoded_key_len(namespace, key_len)?;
        let minimum = RecordHeader::aligned_len(encoded_key_len, value_len)?;
        let maximum = if self.direct_append_active() {
            minimum
                .saturating_add((DIRECT_IO_ALIGNMENT - 1) as u32)
                .min(MAX_RECORD_LEN)
        } else {
            minimum
        };
        Some(u64::from(maximum))
    }

    /// Whether this instance opened from an already-clean lower-tier
    /// checkpoint without running recovery or a fallback rebuild.
    pub(crate) fn opened_clean(&self) -> bool {
        self.inner.opened_clean
    }

    fn direct_append_active(&self) -> bool {
        if self.inner.engine.direct_active() {
            return true;
        }
        #[cfg(test)]
        {
            self.inner
                .force_direct_append_padding
                .load(Ordering::Acquire)
        }
        #[cfg(not(test))]
        {
            false
        }
    }

    fn append_coalesce_delay(&self) -> Duration {
        #[cfg(test)]
        {
            Duration::from_nanos(self.inner.append_coalesce_delay_ns.load(Ordering::Acquire))
        }
        #[cfg(not(test))]
        {
            APPEND_COALESCE_DELAY
        }
    }

    #[cfg(test)]
    fn set_append_coalesce_delay_for_test(&self, delay: Duration) {
        let nanos = u64::try_from(delay.as_nanos()).expect("test append delay must fit u64 nanos");
        self.inner
            .append_coalesce_delay_ns
            .store(nanos, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn force_direct_append_padding_for_test(&self) {
        self.set_direct_append_padding_for_test(true);
    }

    #[cfg(test)]
    pub(crate) fn set_direct_append_padding_for_test(&self, enabled: bool) {
        self.inner
            .force_direct_append_padding
            .store(enabled, Ordering::Release);
    }

    /// Exercise the production reinsertion path synchronously without first
    /// filling and sealing a complete Region. This keeps direct-padding and
    /// namespace-budget regressions portable across non-Linux test hosts.
    #[cfg(test)]
    pub(crate) fn reinsert_current_for_test(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<bool> {
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let (entry, incarnation) = {
            let state = self.lock_state()?;
            ensure_operational(&state)?;
            let Some(entry) = self
                .inner
                .index
                .get(hash, state.superblock.epoch_start_seqno)
                .filter(|entry| entry.namespace_id == namespace && !entry.location.is_tombstone())
            else {
                return Ok(false);
            };
            (
                entry,
                state.regions[entry.location.region_id() as usize]
                    .header
                    .incarnation,
            )
        };
        let bytes = u64::from(entry.location.record_len());
        let counter = &self.inner.region_reinserted_bytes[entry.location.region_id() as usize];
        atomic_saturating_add(counter, bytes);
        if !self.inner.index.mark_second_chance_if(
            hash,
            entry.location,
            entry.seqno,
            entry.namespace_id,
        ) {
            atomic_saturating_sub(counter, bytes);
            return Ok(false);
        }
        self.inner.region_reinsert_pending[entry.location.region_id() as usize]
            .fetch_add(1, Ordering::AcqRel);
        let completed = self.inner.reinsert_completed.load(Ordering::Acquire);
        self.process_reinsert(hash, entry, incarnation, bytes);
        Ok(self.inner.reinsert_completed.load(Ordering::Acquire) > completed)
    }

    pub(crate) fn get_in_with_task_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        context: &TaskContext,
    ) -> Result<Option<Vec<u8>>> {
        self.get_in_with_context(namespace, key, Some(context))
    }

    fn get_in_with_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        context: Option<&TaskContext>,
    ) -> Result<Option<Vec<u8>>> {
        let started = Instant::now();
        let result = (|| -> Result<Option<Vec<u8>>> {
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(context_stop_error(context));
            }
            if !self.ensure_readable()? {
                return Ok(None);
            }
            if !self.inner.delegated_policy && !self.inner.policy.namespaces().contains(namespace) {
                self.inner.read_stats.record_miss();
                return Ok(None);
            }
            let _barrier = self.lock_shared_operation()?;
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(context_stop_error(context));
            }
            let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
            if context.is_some_and(TaskContext::is_stopped) {
                return Err(context_stop_error(context));
            }
            let admitted = if context.is_some() {
                self.inner.resources.try_begin_read()
            } else {
                self.inner.resources.begin_read()
            };
            let request = match admitted {
                Ok(request) => request,
                Err(reason) => {
                    if context.is_some_and(TaskContext::is_stopped) {
                        return Err(context_stop_error(context));
                    }
                    // Lifecycle can change while admission waits. Terminal state
                    // wins over a simultaneous overload.
                    return match self.runtime_status() {
                        CacheStatus::Healthy => Err(CacheError::Overloaded(reason)),
                        CacheStatus::MissOnly => {
                            self.inner.read_stats.record_miss();
                            Ok(None)
                        }
                        CacheStatus::Poisoned => Err(CacheError::Poisoned),
                        CacheStatus::Closed => Err(CacheError::Closed),
                    };
                }
            };
            if context.is_some_and(TaskContext::is_stopped) {
                drop(request);
                return Err(context_stop_error(context));
            }
            match self.runtime_status() {
                CacheStatus::Healthy => {}
                CacheStatus::MissOnly => {
                    self.inner.read_stats.record_miss();
                    return Ok(None);
                }
                CacheStatus::Poisoned => return Err(CacheError::Poisoned),
                CacheStatus::Closed => return Err(CacheError::Closed),
            }
            let superblock = self.read_superblock()?;
            let Some(entry) = self.inner.index.get(hash, superblock.epoch_start_seqno) else {
                if !self.inner.delegated_policy {
                    self.inner.policy.admission().observe(hash);
                }
                self.inner.read_stats.record_miss();
                return Ok(None);
            };
            if entry.namespace_id != namespace || entry.location.is_tombstone() {
                if !self.inner.delegated_policy {
                    self.inner.policy.admission().observe(hash);
                }
                self.inner.read_stats.record_miss();
                return Ok(None);
            }
            let Some(_) = self
                .inner
                .read_view
                .regions
                .get(entry.location.region_id() as usize)
            else {
                if let Some(context) = context {
                    if !context.try_commit() {
                        return Err(context_stop_error(Some(context)));
                    }
                }
                self.mark_dirty_for_managed_retirement()?;
                let _ = self.remove_index_entry_accounted(hash, entry)?;
                self.inner.read_stats.record_corrupt_miss();
                return Ok(None);
            };
            // Pin only the addressed Region across I/O. Rotation can continue
            // to update unrelated Regions and unrelated reads never share a
            // disk-latency-sized global lock.
            let region_guard = self.lock_read_region(entry.location.region_id())?;
            let region = *region_guard;
            let snapshot = ReadSnapshot { superblock, region };

            let (_permit, mut buffer) = request.into_parts();
            let record_len = entry.location.record_len() as usize;
            buffer
                .prepare(record_len)
                .map_err(|()| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
            let (loaded, buffer) =
                match self.load_entry(snapshot, entry, namespace, key, buffer, context) {
                    Ok(loaded) => loaded,
                    Err(error) => {
                        drop(region_guard);
                        let mut state = self.lock_state()?;
                        self.enter_failure_state(&mut state, &error);
                        return Err(error);
                    }
                };
            #[cfg(test)]
            self.observe_schedule(SchedulePoint::ReadCompleted);
            if let LoadedRecord::Unavailable(_) = &loaded {
                drop(region_guard);
                let mut state = self.lock_state()?;
                self.enter_miss_only(&mut state);
                self.inner.read_stats.record_miss();
                return Ok(None);
            }
            if matches!(loaded, LoadedRecord::Cancelled) {
                drop(region_guard);
                return Err(context_stop_error(context));
            }

            match self.runtime_status() {
                CacheStatus::Healthy => {}
                CacheStatus::MissOnly => {
                    self.inner.read_stats.record_miss();
                    return Ok(None);
                }
                CacheStatus::Poisoned => return Err(CacheError::Poisoned),
                CacheStatus::Closed => return Err(CacheError::Closed),
            }
            let current_superblock = self.read_superblock()?;
            let same_region = region_guard.header.state != RegionState::Free
                && region_guard.header.incarnation == snapshot.region.header.incarnation;
            if current_superblock.epoch != snapshot.superblock.epoch
                || current_superblock.epoch_start_seqno != snapshot.superblock.epoch_start_seqno
                || !same_region
                || self
                    .inner
                    .index
                    .get(hash, current_superblock.epoch_start_seqno)
                    != Some(entry)
            {
                self.inner.read_stats.record_miss();
                return Ok(None);
            }
            match loaded {
                LoadedRecord::Value { start, len } => {
                    let mut value = Vec::new();
                    if value.try_reserve_exact(len).is_err() {
                        self.inner.resources.record_read_buffer_rejection();
                        return Err(CacheError::Overloaded(
                            OverloadReason::ReadBufferUnavailable,
                        ));
                    }
                    let encoded = buffer.prepared(record_len).map_err(|()| {
                        CacheError::CorruptMetadata("read completion lost its buffer")
                    })?;
                    value.extend_from_slice(&encoded[start..start + len]);
                    self.inner.read_stats.record_hit(value.len() as u64);
                    if !self.inner.delegated_policy {
                        self.inner.policy.admission().observe(hash);
                    }
                    self.schedule_second_chance(hash, entry, snapshot.region);
                    Ok(Some(value))
                }
                LoadedRecord::KeyMismatch | LoadedRecord::Tombstone => {
                    if !self.inner.delegated_policy {
                        self.inner.policy.admission().observe(hash);
                    }
                    self.inner.read_stats.record_miss();
                    Ok(None)
                }
                LoadedRecord::Expired => {
                    drop(region_guard);
                    if let Some(context) = context {
                        if !context.try_commit() {
                            return Err(context_stop_error(Some(context)));
                        }
                    }
                    self.mark_dirty_for_managed_retirement()?;
                    let _ = self.remove_index_entry_accounted(hash, entry)?;
                    if !self.inner.delegated_policy {
                        self.inner.policy.admission().observe(hash);
                    }
                    self.inner.read_stats.record_miss();
                    Ok(None)
                }
                LoadedRecord::Corrupt => {
                    drop(region_guard);
                    if let Some(context) = context {
                        if !context.try_commit() {
                            return Err(context_stop_error(Some(context)));
                        }
                    }
                    self.mark_dirty_for_managed_retirement()?;
                    let _ = self.remove_index_entry_accounted(hash, entry)?;
                    if !self.inner.delegated_policy {
                        self.inner.policy.admission().observe(hash);
                    }
                    self.inner.read_stats.record_corrupt_miss();
                    Ok(None)
                }
                LoadedRecord::Unavailable(_) | LoadedRecord::Cancelled => {
                    unreachable!("handled before revalidation")
                }
            }
        })();
        self.record_operation(
            CacheOperation::Get,
            &result,
            |value| {
                if value.is_some() {
                    RequestResultClass::Hit
                } else {
                    RequestResultClass::Miss
                }
            },
            started.elapsed(),
        );
        result
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        self.put_in(0, key, value, options)
    }

    /// Store a value in one configured namespace.
    pub fn put_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        self.put_in_with_source(
            namespace,
            key.as_ref(),
            value.as_ref(),
            options,
            PutSource::Foreground,
            None,
        )
        .map(|receipt| receipt.outcome)
    }

    #[allow(dead_code)]
    pub(crate) fn put_in_managed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Result<RegionPutReceipt> {
        self.put_in_with_source(
            namespace,
            key,
            value,
            options,
            PutSource::ManagedForeground,
            None,
        )
    }

    pub(crate) fn put_in_managed_with_commit(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        commit: ManagedPutCommit,
    ) -> Result<RegionPutReceipt> {
        self.put_in_with_source(
            namespace,
            key,
            value,
            options,
            PutSource::ManagedForeground,
            Some(commit),
        )
    }

    fn put_in_with_source(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        source: PutSource,
        mut managed_commit: Option<ManagedPutCommit>,
    ) -> Result<RegionPutReceipt> {
        let started = Instant::now();
        let result = (|| -> Result<RegionPutReceipt> {
            self.ensure_accepting()?;
            let _barrier = self.lock_shared_operation()?;
            let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
            let _key_order = self.inner.key_ordering.lock(hash);
            let (codec, encoded_key_len, expires_at, record_len, is_update) = {
                let mut state = self.lock_state()?;
                ensure_operational(&state)?;
                if !self.inner.delegated_policy
                    && !self.inner.policy.namespaces().contains(namespace)
                {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::NamespaceNotConfigured,
                    )));
                }
                if !self.inner.delegated_policy && self.inner.policy.should_reject_put() {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::DeviceHealth,
                    )));
                }
                if key.len() > self.inner.config.max_key_size {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::KeyTooLarge,
                    )));
                }
                if value.len() > self.inner.config.max_value_size {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::ValueTooLarge,
                    )));
                }
                let codec = record_codec(namespace);
                let Some(encoded_key_len) = encoded_key_len(namespace, key.len()) else {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::KeyTooLarge,
                    )));
                };
                if encoded_key_len > MAX_KEY_SIZE {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::KeyTooLarge,
                    )));
                }
                let expires_at = match options.expires_at_unix_ms {
                    None => 0,
                    Some(expires_at) if expires_at <= now_unix_ms() => {
                        state.stats.rejected = state.stats.rejected.saturating_add(1);
                        return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                            RejectReason::AlreadyExpired,
                        )));
                    }
                    Some(expires_at) => expires_at,
                };
                let Some(record_len) = RecordHeader::aligned_len(encoded_key_len, value.len())
                else {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::RecordTooLarge,
                    )));
                };
                if u64::from(record_len) > self.inner.config.region_size - REGION_HEADER_SIZE as u64
                {
                    state.stats.rejected = state.stats.rejected.saturating_add(1);
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::RecordTooLarge,
                    )));
                }
                let is_update = self
                    .inner
                    .index
                    .get(hash, state.superblock.epoch_start_seqno)
                    .is_some_and(|entry| {
                        entry.namespace_id == namespace && !entry.location.is_tombstone()
                    });
                (codec, encoded_key_len, expires_at, record_len, is_update)
            };

            if !self.inner.delegated_policy
                && self
                    .inner
                    .policy
                    .admission()
                    .consider(hash, value.len(), is_update)
                    == AdmissionDecision::Reject
            {
                let mut state = self.lock_state()?;
                ensure_operational(&state)?;
                state.stats.rejected = state.stats.rejected.saturating_add(1);
                let reason = if value.len() > crate::policy::LARGE_OBJECT_THRESHOLD_BYTES {
                    RejectReason::LargeObjectCold
                } else {
                    RejectReason::AdmissionFiltered
                };
                return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(reason)));
            }

            let mut request = match self.inner.resources.begin_write() {
                Ok(request) => request,
                Err(reason) => {
                    let state = self.lock_state()?;
                    ensure_operational(&state)?;
                    self.inner.resources.record_put_rejection();
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        put_reject_reason(reason),
                    )));
                }
            };
            let encoded = match request.buffer.prepare(record_len as usize) {
                Ok(encoded) => encoded,
                Err(()) => {
                    let mut state = self.lock_state()?;
                    ensure_operational(&state)?;
                    state.stats.rejected += 1;
                    return Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(
                        RejectReason::BufferUnavailable,
                    )));
                }
            };
            let key_start = RECORD_HEADER_SIZE;
            let value_start = key_start + encoded_key_len;
            encode_namespaced_key(&mut encoded[key_start..value_start], namespace, key)
                .map_err(|()| CacheError::CorruptMetadata("namespace key encoding failed"))?;
            encoded[value_start..value_start + value.len()].copy_from_slice(value);

            self.submit_append(hash, |completion| AppendCommand::Put {
                hash,
                namespace_id: namespace,
                codec,
                key_len: encoded_key_len,
                value_len: value.len(),
                expires_at,
                record_len,
                source,
                managed_commit: managed_commit.take(),
                resources: request,
                completion,
            })
        })();
        self.record_operation(
            CacheOperation::Put,
            &result,
            |receipt| match receipt.outcome {
                PutOutcome::Stored => RequestResultClass::Stored,
                PutOutcome::Rejected(_) => RequestResultClass::Rejected,
            },
            started.elapsed(),
        );
        result
    }

    pub fn remove(&self, key: &[u8]) -> Result<RemoveOutcome> {
        self.remove_in(0, key)
    }

    /// Delete a key in one configured namespace.
    pub fn remove_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<RemoveOutcome> {
        self.remove_in_with_mode(namespace, key, false)
            .map(|receipt| receipt.outcome)
    }

    pub(crate) fn remove_in_managed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<RegionRemoveReceipt> {
        self.remove_in_with_mode(namespace, key, true)
    }

    fn remove_in_with_mode(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        managed: bool,
    ) -> Result<RegionRemoveReceipt> {
        let started = Instant::now();
        let result = (|| -> Result<RegionRemoveReceipt> {
            self.ensure_accepting()?;
            let _barrier = self.lock_shared_operation()?;
            let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
            let _key_order = self.inner.key_ordering.lock(hash);
            let (codec, encoded_key_len, record_len) = {
                let state = self.lock_state()?;
                ensure_operational(&state)?;
                if key.len() > MAX_KEY_SIZE {
                    return Ok(RegionRemoveReceipt {
                        outcome: RemoveOutcome::NotFound,
                        previous_usage: None,
                    });
                }
                let Some(encoded_key_len) = encoded_key_len(namespace, key.len()) else {
                    return Ok(RegionRemoveReceipt {
                        outcome: RemoveOutcome::NotFound,
                        previous_usage: None,
                    });
                };
                if encoded_key_len > MAX_KEY_SIZE {
                    return Ok(RegionRemoveReceipt {
                        outcome: RemoveOutcome::NotFound,
                        previous_usage: None,
                    });
                }
                let Some(record_len) = RecordHeader::aligned_len(encoded_key_len, 0) else {
                    return Ok(RegionRemoveReceipt {
                        outcome: RemoveOutcome::NotFound,
                        previous_usage: None,
                    });
                };
                if u64::from(record_len) > self.inner.config.region_size - REGION_HEADER_SIZE as u64
                {
                    return Ok(RegionRemoveReceipt {
                        outcome: RemoveOutcome::NotFound,
                        previous_usage: None,
                    });
                }
                (record_codec(namespace), encoded_key_len, record_len)
            };
            let mut request = match self.inner.resources.begin_remove() {
                Ok(request) => request,
                Err(reason) => return self.operational_overload(reason),
            };
            let encoded = request
                .record
                .prepare(record_len as usize)
                .map_err(|()| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
            let key_start = RECORD_HEADER_SIZE;
            encode_namespaced_key(
                &mut encoded[key_start..key_start + encoded_key_len],
                namespace,
                key,
            )
            .map_err(|()| CacheError::CorruptMetadata("namespace key encoding failed"))?;

            self.submit_append(hash, |completion| AppendCommand::Remove {
                hash,
                namespace_id: namespace,
                codec,
                key_len: encoded_key_len,
                record_len,
                managed,
                resources: request,
                completion,
            })
        })();
        self.record_operation(
            CacheOperation::Remove,
            &result,
            |receipt| match receipt.outcome {
                RemoveOutcome::Removed => RequestResultClass::Removed,
                RemoveOutcome::NotFound => RequestResultClass::NotFound,
            },
            started.elapsed(),
        );
        result
    }

    /// Persist a clean recovery checkpoint for all completed mutations.
    pub fn flush(&self) -> Result<()> {
        let started = Instant::now();
        let result = self.with_mutations_frozen_after_flush(|| Ok(()));
        self.record_operation(
            CacheOperation::Flush,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    /// Fence Region mutations, then keep the exclusive operation barrier held
    /// while a composing driver publishes its own global clean checkpoint.
    pub(crate) fn with_mutations_frozen_after_flush<T>(
        &self,
        after_flush: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        self.ensure_accepting()?;
        let _barrier = self.lock_exclusive_operation()?;
        {
            let state = self.lock_state()?;
            ensure_operational(&state)?;
        }
        let permit = match self.inner.resources.begin_write_control() {
            Ok(permit) => permit,
            Err(reason) => return self.operational_overload(reason),
        };
        let result = self.flush_on_append_lane();
        drop(permit);
        result?;
        after_flush()
    }

    pub fn clear(&self) -> Result<()> {
        let started = Instant::now();
        let result = (|| -> Result<()> {
            self.ensure_accepting()?;
            let _barrier = self.lock_exclusive_operation()?;
            {
                let state = self.lock_state()?;
                ensure_operational(&state)?;
            }
            let _permit = match self.inner.resources.begin_write_control() {
                Ok(permit) => permit,
                Err(reason) => return self.operational_overload(reason),
            };
            let result = self.clear_on_append_lane();
            drop(_permit);
            result
        })();
        self.record_operation(
            CacheOperation::Clear,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    /// Test-only durability fence retained for the Region crash harness. The
    /// production Hybrid hot path no longer rotates a per-mutation journal or
    /// calls this operation.
    #[cfg(test)]
    pub(crate) fn sync_mutations_for_hybrid(&self) -> Result<()> {
        self.ensure_accepting()?;
        let _barrier = self.lock_exclusive_operation()?;
        let permit = match self.inner.resources.begin_write_control() {
            Ok(permit) => permit,
            Err(reason) => return self.operational_overload(reason),
        };
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        if !state.superblock.clean {
            let result = self.persist_active_headers(&mut state).and_then(|()| {
                self.engine_sync(SyncPoint::CheckpointData, SyncMode::Data)
                    .map_err(Into::into)
            });
            if let Err(error) = &result {
                self.enter_failure_state(&mut state, error);
            }
            result?;
        }
        drop(state);
        drop(permit);
        Ok(())
    }

    /// Publish a clean checkpoint and end this instance's service lifetime.
    ///
    /// This is idempotent and releases the writer lock without waiting for all
    /// clones to be dropped. A poisoned instance skips clean publication but
    /// still attempts to release the lock.
    pub fn close(&self) -> Result<()> {
        let started = Instant::now();
        let coordinated = {
            let shared = self
                .inner
                .async_handle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(runtime) = shared.as_ref() {
                // Fence facade admission while sync admission remains open for
                // work that was already accepted into the facade queues.
                let owner = runtime.begin_close();
                Some((Arc::clone(runtime), owner))
            } else {
                // Holding the registry lock closes the construction race with
                // `async_handle`: no executor can appear after this store.
                self.inner.accepting.store(false, Ordering::Release);
                None
            }
        };
        let result = match coordinated {
            Some((runtime, true)) => {
                let worker_panicked = runtime.drain_and_join();
                self.close_after_async_drain(&runtime, worker_panicked)
            }
            Some((runtime, false)) => runtime.wait_close_result(),
            None => self.close_storage(),
        };
        self.record_operation(
            CacheOperation::Close,
            &result,
            |_| RequestResultClass::Success,
            started.elapsed(),
        );
        result
    }

    /// Stop workers, fence in-flight I/O, and release the file lock while
    /// deliberately leaving the on-disk Superblock dirty. This is reserved
    /// for composing engines whose own durable checkpoint fence failed.
    pub(crate) fn close_without_checkpoint(&self) -> Result<()> {
        self.poison_runtime();
        self.close()
    }

    /// Finish the physical close after the async facade has fenced admission
    /// and drained its accepted work. This entry point avoids recursively
    /// invoking the public close coordinator from its own close thread.
    pub(crate) fn close_after_async_drain(
        &self,
        runtime: &AsyncInner,
        worker_panicked: bool,
    ) -> Result<()> {
        let result = self.close_storage();
        runtime.finish_close(result.is_ok() && !worker_panicked);
        if worker_panicked && result.is_ok() {
            Err(CacheError::Poisoned)
        } else {
            result
        }
    }

    fn close_storage(&self) -> Result<()> {
        // Stop new admission before waiting for the exclusive barrier. Calls
        // that already hold the shared side are accepted and drain normally.
        self.inner.accepting.store(false, Ordering::Release);
        self.inner.recovery_cancel.store(true, Ordering::Release);
        let maintenance_failed = self.stop_and_join_maintenance_worker();
        let recovery_failed = self.stop_and_join_recovery_worker();
        let mut mutex_poisoned = false;
        let _barrier = match self.inner.operation_barrier.write() {
            Ok(barrier) => barrier,
            Err(poisoned) => {
                mutex_poisoned = true;
                poisoned.into_inner()
            }
        };
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                mutex_poisoned = true;
                poisoned.into_inner()
            }
        };
        if mutex_poisoned {
            // The protected state may be only partially updated. Make that
            // terminal, skip checkpoint publication, and still release flock.
            state.status = CacheStatus::Poisoned;
            state.index.clear();
            self.set_lifecycle(CacheStatus::Poisoned);
            self.inner.operation_barrier.clear_poison();
            self.inner.state.clear_poison();
        }
        let already_closed = state.status == CacheStatus::Closed;
        drop(state);

        // The exclusive operation barrier proves every accepted mutation has
        // received its lane completion and no get can enqueue another
        // second-chance candidate. Stop all now-idle workers before the final
        // checkpoint so none can touch the file after publication.
        let reinsert_failed = self.stop_and_join_reinsert_worker();
        let workers_failed = self.stop_and_join_append_workers()
            || maintenance_failed
            || recovery_failed
            || reinsert_failed;
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let checkpoint_result = if already_closed {
            self.set_lifecycle(CacheStatus::Closed);
            Ok(())
        } else {
            let prior_status = state.status;
            state.status = CacheStatus::Closed;
            self.set_lifecycle(CacheStatus::Closed);
            if workers_failed {
                state.index.clear();
                Err(CacheError::Poisoned)
            } else {
                match prior_status {
                    CacheStatus::Healthy => self.checkpoint_clean(&mut state),
                    CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
                    CacheStatus::Closed => Ok(()),
                }
            }
        };
        drop(state);

        let shutdown_result = self.inner.engine.shutdown().map_err(CacheError::Io);
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let unlock_result = self.release_file_lock(&mut state);
        self.inner.state.clear_poison();
        if workers_failed {
            return Err(CacheError::Poisoned);
        }
        checkpoint_result?;
        shutdown_result?;
        unlock_result
    }

    pub fn stats(&self) -> CacheStats {
        let state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut stats = state.stats;
        stats.entries = self
            .inner
            .index
            .value_len(state.superblock.epoch_start_seqno) as u64;
        let mut minimum_ratio = None;
        for region in &state.regions {
            if region.header.state == RegionState::Free {
                continue;
            }
            let used = region.used.saturating_sub(REGION_HEADER_SIZE as u64);
            let valid = self
                .inner
                .region_valid_bytes
                .get(region.header.region_id as usize)
                .map_or(0, |counter| counter.load(Ordering::Acquire));
            stats.region_used_bytes = stats.region_used_bytes.saturating_add(used);
            stats.region_valid_bytes = stats.region_valid_bytes.saturating_add(valid);
            let ratio = ratio_bps(valid, used);
            minimum_ratio = Some(minimum_ratio.map_or(ratio, |current: u64| current.min(ratio)));
        }
        stats.minimum_region_valid_ratio_bps = minimum_ratio.unwrap_or(0);
        drop(state);
        let read_stats = self.inner.read_stats.snapshot();
        stats.hits = stats.hits.saturating_add(read_stats.hits);
        stats.misses = stats.misses.saturating_add(read_stats.misses);
        stats.bytes_read = stats.bytes_read.saturating_add(read_stats.bytes_read);
        stats.corrupt_records = stats
            .corrupt_records
            .saturating_add(read_stats.corrupt_records);
        let resources = self.inner.resources.snapshot();
        stats.rejected = stats.rejected.saturating_add(resources.put_rejections);
        stats.read_queue_depth = resources.read_queue_depth;
        stats.write_queue_depth = resources.write_queue_depth;
        stats.control_queue_depth = resources.control_queue_depth;
        stats.read_queue_depth_peak = resources.read_queue_depth_peak;
        stats.write_queue_depth_peak = resources.write_queue_depth_peak;
        stats.control_queue_depth_peak = resources.control_queue_depth_peak;
        stats.read_buffers_in_use = resources.read_buffers_in_use;
        stats.write_buffers_in_use = resources.write_buffers_in_use;
        stats.control_buffers_in_use = resources.control_buffers_in_use;
        stats.metadata_buffers_in_use = resources.metadata_buffers_in_use;
        stats.read_buffers_in_use_peak = resources.read_buffers_in_use_peak;
        stats.write_buffers_in_use_peak = resources.write_buffers_in_use_peak;
        stats.control_buffers_in_use_peak = resources.control_buffers_in_use_peak;
        stats.metadata_buffers_in_use_peak = resources.metadata_buffers_in_use_peak;
        stats.queue_rejections = resources.queue_rejections;
        stats.buffer_rejections = resources.buffer_rejections;
        stats.write_budget_rejections = resources.write_budget_rejections;
        stats.backpressure_wait_ns = resources.backpressure_wait_ns;
        stats.memory_budget_bytes = resources.memory_budget_bytes;
        stats.memory_used_bytes = resources.memory_used_bytes;
        stats.memory_peak_bytes = resources.memory_peak_bytes;
        let async_stats = self
            .inner
            .async_handle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .map(|runtime| runtime.queue_stats())
            .unwrap_or_default();
        stats.async_read_queued = async_stats.read_queued;
        stats.async_read_in_flight = async_stats.read_in_flight;
        stats.async_read_reserved = async_stats.read_reserved;
        stats.async_mutation_queued = async_stats.mutation_queued;
        stats.async_mutation_in_flight = async_stats.mutation_in_flight;
        stats.async_mutation_reserved = async_stats.mutation_reserved;
        stats.async_ordinary_mutation_queued = async_stats.ordinary_mutation_queued;
        stats.async_control_mutation_queued = async_stats.control_mutation_queued;
        stats.async_read_queue_capacity = async_stats.read_queue_capacity;
        stats.async_write_queue_capacity = async_stats.write_queue_capacity;
        stats.async_control_queue_reserve = async_stats.control_queue_reserve;
        stats.async_queue_rejections = async_stats.queue_rejections;
        let io = self.inner.engine.stats();
        stats.io_queue_depth_configured = self.inner.engine.queue_depth() as u64;
        stats.io_in_flight = io.in_flight.max(self.inner.engine.in_flight() as u64);
        stats.io_in_flight_peak = io.in_flight_peak;
        stats.io_submitted = io.submitted;
        stats.io_completed = io.completed;
        stats.io_cancel_requested = io.cancel_requested;
        stats.io_cancelled = io.cancelled;
        stats.io_errors = io.errors;
        stats.io_submit_wait_ns = io.submit_wait_ns;
        stats.io_completion_ns = io.completion_ns;
        stats.direct_io_operations = io.direct_operations;
        stats.direct_io_bytes = io.direct_bytes;
        stats.buffered_io_operations = io.buffered_operations;
        stats.buffered_io_bytes = io.buffered_bytes;
        stats.io_uring_active = self.inner.engine.kind() == EngineKind::IoUring;
        stats.direct_io_active = self.inner.engine.direct_active();
        stats.io_unfenced_mutations = self.inner.engine.has_unfenced_mutations();
        stats.recovery_regions_completed = stats
            .recovery_regions_completed
            .max(self.inner.recovery_regions_done.load(Ordering::Acquire));
        let recovery_total = self.inner.recovery_regions_total.load(Ordering::Acquire);
        if recovery_total != 0 {
            stats.recovery_regions_total = recovery_total;
        }
        stats.recovery_in_progress = self.inner.recovery_active.load(Ordering::Acquire);
        let admission = self.inner.policy.admission().snapshot();
        stats.admission_observations = admission.observations;
        stats.admission_rejections = admission.rejected;
        stats.large_object_rejections = admission.large_object_rejected;
        let (capacity_rejections, namespace_write_rejections) =
            self.inner.policy.namespaces().rejection_totals();
        stats.namespace_capacity_rejections = capacity_rejections;
        stats.namespace_write_budget_rejections = namespace_write_rejections;
        let host = self.inner.policy.host_writes().snapshot();
        stats.host_write_operations = host.host_write_operations;
        stats.host_write_bytes = host.host_write_bytes;
        stats.foreground_record_bytes = host.foreground_record_bytes;
        stats.reinsertion_bytes = host.reinsertion_bytes;
        stats.metadata_write_bytes = host.metadata_bytes;
        stats.checkpoint_write_bytes = host.checkpoint_bytes;
        stats.admitted_value_bytes = host.admitted_value_bytes;
        stats.write_amplification_milli = host.write_amplification_milli;
        stats.daily_host_write_bytes = host.daily_host_write_bytes;
        stats.daily_write_budget_rejections = host.daily_budget_rejections;
        stats.reinsert_queued = self.inner.reinsert_queued.load(Ordering::Relaxed);
        stats.reinsert_dropped = self.inner.reinsert_dropped.load(Ordering::Relaxed);
        stats.reinsert_stale = self.inner.reinsert_stale.load(Ordering::Relaxed);
        stats.reinsert_completed = self.inner.reinsert_completed.load(Ordering::Relaxed);
        stats.reclaim_records_scanned = self.inner.reclaim_records_scanned.load(Ordering::Relaxed);
        stats.reclaim_index_fallbacks = self.inner.reclaim_index_fallbacks.load(Ordering::Relaxed);
        if let Some(health) = self.inner.policy.nvme_health() {
            stats.nvme_health_observations = health.observations;
            stats.nvme_health_critical = health.critical;
        }
        stats
    }

    /// Snapshot fixed-cardinality counters, request latency histograms, and
    /// the bounded lifecycle event history.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        let status = self.status();
        self.inner
            .telemetry
            .snapshot(status, self.stats(), self.origin_fill_stats())
    }

    /// Write a complete OpenMetrics exposition without an exporter thread.
    pub fn write_openmetrics(&self, output: &mut impl fmt::Write) -> fmt::Result {
        self.metrics_snapshot().write_openmetrics(output)
    }

    /// Acquire one bounded permit before loading a cache miss from the origin.
    pub fn try_begin_origin_fill(
        &self,
    ) -> std::result::Result<OriginFillPermit, OriginFillRejectReason> {
        self.inner
            .origin_fill_limiter
            .as_ref()
            .ok_or(OriginFillRejectReason::Disabled)?
            .try_acquire()
    }

    pub fn origin_fill_stats(&self) -> OriginFillStats {
        self.inner
            .origin_fill_limiter
            .as_ref()
            .map_or_else(OriginFillStats::default, |limiter| limiter.snapshot())
    }

    pub fn health_snapshot(&self) -> HealthSnapshot {
        let status = self.status();
        let stats = self.stats();
        HealthSnapshot {
            status,
            ready: status == CacheStatus::Healthy && !stats.recovery_in_progress,
            recovery_in_progress: stats.recovery_in_progress,
            io_errors: stats.io_errors,
            checkpoint_errors: stats.checkpoint_errors,
            corrupt_records: stats.corrupt_records,
            reclaim_backlog_rejections: stats.reclaim_backlog_rejections,
            nvme_health_critical: stats.nvme_health_critical,
            origin_fills: self.origin_fill_stats(),
        }
    }

    /// Diagnostics for the path actually opened by this instance.
    pub fn startup_diagnostics(&self) -> StartupDiagnostics {
        let stats = self.stats();
        StartupDiagnostics {
            path: self.inner.config.path.clone(),
            status: self.status(),
            recovered_entries: stats.recovered_entries,
            checkpoint_loaded: stats.checkpoint_loads != 0,
            checkpoint_fallbacks: stats.checkpoint_fallbacks,
            recovery_regions_scanned: stats.recovery_regions_scanned,
            recovery_records_scanned: stats.recovery_records_scanned,
            recovery_elapsed_us: stats.recovery_elapsed_us,
            recovery_in_progress: stats.recovery_in_progress,
            io_uring_active: stats.io_uring_active,
            direct_io_active: stats.direct_io_active,
            configured_io_engine: self.inner.config.io_engine,
            configured_io_mode: self.inner.config.io_mode,
        }
    }

    pub fn host_write_stats(&self) -> HostWriteSnapshot {
        self.inner.policy.host_writes().snapshot()
    }

    pub fn namespace_stats(&self, namespace: NamespaceId) -> Option<NamespaceSnapshot> {
        self.inner.policy.namespaces().snapshot(namespace)
    }

    pub fn namespace_snapshots(&self) -> Result<Vec<NamespaceSnapshot>> {
        self.inner
            .policy
            .namespaces()
            .try_snapshots()
            .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))
    }

    /// Stream the current logical live-value usage without allocating a
    /// collection proportional to index capacity. The exclusive operation
    /// fence makes the index and its clear floor one coherent snapshot.
    #[allow(dead_code)]
    pub(crate) fn scan_live_usage(&self, mut visit: impl FnMut(NamespaceUsage)) -> Result<usize> {
        let _barrier = self.lock_exclusive_operation()?;
        self.ensure_accepting()?;
        let min_seqno = self.read_superblock()?.epoch_start_seqno;
        let mut values = 0_usize;
        self.inner.index.try_for_each_snapshot_entry(
            min_seqno,
            |_physical_slot, entry| -> Result<()> {
                let location = PackedLocation::try_from_raw(entry.location_raw)
                    .map_err(|_| CacheError::CorruptMetadata("invalid live index location"))?;
                if !location.is_tombstone() {
                    visit(NamespaceUsage {
                        namespace: entry.namespace_id,
                        live_bytes: u64::from(location.record_len()),
                    });
                    values += 1;
                }
                Ok(())
            },
        )?;
        Ok(values)
    }

    pub fn observe_nvme_health(&self, sample: NvmeHealthSample) -> NvmeHealthStats {
        self.inner.policy.observe_nvme_health(sample)
    }

    pub fn nvme_health(&self) -> Option<NvmeHealthStats> {
        self.inner.policy.nvme_health()
    }

    pub fn region_stats(&self) -> Result<Vec<RegionStats>> {
        let state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut output = Vec::new();
        output
            .try_reserve_exact(state.regions.len())
            .map_err(|_| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
        for region in &state.regions {
            let used = region.used.saturating_sub(REGION_HEADER_SIZE as u64);
            let valid = self
                .inner
                .region_valid_bytes
                .get(region.header.region_id as usize)
                .map_or(0, |counter| counter.load(Ordering::Acquire));
            let second_chance_bytes = self
                .inner
                .region_reinserted_bytes
                .get(region.header.region_id as usize)
                .map_or(0, |counter| counter.load(Ordering::Acquire));
            let second_chance_pending_requests = self
                .inner
                .region_reinsert_pending
                .get(region.header.region_id as usize)
                .map_or(0, |counter| counter.load(Ordering::Acquire));
            output.push(RegionStats {
                region_id: region.header.region_id,
                active: region.header.state == RegionState::Active,
                sealed: region.header.state == RegionState::Sealed,
                incarnation: region.header.incarnation,
                used_bytes: used,
                valid_bytes: valid,
                valid_ratio_bps: ratio_bps(valid, used),
                second_chance_bytes,
                second_chance_pending_requests,
            });
        }
        Ok(output)
    }

    pub fn status(&self) -> CacheStatus {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return CacheStatus::Closed;
        }
        self.runtime_status()
    }

    fn runtime_status(&self) -> CacheStatus {
        if self.inner.state.is_poisoned() {
            CacheStatus::Poisoned
        } else {
            decode_cache_status(self.inner.lifecycle.load(Ordering::Acquire))
        }
    }

    fn lock_state(&self) -> Result<MutexGuard<'_, State>> {
        self.inner.state.lock().map_err(|_| CacheError::Poisoned)
    }

    fn ensure_readable(&self) -> Result<bool> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(CacheError::Closed);
        }
        match self.status() {
            CacheStatus::Healthy => Ok(true),
            CacheStatus::MissOnly => {
                self.inner.read_stats.record_miss();
                Ok(false)
            }
            CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Err(CacheError::Closed),
        }
    }

    fn ensure_accepting(&self) -> Result<()> {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return Err(CacheError::Closed);
        }
        match self.status() {
            CacheStatus::Healthy => Ok(()),
            CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Err(CacheError::Closed),
        }
    }

    fn set_lifecycle(&self, status: CacheStatus) {
        let previous =
            decode_cache_status(self.inner.lifecycle.swap(status as u8, Ordering::AcqRel));
        if previous != status {
            let reason = match status {
                CacheStatus::Healthy => StateChangeReason::RecoveryCompleted,
                CacheStatus::MissOnly => StateChangeReason::IoFailure,
                CacheStatus::Poisoned => StateChangeReason::MetadataFailure,
                CacheStatus::Closed => StateChangeReason::Closing,
            };
            self.inner
                .telemetry
                .record_transition(previous, status, reason);
        }
    }

    fn record_operation<T>(
        &self,
        operation: CacheOperation,
        result: &Result<T>,
        success: impl FnOnce(&T) -> RequestResultClass,
        elapsed: std::time::Duration,
    ) {
        let (class, error) = match result {
            Ok(value) => (success(value), None),
            Err(error) => (
                request_result_for_error(error),
                Some(cache_error_class(error)),
            ),
        };
        self.inner
            .telemetry
            .observe(operation, class, error, elapsed);
    }

    fn enter_miss_only(&self, state: &mut State) {
        enter_miss_only(state);
        self.set_lifecycle(state.status);
    }

    fn enter_failure_state(&self, state: &mut State, error: &CacheError) {
        enter_failure_state(state, error);
        self.set_lifecycle(state.status);
    }

    fn fail_append(&self, error: &CacheError) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.enter_failure_state(&mut state, error);
    }

    #[cfg(test)]
    fn observe_schedule(&self, point: SchedulePoint) {
        let observer = self
            .inner
            .schedule_observer
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(observer) = observer {
            observer(point);
        }
    }

    fn read_superblock(&self) -> Result<Superblock> {
        match self.inner.read_view.superblock.read() {
            Ok(superblock) => Ok(*superblock),
            Err(_) => {
                self.poison_runtime();
                Err(CacheError::Poisoned)
            }
        }
    }

    fn lock_read_region(&self, region_id: u32) -> Result<RwLockReadGuard<'_, RegionMeta>> {
        let Some(region) = self.inner.read_view.regions.get(region_id as usize) else {
            return Err(CacheError::CorruptMetadata(
                "index entry region is out of bounds",
            ));
        };
        match region.read() {
            Ok(region) => Ok(region),
            Err(_) => {
                self.poison_runtime();
                Err(CacheError::Poisoned)
            }
        }
    }

    fn publish_read_view(&self, state: &State) {
        *self
            .inner
            .read_view
            .superblock
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state.superblock;
        debug_assert_eq!(self.inner.read_view.regions.len(), state.regions.len());
        for (published, current) in self.inner.read_view.regions.iter().zip(&state.regions) {
            *published
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = *current;
        }
    }

    fn publish_read_epoch(&self, state: &State) {
        *self
            .inner
            .read_view
            .superblock
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state.superblock;
    }

    fn apply_index_accounted(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        min_seqno: u64,
        namespace: NamespaceId,
        flags: u32,
    ) -> ApplyResult {
        let result = self
            .inner
            .index
            .apply_if_newer_with_metadata(hash, location, seqno, min_seqno, namespace, flags);
        if result.applied {
            if let Some(previous) = result.previous {
                self.subtract_region_valid(previous.location);
            }
            self.add_region_valid(location);
        }
        result
    }

    fn remove_index_entry_accounted(&self, hash: u64, expected: IndexEntry) -> Result<bool> {
        let removed = self
            .inner
            .index
            .remove_if_entry(hash, expected.location, expected.seqno);
        if let Some(removed) = removed {
            self.subtract_region_valid(removed.location);
            if !self.record_namespace_retirement(namespace_usage(removed)) {
                let error = CacheError::CorruptMetadata(
                    "managed Region retirement exceeded namespace live usage",
                );
                self.poison_runtime();
                return Err(error);
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn record_namespace_retirement(&self, usage: Option<NamespaceUsage>) -> bool {
        let Some(usage) = usage else {
            return true;
        };
        if self.inner.delegated_policy {
            if let Some(sink) = self.inner.retire_sink.as_ref() {
                return sink(usage);
            }
            false
        } else {
            !self.inner.policy.namespaces().contains(usage.namespace)
                || self.inner.policy.namespaces().record_removal_exact(usage)
        }
    }

    fn mark_dirty_for_managed_retirement(&self) -> Result<()> {
        if !self.inner.delegated_policy {
            return Ok(());
        }
        self.mark_owner_dirty_for_autonomous_mutation()?;
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        self.mark_dirty(&mut state)
    }

    fn mark_owner_dirty_for_autonomous_mutation(&self) -> Result<()> {
        if self.inner.delegated_policy {
            if let Some(owner_dirty) = self.inner.owner_dirty.as_ref() {
                owner_dirty()?;
            }
        }
        Ok(())
    }

    fn record_put_replacement(&self, source: PutSource, applied: ApplyResult) -> Result<()> {
        if source == PutSource::Foreground
            && applied.applied
            && self.inner.delegated_policy
            && !self.record_namespace_retirement(applied.previous.and_then(namespace_usage))
        {
            return Err(CacheError::CorruptMetadata(
                "managed Region replacement exceeded namespace live usage",
            ));
        }
        Ok(())
    }

    fn add_region_valid(&self, location: PackedLocation) {
        if let Some(counter) = self
            .inner
            .region_valid_bytes
            .get(location.region_id() as usize)
        {
            atomic_saturating_add(counter, u64::from(location.record_len()));
        }
    }

    fn subtract_region_valid(&self, location: PackedLocation) {
        if let Some(counter) = self
            .inner
            .region_valid_bytes
            .get(location.region_id() as usize)
        {
            atomic_saturating_sub(counter, u64::from(location.record_len()));
        }
    }

    /// Remove the index identities physically owned by one old Region
    /// incarnation. Work is bounded by the Region contents, not total index
    /// capacity. A malformed header returns `Ok(false)` so the caller can use
    /// the full-table corruption fallback; exact-accounting failures are
    /// returned and must stop Region publication.
    fn scrub_region_index(&self, superblock: &Superblock, region: RegionMeta) -> Result<bool> {
        if region.header.state == RegionState::Free {
            return Ok(true);
        }
        // A per-record positioned read makes a dense small-object Region issue
        // hundreds of thousands of syscalls. Keep one fixed sequential window
        // on the append worker stack and parse every header that falls inside
        // it. Large records simply advance to the next header/window.
        let region_base = match region_base(superblock, region.header.region_id) {
            Ok(base) => base,
            Err(_) => return Ok(false),
        };
        let mut scan = [0_u8; RECLAIM_SCAN_CHUNK_BYTES];
        let mut scan_start = 0_u64;
        let mut scan_len = 0_usize;
        let mut cursor = REGION_HEADER_SIZE as u64;
        while cursor < region.used {
            let scan_end = scan_start.saturating_add(scan_len as u64);
            let header_end = match cursor.checked_add(RECORD_HEADER_SIZE as u64) {
                Some(end) if end <= region.used => end,
                _ => return Ok(false),
            };
            if cursor < scan_start || header_end > scan_end {
                let remaining = match usize::try_from(region.used - cursor) {
                    Ok(remaining) => remaining,
                    Err(_) => return Ok(false),
                };
                scan_len = remaining.min(scan.len());
                let absolute = match region_base.checked_add(cursor) {
                    Some(absolute) => absolute,
                    None => return Ok(false),
                };
                if read_exact_at(self.inner.io.as_ref(), &mut scan[..scan_len], absolute).is_err() {
                    return Ok(false);
                }
                scan_start = cursor;
            }
            let relative = match usize::try_from(cursor - scan_start) {
                Ok(relative) => relative,
                Err(_) => return Ok(false),
            };
            let Some(encoded) = scan.get(relative..relative + RECORD_HEADER_SIZE) else {
                return Ok(false);
            };
            let Some(header) = RecordHeader::decode(encoded) else {
                return Ok(false);
            };
            let end = match cursor
                .checked_add(u64::from(header.record_len))
                .filter(|end| *end <= region.used)
            {
                Some(end) => end,
                None => return Ok(false),
            };
            if header.region_incarnation != region.header.incarnation
                || header.seqno < region.header.created_seqno
            {
                return Ok(false);
            }
            let offset = match u32::try_from(cursor) {
                Ok(offset) => offset,
                Err(_) => return Ok(false),
            };
            let location = match PackedLocation::new(
                region.header.region_id,
                offset,
                header.record_len,
                header.kind == RecordKind::Tombstone,
            ) {
                Ok(location) => location,
                Err(_) => return Ok(false),
            };
            atomic_saturating_add(&self.inner.reclaim_records_scanned, 1);
            let expected = IndexEntry {
                location,
                seqno: header.seqno,
                namespace_id: 0,
                flags: 0,
            };
            match self.remove_index_entry_accounted(header.key_hash, expected) {
                Ok(true) => {}
                Ok(false) => {
                    let _ = self.inner.index.remove_physical_if_entry(
                        header.key_hash,
                        location,
                        header.seqno,
                    );
                }
                Err(error) => return Err(error),
            }
            cursor = end;
        }
        Ok(cursor == region.used)
    }

    fn scrub_or_fallback_region_index(
        &self,
        superblock: &Superblock,
        region: RegionMeta,
        min_seqno: u64,
    ) -> Result<()> {
        if self.scrub_region_index(superblock, region)? {
            return Ok(());
        }
        atomic_saturating_add(&self.inner.reclaim_index_fallbacks, 1);
        let mut accounting_ok = true;
        self.inner
            .index
            .evict_region_with(region.header.region_id, min_seqno, |entry| {
                self.subtract_region_valid(entry.location);
                accounting_ok &= self.record_namespace_retirement(namespace_usage(entry));
            });
        if !accounting_ok {
            self.poison_runtime();
            return Err(CacheError::CorruptMetadata(
                "Region reclaim exceeded namespace live usage",
            ));
        }
        Ok(())
    }

    fn publish_scrubbed_region_generation(&self, header: RegionHeader) -> Result<()> {
        let counts = self
            .inner
            .index
            .invalidate_region_generation(
                header.region_id,
                RegionGeneration::Allocated {
                    created_seqno: header.created_seqno,
                },
            )
            .ok_or(CacheError::CorruptMetadata(
                "reused Region is out of index bounds",
            ))?;
        if counts.entries != 0 || counts.values != 0 {
            return Err(CacheError::CorruptMetadata(
                "Region reclaim left visible index entries",
            ));
        }
        let victim_index = header.region_id as usize;
        let valid_bytes =
            self.inner
                .region_valid_bytes
                .get(victim_index)
                .ok_or(CacheError::CorruptMetadata(
                    "reused Region accounting is out of bounds",
                ))?;
        let reinserted_bytes = self.inner.region_reinserted_bytes.get(victim_index).ok_or(
            CacheError::CorruptMetadata("reused Region reinsertion accounting is out of bounds"),
        )?;
        let reinsert_pending = self.inner.region_reinsert_pending.get(victim_index).ok_or(
            CacheError::CorruptMetadata("reused Region pending accounting is out of bounds"),
        )?;
        if valid_bytes.swap(0, Ordering::AcqRel) != 0 {
            return Err(CacheError::CorruptMetadata(
                "Region reclaim left valid-byte accounting",
            ));
        }
        reinserted_bytes.store(0, Ordering::Release);
        reinsert_pending.store(0, Ordering::Release);
        Ok(())
    }

    fn schedule_second_chance(&self, hash: u64, entry: IndexEntry, region: RegionMeta) {
        if self.inner.config.reclaim_mode != ReclaimMode::SecondChance
            || region.header.state != RegionState::Sealed
            || region.header.region_id != entry.location.region_id()
            || entry.location.record_len() > SECOND_CHANCE_MAX_RECORD_BYTES
            || entry.flags & (INDEX_FLAG_SECOND_CHANCE_PENDING | INDEX_FLAG_SECOND_CHANCE_USED) != 0
        {
            return;
        }
        let Some(counter) = self
            .inner
            .region_reinserted_bytes
            .get(entry.location.region_id() as usize)
        else {
            return;
        };
        let bytes = u64::from(entry.location.record_len());
        let limit = self
            .inner
            .config
            .region_size
            .saturating_sub(REGION_HEADER_SIZE as u64)
            / SECOND_CHANCE_REGION_FRACTION;
        if counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current.checked_add(bytes).filter(|next| *next <= limit)
            })
            .is_err()
        {
            return;
        }
        if !self.inner.index.mark_second_chance_if(
            hash,
            entry.location,
            entry.seqno,
            entry.namespace_id,
        ) {
            atomic_saturating_sub(counter, bytes);
            return;
        }
        let pending = &self.inner.region_reinsert_pending[entry.location.region_id() as usize];
        pending.fetch_add(1, Ordering::AcqRel);
        let command = ReinsertCommand::Candidate {
            hash,
            entry,
            region_incarnation: region.header.incarnation,
            reserved_bytes: bytes,
        };
        match self.inner.reinsert_tx.try_send(command) {
            Ok(()) => {
                self.inner.reinsert_queued.fetch_add(1, Ordering::Relaxed);
            }
            Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                self.inner.index.clear_second_chance_if(
                    hash,
                    entry.location,
                    entry.seqno,
                    entry.namespace_id,
                );
                atomic_saturating_sub(counter, bytes);
                atomic_saturating_sub(pending, 1);
                self.inner.reinsert_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn process_reinsert(
        &self,
        hash: u64,
        entry: IndexEntry,
        region_incarnation: u32,
        reserved_bytes: u64,
    ) {
        let failed = (|| -> Result<PutOutcome> {
            self.ensure_accepting()?;
            let _barrier = self.try_lock_shared_operation()?;
            let current = {
                let state = self.lock_state()?;
                ensure_operational(&state)?;
                self.inner
                    .index
                    .get(hash, state.superblock.epoch_start_seqno)
            };
            if !current.is_some_and(|current| {
                current.location == entry.location
                    && current.seqno == entry.seqno
                    && current.namespace_id == entry.namespace_id
                    && current.flags & INDEX_FLAG_SECOND_CHANCE_PENDING != 0
            }) {
                return Err(CacheError::Cancelled);
            }
            if self.inner.engine.in_flight() >= self.inner.engine.queue_depth() {
                return Err(CacheError::Overloaded(OverloadReason::WriteQueueFull));
            }
            let resources = self
                .inner
                .resources
                .try_begin_write()
                .map_err(CacheError::Overloaded)?;
            let (permit, mut buffer) = resources.into_parts();
            let record_len = entry.location.record_len() as usize;
            buffer
                .prepare(record_len)
                .map_err(|()| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
            let superblock = self.read_superblock()?;
            if self
                .inner
                .read_view
                .regions
                .get(entry.location.region_id() as usize)
                .is_none()
            {
                return Err(CacheError::Cancelled);
            }
            let region_guard = self.lock_read_region(entry.location.region_id())?;
            let region = *region_guard;
            if region.header.state == RegionState::Free
                || region.header.incarnation != region_incarnation
            {
                return Err(CacheError::Cancelled);
            }
            let snapshot = ReadSnapshot { superblock, region };
            let (read_result, completed) = self.engine_read(
                buffer,
                record_len,
                region_base(&snapshot.superblock, entry.location.region_id())?
                    .checked_add(u64::from(entry.location.offset()))
                    .ok_or(CacheError::CorruptMetadata("reinsertion offset overflow"))?,
                None,
            );
            let buffer = match completed {
                Some(buffer) => buffer,
                None => {
                    return Err(match read_result {
                        Err(error) => CacheError::Io(error),
                        Ok(_) => CacheError::CorruptMetadata("reinsertion read lost its buffer"),
                    });
                }
            };
            read_result.map_err(CacheError::Io)?;
            let record = self.validate_reinsert_record(snapshot, entry, hash, &buffer)?;
            drop(region_guard);
            buffer
                .prepared(record.minimum_record_len as usize)
                .map_err(|()| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
            // Only serialize the publication handoff. The potentially slow
            // NVMe read above never holds an ordering shard needed by a
            // foreground key that merely collides in the shard table.
            let key_order = self
                .inner
                .key_ordering
                .try_lock(hash)
                .ok_or(CacheError::Overloaded(OverloadReason::WriteQueueFull))?;
            let current = {
                let state = self.lock_state()?;
                ensure_operational(&state)?;
                self.inner
                    .index
                    .get(hash, state.superblock.epoch_start_seqno)
            };
            if !current.is_some_and(|current| {
                current.location == entry.location
                    && current.seqno == entry.seqno
                    && current.namespace_id == entry.namespace_id
                    && current.flags & INDEX_FLAG_SECOND_CHANCE_PENDING != 0
            }) {
                return Err(CacheError::Cancelled);
            }
            // Reinsertion has no foreground Hybrid journal intent. Fence the
            // owner's usage checkpoint before the append can make a different
            // physical charge durable.
            self.mark_owner_dirty_for_autonomous_mutation()?;
            let resources = DataResources::from_parts(permit, buffer);
            let (completion_tx, completion_rx) = mpsc::sync_channel(1);
            let command = AppendCommand::Put {
                hash,
                namespace_id: entry.namespace_id,
                codec: record.codec.with_second_chance(),
                key_len: record.key_len,
                value_len: record.value_len,
                expires_at: record.expires_at,
                record_len: record.minimum_record_len,
                source: PutSource::Reinsertion,
                managed_commit: None,
                resources,
                completion: completion_tx,
            };
            let lane = (hash as usize) % self.inner.append_txs.len();
            self.inner.append_txs[lane]
                .try_send(command)
                .map_err(|_| CacheError::Overloaded(OverloadReason::WriteQueueFull))?;
            drop(key_order);
            completion_rx
                .recv()
                .map_err(|_| CacheError::Poisoned)?
                .map(|receipt| receipt.outcome)
        })();

        if !matches!(failed, Ok(PutOutcome::Stored)) {
            self.inner.index.clear_second_chance_if(
                hash,
                entry.location,
                entry.seqno,
                entry.namespace_id,
            );
            if let Some(counter) = self
                .inner
                .region_reinserted_bytes
                .get(entry.location.region_id() as usize)
            {
                atomic_saturating_sub(counter, reserved_bytes);
            }
            if matches!(failed, Err(CacheError::Cancelled)) {
                self.inner.reinsert_stale.fetch_add(1, Ordering::Relaxed);
            } else {
                self.inner.reinsert_dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
        if let Some(pending) = self
            .inner
            .region_reinsert_pending
            .get(entry.location.region_id() as usize)
        {
            atomic_saturating_sub(pending, 1);
        }
        self.request_background_reclaim(false);
    }

    fn validate_reinsert_record(
        &self,
        snapshot: ReadSnapshot,
        entry: IndexEntry,
        hash: u64,
        buffer: &BufferLease,
    ) -> Result<ReinsertRecord> {
        let encoded = buffer
            .prepared(entry.location.record_len() as usize)
            .map_err(|()| CacheError::CorruptMetadata("reinsertion buffer length mismatch"))?;
        let header = RecordHeader::decode(&encoded[..RECORD_HEADER_SIZE]).ok_or(
            CacheError::CorruptMetadata("reinsertion record header is invalid"),
        )?;
        if header.kind != RecordKind::Value
            || header.record_len != entry.location.record_len()
            || header.region_incarnation != snapshot.region.header.incarnation
            || header.epoch != snapshot.superblock.epoch
            || header.seqno != entry.seqno
            || header.key_hash != hash
        {
            return Err(CacheError::Cancelled);
        }
        if header.expires_at != 0 && header.expires_at <= now_unix_ms() {
            return Err(CacheError::Cancelled);
        }
        let key_len = header.key_len as usize;
        let value_len = header.stored_len as usize;
        let payload_end = RECORD_HEADER_SIZE
            .checked_add(key_len)
            .and_then(|end| end.checked_add(value_len))
            .filter(|end| *end <= encoded.len())
            .ok_or(CacheError::CorruptMetadata(
                "reinsertion payload length is invalid",
            ))?;
        let payload = &encoded[RECORD_HEADER_SIZE..payload_end];
        let encoded_key = &payload[..key_len];
        if crc32c(payload) != header.payload_crc
            || decode_record_namespace(header.codec, encoded_key) != Some(entry.namespace_id)
            || hash_record_key(snapshot.superblock.hash_seed, header.codec, encoded_key)
                != Some(hash)
        {
            return Err(CacheError::CorruptMetadata(
                "reinsertion payload validation failed",
            ));
        }
        let minimum_record_len = RecordHeader::aligned_len(key_len, value_len).ok_or(
            CacheError::CorruptMetadata("reinsertion record length overflow"),
        )?;
        Ok(ReinsertRecord {
            codec: header.codec,
            key_len,
            value_len,
            expires_at: header.expires_at,
            minimum_record_len,
        })
    }

    fn lock_read_region_for_rotation(&self, region_id: u32) -> RwLockWriteGuard<'_, RegionMeta> {
        let region = &self.inner.read_view.regions[region_id as usize];
        #[cfg(test)]
        match region.try_write() {
            Ok(view) => return view,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => {
                return poisoned.into_inner();
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                // A positive event: rotation attempted the write lock while a
                // reader still pinned the previous region incarnation.
                self.observe_schedule(SchedulePoint::RotateBlockedByReader);
            }
        }
        region
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn lock_shared_operation(&self) -> Result<RwLockReadGuard<'_, ()>> {
        match self.inner.operation_barrier.read() {
            Ok(barrier) => Ok(barrier),
            Err(_) => {
                self.poison_runtime();
                Err(CacheError::Poisoned)
            }
        }
    }

    fn try_lock_shared_operation(&self) -> Result<RwLockReadGuard<'_, ()>> {
        match self.inner.operation_barrier.try_read() {
            Ok(barrier) => Ok(barrier),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.poison_runtime();
                Err(CacheError::Poisoned)
            }
            Err(std::sync::TryLockError::WouldBlock) => Err(CacheError::Cancelled),
        }
    }

    fn lock_exclusive_operation(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        match self.inner.operation_barrier.write() {
            Ok(barrier) => Ok(barrier),
            Err(_) => {
                self.poison_runtime();
                Err(CacheError::Poisoned)
            }
        }
    }

    fn operational_overload<T>(&self, reason: OverloadReason) -> Result<T> {
        let state = self.lock_state()?;
        ensure_operational(&state)?;
        Err(CacheError::Overloaded(reason))
    }

    fn submit_append<T>(
        &self,
        hash: u64,
        command: impl FnOnce(SyncSender<Result<T>>) -> AppendCommand,
    ) -> Result<T> {
        let (completion, completed) = mpsc::sync_channel(1);
        let lane_id = (hash as usize) % self.inner.append_txs.len();
        if self.inner.append_txs[lane_id]
            .send(command(completion))
            .is_err()
        {
            self.mark_append_worker_failed();
            return Err(CacheError::Poisoned);
        }
        match completed.recv() {
            Ok(result) => result,
            Err(_) => {
                self.mark_append_worker_failed();
                Err(CacheError::Poisoned)
            }
        }
    }

    fn mark_append_worker_failed(&self) {
        self.poison_runtime();
    }

    fn poison_runtime(&self) {
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        if state.status != CacheStatus::Closed {
            state.index.clear();
            state.status = CacheStatus::Poisoned;
            self.set_lifecycle(CacheStatus::Poisoned);
        }
    }

    fn stop_and_join_append_workers(&self) -> bool {
        let mut workers = self
            .inner
            .append_workers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if workers.is_empty() {
            return false;
        }
        let mut failed = false;
        for append_tx in &self.inner.append_txs {
            if append_tx.send(AppendCommand::Shutdown).is_err() {
                failed = true;
            }
        }
        for worker in workers.drain(..) {
            failed |= worker.join().is_err();
        }
        failed
    }

    fn stop_and_join_reinsert_worker(&self) -> bool {
        let mut worker = self
            .inner
            .reinsert_worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(worker) = worker.take() else {
            return false;
        };
        let send_failed = self
            .inner
            .reinsert_tx
            .send(ReinsertCommand::Shutdown)
            .is_err();
        send_failed || worker.join().is_err()
    }

    fn stop_and_join_maintenance_worker(&self) -> bool {
        let mut worker = self
            .inner
            .maintenance_worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(worker) = worker.take() else {
            return false;
        };
        let send_failed = self
            .inner
            .maintenance_tx
            .send(MaintenanceCommand::Shutdown)
            .is_err();
        send_failed || worker.join().is_err()
    }

    fn stop_and_join_recovery_worker(&self) -> bool {
        self.inner
            .recovery_worker
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
            .is_some_and(|worker| worker.join().is_err())
    }

    fn request_periodic_checkpoint(&self, bytes: u64) {
        // A composing Hybrid driver owns the global usage checkpoint. If a
        // managed Region made an autonomous reinsertion/reclaim clean by
        // itself, Hybrid could later trust an older namespace snapshot. Only
        // the driver's explicit flush may publish a clean managed Region.
        if self.inner.delegated_policy {
            return;
        }
        let configured = self.inner.config.checkpoint_interval_bytes;
        let interval = if configured == 0 || self.inner.config.checkpoint_interval_explicit {
            configured
        } else {
            let estimated_snapshot = (self.inner.config.index_slots as u64)
                .saturating_mul(CHECKPOINT_INDEX_ENTRY_SIZE as u64);
            configured.max(estimated_snapshot.saturating_mul(DEFAULT_CHECKPOINT_REWRITE_RATIO))
        };
        if interval == 0 || !self.inner.accepting.load(Ordering::Acquire) {
            return;
        }
        let current = self
            .inner
            .checkpoint_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(bytes))
            })
            .unwrap_or_else(|current| current)
            .saturating_add(bytes);
        if current >= interval
            && self
                .inner
                .checkpoint_pending
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            match self
                .inner
                .maintenance_tx
                .try_send(MaintenanceCommand::Checkpoint)
            {
                Ok(()) | Err(TrySendError::Full(_)) => {}
                Err(TrySendError::Disconnected(_)) => self
                    .inner
                    .checkpoint_pending
                    .store(false, Ordering::Release),
            }
        }
    }

    fn request_background_reclaim(&self, force: bool) {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return;
        }
        if self.inner.config.reclaim_mode == ReclaimMode::Fifo
            && !self.inner.reclaim_eligible.load(Ordering::Acquire)
        {
            return;
        }
        if force {
            self.inner.reclaim_forced.store(true, Ordering::Release);
        }
        let _ = self
            .inner
            .maintenance_tx
            .try_send(MaintenanceCommand::Reclaim);
    }

    fn run_background_reclaim(&self) {
        if !self.inner.accepting.load(Ordering::Acquire) {
            return;
        }
        let forced = self.inner.reclaim_forced.swap(false, Ordering::AcqRel);
        let _barrier = match self.lock_shared_operation() {
            Ok(barrier) => barrier,
            Err(_) => return,
        };

        let planned = {
            let mut state = match self.lock_state() {
                Ok(state) => state,
                Err(_) => return,
            };
            if ensure_operational(&state).is_err()
                || state.superblock.clean
                || state.reclaiming_region.is_some()
                || state.reclaim_ready_region.is_some()
                || !state.free_regions.is_empty()
                // FIFO must retain one synchronous victim so background
                // precleaning cannot introduce a new ReclaimBacklog outcome.
                || (self.inner.config.reclaim_mode == ReclaimMode::Fifo
                    && state.sealed_regions.len() <= 1)
            {
                return;
            }
            let usable = self
                .inner
                .config
                .region_size
                .saturating_sub(REGION_HEADER_SIZE as u64);
            let reclaim_threshold = REGION_HEADER_SIZE as u64
                + usable.saturating_mul(RECLAIM_TRIGGER_NUMERATOR) / RECLAIM_TRIGGER_DENOMINATOR;
            if !forced
                && !state
                    .active_regions
                    .iter()
                    .any(|region_id| state.regions[*region_id as usize].used >= reclaim_threshold)
            {
                return;
            }
            let victim = match state.sealed_regions.front().copied() {
                Some(victim) => victim,
                None => return,
            };
            if self.inner.region_reinsert_pending[victim as usize].load(Ordering::Acquire) != 0 {
                return;
            }
            // Reclaim never waits behind a foreground read while holding the
            // State mutex. A later append will request another pass.
            let victim_index = victim as usize;
            let mut read_region = match self.inner.read_view.regions[victim_index].try_write() {
                Ok(view) => view,
                Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                Err(std::sync::TryLockError::WouldBlock) => return,
            };
            let old_region = state.regions[victim_index];
            if state.sealed_regions.pop_front() != Some(victim) {
                let error = CacheError::CorruptMetadata("sealed Region queue is inconsistent");
                self.enter_failure_state(&mut state, &error);
                return;
            }
            let incarnation = match state.regions[victim_index]
                .header
                .incarnation
                .checked_add(1)
            {
                Some(incarnation) => incarnation,
                None => {
                    let error = CacheError::CorruptMetadata("region incarnation overflow");
                    self.enter_failure_state(&mut state, &error);
                    return;
                }
            };
            let created_seqno = match take_seqno(&mut state) {
                Ok(seqno) => seqno,
                Err(error) => {
                    self.enter_failure_state(&mut state, &error);
                    return;
                }
            };
            let header = RegionHeader {
                region_id: victim,
                incarnation,
                state: RegionState::Sealed,
                created_seqno,
                used: REGION_HEADER_SIZE as u64,
            };
            state.reclaiming_region = Some(victim);
            let region = RegionMeta {
                header,
                used: REGION_HEADER_SIZE as u64,
                max_seqno: 0,
            };
            state.regions[victim_index] = region;
            *read_region = region;
            (
                state.superblock,
                header,
                state.superblock.epoch_start_seqno,
                old_region,
            )
        };

        // The new incarnation is already published to readers. Scrub only the
        // old Region's record identities, then advance its generation floor in
        // O(1). Corrupt record headers use the legacy full-index fallback.
        if let Err(error) = self
            .scrub_or_fallback_region_index(&planned.0, planned.3, planned.2)
            .and_then(|()| self.publish_scrubbed_region_generation(planned.1))
        {
            self.finish_background_reclaim(planned.1, Err(error));
            return;
        }

        let result = self
            .write_backend_tracked(
                WritePoint::RegionHeader,
                HostWriteKind::Metadata,
                &planned.1.encode(),
                match region_base(&planned.0, planned.1.region_id) {
                    Ok(offset) => offset,
                    Err(error) => {
                        self.finish_background_reclaim(planned.1, Err(error));
                        return;
                    }
                },
            )
            .and_then(|()| {
                if self.inner.owner_dirty.is_some() {
                    return Ok(());
                }
                sync_backend_tracked(
                    self.inner.io.as_ref(),
                    self.inner.policy.host_writes(),
                    SyncPoint::RegionRotation,
                    SyncMode::Data,
                )
            });
        self.finish_background_reclaim(planned.1, result);
    }

    fn finish_background_reclaim(&self, planned: RegionHeader, result: Result<()>) {
        let mut state = match self.lock_state() {
            Ok(state) => state,
            Err(_) => {
                self.poison_runtime();
                return;
            }
        };
        if state.reclaiming_region != Some(planned.region_id) {
            let error = CacheError::CorruptMetadata("background reclaim state was lost");
            self.enter_failure_state(&mut state, &error);
            return;
        }
        state.reclaiming_region = None;
        match result {
            Ok(()) => {
                let still_planned =
                    state
                        .regions
                        .get(planned.region_id as usize)
                        .is_some_and(|region| {
                            region.header.region_id == planned.region_id
                                && region.header.incarnation == planned.incarnation
                                && region.header.state == RegionState::Sealed
                                && region.header.created_seqno == planned.created_seqno
                                && region.used == REGION_HEADER_SIZE as u64
                                && region.header.used == REGION_HEADER_SIZE as u64
                        });
                if !still_planned {
                    return;
                }
                state.reclaim_ready_region = Some(planned.region_id);
                state.stats.background_regions_reclaimed =
                    state.stats.background_regions_reclaimed.saturating_add(1);
            }
            Err(error) => self.enter_failure_state(&mut state, &error),
        }
    }

    fn run_maintenance_checkpoint(&self) {
        if !self.inner.accepting.load(Ordering::Acquire) {
            self.inner
                .checkpoint_pending
                .store(false, Ordering::Release);
            return;
        }
        // Queue as a real writer so a continuous stream of foreground readers
        // cannot keep the periodic recovery baseline stale forever. `close`
        // fences admission before joining this worker, so accepted operations
        // drain and release this wait without a shutdown cycle.
        let _barrier = match self.inner.operation_barrier.write() {
            Ok(barrier) => barrier,
            Err(_) => {
                self.inner
                    .checkpoint_pending
                    .store(false, Ordering::Release);
                self.poison_runtime();
                return;
            }
        };
        // Drop the pending flag before releasing the operation barrier. A
        // mutation cannot cross this point unnoticed and a new threshold
        // crossing after the barrier receives a fresh wake token.
        let _pending = PendingFlagGuard(&self.inner.checkpoint_pending);
        if !self.inner.accepting.load(Ordering::Acquire) {
            return;
        }
        let _permit = match self.inner.resources.begin_write_control() {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let mut state = match self.inner.state.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                self.enter_failure_state(
                    &mut state,
                    &CacheError::CorruptMetadata("checkpoint state mutex was poisoned"),
                );
                return;
            }
        };
        if state.status != CacheStatus::Healthy {
            return;
        }
        if self.checkpoint_clean(&mut state).is_ok() {
            self.inner.checkpoint_bytes.store(0, Ordering::Release);
        }
    }

    fn release_file_lock(&self, state: &mut State) -> Result<()> {
        if !state.lock_held {
            return Ok(());
        }
        if self.inner.engine.has_unfenced_mutations() {
            // An io_uring target CQE is the only proof that an active write no
            // longer reaches this inode. Keep flock held (the uring runtime
            // retains a duplicate file description) so a new instance cannot
            // race an unfenced old write into freshly formatted metadata.
            return Err(CacheError::Io(io::Error::other(
                "cache lock retained because an io_uring mutation is not fenced",
            )));
        }
        unlock_file(self.inner.io.as_ref())?;
        state.lock_held = false;
        Ok(())
    }

    fn put_batch_on_append_lane(&self, lane_id: usize, commands: Vec<PendingPut>) {
        let mut accepted = VecDeque::with_capacity(commands.len());
        for command in commands {
            if command
                .resources
                .buffer
                .prepared(command.record_len as usize)
                .is_err()
            {
                let error = CacheError::CorruptMetadata("append command lost its prepared buffer");
                self.fail_append(&error);
                let _ = command.completion.send(Err(error));
                continue;
            }
            let operational = match self.lock_state() {
                Ok(state) => match ensure_operational(&state) {
                    Ok(()) => true,
                    Err(error) => {
                        let _ = command.completion.send(Err(error));
                        continue;
                    }
                },
                Err(error) => {
                    let _ = command.completion.send(Err(error));
                    continue;
                }
            };
            if !operational {
                continue;
            }
            let (permit, buffer) = command.resources.into_parts();
            accepted.push_back(PreparedPut {
                hash: command.hash,
                namespace_id: command.namespace_id,
                codec: command.codec,
                key_len: command.key_len,
                value_len: command.value_len,
                expires_at: command.expires_at,
                minimum_record_len: command.record_len,
                source: command.source,
                managed_commit: command.managed_commit,
                _permit: permit,
                buffer: Some(buffer),
                completion: command.completion,
                receipt: None,
            });
        }

        'batches: while !accepted.is_empty() {
            let batch_source = accepted
                .front()
                .map(|put| put.source)
                .expect("non-empty append queue must have a source");
            let mut minimum_lengths = accepted
                .iter()
                .take_while(|put| put.source == batch_source)
                .take(MAX_BATCH_RECORDS)
                .map(|put| put.minimum_record_len)
                .collect::<Vec<_>>();
            let mut align_for_direct = self.direct_append_active();
            let mut plan = loop {
                let plan =
                    match self.preview_batch_on_lane(lane_id, &minimum_lengths, align_for_direct) {
                        Ok(plan) => plan,
                        Err(error) => {
                            complete_puts_with_error(accepted.drain(..), &error);
                            return;
                        }
                    };
                let can_grow = accepted
                    .front_mut()
                    .and_then(|put| put.buffer.as_mut())
                    .is_some_and(|buffer| buffer.grow_preserving(plan.write_len).is_ok());
                if can_grow {
                    break plan;
                }
                if plan.records > 1 {
                    minimum_lengths.truncate(1);
                    continue;
                }
                if align_for_direct {
                    // Coalescing and direct padding are optional. The record's
                    // minimum buffer was already admitted, so preserve service
                    // under a tight budget via the Format V1 buffered path.
                    align_for_direct = false;
                    continue;
                }
                let error = CacheError::CorruptMetadata(
                    "an admitted append buffer cannot hold its original record",
                );
                self.fail_append(&error);
                complete_puts_with_error(accepted.drain(..), &error);
                return;
            };

            match self.lock_state() {
                Ok(state) => match ensure_operational(&state) {
                    Ok(()) => {}
                    Err(error) => {
                        complete_puts_with_error(accepted.drain(..), &error);
                        return;
                    }
                },
                Err(error) => {
                    complete_puts_with_error(accepted.drain(..), &error);
                    return;
                }
            }
            let (capacity_guards, namespace_write_guards) = 'policy: loop {
                let mut capacity_guards = Vec::with_capacity(plan.records);
                let mut namespace_write_guards = Vec::with_capacity(plan.records);
                let mut policy_rejection = None;
                for (index, (put, &record_len)) in accepted
                    .iter()
                    .take(plan.records)
                    .zip(&plan.record_lengths)
                    .enumerate()
                {
                    if self.inner.delegated_policy {
                        if put.source == PutSource::Reinsertion {
                            let Some(namespaces) = self.inner.delegated_namespaces.as_ref() else {
                                policy_rejection =
                                    Some((index, RejectReason::NamespaceNotConfigured));
                                break;
                            };
                            match namespaces
                                .try_reserve_capacity(put.namespace_id, u64::from(record_len))
                            {
                                Ok(guard) => capacity_guards.push(Some(guard)),
                                Err(reason) => {
                                    policy_rejection =
                                        Some((index, namespace_reject_reason(reason)));
                                    break;
                                }
                            }
                        } else {
                            capacity_guards.push(None);
                        }
                        namespace_write_guards.push(None);
                        continue;
                    }
                    match self
                        .inner
                        .policy
                        .namespaces()
                        .try_reserve_capacity(put.namespace_id, u64::from(record_len))
                    {
                        Ok(guard) => capacity_guards.push(Some(guard)),
                        Err(reason) => {
                            policy_rejection = Some((index, namespace_reject_reason(reason)));
                            break;
                        }
                    }
                    if put.source == PutSource::Foreground {
                        match self
                            .inner
                            .policy
                            .namespaces()
                            .try_reserve_write(put.namespace_id, u64::from(record_len))
                        {
                            Ok(guard) => namespace_write_guards.push(Some(guard)),
                            Err(reason) => {
                                policy_rejection = Some((index, namespace_reject_reason(reason)));
                                break;
                            }
                        }
                    } else {
                        namespace_write_guards.push(None);
                    }
                }
                let Some((offender, reason)) = policy_rejection else {
                    break 'policy (capacity_guards, namespace_write_guards);
                };
                drop(capacity_guards);
                drop(namespace_write_guards);
                if offender == 0 {
                    self.reject_planned_puts(&mut accepted, 1, reason);
                    continue 'batches;
                }
                minimum_lengths.truncate(offender);
                plan = match self.preview_batch_on_lane(lane_id, &minimum_lengths, align_for_direct)
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        complete_puts_with_error(accepted.drain(..), &error);
                        return;
                    }
                };
            };
            let daily_guard =
                if !self.inner.delegated_policy || batch_source == PutSource::Reinsertion {
                    match self
                        .inner
                        .policy
                        .host_writes()
                        .try_reserve_daily(plan.write_len as u64)
                    {
                        Ok(guard) => Some(guard),
                        Err(_) => {
                            self.reject_planned_puts(
                                &mut accepted,
                                plan.records,
                                RejectReason::DailyWriteBudgetExceeded,
                            );
                            continue;
                        }
                    }
                } else {
                    None
                };
            let write_budget_guard = match self
                .inner
                .resources
                .try_reserve_write(plan.write_len as u64)
            {
                Ok(guard) => guard,
                Err(()) => {
                    self.reject_planned_puts(
                        &mut accepted,
                        plan.records,
                        RejectReason::WriteBudgetExceeded,
                    );
                    continue;
                }
            };
            let reserved = self.reserve_batch_on_lane(
                lane_id,
                &vec![RecordKind::Value; plan.records],
                &minimum_lengths[..plan.records],
                align_for_direct,
            );
            let (reserved_plan, reservations) = match reserved {
                Ok(reserved) => reserved,
                Err(CacheError::ReclaimBacklog) => {
                    self.reject_planned_puts(
                        &mut accepted,
                        plan.records,
                        RejectReason::ReclaimBacklog,
                    );
                    continue;
                }
                Err(error) => {
                    complete_puts_with_error(accepted.drain(..), &error);
                    return;
                }
            };
            if reserved_plan != plan {
                let error = CacheError::CorruptMetadata(
                    "append lane changed between batch planning and reservation",
                );
                self.fail_append(&error);
                complete_puts_with_error(accepted.drain(..), &error);
                return;
            }
            let mut batch = accepted.drain(..plan.records).collect::<Vec<_>>();
            if let Err(error) = self.write_reserved_put_batch(
                &mut batch,
                &plan,
                &reservations,
                capacity_guards,
                namespace_write_guards,
                daily_guard,
                write_budget_guard,
            ) {
                complete_puts_with_error(batch, &error);
                complete_puts_with_error(accepted.drain(..), &error);
                return;
            }
            for put in batch {
                let receipt = put.receipt.ok_or(CacheError::CorruptMetadata(
                    "stored append did not publish a managed receipt",
                ));
                let _ = put.completion.send(receipt);
            }
        }
    }

    fn reject_planned_puts(
        &self,
        accepted: &mut VecDeque<PreparedPut>,
        count: usize,
        reason: RejectReason,
    ) {
        let foreground = accepted
            .iter()
            .take(count)
            .filter(|put| put.source.is_foreground())
            .count() as u64;
        if foreground != 0 {
            if let Ok(mut state) = self.lock_state() {
                if state.status == CacheStatus::Healthy {
                    state.stats.rejected = state.stats.rejected.saturating_add(foreground);
                }
            }
        }
        for put in accepted.drain(..count) {
            let _ = put
                .completion
                .send(Ok(RegionPutReceipt::rejected(PutOutcome::Rejected(reason))));
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn write_reserved_put_batch(
        &self,
        puts: &mut [PreparedPut],
        plan: &BatchPlan,
        reservations: &[AppendReservation],
        capacity_guards: Vec<Option<NamespaceCapacityReservation>>,
        namespace_write_guards: Vec<Option<NamespaceWriteReservation>>,
        daily_guard: Option<DailyWriteReservation>,
        write_budget_guard: WriteBudgetReservation<'_>,
    ) -> Result<()> {
        if puts.len() != plan.records
            || reservations.len() != puts.len()
            || capacity_guards.len() != puts.len()
            || namespace_write_guards.len() != puts.len()
        {
            let error = CacheError::CorruptMetadata("append batch reservation count mismatch");
            self.fail_append(&error);
            return Err(error);
        }
        let mut target = match puts.first_mut().and_then(|put| put.buffer.take()) {
            Some(target) => target,
            None => {
                let error = CacheError::CorruptMetadata("append batch lost its target buffer");
                self.fail_append(&error);
                return Err(error);
            }
        };
        if target.grow_preserving(plan.write_len).is_err() {
            let error = CacheError::Overloaded(OverloadReason::WriteBufferUnavailable);
            self.fail_append(&error);
            return Err(error);
        }
        let encode_result = (|| -> Result<()> {
            let encoded = target.prepared_mut(plan.write_len).map_err(|()| {
                CacheError::CorruptMetadata("append batch target has an invalid length")
            })?;
            let first_minimum = puts[0].minimum_record_len as usize;
            encoded[first_minimum..].fill(0);
            let mut cursor = 0_usize;
            for (index, ((put, reservation), &record_len)) in puts
                .iter()
                .zip(reservations)
                .zip(&plan.record_lengths)
                .enumerate()
            {
                let minimum = put.minimum_record_len as usize;
                let record_len = record_len as usize;
                if index != 0 {
                    let source = put
                        .buffer
                        .as_ref()
                        .ok_or(CacheError::CorruptMetadata(
                            "append batch lost a source buffer",
                        ))?
                        .prepared(minimum)
                        .map_err(|()| {
                            CacheError::CorruptMetadata("append batch source has an invalid length")
                        })?;
                    encoded[cursor..cursor + minimum].copy_from_slice(source);
                }
                let record = encoded.get_mut(cursor..cursor + record_len).ok_or(
                    CacheError::CorruptMetadata("append batch record exceeds target buffer"),
                )?;
                encode_record(
                    record,
                    RecordKind::Value,
                    put.codec,
                    put.hash,
                    put.key_len,
                    put.value_len,
                    put.expires_at,
                    *reservation,
                )?;
                cursor = cursor
                    .checked_add(record_len)
                    .ok_or(CacheError::CorruptMetadata("append batch cursor overflow"))?;
            }
            if cursor != plan.write_len {
                return Err(CacheError::CorruptMetadata(
                    "append batch encoded length mismatch",
                ));
            }
            Ok(())
        })();
        if let Err(error) = encode_result {
            self.fail_append(&error);
            return Err(error);
        }

        let absolute = reservations[0].absolute;
        let mut expected_absolute = absolute;
        for (reservation, &record_len) in reservations.iter().zip(&plan.record_lengths) {
            if reservation.absolute != expected_absolute {
                let error = CacheError::CorruptMetadata(
                    "append batch reservations are not physically contiguous",
                );
                self.fail_append(&error);
                return Err(error);
            }
            expected_absolute = match expected_absolute.checked_add(u64::from(record_len)) {
                Some(next) => next,
                None => {
                    let error =
                        CacheError::CorruptMetadata("append batch absolute offset overflow");
                    self.fail_append(&error);
                    return Err(error);
                }
            };
        }
        let source = puts
            .first()
            .map(|put| put.source)
            .ok_or(CacheError::CorruptMetadata("append batch is empty"))?;
        let write_kind = match source {
            PutSource::Foreground | PutSource::ManagedForeground => HostWriteKind::ForegroundRecord,
            PutSource::Reinsertion => HostWriteKind::Reinsertion,
        };
        let (write_result, completed) = self.engine_write_with_kind(
            WritePoint::Record,
            write_kind,
            target,
            plan.write_len,
            absolute,
        );
        if completed.is_none() {
            let error = match write_result {
                Err(error) => CacheError::Io(error),
                Ok(_) => {
                    CacheError::CorruptMetadata("successful batch write completion lost its buffer")
                }
            };
            self.fail_append(&error);
            return Err(error);
        }
        if let Err(error) = write_result {
            let error = CacheError::Io(error);
            self.fail_append(&error);
            return Err(error);
        }

        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        let min_seqno = state.superblock.epoch_start_seqno;
        for (((put, reservation), capacity_guard), namespace_write_guard) in puts
            .iter_mut()
            .zip(reservations)
            .zip(capacity_guards)
            .zip(namespace_write_guards)
        {
            let flags = if put.source == PutSource::Reinsertion {
                INDEX_FLAG_SECOND_CHANCE_USED
            } else {
                0
            };
            let applied = self.apply_index_accounted(
                put.hash,
                reservation.location,
                reservation.seqno,
                min_seqno,
                put.namespace_id,
                flags,
            );
            if let Err(error) = self.record_put_replacement(put.source, applied) {
                self.enter_failure_state(&mut state, &error);
                return Err(error);
            }
            let new_usage = applied.applied.then_some(NamespaceUsage {
                namespace: put.namespace_id,
                live_bytes: u64::from(reservation.location.record_len()),
            });
            let previous_usage = (put.source.is_managed() && applied.applied)
                .then(|| applied.previous.and_then(namespace_usage))
                .flatten();
            if let (Some(current), Some(commit)) = (new_usage, put.managed_commit.take()) {
                if let Err(error) = commit(current, previous_usage) {
                    // The record and compact-index publication are already
                    // visible. Latch a terminal state before releasing the
                    // Region state mutex; the composing owner cannot safely
                    // continue if its namespace accounting commit failed.
                    self.enter_failure_state(&mut state, &error);
                    return Err(error);
                }
            }
            put.receipt = Some(RegionPutReceipt {
                outcome: PutOutcome::Stored,
                new_usage,
                previous_usage,
            });
            if let (true, Some(capacity_guard)) = (applied.applied, capacity_guard) {
                capacity_guard.commit(applied.previous.and_then(namespace_usage));
            }
            if let Some(namespace_write_guard) = namespace_write_guard {
                namespace_write_guard.commit();
            }
            if !self.inner.delegated_policy && put.source.is_foreground() {
                self.inner
                    .policy
                    .host_writes()
                    .record_admitted_value(put.value_len as u64);
            }
        }
        if let Some(daily_guard) = daily_guard {
            daily_guard.commit();
        }
        write_budget_guard.commit();
        if source.is_foreground() {
            state.stats.puts = state.stats.puts.saturating_add(puts.len() as u64);
            state.stats.bytes_written = state
                .stats
                .bytes_written
                .saturating_add(plan.write_len as u64);
        } else {
            self.inner
                .reinsert_completed
                .fetch_add(puts.len() as u64, Ordering::Relaxed);
        }
        state.stats.write_batches = state.stats.write_batches.saturating_add(1);
        state.stats.records_coalesced = state
            .stats
            .records_coalesced
            .saturating_add(puts.len().saturating_sub(1) as u64);
        drop(state);
        self.request_periodic_checkpoint(plan.write_len as u64);
        self.request_background_reclaim(false);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn remove_on_append_lane(
        &self,
        lane_id: usize,
        hash: u64,
        namespace: NamespaceId,
        codec: RecordCodec,
        key_len: usize,
        record_len: u32,
        managed: bool,
        resources: RemoveResources,
    ) -> Result<RegionRemoveReceipt> {
        let (_permit, mut record, mut scratch) = resources.into_parts();
        {
            let state = self.lock_state()?;
            ensure_operational(&state)?;
        }
        let key_start = RECORD_HEADER_SIZE;
        let key_end = key_start
            .checked_add(key_len)
            .ok_or(CacheError::CorruptMetadata("remove key length overflow"))?;
        let encoded_record = record
            .prepared(record_len as usize)
            .map_err(|()| CacheError::CorruptMetadata("remove command lost its key buffer"))?;
        let expected_key = encoded_record
            .get(key_start..key_end)
            .ok_or(CacheError::CorruptMetadata("remove key exceeds its record"))?;
        let expected_key = if namespace == 0 {
            expected_key
        } else {
            expected_key
                .get(NAMESPACE_KEY_PREFIX_SIZE..)
                .ok_or(CacheError::CorruptMetadata(
                    "remove namespace key prefix is missing",
                ))?
        };
        let superblock = self.read_superblock()?;
        let found = match self.inner.index.get(hash, superblock.epoch_start_seqno) {
            Some(entry) if entry.namespace_id == namespace && !entry.location.is_tombstone() => {
                scratch
                    .prepare(entry.location.record_len() as usize)
                    .map_err(|()| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
                let loaded = match self
                    .inner
                    .read_view
                    .regions
                    .get(entry.location.region_id() as usize)
                {
                    Some(_) => {
                        let region = self.lock_read_region(entry.location.region_id())?;
                        let snapshot = ReadSnapshot {
                            superblock,
                            region: *region,
                        };
                        match self.load_entry(
                            snapshot,
                            entry,
                            namespace,
                            expected_key,
                            scratch,
                            None,
                        ) {
                            Ok((loaded, _completed)) => loaded,
                            Err(error) => {
                                drop(region);
                                let mut state = self.lock_state()?;
                                self.enter_failure_state(&mut state, &error);
                                return Err(error);
                            }
                        }
                    }
                    None => LoadedRecord::Corrupt,
                };
                match loaded {
                    LoadedRecord::Value { .. } => true,
                    LoadedRecord::Unavailable(error) => {
                        let mut state = self.lock_state()?;
                        self.enter_miss_only(&mut state);
                        return Err(CacheError::Io(error));
                    }
                    _ => false,
                }
            }
            _ => false,
        };

        record
            .prepared_mut(record_len as usize)
            .map_err(|()| CacheError::CorruptMetadata("remove record buffer became unavailable"))?;
        let (location, seqno, encoded_record_len) = self.append_prepared_on_lane(
            lane_id,
            RecordKind::Tombstone,
            codec,
            hash,
            key_len,
            0,
            0,
            record_len,
            record,
        )?;
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        let min_seqno = state.superblock.epoch_start_seqno;
        let applied = self.apply_index_accounted(hash, location, seqno, min_seqno, namespace, 0);
        if applied.applied
            && !managed
            && !self.record_namespace_retirement(applied.previous.and_then(namespace_usage))
        {
            let error =
                CacheError::CorruptMetadata("managed Region removal exceeded namespace live usage");
            self.enter_failure_state(&mut state, &error);
            return Err(error);
        }
        let previous_usage = (managed && applied.applied)
            .then(|| applied.previous.and_then(namespace_usage))
            .flatten();
        state.stats.removes += 1;
        state.stats.bytes_written = state
            .stats
            .bytes_written
            .saturating_add(u64::from(encoded_record_len));
        state.stats.write_batches = state.stats.write_batches.saturating_add(1);
        drop(state);
        self.request_periodic_checkpoint(u64::from(encoded_record_len));
        self.request_background_reclaim(false);
        Ok(RegionRemoveReceipt {
            outcome: if found {
                RemoveOutcome::Removed
            } else {
                RemoveOutcome::NotFound
            },
            previous_usage,
        })
    }

    fn flush_on_append_lane(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        self.checkpoint_clean(&mut state)
    }

    fn clear_on_append_lane(&self) -> Result<()> {
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        self.mark_dirty(&mut state)?;
        let barrier = match take_seqno(&mut state) {
            Ok(barrier) => barrier,
            Err(error) => {
                self.enter_failure_state(&mut state, &error);
                return Err(error);
            }
        };
        state.superblock.epoch = match state.superblock.epoch.checked_add(1) {
            Some(epoch) => epoch,
            None => {
                let error = CacheError::CorruptMetadata("namespace epoch overflow");
                self.enter_failure_state(&mut state, &error);
                return Err(error);
            }
        };
        state.superblock.epoch_start_seqno = barrier;
        if let Err(error) = self.persist_clear_barrier(&mut state) {
            self.enter_failure_state(&mut state, &error);
            return Err(error);
        }
        state.index.advance_clear_floor(barrier);
        if !self.inner.delegated_policy {
            self.inner.policy.namespaces().reset_live_bytes();
        }
        for counter in &self.inner.region_valid_bytes {
            counter.store(0, Ordering::Release);
        }
        for counter in &self.inner.region_reinserted_bytes {
            counter.store(0, Ordering::Release);
        }
        for counter in &self.inner.region_reinsert_pending {
            counter.store(0, Ordering::Release);
        }
        self.publish_read_epoch(&state);
        self.checkpoint_clean(&mut state)
    }

    #[allow(clippy::too_many_arguments)]
    fn append_prepared_on_lane(
        &self,
        lane_id: usize,
        kind: RecordKind,
        codec: RecordCodec,
        hash: u64,
        key_len: usize,
        value_len: usize,
        expires_at: u64,
        record_len: u32,
        mut buffer: BufferLease,
    ) -> Result<(PackedLocation, u64, u32)> {
        let mut align_for_direct = self.direct_append_active();
        let plan = loop {
            let plan = self.preview_batch_on_lane(lane_id, &[record_len], align_for_direct)?;
            if buffer.grow_preserving(plan.write_len).is_ok() {
                break plan;
            }
            if align_for_direct {
                align_for_direct = false;
                continue;
            }
            return Err(CacheError::CorruptMetadata(
                "an admitted append buffer cannot hold its original record",
            ));
        };
        let (reserved_plan, reservations) =
            self.reserve_batch_on_lane(lane_id, &[kind], &[record_len], align_for_direct)?;
        if reserved_plan != plan {
            let error = CacheError::CorruptMetadata(
                "append lane changed between record planning and reservation",
            );
            self.fail_append(&error);
            return Err(error);
        }
        let reservation = *reservations.first().ok_or(CacheError::CorruptMetadata(
            "single append has no reservation",
        ))?;
        let encoded_record_len =
            *plan
                .record_lengths
                .first()
                .ok_or(CacheError::CorruptMetadata(
                    "single append has no encoded record length",
                ))?;
        if buffer.grow_preserving(encoded_record_len as usize).is_err() {
            let error = CacheError::Overloaded(OverloadReason::WriteBufferUnavailable);
            self.fail_append(&error);
            return Err(error);
        }
        if encoded_record_len > record_len {
            buffer
                .prepared_mut(encoded_record_len as usize)
                .map_err(|()| {
                    CacheError::CorruptMetadata("single append buffer cannot hold direct padding")
                })?[record_len as usize..]
                .fill(0);
        }
        let encode_result = (|| -> Result<()> {
            let encoded = match buffer.prepared_mut(encoded_record_len as usize) {
                Ok(encoded) => encoded,
                Err(()) => {
                    return Err(CacheError::CorruptMetadata(
                        "append command lost its prepared buffer",
                    ));
                }
            };
            encode_record(
                encoded,
                kind,
                codec,
                hash,
                key_len,
                value_len,
                expires_at,
                reservation,
            )
        })();
        if let Err(error) = encode_result {
            self.fail_append(&error);
            return Err(error);
        }

        let (write_result, completed) = self.engine_write(
            WritePoint::Record,
            buffer,
            encoded_record_len as usize,
            reservation.absolute,
        );
        if completed.is_none() {
            let error = match write_result {
                Err(error) => CacheError::Io(error),
                Ok(_) => CacheError::CorruptMetadata("successful write completion lost its buffer"),
            };
            self.fail_append(&error);
            return Err(error);
        }
        if let Err(error) = write_result {
            let error = CacheError::Io(error);
            self.fail_append(&error);
            return Err(error);
        }
        Ok((reservation.location, reservation.seqno, encoded_record_len))
    }

    fn reserve_batch_on_lane(
        &self,
        lane_id: usize,
        kinds: &[RecordKind],
        minimum_lengths: &[u32],
        align_for_direct: bool,
    ) -> Result<(BatchPlan, Vec<AppendReservation>)> {
        if kinds.is_empty() || kinds.len() != minimum_lengths.len() {
            return Err(CacheError::CorruptMetadata(
                "append batch inputs have inconsistent lengths",
            ));
        }
        let mut state = self.lock_state()?;
        ensure_operational(&state)?;
        self.mark_dirty(&mut state)?;
        let plan = loop {
            let active_region =
                *state
                    .active_regions
                    .get(lane_id)
                    .ok_or(CacheError::CorruptMetadata(
                        "append lane id is out of bounds",
                    ))?;
            let start = state.regions[active_region as usize].used;
            let remaining = self.inner.config.region_size.checked_sub(start).ok_or(
                CacheError::CorruptMetadata("active region cursor exceeds its boundary"),
            )?;
            if let Some(plan) = plan_batch(minimum_lengths, start, remaining, align_for_direct) {
                break plan;
            }
            if let Err(error) = self.rotate_region(&mut state, lane_id) {
                if !matches!(error, CacheError::ReclaimBacklog) {
                    self.enter_failure_state(&mut state, &error);
                }
                return Err(error);
            }
        };
        let mut reservations = Vec::with_capacity(plan.records);
        for (&kind, &record_len) in kinds.iter().zip(&plan.record_lengths).take(plan.records) {
            match self.reserve_append_locked(&mut state, lane_id, kind, record_len) {
                Ok(reservation) => reservations.push(reservation),
                Err(error) => {
                    if !matches!(error, CacheError::ReclaimBacklog) {
                        self.enter_failure_state(&mut state, &error);
                    }
                    return Err(error);
                }
            }
        }
        Ok((plan, reservations))
    }

    fn preview_batch_on_lane(
        &self,
        lane_id: usize,
        minimum_lengths: &[u32],
        align_for_direct: bool,
    ) -> Result<BatchPlan> {
        if minimum_lengths.is_empty() {
            return Err(CacheError::CorruptMetadata("append batch is empty"));
        }
        let state = self.lock_state()?;
        ensure_operational(&state)?;
        let active_region =
            *state
                .active_regions
                .get(lane_id)
                .ok_or(CacheError::CorruptMetadata(
                    "append lane id is out of bounds",
                ))?;
        let start = state.regions[active_region as usize].used;
        let remaining =
            self.inner
                .config
                .region_size
                .checked_sub(start)
                .ok_or(CacheError::CorruptMetadata(
                    "active region cursor exceeds its boundary",
                ))?;
        if let Some(plan) = plan_batch(minimum_lengths, start, remaining, align_for_direct) {
            return Ok(plan);
        }
        plan_batch(
            minimum_lengths,
            REGION_HEADER_SIZE as u64,
            self.inner.config.region_size - REGION_HEADER_SIZE as u64,
            align_for_direct,
        )
        .ok_or(CacheError::CorruptMetadata(
            "append record cannot fit in a fresh region",
        ))
    }

    fn reserve_append_locked(
        &self,
        state: &mut State,
        lane_id: usize,
        kind: RecordKind,
        record_len: u32,
    ) -> Result<AppendReservation> {
        let active_region =
            *state
                .active_regions
                .get(lane_id)
                .ok_or(CacheError::CorruptMetadata(
                    "append lane id is out of bounds",
                ))?;
        let active = active_region as usize;
        let end = state.regions[active]
            .used
            .checked_add(u64::from(record_len))
            .ok_or(CacheError::CorruptMetadata("active region cursor overflow"))?;
        if end > self.inner.config.region_size {
            self.rotate_region(state, lane_id)?;
        }

        let seqno = take_seqno(state)?;
        let active_region = state.active_regions[lane_id];
        let active = active_region as usize;
        let region = state.regions[active];
        let offset = region.used;
        let end = offset
            .checked_add(u64::from(record_len))
            .filter(|end| *end <= self.inner.config.region_size)
            .ok_or(CacheError::CorruptMetadata(
                "reserved record crosses its active region",
            ))?;
        let offset_u32 = u32::try_from(offset)
            .map_err(|_| CacheError::CorruptMetadata("record offset does not fit u32"))?;
        let location = PackedLocation::new(
            active_region,
            offset_u32,
            record_len,
            kind == RecordKind::Tombstone,
        )
        .map_err(|_| CacheError::CorruptMetadata("record location cannot be packed"))?;
        let absolute = region_base(&state.superblock, active_region)?
            .checked_add(offset)
            .ok_or(CacheError::CorruptMetadata("record offset overflow"))?;
        state.regions[active].used = end;
        state.regions[active].header.used = end;
        state.regions[active].max_seqno = seqno;
        Ok(AppendReservation {
            location,
            seqno,
            epoch: state.superblock.epoch,
            region_incarnation: region.header.incarnation,
            absolute,
        })
    }

    fn rotate_region(&self, state: &mut State, lane_id: usize) -> Result<()> {
        let old_active = *state
            .active_regions
            .get(lane_id)
            .ok_or(CacheError::CorruptMetadata(
                "append lane id is out of bounds",
            ))?;
        // Prefer unused capacity, then a Region that maintenance has already
        // fenced, scrubbed, and emptied. SecondChance requires that prepared
        // victim; FIFO retains synchronous reclaim as a fallback when the
        // maintenance worker has not completed in time.
        let free = state.free_regions.pop_front();
        if free.is_some() && state.free_regions.is_empty() {
            self.inner.reclaim_eligible.store(true, Ordering::Release);
        }
        let victim = if let Some(free) = free {
            if state.regions[free as usize].header.state != RegionState::Free {
                return Err(CacheError::CorruptMetadata(
                    "free Region queue is inconsistent",
                ));
            }
            free
        } else if let Some(ready) = state.reclaim_ready_region.take() {
            if state.regions[ready as usize].header.state != RegionState::Sealed
                || state.regions[ready as usize].used != REGION_HEADER_SIZE as u64
            {
                return Err(CacheError::CorruptMetadata(
                    "background reclaim published an invalid Region",
                ));
            }
            ready
        } else if self.inner.config.reclaim_mode == ReclaimMode::SecondChance {
            state.stats.reclaim_backlog_rejections =
                state.stats.reclaim_backlog_rejections.saturating_add(1);
            self.request_background_reclaim(true);
            return Err(CacheError::ReclaimBacklog);
        } else if state.reclaiming_region.is_some() && state.sealed_regions.is_empty() {
            state.stats.reclaim_backlog_rejections =
                state.stats.reclaim_backlog_rejections.saturating_add(1);
            return Err(CacheError::ReclaimBacklog);
        } else {
            state
                .sealed_regions
                .pop_front()
                .ok_or(CacheError::CorruptMetadata(
                    "no non-Active region is available for rotation",
                ))?
        };

        // Reuse changes the meaning of an on-disk location. Wait for every
        // reader that captured the old incarnation before touching headers or
        // record bytes, then publish the new view before releasing the guard.
        let mut read_region = self.lock_read_region_for_rotation(victim);
        #[cfg(test)]
        self.observe_schedule(SchedulePoint::RotateReadersDrained);
        let old_active_index = old_active as usize;
        let mut sealed = state.regions[old_active_index].header;
        sealed.state = RegionState::Sealed;
        sealed.used = state.regions[old_active_index].used;
        self.write_region_header(&state.superblock, sealed)?;
        // The old lane owner must be durably non-Active before a replacement
        // can become durable. Otherwise a crash between the two header writes
        // can reopen with append_lanes + 1 Active Regions.
        // A Hybrid owner has already published its own dirty-session fence and
        // treats any unclean lower session as disposable. It can therefore
        // defer both rotation barriers to flush/close; dirty lower recovery
        // already formats inconsistent header combinations to an empty cache.
        if self.inner.owner_dirty.is_none() {
            self.engine_sync(SyncPoint::RegionRotation, SyncMode::Data)?;
        }
        state.regions[old_active_index].header = sealed;
        state.sealed_regions.push_back(old_active);
        *self.inner.read_view.regions[old_active_index]
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = state.regions[old_active_index];

        let victim_index = victim as usize;
        let old_victim = state.regions[victim_index];
        let reused = state.regions[victim_index].header.state != RegionState::Free;
        let incarnation = state.regions[victim_index]
            .header
            .incarnation
            .checked_add(1)
            .ok_or(CacheError::CorruptMetadata("region incarnation overflow"))?;
        let header = RegionHeader {
            region_id: victim,
            incarnation,
            state: RegionState::Active,
            created_seqno: state.superblock.next_seqno,
            used: REGION_HEADER_SIZE as u64,
        };
        self.write_region_header(&state.superblock, header)?;
        if self.inner.owner_dirty.is_none() {
            self.engine_sync(SyncPoint::RegionRotation, SyncMode::Data)?;
        }
        self.scrub_or_fallback_region_index(
            &state.superblock,
            old_victim,
            state.superblock.epoch_start_seqno,
        )?;
        self.publish_scrubbed_region_generation(header)?;
        state.regions[victim_index] = RegionMeta {
            header,
            used: REGION_HEADER_SIZE as u64,
            max_seqno: 0,
        };
        *read_region = state.regions[victim_index];
        state.active_regions[lane_id] = victim;
        if reused {
            state.stats.regions_reused += 1;
        }
        Ok(())
    }

    fn engine_read(
        &self,
        lease: BufferLease,
        length: usize,
        offset: u64,
        context: Option<&TaskContext>,
    ) -> (io::Result<usize>, Option<BufferLease>) {
        let buffer = match IoBuffer::from_lease(lease, length) {
            Ok(buffer) => buffer,
            Err(error) => return (Err(error.error), Some(error.lease)),
        };
        let request = match self.inner.engine.read_exact_at(buffer, offset) {
            Ok(request) => request,
            Err(error) if error.error.kind() == io::ErrorKind::WouldBlock => {
                let (_, operation) = error.into_parts();
                let submitted = if let Some(context) = context {
                    let cancelled = Arc::new(AtomicBool::new(false));
                    let cancelled_on_stop = Arc::clone(&cancelled);
                    let engine = Arc::clone(&self.inner.engine);
                    context.set_stop_hook(move |_| {
                        cancelled_on_stop.store(true, Ordering::Release);
                        engine.wake_admission_waiters();
                    });
                    self.inner.engine.submit_wait_controlled(
                        operation,
                        cancelled.as_ref(),
                        context.deadline(),
                    )
                } else {
                    self.inner.engine.submit_wait(operation)
                };
                match submitted {
                    Ok(request) => request,
                    Err(error) => {
                        let (error, lease) = error.into_lease();
                        return (Err(error), lease);
                    }
                }
            }
            Err(error) => {
                let (error, lease) = error.into_lease();
                return (Err(error), lease);
            }
        };
        let request_id = request.id();
        if let Some(context) = context {
            let engine = Arc::clone(&self.inner.engine);
            context.set_stop_hook(move |_| {
                let _ = engine.cancel(request_id);
            });
        }
        let completion = request.wait();
        let valid = completion.request_id == request_id && completion.kind == OperationKind::Read;
        let (result, lease) = completion.into_lease();
        if valid {
            (result, lease)
        } else {
            (
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "read completion identity mismatch",
                )),
                lease,
            )
        }
    }

    fn engine_write(
        &self,
        point: WritePoint,
        lease: BufferLease,
        length: usize,
        offset: u64,
    ) -> (io::Result<usize>, Option<BufferLease>) {
        let kind = if point == WritePoint::Record {
            HostWriteKind::ForegroundRecord
        } else {
            HostWriteKind::Metadata
        };
        self.engine_write_with_kind(point, kind, lease, length, offset)
    }

    fn engine_write_with_kind(
        &self,
        point: WritePoint,
        kind: HostWriteKind,
        lease: BufferLease,
        length: usize,
        offset: u64,
    ) -> (io::Result<usize>, Option<BufferLease>) {
        let buffer = match IoBuffer::from_lease(lease, length) {
            Ok(buffer) => buffer,
            Err(error) => return (Err(error.error), Some(error.lease)),
        };
        self.inner
            .policy
            .host_writes()
            .record_write(kind, length as u64);
        let request = match self.inner.engine.write_all_at(point, buffer, offset) {
            Ok(request) => request,
            Err(error) if error.error.kind() == io::ErrorKind::WouldBlock => {
                let (_, operation) = error.into_parts();
                match self.inner.engine.submit_wait(operation) {
                    Ok(request) => request,
                    Err(error) => {
                        let (error, lease) = error.into_lease();
                        self.inner.policy.host_writes().record_write_failure();
                        return (Err(error), lease);
                    }
                }
            }
            Err(error) => {
                let (error, lease) = error.into_lease();
                self.inner.policy.host_writes().record_write_failure();
                return (Err(error), lease);
            }
        };
        let request_id = request.id();
        let completion = request.wait();
        let valid = completion.request_id == request_id && completion.kind == OperationKind::Write;
        let (result, lease) = completion.into_lease();
        if valid {
            if result.is_err() {
                self.inner.policy.host_writes().record_write_failure();
            }
            (result, lease)
        } else {
            self.inner.policy.host_writes().record_write_failure();
            (
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "write completion identity mismatch",
                )),
                lease,
            )
        }
    }

    fn engine_sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
        let request = match self.inner.engine.flush(point, mode) {
            Ok(request) => request,
            Err(error) if error.error.kind() == io::ErrorKind::WouldBlock => {
                let (_, operation) = error.into_parts();
                self.inner
                    .engine
                    .submit_wait(operation)
                    .map_err(|error| error.error)?
            }
            Err(error) => return Err(error.error),
        };
        let request_id = request.id();
        let completion = request.wait();
        if completion.request_id != request_id || completion.kind != OperationKind::Flush {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "flush completion identity mismatch",
            ));
        }
        let (result, buffer) = completion.into_io_result();
        if buffer.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "flush completion unexpectedly contained a buffer",
            ));
        }
        result.map(|_| ())
    }

    fn load_entry(
        &self,
        snapshot: ReadSnapshot,
        entry: IndexEntry,
        namespace: NamespaceId,
        expected_key: &[u8],
        mut buffer: BufferLease,
        context: Option<&TaskContext>,
    ) -> Result<(LoadedRecord, BufferLease)> {
        let location = entry.location;
        let region_id = location.region_id();
        let region = snapshot.region;
        if region.header.region_id != region_id {
            return Ok((LoadedRecord::Corrupt, buffer));
        }
        if region.header.state == RegionState::Free {
            return Ok((LoadedRecord::Corrupt, buffer));
        }
        let absolute = region_base(&snapshot.superblock, region_id)?
            .checked_add(u64::from(location.offset()))
            .ok_or(CacheError::CorruptMetadata("read offset overflow"))?;
        let len = location.record_len() as usize;
        if len < RECORD_HEADER_SIZE
            || u64::from(location.offset()) + len as u64 > self.inner.config.region_size
        {
            return Ok((LoadedRecord::Corrupt, buffer));
        }
        if buffer.prepared(len).is_err() {
            return Err(CacheError::CorruptMetadata(
                "record scratch length does not match index",
            ));
        }
        let (read_result, completed) = self.engine_read(buffer, len, absolute, context);
        let Some(completed) = completed else {
            return Err(match read_result {
                Err(error) => CacheError::Io(error),
                Ok(_) => CacheError::CorruptMetadata(
                    "successful read completion did not return its buffer",
                ),
            });
        };
        buffer = completed;
        if let Err(error) = read_result {
            if error.kind() == io::ErrorKind::Interrupted
                && context.is_some_and(TaskContext::is_stopped)
            {
                return Ok((LoadedRecord::Cancelled, buffer));
            }
            return Ok((LoadedRecord::Unavailable(error), buffer));
        }
        let encoded = buffer
            .prepared(len)
            .map_err(|()| CacheError::CorruptMetadata("read completion lost its buffer"))?;
        let loaded = (|| -> LoadedRecord {
            let Some(header) = RecordHeader::decode(&encoded[..RECORD_HEADER_SIZE]) else {
                return LoadedRecord::Corrupt;
            };
            if header.record_len != location.record_len()
                || header.region_incarnation != region.header.incarnation
                || header.epoch != snapshot.superblock.epoch
                || header.seqno != entry.seqno
                || entry.namespace_id != namespace
                || !record_codec_matches_namespace(header.codec, namespace)
                || header.key_hash
                    != hash_namespaced_key(snapshot.superblock.hash_seed, namespace, expected_key)
                || header.value_len != header.stored_len
            {
                return LoadedRecord::Corrupt;
            }
            let key_len = header.key_len as usize;
            let stored_len = header.stored_len as usize;
            let Some(payload_len) = key_len.checked_add(stored_len) else {
                return LoadedRecord::Corrupt;
            };
            let Some(payload_end) = RECORD_HEADER_SIZE.checked_add(payload_len) else {
                return LoadedRecord::Corrupt;
            };
            if payload_end > encoded.len() {
                return LoadedRecord::Corrupt;
            }
            let payload = &encoded[RECORD_HEADER_SIZE..payload_end];
            if crc32c(payload) != header.payload_crc {
                return LoadedRecord::Corrupt;
            }
            let key = &payload[..key_len];
            if !namespaced_key_matches(key, namespace, expected_key) {
                return LoadedRecord::KeyMismatch;
            }
            if header.kind == RecordKind::Tombstone {
                return LoadedRecord::Tombstone;
            }
            if header.expires_at != 0 && header.expires_at <= now_unix_ms() {
                return LoadedRecord::Expired;
            }
            LoadedRecord::Value {
                start: RECORD_HEADER_SIZE + key_len,
                len: stored_len,
            }
        })();
        Ok((loaded, buffer))
    }

    fn persist_active_headers(&self, state: &mut State) -> Result<()> {
        // The exclusive operation barrier keeps the set stable while the
        // individual header writes run through the I/O engine.
        for lane_id in 0..state.active_regions.len() {
            let region_id = state.active_regions[lane_id];
            let active = region_id as usize;
            let region = state
                .regions
                .get_mut(active)
                .ok_or(CacheError::CorruptMetadata(
                    "active region id is out of bounds",
                ))?;
            if region.header.state != RegionState::Active {
                return Err(CacheError::CorruptMetadata(
                    "append lane does not reference an Active region",
                ));
            }
            region.header.used = region.used;
            self.write_region_header(&state.superblock, region.header)?;
        }
        Ok(())
    }

    /// Persist the dirty marker before the first write after a clean checkpoint.
    /// If this marker tears, the previous clean checkpoint remains valid because
    /// no region data is changed until the sync completes.
    fn mark_dirty(&self, state: &mut State) -> Result<()> {
        if !state.superblock.clean {
            return Ok(());
        }
        let mut candidate = state.superblock;
        candidate.clean = false;
        candidate.generation = match candidate.generation.checked_add(1) {
            Some(generation) => generation,
            None => {
                let error = CacheError::CorruptMetadata("superblock generation overflow");
                self.enter_failure_state(state, &error);
                return Err(error);
            }
        };
        if let Err(error) = self.write_both_superblocks(&candidate).and_then(|()| {
            self.engine_sync(SyncPoint::DirtyMarker, SyncMode::Data)
                .map_err(Into::into)
        }) {
            self.enter_failure_state(state, &error);
            return Err(error);
        }
        state.superblock = candidate;
        Ok(())
    }

    /// Make a namespace clear independently recoverable before publishing it
    /// to readers. A distinct generation prevents two valid but different
    /// dirty superblocks from tying during restart selection.
    fn persist_clear_barrier(&self, state: &mut State) -> Result<()> {
        let mut candidate = state.superblock;
        candidate.clean = false;
        candidate.generation =
            candidate
                .generation
                .checked_add(1)
                .ok_or(CacheError::CorruptMetadata(
                    "superblock generation overflow while clearing",
                ))?;
        self.write_both_superblocks(&candidate)?;
        self.engine_sync(SyncPoint::ClearBarrier, SyncMode::Data)?;
        state.superblock = candidate;
        Ok(())
    }

    /// Order record/header durability before publishing a clean superblock.
    fn checkpoint_clean(&self, state: &mut State) -> Result<()> {
        let result = (|| -> Result<(Superblock, usize)> {
            self.persist_active_headers(state)?;
            self.engine_sync(SyncPoint::CheckpointData, SyncMode::Data)?;
            let mut candidate = state.superblock;
            if !candidate.clean {
                candidate.clean = true;
                candidate.generation =
                    candidate
                        .generation
                        .checked_add(1)
                        .ok_or(CacheError::CorruptMetadata(
                            "superblock generation overflow",
                        ))?;
            }
            let checkpoint_slot = self.write_index_checkpoint(state, &candidate)?;
            if !state.superblock.clean {
                self.write_superblock(&candidate)?;
                self.engine_sync(SyncPoint::CheckpointClean, SyncMode::Data)?;
            }
            Ok((candidate, checkpoint_slot))
        })();
        match result {
            Ok((candidate, checkpoint_slot)) => {
                state.superblock = candidate;
                state.checkpoint_slot = Some(checkpoint_slot as u8);
                state.stats.checkpoint_writes = state.stats.checkpoint_writes.saturating_add(1);
                self.inner.checkpoint_bytes.store(0, Ordering::Release);
                Ok(())
            }
            Err(error) => {
                state.stats.checkpoint_errors = state.stats.checkpoint_errors.saturating_add(1);
                self.enter_failure_state(state, &error);
                Err(error)
            }
        }
    }

    fn write_index_checkpoint(&self, state: &State, target: &Superblock) -> Result<usize> {
        let min_seqno = target.epoch_start_seqno;
        let entry_count = u32::try_from(state.index.entry_len())
            .map_err(|_| CacheError::CorruptMetadata("checkpoint entry count overflow"))?;
        let directory = self.ensure_checkpoint_directory(target, entry_count as usize)?;
        let max_seqno = target
            .next_seqno
            .checked_sub(1)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint sequence metadata underflow",
            ))?;
        let mut encoder = CheckpointPayloadEncoder::new(
            directory,
            target.epoch_start_seqno,
            max_seqno,
            entry_count,
            u32::try_from(state.index.capacity()).map_err(|_| {
                CacheError::CorruptMetadata("index capacity does not fit checkpoint metadata")
            })?,
        )
        .map_err(checkpoint_codec_error)?;
        let slot = match state.checkpoint_slot {
            Some(slot) if usize::from(slot) < CHECKPOINT_SLOT_COUNT => 1 - usize::from(slot),
            _ => select_checkpoint_slot(self.inner.io.as_ref(), directory, &state.superblock)?,
        };
        let payload_offset = directory
            .slot_payload_offset(slot)
            .map_err(checkpoint_codec_error)?;
        let mut writer = CheckpointPayloadWriter::new(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            payload_offset,
        );
        for region in &state.regions {
            let lane_id = state
                .active_regions
                .iter()
                .position(|region_id| *region_id == region.header.region_id)
                .map(|lane_id| lane_id as u8);
            let snapshot = checkpoint_region(region, lane_id);
            let encoded = encoder
                .encode_region(snapshot)
                .map_err(checkpoint_codec_error)?;
            writer.push(&encoded)?;
        }
        if entry_count != 0 {
            state
                .index
                .try_for_each_snapshot_entry(min_seqno, |physical_slot, entry| {
                    let location = PackedLocation::try_from_raw(entry.location_raw)
                        .map_err(|_| CacheError::CorruptMetadata("invalid live index location"))?;
                    let owner = state.regions.get(location.region_id() as usize).ok_or(
                        CacheError::CorruptMetadata("checkpoint index region is out of bounds"),
                    )?;
                    let encoded = encoder
                        .encode_index_entry(
                            CheckpointIndexEntry {
                                key_hash: entry.hash,
                                location,
                                seqno: entry.seqno,
                                namespace_id: entry.namespace_id,
                                // A queued copy is process-local and may not have
                                // written a record yet. Only the durable USED bit
                                // is meaningful after restart.
                                flags: entry.flags & !INDEX_FLAG_SECOND_CHANCE_PENDING,
                                physical_slot: Some(physical_slot),
                            },
                            checkpoint_region(owner, None),
                        )
                        .map_err(checkpoint_codec_error)?;
                    writer.push(&encoded)
                })?;
        }
        let summary = encoder.finish().map_err(checkpoint_codec_error)?;
        writer.finish(
            summary.payload_len,
            padded_payload_len(summary.payload_len).map_err(checkpoint_codec_error)?,
        )?;
        sync_backend_tracked(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            SyncPoint::CheckpointPayload,
            SyncMode::Data,
        )?;
        let header = CheckpointSlotHeader::new(
            slot,
            CheckpointSnapshotMeta {
                generation: target.generation,
                superblock_generation: target.generation,
                epoch: target.epoch,
                epoch_start_seqno: target.epoch_start_seqno,
                max_seqno,
                hash_seed: target.hash_seed,
                index_slots: u32::try_from(state.index.capacity()).map_err(|_| {
                    CacheError::CorruptMetadata("index capacity does not fit checkpoint metadata")
                })?,
                index_shards: u32::try_from(state.index.shard_count()).map_err(|_| {
                    CacheError::CorruptMetadata(
                        "index shard count does not fit checkpoint metadata",
                    )
                })?,
            },
            summary,
            directory,
        )
        .and_then(|header| header.encode(directory))
        .map_err(checkpoint_codec_error)?;
        self.write_backend_tracked(
            WritePoint::CheckpointHeader,
            HostWriteKind::Checkpoint,
            &header,
            directory
                .slot_header_offset(slot)
                .map_err(checkpoint_codec_error)?,
        )?;
        sync_backend_tracked(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            SyncPoint::CheckpointHeader,
            SyncMode::Data,
        )?;
        Ok(slot)
    }

    fn ensure_checkpoint_directory(
        &self,
        superblock: &Superblock,
        entry_count: usize,
    ) -> Result<CheckpointDirectory> {
        let data_file_len = data_file_len(superblock)?;
        if let Some(directory) = read_checkpoint_directory(self.inner.io.as_ref(), data_file_len)? {
            if directory.region_size == superblock.region_size
                && directory.region_count == superblock.region_count
            {
                let payload_len = u64::from(superblock.region_count)
                    .checked_mul(CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
                    .and_then(|bytes| {
                        (entry_count as u64)
                            .checked_mul(CHECKPOINT_INDEX_ENTRY_SIZE as u64)
                            .and_then(|entries| bytes.checked_add(entries))
                    })
                    .ok_or(CacheError::CorruptMetadata(
                        "checkpoint payload length overflow",
                    ))?;
                if payload_len <= directory.payload_capacity() {
                    return Ok(directory);
                }
            }
        }

        let desired_entries = entry_count
            .max(1024)
            .checked_next_power_of_two()
            .unwrap_or(MAX_INDEX_SLOTS)
            .min(self.inner.config.index_slots)
            .max(entry_count);
        let directory = CheckpointDirectory::for_index_capacity(
            data_file_len,
            superblock.region_size,
            superblock.region_count,
            desired_entries,
        )
        .map_err(checkpoint_codec_error)?;
        self.inner
            .io
            .preallocate(directory.total_file_len().map_err(checkpoint_codec_error)?)?;
        let empty = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        for slot in 0..CHECKPOINT_SLOT_COUNT {
            self.write_backend_tracked(
                WritePoint::CheckpointHeader,
                HostWriteKind::Checkpoint,
                &empty,
                directory
                    .slot_header_offset(slot)
                    .map_err(checkpoint_codec_error)?,
            )?;
        }
        sync_backend_tracked(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            SyncPoint::CheckpointHeader,
            SyncMode::Data,
        )?;
        let encoded = directory.encode().map_err(checkpoint_codec_error)?;
        self.write_backend_tracked(
            WritePoint::CheckpointDirectory,
            HostWriteKind::Checkpoint,
            &encoded,
            data_file_len,
        )?;
        sync_backend_tracked(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            SyncPoint::CheckpointDirectory,
            SyncMode::Data,
        )?;
        Ok(directory)
    }

    fn write_region_header(&self, superblock: &Superblock, header: RegionHeader) -> Result<()> {
        let offset = region_base(superblock, header.region_id)?;
        self.write_metadata(WritePoint::RegionHeader, &header.encode(), offset)
    }

    fn write_superblock(&self, superblock: &Superblock) -> Result<()> {
        let offset = if superblock.generation % 2 == 0 {
            SUPERBLOCK_A_OFFSET
        } else {
            SUPERBLOCK_B_OFFSET
        };
        self.write_metadata(WritePoint::Superblock, &superblock.encode(), offset)
    }

    fn write_both_superblocks(&self, superblock: &Superblock) -> Result<()> {
        let encoded = superblock.encode();
        self.write_metadata(WritePoint::Superblock, &encoded, SUPERBLOCK_A_OFFSET)?;
        self.write_metadata(WritePoint::Superblock, &encoded, SUPERBLOCK_B_OFFSET)?;
        Ok(())
    }

    fn write_backend_tracked(
        &self,
        point: WritePoint,
        kind: HostWriteKind,
        bytes: &[u8],
        offset: u64,
    ) -> Result<()> {
        write_backend_tracked(
            self.inner.io.as_ref(),
            self.inner.policy.host_writes(),
            point,
            kind,
            bytes,
            offset,
        )
    }

    fn write_metadata(&self, point: WritePoint, bytes: &[u8], offset: u64) -> Result<()> {
        let mut lease = self
            .inner
            .resources
            .metadata_buffer()
            .map_err(CacheError::Overloaded)?;
        lease
            .prepare(bytes.len())
            .map_err(|()| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?
            .copy_from_slice(bytes);
        let (result, completed) = self.engine_write(point, lease, bytes.len(), offset);
        if completed.is_none() {
            return Err(match result {
                Err(error) => CacheError::Io(error),
                Ok(_) => {
                    CacheError::CorruptMetadata("successful metadata completion lost its buffer")
                }
            });
        }
        result.map(|_| ()).map_err(CacheError::Io)
    }
}

fn append_worker(inner: Weak<Inner>, lane_id: usize, commands: Receiver<AppendCommand>) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match commands.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };
        let Some(inner) = inner.upgrade() else {
            break;
        };
        let cache = DiskCache { inner };
        let stop = match command {
            AppendCommand::Put {
                hash,
                namespace_id,
                codec,
                key_len,
                value_len,
                expires_at,
                record_len,
                source,
                managed_commit,
                resources,
                completion,
            } => {
                let mut batch = Vec::with_capacity(MAX_BATCH_RECORDS);
                batch.push(PendingPut {
                    hash,
                    namespace_id,
                    codec,
                    key_len,
                    value_len,
                    expires_at,
                    record_len,
                    source,
                    managed_commit,
                    resources,
                    completion,
                });
                let coalesce_deadline =
                    if cache.direct_append_active() || record_len as usize >= MAX_BATCH_BYTES {
                        None
                    } else {
                        Instant::now().checked_add(cache.append_coalesce_delay())
                    };
                while batch.len() < MAX_BATCH_RECORDS {
                    let next = match commands.try_recv() {
                        Ok(command) => command,
                        Err(mpsc::TryRecvError::Disconnected) => break,
                        Err(mpsc::TryRecvError::Empty) => {
                            let Some(deadline) = coalesce_deadline else {
                                break;
                            };
                            if deadline <= Instant::now() {
                                break;
                            }
                            #[cfg(test)]
                            cache.observe_schedule(SchedulePoint::AppendCoalesceWaiting);
                            let Some(remaining) = deadline.checked_duration_since(Instant::now())
                            else {
                                break;
                            };
                            match commands.recv_timeout(remaining) {
                                Ok(command) => command,
                                Err(
                                    mpsc::RecvTimeoutError::Timeout
                                    | mpsc::RecvTimeoutError::Disconnected,
                                ) => break,
                            }
                        }
                    };
                    match next {
                        command @ AppendCommand::Put {
                            source: next_source,
                            ..
                        } if next_source != source => {
                            pending = Some(command);
                            break;
                        }
                        AppendCommand::Put {
                            hash,
                            namespace_id,
                            codec,
                            key_len,
                            value_len,
                            expires_at,
                            record_len,
                            source,
                            managed_commit,
                            resources,
                            completion,
                        } => batch.push(PendingPut {
                            hash,
                            namespace_id,
                            codec,
                            key_len,
                            value_len,
                            expires_at,
                            record_len,
                            source,
                            managed_commit,
                            resources,
                            completion,
                        }),
                        command => {
                            pending = Some(command);
                            break;
                        }
                    }
                }
                cache.put_batch_on_append_lane(lane_id, batch);
                false
            }
            AppendCommand::Remove {
                hash,
                namespace_id,
                codec,
                key_len,
                record_len,
                managed,
                resources,
                completion,
            } => {
                let result = cache.remove_on_append_lane(
                    lane_id,
                    hash,
                    namespace_id,
                    codec,
                    key_len,
                    record_len,
                    managed,
                    resources,
                );
                let _ = completion.send(result);
                false
            }
            AppendCommand::Shutdown => true,
        };
        if stop {
            break;
        }
    }
}

fn reinsert_worker(inner: Weak<Inner>, commands: Receiver<ReinsertCommand>) {
    while let Ok(command) = commands.recv() {
        match command {
            ReinsertCommand::Candidate {
                hash,
                entry,
                region_incarnation,
                reserved_bytes,
            } => {
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                DiskCache { inner }.process_reinsert(
                    hash,
                    entry,
                    region_incarnation,
                    reserved_bytes,
                );
            }
            ReinsertCommand::Shutdown => break,
        }
    }
}

fn maintenance_worker(inner: Weak<Inner>, commands: Receiver<MaintenanceCommand>) {
    while let Ok(command) = commands.recv() {
        match command {
            MaintenanceCommand::Checkpoint => {
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let cache = DiskCache { inner };
                // A failed full-cache rotation sets the forced bit even when
                // this one-slot queue already contains a checkpoint token.
                // Prepare the foreground's next Region before starting a
                // potentially multi-GiB checkpoint so reclaim backlog cannot
                // be stretched by the whole snapshot duration.
                if cache.inner.reclaim_forced.load(Ordering::Acquire) {
                    cache.run_background_reclaim();
                }
                cache.run_maintenance_checkpoint();
            }
            MaintenanceCommand::Reclaim => {
                let Some(inner) = inner.upgrade() else {
                    break;
                };
                let cache = DiskCache { inner };
                cache.run_background_reclaim();
                // A checkpoint threshold may be crossed while the bounded
                // maintenance queue already contains this reclaim token. The
                // pending bit is authoritative; the token only wakes us.
                if cache.inner.checkpoint_pending.load(Ordering::Acquire) {
                    cache.run_maintenance_checkpoint();
                }
            }
            MaintenanceCommand::Shutdown => break,
        }
    }
}

fn background_recovery_worker(inner: Weak<Inner>, pending: PendingRecovery) {
    let Some(inner) = inner.upgrade() else {
        return;
    };
    let cache = DiskCache {
        inner: Arc::clone(&inner),
    };
    let _barrier = match inner.operation_barrier.write() {
        Ok(barrier) => barrier,
        Err(poisoned) => {
            drop(poisoned.into_inner());
            cache.poison_runtime();
            inner.recovery_active.store(false, Ordering::Release);
            return;
        }
    };
    let PendingRecovery {
        layout,
        superblock,
        checkpoint,
        checkpoint_regions,
        mut regions,
        mut ordered,
    } = pending;
    let recovered = recover_dirty_checkpoint_state(
        inner.io.as_ref(),
        inner.policy.host_writes(),
        inner.config.reclaim_mode,
        superblock,
        checkpoint,
        Arc::clone(&inner.index),
        &inner.resources,
        &checkpoint_regions,
        inner.config.append_lanes,
        &mut regions,
        &mut ordered,
        Some(&inner.recovery_cancel),
        Some((&inner.recovery_regions_done, &inner.recovery_regions_total)),
    );
    let mut recovered = match recovered {
        Ok(state) => state,
        Err(CacheError::CorruptMetadata(_)) => match format_state(
            inner.io.as_ref(),
            inner.policy.host_writes(),
            &inner.config,
            &layout,
            Some(superblock.generation),
            Arc::clone(&inner.index),
            regions,
        ) {
            Ok(mut state) => {
                state.stats.checkpoint_fallbacks = 1;
                state
            }
            Err(error) => {
                finish_failed_background_recovery(&cache, error);
                return;
            }
        },
        Err(CacheError::Cancelled) => {
            inner.index.clear();
            if !inner.delegated_policy {
                inner.policy.namespaces().reset_live_bytes();
            }
            for counter in &inner.region_valid_bytes {
                counter.store(0, Ordering::Release);
            }
            inner.recovery_active.store(false, Ordering::Release);
            return;
        }
        Err(error) => {
            finish_failed_background_recovery(&cache, error);
            return;
        }
    };
    if inner.recovery_cancel.load(Ordering::Acquire) {
        inner.index.clear();
        if !inner.delegated_policy {
            inner.policy.namespaces().reset_live_bytes();
        }
        for counter in &inner.region_valid_bytes {
            counter.store(0, Ordering::Release);
        }
        inner.recovery_active.store(false, Ordering::Release);
        return;
    }
    if let Err(error) = rebuild_runtime_accounting(
        &recovered,
        inner.policy.as_ref(),
        !inner.delegated_policy,
        &inner.region_valid_bytes,
        &inner.region_reinserted_bytes,
        &inner.region_reinsert_pending,
    ) {
        finish_failed_background_recovery(&cache, error);
        return;
    }
    if let Err(error) = cache.checkpoint_clean(&mut recovered) {
        finish_failed_background_recovery(&cache, error);
        return;
    }
    recovered.status = CacheStatus::Healthy;
    {
        let mut state = inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *state = recovered;
        cache.publish_read_view(&state);
    }
    cache.set_lifecycle(CacheStatus::Healthy);
    inner.recovery_active.store(false, Ordering::Release);
}

fn finish_failed_background_recovery(cache: &DiskCache, error: CacheError) {
    let mut state = cache
        .inner
        .state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state.index.clear();
    if !cache.inner.delegated_policy {
        cache.inner.policy.namespaces().reset_live_bytes();
    }
    for counter in &cache.inner.region_valid_bytes {
        counter.store(0, Ordering::Release);
    }
    state.stats.checkpoint_errors = state.stats.checkpoint_errors.saturating_add(1);
    state.status = if matches!(error, CacheError::Io(_)) {
        CacheStatus::MissOnly
    } else {
        CacheStatus::Poisoned
    };
    cache.set_lifecycle(state.status);
    cache.inner.recovery_active.store(false, Ordering::Release);
}

struct CheckpointPayloadWriter<'a> {
    io: &'a dyn IoBackend,
    host_writes: &'a HostWriteTracker,
    offset: u64,
    page: [u8; CHECKPOINT_IO_CHUNK_BYTES],
    used: usize,
    logical_bytes: u64,
    physical_bytes: u64,
}

struct CheckpointPayloadReader<'a> {
    io: &'a dyn IoBackend,
    offset: u64,
    remaining: u64,
    page: [u8; CHECKPOINT_IO_CHUNK_BYTES],
    page_start: usize,
    page_end: usize,
}

impl<'a> CheckpointPayloadReader<'a> {
    fn new(io: &'a dyn IoBackend, offset: u64, length: u64) -> Self {
        Self {
            io,
            offset,
            remaining: length,
            page: [0; CHECKPOINT_IO_CHUNK_BYTES],
            page_start: 0,
            page_end: 0,
        }
    }

    fn read_record(&mut self, output: &mut [u8]) -> Result<()> {
        if output.len() as u64 > self.remaining {
            return Err(CacheError::CorruptMetadata(
                "checkpoint payload is shorter than its record counts",
            ));
        }
        let mut written = 0;
        while written < output.len() {
            if self.page_start == self.page_end {
                let length = usize::try_from(self.remaining.min(CHECKPOINT_IO_CHUNK_BYTES as u64))
                    .map_err(|_| CacheError::CorruptMetadata("checkpoint read length overflow"))?;
                read_exact_at(self.io, &mut self.page[..length], self.offset)?;
                self.offset =
                    self.offset
                        .checked_add(length as u64)
                        .ok_or(CacheError::CorruptMetadata(
                            "checkpoint read offset overflow",
                        ))?;
                self.page_start = 0;
                self.page_end = length;
            }
            let copied = (output.len() - written).min(self.page_end - self.page_start);
            output[written..written + copied]
                .copy_from_slice(&self.page[self.page_start..self.page_start + copied]);
            self.page_start += copied;
            written += copied;
            self.remaining -= copied as u64;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        if self.remaining == 0 {
            Ok(())
        } else {
            Err(CacheError::CorruptMetadata(
                "checkpoint payload has unconsumed bytes",
            ))
        }
    }
}

#[derive(Clone, Copy)]
struct LoadedCheckpoint {
    header: CheckpointSlotHeader,
}

/// Accounting assembled in the same streaming pass that restores a
/// checkpoint. A checkpoint may be loaded into a differently sized compact
/// index, so collisions and same-hash versions must be applied exactly like
/// the index: subtract the visible entry replaced by an install, then add the
/// supplied entry only when the install was applied.
struct CheckpointLoadAccounting {
    region_valid_bytes: Vec<u64>,
    namespace_live_bytes: Vec<NamespaceUsage>,
}

impl CheckpointLoadAccounting {
    fn try_new(
        region_count: usize,
        policy: &PolicyController,
        account_namespaces: bool,
    ) -> Result<Self> {
        let mut region_valid_bytes = Vec::new();
        region_valid_bytes
            .try_reserve_exact(region_count)
            .map_err(|_| {
                CacheError::InvalidConfig(
                    "checkpoint Region accounting workspace cannot be allocated".into(),
                )
            })?;
        region_valid_bytes.resize(region_count, 0);

        let namespace_live_bytes = if account_namespaces {
            policy.namespaces().try_zero_usage().map_err(|_| {
                CacheError::InvalidConfig(
                    "checkpoint namespace accounting workspace cannot be allocated".into(),
                )
            })?
        } else {
            Vec::new()
        };
        Ok(Self {
            region_valid_bytes,
            namespace_live_bytes,
        })
    }

    fn record_restore(&mut self, restored: ApplyResult, supplied: IndexEntry) -> Result<()> {
        if !restored.applied {
            return Ok(());
        }
        if let Some(previous) = restored.previous {
            self.subtract(previous)?;
        }
        self.add(supplied)
    }

    fn add(&mut self, entry: IndexEntry) -> Result<()> {
        let bytes = u64::from(entry.location.record_len());
        let region = self
            .region_valid_bytes
            .get_mut(entry.location.region_id() as usize)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint accounting Region is out of bounds",
            ))?;
        *region = region
            .checked_add(bytes)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint Region valid-byte accounting overflow",
            ))?;
        if !entry.location.is_tombstone() {
            if let Ok(index) = self
                .namespace_live_bytes
                .binary_search_by_key(&entry.namespace_id, |usage| usage.namespace)
            {
                let usage = &mut self.namespace_live_bytes[index];
                usage.live_bytes =
                    usage
                        .live_bytes
                        .checked_add(bytes)
                        .ok_or(CacheError::CorruptMetadata(
                            "checkpoint namespace live-byte accounting overflow",
                        ))?;
            }
        }
        Ok(())
    }

    fn subtract(&mut self, entry: IndexEntry) -> Result<()> {
        let bytes = u64::from(entry.location.record_len());
        let region = self
            .region_valid_bytes
            .get_mut(entry.location.region_id() as usize)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint accounting Region is out of bounds",
            ))?;
        *region = region
            .checked_sub(bytes)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint Region valid-byte accounting underflow",
            ))?;
        if !entry.location.is_tombstone() {
            if let Ok(index) = self
                .namespace_live_bytes
                .binary_search_by_key(&entry.namespace_id, |usage| usage.namespace)
            {
                let usage = &mut self.namespace_live_bytes[index];
                usage.live_bytes =
                    usage
                        .live_bytes
                        .checked_sub(bytes)
                        .ok_or(CacheError::CorruptMetadata(
                            "checkpoint namespace live-byte accounting underflow",
                        ))?;
            }
        }
        Ok(())
    }

    fn publish(
        self,
        policy: &PolicyController,
        account_namespaces: bool,
        region_valid_bytes: &[AtomicU64],
    ) -> Result<()> {
        if self.region_valid_bytes.len() != region_valid_bytes.len() {
            return Err(CacheError::CorruptMetadata(
                "checkpoint Region accounting length mismatch",
            ));
        }
        for (counter, bytes) in region_valid_bytes.iter().zip(self.region_valid_bytes) {
            counter.store(bytes, Ordering::Release);
        }
        if account_namespaces {
            policy.namespaces().reset_live_bytes();
            for usage in self.namespace_live_bytes {
                if usage.live_bytes != 0 {
                    policy
                        .namespaces()
                        .restore_live_bytes(usage.namespace, usage.live_bytes)
                        .map_err(|_| {
                            CacheError::CorruptMetadata(
                                "checkpoint namespace live-byte accounting cannot be restored",
                            )
                        })?;
                }
            }
        }
        Ok(())
    }
}

struct PendingRecovery {
    layout: Layout,
    superblock: Superblock,
    checkpoint: LoadedCheckpoint,
    checkpoint_regions: Vec<CheckpointRegionSnapshot>,
    regions: Vec<RegionMeta>,
    ordered: Vec<u32>,
}

impl<'a> CheckpointPayloadWriter<'a> {
    fn new(io: &'a dyn IoBackend, host_writes: &'a HostWriteTracker, offset: u64) -> Self {
        Self {
            io,
            host_writes,
            offset,
            page: [0; CHECKPOINT_IO_CHUNK_BYTES],
            used: 0,
            logical_bytes: 0,
            physical_bytes: 0,
        }
    }

    fn push(&mut self, mut bytes: &[u8]) -> Result<()> {
        self.logical_bytes = self.logical_bytes.checked_add(bytes.len() as u64).ok_or(
            CacheError::CorruptMetadata("checkpoint payload cursor overflow"),
        )?;
        while !bytes.is_empty() {
            let copied = bytes.len().min(CHECKPOINT_IO_CHUNK_BYTES - self.used);
            self.page[self.used..self.used + copied].copy_from_slice(&bytes[..copied]);
            self.used += copied;
            bytes = &bytes[copied..];
            if self.used == CHECKPOINT_IO_CHUNK_BYTES {
                self.flush_bytes(CHECKPOINT_IO_CHUNK_BYTES)?;
            }
        }
        Ok(())
    }

    fn finish(mut self, expected_logical_bytes: u64, expected_physical_bytes: u64) -> Result<()> {
        if self.used != 0 {
            let padded = usize::try_from(
                padded_payload_len(self.used as u64).map_err(checkpoint_codec_error)?,
            )
            .map_err(|_| CacheError::CorruptMetadata("checkpoint padding overflow"))?;
            self.page[self.used..padded].fill(0);
            self.flush_bytes(padded)?;
        }
        if self.logical_bytes != expected_logical_bytes
            || self.physical_bytes != expected_physical_bytes
        {
            return Err(CacheError::CorruptMetadata(
                "checkpoint padded payload length mismatch",
            ));
        }
        Ok(())
    }

    fn flush_bytes(&mut self, length: usize) -> Result<()> {
        if length == 0
            || length > self.page.len()
            || length % CHECKPOINT_PAGE_SIZE != 0
            || self.used > length
        {
            return Err(CacheError::CorruptMetadata(
                "checkpoint payload chunk is invalid",
            ));
        }
        let absolute =
            self.offset
                .checked_add(self.physical_bytes)
                .ok_or(CacheError::CorruptMetadata(
                    "checkpoint payload offset overflow",
                ))?;
        self.host_writes
            .record_write(HostWriteKind::Checkpoint, length as u64);
        if let Err(error) = write_all_at(
            self.io,
            WritePoint::CheckpointPayload,
            &self.page[..length],
            absolute,
        ) {
            self.host_writes.record_write_failure();
            return Err(CacheError::Io(error));
        }
        self.page.fill(0);
        self.used = 0;
        self.physical_bytes =
            self.physical_bytes
                .checked_add(length as u64)
                .ok_or(CacheError::CorruptMetadata(
                    "checkpoint physical cursor overflow",
                ))?;
        Ok(())
    }
}

fn checkpoint_region(region: &RegionMeta, lane_id: Option<u8>) -> CheckpointRegionSnapshot {
    CheckpointRegionSnapshot {
        region_id: region.header.region_id,
        incarnation: region.header.incarnation,
        state: region.header.state,
        lane_id,
        used: region.used,
        created_seqno: region.header.created_seqno,
        max_seqno: region.max_seqno,
    }
}

fn checkpoint_codec_error(_: CheckpointCodecError) -> CacheError {
    CacheError::CorruptMetadata("invalid index checkpoint")
}

fn data_file_len(superblock: &Superblock) -> Result<u64> {
    DATA_OFFSET
        .checked_add(
            u64::from(superblock.region_count)
                .checked_mul(superblock.region_size)
                .ok_or(CacheError::CorruptMetadata(
                    "checkpoint data extent overflow",
                ))?,
        )
        .ok_or(CacheError::CorruptMetadata(
            "checkpoint data file length overflow",
        ))
}

fn read_checkpoint_directory(
    io: &dyn IoBackend,
    data_file_len: u64,
) -> Result<Option<CheckpointDirectory>> {
    let required = data_file_len
        .checked_add(CHECKPOINT_DIRECTORY_SIZE as u64)
        .ok_or(CacheError::CorruptMetadata(
            "checkpoint directory offset overflow",
        ))?;
    if io.len()? < required {
        return Ok(None);
    }
    let mut encoded = [0_u8; CHECKPOINT_DIRECTORY_SIZE];
    read_exact_at(io, &mut encoded, data_file_len)?;
    Ok(CheckpointDirectory::decode(&encoded)
        .ok()
        .filter(|directory| directory.data_file_len == data_file_len))
}

fn try_load_checkpoint(
    io: &dyn IoBackend,
    superblock: &Superblock,
    index: &ShardedIndex,
    checkpoint_regions: &mut Vec<CheckpointRegionSnapshot>,
    policy: &PolicyController,
    account_namespaces: bool,
    region_valid_bytes: &[AtomicU64],
) -> Result<Option<LoadedCheckpoint>> {
    let data_len = data_file_len(superblock)?;
    let Some(directory) = read_checkpoint_directory(io, data_len)? else {
        return Ok(None);
    };
    if directory.region_size != superblock.region_size
        || directory.region_count != superblock.region_count
    {
        return Ok(None);
    }
    let file_len = io.len()?;
    let mut candidates = Vec::with_capacity(CHECKPOINT_SLOT_COUNT);
    for slot in 0..CHECKPOINT_SLOT_COUNT {
        let offset = directory
            .slot_header_offset(slot)
            .map_err(checkpoint_codec_error)?;
        let Some(end) = offset.checked_add(CHECKPOINT_SLOT_HEADER_SIZE as u64) else {
            continue;
        };
        if end > file_len {
            continue;
        }
        let mut encoded = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        read_exact_at(io, &mut encoded, offset)?;
        if let Ok(header) = CheckpointSlotHeader::decode(&encoded, directory, slot) {
            if checkpoint_matches_superblock(header, superblock) {
                candidates.push(header);
            }
        }
    }
    candidates.sort_unstable_by_key(|candidate| std::cmp::Reverse(candidate.generation));
    for header in candidates {
        let payload_offset = directory
            .slot_payload_offset(header.slot as usize)
            .map_err(checkpoint_codec_error)?;
        let padded = padded_payload_len(header.payload_len).map_err(checkpoint_codec_error)?;
        if payload_offset
            .checked_add(padded)
            .is_none_or(|end| end > file_len)
        {
            continue;
        }
        index.clear();
        checkpoint_regions.clear();
        let mut accounting = CheckpointLoadAccounting::try_new(
            superblock.region_count as usize,
            policy,
            account_namespaces,
        )?;
        match load_checkpoint_payload(
            io,
            directory,
            header,
            index,
            checkpoint_regions,
            &mut accounting,
        ) {
            Ok(()) => {
                accounting.publish(policy, account_namespaces, region_valid_bytes)?;
                return Ok(Some(LoadedCheckpoint { header }));
            }
            Err(CacheError::CorruptMetadata(_)) => {
                index.clear();
                checkpoint_regions.clear();
            }
            Err(error) => return Err(error),
        }
    }
    index.clear();
    checkpoint_regions.clear();
    Ok(None)
}

fn select_checkpoint_slot(
    io: &dyn IoBackend,
    directory: CheckpointDirectory,
    current_superblock: &Superblock,
) -> Result<usize> {
    let file_len = io.len()?;
    let mut protected: Option<CheckpointSlotHeader> = None;
    for slot in 0..CHECKPOINT_SLOT_COUNT {
        let offset = directory
            .slot_header_offset(slot)
            .map_err(checkpoint_codec_error)?;
        if offset
            .checked_add(CHECKPOINT_SLOT_HEADER_SIZE as u64)
            .is_none_or(|end| end > file_len)
        {
            continue;
        }
        let mut encoded = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        read_exact_at(io, &mut encoded, offset)?;
        let Ok(header) = CheckpointSlotHeader::decode(&encoded, directory, slot) else {
            continue;
        };
        if !checkpoint_matches_superblock(header, current_superblock)
            || !checkpoint_payload_checksum_matches(io, directory, header, file_len)?
        {
            continue;
        }
        if protected.is_none_or(|current| header.generation > current.generation) {
            protected = Some(header);
        }
    }
    Ok(protected
        .map(|header| 1 - header.slot as usize)
        .unwrap_or(0))
}

fn checkpoint_payload_checksum_matches(
    io: &dyn IoBackend,
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
    file_len: u64,
) -> Result<bool> {
    let mut offset = directory
        .slot_payload_offset(header.slot as usize)
        .map_err(checkpoint_codec_error)?;
    if offset
        .checked_add(header.payload_len)
        .is_none_or(|end| end > file_len)
    {
        return Ok(false);
    }
    let mut remaining = header.payload_len;
    let mut page = [0_u8; CHECKPOINT_PAGE_SIZE];
    let mut checksum = Crc32c::new();
    while remaining != 0 {
        let length = usize::try_from(remaining.min(CHECKPOINT_PAGE_SIZE as u64))
            .map_err(|_| CacheError::CorruptMetadata("checkpoint checksum length overflow"))?;
        read_exact_at(io, &mut page[..length], offset)?;
        checksum.update(&page[..length]);
        offset = offset
            .checked_add(length as u64)
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint checksum offset overflow",
            ))?;
        remaining -= length as u64;
    }
    Ok(checksum.finish() == header.payload_crc)
}

fn checkpoint_matches_superblock(
    checkpoint: CheckpointSlotHeader,
    superblock: &Superblock,
) -> bool {
    if checkpoint.generation != checkpoint.superblock_generation
        || checkpoint.hash_seed != superblock.hash_seed
    {
        return false;
    }
    if superblock.clean {
        return checkpoint.superblock_generation == superblock.generation
            && checkpoint.epoch == superblock.epoch
            && checkpoint.epoch_start_seqno == superblock.epoch_start_seqno
            && superblock
                .next_seqno
                .checked_sub(1)
                .is_some_and(|max| max == checkpoint.max_seqno);
    }
    if checkpoint.superblock_generation >= superblock.generation {
        return false;
    }
    let checkpoint_next = checkpoint.max_seqno.checked_add(1);
    if checkpoint.epoch == superblock.epoch {
        return checkpoint.epoch_start_seqno == superblock.epoch_start_seqno
            && checkpoint_next == Some(superblock.next_seqno)
            && checkpoint.superblock_generation.checked_add(1) == Some(superblock.generation);
    }
    checkpoint.epoch < superblock.epoch
        && checkpoint.epoch_start_seqno < superblock.epoch_start_seqno
        && checkpoint.max_seqno < superblock.epoch_start_seqno
        && superblock
            .epoch_start_seqno
            .checked_add(1)
            .is_some_and(|next| next == superblock.next_seqno)
        && checkpoint
            .superblock_generation
            .checked_add(2)
            .is_some_and(|minimum| minimum <= superblock.generation)
}

fn load_checkpoint_payload(
    io: &dyn IoBackend,
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
    index: &ShardedIndex,
    checkpoint_regions: &mut Vec<CheckpointRegionSnapshot>,
    accounting: &mut CheckpointLoadAccounting,
) -> Result<()> {
    let exact_slots = header.index_slots == u32::try_from(index.capacity()).ok()
        && header.index_shards == u32::try_from(index.shard_count()).ok();
    let offset = directory
        .slot_payload_offset(header.slot as usize)
        .map_err(checkpoint_codec_error)?;
    let mut reader = CheckpointPayloadReader::new(io, offset, header.payload_len);
    let mut decoder =
        CheckpointPayloadDecoder::new(directory, header).map_err(checkpoint_codec_error)?;
    for _ in 0..header.region_count {
        let mut encoded = [0_u8; CHECKPOINT_REGION_SNAPSHOT_SIZE];
        reader.read_record(&mut encoded)?;
        checkpoint_regions.push(
            decoder
                .decode_region(&encoded)
                .map_err(checkpoint_codec_error)?,
        );
    }
    if !index.reset_visibility_for_restore(
        header.epoch_start_seqno,
        checkpoint_regions.iter().map(|region| match region.state {
            RegionState::Free => RegionGeneration::Free,
            RegionState::Active | RegionState::Sealed => RegionGeneration::Allocated {
                created_seqno: region.created_seqno,
            },
        }),
    ) {
        return Err(CacheError::CorruptMetadata(
            "checkpoint Region generations do not match the index",
        ));
    }
    for _ in 0..header.entry_count {
        let mut encoded = [0_u8; CHECKPOINT_INDEX_ENTRY_SIZE];
        let entry_size = decoder.index_entry_size();
        reader.read_record(&mut encoded[..entry_size])?;
        let raw = decode_checkpoint_index_entry(&encoded[..entry_size])
            .map_err(checkpoint_codec_error)?;
        let owner = checkpoint_regions
            .get(raw.location.region_id() as usize)
            .copied()
            .ok_or(CacheError::CorruptMetadata(
                "checkpoint entry region is out of bounds",
            ))?;
        let entry = decoder
            .decode_index_entry(&encoded[..entry_size], owner)
            .map_err(checkpoint_codec_error)?;
        let snapshot = IndexSnapshotEntry {
            hash: entry.key_hash,
            location_raw: entry.location.raw(),
            seqno: entry.seqno,
            namespace_id: entry.namespace_id,
            flags: entry.flags,
        };
        let restored = if exact_slots {
            index.restore_snapshot_entry_exact(
                entry.physical_slot.ok_or(CacheError::CorruptMetadata(
                    "checkpoint physical index slot is missing",
                ))?,
                snapshot,
                header.epoch_start_seqno,
            )
        } else {
            index.restore_snapshot_entry(snapshot, header.epoch_start_seqno)
        }
        .map_err(|_| CacheError::CorruptMetadata("invalid checkpoint index entry"))?;
        if exact_slots && !restored.applied {
            return Err(CacheError::CorruptMetadata(
                "checkpoint physical index entry is not visible",
            ));
        }
        accounting.record_restore(
            restored,
            IndexEntry {
                location: entry.location,
                seqno: entry.seqno,
                namespace_id: entry.namespace_id,
                flags: entry.flags,
            },
        )?;
    }
    reader.finish()?;
    decoder.finish().map_err(checkpoint_codec_error)?;
    if exact_slots && index.entry_len() != header.entry_count as usize {
        return Err(CacheError::CorruptMetadata(
            "checkpoint physical index entry count mismatch",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn encode_record(
    encoded: &mut [u8],
    kind: RecordKind,
    codec: RecordCodec,
    hash: u64,
    key_len: usize,
    value_len: usize,
    expires_at: u64,
    reservation: AppendReservation,
) -> Result<()> {
    let record_len = u32::try_from(encoded.len())
        .map_err(|_| CacheError::CorruptMetadata("encoded record length does not fit u32"))?;
    let key_len_u32 = u32::try_from(key_len)
        .map_err(|_| CacheError::CorruptMetadata("record key length does not fit u32"))?;
    let value_len_u32 = u32::try_from(value_len)
        .map_err(|_| CacheError::CorruptMetadata("record value length does not fit u32"))?;
    let payload_end = RECORD_HEADER_SIZE
        .checked_add(key_len)
        .and_then(|end| end.checked_add(value_len))
        .filter(|end| *end <= encoded.len())
        .ok_or(CacheError::CorruptMetadata(
            "record payload exceeds its encoded length",
        ))?;
    let mut checksum = Crc32c::new();
    checksum.update(&encoded[RECORD_HEADER_SIZE..payload_end]);
    let header = RecordHeader {
        kind,
        codec,
        key_len: key_len_u32,
        value_len: value_len_u32,
        stored_len: value_len_u32,
        record_len,
        region_incarnation: reservation.region_incarnation,
        epoch: reservation.epoch,
        seqno: reservation.seqno,
        key_hash: hash,
        expires_at,
        payload_crc: checksum.finish(),
    };
    encoded[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());
    Ok(())
}

fn complete_puts_with_error(puts: impl IntoIterator<Item = PreparedPut>, error: &CacheError) {
    for put in puts {
        let _ = put.completion.send(Err(clone_cache_error(error)));
    }
}

fn clone_cache_error(error: &CacheError) -> CacheError {
    match error {
        CacheError::Io(error) => CacheError::Io(match error.raw_os_error() {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(error.kind(), error.to_string()),
        }),
        CacheError::InvalidConfig(message) => CacheError::InvalidConfig(message.clone()),
        CacheError::CorruptMetadata(message) => CacheError::CorruptMetadata(message),
        CacheError::Locked => CacheError::Locked,
        CacheError::Closed => CacheError::Closed,
        CacheError::Poisoned => CacheError::Poisoned,
        CacheError::Cancelled => CacheError::Cancelled,
        CacheError::TimedOut => CacheError::TimedOut,
        CacheError::Overloaded(reason) => CacheError::Overloaded(*reason),
        CacheError::ReclaimBacklog => CacheError::ReclaimBacklog,
    }
}

fn build_io_engine(
    config: &CacheConfig,
    backend: Arc<dyn IoBackend>,
    runtime_files: Option<RuntimeFileSet>,
) -> Result<Arc<dyn IoEngine>> {
    let sync = |files: Option<RuntimeFileSet>| {
        let engine = match files {
            Some(files) => BackendIoEngine::new_with_files(files, config.io_queue_depth),
            None => BackendIoEngine::new(Arc::clone(&backend), config.io_queue_depth),
        };
        engine
            .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
            .map_err(CacheError::Io)
    };
    match config.io_engine {
        IoEngineKind::Sync => sync(runtime_files),
        IoEngineKind::Auto => {
            #[cfg(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            ))]
            if let Some(files) = runtime_files {
                let sync_files = files.try_clone().map_err(CacheError::Io)?;
                match UringIoEngine::new_with_files(files, config.io_queue_depth) {
                    Ok(engine) => return Ok(Arc::new(engine)),
                    Err(error) if UringIoEngine::is_unavailable_error(&error) => {
                        return sync(Some(sync_files));
                    }
                    Err(error) => return Err(CacheError::Io(error)),
                }
            }
            #[cfg(not(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            )))]
            return sync(runtime_files);
            #[allow(unreachable_code)]
            sync(None)
        }
        IoEngineKind::IoUring => {
            #[cfg(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            ))]
            {
                let files = runtime_files.ok_or_else(|| {
                    CacheError::InvalidConfig("io_uring requires a native file-backed cache".into())
                })?;
                UringIoEngine::new_with_files(files, config.io_queue_depth)
                    .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
                    .map_err(|error| {
                        CacheError::InvalidConfig(format!(
                            "io_uring is unavailable for this cache file: {error}"
                        ))
                    })
            }
            #[cfg(not(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            )))]
            {
                let _ = runtime_files;
                Err(CacheError::InvalidConfig(
                    "io_uring support is unavailable on this build target".into(),
                ))
            }
        }
    }
}

fn allocate_open_runtime(
    config: &CacheConfig,
    layout: &Layout,
    shared_host_writes: Option<Arc<HostWriteTracker>>,
) -> Result<OpenRuntime> {
    let resources = allocate_resources(config, layout)?;
    let delegated_policy = shared_host_writes.is_some();
    let (origin_fill_limiter, policy, key_ordering) =
        allocate_runtime_controls(config, shared_host_writes)?;
    // Allocate every fallible runtime workspace before creating or modifying
    // the cache path. A resource rejection must not leave an empty file.
    let index = allocate_index(config.index_slots, layout.region_count as usize)?;
    let region_count = layout.region_count as usize;
    let regions = try_region_workspace(region_count, "region metadata cannot be allocated")?;
    let recovery_order =
        try_region_workspace(region_count, "recovery workspace cannot be allocated")?;
    let checkpoint_regions = try_region_workspace(
        region_count,
        "checkpoint region workspace cannot be allocated",
    )?;
    let background_regions = try_region_workspace(
        region_count,
        "background recovery workspace cannot be allocated",
    )?;
    let read_regions = try_region_workspace(region_count, "read region view cannot be allocated")?;
    let region_valid_bytes = try_atomic_u64_workspace(
        region_count,
        "region valid-byte counters cannot be allocated",
    )?;
    let region_reinserted_bytes = try_atomic_u64_workspace(
        region_count,
        "region reinsertion counters cannot be allocated",
    )?;
    let region_reinsert_pending = try_atomic_u64_workspace(
        region_count,
        "region reinsertion pending counters cannot be allocated",
    )?;
    Ok(OpenRuntime {
        index,
        regions,
        recovery_order,
        checkpoint_regions,
        background_regions,
        read_regions,
        key_ordering,
        resources,
        policy,
        delegated_policy,
        origin_fill_limiter,
        region_valid_bytes,
        region_reinserted_bytes,
        region_reinsert_pending,
    })
}

fn allocate_runtime_controls(
    config: &CacheConfig,
    shared_host_writes: Option<Arc<HostWriteTracker>>,
) -> Result<(
    Option<Arc<OriginFillLimiter>>,
    Arc<PolicyController>,
    KeyOrdering,
)> {
    let origin_fill_limiter = config
        .origin_fill_config
        .map(OriginFillLimiter::try_new)
        .transpose()
        .map_err(|message| CacheError::InvalidConfig(message.into()))?;
    let policy = if let Some(host_writes) = shared_host_writes {
        PolicyController::try_new_with_external_host_writes(
            config.admission_mode,
            &config.namespace_configs,
            host_writes,
            config.device_health_policy,
        )
    } else {
        PolicyController::try_new_with_health(
            config.admission_mode,
            &config.namespace_configs,
            config.daily_host_write_budget_bytes,
            config.daily_host_write_baseline,
            config.device_health_policy,
        )
    }
    .map(Arc::new)
    .map_err(|error| CacheError::InvalidConfig(error.to_string()))?;
    let key_ordering = KeyOrdering::try_new()?;
    Ok((origin_fill_limiter, policy, key_ordering))
}

fn allocate_index(slot_count: usize, region_count: usize) -> Result<Arc<ShardedIndex>> {
    ShardedIndex::try_new(slot_count, region_count)
        .map(Arc::new)
        .map_err(|_| {
            CacheError::InvalidConfig(format!("index_slots ({slot_count}) cannot be allocated"))
        })
}

fn try_region_workspace<T>(count: usize, error: &'static str) -> Result<Vec<T>> {
    let mut workspace = Vec::new();
    workspace
        .try_reserve_exact(count)
        .map_err(|_| CacheError::InvalidConfig(error.into()))?;
    Ok(workspace)
}

fn try_atomic_u64_workspace(count: usize, error: &'static str) -> Result<Vec<AtomicU64>> {
    let mut workspace = Vec::new();
    workspace
        .try_reserve_exact(count)
        .map_err(|_| CacheError::InvalidConfig(error.into()))?;
    workspace.resize_with(count, || AtomicU64::new(0));
    Ok(workspace)
}

fn build_region_queues(regions: &[RegionMeta]) -> Result<(VecDeque<u32>, VecDeque<u32>)> {
    let mut free = VecDeque::new();
    let mut sealed = VecDeque::new();
    free.try_reserve(regions.len())
        .map_err(|_| CacheError::InvalidConfig("free Region queue cannot be allocated".into()))?;
    sealed
        .try_reserve(regions.len())
        .map_err(|_| CacheError::InvalidConfig("sealed Region queue cannot be allocated".into()))?;
    for region in regions {
        match region.header.state {
            RegionState::Free => free.push_back(region.header.region_id),
            RegionState::Sealed => sealed.push_back(region.header.region_id),
            RegionState::Active => {}
        }
    }
    sealed
        .make_contiguous()
        .sort_unstable_by_key(|region_id| regions[*region_id as usize].header.created_seqno);
    Ok((free, sealed))
}

fn checkpoint_accounting_workspace_bytes(
    config: &CacheConfig,
    region_count: usize,
) -> Result<usize> {
    let namespace_count = config
        .namespace_configs
        .len()
        .checked_add(usize::from(
            !config
                .namespace_configs
                .iter()
                .any(|namespace| namespace.namespace() == 0),
        ))
        .ok_or_else(|| CacheError::InvalidConfig("checkpoint namespace count overflow".into()))?;
    region_count
        .checked_mul(std::mem::size_of::<u64>())
        .and_then(|bytes| {
            namespace_count
                .checked_mul(std::mem::size_of::<NamespaceUsage>())
                .and_then(|namespace_bytes| bytes.checked_add(namespace_bytes))
        })
        .ok_or_else(|| {
            CacheError::InvalidConfig("checkpoint accounting workspace size overflow".into())
        })
}

fn allocate_resources(config: &CacheConfig, layout: &Layout) -> Result<Arc<ResourceController>> {
    let region_record_cap =
        usize::try_from(config.region_size - REGION_HEADER_SIZE as u64).unwrap_or(usize::MAX);
    // Format V1 permits extra record padding beyond the minimum key/value
    // payload size. Reopen and recovery must therefore accept every packed
    // record length that fits in a region, including records produced later by
    // batched/direct-I/O writers.
    let persisted_record_cap = (MAX_RECORD_LEN as usize).min(region_record_cap);
    let max_buffer_bytes = aligned_buffer_capacity(persisted_record_cap)
        .ok_or_else(|| CacheError::InvalidConfig("aligned buffer size overflow".into()))?;
    let region_count = layout.region_count as usize;
    let index_bytes = ShardedIndex::allocation_bytes(config.index_slots, region_count)
        .ok_or_else(|| CacheError::InvalidConfig("index memory size overflow".into()))?;
    let region_bytes = region_count
        .checked_mul(std::mem::size_of::<RegionMeta>())
        .ok_or_else(|| CacheError::InvalidConfig("region metadata size overflow".into()))?;
    let read_view_region_bytes = region_count
        .checked_mul(std::mem::size_of::<RwLock<RegionMeta>>())
        .ok_or_else(|| CacheError::InvalidConfig("read view memory size overflow".into()))?;
    let recovery_bytes = region_count
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| CacheError::InvalidConfig("recovery workspace size overflow".into()))?;
    let region_queue_bytes = region_count
        .checked_mul(2 * std::mem::size_of::<u32>())
        .ok_or_else(|| CacheError::InvalidConfig("Region queue memory size overflow".into()))?;
    let checkpoint_region_bytes = region_count
        .checked_mul(std::mem::size_of::<CheckpointRegionSnapshot>())
        .ok_or_else(|| {
            CacheError::InvalidConfig("checkpoint region memory size overflow".into())
        })?;
    let checkpoint_accounting_bytes = checkpoint_accounting_workspace_bytes(config, region_count)?;
    let append_queue_bytes = config
        .write_queue_depth
        .checked_add(2)
        .and_then(|slots| {
            std::mem::size_of::<AppendCommand>()
                .checked_add(APPEND_QUEUE_SLOT_OVERHEAD_BYTES)
                .and_then(|bytes| slots.checked_mul(bytes))
        })
        .and_then(|bytes| bytes.checked_mul(config.append_lanes))
        .ok_or_else(|| CacheError::InvalidConfig("append queue memory size overflow".into()))?;
    let append_completion_bytes = config
        .write_queue_depth
        .checked_add(1)
        .and_then(|slots| slots.checked_mul(APPEND_COMPLETION_OVERHEAD_BYTES))
        .and_then(|bytes| bytes.checked_mul(config.append_lanes))
        .ok_or_else(|| {
            CacheError::InvalidConfig("append completion memory size overflow".into())
        })?;
    let key_ordering_bytes = KEY_ORDERING_SHARDS
        .checked_mul(std::mem::size_of::<Mutex<()>>())
        .ok_or_else(|| CacheError::InvalidConfig("key ordering memory size overflow".into()))?;
    let io_engine_bytes = config
        .io_queue_depth
        .checked_mul(IO_ENGINE_SLOT_OVERHEAD_BYTES)
        .ok_or_else(|| CacheError::InvalidConfig("I/O engine memory size overflow".into()))?;
    let policy_namespace_count = config.namespace_configs.len().saturating_add(1);
    let policy_bytes = AdmissionPolicy::allocation_bytes()
        .checked_add(
            policy_namespace_count
                .checked_mul(POLICY_NAMESPACE_SLOT_OVERHEAD_BYTES)
                .ok_or_else(|| {
                    CacheError::InvalidConfig("namespace policy memory size overflow".into())
                })?,
        )
        .and_then(|bytes| {
            region_count
                .checked_mul(3 * std::mem::size_of::<AtomicU64>())
                .and_then(|region_bytes| bytes.checked_add(region_bytes))
        })
        .and_then(|bytes| {
            SECOND_CHANCE_QUEUE_DEPTH
                .checked_mul(SECOND_CHANCE_QUEUE_SLOT_OVERHEAD_BYTES)
                .and_then(|queue_bytes| bytes.checked_add(queue_bytes))
        })
        .ok_or_else(|| CacheError::InvalidConfig("policy memory size overflow".into()))?;
    let observability_bytes = std::mem::size_of::<RequestTelemetry>()
        .checked_add(
            config
                .origin_fill_config
                .map_or(0, |_| std::mem::size_of::<OriginFillLimiter>()),
        )
        .ok_or_else(|| CacheError::InvalidConfig("observability memory size overflow".into()))?;
    // The async facade is lazy, but its maximum retained request inputs are
    // charged up front so creating it cannot escape the configured budget.
    // Queue capacity and active workers are counted separately because both
    // can own copied keys/values at the same time.
    let async_read_workers =
        async_read_worker_count(config.read_queue_depth, config.io_queue_depth);
    let async_mutation_workers =
        async_mutation_worker_count(config.write_queue_depth, config.append_lanes);
    let read_buffer_slots = async_read_workers;
    let write_buffer_slots = config.write_queue_depth.clamp(1, MAX_DATA_BUFFER_SLOTS);
    let async_read_slots = config
        .read_queue_depth
        .checked_add(async_read_workers)
        .ok_or_else(|| CacheError::InvalidConfig("async read slot count overflow".into()))?;
    let async_write_slots = config
        .write_queue_depth
        .checked_add(ASYNC_CONTROL_QUEUE_RESERVE)
        .and_then(|slots| slots.checked_add(async_mutation_workers))
        .ok_or_else(|| CacheError::InvalidConfig("async write slot count overflow".into()))?;
    let async_task_bytes = async_read_slots
        .checked_add(async_write_slots)
        .and_then(|slots| slots.checked_mul(ASYNC_TASK_OVERHEAD_BYTES))
        .ok_or_else(|| CacheError::InvalidConfig("async task memory size overflow".into()))?;
    let async_read_input_bytes = async_read_slots
        .checked_mul(MAX_KEY_SIZE)
        .ok_or_else(|| CacheError::InvalidConfig("async read input size overflow".into()))?;
    let async_write_input_size = config
        .max_key_size
        .checked_add(config.max_value_size)
        .map(|put_bytes| put_bytes.max(MAX_KEY_SIZE))
        .ok_or_else(|| CacheError::InvalidConfig("async write input size overflow".into()))?;
    let async_write_input_bytes = async_write_slots
        .checked_mul(async_write_input_size)
        .ok_or_else(|| CacheError::InvalidConfig("async write input memory overflow".into()))?;
    let base_memory_bytes = index_bytes
        .checked_add(region_bytes)
        .and_then(|bytes| bytes.checked_add(region_bytes))
        .and_then(|bytes| bytes.checked_add(read_view_region_bytes))
        .and_then(|bytes| bytes.checked_add(recovery_bytes))
        .and_then(|bytes| bytes.checked_add(region_queue_bytes))
        .and_then(|bytes| bytes.checked_add(checkpoint_region_bytes))
        .and_then(|bytes| bytes.checked_add(checkpoint_accounting_bytes))
        .and_then(|bytes| bytes.checked_add(append_queue_bytes))
        .and_then(|bytes| bytes.checked_add(append_completion_bytes))
        .and_then(|bytes| bytes.checked_add(key_ordering_bytes))
        .and_then(|bytes| bytes.checked_add(io_engine_bytes))
        .and_then(|bytes| bytes.checked_add(policy_bytes))
        .and_then(|bytes| bytes.checked_add(observability_bytes))
        .and_then(|bytes| bytes.checked_add(async_task_bytes))
        .and_then(|bytes| bytes.checked_add(async_read_input_bytes))
        .and_then(|bytes| bytes.checked_add(async_write_input_bytes))
        .and_then(|bytes| bytes.checked_add(RESOURCE_OVERHEAD_BYTES))
        .ok_or_else(|| CacheError::InvalidConfig("resource memory size overflow".into()))?;
    let resources = ResourceController::try_new(ResourceLimits {
        memory_budget_bytes: config.memory_budget_bytes,
        base_memory_bytes,
        max_buffer_bytes,
        read_queue_depth: config.read_queue_depth,
        write_queue_depth: config.write_queue_depth,
        read_buffer_slots,
        write_buffer_slots,
        control_concurrency: config.append_lanes,
        backpressure: config.backpressure,
        write_budget_bytes_per_second: config.write_budget_bytes_per_second,
    })
    .map_err(|error| CacheError::InvalidConfig(error.to_string()))?;
    Ok(Arc::new(resources))
}

fn async_mutation_worker_count(write_queue_depth: usize, append_lanes: usize) -> usize {
    write_queue_depth
        .min(append_lanes.saturating_mul(ASYNC_MUTATION_WORKERS_PER_LANE))
        .min(MAX_ASYNC_MUTATION_WORKERS)
}

fn write_backend_tracked(
    io: &dyn IoBackend,
    host_writes: &HostWriteTracker,
    point: WritePoint,
    kind: HostWriteKind,
    bytes: &[u8],
    offset: u64,
) -> Result<()> {
    host_writes.record_write(kind, bytes.len() as u64);
    match write_all_at(io, point, bytes, offset) {
        Ok(()) => Ok(()),
        Err(error) => {
            host_writes.record_write_failure();
            Err(CacheError::Io(error))
        }
    }
}

fn sync_backend_tracked(
    io: &dyn IoBackend,
    host_writes: &HostWriteTracker,
    point: SyncPoint,
    mode: SyncMode,
) -> Result<()> {
    match io.sync(point, mode) {
        Ok(()) => Ok(()),
        Err(error) => {
            host_writes.record_write_failure();
            Err(CacheError::Io(error))
        }
    }
}

fn format_state(
    io: &dyn IoBackend,
    host_writes: &HostWriteTracker,
    config: &CacheConfig,
    layout: &Layout,
    previous_generation: Option<u64>,
    index: Arc<ShardedIndex>,
    mut regions: Vec<RegionMeta>,
) -> Result<State> {
    let dirty_generation =
        previous_generation
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(CacheError::CorruptMetadata(
                "superblock generation overflow while formatting",
            ))?;
    // Preflight the final clean generation before truncating or writing any
    // cache bytes. A near-exhausted corrupt generation must be refused without
    // leaving a newly written dirty marker behind.
    let clean_generation = dirty_generation
        .checked_add(1)
        .ok_or(CacheError::CorruptMetadata(
            "superblock generation overflow while formatting",
        ))?;
    // Formatting is also the terminal fallback after a partially decoded or
    // partially replayed checkpoint. Never carry those locations into the new
    // region incarnations.
    index.clear();
    let next_seqno = u64::try_from(config.append_lanes)
        .ok()
        .and_then(|lanes| lanes.checked_add(1))
        .ok_or(CacheError::CorruptMetadata(
            "append lane sequence metadata overflow while formatting",
        ))?;
    let mut superblock = Superblock {
        generation: dirty_generation,
        region_size: config.region_size,
        region_count: layout.region_count,
        epoch: 1,
        epoch_start_seqno: 1,
        next_seqno,
        hash_seed: config.hash_seed,
        clean: false,
    };

    regions.clear();
    debug_assert!(regions.capacity() >= layout.region_count as usize);
    for region_id in 0..layout.region_count {
        regions.push(RegionMeta {
            header: RegionHeader::free(region_id, 0),
            used: REGION_HEADER_SIZE as u64,
            max_seqno: 0,
        });
    }
    let mut active_regions = Vec::new();
    active_regions
        .try_reserve_exact(config.append_lanes)
        .map_err(|_| CacheError::InvalidConfig("append lane state cannot be allocated".into()))?;
    for (lane, region) in regions.iter_mut().enumerate().take(config.append_lanes) {
        let region_id = lane as u32;
        let active = RegionHeader {
            region_id,
            incarnation: 1,
            state: RegionState::Active,
            created_seqno: lane as u64 + 1,
            used: REGION_HEADER_SIZE as u64,
        };
        *region = RegionMeta {
            header: active,
            used: REGION_HEADER_SIZE as u64,
            max_seqno: 0,
        };
        active_regions.push(region_id);
    }
    if !index.reset_visibility_for_restore(
        superblock.epoch_start_seqno,
        regions.iter().map(|region| match region.header.state {
            RegionState::Free => RegionGeneration::Free,
            RegionState::Active | RegionState::Sealed => RegionGeneration::Allocated {
                created_seqno: region.header.created_seqno,
            },
        }),
    ) {
        return Err(CacheError::CorruptMetadata(
            "formatted Region generations do not match the index",
        ));
    }

    // A recognized prior cache may have a valid checkpoint tail. Remove and
    // durably fence that tail before publishing a format marker which could
    // otherwise resemble the next dirty generation of the old cache. A new
    // empty file keeps the marker-first ordering so an interrupted extension
    // remains recognizable as reserved Format V1 state.
    let truncated_before_marker = previous_generation.is_some();
    if truncated_before_marker {
        io.preallocate(layout.file_len)?;
        sync_backend_tracked(io, host_writes, SyncPoint::FormatTruncate, SyncMode::All)?;
    }

    // Region initialization only starts after the dirty marker has reached
    // stable storage. All fallible engine workspace allocation has already
    // completed at this point.
    let encoded = superblock.encode();
    write_backend_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        SUPERBLOCK_A_OFFSET,
    )?;
    write_backend_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        SUPERBLOCK_B_OFFSET,
    )?;
    sync_backend_tracked(io, host_writes, SyncPoint::FormatDirty, SyncMode::All)?;
    // Extend/truncate only after an ownership marker is durable. A crash after
    // only file-length metadata persists is handled as reserved interrupted
    // V1; a partial marker remains recognizable by its reserved prefix.
    if !truncated_before_marker {
        io.preallocate(layout.file_len)?;
    }

    for region in regions.iter() {
        write_backend_tracked(
            io,
            host_writes,
            WritePoint::RegionHeader,
            HostWriteKind::Metadata,
            &region.header.encode(),
            region_base(&superblock, region.header.region_id)?,
        )?;
    }
    sync_backend_tracked(io, host_writes, SyncPoint::FormatRegions, SyncMode::Data)?;

    // Publish the initialized regions only after all headers are durable.
    superblock.generation = clean_generation;
    superblock.clean = true;
    let encoded = superblock.encode();
    write_backend_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        SUPERBLOCK_A_OFFSET,
    )?;
    write_backend_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        SUPERBLOCK_B_OFFSET,
    )?;
    sync_backend_tracked(io, host_writes, SyncPoint::FormatClean, SyncMode::Data)?;
    let (free_regions, sealed_regions) = build_region_queues(&regions)?;
    Ok(State {
        superblock,
        regions,
        active_regions,
        free_regions,
        sealed_regions,
        index,
        checkpoint_slot: None,
        reclaiming_region: None,
        reclaim_ready_region: None,
        stats: CacheStats::default(),
        status: CacheStatus::Healthy,
        lock_held: true,
        runtime_accounting_restored: false,
    })
}

#[allow(clippy::too_many_arguments)]
fn recover_clean_checkpoint_state(
    io: &dyn IoBackend,
    superblock: Superblock,
    checkpoint: LoadedCheckpoint,
    index: Arc<ShardedIndex>,
    checkpoint_regions: &[CheckpointRegionSnapshot],
    expected_append_lanes: usize,
    regions: &mut Vec<RegionMeta>,
    ordered: &mut Vec<u32>,
) -> Result<State> {
    let started = Instant::now();
    if !superblock.clean || checkpoint_regions.len() != superblock.region_count as usize {
        return Err(CacheError::CorruptMetadata(
            "clean checkpoint metadata does not match its superblock",
        ));
    }
    regions.clear();
    for region_id in 0..superblock.region_count {
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        read_exact_at(io, &mut encoded, region_base(&superblock, region_id)?)?;
        let header = RegionHeader::decode(&encoded)
            .filter(|header| header.region_id == region_id)
            .ok_or(CacheError::CorruptMetadata("invalid region header"))?;
        let checkpoint = checkpoint_regions[region_id as usize];
        if header.incarnation != checkpoint.incarnation
            || header.state != checkpoint.state
            || header.created_seqno != checkpoint.created_seqno
            || header.used != checkpoint.used
        {
            return Err(CacheError::CorruptMetadata(
                "region header does not match index checkpoint",
            ));
        }
        regions.push(RegionMeta {
            header,
            used: header.used,
            max_seqno: checkpoint.max_seqno,
        });
    }
    let active_regions = validate_recovery_topology(
        &superblock,
        regions,
        expected_append_lanes,
        ordered,
        true,
        false,
    )?;
    let active_regions = restore_active_region_lanes(
        io,
        &superblock,
        regions,
        &active_regions,
        Some(checkpoint_regions),
        expected_append_lanes,
    )?;
    let recovered_entries = index.value_len(superblock.epoch_start_seqno) as u64;
    let (free_regions, sealed_regions) = build_region_queues(regions)?;
    Ok(State {
        superblock,
        regions: std::mem::take(regions),
        active_regions,
        free_regions,
        sealed_regions,
        index,
        checkpoint_slot: Some(checkpoint.header.slot),
        reclaiming_region: None,
        reclaim_ready_region: None,
        stats: CacheStats {
            recovered_entries,
            checkpoint_loads: 1,
            recovery_bytes_scanned: u64::from(superblock.region_count)
                .saturating_mul(REGION_HEADER_SIZE as u64),
            recovery_elapsed_us: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            recovery_regions_completed: u64::from(superblock.region_count),
            recovery_regions_total: u64::from(superblock.region_count),
            ..CacheStats::default()
        },
        status: CacheStatus::Healthy,
        lock_held: true,
        runtime_accounting_restored: true,
    })
}

fn prepare_miss_only_recovery_state(
    superblock: Superblock,
    checkpoint: LoadedCheckpoint,
    index: Arc<ShardedIndex>,
    checkpoint_regions: &[CheckpointRegionSnapshot],
    expected_append_lanes: usize,
    regions: &mut Vec<RegionMeta>,
    ordered: &mut Vec<u32>,
) -> Result<State> {
    regions.clear();
    for checkpoint in checkpoint_regions {
        regions.push(RegionMeta {
            header: RegionHeader {
                region_id: checkpoint.region_id,
                incarnation: checkpoint.incarnation,
                state: checkpoint.state,
                created_seqno: checkpoint.created_seqno,
                used: checkpoint.used,
            },
            used: checkpoint.used,
            max_seqno: checkpoint.max_seqno,
        });
    }
    let active_regions = validate_recovery_topology(
        &superblock,
        regions,
        expected_append_lanes,
        ordered,
        false,
        false,
    )?;
    let (free_regions, sealed_regions) = build_region_queues(regions)?;
    Ok(State {
        superblock,
        regions: std::mem::take(regions),
        active_regions,
        free_regions,
        sealed_regions,
        index,
        checkpoint_slot: Some(checkpoint.header.slot),
        reclaiming_region: None,
        reclaim_ready_region: None,
        stats: CacheStats {
            checkpoint_loads: 1,
            recovery_regions_total: u64::from(superblock.region_count),
            ..CacheStats::default()
        },
        status: CacheStatus::MissOnly,
        lock_held: true,
        runtime_accounting_restored: true,
    })
}

#[allow(clippy::too_many_arguments)]
fn recover_dirty_checkpoint_state(
    io: &dyn IoBackend,
    host_writes: &HostWriteTracker,
    reclaim_mode: ReclaimMode,
    mut superblock: Superblock,
    checkpoint: LoadedCheckpoint,
    index: Arc<ShardedIndex>,
    resources: &ResourceController,
    checkpoint_regions: &[CheckpointRegionSnapshot],
    expected_append_lanes: usize,
    regions: &mut Vec<RegionMeta>,
    ordered: &mut Vec<u32>,
    cancel: Option<&AtomicBool>,
    progress: Option<(&AtomicU64, &AtomicU64)>,
) -> Result<State> {
    let started = Instant::now();
    if superblock.clean || checkpoint_regions.len() != superblock.region_count as usize {
        return Err(CacheError::CorruptMetadata(
            "dirty checkpoint metadata does not match its superblock",
        ));
    }
    if let Some((done, total)) = progress {
        done.store(0, Ordering::Release);
        total.store(u64::from(superblock.region_count), Ordering::Release);
    }
    // The checkpoint describes the last fully published topology, so it is
    // also the authoritative Format V1 encoding of the configured lane count.
    // Validate it before accepting a current topology that may be between the
    // seal and activate writes of a region rotation.
    let checkpoint_active_count = checkpoint_regions
        .iter()
        .filter(|region| region.state == RegionState::Active)
        .count();
    if checkpoint_active_count != expected_append_lanes {
        return Err(CacheError::InvalidConfig(format!(
            "existing cache has {checkpoint_active_count} active append lanes, configured {expected_append_lanes}"
        )));
    }
    let checkpoint_lanes_complete =
        checkpoint_lane_snapshot_complete(Some(checkpoint_regions), expected_append_lanes)?;
    let epoch_advanced = superblock.epoch > checkpoint.header.epoch;
    if epoch_advanced {
        index.advance_clear_floor(superblock.epoch_start_seqno);
    }
    regions.clear();
    for region_id in 0..superblock.region_count {
        if cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire)) {
            return Err(CacheError::Cancelled);
        }
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        read_exact_at(io, &mut encoded, region_base(&superblock, region_id)?)?;
        let header = RegionHeader::decode(&encoded)
            .filter(|header| header.region_id == region_id)
            .ok_or(CacheError::CorruptMetadata("invalid region header"))?;
        if header.used > superblock.region_size {
            return Err(CacheError::CorruptMetadata(
                "region used cursor exceeds its boundary",
            ));
        }
        let prior = checkpoint_regions[region_id as usize];
        if header.incarnation < prior.incarnation {
            return Err(CacheError::CorruptMetadata(
                "region incarnation moved backwards",
            ));
        }
        let same_incarnation = header.incarnation == prior.incarnation;
        if same_incarnation {
            let valid_transition = matches!(
                (prior.state, header.state),
                (RegionState::Free, RegionState::Free)
                    | (RegionState::Active, RegionState::Active)
                    | (RegionState::Active, RegionState::Sealed)
                    | (RegionState::Sealed, RegionState::Sealed)
            );
            if !valid_transition
                || header.created_seqno != prior.created_seqno
                || header.used < prior.used
            {
                return Err(CacheError::CorruptMetadata(
                    "region changed without a new incarnation",
                ));
            }
        } else if header.state == RegionState::Free
            || header.created_seqno <= checkpoint.header.max_seqno
        {
            return Err(CacheError::CorruptMetadata(
                "reused region has invalid generation metadata",
            ));
        }
        if !same_incarnation
            && index
                .invalidate_region_generation(
                    region_id,
                    RegionGeneration::Allocated {
                        created_seqno: header.created_seqno,
                    },
                )
                .is_none()
        {
            return Err(CacheError::CorruptMetadata(
                "reused Region is out of index bounds",
            ));
        }
        regions.push(RegionMeta {
            header,
            used: header.used,
            max_seqno: if same_incarnation { prior.max_seqno } else { 0 },
        });
    }
    let active_candidates = validate_recovery_topology(
        &superblock,
        regions,
        expected_append_lanes,
        ordered,
        false,
        true,
    )?;
    let mut scratch = resources
        .recovery_buffer()
        .map_err(CacheError::Overloaded)?;
    let mut max_seqno = checkpoint
        .header
        .max_seqno
        .max(superblock.next_seqno.saturating_sub(1));
    for region in regions.iter() {
        max_seqno = max_seqno.max(region.header.created_seqno);
    }
    let mut regions_scanned = 0_u64;
    let mut records_scanned = 0_u64;
    let mut bytes_scanned =
        u64::from(superblock.region_count).saturating_mul(REGION_HEADER_SIZE as u64);
    for region_id in 0..superblock.region_count {
        if cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire)) {
            return Err(CacheError::Cancelled);
        }
        let prior = checkpoint_regions[region_id as usize];
        let current = regions[region_id as usize].header;
        if current.state == RegionState::Free {
            if let Some((done, _)) = progress {
                done.fetch_add(1, Ordering::AcqRel);
            }
            continue;
        }
        let same_incarnation = current.incarnation == prior.incarnation;
        let mut cursor = if same_incarnation {
            prior.used
        } else {
            REGION_HEADER_SIZE as u64
        };
        let required_end = current.used;
        let scan_active_tail = current.state == RegionState::Active;
        if cursor < required_end || scan_active_tail {
            regions_scanned = regions_scanned.saturating_add(1);
        }
        let mut last_seqno = if same_incarnation && prior.max_seqno != 0 {
            Some(prior.max_seqno)
        } else {
            None
        };
        loop {
            if cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire)) {
                return Err(CacheError::Cancelled);
            }
            if !scan_active_tail && cursor >= required_end {
                break;
            }
            if cursor >= superblock.region_size {
                if cursor == superblock.region_size {
                    break;
                }
                return Err(CacheError::CorruptMetadata(
                    "incremental recovery cursor exceeds region",
                ));
            }
            if superblock.region_size - cursor < RECORD_HEADER_SIZE as u64 {
                if cursor < required_end {
                    return Err(CacheError::CorruptMetadata(
                        "persisted region tail cannot contain a record header",
                    ));
                }
                break;
            }
            let absolute = region_base(&superblock, region_id)?
                .checked_add(cursor)
                .ok_or(CacheError::CorruptMetadata("record offset overflow"))?;
            let mut encoded_header = [0_u8; RECORD_HEADER_SIZE];
            read_exact_at(io, &mut encoded_header, absolute)?;
            bytes_scanned = bytes_scanned.saturating_add(RECORD_HEADER_SIZE as u64);
            if encoded_header.iter().all(|byte| *byte == 0) {
                if cursor < required_end {
                    return Err(CacheError::CorruptMetadata(
                        "zero record appears inside persisted region extent",
                    ));
                }
                break;
            }
            let Some(header) = RecordHeader::decode(&encoded_header) else {
                return Err(CacheError::CorruptMetadata(
                    "non-zero incremental tail has an invalid record header",
                ));
            };
            if header.region_incarnation != current.incarnation {
                if cursor < required_end {
                    return Err(CacheError::CorruptMetadata(
                        "persisted record has the wrong region incarnation",
                    ));
                }
                break;
            }
            let record_end = cursor
                .checked_add(u64::from(header.record_len))
                .filter(|end| *end <= superblock.region_size)
                .ok_or(CacheError::CorruptMetadata(
                    "record crosses the recovered region boundary",
                ))?;
            if cursor < required_end && record_end > required_end {
                return Err(CacheError::CorruptMetadata(
                    "record crosses the persisted region extent",
                ));
            }
            if header.seqno <= checkpoint.header.max_seqno
                || header.seqno < current.created_seqno
                || last_seqno.is_some_and(|previous| header.seqno <= previous)
                || header.epoch == 0
                || header.epoch > superblock.epoch
                || (header.epoch == superblock.epoch
                    && header.seqno <= superblock.epoch_start_seqno)
                || (header.epoch < superblock.epoch && header.seqno >= superblock.epoch_start_seqno)
            {
                return Err(CacheError::CorruptMetadata(
                    "incremental record generation metadata is inconsistent",
                ));
            }
            let encoded = scratch
                .prepare(header.record_len as usize)
                .map_err(|()| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
            encoded[..RECORD_HEADER_SIZE].copy_from_slice(&encoded_header);
            let payload_offset = absolute.checked_add(RECORD_HEADER_SIZE as u64).ok_or(
                CacheError::CorruptMetadata("record payload offset overflow"),
            )?;
            read_exact_at(io, &mut encoded[RECORD_HEADER_SIZE..], payload_offset)?;
            bytes_scanned = bytes_scanned.saturating_add(
                u64::from(header.record_len).saturating_sub(RECORD_HEADER_SIZE as u64),
            );
            let payload_len = (header.key_len as usize)
                .checked_add(header.stored_len as usize)
                .ok_or(CacheError::CorruptMetadata(
                    "record payload length overflow",
                ))?;
            let payload_end = RECORD_HEADER_SIZE
                .checked_add(payload_len)
                .filter(|end| *end <= encoded.len())
                .ok_or(CacheError::CorruptMetadata(
                    "record payload exceeds its encoded length",
                ))?;
            let payload = &encoded[RECORD_HEADER_SIZE..payload_end];
            let key_end = header.key_len as usize;
            let encoded_key = &payload[..key_end];
            let namespace = decode_record_namespace(header.codec, encoded_key).ok_or(
                CacheError::CorruptMetadata("record namespace encoding is invalid"),
            )?;
            if crc32c(payload) != header.payload_crc
                || hash_record_key(superblock.hash_seed, header.codec, encoded_key)
                    != Some(header.key_hash)
            {
                return Err(CacheError::CorruptMetadata(
                    "record payload checksum or key hash mismatch",
                ));
            }
            if header.epoch == superblock.epoch {
                let cursor_u32 = u32::try_from(cursor)
                    .map_err(|_| CacheError::CorruptMetadata("record offset does not fit u32"))?;
                let location = PackedLocation::new(
                    region_id,
                    cursor_u32,
                    header.record_len,
                    header.kind == RecordKind::Tombstone,
                )
                .map_err(|_| CacheError::CorruptMetadata("record location cannot be packed"))?;
                index.apply_if_newer_with_metadata(
                    header.key_hash,
                    location,
                    header.seqno,
                    superblock.epoch_start_seqno,
                    namespace,
                    if header.codec.is_second_chance() {
                        INDEX_FLAG_SECOND_CHANCE_USED
                    } else {
                        0
                    },
                );
            }
            last_seqno = Some(header.seqno);
            max_seqno = max_seqno.max(header.seqno);
            records_scanned = records_scanned.saturating_add(1);
            cursor = record_end;
        }
        if current.state == RegionState::Sealed && cursor != required_end {
            return Err(CacheError::CorruptMetadata(
                "sealed region recovery did not reach its persisted cursor",
            ));
        }
        let region = &mut regions[region_id as usize];
        if current.state == RegionState::Active {
            region.used = cursor;
            region.header.used = cursor;
        }
        region.max_seqno = last_seqno.unwrap_or(0);
        if let Some((done, _)) = progress {
            done.fetch_add(1, Ordering::AcqRel);
        }
    }
    let mut active_regions = restore_active_region_lane_slots(
        io,
        &superblock,
        regions,
        &active_candidates,
        Some(checkpoint_regions),
        expected_append_lanes,
        checkpoint_lanes_complete,
    )?;
    let missing_lanes = active_regions
        .iter()
        .enumerate()
        .filter_map(|(lane_id, region_id)| region_id.is_none().then_some(lane_id))
        .collect::<Vec<_>>();
    if active_candidates.len() == expected_append_lanes {
        if !missing_lanes.is_empty() {
            return Err(CacheError::CorruptMetadata(
                "Active Region lane identity is incomplete",
            ));
        }
        superblock.next_seqno = max_seqno.checked_add(1).ok_or(CacheError::CorruptMetadata(
            "recovered sequence number overflow",
        ))?;
    } else {
        if active_candidates
            .len()
            .checked_add(1)
            .is_none_or(|count| count != expected_append_lanes)
            || !checkpoint_lanes_complete
            || missing_lanes.len() != 1
        {
            return Err(CacheError::CorruptMetadata(
                "interrupted Region rotation cannot be repaired safely",
            ));
        }
        if cancel.is_some_and(|cancel| cancel.load(Ordering::Acquire)) {
            return Err(CacheError::Cancelled);
        }
        superblock.next_seqno = repair_interrupted_rotation(
            io,
            host_writes,
            reclaim_mode,
            &superblock,
            index.as_ref(),
            regions,
            &mut active_regions,
            missing_lanes[0],
            max_seqno,
        )?;
    }
    let active_regions = active_regions
        .into_iter()
        .map(|region_id| {
            region_id.ok_or(CacheError::CorruptMetadata(
                "Active Region lane identity is incomplete",
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let recovered_entries = index.value_len(superblock.epoch_start_seqno) as u64;
    let (free_regions, sealed_regions) = build_region_queues(regions)?;
    Ok(State {
        superblock,
        regions: std::mem::take(regions),
        active_regions,
        free_regions,
        sealed_regions,
        index,
        checkpoint_slot: Some(checkpoint.header.slot),
        reclaiming_region: None,
        reclaim_ready_region: None,
        stats: CacheStats {
            recovered_entries,
            checkpoint_loads: 1,
            recovery_regions_scanned: regions_scanned,
            recovery_records_scanned: records_scanned,
            recovery_bytes_scanned: bytes_scanned,
            recovery_elapsed_us: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            recovery_regions_completed: u64::from(superblock.region_count),
            recovery_regions_total: u64::from(superblock.region_count),
            ..CacheStats::default()
        },
        status: CacheStatus::Healthy,
        lock_held: true,
        runtime_accounting_restored: false,
    })
}

#[allow(clippy::too_many_arguments)]
fn repair_interrupted_rotation(
    io: &dyn IoBackend,
    host_writes: &HostWriteTracker,
    reclaim_mode: ReclaimMode,
    superblock: &Superblock,
    index: &ShardedIndex,
    regions: &mut [RegionMeta],
    active_regions: &mut [Option<u32>],
    missing_lane: usize,
    max_seqno: u64,
) -> Result<u64> {
    if active_regions
        .get(missing_lane)
        .is_none_or(|region_id| region_id.is_some())
    {
        return Err(CacheError::CorruptMetadata(
            "interrupted Region rotation has no unique missing lane",
        ));
    }

    // A Free Region has never contained Format V1 records. If capacity is
    // exhausted, FIFO can safely sacrifice the strict oldest Sealed Region;
    // SecondChance may only consume the already-fenced empty Region that made
    // the interrupted foreground rotation possible.
    let free = regions
        .iter()
        .filter(|region| region.header.state == RegionState::Free)
        .map(|region| region.header.region_id)
        .min();
    let victim = free.or_else(|| match reclaim_mode {
        ReclaimMode::Fifo => regions
            .iter()
            .filter(|region| region.header.state == RegionState::Sealed)
            .min_by_key(|region| (region.header.created_seqno, region.header.region_id))
            .map(|region| region.header.region_id),
        ReclaimMode::SecondChance => regions
            .iter()
            .filter(|region| {
                region.header.state == RegionState::Sealed
                    && region.used == REGION_HEADER_SIZE as u64
                    && region.max_seqno == 0
            })
            .min_by_key(|region| (region.header.created_seqno, region.header.region_id))
            .map(|region| region.header.region_id),
    });
    let victim = victim.ok_or(CacheError::CorruptMetadata(
        "interrupted Region rotation has no safe replacement",
    ))?;
    let victim_index = victim as usize;
    let prior = *regions
        .get(victim_index)
        .ok_or(CacheError::CorruptMetadata(
            "interrupted Region rotation replacement is out of bounds",
        ))?;
    let incarnation = prior
        .header
        .incarnation
        .checked_add(1)
        .ok_or(CacheError::CorruptMetadata("region incarnation overflow"))?;
    // Preflight both sequence advances and the target offset before changing
    // any bytes. A repair Region consumes one generation sequence exactly like
    // background reclaim, even though it contains no record yet.
    let created_seqno = max_seqno.checked_add(1).ok_or(CacheError::CorruptMetadata(
        "recovered sequence number overflow",
    ))?;
    let next_seqno = created_seqno
        .checked_add(1)
        .ok_or(CacheError::CorruptMetadata(
            "recovered sequence number overflow",
        ))?;
    let offset = region_base(superblock, victim)?;
    let header = RegionHeader {
        region_id: victim,
        incarnation,
        state: RegionState::Active,
        created_seqno,
        used: REGION_HEADER_SIZE as u64,
    };

    write_backend_tracked(
        io,
        host_writes,
        WritePoint::RegionHeader,
        HostWriteKind::Metadata,
        &header.encode(),
        offset,
    )?;
    sync_backend_tracked(io, host_writes, SyncPoint::RegionRotation, SyncMode::Data)?;

    // A crash after the sync but before these in-memory changes simply leaves
    // another empty replacement Active. The deterministic lane restore handles
    // it on the next restart.
    index
        .invalidate_region_generation(victim, RegionGeneration::Allocated { created_seqno })
        .ok_or(CacheError::CorruptMetadata(
            "repaired Region is out of index bounds",
        ))?;
    regions[victim_index] = RegionMeta {
        header,
        used: REGION_HEADER_SIZE as u64,
        max_seqno: 0,
    };
    active_regions[missing_lane] = Some(victim);
    Ok(next_seqno)
}

fn validate_recovery_topology(
    superblock: &Superblock,
    regions: &[RegionMeta],
    expected_append_lanes: usize,
    ordered: &mut Vec<u32>,
    enforce_clean_sequence_bound: bool,
    allow_interrupted_rotation: bool,
) -> Result<Vec<u32>> {
    ordered.clear();
    for (region_id, region) in regions.iter().enumerate() {
        if region.header.region_id as usize != region_id
            || region.header.used != region.used
            || region.used < REGION_HEADER_SIZE as u64
            || region.used > superblock.region_size
        {
            return Err(CacheError::CorruptMetadata(
                "region recovery metadata is inconsistent",
            ));
        }
        match region.header.state {
            RegionState::Free
                if region.header.incarnation == 0
                    && region.header.created_seqno == 0
                    && region.used == REGION_HEADER_SIZE as u64
                    && region.max_seqno == 0 => {}
            RegionState::Free => {
                return Err(CacheError::CorruptMetadata(
                    "free region has non-empty metadata",
                ));
            }
            RegionState::Active | RegionState::Sealed
                if region.header.incarnation != 0
                    && region.header.created_seqno != 0
                    && (!enforce_clean_sequence_bound
                        || region.header.created_seqno < superblock.next_seqno) =>
            {
                ordered.push(region.header.region_id);
            }
            RegionState::Active | RegionState::Sealed => {
                return Err(CacheError::CorruptMetadata(
                    "allocated region has invalid generation metadata",
                ));
            }
        }
    }
    ordered.sort_unstable_by_key(|region_id| regions[*region_id as usize].header.created_seqno);
    if ordered.is_empty() {
        return Err(CacheError::CorruptMetadata(
            "checkpoint has no active region",
        ));
    }
    if ordered.len() < superblock.region_count as usize && ordered[0] != 0 {
        return Err(CacheError::CorruptMetadata(
            "partially allocated FIFO must start at region zero",
        ));
    }
    if ordered.windows(2).any(|pair| {
        regions[pair[0] as usize].header.created_seqno
            == regions[pair[1] as usize].header.created_seqno
    }) {
        return Err(CacheError::CorruptMetadata(
            "region creation sequence is duplicated",
        ));
    }
    let active_count = ordered
        .iter()
        .filter(|region_id| regions[**region_id as usize].header.state == RegionState::Active)
        .count();
    let interrupted_rotation = allow_interrupted_rotation
        && active_count
            .checked_add(1)
            .is_some_and(|count| count == expected_append_lanes);
    if active_count != expected_append_lanes && !interrupted_rotation {
        if allow_interrupted_rotation {
            return Err(CacheError::CorruptMetadata(
                "dirty Region topology has multiple interrupted rotations",
            ));
        }
        return Err(CacheError::InvalidConfig(format!(
            "existing cache has {active_count} active append lanes, configured {expected_append_lanes}"
        )));
    }
    let mut active_regions = Vec::new();
    active_regions
        .try_reserve_exact(active_count)
        .map_err(|_| CacheError::InvalidConfig("append lane state cannot be allocated".into()))?;
    active_regions.extend(
        ordered
            .iter()
            .copied()
            .filter(|region_id| regions[*region_id as usize].header.state == RegionState::Active),
    );
    Ok(active_regions)
}

/// Restore the stable hash-lane identity of each Active Region. Region
/// headers intentionally remain Format V1, so checkpoint v3 carries the fast
/// path while older checkpoints and full scans infer the lane from records.
fn restore_active_region_lanes(
    io: &dyn IoBackend,
    superblock: &Superblock,
    regions: &[RegionMeta],
    active_candidates: &[u32],
    checkpoint_regions: Option<&[CheckpointRegionSnapshot]>,
    expected_append_lanes: usize,
) -> Result<Vec<u32>> {
    if active_candidates.len() != expected_append_lanes {
        return Err(CacheError::CorruptMetadata(
            "active append lane count changed during recovery",
        ));
    }
    let allow_empty_replacements =
        checkpoint_lane_snapshot_complete(checkpoint_regions, expected_append_lanes)?;
    restore_active_region_lane_slots(
        io,
        superblock,
        regions,
        active_candidates,
        checkpoint_regions,
        expected_append_lanes,
        allow_empty_replacements,
    )?
    .into_iter()
    .map(|region_id| {
        region_id.ok_or(CacheError::CorruptMetadata(
            "Active Region lane identity is incomplete",
        ))
    })
    .collect()
}

/// Checkpoint v3 and later persist a complete lane permutation for the Active
/// Regions. Legacy v1/v2 snapshots have no lane bytes; keep their recovery
/// conservative rather than guessing ownership for multiple replacements.
fn checkpoint_lane_snapshot_complete(
    checkpoint_regions: Option<&[CheckpointRegionSnapshot]>,
    expected_append_lanes: usize,
) -> Result<bool> {
    let Some(checkpoint_regions) = checkpoint_regions else {
        return Ok(false);
    };
    let active = checkpoint_regions
        .iter()
        .filter(|region| region.state == RegionState::Active)
        .collect::<Vec<_>>();
    let lanes_present = active
        .iter()
        .filter(|region| region.lane_id.is_some())
        .count();
    if lanes_present == 0 {
        return Ok(false);
    }
    if active.len() != expected_append_lanes || lanes_present != active.len() {
        return Err(CacheError::CorruptMetadata(
            "checkpoint Active Region lane snapshot is incomplete",
        ));
    }
    let mut seen = vec![false; expected_append_lanes];
    for region in active {
        let lane_id = usize::from(region.lane_id.ok_or(CacheError::CorruptMetadata(
            "checkpoint Active Region lane snapshot is incomplete",
        ))?);
        let Some(seen) = seen.get_mut(lane_id) else {
            return Err(CacheError::CorruptMetadata(
                "checkpoint append lane id exceeds configured lane count",
            ));
        };
        if std::mem::replace(seen, true) {
            return Err(CacheError::CorruptMetadata(
                "checkpoint append lane id is duplicated",
            ));
        }
    }
    Ok(seen.into_iter().all(|seen| seen))
}

#[allow(clippy::too_many_arguments)]
fn restore_active_region_lane_slots(
    io: &dyn IoBackend,
    superblock: &Superblock,
    regions: &[RegionMeta],
    active_candidates: &[u32],
    checkpoint_regions: Option<&[CheckpointRegionSnapshot]>,
    expected_append_lanes: usize,
    allow_empty_replacements: bool,
) -> Result<Vec<Option<u32>>> {
    if active_candidates.len() > expected_append_lanes {
        return Err(CacheError::CorruptMetadata(
            "active append lane count exceeds configured lanes",
        ));
    }
    let mut active_regions = vec![None; expected_append_lanes];
    let mut unresolved = Vec::new();
    unresolved
        .try_reserve_exact(active_candidates.len())
        .map_err(|_| CacheError::InvalidConfig("append lane state cannot be allocated".into()))?;

    for &region_id in active_candidates {
        let region = regions
            .get(region_id as usize)
            .ok_or(CacheError::CorruptMetadata(
                "active region id is out of bounds",
            ))?;
        let checkpoint_lane = checkpoint_regions
            .and_then(|snapshots| snapshots.get(region_id as usize))
            .filter(|snapshot| {
                snapshot.region_id == region_id
                    && snapshot.incarnation == region.header.incarnation
                    && snapshot.state == RegionState::Active
            })
            .and_then(|snapshot| snapshot.lane_id)
            .map(usize::from);
        let inferred_lane = match checkpoint_lane {
            Some(lane_id) => Some(lane_id),
            None => infer_active_region_lane(io, superblock, region, expected_append_lanes)?,
        };
        let inferred_lane = inferred_lane.or_else(|| {
            let initial_lane = region_id as usize;
            (region.header.incarnation == 1
                && initial_lane < expected_append_lanes
                && region.header.created_seqno == u64::from(region_id) + 1)
                .then_some(initial_lane)
        });
        let Some(lane_id) = inferred_lane else {
            if region.used != REGION_HEADER_SIZE as u64 || region.max_seqno != 0 {
                return Err(CacheError::CorruptMetadata(
                    "non-empty Active Region lane identity is ambiguous",
                ));
            }
            unresolved.push(region_id);
            continue;
        };
        let Some(slot) = active_regions.get_mut(lane_id) else {
            return Err(CacheError::CorruptMetadata(
                "checkpoint append lane id exceeds configured lane count",
            ));
        };
        if slot.replace(region_id).is_some() {
            return Err(CacheError::CorruptMetadata(
                "multiple Active Regions claim one append lane",
            ));
        }
    }

    let missing = active_regions
        .iter()
        .enumerate()
        .filter_map(|(lane_id, region_id)| region_id.is_none().then_some(lane_id))
        .collect::<Vec<_>>();
    if allow_empty_replacements {
        unresolved.sort_unstable_by_key(|region_id| {
            let region = regions[*region_id as usize];
            (region.header.created_seqno, *region_id)
        });
        if unresolved.len() > missing.len() {
            return Err(CacheError::CorruptMetadata(
                "Active Region lane identity is ambiguous",
            ));
        }
        for (region_id, lane_id) in unresolved.into_iter().zip(missing) {
            active_regions[lane_id] = Some(region_id);
        }
    } else if unresolved.len() == 1 && missing.len() == 1 {
        active_regions[missing[0]] = Some(unresolved[0]);
    } else if !unresolved.is_empty() {
        return Err(CacheError::CorruptMetadata(
            "Active Region lane identity is ambiguous",
        ));
    }
    Ok(active_regions)
}

fn infer_active_region_lane(
    io: &dyn IoBackend,
    superblock: &Superblock,
    region: &RegionMeta,
    append_lanes: usize,
) -> Result<Option<usize>> {
    let mut cursor = REGION_HEADER_SIZE as u64;
    let mut inferred = None;
    while cursor < region.used {
        if region.used - cursor < RECORD_HEADER_SIZE as u64 {
            return Err(CacheError::CorruptMetadata(
                "active region lane scan ended inside a record header",
            ));
        }
        let absolute = region_base(superblock, region.header.region_id)?
            .checked_add(cursor)
            .ok_or(CacheError::CorruptMetadata(
                "active region lane scan offset overflow",
            ))?;
        let mut encoded = [0_u8; RECORD_HEADER_SIZE];
        read_exact_at(io, &mut encoded, absolute)?;
        let header = RecordHeader::decode(&encoded).ok_or(CacheError::CorruptMetadata(
            "active region lane scan found an invalid record header",
        ))?;
        let record_end = cursor
            .checked_add(u64::from(header.record_len))
            .filter(|end| *end <= region.used)
            .ok_or(CacheError::CorruptMetadata(
                "active region lane scan found a crossing record",
            ))?;
        if header.region_incarnation != region.header.incarnation {
            return Err(CacheError::CorruptMetadata(
                "active region lane scan found the wrong incarnation",
            ));
        }
        let lane_id = header.key_hash as usize % append_lanes;
        if inferred.is_some_and(|current| current != lane_id) {
            return Err(CacheError::CorruptMetadata(
                "Active Region contains records from multiple append lanes",
            ));
        }
        inferred = Some(lane_id);
        cursor = record_end;
    }
    Ok(inferred)
}

fn recover_state(
    io: &dyn IoBackend,
    superblock: Superblock,
    index: Arc<ShardedIndex>,
    resources: &ResourceController,
    expected_append_lanes: usize,
    regions: &mut Vec<RegionMeta>,
    ordered: &mut Vec<u32>,
) -> Result<State> {
    let started = Instant::now();
    if !superblock.clean
        || superblock.epoch == 0
        || superblock.epoch_start_seqno == 0
        || superblock.epoch_start_seqno >= superblock.next_seqno
    {
        return Err(CacheError::CorruptMetadata(
            "invalid clean-checkpoint sequence metadata",
        ));
    }

    regions.clear();
    ordered.clear();
    debug_assert!(regions.capacity() >= superblock.region_count as usize);
    debug_assert!(ordered.capacity() >= superblock.region_count as usize);
    for region_id in 0..superblock.region_count {
        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        read_exact_at(io, &mut encoded, region_base(&superblock, region_id)?)?;
        let header = RegionHeader::decode(&encoded)
            .filter(|header| header.region_id == region_id)
            .ok_or(CacheError::CorruptMetadata("invalid region header"))?;
        if header.used > superblock.region_size {
            return Err(CacheError::CorruptMetadata(
                "region used cursor exceeds its boundary",
            ));
        }
        match header.state {
            RegionState::Free
                if header.incarnation == 0
                    && header.created_seqno == 0
                    && header.used == REGION_HEADER_SIZE as u64 => {}
            RegionState::Free => {
                return Err(CacheError::CorruptMetadata(
                    "free region has non-empty metadata",
                ));
            }
            RegionState::Active | RegionState::Sealed
                if header.incarnation != 0
                    && header.created_seqno != 0
                    && header.created_seqno < superblock.next_seqno => {}
            RegionState::Active | RegionState::Sealed => {
                return Err(CacheError::CorruptMetadata(
                    "allocated region has invalid generation metadata",
                ));
            }
        }
        regions.push(RegionMeta {
            header,
            used: header.used,
            max_seqno: 0,
        });
    }
    if !index.reset_visibility_for_restore(
        superblock.epoch_start_seqno,
        regions.iter().map(|region| match region.header.state {
            RegionState::Free => RegionGeneration::Free,
            RegionState::Active | RegionState::Sealed => RegionGeneration::Allocated {
                created_seqno: region.header.created_seqno,
            },
        }),
    ) {
        return Err(CacheError::CorruptMetadata(
            "recovered Region generations do not match the index",
        ));
    }

    for region in regions.iter() {
        if region.header.state != RegionState::Free {
            ordered.push(region.header.region_id);
        }
    }
    ordered.sort_unstable_by_key(|region_id| regions[*region_id as usize].header.created_seqno);
    if ordered.is_empty() {
        return Err(CacheError::CorruptMetadata(
            "checkpoint has no active region",
        ));
    }
    if ordered.len() < superblock.region_count as usize && ordered[0] != 0 {
        return Err(CacheError::CorruptMetadata(
            "partially allocated FIFO must start at region zero",
        ));
    }
    for pair in ordered.windows(2) {
        let older = regions[pair[0] as usize].header;
        let newer = regions[pair[1] as usize].header;
        if older.created_seqno == newer.created_seqno {
            return Err(CacheError::CorruptMetadata(
                "region creation sequence is duplicated",
            ));
        }
    }
    // Region Header V1 carries no lane identifier. Keep this age-sorted list
    // only as the validated set of Active candidates; record hashes below
    // restore their stable runtime lane identity.
    let active_count = ordered
        .iter()
        .filter(|region_id| regions[**region_id as usize].header.state == RegionState::Active)
        .count();
    if active_count == 0 {
        return Err(CacheError::CorruptMetadata(
            "checkpoint has no active region",
        ));
    }
    if active_count != expected_append_lanes {
        return Err(CacheError::InvalidConfig(format!(
            "existing cache has {active_count} active append lanes, configured {expected_append_lanes}"
        )));
    }
    let mut active_regions = Vec::new();
    active_regions
        .try_reserve_exact(active_count)
        .map_err(|_| CacheError::InvalidConfig("append lane state cannot be allocated".into()))?;
    active_regions.extend(
        ordered
            .iter()
            .copied()
            .filter(|region_id| regions[*region_id as usize].header.state == RegionState::Active),
    );

    let mut scratch = resources
        .recovery_buffer()
        .map_err(CacheError::Overloaded)?;
    let mut records_scanned = 0_u64;
    let mut bytes_scanned =
        u64::from(superblock.region_count).saturating_mul(REGION_HEADER_SIZE as u64);
    for &region_id in ordered.iter() {
        let region = regions[region_id as usize].header;
        let scan_limit = region.used;
        let mut cursor = REGION_HEADER_SIZE as u64;
        // Global sequence numbers can interleave across concurrently written
        // regions. They must remain strictly increasing within one append
        // stream/region; cross-region freshness is resolved by apply_if_newer.
        let mut last_seqno_in_region: Option<u64> = None;
        while cursor < scan_limit {
            if scan_limit - cursor < RECORD_HEADER_SIZE as u64 {
                return Err(CacheError::CorruptMetadata(
                    "record does not fill the persisted region extent",
                ));
            }
            let absolute = region_base(&superblock, region_id)?
                .checked_add(cursor)
                .ok_or(CacheError::CorruptMetadata("record offset overflow"))?;
            let mut encoded_header = [0_u8; RECORD_HEADER_SIZE];
            read_exact_at(io, &mut encoded_header, absolute)?;
            let header = RecordHeader::decode(&encoded_header)
                .ok_or(CacheError::CorruptMetadata("invalid record header"))?;
            let record_len = u64::from(header.record_len);
            let record_end = cursor
                .checked_add(record_len)
                .filter(|end| *end <= scan_limit)
                .ok_or(CacheError::CorruptMetadata(
                    "record crosses the persisted region extent",
                ))?;
            if header.region_incarnation != region.incarnation
                || header.seqno == 0
                || header.seqno >= superblock.next_seqno
                || header.seqno < region.created_seqno
                || last_seqno_in_region.is_some_and(|previous| header.seqno <= previous)
                || header.epoch == 0
                || header.epoch > superblock.epoch
                || (header.epoch == superblock.epoch
                    && header.seqno <= superblock.epoch_start_seqno)
                || (header.epoch < superblock.epoch && header.seqno >= superblock.epoch_start_seqno)
            {
                return Err(CacheError::CorruptMetadata(
                    "record generation or sequence metadata is inconsistent",
                ));
            }

            let encoded = scratch
                .prepare(header.record_len as usize)
                .map_err(|()| CacheError::Overloaded(OverloadReason::ReadBufferUnavailable))?;
            encoded[..RECORD_HEADER_SIZE].copy_from_slice(&encoded_header);
            let payload_offset = absolute.checked_add(RECORD_HEADER_SIZE as u64).ok_or(
                CacheError::CorruptMetadata("record payload offset overflow"),
            )?;
            read_exact_at(io, &mut encoded[RECORD_HEADER_SIZE..], payload_offset)?;
            bytes_scanned = bytes_scanned.saturating_add(u64::from(header.record_len));
            let payload_len = (header.key_len as usize)
                .checked_add(header.stored_len as usize)
                .ok_or(CacheError::CorruptMetadata(
                    "record payload length overflow",
                ))?;
            let payload_end = RECORD_HEADER_SIZE
                .checked_add(payload_len)
                .filter(|end| *end <= encoded.len())
                .ok_or(CacheError::CorruptMetadata(
                    "record payload exceeds its encoded length",
                ))?;
            let payload = &encoded[RECORD_HEADER_SIZE..payload_end];
            let key_end = header.key_len as usize;
            let encoded_key = &payload[..key_end];
            let namespace = decode_record_namespace(header.codec, encoded_key).ok_or(
                CacheError::CorruptMetadata("record namespace encoding is invalid"),
            )?;
            if crc32c(payload) != header.payload_crc
                || hash_record_key(superblock.hash_seed, header.codec, encoded_key)
                    != Some(header.key_hash)
            {
                return Err(CacheError::CorruptMetadata(
                    "record payload checksum or key hash mismatch",
                ));
            }

            if header.epoch == superblock.epoch {
                let cursor_u32 = u32::try_from(cursor)
                    .map_err(|_| CacheError::CorruptMetadata("record offset does not fit u32"))?;
                let location = PackedLocation::new(
                    region_id,
                    cursor_u32,
                    header.record_len,
                    header.kind == RecordKind::Tombstone,
                )
                .map_err(|_| CacheError::CorruptMetadata("record location cannot be packed"))?;
                index.apply_if_newer_with_metadata(
                    header.key_hash,
                    location,
                    header.seqno,
                    superblock.epoch_start_seqno,
                    namespace,
                    if header.codec.is_second_chance() {
                        INDEX_FLAG_SECOND_CHANCE_USED
                    } else {
                        0
                    },
                );
            }
            last_seqno_in_region = Some(header.seqno);
            regions[region_id as usize].max_seqno = header.seqno;
            records_scanned = records_scanned.saturating_add(1);
            cursor = record_end;
        }
    }

    let active_regions = restore_active_region_lanes(
        io,
        &superblock,
        regions,
        &active_regions,
        None,
        expected_append_lanes,
    )?;
    let recovered_entries = index.value_len(superblock.epoch_start_seqno) as u64;
    let (free_regions, sealed_regions) = build_region_queues(regions)?;
    Ok(State {
        superblock,
        regions: std::mem::take(regions),
        active_regions,
        free_regions,
        sealed_regions,
        index,
        checkpoint_slot: None,
        reclaiming_region: None,
        reclaim_ready_region: None,
        stats: CacheStats {
            recovered_entries,
            recovery_regions_scanned: ordered.len() as u64,
            recovery_records_scanned: records_scanned,
            recovery_bytes_scanned: bytes_scanned,
            recovery_elapsed_us: started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
            recovery_regions_completed: u64::from(superblock.region_count),
            recovery_regions_total: u64::from(superblock.region_count),
            ..CacheStats::default()
        },
        status: CacheStatus::Healthy,
        lock_held: true,
        runtime_accounting_restored: false,
    })
}

fn read_superblock(io: &dyn IoBackend) -> Result<Option<Superblock>> {
    let file_len = io.len()?;
    if file_len == 0 {
        return Ok(None);
    }
    let mut best: Option<Superblock> = None;
    let mut first_io_error: Option<io::Error> = None;
    let mut unsupported: Option<u16> = None;
    let mut saw_corrupt_v1 = false;
    let mut saw_unrecognized = false;
    for offset in [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET]
        .into_iter()
        .take(SUPERBLOCK_COUNT)
    {
        let mut encoded = [0_u8; SUPERBLOCK_SIZE];
        if offset < file_len {
            let read_len = usize::try_from((file_len - offset).min(SUPERBLOCK_SIZE as u64))
                .map_err(|_| CacheError::CorruptMetadata("short superblock length overflow"))?;
            if let Err(error) = read_exact_at(io, &mut encoded[..read_len], offset) {
                first_io_error.get_or_insert(error);
                continue;
            }
        }
        match Superblock::probe(&encoded) {
            SuperblockProbe::Empty => {}
            SuperblockProbe::InterruptedV1 => {}
            SuperblockProbe::ValidV1(candidate) => {
                if best.is_none_or(|current| candidate.generation > current.generation) {
                    best = Some(candidate);
                }
            }
            SuperblockProbe::CorruptV1 => saw_corrupt_v1 = true,
            SuperblockProbe::Unsupported(version) => unsupported = Some(version),
            SuperblockProbe::Unrecognized => saw_unrecognized = true,
        }
    }
    if let Some(version) = unsupported {
        return Err(unsupported_format(version));
    }
    if let Some(candidate) = best {
        if candidate.clean || first_io_error.is_none() {
            return Ok(Some(candidate));
        }
    }
    if let Some(error) = first_io_error {
        return Err(CacheError::Io(error));
    }
    if saw_corrupt_v1 {
        return Ok(None);
    }
    if saw_unrecognized {
        return Err(CacheError::CorruptMetadata(
            "unrecognized non-empty cache file",
        ));
    }
    if file_len <= DATA_OFFSET {
        // A pwrite can make file-length metadata durable before its marker
        // bytes. Zero/partial metadata extents are reserved as interrupted V1.
        Ok(None)
    } else {
        Err(CacheError::CorruptMetadata(
            "unrecognized non-empty cache file",
        ))
    }
}

fn recognize_format_v1_for_reset(io: &dyn IoBackend) -> Result<()> {
    let file_len = io.len()?;
    let mut saw_valid = false;
    let mut unsupported = None;
    for offset in [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET]
        .into_iter()
        .take(SUPERBLOCK_COUNT)
    {
        let mut encoded = [0_u8; SUPERBLOCK_SIZE];
        if offset < file_len {
            let read_len = usize::try_from((file_len - offset).min(SUPERBLOCK_SIZE as u64))
                .map_err(|_| CacheError::CorruptMetadata("short superblock length overflow"))?;
            read_exact_at(io, &mut encoded[..read_len], offset)?;
        }
        match Superblock::probe(&encoded) {
            SuperblockProbe::ValidV1(_) => saw_valid = true,
            SuperblockProbe::Unsupported(version) => unsupported = Some(version),
            SuperblockProbe::Empty
            | SuperblockProbe::InterruptedV1
            | SuperblockProbe::CorruptV1
            | SuperblockProbe::Unrecognized => {}
        }
    }
    if let Some(version) = unsupported {
        return Err(unsupported_format(version));
    }
    if saw_valid {
        Ok(())
    } else {
        Err(CacheError::InvalidConfig(
            "reset requires an existing recognized Format V1 cache".into(),
        ))
    }
}

fn unsupported_format(version: u16) -> CacheError {
    CacheError::InvalidConfig(format!(
        "unsupported cache format version {version}; this build supports Format V1"
    ))
}

fn region_base(superblock: &Superblock, region_id: u32) -> Result<u64> {
    if region_id >= superblock.region_count {
        return Err(CacheError::CorruptMetadata("region id out of bounds"));
    }
    DATA_OFFSET
        .checked_add(u64::from(region_id) * superblock.region_size)
        .ok_or(CacheError::CorruptMetadata("region offset overflow"))
}

fn take_seqno(state: &mut State) -> Result<u64> {
    let seqno = state.superblock.next_seqno;
    state.superblock.next_seqno = seqno
        .checked_add(1)
        .ok_or(CacheError::CorruptMetadata("sequence number overflow"))?;
    Ok(seqno)
}

fn ensure_operational(state: &State) -> Result<()> {
    match state.status {
        CacheStatus::Healthy => Ok(()),
        CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
        CacheStatus::Closed => Err(CacheError::Closed),
    }
}

fn decode_cache_status(value: u8) -> CacheStatus {
    match value {
        value if value == CacheStatus::Healthy as u8 => CacheStatus::Healthy,
        value if value == CacheStatus::MissOnly as u8 => CacheStatus::MissOnly,
        value if value == CacheStatus::Poisoned as u8 => CacheStatus::Poisoned,
        value if value == CacheStatus::Closed as u8 => CacheStatus::Closed,
        _ => CacheStatus::Poisoned,
    }
}

fn put_reject_reason(reason: OverloadReason) -> RejectReason {
    match reason {
        OverloadReason::ReadQueueFull
        | OverloadReason::WriteQueueFull
        | OverloadReason::JournalCapacityFull
        | OverloadReason::CloseWaitersFull => RejectReason::SubmissionFull,
        OverloadReason::ReadBufferUnavailable | OverloadReason::WriteBufferUnavailable => {
            RejectReason::BufferUnavailable
        }
        OverloadReason::ReadTimeout | OverloadReason::WriteTimeout => {
            RejectReason::SubmissionTimeout
        }
    }
}

fn request_result_for_error(error: &CacheError) -> RequestResultClass {
    match error {
        CacheError::Io(_) => RequestResultClass::IoError,
        CacheError::CorruptMetadata(_) => RequestResultClass::Corrupt,
        CacheError::Overloaded(_) | CacheError::ReclaimBacklog => RequestResultClass::Overloaded,
        CacheError::Cancelled | CacheError::TimedOut => RequestResultClass::Cancelled,
        CacheError::InvalidConfig(_)
        | CacheError::Locked
        | CacheError::Closed
        | CacheError::Poisoned => RequestResultClass::Unavailable,
    }
}

fn cache_error_class(error: &CacheError) -> CacheErrorClass {
    match error {
        CacheError::Io(error) if error.raw_os_error() == Some(28) => CacheErrorClass::NoSpace,
        CacheError::Io(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            CacheErrorClass::Permission
        }
        CacheError::Io(_) => CacheErrorClass::DeviceIo,
        CacheError::InvalidConfig(_) => CacheErrorClass::InvalidConfig,
        CacheError::CorruptMetadata(_) => CacheErrorClass::CorruptMetadata,
        CacheError::Locked => CacheErrorClass::Locked,
        CacheError::Closed => CacheErrorClass::Closed,
        CacheError::Poisoned => CacheErrorClass::Poisoned,
        CacheError::Cancelled => CacheErrorClass::Cancelled,
        CacheError::TimedOut => CacheErrorClass::TimedOut,
        CacheError::Overloaded(_) => CacheErrorClass::Overloaded,
        CacheError::ReclaimBacklog => CacheErrorClass::ReclaimBacklog,
    }
}

fn enter_miss_only(state: &mut State) {
    if state.status == CacheStatus::Healthy {
        state.index.clear();
        state.status = CacheStatus::MissOnly;
    }
}

fn enter_failure_state(state: &mut State, error: &CacheError) {
    if state.status == CacheStatus::Closed {
        return;
    }
    state.index.clear();
    state.status = if matches!(error, CacheError::Io(_)) {
        CacheStatus::MissOnly
    } else {
        CacheStatus::Poisoned
    };
}

fn context_stop_error(context: Option<&TaskContext>) -> CacheError {
    match context.and_then(TaskContext::stop_reason) {
        Some(AsyncFailure::TimedOut) => CacheError::TimedOut,
        _ => CacheError::Cancelled,
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn hash_key(seed: u64, key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    hash_key_update(&mut hash, key);
    hash
}

fn rebuild_runtime_accounting(
    state: &State,
    policy: &PolicyController,
    account_namespaces: bool,
    region_valid_bytes: &[AtomicU64],
    region_reinserted_bytes: &[AtomicU64],
    region_reinsert_pending: &[AtomicU64],
) -> Result<()> {
    if account_namespaces {
        policy.namespaces().reset_live_bytes();
    }
    for counter in region_valid_bytes {
        counter.store(0, Ordering::Release);
    }
    for counter in region_reinserted_bytes {
        counter.store(0, Ordering::Release);
    }
    for counter in region_reinsert_pending {
        counter.store(0, Ordering::Release);
    }
    state.index.try_for_each_snapshot_entry(
        state.superblock.epoch_start_seqno,
        |_physical_slot, entry| -> Result<()> {
            let location = PackedLocation::try_from_raw(entry.location_raw)
                .map_err(|_| CacheError::CorruptMetadata("invalid live index location"))?;
            let counter = region_valid_bytes
                .get(location.region_id() as usize)
                .ok_or(CacheError::CorruptMetadata(
                    "live index region is out of bounds",
                ))?;
            atomic_saturating_add(counter, u64::from(location.record_len()));
            if account_namespaces
                && !location.is_tombstone()
                && policy.namespaces().contains(entry.namespace_id)
            {
                policy
                    .namespaces()
                    .restore_live_bytes(entry.namespace_id, u64::from(location.record_len()))
                    .map_err(|_| {
                        CacheError::CorruptMetadata("namespace live-byte accounting overflow")
                    })?;
            }
            Ok(())
        },
    )?;
    Ok(())
}

fn reset_reinsertion_accounting(
    region_reinserted_bytes: &[AtomicU64],
    region_reinsert_pending: &[AtomicU64],
) {
    for counter in region_reinserted_bytes {
        counter.store(0, Ordering::Release);
    }
    for counter in region_reinsert_pending {
        counter.store(0, Ordering::Release);
    }
}

fn namespace_usage(entry: IndexEntry) -> Option<NamespaceUsage> {
    (!entry.location.is_tombstone()).then_some(NamespaceUsage {
        namespace: entry.namespace_id,
        live_bytes: u64::from(entry.location.record_len()),
    })
}

fn namespace_reject_reason(reason: NamespaceRejectReason) -> RejectReason {
    match reason {
        NamespaceRejectReason::UnknownNamespace => RejectReason::NamespaceNotConfigured,
        NamespaceRejectReason::CapacityExceeded => RejectReason::NamespaceCapacityExceeded,
        NamespaceRejectReason::WriteBudgetExceeded => RejectReason::NamespaceWriteBudgetExceeded,
    }
}

fn atomic_saturating_add(counter: &AtomicU64, amount: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(amount);
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn atomic_saturating_sub(counter: &AtomicU64, amount: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_sub(amount);
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn ratio_bps(numerator: u64, denominator: u64) -> u64 {
    if denominator == 0 {
        return 0;
    }
    u64::try_from(
        u128::from(numerator)
            .saturating_mul(10_000)
            .checked_div(u128::from(denominator))
            .unwrap_or(0)
            .min(u128::from(u64::MAX)),
    )
    .unwrap_or(u64::MAX)
}

fn hash_namespaced_key(seed: u64, namespace: NamespaceId, key: &[u8]) -> u64 {
    if namespace == 0 {
        return hash_key(seed, key);
    }
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ seed;
    hash_key_update(&mut hash, NAMESPACE_HASH_DOMAIN);
    hash_key_update(&mut hash, &namespace.to_le_bytes());
    hash_key_update(&mut hash, key);
    hash
}

fn hash_record_key(seed: u64, codec: RecordCodec, encoded_key: &[u8]) -> Option<u64> {
    match codec {
        RecordCodec::PlainKey | RecordCodec::SecondChancePlainKey => {
            Some(hash_key(seed, encoded_key))
        }
        RecordCodec::NamespacedKey | RecordCodec::SecondChanceNamespacedKey => {
            let namespace = decode_record_namespace(codec, encoded_key)?;
            let key = encoded_key.get(NAMESPACE_KEY_PREFIX_SIZE..)?;
            Some(hash_namespaced_key(seed, namespace, key))
        }
    }
}

fn hash_key_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn record_codec(namespace: NamespaceId) -> RecordCodec {
    if namespace == 0 {
        RecordCodec::PlainKey
    } else {
        RecordCodec::NamespacedKey
    }
}

fn encoded_key_len(namespace: NamespaceId, raw_key_len: usize) -> Option<usize> {
    raw_key_len.checked_add(if namespace == 0 {
        0
    } else {
        NAMESPACE_KEY_PREFIX_SIZE
    })
}

fn encode_namespaced_key(
    output: &mut [u8],
    namespace: NamespaceId,
    raw_key: &[u8],
) -> std::result::Result<(), ()> {
    let expected = encoded_key_len(namespace, raw_key.len()).ok_or(())?;
    if output.len() != expected {
        return Err(());
    }
    if namespace == 0 {
        output.copy_from_slice(raw_key);
    } else {
        output[..NAMESPACE_KEY_PREFIX_SIZE].copy_from_slice(&namespace.to_le_bytes());
        output[NAMESPACE_KEY_PREFIX_SIZE..].copy_from_slice(raw_key);
    }
    Ok(())
}

fn decode_record_namespace(codec: RecordCodec, encoded_key: &[u8]) -> Option<NamespaceId> {
    match codec {
        RecordCodec::PlainKey | RecordCodec::SecondChancePlainKey => Some(0),
        RecordCodec::NamespacedKey | RecordCodec::SecondChanceNamespacedKey => {
            let bytes: [u8; NAMESPACE_KEY_PREFIX_SIZE] = encoded_key
                .get(..NAMESPACE_KEY_PREFIX_SIZE)?
                .try_into()
                .ok()?;
            let namespace = u32::from_le_bytes(bytes);
            (namespace != 0).then_some(namespace)
        }
    }
}

fn record_codec_matches_namespace(codec: RecordCodec, namespace: NamespaceId) -> bool {
    if namespace == 0 {
        matches!(
            codec,
            RecordCodec::PlainKey | RecordCodec::SecondChancePlainKey
        )
    } else {
        matches!(
            codec,
            RecordCodec::NamespacedKey | RecordCodec::SecondChanceNamespacedKey
        )
    }
}

fn namespaced_key_matches(
    encoded_key: &[u8],
    namespace: NamespaceId,
    expected_raw_key: &[u8],
) -> bool {
    decode_record_namespace(record_codec(namespace), encoded_key) == Some(namespace)
        && encoded_key.get(if namespace == 0 {
            0..
        } else {
            NAMESPACE_KEY_PREFIX_SIZE..
        }) == Some(expected_raw_key)
}

fn try_lock_exclusive(io: &dyn IoBackend) -> Result<()> {
    if let Err(error) = io.try_lock_exclusive() {
        if error.kind() == io::ErrorKind::WouldBlock {
            return Err(CacheError::Locked);
        }
        return Err(CacheError::Io(error));
    }
    Ok(())
}

fn unlock_file(io: &dyn IoBackend) -> Result<()> {
    if let Err(error) = io.unlock() {
        Err(CacheError::Io(error))
    } else {
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod m3_tests;

#[cfg(all(test, unix))]
mod m6_tests;

#[cfg(all(test, unix))]
mod m7_tests;

#[cfg(all(test, unix))]
mod m8_tests;

#[cfg(test)]
mod scale_config_tests {
    use super::*;

    #[test]
    fn expected_entry_sizing_supports_a_hundred_million_live_entries() {
        let config = CacheConfig::new(
            "unused-scale-diagnostics.cache",
            DATA_OFFSET + 16 * DEFAULT_REGION_SIZE,
        )
        .with_expected_entries(100_000_000)
        .with_memory_budget(6 * 1024 * 1024 * 1024);

        assert_eq!(config.index_slots, 125_000_000);
        assert_eq!(config.validate().unwrap().region_count, 16);
        let diagnostics = config.diagnostics().unwrap();
        assert_eq!(diagnostics.index_slots, 125_000_000);
        assert!(diagnostics.planned_memory_bytes < diagnostics.memory_budget_bytes);
    }

    #[test]
    fn async_mutation_workers_scale_per_lane_and_stay_bounded() {
        assert_eq!(async_mutation_worker_count(1, 8), 1);
        assert_eq!(async_mutation_worker_count(128, 1), 8);
        assert_eq!(async_mutation_worker_count(128, 4), 32);
        assert_eq!(async_mutation_worker_count(128, 8), 64);
    }

    #[test]
    fn diagnostics_charge_all_async_workers_and_read_view_locks() {
        let config = |regions, lanes| {
            CacheConfig::new(
                format!("unused-scale-{regions}-{lanes}.cache"),
                DATA_OFFSET + regions * DEFAULT_REGION_SIZE,
            )
            .with_index_slots(64)
            .with_max_key_size(64)
            .with_max_value_size(1024)
            .with_submission_queue_depths(1, 64)
            .with_append_lanes(lanes)
        };

        let one_lane = config(16, 1).diagnostics().unwrap();
        let eight_lanes = config(16, 8).diagnostics().unwrap();
        let worker_slot_delta =
            async_mutation_worker_count(64, 8) - async_mutation_worker_count(64, 1);
        let append_queue_delta = 7
            * (64 + 2)
            * (std::mem::size_of::<AppendCommand>() + APPEND_QUEUE_SLOT_OVERHEAD_BYTES);
        let append_completion_delta = 7 * (64 + 1) * APPEND_COMPLETION_OVERHEAD_BYTES;
        let async_worker_delta =
            worker_slot_delta * (ASYNC_TASK_OVERHEAD_BYTES + MAX_KEY_SIZE.max(64 + 1024));
        assert_eq!(
            eight_lanes.planned_memory_bytes - one_lane.planned_memory_bytes,
            (append_queue_delta + append_completion_delta + async_worker_delta) as u64
        );

        let sixteen_regions = config(16, 1).diagnostics().unwrap();
        let seventeen_regions = config(17, 1).diagnostics().unwrap();
        let index_region_delta = ShardedIndex::allocation_bytes(64, 17).unwrap()
            - ShardedIndex::allocation_bytes(64, 16).unwrap();
        let per_region_delta = index_region_delta
            + 2 * std::mem::size_of::<RegionMeta>()
            + std::mem::size_of::<RwLock<RegionMeta>>()
            + 3 * std::mem::size_of::<u32>()
            + std::mem::size_of::<CheckpointRegionSnapshot>()
            + std::mem::size_of::<u64>()
            + 3 * std::mem::size_of::<AtomicU64>();
        assert_eq!(
            seventeen_regions.planned_memory_bytes - sixteen_regions.planned_memory_bytes,
            per_region_delta as u64
        );
    }
}

#[cfg(all(test, unix))]
mod m4_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new() -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-m4-unfenced-{}-{nonce}.cache",
                std::process::id()
            )))
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn unfenced_mutation_never_unlocks_a_live_cache_instance() {
        let file = TestFile::new();
        let config = CacheConfig::new(&file.0, DATA_OFFSET + 2 * 16 * 1024)
            .with_region_size(16 * 1024)
            .with_index_slots(64)
            .with_max_key_size(256)
            .with_max_value_size(2048);
        let cache = config.clone().open().unwrap();
        cache.inner.engine.mark_unfenced_mutations_for_test();

        assert!(matches!(cache.close(), Err(CacheError::Io(_))));
        assert!(cache.stats().io_unfenced_mutations);
        assert!(matches!(config.clone().open(), Err(CacheError::Locked)));
        assert!(matches!(cache.close(), Err(CacheError::Io(_))));
        assert!(matches!(config.clone().open(), Err(CacheError::Locked)));

        // The test backend has no real kernel request retaining a duplicate;
        // dropping the simulated failed instance releases its final file ref.
        drop(cache);
        let reopened = config.open().unwrap();
        reopened.close().unwrap();
    }
}

#[cfg(all(test, unix))]
mod lifecycle_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn poisoned_operations_are_consistent_and_close_releases_the_lock() {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cache-rs-poisoned-{}-{nonce}.cache",
            std::process::id()
        ));
        let config = CacheConfig::new(&path, DATA_OFFSET + 2 * 8 * 1024)
            .with_region_size(8 * 1024)
            .with_index_slots(8)
            .with_max_key_size(64)
            .with_max_value_size(128);
        let cache = config.clone().open().unwrap();
        cache.inner.state.lock().unwrap().status = CacheStatus::Poisoned;
        cache.set_lifecycle(CacheStatus::Poisoned);

        assert!(matches!(cache.get(b"key"), Err(CacheError::Poisoned)));
        assert!(matches!(
            cache.put(
                "key",
                "value",
                PutOptions {
                    expires_at_unix_ms: Some(0),
                }
            ),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(
            cache.remove(&vec![b'x'; 1024]),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(cache.flush(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.clear(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.get(b"key"), Err(CacheError::Closed)));
        cache.close().unwrap();

        let reopened = config.clone().open().unwrap();
        drop(reopened);

        let cache = config.clone().open().unwrap();
        let panicking_clone = cache.clone();
        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _state = panicking_clone.inner.state.lock().unwrap();
            panic!("intentionally poison the cache state mutex");
        }));
        assert!(unwind.is_err());
        assert!(cache.inner.state.is_poisoned());
        assert!(matches!(cache.get(b"key"), Err(CacheError::Poisoned)));
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert!(!cache.inner.state.is_poisoned());
        assert!(matches!(cache.get(b"key"), Err(CacheError::Closed)));
        cache.close().unwrap();

        let reopened = config.open().unwrap();
        drop(reopened);
        drop(cache);
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(all(test, unix))]
mod m1_tests {
    use super::*;
    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};
    use std::os::unix::process::ExitStatusExt;
    use std::process::Command;
    use std::sync::atomic::{AtomicU64, Ordering};

    const EIO: i32 = 5;
    const ENOSPC: i32 = 28;
    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestFile(PathBuf);

    impl TestFile {
        fn new(name: &str) -> Self {
            let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-m1-{name}-{}-{nonce}.cache",
                std::process::id()
            )))
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn config(path: &Path) -> CacheConfig {
        CacheConfig::new(path, DATA_OFFSET + 2 * 16 * 1024)
            .with_region_size(16 * 1024)
            .with_index_slots(64)
            .with_max_key_size(256)
            .with_max_value_size(2048)
    }

    fn multi_lane_config(path: &Path) -> CacheConfig {
        CacheConfig::new(path, DATA_OFFSET + 3 * 16 * 1024)
            .with_region_size(16 * 1024)
            .with_index_slots(64)
            .with_max_key_size(256)
            .with_max_value_size(2048)
            .with_append_lanes(2)
    }

    #[test]
    fn managed_region_delegates_policy_and_tracks_writes_in_shared_tracker() {
        let file = TestFile::new("managed-policy");
        let shared = Arc::new(HostWriteTracker::try_new(Some(1), None).unwrap());
        let managed_config = config(&file.0)
            .with_admission_mode(AdmissionMode::SecondHit)
            .with_namespace(
                NamespaceConfig::new(7)
                    .with_capacity_bytes(1)
                    .with_write_budget(1),
            )
            .with_daily_host_write_budget(1)
            .with_device_health_policy(DeviceHealthPolicy::RejectPutsOnCritical);
        assert!(managed_config.has_driver_policy_settings());

        let cache = DiskCache::open_managed(managed_config, Arc::clone(&shared)).unwrap();
        assert!(cache.inner.delegated_policy);
        assert!(
            cache
                .observe_nvme_health(NvmeHealthSample {
                    critical_warning: 1,
                    ..NvmeHealthSample::default()
                })
                .critical
        );
        let before = shared.snapshot().host_write_bytes;

        assert_eq!(
            cache
                .put_in(7, b"configured", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(99, b"unknown", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache.get_in(99, b"unknown").unwrap(),
            Some(b"value".to_vec())
        );

        let host = shared.snapshot();
        assert!(host.host_write_bytes > before);
        assert_eq!(host.admitted_value_bytes, 0);
        assert_eq!(cache.inner.policy.admission().snapshot().observations, 0);
        assert_eq!(
            cache
                .inner
                .policy
                .namespaces()
                .snapshot(7)
                .unwrap()
                .live_bytes,
            0
        );
        cache.close().unwrap();
    }

    #[test]
    fn volatile_invalidation_retires_one_candidate_without_device_io() {
        let file = TestFile::new("volatile-invalidation");
        let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let retired = Arc::new(Mutex::new(Vec::<NamespaceUsage>::new()));
        let sink_events = Arc::clone(&retired);
        let sink: Arc<NamespaceRetireSink> = Arc::new(move |usage| {
            sink_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(usage);
            true
        });
        let owner_dirty_calls = Arc::new(AtomicU64::new(0));
        let observed_owner_dirty = Arc::clone(&owner_dirty_calls);
        let owner_dirty: Arc<OwnerDirtyFence> = Arc::new(move || {
            observed_owner_dirty.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        let namespaces =
            Arc::new(NamespaceController::try_new(&[NamespaceConfig::new(7)]).unwrap());
        let cache = DiskCache::open_managed_with_owner_hooks(
            config(&file.0),
            Arc::clone(&shared),
            namespaces,
            sink,
            owner_dirty,
        )
        .unwrap();
        assert_eq!(
            cache
                .put_in(7, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let mut live = Vec::new();
        assert_eq!(cache.scan_live_usage(|usage| live.push(usage)).unwrap(), 1);
        let usage = live[0];
        let before_stats = cache.stats();
        let before_host_writes = shared.snapshot();
        let before_owner_dirty = owner_dirty_calls.load(Ordering::Relaxed);
        assert!(before_stats.region_valid_bytes >= usage.live_bytes);

        assert!(cache.invalidate_in_memory(7, b"key").unwrap());
        assert!(!cache.invalidate_in_memory(7, b"key").unwrap());
        assert!(!cache.may_contain_in(7, b"key").unwrap());
        assert_eq!(cache.get_in(7, b"key").unwrap(), None);

        let after_stats = cache.stats();
        assert_eq!(
            after_stats.region_valid_bytes,
            before_stats.region_valid_bytes - usage.live_bytes
        );
        assert_eq!(after_stats.entries + 1, before_stats.entries);
        assert_eq!(after_stats.io_submitted, before_stats.io_submitted);
        assert_eq!(after_stats.bytes_written, before_stats.bytes_written);
        assert_eq!(shared.snapshot(), before_host_writes);
        assert_eq!(
            owner_dirty_calls.load(Ordering::Relaxed),
            before_owner_dirty
        );
        assert_eq!(
            retired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            [usage]
        );
        assert_eq!(cache.scan_live_usage(|_| {}).unwrap(), 0);
        cache.close().unwrap();
    }

    #[test]
    fn hybrid_index_hints_do_not_wait_for_the_region_manager() {
        let file = TestFile::new("hybrid-index-hints");
        let cache = config(&file.0).open().unwrap();
        assert_eq!(
            cache
                .put_in(0, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );

        // FIFO rotation owns this mutex across header sync and victim scrub.
        // Advisory Hybrid index operations must remain independent of it.
        let region_manager = cache.inner.state.lock().unwrap();
        let probe = cache.clone();
        let (completed_tx, completed_rx) = std::sync::mpsc::sync_channel(1);
        let worker = std::thread::spawn(move || {
            let result = (|| -> Result<_> {
                let present = probe.may_contain_in(0, b"key")?;
                let bytes = probe.candidate_record_bytes_in(0, b"key")?;
                let invalidated = probe.invalidate_in_memory(0, b"key")?;
                Ok((present, bytes, invalidated))
            })();
            completed_tx.send(result).unwrap();
        });
        let completed = completed_rx.recv_timeout(Duration::from_secs(2));
        drop(region_manager);
        worker.join().unwrap();

        let (present, bytes, invalidated) = completed
            .expect("Hybrid index hints waited for the Region-manager mutex")
            .unwrap();
        assert!(present);
        assert!(bytes.is_some());
        assert!(invalidated);
        cache.close().unwrap();
    }

    #[test]
    fn owner_fenced_rotation_defers_sync_until_the_clean_boundary() {
        let file = TestFile::new("owner-fenced-rotation");
        let cache_config = config(&file.0);
        let (backend, faults) = FaultBackend::open(&file.0).unwrap();
        let host_writes = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let namespaces =
            Arc::new(NamespaceController::try_new(&[NamespaceConfig::new(0)]).unwrap());
        let retire_sink: Arc<NamespaceRetireSink> = Arc::new(|_| true);
        let owner_dirty: Arc<OwnerDirtyFence> = Arc::new(|| Ok(()));
        let cache = DiskCache::open_with_backend_and_owner_hooks(
            cache_config.clone(),
            Box::new(backend),
            host_writes,
            namespaces,
            retire_sink,
            owner_dirty,
        )
        .unwrap();

        // A RegionRotation sync would fail this test immediately. The owning
        // session fence permits lossy rotations, while close still publishes a
        // fully durable clean checkpoint.
        faults.arm(
            FaultEvent::Sync(SyncPoint::RegionRotation),
            1,
            FaultAction::Error(EIO),
        );
        let value = vec![b'x'; 1536];
        for index in 0..24 {
            let key = format!("key-{index:02}");
            assert_eq!(
                cache
                    .put(key.as_bytes(), &value, PutOptions::default())
                    .unwrap(),
                PutOutcome::Stored
            );
        }
        assert!(cache.stats().regions_reused > 0);
        assert!(
            !faults
                .events()
                .contains(&FaultEvent::Sync(SyncPoint::RegionRotation))
        );

        cache.close().unwrap();
        let reopened = cache_config.open().unwrap();
        assert_eq!(
            reopened.get(b"key-23").unwrap().as_deref(),
            Some(value.as_slice())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn dirty_reopen_formats_multiple_interrupted_rotations_empty() {
        let file = TestFile::new("multiple-interrupted-rotations");
        let cache_config = multi_lane_config(&file.0);
        let cache = cache_config.clone().open().unwrap();
        assert_eq!(
            cache.put(b"old", b"value", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        cache.flush().unwrap();
        assert_eq!(
            cache
                .put(b"dirty", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );

        // Deferred owner-fenced rotations can make several seal writes durable
        // while their replacement Active headers remain volatile. Persist that
        // valid crash image directly: the clean checkpoint still proves the
        // configured lane count, but the current dirty topology has no Active
        // Regions and cannot be repaired one lane at a time.
        let sealed = {
            let state = cache.inner.state.lock().unwrap();
            state
                .active_regions
                .iter()
                .map(|region_id| {
                    let mut header = state.regions[*region_id as usize].header;
                    header.state = RegionState::Sealed;
                    header
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(sealed.len(), 2);
        for header in sealed {
            write_all_at(
                cache.inner.io.as_ref(),
                WritePoint::RegionHeader,
                &header.encode(),
                region_base(&cache.read_superblock().unwrap(), header.region_id).unwrap(),
            )
            .unwrap();
        }
        cache
            .inner
            .io
            .sync(SyncPoint::RegionRotation, SyncMode::Data)
            .unwrap();
        drop(cache);

        let reopened = cache_config.open().unwrap();
        assert_eq!(reopened.status(), CacheStatus::Healthy);
        assert_eq!(reopened.get(b"old").unwrap(), None);
        assert_eq!(reopened.get(b"dirty").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn managed_retire_sink_tracks_replacement_expiry_and_remove_exactly_once() {
        let file = TestFile::new("managed-retire-sink");
        let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let retired = Arc::new(Mutex::new(Vec::<NamespaceUsage>::new()));
        let sink_events = Arc::clone(&retired);
        let sink: Arc<NamespaceRetireSink> = Arc::new(move |usage| {
            sink_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(usage);
            true
        });
        let namespaces = Arc::new(
            NamespaceController::try_new(&[
                NamespaceConfig::new(7),
                NamespaceConfig::new(8),
                NamespaceConfig::new(9),
            ])
            .unwrap(),
        );
        let cache = DiskCache::open_managed_with_retire_sink(
            config(&file.0),
            Arc::clone(&shared),
            namespaces,
            sink,
        )
        .unwrap();

        assert_eq!(
            cache
                .put_in(7, b"key", b"old", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let mut initial = Vec::new();
        assert_eq!(
            cache.scan_live_usage(|usage| initial.push(usage)).unwrap(),
            1
        );
        let old_usage = initial[0];

        // A second-chance publication replaces only the physical identity and
        // must not release the logical namespace charge.
        let previous = {
            let state = cache.lock_state().unwrap();
            let hash = hash_namespaced_key(state.superblock.hash_seed, 7, b"key");
            cache
                .inner
                .index
                .get(hash, state.superblock.epoch_start_seqno)
                .unwrap()
        };
        cache
            .record_put_replacement(
                PutSource::Reinsertion,
                ApplyResult {
                    applied: true,
                    previous: Some(previous),
                },
            )
            .unwrap();
        assert!(
            retired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );

        assert_eq!(
            cache
                .put_in(7, b"key", b"replacement", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            retired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            [old_usage]
        );
        let mut current = Vec::new();
        assert_eq!(
            cache.scan_live_usage(|usage| current.push(usage)).unwrap(),
            1
        );
        let replacement_usage = current[0];

        let expires_at = now_unix_ms().saturating_add(100);
        assert_eq!(
            cache
                .put_in(
                    8,
                    b"expires",
                    b"value",
                    PutOptions {
                        expires_at_unix_ms: Some(expires_at),
                    },
                )
                .unwrap(),
            PutOutcome::Stored
        );
        let mut before_expiry = Vec::new();
        assert_eq!(
            cache
                .scan_live_usage(|usage| before_expiry.push(usage))
                .unwrap(),
            2
        );
        let expiry_usage = *before_expiry
            .iter()
            .find(|usage| usage.namespace == 8)
            .unwrap();
        while now_unix_ms() < expires_at {
            std::thread::yield_now();
        }
        assert_eq!(cache.get_in(8, b"expires").unwrap(), None);
        assert_eq!(cache.remove_in(7, b"key").unwrap(), RemoveOutcome::Removed);
        assert_eq!(cache.remove_in(7, b"key").unwrap(), RemoveOutcome::NotFound);
        assert_eq!(
            retired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            [old_usage, expiry_usage, replacement_usage]
        );
        assert_eq!(cache.scan_live_usage(|_| {}).unwrap(), 0);

        // Fill beyond the compact-index capacity without filling a Region.
        // Every forced collision replacement must account exactly one retired
        // value, so created == retired + still-live.
        cache.clear().unwrap();
        retired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        for item in 0..80 {
            let key = format!("item-{item:03}");
            assert_eq!(
                cache
                    .put_in(9, key.as_bytes(), b"v", PutOptions::default())
                    .unwrap(),
                PutOutcome::Stored
            );
        }
        let mut collision_live = Vec::new();
        let live_count = cache
            .scan_live_usage(|usage| collision_live.push(usage))
            .unwrap();
        let collision_retired = retired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(!collision_retired.is_empty());
        assert_eq!(collision_retired.len() + live_count, 80);
        assert!(
            collision_retired
                .iter()
                .chain(&collision_live)
                .all(|usage| usage.namespace == 9)
        );
        drop(collision_retired);
        cache.close().unwrap();
    }

    #[test]
    fn managed_region_scrub_retires_only_the_current_live_value() {
        let file = TestFile::new("managed-retire-scrub");
        let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let retired = Arc::new(Mutex::new(Vec::<NamespaceUsage>::new()));
        let sink_events = Arc::clone(&retired);
        let sink: Arc<NamespaceRetireSink> = Arc::new(move |usage| {
            sink_events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(usage);
            true
        });
        let namespaces = Arc::new(
            NamespaceController::try_new(&[NamespaceConfig::new(11), NamespaceConfig::new(12)])
                .unwrap(),
        );
        let cache = DiskCache::open_managed_with_retire_sink(
            config(&file.0),
            Arc::clone(&shared),
            namespaces,
            sink,
        )
        .unwrap();
        assert_eq!(
            cache
                .put_in(11, b"a", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            cache
                .put_in(12, b"b", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let mut live = Vec::new();
        assert_eq!(cache.scan_live_usage(|usage| live.push(usage)).unwrap(), 2);
        let survivor_usage = *live.iter().find(|usage| usage.namespace == 11).unwrap();

        assert_eq!(cache.remove_in(12, b"b").unwrap(), RemoveOutcome::Removed);
        retired
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        let (superblock, active, min_seqno) = {
            let state = cache.lock_state().unwrap();
            let active = state.regions[state.active_regions[0] as usize];
            (state.superblock, active, state.superblock.epoch_start_seqno)
        };
        cache
            .scrub_or_fallback_region_index(&superblock, active, min_seqno)
            .unwrap();

        assert_eq!(
            retired
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            [survivor_usage]
        );
        assert_eq!(cache.scan_live_usage(|_| {}).unwrap(), 0);
        cache.close().unwrap();
    }

    #[test]
    fn managed_region_scrub_failure_is_reported_and_poisons_the_cache() {
        let file = TestFile::new("managed-retire-scrub-failure");
        let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let namespaces =
            Arc::new(NamespaceController::try_new(&[NamespaceConfig::new(11)]).unwrap());
        let cache = DiskCache::open_managed_with_retire_sink(
            config(&file.0),
            shared,
            namespaces,
            Arc::new(|_| false),
        )
        .unwrap();
        assert_eq!(
            cache
                .put_in(11, b"key", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        let (superblock, active, min_seqno) = {
            let state = cache.lock_state().unwrap();
            let active = state.regions[state.active_regions[0] as usize];
            (state.superblock, active, state.superblock.epoch_start_seqno)
        };

        assert!(matches!(
            cache.scrub_or_fallback_region_index(&superblock, active, min_seqno),
            Err(CacheError::CorruptMetadata(_))
        ));
        assert_eq!(cache.status(), CacheStatus::Poisoned);
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn driver_policy_settings_are_detected_without_opening_the_path() {
        let file = TestFile::new("driver-policy-settings");
        let base = config(&file.0);
        assert!(!base.has_driver_policy_settings());
        assert!(
            base.clone()
                .with_admission_mode(AdmissionMode::SecondHit)
                .has_driver_policy_settings()
        );
        assert!(
            base.clone()
                .with_namespace(NamespaceConfig::new(7))
                .has_driver_policy_settings()
        );
        assert!(
            base.clone()
                .with_write_budget(1)
                .has_driver_policy_settings()
        );
        assert!(
            base.clone()
                .with_daily_host_write_budget(1)
                .has_driver_policy_settings()
        );
        assert!(
            base.with_device_health_policy(DeviceHealthPolicy::RejectPutsOnCritical)
                .has_driver_policy_settings()
        );
        assert!(!file.0.exists());
    }

    #[derive(Clone, Copy)]
    struct RecoveryRecord {
        key: &'static [u8],
        value: &'static [u8],
        seqno: u64,
    }

    fn write_recovery_checkpoint<const N: usize>(
        path: &Path,
        mut headers: [RegionHeader; N],
        records: [Vec<RecoveryRecord>; N],
        next_seqno: u64,
    ) {
        let superblock = Superblock {
            generation: 2,
            region_size: 16 * 1024,
            region_count: N as u32,
            epoch: 1,
            epoch_start_seqno: 1,
            next_seqno,
            hash_seed: DEFAULT_HASH_SEED,
            clean: true,
        };
        let backend = FileBackend::open(path).unwrap();
        backend
            .set_len(DATA_OFFSET + superblock.region_size * N as u64)
            .unwrap();
        let encoded_superblock = superblock.encode();
        for offset in [SUPERBLOCK_A_OFFSET, SUPERBLOCK_B_OFFSET] {
            write_all_at(
                &backend,
                WritePoint::Superblock,
                &encoded_superblock,
                offset,
            )
            .unwrap();
        }

        for (region_id, region_records) in records.iter().enumerate() {
            let header = &mut headers[region_id];
            let mut cursor = REGION_HEADER_SIZE as u64;
            for record in region_records {
                let record_len = RecordHeader::aligned_len(record.key.len(), record.value.len())
                    .expect("test record must fit Format V1");
                let mut encoded = vec![0_u8; record_len as usize];
                let key_start = RECORD_HEADER_SIZE;
                let value_start = key_start + record.key.len();
                encoded[key_start..value_start].copy_from_slice(record.key);
                encoded[value_start..value_start + record.value.len()]
                    .copy_from_slice(record.value);
                let payload_end = value_start + record.value.len();
                let record_header = RecordHeader {
                    kind: RecordKind::Value,
                    codec: RecordCodec::PlainKey,
                    key_len: record.key.len() as u32,
                    value_len: record.value.len() as u32,
                    stored_len: record.value.len() as u32,
                    record_len,
                    region_incarnation: header.incarnation,
                    epoch: 1,
                    seqno: record.seqno,
                    key_hash: hash_key(DEFAULT_HASH_SEED, record.key),
                    expires_at: 0,
                    payload_crc: crc32c(&encoded[key_start..payload_end]),
                };
                encoded[..RECORD_HEADER_SIZE].copy_from_slice(&record_header.encode());
                let absolute = region_base(&superblock, region_id as u32)
                    .unwrap()
                    .checked_add(cursor)
                    .unwrap();
                write_all_at(&backend, WritePoint::Record, &encoded, absolute).unwrap();
                cursor += u64::from(record_len);
            }
            header.used = cursor;
            write_all_at(
                &backend,
                WritePoint::RegionHeader,
                &header.encode(),
                region_base(&superblock, region_id as u32).unwrap(),
            )
            .unwrap();
        }
        backend
            .sync(SyncPoint::CheckpointClean, SyncMode::Data)
            .unwrap();
    }

    fn open_fault_cache(config: CacheConfig) -> (DiskCache, FaultHandle) {
        let (backend, handle) = FaultBackend::open(&config.path).unwrap();
        let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
        (cache, handle)
    }

    fn assert_io_error<T>(result: Result<T>, expected_raw_code: i32) {
        match result {
            Err(CacheError::Io(error)) => assert_eq!(error.raw_os_error(), Some(expected_raw_code)),
            _ => panic!("expected injected I/O error"),
        }
    }

    #[test]
    fn recovery_accepts_multiple_active_regions_with_interleaved_global_seqnos() {
        let file = TestFile::new("multi-active-recovery");
        let candidates: [&'static [u8]; 4] =
            [b"lane-key-0", b"lane-key-1", b"lane-key-2", b"lane-key-3"];
        let lane_zero = *candidates
            .iter()
            .find(|key| hash_key(DEFAULT_HASH_SEED, key) as usize % 2 == 0)
            .unwrap();
        let lane_one = *candidates
            .iter()
            .find(|key| hash_key(DEFAULT_HASH_SEED, key) as usize % 2 == 1)
            .unwrap();
        write_recovery_checkpoint(
            &file.0,
            [
                RegionHeader {
                    region_id: 0,
                    incarnation: 1,
                    state: RegionState::Active,
                    created_seqno: 1,
                    used: REGION_HEADER_SIZE as u64,
                },
                RegionHeader {
                    region_id: 1,
                    incarnation: 1,
                    state: RegionState::Active,
                    created_seqno: 3,
                    used: REGION_HEADER_SIZE as u64,
                },
                RegionHeader::free(2, 0),
            ],
            [
                vec![
                    RecoveryRecord {
                        key: lane_one,
                        value: b"old",
                        seqno: 2,
                    },
                    RecoveryRecord {
                        key: lane_one,
                        value: b"newest",
                        seqno: 5,
                    },
                ],
                vec![
                    RecoveryRecord {
                        key: lane_zero,
                        value: b"middle",
                        seqno: 3,
                    },
                    RecoveryRecord {
                        key: lane_zero,
                        value: b"present",
                        seqno: 4,
                    },
                ],
                Vec::new(),
            ],
            6,
        );

        let cache = multi_lane_config(&file.0).open().unwrap();
        assert_eq!(cache.get(lane_zero).unwrap(), Some(b"present".to_vec()));
        assert_eq!(cache.get(lane_one).unwrap(), Some(b"newest".to_vec()));
        assert_eq!(cache.inner.state.lock().unwrap().active_regions, vec![1, 0]);
        assert_eq!(cache.stats().recovered_entries, 2);
        cache.close().unwrap();
    }

    #[test]
    fn recovery_rejects_invalid_multi_active_sequence_metadata() {
        let cases = [
            (
                "duplicate-created-seqno",
                [
                    RegionHeader {
                        region_id: 0,
                        incarnation: 1,
                        state: RegionState::Active,
                        created_seqno: 1,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader {
                        region_id: 1,
                        incarnation: 1,
                        state: RegionState::Active,
                        created_seqno: 1,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader::free(2, 0),
                ],
                [Vec::new(), Vec::new(), Vec::new()],
            ),
            (
                "non-increasing-region-seqno",
                [
                    RegionHeader {
                        region_id: 0,
                        incarnation: 1,
                        state: RegionState::Active,
                        created_seqno: 1,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader {
                        region_id: 1,
                        incarnation: 1,
                        state: RegionState::Active,
                        created_seqno: 2,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader::free(2, 0),
                ],
                [
                    vec![
                        RecoveryRecord {
                            key: b"first",
                            value: b"value",
                            seqno: 4,
                        },
                        RecoveryRecord {
                            key: b"second",
                            value: b"value",
                            seqno: 3,
                        },
                    ],
                    Vec::new(),
                    Vec::new(),
                ],
            ),
            (
                "no-active-region",
                [
                    RegionHeader {
                        region_id: 0,
                        incarnation: 1,
                        state: RegionState::Sealed,
                        created_seqno: 1,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader {
                        region_id: 1,
                        incarnation: 1,
                        state: RegionState::Sealed,
                        created_seqno: 2,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader::free(2, 0),
                ],
                [Vec::new(), Vec::new(), Vec::new()],
            ),
            (
                "invalid-active-metadata",
                [
                    RegionHeader {
                        region_id: 0,
                        incarnation: 0,
                        state: RegionState::Active,
                        created_seqno: 1,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader {
                        region_id: 1,
                        incarnation: 1,
                        state: RegionState::Active,
                        created_seqno: 2,
                        used: REGION_HEADER_SIZE as u64,
                    },
                    RegionHeader::free(2, 0),
                ],
                [Vec::new(), Vec::new(), Vec::new()],
            ),
        ];

        for (name, headers, records) in cases {
            let file = TestFile::new(name);
            write_recovery_checkpoint(&file.0, headers, records, 6);

            // Open deliberately reformats a corrupt clean checkpoint. An empty
            // healthy cache proves the invalid V1 metadata was not replayed.
            let cache = multi_lane_config(&file.0).open().unwrap();
            assert_eq!(cache.stats().recovered_entries, 0, "{name}");
            assert_eq!(cache.get(b"first").unwrap(), None, "{name}");
            cache.close().unwrap();
        }
    }

    #[test]
    fn short_positioned_io_is_retried_without_degrading_health() {
        let file = TestFile::new("short-io");
        let config = config(&file.0);
        let (cache, handle) = open_fault_cache(config.clone());

        handle.arm(
            FaultEvent::Write(WritePoint::Record),
            1,
            FaultAction::Short(7),
        );
        assert_eq!(
            cache.put("key", "value", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.status(), CacheStatus::Healthy);

        handle.arm(FaultEvent::Read, 1, FaultAction::Short(5));
        assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(cache.status(), CacheStatus::Healthy);
        assert!(handle.events().len() >= 2);

        cache.flush().unwrap();
        cache.close().unwrap();
        let reopened = config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn read_failure_enters_miss_only_without_more_backend_access() {
        let file = TestFile::new("read-failure");
        let config = config(&file.0);
        let (cache, handle) = open_fault_cache(config.clone());
        cache.put("key", "value", PutOptions::default()).unwrap();
        cache.flush().unwrap();

        handle.arm(FaultEvent::Read, 1, FaultAction::Error(EIO));
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(cache.status(), CacheStatus::MissOnly);
        let event_count = handle.events().len();
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(handle.events().len(), event_count);
        assert_eq!(cache.stats().entries, 0);

        assert!(matches!(
            cache.put(
                vec![b'x'; 1024],
                "value",
                PutOptions {
                    expires_at_unix_ms: Some(0),
                }
            ),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(cache.remove(b"key"), Err(CacheError::Poisoned)));
        assert!(matches!(cache.flush(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.clear(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert_eq!(cache.status(), CacheStatus::Closed);
        cache.close().unwrap();

        let reopened = config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn remove_read_failure_does_not_append_a_tombstone() {
        let file = TestFile::new("remove-read-failure");
        let config = config(&file.0);
        let (cache, handle) = open_fault_cache(config.clone());
        cache.put("key", "value", PutOptions::default()).unwrap();
        cache.flush().unwrap();

        handle.arm(FaultEvent::Read, 1, FaultAction::Error(EIO));
        assert_io_error(cache.remove(b"key"), EIO);
        assert_eq!(cache.status(), CacheStatus::MissOnly);
        assert_eq!(handle.events(), vec![FaultEvent::Read]);
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));

        let reopened = config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn close_failure_is_terminal_idempotent_and_releases_the_file_lock() {
        let file = TestFile::new("close-failure");
        let config = config(&file.0);
        let (cache, handle) = open_fault_cache(config.clone());
        cache.put("key", "value", PutOptions::default()).unwrap();

        handle.arm(
            FaultEvent::Write(WritePoint::RegionHeader),
            1,
            FaultAction::Error(EIO),
        );
        assert_io_error(cache.close(), EIO);
        assert_eq!(cache.status(), CacheStatus::Closed);
        cache.close().unwrap();

        // The old object deliberately stays alive: close(), not Drop, must
        // release flock even when checkpoint publication fails.
        let reopened = config.open().unwrap();
        assert_eq!(reopened.status(), CacheStatus::Healthy);
        let recovered = reopened.get(b"key").unwrap();
        assert!(recovered.is_none() || recovered == Some(b"value".to_vec()));
    }

    #[test]
    fn lock_and_unlock_io_failures_are_reported_and_unlock_can_be_retried() {
        let lock_file = TestFile::new("lock-io-failure");
        let lock_config = config(&lock_file.0);
        let (backend, handle) = FaultBackend::open(&lock_file.0).unwrap();
        handle.arm(FaultEvent::Lock, 1, FaultAction::Error(EIO));
        assert_io_error(
            DiskCache::open_with_backend(lock_config.clone(), Box::new(backend)),
            EIO,
        );
        assert_eq!(std::fs::metadata(&lock_file.0).unwrap().len(), 0);
        drop(lock_config.open().unwrap());

        let unlock_file = TestFile::new("unlock-io-failure");
        let unlock_config = config(&unlock_file.0);
        let (cache, handle) = open_fault_cache(unlock_config.clone());
        handle.arm(FaultEvent::Unlock, 1, FaultAction::Error(EIO));
        assert_io_error(cache.close(), EIO);
        assert_eq!(cache.status(), CacheStatus::Closed);
        assert!(matches!(
            unlock_config.clone().open(),
            Err(CacheError::Locked)
        ));
        cache.close().unwrap();
        drop(unlock_config.open().unwrap());
    }

    #[test]
    fn internal_overflow_poisoning_stops_record_io_and_close_still_unlocks() {
        let file = TestFile::new("internal-overflow");
        let config = config(&file.0);
        let (cache, handle) = open_fault_cache(config.clone());
        cache.inner.state.lock().unwrap().superblock.next_seqno = u64::MAX;
        handle.arm(
            FaultEvent::Write(WritePoint::Record),
            usize::MAX,
            FaultAction::Error(EIO),
        );

        assert!(matches!(
            cache.put("key", "value", PutOptions::default()),
            Err(CacheError::CorruptMetadata("sequence number overflow"))
        ));
        assert_eq!(cache.status(), CacheStatus::Poisoned);
        assert!(
            !handle
                .events()
                .contains(&FaultEvent::Write(WritePoint::Record))
        );
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert_eq!(cache.status(), CacheStatus::Closed);

        let reopened = config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
    }

    #[test]
    fn bounded_write_overload_preserves_read_and_control_reserves() {
        let file = TestFile::new("bounded-overload");
        let config = config(&file.0).with_submission_queue_depths(2, 2);
        let cache = config.open().unwrap();
        cache.put("key", "value", PutOptions::default()).unwrap();
        cache.flush().unwrap();

        let write_a = cache.inner.resources.begin_write().unwrap();
        let write_b = cache.inner.resources.begin_write().unwrap();
        assert_eq!(
            cache.put("other", "value", PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::SubmissionFull)
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
        assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);

        let stats = cache.stats();
        assert_eq!(stats.write_queue_depth, 2);
        assert_eq!(stats.queue_rejections, 1);
        assert_eq!(stats.rejected, 1);
        assert!(stats.read_buffers_in_use_peak >= 1);
        assert!(stats.control_buffers_in_use_peak >= 1);
        assert!(stats.memory_peak_bytes <= stats.memory_budget_bytes);

        cache.close().unwrap();
        assert!(matches!(
            cache.put(vec![b'x'; MAX_KEY_SIZE + 1], "value", PutOptions::default()),
            Err(CacheError::Closed)
        ));
        drop((write_a, write_b));
    }

    #[test]
    fn write_budget_rejects_before_dirty_marker_or_record_io() {
        let file = TestFile::new("write-budget");
        let config = config(&file.0).with_write_budget(1);
        let (cache, handle) = open_fault_cache(config);
        handle.arm(
            FaultEvent::Write(WritePoint::Record),
            usize::MAX,
            FaultAction::Error(EIO),
        );

        assert_eq!(
            cache.put("key", "value", PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::WriteBudgetExceeded)
        );
        assert!(handle.events().is_empty());
        assert_eq!(cache.status(), CacheStatus::Healthy);
        let stats = cache.stats();
        assert_eq!(stats.rejected, 1);
        assert_eq!(stats.write_budget_rejections, 1);
        assert_eq!(stats.write_buffers_in_use, 0);
        assert!(stats.memory_peak_bytes <= stats.memory_budget_bytes);
    }

    #[test]
    fn recovery_buffer_accepts_the_full_format_v1_padded_record_range() {
        let file = TestFile::new("padded-record-cap");
        let region_size = 20 * 1024 * 1024;
        let config = CacheConfig::new(&file.0, DATA_OFFSET + 2 * region_size)
            .with_region_size(region_size)
            .with_index_slots(64)
            .with_max_key_size(1)
            .with_max_value_size(1);
        let layout = config.validate().unwrap();
        let resources = allocate_resources(&config, &layout).unwrap();
        let valid_padded_length =
            (MAX_RECORD_LEN as usize).min((region_size - REGION_HEADER_SIZE as u64) as usize);

        let mut scratch = resources.recovery_buffer().unwrap();
        assert_eq!(
            scratch.prepare(valid_padded_length).unwrap().len(),
            valid_padded_length
        );
        let stats = resources.snapshot();
        assert!(stats.memory_peak_bytes <= stats.memory_budget_bytes);
        assert!(!file.0.exists());
    }

    #[test]
    fn io_failures_stop_publication_and_enter_miss_only() {
        #[derive(Clone, Copy)]
        enum Operation {
            Put,
            Flush,
        }

        struct Case {
            name: &'static str,
            event: FaultEvent,
            action: FaultAction,
            operation: Operation,
            forbidden_after_failure: FaultEvent,
            error: i32,
            may_recover_new: bool,
        }

        let cases = [
            Case {
                name: "dirty-superblock-eio",
                event: FaultEvent::Write(WritePoint::Superblock),
                action: FaultAction::Error(EIO),
                operation: Operation::Put,
                forbidden_after_failure: FaultEvent::Sync(SyncPoint::DirtyMarker),
                error: EIO,
                may_recover_new: false,
            },
            Case {
                name: "dirty-sync-eio",
                event: FaultEvent::Sync(SyncPoint::DirtyMarker),
                action: FaultAction::Error(EIO),
                operation: Operation::Put,
                forbidden_after_failure: FaultEvent::Write(WritePoint::Record),
                error: EIO,
                may_recover_new: false,
            },
            Case {
                name: "record-torn",
                event: FaultEvent::Write(WritePoint::Record),
                action: FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
                operation: Operation::Put,
                forbidden_after_failure: FaultEvent::Write(WritePoint::RegionHeader),
                error: EIO,
                may_recover_new: false,
            },
            Case {
                name: "record-enospc",
                event: FaultEvent::Write(WritePoint::Record),
                action: FaultAction::Error(ENOSPC),
                operation: Operation::Put,
                forbidden_after_failure: FaultEvent::Write(WritePoint::RegionHeader),
                error: ENOSPC,
                may_recover_new: false,
            },
            Case {
                name: "region-header-eio",
                event: FaultEvent::Write(WritePoint::RegionHeader),
                action: FaultAction::Error(EIO),
                operation: Operation::Flush,
                forbidden_after_failure: FaultEvent::Sync(SyncPoint::CheckpointData),
                error: EIO,
                may_recover_new: true,
            },
            Case {
                name: "data-sync-eio",
                event: FaultEvent::Sync(SyncPoint::CheckpointData),
                action: FaultAction::Error(EIO),
                operation: Operation::Flush,
                forbidden_after_failure: FaultEvent::Write(WritePoint::Superblock),
                error: EIO,
                may_recover_new: true,
            },
            Case {
                name: "clean-superblock-torn",
                event: FaultEvent::Write(WritePoint::Superblock),
                action: FaultAction::Torn {
                    bytes: 31,
                    raw_os_error: EIO,
                },
                operation: Operation::Flush,
                forbidden_after_failure: FaultEvent::Sync(SyncPoint::CheckpointClean),
                error: EIO,
                may_recover_new: true,
            },
            Case {
                name: "clean-sync-eio",
                event: FaultEvent::Sync(SyncPoint::CheckpointClean),
                action: FaultAction::Error(EIO),
                operation: Operation::Flush,
                forbidden_after_failure: FaultEvent::Sync(SyncPoint::FormatDirty),
                error: EIO,
                may_recover_new: true,
            },
        ];

        for case in cases {
            let file = TestFile::new(case.name);
            let config = config(&file.0);
            let (cache, handle) = open_fault_cache(config.clone());
            if matches!(case.operation, Operation::Flush) {
                cache.put("key", "new", PutOptions::default()).unwrap();
            }
            handle.arm(case.event, 1, case.action);
            match case.operation {
                Operation::Put => {
                    assert_io_error(cache.put("key", "new", PutOptions::default()), case.error);
                }
                Operation::Flush => assert_io_error(cache.flush(), case.error),
            }
            assert_eq!(cache.status(), CacheStatus::MissOnly, "{}", case.name);
            let stats = cache.stats();
            assert_eq!(stats.read_buffers_in_use, 0, "{}", case.name);
            assert_eq!(stats.write_buffers_in_use, 0, "{}", case.name);
            assert_eq!(stats.control_buffers_in_use, 0, "{}", case.name);
            assert!(stats.memory_peak_bytes <= stats.memory_budget_bytes);
            assert!(
                !handle.events().contains(&case.forbidden_after_failure),
                "{} continued publication after the injected failure: {:?}",
                case.name,
                handle.events()
            );
            assert_eq!(cache.get(b"key").unwrap(), None);
            assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
            cache.close().unwrap();

            let reopened = config.open().unwrap();
            let recovered = reopened.get(b"key").unwrap();
            if case.may_recover_new {
                assert!(
                    recovered.is_none() || recovered == Some(b"new".to_vec()),
                    "{} recovered an impossible value",
                    case.name
                );
            } else {
                assert_eq!(recovered, None, "{} published after failure", case.name);
            }
        }
    }

    #[test]
    fn superblock_io_errors_are_not_misclassified_as_an_empty_file() {
        let file = TestFile::new("superblock-read-error");
        let config = config(&file.0);
        let cache = config.clone().open().unwrap();
        cache.put("key", "value", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        drop(cache);
        let before = std::fs::read(&file.0).unwrap();

        let (backend, handle) = FaultBackend::open(&file.0).unwrap();
        handle.arm(FaultEvent::Read, 1, FaultAction::ErrorAlways(EIO));
        let result = DiskCache::open_with_backend(config.clone(), Box::new(backend));
        assert_io_error(result, EIO);
        assert_eq!(std::fs::read(&file.0).unwrap(), before);

        let (backend, handle) = FaultBackend::open(&file.0).unwrap();
        handle.arm(FaultEvent::Read, 2, FaultAction::Error(EIO));
        let reopened = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn interrupted_format_is_restart_safe_and_never_publishes_partial_state() {
        for bytes in [0, 1, 7, 8, 9, 10] {
            let file = TestFile::new(&format!("format-superblock-prefix-{bytes}"));
            let config = config(&file.0);
            let (backend, handle) = FaultBackend::open(&file.0).unwrap();
            handle.arm(
                FaultEvent::Write(WritePoint::Superblock),
                1,
                FaultAction::Torn {
                    bytes,
                    raw_os_error: EIO,
                },
            );
            assert_io_error(
                DiskCache::open_with_backend(config.clone(), Box::new(backend)),
                EIO,
            );

            let reopened = config.open().unwrap();
            assert_eq!(reopened.status(), CacheStatus::Healthy);
            assert_eq!(reopened.get(b"never-written").unwrap(), None);
        }

        let cases = [
            (
                "format-dirty-superblock",
                FaultEvent::Write(WritePoint::Superblock),
                1,
                FaultAction::Torn {
                    bytes: 13,
                    raw_os_error: EIO,
                },
            ),
            (
                "format-dirty-sync",
                FaultEvent::Sync(SyncPoint::FormatDirty),
                1,
                FaultAction::Error(EIO),
            ),
            (
                "format-region-header",
                FaultEvent::Write(WritePoint::RegionHeader),
                1,
                FaultAction::Torn {
                    bytes: 31,
                    raw_os_error: EIO,
                },
            ),
            (
                "format-region-sync",
                FaultEvent::Sync(SyncPoint::FormatRegions),
                1,
                FaultAction::Error(EIO),
            ),
            (
                "format-clean-superblock",
                FaultEvent::Write(WritePoint::Superblock),
                3,
                FaultAction::Torn {
                    bytes: 63,
                    raw_os_error: EIO,
                },
            ),
            (
                "format-clean-sync",
                FaultEvent::Sync(SyncPoint::FormatClean),
                1,
                FaultAction::Error(EIO),
            ),
        ];

        for (name, event, occurrence, action) in cases {
            let file = TestFile::new(name);
            let config = config(&file.0);
            let (backend, handle) = FaultBackend::open(&file.0).unwrap();
            handle.arm(event, occurrence, action);
            assert_io_error(
                DiskCache::open_with_backend(config.clone(), Box::new(backend)),
                EIO,
            );

            let reopened = config.open().unwrap();
            assert_eq!(reopened.status(), CacheStatus::Healthy);
            assert_eq!(reopened.get(b"never-written").unwrap(), None);
        }
    }

    #[test]
    fn kill_restart_at_every_persistence_boundary_returns_only_valid_states() {
        let format_scenarios = [
            ("format-dirty-a", "superblock", 1),
            ("format-dirty-b", "superblock", 2),
            ("format-dirty-sync", "format-dirty-sync", 1),
            ("format-region-a", "region", 1),
            ("format-region-b", "region", 2),
            ("format-region-sync", "format-region-sync", 1),
            ("format-clean-a", "superblock", 3),
            ("format-clean-b", "superblock", 4),
            ("format-clean-sync", "format-clean-sync", 1),
        ];
        for (name, event, occurrence) in format_scenarios {
            for timing in ["before", "after"] {
                let file = TestFile::new(&format!("crash-{name}-{timing}"));
                run_crash_worker(&file.0, event, occurrence, timing, "format");
                let reopened = config(&file.0).open().unwrap();
                assert_eq!(reopened.status(), CacheStatus::Healthy);
                assert_eq!(reopened.get(b"key").unwrap(), None);
            }
        }

        let scenarios = [
            ("dirty-a", "superblock", 1),
            ("dirty-b", "superblock", 2),
            ("dirty-sync", "dirty-sync", 1),
            ("record", "record", 1),
            ("region", "region", 1),
            ("data-sync", "data-sync", 1),
            ("clean-superblock", "superblock", 3),
            ("clean-sync", "clean-sync", 1),
        ];

        for (name, event, occurrence) in scenarios {
            for timing in ["before", "after"] {
                let file = TestFile::new(&format!("crash-{name}-{timing}"));
                let config = config(&file.0);
                let cache = config.clone().open().unwrap();
                cache.put("key", "old", PutOptions::default()).unwrap();
                cache.flush().unwrap();
                drop(cache);

                run_crash_worker(&file.0, event, occurrence, timing, "put");
                let reopened = config.open().unwrap_or_else(|error| {
                    panic!("failed to reopen after {name}/{timing}: {error}")
                });
                let recovered = reopened.get(b"key").unwrap();
                assert!(
                    recovered.is_none()
                        || recovered == Some(b"old".to_vec())
                        || recovered == Some(b"new".to_vec()),
                    "unexpected recovery at {name}/{timing}: {recovered:?}"
                );
                if (event == "superblock" && occurrence == 3 && timing == "after")
                    || (event == "clean-sync" && timing == "after")
                {
                    assert_eq!(recovered, Some(b"new".to_vec()));
                }
            }
        }

        let file = TestFile::new("crash-remove-clean");
        let remove_config = config(&file.0);
        let cache = remove_config.clone().open().unwrap();
        cache.put("key", "old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        drop(cache);
        run_crash_worker(&file.0, "clean-sync", 1, "after", "remove");
        let reopened = remove_config.open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);

        for (name, event, occurrence) in [
            ("rotation-seal", "region", 1),
            ("rotation-seal-sync", "region-rotation-sync", 1),
            ("rotation-activate", "region", 2),
            ("rotation-activate-sync", "region-rotation-sync", 2),
            ("rotation-record", "record", 1),
        ] {
            for timing in ["before", "after"] {
                let file = TestFile::new(&format!("crash-{name}-{timing}"));
                let config = config(&file.0);
                let cache = config.clone().open().unwrap();
                cache.put("key", "old", PutOptions::default()).unwrap();
                cache.flush().unwrap();
                drop(cache);
                run_crash_worker(&file.0, event, occurrence, timing, "rotate");
                let reopened = config.open().unwrap_or_else(|error| {
                    panic!("failed to reopen after {name}/{timing}: {error}")
                });
                let recovered = reopened.get(b"key").unwrap();
                assert!(
                    recovered.is_none() || recovered == Some(b"old".to_vec()),
                    "rotation crash recovered an impossible value at {name}/{timing}: {recovered:?}"
                );
            }
        }
    }

    fn run_crash_worker(
        path: &Path,
        event: &str,
        occurrence: usize,
        timing: &str,
        operation: &str,
    ) {
        let output = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("cache::m1_tests::crash_worker")
            .arg("--ignored")
            .arg("--test-threads=1")
            .env("CACHE_RS_CRASH_PATH", path)
            .env("CACHE_RS_CRASH_EVENT", event)
            .env("CACHE_RS_CRASH_OCCURRENCE", occurrence.to_string())
            .env("CACHE_RS_CRASH_TIMING", timing)
            .env("CACHE_RS_CRASH_OPERATION", operation)
            .output()
            .unwrap();
        assert_eq!(
            output.status.signal(),
            Some(9),
            "crash worker did not reach its persistence point: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    #[ignore = "spawned by kill_restart_at_every_persistence_boundary_returns_only_valid_states"]
    fn crash_worker() {
        let Ok(path) = std::env::var("CACHE_RS_CRASH_PATH") else {
            return;
        };
        let event = std::env::var("CACHE_RS_CRASH_EVENT").unwrap();
        let occurrence = std::env::var("CACHE_RS_CRASH_OCCURRENCE")
            .unwrap()
            .parse::<usize>()
            .unwrap();
        let action = match std::env::var("CACHE_RS_CRASH_TIMING").unwrap().as_str() {
            "before" => FaultAction::KillBefore,
            "after" => FaultAction::KillAfter,
            other => panic!("unknown crash timing {other}"),
        };
        let fault_event = match event.as_str() {
            "superblock" => FaultEvent::Write(WritePoint::Superblock),
            "record" => FaultEvent::Write(WritePoint::Record),
            "region" => FaultEvent::Write(WritePoint::RegionHeader),
            "checkpoint-directory" => FaultEvent::Write(WritePoint::CheckpointDirectory),
            "checkpoint-payload" => FaultEvent::Write(WritePoint::CheckpointPayload),
            "checkpoint-header" => FaultEvent::Write(WritePoint::CheckpointHeader),
            "dirty-sync" => FaultEvent::Sync(SyncPoint::DirtyMarker),
            "region-rotation-sync" => FaultEvent::Sync(SyncPoint::RegionRotation),
            "clear-sync" => FaultEvent::Sync(SyncPoint::ClearBarrier),
            "checkpoint-directory-sync" => FaultEvent::Sync(SyncPoint::CheckpointDirectory),
            "checkpoint-payload-sync" => FaultEvent::Sync(SyncPoint::CheckpointPayload),
            "checkpoint-header-sync" => FaultEvent::Sync(SyncPoint::CheckpointHeader),
            "data-sync" => FaultEvent::Sync(SyncPoint::CheckpointData),
            "clean-sync" => FaultEvent::Sync(SyncPoint::CheckpointClean),
            "format-dirty-sync" => FaultEvent::Sync(SyncPoint::FormatDirty),
            "format-truncate-sync" => FaultEvent::Sync(SyncPoint::FormatTruncate),
            "format-region-sync" => FaultEvent::Sync(SyncPoint::FormatRegions),
            "format-clean-sync" => FaultEvent::Sync(SyncPoint::FormatClean),
            other => panic!("unknown crash event {other}"),
        };

        let path = PathBuf::from(path);
        let config = config(&path);
        let operation = std::env::var("CACHE_RS_CRASH_OPERATION").unwrap();
        if operation == "format" {
            let (backend, handle) = FaultBackend::open(&path).unwrap();
            handle.arm(fault_event, occurrence, action);
            let _ = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
            panic!("format crash fault was not reached");
        }

        let (cache, handle) = open_fault_cache(config);
        if operation == "rotate" {
            let value = vec![b'z'; 2048];
            let record_len = u64::from(RecordHeader::aligned_len(16, value.len()).unwrap());
            let mut id = 1_u8;
            loop {
                let remaining = {
                    let state = cache.inner.state.lock().unwrap();
                    let active = state.active_regions[0] as usize;
                    cache.inner.config.region_size - state.regions[active].used
                };
                if remaining < record_len {
                    break;
                }
                cache
                    .put(vec![id; 16], &value, PutOptions::default())
                    .unwrap();
                id = id.wrapping_add(1);
            }
            handle.arm(fault_event, occurrence, action);
            cache
                .put(vec![id; 16], &value, PutOptions::default())
                .unwrap();
            cache.flush().unwrap();
            panic!("rotation crash fault was not reached");
        }

        handle.arm(fault_event, occurrence, action);
        match operation.as_str() {
            "put" => {
                cache.put("key", "new", PutOptions::default()).unwrap();
            }
            "remove" => {
                cache.remove(b"key").unwrap();
            }
            "clear" => cache.clear().unwrap(),
            other => panic!("unknown crash operation {other}"),
        }
        cache.flush().unwrap();
        panic!("crash fault was not reached");
    }
}

#[cfg(not(unix))]
compile_error!("cache-rs v1.1 requires Unix positioned I/O and flock");
