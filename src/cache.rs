//! Public performance-first RAM + Region SSD hybrid cache.
//!
//! Static configuration defines the on-disk identity and clean recovery image.
//! Runtime configuration may change on every open without invalidating that
//! image. Ordinary mutations are never made durable; only `close_warm` publishes
//! state that a later process may recover.

use std::fmt;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::eviction::EvictionPolicy;
use crate::expiry::ExpiryClock;
use crate::index::{MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE};
use crate::index_storage::canonical_index_partition_ranges;
use crate::io_backend::DirectIoMode;
use crate::io_engine::FileIoEngineKind;
use crate::recovery::{
    DataGeometry, DataSuperblock, KEY_HASH_ALGORITHM_XXH3_64, PersistentId,
    RECOVERY_IMAGE_INDEX_OFFSET, STATE_FILE_SIZE, recovery_image_index_len,
};
use crate::region::{FileRegionBackend, RegionFiles, SystemRegionFileSystem};
use crate::region_appender::_WRITE_BATCH_BYTES;
use crate::region_layout::{RegionLayout, RegionSetAllocation, RegionSetConfig, RegionSetId};
use crate::region_metadata::{
    REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE,
};
use crate::region_runtime::{
    DEFAULT_MEMORY_SHARDS, HybridValueRead, RegionPut, RegionRuntimeConfig, RuntimeHealth,
    RuntimeSnapshot,
};
use crate::region_store::{RegionConfig, RegionStartup, RegionStore};
use crate::resources::{BackpressurePolicy, ReadBufferPolicy};

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const DEFAULT_SHARDS: u32 = 4;
const DEFAULT_MEMORY_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const MIN_INDEX_SLOTS: usize = 8;
const MAX_SHARDS: u32 = 256;

pub type Result<T> = std::io::Result<T>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticConfig {
    capacity_bytes: u64,
    region_size: u64,
    index_slots: usize,
    shards: u32,
    hash_seed: u64,
    region_sets: Vec<RegionSetConfig>,
}

impl StaticConfig {
    pub fn new(capacity_bytes: u64) -> Self {
        let expected_entries = capacity_bytes / (64 * 1024);
        let index_slots = expected_entries
            .saturating_mul(5)
            .saturating_add(3)
            .saturating_div(4)
            .clamp(MIN_INDEX_SLOTS as u64, MAX_INDEX_SLOTS as u64)
            as usize;
        Self {
            capacity_bytes,
            region_size: DEFAULT_REGION_SIZE,
            index_slots,
            shards: DEFAULT_SHARDS,
            hash_seed: DEFAULT_HASH_SEED,
            region_sets: Vec::new(),
        }
    }

    pub fn with_region_size(mut self, bytes: u64) -> Self {
        self.region_size = bytes;
        self
    }

    pub fn with_index_slots(mut self, slots: usize) -> Self {
        self.index_slots = slots;
        self
    }

    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.index_slots = entries
            .saturating_mul(5)
            .saturating_add(3)
            .saturating_div(4)
            .max(MIN_INDEX_SLOTS);
        self
    }

    pub fn with_shards(mut self, shards: u32) -> Self {
        self.shards = shards;
        self
    }

    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }

    /// Replaces the physical RegionSet layout.
    ///
    /// Capacity weights divide the fixed Region count. Append shards are
    /// distributed evenly and deterministically across the configured sets.
    /// RegionSet zero is required and receives every namespace not explicitly
    /// listed by another set.
    /// Every set needs one active Region per assigned shard plus one spare.
    /// The complete layout is static recovery identity.
    pub fn with_region_sets(mut self, sets: impl IntoIterator<Item = RegionSetConfig>) -> Self {
        self.region_sets = sets.into_iter().collect();
        self
    }

    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub const fn region_size(&self) -> u64 {
        self.region_size
    }

    pub const fn index_slots(&self) -> usize {
        self.index_slots
    }

    pub const fn shards(&self) -> u32 {
        self.shards
    }

    pub fn region_sets(&self) -> &[RegionSetConfig] {
        &self.region_sets
    }

    /// Validates the complete static geometry without opening cache files.
    pub fn validate(&self) -> Result<()> {
        self.geometry().map(|_| ())
    }

    /// Resolves capacity weights and append-shard assignment without opening
    /// cache files. Results are ordered by RegionSet id.
    pub fn region_set_allocations(&self) -> Result<Vec<RegionSetAllocation>> {
        let geometry = self.geometry()?;
        self.region_layout(geometry)?
            .allocations(geometry.region_size)
    }

    /// Maximum cache-owned logical disk bytes after a successful open.
    ///
    /// The bound includes the fixed data and state files plus both the current
    /// clean image and the temporary image used for atomic warm publication.
    /// Filesystem metadata and block-allocation granularity are outside it.
    pub fn peak_disk_bytes(&self) -> Result<u64> {
        let geometry = self.geometry()?;
        let index_slots = u64::try_from(self.index_slots)
            .map_err(|_| invalid_config("index capacity does not fit u64"))?;
        let index_len = recovery_image_index_len(index_slots)
            .ok_or_else(|| invalid_config("index image length overflow"))?;
        let partition_count = u64::try_from(
            canonical_index_partition_ranges(self.index_slots)
                .map_err(|_| invalid_config("index partition layout is invalid"))?
                .len(),
        )
        .map_err(|_| invalid_config("index partition count does not fit u64"))?;
        let region_pages =
            u64::from(geometry.region_count).div_ceil(REGION_METADATA_REGIONS_PER_PAGE as u64);
        let partition_pages = partition_count.div_ceil(REGION_METADATA_PARTITIONS_PER_PAGE as u64);
        let metadata_len = 1_u64
            .checked_add(region_pages)
            .and_then(|pages| pages.checked_add(partition_pages))
            .and_then(|pages| pages.checked_mul(REGION_METADATA_PAGE_SIZE as u64))
            .ok_or_else(|| invalid_config("Region metadata length overflow"))?;
        let image_len = RECOVERY_IMAGE_INDEX_OFFSET
            .checked_add(index_len)
            .and_then(|bytes| bytes.checked_add(metadata_len))
            .ok_or_else(|| invalid_config("recovery image length overflow"))?;
        geometry
            .data_file_len
            .checked_add(STATE_FILE_SIZE as u64)
            .and_then(|bytes| bytes.checked_add(image_len.checked_mul(2)?))
            .ok_or_else(|| invalid_config("peak disk usage overflow"))
    }

    fn geometry(&self) -> Result<DataGeometry> {
        if self.region_size == 0
            || self.region_size > MAX_PACKED_REGION_SIZE
            || self.region_size % 4096 != 0
            || self.capacity_bytes == 0
            || self.capacity_bytes % self.region_size != 0
        {
            return Err(invalid_config(
                "capacity must be a non-zero multiple of an aligned representable Region size",
            ));
        }
        let region_count = u32::try_from(self.capacity_bytes / self.region_size)
            .ok()
            .filter(|count| *count <= MAX_PACKED_REGION_COUNT)
            .ok_or_else(|| invalid_config("cache Region count is not representable"))?;
        if self.shards == 0 || self.shards > MAX_SHARDS || region_count <= self.shards {
            return Err(invalid_config(
                "data shards must be in 1..=256 with one additional Region for rotation",
            ));
        }
        if !(MIN_INDEX_SLOTS..=MAX_INDEX_SLOTS).contains(&self.index_slots) {
            return Err(invalid_config("index slots must be in 8..=268435456"));
        }
        let data_file_len = DataGeometry::expected_file_len(self.region_size, region_count)
            .ok_or_else(|| invalid_config("cache data length overflow"))?;
        let geometry = DataGeometry {
            data_file_len,
            region_size: self.region_size,
            region_count,
        };
        self.region_layout(geometry)?;
        Ok(geometry)
    }

    fn region_layout(&self, geometry: DataGeometry) -> Result<RegionLayout> {
        RegionLayout::build(geometry.region_count, self.shards, &self.region_sets)
    }

    fn fingerprint(&self, geometry: DataGeometry, layout: &RegionLayout) -> u64 {
        self.fingerprint_with_hash_algorithm(
            geometry,
            layout,
            u64::from(KEY_HASH_ALGORITHM_XXH3_64),
        )
    }

    fn fingerprint_with_hash_algorithm(
        &self,
        geometry: DataGeometry,
        layout: &RegionLayout,
        hash_algorithm_id: u64,
    ) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for value in [
            geometry.data_file_len,
            geometry.region_size,
            u64::from(geometry.region_count),
            self.index_slots as u64,
            u64::from(self.shards),
            self.hash_seed,
            hash_algorithm_id,
        ] {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        if !layout.uses_default_single_set() {
            hash_identity_word(&mut hash, 0x7265_6769_6f6e_7365);
            hash_identity_word(&mut hash, layout.sets().len() as u64);
            hash_identity_word(&mut hash, 0);
            for set in layout.sets() {
                for value in [
                    u64::from(set.id.get()),
                    u64::from(set.first_region),
                    u64::from(set.region_count),
                    u64::from(set.first_shard),
                    u64::from(set.shard_count),
                ] {
                    hash_identity_word(&mut hash, value);
                }
            }
            hash_identity_word(&mut hash, layout.routes().len() as u64);
            for &(namespace_id, set_index) in layout.routes() {
                hash_identity_word(&mut hash, u64::from(namespace_id));
                hash_identity_word(&mut hash, u64::from(layout.sets()[set_index].id.get()));
            }
        }
        hash.max(1)
    }
}

fn hash_identity_word(hash: &mut u64, value: u64) {
    for byte in value.to_le_bytes() {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x100_0000_01b3);
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoEngine {
    Sync,
    #[default]
    Auto,
    IoUring,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoMode {
    Buffered,
    #[default]
    Auto,
    Direct,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    io_engine: IoEngine,
    io_mode: IoMode,
    io_workers: usize,
    io_queue_depth: usize,
    read_queue_depth: usize,
    write_queue_depth: usize,
    read_buffer_slots: usize,
    read_buffer_policy: ReadBufferPolicy,
    memory_capacity_bytes: usize,
    memory_budget_bytes: usize,
    memory_shards: usize,
    eviction_policy: EvictionPolicy,
    staging_bytes: usize,
    batch_target_bytes: usize,
    partial_flush_age: Duration,
    backpressure: BackpressurePolicy,
    stats_enabled: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            io_engine: IoEngine::Auto,
            io_mode: IoMode::Auto,
            io_workers: 4,
            io_queue_depth: 128,
            read_queue_depth: 128,
            write_queue_depth: 128,
            read_buffer_slots: 128,
            read_buffer_policy: ReadBufferPolicy::Reject,
            memory_capacity_bytes: DEFAULT_MEMORY_CAPACITY_BYTES,
            memory_budget_bytes: 1024 * 1024 * 1024,
            memory_shards: DEFAULT_MEMORY_SHARDS,
            eviction_policy: EvictionPolicy::Clock,
            staging_bytes: _WRITE_BATCH_BYTES,
            batch_target_bytes: _WRITE_BATCH_BYTES,
            partial_flush_age: Duration::from_millis(1),
            backpressure: BackpressurePolicy::Reject,
            stats_enabled: false,
        }
    }
}

impl RuntimeConfig {
    pub fn with_io_engine(mut self, engine: IoEngine) -> Self {
        self.io_engine = engine;
        self
    }

    pub fn with_io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    pub fn with_io_workers(mut self, workers: usize) -> Self {
        self.io_workers = workers;
        self
    }

    pub fn with_io_queue_depth(mut self, depth: usize) -> Self {
        self.io_queue_depth = depth;
        self
    }

    /// Sets legacy read admission depth and waiting-policy write admission depth.
    ///
    /// `get` has no read admission queue; `read` is retained and validated for
    /// configuration compatibility. Read concurrency is bounded solely by
    /// [`Self::with_read_buffer_slots`]. The write depth is unused by the
    /// default reject policy because shard staging is its admission boundary.
    pub fn with_submission_queue_depths(mut self, read: usize, write: usize) -> Self {
        self.read_queue_depth = read;
        self.write_queue_depth = write;
        self
    }

    /// Sets the hard foreground L2 read-buffer budget.
    ///
    /// L2 acquires a fully prepared slot only after its index finds a
    /// candidate. Exhaustion returns `WouldBlock` immediately. A successful
    /// L1 promotion releases the transient slot before `get` returns; when L1
    /// promotion bypasses, the returned zero-copy Region value retains its
    /// slot until the caller drops it.
    pub fn with_read_buffer_slots(mut self, slots: usize) -> Self {
        self.read_buffer_slots = slots;
        self
    }

    /// Selects behavior when every foreground L2 read buffer is leased.
    ///
    /// The default is [`ReadBufferPolicy::Reject`]. Waiting is bounded and
    /// applies only after L2 finds a candidate; it never waits for an I/O
    /// submission slot.
    pub fn with_read_buffer_policy(mut self, policy: ReadBufferPolicy) -> Self {
        self.read_buffer_policy = policy;
        self
    }

    pub fn with_memory_capacity(mut self, bytes: usize) -> Self {
        self.memory_capacity_bytes = bytes;
        self
    }

    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget_bytes = bytes;
        self
    }

    /// Sets the runtime-only shard count for the volatile RAM tier.
    ///
    /// This is independent of static append shards and may change on every
    /// open without invalidating a clean recovery image.
    pub fn with_memory_shards(mut self, shards: usize) -> Self {
        self.memory_shards = shards;
        self
    }

    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    pub fn with_staging(mut self, bytes: usize, batch_target_bytes: usize) -> Self {
        self.staging_bytes = bytes;
        self.batch_target_bytes = batch_target_bytes;
        self
    }

    pub fn with_partial_flush_age(mut self, age: Duration) -> Self {
        self.partial_flush_age = age;
        self
    }

    /// Selects write-side overload behavior. L2 reads reject only when their
    /// fixed read-buffer pool is exhausted.
    pub fn with_backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.backpressure = policy;
        self
    }

    pub fn with_stats(mut self, enabled: bool) -> Self {
        self.stats_enabled = enabled;
        self
    }

    pub const fn io_workers(&self) -> usize {
        self.io_workers
    }

    /// Returns the configured foreground L2 read-buffer budget.
    pub const fn read_buffer_slots(&self) -> usize {
        self.read_buffer_slots
    }

    /// Returns the configured foreground L2 read-buffer saturation policy.
    pub const fn read_buffer_policy(&self) -> ReadBufferPolicy {
        self.read_buffer_policy
    }

    pub const fn memory_capacity_bytes(&self) -> usize {
        self.memory_capacity_bytes
    }

    pub const fn memory_budget_bytes(&self) -> usize {
        self.memory_budget_bytes
    }

    pub const fn memory_shards(&self) -> usize {
        self.memory_shards
    }

    pub const fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    pub const fn stats_enabled(&self) -> bool {
        self.stats_enabled
    }

    fn into_region(self) -> RegionRuntimeConfig {
        RegionRuntimeConfig {
            io_engine: match self.io_engine {
                IoEngine::Sync => FileIoEngineKind::Sync,
                IoEngine::Auto => FileIoEngineKind::Auto,
                IoEngine::IoUring => FileIoEngineKind::IoUring,
            },
            io_mode: match self.io_mode {
                IoMode::Buffered => DirectIoMode::Buffered,
                IoMode::Auto => DirectIoMode::Auto,
                IoMode::Direct => DirectIoMode::Required,
            },
            io_workers: self.io_workers,
            io_queue_depth: self.io_queue_depth,
            read_queue_depth: self.read_queue_depth,
            write_queue_depth: self.write_queue_depth,
            read_buffer_slots: self.read_buffer_slots,
            read_buffer_policy: self.read_buffer_policy,
            memory_capacity_bytes: self.memory_capacity_bytes,
            memory_budget_bytes: self.memory_budget_bytes,
            memory_shards: self.memory_shards,
            eviction_policy: self.eviction_policy,
            staging_bytes: self.staging_bytes,
            batch_target_bytes: self.batch_target_bytes,
            partial_flush_age: self.partial_flush_age,
            backpressure: self.backpressure,
            stats_enabled: self.stats_enabled,
        }
    }
}

#[derive(Clone, Debug)]
pub struct HybridCacheConfig {
    path: PathBuf,
    static_config: StaticConfig,
    runtime_config: RuntimeConfig,
}

impl HybridCacheConfig {
    pub fn new(path: impl AsRef<Path>, capacity_bytes: u64) -> Self {
        Self::from_static(path, StaticConfig::new(capacity_bytes))
    }

    /// Creates a cache builder from a complete static configuration.
    pub fn from_static(path: impl AsRef<Path>, static_config: StaticConfig) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            static_config,
            runtime_config: RuntimeConfig::default(),
        }
    }

    pub fn with_runtime_config(mut self, config: RuntimeConfig) -> Self {
        self.runtime_config = config;
        self
    }

    pub fn open(self) -> Result<HybridCache> {
        let geometry = self.static_config.geometry()?;
        let region_layout = self.static_config.region_layout(geometry)?;
        let logical_disk_peak_bytes = self.static_config.peak_disk_bytes()?;
        let runtime_config = self.runtime_config.into_region();
        runtime_config.validate_memory_plan(
            geometry,
            self.static_config.index_slots,
            self.static_config.shards as usize,
            region_layout.memory_bytes(),
        )?;
        let format_data = DataSuperblock {
            generation: 1,
            cache_uuid: next_persistent_id(),
            data_identity: next_persistent_id(),
            geometry,
            hash_seed: self.static_config.hash_seed,
            config_fingerprint: self.static_config.fingerprint(geometry, &region_layout),
        };
        let files = RegionFiles::new(
            &self.path,
            sidecar_path(&self.path, ".state"),
            sidecar_path(&self.path, ".image"),
        );
        let backend = FileRegionBackend::new_with_region_layout(
            files,
            format_data,
            region_layout,
            runtime_config,
        );
        let store = RegionStore::open(
            RegionConfig {
                index_slots: self.static_config.index_slots,
            },
            backend,
        )?;
        Ok(HybridCache {
            store,
            logical_disk_peak_bytes,
        })
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupMode {
    Fresh,
    ColdAfterUncleanShutdown,
    Warm,
    ColdAfterRejectedImage,
}

#[non_exhaustive]
pub struct PutReceipt {
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheTier {
    Memory,
    Region,
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
/// accounting. Counters are process-local, reset on every open, and remain
/// zero unless enabled with [`RuntimeConfig::with_stats`]. Health and resource
/// fields are always populated.
///
/// Managed memory includes fixed-capacity reservations such as L1 and thread
/// stacks plus currently allocated bounded buffers. It excludes allocator
/// metadata, buffered-I/O page cache, and filesystem metadata.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheSnapshot {
    pub health: CacheHealth,
    pub stats_enabled: bool,
    pub puts: u64,
    pub written_bytes: u64,
    pub l1_hits: u64,
    pub l1_misses: u64,
    pub l2_hits: u64,
    pub l2_misses: u64,
    /// Bytes returned by L1 and L2 hits combined.
    pub served_bytes: u64,
    pub promotions: u64,
    pub l1_evictions: u64,
    pub l1_bypasses: u64,
    pub l1_admission_rejections: u64,
    pub queue_saturation: u64,
    pub buffer_saturation: u64,
    pub io_failures: u64,
    pub region_rotations: u64,
    pub managed_memory_bytes: usize,
    pub managed_memory_peak_bytes: usize,
    pub managed_memory_budget_bytes: usize,
    pub logical_disk_peak_bytes: u64,
}

/// Admission and buffer-pool state sampled by [`HybridCache::detailed_snapshot`].
/// Wait and rejection counters are cumulative for the current open.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheQueueSnapshot {
    pub read_requests_in_flight: u64,
    pub read_requests_peak: u64,
    pub read_rejections: u64,
    pub read_wait_ns: u64,
    pub write_requests_in_flight: u64,
    pub write_requests_peak: u64,
    pub write_rejections: u64,
    pub write_wait_ns: u64,
    pub write_staging_rejections: u64,
    pub write_staging_wait_ns: u64,
    pub read_buffers_in_use: u64,
    pub read_buffers_peak: u64,
    pub read_buffer_rejections: u64,
    pub read_buffer_timeouts: u64,
    pub read_buffer_waiters: u64,
    pub read_buffer_waiters_peak: u64,
    pub read_buffer_wait_ns: u64,
    pub metadata_buffers_in_use: u64,
    pub metadata_buffers_peak: u64,
    pub metadata_buffer_rejections: u64,
    pub metadata_buffer_wait_ns: u64,
}

/// Aggregate worker I/O state sampled by [`HybridCache::detailed_snapshot`].
/// Durations and operation counters are cumulative for the current open.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheIoSnapshot {
    pub submitted: u64,
    pub completed: u64,
    pub cancel_requested: u64,
    pub cancelled: u64,
    pub errors: u64,
    pub in_flight: u64,
    pub submit_wait_ns: u64,
    pub completion_ns: u64,
    pub direct_operations: u64,
    pub direct_bytes: u64,
    pub buffered_operations: u64,
    pub buffered_bytes: u64,
}

/// Runtime occupancy and rotation state for one physical RegionSet.
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
    /// Completed Region payload bytes, including record padding but excluding
    /// Region headers.
    pub physical_bytes: u64,
    /// Successful rotations since the current open.
    pub rotations: u64,
}

/// On-demand operational detail. Unlike [`CacheSnapshot`], producing this
/// value locks Region metadata briefly and scans all Regions.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DetailedCacheSnapshot {
    pub summary: CacheSnapshot,
    pub queues: CacheQueueSnapshot,
    pub io: CacheIoSnapshot,
    pub region_sets: Box<[RegionSetSnapshot]>,
}

pub struct Value {
    inner: HybridValueRead,
}

impl Deref for Value {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.inner.value()
    }
}

impl AsRef<[u8]> for Value {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl Value {
    /// Returns the tier that served this lookup.
    ///
    /// A Region hit may already be backed by its promoted L1 value so the
    /// transient L2 read buffer can be released before this handle is returned.
    pub const fn tier(&self) -> CacheTier {
        if self.inner.is_memory() {
            CacheTier::Memory
        } else {
            CacheTier::Region
        }
    }
}

pub struct HybridCache {
    store: RegionStore<FileRegionBackend<SystemRegionFileSystem>>,
    logical_disk_peak_bytes: u64,
}

impl fmt::Debug for HybridCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HybridCache")
            .field("startup", &self.startup_mode())
            .finish_non_exhaustive()
    }
}

impl HybridCache {
    pub fn startup_mode(&self) -> StartupMode {
        match self.store.startup() {
            RegionStartup::FreshEmpty => StartupMode::Fresh,
            RegionStartup::DirtyEmpty => StartupMode::ColdAfterUncleanShutdown,
            RegionStartup::CleanMapped => StartupMode::Warm,
            RegionStartup::CleanRejectedEmpty => StartupMode::ColdAfterRejectedImage,
        }
    }

    pub fn put(&self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<PutReceipt> {
        self.put_in(0, key, value)
    }

    /// Stores a value in a logical namespace without expiration.
    pub fn put_in(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> Result<PutReceipt> {
        self.put_until(namespace, key, value, 0)
    }

    /// Stores a value until an absolute Unix timestamp in milliseconds.
    ///
    /// An expiration of zero means that the value does not expire.
    pub fn put_until(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        expires_at_unix_ms: u64,
    ) -> Result<PutReceipt> {
        match self
            .store
            .put_value(namespace, key.as_ref(), value.as_ref(), expires_at_unix_ms)?
        {
            RegionPut::Buffered(staged) => Ok(PutReceipt {
                sequence: staged.seqno,
            }),
        }
    }

    /// Looks up a value in L1 and then L2.
    ///
    /// An L2 index miss returns directly. An L2 candidate performs one bounded
    /// record read and exact revalidation. If no read buffer is available, the
    /// lookup follows [`RuntimeConfig::with_read_buffer_policy`]: reject
    /// immediately or wait for one bounded duration without retaining its
    /// Region pin.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Value>> {
        self.get_in(0, key)
    }

    /// Looks up a value in a logical namespace.
    ///
    /// The system clock is read lazily only when a matching value carries an
    /// expiration timestamp. This method has the same L2 semantics as
    /// [`Self::get`].
    pub fn get_in(&self, namespace: u32, key: impl AsRef<[u8]>) -> Result<Option<Value>> {
        self.get_with_clock(namespace, key.as_ref(), ExpiryClock::System)
    }

    /// Looks up a value using an explicit Unix timestamp in milliseconds.
    ///
    /// This is useful for deterministic tests and for callers that amortize a
    /// coarse clock sample across a batch of cache reads.
    pub fn get_in_at(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        now_unix_ms: u64,
    ) -> Result<Option<Value>> {
        self.get_with_clock(namespace, key.as_ref(), ExpiryClock::Fixed(now_unix_ms))
    }

    fn get_with_clock(
        &self,
        namespace: u32,
        key: &[u8],
        clock: ExpiryClock,
    ) -> Result<Option<Value>> {
        self.store
            .get_value(namespace, key, clock)
            .map(|value| value.map(|inner| Value { inner }))
    }

    pub fn drain(&self) -> Result<()> {
        self.store.drain()
    }

    pub fn flush(&self) -> Result<()> {
        self.store.flush()
    }

    pub fn snapshot(&self) -> Result<CacheSnapshot> {
        let runtime = self.store.snapshot()?;
        Ok(self.cache_snapshot(runtime))
    }

    /// Samples queue, I/O, and per-RegionSet state in addition to the regular
    /// cache summary. This is intended for periodic diagnostics rather than a
    /// request hot path because it briefly locks and scans Region metadata.
    pub fn detailed_snapshot(&self) -> Result<DetailedCacheSnapshot> {
        let runtime = self.store.detailed_snapshot()?;
        let resources = runtime.resources;
        let io = runtime.io;
        let region_sets = runtime
            .region_sets
            .iter()
            .map(|set| RegionSetSnapshot {
                id: set.id,
                capacity_bytes: set.capacity_bytes,
                append_shard_count: set.append_shard_count,
                active_region_count: set.active_region_count,
                free_region_count: set.free_region_count,
                sealed_region_count: set.sealed_region_count,
                live_record_count: set.live_record_count,
                live_bytes: set.live_bytes,
                physical_record_count: set.physical_record_count,
                physical_bytes: set.physical_bytes,
                rotations: set.rotations,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Ok(DetailedCacheSnapshot {
            summary: self.cache_snapshot(runtime.summary),
            queues: CacheQueueSnapshot {
                read_requests_in_flight: resources.read_requests_in_flight,
                read_requests_peak: resources.read_requests_peak,
                read_rejections: resources.read_rejections,
                read_wait_ns: resources.read_wait_ns,
                write_requests_in_flight: resources.write_requests_in_flight,
                write_requests_peak: resources.write_requests_peak,
                write_rejections: resources.write_rejections,
                write_wait_ns: resources.write_wait_ns,
                write_staging_rejections: runtime.staging_rejections,
                write_staging_wait_ns: runtime.staging_wait_ns,
                read_buffers_in_use: resources.read_buffers_in_use,
                read_buffers_peak: resources.read_buffers_peak,
                read_buffer_rejections: resources.read_buffer_rejections,
                read_buffer_timeouts: resources.read_buffer_timeouts,
                read_buffer_waiters: resources.read_buffer_waiters,
                read_buffer_waiters_peak: resources.read_buffer_waiters_peak,
                read_buffer_wait_ns: resources.read_buffer_wait_ns,
                metadata_buffers_in_use: resources.metadata_buffers_in_use,
                metadata_buffers_peak: resources.metadata_buffers_peak,
                metadata_buffer_rejections: resources.metadata_buffer_rejections,
                metadata_buffer_wait_ns: resources.metadata_buffer_wait_ns,
            },
            io: CacheIoSnapshot {
                submitted: io.submitted,
                completed: io.completed,
                cancel_requested: io.cancel_requested,
                cancelled: io.cancelled,
                errors: io.errors,
                in_flight: io.in_flight,
                submit_wait_ns: io.submit_wait_ns,
                completion_ns: io.completion_ns,
                direct_operations: io.direct_operations,
                direct_bytes: io.direct_bytes,
                buffered_operations: io.buffered_operations,
                buffered_bytes: io.buffered_bytes,
            },
            region_sets,
        })
    }

    fn cache_snapshot(&self, runtime: RuntimeSnapshot) -> CacheSnapshot {
        let health = match runtime.health {
            RuntimeHealth::Running => CacheHealth::Running,
            RuntimeHealth::Draining => CacheHealth::Draining,
            RuntimeHealth::MissOnly => CacheHealth::MissOnly,
            RuntimeHealth::Failed => CacheHealth::Failed,
        };
        CacheSnapshot {
            health,
            stats_enabled: runtime.stats_enabled,
            puts: runtime.puts,
            written_bytes: runtime.written_bytes,
            l1_hits: runtime.l1_hits,
            l1_misses: runtime.l1_misses,
            l2_hits: runtime.l2_hits,
            l2_misses: runtime.l2_misses,
            served_bytes: runtime.served_bytes,
            promotions: runtime.promotions,
            l1_evictions: runtime.l1_evictions,
            l1_bypasses: runtime.l1_bypasses,
            l1_admission_rejections: runtime.l1_admission_rejections,
            queue_saturation: runtime.queue_saturation,
            buffer_saturation: runtime.buffer_saturation,
            io_failures: runtime.io_failures,
            region_rotations: runtime.region_rotations,
            managed_memory_bytes: runtime.memory.current_bytes,
            managed_memory_peak_bytes: runtime.memory.peak_bytes,
            managed_memory_budget_bytes: runtime.memory.budget_bytes,
            logical_disk_peak_bytes: self.logical_disk_peak_bytes,
        }
    }

    pub fn close_fast(mut self) -> Result<()> {
        self.store.close_fast()
    }

    pub fn close_warm(mut self) -> Result<()> {
        self.store.close_warm()
    }
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid_config(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn next_persistent_id() -> PersistentId {
    static NEXT_ID: AtomicU64 = AtomicU64::new(1);
    let counter = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let mut bytes = now.to_le_bytes();
    let mix = counter ^ u64::from(std::process::id()).rotate_left(32);
    for (target, source) in bytes[8..].iter_mut().zip(mix.to_le_bytes()) {
        *target ^= source;
    }
    PersistentId::from_bytes(bytes).unwrap_or_else(|| {
        PersistentId::from_bytes(counter.max(1).to_le_bytes().repeat(2).try_into().unwrap())
            .expect("non-zero counter produces a cache identity")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_shards_are_runtime_tunable() {
        let default = RuntimeConfig::default();
        assert_eq!(default.memory_shards(), DEFAULT_MEMORY_SHARDS);
        assert_eq!(default.with_memory_shards(7).memory_shards(), 7);
    }

    #[test]
    fn static_fingerprint_binds_the_hash_algorithm() {
        let config = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let geometry = config.geometry().unwrap();
        let layout = config.region_layout(geometry).unwrap();
        let algorithm = u64::from(KEY_HASH_ALGORITHM_XXH3_64);
        assert_eq!(
            config.fingerprint(geometry, &layout),
            config.fingerprint_with_hash_algorithm(geometry, &layout, algorithm)
        );
        assert_ne!(
            config.fingerprint(geometry, &layout),
            config.fingerprint_with_hash_algorithm(geometry, &layout, algorithm + 1)
        );
    }

    #[test]
    fn static_fingerprint_binds_region_assignment() {
        let base = StaticConfig::new(10 * DEFAULT_REGION_SIZE).with_region_sets([
            RegionSetConfig::new(0).with_weight(1),
            RegionSetConfig::new(2).with_weight(1).with_namespaces([7]),
        ]);
        let changed = base.clone().with_region_sets([
            RegionSetConfig::new(0).with_weight(1),
            RegionSetConfig::new(2)
                .with_weight(1)
                .with_namespaces([7, 9]),
        ]);
        let geometry = base.geometry().unwrap();
        let base_layout = base.region_layout(geometry).unwrap();
        let changed_layout = changed.region_layout(geometry).unwrap();

        assert_ne!(
            base.fingerprint(geometry, &base_layout),
            changed.fingerprint(geometry, &changed_layout)
        );
    }

    #[test]
    fn equivalent_default_region_layouts_share_fingerprint() {
        let implicit = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let explicit = implicit
            .clone()
            .with_region_sets([RegionSetConfig::new(0).with_weight(99).with_namespaces([7])]);
        let geometry = implicit.geometry().unwrap();
        let implicit_layout = implicit.region_layout(geometry).unwrap();
        let explicit_layout = explicit.region_layout(geometry).unwrap();

        assert!(explicit_layout.routes().is_empty());
        assert_eq!(
            implicit.fingerprint(geometry, &implicit_layout),
            explicit.fingerprint(geometry, &explicit_layout)
        );
    }

    #[test]
    fn resolved_region_set_allocations_expose_rounding_and_shards() {
        let config = StaticConfig::new(10 * DEFAULT_REGION_SIZE)
            .with_shards(4)
            .with_region_sets([
                RegionSetConfig::new(0).with_weight(1),
                RegionSetConfig::new(2).with_weight(3),
            ]);

        let allocations = config.region_set_allocations().unwrap();
        assert_eq!(allocations.len(), 2);
        assert_eq!(allocations[0].id.get(), 0);
        assert_eq!(allocations[0].region_count, 3);
        assert_eq!(allocations[0].capacity_bytes, 3 * DEFAULT_REGION_SIZE);
        assert_eq!(allocations[0].append_shard_count, 2);
        assert_eq!(allocations[1].id.get(), 2);
        assert_eq!(allocations[1].region_count, 7);
        assert_eq!(allocations[1].append_shard_count, 2);
    }
}
