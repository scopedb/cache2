//! Fixed-bucket SSD cache for small objects.
//!
//! The engine intentionally has no per-entry DRAM index. A 64-bit Bloom word
//! and one validity bit are kept per bucket; a hit reads one complete bucket,
//! while a mutation performs one bucket read-modify-write. Full keys remain on
//! disk, so a hash collision can only add work, never return a wrong value.

use std::fmt;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::async_cache::{AsyncFailure, TaskContext};
use crate::cache::{
    CacheError, CacheStatus, IoEngineKind, IoMode, PutOptions, PutOutcome, RejectReason,
    RemoveOutcome, Result,
};
use crate::checksum::{Crc32c, crc32c};
use crate::io_backend::{
    DirectIoMode, FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint, read_exact_at,
    write_all_at,
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
use crate::policy::{HostWriteKind, HostWriteTracker, NamespaceId};
use crate::resources::{BufferLease, DedicatedBufferAcquireError, DedicatedBufferPool};

const SUPERBLOCK_SIZE: usize = 4 * 1024;
const SUPERBLOCK_COUNT: usize = 2;
const DATA_OFFSET: u64 = (SUPERBLOCK_SIZE * SUPERBLOCK_COUNT) as u64;
const SUPERBLOCK_MAGIC: [u8; 8] = *b"CRBKT001";
const BUCKET_MAGIC: [u8; 8] = *b"CRBUCKT1";
const FORMAT_VERSION: u16 = 1;
const DEFAULT_BUCKET_SIZE: usize = 4 * 1024;
const MIN_BUCKET_SIZE: usize = 4 * 1024;
const MAX_BUCKET_SIZE: usize = 64 * 1024;
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 1024 * 1024 * 1024;
const MAX_LOCK_SHARDS: usize = 4 * 1024;
const DEFAULT_BUFFER_SLOTS: usize = 64;
const MAX_BUFFER_SLOTS: usize = 128;
const BUCKET_HEADER_SIZE: usize = 64;
const ENTRY_HEADER_SIZE: usize = 32;
const ENTRY_ALIGNMENT: usize = 8;
const SUPERBLOCK_CRC_OFFSET: usize = SUPERBLOCK_SIZE - size_of::<u32>();

const SB_VERSION_OFFSET: usize = 8;
const SB_GENERATION_OFFSET: usize = 16;
const SB_BUCKET_SIZE_OFFSET: usize = 24;
const SB_BUCKET_COUNT_OFFSET: usize = 32;
const SB_HASH_SEED_OFFSET: usize = 40;
const SB_EPOCH_OFFSET: usize = 48;
const SB_CLEAN_OFFSET: usize = 56;

const BUCKET_VERSION_OFFSET: usize = 8;
const BUCKET_GENERATION_OFFSET: usize = 16;
const BUCKET_EPOCH_OFFSET: usize = 24;
const BUCKET_ENTRY_COUNT_OFFSET: usize = 32;
const BUCKET_USED_OFFSET: usize = 36;

const ENTRY_HASH_OFFSET: usize = 0;
const ENTRY_NAMESPACE_OFFSET: usize = 8;
const ENTRY_KEY_LEN_OFFSET: usize = 12;
const ENTRY_FLAGS_OFFSET: usize = 14;
const ENTRY_VALUE_LEN_OFFSET: usize = 16;
const ENTRY_LEN_OFFSET: usize = 20;
const ENTRY_EXPIRES_AT_OFFSET: usize = 24;

/// Configuration for the small-object fixed-bucket engine.
#[derive(Clone, Debug)]
pub struct BucketCacheConfig {
    path: PathBuf,
    capacity: u64,
    bucket_size: usize,
    hash_seed: u64,
    memory_budget_bytes: usize,
    buffer_slots: usize,
    io_engine: IoEngineKind,
    io_mode: IoMode,
    io_queue_depth: usize,
}

impl BucketCacheConfig {
    pub fn new(path: impl AsRef<Path>, capacity: u64) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            capacity,
            bucket_size: DEFAULT_BUCKET_SIZE,
            hash_seed: 0xbb67_ae85_84ca_a73b,
            memory_budget_bytes: DEFAULT_MEMORY_BUDGET_BYTES,
            buffer_slots: DEFAULT_BUFFER_SLOTS,
            io_engine: IoEngineKind::Sync,
            io_mode: IoMode::Buffered,
            io_queue_depth: DEFAULT_IO_QUEUE_DEPTH,
        }
    }

    pub fn with_bucket_size(mut self, bytes: usize) -> Self {
        self.bucket_size = bytes;
        self
    }

    pub fn with_hash_seed(mut self, seed: u64) -> Self {
        self.hash_seed = seed;
        self
    }

    pub fn with_memory_budget(mut self, bytes: usize) -> Self {
        self.memory_budget_bytes = bytes;
        self
    }

    /// Bound concurrent bucket RMW workspaces. Callers block when every slot
    /// is in use; the fixed pool prevents request concurrency from growing
    /// heap usage without limit.
    pub fn with_buffer_slots(mut self, slots: usize) -> Self {
        self.buffer_slots = slots;
        self
    }

    /// Select the positioned-I/O runtime used for bucket data pages.
    pub fn with_io_engine(mut self, engine: IoEngineKind) -> Self {
        self.io_engine = engine;
        self
    }

    /// Select buffered, opportunistic direct, or required direct page I/O.
    pub fn with_io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    /// Bound submitted Bucket data-page I/O. This is independent from the
    /// smaller page-buffer bound and has a process-wide hard maximum.
    pub fn with_io_queue_depth(mut self, depth: usize) -> Self {
        self.io_queue_depth = depth;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn diagnostics(&self) -> Result<BucketConfigDiagnostics> {
        let plan = self.validate()?;
        Ok(BucketConfigDiagnostics {
            path: self.path.clone(),
            capacity_bytes: self.capacity,
            file_len_bytes: plan.file_len,
            bucket_size_bytes: self.bucket_size,
            bucket_count: plan.bucket_count as u64,
            maximum_item_bytes: plan.maximum_item_bytes,
            bloom_bytes: plan.bloom_bytes,
            known_bitmap_bytes: plan.known_bytes,
            lock_shards: plan.lock_shards,
            buffer_slots: self.buffer_slots,
            buffer_bytes: plan.buffer_bytes,
            io_engine: self.io_engine,
            io_mode: self.io_mode,
            io_queue_depth: self.io_queue_depth,
            memory_budget_bytes: self.memory_budget_bytes,
            planned_memory_bytes: plan.planned_memory_bytes,
        })
    }

    pub fn open(self) -> Result<BucketCache> {
        BucketCache::open(self)
    }

    fn validate(&self) -> Result<BucketPlan> {
        #[cfg(not(target_os = "linux"))]
        if self.io_mode == IoMode::Direct {
            return Err(CacheError::InvalidConfig(
                "Bucket direct I/O is unavailable on this build target".into(),
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
                "Bucket io_uring support is unavailable on this build target".into(),
            ));
        }
        if !self.bucket_size.is_power_of_two()
            || !(MIN_BUCKET_SIZE..=MAX_BUCKET_SIZE).contains(&self.bucket_size)
        {
            return Err(CacheError::InvalidConfig(format!(
                "bucket_size must be a power of two in {MIN_BUCKET_SIZE}..={MAX_BUCKET_SIZE}"
            )));
        }
        if !(1..=MAX_BUFFER_SLOTS).contains(&self.buffer_slots) {
            return Err(CacheError::InvalidConfig(format!(
                "bucket buffer_slots must be in 1..={MAX_BUFFER_SLOTS}"
            )));
        }
        if !(1..=MAX_IO_QUEUE_DEPTH).contains(&self.io_queue_depth) {
            return Err(CacheError::InvalidConfig(format!(
                "bucket io_queue_depth must be in 1..={MAX_IO_QUEUE_DEPTH}"
            )));
        }
        let bucket_size_u64 = self.bucket_size as u64;
        if self.capacity < bucket_size_u64 || self.capacity % bucket_size_u64 != 0 {
            return Err(CacheError::InvalidConfig(
                "bucket capacity must be a positive whole number of buckets".into(),
            ));
        }
        let bucket_count_u64 = self.capacity / bucket_size_u64;
        let bucket_count = usize::try_from(bucket_count_u64).map_err(|_| {
            CacheError::InvalidConfig("bucket count exceeds addressable memory".into())
        })?;
        let file_len = DATA_OFFSET
            .checked_add(self.capacity)
            .ok_or_else(|| CacheError::InvalidConfig("bucket file length overflow".into()))?;
        let bloom_bytes = bucket_count
            .checked_mul(size_of::<AtomicU64>())
            .ok_or_else(|| CacheError::InvalidConfig("bucket Bloom memory overflow".into()))?;
        let known_words = bucket_count
            .checked_add(63)
            .and_then(|count| count.checked_div(64))
            .ok_or_else(|| CacheError::InvalidConfig("bucket bitmap size overflow".into()))?;
        let known_bytes = known_words
            .checked_mul(size_of::<AtomicU64>())
            .ok_or_else(|| CacheError::InvalidConfig("bucket bitmap memory overflow".into()))?;
        let lock_shards = bucket_count.clamp(1, MAX_LOCK_SHARDS);
        let lock_bytes = lock_shards
            .checked_mul(size_of::<Mutex<()>>())
            .ok_or_else(|| CacheError::InvalidConfig("bucket lock memory overflow".into()))?;
        // Mutations still own decoded entries. Charge a conservative eight
        // page-equivalents per concurrent workspace: the I/O image, decoded
        // key/value bytes, entry descriptors, a new entry, and Vec growth
        // slack. Read-only gets use a borrowed page view and need only the
        // fixed I/O page plus the returned value.
        let buffer_bytes = self
            .bucket_size
            .checked_mul(self.buffer_slots)
            .and_then(|bytes| bytes.checked_mul(8))
            .ok_or_else(|| CacheError::InvalidConfig("bucket buffer memory overflow".into()))?;
        let planned_memory_bytes = bloom_bytes
            .checked_add(known_bytes)
            // Volatile bucket invalidation uses a second one-bit-per-bucket
            // bitmap. It is intentionally independent from the persisted
            // page format and is reset on every open.
            .and_then(|bytes| bytes.checked_add(known_bytes))
            .and_then(|bytes| bytes.checked_add(lock_bytes))
            .and_then(|bytes| bytes.checked_add(buffer_bytes))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(|| CacheError::InvalidConfig("bucket memory plan overflow".into()))?;
        if planned_memory_bytes > self.memory_budget_bytes {
            return Err(CacheError::InvalidConfig(format!(
                "bucket engine needs {planned_memory_bytes} bytes, exceeding memory budget {}",
                self.memory_budget_bytes
            )));
        }
        let encoded_capacity = self
            .bucket_size
            .checked_sub(BUCKET_HEADER_SIZE + size_of::<u32>())
            .map(|bytes| bytes / ENTRY_ALIGNMENT * ENTRY_ALIGNMENT)
            .ok_or_else(|| CacheError::InvalidConfig("bucket has no entry capacity".into()))?;
        let maximum_item_bytes = encoded_capacity
            .checked_sub(ENTRY_HEADER_SIZE)
            .ok_or_else(|| CacheError::InvalidConfig("bucket has no entry capacity".into()))?;
        Ok(BucketPlan {
            bucket_count,
            file_len,
            bloom_bytes,
            known_words,
            known_bytes,
            lock_shards,
            buffer_bytes,
            planned_memory_bytes,
            maximum_item_bytes,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BucketConfigDiagnostics {
    pub path: PathBuf,
    pub capacity_bytes: u64,
    pub file_len_bytes: u64,
    pub bucket_size_bytes: usize,
    pub bucket_count: u64,
    pub maximum_item_bytes: usize,
    pub bloom_bytes: usize,
    pub known_bitmap_bytes: usize,
    pub lock_shards: usize,
    pub buffer_slots: usize,
    pub buffer_bytes: usize,
    pub io_engine: IoEngineKind,
    pub io_mode: IoMode,
    pub io_queue_depth: usize,
    pub memory_budget_bytes: usize,
    pub planned_memory_bytes: usize,
}

struct BucketPlan {
    bucket_count: usize,
    file_len: u64,
    bloom_bytes: usize,
    known_words: usize,
    known_bytes: usize,
    lock_shards: usize,
    buffer_bytes: usize,
    planned_memory_bytes: usize,
    maximum_item_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct BucketCacheStats {
    pub gets: u64,
    pub hits: u64,
    pub misses: u64,
    pub puts: u64,
    pub removes: u64,
    pub evictions: u64,
    pub bloom_misses: u64,
    pub corrupt_buckets: u64,
    pub io_errors: u64,
    pub corruption_errors: u64,
    pub miss_only_transitions: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub bucket_count: u64,
    pub bucket_size_bytes: u64,
    pub io_uring_active: bool,
    pub direct_io_active: bool,
    pub io_submitted: u64,
    pub io_completed: u64,
    pub io_cancel_requested: u64,
    pub io_cancelled: u64,
    pub io_engine_errors: u64,
    pub io_queue_capacity: u64,
    pub io_in_flight: u64,
    pub io_in_flight_peak: u64,
    pub io_submit_wait_ns: u64,
    pub io_completion_ns: u64,
    pub direct_io_operations: u64,
    pub direct_io_bytes: u64,
    pub buffered_io_operations: u64,
    pub buffered_io_bytes: u64,
    pub page_buffer_slots: u64,
    pub page_buffers_in_use: u64,
    pub page_buffers_in_use_peak: u64,
    pub page_buffer_rejections: u64,
    pub page_buffer_wait_ns: u64,
    pub page_buffer_bytes: u64,
}

/// Exact physical usage removed by one managed Bucket mutation.
///
/// Keeping the aligned charge in the receipt makes the post-I/O accounting
/// path infallible: a composing cache never has to reconstruct it from lengths
/// after the page mutation is already durable.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BucketEntryUsage {
    pub(crate) namespace: NamespaceId,
    pub(crate) live_bytes: u64,
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BucketPutReceipt {
    pub(crate) outcome: PutOutcome,
    pub(crate) removed: Vec<BucketEntryUsage>,
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BucketGetReceipt {
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) removed: Vec<BucketEntryUsage>,
}

#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct BucketRemoveReceipt {
    pub(crate) outcome: RemoveOutcome,
    pub(crate) removed: Vec<BucketEntryUsage>,
}

type ManagedPutCommitCallback<'a> =
    dyn FnMut(BucketEntryUsage, &[BucketEntryUsage]) -> Result<()> + 'a;

#[derive(Default)]
struct BucketCounters {
    gets: AtomicU64,
    hits: AtomicU64,
    misses: AtomicU64,
    puts: AtomicU64,
    removes: AtomicU64,
    evictions: AtomicU64,
    bloom_misses: AtomicU64,
    corrupt_buckets: AtomicU64,
    io_errors: AtomicU64,
    corruption_errors: AtomicU64,
    miss_only_transitions: AtomicU64,
    bytes_read: AtomicU64,
    bytes_written: AtomicU64,
}

#[derive(Clone, Copy)]
struct BucketSuperblock {
    generation: u64,
    bucket_size: u32,
    bucket_count: u64,
    hash_seed: u64,
    epoch: u64,
    clean: bool,
}

impl BucketSuperblock {
    fn encode(self) -> [u8; SUPERBLOCK_SIZE] {
        let mut output = [0_u8; SUPERBLOCK_SIZE];
        output[..8].copy_from_slice(&SUPERBLOCK_MAGIC);
        put_u16(&mut output, SB_VERSION_OFFSET, FORMAT_VERSION);
        put_u64(&mut output, SB_GENERATION_OFFSET, self.generation);
        put_u32(&mut output, SB_BUCKET_SIZE_OFFSET, self.bucket_size);
        put_u64(&mut output, SB_BUCKET_COUNT_OFFSET, self.bucket_count);
        put_u64(&mut output, SB_HASH_SEED_OFFSET, self.hash_seed);
        put_u64(&mut output, SB_EPOCH_OFFSET, self.epoch);
        output[SB_CLEAN_OFFSET] = u8::from(self.clean);
        let checksum = crc32c(&output);
        put_u32(&mut output, SUPERBLOCK_CRC_OFFSET, checksum);
        output
    }

    fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != SUPERBLOCK_SIZE
            || input.get(..8)? != SUPERBLOCK_MAGIC
            || get_u16(input, SB_VERSION_OFFSET)? != FORMAT_VERSION
            || !fixed_checksum_matches(input, SUPERBLOCK_CRC_OFFSET)
        {
            return None;
        }
        let clean = match *input.get(SB_CLEAN_OFFSET)? {
            0 => false,
            1 => true,
            _ => return None,
        };
        Some(Self {
            generation: get_u64(input, SB_GENERATION_OFFSET)?,
            bucket_size: get_u32(input, SB_BUCKET_SIZE_OFFSET)?,
            bucket_count: get_u64(input, SB_BUCKET_COUNT_OFFSET)?,
            hash_seed: get_u64(input, SB_HASH_SEED_OFFSET)?,
            epoch: get_u64(input, SB_EPOCH_OFFSET)?,
            clean,
        })
    }
}

struct BucketState {
    status: CacheStatus,
    superblock: BucketSuperblock,
    active_slot: usize,
}

#[derive(Clone, Copy)]
enum BucketFailure {
    Io,
    Corruption,
}

struct PageLease {
    lease: Option<BufferLease>,
    page_size: usize,
}

impl PageLease {
    fn acquire(pool: &DedicatedBufferPool, page_size: usize) -> Option<Self> {
        Some(Self {
            lease: Some(pool.acquire()?),
            page_size,
        })
    }

    fn acquire_controlled(
        pool: &DedicatedBufferPool,
        page_size: usize,
        cancelled: &AtomicBool,
        deadline: Option<Instant>,
    ) -> std::result::Result<Self, DedicatedBufferAcquireError> {
        Ok(Self {
            lease: Some(pool.acquire_controlled(cancelled, deadline)?),
            page_size,
        })
    }

    fn take_buffer(&mut self) -> BufferLease {
        self.lease.take().expect("page lease owns one buffer")
    }

    fn restore_buffer(&mut self, lease: BufferLease) {
        debug_assert!(self.lease.is_none());
        self.lease = Some(lease);
    }
}

#[cfg(test)]
pub(crate) struct BucketPageTestGuard {
    _page: PageLease,
}

impl Deref for PageLease {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.lease
            .as_ref()
            .expect("page lease owns one buffer")
            .prepared(self.page_size)
            .expect("dedicated buffer is fully prepared")
    }
}

impl DerefMut for PageLease {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.lease
            .as_mut()
            .expect("page lease owns one buffer")
            .prepared_mut(self.page_size)
            .expect("dedicated buffer is fully prepared")
    }
}

struct BucketInner {
    config: BucketCacheConfig,
    io: Arc<dyn IoBackend>,
    engine: Arc<dyn IoEngine>,
    host_writes: Option<Arc<HostWriteTracker>>,
    owner_dirty: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    opened_clean: bool,
    operation: RwLock<()>,
    state: Mutex<BucketState>,
    locks: Vec<Mutex<()>>,
    bloom: Vec<AtomicU64>,
    known: Vec<AtomicU64>,
    invalidated: Vec<AtomicU64>,
    pages: DedicatedBufferPool,
    counters: BucketCounters,
    #[cfg(test)]
    expiry_cleanup_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    managed_put_commit_observer: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    test_now_unix_ms: AtomicU64,
}

/// A bounded SSD cache for objects that fit in one fixed-size bucket.
#[derive(Clone)]
pub struct BucketCache {
    inner: Arc<BucketInner>,
}

impl fmt::Debug for BucketCache {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BucketCache")
            .field("path", &self.inner.config.path)
            .field("status", &self.status())
            .finish_non_exhaustive()
    }
}

impl BucketCache {
    fn open(config: BucketCacheConfig) -> Result<Self> {
        Self::open_native(config, None, None)
    }

    /// Open a Bucket tier whose physical writes are accounted by its owner.
    #[allow(dead_code)]
    pub(crate) fn open_managed(
        config: BucketCacheConfig,
        shared_host_writes: Arc<HostWriteTracker>,
    ) -> Result<Self> {
        Self::open_native(config, Some(shared_host_writes), None)
    }

    pub(crate) fn open_managed_with_owner_dirty(
        config: BucketCacheConfig,
        shared_host_writes: Arc<HostWriteTracker>,
        owner_dirty: Arc<dyn Fn() -> Result<()> + Send + Sync>,
    ) -> Result<Self> {
        Self::open_native(config, Some(shared_host_writes), Some(owner_dirty))
    }

    fn open_native(
        config: BucketCacheConfig,
        host_writes: Option<Arc<HostWriteTracker>>,
        owner_dirty: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    ) -> Result<Self> {
        let file = Arc::new(FileBackend::open_with_io_mode(
            &config.path,
            DirectIoMode::from(config.io_mode),
        )?);
        #[cfg(unix)]
        let runtime_files = Some(file.try_clone_runtime_files()?);
        #[cfg(not(unix))]
        let runtime_files = None;
        let io: Arc<dyn IoBackend> = file;
        Self::open_with_runtime(config, io, runtime_files, host_writes, owner_dirty)
    }

    #[cfg(test)]
    fn open_with_backend(config: BucketCacheConfig, io: Box<dyn IoBackend>) -> Result<Self> {
        Self::open_with_backend_and_host_writes(config, io, None)
    }

    #[cfg(test)]
    pub(crate) fn open_with_backend_and_host_writes(
        config: BucketCacheConfig,
        io: Box<dyn IoBackend>,
        host_writes: Option<Arc<HostWriteTracker>>,
    ) -> Result<Self> {
        let io: Arc<dyn IoBackend> = Arc::from(io);
        Self::open_with_runtime(config, io, None, host_writes, None)
    }

    fn open_with_runtime(
        config: BucketCacheConfig,
        io: Arc<dyn IoBackend>,
        #[cfg(unix)] runtime_files: Option<crate::io_backend::RuntimeFileSet>,
        #[cfg(not(unix))] _runtime_files: Option<()>,
        host_writes: Option<Arc<HostWriteTracker>>,
        owner_dirty: Option<Arc<dyn Fn() -> Result<()> + Send + Sync>>,
    ) -> Result<Self> {
        let plan = config.validate()?;
        let bloom = allocate_atomics(plan.bucket_count, "bucket Bloom filter")?;
        let known = allocate_atomics(plan.known_words, "bucket known bitmap")?;
        let invalidated = allocate_atomics(plan.known_words, "bucket invalidation bitmap")?;
        let locks = allocate_locks(plan.lock_shards)?;
        let pages = DedicatedBufferPool::try_new(config.buffer_slots, config.bucket_size)
            .map_err(|error| CacheError::InvalidConfig(format!("bucket page pool: {error}")))?;
        let engine = build_bucket_io_engine(
            &config,
            Arc::clone(&io),
            #[cfg(unix)]
            runtime_files,
            #[cfg(not(unix))]
            None,
        )?;

        io.try_lock_exclusive().map_err(map_lock_error)?;
        let open_result = open_or_format(io.as_ref(), host_writes.as_deref(), &config, &plan);
        let (superblock, active_slot, all_buckets_empty, opened_clean) = match open_result {
            Ok(opened) => opened,
            Err(error) => {
                let _ = engine.shutdown();
                let _ = io.unlock();
                return Err(error);
            }
        };
        if all_buckets_empty {
            for word in &known {
                word.store(u64::MAX, Ordering::Relaxed);
            }
        }
        Ok(Self {
            inner: Arc::new(BucketInner {
                config,
                io,
                engine,
                host_writes,
                owner_dirty,
                opened_clean,
                operation: RwLock::new(()),
                state: Mutex::new(BucketState {
                    status: CacheStatus::Healthy,
                    superblock,
                    active_slot,
                }),
                locks,
                bloom,
                known,
                invalidated,
                pages,
                counters: BucketCounters::default(),
                #[cfg(test)]
                expiry_cleanup_observer: Mutex::new(None),
                #[cfg(test)]
                managed_put_commit_observer: Mutex::new(None),
                #[cfg(test)]
                test_now_unix_ms: AtomicU64::new(0),
            }),
        })
    }

    pub fn status(&self) -> CacheStatus {
        lock_mutex(&self.inner.state).status
    }

    pub(crate) fn opened_clean(&self) -> bool {
        self.inner.opened_clean
    }

    #[cfg(test)]
    pub(crate) fn set_expiry_cleanup_observer_for_test(
        &self,
        observer: Arc<dyn Fn() + Send + Sync>,
    ) {
        *lock_mutex(&self.inner.expiry_cleanup_observer) = Some(observer);
    }

    #[cfg(test)]
    pub(crate) fn set_managed_put_commit_observer_for_test(
        &self,
        observer: Arc<dyn Fn() + Send + Sync>,
    ) {
        *lock_mutex(&self.inner.managed_put_commit_observer) = Some(observer);
    }

    #[cfg(test)]
    pub(crate) fn set_now_unix_ms_for_test(&self, now: u64) {
        self.inner.test_now_unix_ms.store(now, Ordering::Release);
    }

    pub(crate) fn poison_managed_accounting(&self) {
        self.poison();
    }

    #[cfg(test)]
    pub(crate) fn force_miss_only_for_test(&self) {
        self.enter_miss_only(BucketFailure::Io);
    }

    pub fn maximum_item_bytes(&self) -> usize {
        let encoded_capacity =
            (self.inner.config.bucket_size - BUCKET_HEADER_SIZE - size_of::<u32>())
                / ENTRY_ALIGNMENT
                * ENTRY_ALIGNMENT;
        encoded_capacity - ENTRY_HEADER_SIZE
    }

    pub(crate) fn fits(&self, key_len: usize, value_len: usize) -> bool {
        encoded_entry_len(key_len, value_len).is_some_and(|length| {
            length <= self.inner.config.bucket_size - BUCKET_HEADER_SIZE - size_of::<u32>()
        })
    }

    pub(crate) fn bucket_id_for(&self, namespace: NamespaceId, key: &[u8]) -> u64 {
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        self.bucket_id(hash) as u64
    }

    /// Conservative in-memory membership hint for a composing driver.
    ///
    /// Unknown pages return `true`. Once a page has been verified, its Bloom
    /// word can prove absence but is never used to prove presence.
    #[allow(dead_code)]
    pub(crate) fn may_contain_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<bool> {
        let _operation = self.read_operation()?;
        self.ensure_healthy()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let bucket_id = self.bucket_id(hash);
        if self.is_invalidated(bucket_id) {
            return Ok(false);
        }
        if !self.is_known(bucket_id) {
            return Ok(true);
        }
        let mask = bloom_mask(hash);
        Ok(self.inner.bloom[bucket_id].load(Ordering::Acquire) & mask == mask)
    }

    /// Hide one complete bucket for the lifetime of this process without
    /// issuing device I/O. The next successful put rebuilds that bucket from
    /// an empty page, so entries hidden by the invalidation cannot reappear.
    #[allow(dead_code)]
    pub(crate) fn invalidate_bucket_in_memory(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<()> {
        let _operation = self.read_operation()?;
        self.ensure_healthy()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let bucket_id = self.bucket_id(hash);
        let _bucket = lock_mutex(&self.inner.locks[bucket_id % self.inner.locks.len()]);
        self.ensure_healthy()?;
        self.set_invalidated(bucket_id);
        // Keep the ordinary membership hint consistent for callers that look
        // at diagnostics while the bucket is hidden.
        self.set_bloom(bucket_id, 0);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_in(0, key)
    }

    pub fn get_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get_in_with_context(namespace, key, None, None)
    }

    pub(crate) fn get_in_managed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<BucketGetReceipt> {
        let mut removed = Vec::new();
        let value = self.get_in_with_context(namespace, key, None, Some(&mut removed))?;
        Ok(BucketGetReceipt { value, removed })
    }

    pub(crate) fn get_in_managed_with_task_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        context: &TaskContext,
    ) -> Result<BucketGetReceipt> {
        let mut removed = Vec::new();
        let value = self.get_in_with_context(namespace, key, Some(context), Some(&mut removed))?;
        Ok(BucketGetReceipt { value, removed })
    }

    fn get_in_with_context(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        context: Option<&TaskContext>,
        mut removed: Option<&mut Vec<BucketEntryUsage>>,
    ) -> Result<Option<Vec<u8>>> {
        if context.is_some_and(TaskContext::is_stopped) {
            return Err(context_stop_error(context));
        }
        let _operation = self.read_operation()?;
        self.inner.counters.gets.fetch_add(1, Ordering::Relaxed);
        if context.is_some_and(TaskContext::is_stopped) {
            return Err(context_stop_error(context));
        }
        if !self.reads_enabled()? {
            self.record_miss();
            return Ok(None);
        }
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let bucket_id = self.bucket_id(hash);
        if self.is_invalidated(bucket_id) {
            self.record_miss();
            return Ok(None);
        }
        if self.is_known(bucket_id)
            && self.inner.bloom[bucket_id].load(Ordering::Acquire) & bloom_mask(hash)
                != bloom_mask(hash)
        {
            self.inner
                .counters
                .bloom_misses
                .fetch_add(1, Ordering::Relaxed);
            self.record_miss();
            return Ok(None);
        }
        let _bucket = lock_mutex(&self.inner.locks[bucket_id % self.inner.locks.len()]);
        if self.is_invalidated(bucket_id) {
            self.record_miss();
            return Ok(None);
        }
        if context.is_some_and(TaskContext::is_stopped) {
            return Err(context_stop_error(context));
        }
        let page = match context.map_or_else(
            || self.acquire_page(),
            |context| self.acquire_page_with_task_context(context),
        ) {
            Ok(page) => page,
            Err(_) if self.status() == CacheStatus::MissOnly => {
                self.record_miss();
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        if !self.reads_enabled()? {
            self.record_miss();
            return Ok(None);
        }
        if context.is_some_and(TaskContext::is_stopped) {
            return Err(context_stop_error(context));
        }
        let epoch = lock_mutex(&self.inner.state).superblock.epoch;
        let mut page = match self.read_page(page, self.bucket_offset(bucket_id)?, context) {
            Ok(page) => page,
            Err(error) if is_context_stop_io_error(context, &error) => {
                return Err(context_stop_io_error(context, &error));
            }
            Err(_) => {
                self.enter_miss_only(BucketFailure::Io);
                self.record_miss();
                return Ok(None);
            }
        };
        if context.is_some_and(TaskContext::is_stopped) {
            return Err(context_stop_error(context));
        }
        self.inner
            .counters
            .bytes_read
            .fetch_add(page.len() as u64, Ordering::Relaxed);
        let now = self.now_unix_ms();
        let scan = match decode_bucket_view(&page, epoch) {
            BucketViewDecode::Empty => BucketGetScan::empty(),
            BucketViewDecode::Valid(view) => match view.scan_for_get(hash, namespace, key, now) {
                Some(scan) => scan,
                None => {
                    if let Some(context) = context {
                        if !context.try_commit() {
                            return Err(context_stop_error(Some(context)));
                        }
                    }
                    self.mark_owner_dirty_for_autonomous_mutation()?;
                    let _ = self.quarantine_corrupt_page();
                    self.record_miss();
                    return Ok(None);
                }
            },
            BucketViewDecode::Corrupt => {
                if let Some(context) = context {
                    if !context.try_commit() {
                        return Err(context_stop_error(Some(context)));
                    }
                }
                self.mark_owner_dirty_for_autonomous_mutation()?;
                let _ = self.quarantine_corrupt_page();
                self.record_miss();
                return Ok(None);
            }
        };
        if !self.reads_enabled()? {
            self.record_miss();
            return Ok(None);
        }
        let found_expired = removed.is_some() && scan.found_expired;
        let mut cleanup_expired = found_expired;
        let mut daily_guard = None;
        if cleanup_expired {
            prepare_usage_receipt(&mut removed, scan.entry_count)?;
            if let Some(host_writes) = self.inner.host_writes.as_ref() {
                match host_writes.try_reserve_daily(self.inner.config.bucket_size as u64) {
                    Ok(reservation) => daily_guard = Some(reservation),
                    Err(_) => cleanup_expired = false,
                }
            }
        }
        if cleanup_expired {
            // Expiry cleanup is a mutation and still uses the owned codec. The
            // common read-only path above never constructs OwnedEntry values.
            let mut decoded = match decode_bucket(&page, epoch) {
                BucketDecode::Valid(contents) => contents,
                BucketDecode::AllocationFailed => {
                    return Err(CacheError::Overloaded(
                        crate::resources::OverloadReason::ReadBufferUnavailable,
                    ));
                }
                BucketDecode::Empty | BucketDecode::Corrupt => {
                    if let Some(context) = context {
                        if !context.try_commit() {
                            return Err(context_stop_error(Some(context)));
                        }
                    }
                    self.mark_owner_dirty_for_autonomous_mutation()?;
                    let _ = self.quarantine_corrupt_page();
                    self.record_miss();
                    return Ok(None);
                }
            };
            let Some(next_generation) = decoded.generation.checked_add(1) else {
                if let Some(context) = context {
                    if !context.try_commit() {
                        return Err(context_stop_error(Some(context)));
                    }
                }
                self.poison();
                return Err(CacheError::CorruptMetadata(
                    "bucket page generation exhausted",
                ));
            };
            decoded.entries.retain(|entry| {
                let expired = entry.is_expired(now);
                if expired {
                    record_removed_usage(&mut removed, entry);
                }
                !expired
            });
            decoded.generation = next_generation.max(1);
            let page_offset = self.bucket_offset(bucket_id)?;
            if let Err(error) = encode_bucket(&mut page, &decoded) {
                if let Some(context) = context {
                    if !context.try_commit() {
                        return Err(context_stop_error(Some(context)));
                    }
                }
                self.poison();
                return Err(error);
            }
            if let Some(context) = context {
                if !context.try_commit() {
                    return Err(context_stop_error(Some(context)));
                }
            }
            #[cfg(test)]
            let observer = { lock_mutex(&self.inner.expiry_cleanup_observer).take() };
            #[cfg(test)]
            if let Some(observer) = observer {
                observer();
            }
            // The TaskContext phase transition above is the mutation commit
            // point. Cancellation cannot complete from here through the dirty
            // fence, compacted page write, and managed usage receipt.
            self.mark_owner_dirty_for_autonomous_mutation()?;
            self.ensure_dirty()?;
            if let Err(error) =
                self.write_page_with_kind(page, page_offset, HostWriteKind::Reclaimer)
            {
                self.enter_miss_only(BucketFailure::Io);
                return Err(CacheError::Io(error));
            }
            self.ensure_healthy()?;
            self.inner
                .counters
                .bytes_written
                .fetch_add(self.inner.config.bucket_size as u64, Ordering::Relaxed);
            if let Some(daily_guard) = daily_guard {
                daily_guard.commit();
            }
            self.set_bloom(bucket_id, decoded.bloom(now));
            let result = decoded
                .entries
                .into_iter()
                .rev()
                .find(|entry| {
                    entry.hash == hash
                        && entry.namespace == namespace
                        && entry.key == key
                        && !entry.is_expired(now)
                })
                .map(|entry| entry.value);
            if let Some(value) = result {
                self.inner.counters.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(Some(value));
            }
            self.record_miss();
            return Ok(None);
        }
        if !found_expired {
            self.set_bloom(bucket_id, scan.bloom);
        }
        let result = scan
            .matching_value
            .map(|(start, end)| {
                try_copy_bytes(&page[start..end]).ok_or(CacheError::Overloaded(
                    crate::resources::OverloadReason::ReadBufferUnavailable,
                ))
            })
            .transpose()?;
        if let Some(value) = result {
            self.inner.counters.hits.fetch_add(1, Ordering::Relaxed);
            Ok(Some(value))
        } else {
            self.record_miss();
            Ok(None)
        }
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        self.put_in(0, key, value, options)
    }

    pub fn put_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        options: PutOptions,
    ) -> Result<PutOutcome> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.put_in_with_removed(namespace, key, value, options, None, None)
    }

    #[allow(dead_code)]
    pub(crate) fn put_in_managed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Result<BucketPutReceipt> {
        let mut removed = Vec::new();
        let outcome =
            self.put_in_with_removed(namespace, key, value, options, Some(&mut removed), None)?;
        Ok(BucketPutReceipt { outcome, removed })
    }

    pub(crate) fn put_in_managed_with_commit(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        mut commit: impl FnMut(BucketEntryUsage, &[BucketEntryUsage]) -> Result<()>,
    ) -> Result<BucketPutReceipt> {
        let mut removed = Vec::new();
        let outcome = self.put_in_with_removed(
            namespace,
            key,
            value,
            options,
            Some(&mut removed),
            Some(&mut commit),
        )?;
        Ok(BucketPutReceipt { outcome, removed })
    }

    fn put_in_with_removed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
        mut removed: Option<&mut Vec<BucketEntryUsage>>,
        mut commit: Option<&mut ManagedPutCommitCallback<'_>>,
    ) -> Result<PutOutcome> {
        let _operation = self.read_operation()?;
        self.ensure_healthy()?;
        let now = self.now_unix_ms();
        let expires_at = match options.expires_at_unix_ms {
            Some(expires_at) if expires_at <= now => {
                return Ok(PutOutcome::Rejected(RejectReason::AlreadyExpired));
            }
            Some(expires_at) => expires_at,
            None => 0,
        };
        let Some(entry_len) = encoded_entry_len(key.len(), value.len()) else {
            return Ok(PutOutcome::Rejected(RejectReason::RecordTooLarge));
        };
        if entry_len > self.inner.config.bucket_size - BUCKET_HEADER_SIZE - size_of::<u32>() {
            return Ok(PutOutcome::Rejected(RejectReason::RecordTooLarge));
        }

        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let bucket_id = self.bucket_id(hash);
        let _bucket = lock_mutex(&self.inner.locks[bucket_id % self.inner.locks.len()]);
        let rebuild_invalidated = self.is_invalidated(bucket_id);
        let mut page = self.acquire_page()?;
        self.ensure_healthy()?;
        let epoch = lock_mutex(&self.inner.state).superblock.epoch;
        let mut contents = if rebuild_invalidated {
            BucketContents::empty(epoch)
        } else {
            page = match self.read_page(page, self.bucket_offset(bucket_id)?, None) {
                Ok(page) => page,
                Err(error) => {
                    self.enter_miss_only(BucketFailure::Io);
                    return Err(CacheError::Io(error));
                }
            };
            self.inner
                .counters
                .bytes_read
                .fetch_add(page.len() as u64, Ordering::Relaxed);
            match decode_bucket(&page, epoch) {
                BucketDecode::Valid(contents) => contents,
                BucketDecode::Empty => BucketContents::empty(epoch),
                BucketDecode::Corrupt => {
                    self.quarantine_corrupt_page()?;
                    return Err(CacheError::CorruptMetadata(
                        "bucket page checksum or encoding is invalid",
                    ));
                }
                BucketDecode::AllocationFailed => {
                    return Err(CacheError::Overloaded(
                        crate::resources::OverloadReason::WriteBufferUnavailable,
                    ));
                }
            }
        };
        prepare_usage_receipt(&mut removed, contents.entries.len())?;
        contents.entries.retain(|entry| {
            let same_key = entry.hash == hash && entry.namespace == namespace && entry.key == key;
            let discard = entry.is_expired(now) || same_key;
            if discard {
                record_removed_usage(&mut removed, entry);
            }
            !discard
        });
        let Some(new_key) = try_copy_bytes(key) else {
            return Err(CacheError::Overloaded(
                crate::resources::OverloadReason::WriteBufferUnavailable,
            ));
        };
        let Some(new_value) = try_copy_bytes(value) else {
            return Err(CacheError::Overloaded(
                crate::resources::OverloadReason::WriteBufferUnavailable,
            ));
        };
        if contents.entries.try_reserve_exact(1).is_err() {
            return Err(CacheError::Overloaded(
                crate::resources::OverloadReason::WriteBufferUnavailable,
            ));
        }
        let new_entry = OwnedEntry {
            hash,
            namespace,
            key: new_key,
            value: new_value,
            expires_at,
        };
        let new_usage = new_entry.usage();
        let capacity_end = self.inner.config.bucket_size - size_of::<u32>();
        let mut used = BUCKET_HEADER_SIZE
            + contents
                .entries
                .iter()
                .map(OwnedEntry::encoded_len)
                .sum::<usize>()
            + new_entry.encoded_len();
        let mut evicted = 0_u64;
        while used > capacity_end && !contents.entries.is_empty() {
            let victim = contents.entries.remove(0);
            used -= victim.encoded_len();
            record_removed_usage(&mut removed, &victim);
            evicted = evicted.saturating_add(1);
        }
        if used > capacity_end {
            if let Some(removed) = removed {
                removed.clear();
            }
            return Ok(PutOutcome::Rejected(RejectReason::RecordTooLarge));
        }
        contents.entries.push(new_entry);
        let Some(next_generation) = contents.generation.checked_add(1) else {
            self.poison();
            return Err(CacheError::CorruptMetadata(
                "bucket page generation exhausted",
            ));
        };
        contents.generation = next_generation.max(1);
        self.ensure_dirty()?;
        if let Err(error) = encode_bucket(&mut page, &contents) {
            self.poison();
            return Err(error);
        }
        if let Err(error) = self.write_page(page, self.bucket_offset(bucket_id)?) {
            self.enter_miss_only(BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        self.ensure_healthy()?;
        if let Some(commit) = commit.as_mut() {
            #[cfg(test)]
            let observer = { lock_mutex(&self.inner.managed_put_commit_observer).take() };
            #[cfg(test)]
            if let Some(observer) = observer {
                observer();
            }
            let retired = removed
                .as_ref()
                .map_or(&[][..], |removed| removed.as_slice());
            if let Err(error) = (*commit)(new_usage, retired) {
                self.poison();
                return Err(error);
            }
        }
        self.inner
            .counters
            .bytes_written
            .fetch_add(self.inner.config.bucket_size as u64, Ordering::Relaxed);
        self.inner.counters.puts.fetch_add(1, Ordering::Relaxed);
        self.inner
            .counters
            .evictions
            .fetch_add(evicted, Ordering::Relaxed);
        self.set_bloom(bucket_id, contents.bloom(now));
        if rebuild_invalidated {
            // Publish the rebuilt membership state before making the bucket
            // visible to lock-free may_contain checks.
            self.clear_invalidated(bucket_id);
        }
        Ok(PutOutcome::Stored)
    }

    pub fn remove(&self, key: &[u8]) -> Result<RemoveOutcome> {
        self.remove_in(0, key)
    }

    pub fn remove_in(&self, namespace: NamespaceId, key: &[u8]) -> Result<RemoveOutcome> {
        self.remove_in_with_removed(namespace, key, None)
    }

    #[allow(dead_code)]
    pub(crate) fn remove_in_managed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
    ) -> Result<BucketRemoveReceipt> {
        let mut removed = Vec::new();
        let outcome = self.remove_in_with_removed(namespace, key, Some(&mut removed))?;
        Ok(BucketRemoveReceipt { outcome, removed })
    }

    fn remove_in_with_removed(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        mut removed: Option<&mut Vec<BucketEntryUsage>>,
    ) -> Result<RemoveOutcome> {
        let _operation = self.read_operation()?;
        self.ensure_healthy()?;
        let hash = hash_namespaced_key(self.inner.config.hash_seed, namespace, key);
        let bucket_id = self.bucket_id(hash);
        if self.is_invalidated(bucket_id) {
            return Ok(RemoveOutcome::NotFound);
        }
        if self.is_known(bucket_id)
            && self.inner.bloom[bucket_id].load(Ordering::Acquire) & bloom_mask(hash)
                != bloom_mask(hash)
        {
            return Ok(RemoveOutcome::NotFound);
        }
        let _bucket = lock_mutex(&self.inner.locks[bucket_id % self.inner.locks.len()]);
        if self.is_invalidated(bucket_id) {
            return Ok(RemoveOutcome::NotFound);
        }
        let page = self.acquire_page()?;
        self.ensure_healthy()?;
        let epoch = lock_mutex(&self.inner.state).superblock.epoch;
        let mut page = match self.read_page(page, self.bucket_offset(bucket_id)?, None) {
            Ok(page) => page,
            Err(error) => {
                self.enter_miss_only(BucketFailure::Io);
                return Err(CacheError::Io(error));
            }
        };
        self.inner
            .counters
            .bytes_read
            .fetch_add(page.len() as u64, Ordering::Relaxed);
        let mut contents = match decode_bucket(&page, epoch) {
            BucketDecode::Valid(contents) => contents,
            BucketDecode::Empty => return Ok(RemoveOutcome::NotFound),
            BucketDecode::Corrupt => {
                self.quarantine_corrupt_page()?;
                return Err(CacheError::CorruptMetadata(
                    "bucket page checksum or encoding is invalid",
                ));
            }
            BucketDecode::AllocationFailed => {
                return Err(CacheError::Overloaded(
                    crate::resources::OverloadReason::WriteBufferUnavailable,
                ));
            }
        };
        let now = self.now_unix_ms();
        let target_present = contents.entries.iter().any(|entry| {
            entry.hash == hash
                && entry.namespace == namespace
                && entry.key == key
                && !entry.is_expired(now)
        });
        let managed_cleanup = removed.is_some()
            && contents.entries.iter().any(|entry| {
                entry.is_expired(now)
                    || (entry.hash == hash && entry.namespace == namespace && entry.key == key)
            });
        if !target_present && !managed_cleanup {
            self.set_bloom(bucket_id, contents.bloom(now));
            return Ok(RemoveOutcome::NotFound);
        }
        prepare_usage_receipt(&mut removed, contents.entries.len())?;
        contents.entries.retain(|entry| {
            let same_key = entry.hash == hash && entry.namespace == namespace && entry.key == key;
            let discard = entry.is_expired(now) || same_key;
            if discard {
                record_removed_usage(&mut removed, entry);
            }
            !discard
        });
        let Some(next_generation) = contents.generation.checked_add(1) else {
            self.poison();
            return Err(CacheError::CorruptMetadata(
                "bucket page generation exhausted",
            ));
        };
        contents.generation = next_generation.max(1);
        self.ensure_dirty()?;
        if let Err(error) = encode_bucket(&mut page, &contents) {
            self.poison();
            return Err(error);
        }
        if let Err(error) = self.write_page(page, self.bucket_offset(bucket_id)?) {
            self.enter_miss_only(BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        self.ensure_healthy()?;
        self.inner
            .counters
            .bytes_written
            .fetch_add(self.inner.config.bucket_size as u64, Ordering::Relaxed);
        if target_present {
            self.inner.counters.removes.fetch_add(1, Ordering::Relaxed);
        }
        self.set_bloom(bucket_id, contents.bloom(now));
        Ok(if target_present {
            RemoveOutcome::Removed
        } else {
            RemoveOutcome::NotFound
        })
    }

    pub fn clear(&self) -> Result<()> {
        let _operation = self.write_operation()?;
        let mut state = lock_mutex(&self.inner.state);
        ensure_state_healthy(&state)?;
        let Some(next_epoch) = state.superblock.epoch.checked_add(1) else {
            state.status = CacheStatus::Poisoned;
            return Err(CacheError::CorruptMetadata("bucket epoch exhausted"));
        };
        let Some(next_generation) = state.superblock.generation.checked_add(1) else {
            state.status = CacheStatus::Poisoned;
            return Err(CacheError::CorruptMetadata(
                "bucket superblock generation exhausted",
            ));
        };
        let next = BucketSuperblock {
            generation: next_generation,
            epoch: next_epoch,
            // No page from the previous epoch is visible, so the new empty
            // epoch is already a clean checkpoint.
            clean: true,
            ..state.superblock
        };
        let first_slot = 1 - state.active_slot;
        let second_slot = state.active_slot;
        if let Err(error) = self.write_superblock(first_slot, next) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::ClearBarrier,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        // Return success only after the epoch fence is redundant. Either
        // Superblock may then be lost without making pre-clear pages visible.
        if let Err(error) = self.write_superblock(second_slot, next) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::ClearBarrier,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        state.superblock = next;
        state.active_slot = second_slot;
        for bloom in &self.inner.bloom {
            bloom.store(0, Ordering::Release);
        }
        for word in &self.inner.known {
            word.store(u64::MAX, Ordering::Release);
        }
        for word in &self.inner.invalidated {
            word.store(0, Ordering::Release);
        }
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let _operation = self.write_operation()?;
        self.flush_locked()
    }

    pub fn close(&self) -> Result<()> {
        let _operation = self.write_operation_allow_closed()?;
        let status = lock_mutex(&self.inner.state).status;
        if status == CacheStatus::Closed {
            return Ok(());
        }

        // Releasing the process lock is unconditional. In particular, a
        // poisoned cache must still become reopenable as soon as close()
        // returns, even though publishing a clean checkpoint is forbidden.
        let flush = match status {
            CacheStatus::Healthy => self.flush_locked(),
            CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Ok(()),
        };
        self.inner.pages.close();
        let shutdown = self.inner.engine.shutdown().map_err(CacheError::Io);
        let unlock = if self.inner.engine.has_unfenced_mutations() {
            Err(CacheError::Io(std::io::Error::other(
                "Bucket lock retained because an I/O mutation is not fenced",
            )))
        } else {
            self.inner.io.unlock().map_err(CacheError::Io)
        };
        lock_mutex(&self.inner.state).status = CacheStatus::Closed;
        flush.and(shutdown).and(unlock)
    }

    /// Drain the engine and release its file lock without publishing a clean
    /// Superblock. The composing Hybrid cache uses this when its global dirty
    /// fence could not be made durable, so an older clean manifest can never
    /// be paired with a newly-clean lower tier after a crash.
    pub(crate) fn close_without_checkpoint(&self) -> Result<()> {
        self.poison();
        self.close()
    }

    pub fn stats(&self) -> BucketCacheStats {
        let io = self.inner.engine.stats();
        let pages = self.inner.pages.snapshot();
        BucketCacheStats {
            gets: self.inner.counters.gets.load(Ordering::Relaxed),
            hits: self.inner.counters.hits.load(Ordering::Relaxed),
            misses: self.inner.counters.misses.load(Ordering::Relaxed),
            puts: self.inner.counters.puts.load(Ordering::Relaxed),
            removes: self.inner.counters.removes.load(Ordering::Relaxed),
            evictions: self.inner.counters.evictions.load(Ordering::Relaxed),
            bloom_misses: self.inner.counters.bloom_misses.load(Ordering::Relaxed),
            corrupt_buckets: self.inner.counters.corrupt_buckets.load(Ordering::Relaxed),
            io_errors: self.inner.counters.io_errors.load(Ordering::Relaxed),
            corruption_errors: self
                .inner
                .counters
                .corruption_errors
                .load(Ordering::Relaxed),
            miss_only_transitions: self
                .inner
                .counters
                .miss_only_transitions
                .load(Ordering::Relaxed),
            bytes_read: self.inner.counters.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.inner.counters.bytes_written.load(Ordering::Relaxed),
            bucket_count: self.inner.bloom.len() as u64,
            bucket_size_bytes: self.inner.config.bucket_size as u64,
            io_uring_active: self.inner.engine.kind() == EngineKind::IoUring,
            direct_io_active: self.inner.engine.direct_active(),
            io_submitted: io.submitted,
            io_completed: io.completed,
            io_cancel_requested: io.cancel_requested,
            io_cancelled: io.cancelled,
            io_engine_errors: io.errors,
            io_queue_capacity: self.inner.engine.queue_depth() as u64,
            io_in_flight: io.in_flight,
            io_in_flight_peak: io.in_flight_peak,
            io_submit_wait_ns: io.submit_wait_ns,
            io_completion_ns: io.completion_ns,
            direct_io_operations: io.direct_operations,
            direct_io_bytes: io.direct_bytes,
            buffered_io_operations: io.buffered_operations,
            buffered_io_bytes: io.buffered_bytes,
            page_buffer_slots: pages.slots,
            page_buffers_in_use: pages.in_use,
            page_buffers_in_use_peak: pages.in_use_peak,
            page_buffer_rejections: pages.rejections,
            page_buffer_wait_ns: pages.wait_ns,
            page_buffer_bytes: pages.allocated_bytes,
        }
    }

    /// Stream current-epoch physical usage one page at a time.
    ///
    /// The exclusive operation guard gives a composing driver a stable startup
    /// view without allocating storage proportional to the device capacity.
    #[allow(dead_code)]
    pub(crate) fn scan_live_entries<F>(&self, mut callback: F) -> Result<()>
    where
        F: FnMut(BucketEntryUsage) -> Result<()>,
    {
        let _operation = self.write_operation()?;
        let epoch = lock_mutex(&self.inner.state).superblock.epoch;
        let mut page = self.acquire_page()?;
        for bucket_id in 0..self.inner.bloom.len() {
            page = match self.read_page(page, self.bucket_offset(bucket_id)?, None) {
                Ok(page) => page,
                Err(error) => {
                    self.enter_miss_only(BucketFailure::Io);
                    return Err(CacheError::Io(error));
                }
            };
            self.inner
                .counters
                .bytes_read
                .fetch_add(page.len() as u64, Ordering::Relaxed);
            match decode_bucket(&page, epoch) {
                BucketDecode::Empty => self.set_bloom(bucket_id, 0),
                BucketDecode::Valid(contents) => {
                    // Expired entries remain physical identities until a
                    // managed page compaction commits and returns an exact
                    // retirement receipt. Recovery must therefore count and
                    // advertise them too; filtering here would make the later
                    // receipt subtract the same bytes from unrelated entries.
                    self.set_bloom(bucket_id, contents.physical_bloom());
                    for entry in &contents.entries {
                        callback(entry.usage())?;
                    }
                }
                BucketDecode::Corrupt => {
                    self.quarantine_corrupt_page()?;
                    return Err(CacheError::CorruptMetadata(
                        "bucket page checksum or encoding is invalid",
                    ));
                }
                BucketDecode::AllocationFailed => {
                    return Err(CacheError::Overloaded(
                        crate::resources::OverloadReason::ReadBufferUnavailable,
                    ));
                }
            }
        }
        Ok(())
    }

    fn flush_locked(&self) -> Result<()> {
        let mut state = lock_mutex(&self.inner.state);
        ensure_state_healthy(&state)?;
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::CheckpointData,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        let Some(next_generation) = state.superblock.generation.checked_add(1) else {
            state.status = CacheStatus::Poisoned;
            return Err(CacheError::CorruptMetadata(
                "bucket superblock generation exhausted",
            ));
        };
        let next = BucketSuperblock {
            generation: next_generation,
            clean: true,
            ..state.superblock
        };
        let slot = 1 - state.active_slot;
        if let Err(error) = self.write_superblock(slot, next) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::CheckpointClean,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        state.superblock = next;
        state.active_slot = slot;
        Ok(())
    }

    fn ensure_dirty(&self) -> Result<()> {
        let mut state = lock_mutex(&self.inner.state);
        ensure_state_healthy(&state)?;
        if !state.superblock.clean {
            return Ok(());
        }
        let Some(first_generation) = state.superblock.generation.checked_add(1) else {
            state.status = CacheStatus::Poisoned;
            return Err(CacheError::CorruptMetadata(
                "bucket superblock generation exhausted",
            ));
        };
        let Some(second_generation) = first_generation.checked_add(1) else {
            state.status = CacheStatus::Poisoned;
            return Err(CacheError::CorruptMetadata(
                "bucket superblock generation exhausted",
            ));
        };
        let first = BucketSuperblock {
            generation: first_generation,
            clean: false,
            ..state.superblock
        };
        let first_slot = 1 - state.active_slot;
        let second_slot = state.active_slot;
        if let Err(error) = self.write_superblock(first_slot, first) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::DirtyMarker,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        let second = BucketSuperblock {
            generation: second_generation,
            ..first
        };
        if let Err(error) = self.write_superblock(second_slot, second) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(error);
        }
        if let Err(error) = sync_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            SyncPoint::DirtyMarker,
            SyncMode::Data,
        ) {
            self.enter_miss_only_locked(&mut state, BucketFailure::Io);
            return Err(CacheError::Io(error));
        }
        state.superblock = second;
        state.active_slot = second_slot;
        Ok(())
    }

    fn write_superblock(&self, slot: usize, superblock: BucketSuperblock) -> Result<()> {
        write_all_at_tracked(
            self.inner.io.as_ref(),
            self.inner.host_writes.as_deref(),
            WritePoint::Superblock,
            HostWriteKind::Metadata,
            &superblock.encode(),
            (slot * SUPERBLOCK_SIZE) as u64,
        )
        .map_err(CacheError::Io)
    }

    fn read_operation(&self) -> Result<RwLockReadGuard<'_, ()>> {
        self.inner
            .operation
            .read()
            .map_err(|_| CacheError::Poisoned)
    }

    fn write_operation(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        let guard = self.write_operation_allow_closed()?;
        self.ensure_healthy()?;
        Ok(guard)
    }

    fn write_operation_allow_closed(&self) -> Result<RwLockWriteGuard<'_, ()>> {
        self.inner
            .operation
            .write()
            .map_err(|_| CacheError::Poisoned)
    }

    fn ensure_healthy(&self) -> Result<()> {
        ensure_state_healthy(&lock_mutex(&self.inner.state))
    }

    fn reads_enabled(&self) -> Result<bool> {
        match lock_mutex(&self.inner.state).status {
            CacheStatus::Healthy => Ok(true),
            CacheStatus::MissOnly => Ok(false),
            CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Err(CacheError::Closed),
        }
    }

    fn enter_miss_only(&self, failure: BucketFailure) {
        let mut state = lock_mutex(&self.inner.state);
        self.enter_miss_only_locked(&mut state, failure);
    }

    fn enter_miss_only_locked(&self, state: &mut BucketState, failure: BucketFailure) {
        match failure {
            BucketFailure::Io => {
                self.inner
                    .counters
                    .io_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            BucketFailure::Corruption => {
                self.inner
                    .counters
                    .corrupt_buckets
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .counters
                    .corruption_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        if state.status == CacheStatus::Healthy {
            state.status = CacheStatus::MissOnly;
            self.inner.pages.close();
            self.inner
                .counters
                .miss_only_transitions
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn quarantine_corrupt_page(&self) -> Result<()> {
        // A clean Superblock paired with a corrupt page would otherwise be
        // trusted again on every reopen, including its stale owner usage.
        // Persist a dirty epoch first so the next opener cold-clears Bucket.
        let dirty = self.ensure_dirty();
        self.enter_miss_only(BucketFailure::Corruption);
        dirty
    }

    fn mark_owner_dirty_for_autonomous_mutation(&self) -> Result<()> {
        if let Some(owner_dirty) = self.inner.owner_dirty.as_ref() {
            owner_dirty()?;
        }
        Ok(())
    }

    fn now_unix_ms(&self) -> u64 {
        #[cfg(test)]
        {
            let overridden = self.inner.test_now_unix_ms.load(Ordering::Acquire);
            if overridden != 0 {
                return overridden;
            }
        }
        now_unix_ms()
    }

    fn poison(&self) {
        let mut state = lock_mutex(&self.inner.state);
        if state.status == CacheStatus::Healthy {
            state.status = CacheStatus::Poisoned;
            self.inner.pages.close();
        }
    }

    fn bucket_id(&self, hash: u64) -> usize {
        (hash % self.inner.bloom.len() as u64) as usize
    }

    fn bucket_offset(&self, bucket_id: usize) -> Result<u64> {
        DATA_OFFSET
            .checked_add(
                (bucket_id as u64)
                    .checked_mul(self.inner.config.bucket_size as u64)
                    .ok_or(CacheError::CorruptMetadata("bucket offset overflow"))?,
            )
            .ok_or(CacheError::CorruptMetadata("bucket offset overflow"))
    }

    fn acquire_page(&self) -> Result<PageLease> {
        PageLease::acquire(&self.inner.pages, self.inner.config.bucket_size)
            .ok_or(CacheError::Poisoned)
    }

    fn acquire_page_with_task_context(&self, context: &TaskContext) -> Result<PageLease> {
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_on_stop = Arc::clone(&cancelled);
        let inner = Arc::clone(&self.inner);
        context.set_stop_hook(move |_| {
            cancelled_on_stop.store(true, Ordering::Release);
            inner.pages.wake_waiters();
        });
        let page = PageLease::acquire_controlled(
            &self.inner.pages,
            self.inner.config.bucket_size,
            cancelled.as_ref(),
            context.deadline(),
        )
        .map_err(|error| match error {
            DedicatedBufferAcquireError::Cancelled => context_stop_error(Some(context)),
            DedicatedBufferAcquireError::TimedOut => match context.stop_reason() {
                Some(AsyncFailure::Cancelled) => CacheError::Cancelled,
                _ => CacheError::TimedOut,
            },
            DedicatedBufferAcquireError::Closed => CacheError::Poisoned,
        })?;
        if context.is_stopped() {
            return Err(context_stop_error(Some(context)));
        }
        Ok(page)
    }

    #[cfg(test)]
    pub(crate) fn hold_page_for_test(&self) -> Result<BucketPageTestGuard> {
        Ok(BucketPageTestGuard {
            _page: self.acquire_page()?,
        })
    }

    #[cfg(test)]
    pub(crate) fn page_waiters_for_test(&self) -> usize {
        self.inner.pages.waiters_for_test()
    }

    fn read_page(
        &self,
        mut page: PageLease,
        offset: u64,
        context: Option<&TaskContext>,
    ) -> std::io::Result<PageLease> {
        let buffer = match IoBuffer::from_lease(page.take_buffer(), self.inner.config.bucket_size) {
            Ok(buffer) => buffer,
            Err(error) => {
                page.restore_buffer(error.lease);
                return Err(error.error);
            }
        };
        let request = match self.inner.engine.read_exact_at(buffer, offset) {
            Ok(request) => request,
            Err(error) if error.error.kind() == std::io::ErrorKind::WouldBlock => {
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
                        if let Some(lease) = lease {
                            page.restore_buffer(lease);
                        }
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                let (error, lease) = error.into_lease();
                if let Some(lease) = lease {
                    page.restore_buffer(lease);
                }
                return Err(error);
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
        let Some(lease) = lease else {
            return Err(std::io::Error::other(
                "Bucket read completion lost its aligned page buffer",
            ));
        };
        page.restore_buffer(lease);
        if !valid {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bucket read completion identity mismatch",
            ));
        }
        let transferred = result?;
        if transferred != self.inner.config.bucket_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Bucket read completion was short",
            ));
        }
        Ok(page)
    }

    fn write_page(&self, page: PageLease, offset: u64) -> std::io::Result<()> {
        self.write_page_with_kind(page, offset, HostWriteKind::ForegroundRecord)
    }

    fn write_page_with_kind(
        &self,
        mut page: PageLease,
        offset: u64,
        kind: HostWriteKind,
    ) -> std::io::Result<()> {
        let length = self.inner.config.bucket_size;
        let buffer = match IoBuffer::from_lease(page.take_buffer(), length) {
            Ok(buffer) => buffer,
            Err(error) => {
                page.restore_buffer(error.lease);
                return Err(error.error);
            }
        };
        if let Some(host_writes) = self.inner.host_writes.as_deref() {
            host_writes.record_write(kind, length as u64);
        }
        let request = match self
            .inner
            .engine
            .write_all_at(WritePoint::Record, buffer, offset)
        {
            Ok(request) => request,
            Err(error) if error.error.kind() == std::io::ErrorKind::WouldBlock => {
                let (_, operation) = error.into_parts();
                match self.inner.engine.submit_wait(operation) {
                    Ok(request) => request,
                    Err(error) => {
                        let (error, lease) = error.into_lease();
                        if let Some(lease) = lease {
                            page.restore_buffer(lease);
                        }
                        self.record_page_write_failure();
                        return Err(error);
                    }
                }
            }
            Err(error) => {
                let (error, lease) = error.into_lease();
                if let Some(lease) = lease {
                    page.restore_buffer(lease);
                }
                self.record_page_write_failure();
                return Err(error);
            }
        };
        let request_id = request.id();
        let completion = request.wait();
        let valid = completion.request_id == request_id && completion.kind == OperationKind::Write;
        let (result, lease) = completion.into_lease();
        if let Some(lease) = lease {
            page.restore_buffer(lease);
        } else {
            self.record_page_write_failure();
            return Err(std::io::Error::other(
                "Bucket write completion lost its aligned page buffer",
            ));
        }
        if !valid {
            self.record_page_write_failure();
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Bucket write completion identity mismatch",
            ));
        }
        match result {
            Ok(transferred) if transferred == length => Ok(()),
            Ok(_) => {
                self.record_page_write_failure();
                Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "Bucket write completion was short",
                ))
            }
            Err(error) => {
                self.record_page_write_failure();
                Err(error)
            }
        }
    }

    fn record_page_write_failure(&self) {
        if let Some(host_writes) = self.inner.host_writes.as_deref() {
            host_writes.record_write_failure();
        }
    }

    fn is_known(&self, bucket_id: usize) -> bool {
        let word = bucket_id / 64;
        let bit = bucket_id % 64;
        self.inner.known[word].load(Ordering::Acquire) & (1_u64 << bit) != 0
    }

    fn is_invalidated(&self, bucket_id: usize) -> bool {
        let word = bucket_id / 64;
        let bit = bucket_id % 64;
        self.inner.invalidated[word].load(Ordering::Acquire) & (1_u64 << bit) != 0
    }

    fn set_invalidated(&self, bucket_id: usize) {
        let word = bucket_id / 64;
        let bit = bucket_id % 64;
        self.inner.invalidated[word].fetch_or(1_u64 << bit, Ordering::AcqRel);
    }

    fn clear_invalidated(&self, bucket_id: usize) {
        let word = bucket_id / 64;
        let bit = bucket_id % 64;
        self.inner.invalidated[word].fetch_and(!(1_u64 << bit), Ordering::AcqRel);
    }

    fn set_bloom(&self, bucket_id: usize, bloom: u64) {
        self.inner.bloom[bucket_id].store(bloom, Ordering::Release);
        let word = bucket_id / 64;
        let bit = bucket_id % 64;
        self.inner.known[word].fetch_or(1_u64 << bit, Ordering::AcqRel);
    }

    fn record_miss(&self) {
        self.inner.counters.misses.fetch_add(1, Ordering::Relaxed);
    }
}

fn write_all_at_tracked(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    point: WritePoint,
    kind: HostWriteKind,
    bytes: &[u8],
    offset: u64,
) -> std::io::Result<()> {
    if let Some(host_writes) = host_writes {
        host_writes.record_write(kind, bytes.len() as u64);
    }
    match write_all_at(io, point, bytes, offset) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(host_writes) = host_writes {
                host_writes.record_write_failure();
            }
            Err(error)
        }
    }
}

fn build_bucket_io_engine(
    config: &BucketCacheConfig,
    backend: Arc<dyn IoBackend>,
    #[cfg(unix)] runtime_files: Option<crate::io_backend::RuntimeFileSet>,
    #[cfg(not(unix))] _runtime_files: Option<()>,
) -> Result<Arc<dyn IoEngine>> {
    #[cfg(unix)]
    let sync = |files: Option<crate::io_backend::RuntimeFileSet>| {
        let engine = match files {
            Some(files) => BackendIoEngine::new_with_files(files, config.io_queue_depth),
            None => BackendIoEngine::new(Arc::clone(&backend), config.io_queue_depth),
        };
        engine
            .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
            .map_err(CacheError::Io)
    };
    #[cfg(not(unix))]
    let sync = || {
        BackendIoEngine::new(Arc::clone(&backend), config.io_queue_depth)
            .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
            .map_err(CacheError::Io)
    };

    match config.io_engine {
        IoEngineKind::Sync => {
            #[cfg(unix)]
            return sync(runtime_files);
            #[cfg(not(unix))]
            return sync();
        }
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
            #[cfg(unix)]
            return sync(runtime_files);
            #[cfg(not(unix))]
            return sync();
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
                    CacheError::InvalidConfig(
                        "bucket io_uring requires a native file-backed cache".into(),
                    )
                })?;
                return UringIoEngine::new_with_files(files, config.io_queue_depth)
                    .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
                    .map_err(|error| {
                        CacheError::InvalidConfig(format!(
                            "bucket io_uring is unavailable for this cache file: {error}"
                        ))
                    });
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
                let _ = backend;
                Err(CacheError::InvalidConfig(
                    "bucket io_uring support is unavailable on this build target".into(),
                ))
            }
        }
    }
}

fn sync_tracked(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    point: SyncPoint,
    mode: SyncMode,
) -> std::io::Result<()> {
    match io.sync(point, mode) {
        Ok(()) => Ok(()),
        Err(error) => {
            if let Some(host_writes) = host_writes {
                host_writes.record_write_failure();
            }
            Err(error)
        }
    }
}

fn open_or_format(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    config: &BucketCacheConfig,
    plan: &BucketPlan,
) -> Result<(BucketSuperblock, usize, bool, bool)> {
    let len = io.len()?;
    if len == 0 {
        return format_bucket_file(io, host_writes, config, plan)
            .map(|(superblock, slot, empty)| (superblock, slot, empty, true));
    }
    if len < DATA_OFFSET {
        return Err(CacheError::CorruptMetadata(
            "bucket file is shorter than its superblocks",
        ));
    }
    let mut pages = [[0_u8; SUPERBLOCK_SIZE]; SUPERBLOCK_COUNT];
    for (slot, page) in pages.iter_mut().enumerate() {
        read_exact_at(io, page, (slot * SUPERBLOCK_SIZE) as u64)?;
    }
    let mut valid = pages
        .iter()
        .enumerate()
        .filter_map(|(slot, page)| BucketSuperblock::decode(page).map(|sb| (sb, slot)))
        .collect::<Vec<_>>();
    if valid.is_empty() {
        if pages.iter().flatten().all(|byte| *byte == 0) {
            return format_bucket_file(io, host_writes, config, plan)
                .map(|(superblock, slot, empty)| (superblock, slot, empty, false));
        }
        return Err(CacheError::CorruptMetadata(
            "bucket superblocks are not recognized",
        ));
    }
    valid.sort_unstable_by_key(|(superblock, _)| superblock.generation);
    let (superblock, active_slot) = valid.pop().expect("checked non-empty superblocks");
    if superblock.bucket_size as usize != config.bucket_size
        || superblock.bucket_count != plan.bucket_count as u64
        || superblock.hash_seed != config.hash_seed
        || len != plan.file_len
    {
        return Err(CacheError::InvalidConfig(
            "bucket layout, capacity, hash seed, or file length does not match".into(),
        ));
    }
    if !superblock.clean {
        // Bucket pages are updated in place. A dirty shutdown therefore
        // cannot distinguish a complete page write from storage that reverted
        // to an older, still checksummed image. Treat the complete small-object
        // tier as disposable and advance its epoch before serving traffic.
        // This deliberately trades warm recovery for the cache invariant that
        // restart returns either the latest verified value or a miss.
        let recovered =
            BucketSuperblock {
                generation: superblock.generation.checked_add(1).ok_or(
                    CacheError::CorruptMetadata("bucket superblock generation exhausted"),
                )?,
                epoch: superblock
                    .epoch
                    .checked_add(1)
                    .ok_or(CacheError::CorruptMetadata("bucket epoch exhausted"))?,
                clean: true,
                ..superblock
            };
        let recovered_slot = 1 - active_slot;
        write_all_at_tracked(
            io,
            host_writes,
            WritePoint::Superblock,
            HostWriteKind::Metadata,
            &recovered.encode(),
            (recovered_slot * SUPERBLOCK_SIZE) as u64,
        )?;
        sync_tracked(io, host_writes, SyncPoint::CheckpointClean, SyncMode::Data)?;
        return Ok((recovered, recovered_slot, false, false));
    }
    Ok((superblock, active_slot, false, true))
}

fn format_bucket_file(
    io: &dyn IoBackend,
    host_writes: Option<&HostWriteTracker>,
    config: &BucketCacheConfig,
    plan: &BucketPlan,
) -> Result<(BucketSuperblock, usize, bool)> {
    // Formatting may be reached for an all-zero, pre-sized file. Truncate it
    // first so stale bucket pages can never become visible under the new
    // superblock if the configured capacity changed.
    io.set_len(0)?;
    sync_tracked(io, host_writes, SyncPoint::FormatTruncate, SyncMode::All)?;
    io.preallocate(plan.file_len)?;
    let superblock = BucketSuperblock {
        generation: 1,
        bucket_size: config.bucket_size as u32,
        bucket_count: plan.bucket_count as u64,
        hash_seed: config.hash_seed,
        epoch: 1,
        clean: true,
    };
    let encoded = superblock.encode();
    write_all_at_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        0,
    )?;
    write_all_at_tracked(
        io,
        host_writes,
        WritePoint::Superblock,
        HostWriteKind::Metadata,
        &encoded,
        SUPERBLOCK_SIZE as u64,
    )?;
    sync_tracked(io, host_writes, SyncPoint::FormatClean, SyncMode::All)?;
    Ok((superblock, 1, true))
}

fn ensure_state_healthy(state: &BucketState) -> Result<()> {
    match state.status {
        CacheStatus::Healthy => Ok(()),
        CacheStatus::Closed => Err(CacheError::Closed),
        CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
    }
}

fn map_lock_error(error: std::io::Error) -> CacheError {
    if error.kind() == std::io::ErrorKind::WouldBlock
        || error
            .raw_os_error()
            .is_some_and(|code| code == 11 || code == 35)
    {
        CacheError::Locked
    } else {
        CacheError::Io(error)
    }
}

fn allocate_atomics(length: usize, what: &str) -> Result<Vec<AtomicU64>> {
    let mut values = Vec::new();
    values.try_reserve_exact(length).map_err(|_| {
        CacheError::InvalidConfig(format!("unable to allocate {what} for {length} elements"))
    })?;
    values.resize_with(length, || AtomicU64::new(0));
    Ok(values)
}

fn allocate_locks(length: usize) -> Result<Vec<Mutex<()>>> {
    let mut locks = Vec::new();
    locks.try_reserve_exact(length).map_err(|_| {
        CacheError::InvalidConfig(format!("unable to allocate {length} bucket locks"))
    })?;
    locks.resize_with(length, || Mutex::new(()));
    Ok(locks)
}

#[derive(Clone)]
struct OwnedEntry {
    hash: u64,
    namespace: NamespaceId,
    key: Vec<u8>,
    value: Vec<u8>,
    expires_at: u64,
}

impl OwnedEntry {
    fn encoded_len(&self) -> usize {
        encoded_entry_len(self.key.len(), self.value.len()).unwrap_or(usize::MAX)
    }

    fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && self.expires_at <= now
    }

    fn usage(&self) -> BucketEntryUsage {
        BucketEntryUsage {
            namespace: self.namespace,
            // Bucket size is capped at 64 KiB, so every validated entry charge
            // is exactly representable by the policy's u64 counter.
            live_bytes: self.encoded_len() as u64,
        }
    }
}

fn prepare_usage_receipt(
    removed: &mut Option<&mut Vec<BucketEntryUsage>>,
    maximum_entries: usize,
) -> Result<()> {
    if let Some(removed) = removed.as_deref_mut() {
        removed.try_reserve_exact(maximum_entries).map_err(|_| {
            CacheError::Overloaded(crate::resources::OverloadReason::WriteBufferUnavailable)
        })?;
    }
    Ok(())
}

fn record_removed_usage(removed: &mut Option<&mut Vec<BucketEntryUsage>>, entry: &OwnedEntry) {
    if let Some(removed) = removed.as_deref_mut() {
        // Capacity was reserved from the decoded entry count before any
        // mutation. Every reported usage consumes one of those page entries.
        debug_assert!(removed.len() < removed.capacity());
        removed.push(entry.usage());
    }
}

struct BucketContents {
    generation: u64,
    epoch: u64,
    entries: Vec<OwnedEntry>,
}

impl BucketContents {
    fn empty(epoch: u64) -> Self {
        Self {
            generation: 0,
            epoch,
            entries: Vec::new(),
        }
    }

    fn bloom(&self, now: u64) -> u64 {
        self.entries
            .iter()
            .filter(|entry| !entry.is_expired(now))
            .fold(0, |bloom, entry| bloom | bloom_mask(entry.hash))
    }

    fn physical_bloom(&self) -> u64 {
        self.entries
            .iter()
            .fold(0, |bloom, entry| bloom | bloom_mask(entry.hash))
    }
}

enum BucketDecode {
    Empty,
    Valid(BucketContents),
    Corrupt,
    AllocationFailed,
}

#[derive(Clone, Copy)]
struct BucketEntryView<'a> {
    hash: u64,
    namespace: NamespaceId,
    key: &'a [u8],
    value: &'a [u8],
    expires_at: u64,
    entry_len: usize,
    value_start: usize,
    value_end: usize,
}

impl BucketEntryView<'_> {
    fn is_expired(&self, now: u64) -> bool {
        self.expires_at != 0 && self.expires_at <= now
    }
}

struct BucketPageView<'a> {
    page: &'a [u8],
    generation: u64,
    epoch: u64,
    entry_count: usize,
    used: usize,
}

struct BucketGetScan {
    entry_count: usize,
    bloom: u64,
    found_expired: bool,
    matching_value: Option<(usize, usize)>,
}

impl BucketGetScan {
    fn empty() -> Self {
        Self {
            entry_count: 0,
            bloom: 0,
            found_expired: false,
            matching_value: None,
        }
    }
}

impl BucketPageView<'_> {
    fn scan_for_get(
        &self,
        hash: u64,
        namespace: NamespaceId,
        key: &[u8],
        now: u64,
    ) -> Option<BucketGetScan> {
        let mut cursor = BUCKET_HEADER_SIZE;
        let mut bloom = 0;
        let mut found_expired = false;
        let mut matching_value = None;
        for _ in 0..self.entry_count {
            let entry = parse_bucket_entry(self.page, cursor, self.used)?;
            if entry.is_expired(now) {
                found_expired = true;
            } else {
                bloom |= bloom_mask(entry.hash);
                // Entries are stored oldest first. Retaining the last matching
                // range preserves the existing newest-entry-wins behavior
                // without allocating for non-matching keys or values.
                if entry.hash == hash && entry.namespace == namespace && entry.key == key {
                    matching_value = Some((entry.value_start, entry.value_end));
                }
            }
            cursor = cursor.checked_add(entry.entry_len)?;
        }
        (cursor == self.used).then_some(BucketGetScan {
            entry_count: self.entry_count,
            bloom,
            found_expired,
            matching_value,
        })
    }
}

enum BucketViewDecode<'a> {
    Empty,
    Valid(BucketPageView<'a>),
    Corrupt,
}

fn decode_bucket_view(page: &[u8], expected_epoch: u64) -> BucketViewDecode<'_> {
    if page.iter().all(|byte| *byte == 0) {
        return BucketViewDecode::Empty;
    }
    if page.len() < BUCKET_HEADER_SIZE + size_of::<u32>()
        || page.get(..8) != Some(BUCKET_MAGIC.as_slice())
        || get_u16(page, BUCKET_VERSION_OFFSET) != Some(FORMAT_VERSION)
        || !trailing_checksum_matches(page)
    {
        return BucketViewDecode::Corrupt;
    }
    let Some(epoch) = get_u64(page, BUCKET_EPOCH_OFFSET) else {
        return BucketViewDecode::Corrupt;
    };
    if epoch != expected_epoch {
        return BucketViewDecode::Empty;
    }
    let Some(generation) = get_u64(page, BUCKET_GENERATION_OFFSET) else {
        return BucketViewDecode::Corrupt;
    };
    let Some(entry_count) = get_u32(page, BUCKET_ENTRY_COUNT_OFFSET).map(|count| count as usize)
    else {
        return BucketViewDecode::Corrupt;
    };
    let Some(used) = get_u32(page, BUCKET_USED_OFFSET).map(|used| used as usize) else {
        return BucketViewDecode::Corrupt;
    };
    if used < BUCKET_HEADER_SIZE || used > page.len() - size_of::<u32>() {
        return BucketViewDecode::Corrupt;
    }
    let maximum_physical_entries = (used - BUCKET_HEADER_SIZE) / ENTRY_HEADER_SIZE;
    if entry_count > maximum_physical_entries {
        return BucketViewDecode::Corrupt;
    }

    let mut cursor = BUCKET_HEADER_SIZE;
    for _ in 0..entry_count {
        let Some(entry) = parse_bucket_entry(page, cursor, used) else {
            return BucketViewDecode::Corrupt;
        };
        let Some(next) = cursor.checked_add(entry.entry_len) else {
            return BucketViewDecode::Corrupt;
        };
        cursor = next;
    }
    if cursor != used {
        return BucketViewDecode::Corrupt;
    }

    BucketViewDecode::Valid(BucketPageView {
        page,
        generation,
        epoch,
        entry_count,
        used,
    })
}

fn parse_bucket_entry(page: &[u8], cursor: usize, used: usize) -> Option<BucketEntryView<'_>> {
    let hash = get_u64(page, cursor.checked_add(ENTRY_HASH_OFFSET)?)?;
    let namespace = get_u32(page, cursor.checked_add(ENTRY_NAMESPACE_OFFSET)?)?;
    let key_len = get_u16(page, cursor.checked_add(ENTRY_KEY_LEN_OFFSET)?)? as usize;
    if get_u16(page, cursor.checked_add(ENTRY_FLAGS_OFFSET)?)? != 0 {
        return None;
    }
    let value_len = get_u32(page, cursor.checked_add(ENTRY_VALUE_LEN_OFFSET)?)? as usize;
    let entry_len = get_u32(page, cursor.checked_add(ENTRY_LEN_OFFSET)?)? as usize;
    let expires_at = get_u64(page, cursor.checked_add(ENTRY_EXPIRES_AT_OFFSET)?)?;
    if entry_len % ENTRY_ALIGNMENT != 0 || encoded_entry_len(key_len, value_len) != Some(entry_len)
    {
        return None;
    }
    let end = cursor.checked_add(entry_len)?;
    let key_start = cursor.checked_add(ENTRY_HEADER_SIZE)?;
    let value_start = key_start.checked_add(key_len)?;
    let value_end = value_start.checked_add(value_len)?;
    if end > used || value_end > end {
        return None;
    }
    Some(BucketEntryView {
        hash,
        namespace,
        key: page.get(key_start..value_start)?,
        value: page.get(value_start..value_end)?,
        expires_at,
        entry_len,
        value_start,
        value_end,
    })
}

fn decode_bucket(page: &[u8], expected_epoch: u64) -> BucketDecode {
    let view = match decode_bucket_view(page, expected_epoch) {
        BucketViewDecode::Empty => return BucketDecode::Empty,
        BucketViewDecode::Corrupt => return BucketDecode::Corrupt,
        BucketViewDecode::Valid(view) => view,
    };
    let mut entries = Vec::new();
    if entries.try_reserve_exact(view.entry_count).is_err() {
        return BucketDecode::AllocationFailed;
    }
    let mut cursor = BUCKET_HEADER_SIZE;
    for _ in 0..view.entry_count {
        let Some(entry) = parse_bucket_entry(view.page, cursor, view.used) else {
            return BucketDecode::Corrupt;
        };
        let Some(key) = try_copy_bytes(entry.key) else {
            return BucketDecode::AllocationFailed;
        };
        let Some(value) = try_copy_bytes(entry.value) else {
            return BucketDecode::AllocationFailed;
        };
        entries.push(OwnedEntry {
            hash: entry.hash,
            namespace: entry.namespace,
            key,
            value,
            expires_at: entry.expires_at,
        });
        cursor += entry.entry_len;
    }
    BucketDecode::Valid(BucketContents {
        generation: view.generation,
        epoch: view.epoch,
        entries,
    })
}

fn try_copy_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len()).ok()?;
    copy.extend_from_slice(bytes);
    Some(copy)
}

fn encode_bucket(page: &mut [u8], contents: &BucketContents) -> Result<()> {
    let bucket_size = page.len();
    page.fill(0);
    page[..8].copy_from_slice(&BUCKET_MAGIC);
    put_u16(page, BUCKET_VERSION_OFFSET, FORMAT_VERSION);
    put_u64(page, BUCKET_GENERATION_OFFSET, contents.generation);
    put_u64(page, BUCKET_EPOCH_OFFSET, contents.epoch);
    let entry_count = u32::try_from(contents.entries.len())
        .map_err(|_| CacheError::CorruptMetadata("bucket entry count overflow"))?;
    put_u32(page, BUCKET_ENTRY_COUNT_OFFSET, entry_count);
    let mut cursor = BUCKET_HEADER_SIZE;
    for entry in &contents.entries {
        let entry_len = entry.encoded_len();
        let end = cursor
            .checked_add(entry_len)
            .ok_or(CacheError::CorruptMetadata("bucket entry offset overflow"))?;
        if end > bucket_size - size_of::<u32>() {
            return Err(CacheError::CorruptMetadata("bucket contents exceed page"));
        }
        put_u64(page, cursor + ENTRY_HASH_OFFSET, entry.hash);
        put_u32(page, cursor + ENTRY_NAMESPACE_OFFSET, entry.namespace);
        put_u16(
            page,
            cursor + ENTRY_KEY_LEN_OFFSET,
            u16::try_from(entry.key.len())
                .map_err(|_| CacheError::CorruptMetadata("bucket key length overflow"))?,
        );
        put_u16(page, cursor + ENTRY_FLAGS_OFFSET, 0);
        put_u32(
            page,
            cursor + ENTRY_VALUE_LEN_OFFSET,
            u32::try_from(entry.value.len())
                .map_err(|_| CacheError::CorruptMetadata("bucket value length overflow"))?,
        );
        put_u32(
            page,
            cursor + ENTRY_LEN_OFFSET,
            u32::try_from(entry_len)
                .map_err(|_| CacheError::CorruptMetadata("bucket entry length overflow"))?,
        );
        put_u64(page, cursor + ENTRY_EXPIRES_AT_OFFSET, entry.expires_at);
        let key_start = cursor + ENTRY_HEADER_SIZE;
        let value_start = key_start + entry.key.len();
        page[key_start..value_start].copy_from_slice(&entry.key);
        page[value_start..value_start + entry.value.len()].copy_from_slice(&entry.value);
        cursor = end;
    }
    put_u32(
        page,
        BUCKET_USED_OFFSET,
        u32::try_from(cursor)
            .map_err(|_| CacheError::CorruptMetadata("bucket used length overflow"))?,
    );
    let checksum = crc32c(page);
    let checksum_offset = bucket_size - size_of::<u32>();
    put_u32(page, checksum_offset, checksum);
    Ok(())
}

fn encoded_entry_len(key_len: usize, value_len: usize) -> Option<usize> {
    let unaligned = ENTRY_HEADER_SIZE
        .checked_add(key_len)?
        .checked_add(value_len)?;
    unaligned
        .checked_add(ENTRY_ALIGNMENT - 1)
        .map(|length| length / ENTRY_ALIGNMENT * ENTRY_ALIGNMENT)
}

fn bloom_mask(hash: u64) -> u64 {
    let mixed = mix64(hash ^ 0x9e37_79b9_7f4a_7c15);
    (1_u64 << (hash & 63))
        | (1_u64 << ((hash >> 17) & 63))
        | (1_u64 << (mixed & 63))
        | (1_u64 << ((mixed >> 23) & 63))
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn hash_namespaced_key(seed: u64, namespace: NamespaceId, key: &[u8]) -> u64 {
    let mut hash = seed ^ 0xcbf2_9ce4_8422_2325;
    hash_update(&mut hash, &namespace.to_le_bytes());
    hash_update(&mut hash, key);
    mix64(hash)
}

fn hash_update(hash: &mut u64, bytes: &[u8]) {
    for &byte in bytes {
        *hash ^= u64::from(byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
}

fn trailing_checksum_matches(input: &[u8]) -> bool {
    let Some(offset) = input.len().checked_sub(size_of::<u32>()) else {
        return false;
    };
    fixed_checksum_matches(input, offset)
}

fn fixed_checksum_matches(input: &[u8], offset: usize) -> bool {
    let Some(stored) = get_u32(input, offset) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&input[..offset]);
    checksum.update(&[0_u8; size_of::<u32>()]);
    checksum.update(&input[offset + size_of::<u32>()..]);
    checksum.finish() == stored
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn is_context_stop_io_error(context: Option<&TaskContext>, error: &std::io::Error) -> bool {
    context.is_some_and(|context| {
        context.is_stopped()
            || (error.kind() == std::io::ErrorKind::TimedOut
                && context
                    .deadline()
                    .is_some_and(|deadline| deadline <= Instant::now()))
    })
}

fn context_stop_io_error(context: Option<&TaskContext>, error: &std::io::Error) -> CacheError {
    match context.and_then(TaskContext::stop_reason) {
        Some(AsyncFailure::TimedOut) => CacheError::TimedOut,
        Some(AsyncFailure::Cancelled) => CacheError::Cancelled,
        _ if error.kind() == std::io::ErrorKind::TimedOut => CacheError::TimedOut,
        _ => CacheError::Cancelled,
    }
}

fn context_stop_error(context: Option<&TaskContext>) -> CacheError {
    match context.and_then(TaskContext::stop_reason) {
        Some(AsyncFailure::TimedOut) => CacheError::TimedOut,
        _ => CacheError::Cancelled,
    }
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn get_u16(input: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        input.get(offset..offset + 2)?.try_into().ok()?,
    ))
}

fn get_u32(input: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        input.get(offset..offset + 4)?.try_into().ok()?,
    ))
}

fn get_u64(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        input.get(offset..offset + 8)?.try_into().ok()?,
    ))
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent};
    use std::fs;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

    fn entry_usage(namespace: NamespaceId, key_len: usize, value_len: usize) -> BucketEntryUsage {
        BucketEntryUsage {
            namespace,
            live_bytes: encoded_entry_len(key_len, value_len).unwrap() as u64,
        }
    }

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(name: &str) -> Self {
            let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-bucket-{name}-{}-{id}.cache",
                std::process::id()
            )))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn config(path: &Path) -> BucketCacheConfig {
        BucketCacheConfig::new(path, 8 * DEFAULT_BUCKET_SIZE as u64)
            .with_memory_budget(4 * 1024 * 1024)
    }

    fn assert_miss_only_rejects_mutations(cache: &BucketCache) {
        assert!(matches!(
            cache.put(b"blocked", b"value", PutOptions::default()),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(
            cache.remove(b"blocked"),
            Err(CacheError::Poisoned)
        ));
        assert!(matches!(cache.clear(), Err(CacheError::Poisoned)));
        assert!(matches!(cache.flush(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn put_get_remove_clear_and_reopen_preserve_behavior() {
        let path = TestPath::new("behavior");
        let cache = config(&path.0).open().unwrap();
        assert_eq!(
            cache.put(b"key", b"one", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"one".to_vec()));
        assert_eq!(
            cache.put(b"key", b"two", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(cache.get(b"key").unwrap(), Some(b"two".to_vec()));
        cache.flush().unwrap();
        cache.close().unwrap();

        let reopened = config(&path.0).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"two".to_vec()));
        assert_eq!(reopened.remove(b"key").unwrap(), RemoveOutcome::Removed);
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened
            .put(b"other", b"value", PutOptions::default())
            .unwrap();
        reopened.clear().unwrap();
        assert_eq!(reopened.get(b"other").unwrap(), None);
        reopened.close().unwrap();

        let cleared = config(&path.0).open().unwrap();
        assert_eq!(cleared.get(b"key").unwrap(), None);
        assert_eq!(cleared.get(b"other").unwrap(), None);
        cleared.close().unwrap();
    }

    #[test]
    fn ttl_and_fifo_bucket_eviction_are_enforced() {
        let path = TestPath::new("ttl-fifo");
        let cache = BucketCacheConfig::new(&path.0, DEFAULT_BUCKET_SIZE as u64)
            .with_memory_budget(4 * 1024 * 1024)
            .open()
            .unwrap();
        assert_eq!(
            cache
                .put(
                    b"expired",
                    b"value",
                    PutOptions {
                        expires_at_unix_ms: Some(now_unix_ms()),
                    },
                )
                .unwrap(),
            PutOutcome::Rejected(RejectReason::AlreadyExpired)
        );

        let payload = vec![7_u8; 900];
        for index in 0..8_u32 {
            let key = index.to_le_bytes();
            cache.put(key, &payload, PutOptions::default()).unwrap();
        }
        assert!(cache.stats().evictions > 0);
        assert!(cache.get(&0_u32.to_le_bytes()).unwrap().is_none());
        assert!(cache.get(&7_u32.to_le_bytes()).unwrap().is_some());
        cache.close().unwrap();
    }

    #[test]
    fn diagnostics_reject_memory_overcommit_without_opening_path() {
        let path = TestPath::new("budget");
        let result = BucketCacheConfig::new(&path.0, 1024 * 1024 * 1024)
            .with_memory_budget(1024)
            .diagnostics();
        assert!(matches!(result, Err(CacheError::InvalidConfig(_))));
        assert!(!path.0.exists());
    }

    #[test]
    fn io_and_page_pool_limits_are_hard_configuration_bounds() {
        let path = TestPath::new("io-limits");
        assert!(matches!(
            config(&path.0).with_io_queue_depth(0).diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(matches!(
            config(&path.0)
                .with_io_queue_depth(MAX_IO_QUEUE_DEPTH + 1)
                .diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(matches!(
            config(&path.0)
                .with_buffer_slots(MAX_BUFFER_SLOTS + 1)
                .diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!path.0.exists());
    }

    #[test]
    fn sync_and_auto_io_engines_share_bucket_behavior_and_bounded_aligned_pages() {
        for (name, engine) in [
            ("sync-engine", IoEngineKind::Sync),
            ("auto-engine", IoEngineKind::Auto),
        ] {
            let path = TestPath::new(name);
            let cache = config(&path.0)
                .with_buffer_slots(2)
                .with_io_queue_depth(4)
                .with_io_engine(engine)
                .with_io_mode(IoMode::Auto)
                .open()
                .unwrap();

            let page = cache.acquire_page().unwrap();
            assert_eq!(
                page.as_ptr() as usize % crate::resources::BUFFER_ALIGNMENT,
                0
            );
            drop(page);

            cache.put(b"key", b"value", PutOptions::default()).unwrap();
            assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
            assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
            assert_eq!(cache.get(b"key").unwrap(), None);

            let stats = cache.stats();
            assert_eq!(stats.page_buffer_slots, 2);
            assert_eq!(stats.page_buffer_bytes, (2 * DEFAULT_BUCKET_SIZE) as u64);
            assert_eq!(stats.page_buffers_in_use, 0);
            assert!(stats.page_buffers_in_use_peak <= 2);
            assert_eq!(stats.io_queue_capacity, 4);
            assert_eq!(stats.io_submitted, stats.io_completed);
            assert!(stats.io_submitted >= 5);
            if stats.direct_io_active {
                assert!(stats.direct_io_operations > 0);
            } else {
                assert!(stats.buffered_io_operations > 0);
            }
            cache.close().unwrap();
        }
    }

    #[test]
    fn buffered_and_auto_paths_cross_reopen_the_same_bucket_format() {
        let path = TestPath::new("io-mode-compatibility");
        let buffered = config(&path.0)
            .with_io_engine(IoEngineKind::Sync)
            .with_io_mode(IoMode::Buffered)
            .open()
            .unwrap();
        buffered
            .put(b"buffered", b"first", PutOptions::default())
            .unwrap();
        buffered.close().unwrap();

        let automatic = config(&path.0)
            .with_io_engine(IoEngineKind::Auto)
            .with_io_mode(IoMode::Auto)
            .open()
            .unwrap();
        assert_eq!(automatic.get(b"buffered").unwrap(), Some(b"first".to_vec()));
        automatic
            .put(b"automatic", b"second", PutOptions::default())
            .unwrap();
        automatic.close().unwrap();

        let reopened = config(&path.0)
            .with_io_engine(IoEngineKind::Sync)
            .with_io_mode(IoMode::Buffered)
            .open()
            .unwrap();
        assert_eq!(reopened.get(b"buffered").unwrap(), Some(b"first".to_vec()));
        assert_eq!(
            reopened.get(b"automatic").unwrap(),
            Some(b"second".to_vec())
        );
        reopened.close().unwrap();
    }

    #[test]
    fn dirty_reopen_discards_in_place_pages_instead_of_reviving_values() {
        let path = TestPath::new("dirty-reopen");
        let cache = config(&path.0).open().unwrap();
        cache
            .put(b"key", b"unflushed", PutOptions::default())
            .unwrap();
        assert_eq!(cache.get(b"key").unwrap(), Some(b"unflushed".to_vec()));
        drop(cache);

        let reopened = config(&path.0).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn poisoned_close_is_terminal_and_releases_the_file_lock() {
        let path = TestPath::new("poisoned-close");
        let cache = config(&path.0).open().unwrap();
        cache.put(b"key", b"value", PutOptions::default()).unwrap();
        lock_mutex(&cache.inner.state).status = CacheStatus::Poisoned;

        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert_eq!(cache.status(), CacheStatus::Closed);
        let reopened = config(&path.0).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), None);
        reopened.close().unwrap();
    }

    #[test]
    fn clear_epoch_survives_loss_of_either_superblock() {
        for damaged_slot in 0..SUPERBLOCK_COUNT {
            let path = TestPath::new(&format!("clear-slot-{damaged_slot}"));
            let cache = config(&path.0).open().unwrap();
            cache.put(b"key", b"old", PutOptions::default()).unwrap();
            cache.flush().unwrap();
            cache.clear().unwrap();
            drop(cache);

            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path.0)
                .unwrap();
            let crc_offset = (damaged_slot * SUPERBLOCK_SIZE + SUPERBLOCK_CRC_OFFSET) as u64;
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            let mut checksum = [0_u8; size_of::<u32>()];
            file.read_exact(&mut checksum).unwrap();
            checksum[0] ^= 0xff;
            file.seek(SeekFrom::Start(crc_offset)).unwrap();
            file.write_all(&checksum).unwrap();
            file.sync_all().unwrap();
            drop(file);

            let reopened = config(&path.0).open().unwrap();
            assert_eq!(reopened.get(b"key").unwrap(), None);
            reopened.close().unwrap();
        }
    }

    #[test]
    fn hostile_entry_count_is_rejected_before_allocation() {
        let mut page = vec![0_u8; DEFAULT_BUCKET_SIZE];
        encode_bucket(&mut page, &BucketContents::empty(1)).unwrap();
        put_u32(&mut page, BUCKET_ENTRY_COUNT_OFFSET, u32::MAX);
        let checksum_offset = page.len() - size_of::<u32>();
        put_u32(&mut page, checksum_offset, 0);
        let checksum = crc32c(&page);
        put_u32(&mut page, checksum_offset, checksum);
        assert!(matches!(decode_bucket(&page, 1), BucketDecode::Corrupt));
    }

    #[test]
    fn page_view_scan_borrows_entries_and_selects_the_latest_live_match() {
        let hash = hash_namespaced_key(17, 9, b"target");
        let expired_hash = hash_namespaced_key(17, 9, b"expired");
        let contents = BucketContents {
            generation: 3,
            epoch: 7,
            entries: vec![
                OwnedEntry {
                    hash,
                    namespace: 9,
                    key: b"target".to_vec(),
                    value: b"older".to_vec(),
                    expires_at: 0,
                },
                OwnedEntry {
                    hash: expired_hash,
                    namespace: 9,
                    key: b"expired".to_vec(),
                    value: b"dead".to_vec(),
                    expires_at: 10,
                },
                OwnedEntry {
                    hash,
                    namespace: 9,
                    key: b"target".to_vec(),
                    value: b"newest".to_vec(),
                    expires_at: 0,
                },
            ],
        };
        let mut page = vec![0_u8; DEFAULT_BUCKET_SIZE];
        encode_bucket(&mut page, &contents).unwrap();

        let BucketViewDecode::Valid(view) = decode_bucket_view(&page, 7) else {
            panic!("encoded page must produce a valid borrowed view");
        };
        let scan = view.scan_for_get(hash, 9, b"target", 10).unwrap();
        assert_eq!(scan.entry_count, 3);
        assert!(scan.found_expired);
        assert_eq!(scan.bloom, bloom_mask(hash));
        let (start, end) = scan.matching_value.unwrap();
        assert_eq!(&page[start..end], b"newest");
    }

    #[test]
    fn volatile_invalidation_hides_a_bucket_and_next_put_rebuilds_it() {
        let path = TestPath::new("volatile-invalidation");
        let cache = BucketCacheConfig::new(&path.0, DEFAULT_BUCKET_SIZE as u64)
            .with_memory_budget(4 * 1024 * 1024)
            .open()
            .unwrap();
        cache
            .put_in(3, b"old-a", b"value-a", PutOptions::default())
            .unwrap();
        cache
            .put_in(4, b"old-b", b"value-b", PutOptions::default())
            .unwrap();
        let before = cache.stats();

        cache.invalidate_bucket_in_memory(3, b"old-a").unwrap();
        assert!(!cache.may_contain_in(3, b"old-a").unwrap());
        assert_eq!(cache.get_in(3, b"old-a").unwrap(), None);
        assert_eq!(cache.get_in(4, b"old-b").unwrap(), None);
        let invalidated = cache.stats();
        assert_eq!(invalidated.bytes_read, before.bytes_read);
        assert_eq!(invalidated.bytes_written, before.bytes_written);

        cache
            .put_in(5, b"new", b"fresh", PutOptions::default())
            .unwrap();
        let rebuilt = cache.stats();
        assert_eq!(rebuilt.bytes_read, before.bytes_read);
        assert_eq!(
            rebuilt.bytes_written,
            before.bytes_written + DEFAULT_BUCKET_SIZE as u64
        );
        assert_eq!(cache.get_in(5, b"new").unwrap(), Some(b"fresh".to_vec()));
        assert_eq!(cache.get_in(3, b"old-a").unwrap(), None);
        assert_eq!(cache.get_in(4, b"old-b").unwrap(), None);
        cache.close().unwrap();
    }

    #[test]
    fn expired_neighbor_does_not_make_an_absent_remove_report_success() {
        let path = TestPath::new("remove-expired-neighbor");
        let cache = BucketCacheConfig::new(&path.0, DEFAULT_BUCKET_SIZE as u64)
            .with_memory_budget(4 * 1024 * 1024)
            .open()
            .unwrap();
        cache
            .put(
                b"expires",
                b"value",
                PutOptions {
                    expires_at_unix_ms: Some(now_unix_ms() + 20),
                },
            )
            .unwrap();
        cache.put(b"live", b"value", PutOptions::default()).unwrap();
        std::thread::sleep(Duration::from_millis(25));

        assert_eq!(cache.remove(b"absent").unwrap(), RemoveOutcome::NotFound);
        assert_eq!(cache.get(b"live").unwrap(), Some(b"value".to_vec()));
        cache.close().unwrap();
    }

    #[test]
    fn maximum_item_bytes_matches_the_aligned_codec_limit() {
        let path = TestPath::new("maximum-item");
        let diagnostics = config(&path.0).diagnostics().unwrap();
        let cache = config(&path.0).open().unwrap();
        assert_eq!(cache.maximum_item_bytes(), diagnostics.maximum_item_bytes);
        assert!(cache.fits(0, diagnostics.maximum_item_bytes));
        assert!(!cache.fits(0, diagnostics.maximum_item_bytes + 1));
        cache.close().unwrap();
    }

    #[test]
    fn runtime_read_error_enters_miss_only_and_close_releases_the_lock() {
        let path = TestPath::new("read-miss-only");
        let cache = config(&path.0).open().unwrap();
        cache.put(b"key", b"stable", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        cache.close().unwrap();

        let (backend, handle) = FaultBackend::open(&path.0).unwrap();
        let cache = BucketCache::open_with_backend(config(&path.0), Box::new(backend)).unwrap();
        handle.arm(FaultEvent::Read, 1, FaultAction::Error(5));

        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(cache.status(), CacheStatus::MissOnly);
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(
            handle
                .events()
                .iter()
                .filter(|event| **event == FaultEvent::Read)
                .count(),
            1
        );
        let stats = cache.stats();
        assert_eq!(stats.io_errors, 1);
        assert_eq!(stats.corruption_errors, 0);
        assert_eq!(stats.miss_only_transitions, 1);
        assert_miss_only_rejects_mutations(&cache);

        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
        assert_eq!(cache.status(), CacheStatus::Closed);
        let reopened = config(&path.0).open().unwrap();
        assert_eq!(reopened.get(b"key").unwrap(), Some(b"stable".to_vec()));
        reopened.close().unwrap();
    }

    #[test]
    fn corrupt_page_enters_miss_only_instead_of_serving_or_rewriting_it() {
        let path = TestPath::new("corrupt-miss-only");
        let cache = config(&path.0).open().unwrap();
        cache.put(b"key", b"value", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        let bucket_id = cache.bucket_id_for(0, b"key");
        cache.close().unwrap();

        let checksum_offset = DATA_OFFSET
            + bucket_id * DEFAULT_BUCKET_SIZE as u64
            + (DEFAULT_BUCKET_SIZE - size_of::<u32>()) as u64;
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path.0)
            .unwrap();
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        let mut checksum = [0_u8; size_of::<u32>()];
        file.read_exact(&mut checksum).unwrap();
        checksum[0] ^= 0xff;
        file.seek(SeekFrom::Start(checksum_offset)).unwrap();
        file.write_all(&checksum).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let cache = config(&path.0).open().unwrap();
        assert_eq!(cache.get(b"key").unwrap(), None);
        assert_eq!(cache.status(), CacheStatus::MissOnly);
        assert_eq!(cache.get(b"other").unwrap(), None);
        let stats = cache.stats();
        assert_eq!(stats.io_errors, 0);
        assert_eq!(stats.corruption_errors, 1);
        assert_eq!(stats.corrupt_buckets, 1);
        assert_eq!(stats.miss_only_transitions, 1);
        assert_miss_only_rejects_mutations(&cache);
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));

        let reopened = config(&path.0).open().unwrap();
        reopened.close().unwrap();
    }

    #[test]
    fn managed_open_tracks_format_and_steady_state_page_writes() {
        let path = TestPath::new("managed-host-writes");
        let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
        let cache = BucketCache::open_managed(config(&path.0), Arc::clone(&shared)).unwrap();
        assert!(
            cache
                .inner
                .host_writes
                .as_ref()
                .is_some_and(|tracker| { Arc::ptr_eq(tracker, &shared) })
        );

        let formatted = shared.snapshot();
        assert_eq!(formatted.host_write_operations, 2);
        assert_eq!(formatted.metadata_bytes, 2 * SUPERBLOCK_SIZE as u64);
        assert_eq!(formatted.foreground_record_bytes, 0);
        assert_eq!(formatted.admitted_value_bytes, 0);

        cache.put(b"key", b"one", PutOptions::default()).unwrap();
        let before = shared.snapshot();
        cache.put(b"key", b"two", PutOptions::default()).unwrap();
        let after = shared.snapshot();
        assert_eq!(
            after.host_write_operations,
            before.host_write_operations + 1
        );
        assert_eq!(
            after.host_write_bytes,
            before.host_write_bytes + DEFAULT_BUCKET_SIZE as u64
        );
        assert_eq!(
            after.foreground_record_bytes,
            before.foreground_record_bytes + DEFAULT_BUCKET_SIZE as u64
        );
        assert_eq!(after.metadata_bytes, before.metadata_bytes);
        assert_eq!(after.failed_write_operations, 0);
        assert_eq!(after.admitted_value_bytes, 0);
        cache.close().unwrap();
    }

    #[test]
    fn managed_receipts_cover_replacement_expiry_fifo_and_remove() {
        let path = TestPath::new("managed-receipts");
        let cache = BucketCacheConfig::new(&path.0, DEFAULT_BUCKET_SIZE as u64)
            .with_memory_budget(4 * 1024 * 1024)
            .open()
            .unwrap();

        // A freshly formatted tier is known empty without reading every data
        // page, so driver admission must not mistake the first put for an
        // existing-key update.
        assert!(!cache.may_contain_in(7, b"same").unwrap());
        let first = cache
            .put_in_managed(7, b"same", b"one", PutOptions::default())
            .unwrap();
        assert_eq!(first.outcome, PutOutcome::Stored);
        assert!(first.removed.is_empty());
        let replacement = cache
            .put_in_managed(7, b"same", b"replacement", PutOptions::default())
            .unwrap();
        assert_eq!(replacement.removed, vec![entry_usage(7, 4, 3)]);

        cache
            .put_in_managed(
                8,
                b"stale-put",
                b"x",
                PutOptions {
                    expires_at_unix_ms: Some(now_unix_ms() + 20),
                },
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));
        let expiry_put = cache
            .put_in_managed(8, b"after-expiry", b"fresh", PutOptions::default())
            .unwrap();
        assert!(expiry_put.removed.contains(&entry_usage(8, 9, 1)));

        cache
            .put_in_managed(
                9,
                b"stale-remove",
                b"old",
                PutOptions {
                    expires_at_unix_ms: Some(now_unix_ms() + 20),
                },
            )
            .unwrap();
        cache
            .put_in_managed(9, b"remove-me", b"live", PutOptions::default())
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));
        let removed = cache.remove_in_managed(9, b"remove-me").unwrap();
        assert_eq!(removed.outcome, RemoveOutcome::Removed);
        assert!(removed.removed.contains(&entry_usage(9, 12, 3)));
        assert!(removed.removed.contains(&entry_usage(9, 9, 4)));

        cache.clear().unwrap();
        let payload = vec![7_u8; 900];
        for index in 0..4_u8 {
            let key = [b'k', b'0' + index];
            let receipt = cache
                .put_in_managed(11, &key, &payload, PutOptions::default())
                .unwrap();
            assert!(receipt.removed.is_empty());
        }
        let fifo = cache
            .put_in_managed(11, b"k4", &payload, PutOptions::default())
            .unwrap();
        assert_eq!(fifo.removed, vec![entry_usage(11, 2, payload.len())]);

        cache.clear().unwrap();
        cache
            .put_in_managed(12, b"probe", b"value", PutOptions::default())
            .unwrap();
        assert!(cache.may_contain_in(12, b"probe").unwrap());
        cache.remove_in_managed(12, b"probe").unwrap();
        assert!(!cache.may_contain_in(12, b"probe").unwrap());
        cache.close().unwrap();
    }

    #[test]
    fn live_scan_is_streaming_counts_physical_expiry_and_degrades_on_read_error() {
        let path = TestPath::new("live-scan");
        let cache = config(&path.0).open().unwrap();
        cache
            .put_in_managed(3, b"a", b"one", PutOptions::default())
            .unwrap();
        cache
            .put_in_managed(4, b"bb", b"two-two", PutOptions::default())
            .unwrap();
        cache
            .put_in_managed(
                5,
                b"expired",
                b"gone",
                PutOptions {
                    expires_at_unix_ms: Some(now_unix_ms() + 20),
                },
            )
            .unwrap();
        std::thread::sleep(Duration::from_millis(25));

        let mut seen = Vec::new();
        cache
            .scan_live_entries(|usage| {
                seen.push(usage);
                Ok(())
            })
            .unwrap();
        seen.sort_unstable_by_key(|usage| usage.namespace);
        assert_eq!(
            seen,
            vec![
                entry_usage(3, 1, 3),
                entry_usage(4, 2, 7),
                entry_usage(5, 7, 4),
            ]
        );
        cache.flush().unwrap();
        cache.close().unwrap();

        let (backend, handle) = FaultBackend::open(&path.0).unwrap();
        let cache = BucketCache::open_with_backend(config(&path.0), Box::new(backend)).unwrap();
        handle.arm(FaultEvent::Read, 1, FaultAction::Error(5));
        assert!(matches!(
            cache.scan_live_entries(|_| Ok(())),
            Err(CacheError::Io(_))
        ));
        assert_eq!(cache.status(), CacheStatus::MissOnly);
        assert!(matches!(cache.close(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn runtime_write_metadata_and_sync_failpoints_enter_miss_only() {
        for (name, event, action, fail_flush, reopen_miss_only) in [
            (
                "page-write",
                FaultEvent::Write(WritePoint::Record),
                FaultAction::Torn {
                    bytes: 512,
                    raw_os_error: 5,
                },
                false,
                true,
            ),
            (
                "metadata-write",
                FaultEvent::Write(WritePoint::Superblock),
                FaultAction::Error(5),
                false,
                false,
            ),
            (
                "dirty-sync",
                FaultEvent::Sync(SyncPoint::DirtyMarker),
                FaultAction::Error(5),
                false,
                false,
            ),
            (
                "flush-sync",
                FaultEvent::Sync(SyncPoint::CheckpointData),
                FaultAction::Error(5),
                true,
                false,
            ),
        ] {
            let path = TestPath::new(name);
            let shared = Arc::new(HostWriteTracker::try_new(None, None).unwrap());
            let (backend, handle) = FaultBackend::open(&path.0).unwrap();
            let cache = BucketCache::open_with_backend_and_host_writes(
                config(&path.0),
                Box::new(backend),
                Some(Arc::clone(&shared)),
            )
            .unwrap();
            if fail_flush {
                cache.put(b"key", b"value", PutOptions::default()).unwrap();
            }
            let host_before = shared.snapshot();
            handle.arm(event, 1, action);

            let result = if fail_flush {
                cache.flush()
            } else {
                cache
                    .put(b"key", b"value", PutOptions::default())
                    .map(|_| ())
            };
            assert!(matches!(result, Err(CacheError::Io(_))), "{name}");
            assert_eq!(cache.status(), CacheStatus::MissOnly, "{name}");
            assert_eq!(cache.get(b"key").unwrap(), None, "{name}");
            let stats = cache.stats();
            assert_eq!(stats.io_errors, 1, "{name}");
            assert_eq!(stats.corruption_errors, 0, "{name}");
            assert_eq!(stats.miss_only_transitions, 1, "{name}");
            let host = shared.snapshot();
            assert_eq!(
                host.failed_write_operations,
                host_before.failed_write_operations + 1,
                "{name}"
            );
            match name {
                "page-write" => {
                    assert_eq!(
                        host.host_write_operations,
                        host_before.host_write_operations + 3
                    );
                    assert_eq!(
                        host.metadata_bytes,
                        host_before.metadata_bytes + 2 * SUPERBLOCK_SIZE as u64
                    );
                    assert_eq!(
                        host.foreground_record_bytes,
                        host_before.foreground_record_bytes + DEFAULT_BUCKET_SIZE as u64
                    );
                }
                "metadata-write" | "dirty-sync" => {
                    assert_eq!(
                        host.host_write_operations,
                        host_before.host_write_operations + 1
                    );
                    assert_eq!(
                        host.metadata_bytes,
                        host_before.metadata_bytes + SUPERBLOCK_SIZE as u64
                    );
                    assert_eq!(
                        host.foreground_record_bytes,
                        host_before.foreground_record_bytes
                    );
                }
                "flush-sync" => {
                    assert_eq!(
                        host.host_write_operations,
                        host_before.host_write_operations
                    );
                    assert_eq!(host.host_write_bytes, host_before.host_write_bytes);
                }
                _ => unreachable!("covered failpoint case"),
            }
            assert_miss_only_rejects_mutations(&cache);
            assert!(matches!(cache.close(), Err(CacheError::Poisoned)));

            let reopened = config(&path.0).open().unwrap();
            assert_eq!(reopened.get(b"key").unwrap(), None, "{name}");
            if reopen_miss_only {
                assert_eq!(reopened.status(), CacheStatus::MissOnly, "{name}");
                assert!(matches!(reopened.close(), Err(CacheError::Poisoned)));
            } else {
                reopened.close().unwrap();
            }
        }
    }
}
