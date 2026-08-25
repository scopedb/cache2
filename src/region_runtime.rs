//! Self-owned steady-state runtime for RegionStore .
//!
//! Foreground writers encode directly into the fixed per-shard staging
//! buffers. Shard workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory or let a benchmark generator inflate the measured
//! device path. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::eviction::EvictionPolicy;
use crate::expiry::ExpiryClock;
use crate::io_backend::{DirectIoMode, RuntimeFileSet, SyncMode, SyncPoint};
use crate::io_engine::{
    FileIoEngineKind, IoEngine, IoEngineStats, IoOperation, MAX_IO_QUEUE_DEPTH, OperationKind,
    build_file_engine, submit_cache_io,
};
use crate::memory::{MemoryLookup, MemoryMetricsSnapshot, MemoryStore, MemoryValue};
use crate::record_codec::{hash_namespaced_key, planned_record_bytes};
use crate::recovery::{DataGeometry, DataSuperblock};
use crate::region::{FileRegionCore, RegionStageValue, RegionStagedValue, RegionValueRead};
use crate::region_appender::_WRITE_BATCH_BYTES;
use crate::region_manager::RegionSetRuntimeSnapshot;
use crate::region_staging::{PendingFenceLookup, RegionStaging, StagingError};
use crate::resources::{
    BackpressurePolicy, CACHE_THREAD_STACK_BYTES, MAX_BACKPRESSURE_TIMEOUT, MAX_QUEUE_DEPTH,
    ManagedMemorySnapshot, OverloadReason, ReadBufferPolicy, ReadBufferTryAcquire,
    ResourceBuildError, ResourceController, ResourceLimits, ResourceRuntimeSnapshot,
};

#[cfg(test)]
const DEFAULT_IO_WORKERS: usize = 4;
#[cfg(test)]
const DEFAULT_IO_QUEUE_DEPTH: usize = 128;
#[cfg(test)]
const DEFAULT_WRITE_QUEUE_DEPTH: usize = 128;
#[cfg(test)]
pub(crate) const DEFAULT_READ_BUFFER_SLOTS: usize = 128;
#[cfg(test)]
pub(crate) const _READ_BUFFER_SLOTS: usize = DEFAULT_READ_BUFFER_SLOTS;
const _MAX_KEY_BYTES: usize = 4 * 1024;
const _MAX_VALUE_BYTES: usize = 256 * 1024;
const _READ_BUFFER_BYTES: usize = 272 * 1024;
pub(crate) const DEFAULT_MEMORY_SHARDS: usize = 32;
#[cfg(test)]
const DEFAULT_MEMORY_BUDGET_BYTES: usize = 1024 * 1024 * 1024;
#[cfg(test)]
const DEFAULT_MEMORY_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
#[cfg(test)]
const DEFAULT_PARTIAL_FLUSH_AGE: Duration = Duration::from_millis(1);
const MAX_PARTIAL_FLUSH_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const _RETRY_AGE: Duration = Duration::from_micros(50);
// Covers the bounded engine registry, command channel, and driver-side
// bookkeeping for one admitted I/O operation. Payload buffers are charged by
// ResourceController separately.
const IO_QUEUE_ENTRY_RESERVATION_BYTES: usize = 512;
// Covers worker/shard controls and handles whose size does not scale with the
// payload or queue depth.
const RUNTIME_CONTROL_RESERVATION_BYTES: usize = 4096;
const LIFECYCLE_RUNNING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FAILED: u8 = 2;

struct LifecycleDrainingGuard<'a> {
    lifecycle: &'a AtomicU8,
    entered: bool,
}

impl<'a> LifecycleDrainingGuard<'a> {
    fn enter(lifecycle: &'a AtomicU8) -> Self {
        let entered = lifecycle
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok();
        Self { lifecycle, entered }
    }
}

impl Drop for LifecycleDrainingGuard<'_> {
    fn drop(&mut self) {
        if self.entered {
            let _ = self.lifecycle.compare_exchange(
                LIFECYCLE_DRAINING,
                LIFECYCLE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RuntimeHealth {
    Running,
    Draining,
    MissOnly,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeSnapshot {
    pub(crate) health: RuntimeHealth,
    pub(crate) stats_enabled: bool,
    pub(crate) puts: u64,
    pub(crate) written_bytes: u64,
    pub(crate) l1_hits: u64,
    pub(crate) l1_misses: u64,
    pub(crate) l2_hits: u64,
    pub(crate) l2_misses: u64,
    pub(crate) served_bytes: u64,
    pub(crate) promotions: u64,
    pub(crate) l1_evictions: u64,
    pub(crate) l1_bypasses: u64,
    pub(crate) l1_admission_rejections: u64,
    pub(crate) queue_saturation: u64,
    pub(crate) buffer_saturation: u64,
    pub(crate) io_failures: u64,
    pub(crate) region_rotations: u64,
    pub(crate) memory: ManagedMemorySnapshot,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RuntimeDetailedSnapshot {
    pub(crate) summary: RuntimeSnapshot,
    pub(crate) resources: ResourceRuntimeSnapshot,
    pub(crate) io: IoEngineStats,
    pub(crate) region_sets: Box<[RegionSetRuntimeSnapshot]>,
    pub(crate) staging_rejections: u64,
    pub(crate) staging_wait_ns: u64,
}

struct RuntimeMetrics {
    lifecycle: AtomicU8,
    activity: Box<[ActivityMetrics]>,
    queue_saturation: AtomicU64,
    buffer_saturation: AtomicU64,
    staging_rejections: AtomicU64,
    staging_wait_ns: AtomicU64,
    io_failures: AtomicU64,
    region_rotations: AtomicU64,
}

#[repr(align(64))]
struct ActivityMetrics {
    puts: AtomicU64,
    written_bytes: AtomicU64,
    l1_hits: AtomicU64,
    l1_misses: AtomicU64,
    l2_hits: AtomicU64,
    l2_misses: AtomicU64,
    served_bytes: AtomicU64,
    promotions: AtomicU64,
}

impl ActivityMetrics {
    fn new() -> Self {
        Self {
            puts: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            l1_hits: AtomicU64::new(0),
            l1_misses: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
            promotions: AtomicU64::new(0),
        }
    }
}

impl RuntimeMetrics {
    fn new(shard_count: usize) -> io::Result<Self> {
        let mut activity = Vec::new();
        activity.try_reserve_exact(shard_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate shard activity counters",
            )
        })?;
        activity.resize_with(shard_count, ActivityMetrics::new);
        Ok(Self {
            lifecycle: AtomicU8::new(LIFECYCLE_RUNNING),
            activity: activity.into_boxed_slice(),
            queue_saturation: AtomicU64::new(0),
            buffer_saturation: AtomicU64::new(0),
            staging_rejections: AtomicU64::new(0),
            staging_wait_ns: AtomicU64::new(0),
            io_failures: AtomicU64::new(0),
            region_rotations: AtomicU64::new(0),
        })
    }

    fn activity(&self, shard_id: usize) -> &ActivityMetrics {
        &self.activity[shard_id]
    }

    fn activity_for_hash(&self, hash: u64) -> &ActivityMetrics {
        self.activity((hash % self.activity.len() as u64) as usize)
    }

    fn add(counter: &AtomicU64, value: usize) {
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        counter.fetch_add(value, Ordering::Relaxed);
    }

    fn increment(counter: &AtomicU64) {
        Self::add(counter, 1);
    }

    fn add_duration(counter: &AtomicU64, started: Option<Instant>) {
        let Some(started) = started else {
            return;
        };
        let elapsed = started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        counter.fetch_add(elapsed, Ordering::Relaxed);
    }

    fn record_overload(&self, reason: OverloadReason) {
        let counter = match reason {
            OverloadReason::WriteQueueFull | OverloadReason::WriteTimeout => &self.queue_saturation,
            OverloadReason::ReadBufferUnavailable
            | OverloadReason::ReadBufferTimeout
            | OverloadReason::WriteStagingUnavailable => &self.buffer_saturation,
        };
        Self::increment(counter);
    }

    fn snapshot(
        &self,
        core_healthy: bool,
        stats_enabled: bool,
        memory: ManagedMemorySnapshot,
        memory_metrics: MemoryMetricsSnapshot,
    ) -> RuntimeSnapshot {
        let lifecycle = self.lifecycle.load(Ordering::Acquire);
        let health = if lifecycle == LIFECYCLE_FAILED {
            RuntimeHealth::Failed
        } else if !core_healthy {
            RuntimeHealth::MissOnly
        } else if lifecycle == LIFECYCLE_DRAINING {
            RuntimeHealth::Draining
        } else {
            RuntimeHealth::Running
        };
        let mut puts = 0_u64;
        let mut written_bytes = 0_u64;
        let mut l1_hits = 0_u64;
        let mut l1_misses = 0_u64;
        let mut l2_hits = 0_u64;
        let mut l2_misses = 0_u64;
        let mut served_bytes = 0_u64;
        let mut promotions = 0_u64;
        for activity in &self.activity {
            puts = puts.saturating_add(activity.puts.load(Ordering::Relaxed));
            written_bytes =
                written_bytes.saturating_add(activity.written_bytes.load(Ordering::Relaxed));
            l1_hits = l1_hits.saturating_add(activity.l1_hits.load(Ordering::Relaxed));
            l1_misses = l1_misses.saturating_add(activity.l1_misses.load(Ordering::Relaxed));
            l2_hits = l2_hits.saturating_add(activity.l2_hits.load(Ordering::Relaxed));
            l2_misses = l2_misses.saturating_add(activity.l2_misses.load(Ordering::Relaxed));
            served_bytes =
                served_bytes.saturating_add(activity.served_bytes.load(Ordering::Relaxed));
            promotions = promotions.saturating_add(activity.promotions.load(Ordering::Relaxed));
        }
        RuntimeSnapshot {
            health,
            stats_enabled,
            puts,
            written_bytes,
            l1_hits,
            l1_misses,
            l2_hits,
            l2_misses,
            served_bytes,
            promotions,
            l1_evictions: memory_metrics.evictions,
            l1_bypasses: memory_metrics.bypasses,
            l1_admission_rejections: memory_metrics.admission_rejections,
            queue_saturation: self.queue_saturation.load(Ordering::Relaxed),
            buffer_saturation: self.buffer_saturation.load(Ordering::Relaxed),
            io_failures: self.io_failures.load(Ordering::Relaxed),
            region_rotations: self.region_rotations.load(Ordering::Relaxed),
            memory,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct RegionRuntimeConfig {
    pub(crate) io_engine: FileIoEngineKind,
    pub(crate) io_mode: DirectIoMode,
    pub(crate) io_workers: usize,
    pub(crate) io_queue_depth: usize,
    pub(crate) write_queue_depth: usize,
    pub(crate) read_buffer_slots: usize,
    pub(crate) read_buffer_policy: ReadBufferPolicy,
    pub(crate) memory_capacity_bytes: usize,
    pub(crate) memory_budget_bytes: usize,
    pub(crate) memory_shards: usize,
    pub(crate) eviction_policy: EvictionPolicy,
    pub(crate) staging_bytes: usize,
    pub(crate) batch_target_bytes: usize,
    pub(crate) partial_flush_age: Duration,
    pub(crate) backpressure: BackpressurePolicy,
    pub(crate) stats_enabled: bool,
}

#[cfg(test)]
impl Default for RegionRuntimeConfig {
    fn default() -> Self {
        Self {
            io_engine: FileIoEngineKind::Auto,
            io_mode: DirectIoMode::Auto,
            io_workers: DEFAULT_IO_WORKERS,
            io_queue_depth: DEFAULT_IO_QUEUE_DEPTH,
            write_queue_depth: DEFAULT_WRITE_QUEUE_DEPTH,
            read_buffer_slots: DEFAULT_READ_BUFFER_SLOTS,
            read_buffer_policy: ReadBufferPolicy::Reject,
            memory_capacity_bytes: DEFAULT_MEMORY_CAPACITY_BYTES,
            memory_budget_bytes: DEFAULT_MEMORY_BUDGET_BYTES,
            memory_shards: DEFAULT_MEMORY_SHARDS,
            eviction_policy: EvictionPolicy::Clock,
            staging_bytes: _WRITE_BATCH_BYTES,
            batch_target_bytes: _WRITE_BATCH_BYTES,
            partial_flush_age: DEFAULT_PARTIAL_FLUSH_AGE,
            backpressure: BackpressurePolicy::Reject,
            stats_enabled: false,
        }
    }
}

impl RegionRuntimeConfig {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.io_workers == 0 || self.io_workers > self.io_queue_depth {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O workers must be in 1..=I/O queue depth",
            ));
        }
        if self.io_queue_depth.div_ceil(self.io_workers) > MAX_IO_QUEUE_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O queue depth per worker exceeds 4096",
            ));
        }
        if self.write_queue_depth == 0 || self.read_buffer_slots == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write queue depth and read buffer slots must be non-zero",
            ));
        }
        if self.write_queue_depth > MAX_QUEUE_DEPTH || self.read_buffer_slots > MAX_QUEUE_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write queue or read buffer slots exceed 65536",
            ));
        }
        if self.memory_budget_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory budget must be non-zero",
            ));
        }
        if self.memory_capacity_bytes > self.memory_budget_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RAM capacity must not exceed the aggregate memory budget",
            ));
        }
        if self.memory_shards == 0 || self.memory_shards > MAX_QUEUE_DEPTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory shards must be in 1..=65536",
            ));
        }
        if matches!(
            self.backpressure,
            BackpressurePolicy::Timeout(duration) if duration > MAX_BACKPRESSURE_TIMEOUT
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backpressure timeout must not exceed 24 hours",
            ));
        }
        if matches!(
            self.read_buffer_policy,
            ReadBufferPolicy::Wait(duration)
                if duration.is_zero() || duration > MAX_BACKPRESSURE_TIMEOUT
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read buffer wait must be in 1ns..=24h",
            ));
        }
        if self.partial_flush_age > MAX_PARTIAL_FLUSH_AGE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "partial flush age must not exceed 24 hours",
            ));
        }
        if self.staging_bytes == 0
            || self.staging_bytes > _WRITE_BATCH_BYTES
            || self.staging_bytes % crate::resources::BUFFER_ALIGNMENT != 0
            || self.batch_target_bytes == 0
            || self.batch_target_bytes > self.staging_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "staging and batch sizes must be aligned and within 1..=4 MiB",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_memory_plan(
        &self,
        geometry: DataGeometry,
        index_slots: usize,
        shard_count: usize,
        layout_memory_bytes: usize,
    ) -> io::Result<()> {
        let fixed_bytes =
            crate::region::runtime_fixed_memory_bytes(index_slots, geometry.region_count)?
                .checked_add(layout_memory_bytes)
                .ok_or_else(|| invalid_runtime_config("fixed memory plan overflow"))?;
        self.validated_base_memory_bytes(geometry, shard_count, fixed_bytes)?;
        Ok(())
    }

    fn validated_base_memory_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<usize> {
        let (base_memory, minimum) = self.memory_plan_bytes(geometry, shard_count, fixed_bytes)?;
        if minimum > self.memory_budget_bytes {
            return Err(invalid_runtime_config(
                "memory budget cannot hold the fixed cache memory plan",
            ));
        }
        Ok(base_memory)
    }

    fn memory_plan_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<(usize, usize)> {
        self.validate()?;
        let topology_bytes = runtime_topology_memory_bytes(shard_count, self)
            .ok_or_else(|| invalid_runtime_config("runtime topology memory plan overflow"))?;
        let usable_region = usize::try_from(geometry.region_size)
            .map_err(|_| invalid_runtime_config("Region size does not fit the memory plan"))?;
        let chunk_bytes =
            usable_region.min(self.staging_bytes) & !(crate::resources::BUFFER_ALIGNMENT - 1);
        let staging_bytes = RegionStaging::reservation_bytes(shard_count, chunk_bytes)
            .ok_or_else(|| invalid_runtime_config("staging memory plan overflow"))?;
        let base_memory = fixed_bytes
            .checked_add(self.memory_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(topology_bytes))
            .ok_or_else(|| invalid_runtime_config("base memory plan overflow"))?;
        let prepared_buffers = self
            .read_buffer_slots
            .checked_mul(_READ_BUFFER_BYTES)
            .ok_or_else(|| invalid_runtime_config("prepared buffer memory plan overflow"))?;
        let minimum = base_memory
            .checked_add(staging_bytes)
            .and_then(|bytes| bytes.checked_add(prepared_buffers))
            .ok_or_else(|| invalid_runtime_config("minimum memory plan overflow"))?;
        Ok((base_memory, minimum))
    }
}

const WAKE_DATA: u8 = 1;
const WAKE_URGENT: u8 = 2;
const WAKE_ROTATE: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionPut {
    /// Bytes are resident in the bounded shard staging buffer until the shard
    /// completion worker publishes the corresponding index entry.
    Buffered(RegionStagedValue),
}

pub(crate) enum HybridValueRead {
    Memory(MemoryValue),
    Region(RegionValueRead),
    /// An L2 hit copied into the bounded L1 tier. The public tier remains
    /// Region because that is where this lookup was served, but the transient
    /// read buffer can return to the foreground pool before `get` returns.
    PromotedRegion(MemoryValue),
}

impl HybridValueRead {
    pub(crate) fn value(&self) -> &[u8] {
        match self {
            Self::Memory(value) | Self::PromotedRegion(value) => value.as_ref(),
            Self::Region(value) => value.value(),
        }
    }

    pub(crate) const fn is_memory(&self) -> bool {
        matches!(self, Self::Memory(_))
    }
}

pub(crate) struct RegionDataPlane {
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    config: RegionRuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
    // Published once after lazy startup; steady-state operations borrow it
    // without taking the lifecycle mutex or cloning an Arc.
    running: OnceLock<Arc<RunningShared>>,
    lifecycle: Mutex<DataPlaneLifecycle>,
    // Fences write admission for drain, flush, and shutdown. Reads do not
    // participate because they cannot extend the set of records being fenced.
    operations: RwLock<()>,
}

enum DataPlaneLifecycle {
    Dormant(Option<RuntimeFileSet>),
    Running(RunningOwner),
    Stopped,
}

struct RunningOwner {
    shared: Arc<RunningShared>,
    shard_workers: Vec<JoinHandle<()>>,
}

struct RunningShared {
    core: Arc<FileRegionCore>,
    engines: Box<[Arc<dyn IoEngine>]>,
    resources: Arc<ResourceController>,
    metrics: Arc<RuntimeMetrics>,
    memory: Arc<MemoryStore>,
    staging: Arc<RegionStaging>,
    shards: Box<[Arc<ShardControl>]>,
    batch_target_bytes: usize,
    partial_flush_age: Duration,
    stats_enabled: bool,
}

impl RunningShared {
    fn engine_for(&self, route: u64) -> &Arc<dyn IoEngine> {
        &self.engines[route as usize % self.engines.len()]
    }
}

#[derive(Clone)]
struct ShardFailure {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl ShardFailure {
    fn from_error(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: Arc::from(error.to_string()),
        }
    }

    fn to_error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.to_string())
    }
}

#[derive(Default)]
struct ShardControlState {
    wake_flags: u8,
    progress: u64,
    drain_requested: u64,
    drain_completed: u64,
    stop: bool,
    failure: Option<ShardFailure>,
}

struct ShardControl {
    state: Mutex<ShardControlState>,
    changed: Condvar,
}

impl ShardControl {
    fn new() -> Self {
        Self {
            state: Mutex::new(ShardControlState::default()),
            changed: Condvar::new(),
        }
    }

    fn progress(&self) -> io::Result<u64> {
        let state = self.lock()?;
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        Ok(state.progress)
    }

    fn notify(&self, flags: u8) -> io::Result<()> {
        let mut state = self.lock()?;
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        if state.stop {
            return Err(closed_runtime_error());
        }
        state.wake_flags |= flags;
        self.changed.notify_one();
        Ok(())
    }

    fn wait_for_progress_until(
        &self,
        observed: u64,
        deadline: Option<Instant>,
    ) -> io::Result<bool> {
        let mut state = self.lock()?;
        while state.progress == observed && state.failure.is_none() && !state.stop {
            state = if let Some(deadline) = deadline {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    return Ok(false);
                };
                let (next, timeout) = self
                    .changed
                    .wait_timeout(state, remaining)
                    .map_err(|_| poisoned_runtime_error())?;
                if timeout.timed_out() && next.progress == observed {
                    return Ok(false);
                }
                next
            } else {
                self.changed
                    .wait(state)
                    .map_err(|_| poisoned_runtime_error())?
            };
        }
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        if state.progress == observed {
            return Err(closed_runtime_error());
        }
        Ok(true)
    }

    fn request_drain(&self, stop: bool) -> io::Result<u64> {
        let mut state = self.lock()?;
        state.drain_requested = state
            .drain_requested
            .checked_add(1)
            .ok_or_else(|| io::Error::other("shard drain generation exhausted"))?;
        state.stop |= stop;
        let generation = state.drain_requested;
        self.changed.notify_one();
        Ok(generation)
    }

    fn wait_for_drain(&self, generation: u64) -> io::Result<()> {
        let mut state = self.lock()?;
        while state.drain_completed < generation && state.failure.is_none() {
            state = self
                .changed
                .wait(state)
                .map_err(|_| poisoned_runtime_error())?;
        }
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        Ok(())
    }

    fn fail(&self, error: &io::Error) {
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state
            .failure
            .get_or_insert_with(|| ShardFailure::from_error(error));
        self.changed.notify_all();
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, ShardControlState>> {
        self.state.lock().map_err(|_| poisoned_runtime_error())
    }
}

impl RegionDataPlane {
    pub(crate) fn new(
        core: Arc<FileRegionCore>,
        data: DataSuperblock,
        files: RuntimeFileSet,
        config: RegionRuntimeConfig,
    ) -> io::Result<Self> {
        let metrics = Arc::new(RuntimeMetrics::new(core.shard_count())?);
        Ok(Self {
            core,
            data,
            config,
            metrics,
            running: OnceLock::new(),
            lifecycle: Mutex::new(DataPlaneLifecycle::Dormant(Some(files))),
            operations: RwLock::new(()),
        })
    }

    pub(crate) fn put(
        &self,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> io::Result<RegionPut> {
        if key.len() > _MAX_KEY_BYTES || value.len() > _MAX_VALUE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file-chunk entry exceeds the 4 KiB key or 256 KiB value limit",
            ));
        }
        let record_bytes = planned_record_bytes(namespace_id, key.len(), value.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let running = self.running()?;
        let hash = hash_namespaced_key(self.data.hash_seed, namespace_id, key);
        let shard_id = self.core.append_shard(namespace_id, hash);
        let control = &running.shards[shard_id];
        let activity = running
            .stats_enabled
            .then(|| running.metrics.activity_for_hash(hash));
        let deadline = match self.config.backpressure {
            BackpressurePolicy::Timeout(duration) => {
                let now = Instant::now();
                Some(now.checked_add(duration).unwrap_or(now))
            }
            BackpressurePolicy::Reject | BackpressurePolicy::Block => None,
        };

        loop {
            // Reject-mode admission is the shard staging transaction itself.
            // The legacy global write gate exists only for callers that
            // explicitly selected a waiting policy.
            let permit = if self.config.backpressure == BackpressurePolicy::Reject {
                None
            } else {
                match running.resources.begin_write_permit_until(deadline) {
                    Ok(permit) => Some(permit),
                    Err(reason) => {
                        if running.stats_enabled {
                            running.metrics.record_overload(reason);
                        }
                        return Err(overload_runtime_error(reason));
                    }
                }
            };
            let operation = match self.operations.try_read() {
                Ok(operation) => operation,
                Err(TryLockError::WouldBlock) => {
                    drop(permit);
                    let reason = OverloadReason::WriteQueueFull;
                    if running.stats_enabled {
                        running.metrics.record_overload(reason);
                    }
                    return Err(overload_runtime_error(reason));
                }
                Err(TryLockError::Poisoned(_)) => return Err(poisoned_runtime_error()),
            };
            match self.core.try_stage_value(
                &running.staging,
                shard_id,
                hash,
                record_bytes,
                namespace_id,
                key,
                value,
                expires_at,
            )? {
                RegionStageValue::Staged(staged) => {
                    let _published = running.memory.publish_pending(
                        shard_id,
                        staged.hash,
                        namespace_id,
                        key,
                        value,
                        expires_at,
                        staged.seqno,
                    );
                    control.notify(WAKE_DATA)?;
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.puts);
                        RuntimeMetrics::add(&activity.written_bytes, value.len());
                    }
                    return Ok(RegionPut::Buffered(staged));
                }
                RegionStageValue::NeedsFlush => {
                    prepare_shard_retry(
                        running,
                        control,
                        WAKE_URGENT,
                        self.config.backpressure,
                        deadline,
                        permit,
                        operation,
                    )?;
                }
                RegionStageValue::NeedsRotation => {
                    prepare_shard_retry(
                        running,
                        control,
                        WAKE_ROTATE | WAKE_URGENT,
                        self.config.backpressure,
                        deadline,
                        permit,
                        operation,
                    )?;
                }
                RegionStageValue::ManagerBusy => {
                    // Region authority contention is unrelated to this shard's
                    // capacity. Waiting on or urgently waking this shard cannot
                    // make it progress, and can force an unrelated partial
                    // batch to flush. Fail fast so the caller can retry.
                    drop(operation);
                    drop(permit);
                    let reason = OverloadReason::WriteStagingUnavailable;
                    if running.stats_enabled {
                        RuntimeMetrics::increment(&running.metrics.staging_rejections);
                        running.metrics.record_overload(reason);
                    }
                    return Err(overload_runtime_error(reason));
                }
                RegionStageValue::Busy => {
                    prepare_shard_retry(
                        running,
                        control,
                        WAKE_URGENT,
                        self.config.backpressure,
                        deadline,
                        permit,
                        operation,
                    )?;
                }
            }
        }
    }

    pub(crate) fn get(
        &self,
        namespace_id: u32,
        key: &[u8],
        clock: ExpiryClock,
    ) -> io::Result<Option<HybridValueRead>> {
        if key.len() > _MAX_KEY_BYTES {
            if self.config.stats_enabled {
                let activity = self.metrics.activity(0);
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(None);
        }
        let running = self.running()?;
        let hash = hash_namespaced_key(self.data.hash_seed, namespace_id, key);
        let activity = running
            .stats_enabled
            .then(|| running.metrics.activity_for_hash(hash));
        if !self.core.is_healthy() {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(None);
        }
        // This health observation is the read's availability linearization
        // point. A later one-way transition to miss-only does not invalidate a
        // value that was already resident here.
        let shard_id = self.core.append_shard(namespace_id, hash);
        let minimum_seqno = match running.staging.pending_fence(shard_id, hash) {
            PendingFenceLookup::Unfenced => None,
            PendingFenceLookup::Fenced(seqno) => Some(seqno),
            PendingFenceLookup::Contended => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l1_misses);
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(None);
            }
        };
        let read_token =
            match running
                .memory
                .lookup_with_fence(hash, namespace_id, key, clock, minimum_seqno)
            {
                MemoryLookup::Hit(value) => {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l1_hits);
                        RuntimeMetrics::add(&activity.served_bytes, value.len());
                    }
                    return Ok(Some(HybridValueRead::Memory(value)));
                }
                MemoryLookup::Hidden => {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l1_misses);
                        RuntimeMetrics::increment(&activity.l2_misses);
                    }
                    return Ok(None);
                }
                MemoryLookup::Miss(token) => {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l1_misses);
                    }
                    token
                }
            };
        let Some(initial_point) = self.core.begin_value_read(hash, namespace_id)? else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(None);
        };
        let (buffer, point) = match self.config.read_buffer_policy {
            ReadBufferPolicy::Reject => match running.resources.try_read_buffer() {
                ReadBufferTryAcquire::Acquired(buffer) => (buffer, initial_point),
                ReadBufferTryAcquire::Exhausted => {
                    running.resources.record_read_buffer_rejection();
                    let reason = OverloadReason::ReadBufferUnavailable;
                    if running.stats_enabled {
                        running.metrics.record_overload(reason);
                    }
                    return Err(overload_runtime_error(reason));
                }
                ReadBufferTryAcquire::Contended => {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l2_misses);
                    }
                    return Ok(None);
                }
            },
            ReadBufferPolicy::Wait(duration) => match running.resources.try_read_buffer() {
                ReadBufferTryAcquire::Acquired(buffer) => (buffer, initial_point),
                ReadBufferTryAcquire::Contended => {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l2_misses);
                    }
                    return Ok(None);
                }
                ReadBufferTryAcquire::Exhausted => {
                    // A Region pin can delay background rotation. Never retain
                    // it while waiting for caller-owned buffer capacity.
                    drop(initial_point);
                    let deadline = Instant::now()
                        .checked_add(duration)
                        .unwrap_or_else(Instant::now);
                    let buffer = match running.resources.wait_read_buffer_until(deadline) {
                        Ok(Some(buffer)) => buffer,
                        Ok(None) => {
                            if let Some(activity) = activity {
                                RuntimeMetrics::increment(&activity.l2_misses);
                            }
                            return Ok(None);
                        }
                        Err(reason) => {
                            if running.stats_enabled {
                                running.metrics.record_overload(reason);
                            }
                            return Err(overload_runtime_error(reason));
                        }
                    };
                    match running.staging.pending_fence(shard_id, hash) {
                        PendingFenceLookup::Unfenced => {}
                        PendingFenceLookup::Fenced(_) | PendingFenceLookup::Contended => {
                            if let Some(activity) = activity {
                                RuntimeMetrics::increment(&activity.l2_misses);
                            }
                            return Ok(None);
                        }
                    }
                    // The candidate may have been replaced, removed, cleared,
                    // or rotated while this request slept. L2 decides again;
                    // there is no retry after this single re-probe.
                    let Some(point) = self.core.begin_value_read(hash, namespace_id)? else {
                        if let Some(activity) = activity {
                            RuntimeMetrics::increment(&activity.l2_misses);
                        }
                        return Ok(None);
                    };
                    (buffer, point)
                }
            },
        };
        let engine = running.engine_for(hash);
        let result = self.core.read_value_from_point(
            engine.as_ref(),
            self.data.geometry,
            buffer,
            point,
            namespace_id,
            key,
            clock,
        );
        match result {
            // MissOnly is a cache availability state, not an application data
            // error. The operation that trips the one-way health latch and all
            // later reads therefore fail open as cache misses. Resource
            // overload remains explicit while the core is still healthy.
            Err(_) if !self.core.is_healthy() => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(None)
            }
            Ok(Some(value)) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_hits);
                    RuntimeMetrics::add(&activity.served_bytes, value.value().len());
                }
                let promoted = running.memory.promote_clean(
                    read_token,
                    hash,
                    namespace_id,
                    key,
                    value.value(),
                    value.expires_at_unix_ms(),
                    value.seqno(),
                );
                if let Some(promoted) = promoted {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.promotions);
                    }
                    return Ok(Some(HybridValueRead::PromotedRegion(promoted)));
                }
                Ok(Some(HybridValueRead::Region(value)))
            }
            Ok(None) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(None)
            }
            Err(error) => {
                if running.stats_enabled {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                Err(error)
            }
        }
    }

    /// Completes and publishes every record admitted before this call. This is
    /// an I/O completion barrier, not an fdatasync durability boundary.
    pub(crate) fn drain(&self) -> io::Result<()> {
        let _exclusive = self
            .operations
            .write()
            .map_err(|_| poisoned_runtime_error())?;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle);
        let running = self.running()?;
        drain_shards(running, false)
    }

    /// Completes admitted spans and syncs the shared data inode once. This
    /// explicit operation does not publish a recovery image.
    pub(crate) fn flush(&self) -> io::Result<()> {
        let result = (|| {
            let _exclusive = self
                .operations
                .write()
                .map_err(|_| poisoned_runtime_error())?;
            let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle);
            let running = self.running()?;
            drain_shards(running, false)?;
            flush_data_inode(&running.engines)?;
            Ok(())
        })();
        if result.is_err() && self.config.stats_enabled {
            RuntimeMetrics::increment(&self.metrics.io_failures);
        }
        result
    }

    pub(crate) fn snapshot(&self) -> io::Result<RuntimeSnapshot> {
        let running = self.running()?;
        Ok(self.snapshot_running(running))
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<RuntimeDetailedSnapshot> {
        let running = self.running()?;
        Ok(RuntimeDetailedSnapshot {
            summary: self.snapshot_running(running),
            resources: running.resources.runtime_snapshot(),
            io: aggregate_io_stats(&running.engines),
            region_sets: self.core.region_set_snapshots()?,
            staging_rejections: running.metrics.staging_rejections.load(Ordering::Relaxed),
            staging_wait_ns: running.metrics.staging_wait_ns.load(Ordering::Relaxed),
        })
    }

    fn snapshot_running(&self, running: &RunningShared) -> RuntimeSnapshot {
        self.metrics.snapshot(
            self.core.is_healthy(),
            self.config.stats_enabled,
            running.resources.managed_memory_snapshot(),
            running.memory.metrics_snapshot(),
        )
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// The return value asks the backend to retain flock for process lifetime
    /// because an issued mutation could not be fenced.
    pub(crate) fn shutdown(self) -> io::Result<bool> {
        let _ = self.metrics.lifecycle.compare_exchange(
            LIFECYCLE_RUNNING,
            LIFECYCLE_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let _exclusive = self
            .operations
            .write()
            .map_err(|_| poisoned_runtime_error())?;
        let owner = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| poisoned_runtime_error())?;
            match std::mem::replace(&mut *lifecycle, DataPlaneLifecycle::Stopped) {
                DataPlaneLifecycle::Dormant(_) | DataPlaneLifecycle::Stopped => {
                    return Ok(false);
                }
                DataPlaneLifecycle::Running(owner) => owner,
            }
        };
        stop_running(owner)
    }

    fn running(&self) -> io::Result<&RunningShared> {
        if let Some(running) = self.running.get() {
            return Ok(running);
        }

        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| poisoned_runtime_error())?;
        if let Some(running) = self.running.get() {
            return Ok(running);
        }
        let files = match &mut *lifecycle {
            DataPlaneLifecycle::Dormant(files) => files.take().ok_or_else(closed_runtime_error)?,
            DataPlaneLifecycle::Stopped => return Err(closed_runtime_error()),
            DataPlaneLifecycle::Running(_) => {
                unreachable!("running owner is published with its shared state")
            }
        };
        let owner = start_running(
            Arc::clone(&self.core),
            self.data,
            files,
            self.config.clone(),
            Arc::clone(&self.metrics),
        )?;
        let shared = Arc::clone(&owner.shared);
        *lifecycle = DataPlaneLifecycle::Running(owner);
        self.running
            .set(shared)
            .map_err(|_| io::Error::other("runtime was initialized twice"))?;
        Ok(self
            .running
            .get()
            .expect("successful runtime publication is immediately visible"))
    }
}

pub(crate) fn flush_data_inode(engines: &[Arc<dyn IoEngine>]) -> io::Result<()> {
    let engine = engines
        .first()
        .ok_or_else(|| io::Error::other("runtime has no I/O engine"))?;
    let request = submit_cache_io(
        engine.as_ref(),
        IoOperation::flush(SyncPoint::ExplicitFlush, SyncMode::Data),
    )
    .map_err(|error| error.error)?;
    let request_id = request.id();
    let completion = request
        .wait(engine.as_ref())
        .map_err(|timeout| timeout.into_buffer().0)?;
    if completion.request_id != request_id
        || completion.kind != OperationKind::Flush
        || completion.bytes_transferred != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "flush completion has the wrong identity",
        ));
    }
    completion.into_io_result().0?;
    Ok(())
}

fn aggregate_io_stats(engines: &[Arc<dyn IoEngine>]) -> IoEngineStats {
    let mut aggregate = IoEngineStats::default();
    for (engine_index, engine) in engines.iter().enumerate() {
        let snapshot = engine.stats();
        aggregate.submitted = aggregate.submitted.saturating_add(snapshot.submitted);
        aggregate.completed = aggregate.completed.saturating_add(snapshot.completed);
        aggregate.cancel_requested = aggregate
            .cancel_requested
            .saturating_add(snapshot.cancel_requested);
        aggregate.cancelled = aggregate.cancelled.saturating_add(snapshot.cancelled);
        aggregate.errors = aggregate.errors.saturating_add(snapshot.errors);
        aggregate.in_flight = aggregate.in_flight.saturating_add(snapshot.in_flight);
        aggregate.in_flight_peak = aggregate
            .in_flight_peak
            .saturating_add(snapshot.in_flight_peak);
        aggregate.submit_wait_ns = aggregate
            .submit_wait_ns
            .saturating_add(snapshot.submit_wait_ns);
        aggregate.completion_ns = aggregate
            .completion_ns
            .saturating_add(snapshot.completion_ns);
        // File-set clones intentionally share one path counter so direct-I/O
        // fallback is globally visible. Read it once rather than multiplying
        // the same totals by the number of workers.
        if engine_index == 0 {
            aggregate.direct_operations = snapshot.direct_operations;
            aggregate.direct_bytes = snapshot.direct_bytes;
            aggregate.buffered_operations = snapshot.buffered_operations;
            aggregate.buffered_bytes = snapshot.buffered_bytes;
        }
    }
    aggregate
}

fn start_running(
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    files: RuntimeFileSet,
    config: RegionRuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
) -> io::Result<RunningOwner> {
    let shard_count = core.shard_count();
    let base_memory = config.validated_base_memory_bytes(
        data.geometry,
        shard_count,
        core.runtime_base_memory_bytes()?,
    )?;
    let memory_budget = config.memory_budget_bytes;
    let resources = Arc::new(
        ResourceController::try_new(ResourceLimits {
            memory_budget_bytes: memory_budget,
            base_memory_bytes: base_memory,
            max_buffer_bytes: _READ_BUFFER_BYTES,
            write_queue_depth: config.write_queue_depth,
            read_buffer_slots: config.read_buffer_slots,
            // HybridCache callers must never hold the shutdown read barrier while
            // waiting for an externally retained hit buffer to return.
            backpressure: config.backpressure,
        })
        .map_err(resource_build_io_error)?,
    );
    let usable_region = usize::try_from(data.geometry.region_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Region size is too large"))?;
    let chunk_bytes =
        usable_region.min(config.staging_bytes) & !(crate::resources::BUFFER_ALIGNMENT - 1);
    let staging = Arc::new(
        RegionStaging::try_new(
            shard_count,
            chunk_bytes,
            data.geometry.region_size,
            &resources,
        )
        .map_err(resource_build_io_error)?,
    );
    let memory = Arc::new(MemoryStore::new(
        config.memory_capacity_bytes,
        config.memory_shards,
        shard_count,
        config.eviction_policy,
        config.stats_enabled,
    )?);
    let engines = build_engine_pool(files, &config)?;
    let mut shards = Vec::new();
    shards.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate shard controls")
    })?;
    shards.resize_with(shard_count, || Arc::new(ShardControl::new()));
    let shared = Arc::new(RunningShared {
        core,
        engines,
        resources,
        metrics,
        memory,
        staging,
        shards: shards.into_boxed_slice(),
        batch_target_bytes: config.batch_target_bytes,
        partial_flush_age: config.partial_flush_age,
        stats_enabled: config.stats_enabled,
    });
    let mut shard_workers = Vec::new();
    shard_workers.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate worker handles")
    })?;
    for shard_id in 0..shard_count {
        let worker_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("cache-rs-shard-{shard_id}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || shard_worker(worker_shared, shard_id))
        {
            Ok(worker) => shard_workers.push(worker),
            Err(error) => {
                for shard in &shared.shards {
                    let _ = shard.request_drain(true);
                }
                for worker in shard_workers {
                    let _ = worker.join();
                }
                shared.staging.close();
                for engine in &shared.engines {
                    let _ = engine.shutdown();
                }
                return Err(error);
            }
        }
    }
    Ok(RunningOwner {
        shared,
        shard_workers,
    })
}

fn runtime_topology_memory_bytes(
    shard_count: usize,
    config: &RegionRuntimeConfig,
) -> Option<usize> {
    // Each I/O engine owns one normal worker. Reserve one additional stack per
    // engine for the bounded shutdown reaper path.
    let stack_count = config.io_workers.checked_mul(2)?.checked_add(shard_count)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let queue = config
        .io_queue_depth
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let controls = config
        .io_workers
        .checked_add(shard_count)?
        .checked_add(config.memory_shards)?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(std::mem::size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn build_engine_pool(
    files: RuntimeFileSet,
    config: &RegionRuntimeConfig,
) -> io::Result<Box<[Arc<dyn IoEngine>]>> {
    let mut source = Some(files);
    let mut engines = Vec::new();
    engines
        .try_reserve_exact(config.io_workers)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate I/O workers"))?;
    let base_depth = config.io_queue_depth / config.io_workers;
    let remainder = config.io_queue_depth % config.io_workers;
    for worker in 0..config.io_workers {
        let worker_files = if worker + 1 == config.io_workers {
            source.take().expect("last I/O worker owns file set")
        } else {
            source.as_ref().expect("I/O file set exists").try_clone()?
        };
        let queue_depth = base_depth + usize::from(worker < remainder);
        engines.push(build_file_engine(
            worker_files,
            queue_depth,
            config.io_engine,
        )?);
    }
    Ok(engines.into_boxed_slice())
}

fn shard_worker(shared: Arc<RunningShared>, shard_id: usize) {
    let control = Arc::clone(&shared.shards[shard_id]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        shard_worker_result(&shared, shard_id, &control)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => io::Error::other("shard worker panicked"),
    };
    if shared.stats_enabled {
        RuntimeMetrics::increment(&shared.metrics.io_failures);
    }
    shared
        .metrics
        .lifecycle
        .store(LIFECYCLE_FAILED, Ordering::Release);
    shared.core.enter_miss_only();
    control.fail(&error);
    // Wake engine admission in case another shard is blocked behind work that
    // can no longer make progress after this runtime entered miss-only.
    for engine in &shared.engines {
        engine.wake_admission_waiters();
    }
    for shard in &shared.shards {
        if !Arc::ptr_eq(shard, &control) {
            shard.fail(&error);
        }
    }
}

fn shard_worker_result(
    shared: &RunningShared,
    shard_id: usize,
    control: &ShardControl,
) -> io::Result<()> {
    let mut deadline = None;
    loop {
        let (flags, drain_generation, stop, timed_out) = wait_for_shard_work(control, deadline)?;
        let draining = drain_generation != 0;
        let force_flush = flags & WAKE_URGENT != 0 || timed_out || draining;
        let rotate = flags & WAKE_ROTATE != 0;

        match shared.staging.shard_fill_snapshot(shard_id) {
            Ok(Some(fill)) => {
                if deadline.is_none() {
                    deadline = Some(
                        Instant::now()
                            .checked_add(shared.partial_flush_age)
                            .ok_or_else(|| {
                                invalid_runtime_config("partial flush deadline overflow")
                            })?,
                    );
                }
                if force_flush || fill.bytes >= shared.batch_target_bytes {
                    let engine = shared.engine_for(shard_id as u64);
                    shared.core.flush_staging_shard(
                        &shared.staging,
                        engine.as_ref(),
                        shard_id,
                        Some(shared.memory.as_ref()),
                    )?;
                    deadline = None;
                    advance_shard_progress(control)?;
                }
            }
            Ok(None) => {
                deadline = None;
                if rotate {
                    let rotated = shared.core.rotate_shard(shard_id)?;
                    if rotated && shared.stats_enabled {
                        RuntimeMetrics::increment(&shared.metrics.region_rotations);
                    }
                    advance_shard_progress(control)?;
                } else if flags & WAKE_DATA != 0 {
                    // A producer may have been followed by an urgent worker
                    // completion before this coalesced wake was observed.
                    advance_shard_progress(control)?;
                }
            }
            Err(StagingError::Encoding | StagingError::Submitted) => {
                deadline = Some(Instant::now() + _RETRY_AGE);
            }
            Err(error) => return Err(staging_runtime_error(error)),
        }

        if draining {
            // Producers are fenced by the owner's write barrier. One forced
            // pass therefore empties the shard completely.
            if shared
                .staging
                .shard_fill_snapshot(shard_id)
                .map_err(staging_runtime_error)?
                .is_some()
            {
                let engine = shared.engine_for(shard_id as u64);
                shared.core.flush_staging_shard(
                    &shared.staging,
                    engine.as_ref(),
                    shard_id,
                    Some(shared.memory.as_ref()),
                )?;
                advance_shard_progress(control)?;
            }
            complete_shard_drain(control, drain_generation)?;
            if stop {
                return Ok(());
            }
        }
    }
}

fn wait_for_shard_work(
    control: &ShardControl,
    deadline: Option<Instant>,
) -> io::Result<(u8, u64, bool, bool)> {
    let mut state = control.lock()?;
    let mut timed_out = false;
    while state.wake_flags == 0 && state.drain_requested == state.drain_completed {
        if let Some(deadline) = deadline {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                timed_out = true;
                break;
            };
            let (next, timeout) = control
                .changed
                .wait_timeout(state, remaining)
                .map_err(|_| poisoned_runtime_error())?;
            state = next;
            if timeout.timed_out()
                && state.wake_flags == 0
                && state.drain_requested == state.drain_completed
            {
                timed_out = true;
                break;
            }
        } else {
            state = control
                .changed
                .wait(state)
                .map_err(|_| poisoned_runtime_error())?;
        }
    }
    if let Some(failure) = &state.failure {
        return Err(failure.to_error());
    }
    let flags = std::mem::take(&mut state.wake_flags);
    let drain_generation = if state.drain_requested > state.drain_completed {
        state.drain_requested
    } else {
        0
    };
    Ok((flags, drain_generation, state.stop, timed_out))
}

fn prepare_shard_retry<Permit, Operation>(
    running: &RunningShared,
    control: &ShardControl,
    flags: u8,
    policy: BackpressurePolicy,
    deadline: Option<Instant>,
    permit: Permit,
    operation: Operation,
) -> io::Result<()> {
    let observed = control.progress()?;
    control.notify(flags)?;
    drop(operation);
    drop(permit);
    let wait_started = running.stats_enabled.then(Instant::now);
    match policy {
        BackpressurePolicy::Reject => {
            let reason = OverloadReason::WriteStagingUnavailable;
            if running.stats_enabled {
                RuntimeMetrics::increment(&running.metrics.staging_rejections);
                running.metrics.record_overload(reason);
            }
            Err(overload_runtime_error(reason))
        }
        BackpressurePolicy::Block => {
            let result = control.wait_for_progress_until(observed, None).map(|_| ());
            RuntimeMetrics::add_duration(&running.metrics.staging_wait_ns, wait_started);
            result
        }
        BackpressurePolicy::Timeout(_) => {
            let progressed = control.wait_for_progress_until(observed, deadline)?;
            RuntimeMetrics::add_duration(&running.metrics.staging_wait_ns, wait_started);
            if progressed {
                Ok(())
            } else {
                let reason = OverloadReason::WriteTimeout;
                if running.stats_enabled {
                    RuntimeMetrics::increment(&running.metrics.staging_rejections);
                    running.metrics.record_overload(reason);
                }
                Err(overload_runtime_error(reason))
            }
        }
    }
}

fn advance_shard_progress(control: &ShardControl) -> io::Result<()> {
    let mut state = control.lock()?;
    state.progress = state.progress.saturating_add(1);
    control.changed.notify_all();
    Ok(())
}

fn complete_shard_drain(control: &ShardControl, generation: u64) -> io::Result<()> {
    let mut state = control.lock()?;
    state.drain_completed = state.drain_completed.max(generation);
    state.progress = state.progress.saturating_add(1);
    control.changed.notify_all();
    Ok(())
}

fn drain_shards(shared: &RunningShared, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(shared.shards.len())
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate drain fence"))?;
    for shard in &shared.shards {
        generations.push(shard.request_drain(stop)?);
    }
    let mut first_error = None;
    for (shard, generation) in shared.shards.iter().zip(generations) {
        if let Err(error) = shard.wait_for_drain(generation) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn stop_running(mut owner: RunningOwner) -> io::Result<bool> {
    let drain = drain_shards(&owner.shared, true);
    let mut join_error = None;
    for worker in owner.shard_workers.drain(..) {
        if worker.join().is_err() {
            join_error.get_or_insert_with(|| io::Error::other("shard worker panicked"));
        }
    }
    owner.shared.staging.close();
    let in_flight = owner
        .shared
        .engines
        .iter()
        .map(|engine| engine.in_flight())
        .sum::<usize>();
    let in_flight_mutations = owner
        .shared
        .engines
        .iter()
        .map(|engine| engine.in_flight_mutations())
        .sum::<usize>();
    let unfenced_before = owner
        .shared
        .engines
        .iter()
        .any(|engine| engine.has_unfenced_mutations());
    // A request that missed its cancellation grace may still own a kernel
    // target and buffer. Joining that engine can wait forever. Retain only the
    // engine Arc; the runtime/core can still be released normally.
    let skip_shutdown = in_flight != 0 || unfenced_before;
    let shutdown = if skip_shutdown {
        Ok(())
    } else {
        let mut result = Ok(());
        for engine in &owner.shared.engines {
            if let Err(error) = engine.shutdown() {
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    };
    let unfenced = unfenced_before
        || owner
            .shared
            .engines
            .iter()
            .any(|engine| engine.has_unfenced_mutations());
    let result = drain
        .and_then(|()| join_error.map_or(Ok(()), Err))
        .and(shutdown);
    if skip_shutdown || unfenced {
        // A merely pending target gets a detached reaper: close returns now,
        // while eventual target completion still shuts the engine down and
        // reclaims its fd/thread/buffer set. A sticky fatal unfenced mutation
        // has no trustworthy future fence and remains process-lifetime state.
        if unfenced {
            for engine in &owner.shared.engines {
                std::mem::forget(Arc::clone(engine));
            }
        } else {
            for engine in &owner.shared.engines {
                if engine.in_flight() != 0 {
                    reap_engine_after_target_fence(engine);
                } else {
                    let _ = engine.shutdown();
                }
            }
        }
        let retain_lock = in_flight_mutations != 0 || unfenced;
        return result.map(|()| retain_lock).or_else(|error| {
            let _ = error;
            Ok(retain_lock)
        });
    }
    result.map(|()| false)
}

fn reap_engine_after_target_fence(engine: &Arc<dyn IoEngine>) {
    let reaper_engine = Arc::clone(engine);
    let spawn = std::thread::Builder::new()
        .name("cache-rs-io-reaper".to_owned())
        .stack_size(CACHE_THREAD_STACK_BYTES)
        .spawn(move || {
            let _ = reaper_engine.shutdown();
        });
    if spawn.is_err() {
        // The original owner is still alive while this fallback clone is
        // created, so a failed thread spawn cannot synchronously run the
        // engine's blocking Drop path.
        std::mem::forget(Arc::clone(engine));
    }
}

fn resource_build_io_error(error: ResourceBuildError) -> io::Error {
    let kind = match error {
        ResourceBuildError::Invalid(_) => io::ErrorKind::InvalidInput,
        ResourceBuildError::Allocation => io::ErrorKind::OutOfMemory,
    };
    io::Error::new(kind, error.to_string())
}

fn overload_runtime_error(error: crate::resources::OverloadReason) -> io::Error {
    let kind = if error == OverloadReason::ReadBufferTimeout {
        io::ErrorKind::TimedOut
    } else {
        io::ErrorKind::WouldBlock
    };
    io::Error::new(kind, error.to_string())
}

fn staging_runtime_error(error: StagingError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn poisoned_runtime_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "runtime synchronization is poisoned",
    )
}

fn closed_runtime_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "data plane is closed")
}

fn invalid_runtime_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preopen_memory_plan_charges_region_layout() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let mut config = RegionRuntimeConfig::default();
        let fixed = crate::region::runtime_fixed_memory_bytes(4096, geometry.region_count).unwrap();
        let (_, without_layout) = config.memory_plan_bytes(geometry, 4, fixed).unwrap();
        config.memory_budget_bytes = without_layout;

        config.validate_memory_plan(geometry, 4096, 4, 0).unwrap();
        let error = config
            .validate_memory_plan(geometry, 4096, 4, 4096)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
