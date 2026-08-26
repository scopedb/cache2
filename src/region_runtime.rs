//! Self-owned steady-state runtime for RegionStore .
//!
//! Foreground writers encode directly into the fixed per-shard write
//! buffers. Shard workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory or let a benchmark generator inflate the measured
//! device path. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::expiry::ExpiryClock;
use crate::format::{RECORD_ALIGNMENT, RECORD_HEADER_SIZE};
use crate::io_backend::{RuntimeFileSet, SyncMode, SyncPoint};
use crate::io_engine::{
    IoEngine, IoOperation, MAX_IO_REQUESTS_PER_WORKER, OperationKind, build_file_engine,
    submit_cache_io,
};
use crate::memory::{MemoryLookup, MemoryMetricsSnapshot, MemoryStore, MemoryValue};
use crate::record_codec::{hash_namespaced_key, required_record_bytes};
use crate::recovery::{DataGeometry, DataSuperblock};
use crate::region::{FileRegionCore, RegionStageValue, RegionValueRead};
use crate::region_reader::{_READ_ALIGNMENT, plan_read};
use crate::region_staging::{PendingFenceLookup, RegionStaging, StagingError};
use crate::resources::{
    CACHE_THREAD_STACK_BYTES, MAX_BACKPRESSURE_TIMEOUT, MAX_CONFIG_COUNT, ManagedMemorySnapshot,
    ResourceBuildError, ResourceController, ResourceLimits, WriteBackpressure, WriteOverloadReason,
};
use crate::runtime_config::{MAX_WRITE_BATCH_BYTES, RuntimeConfig};
use crate::snapshot::{CacheHealth, CacheIoSnapshot, CacheSnapshot, DetailedCacheSnapshot};

const _MAX_KEY_BYTES: usize = 4 * 1024;
const _MAX_VALUE_BYTES: usize = 256 * 1024;
const MAX_RUNTIME_RECORD_BYTES: usize = const_align_up(
    RECORD_HEADER_SIZE + size_of::<u32>() + _MAX_KEY_BYTES + _MAX_VALUE_BYTES,
    RECORD_ALIGNMENT,
);
// A runtime record begins on a RECORD_ALIGNMENT boundary. Tail padding, when
// present, moves the same record end to the next read boundary, so the largest
// exact aligned read is the maximum record plus the largest possible start
// skew, rounded once to the device-read alignment.
const MAX_READ_BUFFER_BYTES: usize = const_align_up(
    MAX_RUNTIME_RECORD_BYTES + (_READ_ALIGNMENT - RECORD_ALIGNMENT),
    _READ_ALIGNMENT,
);
const MAX_WRITE_FLUSH_DELAY: Duration = Duration::from_secs(24 * 60 * 60);
const _RETRY_AGE: Duration = Duration::from_micros(50);
// Covers the bounded engine registry, command channel, and driver-side
// bookkeeping for one admitted I/O operation. Payload buffers are charged by
// ResourceController separately.
const IO_QUEUE_ENTRY_RESERVATION_BYTES: usize = 512;
// Covers worker/shard controls and handles whose size does not scale with the
// payload or configured concurrency.
const RUNTIME_CONTROL_RESERVATION_BYTES: usize = 4096;
const LIFECYCLE_RUNNING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FAILED: u8 = 2;

const fn const_align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

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

struct RuntimeMetrics {
    lifecycle: AtomicU8,
    activity: Box<[ActivityMetrics]>,
    write_rejections: AtomicU64,
    write_buffer_rejections: AtomicU64,
    write_buffer_wait_ns: AtomicU64,
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
    l2_read_memory_misses: AtomicU64,
    l2_read_busy_misses: AtomicU64,
    served_bytes: AtomicU64,
    l1_promotions: AtomicU64,
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
            l2_read_memory_misses: AtomicU64::new(0),
            l2_read_busy_misses: AtomicU64::new(0),
            served_bytes: AtomicU64::new(0),
            l1_promotions: AtomicU64::new(0),
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
            write_rejections: AtomicU64::new(0),
            write_buffer_rejections: AtomicU64::new(0),
            write_buffer_wait_ns: AtomicU64::new(0),
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

    fn record_write_rejection(&self) {
        Self::increment(&self.write_rejections);
    }

    fn snapshot(
        &self,
        core_healthy: bool,
        statistics_enabled: bool,
        memory: ManagedMemorySnapshot,
        memory_metrics: MemoryMetricsSnapshot,
    ) -> CacheSnapshot {
        let lifecycle = self.lifecycle.load(Ordering::Acquire);
        let health = if lifecycle == LIFECYCLE_FAILED {
            CacheHealth::Failed
        } else if !core_healthy {
            CacheHealth::MissOnly
        } else if lifecycle == LIFECYCLE_DRAINING {
            CacheHealth::Draining
        } else {
            CacheHealth::Running
        };
        let mut puts = 0_u64;
        let mut written_bytes = 0_u64;
        let mut l1_hits = 0_u64;
        let mut l1_misses = 0_u64;
        let mut l2_hits = 0_u64;
        let mut l2_misses = 0_u64;
        let mut l2_read_memory_misses = 0_u64;
        let mut l2_read_busy_misses = 0_u64;
        let mut served_bytes = 0_u64;
        let mut l1_promotions = 0_u64;
        for activity in &self.activity {
            puts = puts.saturating_add(activity.puts.load(Ordering::Relaxed));
            written_bytes =
                written_bytes.saturating_add(activity.written_bytes.load(Ordering::Relaxed));
            l1_hits = l1_hits.saturating_add(activity.l1_hits.load(Ordering::Relaxed));
            l1_misses = l1_misses.saturating_add(activity.l1_misses.load(Ordering::Relaxed));
            l2_hits = l2_hits.saturating_add(activity.l2_hits.load(Ordering::Relaxed));
            l2_misses = l2_misses.saturating_add(activity.l2_misses.load(Ordering::Relaxed));
            l2_read_memory_misses = l2_read_memory_misses
                .saturating_add(activity.l2_read_memory_misses.load(Ordering::Relaxed));
            l2_read_busy_misses = l2_read_busy_misses
                .saturating_add(activity.l2_read_busy_misses.load(Ordering::Relaxed));
            served_bytes =
                served_bytes.saturating_add(activity.served_bytes.load(Ordering::Relaxed));
            l1_promotions =
                l1_promotions.saturating_add(activity.l1_promotions.load(Ordering::Relaxed));
        }
        CacheSnapshot {
            health,
            statistics_enabled,
            puts,
            written_bytes,
            l1_hits,
            l1_misses,
            l2_hits,
            l2_misses,
            l2_read_memory_misses,
            l2_read_busy_misses,
            served_bytes,
            l1_promotions,
            l1_evictions: memory_metrics.evictions,
            l1_bypasses: memory_metrics.bypasses,
            l1_admission_rejections: memory_metrics.admission_rejections,
            write_rejections: self.write_rejections.load(Ordering::Relaxed),
            io_failures: self.io_failures.load(Ordering::Relaxed),
            region_rotations: self.region_rotations.load(Ordering::Relaxed),
            managed_memory_bytes: memory.current_bytes,
            managed_memory_peak_bytes: memory.peak_bytes,
            managed_memory_limit_bytes: memory.limit_bytes,
            logical_disk_peak_bytes: 0,
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.io_workers == 0 || self.io_workers > self.io_concurrency {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O workers must be in 1..=I/O concurrency",
            ));
        }
        if self.io_concurrency.div_ceil(self.io_workers) > MAX_IO_REQUESTS_PER_WORKER {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O concurrency per worker exceeds 4096",
            ));
        }
        if self.waiting_write_limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "waiting write limit must be non-zero",
            ));
        }
        if self.waiting_write_limit > MAX_CONFIG_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "waiting write limit exceeds 65536",
            ));
        }
        if self.memory_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory limit must be non-zero",
            ));
        }
        if self.l1_capacity_bytes > self.memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 capacity must not exceed the aggregate memory limit",
            ));
        }
        if self.l1_shards == 0 || self.l1_shards > MAX_CONFIG_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 shards must be in 1..=65536",
            ));
        }
        if matches!(
            self.write_backpressure,
            WriteBackpressure::Timeout(duration) if duration > MAX_BACKPRESSURE_TIMEOUT
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backpressure timeout must not exceed 24 hours",
            ));
        }
        if self.write_flush_delay > MAX_WRITE_FLUSH_DELAY {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write flush delay must not exceed 24 hours",
            ));
        }
        if self.write_buffer_bytes == 0
            || self.write_buffer_bytes > MAX_WRITE_BATCH_BYTES
            || self.write_buffer_bytes % crate::resources::BUFFER_ALIGNMENT != 0
            || self.write_batch_bytes == 0
            || self.write_batch_bytes > self.write_buffer_bytes
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write buffer and batch sizes must be aligned and within 1..=4 MiB",
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
        self.validated_reserved_memory_bytes(geometry, shard_count, fixed_bytes)?;
        Ok(())
    }

    fn validated_reserved_memory_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<usize> {
        let (reserved_memory, minimum) =
            self.memory_plan_bytes(geometry, shard_count, fixed_bytes)?;
        if minimum > self.memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "memory limit cannot hold the fixed cache memory plan: requires {minimum} bytes, configured {} bytes",
                    self.memory_limit_bytes
                ),
            ));
        }
        Ok(reserved_memory)
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
            usable_region.min(self.write_buffer_bytes) & !(crate::resources::BUFFER_ALIGNMENT - 1);
        let write_buffer_reservation =
            RegionStaging::reservation_bytes(shard_count, chunk_bytes)
                .ok_or_else(|| invalid_runtime_config("write buffer memory plan overflow"))?;
        let reserved_memory = fixed_bytes
            .checked_add(self.l1_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(topology_bytes))
            .ok_or_else(|| invalid_runtime_config("reserved memory plan overflow"))?;
        let minimum = reserved_memory
            .checked_add(write_buffer_reservation)
            // Keep enough uncommitted budget for at least one maximum-size
            // exact read. Additional reads consume only their actual aligned
            // buffer size and fail open when the aggregate budget is full.
            .and_then(|bytes| bytes.checked_add(MAX_READ_BUFFER_BYTES))
            .ok_or_else(|| invalid_runtime_config("minimum memory plan overflow"))?;
        Ok((reserved_memory, minimum))
    }
}

const WAKE_DATA: u8 = 1;
const WAKE_URGENT: u8 = 2;
const WAKE_ROTATE: u8 = 4;

pub(crate) enum HybridValueRead {
    L1(MemoryValue),
    L2(RegionValueRead),
    /// An L2 hit copied into the bounded L1 tier. The public tier remains
    /// L2 because that is where this lookup was served, but the exact-size
    /// transient read allocation can be released before `get` returns.
    PromotedL2(MemoryValue),
}

impl HybridValueRead {
    pub(crate) fn value(&self) -> &[u8] {
        match self {
            Self::L1(value) | Self::PromotedL2(value) => value.as_ref(),
            Self::L2(value) => value.value(),
        }
    }

    pub(crate) const fn is_l1(&self) -> bool {
        matches!(self, Self::L1(_))
    }
}

pub(crate) struct RegionDataPlane {
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    config: RuntimeConfig,
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
    Dormant(RuntimeFileSet),
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
    write_batch_bytes: usize,
    write_flush_delay: Duration,
    statistics: bool,
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
        config: RuntimeConfig,
    ) -> io::Result<Self> {
        let metrics = Arc::new(RuntimeMetrics::new(core.shard_count())?);
        Ok(Self {
            core,
            data,
            config,
            metrics,
            running: OnceLock::new(),
            lifecycle: Mutex::new(DataPlaneLifecycle::Dormant(files)),
            operations: RwLock::new(()),
        })
    }

    pub(crate) fn put(
        &self,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> io::Result<u64> {
        if key.len() > _MAX_KEY_BYTES || value.len() > _MAX_VALUE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file-chunk entry exceeds the 4 KiB key or 256 KiB value limit",
            ));
        }
        let record_bytes = required_record_bytes(namespace_id, key.len(), value.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let running = self.running()?;
        let hash = hash_namespaced_key(self.data.hash_seed, namespace_id, key);
        let shard_id = self.core.append_shard(namespace_id, hash);
        let control = &running.shards[shard_id];
        let activity = running
            .statistics
            .then(|| running.metrics.activity_for_hash(hash));
        let deadline = match self.config.write_backpressure {
            WriteBackpressure::Timeout(duration) => {
                let now = Instant::now();
                Some(now.checked_add(duration).unwrap_or(now))
            }
            WriteBackpressure::Reject | WriteBackpressure::Block => None,
        };

        loop {
            // Reject-mode admission is the shard write-buffer transaction itself.
            // The global write gate exists only for callers that explicitly
            // selected a waiting policy.
            let permit = if self.config.write_backpressure == WriteBackpressure::Reject {
                None
            } else {
                match running
                    .resources
                    .begin_write_permit_until(self.config.write_backpressure, deadline)
                {
                    Ok(permit) => Some(permit),
                    Err(reason) => {
                        if running.statistics {
                            running.metrics.record_write_rejection();
                        }
                        return Err(overload_runtime_error(reason));
                    }
                }
            };
            let operation = match self.operations.try_read() {
                Ok(operation) => operation,
                Err(TryLockError::WouldBlock) => {
                    drop(permit);
                    let reason = WriteOverloadReason::WriteGateBusy;
                    if running.statistics {
                        running.metrics.record_write_rejection();
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
                RegionStageValue::Staged(seqno) => {
                    let _published = running.memory.publish_pending(
                        shard_id,
                        hash,
                        namespace_id,
                        key,
                        value,
                        expires_at,
                        seqno,
                    );
                    control.notify(WAKE_DATA)?;
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.puts);
                        RuntimeMetrics::add(&activity.written_bytes, value.len());
                    }
                    return Ok(seqno);
                }
                RegionStageValue::NeedsProgress => {
                    prepare_shard_retry(
                        running,
                        control,
                        WAKE_URGENT,
                        self.config.write_backpressure,
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
                        self.config.write_backpressure,
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
                    let reason = WriteOverloadReason::WriteBufferBusy;
                    if running.statistics {
                        RuntimeMetrics::increment(&running.metrics.write_buffer_rejections);
                        running.metrics.record_write_rejection();
                    }
                    return Err(overload_runtime_error(reason));
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
            if self.config.statistics {
                let activity = self.metrics.activity(0);
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(None);
        }
        let running = self.running()?;
        let hash = hash_namespaced_key(self.data.hash_seed, namespace_id, key);
        let activity = running
            .statistics
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
                    return Ok(Some(HybridValueRead::L1(value)));
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
        let plan = match plan_read(self.data.geometry, initial_point.entry) {
            Ok(plan) if plan.aligned_len <= MAX_READ_BUFFER_BYTES => plan,
            Ok(_) | Err(_) => {
                self.core.enter_miss_only();
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(None);
            }
        };
        let engine = running.engine_for(hash);
        let slot = match engine.try_reserve_read() {
            Ok(slot) => slot,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                return Ok(None);
            }
            Err(_) => {
                self.core.enter_miss_only();
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(None);
            }
        };
        let Some(buffer) = running.resources.try_read_buffer(plan.aligned_len) else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
                RuntimeMetrics::increment(&activity.l2_read_memory_misses);
            }
            return Ok(None);
        };
        let result = self.core.read_value_from_point(
            engine.as_ref(),
            slot,
            buffer,
            plan,
            initial_point,
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
                        RuntimeMetrics::increment(&activity.l1_promotions);
                    }
                    return Ok(Some(HybridValueRead::PromotedL2(promoted)));
                }
                Ok(Some(HybridValueRead::L2(value)))
            }
            Ok(None) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(None)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                Ok(None)
            }
            Err(error) => {
                if running.statistics {
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
        if result.is_err() && self.config.statistics {
            RuntimeMetrics::increment(&self.metrics.io_failures);
        }
        result
    }

    pub(crate) fn snapshot(&self) -> io::Result<CacheSnapshot> {
        let running = self.running()?;
        Ok(self.snapshot_running(running))
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<DetailedCacheSnapshot> {
        let running = self.running()?;
        let writes = running.resources.runtime_snapshot(
            running
                .metrics
                .write_buffer_rejections
                .load(Ordering::Relaxed),
            running.metrics.write_buffer_wait_ns.load(Ordering::Relaxed),
        );
        Ok(DetailedCacheSnapshot {
            summary: self.snapshot_running(running),
            writes,
            io: aggregate_io_stats(&running.engines),
            region_sets: self.core.region_set_snapshots()?,
        })
    }

    fn snapshot_running(&self, running: &RunningShared) -> CacheSnapshot {
        self.metrics.snapshot(
            self.core.is_healthy(),
            self.config.statistics,
            running.resources.managed_memory_snapshot(),
            running.memory.metrics_snapshot(),
        )
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// The return value asks the backend to retain flock for process lifetime
    /// because an issued write or flush could not be fenced.
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
        let files = match std::mem::replace(&mut *lifecycle, DataPlaneLifecycle::Stopped) {
            DataPlaneLifecycle::Dormant(files) => files,
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

fn aggregate_io_stats(engines: &[Arc<dyn IoEngine>]) -> CacheIoSnapshot {
    let mut aggregate = CacheIoSnapshot::default();
    for (engine_index, engine) in engines.iter().enumerate() {
        let snapshot = engine.stats();
        aggregate.submitted = aggregate.submitted.saturating_add(snapshot.submitted);
        aggregate.completed = aggregate.completed.saturating_add(snapshot.completed);
        aggregate.cancel_requested = aggregate
            .cancel_requested
            .saturating_add(snapshot.cancel_requested);
        aggregate.cancelled = aggregate.cancelled.saturating_add(snapshot.cancelled);
        aggregate.errors = aggregate.errors.saturating_add(snapshot.errors);
        aggregate.requests_in_flight = aggregate
            .requests_in_flight
            .saturating_add(snapshot.requests_in_flight);
        aggregate.requests_in_flight_peak = aggregate
            .requests_in_flight_peak
            .saturating_add(snapshot.requests_in_flight_peak);
        aggregate.slot_wait_ns = aggregate.slot_wait_ns.saturating_add(snapshot.slot_wait_ns);
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
    config: RuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
) -> io::Result<RunningOwner> {
    let shard_count = core.shard_count();
    let reserved_memory = config.validated_reserved_memory_bytes(
        data.geometry,
        shard_count,
        core.runtime_reserved_memory_bytes()?,
    )?;
    let memory_limit = config.memory_limit_bytes;
    let resources = Arc::new(
        ResourceController::try_new(ResourceLimits {
            memory_limit_bytes: memory_limit,
            reserved_memory_bytes: reserved_memory,
            waiting_write_limit: config.waiting_write_limit,
        })
        .map_err(resource_build_io_error)?,
    );
    let usable_region = usize::try_from(data.geometry.region_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Region size is too large"))?;
    let chunk_bytes =
        usable_region.min(config.write_buffer_bytes) & !(crate::resources::BUFFER_ALIGNMENT - 1);
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
        config.l1_capacity_bytes,
        config.l1_shards,
        shard_count,
        config.eviction_policy,
        config.statistics,
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
        write_batch_bytes: config.write_batch_bytes,
        write_flush_delay: config.write_flush_delay,
        statistics: config.statistics,
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

fn runtime_topology_memory_bytes(shard_count: usize, config: &RuntimeConfig) -> Option<usize> {
    // Each I/O engine owns one normal worker. Reserve one additional stack per
    // engine for the bounded shutdown reaper path.
    let stack_count = config.io_workers.checked_mul(2)?.checked_add(shard_count)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let queue = config
        .io_concurrency
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let controls = config
        .io_workers
        .checked_add(shard_count)?
        .checked_add(config.l1_shards)?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(std::mem::size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn build_engine_pool(
    files: RuntimeFileSet,
    config: &RuntimeConfig,
) -> io::Result<Box<[Arc<dyn IoEngine>]>> {
    let mut source = Some(files);
    let mut engines = Vec::new();
    engines
        .try_reserve_exact(config.io_workers)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate I/O workers"))?;
    let base_depth = config.io_concurrency / config.io_workers;
    let remainder = config.io_concurrency % config.io_workers;
    for worker in 0..config.io_workers {
        let worker_files = if worker + 1 == config.io_workers {
            source.take().expect("last I/O worker owns file set")
        } else {
            source.as_ref().expect("I/O file set exists").try_clone()?
        };
        let worker_concurrency = base_depth + usize::from(worker < remainder);
        engines.push(build_file_engine(
            worker_files,
            worker_concurrency,
            config.io_engine(),
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
    if shared.statistics {
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
        engine.wake_slot_waiters();
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
                            .checked_add(shared.write_flush_delay)
                            .ok_or_else(|| {
                                invalid_runtime_config("partial flush deadline overflow")
                            })?,
                    );
                }
                if force_flush || fill.bytes >= shared.write_batch_bytes {
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
                    if rotated && shared.statistics {
                        RuntimeMetrics::increment(&shared.metrics.region_rotations);
                    }
                    advance_shard_progress(control)?;
                } else if flags & WAKE_DATA != 0 {
                    // A producer may have been followed by an urgent worker
                    // completion before this coalesced wake was observed.
                    advance_shard_progress(control)?;
                }
            }
            Err(StagingError::WouldBlock) => {
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
    policy: WriteBackpressure,
    deadline: Option<Instant>,
    permit: Permit,
    operation: Operation,
) -> io::Result<()> {
    let observed = control.progress()?;
    control.notify(flags)?;
    drop(operation);
    drop(permit);
    let wait_started = running.statistics.then(Instant::now);
    match policy {
        WriteBackpressure::Reject => {
            let reason = WriteOverloadReason::WriteBufferBusy;
            if running.statistics {
                RuntimeMetrics::increment(&running.metrics.write_buffer_rejections);
                running.metrics.record_write_rejection();
            }
            Err(overload_runtime_error(reason))
        }
        WriteBackpressure::Block => {
            let result = control.wait_for_progress_until(observed, None).map(|_| ());
            RuntimeMetrics::add_duration(&running.metrics.write_buffer_wait_ns, wait_started);
            result
        }
        WriteBackpressure::Timeout(_) => {
            let progressed = control.wait_for_progress_until(observed, deadline)?;
            RuntimeMetrics::add_duration(&running.metrics.write_buffer_wait_ns, wait_started);
            if progressed {
                Ok(())
            } else {
                let reason = WriteOverloadReason::Timeout;
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.write_buffer_rejections);
                    running.metrics.record_write_rejection();
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
    let writes_in_flight = owner
        .shared
        .engines
        .iter()
        .map(|engine| engine.writes_in_flight())
        .sum::<usize>();
    let unfenced_before = owner
        .shared
        .engines
        .iter()
        .any(|engine| engine.has_unfenced_writes());
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
            .any(|engine| engine.has_unfenced_writes());
    let result = drain
        .and_then(|()| join_error.map_or(Ok(()), Err))
        .and(shutdown);
    if skip_shutdown || unfenced {
        // A merely pending target gets a detached reaper: close returns now,
        // while eventual target completion still shuts the engine down and
        // reclaims its fd/thread/buffer set. A sticky fatal unfenced write
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
        let retain_lock = writes_in_flight != 0 || unfenced;
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

fn overload_runtime_error(error: WriteOverloadReason) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, error.to_string())
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
    fn maximum_read_buffer_is_derived_from_runtime_limits() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let record_len = required_record_bytes(1, _MAX_KEY_BYTES, _MAX_VALUE_BYTES).unwrap();
        assert_eq!(record_len as usize, MAX_RUNTIME_RECORD_BYTES);

        let mut observed_max = 0;
        for offset in (0.._READ_ALIGNMENT).step_by(RECORD_ALIGNMENT) {
            let padded_end = const_align_up(offset + record_len as usize, _READ_ALIGNMENT);
            for candidate_len in [record_len, (padded_end - offset) as u32] {
                let entry = crate::index::IndexEntry {
                    location: crate::index::PackedLocation::new(0, offset as u32, candidate_len)
                        .unwrap(),
                    seqno: 1,
                    namespace_id: 1,
                };
                let plan = plan_read(geometry, entry).unwrap();
                observed_max = observed_max.max(plan.aligned_len);
                assert!(plan.aligned_len <= MAX_READ_BUFFER_BYTES);
            }
        }
        assert_eq!(observed_max, MAX_READ_BUFFER_BYTES);
    }

    #[test]
    fn read_resource_misses_remain_separately_observable() {
        let metrics = RuntimeMetrics::new(1).unwrap();
        let activity = metrics.activity(0);
        RuntimeMetrics::add(&activity.l2_misses, 2);
        RuntimeMetrics::increment(&activity.l2_read_memory_misses);
        RuntimeMetrics::increment(&activity.l2_read_busy_misses);
        let snapshot = metrics.snapshot(
            true,
            true,
            ManagedMemorySnapshot {
                limit_bytes: 1024,
                current_bytes: 512,
                peak_bytes: 768,
            },
            MemoryMetricsSnapshot::default(),
        );

        assert_eq!(snapshot.l2_misses, 2);
        assert_eq!(snapshot.l2_read_memory_misses, 1);
        assert_eq!(snapshot.l2_read_busy_misses, 1);
    }

    #[test]
    fn preopen_memory_plan_charges_region_layout() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let mut config = RuntimeConfig::default();
        let fixed = crate::region::runtime_fixed_memory_bytes(4096, geometry.region_count).unwrap();
        let (_, without_layout) = config.memory_plan_bytes(geometry, 4, fixed).unwrap();
        config.memory_limit_bytes = without_layout;

        config.validate_memory_plan(geometry, 4096, 4, 0).unwrap();
        let error = config
            .validate_memory_plan(geometry, 4096, 4, 4096)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
