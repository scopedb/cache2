//! Self-owned steady-state runtime for RegionStore .
//!
//! Foreground writers encode directly into the fixed per-shard write
//! buffers. Shard workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory or let a benchmark generator inflate the measured
//! device path. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::format::MAX_KEY_SIZE;
use crate::hashing::route_hash;
use crate::io_backend::RuntimeFileSet;
use crate::io_engine::{IoEngine, MAX_IO_REQUESTS_PER_ENGINE, ReadSlot, build_file_engine};
use crate::memory::{
    MemoryLookup, MemoryMetricsSnapshot, MemoryReadToken, MemoryStore, MemoryValue,
};
use crate::record_codec::{hash_key, required_record_bytes};
use crate::recovery::{DataGeometry, DataSuperblock};
use crate::region::{FileRegionCore, RegionStageValue, RegionValueRead};
use crate::region_reader::{PendingRead, ReadCompletion, plan_read};
use crate::region_staging::{RegionStaging, StagingError};
use crate::resources::{
    CACHE_THREAD_STACK_BYTES, MAX_CONFIG_COUNT, ManagedMemorySnapshot, ResourceBuildError,
    ResourceController, ResourceLimits,
};
use crate::runtime_config::{MAX_APPEND_SHARDS, MAX_WRITE_FLUSH_THRESHOLD_BYTES, RuntimeConfig};
use crate::snapshot::{
    CacheHealth, CacheIoDirectionSnapshot, CacheIoSnapshot, CacheSnapshot, DetailedCacheSnapshot,
};

const WRITE_FLUSH_DELAY: Duration = Duration::from_millis(1);
const _RETRY_AGE: Duration = Duration::from_micros(50);
// Covers the bounded engine registry, command channel, and driver-side
// bookkeeping for one admitted I/O operation. Payload buffers are charged by
// ResourceController separately.
const IO_QUEUE_ENTRY_RESERVATION_BYTES: usize = 512;
// Covers worker/shard controls and handles whose size does not scale with the
// payload or engine depth.
const RUNTIME_CONTROL_RESERVATION_BYTES: usize = 4096;
// Keep the fixed L1 directory useful when the configured L2 has deliberate
// headroom. Smaller entries may still bypass before the byte budget fills;
// this avoids sizing metadata for the theoretical 64-byte minimum.
const PLANNED_MIN_L1_ENTRY_BYTES: usize = 4 * 1024;
const LIFECYCLE_RUNNING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FAILED: u8 = 2;
const MUTATION_DRAINING: usize = 1_usize << (usize::BITS - 1);
const MUTATION_COUNT_MASK: usize = !MUTATION_DRAINING;
const MUTATION_ENTER_ATTEMPTS: usize = 8;
static NEXT_METRICS_EPOCH: AtomicU64 = AtomicU64::new(1);

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

struct MutationGate {
    state: AtomicUsize,
    quiescent: Mutex<()>,
    quiescent_changed: Condvar,
    async_changed: tokio::sync::Notify,
}

impl MutationGate {
    fn new() -> Self {
        Self {
            state: AtomicUsize::new(0),
            quiescent: Mutex::new(()),
            quiescent_changed: Condvar::new(),
            async_changed: tokio::sync::Notify::new(),
        }
    }

    fn try_enter(&self) -> Option<MutationGuard<'_>> {
        let mut state = self.state.load(Ordering::Acquire);
        for _ in 0..MUTATION_ENTER_ATTEMPTS {
            if state & MUTATION_DRAINING != 0 || state & MUTATION_COUNT_MASK == MUTATION_COUNT_MASK
            {
                return None;
            }
            match self.state.compare_exchange_weak(
                state,
                state + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Some(MutationGuard { gate: self }),
                Err(observed) => state = observed,
            }
        }
        None
    }

    fn begin_drain(&self) -> io::Result<MutationDrainGuard<'_>> {
        let previous = self.state.fetch_or(MUTATION_DRAINING, Ordering::AcqRel);
        if previous & MUTATION_DRAINING != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cache drain is already in progress",
            ));
        }
        Ok(MutationDrainGuard { gate: self })
    }

    fn active_mutations(&self) -> usize {
        self.state.load(Ordering::Acquire) & MUTATION_COUNT_MASK
    }

    fn wait_quiescent(&self) -> io::Result<()> {
        let mut quiescent = self
            .quiescent
            .lock()
            .map_err(|_| poisoned_runtime_error())?;
        while self.active_mutations() != 0 {
            quiescent = self
                .quiescent_changed
                .wait(quiescent)
                .map_err(|_| poisoned_runtime_error())?;
        }
        Ok(())
    }

    async fn wait_quiescent_async(&self) {
        while self.active_mutations() != 0 {
            let notified = self.async_changed.notified();
            if self.active_mutations() == 0 {
                return;
            }
            notified.await;
        }
    }

    fn mutation_finished(&self) {
        let previous = self.state.fetch_sub(1, Ordering::Release);
        debug_assert_ne!(previous & MUTATION_COUNT_MASK, 0);
        if previous == (MUTATION_DRAINING | 1) {
            let quiescent = self
                .quiescent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.quiescent_changed.notify_all();
            drop(quiescent);
            self.async_changed.notify_one();
        }
    }
}

struct MutationGuard<'a> {
    gate: &'a MutationGate,
}

impl Drop for MutationGuard<'_> {
    fn drop(&mut self) {
        self.gate.mutation_finished();
    }
}

struct MutationDrainGuard<'a> {
    gate: &'a MutationGate,
}

impl MutationDrainGuard<'_> {
    fn wait(&self) -> io::Result<()> {
        self.gate.wait_quiescent()
    }

    async fn wait_async(&self) {
        self.gate.wait_quiescent_async().await;
    }
}

impl Drop for MutationDrainGuard<'_> {
    fn drop(&mut self) {
        let previous = self
            .gate
            .state
            .fetch_and(MUTATION_COUNT_MASK, Ordering::Release);
        debug_assert_ne!(previous & MUTATION_DRAINING, 0);
    }
}

struct RuntimeMetrics {
    metrics_epoch: u64,
    lifecycle: AtomicU8,
    activity: Box<[ActivityMetrics]>,
    write_rejections: AtomicU64,
    write_buffer_rejections: AtomicU64,
    io_failures: AtomicU64,
    region_rotations: AtomicU64,
}

#[repr(align(64))]
struct ActivityMetrics {
    puts: AtomicU64,
    deletes: AtomicU64,
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
            deletes: AtomicU64::new(0),
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
            metrics_epoch: NEXT_METRICS_EPOCH.fetch_add(1, Ordering::Relaxed),
            lifecycle: AtomicU8::new(LIFECYCLE_RUNNING),
            activity: activity.into_boxed_slice(),
            write_rejections: AtomicU64::new(0),
            write_buffer_rejections: AtomicU64::new(0),
            io_failures: AtomicU64::new(0),
            region_rotations: AtomicU64::new(0),
        })
    }

    fn activity(&self, shard_id: usize) -> &ActivityMetrics {
        &self.activity[shard_id]
    }

    fn activity_for_hash(&self, hash: u64) -> &ActivityMetrics {
        self.activity(route_hash(hash, self.activity.len()))
    }

    fn add(counter: &AtomicU64, value: usize) {
        let value = u64::try_from(value).unwrap_or(u64::MAX);
        counter.fetch_add(value, Ordering::Relaxed);
    }

    fn increment(counter: &AtomicU64) {
        Self::add(counter, 1);
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
        let mut deletes = 0_u64;
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
            deletes = deletes.saturating_add(activity.deletes.load(Ordering::Relaxed));
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
            metrics_epoch: self.metrics_epoch,
            health,
            statistics_enabled,
            puts,
            deletes,
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
            write_rejections: self.write_rejections.load(Ordering::Relaxed),
            io_failures: self.io_failures.load(Ordering::Relaxed),
            region_rotations: self.region_rotations.load(Ordering::Relaxed),
            managed_memory_bytes: memory.current_bytes,
            managed_memory_peak_bytes: memory.peak_bytes,
            managed_memory_limit_bytes: memory.limit_bytes,
            logical_disk_peak_bytes: 0,
            io: CacheIoSnapshot::default(),
        }
    }
}

impl RuntimeConfig {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if self.append_shards == 0 || self.append_shards > MAX_APPEND_SHARDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append shards must be in 1..=256",
            ));
        }
        if self.read_io_workers == 0 || self.write_io_workers == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read and write I/O worker counts must be non-zero",
            ));
        }
        if self.io_engine == crate::runtime_config::IoEngine::Posix
            && (self.read_io_workers > MAX_IO_REQUESTS_PER_ENGINE
                || self.write_io_workers > MAX_IO_REQUESTS_PER_ENGINE)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "POSIX read and write I/O workers must each be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"
                ),
            ));
        }
        if self.managed_memory_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed memory limit must be non-zero",
            ));
        }
        if self.l1_capacity_bytes > self.managed_memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 capacity must not exceed the managed memory limit",
            ));
        }
        if self.l1_shards == 0 || self.l1_shards > MAX_CONFIG_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 shards must be in 1..=65536",
            ));
        }
        if self.write_flush_threshold_bytes == 0
            || self.write_flush_threshold_bytes > MAX_WRITE_FLUSH_THRESHOLD_BYTES
            || !self
                .write_flush_threshold_bytes
                .is_multiple_of(crate::resources::BUFFER_ALIGNMENT)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write flush threshold must be 4 KiB aligned and within 4 KiB..=4 MiB",
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_memory_plan(
        &self,
        geometry: DataGeometry,
        index_slots: usize,
        shard_count: usize,
    ) -> io::Result<()> {
        let l1_entry_capacity = self.l1_entry_capacity(geometry, index_slots)?;
        let l1_metadata_bytes = MemoryStore::allocation_bytes(
            self.l1_capacity_bytes,
            l1_entry_capacity,
            self.l1_shards,
        )?;
        let fixed_bytes =
            crate::region::runtime_fixed_memory_bytes(index_slots, geometry.region_count)?
                .checked_add(l1_metadata_bytes)
                .ok_or_else(|| invalid_runtime_config("fixed memory plan overflow"))?;
        self.validated_reserved_memory_bytes(geometry, shard_count, fixed_bytes)?;
        Ok(())
    }

    fn l1_entry_capacity(&self, geometry: DataGeometry, index_slots: usize) -> io::Result<usize> {
        if self.l1_capacity_bytes == 0 {
            return Ok(0);
        }
        let l2_capacity = u128::from(geometry.region_size)
            .checked_mul(u128::from(geometry.region_count))
            .filter(|capacity| *capacity != 0)
            .ok_or_else(|| invalid_runtime_config("L2 capacity does not fit the L1 plan"))?;
        let expected_entries = index_slots.saturating_mul(4).div_ceil(5).max(1);
        let proportional = (expected_entries as u128)
            .checked_mul(self.l1_capacity_bytes as u128)
            .and_then(|entries| entries.checked_add(l2_capacity - 1))
            .map(|entries| entries / l2_capacity)
            .and_then(|entries| usize::try_from(entries).ok())
            .ok_or_else(|| invalid_runtime_config("L1 entry capacity does not fit usize"))?;
        let four_kib_density = self.l1_capacity_bytes.div_ceil(PLANNED_MIN_L1_ENTRY_BYTES);
        let maximum = MemoryStore::maximum_entry_capacity(self.l1_capacity_bytes, self.l1_shards);
        let minimum = self.l1_shards.min(maximum);
        Ok(proportional
            .max(four_kib_density)
            .min(expected_entries)
            .max(minimum)
            .min(maximum))
    }

    fn validated_reserved_memory_bytes(
        &self,
        geometry: DataGeometry,
        shard_count: usize,
        fixed_bytes: usize,
    ) -> io::Result<usize> {
        let (reserved_memory, minimum) =
            self.memory_plan_bytes(geometry, shard_count, fixed_bytes)?;
        if minimum > self.managed_memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "managed memory limit cannot hold the fixed cache memory plan: requires {minimum} bytes, configured {} bytes",
                    self.managed_memory_limit_bytes
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
        let chunk_bytes = usable_region;
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
            .and_then(|bytes| bytes.checked_add(usable_region))
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

enum PreparedGet {
    Complete(Option<HybridValueRead>),
    Pending(PendingGet),
}

struct PendingGet {
    engine: Arc<dyn IoEngine>,
    read: PendingRead,
    read_token: MemoryReadToken,
    hash: u64,
}

struct CompletedGet {
    read: ReadCompletion,
    read_token: MemoryReadToken,
    hash: u64,
}

impl PendingGet {
    #[cfg(test)]
    fn wait(self) -> CompletedGet {
        let Self {
            engine,
            read,
            read_token,
            hash,
        } = self;
        CompletedGet {
            read: read.wait(engine.as_ref()),
            read_token,
            hash,
        }
    }

    async fn wait_async(self, tokio_handle: &tokio::runtime::Handle) -> CompletedGet {
        let Self {
            engine,
            read,
            read_token,
            hash,
        } = self;
        CompletedGet {
            read: read.wait_async(engine, tokio_handle).await,
            read_token,
            hash,
        }
    }
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
    running: RunningOwner,
    // Fences write admission for drain, flush, and shutdown. Reads do not
    // participate because they cannot extend the set of records being fenced.
    operations: MutationGate,
}

struct RunningOwner {
    shared: Arc<RunningShared>,
    shard_workers: Vec<JoinHandle<()>>,
}

struct RunningShared {
    core: Arc<FileRegionCore>,
    read_engines: Box<[Arc<dyn IoEngine>]>,
    write_engines: Box<[Arc<dyn IoEngine>]>,
    resources: Arc<ResourceController>,
    metrics: Arc<RuntimeMetrics>,
    memory: Arc<MemoryStore>,
    staging: Arc<RegionStaging>,
    shards: Box<[Arc<ShardControl>]>,
    write_flush_threshold_bytes: usize,
    statistics: bool,
}

impl RunningShared {
    fn write_engine_for(&self, route: u64) -> &Arc<dyn IoEngine> {
        &self.write_engines[route_hash(route, self.write_engines.len())]
    }

    fn try_reserve_read(&self, route: u64) -> io::Result<(Arc<dyn IoEngine>, ReadSlot)> {
        try_reserve_read_lane(&self.read_engines, route)
    }

    fn engines(&self) -> impl Iterator<Item = &Arc<dyn IoEngine>> {
        self.read_engines.iter().chain(self.write_engines.iter())
    }
}

fn try_reserve_read_lane(
    engines: &[Arc<dyn IoEngine>],
    route: u64,
) -> io::Result<(Arc<dyn IoEngine>, ReadSlot)> {
    debug_assert!(!engines.is_empty());
    let primary = route_hash(route, engines.len());
    match engines[primary].try_reserve_read() {
        Ok(slot) => Ok((Arc::clone(&engines[primary]), slot)),
        Err(error) if engines.len() == 1 || !is_read_pressure(error.kind()) => Err(error),
        Err(primary_error) => {
            let offset = 1 + route_hash(route.rotate_right(32), engines.len() - 1);
            let alternate = (primary + offset) % engines.len();
            match engines[alternate].try_reserve_read() {
                Ok(slot) => Ok((Arc::clone(&engines[alternate]), slot)),
                Err(error) if !is_read_pressure(error.kind()) => Err(error),
                Err(_) => Err(primary_error),
            }
        }
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
    drain_requested: u64,
    drain_completed: u64,
    stop: bool,
    failure: Option<ShardFailure>,
}

struct ShardControl {
    state: Mutex<ShardControlState>,
    changed: Condvar,
    async_changed: tokio::sync::Notify,
}

impl ShardControl {
    fn new() -> Self {
        Self {
            state: Mutex::new(ShardControlState::default()),
            changed: Condvar::new(),
            async_changed: tokio::sync::Notify::new(),
        }
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

    fn request_drain(&self, stop: bool) -> io::Result<u64> {
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        state.drain_requested = state
            .drain_requested
            .checked_add(1)
            .ok_or_else(|| io::Error::other("shard drain generation exhausted"))?;
        state.stop |= stop;
        let generation = state.drain_requested;
        self.changed.notify_one();
        if poisoned {
            Err(poisoned_runtime_error())
        } else {
            Ok(generation)
        }
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

    async fn wait_for_drain_async(&self, generation: u64) -> io::Result<()> {
        loop {
            let notified = self.async_changed.notified();
            {
                let state = self.lock()?;
                if let Some(failure) = &state.failure {
                    return Err(failure.to_error());
                }
                if state.drain_completed >= generation {
                    return Ok(());
                }
            }
            notified.await;
        }
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
        self.async_changed.notify_one();
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
        core.set_index_statistics_enabled(config.statistics);
        let metrics = Arc::new(RuntimeMetrics::new(core.shard_count())?);
        let running = start_running(
            Arc::clone(&core),
            data,
            files,
            config.clone(),
            Arc::clone(&metrics),
        )?;
        Ok(Self {
            core,
            data,
            config,
            metrics,
            running,
            operations: MutationGate::new(),
        })
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file-chunk key exceeds the 4 KiB limit",
            ));
        }
        let record_bytes = required_record_bytes(key.len(), value.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        if u64::from(record_bytes) > self.data.geometry.region_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "encoded file-chunk entry exceeds one Region",
            ));
        }
        let running = &self.running.shared;
        let hash = hash_key(self.data.hash_seed, key);
        let shard_id = self.core.append_shard(hash);
        let control = &running.shards[shard_id];
        let activity = running
            .statistics
            .then(|| running.metrics.activity_for_hash(hash));
        let operation = match self.operations.try_enter() {
            Some(operation) => operation,
            None => {
                if running.statistics {
                    running.metrics.record_write_rejection();
                }
                return Err(write_overload_error());
            }
        };
        let staged = self.core.try_stage_value(
            &running.staging,
            shard_id,
            hash,
            record_bytes,
            key,
            value,
        )?;
        match staged {
            RegionStageValue::Staged(seqno) => {
                let _published = running.memory.publish(hash, key, value, seqno);
                control.notify(WAKE_DATA)?;
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.puts);
                    RuntimeMetrics::add(&activity.written_bytes, value.len());
                }
                Ok(seqno)
            }
            RegionStageValue::NeedsProgress => {
                reject_staged_write(running, control, WAKE_URGENT, operation)
            }
            RegionStageValue::NeedsRotation => {
                reject_staged_write(running, control, WAKE_ROTATE | WAKE_URGENT, operation)
            }
        }
    }

    pub(crate) fn delete(&self, key: &[u8]) -> io::Result<u64> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file-chunk key exceeds the 4 KiB limit",
            ));
        }
        let running = &self.running.shared;
        let hash = hash_key(self.data.hash_seed, key);
        let activity = running
            .statistics
            .then(|| running.metrics.activity_for_hash(hash));
        let operation = match self.operations.try_enter() {
            Some(operation) => operation,
            None => {
                if running.statistics {
                    running.metrics.record_write_rejection();
                }
                return Err(write_overload_error());
            }
        };
        let Some(seqno) = self.core.try_delete_value(hash)? else {
            drop(operation);
            if running.statistics {
                running.metrics.record_write_rejection();
            }
            return Err(write_overload_error());
        };
        let _removed = running.memory.delete(hash, key, seqno);
        if let Some(activity) = activity {
            RuntimeMetrics::increment(&activity.deletes);
        }
        Ok(seqno)
    }

    #[cfg(test)]
    pub(crate) fn get(&self, key: &[u8]) -> io::Result<Option<HybridValueRead>> {
        match self.prepare_get(key)? {
            PreparedGet::Complete(value) => Ok(value),
            PreparedGet::Pending(pending) => self.finish_get(pending.wait(), key),
        }
    }

    pub(crate) async fn get_async(
        &self,
        key: &[u8],
        tokio_handle: &tokio::runtime::Handle,
    ) -> io::Result<Option<HybridValueRead>> {
        match self.prepare_get(key)? {
            PreparedGet::Complete(value) => Ok(value),
            PreparedGet::Pending(pending) => {
                self.finish_get(pending.wait_async(tokio_handle).await, key)
            }
        }
    }

    fn prepare_get(&self, key: &[u8]) -> io::Result<PreparedGet> {
        if key.len() > MAX_KEY_SIZE {
            if self.config.statistics {
                let activity = self.metrics.activity(0);
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        }
        let running = &self.running.shared;
        let hash = hash_key(self.data.hash_seed, key);
        let activity = running
            .statistics
            .then(|| running.metrics.activity_for_hash(hash));
        if !self.core.is_healthy() {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        }
        // This health observation is the read's availability linearization
        // point. A later one-way transition to miss-only does not invalidate a
        // value that was already resident here.
        let read_token = match running.memory.lookup(hash, key) {
            MemoryLookup::Hit(value) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l1_hits);
                    RuntimeMetrics::add(&activity.served_bytes, value.len());
                }
                return Ok(PreparedGet::Complete(Some(HybridValueRead::L1(value))));
            }
            MemoryLookup::Miss(token) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l1_misses);
                }
                token
            }
        };
        let Some(entry) = self.core.begin_value_read(hash) else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        };
        let plan = match plan_read(self.data.geometry, hash, entry) {
            Ok(plan) => plan,
            Err(_) => {
                self.core.enter_miss_only();
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
        };
        let (engine, slot) = match running.try_reserve_read(hash) {
            Ok(reservation) => reservation,
            Err(error) if is_read_pressure(error.kind()) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
            Err(_) => {
                self.core.enter_miss_only();
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
        };
        let Some(buffer) = running.resources.try_read_buffer(plan.aligned_len) else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
                RuntimeMetrics::increment(&activity.l2_read_memory_misses);
            }
            return Ok(PreparedGet::Complete(None));
        };
        match self
            .core
            .submit_value_read_from_plan(engine.as_ref(), slot, buffer, plan)
        {
            Ok(read) => Ok(PreparedGet::Pending(PendingGet {
                engine,
                read,
                read_token,
                hash,
            })),
            // MissOnly is a cache availability state, not an application data
            // error. The operation that trips the one-way health latch and all
            // later reads therefore fail open as cache misses. Resource
            // overload remains explicit while the core is still healthy.
            Err(_) if !self.core.is_healthy() => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(PreparedGet::Complete(None))
            }
            Err(error) if is_read_pressure(error.kind()) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                Ok(PreparedGet::Complete(None))
            }
            Err(error) => {
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                Err(error)
            }
        }
    }

    fn finish_get(
        &self,
        completed: CompletedGet,
        key: &[u8],
    ) -> io::Result<Option<HybridValueRead>> {
        let running = &self.running.shared;
        let CompletedGet {
            read,
            read_token,
            hash,
        } = completed;
        let activity = running
            .statistics
            .then(|| running.metrics.activity_for_hash(hash));
        let result = self.core.finish_value_read(read, key);
        match result {
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
                let promoted =
                    running
                        .memory
                        .promote(read_token, hash, key, value.value(), value.seqno());
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
            Err(error) if is_read_pressure(error.kind()) => {
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
    #[cfg(test)]
    pub(crate) fn drain(&self) -> io::Result<()> {
        let operations = self.operations.begin_drain()?;
        operations.wait()?;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle);
        let running = &self.running.shared;
        drain_shards(running, false)
    }

    pub(crate) async fn drain_async(&self) -> io::Result<()> {
        let operations = self.operations.begin_drain()?;
        operations.wait_async().await;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle);
        let running = &self.running.shared;
        drain_shards_async(running, false).await
    }

    pub(crate) fn snapshot(&self) -> io::Result<CacheSnapshot> {
        let running = &self.running.shared;
        Ok(self.snapshot_running(running))
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<DetailedCacheSnapshot> {
        let running = &self.running.shared;
        Ok(DetailedCacheSnapshot {
            summary: self.snapshot_running(running),
            write_buffer_rejections: running
                .metrics
                .write_buffer_rejections
                .load(Ordering::Relaxed),
            l1: running.memory.detailed_snapshot()?,
            index: self.core.index_snapshot()?,
            region: self.core.region_snapshot()?,
        })
    }

    fn snapshot_running(&self, running: &RunningShared) -> CacheSnapshot {
        let mut snapshot = self.metrics.snapshot(
            self.core.is_healthy(),
            self.config.statistics,
            running.resources.managed_memory_snapshot(),
            running.memory.metrics_snapshot(),
        );
        snapshot.io = aggregate_io_stats(&running.read_engines, &running.write_engines);
        snapshot
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// The return value asks the backend to retain flock for process lifetime
    /// because an issued write or flush could not be fenced.
    pub(crate) fn shutdown(self) -> io::Result<bool> {
        let Self {
            metrics,
            running,
            operations,
            ..
        } = self;
        let _ = metrics.lifecycle.compare_exchange(
            LIFECYCLE_RUNNING,
            LIFECYCLE_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let operations = operations.begin_drain()?;
        operations.wait()?;
        let retain_lock = stop_running(running)?;
        Ok(retain_lock)
    }

    #[cfg(test)]
    pub(crate) fn poison_shard_for_test(&self, shard_id: usize) {
        let shard = self
            .running
            .shared
            .shards
            .get(shard_id)
            .expect("test shard exists");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = shard.state.lock().unwrap();
            panic!("poison shard gate");
        }));
        assert!(result.is_err());
    }
}

fn aggregate_io_stats(
    read_engines: &[Arc<dyn IoEngine>],
    write_engines: &[Arc<dyn IoEngine>],
) -> CacheIoSnapshot {
    let mut aggregate = CacheIoSnapshot::default();
    for (engine_index, engine) in read_engines.iter().chain(write_engines).enumerate() {
        let snapshot = engine.stats();
        if engine_index < read_engines.len() {
            add_io_direction(&mut aggregate.read, snapshot.requests);
        } else {
            add_io_direction(&mut aggregate.write, snapshot.requests);
        }
        // File-set clones intentionally share one path counter. Read it once
        // rather than multiplying the same totals by the number of workers.
        if engine_index == 0 {
            aggregate.read.buffered = snapshot.runtime.read.buffered;
            aggregate.read.direct = snapshot.runtime.read.direct;
            aggregate.write.buffered = snapshot.runtime.write.buffered;
            aggregate.write.direct = snapshot.runtime.write.direct;
        }
    }
    aggregate
}

fn add_io_direction(aggregate: &mut CacheIoDirectionSnapshot, snapshot: CacheIoDirectionSnapshot) {
    aggregate.requests_submitted = aggregate
        .requests_submitted
        .saturating_add(snapshot.requests_submitted);
    aggregate.requests_succeeded = aggregate
        .requests_succeeded
        .saturating_add(snapshot.requests_succeeded);
    aggregate.requests_cancelled = aggregate
        .requests_cancelled
        .saturating_add(snapshot.requests_cancelled);
    aggregate.requests_failed = aggregate
        .requests_failed
        .saturating_add(snapshot.requests_failed);
    aggregate.requests_in_flight = aggregate
        .requests_in_flight
        .saturating_add(snapshot.requests_in_flight);
    aggregate.requests_in_flight_peak = aggregate
        .requests_in_flight_peak
        .saturating_add(snapshot.requests_in_flight_peak);
    aggregate.slot_wait_ns = aggregate.slot_wait_ns.saturating_add(snapshot.slot_wait_ns);
    aggregate.request_time_ns = aggregate
        .request_time_ns
        .saturating_add(snapshot.request_time_ns);
}

fn start_running(
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    files: RuntimeFileSet,
    config: RuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
) -> io::Result<RunningOwner> {
    let shard_count = core.shard_count();
    let l1_entry_capacity = config.l1_entry_capacity(data.geometry, core.index_slot_count())?;
    let l1_metadata_bytes = MemoryStore::allocation_bytes(
        config.l1_capacity_bytes,
        l1_entry_capacity,
        config.l1_shards,
    )?;
    let fixed_memory = core
        .runtime_reserved_memory_bytes()?
        .checked_add(l1_metadata_bytes)
        .ok_or_else(|| invalid_runtime_config("fixed memory plan overflow"))?;
    let reserved_memory =
        config.validated_reserved_memory_bytes(data.geometry, shard_count, fixed_memory)?;
    let memory_limit = config.managed_memory_limit_bytes;
    let resources = Arc::new(
        ResourceController::try_new(ResourceLimits {
            memory_limit_bytes: memory_limit,
            reserved_memory_bytes: reserved_memory,
        })
        .map_err(resource_build_io_error)?,
    );
    let usable_region = usize::try_from(data.geometry.region_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Region size is too large"))?;
    let chunk_bytes = usable_region;
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
        l1_entry_capacity,
        config.l1_shards,
        config.statistics,
    )?);
    let write_files = files.try_clone()?;
    let read_engines = build_engine_pool(files, &config, config.read_io_workers)?;
    let write_engines = build_engine_pool(write_files, &config, config.write_io_workers)?;
    let mut shards = Vec::new();
    shards.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate shard controls")
    })?;
    shards.resize_with(shard_count, || Arc::new(ShardControl::new()));
    let shared = Arc::new(RunningShared {
        core,
        read_engines,
        write_engines,
        resources,
        metrics,
        memory,
        staging,
        shards: shards.into_boxed_slice(),
        write_flush_threshold_bytes: config.write_flush_threshold_bytes,
        statistics: config.statistics,
    });
    let mut shard_workers = Vec::new();
    shard_workers.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate worker handles")
    })?;
    for shard_id in 0..shard_count {
        let worker_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("cache2-shard-{shard_id}"))
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
                for engine in shared.engines() {
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
    // Reserve one stack per configured worker, one possible shutdown reaper per
    // engine, and one worker per append shard.
    let read_engine_count = config.io_engine_count(config.read_io_workers);
    let write_engine_count = config.io_engine_count(config.write_io_workers);
    let engine_count = read_engine_count.checked_add(write_engine_count)?;
    let stack_count = config
        .read_io_workers
        .checked_add(config.write_io_workers)?
        .checked_add(engine_count)?
        .checked_add(shard_count)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let read_queue =
        read_engine_count.checked_mul(config.io_depth_per_engine(config.read_io_workers))?;
    let queue = write_engine_count
        .checked_mul(config.io_depth_per_engine(config.write_io_workers))?
        .checked_add(read_queue)?
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let controls = engine_count
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
    workers: usize,
) -> io::Result<Box<[Arc<dyn IoEngine>]>> {
    let mut source = Some(files);
    let engine_count = config.io_engine_count(workers);
    let mut engines = Vec::new();
    engines
        .try_reserve_exact(engine_count)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate I/O workers"))?;
    let engine_depth = config.io_depth_per_engine(workers);
    let posix_workers = if config.io_engine() == crate::runtime_config::IoEngine::Posix {
        workers
    } else {
        1
    };
    for engine in 0..engine_count {
        let worker_files = if engine + 1 == engine_count {
            source.take().expect("last I/O worker owns file set")
        } else {
            source.as_ref().expect("I/O file set exists").try_clone()?
        };
        engines.push(build_file_engine(
            worker_files,
            engine_depth,
            posix_workers,
            config.io_engine(),
            config.statistics,
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
    let first_failure = shared
        .metrics
        .lifecycle
        .swap(LIFECYCLE_FAILED, Ordering::AcqRel)
        != LIFECYCLE_FAILED;
    if first_failure {
        log::error!(
            target: "cache2::health",
            event = "cache_shard_worker_failed",
            shard_id,
            error:% = error;
            "cache shard worker failed"
        );
    }
    shared.core.enter_miss_only();
    control.fail(&error);
    // Wake engine admission in case another shard is blocked behind work that
    // can no longer make progress after this runtime entered miss-only.
    for engine in shared.engines() {
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
                    deadline = Some(Instant::now().checked_add(WRITE_FLUSH_DELAY).ok_or_else(
                        || invalid_runtime_config("partial flush deadline overflow"),
                    )?);
                }
                if force_flush || fill.bytes >= shared.write_flush_threshold_bytes {
                    let engine = shared.write_engine_for(shard_id as u64);
                    shared
                        .core
                        .flush_staging_shard(&shared.staging, engine.as_ref(), shard_id)?;
                    deadline = None;
                }
            }
            Ok(None) => {
                deadline = None;
                if rotate {
                    let rotated = shared.core.rotate_shard(shard_id)?;
                    if rotated && shared.statistics {
                        RuntimeMetrics::increment(&shared.metrics.region_rotations);
                    }
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
                let engine = shared.write_engine_for(shard_id as u64);
                shared
                    .core
                    .flush_staging_shard(&shared.staging, engine.as_ref(), shard_id)?;
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

fn reject_staged_write<Operation>(
    running: &RunningShared,
    control: &ShardControl,
    flags: u8,
    operation: Operation,
) -> io::Result<u64> {
    control.notify(flags)?;
    drop(operation);
    if running.statistics {
        RuntimeMetrics::increment(&running.metrics.write_buffer_rejections);
        running.metrics.record_write_rejection();
    }
    Err(write_overload_error())
}

fn complete_shard_drain(control: &ShardControl, generation: u64) -> io::Result<()> {
    let mut state = control.lock()?;
    state.drain_completed = state.drain_completed.max(generation);
    control.changed.notify_all();
    drop(state);
    control.async_changed.notify_one();
    Ok(())
}

fn drain_shards(shared: &RunningShared, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(shared.shards.len())
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate drain fence"))?;
    let mut first_error = None;
    for shard in &shared.shards {
        match shard.request_drain(stop) {
            Ok(generation) => generations.push(Some(generation)),
            Err(error) => {
                first_error.get_or_insert(error);
                generations.push(None);
            }
        }
    }
    for (shard, generation) in shared.shards.iter().zip(generations) {
        if let Some(generation) = generation
            && let Err(error) = shard.wait_for_drain(generation)
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn drain_shards_async(shared: &RunningShared, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(shared.shards.len())
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate drain fence"))?;
    let mut first_error = None;
    for shard in &shared.shards {
        match shard.request_drain(stop) {
            Ok(generation) => generations.push(Some(generation)),
            Err(error) => {
                first_error.get_or_insert(error);
                generations.push(None);
            }
        }
    }
    for (shard, generation) in shared.shards.iter().zip(generations) {
        if let Some(generation) = generation
            && let Err(error) = shard.wait_for_drain_async(generation).await
        {
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
        .engines()
        .map(|engine| engine.in_flight())
        .sum::<usize>();
    let writes_in_flight = owner
        .shared
        .engines()
        .map(|engine| engine.writes_in_flight())
        .sum::<usize>();
    let unfenced_before = owner
        .shared
        .engines()
        .any(|engine| engine.has_unfenced_writes());
    // A request that missed its cancellation grace may still own a kernel
    // target and buffer. Joining that engine can wait forever. Retain only the
    // engine Arc; the runtime/core can still be released normally.
    let skip_shutdown = in_flight != 0 || unfenced_before;
    let shutdown = if skip_shutdown {
        Ok(())
    } else {
        let mut result = Ok(());
        for engine in owner.shared.engines() {
            if let Err(error) = engine.shutdown()
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    };
    let unfenced = unfenced_before
        || owner
            .shared
            .engines()
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
            for engine in owner.shared.engines() {
                std::mem::forget(Arc::clone(engine));
            }
        } else {
            for engine in owner.shared.engines() {
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
        .name("cache2-io-reaper".to_owned())
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

fn write_overload_error() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "write path is busy")
}

fn is_read_pressure(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::OutOfMemory
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
    )
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
    use crate::io_backend::{FileBackend, IoBackend};
    use crate::io_engine::BackendIoEngine;

    static LANE_TEST_ID: AtomicU64 = AtomicU64::new(1);

    #[test]
    fn read_lane_uses_one_bounded_alternate_on_primary_pressure() {
        let id = LANE_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cache2-read-lane-{}-{id}.cache",
            std::process::id()
        ));
        let backend: Arc<dyn IoBackend> = Arc::new(FileBackend::open(&path).unwrap());
        let engines: Box<[Arc<dyn IoEngine>]> = vec![
            Arc::new(BackendIoEngine::new(Arc::clone(&backend), 1).unwrap()) as Arc<dyn IoEngine>,
            Arc::new(BackendIoEngine::new(Arc::clone(&backend), 1).unwrap()) as Arc<dyn IoEngine>,
        ]
        .into_boxed_slice();
        let primary = engines[0].try_reserve_read().unwrap();

        let (selected, alternate) = try_reserve_read_lane(&engines, 0).unwrap();
        assert!(Arc::ptr_eq(&selected, &engines[1]));
        let error = match try_reserve_read_lane(&engines, 0) {
            Ok(_) => panic!("both read lanes are already reserved"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        drop(alternate);
        drop(primary);
        drop(selected);
        for engine in &engines {
            engine.shutdown().unwrap();
        }
        drop(engines);
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn mutation_gate_fences_existing_and_new_mutations() {
        let gate = MutationGate::new();
        let mutation = gate.try_enter().unwrap();
        let drain = gate.begin_drain().unwrap();

        assert!(gate.try_enter().is_none());
        drop(mutation);
        drain.wait().unwrap();
        assert!(gate.try_enter().is_none());

        drop(drain);
        assert!(gate.try_enter().is_some());
    }

    #[tokio::test]
    async fn mutation_gate_wakes_async_drain_without_blocking() {
        let gate = Arc::new(MutationGate::new());
        let mutation = gate.try_enter().unwrap();
        let drain_gate = Arc::clone(&gate);
        let drain = tokio::spawn(async move {
            let drain = drain_gate.begin_drain().unwrap();
            drain.wait_async().await;
        });
        tokio::task::yield_now().await;
        assert!(gate.try_enter().is_none());

        drop(mutation);
        drain.await.unwrap();
        assert!(gate.try_enter().is_some());
    }

    #[tokio::test]
    async fn cancelling_async_drain_reopens_mutation_admission() {
        let gate = Arc::new(MutationGate::new());
        let mutation = gate.try_enter().unwrap();
        let drain_gate = Arc::clone(&gate);
        let drain = tokio::spawn(async move {
            let drain = drain_gate.begin_drain().unwrap();
            drain.wait_async().await;
        });
        tokio::task::yield_now().await;
        assert!(gate.try_enter().is_none());

        drain.abort();
        assert!(drain.await.unwrap_err().is_cancelled());
        assert!(gate.try_enter().is_some());
        drop(mutation);
    }

    #[test]
    fn urgent_empty_shard_wake_is_consumed() {
        let control = ShardControl::new();
        control.notify(WAKE_URGENT).unwrap();

        let (flags, drain_generation, stop, timed_out) =
            wait_for_shard_work(&control, None).unwrap();
        assert_eq!(flags, WAKE_URGENT);
        assert_eq!(drain_generation, 0);
        assert!(!stop);
        assert!(!timed_out);

        let (flags, drain_generation, stop, timed_out) =
            wait_for_shard_work(&control, Some(Instant::now())).unwrap();
        assert_eq!(flags, 0);
        assert_eq!(drain_generation, 0);
        assert!(!stop);
        assert!(timed_out);
    }

    #[test]
    fn transient_read_pressure_is_not_a_cache_failure() {
        for kind in [
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Interrupted,
        ] {
            assert!(is_read_pressure(kind));
        }
        assert!(!is_read_pressure(io::ErrorKind::InvalidData));
    }

    #[test]
    fn maximum_read_buffer_is_derived_from_runtime_limits() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let value_len = geometry.region_size as usize - crate::format::RECORD_HEADER_SIZE;
        let record_len = required_record_bytes(0, value_len).unwrap();
        assert_eq!(u64::from(record_len), geometry.region_size);
        let entry = crate::index::IndexEntry {
            location: crate::index::PackedLocation::new(0, 0, record_len).unwrap(),
            seqno: 1,
        };
        assert_eq!(
            plan_read(geometry, 1, entry).unwrap().aligned_len,
            geometry.region_size as usize
        );
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
    fn production_l1_entry_plan_scales_from_the_static_index() {
        const GIB: usize = 1024 * 1024 * 1024;
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(32 * 1024 * 1024, 32 * 1024).unwrap(),
            region_size: 32 * 1024 * 1024,
            region_count: 32 * 1024,
        };
        let index_slots = 335_544_320;
        let base = RuntimeConfig::default()
            .with_l1_capacity_bytes(10 * GIB)
            .with_managed_memory_limit_bytes(24 * GIB)
            .with_l1_shards(64);
        let entry_capacity = base.l1_entry_capacity(geometry, index_slots).unwrap();
        assert_eq!(entry_capacity, 2_621_440);
        base.validate_memory_plan(geometry, index_slots, 8).unwrap();
        base.clone()
            .with_managed_memory_limit_bytes(19 * GIB)
            .validate_memory_plan(geometry, index_slots, 8)
            .unwrap();
        let too_small = base.clone().with_managed_memory_limit_bytes(18 * GIB);
        assert_eq!(
            too_small
                .validate_memory_plan(geometry, index_slots, 8)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let metadata =
            MemoryStore::allocation_bytes(base.l1_capacity_bytes, entry_capacity, base.l1_shards)
                .unwrap();
        assert!(metadata < 384 * 1024 * 1024);
    }
}
