// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Cache request execution, L1/L2 coordination, and bounded background workers.
//!
//! Foreground writers encode directly into the fixed per-shard write
//! buffers. Shard workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::mem;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

use asyncband::semaphore::OwnedSemaphorePermit;
use asyncband::semaphore::Semaphore;
use asyncband::watch;

use self::metrics::RuntimeMetrics;
use crate::config::CacheConfig;
use crate::config::l1_entry_capacity;
use crate::config::reserved_memory_bytes;
use crate::config::runtime::IoMode;
use crate::config::runtime::IoPoolTopology;
#[cfg(test)]
use crate::config::runtime::ReadAdmission;
use crate::config::runtime::RuntimeOptions;
use crate::config::runtime::read_io_wait_capacity;
use crate::config::runtime::read_io_wait_timeout;
use crate::config::storage_geometry;
use crate::hashing::route_hash;
use crate::io::engine::IoBuffer;
use crate::io::engine::IoEngine;
use crate::io::engine::IoOperation;
use crate::io::engine::ReadSlot;
use crate::io::engine::ReadSlotWaiter;
use crate::io::engine::build_file_engine;
use crate::io::engine::submit_background_io;
use crate::io::file::DataFileHandles;
use crate::io::fill_control::FLUSH_RETRY;
use crate::io::fill_control::FillController;
use crate::io::fill_control::FlushCharge;
use crate::io::recovery::IoRecovery;
use crate::managed_memory::BufferLease;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;
use crate::managed_memory::ManagedMemory;
use crate::managed_memory::ManagedMemoryError;
use crate::managed_memory::ManagedMemoryLimits;
#[cfg(test)]
use crate::managed_memory::ManagedMemorySnapshot;
use crate::memory::MemoryLookup;
#[cfg(test)]
use crate::memory::MemoryMetricsSnapshot;
use crate::memory::MemoryReadToken;
use crate::memory::MemoryStore;
use crate::memory::MemoryValue;
use crate::region::RegionStageValue;
use crate::region::RegionStore;
use crate::region::RegionValue;
#[cfg(test)]
use crate::region::index::packed::IndexEntry;
#[cfg(test)]
use crate::region::index::packed::PackedLocation;
#[cfg(test)]
use crate::region::index::storage::page_format::INDEX_IMAGE_PAGE_SIZE;
#[cfg(test)]
use crate::region::index::storage::page_format::INDEX_IMAGE_SLOTS_PER_PAGE;
use crate::region::is_read_pressure;
use crate::region::reader::PendingRead;
#[cfg(test)]
use crate::region::reader::ReadCandidate;
use crate::region::reader::ReadCompletion;
use crate::region::reader::ReadDesc;
use crate::region::reader::describe_read;
use crate::region::record::MAX_KEY_SIZE;
#[cfg(test)]
use crate::region::record::RECORD_HEADER_SIZE;
use crate::region::record::codec::hash_key;
use crate::region::record::codec::required_record_bytes;
#[cfg(test)]
use crate::region::recovery::DataGeometry;
use crate::region::recovery::DataSuperblock;
#[cfg(test)]
use crate::region::runtime_fixed_memory_bytes;
use crate::region::staging::AppendStaging;
use crate::region::staging::StagingError;
use crate::snapshot::CacheIoDirectionSnapshot;
use crate::snapshot::CacheIoSnapshot;
use crate::snapshot::CacheSnapshot;
use crate::snapshot::DetailedCacheSnapshot;

pub mod metrics;

const WRITE_FLUSH_DELAY: Duration = Duration::from_millis(1);
const STAGING_RETRY_DELAY: Duration = Duration::from_micros(50);
const LIFECYCLE_RUNNING: u8 = 0;
const LIFECYCLE_DRAINING: u8 = 1;
const LIFECYCLE_FAILED: u8 = 2;
const MUTATION_DRAINING: usize = 1_usize << (usize::BITS - 1);
const MUTATION_CLOSED: usize = 1_usize << (usize::BITS - 2);
const MUTATION_FENCED: usize = MUTATION_DRAINING | MUTATION_CLOSED;
const MUTATION_COUNT_MASK: usize = !MUTATION_FENCED;
const MUTATION_ENTER_ATTEMPTS: usize = 8;

struct LifecycleDrainingGuard<'a> {
    lifecycle: &'a AtomicU8,
    operations: &'a MutationGate,
}

impl<'a> LifecycleDrainingGuard<'a> {
    fn enter(lifecycle: &'a AtomicU8, operations: &'a MutationGate) -> Option<Self> {
        lifecycle
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_DRAINING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
            .then_some(Self {
                lifecycle,
                operations,
            })
    }
}

impl Drop for LifecycleDrainingGuard<'_> {
    fn drop(&mut self) {
        if !self.operations.is_closed() {
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
    async_changed: watch::Sender<()>,
}

impl MutationGate {
    fn new() -> Self {
        let (async_changed, _) = watch::channel(());
        Self {
            state: AtomicUsize::new(0),
            quiescent: Mutex::new(()),
            quiescent_changed: Condvar::new(),
            async_changed,
        }
    }

    fn try_enter(&self) -> Option<MutationGuard<'_>> {
        let mut state = self.state.load(Ordering::Acquire);
        for _ in 0..MUTATION_ENTER_ATTEMPTS {
            if state & MUTATION_FENCED != 0 || state & MUTATION_COUNT_MASK == MUTATION_COUNT_MASK {
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
        if previous & MUTATION_FENCED != 0 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cache drain is already in progress",
            ));
        }
        Ok(MutationDrainGuard { gate: self })
    }

    fn start_close(&self) {
        self.state.fetch_or(MUTATION_CLOSED, Ordering::AcqRel);
    }

    fn is_closed(&self) -> bool {
        self.state.load(Ordering::Acquire) & MUTATION_CLOSED != 0
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
        // Subscribe before inspecting the predicate so a transition racing the check advances the
        // receiver version and cannot be missed.
        let mut changed = self.async_changed.subscribe();
        while self.active_mutations() != 0 {
            changed
                .changed()
                .await
                .expect("the mutation gate retains its watch sender");
        }
    }

    fn mutation_finished(&self) {
        let previous = self.state.fetch_sub(1, Ordering::Release);
        debug_assert_ne!(previous & MUTATION_COUNT_MASK, 0);
        if previous & MUTATION_COUNT_MASK == 1 && previous & MUTATION_FENCED != 0 {
            let quiescent = self
                .quiescent
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.quiescent_changed.notify_all();
            drop(quiescent);
            self.async_changed.send_replace(());
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
    #[cfg(test)]
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
            .fetch_and(!MUTATION_DRAINING, Ordering::Release);
        debug_assert_ne!(previous & MUTATION_DRAINING, 0);
    }
}

const WAKE_DATA: u8 = 1;
const WAKE_URGENT: u8 = 2;
const WAKE_ROTATE: u8 = 4;

pub enum CacheRead {
    L1(MemoryValue),
    L2(RegionValue),
    /// An L2 hit copied into the bounded L1 tier. The public tier remains
    /// L2 because that is where this lookup was served, but the transient
    /// aligned read allocation can be released before `get` returns.
    PromotedL2(MemoryValue),
}

enum PreparedGet {
    Complete(Option<CacheRead>),
    Pending(PendingGet),
    Waiting(WaitingGet),
}

struct PendingGet {
    engine: Arc<IoEngine>,
    read: PendingRead,
    read_token: MemoryReadToken,
    hash: u64,
}

struct WaitingGet {
    engine: Arc<IoEngine>,
    slot_waiter: ReadSlotWaiter,
    desc: ReadDesc,
    read_token: MemoryReadToken,
    hash: u64,
    deadline: Instant,
    waiter_permit: OwnedSemaphorePermit,
}

struct ReservedGet {
    engine: Arc<IoEngine>,
    slot: ReadSlot,
    desc: ReadDesc,
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

impl WaitingGet {
    async fn reserve_async(self, tokio_handle: &tokio::runtime::Handle) -> io::Result<ReservedGet> {
        let Self {
            engine,
            slot_waiter,
            desc,
            read_token,
            hash,
            deadline,
            waiter_permit,
        } = self;
        let slot = slot_waiter.reserve_until(deadline, tokio_handle).await?;
        drop(waiter_permit);
        Ok(ReservedGet {
            engine,
            slot,
            desc,
            read_token,
            hash,
        })
    }
}

impl CacheRead {
    pub fn value(&self) -> &[u8] {
        match self {
            Self::L1(value) | Self::PromotedL2(value) => value.as_ref(),
            Self::L2(value) => value.value(),
        }
    }

    pub const fn is_l1(&self) -> bool {
        matches!(self, Self::L1(_))
    }
}

#[derive(Clone)]
pub struct CacheRuntime {
    regions: Arc<RegionStore>,
    data: DataSuperblock,
    options: RuntimeOptions,
    metrics: Arc<RuntimeMetrics>,
    state: Arc<RuntimeState>,
    workers: Arc<Mutex<Option<RuntimeWorkers>>>,
    // Fences write admission for drain, flush, and shutdown. Reads do not
    // participate because they cannot extend the set of records being fenced.
    operations: Arc<MutationGate>,
}

struct RuntimeWorkers {
    state: Arc<RuntimeState>,
    append_workers: Vec<JoinHandle<()>>,
    reclaim_workers: Vec<JoinHandle<()>>,
}

struct RuntimeState {
    regions: Arc<RegionStore>,
    read_engines: Box<[Arc<IoEngine>]>,
    read_lane_cursor: AtomicUsize,
    read_waiters: Option<Arc<Semaphore>>,
    write_engines: Box<[Arc<IoEngine>]>,
    reclaim_engines: Box<[Arc<IoEngine>]>,
    reclaim_control: ReclaimControl,
    reclaim_io_timeout: Duration,
    io_recovery: IoRecovery,
    managed_memory: Arc<ManagedMemory>,
    metrics: Arc<RuntimeMetrics>,
    memory: Arc<MemoryStore>,
    staging: Arc<AppendStaging>,
    operations: Arc<MutationGate>,
    append_controls: Box<[Arc<AppendWorkerControl>]>,
    write_flush_threshold_bytes: usize,
    align_reads_for_direct_io: bool,
    activity_counters: bool,
    #[cfg(test)]
    after_io_snapshot: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[derive(Default)]
struct ReclaimControlState {
    generation: u64,
    stop: bool,
}

struct ReclaimControl {
    state: Mutex<ReclaimControlState>,
    changed: Condvar,
}

impl ReclaimControl {
    fn new() -> Self {
        Self {
            state: Mutex::new(ReclaimControlState::default()),
            changed: Condvar::new(),
        }
    }

    fn notify(&self) -> io::Result<()> {
        let mut state = self.state.lock().map_err(|_| poisoned_runtime_error())?;
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        Ok(())
    }

    fn stop(&self) -> io::Result<()> {
        let mut state = self.state.lock().map_err(|_| poisoned_runtime_error())?;
        state.stop = true;
        self.changed.notify_all();
        Ok(())
    }

    fn is_stopped(&self) -> io::Result<bool> {
        Ok(self
            .state
            .lock()
            .map_err(|_| poisoned_runtime_error())?
            .stop)
    }

    fn wait(&self, observed_generation: &mut u64) -> io::Result<bool> {
        let mut state = self.state.lock().map_err(|_| poisoned_runtime_error())?;
        while state.generation == *observed_generation && !state.stop {
            state = self
                .changed
                .wait(state)
                .map_err(|_| poisoned_runtime_error())?;
        }
        if state.stop {
            return Ok(false);
        }
        *observed_generation = state.generation;
        Ok(true)
    }
}

impl RuntimeState {
    fn write_engine_for(&self, route: u64) -> &Arc<IoEngine> {
        &self.write_engines[route_hash(route, self.write_engines.len())]
    }

    fn try_reserve_read(&self, route: u64) -> io::Result<(Arc<IoEngine>, ReadSlot)> {
        try_reserve_read_lane(&self.read_engines, route, &self.read_lane_cursor)
    }

    fn try_queue_read(
        &self,
        route: u64,
        desc: ReadDesc,
        read_token: MemoryReadToken,
        timeout: Duration,
    ) -> io::Result<WaitingGet> {
        let waiters = self
            .read_waiters
            .as_ref()
            .expect("non-zero read wait timeout creates a waiter bound");
        let waiter_permit = Arc::clone(waiters).try_acquire_owned(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::WouldBlock, "L2 read wait queue is full")
        })?;
        let engine = Arc::clone(&self.read_engines[route_hash(route, self.read_engines.len())]);
        let slot_waiter = engine.read_slot_waiter();
        let deadline = Instant::now()
            .checked_add(timeout)
            .unwrap_or_else(Instant::now);
        Ok(WaitingGet {
            engine,
            slot_waiter,
            desc,
            read_token,
            hash: route,
            deadline,
            waiter_permit,
        })
    }

    fn engines(&self) -> impl Iterator<Item = &Arc<IoEngine>> {
        self.read_engines
            .iter()
            .chain(self.write_engines.iter())
            .chain(self.reclaim_engines.iter())
    }
}

fn try_reserve_read_lane(
    engines: &[Arc<IoEngine>],
    route: u64,
    pressure_cursor: &AtomicUsize,
) -> io::Result<(Arc<IoEngine>, ReadSlot)> {
    let reserve = |lane: usize| -> io::Result<(Arc<IoEngine>, ReadSlot)> {
        let slot = engines[lane].try_reserve_read()?;
        Ok((Arc::clone(&engines[lane]), slot))
    };
    let lane_count = engines.len();
    debug_assert_ne!(lane_count, 0);
    let primary = route_hash(route, lane_count);
    match reserve(primary) {
        Ok(reservation) => Ok(reservation),
        Err(error) if lane_count == 1 || !is_read_pressure(error.kind()) => Err(error),
        Err(primary_error) => {
            // Keep the uncontended route stable, but rotate the one bounded
            // fallback so a hot route can use every physical lane over time.
            let offset = 1 + pressure_cursor.fetch_add(1, Ordering::Relaxed) % (lane_count - 1);
            let alternate = (primary + offset) % lane_count;
            match reserve(alternate) {
                Ok(reservation) => Ok(reservation),
                Err(error) if !is_read_pressure(error.kind()) => Err(error),
                Err(_) => Err(primary_error),
            }
        }
    }
}

fn should_wake_write(previous_bytes: usize, current_bytes: usize, threshold: usize) -> bool {
    previous_bytes == 0 || (previous_bytes < threshold && current_bytes >= threshold)
}

#[derive(Clone)]
struct AppendWorkerFailure {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl AppendWorkerFailure {
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
struct AppendWorkerState {
    wake_flags: u8,
    drain_requested: u64,
    drain_completed: u64,
    stop: bool,
    failure: Option<AppendWorkerFailure>,
}

struct AppendWorkerControl {
    state: Mutex<AppendWorkerState>,
    changed: Condvar,
    async_changed: watch::Sender<()>,
}

impl AppendWorkerControl {
    fn new() -> Self {
        let (async_changed, _) = watch::channel(());
        Self {
            state: Mutex::new(AppendWorkerState::default()),
            changed: Condvar::new(),
            async_changed,
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
        let was_idle = state.wake_flags == 0;
        state.wake_flags |= flags;
        if was_idle {
            self.changed.notify_one();
        }
        Ok(())
    }

    fn notify_if_running(&self, flags: u8) -> io::Result<()> {
        let mut state = self.lock()?;
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        if state.stop {
            return Ok(());
        }
        let was_idle = state.wake_flags == 0;
        state.wake_flags |= flags;
        if was_idle {
            self.changed.notify_one();
        }
        Ok(())
    }

    fn request_drain(&self, stop: bool) -> io::Result<u64> {
        let (mut state, poisoned) = match self.state.lock() {
            Ok(state) => (state, false),
            Err(error) => (error.into_inner(), true),
        };
        if state.stop {
            return Err(closed_runtime_error());
        }
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
        // Subscribe before inspecting the predicate so a completion racing the check advances the
        // receiver version and cannot be missed.
        let mut changed = self.async_changed.subscribe();
        loop {
            {
                let state = self.lock()?;
                if let Some(failure) = &state.failure {
                    return Err(failure.to_error());
                }
                if state.drain_completed >= generation {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .expect("the shard control retains its watch sender");
        }
    }

    fn fail(&self, error: &io::Error) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .failure
            .get_or_insert_with(|| AppendWorkerFailure::from_error(error));
        drop(state);
        self.changed.notify_all();
        self.async_changed.send_replace(());
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, AppendWorkerState>> {
        self.state.lock().map_err(|_| poisoned_runtime_error())
    }
}

impl CacheRuntime {
    pub fn regions(&self) -> &Arc<RegionStore> {
        &self.regions
    }

    pub fn stats_recorder(&self) -> &crate::stats::recording::Recorder {
        &self.metrics.stats
    }

    pub fn start(
        regions: Arc<RegionStore>,
        data: DataSuperblock,
        handles: DataFileHandles,
        config: CacheConfig,
    ) -> io::Result<Self> {
        // Recovery supplies independently validated metadata. It must still
        // match the configuration selected for this open.
        let storage = config.storage();
        if data.geometry != storage_geometry(storage)
            || regions.region_count()? != storage.region_count() as usize
            || regions.index_slot_count() != storage.index_slots()
            || regions.shard_count() != config.runtime().append_shards as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "recovered layout does not match the cache configuration",
            ));
        }
        let options = config.runtime().clone();
        regions.configure_reclaim_workers(
            IoPoolTopology::reclaim(options.io_engine).max_in_flight(),
        )?;
        regions.set_index_activity_counters_enabled(options.stats.activity_counters);
        let metrics = Arc::new(RuntimeMetrics::new(regions.shard_count(), options.stats)?);
        let operations = Arc::new(MutationGate::new());
        let workers = start_workers(
            Arc::clone(&regions),
            data,
            handles,
            config,
            Arc::clone(&metrics),
            Arc::clone(&operations),
        )?;
        let state = Arc::clone(&workers.state);
        Ok(Self {
            regions,
            data,
            options,
            metrics,
            state,
            workers: Arc::new(Mutex::new(Some(workers))),
            operations,
        })
    }

    pub fn start_close(&self) {
        self.state.io_recovery.stop();
        self.operations.start_close();
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.put_with_l1::<true>(key, value)
    }

    pub fn put_l2(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.put_with_l1::<false>(key, value)
    }

    fn put_with_l1<const ADMIT_L1: bool>(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
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
        let state = &self.state;
        if state.io_recovery.is_recovering() {
            if state.activity_counters {
                state.metrics.record_write_rejection();
            }
            return Err(write_overload_error());
        }
        let hash = hash_key(self.data.hash_seed, key);
        let shard_id = self.regions.append_shard(hash);
        let control = &state.append_controls[shard_id];
        let activity = state
            .activity_counters
            .then(|| state.metrics.activity_for_hash(hash));
        let operation = match self.operations.try_enter() {
            Some(operation) => operation,
            None => {
                if state.activity_counters {
                    state.metrics.record_write_rejection();
                }
                return Err(write_overload_error());
            }
        };
        if let Some(fill) = &state.io_recovery.fill
            && !fill.try_admit_fill()
        {
            if state.activity_counters {
                state.metrics.record_write_rejection();
            }
            return Err(write_overload_error());
        }
        let staged = self.regions.try_stage_value(
            &state.staging,
            shard_id,
            hash,
            record_bytes,
            key,
            value,
        )?;
        match staged {
            RegionStageValue::Staged {
                seqno,
                previous_bytes,
                current_bytes,
            } => {
                if ADMIT_L1 {
                    let _published = state.memory.publish(hash, key, value, seqno);
                } else {
                    // Prevent an older exact-key L1 value from indefinitely
                    // shadowing the prefetched L2 record. Contention remains a
                    // valid best-effort stale outcome.
                    let _removed = state.memory.delete(hash, key, seqno);
                }
                if should_wake_write(
                    previous_bytes,
                    current_bytes,
                    state.write_flush_threshold_bytes,
                ) {
                    control.notify(WAKE_DATA)?;
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.puts);
                    RuntimeMetrics::add(&activity.written_bytes, value.len());
                }
                Ok(seqno)
            }
            RegionStageValue::NeedsProgress => {
                reject_staged_write(state, control, WAKE_URGENT, operation)
            }
            RegionStageValue::NeedsRotation => {
                reject_staged_write(state, control, WAKE_ROTATE | WAKE_URGENT, operation)
            }
        }
    }

    pub fn delete(&self, key: &[u8]) -> io::Result<u64> {
        if key.len() > MAX_KEY_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file-chunk key exceeds the 4 KiB limit",
            ));
        }
        let state = &self.state;
        let hash = hash_key(self.data.hash_seed, key);
        let activity = state
            .activity_counters
            .then(|| state.metrics.activity_for_hash(hash));
        let operation = match self.operations.try_enter() {
            Some(operation) => operation,
            None => {
                if state.activity_counters {
                    state.metrics.record_write_rejection();
                }
                return Err(write_overload_error());
            }
        };
        let Some(seqno) = self.regions.try_delete_value(hash)? else {
            drop(operation);
            if state.activity_counters {
                state.metrics.record_write_rejection();
            }
            return Err(write_overload_error());
        };
        let _removed = state.memory.delete(hash, key, seqno);
        if let Some(activity) = activity {
            RuntimeMetrics::increment(&activity.deletes);
        }
        Ok(seqno)
    }

    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> io::Result<Option<CacheRead>> {
        match self.prepare_get(key, None)? {
            PreparedGet::Complete(value) => Ok(value),
            PreparedGet::Pending(pending) => self.finish_get(pending.wait(), key),
            PreparedGet::Waiting(_) => Err(io::Error::other(
                "bounded read waiting requires the async get path",
            )),
        }
    }

    pub async fn get_async(
        &self,
        key: &[u8],
        tokio_handle: &tokio::runtime::Handle,
        guard: Option<&mut crate::stats::recording::RequestGuard<'_>>,
    ) -> io::Result<Option<CacheRead>> {
        match self.prepare_get(key, guard)? {
            PreparedGet::Complete(value) => Ok(value),
            PreparedGet::Pending(pending) => {
                self.finish_get(pending.wait_async(tokio_handle).await, key)
            }
            PreparedGet::Waiting(waiting) => {
                let wait_started = self.options.stats.activity_counters.then(Instant::now);
                let reserved = waiting.reserve_async(tokio_handle).await;
                if let Some(wait_started) = wait_started {
                    self.metrics.record_read_wait(wait_started.elapsed());
                }
                let reserved = reserved.inspect_err(|error| self.record_read_wait_error(error))?;
                let Some(pending) = self.submit_reserved_get(reserved)? else {
                    return Ok(None);
                };
                self.finish_get(pending.wait_async(tokio_handle).await, key)
            }
        }
    }

    fn record_read_wait_error(&self, error: &io::Error) {
        if !self.options.stats.activity_counters {
            return;
        }
        if is_read_pressure(error.kind()) {
            self.metrics.record_read_overload();
        } else {
            RuntimeMetrics::increment(&self.metrics.io_failures);
        }
    }

    fn submit_reserved_get(&self, reserved: ReservedGet) -> io::Result<Option<PendingGet>> {
        let ReservedGet {
            engine,
            slot,
            desc,
            read_token,
            hash,
        } = reserved;
        let Some(buffer) = self.state.managed_memory.try_read_buffer(desc.read_len) else {
            if self.options.stats.activity_counters {
                self.metrics.record_read_overload();
            }
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "L2 read could not reserve its bounded buffer after waiting",
            ));
        };
        match self
            .regions
            .submit_value_read(engine.as_ref(), slot, buffer, desc)
        {
            Ok(read) => Ok(Some(PendingGet {
                engine,
                read,
                read_token,
                hash,
            })),
            Err(_) if !self.regions.is_healthy() => {
                if self.options.stats.activity_counters {
                    RuntimeMetrics::increment(&self.metrics.io_failures);
                    RuntimeMetrics::increment(&self.metrics.activity_for_hash(hash).l2_misses);
                }
                Ok(None)
            }
            Err(error) => {
                self.record_read_wait_error(&error);
                Err(error)
            }
        }
    }

    fn prepare_get(
        &self,
        key: &[u8],
        guard: Option<&mut crate::stats::recording::RequestGuard<'_>>,
    ) -> io::Result<PreparedGet> {
        if key.len() > MAX_KEY_SIZE {
            if self.options.stats.activity_counters {
                let activity = self.metrics.activity(0);
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        }
        let state = &self.state;
        let hash = hash_key(self.data.hash_seed, key);
        let activity = state
            .activity_counters
            .then(|| state.metrics.activity_for_hash(hash));
        if !self.regions.is_healthy() {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        }
        // This health observation is the read's availability linearization
        // point. A later one-way transition to miss-only does not invalidate a
        // value that was already resident here.
        let read_token = match state.memory.lookup(hash, key) {
            MemoryLookup::Hit(value) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l1_hits);
                    RuntimeMetrics::add(&activity.served_bytes, value.len());
                }
                return Ok(PreparedGet::Complete(Some(CacheRead::L1(value))));
            }
            MemoryLookup::Miss(token) => {
                if let Some(guard) = guard {
                    guard.enter_l2();
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l1_misses);
                }
                token
            }
        };
        let Some(candidate) = self.regions.begin_point_read(hash) else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        };
        let desc = match describe_read(
            self.data.geometry,
            hash,
            candidate,
            state.align_reads_for_direct_io,
        ) {
            Ok(desc) => desc,
            Err(error) => {
                self.regions
                    .enter_miss_only_with_error("record_read_descriptor_invalid", &error);
                if state.activity_counters {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
        };
        let (engine, slot) = match state.try_reserve_read(hash) {
            Ok(reservation) => reservation,
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && !read_io_wait_timeout(&self.options).is_zero() =>
            {
                let waiting = state
                    .try_queue_read(hash, desc, read_token, read_io_wait_timeout(&self.options))
                    .inspect_err(|_| {
                        if state.activity_counters {
                            state.metrics.record_read_overload();
                        }
                    })?;
                return Ok(PreparedGet::Waiting(waiting));
            }
            Err(error) if is_read_pressure(error.kind()) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
            Err(error) => {
                self.regions
                    .enter_miss_only_with_error("read_engine_reservation_failed", &error);
                if state.activity_counters {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
        };
        let Some(buffer) = state.managed_memory.try_read_buffer(desc.read_len) else {
            if !read_io_wait_timeout(&self.options).is_zero() {
                if state.activity_counters {
                    state.metrics.record_read_overload();
                }
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "L2 read could not reserve its bounded buffer",
                ));
            }
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
                RuntimeMetrics::increment(&activity.l2_read_memory_misses);
            }
            return Ok(PreparedGet::Complete(None));
        };
        match self
            .regions
            .submit_value_read(engine.as_ref(), slot, buffer, desc)
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
            // overload remains explicit while the regions is still healthy.
            Err(_) if !self.regions.is_healthy() => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(PreparedGet::Complete(None))
            }
            Err(error) if is_read_pressure(error.kind()) => {
                if !read_io_wait_timeout(&self.options).is_zero() {
                    if state.activity_counters {
                        state.metrics.record_read_overload();
                    }
                    return Err(error);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                Ok(PreparedGet::Complete(None))
            }
            Err(error) => {
                if state.activity_counters {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
                }
                Err(error)
            }
        }
    }

    fn finish_get(&self, completed: CompletedGet, key: &[u8]) -> io::Result<Option<CacheRead>> {
        let state = &self.state;
        let CompletedGet {
            read,
            read_token,
            hash,
        } = completed;
        let activity = state
            .activity_counters
            .then(|| state.metrics.activity_for_hash(hash));
        let result = self.regions.finish_value_read(read, key);
        match result {
            Err(_) if !self.regions.is_healthy() => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
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
                    state
                        .memory
                        .promote(read_token, hash, key, value.value(), value.seqno());
                if let Some(promoted) = promoted {
                    if let Some(activity) = activity {
                        RuntimeMetrics::increment(&activity.l1_promotions);
                    }
                    return Ok(Some(CacheRead::PromotedL2(promoted)));
                }
                Ok(Some(CacheRead::L2(value)))
            }
            Ok(None) => {
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                Ok(None)
            }
            Err(error) if is_read_pressure(error.kind()) => {
                if !read_io_wait_timeout(&self.options).is_zero() {
                    self.record_read_wait_error(&error);
                    return Err(error);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                    RuntimeMetrics::increment(&activity.l2_read_busy_misses);
                }
                Ok(None)
            }
            Err(error) => {
                if state.activity_counters {
                    RuntimeMetrics::increment(&state.metrics.io_failures);
                }
                Err(error)
            }
        }
    }

    /// Completes and publishes every record admitted before this call. This is
    /// an I/O completion barrier, not a fdatasync durability boundary.
    #[cfg(test)]
    pub fn drain(&self) -> io::Result<()> {
        let operations = self.operations.begin_drain()?;
        operations.wait()?;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle, &self.operations);
        let state = &self.state;
        drain_shards(state, false)
    }

    pub async fn drain_async(&self) -> io::Result<()> {
        let operations = self.operations.begin_drain()?;
        operations.wait_async().await;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle, &self.operations);
        let state = &self.state;
        drain_shards_async(state, false).await
    }

    pub fn snapshot(&self) -> io::Result<CacheSnapshot> {
        let state = &self.state;
        Ok(self.snapshot_state(state))
    }

    pub fn detailed_snapshot(&self) -> io::Result<DetailedCacheSnapshot> {
        let state = &self.state;
        Ok(DetailedCacheSnapshot {
            summary: self.snapshot_state(state),
            write_buffer_rejections: state
                .metrics
                .write_buffer_rejections
                .load(Ordering::Relaxed),
            l1: state.memory.detailed_snapshot()?,
            index: self.regions.index_snapshot()?,
            region: self.regions.region_snapshot()?,
        })
    }

    fn snapshot_state(&self, state: &RuntimeState) -> CacheSnapshot {
        let mut snapshot = self.metrics.snapshot(
            self.regions.is_healthy(),
            self.options.stats.activity_counters,
            state.managed_memory.snapshot(),
            state.memory.metrics_snapshot(),
        );
        if snapshot.health == crate::snapshot::CacheHealth::Running
            && state.io_recovery.is_recovering()
        {
            snapshot.health = crate::snapshot::CacheHealth::Recovering;
        }
        if let Some(fill) = &state.io_recovery.fill {
            snapshot.fill_control = fill.snapshot();
        }
        snapshot.io = aggregate_io_stats(
            &state.read_engines,
            &state.write_engines,
            &state.reclaim_engines,
        );
        snapshot
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// A true return value requires the session to retain flock for process lifetime
    /// because an issued write or flush could not be fenced.
    pub fn shutdown(&self) -> io::Result<bool> {
        self.start_close();
        self.operations.wait_quiescent()?;
        let _ = self.metrics.lifecycle.compare_exchange(
            LIFECYCLE_RUNNING,
            LIFECYCLE_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let workers = self
            .workers
            .lock()
            .map_err(|_| poisoned_runtime_error())?
            .take()
            .ok_or_else(closed_runtime_error)?;
        let retain_lock = stop_workers(workers)?;
        Ok(retain_lock)
    }

    #[cfg(test)]
    pub fn reserve_read_slot_for_test(&self) -> ReadSlot {
        self.state
            .try_reserve_read(0)
            .map(|(_, slot)| slot)
            .expect("test read slot is available")
    }

    #[cfg(test)]
    pub fn poison_append_worker_for_test(&self, shard_id: usize) {
        let shard = self
            .state
            .append_controls
            .get(shard_id)
            .expect("test shard exists");
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            let _state = shard.state.lock().unwrap();
            panic!("poison shard gate");
        }));
        assert!(result.is_err());
    }
}

fn aggregate_io_stats(
    read_engines: &[Arc<IoEngine>],
    write_engines: &[Arc<IoEngine>],
    reclaim_engines: &[Arc<IoEngine>],
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
            aggregate.read.buffered = snapshot.file_io.read.buffered;
            aggregate.read.direct = snapshot.file_io.read.direct;
            aggregate.write.buffered = snapshot.file_io.write.buffered;
            aggregate.write.direct = snapshot.file_io.write.direct;
        }
    }
    for engine in reclaim_engines {
        add_io_direction(&mut aggregate.read, engine.stats().requests);
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

fn start_workers(
    regions: Arc<RegionStore>,
    data: DataSuperblock,
    handles: DataFileHandles,
    config: CacheConfig,
    metrics: Arc<RuntimeMetrics>,
    operations: Arc<MutationGate>,
) -> io::Result<RuntimeWorkers> {
    let shard_count = regions.shard_count();
    let options = config.runtime();
    let l1_entry_capacity = l1_entry_capacity(&config);
    let memory_limit = options.managed_memory_limit_bytes;
    let managed_memory = Arc::new(
        ManagedMemory::try_new(ManagedMemoryLimits {
            memory_limit_bytes: memory_limit,
            reserved_memory_bytes: reserved_memory_bytes(&config),
        })
        .map_err(managed_memory_io_error)?,
    );
    let usable_region = usize::try_from(data.geometry.region_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Region size is too large"))?;
    let chunk_bytes = usable_region;
    let staging = Arc::new(
        AppendStaging::try_new(
            shard_count,
            chunk_bytes,
            data.geometry.region_size,
            &managed_memory,
        )
        .map_err(managed_memory_io_error)?,
    );
    let memory = Arc::new(MemoryStore::new(
        options.l1_capacity_bytes,
        l1_entry_capacity,
        options.l1_shards,
        options.l1_eviction_policy,
        options.stats.activity_counters,
    )?);
    let reclaim_worker_count = IoPoolTopology::reclaim(options.io_engine).max_in_flight();
    let mut reclaim_buffers = Vec::new();
    reclaim_buffers
        .try_reserve_exact(reclaim_worker_count)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate Region reclaim buffer owners",
            )
        })?;
    for _ in 0..reclaim_worker_count {
        reclaim_buffers.push(
            managed_memory
                .try_read_buffer(usable_region)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "cannot allocate a fixed Region reclaim buffer",
                    )
                })?,
        );
    }
    let reclaim_handles = handles.try_clone()?;
    let write_handles = handles.try_clone()?;
    let read_wait_enabled = !read_io_wait_timeout(options).is_zero();
    let read_engines = build_engine_pool(
        handles,
        options,
        IoPoolTopology::read(options.io_engine),
        read_wait_enabled,
    )?;
    let read_waiters =
        read_wait_enabled.then(|| Arc::new(Semaphore::new(read_io_wait_capacity(options))));
    let write_engines = build_engine_pool(
        write_handles,
        options,
        IoPoolTopology::write(options.io_engine),
        false,
    )?;
    let reclaim_engines = build_engine_pool(
        reclaim_handles,
        options,
        IoPoolTopology::reclaim(options.io_engine),
        false,
    )?;
    for (engines, role) in [
        (&read_engines, crate::IoRole::Read),
        (&write_engines, crate::IoRole::Write),
        (&reclaim_engines, crate::IoRole::Reclaim),
    ] {
        for engine in engines.iter() {
            if let Some(timing) = metrics.stats.io_timing(role) {
                engine.set_latency_recorder(timing);
            }
        }
    }
    let mut append_controls = Vec::new();
    append_controls
        .try_reserve_exact(shard_count)
        .map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate shard controls")
        })?;
    append_controls.resize_with(shard_count, || Arc::new(AppendWorkerControl::new()));
    let fill = FillController::new(
        options.fill_control,
        shard_count + reclaim_worker_count,
        data.geometry.region_size,
    )?;
    let state = Arc::new(RuntimeState {
        regions,
        read_engines,
        read_lane_cursor: AtomicUsize::new(0),
        read_waiters,
        write_engines,
        reclaim_engines,
        reclaim_control: ReclaimControl::new(),
        reclaim_io_timeout: options.reclaim_io_timeout,
        io_recovery: IoRecovery::with_fill(options.io_recovery_timeout, fill),
        managed_memory,
        metrics,
        memory,
        staging,
        operations,
        append_controls: append_controls.into_boxed_slice(),
        write_flush_threshold_bytes: options.write_flush_threshold_bytes,
        align_reads_for_direct_io: options.io_mode == IoMode::Direct,
        activity_counters: options.stats.activity_counters,
        #[cfg(test)]
        after_io_snapshot: Mutex::new(None),
    });
    // Inspect the recovered queue before workers can contend with foreground
    // mutations. Fresh caches have no sealed Regions and need no wakeup.
    let reclaim_on_start = state.regions.reclaim_needed()?;
    let mut reclaim_workers = Vec::new();
    reclaim_workers
        .try_reserve_exact(reclaim_worker_count)
        .map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate worker handles")
        })?;
    let mut append_workers = Vec::new();
    append_workers.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate worker handles")
    })?;
    for shard_id in 0..shard_count {
        let worker_state = Arc::clone(&state);
        match std::thread::Builder::new()
            .name(format!("cache2-shard-{shard_id}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || append_worker(worker_state, shard_id))
        {
            Ok(worker) => append_workers.push(worker),
            Err(error) => {
                for shard in &state.append_controls {
                    let _ = shard.request_drain(true);
                }
                for worker in append_workers {
                    let _ = worker.join();
                }
                state.staging.close();
                for engine in state.engines() {
                    let _ = engine.shutdown();
                }
                return Err(error);
            }
        }
    }
    for (worker_id, buffer) in reclaim_buffers.into_iter().enumerate() {
        let reclaim_shared = Arc::clone(&state);
        match std::thread::Builder::new()
            .name(format!("cache2-reclaim-{worker_id}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || reclaim_worker(reclaim_shared, buffer, worker_id, reclaim_worker_count))
        {
            Ok(worker) => reclaim_workers.push(worker),
            Err(error) => {
                let _ = state.reclaim_control.stop();
                for worker in reclaim_workers {
                    let _ = worker.join();
                }
                for shard in &state.append_controls {
                    let _ = shard.request_drain(true);
                }
                for worker in append_workers {
                    let _ = worker.join();
                }
                state.staging.close();
                for engine in state.engines() {
                    let _ = engine.shutdown();
                }
                return Err(error);
            }
        }
    }
    if reclaim_on_start {
        state.reclaim_control.notify()?;
    }
    Ok(RuntimeWorkers {
        state,
        append_workers,
        reclaim_workers,
    })
}

fn build_engine_pool(
    handles: DataFileHandles,
    options: &RuntimeOptions,
    topology: IoPoolTopology,
    read_wait_enabled: bool,
) -> io::Result<Box<[Arc<IoEngine>]>> {
    let mut source = Some(handles);
    let engine_count = topology.engine_count();
    let mut engines = Vec::new();
    engines
        .try_reserve_exact(engine_count)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate I/O workers"))?;
    for engine in 0..engine_count {
        let worker_handles = if engine + 1 == engine_count {
            source.take().expect("last I/O worker owns file set")
        } else {
            source.as_ref().expect("I/O file set exists").try_clone()?
        };
        engines.push(build_file_engine(
            worker_handles,
            topology.engine_config(engine),
            options.stats.activity_counters,
            read_wait_enabled,
        )?);
    }
    Ok(engines.into_boxed_slice())
}

fn append_worker(state: Arc<RuntimeState>, shard_id: usize) {
    let control = Arc::clone(&state.append_controls[shard_id]);
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        append_worker_result(&state, shard_id, &control)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => io::Error::other("shard worker panicked"),
    };
    if state.activity_counters {
        RuntimeMetrics::increment(&state.metrics.io_failures);
    }
    let first_failure = state
        .metrics
        .lifecycle
        .swap(LIFECYCLE_FAILED, Ordering::AcqRel)
        != LIFECYCLE_FAILED;
    if first_failure {
        log::error!(
            target: "cache2::health",
            event = "cache_append_worker_failed",
            shard_id,
            error:% = error;
            "cache shard worker failed"
        );
    }
    state.regions.enter_miss_only();
    state.io_recovery.stop();
    control.fail(&error);
    // Wake engine admission in case another shard is blocked behind work that
    // can no longer make progress after this runtime entered miss-only.
    for engine in state.engines() {
        engine.wake_slot_waiters();
    }
    for shard in &state.append_controls {
        if !Arc::ptr_eq(shard, &control) {
            shard.fail(&error);
        }
    }
}

struct ReinsertShardCursor {
    first: usize,
    stride: usize,
    shard_count: usize,
    next: usize,
}

impl ReinsertShardCursor {
    fn new(worker_id: usize, worker_count: usize, shard_count: usize) -> Self {
        debug_assert!(worker_count != 0);
        debug_assert!(worker_id < worker_count);
        debug_assert!(worker_count <= shard_count);
        Self {
            first: worker_id,
            stride: worker_count,
            shard_count,
            next: worker_id,
        }
    }

    fn take(&mut self) -> usize {
        let shard = self.next;
        self.next = shard
            .checked_add(self.stride)
            .filter(|next| *next < self.shard_count)
            .unwrap_or(self.first);
        shard
    }
}

fn reclaim_worker(
    state: Arc<RuntimeState>,
    buffer: BufferLease,
    worker_id: usize,
    worker_count: usize,
) {
    let mut buffer = Some(buffer);
    let result = panic::catch_unwind(AssertUnwindSafe(|| {
        reclaim_worker_result(&state, &mut buffer, worker_id, worker_count)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => io::Error::other("Region reclaim worker panicked"),
    };
    if state.activity_counters {
        RuntimeMetrics::increment(&state.metrics.io_failures);
    }
    state
        .metrics
        .lifecycle
        .store(LIFECYCLE_FAILED, Ordering::Release);
    state.regions.enter_miss_only();
    state.io_recovery.stop();
    log::error!(
        target: "cache2::health",
        event = "cache_reclaim_worker_failed",
        worker_id,
        error:% = error;
        "cache Region reclaim worker failed"
    );
    for shard in &state.append_controls {
        shard.fail(&error);
    }
}

fn reclaim_worker_result(
    state: &RuntimeState,
    buffer: &mut Option<BufferLease>,
    worker_id: usize,
    worker_count: usize,
) -> io::Result<()> {
    let engine_index = route_hash(worker_id as u64, state.reclaim_engines.len());
    let engine = state.reclaim_engines.get(engine_index).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "reclaim worker has no I/O engine",
        )
    })?;
    let mut observed_generation = 0_u64;
    let mut reinsert_shards =
        ReinsertShardCursor::new(worker_id, worker_count, state.append_controls.len());
    while state.reclaim_control.wait(&mut observed_generation)? {
        loop {
            // Finish an already-started victim, but do not begin another once
            // shutdown has asked the worker to stop. A large clean-reserve
            // deficit must not turn close into a multi-Region reclaim pass.
            if state.reclaim_control.is_stopped()? {
                return Ok(());
            }
            let mut attempt = state.io_recovery.attempt();
            let Some(receipt) = state.regions.begin_reclaim()? else {
                break;
            };
            let used = usize::try_from(receipt.used_offset).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reclaim length does not fit usize",
                )
            })?;
            if used != 0 {
                let owned = buffer.take().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "reclaim worker lost its buffer")
                })?;
                let io_buffer = match IoBuffer::for_read(owned, used) {
                    Ok(buffer) => buffer,
                    Err(error) => return Err(error.error),
                };
                let absolute = state.regions.reclaim_absolute(receipt)?;
                // Reclaim owns a dedicated pool whose depth matches its worker
                // count. Use the bounded background wait so transient CAS
                // contention cannot turn a healthy cache miss-only; foreground
                // reads use their separately configured admission path.
                let request = submit_background_io(
                    engine.as_ref(),
                    IoOperation::read(io_buffer, absolute),
                    state.reclaim_io_timeout,
                    &mut attempt,
                )
                .map_err(|error| error.into_lease().0)?;
                let completion = request
                    .wait_with_io_recovery(engine.as_ref(), &mut attempt)
                    .map_err(|error| error.into_lease().0)?;
                let (result, returned) = completion.into_lease();
                let transferred = result?;
                let returned = returned.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim completion lost its buffer",
                    )
                })?;
                if transferred != used {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "reclaim Region read was short",
                    ));
                }
                *buffer = Some(returned);
            }
            let bytes = buffer
                .as_ref()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "reclaim worker lost its buffer")
                })?
                .prepared(used)
                .map_err(|()| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim buffer is not initialized",
                    )
                })?;
            // Keep one completion boundary per source Region while each
            // reclaimer rotates through a disjoint subset of append shards.
            let reinsert_shard = reinsert_shards.take();
            let preserve_hot = state.regions.reclaim_can_reinsert()?
                && !state
                    .io_recovery
                    .fill
                    .as_ref()
                    .is_some_and(|fill| fill.suppress_reinsertion());
            let reinsert_operation = if preserve_hot {
                state.operations.try_enter()
            } else {
                None
            };
            let mut accepting_reinserts = reinsert_operation.is_some();
            let mut staged_reinsert = false;
            let stats = state.regions.scan_reclaim(receipt, bytes, |record| {
                if !accepting_reinserts {
                    return Ok(false);
                }
                match state
                    .regions
                    .try_stage_reinsert(&state.staging, reinsert_shard, record)?
                {
                    RegionStageValue::Staged { .. } => {
                        staged_reinsert = true;
                        Ok(true)
                    }
                    RegionStageValue::NeedsProgress | RegionStageValue::NeedsRotation => {
                        accepting_reinserts = false;
                        Ok(false)
                    }
                }
            })?;
            if staged_reinsert {
                let generation = state.append_controls[reinsert_shard].request_drain(false)?;
                state.append_controls[reinsert_shard].wait_for_drain(generation)?;
            }
            state.regions.complete_reclaim(receipt)?;
            attempt.finish();
            drop(reinsert_operation);
            if state.activity_counters {
                state.metrics.record_reclaim(stats);
            }
            log::debug!(
                target: "cache2::reclaim",
                event = "cache_region_reclaimed",
                worker_id,
                region_id = receipt.region_id,
                reinsert_shard,
                preserve_hot,
                bytes = stats.bytes_read,
                records_scanned = stats.records_scanned,
                records_removed = stats.records_removed,
                reinsert_records = stats.reinsert_records,
                reinsert_bytes = stats.reinsert_bytes,
                reinsert_skipped = stats.reinsert_skipped,
                reinsert_budget_skipped = stats.reinsert_budget_skipped;
                "cache Region reclaimed"
            );
            for shard in &state.append_controls {
                shard.notify_if_running(WAKE_ROTATE)?;
            }
        }
    }
    Ok(())
}

fn append_worker_result(
    state: &RuntimeState,
    shard_id: usize,
    control: &AppendWorkerControl,
) -> io::Result<()> {
    let mut deadline = None;
    loop {
        let (flags, drain_generation, stop, timed_out) = wait_for_shard_work(control, deadline)?;
        let draining = drain_generation != 0;
        let force_flush = flags & WAKE_URGENT != 0 || timed_out || draining;
        let rotate = flags & WAKE_ROTATE != 0;

        match state.staging.shard_fill_snapshot(shard_id) {
            Ok(Some(fill)) => {
                if deadline.is_none() {
                    deadline = Some(Instant::now().checked_add(WRITE_FLUSH_DELAY).ok_or_else(
                        || invalid_runtime_config("partial flush deadline overflow"),
                    )?);
                }
                if force_flush || fill.bytes >= state.write_flush_threshold_bytes {
                    let essential = flags & (WAKE_URGENT | WAKE_ROTATE) != 0 || draining;
                    let bytes = fill.bytes as u64;
                    let records = u32::try_from(fill.records).unwrap_or(u32::MAX);
                    let charge = match &state.io_recovery.fill {
                        None => Some(FlushCharge {
                            records: 0,
                            byte_units: 0,
                        }),
                        Some(control) => {
                            control.try_acquire_flush_budget(bytes, records, essential)
                        }
                    };
                    if let Some(charge) = charge {
                        let engine = state.write_engine_for(shard_id as u64);
                        match state.regions.flush_staging_shard(
                            &state.staging,
                            engine.as_ref(),
                            shard_id,
                            &state.io_recovery,
                        )? {
                            Some(_) => deadline = None,
                            None => {
                                if let Some(control) = &state.io_recovery.fill {
                                    control.refund_flush_budget(charge);
                                }
                                deadline = Some(Instant::now() + STAGING_RETRY_DELAY);
                            }
                        }
                    } else {
                        deadline = Some(Instant::now() + FLUSH_RETRY);
                    }
                }
            }
            Ok(None) => {
                deadline = None;
                if rotate {
                    let rotated = state.regions.rotate_shard(shard_id)?;
                    if rotated && state.activity_counters {
                        RuntimeMetrics::increment(&state.metrics.region_rotations);
                    }
                    if rotated {
                        state.reclaim_control.notify()?;
                    }
                }
            }
            Err(StagingError::WouldBlock) => {
                deadline = Some(Instant::now() + STAGING_RETRY_DELAY);
            }
            Err(error) => return Err(staging_runtime_error(error)),
        }

        if draining {
            // Owner drains fence producers. A reclaimer requests the same
            // completion boundary without fencing foreground mutations, so a
            // short in-progress encode must be retried rather than treated as
            // structural staging failure.
            match state.staging.shard_fill_snapshot(shard_id) {
                Ok(Some(_)) => {
                    let engine = state.write_engine_for(shard_id as u64);
                    state.regions.flush_staging_shard(
                        &state.staging,
                        engine.as_ref(),
                        shard_id,
                        &state.io_recovery,
                    )?;
                }
                Ok(None) => {}
                Err(StagingError::WouldBlock) => {
                    deadline = Some(Instant::now() + STAGING_RETRY_DELAY);
                    continue;
                }
                Err(error) => return Err(staging_runtime_error(error)),
            }
            complete_shard_drain(control, drain_generation)?;
            if stop {
                return Ok(());
            }
        }
    }
}

fn wait_for_shard_work(
    control: &AppendWorkerControl,
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
    let flags = mem::take(&mut state.wake_flags);
    let drain_generation = if state.drain_requested > state.drain_completed {
        state.drain_requested
    } else {
        0
    };
    Ok((flags, drain_generation, state.stop, timed_out))
}

fn reject_staged_write(
    state: &RuntimeState,
    control: &AppendWorkerControl,
    flags: u8,
    operation: MutationGuard<'_>,
) -> io::Result<u64> {
    control.notify(flags)?;
    drop(operation);
    if state.activity_counters {
        RuntimeMetrics::increment(&state.metrics.write_buffer_rejections);
        state.metrics.record_write_rejection();
    }
    Err(write_overload_error())
}

fn complete_shard_drain(control: &AppendWorkerControl, generation: u64) -> io::Result<()> {
    let mut state = control.lock()?;
    state.drain_completed = state.drain_completed.max(generation);
    control.changed.notify_all();
    drop(state);
    control.async_changed.send_replace(());
    Ok(())
}

fn drain_shards(state: &RuntimeState, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(state.append_controls.len())
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate drain fence"))?;
    let mut first_error = None;
    for shard in &state.append_controls {
        match shard.request_drain(stop) {
            Ok(generation) => generations.push(Some(generation)),
            Err(error) => {
                first_error.get_or_insert(error);
                generations.push(None);
            }
        }
    }
    for (shard, generation) in state.append_controls.iter().zip(generations) {
        if let Some(generation) = generation
            && let Err(error) = shard.wait_for_drain(generation)
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

async fn drain_shards_async(state: &RuntimeState, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(state.append_controls.len())
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate drain fence"))?;
    let mut first_error = None;
    for shard in &state.append_controls {
        match shard.request_drain(stop) {
            Ok(generation) => generations.push(Some(generation)),
            Err(error) => {
                first_error.get_or_insert(error);
                generations.push(None);
            }
        }
    }
    for (shard, generation) in state.append_controls.iter().zip(generations) {
        if let Some(generation) = generation
            && let Err(error) = shard.wait_for_drain_async(generation).await
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn stop_workers(mut workers: RuntimeWorkers) -> io::Result<bool> {
    workers.state.io_recovery.stop();
    let drain = drain_shards(&workers.state, true);
    let mut join_error = None;
    for worker in workers.append_workers.drain(..) {
        if worker.join().is_err() {
            join_error.get_or_insert_with(|| io::Error::other("shard worker panicked"));
        }
    }
    if let Err(error) = workers.state.reclaim_control.stop() {
        join_error.get_or_insert(error);
    }
    for worker in workers.reclaim_workers.drain(..) {
        if worker.join().is_err() {
            join_error.get_or_insert_with(|| io::Error::other("Region reclaim worker panicked"));
        }
    }
    workers.state.staging.close();
    // Fence submission before observing idle engines. A read that already
    // passed the public close check must not appear between this snapshot
    // and a synchronous engine shutdown.
    for engine in workers.state.engines() {
        engine.stop_accepting_requests();
    }
    let in_flight = workers
        .state
        .engines()
        .map(|engine| engine.in_flight())
        .sum::<usize>();
    #[cfg(test)]
    {
        let after_snapshot = workers.state.after_io_snapshot.lock().unwrap().take();
        if let Some(after_snapshot) = after_snapshot {
            after_snapshot();
        }
    }
    let writes_in_flight = workers
        .state
        .engines()
        .map(|engine| engine.writes_in_flight())
        .sum::<usize>();
    let unfenced_before = workers
        .state
        .engines()
        .any(|engine| engine.has_unfenced_writes());
    // A request that missed its cancellation grace may still own a kernel
    // target and buffer. Joining that engine can wait forever. Retain only the
    // engine Arc; the runtime/regions can still be released normally.
    let skip_shutdown = in_flight != 0 || unfenced_before;
    let shutdown = if skip_shutdown {
        Ok(())
    } else {
        let mut result = Ok(());
        for engine in workers.state.engines() {
            if let Err(error) = engine.shutdown()
                && result.is_ok()
            {
                result = Err(error);
            }
        }
        result
    };
    let unfenced = unfenced_before
        || workers
            .state
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
            for engine in workers.state.engines() {
                mem::forget(Arc::clone(engine));
            }
        } else {
            for engine in workers.state.engines() {
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

fn reap_engine_after_target_fence(engine: &Arc<IoEngine>) {
    let reaper_engine = Arc::clone(engine);
    let spawn = std::thread::Builder::new()
        .name("cache2-io-reaper".to_owned())
        .stack_size(CACHE_THREAD_STACK_BYTES)
        .spawn(move || {
            let _ = reaper_engine.shutdown();
        });
    if spawn.is_err() {
        // The original workers is still alive while this fallback clone is
        // created, so a failed thread spawn cannot synchronously run the
        // engine's blocking Drop path.
        mem::forget(Arc::clone(engine));
    }
}

fn managed_memory_io_error(error: ManagedMemoryError) -> io::Error {
    let kind = match error {
        ManagedMemoryError::Invalid(_) => io::ErrorKind::InvalidInput,
        ManagedMemoryError::Allocation => io::ErrorKind::OutOfMemory,
    };
    io::Error::new(kind, error.to_string())
}

fn write_overload_error() -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, "write path is busy")
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
    io::Error::new(io::ErrorKind::NotConnected, "cache runtime is closed")
}

fn invalid_runtime_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::AtomicU64;
    use std::task::Context;
    use std::task::Wake;
    use std::task::Waker;

    use super::*;
    use crate::fixtures::TestFile;
    use crate::io::engine::IoEngine;

    static LANE_TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct ShardStateLockProbe {
        control: Arc<AppendWorkerControl>,
        observed_unlocked: AtomicBool,
    }

    impl Wake for ShardStateLockProbe {
        fn wake(self: Arc<Self>) {
            self.observed_unlocked
                .store(self.control.state.try_lock().is_ok(), Ordering::Release);
        }
    }

    #[test]
    fn read_lane_uses_one_bounded_alternate_on_primary_pressure() {
        let file = TestFile::new("read-lane");
        let io = file.io();
        let engines: Box<[Arc<IoEngine>]> = vec![
            Arc::new(IoEngine::for_test(Arc::clone(&io), 1).unwrap()),
            Arc::new(IoEngine::for_test(Arc::clone(&io), 1).unwrap()),
        ]
        .into_boxed_slice();
        let pressure_cursor = AtomicUsize::new(0);
        let primary = engines[0].try_reserve_read().unwrap();

        let (selected, alternate) = try_reserve_read_lane(&engines, 0, &pressure_cursor).unwrap();
        assert!(Arc::ptr_eq(&selected, &engines[1]));
        let error = match try_reserve_read_lane(&engines, 0, &pressure_cursor) {
            Ok(_) => panic!("both read lanes are already reserved"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

        drop(alternate);
        drop(primary);
        drop(selected);

        engines[0].stop_accepting_requests();
        let (selected, alternate) = try_reserve_read_lane(&engines, 0, &pressure_cursor).unwrap();
        assert!(Arc::ptr_eq(&selected, &engines[1]));
        drop(alternate);
        drop(selected);

        for engine in &engines {
            engine.shutdown().unwrap();
        }
        drop(engines);
        drop(io);
    }

    #[test]
    fn hot_read_route_rotates_pressure_fallback_across_all_lanes() {
        let file = TestFile::new("read-lane-rotation");
        let io = file.io();
        let engines: Box<[Arc<IoEngine>]> = (0..4)
            .map(|_| Arc::new(IoEngine::for_test(Arc::clone(&io), 1).unwrap()))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let pressure_cursor = AtomicUsize::new(0);
        let primary = engines[0].try_reserve_read().unwrap();

        for expected in 1..4 {
            let (selected, slot) = try_reserve_read_lane(&engines, 0, &pressure_cursor).unwrap();
            assert!(Arc::ptr_eq(&selected, &engines[expected]));
            drop(slot);
            drop(selected);
        }

        drop(primary);
        for engine in &engines {
            engine.shutdown().unwrap();
        }
        drop(engines);
        drop(io);
    }

    #[test]
    fn write_wake_is_only_needed_for_new_batches_and_threshold_crossings() {
        assert!(should_wake_write(0, 64, 4096));
        assert!(!should_wake_write(64, 128, 4096));
        assert!(should_wake_write(4032, 4096, 4096));
        assert!(!should_wake_write(4096, 4160, 4096));
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

    #[test]
    fn permanent_close_is_not_reopened_by_an_active_drain() {
        let gate = Arc::new(MutationGate::new());
        let mutation = gate.try_enter().unwrap();
        let drain = gate.begin_drain().unwrap();
        let closing_gate = Arc::clone(&gate);
        let close = std::thread::spawn(move || {
            closing_gate.start_close();
            closing_gate.wait_quiescent().unwrap();
        });

        while gate.state.load(Ordering::Acquire) & MUTATION_CLOSED == 0 {
            std::thread::yield_now();
        }
        drop(mutation);
        drain.wait().unwrap();
        drop(drain);
        close.join().unwrap();

        assert!(gate.try_enter().is_none());
        assert!(gate.begin_drain().is_err());
    }

    #[test]
    fn closing_during_drain_does_not_restore_running_lifecycle() {
        let lifecycle = AtomicU8::new(LIFECYCLE_RUNNING);
        let operations = MutationGate::new();
        let drain = operations.begin_drain().unwrap();
        let lifecycle_drain = LifecycleDrainingGuard::enter(&lifecycle, &operations);

        operations.start_close();
        operations.wait_quiescent().unwrap();
        drop(lifecycle_drain);
        drop(drain);

        assert_eq!(lifecycle.load(Ordering::Acquire), LIFECYCLE_DRAINING);
        assert!(operations.try_enter().is_none());
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
    fn shard_failure_wakes_async_waiters_after_releasing_state_lock() {
        let control = Arc::new(AppendWorkerControl::new());
        let probe = Arc::new(ShardStateLockProbe {
            control: Arc::clone(&control),
            observed_unlocked: AtomicBool::new(false),
        });
        let waker = Waker::from(Arc::clone(&probe));
        let mut context = Context::from_waker(&waker);
        let mut wait = Box::pin(control.wait_for_drain_async(1));

        assert!(wait.as_mut().poll(&mut context).is_pending());
        control.fail(&io::Error::other("test shard failure"));

        assert!(probe.observed_unlocked.load(Ordering::Acquire));
    }

    #[test]
    fn urgent_empty_shard_wake_is_consumed() {
        let control = AppendWorkerControl::new();
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
    fn reclaim_progress_does_not_fail_after_a_shard_stops() {
        let control = AppendWorkerControl::new();
        control.request_drain(true).unwrap();

        control.notify_if_running(WAKE_ROTATE).unwrap();
        assert_eq!(
            control.notify(WAKE_ROTATE).unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
    }

    #[tokio::test]
    async fn stopped_shard_rejects_new_drains_and_completes_accepted_drains() {
        let control = AppendWorkerControl::new();
        let first = control.request_drain(false).unwrap();
        let stop = control.request_drain(true).unwrap();

        for stopping in [false, true] {
            assert_eq!(
                control.request_drain(stopping).unwrap_err().kind(),
                io::ErrorKind::NotConnected
            );
        }
        assert_eq!(control.lock().unwrap().drain_requested, stop);

        complete_shard_drain(&control, stop).unwrap();
        control.wait_for_drain_async(first).await.unwrap();
    }

    #[test]
    fn one_reclaim_notification_reaches_every_worker() {
        let control = Arc::new(ReclaimControl::new());
        let ready = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let control = Arc::clone(&control);
            let ready = Arc::clone(&ready);
            workers.push(std::thread::spawn(move || {
                let mut observed_generation = 0;
                ready.wait();
                let notified = control.wait(&mut observed_generation).unwrap();
                (notified, observed_generation)
            }));
        }

        ready.wait();
        control.notify().unwrap();
        for worker in workers {
            assert_eq!(worker.join().unwrap(), (true, 1));
        }
    }

    #[test]
    fn transient_read_pressure_is_not_a_cache_failure() {
        for kind in [
            io::ErrorKind::OutOfMemory,
            io::ErrorKind::WouldBlock,
            io::ErrorKind::TimedOut,
            io::ErrorKind::Interrupted,
            io::ErrorKind::BrokenPipe,
        ] {
            assert!(is_read_pressure(kind));
        }
        assert!(!is_read_pressure(io::ErrorKind::InvalidData));
    }

    #[test]
    fn completion_timeouts_follow_read_wait_mode() {
        use crate::cache::session::CacheSession;
        use crate::config::runtime::IoEngineOptions;
        use crate::config::runtime::PosixIoOptions;
        use crate::region::index::packed::IndexEntry;
        use crate::region::index::packed::PackedLocation;
        use crate::region::persistence::RegionPaths;
        use crate::region::recovery::DATA_REGION_AREA_OFFSET;
        use crate::region::recovery::PersistentId;

        let id = LANE_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "cache2-completion-timeout-{}-{id}",
            std::process::id()
        ));
        let paths = RegionPaths::new(
            path.with_extension("cache"),
            path.with_extension("state"),
            path.with_extension("image"),
        );
        let data = DataSuperblock {
            generation: 1,
            cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
            data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
            geometry: DataGeometry {
                data_file_len: DataGeometry::expected_file_len(4096, 2).unwrap(),
                region_size: 4096,
                region_count: 2,
            },
            hash_seed: 3,
            storage_fingerprint: 4,
        };
        for wait in [Duration::ZERO, Duration::from_millis(1)] {
            let config = RuntimeOptions {
                io_engine: IoEngineOptions::Posix(PosixIoOptions {
                    read_workers: 1,
                    write_workers: 1,
                    reclaim_workers: 1,
                }),
                append_shards: 1,
                l1_capacity_bytes: 0,
                stats: crate::StatsOptions {
                    activity_counters: true,
                    ..Default::default()
                },
                read_admission: if wait.is_zero() {
                    ReadAdmission::Immediate
                } else {
                    ReadAdmission::Wait {
                        timeout: wait,
                        max_waiters: None,
                    }
                },
                ..RuntimeOptions::default()
            };
            let mut session =
                CacheSession::for_test_with_options(paths.clone(), data, 8, config).unwrap();
            let runtime = session.runtime().unwrap().clone();
            let MemoryLookup::Miss(read_token) = runtime.state.memory.lookup(7, b"key") else {
                panic!("empty L1 must miss");
            };
            let result = runtime.finish_get(
                CompletedGet {
                    read: ReadCompletion {
                        desc: ReadDesc {
                            hash: 7,
                            entry: IndexEntry {
                                location: PackedLocation::new(0, 0, 64).unwrap(),
                            },
                            region_generation: 1,
                            absolute: DATA_REGION_AREA_OFFSET,
                            read_len: 64,
                            record_range: 0..64,
                        },
                        result: Err(io::Error::from(io::ErrorKind::TimedOut)),
                        buffer: None,
                    },
                    read_token,
                    hash: 7,
                },
                b"key",
            );
            let snapshot = runtime.snapshot().unwrap();
            assert!(runtime.regions.is_healthy());
            session.close_fast().unwrap();
            if wait.is_zero() {
                assert!(matches!(result, Ok(None)));
                assert_eq!(snapshot.l2_read_busy_misses, 1);
                assert_eq!(snapshot.l2_read_overloads, 0);
            } else {
                assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
                assert_eq!(snapshot.l2_read_busy_misses, 0);
                assert_eq!(snapshot.l2_read_overloads, 1);
            }
        }
        std::fs::remove_file(paths.data).unwrap();
        std::fs::remove_file(paths.state).unwrap();
    }

    #[test]
    fn maximum_read_buffer_is_derived_from_runtime_limits() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let value_len = geometry.region_size as usize - RECORD_HEADER_SIZE;
        let record_len = required_record_bytes(0, value_len).unwrap();
        assert_eq!(u64::from(record_len), geometry.region_size);
        let entry = IndexEntry {
            location: PackedLocation::new(0, 0, record_len).unwrap(),
        };
        assert_eq!(
            describe_read(
                geometry,
                1,
                ReadCandidate {
                    entry,
                    region_generation: 1,
                },
                true,
            )
            .unwrap()
            .read_len,
            geometry.region_size as usize
        );
    }

    #[test]
    fn read_resource_misses_remain_separately_observable() {
        let metrics = RuntimeMetrics::new(1, crate::StatsOptions::default()).unwrap();
        let activity = metrics.activity(0);
        RuntimeMetrics::add(&activity.l2_misses, 2);
        RuntimeMetrics::increment(&activity.l2_read_memory_misses);
        RuntimeMetrics::increment(&activity.l2_read_busy_misses);
        metrics.record_read_overload();
        metrics.record_read_wait(Duration::from_nanos(7));
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
        assert_eq!(snapshot.l2_read_overloads, 1);
        assert_eq!(snapshot.l2_read_wait_ns, 7);
    }

    #[test]
    fn index_page_validation_state_is_fixed_memory_accounted() {
        let one_page = runtime_fixed_memory_bytes(INDEX_IMAGE_SLOTS_PER_PAGE, 2).unwrap();
        let two_pages = runtime_fixed_memory_bytes(INDEX_IMAGE_SLOTS_PER_PAGE + 1, 2).unwrap();

        assert_eq!(
            two_pages - one_page,
            INDEX_IMAGE_PAGE_SIZE + size_of::<AtomicU8>()
        );
    }

    #[test]
    fn reclaim_workers_rotate_over_disjoint_append_shards() {
        for shard_count in 1..=8 {
            for worker_count in 1..=shard_count {
                let mut owners = vec![None; shard_count];
                for worker_id in 0..worker_count {
                    let mut cursor = ReinsertShardCursor::new(worker_id, worker_count, shard_count);
                    for shard in (worker_id..shard_count).step_by(worker_count) {
                        assert_eq!(cursor.take(), shard);
                        assert_eq!(owners[shard].replace(worker_id), None);
                    }
                    assert_eq!(cursor.take(), worker_id);
                }
                assert!(owners.iter().all(Option::is_some));
            }
        }
    }
}

#[cfg(test)]
mod shutdown_tests;
