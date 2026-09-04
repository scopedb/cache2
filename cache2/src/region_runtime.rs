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

//! Self-owned steady-state runtime for RegionStore .
//!
//! Foreground writers encode directly into the fixed per-shard write
//! buffers. Shard workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory or let a benchmark generator inflate the measured
//! device path. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use asyncband::semaphore::{OwnedSemaphorePermit, Semaphore};
use asyncband::watch;

mod metrics;
mod plan;

use self::metrics::RuntimeMetrics;
#[cfg(test)]
use self::plan::{
    IO_QUEUE_ENTRY_RESERVATION_BYTES, RUNTIME_CONTROL_RESERVATION_BYTES,
    runtime_topology_memory_bytes,
};

use crate::format::MAX_KEY_SIZE;
use crate::hashing::route_hash;
use crate::io_backend::RuntimeFileSet;
use crate::io_engine::{
    IoBuffer, IoEngine, IoOperation, ReadSlot, ReadSlotWaiter, build_file_engine, submit_cache_io,
};
use crate::memory::{MemoryLookup, MemoryReadToken, MemoryStore, MemoryValue};
use crate::record_codec::{hash_key, required_record_bytes};
use crate::recovery::DataSuperblock;
use crate::region::core::{FileRegionCore, RegionStageValue, RegionValueRead};
use crate::region_reader::{PendingRead, ReadCompletion, ReadPlan, plan_read};
use crate::region_staging::{RegionStaging, StagingError};
use crate::resources::{
    BufferLease, CACHE_THREAD_STACK_BYTES, ResourceBuildError, ResourceController, ResourceLimits,
};
use crate::runtime_config::{IoMode, IoPoolTopology, RuntimeConfig};
use crate::snapshot::{
    CacheIoDirectionSnapshot, CacheIoSnapshot, CacheSnapshot, DetailedCacheSnapshot,
};

#[cfg(test)]
use crate::index_storage::{INDEX_IMAGE_PAGE_SIZE, INDEX_IMAGE_SLOTS_PER_PAGE};
#[cfg(test)]
use crate::memory::MemoryMetricsSnapshot;
#[cfg(test)]
use crate::recovery::DataGeometry;
#[cfg(test)]
use crate::region::core::runtime_fixed_memory_bytes;
#[cfg(test)]
use crate::resources::ManagedMemorySnapshot;

const WRITE_FLUSH_DELAY: Duration = Duration::from_millis(1);
const _RETRY_AGE: Duration = Duration::from_micros(50);
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

pub(crate) enum HybridValueRead {
    L1(MemoryValue),
    L2(RegionValueRead),
    /// An L2 hit copied into the bounded L1 tier. The public tier remains
    /// L2 because that is where this lookup was served, but the transient
    /// aligned read allocation can be released before `get` returns.
    PromotedL2(MemoryValue),
}

enum PreparedGet {
    Complete(Option<HybridValueRead>),
    Pending(PendingGet),
    Waiting(WaitingGet),
}

struct PendingGet {
    engine: Arc<dyn IoEngine>,
    read: PendingRead,
    read_token: MemoryReadToken,
    hash: u64,
}

struct WaitingGet {
    engine: Arc<dyn IoEngine>,
    slot_waiter: ReadSlotWaiter,
    plan: ReadPlan,
    read_token: MemoryReadToken,
    hash: u64,
    deadline: Instant,
    waiter_permit: OwnedSemaphorePermit,
}

struct ReservedGet {
    engine: Arc<dyn IoEngine>,
    slot: ReadSlot,
    plan: ReadPlan,
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
            plan,
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
            plan,
            read_token,
            hash,
        })
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

#[derive(Clone)]
pub(crate) struct RegionDataPlane {
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    config: RuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
    shared: Arc<RunningShared>,
    owner: Arc<Mutex<Option<RunningOwner>>>,
    // Fences write admission for drain, flush, and shutdown. Reads do not
    // participate because they cannot extend the set of records being fenced.
    operations: Arc<MutationGate>,
}

struct RunningOwner {
    shared: Arc<RunningShared>,
    shard_workers: Vec<JoinHandle<()>>,
    reclaim_workers: Vec<JoinHandle<()>>,
}

struct RunningShared {
    core: Arc<FileRegionCore>,
    read_engines: Box<[Arc<dyn IoEngine>]>,
    read_lane_cursor: AtomicUsize,
    read_waiters: Option<Arc<Semaphore>>,
    write_engines: Box<[Arc<dyn IoEngine>]>,
    reclaim_engines: Box<[Arc<dyn IoEngine>]>,
    reclaim_control: ReclaimControl,
    resources: Arc<ResourceController>,
    metrics: Arc<RuntimeMetrics>,
    memory: Arc<MemoryStore>,
    staging: Arc<RegionStaging>,
    operations: Arc<MutationGate>,
    shards: Box<[Arc<ShardControl>]>,
    write_flush_threshold_bytes: usize,
    align_reads_for_direct_io: bool,
    statistics: bool,
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

impl RunningShared {
    fn write_engine_for(&self, route: u64) -> &Arc<dyn IoEngine> {
        &self.write_engines[route_hash(route, self.write_engines.len())]
    }

    fn try_reserve_read(&self, route: u64) -> io::Result<(Arc<dyn IoEngine>, ReadSlot)> {
        try_reserve_read_lane(&self.read_engines, route, &self.read_lane_cursor)
    }

    fn try_queue_read(
        &self,
        route: u64,
        plan: ReadPlan,
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
            plan,
            read_token,
            hash: route,
            deadline,
            waiter_permit,
        })
    }

    fn engines(&self) -> impl Iterator<Item = &Arc<dyn IoEngine>> {
        self.read_engines
            .iter()
            .chain(self.write_engines.iter())
            .chain(self.reclaim_engines.iter())
    }
}

fn try_reserve_read_lane(
    engines: &[Arc<dyn IoEngine>],
    route: u64,
    pressure_cursor: &AtomicUsize,
) -> io::Result<(Arc<dyn IoEngine>, ReadSlot)> {
    let reserve = |lane: usize| -> io::Result<(Arc<dyn IoEngine>, ReadSlot)> {
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
    async_changed: watch::Sender<()>,
}

impl ShardControl {
    fn new() -> Self {
        let (async_changed, _) = watch::channel(());
        Self {
            state: Mutex::new(ShardControlState::default()),
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
        let mut state = match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        };
        state
            .failure
            .get_or_insert_with(|| ShardFailure::from_error(error));
        self.changed.notify_all();
        self.async_changed.send_replace(());
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
        core.configure_reclaim_workers(config.reclaim_io_max_in_flight())?;
        core.set_index_statistics_enabled(config.statistics);
        let metrics = Arc::new(RuntimeMetrics::new(core.shard_count())?);
        let operations = Arc::new(MutationGate::new());
        let running = start_running(
            Arc::clone(&core),
            data,
            files,
            config.clone(),
            Arc::clone(&metrics),
            Arc::clone(&operations),
        )?;
        let shared = Arc::clone(&running.shared);
        Ok(Self {
            core,
            data,
            config,
            metrics,
            shared,
            owner: Arc::new(Mutex::new(Some(running))),
            operations,
        })
    }

    pub(crate) fn start_close(&self) {
        self.operations.start_close();
    }

    pub(crate) fn put(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.put_with_l1::<true>(key, value)
    }

    pub(crate) fn put_l2(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
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
        let running = &self.shared;
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
            RegionStageValue::Staged {
                seqno,
                previous_bytes,
                current_bytes,
            } => {
                if ADMIT_L1 {
                    let _published = running.memory.publish(hash, key, value, seqno);
                } else {
                    // Prevent an older exact-key L1 value from indefinitely
                    // shadowing the prefetched L2 record. Contention remains a
                    // valid best-effort stale outcome.
                    let _removed = running.memory.delete(hash, key, seqno);
                }
                if should_wake_write(
                    previous_bytes,
                    current_bytes,
                    running.write_flush_threshold_bytes,
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
        let running = &self.shared;
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
            PreparedGet::Waiting(_) => Err(io::Error::other(
                "bounded read waiting requires the async get path",
            )),
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
            PreparedGet::Waiting(waiting) => {
                let wait_started = self.config.statistics.then(Instant::now);
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
        if !self.config.statistics {
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
            plan,
            read_token,
            hash,
        } = reserved;
        let Some(buffer) = self.shared.resources.try_read_buffer(plan.read_len) else {
            if self.config.statistics {
                self.metrics.record_read_overload();
            }
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "L2 read could not reserve its bounded buffer after waiting",
            ));
        };
        match self
            .core
            .submit_value_read_from_plan(engine.as_ref(), slot, buffer, plan)
        {
            Ok(read) => Ok(Some(PendingGet {
                engine,
                read,
                read_token,
                hash,
            })),
            Err(_) if !self.core.is_healthy() => {
                if self.config.statistics {
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

    fn prepare_get(&self, key: &[u8]) -> io::Result<PreparedGet> {
        if key.len() > MAX_KEY_SIZE {
            if self.config.statistics {
                let activity = self.metrics.activity(0);
                RuntimeMetrics::increment(&activity.l1_misses);
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        }
        let running = &self.shared;
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
        let Some(candidate) = self.core.begin_value_read(hash) else {
            if let Some(activity) = activity {
                RuntimeMetrics::increment(&activity.l2_misses);
            }
            return Ok(PreparedGet::Complete(None));
        };
        let plan = match plan_read(
            self.data.geometry,
            hash,
            candidate,
            running.align_reads_for_direct_io,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.core
                    .enter_miss_only_with_error("record_read_plan_invalid", &error);
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
            Err(error)
                if error.kind() == io::ErrorKind::WouldBlock
                    && !self.config.read_io_wait_timeout.is_zero() =>
            {
                let waiting = running
                    .try_queue_read(hash, plan, read_token, self.config.read_io_wait_timeout)
                    .inspect_err(|_| {
                        if running.statistics {
                            running.metrics.record_read_overload();
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
                self.core
                    .enter_miss_only_with_error("read_engine_reservation_failed", &error);
                if running.statistics {
                    RuntimeMetrics::increment(&running.metrics.io_failures);
                }
                if let Some(activity) = activity {
                    RuntimeMetrics::increment(&activity.l2_misses);
                }
                return Ok(PreparedGet::Complete(None));
            }
        };
        let Some(buffer) = running.resources.try_read_buffer(plan.read_len) else {
            if !self.config.read_io_wait_timeout.is_zero() {
                if running.statistics {
                    running.metrics.record_read_overload();
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
                if !self.config.read_io_wait_timeout.is_zero() {
                    if running.statistics {
                        running.metrics.record_read_overload();
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
        let running = &self.shared;
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
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle, &self.operations);
        let running = &self.shared;
        drain_shards(running, false)
    }

    pub(crate) async fn drain_async(&self) -> io::Result<()> {
        let operations = self.operations.begin_drain()?;
        operations.wait_async().await;
        let _draining = LifecycleDrainingGuard::enter(&self.metrics.lifecycle, &self.operations);
        let running = &self.shared;
        drain_shards_async(running, false).await
    }

    pub(crate) fn snapshot(&self) -> io::Result<CacheSnapshot> {
        let running = &self.shared;
        Ok(self.snapshot_running(running))
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<DetailedCacheSnapshot> {
        let running = &self.shared;
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
        snapshot.io = aggregate_io_stats(
            &running.read_engines,
            &running.write_engines,
            &running.reclaim_engines,
        );
        snapshot
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// The return value asks the backend to retain flock for process lifetime
    /// because an issued write or flush could not be fenced.
    pub(crate) fn shutdown(&self) -> io::Result<bool> {
        self.operations.start_close();
        self.operations.wait_quiescent()?;
        let _ = self.metrics.lifecycle.compare_exchange(
            LIFECYCLE_RUNNING,
            LIFECYCLE_DRAINING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let running = self
            .owner
            .lock()
            .map_err(|_| poisoned_runtime_error())?
            .take()
            .ok_or_else(closed_runtime_error)?;
        let retain_lock = stop_running(running)?;
        Ok(retain_lock)
    }

    #[cfg(test)]
    pub(crate) fn reserve_read_slot_for_test(&self) -> ReadSlot {
        self.shared
            .try_reserve_read(0)
            .map(|(_, slot)| slot)
            .expect("test read slot is available")
    }

    #[cfg(test)]
    pub(crate) fn poison_shard_for_test(&self, shard_id: usize) {
        let shard = self.shared.shards.get(shard_id).expect("test shard exists");
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
    reclaim_engines: &[Arc<dyn IoEngine>],
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

fn start_running(
    core: Arc<FileRegionCore>,
    data: DataSuperblock,
    files: RuntimeFileSet,
    config: RuntimeConfig,
    metrics: Arc<RuntimeMetrics>,
    operations: Arc<MutationGate>,
) -> io::Result<RunningOwner> {
    let shard_count = core.shard_count();
    let l1_entry_capacity = config.l1_entry_capacity(data.geometry, core.index_slot_count())?;
    let l1_metadata_bytes = MemoryStore::allocation_bytes(
        config.l1_capacity_bytes,
        l1_entry_capacity,
        config.l1_shards,
        config.l1_eviction_policy,
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
        config.l1_eviction_policy,
        config.statistics,
    )?);
    let reclaim_worker_count = config.reclaim_io_max_in_flight();
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
        reclaim_buffers.push(resources.try_read_buffer(usable_region).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate a fixed Region reclaim buffer",
            )
        })?);
    }
    let reclaim_files = files.try_clone()?;
    let write_files = files.try_clone()?;
    let read_wait_enabled = !config.read_io_wait_timeout.is_zero();
    let read_engines =
        build_engine_pool(files, &config, config.read_io_topology(), read_wait_enabled)?;
    let read_waiters =
        read_wait_enabled.then(|| Arc::new(Semaphore::new(config.read_io_wait_capacity())));
    let write_engines = build_engine_pool(write_files, &config, config.write_io_topology(), false)?;
    let reclaim_engines =
        build_engine_pool(reclaim_files, &config, config.reclaim_io_topology(), false)?;
    let mut shards = Vec::new();
    shards.try_reserve_exact(shard_count).map_err(|_| {
        io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate shard controls")
    })?;
    shards.resize_with(shard_count, || Arc::new(ShardControl::new()));
    let shared = Arc::new(RunningShared {
        core,
        read_engines,
        read_lane_cursor: AtomicUsize::new(0),
        read_waiters,
        write_engines,
        reclaim_engines,
        reclaim_control: ReclaimControl::new(),
        resources,
        metrics,
        memory,
        staging,
        operations,
        shards: shards.into_boxed_slice(),
        write_flush_threshold_bytes: config.write_flush_threshold_bytes,
        align_reads_for_direct_io: config.io_mode == IoMode::Direct,
        statistics: config.statistics,
    });
    // Inspect the recovered queue before workers can contend with foreground
    // mutations. Fresh caches have no sealed Regions and need no wakeup.
    let reclaim_on_start = shared.core.reclaim_needed()?;
    let mut reclaim_workers = Vec::new();
    reclaim_workers
        .try_reserve_exact(reclaim_worker_count)
        .map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate worker handles")
        })?;
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
    for (worker_id, buffer) in reclaim_buffers.into_iter().enumerate() {
        let reclaim_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("cache2-reclaim-{worker_id}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || reclaim_worker(reclaim_shared, buffer, worker_id, reclaim_worker_count))
        {
            Ok(worker) => reclaim_workers.push(worker),
            Err(error) => {
                let _ = shared.reclaim_control.stop();
                for worker in reclaim_workers {
                    let _ = worker.join();
                }
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
    if reclaim_on_start {
        shared.reclaim_control.notify()?;
    }
    Ok(RunningOwner {
        shared,
        shard_workers,
        reclaim_workers,
    })
}

fn build_engine_pool(
    files: RuntimeFileSet,
    config: &RuntimeConfig,
    topology: IoPoolTopology,
    read_wait_enabled: bool,
) -> io::Result<Box<[Arc<dyn IoEngine>]>> {
    let mut source = Some(files);
    let engine_count = topology.engine_count;
    let mut engines = Vec::new();
    engines
        .try_reserve_exact(engine_count)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate I/O workers"))?;
    let posix_workers = if config.io_engine().is_posix() {
        topology.max_in_flight
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
            topology.depth_for_engine(engine),
            posix_workers,
            config.io_engine(),
            topology.io_uring,
            config.statistics,
            read_wait_enabled,
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
    shared: Arc<RunningShared>,
    buffer: BufferLease,
    worker_id: usize,
    worker_count: usize,
) {
    let mut buffer = Some(buffer);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        reclaim_worker_result(&shared, &mut buffer, worker_id, worker_count)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => io::Error::other("Region reclaim worker panicked"),
    };
    if shared.statistics {
        RuntimeMetrics::increment(&shared.metrics.io_failures);
    }
    shared
        .metrics
        .lifecycle
        .store(LIFECYCLE_FAILED, Ordering::Release);
    shared.core.enter_miss_only();
    log::error!(
        target: "cache2::health",
        event = "cache_reclaim_worker_failed",
        worker_id,
        error:% = error;
        "cache Region reclaim worker failed"
    );
    for shard in &shared.shards {
        shard.fail(&error);
    }
}

fn reclaim_worker_result(
    shared: &RunningShared,
    buffer: &mut Option<BufferLease>,
    worker_id: usize,
    worker_count: usize,
) -> io::Result<()> {
    let engine_index = route_hash(worker_id as u64, shared.reclaim_engines.len());
    let engine = shared.reclaim_engines.get(engine_index).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "reclaim worker has no I/O engine",
        )
    })?;
    let mut observed_generation = 0_u64;
    let mut reinsert_shards =
        ReinsertShardCursor::new(worker_id, worker_count, shared.shards.len());
    while shared.reclaim_control.wait(&mut observed_generation)? {
        loop {
            // Finish an already-started victim, but do not begin another once
            // shutdown has asked the worker to stop. A large clean-reserve
            // deficit must not turn close into a multi-Region reclaim pass.
            if shared.reclaim_control.is_stopped()? {
                return Ok(());
            }
            let Some(receipt) = shared.core.begin_reclaim()? else {
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
                let absolute = shared.core.reclaim_absolute(receipt)?;
                // Reclaim owns a dedicated pool whose depth matches its worker
                // count. Use the bounded background wait so transient CAS
                // contention cannot turn a healthy cache miss-only; foreground
                // reads use their separately configured admission path.
                let request =
                    submit_cache_io(engine.as_ref(), IoOperation::read(io_buffer, absolute))
                        .map_err(|error| error.into_lease().0)?;
                let completion = request
                    .wait(engine.as_ref())
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
            let preserve_hot = shared.core.reclaim_can_reinsert()?;
            let reinsert_operation = if preserve_hot {
                shared.operations.try_enter()
            } else {
                None
            };
            let mut accepting_reinserts = reinsert_operation.is_some();
            let mut staged_reinsert = false;
            let stats = shared.core.scan_reclaim(receipt, bytes, |record| {
                if !accepting_reinserts {
                    return Ok(false);
                }
                match shared
                    .core
                    .try_stage_reinsert(&shared.staging, reinsert_shard, record)?
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
                let generation = shared.shards[reinsert_shard].request_drain(false)?;
                shared.shards[reinsert_shard].wait_for_drain(generation)?;
            }
            shared.core.complete_reclaim(receipt)?;
            drop(reinsert_operation);
            if shared.statistics {
                shared.metrics.record_reclaim(stats);
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
            for shard in &shared.shards {
                shard.notify_if_running(WAKE_ROTATE)?;
            }
        }
    }
    Ok(())
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
                    if rotated {
                        shared.reclaim_control.notify()?;
                    }
                }
            }
            Err(StagingError::WouldBlock) => {
                deadline = Some(Instant::now() + _RETRY_AGE);
            }
            Err(error) => return Err(staging_runtime_error(error)),
        }

        if draining {
            // Owner drains fence producers. A reclaimer requests the same
            // completion boundary without fencing foreground mutations, so a
            // short in-progress encode must be retried rather than treated as
            // structural staging failure.
            match shared.staging.shard_fill_snapshot(shard_id) {
                Ok(Some(_)) => {
                    let engine = shared.write_engine_for(shard_id as u64);
                    shared
                        .core
                        .flush_staging_shard(&shared.staging, engine.as_ref(), shard_id)?;
                }
                Ok(None) => {}
                Err(StagingError::WouldBlock) => {
                    deadline = Some(Instant::now() + _RETRY_AGE);
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
    control.async_changed.send_replace(());
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
    if let Err(error) = owner.shared.reclaim_control.stop() {
        join_error.get_or_insert(error);
    }
    for worker in owner.reclaim_workers.drain(..) {
        if worker.join().is_err() {
            join_error.get_or_insert_with(|| io::Error::other("Region reclaim worker panicked"));
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
            | io::ErrorKind::BrokenPipe
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
    use std::sync::Barrier;
    use std::sync::atomic::AtomicU64;

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
        drop(backend);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn hot_read_route_rotates_pressure_fallback_across_all_lanes() {
        let id = LANE_TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cache2-read-lane-rotation-{}-{id}.cache",
            std::process::id()
        ));
        let backend: Arc<dyn IoBackend> = Arc::new(FileBackend::open(&path).unwrap());
        let engines: Box<[Arc<dyn IoEngine>]> = (0..4)
            .map(|_| {
                Arc::new(BackendIoEngine::new(Arc::clone(&backend), 1).unwrap())
                    as Arc<dyn IoEngine>
            })
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
        drop(backend);
        std::fs::remove_file(path).unwrap();
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
    fn reclaim_progress_does_not_fail_after_a_shard_stops() {
        let control = ShardControl::new();
        control.request_drain(true).unwrap();

        control.notify_if_running(WAKE_ROTATE).unwrap();
        assert_eq!(
            control.notify(WAKE_ROTATE).unwrap_err().kind(),
            io::ErrorKind::NotConnected
        );
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
        };
        assert_eq!(
            plan_read(
                geometry,
                1,
                crate::region_reader::ReadCandidate {
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
        let metrics = RuntimeMetrics::new(1).unwrap();
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
    fn optional_read_wait_queue_is_memory_accounted() {
        let base = RuntimeConfig::default()
            .with_io_engine(crate::runtime_config::IoEngine::Posix(
                crate::runtime_config::PosixIoConfig::new(7, 4, 1),
            ))
            .with_read_io_wait_capacity(11);
        let no_wait = runtime_topology_memory_bytes(4, &base).unwrap();
        let with_wait = runtime_topology_memory_bytes(
            4,
            &base.with_read_io_wait_timeout(Duration::from_millis(1)),
        )
        .unwrap();

        assert_eq!(with_wait - no_wait, 11 * IO_QUEUE_ENTRY_RESERVATION_BYTES);
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
    fn four_tib_memory_plan_covers_the_complete_production_shape() {
        const GIB: usize = 1024 * 1024 * 1024;
        const INDEX_SLOTS: usize = 512 * 1024 * 1024;
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(32 * 1024 * 1024, 128 * 1024).unwrap(),
            region_size: 32 * 1024 * 1024,
            region_count: 128 * 1024,
        };
        let index_slots = INDEX_SLOTS;
        let base = RuntimeConfig::default()
            .with_l1_capacity_bytes(10 * GIB)
            .with_managed_memory_limit_bytes(15 * GIB)
            .with_io_engine(crate::runtime_config::IoEngine::Posix(
                crate::runtime_config::PosixIoConfig::new(4, 4, 2),
            ))
            .with_l1_shards(64);
        let entry_capacity = base.l1_entry_capacity(geometry, index_slots).unwrap();
        assert_eq!(entry_capacity, 2_621_440);
        base.validate_memory_plan(geometry, index_slots, 4).unwrap();
        let too_small = base.clone().with_managed_memory_limit_bytes(14 * GIB);
        assert_eq!(
            too_small
                .validate_memory_plan(geometry, index_slots, 4)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let metadata = MemoryStore::allocation_bytes(
            base.l1_capacity_bytes,
            entry_capacity,
            base.l1_shards,
            base.l1_eviction_policy,
        )
        .unwrap();
        assert_eq!(metadata, 130 * 1024 * 1024);

        let s3fifo = base
            .clone()
            .with_l1_eviction_policy(crate::runtime_config::L1EvictionPolicy::S3Fifo);
        s3fifo
            .validate_memory_plan(geometry, index_slots, 4)
            .unwrap();
        assert_eq!(
            too_small
                .with_l1_eviction_policy(crate::runtime_config::L1EvictionPolicy::S3Fifo)
                .validate_memory_plan(geometry, index_slots, 4)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        let s3fifo_metadata = MemoryStore::allocation_bytes(
            s3fifo.l1_capacity_bytes,
            entry_capacity,
            s3fifo.l1_shards,
            s3fifo.l1_eviction_policy,
        )
        .unwrap();
        assert_eq!(s3fifo_metadata - metadata, 110 * 1024 * 1024);
        assert_eq!(s3fifo_metadata, 240 * 1024 * 1024);
    }

    #[test]
    fn each_additional_reclaimer_is_fully_memory_accounted() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let base = RuntimeConfig::default()
            .with_append_shards(4)
            .with_l1_capacity_bytes(0);
        let (_, base_minimum) = base.memory_plan_bytes(geometry, 4, 0).unwrap();
        let (_, parallel_minimum) = base
            .with_io_engine(crate::runtime_config::IoEngine::Posix(
                crate::runtime_config::PosixIoConfig::new(4, 4, 2),
            ))
            .memory_plan_bytes(geometry, 4, 0)
            .unwrap();

        assert_eq!(
            parallel_minimum - base_minimum,
            geometry.region_size as usize
                + 2 * CACHE_THREAD_STACK_BYTES
                + IO_QUEUE_ENTRY_RESERVATION_BYTES
                + RUNTIME_CONTROL_RESERVATION_BYTES
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
