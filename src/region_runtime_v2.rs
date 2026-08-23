//! Self-owned steady-state runtime for RegionStore V2.
//!
//! Foreground writers encode directly into the fixed per-lane staging
//! buffers. Lane workers carry only coalesced control state, so queueing cannot
//! duplicate payload memory or let a benchmark generator inflate the measured
//! device path. A fixed age deadline publishes partial batches without adding
//! a durability sync; CLEAN remains the only steady-state durability boundary.

use std::io;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::io_backend::RuntimeFileSet;
use crate::io_engine::{IoEngine, build_auto_file_engine};
use crate::record_codec_v2::{hash_namespaced_key_v2, planned_record_bytes};
use crate::recovery_v2::DataSuperblockV2;
use crate::region_appender_v2::V2_WRITE_BATCH_BYTES;
use crate::region_staging_v2::{RegionStagingV2, StagingV2Error};
use crate::region_v2::{
    FileRegionCoreV2, RegionStageValueV2, RegionStagedValueV2, RegionValueReadV2,
};
use crate::resources::{
    BackpressurePolicy, ResourceBuildError, ResourceController, ResourceLimits,
};

const V2_IO_QUEUE_DEPTH: usize = 128;
const V2_READ_QUEUE_DEPTH: usize = 128;
const V2_WRITE_QUEUE_DEPTH: usize = 128;
pub(crate) const V2_READ_BUFFER_SLOTS: usize = 128;
const V2_MAX_KEY_BYTES: usize = 4 * 1024;
const V2_MAX_VALUE_BYTES: usize = 256 * 1024;
const V2_READ_BUFFER_BYTES: usize = 272 * 1024;
const V2_RUNTIME_MEMORY_BYTES: usize = 128 * 1024 * 1024;
const V2_PARTIAL_FLUSH_AGE: Duration = Duration::from_millis(1);
const V2_RETRY_AGE: Duration = Duration::from_micros(50);
const V2_BATCH_TARGET_BYTES: usize = V2_WRITE_BATCH_BYTES;

const WAKE_DATA: u8 = 1;
const WAKE_URGENT: u8 = 2;
const WAKE_ROTATE: u8 = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionPutV2 {
    /// Bytes are resident in the bounded lane staging buffer. The Hybrid L1
    /// remains the immediate read authority until the lane completion worker
    /// publishes the corresponding index entry.
    Buffered(RegionStagedValueV2),
}

pub(crate) struct RegionDataPlaneV2 {
    core: Arc<FileRegionCoreV2>,
    data: DataSuperblockV2,
    lifecycle: Mutex<DataPlaneLifecycleV2>,
    operations: RwLock<()>,
}

enum DataPlaneLifecycleV2 {
    Dormant(Option<RuntimeFileSet>),
    Running(RunningOwnerV2),
    Stopped,
}

struct RunningOwnerV2 {
    shared: Arc<RunningSharedV2>,
    workers: Vec<JoinHandle<()>>,
}

struct RunningSharedV2 {
    core: Arc<FileRegionCoreV2>,
    data: DataSuperblockV2,
    engine: Arc<dyn IoEngine>,
    resources: Arc<ResourceController>,
    staging: Arc<RegionStagingV2>,
    lanes: Box<[Arc<LaneControlV2>]>,
}

#[derive(Clone)]
struct LaneFailureV2 {
    kind: io::ErrorKind,
    message: Arc<str>,
}

impl LaneFailureV2 {
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
struct LaneControlStateV2 {
    wake_flags: u8,
    progress: u64,
    drain_requested: u64,
    drain_completed: u64,
    stop: bool,
    failure: Option<LaneFailureV2>,
}

struct LaneControlV2 {
    state: Mutex<LaneControlStateV2>,
    changed: Condvar,
}

impl LaneControlV2 {
    fn new() -> Self {
        Self {
            state: Mutex::new(LaneControlStateV2::default()),
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

    fn wait_for_progress(&self, observed: u64) -> io::Result<()> {
        let mut state = self.lock()?;
        while state.progress == observed && state.failure.is_none() && !state.stop {
            state = self
                .changed
                .wait(state)
                .map_err(|_| poisoned_runtime_error())?;
        }
        if let Some(failure) = &state.failure {
            return Err(failure.to_error());
        }
        if state.progress == observed {
            return Err(closed_runtime_error());
        }
        Ok(())
    }

    fn request_drain(&self, stop: bool) -> io::Result<u64> {
        let mut state = self.lock()?;
        state.drain_requested = state
            .drain_requested
            .checked_add(1)
            .ok_or_else(|| io::Error::other("V2 lane drain generation exhausted"))?;
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
            .get_or_insert_with(|| LaneFailureV2::from_error(error));
        self.changed.notify_all();
    }

    fn lock(&self) -> io::Result<std::sync::MutexGuard<'_, LaneControlStateV2>> {
        self.state.lock().map_err(|_| poisoned_runtime_error())
    }
}

impl RegionDataPlaneV2 {
    pub(crate) fn new(
        core: Arc<FileRegionCoreV2>,
        data: DataSuperblockV2,
        files: RuntimeFileSet,
    ) -> Self {
        Self {
            core,
            data,
            lifecycle: Mutex::new(DataPlaneLifecycleV2::Dormant(Some(files))),
            operations: RwLock::new(()),
        }
    }

    pub(crate) fn put(
        &self,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at: u64,
    ) -> io::Result<RegionPutV2> {
        if key.len() > V2_MAX_KEY_BYTES || value.len() > V2_MAX_VALUE_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "V2 file-chunk entry exceeds the 4 KiB key or 256 KiB value limit",
            ));
        }
        let _ = planned_record_bytes(namespace_id, key.len(), value.len())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        let _operation = self
            .operations
            .read()
            .map_err(|_| poisoned_runtime_error())?;
        let running = self.running()?;
        let _permit = running
            .resources
            .begin_write_permit()
            .map_err(overload_runtime_error)?;
        let hash = hash_namespaced_key_v2(self.data.hash_seed, namespace_id, key);
        let lane_id = usize::try_from(hash % running.lanes.len() as u64)
            .map_err(|_| io::Error::other("V2 lane selection overflow"))?;
        let control = &running.lanes[lane_id];

        loop {
            match self.core.try_stage_value(
                &running.staging,
                lane_id,
                self.data.hash_seed,
                namespace_id,
                key,
                value,
                expires_at,
            )? {
                RegionStageValueV2::Staged(staged) => {
                    control.notify(WAKE_DATA)?;
                    return Ok(RegionPutV2::Buffered(staged));
                }
                RegionStageValueV2::NeedsFlush => {
                    wait_for_lane_action(control, WAKE_URGENT)?;
                }
                RegionStageValueV2::NeedsRotation => {
                    wait_for_lane_action(control, WAKE_ROTATE | WAKE_URGENT)?;
                }
                RegionStageValueV2::Busy => {
                    wait_for_lane_action(control, WAKE_URGENT)?;
                }
            }
        }
    }

    pub(crate) fn get(
        &self,
        namespace_id: u32,
        key: &[u8],
        now_unix_ms: u64,
    ) -> io::Result<Option<RegionValueReadV2>> {
        if key.len() > V2_MAX_KEY_BYTES {
            return Ok(None);
        }
        let _operation = self
            .operations
            .read()
            .map_err(|_| poisoned_runtime_error())?;
        let running = self.running()?;
        let Some(point) = self
            .core
            .begin_value_read(self.data.hash_seed, namespace_id, key)?
        else {
            return Ok(None);
        };
        let resources = running
            .resources
            .begin_read()
            .map_err(overload_runtime_error)?;
        let (queue, buffer) = resources.into_parts();
        let result = self.core.read_value_from_point(
            running.engine.as_ref(),
            self.data.geometry,
            buffer,
            point,
            namespace_id,
            key,
            now_unix_ms,
        );
        drop(queue);
        match result {
            // MissOnly is a cache availability state, not an application data
            // error. The operation that trips the one-way health latch and all
            // later reads therefore fail open as cache misses. Resource
            // overload remains explicit while the core is still healthy.
            Err(_) if !self.core.is_healthy() => Ok(None),
            result => result,
        }
    }

    /// Completes and publishes every record admitted before this call. This is
    /// an I/O completion barrier, not an fdatasync durability boundary.
    pub(crate) fn drain(&self) -> io::Result<()> {
        let _exclusive = self
            .operations
            .write()
            .map_err(|_| poisoned_runtime_error())?;
        let running = self.running()?;
        drain_lanes(&running, false)
    }

    /// Fences admission, drains all workers, and shuts down the I/O engine.
    /// The return value asks the backend to retain flock for process lifetime
    /// because an issued mutation could not be fenced.
    pub(crate) fn shutdown(&self) -> io::Result<bool> {
        let _exclusive = self
            .operations
            .write()
            .map_err(|_| poisoned_runtime_error())?;
        let owner = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .map_err(|_| poisoned_runtime_error())?;
            match std::mem::replace(&mut *lifecycle, DataPlaneLifecycleV2::Stopped) {
                DataPlaneLifecycleV2::Dormant(_) | DataPlaneLifecycleV2::Stopped => {
                    return Ok(false);
                }
                DataPlaneLifecycleV2::Running(owner) => owner,
            }
        };
        stop_running(owner)
    }

    fn running(&self) -> io::Result<Arc<RunningSharedV2>> {
        let mut lifecycle = self
            .lifecycle
            .lock()
            .map_err(|_| poisoned_runtime_error())?;
        if let DataPlaneLifecycleV2::Running(owner) = &*lifecycle {
            return Ok(Arc::clone(&owner.shared));
        }
        let files = match &mut *lifecycle {
            DataPlaneLifecycleV2::Dormant(files) => {
                files.take().ok_or_else(closed_runtime_error)?
            }
            DataPlaneLifecycleV2::Stopped => return Err(closed_runtime_error()),
            DataPlaneLifecycleV2::Running(_) => unreachable!(),
        };
        let owner = start_running(Arc::clone(&self.core), self.data, files)?;
        let shared = Arc::clone(&owner.shared);
        *lifecycle = DataPlaneLifecycleV2::Running(owner);
        Ok(shared)
    }
}

fn start_running(
    core: Arc<FileRegionCoreV2>,
    data: DataSuperblockV2,
    files: RuntimeFileSet,
) -> io::Result<RunningOwnerV2> {
    let lane_count = core.append_lane_count();
    let base_memory = core.runtime_base_memory_bytes()?;
    let memory_budget = base_memory
        .checked_add(V2_RUNTIME_MEMORY_BYTES)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "V2 memory budget overflow"))?;
    let resources = Arc::new(
        ResourceController::try_new(ResourceLimits {
            memory_budget_bytes: memory_budget,
            base_memory_bytes: base_memory,
            max_buffer_bytes: V2_READ_BUFFER_BYTES,
            read_queue_depth: V2_READ_QUEUE_DEPTH,
            write_queue_depth: V2_WRITE_QUEUE_DEPTH,
            read_buffer_slots: V2_READ_BUFFER_SLOTS,
            write_buffer_slots: 1,
            control_concurrency: lane_count,
            // Cache callers must never hold the shutdown read barrier while
            // waiting for an externally retained hit buffer to return.
            backpressure: BackpressurePolicy::Reject,
            write_budget_bytes_per_second: None,
        })
        .map_err(resource_build_io_error)?,
    );
    let usable_region = usize::try_from(
        data.geometry
            .region_size
            .saturating_sub(crate::recovery_v2::REGION_HEADER_SIZE_V2 as u64),
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "V2 Region size is too large"))?;
    let chunk_bytes =
        usable_region.min(V2_WRITE_BATCH_BYTES) & !(crate::resources::BUFFER_ALIGNMENT - 1);
    let staging = Arc::new(
        RegionStagingV2::try_new(
            lane_count,
            chunk_bytes,
            data.geometry.region_size,
            &resources,
        )
        .map_err(resource_build_io_error)?,
    );
    let engine = build_auto_file_engine(files, V2_IO_QUEUE_DEPTH)?;
    let mut lanes = Vec::new();
    lanes.try_reserve_exact(lane_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate V2 lane controls",
        )
    })?;
    lanes.resize_with(lane_count, || Arc::new(LaneControlV2::new()));
    let shared = Arc::new(RunningSharedV2 {
        core,
        data,
        engine,
        resources,
        staging,
        lanes: lanes.into_boxed_slice(),
    });
    let mut workers = Vec::new();
    workers.try_reserve_exact(lane_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate V2 worker handles",
        )
    })?;
    for lane_id in 0..lane_count {
        let worker_shared = Arc::clone(&shared);
        match std::thread::Builder::new()
            .name(format!("cache-rs-v2-lane-{lane_id}"))
            .spawn(move || lane_worker(worker_shared, lane_id))
        {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                for lane in &shared.lanes {
                    let _ = lane.request_drain(true);
                }
                for worker in workers {
                    let _ = worker.join();
                }
                shared.staging.close();
                let _ = shared.engine.shutdown();
                return Err(error);
            }
        }
    }
    Ok(RunningOwnerV2 { shared, workers })
}

fn lane_worker(shared: Arc<RunningSharedV2>, lane_id: usize) {
    let control = Arc::clone(&shared.lanes[lane_id]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        lane_worker_result(&shared, lane_id, &control)
    }));
    let error = match result {
        Ok(Ok(())) => return,
        Ok(Err(error)) => error,
        Err(_) => io::Error::other("V2 lane worker panicked"),
    };
    shared.core.enter_miss_only();
    control.fail(&error);
    // Wake engine admission in case another lane is blocked behind work that
    // can no longer make progress after this runtime entered miss-only.
    shared.engine.wake_admission_waiters();
    for lane in &shared.lanes {
        if !Arc::ptr_eq(lane, &control) {
            lane.fail(&error);
        }
    }
}

fn lane_worker_result(
    shared: &RunningSharedV2,
    lane_id: usize,
    control: &LaneControlV2,
) -> io::Result<()> {
    let mut deadline = None;
    loop {
        let (flags, drain_generation, stop, timed_out) = wait_for_lane_work(control, deadline)?;
        let draining = drain_generation != 0;
        let force_flush = flags & WAKE_URGENT != 0 || timed_out || draining;
        let rotate = flags & WAKE_ROTATE != 0;

        match shared.staging.lane_fill_snapshot(lane_id) {
            Ok(Some(fill)) => {
                deadline.get_or_insert_with(|| Instant::now() + V2_PARTIAL_FLUSH_AGE);
                if force_flush || fill.bytes >= V2_BATCH_TARGET_BYTES {
                    shared.core.flush_staging_lane(
                        &shared.staging,
                        shared.engine.as_ref(),
                        lane_id,
                    )?;
                    deadline = None;
                    advance_lane_progress(control)?;
                }
            }
            Ok(None) => {
                deadline = None;
                if rotate {
                    let buffer = shared
                        .resources
                        .metadata_buffer()
                        .map_err(overload_runtime_error)?;
                    shared.core.rotate_append_lane(
                        shared.engine.as_ref(),
                        shared.data.geometry,
                        lane_id,
                        buffer,
                    )?;
                    advance_lane_progress(control)?;
                } else if flags & WAKE_DATA != 0 {
                    // A producer may have been followed by an urgent worker
                    // completion before this coalesced wake was observed.
                    advance_lane_progress(control)?;
                }
            }
            Err(StagingV2Error::Encoding | StagingV2Error::Submitted) => {
                deadline = Some(Instant::now() + V2_RETRY_AGE);
            }
            Err(error) => return Err(staging_runtime_error(error)),
        }

        if draining {
            // Producers are fenced by the owner's write barrier. One forced
            // pass therefore empties the lane completely.
            if shared
                .staging
                .lane_fill_snapshot(lane_id)
                .map_err(staging_runtime_error)?
                .is_some()
            {
                shared
                    .core
                    .flush_staging_lane(&shared.staging, shared.engine.as_ref(), lane_id)?;
                advance_lane_progress(control)?;
            }
            complete_lane_drain(control, drain_generation)?;
            if stop {
                return Ok(());
            }
        }
    }
}

fn wait_for_lane_work(
    control: &LaneControlV2,
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

fn wait_for_lane_action(control: &LaneControlV2, flags: u8) -> io::Result<()> {
    let observed = control.progress()?;
    control.notify(flags)?;
    control.wait_for_progress(observed)
}

fn advance_lane_progress(control: &LaneControlV2) -> io::Result<()> {
    let mut state = control.lock()?;
    state.progress = state.progress.saturating_add(1);
    control.changed.notify_all();
    Ok(())
}

fn complete_lane_drain(control: &LaneControlV2, generation: u64) -> io::Result<()> {
    let mut state = control.lock()?;
    state.drain_completed = state.drain_completed.max(generation);
    state.progress = state.progress.saturating_add(1);
    control.changed.notify_all();
    Ok(())
}

fn drain_lanes(shared: &RunningSharedV2, stop: bool) -> io::Result<()> {
    let mut generations = Vec::new();
    generations
        .try_reserve_exact(shared.lanes.len())
        .map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate V2 drain fence")
        })?;
    for lane in &shared.lanes {
        generations.push(lane.request_drain(stop)?);
    }
    let mut first_error = None;
    for (lane, generation) in shared.lanes.iter().zip(generations) {
        if let Err(error) = lane.wait_for_drain(generation) {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn stop_running(mut owner: RunningOwnerV2) -> io::Result<bool> {
    let drain = drain_lanes(&owner.shared, true);
    let mut join_error = None;
    for worker in owner.workers.drain(..) {
        if worker.join().is_err() {
            join_error.get_or_insert_with(|| io::Error::other("V2 lane worker panicked"));
        }
    }
    owner.shared.staging.close();
    let in_flight = owner.shared.engine.in_flight();
    let in_flight_mutations = owner.shared.engine.in_flight_mutations();
    let unfenced_before = owner.shared.engine.has_unfenced_mutations();
    // A request that missed its cancellation grace may still own a kernel
    // target and buffer. Joining that engine can wait forever. Retain only the
    // engine Arc; the runtime/core can still be released normally.
    let skip_shutdown = in_flight != 0 || unfenced_before;
    let shutdown = if skip_shutdown {
        Ok(())
    } else {
        owner.shared.engine.shutdown()
    };
    let unfenced = unfenced_before || owner.shared.engine.has_unfenced_mutations();
    let result = drain
        .and_then(|()| join_error.map_or(Ok(()), Err))
        .and(shutdown);
    if skip_shutdown || unfenced {
        // A merely pending target gets a detached reaper: close returns now,
        // while eventual target completion still shuts the engine down and
        // reclaims its fd/thread/buffer set. A sticky fatal unfenced mutation
        // has no trustworthy future fence and remains process-lifetime state.
        if unfenced {
            std::mem::forget(Arc::clone(&owner.shared.engine));
        } else {
            reap_engine_after_target_fence(&owner.shared.engine);
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
        .name("cache-rs-v2-io-reaper".to_owned())
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
    io::Error::new(io::ErrorKind::WouldBlock, error.to_string())
}

fn staging_runtime_error(error: StagingV2Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn poisoned_runtime_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "V2 runtime synchronization is poisoned",
    )
}

fn closed_runtime_error() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "V2 data plane is closed")
}
