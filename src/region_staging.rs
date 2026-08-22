//! Bounded lane-local residency for managed buffered Region writes.
//!
//! Each append lane owns two fixed chunks: one Active and at most one
//! Flushing. The I/O engine receives a third aligned copy, allowing reads to
//! keep using immutable resident bytes until the write CQE makes the physical
//! location safe. No structure grows beyond the configured chunk capacity.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Condvar, Mutex, MutexGuard};

use crate::format::RECORD_ALIGNMENT;
use crate::index::IndexEntry;
use crate::resources::{
    BufferLease, DedicatedBufferPool, ResourceBuildError, ResourceController,
    RuntimeMemoryReservation,
};

pub(crate) const MAX_STAGING_CHUNK_BYTES: usize = 4 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct StagedRecord {
    pub(crate) hash: u64,
    pub(crate) entry: IndexEntry,
}

pub(crate) struct StagedFlush {
    pub(crate) lane_id: usize,
    pub(crate) span_id: u64,
    pub(crate) buffer: BufferLease,
    pub(crate) length: usize,
    pub(crate) absolute: u64,
    pub(crate) records: usize,
}

pub(crate) enum FlushCommand {
    Write(StagedFlush),
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageAppend {
    Appended,
    NeedsSeal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ResidentLookup {
    Found,
    NotFound,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagingError {
    Failed,
    Closed,
    Invariant(&'static str),
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegionStagingSnapshot {
    pub(crate) chunk_bytes: u64,
    pub(crate) resident_bytes: u64,
    pub(crate) flushing_bytes: u64,
    pub(crate) sealed_spans: u64,
    pub(crate) sealed_bytes: u64,
    pub(crate) completion_live_records: u64,
    pub(crate) completion_live_bytes: u64,
    pub(crate) completion_obsolete_records: u64,
    pub(crate) completion_obsolete_bytes: u64,
}

impl fmt::Display for StagingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed => formatter.write_str("Region staging is failed"),
            Self::Closed => formatter.write_str("Region staging is closed"),
            Self::Invariant(message) => formatter.write_str(message),
        }
    }
}

struct ResidentChunk {
    span_id: u64,
    region_id: u32,
    region_incarnation: u32,
    epoch: u32,
    start_offset: u32,
    absolute: u64,
    bytes: Vec<u8>,
    records: Vec<StagedRecord>,
}

impl ResidentChunk {
    fn try_empty(chunk_bytes: usize, record_capacity: usize) -> Result<Self, ResourceBuildError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(chunk_bytes)
            .map_err(|_| ResourceBuildError::Allocation)?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(record_capacity)
            .map_err(|_| ResourceBuildError::Allocation)?;
        Ok(Self {
            span_id: 0,
            region_id: 0,
            region_incarnation: 0,
            epoch: 0,
            start_offset: 0,
            absolute: 0,
            bytes,
            records,
        })
    }

    fn reset(&mut self) {
        self.span_id = 0;
        self.region_id = 0;
        self.region_incarnation = 0;
        self.epoch = 0;
        self.start_offset = 0;
        self.absolute = 0;
        self.bytes.clear();
        self.records.clear();
    }

    fn contains(&self, epoch: u32, incarnation: u32, entry: IndexEntry) -> Option<usize> {
        if self.bytes.is_empty()
            || self.epoch != epoch
            || self.region_id != entry.location.region_id()
            || self.region_incarnation != incarnation
        {
            return None;
        }
        let relative = entry.location.offset().checked_sub(self.start_offset)? as usize;
        let end = relative.checked_add(entry.location.record_len() as usize)?;
        (end <= self.bytes.len()).then_some(relative)
    }
}

struct LaneState {
    active: ResidentChunk,
    flushing: Option<ResidentChunk>,
    spare: Option<ResidentChunk>,
    next_span_id: u64,
    failed: bool,
    closed: bool,
}

struct StagingLane {
    state: Mutex<LaneState>,
    changed: Condvar,
    io_pool: DedicatedBufferPool,
    flush_tx: SyncSender<FlushCommand>,
}

#[derive(Default)]
struct StagingCounters {
    sealed_spans: AtomicU64,
    sealed_bytes: AtomicU64,
    completion_live_records: AtomicU64,
    completion_live_bytes: AtomicU64,
    completion_obsolete_records: AtomicU64,
    completion_obsolete_bytes: AtomicU64,
}

pub(crate) struct RegionStaging {
    lanes: Vec<StagingLane>,
    chunk_bytes: usize,
    counters: StagingCounters,
    _memory: RuntimeMemoryReservation,
}

impl RegionStaging {
    pub(crate) fn try_new(
        lane_count: usize,
        chunk_bytes: usize,
        resources: &ResourceController,
    ) -> Result<(Self, Vec<Receiver<FlushCommand>>), ResourceBuildError> {
        if lane_count == 0
            || chunk_bytes == 0
            || chunk_bytes % crate::resources::BUFFER_ALIGNMENT != 0
            || chunk_bytes % RECORD_ALIGNMENT != 0
        {
            return Err(ResourceBuildError::Invalid(
                "Region staging size must be a non-zero aligned chunk",
            ));
        }
        let record_capacity = chunk_bytes / RECORD_ALIGNMENT;
        let resident_bytes = chunk_bytes
            .checked_add(
                record_capacity
                    .checked_mul(std::mem::size_of::<StagedRecord>())
                    .ok_or(ResourceBuildError::Allocation)?,
            )
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(ResourceBuildError::Allocation)?;
        let per_lane = resident_bytes
            .checked_add(chunk_bytes)
            .ok_or(ResourceBuildError::Allocation)?;
        let reserved = per_lane
            .checked_mul(lane_count)
            .ok_or(ResourceBuildError::Allocation)?;
        let memory = resources.reserve_runtime_memory(reserved)?;

        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(lane_count)
            .map_err(|_| ResourceBuildError::Allocation)?;
        let mut receivers = Vec::new();
        receivers
            .try_reserve_exact(lane_count)
            .map_err(|_| ResourceBuildError::Allocation)?;
        for _ in 0..lane_count {
            let active = ResidentChunk::try_empty(chunk_bytes, record_capacity)?;
            let spare = ResidentChunk::try_empty(chunk_bytes, record_capacity)?;
            let io_pool = DedicatedBufferPool::try_new(1, chunk_bytes)?;
            let (flush_tx, flush_rx) = mpsc::sync_channel(1);
            lanes.push(StagingLane {
                state: Mutex::new(LaneState {
                    active,
                    flushing: None,
                    spare: Some(spare),
                    next_span_id: 1,
                    failed: false,
                    closed: false,
                }),
                changed: Condvar::new(),
                io_pool,
                flush_tx,
            });
            receivers.push(flush_rx);
        }
        Ok((
            Self {
                lanes,
                chunk_bytes,
                counters: StagingCounters::default(),
                _memory: memory,
            },
            receivers,
        ))
    }

    pub(crate) const fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    pub(crate) fn snapshot(&self) -> RegionStagingSnapshot {
        let mut resident_bytes = 0_u64;
        let mut flushing_bytes = 0_u64;
        for lane in &self.lanes {
            let state = lock_unpoisoned(&lane.state);
            let active = usize_to_u64(state.active.bytes.len());
            let flushing = state
                .flushing
                .as_ref()
                .map_or(0, |chunk| usize_to_u64(chunk.bytes.len()));
            resident_bytes = resident_bytes
                .saturating_add(active)
                .saturating_add(flushing);
            flushing_bytes = flushing_bytes.saturating_add(flushing);
        }
        RegionStagingSnapshot {
            chunk_bytes: usize_to_u64(self.chunk_bytes),
            resident_bytes,
            flushing_bytes,
            sealed_spans: self.counters.sealed_spans.load(Ordering::Relaxed),
            sealed_bytes: self.counters.sealed_bytes.load(Ordering::Relaxed),
            completion_live_records: self
                .counters
                .completion_live_records
                .load(Ordering::Relaxed),
            completion_live_bytes: self.counters.completion_live_bytes.load(Ordering::Relaxed),
            completion_obsolete_records: self
                .counters
                .completion_obsolete_records
                .load(Ordering::Relaxed),
            completion_obsolete_bytes: self
                .counters
                .completion_obsolete_bytes
                .load(Ordering::Relaxed),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn append_batch(
        &self,
        lane_id: usize,
        region_id: u32,
        region_incarnation: u32,
        epoch: u32,
        start_offset: u32,
        absolute: u64,
        encoded: &[u8],
        records: &[StagedRecord],
    ) -> Result<StageAppend, StagingError> {
        if encoded.is_empty()
            || encoded.len() > self.chunk_bytes
            || records.is_empty()
            || records.len() > encoded.len() / RECORD_ALIGNMENT
        {
            return Err(StagingError::Invariant(
                "managed staging batch exceeds its fixed bounds",
            ));
        }
        let lane = self.lane(lane_id)?;
        let mut state = lock_unpoisoned(&lane.state);
        Self::ensure_open(&state)?;
        let active = &mut state.active;
        if !active.bytes.is_empty() {
            let expected_offset = u64::from(active.start_offset)
                .checked_add(active.bytes.len() as u64)
                .ok_or(StagingError::Invariant("staging offset overflow"))?;
            let expected_absolute = active
                .absolute
                .checked_add(active.bytes.len() as u64)
                .ok_or(StagingError::Invariant("staging absolute offset overflow"))?;
            if active.region_id != region_id
                || active.region_incarnation != region_incarnation
                || active.epoch != epoch
                || expected_offset != u64::from(start_offset)
                || expected_absolute != absolute
                || active.bytes.len() + encoded.len() > self.chunk_bytes
            {
                return Ok(StageAppend::NeedsSeal);
            }
        } else {
            active.region_id = region_id;
            active.region_incarnation = region_incarnation;
            active.epoch = epoch;
            active.start_offset = start_offset;
            active.absolute = absolute;
        }
        if active.bytes.len() + encoded.len() > active.bytes.capacity()
            || active.records.len() + records.len() > active.records.capacity()
        {
            return Err(StagingError::Invariant(
                "managed staging allocation exceeded its reservation",
            ));
        }
        active.bytes.extend_from_slice(encoded);
        active.records.extend_from_slice(records);
        Ok(StageAppend::Appended)
    }

    pub(crate) fn seal_lane(&self, lane_id: usize) -> Result<bool, StagingError> {
        let lane = self.lane(lane_id)?;
        let mut state = lock_unpoisoned(&lane.state);
        loop {
            Self::ensure_open(&state)?;
            if state.flushing.is_none() {
                break;
            }
            state = wait_unpoisoned(&lane.changed, state);
        }
        if state.active.bytes.is_empty() {
            return Ok(false);
        }
        let mut io = lane.io_pool.acquire().ok_or(StagingError::Closed)?;
        let length = state.active.bytes.len();
        let mut sealed = state.spare.take().ok_or(StagingError::Invariant(
            "staging lane lost its spare resident chunk",
        ))?;
        std::mem::swap(&mut sealed, &mut state.active);
        sealed.span_id = state.next_span_id;
        state.next_span_id = state
            .next_span_id
            .checked_add(1)
            .ok_or(StagingError::Invariant("staging span id overflow"))?;
        io.prepared_mut(length)
            .map_err(|()| StagingError::Invariant("staging I/O buffer is undersized"))?
            .copy_from_slice(&sealed.bytes);
        let job = StagedFlush {
            lane_id,
            span_id: sealed.span_id,
            buffer: io,
            length,
            absolute: sealed.absolute,
            records: sealed.records.len(),
        };
        state.flushing = Some(sealed);
        atomic_saturating_add(&self.counters.sealed_spans, 1);
        atomic_saturating_add(&self.counters.sealed_bytes, usize_to_u64(length));
        drop(state);
        if lane.flush_tx.send(FlushCommand::Write(job)).is_err() {
            let mut state = lock_unpoisoned(&lane.state);
            state.failed = true;
            lane.changed.notify_all();
            return Err(StagingError::Failed);
        }
        Ok(true)
    }

    pub(crate) fn copy_record(
        &self,
        hash: u64,
        epoch: u32,
        incarnation: u32,
        entry: IndexEntry,
        output: &mut [u8],
    ) -> Result<ResidentLookup, StagingError> {
        let lane = self.lane(hash as usize % self.lanes.len())?;
        let state = lock_unpoisoned(&lane.state);
        Self::ensure_open(&state)?;
        if output.len() != entry.location.record_len() as usize {
            return Err(StagingError::Invariant(
                "resident read buffer does not match the indexed record",
            ));
        }
        let resident = state
            .active
            .contains(epoch, incarnation, entry)
            .map(|start| (&state.active, start))
            .or_else(|| {
                state.flushing.as_ref().and_then(|chunk| {
                    chunk
                        .contains(epoch, incarnation, entry)
                        .map(|start| (chunk, start))
                })
            });
        if let Some((chunk, start)) = resident {
            output.copy_from_slice(&chunk.bytes[start..start + output.len()]);
            return Ok(ResidentLookup::Found);
        }
        Ok(ResidentLookup::NotFound)
    }

    pub(crate) fn finish_success(
        &self,
        lane_id: usize,
        span_id: u64,
        mut make_on_device: impl FnMut(StagedRecord) -> bool,
    ) -> Result<(), StagingError> {
        let lane = self.lane(lane_id)?;
        let mut state = lock_unpoisoned(&lane.state);
        let flushing = state.flushing.as_ref().ok_or(StagingError::Invariant(
            "staging completion has no resident span",
        ))?;
        if flushing.span_id != span_id {
            return Err(StagingError::Invariant(
                "staging completion span identity mismatch",
            ));
        }
        let mut live_records = 0_u64;
        let mut live_bytes = 0_u64;
        let mut obsolete_records = 0_u64;
        let mut obsolete_bytes = 0_u64;
        for record in flushing.records.iter().copied() {
            let bytes = u64::from(record.entry.location.record_len());
            if make_on_device(record) {
                live_records = live_records.saturating_add(1);
                live_bytes = live_bytes.saturating_add(bytes);
            } else {
                obsolete_records = obsolete_records.saturating_add(1);
                obsolete_bytes = obsolete_bytes.saturating_add(bytes);
            }
        }
        let mut finished = state
            .flushing
            .take()
            .expect("checked staging flushing span");
        finished.reset();
        if state.spare.replace(finished).is_some() {
            return Err(StagingError::Invariant(
                "staging completion found an occupied spare chunk",
            ));
        }
        atomic_saturating_add(&self.counters.completion_live_records, live_records);
        atomic_saturating_add(&self.counters.completion_live_bytes, live_bytes);
        atomic_saturating_add(&self.counters.completion_obsolete_records, obsolete_records);
        atomic_saturating_add(&self.counters.completion_obsolete_bytes, obsolete_bytes);
        lane.changed.notify_all();
        Ok(())
    }

    pub(crate) fn finish_failure(&self, lane_id: usize, span_id: u64) -> Result<(), StagingError> {
        let lane = self.lane(lane_id)?;
        let mut state = lock_unpoisoned(&lane.state);
        let flushing = state.flushing.as_ref().ok_or(StagingError::Invariant(
            "failed staging completion has no resident span",
        ))?;
        if flushing.span_id != span_id {
            return Err(StagingError::Invariant(
                "failed staging completion span identity mismatch",
            ));
        }
        let mut finished = state
            .flushing
            .take()
            .expect("checked staging flushing span");
        finished.reset();
        state.spare = Some(finished);
        state.failed = true;
        lane.changed.notify_all();
        Ok(())
    }

    pub(crate) fn drain_lane(&self, lane_id: usize) -> Result<(), StagingError> {
        self.seal_lane(lane_id)?;
        self.wait_lane_drained(lane_id)
    }

    pub(crate) fn drain_all(&self) -> Result<(), StagingError> {
        // Submit every lane before waiting so a flush/checkpoint/close fence
        // preserves device queue depth instead of serializing lanes at QD=1.
        for lane_id in 0..self.lanes.len() {
            self.seal_lane(lane_id)?;
        }
        for lane_id in 0..self.lanes.len() {
            self.wait_lane_drained(lane_id)?;
        }
        Ok(())
    }

    fn wait_lane_drained(&self, lane_id: usize) -> Result<(), StagingError> {
        let lane = self.lane(lane_id)?;
        let mut state = lock_unpoisoned(&lane.state);
        while state.flushing.is_some() && !state.failed && !state.closed {
            state = wait_unpoisoned(&lane.changed, state);
        }
        Self::ensure_open(&state)?;
        if !state.active.bytes.is_empty() {
            return Err(StagingError::Invariant(
                "staging drain left an active resident chunk",
            ));
        }
        Ok(())
    }

    pub(crate) fn shutdown(&self) -> bool {
        let mut failed = false;
        for lane in &self.lanes {
            failed |= lane.flush_tx.send(FlushCommand::Shutdown).is_err();
        }
        failed
    }

    pub(crate) fn close(&self) {
        for lane in &self.lanes {
            let mut state = lock_unpoisoned(&lane.state);
            state.closed = true;
            lane.io_pool.close();
            lane.changed.notify_all();
        }
    }

    fn lane(&self, lane_id: usize) -> Result<&StagingLane, StagingError> {
        self.lanes
            .get(lane_id)
            .ok_or(StagingError::Invariant("staging lane id is out of bounds"))
    }

    fn ensure_open(state: &LaneState) -> Result<(), StagingError> {
        if state.failed {
            Err(StagingError::Failed)
        } else if state.closed {
            Err(StagingError::Closed)
        } else {
            Ok(())
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn atomic_saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(amount))
    });
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
