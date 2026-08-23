//! Fixed-memory, zero-copy staging for the RegionStore V2 append path.
//!
//! This module is intentionally independent from the legacy managed-buffered
//! staging path. Region manager receipts are the only span authority.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use crate::format::{RECORD_HEADER_SIZE, RecordHeader, RecordKind};
use crate::index::{IndexEntry, MAX_RECORD_LEN, PackedLocation};
use crate::io_backend::DIRECT_IO_ALIGNMENT;
use crate::io_engine::IoBuffer;
use crate::recovery_v2::{DATA_REGION_AREA_OFFSET_V2, RECORD_ALIGNMENT_V2, REGION_HEADER_SIZE_V2};
use crate::region_appender_v2::V2_WRITE_BATCH_BYTES;
use crate::region_manager_v2::{
    RegionAppendReservationV2, RegionPaddingReceiptV2, RegionWriteSpanV2,
};
use crate::resources::{
    BUFFER_ALIGNMENT, BufferLease, DedicatedBufferPool, ResourceBuildError, ResourceController,
    RuntimeMemoryReservation,
};

pub(crate) const MAX_STAGING_RECORDS_V2: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageAppendV2 {
    Appended,
    NeedsSeal,
}

/// One V2 record whose exact index identity is published only after its
/// containing device span completes. The descriptor stays owned by the
/// completion path; staging never calls into the index while holding a lane
/// lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StagedRecordV2 {
    pub(crate) hash: u64,
    pub(crate) entry: IndexEntry,
}

/// A zero-copy V2 write job. `buffer` is the lane's former fill lease and is
/// therefore 4 KiB aligned. The I/O completion must return this exact buffer
/// and the record vector to [`RegionStagingV2::finish_success`] or
/// [`RegionStagingV2::finish_failure`].
pub(crate) struct StagedWriteV2 {
    pub(crate) span: RegionWriteSpanV2,
    pub(crate) buffer: IoBuffer,
    pub(crate) absolute: u64,
    pub(crate) records: Vec<StagedRecordV2>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagingV2Error {
    Failed,
    Closed,
    InvalidLane,
    WouldBlock,
    StaleReceipt,
    Invariant(&'static str),
}

impl fmt::Display for StagingV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed => formatter.write_str("V2 Region staging is failed"),
            Self::Closed => formatter.write_str("V2 Region staging is closed"),
            Self::InvalidLane => formatter.write_str("V2 Region staging lane is out of bounds"),
            Self::WouldBlock => formatter.write_str("V2 Region staging lane is busy"),
            Self::StaleReceipt => formatter.write_str("V2 Region staging receipt is stale"),
            Self::Invariant(message) => formatter.write_str(message),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StagingV2EncodeError<E> {
    Staging(StagingV2Error),
    Encode(E),
}

impl<E> From<StagingV2Error> for StagingV2EncodeError<E> {
    fn from(error: StagingV2Error) -> Self {
        Self::Staging(error)
    }
}

struct FillChunkV2 {
    buffer: Option<BufferLease>,
    records: Vec<StagedRecordV2>,
    cache_epoch: u32,
    region_id: u32,
    region_incarnation: u32,
    start_offset: u64,
    end_offset: u64,
    max_seqno: u64,
}

impl FillChunkV2 {
    fn new(buffer: BufferLease, records: Vec<StagedRecordV2>) -> Self {
        Self {
            buffer: Some(buffer),
            records,
            cache_epoch: 0,
            region_id: 0,
            region_incarnation: 0,
            start_offset: 0,
            end_offset: 0,
            max_seqno: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn used(&self) -> Option<usize> {
        usize::try_from(self.end_offset.checked_sub(self.start_offset)?).ok()
    }

    fn reset(&mut self, buffer: BufferLease) {
        self.records.clear();
        self.buffer = Some(buffer);
        self.cache_epoch = 0;
        self.region_id = 0;
        self.region_incarnation = 0;
        self.start_offset = 0;
        self.end_offset = 0;
        self.max_seqno = 0;
    }
}

struct LaneStateV2 {
    fill: FillChunkV2,
    spare_buffer: Option<BufferLease>,
    spare_records: Option<Vec<StagedRecordV2>>,
    encoding: Option<RegionAppendReservationV2>,
    submitted: Option<RegionWriteSpanV2>,
    failed: bool,
    closed: bool,
}

struct StagingLaneV2 {
    state: Mutex<LaneStateV2>,
    buffers: DedicatedBufferPool,
}

/// Restores a fill lease if an encoder returns an error or unwinds. The
/// encoder itself runs without the staging mutex held.
struct EncodingBufferV2<'a> {
    lane: &'a StagingLaneV2,
    receipt: RegionAppendReservationV2,
    buffer: Option<BufferLease>,
}

impl Drop for EncodingBufferV2<'_> {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        let mut state = lock_unpoisoned(&self.lane.state);
        if state.encoding == Some(self.receipt) && state.fill.buffer.is_none() {
            state.fill.buffer = Some(buffer);
            state.encoding = None;
        } else {
            state.failed = true;
        }
    }
}

/// V2 lane-local, fixed-memory append staging.
///
/// Each lane owns exactly two eagerly allocated aligned leases. One is the
/// current fill buffer; after sealing, the other immediately becomes the next
/// fill while the former is owned by the I/O engine. There is no resident-read
/// copy and no staging-owned span sequence: the Region manager receipt is the
/// sole identity accepted by sealing and completion.
pub(crate) struct RegionStagingV2 {
    lanes: Vec<StagingLaneV2>,
    chunk_bytes: usize,
    region_size: u64,
    _memory: RuntimeMemoryReservation,
}

impl RegionStagingV2 {
    pub(crate) fn try_new(
        lane_count: usize,
        chunk_bytes: usize,
        region_size: u64,
        resources: &ResourceController,
    ) -> Result<Self, ResourceBuildError> {
        if lane_count == 0 {
            return Err(ResourceBuildError::Invalid(
                "V2 Region staging requires at least one lane",
            ));
        }
        if chunk_bytes == 0
            || chunk_bytes > V2_WRITE_BATCH_BYTES
            || chunk_bytes % BUFFER_ALIGNMENT != 0
            || chunk_bytes % RECORD_ALIGNMENT_V2 as usize != 0
        {
            return Err(ResourceBuildError::Invalid(
                "V2 Region staging chunk must be a bounded aligned size",
            ));
        }
        if region_size <= u64::from(REGION_HEADER_SIZE_V2)
            || region_size % BUFFER_ALIGNMENT as u64 != 0
        {
            return Err(ResourceBuildError::Invalid(
                "V2 Region staging Region size is invalid",
            ));
        }

        let buffers_per_lane = chunk_bytes
            .checked_mul(2)
            .ok_or(ResourceBuildError::Allocation)?;
        let records_per_lane = MAX_STAGING_RECORDS_V2
            .checked_mul(std::mem::size_of::<StagedRecordV2>())
            .and_then(|bytes| bytes.checked_mul(2))
            .ok_or(ResourceBuildError::Allocation)?;
        let reserved = buffers_per_lane
            .checked_add(records_per_lane)
            .and_then(|bytes| bytes.checked_mul(lane_count))
            .ok_or(ResourceBuildError::Allocation)?;
        // DedicatedBufferPool has its own allocator. Keep this one aggregate
        // reservation alive so those eager allocations and both fixed record
        // vectors participate in the cache-wide hard memory budget.
        let memory = resources.reserve_runtime_memory(reserved)?;

        let mut lanes = Vec::new();
        lanes
            .try_reserve_exact(lane_count)
            .map_err(|_| ResourceBuildError::Allocation)?;
        for _ in 0..lane_count {
            let buffers = DedicatedBufferPool::try_new(2, chunk_bytes)?;
            let fill_buffer = buffers.acquire().ok_or(ResourceBuildError::Allocation)?;
            let spare_buffer = buffers.acquire().ok_or(ResourceBuildError::Allocation)?;
            let fill_records = try_staged_records_v2()?;
            let spare_records = try_staged_records_v2()?;
            lanes.push(StagingLaneV2 {
                state: Mutex::new(LaneStateV2 {
                    fill: FillChunkV2::new(fill_buffer, fill_records),
                    spare_buffer: Some(spare_buffer),
                    spare_records: Some(spare_records),
                    encoding: None,
                    submitted: None,
                    failed: false,
                    closed: false,
                }),
                buffers,
            });
        }

        Ok(Self {
            lanes,
            chunk_bytes,
            region_size,
            _memory: memory,
        })
    }

    pub(crate) const fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    /// Encodes one exact manager reservation directly into the fill lease.
    ///
    /// The lane lock is released before `encode` runs. Until it returns, the
    /// exact receipt fences another producer or seal attempt on this lane. An
    /// encode error restores the lease without advancing staging state.
    pub(crate) fn encode_reserved<E>(
        &self,
        receipt: RegionAppendReservationV2,
        encode: impl FnOnce(&mut [u8]) -> Result<StagedRecordV2, E>,
    ) -> Result<StageAppendV2, StagingV2EncodeError<E>> {
        self.validate_reservation(receipt)
            .map_err(StagingV2EncodeError::Staging)?;
        let lane = self
            .lanes
            .get(receipt.lane_id)
            .ok_or(StagingV2EncodeError::Staging(StagingV2Error::InvalidLane))?;

        let (start, end, buffer) = {
            let mut state = lock_unpoisoned(&lane.state);
            ensure_open_v2(&state).map_err(StagingV2EncodeError::Staging)?;
            if state.encoding.is_some() {
                return Err(StagingV2EncodeError::Staging(StagingV2Error::WouldBlock));
            }
            let fill = &state.fill;
            let used =
                fill.used()
                    .ok_or(StagingV2EncodeError::Staging(StagingV2Error::Invariant(
                        "V2 staging fill length overflow",
                    )))?;
            let record_bytes = receipt.record_bytes as usize;
            if record_bytes > self.chunk_bytes {
                return Err(StagingV2EncodeError::Staging(StagingV2Error::Invariant(
                    "V2 staging record exceeds one chunk",
                )));
            }
            if fill.records.len() == MAX_STAGING_RECORDS_V2
                || used
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > self.chunk_bytes)
            {
                return Ok(StageAppendV2::NeedsSeal);
            }
            if !fill.is_empty()
                && (fill.cache_epoch != receipt.cache_epoch
                    || fill.region_id != receipt.region_id
                    || fill.region_incarnation != receipt.region_incarnation
                    || fill.end_offset != u64::from(receipt.offset)
                    || receipt.seqno <= fill.max_seqno)
            {
                return Err(StagingV2EncodeError::Staging(StagingV2Error::StaleReceipt));
            }
            let end = used
                .checked_add(record_bytes)
                .ok_or(StagingV2EncodeError::Staging(StagingV2Error::Invariant(
                    "V2 staging fill cursor overflow",
                )))?;
            let buffer = state
                .fill
                .buffer
                .take()
                .ok_or(StagingV2EncodeError::Staging(StagingV2Error::Invariant(
                    "V2 staging fill lost its buffer",
                )))?;
            state.encoding = Some(receipt);
            (used, end, buffer)
        };

        let mut encoding = EncodingBufferV2 {
            lane,
            receipt,
            buffer: Some(buffer),
        };
        let encoded = encoding
            .buffer
            .as_mut()
            .expect("V2 encoding guard owns its buffer")
            .prepared_mut(end)
            .map_err(|()| {
                StagingV2EncodeError::Staging(StagingV2Error::Invariant(
                    "V2 staging fill buffer is undersized",
                ))
            })?;
        let record = encode(&mut encoded[start..end]).map_err(StagingV2EncodeError::Encode)?;
        self.validate_record(receipt, record)
            .map_err(StagingV2EncodeError::Staging)?;

        let mut state = lock_unpoisoned(&lane.state);
        if state.encoding != Some(receipt) || state.fill.buffer.is_some() {
            state.failed = true;
            return Err(StagingV2EncodeError::Staging(StagingV2Error::StaleReceipt));
        }
        if let Err(error) = ensure_open_v2(&state) {
            return Err(StagingV2EncodeError::Staging(error));
        }
        let buffer = encoding.buffer.take().ok_or(StagingV2EncodeError::Staging(
            StagingV2Error::Invariant("V2 staging encoding lost its buffer"),
        ))?;
        if state.fill.is_empty() {
            state.fill.cache_epoch = receipt.cache_epoch;
            state.fill.region_id = receipt.region_id;
            state.fill.region_incarnation = receipt.region_incarnation;
            state.fill.start_offset = u64::from(receipt.offset);
        }
        state.fill.end_offset = reservation_end(receipt).ok_or(StagingV2EncodeError::Staging(
            StagingV2Error::Invariant("V2 staging reservation end overflow"),
        ))?;
        state.fill.max_seqno = state.fill.max_seqno.max(receipt.seqno);
        state.fill.records.push(record);
        state.fill.buffer = Some(buffer);
        state.encoding = None;
        Ok(StageAppendV2::Appended)
    }

    /// Applies one exact manager-issued tail-padding receipt in place.
    ///
    /// Only the last Format V1 record grows: its header checksum and staged
    /// index location are rewritten together, while the added bytes are zeroed.
    /// Any mismatch is terminal because the manager has already advanced its
    /// exclusive reservation cursor.
    pub(crate) fn apply_write_padding(
        &self,
        receipt: RegionPaddingReceiptV2,
    ) -> Result<(), StagingV2Error> {
        let lane = self
            .lanes
            .get(receipt.lane_id)
            .ok_or(StagingV2Error::InvalidLane)?;
        let mut state = lock_unpoisoned(&lane.state);
        ensure_open_v2(&state)?;
        let result = (|| {
            if state.encoding.is_some() || state.submitted.is_some() {
                return Err(StagingV2Error::WouldBlock);
            }
            let fill = &mut state.fill;
            let padding = receipt
                .padding_bytes()
                .filter(|padding| {
                    *padding != 0
                        && (*padding as usize) < DIRECT_IO_ALIGNMENT
                        && *padding % RECORD_ALIGNMENT_V2 == 0
                })
                .ok_or(StagingV2Error::StaleReceipt)?;
            if receipt.cache_epoch == 0
                || receipt.region_incarnation == 0
                || receipt.span_start_offset >= receipt.unpadded_end_offset
                || receipt.span_start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
                || receipt.unpadded_end_offset % u64::from(RECORD_ALIGNMENT_V2) != 0
                || receipt.padded_end_offset % DIRECT_IO_ALIGNMENT as u64 != 0
                || receipt.padded_end_offset > self.region_size
                || fill.is_empty()
                || fill.cache_epoch != receipt.cache_epoch
                || fill.region_id != receipt.region_id
                || fill.region_incarnation != receipt.region_incarnation
                || fill.start_offset != receipt.span_start_offset
                || fill.end_offset != receipt.unpadded_end_offset
                || fill.records.len() as u64 != receipt.record_count
                || fill.max_seqno != receipt.max_seqno
            {
                return Err(StagingV2Error::StaleReceipt);
            }

            let used = fill
                .used()
                .ok_or(StagingV2Error::Invariant("V2 staging fill length overflow"))?;
            let padded_used = used
                .checked_add(padding as usize)
                .filter(|padded| *padded <= self.chunk_bytes)
                .ok_or(StagingV2Error::Invariant(
                    "V2 staging padding exceeds its fixed chunk",
                ))?;
            let expected_padded_used = receipt
                .padded_end_offset
                .checked_sub(receipt.span_start_offset)
                .and_then(|length| usize::try_from(length).ok())
                .ok_or(StagingV2Error::StaleReceipt)?;
            if padded_used != expected_padded_used {
                return Err(StagingV2Error::StaleReceipt);
            }

            let last = *fill.records.last().ok_or(StagingV2Error::StaleReceipt)?;
            let location = last.entry.location;
            let old_record_len = location.record_len();
            let new_record_len = old_record_len
                .checked_add(padding)
                .filter(|length| *length <= MAX_RECORD_LEN)
                .ok_or(StagingV2Error::Invariant(
                    "V2 padded record exceeds the Format V1 limit",
                ))?;
            let record_end = u64::from(location.offset())
                .checked_add(u64::from(old_record_len))
                .ok_or(StagingV2Error::StaleReceipt)?;
            if location.is_tombstone()
                || location.region_id() != receipt.region_id
                || last.entry.seqno != receipt.max_seqno
                || record_end != receipt.unpadded_end_offset
            {
                return Err(StagingV2Error::StaleReceipt);
            }
            let record_start = u64::from(location.offset())
                .checked_sub(receipt.span_start_offset)
                .and_then(|offset| usize::try_from(offset).ok())
                .ok_or(StagingV2Error::StaleReceipt)?;
            let header_end = record_start
                .checked_add(RECORD_HEADER_SIZE)
                .filter(|end| *end <= used)
                .ok_or(StagingV2Error::StaleReceipt)?;
            let buffer = fill
                .buffer
                .as_mut()
                .ok_or(StagingV2Error::Invariant("V2 staging fill lost its buffer"))?;
            let bytes = buffer
                .prepared_mut(padded_used)
                .map_err(|()| StagingV2Error::Invariant("V2 staging buffer is undersized"))?;
            let mut header = RecordHeader::decode(&bytes[record_start..header_end]).ok_or(
                StagingV2Error::Invariant("V2 staging final record header is corrupt"),
            )?;
            if header.kind != RecordKind::Value
                || header.record_len != old_record_len
                || header.region_incarnation != receipt.region_incarnation
                || header.epoch != receipt.cache_epoch
                || header.seqno != last.entry.seqno
                || header.key_hash != last.hash
            {
                return Err(StagingV2Error::StaleReceipt);
            }
            let padded_location =
                PackedLocation::new(receipt.region_id, location.offset(), new_record_len, false)
                    .map_err(|_| StagingV2Error::Invariant("V2 padded location is invalid"))?;

            bytes[used..padded_used].fill(0);
            header.record_len = new_record_len;
            bytes[record_start..header_end].copy_from_slice(&header.encode());
            fill.records
                .last_mut()
                .expect("validated non-empty V2 staging records")
                .entry
                .location = padded_location;
            fill.end_offset = receipt.padded_end_offset;
            Ok(())
        })();
        if result.is_err() {
            state.failed = true;
        }
        result
    }

    /// Moves the current fill lease into one exact manager-owned span without
    /// copying its bytes. The second fixed lease immediately becomes fill.
    pub(crate) fn take_sealed(
        &self,
        span: RegionWriteSpanV2,
    ) -> Result<Option<StagedWriteV2>, StagingV2Error> {
        let lane = self
            .lanes
            .get(span.lane_id)
            .ok_or(StagingV2Error::InvalidLane)?;
        let mut state = lock_unpoisoned(&lane.state);
        ensure_open_v2(&state)?;
        if state.encoding.is_some() || state.submitted.is_some() {
            return Err(StagingV2Error::WouldBlock);
        }
        if state.fill.is_empty() {
            return Ok(None);
        }
        if !span_matches_records(span, &state.fill.records)
            || state.fill.cache_epoch != span.cache_epoch
            || state.fill.region_id != span.region_id
            || state.fill.region_incarnation != span.region_incarnation
            || state.fill.start_offset != span.start_offset
            || state.fill.end_offset != span.end_offset
            || state.fill.max_seqno != span.max_seqno
        {
            state.failed = true;
            return Err(StagingV2Error::StaleReceipt);
        }
        let length = usize::try_from(
            span.end_offset
                .checked_sub(span.start_offset)
                .ok_or(StagingV2Error::StaleReceipt)?,
        )
        .map_err(|_| StagingV2Error::StaleReceipt)?;
        if length == 0 || length > self.chunk_bytes {
            state.failed = true;
            return Err(StagingV2Error::StaleReceipt);
        }
        let absolute = self.span_absolute(span)?;
        let fill_buffer = state.fill.buffer.take().ok_or_else(|| {
            state.failed = true;
            StagingV2Error::Invariant("V2 staging fill lost its buffer")
        })?;
        let buffer = match IoBuffer::from_lease(fill_buffer, length) {
            Ok(buffer) => buffer,
            Err(error) => {
                state.fill.buffer = Some(error.lease);
                state.failed = true;
                return Err(StagingV2Error::Invariant(
                    "V2 staging could not expose its fill lease",
                ));
            }
        };
        let replacement_buffer = state.spare_buffer.take().ok_or_else(|| {
            state.failed = true;
            StagingV2Error::Invariant("V2 staging lost its second fixed buffer")
        })?;
        let replacement_records = state.spare_records.take().ok_or_else(|| {
            state.failed = true;
            StagingV2Error::Invariant("V2 staging lost its second record vector")
        })?;
        let records = std::mem::replace(&mut state.fill.records, replacement_records);
        state.fill.reset(replacement_buffer);
        state.submitted = Some(span);
        drop(state);
        Ok(Some(StagedWriteV2 {
            span,
            buffer,
            absolute,
            records,
        }))
    }

    pub(crate) fn finish_success(
        &self,
        span: RegionWriteSpanV2,
        buffer: IoBuffer,
        records: Vec<StagedRecordV2>,
    ) -> Result<(), StagingV2Error> {
        self.finish(span, Some(buffer), records, false)
    }

    pub(crate) fn finish_failure(
        &self,
        span: RegionWriteSpanV2,
        buffer: Option<IoBuffer>,
        records: Vec<StagedRecordV2>,
    ) -> Result<(), StagingV2Error> {
        self.finish(span, buffer, records, true)
    }

    fn finish(
        &self,
        span: RegionWriteSpanV2,
        buffer: Option<IoBuffer>,
        mut records: Vec<StagedRecordV2>,
        failed: bool,
    ) -> Result<(), StagingV2Error> {
        let lane = self
            .lanes
            .get(span.lane_id)
            .ok_or(StagingV2Error::InvalidLane)?;
        let expected_len = span
            .end_offset
            .checked_sub(span.start_offset)
            .and_then(|length| usize::try_from(length).ok());
        let buffer_valid = buffer
            .as_ref()
            .map_or(failed, |buffer| expected_len == Some(buffer.len()));
        let completion_valid = buffer_valid
            && records.capacity() >= MAX_STAGING_RECORDS_V2
            && span_matches_records(span, &records);
        let buffer = buffer.map(IoBuffer::into_lease);
        let mut state = lock_unpoisoned(&lane.state);
        if state.submitted != Some(span) || !completion_valid {
            state.failed = true;
            return Err(StagingV2Error::StaleReceipt);
        }
        if state.spare_buffer.is_some() || state.spare_records.is_some() {
            state.failed = true;
            return Err(StagingV2Error::Invariant(
                "V2 staging completion found occupied spare resources",
            ));
        }
        records.clear();
        state.spare_buffer = buffer;
        state.spare_records = Some(records);
        state.submitted = None;
        state.failed |= failed;
        if failed {
            Ok(())
        } else if state.failed {
            Err(StagingV2Error::Failed)
        } else if state.closed {
            Err(StagingV2Error::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        for lane in &self.lanes {
            let mut state = lock_unpoisoned(&lane.state);
            state.closed = true;
            lane.buffers.close();
        }
    }

    fn validate_record(
        &self,
        receipt: RegionAppendReservationV2,
        record: StagedRecordV2,
    ) -> Result<(), StagingV2Error> {
        self.validate_reservation(receipt)?;
        let location = record.entry.location;
        if location.is_tombstone()
            || location.region_id() != receipt.region_id
            || location.offset() != receipt.offset
            || location.record_len() != receipt.record_bytes
            || record.entry.seqno != receipt.seqno
        {
            return Err(StagingV2Error::StaleReceipt);
        }
        Ok(())
    }

    fn validate_reservation(
        &self,
        receipt: RegionAppendReservationV2,
    ) -> Result<(), StagingV2Error> {
        let end = reservation_end(receipt).ok_or(StagingV2Error::StaleReceipt)?;
        if receipt.cache_epoch == 0
            || receipt.region_incarnation == 0
            || receipt.seqno == 0
            || receipt.record_bytes == 0
            || receipt.record_bytes % RECORD_ALIGNMENT_V2 != 0
            || receipt.offset < REGION_HEADER_SIZE_V2
            || receipt.offset % RECORD_ALIGNMENT_V2 != 0
            || end > self.region_size
        {
            return Err(StagingV2Error::StaleReceipt);
        }
        Ok(())
    }

    fn span_absolute(&self, span: RegionWriteSpanV2) -> Result<u64, StagingV2Error> {
        if span.end_offset > self.region_size {
            return Err(StagingV2Error::StaleReceipt);
        }
        DATA_REGION_AREA_OFFSET_V2
            .checked_add(
                u64::from(span.region_id)
                    .checked_mul(self.region_size)
                    .ok_or(StagingV2Error::Invariant(
                        "V2 staging Region offset overflow",
                    ))?,
            )
            .and_then(|base| base.checked_add(span.start_offset))
            .ok_or(StagingV2Error::Invariant(
                "V2 staging absolute offset overflow",
            ))
    }
}

fn try_staged_records_v2() -> Result<Vec<StagedRecordV2>, ResourceBuildError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(MAX_STAGING_RECORDS_V2)
        .map_err(|_| ResourceBuildError::Allocation)?;
    Ok(records)
}

fn ensure_open_v2(state: &LaneStateV2) -> Result<(), StagingV2Error> {
    if state.failed {
        Err(StagingV2Error::Failed)
    } else if state.closed {
        Err(StagingV2Error::Closed)
    } else {
        Ok(())
    }
}

fn reservation_end(receipt: RegionAppendReservationV2) -> Option<u64> {
    u64::from(receipt.offset).checked_add(u64::from(receipt.record_bytes))
}

fn span_matches_records(span: RegionWriteSpanV2, records: &[StagedRecordV2]) -> bool {
    if span.record_count == 0 || span.record_count != records.len() as u64 {
        return false;
    }
    let mut offset = span.start_offset;
    let mut max_seqno = 0_u64;
    for record in records {
        let entry = record.entry;
        if entry.location.is_tombstone()
            || entry.location.region_id() != span.region_id
            || u64::from(entry.location.offset()) != offset
            || entry.seqno == 0
            || entry.seqno <= max_seqno
        {
            return false;
        }
        let Some(end) = offset.checked_add(u64::from(entry.location.record_len())) else {
            return false;
        };
        offset = end;
        max_seqno = entry.seqno;
    }
    offset == span.end_offset && max_seqno == span.max_seqno
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod v2_tests {
    use super::*;
    use crate::format::RecordCodec;
    use crate::index::PackedLocation;
    use crate::resources::{BackpressurePolicy, ResourceLimits};

    fn resources(memory_budget_bytes: usize) -> ResourceController {
        ResourceController::try_new(ResourceLimits {
            memory_budget_bytes,
            base_memory_bytes: 0,
            max_buffer_bytes: BUFFER_ALIGNMENT,
            read_queue_depth: 1,
            write_queue_depth: 1,
            read_buffer_slots: 1,
            write_buffer_slots: 1,
            control_concurrency: 1,
            backpressure: BackpressurePolicy::Reject,
            write_budget_bytes_per_second: None,
        })
        .unwrap()
    }

    fn reservation(
        offset: u32,
        record_bytes: u32,
        seqno: u64,
    ) -> (RegionAppendReservationV2, StagedRecordV2) {
        let receipt = RegionAppendReservationV2 {
            lane_id: 0,
            cache_epoch: 3,
            region_id: 1,
            region_incarnation: 7,
            offset,
            record_bytes,
            seqno,
        };
        let record = StagedRecordV2 {
            hash: seqno.wrapping_mul(17),
            entry: IndexEntry {
                location: PackedLocation::new(1, offset, record_bytes, false).unwrap(),
                seqno,
                namespace_id: 9,
                flags: 0,
            },
        };
        (receipt, record)
    }

    fn span(
        start_offset: u64,
        end_offset: u64,
        record_count: u64,
        max_seqno: u64,
        span_id: u64,
    ) -> RegionWriteSpanV2 {
        RegionWriteSpanV2 {
            lane_id: 0,
            span_id,
            cache_epoch: 3,
            region_id: 1,
            region_incarnation: 7,
            start_offset,
            end_offset,
            record_count,
            max_seqno,
        }
    }

    fn encode_test_value(
        target: &mut [u8],
        receipt: RegionAppendReservationV2,
        record: StagedRecordV2,
    ) -> Result<StagedRecordV2, ()> {
        target.fill(0);
        let header = RecordHeader {
            kind: RecordKind::Value,
            codec: RecordCodec::PlainKey,
            key_len: 0,
            value_len: 0,
            stored_len: 0,
            record_len: receipt.record_bytes,
            region_incarnation: receipt.region_incarnation,
            epoch: receipt.cache_epoch,
            seqno: receipt.seqno,
            key_hash: record.hash,
            expires_at: 0,
            payload_crc: 0,
        };
        target[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());
        Ok(record)
    }

    #[test]
    fn v2_seal_moves_the_aligned_fill_lease_and_keeps_filling_the_second_buffer() {
        let resources = resources(4 * 1024 * 1024);
        let staging = RegionStagingV2::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        assert_eq!(staging.chunk_bytes(), 4096);
        assert_eq!(
            resources.snapshot().memory_used_bytes,
            (2 * 4096 + 2 * MAX_STAGING_RECORDS_V2 * std::mem::size_of::<StagedRecordV2>()) as u64
        );

        let (first, first_record) = reservation(REGION_HEADER_SIZE_V2, 64, 11);
        let mut first_pointer = 0_usize;
        assert_eq!(
            staging
                .encode_reserved(first, |target| {
                    first_pointer = target.as_ptr() as usize;
                    target.fill(0x11);
                    Ok::<StagedRecordV2, ()>(first_record)
                })
                .unwrap(),
            StageAppendV2::Appended
        );
        assert_eq!(first_pointer % BUFFER_ALIGNMENT, 0);

        let first_span = span(4096, 4160, 1, 11, 1);
        let first_job = staging.take_sealed(first_span).unwrap().unwrap();
        assert_eq!(first_job.span, first_span);
        assert_eq!(
            first_job.buffer.as_slice().unwrap().as_ptr() as usize,
            first_pointer
        );
        assert_eq!(first_job.buffer.as_slice().unwrap(), &[0x11; 64]);
        assert_eq!(
            first_job.absolute,
            DATA_REGION_AREA_OFFSET_V2 + 64 * 1024 + 4096
        );

        let (second, second_record) = reservation(4160, 96, 12);
        let mut second_pointer = 0_usize;
        staging
            .encode_reserved(second, |target| {
                second_pointer = target.as_ptr() as usize;
                target.fill(0x22);
                Ok::<StagedRecordV2, ()>(second_record)
            })
            .unwrap();
        assert_ne!(second_pointer, first_pointer);
        assert_eq!(second_pointer % BUFFER_ALIGNMENT, 0);
        let second_span = span(4160, 4256, 1, 12, 2);
        assert!(matches!(
            staging.take_sealed(second_span),
            Err(StagingV2Error::WouldBlock)
        ));

        let StagedWriteV2 {
            buffer, records, ..
        } = first_job;
        staging.finish_success(first_span, buffer, records).unwrap();
        let second_job = staging.take_sealed(second_span).unwrap().unwrap();
        assert_eq!(
            second_job.buffer.as_slice().unwrap().as_ptr() as usize,
            second_pointer
        );
        let StagedWriteV2 {
            buffer, records, ..
        } = second_job;
        staging
            .finish_success(second_span, buffer, records)
            .unwrap();
    }

    #[test]
    fn v2_padding_receipt_expands_only_the_final_record_without_copying() {
        let resources = resources(4 * 1024 * 1024);
        let staging = RegionStagingV2::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        let first_len = RecordHeader::aligned_len(0, 0).unwrap();
        assert_eq!(first_len as usize, RECORD_HEADER_SIZE);
        let (first, mut first_record) = reservation(4096, first_len, 11);
        first_record.entry.namespace_id = 0;
        let mut pointer = 0_usize;
        staging
            .encode_reserved(first, |target| {
                pointer = target.as_ptr() as usize;
                encode_test_value(target, first, first_record)
            })
            .unwrap();

        let second_offset = first.offset + first.record_bytes;
        let (second, mut second_record) = reservation(second_offset, 128, 12);
        second_record.entry.namespace_id = 0;
        staging
            .encode_reserved(second, |target| {
                encode_test_value(target, second, second_record)
            })
            .unwrap();
        let unpadded_end = u64::from(second.offset) + u64::from(second.record_bytes);
        let padding = RegionPaddingReceiptV2 {
            lane_id: 0,
            cache_epoch: 3,
            region_id: 1,
            region_incarnation: 7,
            span_start_offset: 4096,
            unpadded_end_offset: unpadded_end,
            padded_end_offset: 8192,
            record_count: 2,
            max_seqno: 12,
        };
        let padding_bytes = u32::try_from(8192 - unpadded_end).unwrap();
        assert_eq!(padding.padding_bytes(), Some(padding_bytes));
        staging.apply_write_padding(padding).unwrap();

        let padded_span = span(4096, 8192, 2, 12, 1);
        let job = staging.take_sealed(padded_span).unwrap().unwrap();
        assert_eq!(job.buffer.len(), 4096);
        let bytes = job.buffer.as_slice().unwrap();
        assert_eq!(bytes.as_ptr() as usize, pointer);
        let first_header = RecordHeader::decode(&bytes[..RECORD_HEADER_SIZE]).unwrap();
        let second_start = first_len as usize;
        let second_header =
            RecordHeader::decode(&bytes[second_start..second_start + RECORD_HEADER_SIZE]).unwrap();
        assert_eq!(first_header.record_len, first_len);
        let padded_second_len = second.record_bytes + padding_bytes;
        assert_eq!(second_header.record_len, padded_second_len);
        assert_eq!(job.records[0].entry.location.record_len(), first_len);
        assert_eq!(
            job.records[1].entry.location.record_len(),
            padded_second_len
        );
        assert!(
            bytes[unpadded_end as usize - 4096..]
                .iter()
                .all(|byte| *byte == 0)
        );

        let StagedWriteV2 {
            buffer, records, ..
        } = job;
        staging
            .finish_success(padded_span, buffer, records)
            .unwrap();
    }

    #[test]
    fn v2_fixed_record_and_byte_bounds_request_a_seal_without_running_encoder() {
        let resources = resources(8 * 1024 * 1024);
        let chunk_bytes = 256 * 1024;
        let staging = RegionStagingV2::try_new(1, chunk_bytes, 512 * 1024, &resources).unwrap();
        let mut offset = REGION_HEADER_SIZE_V2;
        for index in 0..MAX_STAGING_RECORDS_V2 {
            let (receipt, record) = reservation(offset, RECORD_ALIGNMENT_V2, index as u64 + 1);
            assert_eq!(
                staging
                    .encode_reserved(receipt, |target| {
                        target.fill(index as u8);
                        Ok::<StagedRecordV2, ()>(record)
                    })
                    .unwrap(),
                StageAppendV2::Appended
            );
            offset += RECORD_ALIGNMENT_V2;
        }
        let (overflow, overflow_record) = reservation(offset, RECORD_ALIGNMENT_V2, 4097);
        let mut called = false;
        assert_eq!(
            staging
                .encode_reserved(overflow, |_| {
                    called = true;
                    Ok::<StagedRecordV2, ()>(overflow_record)
                })
                .unwrap(),
            StageAppendV2::NeedsSeal
        );
        assert!(!called);

        let metadata_span = span(4096, u64::from(offset), 4096, 4096, 1);
        let job = staging.take_sealed(metadata_span).unwrap().unwrap();
        let StagedWriteV2 {
            buffer, records, ..
        } = job;
        staging
            .finish_success(metadata_span, buffer, records)
            .unwrap();

        let (full, full_record) = reservation(offset, chunk_bytes as u32, 4097);
        staging
            .encode_reserved(full, |target| {
                target.fill(0x5a);
                Ok::<StagedRecordV2, ()>(full_record)
            })
            .unwrap();
        let next_offset = offset + chunk_bytes as u32;
        let (next, next_record) = reservation(next_offset, RECORD_ALIGNMENT_V2, 4098);
        called = false;
        assert_eq!(
            staging
                .encode_reserved(next, |_| {
                    called = true;
                    Ok::<StagedRecordV2, ()>(next_record)
                })
                .unwrap(),
            StageAppendV2::NeedsSeal
        );
        assert!(!called);
    }

    #[test]
    fn v2_completion_fences_stale_receipts_and_write_failure_is_sticky() {
        let resources = resources(8 * 1024 * 1024);
        let staging = RegionStagingV2::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        let (receipt, record) = reservation(4096, 64, 11);
        let mut mismatched = record;
        mismatched.entry.seqno += 1;
        assert_eq!(
            staging.encode_reserved(receipt, |target| {
                target.fill(0x22);
                Ok::<StagedRecordV2, ()>(mismatched)
            }),
            Err(StagingV2EncodeError::Staging(StagingV2Error::StaleReceipt))
        );
        staging
            .encode_reserved(receipt, |target| {
                target.fill(0x33);
                Ok::<StagedRecordV2, ()>(record)
            })
            .unwrap();
        let submitted = span(4096, 4160, 1, 11, 1);
        let job = staging.take_sealed(submitted).unwrap().unwrap();
        let mut stale = submitted;
        stale.span_id += 1;
        let StagedWriteV2 {
            buffer, records, ..
        } = job;
        assert_eq!(
            staging.finish_success(stale, buffer, records),
            Err(StagingV2Error::StaleReceipt)
        );
        let (later, later_record) = reservation(4160, 64, 12);
        assert_eq!(
            staging.encode_reserved(later, |_| Ok::<StagedRecordV2, ()>(later_record)),
            Err(StagingV2EncodeError::Staging(StagingV2Error::Failed))
        );

        let other = RegionStagingV2::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        other
            .encode_reserved(receipt, |target| {
                target.fill(0x44);
                Ok::<StagedRecordV2, ()>(record)
            })
            .unwrap();
        let job = other.take_sealed(submitted).unwrap().unwrap();
        let StagedWriteV2 {
            buffer, records, ..
        } = job;
        // A failed driver may quarantine the owned I/O buffer. Staging still
        // fences the exact receipt and becomes terminal without waiting for a
        // resource which can no longer return.
        drop(buffer);
        other.finish_failure(submitted, None, records).unwrap();
        assert_eq!(
            other.encode_reserved(later, |_| Ok::<StagedRecordV2, ()>(later_record)),
            Err(StagingV2EncodeError::Staging(StagingV2Error::Failed))
        );
    }
}
