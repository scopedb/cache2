//! Fixed-memory, zero-copy staging for the RegionStore append path.
//!
//! Region manager receipts are the only span authority.

use std::fmt;
use std::sync::{Mutex, MutexGuard};

use crate::format::{RECORD_HEADER_SIZE, RecordHeader};
use crate::index::{IndexEntry, MAX_RECORD_LEN, PackedLocation};
use crate::io_backend::DIRECT_IO_ALIGNMENT;
use crate::io_engine::IoBuffer;
use crate::recovery::{DATA_REGION_AREA_OFFSET, RECORD_ALIGNMENT, RECOVERY_PAGE_SIZE};
use crate::region_manager::{RegionAppendReservation, RegionPaddingReceipt, RegionWriteSpan};
use crate::resources::{
    BUFFER_ALIGNMENT, BufferLease, ResourceBuildError, ResourceController, RuntimeMemoryReservation,
};
use crate::runtime_config::MAX_WRITE_BATCH_BYTES;

pub(crate) const MAX_STAGING_RECORDS: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StageAppend {
    Appended,
    NeedsSeal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShardFillSnapshot {
    pub(crate) bytes: usize,
    pub(crate) records: usize,
}

/// One record whose exact index identity is published only after its
/// containing device span completes. The descriptor stays owned by the
/// completion path; staging never calls into the index while holding a shard
/// lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagedRecordKind {
    Value,
    Tombstone,
}

const STAGED_TOMBSTONE_BIT: u64 = 1_u64 << 63;

/// Compact transient completion descriptor. The packed location's reserved
/// high bit carries the publication kind while the descriptor is in memory;
/// it is masked before reconstructing an [`IndexEntry`] and is never persisted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StagedRecord {
    hash: u64,
    location_and_kind: u64,
    seqno: u64,
}

impl StagedRecord {
    pub(crate) fn new(hash: u64, entry: IndexEntry, kind: StagedRecordKind) -> Self {
        debug_assert_eq!(entry.location.raw() & STAGED_TOMBSTONE_BIT, 0);
        let kind_bit = match kind {
            StagedRecordKind::Value => 0,
            StagedRecordKind::Tombstone => STAGED_TOMBSTONE_BIT,
        };
        Self {
            hash,
            location_and_kind: entry.location.raw() | kind_bit,
            seqno: entry.seqno,
        }
    }

    pub(crate) const fn hash(self) -> u64 {
        self.hash
    }

    pub(crate) const fn entry(self) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::from_raw(self.location_and_kind & !STAGED_TOMBSTONE_BIT),
            seqno: self.seqno,
        }
    }

    pub(crate) const fn kind(self) -> StagedRecordKind {
        if self.location_and_kind & STAGED_TOMBSTONE_BIT == 0 {
            StagedRecordKind::Value
        } else {
            StagedRecordKind::Tombstone
        }
    }

    fn set_location(&mut self, location: PackedLocation) {
        let kind = self.location_and_kind & STAGED_TOMBSTONE_BIT;
        self.location_and_kind = location.raw() | kind;
    }
}

/// A zero-copy write job. `buffer` is the shard's former fill lease and is
/// therefore 4 KiB aligned. The I/O completion must return this exact buffer
/// and the record vector to [`RegionStaging::finish_success`] or
/// [`RegionStaging::finish_failure`].
pub(crate) struct StagedWrite {
    pub(crate) span: RegionWriteSpan,
    pub(crate) buffer: IoBuffer,
    pub(crate) absolute: u64,
    pub(crate) records: Vec<StagedRecord>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StagingError {
    Failed,
    Closed,
    InvalidShard,
    WouldBlock,
    StaleReceipt,
    Invariant(&'static str),
}

impl fmt::Display for StagingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Failed => formatter.write_str("Region staging is failed"),
            Self::Closed => formatter.write_str("Region staging is closed"),
            Self::InvalidShard => formatter.write_str("Region staging shard is out of bounds"),
            Self::WouldBlock => formatter.write_str("Region staging shard is busy"),
            Self::StaleReceipt => formatter.write_str("Region staging receipt is stale"),
            Self::Invariant(message) => formatter.write_str(message),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum StagingEncodeError<E> {
    Staging(StagingError),
    Encode(E),
}

impl<E> From<StagingError> for StagingEncodeError<E> {
    fn from(error: StagingError) -> Self {
        Self::Staging(error)
    }
}

struct FillChunk {
    buffer: Option<BufferLease>,
    records: Vec<StagedRecord>,
    region_id: u32,
    region_incarnation: u32,
    start_offset: u64,
    end_offset: u64,
    max_seqno: u64,
}

impl FillChunk {
    fn new(buffer: BufferLease, records: Vec<StagedRecord>) -> Self {
        Self {
            buffer: Some(buffer),
            records,
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
        self.region_id = 0;
        self.region_incarnation = 0;
        self.start_offset = 0;
        self.end_offset = 0;
        self.max_seqno = 0;
    }
}

struct ShardState {
    fill: FillChunk,
    spare_buffer: Option<BufferLease>,
    spare_records: Option<Vec<StagedRecord>>,
    encoding: Option<RegionAppendReservation>,
    submitted: Option<RegionWriteSpan>,
    failed: bool,
    closed: bool,
}

struct ShardStaging {
    state: Mutex<ShardState>,
}

/// Restores a fill lease if an encoder returns an error or unwinds. The
/// encoder itself runs without the staging mutex held.
struct EncodingBuffer<'a> {
    shard: &'a ShardStaging,
    receipt: RegionAppendReservation,
    buffer: Option<BufferLease>,
}

impl Drop for EncodingBuffer<'_> {
    fn drop(&mut self) {
        let Some(buffer) = self.buffer.take() else {
            return;
        };
        let mut state = lock_unpoisoned(&self.shard.state);
        if state.encoding == Some(self.receipt) && state.fill.buffer.is_none() {
            state.fill.buffer = Some(buffer);
            state.encoding = None;
        } else {
            state.failed = true;
        }
    }
}

/// Shard-local, fixed-memory append staging.
///
/// Each shard owns exactly two eagerly allocated aligned leases. One is the
/// current fill buffer; after sealing, the other immediately becomes the next
/// fill while the former is owned by the I/O engine. There is no resident-read
/// copy and no staging-owned span sequence: the Region manager receipt is the
/// sole identity accepted by sealing and completion.
pub(crate) struct RegionStaging {
    shards: Vec<ShardStaging>,
    chunk_bytes: usize,
    region_size: u64,
    _memory: RuntimeMemoryReservation,
}

impl RegionStaging {
    pub(crate) fn reservation_bytes(shard_count: usize, chunk_bytes: usize) -> Option<usize> {
        let buffers_per_shard = chunk_bytes.checked_mul(2)?;
        let records_per_shard = MAX_STAGING_RECORDS
            .checked_mul(std::mem::size_of::<StagedRecord>())?
            .checked_mul(2)?;
        buffers_per_shard
            .checked_add(records_per_shard)?
            .checked_mul(shard_count)
    }

    pub(crate) fn try_new(
        shard_count: usize,
        chunk_bytes: usize,
        region_size: u64,
        resources: &ResourceController,
    ) -> Result<Self, ResourceBuildError> {
        if shard_count == 0 {
            return Err(ResourceBuildError::Invalid(
                "Region staging requires at least one shard",
            ));
        }
        if chunk_bytes == 0
            || chunk_bytes > MAX_WRITE_BATCH_BYTES
            || !chunk_bytes.is_multiple_of(BUFFER_ALIGNMENT)
            || !chunk_bytes.is_multiple_of(RECORD_ALIGNMENT as usize)
        {
            return Err(ResourceBuildError::Invalid(
                "Region staging chunk must be a bounded aligned size",
            ));
        }
        if region_size < RECOVERY_PAGE_SIZE as u64
            || !region_size.is_multiple_of(BUFFER_ALIGNMENT as u64)
        {
            return Err(ResourceBuildError::Invalid(
                "Region staging Region size is invalid",
            ));
        }

        let reserved = Self::reservation_bytes(shard_count, chunk_bytes)
            .ok_or(ResourceBuildError::Allocation)?;
        // Keep the aggregate reservation alive so eager buffers and record
        // vectors participate in the hard memory limit.
        let memory = resources.reserve_runtime_memory(reserved)?;

        let mut shards = Vec::new();
        shards
            .try_reserve_exact(shard_count)
            .map_err(|_| ResourceBuildError::Allocation)?;
        for _ in 0..shard_count {
            let fill_buffer = BufferLease::try_fixed(chunk_bytes)?;
            let spare_buffer = BufferLease::try_fixed(chunk_bytes)?;
            let fill_records = try_staged_records()?;
            let spare_records = try_staged_records()?;
            shards.push(ShardStaging {
                state: Mutex::new(ShardState {
                    fill: FillChunk::new(fill_buffer, fill_records),
                    spare_buffer: Some(spare_buffer),
                    spare_records: Some(spare_records),
                    encoding: None,
                    submitted: None,
                    failed: false,
                    closed: false,
                }),
            });
        }

        Ok(Self {
            shards,
            chunk_bytes,
            region_size,
            _memory: memory,
        })
    }

    pub(crate) const fn chunk_bytes(&self) -> usize {
        self.chunk_bytes
    }

    /// Checks the fixed fill capacity before the manager allocates a receipt.
    ///
    /// The caller holds the shard mutation gate through this check, manager
    /// reservation, and [`Self::encode_reserved`], so a successful preflight
    /// cannot turn into ordinary staging saturation before encoding begins.
    pub(crate) fn preflight_append(
        &self,
        shard_id: usize,
        record_bytes: u32,
    ) -> Result<StageAppend, StagingError> {
        let record_bytes = usize::try_from(record_bytes)
            .map_err(|_| StagingError::Invariant("staging record length does not fit usize"))?;
        if record_bytes == 0
            || record_bytes > self.chunk_bytes
            || record_bytes % RECORD_ALIGNMENT as usize != 0
        {
            return Err(StagingError::Invariant(
                "staging preflight record length is invalid",
            ));
        }
        let shard = self
            .shards
            .get(shard_id)
            .ok_or(StagingError::InvalidShard)?;
        let state = lock_unpoisoned(&shard.state);
        ensure_open(&state)?;
        if state.encoding.is_some() {
            return Err(StagingError::WouldBlock);
        }
        let used = state
            .fill
            .used()
            .ok_or(StagingError::Invariant("staging fill length overflow"))?;
        if state.fill.records.len() == MAX_STAGING_RECORDS
            || used
                .checked_add(record_bytes)
                .is_none_or(|end| end > self.chunk_bytes)
        {
            return Ok(StageAppend::NeedsSeal);
        }
        Ok(StageAppend::Appended)
    }

    /// Returns the currently sealable fill prefix without waiting for an
    /// encoder or an earlier submitted span. `Ok(None)` is the only empty-shard
    /// result, so a shard worker never has to probe the manager by attempting
    /// to seal an absent span.
    pub(crate) fn shard_fill_snapshot(
        &self,
        shard_id: usize,
    ) -> Result<Option<ShardFillSnapshot>, StagingError> {
        let shard = self
            .shards
            .get(shard_id)
            .ok_or(StagingError::InvalidShard)?;
        let state = lock_unpoisoned(&shard.state);
        ensure_open(&state)?;
        if state.encoding.is_some() {
            return Err(StagingError::WouldBlock);
        }
        if state.submitted.is_some() {
            return Err(StagingError::WouldBlock);
        }
        if state.fill.is_empty() {
            return Ok(None);
        }
        let bytes = state
            .fill
            .used()
            .filter(|bytes| *bytes != 0 && *bytes <= self.chunk_bytes)
            .ok_or(StagingError::Invariant(
                "staging fill snapshot has an invalid length",
            ))?;
        Ok(Some(ShardFillSnapshot {
            bytes,
            records: state.fill.records.len(),
        }))
    }

    /// Encodes one exact manager reservation directly into the fill lease.
    ///
    /// The shard lock is released before `encode` runs. Until it returns, the
    /// exact receipt fences another producer or seal attempt on this shard. An
    /// encode error restores the lease without advancing staging state.
    pub(crate) fn encode_reserved<E>(
        &self,
        receipt: RegionAppendReservation,
        encode: impl FnOnce(&mut [u8]) -> Result<StagedRecord, E>,
    ) -> Result<StageAppend, StagingEncodeError<E>> {
        self.validate_reservation(receipt)
            .map_err(StagingEncodeError::Staging)?;
        let shard = self
            .shards
            .get(receipt.shard_id)
            .ok_or(StagingEncodeError::Staging(StagingError::InvalidShard))?;

        let (start, end, buffer) = {
            let mut state = lock_unpoisoned(&shard.state);
            ensure_open(&state).map_err(StagingEncodeError::Staging)?;
            if state.encoding.is_some() {
                return Err(StagingEncodeError::Staging(StagingError::WouldBlock));
            }
            let fill = &state.fill;
            let used = fill
                .used()
                .ok_or(StagingEncodeError::Staging(StagingError::Invariant(
                    "staging fill length overflow",
                )))?;
            let record_bytes = receipt.record_bytes as usize;
            if record_bytes > self.chunk_bytes {
                return Err(StagingEncodeError::Staging(StagingError::Invariant(
                    "staging record exceeds one chunk",
                )));
            }
            if fill.records.len() == MAX_STAGING_RECORDS
                || used
                    .checked_add(record_bytes)
                    .is_none_or(|end| end > self.chunk_bytes)
            {
                return Ok(StageAppend::NeedsSeal);
            }
            if !fill.is_empty()
                && (fill.region_id != receipt.region_id
                    || fill.region_incarnation != receipt.region_incarnation
                    || fill.end_offset != u64::from(receipt.offset)
                    || receipt.seqno <= fill.max_seqno)
            {
                return Err(StagingEncodeError::Staging(StagingError::StaleReceipt));
            }
            let end = used
                .checked_add(record_bytes)
                .ok_or(StagingEncodeError::Staging(StagingError::Invariant(
                    "staging fill cursor overflow",
                )))?;
            let buffer = state.fill.buffer.take().ok_or(StagingEncodeError::Staging(
                StagingError::Invariant("staging fill lost its buffer"),
            ))?;
            state.encoding = Some(receipt);
            (used, end, buffer)
        };

        let mut encoding = EncodingBuffer {
            shard,
            receipt,
            buffer: Some(buffer),
        };
        let encoded = encoding
            .buffer
            .as_mut()
            .expect("encoding guard owns its buffer")
            .prepared_mut(end)
            .map_err(|()| {
                StagingEncodeError::Staging(StagingError::Invariant(
                    "staging fill buffer is undersized",
                ))
            })?;
        let record = encode(&mut encoded[start..end]).map_err(StagingEncodeError::Encode)?;
        self.validate_record(receipt, record)
            .map_err(StagingEncodeError::Staging)?;

        let mut state = lock_unpoisoned(&shard.state);
        if state.encoding != Some(receipt) || state.fill.buffer.is_some() {
            state.failed = true;
            return Err(StagingEncodeError::Staging(StagingError::StaleReceipt));
        }
        if let Err(error) = ensure_open(&state) {
            return Err(StagingEncodeError::Staging(error));
        }
        let buffer =
            encoding
                .buffer
                .take()
                .ok_or(StagingEncodeError::Staging(StagingError::Invariant(
                    "staging encoding lost its buffer",
                )))?;
        if state.fill.is_empty() {
            state.fill.region_id = receipt.region_id;
            state.fill.region_incarnation = receipt.region_incarnation;
            state.fill.start_offset = u64::from(receipt.offset);
        }
        state.fill.end_offset = reservation_end(receipt).ok_or(StagingEncodeError::Staging(
            StagingError::Invariant("staging reservation end overflow"),
        ))?;
        state.fill.max_seqno = state.fill.max_seqno.max(receipt.seqno);
        state.fill.records.push(record);
        state.fill.buffer = Some(buffer);
        state.encoding = None;
        Ok(StageAppend::Appended)
    }

    /// Applies one exact manager-issued tail-padding receipt in place.
    ///
    /// Only the last record grows: its header checksum and staged
    /// index location are rewritten together, while the added bytes are zeroed.
    /// Any mismatch is terminal because the manager has already advanced its
    /// exclusive reservation cursor.
    pub(crate) fn apply_write_padding(
        &self,
        receipt: RegionPaddingReceipt,
    ) -> Result<(), StagingError> {
        let shard = self
            .shards
            .get(receipt.shard_id)
            .ok_or(StagingError::InvalidShard)?;
        let mut state = lock_unpoisoned(&shard.state);
        ensure_open(&state)?;
        let result = (|| {
            if state.encoding.is_some() || state.submitted.is_some() {
                return Err(StagingError::WouldBlock);
            }
            let fill = &mut state.fill;
            let padding = receipt
                .padding_bytes()
                .filter(|padding| {
                    *padding != 0
                        && (*padding as usize) < DIRECT_IO_ALIGNMENT
                        && *padding % RECORD_ALIGNMENT == 0
                })
                .ok_or(StagingError::StaleReceipt)?;
            if receipt.region_incarnation == 0
                || receipt.span_start_offset >= receipt.unpadded_end_offset
                || !receipt
                    .span_start_offset
                    .is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
                || !receipt
                    .unpadded_end_offset
                    .is_multiple_of(u64::from(RECORD_ALIGNMENT))
                || !receipt
                    .padded_end_offset
                    .is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
                || receipt.padded_end_offset > self.region_size
                || fill.is_empty()
                || fill.region_id != receipt.region_id
                || fill.region_incarnation != receipt.region_incarnation
                || fill.start_offset != receipt.span_start_offset
                || fill.end_offset != receipt.unpadded_end_offset
                || fill.records.len() as u64 != receipt.record_count
                || fill.max_seqno != receipt.max_seqno
            {
                return Err(StagingError::StaleReceipt);
            }

            let used = fill
                .used()
                .ok_or(StagingError::Invariant("staging fill length overflow"))?;
            let padded_used = used
                .checked_add(padding as usize)
                .filter(|padded| *padded <= self.chunk_bytes)
                .ok_or(StagingError::Invariant(
                    "staging padding exceeds its fixed chunk",
                ))?;
            let expected_padded_used = receipt
                .padded_end_offset
                .checked_sub(receipt.span_start_offset)
                .and_then(|length| usize::try_from(length).ok())
                .ok_or(StagingError::StaleReceipt)?;
            if padded_used != expected_padded_used {
                return Err(StagingError::StaleReceipt);
            }

            let last = *fill.records.last().ok_or(StagingError::StaleReceipt)?;
            let last_entry = last.entry();
            let location = last_entry.location;
            let old_record_len = location.record_len();
            let new_record_len = old_record_len
                .checked_add(padding)
                .filter(|length| *length <= MAX_RECORD_LEN)
                .ok_or(StagingError::Invariant(
                    "padded record exceeds the record-format limit",
                ))?;
            let record_end = u64::from(location.offset())
                .checked_add(u64::from(old_record_len))
                .ok_or(StagingError::StaleReceipt)?;
            if location.region_id() != receipt.region_id
                || last_entry.seqno != receipt.max_seqno
                || record_end != receipt.unpadded_end_offset
            {
                return Err(StagingError::StaleReceipt);
            }
            let record_start = u64::from(location.offset())
                .checked_sub(receipt.span_start_offset)
                .and_then(|offset| usize::try_from(offset).ok())
                .ok_or(StagingError::StaleReceipt)?;
            let header_end = record_start
                .checked_add(RECORD_HEADER_SIZE)
                .filter(|end| *end <= used)
                .ok_or(StagingError::StaleReceipt)?;
            let buffer = fill
                .buffer
                .as_mut()
                .ok_or(StagingError::Invariant("staging fill lost its buffer"))?;
            let bytes = buffer
                .prepared_mut(padded_used)
                .map_err(|()| StagingError::Invariant("staging buffer is undersized"))?;
            let mut header = RecordHeader::decode(&bytes[record_start..header_end]).ok_or(
                StagingError::Invariant("staging final record header is corrupt"),
            )?;
            if header.record_len != old_record_len
                || header.seqno != last_entry.seqno
                || header.key_hash != last.hash()
            {
                return Err(StagingError::StaleReceipt);
            }
            let padded_location =
                PackedLocation::new(receipt.region_id, location.offset(), new_record_len)
                    .map_err(|_| StagingError::Invariant("padded location is invalid"))?;

            bytes[used..padded_used].fill(0);
            header.record_len = new_record_len;
            bytes[record_start..header_end].copy_from_slice(&header.encode());
            fill.records
                .last_mut()
                .expect("validated non-empty staging records")
                .set_location(padded_location);
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
        span: RegionWriteSpan,
    ) -> Result<Option<StagedWrite>, StagingError> {
        let shard = self
            .shards
            .get(span.shard_id)
            .ok_or(StagingError::InvalidShard)?;
        let mut state = lock_unpoisoned(&shard.state);
        ensure_open(&state)?;
        if state.encoding.is_some() || state.submitted.is_some() {
            return Err(StagingError::WouldBlock);
        }
        if state.fill.is_empty() {
            return Ok(None);
        }
        if !span_matches_records(span, &state.fill.records)
            || state.fill.region_id != span.region_id
            || state.fill.region_incarnation != span.region_incarnation
            || state.fill.start_offset != span.start_offset
            || state.fill.end_offset != span.end_offset
            || state.fill.max_seqno != span.max_seqno
        {
            state.failed = true;
            return Err(StagingError::StaleReceipt);
        }
        let length = usize::try_from(
            span.end_offset
                .checked_sub(span.start_offset)
                .ok_or(StagingError::StaleReceipt)?,
        )
        .map_err(|_| StagingError::StaleReceipt)?;
        if length == 0 || length > self.chunk_bytes {
            state.failed = true;
            return Err(StagingError::StaleReceipt);
        }
        let absolute = self.span_absolute(span)?;
        let fill_buffer = state.fill.buffer.take().ok_or_else(|| {
            state.failed = true;
            StagingError::Invariant("staging fill lost its buffer")
        })?;
        let buffer = match IoBuffer::for_write(fill_buffer, length) {
            Ok(buffer) => buffer,
            Err(error) => {
                state.fill.buffer = Some(error.lease);
                state.failed = true;
                return Err(StagingError::Invariant(
                    "staging could not expose its fill lease",
                ));
            }
        };
        let replacement_buffer = state.spare_buffer.take().ok_or_else(|| {
            state.failed = true;
            StagingError::Invariant("staging lost its second fixed buffer")
        })?;
        let replacement_records = state.spare_records.take().ok_or_else(|| {
            state.failed = true;
            StagingError::Invariant("staging lost its second record vector")
        })?;
        let records = std::mem::replace(&mut state.fill.records, replacement_records);
        state.fill.reset(replacement_buffer);
        state.submitted = Some(span);
        drop(state);
        Ok(Some(StagedWrite {
            span,
            buffer,
            absolute,
            records,
        }))
    }

    pub(crate) fn finish_success(
        &self,
        span: RegionWriteSpan,
        buffer: IoBuffer,
        records: Vec<StagedRecord>,
    ) -> Result<(), StagingError> {
        self.finish(span, Some(buffer), records, false)
    }

    pub(crate) fn finish_failure(
        &self,
        span: RegionWriteSpan,
        buffer: Option<IoBuffer>,
        records: Vec<StagedRecord>,
    ) -> Result<(), StagingError> {
        self.finish(span, buffer, records, true)
    }

    fn finish(
        &self,
        span: RegionWriteSpan,
        buffer: Option<IoBuffer>,
        mut records: Vec<StagedRecord>,
        failed: bool,
    ) -> Result<(), StagingError> {
        let shard = self
            .shards
            .get(span.shard_id)
            .ok_or(StagingError::InvalidShard)?;
        let expected_len = span
            .end_offset
            .checked_sub(span.start_offset)
            .and_then(|length| usize::try_from(length).ok());
        let buffer_valid = buffer
            .as_ref()
            .map_or(failed, |buffer| expected_len == Some(buffer.len()));
        let completion_valid = buffer_valid
            && records.capacity() >= MAX_STAGING_RECORDS
            && span_matches_records(span, &records);
        let buffer = buffer.map(IoBuffer::into_lease);
        let mut state = lock_unpoisoned(&shard.state);
        if state.submitted != Some(span) || !completion_valid {
            state.failed = true;
            return Err(StagingError::StaleReceipt);
        }
        if state.spare_buffer.is_some() || state.spare_records.is_some() {
            state.failed = true;
            return Err(StagingError::Invariant(
                "staging completion found occupied spare resources",
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
            Err(StagingError::Failed)
        } else if state.closed {
            Err(StagingError::Closed)
        } else {
            Ok(())
        }
    }

    pub(crate) fn close(&self) {
        for shard in &self.shards {
            let mut state = lock_unpoisoned(&shard.state);
            state.closed = true;
        }
    }

    fn validate_record(
        &self,
        receipt: RegionAppendReservation,
        record: StagedRecord,
    ) -> Result<(), StagingError> {
        self.validate_reservation(receipt)?;
        let entry = record.entry();
        let location = entry.location;
        if location.region_id() != receipt.region_id
            || location.offset() != receipt.offset
            || location.record_len() != receipt.record_bytes
            || entry.seqno != receipt.seqno
        {
            return Err(StagingError::StaleReceipt);
        }
        Ok(())
    }

    fn validate_reservation(&self, receipt: RegionAppendReservation) -> Result<(), StagingError> {
        let end = reservation_end(receipt).ok_or(StagingError::StaleReceipt)?;
        if receipt.region_incarnation == 0
            || receipt.seqno == 0
            || receipt.record_bytes == 0
            || !receipt.record_bytes.is_multiple_of(RECORD_ALIGNMENT)
            || !receipt.offset.is_multiple_of(RECORD_ALIGNMENT)
            || end > self.region_size
        {
            return Err(StagingError::StaleReceipt);
        }
        Ok(())
    }

    fn span_absolute(&self, span: RegionWriteSpan) -> Result<u64, StagingError> {
        if span.end_offset > self.region_size {
            return Err(StagingError::StaleReceipt);
        }
        DATA_REGION_AREA_OFFSET
            .checked_add(
                u64::from(span.region_id)
                    .checked_mul(self.region_size)
                    .ok_or(StagingError::Invariant("staging Region offset overflow"))?,
            )
            .and_then(|base| base.checked_add(span.start_offset))
            .ok_or(StagingError::Invariant("staging absolute offset overflow"))
    }
}

fn try_staged_records() -> Result<Vec<StagedRecord>, ResourceBuildError> {
    let mut records = Vec::new();
    records
        .try_reserve_exact(MAX_STAGING_RECORDS)
        .map_err(|_| ResourceBuildError::Allocation)?;
    Ok(records)
}

fn ensure_open(state: &ShardState) -> Result<(), StagingError> {
    if state.failed {
        Err(StagingError::Failed)
    } else if state.closed {
        Err(StagingError::Closed)
    } else {
        Ok(())
    }
}

fn reservation_end(receipt: RegionAppendReservation) -> Option<u64> {
    u64::from(receipt.offset).checked_add(u64::from(receipt.record_bytes))
}

fn span_matches_records(span: RegionWriteSpan, records: &[StagedRecord]) -> bool {
    if span.record_count == 0 || span.record_count != records.len() as u64 {
        return false;
    }
    let mut offset = span.start_offset;
    let mut max_seqno = 0_u64;
    for record in records {
        let entry = record.entry();
        if entry.location.region_id() != span.region_id
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
mod tests {
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    use super::*;
    use crate::index::PackedLocation;
    use crate::resources::ResourceLimits;

    fn resources(memory_limit_bytes: usize) -> ResourceController {
        ResourceController::try_new(ResourceLimits {
            memory_limit_bytes,
            reserved_memory_bytes: 0,
            waiting_write_limit: 1,
        })
        .unwrap()
    }

    fn reservation(
        offset: u32,
        record_bytes: u32,
        seqno: u64,
    ) -> (RegionAppendReservation, StagedRecord) {
        let receipt = RegionAppendReservation {
            shard_id: 0,
            region_id: 1,
            region_incarnation: 7,
            offset,
            record_bytes,
            seqno,
        };
        let record = StagedRecord::new(
            seqno.wrapping_mul(17),
            IndexEntry {
                location: PackedLocation::new(1, offset, record_bytes).unwrap(),
                seqno,
            },
            StagedRecordKind::Value,
        );
        (receipt, record)
    }

    fn span(
        start_offset: u64,
        end_offset: u64,
        record_count: u64,
        max_seqno: u64,
        span_id: u64,
    ) -> RegionWriteSpan {
        RegionWriteSpan {
            shard_id: 0,
            span_id,
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
        receipt: RegionAppendReservation,
        record: StagedRecord,
    ) -> Result<StagedRecord, ()> {
        target.fill(0);
        let header = RecordHeader {
            key_len: 0,
            value_len: 0,
            namespace_id: 0,
            record_len: receipt.record_bytes,
            seqno: receipt.seqno,
            key_hash: record.hash(),
            expires_at: 0,
            payload_crc: 0,
        };
        target[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());
        Ok(record)
    }

    #[test]
    fn seal_moves_the_aligned_fill_lease_and_keeps_filling_the_second_buffer() {
        assert_eq!(std::mem::size_of::<StagedRecord>(), 24);
        let resources = resources(4 * 1024 * 1024);
        let staging = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        assert_eq!(staging.chunk_bytes(), 4096);
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            2 * 4096 + 2 * MAX_STAGING_RECORDS * std::mem::size_of::<StagedRecord>()
        );

        let (first, first_record) = reservation(0, 64, 11);
        let mut first_pointer = 0_usize;
        assert_eq!(
            staging
                .encode_reserved(first, |target| {
                    first_pointer = target.as_ptr() as usize;
                    target.fill(0x11);
                    Ok::<StagedRecord, ()>(first_record)
                })
                .unwrap(),
            StageAppend::Appended
        );
        assert_eq!(first_pointer % BUFFER_ALIGNMENT, 0);
        let first_span = span(0, 64, 1, 11, 1);
        let first_job = staging.take_sealed(first_span).unwrap().unwrap();
        assert_eq!(first_job.span, first_span);
        assert_eq!(
            first_job.buffer.as_slice().unwrap().as_ptr() as usize,
            first_pointer
        );
        assert_eq!(first_job.buffer.as_slice().unwrap(), &[0x11; 64]);
        assert_eq!(first_job.absolute, DATA_REGION_AREA_OFFSET + 64 * 1024);

        let (second, second_record) = reservation(64, 96, 12);
        let mut second_pointer = 0_usize;
        staging
            .encode_reserved(second, |target| {
                second_pointer = target.as_ptr() as usize;
                target.fill(0x22);
                Ok::<StagedRecord, ()>(second_record)
            })
            .unwrap();
        assert_ne!(second_pointer, first_pointer);
        assert_eq!(second_pointer % BUFFER_ALIGNMENT, 0);
        let second_span = span(64, 160, 1, 12, 2);
        assert!(matches!(
            staging.take_sealed(second_span),
            Err(StagingError::WouldBlock)
        ));

        let StagedWrite {
            buffer, records, ..
        } = first_job;
        staging.finish_success(first_span, buffer, records).unwrap();
        let second_job = staging.take_sealed(second_span).unwrap().unwrap();
        assert_eq!(
            second_job.buffer.as_slice().unwrap().as_ptr() as usize,
            second_pointer
        );
        let StagedWrite {
            buffer, records, ..
        } = second_job;
        staging
            .finish_success(second_span, buffer, records)
            .unwrap();
    }

    #[test]
    fn fill_snapshot_distinguishes_empty_ready_submitted_and_terminal_shards() {
        let resources = resources(8 * 1024 * 1024);
        let staging = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        assert_eq!(staging.shard_fill_snapshot(0).unwrap(), None);
        assert_eq!(
            staging.shard_fill_snapshot(1),
            Err(StagingError::InvalidShard)
        );

        let (receipt, record) = reservation(4096, 64, 11);
        staging
            .encode_reserved(receipt, |target| {
                target.fill(0x31);
                Ok::<StagedRecord, ()>(record)
            })
            .unwrap();
        assert_eq!(
            staging.shard_fill_snapshot(0).unwrap(),
            Some(ShardFillSnapshot {
                bytes: 64,
                records: 1,
            })
        );

        let submitted = span(4096, 4160, 1, 11, 1);
        let job = staging.take_sealed(submitted).unwrap().unwrap();
        assert_eq!(
            staging.shard_fill_snapshot(0),
            Err(StagingError::WouldBlock)
        );
        let StagedWrite {
            buffer, records, ..
        } = job;
        staging.finish_success(submitted, buffer, records).unwrap();
        assert_eq!(staging.shard_fill_snapshot(0).unwrap(), None);
        staging.close();
        assert_eq!(staging.shard_fill_snapshot(0), Err(StagingError::Closed));

        let failed = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        failed
            .encode_reserved(receipt, |target| {
                target.fill(0x32);
                Ok::<StagedRecord, ()>(record)
            })
            .unwrap();
        let job = failed.take_sealed(submitted).unwrap().unwrap();
        let StagedWrite {
            buffer, records, ..
        } = job;
        drop(buffer);
        failed.finish_failure(submitted, None, records).unwrap();
        assert_eq!(failed.shard_fill_snapshot(0), Err(StagingError::Failed));
    }

    #[test]
    fn fill_snapshot_never_observes_a_partially_encoded_record() {
        let resources = resources(4 * 1024 * 1024);
        let staging = Arc::new(RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap());
        let (receipt, record) = reservation(4096, 64, 11);
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let encoder_staging = Arc::clone(&staging);
        let encoder = std::thread::spawn(move || {
            encoder_staging.encode_reserved(receipt, |target| {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                target.fill(0x41);
                Ok::<StagedRecord, ()>(record)
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        assert_eq!(
            staging.shard_fill_snapshot(0),
            Err(StagingError::WouldBlock)
        );
        release_tx.send(()).unwrap();
        assert_eq!(encoder.join().unwrap().unwrap(), StageAppend::Appended);
        assert_eq!(
            staging.shard_fill_snapshot(0).unwrap(),
            Some(ShardFillSnapshot {
                bytes: 64,
                records: 1,
            })
        );
    }

    #[test]
    fn padding_receipt_expands_only_the_final_record_without_copying() {
        let resources = resources(4 * 1024 * 1024);
        let staging = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        let first_len = RecordHeader::aligned_len(0, 0).unwrap();
        assert_eq!(first_len as usize, RECORD_HEADER_SIZE);
        let (first, first_record) = reservation(4096, first_len, 11);
        let mut pointer = 0_usize;
        staging
            .encode_reserved(first, |target| {
                pointer = target.as_ptr() as usize;
                encode_test_value(target, first, first_record)
            })
            .unwrap();

        let second_offset = first.offset + first.record_bytes;
        let (second, second_record) = reservation(second_offset, 128, 12);
        let second_record = StagedRecord::new(
            second_record.hash(),
            second_record.entry(),
            StagedRecordKind::Tombstone,
        );
        staging
            .encode_reserved(second, |target| {
                encode_test_value(target, second, second_record)
            })
            .unwrap();
        let unpadded_end = u64::from(second.offset) + u64::from(second.record_bytes);
        let padding = RegionPaddingReceipt {
            shard_id: 0,
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
        assert_eq!(job.records[0].entry().location.record_len(), first_len);
        assert_eq!(
            job.records[1].entry().location.record_len(),
            padded_second_len
        );
        assert_eq!(job.records[1].kind(), StagedRecordKind::Tombstone);
        assert!(
            bytes[unpadded_end as usize - 4096..]
                .iter()
                .all(|byte| *byte == 0)
        );

        let StagedWrite {
            buffer, records, ..
        } = job;
        staging
            .finish_success(padded_span, buffer, records)
            .unwrap();
    }

    #[test]
    fn fixed_record_and_byte_bounds_request_a_seal_without_running_encoder() {
        let resources = resources(8 * 1024 * 1024);
        let chunk_bytes = 256 * 1024;
        let staging = RegionStaging::try_new(1, chunk_bytes, 512 * 1024, &resources).unwrap();
        let mut offset = 0;
        for index in 0..MAX_STAGING_RECORDS {
            let (receipt, record) = reservation(offset, RECORD_ALIGNMENT, index as u64 + 1);
            assert_eq!(
                staging
                    .encode_reserved(receipt, |target| {
                        target.fill(index as u8);
                        Ok::<StagedRecord, ()>(record)
                    })
                    .unwrap(),
                StageAppend::Appended
            );
            offset += RECORD_ALIGNMENT;
        }
        let (overflow, overflow_record) = reservation(offset, RECORD_ALIGNMENT, 4097);
        let mut called = false;
        assert_eq!(
            staging
                .encode_reserved(overflow, |_| {
                    called = true;
                    Ok::<StagedRecord, ()>(overflow_record)
                })
                .unwrap(),
            StageAppend::NeedsSeal
        );
        assert!(!called);

        let metadata_span = span(0, u64::from(offset), 4096, 4096, 1);
        let job = staging.take_sealed(metadata_span).unwrap().unwrap();
        let StagedWrite {
            buffer, records, ..
        } = job;
        staging
            .finish_success(metadata_span, buffer, records)
            .unwrap();

        let (full, full_record) = reservation(offset, chunk_bytes as u32, 4097);
        staging
            .encode_reserved(full, |target| {
                target.fill(0x5a);
                Ok::<StagedRecord, ()>(full_record)
            })
            .unwrap();
        let next_offset = offset + chunk_bytes as u32;
        let (next, next_record) = reservation(next_offset, RECORD_ALIGNMENT, 4098);
        called = false;
        assert_eq!(
            staging
                .encode_reserved(next, |_| {
                    called = true;
                    Ok::<StagedRecord, ()>(next_record)
                })
                .unwrap(),
            StageAppend::NeedsSeal
        );
        assert!(!called);
    }

    #[test]
    fn completion_fences_stale_receipts_and_write_failure_is_sticky() {
        let resources = resources(8 * 1024 * 1024);
        let staging = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        let (receipt, record) = reservation(4096, 64, 11);
        let entry = record.entry();
        let mismatched = StagedRecord::new(
            record.hash(),
            IndexEntry {
                location: entry.location,
                seqno: entry.seqno + 1,
            },
            record.kind(),
        );
        assert_eq!(
            staging.encode_reserved(receipt, |target| {
                target.fill(0x22);
                Ok::<StagedRecord, ()>(mismatched)
            }),
            Err(StagingEncodeError::Staging(StagingError::StaleReceipt))
        );
        staging
            .encode_reserved(receipt, |target| {
                target.fill(0x33);
                Ok::<StagedRecord, ()>(record)
            })
            .unwrap();
        let submitted = span(4096, 4160, 1, 11, 1);
        let job = staging.take_sealed(submitted).unwrap().unwrap();
        let mut stale = submitted;
        stale.span_id += 1;
        let StagedWrite {
            buffer, records, ..
        } = job;
        assert_eq!(
            staging.finish_success(stale, buffer, records),
            Err(StagingError::StaleReceipt)
        );
        let (later, later_record) = reservation(4160, 64, 12);
        assert_eq!(
            staging.encode_reserved(later, |_| Ok::<StagedRecord, ()>(later_record)),
            Err(StagingEncodeError::Staging(StagingError::Failed))
        );

        let other = RegionStaging::try_new(1, 4096, 64 * 1024, &resources).unwrap();
        other
            .encode_reserved(receipt, |target| {
                target.fill(0x44);
                Ok::<StagedRecord, ()>(record)
            })
            .unwrap();
        let job = other.take_sealed(submitted).unwrap().unwrap();
        let StagedWrite {
            buffer, records, ..
        } = job;
        // A failed driver may quarantine the owned I/O buffer. Staging still
        // fences the exact receipt and becomes terminal without waiting for a
        // resource which can no longer return.
        drop(buffer);
        other.finish_failure(submitted, None, records).unwrap();
        assert_eq!(
            other.encode_reserved(later, |_| Ok::<StagedRecord, ()>(later_record)),
            Err(StagingEncodeError::Staging(StagingError::Failed))
        );
    }
}
