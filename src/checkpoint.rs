//! On-disk codec for the restart index checkpoint tail.
//!
//! The checkpoint area starts immediately after the Format V1 data extent. It
//! consists of one 4 KiB directory page followed by two equal-sized slots. A
//! slot contains a 4 KiB commit header and a payload. Callers write and sync
//! the payload before publishing the header, so an interrupted write leaves
//! either the previous generation or a checksum-invalid slot.
//!
//! All integers are little-endian. Rust layouts are deliberately not part of
//! the format. The streaming payload encoder and decoder keep recovery memory
//! bounded; the slice helpers are conveniences for tests and small snapshots.

use std::fmt;

use crate::checksum::{Crc32c, crc32c};
use crate::format::{
    FORMAT_VERSION, RECORD_ALIGNMENT, REGION_HEADER_SIZE, RegionState, SUPERBLOCK_AREA_SIZE,
};
use crate::index::{MAX_INDEX_SHARDS, MAX_INDEX_SLOTS, MAX_REGION_ID, PackedLocation};

pub(crate) const CHECKPOINT_PAGE_SIZE: usize = 4 * 1024;
pub(crate) const CHECKPOINT_SLOT_COUNT: usize = 2;
pub(crate) const CHECKPOINT_DIRECTORY_SIZE: usize = CHECKPOINT_PAGE_SIZE;
pub(crate) const CHECKPOINT_SLOT_HEADER_SIZE: usize = CHECKPOINT_PAGE_SIZE;
pub(crate) const CHECKPOINT_REGION_SNAPSHOT_SIZE: usize = 40;
/// Size of an index entry written by the current checkpoint slot version.
pub(crate) const CHECKPOINT_INDEX_ENTRY_SIZE: usize = 40;
pub(crate) const CHECKPOINT_INDEX_ENTRY_V2_SIZE: usize = 32;
pub(crate) const CHECKPOINT_INDEX_ENTRY_V1_SIZE: usize = 24;

const CHECKPOINT_DIRECTORY_VERSION: u16 = 1;
const CHECKPOINT_SLOT_V1: u16 = 1;
const CHECKPOINT_SLOT_V2: u16 = 2;
const CHECKPOINT_SLOT_V3: u16 = 3;
const CHECKPOINT_SLOT_VERSION: u16 = 4;
const DIRECTORY_MAGIC: [u8; 8] = *b"CRCKPTD\0";
const SLOT_MAGIC: [u8; 8] = *b"CRCKPTS\0";
const MAX_CHECKPOINT_SLOT_SIZE: u64 = 16 * 1024 * 1024 * 1024;

const DIRECTORY_VERSION_OFFSET: usize = 8;
const DIRECTORY_SLOT_COUNT_OFFSET: usize = 10;
const DIRECTORY_PAGE_SIZE_OFFSET: usize = 12;
const DIRECTORY_SLOT_SIZE_OFFSET: usize = 16;
const DIRECTORY_DATA_FILE_LEN_OFFSET: usize = 24;
const DIRECTORY_REGION_SIZE_OFFSET: usize = 32;
const DIRECTORY_REGION_COUNT_OFFSET: usize = 40;
const DIRECTORY_DATA_FORMAT_VERSION_OFFSET: usize = 44;
const DIRECTORY_FIELDS_END: usize = 46;
const DIRECTORY_CRC_OFFSET: usize = CHECKPOINT_DIRECTORY_SIZE - size_of::<u32>();

const SLOT_VERSION_OFFSET: usize = 8;
const SLOT_ID_OFFSET: usize = 10;
const SLOT_PAGE_SIZE_OFFSET: usize = 12;
const SLOT_GENERATION_OFFSET: usize = 16;
const SLOT_PAYLOAD_LEN_OFFSET: usize = 24;
const SLOT_REGION_COUNT_OFFSET: usize = 32;
const SLOT_ENTRY_COUNT_OFFSET: usize = 36;
const SLOT_EPOCH_OFFSET: usize = 40;
const SLOT_DATA_FORMAT_VERSION_OFFSET: usize = 44;
const SLOT_EPOCH_START_SEQNO_OFFSET: usize = 48;
const SLOT_MAX_SEQNO_OFFSET: usize = 56;
const SLOT_SUPERBLOCK_GENERATION_OFFSET: usize = 64;
const SLOT_HASH_SEED_OFFSET: usize = 72;
const SLOT_DATA_FILE_LEN_OFFSET: usize = 80;
const SLOT_REGION_SIZE_OFFSET: usize = 88;
const SLOT_PAYLOAD_CRC_OFFSET: usize = 96;
const SLOT_INDEX_SLOTS_OFFSET: usize = 100;
const SLOT_INDEX_SHARDS_OFFSET: usize = 104;
const SLOT_V3_FIELDS_END: usize = 100;
const SLOT_FIELDS_END: usize = 108;
const SLOT_HEADER_CRC_OFFSET: usize = CHECKPOINT_SLOT_HEADER_SIZE - size_of::<u32>();

const REGION_ID_OFFSET: usize = 0;
const REGION_INCARNATION_OFFSET: usize = 4;
const REGION_STATE_OFFSET: usize = 8;
const REGION_LANE_ID_OFFSET: usize = 9;
const REGION_USED_OFFSET: usize = 16;
const REGION_CREATED_SEQNO_OFFSET: usize = 24;
const REGION_MAX_SEQNO_OFFSET: usize = 32;

const INDEX_KEY_HASH_OFFSET: usize = 0;
const INDEX_LOCATION_OFFSET: usize = 8;
const INDEX_SEQNO_OFFSET: usize = 16;
const INDEX_NAMESPACE_ID_OFFSET: usize = 24;
const INDEX_FLAGS_OFFSET: usize = 28;
const INDEX_PHYSICAL_SLOT_OFFSET: usize = 32;
const INDEX_FIELDS_END: usize = 36;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CheckpointCodecError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion(u16),
    ChecksumMismatch,
    InvalidField(&'static str),
    ArithmeticOverflow,
    PayloadTooLarge,
}

impl fmt::Display for CheckpointCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength => formatter.write_str("invalid checkpoint encoded length"),
            Self::InvalidMagic => formatter.write_str("invalid checkpoint magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported checkpoint version {version}")
            }
            Self::ChecksumMismatch => formatter.write_str("checkpoint checksum mismatch"),
            Self::InvalidField(field) => write!(formatter, "invalid checkpoint field: {field}"),
            Self::ArithmeticOverflow => formatter.write_str("checkpoint size or offset overflow"),
            Self::PayloadTooLarge => formatter.write_str("checkpoint payload exceeds its slot"),
        }
    }
}

impl std::error::Error for CheckpointCodecError {}

type CodecResult<T> = Result<T, CheckpointCodecError>;

/// Static description of the two checkpoint slots.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointDirectory {
    pub(crate) data_file_len: u64,
    pub(crate) region_size: u64,
    pub(crate) region_count: u32,
    pub(crate) slot_size: u64,
}

impl CheckpointDirectory {
    /// Builds a directory sized for every configured index slot.
    pub(crate) fn for_index_capacity(
        data_file_len: u64,
        region_size: u64,
        region_count: u32,
        index_capacity: usize,
    ) -> CodecResult<Self> {
        let slot_size = required_slot_size(region_count, index_capacity)?;
        let directory = Self {
            data_file_len,
            region_size,
            region_count,
            slot_size,
        };
        directory.validate()?;
        Ok(directory)
    }

    pub(crate) fn encode(self) -> CodecResult<[u8; CHECKPOINT_DIRECTORY_SIZE]> {
        self.validate()?;
        let mut output = [0_u8; CHECKPOINT_DIRECTORY_SIZE];
        output[..DIRECTORY_MAGIC.len()].copy_from_slice(&DIRECTORY_MAGIC);
        put_u16(
            &mut output,
            DIRECTORY_VERSION_OFFSET,
            CHECKPOINT_DIRECTORY_VERSION,
        );
        put_u16(
            &mut output,
            DIRECTORY_SLOT_COUNT_OFFSET,
            CHECKPOINT_SLOT_COUNT as u16,
        );
        put_u32(
            &mut output,
            DIRECTORY_PAGE_SIZE_OFFSET,
            CHECKPOINT_PAGE_SIZE as u32,
        );
        put_u64(&mut output, DIRECTORY_SLOT_SIZE_OFFSET, self.slot_size);
        put_u64(
            &mut output,
            DIRECTORY_DATA_FILE_LEN_OFFSET,
            self.data_file_len,
        );
        put_u64(&mut output, DIRECTORY_REGION_SIZE_OFFSET, self.region_size);
        put_u32(
            &mut output,
            DIRECTORY_REGION_COUNT_OFFSET,
            self.region_count,
        );
        put_u16(
            &mut output,
            DIRECTORY_DATA_FORMAT_VERSION_OFFSET,
            FORMAT_VERSION,
        );
        let checksum = crc32c(&output);
        put_u32(&mut output, DIRECTORY_CRC_OFFSET, checksum);
        Ok(output)
    }

    pub(crate) fn decode(input: &[u8]) -> CodecResult<Self> {
        require_page(
            input,
            CHECKPOINT_DIRECTORY_SIZE,
            DIRECTORY_MAGIC,
            DIRECTORY_VERSION_OFFSET,
            DIRECTORY_CRC_OFFSET,
            CHECKPOINT_DIRECTORY_VERSION,
            CHECKPOINT_DIRECTORY_VERSION,
        )?;
        if get_u16(input, DIRECTORY_SLOT_COUNT_OFFSET)? != CHECKPOINT_SLOT_COUNT as u16 {
            return Err(CheckpointCodecError::InvalidField("slot_count"));
        }
        if get_u32(input, DIRECTORY_PAGE_SIZE_OFFSET)? != CHECKPOINT_PAGE_SIZE as u32 {
            return Err(CheckpointCodecError::InvalidField("page_size"));
        }
        if get_u16(input, DIRECTORY_DATA_FORMAT_VERSION_OFFSET)? != FORMAT_VERSION {
            return Err(CheckpointCodecError::InvalidField("data_format_version"));
        }
        if input[DIRECTORY_FIELDS_END..DIRECTORY_CRC_OFFSET]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(CheckpointCodecError::InvalidField("directory_reserved"));
        }
        let directory = Self {
            data_file_len: get_u64(input, DIRECTORY_DATA_FILE_LEN_OFFSET)?,
            region_size: get_u64(input, DIRECTORY_REGION_SIZE_OFFSET)?,
            region_count: get_u32(input, DIRECTORY_REGION_COUNT_OFFSET)?,
            slot_size: get_u64(input, DIRECTORY_SLOT_SIZE_OFFSET)?,
        };
        directory.validate()?;
        Ok(directory)
    }

    pub(crate) fn payload_capacity(self) -> u64 {
        self.slot_size - CHECKPOINT_SLOT_HEADER_SIZE as u64
    }

    #[cfg(test)]
    pub(crate) fn directory_offset(self) -> u64 {
        self.data_file_len
    }

    pub(crate) fn slot_header_offset(self, slot: usize) -> CodecResult<u64> {
        validate_slot(slot)?;
        let relative = (CHECKPOINT_DIRECTORY_SIZE as u64)
            .checked_add(
                u64::try_from(slot)
                    .map_err(|_| CheckpointCodecError::ArithmeticOverflow)?
                    .checked_mul(self.slot_size)
                    .ok_or(CheckpointCodecError::ArithmeticOverflow)?,
            )
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        self.data_file_len
            .checked_add(relative)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)
    }

    pub(crate) fn slot_payload_offset(self, slot: usize) -> CodecResult<u64> {
        self.slot_header_offset(slot)?
            .checked_add(CHECKPOINT_SLOT_HEADER_SIZE as u64)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)
    }

    pub(crate) fn total_file_len(self) -> CodecResult<u64> {
        self.data_file_len
            .checked_add(CHECKPOINT_DIRECTORY_SIZE as u64)
            .and_then(|length| {
                self.slot_size
                    .checked_mul(CHECKPOINT_SLOT_COUNT as u64)
                    .and_then(|slots| length.checked_add(slots))
            })
            .ok_or(CheckpointCodecError::ArithmeticOverflow)
    }

    fn validate(self) -> CodecResult<()> {
        if self.region_count == 0 || self.region_count > MAX_REGION_ID {
            return Err(CheckpointCodecError::InvalidField("region_count"));
        }
        if self.region_size <= REGION_HEADER_SIZE as u64
            || self.region_size % CHECKPOINT_PAGE_SIZE as u64 != 0
        {
            return Err(CheckpointCodecError::InvalidField("region_size"));
        }
        let expected_data_file_len = SUPERBLOCK_AREA_SIZE
            .checked_add(
                u64::from(self.region_count)
                    .checked_mul(self.region_size)
                    .ok_or(CheckpointCodecError::ArithmeticOverflow)?,
            )
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        if self.data_file_len != expected_data_file_len
            || self.data_file_len % CHECKPOINT_PAGE_SIZE as u64 != 0
        {
            return Err(CheckpointCodecError::InvalidField("data_file_len"));
        }
        if self.slot_size % CHECKPOINT_PAGE_SIZE as u64 != 0
            || self.slot_size > MAX_CHECKPOINT_SLOT_SIZE
        {
            return Err(CheckpointCodecError::InvalidField("slot_size"));
        }
        let minimum = required_slot_size(self.region_count, 0)?;
        if self.slot_size < minimum {
            return Err(CheckpointCodecError::InvalidField("slot_size"));
        }
        let _ = self.total_file_len()?;
        Ok(())
    }
}

/// Values shared by the commit header and its main Format V1 superblock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointSnapshotMeta {
    pub(crate) generation: u64,
    pub(crate) superblock_generation: u64,
    pub(crate) epoch: u32,
    pub(crate) epoch_start_seqno: u64,
    pub(crate) max_seqno: u64,
    pub(crate) hash_seed: u64,
    pub(crate) index_slots: u32,
    pub(crate) index_shards: u32,
}

/// The 4 KiB commit record written after its payload is durable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointSlotHeader {
    pub(crate) version: u16,
    pub(crate) slot: u8,
    pub(crate) generation: u64,
    pub(crate) payload_len: u64,
    pub(crate) region_count: u32,
    pub(crate) entry_count: u32,
    pub(crate) epoch: u32,
    pub(crate) epoch_start_seqno: u64,
    pub(crate) max_seqno: u64,
    pub(crate) superblock_generation: u64,
    pub(crate) hash_seed: u64,
    pub(crate) payload_crc: u32,
    /// Source in-memory index capacity. Present only in checkpoint v4+.
    pub(crate) index_slots: Option<u32>,
    /// Source in-memory shard count. Present only in checkpoint v4+.
    pub(crate) index_shards: Option<u32>,
}

impl CheckpointSlotHeader {
    pub(crate) fn new(
        slot: usize,
        metadata: CheckpointSnapshotMeta,
        payload: CheckpointPayloadSummary,
        directory: CheckpointDirectory,
    ) -> CodecResult<Self> {
        validate_slot(slot)?;
        let header = Self {
            version: CHECKPOINT_SLOT_VERSION,
            slot: slot as u8,
            generation: metadata.generation,
            payload_len: payload.payload_len,
            region_count: payload.region_count,
            entry_count: payload.entry_count,
            epoch: metadata.epoch,
            epoch_start_seqno: metadata.epoch_start_seqno,
            max_seqno: metadata.max_seqno,
            superblock_generation: metadata.superblock_generation,
            hash_seed: metadata.hash_seed,
            payload_crc: payload.payload_crc,
            index_slots: Some(metadata.index_slots),
            index_shards: Some(metadata.index_shards),
        };
        header.validate(directory, slot)?;
        Ok(header)
    }

    pub(crate) fn encode(
        self,
        directory: CheckpointDirectory,
    ) -> CodecResult<[u8; CHECKPOINT_SLOT_HEADER_SIZE]> {
        self.validate(directory, usize::from(self.slot))?;
        let mut output = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        output[..SLOT_MAGIC.len()].copy_from_slice(&SLOT_MAGIC);
        put_u16(&mut output, SLOT_VERSION_OFFSET, self.version);
        output[SLOT_ID_OFFSET] = self.slot;
        put_u32(
            &mut output,
            SLOT_PAGE_SIZE_OFFSET,
            CHECKPOINT_PAGE_SIZE as u32,
        );
        put_u64(&mut output, SLOT_GENERATION_OFFSET, self.generation);
        put_u64(&mut output, SLOT_PAYLOAD_LEN_OFFSET, self.payload_len);
        put_u32(&mut output, SLOT_REGION_COUNT_OFFSET, self.region_count);
        put_u32(&mut output, SLOT_ENTRY_COUNT_OFFSET, self.entry_count);
        put_u32(&mut output, SLOT_EPOCH_OFFSET, self.epoch);
        put_u16(&mut output, SLOT_DATA_FORMAT_VERSION_OFFSET, FORMAT_VERSION);
        put_u64(
            &mut output,
            SLOT_EPOCH_START_SEQNO_OFFSET,
            self.epoch_start_seqno,
        );
        put_u64(&mut output, SLOT_MAX_SEQNO_OFFSET, self.max_seqno);
        put_u64(
            &mut output,
            SLOT_SUPERBLOCK_GENERATION_OFFSET,
            self.superblock_generation,
        );
        put_u64(&mut output, SLOT_HASH_SEED_OFFSET, self.hash_seed);
        put_u64(
            &mut output,
            SLOT_DATA_FILE_LEN_OFFSET,
            directory.data_file_len,
        );
        put_u64(&mut output, SLOT_REGION_SIZE_OFFSET, directory.region_size);
        put_u32(&mut output, SLOT_PAYLOAD_CRC_OFFSET, self.payload_crc);
        if self.version >= CHECKPOINT_SLOT_VERSION {
            put_u32(
                &mut output,
                SLOT_INDEX_SLOTS_OFFSET,
                self.index_slots
                    .ok_or(CheckpointCodecError::InvalidField("index_slots"))?,
            );
            put_u32(
                &mut output,
                SLOT_INDEX_SHARDS_OFFSET,
                self.index_shards
                    .ok_or(CheckpointCodecError::InvalidField("index_shards"))?,
            );
        }
        let checksum = crc32c(&output);
        put_u32(&mut output, SLOT_HEADER_CRC_OFFSET, checksum);
        Ok(output)
    }

    pub(crate) fn decode(
        input: &[u8],
        directory: CheckpointDirectory,
        expected_slot: usize,
    ) -> CodecResult<Self> {
        validate_slot(expected_slot)?;
        let version = require_page(
            input,
            CHECKPOINT_SLOT_HEADER_SIZE,
            SLOT_MAGIC,
            SLOT_VERSION_OFFSET,
            SLOT_HEADER_CRC_OFFSET,
            CHECKPOINT_SLOT_V1,
            CHECKPOINT_SLOT_VERSION,
        )?;
        if get_u32(input, SLOT_PAGE_SIZE_OFFSET)? != CHECKPOINT_PAGE_SIZE as u32 {
            return Err(CheckpointCodecError::InvalidField("page_size"));
        }
        if get_u16(input, SLOT_DATA_FORMAT_VERSION_OFFSET)? != FORMAT_VERSION {
            return Err(CheckpointCodecError::InvalidField("data_format_version"));
        }
        let fields_end = if version >= CHECKPOINT_SLOT_VERSION {
            SLOT_FIELDS_END
        } else {
            SLOT_V3_FIELDS_END
        };
        if input[11] != 0
            || input[46..48].iter().any(|byte| *byte != 0)
            || input[fields_end..SLOT_HEADER_CRC_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
        {
            return Err(CheckpointCodecError::InvalidField("slot_reserved"));
        }
        if get_u64(input, SLOT_DATA_FILE_LEN_OFFSET)? != directory.data_file_len
            || get_u64(input, SLOT_REGION_SIZE_OFFSET)? != directory.region_size
        {
            return Err(CheckpointCodecError::InvalidField("slot_layout"));
        }
        let header = Self {
            version,
            slot: input[SLOT_ID_OFFSET],
            generation: get_u64(input, SLOT_GENERATION_OFFSET)?,
            payload_len: get_u64(input, SLOT_PAYLOAD_LEN_OFFSET)?,
            region_count: get_u32(input, SLOT_REGION_COUNT_OFFSET)?,
            entry_count: get_u32(input, SLOT_ENTRY_COUNT_OFFSET)?,
            epoch: get_u32(input, SLOT_EPOCH_OFFSET)?,
            epoch_start_seqno: get_u64(input, SLOT_EPOCH_START_SEQNO_OFFSET)?,
            max_seqno: get_u64(input, SLOT_MAX_SEQNO_OFFSET)?,
            superblock_generation: get_u64(input, SLOT_SUPERBLOCK_GENERATION_OFFSET)?,
            hash_seed: get_u64(input, SLOT_HASH_SEED_OFFSET)?,
            payload_crc: get_u32(input, SLOT_PAYLOAD_CRC_OFFSET)?,
            index_slots: if version >= CHECKPOINT_SLOT_VERSION {
                Some(get_u32(input, SLOT_INDEX_SLOTS_OFFSET)?)
            } else {
                None
            },
            index_shards: if version >= CHECKPOINT_SLOT_VERSION {
                Some(get_u32(input, SLOT_INDEX_SHARDS_OFFSET)?)
            } else {
                None
            },
        };
        header.validate(directory, expected_slot)?;
        Ok(header)
    }

    fn validate(self, directory: CheckpointDirectory, expected_slot: usize) -> CodecResult<()> {
        directory.validate()?;
        validate_slot(expected_slot)?;
        if usize::from(self.slot) != expected_slot {
            return Err(CheckpointCodecError::InvalidField("slot_id"));
        }
        if !(CHECKPOINT_SLOT_V1..=CHECKPOINT_SLOT_VERSION).contains(&self.version) {
            return Err(CheckpointCodecError::UnsupportedVersion(self.version));
        }
        if self.generation == 0 {
            return Err(CheckpointCodecError::InvalidField("generation"));
        }
        if self.superblock_generation == 0 {
            return Err(CheckpointCodecError::InvalidField("superblock_generation"));
        }
        if self.region_count != directory.region_count {
            return Err(CheckpointCodecError::InvalidField("region_count"));
        }
        if usize::try_from(self.entry_count).map_or(true, |count| count > MAX_INDEX_SLOTS) {
            return Err(CheckpointCodecError::InvalidField("entry_count"));
        }
        match (self.version, self.index_slots, self.index_shards) {
            (CHECKPOINT_SLOT_VERSION, Some(slots), Some(shards))
                if (8..=MAX_INDEX_SLOTS as u32).contains(&slots)
                    && (1..=MAX_INDEX_SHARDS as u32).contains(&shards)
                    && shards.is_power_of_two()
                    && shards <= slots / 8 => {}
            (CHECKPOINT_SLOT_V1 | CHECKPOINT_SLOT_V2 | CHECKPOINT_SLOT_V3, None, None) => {}
            _ => return Err(CheckpointCodecError::InvalidField("index_slots")),
        }
        let expected_payload_len = payload_len_for_version(
            self.region_count,
            self.entry_count,
            self.version,
            self.index_slots,
        )?;
        if self.payload_len != expected_payload_len {
            return Err(CheckpointCodecError::InvalidField("payload_len"));
        }
        if self.payload_len > directory.payload_capacity() {
            return Err(CheckpointCodecError::PayloadTooLarge);
        }
        validate_sequence_bounds(self.epoch_start_seqno, self.max_seqno)?;
        if self.epoch == 0 {
            return Err(CheckpointCodecError::InvalidField("epoch"));
        }
        Ok(())
    }

    pub(crate) const fn index_entry_size(self) -> usize {
        match self.version {
            CHECKPOINT_SLOT_V1 => CHECKPOINT_INDEX_ENTRY_V1_SIZE,
            CHECKPOINT_SLOT_V2 | CHECKPOINT_SLOT_V3 => CHECKPOINT_INDEX_ENTRY_V2_SIZE,
            _ => CHECKPOINT_INDEX_ENTRY_SIZE,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointRegionSnapshot {
    pub(crate) region_id: u32,
    pub(crate) incarnation: u32,
    pub(crate) state: RegionState,
    /// Append lane for an Active Region. Checkpoint v1/v2 snapshots decode
    /// this as `None`; v3 stores `lane_id + 1` in a formerly reserved byte.
    pub(crate) lane_id: Option<u8>,
    pub(crate) used: u64,
    pub(crate) created_seqno: u64,
    /// Maximum record sequence present in this incarnation, or zero when the
    /// region contains no records.
    pub(crate) max_seqno: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointIndexEntry {
    pub(crate) key_hash: u64,
    pub(crate) location: PackedLocation,
    pub(crate) seqno: u64,
    pub(crate) namespace_id: u32,
    pub(crate) flags: u32,
    /// Global physical slot ordinal, present in checkpoint v4+.
    pub(crate) physical_slot: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CheckpointPayloadSummary {
    pub(crate) payload_len: u64,
    pub(crate) payload_crc: u32,
    pub(crate) region_count: u32,
    pub(crate) entry_count: u32,
}

/// Streaming payload encoder. Returned records must be written in call order.
pub(crate) struct CheckpointPayloadEncoder {
    directory: CheckpointDirectory,
    epoch_start_seqno: u64,
    max_seqno: u64,
    entry_count: u32,
    index_slots: u32,
    regions_written: u32,
    entries_written: u32,
    bytes_written: u64,
    checksum: Crc32c,
}

impl CheckpointPayloadEncoder {
    pub(crate) fn new(
        directory: CheckpointDirectory,
        epoch_start_seqno: u64,
        max_seqno: u64,
        entry_count: u32,
        index_slots: u32,
    ) -> CodecResult<Self> {
        directory.validate()?;
        validate_sequence_bounds(epoch_start_seqno, max_seqno)?;
        let length = payload_len(directory.region_count, entry_count, index_slots)?;
        if length > directory.payload_capacity() {
            return Err(CheckpointCodecError::PayloadTooLarge);
        }
        Ok(Self {
            directory,
            epoch_start_seqno,
            max_seqno,
            entry_count,
            index_slots,
            regions_written: 0,
            entries_written: 0,
            bytes_written: 0,
            checksum: Crc32c::new(),
        })
    }

    pub(crate) fn encode_region(
        &mut self,
        region: CheckpointRegionSnapshot,
    ) -> CodecResult<[u8; CHECKPOINT_REGION_SNAPSHOT_SIZE]> {
        if self.regions_written >= self.directory.region_count || self.entries_written != 0 {
            return Err(CheckpointCodecError::InvalidField("payload_order"));
        }
        let output =
            encode_region_snapshot(region, self.regions_written, self.directory, self.max_seqno)?;
        self.checksum.update(&output);
        self.regions_written += 1;
        self.bytes_written = self
            .bytes_written
            .checked_add(CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        Ok(output)
    }

    /// Encodes an index entry after the caller supplies its owning region
    /// snapshot. Supplying it avoids retaining a second region table here.
    pub(crate) fn encode_index_entry(
        &mut self,
        entry: CheckpointIndexEntry,
        owning_region: CheckpointRegionSnapshot,
    ) -> CodecResult<[u8; CHECKPOINT_INDEX_ENTRY_SIZE]> {
        if self.regions_written != self.directory.region_count
            || self.entries_written >= self.entry_count
        {
            return Err(CheckpointCodecError::InvalidField("payload_order"));
        }
        if entry
            .physical_slot
            .is_none_or(|physical_slot| physical_slot >= self.index_slots)
        {
            return Err(CheckpointCodecError::InvalidField("physical_slot"));
        }
        let output = encode_checkpoint_index_entry(
            entry,
            owning_region,
            self.directory,
            self.epoch_start_seqno,
            self.max_seqno,
        )?;
        self.checksum.update(&output);
        self.entries_written += 1;
        self.bytes_written = self
            .bytes_written
            .checked_add(CHECKPOINT_INDEX_ENTRY_SIZE as u64)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        Ok(output)
    }

    pub(crate) fn finish(self) -> CodecResult<CheckpointPayloadSummary> {
        if self.regions_written != self.directory.region_count
            || self.entries_written != self.entry_count
            || self.bytes_written
                != payload_len(
                    self.directory.region_count,
                    self.entry_count,
                    self.index_slots,
                )?
        {
            return Err(CheckpointCodecError::InvalidLength);
        }
        Ok(CheckpointPayloadSummary {
            payload_len: self.bytes_written,
            payload_crc: self.checksum.finish(),
            region_count: self.directory.region_count,
            entry_count: self.entry_count,
        })
    }
}

/// Streaming verifier for a payload read in fixed-size records.
pub(crate) struct CheckpointPayloadDecoder {
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
    regions_read: u32,
    entries_read: u32,
    bytes_read: u64,
    checksum: Crc32c,
}

impl CheckpointPayloadDecoder {
    pub(crate) fn new(
        directory: CheckpointDirectory,
        header: CheckpointSlotHeader,
    ) -> CodecResult<Self> {
        header.validate(directory, usize::from(header.slot))?;
        Ok(Self {
            directory,
            header,
            regions_read: 0,
            entries_read: 0,
            bytes_read: 0,
            checksum: Crc32c::new(),
        })
    }

    pub(crate) fn decode_region(&mut self, input: &[u8]) -> CodecResult<CheckpointRegionSnapshot> {
        if input.len() != CHECKPOINT_REGION_SNAPSHOT_SIZE
            || self.regions_read >= self.header.region_count
            || self.entries_read != 0
        {
            return Err(CheckpointCodecError::InvalidLength);
        }
        let region = decode_region_snapshot(input, self.header.version)?;
        validate_region(
            region,
            self.regions_read,
            self.directory,
            self.header.max_seqno,
        )?;
        self.checksum.update(input);
        self.regions_read += 1;
        self.bytes_read = self
            .bytes_read
            .checked_add(CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        Ok(region)
    }

    pub(crate) fn decode_index_entry(
        &mut self,
        input: &[u8],
        owning_region: CheckpointRegionSnapshot,
    ) -> CodecResult<CheckpointIndexEntry> {
        if input.len() != self.header.index_entry_size()
            || self.regions_read != self.header.region_count
            || self.entries_read >= self.header.entry_count
        {
            return Err(CheckpointCodecError::InvalidLength);
        }
        let entry = decode_checkpoint_index_entry(input)?;
        if self.header.version >= CHECKPOINT_SLOT_VERSION
            && entry.physical_slot.is_none_or(|slot| {
                self.header
                    .index_slots
                    .is_none_or(|index_slots| slot >= index_slots)
            })
        {
            return Err(CheckpointCodecError::InvalidField("physical_slot"));
        }
        validate_index_entry(
            entry,
            owning_region,
            self.directory,
            self.header.epoch_start_seqno,
            self.header.max_seqno,
        )?;
        self.checksum.update(input);
        self.entries_read += 1;
        self.bytes_read = self
            .bytes_read
            .checked_add(input.len() as u64)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        Ok(entry)
    }

    pub(crate) const fn index_entry_size(&self) -> usize {
        self.header.index_entry_size()
    }

    pub(crate) fn finish(self) -> CodecResult<()> {
        if self.regions_read != self.header.region_count
            || self.entries_read != self.header.entry_count
            || self.bytes_read != self.header.payload_len
        {
            return Err(CheckpointCodecError::InvalidLength);
        }
        if self.checksum.finish() != self.header.payload_crc {
            return Err(CheckpointCodecError::ChecksumMismatch);
        }
        Ok(())
    }
}

/// Borrowed, checksum-verified payload view.
#[cfg(test)]
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct DecodedCheckpointPayload<'a> {
    region_bytes: &'a [u8],
    entry_bytes: &'a [u8],
    entry_size: usize,
    version: u16,
}

#[cfg(test)]
impl DecodedCheckpointPayload<'_> {
    pub(crate) fn regions(&self) -> impl ExactSizeIterator<Item = CheckpointRegionSnapshot> + '_ {
        self.region_bytes
            .chunks_exact(CHECKPOINT_REGION_SNAPSHOT_SIZE)
            .map(|record| {
                decode_region_snapshot(record, self.version).expect("verified checkpoint region")
            })
    }

    pub(crate) fn entries(&self) -> impl ExactSizeIterator<Item = CheckpointIndexEntry> + '_ {
        self.entry_bytes
            .chunks_exact(self.entry_size)
            .map(|record| {
                decode_checkpoint_index_entry(record).expect("verified checkpoint index entry")
            })
    }
}

#[cfg(test)]
pub(crate) fn encode_payload_into(
    directory: CheckpointDirectory,
    epoch_start_seqno: u64,
    max_seqno: u64,
    index_slots: u32,
    regions: &[CheckpointRegionSnapshot],
    entries: &[CheckpointIndexEntry],
    output: &mut [u8],
) -> CodecResult<CheckpointPayloadSummary> {
    let entry_count = u32::try_from(entries.len())
        .map_err(|_| CheckpointCodecError::InvalidField("entry_count"))?;
    let expected_len = usize::try_from(payload_len(
        directory.region_count,
        entry_count,
        index_slots,
    )?)
    .map_err(|_| CheckpointCodecError::ArithmeticOverflow)?;
    if output.len() != expected_len || regions.len() != directory.region_count as usize {
        return Err(CheckpointCodecError::InvalidLength);
    }
    let mut encoder = CheckpointPayloadEncoder::new(
        directory,
        epoch_start_seqno,
        max_seqno,
        entry_count,
        index_slots,
    )?;
    let mut cursor = 0;
    for &region in regions {
        let encoded = encoder.encode_region(region)?;
        output[cursor..cursor + encoded.len()].copy_from_slice(&encoded);
        cursor += encoded.len();
    }
    for &entry in entries {
        let owner = regions
            .get(entry.location.region_id() as usize)
            .copied()
            .ok_or(CheckpointCodecError::InvalidField("entry_region"))?;
        let encoded = encoder.encode_index_entry(entry, owner)?;
        output[cursor..cursor + encoded.len()].copy_from_slice(&encoded);
        cursor += encoded.len();
    }
    debug_assert_eq!(cursor, output.len());
    encoder.finish()
}

#[cfg(test)]
pub(crate) fn decode_payload(
    directory: CheckpointDirectory,
    header: CheckpointSlotHeader,
    input: &[u8],
) -> CodecResult<DecodedCheckpointPayload<'_>> {
    let expected_len = usize::try_from(header.payload_len)
        .map_err(|_| CheckpointCodecError::ArithmeticOverflow)?;
    if input.len() != expected_len {
        return Err(CheckpointCodecError::InvalidLength);
    }
    let region_bytes_len = (header.region_count as usize)
        .checked_mul(CHECKPOINT_REGION_SNAPSHOT_SIZE)
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let (region_bytes, remaining) = input
        .split_at_checked(region_bytes_len)
        .ok_or(CheckpointCodecError::InvalidLength)?;
    let entry_bytes = remaining;
    let mut decoder = CheckpointPayloadDecoder::new(directory, header)?;
    for record in region_bytes.chunks_exact(CHECKPOINT_REGION_SNAPSHOT_SIZE) {
        decoder.decode_region(record)?;
    }
    for record in entry_bytes.chunks_exact(header.index_entry_size()) {
        let raw_location = get_u64(record, INDEX_LOCATION_OFFSET)?;
        let region_id = PackedLocation::from_raw(raw_location).region_id() as usize;
        let start = region_id
            .checked_mul(CHECKPOINT_REGION_SNAPSHOT_SIZE)
            .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
        let owner = region_bytes
            .get(start..start + CHECKPOINT_REGION_SNAPSHOT_SIZE)
            .ok_or(CheckpointCodecError::InvalidField("entry_region"))?;
        decoder.decode_index_entry(record, decode_region_snapshot(owner, header.version)?)?;
    }
    decoder.finish()?;
    Ok(DecodedCheckpointPayload {
        region_bytes,
        entry_bytes,
        entry_size: header.index_entry_size(),
        version: header.version,
    })
}

pub(crate) fn required_slot_size(region_count: u32, max_index_entries: usize) -> CodecResult<u64> {
    if region_count == 0 || region_count > MAX_REGION_ID {
        return Err(CheckpointCodecError::InvalidField("region_count"));
    }
    if max_index_entries > MAX_INDEX_SLOTS {
        return Err(CheckpointCodecError::InvalidField("index_capacity"));
    }
    let entry_count =
        u32::try_from(max_index_entries).map_err(|_| CheckpointCodecError::ArithmeticOverflow)?;
    let index_slots =
        u32::try_from(max_index_entries).map_err(|_| CheckpointCodecError::ArithmeticOverflow)?;
    let unaligned = (CHECKPOINT_SLOT_HEADER_SIZE as u64)
        .checked_add(payload_len(region_count, entry_count, index_slots)?)
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let aligned = align_up_u64(unaligned, CHECKPOINT_PAGE_SIZE as u64)?;
    if aligned > MAX_CHECKPOINT_SLOT_SIZE {
        return Err(CheckpointCodecError::PayloadTooLarge);
    }
    Ok(aligned)
}

pub(crate) fn padded_payload_len(payload_len: u64) -> CodecResult<u64> {
    align_up_u64(payload_len, CHECKPOINT_PAGE_SIZE as u64)
}

fn payload_len(region_count: u32, entry_count: u32, index_slots: u32) -> CodecResult<u64> {
    payload_len_for_version(
        region_count,
        entry_count,
        CHECKPOINT_SLOT_VERSION,
        Some(index_slots),
    )
}

fn payload_len_for_version(
    region_count: u32,
    entry_count: u32,
    version: u16,
    index_slots: Option<u32>,
) -> CodecResult<u64> {
    if region_count == 0 || region_count > MAX_REGION_ID {
        return Err(CheckpointCodecError::InvalidField("region_count"));
    }
    if usize::try_from(entry_count).map_or(true, |count| count > MAX_INDEX_SLOTS) {
        return Err(CheckpointCodecError::InvalidField("entry_count"));
    }
    let entry_size = match version {
        CHECKPOINT_SLOT_V1 => CHECKPOINT_INDEX_ENTRY_V1_SIZE,
        CHECKPOINT_SLOT_V2 | CHECKPOINT_SLOT_V3 => CHECKPOINT_INDEX_ENTRY_V2_SIZE,
        CHECKPOINT_SLOT_VERSION => CHECKPOINT_INDEX_ENTRY_SIZE,
        _ => return Err(CheckpointCodecError::UnsupportedVersion(version)),
    };
    if version >= CHECKPOINT_SLOT_VERSION {
        index_slots.ok_or(CheckpointCodecError::InvalidField("index_slots"))?;
    } else if index_slots.is_some() {
        return Err(CheckpointCodecError::InvalidField("index_slots"));
    }
    u64::from(region_count)
        .checked_mul(CHECKPOINT_REGION_SNAPSHOT_SIZE as u64)
        .and_then(|regions| {
            u64::from(entry_count)
                .checked_mul(entry_size as u64)
                .and_then(|entries| regions.checked_add(entries))
        })
        .ok_or(CheckpointCodecError::ArithmeticOverflow)
}

pub(crate) fn encode_region_snapshot(
    region: CheckpointRegionSnapshot,
    expected_region_id: u32,
    directory: CheckpointDirectory,
    global_max_seqno: u64,
) -> CodecResult<[u8; CHECKPOINT_REGION_SNAPSHOT_SIZE]> {
    validate_region(region, expected_region_id, directory, global_max_seqno)?;
    let mut output = [0_u8; CHECKPOINT_REGION_SNAPSHOT_SIZE];
    put_u32(&mut output, REGION_ID_OFFSET, region.region_id);
    put_u32(&mut output, REGION_INCARNATION_OFFSET, region.incarnation);
    output[REGION_STATE_OFFSET] = region.state as u8;
    if let Some(lane_id) = region.lane_id {
        output[REGION_LANE_ID_OFFSET] = lane_id
            .checked_add(1)
            .ok_or(CheckpointCodecError::InvalidField("region_lane_id"))?;
    }
    put_u64(&mut output, REGION_USED_OFFSET, region.used);
    put_u64(
        &mut output,
        REGION_CREATED_SEQNO_OFFSET,
        region.created_seqno,
    );
    put_u64(&mut output, REGION_MAX_SEQNO_OFFSET, region.max_seqno);
    Ok(output)
}

pub(crate) fn encode_checkpoint_index_entry(
    entry: CheckpointIndexEntry,
    owning_region: CheckpointRegionSnapshot,
    directory: CheckpointDirectory,
    epoch_start_seqno: u64,
    global_max_seqno: u64,
) -> CodecResult<[u8; CHECKPOINT_INDEX_ENTRY_SIZE]> {
    validate_index_entry(
        entry,
        owning_region,
        directory,
        epoch_start_seqno,
        global_max_seqno,
    )?;
    let mut output = [0_u8; CHECKPOINT_INDEX_ENTRY_SIZE];
    put_u64(&mut output, INDEX_KEY_HASH_OFFSET, entry.key_hash);
    put_u64(&mut output, INDEX_LOCATION_OFFSET, entry.location.raw());
    put_u64(&mut output, INDEX_SEQNO_OFFSET, entry.seqno);
    put_u32(&mut output, INDEX_NAMESPACE_ID_OFFSET, entry.namespace_id);
    put_u32(&mut output, INDEX_FLAGS_OFFSET, entry.flags);
    put_u32(
        &mut output,
        INDEX_PHYSICAL_SLOT_OFFSET,
        entry
            .physical_slot
            .ok_or(CheckpointCodecError::InvalidField("physical_slot"))?,
    );
    Ok(output)
}

pub(crate) fn decode_region_snapshot(
    input: &[u8],
    checkpoint_version: u16,
) -> CodecResult<CheckpointRegionSnapshot> {
    if input.len() != CHECKPOINT_REGION_SNAPSHOT_SIZE {
        return Err(CheckpointCodecError::InvalidLength);
    }
    let lane_id = if checkpoint_version >= CHECKPOINT_SLOT_V3 {
        if input[10..16].iter().any(|byte| *byte != 0) {
            return Err(CheckpointCodecError::InvalidField("region_reserved"));
        }
        input[REGION_LANE_ID_OFFSET].checked_sub(1)
    } else if checkpoint_version == CHECKPOINT_SLOT_V1 || checkpoint_version == CHECKPOINT_SLOT_V2 {
        if input[9..16].iter().any(|byte| *byte != 0) {
            return Err(CheckpointCodecError::InvalidField("region_reserved"));
        }
        None
    } else {
        return Err(CheckpointCodecError::UnsupportedVersion(checkpoint_version));
    };
    let state = match input[REGION_STATE_OFFSET] {
        0 => RegionState::Free,
        1 => RegionState::Active,
        2 => RegionState::Sealed,
        _ => return Err(CheckpointCodecError::InvalidField("region_state")),
    };
    Ok(CheckpointRegionSnapshot {
        region_id: get_u32(input, REGION_ID_OFFSET)?,
        incarnation: get_u32(input, REGION_INCARNATION_OFFSET)?,
        state,
        lane_id,
        used: get_u64(input, REGION_USED_OFFSET)?,
        created_seqno: get_u64(input, REGION_CREATED_SEQNO_OFFSET)?,
        max_seqno: get_u64(input, REGION_MAX_SEQNO_OFFSET)?,
    })
}

pub(crate) fn decode_checkpoint_index_entry(input: &[u8]) -> CodecResult<CheckpointIndexEntry> {
    if input.len() != CHECKPOINT_INDEX_ENTRY_V1_SIZE
        && input.len() != CHECKPOINT_INDEX_ENTRY_V2_SIZE
        && input.len() != CHECKPOINT_INDEX_ENTRY_SIZE
    {
        return Err(CheckpointCodecError::InvalidLength);
    }
    if input.len() == CHECKPOINT_INDEX_ENTRY_SIZE
        && input[INDEX_FIELDS_END..CHECKPOINT_INDEX_ENTRY_SIZE]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(CheckpointCodecError::InvalidField("entry_reserved"));
    }
    let location = PackedLocation::try_from_raw(get_u64(input, INDEX_LOCATION_OFFSET)?)
        .map_err(|_| CheckpointCodecError::InvalidField("entry_location"))?;
    Ok(CheckpointIndexEntry {
        key_hash: get_u64(input, INDEX_KEY_HASH_OFFSET)?,
        location,
        seqno: get_u64(input, INDEX_SEQNO_OFFSET)?,
        namespace_id: if input.len() != CHECKPOINT_INDEX_ENTRY_V1_SIZE {
            get_u32(input, INDEX_NAMESPACE_ID_OFFSET)?
        } else {
            0
        },
        flags: if input.len() != CHECKPOINT_INDEX_ENTRY_V1_SIZE {
            get_u32(input, INDEX_FLAGS_OFFSET)?
        } else {
            0
        },
        physical_slot: if input.len() == CHECKPOINT_INDEX_ENTRY_SIZE {
            Some(get_u32(input, INDEX_PHYSICAL_SLOT_OFFSET)?)
        } else {
            None
        },
    })
}

fn validate_region(
    region: CheckpointRegionSnapshot,
    expected_region_id: u32,
    directory: CheckpointDirectory,
    global_max_seqno: u64,
) -> CodecResult<()> {
    if region.region_id != expected_region_id {
        return Err(CheckpointCodecError::InvalidField("region_id"));
    }
    if region.used < REGION_HEADER_SIZE as u64
        || region.used > directory.region_size
        || region.used % RECORD_ALIGNMENT as u64 != 0
    {
        return Err(CheckpointCodecError::InvalidField("region_used"));
    }
    match region.state {
        RegionState::Free
            if region.incarnation == 0
                && region.lane_id.is_none()
                && region.used == REGION_HEADER_SIZE as u64
                && region.created_seqno == 0
                && region.max_seqno == 0 => {}
        RegionState::Free => {
            return Err(CheckpointCodecError::InvalidField("free_region_metadata"));
        }
        RegionState::Active | RegionState::Sealed => {
            if (region.state != RegionState::Active && region.lane_id.is_some())
                || region.incarnation == 0
                || region.created_seqno == 0
                || region.created_seqno > global_max_seqno
                || region.max_seqno > global_max_seqno
                || (region.used == REGION_HEADER_SIZE as u64 && region.max_seqno != 0)
                || (region.used > REGION_HEADER_SIZE as u64
                    && region.max_seqno < region.created_seqno)
            {
                return Err(CheckpointCodecError::InvalidField(
                    "allocated_region_metadata",
                ));
            }
        }
    }
    Ok(())
}

fn validate_index_entry(
    entry: CheckpointIndexEntry,
    region: CheckpointRegionSnapshot,
    directory: CheckpointDirectory,
    epoch_start_seqno: u64,
    global_max_seqno: u64,
) -> CodecResult<()> {
    let location = entry.location;
    if location.record_len() == 0
        || location.region_id() >= directory.region_count
        || location.region_id() != region.region_id
        || location.offset() < REGION_HEADER_SIZE as u32
        || location.offset() % RECORD_ALIGNMENT as u32 != 0
    {
        return Err(CheckpointCodecError::InvalidField("entry_location"));
    }
    let record_end = u64::from(location.offset())
        .checked_add(u64::from(location.record_len()))
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    if region.state == RegionState::Free || record_end > region.used {
        return Err(CheckpointCodecError::InvalidField("entry_location"));
    }
    if entry.seqno <= epoch_start_seqno
        || entry.seqno > global_max_seqno
        || entry.seqno < region.created_seqno
        || region.max_seqno == 0
        || entry.seqno > region.max_seqno
    {
        return Err(CheckpointCodecError::InvalidField("entry_seqno"));
    }
    Ok(())
}

fn validate_sequence_bounds(epoch_start_seqno: u64, max_seqno: u64) -> CodecResult<()> {
    if epoch_start_seqno == 0 || max_seqno < epoch_start_seqno {
        return Err(CheckpointCodecError::InvalidField("sequence_bounds"));
    }
    Ok(())
}

fn validate_slot(slot: usize) -> CodecResult<()> {
    if slot >= CHECKPOINT_SLOT_COUNT {
        return Err(CheckpointCodecError::InvalidField("slot_id"));
    }
    Ok(())
}

fn require_page(
    input: &[u8],
    expected_len: usize,
    magic: [u8; 8],
    version_offset: usize,
    crc_offset: usize,
    minimum_version: u16,
    maximum_version: u16,
) -> CodecResult<u16> {
    if input.len() != expected_len {
        return Err(CheckpointCodecError::InvalidLength);
    }
    if input.get(..magic.len()) != Some(magic.as_slice()) {
        return Err(CheckpointCodecError::InvalidMagic);
    }
    let version = get_u16(input, version_offset)?;
    if version < minimum_version || version > maximum_version {
        return Err(CheckpointCodecError::UnsupportedVersion(version));
    }
    if !checksum_matches(input, crc_offset)? {
        return Err(CheckpointCodecError::ChecksumMismatch);
    }
    Ok(version)
}

fn checksum_matches(input: &[u8], checksum_offset: usize) -> CodecResult<bool> {
    let expected = get_u32(input, checksum_offset)?;
    let after_checksum = checksum_offset
        .checked_add(size_of::<u32>())
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let before = input
        .get(..checksum_offset)
        .ok_or(CheckpointCodecError::InvalidLength)?;
    let after = input
        .get(after_checksum..)
        .ok_or(CheckpointCodecError::InvalidLength)?;
    let mut checksum = Crc32c::new();
    checksum.update(before);
    checksum.update(&[0; size_of::<u32>()]);
    checksum.update(after);
    Ok(checksum.finish() == expected)
}

fn align_up_u64(value: u64, alignment: u64) -> CodecResult<u64> {
    if !alignment.is_power_of_two() {
        return Err(CheckpointCodecError::InvalidField("alignment"));
    }
    value
        .checked_add(alignment - 1)
        .map(|rounded| rounded & !(alignment - 1))
        .ok_or(CheckpointCodecError::ArithmeticOverflow)
}

fn get_u16(input: &[u8], offset: usize) -> CodecResult<u16> {
    let end = offset
        .checked_add(size_of::<u16>())
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let bytes: [u8; size_of::<u16>()] = input
        .get(offset..end)
        .ok_or(CheckpointCodecError::InvalidLength)?
        .try_into()
        .map_err(|_| CheckpointCodecError::InvalidLength)?;
    Ok(u16::from_le_bytes(bytes))
}

fn get_u32(input: &[u8], offset: usize) -> CodecResult<u32> {
    let end = offset
        .checked_add(size_of::<u32>())
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let bytes: [u8; size_of::<u32>()] = input
        .get(offset..end)
        .ok_or(CheckpointCodecError::InvalidLength)?
        .try_into()
        .map_err(|_| CheckpointCodecError::InvalidLength)?;
    Ok(u32::from_le_bytes(bytes))
}

fn get_u64(input: &[u8], offset: usize) -> CodecResult<u64> {
    let end = offset
        .checked_add(size_of::<u64>())
        .ok_or(CheckpointCodecError::ArithmeticOverflow)?;
    let bytes: [u8; size_of::<u64>()] = input
        .get(offset..end)
        .ok_or(CheckpointCodecError::InvalidLength)?
        .try_into()
        .map_err(|_| CheckpointCodecError::InvalidLength)?;
    Ok(u64::from_le_bytes(bytes))
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn directory() -> CheckpointDirectory {
        let region_size = 64 * 1024;
        let region_count = 3;
        let data_file_len = SUPERBLOCK_AREA_SIZE + u64::from(region_count) * region_size;
        CheckpointDirectory::for_index_capacity(data_file_len, region_size, region_count, 8)
            .unwrap()
    }

    fn regions() -> [CheckpointRegionSnapshot; 3] {
        [
            CheckpointRegionSnapshot {
                region_id: 0,
                incarnation: 3,
                state: RegionState::Sealed,
                lane_id: None,
                used: 8192,
                created_seqno: 3,
                max_seqno: 11,
            },
            CheckpointRegionSnapshot {
                region_id: 1,
                incarnation: 2,
                state: RegionState::Active,
                lane_id: None,
                used: 8192,
                created_seqno: 12,
                max_seqno: 13,
            },
            CheckpointRegionSnapshot {
                region_id: 2,
                incarnation: 0,
                state: RegionState::Free,
                lane_id: None,
                used: REGION_HEADER_SIZE as u64,
                created_seqno: 0,
                max_seqno: 0,
            },
        ]
    }

    fn entries() -> [CheckpointIndexEntry; 2] {
        [
            CheckpointIndexEntry {
                key_hash: 0x0123_4567_89ab_cdef,
                location: PackedLocation::new(0, 4096, 64, false).unwrap(),
                seqno: 11,
                namespace_id: 42,
                flags: 0x10,
                physical_slot: Some(1),
            },
            CheckpointIndexEntry {
                key_hash: 7,
                location: PackedLocation::new(1, 4096, 96, true).unwrap(),
                seqno: 13,
                namespace_id: 9,
                flags: 1,
                physical_slot: Some(2),
            },
        ]
    }

    fn encode_fixture() -> (
        CheckpointDirectory,
        Vec<u8>,
        CheckpointPayloadSummary,
        CheckpointSlotHeader,
    ) {
        let directory = directory();
        let mut payload = vec![
            0_u8;
            payload_len(directory.region_count, entries().len() as u32, 8).unwrap()
                as usize
        ];
        let summary =
            encode_payload_into(directory, 1, 13, 8, &regions(), &entries(), &mut payload).unwrap();
        let header = CheckpointSlotHeader::new(
            1,
            CheckpointSnapshotMeta {
                generation: 7,
                superblock_generation: 19,
                epoch: 2,
                epoch_start_seqno: 1,
                max_seqno: 13,
                hash_seed: 0x1020_3040_5060_7080,
                index_slots: 8,
                index_shards: 1,
            },
            summary,
            directory,
        )
        .unwrap();
        (directory, payload, summary, header)
    }

    fn encode_v1_fixture() -> (CheckpointDirectory, Vec<u8>, CheckpointSlotHeader) {
        let (directory, _, _, current_header) = encode_fixture();
        let source_regions = regions();
        let source_entries = entries();
        let mut payload = Vec::with_capacity(
            payload_len_for_version(
                directory.region_count,
                source_entries.len() as u32,
                CHECKPOINT_SLOT_V1,
                None,
            )
            .unwrap() as usize,
        );
        for (region_id, region) in source_regions.into_iter().enumerate() {
            payload.extend_from_slice(
                &encode_region_snapshot(region, region_id as u32, directory, 13).unwrap(),
            );
        }
        for entry in source_entries {
            let owner = regions()[entry.location.region_id() as usize];
            let encoded = encode_checkpoint_index_entry(entry, owner, directory, 1, 13).unwrap();
            payload.extend_from_slice(&encoded[..CHECKPOINT_INDEX_ENTRY_V1_SIZE]);
        }
        let header = CheckpointSlotHeader {
            version: CHECKPOINT_SLOT_V1,
            payload_len: payload.len() as u64,
            payload_crc: crc32c(&payload),
            index_slots: None,
            index_shards: None,
            ..current_header
        };
        header.validate(directory, 1).unwrap();
        (directory, payload, header)
    }

    #[test]
    fn directory_and_offsets_are_page_aligned_and_overflow_checked() {
        let directory = directory();
        assert_eq!(directory.directory_offset() % 4096, 0);
        assert_eq!(directory.slot_header_offset(0).unwrap() % 4096, 0);
        assert_eq!(directory.slot_payload_offset(0).unwrap() % 4096, 0);
        assert_eq!(directory.slot_header_offset(1).unwrap() % 4096, 0);
        assert_eq!(directory.slot_payload_offset(1).unwrap() % 4096, 0);
        assert_eq!(directory.slot_header_offset(1).unwrap(), 217_088);
        assert_eq!(directory.total_file_len().unwrap(), 225_280);
        assert_eq!(padded_payload_len(1).unwrap(), 4096);
        assert_eq!(padded_payload_len(4096).unwrap(), 4096);

        let mut overflow = directory;
        overflow.data_file_len = u64::MAX - 4095;
        assert_eq!(
            overflow.total_file_len(),
            Err(CheckpointCodecError::ArithmeticOverflow)
        );
        assert_eq!(
            directory.slot_header_offset(2),
            Err(CheckpointCodecError::InvalidField("slot_id"))
        );
    }

    #[test]
    fn directory_header_and_payload_round_trip() {
        let (directory, payload, summary, header) = encode_fixture();
        assert_eq!(
            CheckpointDirectory::decode(&directory.encode().unwrap()).unwrap(),
            directory
        );
        let encoded_header = header.encode(directory).unwrap();
        assert_eq!(
            CheckpointSlotHeader::decode(&encoded_header, directory, 1).unwrap(),
            header
        );
        assert_eq!(summary.payload_len, 200);
        assert_eq!(summary.region_count, 3);
        assert_eq!(summary.entry_count, 2);

        let decoded = decode_payload(directory, header, &payload).unwrap();
        assert_eq!(decoded.regions().collect::<Vec<_>>(), regions());
        assert_eq!(decoded.entries().collect::<Vec<_>>(), entries());
    }

    #[test]
    fn sparse_v4_payload_size_depends_on_live_entries_not_source_slots() {
        let directory = directory();
        let index_slots = 125_000_000;
        let expected = directory.region_count as usize * CHECKPOINT_REGION_SNAPSHOT_SIZE
            + entries().len() * CHECKPOINT_INDEX_ENTRY_SIZE;
        assert_eq!(
            payload_len(directory.region_count, entries().len() as u32, index_slots).unwrap(),
            expected as u64
        );

        let mut payload = vec![0_u8; expected];
        let summary = encode_payload_into(
            directory,
            1,
            13,
            index_slots,
            &regions(),
            &entries(),
            &mut payload,
        )
        .unwrap();
        assert_eq!(summary.payload_len, expected as u64);
    }

    #[test]
    fn v1_slot_entries_decode_as_default_namespace_and_flags() {
        let (directory, payload, header) = encode_v1_fixture();
        assert_eq!(header.index_entry_size(), CHECKPOINT_INDEX_ENTRY_V1_SIZE);
        let encoded_header = header.encode(directory).unwrap();
        let decoded_header = CheckpointSlotHeader::decode(&encoded_header, directory, 1).unwrap();
        assert_eq!(decoded_header.version, CHECKPOINT_SLOT_V1);
        let decoded = decode_payload(directory, decoded_header, &payload).unwrap();
        let decoded_entries = decoded.entries().collect::<Vec<_>>();
        assert_eq!(decoded_entries.len(), 2);
        assert!(
            decoded_entries
                .iter()
                .all(|entry| entry.namespace_id == 0 && entry.flags == 0)
        );
        assert_eq!(decoded_entries[0].key_hash, entries()[0].key_hash);
        assert_eq!(decoded_entries[0].location, entries()[0].location);
        assert_eq!(decoded_entries[0].seqno, entries()[0].seqno);
    }

    #[test]
    fn streaming_codec_enforces_order_counts_and_crc() {
        let directory = directory();
        let source_regions = regions();
        let source_entries = entries();
        let mut encoder = CheckpointPayloadEncoder::new(directory, 1, 13, 2, 8).unwrap();
        let mut encoded = Vec::new();
        for region in source_regions {
            encoded.extend_from_slice(&encoder.encode_region(region).unwrap());
        }
        for entry in source_entries {
            let owner = source_regions[entry.location.region_id() as usize];
            encoded.extend_from_slice(&encoder.encode_index_entry(entry, owner).unwrap());
        }
        let summary = encoder.finish().unwrap();
        let header = CheckpointSlotHeader::new(
            0,
            CheckpointSnapshotMeta {
                generation: 1,
                superblock_generation: 2,
                epoch: 1,
                epoch_start_seqno: 1,
                max_seqno: 13,
                hash_seed: 0,
                index_slots: 8,
                index_shards: 1,
            },
            summary,
            directory,
        )
        .unwrap();
        let mut decoder = CheckpointPayloadDecoder::new(directory, header).unwrap();
        let region_len = directory.region_count as usize * CHECKPOINT_REGION_SNAPSHOT_SIZE;
        let mut decoded_regions = Vec::new();
        for record in encoded[..region_len].chunks_exact(CHECKPOINT_REGION_SNAPSHOT_SIZE) {
            decoded_regions.push(decoder.decode_region(record).unwrap());
        }
        for record in encoded[region_len..].chunks_exact(CHECKPOINT_INDEX_ENTRY_SIZE) {
            let entry = decode_checkpoint_index_entry(record).unwrap();
            decoder
                .decode_index_entry(record, decoded_regions[entry.location.region_id() as usize])
                .unwrap();
        }
        decoder.finish().unwrap();

        let mut incomplete = CheckpointPayloadEncoder::new(directory, 1, 13, 1, 8).unwrap();
        incomplete.encode_region(source_regions[0]).unwrap();
        assert_eq!(
            incomplete.finish(),
            Err(CheckpointCodecError::InvalidLength)
        );
    }

    #[test]
    fn corruption_and_cross_structure_inconsistency_are_rejected() {
        let (directory, payload, _summary, header) = encode_fixture();

        let mut bad_directory = directory.encode().unwrap();
        bad_directory[32] ^= 1;
        assert_eq!(
            CheckpointDirectory::decode(&bad_directory),
            Err(CheckpointCodecError::ChecksumMismatch)
        );

        let mut bad_header = header.encode(directory).unwrap();
        bad_header[SLOT_HASH_SEED_OFFSET] ^= 1;
        assert_eq!(
            CheckpointSlotHeader::decode(&bad_header, directory, 1),
            Err(CheckpointCodecError::ChecksumMismatch)
        );

        let mut bad_payload = payload.clone();
        bad_payload[REGION_MAX_SEQNO_OFFSET] ^= 1;
        assert_eq!(
            decode_payload(directory, header, &bad_payload),
            Err(CheckpointCodecError::InvalidField("entry_seqno"))
        );

        let mut checksum_only = payload.clone();
        let key_hash_offset = directory.region_count as usize * CHECKPOINT_REGION_SNAPSHOT_SIZE;
        checksum_only[key_hash_offset] ^= 1;
        assert_eq!(
            decode_payload(directory, header, &checksum_only),
            Err(CheckpointCodecError::ChecksumMismatch)
        );

        let mut bad_location = payload;
        let location_offset = key_hash_offset + INDEX_LOCATION_OFFSET;
        bad_location[location_offset..location_offset + 8].copy_from_slice(
            &PackedLocation::new(2, 4096, 64, false)
                .unwrap()
                .raw()
                .to_le_bytes(),
        );
        let mut repaired_header = header;
        repaired_header.payload_crc = crc32c(&bad_location);
        assert_eq!(
            decode_payload(directory, repaired_header, &bad_location),
            Err(CheckpointCodecError::InvalidField("entry_location"))
        );
    }

    #[test]
    fn golden_little_endian_bytes_freeze_checkpoint_v3() {
        let (directory, _, _, current_header) = encode_fixture();
        let source_regions = regions();
        let source_entries = entries();
        let mut payload = Vec::new();
        for (region_id, region) in source_regions.into_iter().enumerate() {
            payload.extend_from_slice(
                &encode_region_snapshot(region, region_id as u32, directory, 13).unwrap(),
            );
        }
        for entry in source_entries {
            let owner = regions()[entry.location.region_id() as usize];
            let encoded = encode_checkpoint_index_entry(entry, owner, directory, 1, 13).unwrap();
            payload.extend_from_slice(&encoded[..CHECKPOINT_INDEX_ENTRY_V2_SIZE]);
        }
        let summary = CheckpointPayloadSummary {
            payload_len: payload.len() as u64,
            payload_crc: crc32c(&payload),
            region_count: directory.region_count,
            entry_count: source_entries.len() as u32,
        };
        let header = CheckpointSlotHeader {
            version: CHECKPOINT_SLOT_V3,
            payload_len: summary.payload_len,
            payload_crc: summary.payload_crc,
            index_slots: None,
            index_shards: None,
            ..current_header
        };
        let encoded_directory = directory.encode().unwrap();
        let encoded_header = header.encode(directory).unwrap();

        assert_eq!(
            &encoded_directory[..46],
            &[
                0x43, 0x52, 0x43, 0x4b, 0x50, 0x54, 0x44, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x10,
                0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x03, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00,
                0x00, 0x00, 0x01, 0x00,
            ]
        );
        assert_eq!(
            get_u32(&encoded_directory, DIRECTORY_CRC_OFFSET).unwrap(),
            0x3d76_1245
        );
        assert_eq!(summary.payload_crc, 0x2f1b_c175);
        assert_eq!(
            get_u32(&encoded_header, SLOT_HEADER_CRC_OFFSET).unwrap(),
            0xabe4_15e5
        );
        assert_eq!(
            &payload[..40],
            &[
                0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00,
                0x00, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]
        );
        assert_eq!(
            &payload[120..152],
            &[
                0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, 0x00, 0x00, 0x00, 0x40, 0x00, 0x10,
                0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x00, 0x00,
                0x10, 0x00, 0x00, 0x00,
            ]
        );
    }

    #[test]
    fn checkpoint_v3_persists_active_lane_and_legacy_versions_require_zero() {
        let directory = directory();
        let mut active = regions()[1];
        active.lane_id = Some(7);
        let encoded = encode_region_snapshot(active, 1, directory, 13).unwrap();
        assert_eq!(encoded[REGION_LANE_ID_OFFSET], 8);
        assert_eq!(
            decode_region_snapshot(&encoded, CHECKPOINT_SLOT_VERSION).unwrap(),
            active
        );
        assert_eq!(
            decode_region_snapshot(&encoded, CHECKPOINT_SLOT_V2),
            Err(CheckpointCodecError::InvalidField("region_reserved"))
        );

        let mut sealed = active;
        sealed.state = RegionState::Sealed;
        assert_eq!(
            encode_region_snapshot(sealed, 1, directory, 13),
            Err(CheckpointCodecError::InvalidField(
                "allocated_region_metadata"
            ))
        );
    }

    #[test]
    fn malformed_lengths_versions_reserved_bytes_and_limits_are_rejected() {
        let (directory, payload, _, header) = encode_fixture();
        assert_eq!(
            CheckpointDirectory::decode(&[0; 8]),
            Err(CheckpointCodecError::InvalidLength)
        );
        assert_eq!(
            decode_payload(directory, header, &payload[..payload.len() - 1]),
            Err(CheckpointCodecError::InvalidLength)
        );

        let mut unsupported = directory.encode().unwrap();
        put_u16(&mut unsupported, DIRECTORY_VERSION_OFFSET, 2);
        put_u32(&mut unsupported, DIRECTORY_CRC_OFFSET, 0);
        let checksum = crc32c(&unsupported);
        put_u32(&mut unsupported, DIRECTORY_CRC_OFFSET, checksum);
        assert_eq!(
            CheckpointDirectory::decode(&unsupported),
            Err(CheckpointCodecError::UnsupportedVersion(2))
        );

        let mut reserved = header.encode(directory).unwrap();
        reserved[128] = 1;
        put_u32(&mut reserved, SLOT_HEADER_CRC_OFFSET, 0);
        let checksum = crc32c(&reserved);
        put_u32(&mut reserved, SLOT_HEADER_CRC_OFFSET, checksum);
        assert_eq!(
            CheckpointSlotHeader::decode(&reserved, directory, 1),
            Err(CheckpointCodecError::InvalidField("slot_reserved"))
        );

        assert_eq!(
            required_slot_size(1, MAX_INDEX_SLOTS + 1),
            Err(CheckpointCodecError::InvalidField("index_capacity"))
        );
        assert!(required_slot_size(1, MAX_INDEX_SLOTS).is_ok());
        let hundred_million_entry_slot = required_slot_size(2, 125_000_000).unwrap();
        assert!(hundred_million_entry_slot > 1024 * 1024 * 1024);
        assert!(hundred_million_entry_slot <= MAX_CHECKPOINT_SLOT_SIZE);
        let mut too_small = directory;
        too_small.slot_size = CHECKPOINT_PAGE_SIZE as u64;
        assert_eq!(
            too_small.encode(),
            Err(CheckpointCodecError::InvalidField("slot_size"))
        );
    }
}
