//! Stable, bounded Region/FIFO metadata for a clean recovery image.
//!
//! The complete section is eagerly validated before any recovered Region
//! manager or index mapping becomes visible. Index slots remain independently
//! lazy-validated; this section contains only O(regions + index partitions) state.

use crate::checksum::Crc32c;
use crate::index::{
    MAX_INDEX_PARTITIONS, MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE,
};
use crate::index_storage::{
    INDEX_IMAGE_PAGE_SIZE, INDEX_IMAGE_SLOTS_PER_PAGE, IndexStorageError,
    canonical_index_partition_ranges,
};
use crate::recovery::{DataSuperblock, PersistentId, RECOVERY_PAGE_SIZE, RecoveryImageHeader};
use std::fmt;

pub(crate) const REGION_METADATA_PAGE_SIZE: usize = RECOVERY_PAGE_SIZE;
pub(crate) const REGION_METADATA_PAGE_HEADER_SIZE: usize = 64;
pub(crate) const REGION_METADATA_ROOT_SIZE: usize = 256;
pub(crate) const REGION_METADATA_REGION_SIZE: usize = 21;
pub(crate) const REGION_METADATA_PARTITION_SIZE: usize = 16;
pub(crate) const REGION_METADATA_REGIONS_PER_PAGE: usize =
    (REGION_METADATA_PAGE_SIZE - REGION_METADATA_PAGE_HEADER_SIZE) / REGION_METADATA_REGION_SIZE;
pub(crate) const REGION_METADATA_PARTITIONS_PER_PAGE: usize =
    (REGION_METADATA_PAGE_SIZE - REGION_METADATA_PAGE_HEADER_SIZE) / REGION_METADATA_PARTITION_SIZE;

const PAGE_MAGIC: [u8; 8] = *b"CRRMD\0\0\0";
const FORMAT_VERSION: u16 = 1;
const MIN_ENCODED_RECORD_SIZE: u64 = 64;
const MAX_SHARDS: usize = 256;

const PAGE_VERSION_OFFSET: usize = 8;
const PAGE_HEADER_SIZE_OFFSET: usize = 10;
const PAGE_KIND_OFFSET: usize = 12;
const PAGE_RECORD_SIZE_OFFSET: usize = 14;
const PAGE_IMAGE_IDENTITY_OFFSET: usize = 16;
const PAGE_IMAGE_GENERATION_OFFSET: usize = 32;
const PAGE_INDEX_OFFSET: usize = 40;
const PAGE_FIRST_RECORD_OFFSET: usize = 44;
const PAGE_RECORD_COUNT_OFFSET: usize = 48;
const PAGE_FLAGS_OFFSET: usize = 52;
const PAGE_CRC_OFFSET: usize = 56;
const PAGE_RESERVED_OFFSET: usize = 60;

const ROOT_CACHE_UUID_OFFSET: usize = 0;
const ROOT_DATA_IDENTITY_OFFSET: usize = 16;
const ROOT_DATA_GENERATION_OFFSET: usize = 32;
const ROOT_IMAGE_IDENTITY_OFFSET: usize = 40;
const ROOT_IMAGE_GENERATION_OFFSET: usize = 56;
const ROOT_CONFIG_FINGERPRINT_OFFSET: usize = 64;
const ROOT_INDEX_SLOTS_OFFSET: usize = 72;
const ROOT_INDEX_PAGE_COUNT_OFFSET: usize = 80;
const ROOT_REGION_SIZE_OFFSET: usize = 88;
const ROOT_REGION_COUNT_OFFSET: usize = 96;
const ROOT_PARTITION_COUNT_OFFSET: usize = 100;
const ROOT_SHARD_COUNT_OFFSET: usize = 104;
const ROOT_RESERVED32_OFFSET: usize = 108;
const ROOT_RESERVED_EPOCH_OFFSET: usize = 112;
const ROOT_RESERVED_CLEAR_FLOOR_OFFSET: usize = 120;
const ROOT_MAX_SEQNO_OFFSET: usize = 128;
const ROOT_RESERVED_ACCOUNTING_START: usize = 136;
const ROOT_RESERVED_ACCOUNTING_END: usize = 184;
const ROOT_RESERVED_TAIL_START: usize = 184;
const ROOT_RESERVED_TAIL_END: usize = 224;
const ROOT_REGION_FIRST_PAGE_OFFSET: usize = 224;
const ROOT_REGION_PAGE_COUNT_OFFSET: usize = 228;
const ROOT_PARTITION_FIRST_PAGE_OFFSET: usize = 232;
const ROOT_PARTITION_PAGE_COUNT_OFFSET: usize = 236;
const ROOT_FREE_REGION_COUNT_OFFSET: usize = 240;
const ROOT_ACTIVE_REGION_COUNT_OFFSET: usize = 244;
const ROOT_SEALED_REGION_COUNT_OFFSET: usize = 248;
const ROOT_RESERVED_OFFSET: usize = 252;

const REGION_CREATED_SEQNO_OFFSET: usize = 0;
const REGION_DURABLE_USED_OFFSET: usize = 8;
const REGION_RECORD_COUNT_OFFSET: usize = 12;
const REGION_QUEUE_ORDINAL_OFFSET: usize = 16;
const REGION_STATE_OFFSET: usize = 20;

const PARTITION_ID_OFFSET: usize = 0;
const PARTITION_PHYSICAL_VALUE_SLOTS_OFFSET: usize = 4;
const PARTITION_PHYSICAL_DELETED_SLOTS_OFFSET: usize = 8;
const PARTITION_RESERVED_OFFSET: usize = 12;

const _: () = assert!(REGION_METADATA_PAGE_SIZE == INDEX_IMAGE_PAGE_SIZE);
const _: () = assert!(REGION_METADATA_REGIONS_PER_PAGE == 192);
const _: () = assert!(REGION_METADATA_PARTITIONS_PER_PAGE == 252);

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageKind {
    Root = 1,
    Region = 2,
    Partition = 3,
}

impl PageKind {
    fn decode(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::Root),
            2 => Some(Self::Region),
            3 => Some(Self::Partition),
            _ => None,
        }
    }
}

/// Only stable, quiescent Region states can appear in a CLEAN image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMetadataState {
    Free = 0,
    Active = 1,
    Sealed = 2,
}

impl RegionMetadataState {
    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Free),
            1 => Some(Self::Active),
            2 => Some(Self::Sealed),
            _ => None,
        }
    }
}

/// Exact global authority frozen together with the index and Region table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataRoot {
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) data_superblock_generation: u64,
    pub(crate) image_identity: PersistentId,
    pub(crate) image_generation: u64,
    pub(crate) config_fingerprint: u64,
    pub(crate) index_slots: u64,
    pub(crate) index_page_count: u64,
    pub(crate) region_size: u64,
    pub(crate) region_count: u32,
    pub(crate) partition_count: u32,
    pub(crate) shard_count: u32,
    pub(crate) max_seqno: u64,
    pub(crate) free_region_count: u32,
    pub(crate) active_region_count: u32,
    pub(crate) sealed_region_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataRecord {
    pub(crate) state: RegionMetadataState,
    /// Free queue position, Active shard id, or Sealed FIFO position.
    pub(crate) queue_ordinal: u32,
    pub(crate) created_seqno: u64,
    pub(crate) durable_used_offset: u64,
    pub(crate) physical_record_count: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PartitionMetadataRecord {
    pub(crate) partition_id: u32,
    pub(crate) first_index_page: u64,
    pub(crate) index_page_count: u64,
    pub(crate) first_slot: u64,
    pub(crate) slot_count: u64,
    pub(crate) physical_value_slots: u64,
    pub(crate) physical_deleted_slots: u64,
}

#[derive(Clone, Copy)]
struct EncodedPartitionCounters {
    partition_id: u32,
    physical_value_slots: u32,
    physical_deleted_slots: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadata {
    pub(crate) root: RegionMetadataRoot,
    pub(crate) regions: Box<[RegionMetadataRecord]>,
    pub(crate) partitions: Box<[PartitionMetadataRecord]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMetadataError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion(u16),
    ChecksumMismatch,
    InvalidField(&'static str),
    ArithmeticOverflow,
    Allocation,
}

impl fmt::Display for RegionMetadataError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength => formatter.write_str("invalid Region metadata length"),
            Self::InvalidMagic => formatter.write_str("invalid Region metadata magic"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Region metadata version {version}")
            }
            Self::ChecksumMismatch => formatter.write_str("Region metadata checksum mismatch"),
            Self::InvalidField(field) => {
                write!(formatter, "invalid Region metadata field: {field}")
            }
            Self::ArithmeticOverflow => formatter.write_str("Region metadata size overflow"),
            Self::Allocation => formatter.write_str("Region metadata allocation failed"),
        }
    }
}

impl std::error::Error for RegionMetadataError {}

type Result<T> = std::result::Result<T, RegionMetadataError>;

impl RegionMetadata {
    pub(crate) fn encoded_len(&self) -> Result<u64> {
        encoded_len_for_counts(self.root.region_count, self.root.partition_count)
    }

    /// Proves that this section belongs to the exact data and image authority
    /// selected by CLEAN. Content validation is performed separately by
    /// [`Self::validate`].
    pub(crate) fn matches_image(&self, data: DataSuperblock, image: RecoveryImageHeader) -> bool {
        let Ok(encoded_len) = self.encoded_len() else {
            return false;
        };
        self.root.cache_uuid == data.cache_uuid
            && self.root.data_identity == data.data_identity
            && self.root.data_superblock_generation == data.generation
            && self.root.region_size == data.geometry.region_size
            && self.root.region_count == data.geometry.region_count
            && self.root.config_fingerprint == data.config_fingerprint
            && self.root.cache_uuid == image.cache_uuid
            && self.root.data_identity == image.data_identity
            && self.root.data_superblock_generation == image.data_superblock_generation
            && self.root.image_identity == image.image_identity
            && self.root.image_generation == image.image_generation
            && self.root.config_fingerprint == image.config_fingerprint
            && self.root.index_slots == image.index_slots
            && encoded_len == image.region_table_len
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let layout = MetadataLayout::new(self.root.region_count, self.root.partition_count)?;
        let encoded_len = usize::try_from(layout.encoded_len)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(encoded_len)
            .map_err(|_| RegionMetadataError::Allocation)?;
        output.resize(encoded_len, 0);

        {
            let page = page_mut(&mut output, 0)?;
            encode_page_envelope(
                page,
                PageEnvelope {
                    kind: PageKind::Root,
                    record_size: REGION_METADATA_ROOT_SIZE as u16,
                    image_identity: self.root.image_identity,
                    image_generation: self.root.image_generation,
                    page_index: 0,
                    first_record: 0,
                    record_count: 1,
                },
            );
            encode_root(
                &self.root,
                layout,
                page_payload_mut(page, 0, REGION_METADATA_ROOT_SIZE),
            );
            finish_page(page);
        }

        encode_record_pages(
            &mut output,
            layout.region_first_page,
            PageKind::Region,
            REGION_METADATA_REGION_SIZE,
            REGION_METADATA_REGIONS_PER_PAGE,
            self.root.image_identity,
            self.root.image_generation,
            &self.regions,
            encode_region,
        )?;
        encode_record_pages(
            &mut output,
            layout.partition_first_page,
            PageKind::Partition,
            REGION_METADATA_PARTITION_SIZE,
            REGION_METADATA_PARTITIONS_PER_PAGE,
            self.root.image_identity,
            self.root.image_generation,
            &self.partitions,
            encode_partition,
        )?;
        Ok(output)
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn decode(input: &[u8]) -> Result<Self> {
        let metadata = Self::decode_pages(input)?;
        metadata.validate()?;
        Ok(metadata)
    }

    /// Decodes an owned image and releases its encoded pages before allocating
    /// the queue-validation workspaces used by [`Self::validate`].
    pub(crate) fn decode_owned(input: Vec<u8>) -> Result<Self> {
        let metadata = Self::decode_pages(&input)?;
        drop(input);
        metadata.validate()?;
        Ok(metadata)
    }

    fn decode_pages(input: &[u8]) -> Result<Self> {
        if input.len() < REGION_METADATA_PAGE_SIZE
            || !input.len().is_multiple_of(REGION_METADATA_PAGE_SIZE)
        {
            return Err(RegionMetadataError::InvalidLength);
        }
        let first = page(input, 0)?;
        let first_envelope = decode_page_envelope(first)?;
        validate_envelope_shape(
            first_envelope,
            PageKind::Root,
            REGION_METADATA_ROOT_SIZE,
            0,
            0,
            1,
        )?;
        require_zero_padding(first, REGION_METADATA_ROOT_SIZE)?;
        let root = decode_root(page_payload(first, 0, REGION_METADATA_ROOT_SIZE))?;
        if root.image_identity != first_envelope.image_identity
            || root.image_generation != first_envelope.image_generation
        {
            return Err(RegionMetadataError::InvalidField("root_image_binding"));
        }
        let layout = MetadataLayout::new(root.region_count, root.partition_count)?;
        if input.len() as u64 != layout.encoded_len {
            return Err(RegionMetadataError::InvalidLength);
        }
        validate_encoded_root_directory(page_payload(first, 0, REGION_METADATA_ROOT_SIZE), layout)?;
        validate_root_directory(root, layout)?;

        let regions = decode_record_pages(
            input,
            layout.region_first_page,
            PageKind::Region,
            REGION_METADATA_REGION_SIZE,
            REGION_METADATA_REGIONS_PER_PAGE,
            root.image_identity,
            root.image_generation,
            root.region_count as usize,
            decode_region,
        )?;
        let counters = decode_record_pages(
            input,
            layout.partition_first_page,
            PageKind::Partition,
            REGION_METADATA_PARTITION_SIZE,
            REGION_METADATA_PARTITIONS_PER_PAGE,
            root.image_identity,
            root.image_generation,
            root.partition_count as usize,
            decode_partition,
        )?;
        let index_slots = usize::try_from(root.index_slots)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let canonical =
            canonical_index_partition_ranges(index_slots).map_err(|error| match error {
                IndexStorageError::SizeOverflow => RegionMetadataError::ArithmeticOverflow,
                IndexStorageError::Io(_) => RegionMetadataError::Allocation,
                _ => RegionMetadataError::InvalidField("canonical_partition_directory"),
            })?;
        if counters.len() != canonical.len() {
            return Err(RegionMetadataError::InvalidField(
                "canonical_partition_directory",
            ));
        }
        let mut partitions = Vec::new();
        partitions
            .try_reserve_exact(counters.len())
            .map_err(|_| RegionMetadataError::Allocation)?;
        for (counter, range) in counters.into_iter().zip(canonical) {
            if counter.partition_id as usize != range.partition_id {
                return Err(RegionMetadataError::InvalidField(
                    "canonical_partition_directory",
                ));
            }
            partitions.push(PartitionMetadataRecord {
                partition_id: counter.partition_id,
                first_index_page: range.first_page as u64,
                index_page_count: range.page_count as u64,
                first_slot: range.first_slot as u64,
                slot_count: range.slot_count as u64,
                physical_value_slots: u64::from(counter.physical_value_slots),
                physical_deleted_slots: u64::from(counter.physical_deleted_slots),
            });
        }
        Ok(Self {
            root,
            regions: regions.into_boxed_slice(),
            partitions: partitions.into_boxed_slice(),
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let layout = MetadataLayout::new(self.root.region_count, self.root.partition_count)?;
        validate_root_directory(self.root, layout)?;
        if self.regions.len() != self.root.region_count as usize {
            return Err(RegionMetadataError::InvalidField("region_count"));
        }
        if self.partitions.len() != self.root.partition_count as usize {
            return Err(RegionMetadataError::InvalidField("partition_count"));
        }
        validate_regions(self.root, &self.regions)?;
        validate_partitions(self.root, &self.partitions)?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct MetadataLayout {
    region_first_page: u32,
    region_page_count: u32,
    partition_first_page: u32,
    partition_page_count: u32,
    encoded_len: u64,
}

impl MetadataLayout {
    fn new(region_count: u32, partition_count: u32) -> Result<Self> {
        if region_count == 0 || partition_count == 0 {
            return Err(RegionMetadataError::InvalidField("record_count"));
        }
        let region_page_count = pages_for_records(
            u64::from(region_count),
            REGION_METADATA_REGIONS_PER_PAGE as u64,
        )?;
        let partition_page_count = pages_for_records(
            u64::from(partition_count),
            REGION_METADATA_PARTITIONS_PER_PAGE as u64,
        )?;
        let region_first_page = 1_u32;
        let partition_first_page = region_first_page
            .checked_add(region_page_count)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        let page_count = partition_first_page
            .checked_add(partition_page_count)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        let encoded_len = u64::from(page_count)
            .checked_mul(REGION_METADATA_PAGE_SIZE as u64)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        Ok(Self {
            region_first_page,
            region_page_count,
            partition_first_page,
            partition_page_count,
            encoded_len,
        })
    }
}

fn encoded_len_for_counts(region_count: u32, partition_count: u32) -> Result<u64> {
    Ok(MetadataLayout::new(region_count, partition_count)?.encoded_len)
}

fn pages_for_records(records: u64, per_page: u64) -> Result<u32> {
    let pages = records
        .checked_add(per_page - 1)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?
        / per_page;
    u32::try_from(pages).map_err(|_| RegionMetadataError::ArithmeticOverflow)
}

fn validate_root_directory(root: RegionMetadataRoot, layout: MetadataLayout) -> Result<()> {
    let expected_index_pages =
        pages_for_records(root.index_slots, INDEX_IMAGE_SLOTS_PER_PAGE as u64)?;
    let maximum_index_slots =
        u64::try_from(MAX_INDEX_SLOTS).map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
    if root.data_superblock_generation == 0
        || root.image_generation == 0
        || root.index_slots < 8
        || root.index_slots > maximum_index_slots
        || root.index_page_count != u64::from(expected_index_pages)
        || root.region_size < RECOVERY_PAGE_SIZE as u64
        || root.region_size > MAX_PACKED_REGION_SIZE
        || !root.region_size.is_multiple_of(RECOVERY_PAGE_SIZE as u64)
        || root.region_count == 0
        || root.region_count > MAX_PACKED_REGION_COUNT
        || root.partition_count == 0
        || root.partition_count as usize > MAX_INDEX_PARTITIONS
        || !root.partition_count.is_power_of_two()
        || u64::from(root.partition_count) > root.index_page_count
        || root.shard_count == 0
        || root.shard_count as usize > MAX_SHARDS
        || root.shard_count >= root.region_count
        || root.max_seqno == u64::MAX
        || root.active_region_count != root.shard_count
        || root
            .free_region_count
            .checked_add(root.active_region_count)
            .and_then(|count| count.checked_add(root.sealed_region_count))
            != Some(root.region_count)
    {
        return Err(RegionMetadataError::InvalidField("root"));
    }
    if layout.region_first_page != 1
        || layout.partition_first_page
            != layout
                .region_first_page
                .checked_add(layout.region_page_count)
                .ok_or(RegionMetadataError::ArithmeticOverflow)?
    {
        return Err(RegionMetadataError::InvalidField("section_directory"));
    }
    Ok(())
}

fn validate_encoded_root_directory(input: &[u8], layout: MetadataLayout) -> Result<()> {
    if get_u32(input, ROOT_REGION_FIRST_PAGE_OFFSET)? != layout.region_first_page
        || get_u32(input, ROOT_REGION_PAGE_COUNT_OFFSET)? != layout.region_page_count
        || get_u32(input, ROOT_PARTITION_FIRST_PAGE_OFFSET)? != layout.partition_first_page
        || get_u32(input, ROOT_PARTITION_PAGE_COUNT_OFFSET)? != layout.partition_page_count
    {
        return Err(RegionMetadataError::InvalidField("section_directory"));
    }
    Ok(())
}

fn validate_regions(root: RegionMetadataRoot, regions: &[RegionMetadataRecord]) -> Result<()> {
    let mut free_seen = zeroed_bytes(root.free_region_count as usize)?;
    let mut active_seen = zeroed_bytes(root.active_region_count as usize)?;
    let mut sealed_seen = zeroed_bytes(root.sealed_region_count as usize)?;
    for region in regions.iter().copied() {
        if region.durable_used_offset > root.region_size || region.durable_used_offset % 32 != 0 {
            return Err(RegionMetadataError::InvalidField("region_geometry"));
        }
        let (seen, state_count) = match region.state {
            RegionMetadataState::Free => (&mut free_seen, root.free_region_count),
            RegionMetadataState::Active => (&mut active_seen, root.active_region_count),
            RegionMetadataState::Sealed => (&mut sealed_seen, root.sealed_region_count),
        };
        if region.queue_ordinal >= state_count
            || std::mem::replace(&mut seen[region.queue_ordinal as usize], 1) != 0
        {
            return Err(RegionMetadataError::InvalidField("region_queue_ordinal"));
        }
        if region.state == RegionMetadataState::Free {
            if region.created_seqno != 0
                || region.durable_used_offset != 0
                || region.physical_record_count != 0
            {
                return Err(RegionMetadataError::InvalidField("free_region"));
            }
            continue;
        }

        let used_bytes = region.durable_used_offset;
        let empty = used_bytes == 0;
        if region.created_seqno == 0
            || region.created_seqno > root.max_seqno
            || empty != (region.physical_record_count == 0)
            || !minimum_bytes_fit(region.physical_record_count, used_bytes)?
        {
            return Err(RegionMetadataError::InvalidField("allocated_region"));
        }
    }
    if free_seen
        .iter()
        .chain(&active_seen)
        .chain(&sealed_seen)
        .any(|seen| *seen == 0)
    {
        return Err(RegionMetadataError::InvalidField(
            "region_queue_permutation",
        ));
    }
    Ok(())
}

fn validate_partitions(
    root: RegionMetadataRoot,
    partitions: &[PartitionMetadataRecord],
) -> Result<()> {
    let index_slots =
        usize::try_from(root.index_slots).map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
    let canonical = canonical_index_partition_ranges(index_slots).map_err(|error| match error {
        IndexStorageError::SizeOverflow => RegionMetadataError::ArithmeticOverflow,
        // The canonical helper performs no I/O. Its only I/O-shaped failure
        // is the fallible allocation of this O(partitions) directory.
        IndexStorageError::Io(_) => RegionMetadataError::Allocation,
        _ => RegionMetadataError::InvalidField("canonical_partition_directory"),
    })?;
    if canonical.len() != partitions.len() {
        return Err(RegionMetadataError::InvalidField(
            "canonical_partition_directory",
        ));
    }

    for (partition, expected) in partitions.iter().copied().zip(canonical.iter().copied()) {
        let expected_first_page = u64::try_from(expected.first_page)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let expected_page_count = u64::try_from(expected.page_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let expected_first_slot = u64::try_from(expected.first_slot)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let expected_slot_count = u64::try_from(expected.slot_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        if partition.partition_id as usize != expected.partition_id
            || partition.first_index_page != expected_first_page
            || partition.index_page_count != expected_page_count
            || partition.first_slot != expected_first_slot
            || partition.slot_count != expected_slot_count
        {
            return Err(RegionMetadataError::InvalidField(
                "canonical_partition_directory",
            ));
        }
        if partition.physical_deleted_slots != 0 {
            return Err(RegionMetadataError::InvalidField("partition_deleted_slots"));
        }
        if partition.physical_value_slots > partition.slot_count {
            return Err(RegionMetadataError::InvalidField("partition"));
        }
    }
    Ok(())
}

fn minimum_bytes_fit(count: u64, bytes: u64) -> Result<bool> {
    Ok(count
        .checked_mul(MIN_ENCODED_RECORD_SIZE)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?
        <= bytes)
}

fn zeroed_bytes(len: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| RegionMetadataError::Allocation)?;
    output.resize(len, 0);
    Ok(output)
}

#[derive(Clone, Copy)]
struct PageEnvelope {
    kind: PageKind,
    record_size: u16,
    image_identity: PersistentId,
    image_generation: u64,
    page_index: u32,
    first_record: u32,
    record_count: u32,
}

fn encode_page_envelope(page: &mut [u8], envelope: PageEnvelope) {
    page[..8].copy_from_slice(&PAGE_MAGIC);
    put_u16(page, PAGE_VERSION_OFFSET, FORMAT_VERSION);
    put_u16(
        page,
        PAGE_HEADER_SIZE_OFFSET,
        REGION_METADATA_PAGE_HEADER_SIZE as u16,
    );
    put_u16(page, PAGE_KIND_OFFSET, envelope.kind as u16);
    put_u16(page, PAGE_RECORD_SIZE_OFFSET, envelope.record_size);
    page[PAGE_IMAGE_IDENTITY_OFFSET..PAGE_IMAGE_IDENTITY_OFFSET + 16]
        .copy_from_slice(&envelope.image_identity.to_bytes());
    put_u64(
        page,
        PAGE_IMAGE_GENERATION_OFFSET,
        envelope.image_generation,
    );
    put_u32(page, PAGE_INDEX_OFFSET, envelope.page_index);
    put_u32(page, PAGE_FIRST_RECORD_OFFSET, envelope.first_record);
    put_u32(page, PAGE_RECORD_COUNT_OFFSET, envelope.record_count);
    put_u32(page, PAGE_FLAGS_OFFSET, 0);
    put_u32(page, PAGE_CRC_OFFSET, 0);
    put_u32(page, PAGE_RESERVED_OFFSET, 0);
}

fn decode_page_envelope(page: &[u8]) -> Result<PageEnvelope> {
    if page.len() != REGION_METADATA_PAGE_SIZE {
        return Err(RegionMetadataError::InvalidLength);
    }
    if page[..8] != PAGE_MAGIC {
        return Err(RegionMetadataError::InvalidMagic);
    }
    if page_crc(page) != get_u32(page, PAGE_CRC_OFFSET)? {
        return Err(RegionMetadataError::ChecksumMismatch);
    }
    let version = get_u16(page, PAGE_VERSION_OFFSET)?;
    if version != FORMAT_VERSION {
        return Err(RegionMetadataError::UnsupportedVersion(version));
    }
    if get_u16(page, PAGE_HEADER_SIZE_OFFSET)? != REGION_METADATA_PAGE_HEADER_SIZE as u16
        || get_u32(page, PAGE_FLAGS_OFFSET)? != 0
        || get_u32(page, PAGE_RESERVED_OFFSET)? != 0
    {
        return Err(RegionMetadataError::InvalidField("page_header"));
    }
    let identity: [u8; 16] = page
        .get(PAGE_IMAGE_IDENTITY_OFFSET..PAGE_IMAGE_IDENTITY_OFFSET + 16)
        .ok_or(RegionMetadataError::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataError::InvalidLength)?;
    Ok(PageEnvelope {
        kind: PageKind::decode(get_u16(page, PAGE_KIND_OFFSET)?)
            .ok_or(RegionMetadataError::InvalidField("page_kind"))?,
        record_size: get_u16(page, PAGE_RECORD_SIZE_OFFSET)?,
        image_identity: PersistentId::from_bytes(identity)
            .ok_or(RegionMetadataError::InvalidField("page_image_identity"))?,
        image_generation: get_u64(page, PAGE_IMAGE_GENERATION_OFFSET)?,
        page_index: get_u32(page, PAGE_INDEX_OFFSET)?,
        first_record: get_u32(page, PAGE_FIRST_RECORD_OFFSET)?,
        record_count: get_u32(page, PAGE_RECORD_COUNT_OFFSET)?,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_envelope_shape(
    envelope: PageEnvelope,
    kind: PageKind,
    record_size: usize,
    page_index: u32,
    first_record: u32,
    record_count: u32,
) -> Result<()> {
    if envelope.kind != kind
        || usize::from(envelope.record_size) != record_size
        || envelope.image_generation == 0
        || envelope.page_index != page_index
        || envelope.first_record != first_record
        || envelope.record_count != record_count
    {
        return Err(RegionMetadataError::InvalidField("page_directory"));
    }
    Ok(())
}

fn finish_page(page: &mut [u8]) {
    put_u32(page, PAGE_CRC_OFFSET, 0);
    let checksum = page_crc(page);
    put_u32(page, PAGE_CRC_OFFSET, checksum);
}

fn page_crc(page: &[u8]) -> u32 {
    let mut checksum = Crc32c::new();
    checksum.update(&page[..PAGE_CRC_OFFSET]);
    checksum.update(&[0_u8; 4]);
    checksum.update(&page[PAGE_CRC_OFFSET + 4..]);
    checksum.finish()
}

#[allow(clippy::too_many_arguments)]
fn encode_record_pages<T>(
    output: &mut [u8],
    first_page: u32,
    kind: PageKind,
    record_size: usize,
    records_per_page: usize,
    image_identity: PersistentId,
    image_generation: u64,
    records: &[T],
    encode_record: fn(&T, &mut [u8]),
) -> Result<()> {
    for (page_in_section, records) in records.chunks(records_per_page).enumerate() {
        let first_record = page_in_section
            .checked_mul(records_per_page)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        let page_index = usize::try_from(first_page)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?
            .checked_add(page_in_section)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        let page = page_mut(output, page_index)?;
        encode_page_envelope(
            page,
            PageEnvelope {
                kind,
                record_size: u16::try_from(record_size)
                    .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
                image_identity,
                image_generation,
                page_index: u32::try_from(page_index)
                    .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
                first_record: u32::try_from(first_record)
                    .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
                record_count: u32::try_from(records.len())
                    .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            },
        );
        for (index, record) in records.iter().enumerate() {
            encode_record(record, page_payload_mut(page, index, record_size));
        }
        finish_page(page);
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn decode_record_pages<T>(
    input: &[u8],
    first_page: u32,
    kind: PageKind,
    record_size: usize,
    records_per_page: usize,
    image_identity: PersistentId,
    image_generation: u64,
    record_count: usize,
    decode_record: fn(&[u8]) -> Result<T>,
) -> Result<Vec<T>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(record_count)
        .map_err(|_| RegionMetadataError::Allocation)?;
    let page_count = record_count.div_ceil(records_per_page);
    for page_in_section in 0..page_count {
        let first_record = page_in_section
            .checked_mul(records_per_page)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        let records_here = (record_count - first_record).min(records_per_page);
        let page_index = first_page as usize + page_in_section;
        let source = page(input, page_index)?;
        let envelope = decode_page_envelope(source)?;
        validate_envelope_shape(
            envelope,
            kind,
            record_size,
            u32::try_from(page_index).map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            u32::try_from(first_record).map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            u32::try_from(records_here).map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
        )?;
        if envelope.image_identity != image_identity
            || envelope.image_generation != image_generation
        {
            return Err(RegionMetadataError::InvalidField("page_image_binding"));
        }
        require_zero_padding(source, records_here * record_size)?;
        for record in 0..records_here {
            output.push(decode_record(page_payload(source, record, record_size))?);
        }
    }
    Ok(output)
}

fn require_zero_padding(page: &[u8], payload_len: usize) -> Result<()> {
    let end = REGION_METADATA_PAGE_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?;
    if end > page.len() || page[end..].iter().any(|byte| *byte != 0) {
        return Err(RegionMetadataError::InvalidField("page_padding"));
    }
    Ok(())
}

fn encode_root(root: &RegionMetadataRoot, layout: MetadataLayout, output: &mut [u8]) {
    put_id(output, ROOT_CACHE_UUID_OFFSET, root.cache_uuid);
    put_id(output, ROOT_DATA_IDENTITY_OFFSET, root.data_identity);
    put_u64(
        output,
        ROOT_DATA_GENERATION_OFFSET,
        root.data_superblock_generation,
    );
    put_id(output, ROOT_IMAGE_IDENTITY_OFFSET, root.image_identity);
    put_u64(output, ROOT_IMAGE_GENERATION_OFFSET, root.image_generation);
    put_u64(
        output,
        ROOT_CONFIG_FINGERPRINT_OFFSET,
        root.config_fingerprint,
    );
    put_u64(output, ROOT_INDEX_SLOTS_OFFSET, root.index_slots);
    put_u64(output, ROOT_INDEX_PAGE_COUNT_OFFSET, root.index_page_count);
    put_u64(output, ROOT_REGION_SIZE_OFFSET, root.region_size);
    put_u32(output, ROOT_REGION_COUNT_OFFSET, root.region_count);
    put_u32(output, ROOT_PARTITION_COUNT_OFFSET, root.partition_count);
    put_u32(output, ROOT_SHARD_COUNT_OFFSET, root.shard_count);
    put_u32(output, ROOT_RESERVED32_OFFSET, 0);
    put_u64(output, ROOT_RESERVED_EPOCH_OFFSET, 0);
    put_u64(output, ROOT_RESERVED_CLEAR_FLOOR_OFFSET, 0);
    put_u64(output, ROOT_MAX_SEQNO_OFFSET, root.max_seqno);
    output[ROOT_RESERVED_ACCOUNTING_START..ROOT_RESERVED_ACCOUNTING_END].fill(0);
    output[ROOT_RESERVED_TAIL_START..ROOT_RESERVED_TAIL_END].fill(0);
    put_u32(
        output,
        ROOT_REGION_FIRST_PAGE_OFFSET,
        layout.region_first_page,
    );
    put_u32(
        output,
        ROOT_REGION_PAGE_COUNT_OFFSET,
        layout.region_page_count,
    );
    put_u32(
        output,
        ROOT_PARTITION_FIRST_PAGE_OFFSET,
        layout.partition_first_page,
    );
    put_u32(
        output,
        ROOT_PARTITION_PAGE_COUNT_OFFSET,
        layout.partition_page_count,
    );
    put_u32(
        output,
        ROOT_FREE_REGION_COUNT_OFFSET,
        root.free_region_count,
    );
    put_u32(
        output,
        ROOT_ACTIVE_REGION_COUNT_OFFSET,
        root.active_region_count,
    );
    put_u32(
        output,
        ROOT_SEALED_REGION_COUNT_OFFSET,
        root.sealed_region_count,
    );
    put_u32(output, ROOT_RESERVED_OFFSET, 0);
}

fn decode_root(input: &[u8]) -> Result<RegionMetadataRoot> {
    if input.len() != REGION_METADATA_ROOT_SIZE
        || get_u32(input, ROOT_RESERVED32_OFFSET)? != 0
        || get_u64(input, ROOT_RESERVED_EPOCH_OFFSET)? != 0
        || get_u64(input, ROOT_RESERVED_CLEAR_FLOOR_OFFSET)? != 0
        || input[ROOT_RESERVED_ACCOUNTING_START..ROOT_RESERVED_ACCOUNTING_END]
            .iter()
            .any(|byte| *byte != 0)
        || input[ROOT_RESERVED_TAIL_START..ROOT_RESERVED_TAIL_END]
            .iter()
            .any(|byte| *byte != 0)
        || get_u32(input, ROOT_RESERVED_OFFSET)? != 0
    {
        return Err(RegionMetadataError::InvalidField("root_encoding"));
    }
    Ok(RegionMetadataRoot {
        cache_uuid: get_id(input, ROOT_CACHE_UUID_OFFSET)?,
        data_identity: get_id(input, ROOT_DATA_IDENTITY_OFFSET)?,
        data_superblock_generation: get_u64(input, ROOT_DATA_GENERATION_OFFSET)?,
        image_identity: get_id(input, ROOT_IMAGE_IDENTITY_OFFSET)?,
        image_generation: get_u64(input, ROOT_IMAGE_GENERATION_OFFSET)?,
        config_fingerprint: get_u64(input, ROOT_CONFIG_FINGERPRINT_OFFSET)?,
        index_slots: get_u64(input, ROOT_INDEX_SLOTS_OFFSET)?,
        index_page_count: get_u64(input, ROOT_INDEX_PAGE_COUNT_OFFSET)?,
        region_size: get_u64(input, ROOT_REGION_SIZE_OFFSET)?,
        region_count: get_u32(input, ROOT_REGION_COUNT_OFFSET)?,
        partition_count: get_u32(input, ROOT_PARTITION_COUNT_OFFSET)?,
        shard_count: get_u32(input, ROOT_SHARD_COUNT_OFFSET)?,
        max_seqno: get_u64(input, ROOT_MAX_SEQNO_OFFSET)?,
        free_region_count: get_u32(input, ROOT_FREE_REGION_COUNT_OFFSET)?,
        active_region_count: get_u32(input, ROOT_ACTIVE_REGION_COUNT_OFFSET)?,
        sealed_region_count: get_u32(input, ROOT_SEALED_REGION_COUNT_OFFSET)?,
    })
}

fn encode_region(region: &RegionMetadataRecord, output: &mut [u8]) {
    let durable_used = u32::try_from(region.durable_used_offset)
        .expect("validated Region used offset fits its packed field");
    let physical_record_count = u32::try_from(region.physical_record_count)
        .expect("validated Region record count fits its packed field");
    put_u64(output, REGION_CREATED_SEQNO_OFFSET, region.created_seqno);
    put_u32(output, REGION_DURABLE_USED_OFFSET, durable_used);
    put_u32(output, REGION_RECORD_COUNT_OFFSET, physical_record_count);
    put_u32(output, REGION_QUEUE_ORDINAL_OFFSET, region.queue_ordinal);
    output[REGION_STATE_OFFSET] = region.state as u8;
}

fn decode_region(input: &[u8]) -> Result<RegionMetadataRecord> {
    if input.len() != REGION_METADATA_REGION_SIZE {
        return Err(RegionMetadataError::InvalidField("region_encoding"));
    }
    Ok(RegionMetadataRecord {
        state: RegionMetadataState::decode(input[REGION_STATE_OFFSET])
            .ok_or(RegionMetadataError::InvalidField("region_state"))?,
        queue_ordinal: get_u32(input, REGION_QUEUE_ORDINAL_OFFSET)?,
        created_seqno: get_u64(input, REGION_CREATED_SEQNO_OFFSET)?,
        durable_used_offset: u64::from(get_u32(input, REGION_DURABLE_USED_OFFSET)?),
        physical_record_count: u64::from(get_u32(input, REGION_RECORD_COUNT_OFFSET)?),
    })
}

fn encode_partition(partition: &PartitionMetadataRecord, output: &mut [u8]) {
    let physical_value_slots = u32::try_from(partition.physical_value_slots)
        .expect("validated partition value count fits its packed field");
    let physical_deleted_slots = u32::try_from(partition.physical_deleted_slots)
        .expect("validated partition deleted count fits its packed field");
    put_u32(output, PARTITION_ID_OFFSET, partition.partition_id);
    put_u32(
        output,
        PARTITION_PHYSICAL_VALUE_SLOTS_OFFSET,
        physical_value_slots,
    );
    put_u32(
        output,
        PARTITION_PHYSICAL_DELETED_SLOTS_OFFSET,
        physical_deleted_slots,
    );
    put_u32(output, PARTITION_RESERVED_OFFSET, 0);
}

fn decode_partition(input: &[u8]) -> Result<EncodedPartitionCounters> {
    if input.len() != REGION_METADATA_PARTITION_SIZE
        || get_u32(input, PARTITION_RESERVED_OFFSET)? != 0
    {
        return Err(RegionMetadataError::InvalidField("partition_encoding"));
    }
    Ok(EncodedPartitionCounters {
        partition_id: get_u32(input, PARTITION_ID_OFFSET)?,
        physical_value_slots: get_u32(input, PARTITION_PHYSICAL_VALUE_SLOTS_OFFSET)?,
        physical_deleted_slots: get_u32(input, PARTITION_PHYSICAL_DELETED_SLOTS_OFFSET)?,
    })
}

fn page(input: &[u8], page_index: usize) -> Result<&[u8]> {
    let start = page_index
        .checked_mul(REGION_METADATA_PAGE_SIZE)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?;
    let end = start
        .checked_add(REGION_METADATA_PAGE_SIZE)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?;
    input
        .get(start..end)
        .ok_or(RegionMetadataError::InvalidLength)
}

fn page_mut(input: &mut [u8], page_index: usize) -> Result<&mut [u8]> {
    let start = page_index
        .checked_mul(REGION_METADATA_PAGE_SIZE)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?;
    let end = start
        .checked_add(REGION_METADATA_PAGE_SIZE)
        .ok_or(RegionMetadataError::ArithmeticOverflow)?;
    input
        .get_mut(start..end)
        .ok_or(RegionMetadataError::InvalidLength)
}

fn page_payload(page: &[u8], record: usize, record_size: usize) -> &[u8] {
    let start = REGION_METADATA_PAGE_HEADER_SIZE + record * record_size;
    &page[start..start + record_size]
}

fn page_payload_mut(page: &mut [u8], record: usize, record_size: usize) -> &mut [u8] {
    let start = REGION_METADATA_PAGE_HEADER_SIZE + record * record_size;
    &mut page[start..start + record_size]
}

fn get_id(input: &[u8], offset: usize) -> Result<PersistentId> {
    let bytes: [u8; 16] = input
        .get(offset..offset + 16)
        .ok_or(RegionMetadataError::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataError::InvalidLength)?;
    PersistentId::from_bytes(bytes).ok_or(RegionMetadataError::InvalidField("identity"))
}

fn put_id(output: &mut [u8], offset: usize, id: PersistentId) {
    output[offset..offset + 16].copy_from_slice(&id.to_bytes());
}

fn get_u16(input: &[u8], offset: usize) -> Result<u16> {
    let bytes = input
        .get(offset..offset + 2)
        .ok_or(RegionMetadataError::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataError::InvalidLength)?;
    Ok(u16::from_le_bytes(bytes))
}

fn get_u32(input: &[u8], offset: usize) -> Result<u32> {
    let bytes = input
        .get(offset..offset + 4)
        .ok_or(RegionMetadataError::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataError::InvalidLength)?;
    Ok(u32::from_le_bytes(bytes))
}

fn get_u64(input: &[u8], offset: usize) -> Result<u64> {
    let bytes = input
        .get(offset..offset + 8)
        .ok_or(RegionMetadataError::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataError::InvalidLength)?;
    Ok(u64::from_le_bytes(bytes))
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn sample() -> RegionMetadata {
        RegionMetadata {
            root: RegionMetadataRoot {
                cache_uuid: id(1),
                data_identity: id(2),
                data_superblock_generation: 3,
                image_identity: id(4),
                image_generation: 5,
                config_fingerprint: 6,
                index_slots: 508,
                index_page_count: 2,
                region_size: 32 * 1024 * 1024,
                region_count: 4,
                partition_count: 2,
                shard_count: 1,
                max_seqno: 4,
                free_region_count: 1,
                active_region_count: 1,
                sealed_region_count: 2,
            },
            regions: vec![
                RegionMetadataRecord {
                    state: RegionMetadataState::Active,
                    queue_ordinal: 0,
                    created_seqno: 1,
                    durable_used_offset: 0,
                    physical_record_count: 0,
                },
                RegionMetadataRecord {
                    state: RegionMetadataState::Sealed,
                    queue_ordinal: 0,
                    created_seqno: 2,
                    durable_used_offset: 128,
                    physical_record_count: 2,
                },
                RegionMetadataRecord {
                    state: RegionMetadataState::Sealed,
                    queue_ordinal: 1,
                    created_seqno: 4,
                    durable_used_offset: 64,
                    physical_record_count: 1,
                },
                RegionMetadataRecord {
                    state: RegionMetadataState::Free,
                    queue_ordinal: 0,
                    created_seqno: 0,
                    durable_used_offset: 0,
                    physical_record_count: 0,
                },
            ]
            .into_boxed_slice(),
            partitions: vec![
                PartitionMetadataRecord {
                    partition_id: 0,
                    first_index_page: 0,
                    index_page_count: 1,
                    first_slot: 0,
                    slot_count: 504,
                    physical_value_slots: 2,
                    physical_deleted_slots: 0,
                },
                PartitionMetadataRecord {
                    partition_id: 1,
                    first_index_page: 1,
                    index_page_count: 1,
                    first_slot: 504,
                    slot_count: 4,
                    physical_value_slots: 1,
                    physical_deleted_slots: 0,
                },
            ]
            .into_boxed_slice(),
        }
    }

    fn sample_with_index_slots(index_slots: usize) -> RegionMetadata {
        let mut metadata = sample();
        let ranges = canonical_index_partition_ranges(index_slots).unwrap();
        metadata.root.index_slots = index_slots as u64;
        metadata.root.index_page_count = ranges.iter().map(|range| range.page_count as u64).sum();
        metadata.root.partition_count = ranges.len() as u32;
        metadata.partitions = ranges
            .iter()
            .map(|range| PartitionMetadataRecord {
                partition_id: range.partition_id as u32,
                first_index_page: range.first_page as u64,
                index_page_count: range.page_count as u64,
                first_slot: range.first_slot as u64,
                slot_count: range.slot_count as u64,
                physical_value_slots: 0,
                physical_deleted_slots: 0,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        metadata.partitions[0].physical_value_slots = 3;
        metadata
    }

    #[test]
    fn round_trip_preserves_exact_frozen_authority() {
        let expected = sample();
        let encoded = expected.encode().unwrap();
        assert_eq!(REGION_METADATA_REGION_SIZE, 21);
        assert_eq!(REGION_METADATA_PARTITION_SIZE, 16);
        assert_eq!(encoded.len() as u64, expected.encoded_len().unwrap());
        assert_eq!(RegionMetadata::decode(&encoded).unwrap(), expected);
        assert_eq!(&encoded[..8], &PAGE_MAGIC);
        assert_eq!(
            &encoded[REGION_METADATA_PAGE_HEADER_SIZE + ROOT_INDEX_SLOTS_OFFSET
                ..REGION_METADATA_PAGE_HEADER_SIZE + ROOT_INDEX_SLOTS_OFFSET + 8],
            &508_u64.to_le_bytes()
        );
        let root = &encoded[REGION_METADATA_PAGE_HEADER_SIZE
            ..REGION_METADATA_PAGE_HEADER_SIZE + REGION_METADATA_ROOT_SIZE];
        assert!(
            root[ROOT_RESERVED_ACCOUNTING_START..ROOT_RESERVED_ACCOUNTING_END]
                .iter()
                .all(|byte| *byte == 0)
        );
        assert!(
            root[ROOT_RESERVED_TAIL_START..ROOT_RESERVED_TAIL_END]
                .iter()
                .all(|byte| *byte == 0)
        );
    }

    #[test]
    fn reserved_root_slots_are_rejected() {
        let mut reserved32 = sample().encode().unwrap();
        let root_page = page_mut(&mut reserved32, 0).unwrap();
        put_u32(
            page_payload_mut(root_page, 0, REGION_METADATA_ROOT_SIZE),
            ROOT_RESERVED32_OFFSET,
            1,
        );
        finish_page(root_page);
        assert_eq!(
            RegionMetadata::decode(&reserved32),
            Err(RegionMetadataError::InvalidField("root_encoding"))
        );

        let mut root_reserved = sample().encode().unwrap();
        let root_page = page_mut(&mut root_reserved, 0).unwrap();
        page_payload_mut(root_page, 0, REGION_METADATA_ROOT_SIZE)[ROOT_RESERVED_ACCOUNTING_START] =
            1;
        finish_page(root_page);
        assert_eq!(
            RegionMetadata::decode(&root_reserved),
            Err(RegionMetadataError::InvalidField("root_encoding"))
        );

        let mut reserved_epoch = sample().encode().unwrap();
        let root_page = page_mut(&mut reserved_epoch, 0).unwrap();
        put_u64(
            page_payload_mut(root_page, 0, REGION_METADATA_ROOT_SIZE),
            ROOT_RESERVED_EPOCH_OFFSET,
            1,
        );
        finish_page(root_page);
        assert_eq!(
            RegionMetadata::decode(&reserved_epoch),
            Err(RegionMetadataError::InvalidField("root_encoding"))
        );
    }

    #[test]
    fn any_metadata_page_crc_failure_rejects_the_whole_image() {
        let mut encoded = sample().encode().unwrap();
        encoded[REGION_METADATA_PAGE_SIZE + REGION_METADATA_PAGE_HEADER_SIZE + 7] ^= 0x40;
        assert_eq!(
            RegionMetadata::decode(&encoded),
            Err(RegionMetadataError::ChecksumMismatch)
        );
    }

    #[test]
    fn owned_decode_runs_queue_validation_after_page_parsing() {
        let mut encoded = sample().encode().unwrap();
        let region_page = page_mut(&mut encoded, 1).unwrap();
        put_u32(
            page_payload_mut(region_page, 2, REGION_METADATA_REGION_SIZE),
            REGION_QUEUE_ORDINAL_OFFSET,
            0,
        );
        finish_page(region_page);
        assert_eq!(
            RegionMetadata::decode_owned(encoded),
            Err(RegionMetadataError::InvalidField("region_queue_ordinal"))
        );
    }

    #[test]
    fn sealed_fifo_order_is_defined_only_by_queue_ordinal() {
        let mut metadata = sample();
        metadata.regions[1].created_seqno = 3;
        metadata.regions[2].created_seqno = 2;
        metadata.validate().unwrap();
        let decoded = RegionMetadata::decode(&metadata.encode().unwrap()).unwrap();
        assert_eq!(decoded.regions[1].queue_ordinal, 0);
        assert_eq!(decoded.regions[2].queue_ordinal, 1);
        assert!(decoded.regions[1].created_seqno > decoded.regions[2].created_seqno);
    }

    #[test]
    fn partition_ranges_must_be_page_aligned_and_cover_every_slot() {
        let mut invalid = sample();
        invalid.partitions[1].first_slot = 125;
        assert_eq!(
            invalid.encode(),
            Err(RegionMetadataError::InvalidField(
                "canonical_partition_directory"
            ))
        );
    }

    #[test]
    fn tombstone_accounting_is_not_valid_in_the_compact_index_format() {
        let mut invalid = sample();
        invalid.partitions[0].physical_deleted_slots = 1;
        assert_eq!(
            invalid.encode(),
            Err(RegionMetadataError::InvalidField("partition_deleted_slots"))
        );
    }

    #[test]
    fn compact_partition_ids_must_match_the_derived_directory() {
        let metadata = sample_with_index_slots(12 * INDEX_IMAGE_SLOTS_PER_PAGE);
        let mut encoded = metadata.encode().unwrap();
        let layout =
            MetadataLayout::new(metadata.root.region_count, metadata.root.partition_count).unwrap();
        let partition_page = page_mut(&mut encoded, layout.partition_first_page as usize).unwrap();
        let second = page_payload_mut(partition_page, 1, REGION_METADATA_PARTITION_SIZE);
        put_u32(second, PARTITION_ID_OFFSET, 0);
        finish_page(partition_page);

        assert_eq!(
            RegionMetadata::decode(&encoded),
            Err(RegionMetadataError::InvalidField(
                "canonical_partition_directory"
            ))
        );
    }

    #[test]
    fn metadata_accepts_canonical_partial_tail_and_maximum_layouts() {
        for index_slots in [
            INDEX_IMAGE_SLOTS_PER_PAGE + 1,
            INDEX_IMAGE_SLOTS_PER_PAGE + 7,
            INDEX_IMAGE_SLOTS_PER_PAGE + 8,
            MAX_INDEX_SLOTS,
        ] {
            sample_with_index_slots(index_slots).validate().unwrap();
        }
    }

    #[test]
    fn clean_metadata_rejects_trailing_pages() {
        let mut trailing = sample().encode().unwrap();
        trailing.resize(trailing.len() + REGION_METADATA_PAGE_SIZE, 0);
        assert_eq!(
            RegionMetadata::decode(&trailing),
            Err(RegionMetadataError::InvalidLength)
        );
    }
}
