//! Stable, bounded Region/FIFO metadata for a V2 clean recovery image.
//!
//! The complete section is eagerly validated before any recovered Region
//! manager or index mapping becomes visible. Index slots remain independently
//! lazy-validated; this section contains only O(regions + index shards) state.

use crate::cache::MAX_APPEND_LANES;
use crate::checksum::Crc32c;
use crate::index::{
    MAX_INDEX_SHARDS, MAX_INDEX_SLOTS, MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE,
};
use crate::index_storage::{
    INDEX_IMAGE_PAGE_SIZE, INDEX_IMAGE_SLOTS_PER_PAGE, IndexStorageError,
    canonical_index_shard_ranges,
};
use crate::recovery_v2::{
    CacheEpochV2, DataSuperblockV2, PersistentId, RECOVERY_PAGE_SIZE, RecoveryImageHeaderV1,
};
use std::fmt;

pub(crate) const REGION_METADATA_V1_PAGE_SIZE: usize = RECOVERY_PAGE_SIZE;
pub(crate) const REGION_METADATA_V1_PAGE_HEADER_SIZE: usize = 64;
pub(crate) const REGION_METADATA_V1_ROOT_SIZE: usize = 256;
pub(crate) const REGION_METADATA_V1_REGION_SIZE: usize = 96;
pub(crate) const REGION_METADATA_V1_SHARD_SIZE: usize = 64;
pub(crate) const REGION_METADATA_V1_REGIONS_PER_PAGE: usize = (REGION_METADATA_V1_PAGE_SIZE
    - REGION_METADATA_V1_PAGE_HEADER_SIZE)
    / REGION_METADATA_V1_REGION_SIZE;
pub(crate) const REGION_METADATA_V1_SHARDS_PER_PAGE: usize = (REGION_METADATA_V1_PAGE_SIZE
    - REGION_METADATA_V1_PAGE_HEADER_SIZE)
    / REGION_METADATA_V1_SHARD_SIZE;

const PAGE_MAGIC: [u8; 8] = *b"CRRMD\0\0\0";
const FORMAT_VERSION: u16 = 1;
const ROOT_FLAGS_SUPPORTED: u32 = ROOT_FLAG_HAS_WRITE_BUDGET_WINDOW;
const ROOT_FLAG_HAS_WRITE_BUDGET_WINDOW: u32 = 1;
const MIN_ENCODED_RECORD_SIZE: u64 = 64;

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
const ROOT_SHARD_COUNT_OFFSET: usize = 100;
const ROOT_APPEND_LANE_COUNT_OFFSET: usize = 104;
const ROOT_NAMESPACE_COUNT_OFFSET: usize = 108;
const ROOT_CACHE_EPOCH_OFFSET: usize = 112;
const ROOT_CLEAR_FLOOR_SEQNO_OFFSET: usize = 120;
const ROOT_MAX_SEQNO_OFFSET: usize = 128;
const ROOT_PHYSICAL_VALUE_SLOTS_OFFSET: usize = 136;
const ROOT_PHYSICAL_DELETED_SLOTS_OFFSET: usize = 144;
const ROOT_PHYSICAL_MASKED_SLOTS_OFFSET: usize = 152;
const ROOT_LIVE_RECORD_COUNT_OFFSET: usize = 160;
const ROOT_DEPRECATED_VALUE_COUNT_OFFSET: usize = 168;
const ROOT_LIVE_RECORD_BYTES_OFFSET: usize = 176;
const ROOT_DEPRECATED_VALUE_BYTES_OFFSET: usize = 184;
const ROOT_DEPRECATED_ADMISSION_NAMESPACE_OFFSET: usize = 192;
const ROOT_FLAGS_OFFSET: usize = 196;
const ROOT_DEPRECATED_ADMISSION_LIVE_BYTES_OFFSET: usize = 200;
const ROOT_WRITE_BUDGET_WINDOW_OFFSET: usize = 208;
const ROOT_WRITE_BUDGET_USED_OFFSET: usize = 216;
const ROOT_REGION_FIRST_PAGE_OFFSET: usize = 224;
const ROOT_REGION_PAGE_COUNT_OFFSET: usize = 228;
const ROOT_SHARD_FIRST_PAGE_OFFSET: usize = 232;
const ROOT_SHARD_PAGE_COUNT_OFFSET: usize = 236;
const ROOT_FREE_REGION_COUNT_OFFSET: usize = 240;
const ROOT_ACTIVE_REGION_COUNT_OFFSET: usize = 244;
const ROOT_SEALED_REGION_COUNT_OFFSET: usize = 248;
const ROOT_RESERVED_OFFSET: usize = 252;

const REGION_ID_OFFSET: usize = 0;
const REGION_INCARNATION_OFFSET: usize = 4;
const REGION_STATE_OFFSET: usize = 8;
const REGION_FLAGS_OFFSET: usize = 9;
const REGION_RESERVED16_OFFSET: usize = 10;
const REGION_QUEUE_ORDINAL_OFFSET: usize = 12;
const REGION_CREATED_SEQNO_OFFSET: usize = 16;
const REGION_DURABLE_USED_OFFSET: usize = 24;
const REGION_MAX_SEQNO_OFFSET: usize = 32;
const REGION_RECORD_COUNT_OFFSET: usize = 40;
const REGION_LIVE_RECORD_COUNT_OFFSET: usize = 48;
const REGION_DEPRECATED_VALUE_COUNT_OFFSET: usize = 56;
const REGION_LIVE_RECORD_BYTES_OFFSET: usize = 64;
const REGION_DEPRECATED_VALUE_BYTES_OFFSET: usize = 72;
const REGION_RESERVED_OFFSET: usize = 80;

const SHARD_ID_OFFSET: usize = 0;
const SHARD_FLAGS_OFFSET: usize = 4;
const SHARD_FIRST_INDEX_PAGE_OFFSET: usize = 8;
const SHARD_INDEX_PAGE_COUNT_OFFSET: usize = 16;
const SHARD_FIRST_SLOT_OFFSET: usize = 24;
const SHARD_SLOT_COUNT_OFFSET: usize = 32;
const SHARD_PHYSICAL_VALUE_SLOTS_OFFSET: usize = 40;
const SHARD_PHYSICAL_DELETED_SLOTS_OFFSET: usize = 48;
const SHARD_PHYSICAL_MASKED_SLOTS_OFFSET: usize = 56;

const _: () = assert!(REGION_METADATA_V1_PAGE_SIZE == INDEX_IMAGE_PAGE_SIZE);
const _: () = assert!(REGION_METADATA_V1_REGIONS_PER_PAGE == 42);
const _: () = assert!(REGION_METADATA_V1_SHARDS_PER_PAGE == 63);

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PageKind {
    Root = 1,
    Region = 2,
    Shard = 3,
}

impl PageKind {
    fn decode(value: u16) -> Option<Self> {
        match value {
            1 => Some(Self::Root),
            2 => Some(Self::Region),
            3 => Some(Self::Shard),
            _ => None,
        }
    }
}

/// Only stable, quiescent Region states can appear in a CLEAN image.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMetadataStateV1 {
    Free = 0,
    Active = 1,
    Sealed = 2,
}

impl RegionMetadataStateV1 {
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
pub(crate) struct RegionMetadataRootV1 {
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
    pub(crate) shard_count: u32,
    pub(crate) append_lane_count: u32,
    /// Matches the fixed-width epoch in the V2 record header. The physical
    /// root slot remains eight bytes so the metadata page layout stays fixed;
    /// its high 32 bits are reserved and must be zero.
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) clear_floor_seqno: u64,
    pub(crate) max_seqno: u64,
    pub(crate) physical_value_slots: u64,
    pub(crate) physical_deleted_slots: u64,
    pub(crate) physical_masked_slots: u64,
    /// Number of logically reachable Value records.
    pub(crate) live_record_count: u64,
    /// Sum of their aligned on-disk record lengths. This is the sole live-byte
    /// and admission charge used by V2.
    pub(crate) live_record_bytes: u64,
    /// Zero means that no write-budget window is persisted.
    pub(crate) write_budget_window: u64,
    pub(crate) write_budget_used_bytes: u64,
    pub(crate) free_region_count: u32,
    pub(crate) active_region_count: u32,
    pub(crate) sealed_region_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataRecordV1 {
    pub(crate) region_id: u32,
    /// Last assigned incarnation. Free Regions retain it so reuse can advance
    /// without making stale bytes from an older session reachable.
    pub(crate) incarnation: u32,
    pub(crate) state: RegionMetadataStateV1,
    /// Free queue position, Active lane id, or Sealed FIFO position.
    pub(crate) queue_ordinal: u32,
    pub(crate) created_seqno: u64,
    pub(crate) durable_used_offset: u64,
    pub(crate) max_seqno: u64,
    pub(crate) physical_record_count: u64,
    pub(crate) live_record_count: u64,
    /// Sum of aligned record lengths for reachable Value records in this
    /// Region.
    pub(crate) live_record_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ShardMetadataRecordV1 {
    pub(crate) shard_id: u32,
    pub(crate) first_index_page: u64,
    pub(crate) index_page_count: u64,
    pub(crate) first_slot: u64,
    pub(crate) slot_count: u64,
    pub(crate) physical_value_slots: u64,
    pub(crate) physical_deleted_slots: u64,
    /// Transient masks must be normalized before CLEAN publication, so V1
    /// decoders require this count to be zero.
    pub(crate) physical_masked_slots: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataV1 {
    pub(crate) root: RegionMetadataRootV1,
    pub(crate) regions: Box<[RegionMetadataRecordV1]>,
    pub(crate) shards: Box<[ShardMetadataRecordV1]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMetadataV1Error {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion(u16),
    ChecksumMismatch,
    InvalidField(&'static str),
    ArithmeticOverflow,
    Allocation,
}

impl fmt::Display for RegionMetadataV1Error {
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

impl std::error::Error for RegionMetadataV1Error {}

type Result<T> = std::result::Result<T, RegionMetadataV1Error>;

impl RegionMetadataV1 {
    pub(crate) fn encoded_len(&self) -> Result<u64> {
        encoded_len_for_counts(self.root.region_count, self.root.shard_count)
    }

    /// Proves that this section belongs to the exact data and image authority
    /// selected by CLEAN. Content validation is performed separately by
    /// [`Self::validate`].
    pub(crate) fn matches_image(
        &self,
        data: DataSuperblockV2,
        image: RecoveryImageHeaderV1,
    ) -> bool {
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
        let layout = MetadataLayout::new(self.root.region_count, self.root.shard_count)?;
        let encoded_len = usize::try_from(layout.encoded_len)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(encoded_len)
            .map_err(|_| RegionMetadataV1Error::Allocation)?;
        output.resize(encoded_len, 0);

        {
            let page = page_mut(&mut output, 0)?;
            encode_page_envelope(
                page,
                PageEnvelope {
                    kind: PageKind::Root,
                    record_size: REGION_METADATA_V1_ROOT_SIZE as u16,
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
                page_payload_mut(page, 0, REGION_METADATA_V1_ROOT_SIZE),
            );
            finish_page(page);
        }

        encode_record_pages(
            &mut output,
            layout.region_first_page,
            PageKind::Region,
            REGION_METADATA_V1_REGION_SIZE,
            REGION_METADATA_V1_REGIONS_PER_PAGE,
            self.root.image_identity,
            self.root.image_generation,
            &self.regions,
            encode_region,
        )?;
        encode_record_pages(
            &mut output,
            layout.shard_first_page,
            PageKind::Shard,
            REGION_METADATA_V1_SHARD_SIZE,
            REGION_METADATA_V1_SHARDS_PER_PAGE,
            self.root.image_identity,
            self.root.image_generation,
            &self.shards,
            encode_shard,
        )?;
        Ok(output)
    }

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
        if input.len() < REGION_METADATA_V1_PAGE_SIZE
            || input.len() % REGION_METADATA_V1_PAGE_SIZE != 0
        {
            return Err(RegionMetadataV1Error::InvalidLength);
        }
        let first = page(input, 0)?;
        let first_envelope = decode_page_envelope(first)?;
        validate_envelope_shape(
            first_envelope,
            PageKind::Root,
            REGION_METADATA_V1_ROOT_SIZE,
            0,
            0,
            1,
        )?;
        require_zero_padding(first, REGION_METADATA_V1_ROOT_SIZE)?;
        let root = decode_root(page_payload(first, 0, REGION_METADATA_V1_ROOT_SIZE))?;
        if root.image_identity != first_envelope.image_identity
            || root.image_generation != first_envelope.image_generation
        {
            return Err(RegionMetadataV1Error::InvalidField("root_image_binding"));
        }
        let layout = MetadataLayout::new(root.region_count, root.shard_count)?;
        if input.len() as u64 != layout.encoded_len {
            return Err(RegionMetadataV1Error::InvalidLength);
        }
        validate_encoded_root_directory(
            page_payload(first, 0, REGION_METADATA_V1_ROOT_SIZE),
            layout,
        )?;
        validate_root_directory(root, layout)?;

        let regions = decode_record_pages(
            input,
            layout.region_first_page,
            PageKind::Region,
            REGION_METADATA_V1_REGION_SIZE,
            REGION_METADATA_V1_REGIONS_PER_PAGE,
            root.image_identity,
            root.image_generation,
            root.region_count as usize,
            decode_region,
        )?;
        let shards = decode_record_pages(
            input,
            layout.shard_first_page,
            PageKind::Shard,
            REGION_METADATA_V1_SHARD_SIZE,
            REGION_METADATA_V1_SHARDS_PER_PAGE,
            root.image_identity,
            root.image_generation,
            root.shard_count as usize,
            decode_shard,
        )?;
        Ok(Self {
            root,
            regions: regions.into_boxed_slice(),
            shards: shards.into_boxed_slice(),
        })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        let layout = MetadataLayout::new(self.root.region_count, self.root.shard_count)?;
        validate_root_directory(self.root, layout)?;
        if self.regions.len() != self.root.region_count as usize {
            return Err(RegionMetadataV1Error::InvalidField("region_count"));
        }
        if self.shards.len() != self.root.shard_count as usize {
            return Err(RegionMetadataV1Error::InvalidField("shard_count"));
        }
        validate_regions(self.root, &self.regions)?;
        validate_shards(self.root, &self.shards)?;
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct MetadataLayout {
    region_first_page: u32,
    region_page_count: u32,
    shard_first_page: u32,
    shard_page_count: u32,
    encoded_len: u64,
}

impl MetadataLayout {
    fn new(region_count: u32, shard_count: u32) -> Result<Self> {
        if region_count == 0 || shard_count == 0 {
            return Err(RegionMetadataV1Error::InvalidField("record_count"));
        }
        let region_page_count = pages_for_records(
            u64::from(region_count),
            REGION_METADATA_V1_REGIONS_PER_PAGE as u64,
        )?;
        let shard_page_count = pages_for_records(
            u64::from(shard_count),
            REGION_METADATA_V1_SHARDS_PER_PAGE as u64,
        )?;
        let region_first_page = 1_u32;
        let shard_first_page = region_first_page
            .checked_add(region_page_count)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        let page_count = shard_first_page
            .checked_add(shard_page_count)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        let encoded_len = u64::from(page_count)
            .checked_mul(REGION_METADATA_V1_PAGE_SIZE as u64)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        Ok(Self {
            region_first_page,
            region_page_count,
            shard_first_page,
            shard_page_count,
            encoded_len,
        })
    }
}

fn encoded_len_for_counts(region_count: u32, shard_count: u32) -> Result<u64> {
    Ok(MetadataLayout::new(region_count, shard_count)?.encoded_len)
}

fn pages_for_records(records: u64, per_page: u64) -> Result<u32> {
    let pages = records
        .checked_add(per_page - 1)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?
        / per_page;
    u32::try_from(pages).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)
}

fn validate_root_directory(root: RegionMetadataRootV1, layout: MetadataLayout) -> Result<()> {
    let expected_index_pages =
        pages_for_records(root.index_slots, INDEX_IMAGE_SLOTS_PER_PAGE as u64)?;
    let maximum_index_slots =
        u64::try_from(MAX_INDEX_SLOTS).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
    if root.data_superblock_generation == 0
        || root.image_generation == 0
        || root.index_slots < 8
        || root.index_slots > maximum_index_slots
        || root.index_page_count != u64::from(expected_index_pages)
        || root.region_size < RECOVERY_PAGE_SIZE as u64 + MIN_ENCODED_RECORD_SIZE
        || root.region_size > MAX_PACKED_REGION_SIZE
        || root.region_size % RECOVERY_PAGE_SIZE as u64 != 0
        || root.region_count == 0
        || root.region_count > MAX_PACKED_REGION_COUNT
        || root.shard_count == 0
        || root.shard_count as usize > MAX_INDEX_SHARDS
        || !root.shard_count.is_power_of_two()
        || u64::from(root.shard_count) > root.index_page_count
        || root.append_lane_count == 0
        || root.append_lane_count as usize > MAX_APPEND_LANES
        || root.append_lane_count >= root.region_count
        || root.cache_epoch == 0
        || root.clear_floor_seqno == 0
        || root.clear_floor_seqno > root.max_seqno
        || root.max_seqno == u64::MAX
        || (root.live_record_count == 0) != (root.live_record_bytes == 0)
        || root.live_record_count > root.physical_value_slots
        || root.live_record_bytes % 32 != 0
        || root.active_region_count != root.append_lane_count
        || root
            .free_region_count
            .checked_add(root.active_region_count)
            .and_then(|count| count.checked_add(root.sealed_region_count))
            != Some(root.region_count)
    {
        return Err(RegionMetadataV1Error::InvalidField("root"));
    }
    checked_sum3(
        root.physical_value_slots,
        root.physical_deleted_slots,
        root.physical_masked_slots,
    )?
    .le(&root.index_slots)
    .then_some(())
    .ok_or(RegionMetadataV1Error::InvalidField("physical_slot_counts"))?;
    if root.physical_masked_slots != 0 {
        return Err(RegionMetadataV1Error::InvalidField("physical_masked_slots"));
    }
    if root.write_budget_window == 0 && root.write_budget_used_bytes != 0 {
        return Err(RegionMetadataV1Error::InvalidField("write_budget_window"));
    }
    if layout.region_first_page != 1
        || layout.shard_first_page
            != layout
                .region_first_page
                .checked_add(layout.region_page_count)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?
    {
        return Err(RegionMetadataV1Error::InvalidField("section_directory"));
    }
    Ok(())
}

fn validate_encoded_root_directory(input: &[u8], layout: MetadataLayout) -> Result<()> {
    if get_u32(input, ROOT_REGION_FIRST_PAGE_OFFSET)? != layout.region_first_page
        || get_u32(input, ROOT_REGION_PAGE_COUNT_OFFSET)? != layout.region_page_count
        || get_u32(input, ROOT_SHARD_FIRST_PAGE_OFFSET)? != layout.shard_first_page
        || get_u32(input, ROOT_SHARD_PAGE_COUNT_OFFSET)? != layout.shard_page_count
    {
        return Err(RegionMetadataV1Error::InvalidField("section_directory"));
    }
    Ok(())
}

fn validate_regions(root: RegionMetadataRootV1, regions: &[RegionMetadataRecordV1]) -> Result<()> {
    let mut free_seen = zeroed_bytes(root.free_region_count as usize)?;
    let mut active_seen = zeroed_bytes(root.active_region_count as usize)?;
    let mut sealed_seen = zeroed_bytes(root.sealed_region_count as usize)?;
    let mut sealed_created = zeroed_u64(root.sealed_region_count as usize)?;
    let mut totals = RegionTotals::default();

    for (expected_id, region) in regions.iter().copied().enumerate() {
        if region.region_id as usize != expected_id
            || region.incarnation == u32::MAX
            || region.durable_used_offset < RECOVERY_PAGE_SIZE as u64
            || region.durable_used_offset > root.region_size
            || region.durable_used_offset % 32 != 0
        {
            return Err(RegionMetadataV1Error::InvalidField("region_geometry"));
        }
        let (seen, state_count) = match region.state {
            RegionMetadataStateV1::Free => (&mut free_seen, root.free_region_count),
            RegionMetadataStateV1::Active => (&mut active_seen, root.active_region_count),
            RegionMetadataStateV1::Sealed => (&mut sealed_seen, root.sealed_region_count),
        };
        if region.queue_ordinal >= state_count
            || std::mem::replace(&mut seen[region.queue_ordinal as usize], 1) != 0
        {
            return Err(RegionMetadataV1Error::InvalidField("region_queue_ordinal"));
        }
        if region.state == RegionMetadataStateV1::Sealed {
            sealed_created[region.queue_ordinal as usize] = region.created_seqno;
        }

        if region.state == RegionMetadataStateV1::Free {
            if region.created_seqno != 0
                || region.durable_used_offset != RECOVERY_PAGE_SIZE as u64
                || region.max_seqno != 0
                || region.physical_record_count != 0
                || region.live_record_count != 0
                || region.live_record_bytes != 0
            {
                return Err(RegionMetadataV1Error::InvalidField("free_region"));
            }
            continue;
        }

        let used_bytes = region.durable_used_offset - RECOVERY_PAGE_SIZE as u64;
        let empty = used_bytes == 0;
        if region.incarnation == 0
            || region.created_seqno == 0
            || region.created_seqno > root.max_seqno
            || empty != (region.physical_record_count == 0)
            || (empty && region.max_seqno != 0)
            || (!empty
                && (region.max_seqno < region.created_seqno || region.max_seqno > root.max_seqno))
            || region.live_record_count > region.physical_record_count
            || (region.live_record_count == 0) != (region.live_record_bytes == 0)
            || region.live_record_bytes > used_bytes
            || region.live_record_bytes % 32 != 0
            || !minimum_bytes_fit(region.physical_record_count, used_bytes)?
            || !minimum_bytes_fit(region.live_record_count, region.live_record_bytes)?
        {
            return Err(RegionMetadataV1Error::InvalidField("allocated_region"));
        }
        totals.add(region)?;
    }
    if free_seen
        .iter()
        .chain(&active_seen)
        .chain(&sealed_seen)
        .any(|seen| *seen == 0)
    {
        return Err(RegionMetadataV1Error::InvalidField(
            "region_queue_permutation",
        ));
    }
    if sealed_created.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(RegionMetadataV1Error::InvalidField("sealed_fifo_order"));
    }
    if totals.live_record_count != root.live_record_count
        || totals.live_record_bytes != root.live_record_bytes
    {
        return Err(RegionMetadataV1Error::InvalidField("region_accounting"));
    }
    Ok(())
}

fn validate_shards(root: RegionMetadataRootV1, shards: &[ShardMetadataRecordV1]) -> Result<()> {
    let index_slots =
        usize::try_from(root.index_slots).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
    let canonical = canonical_index_shard_ranges(index_slots).map_err(|error| match error {
        IndexStorageError::SizeOverflow => RegionMetadataV1Error::ArithmeticOverflow,
        // The canonical helper performs no I/O. Its only I/O-shaped failure
        // is the fallible allocation of this O(shards) directory.
        IndexStorageError::Io(_) => RegionMetadataV1Error::Allocation,
        _ => RegionMetadataV1Error::InvalidField("canonical_shard_directory"),
    })?;
    if canonical.len() != shards.len() {
        return Err(RegionMetadataV1Error::InvalidField(
            "canonical_shard_directory",
        ));
    }

    let mut live = 0_u64;
    let mut deleted = 0_u64;
    let mut masked = 0_u64;
    for (shard, expected) in shards.iter().copied().zip(canonical.iter().copied()) {
        let expected_first_page = u64::try_from(expected.first_page)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let expected_page_count = u64::try_from(expected.page_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let expected_first_slot = u64::try_from(expected.first_slot)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let expected_slot_count = u64::try_from(expected.slot_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        if shard.shard_id as usize != expected.shard_id
            || shard.first_index_page != expected_first_page
            || shard.index_page_count != expected_page_count
            || shard.first_slot != expected_first_slot
            || shard.slot_count != expected_slot_count
        {
            return Err(RegionMetadataV1Error::InvalidField(
                "canonical_shard_directory",
            ));
        }
        if shard.physical_masked_slots != 0
            || checked_sum3(
                shard.physical_value_slots,
                shard.physical_deleted_slots,
                shard.physical_masked_slots,
            )? > shard.slot_count
        {
            return Err(RegionMetadataV1Error::InvalidField("shard"));
        }
        live = live
            .checked_add(shard.physical_value_slots)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        deleted = deleted
            .checked_add(shard.physical_deleted_slots)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        masked = masked
            .checked_add(shard.physical_masked_slots)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    }
    if live != root.physical_value_slots
        || deleted != root.physical_deleted_slots
        || masked != root.physical_masked_slots
    {
        return Err(RegionMetadataV1Error::InvalidField("shard_accounting"));
    }
    Ok(())
}

fn minimum_bytes_fit(count: u64, bytes: u64) -> Result<bool> {
    Ok(count
        .checked_mul(MIN_ENCODED_RECORD_SIZE)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?
        <= bytes)
}

#[derive(Default)]
struct RegionTotals {
    live_record_count: u64,
    live_record_bytes: u64,
}

impl RegionTotals {
    fn add(&mut self, region: RegionMetadataRecordV1) -> Result<()> {
        self.live_record_count = checked_add(self.live_record_count, region.live_record_count)?;
        self.live_record_bytes = checked_add(self.live_record_bytes, region.live_record_bytes)?;
        Ok(())
    }
}

fn checked_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)
}

fn checked_sum3(first: u64, second: u64, third: u64) -> Result<u64> {
    checked_add(checked_add(first, second)?, third)
}

fn zeroed_bytes(len: usize) -> Result<Vec<u8>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| RegionMetadataV1Error::Allocation)?;
    output.resize(len, 0);
    Ok(output)
}

fn zeroed_u64(len: usize) -> Result<Vec<u64>> {
    let mut output = Vec::new();
    output
        .try_reserve_exact(len)
        .map_err(|_| RegionMetadataV1Error::Allocation)?;
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
        REGION_METADATA_V1_PAGE_HEADER_SIZE as u16,
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
    if page.len() != REGION_METADATA_V1_PAGE_SIZE {
        return Err(RegionMetadataV1Error::InvalidLength);
    }
    if page[..8] != PAGE_MAGIC {
        return Err(RegionMetadataV1Error::InvalidMagic);
    }
    if page_crc(page) != get_u32(page, PAGE_CRC_OFFSET)? {
        return Err(RegionMetadataV1Error::ChecksumMismatch);
    }
    let version = get_u16(page, PAGE_VERSION_OFFSET)?;
    if version != FORMAT_VERSION {
        return Err(RegionMetadataV1Error::UnsupportedVersion(version));
    }
    if get_u16(page, PAGE_HEADER_SIZE_OFFSET)? != REGION_METADATA_V1_PAGE_HEADER_SIZE as u16
        || get_u32(page, PAGE_FLAGS_OFFSET)? != 0
        || get_u32(page, PAGE_RESERVED_OFFSET)? != 0
    {
        return Err(RegionMetadataV1Error::InvalidField("page_header"));
    }
    let identity: [u8; 16] = page
        .get(PAGE_IMAGE_IDENTITY_OFFSET..PAGE_IMAGE_IDENTITY_OFFSET + 16)
        .ok_or(RegionMetadataV1Error::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataV1Error::InvalidLength)?;
    Ok(PageEnvelope {
        kind: PageKind::decode(get_u16(page, PAGE_KIND_OFFSET)?)
            .ok_or(RegionMetadataV1Error::InvalidField("page_kind"))?,
        record_size: get_u16(page, PAGE_RECORD_SIZE_OFFSET)?,
        image_identity: PersistentId::from_bytes(identity)
            .ok_or(RegionMetadataV1Error::InvalidField("page_image_identity"))?,
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
        return Err(RegionMetadataV1Error::InvalidField("page_directory"));
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
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        let page_index = usize::try_from(first_page)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?
            .checked_add(page_in_section)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        let page = page_mut(output, page_index)?;
        encode_page_envelope(
            page,
            PageEnvelope {
                kind,
                record_size: u16::try_from(record_size)
                    .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
                image_identity,
                image_generation,
                page_index: u32::try_from(page_index)
                    .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
                first_record: u32::try_from(first_record)
                    .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
                record_count: u32::try_from(records.len())
                    .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
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
        .map_err(|_| RegionMetadataV1Error::Allocation)?;
    let page_count = record_count.div_ceil(records_per_page);
    for page_in_section in 0..page_count {
        let first_record = page_in_section
            .checked_mul(records_per_page)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        let records_here = (record_count - first_record).min(records_per_page);
        let page_index = first_page as usize + page_in_section;
        let source = page(input, page_index)?;
        let envelope = decode_page_envelope(source)?;
        validate_envelope_shape(
            envelope,
            kind,
            record_size,
            u32::try_from(page_index).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
            u32::try_from(first_record).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
            u32::try_from(records_here).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
        )?;
        if envelope.image_identity != image_identity
            || envelope.image_generation != image_generation
        {
            return Err(RegionMetadataV1Error::InvalidField("page_image_binding"));
        }
        require_zero_padding(source, records_here * record_size)?;
        for record in 0..records_here {
            output.push(decode_record(page_payload(source, record, record_size))?);
        }
    }
    Ok(output)
}

fn require_zero_padding(page: &[u8], payload_len: usize) -> Result<()> {
    let end = REGION_METADATA_V1_PAGE_HEADER_SIZE
        .checked_add(payload_len)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    if end > page.len() || page[end..].iter().any(|byte| *byte != 0) {
        return Err(RegionMetadataV1Error::InvalidField("page_padding"));
    }
    Ok(())
}

fn encode_root(root: &RegionMetadataRootV1, layout: MetadataLayout, output: &mut [u8]) {
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
    put_u32(output, ROOT_SHARD_COUNT_OFFSET, root.shard_count);
    put_u32(
        output,
        ROOT_APPEND_LANE_COUNT_OFFSET,
        root.append_lane_count,
    );
    put_u32(output, ROOT_NAMESPACE_COUNT_OFFSET, 1);
    put_u64(output, ROOT_CACHE_EPOCH_OFFSET, u64::from(root.cache_epoch));
    put_u64(
        output,
        ROOT_CLEAR_FLOOR_SEQNO_OFFSET,
        root.clear_floor_seqno,
    );
    put_u64(output, ROOT_MAX_SEQNO_OFFSET, root.max_seqno);
    put_u64(
        output,
        ROOT_PHYSICAL_VALUE_SLOTS_OFFSET,
        root.physical_value_slots,
    );
    put_u64(
        output,
        ROOT_PHYSICAL_DELETED_SLOTS_OFFSET,
        root.physical_deleted_slots,
    );
    put_u64(
        output,
        ROOT_PHYSICAL_MASKED_SLOTS_OFFSET,
        root.physical_masked_slots,
    );
    put_u64(
        output,
        ROOT_LIVE_RECORD_COUNT_OFFSET,
        root.live_record_count,
    );
    put_u64(output, ROOT_DEPRECATED_VALUE_COUNT_OFFSET, 0);
    put_u64(
        output,
        ROOT_LIVE_RECORD_BYTES_OFFSET,
        root.live_record_bytes,
    );
    put_u64(output, ROOT_DEPRECATED_VALUE_BYTES_OFFSET, 0);
    put_u32(output, ROOT_DEPRECATED_ADMISSION_NAMESPACE_OFFSET, 0);
    let flags = u32::from(root.write_budget_window != 0) * ROOT_FLAG_HAS_WRITE_BUDGET_WINDOW;
    put_u32(output, ROOT_FLAGS_OFFSET, flags);
    put_u64(output, ROOT_DEPRECATED_ADMISSION_LIVE_BYTES_OFFSET, 0);
    put_u64(
        output,
        ROOT_WRITE_BUDGET_WINDOW_OFFSET,
        root.write_budget_window,
    );
    put_u64(
        output,
        ROOT_WRITE_BUDGET_USED_OFFSET,
        root.write_budget_used_bytes,
    );
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
        ROOT_SHARD_FIRST_PAGE_OFFSET,
        layout.shard_first_page,
    );
    put_u32(
        output,
        ROOT_SHARD_PAGE_COUNT_OFFSET,
        layout.shard_page_count,
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

fn decode_root(input: &[u8]) -> Result<RegionMetadataRootV1> {
    if input.len() != REGION_METADATA_V1_ROOT_SIZE
        || get_u32(input, ROOT_NAMESPACE_COUNT_OFFSET)? != 1
        || get_u32(input, ROOT_FLAGS_OFFSET)? & !ROOT_FLAGS_SUPPORTED != 0
        || get_u64(input, ROOT_DEPRECATED_VALUE_COUNT_OFFSET)? != 0
        || get_u64(input, ROOT_DEPRECATED_VALUE_BYTES_OFFSET)? != 0
        || get_u32(input, ROOT_DEPRECATED_ADMISSION_NAMESPACE_OFFSET)? != 0
        || get_u64(input, ROOT_DEPRECATED_ADMISSION_LIVE_BYTES_OFFSET)? != 0
        || get_u32(input, ROOT_RESERVED_OFFSET)? != 0
    {
        return Err(RegionMetadataV1Error::InvalidField("root_encoding"));
    }
    let flags = get_u32(input, ROOT_FLAGS_OFFSET)?;
    let write_budget_window = get_u64(input, ROOT_WRITE_BUDGET_WINDOW_OFFSET)?;
    let write_budget_used_bytes = get_u64(input, ROOT_WRITE_BUDGET_USED_OFFSET)?;
    if (flags & ROOT_FLAG_HAS_WRITE_BUDGET_WINDOW != 0) != (write_budget_window != 0)
        || (flags & ROOT_FLAG_HAS_WRITE_BUDGET_WINDOW == 0 && write_budget_used_bytes != 0)
    {
        return Err(RegionMetadataV1Error::InvalidField("root_write_budget"));
    }
    let cache_epoch = u32::try_from(get_u64(input, ROOT_CACHE_EPOCH_OFFSET)?)
        .map_err(|_| RegionMetadataV1Error::InvalidField("cache_epoch"))?;
    Ok(RegionMetadataRootV1 {
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
        shard_count: get_u32(input, ROOT_SHARD_COUNT_OFFSET)?,
        append_lane_count: get_u32(input, ROOT_APPEND_LANE_COUNT_OFFSET)?,
        cache_epoch,
        clear_floor_seqno: get_u64(input, ROOT_CLEAR_FLOOR_SEQNO_OFFSET)?,
        max_seqno: get_u64(input, ROOT_MAX_SEQNO_OFFSET)?,
        physical_value_slots: get_u64(input, ROOT_PHYSICAL_VALUE_SLOTS_OFFSET)?,
        physical_deleted_slots: get_u64(input, ROOT_PHYSICAL_DELETED_SLOTS_OFFSET)?,
        physical_masked_slots: get_u64(input, ROOT_PHYSICAL_MASKED_SLOTS_OFFSET)?,
        live_record_count: get_u64(input, ROOT_LIVE_RECORD_COUNT_OFFSET)?,
        live_record_bytes: get_u64(input, ROOT_LIVE_RECORD_BYTES_OFFSET)?,
        write_budget_window,
        write_budget_used_bytes,
        free_region_count: get_u32(input, ROOT_FREE_REGION_COUNT_OFFSET)?,
        active_region_count: get_u32(input, ROOT_ACTIVE_REGION_COUNT_OFFSET)?,
        sealed_region_count: get_u32(input, ROOT_SEALED_REGION_COUNT_OFFSET)?,
    })
}

fn encode_region(region: &RegionMetadataRecordV1, output: &mut [u8]) {
    put_u32(output, REGION_ID_OFFSET, region.region_id);
    put_u32(output, REGION_INCARNATION_OFFSET, region.incarnation);
    output[REGION_STATE_OFFSET] = region.state as u8;
    output[REGION_FLAGS_OFFSET] = 0;
    put_u16(output, REGION_RESERVED16_OFFSET, 0);
    put_u32(output, REGION_QUEUE_ORDINAL_OFFSET, region.queue_ordinal);
    put_u64(output, REGION_CREATED_SEQNO_OFFSET, region.created_seqno);
    put_u64(
        output,
        REGION_DURABLE_USED_OFFSET,
        region.durable_used_offset,
    );
    put_u64(output, REGION_MAX_SEQNO_OFFSET, region.max_seqno);
    put_u64(
        output,
        REGION_RECORD_COUNT_OFFSET,
        region.physical_record_count,
    );
    put_u64(
        output,
        REGION_LIVE_RECORD_COUNT_OFFSET,
        region.live_record_count,
    );
    put_u64(output, REGION_DEPRECATED_VALUE_COUNT_OFFSET, 0);
    put_u64(
        output,
        REGION_LIVE_RECORD_BYTES_OFFSET,
        region.live_record_bytes,
    );
    put_u64(output, REGION_DEPRECATED_VALUE_BYTES_OFFSET, 0);
}

fn decode_region(input: &[u8]) -> Result<RegionMetadataRecordV1> {
    if input.len() != REGION_METADATA_V1_REGION_SIZE
        || input[REGION_FLAGS_OFFSET] != 0
        || get_u16(input, REGION_RESERVED16_OFFSET)? != 0
        || get_u64(input, REGION_DEPRECATED_VALUE_COUNT_OFFSET)? != 0
        || get_u64(input, REGION_DEPRECATED_VALUE_BYTES_OFFSET)? != 0
        || input[REGION_RESERVED_OFFSET..]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(RegionMetadataV1Error::InvalidField("region_encoding"));
    }
    Ok(RegionMetadataRecordV1 {
        region_id: get_u32(input, REGION_ID_OFFSET)?,
        incarnation: get_u32(input, REGION_INCARNATION_OFFSET)?,
        state: RegionMetadataStateV1::decode(input[REGION_STATE_OFFSET])
            .ok_or(RegionMetadataV1Error::InvalidField("region_state"))?,
        queue_ordinal: get_u32(input, REGION_QUEUE_ORDINAL_OFFSET)?,
        created_seqno: get_u64(input, REGION_CREATED_SEQNO_OFFSET)?,
        durable_used_offset: get_u64(input, REGION_DURABLE_USED_OFFSET)?,
        max_seqno: get_u64(input, REGION_MAX_SEQNO_OFFSET)?,
        physical_record_count: get_u64(input, REGION_RECORD_COUNT_OFFSET)?,
        live_record_count: get_u64(input, REGION_LIVE_RECORD_COUNT_OFFSET)?,
        live_record_bytes: get_u64(input, REGION_LIVE_RECORD_BYTES_OFFSET)?,
    })
}

fn encode_shard(shard: &ShardMetadataRecordV1, output: &mut [u8]) {
    put_u32(output, SHARD_ID_OFFSET, shard.shard_id);
    put_u32(output, SHARD_FLAGS_OFFSET, 0);
    put_u64(
        output,
        SHARD_FIRST_INDEX_PAGE_OFFSET,
        shard.first_index_page,
    );
    put_u64(
        output,
        SHARD_INDEX_PAGE_COUNT_OFFSET,
        shard.index_page_count,
    );
    put_u64(output, SHARD_FIRST_SLOT_OFFSET, shard.first_slot);
    put_u64(output, SHARD_SLOT_COUNT_OFFSET, shard.slot_count);
    put_u64(
        output,
        SHARD_PHYSICAL_VALUE_SLOTS_OFFSET,
        shard.physical_value_slots,
    );
    put_u64(
        output,
        SHARD_PHYSICAL_DELETED_SLOTS_OFFSET,
        shard.physical_deleted_slots,
    );
    put_u64(
        output,
        SHARD_PHYSICAL_MASKED_SLOTS_OFFSET,
        shard.physical_masked_slots,
    );
}

fn decode_shard(input: &[u8]) -> Result<ShardMetadataRecordV1> {
    if input.len() != REGION_METADATA_V1_SHARD_SIZE || get_u32(input, SHARD_FLAGS_OFFSET)? != 0 {
        return Err(RegionMetadataV1Error::InvalidField("shard_encoding"));
    }
    Ok(ShardMetadataRecordV1 {
        shard_id: get_u32(input, SHARD_ID_OFFSET)?,
        first_index_page: get_u64(input, SHARD_FIRST_INDEX_PAGE_OFFSET)?,
        index_page_count: get_u64(input, SHARD_INDEX_PAGE_COUNT_OFFSET)?,
        first_slot: get_u64(input, SHARD_FIRST_SLOT_OFFSET)?,
        slot_count: get_u64(input, SHARD_SLOT_COUNT_OFFSET)?,
        physical_value_slots: get_u64(input, SHARD_PHYSICAL_VALUE_SLOTS_OFFSET)?,
        physical_deleted_slots: get_u64(input, SHARD_PHYSICAL_DELETED_SLOTS_OFFSET)?,
        physical_masked_slots: get_u64(input, SHARD_PHYSICAL_MASKED_SLOTS_OFFSET)?,
    })
}

fn page(input: &[u8], page_index: usize) -> Result<&[u8]> {
    let start = page_index
        .checked_mul(REGION_METADATA_V1_PAGE_SIZE)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    let end = start
        .checked_add(REGION_METADATA_V1_PAGE_SIZE)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    input
        .get(start..end)
        .ok_or(RegionMetadataV1Error::InvalidLength)
}

fn page_mut(input: &mut [u8], page_index: usize) -> Result<&mut [u8]> {
    let start = page_index
        .checked_mul(REGION_METADATA_V1_PAGE_SIZE)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    let end = start
        .checked_add(REGION_METADATA_V1_PAGE_SIZE)
        .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
    input
        .get_mut(start..end)
        .ok_or(RegionMetadataV1Error::InvalidLength)
}

fn page_payload(page: &[u8], record: usize, record_size: usize) -> &[u8] {
    let start = REGION_METADATA_V1_PAGE_HEADER_SIZE + record * record_size;
    &page[start..start + record_size]
}

fn page_payload_mut(page: &mut [u8], record: usize, record_size: usize) -> &mut [u8] {
    let start = REGION_METADATA_V1_PAGE_HEADER_SIZE + record * record_size;
    &mut page[start..start + record_size]
}

fn get_id(input: &[u8], offset: usize) -> Result<PersistentId> {
    let bytes: [u8; 16] = input
        .get(offset..offset + 16)
        .ok_or(RegionMetadataV1Error::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataV1Error::InvalidLength)?;
    PersistentId::from_bytes(bytes).ok_or(RegionMetadataV1Error::InvalidField("identity"))
}

fn put_id(output: &mut [u8], offset: usize, id: PersistentId) {
    output[offset..offset + 16].copy_from_slice(&id.to_bytes());
}

fn get_u16(input: &[u8], offset: usize) -> Result<u16> {
    let bytes = input
        .get(offset..offset + 2)
        .ok_or(RegionMetadataV1Error::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataV1Error::InvalidLength)?;
    Ok(u16::from_le_bytes(bytes))
}

fn get_u32(input: &[u8], offset: usize) -> Result<u32> {
    let bytes = input
        .get(offset..offset + 4)
        .ok_or(RegionMetadataV1Error::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataV1Error::InvalidLength)?;
    Ok(u32::from_le_bytes(bytes))
}

fn get_u64(input: &[u8], offset: usize) -> Result<u64> {
    let bytes = input
        .get(offset..offset + 8)
        .ok_or(RegionMetadataV1Error::InvalidLength)?
        .try_into()
        .map_err(|_| RegionMetadataV1Error::InvalidLength)?;
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

    fn sample() -> RegionMetadataV1 {
        RegionMetadataV1 {
            root: RegionMetadataRootV1 {
                cache_uuid: id(1),
                data_identity: id(2),
                data_superblock_generation: 3,
                image_identity: id(4),
                image_generation: 5,
                config_fingerprint: 6,
                index_slots: 200,
                index_page_count: 2,
                region_size: 32 * 1024 * 1024,
                region_count: 4,
                shard_count: 2,
                append_lane_count: 1,
                cache_epoch: 1,
                clear_floor_seqno: 1,
                max_seqno: 4,
                physical_value_slots: 1,
                physical_deleted_slots: 2,
                physical_masked_slots: 0,
                live_record_count: 1,
                live_record_bytes: 128,
                write_budget_window: 20_000,
                write_budget_used_bytes: 4096,
                free_region_count: 1,
                active_region_count: 1,
                sealed_region_count: 2,
            },
            regions: vec![
                RegionMetadataRecordV1 {
                    region_id: 0,
                    incarnation: 1,
                    state: RegionMetadataStateV1::Active,
                    queue_ordinal: 0,
                    created_seqno: 1,
                    durable_used_offset: RECOVERY_PAGE_SIZE as u64,
                    max_seqno: 0,
                    physical_record_count: 0,
                    live_record_count: 0,
                    live_record_bytes: 0,
                },
                RegionMetadataRecordV1 {
                    region_id: 1,
                    incarnation: 1,
                    state: RegionMetadataStateV1::Sealed,
                    queue_ordinal: 0,
                    created_seqno: 2,
                    durable_used_offset: RECOVERY_PAGE_SIZE as u64 + 128,
                    max_seqno: 3,
                    physical_record_count: 2,
                    live_record_count: 1,
                    live_record_bytes: 128,
                },
                RegionMetadataRecordV1 {
                    region_id: 2,
                    incarnation: 1,
                    state: RegionMetadataStateV1::Sealed,
                    queue_ordinal: 1,
                    created_seqno: 4,
                    durable_used_offset: RECOVERY_PAGE_SIZE as u64 + 64,
                    max_seqno: 4,
                    physical_record_count: 1,
                    live_record_count: 0,
                    live_record_bytes: 0,
                },
                RegionMetadataRecordV1 {
                    region_id: 3,
                    incarnation: 0,
                    state: RegionMetadataStateV1::Free,
                    queue_ordinal: 0,
                    created_seqno: 0,
                    durable_used_offset: RECOVERY_PAGE_SIZE as u64,
                    max_seqno: 0,
                    physical_record_count: 0,
                    live_record_count: 0,
                    live_record_bytes: 0,
                },
            ]
            .into_boxed_slice(),
            shards: vec![
                ShardMetadataRecordV1 {
                    shard_id: 0,
                    first_index_page: 0,
                    index_page_count: 1,
                    first_slot: 0,
                    slot_count: 126,
                    physical_value_slots: 1,
                    physical_deleted_slots: 1,
                    physical_masked_slots: 0,
                },
                ShardMetadataRecordV1 {
                    shard_id: 1,
                    first_index_page: 1,
                    index_page_count: 1,
                    first_slot: 126,
                    slot_count: 74,
                    physical_value_slots: 0,
                    physical_deleted_slots: 1,
                    physical_masked_slots: 0,
                },
            ]
            .into_boxed_slice(),
        }
    }

    fn sample_with_index_slots(index_slots: usize) -> RegionMetadataV1 {
        let mut metadata = sample();
        let ranges = canonical_index_shard_ranges(index_slots).unwrap();
        metadata.root.index_slots = index_slots as u64;
        metadata.root.index_page_count = ranges.iter().map(|range| range.page_count as u64).sum();
        metadata.root.shard_count = ranges.len() as u32;
        metadata.shards = ranges
            .iter()
            .map(|range| ShardMetadataRecordV1 {
                shard_id: range.shard_id as u32,
                first_index_page: range.first_page as u64,
                index_page_count: range.page_count as u64,
                first_slot: range.first_slot as u64,
                slot_count: range.slot_count as u64,
                physical_value_slots: 0,
                physical_deleted_slots: 0,
                physical_masked_slots: 0,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        metadata.shards[0].physical_value_slots = metadata.root.physical_value_slots;
        metadata.shards[0].physical_deleted_slots = metadata.root.physical_deleted_slots;
        metadata
    }

    #[test]
    fn round_trip_preserves_exact_frozen_authority() {
        let expected = sample();
        let encoded = expected.encode().unwrap();
        assert_eq!(encoded.len() as u64, expected.encoded_len().unwrap());
        assert_eq!(RegionMetadataV1::decode(&encoded).unwrap(), expected);
        assert_eq!(&encoded[..8], &PAGE_MAGIC);
        assert_eq!(
            &encoded[REGION_METADATA_V1_PAGE_HEADER_SIZE + ROOT_INDEX_SLOTS_OFFSET
                ..REGION_METADATA_V1_PAGE_HEADER_SIZE + ROOT_INDEX_SLOTS_OFFSET + 8],
            &200_u64.to_le_bytes()
        );
        let root = &encoded[REGION_METADATA_V1_PAGE_HEADER_SIZE
            ..REGION_METADATA_V1_PAGE_HEADER_SIZE + REGION_METADATA_V1_ROOT_SIZE];
        assert_eq!(get_u64(root, ROOT_DEPRECATED_VALUE_COUNT_OFFSET), Ok(0));
        assert_eq!(get_u64(root, ROOT_DEPRECATED_VALUE_BYTES_OFFSET), Ok(0));
        assert_eq!(
            get_u32(root, ROOT_DEPRECATED_ADMISSION_NAMESPACE_OFFSET),
            Ok(0)
        );
        assert_eq!(
            get_u64(root, ROOT_DEPRECATED_ADMISSION_LIVE_BYTES_OFFSET),
            Ok(0)
        );
    }

    #[test]
    fn deprecated_accounting_slots_and_epoch_high_bits_are_rejected() {
        let mut root_reserved = sample().encode().unwrap();
        let root_page = page_mut(&mut root_reserved, 0).unwrap();
        put_u64(
            page_payload_mut(root_page, 0, REGION_METADATA_V1_ROOT_SIZE),
            ROOT_DEPRECATED_VALUE_COUNT_OFFSET,
            1,
        );
        finish_page(root_page);
        assert_eq!(
            RegionMetadataV1::decode(&root_reserved),
            Err(RegionMetadataV1Error::InvalidField("root_encoding"))
        );

        let mut region_reserved = sample().encode().unwrap();
        let region_page = page_mut(&mut region_reserved, 1).unwrap();
        put_u64(
            page_payload_mut(region_page, 1, REGION_METADATA_V1_REGION_SIZE),
            REGION_DEPRECATED_VALUE_BYTES_OFFSET,
            32,
        );
        finish_page(region_page);
        assert_eq!(
            RegionMetadataV1::decode(&region_reserved),
            Err(RegionMetadataV1Error::InvalidField("region_encoding"))
        );

        let mut wide_epoch = sample().encode().unwrap();
        let root_page = page_mut(&mut wide_epoch, 0).unwrap();
        put_u64(
            page_payload_mut(root_page, 0, REGION_METADATA_V1_ROOT_SIZE),
            ROOT_CACHE_EPOCH_OFFSET,
            u64::from(u32::MAX) + 1,
        );
        finish_page(root_page);
        assert_eq!(
            RegionMetadataV1::decode(&wide_epoch),
            Err(RegionMetadataV1Error::InvalidField("cache_epoch"))
        );
    }

    #[test]
    fn live_record_charge_is_aligned_and_exactly_summed() {
        let mut unaligned = sample();
        unaligned.root.live_record_bytes = 127;
        unaligned.regions[1].live_record_bytes = 127;
        assert_eq!(
            unaligned.validate(),
            Err(RegionMetadataV1Error::InvalidField("root"))
        );

        let mut duplicate = sample();
        duplicate.root.live_record_bytes += 64;
        assert_eq!(
            duplicate.validate(),
            Err(RegionMetadataV1Error::InvalidField("region_accounting"))
        );
    }

    #[test]
    fn any_metadata_page_crc_failure_rejects_the_whole_image() {
        let mut encoded = sample().encode().unwrap();
        encoded[REGION_METADATA_V1_PAGE_SIZE + REGION_METADATA_V1_PAGE_HEADER_SIZE + 7] ^= 0x40;
        assert_eq!(
            RegionMetadataV1::decode(&encoded),
            Err(RegionMetadataV1Error::ChecksumMismatch)
        );
    }

    #[test]
    fn owned_decode_runs_queue_validation_after_page_parsing() {
        let mut encoded = sample().encode().unwrap();
        let region_page = page_mut(&mut encoded, 1).unwrap();
        put_u32(
            page_payload_mut(region_page, 2, REGION_METADATA_V1_REGION_SIZE),
            REGION_QUEUE_ORDINAL_OFFSET,
            0,
        );
        finish_page(region_page);
        assert_eq!(
            RegionMetadataV1::decode_owned(encoded),
            Err(RegionMetadataV1Error::InvalidField("region_queue_ordinal"))
        );
    }

    #[test]
    fn shard_ranges_must_be_page_aligned_and_cover_every_slot() {
        let mut invalid = sample();
        invalid.shards[1].first_slot = 125;
        assert_eq!(
            invalid.encode(),
            Err(RegionMetadataV1Error::InvalidField(
                "canonical_shard_directory"
            ))
        );
    }

    #[test]
    fn crc_valid_page_balanced_but_non_canonical_shard_directory_is_rejected() {
        let metadata = sample_with_index_slots(12 * INDEX_IMAGE_SLOTS_PER_PAGE);
        let ranges = canonical_index_shard_ranges(metadata.root.index_slots as usize).unwrap();
        let right = ranges
            .iter()
            .position(|range| range.shard_id != 0 && range.page_count >= 2)
            .unwrap();
        let left = right - 1;
        assert_eq!(right / REGION_METADATA_V1_SHARDS_PER_PAGE, 0);

        let mut encoded = metadata.encode().unwrap();
        let layout =
            MetadataLayout::new(metadata.root.region_count, metadata.root.shard_count).unwrap();
        let shard_page = page_mut(&mut encoded, layout.shard_first_page as usize).unwrap();
        let left_record = page_payload_mut(shard_page, left, REGION_METADATA_V1_SHARD_SIZE);
        let left_pages = get_u64(left_record, SHARD_INDEX_PAGE_COUNT_OFFSET).unwrap();
        let left_slots = get_u64(left_record, SHARD_SLOT_COUNT_OFFSET).unwrap();
        put_u64(left_record, SHARD_INDEX_PAGE_COUNT_OFFSET, left_pages + 1);
        put_u64(
            left_record,
            SHARD_SLOT_COUNT_OFFSET,
            left_slots + INDEX_IMAGE_SLOTS_PER_PAGE as u64,
        );
        let right_record = page_payload_mut(shard_page, right, REGION_METADATA_V1_SHARD_SIZE);
        let right_first_page = get_u64(right_record, SHARD_FIRST_INDEX_PAGE_OFFSET).unwrap();
        let right_pages = get_u64(right_record, SHARD_INDEX_PAGE_COUNT_OFFSET).unwrap();
        let right_first_slot = get_u64(right_record, SHARD_FIRST_SLOT_OFFSET).unwrap();
        let right_slots = get_u64(right_record, SHARD_SLOT_COUNT_OFFSET).unwrap();
        put_u64(
            right_record,
            SHARD_FIRST_INDEX_PAGE_OFFSET,
            right_first_page + 1,
        );
        put_u64(right_record, SHARD_INDEX_PAGE_COUNT_OFFSET, right_pages - 1);
        put_u64(
            right_record,
            SHARD_FIRST_SLOT_OFFSET,
            right_first_slot + INDEX_IMAGE_SLOTS_PER_PAGE as u64,
        );
        put_u64(
            right_record,
            SHARD_SLOT_COUNT_OFFSET,
            right_slots - INDEX_IMAGE_SLOTS_PER_PAGE as u64,
        );
        finish_page(shard_page);

        assert_eq!(
            RegionMetadataV1::decode(&encoded),
            Err(RegionMetadataV1Error::InvalidField(
                "canonical_shard_directory"
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
    fn clean_metadata_rejects_transient_masks_and_trailing_pages() {
        let mut masked = sample();
        masked.root.physical_masked_slots = 1;
        masked.shards[0].physical_masked_slots = 1;
        assert_eq!(
            masked.encode(),
            Err(RegionMetadataV1Error::InvalidField("physical_masked_slots"))
        );

        let mut trailing = sample().encode().unwrap();
        trailing.resize(trailing.len() + REGION_METADATA_V1_PAGE_SIZE, 0);
        assert_eq!(
            RegionMetadataV1::decode(&trailing),
            Err(RegionMetadataV1Error::InvalidLength)
        );
    }
}
