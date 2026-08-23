//! Stable codecs for the V2 recovery control plane.
//!
//! V2 deliberately keeps session state outside the data superblock. The data
//! superblock describes an immutable cache/data identity and geometry. A small
//! two-page state file is the sole authority for `EMPTY`, `RUNNING`, and
//! `CLEAN`. This module performs no I/O; callers must write the returned page
//! to the selected slot and provide the required `fdatasync` barrier.

use crate::checksum::{Crc32c, crc32c};
use crate::index::{MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE};
use crate::index_storage::{INDEX_IMAGE_PAGE_SIZE, INDEX_IMAGE_SLOTS_PER_PAGE};

pub(crate) const RECOVERY_V2_FORMAT_VERSION: u16 = 2;
pub(crate) const RECOVERY_PAGE_SIZE: usize = 4 * 1024;
pub(crate) const STATE_SLOT_COUNT: usize = 2;
pub(crate) const STATE_FILE_SIZE: usize = STATE_SLOT_COUNT * RECOVERY_PAGE_SIZE;
pub(crate) const DATA_REGION_AREA_OFFSET_V2: u64 = RECOVERY_PAGE_SIZE as u64;
pub(crate) const RECOVERY_IMAGE_INDEX_OFFSET_V1: u64 = INDEX_IMAGE_PAGE_SIZE as u64;
pub(crate) const RECOVERY_IMAGE_SLOTS_PER_PAGE_V1: u64 = INDEX_IMAGE_SLOTS_PER_PAGE as u64;
pub(crate) const REGION_HEADER_SIZE_V2: u32 = RECOVERY_PAGE_SIZE as u32;
pub(crate) const RECORD_ALIGNMENT_V2: u32 = 32;
pub(crate) const RECORD_FORMAT_VERSION_V2: u16 = 1;

const DATA_MAGIC: [u8; 8] = *b"CRDATA\0\0";
const STATE_MAGIC: [u8; 8] = *b"CRSTATE\0";
const IMAGE_MAGIC: [u8; 8] = *b"CRIMAGE\0";
const PAGE_CRC_OFFSET: usize = RECOVERY_PAGE_SIZE - size_of::<u32>();

const _: () = assert!(RECOVERY_PAGE_SIZE == INDEX_IMAGE_PAGE_SIZE);

const DATA_HEADER_SIZE: u16 = 112;
const DATA_VERSION_OFFSET: usize = 8;
const DATA_HEADER_SIZE_OFFSET: usize = 10;
const DATA_FLAGS_OFFSET: usize = 12;
const DATA_GENERATION_OFFSET: usize = 16;
const DATA_CACHE_UUID_OFFSET: usize = 24;
const DATA_IDENTITY_OFFSET: usize = 40;
const DATA_FILE_LEN_OFFSET: usize = 56;
const DATA_REGION_SIZE_OFFSET: usize = 64;
const DATA_REGION_AREA_OFFSET: usize = 72;
const DATA_REGION_COUNT_OFFSET: usize = 80;
const DATA_REGION_HEADER_SIZE_OFFSET: usize = 84;
const DATA_RECORD_ALIGNMENT_OFFSET: usize = 88;
const DATA_RECORD_FORMAT_OFFSET: usize = 92;
const DATA_HASH_SEED_OFFSET: usize = 96;
const DATA_CONFIG_FINGERPRINT_OFFSET: usize = 104;

const STATE_HEADER_SIZE: u16 = 120;
const STATE_VERSION_OFFSET: usize = 8;
const STATE_HEADER_SIZE_OFFSET: usize = 10;
const STATE_KIND_OFFSET: usize = 12;
const STATE_FLAGS_OFFSET: usize = 13;
const STATE_GENERATION_OFFSET: usize = 16;
const STATE_CACHE_UUID_OFFSET: usize = 24;
const STATE_DATA_IDENTITY_OFFSET: usize = 40;
const STATE_DATA_GENERATION_OFFSET: usize = 56;
const STATE_DATA_FILE_LEN_OFFSET: usize = 64;
const STATE_HASH_SEED_OFFSET: usize = 72;
const STATE_CONFIG_FINGERPRINT_OFFSET: usize = 80;
const STATE_IMAGE_IDENTITY_OFFSET: usize = 88;
const STATE_IMAGE_GENERATION_OFFSET: usize = 104;
const STATE_IMAGE_FILE_LEN_OFFSET: usize = 112;

const STATE_FLAG_HAS_IMAGE: u8 = 1;

const IMAGE_FORMAT_VERSION: u16 = 1;
const IMAGE_HEADER_SIZE: u16 = 144;
const IMAGE_VERSION_OFFSET: usize = 8;
const IMAGE_HEADER_SIZE_OFFSET: usize = 10;
const IMAGE_FLAGS_OFFSET: usize = 12;
const IMAGE_CACHE_UUID_OFFSET: usize = 16;
const IMAGE_DATA_IDENTITY_OFFSET: usize = 32;
const IMAGE_DATA_GENERATION_OFFSET: usize = 48;
const IMAGE_HASH_SEED_OFFSET: usize = 56;
const IMAGE_CONFIG_FINGERPRINT_OFFSET: usize = 64;
const IMAGE_IDENTITY_OFFSET: usize = 72;
const IMAGE_GENERATION_OFFSET: usize = 88;
const IMAGE_FILE_LEN_OFFSET: usize = 96;
const IMAGE_INDEX_SLOTS_OFFSET: usize = 104;
const IMAGE_INDEX_OFFSET_OFFSET: usize = 112;
const IMAGE_INDEX_LEN_OFFSET: usize = 120;
const IMAGE_REGION_TABLE_OFFSET_OFFSET: usize = 128;
const IMAGE_REGION_TABLE_LEN_OFFSET: usize = 136;

/// A format-time generated, non-zero 128-bit identity.
///
/// This type does not generate randomness. The format owner must supply bytes
/// from its UUID/random source and must never reuse the resulting cache UUID.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct PersistentId([u8; 16]);

impl PersistentId {
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Option<Self> {
        (bytes != [0; 16]).then_some(Self(bytes))
    }

    pub(crate) const fn to_bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Geometry whose exact values are part of the V2 data identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DataGeometryV2 {
    pub(crate) data_file_len: u64,
    pub(crate) region_size: u64,
    pub(crate) region_count: u32,
}

impl DataGeometryV2 {
    pub(crate) fn expected_file_len(region_size: u64, region_count: u32) -> Option<u64> {
        region_size
            .checked_mul(u64::from(region_count))?
            .checked_add(DATA_REGION_AREA_OFFSET_V2)
    }

    pub(crate) fn is_valid(self) -> bool {
        let minimum_region_size = u64::from(REGION_HEADER_SIZE_V2) + 64;
        self.region_count != 0
            && self.region_count <= MAX_PACKED_REGION_COUNT
            && self.region_size >= minimum_region_size
            && self.region_size <= MAX_PACKED_REGION_SIZE
            && self.region_size % RECOVERY_PAGE_SIZE as u64 == 0
            && Self::expected_file_len(self.region_size, self.region_count)
                == Some(self.data_file_len)
    }
}

/// Immutable metadata at offset zero of a V2 data file.
///
/// There is intentionally no clean/dirty/session bit here. Rewriting this page
/// is a format/reset operation, not a normal cache mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DataSuperblockV2 {
    pub(crate) generation: u64,
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) geometry: DataGeometryV2,
    pub(crate) hash_seed: u64,
    pub(crate) config_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DataSuperblockV2Probe {
    Empty,
    Valid(DataSuperblockV2),
    Corrupt,
    Unsupported(u16),
    Unrecognized,
    Truncated,
}

impl DataSuperblockV2 {
    pub(crate) fn encode(self) -> Result<[u8; RECOVERY_PAGE_SIZE], CodecError> {
        if !self.is_valid() {
            return Err(CodecError::DataSuperblock);
        }

        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        page[..DATA_MAGIC.len()].copy_from_slice(&DATA_MAGIC);
        put_u16(&mut page, DATA_VERSION_OFFSET, RECOVERY_V2_FORMAT_VERSION);
        put_u16(&mut page, DATA_HEADER_SIZE_OFFSET, DATA_HEADER_SIZE);
        put_u32(&mut page, DATA_FLAGS_OFFSET, 0);
        put_u64(&mut page, DATA_GENERATION_OFFSET, self.generation);
        put_id(&mut page, DATA_CACHE_UUID_OFFSET, self.cache_uuid);
        put_id(&mut page, DATA_IDENTITY_OFFSET, self.data_identity);
        put_u64(&mut page, DATA_FILE_LEN_OFFSET, self.geometry.data_file_len);
        put_u64(
            &mut page,
            DATA_REGION_SIZE_OFFSET,
            self.geometry.region_size,
        );
        put_u64(
            &mut page,
            DATA_REGION_AREA_OFFSET,
            DATA_REGION_AREA_OFFSET_V2,
        );
        put_u32(
            &mut page,
            DATA_REGION_COUNT_OFFSET,
            self.geometry.region_count,
        );
        put_u32(
            &mut page,
            DATA_REGION_HEADER_SIZE_OFFSET,
            REGION_HEADER_SIZE_V2,
        );
        put_u32(&mut page, DATA_RECORD_ALIGNMENT_OFFSET, RECORD_ALIGNMENT_V2);
        put_u16(
            &mut page,
            DATA_RECORD_FORMAT_OFFSET,
            RECORD_FORMAT_VERSION_V2,
        );
        put_u64(&mut page, DATA_HASH_SEED_OFFSET, self.hash_seed);
        put_u64(
            &mut page,
            DATA_CONFIG_FINGERPRINT_OFFSET,
            self.config_fingerprint,
        );
        write_page_crc(&mut page);
        Ok(page)
    }

    pub(crate) fn decode(page: &[u8]) -> Option<Self> {
        match Self::probe(page) {
            DataSuperblockV2Probe::Valid(superblock) => Some(superblock),
            _ => None,
        }
    }

    pub(crate) fn probe(page: &[u8]) -> DataSuperblockV2Probe {
        if page.len() != RECOVERY_PAGE_SIZE {
            return DataSuperblockV2Probe::Truncated;
        }
        if page.iter().all(|byte| *byte == 0) {
            return DataSuperblockV2Probe::Empty;
        }
        if page[..DATA_MAGIC.len()] != DATA_MAGIC {
            return DataSuperblockV2Probe::Unrecognized;
        }
        if !page_crc_matches(page) {
            return DataSuperblockV2Probe::Corrupt;
        }
        let Some(version) = get_u16(page, DATA_VERSION_OFFSET) else {
            return DataSuperblockV2Probe::Corrupt;
        };
        if version != RECOVERY_V2_FORMAT_VERSION {
            return DataSuperblockV2Probe::Unsupported(version);
        }
        if get_u16(page, DATA_HEADER_SIZE_OFFSET) != Some(DATA_HEADER_SIZE)
            || get_u32(page, DATA_FLAGS_OFFSET) != Some(0)
            || get_u64(page, DATA_REGION_AREA_OFFSET) != Some(DATA_REGION_AREA_OFFSET_V2)
            || get_u32(page, DATA_REGION_HEADER_SIZE_OFFSET) != Some(REGION_HEADER_SIZE_V2)
            || get_u32(page, DATA_RECORD_ALIGNMENT_OFFSET) != Some(RECORD_ALIGNMENT_V2)
            || get_u16(page, DATA_RECORD_FORMAT_OFFSET) != Some(RECORD_FORMAT_VERSION_V2)
            || page[DATA_RECORD_FORMAT_OFFSET + size_of::<u16>()..DATA_HASH_SEED_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
            || page[usize::from(DATA_HEADER_SIZE)..PAGE_CRC_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
        {
            return DataSuperblockV2Probe::Corrupt;
        }

        let Some(cache_uuid) = get_id(page, DATA_CACHE_UUID_OFFSET) else {
            return DataSuperblockV2Probe::Corrupt;
        };
        let Some(data_identity) = get_id(page, DATA_IDENTITY_OFFSET) else {
            return DataSuperblockV2Probe::Corrupt;
        };
        let Some(superblock) = (|| {
            Some(Self {
                generation: get_u64(page, DATA_GENERATION_OFFSET)?,
                cache_uuid,
                data_identity,
                geometry: DataGeometryV2 {
                    data_file_len: get_u64(page, DATA_FILE_LEN_OFFSET)?,
                    region_size: get_u64(page, DATA_REGION_SIZE_OFFSET)?,
                    region_count: get_u32(page, DATA_REGION_COUNT_OFFSET)?,
                },
                hash_seed: get_u64(page, DATA_HASH_SEED_OFFSET)?,
                config_fingerprint: get_u64(page, DATA_CONFIG_FINGERPRINT_OFFSET)?,
            })
        })() else {
            return DataSuperblockV2Probe::Corrupt;
        };

        if !superblock.is_valid() {
            return DataSuperblockV2Probe::Corrupt;
        }
        DataSuperblockV2Probe::Valid(superblock)
    }

    fn is_valid(self) -> bool {
        self.generation != 0 && self.geometry.is_valid()
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryState {
    Empty = 0,
    Running = 1,
    Clean = 2,
}

impl RecoveryState {
    fn decode(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Empty),
            1 => Some(Self::Running),
            2 => Some(Self::Clean),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ImageBindingV2 {
    pub(crate) identity: PersistentId,
    pub(crate) generation: u64,
    pub(crate) file_len: u64,
}

impl ImageBindingV2 {
    fn is_valid(self) -> bool {
        self.generation != 0 && self.file_len >= RECOVERY_PAGE_SIZE as u64
    }
}

/// Header of the immutable clean-recovery image.
///
/// The index mapping starts at exactly 4 KiB. A mandatory page-aligned Region
/// metadata section follows it; that format can evolve independently of this header.
/// The state file must bind the exact `image_binding()` before this image is
/// eligible for a clean open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecoveryImageHeaderV1 {
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) data_superblock_generation: u64,
    pub(crate) hash_seed: u64,
    pub(crate) config_fingerprint: u64,
    pub(crate) image_identity: PersistentId,
    pub(crate) image_generation: u64,
    pub(crate) image_file_len: u64,
    pub(crate) index_slots: u64,
    pub(crate) index_offset: u64,
    pub(crate) index_len: u64,
    pub(crate) region_table_offset: u64,
    pub(crate) region_table_len: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecoveryImageHeaderV1Probe {
    Empty,
    Valid(RecoveryImageHeaderV1),
    Corrupt,
    Unsupported(u16),
    Unrecognized,
    Truncated,
}

impl RecoveryImageHeaderV1 {
    pub(crate) fn encode(self) -> Result<[u8; RECOVERY_PAGE_SIZE], CodecError> {
        if !self.is_valid() {
            return Err(CodecError::RecoveryImageHeader);
        }

        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        page[..IMAGE_MAGIC.len()].copy_from_slice(&IMAGE_MAGIC);
        put_u16(&mut page, IMAGE_VERSION_OFFSET, IMAGE_FORMAT_VERSION);
        put_u16(&mut page, IMAGE_HEADER_SIZE_OFFSET, IMAGE_HEADER_SIZE);
        put_u32(&mut page, IMAGE_FLAGS_OFFSET, 0);
        put_id(&mut page, IMAGE_CACHE_UUID_OFFSET, self.cache_uuid);
        put_id(&mut page, IMAGE_DATA_IDENTITY_OFFSET, self.data_identity);
        put_u64(
            &mut page,
            IMAGE_DATA_GENERATION_OFFSET,
            self.data_superblock_generation,
        );
        put_u64(&mut page, IMAGE_HASH_SEED_OFFSET, self.hash_seed);
        put_u64(
            &mut page,
            IMAGE_CONFIG_FINGERPRINT_OFFSET,
            self.config_fingerprint,
        );
        put_id(&mut page, IMAGE_IDENTITY_OFFSET, self.image_identity);
        put_u64(&mut page, IMAGE_GENERATION_OFFSET, self.image_generation);
        put_u64(&mut page, IMAGE_FILE_LEN_OFFSET, self.image_file_len);
        put_u64(&mut page, IMAGE_INDEX_SLOTS_OFFSET, self.index_slots);
        put_u64(&mut page, IMAGE_INDEX_OFFSET_OFFSET, self.index_offset);
        put_u64(&mut page, IMAGE_INDEX_LEN_OFFSET, self.index_len);
        put_u64(
            &mut page,
            IMAGE_REGION_TABLE_OFFSET_OFFSET,
            self.region_table_offset,
        );
        put_u64(
            &mut page,
            IMAGE_REGION_TABLE_LEN_OFFSET,
            self.region_table_len,
        );
        write_page_crc(&mut page);
        Ok(page)
    }

    pub(crate) fn decode(page: &[u8]) -> Option<Self> {
        match Self::probe(page) {
            RecoveryImageHeaderV1Probe::Valid(header) => Some(header),
            _ => None,
        }
    }

    pub(crate) fn probe(page: &[u8]) -> RecoveryImageHeaderV1Probe {
        if page.len() != RECOVERY_PAGE_SIZE {
            return RecoveryImageHeaderV1Probe::Truncated;
        }
        if page.iter().all(|byte| *byte == 0) {
            return RecoveryImageHeaderV1Probe::Empty;
        }
        if page[..IMAGE_MAGIC.len()] != IMAGE_MAGIC {
            return RecoveryImageHeaderV1Probe::Unrecognized;
        }
        if !page_crc_matches(page) {
            return RecoveryImageHeaderV1Probe::Corrupt;
        }
        let Some(version) = get_u16(page, IMAGE_VERSION_OFFSET) else {
            return RecoveryImageHeaderV1Probe::Corrupt;
        };
        if version != IMAGE_FORMAT_VERSION {
            return RecoveryImageHeaderV1Probe::Unsupported(version);
        }
        if get_u16(page, IMAGE_HEADER_SIZE_OFFSET) != Some(IMAGE_HEADER_SIZE)
            || get_u32(page, IMAGE_FLAGS_OFFSET) != Some(0)
            || page[usize::from(IMAGE_HEADER_SIZE)..PAGE_CRC_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
        {
            return RecoveryImageHeaderV1Probe::Corrupt;
        }

        let Some(cache_uuid) = get_id(page, IMAGE_CACHE_UUID_OFFSET) else {
            return RecoveryImageHeaderV1Probe::Corrupt;
        };
        let Some(data_identity) = get_id(page, IMAGE_DATA_IDENTITY_OFFSET) else {
            return RecoveryImageHeaderV1Probe::Corrupt;
        };
        let Some(image_identity) = get_id(page, IMAGE_IDENTITY_OFFSET) else {
            return RecoveryImageHeaderV1Probe::Corrupt;
        };
        let Some(header) = (|| {
            Some(Self {
                cache_uuid,
                data_identity,
                data_superblock_generation: get_u64(page, IMAGE_DATA_GENERATION_OFFSET)?,
                hash_seed: get_u64(page, IMAGE_HASH_SEED_OFFSET)?,
                config_fingerprint: get_u64(page, IMAGE_CONFIG_FINGERPRINT_OFFSET)?,
                image_identity,
                image_generation: get_u64(page, IMAGE_GENERATION_OFFSET)?,
                image_file_len: get_u64(page, IMAGE_FILE_LEN_OFFSET)?,
                index_slots: get_u64(page, IMAGE_INDEX_SLOTS_OFFSET)?,
                index_offset: get_u64(page, IMAGE_INDEX_OFFSET_OFFSET)?,
                index_len: get_u64(page, IMAGE_INDEX_LEN_OFFSET)?,
                region_table_offset: get_u64(page, IMAGE_REGION_TABLE_OFFSET_OFFSET)?,
                region_table_len: get_u64(page, IMAGE_REGION_TABLE_LEN_OFFSET)?,
            })
        })() else {
            return RecoveryImageHeaderV1Probe::Corrupt;
        };
        if !header.is_valid() {
            return RecoveryImageHeaderV1Probe::Corrupt;
        }
        RecoveryImageHeaderV1Probe::Valid(header)
    }

    pub(crate) const fn image_binding(self) -> ImageBindingV2 {
        ImageBindingV2 {
            identity: self.image_identity,
            generation: self.image_generation,
            file_len: self.image_file_len,
        }
    }

    pub(crate) fn matches_data(self, data: DataSuperblockV2) -> bool {
        self.cache_uuid == data.cache_uuid
            && self.data_identity == data.data_identity
            && self.data_superblock_generation == data.generation
            && self.hash_seed == data.hash_seed
            && self.config_fingerprint == data.config_fingerprint
    }

    fn is_valid(self) -> bool {
        let Some(expected_index_len) = recovery_image_index_len_v1(self.index_slots) else {
            return false;
        };
        let Some(index_end) = self.index_offset.checked_add(self.index_len) else {
            return false;
        };
        let expected_file_len = (self.region_table_offset == index_end
            && self.region_table_len % RECOVERY_PAGE_SIZE as u64 == 0
            && self.region_table_len != 0)
            .then(|| self.region_table_offset.checked_add(self.region_table_len))
            .flatten();

        self.data_superblock_generation != 0
            && self.image_generation != 0
            && self.index_slots != 0
            && self.index_offset == RECOVERY_IMAGE_INDEX_OFFSET_V1
            && self.index_len == expected_index_len
            && expected_file_len == Some(self.image_file_len)
    }
}

/// Exact byte length of the self-checking V1 index pages.
pub(crate) fn recovery_image_index_len_v1(index_slots: u64) -> Option<u64> {
    if index_slots == 0 {
        return None;
    }
    let complete_pages = index_slots / RECOVERY_IMAGE_SLOTS_PER_PAGE_V1;
    let partial_page = u64::from(index_slots % RECOVERY_IMAGE_SLOTS_PER_PAGE_V1 != 0);
    complete_pages
        .checked_add(partial_page)?
        .checked_mul(RECOVERY_PAGE_SIZE as u64)
}

/// Values that bind one state record to an exact data file and, for `CLEAN`,
/// an exact recovery image.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateBindingV2 {
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) data_superblock_generation: u64,
    pub(crate) data_file_len: u64,
    pub(crate) hash_seed: u64,
    pub(crate) config_fingerprint: u64,
    pub(crate) image: Option<ImageBindingV2>,
}

impl StateBindingV2 {
    pub(crate) fn from_data(data: DataSuperblockV2, image: Option<ImageBindingV2>) -> Self {
        Self {
            cache_uuid: data.cache_uuid,
            data_identity: data.data_identity,
            data_superblock_generation: data.generation,
            data_file_len: data.geometry.data_file_len,
            hash_seed: data.hash_seed,
            config_fingerprint: data.config_fingerprint,
            image,
        }
    }

    pub(crate) fn matches_data(self, data: DataSuperblockV2) -> bool {
        self.cache_uuid == data.cache_uuid
            && self.data_identity == data.data_identity
            && self.data_superblock_generation == data.generation
            && self.data_file_len == data.geometry.data_file_len
            && self.hash_seed == data.hash_seed
            && self.config_fingerprint == data.config_fingerprint
    }

    fn is_valid(self) -> bool {
        self.data_superblock_generation != 0
            && self.data_file_len >= RECOVERY_PAGE_SIZE as u64
            && self.image.is_none_or(ImageBindingV2::is_valid)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct StateRecordV2 {
    pub(crate) generation: u64,
    pub(crate) state: RecoveryState,
    pub(crate) binding: StateBindingV2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateSlotProbeV2 {
    Empty,
    Valid(StateRecordV2),
    Corrupt,
    Unsupported(u16),
    Unrecognized,
    Truncated,
}

impl StateRecordV2 {
    pub(crate) fn encode(self) -> Result<[u8; RECOVERY_PAGE_SIZE], CodecError> {
        if !self.is_valid() {
            return Err(CodecError::StateRecord);
        }

        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        page[..STATE_MAGIC.len()].copy_from_slice(&STATE_MAGIC);
        put_u16(&mut page, STATE_VERSION_OFFSET, RECOVERY_V2_FORMAT_VERSION);
        put_u16(&mut page, STATE_HEADER_SIZE_OFFSET, STATE_HEADER_SIZE);
        page[STATE_KIND_OFFSET] = self.state as u8;
        page[STATE_FLAGS_OFFSET] = u8::from(self.binding.image.is_some()) * STATE_FLAG_HAS_IMAGE;
        put_u64(&mut page, STATE_GENERATION_OFFSET, self.generation);
        put_id(&mut page, STATE_CACHE_UUID_OFFSET, self.binding.cache_uuid);
        put_id(
            &mut page,
            STATE_DATA_IDENTITY_OFFSET,
            self.binding.data_identity,
        );
        put_u64(
            &mut page,
            STATE_DATA_GENERATION_OFFSET,
            self.binding.data_superblock_generation,
        );
        put_u64(
            &mut page,
            STATE_DATA_FILE_LEN_OFFSET,
            self.binding.data_file_len,
        );
        put_u64(&mut page, STATE_HASH_SEED_OFFSET, self.binding.hash_seed);
        put_u64(
            &mut page,
            STATE_CONFIG_FINGERPRINT_OFFSET,
            self.binding.config_fingerprint,
        );
        if let Some(image) = self.binding.image {
            put_id(&mut page, STATE_IMAGE_IDENTITY_OFFSET, image.identity);
            put_u64(&mut page, STATE_IMAGE_GENERATION_OFFSET, image.generation);
            put_u64(&mut page, STATE_IMAGE_FILE_LEN_OFFSET, image.file_len);
        }
        write_page_crc(&mut page);
        Ok(page)
    }

    pub(crate) fn decode(page: &[u8]) -> Option<Self> {
        match Self::probe(page) {
            StateSlotProbeV2::Valid(record) => Some(record),
            _ => None,
        }
    }

    pub(crate) fn probe(page: &[u8]) -> StateSlotProbeV2 {
        if page.len() != RECOVERY_PAGE_SIZE {
            return StateSlotProbeV2::Truncated;
        }
        if page.iter().all(|byte| *byte == 0) {
            return StateSlotProbeV2::Empty;
        }
        if page[..STATE_MAGIC.len()] != STATE_MAGIC {
            return StateSlotProbeV2::Unrecognized;
        }
        if !page_crc_matches(page) {
            return StateSlotProbeV2::Corrupt;
        }
        let Some(version) = get_u16(page, STATE_VERSION_OFFSET) else {
            return StateSlotProbeV2::Corrupt;
        };
        if version != RECOVERY_V2_FORMAT_VERSION {
            return StateSlotProbeV2::Unsupported(version);
        }
        let flags = page[STATE_FLAGS_OFFSET];
        if get_u16(page, STATE_HEADER_SIZE_OFFSET) != Some(STATE_HEADER_SIZE)
            || flags & !STATE_FLAG_HAS_IMAGE != 0
            || page[14..16].iter().any(|byte| *byte != 0)
            || page[usize::from(STATE_HEADER_SIZE)..PAGE_CRC_OFFSET]
                .iter()
                .any(|byte| *byte != 0)
        {
            return StateSlotProbeV2::Corrupt;
        }

        let Some(state) = RecoveryState::decode(page[STATE_KIND_OFFSET]) else {
            return StateSlotProbeV2::Corrupt;
        };
        let Some(cache_uuid) = get_id(page, STATE_CACHE_UUID_OFFSET) else {
            return StateSlotProbeV2::Corrupt;
        };
        let Some(data_identity) = get_id(page, STATE_DATA_IDENTITY_OFFSET) else {
            return StateSlotProbeV2::Corrupt;
        };
        let has_image = flags & STATE_FLAG_HAS_IMAGE != 0;
        let image = if has_image {
            let Some(identity) = get_id(page, STATE_IMAGE_IDENTITY_OFFSET) else {
                return StateSlotProbeV2::Corrupt;
            };
            Some(ImageBindingV2 {
                identity,
                generation: get_u64(page, STATE_IMAGE_GENERATION_OFFSET).unwrap_or(0),
                file_len: get_u64(page, STATE_IMAGE_FILE_LEN_OFFSET).unwrap_or(0),
            })
        } else {
            if page[STATE_IMAGE_IDENTITY_OFFSET..STATE_HEADER_SIZE as usize]
                .iter()
                .any(|byte| *byte != 0)
            {
                return StateSlotProbeV2::Corrupt;
            }
            None
        };

        let Some(record) = (|| {
            Some(Self {
                generation: get_u64(page, STATE_GENERATION_OFFSET)?,
                state,
                binding: StateBindingV2 {
                    cache_uuid,
                    data_identity,
                    data_superblock_generation: get_u64(page, STATE_DATA_GENERATION_OFFSET)?,
                    data_file_len: get_u64(page, STATE_DATA_FILE_LEN_OFFSET)?,
                    hash_seed: get_u64(page, STATE_HASH_SEED_OFFSET)?,
                    config_fingerprint: get_u64(page, STATE_CONFIG_FINGERPRINT_OFFSET)?,
                    image,
                },
            })
        })() else {
            return StateSlotProbeV2::Corrupt;
        };

        if !record.is_valid() {
            return StateSlotProbeV2::Corrupt;
        }
        StateSlotProbeV2::Valid(record)
    }

    pub(crate) fn matches_clean(self, data: DataSuperblockV2, image: ImageBindingV2) -> bool {
        self.state == RecoveryState::Clean
            && self.binding.matches_data(data)
            && self.binding.image == Some(image)
    }

    fn is_valid(self) -> bool {
        self.generation != 0
            && self.binding.is_valid()
            && (self.state != RecoveryState::Clean || self.binding.image.is_some())
    }
}

/// Validates the state/data/image identity triad and the runtime index layout.
///
/// This proves that a complete-layout immutable image is the one named by
/// `CLEAN`; the Region recovery adapter must additionally validate the
/// contents of its mandatory Region table before restoring any cache state.
pub(crate) fn clean_image_matches_v2(
    state: StateRecordV2,
    data: DataSuperblockV2,
    header: RecoveryImageHeaderV1,
    actual_file_len: u64,
    expected_slots: u64,
    expected_index_len: u64,
) -> bool {
    header.is_valid()
        && recovery_image_index_len_v1(expected_slots) == Some(expected_index_len)
        && state.matches_clean(data, header.image_binding())
        && header.matches_data(data)
        && header.image_file_len == actual_file_len
        && header.index_slots == expected_slots
        && header.index_offset == RECOVERY_IMAGE_INDEX_OFFSET_V1
        && header.index_len == expected_index_len
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectedStateV2 {
    pub(crate) slot: u8,
    pub(crate) record: StateRecordV2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StateSelectionError {
    ConflictingGeneration(u64),
    UnsupportedVersion { slot: u8, version: u16 },
}

/// Selects the checksum-valid state with the greatest generation.
///
/// Corrupt, torn, and blank pages are ignored. A page with the state magic but
/// an unsupported version rejects the selection: falling back to an older
/// `CLEAN` generation would allow a newer implementation's state to be
/// misinterpreted. Two different records with the same greatest generation are
/// likewise rejected instead of choosing an arbitrary recovery authority.
pub(crate) fn latest_state_v2(
    pages: [&[u8]; STATE_SLOT_COUNT],
) -> Result<Option<SelectedStateV2>, StateSelectionError> {
    let mut selected: Option<SelectedStateV2> = None;
    for (slot, page) in pages.into_iter().enumerate() {
        let record = match StateRecordV2::probe(page) {
            StateSlotProbeV2::Valid(record) => record,
            StateSlotProbeV2::Unsupported(version) => {
                return Err(StateSelectionError::UnsupportedVersion {
                    slot: slot as u8,
                    version,
                });
            }
            _ => continue,
        };
        let candidate = SelectedStateV2 {
            slot: slot as u8,
            record,
        };
        match selected {
            None => selected = Some(candidate),
            Some(current) if candidate.record.generation > current.record.generation => {
                selected = Some(candidate);
            }
            Some(current) if candidate.record.generation == current.record.generation => {
                if candidate.record != current.record {
                    return Err(StateSelectionError::ConflictingGeneration(
                        candidate.record.generation,
                    ));
                }
            }
            Some(_) => {}
        }
    }
    Ok(selected)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct StatePageWriteV2 {
    pub(crate) slot: u8,
    pub(crate) record: StateRecordV2,
    pub(crate) page: [u8; RECOVERY_PAGE_SIZE],
}

/// The two writes required to invalidate every old `CLEAN` authority before
/// opening the cache to mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunningBarrierWriteV2 {
    /// Write this page first, without an intervening sync.
    pub(crate) first: StatePageWriteV2,
    /// Write this page second, then `fdatasync` the state file once.
    pub(crate) second: StatePageWriteV2,
}

impl StatePageWriteV2 {
    pub(crate) const fn offset(&self) -> u64 {
        self.slot as u64 * RECOVERY_PAGE_SIZE as u64
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrepareStateError {
    GenerationExhausted,
    InvalidSlot(u8),
    InvalidStateRecord,
}

/// Encodes the next monotonic state into the slot opposite the selected one.
///
/// With a blank state file, generation one is written to slot zero. This is a
/// pure preparation step; publication still requires a full-page positioned
/// write followed by `fdatasync` before the caller acts on the transition.
pub(crate) fn prepare_next_state_v2(
    current: Option<SelectedStateV2>,
    state: RecoveryState,
    binding: StateBindingV2,
) -> Result<StatePageWriteV2, PrepareStateError> {
    let (slot, generation) = match current {
        None => (0, 1),
        Some(current) => {
            if usize::from(current.slot) >= STATE_SLOT_COUNT {
                return Err(PrepareStateError::InvalidSlot(current.slot));
            }
            (
                1 - current.slot,
                current
                    .record
                    .generation
                    .checked_add(1)
                    .ok_or(PrepareStateError::GenerationExhausted)?,
            )
        }
    };
    let record = StateRecordV2 {
        generation,
        state,
        binding,
    };
    let page = record
        .encode()
        .map_err(|_| PrepareStateError::InvalidStateRecord)?;
    Ok(StatePageWriteV2 { slot, record, page })
}

/// Prepares a crash-safe startup `RUNNING` barrier covering both state slots.
///
/// A caller opening from `CLEAN` must not publish only one `RUNNING` page: if
/// that page were later unreadable, latest-valid selection could fall back to
/// the old `CLEAN` record after this session had mutated data. Write `first`,
/// write `second`, and perform one `fdatasync` before admitting any operation.
/// A failed barrier must abort open.
pub(crate) fn prepare_running_barrier_v2(
    current: Option<SelectedStateV2>,
    binding: StateBindingV2,
) -> Result<RunningBarrierWriteV2, PrepareStateError> {
    let first = prepare_next_state_v2(current, RecoveryState::Running, binding)?;
    let first_selected = SelectedStateV2 {
        slot: first.slot,
        record: first.record,
    };
    let second = prepare_next_state_v2(Some(first_selected), RecoveryState::Running, binding)?;
    Ok(RunningBarrierWriteV2 { first, second })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CodecError {
    DataSuperblock,
    RecoveryImageHeader,
    StateRecord,
}

fn put_id(output: &mut [u8], offset: usize, value: PersistentId) {
    output[offset..offset + 16].copy_from_slice(&value.to_bytes());
}

fn get_id(input: &[u8], offset: usize) -> Option<PersistentId> {
    PersistentId::from_bytes(input.get(offset..offset + 16)?.try_into().ok()?)
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

fn get_u16(input: &[u8], offset: usize) -> Option<u16> {
    Some(u16::from_le_bytes(
        input
            .get(offset..offset + size_of::<u16>())?
            .try_into()
            .ok()?,
    ))
}

fn get_u32(input: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        input
            .get(offset..offset + size_of::<u32>())?
            .try_into()
            .ok()?,
    ))
}

fn get_u64(input: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        input
            .get(offset..offset + size_of::<u64>())?
            .try_into()
            .ok()?,
    ))
}

fn write_page_crc(page: &mut [u8; RECOVERY_PAGE_SIZE]) {
    put_u32(page, PAGE_CRC_OFFSET, 0);
    let checksum = crc32c(page);
    put_u32(page, PAGE_CRC_OFFSET, checksum);
}

fn page_crc_matches(page: &[u8]) -> bool {
    if page.len() != RECOVERY_PAGE_SIZE {
        return false;
    }
    let Some(expected) = get_u32(page, PAGE_CRC_OFFSET) else {
        return false;
    };
    let mut checksum = Crc32c::new();
    checksum.update(&page[..PAGE_CRC_OFFSET]);
    checksum.update(&[0; size_of::<u32>()]);
    checksum.finish() == expected
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_INDEX_SLOTS: u64 = RECOVERY_IMAGE_SLOTS_PER_PAGE_V1 * 16;
    const TEST_INDEX_LEN: u64 = RECOVERY_PAGE_SIZE as u64 * 16;
    const TEST_REGION_TABLE_LEN: u64 = RECOVERY_PAGE_SIZE as u64;
    const TEST_IMAGE_FILE_LEN: u64 =
        RECOVERY_IMAGE_INDEX_OFFSET_V1 + TEST_INDEX_LEN + TEST_REGION_TABLE_LEN;

    fn id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn data_superblock() -> DataSuperblockV2 {
        let region_size = 32 * 1024 * 1024;
        let region_count = 128;
        DataSuperblockV2 {
            generation: 7,
            cache_uuid: id(1),
            data_identity: id(2),
            geometry: DataGeometryV2 {
                data_file_len: DataGeometryV2::expected_file_len(region_size, region_count)
                    .unwrap(),
                region_size,
                region_count,
            },
            hash_seed: 0x1234_5678_9abc_def0,
            config_fingerprint: 0x8877_6655_4433_2211,
        }
    }

    fn image() -> ImageBindingV2 {
        ImageBindingV2 {
            identity: id(3),
            generation: 11,
            file_len: TEST_IMAGE_FILE_LEN,
        }
    }

    fn image_header() -> RecoveryImageHeaderV1 {
        let image = image();
        let data = data_superblock();
        RecoveryImageHeaderV1 {
            cache_uuid: data.cache_uuid,
            data_identity: data.data_identity,
            data_superblock_generation: data.generation,
            hash_seed: data.hash_seed,
            config_fingerprint: data.config_fingerprint,
            image_identity: image.identity,
            image_generation: image.generation,
            image_file_len: image.file_len,
            index_slots: TEST_INDEX_SLOTS,
            index_offset: RECOVERY_IMAGE_INDEX_OFFSET_V1,
            index_len: TEST_INDEX_LEN,
            region_table_offset: RECOVERY_IMAGE_INDEX_OFFSET_V1 + TEST_INDEX_LEN,
            region_table_len: TEST_REGION_TABLE_LEN,
        }
    }

    fn record(generation: u64, state: RecoveryState) -> StateRecordV2 {
        StateRecordV2 {
            generation,
            state,
            binding: StateBindingV2::from_data(
                data_superblock(),
                (state == RecoveryState::Clean).then_some(image()),
            ),
        }
    }

    #[test]
    fn data_superblock_round_trips_without_session_state() {
        let expected = data_superblock();
        let encoded = expected.encode().unwrap();

        assert_eq!(DataSuperblockV2::decode(&encoded), Some(expected));
        assert_eq!(
            DataSuperblockV2::probe(&encoded),
            DataSuperblockV2Probe::Valid(expected)
        );
        assert_eq!(&encoded[112..PAGE_CRC_OFFSET], &[0; PAGE_CRC_OFFSET - 112]);
    }

    #[test]
    fn data_superblock_rejects_corruption_torn_pages_and_bad_geometry() {
        let mut corrupt = data_superblock().encode().unwrap();
        corrupt[64] ^= 1;
        assert_eq!(
            DataSuperblockV2::probe(&corrupt),
            DataSuperblockV2Probe::Corrupt
        );
        assert_eq!(
            DataSuperblockV2::probe(&corrupt[..2048]),
            DataSuperblockV2Probe::Truncated
        );

        let mut reserved = data_superblock().encode().unwrap();
        reserved[DATA_RECORD_FORMAT_OFFSET + size_of::<u16>()] = 1;
        write_page_crc(&mut reserved);
        assert!(page_crc_matches(&reserved));
        assert_eq!(
            DataSuperblockV2::probe(&reserved),
            DataSuperblockV2Probe::Corrupt
        );

        let mut bad = data_superblock();
        bad.geometry.data_file_len += 4096;
        assert_eq!(bad.encode(), Err(CodecError::DataSuperblock));

        let mut too_many_regions = data_superblock();
        too_many_regions.geometry.region_count = MAX_PACKED_REGION_COUNT + 1;
        too_many_regions.geometry.data_file_len = DataGeometryV2::expected_file_len(
            too_many_regions.geometry.region_size,
            too_many_regions.geometry.region_count,
        )
        .unwrap();
        assert_eq!(too_many_regions.encode(), Err(CodecError::DataSuperblock));

        let mut oversized_region = data_superblock();
        oversized_region.geometry.region_size = MAX_PACKED_REGION_SIZE + RECOVERY_PAGE_SIZE as u64;
        oversized_region.geometry.data_file_len = DataGeometryV2::expected_file_len(
            oversized_region.geometry.region_size,
            oversized_region.geometry.region_count,
        )
        .unwrap();
        assert_eq!(oversized_region.encode(), Err(CodecError::DataSuperblock));
    }

    #[test]
    fn state_records_round_trip_all_states() {
        for state in [
            RecoveryState::Empty,
            RecoveryState::Running,
            RecoveryState::Clean,
        ] {
            let expected = record(19, state);
            let encoded = expected.encode().unwrap();
            assert_eq!(StateRecordV2::decode(&encoded), Some(expected));
        }
    }

    #[test]
    fn recovery_image_header_round_trips_and_binds_data() {
        let expected = image_header();
        let encoded = expected.encode().unwrap();

        assert_eq!(RecoveryImageHeaderV1::decode(&encoded), Some(expected));
        assert!(expected.matches_data(data_superblock()));
        assert_eq!(expected.image_binding(), image());
        assert_eq!(
            &encoded[usize::from(IMAGE_HEADER_SIZE)..PAGE_CRC_OFFSET],
            &[0; PAGE_CRC_OFFSET - IMAGE_HEADER_SIZE as usize]
        );
    }

    #[test]
    fn recovery_image_header_rejects_corruption_and_non_page_index() {
        let mut corrupt = image_header().encode().unwrap();
        corrupt[IMAGE_INDEX_SLOTS_OFFSET] ^= 1;
        assert_eq!(
            RecoveryImageHeaderV1::probe(&corrupt),
            RecoveryImageHeaderV1Probe::Corrupt
        );

        let mut bad = image_header();
        bad.index_offset += RECOVERY_PAGE_SIZE as u64;
        assert_eq!(bad.encode(), Err(CodecError::RecoveryImageHeader));

        let mut wrong_slot_length = image_header();
        wrong_slot_length.index_len += RECOVERY_PAGE_SIZE as u64;
        wrong_slot_length.region_table_offset += RECOVERY_PAGE_SIZE as u64;
        wrong_slot_length.image_file_len += RECOVERY_PAGE_SIZE as u64;
        assert_eq!(
            wrong_slot_length.encode(),
            Err(CodecError::RecoveryImageHeader)
        );

        let mut trailing_bytes = image_header();
        trailing_bytes.image_file_len += RECOVERY_PAGE_SIZE as u64;
        assert_eq!(
            trailing_bytes.encode(),
            Err(CodecError::RecoveryImageHeader)
        );

        let mut metadata_gap = image_header();
        metadata_gap.region_table_offset += RECOVERY_PAGE_SIZE as u64;
        metadata_gap.image_file_len += RECOVERY_PAGE_SIZE as u64;
        assert_eq!(metadata_gap.encode(), Err(CodecError::RecoveryImageHeader));

        let mut index_only = image_header();
        index_only.region_table_offset = 0;
        index_only.region_table_len = 0;
        index_only.image_file_len = index_only.index_offset + index_only.index_len;
        assert_eq!(index_only.encode(), Err(CodecError::RecoveryImageHeader));

        let mut wrong_data = data_superblock();
        wrong_data.generation += 1;
        assert!(!image_header().matches_data(wrong_data));
    }

    #[test]
    fn recovery_image_length_is_exact_and_checked() {
        assert_eq!(recovery_image_index_len_v1(0), None);
        assert_eq!(recovery_image_index_len_v1(1), Some(4096));
        assert_eq!(recovery_image_index_len_v1(126), Some(4096));
        assert_eq!(recovery_image_index_len_v1(127), Some(8192));
        assert_eq!(recovery_image_index_len_v1(u64::MAX), None);
    }

    #[test]
    fn clean_requires_an_image_binding() {
        let mut invalid = record(1, RecoveryState::Running);
        invalid.state = RecoveryState::Clean;
        assert_eq!(invalid.encode(), Err(CodecError::StateRecord));
    }

    #[test]
    fn latest_valid_ignores_a_torn_or_corrupt_newer_slot() {
        let older = record(8, RecoveryState::Clean).encode().unwrap();
        let mut newer = record(9, RecoveryState::Running).encode().unwrap();
        newer[72] ^= 1;

        let selected = latest_state_v2([&older, &newer]).unwrap().unwrap();
        assert_eq!(selected.slot, 0);
        assert_eq!(selected.record.generation, 8);
        assert_eq!(selected.record.state, RecoveryState::Clean);

        assert_eq!(
            StateRecordV2::probe(&newer[..2048]),
            StateSlotProbeV2::Truncated
        );
    }

    #[test]
    fn latest_valid_uses_monotonic_generation_not_slot_number() {
        let newer = record(42, RecoveryState::Running).encode().unwrap();
        let older = record(41, RecoveryState::Clean).encode().unwrap();

        let selected = latest_state_v2([&newer, &older]).unwrap().unwrap();
        assert_eq!(selected.slot, 0);
        assert_eq!(selected.record.generation, 42);
    }

    #[test]
    fn equal_generation_with_different_records_is_ambiguous() {
        let first = record(5, RecoveryState::Running).encode().unwrap();
        let second = record(5, RecoveryState::Clean).encode().unwrap();

        assert_eq!(
            latest_state_v2([&first, &second]),
            Err(StateSelectionError::ConflictingGeneration(5))
        );
    }

    #[test]
    fn unsupported_state_version_never_falls_back_to_old_clean() {
        let old_clean = record(7, RecoveryState::Clean).encode().unwrap();
        let mut unsupported = record(8, RecoveryState::Running).encode().unwrap();
        put_u16(&mut unsupported, STATE_VERSION_OFFSET, 99);
        write_page_crc(&mut unsupported);

        assert_eq!(
            latest_state_v2([&old_clean, &unsupported]),
            Err(StateSelectionError::UnsupportedVersion {
                slot: 1,
                version: 99,
            })
        );
    }

    #[test]
    fn unsupported_versions_require_an_intact_envelope_checksum() {
        let mut data = data_superblock().encode().unwrap();
        put_u16(&mut data, DATA_VERSION_OFFSET, 99);
        assert_eq!(
            DataSuperblockV2::probe(&data),
            DataSuperblockV2Probe::Corrupt
        );
        write_page_crc(&mut data);
        assert_eq!(
            DataSuperblockV2::probe(&data),
            DataSuperblockV2Probe::Unsupported(99)
        );

        let mut image = image_header().encode().unwrap();
        put_u16(&mut image, IMAGE_VERSION_OFFSET, 99);
        assert_eq!(
            RecoveryImageHeaderV1::probe(&image),
            RecoveryImageHeaderV1Probe::Corrupt
        );
        write_page_crc(&mut image);
        assert_eq!(
            RecoveryImageHeaderV1::probe(&image),
            RecoveryImageHeaderV1Probe::Unsupported(99)
        );

        let mut state = record(8, RecoveryState::Running).encode().unwrap();
        put_u16(&mut state, STATE_VERSION_OFFSET, 99);
        assert_eq!(StateRecordV2::probe(&state), StateSlotProbeV2::Corrupt);
        write_page_crc(&mut state);
        assert_eq!(
            StateRecordV2::probe(&state),
            StateSlotProbeV2::Unsupported(99)
        );
    }

    #[test]
    fn prepare_next_state_alternates_slots_and_increments_generation() {
        let binding = StateBindingV2::from_data(data_superblock(), None);
        let first = prepare_next_state_v2(None, RecoveryState::Running, binding).unwrap();
        assert_eq!(first.slot, 0);
        assert_eq!(first.offset(), 0);
        assert_eq!(first.record.generation, 1);

        let selected = SelectedStateV2 {
            slot: first.slot,
            record: first.record,
        };
        let second = prepare_next_state_v2(Some(selected), RecoveryState::Empty, binding).unwrap();
        assert_eq!(second.slot, 1);
        assert_eq!(second.offset(), RECOVERY_PAGE_SIZE as u64);
        assert_eq!(second.record.generation, 2);
        assert_eq!(StateRecordV2::decode(&second.page), Some(second.record));
    }

    #[test]
    fn prepare_next_state_rejects_an_invalid_selected_slot() {
        let current = SelectedStateV2 {
            slot: 2,
            record: record(10, RecoveryState::Running),
        };
        assert_eq!(
            prepare_next_state_v2(
                Some(current),
                RecoveryState::Running,
                current.record.binding
            ),
            Err(PrepareStateError::InvalidSlot(2))
        );
    }

    #[test]
    fn running_barrier_replaces_both_slots_before_open() {
        let old_clean = record(9, RecoveryState::Clean);
        let old_running = record(8, RecoveryState::Running);
        let current = SelectedStateV2 {
            slot: 0,
            record: old_clean,
        };
        let binding = StateBindingV2::from_data(data_superblock(), Some(image()));
        let barrier = prepare_running_barrier_v2(Some(current), binding).unwrap();

        assert_eq!(barrier.first.slot, 1);
        assert_eq!(barrier.first.record.generation, 10);
        assert_eq!(barrier.second.slot, 0);
        assert_eq!(barrier.second.record.generation, 11);
        assert_eq!(barrier.first.record.state, RecoveryState::Running);
        assert_eq!(barrier.second.record.state, RecoveryState::Running);

        let mut slot0 = barrier.second.page;
        let mut slot1 = barrier.first.page;
        slot0[200] ^= 1;
        let selected = latest_state_v2([&slot0, &slot1]).unwrap().unwrap();
        assert_eq!(selected.record.state, RecoveryState::Running);

        slot0 = barrier.second.page;
        slot1[201] ^= 1;
        let selected = latest_state_v2([&slot0, &slot1]).unwrap().unwrap();
        assert_eq!(selected.record.state, RecoveryState::Running);

        // Before either new page reaches storage, the old CLEAN remains safe:
        // this session has not yet admitted mutations.
        let old_clean_page = old_clean.encode().unwrap();
        let old_running_page = old_running.encode().unwrap();
        assert_eq!(
            latest_state_v2([&old_clean_page, &old_running_page])
                .unwrap()
                .unwrap()
                .record
                .state,
            RecoveryState::Clean
        );
    }

    #[test]
    fn generation_overflow_is_rejected() {
        let selected = SelectedStateV2 {
            slot: 1,
            record: record(u64::MAX, RecoveryState::Running),
        };
        assert_eq!(
            prepare_next_state_v2(
                Some(selected),
                RecoveryState::Running,
                selected.record.binding
            ),
            Err(PrepareStateError::GenerationExhausted)
        );
    }

    #[test]
    fn clean_match_binds_every_identity_and_generation() {
        let data = data_superblock();
        let image = image();
        let clean = record(23, RecoveryState::Clean);
        assert!(clean.matches_clean(data, image));

        let mut wrong_data = data;
        wrong_data.data_identity = id(9);
        assert!(!clean.matches_clean(wrong_data, image));

        let mut wrong_image = image;
        wrong_image.generation += 1;
        assert!(!clean.matches_clean(data, wrong_image));

        let mut wrong_config = data;
        wrong_config.config_fingerprint ^= 1;
        assert!(!clean.matches_clean(wrong_config, image));
    }

    #[test]
    fn clean_image_match_checks_the_complete_identity_triad() {
        let data = data_superblock();
        let clean = record(23, RecoveryState::Clean);
        let header = image_header();
        assert!(clean_image_matches_v2(
            clean,
            data,
            header,
            TEST_IMAGE_FILE_LEN,
            TEST_INDEX_SLOTS,
            TEST_INDEX_LEN
        ));
        assert!(!clean_image_matches_v2(
            clean,
            data,
            header,
            TEST_IMAGE_FILE_LEN + 1,
            TEST_INDEX_SLOTS,
            TEST_INDEX_LEN
        ));
        assert!(!clean_image_matches_v2(
            clean,
            data,
            header,
            TEST_IMAGE_FILE_LEN,
            TEST_INDEX_SLOTS + 1,
            TEST_INDEX_LEN
        ));
        assert!(!clean_image_matches_v2(
            record(24, RecoveryState::Running),
            data,
            header,
            TEST_IMAGE_FILE_LEN,
            TEST_INDEX_SLOTS,
            TEST_INDEX_LEN
        ));
    }

    #[test]
    fn zero_identifiers_are_never_decoded_as_owned_data() {
        assert_eq!(PersistentId::from_bytes([0; 16]), None);

        let mut encoded = data_superblock().encode().unwrap();
        encoded[DATA_CACHE_UUID_OFFSET..DATA_CACHE_UUID_OFFSET + 16].fill(0);
        write_page_crc(&mut encoded);
        assert_eq!(
            DataSuperblockV2::probe(&encoded),
            DataSuperblockV2Probe::Corrupt
        );
    }
}
