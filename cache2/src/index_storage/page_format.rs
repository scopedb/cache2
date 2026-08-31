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

use crate::checksum::Crc32c;

use super::{CorruptPageReason, IndexImageBinding, IndexStorageError};

pub(crate) const INDEX_IMAGE_PAGE_SIZE: usize = 4096;
pub(crate) const INDEX_IMAGE_PAGE_HEADER_SIZE: usize = 64;
pub(crate) const INDEX_IMAGE_SLOT_SIZE: usize = 8;
pub(crate) const INDEX_IMAGE_SLOTS_PER_PAGE: usize =
    (INDEX_IMAGE_PAGE_SIZE - INDEX_IMAGE_PAGE_HEADER_SIZE) / INDEX_IMAGE_SLOT_SIZE;

const PAGE_MAGIC: [u8; 8] = *b"C2SIDX1\0";
const PAGE_FORMAT_VERSION: u16 = 1;

const PAGE_VERSION_OFFSET: usize = 8;
const PAGE_HEADER_SIZE_OFFSET: usize = 10;
const PAGE_SLOT_SIZE_OFFSET: usize = 12;
const PAGE_SLOTS_PER_PAGE_OFFSET: usize = 14;
const PAGE_INDEX_OFFSET: usize = 16;
const PAGE_FIRST_SLOT_OFFSET: usize = 24;
const PAGE_VALID_SLOTS_OFFSET: usize = 32;
const PAGE_FLAGS_OFFSET: usize = 36;
const PAGE_GENERATION_OFFSET: usize = 40;
const PAGE_IMAGE_TAG_OFFSET: usize = 48;
pub(super) const PAGE_CHECKSUM_OFFSET: usize = 56;
const PAGE_TRAILING_RESERVED_OFFSET: usize = 60;

const PAGE_FLAG_NONE: u32 = 0;

const _: () = assert!(INDEX_IMAGE_SLOTS_PER_PAGE == 504);
const _: () = assert!(
    INDEX_IMAGE_PAGE_HEADER_SIZE + INDEX_IMAGE_SLOTS_PER_PAGE * INDEX_IMAGE_SLOT_SIZE
        <= INDEX_IMAGE_PAGE_SIZE
);

pub(super) fn encode_page_header(
    page: &mut [u8; INDEX_IMAGE_PAGE_SIZE],
    page_index: usize,
    first_slot: usize,
    valid_slots: usize,
    binding: IndexImageBinding,
) -> Result<(), IndexStorageError> {
    page[..8].copy_from_slice(&PAGE_MAGIC);
    put_u16(page, PAGE_VERSION_OFFSET, PAGE_FORMAT_VERSION);
    put_u16(
        page,
        PAGE_HEADER_SIZE_OFFSET,
        INDEX_IMAGE_PAGE_HEADER_SIZE as u16,
    );
    put_u16(page, PAGE_SLOT_SIZE_OFFSET, INDEX_IMAGE_SLOT_SIZE as u16);
    put_u16(
        page,
        PAGE_SLOTS_PER_PAGE_OFFSET,
        INDEX_IMAGE_SLOTS_PER_PAGE as u16,
    );
    put_u64(
        page,
        PAGE_INDEX_OFFSET,
        u64::try_from(page_index).map_err(|_| IndexStorageError::SizeOverflow)?,
    );
    put_u64(
        page,
        PAGE_FIRST_SLOT_OFFSET,
        u64::try_from(first_slot).map_err(|_| IndexStorageError::SizeOverflow)?,
    );
    put_u32(
        page,
        PAGE_VALID_SLOTS_OFFSET,
        u32::try_from(valid_slots).map_err(|_| IndexStorageError::SizeOverflow)?,
    );
    put_u32(page, PAGE_FLAGS_OFFSET, PAGE_FLAG_NONE);
    put_u64(page, PAGE_GENERATION_OFFSET, binding.generation);
    put_u64(page, PAGE_IMAGE_TAG_OFFSET, binding.image_tag);
    // The caller starts from a zero-filled page; write the checksum last.
    put_u32(page, PAGE_CHECKSUM_OFFSET, 0);
    Ok(())
}

pub(super) fn validate_page_header(
    page: &[u8; INDEX_IMAGE_PAGE_SIZE],
    expected_page_index: usize,
    expected_first_slot: usize,
    expected_valid_slots: usize,
    expected_binding: IndexImageBinding,
) -> Result<(), CorruptPageReason> {
    if page[..8] != PAGE_MAGIC {
        return Err(CorruptPageReason::InvalidMagic);
    }
    let version = read_u16(page, PAGE_VERSION_OFFSET);
    if version != PAGE_FORMAT_VERSION {
        return Err(CorruptPageReason::UnsupportedVersion { actual: version });
    }
    let header_size = read_u16(page, PAGE_HEADER_SIZE_OFFSET);
    if header_size != INDEX_IMAGE_PAGE_HEADER_SIZE as u16 {
        return Err(CorruptPageReason::InvalidHeaderSize {
            actual: header_size,
        });
    }
    let slot_size = read_u16(page, PAGE_SLOT_SIZE_OFFSET);
    if slot_size != INDEX_IMAGE_SLOT_SIZE as u16 {
        return Err(CorruptPageReason::InvalidSlotSize { actual: slot_size });
    }
    let slots_per_page = read_u16(page, PAGE_SLOTS_PER_PAGE_OFFSET);
    if slots_per_page != INDEX_IMAGE_SLOTS_PER_PAGE as u16 {
        return Err(CorruptPageReason::InvalidSlotsPerPage {
            actual: slots_per_page,
        });
    }

    let expected_page_index =
        u64::try_from(expected_page_index).map_err(|_| CorruptPageReason::WrongPageIndex {
            expected: u64::MAX,
            actual: read_u64(page, PAGE_INDEX_OFFSET),
        })?;
    let actual_page_index = read_u64(page, PAGE_INDEX_OFFSET);
    if actual_page_index != expected_page_index {
        return Err(CorruptPageReason::WrongPageIndex {
            expected: expected_page_index,
            actual: actual_page_index,
        });
    }

    let expected_first_slot =
        u64::try_from(expected_first_slot).map_err(|_| CorruptPageReason::WrongFirstSlot {
            expected: u64::MAX,
            actual: read_u64(page, PAGE_FIRST_SLOT_OFFSET),
        })?;
    let actual_first_slot = read_u64(page, PAGE_FIRST_SLOT_OFFSET);
    if actual_first_slot != expected_first_slot {
        return Err(CorruptPageReason::WrongFirstSlot {
            expected: expected_first_slot,
            actual: actual_first_slot,
        });
    }

    let expected_valid_slots = u32::try_from(expected_valid_slots).map_err(|_| {
        CorruptPageReason::WrongValidSlotCount {
            expected: u32::MAX,
            actual: read_u32(page, PAGE_VALID_SLOTS_OFFSET),
        }
    })?;
    let actual_valid_slots = read_u32(page, PAGE_VALID_SLOTS_OFFSET);
    if actual_valid_slots != expected_valid_slots {
        return Err(CorruptPageReason::WrongValidSlotCount {
            expected: expected_valid_slots,
            actual: actual_valid_slots,
        });
    }

    let flags = read_u32(page, PAGE_FLAGS_OFFSET);
    if flags != PAGE_FLAG_NONE {
        return Err(CorruptPageReason::UnsupportedFlags { actual: flags });
    }
    let actual_generation = read_u64(page, PAGE_GENERATION_OFFSET);
    if actual_generation != expected_binding.generation {
        return Err(CorruptPageReason::WrongGeneration {
            expected: expected_binding.generation,
            actual: actual_generation,
        });
    }
    let actual_image_tag = read_u64(page, PAGE_IMAGE_TAG_OFFSET);
    if actual_image_tag != expected_binding.image_tag {
        return Err(CorruptPageReason::WrongImageTag {
            expected: expected_binding.image_tag,
            actual: actual_image_tag,
        });
    }
    if page[PAGE_TRAILING_RESERVED_OFFSET..INDEX_IMAGE_PAGE_HEADER_SIZE]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(CorruptPageReason::ReservedBytesNonZero);
    }

    let stored = read_u32(page, PAGE_CHECKSUM_OFFSET);
    let computed = page_checksum(page);
    if stored != computed {
        return Err(CorruptPageReason::ChecksumMismatch { stored, computed });
    }
    Ok(())
}

pub(super) fn page_checksum(page: &[u8; INDEX_IMAGE_PAGE_SIZE]) -> u32 {
    let mut checksum = Crc32c::new();
    checksum.update(&page[..PAGE_CHECKSUM_OFFSET]);
    checksum.update(&[0_u8; std::mem::size_of::<u32>()]);
    checksum.update(&page[PAGE_CHECKSUM_OFFSET + std::mem::size_of::<u32>()..]);
    checksum.finish()
}

fn read_u16(input: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        input[offset..offset + 2]
            .try_into()
            .expect("fixed u16 field is in bounds"),
    )
}

fn read_u32(input: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        input[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field is in bounds"),
    )
}

pub(super) fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field is in bounds"),
    )
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}
