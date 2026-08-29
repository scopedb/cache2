// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

//! Stable, mmap-friendly storage for the Region fixed-size index.
//!
//! The runtime index deliberately does not persist a Rust structure. Every
//! slot is encoded field-by-field in little-endian order, and every 4 KiB page
//! carries an independently verifiable header and CRC32C. A recovered image is
//! mapped writable with `MAP_PRIVATE`: reads initially use the clean image and
//! runtime mutations become private copy-on-write pages.

use crate::index::{
    INDEX_CANDIDATES, IndexEntry, MAX_INDEX_PARTITIONS, PackedLocation, PackedLocationError,
    index_partition_for, record_size_class_upper_bound,
};
use std::cell::UnsafeCell;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

mod page_format;

#[cfg(test)]
use self::page_format::put_u64;
pub(crate) use self::page_format::{
    INDEX_IMAGE_PAGE_HEADER_SIZE, INDEX_IMAGE_PAGE_SIZE, INDEX_IMAGE_SLOT_SIZE,
    INDEX_IMAGE_SLOTS_PER_PAGE,
};
use self::page_format::{
    PAGE_CHECKSUM_OFFSET, encode_page_header, page_checksum, put_u32, read_u64,
    validate_page_header,
};

/// Upper bound for one underlying warm-image write.
///
/// Page encoding remains independently checksummed at 4 KiB, but warm close
/// accumulates those pages into a sequential MiB-sized write so a large index
/// does not issue one positioned syscall per page.
pub(crate) const WARM_IMAGE_WRITE_BATCH_BYTES: usize = 1024 * 1024;

const PAGE_STATE_UNCHECKED: u8 = 0;
const PAGE_STATE_VALIDATING: u8 = 1;
const PAGE_STATE_VALID: u8 = 2;
const PAGE_STATE_DIRTY: u8 = 3;
const PAGE_STATE_REJECTED: u8 = 4;
const IMAGE_STATE_USABLE: u8 = 0;
const IMAGE_STATE_REJECTED: u8 = 1;

const _: () = assert!(WARM_IMAGE_WRITE_BATCH_BYTES.is_multiple_of(INDEX_IMAGE_PAGE_SIZE));

/// One canonical, page-aligned partition of Index Image .
///
/// Ordinals are global to the complete image. Every physical 4 KiB page is
/// owned by exactly one range, so a partition lock also owns every slot byte and
/// lazy-validation state it can mutate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexPartitionRange {
    pub(crate) partition_id: usize,
    pub(crate) first_page: usize,
    pub(crate) page_count: usize,
    pub(crate) first_slot: usize,
    pub(crate) slot_count: usize,
}

/// Builds the stable page-balanced partition directory for one slot capacity.
///
/// The partition count is the greatest usable power of two bounded by the physical
/// page count and [`MAX_INDEX_PARTITIONS`]. Extra pages are assigned to the final
/// partitions so the partially filled final image page stays with a larger range.
/// If that range would still contain fewer than four buckets, the partition count is
/// halved until every range is a valid bounded-probe table.
pub(crate) fn canonical_index_partition_ranges(
    slot_count: usize,
) -> Result<Box<[IndexPartitionRange]>, IndexStorageError> {
    if slot_count < INDEX_CANDIDATES {
        return Err(IndexStorageError::InvalidArgument(
            "partitioned index storage requires at least 4 buckets",
        ));
    }
    let layout = ImageLayout::new(slot_count)?;
    let maximum = layout.page_count.min(MAX_INDEX_PARTITIONS);
    let mut partition_count = greatest_power_of_two(maximum);
    while partition_count > 1
        && final_partition_slots(slot_count, layout.page_count, partition_count)? < INDEX_CANDIDATES
    {
        partition_count /= 2;
    }

    let base_pages = layout.page_count / partition_count;
    let extra_pages = layout.page_count % partition_count;
    let first_extra = partition_count - extra_pages;
    let mut ranges = Vec::new();
    ranges.try_reserve_exact(partition_count).map_err(|_| {
        IndexStorageError::Io(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "unable to allocate canonical index partition directory",
        ))
    })?;
    let mut first_page = 0_usize;
    for partition_id in 0..partition_count {
        let page_count = base_pages + usize::from(partition_id >= first_extra);
        let first_slot = first_page
            .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
            .ok_or(IndexStorageError::SizeOverflow)?;
        let range_capacity = page_count
            .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
            .ok_or(IndexStorageError::SizeOverflow)?;
        let slots_remaining = slot_count
            .checked_sub(first_slot)
            .ok_or(IndexStorageError::SizeOverflow)?;
        let range_slot_count = slots_remaining.min(range_capacity);
        if page_count == 0 || range_slot_count < INDEX_CANDIDATES {
            return Err(IndexStorageError::InvalidArgument(
                "canonical index partition contains fewer than 4 buckets",
            ));
        }
        ranges.push(IndexPartitionRange {
            partition_id,
            first_page,
            page_count,
            first_slot,
            slot_count: range_slot_count,
        });
        first_page = first_page
            .checked_add(page_count)
            .ok_or(IndexStorageError::SizeOverflow)?;
    }
    debug_assert_eq!(first_page, layout.page_count);
    debug_assert_eq!(
        ranges
            .last()
            .and_then(|range| range.first_slot.checked_add(range.slot_count)),
        Some(slot_count)
    );
    Ok(ranges.into_boxed_slice())
}

fn greatest_power_of_two(limit: usize) -> usize {
    debug_assert!(limit != 0);
    1_usize << (usize::BITS - 1 - limit.leading_zeros())
}

fn final_partition_slots(
    slot_count: usize,
    page_count: usize,
    partition_count: usize,
) -> Result<usize, IndexStorageError> {
    let base_pages = page_count / partition_count;
    let extra_pages = page_count % partition_count;
    let final_pages = base_pages + usize::from(extra_pages != 0);
    let first_page = page_count
        .checked_sub(final_pages)
        .ok_or(IndexStorageError::SizeOverflow)?;
    let first_slot = first_page
        .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
        .ok_or(IndexStorageError::SizeOverflow)?;
    slot_count
        .checked_sub(first_slot)
        .ok_or(IndexStorageError::SizeOverflow)
}

/// Logical fields in one Index Image bucket.
///
/// This type is intentionally not `repr(C)` and is never copied directly to
/// or from an image. Its stable representation is exactly 8 bytes encoded by
/// [`Self::encode`] and [`Self::decode`]. A zeroed bucket is the empty state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub(crate) struct IndexSlot {
    encoded: u64,
}

const SLOT_REGION_BITS: u32 = 20;
const SLOT_OFFSET_BITS: u32 = 20;
const SLOT_SIZE_CLASS_BITS: u32 = 8;
const SLOT_FINGERPRINT_BITS: u32 = 14;
const SLOT_DISPLACEMENT_BITS: u32 = 2;

const SLOT_REGION_SHIFT: u32 = 0;
const SLOT_OFFSET_SHIFT: u32 = SLOT_REGION_SHIFT + SLOT_REGION_BITS;
const SLOT_SIZE_CLASS_SHIFT: u32 = SLOT_OFFSET_SHIFT + SLOT_OFFSET_BITS;
const SLOT_FINGERPRINT_SHIFT: u32 = SLOT_SIZE_CLASS_SHIFT + SLOT_SIZE_CLASS_BITS;
const SLOT_DISPLACEMENT_SHIFT: u32 = SLOT_FINGERPRINT_SHIFT + SLOT_FINGERPRINT_BITS;

const SLOT_REGION_MASK: u64 = (1_u64 << SLOT_REGION_BITS) - 1;
const SLOT_OFFSET_MASK: u64 = (1_u64 << SLOT_OFFSET_BITS) - 1;
const SLOT_SIZE_CLASS_MASK: u64 = (1_u64 << SLOT_SIZE_CLASS_BITS) - 1;
const SLOT_FINGERPRINT_MASK: u64 = (1_u64 << SLOT_FINGERPRINT_BITS) - 1;
const SLOT_DISPLACEMENT_MASK: u64 = (1_u64 << SLOT_DISPLACEMENT_BITS) - 1;

const _: () = assert!(SLOT_DISPLACEMENT_SHIFT + SLOT_DISPLACEMENT_BITS == u64::BITS);

/// Typed runtime meaning of one canonical Index Image bucket.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexSlotState {
    Empty,
    Value {
        fingerprint: u16,
        displacement: u8,
        entry: IndexEntry,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexSlotSemanticError {
    NonCanonicalMarker,
    InvalidLocation(PackedLocationError),
}

impl fmt::Display for IndexSlotSemanticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonCanonicalMarker => {
                formatter.write_str("bucket is neither Empty nor a valid value")
            }
            Self::InvalidLocation(error) => write!(formatter, "invalid packed location: {error}"),
        }
    }
}

impl IndexSlot {
    pub(crate) const EMPTY: Self = Self { encoded: 0 };

    pub(crate) fn from_state(state: IndexSlotState) -> Self {
        match state {
            IndexSlotState::Empty => Self::EMPTY,
            IndexSlotState::Value {
                fingerprint,
                displacement,
                entry,
            } => {
                let location = entry.location;
                let offset_units = u64::from(location.offset() / crate::format::RECORD_ALIGNMENT);
                Self {
                    encoded: u64::from(location.region_id())
                        | (offset_units << SLOT_OFFSET_SHIFT)
                        | (u64::from(location.index_size_class()) << SLOT_SIZE_CLASS_SHIFT)
                        | ((u64::from(fingerprint) & SLOT_FINGERPRINT_MASK)
                            << SLOT_FINGERPRINT_SHIFT)
                        | ((u64::from(displacement) & SLOT_DISPLACEMENT_MASK)
                            << SLOT_DISPLACEMENT_SHIFT),
                }
            }
        }
    }

    pub(crate) fn encode(self, output: &mut [u8; INDEX_IMAGE_SLOT_SIZE]) {
        output.copy_from_slice(&self.encoded.to_le_bytes());
    }

    pub(crate) fn decode(input: &[u8; INDEX_IMAGE_SLOT_SIZE]) -> Self {
        Self {
            encoded: read_u64(input, 0),
        }
    }

    pub(crate) fn runtime_state(self) -> Result<IndexSlotState, IndexSlotSemanticError> {
        if self.encoded == 0 {
            return Ok(IndexSlotState::Empty);
        }
        let size_class = ((self.encoded >> SLOT_SIZE_CLASS_SHIFT) & SLOT_SIZE_CLASS_MASK) as u8;
        let record_len = record_size_class_upper_bound(size_class)
            .ok_or(IndexSlotSemanticError::NonCanonicalMarker)?;
        let region_id = ((self.encoded >> SLOT_REGION_SHIFT) & SLOT_REGION_MASK) as u32;
        let offset_units = ((self.encoded >> SLOT_OFFSET_SHIFT) & SLOT_OFFSET_MASK) as u32;
        let offset = offset_units * crate::format::RECORD_ALIGNMENT;
        let location = PackedLocation::new(region_id, offset, record_len)
            .map_err(IndexSlotSemanticError::InvalidLocation)?;
        Ok(IndexSlotState::Value {
            fingerprint: ((self.encoded >> SLOT_FINGERPRINT_SHIFT) & SLOT_FINGERPRINT_MASK) as u16,
            displacement: ((self.encoded >> SLOT_DISPLACEMENT_SHIFT) & SLOT_DISPLACEMENT_MASK)
                as u8,
            entry: IndexEntry { location },
        })
    }

    fn physical_kind(self) -> SlotPhysicalKind {
        if self.encoded == 0 {
            SlotPhysicalKind::Empty
        } else {
            SlotPhysicalKind::Value
        }
    }
}

/// Identity carried by every self-checking index page in one recovery image.
///
/// `image_tag` is the stable non-zero 64-bit tag derived by the recovery-image
/// owner. Together with a non-reused generation it prevents a page from an
/// older image being accepted merely because its physical position matches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexImageBinding {
    pub(crate) generation: u64,
    pub(crate) image_tag: u64,
}

impl IndexImageBinding {
    const fn is_valid(self) -> bool {
        self.generation != 0 && self.image_tag != 0
    }
}

/// Counts carried by clean metadata. Deleted buckets are forbidden by v1.
///
/// Clean recovery metadata validates these counts before passing them to
/// [`IndexStorage::map_private`], avoiding an O(slot-count) startup scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IndexPhysicalStats {
    pub(crate) value: u64,
    pub(crate) deleted: u64,
}

impl IndexPhysicalStats {
    fn total(self) -> Option<u64> {
        self.value.checked_add(self.deleted)
    }

    fn is_valid_for(self, slot_count: usize) -> bool {
        let Ok(slot_count) = u64::try_from(slot_count) else {
            return false;
        };
        self.deleted == 0 && self.total().is_some_and(|total| total <= slot_count)
    }

    fn transitioned(
        self,
        old: SlotPhysicalKind,
        new: SlotPhysicalKind,
        slot_count: usize,
    ) -> Option<Self> {
        let mut next = self;
        next.decrement(old)?;
        next.increment(new)?;
        next.is_valid_for(slot_count).then_some(next)
    }

    fn decrement(&mut self, kind: SlotPhysicalKind) -> Option<()> {
        let counter = match kind {
            SlotPhysicalKind::Empty => return Some(()),
            SlotPhysicalKind::Value => &mut self.value,
        };
        *counter = counter.checked_sub(1)?;
        Some(())
    }

    fn increment(&mut self, kind: SlotPhysicalKind) -> Option<()> {
        let counter = match kind {
            SlotPhysicalKind::Empty => return Some(()),
            SlotPhysicalKind::Value => &mut self.value,
        };
        *counter = counter.checked_add(1)?;
        Some(())
    }
}

#[derive(Clone, Copy)]
enum SlotPhysicalKind {
    Empty,
    Value,
}

/// The lazy validation state of one physical image page.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageValidationState {
    Unchecked,
    Validating,
    Valid,
    Dirty,
    Rejected,
}

/// Why a recovered Index Image page was rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CorruptPageReason {
    InvalidMagic,
    UnsupportedVersion { actual: u16 },
    InvalidHeaderSize { actual: u16 },
    InvalidSlotSize { actual: u16 },
    InvalidSlotsPerPage { actual: u16 },
    WrongPageIndex { expected: u64, actual: u64 },
    WrongFirstSlot { expected: u64, actual: u64 },
    WrongValidSlotCount { expected: u32, actual: u32 },
    UnsupportedFlags { actual: u32 },
    WrongGeneration { expected: u64, actual: u64 },
    WrongImageTag { expected: u64, actual: u64 },
    ReservedBytesNonZero,
    ChecksumMismatch { stored: u32, computed: u32 },
    PreviouslyRejected,
}

impl fmt::Display for CorruptPageReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::InvalidMagic => formatter.write_str("invalid page magic"),
            Self::UnsupportedVersion { actual } => {
                write!(formatter, "unsupported page format version {actual}")
            }
            Self::InvalidHeaderSize { actual } => {
                write!(formatter, "invalid page header size {actual}")
            }
            Self::InvalidSlotSize { actual } => {
                write!(formatter, "invalid index slot size {actual}")
            }
            Self::InvalidSlotsPerPage { actual } => {
                write!(formatter, "invalid slots-per-page value {actual}")
            }
            Self::WrongPageIndex { expected, actual } => write!(
                formatter,
                "page index mismatch: expected {expected}, found {actual}"
            ),
            Self::WrongFirstSlot { expected, actual } => write!(
                formatter,
                "first-slot mismatch: expected {expected}, found {actual}"
            ),
            Self::WrongValidSlotCount { expected, actual } => write!(
                formatter,
                "valid-slot count mismatch: expected {expected}, found {actual}"
            ),
            Self::UnsupportedFlags { actual } => {
                write!(formatter, "unsupported page flags {actual:#x}")
            }
            Self::WrongGeneration { expected, actual } => write!(
                formatter,
                "page generation mismatch: expected {expected}, found {actual}"
            ),
            Self::WrongImageTag { expected, actual } => write!(
                formatter,
                "page image tag mismatch: expected {expected:#018x}, found {actual:#018x}"
            ),
            Self::ReservedBytesNonZero => formatter.write_str("reserved page bytes are non-zero"),
            Self::ChecksumMismatch { stored, computed } => write!(
                formatter,
                "page checksum mismatch: stored {stored:#010x}, computed {computed:#010x}"
            ),
            Self::PreviouslyRejected => {
                formatter.write_str("page was rejected on an earlier touch")
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum IndexStorageError {
    Io(io::Error),
    InvalidArgument(&'static str),
    InvalidSlot(IndexSlotSemanticError),
    SizeOverflow,
    SlotOutOfBounds {
        slot: usize,
        slot_count: usize,
    },
    PageOutOfBounds {
        page: usize,
        page_count: usize,
    },
    PageBusy {
        page_index: usize,
    },
    PartitionBusy {
        partition_id: usize,
    },
    PartitionPoisoned {
        partition_id: usize,
    },
    CorruptPage {
        page_index: usize,
        reason: CorruptPageReason,
    },
    CorruptSlot {
        slot_index: usize,
        reason: IndexSlotSemanticError,
    },
    InvalidPhysicalStats,
}

impl fmt::Display for IndexStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "index storage I/O failed: {error}"),
            Self::InvalidArgument(message) => formatter.write_str(message),
            Self::InvalidSlot(reason) => write!(formatter, "invalid index slot: {reason}"),
            Self::SizeOverflow => {
                formatter.write_str("index image size overflows the address space")
            }
            Self::SlotOutOfBounds { slot, slot_count } => {
                write!(formatter, "index slot {slot} is outside {slot_count} slots")
            }
            Self::PageOutOfBounds { page, page_count } => {
                write!(formatter, "index page {page} is outside {page_count} pages")
            }
            Self::PageBusy { page_index } => {
                write!(formatter, "index page {page_index} is being validated")
            }
            Self::PartitionBusy { partition_id } => {
                write!(formatter, "index partition {partition_id} is being mutated")
            }
            Self::PartitionPoisoned { partition_id } => {
                write!(formatter, "index partition {partition_id} is poisoned")
            }
            Self::CorruptPage { page_index, reason } => {
                write!(formatter, "index page {page_index} is corrupt: {reason}")
            }
            Self::CorruptSlot { slot_index, reason } => {
                write!(formatter, "index slot {slot_index} is corrupt: {reason}")
            }
            Self::InvalidPhysicalStats => {
                formatter.write_str("index physical slot counts are inconsistent")
            }
        }
    }
}

impl std::error::Error for IndexStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidArgument(_)
            | Self::InvalidSlot(_)
            | Self::SizeOverflow
            | Self::SlotOutOfBounds { .. }
            | Self::PageOutOfBounds { .. }
            | Self::PageBusy { .. }
            | Self::PartitionBusy { .. }
            | Self::PartitionPoisoned { .. }
            | Self::CorruptPage { .. }
            | Self::CorruptSlot { .. }
            | Self::InvalidPhysicalStats => None,
        }
    }
}

impl From<io::Error> for IndexStorageError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct WarmImageStats {
    pub(crate) pages_written: usize,
    pub(crate) slots_written: usize,
    pub(crate) bytes_written: u64,
    pub(crate) physical_stats: IndexPhysicalStats,
}

/// Fixed-capacity bytes backing the Region index.
///
/// Anonymous storage uses a zero-filled private mapping and treats its pages
/// as valid without materializing page headers. File-backed storage validates
/// each source page only when a slot on that page is first read or mutated.
/// Callers must freeze mutations while [`Self::write_warm_image`] runs.
pub(crate) struct IndexStorage {
    core: Arc<IndexStorageCore>,
    range: IndexPartitionRange,
    physical_stats: IndexPhysicalStats,
}

impl IndexStorage {
    /// Creates a lazily allocated, zero-filled runtime image.
    pub(crate) fn anonymous(slot_count: usize) -> Result<Self, IndexStorageError> {
        let layout = ImageLayout::new(slot_count)?;
        let mut backing = Backing::anonymous(layout.image_len)?;
        let data_pointer = backing.as_mut_ptr();
        let page_states = allocate_page_states(layout.page_count, PAGE_STATE_VALID)?;
        let core = Arc::new(IndexStorageCore {
            _backing: UnsafeCell::new(backing),
            data_pointer,
            data_offset: 0,
            slot_count,
            page_count: layout.page_count,
            expected_binding: None,
            image_state: AtomicU8::new(IMAGE_STATE_USABLE),
            page_states,
        });
        Ok(Self {
            core,
            range: whole_image_range(slot_count, layout.page_count),
            physical_stats: IndexPhysicalStats::default(),
        })
    }

    /// Maps an Index Image range writable and private.
    ///
    /// Opening checks only range bounds and establishes the mapping. It does
    /// not scan page headers, slots, or CRCs. `file_offset` must be 4 KiB image
    /// aligned, but need not match the host's mmap page size because the
    /// mapping starts at file offset zero and addresses the requested subrange.
    pub(crate) fn map_private(
        file: &File,
        file_offset: u64,
        slot_count: usize,
        expected_binding: IndexImageBinding,
        physical_stats: IndexPhysicalStats,
    ) -> Result<Self, IndexStorageError> {
        if !expected_binding.is_valid() {
            return Err(IndexStorageError::InvalidArgument(
                "mapped index image binding must be non-zero",
            ));
        }
        if !physical_stats.is_valid_for(slot_count) {
            return Err(IndexStorageError::InvalidPhysicalStats);
        }
        if !file_offset.is_multiple_of(INDEX_IMAGE_PAGE_SIZE as u64) {
            return Err(IndexStorageError::InvalidArgument(
                "index image file offset must be 4 KiB aligned",
            ));
        }
        let layout = ImageLayout::new(slot_count)?;
        let data_offset =
            usize::try_from(file_offset).map_err(|_| IndexStorageError::SizeOverflow)?;
        let mapping_len = data_offset
            .checked_add(layout.image_len)
            .ok_or(IndexStorageError::SizeOverflow)?;
        if mapping_len > isize::MAX as usize {
            return Err(IndexStorageError::SizeOverflow);
        }
        let required_len =
            u64::try_from(mapping_len).map_err(|_| IndexStorageError::SizeOverflow)?;
        if file.metadata()?.len() < required_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "index image range extends past the end of the file",
            )
            .into());
        }

        let mut backing = Backing::map_file_private(file, mapping_len)?;
        let data_pointer = backing.as_mut_ptr();
        let page_states = allocate_page_states(layout.page_count, PAGE_STATE_UNCHECKED)?;
        let core = Arc::new(IndexStorageCore {
            _backing: UnsafeCell::new(backing),
            data_pointer,
            data_offset,
            slot_count,
            page_count: layout.page_count,
            expected_binding: Some(expected_binding),
            image_state: AtomicU8::new(IMAGE_STATE_USABLE),
            page_states,
        });
        Ok(Self {
            core,
            range: whole_image_range(slot_count, layout.page_count),
            physical_stats,
        })
    }

    pub(crate) const fn physical_stats(&self) -> IndexPhysicalStats {
        self.physical_stats
    }

    #[cfg(test)]
    pub(crate) fn page_validation_state(
        &self,
        page: usize,
    ) -> Result<PageValidationState, IndexStorageError> {
        self.core.page_validation_state(self.global_page(page)?)
    }

    #[cfg(test)]
    pub(crate) fn read_slot(&self, slot: usize) -> Result<IndexSlot, IndexStorageError> {
        self.core.read_slot(self.global_slot(slot)?)
    }

    fn state_at(&self, slot: usize) -> Result<IndexSlotState, IndexStorageError> {
        let global_slot = self.global_slot(slot)?;
        let (page, offset) = self.core.slot_address(global_slot)?;
        self.core.ensure_page_valid(page)?;
        let state = self
            .core
            .decode_at(offset)
            .runtime_state()
            .map_err(|reason| {
                self.core.reject_image();
                IndexStorageError::CorruptSlot {
                    slot_index: global_slot,
                    reason,
                }
            })?;
        self.core.ensure_image_usable(page)?;
        Ok(state)
    }

    fn replace_observed_state(
        &mut self,
        slot: usize,
        previous: IndexSlotState,
        state: IndexSlotState,
    ) -> Result<(), IndexStorageError> {
        // The exclusive partition guard keeps the state observed by the probe
        // stable, so updating its physical count needs no second slot decode.
        let global_slot = self.global_slot(slot)?;
        let previous = IndexSlot::from_state(previous);
        let value = IndexSlot::from_state(state);
        let next_stats = self
            .physical_stats
            .transitioned(
                previous.physical_kind(),
                value.physical_kind(),
                self.range.slot_count,
            )
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        self.core.write_slot(global_slot, value)?;
        self.physical_stats = next_stats;
        Ok(())
    }

    /// Encodes one range-local slot.
    ///
    /// Exclusive access owns this complete page-aligned range. Canonical range
    /// construction guarantees that another view of the same core cannot
    /// address these slot bytes.
    #[cfg(test)]
    pub(crate) fn write_slot(
        &mut self,
        slot: usize,
        value: IndexSlot,
    ) -> Result<(), IndexStorageError> {
        let global_slot = self.global_slot(slot)?;
        value
            .runtime_state()
            .map_err(IndexStorageError::InvalidSlot)?;
        let old = self.core.read_slot(global_slot)?;
        let next_stats = self
            .physical_stats
            .transitioned(
                old.physical_kind(),
                value.physical_kind(),
                self.range.slot_count,
            )
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        self.core.write_slot(global_slot, value)?;
        self.physical_stats = next_stats;
        Ok(())
    }

    /// Sequentially emits a new, fully checksummed Index Image .
    ///
    /// Encoding uses one 4 KiB stack page and one lazily allocated, fixed
    /// 1 MiB write batch regardless of index size. The destination should be
    /// an unpublished temporary image because an error can leave a prefix written.
    #[cfg(test)]
    pub(crate) fn write_warm_image<W>(
        &self,
        writer: &mut W,
        binding: IndexImageBinding,
    ) -> Result<WarmImageStats, IndexStorageError>
    where
        W: Write + ?Sized,
    {
        let mut batch_writer = WarmImageBatchWriter::new(writer);
        let stats = self.write_warm_image_pages(&mut batch_writer, binding)?;
        batch_writer.finish()?;
        Ok(stats)
    }

    /// Emits checksummed pages into a caller-owned batching boundary.
    ///
    /// `PartitionedIndexStorage` uses this directly so one MiB batch can span
    /// canonical partition boundaries while all partition read guards stay frozen.
    fn write_warm_image_pages<W>(
        &self,
        writer: &mut W,
        binding: IndexImageBinding,
    ) -> Result<WarmImageStats, IndexStorageError>
    where
        W: Write + ?Sized,
    {
        if !binding.is_valid() {
            return Err(IndexStorageError::InvalidArgument(
                "warm index image binding must be non-zero",
            ));
        }
        self.core.ensure_image_usable(self.range.first_page)?;
        let mut output = [0_u8; INDEX_IMAGE_PAGE_SIZE];
        let mut emitted_physical_stats = IndexPhysicalStats::default();
        for local_page in 0..self.range.page_count {
            let page = self
                .range
                .first_page
                .checked_add(local_page)
                .ok_or(IndexStorageError::SizeOverflow)?;
            self.core.ensure_page_valid(page)?;
            output.fill(0);
            let first_slot = page
                .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
                .ok_or(IndexStorageError::SizeOverflow)?;
            let valid_slots = self.core.valid_slots_in_page(page);
            encode_page_header(&mut output, page, first_slot, valid_slots, binding)?;

            for slot_in_page in 0..valid_slots {
                let source_offset = page
                    .checked_mul(INDEX_IMAGE_PAGE_SIZE)
                    .and_then(|base| base.checked_add(INDEX_IMAGE_PAGE_HEADER_SIZE))
                    .and_then(|base| {
                        base.checked_add(slot_in_page.checked_mul(INDEX_IMAGE_SLOT_SIZE)?)
                    })
                    .ok_or(IndexStorageError::SizeOverflow)?;
                let value = self.core.decode_at(source_offset);
                value
                    .runtime_state()
                    .map_err(IndexStorageError::InvalidSlot)?;
                emitted_physical_stats = emitted_physical_stats
                    .transitioned(
                        SlotPhysicalKind::Empty,
                        value.physical_kind(),
                        self.range.slot_count,
                    )
                    .ok_or(IndexStorageError::InvalidPhysicalStats)?;
                let target_offset =
                    INDEX_IMAGE_PAGE_HEADER_SIZE + slot_in_page * INDEX_IMAGE_SLOT_SIZE;
                let target: &mut [u8; INDEX_IMAGE_SLOT_SIZE] = (&mut output
                    [target_offset..target_offset + INDEX_IMAGE_SLOT_SIZE])
                    .try_into()
                    .expect("fixed index slot range has the encoded slot size");
                value.encode(target);
            }

            let checksum = page_checksum(&output);
            put_u32(&mut output, PAGE_CHECKSUM_OFFSET, checksum);
            writer.write_all(&output)?;
        }

        if emitted_physical_stats != self.physical_stats {
            return Err(IndexStorageError::InvalidPhysicalStats);
        }

        let bytes_written = self
            .range
            .page_count
            .checked_mul(INDEX_IMAGE_PAGE_SIZE)
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(IndexStorageError::SizeOverflow)?;
        Ok(WarmImageStats {
            pages_written: self.range.page_count,
            slots_written: self.range.slot_count,
            bytes_written,
            physical_stats: emitted_physical_stats,
        })
    }

    fn global_slot(&self, slot: usize) -> Result<usize, IndexStorageError> {
        if slot >= self.range.slot_count {
            return Err(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.range.slot_count,
            });
        }
        self.range
            .first_slot
            .checked_add(slot)
            .ok_or(IndexStorageError::SizeOverflow)
    }

    #[cfg(test)]
    fn global_page(&self, page: usize) -> Result<usize, IndexStorageError> {
        if page >= self.range.page_count {
            return Err(IndexStorageError::PageOutOfBounds {
                page,
                page_count: self.range.page_count,
            });
        }
        self.range
            .first_page
            .checked_add(page)
            .ok_or(IndexStorageError::SizeOverflow)
    }
}

struct IndexStorageCore {
    _backing: UnsafeCell<Backing>,
    data_pointer: *mut u8,
    data_offset: usize,
    slot_count: usize,
    page_count: usize,
    expected_binding: Option<IndexImageBinding>,
    image_state: AtomicU8,
    page_states: Box<[AtomicU8]>,
}

impl IndexStorageCore {
    #[cfg(test)]
    fn page_validation_state(&self, page: usize) -> Result<PageValidationState, IndexStorageError> {
        let state = self
            .page_states
            .get(page)
            .ok_or(IndexStorageError::PageOutOfBounds {
                page,
                page_count: self.page_count,
            })?;
        if self.image_is_rejected() {
            return Ok(PageValidationState::Rejected);
        }
        Ok(match state.load(Ordering::Acquire) {
            PAGE_STATE_UNCHECKED => PageValidationState::Unchecked,
            PAGE_STATE_VALIDATING => PageValidationState::Validating,
            PAGE_STATE_VALID => PageValidationState::Valid,
            PAGE_STATE_DIRTY => PageValidationState::Dirty,
            PAGE_STATE_REJECTED => PageValidationState::Rejected,
            _ => PageValidationState::Rejected,
        })
    }

    #[cfg(test)]
    fn read_slot(&self, slot: usize) -> Result<IndexSlot, IndexStorageError> {
        let (page, offset) = self.slot_address(slot)?;
        self.ensure_page_valid(page)?;
        let value = self.decode_at(offset);
        self.ensure_image_usable(page)?;
        Ok(value)
    }

    fn write_slot(&self, slot: usize, value: IndexSlot) -> Result<(), IndexStorageError> {
        let (page, offset) = self.slot_address(slot)?;
        self.ensure_page_valid(page)?;
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        value.encode(&mut encoded);
        // SAFETY: callers can reach this method only through one non-Clone
        // `IndexStorage` range while holding its exclusive partition lock or `&mut`
        // owner. Canonical ranges never overlap, and `offset` addresses one
        // complete slot inside that range.
        unsafe {
            ptr::copy_nonoverlapping(
                encoded.as_ptr(),
                self.data_mut_ptr().add(offset),
                INDEX_IMAGE_SLOT_SIZE,
            );
        }
        self.page_states[page].store(PAGE_STATE_DIRTY, Ordering::Release);
        self.ensure_image_usable(page)
    }

    fn slot_address(&self, slot: usize) -> Result<(usize, usize), IndexStorageError> {
        if slot >= self.slot_count {
            return Err(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.slot_count,
            });
        }
        let page = slot / INDEX_IMAGE_SLOTS_PER_PAGE;
        let slot_in_page = slot % INDEX_IMAGE_SLOTS_PER_PAGE;
        let offset = page
            .checked_mul(INDEX_IMAGE_PAGE_SIZE)
            .and_then(|base| base.checked_add(INDEX_IMAGE_PAGE_HEADER_SIZE))
            .and_then(|base| base.checked_add(slot_in_page.checked_mul(INDEX_IMAGE_SLOT_SIZE)?))
            .ok_or(IndexStorageError::SizeOverflow)?;
        Ok((page, offset))
    }

    fn ensure_page_valid(&self, page: usize) -> Result<(), IndexStorageError> {
        let Some(state) = self.page_states.get(page) else {
            return Err(IndexStorageError::PageOutOfBounds {
                page,
                page_count: self.page_count,
            });
        };
        loop {
            self.ensure_image_usable(page)?;
            match state.load(Ordering::Acquire) {
                PAGE_STATE_VALID | PAGE_STATE_DIRTY => return Ok(()),
                PAGE_STATE_REJECTED => {
                    return Err(IndexStorageError::CorruptPage {
                        page_index: page,
                        reason: CorruptPageReason::PreviouslyRejected,
                    });
                }
                PAGE_STATE_UNCHECKED => {
                    if state
                        .compare_exchange(
                            PAGE_STATE_UNCHECKED,
                            PAGE_STATE_VALIDATING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    let result = self.validate_mapped_page(page);
                    match result {
                        Ok(()) => {
                            state.store(PAGE_STATE_VALID, Ordering::Release);
                            self.ensure_image_usable(page)?;
                            return Ok(());
                        }
                        Err(error) => {
                            self.reject_image();
                            state.store(PAGE_STATE_REJECTED, Ordering::Release);
                            return Err(error);
                        }
                    }
                }
                PAGE_STATE_VALIDATING => {
                    return Err(IndexStorageError::PageBusy { page_index: page });
                }
                _ => {
                    self.reject_image();
                    state.store(PAGE_STATE_REJECTED, Ordering::Release);
                    return Err(IndexStorageError::CorruptPage {
                        page_index: page,
                        reason: CorruptPageReason::PreviouslyRejected,
                    });
                }
            }
        }
    }

    fn validate_mapped_page(&self, page_index: usize) -> Result<(), IndexStorageError> {
        let expected_binding = self
            .expected_binding
            .ok_or(IndexStorageError::InvalidArgument(
                "anonymous index page unexpectedly requires validation",
            ))?;
        let offset = page_index
            .checked_mul(INDEX_IMAGE_PAGE_SIZE)
            .ok_or(IndexStorageError::SizeOverflow)?;
        // SAFETY: `offset` and the fixed page length are inside `image_len` by
        // construction, and the mapping remains alive for this borrow.
        let page = unsafe {
            std::slice::from_raw_parts(self.data_ptr().add(offset), INDEX_IMAGE_PAGE_SIZE)
        };
        let page: &[u8; INDEX_IMAGE_PAGE_SIZE] = page
            .try_into()
            .expect("fixed mapped page has the Index Image page size");
        let expected_first_slot = page_index
            .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
            .ok_or(IndexStorageError::SizeOverflow)?;
        let expected_valid_slots = self.valid_slots_in_page(page_index);
        validate_page_header(
            page,
            page_index,
            expected_first_slot,
            expected_valid_slots,
            expected_binding,
        )
        .map_err(|reason| IndexStorageError::CorruptPage { page_index, reason })?;

        for slot_in_page in 0..expected_valid_slots {
            let slot_index = expected_first_slot
                .checked_add(slot_in_page)
                .ok_or(IndexStorageError::SizeOverflow)?;
            let offset = INDEX_IMAGE_PAGE_HEADER_SIZE
                .checked_add(
                    slot_in_page
                        .checked_mul(INDEX_IMAGE_SLOT_SIZE)
                        .ok_or(IndexStorageError::SizeOverflow)?,
                )
                .ok_or(IndexStorageError::SizeOverflow)?;
            let encoded: &[u8; INDEX_IMAGE_SLOT_SIZE] = page
                [offset..offset + INDEX_IMAGE_SLOT_SIZE]
                .try_into()
                .expect("validated index slot range has the fixed slot size");
            IndexSlot::decode(encoded)
                .runtime_state()
                .map_err(|reason| IndexStorageError::CorruptSlot { slot_index, reason })?;
        }
        Ok(())
    }

    fn image_is_rejected(&self) -> bool {
        self.image_state.load(Ordering::Acquire) == IMAGE_STATE_REJECTED
    }

    fn ensure_image_usable(&self, page_index: usize) -> Result<(), IndexStorageError> {
        if self.image_is_rejected() {
            return Err(IndexStorageError::CorruptPage {
                page_index,
                reason: CorruptPageReason::PreviouslyRejected,
            });
        }
        Ok(())
    }

    fn reject_image(&self) {
        self.image_state
            .store(IMAGE_STATE_REJECTED, Ordering::Release);
    }

    fn valid_slots_in_page(&self, page: usize) -> usize {
        let first = page * INDEX_IMAGE_SLOTS_PER_PAGE;
        self.slot_count
            .saturating_sub(first)
            .min(INDEX_IMAGE_SLOTS_PER_PAGE)
    }

    fn decode_at(&self, offset: usize) -> IndexSlot {
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        // SAFETY: every caller supplies an offset computed for a complete slot
        // within the owned image mapping.
        unsafe {
            ptr::copy_nonoverlapping(
                self.data_ptr().add(offset),
                encoded.as_mut_ptr(),
                INDEX_IMAGE_SLOT_SIZE,
            );
        }
        IndexSlot::decode(&encoded)
    }

    fn data_ptr(&self) -> *const u8 {
        // SAFETY: `data_pointer` addresses the stable allocation owned by
        // `_backing`, and `data_offset + image_len` was checked against that
        // allocation during construction.
        unsafe { self.data_pointer.cast_const().add(self.data_offset) }
    }

    fn data_mut_ptr(&self) -> *mut u8 {
        // SAFETY: callers provide exclusive access to a non-overlapping partition
        // range. Using the cached raw allocation pointer avoids manufacturing
        // overlapping `&mut Backing` references for concurrent ranges.
        unsafe { self.data_pointer.add(self.data_offset) }
    }
}

// `IndexStorageCore` owns one stable mapping shared by non-overlapping canonical
// ranges. Shared operations only validate/read; byte writes require the owning
// range's exclusive lock or `&mut IndexStorage`.
unsafe impl Send for IndexStorageCore {}
unsafe impl Sync for IndexStorageCore {}

fn whole_image_range(slot_count: usize, page_count: usize) -> IndexPartitionRange {
    IndexPartitionRange {
        partition_id: 0,
        first_page: 0,
        page_count,
        first_slot: 0,
        slot_count,
    }
}

/// Canonically sharded views over one anonymous or MAP_PRIVATE index image.
///
/// Every view owns its physical counters and is protected by one range lock;
/// all views share a single backing mapping, page-validation table, and sticky
/// image-health state. Mapping and warm-image work are therefore O(partitions), not
/// O(slots), until a slot page is actually touched.
pub(crate) struct PartitionedIndexStorage {
    slot_count: usize,
    ranges: Box<[IndexPartitionRange]>,
    partitions: Box<[RwLock<IndexStorage>]>,
}

pub(crate) struct IndexPartitionReadGuard<'a> {
    range: IndexPartitionRange,
    guard: RwLockReadGuard<'a, IndexStorage>,
}

impl IndexPartitionReadGuard<'_> {
    pub(crate) const fn slot_count(&self) -> usize {
        self.range.slot_count
    }

    pub(crate) fn slot_state(&self, slot: usize) -> Result<IndexSlotState, IndexStorageError> {
        self.guard.state_at(slot)
    }

    pub(crate) fn global_slot(&self, slot: usize) -> Result<usize, IndexStorageError> {
        if slot >= self.range.slot_count {
            return Err(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.range.slot_count,
            });
        }
        self.range
            .first_slot
            .checked_add(slot)
            .ok_or(IndexStorageError::SizeOverflow)
    }
}

pub(crate) struct IndexPartitionWriteGuard<'a> {
    range: IndexPartitionRange,
    guard: RwLockWriteGuard<'a, IndexStorage>,
}

impl IndexPartitionWriteGuard<'_> {
    pub(crate) const fn slot_count(&self) -> usize {
        self.range.slot_count
    }

    pub(crate) fn slot_state(&self, slot: usize) -> Result<IndexSlotState, IndexStorageError> {
        self.guard.state_at(slot)
    }

    pub(crate) fn global_slot(&self, slot: usize) -> Result<usize, IndexStorageError> {
        if slot >= self.range.slot_count {
            return Err(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.range.slot_count,
            });
        }
        self.range
            .first_slot
            .checked_add(slot)
            .ok_or(IndexStorageError::SizeOverflow)
    }

    pub(crate) fn replace_observed(
        &mut self,
        slot: usize,
        previous: IndexSlotState,
        state: IndexSlotState,
    ) -> Result<(), IndexStorageError> {
        self.guard.replace_observed_state(slot, previous, state)
    }
}

impl PartitionedIndexStorage {
    pub(crate) fn anonymous(slot_count: usize) -> Result<Self, IndexStorageError> {
        let ranges = canonical_index_partition_ranges(slot_count)?;
        let whole = IndexStorage::anonymous(slot_count)?;
        let IndexStorage { core, .. } = whole;
        Self::from_core(slot_count, ranges, core, None)
    }

    #[cfg(feature = "benchmarking")]
    pub(crate) fn anonymous_single_partition(slot_count: usize) -> Result<Self, IndexStorageError> {
        let layout = ImageLayout::new(slot_count)?;
        let mut ranges = Vec::new();
        ranges.try_reserve_exact(1).map_err(|_| {
            IndexStorageError::Io(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "unable to allocate benchmark index partition",
            ))
        })?;
        ranges.push(IndexPartitionRange {
            partition_id: 0,
            first_page: 0,
            page_count: layout.page_count,
            first_slot: 0,
            slot_count,
        });
        let whole = IndexStorage::anonymous(slot_count)?;
        let IndexStorage { core, .. } = whole;
        Self::from_core(slot_count, ranges.into_boxed_slice(), core, None)
    }

    pub(crate) fn map_private(
        file: &File,
        file_offset: u64,
        slot_count: usize,
        expected_binding: IndexImageBinding,
        partition_stats: &[IndexPhysicalStats],
    ) -> Result<Self, IndexStorageError> {
        let ranges = canonical_index_partition_ranges(slot_count)?;
        if partition_stats.len() != ranges.len() {
            return Err(IndexStorageError::InvalidPhysicalStats);
        }
        for (range, stats) in ranges.iter().zip(partition_stats) {
            if !stats.is_valid_for(range.slot_count) {
                return Err(IndexStorageError::InvalidPhysicalStats);
            }
        }
        let physical_stats = checked_sum_physical_stats(partition_stats)?;
        if !physical_stats.is_valid_for(slot_count) {
            return Err(IndexStorageError::InvalidPhysicalStats);
        }
        let whole = IndexStorage::map_private(
            file,
            file_offset,
            slot_count,
            expected_binding,
            physical_stats,
        )?;
        let IndexStorage { core, .. } = whole;
        Self::from_core(slot_count, ranges, core, Some(partition_stats))
    }

    pub(crate) const fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub(crate) fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub(crate) fn partition_ranges(&self) -> &[IndexPartitionRange] {
        &self.ranges
    }

    pub(crate) fn try_read_hash_partition(
        &self,
        hash: u64,
    ) -> Result<IndexPartitionReadGuard<'_>, IndexStorageError> {
        let partition = index_partition_for(hash, self.partitions.len());
        let guard = match self.partitions[partition].try_read() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(IndexStorageError::PartitionBusy {
                    partition_id: partition,
                });
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(IndexStorageError::PartitionPoisoned {
                    partition_id: partition,
                });
            }
        };
        Ok(IndexPartitionReadGuard {
            range: self.ranges[partition],
            guard,
        })
    }

    pub(crate) fn write_hash_partition(
        &self,
        hash: u64,
    ) -> Result<IndexPartitionWriteGuard<'_>, IndexStorageError> {
        let partition = index_partition_for(hash, self.partitions.len());
        Ok(IndexPartitionWriteGuard {
            range: self.ranges[partition],
            guard: write_partition(&self.partitions[partition], partition)?,
        })
    }

    pub(crate) fn try_write_hash_partition(
        &self,
        hash: u64,
    ) -> Result<IndexPartitionWriteGuard<'_>, IndexStorageError> {
        let partition = index_partition_for(hash, self.partitions.len());
        let guard = match self.partitions[partition].try_write() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => {
                return Err(IndexStorageError::PartitionBusy {
                    partition_id: partition,
                });
            }
            Err(TryLockError::Poisoned(_)) => {
                return Err(IndexStorageError::PartitionPoisoned {
                    partition_id: partition,
                });
            }
        };
        Ok(IndexPartitionWriteGuard {
            range: self.ranges[partition],
            guard,
        })
    }

    pub(crate) fn physical_stats(&self) -> Result<IndexPhysicalStats, IndexStorageError> {
        let mut stats = IndexPhysicalStats::default();
        for (partition_id, partition) in self.partitions.iter().enumerate() {
            stats = checked_add_physical_stats(
                stats,
                read_partition(partition, partition_id)?.physical_stats(),
            )?;
        }
        stats
            .is_valid_for(self.slot_count)
            .then_some(stats)
            .ok_or(IndexStorageError::InvalidPhysicalStats)
    }

    pub(crate) fn partition_stats(&self) -> Result<Box<[IndexPhysicalStats]>, IndexStorageError> {
        let mut stats = Vec::new();
        stats
            .try_reserve_exact(self.partitions.len())
            .map_err(|_| {
                IndexStorageError::Io(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "unable to allocate index partition statistics",
                ))
            })?;
        for (partition_id, partition) in self.partitions.iter().enumerate() {
            stats.push(read_partition(partition, partition_id)?.physical_stats());
        }
        Ok(stats.into_boxed_slice())
    }

    #[cfg(test)]
    pub(crate) fn read_slot(&self, slot: usize) -> Result<IndexSlot, IndexStorageError> {
        let (partition, local_slot) = self.partition_for_slot(slot)?;
        read_partition(&self.partitions[partition], partition)?.read_slot(local_slot)
    }

    #[cfg(test)]
    pub(crate) fn write_slot(
        &self,
        slot: usize,
        value: IndexSlot,
    ) -> Result<(), IndexStorageError> {
        let (partition, local_slot) = self.partition_for_slot(slot)?;
        write_partition(&self.partitions[partition], partition)?.write_slot(local_slot, value)
    }

    /// Sequentially writes all canonical ranges in global page order.
    ///
    /// Every range read lock remains held for the complete emission, making
    /// the file one coherent index snapshot. The warm-close owner must still
    /// keep runtime mutation frozen until the matching metadata is published.
    pub(crate) fn write_warm_image<W>(
        &self,
        writer: &mut W,
        binding: IndexImageBinding,
    ) -> Result<WarmImageStats, IndexStorageError>
    where
        W: Write + ?Sized,
    {
        let mut frozen_partitions = Vec::new();
        frozen_partitions
            .try_reserve_exact(self.partitions.len())
            .map_err(|_| {
                IndexStorageError::Io(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "unable to allocate frozen index partition guards",
                ))
            })?;
        for (partition_id, partition) in self.partitions.iter().enumerate() {
            frozen_partitions.push(read_partition(partition, partition_id)?);
        }

        let physical_stats = frozen_partitions
            .iter()
            .try_fold(IndexPhysicalStats::default(), |stats, partition| {
                checked_add_physical_stats(stats, partition.physical_stats())
            })?;
        if !physical_stats.is_valid_for(self.slot_count) {
            return Err(IndexStorageError::InvalidPhysicalStats);
        }
        let mut total = WarmImageStats {
            pages_written: 0,
            slots_written: 0,
            bytes_written: 0,
            physical_stats: IndexPhysicalStats::default(),
        };
        let mut batch_writer = WarmImageBatchWriter::new(writer);
        for partition in &frozen_partitions {
            let written = partition.write_warm_image_pages(&mut batch_writer, binding)?;
            total.pages_written = total
                .pages_written
                .checked_add(written.pages_written)
                .ok_or(IndexStorageError::SizeOverflow)?;
            total.slots_written = total
                .slots_written
                .checked_add(written.slots_written)
                .ok_or(IndexStorageError::SizeOverflow)?;
            total.bytes_written = total
                .bytes_written
                .checked_add(written.bytes_written)
                .ok_or(IndexStorageError::SizeOverflow)?;
            total.physical_stats =
                checked_add_physical_stats(total.physical_stats, written.physical_stats)?;
        }
        batch_writer.finish()?;
        if total.slots_written != self.slot_count || total.physical_stats != physical_stats {
            return Err(IndexStorageError::InvalidArgument(
                "canonical index partition image emission is inconsistent",
            ));
        }
        Ok(total)
    }

    fn from_core(
        slot_count: usize,
        ranges: Box<[IndexPartitionRange]>,
        core: Arc<IndexStorageCore>,
        partition_stats: Option<&[IndexPhysicalStats]>,
    ) -> Result<Self, IndexStorageError> {
        if core.slot_count != slot_count || ranges.is_empty() {
            return Err(IndexStorageError::InvalidArgument(
                "index partition directory does not match the shared image",
            ));
        }
        let mut partitions = Vec::new();
        partitions.try_reserve_exact(ranges.len()).map_err(|_| {
            IndexStorageError::Io(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "unable to allocate index partition locks",
            ))
        })?;
        for (index, range) in ranges.iter().copied().enumerate() {
            let physical_stats = partition_stats
                .map(|stats| stats[index])
                .unwrap_or_default();
            partitions.push(RwLock::new(IndexStorage {
                core: Arc::clone(&core),
                range,
                physical_stats,
            }));
        }
        Ok(Self {
            slot_count,
            ranges,
            partitions: partitions.into_boxed_slice(),
        })
    }

    #[cfg(test)]
    fn partition_for_slot(&self, slot: usize) -> Result<(usize, usize), IndexStorageError> {
        if slot >= self.slot_count {
            return Err(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.slot_count,
            });
        }
        let partition = self
            .ranges
            .partition_point(|range| range.first_slot + range.slot_count <= slot);
        let range = self
            .ranges
            .get(partition)
            .ok_or(IndexStorageError::SlotOutOfBounds {
                slot,
                slot_count: self.slot_count,
            })?;
        let local_slot = slot
            .checked_sub(range.first_slot)
            .ok_or(IndexStorageError::SizeOverflow)?;
        Ok((partition, local_slot))
    }

    #[cfg(test)]
    pub(crate) fn poison_hash_partition_for_test(&self, hash: u64) {
        let partition = index_partition_for(hash, self.partitions.len());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = self.partitions[partition].write().unwrap();
            panic!("poison index partition for test");
        }));
        assert!(result.is_err());
    }
}

fn checked_sum_physical_stats(
    stats: &[IndexPhysicalStats],
) -> Result<IndexPhysicalStats, IndexStorageError> {
    stats
        .iter()
        .copied()
        .try_fold(IndexPhysicalStats::default(), checked_add_physical_stats)
}

fn checked_add_physical_stats(
    left: IndexPhysicalStats,
    right: IndexPhysicalStats,
) -> Result<IndexPhysicalStats, IndexStorageError> {
    Ok(IndexPhysicalStats {
        value: left
            .value
            .checked_add(right.value)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?,
        deleted: left
            .deleted
            .checked_add(right.deleted)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?,
    })
}

fn read_partition(
    lock: &RwLock<IndexStorage>,
    partition_id: usize,
) -> Result<RwLockReadGuard<'_, IndexStorage>, IndexStorageError> {
    lock.read()
        .map_err(|_| IndexStorageError::PartitionPoisoned { partition_id })
}

fn write_partition(
    lock: &RwLock<IndexStorage>,
    partition_id: usize,
) -> Result<RwLockWriteGuard<'_, IndexStorage>, IndexStorageError> {
    lock.write()
        .map_err(|_| IndexStorageError::PartitionPoisoned { partition_id })
}

/// Fixed-memory sequential write combiner used only while producing an
/// unpublished warm image.
///
/// The buffer is allocated lazily after the index writer validates the image
/// binding. Dropping this writer after an encoding error intentionally discards
/// an unflushed tail; the destination image is temporary and must never be
/// published after any error.
struct WarmImageBatchWriter<'a, W: Write + ?Sized> {
    inner: &'a mut W,
    buffer: Vec<u8>,
    used: usize,
}

impl<'a, W: Write + ?Sized> WarmImageBatchWriter<'a, W> {
    const fn new(inner: &'a mut W) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            used: 0,
        }
    }

    fn ensure_buffer(&mut self) -> io::Result<()> {
        if self.buffer.is_empty() {
            self.buffer
                .try_reserve_exact(WARM_IMAGE_WRITE_BATCH_BYTES)
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "unable to allocate warm index write batch",
                    )
                })?;
            self.buffer.resize(WARM_IMAGE_WRITE_BATCH_BYTES, 0);
        }
        Ok(())
    }

    fn flush_buffer(&mut self) -> io::Result<()> {
        if self.used != 0 {
            self.inner.write_all(&self.buffer[..self.used])?;
            self.used = 0;
        }
        Ok(())
    }

    fn finish(mut self) -> io::Result<()> {
        self.flush_buffer()
    }
}

impl<W: Write + ?Sized> Write for WarmImageBatchWriter<'_, W> {
    fn write(&mut self, mut input: &[u8]) -> io::Result<usize> {
        let input_len = input.len();
        if input_len == 0 {
            return Ok(0);
        }
        self.ensure_buffer()?;
        while !input.is_empty() {
            let available = WARM_IMAGE_WRITE_BATCH_BYTES - self.used;
            let copied = available.min(input.len());
            self.buffer[self.used..self.used + copied].copy_from_slice(&input[..copied]);
            self.used += copied;
            input = &input[copied..];
            if self.used == WARM_IMAGE_WRITE_BATCH_BYTES {
                self.flush_buffer()?;
            }
        }
        Ok(input_len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.flush_buffer()?;
        self.inner.flush()
    }
}

#[derive(Clone, Copy)]
struct ImageLayout {
    page_count: usize,
    image_len: usize,
}

impl ImageLayout {
    fn new(slot_count: usize) -> Result<Self, IndexStorageError> {
        if slot_count == 0 {
            return Err(IndexStorageError::InvalidArgument(
                "index storage requires at least one slot",
            ));
        }
        let page_count = slot_count
            .checked_add(INDEX_IMAGE_SLOTS_PER_PAGE - 1)
            .ok_or(IndexStorageError::SizeOverflow)?
            / INDEX_IMAGE_SLOTS_PER_PAGE;
        let image_len = page_count
            .checked_mul(INDEX_IMAGE_PAGE_SIZE)
            .ok_or(IndexStorageError::SizeOverflow)?;
        if image_len > isize::MAX as usize {
            return Err(IndexStorageError::SizeOverflow);
        }
        Ok(Self {
            page_count,
            image_len,
        })
    }
}

fn allocate_page_states(
    page_count: usize,
    initial: u8,
) -> Result<Box<[AtomicU8]>, IndexStorageError> {
    let mut states = Vec::new();
    states.try_reserve_exact(page_count).map_err(|_| {
        IndexStorageError::Io(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "unable to allocate index page validation bitmap",
        ))
    })?;
    for _ in 0..page_count {
        states.push(AtomicU8::new(initial));
    }
    Ok(states.into_boxed_slice())
}

enum Backing {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    Mapping(Mapping),
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    Heap(Box<[u8]>),
}

impl Backing {
    fn anonymous(length: usize) -> Result<Self, IndexStorageError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            Ok(Self::Mapping(Mapping::anonymous(length)?))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(length).map_err(|_| {
                IndexStorageError::Io(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "unable to allocate anonymous index image",
                ))
            })?;
            bytes.resize(length, 0);
            Ok(Self::Heap(bytes.into_boxed_slice()))
        }
    }

    fn map_file_private(file: &File, length: usize) -> Result<Self, IndexStorageError> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            Ok(Self::Mapping(Mapping::file_private(file, length)?))
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        {
            let _ = (file, length);
            Err(IndexStorageError::Io(io::Error::new(
                io::ErrorKind::Unsupported,
                "writable MAP_PRIVATE index recovery is only supported on Linux and macOS",
            )))
        }
    }

    fn as_mut_ptr(&mut self) -> *mut u8 {
        match self {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Mapping(mapping) => mapping.pointer,
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            Self::Heap(bytes) => bytes.as_mut_ptr(),
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct Mapping {
    pointer: *mut u8,
    length: usize,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Mapping {
    fn anonymous(length: usize) -> io::Result<Self> {
        let flags = libc::MAP_PRIVATE | libc::MAP_ANON;
        Self::map(length, flags, -1)
    }

    fn file_private(file: &File, length: usize) -> io::Result<Self> {
        Self::map(length, libc::MAP_PRIVATE, file.as_raw_fd())
    }

    fn map(length: usize, flags: i32, descriptor: i32) -> io::Result<Self> {
        // SAFETY: the requested range is non-zero and bounded by `isize::MAX`.
        // Anonymous mappings ignore descriptor; file mappings use offset zero
        // after the caller verifies the file range.
        let mapped = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                descriptor,
                0,
            )
        };
        if mapped == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            pointer: mapped.cast(),
            length,
        })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: this is the exact live range returned by `mmap`, owned by
        // this object and released once here.
        let result = unsafe { libc::munmap(self.pointer.cast(), self.length) };
        debug_assert_eq!(result, 0, "index image munmap failed");
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
unsafe impl Send for Mapping {}
#[cfg(any(target_os = "linux", target_os = "macos"))]
unsafe impl Sync for Mapping {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::sparse_golden;
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(0);

    const fn binding(generation: u64) -> IndexImageBinding {
        IndexImageBinding {
            generation,
            image_tag: 0x0102_0304_0506_0708,
        }
    }

    struct TestFile {
        path: PathBuf,
        file: File,
    }

    impl TestFile {
        fn create() -> Self {
            let id = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cache2-index-image-{}-{id}.tmp",
                std::process::id()
            ));
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)
                .unwrap();
            Self { path, file }
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn sample_slot(seed: u64) -> IndexSlot {
        let location = PackedLocation::new(
            (seed % 64) as u32,
            ((seed % 128) * u64::from(crate::format::RECORD_ALIGNMENT)) as u32,
            32,
        )
        .unwrap();
        IndexSlot::from_state(IndexSlotState::Value {
            fingerprint: seed as u16 & 0x3fff,
            displacement: seed as u8 & 3,
            entry: IndexEntry { location },
        })
    }

    const PARTITIONED_SLOT_COUNT: usize = 1009;
    const FIRST_PARTITION_LAST_SLOT: usize = INDEX_IMAGE_SLOTS_PER_PAGE - 1;
    const SECOND_PARTITION_FIRST_SLOT: usize = INDEX_IMAGE_SLOTS_PER_PAGE;

    fn populated_partitioned_storage() -> (PartitionedIndexStorage, [IndexSlot; 3]) {
        let storage = PartitionedIndexStorage::anonymous(PARTITIONED_SLOT_COUNT).unwrap();
        let values = [sample_slot(1), sample_slot(2), sample_slot(3)];
        storage
            .write_slot(FIRST_PARTITION_LAST_SLOT, values[0])
            .unwrap();
        storage
            .write_slot(SECOND_PARTITION_FIRST_SLOT, values[1])
            .unwrap();
        storage
            .write_slot(PARTITIONED_SLOT_COUNT - 1, values[2])
            .unwrap();
        (storage, values)
    }

    #[test]
    fn canonical_partition_ranges_balance_pages_and_keep_a_usable_tail() {
        type RangeShape = (usize, usize, usize, usize);
        let cases: &[(usize, &[RangeShape])] = &[
            (8, &[(0, 1, 0, 8)]),
            (504, &[(0, 1, 0, 504)]),
            (505, &[(0, 2, 0, 505)]),
            (507, &[(0, 2, 0, 507)]),
            (508, &[(0, 1, 0, 504), (1, 1, 504, 4)]),
            (1008, &[(0, 1, 0, 504), (1, 1, 504, 504)]),
            (1009, &[(0, 1, 0, 504), (1, 2, 504, 505)]),
        ];

        for &(slot_count, expected) in cases {
            let ranges = canonical_index_partition_ranges(slot_count).unwrap();
            assert!(
                ranges
                    .iter()
                    .enumerate()
                    .all(|(partition_id, range)| range.partition_id == partition_id)
            );
            let actual: Vec<_> = ranges
                .iter()
                .map(|range| {
                    (
                        range.first_page,
                        range.page_count,
                        range.first_slot,
                        range.slot_count,
                    )
                })
                .collect();
            assert_eq!(actual, expected, "slot_count={slot_count}");
            assert!(actual.iter().all(|range| range.3 >= INDEX_CANDIDATES));
        }
    }

    #[test]
    fn partitioned_storage_routes_boundary_slots_and_tracks_stats() {
        let (storage, values) = populated_partitioned_storage();
        assert_eq!(
            storage.read_slot(FIRST_PARTITION_LAST_SLOT).unwrap(),
            values[0]
        );
        assert_eq!(
            storage.read_slot(SECOND_PARTITION_FIRST_SLOT).unwrap(),
            values[1]
        );
        assert_eq!(
            storage.read_slot(PARTITIONED_SLOT_COUNT - 1).unwrap(),
            values[2]
        );

        let expected_partition_stats: Box<[IndexPhysicalStats]> = Box::new([
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
            },
            IndexPhysicalStats {
                value: 2,
                deleted: 0,
            },
        ]);
        assert_eq!(storage.partition_stats().unwrap(), expected_partition_stats);
        assert_eq!(
            storage.physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 3,
                deleted: 0,
            }
        );

        storage
            .write_slot(FIRST_PARTITION_LAST_SLOT, IndexSlot::EMPTY)
            .unwrap();
        assert_eq!(
            storage.partition_stats().unwrap().as_ref(),
            &[IndexPhysicalStats::default(), expected_partition_stats[1]]
        );
    }

    #[test]
    fn partitioned_warm_image_round_trips_boundary_slots_and_stats() {
        const GENERATION: u64 = 113;

        let (source, values) = populated_partitioned_storage();
        let expected_partition_stats = source.partition_stats().unwrap();

        let mut test_file = TestFile::create();
        let written = source
            .write_warm_image(&mut test_file.file, binding(GENERATION))
            .unwrap();
        assert_eq!(written.pages_written, 3);
        assert_eq!(written.slots_written, PARTITIONED_SLOT_COUNT);
        assert_eq!(written.bytes_written, (3 * INDEX_IMAGE_PAGE_SIZE) as u64);
        assert_eq!(written.physical_stats, source.physical_stats().unwrap());
        test_file.file.sync_all().unwrap();

        let recovered = PartitionedIndexStorage::map_private(
            &test_file.file,
            0,
            PARTITIONED_SLOT_COUNT,
            binding(GENERATION),
            &expected_partition_stats,
        )
        .unwrap();
        assert_eq!(
            recovered.partition_stats().unwrap(),
            expected_partition_stats
        );
        assert_eq!(
            recovered.read_slot(FIRST_PARTITION_LAST_SLOT).unwrap(),
            values[0]
        );
        assert_eq!(
            recovered.read_slot(SECOND_PARTITION_FIRST_SLOT).unwrap(),
            values[1]
        );
        assert_eq!(
            recovered.read_slot(PARTITIONED_SLOT_COUNT - 1).unwrap(),
            values[2]
        );
    }

    #[test]
    fn poisoned_partition_is_never_read_written_or_persisted() {
        let storage = PartitionedIndexStorage::anonymous(8).unwrap();
        storage.poison_hash_partition_for_test(7);

        assert!(matches!(
            storage.try_read_hash_partition(7),
            Err(IndexStorageError::PartitionPoisoned { partition_id: 0 })
        ));
        assert!(matches!(
            storage.write_hash_partition(7),
            Err(IndexStorageError::PartitionPoisoned { partition_id: 0 })
        ));
        assert!(matches!(
            storage.physical_stats(),
            Err(IndexStorageError::PartitionPoisoned { partition_id: 0 })
        ));
        assert!(matches!(
            storage.partition_stats(),
            Err(IndexStorageError::PartitionPoisoned { partition_id: 0 })
        ));
        assert!(matches!(
            storage.write_warm_image(&mut Vec::new(), binding(1)),
            Err(IndexStorageError::PartitionPoisoned { partition_id: 0 })
        ));
    }

    #[test]
    fn partitioned_warm_image_aggregates_pages_into_bounded_mib_writes() {
        const PAGES_PER_BATCH: usize = WARM_IMAGE_WRITE_BATCH_BYTES / INDEX_IMAGE_PAGE_SIZE;
        const PAGE_COUNT: usize = PAGES_PER_BATCH * 3 + 7;
        const SLOT_COUNT: usize = PAGE_COUNT * INDEX_IMAGE_SLOTS_PER_PAGE;

        #[derive(Default)]
        struct CountingSink {
            calls: Vec<usize>,
            bytes: usize,
        }

        impl Write for CountingSink {
            fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
                self.calls.push(buffer.len());
                self.bytes += buffer.len();
                Ok(buffer.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        assert!(
            source.partition_count() > 1,
            "test image must span canonical partitions"
        );
        let mut sink = CountingSink::default();
        let written = source.write_warm_image(&mut sink, binding(149)).unwrap();

        assert_eq!(written.pages_written, PAGE_COUNT);
        assert_eq!(written.slots_written, SLOT_COUNT);
        assert_eq!(sink.bytes, PAGE_COUNT * INDEX_IMAGE_PAGE_SIZE);
        assert_eq!(
            sink.calls,
            [
                WARM_IMAGE_WRITE_BATCH_BYTES,
                WARM_IMAGE_WRITE_BATCH_BYTES,
                WARM_IMAGE_WRITE_BATCH_BYTES,
                7 * INDEX_IMAGE_PAGE_SIZE,
            ]
        );
        assert!(
            sink.calls.len() * 100 < PAGE_COUNT,
            "warm image writes must be aggregated far below one call per page"
        );
        assert!(
            sink.calls
                .iter()
                .all(|bytes| *bytes <= WARM_IMAGE_WRITE_BATCH_BYTES)
        );
    }

    #[test]
    fn corrupt_page_in_one_partition_rejects_every_partition() {
        const SLOT_COUNT: usize = INDEX_IMAGE_SLOTS_PER_PAGE * 2;
        const GENERATION: u64 = 127;

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        source.write_slot(0, sample_slot(1)).unwrap();
        source
            .write_slot(INDEX_IMAGE_SLOTS_PER_PAGE, sample_slot(2))
            .unwrap();
        let partition_stats = source.partition_stats().unwrap();
        let mut image = Vec::new();
        source
            .write_warm_image(&mut image, binding(GENERATION))
            .unwrap();
        image[INDEX_IMAGE_PAGE_SIZE + INDEX_IMAGE_PAGE_HEADER_SIZE + 7] ^= 0x80;

        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();
        let recovered = PartitionedIndexStorage::map_private(
            &test_file.file,
            0,
            SLOT_COUNT,
            binding(GENERATION),
            &partition_stats,
        )
        .unwrap();

        assert_eq!(recovered.read_slot(0).unwrap(), sample_slot(1));
        assert!(matches!(
            recovered.read_slot(INDEX_IMAGE_SLOTS_PER_PAGE),
            Err(IndexStorageError::CorruptPage {
                page_index: 1,
                reason: CorruptPageReason::ChecksumMismatch { .. }
            })
        ));
        assert!(matches!(
            recovered.read_slot(0),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::PreviouslyRejected
            })
        ));
        assert!(matches!(
            recovered.write_slot(0, sample_slot(3)),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::PreviouslyRejected
            })
        ));
    }

    #[test]
    fn crc_valid_semantic_slot_corruption_rejects_the_complete_image() {
        const SLOT_COUNT: usize = INDEX_IMAGE_SLOTS_PER_PAGE;
        const GENERATION: u64 = 131;

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        let mut image = Vec::new();
        source
            .write_warm_image(&mut image, binding(GENERATION))
            .unwrap();
        let page: &mut [u8; INDEX_IMAGE_PAGE_SIZE] = image.as_mut_slice().try_into().unwrap();
        put_u64(page, INDEX_IMAGE_PAGE_HEADER_SIZE, 1);
        let checksum = page_checksum(page);
        put_u32(page, PAGE_CHECKSUM_OFFSET, checksum);

        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();
        let recovered = PartitionedIndexStorage::map_private(
            &test_file.file,
            0,
            SLOT_COUNT,
            binding(GENERATION),
            &[IndexPhysicalStats::default()],
        )
        .unwrap();

        assert!(matches!(
            recovered.read_slot(0),
            Err(IndexStorageError::CorruptSlot {
                slot_index: 0,
                reason: IndexSlotSemanticError::NonCanonicalMarker
            })
        ));
        assert!(matches!(
            recovered.read_slot(1),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::PreviouslyRejected
            })
        ));
    }

    #[test]
    fn slot_codec_quantizes_only_the_record_length() {
        let exact = PackedLocation::new(0x54321, 0x12340, 1056).unwrap();
        let state = IndexSlotState::Value {
            fingerprint: 0x2345,
            displacement: 2,
            entry: IndexEntry { location: exact },
        };
        let slot = IndexSlot::from_state(state);
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        slot.encode(&mut encoded);
        assert_eq!(encoded, [0x21, 0x43, 0xa5, 0x91, 0x00, 0x21, 0x45, 0xa3]);

        let IndexSlotState::Value {
            fingerprint,
            displacement,
            entry,
        } = IndexSlot::decode(&encoded).runtime_state().unwrap()
        else {
            panic!("non-empty slot must decode as a value");
        };
        assert_eq!(fingerprint, 0x2345);
        assert_eq!(displacement, 2);
        assert_eq!(entry.location.region_id(), exact.region_id());
        assert_eq!(entry.location.offset(), exact.offset());
        assert_eq!(entry.location.record_len(), 1120);
        assert!(entry.location.index_equivalent(exact));
    }

    #[test]
    fn index_page_matches_committed_golden_bytes() {
        let source = PartitionedIndexStorage::anonymous(INDEX_IMAGE_SLOTS_PER_PAGE).unwrap();
        let location = PackedLocation::new(0x54321, 0x12340, 1056).unwrap();
        source
            .write_slot(
                0,
                IndexSlot::from_state(IndexSlotState::Value {
                    fingerprint: 0x2345,
                    displacement: 2,
                    entry: IndexEntry { location },
                }),
            )
            .unwrap();
        let mut encoded = Vec::new();
        source
            .write_warm_image(&mut encoded, binding(0x1122_3344_5566_7788))
            .unwrap();
        let golden = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/index_page.golden"
        ));
        assert_eq!(encoded, golden);
    }

    #[test]
    fn warm_writer_rejects_counter_drift_while_emitting_the_image() {
        let mut storage = IndexStorage::anonymous(8).unwrap();
        storage.physical_stats.value = 1;

        assert!(matches!(
            storage.write_warm_image(&mut Vec::new(), binding(1)),
            Err(IndexStorageError::InvalidPhysicalStats)
        ));
    }

    #[test]
    fn mapped_physical_stats_must_fit_the_slot_capacity() {
        let test_file = TestFile::create();
        assert!(matches!(
            IndexStorage::map_private(
                &test_file.file,
                0,
                1,
                binding(1),
                IndexPhysicalStats {
                    value: 1,
                    deleted: 1,
                }
            ),
            Err(IndexStorageError::InvalidPhysicalStats)
        ));
    }

    #[test]
    fn mapped_image_rejects_unaligned_offset_and_zero_binding() {
        const PREFIX: usize = INDEX_IMAGE_PAGE_SIZE;

        let source = IndexStorage::anonymous(1).unwrap();
        let mut test_file = TestFile::create();
        test_file.file.set_len(PREFIX as u64).unwrap();
        test_file.file.seek(SeekFrom::Start(PREFIX as u64)).unwrap();
        source
            .write_warm_image(&mut test_file.file, binding(47))
            .unwrap();
        test_file.file.sync_all().unwrap();

        assert!(matches!(
            IndexStorage::map_private(
                &test_file.file,
                (PREFIX + 1) as u64,
                1,
                binding(47),
                source.physical_stats()
            ),
            Err(IndexStorageError::InvalidArgument(
                "index image file offset must be 4 KiB aligned"
            ))
        ));
        assert!(matches!(
            IndexStorage::map_private(
                &test_file.file,
                PREFIX as u64,
                1,
                IndexImageBinding {
                    generation: 0,
                    image_tag: 1
                },
                source.physical_stats()
            ),
            Err(IndexStorageError::InvalidArgument(
                "mapped index image binding must be non-zero"
            ))
        ));
    }

    #[test]
    fn mapped_warm_image_validates_lazily_and_is_copy_on_write() {
        const SLOT_COUNT: usize = INDEX_IMAGE_SLOTS_PER_PAGE + 3;
        const GENERATION: u64 = 47;
        const PREFIX: usize = INDEX_IMAGE_PAGE_SIZE;

        let mut source = IndexStorage::anonymous(SLOT_COUNT).unwrap();
        source.write_slot(0, sample_slot(1)).unwrap();
        source
            .write_slot(INDEX_IMAGE_SLOTS_PER_PAGE + 2, sample_slot(2))
            .unwrap();

        let mut test_file = TestFile::create();
        test_file.file.set_len(PREFIX as u64).unwrap();
        test_file.file.seek(SeekFrom::Start(PREFIX as u64)).unwrap();
        source
            .write_warm_image(&mut test_file.file, binding(GENERATION))
            .unwrap();
        test_file.file.sync_all().unwrap();

        let mut recovered = IndexStorage::map_private(
            &test_file.file,
            PREFIX as u64,
            SLOT_COUNT,
            binding(GENERATION),
            source.physical_stats(),
        )
        .unwrap();
        assert_eq!(recovered.physical_stats(), source.physical_stats());
        assert_eq!(
            recovered.page_validation_state(0).unwrap(),
            PageValidationState::Unchecked
        );
        let expected = sample_slot(1);
        assert_eq!(recovered.read_slot(0).unwrap(), expected);
        assert_eq!(
            recovered.page_validation_state(0).unwrap(),
            PageValidationState::Valid
        );

        let private_value = sample_slot(3);
        recovered.write_slot(0, private_value).unwrap();
        assert_eq!(recovered.read_slot(0).unwrap(), private_value);
        drop(recovered);

        test_file
            .file
            .seek(SeekFrom::Start(
                (PREFIX + INDEX_IMAGE_PAGE_HEADER_SIZE) as u64,
            ))
            .unwrap();
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        test_file.file.read_exact(&mut encoded).unwrap();
        assert_eq!(IndexSlot::decode(&encoded), expected);
    }

    #[test]
    fn image_binding_is_checked_on_first_page_touch() {
        let source = IndexStorage::anonymous(1).unwrap();
        assert!(matches!(
            source.write_warm_image(
                &mut Vec::new(),
                IndexImageBinding {
                    generation: 0,
                    image_tag: 1
                }
            ),
            Err(IndexStorageError::InvalidArgument(
                "warm index image binding must be non-zero"
            ))
        ));
        let mut image = Vec::new();
        source.write_warm_image(&mut image, binding(7)).unwrap();
        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();

        let recovered = IndexStorage::map_private(
            &test_file.file,
            0,
            1,
            binding(8),
            IndexPhysicalStats::default(),
        )
        .unwrap();
        assert_eq!(
            recovered.page_validation_state(0).unwrap(),
            PageValidationState::Unchecked
        );
        assert!(matches!(
            recovered.read_slot(0),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::WrongGeneration {
                    expected: 8,
                    actual: 7
                }
            })
        ));

        let recovered = IndexStorage::map_private(
            &test_file.file,
            0,
            1,
            IndexImageBinding {
                generation: 7,
                image_tag: binding(7).image_tag ^ 1,
            },
            IndexPhysicalStats::default(),
        )
        .unwrap();
        assert!(matches!(
            recovered.read_slot(0),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::WrongImageTag { .. }
            })
        ));
    }
}
