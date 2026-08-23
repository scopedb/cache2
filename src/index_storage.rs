//! Stable, mmap-friendly storage for the Region V2 fixed-size index.
//!
//! The runtime index deliberately does not persist a Rust structure. Every
//! slot is encoded field-by-field in little-endian order, and every 4 KiB page
//! carries an independently verifiable header and CRC32C. A recovered image is
//! mapped writable with `MAP_PRIVATE`: reads initially use the clean image and
//! runtime mutations become private copy-on-write pages.

use crate::checksum::Crc32c;
use crate::index::INDEX_RUNTIME_ONLY_FLAGS;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::ptr;
use std::sync::atomic::{AtomicU8, Ordering};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::fd::AsRawFd;

pub(crate) const INDEX_IMAGE_PAGE_SIZE: usize = 4096;
pub(crate) const INDEX_IMAGE_PAGE_HEADER_SIZE: usize = 64;
pub(crate) const INDEX_IMAGE_SLOT_SIZE: usize = 32;
pub(crate) const INDEX_IMAGE_SLOTS_PER_PAGE: usize =
    (INDEX_IMAGE_PAGE_SIZE - INDEX_IMAGE_PAGE_HEADER_SIZE) / INDEX_IMAGE_SLOT_SIZE;

const PAGE_MAGIC: [u8; 8] = *b"CRSIDX1\0";
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
const PAGE_CHECKSUM_OFFSET: usize = 56;
const PAGE_TRAILING_RESERVED_OFFSET: usize = 60;

const PAGE_FLAG_NONE: u32 = 0;
const PAGE_STATE_UNCHECKED: u8 = 0;
const PAGE_STATE_VALIDATING: u8 = 1;
const PAGE_STATE_VALID: u8 = 2;
const PAGE_STATE_DIRTY: u8 = 3;
const PAGE_STATE_REJECTED: u8 = 4;
const IMAGE_STATE_USABLE: u8 = 0;
const IMAGE_STATE_REJECTED: u8 = 1;

pub(crate) const INDEX_SLOT_FLAG_MASKED: u32 = 1 << 31;
const INDEX_LOCATION_TOMBSTONE_BIT: u64 = 1 << 63;

const _: () = assert!(INDEX_IMAGE_SLOTS_PER_PAGE == 126);
const _: () = assert!(
    INDEX_IMAGE_PAGE_HEADER_SIZE + INDEX_IMAGE_SLOTS_PER_PAGE * INDEX_IMAGE_SLOT_SIZE
        == INDEX_IMAGE_PAGE_SIZE
);

/// Logical fields in one Index Image V1 slot.
///
/// This type is intentionally not `repr(C)` and is never copied directly to
/// or from an image. Its stable representation is exactly 32 bytes encoded by
/// [`Self::encode`] and [`Self::decode`]. A zeroed slot is the empty state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct IndexSlotV1 {
    pub(crate) hash: u64,
    pub(crate) location_raw: u64,
    pub(crate) seqno: u64,
    pub(crate) namespace_id: u32,
    pub(crate) flags: u32,
}

impl IndexSlotV1 {
    pub(crate) const EMPTY: Self = Self {
        hash: 0,
        location_raw: 0,
        seqno: 0,
        namespace_id: 0,
        flags: 0,
    };

    pub(crate) fn encode(self, output: &mut [u8; INDEX_IMAGE_SLOT_SIZE]) {
        output[0..8].copy_from_slice(&self.hash.to_le_bytes());
        output[8..16].copy_from_slice(&self.location_raw.to_le_bytes());
        output[16..24].copy_from_slice(&self.seqno.to_le_bytes());
        output[24..28].copy_from_slice(&self.namespace_id.to_le_bytes());
        output[28..32].copy_from_slice(&self.flags.to_le_bytes());
    }

    pub(crate) fn decode(input: &[u8; INDEX_IMAGE_SLOT_SIZE]) -> Self {
        Self {
            hash: read_u64(input, 0),
            location_raw: read_u64(input, 8),
            seqno: read_u64(input, 16),
            namespace_id: read_u32(input, 24),
            flags: read_u32(input, 28),
        }
    }

    fn physical_kind(self) -> SlotPhysicalKind {
        if self == Self::EMPTY {
            SlotPhysicalKind::Empty
        } else if self.flags & INDEX_SLOT_FLAG_MASKED != 0 {
            SlotPhysicalKind::Masked
        } else if self.location_raw & INDEX_LOCATION_TOMBSTONE_BIT != 0 {
            SlotPhysicalKind::Deleted
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
pub(crate) struct IndexImageBindingV1 {
    pub(crate) generation: u64,
    pub(crate) image_tag: u64,
}

impl IndexImageBindingV1 {
    const fn is_valid(self) -> bool {
        self.generation != 0 && self.image_tag != 0
    }
}

/// Counts of the three non-empty physical slot states.
///
/// Clean recovery metadata validates these counts before passing them to
/// [`IndexStorage::map_private`], avoiding an O(slot-count) startup scan.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct IndexPhysicalStats {
    pub(crate) value: u64,
    pub(crate) deleted: u64,
    pub(crate) masked: u64,
}

impl IndexPhysicalStats {
    fn total(self) -> Option<u64> {
        self.value
            .checked_add(self.deleted)?
            .checked_add(self.masked)
    }

    fn is_valid_for(self, slot_count: usize) -> bool {
        let Ok(slot_count) = u64::try_from(slot_count) else {
            return false;
        };
        self.total().is_some_and(|total| total <= slot_count)
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
            SlotPhysicalKind::Deleted => &mut self.deleted,
            SlotPhysicalKind::Masked => &mut self.masked,
        };
        *counter = counter.checked_sub(1)?;
        Some(())
    }

    fn increment(&mut self, kind: SlotPhysicalKind) -> Option<()> {
        let counter = match kind {
            SlotPhysicalKind::Empty => return Some(()),
            SlotPhysicalKind::Value => &mut self.value,
            SlotPhysicalKind::Deleted => &mut self.deleted,
            SlotPhysicalKind::Masked => &mut self.masked,
        };
        *counter = counter.checked_add(1)?;
        Some(())
    }
}

#[derive(Clone, Copy)]
enum SlotPhysicalKind {
    Empty,
    Value,
    Deleted,
    Masked,
}

impl SlotPhysicalKind {
    const fn is_masked(self) -> bool {
        matches!(self, Self::Masked)
    }
}

/// The lazy validation state of one physical image page.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PageValidationState {
    Unchecked,
    Validating,
    Valid,
    Dirty,
    Rejected,
}

/// Why a recovered Index Image V1 page was rejected.
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
    SizeOverflow,
    SlotOutOfBounds {
        slot: usize,
        slot_count: usize,
    },
    PageOutOfBounds {
        page: usize,
        page_count: usize,
    },
    CorruptPage {
        page_index: usize,
        reason: CorruptPageReason,
    },
    InvalidPhysicalStats,
    MaskedSlotsPresent {
        count: u64,
    },
}

impl fmt::Display for IndexStorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "index storage I/O failed: {error}"),
            Self::InvalidArgument(message) => formatter.write_str(message),
            Self::SizeOverflow => {
                formatter.write_str("index image size overflows the address space")
            }
            Self::SlotOutOfBounds { slot, slot_count } => {
                write!(formatter, "index slot {slot} is outside {slot_count} slots")
            }
            Self::PageOutOfBounds { page, page_count } => {
                write!(formatter, "index page {page} is outside {page_count} pages")
            }
            Self::CorruptPage { page_index, reason } => {
                write!(formatter, "index page {page_index} is corrupt: {reason}")
            }
            Self::InvalidPhysicalStats => {
                formatter.write_str("index physical slot counts are inconsistent")
            }
            Self::MaskedSlotsPresent { count } => {
                write!(formatter, "warm index image contains {count} masked slots")
            }
        }
    }
}

impl std::error::Error for IndexStorageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidArgument(_)
            | Self::SizeOverflow
            | Self::SlotOutOfBounds { .. }
            | Self::PageOutOfBounds { .. }
            | Self::CorruptPage { .. }
            | Self::InvalidPhysicalStats
            | Self::MaskedSlotsPresent { .. } => None,
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
}

/// Fixed-capacity bytes backing the Region V2 index.
///
/// Anonymous storage uses a zero-filled private mapping and treats its pages
/// as valid without materializing page headers. File-backed storage validates
/// each source page only when a slot on that page is first read or mutated.
/// Callers must freeze mutations while [`Self::write_warm_image`] runs.
pub(crate) struct IndexStorage {
    backing: Backing,
    data_offset: usize,
    image_len: usize,
    slot_count: usize,
    page_count: usize,
    expected_binding: Option<IndexImageBindingV1>,
    physical_stats: IndexPhysicalStats,
    image_state: AtomicU8,
    page_states: Box<[AtomicU8]>,
}

impl IndexStorage {
    /// Creates a lazily allocated, zero-filled runtime image.
    pub(crate) fn anonymous(slot_count: usize) -> Result<Self, IndexStorageError> {
        let layout = ImageLayout::new(slot_count)?;
        let backing = Backing::anonymous(layout.image_len)?;
        let page_states = allocate_page_states(layout.page_count, PAGE_STATE_VALID)?;
        Ok(Self {
            backing,
            data_offset: 0,
            image_len: layout.image_len,
            slot_count,
            page_count: layout.page_count,
            expected_binding: None,
            physical_stats: IndexPhysicalStats::default(),
            image_state: AtomicU8::new(IMAGE_STATE_USABLE),
            page_states,
        })
    }

    /// Maps an Index Image V1 range writable and private.
    ///
    /// Opening checks only range bounds and establishes the mapping. It does
    /// not scan page headers, slots, or CRCs. `file_offset` must be 4 KiB image
    /// aligned, but need not match the host's mmap page size because the
    /// mapping starts at file offset zero and addresses the requested subrange.
    pub(crate) fn map_private(
        file: &File,
        file_offset: u64,
        slot_count: usize,
        expected_binding: IndexImageBindingV1,
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
        if file_offset % INDEX_IMAGE_PAGE_SIZE as u64 != 0 {
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

        let backing = Backing::map_file_private(file, mapping_len)?;
        let page_states = allocate_page_states(layout.page_count, PAGE_STATE_UNCHECKED)?;
        Ok(Self {
            backing,
            data_offset,
            image_len: layout.image_len,
            slot_count,
            page_count: layout.page_count,
            expected_binding: Some(expected_binding),
            physical_stats,
            image_state: AtomicU8::new(IMAGE_STATE_USABLE),
            page_states,
        })
    }

    pub(crate) const fn slot_count(&self) -> usize {
        self.slot_count
    }

    pub(crate) const fn physical_stats(&self) -> IndexPhysicalStats {
        self.physical_stats
    }

    pub(crate) fn image_len_for_slots(slot_count: usize) -> Result<u64, IndexStorageError> {
        let layout = ImageLayout::new(slot_count)?;
        u64::try_from(layout.image_len).map_err(|_| IndexStorageError::SizeOverflow)
    }

    pub(crate) fn page_validation_state(
        &self,
        page: usize,
    ) -> Result<PageValidationState, IndexStorageError> {
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
        let state = state.load(Ordering::Acquire);
        Ok(match state {
            PAGE_STATE_UNCHECKED => PageValidationState::Unchecked,
            PAGE_STATE_VALIDATING => PageValidationState::Validating,
            PAGE_STATE_VALID => PageValidationState::Valid,
            PAGE_STATE_DIRTY => PageValidationState::Dirty,
            PAGE_STATE_REJECTED => PageValidationState::Rejected,
            _ => PageValidationState::Rejected,
        })
    }

    pub(crate) fn read_slot(&self, slot: usize) -> Result<IndexSlotV1, IndexStorageError> {
        let (page, offset) = self.slot_address(slot)?;
        self.ensure_page_valid(page)?;
        Ok(self.decode_at(offset))
    }

    /// Encodes one logical slot. Exclusive access makes the byte mutation
    /// sound; the future sharded index must split storage on page boundaries or
    /// put the owning range behind its shard lock.
    pub(crate) fn write_slot(
        &mut self,
        slot: usize,
        value: IndexSlotV1,
    ) -> Result<(), IndexStorageError> {
        let (page, offset) = self.slot_address(slot)?;
        self.ensure_page_valid(page)?;
        let old = self.decode_at(offset);
        let next_stats = self
            .physical_stats
            .transitioned(old.physical_kind(), value.physical_kind(), self.slot_count)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        value.encode(&mut encoded);
        // SAFETY: `offset` identifies one complete slot inside the owned
        // mapping and `&mut self` excludes simultaneous safe readers/writers.
        unsafe {
            ptr::copy_nonoverlapping(
                encoded.as_ptr(),
                self.data_mut_ptr().add(offset),
                INDEX_IMAGE_SLOT_SIZE,
            );
        }
        self.physical_stats = next_stats;
        self.page_states[page].store(PAGE_STATE_DIRTY, Ordering::Release);
        Ok(())
    }

    /// Sequentially emits a new, fully checksummed Index Image V1.
    ///
    /// Only a single 4 KiB stack buffer is used, regardless of index size.
    /// Process-local flags are always cleared before persistence. The
    /// destination should be an
    /// unpublished temporary image because an error can leave a prefix written.
    pub(crate) fn write_warm_image<W>(
        &self,
        writer: &mut W,
        binding: IndexImageBindingV1,
    ) -> Result<WarmImageStats, IndexStorageError>
    where
        W: Write + ?Sized,
    {
        if !binding.is_valid() {
            return Err(IndexStorageError::InvalidArgument(
                "warm index image binding must be non-zero",
            ));
        }
        self.ensure_image_usable(0)?;
        if self.physical_stats.masked != 0 {
            return Err(IndexStorageError::MaskedSlotsPresent {
                count: self.physical_stats.masked,
            });
        }
        let mut output = [0_u8; INDEX_IMAGE_PAGE_SIZE];
        for page in 0..self.page_count {
            self.ensure_page_valid(page)?;
            output.fill(0);
            let first_slot = page
                .checked_mul(INDEX_IMAGE_SLOTS_PER_PAGE)
                .ok_or(IndexStorageError::SizeOverflow)?;
            let valid_slots = self.valid_slots_in_page(page);
            encode_page_header(&mut output, page, first_slot, valid_slots, binding)?;

            for slot_in_page in 0..valid_slots {
                let source_offset = page
                    .checked_mul(INDEX_IMAGE_PAGE_SIZE)
                    .and_then(|base| base.checked_add(INDEX_IMAGE_PAGE_HEADER_SIZE))
                    .and_then(|base| {
                        base.checked_add(slot_in_page.checked_mul(INDEX_IMAGE_SLOT_SIZE)?)
                    })
                    .ok_or(IndexStorageError::SizeOverflow)?;
                let mut value = self.decode_at(source_offset);
                if value.physical_kind().is_masked() {
                    return Err(IndexStorageError::MaskedSlotsPresent { count: 1 });
                }
                value.flags &= !INDEX_RUNTIME_ONLY_FLAGS;
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

        Ok(WarmImageStats {
            pages_written: self.page_count,
            slots_written: self.slot_count,
            bytes_written: u64::try_from(self.image_len)
                .map_err(|_| IndexStorageError::SizeOverflow)?,
        })
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
                PAGE_STATE_VALIDATING => std::hint::spin_loop(),
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
            .expect("fixed mapped page has the Index Image V1 page size");
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
        .map_err(|reason| IndexStorageError::CorruptPage { page_index, reason })
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

    fn decode_at(&self, offset: usize) -> IndexSlotV1 {
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
        IndexSlotV1::decode(&encoded)
    }

    fn data_ptr(&self) -> *const u8 {
        // SAFETY: `data_offset + image_len` was checked against the backing
        // length during construction.
        unsafe { self.backing.as_ptr().add(self.data_offset) }
    }

    fn data_mut_ptr(&mut self) -> *mut u8 {
        // SAFETY: `data_offset + image_len` was checked against the backing
        // length during construction, and mutable access is exclusive.
        unsafe { self.backing.as_mut_ptr().add(self.data_offset) }
    }
}

// Shared access only validates or reads bytes. All byte mutation requires
// `&mut IndexStorage`, so these marker implementations preserve Rust's aliasing
// and data-race requirements.
unsafe impl Send for IndexStorage {}
unsafe impl Sync for IndexStorage {}

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
        IndexStorageError::Io(io::Error::other(
            "unable to allocate index page validation bitmap",
        ))
    })?;
    for _ in 0..page_count {
        states.push(AtomicU8::new(initial));
    }
    Ok(states.into_boxed_slice())
}

fn encode_page_header(
    page: &mut [u8; INDEX_IMAGE_PAGE_SIZE],
    page_index: usize,
    first_slot: usize,
    valid_slots: usize,
    binding: IndexImageBindingV1,
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

fn validate_page_header(
    page: &[u8; INDEX_IMAGE_PAGE_SIZE],
    expected_page_index: usize,
    expected_first_slot: usize,
    expected_valid_slots: usize,
    expected_binding: IndexImageBindingV1,
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

fn page_checksum(page: &[u8; INDEX_IMAGE_PAGE_SIZE]) -> u32 {
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

fn read_u64(input: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        input[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field is in bounds"),
    )
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
                IndexStorageError::Io(io::Error::other("unable to allocate anonymous index image"))
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

    fn as_ptr(&self) -> *const u8 {
        match self {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            Self::Mapping(mapping) => mapping.pointer.cast_const(),
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            Self::Heap(bytes) => bytes.as_ptr(),
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
    use std::fs::OpenOptions;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(0);

    const fn binding(generation: u64) -> IndexImageBindingV1 {
        IndexImageBindingV1 {
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
                "cache-rs-index-image-{}-{id}.tmp",
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

    fn sample_slot(seed: u64) -> IndexSlotV1 {
        IndexSlotV1 {
            hash: 0x0102_0304_0506_0708 ^ seed,
            location_raw: 0x1112_1314_1516_1718 ^ seed,
            seqno: 0x2122_2324_2526_2728 ^ seed,
            namespace_id: 0x3132_3334 ^ seed as u32,
            flags: (0x4142_4344 ^ seed as u32) & !INDEX_RUNTIME_ONLY_FLAGS,
        }
    }

    #[test]
    fn slot_codec_is_stable_little_endian() {
        let value = sample_slot(0);
        let mut encoded = [0_u8; INDEX_IMAGE_SLOT_SIZE];
        value.encode(&mut encoded);
        assert_eq!(&encoded[0..8], &value.hash.to_le_bytes());
        assert_eq!(&encoded[8..16], &value.location_raw.to_le_bytes());
        assert_eq!(&encoded[16..24], &value.seqno.to_le_bytes());
        assert_eq!(&encoded[24..28], &value.namespace_id.to_le_bytes());
        assert_eq!(&encoded[28..32], &value.flags.to_le_bytes());
        assert_eq!(IndexSlotV1::decode(&encoded), value);
        assert_eq!(IndexSlotV1::decode(&[0_u8; 32]), IndexSlotV1::EMPTY);
    }

    #[test]
    fn physical_stats_follow_constant_time_slot_transitions() {
        let mut storage = IndexStorage::anonymous(3).unwrap();
        assert_eq!(storage.physical_stats(), IndexPhysicalStats::default());

        storage.write_slot(0, sample_slot(1)).unwrap();
        assert_eq!(
            storage.physical_stats(),
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
                masked: 0,
            }
        );

        let mut deleted = sample_slot(2);
        deleted.location_raw |= INDEX_LOCATION_TOMBSTONE_BIT;
        storage.write_slot(0, deleted).unwrap();
        assert_eq!(
            storage.physical_stats(),
            IndexPhysicalStats {
                value: 0,
                deleted: 1,
                masked: 0,
            }
        );

        let mut masked = sample_slot(3);
        masked.flags |= INDEX_SLOT_FLAG_MASKED;
        storage.write_slot(0, masked).unwrap();
        assert_eq!(
            storage.physical_stats(),
            IndexPhysicalStats {
                value: 0,
                deleted: 0,
                masked: 1,
            }
        );
        assert!(matches!(
            storage.write_warm_image(&mut Vec::new(), binding(1)),
            Err(IndexStorageError::MaskedSlotsPresent { count: 1 })
        ));

        storage.write_slot(0, IndexSlotV1::EMPTY).unwrap();
        assert_eq!(storage.physical_stats(), IndexPhysicalStats::default());
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
                    masked: 0,
                }
            ),
            Err(IndexStorageError::InvalidPhysicalStats)
        ));
    }

    #[test]
    fn anonymous_warm_image_maps_private_without_rebuilding_slots() {
        const SLOT_COUNT: usize = INDEX_IMAGE_SLOTS_PER_PAGE + 3;
        const GENERATION: u64 = 47;
        const PREFIX: usize = INDEX_IMAGE_PAGE_SIZE;

        let mut source = IndexStorage::anonymous(SLOT_COUNT).unwrap();
        assert_eq!(source.read_slot(0).unwrap(), IndexSlotV1::EMPTY);
        let mut runtime_slot = sample_slot(1);
        runtime_slot.flags |= INDEX_RUNTIME_ONLY_FLAGS;
        source.write_slot(0, runtime_slot).unwrap();
        source
            .write_slot(INDEX_IMAGE_SLOTS_PER_PAGE + 2, sample_slot(2))
            .unwrap();

        let mut test_file = TestFile::create();
        test_file.file.set_len(PREFIX as u64).unwrap();
        test_file.file.seek(SeekFrom::Start(PREFIX as u64)).unwrap();
        let stats = source
            .write_warm_image(&mut test_file.file, binding(GENERATION))
            .unwrap();
        assert_eq!(stats.pages_written, 2);
        assert_eq!(stats.slots_written, SLOT_COUNT);
        assert_eq!(stats.bytes_written, (2 * INDEX_IMAGE_PAGE_SIZE) as u64);
        test_file.file.sync_all().unwrap();

        assert!(matches!(
            IndexStorage::map_private(
                &test_file.file,
                (PREFIX + 1) as u64,
                SLOT_COUNT,
                binding(GENERATION),
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
                SLOT_COUNT,
                IndexImageBindingV1 {
                    generation: 0,
                    image_tag: 1
                },
                source.physical_stats()
            ),
            Err(IndexStorageError::InvalidArgument(
                "mapped index image binding must be non-zero"
            ))
        ));

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
        assert_eq!(IndexSlotV1::decode(&encoded), expected);
    }

    #[test]
    fn corrupt_page_rejects_the_whole_image_when_touched() {
        const SLOT_COUNT: usize = INDEX_IMAGE_SLOTS_PER_PAGE * 2;
        const GENERATION: u64 = 91;

        let mut source = IndexStorage::anonymous(SLOT_COUNT).unwrap();
        source.write_slot(0, sample_slot(1)).unwrap();
        source
            .write_slot(INDEX_IMAGE_SLOTS_PER_PAGE, sample_slot(2))
            .unwrap();
        let mut image = Vec::new();
        source
            .write_warm_image(&mut image, binding(GENERATION))
            .unwrap();
        image[INDEX_IMAGE_PAGE_SIZE + INDEX_IMAGE_PAGE_HEADER_SIZE + 7] ^= 0x80;

        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();
        let recovered = IndexStorage::map_private(
            &test_file.file,
            0,
            SLOT_COUNT,
            binding(GENERATION),
            source.physical_stats(),
        )
        .unwrap();

        assert_eq!(recovered.read_slot(0).unwrap(), sample_slot(1));
        let error = recovered.read_slot(INDEX_IMAGE_SLOTS_PER_PAGE).unwrap_err();
        assert!(matches!(
            error,
            IndexStorageError::CorruptPage {
                page_index: 1,
                reason: CorruptPageReason::ChecksumMismatch { .. }
            }
        ));
        assert_eq!(
            recovered.page_validation_state(1).unwrap(),
            PageValidationState::Rejected
        );
        assert_eq!(
            recovered.page_validation_state(0).unwrap(),
            PageValidationState::Rejected
        );
        assert!(matches!(
            recovered.read_slot(0),
            Err(IndexStorageError::CorruptPage {
                page_index: 0,
                reason: CorruptPageReason::PreviouslyRejected
            })
        ));
    }

    #[test]
    fn image_binding_is_checked_on_first_page_touch() {
        let source = IndexStorage::anonymous(1).unwrap();
        assert!(matches!(
            source.write_warm_image(
                &mut Vec::new(),
                IndexImageBindingV1 {
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
            IndexImageBindingV1 {
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
