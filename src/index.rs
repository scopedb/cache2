//! Fixed-capacity in-memory hash index.
//!
//! The index deliberately stores only a 64-bit hash. Callers must validate the
//! full key in the record before returning a value. Two keys with the same hash
//! are therefore one identity here; a collision can cause a miss or eviction,
//! but must never cause a caller to return an unchecked value.

use std::collections::TryReserveError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

const REGION_BITS: u32 = 21;
const OFFSET_BITS: u32 = 22;
const RECORD_LEN_BITS: u32 = 20;

const REGION_SHIFT: u32 = 0;
const OFFSET_SHIFT: u32 = REGION_SHIFT + REGION_BITS;
const RECORD_LEN_SHIFT: u32 = OFFSET_SHIFT + OFFSET_BITS;
const TOMBSTONE_SHIFT: u32 = RECORD_LEN_SHIFT + RECORD_LEN_BITS;

const REGION_MASK: u64 = (1_u64 << REGION_BITS) - 1;
const OFFSET_MASK: u64 = (1_u64 << OFFSET_BITS) - 1;
const RECORD_LEN_MASK: u64 = (1_u64 << RECORD_LEN_BITS) - 1;

const OFFSET_ALIGNMENT: u32 = 8;
const RECORD_LEN_ALIGNMENT: u32 = 32;

pub(crate) const MAX_REGION_ID: u32 = REGION_MASK as u32;
pub(crate) const MAX_REGION_OFFSET: u32 = (OFFSET_MASK as u32) * OFFSET_ALIGNMENT;
pub(crate) const MAX_RECORD_LEN: u32 = (RECORD_LEN_MASK as u32) * RECORD_LEN_ALIGNMENT;
/// Format V1 checkpoints encode the entry count as `u32`. Keep a conservative
/// 256M-slot runtime ceiling (8 GiB at 32 bytes/slot), which supports well over
/// 100M live entries at the recommended <= 80% load factor.
pub(crate) const MAX_INDEX_SLOTS: usize = 256 * 1024 * 1024;
pub(crate) const INDEX_FLAG_SECOND_CHANCE_PENDING: u32 = 1;
pub(crate) const INDEX_FLAG_SECOND_CHANCE_USED: u32 = 1 << 1;

/// A record location packed into one machine word.
///
/// Offset and record length are stored in 8-byte and 32-byte units,
/// respectively.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct PackedLocation(u64);

impl PackedLocation {
    pub(crate) fn new(
        region_id: u32,
        offset: u32,
        record_len: u32,
        tombstone: bool,
    ) -> Result<Self, PackedLocationError> {
        if region_id > MAX_REGION_ID {
            return Err(PackedLocationError::RegionOutOfRange);
        }
        if offset % OFFSET_ALIGNMENT != 0 {
            return Err(PackedLocationError::OffsetUnaligned);
        }
        if offset > MAX_REGION_OFFSET {
            return Err(PackedLocationError::OffsetOutOfRange);
        }
        if record_len == 0 {
            return Err(PackedLocationError::RecordLengthZero);
        }
        if record_len % RECORD_LEN_ALIGNMENT != 0 {
            return Err(PackedLocationError::RecordLengthUnaligned);
        }
        if record_len > MAX_RECORD_LEN {
            return Err(PackedLocationError::RecordLengthOutOfRange);
        }

        let offset_units = u64::from(offset / OFFSET_ALIGNMENT);
        let record_len_units = u64::from(record_len / RECORD_LEN_ALIGNMENT);
        let tombstone_bit = u64::from(tombstone);

        Ok(Self(
            u64::from(region_id)
                | (offset_units << OFFSET_SHIFT)
                | (record_len_units << RECORD_LEN_SHIFT)
                | (tombstone_bit << TOMBSTONE_SHIFT),
        ))
    }

    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Decodes a persisted packed location and rejects bit patterns which do
    /// not describe a record. Field widths and alignments are inherent in the
    /// packed representation; the only otherwise representable invalid value
    /// is a zero record length.
    pub(crate) fn try_from_raw(raw: u64) -> Result<Self, PackedLocationError> {
        let location = Self::from_raw(raw);
        Self::new(
            location.region_id(),
            location.offset(),
            location.record_len(),
            location.tombstone(),
        )
    }

    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    pub(crate) const fn region_id(self) -> u32 {
        ((self.0 >> REGION_SHIFT) & REGION_MASK) as u32
    }

    pub(crate) const fn offset(self) -> u32 {
        (((self.0 >> OFFSET_SHIFT) & OFFSET_MASK) as u32) * OFFSET_ALIGNMENT
    }

    pub(crate) const fn record_len(self) -> u32 {
        (((self.0 >> RECORD_LEN_SHIFT) & RECORD_LEN_MASK) as u32) * RECORD_LEN_ALIGNMENT
    }

    pub(crate) const fn tombstone(self) -> bool {
        (self.0 & (1_u64 << TOMBSTONE_SHIFT)) != 0
    }

    pub(crate) const fn is_tombstone(self) -> bool {
        self.tombstone()
    }
}

impl fmt::Debug for PackedLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackedLocation")
            .field("region_id", &self.region_id())
            .field("offset", &self.offset())
            .field("record_len", &self.record_len())
            .field("tombstone", &self.tombstone())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PackedLocationError {
    RegionOutOfRange,
    OffsetUnaligned,
    OffsetOutOfRange,
    RecordLengthZero,
    RecordLengthUnaligned,
    RecordLengthOutOfRange,
}

impl fmt::Display for PackedLocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::RegionOutOfRange => "region id does not fit in 21 bits",
            Self::OffsetUnaligned => "region offset is not 8-byte aligned",
            Self::OffsetOutOfRange => "region offset does not fit in 22 units",
            Self::RecordLengthZero => "record length must be non-zero",
            Self::RecordLengthUnaligned => "record length is not 32-byte aligned",
            Self::RecordLengthOutOfRange => "record length does not fit in 20 units",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for PackedLocationError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) location: PackedLocation,
    pub(crate) seqno: u64,
    pub(crate) namespace_id: u32,
    pub(crate) flags: u32,
}

/// Stable fields emitted to, and restored from, an index checkpoint.
///
/// The raw packed location is kept here so checkpoint codecs need not depend
/// on the in-memory `PackedLocation` representation. Restoration validates it
/// before publishing the entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexSnapshotEntry {
    pub(crate) hash: u64,
    pub(crate) location_raw: u64,
    pub(crate) seqno: u64,
    pub(crate) namespace_id: u32,
    pub(crate) flags: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IndexSnapshotError {
    SequenceZero,
    InvalidLocation(PackedLocationError),
    PhysicalSlotOutOfRange,
    PhysicalSlotWrongShard,
    PhysicalSlotOutsideProbeWindow,
    PhysicalSlotOccupied,
}

impl fmt::Display for IndexSnapshotError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SequenceZero => formatter.write_str("checkpoint index sequence must be non-zero"),
            Self::InvalidLocation(error) => {
                write!(formatter, "invalid checkpoint location: {error}")
            }
            Self::PhysicalSlotOutOfRange => {
                formatter.write_str("checkpoint physical index slot is out of range")
            }
            Self::PhysicalSlotWrongShard => {
                formatter.write_str("checkpoint physical index slot belongs to the wrong shard")
            }
            Self::PhysicalSlotOutsideProbeWindow => formatter
                .write_str("checkpoint physical index slot is outside the hash probe window"),
            Self::PhysicalSlotOccupied => {
                formatter.write_str("checkpoint physical index slot is duplicated")
            }
        }
    }
}

impl std::error::Error for IndexSnapshotError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::SequenceZero
            | Self::PhysicalSlotOutOfRange
            | Self::PhysicalSlotWrongShard
            | Self::PhysicalSlotOutsideProbeWindow
            | Self::PhysicalSlotOccupied => None,
            Self::InvalidLocation(error) => Some(error),
        }
    }
}

/// Result of trying to publish a version into the index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ApplyResult {
    /// Whether the supplied version was installed.
    pub(crate) applied: bool,
    /// The entry replaced by an install, or the current entry which rejected
    /// an older install. It is `None` for insertion into an empty/deleted slot
    /// and for a supplied version older than `min_seqno`.
    pub(crate) previous: Option<IndexEntry>,
}

impl ApplyResult {
    const fn applied(previous: Option<IndexEntry>) -> Self {
        Self {
            applied: true,
            previous,
        }
    }

    const fn ignored(current: Option<IndexEntry>) -> Self {
        Self {
            applied: false,
            previous: current,
        }
    }
}

/// One 32-byte logical slot. A zero sequence number is reserved for table
/// state, so a live location may use any packed value, including zero or
/// `u64::MAX`.
#[derive(Clone, Copy, Debug, Default)]
struct Slot {
    hash: u64,
    location: u64,
    seqno: u64,
    namespace_id: u32,
    flags: u32,
}

impl Slot {
    const EMPTY_LOCATION: u64 = 0;
    const DELETED_LOCATION: u64 = u64::MAX;

    const fn is_empty(self) -> bool {
        self.seqno == 0 && self.location == Self::EMPTY_LOCATION
    }

    const fn is_deleted(self) -> bool {
        self.seqno == 0 && self.location == Self::DELETED_LOCATION
    }

    const fn is_live(self) -> bool {
        self.seqno != 0
    }

    const fn deleted() -> Self {
        Self {
            hash: 0,
            location: Self::DELETED_LOCATION,
            seqno: 0,
            namespace_id: 0,
            flags: 0,
        }
    }

    const fn entry(self) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::from_raw(self.location),
            seqno: self.seqno,
            namespace_id: self.namespace_id,
            flags: self.flags,
        }
    }
}

const MAX_PROBES: usize = 64;
pub(crate) const MAX_INDEX_SHARDS: usize = 4096;

/// A bounded open-addressing table.
///
/// `len()` counts physically live slots. Entries below a caller's monotonically
/// increasing `min_seqno` are logically absent and are reclaimed lazily by
/// insertion or `evict_region`.
pub(crate) struct CompactIndex {
    slots: Vec<Slot>,
    len: usize,
}

impl CompactIndex {
    pub(crate) const fn allocation_bytes(slot_count: usize) -> Option<usize> {
        slot_count.checked_mul(std::mem::size_of::<Slot>())
    }

    pub(crate) fn try_new(slot_count: usize) -> Result<Self, TryReserveError> {
        assert!(slot_count >= 8, "compact index needs at least 8 slots");
        let mut slots = Vec::new();
        slots.try_reserve_exact(slot_count)?;
        slots.resize(slot_count, Slot::default());
        Ok(Self { slots, len: 0 })
    }

    #[cfg(test)]
    pub(crate) fn new(slot_count: usize) -> Self {
        Self::try_new(slot_count).expect("test index allocation must succeed")
    }

    #[cfg(test)]
    pub(crate) const fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn clear(&mut self) {
        self.slots.fill(Slot::default());
        self.len = 0;
    }

    /// Visits physically live entries which remain visible at `min_seqno`.
    /// Tombstones are included because omitting them from a checkpoint could
    /// resurrect an older value during incremental recovery.
    ///
    /// Entries are passed to `visit` one slot at a time; this method never
    /// allocates a collection proportional to the index size.
    #[cfg(test)]
    fn try_for_each_snapshot_entry<E>(
        &self,
        min_seqno: u64,
        visit: &mut impl FnMut(IndexSnapshotEntry) -> Result<(), E>,
    ) -> Result<usize, E> {
        self.try_for_each_snapshot_entry_if(|entry| entry.seqno >= min_seqno, &mut |_, entry| {
            visit(entry)
        })
    }

    fn try_for_each_snapshot_entry_if<E>(
        &self,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
        visit: &mut impl FnMut(usize, IndexSnapshotEntry) -> Result<(), E>,
    ) -> Result<usize, E> {
        let mut visited = 0;
        for (physical_slot, slot) in self.slots.iter().enumerate() {
            if !slot.is_live() || !is_visible(slot.entry()) {
                continue;
            }
            visit(
                physical_slot,
                IndexSnapshotEntry {
                    hash: slot.hash,
                    location_raw: slot.location,
                    seqno: slot.seqno,
                    namespace_id: slot.namespace_id,
                    flags: slot.flags,
                },
            )?;
            visited += 1;
        }
        Ok(visited)
    }

    /// Publishes one already integrity-checked checkpoint entry.
    ///
    /// This does not clear existing entries. A full checkpoint replacement
    /// must call `clear` before restoring the first entry. Versions below
    /// `min_seqno` are logically cleared and are ignored by `apply_if_newer`.
    #[cfg(test)]
    fn restore_snapshot_entry(
        &mut self,
        entry: IndexSnapshotEntry,
        min_seqno: u64,
    ) -> Result<ApplyResult, IndexSnapshotError> {
        if entry.seqno == 0 {
            return Err(IndexSnapshotError::SequenceZero);
        }
        let location = PackedLocation::try_from_raw(entry.location_raw)
            .map_err(IndexSnapshotError::InvalidLocation)?;
        Ok(self.apply_if_newer_with_metadata(
            entry.hash,
            location,
            entry.seqno,
            min_seqno,
            entry.namespace_id,
            entry.flags,
        ))
    }

    #[cfg(test)]
    pub(crate) const fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> usize {
        self.slots.len()
    }

    #[cfg(test)]
    pub(crate) fn get(&self, hash: u64, min_seqno: u64) -> Option<IndexEntry> {
        self.get_if(hash, |entry| entry.seqno >= min_seqno)
    }

    fn get_if(
        &self,
        hash: u64,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
    ) -> Option<IndexEntry> {
        let start = self.start_slot(hash);
        for step in 0..self.probe_limit() {
            let slot = self.slots[self.probe_slot(start, step)];
            if slot.is_empty() {
                return None;
            }
            if slot.is_live() && slot.hash == hash {
                let entry = slot.entry();
                return is_visible(entry).then_some(entry);
            }
        }
        None
    }

    /// Installs `location` only if `seqno` is newer than the current version.
    ///
    /// If the probe window is full, its starting slot is replaced. Losing an
    /// index entry is valid cache eviction and keeps both latency and memory
    /// strictly bounded.
    #[cfg(test)]
    pub(crate) fn apply_if_newer(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        min_seqno: u64,
    ) -> ApplyResult {
        self.apply_if_newer_with_metadata(hash, location, seqno, min_seqno, 0, 0)
    }

    /// Installs a version together with the cache-policy metadata which must
    /// follow it through replacement, eviction, and checkpoints.
    #[cfg(test)]
    pub(crate) fn apply_if_newer_with_metadata(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        min_seqno: u64,
        namespace_id: u32,
        flags: u32,
    ) -> ApplyResult {
        self.apply_if_newer_with_visibility(hash, location, seqno, namespace_id, flags, |entry| {
            entry.seqno >= min_seqno
        })
    }

    fn apply_if_newer_with_visibility(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
        flags: u32,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
    ) -> ApplyResult {
        let supplied = IndexEntry {
            location,
            seqno,
            namespace_id,
            flags,
        };
        if seqno == 0 || !is_visible(supplied) {
            return ApplyResult::ignored(None);
        }

        let start = self.start_slot(hash);
        let mut reusable: Option<(usize, Option<IndexEntry>)> = None;

        for step in 0..self.probe_limit() {
            let index = self.probe_slot(start, step);
            let slot = self.slots[index];

            if slot.is_empty() {
                let (target, previous) = reusable.unwrap_or((index, None));
                self.install(target, hash, location, seqno, namespace_id, flags);
                return ApplyResult::applied(previous);
            }

            if slot.is_deleted() {
                reusable.get_or_insert((index, None));
                continue;
            }

            // A zero-sequence slot with any other marker is not produced by
            // this implementation. Treat it as deleted rather than allowing a
            // malformed marker to make probing unbounded.
            if !slot.is_live() {
                reusable.get_or_insert((index, None));
                continue;
            }

            let current = slot.entry();
            if slot.hash == hash {
                if is_visible(current) && seqno <= slot.seqno {
                    return ApplyResult::ignored(Some(current));
                }

                self.install(index, hash, location, seqno, namespace_id, flags);
                return ApplyResult::applied(is_visible(current).then_some(current));
            }

            if !is_visible(current) && reusable.is_none() {
                reusable = Some((index, None));
            }
        }

        if let Some((target, previous)) = reusable {
            self.install(target, hash, location, seqno, namespace_id, flags);
            return ApplyResult::applied(previous);
        }

        let previous = self.slots[start].entry();
        self.install(start, hash, location, seqno, namespace_id, flags);
        ApplyResult::applied(Some(previous))
    }

    fn update_second_chance_if_visible(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
        marked: bool,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
    ) -> bool {
        let start = self.start_slot(hash);
        for step in 0..self.probe_limit() {
            let index = self.probe_slot(start, step);
            let slot = self.slots[index];
            if slot.is_empty() {
                return false;
            }
            if slot.is_live() && slot.hash == hash {
                if !is_visible(slot.entry()) {
                    return false;
                }
                if slot.location != location.raw()
                    || slot.seqno != seqno
                    || slot.namespace_id != namespace_id
                {
                    return false;
                }
                let updated = if marked {
                    slot.flags | INDEX_FLAG_SECOND_CHANCE_PENDING
                } else {
                    slot.flags & !INDEX_FLAG_SECOND_CHANCE_PENDING
                };
                if updated == slot.flags {
                    return false;
                }
                self.slots[index].flags = updated;
                return true;
            }
        }
        false
    }

    /// Removes an entry only if all identity and version fields still match.
    /// This prevents a late read or reclaim operation from deleting a newer
    /// update.
    #[cfg(test)]
    pub(crate) fn remove_if(&mut self, hash: u64, location: PackedLocation, seqno: u64) -> bool {
        self.remove_if_entry(hash, location, seqno).is_some()
    }

    /// Compare-and-delete variant which returns the exact removed metadata so
    /// callers can update bounded accounting without a second index lookup.
    #[cfg(test)]
    pub(crate) fn remove_if_entry(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
    ) -> Option<IndexEntry> {
        self.remove_if_entry_visible(hash, location, seqno, |_| true)
    }

    fn remove_if_entry_visible(
        &mut self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
    ) -> Option<IndexEntry> {
        let start = self.start_slot(hash);
        for step in 0..self.probe_limit() {
            let index = self.probe_slot(start, step);
            let slot = self.slots[index];

            if slot.is_empty() {
                return None;
            }
            if slot.is_live() && slot.hash == hash {
                if !is_visible(slot.entry()) {
                    return None;
                }
                if slot.location != location.raw() || slot.seqno != seqno {
                    return None;
                }
                self.slots[index] = Slot::deleted();
                self.len -= 1;
                return Some(slot.entry());
            }
        }
        None
    }

    /// Drops entries in `region_id` and opportunistically purges entries made
    /// obsolete by a logical clear. Returns the number of removed slots.
    #[cfg(test)]
    pub(crate) fn evict_region(&mut self, region_id: u32, min_seqno: u64) -> usize {
        self.evict_region_with(region_id, min_seqno, |_| {})
    }

    /// Streaming eviction variant. `visit` runs once for each removed entry
    /// while this compact index is exclusively borrowed; it must not call back
    /// into the same index.
    #[cfg(test)]
    pub(crate) fn evict_region_with(
        &mut self,
        region_id: u32,
        min_seqno: u64,
        mut visit: impl FnMut(IndexEntry),
    ) -> usize {
        self.evict_region_with_visibility(
            region_id,
            |entry| entry.seqno >= min_seqno,
            &mut |entry, _| {
                visit(entry);
            },
        )
    }

    fn evict_region_with_visibility(
        &mut self,
        region_id: u32,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
        visit: &mut impl FnMut(IndexEntry, bool),
    ) -> usize {
        let mut evicted = 0;
        for slot in &mut self.slots {
            if !slot.is_live() {
                continue;
            }
            let entry = slot.entry();
            let visible = is_visible(entry);
            if !visible || entry.location.region_id() == region_id {
                evicted += 1;
                *slot = Slot::deleted();
                visit(entry, visible);
            }
        }
        self.len -= evicted;
        evicted
    }

    fn install(
        &mut self,
        index: usize,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
        flags: u32,
    ) {
        if !self.slots[index].is_live() {
            self.len += 1;
        }
        self.slots[index] = Slot {
            hash,
            location: location.raw(),
            seqno,
            namespace_id,
            flags,
        };
    }

    fn restore_exact_if_visible(
        &mut self,
        physical_slot: usize,
        hash: u64,
        entry: IndexEntry,
        mut is_visible: impl FnMut(IndexEntry) -> bool,
    ) -> Result<bool, IndexSnapshotError> {
        if !is_visible(entry) {
            return Ok(false);
        }
        if physical_slot >= self.slots.len() {
            return Err(IndexSnapshotError::PhysicalSlotOutOfRange);
        }
        let start = self.start_slot(hash);
        let step = if physical_slot >= start {
            physical_slot - start
        } else {
            self.slots.len() - start + physical_slot
        };
        if step >= self.probe_limit() {
            return Err(IndexSnapshotError::PhysicalSlotOutsideProbeWindow);
        }
        let target = self
            .slots
            .get(physical_slot)
            .copied()
            .ok_or(IndexSnapshotError::PhysicalSlotOutOfRange)?;
        if !target.is_empty() && !target.is_deleted() {
            return Err(IndexSnapshotError::PhysicalSlotOccupied);
        }

        // A checkpoint stores only live entries, not the historical deleted
        // slots which made a later probe position reachable. Recreate the
        // minimum safe probe chain before installing the persisted target.
        // Extra tombstones affect placement only; they cannot make a missing
        // key visible or change an installed value.
        for probe_step in 0..step {
            let probe = self.probe_slot(start, probe_step);
            if self.slots[probe].is_empty() {
                self.slots[probe] = Slot::deleted();
            }
        }
        self.install(
            physical_slot,
            hash,
            entry.location,
            entry.seqno,
            entry.namespace_id,
            entry.flags,
        );
        Ok(true)
    }

    fn start_slot(&self, hash: u64) -> usize {
        (hash % self.slots.len() as u64) as usize
    }

    fn probe_limit(&self) -> usize {
        self.slots.len().min(MAX_PROBES)
    }

    fn probe_slot(&self, start: usize, step: usize) -> usize {
        let index = start + step;
        if index >= self.slots.len() {
            index - self.slots.len()
        } else {
            index
        }
    }
}

/// Whether one Region incarnation may own visible index entries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RegionGeneration {
    Free,
    Allocated { created_seqno: u64 },
}

/// Counts removed when a Region generation is invalidated in O(1).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RegionIndexCounts {
    pub(crate) entries: usize,
    pub(crate) values: usize,
}

struct RegionVisibility {
    generation: RegionGeneration,
    entries: AtomicUsize,
    values: AtomicUsize,
}

impl RegionVisibility {
    fn new(generation: RegionGeneration) -> Self {
        Self {
            generation,
            entries: AtomicUsize::new(0),
            values: AtomicUsize::new(0),
        }
    }
}

struct Visibility {
    min_seqno: u64,
    regions: Vec<RegionVisibility>,
}

impl Visibility {
    fn is_visible(&self, entry: IndexEntry, caller_min_seqno: u64) -> bool {
        if entry.seqno < self.min_seqno.max(caller_min_seqno) {
            return false;
        }
        let Some(region) = self.regions.get(entry.location.region_id() as usize) else {
            return false;
        };
        match region.generation {
            RegionGeneration::Free => false,
            RegionGeneration::Allocated { created_seqno } => entry.seqno >= created_seqno,
        }
    }
}

/// Fixed-capacity index split into independently locked shards.
///
/// Physical slots are reclaimed lazily. Logical visibility and counts are
/// independent of physical occupancy, so clearing the cache and reusing one
/// Region never require a scan proportional to the index capacity.
pub(crate) struct ShardedIndex {
    shards: Vec<RwLock<CompactIndex>>,
    slot_count: usize,
    visibility: RwLock<Visibility>,
    entries: AtomicUsize,
    values: AtomicUsize,
    #[cfg(test)]
    snapshot_scans: AtomicUsize,
}

impl ShardedIndex {
    pub(crate) fn allocation_bytes(slot_count: usize, region_count: usize) -> Option<usize> {
        CompactIndex::allocation_bytes(slot_count)?
            .checked_add(
                index_shard_count(slot_count)
                    .checked_mul(std::mem::size_of::<RwLock<CompactIndex>>())?,
            )?
            .checked_add(region_count.checked_mul(std::mem::size_of::<RegionVisibility>())?)
    }

    pub(crate) fn try_new(slot_count: usize, region_count: usize) -> Result<Self, TryReserveError> {
        assert!(slot_count >= 8, "sharded index needs at least 8 slots");
        let shard_count = index_shard_count(slot_count);
        let base = slot_count / shard_count;
        let remainder = slot_count % shard_count;
        let mut shards = Vec::new();
        shards.try_reserve_exact(shard_count)?;
        for shard in 0..shard_count {
            let slots = base + usize::from(shard < remainder);
            shards.push(RwLock::new(CompactIndex::try_new(slots)?));
        }
        let mut regions = Vec::new();
        regions.try_reserve_exact(region_count)?;
        regions.extend((0..region_count).map(|_| RegionVisibility::new(RegionGeneration::Free)));
        Ok(Self {
            shards,
            slot_count,
            visibility: RwLock::new(Visibility {
                min_seqno: 0,
                regions,
            }),
            entries: AtomicUsize::new(0),
            values: AtomicUsize::new(0),
            #[cfg(test)]
            snapshot_scans: AtomicUsize::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn new(slot_count: usize) -> Self {
        let index =
            Self::try_new(slot_count, 64).expect("test sharded-index allocation must succeed");
        let generations = vec![RegionGeneration::Allocated { created_seqno: 0 }; 64];
        assert!(index.reset_visibility_for_restore(0, generations.into_iter()));
        index
    }

    /// Resets all Region generations before rebuilding an index from recovery.
    /// Physical slots and counters are cleared so no prior in-memory state can
    /// become visible under the supplied generations.
    pub(crate) fn reset_visibility_for_restore<I>(&self, min_seqno: u64, generations: I) -> bool
    where
        I: ExactSizeIterator<Item = RegionGeneration>,
    {
        let mut visibility = write_unpoisoned(&self.visibility);
        if generations.len() != visibility.regions.len() {
            return false;
        }
        for shard in &self.shards {
            write_unpoisoned(shard).clear();
        }
        visibility.min_seqno = min_seqno;
        for (region, generation) in visibility.regions.iter_mut().zip(generations) {
            region.generation = generation;
            region.entries.store(0, Ordering::Relaxed);
            region.values.store(0, Ordering::Relaxed);
        }
        self.entries.store(0, Ordering::Relaxed);
        self.values.store(0, Ordering::Relaxed);
        true
    }

    /// Advances the global clear floor without visiting physical index slots.
    /// Region counters are reset rather than the much larger slot array.
    pub(crate) fn advance_clear_floor(&self, min_seqno: u64) {
        let mut visibility = write_unpoisoned(&self.visibility);
        visibility.min_seqno = visibility.min_seqno.max(min_seqno);
        for region in &visibility.regions {
            region.entries.store(0, Ordering::Relaxed);
            region.values.store(0, Ordering::Relaxed);
        }
        self.entries.store(0, Ordering::Relaxed);
        self.values.store(0, Ordering::Relaxed);
    }

    /// Changes one Region generation and invalidates everything charged to its
    /// previous incarnation. This is independent of the number of index slots.
    pub(crate) fn invalidate_region_generation(
        &self,
        region_id: u32,
        generation: RegionGeneration,
    ) -> Option<RegionIndexCounts> {
        let mut visibility = write_unpoisoned(&self.visibility);
        let region = visibility.regions.get_mut(region_id as usize)?;
        let counts = RegionIndexCounts {
            entries: region.entries.swap(0, Ordering::Relaxed),
            values: region.values.swap(0, Ordering::Relaxed),
        };
        subtract_exact(&self.entries, counts.entries);
        subtract_exact(&self.values, counts.values);
        region.generation = generation;
        Some(counts)
    }

    pub(crate) fn get(&self, hash: u64, min_seqno: u64) -> Option<IndexEntry> {
        let visibility = read_unpoisoned(&self.visibility);
        read_unpoisoned(&self.shards[self.shard_for(hash)])
            .get_if(hash, |entry| visibility.is_visible(entry, min_seqno))
    }

    #[cfg(test)]
    pub(crate) fn apply_if_newer(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        min_seqno: u64,
    ) -> ApplyResult {
        self.apply_if_newer_with_metadata(hash, location, seqno, min_seqno, 0, 0)
    }

    pub(crate) fn apply_if_newer_with_metadata(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        min_seqno: u64,
        namespace_id: u32,
        flags: u32,
    ) -> ApplyResult {
        let visibility = read_unpoisoned(&self.visibility);
        let result = write_unpoisoned(&self.shards[self.shard_for(hash)])
            .apply_if_newer_with_visibility(hash, location, seqno, namespace_id, flags, |entry| {
                visibility.is_visible(entry, min_seqno)
            });
        if result.applied {
            if let Some(previous) = result.previous {
                self.subtract_entry(&visibility, previous);
            }
            self.add_entry(
                &visibility,
                IndexEntry {
                    location,
                    seqno,
                    namespace_id,
                    flags,
                },
            );
        }
        result
    }

    pub(crate) fn mark_second_chance_if(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
    ) -> bool {
        self.update_second_chance_if(hash, location, seqno, namespace_id, true)
    }

    pub(crate) fn clear_second_chance_if(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
    ) -> bool {
        self.update_second_chance_if(hash, location, seqno, namespace_id, false)
    }

    fn update_second_chance_if(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
        namespace_id: u32,
        marked: bool,
    ) -> bool {
        let visibility = read_unpoisoned(&self.visibility);
        write_unpoisoned(&self.shards[self.shard_for(hash)]).update_second_chance_if_visible(
            hash,
            location,
            seqno,
            namespace_id,
            marked,
            |entry| visibility.is_visible(entry, 0),
        )
    }

    pub(crate) fn remove_if_entry(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
    ) -> Option<IndexEntry> {
        let visibility = read_unpoisoned(&self.visibility);
        let removed = write_unpoisoned(&self.shards[self.shard_for(hash)]).remove_if_entry_visible(
            hash,
            location,
            seqno,
            |entry| visibility.is_visible(entry, 0),
        );
        if let Some(entry) = removed {
            self.subtract_entry(&visibility, entry);
        }
        removed
    }

    /// Removes an exact physical slot without consulting visibility or
    /// changing logical counters. Callers use this only after advancing a
    /// Region floor, when a victim-local scrub encounters an already-hidden
    /// record from the previous incarnation.
    pub(crate) fn remove_physical_if_entry(
        &self,
        hash: u64,
        location: PackedLocation,
        seqno: u64,
    ) -> Option<IndexEntry> {
        write_unpoisoned(&self.shards[self.shard_for(hash)]).remove_if_entry_visible(
            hash,
            location,
            seqno,
            |_| true,
        )
    }

    /// Physical clear retained for initialization and corruption fallback.
    pub(crate) fn clear(&self) {
        let visibility = write_unpoisoned(&self.visibility);
        for shard in &self.shards {
            write_unpoisoned(shard).clear();
        }
        for region in &visibility.regions {
            region.entries.store(0, Ordering::Relaxed);
            region.values.store(0, Ordering::Relaxed);
        }
        self.entries.store(0, Ordering::Relaxed);
        self.values.store(0, Ordering::Relaxed);
    }

    pub(crate) fn try_for_each_snapshot_entry<E>(
        &self,
        min_seqno: u64,
        mut visit: impl FnMut(u32, IndexSnapshotEntry) -> Result<(), E>,
    ) -> Result<usize, E> {
        #[cfg(test)]
        self.snapshot_scans.fetch_add(1, Ordering::Relaxed);
        let visibility = read_unpoisoned(&self.visibility);
        let mut visited = 0;
        let mut shard_base = 0_usize;
        for shard in &self.shards {
            visited += read_unpoisoned(shard).try_for_each_snapshot_entry_if(
                |entry| visibility.is_visible(entry, min_seqno),
                &mut |local_slot, entry| {
                    let physical_slot = u32::try_from(shard_base + local_slot)
                        .expect("configured index capacity fits the checkpoint slot field");
                    visit(physical_slot, entry)
                },
            )?;
            shard_base += read_unpoisoned(shard).slots.len();
        }
        Ok(visited)
    }

    #[cfg(test)]
    pub(crate) fn snapshot_scan_count(&self) -> usize {
        self.snapshot_scans.load(Ordering::Relaxed)
    }

    pub(crate) fn restore_snapshot_entry_exact(
        &self,
        physical_slot: u32,
        entry: IndexSnapshotEntry,
        min_seqno: u64,
    ) -> Result<ApplyResult, IndexSnapshotError> {
        if entry.seqno == 0 {
            return Err(IndexSnapshotError::SequenceZero);
        }
        let location = PackedLocation::try_from_raw(entry.location_raw)
            .map_err(IndexSnapshotError::InvalidLocation)?;
        let physical_slot = physical_slot as usize;
        let (shard_id, local_slot) = self
            .physical_slot_parts(physical_slot)
            .ok_or(IndexSnapshotError::PhysicalSlotOutOfRange)?;
        if shard_id != self.shard_for(entry.hash) {
            return Err(IndexSnapshotError::PhysicalSlotWrongShard);
        }
        let supplied = IndexEntry {
            location,
            seqno: entry.seqno,
            namespace_id: entry.namespace_id,
            flags: entry.flags,
        };
        let visibility = read_unpoisoned(&self.visibility);
        let applied = write_unpoisoned(&self.shards[shard_id]).restore_exact_if_visible(
            local_slot,
            entry.hash,
            supplied,
            |entry| visibility.is_visible(entry, min_seqno),
        )?;
        if applied {
            self.add_entry(&visibility, supplied);
            Ok(ApplyResult::applied(None))
        } else {
            Ok(ApplyResult::ignored(None))
        }
    }

    pub(crate) fn restore_snapshot_entry(
        &self,
        entry: IndexSnapshotEntry,
        min_seqno: u64,
    ) -> Result<ApplyResult, IndexSnapshotError> {
        if entry.seqno == 0 {
            return Err(IndexSnapshotError::SequenceZero);
        }
        let location = PackedLocation::try_from_raw(entry.location_raw)
            .map_err(IndexSnapshotError::InvalidLocation)?;
        Ok(self.apply_if_newer_with_metadata(
            entry.hash,
            location,
            entry.seqno,
            min_seqno,
            entry.namespace_id,
            entry.flags,
        ))
    }

    #[cfg(test)]
    pub(crate) fn evict_region(&self, region_id: u32, min_seqno: u64) -> usize {
        self.evict_region_with(region_id, min_seqno, |_| {})
    }

    /// Full-table physical purge retained as a corruption-recovery fallback.
    pub(crate) fn evict_region_with(
        &self,
        region_id: u32,
        min_seqno: u64,
        mut visit: impl FnMut(IndexEntry),
    ) -> usize {
        let visibility = read_unpoisoned(&self.visibility);
        self.shards
            .iter()
            .map(|shard| {
                write_unpoisoned(shard).evict_region_with_visibility(
                    region_id,
                    |entry| visibility.is_visible(entry, min_seqno),
                    &mut |entry, was_visible| {
                        if was_visible {
                            self.subtract_entry(&visibility, entry);
                            visit(entry);
                        }
                    },
                )
            })
            .sum()
    }

    pub(crate) fn entry_len(&self) -> usize {
        self.entries.load(Ordering::Relaxed)
    }

    pub(crate) const fn capacity(&self) -> usize {
        self.slot_count
    }

    pub(crate) fn value_len(&self, min_seqno: u64) -> usize {
        let visibility = read_unpoisoned(&self.visibility);
        if min_seqno <= visibility.min_seqno {
            return self.values.load(Ordering::Relaxed);
        }
        self.shards
            .iter()
            .map(|shard| {
                read_unpoisoned(shard)
                    .slots
                    .iter()
                    .filter(|slot| {
                        slot.is_live()
                            && !slot.entry().location.is_tombstone()
                            && visibility.is_visible(slot.entry(), min_seqno)
                    })
                    .count()
            })
            .sum()
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    fn add_entry(&self, visibility: &Visibility, entry: IndexEntry) {
        let region = &visibility.regions[entry.location.region_id() as usize];
        region.entries.fetch_add(1, Ordering::Relaxed);
        self.entries.fetch_add(1, Ordering::Relaxed);
        if !entry.location.is_tombstone() {
            region.values.fetch_add(1, Ordering::Relaxed);
            self.values.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn subtract_entry(&self, visibility: &Visibility, entry: IndexEntry) {
        let region = &visibility.regions[entry.location.region_id() as usize];
        subtract_exact(&region.entries, 1);
        subtract_exact(&self.entries, 1);
        if !entry.location.is_tombstone() {
            subtract_exact(&region.values, 1);
            subtract_exact(&self.values, 1);
        }
    }

    fn shard_for(&self, hash: u64) -> usize {
        debug_assert!(self.shards.len().is_power_of_two());
        if self.shards.len() == 1 {
            return 0;
        }
        let shard_bits = self.shards.len().trailing_zeros();
        (mix_for_shard(hash) >> (u64::BITS - shard_bits)) as usize
    }

    fn physical_slot_parts(&self, physical_slot: usize) -> Option<(usize, usize)> {
        if physical_slot >= self.slot_count {
            return None;
        }
        let shard_count = self.shards.len();
        let base = self.slot_count / shard_count;
        let remainder = self.slot_count % shard_count;
        let larger_span = (base + 1) * remainder;
        if physical_slot < larger_span {
            Some((physical_slot / (base + 1), physical_slot % (base + 1)))
        } else {
            let relative = physical_slot - larger_span;
            Some((remainder + relative / base, relative % base))
        }
    }
}

fn subtract_exact(counter: &AtomicUsize, amount: usize) {
    if amount != 0 {
        let previous = counter.fetch_sub(amount, Ordering::Relaxed);
        debug_assert!(previous >= amount, "index visibility counter underflow");
    }
}

// Format V1 persists the FNV-derived key hash, whose high bits have weak
// dispersion for keys with a common prefix. Mix only for the in-memory shard
// route so similar keys do not exhaust one tiny shard while preserving the
// stable on-disk hash and the shard-local probe behavior.
fn mix_for_shard(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

fn index_shard_count(slot_count: usize) -> usize {
    let upper = (slot_count / 8).min(MAX_INDEX_SHARDS);
    let mut count = 1;
    while count * 2 <= upper {
        count *= 2;
    }
    count
}

fn read_unpoisoned<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_unpoisoned<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn location(region_id: u32, offset: u32) -> PackedLocation {
        PackedLocation::new(region_id, offset, 32, false).unwrap()
    }

    #[test]
    fn packed_location_round_trips_boundaries() {
        let minimum = PackedLocation::new(0, 0, 32, false).unwrap();
        assert_eq!(minimum.raw(), 1_u64 << RECORD_LEN_SHIFT);
        assert_eq!(PackedLocation::try_from_raw(minimum.raw()), Ok(minimum));
        assert_eq!(minimum.region_id(), 0);
        assert_eq!(minimum.offset(), 0);
        assert_eq!(minimum.record_len(), 32);
        assert!(!minimum.is_tombstone());

        let maximum =
            PackedLocation::new(MAX_REGION_ID, MAX_REGION_OFFSET, MAX_RECORD_LEN, true).unwrap();
        assert_eq!(maximum.raw(), u64::MAX);
        assert_eq!(PackedLocation::try_from_raw(maximum.raw()), Ok(maximum));
        assert_eq!(maximum.region_id(), MAX_REGION_ID);
        assert_eq!(maximum.offset(), MAX_REGION_OFFSET);
        assert_eq!(maximum.record_len(), MAX_RECORD_LEN);
        assert!(maximum.tombstone());
    }

    #[test]
    fn packed_location_rejects_unrepresentable_values() {
        assert_eq!(
            PackedLocation::new(MAX_REGION_ID + 1, 0, 32, false),
            Err(PackedLocationError::RegionOutOfRange)
        );
        assert_eq!(
            PackedLocation::new(0, 1, 32, false),
            Err(PackedLocationError::OffsetUnaligned)
        );
        assert_eq!(
            PackedLocation::new(0, MAX_REGION_OFFSET + OFFSET_ALIGNMENT, 32, false),
            Err(PackedLocationError::OffsetOutOfRange)
        );
        assert_eq!(
            PackedLocation::new(0, 0, 0, false),
            Err(PackedLocationError::RecordLengthZero)
        );
        assert_eq!(
            PackedLocation::new(0, 0, 33, false),
            Err(PackedLocationError::RecordLengthUnaligned)
        );
        assert_eq!(
            PackedLocation::new(0, 0, MAX_RECORD_LEN + RECORD_LEN_ALIGNMENT, false),
            Err(PackedLocationError::RecordLengthOutOfRange)
        );
    }

    #[test]
    fn newer_versions_win_and_remove_is_compare_and_delete() {
        let mut index = CompactIndex::new(8);
        let first = location(1, 0);
        let second = location(2, 8);

        assert!(index.apply_if_newer(7, first, 10, 1).applied);
        let ignored = index.apply_if_newer(7, second, 9, 1);
        assert!(!ignored.applied);
        assert_eq!(ignored.previous.unwrap().location, first);
        assert_eq!(index.get(7, 1).unwrap().seqno, 10);

        let replaced = index.apply_if_newer(7, second, 11, 1);
        assert!(replaced.applied);
        assert_eq!(replaced.previous.unwrap().location, first);
        assert!(!index.remove_if(7, first, 10));
        assert!(index.remove_if(7, second, 11));
        assert!(index.is_empty());
    }

    #[test]
    fn min_seqno_logically_clears_and_lazily_reuses_slots() {
        let mut index = CompactIndex::new(8);
        let old = location(1, 0);
        let new = location(2, 8);
        assert!(index.apply_if_newer(1, old, 10, 1).applied);
        assert_eq!(index.len(), 1);

        assert!(index.get(1, 11).is_none());
        // Both hashes start in slot 1, so the stale slot is reused in place.
        let result = index.apply_if_newer(9, new, 11, 11);
        assert!(result.applied);
        assert_eq!(result.previous, None);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(9, 11).unwrap().location, new);
        assert!(!index.apply_if_newer(17, old, 10, 11).applied);
    }

    #[test]
    fn probe_limit_replaces_starting_slot_instead_of_growing() {
        let mut index = CompactIndex::new(128);
        for sequence in 1..=64_u64 {
            let hash = (sequence - 1) * 128;
            assert!(
                index
                    .apply_if_newer(hash, location(1, (sequence as u32 - 1) * 8), sequence, 1)
                    .applied
            );
        }
        assert_eq!(index.len(), 64);

        let replacement = location(2, 0);
        let result = index.apply_if_newer(64 * 128, replacement, 65, 1);
        assert!(result.applied);
        assert_eq!(result.previous.unwrap().location, location(1, 0));
        assert_eq!(index.len(), 64);
        assert!(index.get(0, 1).is_none());
        assert_eq!(index.get(64 * 128, 1).unwrap().location, replacement);
        assert_eq!(index.capacity(), 128);
    }

    #[test]
    fn evict_region_also_purges_logically_cleared_entries() {
        let mut index = CompactIndex::new(16);
        let stale = location(3, 0);
        let victim = location(3, 8);
        let survivor = location(4, 16);
        assert!(index.apply_if_newer(1, stale, 2, 1).applied);
        assert!(index.apply_if_newer(2, victim, 10, 1).applied);
        assert!(index.apply_if_newer(3, survivor, 11, 1).applied);

        assert_eq!(index.evict_region(3, 5), 2);
        assert_eq!(index.len(), 1);
        assert_eq!(index.get(3, 5).unwrap().location, survivor);
    }

    #[test]
    fn sharded_index_preserves_total_capacity_and_parallel_shards() {
        use std::sync::{Arc, Barrier};

        let index = Arc::new(ShardedIndex::new(1000));
        assert_eq!(index.capacity(), 1000);
        assert_eq!(index.shard_count(), 64);

        let hashes = (0_u64..10_000)
            .filter(|hash| index.shard_for(*hash) < 8)
            .fold(Vec::new(), |mut hashes, hash| {
                if !hashes
                    .iter()
                    .any(|existing| index.shard_for(*existing) == index.shard_for(hash))
                {
                    hashes.push(hash);
                }
                hashes
            });
        assert_eq!(hashes.len(), 8);
        let barrier = Arc::new(Barrier::new(hashes.len()));
        let workers = hashes
            .into_iter()
            .enumerate()
            .map(|(worker, hash)| {
                let index = Arc::clone(&index);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    let entry_location = location(worker as u32, worker as u32 * 8);
                    assert!(
                        index
                            .apply_if_newer(hash, entry_location, worker as u64 + 1, 1)
                            .applied
                    );
                    assert_eq!(index.get(hash, 1).unwrap().location, entry_location);
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(index.value_len(1), 8);
        index.clear();
        assert_eq!(index.value_len(1), 0);
    }

    #[test]
    fn snapshot_stream_filters_clear_epoch_and_restores_tombstones() {
        let source = ShardedIndex::new(64);
        let value = location(1, 0);
        let tombstone = PackedLocation::new(2, 8, 32, true).unwrap();
        let stale = location(3, 16);
        let reclaimed = location(9, 24);

        assert!(source.apply_if_newer(11, value, 5, 1).applied);
        assert!(source.apply_if_newer(22, tombstone, 6, 1).applied);
        assert!(source.apply_if_newer(33, stale, 4, 1).applied);
        assert!(source.apply_if_newer(44, reclaimed, 7, 1).applied);

        // Region eviction also physically purges entries hidden by the clear
        // epoch, so neither kind can leak into the checkpoint stream.
        assert_eq!(source.evict_region(9, 5), 2);

        let mut snapshot = Vec::new();
        let visited = source
            .try_for_each_snapshot_entry(5, |_, entry| {
                snapshot.push(entry);
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(visited, 2);
        assert_eq!(snapshot.len(), 2);
        assert!(snapshot.iter().any(|entry| entry.hash == 11));
        assert!(snapshot.iter().any(|entry| {
            entry.hash == 22 && PackedLocation::from_raw(entry.location_raw).is_tombstone()
        }));

        let restored = ShardedIndex::new(64);
        assert!(restored.apply_if_newer(99, location(4, 0), 9, 1).applied);
        restored.clear();
        for entry in snapshot {
            assert!(restored.restore_snapshot_entry(entry, 5).unwrap().applied);
        }

        assert_eq!(restored.get(11, 5).unwrap().location, value);
        assert_eq!(restored.get(22, 5).unwrap().location, tombstone);
        assert!(restored.get(33, 1).is_none());
        assert!(restored.get(44, 1).is_none());
        assert!(restored.get(99, 1).is_none());
        assert_eq!(restored.value_len(5), 1);
    }

    #[test]
    fn metadata_round_trips_and_second_chance_updates_are_compare_and_set() {
        let source = ShardedIndex::new(16);
        let first = location(1, 0);
        let replacement = location(2, 8);
        assert!(
            source
                .apply_if_newer_with_metadata(7, first, 5, 1, 42, 0x20)
                .applied
        );
        let observed = source.get(7, 1).unwrap();
        assert_eq!(observed.namespace_id, 42);
        assert_eq!(observed.flags, 0x20);

        assert!(!source.mark_second_chance_if(7, first, 5, 41));
        assert!(!source.mark_second_chance_if(7, replacement, 5, 42));
        assert!(source.mark_second_chance_if(7, first, 5, 42));
        assert!(!source.mark_second_chance_if(7, first, 5, 42));
        assert_eq!(
            source.get(7, 1).unwrap().flags,
            0x20 | INDEX_FLAG_SECOND_CHANCE_PENDING
        );

        let mut snapshot = Vec::new();
        source
            .try_for_each_snapshot_entry(1, |_, entry| {
                snapshot.push(entry);
                Ok::<(), ()>(())
            })
            .unwrap();
        let restored = ShardedIndex::new(16);
        assert!(
            restored
                .restore_snapshot_entry(snapshot[0], 1)
                .unwrap()
                .applied
        );
        assert_eq!(restored.get(7, 1), source.get(7, 1));

        assert!(!restored.clear_second_chance_if(7, first, 6, 42));
        assert!(restored.clear_second_chance_if(7, first, 5, 42));
        assert!(!restored.clear_second_chance_if(7, first, 5, 42));
        assert_eq!(restored.get(7, 1).unwrap().flags, 0x20);

        assert!(restored.apply_if_newer(7, replacement, 6, 1).applied);
        let default_metadata = restored.get(7, 1).unwrap();
        assert_eq!(default_metadata.namespace_id, 0);
        assert_eq!(default_metadata.flags, 0);
        assert_eq!(
            restored.remove_if_entry(7, replacement, 6),
            Some(default_metadata)
        );
    }

    #[test]
    fn region_eviction_streams_exact_removed_metadata() {
        let index = ShardedIndex::new(32);
        let first = location(3, 0);
        let stale = location(4, 8);
        let survivor = location(5, 16);
        assert!(
            index
                .apply_if_newer_with_metadata(1, first, 7, 1, 11, 0x10)
                .applied
        );
        assert!(
            index
                .apply_if_newer_with_metadata(2, stale, 2, 1, 12, 0x20)
                .applied
        );
        assert!(index.apply_if_newer(3, survivor, 8, 1).applied);

        let mut removed = Vec::new();
        assert_eq!(
            index.evict_region_with(3, 5, |entry| removed.push(entry)),
            2
        );
        removed.sort_unstable_by_key(|entry| entry.namespace_id);
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].location, first);
        assert_eq!(removed[0].namespace_id, 11);
        assert_eq!(removed[0].flags, 0x10);
        assert_eq!(index.get(3, 5).unwrap().location, survivor);
    }

    #[test]
    fn snapshot_restore_stays_bounded_under_probe_collisions() {
        let mut index = CompactIndex::new(128);
        for sequence in 1..=65_u64 {
            let entry = IndexSnapshotEntry {
                hash: (sequence - 1) * 128,
                location_raw: location(1, (sequence as u32 - 1) * 8).raw(),
                seqno: sequence,
                namespace_id: 0,
                flags: 0,
            };
            assert!(index.restore_snapshot_entry(entry, 1).unwrap().applied);
        }

        assert_eq!(index.len(), 64);
        assert!(index.get(0, 1).is_none());
        assert_eq!(
            index.get(64 * 128, 1).unwrap().location,
            location(1, 64 * 8)
        );
        let visited = index
            .try_for_each_snapshot_entry(1, &mut |_| Ok::<(), ()>(()))
            .unwrap();
        assert_eq!(visited, 64);
    }

    #[test]
    fn exact_slot_restore_avoids_slot_order_reinsertion_loss() {
        // Seed 89 is a stable collision-heavy table where replaying the final
        // slots in physical order through the normal 64-probe insertion policy
        // evicts one additional entry. This is the ordering bug checkpoint v4
        // avoids by restoring each entry to its persisted physical slot.
        let mut source = CompactIndex::new(128);
        let mut hash = 89_u64;
        for sequence in 1..=400_u64 {
            hash ^= hash << 13;
            hash ^= hash >> 7;
            hash ^= hash << 17;
            source.apply_if_newer(hash, location(1, (sequence as u32 - 1) * 8), sequence, 1);
        }
        let mut snapshot = Vec::new();
        source
            .try_for_each_snapshot_entry_if(|_| true, &mut |slot, entry| {
                snapshot.push((slot, entry));
                Ok::<(), ()>(())
            })
            .unwrap();
        assert_eq!(snapshot.len(), 128);

        let mut fallback = CompactIndex::new(128);
        for (_, entry) in &snapshot {
            fallback.restore_snapshot_entry(*entry, 1).unwrap();
        }
        assert_eq!(fallback.len(), 127);

        let mut exact = CompactIndex::new(128);
        for &(slot, snapshot_entry) in &snapshot {
            let entry = IndexEntry {
                location: PackedLocation::from_raw(snapshot_entry.location_raw),
                seqno: snapshot_entry.seqno,
                namespace_id: snapshot_entry.namespace_id,
                flags: snapshot_entry.flags,
            };
            assert!(
                exact
                    .restore_exact_if_visible(slot, snapshot_entry.hash, entry, |_| true)
                    .unwrap()
            );
        }
        assert_eq!(exact.len(), snapshot.len());
        for (_, entry) in snapshot {
            assert_eq!(exact.get(entry.hash, 1).unwrap().seqno, entry.seqno);
        }
        assert!(exact.get(u64::MAX, 1).is_none());
    }

    #[test]
    fn exact_slot_restore_builds_a_full_probe_chain() {
        let mut index = CompactIndex::new(128);
        let entry = IndexEntry {
            location: location(1, 0),
            seqno: 7,
            namespace_id: 3,
            flags: 0,
        };

        assert!(
            index
                .restore_exact_if_visible(MAX_PROBES - 1, 0, entry, |_| true)
                .unwrap()
        );
        assert_eq!(index.get(0, 1), Some(entry));
        assert!(index.get(128, 1).is_none());

        let mut invalid = CompactIndex::new(128);
        assert_eq!(
            invalid.restore_exact_if_visible(MAX_PROBES, 0, entry, |_| true),
            Err(IndexSnapshotError::PhysicalSlotOutsideProbeWindow)
        );
        assert_eq!(invalid.len(), 0);
        assert!(invalid.slots.iter().all(|slot| slot.is_empty()));
    }

    #[test]
    fn snapshot_restore_rejects_invalid_fields_without_mutation() {
        let index = ShardedIndex::new(16);
        let valid = IndexSnapshotEntry {
            hash: 1,
            location_raw: location(1, 0).raw(),
            seqno: 5,
            namespace_id: 0,
            flags: 0,
        };
        assert!(index.restore_snapshot_entry(valid, 1).unwrap().applied);

        let zero_sequence = IndexSnapshotEntry { seqno: 0, ..valid };
        assert_eq!(
            index.restore_snapshot_entry(zero_sequence, 1),
            Err(IndexSnapshotError::SequenceZero)
        );
        let invalid_location = IndexSnapshotEntry {
            hash: 2,
            location_raw: 0,
            seqno: 6,
            namespace_id: 0,
            flags: 0,
        };
        assert_eq!(
            index.restore_snapshot_entry(invalid_location, 1),
            Err(IndexSnapshotError::InvalidLocation(
                PackedLocationError::RecordLengthZero
            ))
        );

        let logically_cleared = IndexSnapshotEntry {
            hash: 3,
            location_raw: location(2, 8).raw(),
            seqno: 4,
            namespace_id: 0,
            flags: 0,
        };
        assert!(
            !index
                .restore_snapshot_entry(logically_cleared, 5)
                .unwrap()
                .applied
        );
        assert_eq!(index.value_len(1), 1);
        assert!(index.get(2, 1).is_none());
        assert!(index.get(3, 1).is_none());
    }

    #[test]
    fn region_generation_invalidation_is_constant_time_and_exactly_counted() {
        let index = ShardedIndex::try_new(64, 4).unwrap();
        let value = location(1, 0);
        let tombstone = PackedLocation::new(1, 8, 32, true).unwrap();

        // New production indexes start with every Region explicitly Free.
        assert!(!index.apply_if_newer(1, value, 10, 1).applied);
        assert_eq!(index.entry_len(), 0);
        assert_eq!(index.value_len(1), 0);

        assert_eq!(
            index
                .invalidate_region_generation(1, RegionGeneration::Allocated { created_seqno: 10 }),
            Some(RegionIndexCounts::default())
        );
        assert!(index.apply_if_newer(1, value, 10, 1).applied);
        assert!(
            index
                .apply_if_newer_with_metadata(2, tombstone, 11, 1, 7, 0)
                .applied
        );
        assert_eq!(index.entry_len(), 2);
        assert_eq!(index.value_len(1), 1);

        let removed = index
            .invalidate_region_generation(1, RegionGeneration::Allocated { created_seqno: 20 })
            .unwrap();
        assert_eq!(
            removed,
            RegionIndexCounts {
                entries: 2,
                values: 1
            }
        );
        assert_eq!(index.entry_len(), 0);
        assert_eq!(index.value_len(1), 0);
        assert!(index.get(1, 1).is_none());
        assert!(index.get(2, 1).is_none());

        let scrubbed = index
            .remove_physical_if_entry(2, tombstone, 11)
            .expect("stale physical tombstone must remain scrub-able");
        assert_eq!(scrubbed.location, tombstone);
        assert_eq!(index.entry_len(), 0);
        assert_eq!(index.value_len(1), 0);

        // The old physical slot is lazily reusable but never reported as the
        // previous logical version of the new Region incarnation.
        assert!(!index.apply_if_newer(1, value, 19, 1).applied);
        let replacement = index.apply_if_newer(1, value, 20, 1);
        assert!(replacement.applied);
        assert_eq!(replacement.previous, None);
        assert_eq!(index.entry_len(), 1);
        assert_eq!(index.value_len(1), 1);
    }

    #[test]
    fn logical_clear_hides_slots_resets_counts_and_filters_checkpoint() {
        let index = ShardedIndex::new(64);
        let old = location(1, 0);
        let survivor = location(2, 8);
        assert!(index.apply_if_newer(7, old, 5, 1).applied);
        assert!(index.apply_if_newer(8, survivor, 6, 1).applied);
        assert_eq!(index.entry_len(), 2);

        index.advance_clear_floor(10);
        assert_eq!(index.entry_len(), 0);
        assert_eq!(index.value_len(10), 0);
        assert!(index.get(7, 1).is_none());
        assert!(index.get(8, 1).is_none());

        let newer = location(3, 16);
        let result = index.apply_if_newer(7, newer, 10, 1);
        assert!(result.applied);
        assert_eq!(result.previous, None);
        assert_eq!(index.entry_len(), 1);
        assert_eq!(index.value_len(10), 1);

        let mut snapshot = Vec::new();
        assert_eq!(
            index
                .try_for_each_snapshot_entry(1, |_, entry| {
                    snapshot.push(entry);
                    Ok::<(), ()>(())
                })
                .unwrap(),
            1
        );
        assert_eq!(snapshot[0].hash, 7);
        assert_eq!(snapshot[0].location_raw, newer.raw());
    }

    #[test]
    fn reset_visibility_for_restore_rejects_wrong_region_count() {
        let index = ShardedIndex::try_new(16, 2).unwrap();
        assert!(!index.reset_visibility_for_restore(
            1,
            [RegionGeneration::Allocated { created_seqno: 1 }].into_iter()
        ));
        assert!(!index.apply_if_newer(1, location(0, 0), 1, 1).applied);
    }

    #[test]
    #[should_panic(expected = "at least 8 slots")]
    fn rejects_tiny_tables() {
        let _ = CompactIndex::new(7);
    }

    #[test]
    fn logical_slot_stays_compact() {
        assert_eq!(std::mem::size_of::<Slot>(), 32);
    }
}
