//! Fixed-candidate point index for Region records.
//!
//! Each key has four deterministic buckets in one canonical partition. A
//! bucket stores only a 14-bit fingerprint, its two-bit displacement, and the
//! packed Region/offset plus a record-size upper class. Full-key, exact-envelope,
//! and checksum validation remain the authority after the single record read.
//! There are no probe chains,
//! tombstones, generation tables, retries, or request-time allocations.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(feature = "benchmarking")]
use std::cell::Cell;

use crate::hashing::route_hash;
use crate::index::{INDEX_CANDIDATES, IndexEntry, PackedLocation};
use crate::index_storage::{
    IndexPartitionWriteGuard, IndexSlotState, IndexStorageError, PartitionedIndexStorage,
};
use crate::snapshot::CacheIndexSnapshot;

const CANDIDATE_OFFSETS: [usize; INDEX_CANDIDATES] = [0, 23, 61, 97];
const FINGERPRINT_MASK: u16 = (1 << 14) - 1;
const REFERENCE_WORD_BITS: usize = u64::BITS as usize;

pub(crate) fn heat_memory_bytes(slot_count: usize) -> Option<usize> {
    let bitmap_bytes = slot_count
        .div_ceil(REFERENCE_WORD_BITS)
        .checked_mul(std::mem::size_of::<AtomicU64>())?;
    bitmap_bytes.checked_mul(2)
}

struct SlotHeat {
    seen: Box<[AtomicU64]>,
    hot: Box<[AtomicU64]>,
}

impl SlotHeat {
    fn try_new(slot_count: usize) -> Result<Self, IndexStorageError> {
        let word_count = slot_count.div_ceil(REFERENCE_WORD_BITS);
        let mut seen = Vec::new();
        seen.try_reserve_exact(word_count).map_err(|_| {
            IndexStorageError::Io(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "unable to allocate index heat bitmap",
            ))
        })?;
        let mut hot = Vec::new();
        hot.try_reserve_exact(word_count).map_err(|_| {
            IndexStorageError::Io(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "unable to allocate index heat bitmap",
            ))
        })?;
        seen.resize_with(word_count, || AtomicU64::new(0));
        hot.resize_with(word_count, || AtomicU64::new(0));
        Ok(Self {
            seen: seen.into_boxed_slice(),
            hot: hot.into_boxed_slice(),
        })
    }

    fn mark(&self, slot: usize) {
        let word = slot / REFERENCE_WORD_BITS;
        let mask = 1_u64 << (slot % REFERENCE_WORD_BITS);
        let (Some(seen), Some(hot)) = (self.seen.get(word), self.hot.get(word)) else {
            return;
        };
        // The caller retains this slot's partition guard. Reclaim cannot clear
        // the same slot while it is observed, but concurrent readers may
        // deliberately undercount one another. Each candidate performs at most
        // one relaxed RMW; once hot, later candidates are read-only.
        if hot.load(Ordering::Relaxed) & mask != 0 {
            return;
        }
        if seen.load(Ordering::Relaxed) & mask == 0 {
            seen.fetch_or(mask, Ordering::Relaxed);
        } else {
            hot.fetch_or(mask, Ordering::Relaxed);
        }
    }

    fn take_hot(&self, slot: usize) -> bool {
        let word = slot / REFERENCE_WORD_BITS;
        let mask = 1_u64 << (slot % REFERENCE_WORD_BITS);
        let was_hot = self
            .hot
            .get(word)
            .is_some_and(|current| current.fetch_and(!mask, Ordering::Relaxed) & mask != 0);
        if let Some(current) = self.seen.get(word) {
            current.fetch_and(!mask, Ordering::Relaxed);
        }
        was_hot
    }

    fn clear(&self, slot: usize) {
        let _ = self.take_hot(slot);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ReclaimIndexAction {
    Missing,
    Removed,
    Reinsert,
}

pub(crate) struct RegionIndex {
    storage: PartitionedIndexStorage,
    heat: SlotHeat,
    statistics_enabled: AtomicBool,
    relocations: AtomicU64,
    overflow_evictions: AtomicU64,
    conditional_remove_misses: AtomicU64,
    conditional_replace_misses: AtomicU64,
}

#[cfg(feature = "benchmarking")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BenchmarkProbeStats {
    pub(crate) operations: u64,
    pub(crate) probes: u64,
    pub(crate) stale_slots: u64,
    pub(crate) full_windows: u64,
    pub(crate) max_probes: usize,
}

#[cfg(feature = "benchmarking")]
thread_local! {
    static BENCHMARK_PROBE_STATS: Cell<BenchmarkProbeStats> =
        const { Cell::new(BenchmarkProbeStats::EMPTY) };
}

#[cfg(feature = "benchmarking")]
impl BenchmarkProbeStats {
    const EMPTY: Self = Self {
        operations: 0,
        probes: 0,
        stale_slots: 0,
        full_windows: 0,
        max_probes: 0,
    };
}

#[cfg(feature = "benchmarking")]
pub(crate) fn reset_benchmark_probe_stats() {
    BENCHMARK_PROBE_STATS.set(BenchmarkProbeStats::EMPTY);
}

#[cfg(feature = "benchmarking")]
pub(crate) fn take_benchmark_probe_stats() -> BenchmarkProbeStats {
    BENCHMARK_PROBE_STATS.replace(BenchmarkProbeStats::EMPTY)
}

#[cfg(feature = "benchmarking")]
fn record_benchmark_probe(probes: usize) {
    BENCHMARK_PROBE_STATS.with(|cell| {
        let mut stats = cell.get();
        stats.operations = stats.operations.saturating_add(1);
        stats.probes = stats
            .probes
            .saturating_add(u64::try_from(probes).unwrap_or(u64::MAX));
        stats.full_windows = stats
            .full_windows
            .saturating_add(u64::from(probes == INDEX_CANDIDATES));
        stats.max_probes = stats.max_probes.max(probes);
        cell.set(stats);
    });
}

impl RegionIndex {
    pub(crate) fn from_storage(
        storage: PartitionedIndexStorage,
    ) -> Result<Self, IndexStorageError> {
        let heat = SlotHeat::try_new(storage.slot_count())?;
        Ok(Self {
            storage,
            heat,
            statistics_enabled: AtomicBool::new(false),
            relocations: AtomicU64::new(0),
            overflow_evictions: AtomicU64::new(0),
            conditional_remove_misses: AtomicU64::new(0),
            conditional_replace_misses: AtomicU64::new(0),
        })
    }

    pub(crate) const fn storage(&self) -> &PartitionedIndexStorage {
        &self.storage
    }

    pub(crate) fn set_statistics_enabled(&self, enabled: bool) {
        self.statistics_enabled.store(enabled, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> Result<CacheIndexSnapshot, IndexStorageError> {
        let physical = self.storage.physical_stats()?;
        let slot_capacity = u64::try_from(self.storage.slot_count())
            .map_err(|_| IndexStorageError::SizeOverflow)?;
        let empty_slots = slot_capacity
            .checked_sub(physical.value)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        Ok(CacheIndexSnapshot {
            slot_capacity,
            physical_value_slots: physical.value,
            empty_slots,
            relocations: self.relocations.load(Ordering::Relaxed),
            overflow_evictions: self.overflow_evictions.load(Ordering::Relaxed),
            conditional_remove_misses: self.conditional_remove_misses.load(Ordering::Relaxed),
            conditional_replace_misses: self.conditional_replace_misses.load(Ordering::Relaxed),
        })
    }

    /// Returns one fingerprint candidate. Record validation owns correctness.
    pub(crate) fn lookup_raw(&self, hash: u64) -> Result<Option<IndexEntry>, IndexStorageError> {
        let partition = self.storage.try_read_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let fingerprint = fingerprint(hash);
        let candidates = candidate_slots(hash, slot_count);
        #[cfg(feature = "benchmarking")]
        let mut probes = 0;
        for displacement in 0..INDEX_CANDIDATES {
            let slot = candidates[displacement];
            if candidates[..displacement].contains(&slot) {
                continue;
            }
            #[cfg(feature = "benchmarking")]
            {
                probes += 1;
            }
            if let IndexSlotState::Value {
                fingerprint: current,
                displacement: current_displacement,
                entry,
            } = partition.slot_state(slot)?
                && current == fingerprint
                && usize::from(current_displacement) == displacement
            {
                self.heat.mark(partition.global_slot(slot)?);
                #[cfg(feature = "benchmarking")]
                record_benchmark_probe(probes);
                return Ok(Some(entry));
            }
        }
        #[cfg(feature = "benchmarking")]
        record_benchmark_probe(probes);
        Ok(None)
    }

    /// Installs a value with up to two bounded relocations and one bounded eviction.
    pub(crate) fn upsert(
        &self,
        hash: u64,
        supplied: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        #[cfg(feature = "benchmarking")]
        record_benchmark_probe(INDEX_CANDIDATES);
        let mut partition = self.storage.write_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let fingerprint = fingerprint(hash);
        let candidates = candidate_slots(hash, slot_count);
        let mut observed = [IndexSlotState::Empty; INDEX_CANDIDATES];
        let mut first_empty = None;

        for displacement in 0..INDEX_CANDIDATES {
            let slot = candidates[displacement];
            if candidates[..displacement].contains(&slot) {
                continue;
            }
            let state = partition.slot_state(slot)?;
            observed[displacement] = state;
            match state {
                IndexSlotState::Empty => {
                    first_empty.get_or_insert(displacement);
                }
                IndexSlotState::Value {
                    fingerprint: current,
                    displacement: current_displacement,
                    ..
                } if current == fingerprint
                    && usize::from(current_displacement) == displacement =>
                {
                    self.replace_slot(
                        &mut partition,
                        slot,
                        state,
                        value_state(fingerprint, displacement, supplied),
                    )?;
                    return Ok(true);
                }
                IndexSlotState::Value { .. } => {}
            }
        }

        if let Some(displacement) = first_empty {
            self.replace_slot(
                &mut partition,
                candidates[displacement],
                IndexSlotState::Empty,
                value_state(fingerprint, displacement, supplied),
            )?;
            return Ok(true);
        }

        // One hop recovers common candidate-set collisions without a cuckoo
        // walk, retry loop, or unbounded mutation latency.
        for source_displacement in 0..INDEX_CANDIDATES {
            let source_slot = candidates[source_displacement];
            if candidates[..source_displacement].contains(&source_slot) {
                continue;
            }
            let source = observed[source_displacement];
            let IndexSlotState::Value {
                fingerprint: occupant_fingerprint,
                displacement: occupant_displacement,
                entry: occupant_entry,
            } = source
            else {
                continue;
            };
            let occupant_displacement = usize::from(occupant_displacement);
            if occupant_displacement >= INDEX_CANDIDATES {
                return Err(IndexStorageError::InvalidArgument(
                    "index bucket displacement is out of range",
                ));
            }
            let occupant_home = home_from_slot(source_slot, occupant_displacement, slot_count);
            for target_displacement in 0..INDEX_CANDIDATES {
                if target_displacement == occupant_displacement {
                    continue;
                }
                let target_slot = slot_from_home(occupant_home, target_displacement, slot_count);
                if target_slot == source_slot
                    || !matches!(partition.slot_state(target_slot)?, IndexSlotState::Empty)
                {
                    continue;
                }
                self.replace_slot(
                    &mut partition,
                    target_slot,
                    IndexSlotState::Empty,
                    value_state(occupant_fingerprint, target_displacement, occupant_entry),
                )?;
                self.replace_slot(
                    &mut partition,
                    source_slot,
                    source,
                    value_state(fingerprint, source_displacement, supplied),
                )?;
                if self.statistics_enabled.load(Ordering::Relaxed) {
                    self.relocations.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(true);
            }
        }

        // A second bounded hop recovers the remaining common collisions while
        // leaving the four-probe lookup path unchanged. This path runs only
        // after every direct candidate and one-hop alternate is occupied.
        for source_displacement in 0..INDEX_CANDIDATES {
            let source_slot = candidates[source_displacement];
            if candidates[..source_displacement].contains(&source_slot) {
                continue;
            }
            let source = observed[source_displacement];
            let IndexSlotState::Value {
                fingerprint: source_fingerprint,
                displacement: source_current_displacement,
                entry: source_entry,
            } = source
            else {
                continue;
            };
            let source_current_displacement = usize::from(source_current_displacement);
            if source_current_displacement >= INDEX_CANDIDATES {
                return Err(IndexStorageError::InvalidArgument(
                    "index bucket displacement is out of range",
                ));
            }
            let source_home = home_from_slot(source_slot, source_current_displacement, slot_count);
            for middle_displacement in 0..INDEX_CANDIDATES {
                if middle_displacement == source_current_displacement {
                    continue;
                }
                let middle_slot = slot_from_home(source_home, middle_displacement, slot_count);
                if middle_slot == source_slot {
                    continue;
                }
                let middle = partition.slot_state(middle_slot)?;
                let IndexSlotState::Value {
                    fingerprint: middle_fingerprint,
                    displacement: middle_current_displacement,
                    entry: middle_entry,
                } = middle
                else {
                    continue;
                };
                let middle_current_displacement = usize::from(middle_current_displacement);
                if middle_current_displacement >= INDEX_CANDIDATES {
                    return Err(IndexStorageError::InvalidArgument(
                        "index bucket displacement is out of range",
                    ));
                }
                let middle_home =
                    home_from_slot(middle_slot, middle_current_displacement, slot_count);
                for target_displacement in 0..INDEX_CANDIDATES {
                    if target_displacement == middle_current_displacement {
                        continue;
                    }
                    let target_slot = slot_from_home(middle_home, target_displacement, slot_count);
                    if target_slot == source_slot
                        || target_slot == middle_slot
                        || !matches!(partition.slot_state(target_slot)?, IndexSlotState::Empty)
                    {
                        continue;
                    }
                    self.replace_slot(
                        &mut partition,
                        target_slot,
                        IndexSlotState::Empty,
                        value_state(middle_fingerprint, target_displacement, middle_entry),
                    )?;
                    self.replace_slot(
                        &mut partition,
                        middle_slot,
                        middle,
                        value_state(source_fingerprint, middle_displacement, source_entry),
                    )?;
                    self.replace_slot(
                        &mut partition,
                        source_slot,
                        source,
                        value_state(fingerprint, source_displacement, supplied),
                    )?;
                    if self.statistics_enabled.load(Ordering::Relaxed) {
                        self.relocations.fetch_add(2, Ordering::Relaxed);
                    }
                    return Ok(true);
                }
            }
        }

        let victim_start = (hash.rotate_left(17) as usize) & (INDEX_CANDIDATES - 1);
        let victim_displacement = (0..INDEX_CANDIDATES)
            .map(|step| (victim_start + step) & (INDEX_CANDIDATES - 1))
            .find(|&displacement| !candidates[..displacement].contains(&candidates[displacement]))
            .expect("every non-empty index partition has one distinct candidate");
        let victim_slot = candidates[victim_displacement];
        self.replace_slot(
            &mut partition,
            victim_slot,
            observed[victim_displacement],
            value_state(fingerprint, victim_displacement, supplied),
        )?;
        if self.statistics_enabled.load(Ordering::Relaxed) {
            self.overflow_evictions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(true)
    }

    pub(crate) fn try_delete(&self, hash: u64) -> Result<bool, IndexStorageError> {
        self.remove_matching(hash, None, true)
    }

    /// Clears a mapping only while it still points at the reclaimed address.
    /// Persisted lengths are size-class upper bounds, so address identity uses
    /// Region, offset, and encoded size class rather than an exact byte count.
    pub(crate) fn remove_if_match(
        &self,
        hash: u64,
        old_location: PackedLocation,
    ) -> Result<bool, IndexStorageError> {
        self.remove_matching(hash, Some(old_location), false)
    }

    /// Atomically classifies one reclaim candidate under its index partition.
    /// A hot current mapping is retained for a later conditional rewrite;
    /// every other current mapping is removed before the source Region is freed.
    pub(crate) fn prepare_reclaim(
        &self,
        hash: u64,
        old_location: PackedLocation,
    ) -> Result<ReclaimIndexAction, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let fingerprint = fingerprint(hash);
        let candidates = candidate_slots(hash, slot_count);
        for displacement in 0..INDEX_CANDIDATES {
            let slot = candidates[displacement];
            if candidates[..displacement].contains(&slot) {
                continue;
            }
            let state = partition.slot_state(slot)?;
            if !token_location_matches(state, fingerprint, displacement, old_location) {
                continue;
            }
            let global_slot = partition.global_slot(slot)?;
            if self.heat.take_hot(global_slot) {
                return Ok(ReclaimIndexAction::Reinsert);
            }
            self.replace_slot(&mut partition, slot, state, IndexSlotState::Empty)?;
            return Ok(ReclaimIndexAction::Removed);
        }
        self.record_conditional_remove_miss();
        Ok(ReclaimIndexAction::Missing)
    }

    /// Repoints one still-current source mapping after the replacement bytes
    /// have completed. Concurrent puts and deletes win by changing or removing
    /// the expected old address first.
    pub(crate) fn replace_if_match(
        &self,
        hash: u64,
        old_location: PackedLocation,
        replacement: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let fingerprint = fingerprint(hash);
        let candidates = candidate_slots(hash, slot_count);
        for displacement in 0..INDEX_CANDIDATES {
            let slot = candidates[displacement];
            if candidates[..displacement].contains(&slot) {
                continue;
            }
            let state = partition.slot_state(slot)?;
            if token_location_matches(state, fingerprint, displacement, old_location) {
                self.replace_slot(
                    &mut partition,
                    slot,
                    state,
                    value_state(fingerprint, displacement, replacement),
                )?;
                return Ok(true);
            }
        }
        if self.statistics_enabled.load(Ordering::Relaxed) {
            self.conditional_replace_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(false)
    }

    fn remove_matching(
        &self,
        hash: u64,
        expected_location: Option<PackedLocation>,
        non_waiting: bool,
    ) -> Result<bool, IndexStorageError> {
        let mut partition = if non_waiting {
            self.storage.try_write_hash_partition(hash)?
        } else {
            self.storage.write_hash_partition(hash)?
        };
        let slot_count = partition.slot_count();
        let fingerprint = fingerprint(hash);
        let candidates = candidate_slots(hash, slot_count);
        for displacement in 0..INDEX_CANDIDATES {
            let slot = candidates[displacement];
            if candidates[..displacement].contains(&slot) {
                continue;
            }
            let state = partition.slot_state(slot)?;
            let matches = match expected_location {
                Some(location) => {
                    token_location_matches(state, fingerprint, displacement, location)
                }
                None => token_matches(state, fingerprint, displacement),
            };
            if matches {
                self.replace_slot(&mut partition, slot, state, IndexSlotState::Empty)?;
                return Ok(true);
            }
        }
        if expected_location.is_some() {
            self.record_conditional_remove_miss();
        }
        Ok(false)
    }

    fn record_conditional_remove_miss(&self) {
        if self.statistics_enabled.load(Ordering::Relaxed) {
            self.conditional_remove_misses
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn replace_slot(
        &self,
        partition: &mut IndexPartitionWriteGuard<'_>,
        slot: usize,
        previous: IndexSlotState,
        next: IndexSlotState,
    ) -> Result<(), IndexStorageError> {
        let global_slot = partition.global_slot(slot)?;
        partition.replace_observed(slot, previous, next)?;
        self.heat.clear(global_slot);
        Ok(())
    }
}

fn value_state(fingerprint: u16, displacement: usize, entry: IndexEntry) -> IndexSlotState {
    IndexSlotState::Value {
        fingerprint,
        displacement: displacement as u8,
        entry,
    }
}

fn token_matches(state: IndexSlotState, fingerprint: u16, displacement: usize) -> bool {
    matches!(
        state,
        IndexSlotState::Value {
            fingerprint: current,
            displacement: current_displacement,
            ..
        } if current == fingerprint && usize::from(current_displacement) == displacement
    )
}

fn token_location_matches(
    state: IndexSlotState,
    fingerprint: u16,
    displacement: usize,
    location: PackedLocation,
) -> bool {
    matches!(
        state,
        IndexSlotState::Value {
            fingerprint: current,
            displacement: current_displacement,
            entry,
        } if current == fingerprint
            && usize::from(current_displacement) == displacement
            && entry.location.index_equivalent(location)
    )
}

fn fingerprint(hash: u64) -> u16 {
    let mixed = hash ^ hash.rotate_left(19) ^ hash.rotate_right(23);
    ((mixed >> 17) as u16) & FINGERPRINT_MASK
}

fn candidate_slots(hash: u64, slot_count: usize) -> [usize; INDEX_CANDIDATES] {
    let home = route_hash(hash.rotate_left(32), slot_count);
    std::array::from_fn(|displacement| slot_from_home(home, displacement, slot_count))
}

fn slot_from_home(home: usize, displacement: usize, slot_count: usize) -> usize {
    debug_assert!(displacement < INDEX_CANDIDATES);
    debug_assert!(home < slot_count);
    let offset = candidate_offset(displacement, slot_count);
    let slot = home + offset;
    if slot >= slot_count {
        slot - slot_count
    } else {
        slot
    }
}

fn home_from_slot(slot: usize, displacement: usize, slot_count: usize) -> usize {
    debug_assert!(slot < slot_count);
    let offset = candidate_offset(displacement, slot_count);
    if slot >= offset {
        slot - offset
    } else {
        slot_count - (offset - slot)
    }
}

fn candidate_offset(displacement: usize, slot_count: usize) -> usize {
    let offset = CANDIDATE_OFFSETS[displacement];
    if offset < slot_count {
        offset
    } else {
        offset % slot_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_storage::IndexPhysicalStats;
    use crate::record_codec::hash_key;

    fn entry(region_id: u32, offset: u32) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::new(region_id, offset, 32).unwrap(),
        }
    }

    fn anonymous(slot_count: usize) -> RegionIndex {
        RegionIndex::from_storage(PartitionedIndexStorage::anonymous(slot_count).unwrap()).unwrap()
    }

    #[test]
    fn production_heat_bitmaps_are_exactly_two_bits_per_slot() {
        assert_eq!(
            heat_memory_bytes(crate::index::MAX_INDEX_SLOTS),
            Some(128 * 1024 * 1024)
        );
    }

    #[test]
    fn point_operations_use_fixed_candidates_without_tombstones() {
        let index = anonymous(128);
        let hash = 0x1234_5678_90ab_cdef;
        let first = entry(1, 0);
        let second = entry(2, 32);

        assert_eq!(index.lookup_raw(hash).unwrap(), None);
        assert!(index.upsert(hash, first).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(first));
        assert!(index.upsert(hash, second).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(second));
        assert!(index.try_delete(hash).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 0,
                deleted: 0,
            }
        );
    }

    #[test]
    fn full_candidate_window_uses_the_bounded_second_relocation_hop() {
        let index = anonymous(128);
        index.set_statistics_enabled(true);
        let mut exercised = false;
        for ordinal in 0..10_000_u64 {
            let hash = hash_key(7, &ordinal.to_le_bytes());
            let supplied = entry(1, (ordinal as u32) * 32);
            let before = index.snapshot().unwrap();
            index.upsert(hash, supplied).unwrap();
            let after = index.snapshot().unwrap();
            if after.relocations == before.relocations + 2 {
                assert_eq!(after.overflow_evictions, before.overflow_evictions);
                assert_eq!(index.lookup_raw(hash).unwrap(), Some(supplied));
                exercised = true;
                break;
            }
        }
        assert!(
            exercised,
            "test hash stream never exercised a two-hop relocation"
        );
    }

    #[test]
    fn reclaim_remove_is_address_conditional() {
        let index = anonymous(128);
        let hash = 9;
        let old = entry(1, 0);
        let new = entry(2, 0);
        index.upsert(hash, old).unwrap();
        index.upsert(hash, new).unwrap();

        assert!(!index.remove_if_match(hash, old.location).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(new));
        assert!(index.remove_if_match(hash, new.location).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
    }

    #[test]
    fn reclaim_reinserts_only_an_exact_mapping_seen_at_least_twice() {
        let index = anonymous(128);
        let hash = 17;
        let old = entry(1, 0);
        let replacement = entry(2, 0);

        index.upsert(hash, old).unwrap();
        assert_eq!(
            index.prepare_reclaim(hash, old.location).unwrap(),
            ReclaimIndexAction::Removed
        );

        index.upsert(hash, old).unwrap();
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(
            index.prepare_reclaim(hash, old.location).unwrap(),
            ReclaimIndexAction::Removed,
            "one candidate access must not survive a scan"
        );

        index.upsert(hash, old).unwrap();
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(
            index.prepare_reclaim(hash, old.location).unwrap(),
            ReclaimIndexAction::Reinsert
        );
        assert!(
            index
                .replace_if_match(hash, old.location, replacement)
                .unwrap()
        );
        assert_eq!(
            index.prepare_reclaim(hash, replacement.location).unwrap(),
            ReclaimIndexAction::Removed,
            "conditional publication must reset both heat bits"
        );
    }

    #[test]
    fn newer_index_publication_wins_a_delayed_reinsert() {
        let index = anonymous(128);
        let hash = 23;
        let old = entry(1, 0);
        let replacement = entry(2, 0);
        let newer = entry(3, 0);

        index.upsert(hash, old).unwrap();
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(
            index.prepare_reclaim(hash, old.location).unwrap(),
            ReclaimIndexAction::Reinsert
        );
        index.upsert(hash, newer).unwrap();
        assert!(
            !index
                .replace_if_match(hash, old.location, replacement)
                .unwrap()
        );
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(newer));
    }

    #[test]
    fn delete_wins_a_delayed_reinsert() {
        let index = anonymous(128);
        let hash = 29;
        let old = entry(1, 0);
        let replacement = entry(2, 0);

        index.upsert(hash, old).unwrap();
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(old));
        assert_eq!(
            index.prepare_reclaim(hash, old.location).unwrap(),
            ReclaimIndexAction::Reinsert
        );
        assert!(index.try_delete(hash).unwrap());
        assert!(
            !index
                .replace_if_match(hash, old.location, replacement)
                .unwrap()
        );
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
    }

    #[test]
    fn candidate_arithmetic_round_trips() {
        for slot_count in 4..512 {
            for home in 0..slot_count {
                for displacement in 0..INDEX_CANDIDATES {
                    let slot = slot_from_home(home, displacement, slot_count);
                    assert_eq!(home_from_slot(slot, displacement, slot_count), home);
                }
            }
        }
    }

    #[test]
    fn duplicate_candidates_in_a_minimum_partition_keep_exact_stats() {
        let index = anonymous(INDEX_CANDIDATES);
        for hash in 0_u64..1_000 {
            assert!(index.upsert(hash, entry(1, (hash as u32) * 32)).unwrap());
            let physical = index.storage().physical_stats().unwrap();
            assert!(physical.value <= INDEX_CANDIDATES as u64);
            assert_eq!(physical.deleted, 0);
        }
        index.snapshot().unwrap();
    }
}
