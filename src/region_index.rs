//! Fixed-candidate point index for Region records.
//!
//! Each key has four deterministic buckets in one canonical partition. A
//! bucket stores only a 14-bit fingerprint, its two-bit displacement, and the
//! exact packed record location. Full-key and checksum validation remain the
//! authority after the single record read. There are no probe chains,
//! tombstones, generation tables, retries, or dynamic allocations.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[cfg(feature = "benchmarking")]
use std::cell::Cell;

use crate::hashing::route_hash;
use crate::index::{INDEX_CANDIDATES, IndexEntry, PackedLocation};
use crate::index_storage::{IndexSlotState, IndexStorageError, PartitionedIndexStorage};
use crate::snapshot::CacheIndexSnapshot;

const CANDIDATE_OFFSETS: [usize; INDEX_CANDIDATES] = [0, 23, 61, 97];
const FINGERPRINT_MASK: u16 = (1 << 14) - 1;

pub(crate) struct RegionIndex {
    storage: PartitionedIndexStorage,
    statistics_enabled: AtomicBool,
    relocations: AtomicU64,
    overflow_evictions: AtomicU64,
    conditional_remove_misses: AtomicU64,
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
    pub(crate) fn from_storage(storage: PartitionedIndexStorage) -> Self {
        Self {
            storage,
            statistics_enabled: AtomicBool::new(false),
            relocations: AtomicU64::new(0),
            overflow_evictions: AtomicU64::new(0),
            conditional_remove_misses: AtomicU64::new(0),
        }
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
                #[cfg(feature = "benchmarking")]
                record_benchmark_probe(probes);
                return Ok(Some(entry));
            }
        }
        #[cfg(feature = "benchmarking")]
        record_benchmark_probe(probes);
        Ok(None)
    }

    /// Installs a value with one bounded relocation and one bounded eviction.
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
                    partition.replace_observed(
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
            partition.replace_observed(
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
                partition.replace_observed(
                    target_slot,
                    IndexSlotState::Empty,
                    value_state(occupant_fingerprint, target_displacement, occupant_entry),
                )?;
                partition.replace_observed(
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

        let victim_start = (hash.rotate_left(17) as usize) & (INDEX_CANDIDATES - 1);
        let victim_displacement = (0..INDEX_CANDIDATES)
            .map(|step| (victim_start + step) & (INDEX_CANDIDATES - 1))
            .find(|&displacement| !candidates[..displacement].contains(&candidates[displacement]))
            .expect("every non-empty index partition has one distinct candidate");
        let victim_slot = candidates[victim_displacement];
        partition.replace_observed(
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
    pub(crate) fn remove_if_match(
        &self,
        hash: u64,
        old_location: PackedLocation,
    ) -> Result<bool, IndexStorageError> {
        self.remove_matching(hash, Some(old_location), false)
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
                partition.replace_observed(slot, state, IndexSlotState::Empty)?;
                return Ok(true);
            }
        }
        if expected_location.is_some() && self.statistics_enabled.load(Ordering::Relaxed) {
            self.conditional_remove_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(false)
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
            && entry.location == location
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
    (home + CANDIDATE_OFFSETS[displacement] % slot_count) % slot_count
}

fn home_from_slot(slot: usize, displacement: usize, slot_count: usize) -> usize {
    let offset = CANDIDATE_OFFSETS[displacement] % slot_count;
    (slot + slot_count - offset) % slot_count
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_storage::IndexPhysicalStats;

    fn entry(region_id: u32, offset: u32) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::new(region_id, offset, 32).unwrap(),
        }
    }

    fn anonymous(slot_count: usize) -> RegionIndex {
        RegionIndex::from_storage(PartitionedIndexStorage::anonymous(slot_count).unwrap())
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
