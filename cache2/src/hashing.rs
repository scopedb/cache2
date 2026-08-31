// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

use std::io;

const EMPTY_VALUE: u32 = u32::MAX;
const DELETED_VALUE: u32 = u32::MAX - 1;
const MAX_FIXED_MAP_PROBES: usize = 64;

#[derive(Clone, Copy)]
struct FixedMapSlot {
    fingerprint: u32,
    value: u32,
}

impl Default for FixedMapSlot {
    fn default() -> Self {
        Self {
            fingerprint: 0,
            value: EMPTY_VALUE,
        }
    }
}

/// Fixed-capacity directory for cache hashes that are already seeded XXH3 results.
///
/// Each slot stores a 32-bit fingerprint and a 32-bit owner index. Callers that
/// require full-hash identity validate it in the owner slot. The table is
/// allocated once during open; point operations inspect a small constant
/// number of slots and never allocate, resize, or rehash.
pub(crate) struct FixedPrehashedMap {
    slots: Box<[FixedMapSlot]>,
}

impl FixedPrehashedMap {
    pub(crate) fn try_new(maximum_entries: usize) -> io::Result<Self> {
        let slot_count = Self::slot_count(maximum_entries)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(slot_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate fixed prehashed map",
            )
        })?;
        slots.resize(slot_count, FixedMapSlot::default());
        Ok(Self {
            slots: slots.into_boxed_slice(),
        })
    }

    pub(crate) fn allocation_bytes(maximum_entries: usize) -> io::Result<usize> {
        Self::slot_count(maximum_entries)?
            .checked_mul(std::mem::size_of::<FixedMapSlot>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixed map is too large"))
    }

    pub(crate) fn get(&self, hash: u64) -> Option<u32> {
        let slots = &self.slots;
        if slots.is_empty() {
            return None;
        }
        let fingerprint = fixed_map_fingerprint(hash);
        let start = fixed_map_start(hash, slots.len());
        for step in 0..slots.len().min(MAX_FIXED_MAP_PROBES) {
            let slot = slots[probe_index(start, step, slots.len())];
            if slot.value == EMPTY_VALUE {
                return None;
            }
            if slot.value != DELETED_VALUE && slot.fingerprint == fingerprint {
                return Some(slot.value);
            }
        }
        None
    }

    pub(crate) fn can_upsert(&self, hash: u64) -> bool {
        self.find_upsert_slot(hash).is_some()
    }

    pub(crate) fn insert(&mut self, hash: u64, value: u32) -> Option<Option<u32>> {
        debug_assert!(value < DELETED_VALUE);
        let (slot_index, previous) = self.find_upsert_slot(hash)?;
        self.slots[slot_index] = FixedMapSlot {
            fingerprint: fixed_map_fingerprint(hash),
            value,
        };
        Some(previous)
    }

    pub(crate) fn remove(&mut self, hash: u64) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let fingerprint = fixed_map_fingerprint(hash);
        let start = fixed_map_start(hash, self.slots.len());
        for step in 0..self.slots.len().min(MAX_FIXED_MAP_PROBES) {
            let index = probe_index(start, step, self.slots.len());
            let slot = self.slots[index];
            if slot.value == EMPTY_VALUE {
                return None;
            }
            if slot.value != DELETED_VALUE && slot.fingerprint == fingerprint {
                let previous = slot.value;
                self.slots[index].value = DELETED_VALUE;
                self.compact_deleted(index);
                return Some(previous);
            }
        }
        None
    }

    /// Moves one deletion hole toward the end of its linear-probe cluster.
    /// Finding an empty slot restores the hole to Empty, so lifetime key churn
    /// cannot turn the whole fixed table into tombstones. Pathological clusters
    /// retain one tombstone after the same fixed probe budget used by lookups.
    fn compact_deleted(&mut self, mut hole: usize) {
        let slot_count = self.slots.len();
        let scan_steps = slot_count.saturating_sub(1).min(MAX_FIXED_MAP_PROBES);
        let mut cursor = next_probe_index(hole, slot_count);
        for _ in 0..scan_steps {
            let candidate = self.slots[cursor];
            if candidate.value == EMPTY_VALUE {
                self.slots[hole] = FixedMapSlot::default();
                return;
            }
            if candidate.value != DELETED_VALUE {
                let home = fixed_map_fingerprint_start(candidate.fingerprint, slot_count);
                if probe_distance(home, hole, slot_count) < probe_distance(home, cursor, slot_count)
                {
                    self.slots[hole] = candidate;
                    self.slots[cursor].value = DELETED_VALUE;
                    hole = cursor;
                }
            }
            cursor = next_probe_index(cursor, slot_count);
        }
    }

    fn find_upsert_slot(&self, hash: u64) -> Option<(usize, Option<u32>)> {
        let slots = &self.slots;
        if slots.is_empty() {
            return None;
        }
        let fingerprint = fixed_map_fingerprint(hash);
        let start = fixed_map_start(hash, slots.len());
        let mut deleted = None;
        for step in 0..slots.len().min(MAX_FIXED_MAP_PROBES) {
            let index = probe_index(start, step, slots.len());
            let slot = slots[index];
            if slot.value == EMPTY_VALUE {
                return Some((deleted.unwrap_or(index), None));
            }
            if slot.value == DELETED_VALUE {
                deleted.get_or_insert(index);
            } else if slot.fingerprint == fingerprint {
                return Some((index, Some(slot.value)));
            }
        }
        deleted.map(|index| (index, None))
    }

    fn slot_count(maximum_entries: usize) -> io::Result<usize> {
        if maximum_entries == 0 {
            return Ok(0);
        }
        maximum_entries
            .checked_mul(2)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixed map is too large"))
    }
}

fn fixed_map_start(hash: u64, slots: usize) -> usize {
    fixed_map_fingerprint_start(fixed_map_fingerprint(hash), slots)
}

fn fixed_map_fingerprint(hash: u64) -> u32 {
    (hash as u32).rotate_left(13) ^ (hash >> 32) as u32
}

fn fixed_map_fingerprint_start(fingerprint: u32, slots: usize) -> usize {
    // Folding both halves retains entropy after shard routing has consumed a
    // fixed subset of the original XXH3 low bits. Fibonacci scrambling keeps
    // that entropy visible to multiply-high, which maps across an exact-sized
    // table without integer division.
    let mixed = fingerprint.wrapping_mul(0x9e37_79b9);
    ((u128::from(mixed) * slots as u128) >> 32) as usize
}

fn probe_index(start: usize, step: usize, slots: usize) -> usize {
    let remaining = slots - start;
    if step < remaining {
        start + step
    } else {
        step - remaining
    }
}

fn next_probe_index(index: usize, slots: usize) -> usize {
    if index + 1 == slots { 0 } else { index + 1 }
}

fn probe_distance(home: usize, index: usize, slots: usize) -> usize {
    if index >= home {
        index - home
    } else {
        slots - (home - index)
    }
}

/// Preserves modulo routing while avoiding integer division for the common
/// power-of-two shard and worker counts.
pub(crate) fn route_hash(hash: u64, buckets: usize) -> usize {
    debug_assert_ne!(buckets, 0);
    if buckets.is_power_of_two() {
        hash as usize & (buckets - 1)
    } else {
        (hash % buckets as u64) as usize
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn fixed_map_slot_is_one_u64() {
        assert_eq!(std::mem::size_of::<FixedMapSlot>(), 8);
    }

    #[test]
    fn fixed_map_keeps_exactly_one_empty_slot_per_entry() {
        assert_eq!(FixedPrehashedMap::slot_count(0).unwrap(), 0);
        assert_eq!(FixedPrehashedMap::slot_count(5).unwrap(), 10);
        assert_eq!(FixedPrehashedMap::slot_count(40_960).unwrap(), 81_920);
        assert_eq!(
            FixedPrehashedMap::allocation_bytes(40_960).unwrap(),
            40_960 * 2 * std::mem::size_of::<FixedMapSlot>()
        );
    }

    #[test]
    fn fixed_map_collapses_equal_fingerprints_for_owner_validation() {
        let mut map = FixedPrehashedMap::try_new(2).unwrap();
        let first = 1_u64;
        let second = 1_u64 << 45;
        assert_eq!(fixed_map_fingerprint(first), fixed_map_fingerprint(second));

        assert_eq!(map.insert(first, 7), Some(None));
        assert_eq!(map.insert(second, 9), Some(Some(7)));
        assert_eq!(map.get(first), Some(9));
        assert_eq!(map.get(second), Some(9));
    }

    #[test]
    fn fixed_prehashed_map_reuses_deleted_slots_without_growing() {
        let mut map = FixedPrehashedMap::try_new(2).unwrap();
        assert_eq!(map.insert(7, 11), Some(None));
        assert_eq!(map.insert(u64::MAX, 13), Some(None));

        assert_eq!(map.get(7), Some(11));
        assert_eq!(map.get(u64::MAX), Some(13));
        assert_eq!(map.insert(7, 17), Some(Some(11)));
        assert_eq!(map.remove(7), Some(17));
        assert_eq!(map.get(7), None);
        assert_eq!(map.insert(23, 19), Some(None));
        assert_eq!(map.get(23), Some(19));
    }

    #[test]
    fn fixed_map_delete_preserves_a_wrapped_collision_chain() {
        let mut map = FixedPrehashedMap::try_new(4).unwrap();
        let mut hashes = Vec::new();
        for fingerprint in 0_u32..1024 {
            if fixed_map_fingerprint_start(fingerprint, map.slots.len()) == map.slots.len() - 1 {
                hashes.push(u64::from(fingerprint.rotate_right(13)));
                if hashes.len() == 3 {
                    break;
                }
            }
        }
        assert_eq!(hashes.len(), 3);
        for (value, &hash) in hashes.iter().enumerate() {
            assert_eq!(map.insert(hash, value as u32), Some(None));
        }

        assert_eq!(map.remove(hashes[0]), Some(0));
        assert_eq!(map.get(hashes[0]), None);
        assert_eq!(map.get(hashes[1]), Some(1));
        assert_eq!(map.get(hashes[2]), Some(2));
        assert_eq!(map.remove(hashes[1]), Some(1));
        assert_eq!(map.get(hashes[2]), Some(2));
    }

    #[test]
    fn lifetime_key_turnover_restores_empty_directory_slots() {
        let mut map = FixedPrehashedMap::try_new(32).unwrap();
        let slot_count = map.slots.len();
        let mut hashes = vec![None; slot_count];
        for fingerprint in 0..u32::try_from(slot_count * 64).unwrap() {
            let start = fixed_map_fingerprint_start(fingerprint, slot_count);
            hashes[start].get_or_insert_with(|| u64::from(fingerprint.rotate_right(13)));
        }
        assert!(hashes.iter().all(Option::is_some));

        for (start, hash) in hashes.into_iter().enumerate() {
            let hash = hash.unwrap();
            assert_eq!(fixed_map_start(hash, slot_count), start);
            assert_eq!(map.insert(hash, start as u32), Some(None));
            assert_eq!(map.remove(hash), Some(start as u32));
        }

        assert!(map.slots.iter().all(|slot| slot.value == EMPTY_VALUE));
    }

    #[test]
    fn bounded_delete_compaction_matches_long_churn_reference() {
        let mut map = FixedPrehashedMap::try_new(32).unwrap();
        let mut expected = Vec::<(u64, u32)>::new();
        for ordinal in 0_u32..100_000 {
            if expected.len() == 32 {
                let victim = (ordinal as usize * 17) % expected.len();
                let (hash, value) = expected.swap_remove(victim);
                assert_eq!(map.remove(hash), Some(value));
            }

            let hash = u64::from(ordinal * 8) << 32;
            assert_eq!(map.insert(hash, ordinal), Some(None));
            expected.push((hash, ordinal));

            if ordinal % 11 == 0 {
                let victim = (ordinal as usize * 13) % expected.len();
                let (hash, value) = expected.swap_remove(victim);
                assert_eq!(map.remove(hash), Some(value));
            }
            if ordinal % 64 == 0 {
                for &(hash, value) in &expected {
                    assert_eq!(map.get(hash), Some(value));
                }
                let missing = u64::from(ordinal + 1_000_000) << 32;
                assert_eq!(map.get(missing), None);
            }
        }
    }

    #[test]
    fn empty_fixed_prehashed_map_cannot_insert() {
        let mut map = FixedPrehashedMap::try_new(0).unwrap();
        assert!(!map.can_upsert(1));
        assert_eq!(map.insert(1, 1), None);
    }

    #[test]
    fn fixed_map_retains_start_entropy_after_low_bit_shard_routing() {
        let mut map = FixedPrehashedMap::try_new(4096).unwrap();
        for ordinal in 0_u64..4096 {
            let hash = (ordinal << 32) | 31;
            assert_eq!(map.insert(hash, ordinal as u32), Some(None));
        }
        for ordinal in 0_u64..4096 {
            let hash = (ordinal << 32) | 31;
            assert_eq!(map.get(hash), Some(ordinal as u32));
        }
    }

    #[test]
    fn production_sized_fixed_map_fills_within_the_probe_budget() {
        const ENTRIES: usize = 40_960;
        let mut map = FixedPrehashedMap::try_new(ENTRIES).unwrap();
        let mut fingerprints = HashSet::with_capacity(ENTRIES);
        let mut hashes = Vec::with_capacity(ENTRIES);
        for ordinal in 0_u64..(ENTRIES as u64 * 2) {
            let hash = splitmix64(ordinal);
            if fingerprints.insert(fixed_map_fingerprint(hash)) {
                let value = hashes.len() as u32;
                assert_eq!(map.insert(hash, value), Some(None));
                hashes.push(hash);
                if hashes.len() == ENTRIES {
                    break;
                }
            }
        }
        assert_eq!(hashes.len(), ENTRIES);
        for (value, hash) in hashes.into_iter().enumerate() {
            assert_eq!(map.get(hash), Some(value as u32));
        }
    }

    #[test]
    fn fast_route_preserves_modulo_assignment() {
        for buckets in 1..=65 {
            for hash in [0, 1, 31, 32, 63, 64, u32::MAX as u64, u64::MAX] {
                assert_eq!(route_hash(hash, buckets), (hash % buckets as u64) as usize);
            }
        }
    }

    fn splitmix64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
