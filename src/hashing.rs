use std::io;

const EMPTY_VALUE: u32 = u32::MAX;
const DELETED_VALUE: u32 = u32::MAX - 1;
const MAX_FIXED_MAP_PROBES: usize = 64;

#[derive(Clone, Copy)]
struct FixedMapSlot {
    hash: u64,
    value: u32,
}

impl Default for FixedMapSlot {
    fn default() -> Self {
        Self {
            hash: 0,
            value: EMPTY_VALUE,
        }
    }
}

/// Fixed-capacity map for cache hashes that are already seeded XXH3 results.
///
/// The table is allocated once during open. Point operations inspect a small
/// constant number of slots and never allocate, resize, or rehash.
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
        let start = fixed_map_start(hash, slots.len());
        for step in 0..slots.len().min(MAX_FIXED_MAP_PROBES) {
            let slot = slots[(start + step) & (slots.len() - 1)];
            if slot.value == EMPTY_VALUE {
                return None;
            }
            if slot.value != DELETED_VALUE && slot.hash == hash {
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
        self.slots[slot_index] = FixedMapSlot { hash, value };
        Some(previous)
    }

    pub(crate) fn remove(&mut self, hash: u64) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let start = fixed_map_start(hash, self.slots.len());
        for step in 0..self.slots.len().min(MAX_FIXED_MAP_PROBES) {
            let index = (start + step) & (self.slots.len() - 1);
            let slot = self.slots[index];
            if slot.value == EMPTY_VALUE {
                return None;
            }
            if slot.value != DELETED_VALUE && slot.hash == hash {
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
        debug_assert!(slot_count.is_power_of_two());
        let mask = slot_count - 1;
        let scan_steps = slot_count.saturating_sub(1).min(MAX_FIXED_MAP_PROBES);
        let mut cursor = (hole + 1) & mask;
        for _ in 0..scan_steps {
            let candidate = self.slots[cursor];
            if candidate.value == EMPTY_VALUE {
                self.slots[hole] = FixedMapSlot::default();
                return;
            }
            if candidate.value != DELETED_VALUE {
                let home = fixed_map_start(candidate.hash, slot_count);
                if probe_distance(home, hole, mask) < probe_distance(home, cursor, mask) {
                    self.slots[hole] = candidate;
                    self.slots[cursor].value = DELETED_VALUE;
                    hole = cursor;
                }
            }
            cursor = (cursor + 1) & mask;
        }
    }

    fn find_upsert_slot(&self, hash: u64) -> Option<(usize, Option<u32>)> {
        let slots = &self.slots;
        if slots.is_empty() {
            return None;
        }
        let start = fixed_map_start(hash, slots.len());
        let mut deleted = None;
        for step in 0..slots.len().min(MAX_FIXED_MAP_PROBES) {
            let index = (start + step) & (slots.len() - 1);
            let slot = slots[index];
            if slot.value == EMPTY_VALUE {
                return Some((deleted.unwrap_or(index), None));
            }
            if slot.value == DELETED_VALUE {
                deleted.get_or_insert(index);
            } else if slot.hash == hash {
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
            .and_then(usize::checked_next_power_of_two)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fixed map is too large"))
    }
}

fn fixed_map_start(hash: u64, slots: usize) -> usize {
    // L1 shard routing already consumes the low XXH3 bits. Rotate the same
    // precomputed hash so one shard still gets the full directory start space.
    route_hash(hash.rotate_left(32), slots)
}

fn probe_distance(home: usize, index: usize, mask: usize) -> usize {
    index.wrapping_sub(home) & mask
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
    use super::*;

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
        let hashes = [7_u64 << 32, 15_u64 << 32, 23_u64 << 32];
        for (value, hash) in hashes.into_iter().enumerate() {
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
        for start in 0..slot_count {
            let hash = (start as u64) << 32;
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

            let hash = (u64::from(ordinal & 7) << 32) | u64::from(ordinal);
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
                let missing = u64::from(ordinal) | (1_u64 << 63);
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
    fn fast_route_preserves_modulo_assignment() {
        for buckets in 1..=65 {
            for hash in [0, 1, 31, 32, 63, 64, u32::MAX as u64, u64::MAX] {
                assert_eq!(route_hash(hash, buckets), (hash % buckets as u64) as usize);
            }
        }
    }
}
