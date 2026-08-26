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
        let slots = &mut self.slots;
        if slots.is_empty() {
            return None;
        }
        let start = fixed_map_start(hash, slots.len());
        for step in 0..slots.len().min(MAX_FIXED_MAP_PROBES) {
            let slot = &mut slots[(start + step) & (slots.len() - 1)];
            if slot.value == EMPTY_VALUE {
                return None;
            }
            if slot.value != DELETED_VALUE && slot.hash == hash {
                let previous = slot.value;
                slot.value = DELETED_VALUE;
                return Some(previous);
            }
        }
        None
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
