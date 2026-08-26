use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// A map whose `u64` keys are already the cache's seeded XXH3 result.
///
/// Feeding that value through another hasher only adds work inside the shard
/// critical section. Full-key validation still handles the bounded collision
/// chain owned by each caller.
pub(crate) type PrehashedMap<V> = HashMap<u64, V, BuildHasherDefault<PrehashedU64Hasher>>;

#[derive(Default)]
pub(crate) struct PrehashedU64Hasher(u64);

impl Hasher for PrehashedU64Hasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, _bytes: &[u8]) {
        unreachable!("PrehashedU64Hasher accepts only u64 keys")
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = value;
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
    use super::*;

    #[test]
    fn prehashed_map_uses_the_supplied_hash_and_keeps_keys_distinct() {
        let mut map = PrehashedMap::default();
        map.insert(7, 11);
        map.insert(u64::MAX, 13);

        assert_eq!(map.get(&7), Some(&11));
        assert_eq!(map.get(&u64::MAX), Some(&13));
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
