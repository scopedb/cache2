//! Fixed-capacity L1 hash directory with compact heads and bounded cuckoo work.

use std::io;

const GROUP_SIZE: usize = size_of::<u64>();
const MAX_FAST_KICKS: usize = 8;
const MAX_CUCKOO_GROUPS: usize = 32;
const EMPTY: u8 = u8::MAX;
const LOAD_NUMERATOR: usize = 7;
const LOAD_DENOMINATOR: usize = 8;
const BYTE_ONES: u64 = 0x0101_0101_0101_0101;
const BYTE_HIGHS: u64 = 0x8080_8080_8080_8080;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DirectoryUpsert {
    Inserted,
    Replaced(u32),
    Full,
}

struct CuckooPath {
    buckets: [usize; MAX_CUCKOO_GROUPS],
    len: usize,
    empty: usize,
}

pub(crate) struct MemoryDirectory {
    controls: Box<[u8]>,
    heads: Box<[u32]>,
    len: usize,
    maximum_entries: usize,
}

impl MemoryDirectory {
    pub(crate) fn new(maximum_entries: usize) -> io::Result<Self> {
        if maximum_entries == 0 {
            return Ok(Self {
                controls: Box::new([]),
                heads: Box::new([]),
                len: 0,
                maximum_entries: 0,
            });
        }
        let minimum_buckets = maximum_entries
            .checked_mul(LOAD_DENOMINATOR)
            .and_then(|buckets| buckets.checked_add(LOAD_NUMERATOR - 1))
            .map(|buckets| buckets / LOAD_NUMERATOR)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "L1 directory is too large")
            })?;
        let bucket_count = minimum_buckets
            .max(GROUP_SIZE)
            .checked_next_power_of_two()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "L1 directory is too large")
            })?;

        let mut controls = Vec::new();
        controls.try_reserve_exact(bucket_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate L1 directory controls",
            )
        })?;
        controls.resize(bucket_count, EMPTY);
        let mut heads = Vec::new();
        heads.try_reserve_exact(bucket_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate L1 directory heads",
            )
        })?;
        heads.resize(bucket_count, 0);
        Ok(Self {
            controls: controls.into_boxed_slice(),
            heads: heads.into_boxed_slice(),
            len: 0,
            maximum_entries,
        })
    }

    #[inline]
    pub(crate) fn get<F>(&self, hash: u64, mut equal: F) -> Option<u32>
    where
        F: FnMut(u32) -> bool,
    {
        self.find_bucket(hash, &mut equal)
            .map(|bucket| self.heads[bucket])
    }

    #[inline]
    pub(crate) fn can_upsert<F, H>(&self, hash: u64, mut equal: F, mut hash_of: H) -> bool
    where
        F: FnMut(u32) -> bool,
        H: FnMut(u32) -> u64,
    {
        self.find_bucket(hash, &mut equal).is_some() || self.find_path(hash, &mut hash_of).is_some()
    }

    #[inline]
    pub(crate) fn upsert<F, H>(
        &mut self,
        hash: u64,
        head: u32,
        mut equal: F,
        mut hash_of: H,
    ) -> DirectoryUpsert
    where
        F: FnMut(u32) -> bool,
        H: FnMut(u32) -> u64,
    {
        if let Some(bucket) = self.find_bucket(hash, &mut equal) {
            let previous = std::mem::replace(&mut self.heads[bucket], head);
            return DirectoryUpsert::Replaced(previous);
        }
        if self.len >= self.maximum_entries {
            return DirectoryUpsert::Full;
        }
        let Some(path) = self.find_path(hash, &mut hash_of) else {
            return DirectoryUpsert::Full;
        };

        let mut empty = path.empty;
        for ordinal in (0..path.len).rev() {
            let occupied = path.buckets[ordinal];
            self.controls[empty] = self.controls[occupied];
            self.heads[empty] = self.heads[occupied];
            empty = occupied;
        }
        self.controls[empty] = hash_tag(hash);
        self.heads[empty] = head;
        self.len += 1;
        DirectoryUpsert::Inserted
    }

    #[inline]
    pub(crate) fn remove<F>(&mut self, hash: u64, mut equal: F) -> Option<u32>
    where
        F: FnMut(u32) -> bool,
    {
        let bucket = self.find_bucket(hash, &mut equal)?;
        let head = self.heads[bucket];
        self.controls[bucket] = EMPTY;
        self.len -= 1;
        Some(head)
    }

    #[cfg(test)]
    pub(crate) fn allocation_bytes(&self) -> usize {
        self.controls
            .len()
            .saturating_add(self.heads.len().saturating_mul(size_of::<u32>()))
    }

    #[inline]
    fn find_bucket<F>(&self, hash: u64, equal: &mut F) -> Option<usize>
    where
        F: FnMut(u32) -> bool,
    {
        if self.controls.is_empty() {
            return None;
        }
        let (primary, secondary) = hash_groups(hash, self.group_mask());
        self.find_in_group(primary, hash, equal).or_else(|| {
            (secondary != primary)
                .then(|| self.find_in_group(secondary, hash, equal))
                .flatten()
        })
    }

    #[inline]
    fn find_in_group<F>(&self, group: usize, hash: u64, equal: &mut F) -> Option<usize>
    where
        F: FnMut(u32) -> bool,
    {
        let start = group * GROUP_SIZE;
        let controls = control_word(&self.controls[start..start + GROUP_SIZE]);
        let tag = hash_tag(hash);
        let mut matches = matching_bytes(controls, tag);
        while matches != 0 {
            let byte = matches.trailing_zeros() as usize / 8;
            let bucket = start + byte;
            if self.controls[bucket] == tag && equal(self.heads[bucket]) {
                return Some(bucket);
            }
            matches &= matches - 1;
        }
        None
    }

    fn find_path<H>(&self, hash: u64, hash_of: &mut H) -> Option<CuckooPath>
    where
        H: FnMut(u32) -> u64,
    {
        if self.controls.is_empty() {
            return None;
        }
        let group_mask = self.group_mask();
        let (primary, secondary) = hash_groups(hash, group_mask);
        if let Some(empty) = self.empty_in_group(primary) {
            return Some(CuckooPath {
                buckets: [0; MAX_CUCKOO_GROUPS],
                len: 0,
                empty,
            });
        }
        if secondary != primary {
            if let Some(empty) = self.empty_in_group(secondary) {
                return Some(CuckooPath {
                    buckets: [0; MAX_CUCKOO_GROUPS],
                    len: 0,
                    empty,
                });
            }
        } else {
            return None;
        }

        let mut buckets = [0; MAX_CUCKOO_GROUPS];
        let mut fast_groups = [usize::MAX; MAX_FAST_KICKS + 1];
        let mut displaced_hash = hash;
        let mut current_group = if hash & 1 == 0 { primary } else { secondary };
        fast_groups[0] = current_group;
        for kick in 0..MAX_FAST_KICKS {
            let occupied = current_group * GROUP_SIZE + victim_offset(displaced_hash, kick);
            buckets[kick] = occupied;
            let occupant_hash = hash_of(self.heads[occupied]);
            let (occupant_primary, occupant_secondary) = hash_groups(occupant_hash, group_mask);
            let next_group = if current_group == occupant_primary {
                occupant_secondary
            } else if current_group == occupant_secondary {
                occupant_primary
            } else {
                return None;
            };
            if fast_groups[..kick + 1].contains(&next_group) {
                break;
            }
            if let Some(empty) = self.empty_in_group(next_group) {
                return Some(CuckooPath {
                    buckets,
                    len: kick + 1,
                    empty,
                });
            }
            fast_groups[kick + 1] = next_group;
            displaced_hash = occupant_hash;
            current_group = next_group;
        }

        let mut groups = [0; MAX_CUCKOO_GROUPS];
        let mut parent_nodes = [usize::MAX; MAX_CUCKOO_GROUPS];
        let mut parent_buckets = [usize::MAX; MAX_CUCKOO_GROUPS];
        groups[0] = primary;
        groups[1] = secondary;
        let mut queued = 2;
        let mut cursor = 0;
        while cursor < queued {
            let group = groups[cursor];
            if let Some(empty) = self.empty_in_group(group) {
                let mut reverse = [0; MAX_CUCKOO_GROUPS];
                let mut len = 0;
                let mut node = cursor;
                while parent_buckets[node] != usize::MAX {
                    reverse[len] = parent_buckets[node];
                    len += 1;
                    node = parent_nodes[node];
                }
                let mut buckets = [0; MAX_CUCKOO_GROUPS];
                for ordinal in 0..len {
                    buckets[ordinal] = reverse[len - ordinal - 1];
                }
                return Some(CuckooPath {
                    buckets,
                    len,
                    empty,
                });
            }

            let start = group * GROUP_SIZE;
            for occupied in start..start + GROUP_SIZE {
                let occupant_hash = hash_of(self.heads[occupied]);
                let (occupant_primary, occupant_secondary) = hash_groups(occupant_hash, group_mask);
                let next_group = if group == occupant_primary {
                    occupant_secondary
                } else if group == occupant_secondary {
                    occupant_primary
                } else {
                    return None;
                };
                if next_group == group || groups[..queued].contains(&next_group) {
                    continue;
                }
                if queued == MAX_CUCKOO_GROUPS {
                    continue;
                }
                groups[queued] = next_group;
                parent_nodes[queued] = cursor;
                parent_buckets[queued] = occupied;
                queued += 1;
            }
            cursor += 1;
        }
        None
    }

    #[inline]
    fn empty_in_group(&self, group: usize) -> Option<usize> {
        let start = group * GROUP_SIZE;
        self.controls[start..start + GROUP_SIZE]
            .iter()
            .position(|control| *control == EMPTY)
            .map(|offset| start + offset)
    }

    fn group_mask(&self) -> usize {
        self.controls.len() / GROUP_SIZE - 1
    }
}

#[inline]
fn hash_groups(hash: u64, group_mask: usize) -> (usize, usize) {
    let primary = hash as usize & group_mask;
    if group_mask == 0 {
        return (primary, primary);
    }
    let folded = hash ^ hash.rotate_left(23) ^ (hash >> 29);
    let mut secondary = folded.wrapping_mul(0x9e37_79b9_7f4a_7c15) as usize & group_mask;
    if secondary == primary {
        secondary = (secondary + 1) & group_mask;
    }
    (primary, secondary)
}

#[inline]
fn victim_offset(hash: u64, kick: usize) -> usize {
    ((hash >> 7) as usize).wrapping_add(kick.wrapping_mul(5)) & (GROUP_SIZE - 1)
}

#[inline]
fn hash_tag(hash: u64) -> u8 {
    (hash >> 57) as u8
}

#[inline]
fn control_word(controls: &[u8]) -> u64 {
    u64::from_ne_bytes(
        controls
            .try_into()
            .expect("directory control group has a fixed width"),
    )
}

#[inline]
fn matching_bytes(controls: u64, byte: u8) -> u64 {
    let difference = controls ^ u64::from(byte).wrapping_mul(BYTE_ONES);
    difference.wrapping_sub(BYTE_ONES) & !difference & BYTE_HIGHS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_replace_remove_and_empty_reuse() {
        let mut directory = MemoryDirectory::new(8).unwrap();
        assert_eq!(
            directory.upsert(7, 1, |head| head == 1, |_| 7),
            DirectoryUpsert::Inserted
        );
        assert_eq!(directory.get(7, |head| head == 1), Some(1));
        assert_eq!(
            directory.upsert(7, 2, |head| head == 1, |_| 7),
            DirectoryUpsert::Replaced(1)
        );
        assert_eq!(directory.remove(7, |head| head == 2), Some(2));
        assert_eq!(directory.get(7, |_| true), None);
        assert_eq!(
            directory.upsert(7, 3, |_| false, |_| 7),
            DirectoryUpsert::Inserted
        );
        assert_eq!(directory.get(7, |head| head == 3), Some(3));
    }

    #[test]
    fn full_hash_validation_disambiguates_equal_tags() {
        let mut directory = MemoryDirectory::new(8).unwrap();
        let first = 3_u64;
        let second = first | (1_u64 << 20);
        let hashes = [first, second];
        assert_eq!(hash_tag(first), hash_tag(second));
        assert_eq!(
            directory.upsert(first, 0, |_| false, |head| hashes[head as usize]),
            DirectoryUpsert::Inserted
        );
        assert_eq!(
            directory.upsert(second, 1, |_| false, |head| hashes[head as usize]),
            DirectoryUpsert::Inserted
        );
        assert_eq!(directory.get(first, |head| head == 0), Some(0));
        assert_eq!(directory.get(second, |head| head == 1), Some(1));
    }

    #[test]
    fn directory_storage_is_five_bytes_per_bucket() {
        let directory = MemoryDirectory::new(7).unwrap();
        assert_eq!(directory.allocation_bytes(), 8 * 5);
    }

    #[test]
    fn configured_load_remains_reachable_during_churn() {
        const ENTRIES: usize = 7 * 1024;
        let mut directory = MemoryDirectory::new(ENTRIES).unwrap();
        let mut hashes: Vec<_> = (0..ENTRIES).map(|ordinal| mix(ordinal as u64)).collect();
        for (ordinal, hash) in hashes.iter().copied().enumerate() {
            assert_eq!(
                directory.upsert(
                    hash,
                    ordinal as u32,
                    |head| hashes[head as usize] == hash,
                    |head| hashes[head as usize]
                ),
                DirectoryUpsert::Inserted,
                "directory exhausted at {ordinal}/{ENTRIES} entries"
            );
        }

        for operation in 0..ENTRIES * 2 {
            let index = operation % ENTRIES;
            let old_hash = hashes[index];
            assert_eq!(
                directory.remove(old_hash, |head| hashes[head as usize] == old_hash),
                Some(index as u32)
            );
            let new_hash = mix((ENTRIES + operation) as u64);
            hashes[index] = new_hash;
            assert_eq!(
                directory.upsert(
                    new_hash,
                    index as u32,
                    |head| hashes[head as usize] == new_hash,
                    |head| hashes[head as usize]
                ),
                DirectoryUpsert::Inserted
            );
        }
        for (ordinal, hash) in hashes.iter().copied().enumerate() {
            assert_eq!(
                directory.get(hash, |head| hashes[head as usize] == hash),
                Some(ordinal as u32)
            );
        }
    }

    fn mix(mut value: u64) -> u64 {
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }
}
