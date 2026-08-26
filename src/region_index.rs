//! Bounded point operations over the Region mmap index.
//!
//! This layer owns the sharded storage but deliberately does not own Region
//! lifecycle state. A lookup returns only the raw typed point state and
//! releases its canonical partition before record I/O. Mutations use sequence
//! order within one bounded partition probe. Record reads validate physical
//! Region identity locally, so index publication never needs global
//! Region-manager authority.

use crate::hashing::route_hash;
use crate::index::{IndexEntry, MAX_INDEX_PROBES};
use crate::index_storage::{
    IndexPartitionWriteGuard, IndexSlotState, IndexStorageError, PartitionedIndexStorage,
};

/// A bounded point index over one canonical [`PartitionedIndexStorage`].
pub(crate) struct RegionIndex {
    storage: PartitionedIndexStorage,
}

impl RegionIndex {
    pub(crate) const fn from_storage(storage: PartitionedIndexStorage) -> Self {
        Self { storage }
    }

    pub(crate) const fn storage(&self) -> &PartitionedIndexStorage {
        &self.storage
    }

    /// Looks up the raw typed state for one same-hash identity.
    ///
    /// Region-generation visibility deliberately does not run here. The read
    /// path validates the selected physical record locally after I/O.
    pub(crate) fn lookup_raw(&self, hash: u64) -> Result<Option<IndexEntry>, IndexStorageError> {
        let partition = self.storage.try_read_hash_partition(hash)?;
        let start = start_slot(hash, partition.slot_count());
        let mut found = None;
        partition.probe(
            start,
            probe_limit(partition.slot_count()),
            |_, state| match state {
                IndexSlotState::Empty => true,
                IndexSlotState::Deleted => false,
                IndexSlotState::Value {
                    hash: current_hash,
                    entry,
                } => {
                    if current_hash == hash {
                        found = Some(entry);
                        true
                    } else {
                        false
                    }
                }
            },
        )?;
        Ok(found)
    }

    /// Installs a live value using the existing bounded replacement policy.
    ///
    /// Deleted slots are reused first. If the window has no reusable slot, the
    /// first foreign value becomes the bounded eviction victim. A delayed
    /// older publication cannot replace a newer same-hash entry.
    pub(crate) fn upsert(
        &self,
        hash: u64,
        supplied: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        if supplied.seqno == 0 {
            return Ok(false);
        }

        let installed = IndexSlotState::Value {
            hash,
            entry: supplied,
        };
        self.install_if_newer(hash, supplied.seqno, installed)
    }

    fn install_if_newer(
        &self,
        hash: u64,
        supplied_seqno: u64,
        installed: IndexSlotState,
    ) -> Result<bool, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let start = start_slot(hash, slot_count);
        let mut reusable = None;
        let mut victim = None;
        let mut apply = None;
        let mut rejected = false;

        partition.probe(
            start,
            probe_limit(slot_count),
            |local_slot, state| match state {
                IndexSlotState::Empty => {
                    apply = Some(reusable.unwrap_or((local_slot, state)));
                    true
                }
                IndexSlotState::Deleted => {
                    reusable.get_or_insert((local_slot, state));
                    false
                }
                IndexSlotState::Value {
                    hash: current_hash,
                    entry: current,
                } => {
                    if current_hash == hash {
                        if supplied_seqno <= current.seqno {
                            rejected = true;
                        } else {
                            apply = Some((local_slot, state));
                        }
                        true
                    } else {
                        if victim.is_none() {
                            victim = Some((local_slot, state));
                        }
                        false
                    }
                }
            },
        )?;
        if rejected {
            return Ok(false);
        }

        let Some((local_slot, previous)) = apply.or(reusable).or(victim) else {
            return Ok(false);
        };
        partition.replace_observed(local_slot, previous, installed)?;
        Ok(true)
    }

    /// Replaces one exact immutable record identity with the canonical
    /// probe-deleted marker.
    #[cfg(test)]
    fn remove_if(&self, hash: u64, expected: IndexEntry) -> Result<bool, IndexStorageError> {
        let partition = self.storage.write_hash_partition(hash)?;
        self.remove_exact_in_partition(hash, expected, partition)
    }

    /// Tries to remove one exact immutable record without waiting for its
    /// index partition. Foreground expiry cleanup treats contention as a miss.
    pub(crate) fn try_remove_if(
        &self,
        hash: u64,
        expected: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        let partition = self.storage.try_write_hash_partition(hash)?;
        self.remove_exact_in_partition(hash, expected, partition)
    }

    fn remove_exact_in_partition(
        &self,
        hash: u64,
        expected: IndexEntry,
        mut partition: IndexPartitionWriteGuard<'_>,
    ) -> Result<bool, IndexStorageError> {
        let slot_count = partition.slot_count();
        let start = start_slot(hash, slot_count);
        let mut target = None;
        partition.probe(
            start,
            probe_limit(slot_count),
            |local_slot, state| match state {
                IndexSlotState::Empty => true,
                IndexSlotState::Deleted => false,
                IndexSlotState::Value {
                    hash: current_hash,
                    entry,
                } => {
                    if current_hash == hash {
                        if entry.same_record_identity(expected) {
                            target = Some((local_slot, state));
                        }
                        true
                    } else {
                        false
                    }
                }
            },
        )?;
        let Some((local_slot, previous)) = target else {
            return Ok(false);
        };
        partition.replace_observed(local_slot, previous, IndexSlotState::Deleted)?;
        Ok(true)
    }
}

fn start_slot(hash: u64, slot_count: usize) -> usize {
    debug_assert_ne!(slot_count, 0);
    route_hash(hash, slot_count)
}

fn probe_limit(slot_count: usize) -> usize {
    slot_count.min(MAX_INDEX_PROBES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PackedLocation;
    use crate::index_storage::{
        CorruptPageReason, INDEX_IMAGE_PAGE_HEADER_SIZE, INDEX_IMAGE_PAGE_SIZE, IndexImageBinding,
        IndexPhysicalStats,
    };
    use std::fs::{File, OpenOptions};
    use std::io::Write;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEST_FILE: AtomicU64 = AtomicU64::new(0);

    const BINDING: IndexImageBinding = IndexImageBinding {
        generation: 17,
        image_tag: 0x0102_0304_0506_0708,
    };

    struct TestFile {
        path: PathBuf,
        file: File,
    }

    impl TestFile {
        fn create() -> Self {
            let id = NEXT_TEST_FILE.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("cache-rs-index-{}-{id}.tmp", std::process::id()));
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

    fn entry(region_id: u32, offset: u32, seqno: u64) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::new(region_id, offset, 32).unwrap(),
            seqno,
            namespace_id: region_id + 100,
        }
    }

    fn anonymous(slot_count: usize) -> RegionIndex {
        RegionIndex::from_storage(PartitionedIndexStorage::anonymous(slot_count).unwrap())
    }

    #[test]
    fn lookup_returns_busy_instead_of_waiting_for_partition_mutation() {
        let index = anonymous(8);
        let hash = 7;
        let _mutation = index.storage().write_hash_partition(hash).unwrap();

        assert!(matches!(
            index.lookup_raw(hash),
            Err(IndexStorageError::PartitionBusy { .. })
        ));
    }

    #[test]
    fn expiry_cleanup_returns_busy_without_removing_the_record() {
        let index = anonymous(8);
        let hash = 7;
        let current = entry(1, 8, 10);
        assert!(index.upsert(hash, current).unwrap());
        let mutation = index.storage().write_hash_partition(hash).unwrap();

        assert!(matches!(
            index.try_remove_if(hash, current),
            Err(IndexStorageError::PartitionBusy { .. })
        ));
        drop(mutation);
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(current));
    }

    #[test]
    fn newer_versions_win_and_removal_matches_record_identity() {
        let index = anonymous(8);
        let hash = 7;
        let first = entry(1, 8, 10);
        let older = entry(2, 16, 9);
        let second = entry(3, 24, 11);
        assert!(index.upsert(hash, first).unwrap());
        assert!(!index.upsert(hash, older).unwrap());
        assert!(index.upsert(hash, second).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(second));

        assert!(!index.remove_if(hash, first).unwrap());
        assert!(index.remove_if(hash, second).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
    }

    #[test]
    fn raw_lookup_needs_no_visibility_callback() {
        let index = anonymous(8);
        let hash = 5;
        let current = entry(2, 16, 9);
        assert!(index.upsert(hash, current).unwrap());

        assert_eq!(index.lookup_raw(hash).unwrap(), Some(current));
    }

    #[test]
    fn foreign_values_share_the_bounded_probe_window() {
        let index = anonymous(8);
        let stale = entry(1, 8, 10);
        let supplied = entry(2, 16, 11);
        index.upsert(1, stale).unwrap();

        assert!(index.upsert(9, supplied).unwrap());
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 2,
                deleted: 0,
            }
        );
    }

    #[test]
    fn a_probe_crossing_a_corrupt_page_returns_the_fault_without_mutation() {
        const SLOT_COUNT: usize = 133;
        const HASH: u64 = 124;

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        {
            let mut partition = source.write_hash_partition(HASH).unwrap();
            partition
                .replace_observed(124, IndexSlotState::Empty, IndexSlotState::Deleted)
                .unwrap();
            partition
                .replace_observed(125, IndexSlotState::Empty, IndexSlotState::Deleted)
                .unwrap();
        }
        let partition_stats = source.partition_stats().unwrap();
        let mut image = Vec::new();
        source.write_warm_image(&mut image, BINDING).unwrap();
        image[INDEX_IMAGE_PAGE_SIZE + INDEX_IMAGE_PAGE_HEADER_SIZE + 7] ^= 0x80;

        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();
        let recovered = PartitionedIndexStorage::map_private(
            &test_file.file,
            0,
            SLOT_COUNT,
            BINDING,
            &partition_stats,
        )
        .unwrap();
        let index = RegionIndex::from_storage(recovered);
        let before = index.storage().partition_stats().unwrap();
        assert!(matches!(
            index.upsert(HASH, entry(3, 24, 20)),
            Err(IndexStorageError::CorruptPage {
                page_index: 1,
                reason: CorruptPageReason::ChecksumMismatch { .. },
            })
        ));
        assert_eq!(index.storage().partition_stats().unwrap(), before);
    }

    #[test]
    fn recovered_two_partition_image_serves_hash_lookup_without_rebuild() {
        const SLOT_COUNT: usize = 134;
        const HASH_IN_SECOND_SHARD: u64 = 1;

        let source = anonymous(SLOT_COUNT);
        let value = entry(5, 40, 21);
        assert!(source.upsert(HASH_IN_SECOND_SHARD, value).unwrap());
        assert_eq!(source.storage().partition_count(), 2);
        let partition_stats = source.storage().partition_stats().unwrap();
        let mut image = Vec::new();
        source
            .storage()
            .write_warm_image(&mut image, BINDING)
            .unwrap();

        let mut test_file = TestFile::create();
        test_file.file.write_all(&image).unwrap();
        test_file.file.sync_all().unwrap();
        let recovered = RegionIndex::from_storage(
            PartitionedIndexStorage::map_private(
                &test_file.file,
                0,
                SLOT_COUNT,
                BINDING,
                &partition_stats,
            )
            .unwrap(),
        );

        assert_eq!(
            recovered.lookup_raw(HASH_IN_SECOND_SHARD).unwrap(),
            Some(value)
        );
        assert_eq!(
            recovered.storage().partition_stats().unwrap(),
            partition_stats
        );
    }
}
