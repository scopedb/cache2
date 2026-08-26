//! Bounded point operations over the Region mmap index.
//!
//! This layer owns the sharded storage and one monotonic created-sequence
//! watermark per Region. A lookup returns only the raw typed point state and
//! releases its canonical partition before record I/O. Mutations use sequence
//! order within one bounded partition probe. Region reuse advances one
//! watermark so obsolete slots become logical tombstones without an index
//! scan or Region-manager access on point operations.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::hashing::route_hash;
use crate::index::{IndexEntry, MAX_INDEX_PROBES};
use crate::index_storage::{
    IndexPartitionWriteGuard, IndexSlotState, IndexStorageError, PartitionedIndexStorage,
};
use crate::snapshot::CacheIndexSnapshot;

/// A bounded point index over one canonical [`PartitionedIndexStorage`].
pub(crate) struct RegionIndex {
    storage: PartitionedIndexStorage,
    region_created_seqnos: Box<[AtomicU64]>,
    statistics_enabled: AtomicBool,
    deleted_slot_reuses: AtomicU64,
    stale_slot_reuses: AtomicU64,
    live_slot_replacements: AtomicU64,
}

#[derive(Clone, Copy)]
enum InstallKind {
    Existing,
    Empty,
    Deleted,
    Stale,
    LiveVictim,
}

impl RegionIndex {
    pub(crate) fn try_from_storage(
        storage: PartitionedIndexStorage,
        created_seqnos: impl ExactSizeIterator<Item = u64>,
    ) -> io::Result<Self> {
        let mut region_created_seqnos = Vec::new();
        region_created_seqnos
            .try_reserve_exact(created_seqnos.len())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate Region index generation table",
                )
            })?;
        region_created_seqnos.extend(created_seqnos.map(AtomicU64::new));
        Ok(Self {
            storage,
            region_created_seqnos: region_created_seqnos.into_boxed_slice(),
            statistics_enabled: AtomicBool::new(false),
            deleted_slot_reuses: AtomicU64::new(0),
            stale_slot_reuses: AtomicU64::new(0),
            live_slot_replacements: AtomicU64::new(0),
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
        let occupied = physical
            .value
            .checked_add(physical.deleted)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        let empty_slots = slot_capacity
            .checked_sub(occupied)
            .ok_or(IndexStorageError::InvalidPhysicalStats)?;
        Ok(CacheIndexSnapshot {
            slot_capacity,
            physical_value_slots: physical.value,
            deleted_slots: physical.deleted,
            empty_slots,
            deleted_slot_reuses: self.deleted_slot_reuses.load(Ordering::Relaxed),
            stale_slot_reuses: self.stale_slot_reuses.load(Ordering::Relaxed),
            live_slot_replacements: self.live_slot_replacements.load(Ordering::Relaxed),
        })
    }

    /// Advances one Region's logical generation after reuse.
    ///
    /// This is the complete reclaim-side index cleanup. Point operations turn
    /// older slots into misses or bounded-probe reuse candidates lazily.
    pub(crate) fn publish_region_generation(&self, region_id: u32, created_seqno: u64) -> bool {
        let Some(watermark) = self.region_created_seqnos.get(region_id as usize) else {
            return false;
        };
        if created_seqno == 0 {
            return false;
        }
        watermark.fetch_max(created_seqno, Ordering::Relaxed);
        true
    }

    /// Looks up the raw typed state for one same-hash identity.
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
                IndexSlotState::Tombstone {
                    hash: current_hash, ..
                } => current_hash == hash,
                IndexSlotState::Value {
                    hash: current_hash,
                    entry,
                } => {
                    if current_hash == hash {
                        if !self.is_stale(entry) {
                            found = Some(entry);
                        }
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
    /// Deleted slots are reused first, followed by stale foreign slots. If the
    /// window has no reusable slot, the first live foreign value becomes the
    /// bounded eviction victim. A delayed older publication cannot replace a
    /// newer same-hash entry.
    pub(crate) fn upsert(
        &self,
        hash: u64,
        supplied: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        if supplied.seqno == 0 || self.is_stale(supplied) {
            return Ok(false);
        }

        let installed = IndexSlotState::Value {
            hash,
            entry: supplied,
        };
        self.install_if_newer(hash, supplied.seqno, installed, true)
    }

    /// Replaces a matching entry with a sequenced logical delete. An already
    /// missing hash consumes no slot and never evicts an unrelated live value.
    pub(crate) fn upsert_tombstone(
        &self,
        hash: u64,
        seqno: u64,
    ) -> Result<bool, IndexStorageError> {
        if seqno == 0 {
            return Ok(false);
        }
        self.install_if_newer(
            hash,
            seqno,
            IndexSlotState::Tombstone { hash, seqno },
            false,
        )
    }

    fn install_if_newer(
        &self,
        hash: u64,
        supplied_seqno: u64,
        installed: IndexSlotState,
        install_missing: bool,
    ) -> Result<bool, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash)?;
        let slot_count = partition.slot_count();
        let start = start_slot(hash, slot_count);
        let mut deleted = None;
        let mut stale = None;
        let mut victim = None;
        let mut apply = None;
        let mut rejected = false;

        partition.probe(
            start,
            probe_limit(slot_count),
            |local_slot, state| match state {
                IndexSlotState::Empty => {
                    if install_missing {
                        apply = Some(deleted.or(stale).unwrap_or((
                            local_slot,
                            state,
                            InstallKind::Empty,
                        )));
                    }
                    true
                }
                IndexSlotState::Deleted => {
                    deleted.get_or_insert((local_slot, state, InstallKind::Deleted));
                    false
                }
                IndexSlotState::Tombstone {
                    hash: current_hash,
                    seqno: current_seqno,
                } => {
                    if current_hash == hash {
                        if supplied_seqno <= current_seqno {
                            rejected = true;
                        } else {
                            apply = Some((local_slot, state, InstallKind::Existing));
                        }
                        true
                    } else {
                        deleted.get_or_insert((local_slot, state, InstallKind::Deleted));
                        false
                    }
                }
                IndexSlotState::Value {
                    hash: current_hash,
                    entry: current,
                } => {
                    if current_hash == hash {
                        if supplied_seqno <= current.seqno {
                            rejected = true;
                        } else {
                            apply = Some((local_slot, state, InstallKind::Existing));
                        }
                        true
                    } else {
                        if self.is_stale(current) {
                            stale.get_or_insert((local_slot, state, InstallKind::Stale));
                        } else if install_missing && victim.is_none() {
                            victim = Some((local_slot, state, InstallKind::LiveVictim));
                        }
                        false
                    }
                }
            },
        )?;
        if rejected {
            return Ok(false);
        }

        let fallback = if install_missing {
            deleted.or(stale).or(victim)
        } else {
            None
        };
        let Some((local_slot, previous, kind)) = apply.or(fallback) else {
            return Ok(false);
        };
        partition.replace_observed(local_slot, previous, installed)?;
        if matches!(
            kind,
            InstallKind::Deleted | InstallKind::Stale | InstallKind::LiveVictim
        ) && self.statistics_enabled.load(Ordering::Relaxed)
        {
            match kind {
                InstallKind::Deleted => {
                    self.deleted_slot_reuses.fetch_add(1, Ordering::Relaxed);
                }
                InstallKind::Stale => {
                    self.stale_slot_reuses.fetch_add(1, Ordering::Relaxed);
                }
                InstallKind::LiveVictim => {
                    self.live_slot_replacements.fetch_add(1, Ordering::Relaxed);
                }
                InstallKind::Existing | InstallKind::Empty => {}
            }
        }
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
                IndexSlotState::Tombstone {
                    hash: current_hash, ..
                } => current_hash == hash,
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

    fn is_stale(&self, entry: IndexEntry) -> bool {
        self.region_created_seqnos
            .get(entry.location.region_id() as usize)
            .is_some_and(|watermark| entry.seqno < watermark.load(Ordering::Relaxed))
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
        }
    }

    fn anonymous(slot_count: usize) -> RegionIndex {
        let index = RegionIndex::try_from_storage(
            PartitionedIndexStorage::anonymous(slot_count).unwrap(),
            (0..16).map(|_| 0),
        )
        .unwrap();
        index.set_statistics_enabled(true);
        index
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
        assert!(index.upsert(15, entry(4, 32, 12)).unwrap());
        assert_eq!(index.snapshot().unwrap().deleted_slot_reuses, 1);
    }

    #[test]
    fn tombstones_order_puts_and_deletes_by_sequence() {
        let index = anonymous(8);
        let hash = 7;
        let first = entry(1, 8, 10);
        let newer = entry(2, 16, 12);

        assert!(index.upsert(hash, first).unwrap());
        assert!(!index.upsert_tombstone(hash, 9).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(first));

        assert!(index.upsert_tombstone(hash, 11).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
        assert!(!index.upsert(hash, first).unwrap());

        assert!(index.upsert(hash, newer).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(newer));
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
            }
        );
    }

    #[test]
    fn missing_delete_does_not_replace_a_live_foreign_value() {
        let empty = anonymous(8);
        assert!(!empty.upsert_tombstone(8, 20).unwrap());
        assert_eq!(
            empty.storage().physical_stats().unwrap(),
            IndexPhysicalStats::default()
        );

        let index = anonymous(8);
        for hash in 0..8 {
            assert!(
                index
                    .upsert(hash, entry(0, hash as u32 * 32, hash + 1))
                    .unwrap()
            );
        }

        assert!(!index.upsert_tombstone(8, 20).unwrap());
        for hash in 0..8 {
            assert_eq!(
                index.lookup_raw(hash).unwrap(),
                Some(entry(0, hash as u32 * 32, hash + 1))
            );
        }
        assert_eq!(index.snapshot().unwrap().live_slot_replacements, 0);
    }

    #[test]
    fn generation_advance_hides_old_slots_and_rejects_delayed_publication() {
        let index = anonymous(8);
        let hash = 5;
        let current = entry(2, 16, 9);
        assert!(index.upsert(hash, current).unwrap());

        assert!(index.publish_region_generation(2, 10));
        assert_eq!(index.lookup_raw(hash).unwrap(), None);
        assert!(!index.upsert(hash, current).unwrap());

        let replacement = entry(2, 24, 11);
        assert!(index.upsert(hash, replacement).unwrap());
        assert_eq!(index.lookup_raw(hash).unwrap(), Some(replacement));
    }

    #[test]
    fn stale_foreign_slot_is_reused_before_a_live_victim() {
        let index = anonymous(8);
        let live = entry(0, 0, 10);
        {
            let mut partition = index.storage().write_hash_partition(0).unwrap();
            partition
                .replace_observed(
                    0,
                    IndexSlotState::Empty,
                    IndexSlotState::Value {
                        hash: 0,
                        entry: live,
                    },
                )
                .unwrap();
            for local_slot in 1..8 {
                partition
                    .replace_observed(
                        local_slot,
                        IndexSlotState::Empty,
                        IndexSlotState::Value {
                            hash: local_slot as u64,
                            entry: entry(1, local_slot as u32 * 32, 10),
                        },
                    )
                    .unwrap();
            }
        }
        assert!(index.publish_region_generation(0, 1));
        assert!(index.publish_region_generation(1, 20));

        let supplied = entry(0, 32, 30);
        assert!(index.upsert(8, supplied).unwrap());
        assert_eq!(index.lookup_raw(0).unwrap(), Some(live));
        assert_eq!(index.lookup_raw(8).unwrap(), Some(supplied));
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 8,
                deleted: 0,
            }
        );
        let snapshot = index.snapshot().unwrap();
        assert_eq!(snapshot.stale_slot_reuses, 1);
        assert_eq!(snapshot.live_slot_replacements, 0);
    }

    #[test]
    fn full_probe_window_counts_live_replacement() {
        let index = anonymous(8);
        for hash in 0..8 {
            assert!(
                index
                    .upsert(hash, entry(0, hash as u32 * 32, hash + 1))
                    .unwrap()
            );
        }
        assert!(index.upsert(8, entry(1, 0, 20)).unwrap());

        let snapshot = index.snapshot().unwrap();
        assert_eq!(snapshot.slot_capacity, 8);
        assert_eq!(snapshot.physical_value_slots, 8);
        assert_eq!(snapshot.empty_slots, 0);
        assert_eq!(snapshot.live_slot_replacements, 1);
    }

    #[test]
    fn replacement_statistics_are_optional() {
        let index = anonymous(8);
        index.set_statistics_enabled(false);
        for hash in 0..8 {
            assert!(
                index
                    .upsert(hash, entry(0, hash as u32 * 32, hash + 1))
                    .unwrap()
            );
        }
        assert!(index.upsert(8, entry(1, 0, 20)).unwrap());

        assert_eq!(index.snapshot().unwrap().live_slot_replacements, 0);
    }

    #[test]
    fn unknown_region_generation_is_not_published() {
        let index = anonymous(8);

        assert!(!index.publish_region_generation(16, 1));
        assert!(!index.publish_region_generation(0, 0));
    }

    #[test]
    fn zero_watermark_preserves_recovered_values() {
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
        const SLOT_COUNT: usize = 175;
        const HASH: u64 = 166;

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        {
            let mut partition = source.write_hash_partition(HASH).unwrap();
            partition
                .replace_observed(166, IndexSlotState::Empty, IndexSlotState::Deleted)
                .unwrap();
            partition
                .replace_observed(167, IndexSlotState::Empty, IndexSlotState::Deleted)
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
        let index = RegionIndex::try_from_storage(recovered, (0..16).map(|_| 0)).unwrap();
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
        const SLOT_COUNT: usize = 176;
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
        let recovered = RegionIndex::try_from_storage(
            PartitionedIndexStorage::map_private(
                &test_file.file,
                0,
                SLOT_COUNT,
                BINDING,
                &partition_stats,
            )
            .unwrap(),
            (0..16).map(|_| 0),
        )
        .unwrap();

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
