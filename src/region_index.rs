//! Mask-aware bounded point operations over the Region mmap index.
//!
//! This layer owns the sharded storage but deliberately does not own Region
//! visibility or logical accounting. A lookup returns only the raw typed point
//! state and releases its canonical partition before the caller consults Region
//! authority. Mutations receive one authority object after the caller has
//! acquired stable Region-manager authority. No slot is modified until the
//! probe has selected its final target. The same object supplies visibility
//! during the probe and consumes the exact committed transition before the
//! partition guard is released; neither method may perform I/O or re-enter the
//! index.

use crate::index::{IndexEntry, MAX_INDEX_PROBES};
use crate::index_storage::{IndexSlotState, IndexStorageError, PartitionedIndexStorage};

/// One exact physical mutation committed under a partition write lock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexTransition {
    pub(crate) global_slot: usize,
    pub(crate) previous: IndexSlotState,
    pub(crate) installed: IndexSlotState,
}

/// Stable Region authority shared by one complete index mutation.
///
/// Callers acquire the Region-manager authority before entering the index.
/// `commit` runs synchronously after the slot and partition physical statistics
/// have changed, but before the partition write guard is released. It must be
/// infallible and must not acquire another manager or index lock.
pub(crate) trait IndexMutationAuthority {
    fn is_visible(&self, entry: IndexEntry) -> bool;

    fn commit(&mut self, transition: IndexTransition);
}

#[cfg(test)]
struct PredicateAuthority<F> {
    is_visible: F,
}

#[cfg(test)]
impl<F> IndexMutationAuthority for PredicateAuthority<F>
where
    F: Fn(IndexEntry) -> bool,
{
    fn is_visible(&self, entry: IndexEntry) -> bool {
        (self.is_visible)(entry)
    }

    fn commit(&mut self, _transition: IndexTransition) {}
}

#[cfg(test)]
struct NoopAuthority;

#[cfg(test)]
impl IndexMutationAuthority for NoopAuthority {
    fn is_visible(&self, _entry: IndexEntry) -> bool {
        true
    }

    fn commit(&mut self, _transition: IndexTransition) {}
}

/// Typed result of a point lookup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexLookup {
    Hit(IndexEntry),
    Miss,
    /// A same-hash producer owns this point until it publishes or normalizes
    /// the mask. Foreign masks are invisible and probing continues past them.
    Masked {
        seqno: u64,
    },
}

/// Result of attempting to install one live value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexUpsert {
    Applied {
        transition: IndexTransition,
        /// The logically visible entry displaced by this install. Reusing a
        /// stale physical value reports `None` while the transition still
        /// carries the exact physical predecessor.
        previous: Option<IndexEntry>,
    },
    Ignored {
        current: Option<IndexEntry>,
    },
    /// A same-hash mask is at least as new as the supplied value.
    Masked {
        seqno: u64,
    },
    /// Every candidate in the bounded window is protected by a foreign mask.
    Saturated,
}

/// Result of reserving one hash/version with a producer mask.
#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum IndexMask {
    Applied {
        transition: IndexTransition,
        previous: Option<IndexEntry>,
    },
    Ignored {
        current: Option<IndexEntry>,
    },
    /// A same-hash mask already owns an equal or newer version.
    Masked {
        seqno: u64,
    },
    Saturated,
}

#[derive(Clone, Copy)]
enum IndexInstall {
    Applied {
        transition: IndexTransition,
        previous: Option<IndexEntry>,
    },
    Ignored {
        current: Option<IndexEntry>,
    },
    Masked {
        seqno: u64,
    },
    Saturated,
}

impl From<IndexInstall> for IndexUpsert {
    fn from(outcome: IndexInstall) -> Self {
        match outcome {
            IndexInstall::Applied {
                transition,
                previous,
            } => Self::Applied {
                transition,
                previous,
            },
            IndexInstall::Ignored { current } => Self::Ignored { current },
            IndexInstall::Masked { seqno } => Self::Masked { seqno },
            IndexInstall::Saturated => Self::Saturated,
        }
    }
}

#[cfg(test)]
impl From<IndexInstall> for IndexMask {
    fn from(outcome: IndexInstall) -> Self {
        match outcome {
            IndexInstall::Applied {
                transition,
                previous,
            } => Self::Applied {
                transition,
                previous,
            },
            IndexInstall::Ignored { current } => Self::Ignored { current },
            IndexInstall::Masked { seqno } => Self::Masked { seqno },
            IndexInstall::Saturated => Self::Saturated,
        }
    }
}

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
    /// Region generation and clear-floor visibility deliberately do not run
    /// here: the partition guard is released when this method returns, before the
    /// runtime consults its Region manager.
    pub(crate) fn lookup_raw(&self, hash: u64) -> Result<IndexLookup, IndexStorageError> {
        let partition = self.storage.read_hash_partition(hash);
        let start = start_slot(hash, partition.slot_count());
        for step in 0..probe_limit(partition.slot_count()) {
            match partition.read(probe_slot(start, step, partition.slot_count()))? {
                IndexSlotState::Empty => return Ok(IndexLookup::Miss),
                IndexSlotState::Deleted => {}
                IndexSlotState::Masked {
                    hash: current_hash,
                    seqno,
                } => {
                    if current_hash == hash {
                        return Ok(IndexLookup::Masked { seqno });
                    }
                }
                IndexSlotState::Value {
                    hash: current_hash,
                    entry,
                } => {
                    if current_hash == hash {
                        return Ok(IndexLookup::Hit(entry));
                    }
                }
            }
        }
        Ok(IndexLookup::Miss)
    }

    /// Revalidates the exact immutable record identity observed by a prior
    /// lookup without consulting Region visibility.
    ///
    /// Runtime-only flags are ignored because their transitions do not change
    /// the physical record version. The caller separately revalidates its
    /// Region generation/epoch snapshot after this partition guard is released.
    pub(crate) fn revalidate_exact(
        &self,
        hash: u64,
        expected: IndexEntry,
    ) -> Result<bool, IndexStorageError> {
        Ok(matches!(
            self.lookup_raw(hash)?,
            IndexLookup::Hit(current) if current.same_record_identity(expected)
        ))
    }

    /// Installs a live value using the existing bounded replacement policy.
    ///
    /// Deleted slots and the first invisible foreign value are reusable. A
    /// foreign mask is never reusable or evictable. If the window has no
    /// reusable slot, the first visible value becomes the bounded eviction
    /// victim; an all-mask window is saturated instead of violating a mask.
    #[cfg(test)]
    fn upsert(
        &self,
        hash: u64,
        supplied: IndexEntry,
        is_visible: impl Fn(IndexEntry) -> bool,
    ) -> Result<IndexUpsert, IndexStorageError> {
        let mut authority = PredicateAuthority { is_visible };
        self.upsert_with_authority(hash, supplied, &mut authority)
    }

    /// Installs a live value under one stable Region authority.
    pub(crate) fn upsert_with_authority(
        &self,
        hash: u64,
        supplied: IndexEntry,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<IndexUpsert, IndexStorageError> {
        if supplied.seqno == 0 || !authority.is_visible(supplied) {
            return Ok(IndexUpsert::Ignored { current: None });
        }

        let installed = IndexSlotState::Value {
            hash,
            entry: supplied,
        };
        self.install_if_newer(hash, supplied.seqno, installed, authority)
            .map(IndexUpsert::from)
    }

    /// Reserves a hash/version for an in-flight producer.
    ///
    /// Equal or newer same-hash values and masks win. Foreign masks are never
    /// overwritten, including when they fill the complete bounded window.
    #[cfg(test)]
    fn mask_if_newer(
        &self,
        hash: u64,
        seqno: u64,
        is_visible: impl Fn(IndexEntry) -> bool,
    ) -> Result<IndexMask, IndexStorageError> {
        let mut authority = PredicateAuthority { is_visible };
        self.mask_if_newer_with_authority(hash, seqno, &mut authority)
    }

    /// Reserves a hash/version under one stable Region authority.
    #[cfg(test)]
    pub(crate) fn mask_if_newer_with_authority(
        &self,
        hash: u64,
        seqno: u64,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<IndexMask, IndexStorageError> {
        if seqno == 0 {
            return Ok(IndexMask::Ignored { current: None });
        }
        self.install_if_newer(
            hash,
            seqno,
            IndexSlotState::Masked { hash, seqno },
            authority,
        )
        .map(IndexMask::from)
    }

    fn install_if_newer(
        &self,
        hash: u64,
        supplied_seqno: u64,
        installed: IndexSlotState,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<IndexInstall, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash);
        let slot_count = partition.slot_count();
        let first_slot = partition.first_slot();
        let start = start_slot(hash, slot_count);
        let mut reusable = None;
        let mut victim = None;
        let mut apply = None;

        for step in 0..probe_limit(slot_count) {
            let local_slot = probe_slot(start, step, slot_count);
            match partition.read(local_slot)? {
                IndexSlotState::Empty => {
                    apply = Some(reusable.unwrap_or((local_slot, None)));
                    break;
                }
                IndexSlotState::Deleted => {
                    reusable.get_or_insert((local_slot, None));
                }
                IndexSlotState::Masked {
                    hash: current_hash,
                    seqno,
                } => {
                    if current_hash == hash {
                        if supplied_seqno <= seqno {
                            return Ok(IndexInstall::Masked { seqno });
                        }
                        apply = Some((local_slot, None));
                        break;
                    }
                }
                IndexSlotState::Value {
                    hash: current_hash,
                    entry: current,
                } => {
                    let current_visible = authority.is_visible(current);
                    if current_hash == hash {
                        if current_visible && supplied_seqno <= current.seqno {
                            return Ok(IndexInstall::Ignored {
                                current: Some(current),
                            });
                        }
                        apply = Some((local_slot, current_visible.then_some(current)));
                        break;
                    }
                    if !current_visible {
                        reusable.get_or_insert((local_slot, None));
                    } else if victim.is_none() {
                        victim = Some((local_slot, Some(current)));
                    }
                }
            }
        }

        let Some((local_slot, previous)) = apply.or(reusable).or(victim) else {
            return Ok(IndexInstall::Saturated);
        };
        let old = partition.replace(local_slot, installed)?;
        let transition = transition(first_slot, local_slot, old, installed);
        authority.commit(transition);
        drop(partition);
        Ok(IndexInstall::Applied {
            transition,
            previous,
        })
    }

    /// Replaces one exact immutable record identity with the canonical
    /// probe-deleted marker.
    #[cfg(test)]
    fn remove_if(
        &self,
        hash: u64,
        expected: IndexEntry,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        let mut authority = NoopAuthority;
        self.remove_if_with_authority(hash, expected, &mut authority)
    }

    /// Removes one exact record under one stable Region authority.
    #[cfg(test)]
    pub(crate) fn remove_if_with_authority(
        &self,
        hash: u64,
        expected: IndexEntry,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        self.replace_exact_value(hash, expected, None, authority)
    }

    /// Physically relocates one exact immutable record identity. The new
    /// location may differ, but seqno and namespace must remain unchanged;
    /// logical updates use [`Self::upsert_with_authority`]. Runtime flag
    /// changes do not make the same record a different completion target.
    /// Masked, deleted, stale, and foreign states are never modified.
    #[cfg(test)]
    fn replace_if(
        &self,
        hash: u64,
        expected: IndexEntry,
        replacement: IndexEntry,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        let mut authority = NoopAuthority;
        self.replace_if_with_authority(hash, expected, replacement, &mut authority)
    }

    /// Relocates one exact record under one stable Region authority.
    #[cfg(test)]
    pub(crate) fn replace_if_with_authority(
        &self,
        hash: u64,
        expected: IndexEntry,
        replacement: IndexEntry,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        if replacement.seqno == 0
            || replacement.seqno != expected.seqno
            || replacement.namespace_id != expected.namespace_id
            || replacement.location.record_len() != expected.location.record_len()
            || replacement.location.is_tombstone() != expected.location.is_tombstone()
            || !authority.is_visible(replacement)
        {
            return Ok(None);
        }
        self.replace_exact_value(hash, expected, Some(replacement), authority)
    }

    /// Normalizes only the exact same-hash producer mask to a deleted marker.
    #[cfg(test)]
    fn normalize_mask_if(
        &self,
        hash: u64,
        seqno: u64,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        let mut authority = NoopAuthority;
        self.normalize_mask_if_with_authority(hash, seqno, &mut authority)
    }

    /// Normalizes one exact producer mask under one stable Region authority.
    #[cfg(test)]
    pub(crate) fn normalize_mask_if_with_authority(
        &self,
        hash: u64,
        seqno: u64,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash);
        let slot_count = partition.slot_count();
        let first_slot = partition.first_slot();
        let start = start_slot(hash, slot_count);
        let mut target = None;
        for step in 0..probe_limit(slot_count) {
            let local_slot = probe_slot(start, step, slot_count);
            match partition.read(local_slot)? {
                IndexSlotState::Empty => break,
                IndexSlotState::Deleted => {}
                IndexSlotState::Masked {
                    hash: current_hash,
                    seqno: current_seqno,
                } => {
                    if current_hash == hash {
                        if current_seqno == seqno {
                            target = Some(local_slot);
                        }
                        break;
                    }
                }
                IndexSlotState::Value {
                    hash: current_hash, ..
                } => {
                    if current_hash == hash {
                        break;
                    }
                }
            }
        }
        let Some(local_slot) = target else {
            return Ok(None);
        };
        let installed = IndexSlotState::Deleted;
        let old = partition.replace(local_slot, installed)?;
        let transition = transition(first_slot, local_slot, old, installed);
        authority.commit(transition);
        drop(partition);
        Ok(Some(transition))
    }

    #[cfg(test)]
    fn replace_exact_value(
        &self,
        hash: u64,
        expected: IndexEntry,
        replacement: Option<IndexEntry>,
        authority: &mut impl IndexMutationAuthority,
    ) -> Result<Option<IndexTransition>, IndexStorageError> {
        let mut partition = self.storage.write_hash_partition(hash);
        let slot_count = partition.slot_count();
        let first_slot = partition.first_slot();
        let start = start_slot(hash, slot_count);
        let mut target = None;
        for step in 0..probe_limit(slot_count) {
            let local_slot = probe_slot(start, step, slot_count);
            match partition.read(local_slot)? {
                IndexSlotState::Empty => break,
                IndexSlotState::Deleted => {}
                IndexSlotState::Masked {
                    hash: current_hash, ..
                } => {
                    if current_hash == hash {
                        break;
                    }
                }
                IndexSlotState::Value {
                    hash: current_hash,
                    entry,
                } => {
                    if current_hash == hash {
                        if entry.same_record_identity(expected)
                            && (replacement.is_none() || authority.is_visible(entry))
                        {
                            target = Some(local_slot);
                        }
                        break;
                    }
                }
            }
        }
        let Some(local_slot) = target else {
            return Ok(None);
        };
        let installed = replacement.map_or(IndexSlotState::Deleted, |entry| {
            IndexSlotState::Value { hash, entry }
        });
        let old = partition.replace(local_slot, installed)?;
        let transition = transition(first_slot, local_slot, old, installed);
        authority.commit(transition);
        drop(partition);
        Ok(Some(transition))
    }
}

fn transition(
    first_slot: usize,
    local_slot: usize,
    previous: IndexSlotState,
    installed: IndexSlotState,
) -> IndexTransition {
    IndexTransition {
        global_slot: first_slot + local_slot,
        previous,
        installed,
    }
}

fn start_slot(hash: u64, slot_count: usize) -> usize {
    debug_assert_ne!(slot_count, 0);
    (hash % slot_count as u64) as usize
}

fn probe_limit(slot_count: usize) -> usize {
    slot_count.min(MAX_INDEX_PROBES)
}

fn probe_slot(start: usize, step: usize, slot_count: usize) -> usize {
    let index = start + step;
    if index >= slot_count {
        index - slot_count
    } else {
        index
    }
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
            location: PackedLocation::new(region_id, offset, 32, false).unwrap(),
            seqno,
            namespace_id: region_id + 100,
            flags: 0,
        }
    }

    fn anonymous(slot_count: usize) -> RegionIndex {
        RegionIndex::from_storage(PartitionedIndexStorage::anonymous(slot_count).unwrap())
    }

    #[derive(Default)]
    struct RecordingAuthority {
        min_visible_seqno: u64,
        invisible_region: Option<u32>,
        live_records: i64,
        transitions: Vec<IndexTransition>,
    }

    impl RecordingAuthority {
        fn charge(&self, state: IndexSlotState) -> i64 {
            match state {
                IndexSlotState::Value { entry, .. } if self.is_visible(entry) => 1,
                _ => 0,
            }
        }
    }

    impl IndexMutationAuthority for RecordingAuthority {
        fn is_visible(&self, entry: IndexEntry) -> bool {
            entry.seqno >= self.min_visible_seqno
                && self.invisible_region != Some(entry.location.region_id())
        }

        fn commit(&mut self, transition: IndexTransition) {
            self.live_records +=
                self.charge(transition.installed) - self.charge(transition.previous);
            self.transitions.push(transition);
        }
    }

    #[test]
    fn authority_commits_only_applied_transitions_using_stable_visibility() {
        let index = anonymous(8);
        let hash = 3;
        let first = entry(1, 8, 10);
        let mut authority = RecordingAuthority::default();

        let outcome = index
            .upsert_with_authority(hash, first, &mut authority)
            .unwrap();
        let IndexUpsert::Applied { transition, .. } = outcome else {
            panic!("upsert must commit");
        };
        assert_eq!(authority.transitions, [transition]);
        assert_eq!(authority.live_records, 1);

        assert_eq!(
            index
                .upsert_with_authority(hash, entry(2, 16, 9), &mut authority)
                .unwrap(),
            IndexUpsert::Ignored {
                current: Some(first)
            }
        );
        assert_eq!(
            index
                .remove_if_with_authority(hash, entry(2, 16, 9), &mut authority)
                .unwrap(),
            None
        );
        assert_eq!(authority.transitions.len(), 1);
        assert_eq!(authority.live_records, 1);

        // Advancing the authority floor invalidates the old physical value in
        // O(1). Replacing it must add the new visible record without charging
        // a subtraction for the now-invisible predecessor.
        authority.min_visible_seqno = 11;
        authority.live_records = 0;
        let second = entry(2, 16, 11);
        assert!(matches!(
            index
                .upsert_with_authority(hash, second, &mut authority)
                .unwrap(),
            IndexUpsert::Applied { previous: None, .. }
        ));
        assert_eq!(authority.transitions.len(), 2);
        assert_eq!(authority.live_records, 1);

        assert!(matches!(
            index
                .mask_if_newer_with_authority(hash, 12, &mut authority)
                .unwrap(),
            IndexMask::Applied { previous: Some(entry), .. } if entry == second
        ));
        assert_eq!(authority.transitions.len(), 3);
        assert_eq!(authority.live_records, 0);
    }

    #[test]
    fn relocation_requires_both_record_locations_to_remain_visible() {
        let index = anonymous(8);
        let hash = 3;
        let source = entry(1, 4096, 10);
        let target = entry(2, 8192, 10);
        let mut authority = RecordingAuthority::default();
        index
            .upsert_with_authority(hash, source, &mut authority)
            .unwrap();
        authority.transitions.clear();

        authority.invisible_region = Some(source.location.region_id());
        assert_eq!(
            index
                .replace_if_with_authority(hash, source, target, &mut authority)
                .unwrap(),
            None
        );
        assert_eq!(index.lookup_raw(hash).unwrap(), IndexLookup::Hit(source));
        assert!(authority.transitions.is_empty());

        authority.invisible_region = Some(target.location.region_id());
        assert_eq!(
            index
                .replace_if_with_authority(hash, source, target, &mut authority)
                .unwrap(),
            None
        );
        assert_eq!(index.lookup_raw(hash).unwrap(), IndexLookup::Hit(source));
        assert!(authority.transitions.is_empty());
    }

    #[test]
    fn newer_versions_win_and_conditional_changes_match_record_identity() {
        let index = anonymous(8);
        let hash = 7;
        let first = entry(1, 8, 10);
        let older = entry(2, 16, 9);
        let second = entry(3, 24, 11);
        let replacement = IndexEntry {
            location: PackedLocation::new(4, 32, 32, false).unwrap(),
            ..second
        };

        assert!(matches!(
            index.upsert(hash, first, |_| true).unwrap(),
            IndexUpsert::Applied {
                transition: IndexTransition {
                    global_slot: 7,
                    previous: IndexSlotState::Empty,
                    installed: IndexSlotState::Value { hash: 7, entry },
                },
                previous: None,
            } if entry == first
        ));
        assert_eq!(
            index.upsert(hash, older, |_| true).unwrap(),
            IndexUpsert::Ignored {
                current: Some(first)
            }
        );
        assert!(matches!(
            index.upsert(hash, second, |_| true).unwrap(),
            IndexUpsert::Applied {
                transition: IndexTransition {
                    global_slot: 7,
                    previous: IndexSlotState::Value { hash: 7, entry: old },
                    installed: IndexSlotState::Value { hash: 7, entry: new },
                },
                previous: Some(visible),
            } if old == first && new == second && visible == first
        ));
        assert_eq!(index.lookup_raw(hash).unwrap(), IndexLookup::Hit(second));

        assert_eq!(index.replace_if(hash, first, replacement).unwrap(), None);
        assert_eq!(
            index.replace_if(hash, second, entry(4, 32, 12)).unwrap(),
            None
        );
        let second_with_new_flags = IndexEntry { flags: 7, ..second };
        assert!(matches!(
            index
                .replace_if(hash, second_with_new_flags, replacement)
                .unwrap(),
            Some(IndexTransition {
                previous: IndexSlotState::Value { entry: old, .. },
                installed: IndexSlotState::Value { entry: new, .. },
                ..
            }) if old == second && new == replacement
        ));
        assert_eq!(index.remove_if(hash, second).unwrap(), None);
        let replacement_with_new_flags = IndexEntry {
            flags: 11,
            ..replacement
        };
        assert!(matches!(
            index
                .remove_if(hash, replacement_with_new_flags)
                .unwrap(),
            Some(IndexTransition {
                previous: IndexSlotState::Value { entry: old, .. },
                installed: IndexSlotState::Deleted,
                ..
            }) if old == replacement
        ));
        assert_eq!(index.lookup_raw(hash).unwrap(), IndexLookup::Miss);
    }

    #[test]
    fn raw_lookup_and_exact_revalidate_need_no_visibility_callback() {
        let index = anonymous(8);
        let hash = 5;
        let current = entry(2, 16, 9);
        assert!(matches!(
            index.upsert(hash, current, |_| true).unwrap(),
            IndexUpsert::Applied { .. }
        ));

        assert_eq!(index.lookup_raw(hash).unwrap(), IndexLookup::Hit(current));
        assert!(index.revalidate_exact(hash, current).unwrap());
        assert!(
            index
                .revalidate_exact(
                    hash,
                    IndexEntry {
                        flags: 7,
                        ..current
                    }
                )
                .unwrap()
        );
        assert!(!index.revalidate_exact(hash, entry(3, 24, 9)).unwrap());
        assert!(!index.revalidate_exact(hash, entry(2, 16, 10)).unwrap());

        assert!(matches!(
            index.mask_if_newer(hash, 10, |_| true).unwrap(),
            IndexMask::Applied { .. }
        ));
        assert_eq!(
            index.lookup_raw(hash).unwrap(),
            IndexLookup::Masked { seqno: 10 }
        );
        assert!(!index.revalidate_exact(hash, current).unwrap());
    }

    #[test]
    fn stale_foreign_value_is_reused_without_a_visible_predecessor() {
        let index = anonymous(8);
        let stale = entry(1, 8, 10);
        let supplied = entry(2, 16, 11);
        index.upsert(1, stale, |_| true).unwrap();

        assert!(matches!(
            index
                .upsert(9, supplied, |candidate| candidate.seqno >= 11)
                .unwrap(),
            IndexUpsert::Applied {
                transition: IndexTransition {
                    global_slot: 1,
                    previous: IndexSlotState::Value { hash: 1, entry: old },
                    installed: IndexSlotState::Value { hash: 9, entry: new },
                },
                previous: None,
            } if old == stale && new == supplied
        ));
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
                masked: 0,
            }
        );
    }

    #[test]
    fn a_full_foreign_mask_window_is_saturated_and_unchanged() {
        let storage = PartitionedIndexStorage::anonymous(8).unwrap();
        {
            let mut partition = storage.write_hash_partition(0);
            for slot in 0..partition.slot_count() {
                partition
                    .replace(
                        slot,
                        IndexSlotState::Masked {
                            hash: 100 + slot as u64,
                            seqno: 10 + slot as u64,
                        },
                    )
                    .unwrap();
            }
        }
        let index = RegionIndex::from_storage(storage);
        let before = index.storage().partition_stats().unwrap();
        let mut authority = RecordingAuthority::default();

        assert_eq!(
            index
                .upsert_with_authority(1, entry(1, 8, 20), &mut authority)
                .unwrap(),
            IndexUpsert::Saturated
        );
        assert_eq!(
            index
                .mask_if_newer_with_authority(1, 20, &mut authority)
                .unwrap(),
            IndexMask::Saturated
        );
        assert!(authority.transitions.is_empty());
        assert_eq!(index.storage().partition_stats().unwrap(), before);
        let partition = index.storage().read_hash_partition(0);
        for slot in 0..partition.slot_count() {
            assert_eq!(
                partition.read(slot).unwrap(),
                IndexSlotState::Masked {
                    hash: 100 + slot as u64,
                    seqno: 10 + slot as u64,
                }
            );
        }
    }

    #[test]
    fn same_hash_mask_orders_publish_and_normalization_updates_stats() {
        let storage = PartitionedIndexStorage::anonymous(8).unwrap();
        let hash = 7;
        let local_slot = 7;
        let index = RegionIndex::from_storage(storage);
        let prior = entry(1, 8, 10);
        assert!(matches!(
            index.upsert(hash, prior, |_| true).unwrap(),
            IndexUpsert::Applied { .. }
        ));

        assert!(matches!(
            index.mask_if_newer(hash, 12, |_| true).unwrap(),
            IndexMask::Applied {
                transition: IndexTransition {
                    previous: IndexSlotState::Value { hash: 7, entry },
                    installed: IndexSlotState::Masked { hash: 7, seqno: 12 },
                    ..
                },
                previous: Some(visible),
            } if entry == prior && visible == prior
        ));
        assert_eq!(
            index.lookup_raw(hash).unwrap(),
            IndexLookup::Masked { seqno: 12 }
        );
        assert_eq!(
            index.mask_if_newer(hash, 11, |_| true).unwrap(),
            IndexMask::Masked { seqno: 12 }
        );
        assert_eq!(
            index.upsert(hash, entry(1, 8, 11), |_| true).unwrap(),
            IndexUpsert::Masked { seqno: 12 }
        );
        assert!(matches!(
            index.mask_if_newer(hash, 13, |_| true).unwrap(),
            IndexMask::Applied {
                transition: IndexTransition {
                    previous: IndexSlotState::Masked { hash: 7, seqno: 12 },
                    installed: IndexSlotState::Masked { hash: 7, seqno: 13 },
                    ..
                },
                previous: None,
            }
        ));
        assert_eq!(index.normalize_mask_if(hash, 12).unwrap(), None);
        assert!(matches!(
            index.normalize_mask_if(hash, 13).unwrap(),
            Some(IndexTransition {
                previous: IndexSlotState::Masked { hash: 7, seqno: 13 },
                installed: IndexSlotState::Deleted,
                ..
            })
        ));
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 0,
                deleted: 1,
                masked: 0,
            }
        );

        {
            let mut partition = index.storage().write_hash_partition(hash);
            assert_eq!(
                partition
                    .replace(local_slot, IndexSlotState::Masked { hash, seqno: 12 })
                    .unwrap(),
                IndexSlotState::Deleted
            );
        }
        let newer = entry(2, 16, 13);
        assert!(matches!(
            index.upsert(hash, newer, |_| true).unwrap(),
            IndexUpsert::Applied {
                transition: IndexTransition {
                    previous: IndexSlotState::Masked { hash: 7, seqno: 12 },
                    installed: IndexSlotState::Value { hash: 7, entry },
                    ..
                },
                previous: None,
            } if entry == newer
        ));
        assert_eq!(
            index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
                masked: 0,
            }
        );
    }

    #[test]
    fn a_probe_crossing_a_corrupt_page_returns_the_fault_without_mutation() {
        const SLOT_COUNT: usize = 133;
        const HASH: u64 = 124;

        let source = PartitionedIndexStorage::anonymous(SLOT_COUNT).unwrap();
        {
            let mut partition = source.write_hash_partition(HASH);
            partition.replace(124, IndexSlotState::Deleted).unwrap();
            partition.replace(125, IndexSlotState::Deleted).unwrap();
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
        let mut authority = RecordingAuthority::default();

        assert!(matches!(
            index.upsert_with_authority(HASH, entry(3, 24, 20), &mut authority),
            Err(IndexStorageError::CorruptPage {
                page_index: 1,
                reason: CorruptPageReason::ChecksumMismatch { .. },
            })
        ));
        assert!(authority.transitions.is_empty());
        assert_eq!(index.storage().partition_stats().unwrap(), before);
    }

    #[test]
    fn recovered_two_partition_image_serves_hash_lookup_without_rebuild() {
        const SLOT_COUNT: usize = 134;
        const HASH_IN_SECOND_SHARD: u64 = 1;

        let source = anonymous(SLOT_COUNT);
        let value = entry(5, 40, 21);
        assert!(matches!(
            source
                .upsert(HASH_IN_SECOND_SHARD, value, |_| true)
                .unwrap(),
            IndexUpsert::Applied {
                transition: IndexTransition {
                    global_slot: 127,
                    ..
                },
                ..
            }
        ));
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
            IndexLookup::Hit(value)
        );
        assert_eq!(
            recovered.storage().partition_stats().unwrap(),
            partition_stats
        );
    }
}
