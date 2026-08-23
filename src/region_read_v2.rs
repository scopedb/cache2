//! Per-Region read projection for the RegionStore V2 data path.
//!
//! The Region manager remains the mutation authority. This directory is only
//! its fixed-size read projection: a durable lookup holds one Region read
//! guard across positioned I/O, while rotation takes that Region's write guard
//! to drain readers before reusing its bytes. No operation in this module
//! acquires or calls the Region manager.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::index::{INDEX_FLAG_VOLATILE, IndexEntry};
use crate::recovery_v2::{
    CacheEpochV2, RECORD_ALIGNMENT_V2, RECOVERY_PAGE_SIZE, REGION_HEADER_SIZE_V2,
};

/// Fields needed to authorize reads from one Region generation.
///
/// Generation identity is immutable while pinned. `completed_used` and
/// `max_seqno` may advance monotonically as device spans complete; reserved or
/// resident-only bytes never enter this projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionReadSnapshotV2 {
    pub(crate) region_id: u32,
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) incarnation: u32,
    pub(crate) created_seqno: u64,
    pub(crate) completed_used: u64,
    pub(crate) max_seqno: u64,
}

impl RegionReadSnapshotV2 {
    fn is_valid(self, region_size: u64) -> bool {
        let header_size = u64::from(REGION_HEADER_SIZE_V2);
        let empty = self.completed_used == header_size;
        self.cache_epoch != 0
            && self.incarnation != 0
            && self.incarnation != u32::MAX
            && self.created_seqno != 0
            && self.completed_used >= header_size
            && self.completed_used <= region_size
            && self.completed_used % u64::from(RECORD_ALIGNMENT_V2) == 0
            && (empty == (self.max_seqno == 0))
            && (empty || self.max_seqno >= self.created_seqno)
    }

    fn makes_visible(
        self,
        entry: IndexEntry,
        expected_epoch: CacheEpochV2,
        clear_floor_seqno: u64,
    ) -> bool {
        if self.cache_epoch != expected_epoch
            || entry.seqno == 0
            || entry.seqno < clear_floor_seqno
            || entry.seqno < self.created_seqno
            || entry.seqno > self.max_seqno
            || entry.flags & INDEX_FLAG_VOLATILE != 0
            || entry.location.is_tombstone()
            || entry.location.region_id() != self.region_id
        {
            return false;
        }
        let offset = u64::from(entry.location.offset());
        let Some(end) = offset.checked_add(u64::from(entry.location.record_len())) else {
            return false;
        };
        offset >= u64::from(REGION_HEADER_SIZE_V2)
            && offset % u64::from(RECORD_ALIGNMENT_V2) == 0
            && end <= self.completed_used
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionReadErrorV2 {
    Allocation,
    InvalidGeometry,
    InvalidRegion,
    InvalidSnapshot,
    AlreadyReadable,
    NotReadable,
    StaleGeneration,
    CompletionRegressed,
    Poisoned,
}

impl fmt::Display for RegionReadErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Allocation => "Region read directory cannot be allocated",
            Self::InvalidGeometry => "Region read directory geometry is invalid",
            Self::InvalidRegion => "Region read id is out of bounds",
            Self::InvalidSnapshot => "Region read snapshot is invalid",
            Self::AlreadyReadable => "Region already has a readable generation",
            Self::NotReadable => "Region generation is not readable",
            Self::StaleGeneration => "Region read generation is stale",
            Self::CompletionRegressed => "Region completed prefix moved backwards",
            Self::Poisoned => "Region read projection lock is poisoned",
        })
    }
}

impl std::error::Error for RegionReadErrorV2 {}

#[derive(Clone, Copy, Debug)]
struct RegionReadGenerationV2 {
    region_id: u32,
    cache_epoch: CacheEpochV2,
    incarnation: u32,
    created_seqno: u64,
    readable: bool,
}

impl RegionReadGenerationV2 {
    const fn empty(region_id: u32) -> Self {
        Self {
            region_id,
            cache_epoch: 0,
            incarnation: 0,
            created_seqno: 0,
            readable: false,
        }
    }
}

struct RegionReadCellV2 {
    generation: RwLock<RegionReadGenerationV2>,
    completed_used: AtomicU64,
    max_seqno: AtomicU64,
    /// Set before rotation waits for the generation write lock. This prevents
    /// a hot read stream from continuously barging ahead of the writer.
    draining: AtomicBool,
}

impl RegionReadCellV2 {
    fn empty(region_id: u32) -> Self {
        Self {
            generation: RwLock::new(RegionReadGenerationV2::empty(region_id)),
            completed_used: AtomicU64::new(REGION_HEADER_SIZE_V2 as u64),
            max_seqno: AtomicU64::new(0),
            draining: AtomicBool::new(false),
        }
    }

    fn snapshot(&self, generation: &RegionReadGenerationV2) -> RegionReadSnapshotV2 {
        // completed_used is the publication edge: update_active stores max_seqno
        // first and completed_used second. Seeing a newer completed prefix thus
        // also sees the matching maximum sequence number.
        let completed_used = self.completed_used.load(Ordering::Acquire);
        let max_seqno = self.max_seqno.load(Ordering::Acquire);
        RegionReadSnapshotV2 {
            region_id: generation.region_id,
            cache_epoch: generation.cache_epoch,
            incarnation: generation.incarnation,
            created_seqno: generation.created_seqno,
            completed_used,
            max_seqno,
        }
    }

    fn install_completion(&self, snapshot: RegionReadSnapshotV2) {
        self.max_seqno.store(snapshot.max_seqno, Ordering::Relaxed);
        self.completed_used
            .store(snapshot.completed_used, Ordering::Release);
    }
}

/// Fixed-size Region read projection.
///
/// Reads of one Region share only that Region's generation lock. Active-prefix
/// completion advances through atomics without draining those readers. A
/// rotation write guard blocks new readers and waits for existing readers of
/// the victim, without affecting any other Region.
pub(crate) struct RegionReadDirectoryV2 {
    region_size: u64,
    regions: Box<[RegionReadCellV2]>,
}

impl RegionReadDirectoryV2 {
    pub(crate) fn try_new(region_count: u32, region_size: u64) -> Result<Self, RegionReadErrorV2> {
        if region_count == 0
            || region_size < u64::from(REGION_HEADER_SIZE_V2 + RECORD_ALIGNMENT_V2)
            || region_size % RECOVERY_PAGE_SIZE as u64 != 0
        {
            return Err(RegionReadErrorV2::InvalidGeometry);
        }
        let count =
            usize::try_from(region_count).map_err(|_| RegionReadErrorV2::InvalidGeometry)?;
        let mut regions = Vec::new();
        regions
            .try_reserve_exact(count)
            .map_err(|_| RegionReadErrorV2::Allocation)?;
        for region_id in 0..region_count {
            regions.push(RegionReadCellV2::empty(region_id));
        }
        Ok(Self {
            region_size,
            regions: regions.into_boxed_slice(),
        })
    }

    pub(crate) fn region_count(&self) -> usize {
        self.regions.len()
    }

    pub(crate) const fn region_size(&self) -> u64 {
        self.region_size
    }

    /// Installs an initial or newly rotated readable generation.
    ///
    /// Reinstalling the exact prior snapshot is allowed as an idempotent
    /// rollback. A different generation may replace only an unreadable cell
    /// and must advance its epoch or incarnation.
    pub(crate) fn install(&self, snapshot: RegionReadSnapshotV2) -> Result<(), RegionReadErrorV2> {
        self.validate_install_target(snapshot)?;
        let cell = self.cell(snapshot.region_id)?;
        let mut generation = write_generation(cell)?;
        install_snapshot(cell, &mut generation, snapshot, self.region_size)
    }

    /// Advances the completed prefix of one existing Active generation.
    /// Updates are monotonic and occur once per completed write span, not once
    /// per record.
    pub(crate) fn update_active(
        &self,
        snapshot: RegionReadSnapshotV2,
    ) -> Result<(), RegionReadErrorV2> {
        self.validate_install_target(snapshot)?;
        let cell = self.cell(snapshot.region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Err(RegionReadErrorV2::StaleGeneration);
        }
        let generation = read_generation(cell)?;
        if cell.draining.load(Ordering::Acquire) {
            return Err(RegionReadErrorV2::StaleGeneration);
        }
        if !generation.readable {
            return Err(RegionReadErrorV2::NotReadable);
        }
        if generation.cache_epoch != snapshot.cache_epoch
            || generation.incarnation != snapshot.incarnation
            || generation.created_seqno != snapshot.created_seqno
        {
            return Err(RegionReadErrorV2::StaleGeneration);
        }
        let current_completed = cell.completed_used.load(Ordering::Acquire);
        let current_max_seqno = cell.max_seqno.load(Ordering::Acquire);
        if snapshot.completed_used < current_completed || snapshot.max_seqno < current_max_seqno {
            return Err(RegionReadErrorV2::CompletionRegressed);
        }
        // A lane has one ordered completion worker. Atomics keep this update
        // compatible with all in-flight Region read guards without draining
        // them; manager receipts remain the sole writer-serialization source.
        cell.install_completion(snapshot);
        Ok(())
    }

    /// Acquires a read pin only when the indexed record belongs to the
    /// completed prefix of the current readable generation.
    pub(crate) fn acquire_visible(
        &self,
        entry: IndexEntry,
        expected_epoch: CacheEpochV2,
        clear_floor_seqno: u64,
    ) -> Result<Option<RegionReadGuardV2<'_>>, RegionReadErrorV2> {
        let region_id = entry.location.region_id();
        let cell = self.cell(region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Ok(None);
        }
        let generation = read_generation(cell)?;
        if cell.draining.load(Ordering::Acquire) || !generation.readable {
            return Ok(None);
        }
        let snapshot = cell.snapshot(&generation);
        if !snapshot.makes_visible(entry, expected_epoch, clear_floor_seqno)
            || cell.draining.load(Ordering::Acquire)
        {
            return Ok(None);
        }
        Ok(Some(RegionReadGuardV2 { cell, generation }))
    }

    /// Drains readers of one exact victim generation and prevents new ones
    /// until the returned guard is dropped. The caller may wait here, so it
    /// must not hold manager authority.
    pub(crate) fn acquire_rotation_write(
        &self,
        region_id: u32,
        expected_epoch: CacheEpochV2,
        expected_incarnation: u32,
    ) -> Result<RegionRotationWriteGuardV2<'_>, RegionReadErrorV2> {
        let cell = self.cell(region_id)?;
        if cell
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RegionReadErrorV2::StaleGeneration);
        }
        let generation = match write_generation(cell) {
            Ok(generation) => generation,
            Err(error) => {
                cell.draining.store(false, Ordering::Release);
                return Err(error);
            }
        };
        if !generation.readable {
            cell.draining.store(false, Ordering::Release);
            return Err(RegionReadErrorV2::NotReadable);
        }
        if generation.cache_epoch != expected_epoch
            || generation.incarnation != expected_incarnation
        {
            cell.draining.store(false, Ordering::Release);
            return Err(RegionReadErrorV2::StaleGeneration);
        }
        Ok(RegionRotationWriteGuardV2 {
            region_size: self.region_size,
            cell,
            generation,
        })
    }

    /// Convenience boundary for making a generation unreadable without
    /// retaining its rotation guard. Normal reuse should call
    /// [`Self::acquire_rotation_write`] and keep that guard through overwrite.
    pub(crate) fn mark_unreadable(
        &self,
        region_id: u32,
        expected_epoch: CacheEpochV2,
        expected_incarnation: u32,
    ) -> Result<RegionReadSnapshotV2, RegionReadErrorV2> {
        let mut guard =
            self.acquire_rotation_write(region_id, expected_epoch, expected_incarnation)?;
        Ok(guard.mark_unreadable())
    }

    pub(crate) fn snapshot(
        &self,
        region_id: u32,
    ) -> Result<Option<RegionReadSnapshotV2>, RegionReadErrorV2> {
        let cell = self.cell(region_id)?;
        let generation = read_generation(cell)?;
        Ok(generation.readable.then(|| cell.snapshot(&generation)))
    }

    /// Cross-checks a read projection against an exact manager/freeze
    /// snapshot. This method acquires the Region read lock and must not be
    /// called while the same thread retains its rotation write guard.
    pub(crate) fn validate_snapshot(
        &self,
        expected: RegionReadSnapshotV2,
    ) -> Result<bool, RegionReadErrorV2> {
        let cell = self.cell(expected.region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Ok(false);
        }
        let generation = read_generation(cell)?;
        Ok(!cell.draining.load(Ordering::Acquire)
            && generation.readable
            && cell.snapshot(&generation) == expected)
    }

    fn validate_install_target(
        &self,
        snapshot: RegionReadSnapshotV2,
    ) -> Result<(), RegionReadErrorV2> {
        if snapshot.region_id as usize >= self.regions.len() {
            return Err(RegionReadErrorV2::InvalidRegion);
        }
        if !snapshot.is_valid(self.region_size) {
            return Err(RegionReadErrorV2::InvalidSnapshot);
        }
        Ok(())
    }

    fn cell(&self, region_id: u32) -> Result<&RegionReadCellV2, RegionReadErrorV2> {
        self.regions
            .get(region_id as usize)
            .ok_or(RegionReadErrorV2::InvalidRegion)
    }
}

fn install_snapshot(
    cell: &RegionReadCellV2,
    generation: &mut RegionReadGenerationV2,
    snapshot: RegionReadSnapshotV2,
    region_size: u64,
) -> Result<(), RegionReadErrorV2> {
    if !snapshot.is_valid(region_size) || snapshot.region_id != generation.region_id {
        return Err(RegionReadErrorV2::InvalidSnapshot);
    }
    let current = cell.snapshot(generation);
    if generation.readable {
        return if current == snapshot {
            Ok(())
        } else {
            Err(RegionReadErrorV2::AlreadyReadable)
        };
    }

    let prior = current;
    if prior.incarnation != 0
        && prior != snapshot
        && (snapshot.cache_epoch < prior.cache_epoch
            || (snapshot.cache_epoch == prior.cache_epoch
                && snapshot.incarnation <= prior.incarnation))
    {
        return Err(RegionReadErrorV2::StaleGeneration);
    }
    cell.install_completion(snapshot);
    generation.cache_epoch = snapshot.cache_epoch;
    generation.incarnation = snapshot.incarnation;
    generation.created_seqno = snapshot.created_seqno;
    generation.readable = true;
    Ok(())
}

fn read_generation(
    cell: &RegionReadCellV2,
) -> Result<RwLockReadGuard<'_, RegionReadGenerationV2>, RegionReadErrorV2> {
    cell.generation
        .read()
        .map_err(|_| RegionReadErrorV2::Poisoned)
}

fn write_generation(
    cell: &RegionReadCellV2,
) -> Result<RwLockWriteGuard<'_, RegionReadGenerationV2>, RegionReadErrorV2> {
    cell.generation
        .write()
        .map_err(|_| RegionReadErrorV2::Poisoned)
}

/// Read pin retained across record I/O and exact index revalidation.
pub(crate) struct RegionReadGuardV2<'a> {
    cell: &'a RegionReadCellV2,
    generation: RwLockReadGuard<'a, RegionReadGenerationV2>,
}

impl RegionReadGuardV2<'_> {
    pub(crate) fn snapshot(&self) -> RegionReadSnapshotV2 {
        self.cell.snapshot(&self.generation)
    }

    /// Validates the generation fields decoded from the record header while
    /// this guard still prevents rotation from changing the projection.
    pub(crate) fn validate_snapshot(
        &self,
        record_epoch: CacheEpochV2,
        record_incarnation: u32,
    ) -> bool {
        self.generation.readable
            && self.generation.cache_epoch == record_epoch
            && self.generation.incarnation == record_incarnation
    }
}

/// Exclusive victim pin retained while rotation overwrites Region bytes.
pub(crate) struct RegionRotationWriteGuardV2<'a> {
    region_size: u64,
    cell: &'a RegionReadCellV2,
    generation: RwLockWriteGuard<'a, RegionReadGenerationV2>,
}

impl RegionRotationWriteGuardV2<'_> {
    pub(crate) fn snapshot(&self) -> RegionReadSnapshotV2 {
        self.cell.snapshot(&self.generation)
    }

    pub(crate) fn mark_unreadable(&mut self) -> RegionReadSnapshotV2 {
        let snapshot = self.cell.snapshot(&self.generation);
        self.generation.readable = false;
        snapshot
    }

    /// Publishes the activated generation while retaining the write guard.
    /// Callers normally finish manager rotation before dropping this guard.
    pub(crate) fn install(
        &mut self,
        snapshot: RegionReadSnapshotV2,
    ) -> Result<(), RegionReadErrorV2> {
        install_snapshot(self.cell, &mut self.generation, snapshot, self.region_size)
    }
}

impl Drop for RegionRotationWriteGuardV2<'_> {
    fn drop(&mut self) {
        self.cell.draining.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::mpsc::{self, RecvTimeoutError};
    use std::thread;
    use std::time::{Duration, Instant};

    use crate::index::PackedLocation;

    use super::*;

    const REGION_SIZE: u64 = 64 * 1024;

    fn directory(region_count: u32) -> RegionReadDirectoryV2 {
        RegionReadDirectoryV2::try_new(region_count, REGION_SIZE).unwrap()
    }

    fn snapshot(
        region_id: u32,
        incarnation: u32,
        created_seqno: u64,
        completed_used: u64,
        max_seqno: u64,
    ) -> RegionReadSnapshotV2 {
        RegionReadSnapshotV2 {
            region_id,
            cache_epoch: 3,
            incarnation,
            created_seqno,
            completed_used,
            max_seqno,
        }
    }

    fn entry(region_id: u32, seqno: u64) -> IndexEntry {
        IndexEntry {
            location: PackedLocation::new(region_id, REGION_HEADER_SIZE_V2, 64, false).unwrap(),
            seqno,
            namespace_id: 0,
            flags: 0,
        }
    }

    #[test]
    fn readers_of_one_region_share_the_generation_pin() {
        let directory = Arc::new(directory(1));
        directory.install(snapshot(0, 1, 1, 4160, 7)).unwrap();
        let first = directory
            .acquire_visible(entry(0, 7), 3, 1)
            .unwrap()
            .unwrap();

        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let other = Arc::clone(&directory);
        let worker = thread::spawn(move || {
            let second = other.acquire_visible(entry(0, 7), 3, 1).unwrap().unwrap();
            acquired_tx.send(second.snapshot()).unwrap();
            release_rx.recv().unwrap();
            drop(second);
        });

        assert_eq!(
            acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            first.snapshot()
        );
        release_tx.send(()).unwrap();
        drop(first);
        worker.join().unwrap();
    }

    #[test]
    fn active_completion_update_does_not_wait_for_an_inflight_read() {
        let directory = Arc::new(directory(1));
        directory.install(snapshot(0, 1, 1, 4160, 7)).unwrap();
        let reader = directory
            .acquire_visible(entry(0, 7), 3, 1)
            .unwrap()
            .unwrap();

        let (updated_tx, updated_rx) = mpsc::channel();
        let other = Arc::clone(&directory);
        let updated = snapshot(0, 1, 1, 4224, 8);
        let worker = thread::spawn(move || updated_tx.send(other.update_active(updated)).unwrap());

        assert_eq!(
            updated_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            Ok(())
        );
        assert_eq!(reader.snapshot(), updated);
        drop(reader);
        worker.join().unwrap();
    }

    #[test]
    fn draining_rejects_new_reads_while_rotation_waits_for_the_old_reader() {
        let directory = Arc::new(directory(1));
        directory.install(snapshot(0, 1, 1, 4160, 7)).unwrap();
        let reader = directory
            .acquire_visible(entry(0, 7), 3, 1)
            .unwrap()
            .unwrap();

        let (started_tx, started_rx) = mpsc::channel();
        let (acquired_tx, acquired_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let other = Arc::clone(&directory);
        let worker = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let guard = other.acquire_rotation_write(0, 3, 1).unwrap();
            acquired_tx.send(guard.snapshot()).unwrap();
            release_rx.recv().unwrap();
            drop(guard);
        });

        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(1);
        while !directory.regions[0].draining.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "rotation did not enter draining");
            thread::yield_now();
        }
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            acquired_rx.recv_timeout(Duration::from_millis(50)),
            Err(RecvTimeoutError::Timeout)
        );
        drop(reader);
        assert_eq!(
            acquired_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            snapshot(0, 1, 1, 4160, 7)
        );
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_none()
        );
        release_tx.send(()).unwrap();
        worker.join().unwrap();
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn rotating_one_region_does_not_block_another_region() {
        let directory = directory(2);
        directory.install(snapshot(0, 1, 1, 4160, 7)).unwrap();
        directory.install(snapshot(1, 4, 8, 4160, 9)).unwrap();

        let _rotation = directory.acquire_rotation_write(0, 3, 1).unwrap();
        let other = directory.acquire_visible(entry(1, 9), 3, 1).unwrap();
        assert!(other.is_some());
    }

    #[test]
    fn activated_generation_rejects_the_old_incarnation_and_entry() {
        let directory = directory(1);
        let old = snapshot(0, 1, 1, 4160, 7);
        directory.install(old).unwrap();

        let mut rotation = directory.acquire_rotation_write(0, 3, 1).unwrap();
        assert_eq!(rotation.mark_unreadable(), old);
        let activated = snapshot(0, 2, 8, 4160, 9);
        rotation.install(activated).unwrap();
        drop(rotation);

        assert!(!directory.validate_snapshot(old).unwrap());
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_none()
        );
        assert!(matches!(
            directory.acquire_rotation_write(0, 3, 1),
            Err(RegionReadErrorV2::StaleGeneration)
        ));
        let current = directory
            .acquire_visible(entry(0, 9), 3, 1)
            .unwrap()
            .unwrap();
        assert!(!current.validate_snapshot(3, 1));
        assert!(current.validate_snapshot(3, 2));
    }

    #[test]
    fn clear_floor_rejects_an_older_visible_generation_entry() {
        let directory = directory(1);
        directory.install(snapshot(0, 1, 1, 4160, 7)).unwrap();

        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 8)
                .unwrap()
                .is_none()
        );
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 7)
                .unwrap()
                .is_some()
        );
    }
}
