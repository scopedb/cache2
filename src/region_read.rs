//! Per-Region read projection for the RegionStore data path.
//!
//! The Region manager remains the mutation authority. This directory is only
//! its fixed-size read projection: a durable lookup holds one atomic Region pin
//! across positioned I/O, while the background rotation worker drains those
//! pins before reusing the bytes. No generation lock crosses device I/O, and no
//! operation in this module acquires or calls the Region manager.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::Duration;

use crate::index::IndexEntry;
use crate::recovery::{CacheEpoch, RECORD_ALIGNMENT, RECOVERY_PAGE_SIZE};

const READER_DRAIN_POLL: Duration = Duration::from_micros(50);

/// Fields needed to authorize reads from one Region generation.
///
/// Generation identity is immutable while pinned. `completed_used` and
/// `max_seqno` may advance monotonically as device spans complete; reserved or
/// resident-only bytes never enter this projection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionReadSnapshot {
    pub(crate) region_id: u32,
    pub(crate) cache_epoch: CacheEpoch,
    pub(crate) incarnation: u32,
    pub(crate) created_seqno: u64,
    pub(crate) completed_used: u64,
    pub(crate) max_seqno: u64,
}

impl RegionReadSnapshot {
    fn is_valid(self, region_size: u64) -> bool {
        let empty = self.completed_used == 0;
        self.cache_epoch != 0
            && self.incarnation != 0
            && self.incarnation != u32::MAX
            && self.created_seqno != 0
            && self.completed_used <= region_size
            && self.completed_used % u64::from(RECORD_ALIGNMENT) == 0
            && (empty == (self.max_seqno == 0))
            && (empty || self.max_seqno >= self.created_seqno)
    }

    fn makes_visible(
        self,
        entry: IndexEntry,
        expected_epoch: CacheEpoch,
        clear_floor_seqno: u64,
    ) -> bool {
        if self.cache_epoch != expected_epoch
            || entry.seqno == 0
            || entry.seqno < clear_floor_seqno
            || entry.seqno < self.created_seqno
            || entry.seqno > self.max_seqno
            || entry.location.region_id() != self.region_id
        {
            return false;
        }
        let offset = u64::from(entry.location.offset());
        let Some(end) = offset.checked_add(u64::from(entry.location.record_len())) else {
            return false;
        };
        offset % u64::from(RECORD_ALIGNMENT) == 0 && end <= self.completed_used
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionReadError {
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

impl fmt::Display for RegionReadError {
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

impl std::error::Error for RegionReadError {}

#[derive(Clone, Copy, Debug)]
struct RegionReadGeneration {
    region_id: u32,
    cache_epoch: CacheEpoch,
    incarnation: u32,
    created_seqno: u64,
    readable: bool,
}

impl RegionReadGeneration {
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

struct RegionReadCell {
    generation: RwLock<RegionReadGeneration>,
    completed_used: AtomicU64,
    max_seqno: AtomicU64,
    readers: AtomicUsize,
    /// Set before rotation waits for existing atomic reader pins. This prevents
    /// a hot read stream from continuously barging ahead of the victim worker.
    draining: AtomicBool,
}

impl RegionReadCell {
    fn empty(region_id: u32) -> Self {
        Self {
            generation: RwLock::new(RegionReadGeneration::empty(region_id)),
            completed_used: AtomicU64::new(0),
            max_seqno: AtomicU64::new(0),
            readers: AtomicUsize::new(0),
            draining: AtomicBool::new(false),
        }
    }

    fn snapshot(&self, generation: &RegionReadGeneration) -> RegionReadSnapshot {
        // completed_used is the publication edge: update_active stores max_seqno
        // first and completed_used second. Seeing a newer completed prefix thus
        // also sees the matching maximum sequence number.
        let completed_used = self.completed_used.load(Ordering::Acquire);
        let max_seqno = self.max_seqno.load(Ordering::Acquire);
        RegionReadSnapshot {
            region_id: generation.region_id,
            cache_epoch: generation.cache_epoch,
            incarnation: generation.incarnation,
            created_seqno: generation.created_seqno,
            completed_used,
            max_seqno,
        }
    }

    fn install_completion(&self, snapshot: RegionReadSnapshot) {
        self.max_seqno.store(snapshot.max_seqno, Ordering::Relaxed);
        self.completed_used
            .store(snapshot.completed_used, Ordering::Release);
    }

    fn release_reader(&self) {
        let previous = self.readers.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous != 0);
    }
}

/// Fixed-size Region read projection.
///
/// Reads briefly snapshot one Region's generation and then retain only an
/// atomic pin across device I/O. Active-prefix completion advances through
/// atomics. Rotation rejects new pins and waits in its background worker for
/// existing pins to drain, without holding a lock across device I/O.
pub(crate) struct RegionReadDirectory {
    region_size: u64,
    regions: Box<[RegionReadCell]>,
}

impl RegionReadDirectory {
    pub(crate) fn try_new(region_count: u32, region_size: u64) -> Result<Self, RegionReadError> {
        if region_count == 0
            || region_size < RECOVERY_PAGE_SIZE as u64
            || region_size % RECOVERY_PAGE_SIZE as u64 != 0
        {
            return Err(RegionReadError::InvalidGeometry);
        }
        let count = usize::try_from(region_count).map_err(|_| RegionReadError::InvalidGeometry)?;
        let mut regions = Vec::new();
        regions
            .try_reserve_exact(count)
            .map_err(|_| RegionReadError::Allocation)?;
        for region_id in 0..region_count {
            regions.push(RegionReadCell::empty(region_id));
        }
        Ok(Self {
            region_size,
            regions: regions.into_boxed_slice(),
        })
    }

    /// Installs an initial or newly rotated readable generation.
    ///
    /// Reinstalling the exact prior snapshot is allowed as an idempotent
    /// rollback. A different generation may replace only an unreadable cell
    /// and must advance its epoch or incarnation.
    pub(crate) fn install(&self, snapshot: RegionReadSnapshot) -> Result<(), RegionReadError> {
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
        snapshot: RegionReadSnapshot,
    ) -> Result<(), RegionReadError> {
        self.validate_install_target(snapshot)?;
        let cell = self.cell(snapshot.region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Err(RegionReadError::StaleGeneration);
        }
        let generation = read_generation(cell)?;
        if cell.draining.load(Ordering::Acquire) {
            return Err(RegionReadError::StaleGeneration);
        }
        if !generation.readable {
            return Err(RegionReadError::NotReadable);
        }
        if generation.cache_epoch != snapshot.cache_epoch
            || generation.incarnation != snapshot.incarnation
            || generation.created_seqno != snapshot.created_seqno
        {
            return Err(RegionReadError::StaleGeneration);
        }
        let current_completed = cell.completed_used.load(Ordering::Acquire);
        let current_max_seqno = cell.max_seqno.load(Ordering::Acquire);
        if snapshot.completed_used < current_completed || snapshot.max_seqno < current_max_seqno {
            return Err(RegionReadError::CompletionRegressed);
        }
        // A shard has one ordered completion worker. Atomics keep this update
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
        expected_epoch: CacheEpoch,
        clear_floor_seqno: u64,
    ) -> Result<Option<RegionReadGuard<'_>>, RegionReadError> {
        let region_id = entry.location.region_id();
        let cell = self.cell(region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Ok(None);
        }
        cell.readers.fetch_add(1, Ordering::AcqRel);
        if cell.draining.load(Ordering::Acquire) {
            cell.release_reader();
            return Ok(None);
        }
        let generation = match cell.generation.try_read() {
            Ok(generation) => *generation,
            Err(TryLockError::WouldBlock) => {
                cell.release_reader();
                return Ok(None);
            }
            Err(TryLockError::Poisoned(_)) => {
                cell.release_reader();
                return Err(RegionReadError::Poisoned);
            }
        };
        if cell.draining.load(Ordering::Acquire) || !generation.readable {
            cell.release_reader();
            return Ok(None);
        }
        let snapshot = cell.snapshot(&generation);
        if !snapshot.makes_visible(entry, expected_epoch, clear_floor_seqno)
            || cell.draining.load(Ordering::Acquire)
        {
            cell.release_reader();
            return Ok(None);
        }
        Ok(Some(RegionReadGuard { cell, generation }))
    }

    /// Drains atomic readers of one exact victim generation and prevents new
    /// ones until the returned guard is dropped. Only the background rotation
    /// worker may wait here, and no generation lock is retained across I/O.
    pub(crate) fn acquire_rotation_write(
        &self,
        region_id: u32,
        expected_epoch: CacheEpoch,
        expected_incarnation: u32,
    ) -> Result<RegionRotationWriteGuard<'_>, RegionReadError> {
        let cell = self.cell(region_id)?;
        if cell
            .draining
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RegionReadError::StaleGeneration);
        }
        // This is a background-only rotation path. Polling keeps read-pin
        // release completely lock-free, so a completed cache read never waits
        // behind victim selection or generation reuse.
        while cell.readers.load(Ordering::Acquire) != 0 {
            std::thread::sleep(READER_DRAIN_POLL);
        }
        let generation = match read_generation(cell) {
            Ok(generation) => *generation,
            Err(error) => {
                cell.draining.store(false, Ordering::Release);
                return Err(error);
            }
        };
        if !generation.readable {
            cell.draining.store(false, Ordering::Release);
            return Err(RegionReadError::NotReadable);
        }
        if generation.cache_epoch != expected_epoch
            || generation.incarnation != expected_incarnation
        {
            cell.draining.store(false, Ordering::Release);
            return Err(RegionReadError::StaleGeneration);
        }
        Ok(RegionRotationWriteGuard {
            region_size: self.region_size,
            cell,
            generation,
        })
    }

    pub(crate) fn snapshot(
        &self,
        region_id: u32,
    ) -> Result<Option<RegionReadSnapshot>, RegionReadError> {
        let cell = self.cell(region_id)?;
        let generation = read_generation(cell)?;
        Ok(generation.readable.then(|| cell.snapshot(&generation)))
    }

    /// Cross-checks a read projection against an exact manager/freeze
    /// snapshot. This method acquires the Region read lock and must not be
    /// called while the same thread retains its rotation write guard.
    pub(crate) fn validate_snapshot(
        &self,
        expected: RegionReadSnapshot,
    ) -> Result<bool, RegionReadError> {
        let cell = self.cell(expected.region_id)?;
        if cell.draining.load(Ordering::Acquire) {
            return Ok(false);
        }
        let generation = read_generation(cell)?;
        Ok(!cell.draining.load(Ordering::Acquire)
            && generation.readable
            && cell.snapshot(&generation) == expected)
    }

    fn validate_install_target(&self, snapshot: RegionReadSnapshot) -> Result<(), RegionReadError> {
        if snapshot.region_id as usize >= self.regions.len() {
            return Err(RegionReadError::InvalidRegion);
        }
        if !snapshot.is_valid(self.region_size) {
            return Err(RegionReadError::InvalidSnapshot);
        }
        Ok(())
    }

    fn cell(&self, region_id: u32) -> Result<&RegionReadCell, RegionReadError> {
        self.regions
            .get(region_id as usize)
            .ok_or(RegionReadError::InvalidRegion)
    }
}

fn install_snapshot(
    cell: &RegionReadCell,
    generation: &mut RegionReadGeneration,
    snapshot: RegionReadSnapshot,
    region_size: u64,
) -> Result<(), RegionReadError> {
    if !snapshot.is_valid(region_size) || snapshot.region_id != generation.region_id {
        return Err(RegionReadError::InvalidSnapshot);
    }
    let current = cell.snapshot(generation);
    if generation.readable {
        return if current == snapshot {
            Ok(())
        } else {
            Err(RegionReadError::AlreadyReadable)
        };
    }

    let prior = current;
    if prior.incarnation != 0
        && prior != snapshot
        && (snapshot.cache_epoch < prior.cache_epoch
            || (snapshot.cache_epoch == prior.cache_epoch
                && snapshot.incarnation <= prior.incarnation))
    {
        return Err(RegionReadError::StaleGeneration);
    }
    cell.install_completion(snapshot);
    generation.cache_epoch = snapshot.cache_epoch;
    generation.incarnation = snapshot.incarnation;
    generation.created_seqno = snapshot.created_seqno;
    generation.readable = true;
    Ok(())
}

fn read_generation(
    cell: &RegionReadCell,
) -> Result<RwLockReadGuard<'_, RegionReadGeneration>, RegionReadError> {
    cell.generation
        .read()
        .map_err(|_| RegionReadError::Poisoned)
}

fn write_generation(
    cell: &RegionReadCell,
) -> Result<RwLockWriteGuard<'_, RegionReadGeneration>, RegionReadError> {
    cell.generation
        .write()
        .map_err(|_| RegionReadError::Poisoned)
}

/// Read pin retained across record I/O and exact index revalidation.
pub(crate) struct RegionReadGuard<'a> {
    cell: &'a RegionReadCell,
    generation: RegionReadGeneration,
}

impl RegionReadGuard<'_> {
    pub(crate) fn snapshot(&self) -> RegionReadSnapshot {
        self.cell.snapshot(&self.generation)
    }

    /// Validates the generation fields decoded from the record header while
    /// this guard still prevents rotation from changing the projection.
    pub(crate) fn validate_snapshot(
        &self,
        record_epoch: CacheEpoch,
        record_incarnation: u32,
    ) -> bool {
        self.generation.readable
            && self.generation.cache_epoch == record_epoch
            && self.generation.incarnation == record_incarnation
    }
}

impl Drop for RegionReadGuard<'_> {
    fn drop(&mut self) {
        self.cell.release_reader();
    }
}

/// Exclusive victim pin retained while rotation overwrites Region bytes.
pub(crate) struct RegionRotationWriteGuard<'a> {
    region_size: u64,
    cell: &'a RegionReadCell,
    generation: RegionReadGeneration,
}

impl RegionRotationWriteGuard<'_> {
    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> RegionReadSnapshot {
        self.cell.snapshot(&self.generation)
    }

    pub(crate) fn mark_unreadable(&mut self) -> Result<RegionReadSnapshot, RegionReadError> {
        let mut generation = write_generation(self.cell)?;
        if !generation.readable
            || generation.cache_epoch != self.generation.cache_epoch
            || generation.incarnation != self.generation.incarnation
            || generation.created_seqno != self.generation.created_seqno
        {
            return Err(RegionReadError::StaleGeneration);
        }
        let snapshot = self.cell.snapshot(&generation);
        generation.readable = false;
        self.generation = *generation;
        Ok(snapshot)
    }

    /// Publishes the activated generation under a short generation write lock.
    pub(crate) fn install(&mut self, snapshot: RegionReadSnapshot) -> Result<(), RegionReadError> {
        let mut generation = write_generation(self.cell)?;
        install_snapshot(self.cell, &mut generation, snapshot, self.region_size)?;
        self.generation = *generation;
        Ok(())
    }
}

impl Drop for RegionRotationWriteGuard<'_> {
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

    fn directory(region_count: u32) -> RegionReadDirectory {
        RegionReadDirectory::try_new(region_count, REGION_SIZE).unwrap()
    }

    fn snapshot(
        region_id: u32,
        incarnation: u32,
        created_seqno: u64,
        completed_used: u64,
        max_seqno: u64,
    ) -> RegionReadSnapshot {
        RegionReadSnapshot {
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
            location: PackedLocation::new(region_id, 0, 64).unwrap(),
            seqno,
            namespace_id: 0,
        }
    }

    #[test]
    fn readers_of_one_region_share_the_generation_pin() {
        let directory = Arc::new(directory(1));
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();
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
    fn generation_lock_contention_is_an_immediate_miss() {
        let directory = directory(1);
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();
        let cell = &directory.regions[0];
        let generation = cell
            .generation
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_none()
        );
        assert_eq!(cell.readers.load(Ordering::Acquire), 0);

        drop(generation);
        assert!(
            directory
                .acquire_visible(entry(0, 7), 3, 1)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn active_completion_update_does_not_wait_for_an_inflight_read() {
        let directory = Arc::new(directory(1));
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();
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
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();
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
            snapshot(0, 1, 1, 64, 7)
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
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();
        directory.install(snapshot(1, 4, 8, 64, 9)).unwrap();

        let _rotation = directory.acquire_rotation_write(0, 3, 1).unwrap();
        let other = directory.acquire_visible(entry(1, 9), 3, 1).unwrap();
        assert!(other.is_some());
    }

    #[test]
    fn activated_generation_rejects_the_old_incarnation_and_entry() {
        let directory = directory(1);
        let old = snapshot(0, 1, 1, 64, 7);
        directory.install(old).unwrap();

        let mut rotation = directory.acquire_rotation_write(0, 3, 1).unwrap();
        assert_eq!(rotation.mark_unreadable().unwrap(), old);
        let activated = snapshot(0, 2, 8, 64, 9);
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
            Err(RegionReadError::StaleGeneration)
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
        directory.install(snapshot(0, 1, 1, 64, 7)).unwrap();

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
