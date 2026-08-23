//! Lifecycle seam for the RegionStore V2 profile.
//!
//! This module deliberately does not wrap `DiskCache::open`.  The legacy open
//! path allocates the complete `ShardedIndex` before it acquires the file lock
//! or inspects recovery state, and it can rebuild a dirty cache by walking the
//! data file.  V2 must make the recovery decision first and has only two index
//! construction paths: anonymous empty storage, or a private mapping of a
//! clean recovery image.
//!
//! The concrete state/image codecs and index mapping live behind
//! [`RegionV2Backend`].  Keeping this coordinator independent makes the order
//! of persistence operations testable without coupling it to the legacy
//! Superblock/checkpoint implementation.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::index::MAX_INDEX_SLOTS;
use crate::index_storage::{IndexStorage, IndexStorageError};
use crate::io_backend::{
    DirectIoMode, FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint, read_exact_at,
    write_all_at,
};
use crate::recovery_v2::{
    DataSuperblockV2, DataSuperblockV2Probe, RECOVERY_PAGE_SIZE, RecoveryState, STATE_FILE_SIZE,
    STATE_SLOT_COUNT, SelectedStateV2, StateBindingV2, StateRecordV2, StateSelectionError,
    latest_state_v2, prepare_running_barrier_v2,
};

/// Static inputs needed before recovery state is inspected.
///
/// Persistent identity and layout validation belongs to the backend because
/// it owns the Data Superblock V2 and recovery-state codecs.  `index_slots` is
/// kept here so the coordinator can prove that no full index allocation occurs
/// until after [`RegionV2Backend::inspect_recovery`] returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionV2Config {
    pub(crate) index_slots: usize,
}

impl RegionV2Config {
    fn validate(self) -> io::Result<Self> {
        if self.index_slots == 0 || self.index_slots > MAX_INDEX_SLOTS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 index slots must be in 1..=268435456",
            ));
        }
        Ok(self)
    }
}

/// Result of inspecting the latest valid V2 state record.
///
/// `Fresh` and `Running` both start with an empty anonymous index.  They are
/// distinct only for startup diagnostics.  A `Clean` token is backend-owned
/// and must bind the state record, data file, and recovery image identities.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum RecoveryPlan<T> {
    Fresh,
    Running,
    Clean(T),
}

/// How this process initialized its L2 index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionV2Startup {
    FreshEmpty,
    DirtyEmpty,
    CleanMapped,
    /// A state record selected a clean image, but lazy mapping validation
    /// rejected its small header/binding.  This is a cache miss event, not a
    /// fatal storage error.
    CleanRejectedEmpty,
}

/// Physical lifecycle operations required by [`RegionStoreV2`].
///
/// Implementations own file descriptors and the exclusive writer lock.
/// Methods returning `Ok(None)` from `map_clean_index` mean the clean image is
/// unusable and the coordinator should cold-start.  Resource failures such as
/// descriptor exhaustion or address-space exhaustion must be returned as
/// `Err`, not converted to `None`.
pub(crate) trait RegionV2Backend {
    type Index;
    type CleanImage;

    /// Acquire the exclusive ownership lock for data, state, and image files.
    fn acquire_exclusive(&mut self) -> io::Result<()>;

    /// Read Data Superblock V2 plus the two small state pages and decide
    /// whether a recovery image is eligible.  This must not allocate or scan
    /// the full index and must not read Region data extents.
    fn inspect_recovery(
        &mut self,
        config: RegionV2Config,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>>;

    /// Allocate a zero-filled, anonymous runtime index.
    fn anonymous_index(&mut self, slot_count: usize) -> io::Result<Self::Index>;

    /// Map an eligible image as writable private memory.  Returning `None`
    /// safely rejects a corrupt or mismatched cache image.
    fn map_clean_index(
        &mut self,
        clean: &Self::CleanImage,
        slot_count: usize,
    ) -> io::Result<Option<Self::Index>>;

    /// Publish RUNNING to both state slots, then issue one fdatasync.
    ///
    /// Both slots must be replaced: if only the newest slot were RUNNING and
    /// that page later tore, selection could fall back to the previous CLEAN
    /// image after this session had already reused Region bytes.  Open must not
    /// expose the index or start workers until this barrier succeeds.
    fn publish_running(&mut self) -> io::Result<()>;

    /// Stop producers and ensure no completion can mutate runtime state after
    /// the close sequence proceeds.  This operation does not flush data.
    fn freeze_runtime(&mut self) -> io::Result<()>;

    /// Drain accepted append work, seal staging buffers, wait for data I/O,
    /// and fdatasync the data file.  Used only by warm close.
    fn flush_data_for_warm_close(&mut self) -> io::Result<()>;

    /// Encode, install, and durably sync the next recovery image from one
    /// frozen runtime view.  It must not publish CLEAN.
    fn write_warm_image(&mut self, index: &Self::Index) -> io::Result<()>;

    /// Publish and fdatasync CLEAN to one slot after the data and image
    /// barriers succeed.  The other slot remains RUNNING, so a damaged CLEAN
    /// page makes the next process cold-start instead of reviving an older
    /// image.
    fn publish_clean(&mut self) -> io::Result<()>;

    /// Release the exclusive writer lock.  Must be safe to call during error
    /// unwinding after a successful `acquire_exclusive`.
    fn release_exclusive(&mut self) -> io::Result<()>;
}

/// Sidecar files owned by the first RegionStore V2 vertical slice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionV2Files {
    pub(crate) data: PathBuf,
    pub(crate) state: PathBuf,
    pub(crate) image: PathBuf,
}

impl RegionV2Files {
    pub(crate) fn new(
        data: impl Into<PathBuf>,
        state: impl Into<PathBuf>,
        image: impl Into<PathBuf>,
    ) -> Self {
        Self {
            data: data.into(),
            state: state.into(),
            image: image.into(),
        }
    }
}

/// Concrete V2 state/index lifecycle backed by one data file and two sidecars.
///
/// This intentionally implements only the index/lifecycle vertical slice.
/// Warm close is rejected until one frozen view can encode the index together
/// with Region/FIFO/epoch/accounting metadata. No index-only CLEAN authority is
/// ever published or recovered, and no legacy checkpoint or record scan is
/// reachable from this backend.
pub(crate) struct FileRegionV2Backend {
    files: RegionV2Files,
    /// Used when the data file is missing, empty, or not V2.  Existing V2
    /// files retain their on-disk identities but must match this geometry and
    /// configuration fingerprint.
    format_data: DataSuperblockV2,
    data_file: Option<FileBackend>,
    state_lock: Option<FileBackend>,
    state_file: Option<File>,
    data: Option<DataSuperblockV2>,
    current_state: Option<SelectedStateV2>,
    locked: bool,
}

impl FileRegionV2Backend {
    pub(crate) fn new(files: RegionV2Files, format_data: DataSuperblockV2) -> Self {
        Self {
            files,
            format_data,
            data_file: None,
            state_lock: None,
            state_file: None,
            data: None,
            current_state: None,
            locked: false,
        }
    }

    fn state_file(&mut self) -> io::Result<&mut File> {
        self.state_file
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "V2 state file is not open"))
    }

    fn data_superblock(&self) -> io::Result<DataSuperblockV2> {
        self.data.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 data superblock was not inspected",
            )
        })
    }
}

impl RegionV2Backend for FileRegionV2Backend {
    type Index = IndexStorage;
    type CleanImage = ();

    fn acquire_exclusive(&mut self) -> io::Result<()> {
        if self.locked {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "RegionStore V2 backend is already locked",
            ));
        }
        if self.files.data == self.files.state
            || self.files.data == self.files.image
            || self.files.state == self.files.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 data/state/image paths must be distinct",
            ));
        }
        let data = FileBackend::open_with_io_mode(&self.files.data, DirectIoMode::Buffered)?;
        data.try_lock_exclusive()?;
        let state_lock =
            match FileBackend::open_with_io_mode(&self.files.state, DirectIoMode::Buffered) {
                Ok(state) => state,
                Err(error) => {
                    let _ = data.unlock();
                    return Err(error);
                }
            };
        let aliases_data = match data.is_same_file(&state_lock) {
            Ok(aliases_data) => aliases_data,
            Err(error) => {
                let _ = data.unlock();
                return Err(error);
            }
        };
        if aliases_data {
            let _ = data.unlock();
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 data and state paths resolve to the same file",
            ));
        }
        if let Err(error) = state_lock.try_lock_exclusive() {
            let _ = data.unlock();
            return Err(error);
        }
        let state = match state_lock.try_clone_control_file() {
            Ok(state) => state,
            Err(error) => {
                let _ = state_lock.unlock();
                let _ = data.unlock();
                return Err(error);
            }
        };
        self.data_file = Some(data);
        self.state_lock = Some(state_lock);
        self.state_file = Some(state);
        self.locked = true;
        Ok(())
    }

    fn inspect_recovery(
        &mut self,
        _config: RegionV2Config,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>> {
        let format_data = self.format_data;
        let (data, fresh) = {
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
            })?;
            let state_file = self.state_file.as_mut().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 state file is not open")
            })?;
            inspect_or_format_data(data_file, state_file, format_data)?
        };
        self.data = Some(data);

        let pages = read_state_pages(self.state_file()?)?;
        let recovery_state = match latest_state_v2([&pages[0], &pages[1]]) {
            Ok(selected) => selected,
            Err(StateSelectionError::ConflictingGeneration(_)) => None,
            Err(StateSelectionError::UnsupportedVersion { slot, version }) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported V2 state format version {version} in slot {slot}"),
                ));
            }
        };
        // A conflicting same-generation pair is disposable cache state. Keep
        // the greatest decodable record only so RUNNING advances beyond it.
        self.current_state = select_state_for_fence(&pages);
        if fresh {
            return Ok(RecoveryPlan::Fresh);
        }
        let Some(selected) = recovery_state else {
            return Ok(RecoveryPlan::Running);
        };
        if selected.record.state != RecoveryState::Clean {
            return Ok(RecoveryPlan::Running);
        }
        // The concrete backend cannot yet validate a complete frozen Region
        // view, so even a codec-valid CLEAN record is deliberately cold.
        Ok(RecoveryPlan::Running)
    }

    fn anonymous_index(&mut self, slot_count: usize) -> io::Result<Self::Index> {
        IndexStorage::anonymous(slot_count).map_err(index_storage_io_error)
    }

    fn map_clean_index(
        &mut self,
        _clean: &Self::CleanImage,
        _slot_count: usize,
    ) -> io::Result<Option<Self::Index>> {
        Ok(None)
    }

    fn publish_running(&mut self) -> io::Result<()> {
        let binding = StateBindingV2::from_data(self.data_superblock()?, None);
        let barrier = prepare_running_barrier_v2(self.current_state, binding)
            .map_err(|_| io::Error::other("V2 RUNNING generation cannot advance"))?;
        let state = self.state_file()?;
        state.set_len(STATE_FILE_SIZE as u64)?;
        write_state_page(state, &barrier.first.page, barrier.first.offset())?;
        write_state_page(state, &barrier.second.page, barrier.second.offset())?;
        // One barrier covers both full-page writes.  No operation can be
        // admitted before this method returns success.
        state.sync_data()?;
        self.current_state = Some(SelectedStateV2 {
            slot: barrier.second.slot,
            record: barrier.second.record,
        });
        Ok(())
    }

    fn freeze_runtime(&mut self) -> io::Result<()> {
        // This vertical slice has no append workers yet.  The RegionManager
        // integration point must stop producers and completions here.
        Ok(())
    }

    fn flush_data_for_warm_close(&mut self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "V2 warm recovery requires a complete frozen Region view",
        ))
    }

    fn write_warm_image(&mut self, _index: &Self::Index) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "V2 warm recovery requires a complete frozen Region view",
        ))
    }

    fn publish_clean(&mut self) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "V2 warm recovery requires a complete frozen Region view",
        ))
    }

    fn release_exclusive(&mut self) -> io::Result<()> {
        if !self.locked {
            return Ok(());
        }
        let state_result = self
            .state_lock
            .as_ref()
            .map(IoBackend::unlock)
            .unwrap_or(Ok(()));
        let data_result = self
            .data_file
            .as_ref()
            .map(IoBackend::unlock)
            .unwrap_or(Ok(()));
        self.locked = false;
        self.state_file.take();
        self.state_lock.take();
        self.data_file.take();
        state_result.and(data_result)
    }
}

/// Minimal RegionStore V2 ownership and recovery coordinator.
///
/// The steady-state Region append/read implementation will be attached below
/// this seam.  The seam already freezes the two important contracts:
///
/// - open never calls the legacy `DiskCache::open` or its scan recovery;
/// - fast close leaves RUNNING, while the generic warm sequence publishes
///   CLEAN last. The concrete backend rejects warm close until Region metadata
///   can join the frozen image.
pub(crate) struct RegionStoreV2<B: RegionV2Backend> {
    backend: B,
    index: Option<B::Index>,
    startup: RegionV2Startup,
    closed: bool,
}

impl<B: RegionV2Backend> RegionStoreV2<B> {
    pub(crate) fn open_v2(config: RegionV2Config, mut backend: B) -> io::Result<Self> {
        let config = config.validate()?;
        backend.acquire_exclusive()?;

        let opened = (|| {
            // This decision intentionally precedes both index construction
            // calls.  A 100M-slot dirty cache therefore takes the anonymous
            // zero-page path without first allocating a legacy Vec index.
            let plan = backend.inspect_recovery(config)?;
            let (index, startup) = match plan {
                RecoveryPlan::Fresh => (
                    backend.anonymous_index(config.index_slots)?,
                    RegionV2Startup::FreshEmpty,
                ),
                RecoveryPlan::Running => (
                    backend.anonymous_index(config.index_slots)?,
                    RegionV2Startup::DirtyEmpty,
                ),
                RecoveryPlan::Clean(clean) => {
                    match backend.map_clean_index(&clean, config.index_slots)? {
                        Some(index) => (index, RegionV2Startup::CleanMapped),
                        None => (
                            backend.anonymous_index(config.index_slots)?,
                            RegionV2Startup::CleanRejectedEmpty,
                        ),
                    }
                }
            };

            // RUNNING is the no-reuse barrier for the selected clean image.
            // It is written only after index setup is known to have succeeded,
            // but before the caller can observe or mutate the recovered view.
            backend.publish_running()?;
            Ok((index, startup))
        })();

        match opened {
            Ok((index, startup)) => Ok(Self {
                backend,
                index: Some(index),
                startup,
                closed: false,
            }),
            Err(error) => {
                let _ = backend.release_exclusive();
                Err(error)
            }
        }
    }

    pub(crate) const fn startup(&self) -> RegionV2Startup {
        self.startup
    }

    pub(crate) fn index(&self) -> io::Result<&B::Index> {
        self.index.as_ref().ok_or_else(closed_error)
    }

    pub(crate) fn index_mut(&mut self) -> io::Result<&mut B::Index> {
        self.index.as_mut().ok_or_else(closed_error)
    }

    /// Stop the process without producing a recovery image.
    ///
    /// The state remains RUNNING, so the next open must choose an empty index.
    pub(crate) fn close_fast(&mut self) -> io::Result<()> {
        self.close(false)
    }

    /// Produce a clean warm-restart image and publish CLEAN last.
    pub(crate) fn close_warm(&mut self) -> io::Result<()> {
        self.close(true)
    }

    fn close(&mut self, warm: bool) -> io::Result<()> {
        if self.closed {
            return Ok(());
        }

        // Preserve the first protocol error while still releasing the writer
        // lock.  In particular, never call publish_clean after an earlier warm
        // close step fails; the durable RUNNING record remains authoritative.
        let mut result = self.backend.freeze_runtime();
        if result.is_ok() && warm {
            result = self.backend.flush_data_for_warm_close();
            if result.is_ok() {
                result = match self.index.as_ref() {
                    Some(index) => self.backend.write_warm_image(index),
                    None => Err(closed_error()),
                };
            }
            if result.is_ok() {
                result = self.backend.publish_clean();
            }
        }

        // Drop the private/anonymous mapping before releasing ownership.  A
        // concrete backend may then close all descriptors in release_exclusive.
        self.index.take();
        let unlock = self.backend.release_exclusive();
        self.closed = true;
        result.and(unlock)
    }
}

impl<B: RegionV2Backend> Drop for RegionStoreV2<B> {
    fn drop(&mut self) {
        if !self.closed {
            // Drop is deliberately equivalent to best-effort fast close.  It
            // must never turn an unrequested shutdown into an O(index) warm
            // image write.
            let _ = self.close_fast();
        }
    }
}

fn inspect_or_format_data(
    file: &FileBackend,
    state: &mut File,
    format_data: DataSuperblockV2,
) -> io::Result<(DataSuperblockV2, bool)> {
    let file_len = file.len()?;
    if file_len >= RECOVERY_PAGE_SIZE as u64 {
        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        read_exact_at(file, &mut page, 0)?;
        match DataSuperblockV2::probe(&page) {
            DataSuperblockV2Probe::Valid(data) => {
                if data.geometry != format_data.geometry
                    || data.hash_seed != format_data.hash_seed
                    || data.config_fingerprint != format_data.config_fingerprint
                    || file_len != data.geometry.data_file_len
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "existing V2 data geometry/configuration does not match",
                    ));
                }
                return Ok((data, false));
            }
            DataSuperblockV2Probe::Unsupported(version) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported V2 data format version {version}"),
                ));
            }
            DataSuperblockV2Probe::Empty
            | DataSuperblockV2Probe::Corrupt
            | DataSuperblockV2Probe::Unrecognized
            | DataSuperblockV2Probe::Truncated => {}
        }
    }

    // V2 is a disposable cache profile. Missing, interrupted, legacy, and
    // unrecognized cache bytes are cold-formatted with a new caller-supplied
    // identity; no old Region record is scanned or made reachable. Invalidate
    // every old CLEAN authority durably *before* touching Data Superblock V2.
    // Otherwise an interrupted reset which reused an identity could make an
    // old image and old Region bytes eligible again.
    state.set_len(0)?;
    state.sync_data()?;
    let encoded = format_data
        .encode()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid V2 data format"))?;
    file.set_len(format_data.geometry.data_file_len)?;
    write_all_at(file, WritePoint::Superblock, &encoded, 0)?;
    file.sync(SyncPoint::FormatClean, SyncMode::All)?;
    Ok((format_data, true))
}

fn read_state_pages(file: &mut File) -> io::Result<[[u8; RECOVERY_PAGE_SIZE]; STATE_SLOT_COUNT]> {
    let mut pages = [[0_u8; RECOVERY_PAGE_SIZE]; STATE_SLOT_COUNT];
    file.seek(SeekFrom::Start(0))?;
    for page in &mut pages {
        let mut filled = 0;
        while filled < page.len() {
            match file.read(&mut page[filled..]) {
                Ok(0) => return Ok(pages),
                Ok(read) => filled += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
    Ok(pages)
}

/// Select a usable authority for recovery and, on conflicting equal
/// generations, still retain a greatest-generation page so the two-slot
/// RUNNING overwrite advances beyond every prior valid record.
fn select_state_for_fence(
    pages: &[[u8; RECOVERY_PAGE_SIZE]; STATE_SLOT_COUNT],
) -> Option<SelectedStateV2> {
    if let Ok(selected) = latest_state_v2([&pages[0], &pages[1]]) {
        return selected;
    }
    pages
        .iter()
        .enumerate()
        .filter_map(|(slot, page)| {
            StateRecordV2::decode(page).map(|record| SelectedStateV2 {
                slot: slot as u8,
                record,
            })
        })
        .max_by_key(|selected| (selected.record.generation, selected.slot))
}

fn write_state_page(
    file: &mut File,
    page: &[u8; RECOVERY_PAGE_SIZE],
    offset: u64,
) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(page)
}

fn index_storage_io_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "RegionStore V2 is closed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_storage::IndexSlotV1;
    use crate::recovery_v2::{DataGeometryV2, PersistentId};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Clone, Copy)]
    enum Plan {
        Fresh,
        Running,
        Clean,
        RejectedClean,
    }

    struct Backend {
        plan: Plan,
        events: Rc<RefCell<Vec<&'static str>>>,
        fail_at: Option<&'static str>,
    }

    impl Backend {
        fn record(&self, event: &'static str) -> io::Result<()> {
            self.events.borrow_mut().push(event);
            if self.fail_at == Some(event) {
                Err(io::Error::other(event))
            } else {
                Ok(())
            }
        }
    }

    impl RegionV2Backend for Backend {
        type Index = usize;
        type CleanImage = bool;

        fn acquire_exclusive(&mut self) -> io::Result<()> {
            self.record("lock")
        }

        fn inspect_recovery(
            &mut self,
            _config: RegionV2Config,
        ) -> io::Result<RecoveryPlan<Self::CleanImage>> {
            self.record("inspect")?;
            Ok(match self.plan {
                Plan::Fresh => RecoveryPlan::Fresh,
                Plan::Running => RecoveryPlan::Running,
                Plan::Clean => RecoveryPlan::Clean(true),
                Plan::RejectedClean => RecoveryPlan::Clean(false),
            })
        }

        fn anonymous_index(&mut self, slot_count: usize) -> io::Result<Self::Index> {
            self.record("anonymous")?;
            Ok(slot_count)
        }

        fn map_clean_index(
            &mut self,
            clean: &Self::CleanImage,
            slot_count: usize,
        ) -> io::Result<Option<Self::Index>> {
            self.record("map")?;
            Ok(clean.then_some(slot_count))
        }

        fn publish_running(&mut self) -> io::Result<()> {
            self.record("running")
        }

        fn freeze_runtime(&mut self) -> io::Result<()> {
            self.record("freeze")
        }

        fn flush_data_for_warm_close(&mut self) -> io::Result<()> {
            self.record("data")
        }

        fn write_warm_image(&mut self, _index: &Self::Index) -> io::Result<()> {
            self.record("image")
        }

        fn publish_clean(&mut self) -> io::Result<()> {
            self.record("clean")
        }

        fn release_exclusive(&mut self) -> io::Result<()> {
            self.record("unlock")
        }
    }

    fn backend(
        plan: Plan,
        fail_at: Option<&'static str>,
    ) -> (Backend, Rc<RefCell<Vec<&'static str>>>) {
        let events = Rc::new(RefCell::new(Vec::new()));
        (
            Backend {
                plan,
                events: Rc::clone(&events),
                fail_at,
            },
            events,
        )
    }

    #[test]
    fn invalid_slot_capacity_is_rejected_before_locking_or_allocating() {
        for index_slots in [0, MAX_INDEX_SLOTS + 1] {
            let (backend, events) = backend(Plan::Fresh, None);
            assert!(RegionStoreV2::open_v2(RegionV2Config { index_slots }, backend).is_err());
            assert!(events.borrow().is_empty());
        }
    }

    #[test]
    fn recovery_is_inspected_before_any_index_construction() {
        for (plan, expected_startup, expected_events) in [
            (
                Plan::Fresh,
                RegionV2Startup::FreshEmpty,
                vec!["lock", "inspect", "anonymous", "running"],
            ),
            (
                Plan::Running,
                RegionV2Startup::DirtyEmpty,
                vec!["lock", "inspect", "anonymous", "running"],
            ),
            (
                Plan::Clean,
                RegionV2Startup::CleanMapped,
                vec!["lock", "inspect", "map", "running"],
            ),
            (
                Plan::RejectedClean,
                RegionV2Startup::CleanRejectedEmpty,
                vec!["lock", "inspect", "map", "anonymous", "running"],
            ),
        ] {
            let (backend, events) = backend(plan, None);
            let mut store =
                RegionStoreV2::open_v2(RegionV2Config { index_slots: 1024 }, backend).unwrap();
            assert_eq!(store.startup(), expected_startup);
            assert_eq!(*events.borrow(), expected_events);
            store.close_fast().unwrap();
        }
    }

    #[test]
    fn fast_close_leaves_running_without_data_or_image_publication() {
        let (backend, events) = backend(Plan::Fresh, None);
        let mut store = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend).unwrap();
        store.close_fast().unwrap();
        assert_eq!(
            *events.borrow(),
            vec![
                "lock",
                "inspect",
                "anonymous",
                "running",
                "freeze",
                "unlock"
            ]
        );
    }

    #[test]
    fn warm_close_publishes_clean_only_after_data_and_image() {
        let (backend, events) = backend(Plan::Clean, None);
        let mut store = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend).unwrap();
        store.close_warm().unwrap();
        assert_eq!(
            *events.borrow(),
            vec![
                "lock", "inspect", "map", "running", "freeze", "data", "image", "clean", "unlock"
            ]
        );
    }

    #[test]
    fn failed_warm_image_keeps_running_and_still_unlocks() {
        let (backend, events) = backend(Plan::Running, Some("image"));
        let mut store = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend).unwrap();
        assert!(store.close_warm().is_err());
        assert_eq!(
            *events.borrow(),
            vec![
                "lock",
                "inspect",
                "anonymous",
                "running",
                "freeze",
                "data",
                "image",
                "unlock"
            ]
        );
    }

    #[test]
    fn failed_running_barrier_aborts_open_and_releases_lock() {
        let (backend, events) = backend(Plan::Clean, Some("running"));
        let opened = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend);
        assert!(opened.is_err());
        assert_eq!(
            *events.borrow(),
            vec!["lock", "inspect", "map", "running", "unlock"]
        );
    }

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory {
        root: PathBuf,
        files: RegionV2Files,
    }

    impl TestDirectory {
        fn new() -> Self {
            let ordinal = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "cache-rs-region-v2-{}-{ordinal}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir(&root).unwrap();
            let files = RegionV2Files::new(
                root.join("data"),
                root.join("state"),
                root.join("recovery.image"),
            );
            Self { root, files }
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }

    fn persistent_id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn test_data_superblock() -> DataSuperblockV2 {
        let region_size = 2 * RECOVERY_PAGE_SIZE as u64;
        let region_count = 1;
        DataSuperblockV2 {
            generation: 1,
            cache_uuid: persistent_id(1),
            data_identity: persistent_id(2),
            geometry: DataGeometryV2 {
                data_file_len: DataGeometryV2::expected_file_len(region_size, region_count)
                    .unwrap(),
                region_size,
                region_count,
            },
            hash_seed: 3,
            config_fingerprint: 4,
        }
    }

    #[test]
    fn file_backend_rejects_incomplete_warm_image_and_next_open_is_empty() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 130 };
        let data = test_data_superblock();
        let value = IndexSlotV1 {
            hash: 11,
            location_raw: 22,
            seqno: 33,
            namespace_id: 44,
            flags: 0,
        };

        let mut fresh = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(fresh.startup(), RegionV2Startup::FreshEmpty);
        fresh.index_mut().unwrap().write_slot(0, value).unwrap();
        let error = fresh.close_warm().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(!directory.files.image.exists());

        let mut dirty = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(dirty.startup(), RegionV2Startup::DirtyEmpty);
        assert_eq!(
            dirty.index().unwrap().read_slot(0).unwrap(),
            IndexSlotV1::EMPTY
        );
        dirty.close_fast().unwrap();
    }

    #[test]
    fn data_and_state_inode_alias_is_rejected_without_truncation() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let marker = b"do-not-truncate";
        std::fs::write(&directory.files.data, marker).unwrap();
        std::fs::hard_link(&directory.files.data, &directory.files.state).unwrap();

        let opened = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        );
        assert!(matches!(
            opened,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert_eq!(std::fs::read(&directory.files.data).unwrap(), marker);
    }

    #[test]
    fn state_sidecar_lock_prevents_cross_data_file_races() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();

        let conflicting_files = RegionV2Files::new(
            directory.root.join("other-data"),
            directory.files.state.clone(),
            directory.root.join("other-image"),
        );
        let opened =
            RegionStoreV2::open_v2(config, FileRegionV2Backend::new(conflicting_files, data));
        assert!(opened.is_err());
        first.close_fast().unwrap();
    }
}
