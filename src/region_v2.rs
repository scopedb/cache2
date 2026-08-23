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
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::format::{REGION_HEADER_SIZE, RegionHeader, RegionState};
use crate::index::{IndexEntry, MAX_INDEX_SHARDS, MAX_INDEX_SLOTS};
use crate::index_storage::{
    IndexImageBindingV1, IndexPhysicalStats, IndexShardRangeV1, IndexStorageError,
    ShardedIndexStorage, canonical_index_shard_ranges,
};
use crate::index_v2::{
    IndexLookupV2, IndexMaskV2, IndexMutationAuthorityV2, IndexTransitionV2, IndexUpsertV2,
    RegionIndexV2,
};
use crate::io_backend::{
    ControlIoBackend, DirectIoMode, FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint,
    read_exact_at, write_all_at,
};
use crate::recovery_v2::{
    DATA_REGION_AREA_OFFSET_V2, DataSuperblockV2, DataSuperblockV2Probe, PersistentId,
    RECOVERY_IMAGE_INDEX_OFFSET_V1, RECOVERY_PAGE_SIZE, RecoveryImageHeaderV1,
    RecoveryImageHeaderV1Probe, RecoveryState, STATE_FILE_SIZE, STATE_SLOT_COUNT, SelectedStateV2,
    StateBindingV2, StatePageWriteV2, StateRecordV2, StateSelectionError, clean_image_matches_v2,
    latest_state_v2, prepare_next_state_v2, prepare_running_barrier_v2,
    recovery_image_index_len_v1,
};
use crate::region_manager_v2::{RegionManagerV2, RegionMutationErrorV2};
use crate::region_metadata_v1::{
    REGION_METADATA_V1_PAGE_SIZE, REGION_METADATA_V1_REGIONS_PER_PAGE,
    REGION_METADATA_V1_SHARDS_PER_PAGE, RegionMetadataRecordV1, RegionMetadataRootV1,
    RegionMetadataStateV1, RegionMetadataV1, RegionMetadataV1Error, ShardMetadataRecordV1,
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
        if !(8..=MAX_INDEX_SLOTS).contains(&self.index_slots) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 index slots must be in 8..=268435456",
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
    type Runtime;
    type CleanImage;
    type FrozenView;
    type PreparedClean;

    /// Acquire the exclusive ownership lock for data, state, and image files.
    fn acquire_exclusive(&mut self) -> io::Result<()>;

    /// Read Data Superblock V2 plus the two small state pages and decide
    /// whether a recovery image is eligible.  This must not allocate or scan
    /// the full index and must not read Region data extents.
    fn inspect_recovery(
        &mut self,
        config: RegionV2Config,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>>;

    /// Build one provisional empty runtime without starting any worker.
    fn anonymous_runtime(&mut self, config: RegionV2Config) -> io::Result<Self::Runtime>;

    /// Build one provisional runtime from a fully validated image. Returning
    /// `None` safely rejects the complete image without partially installing
    /// its index or Region metadata.
    fn map_clean_runtime(
        &mut self,
        clean: Self::CleanImage,
        config: RegionV2Config,
    ) -> io::Result<Option<Self::Runtime>>;

    /// Publish RUNNING to both state slots, then issue one fdatasync.
    ///
    /// Both slots must be replaced: if only the newest slot were RUNNING and
    /// that page later tore, selection could fall back to the previous CLEAN
    /// image after this session had already reused Region bytes.  Open must not
    /// expose the index or start workers until this barrier succeeds.
    fn publish_running(&mut self) -> io::Result<()>;

    /// Start workers or allocate Active Regions only after RUNNING is durable.
    /// On error, the backend must synchronously tear down every partially
    /// started worker because ownership of `runtime` has been consumed.
    fn start_runtime(&mut self, runtime: Self::Runtime) -> io::Result<Self::Runtime>;

    /// Stop a runtime without constructing recovery metadata. All mutation
    /// sources must be quiescent on both success and error returns.
    fn stop_fast(&mut self, runtime: Self::Runtime) -> io::Result<()>;

    /// Consume the runtime after every admitted operation is quiescent,
    /// including readers, producers, and completions, then produce one
    /// immutable owner of index + Region/FIFO/accounting state. On error, the
    /// backend must synchronously tear down every worker and completion source
    /// because ownership of `runtime` has been consumed.
    fn freeze_warm(&mut self, runtime: Self::Runtime) -> io::Result<Self::FrozenView>;

    /// Make data and one complete image durable. Success returns the only
    /// token which can authorize CLEAN publication.
    fn persist_frozen(&mut self, view: &Self::FrozenView) -> io::Result<Self::PreparedClean>;

    /// Publish and fdatasync CLEAN to one slot after the data and image
    /// barriers succeed.  The other slot remains RUNNING, so a damaged CLEAN
    /// page makes the next process cold-start instead of reviving an older
    /// image.
    fn publish_clean(&mut self, prepared: Self::PreparedClean) -> io::Result<()>;

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

const REGION_V2_HEALTHY: u8 = 0;
const REGION_V2_MISS_ONLY: u8 = 1;
const _: () = assert!(REGION_HEADER_SIZE == RECOVERY_PAGE_SIZE);

/// One-way health fence shared by the live, frozen, and prepared-clean owners.
/// Once a lazy index fault rejects the recovery image, no later phase may
/// publish CLEAN from the partially trusted authority.
#[derive(Clone)]
pub(crate) struct RegionV2HealthLatch {
    state: Arc<AtomicU8>,
}

impl RegionV2HealthLatch {
    fn healthy() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(REGION_V2_HEALTHY)),
        }
    }

    fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == REGION_V2_HEALTHY
    }

    fn enter_miss_only(&self) {
        self.state.store(REGION_V2_MISS_ONLY, Ordering::Release);
    }

    fn require_healthy(&self) -> io::Result<()> {
        self.is_healthy().then_some(()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore V2 is miss-only and cannot publish CLEAN",
            )
        })
    }
}

/// The only steady-state owner of Region visibility, FIFO state, and logical
/// accounting. Its mutex is intentionally narrower than the index: an index
/// mutation holds it across one bounded shard probe and the matching
/// accounting commit, but never across record encoding, staging, queueing, or
/// device I/O.
struct RegionManagerAuthorityV2 {
    inner: Mutex<RegionManagerV2>,
    health: RegionV2HealthLatch,
}

impl RegionManagerAuthorityV2 {
    fn new(manager: RegionManagerV2, health: RegionV2HealthLatch) -> Self {
        Self {
            inner: Mutex::new(manager),
            health,
        }
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, RegionManagerV2>> {
        self.health.require_healthy()?;
        match self.inner.lock() {
            Ok(guard) if self.health.is_healthy() => Ok(guard),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore V2 became miss-only while acquiring Region authority",
            )),
            Err(_) => {
                self.health.enter_miss_only();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RegionStore V2 Region authority is poisoned",
                ))
            }
        }
    }

    fn is_visible(&self, entry: IndexEntry) -> bool {
        let Ok(manager) = self.lock() else {
            return false;
        };
        let visible = manager.is_visible(entry);
        visible && self.health.is_healthy()
    }

    fn begin_index_mutation(&self) -> io::Result<RegionIndexMutationAuthorityV2<'_>> {
        Ok(RegionIndexMutationAuthorityV2 {
            manager: self.lock()?,
            health: self.health.clone(),
            accounting_error: None,
        })
    }

    fn empty_active_headers(&self, data: DataSuperblockV2) -> io::Result<Vec<(u64, RegionHeader)>> {
        let manager = self.lock()?;
        snapshot_empty_active_region_headers(data, &manager)
    }

    fn into_inner(self) -> io::Result<RegionManagerV2> {
        match self.inner.into_inner() {
            Ok(manager) if self.health.is_healthy() => Ok(manager),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore V2 became miss-only while freezing Region authority",
            )),
            Err(_) => {
                self.health.enter_miss_only();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RegionStore V2 Region authority is poisoned",
                ))
            }
        }
    }
}

/// One manager guard paired with one index mutation. `commit` cannot return
/// through the index API after the physical slot is installed, so accounting
/// failures are recorded here, latch miss-only immediately, and are surfaced
/// when the short critical section ends.
struct RegionIndexMutationAuthorityV2<'a> {
    manager: MutexGuard<'a, RegionManagerV2>,
    health: RegionV2HealthLatch,
    accounting_error: Option<RegionMutationErrorV2>,
}

impl RegionIndexMutationAuthorityV2<'_> {
    fn finish(self) -> io::Result<()> {
        drop(self.manager);
        match self.accounting_error {
            Some(error) => Err(region_mutation_io_error(error)),
            None => self.health.require_healthy(),
        }
    }
}

impl IndexMutationAuthorityV2 for RegionIndexMutationAuthorityV2<'_> {
    fn is_visible(&self, entry: IndexEntry) -> bool {
        self.manager.is_visible(entry)
    }

    fn commit(&mut self, transition: IndexTransitionV2) {
        if self.accounting_error.is_none() {
            if let Err(error) = self.manager.apply_index_transition(transition) {
                self.health.enter_miss_only();
                self.accounting_error = Some(error);
            }
        }
    }
}

pub(crate) struct FileRegionRuntimeV2 {
    index: RegionIndexV2,
    manager: RegionManagerAuthorityV2,
    health: RegionV2HealthLatch,
}

pub(crate) struct FrozenFileRegionViewV2 {
    index: RegionIndexV2,
    metadata: RegionMetadataV1,
    health: RegionV2HealthLatch,
}

pub(crate) struct CleanFileRegionImageV1 {
    file: File,
    header: RecoveryImageHeaderV1,
    metadata: RegionMetadataV1,
}

pub(crate) struct PreparedFileRegionCleanV2 {
    state: StatePageWriteV2,
    health: RegionV2HealthLatch,
}

impl FileRegionRuntimeV2 {
    /// Installs one complete authority. Recovery metadata is consumed here so
    /// the live runtime cannot retain a stale second copy beside the manager.
    fn install(index: ShardedIndexStorage, metadata: RegionMetadataV1) -> io::Result<Self> {
        let physical_stats = index.shard_stats().map_err(index_storage_io_error)?;
        let slot_count = u64::try_from(index.slot_count()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "V2 index capacity is too large")
        })?;
        if metadata.root.index_slots != slot_count
            || metadata.root.shard_count as usize != index.shard_count()
            || !metadata_shard_stats_match(&metadata, &physical_stats)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 index and Region metadata do not describe one authority",
            ));
        }
        let manager = RegionManagerV2::from_metadata(metadata).map_err(region_metadata_io_error)?;
        let health = RegionV2HealthLatch::healthy();
        Ok(Self {
            index: RegionIndexV2::from_storage(index),
            manager: RegionManagerAuthorityV2::new(manager, health.clone()),
            health,
        })
    }

    /// The first production-facing operation intentionally exposes only typed
    /// point semantics. A lazy image failure latches the whole L2 miss-only;
    /// it is never surfaced as a cache hit or allowed to authorize CLEAN.
    fn lookup_snapshot(&self, hash: u64) -> io::Result<IndexLookupV2> {
        if !self.health.is_healthy() {
            return Ok(IndexLookupV2::Miss);
        }
        match self.index.lookup_raw(hash) {
            Ok(IndexLookupV2::Hit(entry)) if self.health.is_healthy() => {
                Ok(if self.manager.is_visible(entry) {
                    IndexLookupV2::Hit(entry)
                } else {
                    IndexLookupV2::Miss
                })
            }
            Ok(result) if self.health.is_healthy() => Ok(result),
            Ok(_) => Ok(IndexLookupV2::Miss),
            Err(_) => {
                self.health.enter_miss_only();
                Ok(IndexLookupV2::Miss)
            }
        }
    }

    /// Revalidate only the exact index identity. Region visibility is checked
    /// separately by the read snapshot owner after the index shard is free.
    fn revalidate_exact(&self, hash: u64, expected: crate::index::IndexEntry) -> io::Result<bool> {
        if !self.health.is_healthy() {
            return Ok(false);
        }
        match self.index.revalidate_exact(hash, expected) {
            Ok(matches) if self.health.is_healthy() => Ok(matches),
            Ok(_) => Ok(false),
            Err(_) => {
                self.health.enter_miss_only();
                Ok(false)
            }
        }
    }

    fn upsert_entry(&self, hash: u64, entry: IndexEntry) -> io::Result<IndexUpsertV2> {
        self.mutate_index(|index, authority| index.upsert_with_authority(hash, entry, authority))
    }

    fn mask_hash(&self, hash: u64, seqno: u64) -> io::Result<IndexMaskV2> {
        self.mutate_index(|index, authority| {
            index.mask_if_newer_with_authority(hash, seqno, authority)
        })
    }

    fn remove_entry(
        &self,
        hash: u64,
        expected: IndexEntry,
    ) -> io::Result<Option<IndexTransitionV2>> {
        self.mutate_index(|index, authority| {
            index.remove_if_with_authority(hash, expected, authority)
        })
    }

    fn replace_entry(
        &self,
        hash: u64,
        expected: IndexEntry,
        replacement: IndexEntry,
    ) -> io::Result<Option<IndexTransitionV2>> {
        self.mutate_index(|index, authority| {
            index.replace_if_with_authority(hash, expected, replacement, authority)
        })
    }

    fn normalize_mask(&self, hash: u64, seqno: u64) -> io::Result<Option<IndexTransitionV2>> {
        self.mutate_index(|index, authority| {
            index.normalize_mask_if_with_authority(hash, seqno, authority)
        })
    }

    fn mutate_index<T>(
        &self,
        mutation: impl FnOnce(
            &RegionIndexV2,
            &mut RegionIndexMutationAuthorityV2<'_>,
        ) -> Result<T, IndexStorageError>,
    ) -> io::Result<T> {
        let mut authority = self.manager.begin_index_mutation()?;
        let result = mutation(&self.index, &mut authority);
        let accounting = authority.finish();
        match result {
            Ok(value) => {
                accounting?;
                Ok(value)
            }
            Err(error) => {
                self.health.enter_miss_only();
                Err(index_storage_io_error(error))
            }
        }
    }
}

pub(crate) trait RegionV2FileSystem {
    type File: ControlIoBackend;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File>;

    fn create_new(&self, path: &Path) -> io::Result<Self::File>;

    fn remove_file(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()>;

    fn sync_parent(&self, path: &Path) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemRegionV2FileSystem;

impl RegionV2FileSystem for SystemRegionV2FileSystem {
    type File = FileBackend;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File> {
        if create {
            FileBackend::open_with_io_mode(path, DirectIoMode::Buffered)
        } else {
            FileBackend::open_existing_with_io_mode(path, DirectIoMode::Buffered)
        }
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File> {
        FileBackend::create_new_buffered(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        std::fs::rename(source, destination)
    }

    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        File::open(parent_directory(path))?.sync_all()
    }
}

/// Concrete V2 state/index lifecycle backed by one data file and two sidecars.
///
/// This intentionally remains a crate-private recovery vertical slice. It can
/// persist and recover one complete frozen index + Region/FIFO/accounting view,
/// but it is not connected to the production Region append/read workers yet.
/// No index-only CLEAN authority is published, and no legacy checkpoint or
/// record scan is reachable from this backend.
pub(crate) struct FileRegionV2Backend<F = SystemRegionV2FileSystem>
where
    F: RegionV2FileSystem,
{
    files: RegionV2Files,
    /// Used when the data file is missing, empty, or not V2.  Existing V2
    /// files retain their on-disk identities but must match this geometry and
    /// configuration fingerprint.
    format_data: DataSuperblockV2,
    file_system: F,
    data_file: Option<F::File>,
    state_file: Option<F::File>,
    data: Option<DataSuperblockV2>,
    current_state: Option<SelectedStateV2>,
    prepared_clean: Option<(u8, StateRecordV2)>,
    cold_reset_needed: bool,
    materialize_active_headers: bool,
    locked: bool,
}

impl FileRegionV2Backend<SystemRegionV2FileSystem> {
    pub(crate) fn new(files: RegionV2Files, format_data: DataSuperblockV2) -> Self {
        Self::new_with_file_system(files, format_data, SystemRegionV2FileSystem)
    }
}

impl<F> FileRegionV2Backend<F>
where
    F: RegionV2FileSystem,
{
    fn new_with_file_system(
        files: RegionV2Files,
        format_data: DataSuperblockV2,
        file_system: F,
    ) -> Self {
        Self {
            files,
            format_data,
            file_system,
            data_file: None,
            state_file: None,
            data: None,
            current_state: None,
            prepared_clean: None,
            cold_reset_needed: false,
            materialize_active_headers: false,
            locked: false,
        }
    }

    fn state_file(&self) -> io::Result<&F::File> {
        self.state_file
            .as_ref()
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

impl<F> RegionV2Backend for FileRegionV2Backend<F>
where
    F: RegionV2FileSystem,
{
    type Runtime = FileRegionRuntimeV2;
    type CleanImage = CleanFileRegionImageV1;
    type FrozenView = FrozenFileRegionViewV2;
    type PreparedClean = PreparedFileRegionCleanV2;

    fn acquire_exclusive(&mut self) -> io::Result<()> {
        if self.locked {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "RegionStore V2 backend is already locked",
            ));
        }
        if self.format_data.geometry.region_count < 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 requires at least two Regions",
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
        if parent_directory(&self.files.data) != parent_directory(&self.files.state)
            || parent_directory(&self.files.data) != parent_directory(&self.files.image)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 data/state/image files must share one directory",
            ));
        }
        let temporary = recovery_temporary_path(&self.files.image);
        if temporary == self.files.data
            || temporary == self.files.state
            || temporary == self.files.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 recovery temporary path collides with a cache file",
            ));
        }
        let data = self.file_system.open(&self.files.data, true)?;
        data.try_lock_exclusive()?;
        let state = match self.file_system.open(&self.files.state, true) {
            Ok(state) => state,
            Err(error) => {
                let _ = data.unlock();
                return Err(error);
            }
        };
        let aliases_data = match data.is_same_file(&state) {
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
        if let Err(error) = state.try_lock_exclusive() {
            let _ = data.unlock();
            return Err(error);
        }
        self.data_file = Some(data);
        self.state_file = Some(state);
        self.locked = true;
        Ok(())
    }

    fn inspect_recovery(
        &mut self,
        config: RegionV2Config,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>> {
        let format_data = self.format_data;
        let (data, fresh) = {
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
            })?;
            let state_file = self.state_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 state file is not open")
            })?;
            inspect_or_format_data(data_file, state_file, format_data)?
        };
        self.data = Some(data);
        self.cold_reset_needed = !fresh;

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
        if !selected.record.binding.matches_data(data) {
            return Ok(RecoveryPlan::Running);
        }

        let image = match self.file_system.open(&self.files.image, false) {
            Ok(image) => image,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(RecoveryPlan::Running);
            }
            Err(error) => return Err(error),
        };
        let data_file = self.data_file.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
        })?;
        let state_file = self.state_file.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "V2 state file is not open")
        })?;
        if image.is_same_file(data_file)? || image.is_same_file(state_file)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore V2 image aliases the data or state file",
            ));
        }

        let actual_file_len = image.len()?;
        if actual_file_len < RECOVERY_PAGE_SIZE as u64 {
            return Ok(RecoveryPlan::Running);
        }
        let mut header_page = [0_u8; RECOVERY_PAGE_SIZE];
        if let Err(error) = read_exact_at(&image, &mut header_page, 0) {
            return if error.kind() == io::ErrorKind::UnexpectedEof {
                Ok(RecoveryPlan::Running)
            } else {
                Err(error)
            };
        }
        let header = match RecoveryImageHeaderV1::probe(&header_page) {
            RecoveryImageHeaderV1Probe::Valid(header) => header,
            RecoveryImageHeaderV1Probe::Unsupported(version) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported V2 recovery image version {version}"),
                ));
            }
            RecoveryImageHeaderV1Probe::Empty
            | RecoveryImageHeaderV1Probe::Corrupt
            | RecoveryImageHeaderV1Probe::Unrecognized
            | RecoveryImageHeaderV1Probe::Truncated => return Ok(RecoveryPlan::Running),
        };
        let expected_slots = u64::try_from(config.index_slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "V2 index capacity does not fit u64",
            )
        })?;
        let expected_index_len = recovery_image_index_len_v1(expected_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid V2 index image length")
        })?;
        if !clean_image_matches_v2(
            selected.record,
            data,
            header,
            actual_file_len,
            expected_slots,
            expected_index_len,
        ) || header.region_table_len > maximum_region_metadata_len(data.geometry.region_count)?
        {
            return Ok(RecoveryPlan::Running);
        }

        let metadata_len = usize::try_from(header.region_table_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "V2 Region metadata exceeds this address space",
            )
        })?;
        let mut metadata_bytes = Vec::new();
        metadata_bytes
            .try_reserve_exact(metadata_len)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate V2 Region metadata",
                )
            })?;
        metadata_bytes.resize(metadata_len, 0);
        if let Err(error) = read_exact_at(&image, &mut metadata_bytes, header.region_table_offset) {
            return if error.kind() == io::ErrorKind::UnexpectedEof {
                Ok(RecoveryPlan::Running)
            } else {
                Err(error)
            };
        }
        let metadata = match RegionMetadataV1::decode_owned(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RegionMetadataV1Error::UnsupportedVersion(version)) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported V2 Region metadata version {version}"),
                ));
            }
            Err(RegionMetadataV1Error::Allocation) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot decode V2 Region metadata",
                ));
            }
            Err(_) => return Ok(RecoveryPlan::Running),
        };
        if !metadata.matches_image(data, header) {
            return Ok(RecoveryPlan::Running);
        }
        let file = image.try_clone_control_file()?;
        self.cold_reset_needed = false;
        Ok(RecoveryPlan::Clean(CleanFileRegionImageV1 {
            file,
            header,
            metadata,
        }))
    }

    fn anonymous_runtime(&mut self, config: RegionV2Config) -> io::Result<Self::Runtime> {
        let data = self.data_superblock()?;
        if self.cold_reset_needed {
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
            })?;
            let state_file = self.state_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 state file is not open")
            })?;
            format_empty_data(data_file, state_file, data)?;
            self.current_state = None;
            self.cold_reset_needed = false;
        }
        let metadata = empty_region_metadata(data, config.index_slots)?;
        let index =
            ShardedIndexStorage::anonymous(config.index_slots).map_err(index_storage_io_error)?;
        let runtime = FileRegionRuntimeV2::install(index, metadata)?;
        self.materialize_active_headers = true;
        Ok(runtime)
    }

    fn map_clean_runtime(
        &mut self,
        clean: Self::CleanImage,
        config: RegionV2Config,
    ) -> io::Result<Option<Self::Runtime>> {
        let data = self.data_superblock()?;
        let expected_slots = u64::try_from(config.index_slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "V2 index capacity does not fit u64",
            )
        })?;
        let expected_index_len = recovery_image_index_len_v1(expected_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid V2 index image length")
        })?;
        let actual_file_len = clean.file.metadata()?.len();
        let eligible = self.current_state.is_some_and(|selected| {
            clean_image_matches_v2(
                selected.record,
                data,
                clean.header,
                actual_file_len,
                expected_slots,
                expected_index_len,
            )
        }) && clean.metadata.matches_image(data, clean.header)
            && clean.metadata.validate().is_ok();
        if !eligible {
            self.cold_reset_needed = true;
            return Ok(None);
        }
        let shard_stats = metadata_shard_stats(&clean.metadata)?;
        let binding = index_image_binding(clean.header);
        let index = ShardedIndexStorage::map_private(
            &clean.file,
            clean.header.index_offset,
            config.index_slots,
            binding,
            &shard_stats,
        )
        .map_err(index_storage_io_error)?;
        let runtime = FileRegionRuntimeV2::install(index, clean.metadata)?;
        self.materialize_active_headers = false;
        Ok(Some(runtime))
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
        state.sync(SyncPoint::V2RunningState, SyncMode::Data)?;
        self.current_state = Some(SelectedStateV2 {
            slot: barrier.second.slot,
            record: barrier.second.record,
        });
        Ok(())
    }

    fn start_runtime(&mut self, runtime: Self::Runtime) -> io::Result<Self::Runtime> {
        if self.materialize_active_headers {
            let data = self.data_superblock()?;
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
            })?;
            materialize_empty_active_region_headers(data_file, data, &runtime.manager)?;
            self.materialize_active_headers = false;
        }
        Ok(runtime)
    }

    fn stop_fast(&mut self, _runtime: Self::Runtime) -> io::Result<()> {
        Ok(())
    }

    fn freeze_warm(&mut self, runtime: Self::Runtime) -> io::Result<Self::FrozenView> {
        runtime.health.require_healthy()?;
        let FileRegionRuntimeV2 {
            index,
            manager,
            health,
        } = runtime;
        let manager = manager.into_inner()?;
        let shards = index_shard_metadata(index.storage(), &health)?;
        let metadata = manager
            .freeze_metadata(shards)
            .map_err(region_metadata_io_error)?;
        health.require_healthy()?;
        Ok(FrozenFileRegionViewV2 {
            index,
            metadata,
            health,
        })
    }

    fn persist_frozen(&mut self, view: &Self::FrozenView) -> io::Result<Self::PreparedClean> {
        view.health.require_healthy()?;
        let source_metadata = &view.metadata;
        source_metadata
            .validate()
            .map_err(region_metadata_io_error)?;
        let storage = view.index.storage();
        let physical_stats = guarded_index_result(&view.health, storage.physical_stats())?;
        let shard_stats = guarded_index_result(&view.health, storage.shard_stats())?;
        if physical_stats.masked != 0
            || source_metadata.root.index_slots
                != u64::try_from(storage.slot_count()).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "V2 index capacity is too large")
                })?
            || source_metadata.root.physical_value_slots != physical_stats.value
            || source_metadata.root.physical_deleted_slots != physical_stats.deleted
            || source_metadata.root.physical_masked_slots != physical_stats.masked
            || !metadata_shard_stats_match(source_metadata, &shard_stats)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frozen V2 index and Region metadata accounting disagree",
            ));
        }

        let data = self.data_superblock()?;
        let image_generation = next_state_generation(self.current_state)?;
        let image_identity = derive_image_identity(data.data_identity, image_generation);
        let mut metadata = source_metadata.clone();
        metadata.root.image_identity = image_identity;
        metadata.root.image_generation = image_generation;
        let metadata_bytes = metadata.encode().map_err(region_metadata_io_error)?;
        let metadata_len = u64::try_from(metadata_bytes.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 Region metadata is too large",
            )
        })?;
        let index_slots = u64::try_from(storage.slot_count()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "V2 index capacity is too large")
        })?;
        let index_len = recovery_image_index_len_v1(index_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid V2 index image length")
        })?;
        let region_table_offset = RECOVERY_IMAGE_INDEX_OFFSET_V1
            .checked_add(index_len)
            .ok_or_else(|| io::Error::other("V2 image offset overflow"))?;
        let image_file_len = region_table_offset
            .checked_add(metadata_len)
            .ok_or_else(|| io::Error::other("V2 image length overflow"))?;
        let header = RecoveryImageHeaderV1 {
            cache_uuid: data.cache_uuid,
            data_identity: data.data_identity,
            data_superblock_generation: data.generation,
            hash_seed: data.hash_seed,
            config_fingerprint: data.config_fingerprint,
            image_identity,
            image_generation,
            image_file_len,
            index_slots,
            index_offset: RECOVERY_IMAGE_INDEX_OFFSET_V1,
            index_len,
            region_table_offset,
            region_table_len: metadata_len,
        };
        let header_page = header.encode().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid V2 recovery image header",
            )
        })?;
        let clean_state = prepare_next_state_v2(
            self.current_state,
            RecoveryState::Clean,
            StateBindingV2::from_data(data, Some(header.image_binding())),
        )
        .map_err(|_| io::Error::other("V2 CLEAN generation cannot advance"))?;
        if clean_state.record.generation != image_generation {
            return Err(io::Error::other(
                "V2 image and state generations were not frozen together",
            ));
        }

        let data_file = self.data_file.as_ref().ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotConnected, "V2 data file is not open")
        })?;
        data_file.sync(SyncPoint::V2WarmData, SyncMode::Data)?;

        let temporary = recovery_temporary_path(&self.files.image);
        self.file_system.remove_file(&temporary)?;
        let persisted = (|| {
            let image = self.file_system.create_new(&temporary)?;
            image.set_len(image_file_len)?;
            write_all_at(&image, WritePoint::V2RecoveryImageHeader, &header_page, 0)?;
            let mut writer = PositionedIoWriter::new(
                &image,
                WritePoint::V2RecoveryImageIndex,
                RECOVERY_IMAGE_INDEX_OFFSET_V1,
            );
            let written = guarded_index_result(
                &view.health,
                storage.write_warm_image(&mut writer, index_image_binding(header)),
            )?;
            if written.bytes_written != index_len
                || written.physical_stats != physical_stats
                || writer.offset() != region_table_offset
            {
                return Err(io::Error::other(
                    "V2 index writer produced inconsistent length or physical statistics",
                ));
            }
            write_all_at(
                &image,
                WritePoint::V2RecoveryImageMetadata,
                &metadata_bytes,
                region_table_offset,
            )?;
            image.sync(SyncPoint::V2RecoveryImage, SyncMode::Data)?;
            view.health.require_healthy()?;
            self.file_system.rename(&temporary, &self.files.image)?;
            self.file_system.sync_parent(&self.files.image)?;
            view.health.require_healthy()
        })();
        if persisted.is_err() {
            let _ = self.file_system.remove_file(&temporary);
        }
        persisted?;
        self.prepared_clean = Some((clean_state.slot, clean_state.record));
        Ok(PreparedFileRegionCleanV2 {
            state: clean_state,
            health: view.health.clone(),
        })
    }

    fn publish_clean(&mut self, prepared: Self::PreparedClean) -> io::Result<()> {
        prepared.health.require_healthy()?;
        if self.prepared_clean.take() != Some((prepared.state.slot, prepared.state.record)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "V2 CLEAN token does not belong to this backend session",
            ));
        }
        let data = self.data_superblock()?;
        let expected = prepare_next_state_v2(
            self.current_state,
            RecoveryState::Clean,
            prepared.state.record.binding,
        )
        .map_err(|_| io::Error::other("V2 CLEAN generation cannot advance"))?;
        if expected != prepared.state
            || !prepared.state.record.binding.matches_data(data)
            || prepared.state.record.binding.image.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "V2 CLEAN token no longer matches current data/state authority",
            ));
        }
        let state = self.state_file()?;
        prepared.health.require_healthy()?;
        write_state_page(state, &prepared.state.page, prepared.state.offset())?;
        state.sync(SyncPoint::V2CleanState, SyncMode::Data)?;
        self.current_state = Some(SelectedStateV2 {
            slot: prepared.state.slot,
            record: prepared.state.record,
        });
        Ok(())
    }

    fn release_exclusive(&mut self) -> io::Result<()> {
        if !self.locked {
            return Ok(());
        }
        let state_result = self
            .state_file
            .as_ref()
            .map(IoBackend::unlock)
            .unwrap_or(Ok(()));
        let data_result = self
            .data_file
            .as_ref()
            .map(IoBackend::unlock)
            .unwrap_or(Ok(()));
        self.locked = false;
        self.prepared_clean = None;
        self.materialize_active_headers = false;
        self.state_file.take();
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
///   CLEAN last.
pub(crate) struct RegionStoreV2<B: RegionV2Backend> {
    backend: B,
    runtime: Option<B::Runtime>,
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
            let (runtime, startup) = match plan {
                RecoveryPlan::Fresh => (
                    backend.anonymous_runtime(config)?,
                    RegionV2Startup::FreshEmpty,
                ),
                RecoveryPlan::Running => (
                    backend.anonymous_runtime(config)?,
                    RegionV2Startup::DirtyEmpty,
                ),
                RecoveryPlan::Clean(clean) => match backend.map_clean_runtime(clean, config)? {
                    Some(runtime) => (runtime, RegionV2Startup::CleanMapped),
                    None => (
                        backend.anonymous_runtime(config)?,
                        RegionV2Startup::CleanRejectedEmpty,
                    ),
                },
            };

            // RUNNING is the no-reuse barrier for the selected clean image.
            // It is written only after index setup is known to have succeeded,
            // but before the caller can observe or mutate the recovered view.
            backend.publish_running()?;
            let runtime = backend.start_runtime(runtime)?;
            Ok((runtime, startup))
        })();

        match opened {
            Ok((runtime, startup)) => Ok(Self {
                backend,
                runtime: Some(runtime),
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

        // Taking ownership first makes every subsequent warm-close artifact
        // immutable to the safe API. A prepared token exists only after data,
        // image, rename, and directory barriers all succeed.
        let result = match self.runtime.take() {
            Some(runtime) if warm => self
                .backend
                .freeze_warm(runtime)
                .and_then(|frozen| self.backend.persist_frozen(&frozen))
                .and_then(|prepared| self.backend.publish_clean(prepared)),
            Some(runtime) => self.backend.stop_fast(runtime),
            None => Err(closed_error()),
        };

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

fn inspect_or_format_data<D, S>(
    file: &D,
    state: &S,
    format_data: DataSuperblockV2,
) -> io::Result<(DataSuperblockV2, bool)>
where
    D: IoBackend,
    S: IoBackend,
{
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

    format_empty_data(file, state, format_data)?;
    Ok((format_data, true))
}

/// Invalidates every old recovery authority before discarding Region bytes.
/// Truncating the cold data extent prevents a later record-version domain from
/// ever matching stale bytes, without scanning the file or its old records.
fn format_empty_data<D, S>(file: &D, state: &S, format_data: DataSuperblockV2) -> io::Result<()>
where
    D: IoBackend,
    S: IoBackend,
{
    let encoded = format_data
        .encode()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid V2 data format"))?;
    state.set_len(0)?;
    state.sync(SyncPoint::V2StateReset, SyncMode::Data)?;
    file.set_len(0)?;
    file.sync(SyncPoint::FormatTruncate, SyncMode::All)?;
    file.set_len(format_data.geometry.data_file_len)?;
    write_all_at(file, WritePoint::Superblock, &encoded, 0)?;
    file.sync(SyncPoint::FormatClean, SyncMode::All)?;
    Ok(())
}

fn read_state_pages<B>(file: &B) -> io::Result<[[u8; RECOVERY_PAGE_SIZE]; STATE_SLOT_COUNT]>
where
    B: IoBackend,
{
    let mut pages = [[0_u8; RECOVERY_PAGE_SIZE]; STATE_SLOT_COUNT];
    for (slot, page) in pages.iter_mut().enumerate() {
        let mut filled = 0;
        while filled < page.len() {
            let offset = (slot * RECOVERY_PAGE_SIZE + filled) as u64;
            match file.read_at(&mut page[filled..], offset) {
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

fn write_state_page<B>(file: &B, page: &[u8; RECOVERY_PAGE_SIZE], offset: u64) -> io::Result<()>
where
    B: IoBackend,
{
    write_all_at(file, WritePoint::V2State, page, offset)
}

fn maximum_region_metadata_len(region_count: u32) -> io::Result<u64> {
    fn pages_for(count: u64, per_page: u64) -> io::Result<u64> {
        count
            .checked_add(per_page - 1)
            .map(|rounded| rounded / per_page)
            .ok_or_else(|| io::Error::other("V2 Region metadata page count overflow"))
    }

    let region_pages = pages_for(
        u64::from(region_count),
        REGION_METADATA_V1_REGIONS_PER_PAGE as u64,
    )?;
    let shard_pages = pages_for(
        MAX_INDEX_SHARDS as u64,
        REGION_METADATA_V1_SHARDS_PER_PAGE as u64,
    )?;
    1_u64
        .checked_add(region_pages)
        .and_then(|pages| pages.checked_add(shard_pages))
        .and_then(|pages| pages.checked_mul(REGION_METADATA_V1_PAGE_SIZE as u64))
        .ok_or_else(|| io::Error::other("V2 Region metadata length overflow"))
}

fn empty_shard_metadata(ranges: &[IndexShardRangeV1]) -> io::Result<Box<[ShardMetadataRecordV1]>> {
    let mut stats = Vec::new();
    stats.try_reserve_exact(ranges.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate V2 index shard statistics",
        )
    })?;
    stats.resize(ranges.len(), IndexPhysicalStats::default());
    shard_metadata_from_stats(ranges, &stats)
}

fn index_shard_metadata(
    index: &ShardedIndexStorage,
    health: &RegionV2HealthLatch,
) -> io::Result<Box<[ShardMetadataRecordV1]>> {
    let stats = guarded_index_result(health, index.shard_stats())?;
    shard_metadata_from_stats(index.shard_ranges(), &stats)
}

fn shard_metadata_from_stats(
    ranges: &[IndexShardRangeV1],
    stats: &[IndexPhysicalStats],
) -> io::Result<Box<[ShardMetadataRecordV1]>> {
    if ranges.len() != stats.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "V2 index shard ranges and statistics disagree",
        ));
    }
    let mut shards = Vec::new();
    shards.try_reserve_exact(ranges.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate V2 index shard directory",
        )
    })?;
    for (range, stats) in ranges.iter().zip(stats) {
        shards.push(ShardMetadataRecordV1 {
            shard_id: u32::try_from(range.shard_id).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "V2 shard id is too large")
            })?,
            first_index_page: u64::try_from(range.first_page).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "V2 shard page offset is too large",
                )
            })?,
            index_page_count: u64::try_from(range.page_count).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "V2 shard page count is too large",
                )
            })?,
            first_slot: u64::try_from(range.first_slot).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "V2 shard slot offset is too large",
                )
            })?,
            slot_count: u64::try_from(range.slot_count).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "V2 shard slot count is too large",
                )
            })?,
            physical_value_slots: stats.value,
            physical_deleted_slots: stats.deleted,
            physical_masked_slots: stats.masked,
        });
    }
    Ok(shards.into_boxed_slice())
}

fn metadata_shard_stats(metadata: &RegionMetadataV1) -> io::Result<Box<[IndexPhysicalStats]>> {
    let mut stats = Vec::new();
    stats
        .try_reserve_exact(metadata.shards.len())
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate V2 index shard statistics",
            )
        })?;
    for shard in &metadata.shards {
        stats.push(IndexPhysicalStats {
            value: shard.physical_value_slots,
            deleted: shard.physical_deleted_slots,
            masked: shard.physical_masked_slots,
        });
    }
    Ok(stats.into_boxed_slice())
}

fn metadata_shard_stats_match(metadata: &RegionMetadataV1, stats: &[IndexPhysicalStats]) -> bool {
    metadata.shards.len() == stats.len()
        && metadata.shards.iter().zip(stats).all(|(metadata, actual)| {
            metadata.physical_value_slots == actual.value
                && metadata.physical_deleted_slots == actual.deleted
                && metadata.physical_masked_slots == actual.masked
        })
}

/// Materializes the first page of every empty append lane after RUNNING is
/// durable and before the runtime can admit requests.
///
/// This is deliberately only a positioned write boundary. The normal data
/// durability policy will cover these pages later; startup must not add one
/// sync per lane. Clean-recovered lanes already have authoritative headers and
/// never call this helper.
fn materialize_empty_active_region_headers<B>(
    data_file: &B,
    data: DataSuperblockV2,
    manager: &RegionManagerAuthorityV2,
) -> io::Result<()>
where
    B: IoBackend,
{
    // Snapshot under the manager lock, then release it before the first write.
    // This keeps startup on the same lock contract as steady-state rotation.
    for (region_offset, header) in manager.empty_active_headers(data)? {
        write_all_at(
            data_file,
            WritePoint::RegionHeader,
            &header.encode(),
            region_offset,
        )?;
    }
    Ok(())
}

fn snapshot_empty_active_region_headers(
    data: DataSuperblockV2,
    manager: &RegionManagerV2,
) -> io::Result<Vec<(u64, RegionHeader)>> {
    if manager.region_size() != data.geometry.region_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "V2 runtime Region size does not match the data geometry",
        ));
    }

    let mut headers = Vec::new();
    headers
        .try_reserve_exact(manager.active_regions().len())
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate V2 Active Region header snapshot",
            )
        })?;
    for &region_id in manager.active_regions() {
        let region_index = usize::try_from(region_id).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 Active Region id is too large",
            )
        })?;
        let region = manager.regions().get(region_index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 Active Region is outside the data geometry",
            )
        })?;
        if region.region_id != region_id
            || region.state != RegionMetadataStateV1::Active
            || region.durable_used != REGION_HEADER_SIZE as u64
            || region.reserved_used != REGION_HEADER_SIZE as u64
            || region.max_seqno != 0
            || region.physical_record_count != 0
            || region.logical.live_record_count != 0
            || region.logical.live_record_bytes != 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 anonymous Active Region is not empty",
            ));
        }

        let region_offset = u64::from(region_id)
            .checked_mul(data.geometry.region_size)
            .and_then(|offset| DATA_REGION_AREA_OFFSET_V2.checked_add(offset))
            .ok_or_else(|| io::Error::other("V2 Active Region offset overflow"))?;
        let region_end = region_offset
            .checked_add(REGION_HEADER_SIZE as u64)
            .ok_or_else(|| io::Error::other("V2 Active Region header end overflow"))?;
        if region_end > data.geometry.data_file_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "V2 Active Region header exceeds the data file",
            ));
        }

        let header = RegionHeader {
            region_id,
            incarnation: region.incarnation,
            state: RegionState::Active,
            created_seqno: region.created_seqno,
            used: region.durable_used,
        };
        headers.push((region_offset, header));
    }
    Ok(headers)
}

fn empty_region_metadata(
    data: DataSuperblockV2,
    index_slots: usize,
) -> io::Result<RegionMetadataV1> {
    let shard_ranges = canonical_index_shard_ranges(index_slots).map_err(index_storage_io_error)?;
    let index_slots = u64::try_from(index_slots)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "V2 index is too large"))?;
    let index_len = recovery_image_index_len_v1(index_slots)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid V2 index length"))?;
    let index_page_count = index_len / RECOVERY_PAGE_SIZE as u64;

    let region_count = data.geometry.region_count as usize;
    let mut regions = Vec::new();
    regions.try_reserve_exact(region_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate V2 Region table",
        )
    })?;
    for region_id in 0..data.geometry.region_count {
        let active = region_id == 0;
        regions.push(RegionMetadataRecordV1 {
            region_id,
            incarnation: u32::from(active),
            state: if active {
                RegionMetadataStateV1::Active
            } else {
                RegionMetadataStateV1::Free
            },
            queue_ordinal: if active { 0 } else { region_id - 1 },
            created_seqno: u64::from(active),
            durable_used_offset: RECOVERY_PAGE_SIZE as u64,
            max_seqno: 0,
            physical_record_count: 0,
            live_record_count: 0,
            live_record_bytes: 0,
        });
    }
    let shards = empty_shard_metadata(&shard_ranges)?;
    let metadata = RegionMetadataV1 {
        root: RegionMetadataRootV1 {
            cache_uuid: data.cache_uuid,
            data_identity: data.data_identity,
            data_superblock_generation: data.generation,
            image_identity: data.data_identity,
            image_generation: 1,
            config_fingerprint: data.config_fingerprint,
            index_slots,
            index_page_count,
            region_size: data.geometry.region_size,
            region_count: data.geometry.region_count,
            shard_count: u32::try_from(shard_ranges.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many V2 index shards")
            })?,
            append_lane_count: 1,
            cache_epoch: 1,
            clear_floor_seqno: 1,
            max_seqno: 1,
            physical_value_slots: 0,
            physical_deleted_slots: 0,
            physical_masked_slots: 0,
            live_record_count: 0,
            live_record_bytes: 0,
            write_budget_window: 0,
            write_budget_used_bytes: 0,
            free_region_count: data.geometry.region_count - 1,
            active_region_count: 1,
            sealed_region_count: 0,
        },
        regions: regions.into_boxed_slice(),
        shards,
    };
    metadata.validate().map_err(region_metadata_io_error)?;
    Ok(metadata)
}

fn next_state_generation(current: Option<SelectedStateV2>) -> io::Result<u64> {
    current
        .map_or(Some(1), |selected| {
            selected.record.generation.checked_add(1)
        })
        .ok_or_else(|| io::Error::other("V2 state generation is exhausted"))
}

fn derive_image_identity(data_identity: PersistentId, generation: u64) -> PersistentId {
    let bytes = data_identity.to_bytes();
    let left = u64::from_le_bytes(bytes[..8].try_into().expect("fixed identity half"));
    let right = u64::from_le_bytes(bytes[8..].try_into().expect("fixed identity half"));
    let mut image = [0_u8; 16];
    image[..8].copy_from_slice(&generation.to_le_bytes());
    image[8..].copy_from_slice(&(left ^ right ^ 0x9e37_79b9_7f4a_7c15).to_le_bytes());
    PersistentId::from_bytes(image).expect("non-zero generation makes image identity non-zero")
}

fn index_image_binding(header: RecoveryImageHeaderV1) -> IndexImageBindingV1 {
    let bytes = header.image_identity.to_bytes();
    let left = u64::from_le_bytes(bytes[..8].try_into().expect("fixed identity half"));
    let right = u64::from_le_bytes(bytes[8..].try_into().expect("fixed identity half"));
    let mixed = left ^ right.rotate_left(17);
    IndexImageBindingV1 {
        generation: header.image_generation,
        image_tag: if mixed == 0 {
            0xa076_1d64_78bd_642f
        } else {
            mixed
        },
    }
}

fn recovery_temporary_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_os_string();
    path.push(".next");
    PathBuf::from(path)
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

struct PositionedIoWriter<'a, B: IoBackend + ?Sized> {
    backend: &'a B,
    point: WritePoint,
    offset: u64,
}

impl<'a, B: IoBackend + ?Sized> PositionedIoWriter<'a, B> {
    const fn new(backend: &'a B, point: WritePoint, offset: u64) -> Self {
        Self {
            backend,
            point,
            offset,
        }
    }

    const fn offset(&self) -> u64 {
        self.offset
    }
}

impl<B: IoBackend + ?Sized> Write for PositionedIoWriter<'_, B> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.backend.write_at(self.point, buffer, self.offset)?;
        self.offset = self
            .offset
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("V2 image writer offset overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn region_metadata_io_error(error: RegionMetadataV1Error) -> io::Error {
    match error {
        RegionMetadataV1Error::Allocation => io::Error::new(io::ErrorKind::OutOfMemory, error),
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn region_mutation_io_error(error: RegionMutationErrorV2) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("RegionStore V2 authority mutation failed: {error:?}"),
    )
}

fn index_storage_io_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn guarded_index_result<T>(
    health: &RegionV2HealthLatch,
    result: Result<T, IndexStorageError>,
) -> io::Result<T> {
    result.map_err(|error| {
        health.enter_miss_only();
        index_storage_io_error(error)
    })
}

fn closed_error() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "RegionStore V2 is closed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::{IndexEntry, PackedLocation};
    use crate::index_storage::IndexSlotV1;
    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};
    use crate::recovery_v2::{DataGeometryV2, PersistentId};
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

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
        type Runtime = usize;
        type CleanImage = bool;
        type FrozenView = usize;
        type PreparedClean = bool;

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

        fn anonymous_runtime(&mut self, config: RegionV2Config) -> io::Result<Self::Runtime> {
            self.record("anonymous")?;
            Ok(config.index_slots)
        }

        fn map_clean_runtime(
            &mut self,
            clean: Self::CleanImage,
            config: RegionV2Config,
        ) -> io::Result<Option<Self::Runtime>> {
            self.record("map")?;
            Ok(clean.then_some(config.index_slots))
        }

        fn publish_running(&mut self) -> io::Result<()> {
            self.record("running")
        }

        fn start_runtime(&mut self, runtime: Self::Runtime) -> io::Result<Self::Runtime> {
            self.record("start")?;
            Ok(runtime)
        }

        fn stop_fast(&mut self, _runtime: Self::Runtime) -> io::Result<()> {
            self.record("stop-fast")
        }

        fn freeze_warm(&mut self, runtime: Self::Runtime) -> io::Result<Self::FrozenView> {
            self.record("freeze")?;
            Ok(runtime)
        }

        fn persist_frozen(&mut self, _view: &Self::FrozenView) -> io::Result<Self::PreparedClean> {
            self.record("persist")?;
            Ok(true)
        }

        fn publish_clean(&mut self, _prepared: Self::PreparedClean) -> io::Result<()> {
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
        for index_slots in [0, 1, 7, MAX_INDEX_SLOTS + 1] {
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
                vec!["lock", "inspect", "anonymous", "running", "start"],
            ),
            (
                Plan::Running,
                RegionV2Startup::DirtyEmpty,
                vec!["lock", "inspect", "anonymous", "running", "start"],
            ),
            (
                Plan::Clean,
                RegionV2Startup::CleanMapped,
                vec!["lock", "inspect", "map", "running", "start"],
            ),
            (
                Plan::RejectedClean,
                RegionV2Startup::CleanRejectedEmpty,
                vec!["lock", "inspect", "map", "anonymous", "running", "start"],
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
                "start",
                "stop-fast",
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
                "lock", "inspect", "map", "running", "start", "freeze", "persist", "clean",
                "unlock"
            ]
        );
    }

    #[test]
    fn failed_warm_image_keeps_running_and_still_unlocks() {
        let (backend, events) = backend(Plan::Running, Some("persist"));
        let mut store = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend).unwrap();
        assert!(store.close_warm().is_err());
        assert_eq!(
            *events.borrow(),
            vec![
                "lock",
                "inspect",
                "anonymous",
                "running",
                "start",
                "freeze",
                "persist",
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

    #[test]
    fn failed_runtime_start_happens_after_running_and_releases_lock() {
        let (backend, events) = backend(Plan::Fresh, Some("start"));
        let opened = RegionStoreV2::open_v2(RegionV2Config { index_slots: 8 }, backend);
        assert!(opened.is_err());
        assert_eq!(
            *events.borrow(),
            vec!["lock", "inspect", "anonymous", "running", "start", "unlock"]
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

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FileSystemFault {
        Rename,
        SyncParent,
    }

    #[derive(Clone, Default)]
    struct FileSystemFaultHandle {
        armed: Arc<Mutex<Option<FileSystemFault>>>,
    }

    impl FileSystemFaultHandle {
        fn arm(&self, fault: FileSystemFault) {
            *self.armed.lock().unwrap() = Some(fault);
        }

        fn check(&self, fault: FileSystemFault) -> io::Result<()> {
            let mut armed = self.armed.lock().unwrap();
            if *armed == Some(fault) {
                *armed = None;
                Err(io::Error::from_raw_os_error(5))
            } else {
                Ok(())
            }
        }
    }

    #[derive(Clone)]
    struct FaultRegionV2FileSystem {
        io: FaultHandle,
        file_system: FileSystemFaultHandle,
    }

    impl FaultRegionV2FileSystem {
        fn new() -> (Self, FaultHandle, FileSystemFaultHandle) {
            let io = FaultHandle::default();
            let file_system = FileSystemFaultHandle::default();
            (
                Self {
                    io: io.clone(),
                    file_system: file_system.clone(),
                },
                io,
                file_system,
            )
        }
    }

    impl RegionV2FileSystem for FaultRegionV2FileSystem {
        type File = FaultBackend;

        fn open(&self, path: &Path, create: bool) -> io::Result<Self::File> {
            if create {
                FaultBackend::open_with_handle(path, self.io.clone())
            } else {
                FaultBackend::open_existing_with_handle(path, self.io.clone())
            }
        }

        fn create_new(&self, path: &Path) -> io::Result<Self::File> {
            FaultBackend::create_new_buffered_with_handle(path, self.io.clone())
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            match std::fs::remove_file(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        }

        fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
            self.file_system.check(FileSystemFault::Rename)?;
            std::fs::rename(source, destination)
        }

        fn sync_parent(&self, path: &Path) -> io::Result<()> {
            self.file_system.check(FileSystemFault::SyncParent)?;
            SystemRegionV2FileSystem.sync_parent(path)
        }
    }

    fn persistent_id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn test_data_superblock_with_regions(region_count: u32) -> DataSuperblockV2 {
        let region_size = 2 * RECOVERY_PAGE_SIZE as u64;
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

    fn test_data_superblock() -> DataSuperblockV2 {
        test_data_superblock_with_regions(2)
    }

    #[test]
    fn index_mutation_and_region_accounting_share_one_failure_fence() {
        let data = test_data_superblock();
        let runtime = FileRegionRuntimeV2::install(
            ShardedIndexStorage::anonymous(8).unwrap(),
            empty_region_metadata(data, 8).unwrap(),
        )
        .unwrap();

        let append = |runtime: &FileRegionRuntimeV2| {
            let mut manager = runtime.manager.lock().unwrap();
            let reservation = manager.reserve_append(0, 64).unwrap();
            manager.stage_reservation(reservation).unwrap();
            let span = manager.seal_write_span(0).unwrap();
            manager.complete_write_span(span).unwrap();
            reservation
        };
        let entry_for =
            |reservation: crate::region_manager_v2::RegionAppendReservationV2| IndexEntry {
                location: PackedLocation::new(
                    reservation.region_id,
                    reservation.offset,
                    reservation.record_bytes,
                    false,
                )
                .unwrap(),
                seqno: reservation.seqno,
                namespace_id: 0,
                flags: 0,
            };

        let first = entry_for(append(&runtime));
        assert!(matches!(
            runtime.upsert_entry(7, first).unwrap(),
            IndexUpsertV2::Applied { .. }
        ));
        assert_eq!(
            runtime
                .manager
                .lock()
                .unwrap()
                .logical_accounting()
                .unwrap()
                .live_record_bytes,
            64
        );

        let remove_seqno = runtime.manager.lock().unwrap().allocate_seqno().unwrap();
        assert!(matches!(
            runtime.mask_hash(7, remove_seqno).unwrap(),
            IndexMaskV2::Applied { .. }
        ));
        runtime.normalize_mask(7, remove_seqno).unwrap();
        assert_eq!(
            runtime
                .manager
                .lock()
                .unwrap()
                .logical_accounting()
                .unwrap()
                .live_record_count,
            0
        );

        let second = entry_for(append(&runtime));
        runtime.upsert_entry(7, second).unwrap();
        runtime
            .manager
            .lock()
            .unwrap()
            .apply_index_transition(IndexTransitionV2 {
                global_slot: 0,
                previous: crate::index_storage::IndexSlotStateV1::Value {
                    hash: 7,
                    entry: second,
                },
                installed: crate::index_storage::IndexSlotStateV1::Deleted,
            })
            .unwrap();

        let error = runtime.remove_entry(7, second).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!runtime.health.is_healthy());
        assert_eq!(runtime.lookup_snapshot(7).unwrap(), IndexLookupV2::Miss);
    }

    fn assert_active_header_startup_boundary(events: &[FaultEvent]) {
        let running_sync = events
            .iter()
            .rposition(|event| *event == FaultEvent::Sync(SyncPoint::V2RunningState))
            .expect("startup must make RUNNING durable");
        let header_writes = events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| {
                (*event == FaultEvent::Write(WritePoint::RegionHeader)).then_some(index)
            })
            .collect::<Vec<_>>();
        assert_eq!(header_writes.len(), 1, "one Active lane needs one header");
        let header_write = header_writes[0];
        assert!(
            running_sync < header_write,
            "Active header must follow the durable RUNNING barrier"
        );
        assert!(
            events[header_write + 1..]
                .iter()
                .all(|event| !matches!(event, FaultEvent::Sync(_))),
            "Active header materialization must not add a startup sync"
        );
    }

    #[test]
    fn fresh_and_dirty_empty_materialize_active_header_after_running_without_sync() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();

        let (fresh_file_system, fresh_io, _) = FaultRegionV2FileSystem::new();
        let mut fresh = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new_with_file_system(
                directory.files.clone(),
                data,
                fresh_file_system,
            ),
        )
        .unwrap();
        assert_eq!(fresh.startup(), RegionV2Startup::FreshEmpty);
        assert_active_header_startup_boundary(&fresh_io.events());

        let mut encoded = [0_u8; REGION_HEADER_SIZE];
        File::open(&directory.files.data)
            .unwrap()
            .read_exact_at(&mut encoded, DATA_REGION_AREA_OFFSET_V2)
            .unwrap();
        assert_eq!(
            RegionHeader::decode(&encoded),
            Some(RegionHeader {
                region_id: 0,
                incarnation: 1,
                state: RegionState::Active,
                created_seqno: 1,
                used: REGION_HEADER_SIZE as u64,
            })
        );
        fresh.close_fast().unwrap();

        let (dirty_file_system, dirty_io, _) = FaultRegionV2FileSystem::new();
        let mut dirty = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new_with_file_system(
                directory.files.clone(),
                data,
                dirty_file_system,
            ),
        )
        .unwrap();
        assert_eq!(dirty.startup(), RegionV2Startup::DirtyEmpty);
        assert_active_header_startup_boundary(&dirty_io.events());
        dirty.close_fast().unwrap();
    }

    #[test]
    fn active_header_failure_aborts_open_unlocks_and_leaves_running() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let (file_system, faults, _) = FaultRegionV2FileSystem::new();
        faults.arm(
            FaultEvent::Write(WritePoint::RegionHeader),
            1,
            FaultAction::Torn {
                bytes: 128,
                raw_os_error: 5,
            },
        );

        let error = match RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new_with_file_system(directory.files.clone(), data, file_system),
        ) {
            Ok(_) => panic!("torn Active header must abort open"),
            Err(error) => error,
        };
        assert_eq!(error.raw_os_error(), Some(5));

        let events = faults.events();
        let running_sync = events
            .iter()
            .position(|event| *event == FaultEvent::Sync(SyncPoint::V2RunningState))
            .unwrap();
        let header_write = events
            .iter()
            .position(|event| *event == FaultEvent::Write(WritePoint::RegionHeader))
            .unwrap();
        let first_unlock = events
            .iter()
            .position(|event| *event == FaultEvent::Unlock)
            .unwrap();
        assert!(running_sync < header_write && header_write < first_unlock);

        let state = std::fs::read(&directory.files.state).unwrap();
        let selected = latest_state_v2([
            &state[..RECOVERY_PAGE_SIZE],
            &state[RECOVERY_PAGE_SIZE..STATE_FILE_SIZE],
        ])
        .unwrap()
        .unwrap();
        assert_eq!(selected.record.state, RecoveryState::Running);

        let mut reopened = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .expect("failed open must release both V2 file locks");
        assert_eq!(reopened.startup(), RegionV2Startup::DirtyEmpty);
        reopened.close_fast().unwrap();
    }

    #[test]
    fn clean_recovery_does_not_rewrite_active_header() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        first.close_warm().unwrap();

        let (file_system, faults, _) = FaultRegionV2FileSystem::new();
        faults.arm(
            FaultEvent::Write(WritePoint::RegionHeader),
            1,
            FaultAction::ErrorAlways(5),
        );
        let mut recovered = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new_with_file_system(directory.files.clone(), data, file_system),
        )
        .expect("clean recovery must preserve the existing Active header");
        assert_eq!(recovered.startup(), RegionV2Startup::CleanMapped);
        assert!(
            faults
                .events()
                .iter()
                .all(|event| *event != FaultEvent::Write(WritePoint::RegionHeader))
        );
        recovered.close_fast().unwrap();
    }

    #[test]
    fn transient_index_mask_cannot_publish_a_warm_image() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 130 };
        let data = test_data_superblock();
        let mut fresh = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(fresh.startup(), RegionV2Startup::FreshEmpty);
        fresh
            .runtime
            .as_mut()
            .unwrap()
            .index
            .storage()
            .write_slot(
                0,
                IndexSlotV1::from_state(crate::index_storage::IndexSlotStateV1::Masked {
                    hash: 11,
                    seqno: 33,
                }),
            )
            .unwrap();
        let error = fresh.close_warm().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(!directory.files.image.exists());

        let mut dirty = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(dirty.startup(), RegionV2Startup::DirtyEmpty);
        assert_eq!(
            dirty.runtime.as_ref().unwrap().lookup_snapshot(11).unwrap(),
            IndexLookupV2::Miss
        );
        dirty.close_fast().unwrap();
    }

    #[test]
    fn dirty_cold_start_discards_stale_region_bytes_without_scanning() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        first.close_fast().unwrap();

        let stale_offset = 2 * RECOVERY_PAGE_SIZE as u64;
        let file = File::options()
            .read(true)
            .write(true)
            .open(&directory.files.data)
            .unwrap();
        file.write_all_at(b"stale-record", stale_offset).unwrap();
        file.sync_data().unwrap();

        let mut cold = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(cold.startup(), RegionV2Startup::DirtyEmpty);
        let mut observed = [0xff_u8; 12];
        File::open(&directory.files.data)
            .unwrap()
            .read_exact_at(&mut observed, stale_offset)
            .unwrap();
        assert_eq!(observed, [0; 12]);
        cold.close_fast().unwrap();
    }

    #[test]
    fn concrete_recovery_profile_rejects_a_single_region_before_file_creation() {
        let directory = TestDirectory::new();
        let opened = RegionStoreV2::open_v2(
            RegionV2Config { index_slots: 8 },
            FileRegionV2Backend::new(
                directory.files.clone(),
                test_data_superblock_with_regions(1),
            ),
        );
        assert!(matches!(
            opened,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(!directory.files.data.exists());
        assert!(!directory.files.state.exists());
    }

    #[test]
    fn complete_warm_image_maps_without_rebuilding_index_slots() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 134 };
        let data = test_data_superblock_with_regions(2);
        let value = IndexSlotV1::DELETED;

        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        let runtime = first.runtime.as_mut().unwrap();
        assert_eq!(runtime.index.storage().shard_count(), 2);
        runtime.index.storage().write_slot(133, value).unwrap();
        first.close_warm().unwrap();
        assert!(directory.files.image.exists());

        let mut recovered = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(recovered.startup(), RegionV2Startup::CleanMapped);
        let recovered_runtime = recovered.runtime.as_ref().unwrap();
        assert_eq!(recovered_runtime.index.storage().shard_count(), 2);
        assert_eq!(
            recovered_runtime.index.storage().read_slot(133).unwrap(),
            value
        );
        recovered.close_fast().unwrap();
    }

    #[test]
    fn corrupt_region_metadata_rejects_the_complete_clean_image() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 130 };
        let data = test_data_superblock_with_regions(2);
        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        first.close_warm().unwrap();

        let image = File::options()
            .read(true)
            .write(true)
            .open(&directory.files.image)
            .unwrap();
        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        image.read_exact_at(&mut page, 0).unwrap();
        let header = RecoveryImageHeaderV1::decode(&page).unwrap();
        image
            .write_all_at(&[0x5a], header.region_table_offset + 100)
            .unwrap();
        image.sync_data().unwrap();

        let mut rejected = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(rejected.startup(), RegionV2Startup::DirtyEmpty);
        assert_eq!(
            rejected
                .runtime
                .as_ref()
                .unwrap()
                .lookup_snapshot(0)
                .unwrap(),
            IndexLookupV2::Miss
        );
        rejected.close_fast().unwrap();
    }

    #[test]
    fn one_corrupt_lazy_index_page_rejects_all_pages() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 134 };
        let data = test_data_superblock_with_regions(2);
        let mut first = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        first.close_warm().unwrap();

        let image = File::options()
            .read(true)
            .write(true)
            .open(&directory.files.image)
            .unwrap();
        image
            .write_all_at(
                &[0x5a],
                RECOVERY_IMAGE_INDEX_OFFSET_V1 + RECOVERY_PAGE_SIZE as u64 + 100,
            )
            .unwrap();
        image.sync_data().unwrap();

        let mut recovered = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(recovered.startup(), RegionV2Startup::CleanMapped);
        let runtime = recovered.runtime.as_ref().unwrap();
        assert_eq!(runtime.lookup_snapshot(0).unwrap(), IndexLookupV2::Miss);
        assert!(runtime.health.is_healthy());
        assert!(
            !runtime
                .revalidate_exact(
                    1,
                    IndexEntry {
                        location: PackedLocation::new(0, 4096, 32, false).unwrap(),
                        seqno: 1,
                        namespace_id: 0,
                        flags: 0,
                    },
                )
                .unwrap()
        );
        assert!(!runtime.health.is_healthy());
        assert_eq!(runtime.lookup_snapshot(1).unwrap(), IndexLookupV2::Miss);
        assert_eq!(runtime.lookup_snapshot(0).unwrap(), IndexLookupV2::Miss);
        assert!(recovered.close_warm().is_err());

        let mut cold = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(cold.startup(), RegionV2Startup::DirtyEmpty);
        assert_eq!(
            cold.runtime.as_ref().unwrap().lookup_snapshot(1).unwrap(),
            IndexLookupV2::Miss
        );
        cold.close_fast().unwrap();
    }

    #[test]
    fn every_prepublication_failure_leaves_no_selectable_clean_state() {
        let cases = [
            (
                Some((
                    FaultEvent::Sync(SyncPoint::V2WarmData),
                    FaultAction::Error(5),
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::V2RecoveryImageHeader),
                    FaultAction::Torn {
                        bytes: 128,
                        raw_os_error: 5,
                    },
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::V2RecoveryImageIndex),
                    FaultAction::Error(5),
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::V2RecoveryImageMetadata),
                    FaultAction::Torn {
                        bytes: 128,
                        raw_os_error: 5,
                    },
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Sync(SyncPoint::V2RecoveryImage),
                    FaultAction::Error(5),
                )),
                None,
            ),
            (None, Some(FileSystemFault::Rename)),
            (None, Some(FileSystemFault::SyncParent)),
            (
                Some((
                    FaultEvent::Write(WritePoint::V2State),
                    FaultAction::Torn {
                        bytes: 128,
                        raw_os_error: 5,
                    },
                )),
                None,
            ),
        ];

        for (case, (io_fault, file_system_fault)) in cases.into_iter().enumerate() {
            let directory = TestDirectory::new();
            let config = RegionV2Config { index_slots: 8 };
            let data = test_data_superblock_with_regions(2);
            let (file_system, io_faults, file_system_faults) = FaultRegionV2FileSystem::new();
            let backend = FileRegionV2Backend::new_with_file_system(
                directory.files.clone(),
                data,
                file_system,
            );
            let mut store = RegionStoreV2::open_v2(config, backend).unwrap();
            if let Some((event, action)) = io_fault {
                io_faults.arm(event, 1, action);
            }
            if let Some(fault) = file_system_fault {
                file_system_faults.arm(fault);
            }
            assert!(store.close_warm().is_err(), "failure case {case}");

            let mut reopened = RegionStoreV2::open_v2(
                config,
                FileRegionV2Backend::new(directory.files.clone(), data),
            )
            .unwrap();
            assert_eq!(
                reopened.startup(),
                RegionV2Startup::DirtyEmpty,
                "failure case {case}"
            );
            reopened.close_fast().unwrap();
        }
    }

    #[test]
    fn concrete_running_barrier_failures_abort_before_runtime_start() {
        let cases = [
            (
                FaultEvent::Write(WritePoint::V2State),
                1,
                FaultAction::Error(5),
            ),
            (
                FaultEvent::Write(WritePoint::V2State),
                2,
                FaultAction::Torn {
                    bytes: 128,
                    raw_os_error: 5,
                },
            ),
            (
                FaultEvent::Sync(SyncPoint::V2RunningState),
                1,
                FaultAction::Error(5),
            ),
        ];

        for (case, (event, occurrence, action)) in cases.into_iter().enumerate() {
            let directory = TestDirectory::new();
            let config = RegionV2Config { index_slots: 8 };
            let data = test_data_superblock();
            let (file_system, faults, _) = FaultRegionV2FileSystem::new();
            faults.arm(event, occurrence, action);
            let opened = RegionStoreV2::open_v2(
                config,
                FileRegionV2Backend::new_with_file_system(
                    directory.files.clone(),
                    data,
                    file_system,
                ),
            );
            assert!(opened.is_err(), "RUNNING barrier case {case}");

            let mut cold = RegionStoreV2::open_v2(
                config,
                FileRegionV2Backend::new(directory.files.clone(), data),
            )
            .unwrap();
            assert!(matches!(
                cold.startup(),
                RegionV2Startup::FreshEmpty | RegionV2Startup::DirtyEmpty
            ));
            cold.close_fast().unwrap();
        }
    }

    #[test]
    fn final_clean_sync_failure_reopens_as_safe_clean_or_empty() {
        let directory = TestDirectory::new();
        let config = RegionV2Config { index_slots: 8 };
        let data = test_data_superblock();
        let (file_system, faults, _) = FaultRegionV2FileSystem::new();
        let mut store = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new_with_file_system(directory.files.clone(), data, file_system),
        )
        .unwrap();
        faults.arm(
            FaultEvent::Sync(SyncPoint::V2CleanState),
            1,
            FaultAction::Error(5),
        );
        assert!(store.close_warm().is_err());

        let mut reopened = RegionStoreV2::open_v2(
            config,
            FileRegionV2Backend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert!(matches!(
            reopened.startup(),
            RegionV2Startup::CleanMapped | RegionV2Startup::DirtyEmpty
        ));
        assert_eq!(
            reopened
                .runtime
                .as_ref()
                .unwrap()
                .lookup_snapshot(0)
                .unwrap(),
            IndexLookupV2::Miss
        );
        reopened.close_fast().unwrap();
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
    fn recovery_temporary_path_cannot_name_the_data_or_state_file() {
        let directory = TestDirectory::new();
        let marker = b"keep-data";
        let image = directory.root.join("recovery");
        let data_path = directory.root.join("recovery.next");
        std::fs::write(&data_path, marker).unwrap();
        let files = RegionV2Files::new(&data_path, directory.root.join("state"), image);

        let opened = RegionStoreV2::open_v2(
            RegionV2Config { index_slots: 8 },
            FileRegionV2Backend::new(files, test_data_superblock_with_regions(2)),
        );
        assert!(matches!(
            opened,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert_eq!(std::fs::read(data_path).unwrap(), marker);
    }

    #[test]
    fn recovery_sidecars_must_share_one_directory() {
        let directory = TestDirectory::new();
        let other = directory.root.join("other");
        std::fs::create_dir(&other).unwrap();
        let files = RegionV2Files::new(
            directory.root.join("data"),
            other.join("state"),
            directory.root.join("image"),
        );
        let opened = RegionStoreV2::open_v2(
            RegionV2Config { index_slots: 8 },
            FileRegionV2Backend::new(files, test_data_superblock()),
        );
        assert!(matches!(
            opened,
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
        assert!(!directory.root.join("data").exists());
        assert!(!other.join("state").exists());
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
