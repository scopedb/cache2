// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

//! File ownership, recovery, and lifecycle adapter for the Region core.

use std::fs::File;
use std::io::{self, Write};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use crate::index::MAX_INDEX_PARTITIONS;
use crate::index_storage::{
    IndexImageBinding, IndexPartitionRange, IndexPhysicalStats, PartitionedIndexStorage,
    canonical_index_partition_ranges,
};
use crate::io_backend::{
    ControlIoBackend, FileBackend, IoBackend, RuntimeFileSet, SyncMode, SyncPoint, WritePoint,
    read_at_bounded, read_exact_at, write_all_at,
};
use crate::recovery::{
    DataSuperblock, DataSuperblockProbe, PersistentId, RECOVERY_IMAGE_INDEX_OFFSET,
    RECOVERY_PAGE_SIZE, RecoveryImageHeader, RecoveryImageHeaderProbe, RecoveryState,
    STATE_FILE_SIZE, STATE_SLOT_COUNT, SelectedState, StateBinding, StatePageWrite, StateRecord,
    StateSelectionError, clean_image_matches, latest_state, prepare_next_state,
    prepare_running_barrier, recovery_image_index_len,
};
use crate::region_index::RegionIndex;
use crate::region_manager::RegionManager;
use crate::region_metadata::{
    PartitionMetadataRecord, REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE, RegionMetadata, RegionMetadataError, RegionMetadataRecord,
    RegionMetadataRoot, RegionMetadataState,
};
use crate::region_store::{RecoveryPlan, RegionBackend, RegionStore};
use crate::runtime_config::{IoMode, RuntimeConfig};
use crate::snapshot::{CacheSnapshot, DetailedCacheSnapshot};

use super::core::{
    FileRegionCore, RegionAccessState, RegionHealthLatch, RegionManagerAuthority, RegionShard,
    guarded_index_result, index_storage_io_error, region_metadata_io_error,
};
use crate::region_runtime::{HybridValueRead, RegionDataPlane};

/// Shared shard count for compact concrete-backend fixtures.
#[cfg(test)]
const REGION_SHARDS: u32 = 4;

/// Data and recovery sidecars owned by one concrete Region backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionFiles {
    pub(crate) data: PathBuf,
    pub(crate) state: PathBuf,
    pub(crate) image: PathBuf,
}

impl RegionFiles {
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
/// Unique steady-state owner. Workers share only `core`; runtime resources and
/// their join handles are attached here after RUNNING is durable.
pub(crate) struct FileRegionRuntime {
    core: Arc<FileRegionCore>,
    data_plane: Option<RegionDataPlane>,
}

impl Deref for FileRegionRuntime {
    type Target = FileRegionCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}
pub(crate) struct FrozenFileRegionView {
    index: RegionIndex,
    metadata: RegionMetadata,
    health: RegionHealthLatch,
}

pub(crate) struct CleanFileRegionImage {
    file: File,
    header: RecoveryImageHeader,
    metadata: RegionMetadata,
}

pub(crate) struct PreparedFileRegionClean {
    state: StatePageWrite,
    health: RegionHealthLatch,
}

impl FileRegionRuntime {
    /// Installs one complete authority. Recovery metadata is consumed here so
    /// the live runtime cannot retain a stale second copy beside the manager.
    fn install(index: PartitionedIndexStorage, metadata: RegionMetadata) -> io::Result<Self> {
        let physical_stats = index.partition_stats().map_err(index_storage_io_error)?;
        let slot_count = u64::try_from(index.slot_count()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "index capacity is too large")
        })?;
        if metadata.root.index_slots != slot_count
            || metadata.root.partition_count as usize != index.partition_count()
            || !metadata_partition_stats_match(&metadata, &physical_stats)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index and Region metadata do not describe one authority",
            ));
        }
        let manager = RegionManager::from_metadata(metadata).map_err(region_metadata_io_error)?;
        let mut shards = Vec::new();
        shards
            .try_reserve_exact(manager.active_regions().len())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate data shard gates",
                )
            })?;
        shards.resize_with(manager.active_regions().len(), RegionShard::default);
        let mut region_access = Vec::new();
        region_access
            .try_reserve_exact(manager.regions().len())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate Region access state",
                )
            })?;
        for region in manager.regions() {
            region_access.push(RegionAccessState {
                generation: AtomicU64::new(region.created_seqno),
            });
        }
        let health = RegionHealthLatch::healthy();
        let index = RegionIndex::from_storage(index).map_err(index_storage_io_error)?;
        Ok(Self {
            core: Arc::new(FileRegionCore {
                index,
                manager: RegionManagerAuthority::new(manager, health.clone()),
                shards: shards.into_boxed_slice(),
                region_access: region_access.into_boxed_slice(),
                rotation: Mutex::new(()),
                health,
            }),
            data_plane: None,
        })
    }

    fn attach_data_plane(
        &mut self,
        data: DataSuperblock,
        files: RuntimeFileSet,
        config: RuntimeConfig,
    ) -> io::Result<()> {
        if self.data_plane.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "data plane is already attached",
            ));
        }
        self.data_plane = Some(RegionDataPlane::new(
            Arc::clone(&self.core),
            data,
            files,
            config,
        )?);
        Ok(())
    }

    fn shutdown_data_plane(&mut self) -> io::Result<bool> {
        self.data_plane
            .take()
            .map(|plane| plane.shutdown())
            .unwrap_or(Ok(false))
    }

    pub(crate) fn data_plane(&self) -> io::Result<&RegionDataPlane> {
        self.data_plane.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "backend does not provide a native runtime data path",
            )
        })
    }

    fn into_core(self) -> io::Result<FileRegionCore> {
        let Self { core, data_plane } = self;
        if data_plane.is_some() {
            return Err(io::Error::other(
                "data plane must stop before freezing its core",
            ));
        }
        Arc::try_unwrap(core)
            .map_err(|_| io::Error::other("runtime still has live data-plane references"))
    }
}

impl RegionStore<FileRegionBackend<SystemRegionFileSystem>> {
    pub(crate) fn put_value(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.runtime()?.data_plane()?.put(key, value)
    }

    pub(crate) fn put_value_l2(&self, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.runtime()?.data_plane()?.put_l2(key, value)
    }

    #[cfg(test)]
    pub(crate) fn get_value(&self, key: &[u8]) -> io::Result<Option<HybridValueRead>> {
        self.runtime()?.data_plane()?.get(key)
    }

    pub(crate) async fn get_value_async(
        &self,
        key: &[u8],
        tokio_handle: &tokio::runtime::Handle,
    ) -> io::Result<Option<HybridValueRead>> {
        self.runtime()?
            .data_plane()?
            .get_async(key, tokio_handle)
            .await
    }

    pub(crate) fn delete_value(&self, key: &[u8]) -> io::Result<u64> {
        self.runtime()?.data_plane()?.delete(key)
    }

    #[cfg(test)]
    pub(crate) fn drain(&self) -> io::Result<()> {
        self.runtime()?.data_plane()?.drain()
    }

    pub(crate) async fn drain_async(&self) -> io::Result<()> {
        self.runtime()?.data_plane()?.drain_async().await
    }

    pub(crate) fn snapshot(&self) -> io::Result<CacheSnapshot> {
        self.runtime()?.data_plane()?.snapshot()
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<DetailedCacheSnapshot> {
        self.runtime()?.data_plane()?.detailed_snapshot()
    }
}
pub(crate) trait RegionFileSystem {
    type File: ControlIoBackend;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File>;

    fn open_data(&self, path: &Path, create: bool, _mode: IoMode) -> io::Result<Self::File> {
        self.open(path, create)
    }

    fn try_clone_runtime_files(&self, _file: &Self::File) -> io::Result<Option<RuntimeFileSet>> {
        Ok(None)
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File>;

    fn remove_file(&self, path: &Path) -> io::Result<()>;

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()>;

    fn sync_parent(&self, path: &Path) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SystemRegionFileSystem;

impl RegionFileSystem for SystemRegionFileSystem {
    type File = FileBackend;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File> {
        if create {
            FileBackend::open_with_io_mode(path, IoMode::Buffered)
        } else {
            FileBackend::open_existing_with_io_mode(path, IoMode::Buffered)
        }
    }

    fn open_data(&self, path: &Path, create: bool, mode: IoMode) -> io::Result<Self::File> {
        if create {
            FileBackend::open_with_io_mode(path, mode)
        } else {
            FileBackend::open_existing_with_io_mode(path, mode)
        }
    }

    fn try_clone_runtime_files(&self, file: &Self::File) -> io::Result<Option<RuntimeFileSet>> {
        file.try_clone_runtime_files().map(Some)
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

/// Concrete state/index lifecycle backed by one data file and two sidecars.
///
/// It owns the append/read runtime, persists one complete index plus the
/// Region/FIFO physical view, and never scans records during open.
pub(crate) struct FileRegionBackend<F = SystemRegionFileSystem>
where
    F: RegionFileSystem,
{
    files: RegionFiles,
    /// Used when the data file is missing or empty. Existing
    /// files retain their on-disk identities but must match this geometry and
    /// configuration fingerprint.
    format_data: DataSuperblock,
    shard_count: u32,
    runtime_config: RuntimeConfig,
    file_system: F,
    data_file: Option<F::File>,
    state_file: Option<F::File>,
    data: Option<DataSuperblock>,
    current_state: Option<SelectedState>,
    prepared_clean: Option<(u8, StateRecord)>,
    cold_reset_needed: bool,
    locked: bool,
    retain_lock: bool,
}

impl FileRegionBackend<SystemRegionFileSystem> {
    #[cfg(test)]
    pub(crate) fn new(files: RegionFiles, format_data: DataSuperblock) -> Self {
        Self::new_with_configs(files, format_data, REGION_SHARDS, RuntimeConfig::default())
    }

    pub(crate) fn new_with_configs(
        files: RegionFiles,
        format_data: DataSuperblock,
        shards: u32,
        runtime_config: RuntimeConfig,
    ) -> Self {
        Self::new_with_file_system_and_configs(
            files,
            format_data,
            SystemRegionFileSystem,
            shards,
            runtime_config,
        )
    }
}

impl<F> FileRegionBackend<F>
where
    F: RegionFileSystem,
{
    #[cfg(test)]
    fn new_with_file_system(
        files: RegionFiles,
        format_data: DataSuperblock,
        file_system: F,
    ) -> Self {
        Self::new_with_file_system_and_configs(
            files,
            format_data,
            file_system,
            REGION_SHARDS,
            RuntimeConfig::default(),
        )
    }

    fn new_with_file_system_and_configs(
        files: RegionFiles,
        format_data: DataSuperblock,
        file_system: F,
        shard_count: u32,
        runtime_config: RuntimeConfig,
    ) -> Self {
        Self {
            files,
            format_data,
            shard_count,
            runtime_config,
            file_system,
            data_file: None,
            state_file: None,
            data: None,
            current_state: None,
            prepared_clean: None,
            cold_reset_needed: false,
            locked: false,
            retain_lock: false,
        }
    }

    fn state_file(&self) -> io::Result<&F::File> {
        self.state_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "state file is not open"))
    }

    fn data_superblock(&self) -> io::Result<DataSuperblock> {
        self.data.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "data superblock was not inspected",
            )
        })
    }

    fn log_cold_recovery(&self, reason: &'static str) {
        log::info!(
            target: "cache2::recovery",
            event = "cache_recovery_cold",
            path:% = self.files.data.display(),
            reason;
            "cache recovery selected cold start"
        );
    }

    fn cold_recovery(
        &self,
        reason: &'static str,
    ) -> io::Result<RecoveryPlan<CleanFileRegionImage>> {
        self.log_cold_recovery(reason);
        Ok(RecoveryPlan::Running)
    }
}

impl<F> RegionBackend for FileRegionBackend<F>
where
    F: RegionFileSystem,
{
    type Runtime = FileRegionRuntime;
    type CleanImage = CleanFileRegionImage;
    type FrozenView = FrozenFileRegionView;
    type PreparedClean = PreparedFileRegionClean;

    fn acquire_exclusive(&mut self) -> io::Result<()> {
        if self.locked {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "RegionStore backend is already locked",
            ));
        }
        self.runtime_config.validate()?;
        if self.shard_count == 0 || self.format_data.geometry.region_count <= self.shard_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore requires at least one data shard and one additional Region",
            ));
        }
        if self.files.data == self.files.state
            || self.files.data == self.files.image
            || self.files.state == self.files.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore data/state/image paths must be distinct",
            ));
        }
        if parent_directory(&self.files.data) != parent_directory(&self.files.state)
            || parent_directory(&self.files.data) != parent_directory(&self.files.image)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore data/state/image files must share one directory",
            ));
        }
        let temporary = recovery_temporary_path(&self.files.image);
        if temporary == self.files.data
            || temporary == self.files.state
            || temporary == self.files.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore recovery temporary path collides with a cache file",
            ));
        }
        let data =
            self.file_system
                .open_data(&self.files.data, true, self.runtime_config.io_mode())?;
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
                "RegionStore data and state paths resolve to the same file",
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
        index_slots: usize,
    ) -> io::Result<RecoveryPlan<Self::CleanImage>> {
        self.file_system
            .remove_file(&recovery_temporary_path(&self.files.image))?;
        let format_data = self.format_data;
        let (data, fresh) = {
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "data file is not open")
            })?;
            let state_file = self.state_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "state file is not open")
            })?;
            inspect_or_format_data(data_file, state_file, format_data)?
        };
        self.data = Some(data);
        self.cold_reset_needed = !fresh;

        let pages = read_state_pages(self.state_file()?)?;
        let (recovery_state, state_rejection) = match latest_state([&pages[0], &pages[1]]) {
            Ok(Some(selected)) => (Some(selected), None),
            Ok(None) => (None, Some("no_valid_state")),
            Err(StateSelectionError::ConflictingGeneration(_)) => {
                (None, Some("state_generation_conflict"))
            }
            Err(StateSelectionError::UnsupportedVersion { .. }) => {
                (None, Some("state_version_unsupported"))
            }
        };
        // A conflicting same-generation pair is disposable cache state. Keep
        // the greatest decodable record only so RUNNING advances beyond it.
        self.current_state = select_state_for_fence(&pages);
        if fresh {
            self.log_cold_recovery("fresh_data_file");
            return Ok(RecoveryPlan::Fresh);
        }
        let Some(selected) = recovery_state else {
            return self.cold_recovery(state_rejection.unwrap_or("no_valid_state"));
        };
        if selected.record.state != RecoveryState::Clean {
            return self.cold_recovery("unclean_shutdown");
        }
        if !selected.record.binding.matches_data(data) {
            return self.cold_recovery("state_data_mismatch");
        }

        let image = match self.file_system.open(&self.files.image, false) {
            Ok(image) => image,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return self.cold_recovery("image_missing");
            }
            Err(error) => return Err(error),
        };
        let data_file = self
            .data_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "data file is not open"))?;
        let state_file = self
            .state_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "state file is not open"))?;
        if image.is_same_file(data_file)? || image.is_same_file(state_file)? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionStore image aliases the data or state file",
            ));
        }

        let actual_file_len = image.len()?;
        if actual_file_len < RECOVERY_PAGE_SIZE as u64 {
            return self.cold_recovery("image_truncated");
        }
        let mut header_page = [0_u8; RECOVERY_PAGE_SIZE];
        if let Err(error) = read_exact_at(&image, &mut header_page, 0) {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                return self.cold_recovery("image_truncated");
            }
            return Err(error);
        }
        let header = match RecoveryImageHeader::probe(&header_page) {
            RecoveryImageHeaderProbe::Valid(header) => header,
            RecoveryImageHeaderProbe::Unsupported(_) => {
                return self.cold_recovery("image_version_unsupported");
            }
            RecoveryImageHeaderProbe::Empty
            | RecoveryImageHeaderProbe::Corrupt
            | RecoveryImageHeaderProbe::Unrecognized
            | RecoveryImageHeaderProbe::Truncated => {
                return self.cold_recovery("image_header_invalid");
            }
        };
        let expected_slots = u64::try_from(index_slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "index capacity does not fit u64",
            )
        })?;
        let expected_index_len = recovery_image_index_len(expected_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid index image length")
        })?;
        if !clean_image_matches(
            selected.record,
            data,
            header,
            actual_file_len,
            expected_slots,
            expected_index_len,
        ) {
            return self.cold_recovery("image_identity_or_layout_mismatch");
        }
        if header.region_table_len > maximum_region_metadata_len(data.geometry.region_count)? {
            return self.cold_recovery("metadata_too_large");
        }

        let metadata_len = usize::try_from(header.region_table_len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "Region metadata exceeds this address space",
            )
        })?;
        let mut metadata_bytes = Vec::new();
        metadata_bytes
            .try_reserve_exact(metadata_len)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate Region metadata",
                )
            })?;
        metadata_bytes.resize(metadata_len, 0);
        if let Err(error) = read_exact_at(&image, &mut metadata_bytes, header.region_table_offset) {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                return self.cold_recovery("metadata_truncated");
            }
            return Err(error);
        }
        let metadata = match RegionMetadata::decode_owned(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RegionMetadataError::UnsupportedVersion(_)) => {
                return self.cold_recovery("metadata_version_unsupported");
            }
            Err(RegionMetadataError::Allocation) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot decode Region metadata",
                ));
            }
            Err(_) => return self.cold_recovery("metadata_invalid"),
        };
        if !metadata.matches_image(data, header) {
            return self.cold_recovery("metadata_identity_mismatch");
        }
        if metadata.root.shard_count != self.shard_count {
            return self.cold_recovery("append_shards_mismatch");
        }
        let file = image.try_clone_control_file()?;
        self.cold_reset_needed = false;
        Ok(RecoveryPlan::Clean(CleanFileRegionImage {
            file,
            header,
            metadata,
        }))
    }

    fn anonymous_runtime(&mut self, index_slots: usize) -> io::Result<Self::Runtime> {
        self.file_system.remove_file(&self.files.image)?;
        self.file_system
            .remove_file(&recovery_temporary_path(&self.files.image))?;
        let data = self.data_superblock()?;
        if self.cold_reset_needed {
            let data_file = self.data_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "data file is not open")
            })?;
            let state_file = self.state_file.as_ref().ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "state file is not open")
            })?;
            format_empty_data(data_file, state_file, data)?;
            self.current_state = None;
            self.cold_reset_needed = false;
        }
        let metadata = empty_region_metadata(data, index_slots, self.shard_count)?;
        let index =
            PartitionedIndexStorage::anonymous(index_slots).map_err(index_storage_io_error)?;
        let runtime = FileRegionRuntime::install(index, metadata)?;
        Ok(runtime)
    }

    fn map_clean_runtime(
        &mut self,
        clean: Self::CleanImage,
        index_slots: usize,
    ) -> io::Result<Option<Self::Runtime>> {
        let data = self.data_superblock()?;
        let expected_slots = u64::try_from(index_slots).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "index capacity does not fit u64",
            )
        })?;
        let expected_index_len = recovery_image_index_len(expected_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid index image length")
        })?;
        let actual_file_len = clean.file.metadata()?.len();
        let eligible = self.current_state.is_some_and(|selected| {
            clean_image_matches(
                selected.record,
                data,
                clean.header,
                actual_file_len,
                expected_slots,
                expected_index_len,
            )
        }) && clean.metadata.matches_image(data, clean.header)
            && clean.metadata.root.shard_count == self.shard_count
            && clean.metadata.validate().is_ok();
        if !eligible {
            self.cold_reset_needed = true;
            self.log_cold_recovery("image_became_ineligible");
            return Ok(None);
        }
        let partition_stats = metadata_partition_stats(&clean.metadata)?;
        let binding = index_image_binding(clean.header);
        let index = PartitionedIndexStorage::map_private(
            &clean.file,
            clean.header.index_offset,
            index_slots,
            binding,
            &partition_stats,
        )
        .map_err(index_storage_io_error)?;
        let runtime = FileRegionRuntime::install(index, clean.metadata)?;
        Ok(Some(runtime))
    }

    fn publish_running(&mut self) -> io::Result<()> {
        let binding = StateBinding::from_data(self.data_superblock()?, None);
        let barrier = prepare_running_barrier(self.current_state, binding)
            .map_err(|_| io::Error::other("RUNNING generation cannot advance"))?;
        let state = self.state_file()?;
        state.set_len(STATE_FILE_SIZE as u64)?;
        write_state_page(state, &barrier.first.page, barrier.first.offset())?;
        write_state_page(state, &barrier.second.page, barrier.second.offset())?;
        // One barrier covers both full-page writes. No operation can be
        // admitted before this method returns success.
        state.sync(SyncPoint::RunningState, SyncMode::Data)?;
        self.current_state = Some(SelectedState {
            slot: barrier.second.slot,
            record: barrier.second.record,
        });
        Ok(())
    }

    fn start_runtime(&mut self, mut runtime: Self::Runtime) -> io::Result<Self::Runtime> {
        let data = self.data_superblock()?;
        let data_file = self
            .data_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "data file is not open"))?;
        if let Some(files) = self.file_system.try_clone_runtime_files(data_file)? {
            runtime.attach_data_plane(data, files, self.runtime_config.clone())?;
        }
        Ok(runtime)
    }

    fn stop_fast(&mut self, mut runtime: Self::Runtime) -> io::Result<()> {
        match runtime.shutdown_data_plane() {
            Ok(false) => Ok(()),
            Ok(true) => {
                self.retain_lock = true;
                Err(io::Error::other(
                    "I/O engine could not fence an issued write; lock retained",
                ))
            }
            Err(error) => {
                runtime.health.enter_miss_only();
                Err(error)
            }
        }
    }

    fn freeze_warm(&mut self, mut runtime: Self::Runtime) -> io::Result<Self::FrozenView> {
        match runtime.shutdown_data_plane() {
            Ok(false) => {}
            Ok(true) => {
                self.retain_lock = true;
                runtime.health.enter_miss_only();
                return Err(io::Error::other(
                    "I/O engine could not fence an issued write; CLEAN rejected",
                ));
            }
            Err(error) => {
                runtime.health.enter_miss_only();
                return Err(error);
            }
        }
        runtime.health.require_healthy()?;
        let FileRegionCore {
            index,
            manager,
            shards,
            region_access: _,
            rotation,
            health,
        } = runtime.into_core()?;
        if shards.iter().any(|shard| shard.mutation.is_poisoned()) || rotation.is_poisoned() {
            health.enter_miss_only();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data shard gate is poisoned",
            ));
        }
        let manager = manager.into_inner()?;
        let partitions = index_partition_metadata(index.storage(), &health)?;
        let metadata = manager
            .freeze_metadata(partitions)
            .map_err(region_metadata_io_error)?;
        health.require_healthy()?;
        Ok(FrozenFileRegionView {
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
        let partition_stats = guarded_index_result(&view.health, storage.partition_stats())?;
        if source_metadata.root.index_slots
            != u64::try_from(storage.slot_count()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "index capacity is too large")
            })?
            || !metadata_partition_stats_match(source_metadata, &partition_stats)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frozen index and Region metadata accounting disagree",
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
            io::Error::new(io::ErrorKind::InvalidData, "Region metadata is too large")
        })?;
        let index_slots = u64::try_from(storage.slot_count()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "index capacity is too large")
        })?;
        let index_len = recovery_image_index_len(index_slots).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid index image length")
        })?;
        let region_table_offset = RECOVERY_IMAGE_INDEX_OFFSET
            .checked_add(index_len)
            .ok_or_else(|| io::Error::other("image offset overflow"))?;
        let image_file_len = region_table_offset
            .checked_add(metadata_len)
            .ok_or_else(|| io::Error::other("image length overflow"))?;
        let header = RecoveryImageHeader {
            cache_uuid: data.cache_uuid,
            data_identity: data.data_identity,
            data_superblock_generation: data.generation,
            hash_seed: data.hash_seed,
            config_fingerprint: data.config_fingerprint,
            image_identity,
            image_generation,
            image_file_len,
            index_slots,
            index_offset: RECOVERY_IMAGE_INDEX_OFFSET,
            index_len,
            region_table_offset,
            region_table_len: metadata_len,
        };
        let header_page = header.encode().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid recovery image header")
        })?;
        let clean_state = prepare_next_state(
            self.current_state,
            RecoveryState::Clean,
            StateBinding::from_data(data, Some(header.image_binding())),
        )
        .map_err(|_| io::Error::other("CLEAN generation cannot advance"))?;
        if clean_state.record.generation != image_generation {
            return Err(io::Error::other(
                "image and state generations were not frozen together",
            ));
        }

        let data_file = self
            .data_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "data file is not open"))?;
        data_file.sync(SyncPoint::WarmData, SyncMode::Data)?;

        let temporary = recovery_temporary_path(&self.files.image);
        self.file_system.remove_file(&temporary)?;
        let persisted = (|| {
            let image = self.file_system.create_new(&temporary)?;
            image.set_len(image_file_len)?;
            write_all_at(&image, WritePoint::RecoveryImageHeader, &header_page, 0)?;
            let mut writer = PositionedIoWriter::new(
                &image,
                WritePoint::RecoveryImageIndex,
                RECOVERY_IMAGE_INDEX_OFFSET,
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
                    "index writer produced inconsistent length or physical statistics",
                ));
            }
            write_all_at(
                &image,
                WritePoint::RecoveryImageMetadata,
                &metadata_bytes,
                region_table_offset,
            )?;
            image.sync(SyncPoint::RecoveryImage, SyncMode::Data)?;
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
        Ok(PreparedFileRegionClean {
            state: clean_state,
            health: view.health.clone(),
        })
    }

    fn publish_clean(&mut self, prepared: Self::PreparedClean) -> io::Result<()> {
        prepared.health.require_healthy()?;
        if self.prepared_clean.take() != Some((prepared.state.slot, prepared.state.record)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CLEAN token does not belong to this backend session",
            ));
        }
        let data = self.data_superblock()?;
        let expected = prepare_next_state(
            self.current_state,
            RecoveryState::Clean,
            prepared.state.record.binding,
        )
        .map_err(|_| io::Error::other("CLEAN generation cannot advance"))?;
        if expected != prepared.state
            || !prepared.state.record.binding.matches_data(data)
            || prepared.state.record.binding.image.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CLEAN token no longer matches current data/state authority",
            ));
        }
        let state = self.state_file()?;
        prepared.health.require_healthy()?;
        write_state_page(state, &prepared.state.page, prepared.state.offset())?;
        state.sync(SyncPoint::CleanState, SyncMode::Data)?;
        self.current_state = Some(SelectedState {
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
        let data_result = if self.retain_lock {
            Ok(())
        } else {
            self.data_file
                .as_ref()
                .map(IoBackend::unlock)
                .unwrap_or(Ok(()))
        };
        self.locked = false;
        self.prepared_clean = None;
        self.state_file.take();
        self.data_file.take();
        state_result.and(data_result)
    }
}

fn inspect_or_format_data<D, S>(
    file: &D,
    state: &S,
    format_data: DataSuperblock,
) -> io::Result<(DataSuperblock, bool)>
where
    D: IoBackend,
    S: IoBackend,
{
    let file_len = file.len()?;
    if file_len >= RECOVERY_PAGE_SIZE as u64 {
        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        read_exact_at(file, &mut page, 0)?;
        match DataSuperblock::probe(&page) {
            DataSuperblockProbe::Valid(data) => {
                if data.geometry != format_data.geometry
                    || data.hash_seed != format_data.hash_seed
                    || data.config_fingerprint != format_data.config_fingerprint
                    || file_len != data.geometry.data_file_len
                {
                    format_empty_data(file, state, format_data)?;
                    return Ok((format_data, true));
                }
                return Ok((data, false));
            }
            DataSuperblockProbe::Unsupported(_)
            | DataSuperblockProbe::Empty
            | DataSuperblockProbe::Corrupt
            | DataSuperblockProbe::Unrecognized
            | DataSuperblockProbe::Truncated => {}
        }
    }

    format_empty_data(file, state, format_data)?;
    Ok((format_data, true))
}

/// Invalidates every old recovery authority before discarding Region bytes.
/// Truncating the cold data extent prevents a later record-version domain from
/// ever matching stale bytes, without scanning the file or its old records.
fn format_empty_data<D, S>(file: &D, state: &S, format_data: DataSuperblock) -> io::Result<()>
where
    D: IoBackend,
    S: IoBackend,
{
    let encoded = format_data
        .encode()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid data format"))?;
    state.set_len(0)?;
    state.sync(SyncPoint::StateReset, SyncMode::Data)?;
    file.set_len(0)?;
    file.sync(SyncPoint::FormatTruncate, SyncMode::All)?;
    // Establish the complete extent once. Runtime shard writes then remain
    // positioned and sequential within Regions instead of allocating blocks
    // on the latency-sensitive path or discovering ENOSPC after admission.
    file.preallocate(format_data.geometry.data_file_len)?;
    write_all_at(file, WritePoint::DataSuperblock, &encoded, 0)?;
    file.sync(SyncPoint::FormatData, SyncMode::All)?;
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
            match read_at_bounded(file, &mut page[filled..], offset) {
                Ok(0) => return Ok(pages),
                Ok(read) => filled += read,
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
) -> Option<SelectedState> {
    if let Ok(selected) = latest_state([&pages[0], &pages[1]]) {
        return selected;
    }
    pages
        .iter()
        .enumerate()
        .filter_map(|(slot, page)| {
            StateRecord::decode(page).map(|record| SelectedState {
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
    write_all_at(file, WritePoint::State, page, offset)
}

fn maximum_region_metadata_len(region_count: u32) -> io::Result<u64> {
    fn pages_for(count: u64, per_page: u64) -> io::Result<u64> {
        count
            .checked_add(per_page - 1)
            .map(|rounded| rounded / per_page)
            .ok_or_else(|| io::Error::other("Region metadata page count overflow"))
    }

    let region_pages = pages_for(
        u64::from(region_count),
        REGION_METADATA_REGIONS_PER_PAGE as u64,
    )?;
    let shard_pages = pages_for(
        MAX_INDEX_PARTITIONS as u64,
        REGION_METADATA_PARTITIONS_PER_PAGE as u64,
    )?;
    1_u64
        .checked_add(region_pages)
        .and_then(|pages| pages.checked_add(shard_pages))
        .and_then(|pages| pages.checked_mul(REGION_METADATA_PAGE_SIZE as u64))
        .ok_or_else(|| io::Error::other("Region metadata length overflow"))
}

fn empty_partition_metadata(
    ranges: &[IndexPartitionRange],
) -> io::Result<Box<[PartitionMetadataRecord]>> {
    let mut stats = Vec::new();
    stats.try_reserve_exact(ranges.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate index partition statistics",
        )
    })?;
    stats.resize(ranges.len(), IndexPhysicalStats::default());
    partition_metadata_from_stats(ranges, &stats)
}

fn index_partition_metadata(
    index: &PartitionedIndexStorage,
    health: &RegionHealthLatch,
) -> io::Result<Box<[PartitionMetadataRecord]>> {
    let stats = guarded_index_result(health, index.partition_stats())?;
    partition_metadata_from_stats(index.partition_ranges(), &stats)
}

fn partition_metadata_from_stats(
    ranges: &[IndexPartitionRange],
    stats: &[IndexPhysicalStats],
) -> io::Result<Box<[PartitionMetadataRecord]>> {
    if ranges.len() != stats.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index partition ranges and statistics disagree",
        ));
    }
    let mut partitions = Vec::new();
    partitions.try_reserve_exact(ranges.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate index partition directory",
        )
    })?;
    for (range, stats) in ranges.iter().zip(stats) {
        partitions.push(PartitionMetadataRecord {
            partition_id: u32::try_from(range.partition_id).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "partition id is too large")
            })?,
            first_index_page: u64::try_from(range.first_page).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "partition page offset is too large",
                )
            })?,
            index_page_count: u64::try_from(range.page_count).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "partition page count is too large",
                )
            })?,
            first_slot: u64::try_from(range.first_slot).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "partition slot offset is too large",
                )
            })?,
            slot_count: u64::try_from(range.slot_count).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "partition slot count is too large",
                )
            })?,
            physical_value_slots: stats.value,
            physical_deleted_slots: stats.deleted,
        });
    }
    Ok(partitions.into_boxed_slice())
}

fn metadata_partition_stats(metadata: &RegionMetadata) -> io::Result<Box<[IndexPhysicalStats]>> {
    let mut stats = Vec::new();
    stats
        .try_reserve_exact(metadata.partitions.len())
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate index partition statistics",
            )
        })?;
    for partition in &metadata.partitions {
        stats.push(IndexPhysicalStats {
            value: partition.physical_value_slots,
            deleted: partition.physical_deleted_slots,
        });
    }
    Ok(stats.into_boxed_slice())
}

fn metadata_partition_stats_match(metadata: &RegionMetadata, stats: &[IndexPhysicalStats]) -> bool {
    metadata.partitions.len() == stats.len()
        && metadata
            .partitions
            .iter()
            .zip(stats)
            .all(|(metadata, actual)| {
                metadata.physical_value_slots == actual.value
                    && metadata.physical_deleted_slots == actual.deleted
            })
}

fn empty_region_metadata(
    data: DataSuperblock,
    index_slots: usize,
    shards: u32,
) -> io::Result<RegionMetadata> {
    if shards == 0 || data.geometry.region_count <= shards {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RegionStore requires one Active Region per shard plus one spare",
        ));
    }
    let partition_ranges =
        canonical_index_partition_ranges(index_slots).map_err(index_storage_io_error)?;
    let index_slots = u64::try_from(index_slots)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "index is too large"))?;
    let index_len = recovery_image_index_len(index_slots)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid index length"))?;
    let index_page_count = index_len / RECOVERY_PAGE_SIZE as u64;

    let region_count = data.geometry.region_count as usize;
    let mut regions = Vec::new();
    regions
        .try_reserve_exact(region_count)
        .map_err(|_| io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate Region table"))?;
    let mut free_ordinal = 0_u32;
    for region_id in 0..data.geometry.region_count {
        let active = region_id < shards;
        let queue_ordinal = if active {
            region_id
        } else {
            let ordinal = free_ordinal;
            free_ordinal = free_ordinal
                .checked_add(1)
                .ok_or_else(|| io::Error::other("free Region ordinal overflow"))?;
            ordinal
        };
        regions.push(RegionMetadataRecord {
            state: if active {
                RegionMetadataState::Active
            } else {
                RegionMetadataState::Free
            },
            queue_ordinal,
            created_seqno: if active { u64::from(region_id) + 1 } else { 0 },
            durable_used_offset: 0,
            physical_record_count: 0,
        });
    }
    let partitions = empty_partition_metadata(&partition_ranges)?;
    let metadata = RegionMetadata {
        root: RegionMetadataRoot {
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
            partition_count: u32::try_from(partition_ranges.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many index partitions")
            })?,
            shard_count: shards,
            max_seqno: u64::from(shards),
            free_region_count: data.geometry.region_count - shards,
            active_region_count: shards,
            sealed_region_count: 0,
        },
        regions: regions.into_boxed_slice(),
        partitions,
    };
    metadata.validate().map_err(region_metadata_io_error)?;
    Ok(metadata)
}

fn next_state_generation(current: Option<SelectedState>) -> io::Result<u64> {
    current
        .map_or(Some(1), |selected| {
            selected.record.generation.checked_add(1)
        })
        .ok_or_else(|| io::Error::other("state generation is exhausted"))
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

fn index_image_binding(header: RecoveryImageHeader) -> IndexImageBinding {
    let bytes = header.image_identity.to_bytes();
    let left = u64::from_le_bytes(bytes[..8].try_into().expect("fixed identity half"));
    let right = u64::from_le_bytes(bytes[8..].try_into().expect("fixed identity half"));
    let mixed = left ^ right.rotate_left(17);
    IndexImageBinding {
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
            .ok_or_else(|| io::Error::other("image writer offset overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests;
