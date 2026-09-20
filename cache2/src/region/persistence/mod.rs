// Copyright 2026 ScopeDB, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! File ownership, restart recovery, and durable Region image publication.

use std::fmt;
use std::fs::File;
use std::io::Write;
use std::io::{self};
use std::path::Path;
use std::path::PathBuf;

use crate::config::runtime::IoMode;
use crate::io::file::DataFileHandles;
use crate::io::file::PositionedIo;
use crate::io::file::StorageFile;
use crate::io::file::SyncMode;
use crate::io::file::SyncPoint;
use crate::io::file::WritePoint;
use crate::io::file::read_at_bounded;
use crate::io::file::read_exact_at;
use crate::io::file::write_all_at;
use crate::io::fs::FileSystem;
use crate::io::fs::OsFileSystem;
use crate::io::fs::parent_directory;
use crate::region::FrozenRegionStore;
use crate::region::RegionHealthLatch;
use crate::region::RegionStore;
use crate::region::empty_partition_metadata;
use crate::region::guarded_index_result;
use crate::region::index::packed::MAX_INDEX_PARTITIONS;
use crate::region::index::storage::IndexImageBinding;
use crate::region::index::storage::PartitionedIndexStorage;
use crate::region::index::storage::canonical_index_partition_ranges;
use crate::region::index_storage_io_error;
use crate::region::metadata_partition_stats;
use crate::region::metadata_partition_stats_match;
use crate::region::recovery::DataSuperblock;
use crate::region::recovery::DataSuperblockProbe;
use crate::region::recovery::PersistentId;
use crate::region::recovery::RECOVERY_IMAGE_INDEX_OFFSET;
use crate::region::recovery::RECOVERY_PAGE_SIZE;
use crate::region::recovery::RecoveryImageHeader;
use crate::region::recovery::RecoveryImageHeaderProbe;
use crate::region::recovery::RecoveryState;
use crate::region::recovery::STATE_FILE_SIZE;
use crate::region::recovery::STATE_SLOT_COUNT;
use crate::region::recovery::SelectedState;
use crate::region::recovery::StateBinding;
use crate::region::recovery::StatePageWrite;
use crate::region::recovery::StateRecord;
use crate::region::recovery::StateSelectionError;
use crate::region::recovery::clean_image_matches;
use crate::region::recovery::latest_state;
use crate::region::recovery::metadata::REGION_METADATA_PAGE_SIZE;
use crate::region::recovery::metadata::REGION_METADATA_PARTITIONS_PER_PAGE;
use crate::region::recovery::metadata::REGION_METADATA_REGIONS_PER_PAGE;
use crate::region::recovery::metadata::RegionMetadata;
use crate::region::recovery::metadata::RegionMetadataError;
use crate::region::recovery::metadata::RegionMetadataRecord;
use crate::region::recovery::metadata::RegionMetadataRoot;
use crate::region::recovery::metadata::RegionState;
use crate::region::recovery::prepare_next_state;
use crate::region::recovery::prepare_running_barrier;
use crate::region::recovery::recovery_image_index_len;
use crate::region::region_metadata_io_error;

/// Shared shard count for compact concrete-file fixtures.
#[cfg(test)]
const REGION_SHARDS: u32 = 4;

/// Paths to the data file and its state and recovery-image sidecars.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegionPaths {
    pub data: PathBuf,
    pub state: PathBuf,
    pub image: PathBuf,
}

impl RegionPaths {
    pub fn new(
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
pub struct RecoveryImage {
    file: File,
    header: RecoveryImageHeader,
    metadata: RegionMetadata,
}

pub struct PreparedClean {
    state: StatePageWrite,
    health: RegionHealthLatch,
}

/// Owns cache files and publishes the persistent state used by restart recovery.
pub struct RegionPersistence<F = OsFileSystem>
where
    F: FileSystem,
{
    paths: RegionPaths,
    /// Used when the data file is missing or empty. Existing
    /// files retain their on-disk identities but must match this geometry and
    /// storage-layout fingerprint.
    format_data: DataSuperblock,
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

impl<F: FileSystem> RegionPersistence<F> {
    pub fn new(paths: RegionPaths, format_data: DataSuperblock, file_system: F) -> Self {
        Self {
            paths,
            format_data,
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

    pub fn data_superblock(&self) -> io::Result<DataSuperblock> {
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
            path:% = self.paths.data.display(),
            image_path:% = self.paths.image.display(),
            reason;
            "cache recovery selected cold start"
        );
    }

    fn log_cold_recovery_error(
        &self,
        reason: &'static str,
        index_slots: usize,
        index_mapping_bytes: u64,
        error: &impl fmt::Display,
    ) {
        log::warn!(
            target: "cache2::recovery",
            event = "cache_recovery_cold",
            path:% = self.paths.data.display(),
            image_path:% = self.paths.image.display(),
            reason,
            index_backing = "file_private_mmap",
            index_slots,
            index_mapping_bytes,
            error:% = error;
            "warm index mapping failed; cache recovery selected cold start"
        );
    }

    fn log_append_shards_rebind_planned(&self, previous: u32, current: u32) {
        log::info!(
            target: "cache2::recovery",
            event = "cache_append_shards_rebind_planned",
            path:% = self.paths.data.display(),
            previous_append_shards = previous,
            append_shards = current,
            reused_active_regions = previous.min(current),
            activated_free_regions = current.saturating_sub(previous),
            sealed_active_regions = previous.saturating_sub(current);
            "warm recovery planned append shard rebind"
        );
    }

    fn cold_recovery(&self, reason: &'static str) -> io::Result<Option<RecoveryImage>> {
        self.log_cold_recovery(reason);
        Ok(None)
    }

    /// Acquire exclusive ownership of the data and state files before inspection.
    pub fn acquire_exclusive(&mut self, io_mode: IoMode) -> io::Result<()> {
        if self.locked {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "cache files are already locked",
            ));
        }
        if self.paths.data == self.paths.state
            || self.paths.data == self.paths.image
            || self.paths.state == self.paths.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache data/state/image paths must be distinct",
            ));
        }
        if parent_directory(&self.paths.data) != parent_directory(&self.paths.state)
            || parent_directory(&self.paths.data) != parent_directory(&self.paths.image)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache data/state/image files must share one directory",
            ));
        }
        let temporary = recovery_temporary_path(&self.paths.image);
        if temporary == self.paths.data
            || temporary == self.paths.state
            || temporary == self.paths.image
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache recovery temporary path collides with a cache file",
            ));
        }
        let data = self.file_system.open(&self.paths.data, true, io_mode)?;
        data.try_lock_exclusive()?;
        let state = match self
            .file_system
            .open(&self.paths.state, true, IoMode::Buffered)
        {
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
                "cache data and state paths resolve to the same file",
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

    /// Return an eligible clean image, or `None` to select a cold start.
    /// This must not allocate or scan the full index or Region data extents.
    pub fn inspect_recovery(&mut self, index_slots: usize) -> io::Result<Option<RecoveryImage>> {
        self.file_system
            .remove_file(&recovery_temporary_path(&self.paths.image))?;
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
            return self.cold_recovery("fresh_data_file");
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

        let image = match self
            .file_system
            .open(&self.paths.image, false, IoMode::Buffered)
        {
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
                "cache image aliases the data or state file",
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
        let file = image.try_clone_mapping_file()?;
        self.cold_reset_needed = false;
        Ok(Some(RecoveryImage {
            file,
            header,
            metadata,
        }))
    }

    /// Discard stale recovery artifacts and construct an empty Region store.
    pub fn cold_regions(
        &mut self,
        index_slots: usize,
        append_shards: u32,
    ) -> io::Result<RegionStore> {
        self.file_system.remove_file(&self.paths.image)?;
        self.file_system
            .remove_file(&recovery_temporary_path(&self.paths.image))?;
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
        let metadata = empty_region_metadata(data, index_slots, append_shards)?;
        let index = match PartitionedIndexStorage::anonymous(index_slots) {
            Ok(index) => index,
            Err(error) => {
                let index_mapping_bytes = u64::try_from(index_slots)
                    .ok()
                    .and_then(recovery_image_index_len)
                    .unwrap_or(0);
                log::error!(
                    target: "cache2::recovery",
                    event = "cache_index_backing_failed",
                    path:% = self.paths.data.display(),
                    index_backing = anonymous_index_backing_name(),
                    index_slots,
                    index_mapping_bytes,
                    error:% = error;
                    "anonymous index backing could not be created"
                );
                return Err(index_storage_io_error(error));
            }
        };
        RegionStore::from_recovery(index, metadata)
    }

    /// `Ok(None)` rejects the complete image and selects a cold start.
    pub fn recover_regions(
        &mut self,
        mut image: RecoveryImage,
        index_slots: usize,
        append_shards: u32,
    ) -> io::Result<Option<RegionStore>> {
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
        let actual_file_len = image.file.metadata()?.len();
        let eligible = self.current_state.is_some_and(|selected| {
            clean_image_matches(
                selected.record,
                data,
                image.header,
                actual_file_len,
                expected_slots,
                expected_index_len,
            )
        }) && image.metadata.matches_image(data, image.header)
            && image.metadata.validate().is_ok();
        if !eligible {
            self.cold_reset_needed = true;
            self.log_cold_recovery("image_became_ineligible");
            return Ok(None);
        }
        let previous_append_shards = image.metadata.root.shard_count;
        if previous_append_shards != append_shards {
            let added_shards = append_shards.saturating_sub(previous_append_shards);
            if added_shards > image.metadata.root.free_region_count {
                self.cold_reset_needed = true;
                self.log_cold_recovery("append_shards_rebind_insufficient_free_regions");
                return Ok(None);
            }
            if image.metadata.rebind_append_shards(append_shards).is_err() {
                self.cold_reset_needed = true;
                self.log_cold_recovery("append_shards_rebind_invalid");
                return Ok(None);
            }
            self.log_append_shards_rebind_planned(previous_append_shards, append_shards);
        }
        let partition_stats = metadata_partition_stats(&image.metadata)?;
        let binding = index_image_binding(image.header);
        let index_mapping_bytes = image.header.index_offset.saturating_add(expected_index_len);
        let index = match PartitionedIndexStorage::map_private(
            &image.file,
            image.header.index_offset,
            index_slots,
            binding,
            &partition_stats,
        ) {
            Ok(index) => index,
            Err(error) => {
                self.cold_reset_needed = true;
                self.log_cold_recovery_error(
                    "index_private_mmap_failed",
                    index_slots,
                    index_mapping_bytes,
                    &error,
                );
                return Ok(None);
            }
        };
        let regions = RegionStore::from_recovery(index, image.metadata)?;
        Ok(Some(regions))
    }

    /// Replace both state slots with durable `RUNNING` generations so a torn
    /// page cannot revive a previous `CLEAN` generation after Region reuse.
    pub fn publish_running(&mut self) -> io::Result<()> {
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

    pub fn clone_data_handles(&self) -> io::Result<DataFileHandles> {
        self.data_file
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "data file is not open"))?
            .try_clone_data_handles()
    }

    /// Keep ownership with issued writes whose completion cannot be fenced.
    pub fn retain_data_lock(&mut self) {
        self.retain_lock = true;
    }

    /// Make completed data and one complete image durable.
    pub fn persist_frozen(&mut self, frozen: &FrozenRegionStore) -> io::Result<PreparedClean> {
        let health = &frozen.regions.health;
        health.require_healthy()?;
        let source_metadata = &frozen.metadata;
        source_metadata
            .validate()
            .map_err(region_metadata_io_error)?;
        let storage = frozen.regions.index.storage();
        let physical_stats = guarded_index_result(health, storage.physical_stats())?;
        let partition_stats = guarded_index_result(health, storage.partition_stats())?;
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
            storage_fingerprint: data.storage_fingerprint,
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

        let temporary = recovery_temporary_path(&self.paths.image);
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
                health,
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
            health.require_healthy()?;
            self.file_system.rename(&temporary, &self.paths.image)?;
            self.file_system.sync_parent(&self.paths.image)?;
            health.require_healthy()
        })();
        if persisted.is_err() {
            let _ = self.file_system.remove_file(&temporary);
        }
        persisted?;
        self.prepared_clean = Some((clean_state.slot, clean_state.record));
        Ok(PreparedClean {
            state: clean_state,
            health: health.clone(),
        })
    }

    /// Publish `CLEAN` durably using the token returned after persistence.
    pub fn publish_clean(&mut self, prepared: PreparedClean) -> io::Result<()> {
        prepared.health.require_healthy()?;
        if self.prepared_clean.take() != Some((prepared.state.slot, prepared.state.record)) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "CLEAN token does not belong to this persistence session",
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

    /// Release ownership, including during error unwinding after acquisition.
    pub fn release_exclusive(&mut self) -> io::Result<()> {
        if !self.locked {
            return Ok(());
        }
        let state_result = self
            .state_file
            .as_ref()
            .map(StorageFile::unlock)
            .unwrap_or(Ok(()));
        let data_result = if self.retain_lock {
            Ok(())
        } else {
            self.data_file
                .as_ref()
                .map(StorageFile::unlock)
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
    D: StorageFile,
    S: StorageFile,
{
    let file_len = file.len()?;
    if file_len >= RECOVERY_PAGE_SIZE as u64 {
        let mut page = [0_u8; RECOVERY_PAGE_SIZE];
        read_exact_at(file, &mut page, 0)?;
        match DataSuperblock::probe(&page) {
            DataSuperblockProbe::Valid(data) => {
                if data.geometry != format_data.geometry
                    || data.hash_seed != format_data.hash_seed
                    || data.storage_fingerprint != format_data.storage_fingerprint
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
    D: StorageFile,
    S: StorageFile,
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
    B: PositionedIo,
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
    B: PositionedIo,
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

fn empty_region_metadata(
    data: DataSuperblock,
    index_slots: usize,
    append_shards: u32,
) -> io::Result<RegionMetadata> {
    if append_shards == 0 || data.geometry.region_count <= append_shards {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "cache requires one Active Region per shard plus one spare",
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
        let active = region_id < append_shards;
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
                RegionState::Active
            } else {
                RegionState::Free
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
            storage_fingerprint: data.storage_fingerprint,
            index_slots,
            index_page_count,
            region_size: data.geometry.region_size,
            region_count: data.geometry.region_count,
            partition_count: u32::try_from(partition_ranges.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "too many index partitions")
            })?,
            shard_count: append_shards,
            max_seqno: u64::from(append_shards),
            free_region_count: data.geometry.region_count - append_shards,
            active_region_count: append_shards,
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

const fn anonymous_index_backing_name() -> &'static str {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        "anonymous_mmap"
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        "heap"
    }
}

struct PositionedIoWriter<'a, B: PositionedIo + ?Sized> {
    file: &'a B,
    point: WritePoint,
    offset: u64,
}

impl<'a, B: PositionedIo + ?Sized> PositionedIoWriter<'a, B> {
    const fn new(file: &'a B, point: WritePoint, offset: u64) -> Self {
        Self {
            file,
            point,
            offset,
        }
    }

    const fn offset(&self) -> u64 {
        self.offset
    }
}

impl<B: PositionedIo + ?Sized> Write for PositionedIoWriter<'_, B> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.file.write_at(self.point, buffer, self.offset)?;
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
