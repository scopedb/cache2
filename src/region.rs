//! Concrete Region file backend and data-plane authority.
//!
//! This module owns Region/index state, recovery-image I/O, and the production
//! backend implementation. The backend-independent recovery and shutdown
//! state machine lives in `region_store`, keeping lifecycle ordering separate
//! from file-format and steady-state data-path mechanics.

use std::fs::File;
use std::io::{self, Write};
use std::ops::{Deref, Range};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::checksum::crc32c;
use crate::format::{RECORD_HEADER_SIZE, RecordHeader};
use crate::index::{IndexEntry, MAX_INDEX_PARTITIONS};
use crate::index_storage::{
    IndexImageBinding, IndexPartitionRange, IndexPhysicalStats, IndexStorageError,
    PartitionedIndexStorage, WARM_IMAGE_WRITE_BATCH_BYTES, canonical_index_partition_ranges,
};
use crate::io_backend::{
    ControlIoBackend, FileBackend, IoBackend, RuntimeFileSet, SyncMode, SyncPoint, WritePoint,
    read_at_bounded, read_exact_at, write_all_at,
};
use crate::io_engine::{IoEngine, ReadSlot};
use crate::record_codec::{RecordEncodeError, encode_value_into_hashed};
#[cfg(test)]
use crate::record_codec::{hash_namespaced_key, required_record_bytes};
use crate::recovery::{
    DataSuperblock, DataSuperblockProbe, PersistentId, RECOVERY_IMAGE_INDEX_OFFSET,
    RECOVERY_PAGE_SIZE, RecoveryImageHeader, RecoveryImageHeaderProbe, RecoveryState,
    STATE_FILE_SIZE, STATE_SLOT_COUNT, SelectedState, StateBinding, StatePageWrite, StateRecord,
    StateSelectionError, clean_image_matches, latest_state, prepare_next_state,
    prepare_running_barrier, recovery_image_index_len,
};
use crate::region_appender::submit_span;
use crate::region_index::RegionIndex;
use crate::region_layout::RegionLayout;
use crate::region_manager::{RegionManager, RegionMutationError};
use crate::region_metadata::{
    PartitionMetadataRecord, REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE, RegionMetadata, RegionMetadataError, RegionMetadataRecord,
    RegionMetadataRoot, RegionMetadataState,
};
#[cfg(test)]
use crate::region_reader::plan_read;
use crate::region_reader::{PendingRead, ReadCompletion, ReadPlan, submit_read};
use crate::region_runtime::{HybridValueRead, RegionDataPlane};
use crate::region_staging::{
    RegionStaging, StageAppend, StagedRecord, StagedRecordKind, StagedWrite, StagingEncodeError,
    StagingError,
};
use crate::region_store::{RecoveryPlan, RegionBackend, RegionStore};
use crate::resources::BufferLease;
use crate::runtime_config::{IoMode, RuntimeConfig};
#[cfg(test)]
use crate::snapshot::StartupMode;
use crate::snapshot::{
    CacheIndexSnapshot, CacheSnapshot, DetailedCacheSnapshot, RegionSetSnapshot,
};

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

const REGION_HEALTHY: u8 = 0;
const REGION_MISS_ONLY: u8 = 1;
/// One-way health fence shared by the live, frozen, and prepared-clean owners.
/// Once a lazy index fault rejects the recovery image, no later phase may
/// publish CLEAN from the partially trusted authority.
#[derive(Clone)]
pub(crate) struct RegionHealthLatch {
    state: Arc<AtomicU8>,
}

impl RegionHealthLatch {
    fn healthy() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(REGION_HEALTHY)),
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == REGION_HEALTHY
    }

    pub(crate) fn enter_miss_only(&self) {
        self.state.store(REGION_MISS_ONLY, Ordering::Release);
    }

    pub(crate) fn require_healthy(&self) -> io::Result<()> {
        self.is_healthy().then_some(()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore is miss-only and cannot publish CLEAN",
            )
        })
    }
}

/// The steady-state owner of Region allocation, FIFO rotation, and write-span
/// accounting. Index publication is deliberately independent: sequence order
/// selects newer entries, and reads validate the physical record locally.
struct RegionManagerAuthority {
    inner: Mutex<RegionManager>,
    health: RegionHealthLatch,
}

impl RegionManagerAuthority {
    fn new(manager: RegionManager, health: RegionHealthLatch) -> Self {
        Self {
            inner: Mutex::new(manager),
            health,
        }
    }

    fn lock(&self) -> io::Result<MutexGuard<'_, RegionManager>> {
        self.health.require_healthy()?;
        match self.inner.lock() {
            Ok(guard) if self.health.is_healthy() => Ok(guard),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore became miss-only while acquiring Region authority",
            )),
            Err(_) => {
                self.health.enter_miss_only();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RegionStore Region authority is poisoned",
                ))
            }
        }
    }

    fn try_lock(&self) -> io::Result<Option<MutexGuard<'_, RegionManager>>> {
        self.health.require_healthy()?;
        match self.inner.try_lock() {
            Ok(guard) if self.health.is_healthy() => Ok(Some(guard)),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore became miss-only while acquiring Region authority",
            )),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                self.health.enter_miss_only();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RegionStore Region authority is poisoned",
                ))
            }
        }
    }

    fn into_inner(self) -> io::Result<RegionManager> {
        match self.inner.into_inner() {
            Ok(manager) if self.health.is_healthy() => Ok(manager),
            Ok(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionStore became miss-only while freezing Region authority",
            )),
            Err(_) => {
                self.health.enter_miss_only();
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RegionStore Region authority is poisoned",
                ))
            }
        }
    }
}

pub(crate) struct FileRegionCore {
    index: RegionIndex,
    manager: RegionManagerAuthority,
    layout: Arc<RegionLayout>,
    shards: Box<[RegionShard]>,
    rotation: Box<[Mutex<()>]>,
    health: RegionHealthLatch,
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

/// Short shard-local transaction gate. `mutation` makes manager receipts and
/// staging transitions one operation. Span completion and rotation are already
/// ordered by the shard's single production worker.
#[derive(Default)]
struct RegionShard {
    mutation: Mutex<()>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionStageValue {
    Staged(u64),
    NeedsProgress,
    NeedsRotation,
}

pub(crate) struct RegionValueRead {
    buffer: BufferLease,
    buffer_len: usize,
    value_range: Range<usize>,
    seqno: u64,
}

impl RegionValueRead {
    pub(crate) fn value(&self) -> &[u8] {
        &self
            .buffer
            .prepared(self.buffer_len)
            .expect("validated read retains its prepared buffer")[self.value_range.clone()]
    }

    pub(crate) const fn seqno(&self) -> u64 {
        self.seqno
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
    #[cfg(test)]
    fn install(index: PartitionedIndexStorage, metadata: RegionMetadata) -> io::Result<Self> {
        let layout = Arc::new(RegionLayout::single(
            metadata.root.region_count,
            metadata.root.shard_count,
        )?);
        Self::install_with_layout(index, metadata, layout)
    }

    fn install_with_layout(
        index: PartitionedIndexStorage,
        metadata: RegionMetadata,
        layout: Arc<RegionLayout>,
    ) -> io::Result<Self> {
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
        let manager = RegionManager::from_metadata_with_layout(metadata, Arc::clone(&layout))
            .map_err(region_metadata_io_error)?;
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
        let mut rotation = Vec::new();
        rotation
            .try_reserve_exact(layout.sets().len())
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate RegionSet rotation gates",
                )
            })?;
        rotation.resize_with(layout.sets().len(), || Mutex::new(()));
        let health = RegionHealthLatch::healthy();
        let index = RegionIndex::try_from_storage(
            index,
            manager.regions().iter().map(|region| region.created_seqno),
        )?;
        Ok(Self {
            core: Arc::new(FileRegionCore {
                index,
                manager: RegionManagerAuthority::new(manager, health.clone()),
                layout,
                shards: shards.into_boxed_slice(),
                rotation: rotation.into_boxed_slice(),
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
    pub(crate) fn put_value(&self, namespace_id: u32, key: &[u8], value: &[u8]) -> io::Result<u64> {
        self.runtime()?.data_plane()?.put(namespace_id, key, value)
    }

    #[cfg(test)]
    pub(crate) fn get_value(
        &self,
        namespace_id: u32,
        key: &[u8],
    ) -> io::Result<Option<HybridValueRead>> {
        self.runtime()?.data_plane()?.get(namespace_id, key)
    }

    pub(crate) async fn get_value_async(
        &self,
        namespace_id: u32,
        key: &[u8],
        tokio_handle: &tokio::runtime::Handle,
    ) -> io::Result<Option<HybridValueRead>> {
        self.runtime()?
            .data_plane()?
            .get_async(namespace_id, key, tokio_handle)
            .await
    }

    pub(crate) fn delete_value(&self, namespace_id: u32, key: &[u8]) -> io::Result<u64> {
        self.runtime()?.data_plane()?.delete(namespace_id, key)
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

impl FileRegionCore {
    pub(crate) const fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub(crate) const fn index_slot_count(&self) -> usize {
        self.index.storage().slot_count()
    }

    pub(crate) fn runtime_reserved_memory_bytes(&self) -> io::Result<usize> {
        let region_count = u32::try_from(self.manager.lock()?.regions().len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "Region count is too large")
        })?;
        runtime_fixed_memory_bytes(self.index.storage().slot_count(), region_count)?
            .checked_add(self.layout.memory_bytes())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "memory plan overflow"))
    }

    pub(crate) fn append_shard(&self, namespace_id: u32, hash: u64) -> usize {
        self.layout.append_shard(namespace_id, hash)
    }

    pub(crate) fn region_set_snapshots(&self) -> io::Result<Box<[RegionSetSnapshot]>> {
        self.manager
            .lock()?
            .region_set_snapshots()
            .map_err(region_metadata_io_error)
    }

    pub(crate) fn index_snapshot(&self) -> io::Result<CacheIndexSnapshot> {
        self.index.snapshot().map_err(index_storage_io_error)
    }

    pub(crate) fn set_index_statistics_enabled(&self, enabled: bool) {
        self.index.set_statistics_enabled(enabled);
    }

    pub(crate) fn enter_miss_only(&self) {
        self.health.enter_miss_only();
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.health.is_healthy()
    }

    fn lock_shard_mutation(&self, shard_id: usize) -> io::Result<MutexGuard<'_, ()>> {
        let shard = self.shard(shard_id)?;
        self.lock_shard_gate(&shard.mutation)
    }

    fn shard(&self, shard_id: usize) -> io::Result<&RegionShard> {
        self.shards.get(shard_id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "data shard is out of bounds")
        })
    }

    fn lock_shard_gate<'a>(&self, gate: &'a Mutex<()>) -> io::Result<MutexGuard<'a, ()>> {
        self.health.require_healthy()?;
        gate.lock().map_err(|_| {
            self.health.enter_miss_only();
            io::Error::new(io::ErrorKind::InvalidData, "data shard gate is poisoned")
        })
    }

    /// The first production-facing operation intentionally exposes only typed
    /// point semantics. A lazy image failure latches the whole L2 miss-only;
    /// it is never surfaced as a cache hit or allowed to authorize CLEAN.
    #[cfg(test)]
    fn lookup_snapshot(&self, hash: u64) -> io::Result<Option<IndexEntry>> {
        if !self.health.is_healthy() {
            return Ok(None);
        }
        match self.index.lookup_raw(hash) {
            Ok(Some(entry)) if self.health.is_healthy() => Ok(Some(entry)),
            Ok(result) if self.health.is_healthy() => Ok(result),
            Ok(_) => Ok(None),
            Err(_) => {
                self.health.enter_miss_only();
                Ok(None)
            }
        }
    }

    /// Begins one physical read with a single bounded index lookup.
    fn begin_point_read(&self, hash: u64) -> Option<IndexEntry> {
        if !self.health.is_healthy() {
            return None;
        }
        let entry = match self.index.lookup_raw(hash) {
            Ok(Some(entry)) => entry,
            Ok(None) => return None,
            Err(IndexStorageError::PageBusy { .. } | IndexStorageError::PartitionBusy { .. }) => {
                return None;
            }
            Err(_) => {
                self.health.enter_miss_only();
                return None;
            }
        };
        Some(entry)
    }

    /// Begins one durable value read. The aligned read buffer becomes the value
    /// owner on a hit, avoiding a second payload copy.
    pub(crate) fn begin_value_read(&self, hash: u64, namespace_id: u32) -> Option<IndexEntry> {
        self.begin_point_read(hash).filter(|entry| {
            self.layout
                .region_belongs_to_namespace(namespace_id, entry.location.region_id())
        })
    }

    #[cfg(test)]
    pub(crate) fn read_value(
        &self,
        engine: &dyn IoEngine,
        geometry: crate::recovery::DataGeometry,
        buffer: BufferLease,
        hash_seed: u64,
        namespace_id: u32,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let hash = hash_namespaced_key(hash_seed, namespace_id, key);
        let Some(entry) = self.begin_value_read(hash, namespace_id) else {
            return Ok(None);
        };
        let plan = plan_read(geometry, hash, entry)?;
        let slot = engine.try_reserve_read()?;
        self.read_value_from_plan(engine, slot, buffer, plan, namespace_id, key)
    }

    #[cfg(test)]
    pub(crate) fn read_value_from_plan(
        &self,
        engine: &dyn IoEngine,
        slot: ReadSlot,
        buffer: BufferLease,
        plan: ReadPlan,
        namespace_id: u32,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let pending = self.submit_value_read_from_plan(engine, slot, buffer, plan)?;
        let completion = pending.wait(engine);
        self.finish_value_read(completion, namespace_id, key)
    }

    pub(crate) fn submit_value_read_from_plan(
        &self,
        engine: &dyn IoEngine,
        slot: ReadSlot,
        buffer: BufferLease,
        plan: ReadPlan,
    ) -> io::Result<PendingRead> {
        match submit_read(engine, slot, plan, buffer) {
            Ok(pending) => Ok(pending),
            Err(error) => {
                if !matches!(
                    error.kind(),
                    io::ErrorKind::OutOfMemory
                        | io::ErrorKind::WouldBlock
                        | io::ErrorKind::TimedOut
                        | io::ErrorKind::Interrupted
                ) {
                    self.health.enter_miss_only();
                }
                Err(error)
            }
        }
    }

    pub(crate) fn finish_value_read(
        &self,
        completion: ReadCompletion,
        namespace_id: u32,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let hash = completion.plan.hash;
        let entry = completion.plan.entry;
        if let Err(error) = completion.result {
            self.health.enter_miss_only();
            return Err(error);
        }
        let record = completion.record_bytes().ok_or_else(|| {
            self.health.enter_miss_only();
            io::Error::new(
                io::ErrorKind::InvalidData,
                "record completion lost its exact record slice",
            )
        })?;
        let Some(header) = record
            .get(..RECORD_HEADER_SIZE)
            .and_then(RecordHeader::decode)
        else {
            return Ok(None);
        };
        if header.record_len != entry.location.record_len()
            || header.seqno != entry.seqno
            || header.key_hash != hash
        {
            return Ok(None);
        }
        let key_len = header.key_len as usize;
        let value_len = header.value_len as usize;
        let Some(payload_end) = RECORD_HEADER_SIZE
            .checked_add(key_len)
            .and_then(|end| end.checked_add(value_len))
            .filter(|end| *end <= record.len())
        else {
            return Ok(None);
        };
        let encoded_key = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + key_len];
        if header.namespace_id != namespace_id || encoded_key != key {
            return Ok(None);
        }
        if crc32c(&record[RECORD_HEADER_SIZE..payload_end]) != header.payload_crc {
            return Ok(None);
        }
        let value_start = completion.plan.record_range.start + RECORD_HEADER_SIZE + key_len;
        let Some(value_end) = value_start.checked_add(value_len) else {
            return Ok(None);
        };
        let Some(buffer) = completion.buffer else {
            self.health.enter_miss_only();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "validated Region read lost its owned buffer",
            ));
        };
        Ok(Some(RegionValueRead {
            buffer,
            buffer_len: completion.plan.aligned_len,
            value_range: value_start..value_end,
            seqno: header.seqno,
        }))
    }

    /// Preflights, reserves, and encodes one value directly into a shard's
    /// aligned fill buffer. Region reservation and open-span accounting share
    /// one manager try-lock; the shard mutation gate then protects encoding
    /// without retaining global authority. This method performs no device I/O
    /// and never publishes an index entry.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_stage_value(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        hash: u64,
        record_bytes: u32,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<RegionStageValue> {
        self.try_stage_record(
            staging,
            shard_id,
            hash,
            record_bytes,
            namespace_id,
            key,
            value,
            StagedRecordKind::Value,
        )
    }

    pub(crate) fn try_stage_delete(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        hash: u64,
        record_bytes: u32,
        namespace_id: u32,
        key: &[u8],
    ) -> io::Result<RegionStageValue> {
        self.try_stage_record(
            staging,
            shard_id,
            hash,
            record_bytes,
            namespace_id,
            key,
            &[],
            StagedRecordKind::Tombstone,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_stage_record(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        hash: u64,
        record_bytes: u32,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        kind: StagedRecordKind,
    ) -> io::Result<RegionStageValue> {
        self.health.require_healthy()?;
        if record_bytes as usize > staging.chunk_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "value exceeds one fixed write buffer",
            ));
        }
        let _shard_mutation = self.lock_shard_mutation(shard_id)?;
        match staging.preflight_append(shard_id, record_bytes) {
            Ok(StageAppend::Appended) => {}
            Ok(StageAppend::NeedsSeal) => return Ok(RegionStageValue::NeedsProgress),
            Err(StagingError::WouldBlock) => {
                return Ok(RegionStageValue::NeedsProgress);
            }
            Err(error) => {
                self.health.enter_miss_only();
                staging.close();
                return Err(staging_io_error(error));
            }
        }
        let receipt = {
            let Some(mut manager) = self.manager.try_lock()? else {
                return Ok(RegionStageValue::NeedsProgress);
            };
            let receipt = match manager.reserve_append(shard_id, record_bytes) {
                Ok(receipt) => receipt,
                Err(RegionMutationError::WouldBlock) => {
                    return Ok(RegionStageValue::NeedsProgress);
                }
                Err(RegionMutationError::FlushBeforeRotation) => {
                    return Ok(RegionStageValue::NeedsProgress);
                }
                Err(RegionMutationError::RegionFull) => {
                    return Ok(RegionStageValue::NeedsRotation);
                }
                Err(error) => {
                    self.health.enter_miss_only();
                    return Err(region_mutation_io_error(error));
                }
            };
            if let Err(error) = manager.stage_reservation(receipt) {
                self.health.enter_miss_only();
                staging.close();
                return Err(region_mutation_io_error(error));
            }
            receipt
        };

        let staged = staging.encode_reserved(receipt, |destination| {
            let entry = encode_value_into_hashed(
                destination,
                receipt,
                hash,
                record_bytes,
                namespace_id,
                key,
                value,
            )?;
            Ok::<StagedRecord, RecordEncodeError>(StagedRecord::new(hash, entry, kind))
        });
        match staged {
            Ok(StageAppend::Appended) => Ok(RegionStageValue::Staged(receipt.seqno)),
            Ok(StageAppend::NeedsSeal) => self.fail_preflighted_stage(
                staging,
                "staging capacity changed after successful preflight",
            ),
            Err(StagingEncodeError::Encode(error)) => {
                self.health.enter_miss_only();
                staging.close();
                Err(record_encode_io_error(error))
            }
            Err(StagingEncodeError::Staging(StagingError::WouldBlock)) => {
                self.fail_preflighted_stage(staging, "staging became busy after preflight")
            }
            Err(StagingEncodeError::Staging(error)) => {
                self.health.enter_miss_only();
                staging.close();
                Err(staging_io_error(error))
            }
        }
    }

    fn fail_preflighted_stage(
        &self,
        staging: &RegionStaging,
        message: &'static str,
    ) -> io::Result<RegionStageValue> {
        self.health.enter_miss_only();
        staging.close();
        Err(io::Error::new(io::ErrorKind::InvalidData, message))
    }

    /// Shard-worker kernel for one complete staging span. It performs no sync:
    /// RUNNING recovery is safe-empty, and the only durability barrier is the
    /// later CLEAN data sync. Index entries become visible only after the
    /// exact owned-buffer write completion succeeds.
    pub(crate) fn flush_staging_shard(
        &self,
        staging: &RegionStaging,
        engine: &dyn IoEngine,
        shard_id: usize,
    ) -> io::Result<Option<crate::region_manager::RegionWriteSpan>> {
        let shard_mutation = self.lock_shard_mutation(shard_id)?;
        let geometry_for = |manager: &RegionManager| {
            let region_count = u32::try_from(manager.regions().len()).map_err(|_| {
                self.health.enter_miss_only();
                io::Error::new(io::ErrorKind::InvalidData, "Region count is too large")
            })?;
            let data_file_len = crate::recovery::DataGeometry::expected_file_len(
                manager.region_size(),
                region_count,
            )
            .ok_or_else(|| {
                self.health.enter_miss_only();
                io::Error::new(io::ErrorKind::InvalidData, "data geometry overflow")
            })?;
            Ok::<_, io::Error>(crate::recovery::DataGeometry {
                data_file_len,
                region_size: manager.region_size(),
                region_count,
            })
        };

        // An already aligned span is sealed under this first manager guard. A
        // non-zero tail receipt remains a shard fence while staging rewrites its
        // last record outside the manager lock.
        let (padding, sealed) = {
            let mut manager = self.manager.lock()?;
            let padding = match manager.reserve_write_padding(shard_id) {
                Ok(padding) => padding,
                Err(RegionMutationError::WouldBlock) => return Ok(None),
                Err(error) => {
                    self.health.enter_miss_only();
                    return Err(region_mutation_io_error(error));
                }
            };
            let sealed = if padding.is_none() {
                let span = manager.seal_write_span(shard_id).map_err(|error| {
                    self.health.enter_miss_only();
                    region_mutation_io_error(error)
                })?;
                Some((span, geometry_for(&manager)?))
            } else {
                None
            };
            (padding, sealed)
        };
        let (span, geometry) = if let Some(sealed) = sealed {
            sealed
        } else {
            let padding = padding.ok_or_else(|| {
                self.health.enter_miss_only();
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "padding receipt disappeared before sealing",
                )
            })?;
            if let Err(error) = staging.apply_write_padding(padding) {
                self.health.enter_miss_only();
                staging.close();
                return Err(staging_io_error(error));
            }
            let mut manager = self.manager.lock()?;
            let span = match manager.seal_write_span_with_padding(padding) {
                Ok(span) => span,
                Err(error) => {
                    drop(manager);
                    self.health.enter_miss_only();
                    staging.close();
                    return Err(region_mutation_io_error(error));
                }
            };
            let geometry = geometry_for(&manager)?;
            (span, geometry)
        };

        let job = match staging.take_sealed(span) {
            Ok(Some(job)) => job,
            Ok(None) => {
                self.health.enter_miss_only();
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manager sealed a span with no staging bytes",
                ));
            }
            Err(error) => {
                self.health.enter_miss_only();
                return Err(staging_io_error(error));
            }
        };
        drop(shard_mutation);
        let StagedWrite {
            span,
            buffer,
            absolute,
            records,
        } = job;
        let flight = match submit_span(engine, geometry, span, buffer, absolute) {
            Ok(flight) => flight,
            Err(error) => {
                let original = error.error;
                self.fail_staged_span(staging, span, error.buffer, records);
                return Err(original);
            }
        };
        let completion = flight.wait(engine);
        let crate::region_appender::RegionSpanCompletion {
            span,
            result,
            buffer,
        } = completion;
        if let Err(error) = result {
            self.fail_staged_span(staging, span, buffer, records);
            return Err(error);
        }
        let Some(buffer) = buffer else {
            self.fail_staged_span(staging, span, None, records);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "span completion lost its owned buffer",
            ));
        };

        {
            let mut manager = match self.manager.lock() {
                Ok(manager) => manager,
                Err(error) => {
                    self.fail_staged_span(staging, span, Some(buffer), records);
                    return Err(error);
                }
            };
            if let Err(error) = manager.complete_write_span(span) {
                self.health.enter_miss_only();
                drop(manager);
                self.fail_staged_span(staging, span, Some(buffer), records);
                return Err(region_mutation_io_error(error));
            }
        }

        if let Err(error) = self.publish_completed_records(records.as_slice()) {
            self.fail_staged_span(staging, span, Some(buffer), records);
            return Err(error);
        }
        if let Err(error) = staging.finish_success(span, buffer, records) {
            self.health.enter_miss_only();
            return Err(staging_io_error(error));
        }
        Ok(Some(span))
    }

    /// Rotates one empty data shard. Concurrent reads validate the returned
    /// record identity and may observe either valid generation.
    pub(crate) fn rotate_shard(&self, shard_id: usize) -> io::Result<bool> {
        self.health.require_healthy()?;
        let shard_mutation = self.lock_shard_mutation(shard_id)?;
        let set_index = self.layout.set_index_for_shard(shard_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "rotation shard is outside the RegionSet layout",
            )
        })?;
        let rotation = self.rotation[set_index].lock().map_err(|_| {
            self.health.enter_miss_only();
            io::Error::new(
                io::ErrorKind::InvalidData,
                "RegionSet rotation gate is poisoned",
            )
        })?;
        let plan = match self.manager.lock()?.plan_rotation(shard_id) {
            Ok(plan) => plan,
            Err(RegionMutationError::WouldBlock) => return Ok(false),
            Err(error) => return Err(region_mutation_context("rotation planning", error)),
        };

        let receipt = self
            .manager
            .lock()?
            .begin_rotation(plan)
            .map_err(|error| region_mutation_context("rotation begin", error))?;
        if !self
            .index
            .publish_region_generation(receipt.activated_region_id, receipt.activated_created_seqno)
        {
            self.health.enter_miss_only();
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rotation activated an untracked Region generation",
            ));
        }
        // Manager authority now carries the exact in-progress rotation
        // receipt, so foreground staging fails fast on this shard. Release the
        // shard gates before publishing the completed in-memory rotation.
        drop(rotation);
        drop(shard_mutation);

        if let Err(error) = self.manager.lock()?.finish_rotation(receipt) {
            self.health.enter_miss_only();
            return Err(region_mutation_context("rotation completion", error));
        }
        Ok(true)
    }

    /// Publishes a completed batch without entering global Region authority.
    /// A delayed older completion cannot replace a newer same-hash sequence.
    fn publish_completed_records(&self, records: &[StagedRecord]) -> io::Result<()> {
        for record in records.iter().copied() {
            let entry = record.entry();
            let published = match record.kind() {
                StagedRecordKind::Value => self.index.upsert(record.hash(), entry),
                StagedRecordKind::Tombstone => {
                    self.index.upsert_tombstone(record.hash(), entry.seqno)
                }
            };
            if let Err(error) = published {
                self.health.enter_miss_only();
                return Err(index_storage_io_error(error));
            }
        }
        Ok(())
    }

    fn fail_staged_span(
        &self,
        staging: &RegionStaging,
        span: crate::region_manager::RegionWriteSpan,
        buffer: Option<crate::io_engine::IoBuffer>,
        records: Vec<StagedRecord>,
    ) {
        self.health.enter_miss_only();
        let _ = staging.finish_failure(span, buffer, records);
    }
}

pub(crate) fn runtime_fixed_memory_bytes(
    index_slots: usize,
    region_count: u32,
) -> io::Result<usize> {
    let slots = u64::try_from(index_slots)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "index capacity is too large"))?;
    let index_bytes = recovery_image_index_len(slots)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "index memory does not fit the platform",
            )
        })?;
    let region_count = usize::try_from(region_count)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Region count is too large"))?;
    // Manager, read-directory, shard gates, and FIFO nodes are all fixed by
    // Region count. Charge a conservative constant per Region instead of
    // exposing allocator-specific layout as a tuning surface.
    let region_bytes = region_count
        .checked_mul(256)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "memory size overflow"))?;
    let partition_count = canonical_index_partition_ranges(index_slots)
        .map_err(index_storage_io_error)?
        .len();
    let metadata_pages = 1_usize
        .checked_add(region_count.div_ceil(REGION_METADATA_REGIONS_PER_PAGE))
        .and_then(|pages| {
            pages.checked_add(partition_count.div_ceil(REGION_METADATA_PARTITIONS_PER_PAGE))
        })
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "metadata size overflow"))?;
    let metadata_bytes = metadata_pages
        .checked_mul(REGION_METADATA_PAGE_SIZE)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "metadata size overflow"))?;
    // Warm close may hold an encoded metadata image, its cloned record tables,
    // and the fixed sequential write batch at once. Two encoded lengths are a
    // conservative bound for the first two components. The same reservation
    // also covers owned metadata decoding during warm open.
    let recovery_scratch = metadata_bytes
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(WARM_IMAGE_WRITE_BATCH_BYTES))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "recovery memory overflow"))?;
    index_bytes
        .checked_add(region_bytes)
        .and_then(|bytes| bytes.checked_add(recovery_scratch))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "memory size overflow"))
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
    region_layout: Arc<RegionLayout>,
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

    #[cfg(test)]
    pub(crate) fn new_with_configs(
        files: RegionFiles,
        format_data: DataSuperblock,
        shards: u32,
        runtime_config: RuntimeConfig,
    ) -> Self {
        let region_layout =
            RegionLayout::single_unchecked(format_data.geometry.region_count, shards);
        Self::new_with_region_layout(files, format_data, region_layout, runtime_config)
    }

    pub(crate) fn new_with_region_layout(
        files: RegionFiles,
        format_data: DataSuperblock,
        region_layout: RegionLayout,
        runtime_config: RuntimeConfig,
    ) -> Self {
        Self::new_with_file_system_and_configs(
            files,
            format_data,
            SystemRegionFileSystem,
            Arc::new(region_layout),
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
            Arc::new(RegionLayout::single_unchecked(
                format_data.geometry.region_count,
                REGION_SHARDS,
            )),
            RuntimeConfig::default(),
        )
    }

    fn new_with_file_system_and_configs(
        files: RegionFiles,
        format_data: DataSuperblock,
        file_system: F,
        region_layout: Arc<RegionLayout>,
        runtime_config: RuntimeConfig,
    ) -> Self {
        Self {
            files,
            format_data,
            region_layout,
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
        if self.region_layout.shard_count() == 0
            || self.format_data.geometry.region_count != self.region_layout.region_count()
            || self.format_data.geometry.region_count <= self.region_layout.shard_count()
        {
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
        let recovery_state = match latest_state([&pages[0], &pages[1]]) {
            Ok(selected) => selected,
            Err(StateSelectionError::ConflictingGeneration(_)) => None,
            Err(StateSelectionError::UnsupportedVersion { .. }) => None,
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
        let header = match RecoveryImageHeader::probe(&header_page) {
            RecoveryImageHeaderProbe::Valid(header) => header,
            RecoveryImageHeaderProbe::Unsupported(_) => return Ok(RecoveryPlan::Running),
            RecoveryImageHeaderProbe::Empty
            | RecoveryImageHeaderProbe::Corrupt
            | RecoveryImageHeaderProbe::Unrecognized
            | RecoveryImageHeaderProbe::Truncated => return Ok(RecoveryPlan::Running),
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
        ) || header.region_table_len > maximum_region_metadata_len(data.geometry.region_count)?
        {
            return Ok(RecoveryPlan::Running);
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
            return if error.kind() == io::ErrorKind::UnexpectedEof {
                Ok(RecoveryPlan::Running)
            } else {
                Err(error)
            };
        }
        let metadata = match RegionMetadata::decode_owned(metadata_bytes) {
            Ok(metadata) => metadata,
            Err(RegionMetadataError::UnsupportedVersion(_)) => {
                return Ok(RecoveryPlan::Running);
            }
            Err(RegionMetadataError::Allocation) => {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot decode Region metadata",
                ));
            }
            Err(_) => return Ok(RecoveryPlan::Running),
        };
        if !metadata.matches_image(data, header)
            || metadata.root.shard_count != self.region_layout.shard_count()
        {
            return Ok(RecoveryPlan::Running);
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
        let metadata = empty_region_metadata_with_layout(data, index_slots, &self.region_layout)?;
        let index =
            PartitionedIndexStorage::anonymous(index_slots).map_err(index_storage_io_error)?;
        let runtime = FileRegionRuntime::install_with_layout(
            index,
            metadata,
            Arc::clone(&self.region_layout),
        )?;
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
            && clean.metadata.root.shard_count == self.region_layout.shard_count()
            && clean.metadata.validate().is_ok();
        if !eligible {
            self.cold_reset_needed = true;
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
        let runtime = FileRegionRuntime::install_with_layout(
            index,
            clean.metadata,
            Arc::clone(&self.region_layout),
        )?;
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
            layout: _,
            shards,
            rotation,
            health,
        } = runtime.into_core()?;
        if shards.iter().any(|shard| shard.mutation.is_poisoned())
            || rotation.iter().any(Mutex::is_poisoned)
        {
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

#[cfg(test)]
fn empty_region_metadata(
    data: DataSuperblock,
    index_slots: usize,
    shards: u32,
) -> io::Result<RegionMetadata> {
    let layout = RegionLayout::single(data.geometry.region_count, shards)?;
    empty_region_metadata_with_layout(data, index_slots, &layout)
}

fn empty_region_metadata_with_layout(
    data: DataSuperblock,
    index_slots: usize,
    layout: &RegionLayout,
) -> io::Result<RegionMetadata> {
    let shards = layout.shard_count();
    if shards == 0
        || data.geometry.region_count <= shards
        || data.geometry.region_count != layout.region_count()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RegionStore layout does not match its Region geometry",
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
        let set_index = layout.set_index_for_region(region_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "RegionSet ranges do not cover the data geometry",
            )
        })?;
        let set = layout.sets()[set_index];
        let local_region = region_id - set.first_region;
        let active = local_region < set.shard_count;
        let shard_id = set.first_shard + local_region;
        let queue_ordinal = if active {
            shard_id
        } else {
            let ordinal = free_ordinal;
            free_ordinal = free_ordinal
                .checked_add(1)
                .ok_or_else(|| io::Error::other("free Region ordinal overflow"))?;
            ordinal
        };
        regions.push(RegionMetadataRecord {
            region_id,
            incarnation: u32::from(active),
            state: if active {
                RegionMetadataState::Active
            } else {
                RegionMetadataState::Free
            },
            queue_ordinal,
            created_seqno: if active { u64::from(shard_id) + 1 } else { 0 },
            durable_used_offset: 0,
            max_seqno: 0,
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

fn region_metadata_io_error(error: RegionMetadataError) -> io::Error {
    match error {
        RegionMetadataError::Allocation => io::Error::new(io::ErrorKind::OutOfMemory, error),
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn region_mutation_io_error(error: RegionMutationError) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("RegionStore authority mutation failed: {error:?}"),
    )
}

fn region_mutation_context(context: &'static str, error: RegionMutationError) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("RegionStore {context} failed: {error:?}"),
    )
}

fn record_encode_io_error(error: RecordEncodeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error)
}

fn staging_io_error(error: StagingError) -> io::Error {
    let kind = match error {
        StagingError::Closed => io::ErrorKind::BrokenPipe,
        StagingError::WouldBlock => io::ErrorKind::WouldBlock,
        StagingError::InvalidShard => io::ErrorKind::InvalidInput,
        _ => io::ErrorKind::InvalidData,
    };
    io::Error::new(kind, error.to_string())
}

fn index_storage_io_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

fn guarded_index_result<T>(
    health: &RegionHealthLatch,
    result: Result<T, IndexStorageError>,
) -> io::Result<T> {
    result.map_err(|error| {
        health.enter_miss_only();
        index_storage_io_error(error)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_storage::{INDEX_IMAGE_SLOTS_PER_PAGE, IndexSlot};
    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};
    use crate::io_engine::BackendIoEngine;
    use crate::recovery::{DataGeometry, PersistentId};
    use crate::resources::{ResourceController, ResourceLimits};
    #[cfg(unix)]
    use std::os::unix::process::ExitStatusExt;
    #[cfg(unix)]
    use std::process::{Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    fn eventually_admitted<T>(mut put: impl FnMut() -> io::Result<T>) -> T {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match put() {
                Ok(value) => return value,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "write buffer did not make progress"
                    );
                    std::thread::yield_now();
                }
                Err(error) => panic!("cache write failed: {error}"),
            }
        }
    }

    struct TestDirectory {
        root: PathBuf,
        files: RegionFiles,
    }

    impl TestDirectory {
        fn new() -> Self {
            let ordinal = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir()
                .join(format!("cache-rs-region-{}-{ordinal}", std::process::id()));
            let _ = std::fs::remove_dir_all(&root);
            std::fs::create_dir(&root).unwrap();
            let files = RegionFiles::new(
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

    #[cfg(unix)]
    #[test]
    fn state_page_reads_stop_after_the_interrupted_retry_budget() {
        let directory = TestDirectory::new();
        let (state, faults) = FaultBackend::open(&directory.files.state).unwrap();
        state.set_len(STATE_FILE_SIZE as u64).unwrap();
        faults.arm(FaultEvent::Read, 1, FaultAction::ErrorAlways(libc::EINTR));

        let error = read_state_pages(&state).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(
            faults
                .events()
                .iter()
                .filter(|event| **event == FaultEvent::Read)
                .count(),
            crate::io_backend::MAX_INTERRUPTED_RETRIES + 1
        );
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
    struct FaultRegionFileSystem {
        io: FaultHandle,
        file_system: FileSystemFaultHandle,
    }

    impl FaultRegionFileSystem {
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

    impl RegionFileSystem for FaultRegionFileSystem {
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
            SystemRegionFileSystem.sync_parent(path)
        }
    }

    fn persistent_id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn test_data_superblock_with_regions(region_count: u32) -> DataSuperblock {
        let region_size = 2 * RECOVERY_PAGE_SIZE as u64;
        DataSuperblock {
            generation: 1,
            cache_uuid: persistent_id(1),
            data_identity: persistent_id(2),
            geometry: DataGeometry {
                data_file_len: DataGeometry::expected_file_len(region_size, region_count).unwrap(),
                region_size,
                region_count,
            },
            hash_seed: 3,
            config_fingerprint: 4,
        }
    }

    fn test_data_superblock() -> DataSuperblock {
        test_data_superblock_with_regions(REGION_SHARDS + 1)
    }

    fn data_path_superblock() -> DataSuperblock {
        let region_size = 8 * 1024 * 1024;
        DataSuperblock {
            geometry: DataGeometry {
                data_file_len: DataGeometry::expected_file_len(region_size, REGION_SHARDS + 1)
                    .unwrap(),
                region_size,
                region_count: REGION_SHARDS + 1,
            },
            ..test_data_superblock()
        }
    }

    fn data_path_resources() -> ResourceController {
        ResourceController::try_new(ResourceLimits {
            memory_limit_bytes: 32 * 1024 * 1024,
            reserved_memory_bytes: 0,
        })
        .unwrap()
    }

    #[cfg(unix)]
    #[test]
    fn external_process_kill_recovery_contract() {
        const CHILD_CASE: &str = "CACHE_RS_CRASH_CHILD_CASE";
        const CHILD_ROOT: &str = "CACHE_RS_CRASH_CHILD_ROOT";

        if let Ok(case) = std::env::var(CHILD_CASE) {
            let root = PathBuf::from(std::env::var_os(CHILD_ROOT).expect("child root is set"));
            let files = RegionFiles::new(
                root.join("data"),
                root.join("state"),
                root.join("recovery.image"),
            );
            run_crash_child(&case, files);
        }

        for (case, expect_clean) in [
            ("open", false),
            ("write", false),
            ("drain", false),
            ("warm-data", false),
            ("warm-image", false),
            ("clean-state", true),
        ] {
            let directory = TestDirectory::new();
            let data = data_path_superblock();
            let mut initial =
                RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data))
                    .unwrap();
            initial.put_value(7, b"survivor", b"old").unwrap();
            initial.drain().unwrap();
            initial.close_warm().unwrap();

            let status = Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("region::tests::external_process_kill_recovery_contract")
                .arg("--nocapture")
                .env(CHILD_CASE, case)
                .env(CHILD_ROOT, &directory.root)
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert_eq!(
                status.signal(),
                Some(9),
                "crash case {case} did not SIGKILL"
            );

            let mut reopened =
                RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data))
                    .unwrap();
            if expect_clean {
                assert_eq!(reopened.startup(), StartupMode::Warm, "{case}");
                assert_eq!(
                    reopened.get_value(7, b"survivor").unwrap().unwrap().value(),
                    b"old",
                    "{case}",
                );
            } else {
                assert_eq!(reopened.startup(), StartupMode::Cold, "{case}");
                assert!(
                    reopened.get_value(7, b"survivor").unwrap().is_none(),
                    "{case}",
                );
            }
            reopened.close_fast().unwrap();
        }
    }

    #[cfg(unix)]
    fn run_crash_child(case: &str, files: RegionFiles) -> ! {
        let data = data_path_superblock();
        match case {
            "open" => {
                let _store = RegionStore::open(4096, FileRegionBackend::new(files, data)).unwrap();
                crate::io_backend::testing::kill_process();
            }
            "write" | "drain" => {
                let store = RegionStore::open(4096, FileRegionBackend::new(files, data)).unwrap();
                store.put_value(7, b"replacement", b"new").unwrap();
                if case == "drain" {
                    store.drain().unwrap();
                }
                crate::io_backend::testing::kill_process();
            }
            "warm-data" | "warm-image" | "clean-state" => {
                let (file_system, faults, _) = FaultRegionFileSystem::new();
                let mut store = RegionStore::open(
                    4096,
                    FileRegionBackend::new_with_file_system(files, data, file_system),
                )
                .unwrap();
                let point = match case {
                    "warm-data" => SyncPoint::WarmData,
                    "warm-image" => SyncPoint::RecoveryImage,
                    "clean-state" => SyncPoint::CleanState,
                    _ => unreachable!(),
                };
                faults.arm(FaultEvent::Sync(point), 1, FaultAction::KillAfter);
                let _ = store.close_warm();
                panic!("crash fault did not terminate the child");
            }
            _ => panic!("unknown crash child case: {case}"),
        }
    }

    fn production_data_superblock(region_size: u64) -> DataSuperblock {
        let region_count = REGION_SHARDS + 1;
        DataSuperblock {
            generation: 1,
            cache_uuid: persistent_id(21),
            data_identity: persistent_id(22),
            geometry: DataGeometry {
                data_file_len: DataGeometry::expected_file_len(region_size, region_count).unwrap(),
                region_size,
                region_count,
            },
            hash_seed: 23,
            config_fingerprint: 24,
        }
    }

    fn key_for_shard(data: DataSuperblock, namespace_id: u32, shard: u64, ordinal: u64) -> Vec<u8> {
        for attempt in 0_u64..10_000 {
            let key = format!("shard-{shard}-object-{ordinal}-{attempt}").into_bytes();
            if hash_namespaced_key(data.hash_seed, namespace_id, &key) % u64::from(REGION_SHARDS)
                == shard
            {
                return key;
            }
        }
        panic!("could not find a deterministic key for shard {shard}");
    }

    #[test]
    fn production_data_plane_reads_mixed_chunks_rotates_and_warm_recovers() {
        let directory = TestDirectory::new();
        let data = production_data_superblock(512 * 1024);
        let namespace_id = 7;
        let runtime_config = RuntimeConfig {
            l1_capacity_bytes: 0,
            ..RuntimeConfig::default()
        };
        let mut store = RegionStore::open(
            4096,
            FileRegionBackend::new_with_configs(
                directory.files.clone(),
                data,
                REGION_SHARDS,
                runtime_config,
            ),
        )
        .unwrap();

        let mixed = [16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024];
        let mut expected = Vec::new();
        for (ordinal, size) in mixed.into_iter().enumerate() {
            let key = key_for_shard(data, namespace_id, ordinal as u64, ordinal as u64);
            let value = vec![(ordinal as u8) + 1; size];
            eventually_admitted(|| store.put_value(namespace_id, &key, &value));
            expected.push((key, value));
        }
        store.drain().unwrap();
        for (key, value) in &expected {
            assert_eq!(
                store.get_value(namespace_id, key).unwrap().unwrap().value(),
                value
            );
        }

        // A 256 KiB record leaves insufficient room for another same-shard
        // record in this test geometry. Repeated writes therefore consume the
        // free Region and then exercise sealed FIFO reuse.
        let rotation_value = vec![0xa5; 256 * 1024];
        let mut recent = Vec::new();
        let rotations_before = store.detailed_snapshot().unwrap().region_sets[0].rotations;
        for ordinal in 0..32 {
            let key = key_for_shard(data, namespace_id, 0, 100 + ordinal);
            eventually_admitted(|| store.put_value(namespace_id, &key, &rotation_value));
            store.drain().unwrap();
            recent.push(key);
        }
        assert_eq!(
            store.detailed_snapshot().unwrap().region_sets[0].rotations - rotations_before,
            31,
            "one full-Region signal must cause exactly one rotation"
        );
        for key in recent.iter().rev().take(2) {
            assert_eq!(
                store.get_value(namespace_id, key).unwrap().unwrap().value(),
                rotation_value
            );
        }

        let retained_hits: Vec<_> = (0..129)
            .map(|_| {
                store
                    .get_value(namespace_id, recent.last().unwrap())
                    .unwrap()
                    .unwrap()
            })
            .collect();
        assert!(
            retained_hits
                .iter()
                .all(|hit| hit.value() == rotation_value)
        );
        assert!(
            store
                .get_value(namespace_id, b"definite-index-miss")
                .unwrap()
                .is_none(),
            "an index miss must not acquire a retained-hit buffer"
        );

        // Retained exact-size hits own their transient allocations, but cannot
        // pin the runtime operation barrier or prevent a warm shutdown.
        store.close_warm().unwrap();
        assert_eq!(retained_hits[0].value(), rotation_value);
        drop(retained_hits);
        let mut recovered =
            RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data)).unwrap();
        assert_eq!(recovered.startup(), StartupMode::Warm);
        assert_eq!(
            recovered
                .get_value(namespace_id, recent.last().unwrap())
                .unwrap()
                .unwrap()
                .value(),
            rotation_value
        );
        recovered.close_fast().unwrap();
    }

    #[test]
    fn poisoned_runtime_gates_stop_workers_and_reject_warm_close() {
        for case in ["shard", "index"] {
            let directory = TestDirectory::new();
            let data = production_data_superblock(512 * 1024);
            let runtime_config = RuntimeConfig::default()
                .with_io_engine(crate::runtime_config::IoEngine::Posix)
                .with_io_workers(1)
                .with_l1_capacity(0)
                .with_memory_limit(32 * 1024 * 1024)
                .with_write_batch_size(128 * 1024);
            let mut store = RegionStore::open(
                4096,
                FileRegionBackend::new_with_configs(
                    directory.files.clone(),
                    data,
                    REGION_SHARDS,
                    runtime_config.clone(),
                ),
            )
            .unwrap();

            let runtime = store.runtime().unwrap();
            match case {
                "shard" => runtime.data_plane().unwrap().poison_shard_for_test(0),
                "index" => runtime
                    .core
                    .index
                    .storage()
                    .poison_hash_partition_for_test(0),
                _ => unreachable!(),
            }
            let error = store.close_warm().unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}");

            let mut reopened = RegionStore::open(
                4096,
                FileRegionBackend::new_with_configs(
                    directory.files.clone(),
                    data,
                    REGION_SHARDS,
                    runtime_config,
                ),
            )
            .unwrap();
            assert_eq!(reopened.startup(), StartupMode::Cold, "{case}");
            reopened.close_fast().unwrap();
        }
    }

    #[test]
    fn fresh_metadata_assigns_four_active_shards_and_a_free_victim() {
        let data = test_data_superblock();
        let metadata = empty_region_metadata(data, 64, REGION_SHARDS).unwrap();
        let shards = usize::try_from(REGION_SHARDS).unwrap();

        assert_eq!(metadata.root.shard_count, REGION_SHARDS);
        assert_eq!(metadata.root.active_region_count, REGION_SHARDS);
        assert_eq!(metadata.root.free_region_count, 1);
        assert_eq!(metadata.root.max_seqno, u64::from(REGION_SHARDS));
        for shard_id in 0..REGION_SHARDS {
            let region = metadata.regions[usize::try_from(shard_id).unwrap()];
            assert_eq!(region.state, RegionMetadataState::Active);
            assert_eq!(region.queue_ordinal, shard_id);
            assert_eq!(region.created_seqno, u64::from(shard_id) + 1);
        }
        let free = metadata.regions[shards];
        assert_eq!(free.state, RegionMetadataState::Free);
        assert_eq!(free.queue_ordinal, 0);

        let manager = RegionManager::from_metadata(metadata).unwrap();
        assert_eq!(manager.active_regions(), &[0, 1, 2, 3]);
        assert_eq!(
            manager.free_regions().iter().copied().collect::<Vec<_>>(),
            vec![4]
        );
        assert_eq!(manager.next_seqno(), 5);
    }

    #[test]
    fn foreground_stage_bypasses_busy_manager_without_consuming_a_sequence() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let resources = data_path_resources();
        let staging = RegionStaging::try_new(
            1,
            crate::runtime_config::MAX_WRITE_BATCH_BYTES,
            data.geometry.region_size,
            &resources,
        )
        .unwrap();
        let manager = runtime.manager.inner.lock().unwrap();
        let next_seqno = manager.next_seqno();
        let hash = hash_namespaced_key(data.hash_seed, 7, b"key");
        let record_bytes = required_record_bytes(b"key".len(), b"value".len()).unwrap();

        assert_eq!(
            runtime
                .try_stage_value(&staging, 0, hash, record_bytes, 7, b"key", b"value")
                .unwrap(),
            RegionStageValue::NeedsProgress
        );
        assert_eq!(manager.next_seqno(), next_seqno);
    }

    #[test]
    fn completed_record_publication_does_not_enter_region_manager() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let core = Arc::clone(&runtime.core);
        let manager = core.manager.inner.lock().unwrap();
        let record = StagedRecord::new(
            7,
            IndexEntry {
                location: crate::index::PackedLocation::new(0, 0, 64).unwrap(),
                seqno: 1,
            },
            StagedRecordKind::Value,
        );
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let publisher_core = Arc::clone(&core);
        let publisher = std::thread::spawn(move || {
            sender
                .send(publisher_core.publish_completed_records(&[record]))
                .unwrap();
        });

        let published = receiver.recv_timeout(std::time::Duration::from_secs(1));
        drop(manager);
        publisher.join().unwrap();
        published.unwrap().unwrap();
        assert_eq!(runtime.lookup_snapshot(7).unwrap(), Some(record.entry()));
    }

    #[test]
    fn completed_owned_span_publishes_index_without_a_steady_state_sync() {
        let data = data_path_superblock();
        let index_slots = INDEX_IMAGE_SLOTS_PER_PAGE * 4;
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(index_slots).unwrap(),
            empty_region_metadata(data, index_slots, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let resources = data_path_resources();
        let staging = RegionStaging::try_new(
            1,
            crate::runtime_config::MAX_WRITE_BATCH_BYTES,
            data.geometry.region_size,
            &resources,
        )
        .unwrap();
        let directory = TestDirectory::new();
        let (backend, faults) = FaultBackend::open(&directory.files.data).unwrap();
        backend.set_len(data.geometry.data_file_len).unwrap();
        let engine = BackendIoEngine::new(Arc::new(backend), 2).unwrap();
        let value = vec![0x5a; 16 * 1024];
        let mut first = None;
        let mut last = None;
        let mut staged_records = 0_u64;
        loop {
            let key = format!("file/chunk/{staged_records:04}");
            let hash = hash_namespaced_key(data.hash_seed, 7, key.as_bytes());
            let record_bytes = required_record_bytes(key.len(), value.len()).unwrap();
            match runtime
                .try_stage_value(&staging, 0, hash, record_bytes, 7, key.as_bytes(), &value)
                .unwrap()
            {
                RegionStageValue::Staged(seqno) => {
                    if first.is_none() {
                        first = Some((key.clone(), hash, seqno));
                    }
                    last = Some((key, hash, seqno));
                    staged_records += 1;
                }
                RegionStageValue::NeedsProgress => break,
                outcome => panic!("unexpected staging outcome: {outcome:?}"),
            }
        }
        let (first_key, first_hash, first_seqno) =
            first.expect("4 MiB span must contain target-size records");
        let (last_key, last_hash, last_seqno) =
            last.expect("4 MiB span must retain its final record");
        assert!(staged_records > 240);
        assert_eq!(runtime.lookup_snapshot(first_hash).unwrap(), None);

        let published = runtime
            .flush_staging_shard(&staging, &engine, 0)
            .unwrap()
            .unwrap();
        assert_eq!(
            (published.end_offset - published.start_offset) % RECOVERY_PAGE_SIZE as u64,
            0
        );
        let Some(entry) = runtime.lookup_snapshot(first_hash).unwrap() else {
            panic!("completed first record must be published");
        };
        assert_eq!(entry.seqno, first_seqno);
        assert_eq!(
            entry.location.record_len(),
            required_record_bytes(first_key.len(), value.len()).unwrap()
        );
        assert_ne!(entry.location.record_len() % RECOVERY_PAGE_SIZE as u32, 0);
        let Some(last_entry) = runtime.lookup_snapshot(last_hash).unwrap() else {
            panic!("completed final record must be published");
        };
        assert_eq!(last_entry.seqno, last_seqno);
        assert!(
            last_entry.location.record_len()
                > required_record_bytes(last_key.len(), value.len()).unwrap()
        );
        assert_eq!(
            u64::from(last_entry.location.offset()) + u64::from(last_entry.location.record_len()),
            published.end_offset
        );
        let read = runtime
            .begin_point_read(first_hash)
            .expect("completed entry must plan a Region read");
        assert_eq!(read, entry);

        let read_buffer_bytes = (last_entry.location.record_len() as usize)
            .div_ceil(RECOVERY_PAGE_SIZE)
            * RECOVERY_PAGE_SIZE;
        let memory_before_read = resources.managed_memory_snapshot().current_bytes;
        let hit = runtime
            .read_value(
                &engine,
                data.geometry,
                resources.try_read_buffer(read_buffer_bytes).unwrap(),
                data.hash_seed,
                7,
                last_key.as_bytes(),
            )
            .unwrap()
            .expect("completed record must validate as a disk hit");
        assert_eq!(hit.value(), value);
        drop(hit);
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            memory_before_read
        );

        assert_eq!(
            faults.events(),
            vec![FaultEvent::Write(WritePoint::Record), FaultEvent::Read]
        );
        assert_eq!(engine.stats().submitted, 2);
        assert_eq!(engine.stats().completed, 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn same_hash_candidate_requires_full_key_and_namespace() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let resources = data_path_resources();
        let staging = RegionStaging::try_new(
            1,
            crate::runtime_config::MAX_WRITE_BATCH_BYTES,
            data.geometry.region_size,
            &resources,
        )
        .unwrap();
        let directory = TestDirectory::new();
        let (backend, _) = FaultBackend::open(&directory.files.data).unwrap();
        backend.set_len(data.geometry.data_file_len).unwrap();
        let engine = BackendIoEngine::new(Arc::new(backend), 1).unwrap();
        let namespace_id = 7;
        let owner_key = b"collision-owner";
        let foreign_key = b"collision-foreign";
        let value = b"owner-value-must-not-leak";
        let owner_hash = hash_namespaced_key(data.hash_seed, namespace_id, owner_key);
        let owner_record_bytes = required_record_bytes(owner_key.len(), value.len()).unwrap();
        let RegionStageValue::Staged(_) = runtime
            .try_stage_value(
                &staging,
                0,
                owner_hash,
                owner_record_bytes,
                namespace_id,
                owner_key,
                value,
            )
            .unwrap()
        else {
            panic!("collision owner must stage");
        };
        runtime
            .flush_staging_shard(&staging, &engine, 0)
            .unwrap()
            .expect("collision owner must publish");

        let Some(entry) = runtime.lookup_snapshot(owner_hash).unwrap() else {
            panic!("collision owner must be indexed after publication");
        };
        let read_buffer_bytes = (entry.location.record_len() as usize).div_ceil(RECOVERY_PAGE_SIZE)
            * RECOVERY_PAGE_SIZE;
        let memory_before_read = resources.managed_memory_snapshot().current_bytes;
        let read_buffer = resources.try_read_buffer(read_buffer_bytes).unwrap();
        let entry = runtime
            .begin_point_read(owner_hash)
            .expect("hash lookup must return the collision candidate");
        let plan = plan_read(data.geometry, owner_hash, entry).unwrap();

        // Supplying a different key after the hash lookup precisely models a
        // 64-bit collision at the L2 record-validation boundary.
        assert!(
            runtime
                .read_value_from_plan(
                    &engine,
                    engine.try_reserve_read().unwrap(),
                    read_buffer,
                    plan,
                    namespace_id,
                    foreign_key,
                )
                .unwrap()
                .is_none()
        );
        assert!(runtime.health.is_healthy());
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            memory_before_read
        );

        let foreign_namespace = namespace_id + 1;
        let read_buffer = resources.try_read_buffer(read_buffer_bytes).unwrap();
        let entry = runtime
            .begin_value_read(owner_hash, foreign_namespace)
            .expect("a same-hash namespace collision must reach record validation");
        let plan = plan_read(data.geometry, owner_hash, entry).unwrap();
        assert!(
            runtime
                .read_value_from_plan(
                    &engine,
                    engine.try_reserve_read().unwrap(),
                    read_buffer,
                    plan,
                    foreign_namespace,
                    owner_key,
                )
                .unwrap()
                .is_none()
        );
        assert!(runtime.health.is_healthy());
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            memory_before_read
        );

        let hit = runtime
            .read_value(
                &engine,
                data.geometry,
                resources.try_read_buffer(read_buffer_bytes).unwrap(),
                data.hash_seed,
                namespace_id,
                owner_key,
            )
            .unwrap()
            .expect("the owning full key must still hit");
        assert_eq!(hit.value(), value);
        drop(hit);
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            memory_before_read
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn failed_span_write_never_publishes_and_latches_miss_only() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let resources = data_path_resources();
        let staging = RegionStaging::try_new(
            1,
            crate::runtime_config::MAX_WRITE_BATCH_BYTES,
            data.geometry.region_size,
            &resources,
        )
        .unwrap();
        let directory = TestDirectory::new();
        let (backend, faults) = FaultBackend::open(&directory.files.data).unwrap();
        backend.set_len(data.geometry.data_file_len).unwrap();
        let engine = BackendIoEngine::new(Arc::new(backend), 1).unwrap();
        let hash = hash_namespaced_key(data.hash_seed, 0, b"key");
        let record_bytes = required_record_bytes(b"key".len(), 16 * 1024).unwrap();
        let RegionStageValue::Staged(_) = runtime
            .try_stage_value(&staging, 0, hash, record_bytes, 0, b"key", &[7; 16 * 1024])
            .unwrap()
        else {
            panic!("value must stage before the injected write failure");
        };
        faults.arm(
            FaultEvent::Write(WritePoint::Record),
            1,
            FaultAction::ErrorAlways(5),
        );

        assert_eq!(
            runtime
                .flush_staging_shard(&staging, &engine, 0)
                .unwrap_err()
                .raw_os_error(),
            Some(5)
        );
        assert!(!runtime.health.is_healthy());
        assert_eq!(runtime.lookup_snapshot(hash).unwrap(), None);
        assert_eq!(runtime.index.lookup_raw(hash).unwrap(), None);
        assert_eq!(
            runtime.manager.inner.lock().unwrap().regions()[0].completed_used,
            0
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn rotation_is_committed_without_a_metadata_io_boundary() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        runtime
            .manager
            .inner
            .lock()
            .unwrap()
            .request_rotation_for_test(0)
            .unwrap();

        assert!(runtime.rotate_shard(0).unwrap());
        assert!(runtime.health.is_healthy());
        let manager = runtime.manager.lock().unwrap();
        assert_eq!(manager.active_regions()[0], REGION_SHARDS);
        assert_eq!(manager.sealed_regions().back(), Some(&0));
    }

    #[test]
    fn reclaim_hides_the_victim_regions_old_index_slots() {
        let data = data_path_superblock();
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
        )
        .unwrap();
        let hash = 19;
        let old = IndexEntry {
            location: crate::index::PackedLocation::new(0, 0, 32).unwrap(),
            seqno: u64::from(REGION_SHARDS),
        };
        assert!(runtime.index.upsert(hash, old).unwrap());

        for expected_hit in [Some(old), None] {
            runtime
                .manager
                .inner
                .lock()
                .unwrap()
                .request_rotation_for_test(0)
                .unwrap();
            assert!(runtime.rotate_shard(0).unwrap());
            assert_eq!(runtime.index.lookup_raw(hash).unwrap(), expected_hit);
        }
        assert_eq!(runtime.manager.lock().unwrap().active_regions()[0], 0);
    }

    #[test]
    fn recovery_watermarks_hide_stale_slots_without_rebuilding_the_index() {
        let data = data_path_superblock();
        let index = PartitionedIndexStorage::anonymous(64).unwrap();
        let hash = 19;
        let stale = IndexEntry {
            location: crate::index::PackedLocation::new(1, 0, 32).unwrap(),
            seqno: 1,
        };
        {
            let mut partition = index.write_hash_partition(hash).unwrap();
            let local_slot = crate::hashing::route_hash(hash, partition.slot_count());
            partition
                .replace_observed(
                    local_slot,
                    crate::index_storage::IndexSlotState::Empty,
                    crate::index_storage::IndexSlotState::Value { hash, entry: stale },
                )
                .unwrap();
        }
        let mut metadata = empty_region_metadata(data, 64, REGION_SHARDS).unwrap();
        metadata.partitions[0].physical_value_slots = 1;

        let runtime = FileRegionRuntime::install(index, metadata).unwrap();

        assert_eq!(runtime.index.lookup_raw(hash).unwrap(), None);
        assert_eq!(
            runtime.index.storage().physical_stats().unwrap(),
            IndexPhysicalStats {
                value: 1,
                deleted: 0,
            }
        );
    }

    #[test]
    fn rotation_wait_in_one_region_set_does_not_block_another_set() {
        let data = test_data_superblock_with_regions(6);
        let layout = RegionLayout::build(
            data.geometry.region_count,
            2,
            &[
                crate::region_layout::RegionSetConfig::new(0),
                crate::region_layout::RegionSetConfig::new(1),
            ],
        )
        .unwrap();
        let other_shard = layout.sets()[1].first_shard as usize;
        let metadata = empty_region_metadata_with_layout(data, 64, &layout).unwrap();
        let runtime = FileRegionRuntime::install_with_layout(
            PartitionedIndexStorage::anonymous(64).unwrap(),
            metadata,
            Arc::new(layout),
        )
        .unwrap();
        runtime
            .manager
            .inner
            .lock()
            .unwrap()
            .request_rotation_for_test(other_shard)
            .unwrap();

        let core = Arc::clone(&runtime.core);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let observed = std::thread::scope(|scope| {
            let held_first_set = runtime.rotation[0].lock().unwrap();
            let worker = scope.spawn(move || {
                sender.send(core.rotate_shard(other_shard)).unwrap();
            });
            let observed = receiver.recv_timeout(Duration::from_secs(1));
            drop(held_first_set);
            worker.join().unwrap();
            observed
        });

        assert!(observed.unwrap().unwrap());
    }

    fn assert_no_runtime_data_write_during_startup(events: &[FaultEvent]) {
        let running_sync = events
            .iter()
            .rposition(|event| *event == FaultEvent::Sync(SyncPoint::RunningState))
            .expect("startup must make RUNNING durable");
        assert!(
            events[running_sync + 1..]
                .iter()
                .all(|event| !matches!(event, FaultEvent::Write(_))),
            "startup must not materialize recovery-only Region state in the data file"
        );
    }

    #[test]
    fn fresh_and_dirty_startup_do_not_write_runtime_region_metadata() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();

        let (fresh_file_system, fresh_io, _) = FaultRegionFileSystem::new();
        let mut fresh = RegionStore::open(
            config,
            FileRegionBackend::new_with_file_system(
                directory.files.clone(),
                data,
                fresh_file_system,
            ),
        )
        .unwrap();
        assert_eq!(fresh.startup(), StartupMode::Cold);
        assert_no_runtime_data_write_during_startup(&fresh_io.events());
        fresh.close_fast().unwrap();

        let (dirty_file_system, dirty_io, _) = FaultRegionFileSystem::new();
        let mut dirty = RegionStore::open(
            config,
            FileRegionBackend::new_with_file_system(
                directory.files.clone(),
                data,
                dirty_file_system,
            ),
        )
        .unwrap();
        assert_eq!(dirty.startup(), StartupMode::Cold);
        assert_no_runtime_data_write_during_startup(&dirty_io.events());
        dirty.close_fast().unwrap();
    }

    #[test]
    fn clean_image_with_a_different_shard_topology_cold_starts_safely() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();

        // Build one otherwise valid CLEAN image with the previous one-shard
        // topology. This exercises the real state/image selection path rather
        // than treating a shard mismatch as malformed recovery metadata.
        let mut metadata = empty_region_metadata(data, config, REGION_SHARDS).unwrap();
        metadata.root.shard_count = 1;
        metadata.root.active_region_count = 1;
        metadata.root.free_region_count = data.geometry.region_count - 1;
        metadata.root.max_seqno = 1;
        for (ordinal, region) in metadata.regions.iter_mut().skip(1).enumerate() {
            region.incarnation = 0;
            region.state = RegionMetadataState::Free;
            region.queue_ordinal = u32::try_from(ordinal).unwrap();
            region.created_seqno = 0;
        }
        metadata.validate().unwrap();

        let mut old = FileRegionBackend::new(directory.files.clone(), data);
        old.acquire_exclusive().unwrap();
        assert!(matches!(
            old.inspect_recovery(config).unwrap(),
            RecoveryPlan::Fresh
        ));
        let runtime = FileRegionRuntime::install(
            PartitionedIndexStorage::anonymous(config).unwrap(),
            metadata,
        )
        .unwrap();
        old.publish_running().unwrap();
        let runtime = old.start_runtime(runtime).unwrap();
        let frozen = old.freeze_warm(runtime).unwrap();
        let prepared = old.persist_frozen(&frozen).unwrap();
        old.publish_clean(prepared).unwrap();
        old.release_exclusive().unwrap();

        let mut reopened = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(reopened.startup(), StartupMode::Cold);
        let manager = reopened.runtime().unwrap().manager.lock().unwrap();
        assert_eq!(manager.active_regions(), &[0, 1, 2, 3]);
        assert_eq!(manager.free_regions().len(), 1);
        drop(manager);
        reopened.close_fast().unwrap();
    }

    #[test]
    fn dirty_cold_start_discards_stale_region_bytes_without_scanning() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();
        let mut first = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
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

        let mut cold = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(cold.startup(), StartupMode::Cold);
        let mut observed = [0xff_u8; 12];
        File::open(&directory.files.data)
            .unwrap()
            .read_exact_at(&mut observed, stale_offset)
            .unwrap();
        assert_eq!(observed, [0; 12]);
        cold.close_fast().unwrap();
    }

    #[test]
    fn concrete_recovery_profile_rejects_fewer_than_five_regions_before_file_creation() {
        let directory = TestDirectory::new();
        let opened = RegionStore::open(
            8,
            FileRegionBackend::new(
                directory.files.clone(),
                test_data_superblock_with_regions(REGION_SHARDS),
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
        let config = INDEX_IMAGE_SLOTS_PER_PAGE + 8;
        let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
        let value = IndexSlot::DELETED;

        let mut first = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        let runtime = first.runtime_mut().unwrap();
        assert_eq!(runtime.index.storage().partition_count(), 2);
        runtime
            .index
            .storage()
            .write_slot(config - 1, value)
            .unwrap();
        first.close_warm().unwrap();
        assert!(directory.files.image.exists());

        let mut recovered = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(recovered.startup(), StartupMode::Warm);
        let recovered_runtime = recovered.runtime().unwrap();
        assert_eq!(recovered_runtime.index.storage().partition_count(), 2);
        assert_eq!(
            recovered_runtime
                .index
                .storage()
                .read_slot(config - 1)
                .unwrap(),
            value
        );
        recovered.close_fast().unwrap();
    }

    #[test]
    fn corrupt_region_metadata_rejects_the_complete_clean_image() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = 130;
        let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
        let mut first = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
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
        let header = RecoveryImageHeader::decode(&page).unwrap();
        image
            .write_all_at(&[0x5a], header.region_table_offset + 100)
            .unwrap();
        image.sync_data().unwrap();

        let mut rejected = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(rejected.startup(), StartupMode::Cold);
        assert_eq!(
            rejected.runtime().unwrap().lookup_snapshot(0).unwrap(),
            None
        );
        rejected.close_fast().unwrap();
    }

    #[test]
    fn one_corrupt_lazy_index_page_rejects_all_pages() {
        use std::os::unix::fs::FileExt;

        let directory = TestDirectory::new();
        let config = INDEX_IMAGE_SLOTS_PER_PAGE + 8;
        let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
        let mut first = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
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
                RECOVERY_IMAGE_INDEX_OFFSET + RECOVERY_PAGE_SIZE as u64 + 100,
            )
            .unwrap();
        image.sync_data().unwrap();

        let mut recovered = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(recovered.startup(), StartupMode::Warm);
        let runtime = recovered.runtime().unwrap();
        assert_eq!(runtime.lookup_snapshot(0).unwrap(), None);
        assert!(runtime.health.is_healthy());
        assert_eq!(runtime.lookup_snapshot(1).unwrap(), None);
        assert!(!runtime.health.is_healthy());
        assert_eq!(runtime.lookup_snapshot(1).unwrap(), None);
        assert_eq!(runtime.lookup_snapshot(0).unwrap(), None);
        assert!(recovered.close_warm().is_err());

        let mut cold = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(cold.startup(), StartupMode::Cold);
        assert_eq!(cold.runtime().unwrap().lookup_snapshot(1).unwrap(), None);
        cold.close_fast().unwrap();
    }

    #[test]
    fn every_prepublication_failure_leaves_no_selectable_clean_state() {
        let cases = [
            (
                Some((FaultEvent::Sync(SyncPoint::WarmData), FaultAction::Error(5))),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::RecoveryImageHeader),
                    FaultAction::Torn {
                        bytes: 128,
                        raw_os_error: 5,
                    },
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::RecoveryImageIndex),
                    FaultAction::Error(5),
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Write(WritePoint::RecoveryImageMetadata),
                    FaultAction::Torn {
                        bytes: 128,
                        raw_os_error: 5,
                    },
                )),
                None,
            ),
            (
                Some((
                    FaultEvent::Sync(SyncPoint::RecoveryImage),
                    FaultAction::Error(5),
                )),
                None,
            ),
            (None, Some(FileSystemFault::Rename)),
            (None, Some(FileSystemFault::SyncParent)),
            (
                Some((
                    FaultEvent::Write(WritePoint::State),
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
            let config = 8;
            let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
            let (file_system, io_faults, file_system_faults) = FaultRegionFileSystem::new();
            let backend =
                FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system);
            let mut store = RegionStore::open(config, backend).unwrap();
            if let Some((event, action)) = io_fault {
                io_faults.arm(event, 1, action);
            }
            if let Some(fault) = file_system_fault {
                file_system_faults.arm(fault);
            }
            assert!(store.close_warm().is_err(), "failure case {case}");

            let mut reopened = RegionStore::open(
                config,
                FileRegionBackend::new(directory.files.clone(), data),
            )
            .unwrap();
            assert_eq!(reopened.startup(), StartupMode::Cold, "failure case {case}");
            reopened.close_fast().unwrap();
        }
    }

    #[test]
    fn concrete_running_barrier_failures_abort_before_runtime_start() {
        let cases = [
            (
                FaultEvent::Write(WritePoint::State),
                1,
                FaultAction::Error(5),
            ),
            (
                FaultEvent::Write(WritePoint::State),
                2,
                FaultAction::Torn {
                    bytes: 128,
                    raw_os_error: 5,
                },
            ),
            (
                FaultEvent::Sync(SyncPoint::RunningState),
                1,
                FaultAction::Error(5),
            ),
        ];

        for (case, (event, occurrence, action)) in cases.into_iter().enumerate() {
            let directory = TestDirectory::new();
            let config = 8;
            let data = test_data_superblock();
            let (file_system, faults, _) = FaultRegionFileSystem::new();
            faults.arm(event, occurrence, action);
            let opened = RegionStore::open(
                config,
                FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system),
            );
            assert!(opened.is_err(), "RUNNING barrier case {case}");

            let mut cold = RegionStore::open(
                config,
                FileRegionBackend::new(directory.files.clone(), data),
            )
            .unwrap();
            assert!(matches!(cold.startup(), StartupMode::Cold));
            cold.close_fast().unwrap();
        }
    }

    #[test]
    fn final_clean_sync_failure_reopens_as_safe_clean_or_empty() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();
        let (file_system, faults, _) = FaultRegionFileSystem::new();
        let mut store = RegionStore::open(
            config,
            FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system),
        )
        .unwrap();
        faults.arm(
            FaultEvent::Sync(SyncPoint::CleanState),
            1,
            FaultAction::Error(5),
        );
        assert!(store.close_warm().is_err());

        let mut reopened = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert!(matches!(
            reopened.startup(),
            StartupMode::Warm | StartupMode::Cold
        ));
        assert_eq!(
            reopened.runtime().unwrap().lookup_snapshot(0).unwrap(),
            None
        );
        reopened.close_fast().unwrap();
    }

    #[test]
    fn data_and_state_inode_alias_is_rejected_without_truncation() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();
        let marker = b"do-not-truncate";
        std::fs::write(&directory.files.data, marker).unwrap();
        std::fs::hard_link(&directory.files.data, &directory.files.state).unwrap();

        let opened = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
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
        let files = RegionFiles::new(&data_path, directory.root.join("state"), image);

        let opened = RegionStore::open(
            8,
            FileRegionBackend::new(files, test_data_superblock_with_regions(REGION_SHARDS + 1)),
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
        let files = RegionFiles::new(
            directory.root.join("data"),
            other.join("state"),
            directory.root.join("image"),
        );
        let opened = RegionStore::open(8, FileRegionBackend::new(files, test_data_superblock()));
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
        let config = 8;
        let data = test_data_superblock();
        let mut first = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();

        let conflicting_files = RegionFiles::new(
            directory.root.join("other-data"),
            directory.files.state.clone(),
            directory.root.join("other-image"),
        );
        let opened = RegionStore::open(config, FileRegionBackend::new(conflicting_files, data));
        assert!(opened.is_err());
        first.close_fast().unwrap();
    }
}
