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

//! Steady-state Region authority and bounded request-path operations.

use std::io;
use std::ops::Range;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::checksum::crc32c;
use crate::format::{RECORD_ALIGNMENT, RECORD_HEADER_SIZE, RecordHeader};
use crate::hashing::route_hash;
#[cfg(test)]
use crate::index::IndexEntry;
use crate::index::PackedLocation;
use crate::index_storage::{
    INDEX_IMAGE_PAGE_SIZE, IndexStorageError, WARM_IMAGE_WRITE_BATCH_BYTES,
    canonical_index_partition_ranges,
};
use crate::io_engine::{IoEngine, ReadSlot};
#[cfg(test)]
use crate::record_codec::hash_key;
use crate::record_codec::{
    RecordEncodeError, encode_reinsert_into_hashed, encode_value_into_hashed,
};
use crate::recovery::{DATA_REGION_AREA_OFFSET, recovery_image_index_len};
use crate::region_appender::submit_span;
use crate::region_index::{ReclaimIndexAction, RegionIndex, heat_memory_bytes};
use crate::region_manager::{RegionManager, RegionMutationError, RegionReclaimReceipt};
use crate::region_metadata::{
    REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE, RegionMetadataError,
};
#[cfg(test)]
use crate::region_reader::plan_read;
use crate::region_reader::{PendingRead, ReadCandidate, ReadCompletion, ReadPlan, submit_read};
use crate::region_staging::{
    RegionStaging, StageAppend, StagedRecord, StagedWrite, StagingEncodeError, StagingError,
};
use crate::resources::BufferLease;
use crate::snapshot::{CacheIndexSnapshot, RegionSnapshot};

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
    pub(super) fn healthy() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(REGION_HEALTHY)),
        }
    }

    pub(crate) fn is_healthy(&self) -> bool {
        self.state.load(Ordering::Acquire) == REGION_HEALTHY
    }

    pub(crate) fn enter_miss_only(&self) {
        if self.transition_to_miss_only() {
            log::warn!(
                target: "cache2::health",
                event = "cache_miss_only",
                reason = "internal_failure";
                "cache entered miss-only mode"
            );
        }
    }

    fn enter_miss_only_with_error(&self, reason: &'static str, error: &impl std::fmt::Display) {
        if self.transition_to_miss_only() {
            log::warn!(
                target: "cache2::health",
                event = "cache_miss_only",
                reason,
                error:% = error;
                "cache entered miss-only mode"
            );
        }
    }

    fn transition_to_miss_only(&self) -> bool {
        self.state
            .compare_exchange(
                REGION_HEALTHY,
                REGION_MISS_ONLY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
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
/// accounting. Index publication is deliberately independent; reads validate
/// the physical record locally and may observe an older valid completion.
pub(super) struct RegionManagerAuthority {
    pub(super) inner: Mutex<RegionManager>,
    health: RegionHealthLatch,
}

impl RegionManagerAuthority {
    pub(super) fn new(manager: RegionManager, health: RegionHealthLatch) -> Self {
        Self {
            inner: Mutex::new(manager),
            health,
        }
    }

    pub(super) fn lock(&self) -> io::Result<MutexGuard<'_, RegionManager>> {
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
}

pub(crate) struct FileRegionCore {
    pub(super) index: RegionIndex,
    pub(super) manager: RegionManagerAuthority,
    pub(super) shards: Box<[RegionShard]>,
    pub(super) region_access: Box<[RegionAccessState]>,
    pub(super) rotation: Mutex<()>,
    pub(super) health: RegionHealthLatch,
}

pub(super) struct RegionAccessState {
    pub(super) generation: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegionReclaimStats {
    pub(crate) records_scanned: u64,
    pub(crate) records_removed: u64,
    pub(crate) bytes_read: u64,
    pub(crate) reinsert_records: u64,
    pub(crate) reinsert_bytes: u64,
    pub(crate) reinsert_skipped: u64,
    pub(crate) reinsert_budget_skipped: u64,
}

pub(crate) struct RegionReinsertRecord<'a> {
    pub(crate) hash: u64,
    pub(crate) previous_location: PackedLocation,
    pub(crate) logical_seqno: u64,
    pub(crate) record_bytes: u32,
    pub(crate) key: &'a [u8],
    pub(crate) value: &'a [u8],
}

/// Short shard-local transaction gate. `mutation` makes manager receipts and
/// staging transitions one operation. Span completion and rotation are already
/// ordered by the shard's single production worker.
#[derive(Default)]
pub(super) struct RegionShard {
    pub(super) mutation: Mutex<()>,
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

// SAFETY: this type is constructed only after the owned record read reaches
// terminal completion. From then until drop, it exposes initialized bytes only
// through shared slices and never returns the allocation to a mutable pool.
unsafe impl Sync for RegionValueRead {}

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
        runtime_fixed_memory_bytes(self.index.storage().slot_count(), region_count)
    }

    pub(crate) fn configure_reclaim_workers(&self, workers: usize) -> io::Result<()> {
        self.manager
            .lock()?
            .configure_reclaim_workers(workers)
            .map_err(region_metadata_io_error)
    }

    pub(crate) fn append_shard(&self, hash: u64) -> usize {
        route_hash(hash, self.shards.len())
    }

    pub(crate) fn region_snapshot(&self) -> io::Result<RegionSnapshot> {
        self.manager
            .lock()?
            .region_snapshot()
            .map_err(region_metadata_io_error)
    }

    pub(crate) fn index_snapshot(&self) -> io::Result<CacheIndexSnapshot> {
        self.index.snapshot().map_err(index_storage_io_error)
    }

    pub(crate) fn set_index_statistics_enabled(&self, enabled: bool) {
        self.index.set_statistics_enabled(enabled);
    }

    pub(crate) fn begin_reclaim(&self) -> io::Result<Option<RegionReclaimReceipt>> {
        self.health.require_healthy()?;
        self.manager
            .lock()?
            .begin_reclaim()
            .map_err(|error| region_mutation_context("reclaim begin", error))
    }

    pub(crate) fn reclaim_needed(&self) -> io::Result<bool> {
        Ok(self.manager.lock()?.reclaim_needed())
    }

    pub(crate) fn reclaim_can_reinsert(&self) -> io::Result<bool> {
        Ok(self.manager.lock()?.reclaim_can_reinsert())
    }

    pub(crate) fn reclaim_absolute(&self, receipt: RegionReclaimReceipt) -> io::Result<u64> {
        DATA_REGION_AREA_OFFSET
            .checked_add(
                u64::from(receipt.region_id)
                    .checked_mul(self.manager.lock()?.region_size())
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "reclaim offset overflow")
                    })?,
            )
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "reclaim offset overflow"))
    }

    /// Scans one exact sealed prefix, removes cold mappings, and offers each
    /// hot current record once to a bounded reinsertion sink. The
    /// source Region remains exclusively pinned until [`Self::complete_reclaim`].
    pub(crate) fn scan_reclaim(
        &self,
        receipt: RegionReclaimReceipt,
        bytes: &[u8],
        mut try_reinsert: impl FnMut(RegionReinsertRecord<'_>) -> io::Result<bool>,
    ) -> io::Result<RegionReclaimStats> {
        if bytes.len() as u64 != receipt.used_offset {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reclaim read does not match the sealed Region prefix",
            ));
        }
        let mut offset = 0_usize;
        let mut stats = RegionReclaimStats {
            bytes_read: receipt.used_offset,
            ..RegionReclaimStats::default()
        };
        let alignment = u64::from(RECORD_ALIGNMENT);
        let raw_budget =
            (receipt.used_offset / 8).saturating_sub(crate::io_backend::DIRECT_IO_ALIGNMENT as u64);
        let mut reinsert_budget = raw_budget - raw_budget % alignment;
        while offset < bytes.len() {
            let header_end = offset.checked_add(RECORD_HEADER_SIZE).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "reclaim header offset overflow")
            })?;
            let header = bytes
                .get(offset..header_end)
                .and_then(RecordHeader::decode)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim found an invalid record header",
                    )
                })?;
            let record_len = usize::try_from(header.record_len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reclaim record length is too large",
                )
            })?;
            let record_end = offset
                .checked_add(record_len)
                .filter(|end| *end <= bytes.len())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim record crosses the sealed prefix",
                    )
                })?;
            let payload_end = header_end
                .checked_add(usize::from(header.key_len))
                .and_then(|end| end.checked_add(header.value_len as usize))
                .filter(|end| *end <= record_end)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim record payload is out of bounds",
                    )
                })?;
            if header.region_generation != receipt.created_seqno || payload_end > record_end {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reclaim record belongs to another Region generation",
                ));
            }
            let location = PackedLocation::new(
                receipt.region_id,
                u32::try_from(offset).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "reclaim record offset is too large",
                    )
                })?,
                header.record_len,
            )
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let action = self
                .index
                .prepare_reclaim(header.key_hash, location)
                .map_err(index_storage_io_error)?;
            match action {
                ReclaimIndexAction::Missing => {}
                ReclaimIndexAction::Removed => {
                    stats.records_removed = stats.records_removed.saturating_add(1);
                }
                ReclaimIndexAction::Reinsert => {
                    let key_start = header_end;
                    let key_end = key_start
                        .checked_add(usize::from(header.key_len))
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "reclaim key range overflow")
                        })?;
                    let key = &bytes[key_start..key_end];
                    let value = &bytes[key_end..payload_end];
                    let rewrite_bytes = RecordHeader::aligned_len(key.len(), value.len())
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "reclaim replacement record is too large",
                            )
                        })?;
                    let payload_valid =
                        crc32c(&bytes[header_end..payload_end]) == header.payload_crc;
                    let within_budget = u64::from(rewrite_bytes) <= reinsert_budget;
                    let budget_exhausted = payload_valid && !within_budget;
                    let reinserted = payload_valid
                        && within_budget
                        && try_reinsert(RegionReinsertRecord {
                            hash: header.key_hash,
                            previous_location: location,
                            logical_seqno: header.seqno,
                            record_bytes: rewrite_bytes,
                            key,
                            value,
                        })?;
                    if reinserted {
                        reinsert_budget = reinsert_budget.saturating_sub(u64::from(rewrite_bytes));
                        stats.reinsert_records = stats.reinsert_records.saturating_add(1);
                        stats.reinsert_bytes = stats
                            .reinsert_bytes
                            .saturating_add(u64::from(rewrite_bytes));
                    } else {
                        if budget_exhausted {
                            stats.reinsert_budget_skipped =
                                stats.reinsert_budget_skipped.saturating_add(1);
                        }
                        if self
                            .index
                            .remove_if_match(header.key_hash, location)
                            .map_err(index_storage_io_error)?
                        {
                            stats.records_removed = stats.records_removed.saturating_add(1);
                        }
                        stats.reinsert_skipped = stats.reinsert_skipped.saturating_add(1);
                    }
                }
            }
            stats.records_scanned = stats.records_scanned.saturating_add(1);
            offset = record_end;
        }
        if stats.records_scanned != receipt.physical_record_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "reclaim record count does not match Region metadata",
            ));
        }
        Ok(stats)
    }

    /// Releases one fully scanned source only after every accepted replacement
    /// batch has completed and conditionally published.
    pub(crate) fn complete_reclaim(&self, receipt: RegionReclaimReceipt) -> io::Result<()> {
        let access = self
            .region_access
            .get(receipt.region_id as usize)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "reclaim Region id is out of bounds",
                )
            })?;
        access.generation.store(0, Ordering::Release);
        self.manager
            .lock()?
            .finish_reclaim(receipt)
            .map_err(|error| region_mutation_context("reclaim completion", error))?;
        Ok(())
    }

    pub(crate) fn enter_miss_only(&self) {
        self.health.enter_miss_only();
    }

    pub(crate) fn enter_miss_only_with_error(
        &self,
        reason: &'static str,
        error: &impl std::fmt::Display,
    ) {
        self.health.enter_miss_only_with_error(reason, error);
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
    pub(super) fn lookup_snapshot(&self, hash: u64) -> io::Result<Option<IndexEntry>> {
        if !self.health.is_healthy() {
            return Ok(None);
        }
        match self.index.lookup_raw(hash) {
            Ok(Some(entry)) if self.health.is_healthy() => Ok(Some(entry)),
            Ok(result) if self.health.is_healthy() => Ok(result),
            Ok(_) => Ok(None),
            Err(error) => {
                enter_miss_only_for_index_error(&self.health, &error);
                Ok(None)
            }
        }
    }

    /// Begins one physical read with a single bounded index lookup.
    pub(super) fn begin_point_read(&self, hash: u64) -> Option<ReadCandidate> {
        if !self.health.is_healthy() {
            return None;
        }
        let entry = match self.index.lookup_raw(hash) {
            Ok(Some(entry)) => entry,
            Ok(None) => return None,
            Err(IndexStorageError::PageBusy { .. } | IndexStorageError::PartitionBusy { .. }) => {
                return None;
            }
            Err(error) => {
                enter_miss_only_for_index_error(&self.health, &error);
                return None;
            }
        };
        let access = self
            .region_access
            .get(entry.location.region_id() as usize)?;
        let region_generation = access.generation.load(Ordering::Acquire);
        if region_generation == 0 {
            return None;
        }
        Some(ReadCandidate {
            entry,
            region_generation,
        })
    }

    /// Begins one durable value read. The owned read buffer becomes the value
    /// owner on a hit, avoiding a second payload copy.
    pub(crate) fn begin_value_read(&self, hash: u64) -> Option<ReadCandidate> {
        self.begin_point_read(hash)
    }

    #[cfg(test)]
    pub(crate) fn read_value(
        &self,
        engine: &dyn IoEngine,
        geometry: crate::recovery::DataGeometry,
        buffer: BufferLease,
        hash_seed: u64,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let hash = hash_key(hash_seed, key);
        let Some(candidate) = self.begin_value_read(hash) else {
            return Ok(None);
        };
        let plan = plan_read(geometry, hash, candidate, true)?;
        let slot = engine.try_reserve_read()?;
        self.read_value_from_plan(engine, slot, buffer, plan, key)
    }

    #[cfg(test)]
    pub(crate) fn read_value_from_plan(
        &self,
        engine: &dyn IoEngine,
        slot: ReadSlot,
        buffer: BufferLease,
        plan: ReadPlan,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let pending = self.submit_value_read_from_plan(engine, slot, buffer, plan)?;
        let completion = pending.wait(engine);
        self.finish_value_read(completion, key)
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
                if !is_read_availability_error(error.kind()) {
                    self.health
                        .enter_miss_only_with_error("record_read_submit_failed", &error);
                }
                Err(error)
            }
        }
    }

    pub(crate) fn finish_value_read(
        &self,
        completion: ReadCompletion,
        key: &[u8],
    ) -> io::Result<Option<RegionValueRead>> {
        let hash = completion.plan.hash;
        if let Err(error) = completion.result {
            if !is_read_availability_error(error.kind()) {
                self.health
                    .enter_miss_only_with_error("record_read_completion_failed", &error);
            }
            return Err(error);
        }
        let Some(record) = completion.record_bytes() else {
            let error = io::Error::new(
                io::ErrorKind::InvalidData,
                "record completion lost its bounded candidate range",
            );
            self.health
                .enter_miss_only_with_error("record_read_completion_invalid", &error);
            return Err(error);
        };
        let Some(header) = record
            .get(..RECORD_HEADER_SIZE)
            .and_then(RecordHeader::decode)
        else {
            return Ok(None);
        };
        if header.region_generation != completion.plan.region_generation || header.key_hash != hash
        {
            return Ok(None);
        }
        let indexed_location = completion.plan.entry.location;
        let Ok(exact_location) = PackedLocation::new(
            indexed_location.region_id(),
            indexed_location.offset(),
            header.record_len,
        ) else {
            return Ok(None);
        };
        if !indexed_location.index_equivalent(exact_location) {
            return Ok(None);
        }
        let Some(record) = usize::try_from(header.record_len)
            .ok()
            .and_then(|record_len| record.get(..record_len))
        else {
            return Ok(None);
        };
        let key_len = usize::from(header.key_len);
        let value_len = header.value_len as usize;
        let Some(payload_end) = RECORD_HEADER_SIZE
            .checked_add(key_len)
            .and_then(|end| end.checked_add(value_len))
            .filter(|end| *end <= record.len())
        else {
            return Ok(None);
        };
        let encoded_key = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + key_len];
        if encoded_key != key {
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
            let error = io::Error::new(
                io::ErrorKind::InvalidData,
                "validated Region read lost its owned buffer",
            );
            self.health
                .enter_miss_only_with_error("record_read_completion_invalid", &error);
            return Err(error);
        };
        Ok(Some(RegionValueRead {
            buffer,
            buffer_len: completion.plan.read_len,
            value_range: value_start..value_end,
            seqno: header.seqno,
        }))
    }

    /// Preflights, reserves, and encodes one value directly into a shard's
    /// aligned fill buffer. Region reservation and open-span accounting share
    /// one manager try-lock; the shard mutation gate then protects encoding
    /// without retaining global authority. This method performs no device I/O
    /// and never publishes an index entry.
    pub(crate) fn try_stage_value(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        hash: u64,
        record_bytes: u32,
        key: &[u8],
        value: &[u8],
    ) -> io::Result<RegionStageValue> {
        self.try_stage_record(staging, shard_id, hash, record_bytes, key, value, None)
    }

    pub(crate) fn try_stage_reinsert(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        record: RegionReinsertRecord<'_>,
    ) -> io::Result<RegionStageValue> {
        self.try_stage_record(
            staging,
            shard_id,
            record.hash,
            record.record_bytes,
            record.key,
            record.value,
            Some((record.logical_seqno, record.previous_location)),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn try_stage_record(
        &self,
        staging: &RegionStaging,
        shard_id: usize,
        hash: u64,
        record_bytes: u32,
        key: &[u8],
        value: &[u8],
        reinsert: Option<(u64, PackedLocation)>,
    ) -> io::Result<RegionStageValue> {
        self.health.require_healthy()?;
        if record_bytes as usize > staging.chunk_bytes() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "value exceeds one Region staging buffer",
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
            let entry = match reinsert {
                Some((logical_seqno, _)) => encode_reinsert_into_hashed(
                    destination,
                    receipt,
                    hash,
                    record_bytes,
                    key,
                    value,
                    logical_seqno,
                )?,
                None => {
                    encode_value_into_hashed(destination, receipt, hash, record_bytes, key, value)?
                }
            };
            Ok::<StagedRecord, RecordEncodeError>(match reinsert {
                Some((_, previous_location)) => {
                    StagedRecord::reinsert(hash, entry, receipt.seqno, previous_location)
                }
                None => StagedRecord::new(hash, entry, receipt.seqno),
            })
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

    /// Allocates one ordering sequence and removes the current L2 candidate
    /// with a single non-waiting bounded index probe. No Region bytes are
    /// reserved or written for a delete.
    pub(crate) fn try_delete_value(&self, hash: u64) -> io::Result<Option<u64>> {
        self.health.require_healthy()?;
        let seqno = {
            let Some(mut manager) = self.manager.try_lock()? else {
                return Ok(None);
            };
            match manager.allocate_seqno() {
                Ok(seqno) => seqno,
                Err(error) => {
                    self.health.enter_miss_only();
                    return Err(region_mutation_io_error(error));
                }
            }
        };
        match self.index.try_delete(hash) {
            Ok(_) => Ok(Some(seqno)),
            Err(IndexStorageError::PageBusy { .. } | IndexStorageError::PartitionBusy { .. }) => {
                Ok(None)
            }
            Err(error) => {
                enter_miss_only_for_index_error(&self.health, &error);
                Err(index_storage_io_error(error))
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
        // non-zero tail receipt remains a shard fence while staging extends its
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
        let rotation = self.rotation.lock().map_err(|_| {
            self.health.enter_miss_only();
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Region rotation gate is poisoned",
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
        let access = self
            .region_access
            .get(receipt.activated_region_id as usize)
            .ok_or_else(|| {
                self.health.enter_miss_only();
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "rotation activated an untracked Region",
                )
            })?;
        access
            .generation
            .store(receipt.activated_created_seqno, Ordering::Release);
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
    /// A delayed older completion may replace a newer candidate; stale values
    /// are accepted and exact physical identity is checked after the read.
    pub(super) fn publish_completed_records(&self, records: &[StagedRecord]) -> io::Result<()> {
        for record in records.iter().copied() {
            let entry = record.entry();
            let published = match record.previous_location() {
                Some(previous) => self.index.replace_if_match(record.hash(), previous, entry),
                None => self.index.upsert(record.hash(), entry),
            };
            if let Err(error) = published {
                enter_miss_only_for_index_error(&self.health, &error);
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

fn is_read_availability_error(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::OutOfMemory
            | io::ErrorKind::WouldBlock
            | io::ErrorKind::TimedOut
            | io::ErrorKind::Interrupted
            | io::ErrorKind::BrokenPipe
    )
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
    let index_page_state_bytes = (index_bytes / INDEX_IMAGE_PAGE_SIZE)
        .checked_mul(std::mem::size_of::<AtomicU8>())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "index page-state memory does not fit the platform",
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
    let heat_bytes = heat_memory_bytes(index_slots).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "index heat memory does not fit the platform",
        )
    })?;
    index_bytes
        .checked_add(index_page_state_bytes)
        .and_then(|bytes| bytes.checked_add(heat_bytes))
        .and_then(|bytes| bytes.checked_add(region_bytes))
        .and_then(|bytes| bytes.checked_add(recovery_scratch))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "memory size overflow"))
}

pub(super) fn region_metadata_io_error(error: RegionMetadataError) -> io::Error {
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

pub(super) fn index_storage_io_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidData, error),
    }
}

pub(super) fn guarded_index_result<T>(
    health: &RegionHealthLatch,
    result: Result<T, IndexStorageError>,
) -> io::Result<T> {
    result.map_err(|error| {
        enter_miss_only_for_index_error(health, &error);
        index_storage_io_error(error)
    })
}

fn enter_miss_only_for_index_error(health: &RegionHealthLatch, error: &IndexStorageError) {
    let reason = match error {
        IndexStorageError::CorruptPage { .. } | IndexStorageError::CorruptSlot { .. } => {
            "index_recovery_validation_failed"
        }
        IndexStorageError::PartitionPoisoned { .. } => "index_partition_poisoned",
        IndexStorageError::Io(_) => "index_storage_io_failed",
        _ => "index_storage_invalid",
    };
    health.enter_miss_only_with_error(reason, error);
}
