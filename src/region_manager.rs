//! In-memory Region authority reconstructed from one validated clean image.
//!
//! The manager deliberately does not retain the recovered index partition
//! directory or its physical counters. A clean freeze accepts the current
//! canonical shard records from the index owner and derives every Region and
//! queue field from live manager state.

use crate::io_backend::DIRECT_IO_ALIGNMENT;
use crate::recovery::{PersistentId, RECORD_ALIGNMENT};
use crate::region_metadata::{
    PartitionMetadataRecord, RegionMetadata, RegionMetadataError, RegionMetadataRecord,
    RegionMetadataRoot, RegionMetadataState,
};
use crate::snapshot::RegionSnapshot;
use std::collections::VecDeque;

const UNASSIGNED_REGION: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataBinding {
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) data_superblock_generation: u64,
    pub(crate) image_identity: PersistentId,
    pub(crate) image_generation: u64,
    pub(crate) config_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMutationError {
    InvalidShard,
    InvalidRecordLength,
    WouldBlock,
    FlushBeforeRotation,
    RegionFull,
    StaleReceipt,
    SequenceExhausted,
    IncarnationExhausted,
    ArithmeticOverflow,
    Invariant(&'static str),
}

/// Exclusive tail reservation owned by one data shard until its encoded
/// bytes have either entered staging or been cancelled. Device completion is
/// deliberately represented by [`RegionWriteSpan`], not by this per-record
/// receipt, so one shard can accumulate many records into a MiB-scale write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionAppendReservation {
    pub(crate) shard_id: usize,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) offset: u32,
    pub(crate) record_bytes: u32,
    pub(crate) seqno: u64,
}

impl RegionAppendReservation {
    fn end_offset(self) -> Option<u64> {
        u64::from(self.offset).checked_add(u64::from(self.record_bytes))
    }
}

/// Exclusive tail padding for one non-empty open write span. The manager owns
/// both offsets; staging may only extend its final record after validating this
/// exact generation and span identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionPaddingReceipt {
    pub(crate) shard_id: usize,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) span_start_offset: u64,
    pub(crate) unpadded_end_offset: u64,
    pub(crate) padded_end_offset: u64,
    pub(crate) record_count: u64,
    pub(crate) max_seqno: u64,
}

impl RegionPaddingReceipt {
    pub(crate) fn padding_bytes(self) -> Option<u32> {
        self.padded_end_offset
            .checked_sub(self.unpadded_end_offset)
            .and_then(|padding| u32::try_from(padding).ok())
    }
}

/// One ordered staging span. A shard has at most one submitted span while it
/// continues filling the next resident staging chunk. Completion advances the
/// written cursor only when this exact generation and start offset still own
/// the shard. Durability is established once, by the CLEAN data sync.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionWriteSpan {
    pub(crate) shard_id: usize,
    pub(crate) span_id: u64,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) record_count: u64,
    pub(crate) max_seqno: u64,
}

/// Exact in-memory authority carried across a Region rotation. Until
/// [`RegionManager::finish_rotation`] accepts this receipt, the shard rejects
/// new reservations and the sealed Region stays out of the reclaim FIFO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRotationReceipt {
    pub(crate) shard_id: usize,
    pub(crate) sealed_region_id: u32,
    pub(crate) activated_region_id: u32,
    pub(crate) activated_incarnation: u32,
    pub(crate) activated_created_seqno: u64,
    pub(crate) reused: bool,
}

/// Read-only selection of the next FIFO rotation victim.
///
/// A caller may retain this value while it drops manager authority. It must
/// hold the process-wide rotation gate until
/// [`RegionManager::begin_rotation`] consumes the plan.
/// `victim_incarnation` identifies the generation being replaced; the newly
/// activated generation advances it by exactly one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRotationPlan {
    pub(crate) shard_id: usize,
    pub(crate) victim_region_id: u32,
    pub(crate) victim_incarnation: u32,
    pub(crate) reused: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegionRotationSelection {
    plan: RegionRotationPlan,
    old_index: usize,
    old: RegionRuntime,
    victim_index: usize,
    activated_incarnation: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OpenWriteSpan {
    region_id: u32,
    region_incarnation: u32,
    start_offset: u64,
    end_offset: u64,
    record_count: u64,
    max_seqno: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ShardMutation {
    tail: Option<RegionAppendReservation>,
    open_span: Option<OpenWriteSpan>,
    pending_padding: Option<RegionPaddingReceipt>,
    submitted_span: Option<RegionWriteSpan>,
    rotation_requested: bool,
    rotation: Option<RegionRotationReceipt>,
    next_span_id: u64,
}

impl Default for ShardMutation {
    fn default() -> Self {
        Self {
            tail: None,
            open_span: None,
            pending_padding: None,
            submitted_span: None,
            rotation_requested: false,
            rotation: None,
            next_span_id: 1,
        }
    }
}

impl ShardMutation {
    const fn is_quiescent(self) -> bool {
        self.tail.is_none()
            && self.open_span.is_none()
            && self.pending_padding.is_none()
            && self.submitted_span.is_none()
            && self.rotation.is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRuntime {
    pub(crate) region_id: u32,
    pub(crate) incarnation: u32,
    pub(crate) state: RegionMetadataState,
    pub(crate) created_seqno: u64,
    /// Last byte covered by a successful write completion. A buffered or
    /// io_uring CQE is not a durability barrier; CLEAN later syncs this prefix.
    pub(crate) completed_used: u64,
    /// Reservation cursor. A clean recovery has no outstanding writes, so it
    /// starts exactly at `completed_used`.
    pub(crate) reserved_used: u64,
    pub(crate) max_seqno: u64,
    pub(crate) physical_record_count: u64,
}

#[derive(Debug)]
pub(crate) struct RegionManager {
    binding: RegionMetadataBinding,
    region_size: u64,
    next_seqno: u64,
    regions: Vec<RegionRuntime>,
    active_regions: Vec<u32>,
    shard_mutations: Vec<ShardMutation>,
    free_regions: VecDeque<u32>,
    sealed_regions: VecDeque<u32>,
    queue_capacity: usize,
    rotations: u64,
}

impl RegionManager {
    /// Consumes one complete CLEAN metadata image and reconstructs runtime
    /// authority without scanning the index or Region data records.
    pub(crate) fn from_metadata(metadata: RegionMetadata) -> Result<Self, RegionMetadataError> {
        metadata.validate()?;
        let RegionMetadata {
            root,
            regions: encoded_regions,
            partitions,
        } = metadata;
        // Partition topology and slot counters are owned by the live index.
        // Do not retain the recovered copy as a second authority.
        drop(partitions);

        let region_count = usize::try_from(root.region_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let active_count = usize::try_from(root.shard_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let free_count = usize::try_from(root.free_region_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let sealed_count = usize::try_from(root.sealed_region_count)
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let sealed_capacity = region_count
            .checked_sub(active_count)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;

        let mut regions = try_vec(region_count)?;
        let mut active_regions = try_unassigned_vec(active_count)?;
        let mut shard_mutations = try_vec(active_count)?;
        shard_mutations.resize(active_count, ShardMutation::default());
        let mut free_regions = try_unassigned_queue(free_count, free_count)?;
        let mut sealed_regions = try_unassigned_queue(sealed_count, sealed_capacity)?;

        for encoded in encoded_regions.iter().copied() {
            let runtime = RegionRuntime {
                region_id: encoded.region_id,
                incarnation: encoded.incarnation,
                state: encoded.state,
                created_seqno: encoded.created_seqno,
                completed_used: encoded.durable_used_offset,
                reserved_used: encoded.durable_used_offset,
                max_seqno: encoded.max_seqno,
                physical_record_count: encoded.physical_record_count,
            };
            install_recovered_queue_entry(
                runtime.state,
                encoded.queue_ordinal,
                runtime.region_id,
                &mut active_regions,
                &mut free_regions,
                &mut sealed_regions,
            )?;
            regions.push(runtime);
        }
        if regions.len() != region_count
            || active_regions.contains(&UNASSIGNED_REGION)
            || free_regions.contains(&UNASSIGNED_REGION)
            || sealed_regions.contains(&UNASSIGNED_REGION)
        {
            return Err(RegionMetadataError::InvalidField(
                "region_queue_permutation",
            ));
        }
        let next_seqno = root
            .max_seqno
            .checked_add(1)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        Ok(Self {
            binding: RegionMetadataBinding {
                cache_uuid: root.cache_uuid,
                data_identity: root.data_identity,
                data_superblock_generation: root.data_superblock_generation,
                image_identity: root.image_identity,
                image_generation: root.image_generation,
                config_fingerprint: root.config_fingerprint,
            },
            region_size: root.region_size,
            next_seqno,
            regions,
            active_regions,
            shard_mutations,
            free_regions,
            sealed_regions,
            queue_capacity: sealed_capacity,
            rotations: 0,
        })
    }

    pub(crate) const fn region_size(&self) -> u64 {
        self.region_size
    }

    #[cfg(test)]
    pub(crate) const fn next_seqno(&self) -> u64 {
        self.next_seqno
    }

    pub(crate) fn regions(&self) -> &[RegionRuntime] {
        &self.regions
    }

    pub(crate) fn active_regions(&self) -> &[u32] {
        &self.active_regions
    }

    pub(crate) fn region_snapshot(&self) -> Result<RegionSnapshot, RegionMetadataError> {
        let mut snapshot = RegionSnapshot {
            capacity_bytes: u64::try_from(self.regions.len())
                .ok()
                .and_then(|count| count.checked_mul(self.region_size))
                .ok_or(RegionMetadataError::ArithmeticOverflow)?,
            append_shard_count: u32::try_from(self.active_regions.len())
                .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            active_region_count: u32::try_from(self.active_regions.len())
                .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            free_region_count: u32::try_from(self.free_regions.len())
                .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            sealed_region_count: u32::try_from(self.sealed_regions.len())
                .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            rotations: self.rotations,
            ..RegionSnapshot::default()
        };
        for region in &self.regions {
            snapshot.physical_record_count = snapshot
                .physical_record_count
                .saturating_add(region.physical_record_count);
            snapshot.physical_bytes = snapshot
                .physical_bytes
                .saturating_add(region.completed_used);
        }
        Ok(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn free_regions(&self) -> &VecDeque<u32> {
        &self.free_regions
    }

    #[cfg(test)]
    pub(crate) fn sealed_regions(&self) -> &VecDeque<u32> {
        &self.sealed_regions
    }

    /// Allocates one process-local ordering version. Sequence exhaustion is a
    /// terminal condition for the current cache identity; `u64::MAX` is never
    /// issued because clean metadata reserves it as invalid.
    pub(crate) fn allocate_seqno(&mut self) -> Result<u64, RegionMutationError> {
        if self.next_seqno == u64::MAX {
            return Err(RegionMutationError::SequenceExhausted);
        }
        let allocated = self.next_seqno;
        self.next_seqno += 1;
        Ok(allocated)
    }

    /// Reserves the aligned tail of one shard's Active Region. Only the bytes
    /// needed to encode this record are exclusive; once staged, the shard may
    /// reserve the next record without waiting for device completion.
    pub(crate) fn reserve_append(
        &mut self,
        shard_id: usize,
        record_bytes: u32,
    ) -> Result<RegionAppendReservation, RegionMutationError> {
        if record_bytes == 0 || !record_bytes.is_multiple_of(RECORD_ALIGNMENT) {
            return Err(RegionMutationError::InvalidRecordLength);
        }
        let shard = self
            .shard_mutations
            .get(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail.is_some() || shard.pending_padding.is_some() || shard.rotation.is_some() {
            return Err(RegionMutationError::WouldBlock);
        }
        let region_id = *self
            .active_regions
            .get(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        let region_index =
            usize::try_from(region_id).map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let region =
            self.regions
                .get(region_index)
                .copied()
                .ok_or(RegionMutationError::Invariant(
                    "active Region id is out of bounds",
                ))?;
        if region.state != RegionMetadataState::Active || region.region_id != region_id {
            return Err(RegionMutationError::Invariant(
                "data shard does not own an Active Region",
            ));
        }
        let end = region
            .reserved_used
            .checked_add(u64::from(record_bytes))
            .ok_or(RegionMutationError::ArithmeticOverflow)?;
        if end > self.region_size {
            let error = if shard.open_span.is_some() {
                RegionMutationError::FlushBeforeRotation
            } else if shard.submitted_span.is_some() {
                RegionMutationError::WouldBlock
            } else {
                RegionMutationError::RegionFull
            };
            if error == RegionMutationError::RegionFull {
                self.shard_mutations[shard_id].rotation_requested = true;
            }
            return Err(error);
        }
        let offset = u32::try_from(region.reserved_used)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let seqno = self.allocate_seqno()?;
        let receipt = RegionAppendReservation {
            shard_id,
            region_id,
            region_incarnation: region.incarnation,
            offset,
            record_bytes,
            seqno,
        };
        self.regions[region_index].reserved_used = end;
        self.shard_mutations[shard_id].tail = Some(receipt);
        Ok(receipt)
    }

    /// Commits one tail reservation to the manager's resident staging span.
    ///
    /// The production caller holds the shard mutation gate, commits here, and
    /// then encodes the matching bytes before releasing that gate. Any encode
    /// failure is terminal for the runtime, so no worker can seal manager
    /// accounting without its exact staged bytes. This is not a write
    /// completion and does not move `completed_used` or physical accounting.
    pub(crate) fn stage_reservation(
        &mut self,
        receipt: RegionAppendReservation,
    ) -> Result<(), RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(receipt.shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail != Some(receipt) {
            return Err(RegionMutationError::StaleReceipt);
        }
        let end = receipt
            .end_offset()
            .ok_or(RegionMutationError::ArithmeticOverflow)?;
        let next_span = match shard.open_span {
            Some(span)
                if span.region_id == receipt.region_id
                    && span.region_incarnation == receipt.region_incarnation
                    && span.end_offset == u64::from(receipt.offset) =>
            {
                OpenWriteSpan {
                    end_offset: end,
                    record_count: span
                        .record_count
                        .checked_add(1)
                        .ok_or(RegionMutationError::ArithmeticOverflow)?,
                    max_seqno: span.max_seqno.max(receipt.seqno),
                    ..span
                }
            }
            None => OpenWriteSpan {
                region_id: receipt.region_id,
                region_incarnation: receipt.region_incarnation,
                start_offset: u64::from(receipt.offset),
                end_offset: end,
                record_count: 1,
                max_seqno: receipt.seqno,
            },
            Some(_) => {
                return Err(RegionMutationError::Invariant(
                    "staged reservations are not contiguous",
                ));
            }
        };
        self.shard_mutations[receipt.shard_id].open_span = Some(next_span);
        self.shard_mutations[receipt.shard_id].tail = None;
        Ok(())
    }

    /// Reserves the bytes needed to align the current span end for direct I/O.
    /// No receipt is needed when the open span already ends on a 4 KiB
    /// boundary. A non-zero receipt remains an exclusive shard fence until
    /// [`Self::seal_write_span_with_padding`] consumes it.
    pub(crate) fn reserve_write_padding(
        &mut self,
        shard_id: usize,
    ) -> Result<Option<RegionPaddingReceipt>, RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(shard_id)
            .copied()
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail.is_some()
            || shard.pending_padding.is_some()
            || shard.submitted_span.is_some()
            || shard.rotation.is_some()
        {
            return Err(RegionMutationError::WouldBlock);
        }
        let open = shard.open_span.ok_or(RegionMutationError::Invariant(
            "data shard has no staged records",
        ))?;
        if open.start_offset >= open.end_offset
            || open.start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
            || open.end_offset % u64::from(RECORD_ALIGNMENT) != 0
            || open.record_count == 0
            || open.max_seqno == 0
        {
            return Err(RegionMutationError::Invariant(
                "open write span identity is invalid",
            ));
        }
        let region_index =
            usize::try_from(open.region_id).map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let region =
            self.regions
                .get(region_index)
                .copied()
                .ok_or(RegionMutationError::Invariant(
                    "open write span Region is out of bounds",
                ))?;
        if self.active_regions.get(shard_id) != Some(&open.region_id)
            || region.state != RegionMetadataState::Active
            || region.incarnation != open.region_incarnation
            || region.reserved_used != open.end_offset
        {
            return Err(RegionMutationError::Invariant(
                "open write span lost its Region authority",
            ));
        }

        let alignment = DIRECT_IO_ALIGNMENT as u64;
        let padding = open.end_offset.wrapping_neg() & (alignment - 1);
        if padding == 0 {
            return Ok(None);
        }
        let padded_end_offset = open
            .end_offset
            .checked_add(padding)
            .ok_or(RegionMutationError::ArithmeticOverflow)?;
        if padding >= alignment
            || !padding.is_multiple_of(u64::from(RECORD_ALIGNMENT))
            || padded_end_offset % alignment != 0
            || padded_end_offset > self.region_size
        {
            return Err(RegionMutationError::Invariant(
                "write span padding is invalid",
            ));
        }
        let receipt = RegionPaddingReceipt {
            shard_id,
            region_id: open.region_id,
            region_incarnation: open.region_incarnation,
            span_start_offset: open.start_offset,
            unpadded_end_offset: open.end_offset,
            padded_end_offset,
            record_count: open.record_count,
            max_seqno: open.max_seqno,
        };
        self.regions[region_index].reserved_used = padded_end_offset;
        self.shard_mutations[shard_id].pending_padding = Some(receipt);
        Ok(Some(receipt))
    }

    /// Cancels only the current, not-yet-staged tail. Sequence numbers remain
    /// consumed, but the reservation cursor is rolled back exactly because no
    /// later reservation can exist on the same shard.
    #[cfg(test)]
    pub(crate) fn cancel_reservation(
        &mut self,
        receipt: RegionAppendReservation,
    ) -> Result<(), RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(receipt.shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail != Some(receipt) {
            return Err(RegionMutationError::StaleReceipt);
        }
        let region_index = usize::try_from(receipt.region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .ok_or(RegionMutationError::StaleReceipt)?;
        if self.active_regions.get(receipt.shard_id) != Some(&receipt.region_id)
            || region.state != RegionMetadataState::Active
            || region.incarnation != receipt.region_incarnation
            || region.reserved_used
                != receipt
                    .end_offset()
                    .ok_or(RegionMutationError::ArithmeticOverflow)?
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        self.regions[region_index].reserved_used = u64::from(receipt.offset);
        self.shard_mutations[receipt.shard_id].tail = None;
        Ok(())
    }

    /// Seals the shard's accumulated resident records into one ordered device
    /// span. A second span may be built concurrently in resident staging, but
    /// only one submitted span per shard is admitted in this first kernel.
    pub(crate) fn seal_write_span(
        &mut self,
        shard_id: usize,
    ) -> Result<RegionWriteSpan, RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail.is_some()
            || shard.pending_padding.is_some()
            || shard.rotation.is_some()
            || shard.submitted_span.is_some()
        {
            return Err(RegionMutationError::WouldBlock);
        }
        let open = shard.open_span.ok_or(RegionMutationError::Invariant(
            "data shard has no staged records",
        ))?;
        self.submit_open_span(shard_id, open.end_offset)
    }

    /// Atomically consumes one exact padding receipt and seals its open span.
    /// Keeping the receipt pending until this call prevents another append from
    /// entering between staging's header rewrite and manager publication.
    pub(crate) fn seal_write_span_with_padding(
        &mut self,
        padding: RegionPaddingReceipt,
    ) -> Result<RegionWriteSpan, RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(padding.shard_id)
            .copied()
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.tail.is_some()
            || shard.rotation.is_some()
            || shard.submitted_span.is_some()
            || shard.pending_padding != Some(padding)
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        let open = shard.open_span.ok_or(RegionMutationError::StaleReceipt)?;
        if open.region_id != padding.region_id
            || open.region_incarnation != padding.region_incarnation
            || open.start_offset != padding.span_start_offset
            || open.end_offset != padding.unpadded_end_offset
            || open.record_count != padding.record_count
            || open.max_seqno != padding.max_seqno
            || padding
                .padding_bytes()
                .is_none_or(|bytes| bytes == 0 || bytes as usize >= DIRECT_IO_ALIGNMENT)
            || !padding
                .padded_end_offset
                .is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        let region_index = usize::try_from(padding.region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .ok_or(RegionMutationError::StaleReceipt)?;
        if self.active_regions.get(padding.shard_id) != Some(&padding.region_id)
            || region.state != RegionMetadataState::Active
            || region.incarnation != padding.region_incarnation
            || region.reserved_used != padding.padded_end_offset
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        self.submit_open_span(padding.shard_id, padding.padded_end_offset)
    }

    fn submit_open_span(
        &mut self,
        shard_id: usize,
        end_offset: u64,
    ) -> Result<RegionWriteSpan, RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(shard_id)
            .copied()
            .ok_or(RegionMutationError::InvalidShard)?;
        let open = shard.open_span.ok_or(RegionMutationError::Invariant(
            "data shard has no staged records",
        ))?;
        if open.start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
            || !end_offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
            || end_offset <= open.start_offset
        {
            return Err(RegionMutationError::Invariant(
                "submitted write span is not direct-I/O aligned",
            ));
        }
        if shard.next_span_id == u64::MAX {
            return Err(RegionMutationError::SequenceExhausted);
        }
        let receipt = RegionWriteSpan {
            shard_id,
            span_id: shard.next_span_id,
            region_id: open.region_id,
            region_incarnation: open.region_incarnation,
            start_offset: open.start_offset,
            end_offset,
            record_count: open.record_count,
            max_seqno: open.max_seqno,
        };
        let shard = &mut self.shard_mutations[shard_id];
        shard.next_span_id += 1;
        shard.open_span = None;
        shard.pending_padding = None;
        shard.submitted_span = Some(receipt);
        Ok(receipt)
    }

    /// Advances the completed prefix for one exact, ordered device completion.
    /// Duplicate, cancelled, wrong-generation, and late completions are
    /// rejected without touching the current Region incarnation.
    pub(crate) fn complete_write_span(
        &mut self,
        receipt: RegionWriteSpan,
    ) -> Result<(), RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(receipt.shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.submitted_span != Some(receipt) {
            return Err(RegionMutationError::StaleReceipt);
        }
        let region_index = usize::try_from(receipt.region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .copied()
            .ok_or(RegionMutationError::StaleReceipt)?;
        if self.active_regions.get(receipt.shard_id) != Some(&receipt.region_id)
            || region.state != RegionMetadataState::Active
            || region.incarnation != receipt.region_incarnation
            || region.completed_used != receipt.start_offset
            || receipt.end_offset > region.reserved_used
            || receipt.start_offset >= receipt.end_offset
            || receipt.record_count == 0
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        let physical_record_count = region
            .physical_record_count
            .checked_add(receipt.record_count)
            .ok_or(RegionMutationError::ArithmeticOverflow)?;
        let region = &mut self.regions[region_index];
        region.completed_used = receipt.end_offset;
        region.max_seqno = region.max_seqno.max(receipt.max_seqno);
        region.physical_record_count = physical_record_count;
        self.shard_mutations[receipt.shard_id].submitted_span = None;
        Ok(())
    }

    /// Selects the next FIFO victim without changing manager authority.
    ///
    /// [`Self::begin_rotation`] validates the same FIFO selection again before
    /// it mutates any state.
    pub(crate) fn plan_rotation(
        &self,
        shard_id: usize,
    ) -> Result<RegionRotationPlan, RegionMutationError> {
        Ok(self.select_rotation(shard_id)?.plan)
    }

    #[cfg(test)]
    pub(crate) fn request_rotation_for_test(
        &mut self,
        shard_id: usize,
    ) -> Result<(), RegionMutationError> {
        let shard = self
            .shard_mutations
            .get_mut(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        shard.rotation_requested = true;
        Ok(())
    }

    /// Starts one previously planned FIFO rotation without performing I/O.
    /// Free Regions are used first; once exhausted, the oldest sealed Region
    /// generation is reused. The outgoing Active Region is withheld from the
    /// FIFO until [`Self::finish_rotation`] commits the in-memory rotation.
    ///
    /// This remains the only rotation mutation authority. A stale plan is
    /// rejected before a sequence number or queue entry is consumed.
    pub(crate) fn begin_rotation(
        &mut self,
        plan: RegionRotationPlan,
    ) -> Result<RegionRotationReceipt, RegionMutationError> {
        let selection = self.select_rotation(plan.shard_id)?;
        if selection.plan != plan {
            return Err(RegionMutationError::StaleReceipt);
        }
        let RegionRotationSelection {
            old_index,
            old,
            victim_index,
            activated_incarnation: incarnation,
            ..
        } = selection;
        let shard_id = plan.shard_id;
        let victim_region_id = plan.victim_region_id;
        let reused = plan.reused;
        let created_seqno = self.allocate_seqno()?;
        let removed = if reused {
            self.sealed_regions.pop_front()
        } else {
            self.free_regions.pop_front()
        };
        if removed != Some(victim_region_id) {
            return Err(RegionMutationError::Invariant(
                "rotation victim changed during selection",
            ));
        }

        self.regions[old_index].state = RegionMetadataState::Sealed;
        self.regions[victim_index] = RegionRuntime {
            region_id: victim_region_id,
            incarnation,
            state: RegionMetadataState::Active,
            created_seqno,
            completed_used: 0,
            reserved_used: 0,
            max_seqno: 0,
            physical_record_count: 0,
        };
        self.active_regions[shard_id] = victim_region_id;
        let receipt = RegionRotationReceipt {
            shard_id,
            sealed_region_id: old.region_id,
            activated_region_id: victim_region_id,
            activated_incarnation: incarnation,
            activated_created_seqno: created_seqno,
            reused,
        };
        self.shard_mutations[shard_id].rotation_requested = false;
        self.shard_mutations[shard_id].rotation = Some(receipt);
        Ok(receipt)
    }

    fn select_rotation(
        &self,
        shard_id: usize,
    ) -> Result<RegionRotationSelection, RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if !shard.rotation_requested
            || shard.tail.is_some()
            || shard.open_span.is_some()
            || shard.pending_padding.is_some()
            || shard.submitted_span.is_some()
            || shard.rotation.is_some()
        {
            return Err(RegionMutationError::WouldBlock);
        }
        let old_region_id = *self
            .active_regions
            .get(shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        let old_index =
            usize::try_from(old_region_id).map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let old = self
            .regions
            .get(old_index)
            .copied()
            .ok_or(RegionMutationError::Invariant(
                "active Region id is out of bounds",
            ))?;
        if old.region_id != old_region_id
            || old.state != RegionMetadataState::Active
            || old.reserved_used != old.completed_used
        {
            return Err(RegionMutationError::WouldBlock);
        }

        let free = self.free_regions.front().copied();
        let (victim_region_id, reused) = match free {
            Some(region_id) => (region_id, false),
            None => (
                self.sealed_regions
                    .front()
                    .copied()
                    .ok_or(RegionMutationError::WouldBlock)?,
                true,
            ),
        };
        let victim_index = usize::try_from(victim_region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let victim =
            self.regions
                .get(victim_index)
                .copied()
                .ok_or(RegionMutationError::Invariant(
                    "rotation victim id is out of bounds",
                ))?;
        let expected_victim_state = if reused {
            RegionMetadataState::Sealed
        } else {
            RegionMetadataState::Free
        };
        if victim.region_id != victim_region_id
            || victim.state != expected_victim_state
            || victim.region_id == old.region_id
        {
            return Err(RegionMutationError::Invariant(
                "rotation victim queue is inconsistent",
            ));
        }
        let activated_incarnation = victim
            .incarnation
            .checked_add(1)
            .filter(|incarnation| *incarnation != u32::MAX)
            .ok_or(RegionMutationError::IncarnationExhausted)?;
        Ok(RegionRotationSelection {
            plan: RegionRotationPlan {
                shard_id,
                victim_region_id,
                victim_incarnation: victim.incarnation,
                reused,
            },
            old_index,
            old,
            victim_index,
            activated_incarnation,
        })
    }

    /// Publishes the outgoing Region at the tail of the sealed FIFO. A
    /// repeated or late receipt cannot make a Region reachable by the current
    /// generation.
    pub(crate) fn finish_rotation(
        &mut self,
        receipt: RegionRotationReceipt,
    ) -> Result<(), RegionMutationError> {
        let shard = self
            .shard_mutations
            .get(receipt.shard_id)
            .ok_or(RegionMutationError::InvalidShard)?;
        if shard.rotation != Some(receipt) {
            return Err(RegionMutationError::StaleReceipt);
        }
        let sealed_index = usize::try_from(receipt.sealed_region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let activated_index = usize::try_from(receipt.activated_region_id)
            .map_err(|_| RegionMutationError::ArithmeticOverflow)?;
        let sealed = self
            .regions
            .get(sealed_index)
            .ok_or(RegionMutationError::StaleReceipt)?;
        let activated = self
            .regions
            .get(activated_index)
            .ok_or(RegionMutationError::StaleReceipt)?;
        if self.active_regions.get(receipt.shard_id) != Some(&receipt.activated_region_id)
            || sealed.state != RegionMetadataState::Sealed
            || activated.state != RegionMetadataState::Active
            || activated.incarnation != receipt.activated_incarnation
            || activated.created_seqno != receipt.activated_created_seqno
        {
            return Err(RegionMutationError::StaleReceipt);
        }
        if self
            .free_regions
            .len()
            .saturating_add(self.sealed_regions.len())
            >= self.queue_capacity
        {
            return Err(RegionMutationError::Invariant(
                "sealed Region queue exceeded its reserved capacity",
            ));
        }
        self.sealed_regions.push_back(receipt.sealed_region_id);
        self.rotations = self.rotations.saturating_add(1);
        self.shard_mutations[receipt.shard_id].rotation = None;
        Ok(())
    }

    /// Freezes the complete Region metadata table against the current
    /// canonical index partition directory and physical counters supplied by the
    /// index owner.
    pub(crate) fn freeze_metadata(
        &self,
        partitions: Box<[PartitionMetadataRecord]>,
    ) -> Result<RegionMetadata, RegionMetadataError> {
        if self
            .shard_mutations
            .iter()
            .any(|shard| !shard.is_quiescent())
        {
            return Err(RegionMetadataError::InvalidField("live_region_authority"));
        }
        let partition_totals = PartitionTotals::from_records(&partitions)?;
        let queue_ordinals = self.freeze_queue_ordinals()?;
        let mut records = try_vec(self.regions.len())?;

        for (expected_id, region) in self.regions.iter().enumerate() {
            if region.region_id as usize != expected_id
                || region.reserved_used != region.completed_used
            {
                return Err(RegionMetadataError::InvalidField("live_region_authority"));
            }
            records.push(RegionMetadataRecord {
                region_id: region.region_id,
                incarnation: region.incarnation,
                state: region.state,
                queue_ordinal: queue_ordinals[expected_id],
                created_seqno: region.created_seqno,
                durable_used_offset: region.completed_used,
                max_seqno: region.max_seqno,
                physical_record_count: region.physical_record_count,
            });
        }

        let region_count = u32::try_from(self.regions.len())
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let shard_count = u32::try_from(self.active_regions.len())
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let free_region_count = u32::try_from(self.free_regions.len())
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let sealed_region_count = u32::try_from(self.sealed_regions.len())
            .map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let max_seqno = self
            .next_seqno
            .checked_sub(1)
            .ok_or(RegionMetadataError::ArithmeticOverflow)?;

        let metadata = RegionMetadata {
            root: RegionMetadataRoot {
                cache_uuid: self.binding.cache_uuid,
                data_identity: self.binding.data_identity,
                data_superblock_generation: self.binding.data_superblock_generation,
                image_identity: self.binding.image_identity,
                image_generation: self.binding.image_generation,
                config_fingerprint: self.binding.config_fingerprint,
                index_slots: partition_totals.slot_count,
                index_page_count: partition_totals.page_count,
                region_size: self.region_size,
                region_count,
                partition_count: partition_totals.partition_count,
                shard_count,
                max_seqno,
                free_region_count,
                active_region_count: shard_count,
                sealed_region_count,
            },
            regions: records.into_boxed_slice(),
            partitions,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    fn freeze_queue_ordinals(&self) -> Result<Vec<u32>, RegionMetadataError> {
        let mut ordinals = try_unassigned_vec(self.regions.len())?;
        install_live_queue(
            &self.regions,
            RegionMetadataState::Active,
            self.active_regions.iter().copied(),
            &mut ordinals,
        )?;
        install_live_queue(
            &self.regions,
            RegionMetadataState::Free,
            self.free_regions.iter().copied(),
            &mut ordinals,
        )?;
        install_live_queue(
            &self.regions,
            RegionMetadataState::Sealed,
            self.sealed_regions.iter().copied(),
            &mut ordinals,
        )?;
        if ordinals.contains(&UNASSIGNED_REGION) {
            return Err(RegionMetadataError::InvalidField(
                "live_region_queue_permutation",
            ));
        }
        Ok(ordinals)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PartitionTotals {
    partition_count: u32,
    page_count: u64,
    slot_count: u64,
}

impl PartitionTotals {
    fn from_records(partitions: &[PartitionMetadataRecord]) -> Result<Self, RegionMetadataError> {
        let mut totals = Self {
            partition_count: u32::try_from(partitions.len())
                .map_err(|_| RegionMetadataError::ArithmeticOverflow)?,
            ..Self::default()
        };
        for partition in partitions {
            totals.page_count = totals
                .page_count
                .checked_add(partition.index_page_count)
                .ok_or(RegionMetadataError::ArithmeticOverflow)?;
            totals.slot_count = totals
                .slot_count
                .checked_add(partition.slot_count)
                .ok_or(RegionMetadataError::ArithmeticOverflow)?;
        }
        Ok(totals)
    }
}

fn install_recovered_queue_entry(
    state: RegionMetadataState,
    ordinal: u32,
    region_id: u32,
    active: &mut [u32],
    free: &mut VecDeque<u32>,
    sealed: &mut VecDeque<u32>,
) -> Result<(), RegionMetadataError> {
    let target: &mut [u32] = match state {
        RegionMetadataState::Active => active,
        RegionMetadataState::Free => free.make_contiguous(),
        RegionMetadataState::Sealed => sealed.make_contiguous(),
    };
    let ordinal = usize::try_from(ordinal).map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
    let slot = target
        .get_mut(ordinal)
        .ok_or(RegionMetadataError::InvalidField("region_queue_ordinal"))?;
    if *slot != UNASSIGNED_REGION {
        return Err(RegionMetadataError::InvalidField("region_queue_ordinal"));
    }
    *slot = region_id;
    Ok(())
}

fn install_live_queue<I>(
    regions: &[RegionRuntime],
    expected_state: RegionMetadataState,
    queue: I,
    ordinals: &mut [u32],
) -> Result<(), RegionMetadataError>
where
    I: IntoIterator<Item = u32>,
{
    for (ordinal, region_id) in queue.into_iter().enumerate() {
        let region_index =
            usize::try_from(region_id).map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        let region = regions
            .get(region_index)
            .ok_or(RegionMetadataError::InvalidField("live_region_queue"))?;
        let ordinal =
            u32::try_from(ordinal).map_err(|_| RegionMetadataError::ArithmeticOverflow)?;
        if region.region_id != region_id
            || region.state != expected_state
            || ordinals[region_index] != UNASSIGNED_REGION
        {
            return Err(RegionMetadataError::InvalidField(
                "live_region_queue_permutation",
            ));
        }
        ordinals[region_index] = ordinal;
    }
    Ok(())
}

fn try_vec<T>(capacity: usize) -> Result<Vec<T>, RegionMetadataError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| RegionMetadataError::Allocation)?;
    Ok(values)
}

fn try_unassigned_vec(count: usize) -> Result<Vec<u32>, RegionMetadataError> {
    let mut values = try_vec(count)?;
    values.resize(count, UNASSIGNED_REGION);
    Ok(values)
}

fn try_unassigned_queue(
    count: usize,
    capacity: usize,
) -> Result<VecDeque<u32>, RegionMetadataError> {
    let mut values = VecDeque::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| RegionMetadataError::Allocation)?;
    values.resize(count, UNASSIGNED_REGION);
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_storage::canonical_index_partition_ranges;

    fn id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn sample() -> RegionMetadata {
        let ranges = canonical_index_partition_ranges(200).unwrap();
        let mut shards = ranges
            .iter()
            .map(|range| PartitionMetadataRecord {
                partition_id: range.partition_id as u32,
                first_index_page: range.first_page as u64,
                index_page_count: range.page_count as u64,
                first_slot: range.first_slot as u64,
                slot_count: range.slot_count as u64,
                physical_value_slots: 0,
                physical_deleted_slots: 0,
            })
            .collect::<Vec<_>>();
        shards[0].physical_value_slots = 1;
        shards[0].physical_deleted_slots = 1;
        shards[1].physical_deleted_slots = 1;

        RegionMetadata {
            root: RegionMetadataRoot {
                cache_uuid: id(1),
                data_identity: id(2),
                data_superblock_generation: 3,
                image_identity: id(4),
                image_generation: 5,
                config_fingerprint: 6,
                index_slots: 200,
                index_page_count: 2,
                region_size: 32 * 1024 * 1024,
                region_count: 6,
                partition_count: 2,
                shard_count: 2,
                max_seqno: 7,
                free_region_count: 2,
                active_region_count: 2,
                sealed_region_count: 2,
            },
            regions: vec![
                region(0, 3, RegionMetadataState::Active, 1, 2, 0, 0),
                region(1, 4, RegionMetadataState::Free, 1, 0, 0, 0),
                region(2, 2, RegionMetadataState::Sealed, 1, 7, 64, 7),
                region(3, 1, RegionMetadataState::Active, 0, 1, 0, 0),
                region(4, 8, RegionMetadataState::Sealed, 0, 4, 128, 5),
                region(5, 7, RegionMetadataState::Free, 0, 0, 0, 0),
            ]
            .into_boxed_slice(),
            partitions: shards.into_boxed_slice(),
        }
    }

    fn region(
        region_id: u32,
        incarnation: u32,
        state: RegionMetadataState,
        queue_ordinal: u32,
        created_seqno: u64,
        used_bytes: u64,
        max_seqno: u64,
    ) -> RegionMetadataRecord {
        RegionMetadataRecord {
            region_id,
            incarnation,
            state,
            queue_ordinal,
            created_seqno,
            durable_used_offset: used_bytes,
            max_seqno,
            physical_record_count: used_bytes / 64,
        }
    }

    fn sample_without_free_regions() -> RegionMetadata {
        let mut metadata = sample();
        metadata.root.free_region_count = 0;
        metadata.root.sealed_region_count = 4;

        metadata.regions[4].created_seqno = 3;
        metadata.regions[4].queue_ordinal = 0;

        metadata.regions[1].state = RegionMetadataState::Sealed;
        metadata.regions[1].created_seqno = 4;
        metadata.regions[1].queue_ordinal = 1;

        metadata.regions[5].state = RegionMetadataState::Sealed;
        metadata.regions[5].created_seqno = 6;
        metadata.regions[5].queue_ordinal = 2;

        metadata.regions[2].queue_ordinal = 3;
        metadata.validate().unwrap();
        metadata
    }

    fn request_rotation(manager: &mut RegionManager, shard_id: usize) {
        manager.request_rotation_for_test(shard_id).unwrap();
    }

    #[test]
    fn restores_non_id_queue_and_shard_order() {
        let manager = RegionManager::from_metadata(sample()).unwrap();
        assert_eq!(manager.active_regions(), &[3, 0]);
        assert_eq!(
            manager.free_regions().iter().copied().collect::<Vec<_>>(),
            [5, 1]
        );
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [4, 2]
        );
        assert!(
            manager
                .regions()
                .iter()
                .all(|region| region.reserved_used == region.completed_used)
        );
        assert_eq!(manager.next_seqno(), 8);
    }

    #[test]
    fn freeze_preserves_physical_region_state() {
        let source = sample();
        let shards = source.partitions.clone();
        let manager = RegionManager::from_metadata(source.clone()).unwrap();

        assert_eq!(manager.freeze_metadata(shards).unwrap(), source);
    }

    #[test]
    fn invalid_metadata_is_rejected_before_install() {
        let mut invalid = sample();
        invalid.regions[1].queue_ordinal = 0;
        assert_eq!(
            RegionManager::from_metadata(invalid).unwrap_err(),
            RegionMetadataError::InvalidField("region_queue_ordinal")
        );
    }

    #[test]
    fn shard_accounting_overflow_is_rejected_during_freeze() {
        let metadata = sample();
        let mut shards = metadata.partitions.clone();
        let manager = RegionManager::from_metadata(metadata).unwrap();
        shards[0].physical_value_slots = u64::MAX;
        shards[1].physical_value_slots = 1;
        assert_eq!(
            manager.freeze_metadata(shards).unwrap_err(),
            RegionMetadataError::ArithmeticOverflow
        );
    }

    #[test]
    fn tail_reservation_is_exclusive_until_staged_or_cancelled() {
        let mut manager = RegionManager::from_metadata(sample()).unwrap();
        assert_eq!(
            manager.reserve_append(0, 0),
            Err(RegionMutationError::InvalidRecordLength)
        );
        assert_eq!(
            manager.reserve_append(0, RECORD_ALIGNMENT + 1),
            Err(RegionMutationError::InvalidRecordLength)
        );
        assert_eq!(
            manager.reserve_append(99, 64),
            Err(RegionMutationError::InvalidShard)
        );

        let reservation = manager.reserve_append(0, 64).unwrap();
        assert_eq!(
            reservation,
            RegionAppendReservation {
                shard_id: 0,
                region_id: 3,
                region_incarnation: 1,
                offset: 0,
                record_bytes: 64,
                seqno: 8,
            }
        );
        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationError::WouldBlock)
        );
        assert_eq!(manager.regions[3].completed_used, 0);
        assert_eq!(manager.regions[3].reserved_used, 64);

        let mut stale = reservation;
        stale.seqno += 1;
        assert_eq!(
            manager.cancel_reservation(stale),
            Err(RegionMutationError::StaleReceipt)
        );
        manager.cancel_reservation(reservation).unwrap();
        assert_eq!(manager.regions[3].reserved_used, 0);
        assert_eq!(
            manager.cancel_reservation(reservation),
            Err(RegionMutationError::StaleReceipt)
        );
        assert_eq!(manager.next_seqno(), 9);
    }

    #[test]
    fn many_records_share_ordered_spans_without_waiting_per_record() {
        let metadata = sample();
        let shards = metadata.partitions.clone();
        let mut manager = RegionManager::from_metadata(metadata).unwrap();

        let first = manager.reserve_append(0, 64).unwrap();
        manager.stage_reservation(first).unwrap();
        let second = manager.reserve_append(0, 128).unwrap();
        manager.stage_reservation(second).unwrap();
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(padding.span_start_offset, 0);
        assert_eq!(padding.unpadded_end_offset, 192);
        assert_eq!(padding.padded_end_offset, 4096);
        assert_eq!(padding.padding_bytes(), Some(3904));
        assert_eq!(padding.record_count, 2);
        assert_eq!(padding.max_seqno, second.seqno);
        assert_eq!(manager.regions[3].reserved_used, 4096);
        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationError::WouldBlock)
        );
        assert_eq!(
            manager.seal_write_span(0),
            Err(RegionMutationError::WouldBlock)
        );
        let mut stale_padding = padding;
        stale_padding.max_seqno -= 1;
        assert_eq!(
            manager.seal_write_span_with_padding(stale_padding),
            Err(RegionMutationError::StaleReceipt)
        );
        let submitted = manager.seal_write_span_with_padding(padding).unwrap();
        assert_eq!(submitted.start_offset, 0);
        assert_eq!(submitted.end_offset, 4096);
        assert_eq!(submitted.record_count, 2);
        assert_eq!(submitted.max_seqno, second.seqno);
        assert_eq!(manager.regions[3].completed_used, 0);

        // The next staging chunk is filled while the first span is in flight.
        let third = manager.reserve_append(0, 64).unwrap();
        assert_eq!(third.offset, 4096);
        manager.stage_reservation(third).unwrap();
        assert_eq!(
            manager.seal_write_span(0),
            Err(RegionMutationError::WouldBlock)
        );
        assert_eq!(
            manager.freeze_metadata(shards.clone()),
            Err(RegionMetadataError::InvalidField("live_region_authority"))
        );

        manager.complete_write_span(submitted).unwrap();
        assert_eq!(manager.regions[3].completed_used, 4096);
        assert_eq!(manager.regions[3].physical_record_count, 2);
        assert_eq!(manager.regions[3].max_seqno, second.seqno);
        assert_eq!(
            manager.complete_write_span(submitted),
            Err(RegionMutationError::StaleReceipt)
        );

        let next_padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(next_padding.padding_bytes(), Some(4032));
        let next = manager.seal_write_span_with_padding(next_padding).unwrap();
        assert_eq!(next.start_offset, 4096);
        assert_eq!(next.end_offset, 8192);
        assert_eq!(next.record_count, 1);
        manager.complete_write_span(next).unwrap();
        assert_eq!(manager.regions[3].completed_used, 8192);
        assert_eq!(manager.regions[3].reserved_used, 8192);
        assert_eq!(manager.regions[3].physical_record_count, 3);
        manager.freeze_metadata(shards).unwrap();
    }

    #[test]
    fn full_region_flushes_its_open_tail_before_requesting_rotation() {
        let mut manager = RegionManager::from_metadata(sample()).unwrap();
        let active_region = manager.active_regions()[0] as usize;
        let record_bytes = u32::try_from(manager.region_size() - RECORD_ALIGNMENT as u64).unwrap();
        let reservation = manager.reserve_append(0, record_bytes).unwrap();
        manager.stage_reservation(reservation).unwrap();

        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationError::FlushBeforeRotation)
        );
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(padding.padding_bytes(), Some(RECORD_ALIGNMENT));
        let span = manager.seal_write_span_with_padding(padding).unwrap();
        manager.complete_write_span(span).unwrap();
        assert_eq!(
            manager.regions()[active_region].completed_used,
            manager.region_size()
        );
        assert_eq!(
            manager.reserve_append(0, RECORD_ALIGNMENT),
            Err(RegionMutationError::RegionFull)
        );
    }

    #[test]
    fn rotation_prefers_free_and_withholds_old_active_until_finish() {
        let metadata = sample();
        let shards = metadata.partitions.clone();
        let mut manager = RegionManager::from_metadata(metadata).unwrap();

        request_rotation(&mut manager, 0);
        let plan = manager.plan_rotation(0).unwrap();
        assert_eq!(
            plan,
            RegionRotationPlan {
                shard_id: 0,
                victim_region_id: 5,
                victim_incarnation: 7,
                reused: false,
            }
        );
        let rotation = manager.begin_rotation(plan).unwrap();
        assert!(!rotation.reused);
        assert_eq!(rotation.sealed_region_id, 3);
        assert_eq!(rotation.activated_region_id, 5);
        assert_eq!(rotation.activated_incarnation, 8);
        assert_eq!(rotation.activated_created_seqno, 8);
        assert_eq!(manager.active_regions(), &[5, 0]);
        assert_eq!(
            manager.free_regions().iter().copied().collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [4, 2]
        );
        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationError::WouldBlock)
        );
        assert_eq!(
            manager.freeze_metadata(shards.clone()),
            Err(RegionMetadataError::InvalidField("live_region_authority"))
        );

        manager.finish_rotation(rotation).unwrap();
        assert_eq!(
            manager.plan_rotation(0),
            Err(RegionMutationError::WouldBlock),
            "a delayed duplicate wake must not rotate the new empty Region"
        );
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [4, 2, 3]
        );
        assert_eq!(
            manager.finish_rotation(rotation),
            Err(RegionMutationError::StaleReceipt)
        );
        manager.freeze_metadata(shards).unwrap();
    }

    #[test]
    fn fifo_reuse_bumps_generation_and_resets_physical_state() {
        let metadata = sample_without_free_regions();
        let shards = metadata.partitions.clone();
        let mut manager = RegionManager::from_metadata(metadata).unwrap();
        request_rotation(&mut manager, 0);
        let plan = manager.plan_rotation(0).unwrap();
        assert_eq!(
            plan,
            RegionRotationPlan {
                shard_id: 0,
                victim_region_id: 4,
                victim_incarnation: 8,
                reused: true,
            }
        );
        let rotation = manager.begin_rotation(plan).unwrap();
        assert!(rotation.reused);
        assert_eq!(rotation.activated_region_id, 4);
        assert_eq!(rotation.activated_incarnation, 9);
        assert_eq!(rotation.activated_created_seqno, 8);
        assert_eq!(manager.regions[4].physical_record_count, 0);

        manager.finish_rotation(rotation).unwrap();
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [1, 5, 2, 3]
        );
        let frozen = manager.freeze_metadata(shards).unwrap();
        assert_eq!(frozen.regions[4].physical_record_count, 0);
    }

    #[test]
    fn stale_rotation_plan_does_not_consume_the_next_victim_or_seqno() {
        let mut manager = RegionManager::from_metadata(sample()).unwrap();
        request_rotation(&mut manager, 0);
        let stale = manager.plan_rotation(0).unwrap();

        request_rotation(&mut manager, 1);
        let other_plan = manager.plan_rotation(1).unwrap();
        let other_rotation = manager.begin_rotation(other_plan).unwrap();
        manager.finish_rotation(other_rotation).unwrap();
        let next_seqno = manager.next_seqno();
        let active = manager.active_regions().to_vec();
        let free = manager.free_regions().clone();
        let sealed = manager.sealed_regions().clone();

        assert_eq!(
            manager.begin_rotation(stale),
            Err(RegionMutationError::StaleReceipt)
        );
        assert_eq!(manager.next_seqno(), next_seqno);
        assert_eq!(manager.active_regions(), active);
        assert_eq!(manager.free_regions(), &free);
        assert_eq!(manager.sealed_regions(), &sealed);
        assert_eq!(manager.plan_rotation(0).unwrap().victim_region_id, 1);
    }

    #[test]
    fn rotation_plan_rejects_a_victim_queue_state_mismatch() {
        let mut manager = RegionManager::from_metadata(sample()).unwrap();
        let next_seqno = manager.next_seqno();
        let active = manager.active_regions().to_vec();
        let sealed = manager.sealed_regions().clone();

        // Region 4 is Sealed, so it cannot be selected from the Free FIFO.
        manager.free_regions[0] = 4;
        request_rotation(&mut manager, 0);
        assert_eq!(
            manager.plan_rotation(0),
            Err(RegionMutationError::Invariant(
                "rotation victim queue is inconsistent"
            ))
        );
        assert_eq!(manager.next_seqno(), next_seqno);
        assert_eq!(manager.active_regions(), active);
        assert_eq!(manager.sealed_regions(), &sealed);
    }

    #[test]
    fn incomplete_or_late_span_cannot_cross_a_region_generation() {
        let mut manager = RegionManager::from_metadata(sample()).unwrap();
        let reservation = manager.reserve_append(0, 64).unwrap();
        manager.stage_reservation(reservation).unwrap();
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        let span = manager.seal_write_span_with_padding(padding).unwrap();
        request_rotation(&mut manager, 0);
        assert_eq!(
            manager.plan_rotation(0),
            Err(RegionMutationError::WouldBlock)
        );
        manager.complete_write_span(span).unwrap();

        let plan = manager.plan_rotation(0).unwrap();
        let rotation = manager.begin_rotation(plan).unwrap();
        manager.finish_rotation(rotation).unwrap();
        let activated = manager.regions[rotation.activated_region_id as usize];
        assert_eq!(
            manager.complete_write_span(span),
            Err(RegionMutationError::StaleReceipt)
        );
        assert_eq!(
            manager.regions[rotation.activated_region_id as usize],
            activated
        );
    }

    #[test]
    fn incarnation_exhaustion_does_not_consume_fifo_or_seqno() {
        let mut metadata = sample();
        metadata.regions[5].incarnation = u32::MAX - 1;
        let mut manager = RegionManager::from_metadata(metadata).unwrap();
        let next_seqno = manager.next_seqno();
        request_rotation(&mut manager, 0);
        assert_eq!(
            manager.plan_rotation(0),
            Err(RegionMutationError::IncarnationExhausted)
        );
        assert_eq!(manager.next_seqno(), next_seqno);
        assert_eq!(manager.active_regions(), &[3, 0]);
        assert_eq!(
            manager.free_regions().iter().copied().collect::<Vec<_>>(),
            [5, 1]
        );
    }
}
