//! In-memory Region authority reconstructed from one validated clean V2 image.
//!
//! The manager deliberately does not retain the recovered index shard
//! directory or its physical counters. A clean freeze accepts the current
//! canonical shard records from the index owner and derives every Region,
//! queue, and root accounting field from live manager state.

use crate::format::{RegionHeader, RegionState};
use crate::index::{INDEX_FLAG_VOLATILE, IndexEntry};
use crate::index_storage::IndexSlotStateV1;
use crate::index_v2::IndexTransitionV2;
use crate::io_backend::DIRECT_IO_ALIGNMENT;
use crate::recovery_v2::{CacheEpochV2, PersistentId, RECORD_ALIGNMENT_V2, REGION_HEADER_SIZE_V2};
use crate::region_metadata_v1::{
    RegionMetadataRecordV1, RegionMetadataRootV1, RegionMetadataStateV1, RegionMetadataV1,
    RegionMetadataV1Error, ShardMetadataRecordV1,
};
use std::collections::VecDeque;

const UNASSIGNED_REGION: u32 = u32::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionMetadataBindingV2 {
    pub(crate) cache_uuid: PersistentId,
    pub(crate) data_identity: PersistentId,
    pub(crate) data_superblock_generation: u64,
    pub(crate) image_identity: PersistentId,
    pub(crate) image_generation: u64,
    pub(crate) config_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegionLogicalAccountingV2 {
    pub(crate) live_record_count: u64,
    pub(crate) live_record_bytes: u64,
}

impl RegionLogicalAccountingV2 {
    fn checked_add_region(self, region: &RegionRuntimeV2) -> Result<Self, RegionMetadataV1Error> {
        Ok(Self {
            live_record_count: self
                .live_record_count
                .checked_add(region.logical.live_record_count)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?,
            live_record_bytes: self
                .live_record_bytes
                .checked_add(region.logical.live_record_bytes)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RegionLogicalChargeV2 {
    region_index: usize,
    record_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegionWriteBudgetV2 {
    /// Zero means that no persisted write-budget window is active.
    pub(crate) window: u64,
    pub(crate) used_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RegionMutationErrorV2 {
    InvalidLane,
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

/// Exclusive tail reservation owned by one append lane until its encoded
/// bytes have either entered staging or been cancelled. Device completion is
/// deliberately represented by [`RegionWriteSpanV2`], not by this per-record
/// receipt, so one lane can accumulate many records into a MiB-scale write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionAppendReservationV2 {
    pub(crate) lane_id: usize,
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) offset: u32,
    pub(crate) record_bytes: u32,
    pub(crate) seqno: u64,
}

impl RegionAppendReservationV2 {
    fn end_offset(self) -> Option<u64> {
        u64::from(self.offset).checked_add(u64::from(self.record_bytes))
    }
}

/// Exclusive tail padding for one non-empty open write span. The manager owns
/// both offsets; staging may only extend its final record after validating this
/// exact generation and span identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionPaddingReceiptV2 {
    pub(crate) lane_id: usize,
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) span_start_offset: u64,
    pub(crate) unpadded_end_offset: u64,
    pub(crate) padded_end_offset: u64,
    pub(crate) record_count: u64,
    pub(crate) max_seqno: u64,
}

impl RegionPaddingReceiptV2 {
    pub(crate) fn padding_bytes(self) -> Option<u32> {
        self.padded_end_offset
            .checked_sub(self.unpadded_end_offset)
            .and_then(|padding| u32::try_from(padding).ok())
    }
}

/// One ordered staging span. A lane has at most one submitted span while it
/// continues filling the next resident staging chunk. Completion advances the
/// written cursor only when this exact generation and start offset still own
/// the lane. Durability is established once, by the CLEAN data sync.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionWriteSpanV2 {
    pub(crate) lane_id: usize,
    pub(crate) span_id: u64,
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) region_id: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) start_offset: u64,
    pub(crate) end_offset: u64,
    pub(crate) record_count: u64,
    pub(crate) max_seqno: u64,
}

/// Headers which must be written outside the manager's short critical
/// section. Until [`RegionManagerV2::finish_rotation`] accepts this exact
/// receipt, the lane rejects new reservations and the sealed Region is kept
/// out of the reclaim FIFO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRotationReceiptV2 {
    pub(crate) lane_id: usize,
    pub(crate) cache_epoch: CacheEpochV2,
    pub(crate) sealed: RegionHeader,
    pub(crate) activated: RegionHeader,
    pub(crate) reused: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OpenWriteSpanV2 {
    cache_epoch: CacheEpochV2,
    region_id: u32,
    region_incarnation: u32,
    start_offset: u64,
    end_offset: u64,
    record_count: u64,
    max_seqno: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LaneMutationV2 {
    tail: Option<RegionAppendReservationV2>,
    open_span: Option<OpenWriteSpanV2>,
    pending_padding: Option<RegionPaddingReceiptV2>,
    submitted_span: Option<RegionWriteSpanV2>,
    rotation: Option<RegionRotationReceiptV2>,
    next_span_id: u64,
}

impl Default for LaneMutationV2 {
    fn default() -> Self {
        Self {
            tail: None,
            open_span: None,
            pending_padding: None,
            submitted_span: None,
            rotation: None,
            next_span_id: 1,
        }
    }
}

impl LaneMutationV2 {
    const fn is_quiescent(self) -> bool {
        self.tail.is_none()
            && self.open_span.is_none()
            && self.pending_padding.is_none()
            && self.submitted_span.is_none()
            && self.rotation.is_none()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRuntimeV2 {
    pub(crate) region_id: u32,
    pub(crate) incarnation: u32,
    pub(crate) state: RegionMetadataStateV1,
    pub(crate) created_seqno: u64,
    /// Last byte covered by a successful write completion. A buffered or
    /// io_uring CQE is not a durability barrier; CLEAN later syncs this prefix.
    pub(crate) completed_used: u64,
    /// Reservation cursor. A clean recovery has no outstanding writes, so it
    /// starts exactly at `completed_used`.
    pub(crate) reserved_used: u64,
    pub(crate) max_seqno: u64,
    pub(crate) physical_record_count: u64,
    pub(crate) logical: RegionLogicalAccountingV2,
}

#[derive(Debug)]
pub(crate) struct RegionManagerV2 {
    binding: RegionMetadataBindingV2,
    region_size: u64,
    cache_epoch: CacheEpochV2,
    clear_floor_seqno: u64,
    next_seqno: u64,
    regions: Vec<RegionRuntimeV2>,
    active_regions: Vec<u32>,
    lane_mutations: Vec<LaneMutationV2>,
    free_regions: VecDeque<u32>,
    sealed_regions: VecDeque<u32>,
    write_budget: RegionWriteBudgetV2,
}

impl RegionManagerV2 {
    /// Consumes one complete CLEAN metadata image and reconstructs runtime
    /// authority without scanning the index or Region data records.
    pub(crate) fn from_metadata(metadata: RegionMetadataV1) -> Result<Self, RegionMetadataV1Error> {
        metadata.validate()?;
        let RegionMetadataV1 {
            root,
            regions: encoded_regions,
            shards,
        } = metadata;
        // Shard topology and physical accounting are owned by the live index.
        // Do not retain the recovered copy as a second authority.
        drop(shards);

        let region_count = usize::try_from(root.region_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let active_count = usize::try_from(root.append_lane_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let free_count = usize::try_from(root.free_region_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let sealed_count = usize::try_from(root.sealed_region_count)
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let sealed_capacity = region_count
            .checked_sub(active_count)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;

        let mut regions = try_vec(region_count)?;
        let mut active_regions = try_unassigned_vec(active_count)?;
        let mut lane_mutations = try_vec(active_count)?;
        lane_mutations.resize(active_count, LaneMutationV2::default());
        let mut free_regions = try_unassigned_queue(free_count, free_count)?;
        let mut sealed_regions = try_unassigned_queue(sealed_count, sealed_capacity)?;

        for encoded in encoded_regions.iter().copied() {
            let runtime = RegionRuntimeV2 {
                region_id: encoded.region_id,
                incarnation: encoded.incarnation,
                state: encoded.state,
                created_seqno: encoded.created_seqno,
                completed_used: encoded.durable_used_offset,
                reserved_used: encoded.durable_used_offset,
                max_seqno: encoded.max_seqno,
                physical_record_count: encoded.physical_record_count,
                logical: RegionLogicalAccountingV2 {
                    live_record_count: encoded.live_record_count,
                    live_record_bytes: encoded.live_record_bytes,
                },
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
            return Err(RegionMetadataV1Error::InvalidField(
                "region_queue_permutation",
            ));
        }

        let next_seqno = root
            .max_seqno
            .checked_add(1)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        Ok(Self {
            binding: RegionMetadataBindingV2 {
                cache_uuid: root.cache_uuid,
                data_identity: root.data_identity,
                data_superblock_generation: root.data_superblock_generation,
                image_identity: root.image_identity,
                image_generation: root.image_generation,
                config_fingerprint: root.config_fingerprint,
            },
            region_size: root.region_size,
            cache_epoch: root.cache_epoch,
            clear_floor_seqno: root.clear_floor_seqno,
            next_seqno,
            regions,
            active_regions,
            lane_mutations,
            free_regions,
            sealed_regions,
            write_budget: RegionWriteBudgetV2 {
                window: root.write_budget_window,
                used_bytes: root.write_budget_used_bytes,
            },
        })
    }

    pub(crate) const fn binding(&self) -> RegionMetadataBindingV2 {
        self.binding
    }

    pub(crate) const fn region_size(&self) -> u64 {
        self.region_size
    }

    pub(crate) const fn cache_epoch(&self) -> CacheEpochV2 {
        self.cache_epoch
    }

    pub(crate) const fn clear_floor_seqno(&self) -> u64 {
        self.clear_floor_seqno
    }

    pub(crate) const fn next_seqno(&self) -> u64 {
        self.next_seqno
    }

    pub(crate) fn regions(&self) -> &[RegionRuntimeV2] {
        &self.regions
    }

    pub(crate) fn active_regions(&self) -> &[u32] {
        &self.active_regions
    }

    pub(crate) const fn free_regions(&self) -> &VecDeque<u32> {
        &self.free_regions
    }

    pub(crate) const fn sealed_regions(&self) -> &VecDeque<u32> {
        &self.sealed_regions
    }

    pub(crate) const fn write_budget(&self) -> RegionWriteBudgetV2 {
        self.write_budget
    }

    /// Allocates one process-local ordering version. Sequence exhaustion is a
    /// terminal condition for the current cache identity; `u64::MAX` is never
    /// issued because clean metadata reserves it as invalid.
    pub(crate) fn allocate_seqno(&mut self) -> Result<u64, RegionMutationErrorV2> {
        if self.next_seqno == u64::MAX {
            return Err(RegionMutationErrorV2::SequenceExhausted);
        }
        let allocated = self.next_seqno;
        self.next_seqno += 1;
        Ok(allocated)
    }

    /// Reserves the aligned tail of one lane's Active Region. Only the bytes
    /// needed to encode this record are exclusive; once staged, the lane may
    /// reserve the next record without waiting for device completion.
    pub(crate) fn reserve_append(
        &mut self,
        lane_id: usize,
        record_bytes: u32,
    ) -> Result<RegionAppendReservationV2, RegionMutationErrorV2> {
        if record_bytes == 0 || record_bytes % RECORD_ALIGNMENT_V2 != 0 {
            return Err(RegionMutationErrorV2::InvalidRecordLength);
        }
        let lane = self
            .lane_mutations
            .get(lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail.is_some() || lane.pending_padding.is_some() || lane.rotation.is_some() {
            return Err(RegionMutationErrorV2::WouldBlock);
        }
        let region_id = *self
            .active_regions
            .get(lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        let region_index =
            usize::try_from(region_id).map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let region =
            self.regions
                .get(region_index)
                .copied()
                .ok_or(RegionMutationErrorV2::Invariant(
                    "active Region id is out of bounds",
                ))?;
        if region.state != RegionMetadataStateV1::Active || region.region_id != region_id {
            return Err(RegionMutationErrorV2::Invariant(
                "append lane does not own an Active Region",
            ));
        }
        let end = region
            .reserved_used
            .checked_add(u64::from(record_bytes))
            .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?;
        if end > self.region_size {
            return Err(if lane.open_span.is_some() {
                RegionMutationErrorV2::FlushBeforeRotation
            } else if lane.submitted_span.is_some() {
                RegionMutationErrorV2::WouldBlock
            } else {
                RegionMutationErrorV2::RegionFull
            });
        }
        let offset = u32::try_from(region.reserved_used)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let seqno = self.allocate_seqno()?;
        let receipt = RegionAppendReservationV2 {
            lane_id,
            cache_epoch: self.cache_epoch,
            region_id,
            region_incarnation: region.incarnation,
            offset,
            record_bytes,
            seqno,
        };
        self.regions[region_index].reserved_used = end;
        self.lane_mutations[lane_id].tail = Some(receipt);
        Ok(receipt)
    }

    /// Publishes one fully encoded tail reservation into the resident staging
    /// span. This is not a write completion and therefore does not move
    /// `completed_used` or physical record accounting.
    pub(crate) fn stage_reservation(
        &mut self,
        receipt: RegionAppendReservationV2,
    ) -> Result<(), RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(receipt.lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail != Some(receipt) || receipt.cache_epoch != self.cache_epoch {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let end = receipt
            .end_offset()
            .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?;
        let next_span = match lane.open_span {
            Some(span)
                if span.cache_epoch == receipt.cache_epoch
                    && span.region_id == receipt.region_id
                    && span.region_incarnation == receipt.region_incarnation
                    && span.end_offset == u64::from(receipt.offset) =>
            {
                OpenWriteSpanV2 {
                    end_offset: end,
                    record_count: span
                        .record_count
                        .checked_add(1)
                        .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?,
                    max_seqno: span.max_seqno.max(receipt.seqno),
                    ..span
                }
            }
            None => OpenWriteSpanV2 {
                cache_epoch: receipt.cache_epoch,
                region_id: receipt.region_id,
                region_incarnation: receipt.region_incarnation,
                start_offset: u64::from(receipt.offset),
                end_offset: end,
                record_count: 1,
                max_seqno: receipt.seqno,
            },
            Some(_) => {
                return Err(RegionMutationErrorV2::Invariant(
                    "staged reservations are not contiguous",
                ));
            }
        };
        self.lane_mutations[receipt.lane_id].open_span = Some(next_span);
        self.lane_mutations[receipt.lane_id].tail = None;
        Ok(())
    }

    /// Reserves the bytes needed to align the current span end for direct I/O.
    /// No receipt is needed when the open span already ends on a 4 KiB
    /// boundary. A non-zero receipt remains an exclusive lane fence until
    /// [`Self::seal_write_span_with_padding`] consumes it.
    pub(crate) fn reserve_write_padding(
        &mut self,
        lane_id: usize,
    ) -> Result<Option<RegionPaddingReceiptV2>, RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(lane_id)
            .copied()
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail.is_some()
            || lane.pending_padding.is_some()
            || lane.submitted_span.is_some()
            || lane.rotation.is_some()
        {
            return Err(RegionMutationErrorV2::WouldBlock);
        }
        let open = lane.open_span.ok_or(RegionMutationErrorV2::Invariant(
            "append lane has no staged records",
        ))?;
        if open.cache_epoch != self.cache_epoch
            || open.start_offset >= open.end_offset
            || open.start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
            || open.end_offset % u64::from(RECORD_ALIGNMENT_V2) != 0
            || open.record_count == 0
            || open.max_seqno == 0
        {
            return Err(RegionMutationErrorV2::Invariant(
                "open write span identity is invalid",
            ));
        }
        let region_index = usize::try_from(open.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let region =
            self.regions
                .get(region_index)
                .copied()
                .ok_or(RegionMutationErrorV2::Invariant(
                    "open write span Region is out of bounds",
                ))?;
        if self.active_regions.get(lane_id) != Some(&open.region_id)
            || region.state != RegionMetadataStateV1::Active
            || region.incarnation != open.region_incarnation
            || region.reserved_used != open.end_offset
        {
            return Err(RegionMutationErrorV2::Invariant(
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
            .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?;
        if padding >= alignment
            || padding % u64::from(RECORD_ALIGNMENT_V2) != 0
            || padded_end_offset % alignment != 0
            || padded_end_offset > self.region_size
        {
            return Err(RegionMutationErrorV2::Invariant(
                "write span padding is invalid",
            ));
        }
        let receipt = RegionPaddingReceiptV2 {
            lane_id,
            cache_epoch: open.cache_epoch,
            region_id: open.region_id,
            region_incarnation: open.region_incarnation,
            span_start_offset: open.start_offset,
            unpadded_end_offset: open.end_offset,
            padded_end_offset,
            record_count: open.record_count,
            max_seqno: open.max_seqno,
        };
        self.regions[region_index].reserved_used = padded_end_offset;
        self.lane_mutations[lane_id].pending_padding = Some(receipt);
        Ok(Some(receipt))
    }

    /// Cancels only the current, not-yet-staged tail. Sequence numbers remain
    /// consumed, but the reservation cursor is rolled back exactly because no
    /// later reservation can exist on the same lane.
    pub(crate) fn cancel_reservation(
        &mut self,
        receipt: RegionAppendReservationV2,
    ) -> Result<(), RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(receipt.lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail != Some(receipt) || receipt.cache_epoch != self.cache_epoch {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let region_index = usize::try_from(receipt.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .ok_or(RegionMutationErrorV2::StaleReceipt)?;
        if self.active_regions.get(receipt.lane_id) != Some(&receipt.region_id)
            || region.state != RegionMetadataStateV1::Active
            || region.incarnation != receipt.region_incarnation
            || region.reserved_used
                != receipt
                    .end_offset()
                    .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        self.regions[region_index].reserved_used = u64::from(receipt.offset);
        self.lane_mutations[receipt.lane_id].tail = None;
        Ok(())
    }

    /// Seals the lane's accumulated resident records into one ordered device
    /// span. A second span may be built concurrently in resident staging, but
    /// only one submitted span per lane is admitted in this first kernel.
    pub(crate) fn seal_write_span(
        &mut self,
        lane_id: usize,
    ) -> Result<RegionWriteSpanV2, RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail.is_some()
            || lane.pending_padding.is_some()
            || lane.rotation.is_some()
            || lane.submitted_span.is_some()
        {
            return Err(RegionMutationErrorV2::WouldBlock);
        }
        let open = lane.open_span.ok_or(RegionMutationErrorV2::Invariant(
            "append lane has no staged records",
        ))?;
        self.submit_open_span(lane_id, open.end_offset)
    }

    /// Atomically consumes one exact padding receipt and seals its open span.
    /// Keeping the receipt pending until this call prevents another append from
    /// entering between staging's header rewrite and manager publication.
    pub(crate) fn seal_write_span_with_padding(
        &mut self,
        padding: RegionPaddingReceiptV2,
    ) -> Result<RegionWriteSpanV2, RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(padding.lane_id)
            .copied()
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail.is_some()
            || lane.rotation.is_some()
            || lane.submitted_span.is_some()
            || lane.pending_padding != Some(padding)
            || padding.cache_epoch != self.cache_epoch
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let open = lane.open_span.ok_or(RegionMutationErrorV2::StaleReceipt)?;
        if open.cache_epoch != padding.cache_epoch
            || open.region_id != padding.region_id
            || open.region_incarnation != padding.region_incarnation
            || open.start_offset != padding.span_start_offset
            || open.end_offset != padding.unpadded_end_offset
            || open.record_count != padding.record_count
            || open.max_seqno != padding.max_seqno
            || padding
                .padding_bytes()
                .is_none_or(|bytes| bytes == 0 || bytes as usize >= DIRECT_IO_ALIGNMENT)
            || padding.padded_end_offset % DIRECT_IO_ALIGNMENT as u64 != 0
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let region_index = usize::try_from(padding.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .ok_or(RegionMutationErrorV2::StaleReceipt)?;
        if self.active_regions.get(padding.lane_id) != Some(&padding.region_id)
            || region.state != RegionMetadataStateV1::Active
            || region.incarnation != padding.region_incarnation
            || region.reserved_used != padding.padded_end_offset
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        self.submit_open_span(padding.lane_id, padding.padded_end_offset)
    }

    fn submit_open_span(
        &mut self,
        lane_id: usize,
        end_offset: u64,
    ) -> Result<RegionWriteSpanV2, RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(lane_id)
            .copied()
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        let open = lane.open_span.ok_or(RegionMutationErrorV2::Invariant(
            "append lane has no staged records",
        ))?;
        if open.start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
            || end_offset % DIRECT_IO_ALIGNMENT as u64 != 0
            || end_offset <= open.start_offset
        {
            return Err(RegionMutationErrorV2::Invariant(
                "submitted write span is not direct-I/O aligned",
            ));
        }
        if lane.next_span_id == u64::MAX {
            return Err(RegionMutationErrorV2::SequenceExhausted);
        }
        let receipt = RegionWriteSpanV2 {
            lane_id,
            span_id: lane.next_span_id,
            cache_epoch: open.cache_epoch,
            region_id: open.region_id,
            region_incarnation: open.region_incarnation,
            start_offset: open.start_offset,
            end_offset,
            record_count: open.record_count,
            max_seqno: open.max_seqno,
        };
        let lane = &mut self.lane_mutations[lane_id];
        lane.next_span_id += 1;
        lane.open_span = None;
        lane.pending_padding = None;
        lane.submitted_span = Some(receipt);
        Ok(receipt)
    }

    /// Advances the completed prefix for one exact, ordered device completion.
    /// Duplicate, cancelled, wrong-generation, and late completions are
    /// rejected without touching the current Region incarnation.
    pub(crate) fn complete_write_span(
        &mut self,
        receipt: RegionWriteSpanV2,
    ) -> Result<(), RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(receipt.lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.submitted_span != Some(receipt) || receipt.cache_epoch != self.cache_epoch {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let region_index = usize::try_from(receipt.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let region = self
            .regions
            .get(region_index)
            .copied()
            .ok_or(RegionMutationErrorV2::StaleReceipt)?;
        if self.active_regions.get(receipt.lane_id) != Some(&receipt.region_id)
            || region.state != RegionMetadataStateV1::Active
            || region.incarnation != receipt.region_incarnation
            || region.completed_used != receipt.start_offset
            || receipt.end_offset > region.reserved_used
            || receipt.start_offset >= receipt.end_offset
            || receipt.record_count == 0
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let physical_record_count = region
            .physical_record_count
            .checked_add(receipt.record_count)
            .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?;
        let region = &mut self.regions[region_index];
        region.completed_used = receipt.end_offset;
        region.max_seqno = region.max_seqno.max(receipt.max_seqno);
        region.physical_record_count = physical_record_count;
        self.lane_mutations[receipt.lane_id].submitted_span = None;
        Ok(())
    }

    /// Starts one FIFO rotation without performing I/O. Free Regions are used
    /// first; once exhausted, the oldest sealed Region generation is reused.
    /// The outgoing Active Region is withheld from the FIFO until its two
    /// header writes complete and [`Self::finish_rotation`] is called.
    pub(crate) fn begin_rotation(
        &mut self,
        lane_id: usize,
    ) -> Result<RegionRotationReceiptV2, RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.tail.is_some()
            || lane.open_span.is_some()
            || lane.pending_padding.is_some()
            || lane.submitted_span.is_some()
            || lane.rotation.is_some()
        {
            return Err(RegionMutationErrorV2::WouldBlock);
        }
        let old_region_id = *self
            .active_regions
            .get(lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        let old_index = usize::try_from(old_region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let old = self
            .regions
            .get(old_index)
            .copied()
            .ok_or(RegionMutationErrorV2::Invariant(
                "active Region id is out of bounds",
            ))?;
        if old.state != RegionMetadataStateV1::Active || old.reserved_used != old.completed_used {
            return Err(RegionMutationErrorV2::WouldBlock);
        }

        let free = self.free_regions.front().copied();
        let (victim_region_id, reused) = match free {
            Some(region_id) => (region_id, false),
            None => (
                self.sealed_regions
                    .front()
                    .copied()
                    .ok_or(RegionMutationErrorV2::WouldBlock)?,
                true,
            ),
        };
        let victim_index = usize::try_from(victim_region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let victim =
            self.regions
                .get(victim_index)
                .copied()
                .ok_or(RegionMutationErrorV2::Invariant(
                    "rotation victim id is out of bounds",
                ))?;
        let expected_victim_state = if reused {
            RegionMetadataStateV1::Sealed
        } else {
            RegionMetadataStateV1::Free
        };
        if victim.state != expected_victim_state || victim.region_id == old.region_id {
            return Err(RegionMutationErrorV2::Invariant(
                "rotation victim queue is inconsistent",
            ));
        }
        let incarnation = victim
            .incarnation
            .checked_add(1)
            .filter(|incarnation| *incarnation != u32::MAX)
            .ok_or(RegionMutationErrorV2::IncarnationExhausted)?;
        let created_seqno = self.allocate_seqno()?;
        let removed = if reused {
            self.sealed_regions.pop_front()
        } else {
            self.free_regions.pop_front()
        };
        if removed != Some(victim_region_id) {
            return Err(RegionMutationErrorV2::Invariant(
                "rotation victim changed during selection",
            ));
        }

        self.regions[old_index].state = RegionMetadataStateV1::Sealed;
        self.regions[victim_index] = RegionRuntimeV2 {
            region_id: victim_region_id,
            incarnation,
            state: RegionMetadataStateV1::Active,
            created_seqno,
            completed_used: u64::from(REGION_HEADER_SIZE_V2),
            reserved_used: u64::from(REGION_HEADER_SIZE_V2),
            max_seqno: 0,
            physical_record_count: 0,
            logical: RegionLogicalAccountingV2::default(),
        };
        self.active_regions[lane_id] = victim_region_id;
        let receipt = RegionRotationReceiptV2 {
            lane_id,
            cache_epoch: self.cache_epoch,
            sealed: RegionHeader {
                region_id: old.region_id,
                incarnation: old.incarnation,
                state: RegionState::Sealed,
                created_seqno: old.created_seqno,
                used: old.completed_used,
            },
            activated: RegionHeader {
                region_id: victim_region_id,
                incarnation,
                state: RegionState::Active,
                created_seqno,
                used: u64::from(REGION_HEADER_SIZE_V2),
            },
            reused,
        };
        self.lane_mutations[lane_id].rotation = Some(receipt);
        Ok(receipt)
    }

    /// Publishes the outgoing Region at the tail of the sealed FIFO
    /// after the caller has completed both header writes. A repeated or late
    /// receipt cannot make a Region reachable by the current generation.
    pub(crate) fn finish_rotation(
        &mut self,
        receipt: RegionRotationReceiptV2,
    ) -> Result<(), RegionMutationErrorV2> {
        let lane = self
            .lane_mutations
            .get(receipt.lane_id)
            .ok_or(RegionMutationErrorV2::InvalidLane)?;
        if lane.rotation != Some(receipt) || receipt.cache_epoch != self.cache_epoch {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        let sealed_index = usize::try_from(receipt.sealed.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let activated_index = usize::try_from(receipt.activated.region_id)
            .map_err(|_| RegionMutationErrorV2::ArithmeticOverflow)?;
        let sealed = self
            .regions
            .get(sealed_index)
            .ok_or(RegionMutationErrorV2::StaleReceipt)?;
        let activated = self
            .regions
            .get(activated_index)
            .ok_or(RegionMutationErrorV2::StaleReceipt)?;
        if self.active_regions.get(receipt.lane_id) != Some(&receipt.activated.region_id)
            || sealed.state != RegionMetadataStateV1::Sealed
            || sealed.incarnation != receipt.sealed.incarnation
            || sealed.created_seqno != receipt.sealed.created_seqno
            || sealed.completed_used != receipt.sealed.used
            || activated.state != RegionMetadataStateV1::Active
            || activated.incarnation != receipt.activated.incarnation
            || activated.created_seqno != receipt.activated.created_seqno
        {
            return Err(RegionMutationErrorV2::StaleReceipt);
        }
        if self.sealed_regions.len() == self.sealed_regions.capacity() {
            return Err(RegionMutationErrorV2::Invariant(
                "sealed Region queue exceeded its reserved capacity",
            ));
        }
        self.sealed_regions.push_back(receipt.sealed.region_id);
        self.lane_mutations[receipt.lane_id].rotation = None;
        Ok(())
    }

    /// Tests the Region generation and global clear fence which make one
    /// completed physical index entry logically reachable. Resident-only
    /// values need a typed staging lookup and are deliberately rejected until
    /// that read path exists; a plain point lookup may address only the
    /// Region's completed prefix. Invalid fields fail closed.
    pub(crate) fn is_visible(&self, entry: IndexEntry) -> bool {
        if entry.seqno < self.clear_floor_seqno || entry.flags & INDEX_FLAG_VOLATILE != 0 {
            return false;
        }
        let Ok(region_id) = usize::try_from(entry.location.region_id()) else {
            return false;
        };
        let offset = u64::from(entry.location.offset());
        let Some(end) = offset.checked_add(u64::from(entry.location.record_len())) else {
            return false;
        };
        self.regions.get(region_id).is_some_and(|region| {
            region.state != RegionMetadataStateV1::Free
                && entry.seqno >= region.created_seqno
                && entry.seqno <= region.max_seqno
                && offset >= u64::from(REGION_HEADER_SIZE_V2)
                && offset % u64::from(RECORD_ALIGNMENT_V2) == 0
                && end <= region.completed_used
        })
    }

    /// Applies one exact index mutation to per-Region logical accounting.
    ///
    /// The caller must keep this manager authority stable for the complete
    /// index probe and invoke this method from the index commit callback. Both
    /// affected Regions are checked before either is changed, so an invariant
    /// or arithmetic failure cannot leave a partial cross-Region transfer.
    pub(crate) fn apply_index_transition(
        &mut self,
        transition: IndexTransitionV2,
    ) -> Result<(), RegionMutationErrorV2> {
        let previous = self.logical_charge(transition.previous);
        let installed = self.logical_charge(transition.installed);

        let mut updates = [None, None];
        match (previous, installed) {
            (None, None) => return Ok(()),
            (Some(previous), Some(installed))
                if previous.region_index == installed.region_index =>
            {
                updates[0] = Some((
                    previous.region_index,
                    self.checked_logical_update(
                        previous.region_index,
                        Some(previous),
                        Some(installed),
                    )?,
                ));
            }
            (Some(previous), Some(installed)) => {
                let previous_update =
                    self.checked_logical_update(previous.region_index, Some(previous), None)?;
                let installed_update =
                    self.checked_logical_update(installed.region_index, None, Some(installed))?;
                updates[0] = Some((previous.region_index, previous_update));
                updates[1] = Some((installed.region_index, installed_update));
            }
            (Some(previous), None) => {
                updates[0] = Some((
                    previous.region_index,
                    self.checked_logical_update(previous.region_index, Some(previous), None)?,
                ));
            }
            (None, Some(installed)) => {
                updates[0] = Some((
                    installed.region_index,
                    self.checked_logical_update(installed.region_index, None, Some(installed))?,
                ));
            }
        }

        for (region_index, logical) in updates.into_iter().flatten() {
            self.regions[region_index].logical = logical;
        }
        Ok(())
    }

    fn logical_charge(&self, state: IndexSlotStateV1) -> Option<RegionLogicalChargeV2> {
        let IndexSlotStateV1::Value { entry, .. } = state else {
            return None;
        };
        if entry.location.is_tombstone() || !self.is_visible(entry) {
            return None;
        }
        Some(RegionLogicalChargeV2 {
            region_index: entry.location.region_id() as usize,
            record_bytes: u64::from(entry.location.record_len()),
        })
    }

    fn checked_logical_update(
        &self,
        region_index: usize,
        previous: Option<RegionLogicalChargeV2>,
        installed: Option<RegionLogicalChargeV2>,
    ) -> Result<RegionLogicalAccountingV2, RegionMutationErrorV2> {
        let current = self
            .regions
            .get(region_index)
            .ok_or(RegionMutationErrorV2::Invariant(
                "visible index entry has an invalid Region id",
            ))?
            .logical;
        let removed_count = u64::from(previous.is_some());
        let removed_bytes = previous.map_or(0, |charge| charge.record_bytes);
        let added_count = u64::from(installed.is_some());
        let added_bytes = installed.map_or(0, |charge| charge.record_bytes);

        Ok(RegionLogicalAccountingV2 {
            live_record_count: current
                .live_record_count
                .checked_sub(removed_count)
                .ok_or(RegionMutationErrorV2::Invariant(
                    "logical record count underflow",
                ))?
                .checked_add(added_count)
                .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?,
            live_record_bytes: current
                .live_record_bytes
                .checked_sub(removed_bytes)
                .ok_or(RegionMutationErrorV2::Invariant(
                    "logical record bytes underflow",
                ))?
                .checked_add(added_bytes)
                .ok_or(RegionMutationErrorV2::ArithmeticOverflow)?,
        })
    }

    pub(crate) fn logical_accounting(
        &self,
    ) -> Result<RegionLogicalAccountingV2, RegionMetadataV1Error> {
        self.regions
            .iter()
            .try_fold(RegionLogicalAccountingV2::default(), |total, region| {
                total.checked_add_region(region)
            })
    }

    /// Freezes the complete Region metadata table against the current
    /// canonical index shard directory and physical counters supplied by the
    /// index owner.
    pub(crate) fn freeze_metadata(
        &self,
        shards: Box<[ShardMetadataRecordV1]>,
    ) -> Result<RegionMetadataV1, RegionMetadataV1Error> {
        if self.lane_mutations.iter().any(|lane| !lane.is_quiescent()) {
            return Err(RegionMetadataV1Error::InvalidField("live_region_authority"));
        }
        let shard_totals = ShardTotalsV2::from_records(&shards)?;
        let queue_ordinals = self.freeze_queue_ordinals()?;
        let mut records = try_vec(self.regions.len())?;
        let mut logical = RegionLogicalAccountingV2::default();

        for (expected_id, region) in self.regions.iter().enumerate() {
            if region.region_id as usize != expected_id
                || region.reserved_used != region.completed_used
            {
                return Err(RegionMetadataV1Error::InvalidField("live_region_authority"));
            }
            logical = logical.checked_add_region(region)?;
            records.push(RegionMetadataRecordV1 {
                region_id: region.region_id,
                incarnation: region.incarnation,
                state: region.state,
                queue_ordinal: queue_ordinals[expected_id],
                created_seqno: region.created_seqno,
                durable_used_offset: region.completed_used,
                max_seqno: region.max_seqno,
                physical_record_count: region.physical_record_count,
                live_record_count: region.logical.live_record_count,
                live_record_bytes: region.logical.live_record_bytes,
            });
        }

        let region_count = u32::try_from(self.regions.len())
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let append_lane_count = u32::try_from(self.active_regions.len())
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let free_region_count = u32::try_from(self.free_regions.len())
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let sealed_region_count = u32::try_from(self.sealed_regions.len())
            .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let max_seqno = self
            .next_seqno
            .checked_sub(1)
            .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;

        let metadata = RegionMetadataV1 {
            root: RegionMetadataRootV1 {
                cache_uuid: self.binding.cache_uuid,
                data_identity: self.binding.data_identity,
                data_superblock_generation: self.binding.data_superblock_generation,
                image_identity: self.binding.image_identity,
                image_generation: self.binding.image_generation,
                config_fingerprint: self.binding.config_fingerprint,
                index_slots: shard_totals.slot_count,
                index_page_count: shard_totals.page_count,
                region_size: self.region_size,
                region_count,
                shard_count: shard_totals.shard_count,
                append_lane_count,
                cache_epoch: self.cache_epoch,
                clear_floor_seqno: self.clear_floor_seqno,
                max_seqno,
                physical_value_slots: shard_totals.physical_value_slots,
                physical_deleted_slots: shard_totals.physical_deleted_slots,
                physical_masked_slots: shard_totals.physical_masked_slots,
                live_record_count: logical.live_record_count,
                live_record_bytes: logical.live_record_bytes,
                write_budget_window: self.write_budget.window,
                write_budget_used_bytes: self.write_budget.used_bytes,
                free_region_count,
                active_region_count: append_lane_count,
                sealed_region_count,
            },
            regions: records.into_boxed_slice(),
            shards,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    fn freeze_queue_ordinals(&self) -> Result<Vec<u32>, RegionMetadataV1Error> {
        let mut ordinals = try_unassigned_vec(self.regions.len())?;
        install_live_queue(
            &self.regions,
            RegionMetadataStateV1::Active,
            self.active_regions.iter().copied(),
            &mut ordinals,
        )?;
        install_live_queue(
            &self.regions,
            RegionMetadataStateV1::Free,
            self.free_regions.iter().copied(),
            &mut ordinals,
        )?;
        install_live_queue(
            &self.regions,
            RegionMetadataStateV1::Sealed,
            self.sealed_regions.iter().copied(),
            &mut ordinals,
        )?;
        if ordinals.contains(&UNASSIGNED_REGION) {
            return Err(RegionMetadataV1Error::InvalidField(
                "live_region_queue_permutation",
            ));
        }
        Ok(ordinals)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ShardTotalsV2 {
    shard_count: u32,
    page_count: u64,
    slot_count: u64,
    physical_value_slots: u64,
    physical_deleted_slots: u64,
    physical_masked_slots: u64,
}

impl ShardTotalsV2 {
    fn from_records(shards: &[ShardMetadataRecordV1]) -> Result<Self, RegionMetadataV1Error> {
        let mut totals = Self {
            shard_count: u32::try_from(shards.len())
                .map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?,
            ..Self::default()
        };
        for shard in shards {
            totals.page_count = totals
                .page_count
                .checked_add(shard.index_page_count)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
            totals.slot_count = totals
                .slot_count
                .checked_add(shard.slot_count)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
            totals.physical_value_slots = totals
                .physical_value_slots
                .checked_add(shard.physical_value_slots)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
            totals.physical_deleted_slots = totals
                .physical_deleted_slots
                .checked_add(shard.physical_deleted_slots)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
            totals.physical_masked_slots = totals
                .physical_masked_slots
                .checked_add(shard.physical_masked_slots)
                .ok_or(RegionMetadataV1Error::ArithmeticOverflow)?;
        }
        Ok(totals)
    }
}

fn install_recovered_queue_entry(
    state: RegionMetadataStateV1,
    ordinal: u32,
    region_id: u32,
    active: &mut [u32],
    free: &mut VecDeque<u32>,
    sealed: &mut VecDeque<u32>,
) -> Result<(), RegionMetadataV1Error> {
    let target: &mut [u32] = match state {
        RegionMetadataStateV1::Active => active,
        RegionMetadataStateV1::Free => free.make_contiguous(),
        RegionMetadataStateV1::Sealed => sealed.make_contiguous(),
    };
    let ordinal =
        usize::try_from(ordinal).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
    let slot = target
        .get_mut(ordinal)
        .ok_or(RegionMetadataV1Error::InvalidField("region_queue_ordinal"))?;
    if *slot != UNASSIGNED_REGION {
        return Err(RegionMetadataV1Error::InvalidField("region_queue_ordinal"));
    }
    *slot = region_id;
    Ok(())
}

fn install_live_queue<I>(
    regions: &[RegionRuntimeV2],
    expected_state: RegionMetadataStateV1,
    queue: I,
    ordinals: &mut [u32],
) -> Result<(), RegionMetadataV1Error>
where
    I: IntoIterator<Item = u32>,
{
    for (ordinal, region_id) in queue.into_iter().enumerate() {
        let region_index =
            usize::try_from(region_id).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        let region = regions
            .get(region_index)
            .ok_or(RegionMetadataV1Error::InvalidField("live_region_queue"))?;
        let ordinal =
            u32::try_from(ordinal).map_err(|_| RegionMetadataV1Error::ArithmeticOverflow)?;
        if region.region_id != region_id
            || region.state != expected_state
            || ordinals[region_index] != UNASSIGNED_REGION
        {
            return Err(RegionMetadataV1Error::InvalidField(
                "live_region_queue_permutation",
            ));
        }
        ordinals[region_index] = ordinal;
    }
    Ok(())
}

fn try_vec<T>(capacity: usize) -> Result<Vec<T>, RegionMetadataV1Error> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| RegionMetadataV1Error::Allocation)?;
    Ok(values)
}

fn try_unassigned_vec(count: usize) -> Result<Vec<u32>, RegionMetadataV1Error> {
    let mut values = try_vec(count)?;
    values.resize(count, UNASSIGNED_REGION);
    Ok(values)
}

fn try_unassigned_queue(
    count: usize,
    capacity: usize,
) -> Result<VecDeque<u32>, RegionMetadataV1Error> {
    let mut values = VecDeque::new();
    values
        .try_reserve_exact(capacity)
        .map_err(|_| RegionMetadataV1Error::Allocation)?;
    values.resize(count, UNASSIGNED_REGION);
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::PackedLocation;
    use crate::index_storage::canonical_index_shard_ranges;

    fn id(byte: u8) -> PersistentId {
        PersistentId::from_bytes([byte; 16]).unwrap()
    }

    fn sample() -> RegionMetadataV1 {
        let ranges = canonical_index_shard_ranges(200).unwrap();
        let mut shards = ranges
            .iter()
            .map(|range| ShardMetadataRecordV1 {
                shard_id: range.shard_id as u32,
                first_index_page: range.first_page as u64,
                index_page_count: range.page_count as u64,
                first_slot: range.first_slot as u64,
                slot_count: range.slot_count as u64,
                physical_value_slots: 0,
                physical_deleted_slots: 0,
                physical_masked_slots: 0,
            })
            .collect::<Vec<_>>();
        shards[0].physical_value_slots = 1;
        shards[0].physical_deleted_slots = 1;
        shards[1].physical_deleted_slots = 1;

        RegionMetadataV1 {
            root: RegionMetadataRootV1 {
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
                shard_count: 2,
                append_lane_count: 2,
                cache_epoch: 3,
                clear_floor_seqno: 2,
                max_seqno: 7,
                physical_value_slots: 1,
                physical_deleted_slots: 2,
                physical_masked_slots: 0,
                live_record_count: 1,
                live_record_bytes: 64,
                write_budget_window: 123,
                write_budget_used_bytes: 456,
                free_region_count: 2,
                active_region_count: 2,
                sealed_region_count: 2,
            },
            regions: vec![
                region(0, 3, RegionMetadataStateV1::Active, 1, 2, 0, 0, 0),
                region(1, 4, RegionMetadataStateV1::Free, 1, 0, 0, 0, 0),
                region(2, 2, RegionMetadataStateV1::Sealed, 1, 7, 64, 7, 0),
                region(3, 1, RegionMetadataStateV1::Active, 0, 1, 0, 0, 0),
                region(4, 8, RegionMetadataStateV1::Sealed, 0, 4, 128, 5, 1),
                region(5, 7, RegionMetadataStateV1::Free, 0, 0, 0, 0, 0),
            ]
            .into_boxed_slice(),
            shards: shards.into_boxed_slice(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn region(
        region_id: u32,
        incarnation: u32,
        state: RegionMetadataStateV1,
        queue_ordinal: u32,
        created_seqno: u64,
        used_bytes: u64,
        max_seqno: u64,
        live_records: u64,
    ) -> RegionMetadataRecordV1 {
        RegionMetadataRecordV1 {
            region_id,
            incarnation,
            state,
            queue_ordinal,
            created_seqno,
            durable_used_offset: crate::recovery_v2::RECOVERY_PAGE_SIZE as u64 + used_bytes,
            max_seqno,
            physical_record_count: used_bytes / 64,
            live_record_count: live_records,
            live_record_bytes: live_records * 64,
        }
    }

    fn sample_without_free_regions() -> RegionMetadataV1 {
        let mut metadata = sample();
        metadata.root.free_region_count = 0;
        metadata.root.sealed_region_count = 4;

        metadata.regions[4].created_seqno = 3;
        metadata.regions[4].queue_ordinal = 0;

        metadata.regions[1].state = RegionMetadataStateV1::Sealed;
        metadata.regions[1].created_seqno = 4;
        metadata.regions[1].queue_ordinal = 1;

        metadata.regions[5].state = RegionMetadataStateV1::Sealed;
        metadata.regions[5].created_seqno = 6;
        metadata.regions[5].queue_ordinal = 2;

        metadata.regions[2].queue_ordinal = 3;
        metadata.validate().unwrap();
        metadata
    }

    fn value_slot(
        hash: u64,
        region_id: u32,
        seqno: u64,
        record_bytes: u32,
        tombstone: bool,
    ) -> IndexSlotStateV1 {
        IndexSlotStateV1::Value {
            hash,
            entry: IndexEntry {
                location: PackedLocation::new(region_id, 4096, record_bytes, tombstone).unwrap(),
                seqno,
                namespace_id: 0,
                flags: 0,
            },
        }
    }

    fn index_transition(
        previous: IndexSlotStateV1,
        installed: IndexSlotStateV1,
    ) -> IndexTransitionV2 {
        IndexTransitionV2 {
            global_slot: 17,
            previous,
            installed,
        }
    }

    fn append_completed(
        manager: &mut RegionManagerV2,
        lane_id: usize,
        record_bytes: u32,
    ) -> RegionAppendReservationV2 {
        let reservation = manager.reserve_append(lane_id, record_bytes).unwrap();
        manager.stage_reservation(reservation).unwrap();
        let span = match manager.reserve_write_padding(lane_id).unwrap() {
            Some(padding) => manager.seal_write_span_with_padding(padding).unwrap(),
            None => manager.seal_write_span(lane_id).unwrap(),
        };
        manager.complete_write_span(span).unwrap();
        reservation
    }

    #[test]
    fn restores_non_id_queue_and_lane_order() {
        let manager = RegionManagerV2::from_metadata(sample()).unwrap();
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
        assert_eq!(manager.cache_epoch(), 3);
        assert_eq!(manager.clear_floor_seqno(), 2);
        assert_eq!(manager.next_seqno(), 8);
    }

    #[test]
    fn metadata_round_trip_rebuilds_all_live_accounting_and_ordinals() {
        let expected = sample();
        let shards = expected.shards.clone();
        let manager = RegionManagerV2::from_metadata(expected.clone()).unwrap();
        assert_eq!(
            manager.logical_accounting().unwrap(),
            RegionLogicalAccountingV2 {
                live_record_count: 1,
                live_record_bytes: 64,
            }
        );
        assert_eq!(
            manager.write_budget(),
            RegionWriteBudgetV2 {
                window: 123,
                used_bytes: 456,
            }
        );
        assert_eq!(manager.freeze_metadata(shards).unwrap(), expected);
    }

    #[test]
    fn visibility_applies_clear_floor_region_generation_and_bounds() {
        let manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let entry = |region_id, offset, seqno, flags| IndexEntry {
            location: PackedLocation::new(region_id, offset, 32, false).unwrap(),
            seqno,
            namespace_id: 0,
            flags,
        };
        assert!(manager.is_visible(entry(4, 4096, 4, 0)));
        assert!(!manager.is_visible(entry(4, 4096, 3, 0)));
        assert!(!manager.is_visible(entry(4, 4096, 6, 0)));
        assert!(!manager.is_visible(entry(4, 4088, 4, 0)));
        assert!(!manager.is_visible(entry(4, 4104, 4, 0)));
        assert!(!manager.is_visible(entry(4, 4224, 4, 0)));
        assert!(!manager.is_visible(entry(4, 4096, 4, INDEX_FLAG_VOLATILE)));
        assert!(!manager.is_visible(entry(1, 4096, 7, 0)));
        assert!(!manager.is_visible(entry(3, 4096, 1, 0)));
        assert!(!manager.is_visible(entry(99, 4096, 7, 0)));
    }

    #[test]
    fn index_transitions_move_and_remove_exact_live_record_charges() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let appended = append_completed(&mut manager, 0, 96);
        assert_eq!((appended.region_id, appended.seqno), (3, 8));
        let old = value_slot(7, 4, 4, 64, false);
        let new = value_slot(7, 3, 8, 96, false);

        manager
            .apply_index_transition(index_transition(old, new))
            .unwrap();
        assert_eq!(
            manager.regions[4].logical,
            RegionLogicalAccountingV2::default()
        );
        assert_eq!(
            manager.regions[3].logical,
            RegionLogicalAccountingV2 {
                live_record_count: 1,
                live_record_bytes: 96,
            }
        );

        manager
            .apply_index_transition(index_transition(
                new,
                IndexSlotStateV1::Masked { hash: 7, seqno: 9 },
            ))
            .unwrap();
        assert_eq!(
            manager.regions[3].logical,
            RegionLogicalAccountingV2::default()
        );

        manager
            .apply_index_transition(index_transition(IndexSlotStateV1::Deleted, new))
            .unwrap();
        manager
            .apply_index_transition(index_transition(new, IndexSlotStateV1::Deleted))
            .unwrap();
        assert_eq!(
            manager.regions[3].logical,
            RegionLogicalAccountingV2::default()
        );
    }

    #[test]
    fn invisible_and_tombstone_values_do_not_change_logical_accounting() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let appended = append_completed(&mut manager, 0, 32);
        assert_eq!((appended.region_id, appended.seqno), (3, 8));
        let before = manager.regions[4].logical;
        let stale = value_slot(7, 4, 3, 64, false);
        let installed = value_slot(7, 3, 8, 32, false);

        manager
            .apply_index_transition(index_transition(stale, installed))
            .unwrap();
        assert_eq!(manager.regions[4].logical, before);
        assert_eq!(
            manager.regions[3].logical,
            RegionLogicalAccountingV2 {
                live_record_count: 1,
                live_record_bytes: 32,
            }
        );

        let tombstone = value_slot(11, 2, 7, 64, true);
        let all_before = manager.regions.clone();
        manager
            .apply_index_transition(index_transition(IndexSlotStateV1::Deleted, tombstone))
            .unwrap();
        manager
            .apply_index_transition(index_transition(tombstone, IndexSlotStateV1::Deleted))
            .unwrap();
        assert_eq!(manager.regions, all_before);
    }

    #[test]
    fn failed_logical_transition_never_partially_updates_either_region() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let appended = append_completed(&mut manager, 0, 32);
        assert_eq!((appended.region_id, appended.seqno), (3, 8));
        let old = value_slot(7, 3, 8, 32, false);
        let installed = value_slot(7, 4, 4, 32, false);

        assert_eq!(
            manager.apply_index_transition(index_transition(old, IndexSlotStateV1::Deleted,)),
            Err(RegionMutationErrorV2::Invariant(
                "logical record count underflow"
            ))
        );
        assert_eq!(
            manager.regions[3].logical,
            RegionLogicalAccountingV2::default()
        );

        manager.regions[3].logical = RegionLogicalAccountingV2 {
            live_record_count: 1,
            live_record_bytes: 32,
        };
        manager.regions[4].logical = RegionLogicalAccountingV2 {
            live_record_count: u64::MAX,
            live_record_bytes: u64::MAX,
        };
        let before = manager.regions.clone();
        assert_eq!(
            manager.apply_index_transition(index_transition(old, installed)),
            Err(RegionMutationErrorV2::ArithmeticOverflow)
        );
        assert_eq!(manager.regions, before);
    }

    #[test]
    fn invalid_metadata_is_rejected_before_install() {
        let mut invalid = sample();
        invalid.regions[1].queue_ordinal = 0;
        assert_eq!(
            RegionManagerV2::from_metadata(invalid).unwrap_err(),
            RegionMetadataV1Error::InvalidField("region_queue_ordinal")
        );
    }

    #[test]
    fn shard_accounting_overflow_is_rejected_during_freeze() {
        let metadata = sample();
        let mut shards = metadata.shards.clone();
        let manager = RegionManagerV2::from_metadata(metadata).unwrap();
        shards[0].physical_value_slots = u64::MAX;
        shards[1].physical_value_slots = 1;
        assert_eq!(
            manager.freeze_metadata(shards).unwrap_err(),
            RegionMetadataV1Error::ArithmeticOverflow
        );
    }

    #[test]
    fn tail_reservation_is_exclusive_until_staged_or_cancelled() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        assert_eq!(
            manager.reserve_append(0, 0),
            Err(RegionMutationErrorV2::InvalidRecordLength)
        );
        assert_eq!(
            manager.reserve_append(0, RECORD_ALIGNMENT_V2 + 1),
            Err(RegionMutationErrorV2::InvalidRecordLength)
        );
        assert_eq!(
            manager.reserve_append(99, 64),
            Err(RegionMutationErrorV2::InvalidLane)
        );

        let reservation = manager.reserve_append(0, 64).unwrap();
        assert_eq!(
            reservation,
            RegionAppendReservationV2 {
                lane_id: 0,
                cache_epoch: 3,
                region_id: 3,
                region_incarnation: 1,
                offset: REGION_HEADER_SIZE_V2,
                record_bytes: 64,
                seqno: 8,
            }
        );
        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationErrorV2::WouldBlock)
        );
        assert_eq!(manager.regions[3].completed_used, 4096);
        assert_eq!(manager.regions[3].reserved_used, 4160);

        let mut stale = reservation;
        stale.seqno += 1;
        assert_eq!(
            manager.cancel_reservation(stale),
            Err(RegionMutationErrorV2::StaleReceipt)
        );
        manager.cancel_reservation(reservation).unwrap();
        assert_eq!(manager.regions[3].reserved_used, 4096);
        assert_eq!(
            manager.cancel_reservation(reservation),
            Err(RegionMutationErrorV2::StaleReceipt)
        );
        assert_eq!(manager.next_seqno(), 9);
    }

    #[test]
    fn many_records_share_ordered_spans_without_waiting_per_record() {
        let metadata = sample();
        let shards = metadata.shards.clone();
        let mut manager = RegionManagerV2::from_metadata(metadata).unwrap();

        let first = manager.reserve_append(0, 64).unwrap();
        manager.stage_reservation(first).unwrap();
        let second = manager.reserve_append(0, 128).unwrap();
        manager.stage_reservation(second).unwrap();
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(padding.span_start_offset, 4096);
        assert_eq!(padding.unpadded_end_offset, 4288);
        assert_eq!(padding.padded_end_offset, 8192);
        assert_eq!(padding.padding_bytes(), Some(3904));
        assert_eq!(padding.record_count, 2);
        assert_eq!(padding.max_seqno, second.seqno);
        assert_eq!(manager.regions[3].reserved_used, 8192);
        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationErrorV2::WouldBlock)
        );
        assert_eq!(
            manager.seal_write_span(0),
            Err(RegionMutationErrorV2::WouldBlock)
        );
        let mut stale_padding = padding;
        stale_padding.max_seqno -= 1;
        assert_eq!(
            manager.seal_write_span_with_padding(stale_padding),
            Err(RegionMutationErrorV2::StaleReceipt)
        );
        let submitted = manager.seal_write_span_with_padding(padding).unwrap();
        assert_eq!(submitted.start_offset, 4096);
        assert_eq!(submitted.end_offset, 8192);
        assert_eq!(submitted.record_count, 2);
        assert_eq!(submitted.max_seqno, second.seqno);
        assert_eq!(manager.regions[3].completed_used, 4096);

        // The next staging chunk is filled while the first span is in flight.
        let third = manager.reserve_append(0, 64).unwrap();
        assert_eq!(third.offset, 8192);
        manager.stage_reservation(third).unwrap();
        assert_eq!(
            manager.seal_write_span(0),
            Err(RegionMutationErrorV2::WouldBlock)
        );
        assert_eq!(
            manager.freeze_metadata(shards.clone()),
            Err(RegionMetadataV1Error::InvalidField("live_region_authority"))
        );

        manager.complete_write_span(submitted).unwrap();
        assert_eq!(manager.regions[3].completed_used, 8192);
        assert_eq!(manager.regions[3].physical_record_count, 2);
        assert_eq!(manager.regions[3].max_seqno, second.seqno);
        assert_eq!(
            manager.complete_write_span(submitted),
            Err(RegionMutationErrorV2::StaleReceipt)
        );

        let next_padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(next_padding.padding_bytes(), Some(4032));
        let next = manager.seal_write_span_with_padding(next_padding).unwrap();
        assert_eq!(next.start_offset, 8192);
        assert_eq!(next.end_offset, 12_288);
        assert_eq!(next.record_count, 1);
        manager.complete_write_span(next).unwrap();
        assert_eq!(manager.regions[3].completed_used, 12_288);
        assert_eq!(manager.regions[3].reserved_used, 12_288);
        assert_eq!(manager.regions[3].physical_record_count, 3);
        manager.freeze_metadata(shards).unwrap();
    }

    #[test]
    fn full_region_flushes_its_open_tail_before_requesting_rotation() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let active_region = manager.active_regions()[0] as usize;
        let record_bytes = u32::try_from(
            manager.region_size() - u64::from(REGION_HEADER_SIZE_V2) - RECORD_ALIGNMENT_V2 as u64,
        )
        .unwrap();
        let reservation = manager.reserve_append(0, record_bytes).unwrap();
        manager.stage_reservation(reservation).unwrap();

        assert_eq!(
            manager.reserve_append(0, 64),
            Err(RegionMutationErrorV2::FlushBeforeRotation)
        );
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        assert_eq!(padding.padding_bytes(), Some(RECORD_ALIGNMENT_V2));
        let span = manager.seal_write_span_with_padding(padding).unwrap();
        manager.complete_write_span(span).unwrap();
        assert_eq!(
            manager.regions()[active_region].completed_used,
            manager.region_size()
        );
        assert_eq!(
            manager.reserve_append(0, RECORD_ALIGNMENT_V2),
            Err(RegionMutationErrorV2::RegionFull)
        );
    }

    #[test]
    fn rotation_prefers_free_and_withholds_old_active_until_headers_finish() {
        let metadata = sample();
        let shards = metadata.shards.clone();
        let mut manager = RegionManagerV2::from_metadata(metadata).unwrap();

        let rotation = manager.begin_rotation(0).unwrap();
        assert!(!rotation.reused);
        assert_eq!(rotation.sealed.region_id, 3);
        assert_eq!(rotation.activated.region_id, 5);
        assert_eq!(rotation.activated.incarnation, 8);
        assert_eq!(rotation.activated.created_seqno, 8);
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
            Err(RegionMutationErrorV2::WouldBlock)
        );
        assert_eq!(
            manager.freeze_metadata(shards.clone()),
            Err(RegionMetadataV1Error::InvalidField("live_region_authority"))
        );

        manager.finish_rotation(rotation).unwrap();
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [4, 2, 3]
        );
        assert_eq!(
            manager.finish_rotation(rotation),
            Err(RegionMutationErrorV2::StaleReceipt)
        );
        manager.freeze_metadata(shards).unwrap();
    }

    #[test]
    fn fifo_reuse_bumps_generation_and_clears_live_accounting_in_constant_time() {
        let metadata = sample_without_free_regions();
        let shards = metadata.shards.clone();
        let mut manager = RegionManagerV2::from_metadata(metadata).unwrap();
        assert_eq!(manager.logical_accounting().unwrap().live_record_count, 1);

        let rotation = manager.begin_rotation(0).unwrap();
        assert!(rotation.reused);
        assert_eq!(rotation.activated.region_id, 4);
        assert_eq!(rotation.activated.incarnation, 9);
        assert_eq!(rotation.activated.created_seqno, 8);
        assert_eq!(manager.regions[4].physical_record_count, 0);
        assert_eq!(
            manager.regions[4].logical,
            RegionLogicalAccountingV2::default()
        );
        assert_eq!(manager.logical_accounting().unwrap().live_record_count, 0);

        manager.finish_rotation(rotation).unwrap();
        assert_eq!(
            manager.sealed_regions().iter().copied().collect::<Vec<_>>(),
            [1, 5, 2, 3]
        );
        let frozen = manager.freeze_metadata(shards).unwrap();
        assert_eq!(frozen.root.live_record_count, 0);
        assert_eq!(frozen.root.live_record_bytes, 0);
    }

    #[test]
    fn incomplete_or_late_span_cannot_cross_a_region_generation() {
        let mut manager = RegionManagerV2::from_metadata(sample()).unwrap();
        let reservation = manager.reserve_append(0, 64).unwrap();
        manager.stage_reservation(reservation).unwrap();
        let padding = manager.reserve_write_padding(0).unwrap().unwrap();
        let span = manager.seal_write_span_with_padding(padding).unwrap();
        assert_eq!(
            manager.begin_rotation(0),
            Err(RegionMutationErrorV2::WouldBlock)
        );
        manager.complete_write_span(span).unwrap();

        let rotation = manager.begin_rotation(0).unwrap();
        manager.finish_rotation(rotation).unwrap();
        let activated = manager.regions[rotation.activated.region_id as usize];
        assert_eq!(
            manager.complete_write_span(span),
            Err(RegionMutationErrorV2::StaleReceipt)
        );
        assert_eq!(
            manager.regions[rotation.activated.region_id as usize],
            activated
        );
    }

    #[test]
    fn incarnation_exhaustion_does_not_consume_fifo_or_seqno() {
        let mut metadata = sample();
        metadata.regions[5].incarnation = u32::MAX - 1;
        let mut manager = RegionManagerV2::from_metadata(metadata).unwrap();
        let next_seqno = manager.next_seqno();
        assert_eq!(
            manager.begin_rotation(0),
            Err(RegionMutationErrorV2::IncarnationExhausted)
        );
        assert_eq!(manager.next_seqno(), next_seqno);
        assert_eq!(manager.active_regions(), &[3, 0]);
        assert_eq!(
            manager.free_regions().iter().copied().collect::<Vec<_>>(),
            [5, 1]
        );
    }
}
