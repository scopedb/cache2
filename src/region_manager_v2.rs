//! In-memory Region authority reconstructed from one validated clean V2 image.
//!
//! The manager deliberately does not retain the recovered index shard
//! directory or its physical counters. A clean freeze accepts the current
//! canonical shard records from the index owner and derives every Region,
//! queue, and root accounting field from live manager state.

use crate::index::IndexEntry;
use crate::recovery_v2::{CacheEpochV2, PersistentId};
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RegionWriteBudgetV2 {
    /// Zero means that no persisted write-budget window is active.
    pub(crate) window: u64,
    pub(crate) used_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RegionRuntimeV2 {
    pub(crate) region_id: u32,
    pub(crate) incarnation: u32,
    pub(crate) state: RegionMetadataStateV1,
    pub(crate) created_seqno: u64,
    /// Last byte known to be durable in the Region data file.
    pub(crate) durable_used: u64,
    /// Reservation cursor. A clean recovery has no outstanding writes, so it
    /// starts exactly at `durable_used`.
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

        let mut regions = try_vec(region_count)?;
        let mut active_regions = try_unassigned_vec(active_count)?;
        let mut free_regions = try_unassigned_queue(free_count)?;
        let mut sealed_regions = try_unassigned_queue(sealed_count)?;

        for encoded in encoded_regions.iter().copied() {
            let runtime = RegionRuntimeV2 {
                region_id: encoded.region_id,
                incarnation: encoded.incarnation,
                state: encoded.state,
                created_seqno: encoded.created_seqno,
                durable_used: encoded.durable_used_offset,
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

    /// Tests the Region generation and global clear fence which make one
    /// physical index entry logically reachable. Invalid Region ids fail
    /// closed.
    pub(crate) fn is_visible(&self, entry: IndexEntry) -> bool {
        if entry.seqno < self.clear_floor_seqno {
            return false;
        }
        let Ok(region_id) = usize::try_from(entry.location.region_id()) else {
            return false;
        };
        self.regions.get(region_id).is_some_and(|region| {
            region.state != RegionMetadataStateV1::Free && entry.seqno >= region.created_seqno
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
        let shard_totals = ShardTotalsV2::from_records(&shards)?;
        let queue_ordinals = self.freeze_queue_ordinals()?;
        let mut records = try_vec(self.regions.len())?;
        let mut logical = RegionLogicalAccountingV2::default();

        for (expected_id, region) in self.regions.iter().enumerate() {
            if region.region_id as usize != expected_id
                || region.reserved_used != region.durable_used
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
                durable_used_offset: region.durable_used,
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

fn try_unassigned_queue(count: usize) -> Result<VecDeque<u32>, RegionMetadataV1Error> {
    let mut values = VecDeque::new();
    values
        .try_reserve_exact(count)
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
                .all(|region| region.reserved_used == region.durable_used)
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
        let entry = |region_id, seqno| IndexEntry {
            location: PackedLocation::new(region_id, 4096, 32, false).unwrap(),
            seqno,
            namespace_id: 0,
            flags: 0,
        };
        assert!(manager.is_visible(entry(4, 4)));
        assert!(!manager.is_visible(entry(4, 3)));
        assert!(!manager.is_visible(entry(1, 7)));
        assert!(!manager.is_visible(entry(3, 1)));
        assert!(!manager.is_visible(entry(99, 7)));
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
}
