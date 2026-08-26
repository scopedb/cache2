//! Internal libFuzzer entry points. This module is available only with the
//! `fuzzing` feature and is not part of the supported cache API.

use crate::format::RecordHeader;
use crate::index::{IndexEntry, PackedLocation};
use crate::index_storage::{IndexSlot, PartitionedIndexStorage};
use crate::recovery::{DataSuperblock, RecoveryImageHeader, StateRecord};
use crate::region_index::{IndexMutationAuthority, IndexTransition, RegionIndex};
use crate::region_metadata::RegionMetadata;

/// Exercises all persistent byte decoders and a bounded canonical index probe.
/// No result is trusted: the invariant is that arbitrary bytes never panic,
/// access memory out of bounds, or create an unbounded probe/allocation path.
pub fn persistent_decoders_and_index_probe(input: &[u8]) {
    let _ = DataSuperblock::probe(input);
    let _ = RecoveryImageHeader::probe(input);
    let _ = StateRecord::decode(input);
    let _ = RecordHeader::decode(input);
    let _ = RegionMetadata::decode(input);

    if let Some(encoded) = input
        .get(..32)
        .and_then(|bytes| <&[u8; 32]>::try_from(bytes).ok())
    {
        let _ = IndexSlot::decode(encoded).runtime_state();
    }

    fuzz_index_probe(input);
}

fn fuzz_index_probe(input: &[u8]) {
    let slot_count = 8_usize << input.first().copied().unwrap_or(0).min(4);
    let Ok(storage) = PartitionedIndexStorage::anonymous(slot_count) else {
        return;
    };
    let index = RegionIndex::from_storage(storage);
    let mut authority = VisibleAuthority;

    for chunk in input.get(1..).unwrap_or_default().chunks(24).take(128) {
        let mut bytes = [0_u8; 24];
        bytes[..chunk.len()].copy_from_slice(chunk);
        let hash = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let seqno = u64::from_le_bytes(bytes[8..16].try_into().unwrap()).max(1);
        let raw = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let region_id = (raw as u32) & 0x3ff;
        let offset = ((raw >> 32) as u32 & 0xffff) * 8;
        let location = PackedLocation::new(region_id, offset, 32)
            .expect("bounded aligned fuzz location is representable");
        let entry = IndexEntry {
            location,
            seqno,
            namespace_id: (raw >> 16) as u32,
        };
        let _ = index.upsert_with_authority(hash, entry, &mut authority);
        let _ = index.lookup_raw(hash);
        let _ = index.revalidate_exact(hash, entry);
    }
}

struct VisibleAuthority;

impl IndexMutationAuthority for VisibleAuthority {
    fn is_visible(&self, _entry: IndexEntry) -> bool {
        true
    }

    fn commit(&mut self, _transition: IndexTransition) {}
}
