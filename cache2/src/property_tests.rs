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

//! Property tests for persistent decoders and bounded index operations.

use quickcheck::{Gen, QuickCheck};

use crate::format::RecordHeader;
use crate::index::{IndexEntry, PackedLocation};
use crate::index_storage::{INDEX_IMAGE_SLOT_SIZE, IndexSlot, PartitionedIndexStorage};
use crate::recovery::{DataSuperblock, RecoveryImageHeader, StateRecord};
use crate::region_index::RegionIndex;
use crate::region_metadata::RegionMetadata;

const MAX_PROPERTY_INPUT_BYTES: usize = 16 * 1024;
const PROPERTY_CASES: u64 = 10_000;

/// Exercises all persistent byte decoders and a bounded canonical index probe.
/// No result is trusted: the invariant is that arbitrary bytes never panic,
/// access memory out of bounds, or create an unbounded probe/allocation path.
fn exercise_persistent_decoders_and_index_probe(input: &[u8]) {
    let _ = DataSuperblock::probe(input);
    let _ = RecoveryImageHeader::probe(input);
    let _ = StateRecord::decode(input);
    let _ = RecordHeader::decode(input);
    let _ = RegionMetadata::decode(input);

    if let Some(encoded) = input
        .get(..INDEX_IMAGE_SLOT_SIZE)
        .and_then(|bytes| <&[u8; INDEX_IMAGE_SLOT_SIZE]>::try_from(bytes).ok())
    {
        let _ = IndexSlot::decode(encoded).runtime_state();
    }

    exercise_index_probe(input);
}

fn exercise_index_probe(input: &[u8]) {
    let slot_count = 8_usize << input.first().copied().unwrap_or(0).min(4);
    let Ok(storage) = PartitionedIndexStorage::anonymous(slot_count) else {
        return;
    };
    let Ok(index) = RegionIndex::from_storage(storage) else {
        return;
    };
    for chunk in input.get(1..).unwrap_or_default().chunks(24).take(128) {
        let mut bytes = [0_u8; 24];
        bytes[..chunk.len()].copy_from_slice(chunk);
        let hash = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let raw = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let region_id = (raw as u32) & 0x3ff;
        let offset = ((raw >> 32) as u32 & 0xffff) * crate::format::RECORD_ALIGNMENT;
        let location = PackedLocation::new(region_id, offset, 32)
            .expect("bounded aligned property-test location is representable");
        let entry = IndexEntry { location };
        let _ = index.upsert(hash, entry);
        let _ = index.lookup_raw(hash);
    }
}

#[test]
fn arbitrary_persistent_bytes_never_escape_bounds() {
    fn property(input: Vec<u8>) -> bool {
        exercise_persistent_decoders_and_index_probe(&input);
        true
    }

    // QuickCheck generates lengths below the configured size, so cover the
    // exact upper bound explicitly as well.
    exercise_persistent_decoders_and_index_probe(&vec![u8::MAX; MAX_PROPERTY_INPUT_BYTES]);
    QuickCheck::new()
        .tests(PROPERTY_CASES)
        .max_tests(PROPERTY_CASES)
        .min_tests_passed(PROPERTY_CASES)
        .rng(Gen::new(MAX_PROPERTY_INPUT_BYTES + 1))
        .quickcheck(property as fn(Vec<u8>) -> bool);
}
