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

//! Property tests for persistent decoders, record encoding, and bounded indexes.

use std::collections::{BTreeMap, BTreeSet};

use quickcheck::{Gen, QuickCheck};

use crate::checksum::{Crc32c, crc32c};
use crate::format::{MAX_KEY_SIZE, RECORD_ALIGNMENT, RECORD_HEADER_SIZE, RecordHeader};
use crate::hashing::FixedPrehashedMap;
use crate::index::{IndexEntry, PackedLocation, record_size_class_upper_bound};
use crate::index_storage::{INDEX_IMAGE_SLOT_SIZE, IndexSlot, PartitionedIndexStorage};
use crate::record_codec::{
    encode_reinsert_into_hashed, encode_value_into_hashed, required_record_bytes,
};
use crate::recovery::{DataSuperblock, RECOVERY_PAGE_SIZE, RecoveryImageHeader, StateRecord};
use crate::region_index::{ReclaimIndexAction, RegionIndex};
use crate::region_manager::RegionAppendReservation;
use crate::region_metadata::RegionMetadata;

const MAX_PROPERTY_INPUT_BYTES: usize = 16 * 1024;
const MAX_PROPERTY_MAP_ENTRIES: usize = 64;
const MAX_PROPERTY_OPERATIONS: usize = 256;
const PROPERTY_CASES: u64 = 10_000;

fn exercise_persistent_decoders(input: &[u8]) {
    probe_persistent_bytes(input);

    // QuickCheck grows inputs gradually. Also probe a complete recovery page
    // so length guards do not hide decoder bodies from short generated inputs.
    let page = padded::<RECOVERY_PAGE_SIZE>(input);
    probe_persistent_bytes(&page);
}

fn probe_persistent_bytes(input: &[u8]) {
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
}

fn exercise_record_roundtrip(input: &[u8]) {
    let control = padded::<32>(input);
    let payload_len = usize::from(u16::from_le_bytes(control[0..2].try_into().unwrap()))
        .min(MAX_PROPERTY_INPUT_BYTES);
    let payload_seed = input.get(control.len()..).unwrap_or_default();
    let pattern = if payload_seed.is_empty() {
        control.as_slice()
    } else {
        payload_seed
    };
    let payload = (0..payload_len)
        .map(|index| pattern[index % pattern.len()])
        .collect::<Vec<_>>();
    let key_limit = payload.len().min(MAX_KEY_SIZE);
    let key_len =
        usize::from(u16::from_le_bytes(control[2..4].try_into().unwrap())) % (key_limit + 1);
    let (key, value) = payload.split_at(key_len);
    let record_bytes = required_record_bytes(key.len(), value.len()).unwrap();
    let hash = u64::from_le_bytes(control[4..12].try_into().unwrap());
    let region_created_seqno =
        u64::from(u16::from_le_bytes(control[12..14].try_into().unwrap())) + 1;
    let seqno =
        region_created_seqno + u64::from(u16::from_le_bytes(control[14..16].try_into().unwrap()));
    let region_id = u32::from_le_bytes(control[16..20].try_into().unwrap()) & 0x3ff;
    let offset_units = u32::from_le_bytes(control[20..24].try_into().unwrap()) & 0xffff;
    let reservation = RegionAppendReservation {
        shard_id: usize::from(control[30] & 3),
        region_id,
        region_created_seqno,
        offset: offset_units * RECORD_ALIGNMENT,
        record_bytes,
        seqno,
    };
    let logical_seqno = u64::from_le_bytes(control[24..32].try_into().unwrap()).max(1);
    let reinsert = control[31] & 1 != 0;
    let expected_seqno = if reinsert { logical_seqno } else { seqno };
    let mut destination = vec![0xa5; record_bytes as usize];

    let entry = if reinsert {
        encode_reinsert_into_hashed(
            &mut destination,
            reservation,
            hash,
            record_bytes,
            key,
            value,
            logical_seqno,
        )
    } else {
        encode_value_into_hashed(
            &mut destination,
            reservation,
            hash,
            record_bytes,
            key,
            value,
        )
    }
    .unwrap();

    assert_eq!(entry.location.region_id(), region_id);
    assert_eq!(entry.location.offset(), reservation.offset);
    assert_eq!(entry.location.record_len(), record_bytes);
    let header = RecordHeader::decode(&destination[..RECORD_HEADER_SIZE]).unwrap();
    assert_eq!(usize::from(header.key_len), key.len());
    assert_eq!(header.value_len as usize, value.len());
    assert_eq!(header.seqno, expected_seqno);
    assert_eq!(header.key_hash, hash);
    assert_eq!(header.payload_crc, crc32c(&payload));
    assert_eq!(header.region_generation, region_created_seqno);
    assert_eq!(header.record_len, record_bytes);

    let mut chunked_crc = Crc32c::new();
    let mut remaining = payload.as_slice();
    for control_byte in control {
        if remaining.is_empty() {
            break;
        }
        let chunk_len = usize::from(control_byte) % remaining.len() + 1;
        let (chunk, tail) = remaining.split_at(chunk_len);
        chunked_crc.update(chunk);
        remaining = tail;
    }
    chunked_crc.update(remaining);
    assert_eq!(chunked_crc.finish(), header.payload_crc);

    let payload_end = RECORD_HEADER_SIZE + payload.len();
    assert_eq!(&destination[RECORD_HEADER_SIZE..payload_end], payload);
    assert!(destination[payload_end..].iter().all(|byte| *byte == 0));
}

fn exercise_fixed_map_operations(input: &[u8]) {
    let operation_bytes = input.get(1..).unwrap_or_default();
    let maximum_entries = operation_bytes
        .len()
        .div_ceil(3)
        .clamp(1, MAX_PROPERTY_MAP_ENTRIES);
    let capacity = usize::from(input.first().copied().unwrap_or(0)) % maximum_entries + 1;
    let mut actual = FixedPrehashedMap::try_new(capacity).unwrap();
    let mut expected = BTreeMap::<u64, u32>::new();
    let mut seen = BTreeSet::new();

    for chunk in operation_bytes.chunks(3).take(MAX_PROPERTY_OPERATIONS) {
        let operation = padded::<3>(chunk);
        // Low-eight-bit hashes have unique 32-bit fingerprints, matching the
        // fixed directory's identity contract in the reference model.
        let hash = u64::from(operation[1]);
        let value = u32::from(operation[2]);
        seen.insert(hash);

        match operation[0] % 3 {
            0 => {
                let previous = expected.get(&hash).copied();
                if previous.is_none() && expected.len() == capacity {
                    assert_eq!(actual.get(hash), None);
                    continue;
                }
                let can_insert = actual.can_upsert(hash);
                let inserted = actual.insert(hash, value);
                assert_eq!(inserted.is_some(), can_insert);
                if let Some(actual_previous) = inserted {
                    assert_eq!(actual_previous, previous);
                    expected.insert(hash, value);
                } else {
                    assert!(previous.is_none());
                }
            }
            1 => assert_eq!(actual.get(hash), expected.get(&hash).copied()),
            _ => {
                let expected_removed = expected.remove(&hash);
                assert_eq!(actual.remove(hash), expected_removed);
            }
        }
        assert_eq!(actual.get(hash), expected.get(&hash).copied());
    }

    for hash in seen {
        assert_eq!(actual.get(hash), expected.get(&hash).copied());
    }
}

fn exercise_region_index_operations(input: &[u8]) {
    let slot_count = 8_usize << usize::from(input.first().copied().unwrap_or(0) & 3);
    let storage = PartitionedIndexStorage::anonymous(slot_count).unwrap();
    let index = RegionIndex::from_storage(storage).unwrap();
    index.set_statistics_enabled(true);

    for chunk in input
        .get(1..)
        .unwrap_or_default()
        .chunks(4)
        .take(MAX_PROPERTY_OPERATIONS)
    {
        let operation = padded::<4>(chunk);
        let hash = splitmix64(u64::from(operation[1]));
        let location = property_location(splitmix64(u64::from(operation[2])));
        let replacement = IndexEntry {
            location: property_location(splitmix64(u64::from(operation[3]) ^ u64::MAX)),
        };
        let before = index.lookup_raw(hash).unwrap();

        match operation[0] % 7 {
            0 => {
                assert!(index.upsert(hash, IndexEntry { location }).unwrap());
                assert_eq!(
                    index.lookup_raw(hash).unwrap(),
                    Some(indexed_entry(location))
                );
            }
            1 => {}
            2 => {
                assert_eq!(index.try_delete(hash).unwrap(), before.is_some());
                assert_eq!(index.lookup_raw(hash).unwrap(), None);
            }
            3 => {
                let expected_match =
                    before.is_some_and(|entry| entry.location.index_equivalent(location));
                assert_eq!(
                    index.remove_if_match(hash, location).unwrap(),
                    expected_match
                );
                assert_eq!(
                    index.lookup_raw(hash).unwrap(),
                    if expected_match { None } else { before }
                );
            }
            4 => {
                let expected_match =
                    before.is_some_and(|entry| entry.location.index_equivalent(location));
                assert_eq!(
                    index.replace_if_match(hash, location, replacement).unwrap(),
                    expected_match
                );
                assert_eq!(
                    index.lookup_raw(hash).unwrap(),
                    if expected_match {
                        Some(indexed_entry(replacement.location))
                    } else {
                        before
                    }
                );
            }
            5 => {
                let expected_match =
                    before.is_some_and(|entry| entry.location.index_equivalent(location));
                match index.prepare_reclaim(hash, location).unwrap() {
                    ReclaimIndexAction::Missing => {
                        assert!(!expected_match);
                        assert_eq!(index.lookup_raw(hash).unwrap(), before);
                    }
                    ReclaimIndexAction::Removed => {
                        assert!(expected_match);
                        assert_eq!(index.lookup_raw(hash).unwrap(), None);
                    }
                    ReclaimIndexAction::Reinsert => {
                        assert!(expected_match);
                        assert_eq!(index.lookup_raw(hash).unwrap(), before);
                    }
                }
            }
            _ => {
                assert!(index.upsert(hash, IndexEntry { location }).unwrap());
                assert_eq!(
                    index.prepare_reclaim(hash, location).unwrap(),
                    ReclaimIndexAction::Removed
                );
                assert_eq!(index.lookup_raw(hash).unwrap(), None);
            }
        }

        let snapshot = index.snapshot().unwrap();
        assert_eq!(
            snapshot.physical_value_slots + snapshot.empty_slots,
            snapshot.slot_capacity
        );
    }
}

fn property_location(raw: u64) -> PackedLocation {
    let region_id = raw as u32 & 0x3ff;
    let offset = ((raw >> 10) as u32 & 0xffff) * RECORD_ALIGNMENT;
    let record_len = (((raw >> 26) as u32 & 0x3ff) + 1) * RECORD_ALIGNMENT;
    PackedLocation::new(region_id, offset, record_len)
        .expect("bounded aligned property-test location is representable")
}

fn indexed_entry(location: PackedLocation) -> IndexEntry {
    let record_len = record_size_class_upper_bound(location.index_size_class()).unwrap();
    IndexEntry {
        location: PackedLocation::new(location.region_id(), location.offset(), record_len)
            .expect("a valid property-test location has a representable size-class upper bound"),
    }
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn padded<const N: usize>(input: &[u8]) -> [u8; N] {
    let mut output = [0; N];
    let len = input.len().min(N);
    output[..len].copy_from_slice(&input[..len]);
    output
}

fn check_arbitrary_bytes(property: fn(Vec<u8>) -> bool) {
    QuickCheck::new()
        .tests(PROPERTY_CASES)
        .max_tests(PROPERTY_CASES)
        .min_tests_passed(PROPERTY_CASES)
        .rng(Gen::new(MAX_PROPERTY_INPUT_BYTES + 1))
        .quickcheck(property);
}

#[test]
fn arbitrary_persistent_bytes_never_escape_bounds() {
    fn property(input: Vec<u8>) -> bool {
        exercise_persistent_decoders(&input);
        true
    }

    exercise_persistent_decoders(&vec![u8::MAX; MAX_PROPERTY_INPUT_BYTES]);
    check_arbitrary_bytes(property);
}

#[test]
fn arbitrary_record_roundtrips_preserve_encoding_contract() {
    fn property(input: Vec<u8>) -> bool {
        exercise_record_roundtrip(&input);
        true
    }

    exercise_record_roundtrip(&vec![u8::MAX; MAX_PROPERTY_INPUT_BYTES]);
    check_arbitrary_bytes(property);
}

#[test]
fn arbitrary_fixed_map_operations_match_the_reference_model() {
    fn property(input: Vec<u8>) -> bool {
        exercise_fixed_map_operations(&input);
        true
    }

    exercise_fixed_map_operations(&vec![u8::MAX; MAX_PROPERTY_INPUT_BYTES]);
    check_arbitrary_bytes(property);
}

#[test]
fn arbitrary_region_index_operations_preserve_invariants() {
    fn property(input: Vec<u8>) -> bool {
        exercise_region_index_operations(&input);
        true
    }

    exercise_region_index_operations(&vec![u8::MAX; MAX_PROPERTY_INPUT_BYTES]);
    check_arbitrary_bytes(property);
}
