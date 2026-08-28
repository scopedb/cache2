// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

//! Shared packed-location and index-entry primitives for the index.

use std::fmt;

const REGION_BITS: u32 = 20;
const OFFSET_BITS: u32 = 20;
const RECORD_LEN_BITS: u32 = 20;

const REGION_SHIFT: u32 = 0;
const OFFSET_SHIFT: u32 = REGION_SHIFT + REGION_BITS;
const RECORD_LEN_SHIFT: u32 = OFFSET_SHIFT + OFFSET_BITS;

const REGION_MASK: u64 = (1_u64 << REGION_BITS) - 1;
const OFFSET_MASK: u64 = (1_u64 << OFFSET_BITS) - 1;
const RECORD_LEN_MASK: u64 = (1_u64 << RECORD_LEN_BITS) - 1;

const OFFSET_ALIGNMENT: u32 = 32;
const RECORD_LEN_ALIGNMENT: u32 = 32;

const EXACT_SIZE_CLASS_UNITS: u32 = 32;
const TRANSITION_SIZE_CLASS_UNITS: u32 = 64;
const TRANSITION_SIZE_CLASS_STEPS: u32 = 13;
const LOG_SIZE_CLASS_FIRST_CODE: u32 = 46;
const LOG_SIZE_CLASS_FIRST_EXPONENT: u32 = 6;
const LOG_SIZE_CLASS_STEPS: u32 = 15;
const RECORD_SIZE_CLASS_UPPER_BOUNDS: [u32; 256] = build_record_size_class_upper_bounds();

pub(crate) const MAX_REGION_ID: u32 = REGION_MASK as u32;
pub(crate) const MAX_REGION_OFFSET: u32 = (OFFSET_MASK as u32) * OFFSET_ALIGNMENT;
pub(crate) const MAX_RECORD_LEN: u32 = (RECORD_LEN_MASK as u32 + 1) * RECORD_LEN_ALIGNMENT;
pub(crate) const MAX_PACKED_REGION_COUNT: u32 = MAX_REGION_ID + 1;
pub(crate) const MAX_PACKED_REGION_SIZE: u64 = MAX_REGION_OFFSET as u64 + OFFSET_ALIGNMENT as u64;
/// 512M buckets covers a 4 TiB cache containing 16 KiB records at the
/// recommended 2x capacity.
pub(crate) const MAX_INDEX_SLOTS: usize = 512 * 1024 * 1024;
pub(crate) const INDEX_CANDIDATES: usize = 4;
#[cfg(feature = "benchmarking")]
pub(crate) const MAX_INDEX_PROBES: usize = INDEX_CANDIDATES;
pub(crate) const MAX_INDEX_PARTITIONS: usize = 4096;

/// A record location packed into one machine word.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct PackedLocation(u64);

impl PackedLocation {
    pub(crate) fn new(
        region_id: u32,
        offset: u32,
        record_len: u32,
    ) -> Result<Self, PackedLocationError> {
        if region_id > MAX_REGION_ID {
            return Err(PackedLocationError::RegionOutOfRange);
        }
        if !offset.is_multiple_of(OFFSET_ALIGNMENT) {
            return Err(PackedLocationError::OffsetUnaligned);
        }
        if offset > MAX_REGION_OFFSET {
            return Err(PackedLocationError::OffsetOutOfRange);
        }
        if record_len == 0 {
            return Err(PackedLocationError::RecordLengthZero);
        }
        if !record_len.is_multiple_of(RECORD_LEN_ALIGNMENT) {
            return Err(PackedLocationError::RecordLengthUnaligned);
        }
        if record_len > MAX_RECORD_LEN {
            return Err(PackedLocationError::RecordLengthOutOfRange);
        }

        let offset_units = u64::from(offset / OFFSET_ALIGNMENT);
        let record_len_units = u64::from(record_len / RECORD_LEN_ALIGNMENT - 1);
        Ok(Self(
            u64::from(region_id)
                | (offset_units << OFFSET_SHIFT)
                | (record_len_units << RECORD_LEN_SHIFT),
        ))
    }

    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) const fn raw(self) -> u64 {
        self.0
    }

    pub(crate) const fn region_id(self) -> u32 {
        ((self.0 >> REGION_SHIFT) & REGION_MASK) as u32
    }

    pub(crate) const fn offset(self) -> u32 {
        (((self.0 >> OFFSET_SHIFT) & OFFSET_MASK) as u32) * OFFSET_ALIGNMENT
    }

    pub(crate) const fn record_len(self) -> u32 {
        ((((self.0 >> RECORD_LEN_SHIFT) & RECORD_LEN_MASK) as u32) + 1) * RECORD_LEN_ALIGNMENT
    }

    pub(crate) fn index_size_class(self) -> u8 {
        record_size_class(self.record_len())
            .expect("a valid packed location always has an index size class")
    }

    pub(crate) fn index_equivalent(self, other: Self) -> bool {
        self.region_id() == other.region_id()
            && self.offset() == other.offset()
            && self.index_size_class() == other.index_size_class()
    }
}

/// Encodes one exact 32-byte unit count into the smallest representable upper
/// bound. Sizes through 1 KiB remain exact; larger classes add less than 7%
/// over-read while covering the complete 32 MiB Region limit with one byte.
pub(crate) fn record_size_class(record_len: u32) -> Option<u8> {
    if record_len == 0
        || record_len > MAX_RECORD_LEN
        || !record_len.is_multiple_of(RECORD_LEN_ALIGNMENT)
    {
        return None;
    }
    let units = record_len / RECORD_LEN_ALIGNMENT;
    let code = if units <= EXACT_SIZE_CLASS_UNITS {
        units
    } else if units <= TRANSITION_SIZE_CLASS_UNITS {
        let step = ((units - EXACT_SIZE_CLASS_UNITS - 1) * TRANSITION_SIZE_CLASS_STEPS
            / EXACT_SIZE_CLASS_UNITS)
            + 1;
        EXACT_SIZE_CLASS_UNITS + step
    } else {
        let exponent = u32::BITS - 1 - (units - 1).leading_zeros();
        let base = 1_u32 << exponent;
        let step = ((units - base - 1) * LOG_SIZE_CLASS_STEPS / base) + 1;
        LOG_SIZE_CLASS_FIRST_CODE
            + (exponent - LOG_SIZE_CLASS_FIRST_EXPONENT) * LOG_SIZE_CLASS_STEPS
            + step
            - 1
    };
    u8::try_from(code).ok()
}

pub(crate) fn record_size_class_upper_bound(class: u8) -> Option<u32> {
    let upper = RECORD_SIZE_CLASS_UPPER_BOUNDS[usize::from(class)];
    (upper != 0).then_some(upper)
}

const fn build_record_size_class_upper_bounds() -> [u32; 256] {
    let mut bounds = [0_u32; 256];
    let mut class = 1_u32;
    while class <= u8::MAX as u32 {
        let units = if class <= EXACT_SIZE_CLASS_UNITS {
            class
        } else if class < LOG_SIZE_CLASS_FIRST_CODE {
            let step = class - EXACT_SIZE_CLASS_UNITS;
            EXACT_SIZE_CLASS_UNITS
                + (step * EXACT_SIZE_CLASS_UNITS).div_ceil(TRANSITION_SIZE_CLASS_STEPS)
        } else {
            let ordinal = class - LOG_SIZE_CLASS_FIRST_CODE;
            let exponent = LOG_SIZE_CLASS_FIRST_EXPONENT + ordinal / LOG_SIZE_CLASS_STEPS;
            let step = ordinal % LOG_SIZE_CLASS_STEPS + 1;
            let base = 1_u32 << exponent;
            base + (step * base).div_ceil(LOG_SIZE_CLASS_STEPS)
        };
        bounds[class as usize] = units * RECORD_LEN_ALIGNMENT;
        class += 1;
    }
    bounds
}

impl fmt::Debug for PackedLocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PackedLocation")
            .field("region_id", &self.region_id())
            .field("offset", &self.offset())
            .field("record_len", &self.record_len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PackedLocationError {
    RegionOutOfRange,
    OffsetUnaligned,
    OffsetOutOfRange,
    RecordLengthZero,
    RecordLengthUnaligned,
    RecordLengthOutOfRange,
}

impl fmt::Display for PackedLocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RegionOutOfRange => "region id does not fit in 20 bits",
            Self::OffsetUnaligned => "region offset is not 32-byte aligned",
            Self::OffsetOutOfRange => "region offset does not fit in 20 units",
            Self::RecordLengthZero => "record length must be non-zero",
            Self::RecordLengthUnaligned => "record length is not 32-byte aligned",
            Self::RecordLengthOutOfRange => "record length does not fit in 20 units",
        })
    }
}

impl std::error::Error for PackedLocationError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) location: PackedLocation,
}

/// Stable power-of-two partition routing for persisted index layouts.
pub(crate) fn index_partition_for(hash: u64, partition_count: usize) -> usize {
    debug_assert!(partition_count.is_power_of_two());
    if partition_count == 1 {
        return 0;
    }
    let partition_bits = partition_count.trailing_zeros();
    (mix_for_partition(hash) >> (u64::BITS - partition_bits)) as usize
}

fn mix_for_partition(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51_afd7_ed55_8ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    hash ^ (hash >> 33)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_location_round_trips_boundaries() {
        for (region, offset, len) in [
            (0, 0, 32),
            (MAX_REGION_ID, MAX_REGION_OFFSET, MAX_RECORD_LEN),
        ] {
            let location = PackedLocation::new(region, offset, len).unwrap();
            assert_eq!(PackedLocation::from_raw(location.raw()), location);
            assert_eq!(location.region_id(), region);
            assert_eq!(location.offset(), offset);
            assert_eq!(location.record_len(), len);
        }
    }

    #[test]
    fn index_size_classes_are_monotonic_upper_bounds() {
        let mut previous_class = 0;
        let mut previous_upper = 0;
        for units in 1..=RECORD_LEN_MASK as u32 + 1 {
            let record_len = units * RECORD_LEN_ALIGNMENT;
            let class = record_size_class(record_len).unwrap();
            let upper = record_size_class_upper_bound(class).unwrap();
            assert!(class >= previous_class);
            assert!(upper >= record_len);
            assert!(upper >= previous_upper);
            assert_eq!(record_size_class(upper), Some(class));
            if record_len <= 1024 {
                assert_eq!(upper, record_len);
            } else {
                assert!(
                    u64::from(upper) * 100 < u64::from(record_len) * 107,
                    "record_len={record_len}, upper={upper}, class={class}"
                );
            }
            previous_class = class;
            previous_upper = upper;
        }
        assert_eq!(previous_class, u8::MAX);
        assert_eq!(previous_upper, MAX_RECORD_LEN);
        assert_eq!(record_size_class(0), None);
        assert_eq!(record_size_class_upper_bound(0), None);
    }

    #[test]
    fn partition_route_stays_in_range() {
        for count in [1, 2, 8, MAX_INDEX_PARTITIONS] {
            for hash in [0, 1, u64::MAX, 0x1234_5678_90ab_cdef] {
                assert!(index_partition_for(hash, count) < count);
            }
        }
    }
}
