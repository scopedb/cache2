//! Shared packed-location and index-entry primitives for the index.

use std::fmt;

const REGION_BITS: u32 = 21;
const OFFSET_BITS: u32 = 22;
const RECORD_LEN_BITS: u32 = 20;

const REGION_SHIFT: u32 = 0;
const OFFSET_SHIFT: u32 = REGION_SHIFT + REGION_BITS;
const RECORD_LEN_SHIFT: u32 = OFFSET_SHIFT + OFFSET_BITS;
const RESERVED_SHIFT: u32 = RECORD_LEN_SHIFT + RECORD_LEN_BITS;

const REGION_MASK: u64 = (1_u64 << REGION_BITS) - 1;
const OFFSET_MASK: u64 = (1_u64 << OFFSET_BITS) - 1;
const RECORD_LEN_MASK: u64 = (1_u64 << RECORD_LEN_BITS) - 1;

const OFFSET_ALIGNMENT: u32 = 8;
const RECORD_LEN_ALIGNMENT: u32 = 32;

pub(crate) const MAX_REGION_ID: u32 = REGION_MASK as u32;
pub(crate) const MAX_REGION_OFFSET: u32 = (OFFSET_MASK as u32) * OFFSET_ALIGNMENT;
pub(crate) const MAX_RECORD_LEN: u32 = (RECORD_LEN_MASK as u32) * RECORD_LEN_ALIGNMENT;
pub(crate) const MAX_PACKED_REGION_COUNT: u32 = MAX_REGION_ID + 1;
pub(crate) const MAX_PACKED_REGION_SIZE: u64 = MAX_REGION_OFFSET as u64 + OFFSET_ALIGNMENT as u64;
/// 512M slots is a 12 GiB index at the stable 24-byte slot size and covers a
/// 1 TiB cache containing 4 KiB records at the recommended 1.25x load factor.
pub(crate) const MAX_INDEX_SLOTS: usize = 512 * 1024 * 1024;
pub(crate) const MAX_INDEX_PROBES: usize = 64;
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
        let record_len_units = u64::from(record_len / RECORD_LEN_ALIGNMENT);
        Ok(Self(
            u64::from(region_id)
                | (offset_units << OFFSET_SHIFT)
                | (record_len_units << RECORD_LEN_SHIFT),
        ))
    }

    pub(crate) const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub(crate) fn try_from_raw(raw: u64) -> Result<Self, PackedLocationError> {
        if raw & (1_u64 << RESERVED_SHIFT) != 0 {
            return Err(PackedLocationError::ReservedBitSet);
        }
        let location = Self::from_raw(raw);
        Self::new(
            location.region_id(),
            location.offset(),
            location.record_len(),
        )
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
        (((self.0 >> RECORD_LEN_SHIFT) & RECORD_LEN_MASK) as u32) * RECORD_LEN_ALIGNMENT
    }
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
    ReservedBitSet,
}

impl fmt::Display for PackedLocationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::RegionOutOfRange => "region id does not fit in 21 bits",
            Self::OffsetUnaligned => "region offset is not 8-byte aligned",
            Self::OffsetOutOfRange => "region offset does not fit in 22 units",
            Self::RecordLengthZero => "record length must be non-zero",
            Self::RecordLengthUnaligned => "record length is not 32-byte aligned",
            Self::RecordLengthOutOfRange => "record length does not fit in 20 units",
            Self::ReservedBitSet => "packed location reserved bit is set",
        })
    }
}

impl std::error::Error for PackedLocationError {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub(crate) location: PackedLocation,
    pub(crate) seqno: u64,
}

impl IndexEntry {
    pub(crate) const fn same_record_identity(self, other: Self) -> bool {
        self.location.raw() == other.location.raw() && self.seqno == other.seqno
    }
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
            assert_eq!(PackedLocation::try_from_raw(location.raw()), Ok(location));
            assert_eq!(location.region_id(), region);
            assert_eq!(location.offset(), offset);
            assert_eq!(location.record_len(), len);
        }
        assert_eq!(
            PackedLocation::try_from_raw(0),
            Err(PackedLocationError::RecordLengthZero)
        );
        assert_eq!(
            PackedLocation::try_from_raw(1_u64 << RESERVED_SHIFT),
            Err(PackedLocationError::ReservedBitSet)
        );
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
