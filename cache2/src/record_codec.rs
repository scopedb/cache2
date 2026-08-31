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

//! Checked value-record encoding for the RegionStore append path.
//!
//! The encoder uses the stable record envelope while keeping batch padding and
//! physical location authority in the append reservation. The encoder performs
//! no allocation and computes the payload CRC incrementally from the key and
//! value.

use std::fmt;

use twox_hash::XxHash3_64;

use crate::checksum::Crc32c;
use crate::format::{MAX_KEY_SIZE, RECORD_ALIGNMENT, RECORD_HEADER_SIZE, RecordHeader};
use crate::index::{IndexEntry, MAX_RECORD_LEN, PackedLocation, PackedLocationError};
use crate::region_manager::RegionAppendReservation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordEncodeError {
    LengthOverflow,
    KeyTooLarge,
    RecordTooLarge,
    DestinationLengthMismatch { reserved: u32, destination: usize },
    ReservationTooSmall { required: u32, reserved: u32 },
    InvalidReservation,
    InvalidLocation(PackedLocationError),
}

impl fmt::Display for RecordEncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LengthOverflow => formatter.write_str("record length overflow"),
            Self::KeyTooLarge => formatter.write_str("record key exceeds the format limit"),
            Self::RecordTooLarge => {
                formatter.write_str("record length exceeds the packed-location limit")
            }
            Self::DestinationLengthMismatch {
                reserved,
                destination,
            } => write!(
                formatter,
                "record destination length {destination} does not match reservation {reserved}"
            ),
            Self::ReservationTooSmall { required, reserved } => write!(
                formatter,
                "record reservation {reserved} is smaller than required length {required}"
            ),
            Self::InvalidReservation => {
                formatter.write_str("record reservation identity is invalid")
            }
            Self::InvalidLocation(error) => {
                write!(formatter, "invalid record location: {error}")
            }
        }
    }
}

impl std::error::Error for RecordEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidLocation(error) => Some(error),
            _ => None,
        }
    }
}

/// Returns the minimum 32-byte-aligned envelope for one logical value.
pub(crate) fn required_record_bytes(
    key_len: usize,
    value_len: usize,
) -> Result<u32, RecordEncodeError> {
    if key_len > MAX_KEY_SIZE {
        return Err(RecordEncodeError::KeyTooLarge);
    }
    let record_bytes =
        RecordHeader::aligned_len(key_len, value_len).ok_or(RecordEncodeError::LengthOverflow)?;
    if record_bytes > MAX_RECORD_LEN {
        return Err(RecordEncodeError::RecordTooLarge);
    }
    Ok(record_bytes)
}

/// Returns the minimum record envelope used while filling an append staging span.
///
/// Direct-I/O alignment belongs to the batch seal: staging extends only the
/// final record under a manager-issued padding receipt.
/// Encodes one value into its exact reserved range and returns its index key.
///
/// `reservation.record_bytes` is the minimum record envelope on the
/// path. Staging may later extend the final record of a sealed batch, rewriting
/// its header and completion descriptor together.
#[cfg(test)]
pub(crate) fn encode_value_into(
    destination: &mut [u8],
    reservation: RegionAppendReservation,
    hash_seed: u64,
    key: &[u8],
    value: &[u8],
) -> Result<(u64, IndexEntry), RecordEncodeError> {
    let required = required_record_bytes(key.len(), value.len())?;
    let hash = hash_key(hash_seed, key);
    let entry = encode_value_into_hashed(destination, reservation, hash, required, key, value)?;
    Ok((hash, entry))
}

/// Encodes using point metadata computed once by the public operation entry.
pub(crate) fn encode_value_into_hashed(
    destination: &mut [u8],
    reservation: RegionAppendReservation,
    hash: u64,
    required: u32,
    key: &[u8],
    value: &[u8],
) -> Result<IndexEntry, RecordEncodeError> {
    encode_value_into_hashed_with_seqno(
        destination,
        reservation,
        hash,
        required,
        key,
        value,
        reservation.seqno,
    )
}

/// Re-encodes one retained cache value at a new physical reservation while
/// preserving the logical mutation sequence used by L1 publication.
pub(crate) fn encode_reinsert_into_hashed(
    destination: &mut [u8],
    reservation: RegionAppendReservation,
    hash: u64,
    required: u32,
    key: &[u8],
    value: &[u8],
    logical_seqno: u64,
) -> Result<IndexEntry, RecordEncodeError> {
    if logical_seqno == 0 {
        return Err(RecordEncodeError::InvalidReservation);
    }
    encode_value_into_hashed_with_seqno(
        destination,
        reservation,
        hash,
        required,
        key,
        value,
        logical_seqno,
    )
}

fn encode_value_into_hashed_with_seqno(
    destination: &mut [u8],
    reservation: RegionAppendReservation,
    hash: u64,
    required: u32,
    key: &[u8],
    value: &[u8],
    logical_seqno: u64,
) -> Result<IndexEntry, RecordEncodeError> {
    let destination_len = destination.len();
    let reserved_len =
        usize::try_from(reservation.record_bytes).map_err(|_| RecordEncodeError::LengthOverflow)?;
    if destination_len != reserved_len {
        return Err(RecordEncodeError::DestinationLengthMismatch {
            reserved: reservation.record_bytes,
            destination: destination_len,
        });
    }
    if reservation.record_bytes < required {
        return Err(RecordEncodeError::ReservationTooSmall {
            required,
            reserved: reservation.record_bytes,
        });
    }
    if reservation.region_created_seqno == 0
        || reservation.seqno < reservation.region_created_seqno
        || !reservation.offset.is_multiple_of(RECORD_ALIGNMENT)
    {
        return Err(RecordEncodeError::InvalidReservation);
    }

    let location = PackedLocation::new(
        reservation.region_id,
        reservation.offset,
        reservation.record_bytes,
    )
    .map_err(RecordEncodeError::InvalidLocation)?;
    let value_len = u32::try_from(value.len()).map_err(|_| RecordEncodeError::LengthOverflow)?;
    let key_len = u16::try_from(key.len()).map_err(|_| RecordEncodeError::KeyTooLarge)?;
    let payload_end = RECORD_HEADER_SIZE
        .checked_add(key.len())
        .and_then(|end| end.checked_add(value.len()))
        .ok_or(RecordEncodeError::LengthOverflow)?;
    if payload_end > destination_len {
        return Err(RecordEncodeError::ReservationTooSmall {
            required,
            reserved: reservation.record_bytes,
        });
    }

    let mut payload_crc = Crc32c::new();
    payload_crc.update(key);
    payload_crc.update(value);

    // Header, key, and value bytes are overwritten below. Clear only the
    // alignment tail so a reused staging span cannot leak old padding.
    destination[payload_end..].fill(0);
    let key_start = RECORD_HEADER_SIZE;
    let value_start = key_start
        .checked_add(key.len())
        .ok_or(RecordEncodeError::LengthOverflow)?;
    destination[key_start..value_start].copy_from_slice(key);
    destination[value_start..payload_end].copy_from_slice(value);

    let header = RecordHeader {
        key_len,
        value_len,
        seqno: logical_seqno,
        key_hash: hash,
        payload_crc: payload_crc.finish(),
        region_generation: reservation.region_created_seqno,
        record_len: reservation.record_bytes,
    };
    destination[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());

    Ok(IndexEntry { location })
}

pub(crate) fn hash_key(seed: u64, key: &[u8]) -> u64 {
    XxHash3_64::oneshot_with_seed(seed, key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seeded_xxh3_vectors_define_key_hashing() {
        let seed = 0x6a09_e667_f3bc_c909;
        let actual = [hash_key(seed, b""), hash_key(seed, b"cache2\0key")];
        assert_eq!(actual, [0x4e79_f242_1392_7a65, 0xd168_c107_36e1_695c,]);
    }

    #[test]
    fn default_sized_value_uses_only_record_alignment() {
        let key_len = b"file/chunk/0007".len();
        let value_len = 16 * 1024;
        let payload_end = RECORD_HEADER_SIZE + key_len + value_len;
        let expected = payload_end.next_multiple_of(RECORD_ALIGNMENT as usize);

        assert_eq!(
            required_record_bytes(key_len, value_len).unwrap() as usize,
            expected
        );
        assert!(!expected.is_multiple_of(crate::io_backend::DIRECT_IO_ALIGNMENT));
    }

    #[test]
    fn reinsertion_preserves_logical_seqno_across_a_new_region_generation() {
        let key = b"key";
        let value = b"value";
        let required = required_record_bytes(key.len(), value.len()).unwrap();
        let reservation = RegionAppendReservation {
            shard_id: 0,
            region_id: 3,
            region_created_seqno: 100,
            offset: 0,
            record_bytes: required,
            seqno: 101,
        };
        let mut destination = vec![0_u8; required as usize];

        encode_reinsert_into_hashed(&mut destination, reservation, 7, required, key, value, 11)
            .unwrap();
        let header = RecordHeader::decode(&destination[..RECORD_HEADER_SIZE]).unwrap();
        assert_eq!(header.seqno, 11);
        assert_eq!(header.region_generation, 100);
    }

    #[test]
    fn invalid_lengths_and_reservations_return_typed_errors_without_writes() {
        assert_eq!(
            required_record_bytes(MAX_KEY_SIZE + 1, 0),
            Err(RecordEncodeError::KeyTooLarge)
        );
        assert_eq!(
            required_record_bytes(0, MAX_RECORD_LEN as usize - RECORD_HEADER_SIZE + 1),
            Err(RecordEncodeError::RecordTooLarge)
        );

        let required = required_record_bytes(3, 16).unwrap();
        let reservation = RegionAppendReservation {
            shard_id: 0,
            region_id: 0,
            region_created_seqno: 1,
            offset: 0,
            record_bytes: required,
            seqno: 1,
        };
        let mut wrong_destination = vec![0xa5; required as usize + 32];
        assert!(matches!(
            encode_value_into(&mut wrong_destination, reservation, 0, b"key", &[7; 16],),
            Err(RecordEncodeError::DestinationLengthMismatch { .. })
        ));
        assert!(wrong_destination.iter().all(|byte| *byte == 0xa5));

        let invalid_generation = RegionAppendReservation {
            region_created_seqno: 0,
            ..reservation
        };
        let mut exact_destination = vec![0xa5; required as usize];
        assert_eq!(
            encode_value_into(
                &mut exact_destination,
                invalid_generation,
                0,
                b"key",
                &[7; 16],
            ),
            Err(RecordEncodeError::InvalidReservation)
        );
        assert!(exact_destination.iter().all(|byte| *byte == 0xa5));
    }
}
