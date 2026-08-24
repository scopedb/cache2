//! Checked value-record encoding for the RegionStore append path.
//!
//! The encoder uses the stable record envelope while keeping batch padding and
//! physical location authority in the append reservation. The encoder performs
//! no allocation and computes the payload CRC incrementally from the logical
//! namespace key and value.

use std::fmt;

use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::checksum::Crc32c;
use crate::format::{
    MAX_KEY_SIZE, MAX_VALUE_SIZE, RECORD_HEADER_SIZE, RecordCodec, RecordHeader, RecordKind,
};
use crate::index::{IndexEntry, MAX_RECORD_LEN, PackedLocation, PackedLocationError};
use crate::recovery::{RECORD_ALIGNMENT, REGION_HEADER_SIZE};
use crate::region_manager::RegionAppendReservation;

const NAMESPACE_KEY_PREFIX_SIZE: usize = size_of::<u32>();
const NAMESPACE_HASH_DOMAIN: &[u8] = b"cache-rs/ns/v1\0";
const NAMESPACE_HASH_CONTEXT_SIZE: usize = NAMESPACE_HASH_DOMAIN.len() + size_of::<u32>();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct EncodedValue {
    pub(crate) hash: u64,
    pub(crate) entry: IndexEntry,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordEncodeError {
    LengthOverflow,
    KeyTooLarge,
    ValueTooLarge,
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
            Self::ValueTooLarge => formatter.write_str("record value exceeds the format limit"),
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
    namespace_id: u32,
    key_len: usize,
    value_len: usize,
) -> Result<u32, RecordEncodeError> {
    let encoded_key_len = encoded_key_len(namespace_id, key_len)?;
    if encoded_key_len > MAX_KEY_SIZE {
        return Err(RecordEncodeError::KeyTooLarge);
    }
    if value_len > MAX_VALUE_SIZE {
        return Err(RecordEncodeError::ValueTooLarge);
    }
    let record_bytes = RecordHeader::aligned_len(encoded_key_len, value_len)
        .ok_or(RecordEncodeError::LengthOverflow)?;
    if record_bytes > MAX_RECORD_LEN {
        return Err(RecordEncodeError::RecordTooLarge);
    }
    Ok(record_bytes)
}

/// Returns the minimum record envelope used while filling a write batch.
///
/// Direct-I/O alignment belongs to the batch seal: staging extends only the
/// final record under a manager-issued padding receipt.
pub(crate) fn planned_record_bytes(
    namespace_id: u32,
    key_len: usize,
    value_len: usize,
) -> Result<u32, RecordEncodeError> {
    required_record_bytes(namespace_id, key_len, value_len)
}

/// Encodes one value into its exact reserved range and returns its index key.
///
/// `reservation.record_bytes` is the minimum record envelope on the
/// path. Staging may later extend the final record of a sealed batch, rewriting
/// its header and completion descriptor together.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_value_into(
    destination: &mut [u8],
    reservation: RegionAppendReservation,
    hash_seed: u64,
    namespace_id: u32,
    key: &[u8],
    value: &[u8],
    expires_at: u64,
) -> Result<EncodedValue, RecordEncodeError> {
    let required = required_record_bytes(namespace_id, key.len(), value.len())?;
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
    if reservation.cache_epoch == 0
        || reservation.region_incarnation == 0
        || reservation.region_incarnation == u32::MAX
        || reservation.seqno == 0
        || reservation.offset < REGION_HEADER_SIZE
        || reservation.offset % RECORD_ALIGNMENT != 0
    {
        return Err(RecordEncodeError::InvalidReservation);
    }

    let location = PackedLocation::new(
        reservation.region_id,
        reservation.offset,
        reservation.record_bytes,
        false,
    )
    .map_err(RecordEncodeError::InvalidLocation)?;
    let encoded_key_len = encoded_key_len(namespace_id, key.len())?;
    let value_len = u32::try_from(value.len()).map_err(|_| RecordEncodeError::ValueTooLarge)?;
    let key_len = u32::try_from(encoded_key_len).map_err(|_| RecordEncodeError::LengthOverflow)?;
    let payload_end = RECORD_HEADER_SIZE
        .checked_add(encoded_key_len)
        .and_then(|end| end.checked_add(value.len()))
        .ok_or(RecordEncodeError::LengthOverflow)?;
    if payload_end > destination_len {
        return Err(RecordEncodeError::ReservationTooSmall {
            required,
            reserved: reservation.record_bytes,
        });
    }

    let namespace_bytes = namespace_id.to_le_bytes();
    let hash = hash_namespaced_key(hash_seed, namespace_id, key);
    let mut payload_crc = Crc32c::new();
    if namespace_id != 0 {
        payload_crc.update(&namespace_bytes);
    }
    payload_crc.update(key);
    payload_crc.update(value);

    destination.fill(0);
    let key_start = RECORD_HEADER_SIZE;
    let raw_key_start = if namespace_id == 0 {
        key_start
    } else {
        let raw_key_start = key_start
            .checked_add(NAMESPACE_KEY_PREFIX_SIZE)
            .ok_or(RecordEncodeError::LengthOverflow)?;
        destination[key_start..raw_key_start].copy_from_slice(&namespace_bytes);
        raw_key_start
    };
    let value_start = key_start
        .checked_add(encoded_key_len)
        .ok_or(RecordEncodeError::LengthOverflow)?;
    destination[raw_key_start..value_start].copy_from_slice(key);
    destination[value_start..payload_end].copy_from_slice(value);

    let codec = if namespace_id == 0 {
        RecordCodec::PlainKey
    } else {
        RecordCodec::NamespacedKey
    };
    let header = RecordHeader {
        kind: RecordKind::Value,
        codec,
        key_len,
        value_len,
        stored_len: value_len,
        record_len: reservation.record_bytes,
        region_incarnation: reservation.region_incarnation,
        epoch: reservation.cache_epoch,
        seqno: reservation.seqno,
        key_hash: hash,
        expires_at,
        payload_crc: payload_crc.finish(),
    };
    destination[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());

    Ok(EncodedValue {
        hash,
        entry: IndexEntry {
            location,
            seqno: reservation.seqno,
            namespace_id,
            flags: 0,
        },
    })
}

fn encoded_key_len(namespace_id: u32, raw_key_len: usize) -> Result<usize, RecordEncodeError> {
    raw_key_len
        .checked_add(if namespace_id == 0 {
            0
        } else {
            NAMESPACE_KEY_PREFIX_SIZE
        })
        .ok_or(RecordEncodeError::LengthOverflow)
}

pub(crate) fn hash_namespaced_key(seed: u64, namespace_id: u32, key: &[u8]) -> u64 {
    if namespace_id == 0 {
        return xxh3_64_with_seed(key, seed);
    }

    // Deriving a namespace-specific seed keeps the common namespace-zero path
    // to one XXH3 call and avoids allocating or constructing XXH3's large
    // streaming state for every point operation.
    let mut context = [0_u8; NAMESPACE_HASH_CONTEXT_SIZE];
    context[..NAMESPACE_HASH_DOMAIN.len()].copy_from_slice(NAMESPACE_HASH_DOMAIN);
    context[NAMESPACE_HASH_DOMAIN.len()..].copy_from_slice(&namespace_id.to_le_bytes());
    let namespaced_seed = xxh3_64_with_seed(&context, seed);
    xxh3_64_with_seed(key, namespaced_seed)
}

#[cfg(test)]
mod tests {
    use crate::checksum::crc32c;

    use super::*;

    #[test]
    fn seeded_xxh3_vectors_define_namespace_hashing() {
        let seed = 0x6a09_e667_f3bc_c909;
        let actual = [
            hash_namespaced_key(seed, 0, b""),
            hash_namespaced_key(seed, 0, b"cache-rs\0key"),
            hash_namespaced_key(seed, 7, b"cache-rs\0key"),
            hash_namespaced_key(seed, u32::MAX, b"cache-rs\0key"),
        ];
        assert_eq!(
            actual,
            [
                0x4e79_f242_1392_7a65,
                0xc0ec_b0e2_0a80_44ca,
                0x13a9_9969_cbe6_fa32,
                0x9102_df2d_b679_ea94,
            ]
        );
    }

    #[test]
    fn namespaced_16k_value_uses_the_minimum_format_envelope() {
        let namespace_id = 42;
        let key = b"file/chunk/0007";
        let value = (0..16 * 1024)
            .map(|index| ((index * 17 + index / 13) & 0xff) as u8)
            .collect::<Vec<_>>();
        let required = required_record_bytes(namespace_id, key.len(), value.len()).unwrap();
        let planned = planned_record_bytes(namespace_id, key.len(), value.len()).unwrap();
        assert_eq!(planned, required);
        assert_ne!(planned as usize % crate::io_backend::DIRECT_IO_ALIGNMENT, 0);
        let reservation = RegionAppendReservation {
            shard_id: 0,
            cache_epoch: 5,
            region_id: 7,
            region_incarnation: 3,
            offset: REGION_HEADER_SIZE,
            record_bytes: planned,
            seqno: 17,
        };
        let mut destination = vec![0xa5; planned as usize];

        let encoded = encode_value_into(
            &mut destination,
            reservation,
            0x0123_4567_89ab_cdef,
            namespace_id,
            key,
            &value,
            123_456,
        )
        .unwrap();

        assert_eq!(encoded.hash, 0x3c68_d7f9_bfae_9378);
        assert_eq!(encoded.entry.location.region_id(), 7);
        assert_eq!(encoded.entry.location.offset(), REGION_HEADER_SIZE);
        assert_eq!(encoded.entry.location.record_len(), planned);
        assert!(!encoded.entry.location.is_tombstone());
        assert_eq!(encoded.entry.seqno, 17);
        assert_eq!(encoded.entry.namespace_id, namespace_id);
        assert_eq!(encoded.entry.flags, 0);

        let header = RecordHeader::decode(&destination[..RECORD_HEADER_SIZE]).unwrap();
        assert_eq!(header.kind, RecordKind::Value);
        assert_eq!(header.codec, RecordCodec::NamespacedKey);
        assert_eq!(
            header.key_len,
            (NAMESPACE_KEY_PREFIX_SIZE + key.len()) as u32
        );
        assert_eq!(header.value_len, value.len() as u32);
        assert_eq!(header.stored_len, value.len() as u32);
        assert_eq!(header.record_len, planned);
        assert_eq!(header.region_incarnation, 3);
        assert_eq!(header.epoch, 5);
        assert_eq!(header.seqno, 17);
        assert_eq!(header.key_hash, encoded.hash);
        assert_eq!(header.expires_at, 123_456);

        let key_start = RECORD_HEADER_SIZE;
        let raw_key_start = key_start + NAMESPACE_KEY_PREFIX_SIZE;
        let value_start = raw_key_start + key.len();
        let payload_end = value_start + value.len();
        assert_eq!(
            &destination[key_start..raw_key_start],
            &namespace_id.to_le_bytes()
        );
        assert_eq!(&destination[raw_key_start..value_start], key);
        assert_eq!(&destination[value_start..payload_end], value);
        assert_eq!(
            header.payload_crc,
            crc32c(&destination[key_start..payload_end])
        );
        assert!(destination[payload_end..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn target_chunk_sizes_do_not_pay_per_record_direct_io_padding() {
        let mut cursor = usize::try_from(REGION_HEADER_SIZE).unwrap();
        let mut saw_non_direct_boundary = false;
        for (namespace_id, key_len, value_len) in [
            (7, b"file/chunk/0007".len(), 16 * 1024),
            (0, b"0123456789abcdef0123456789abcdef".len(), 256 * 1024),
        ] {
            let required = required_record_bytes(namespace_id, key_len, value_len).unwrap();
            let planned = planned_record_bytes(namespace_id, key_len, value_len).unwrap();
            assert_eq!(planned, required);
            assert_eq!(planned % RECORD_ALIGNMENT, 0);
            cursor += planned as usize;
            saw_non_direct_boundary |= cursor % crate::io_backend::DIRECT_IO_ALIGNMENT != 0;
        }
        assert!(saw_non_direct_boundary);
    }

    #[test]
    fn invalid_lengths_and_reservations_return_typed_errors_without_writes() {
        assert_eq!(
            required_record_bytes(1, MAX_KEY_SIZE, 0),
            Err(RecordEncodeError::KeyTooLarge)
        );
        assert_eq!(
            required_record_bytes(0, 1, MAX_VALUE_SIZE + 1),
            Err(RecordEncodeError::ValueTooLarge)
        );

        let required = required_record_bytes(0, 3, 16).unwrap();
        let reservation = RegionAppendReservation {
            shard_id: 0,
            cache_epoch: 1,
            region_id: 0,
            region_incarnation: 1,
            offset: REGION_HEADER_SIZE,
            record_bytes: required,
            seqno: 1,
        };
        let mut wrong_destination = vec![0xa5; required as usize + 32];
        assert!(matches!(
            encode_value_into(
                &mut wrong_destination,
                reservation,
                0,
                0,
                b"key",
                &[7; 16],
                0,
            ),
            Err(RecordEncodeError::DestinationLengthMismatch { .. })
        ));
        assert!(wrong_destination.iter().all(|byte| *byte == 0xa5));
    }
}
