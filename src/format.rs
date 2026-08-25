//! Explicit little-endian codecs for the persistent cache metadata and records.
//!
//! These types are deliberately encoded field-by-field. Their Rust layout is
//! not part of the disk format.

use crate::checksum::{Crc32c, crc32c};

pub(crate) const FORMAT_VERSION: u16 = 1;

pub(crate) const RECORD_HEADER_SIZE: usize = 64;
pub(crate) const RECORD_ALIGNMENT: usize = 32;

pub(crate) const MAX_KEY_SIZE: usize = 64 * 1024;
pub(crate) const MAX_VALUE_SIZE: usize = 16 * 1024 * 1024;

pub(crate) const RECORD_HEADER_MAGIC: [u8; 4] = *b"CRCD";

pub(crate) const RECORD_HEADER_CRC_OFFSET: usize = RECORD_HEADER_SIZE - size_of::<u32>();

const RECORD_VERSION_OFFSET: usize = 4;
const RECORD_KIND_OFFSET: usize = 6;
const RECORD_CODEC_OFFSET: usize = 7;
const RECORD_KEY_LEN_OFFSET: usize = 8;
const RECORD_VALUE_LEN_OFFSET: usize = 12;
const RECORD_STORED_LEN_OFFSET: usize = 16;
const RECORD_LEN_OFFSET: usize = 20;
const RECORD_REGION_INCARNATION_OFFSET: usize = 24;
const RECORD_EPOCH_OFFSET: usize = 28;
const RECORD_SEQNO_OFFSET: usize = 32;
const RECORD_KEY_HASH_OFFSET: usize = 40;
const RECORD_EXPIRES_AT_OFFSET: usize = 48;
const RECORD_PAYLOAD_CRC_OFFSET: usize = 56;

const CODEC_NONE: u8 = 0;
const CODEC_NAMESPACED_KEY: u8 = 1;
const RECORD_KIND_VALUE: u8 = 1;

/// Interpretation of the key bytes stored in a record.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RecordCodec {
    PlainKey = CODEC_NONE,
    NamespacedKey = CODEC_NAMESPACED_KEY,
}

impl RecordCodec {
    fn decode(value: u8) -> Option<Self> {
        match value {
            CODEC_NONE => Some(Self::PlainKey),
            CODEC_NAMESPACED_KEY => Some(Self::NamespacedKey),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RecordHeader {
    pub(crate) codec: RecordCodec,
    pub(crate) key_len: u32,
    pub(crate) value_len: u32,
    pub(crate) stored_len: u32,
    pub(crate) record_len: u32,
    pub(crate) region_incarnation: u32,
    pub(crate) epoch: u32,
    pub(crate) seqno: u64,
    pub(crate) key_hash: u64,
    pub(crate) expires_at: u64,
    pub(crate) payload_crc: u32,
}

impl RecordHeader {
    /// Returns the smallest 32-byte-aligned record length for this payload.
    pub(crate) fn aligned_len(key_len: usize, stored_len: usize) -> Option<u32> {
        let unaligned = RECORD_HEADER_SIZE
            .checked_add(key_len)?
            .checked_add(stored_len)?;
        let aligned = checked_align_up(unaligned, RECORD_ALIGNMENT)?;
        u32::try_from(aligned).ok()
    }

    pub(crate) fn encode(&self) -> [u8; RECORD_HEADER_SIZE] {
        let mut output = [0_u8; RECORD_HEADER_SIZE];
        output[..RECORD_HEADER_MAGIC.len()].copy_from_slice(&RECORD_HEADER_MAGIC);
        put_u16(&mut output, RECORD_VERSION_OFFSET, FORMAT_VERSION);
        output[RECORD_KIND_OFFSET] = RECORD_KIND_VALUE;
        output[RECORD_CODEC_OFFSET] = self.codec as u8;
        put_u32(&mut output, RECORD_KEY_LEN_OFFSET, self.key_len);
        put_u32(&mut output, RECORD_VALUE_LEN_OFFSET, self.value_len);
        put_u32(&mut output, RECORD_STORED_LEN_OFFSET, self.stored_len);
        put_u32(&mut output, RECORD_LEN_OFFSET, self.record_len);
        put_u32(
            &mut output,
            RECORD_REGION_INCARNATION_OFFSET,
            self.region_incarnation,
        );
        put_u32(&mut output, RECORD_EPOCH_OFFSET, self.epoch);
        put_u64(&mut output, RECORD_SEQNO_OFFSET, self.seqno);
        put_u64(&mut output, RECORD_KEY_HASH_OFFSET, self.key_hash);
        put_u64(&mut output, RECORD_EXPIRES_AT_OFFSET, self.expires_at);
        put_u32(&mut output, RECORD_PAYLOAD_CRC_OFFSET, self.payload_crc);

        let checksum = crc32c(&output);
        put_u32(&mut output, RECORD_HEADER_CRC_OFFSET, checksum);
        output
    }

    pub(crate) fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != RECORD_HEADER_SIZE
            || input.get(..RECORD_HEADER_MAGIC.len())? != RECORD_HEADER_MAGIC
            || get_u16(input, RECORD_VERSION_OFFSET)? != FORMAT_VERSION
            || *input.get(RECORD_KIND_OFFSET)? != RECORD_KIND_VALUE
            || !checksum_matches(input, RECORD_HEADER_CRC_OFFSET)
        {
            return None;
        }

        let header = Self {
            codec: RecordCodec::decode(*input.get(RECORD_CODEC_OFFSET)?)?,
            key_len: get_u32(input, RECORD_KEY_LEN_OFFSET)?,
            value_len: get_u32(input, RECORD_VALUE_LEN_OFFSET)?,
            stored_len: get_u32(input, RECORD_STORED_LEN_OFFSET)?,
            record_len: get_u32(input, RECORD_LEN_OFFSET)?,
            region_incarnation: get_u32(input, RECORD_REGION_INCARNATION_OFFSET)?,
            epoch: get_u32(input, RECORD_EPOCH_OFFSET)?,
            seqno: get_u64(input, RECORD_SEQNO_OFFSET)?,
            key_hash: get_u64(input, RECORD_KEY_HASH_OFFSET)?,
            expires_at: get_u64(input, RECORD_EXPIRES_AT_OFFSET)?,
            payload_crc: get_u32(input, RECORD_PAYLOAD_CRC_OFFSET)?,
        };

        if !header.has_valid_lengths() {
            return None;
        }
        Some(header)
    }

    pub(crate) fn has_valid_lengths(&self) -> bool {
        let Ok(key_len) = usize::try_from(self.key_len) else {
            return false;
        };
        let Ok(value_len) = usize::try_from(self.value_len) else {
            return false;
        };
        let Ok(stored_len) = usize::try_from(self.stored_len) else {
            return false;
        };
        let Ok(record_len) = usize::try_from(self.record_len) else {
            return false;
        };

        if key_len > MAX_KEY_SIZE
            || value_len > MAX_VALUE_SIZE
            || stored_len > MAX_VALUE_SIZE
            || record_len % RECORD_ALIGNMENT != 0
        {
            return false;
        }
        if value_len != stored_len {
            return false;
        }

        Self::aligned_len(key_len, stored_len).is_some_and(|minimum| self.record_len >= minimum)
    }
}

pub(crate) fn checked_align_up(value: usize, alignment: usize) -> Option<usize> {
    if !alignment.is_power_of_two() {
        return None;
    }
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|rounded| rounded & !(alignment - 1))
}

fn checksum_matches(input: &[u8], checksum_offset: usize) -> bool {
    let Some(expected) = get_u32(input, checksum_offset) else {
        return false;
    };
    let Some(after_checksum) = checksum_offset.checked_add(size_of::<u32>()) else {
        return false;
    };
    let (Some(before), Some(after)) = (input.get(..checksum_offset), input.get(after_checksum..))
    else {
        return false;
    };

    let mut checksum = Crc32c::new();
    checksum.update(before);
    checksum.update(&[0; size_of::<u32>()]);
    checksum.update(after);
    checksum.finish() == expected
}

fn get_u16(input: &[u8], offset: usize) -> Option<u16> {
    let bytes: [u8; size_of::<u16>()] = input
        .get(offset..offset.checked_add(size_of::<u16>())?)?
        .try_into()
        .ok()?;
    Some(u16::from_le_bytes(bytes))
}

fn get_u32(input: &[u8], offset: usize) -> Option<u32> {
    let bytes: [u8; size_of::<u32>()] = input
        .get(offset..offset.checked_add(size_of::<u32>())?)?
        .try_into()
        .ok()?;
    Some(u32::from_le_bytes(bytes))
}

fn get_u64(input: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; size_of::<u64>()] = input
        .get(offset..offset.checked_add(size_of::<u64>())?)?
        .try_into()
        .ok()?;
    Some(u64::from_le_bytes(bytes))
}

fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sparse_golden(input: &str) -> Vec<u8> {
        let mut output: Option<Vec<u8>> = None;
        for raw_line in input.lines() {
            let line = raw_line.split('#').next().unwrap().trim();
            if line.is_empty() {
                continue;
            }
            let mut fields = line.split_whitespace();
            let first = fields.next().unwrap();
            if first == "length" {
                let length = fields.next().unwrap().parse::<usize>().unwrap();
                assert!(output.replace(vec![0_u8; length]).is_none());
                continue;
            }
            let offset = usize::from_str_radix(first, 16).unwrap();
            let encoded = fields.next().unwrap();
            assert_eq!(encoded.len() % 2, 0);
            let bytes = encoded
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect::<Vec<_>>();
            let output = output.as_mut().expect("golden length must come first");
            output[offset..offset + bytes.len()].copy_from_slice(&bytes);
            assert!(fields.next().is_none());
        }
        output.expect("golden fixture must declare its length")
    }

    #[test]
    fn value_record_matches_committed_golden_bytes() {
        let key = b"key";
        let value = b"value";
        let mut payload = key.to_vec();
        payload.extend_from_slice(value);
        let header = RecordHeader {
            codec: RecordCodec::PlainKey,
            key_len: key.len() as u32,
            value_len: value.len() as u32,
            stored_len: value.len() as u32,
            record_len: 96,
            region_incarnation: 9,
            epoch: 3,
            seqno: 34,
            key_hash: 0x1122_3344_5566_7788,
            expires_at: 0x0102_0304_0506_0708,
            payload_crc: crc32c(&payload),
        };
        let mut encoded = vec![0_u8; header.record_len as usize];
        encoded[..RECORD_HEADER_SIZE].copy_from_slice(&header.encode());
        encoded[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload.len()].copy_from_slice(&payload);
        let golden = sparse_golden(include_str!(
            "../tests/fixtures/format_v1/value_record.golden"
        ));
        assert_eq!(encoded, golden);
        assert_eq!(
            RecordHeader::decode(&golden[..RECORD_HEADER_SIZE]),
            Some(header)
        );
    }

    #[test]
    fn record_round_trip_and_alignment() {
        let header = RecordHeader {
            codec: RecordCodec::PlainKey,
            key_len: 3,
            value_len: 5,
            stored_len: 5,
            record_len: RecordHeader::aligned_len(3, 5).unwrap(),
            region_incarnation: 9,
            epoch: 2,
            seqno: 101,
            key_hash: 0xfedc_ba98_7654_3210,
            expires_at: 1234,
            payload_crc: 77,
        };

        assert_eq!(header.record_len, 96);
        assert_eq!(RecordHeader::decode(&header.encode()), Some(header));

        let mut namespaced = header;
        namespaced.codec = RecordCodec::NamespacedKey;
        assert_eq!(RecordHeader::decode(&namespaced.encode()), Some(namespaced));

        let mut with_batch_padding = header;
        with_batch_padding.record_len = 4 * 1024;
        assert_eq!(
            RecordHeader::decode(&with_batch_padding.encode()),
            Some(with_batch_padding)
        );
    }

    #[test]
    fn record_decode_rejects_bad_crc_and_lengths() {
        let header = RecordHeader {
            codec: RecordCodec::PlainKey,
            key_len: 8,
            value_len: 1,
            stored_len: 1,
            record_len: RecordHeader::aligned_len(8, 1).unwrap(),
            region_incarnation: 1,
            epoch: 1,
            seqno: 2,
            key_hash: 3,
            expires_at: 0,
            payload_crc: 4,
        };
        let mut encoded = header.encode();
        encoded[RECORD_SEQNO_OFFSET] ^= 0x80;
        assert_eq!(RecordHeader::decode(&encoded), None);

        let mut too_short = header;
        too_short.record_len = RECORD_HEADER_SIZE as u32;
        assert_eq!(RecordHeader::decode(&too_short.encode()), None);

        let mut mismatched_value = header;
        mismatched_value.value_len = 2;
        assert_eq!(RecordHeader::decode(&mismatched_value.encode()), None);

        assert_eq!(RecordHeader::aligned_len(usize::MAX, 1), None);
    }
}
