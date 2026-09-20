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

//! Little-endian field codecs shared by the persistent formats.
//!
//! The on-disk structures are encoded field-by-field; their Rust layout is not
//! part of the disk format. Reads are checked so decoders can reject truncated
//! input instead of panicking, while writes panic because encoders size their
//! buffers from the same constants as their field offsets.

use crate::checksum::Crc32c;

/// Reads the little-endian `u16` at `offset`, or returns `None` when the field
/// range falls outside `input`.
pub fn get_u16(input: &[u8], offset: usize) -> Option<u16> {
    let bytes: [u8; size_of::<u16>()] = input
        .get(offset..offset.checked_add(size_of::<u16>())?)?
        .try_into()
        .ok()?;
    Some(u16::from_le_bytes(bytes))
}

/// Reads the little-endian `u32` at `offset`, or returns `None` when the field
/// range falls outside `input`.
pub fn get_u32(input: &[u8], offset: usize) -> Option<u32> {
    let bytes: [u8; size_of::<u32>()] = input
        .get(offset..offset.checked_add(size_of::<u32>())?)?
        .try_into()
        .ok()?;
    Some(u32::from_le_bytes(bytes))
}

/// Reads the little-endian `u64` at `offset`, or returns `None` when the field
/// range falls outside `input`.
pub fn get_u64(input: &[u8], offset: usize) -> Option<u64> {
    let bytes: [u8; size_of::<u64>()] = input
        .get(offset..offset.checked_add(size_of::<u64>())?)?
        .try_into()
        .ok()?;
    Some(u64::from_le_bytes(bytes))
}

/// Writes `value` as little-endian bytes at `offset`.
///
/// Panics when the field range falls outside `output`; encoders size their
/// buffers from the same constants as their field offsets.
pub fn put_u16(output: &mut [u8], offset: usize, value: u16) {
    output[offset..offset + size_of::<u16>()].copy_from_slice(&value.to_le_bytes());
}

/// Writes `value` as little-endian bytes at `offset`.
///
/// Panics when the field range falls outside `output`; encoders size their
/// buffers from the same constants as their field offsets.
pub fn put_u32(output: &mut [u8], offset: usize, value: u32) {
    output[offset..offset + size_of::<u32>()].copy_from_slice(&value.to_le_bytes());
}

/// Writes `value` as little-endian bytes at `offset`.
///
/// Panics when the field range falls outside `output`; encoders size their
/// buffers from the same constants as their field offsets.
pub fn put_u64(output: &mut [u8], offset: usize, value: u64) {
    output[offset..offset + size_of::<u64>()].copy_from_slice(&value.to_le_bytes());
}

/// Returns the CRC32C of `input` with the little-endian `u32` checksum field at
/// `checksum_offset` treated as zero, or `None` when the field range falls
/// outside `input`.
///
/// Persistent headers and pages checksum their image with the checksum field
/// itself zeroed, so writers and readers cover the same bytes without copying.
pub fn crc32c_with_zeroed_u32(input: &[u8], checksum_offset: usize) -> Option<u32> {
    let field_end = checksum_offset.checked_add(size_of::<u32>())?;
    let before = input.get(..checksum_offset)?;
    let after = input.get(field_end..)?;
    let mut checksum = Crc32c::new();
    checksum.update(before);
    checksum.update(&[0; size_of::<u32>()]);
    checksum.update(after);
    Some(checksum.finish())
}

/// Returns whether the stored checksum field at `checksum_offset` matches
/// [`crc32c_with_zeroed_u32`].
pub fn crc32c_with_zeroed_u32_matches(input: &[u8], checksum_offset: usize) -> bool {
    let Some(expected) = get_u32(input, checksum_offset) else {
        return false;
    };
    crc32c_with_zeroed_u32(input, checksum_offset) == Some(expected)
}
