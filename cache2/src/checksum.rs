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

//! CRC32C over borrowed byte segments used by the on-disk formats.
//!
//! The dependency selects hardware acceleration when the host supports it and
//! retains a portable software fallback. Each format selects its checksum
//! input, including any fields represented as zero bytes.

use hashcrew::crc::Crc32Iscsi;

/// Computes CRC32C over the concatenation of `parts` without allocating or copying.
/// Segment boundaries do not affect the result; empty input returns zero.
pub fn crc32c(parts: &[&[u8]]) -> u32 {
    let mut checksum = Crc32Iscsi::new();
    for part in parts {
        checksum.update(part);
    }
    checksum.digest()
}

#[cfg(test)]
mod tests {
    use crate::checksum::crc32c;

    #[test]
    fn matches_the_crc32c_check_value() {
        assert_eq!(crc32c(&[b"123456789"]), 0xe306_9283);
        assert_eq!(crc32c(&[b""]), 0);
        assert_eq!(crc32c(&[]), 0);
    }

    #[test]
    fn fragmented_checksums_match_the_crc_fast_reference() {
        let bytes: Vec<_> = (0..65_544).map(|index| (index * 37) as u8).collect();
        for offset in [0, 1, 7] {
            for len in [0, 1, 44, 48, 4092, 4096, 65_537] {
                let input = &bytes[offset..offset + len];
                let expected = crc_fast::crc32_iscsi(input);
                assert_eq!(crc32c(&[input]), expected);
                for split in [0, len.min(44), len.min(56), len / 2, len] {
                    let checksum = crc32c(&[&input[..split], &[], &input[split..]]);
                    assert_eq!(checksum, expected, "len={len}, split={split}");
                }
            }
        }

        // Record headers, index pages, and recovery pages zero their checksum
        // field without concatenating the surrounding slices.
        for (len, checksum_offset) in [(48, 44), (4096, 56), (4096, 4092)] {
            let page = &bytes[..len];
            let mut expected = page.to_vec();
            expected[checksum_offset..checksum_offset + 4].fill(0);
            let checksum = crc32c(&[
                &page[..checksum_offset],
                &[0; 4],
                &page[checksum_offset + 4..],
            ]);
            assert_eq!(checksum, crc_fast::crc32_iscsi(&expected));
        }
    }
}
