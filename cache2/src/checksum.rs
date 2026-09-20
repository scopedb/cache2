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

//! CRC32C used by the on-disk format.
//!
//! The dependency selects hardware acceleration when the host supports it and
//! retains a portable software fallback. This wrapper keeps the cache's codec
//! API and checksum values independent of that implementation detail.

use hashcrew::crc::Crc32Iscsi;
use hashcrew::crc::crc32_iscsi;

/// Computes the standard CRC32C checksum of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32_iscsi(bytes)
}

/// Incremental CRC32C state, useful for checksumming a key and value without first joining them in
/// a temporary allocation.
pub struct Crc32c {
    digest: Crc32Iscsi,
}

impl Crc32c {
    pub fn new() -> Self {
        Self {
            digest: Crc32Iscsi::new(),
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    pub fn finish(self) -> u32 {
        self.digest.digest()
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use crate::checksum::Crc32c;
    use crate::checksum::crc32c;

    #[test]
    fn matches_the_crc32c_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn fragmented_checksums_match_the_crc_fast_reference() {
        let bytes: Vec<_> = (0..65_544).map(|index| (index * 37) as u8).collect();
        for offset in [0, 1, 7] {
            for len in [0, 1, 44, 48, 4092, 4096, 65_537] {
                let input = &bytes[offset..offset + len];
                let expected = crc_fast::crc32_iscsi(input);
                assert_eq!(crc32c(input), expected);
                for split in [0, len.min(44), len.min(56), len / 2, len] {
                    let mut checksum = Crc32c::new();
                    checksum.update(&input[..split]);
                    checksum.update(&[]);
                    checksum.update(&input[split..]);
                    assert_eq!(checksum.finish(), expected, "len={len}, split={split}");
                }
            }
        }

        // Record headers, index pages, and recovery pages zero their checksum
        // field without concatenating the surrounding slices.
        for (len, checksum_offset) in [(48, 44), (4096, 56), (4096, 4092)] {
            let mut page = bytes[..len].to_vec();
            page[checksum_offset..checksum_offset + 4].fill(0);
            let mut checksum = Crc32c::new();
            checksum.update(&page[..checksum_offset]);
            checksum.update(&[0; 4]);
            checksum.update(&page[checksum_offset + 4..]);
            assert_eq!(checksum.finish(), crc_fast::crc32_iscsi(&page));
        }
    }
}
