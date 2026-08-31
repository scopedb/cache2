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

//! CRC32C (Castagnoli) used by the on-disk format.
//!
//! The dependency selects hardware acceleration when the host supports it and
//! retains a portable software fallback. This wrapper keeps the cache's codec
//! API and checksum values independent from that implementation detail.

use crc_fast::{CrcAlgorithm, Digest, crc32_iscsi};

/// Computes the standard CRC32C checksum of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc32_iscsi(bytes)
}

/// Incremental CRC32C state, useful for checksumming a key and value without
/// first joining them in a temporary allocation.
pub(crate) struct Crc32c {
    digest: Digest,
}

impl Crc32c {
    pub(crate) fn new() -> Self {
        Self {
            digest: Digest::new(CrcAlgorithm::Crc32Iscsi),
        }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.digest.update(bytes);
    }

    pub(crate) fn finish(self) -> u32 {
        self.digest.finalize() as u32
    }
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{Crc32c, crc32c};

    #[test]
    fn matches_the_crc32c_check_value() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(b""), 0);
    }

    #[test]
    fn arbitrary_chunking_matches_one_shot_checksum() {
        let payload = (0..64 * 1024)
            .map(|index| ((index * 31 + index / 7) & 0xff) as u8)
            .collect::<Vec<_>>();
        let expected = crc32c(&payload);

        for chunk_bytes in [1, 7, 64, 4093, payload.len()] {
            let mut checksum = Crc32c::new();
            for chunk in payload.chunks(chunk_bytes) {
                checksum.update(chunk);
            }
            assert_eq!(checksum.finish(), expected, "chunk size {chunk_bytes}");
        }
    }
}
