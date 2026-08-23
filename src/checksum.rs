//! CRC32C (Castagnoli) used by the on-disk format.
//!
//! The dependency selects hardware acceleration when the host supports it and
//! retains a portable software fallback. This wrapper keeps the cache's codec
//! API and checksum values independent from that implementation detail.

use crc32c::{crc32c as accelerated_crc32c, crc32c_append};

/// Computes the standard CRC32C checksum of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    accelerated_crc32c(bytes)
}

/// Incremental CRC32C state, useful for checksumming a key and value without
/// first joining them in a temporary allocation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Crc32c {
    state: u32,
}

impl Crc32c {
    pub(crate) const fn new() -> Self {
        Self { state: 0 }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        self.state = crc32c_append(self.state, bytes);
    }

    pub(crate) const fn finish(self) -> u32 {
        self.state
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
    fn incremental_updates_match_one_shot_checksum() {
        let mut checksum = Crc32c::new();
        checksum.update(b"key");
        checksum.update(b"value");

        assert_eq!(checksum.finish(), crc32c(b"keyvalue"));
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
