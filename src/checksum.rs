//! Dependency-free CRC32C (Castagnoli) used by the on-disk format.

const CRC32C_POLYNOMIAL: u32 = 0x82f6_3b78;

const CRC32C_TABLE: [u32; 256] = make_table();

const fn make_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0;
    while index < table.len() {
        let mut value = index as u32;
        let mut bit = 0;
        while bit < 8 {
            value = if value & 1 == 1 {
                (value >> 1) ^ CRC32C_POLYNOMIAL
            } else {
                value >> 1
            };
            bit += 1;
        }
        table[index] = value;
        index += 1;
    }
    table
}

/// Computes the standard CRC32C checksum of `bytes`.
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    let mut checksum = Crc32c::new();
    checksum.update(bytes);
    checksum.finish()
}

/// Incremental CRC32C state, useful for checksumming a key and value without
/// first joining them in a temporary allocation.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Crc32c {
    state: u32,
}

impl Crc32c {
    pub(crate) const fn new() -> Self {
        Self { state: u32::MAX }
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            let index = ((self.state ^ u32::from(byte)) & 0xff) as usize;
            self.state = (self.state >> 8) ^ CRC32C_TABLE[index];
        }
    }

    pub(crate) const fn finish(self) -> u32 {
        !self.state
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
}
