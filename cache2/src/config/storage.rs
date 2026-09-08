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

//! Persistent layout construction and disk accounting.

use std::io;

use crate::error::{Error, ErrorOperation, Result};
use crate::index::{MAX_PACKED_REGION_COUNT, MAX_PACKED_REGION_SIZE};
use crate::index_storage::{
    IndexStorageError, canonical_index_partition_ranges, validate_index_slot_count,
};
use crate::recovery::{
    DataGeometry, KEY_HASH_ALGORITHM_XXH3_64, RECOVERY_IMAGE_INDEX_OFFSET, STATE_FILE_SIZE,
    recovery_image_index_len,
};
use crate::region_metadata::{
    REGION_METADATA_PAGE_SIZE, REGION_METADATA_PARTITIONS_PER_PAGE,
    REGION_METADATA_REGIONS_PER_PAGE,
};

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_EXPECTED_ENTRY_BYTES: u64 = 16 * 1024;
pub(crate) const KEY_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const MIN_INDEX_SLOTS: usize = 8;
const STATIC_FINGERPRINT_SCHEMA: u64 = 3;

/// Persistent L2 geometry and fixed-index sizing.
///
/// These values define the static disk identity. A clean image created with a
/// different static configuration is discarded and the cache starts empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StaticConfig {
    pub(crate) capacity_bytes: u64,
    pub(crate) region_size_bytes: u64,
    pub(crate) index_slots: usize,
}

impl StaticConfig {
    /// Creates static L2 geometry and sizes its index assuming an average
    /// 16 KiB live entry.
    ///
    /// `capacity_bytes` is the total Region extent, excluding the data
    /// superblock, state, and clean-image files. The default Region size is
    /// 32 MiB. Index size grows with capacity and is accepted when its complete
    /// page and mapping layout is representable. Use
    /// [`Self::with_expected_entries`] when the live-key count is known, and use
    /// [`Self::peak_disk_bytes`] for the cache-owned logical disk bound.
    pub fn new(capacity_bytes: u64) -> Self {
        let expected_entries = capacity_bytes / DEFAULT_EXPECTED_ENTRY_BYTES;
        let index_slots = usize::try_from(
            expected_entries
                .saturating_mul(2)
                .max(MIN_INDEX_SLOTS as u64),
        )
        .unwrap_or(usize::MAX);
        Self {
            capacity_bytes,
            region_size_bytes: DEFAULT_REGION_SIZE,
            index_slots,
        }
    }

    /// Sets the Region size in bytes.
    ///
    /// Valid sizes are 4 KiB multiples through 32 MiB. L2 capacity is an exact
    /// multiple containing at least two Regions. A complete encoded record fits
    /// in one Region. Every append shard eagerly owns two Region-sized staging
    /// buffers.
    pub fn with_region_size_bytes(mut self, bytes: u64) -> Self {
        self.region_size_bytes = bytes;
        self
    }

    /// Sizes the fixed L2 index for the expected number of simultaneously live
    /// keys.
    ///
    /// This counts simultaneously live keys. The resulting index uses roughly
    /// two physical buckets per expected live key and is part of the static disk
    /// identity.
    pub fn with_expected_entries(mut self, entries: usize) -> Self {
        self.index_slots = entries.saturating_mul(2).max(MIN_INDEX_SLOTS);
        self
    }

    /// Returns the total Region extent in bytes.
    pub const fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    /// Returns the Region size in bytes.
    pub const fn region_size_bytes(&self) -> u64 {
        self.region_size_bytes
    }

    /// Returns the fixed number of physical L2 index slots.
    pub const fn index_slots(&self) -> usize {
        self.index_slots
    }

    /// Validates the static physical geometry before opening cache files.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorKind::InvalidInput`] when the capacity, Region
    /// geometry, or index size is not representable.
    pub fn validate(&self) -> Result<()> {
        public_result(ErrorOperation::ValidateConfig, self.geometry().map(|_| ()))
    }

    /// Maximum cache-owned logical disk bytes after a successful open.
    ///
    /// The bound includes the fixed data and state files plus both the current
    /// clean image and the temporary image used for atomic warm publication.
    /// Filesystem metadata and block-allocation granularity are outside it.
    ///
    /// # Errors
    ///
    /// Returns [`crate::ErrorKind::InvalidInput`] when the static geometry is
    /// invalid or its disk bound overflows.
    pub fn peak_disk_bytes(&self) -> Result<u64> {
        public_result(ErrorOperation::PeakDiskBytes, self.peak_disk_bytes_inner())
    }

    pub(crate) fn peak_disk_bytes_inner(&self) -> io::Result<u64> {
        let geometry = self.geometry()?;
        let index_slots = u64::try_from(self.index_slots)
            .map_err(|_| invalid_config("index capacity does not fit u64"))?;
        let index_len = recovery_image_index_len(index_slots)
            .ok_or_else(|| invalid_config("index image length overflow"))?;
        let partition_count = u64::try_from(
            canonical_index_partition_ranges(self.index_slots)
                .map_err(index_layout_error)?
                .len(),
        )
        .map_err(|_| invalid_config("index partition count does not fit u64"))?;
        let region_pages =
            u64::from(geometry.region_count).div_ceil(REGION_METADATA_REGIONS_PER_PAGE as u64);
        let partition_pages = partition_count.div_ceil(REGION_METADATA_PARTITIONS_PER_PAGE as u64);
        let metadata_len = 1_u64
            .checked_add(region_pages)
            .and_then(|pages| pages.checked_add(partition_pages))
            .and_then(|pages| pages.checked_mul(REGION_METADATA_PAGE_SIZE as u64))
            .ok_or_else(|| invalid_config("Region metadata length overflow"))?;
        let image_len = RECOVERY_IMAGE_INDEX_OFFSET
            .checked_add(index_len)
            .and_then(|bytes| bytes.checked_add(metadata_len))
            .ok_or_else(|| invalid_config("recovery image length overflow"))?;
        geometry
            .data_file_len
            .checked_add(STATE_FILE_SIZE as u64)
            .and_then(|bytes| bytes.checked_add(image_len.checked_mul(2)?))
            .ok_or_else(|| invalid_config("peak disk usage overflow"))
    }

    pub(crate) fn geometry(&self) -> io::Result<DataGeometry> {
        if self.region_size_bytes == 0
            || self.region_size_bytes > MAX_PACKED_REGION_SIZE
            || !self.region_size_bytes.is_multiple_of(4096)
            || self.capacity_bytes == 0
            || !self.capacity_bytes.is_multiple_of(self.region_size_bytes)
        {
            return Err(invalid_config(
                "capacity must be a non-zero multiple of an aligned representable Region size",
            ));
        }
        let region_count = u32::try_from(self.capacity_bytes / self.region_size_bytes)
            .ok()
            .filter(|count| *count <= MAX_PACKED_REGION_COUNT)
            .ok_or_else(|| invalid_config("cache Region count is not representable"))?;
        if region_count < 2 {
            return Err(invalid_config("cache requires at least two Regions"));
        }
        if self.index_slots < MIN_INDEX_SLOTS {
            return Err(invalid_config("index slots must be at least 8"));
        }
        validate_index_slot_count(self.index_slots).map_err(index_layout_error)?;
        let data_file_len = DataGeometry::expected_file_len(self.region_size_bytes, region_count)
            .ok_or_else(|| invalid_config("cache data length overflow"))?;
        let geometry = DataGeometry {
            data_file_len,
            region_size: self.region_size_bytes,
            region_count,
        };
        if !geometry.is_valid() {
            return Err(invalid_config("cache data geometry is not representable"));
        }
        Ok(geometry)
    }

    pub(crate) fn fingerprint(&self, geometry: DataGeometry) -> u64 {
        self.fingerprint_with_hash_algorithm(geometry, u64::from(KEY_HASH_ALGORITHM_XXH3_64))
    }

    fn fingerprint_with_hash_algorithm(
        &self,
        geometry: DataGeometry,
        hash_algorithm_id: u64,
    ) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325_u64;
        for value in [
            STATIC_FINGERPRINT_SCHEMA,
            geometry.data_file_len,
            geometry.region_size,
            u64::from(geometry.region_count),
            self.index_slots as u64,
            KEY_HASH_SEED,
            hash_algorithm_id,
        ] {
            for byte in value.to_le_bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100_0000_01b3);
            }
        }
        hash.max(1)
    }
}

fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn index_layout_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidInput, error),
    }
}

fn public_result<T>(operation: ErrorOperation, result: io::Result<T>) -> Result<T> {
    result.map_err(|error| Error::from_io(operation, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::{DataSuperblock, PersistentId};

    #[test]
    fn static_fingerprint_binds_the_hash_algorithm() {
        let config = StaticConfig::new(5 * DEFAULT_REGION_SIZE);
        let geometry = config.geometry().unwrap();
        let algorithm = u64::from(KEY_HASH_ALGORITHM_XXH3_64);
        assert_eq!(
            config.fingerprint(geometry),
            config.fingerprint_with_hash_algorithm(geometry, algorithm)
        );
        assert_ne!(
            config.fingerprint(geometry),
            config.fingerprint_with_hash_algorithm(geometry, algorithm + 1)
        );
    }

    #[test]
    fn minimum_static_region_geometry_is_encodable() {
        let config = StaticConfig::new(5 * 4096).with_region_size_bytes(4096);
        config.validate().unwrap();
        let geometry = config.geometry().unwrap();
        let data = DataSuperblock {
            generation: 1,
            cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
            data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
            geometry,
            hash_seed: KEY_HASH_SEED,
            config_fingerprint: config.fingerprint(geometry),
        };
        assert!(data.encode().is_ok());
    }

    #[test]
    fn four_tib_default_index_matches_sixteen_kib_entries() {
        let config = StaticConfig::new(4_u64 << 40);

        assert_eq!(config.index_slots(), 536_870_912);
        config.validate().unwrap();
    }

    #[test]
    fn maximum_capacity_default_index_remains_proportional_and_representable() {
        let capacity = MAX_PACKED_REGION_SIZE * u64::from(MAX_PACKED_REGION_COUNT);
        let config = StaticConfig::new(capacity);

        assert_eq!(capacity, 32_u64 << 40);
        assert_eq!(config.index_slots(), 1_usize << 32);
        config.validate().unwrap();
        assert!(config.peak_disk_bytes().unwrap() > capacity);
    }

    #[test]
    fn unrepresentable_explicit_index_is_rejected_without_clamping() {
        let config = StaticConfig::new(2 * DEFAULT_REGION_SIZE).with_expected_entries(usize::MAX);

        assert_eq!(config.index_slots(), usize::MAX);
        assert!(config.validate().is_err());
    }

    #[test]
    fn index_layout_allocation_error_remains_out_of_memory() {
        let error = index_layout_error(IndexStorageError::Io(io::Error::new(
            io::ErrorKind::OutOfMemory,
            "injected allocation failure",
        )));

        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    }
}
