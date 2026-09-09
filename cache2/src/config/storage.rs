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

use crate::error::Error;
use crate::error::ErrorOperation;
use crate::error::Result;
use crate::index::MAX_PACKED_REGION_COUNT;
use crate::index::MAX_PACKED_REGION_SIZE;
use crate::index_storage::IndexStorageError;
use crate::index_storage::validated_index_partition_ranges;
use crate::recovery::DataGeometry;
use crate::recovery::KEY_HASH_ALGORITHM_XXH3_64;
use crate::recovery::RECOVERY_IMAGE_INDEX_OFFSET;
use crate::recovery::STATE_FILE_SIZE;
use crate::recovery::recovery_image_index_len;
use crate::region_metadata::REGION_METADATA_PAGE_SIZE;
use crate::region_metadata::REGION_METADATA_PARTITIONS_PER_PAGE;
use crate::region_metadata::REGION_METADATA_REGIONS_PER_PAGE;

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_EXPECTED_ENTRY_BYTES: u64 = 16 * 1024;
pub(crate) const KEY_HASH_SEED: u64 = 0x6a09_e667_f3bc_c909;
const MIN_INDEX_SLOTS: usize = 8;
const STATIC_FINGERPRINT_SCHEMA: u64 = 3;

/// Inputs for a persistent L2 layout, checked by [`Self::build`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageOptions {
    /// Total Region capacity, excluding file headers, state, and recovery images.
    /// Must contain at least two Regions and be an exact multiple of their size.
    pub capacity_bytes: u64,
    /// Bytes per Region, in 4 KiB multiples through 32 MiB. Each encoded record
    /// must fit in one Region. Defaults to 32 MiB.
    pub region_size_bytes: u64,
    /// Expected simultaneously live keys. The index reserves two slots per key,
    /// with a minimum of eight slots. `None` assumes 16 KiB per live entry using
    /// the final capacity supplied to [`Self::build`].
    pub expected_entries: Option<usize>,
}

impl StorageOptions {
    /// Selects a Region capacity with default Region and index sizing.
    pub const fn new(capacity_bytes: u64) -> Self {
        Self {
            capacity_bytes,
            region_size_bytes: DEFAULT_REGION_SIZE,
            expected_entries: None,
        }
    }

    /// Checks the inputs and computes an immutable layout without opening files.
    /// Use [`StorageLayout::peak_disk_bytes`] to compare a candidate with a disk
    /// budget, then pass the chosen layout to [`super::CacheConfig::new`].
    ///
    /// # Errors
    ///
    /// Returns [`ErrorOperation::BuildStorage`] if the geometry or index cannot
    /// be represented, disk accounting overflows, or bounded layout allocation
    /// fails.
    pub fn build(self) -> Result<StorageLayout> {
        let build = || {
            let entries = match self.expected_entries {
                Some(entries) => entries,
                None => usize::try_from(self.capacity_bytes / DEFAULT_EXPECTED_ENTRY_BYTES)
                    .map_err(|_| invalid_config("expected entries do not fit usize"))?,
            };
            let index_slots = entries
                .checked_mul(2)
                .ok_or_else(|| invalid_config("index slot count overflow"))?
                .max(MIN_INDEX_SLOTS);
            StorageLayout::new(self.capacity_bytes, self.region_size_bytes, index_slots)
        };
        build().map_err(|error| Error::from_io(ErrorOperation::BuildStorage, error))
    }
}

/// Immutable persistent geometry with a checked logical disk bound.
///
/// Created by [`StorageOptions::build`]. Changing the geometry or index size
/// changes the disk identity, so an incompatible recovery image opens empty.
/// Layout construction neither reserves disk space nor requires a Tokio runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageLayout {
    pub(super) geometry: DataGeometry,
    pub(super) index_slots: usize,
    fingerprint: u64,
    peak_disk_bytes: u64,
}

impl StorageLayout {
    pub(super) fn new(
        capacity_bytes: u64,
        region_size_bytes: u64,
        index_slots: usize,
    ) -> io::Result<Self> {
        if region_size_bytes == 0
            || region_size_bytes > MAX_PACKED_REGION_SIZE
            || !region_size_bytes.is_multiple_of(4096)
            || capacity_bytes == 0
            || !capacity_bytes.is_multiple_of(region_size_bytes)
        {
            return Err(invalid_config(
                "capacity must be a non-zero multiple of an aligned representable Region size",
            ));
        }
        let region_count = u32::try_from(capacity_bytes / region_size_bytes)
            .ok()
            .filter(|count| *count <= MAX_PACKED_REGION_COUNT)
            .ok_or_else(|| invalid_config("cache Region count is not representable"))?;
        if region_count < 2 {
            return Err(invalid_config("cache requires at least two Regions"));
        }
        if index_slots < MIN_INDEX_SLOTS {
            return Err(invalid_config("index slots must be at least 8"));
        }
        let data_file_len = DataGeometry::expected_file_len(region_size_bytes, region_count)
            .ok_or_else(|| invalid_config("cache data length overflow"))?;
        let geometry = DataGeometry {
            data_file_len,
            region_size: region_size_bytes,
            region_count,
        };
        Ok(Self {
            geometry,
            index_slots,
            fingerprint: fingerprint(geometry, index_slots, u64::from(KEY_HASH_ALGORITHM_XXH3_64)),
            peak_disk_bytes: disk_peak_bytes(geometry, index_slots)?,
        })
    }

    /// Returns the total Region capacity, excluding file headers and sidecars.
    pub const fn capacity_bytes(&self) -> u64 {
        self.geometry.region_size * self.geometry.region_count as u64
    }

    /// Returns the size of each Region in bytes.
    pub const fn region_size_bytes(&self) -> u64 {
        self.geometry.region_size
    }

    /// Returns the number of Regions available to append shards and reclaim.
    pub const fn region_count(&self) -> u32 {
        self.geometry.region_count
    }

    /// Returns the number of physical slots in the fixed L2 index.
    pub const fn index_slots(&self) -> usize {
        self.index_slots
    }

    /// Returns the maximum cache-owned logical disk usage: data and state files,
    /// plus both the current and temporary recovery images used by warm close.
    /// Filesystem metadata and block-allocation granularity are outside this bound.
    pub const fn peak_disk_bytes(&self) -> u64 {
        self.peak_disk_bytes
    }

    pub(crate) const fn geometry(&self) -> DataGeometry {
        self.geometry
    }
    pub(crate) const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }
}

fn disk_peak_bytes(geometry: DataGeometry, index_slots: usize) -> io::Result<u64> {
    let image_slots = u64::try_from(index_slots)
        .map_err(|_| invalid_config("index capacity does not fit u64"))?;
    let index_len = recovery_image_index_len(image_slots)
        .ok_or_else(|| invalid_config("index image length overflow"))?;
    let partition_count = u64::try_from(
        validated_index_partition_ranges(index_slots)
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

fn fingerprint(geometry: DataGeometry, index_slots: usize, hash_algorithm_id: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for value in [
        STATIC_FINGERPRINT_SCHEMA,
        geometry.data_file_len,
        geometry.region_size,
        u64::from(geometry.region_count),
        index_slots as u64,
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
fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn index_layout_error(error: IndexStorageError) -> io::Error {
    match error {
        IndexStorageError::Io(error) => error,
        error => io::Error::new(io::ErrorKind::InvalidInput, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::DataSuperblock;
    use crate::recovery::PersistentId;

    #[test]
    fn constructed_layouts_encode_at_format_boundaries() {
        for (region_size, region_count) in
            [(4096, 2), (MAX_PACKED_REGION_SIZE, MAX_PACKED_REGION_COUNT)]
        {
            let storage = StorageOptions {
                capacity_bytes: region_size * u64::from(region_count),
                region_size_bytes: region_size,
                expected_entries: None,
            }
            .build()
            .unwrap();
            let data = DataSuperblock {
                generation: 1,
                cache_uuid: PersistentId::from_bytes([1; 16]).unwrap(),
                data_identity: PersistentId::from_bytes([2; 16]).unwrap(),
                geometry: storage.geometry,
                hash_seed: KEY_HASH_SEED,
                config_fingerprint: storage.fingerprint,
            };
            assert!(data.encode().is_ok());
            assert_ne!(
                storage.fingerprint,
                fingerprint(
                    storage.geometry,
                    storage.index_slots,
                    u64::from(KEY_HASH_ALGORITHM_XXH3_64) + 1
                )
            );
        }
    }
}
