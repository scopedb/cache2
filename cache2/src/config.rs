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

//! Configuration construction, independent of file paths and runtime handles.

use std::io;

use crate::error::Error;
use crate::error::ErrorOperation;
use crate::error::Result;
use crate::memory::MemoryStore;
#[cfg(test)]
use crate::recovery::DataGeometry;

mod runtime;
mod storage;

pub use runtime::IoEngine;
pub use runtime::IoMode;
pub(crate) use runtime::IoPoolTopology;
pub use runtime::IoUringConfig;
pub use runtime::IoUringPoolConfig;
pub use runtime::IoUringSqPollConfig;
pub use runtime::L1EvictionPolicy;
#[cfg(test)]
pub(crate) use runtime::MAX_WRITE_FLUSH_THRESHOLD_BYTES;
pub use runtime::PosixIoConfig;
pub use runtime::ReadAdmission;
pub use runtime::RuntimeOptions;
pub(crate) use storage::KEY_HASH_SEED;
pub use storage::StorageLayout;
pub use storage::StorageOptions;

/// Complete, immutable configuration for opening a [`crate::Cache`].
///
/// Construction checks runtime settings against the storage layout and managed
/// memory limit. It performs bounded calculations without opening files, starting
/// workers, or requiring Tokio. Resource queries reuse the computed results.
/// Clone a configuration to reuse it across paths or successive opens; each open
/// acquires its own resources and can still fail on I/O or allocation.
#[derive(Clone, Debug)]
pub struct CacheConfig {
    storage: StorageLayout,
    runtime: RuntimeOptions,
    l1_entry_capacity: usize,
    reserved_memory_bytes: usize,
    minimum_memory_bytes: usize,
}

impl CacheConfig {
    /// Checks the complete combination and resolves dependent runtime defaults.
    ///
    /// ```no_run
    /// # async fn example() -> cache2::Result<()> {
    /// use cache2::Cache;
    /// use cache2::CacheConfig;
    /// use cache2::RuntimeOptions;
    /// use cache2::StorageOptions;
    /// let storage = StorageOptions::new(1024 * 1024 * 1024).build()?;
    /// let config = CacheConfig::new(storage, RuntimeOptions::default())?;
    /// let disk_peak = config.storage().peak_disk_bytes();
    /// let memory_floor = config.minimum_memory_bytes();
    /// let cache = Cache::open("cache.data", config).await?;
    /// # cache.close_fast().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ErrorOperation::BuildConfig`] for incompatible Region/shard
    /// counts, invalid runtime settings, unavailable build/platform features,
    /// or insufficient managed memory. Device capabilities are checked at open.
    pub fn new(storage: StorageLayout, mut runtime: RuntimeOptions) -> Result<Self> {
        let build = || -> io::Result<Self> {
            let geometry = storage.geometry;
            let index_slots = storage.index_slots;
            runtime.resolve()?;
            if geometry.region_count <= runtime.append_shards {
                return Err(invalid_config(
                    "append shards require valid geometry with one Active Region each plus one spare Region",
                ));
            }
            let l1_entry_capacity = runtime.l1_entry_capacity(geometry, index_slots)?;
            let l1_metadata_bytes = MemoryStore::allocation_bytes(
                runtime.l1_capacity_bytes,
                l1_entry_capacity,
                runtime.l1_shards,
                runtime.l1_eviction_policy,
            )?;
            let fixed_bytes = crate::region::core::runtime_fixed_memory_bytes(
                index_slots,
                geometry.region_count,
            )?
            .checked_add(l1_metadata_bytes)
            .ok_or_else(|| invalid_config("fixed memory requirements overflow"))?;
            let (reserved_memory_bytes, minimum_memory_bytes) =
                runtime.memory_requirements(geometry, fixed_bytes)?;
            if minimum_memory_bytes > runtime.managed_memory_limit_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "managed memory limit cannot hold the cache memory requirements: requires {minimum_memory_bytes} bytes, configured {} bytes",
                        runtime.managed_memory_limit_bytes
                    ),
                ));
            }
            Ok(Self {
                storage,
                runtime,
                l1_entry_capacity,
                reserved_memory_bytes,
                minimum_memory_bytes,
            })
        };
        build().map_err(|error| Error::from_io(ErrorOperation::BuildConfig, error))
    }

    /// Returns the immutable persistent layout and its disk bound.
    pub const fn storage(&self) -> &StorageLayout {
        &self.storage
    }

    /// Returns the selected runtime options with dependent defaults resolved.
    /// Clone these inputs to construct a new configuration with different tuning.
    pub const fn runtime(&self) -> &RuntimeOptions {
        &self.runtime
    }

    /// Returns the minimum managed-memory budget required by this configuration.
    /// Includes metadata, L1 capacity, workers, queues, append/reclaim buffers,
    /// and one Region-sized read allowance. Concurrent reads and retained values
    /// need additional headroom. This is not an RSS bound.
    pub const fn minimum_memory_bytes(&self) -> usize {
        self.minimum_memory_bytes
    }

    pub(crate) const fn l1_entry_capacity(&self) -> usize {
        self.l1_entry_capacity
    }
    pub(crate) const fn reserved_memory_bytes(&self) -> usize {
        self.reserved_memory_bytes
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        geometry: DataGeometry,
        index_slots: usize,
        runtime: RuntimeOptions,
    ) -> Self {
        let storage = StorageLayout::new(
            geometry.region_size * u64::from(geometry.region_count),
            geometry.region_size,
            index_slots,
        )
        .expect("test storage layout must be valid");
        Self::new(storage, runtime).expect("test configuration must be valid")
    }
}

fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
