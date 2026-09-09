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

use crate::config::runtime::RuntimeOptions;
use crate::region::recovery::DataGeometry;

pub mod runtime;
pub mod storage;

/// Complete, immutable configuration for opening a [`Cache`](crate::Cache).
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
}

/// Immutable persistent geometry with a checked logical disk bound.
///
/// Created by [`StorageOptions::build`](crate::StorageOptions::build). Changing the geometry or
/// index size changes the disk identity, so an incompatible recovery image opens empty.
/// Layout construction neither reserves disk space nor requires a Tokio runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageLayout {
    geometry: DataGeometry,
    index_slots: usize,
    fingerprint: u64,
    peak_disk_bytes: u64,
}

impl StorageLayout {
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
}

pub const fn storage_geometry(storage: &StorageLayout) -> DataGeometry {
    storage.geometry
}

pub const fn storage_fingerprint(storage: &StorageLayout) -> u64 {
    storage.fingerprint
}

pub const fn l1_entry_capacity(config: &CacheConfig) -> usize {
    config.l1_entry_capacity
}

pub const fn reserved_memory_bytes(config: &CacheConfig) -> usize {
    config.reserved_memory_bytes
}
