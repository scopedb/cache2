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

//! Shared storage layouts and runtime configuration helpers for end-to-end integration tests.

use cache2::CacheConfig;
use cache2::IoEngineOptions;
use cache2::PosixIoOptions;
use cache2::RuntimeOptions;
use cache2::StorageLayout;
use cache2::StorageOptions;

pub fn test_storage() -> StorageLayout {
    let mut options = StorageOptions::new(3 * 512 * 1024);
    options.region_size_bytes = 512 * 1024;
    options.expected_entries = Some(3277);
    options.build().unwrap()
}

pub fn test_runtime_options(workers: usize, append_shards: u32) -> RuntimeOptions {
    let mut io = PosixIoOptions::default();
    io.read_workers = workers;
    io.write_workers = workers;
    io.reclaim_workers = 1;
    let mut options = RuntimeOptions::default();
    options.io_engine = IoEngineOptions::Posix(io);
    options.append_shards = append_shards;
    options.l1_capacity_bytes = 4 * 1024 * 1024;
    options.managed_memory_limit_bytes = 32 * 1024 * 1024;
    options.write_flush_threshold_bytes = 256 * 1024;
    options.stats.activity_counters = true;
    options
}

pub fn test_config(workers: usize) -> CacheConfig {
    test_config_with_storage(workers, test_storage())
}

pub fn test_config_with_storage(workers: usize, storage: StorageLayout) -> CacheConfig {
    test_config_with_storage_and_shards(workers, storage, 2)
}

pub fn test_config_with_storage_and_shards(
    workers: usize,
    storage: StorageLayout,
    append_shards: u32,
) -> CacheConfig {
    CacheConfig::new(storage, test_runtime_options(workers, append_shards)).unwrap()
}
