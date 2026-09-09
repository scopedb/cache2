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

//! Bounded RAM + Region SSD cache.
//!
//! Mutations provide process-local visibility. An unclean restart opens empty;
//! [`Cache::close_warm`] publishes a recoverable image.
//! See the [`error`] module for failure classifications and handling policy.

#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod benchmarking;
mod cache;
mod checksum;
mod config;
pub mod error;
mod eviction;
mod format;
#[cfg(test)]
mod format_fixtures;
mod hashing;
mod index;
mod index_storage;
mod io_backend;
mod io_engine;
mod memory;
#[cfg(test)]
mod property_tests;
mod record_codec;
mod recovery;
mod region;
mod region_appender;
mod region_index;
mod region_manager;
mod region_metadata;
mod region_reader;
mod region_runtime;
mod region_staging;
mod region_store;
mod resources;
mod snapshot;

pub use cache::Cache;
pub use cache::CacheTier;
pub use cache::Value;
pub use config::CacheConfig;
pub use config::IoEngine;
pub use config::IoMode;
pub use config::IoUringConfig;
pub use config::IoUringPoolConfig;
pub use config::IoUringSqPollConfig;
pub use config::L1EvictionPolicy;
pub use config::PosixIoConfig;
pub use config::ReadAdmission;
pub use config::RuntimeOptions;
pub use config::StorageLayout;
pub use config::StorageOptions;
pub use error::Error;
pub use error::ErrorKind;
pub use error::ErrorOperation;
pub use error::Result;
pub use snapshot::CacheHealth;
pub use snapshot::CacheIndexSnapshot;
pub use snapshot::CacheIoDirectionSnapshot;
pub use snapshot::CacheIoPathSnapshot;
pub use snapshot::CacheIoSnapshot;
pub use snapshot::CacheL1Snapshot;
pub use snapshot::CacheReclaimSnapshot;
pub use snapshot::CacheSnapshot;
pub use snapshot::DetailedCacheSnapshot;
pub use snapshot::RegionSnapshot;
pub use snapshot::StartupMode;
