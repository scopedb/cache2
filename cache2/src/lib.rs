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
//! See [`Error`] for failure classifications and handling policy.

#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod benchmarking;

mod error;
pub use self::error::Error;
pub use self::error::ErrorKind;
pub use self::error::ErrorOperation;

mod cache;
pub use self::cache::Cache;
pub use self::cache::CacheTier;
pub use self::cache::Value;

mod config;
pub use self::config::CacheConfig;
pub use self::config::IoEngineConfig;
pub use self::config::IoMode;
pub use self::config::IoUringConfig;
pub use self::config::IoUringPoolConfig;
pub use self::config::IoUringSqPollConfig;
pub use self::config::L1EvictionPolicy;
pub use self::config::PosixIoConfig;
pub use self::config::ReadAdmission;
pub use self::config::RuntimeOptions;
pub use self::config::StorageLayout;
pub use self::config::StorageOptions;

mod snapshot;
pub use self::snapshot::CacheHealth;
pub use self::snapshot::CacheIndexSnapshot;
pub use self::snapshot::CacheIoDirectionSnapshot;
pub use self::snapshot::CacheIoPathSnapshot;
pub use self::snapshot::CacheIoSnapshot;
pub use self::snapshot::CacheL1Snapshot;
pub use self::snapshot::CacheReclaimSnapshot;
pub use self::snapshot::CacheSnapshot;
pub use self::snapshot::DetailedCacheSnapshot;
pub use self::snapshot::RegionSnapshot;
pub use self::snapshot::StartupMode;

mod checksum;
mod hashing;
mod io;
mod memory;
mod region;
mod resources;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod property_tests;
