// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

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
pub mod error;
mod eviction;
mod format;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod hashing;
mod index;
mod index_storage;
mod io_backend;
mod io_engine;
mod memory;
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
mod runtime_config;
mod snapshot;

pub use cache::{Cache, CacheBuilder, CacheTier, StaticConfig, Value};
pub use error::{Error, ErrorKind, ErrorOperation, Result};
pub use runtime_config::{IoEngine, IoMode, L1EvictionPolicy, RuntimeConfig};
pub use snapshot::{
    CacheHealth, CacheIndexSnapshot, CacheIoDirectionSnapshot, CacheIoPathSnapshot,
    CacheIoSnapshot, CacheL1Snapshot, CacheReclaimSnapshot, CacheSnapshot, DetailedCacheSnapshot,
    RegionSnapshot, StartupMode,
};
