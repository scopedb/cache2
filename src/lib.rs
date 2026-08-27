//! Performance-first bounded RAM + Region SSD hybrid cache.
//!
//! Ordinary mutations are process-visible cache updates, not durable storage
//! writes. An unclean restart always opens empty without scanning the data
//! extent. Only [`Cache::close_warm`] publishes a recoverable image.

#[cfg(feature = "benchmarking")]
#[doc(hidden)]
pub mod benchmarking;
mod cache;
mod checksum;
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

pub use cache::{Cache, CacheConfig, CacheTier, Result, StaticConfig, Value};
pub use runtime_config::{IoEngine, IoMode, RuntimeConfig};
pub use snapshot::{
    CacheHealth, CacheIndexSnapshot, CacheIoSnapshot, CacheL1Snapshot, CacheSnapshot,
    DetailedCacheSnapshot, RegionSnapshot, StartupMode,
};
