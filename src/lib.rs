//! Performance-first bounded RAM + Region SSD hybrid cache.
//!
//! Ordinary mutations are process-visible cache updates, not durable storage
//! writes. An unclean restart always opens empty without scanning the data
//! extent. Only [`HybridCache::close_warm`] publishes a recoverable image.

mod cache;
mod checksum;
mod eviction;
mod expiry;
mod format;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod index;
mod index_storage;
mod io_backend;
mod io_engine;
mod memory;
mod memory_arena;
#[cfg_attr(not(feature = "experimental-l1-directory"), allow(dead_code))]
mod memory_directory;
mod record_codec;
mod recovery;
mod region;
mod region_appender;
mod region_index;
mod region_layout;
mod region_manager;
mod region_metadata;
mod region_read;
mod region_reader;
mod region_runtime;
mod region_staging;
mod region_store;
mod resources;

pub use cache::{
    CacheHealth, CacheIoSnapshot, CacheQueueSnapshot, CacheSnapshot, CacheTier,
    DetailedCacheSnapshot, HybridCache, HybridCacheConfig, IoEngine, IoMode, PutReceipt,
    RegionSetSnapshot, Result, RuntimeConfig, StartupMode, StaticConfig, Value,
};
pub use eviction::EvictionPolicy;
pub use region_layout::{RegionSetAllocation, RegionSetConfig, RegionSetId};
pub use resources::{BackpressurePolicy, ReadBufferPolicy};
