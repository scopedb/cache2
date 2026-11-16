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

//! A bounded RAM and SSD cache for large file chunks.
//!
//! C² (`cache2`) keeps frequently reused values in memory (L1) and batches writes
//! to a file-backed cache (L2). Cache-owned memory, queues, and I/O concurrency
//! have explicit bounds. Use it as disposable acceleration in front of an
//! authoritative data source: entries may be evicted, reads may miss under
//! pressure, and a validated hit may still be stale.
//!
//! # Getting started
//!
//! Add the cache and a Tokio runtime to your application:
//!
//! ```toml
//! [dependencies]
//! cache2 = "0.6"
//! tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
//! ```
//!
//! Build a storage layout, combine it with runtime options, and open a cache
//! file. This example reserves 1 GiB of Region capacity plus metadata and
//! recovery space. The parent directory must exist, and the filesystem must
//! support physical file preallocation.
//!
//! ```no_run
//! use cache2::Cache;
//! use cache2::CacheConfig;
//! use cache2::Error;
//! use cache2::ErrorKind;
//! use cache2::RuntimeOptions;
//! use cache2::StorageOptions;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Error> {
//!     let storage = StorageOptions::new(1024 * 1024 * 1024).build()?;
//!     let config = CacheConfig::new(storage, RuntimeOptions::default())?;
//!     let cache = Cache::open("cache.data", config).await?;
//!
//!     // Fill with bytes already obtained from the authoritative source.
//!     match cache.put(b"chunk:42", b"cached bytes") {
//!         Ok(_) => {}
//!         Err(error) if error.kind() == ErrorKind::Overloaded => {
//!             // Skipping a cache fill must not fail the application request.
//!         }
//!         Err(error) => return Err(error),
//!     }
//!
//!     match cache.get(b"chunk:42").await? {
//!         Some(value) => println!("cache hit: {} bytes", value.len()),
//!         None => println!("cache miss: read from the authoritative source"),
//!     }
//!
//!     cache.close_warm().await
//! }
//! ```
//!
//! [`Cache::open`] uses the current Tokio runtime, which must have time enabled
//! and outlive the cache. Use [`Cache::open_with_handle`] to select another
//! runtime. Share an open cache through [`std::sync::Arc`].
//!
//! # Integrating with the data path
//!
//! Keys and values are bytes. Keys may be at most 4 KiB, and each encoded
//! key/value record must fit in one Region. When freshness matters, use keys
//! that identify the version of the source data; a cache hit is not a freshness
//! guarantee.
//!
//! | Task | API and behavior |
//! | --- | --- |
//! | Look up a value | [`Cache::get`] checks L1, then performs at most one validated L2 record read. |
//! | Fill both tiers | [`Cache::put`] attempts L1 admission and stages an L2 write. Success means bounded in-memory admission. |
//! | Fill L2 directly | [`Cache::put_l2`] bypasses L1 admission. The L2 value becomes visible after its Region write publishes. |
//! | Remove a mapping | [`Cache::delete`] removes the L2 mapping and cleans L1 on a best-effort basis. |
//! | Wait for publication | [`Cache::drain`] waits for accepted writes and L2 index publication. It does not publish a restart image. |
//!
//! A miss and [`ErrorKind::Overloaded`] are expected outcomes of bounded
//! resources. Fall through to the authoritative source or use a bounded retry.
//! The default [`ReadAdmission::Immediate`] turns read-engine or buffer pressure
//! into a miss; [`ReadAdmission::Wait`] can instead return explicit overload.
//! See [`Error`] for the classification table, handling policy, and diagnostic
//! fields.
//!
//! # Sizing and configuration
//!
//! [`StorageOptions`] selects persistent capacity, Region size, and index size.
//! Its [`build`](StorageOptions::build) method produces a [`StorageLayout`].
//! [`RuntimeOptions`] selects L1 capacity and eviction, I/O concurrency, read
//! admission, statistics, and background recovery policy. Start with `new` or
//! `Default` and assign fields; option structs are non-exhaustive.
//!
//! [`CacheConfig::new`] checks the complete combination without opening files
//! or requiring Tokio. Inspect [`StorageLayout::peak_disk_bytes`] and
//! [`CacheConfig::minimum_memory_bytes`] before opening. The minimum memory is
//! a floor: leave headroom for concurrent reads and values retained by callers.
//! The managed-memory limit does not cover total process RSS or the kernel
//! page cache. Each open acquires its own files and resources, even when using
//! a cloned configuration.
//!
//! # Shutdown and restart
//!
//! Cache mutations do not provide crash durability. [`Cache::close_warm`]
//! publishes a clean recovery image; [`Cache::close_fast`], dropping the cache,
//! or an unclean exit makes the next open start empty. Warm recovery can also
//! fall back to a cold start if the image is rejected or the storage layout
//! changes. Inspect [`Cache::startup_mode`] after opening.
//!
//! Prefer explicit async close: dropping an open cache can block the dropping
//! thread. Both close methods work through shared handles. The first close
//! call immediately stops admission through every handle, and awaiting it
//! waits for shutdown to finish. Retained handles and [`Value`] objects can
//! keep memory allocated until they are dropped.
//!
//! # Monitoring
//!
//! - [`Cache::snapshot`] provides health and resource gauges; enable
//!   [`StatsOptions::activity_counters`] for cumulative activity and I/O counts.
//! - [`Cache::stats_snapshot`] adds request outcomes and latency histograms enabled through
//!   [`StatsOptions`], without scanning cache metadata.
//! - [`Cache::detailed_snapshot`] scans metadata for periodic diagnostics. Keep it off the request
//!   path.
//!
//! Applications own metric export and the global `log` logger. Inspect
//! [`CacheHealth`] to distinguish normal operation, background I/O recovery,
//! and terminal failure.
//!
//! # Platforms and optional features
//!
//! C² supports 64-bit Linux and macOS with buffered POSIX I/O by default.
//! [`IoMode::Direct`] is Linux-only. The `io-uring` feature enables the
//! experimental [`IoEngineOptions::IoUring`] backend on supported Linux
//! architectures; enabling the feature alone does not select that backend.
//! Configure it through [`IoUringOptions`].

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
pub use self::config::StorageLayout;
pub use self::config::runtime::FillControlOptions;
pub use self::config::runtime::FillLimits;
pub use self::config::runtime::IoEngineOptions;
pub use self::config::runtime::IoMode;
pub use self::config::runtime::IoUringOptions;
pub use self::config::runtime::IoUringPoolOptions;
pub use self::config::runtime::IoUringSqPollOptions;
pub use self::config::runtime::L1EvictionPolicy;
pub use self::config::runtime::PosixIoOptions;
pub use self::config::runtime::ReadAdmission;
pub use self::config::runtime::RuntimeOptions;
pub use self::config::storage::StorageOptions;

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
pub use self::snapshot::FillControlSnapshot;
pub use self::snapshot::FillPressure;
pub use self::snapshot::RegionSnapshot;
pub use self::snapshot::StartupMode;

mod stats;
pub use self::stats::CacheStatsSnapshot;
pub use self::stats::IoLatencySnapshot;
pub use self::stats::IoOutcome;
pub use self::stats::IoRole;
pub use self::stats::LATENCY_BUCKET_UPPER_BOUNDS_NS;
pub use self::stats::LatencyMode;
pub use self::stats::LatencySnapshot;
pub use self::stats::RequestLatencyScope;
pub use self::stats::RequestOperation;
pub use self::stats::RequestOutcome;
pub use self::stats::RequestStatsSnapshot;
pub use self::stats::StatsOptions;

mod checksum;
mod codec;
mod hashing;
mod io;
mod managed_memory;
mod memory;
mod region;

#[cfg(test)]
mod fixtures;
#[cfg(test)]
mod property_tests;
