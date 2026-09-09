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

use std::io;
use std::mem::size_of;
use std::time::Duration;

use crate::config::CacheConfig;
use crate::config::StorageLayout;
use crate::error::ErrorOperation;
use crate::error::Result;
use crate::error::from_io;
use crate::io::engine::IO_QUEUE_ENTRY_RESERVATION_BYTES;
use crate::io::engine::MAX_IO_REQUESTS_PER_ENGINE;
use crate::io::engine::io_uring_extra_memory_bytes;
use crate::memory::MemoryStore;
use crate::region::ActivityMetrics;
use crate::region::RegionStaging;
use crate::region::recovery::DataGeometry;
use crate::region::runtime_fixed_memory_bytes;
use crate::resources::BUFFER_ALIGNMENT;
use crate::resources::CACHE_THREAD_STACK_BYTES;
use crate::resources::MAX_CONFIG_COUNT;

const DEFAULT_L1_SHARDS: usize = 32;
const MAX_APPEND_SHARDS: u32 = 256;
pub const MAX_WRITE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;
const MAX_READ_IO_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_L1_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_APPEND_SHARDS: u32 = 4;
const DEFAULT_POSIX_IO_WORKERS: usize = 4;
const DEFAULT_IO_URING_MAX_IN_FLIGHT: usize = 64;
const DEFAULT_RECLAIM_IO_CONCURRENCY: usize = 1;

/// Worker topology for POSIX positioned I/O.
///
/// Every admitted request occupies one worker until its blocking system call
/// completes, so worker counts are also the per-pool in-flight limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PosixIoConfig {
    read_workers: usize,
    write_workers: usize,
    reclaim_workers: usize,
}

impl PosixIoConfig {
    /// Creates a POSIX topology with independent read, write, and reclaim
    /// worker pools.
    pub const fn new(read_workers: usize, write_workers: usize, reclaim_workers: usize) -> Self {
        Self {
            read_workers,
            write_workers,
            reclaim_workers,
        }
    }

    /// Returns the number of read workers.
    pub const fn read_workers(self) -> usize {
        self.read_workers
    }

    /// Returns the number of write workers.
    pub const fn write_workers(self) -> usize {
        self.write_workers
    }

    /// Returns the number of reclaim workers.
    pub const fn reclaim_workers(self) -> usize {
        self.reclaim_workers
    }
}

impl Default for PosixIoConfig {
    fn default() -> Self {
        Self::new(
            DEFAULT_POSIX_IO_WORKERS,
            DEFAULT_POSIX_IO_WORKERS,
            DEFAULT_RECLAIM_IO_CONCURRENCY,
        )
    }
}

/// One pool's physical rings and aggregate execution bound for the
/// experimental io_uring engine.
///
/// `max_in_flight` is distributed as evenly as possible across `rings`. This
/// keeps admission capacity independent from the number of driver threads.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringPoolConfig {
    rings: usize,
    max_in_flight: usize,
    sq_poll: Option<IoUringSqPollConfig>,
    io_poll: bool,
}

impl IoUringPoolConfig {
    /// Creates one io_uring pool.
    pub const fn new(rings: usize, max_in_flight: usize) -> Self {
        Self {
            rings,
            max_in_flight,
            sq_poll: None,
            io_poll: false,
        }
    }

    /// Enables kernel-side submission queue polling for every ring in this
    /// pool.
    pub const fn with_sq_poll(mut self, sq_poll: IoUringSqPollConfig) -> Self {
        self.sq_poll = Some(sq_poll);
        self
    }

    /// Enables or disables completion polling for every ring in this pool.
    ///
    /// I/O polling consumes CPU while waiting and requires direct I/O on a
    /// filesystem and block device that support polling.
    pub const fn with_io_poll(mut self, enabled: bool) -> Self {
        self.io_poll = enabled;
        self
    }

    /// Returns the number of independent rings and driver threads.
    pub const fn rings(self) -> usize {
        self.rings
    }

    /// Returns the aggregate maximum number of in-flight requests.
    pub const fn max_in_flight(self) -> usize {
        self.max_in_flight
    }

    /// Returns the submission queue polling configuration.
    pub const fn sq_poll(self) -> Option<IoUringSqPollConfig> {
        self.sq_poll
    }

    /// Returns whether completion polling is enabled.
    pub const fn io_poll(self) -> bool {
        self.io_poll
    }
}

impl Default for IoUringPoolConfig {
    fn default() -> Self {
        Self::new(1, DEFAULT_IO_URING_MAX_IN_FLIGHT)
    }
}

/// Kernel submission queue polling parameters for the experimental io_uring
/// engine.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringSqPollConfig {
    idle_millis: u32,
    cpu: Option<u32>,
}

impl IoUringSqPollConfig {
    /// Enables submission polling and lets the kernel polling thread sleep
    /// after `idle_millis` without new submissions.
    pub const fn new(idle_millis: u32) -> Self {
        Self {
            idle_millis,
            cpu: None,
        }
    }

    /// Pins the kernel polling thread to one CPU.
    /// For a multi-ring pool, every ring's polling thread uses this CPU.
    pub const fn with_cpu(mut self, cpu: u32) -> Self {
        self.cpu = Some(cpu);
        self
    }

    /// Returns the idle time in milliseconds.
    pub const fn idle_millis(self) -> u32 {
        self.idle_millis
    }

    /// Returns the optional CPU affinity.
    pub const fn cpu(self) -> Option<u32> {
        self.cpu
    }
}

/// Independent topology for the experimental io_uring engine's read, write,
/// and reclaim traffic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringConfig {
    read: IoUringPoolConfig,
    write: IoUringPoolConfig,
    reclaim: IoUringPoolConfig,
}

impl IoUringConfig {
    /// Creates an io_uring topology from its three independent pools.
    pub const fn new(
        read: IoUringPoolConfig,
        write: IoUringPoolConfig,
        reclaim: IoUringPoolConfig,
    ) -> Self {
        Self {
            read,
            write,
            reclaim,
        }
    }

    /// Returns the read-pool topology.
    pub const fn read(self) -> IoUringPoolConfig {
        self.read
    }

    /// Returns the write-pool topology.
    pub const fn write(self) -> IoUringPoolConfig {
        self.write
    }

    /// Returns the reclaim-pool topology.
    pub const fn reclaim(self) -> IoUringPoolConfig {
        self.reclaim
    }
}

impl Default for IoUringConfig {
    fn default() -> Self {
        Self::new(
            IoUringPoolConfig::new(1, DEFAULT_IO_URING_MAX_IN_FLIGHT),
            IoUringPoolConfig::new(1, DEFAULT_IO_URING_MAX_IN_FLIGHT),
            IoUringPoolConfig::new(1, DEFAULT_RECLAIM_IO_CONCURRENCY),
        )
    }
}

/// Runtime implementation used by the independent read, write, and reclaim
/// I/O pools.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoEngine {
    /// Worker-backed POSIX positioned I/O with explicit thread counts.
    Posix(PosixIoConfig),
    /// Experimental Linux io_uring engine with independent ring and in-flight
    /// bounds.
    ///
    /// Its API, configuration, and runtime behavior may change between
    /// releases.
    ///
    /// This variant is available only with the `io-uring` crate feature on a
    /// supported Linux target.
    IoUring(IoUringConfig),
}

impl Default for IoEngine {
    fn default() -> Self {
        Self::Posix(PosixIoConfig::default())
    }
}

impl IoEngine {
    const fn is_available(self) -> bool {
        match self {
            Self::Posix(_) => true,
            Self::IoUring(_) => cfg!(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoPoolTopology {
    pub engine_count: usize,
    pub max_in_flight: usize,
    pub worker_threads: usize,
    pub io_uring: Option<IoUringPoolConfig>,
}

impl IoPoolTopology {
    pub const fn read(engine: IoEngine) -> Self {
        match engine {
            IoEngine::Posix(config) => Self::posix(config.read_workers),
            IoEngine::IoUring(config) => Self::io_uring(config.read),
        }
    }

    pub const fn write(engine: IoEngine) -> Self {
        match engine {
            IoEngine::Posix(config) => Self::posix(config.write_workers),
            IoEngine::IoUring(config) => Self::io_uring(config.write),
        }
    }

    pub const fn reclaim(engine: IoEngine) -> Self {
        match engine {
            IoEngine::Posix(config) => Self::posix(config.reclaim_workers),
            IoEngine::IoUring(config) => Self::io_uring(config.reclaim),
        }
    }

    const fn posix(workers: usize) -> Self {
        Self {
            engine_count: 1,
            max_in_flight: workers,
            worker_threads: workers,
            io_uring: None,
        }
    }

    const fn io_uring(config: IoUringPoolConfig) -> Self {
        Self {
            engine_count: config.rings,
            max_in_flight: config.max_in_flight,
            worker_threads: config.rings,
            io_uring: Some(config),
        }
    }

    pub const fn depth_for_engine(self, engine: usize) -> usize {
        let base = self.max_in_flight / self.engine_count;
        let remainder = self.max_in_flight % self.engine_count;
        base + if engine < remainder { 1 } else { 0 }
    }
}

/// Buffered/direct policy for runtime cache-record I/O.
///
/// Control files, recovery images, and any necessarily unaligned remainder use
/// buffered I/O in every mode.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoMode {
    /// Always use buffered positioned I/O.
    #[default]
    Buffered,
    /// Require Linux `O_DIRECT` support for aligned record I/O.
    ///
    /// Aligned direct-I/O errors are returned instead of falling back. A
    /// necessarily unaligned remainder still uses the buffered descriptor.
    Direct,
}

impl IoMode {
    const fn is_available(self) -> bool {
        match self {
            Self::Buffered => true,
            Self::Direct => cfg!(target_os = "linux"),
        }
    }
}

/// Eviction policy for the process-local L1 tier.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum L1EvictionPolicy {
    /// One-bit shard-local CLOCK with bounded victim scans.
    #[default]
    Clock,
    /// Three static FIFO queues: small, main, and a metadata-only ghost queue.
    S3Fifo,
}

/// Admission after an L2 index candidate has been selected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadAdmission {
    /// Return a miss when execution or read-memory capacity is unavailable.
    #[default]
    Immediate,
    /// Wait for execution capacity within a bounded queue and deadline.
    /// Queue saturation, read-memory pressure, and timeout return overload.
    Wait {
        /// Maximum wait, greater than zero and no longer than five seconds.
        timeout: Duration,
        /// Maximum queued readers, from one through 65536. `None` follows the
        /// aggregate read in-flight limit when [`CacheConfig`] is built.
        max_waiters: Option<usize>,
    },
}

/// Process-local resource choices, checked together by [`CacheConfig::new`].
///
/// These values may change across opens. Warm recovery rebinds append shards
/// from recovered Active and Free Regions when the requested topology fits.
/// Fields are unchecked inputs. Configuration construction resolves defaults and
/// checks the complete combination before any files are opened.
#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    /// Independent read, write, and reclaim pools. Defaults to POSIX with 4, 4,
    /// and 1 workers.
    pub io_engine: IoEngine,
    /// Record I/O mode. Defaults to buffered; direct I/O requires supported Linux storage.
    pub io_mode: IoMode,
    /// Admission policy after an L2 candidate has been selected.
    pub read_admission: ReadAdmission,
    /// Hash-routed append paths, from 1 through 256 (default 4). Each needs one
    /// Active Region, two Region-sized buffers, and a worker. The layout also needs a
    /// spare Region.
    pub append_shards: u32,
    /// Retained L1 byte budget, including keys, values, and ownership charges.
    /// Defaults to 256 MiB; zero disables L1. Fixed L1 metadata is charged
    /// separately.
    pub l1_capacity_bytes: usize,
    /// Bounded L1 eviction policy. Defaults to CLOCK; S3-FIFO adds ghost metadata.
    pub l1_eviction_policy: L1EvictionPolicy,
    /// Aggregate cache-managed memory limit, defaulting to 1 GiB. Covers index
    /// mappings, L1, buffers, metadata, queues, and cache threads. Allocator
    /// overhead, Tokio, application memory, and the kernel page cache are outside it.
    pub managed_memory_limit_bytes: usize,
    /// Independently locked L1 shards, from 1 through 65536 (default 32). Powers
    /// of two give the cheapest routing; more shards require more metadata.
    pub l1_shards: usize,
    /// Per-append-shard threshold for requesting a flush, in 4 KiB multiples
    /// through 4 MiB (the default). Partial buffers also flush on a bounded delay,
    /// pressure, and completion barriers.
    pub write_flush_threshold_bytes: usize,
    /// Enable cumulative request, cache, and I/O counters. Defaults to false;
    /// health and managed-resource gauges remain available.
    pub statistics: bool,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            io_engine: IoEngine::default(),
            io_mode: IoMode::Buffered,
            read_admission: ReadAdmission::Immediate,
            append_shards: DEFAULT_APPEND_SHARDS,
            l1_capacity_bytes: DEFAULT_L1_CAPACITY_BYTES,
            l1_eviction_policy: L1EvictionPolicy::Clock,
            managed_memory_limit_bytes: 1024 * 1024 * 1024,
            l1_shards: DEFAULT_L1_SHARDS,
            write_flush_threshold_bytes: MAX_WRITE_FLUSH_THRESHOLD_BYTES,
            statistics: false,
        }
    }
}

pub const fn read_io_wait_capacity(config: &RuntimeOptions) -> usize {
    match config.read_admission {
        ReadAdmission::Immediate => 0,
        ReadAdmission::Wait { max_waiters, .. } => match max_waiters {
            Some(capacity) => capacity,
            None => IoPoolTopology::read(config.io_engine).max_in_flight,
        },
    }
}

pub const fn read_io_wait_timeout(config: &RuntimeOptions) -> Duration {
    match config.read_admission {
        ReadAdmission::Immediate => Duration::ZERO,
        ReadAdmission::Wait { timeout, .. } => timeout,
    }
}

// Covers worker/shard controls and handles whose size does not scale with the
// payload or engine depth.
const RUNTIME_CONTROL_RESERVATION_BYTES: usize = 4096;
// Keep the fixed L1 directory useful when the configured L2 has deliberate
// headroom. Smaller entries may still bypass before the byte budget fills;
// this avoids sizing metadata for the theoretical 64-byte minimum.
const MIN_L1_SIZING_ENTRY_BYTES: usize = 4 * 1024;

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
            let fixed_bytes = runtime_fixed_memory_bytes(index_slots, geometry.region_count)?
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
        build().map_err(|error| from_io(ErrorOperation::BuildConfig, error))
    }
}

fn invalid_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

impl RuntimeOptions {
    fn resolve(&mut self) -> io::Result<()> {
        if !self.io_engine.is_available() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is unavailable on this build or platform",
            ));
        }
        if !self.io_mode.is_available() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "direct I/O is unavailable on this platform",
            ));
        }
        if self.append_shards == 0 || self.append_shards > MAX_APPEND_SHARDS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "append shards must be in 1..=256",
            ));
        }
        let read_topology = IoPoolTopology::read(self.io_engine);
        let write_topology = IoPoolTopology::write(self.io_engine);
        let reclaim_topology = IoPoolTopology::reclaim(self.io_engine);
        match self.io_engine {
            IoEngine::Posix(_) => {
                validate_posix_pool("read", read_topology)?;
                validate_posix_pool("write", write_topology)?;
                validate_posix_pool("reclaim", reclaim_topology)?;
            }
            IoEngine::IoUring(config) => {
                validate_io_uring_pool("read", config.read())?;
                validate_io_uring_pool("write", config.write())?;
                validate_io_uring_pool("reclaim", config.reclaim())?;
                if self.io_mode != IoMode::Direct
                    && (config.read().io_poll()
                        || config.write().io_poll()
                        || config.reclaim().io_poll())
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "io_uring IOPOLL requires direct I/O mode",
                    ));
                }
            }
        }
        if reclaim_topology.max_in_flight > self.append_shards as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "reclaim I/O concurrency must be no greater than append shards",
            ));
        }
        if let ReadAdmission::Wait {
            timeout,
            max_waiters,
        } = &mut self.read_admission
        {
            if timeout.is_zero() || *timeout > MAX_READ_IO_WAIT_TIMEOUT {
                return Err(invalid_runtime_config(
                    "read wait timeout must be greater than zero and at most five seconds",
                ));
            }
            let capacity = max_waiters.unwrap_or(read_topology.max_in_flight);
            if !(1..=MAX_CONFIG_COUNT).contains(&capacity) {
                return Err(invalid_runtime_config(
                    "maximum read waiters must be in 1..=65536",
                ));
            }
            *max_waiters = Some(capacity);
        }
        if self.managed_memory_limit_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "managed memory limit must be non-zero",
            ));
        }
        if self.l1_capacity_bytes > self.managed_memory_limit_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 capacity must not exceed the managed memory limit",
            ));
        }
        if self.l1_shards == 0 || self.l1_shards > MAX_CONFIG_COUNT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "L1 shards must be in 1..=65536",
            ));
        }
        if self.write_flush_threshold_bytes == 0
            || self.write_flush_threshold_bytes > MAX_WRITE_FLUSH_THRESHOLD_BYTES
            || !self
                .write_flush_threshold_bytes
                .is_multiple_of(BUFFER_ALIGNMENT)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "write flush threshold must be 4 KiB aligned and within 4 KiB..=4 MiB",
            ));
        }
        Ok(())
    }

    fn l1_entry_capacity(&self, geometry: DataGeometry, index_slots: usize) -> io::Result<usize> {
        if self.l1_capacity_bytes == 0 {
            return Ok(0);
        }
        let l2_capacity = u128::from(geometry.region_size)
            .checked_mul(u128::from(geometry.region_count))
            .filter(|capacity| *capacity != 0)
            .ok_or_else(|| invalid_runtime_config("L2 capacity does not fit the L1 sizing"))?;
        let expected_entries = index_slots.div_ceil(2).max(1);
        let proportional = (expected_entries as u128)
            .checked_mul(self.l1_capacity_bytes as u128)
            .and_then(|entries| entries.checked_add(l2_capacity - 1))
            .map(|entries| entries / l2_capacity)
            .and_then(|entries| usize::try_from(entries).ok())
            .ok_or_else(|| invalid_runtime_config("L1 entry capacity does not fit usize"))?;
        let four_kib_density = self.l1_capacity_bytes.div_ceil(MIN_L1_SIZING_ENTRY_BYTES);
        let maximum = MemoryStore::maximum_entry_capacity(self.l1_capacity_bytes, self.l1_shards);
        let minimum = self.l1_shards.min(maximum);
        Ok(proportional
            .max(four_kib_density)
            .min(expected_entries)
            .max(minimum)
            .min(maximum))
    }

    fn memory_requirements(
        &self,
        geometry: DataGeometry,
        fixed_bytes: usize,
    ) -> io::Result<(usize, usize)> {
        let shard_count = self.append_shards as usize;
        let topology_bytes = runtime_topology_memory_bytes(self).ok_or_else(|| {
            invalid_runtime_config("runtime topology memory requirements overflow")
        })?;
        let usable_region = usize::try_from(geometry.region_size).map_err(|_| {
            invalid_runtime_config("Region size does not fit the memory requirements")
        })?;
        let chunk_bytes = usable_region;
        let write_buffer_reservation = RegionStaging::reservation_bytes(shard_count, chunk_bytes)
            .ok_or_else(|| {
            invalid_runtime_config("write buffer memory requirements overflow")
        })?;
        let reserved_memory = fixed_bytes
            .checked_add(self.l1_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(topology_bytes))
            .ok_or_else(|| invalid_runtime_config("reserved memory requirements overflow"))?;
        let reclaim_buffers = usable_region
            .checked_mul(IoPoolTopology::reclaim(self.io_engine).max_in_flight)
            .ok_or_else(|| invalid_runtime_config("reclaim buffer memory requirements overflow"))?;
        let minimum = reserved_memory
            .checked_add(write_buffer_reservation)
            // Every reclaimer permanently owns one Region-sized buffer. Keep
            // one additional maximum-size bounded read for the foreground.
            .and_then(|bytes| bytes.checked_add(reclaim_buffers))
            .and_then(|bytes| bytes.checked_add(usable_region))
            .ok_or_else(|| invalid_runtime_config("minimum memory requirements overflow"))?;
        Ok((reserved_memory, minimum))
    }
}

fn runtime_topology_memory_bytes(config: &RuntimeOptions) -> Option<usize> {
    let shard_count = config.append_shards as usize;
    // Reserve one stack per physical I/O thread, one possible shutdown reaper
    // per engine, and every append/reclaim worker.
    let read = IoPoolTopology::read(config.io_engine);
    let write = IoPoolTopology::write(config.io_engine);
    let reclaim = IoPoolTopology::reclaim(config.io_engine);
    let engine_count = read
        .engine_count
        .checked_add(write.engine_count)?
        .checked_add(reclaim.engine_count)?;
    let stack_count = read
        .worker_threads
        .checked_add(write.worker_threads)?
        .checked_add(reclaim.worker_threads)?
        .checked_add(engine_count)?
        .checked_add(shard_count)?
        .checked_add(reclaim.max_in_flight)?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let read_wait_queue = read_io_wait_capacity(config);
    let queue = write
        .max_in_flight
        .checked_add(read.max_in_flight)?
        .checked_add(read_wait_queue)?
        .checked_add(reclaim.max_in_flight)?
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let uring = [read, write, reclaim]
        .into_iter()
        .filter(|pool| pool.io_uring.is_some())
        .try_fold(0_usize, |bytes, pool| {
            bytes.checked_add(io_uring_extra_memory_bytes(
                pool.max_in_flight,
                pool.engine_count,
            )?)
        })?;
    let controls = engine_count
        .checked_add(shard_count)?
        .checked_add(config.l1_shards)?
        .checked_add(reclaim.max_in_flight)?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(uring)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn validate_posix_pool(name: &str, topology: IoPoolTopology) -> io::Result<()> {
    if !(1..=MAX_IO_REQUESTS_PER_ENGINE).contains(&topology.max_in_flight) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("POSIX {name} worker count must be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

fn validate_io_uring_pool(name: &str, config: IoUringPoolConfig) -> io::Result<()> {
    if !(1..=MAX_CONFIG_COUNT).contains(&config.rings()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if !(1..=MAX_CONFIG_COUNT).contains(&config.max_in_flight()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} maximum in-flight requests must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if config.rings() > config.max_in_flight() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must not exceed its in-flight limit"),
        ));
    }
    if config.max_in_flight().div_ceil(config.rings()) > MAX_IO_REQUESTS_PER_ENGINE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} per-ring depth must not exceed {MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

fn invalid_runtime_config(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optional_read_wait_queue_is_memory_accounted() {
        let base = RuntimeOptions {
            io_engine: IoEngine::Posix(PosixIoConfig::new(7, 4, 1)),
            ..RuntimeOptions::default()
        };
        let no_wait = runtime_topology_memory_bytes(&base).unwrap();
        let with_wait = runtime_topology_memory_bytes(&RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(11),
            },
            ..base
        })
        .unwrap();

        assert_eq!(with_wait - no_wait, 11 * IO_QUEUE_ENTRY_RESERVATION_BYTES);
    }

    #[test]
    fn each_additional_reclaimer_is_fully_memory_accounted() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let base = RuntimeOptions {
            append_shards: 4,
            l1_capacity_bytes: 0,
            ..RuntimeOptions::default()
        };
        let (_, base_minimum) = base.memory_requirements(geometry, 0).unwrap();
        let (_, parallel_minimum) = RuntimeOptions {
            io_engine: IoEngine::Posix(PosixIoConfig::new(4, 4, 2)),
            ..base
        }
        .memory_requirements(geometry, 0)
        .unwrap();

        assert_eq!(
            parallel_minimum - base_minimum,
            geometry.region_size as usize
                + 2 * CACHE_THREAD_STACK_BYTES
                + IO_QUEUE_ENTRY_RESERVATION_BYTES
                + RUNTIME_CONTROL_RESERVATION_BYTES
        );
    }

    #[test]
    fn io_engine_topology_matches_backend_shape() {
        let posix = IoEngine::Posix(PosixIoConfig::new(7, 5, 2));
        assert_eq!(
            IoPoolTopology::read(posix),
            IoPoolTopology {
                engine_count: 1,
                max_in_flight: 7,
                worker_threads: 7,
                io_uring: None,
            }
        );

        let io_uring = IoEngine::IoUring(IoUringConfig::new(
            IoUringPoolConfig::new(3, 8),
            IoUringPoolConfig::new(2, 5),
            IoUringPoolConfig::new(1, 2),
        ));
        let read = IoPoolTopology::read(io_uring);
        assert_eq!(read.engine_count, 3);
        assert_eq!(read.max_in_flight, 8);
        assert_eq!(read.worker_threads, 3);
        assert_eq!(read.depth_for_engine(0), 3);
        assert_eq!(read.depth_for_engine(1), 3);
        assert_eq!(read.depth_for_engine(2), 2);
    }

    #[test]
    fn io_uring_depth_reserves_more_than_common_request_bookkeeping() {
        let pool = IoUringPoolConfig::new(1, 1);
        let shallow = RuntimeOptions {
            io_engine: IoEngine::IoUring(IoUringConfig::new(pool, pool, pool)),
            ..RuntimeOptions::default()
        };
        let deep = RuntimeOptions {
            io_engine: IoEngine::IoUring(IoUringConfig::new(
                IoUringPoolConfig::new(1, MAX_IO_REQUESTS_PER_ENGINE),
                pool,
                pool,
            )),
            ..shallow.clone()
        };
        let growth = runtime_topology_memory_bytes(&deep).unwrap()
            - runtime_topology_memory_bytes(&shallow).unwrap();
        assert!(growth > (MAX_IO_REQUESTS_PER_ENGINE - 1) * IO_QUEUE_ENTRY_RESERVATION_BYTES);
    }

    #[test]
    fn io_uring_pool_validation_bounds_rings_and_depth() {
        validate_io_uring_pool("read", IoUringPoolConfig::new(3, 8)).unwrap();

        for config in [
            IoUringPoolConfig::new(0, 8),
            IoUringPoolConfig::new(1, 0),
            IoUringPoolConfig::new(3, 2),
            IoUringPoolConfig::new(1, MAX_IO_REQUESTS_PER_ENGINE + 1),
        ] {
            assert_eq!(
                validate_io_uring_pool("read", config).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    #[test]
    fn io_poll_requires_direct_mode() {
        let pool = IoUringPoolConfig::default().with_io_poll(true);
        let mut config = RuntimeOptions {
            io_engine: IoEngine::IoUring(IoUringConfig::new(
                pool,
                IoUringPoolConfig::default(),
                IoUringPoolConfig::new(1, 1),
            )),
            ..RuntimeOptions::default()
        };

        assert_eq!(
            config.resolve().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
