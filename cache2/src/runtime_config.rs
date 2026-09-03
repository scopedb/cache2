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

use std::time::Duration;

pub(crate) const DEFAULT_L1_SHARDS: usize = 32;
pub(crate) const MAX_APPEND_SHARDS: u32 = 256;
pub(crate) const MAX_WRITE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_READ_IO_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
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

/// One io_uring pool's physical rings and aggregate execution bound.
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

/// Kernel submission queue polling parameters for an io_uring pool.
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

/// Independent io_uring topology for read, write, and reclaim traffic.
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
    /// Linux io_uring with independent ring and in-flight bounds.
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
    pub(crate) const fn is_available(self) -> bool {
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

    pub(crate) const fn read_topology(self) -> IoPoolTopology {
        match self {
            Self::Posix(config) => IoPoolTopology::posix(config.read_workers),
            Self::IoUring(config) => IoPoolTopology::io_uring(config.read),
        }
    }

    pub(crate) const fn write_topology(self) -> IoPoolTopology {
        match self {
            Self::Posix(config) => IoPoolTopology::posix(config.write_workers),
            Self::IoUring(config) => IoPoolTopology::io_uring(config.write),
        }
    }

    pub(crate) const fn reclaim_topology(self) -> IoPoolTopology {
        match self {
            Self::Posix(config) => IoPoolTopology::posix(config.reclaim_workers),
            Self::IoUring(config) => IoPoolTopology::io_uring(config.reclaim),
        }
    }

    pub(crate) const fn is_posix(self) -> bool {
        matches!(self, Self::Posix(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IoPoolTopology {
    pub(crate) engine_count: usize,
    pub(crate) max_in_flight: usize,
    pub(crate) worker_threads: usize,
    pub(crate) io_uring: Option<IoUringPoolConfig>,
}

impl IoPoolTopology {
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

    pub(crate) const fn depth_for_engine(self, engine: usize) -> usize {
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
    /// necessarily unaligned remainder still uses the buffered descriptor
    /// unless io_uring IOPOLL is enabled, in which case it is rejected.
    Direct,
}

impl IoMode {
    pub(crate) const fn is_available(self) -> bool {
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

/// Process-local cache topology and resource tuning validated during open.
///
/// These values may change across opens. Warm recovery rebinds append shards
/// from recovered Active and Free Regions when the requested topology fits.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub(crate) io_engine: IoEngine,
    pub(crate) io_mode: IoMode,
    pub(crate) read_io_wait_capacity: Option<usize>,
    pub(crate) read_io_wait_timeout: Duration,
    pub(crate) append_shards: u32,
    pub(crate) l1_capacity_bytes: usize,
    pub(crate) l1_eviction_policy: L1EvictionPolicy,
    pub(crate) managed_memory_limit_bytes: usize,
    pub(crate) l1_shards: usize,
    pub(crate) write_flush_threshold_bytes: usize,
    pub(crate) statistics: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            io_engine: IoEngine::default(),
            io_mode: IoMode::Buffered,
            read_io_wait_capacity: None,
            read_io_wait_timeout: Duration::ZERO,
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

impl RuntimeConfig {
    /// Selects the implementation used by the independent I/O pools.
    ///
    /// [`IoEngine::Posix`] is the default. Each engine variant carries only
    /// the physical topology meaningful to that backend.
    pub fn with_io_engine(mut self, engine: IoEngine) -> Self {
        self.io_engine = engine;
        self
    }

    /// Selects the buffered/direct policy for runtime record I/O.
    ///
    /// Buffered I/O is the default. `Direct` requires Linux `O_DIRECT` support
    /// and returns direct-I/O errors directly.
    pub fn with_io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    /// Sets the maximum number of reads waiting for execution capacity.
    ///
    /// By default this follows the configured aggregate read in-flight limit.
    /// Waiting is active only when [`Self::with_read_io_wait_timeout`] is
    /// non-zero. Valid capacities range from one through 65536.
    pub fn with_read_io_wait_capacity(mut self, capacity: usize) -> Self {
        self.read_io_wait_capacity = Some(capacity);
        self
    }

    /// Sets how long an L2 candidate may wait for read execution capacity.
    ///
    /// Zero, the default, makes read-pool pressure a miss. A non-zero timeout
    /// enables the bounded wait capacity. A full queue, memory pressure, or
    /// timeout is an overload error. Valid timeouts range from zero through five
    /// seconds.
    pub fn with_read_io_wait_timeout(mut self, timeout: Duration) -> Self {
        self.read_io_wait_timeout = timeout;
        self
    }

    /// Sets the number of hash-routed append/staging paths created at open.
    ///
    /// Each path owns one Active Region, two Region-sized write buffers, and
    /// one ordered shard worker. The static geometry must provide one Region
    /// per append shard plus at least one spare Region. The valid range is
    /// `1..=256`.
    ///
    /// Changing this value across opens rebinds a clean recovery image when
    /// enough Free Regions are available. Otherwise the disposable cache safely
    /// starts empty.
    pub fn with_append_shards(mut self, shards: u32) -> Self {
        self.append_shards = shards;
        self
    }

    /// Sets the retained-entry byte budget for L1. Zero disables L1.
    ///
    /// Entry charges include the key, value, and fixed ownership charge. L1
    /// slot, eviction-policy, free-list, and directory allocations are
    /// accounted separately against the managed-memory limit.
    pub fn with_l1_capacity_bytes(mut self, bytes: usize) -> Self {
        self.l1_capacity_bytes = bytes;
        self
    }

    /// Selects the bounded shard-local L1 eviction policy.
    ///
    /// CLOCK is the default. S3-FIFO adds a metadata-only ghost queue, uses a
    /// two-bit hit counter, and preserves queue position on the hit path.
    pub fn with_l1_eviction_policy(mut self, policy: L1EvictionPolicy) -> Self {
        self.l1_eviction_policy = policy;
        self
    }

    /// Sets the aggregate cache-managed memory budget.
    ///
    /// The budget accounts for the index mapping extent, L1, append staging,
    /// cache-owned thread topology, metadata, recovery scratch, and transient
    /// record reads. Total deployment memory additionally includes allocator
    /// metadata, Tokio, process overhead, and the kernel page cache.
    pub fn with_managed_memory_limit_bytes(mut self, bytes: usize) -> Self {
        self.managed_memory_limit_bytes = bytes;
        self
    }

    /// Sets the number of independently locked L1 shards, in `1..=65536`.
    ///
    /// Power-of-two counts use the cheapest routing path. More shards reduce
    /// contention but increase fixed metadata and runtime-control accounting.
    pub fn with_l1_shards(mut self, shards: usize) -> Self {
        self.l1_shards = shards;
        self
    }

    /// Sets the per-append-shard buffered-byte threshold for requesting a flush.
    ///
    /// A record may cross the threshold, and partial buffers also flush after
    /// the bounded delay or during pressure and lifecycle barriers. Valid values
    /// are 4 KiB multiples from 4 KiB through 4 MiB.
    pub fn with_write_flush_threshold_bytes(mut self, bytes: usize) -> Self {
        self.write_flush_threshold_bytes = bytes;
        self
    }

    /// Enables optional cumulative request, L1, index, and I/O counters.
    ///
    /// Health and managed-resource gauges remain available when disabled.
    pub fn with_statistics(mut self, enabled: bool) -> Self {
        self.statistics = enabled;
        self
    }

    /// Returns the configured I/O engine.
    pub const fn io_engine(&self) -> IoEngine {
        self.io_engine
    }

    /// Returns the configured runtime record-I/O mode.
    pub const fn io_mode(&self) -> IoMode {
        self.io_mode
    }

    /// Returns the aggregate maximum number of in-flight reads.
    pub const fn read_io_max_in_flight(&self) -> usize {
        self.io_engine.read_topology().max_in_flight
    }

    /// Returns the maximum number of reads waiting for execution capacity.
    pub const fn read_io_wait_capacity(&self) -> usize {
        match self.read_io_wait_capacity {
            Some(capacity) => capacity,
            None => self.read_io_max_in_flight(),
        }
    }

    /// Returns the maximum wait for L2 read execution capacity.
    pub const fn read_io_wait_timeout(&self) -> Duration {
        self.read_io_wait_timeout
    }

    /// Returns the aggregate maximum number of in-flight writes.
    pub const fn write_io_max_in_flight(&self) -> usize {
        self.io_engine.write_topology().max_in_flight
    }

    /// Returns the aggregate maximum number of concurrent Region reclaims.
    pub const fn reclaim_io_max_in_flight(&self) -> usize {
        self.io_engine.reclaim_topology().max_in_flight
    }

    /// Returns the number of hash-routed append shards.
    pub const fn append_shards(&self) -> u32 {
        self.append_shards
    }

    /// Returns the retained-entry byte budget for L1.
    pub const fn l1_capacity_bytes(&self) -> usize {
        self.l1_capacity_bytes
    }

    /// Returns the configured L1 eviction policy.
    pub const fn l1_eviction_policy(&self) -> L1EvictionPolicy {
        self.l1_eviction_policy
    }

    /// Returns the aggregate cache-managed memory budget.
    pub const fn managed_memory_limit_bytes(&self) -> usize {
        self.managed_memory_limit_bytes
    }

    /// Returns the number of independently locked L1 shards.
    pub const fn l1_shards(&self) -> usize {
        self.l1_shards
    }

    /// Returns the per-append-shard flush threshold in bytes.
    pub const fn write_flush_threshold_bytes(&self) -> usize {
        self.write_flush_threshold_bytes
    }

    /// Returns whether optional cumulative statistics are enabled.
    pub const fn statistics_enabled(&self) -> bool {
        self.statistics
    }

    pub(crate) const fn read_io_topology(&self) -> IoPoolTopology {
        self.io_engine.read_topology()
    }

    pub(crate) const fn write_io_topology(&self) -> IoPoolTopology {
        self.io_engine.write_topology()
    }

    pub(crate) const fn reclaim_io_topology(&self) -> IoPoolTopology {
        self.io_engine.reclaim_topology()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_defaults_are_stable() {
        let config = RuntimeConfig::default();
        assert_eq!(config.io_engine(), IoEngine::default());
        assert_eq!(config.io_mode(), IoMode::Buffered);
        assert_eq!(config.read_io_max_in_flight(), 4);
        assert_eq!(config.read_io_wait_capacity(), 4);
        assert_eq!(config.read_io_wait_timeout(), Duration::ZERO);
        assert_eq!(config.write_io_max_in_flight(), 4);
        assert_eq!(config.reclaim_io_max_in_flight(), 1);
        assert_eq!(config.append_shards(), DEFAULT_APPEND_SHARDS);
        assert_eq!(config.l1_capacity_bytes(), DEFAULT_L1_CAPACITY_BYTES);
        assert_eq!(config.l1_eviction_policy(), L1EvictionPolicy::Clock);
        assert_eq!(config.managed_memory_limit_bytes(), 1024 * 1024 * 1024);
        assert_eq!(config.l1_shards(), DEFAULT_L1_SHARDS);
        assert_eq!(
            config.write_flush_threshold_bytes(),
            MAX_WRITE_FLUSH_THRESHOLD_BYTES
        );
        assert!(!config.statistics_enabled());
    }

    #[test]
    fn explicit_read_wait_capacity_is_independent_from_execution() {
        let config = RuntimeConfig::default()
            .with_read_io_wait_capacity(11)
            .with_io_engine(IoEngine::Posix(PosixIoConfig::new(7, 4, 1)));

        assert_eq!(config.read_io_max_in_flight(), 7);
        assert_eq!(config.read_io_wait_capacity(), 11);
    }

    #[test]
    fn io_engine_topology_matches_backend_shape() {
        let posix = IoEngine::Posix(PosixIoConfig::new(7, 5, 2));
        assert_eq!(
            posix.read_topology(),
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
        let read = io_uring.read_topology();
        assert_eq!(read.engine_count, 3);
        assert_eq!(read.max_in_flight, 8);
        assert_eq!(read.worker_threads, 3);
        assert_eq!(read.depth_for_engine(0), 3);
        assert_eq!(read.depth_for_engine(1), 3);
        assert_eq!(read.depth_for_engine(2), 2);
    }

    #[test]
    fn io_uring_pool_exposes_polling_options() {
        let sq_poll = IoUringSqPollConfig::new(2_000).with_cpu(3);
        let pool = IoUringPoolConfig::new(2, 96)
            .with_sq_poll(sq_poll)
            .with_io_poll(true);

        assert_eq!(pool.rings(), 2);
        assert_eq!(pool.max_in_flight(), 96);
        assert_eq!(pool.sq_poll(), Some(sq_poll));
        assert!(pool.io_poll());
    }
}
