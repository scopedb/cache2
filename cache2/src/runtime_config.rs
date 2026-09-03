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
const IO_URING_DEPTH_PER_WORKER: usize = 64;

/// Runtime implementation used by the independent read, write, and reclaim
/// I/O pools.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoEngine {
    /// Worker-backed POSIX positioned I/O.
    #[default]
    Posix,
    /// Linux io_uring, available only with the `io-uring` crate feature.
    IoUring,
}

impl IoEngine {
    pub(crate) const fn is_available(self) -> bool {
        match self {
            Self::Posix => true,
            Self::IoUring => cfg!(all(
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
    pub(crate) read_io_workers: usize,
    pub(crate) read_io_wait_capacity: Option<usize>,
    pub(crate) read_io_wait_timeout: Duration,
    pub(crate) write_io_workers: usize,
    pub(crate) reclaim_workers: usize,
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
            io_engine: IoEngine::Posix,
            io_mode: IoMode::Buffered,
            read_io_workers: 4,
            read_io_wait_capacity: None,
            read_io_wait_timeout: Duration::ZERO,
            write_io_workers: 4,
            reclaim_workers: 1,
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
    /// POSIX worker-backed positioned I/O is the default. `IoUring` requires
    /// the `io-uring` crate feature and a supported Linux target.
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

    /// Sets read execution concurrency.
    ///
    /// With the POSIX engine this is both the number of worker threads and the
    /// maximum number of admitted reads, in `1..=4096`. With `io_uring` it is
    /// the number of independent fixed-depth rings.
    pub fn with_read_io_workers(mut self, workers: usize) -> Self {
        self.read_io_workers = workers;
        self
    }

    /// Sets the maximum number of reads waiting for execution capacity.
    ///
    /// By default this follows the configured read worker count. Waiting is
    /// active only when [`Self::with_read_io_wait_timeout`] is non-zero. Valid
    /// capacities range from one through 65536.
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

    /// Sets write execution concurrency independently from reads.
    ///
    /// With the POSIX engine this is both the number of worker threads and the
    /// maximum number of in-flight writes, in `1..=4096`. With `io_uring` it is
    /// the number of independent fixed-depth rings.
    pub fn with_write_io_workers(mut self, workers: usize) -> Self {
        self.write_io_workers = workers;
        self
    }

    /// Sets the number of concurrent Region reclaim workers.
    ///
    /// Each worker owns one Region-sized scan buffer and one reclaim I/O lane.
    /// Valid counts range from one through the append-shard count; that range
    /// matches the available per-shard clean reserve.
    pub fn with_reclaim_workers(mut self, workers: usize) -> Self {
        self.reclaim_workers = workers;
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

    /// Returns the configured read execution concurrency.
    pub const fn read_io_workers(&self) -> usize {
        self.read_io_workers
    }

    /// Returns the maximum number of reads waiting for execution capacity.
    pub const fn read_io_wait_capacity(&self) -> usize {
        match self.read_io_wait_capacity {
            Some(capacity) => capacity,
            None => self.read_io_workers,
        }
    }

    /// Returns the maximum wait for L2 read execution capacity.
    pub const fn read_io_wait_timeout(&self) -> Duration {
        self.read_io_wait_timeout
    }

    /// Returns the configured write execution concurrency.
    pub const fn write_io_workers(&self) -> usize {
        self.write_io_workers
    }

    /// Returns the number of concurrent Region reclaim workers.
    pub const fn reclaim_workers(&self) -> usize {
        self.reclaim_workers
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

    pub(crate) const fn io_engine_count(&self, workers: usize) -> usize {
        match self.io_engine {
            IoEngine::Posix => 1,
            IoEngine::IoUring => workers,
        }
    }

    pub(crate) const fn io_depth_per_engine(&self, workers: usize) -> usize {
        match self.io_engine {
            IoEngine::Posix => workers,
            IoEngine::IoUring => IO_URING_DEPTH_PER_WORKER,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_defaults_are_stable() {
        let config = RuntimeConfig::default();
        assert_eq!(config.io_engine(), IoEngine::Posix);
        assert_eq!(config.io_mode(), IoMode::Buffered);
        assert_eq!(config.read_io_workers(), 4);
        assert_eq!(config.read_io_wait_capacity(), 4);
        assert_eq!(config.read_io_wait_timeout(), Duration::ZERO);
        assert_eq!(config.write_io_workers(), 4);
        assert_eq!(config.reclaim_workers(), 1);
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
    fn explicit_read_wait_capacity_is_independent_from_workers() {
        let config = RuntimeConfig::default()
            .with_read_io_wait_capacity(11)
            .with_read_io_workers(7);

        assert_eq!(config.read_io_workers(), 7);
        assert_eq!(config.read_io_wait_capacity(), 11);
    }

    #[test]
    fn io_engine_capacity_matches_backend_shape() {
        let posix = RuntimeConfig::default();
        assert_eq!(posix.io_engine_count(7), 1);
        assert_eq!(posix.io_depth_per_engine(7), 7);

        let io_uring = posix.with_io_engine(IoEngine::IoUring);
        assert_eq!(io_uring.io_engine_count(7), 7);
        assert_eq!(io_uring.io_depth_per_engine(7), IO_URING_DEPTH_PER_WORKER);
    }
}
