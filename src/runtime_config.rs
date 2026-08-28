pub(crate) const DEFAULT_L1_SHARDS: usize = 32;
pub(crate) const MAX_APPEND_SHARDS: u32 = 256;
pub(crate) const MAX_WRITE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;
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

/// Process-local cache topology and resource tuning validated during open.
///
/// These values do not form the static disk identity. They may change across
/// opens, although changing the append-shard count makes an existing clean
/// recovery image ineligible and therefore starts with an empty cache.
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub(crate) io_engine: IoEngine,
    pub(crate) io_mode: IoMode,
    pub(crate) read_io_workers: usize,
    pub(crate) write_io_workers: usize,
    pub(crate) reclaim_workers: usize,
    pub(crate) append_shards: u32,
    pub(crate) l1_capacity_bytes: usize,
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
            write_io_workers: 4,
            reclaim_workers: 1,
            append_shards: DEFAULT_APPEND_SHARDS,
            l1_capacity_bytes: DEFAULT_L1_CAPACITY_BYTES,
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
    /// and returns direct-I/O errors without retrying through buffered I/O.
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
    /// The count must be non-zero and no greater than the append-shard count;
    /// additional workers cannot increase the per-shard clean reserve.
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
    /// Changing this value across opens safely cold-starts the disposable cache
    /// instead of migrating recovered shard state; it does not change the
    /// static disk identity.
    pub fn with_append_shards(mut self, shards: u32) -> Self {
        self.append_shards = shards;
        self
    }

    /// Sets the retained-entry byte budget for L1. Zero disables L1.
    ///
    /// Entry charges include the key, value, and fixed ownership charge. L1
    /// slot, CLOCK, free-list, and directory allocations are accounted
    /// separately against the managed-memory limit.
    pub fn with_l1_capacity_bytes(mut self, bytes: usize) -> Self {
        self.l1_capacity_bytes = bytes;
        self
    }

    /// Sets the aggregate cache-managed memory budget.
    ///
    /// The budget accounts for the index mapping extent, L1, append staging,
    /// cache-owned thread topology, metadata, recovery scratch, and transient
    /// record reads. It is not a bound on process RSS, allocator metadata,
    /// Tokio, or the kernel page cache.
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
    /// This is not a maximum record or I/O size: a record may cross the
    /// threshold, and partial buffers also flush after the bounded delay or
    /// during pressure and lifecycle barriers. The value must be a non-zero
    /// 4 KiB multiple no larger than 4 MiB.
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
    use super::{IoEngine, IoMode, RuntimeConfig};

    #[test]
    fn runtime_defaults_to_posix_buffered_io() {
        let config = RuntimeConfig::default();
        assert_eq!(config.io_engine(), IoEngine::Posix);
        assert_eq!(config.io_mode(), IoMode::Buffered);
        assert_eq!(config.read_io_workers(), 4);
        assert_eq!(config.write_io_workers(), 4);
        assert_eq!(config.reclaim_workers(), 1);
        assert_eq!(config.append_shards(), 4);
        assert_eq!(config.io_engine_count(config.read_io_workers()), 1);
        assert_eq!(config.io_depth_per_engine(config.read_io_workers()), 4);
        assert_eq!(
            config
                .with_io_engine(IoEngine::IoUring)
                .io_depth_per_engine(4),
            64
        );
    }
}
