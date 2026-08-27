pub(crate) const DEFAULT_L1_SHARDS: usize = 32;
pub(crate) const MAX_WRITE_SHARDS: u32 = 256;
pub(crate) const MAX_WRITE_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_L1_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_WRITE_SHARDS: u32 = 4;
const IO_URING_DEPTH_PER_WORKER: usize = 64;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoEngine {
    /// Worker-backed POSIX positioned I/O.
    #[default]
    Posix,
    /// Linux io_uring, available only with the `io-uring` crate feature.
    IoUring,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoMode {
    Buffered,
    #[default]
    Auto,
    Direct,
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub(crate) io_engine: IoEngine,
    pub(crate) io_mode: IoMode,
    pub(crate) read_io_workers: usize,
    pub(crate) write_io_workers: usize,
    pub(crate) write_shards: u32,
    pub(crate) l1_capacity_bytes: usize,
    pub(crate) memory_limit_bytes: usize,
    pub(crate) l1_shards: usize,
    pub(crate) write_batch_bytes: usize,
    pub(crate) statistics: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            io_engine: IoEngine::Posix,
            io_mode: IoMode::Auto,
            read_io_workers: 4,
            write_io_workers: 4,
            write_shards: DEFAULT_WRITE_SHARDS,
            l1_capacity_bytes: DEFAULT_L1_CAPACITY_BYTES,
            memory_limit_bytes: 1024 * 1024 * 1024,
            l1_shards: DEFAULT_L1_SHARDS,
            write_batch_bytes: MAX_WRITE_BATCH_BYTES,
            statistics: false,
        }
    }
}

impl RuntimeConfig {
    pub fn with_io_engine(mut self, engine: IoEngine) -> Self {
        self.io_engine = engine;
        self
    }

    pub fn with_io_mode(mut self, mode: IoMode) -> Self {
        self.io_mode = mode;
        self
    }

    pub fn with_read_io_workers(mut self, workers: usize) -> Self {
        self.read_io_workers = workers;
        self
    }

    pub fn with_write_io_workers(mut self, workers: usize) -> Self {
        self.write_io_workers = workers;
        self
    }

    /// Sets the number of independent append/staging paths created at open.
    ///
    /// Each path owns one Active Region, two fixed write buffers, and one
    /// ordered worker. The valid range is 1..=256. Changing this value across
    /// opens safely cold-starts the disposable cache instead of migrating
    /// recovered shard state; it does not change the static disk identity.
    pub fn with_write_shards(mut self, shards: u32) -> Self {
        self.write_shards = shards;
        self
    }

    pub fn with_l1_capacity(mut self, bytes: usize) -> Self {
        self.l1_capacity_bytes = bytes;
        self
    }

    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit_bytes = bytes;
        self
    }

    pub fn with_l1_shards(mut self, shards: usize) -> Self {
        self.l1_shards = shards;
        self
    }

    pub fn with_write_batch_size(mut self, bytes: usize) -> Self {
        self.write_batch_bytes = bytes;
        self
    }

    pub fn with_statistics(mut self, enabled: bool) -> Self {
        self.statistics = enabled;
        self
    }

    pub const fn io_engine(&self) -> IoEngine {
        self.io_engine
    }

    pub const fn io_mode(&self) -> IoMode {
        self.io_mode
    }

    pub const fn read_io_workers(&self) -> usize {
        self.read_io_workers
    }

    pub const fn write_io_workers(&self) -> usize {
        self.write_io_workers
    }

    pub const fn write_shards(&self) -> u32 {
        self.write_shards
    }

    pub const fn l1_capacity_bytes(&self) -> usize {
        self.l1_capacity_bytes
    }

    pub const fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes
    }

    pub const fn l1_shards(&self) -> usize {
        self.l1_shards
    }

    pub const fn write_batch_bytes(&self) -> usize {
        self.write_batch_bytes
    }

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
    use super::{IoEngine, RuntimeConfig};

    #[test]
    fn runtime_defaults_to_posix_io() {
        let config = RuntimeConfig::default();
        assert_eq!(config.io_engine(), IoEngine::Posix);
        assert_eq!(config.read_io_workers(), 4);
        assert_eq!(config.write_io_workers(), 4);
        assert_eq!(config.write_shards(), 4);
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
