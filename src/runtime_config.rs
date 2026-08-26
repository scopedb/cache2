use std::time::Duration;

use crate::eviction::EvictionPolicy;
use crate::resources::WriteBackpressure;

pub(crate) const DEFAULT_L1_SHARDS: usize = 32;
pub(crate) const MAX_WRITE_BATCH_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_L1_CAPACITY_BYTES: usize = 256 * 1024 * 1024;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IoEngine {
    Sync,
    #[default]
    Auto,
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
    pub(crate) io_workers: usize,
    pub(crate) io_concurrency: usize,
    pub(crate) waiting_write_limit: usize,
    pub(crate) l1_capacity_bytes: usize,
    pub(crate) memory_limit_bytes: usize,
    pub(crate) l1_shards: usize,
    pub(crate) eviction_policy: EvictionPolicy,
    pub(crate) write_buffer_bytes: usize,
    pub(crate) write_batch_bytes: usize,
    pub(crate) write_flush_delay: Duration,
    pub(crate) write_backpressure: WriteBackpressure,
    pub(crate) statistics: bool,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            io_engine: IoEngine::Auto,
            io_mode: IoMode::Auto,
            io_workers: 4,
            io_concurrency: 128,
            waiting_write_limit: 128,
            l1_capacity_bytes: DEFAULT_L1_CAPACITY_BYTES,
            memory_limit_bytes: 1024 * 1024 * 1024,
            l1_shards: DEFAULT_L1_SHARDS,
            eviction_policy: EvictionPolicy::Clock,
            write_buffer_bytes: MAX_WRITE_BATCH_BYTES,
            write_batch_bytes: MAX_WRITE_BATCH_BYTES,
            write_flush_delay: Duration::from_millis(1),
            write_backpressure: WriteBackpressure::Reject,
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

    pub fn with_io_workers(mut self, workers: usize) -> Self {
        self.io_workers = workers;
        self
    }

    /// Sets aggregate asynchronous I/O admission. The synchronous engine has
    /// one executable slot per `io_worker` and does not queue cache reads.
    pub fn with_io_concurrency(mut self, requests: usize) -> Self {
        self.io_concurrency = requests;
        self
    }

    pub fn with_waiting_write_limit(mut self, writes: usize) -> Self {
        self.waiting_write_limit = writes;
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

    pub fn with_eviction_policy(mut self, policy: EvictionPolicy) -> Self {
        self.eviction_policy = policy;
        self
    }

    pub fn with_write_buffer_size(mut self, bytes: usize) -> Self {
        self.write_buffer_bytes = bytes;
        self
    }

    pub fn with_write_batch_size(mut self, bytes: usize) -> Self {
        self.write_batch_bytes = bytes;
        self
    }

    pub fn with_write_flush_delay(mut self, delay: Duration) -> Self {
        self.write_flush_delay = delay;
        self
    }

    pub fn with_write_backpressure(mut self, policy: WriteBackpressure) -> Self {
        self.write_backpressure = policy;
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

    pub const fn io_workers(&self) -> usize {
        self.io_workers
    }

    pub const fn io_concurrency(&self) -> usize {
        self.io_concurrency
    }

    pub const fn waiting_write_limit(&self) -> usize {
        self.waiting_write_limit
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

    pub const fn eviction_policy(&self) -> EvictionPolicy {
        self.eviction_policy
    }

    pub const fn write_buffer_bytes(&self) -> usize {
        self.write_buffer_bytes
    }

    pub const fn write_batch_bytes(&self) -> usize {
        self.write_batch_bytes
    }

    pub const fn write_flush_delay(&self) -> Duration {
        self.write_flush_delay
    }

    pub const fn write_backpressure(&self) -> WriteBackpressure {
        self.write_backpressure
    }

    pub const fn statistics_enabled(&self) -> bool {
        self.statistics
    }
}
