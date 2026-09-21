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
use std::time::Duration;
use std::time::Instant;

use crate::StatsOptions;
use crate::config::CacheConfig;
use crate::config::StorageLayout;
use crate::error::Error;
use crate::error::ErrorOperation;
use crate::error::from_io;
use crate::io::engine::IO_QUEUE_ENTRY_RESERVATION_BYTES;
use crate::io::engine::MAX_IO_REQUESTS_PER_ENGINE;
use crate::io::engine::io_uring_extra_memory_bytes;
use crate::managed_memory::BUFFER_ALIGNMENT;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;
use crate::managed_memory::MAX_CONFIG_COUNT;
use crate::memory::MemoryStore;
use crate::region::recovery::DataGeometry;
use crate::region::runtime::metrics::ActivityMetrics;
use crate::region::runtime_fixed_memory_bytes;
use crate::region::staging::RegionStaging;
use crate::stats::recording::Recorder;

const DEFAULT_L1_SHARDS: usize = 32;
const MAX_APPEND_SHARDS: u32 = 256;
pub const MAX_WRITE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;
const MAX_READ_IO_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_L1_CAPACITY_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_APPEND_SHARDS: u32 = 4;
const DEFAULT_POSIX_IO_WORKERS: usize = 4;
const DEFAULT_IO_URING_MAX_IN_FLIGHT: usize = 64;
const DEFAULT_RECLAIM_IO_CONCURRENCY: usize = 1;

/// Unchecked worker topology for POSIX positioned I/O.
///
/// Start with [`Self::default`] and assign fields before building [`CacheConfig`].
/// Every admitted request occupies one worker until its blocking system call
/// completes, so worker counts are also the per-pool in-flight limits.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PosixIoOptions {
    /// Number of read workers. Defaults to 4.
    pub read_workers: usize,
    /// Number of write workers. Defaults to 4.
    pub write_workers: usize,
    /// Number of reclaim workers. Defaults to 1.
    pub reclaim_workers: usize,
}

impl Default for PosixIoOptions {
    fn default() -> Self {
        Self {
            read_workers: DEFAULT_POSIX_IO_WORKERS,
            write_workers: DEFAULT_POSIX_IO_WORKERS,
            reclaim_workers: DEFAULT_RECLAIM_IO_CONCURRENCY,
        }
    }
}

/// Unchecked physical rings and aggregate execution bound for one pool of the
/// experimental io_uring engine.
///
/// Start with [`Self::default`] and assign fields before building [`CacheConfig`].
/// Keep [`Self::rings`] at 1 and size [`Self::max_in_flight`] to concurrent work
/// for this pool. Extra rings split the same depth across driver threads; they
/// do not add execution slots. Leave [`Self::sq_poll`] and [`Self::io_poll`]
/// unset unless a host profile needs one of them; they are independent advanced
/// flags and are easy to combine incorrectly.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringPoolOptions {
    /// Independent rings and driver threads. Defaults to 1.
    ///
    /// Raise this only after `max_in_flight` matches caller concurrency and a
    /// single driver thread is CPU-bound. Must not exceed `max_in_flight`.
    pub rings: usize,
    /// Aggregate maximum in-flight requests. Defaults to 64.
    ///
    /// This is the admission bound. It is distributed as evenly as possible
    /// across `rings`. For the read pool, set it to concurrent L2 gets.
    pub max_in_flight: usize,
    /// Advanced kernel submission-queue polling for every ring in this pool.
    ///
    /// Defaults to `None`, which disables SQPOLL. SQPOLL does not require
    /// Direct I/O. Optional CPU affinity applies to every ring in the pool.
    pub sq_poll: Option<IoUringSqPollOptions>,
    /// Advanced completion polling for every ring in this pool.
    ///
    /// Defaults to false. Requires Direct I/O on a polling-capable filesystem
    /// and block device. Consumes CPU while requests are outstanding, and a
    /// cancellation stays advisory until the polled operation completes.
    /// Independent of [`Self::sq_poll`].
    pub io_poll: bool,
}

impl Default for IoUringPoolOptions {
    fn default() -> Self {
        Self {
            rings: 1,
            max_in_flight: DEFAULT_IO_URING_MAX_IN_FLIGHT,
            sq_poll: None,
            io_poll: false,
        }
    }
}

/// Unchecked kernel submission queue polling parameters for the experimental
/// io_uring engine.
///
/// This is an advanced opt-in. Start with [`Self::new`] and assign fields
/// before building [`CacheConfig`]. For a multi-ring pool, every ring's
/// polling thread uses the same optional CPU affinity.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringSqPollOptions {
    /// Idle time in milliseconds before the kernel polling thread can sleep.
    pub idle_millis: u32,
    /// Optional CPU affinity. Defaults to `None`, which leaves the thread unpinned.
    /// For a multi-ring pool, every ring's polling thread uses this CPU.
    pub cpu: Option<u32>,
}

impl IoUringSqPollOptions {
    /// Selects a submission polling idle time with no CPU affinity.
    pub const fn new(idle_millis: u32) -> Self {
        Self {
            idle_millis,
            cpu: None,
        }
    }
}

/// Unchecked topology for the experimental io_uring engine's independent read,
/// write, and reclaim pools.
///
/// Start with [`Self::default`] and assign fields before building [`CacheConfig`].
/// Keep one ring per pool. Set [`IoUringPoolOptions::max_in_flight`] on the read
/// pool to concurrent L2 gets; leave write and reclaim at their defaults unless
/// write slot wait or reclaim lag is the limiter.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringOptions {
    /// Read pool. Defaults to one ring and 64 in-flight requests.
    pub read: IoUringPoolOptions,
    /// Write pool. Defaults to one ring and 64 in-flight requests.
    pub write: IoUringPoolOptions,
    /// Reclaim pool. Defaults to one ring and one in-flight request.
    pub reclaim: IoUringPoolOptions,
}

impl Default for IoUringOptions {
    fn default() -> Self {
        Self {
            read: IoUringPoolOptions::default(),
            write: IoUringPoolOptions::default(),
            reclaim: IoUringPoolOptions {
                max_in_flight: DEFAULT_RECLAIM_IO_CONCURRENCY,
                ..IoUringPoolOptions::default()
            },
        }
    }
}

/// Unchecked engine selection and topology for the independent read, write,
/// and reclaim I/O pools, validated by [`CacheConfig::new`].
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoEngineOptions {
    /// Worker-backed POSIX positioned I/O with explicit thread counts.
    Posix(PosixIoOptions),
    /// Experimental Linux io_uring engine with independent ring and in-flight
    /// bounds.
    ///
    /// Its API, configuration, and runtime behavior may change between
    /// releases. Keep one ring per pool and size read `max_in_flight` to
    /// concurrent L2 gets. SQPOLL and IOPOLL are advanced per-pool opt-ins
    /// with different requirements.
    ///
    /// This variant is available only with the `io-uring` crate feature on a
    /// supported Linux target.
    IoUring(IoUringOptions),
}

impl Default for IoEngineOptions {
    fn default() -> Self {
        Self::Posix(PosixIoOptions::default())
    }
}

impl IoEngineOptions {
    const fn is_available(self) -> bool {
        match self {
            Self::Posix(_) => true,
            Self::IoUring(_) => io_uring_unavailability().is_none(),
        }
    }
}

const IO_URING_UNAVAILABLE_FEATURE: &str = "io_uring requires the io-uring crate feature";
const IO_URING_UNAVAILABLE_PLATFORM: &str = "io_uring is unavailable on this platform";
const IO_URING_UNAVAILABLE_ARCHITECTURE: &str = "io_uring is unavailable on this architecture";

/// Names the missing io_uring requirement, if any, for this build and target.
pub(crate) const fn io_uring_unavailability() -> Option<&'static str> {
    if cfg!(not(target_os = "linux")) {
        Some(IO_URING_UNAVAILABLE_PLATFORM)
    } else if cfg!(not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    ))) {
        Some(IO_URING_UNAVAILABLE_ARCHITECTURE)
    } else if cfg!(not(feature = "io-uring")) {
        Some(IO_URING_UNAVAILABLE_FEATURE)
    } else {
        None
    }
}

// Pool topology owns aggregate bounds; each engine config contains one instance's
// execution parameters. Both accounting and construction derive from this shape.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoPoolTopology {
    Posix { workers: usize },
    IoUring(IoUringPoolOptions),
}

impl IoPoolTopology {
    pub const fn read(options: IoEngineOptions) -> Self {
        match options {
            IoEngineOptions::Posix(options) => Self::Posix {
                workers: options.read_workers,
            },
            IoEngineOptions::IoUring(options) => Self::IoUring(options.read),
        }
    }

    pub const fn write(options: IoEngineOptions) -> Self {
        match options {
            IoEngineOptions::Posix(options) => Self::Posix {
                workers: options.write_workers,
            },
            IoEngineOptions::IoUring(options) => Self::IoUring(options.write),
        }
    }

    pub const fn reclaim(options: IoEngineOptions) -> Self {
        match options {
            IoEngineOptions::Posix(options) => Self::Posix {
                workers: options.reclaim_workers,
            },
            IoEngineOptions::IoUring(options) => Self::IoUring(options.reclaim),
        }
    }

    pub const fn engine_count(self) -> usize {
        match self {
            Self::Posix { .. } => 1,
            Self::IoUring(options) => options.rings,
        }
    }

    pub const fn max_in_flight(self) -> usize {
        match self {
            Self::Posix { workers } => workers,
            Self::IoUring(options) => options.max_in_flight,
        }
    }

    pub const fn worker_threads(self) -> usize {
        match self {
            Self::Posix { workers } => workers,
            Self::IoUring(options) => options.rings,
        }
    }

    pub fn extra_memory_bytes(self) -> Option<usize> {
        match self {
            Self::Posix { .. } => Some(0),
            Self::IoUring(options) => {
                io_uring_extra_memory_bytes(options.max_in_flight, options.rings)
            }
        }
    }

    pub const fn engine_config(self, engine: usize) -> IoEngineConfig {
        assert!(
            engine < self.engine_count(),
            "engine index exceeds pool topology"
        );
        match self {
            Self::Posix { workers } => IoEngineConfig::Posix { workers },
            Self::IoUring(options) => {
                let base = options.max_in_flight / options.rings;
                let remainder = options.max_in_flight % options.rings;
                IoEngineConfig::IoUring(IoUringEngineConfig {
                    max_in_flight: base + if engine < remainder { 1 } else { 0 },
                    sq_poll: options.sq_poll,
                    io_poll: options.io_poll,
                })
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoEngineConfig {
    Posix { workers: usize },
    IoUring(IoUringEngineConfig),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoUringEngineConfig {
    pub max_in_flight: usize,
    pub sq_poll: Option<IoUringSqPollOptions>,
    pub io_poll: bool,
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

/// Optional pre-timeout pressure observation and fill admission control.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum FillControlOptions {
    /// No controller, timer, or additional request accounting.
    #[default]
    Disabled,
    /// Report pressure and hypothetical rejections without changing admission.
    Observe(FillLimits),
    /// Reject new fills under pressure. Accepted writes and essential reclaim continue.
    Adaptive(FillLimits),
}

/// Logical fill-rate ceilings shared by [`FillControlOptions::Observe`] and
/// [`FillControlOptions::Adaptive`]. They are instance-wide, not per worker,
/// and are not device bandwidth or IOPS guarantees. Foreground `put` does not
/// consume them; Adaptive uses them to pace non-essential background flush.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FillLimits {
    /// Maximum encoded fill bytes per second, from 640 through 1 TiB/s.
    /// Instance-wide across all shard workers.
    pub max_bytes_per_second: u64,
    /// Maximum fill records per second, from 10 through 4,294,967,295.
    /// Instance-wide across all shard workers.
    pub max_records_per_second: u32,
}

impl FillLimits {
    /// Creates unchecked rate ceilings. [`CacheConfig::new`] validates them.
    pub const fn new(max_bytes_per_second: u64, max_records_per_second: u32) -> Self {
        Self {
            max_bytes_per_second,
            max_records_per_second,
        }
    }
}

/// Process-local resource choices, checked together by [`CacheConfig::new`].
///
/// These values may change across opens. Warm recovery rebinds append shards
/// from recovered Active and Free Regions when the requested topology fits.
/// Fields are unchecked inputs. Configuration construction resolves defaults and
/// checks the complete combination before any files are opened.
/// Start with [`Self::default`] and assign the fields to customize.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct RuntimeOptions {
    /// Independent read, write, and reclaim pools. Defaults to POSIX with 4, 4,
    /// and 1 workers.
    pub io_engine: IoEngineOptions,
    /// Record I/O mode. Defaults to buffered; direct I/O requires supported Linux storage.
    pub io_mode: IoMode,
    /// Admission policy after an L2 candidate has been selected.
    pub read_admission: ReadAdmission,
    /// Deadline for each background reclaim read, including I/O admission.
    /// Defaults to five seconds. Must be positive and representable as an
    /// absolute deadline. Does not change foreground reads or writes. Longer
    /// deadlines can delay shutdown and exhaust free Regions while waiting;
    /// expiration enters `io_recovery_timeout` recovery before cancellation
    /// can fail the cache instance.
    pub reclaim_io_timeout: Duration,
    /// Background timeout recovery budget after the normal admission/completion
    /// deadline. Defaults to `None`: recover until completion or close. Use
    /// `Some(Duration::ZERO)` for immediate cancellation, or a finite duration
    /// to limit recovery. While recovering, new fills return overload; existing
    /// reads and deletes remain available. Original requests retain their slots,
    /// buffers, and Regions and are never resubmitted. Recovery checks completion
    /// and shutdown at one-second intervals. Close interrupts recovery; drain
    /// can wait indefinitely with `None`. Actual I/O errors and invalid
    /// completions still fail the instance.
    pub io_recovery_timeout: Option<Duration>,
    /// Optional pre-timeout fill pressure control. Disabled by default.
    /// Enabled modes reserve bounded worker observations; Adaptive paces
    /// background flush and pauses new fills when outstanding work is old.
    pub fill_control: FillControlOptions,
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
    /// Activity counters, request outcomes, and latency distributions.
    /// Defaults disable collection; health and resource gauges remain available.
    pub stats: StatsOptions,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            io_engine: IoEngineOptions::default(),
            io_mode: IoMode::Buffered,
            read_admission: ReadAdmission::Immediate,
            reclaim_io_timeout: Duration::from_secs(5),
            io_recovery_timeout: None,
            fill_control: FillControlOptions::Disabled,
            append_shards: DEFAULT_APPEND_SHARDS,
            l1_capacity_bytes: DEFAULT_L1_CAPACITY_BYTES,
            l1_eviction_policy: L1EvictionPolicy::Clock,
            managed_memory_limit_bytes: 1024 * 1024 * 1024,
            l1_shards: DEFAULT_L1_SHARDS,
            write_flush_threshold_bytes: MAX_WRITE_FLUSH_THRESHOLD_BYTES,
            stats: StatsOptions::default(),
        }
    }
}

pub const fn read_io_wait_capacity(options: &RuntimeOptions) -> usize {
    match options.read_admission {
        ReadAdmission::Immediate => 0,
        ReadAdmission::Wait { max_waiters, .. } => match max_waiters {
            Some(capacity) => capacity,
            None => IoPoolTopology::read(options.io_engine).max_in_flight(),
        },
    }
}

pub const fn read_io_wait_timeout(options: &RuntimeOptions) -> Duration {
    match options.read_admission {
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
    /// # async fn example() -> Result<(), cache2::Error> {
    /// use cache2::Cache;
    /// use cache2::CacheConfig;
    /// use cache2::IoEngineOptions;
    /// use cache2::PosixIoOptions;
    /// use cache2::RuntimeOptions;
    /// use cache2::StorageOptions;
    /// let mut storage = StorageOptions::new(1024 * 1024 * 1024);
    /// storage.expected_entries = Some(100_000);
    /// let storage = storage.build()?;
    /// let mut io = PosixIoOptions::default();
    /// io.read_workers = 8;
    /// let mut runtime = RuntimeOptions::default();
    /// runtime.io_engine = IoEngineOptions::Posix(io);
    /// runtime.stats.activity_counters = true;
    /// let config = CacheConfig::new(storage, runtime)?;
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
    pub fn new(storage: StorageLayout, mut runtime: RuntimeOptions) -> Result<Self, Error> {
        let build = || -> io::Result<Self> {
            let geometry = storage.geometry;
            let index_slots = storage.index_slots;
            runtime.resolve()?;
            let stats_bytes = Recorder::allocation_bytes(runtime.stats)?;
            let fill_bytes = crate::io::fill_control::FillController::allocation_bytes(
                runtime.fill_control,
                runtime.append_shards as usize
                    + IoPoolTopology::reclaim(runtime.io_engine).max_in_flight(),
            )?;
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
                .and_then(|bytes| bytes.checked_add(stats_bytes))
                .and_then(|bytes| bytes.checked_add(fill_bytes))
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
                io_uring_unavailability().expect("unavailable io_uring names a reason"),
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
        if self.reclaim_io_timeout.is_zero()
            || Instant::now()
                .checked_add(self.reclaim_io_timeout)
                .is_none()
        {
            return Err(invalid_config(
                "reclaim I/O timeout must be positive and fit an absolute deadline",
            ));
        }
        if let Some(recovery_timeout) = self.io_recovery_timeout
            && self
                .reclaim_io_timeout
                .max(Duration::from_secs(5))
                .checked_add(recovery_timeout)
                .and_then(|timeout| Instant::now().checked_add(timeout))
                .is_none()
        {
            return Err(invalid_config(
                "I/O recovery timeout must fit the combined absolute deadline",
            ));
        }
        let read_topology = IoPoolTopology::read(self.io_engine);
        let write_topology = IoPoolTopology::write(self.io_engine);
        let reclaim_topology = IoPoolTopology::reclaim(self.io_engine);
        match self.io_engine {
            IoEngineOptions::Posix(_) => {
                validate_posix_pool("read", read_topology)?;
                validate_posix_pool("write", write_topology)?;
                validate_posix_pool("reclaim", reclaim_topology)?;
            }
            IoEngineOptions::IoUring(options) => {
                validate_io_uring_pool("read", options.read)?;
                validate_io_uring_pool("write", options.write)?;
                validate_io_uring_pool("reclaim", options.reclaim)?;
                if self.io_mode != IoMode::Direct
                    && (options.read.io_poll || options.write.io_poll || options.reclaim.io_poll)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "io_uring IOPOLL requires direct I/O mode",
                    ));
                }
            }
        }
        if reclaim_topology.max_in_flight() > self.append_shards as usize {
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
                return Err(invalid_config(
                    "read wait timeout must be greater than zero and at most five seconds",
                ));
            }
            let capacity = max_waiters.unwrap_or(read_topology.max_in_flight());
            if !(1..=MAX_CONFIG_COUNT).contains(&capacity) {
                return Err(invalid_config("maximum read waiters must be in 1..=65536"));
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
            .ok_or_else(|| invalid_config("L2 capacity does not fit the L1 sizing"))?;
        let expected_entries = index_slots.div_ceil(2).max(1);
        let proportional = (expected_entries as u128)
            .checked_mul(self.l1_capacity_bytes as u128)
            .and_then(|entries| entries.checked_add(l2_capacity - 1))
            .map(|entries| entries / l2_capacity)
            .and_then(|entries| usize::try_from(entries).ok())
            .ok_or_else(|| invalid_config("L1 entry capacity does not fit usize"))?;
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
        let topology_bytes = runtime_topology_memory_bytes(self)
            .ok_or_else(|| invalid_config("runtime topology memory requirements overflow"))?;
        let usable_region = usize::try_from(geometry.region_size)
            .map_err(|_| invalid_config("Region size does not fit the memory requirements"))?;
        let chunk_bytes = usable_region;
        let write_buffer_reservation =
            RegionStaging::reservation_bytes(shard_count, chunk_bytes)
                .ok_or_else(|| invalid_config("write buffer memory requirements overflow"))?;
        let reserved_memory = fixed_bytes
            .checked_add(self.l1_capacity_bytes)
            .and_then(|bytes| bytes.checked_add(topology_bytes))
            .ok_or_else(|| invalid_config("reserved memory requirements overflow"))?;
        let reclaim_buffers = usable_region
            .checked_mul(IoPoolTopology::reclaim(self.io_engine).max_in_flight())
            .ok_or_else(|| invalid_config("reclaim buffer memory requirements overflow"))?;
        let minimum = reserved_memory
            .checked_add(write_buffer_reservation)
            // Every reclaimer permanently owns one Region-sized buffer. Keep
            // one additional maximum-size bounded read for the foreground.
            .and_then(|bytes| bytes.checked_add(reclaim_buffers))
            .and_then(|bytes| bytes.checked_add(usable_region))
            .ok_or_else(|| invalid_config("minimum memory requirements overflow"))?;
        Ok((reserved_memory, minimum))
    }
}

fn runtime_topology_memory_bytes(options: &RuntimeOptions) -> Option<usize> {
    let shard_count = options.append_shards as usize;
    // Reserve one stack per physical I/O thread, one possible shutdown reaper
    // per engine, and every append/reclaim worker.
    let read = IoPoolTopology::read(options.io_engine);
    let write = IoPoolTopology::write(options.io_engine);
    let reclaim = IoPoolTopology::reclaim(options.io_engine);
    let engine_count = read
        .engine_count()
        .checked_add(write.engine_count())?
        .checked_add(reclaim.engine_count())?;
    let stack_count = read
        .worker_threads()
        .checked_add(write.worker_threads())?
        .checked_add(reclaim.worker_threads())?
        .checked_add(engine_count)?
        .checked_add(shard_count)?
        .checked_add(reclaim.max_in_flight())?;
    let stacks = stack_count.checked_mul(CACHE_THREAD_STACK_BYTES)?;
    let read_wait_queue = read_io_wait_capacity(options);
    let queue = write
        .max_in_flight()
        .checked_add(read.max_in_flight())?
        .checked_add(read_wait_queue)?
        .checked_add(reclaim.max_in_flight())?
        .checked_mul(IO_QUEUE_ENTRY_RESERVATION_BYTES)?;
    let uring = [read, write, reclaim]
        .into_iter()
        .try_fold(0_usize, |bytes, pool| {
            bytes.checked_add(pool.extra_memory_bytes()?)
        })?;
    let controls = engine_count
        .checked_add(shard_count)?
        .checked_add(options.l1_shards)?
        .checked_add(reclaim.max_in_flight())?
        .checked_mul(RUNTIME_CONTROL_RESERVATION_BYTES)?;
    let metrics = shard_count.checked_mul(size_of::<ActivityMetrics>())?;
    stacks
        .checked_add(queue)?
        .checked_add(uring)?
        .checked_add(controls)?
        .checked_add(metrics)
}

fn validate_posix_pool(name: &str, topology: IoPoolTopology) -> io::Result<()> {
    if !(1..=MAX_IO_REQUESTS_PER_ENGINE).contains(&topology.max_in_flight()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("POSIX {name} worker count must be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

fn validate_io_uring_pool(name: &str, options: IoUringPoolOptions) -> io::Result<()> {
    if !(1..=MAX_CONFIG_COUNT).contains(&options.rings) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if !(1..=MAX_CONFIG_COUNT).contains(&options.max_in_flight) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} maximum in-flight requests must be in 1..={MAX_CONFIG_COUNT}"),
        ));
    }
    if options.rings > options.max_in_flight {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} ring count must not exceed its in-flight limit"),
        ));
    }
    if options.max_in_flight.div_ceil(options.rings) > MAX_IO_REQUESTS_PER_ENGINE {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("io_uring {name} per-ring depth must not exceed {MAX_IO_REQUESTS_PER_ENGINE}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::StorageOptions;

    #[test]
    fn fill_control_validates_ceilings_and_accounts_observation_memory() {
        let storage = StorageOptions::new(1024 * 1024 * 1024).build().unwrap();
        let base = CacheConfig::new(storage.clone(), RuntimeOptions::default()).unwrap();
        assert_eq!(base.runtime().fill_control, FillControlOptions::Disabled);
        for (bytes, operations) in [(639, 100), ((1 << 40) + 1, 100), (640, 9)] {
            let options = RuntimeOptions {
                fill_control: FillControlOptions::Adaptive(FillLimits::new(bytes, operations)),
                ..RuntimeOptions::default()
            };
            assert_eq!(
                CacheConfig::new(storage.clone(), options)
                    .unwrap_err()
                    .kind(),
                ErrorKind::InvalidInput
            );
        }
        CacheConfig::new(
            storage.clone(),
            RuntimeOptions {
                fill_control: FillControlOptions::Adaptive(FillLimits::new(640, u32::MAX)),
                ..RuntimeOptions::default()
            },
        )
        .unwrap();
        let mut minimum = None;
        for mode in [FillControlOptions::Observe, FillControlOptions::Adaptive] {
            let config = CacheConfig::new(
                storage.clone(),
                RuntimeOptions {
                    fill_control: mode(FillLimits::new(64_000, 100)),
                    ..RuntimeOptions::default()
                },
            )
            .unwrap();
            let extra = config.minimum_memory_bytes() - base.minimum_memory_bytes();
            assert!(extra > 0);
            assert!(extra < CACHE_THREAD_STACK_BYTES);
            if let Some(previous) = minimum {
                assert_eq!(extra, previous);
            }
            minimum = Some(extra);
        }
    }

    #[test]
    fn recovery_timeout_allows_zero_and_rejects_overflow() {
        let storage = crate::StorageOptions::new(1024 * 1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(RuntimeOptions::default().io_recovery_timeout, None);
        for timeout in [Duration::ZERO, Duration::from_secs(30)] {
            let options = RuntimeOptions {
                io_recovery_timeout: Some(timeout),
                ..RuntimeOptions::default()
            };
            CacheConfig::new(storage.clone(), options).unwrap();
        }
        let options = RuntimeOptions {
            io_recovery_timeout: Some(Duration::MAX),
            ..RuntimeOptions::default()
        };
        assert_eq!(
            CacheConfig::new(storage, options).unwrap_err().kind(),
            crate::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn reclaim_timeout_is_validated_at_config_construction() {
        let storage = StorageOptions::new(1024 * 1024 * 1024).build().unwrap();
        assert_eq!(
            RuntimeOptions::default().reclaim_io_timeout,
            Duration::from_secs(5)
        );
        for timeout in [Duration::ZERO, Duration::MAX] {
            let error = CacheConfig::new(
                storage.clone(),
                RuntimeOptions {
                    reclaim_io_timeout: timeout,
                    ..RuntimeOptions::default()
                },
            )
            .unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidInput);
        }
        CacheConfig::new(
            storage,
            RuntimeOptions {
                reclaim_io_timeout: Duration::from_secs(30),
                ..RuntimeOptions::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn optional_read_wait_queue_is_memory_accounted() {
        let base = RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions {
                read_workers: 7,
                write_workers: 4,
                reclaim_workers: 1,
            }),
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
            io_engine: IoEngineOptions::Posix(PosixIoOptions {
                read_workers: 4,
                write_workers: 4,
                reclaim_workers: 2,
            }),
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
        let posix = IoEngineOptions::Posix(PosixIoOptions {
            read_workers: 7,
            write_workers: 5,
            reclaim_workers: 2,
        });
        assert_eq!(
            IoPoolTopology::read(posix),
            IoPoolTopology::Posix { workers: 7 }
        );

        let io_uring = IoEngineOptions::IoUring(IoUringOptions {
            read: IoUringPoolOptions {
                rings: 3,
                max_in_flight: 8,
                ..IoUringPoolOptions::default()
            },
            write: IoUringPoolOptions {
                rings: 2,
                max_in_flight: 5,
                ..IoUringPoolOptions::default()
            },
            reclaim: IoUringPoolOptions {
                rings: 1,
                max_in_flight: 2,
                ..IoUringPoolOptions::default()
            },
        });
        let read = IoPoolTopology::read(io_uring);
        assert_eq!(read.engine_count(), 3);
        assert_eq!(read.max_in_flight(), 8);
        assert_eq!(read.worker_threads(), 3);
        let depths: Vec<_> = (0..read.engine_count())
            .map(|engine| match read.engine_config(engine) {
                IoEngineConfig::IoUring(config) => config.max_in_flight,
                IoEngineConfig::Posix { .. } => panic!("expected an io_uring engine"),
            })
            .collect();
        assert_eq!(depths, [3, 3, 2]);
        assert_eq!(depths.iter().sum::<usize>(), read.max_in_flight());
    }

    #[test]
    fn io_uring_depth_reserves_more_than_common_request_bookkeeping() {
        let pool = IoUringPoolOptions {
            rings: 1,
            max_in_flight: 1,
            ..IoUringPoolOptions::default()
        };
        let shallow = RuntimeOptions {
            io_engine: IoEngineOptions::IoUring(IoUringOptions {
                read: pool,
                write: pool,
                reclaim: pool,
            }),
            ..RuntimeOptions::default()
        };
        let deep = RuntimeOptions {
            io_engine: IoEngineOptions::IoUring(IoUringOptions {
                read: IoUringPoolOptions {
                    rings: 1,
                    max_in_flight: MAX_IO_REQUESTS_PER_ENGINE,
                    ..IoUringPoolOptions::default()
                },
                write: pool,
                reclaim: pool,
            }),
            ..shallow.clone()
        };
        let growth = runtime_topology_memory_bytes(&deep).unwrap()
            - runtime_topology_memory_bytes(&shallow).unwrap();
        assert!(growth > (MAX_IO_REQUESTS_PER_ENGINE - 1) * IO_QUEUE_ENTRY_RESERVATION_BYTES);
    }

    #[test]
    fn io_uring_pool_validation_bounds_rings_and_depth() {
        validate_io_uring_pool(
            "read",
            IoUringPoolOptions {
                rings: 3,
                max_in_flight: 8,
                ..IoUringPoolOptions::default()
            },
        )
        .unwrap();

        for config in [
            IoUringPoolOptions {
                rings: 0,
                max_in_flight: 8,
                ..IoUringPoolOptions::default()
            },
            IoUringPoolOptions {
                rings: 1,
                max_in_flight: 0,
                ..IoUringPoolOptions::default()
            },
            IoUringPoolOptions {
                rings: 3,
                max_in_flight: 2,
                ..IoUringPoolOptions::default()
            },
            IoUringPoolOptions {
                rings: 1,
                max_in_flight: MAX_IO_REQUESTS_PER_ENGINE + 1,
                ..IoUringPoolOptions::default()
            },
        ] {
            assert_eq!(
                validate_io_uring_pool("read", config).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn io_uring_unavailability_names_the_missing_requirement() {
        #[cfg(not(target_os = "linux"))]
        assert_eq!(
            io_uring_unavailability(),
            Some(IO_URING_UNAVAILABLE_PLATFORM)
        );
        #[cfg(all(
            target_os = "linux",
            not(any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            ))
        ))]
        assert_eq!(
            io_uring_unavailability(),
            Some(IO_URING_UNAVAILABLE_ARCHITECTURE)
        );
        #[cfg(all(
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            ),
            not(feature = "io-uring")
        ))]
        assert_eq!(
            io_uring_unavailability(),
            Some(IO_URING_UNAVAILABLE_FEATURE)
        );
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
        assert_eq!(io_uring_unavailability(), None);
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
        let pool = IoUringPoolOptions {
            io_poll: true,
            ..IoUringPoolOptions::default()
        };
        let mut config = RuntimeOptions {
            io_engine: IoEngineOptions::IoUring(IoUringOptions {
                read: pool,
                write: IoUringPoolOptions::default(),
                reclaim: IoUringPoolOptions {
                    rings: 1,
                    max_in_flight: 1,
                    ..IoUringPoolOptions::default()
                },
            }),
            ..RuntimeOptions::default()
        };

        assert_eq!(
            config.resolve().unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
    }
}
