//! Bounded request admission and aligned record buffers.
//!
//! Read and write admission are independently bounded so write pressure cannot
//! consume the resources reserved for cache hits.

#[cfg(not(target_os = "linux"))]
use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(crate) const BUFFER_ALIGNMENT: usize = 4096;
/// Every cache-owned thread uses an explicit stack reservation so configured
/// topology cannot inherit an environment-dependent `RUST_MIN_STACK` value.
pub(crate) const CACHE_THREAD_STACK_BYTES: usize = 512 * 1024;
pub(crate) const MAX_QUEUE_DEPTH: usize = 65_536;
pub(crate) const MAX_BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);
#[cfg(test)]
const READ_BUFFER_SLOTS: usize = 2;
#[cfg(test)]
const WRITE_BUFFER_SLOTS: usize = 2;
#[cfg(test)]
pub(crate) const CONTROL_BUFFERS_PER_REQUEST: usize = 2;
pub(crate) const METADATA_BUFFER_SLOTS: usize = 1;

/// Behavior when a bounded submission gate or buffer pool is full.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BackpressurePolicy {
    /// Reject immediately without changing cache state or disk contents.
    #[default]
    Reject,
    /// Block the calling thread until capacity becomes available.
    Block,
    /// Block the calling thread for at most the supplied duration.
    Timeout(Duration),
}

/// The bounded resource that prevented an internal operation from admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OverloadReason {
    /// The read submission gate is at its configured depth.
    ReadQueueFull,
    /// The write or reserved control submission gate is at capacity.
    WriteQueueFull,
    /// Every read scratch buffer is leased or a read allocation failed.
    ReadBufferUnavailable,
    /// Every write/control scratch buffer is leased or allocation failed.
    WriteBufferUnavailable,
    /// Read admission exceeded the configured timeout.
    ReadTimeout,
    /// Write/control admission exceeded the configured timeout.
    WriteTimeout,
}

impl fmt::Display for OverloadReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ReadQueueFull => "read submission queue is full",
            Self::WriteQueueFull => "write submission queue is full",
            Self::ReadBufferUnavailable => "read buffer pool is exhausted",
            Self::WriteBufferUnavailable => "write buffer pool is exhausted",
            Self::ReadTimeout => "read backpressure timeout expired",
            Self::WriteTimeout => "write backpressure timeout expired",
        })
    }
}

pub(crate) struct ResourceLimits {
    pub(crate) memory_budget_bytes: usize,
    pub(crate) base_memory_bytes: usize,
    pub(crate) max_buffer_bytes: usize,
    pub(crate) read_queue_depth: usize,
    pub(crate) write_queue_depth: usize,
    pub(crate) read_buffer_slots: usize,
    #[cfg(test)]
    pub(crate) write_buffer_slots: usize,
    #[cfg(test)]
    pub(crate) control_concurrency: usize,
    pub(crate) backpressure: BackpressurePolicy,
}

#[derive(Debug)]
pub(crate) enum ResourceBuildError {
    Invalid(&'static str),
    Allocation,
}

impl fmt::Display for ResourceBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Allocation => formatter.write_str("resource bookkeeping cannot be allocated"),
        }
    }
}

pub(crate) struct ResourceController {
    memory: Arc<MemoryTracker>,
    read_gate: Arc<RequestGate>,
    write_gate: Arc<RequestGate>,
    #[cfg(test)]
    control_gate: Arc<RequestGate>,
    read_pool: Arc<BufferPool>,
    #[cfg(test)]
    write_pool: Arc<BufferPool>,
    #[cfg(test)]
    control_pool: Arc<BufferPool>,
    metadata_pool: Arc<BufferPool>,
    backpressure: BackpressurePolicy,
}

/// A fixed runtime allocation charged to the same hard memory budget as the
/// request pools. The owner keeps this guard for exactly as long as the
/// associated bounded structure exists.
pub(crate) struct RuntimeMemoryReservation {
    memory: Arc<MemoryTracker>,
    bytes: usize,
}

impl Drop for RuntimeMemoryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

impl ResourceController {
    pub(crate) fn try_new(limits: ResourceLimits) -> Result<Self, ResourceBuildError> {
        if limits.read_queue_depth == 0 || limits.write_queue_depth == 0 {
            return Err(ResourceBuildError::Invalid(
                "read/write queue depth must be at least 1",
            ));
        }
        if limits.read_queue_depth > MAX_QUEUE_DEPTH || limits.write_queue_depth > MAX_QUEUE_DEPTH {
            return Err(ResourceBuildError::Invalid(
                "read/write queue depth exceeds its hard limit",
            ));
        }
        if limits.read_buffer_slots == 0 {
            return Err(ResourceBuildError::Invalid(
                "read buffer slots must be at least 1",
            ));
        }
        if limits.read_buffer_slots > MAX_QUEUE_DEPTH {
            return Err(ResourceBuildError::Invalid(
                "read buffer slots exceed their hard limit",
            ));
        }
        #[cfg(test)]
        if limits.write_buffer_slots == 0 || limits.write_buffer_slots > MAX_QUEUE_DEPTH {
            return Err(ResourceBuildError::Invalid(
                "write buffer slots must be within their hard limit",
            ));
        }
        #[cfg(test)]
        if limits.control_concurrency == 0 || limits.control_concurrency > MAX_QUEUE_DEPTH {
            return Err(ResourceBuildError::Invalid(
                "control concurrency must be within the queue depth hard limit",
            ));
        }
        #[cfg(test)]
        let control_buffer_slots = limits
            .control_concurrency
            .checked_mul(CONTROL_BUFFERS_PER_REQUEST)
            .filter(|slots| *slots <= MAX_QUEUE_DEPTH)
            .ok_or(ResourceBuildError::Invalid(
                "control buffer slots exceed their hard limit",
            ))?;
        if limits.max_buffer_bytes == 0 || limits.max_buffer_bytes % BUFFER_ALIGNMENT != 0 {
            return Err(ResourceBuildError::Invalid(
                "maximum buffer size must be a non-zero 4096-byte multiple",
            ));
        }
        if let BackpressurePolicy::Timeout(duration) = limits.backpressure {
            if duration > MAX_BACKPRESSURE_TIMEOUT {
                return Err(ResourceBuildError::Invalid(
                    "backpressure timeout must not exceed 24 hours",
                ));
            }
        }
        if limits.base_memory_bytes > limits.memory_budget_bytes {
            return Err(ResourceBuildError::Invalid(
                "memory budget cannot hold the cache's base memory",
            ));
        }

        let memory = Arc::new(MemoryTracker::new(
            limits.memory_budget_bytes,
            limits.base_memory_bytes,
        ));
        Ok(Self {
            read_gate: Arc::new(RequestGate::new(limits.read_queue_depth)),
            write_gate: Arc::new(RequestGate::new(limits.write_queue_depth)),
            #[cfg(test)]
            control_gate: Arc::new(RequestGate::new(limits.control_concurrency)),
            read_pool: Arc::new(BufferPool::try_new(
                limits.read_buffer_slots,
                limits.max_buffer_bytes,
                Arc::clone(&memory),
            )?),
            #[cfg(test)]
            write_pool: Arc::new(BufferPool::try_new(
                limits.write_buffer_slots,
                limits.max_buffer_bytes,
                Arc::clone(&memory),
            )?),
            #[cfg(test)]
            control_pool: Arc::new(BufferPool::try_new(
                control_buffer_slots,
                limits.max_buffer_bytes,
                Arc::clone(&memory),
            )?),
            metadata_pool: Arc::new(BufferPool::try_new(
                METADATA_BUFFER_SLOTS,
                limits.max_buffer_bytes,
                Arc::clone(&memory),
            )?),
            backpressure: limits.backpressure,
            memory,
        })
    }

    pub(crate) fn reserve_runtime_memory(
        &self,
        bytes: usize,
    ) -> Result<RuntimeMemoryReservation, ResourceBuildError> {
        if !self.memory.try_reserve(bytes) {
            return Err(ResourceBuildError::Allocation);
        }
        Ok(RuntimeMemoryReservation {
            memory: Arc::clone(&self.memory),
            bytes,
        })
    }

    pub(crate) fn begin_read(&self) -> Result<DataResources, OverloadReason> {
        self.acquire_read(WaitMode::new(self.backpressure))
    }

    fn acquire_read(&self, wait: WaitMode) -> Result<DataResources, OverloadReason> {
        let queue = self
            .read_gate
            .enter(wait)
            .map_err(|failure| match failure {
                WaitFailure::Full => OverloadReason::ReadQueueFull,
                WaitFailure::TimedOut => OverloadReason::ReadTimeout,
            })?;
        let buffer = self
            .read_pool
            .acquire(wait)
            .map_err(|failure| match failure {
                WaitFailure::Full => OverloadReason::ReadBufferUnavailable,
                WaitFailure::TimedOut => OverloadReason::ReadTimeout,
            })?;
        Ok(DataResources {
            _queue: queue,
            buffer,
        })
    }

    #[cfg(test)]
    pub(crate) fn begin_write(&self) -> Result<DataResources, OverloadReason> {
        self.acquire_write(WaitMode::new(self.backpressure))
    }

    /// Admit a write whose payload already lives in engine-owned storage.
    ///
    /// RegionStore encodes directly into its fixed per-shard staging
    /// buffers, so leasing another general-purpose write buffer here would
    /// double-account memory and reduce useful concurrency.
    pub(crate) fn begin_write_permit(&self) -> Result<QueuePermit, OverloadReason> {
        self.acquire_write_permit(WaitMode::new(self.backpressure))
    }

    #[cfg(test)]
    fn acquire_write(&self, wait: WaitMode) -> Result<DataResources, OverloadReason> {
        let queue = self.acquire_write_permit(wait)?;
        let buffer = self
            .write_pool
            .acquire(wait)
            .map_err(|failure| match failure {
                WaitFailure::Full => OverloadReason::WriteBufferUnavailable,
                WaitFailure::TimedOut => OverloadReason::WriteTimeout,
            })?;
        Ok(DataResources {
            _queue: queue,
            buffer,
        })
    }

    fn acquire_write_permit(&self, wait: WaitMode) -> Result<QueuePermit, OverloadReason> {
        self.write_gate
            .enter(wait)
            .map_err(|failure| match failure {
                WaitFailure::Full => OverloadReason::WriteQueueFull,
                WaitFailure::TimedOut => OverloadReason::WriteTimeout,
            })
    }

    /// Lease the Region manager's dedicated metadata buffer.
    ///
    /// This pool is independent of foreground admission so an accepted write
    /// can always finish its region-header or superblock publication. Region
    /// rotation is globally ordered, so one blocking slot is sufficient even
    /// when several data shards are active.
    pub(crate) fn metadata_buffer(&self) -> Result<BufferLease, OverloadReason> {
        self.metadata_pool
            .acquire(WaitMode::Block)
            .map_err(|_| OverloadReason::WriteBufferUnavailable)
    }

    pub(crate) fn managed_memory_snapshot(&self) -> ManagedMemorySnapshot {
        let current_bytes = self.memory.current.load(Ordering::Relaxed);
        ManagedMemorySnapshot {
            budget_bytes: self.memory.budget,
            current_bytes,
            peak_bytes: self.memory.peak.load(Ordering::Relaxed).max(current_bytes),
        }
    }

    pub(crate) fn runtime_snapshot(&self) -> ResourceRuntimeSnapshot {
        ResourceRuntimeSnapshot {
            read_requests_in_flight: usize_to_u64(self.read_gate.current.load(Ordering::Relaxed)),
            read_requests_peak: usize_to_u64(self.read_gate.peak.load(Ordering::Relaxed)),
            read_rejections: self.read_gate.rejections.load(Ordering::Relaxed),
            read_wait_ns: self.read_gate.wait_ns.load(Ordering::Relaxed),
            write_requests_in_flight: usize_to_u64(self.write_gate.current.load(Ordering::Relaxed)),
            write_requests_peak: usize_to_u64(self.write_gate.peak.load(Ordering::Relaxed)),
            write_rejections: self.write_gate.rejections.load(Ordering::Relaxed),
            write_wait_ns: self.write_gate.wait_ns.load(Ordering::Relaxed),
            read_buffers_in_use: usize_to_u64(self.read_pool.in_use.load(Ordering::Relaxed)),
            read_buffers_peak: usize_to_u64(self.read_pool.peak.load(Ordering::Relaxed)),
            read_buffer_rejections: self.read_pool.rejections.load(Ordering::Relaxed),
            read_buffer_wait_ns: self.read_pool.wait_ns.load(Ordering::Relaxed),
            metadata_buffers_in_use: usize_to_u64(
                self.metadata_pool.in_use.load(Ordering::Relaxed),
            ),
            metadata_buffers_peak: usize_to_u64(self.metadata_pool.peak.load(Ordering::Relaxed)),
            metadata_buffer_rejections: self.metadata_pool.rejections.load(Ordering::Relaxed),
            metadata_buffer_wait_ns: self.metadata_pool.wait_ns.load(Ordering::Relaxed),
        }
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> ResourceSnapshot {
        let read_queue_depth = self.read_gate.current.load(Ordering::Relaxed);
        let write_queue_depth = self.write_gate.current.load(Ordering::Relaxed);
        let read_buffers_in_use = self.read_pool.in_use.load(Ordering::Relaxed);
        let write_buffers_in_use = self.write_pool.in_use.load(Ordering::Relaxed);
        let memory_used_bytes = self.memory.current.load(Ordering::Relaxed);
        let read_queue_wait_ns = self.read_gate.wait_ns.load(Ordering::Relaxed);
        let write_queue_wait_ns = self.write_gate.wait_ns.load(Ordering::Relaxed);
        let control_queue_wait_ns = self.control_gate.wait_ns.load(Ordering::Relaxed);
        let read_buffer_wait_ns = self.read_pool.wait_ns.load(Ordering::Relaxed);
        let write_buffer_wait_ns = self.write_pool.wait_ns.load(Ordering::Relaxed);
        let control_buffer_wait_ns = self.control_pool.wait_ns.load(Ordering::Relaxed);
        let metadata_buffer_wait_ns = self.metadata_pool.wait_ns.load(Ordering::Relaxed);
        ResourceSnapshot {
            read_queue_depth: usize_to_u64(read_queue_depth),
            write_queue_depth: usize_to_u64(write_queue_depth),
            read_buffers_in_use: usize_to_u64(read_buffers_in_use),
            write_buffers_in_use: usize_to_u64(write_buffers_in_use),
            queue_rejections: self
                .read_gate
                .rejections
                .load(Ordering::Relaxed)
                .saturating_add(self.write_gate.rejections.load(Ordering::Relaxed))
                .saturating_add(self.control_gate.rejections.load(Ordering::Relaxed)),
            buffer_rejections: self
                .read_pool
                .rejections
                .load(Ordering::Relaxed)
                .saturating_add(self.write_pool.rejections.load(Ordering::Relaxed))
                .saturating_add(self.control_pool.rejections.load(Ordering::Relaxed))
                .saturating_add(self.metadata_pool.rejections.load(Ordering::Relaxed)),
            read_queue_wait_ns,
            write_queue_wait_ns,
            control_queue_wait_ns,
            read_buffer_wait_ns,
            write_buffer_wait_ns,
            control_buffer_wait_ns,
            metadata_buffer_wait_ns,
            backpressure_wait_ns: read_queue_wait_ns
                .saturating_add(write_queue_wait_ns)
                .saturating_add(control_queue_wait_ns)
                .saturating_add(read_buffer_wait_ns)
                .saturating_add(write_buffer_wait_ns)
                .saturating_add(control_buffer_wait_ns)
                .saturating_add(metadata_buffer_wait_ns),
            memory_budget_bytes: usize_to_u64(self.memory.budget),
            memory_used_bytes: usize_to_u64(memory_used_bytes),
            memory_peak_bytes: usize_to_u64(
                self.memory
                    .peak
                    .load(Ordering::Relaxed)
                    .max(memory_used_bytes),
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManagedMemorySnapshot {
    pub(crate) budget_bytes: usize,
    pub(crate) current_bytes: usize,
    pub(crate) peak_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResourceRuntimeSnapshot {
    pub(crate) read_requests_in_flight: u64,
    pub(crate) read_requests_peak: u64,
    pub(crate) read_rejections: u64,
    pub(crate) read_wait_ns: u64,
    pub(crate) write_requests_in_flight: u64,
    pub(crate) write_requests_peak: u64,
    pub(crate) write_rejections: u64,
    pub(crate) write_wait_ns: u64,
    pub(crate) read_buffers_in_use: u64,
    pub(crate) read_buffers_peak: u64,
    pub(crate) read_buffer_rejections: u64,
    pub(crate) read_buffer_wait_ns: u64,
    pub(crate) metadata_buffers_in_use: u64,
    pub(crate) metadata_buffers_peak: u64,
    pub(crate) metadata_buffer_rejections: u64,
    pub(crate) metadata_buffer_wait_ns: u64,
}

pub(crate) struct DataResources {
    _queue: QueuePermit,
    pub(crate) buffer: BufferLease,
}

impl DataResources {
    pub(crate) fn into_parts(self) -> (QueuePermit, BufferLease) {
        (self._queue, self.buffer)
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ResourceSnapshot {
    pub(crate) read_queue_depth: u64,
    pub(crate) write_queue_depth: u64,
    pub(crate) read_buffers_in_use: u64,
    pub(crate) write_buffers_in_use: u64,
    pub(crate) queue_rejections: u64,
    pub(crate) buffer_rejections: u64,
    pub(crate) read_queue_wait_ns: u64,
    pub(crate) write_queue_wait_ns: u64,
    pub(crate) control_queue_wait_ns: u64,
    pub(crate) read_buffer_wait_ns: u64,
    pub(crate) write_buffer_wait_ns: u64,
    pub(crate) control_buffer_wait_ns: u64,
    pub(crate) metadata_buffer_wait_ns: u64,
    pub(crate) backpressure_wait_ns: u64,
    pub(crate) memory_budget_bytes: u64,
    pub(crate) memory_used_bytes: u64,
    pub(crate) memory_peak_bytes: u64,
}

#[derive(Clone, Copy)]
enum WaitMode {
    Reject,
    Block,
    Deadline(Instant),
}

impl WaitMode {
    fn new(policy: BackpressurePolicy) -> Self {
        match policy {
            BackpressurePolicy::Reject => Self::Reject,
            BackpressurePolicy::Block => Self::Block,
            BackpressurePolicy::Timeout(duration) => {
                let now = Instant::now();
                // The configuration is capped at 24 hours. Keep construction
                // fallible in spirit even on an unusual Instant representation:
                // an unrepresentable deadline expires immediately, never panics.
                Self::Deadline(now.checked_add(duration).unwrap_or(now))
            }
        }
    }
}

#[derive(Clone, Copy)]
enum WaitFailure {
    Full,
    TimedOut,
}

struct GateState {
    current: usize,
}

struct RequestGate {
    limit: usize,
    state: Mutex<GateState>,
    available: Condvar,
    current: AtomicUsize,
    peak: AtomicUsize,
    rejections: AtomicU64,
    wait_ns: AtomicU64,
}

impl RequestGate {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(GateState { current: 0 }),
            available: Condvar::new(),
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
            wait_ns: AtomicU64::new(0),
        }
    }

    fn enter(self: &Arc<Self>, wait: WaitMode) -> Result<QueuePermit, WaitFailure> {
        let mut wait_started = None;
        let mut state = lock_unpoisoned(&self.state);
        while state.current == self.limit {
            if wait_started.is_none() && !matches!(wait, WaitMode::Reject) {
                wait_started = Some(Instant::now());
            }
            state = match wait {
                WaitMode::Reject => {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    return Err(WaitFailure::Full);
                }
                WaitMode::Block => wait_unpoisoned(&self.available, state),
                WaitMode::Deadline(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WaitFailure::TimedOut);
                    };
                    let (guard, timed_out) =
                        wait_timeout_unpoisoned(&self.available, state, remaining);
                    if timed_out && guard.current == self.limit {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WaitFailure::TimedOut);
                    }
                    guard
                }
            };
        }
        state.current += 1;
        let current = state.current;
        self.current.store(current, Ordering::Relaxed);
        update_peak(&self.peak, current);
        add_wait_duration(&self.wait_ns, wait_started);
        Ok(QueuePermit {
            gate: Arc::clone(self),
        })
    }

    fn release(&self) {
        let mut state = lock_unpoisoned(&self.state);
        debug_assert!(state.current != 0);
        if state.current != 0 {
            state.current -= 1;
        }
        self.current.store(state.current, Ordering::Relaxed);
        self.available.notify_one();
    }
}

pub(crate) struct QueuePermit {
    gate: Arc<RequestGate>,
}

impl Drop for QueuePermit {
    fn drop(&mut self) {
        self.gate.release();
    }
}

struct PoolState {
    free: Vec<AlignedBuffer>,
}

struct BufferPool {
    max_buffer_bytes: usize,
    state: Mutex<PoolState>,
    available: Condvar,
    memory: Arc<MemoryTracker>,
    in_use: AtomicUsize,
    peak: AtomicUsize,
    rejections: AtomicU64,
    wait_ns: AtomicU64,
}

/// A fixed-slot pool for engines whose I/O unit has one constant size.
///
/// Unlike `ResourceController`, this wrapper has no request gate and does not
/// create separate read/write/control pools. Every slot is allocated eagerly,
/// so opening the engine either establishes the complete aligned memory bound
/// or fails before the cache file is modified.
pub(crate) struct DedicatedBufferPool {
    inner: Arc<BufferPool>,
    buffer_size: usize,
    closed: AtomicBool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct DedicatedBufferPoolSnapshot {
    pub(crate) in_use: u64,
    pub(crate) rejections: u64,
}

impl DedicatedBufferPool {
    pub(crate) fn try_new(slots: usize, buffer_size: usize) -> Result<Self, ResourceBuildError> {
        if slots == 0 || slots > MAX_QUEUE_DEPTH {
            return Err(ResourceBuildError::Invalid(
                "dedicated buffer slots must be within the hard queue limit",
            ));
        }
        if buffer_size == 0 || buffer_size % BUFFER_ALIGNMENT != 0 {
            return Err(ResourceBuildError::Invalid(
                "dedicated buffer size must be a non-zero 4096-byte multiple",
            ));
        }
        let budget = slots
            .checked_mul(buffer_size)
            .ok_or(ResourceBuildError::Invalid(
                "dedicated buffer memory size overflow",
            ))?;
        let memory = Arc::new(MemoryTracker::new(budget, 0));
        let inner = Arc::new(BufferPool::try_new(
            slots,
            buffer_size,
            Arc::clone(&memory),
        )?);

        // Hold every lease while preparing it so allocation cannot repeatedly
        // reuse one slot and leave later runtime allocation fallible.
        let mut prepared = Vec::new();
        prepared
            .try_reserve_exact(slots)
            .map_err(|_| ResourceBuildError::Allocation)?;
        for _ in 0..slots {
            let mut lease = inner
                .acquire(WaitMode::Block)
                .map_err(|_| ResourceBuildError::Allocation)?;
            lease
                .prepare(buffer_size)
                .map_err(|_| ResourceBuildError::Allocation)?;
            prepared.push(lease);
        }
        drop(prepared);

        Ok(Self {
            inner,
            buffer_size,
            closed: AtomicBool::new(false),
        })
    }

    pub(crate) fn acquire(&self) -> Option<BufferLease> {
        let mut wait_started = None;
        let mut state = lock_unpoisoned(&self.inner.state);
        while state.free.is_empty() && !self.closed.load(Ordering::Acquire) {
            wait_started.get_or_insert_with(Instant::now);
            state = wait_unpoisoned(&self.inner.available, state);
        }
        if self.closed.load(Ordering::Acquire) {
            self.inner.rejections.fetch_add(1, Ordering::Relaxed);
            add_wait_duration(&self.inner.wait_ns, wait_started);
            return None;
        }
        let buffer = state
            .free
            .pop()
            .expect("dedicated pool wake requires a free buffer or closure");
        let current = self.inner.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.inner.peak, current);
        add_wait_duration(&self.inner.wait_ns, wait_started);
        drop(state);
        let mut lease = BufferLease {
            pool: Arc::clone(&self.inner),
            buffer: Some(buffer),
        };
        lease
            .prepare(self.buffer_size)
            .expect("dedicated buffers are fully allocated during construction");
        Some(lease)
    }

    /// Permanently stop admission and wake every blocked caller. This is used
    /// when a fatal asynchronous driver error quarantines one or more buffers:
    /// no caller may remain asleep waiting for a slot that cannot return.
    pub(crate) fn close(&self) {
        // The condition transition and notification use the same mutex as
        // `acquire`; otherwise close could race between its condition check
        // and the atomic sleep inside `Condvar::wait` and lose the wakeup.
        let _state = lock_unpoisoned(&self.inner.state);
        self.closed.store(true, Ordering::Release);
        self.inner.available.notify_all();
    }

    #[cfg(test)]
    pub(crate) fn snapshot(&self) -> DedicatedBufferPoolSnapshot {
        DedicatedBufferPoolSnapshot {
            in_use: self.inner.in_use.load(Ordering::Relaxed) as u64,
            rejections: self.inner.rejections.load(Ordering::Relaxed),
        }
    }
}

impl BufferPool {
    fn try_new(
        slots: usize,
        max_buffer_bytes: usize,
        memory: Arc<MemoryTracker>,
    ) -> Result<Self, ResourceBuildError> {
        let mut free = Vec::new();
        free.try_reserve_exact(slots)
            .map_err(|_| ResourceBuildError::Allocation)?;
        free.resize_with(slots, AlignedBuffer::empty);
        Ok(Self {
            max_buffer_bytes,
            state: Mutex::new(PoolState { free }),
            available: Condvar::new(),
            memory,
            in_use: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
            wait_ns: AtomicU64::new(0),
        })
    }

    fn acquire(self: &Arc<Self>, wait: WaitMode) -> Result<BufferLease, WaitFailure> {
        let mut wait_started = None;
        let mut state = lock_unpoisoned(&self.state);
        while state.free.is_empty() {
            if wait_started.is_none() && !matches!(wait, WaitMode::Reject) {
                wait_started = Some(Instant::now());
            }
            state = match wait {
                WaitMode::Reject => {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    return Err(WaitFailure::Full);
                }
                WaitMode::Block => wait_unpoisoned(&self.available, state),
                WaitMode::Deadline(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WaitFailure::TimedOut);
                    };
                    let (guard, timed_out) =
                        wait_timeout_unpoisoned(&self.available, state, remaining);
                    if timed_out && guard.free.is_empty() {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WaitFailure::TimedOut);
                    }
                    guard
                }
            };
        }
        let buffer = state.free.pop().expect("checked non-empty buffer pool");
        let current = self.in_use.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.peak, current);
        add_wait_duration(&self.wait_ns, wait_started);
        Ok(BufferLease {
            pool: Arc::clone(self),
            buffer: Some(buffer),
        })
    }

    fn release(&self, buffer: AlignedBuffer) {
        let mut state = lock_unpoisoned(&self.state);
        state.free.push(buffer);
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        self.available.notify_one();
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let state = lock_unpoisoned(&self.state);
        let allocated = state
            .free
            .iter()
            .map(|buffer| buffer.capacity)
            .sum::<usize>();
        self.memory.release(allocated);
    }
}

pub(crate) struct BufferLease {
    pool: Arc<BufferPool>,
    buffer: Option<AlignedBuffer>,
}

impl BufferLease {
    pub(crate) fn prepare(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if buffer
            .ensure_capacity(length, self.pool.max_buffer_bytes, &self.pool.memory)
            .is_err()
        {
            self.pool.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(());
        }
        // Callers encode complete records. Clearing here also fixes padding and
        // prevents bytes from a prior key/value escaping into a later write.
        let slice = buffer.prefix_mut(length);
        slice.fill(0);
        Ok(slice)
    }

    /// Grow the leased buffer without clearing bytes already in the buffer.
    ///
    /// Fresh capacity is zero-initialized by the allocator, but callers must
    /// initialize the exact appended range they expose because this lease does
    /// not track a separate prepared length.
    pub(crate) fn grow_preserving(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if buffer
            .ensure_capacity(length, self.pool.max_buffer_bytes, &self.pool.memory)
            .is_err()
        {
            self.pool.rejections.fetch_add(1, Ordering::Relaxed);
            return Err(());
        }
        Ok(buffer.prefix_mut(length))
    }

    pub(crate) fn prepared(&self, length: usize) -> Result<&[u8], ()> {
        let buffer = self.buffer.as_ref().ok_or(())?;
        if length > buffer.capacity {
            return Err(());
        }
        // SAFETY: the allocation holds `capacity` initialized bytes and the
        // returned shared slice cannot mutate the exclusively leased buffer.
        Ok(unsafe { std::slice::from_raw_parts(buffer.ptr.as_ptr(), length) })
    }

    pub(crate) fn prepared_mut(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = self.buffer.as_mut().ok_or(())?;
        if length > buffer.capacity {
            return Err(());
        }
        Ok(buffer.prefix_mut(length))
    }

    #[cfg(test)]
    fn address(&self) -> usize {
        self.buffer
            .as_ref()
            .expect("buffer lease owns a buffer")
            .ptr
            .as_ptr() as usize
    }
}

impl Drop for BufferLease {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.release(buffer);
        }
    }
}

struct AlignedBuffer {
    ptr: NonNull<u8>,
    capacity: usize,
}

// SAFETY: ownership of the allocation moves with this value. Bytes are only
// exposed through `&mut self`, and a buffer is leased to at most one thread.
unsafe impl Send for AlignedBuffer {}

impl AlignedBuffer {
    const fn empty() -> Self {
        Self {
            ptr: NonNull::dangling(),
            capacity: 0,
        }
    }

    fn ensure_capacity(
        &mut self,
        required: usize,
        maximum: usize,
        memory: &MemoryTracker,
    ) -> Result<(), ()> {
        if required > maximum {
            return Err(());
        }
        let target = align_up(required, BUFFER_ALIGNMENT).ok_or(())?;
        if target <= self.capacity {
            return Ok(());
        }
        if target > isize::MAX as usize {
            return Err(());
        }

        let old_capacity = self.capacity;
        // Growth briefly owns both mappings while preserving the prefix. Count
        // that physical overlap against the hard budget, then release the old
        // charge after the copy. This keeps real allocation peak bounded too,
        // not only the final logical capacity.
        if !memory.try_reserve(target) {
            return Err(());
        }
        let Some(ptr) = allocate_buffer(target) else {
            memory.release(target);
            return Err(());
        };

        if old_capacity != 0 {
            // SAFETY: both allocations are valid for `old_capacity` bytes and
            // cannot overlap. The old allocation remains live until the copy
            // completes, so a failed growth never destroys buffered data.
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), old_capacity);
            }
            self.deallocate();
            memory.release(old_capacity);
        }
        self.ptr = ptr;
        self.capacity = target;
        Ok(())
    }

    fn prefix_mut(&mut self, length: usize) -> &mut [u8] {
        debug_assert!(length <= self.capacity);
        // SAFETY: the allocation holds `capacity` initialized bytes, this
        // mutable borrow is exclusive, and `length <= capacity`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), length) }
    }

    fn deallocate(&mut self) {
        if self.capacity == 0 {
            return;
        }
        deallocate_buffer(self.ptr, self.capacity);
        self.ptr = NonNull::dangling();
        self.capacity = 0;
    }
}

#[cfg(target_os = "linux")]
fn allocate_buffer(capacity: usize) -> Option<NonNull<u8>> {
    // SAFETY: the anonymous mapping ignores the descriptor and offset. The
    // returned pages are readable, writable, zero-filled, and page-aligned.
    let mapped = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            capacity,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if mapped == libc::MAP_FAILED {
        return None;
    }
    let Some(pointer) = NonNull::new(mapped.cast::<u8>()) else {
        // A null mapping cannot be represented by `NonNull`. It is still a
        // successful mapping under POSIX, so explicitly release it.
        // SAFETY: `mapped` and `capacity` identify the mapping just returned.
        let _ = unsafe { libc::munmap(mapped, capacity) };
        return None;
    };
    Some(pointer)
}

#[cfg(target_os = "linux")]
fn deallocate_buffer(pointer: NonNull<u8>, capacity: usize) {
    // SAFETY: the pointer and capacity came from one successful `mmap` call
    // and ownership has not been released before this call.
    let result = unsafe { libc::munmap(pointer.as_ptr().cast(), capacity) };
    debug_assert_eq!(result, 0, "aligned buffer munmap failed");
}

#[cfg(not(target_os = "linux"))]
fn allocate_buffer(capacity: usize) -> Option<NonNull<u8>> {
    let layout = Layout::from_size_align(capacity, BUFFER_ALIGNMENT).ok()?;
    // SAFETY: `layout` has non-zero size and valid power-of-two alignment.
    NonNull::new(unsafe { alloc_zeroed(layout) })
}

#[cfg(not(target_os = "linux"))]
fn deallocate_buffer(pointer: NonNull<u8>, capacity: usize) {
    let layout = Layout::from_size_align(capacity, BUFFER_ALIGNMENT)
        .expect("stored aligned-buffer layout is valid");
    // SAFETY: `pointer` was allocated with this exact layout and is owned here.
    unsafe { dealloc(pointer.as_ptr(), layout) };
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        self.deallocate();
    }
}

struct MemoryTracker {
    budget: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl MemoryTracker {
    fn new(budget: usize, base: usize) -> Self {
        Self {
            budget,
            current: AtomicUsize::new(base),
            peak: AtomicUsize::new(base),
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.current.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.budget {
                return false;
            }
            match self.current.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    update_peak(&self.peak, next);
                    return true;
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn release(&self, bytes: usize) {
        if bytes != 0 {
            self.current.fetch_sub(bytes, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
pub(crate) fn aligned_buffer_capacity(value: usize) -> Option<usize> {
    align_up(value, BUFFER_ALIGNMENT)
}

fn align_up(value: usize, alignment: usize) -> Option<usize> {
    value
        .checked_add(alignment - 1)
        .map(|sum| sum / alignment * alignment)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_timeout_unpoisoned<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    duration: Duration,
) -> (MutexGuard<'a, T>, bool) {
    match condvar.wait_timeout(guard, duration) {
        Ok((guard, result)) => (guard, result.timed_out()),
        Err(poisoned) => {
            let (guard, result) = poisoned.into_inner();
            (guard, result.timed_out())
        }
    }
}

fn update_peak(peak: &AtomicUsize, candidate: usize) {
    let mut current = peak.load(Ordering::Relaxed);
    while candidate > current {
        match peak.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(observed) => current = observed,
        }
    }
}

fn add_duration(counter: &AtomicU64, duration: Duration) {
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(nanos))
    });
}

fn add_wait_duration(counter: &AtomicU64, started: Option<Instant>) {
    if let Some(started) = started {
        add_duration(counter, started.elapsed());
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(policy: BackpressurePolicy) -> ResourceLimits {
        ResourceLimits {
            memory_budget_bytes: 128 * 1024,
            base_memory_bytes: 16 * 1024,
            max_buffer_bytes: 16 * 1024,
            read_queue_depth: 2,
            write_queue_depth: 2,
            read_buffer_slots: READ_BUFFER_SLOTS,
            write_buffer_slots: WRITE_BUFFER_SLOTS,
            control_concurrency: 1,
            backpressure: policy,
        }
    }

    #[test]
    fn aligned_pool_has_fixed_slots_and_bounded_lazy_growth() {
        let resources = ResourceController::try_new(limits(BackpressurePolicy::Reject)).unwrap();
        let mut first = resources.begin_read().unwrap();
        let mut second = resources.begin_read().unwrap();
        assert_eq!(first.buffer.prepare(5000).unwrap().len(), 5000);
        assert_eq!(second.buffer.prepare(9000).unwrap().len(), 9000);
        assert_eq!(first.buffer.address() % BUFFER_ALIGNMENT, 0);
        assert_eq!(second.buffer.address() % BUFFER_ALIGNMENT, 0);
        assert_eq!(
            resources.begin_read().err(),
            Some(OverloadReason::ReadQueueFull)
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.read_queue_depth, 2);
        assert_eq!(snapshot.read_buffers_in_use, 2);
        assert!(snapshot.memory_peak_bytes <= snapshot.memory_budget_bytes);
        drop(first);
        assert!(resources.begin_read().is_ok());
    }

    #[test]
    fn preserving_growth_keeps_prefix_and_zeroes_fresh_capacity() {
        let mut configured = limits(BackpressurePolicy::Reject);
        configured.max_buffer_bytes = 3 * BUFFER_ALIGNMENT;
        let resources = ResourceController::try_new(configured).unwrap();
        let mut request = resources.begin_read().unwrap();

        let first = request.buffer.grow_preserving(BUFFER_ALIGNMENT).unwrap();
        assert!(first.iter().all(|byte| *byte == 0));
        first.fill(0x5a);

        let grown = request
            .buffer
            .grow_preserving(2 * BUFFER_ALIGNMENT)
            .unwrap();
        assert!(grown[..BUFFER_ALIGNMENT].iter().all(|byte| *byte == 0x5a));
        assert!(grown[BUFFER_ALIGNMENT..].iter().all(|byte| *byte == 0));
        assert_eq!(request.buffer.address() % BUFFER_ALIGNMENT, 0);
        let snapshot = resources.snapshot();
        assert_eq!(
            snapshot.memory_used_bytes,
            (16 * 1024 + 2 * BUFFER_ALIGNMENT) as u64
        );
        assert_eq!(
            snapshot.memory_peak_bytes,
            (16 * 1024 + 3 * BUFFER_ALIGNMENT) as u64,
            "growth peak must count the old and replacement mappings"
        );
        assert!(snapshot.memory_peak_bytes <= snapshot.memory_budget_bytes);
    }

    #[test]
    fn failed_preserving_growth_keeps_the_existing_buffer() {
        let mut configured = limits(BackpressurePolicy::Reject);
        configured.base_memory_bytes = 0;
        configured.memory_budget_bytes = BUFFER_ALIGNMENT;
        configured.max_buffer_bytes = 2 * BUFFER_ALIGNMENT;
        let resources = ResourceController::try_new(configured).unwrap();
        let mut request = resources.begin_read().unwrap();
        request
            .buffer
            .grow_preserving(BUFFER_ALIGNMENT)
            .unwrap()
            .fill(0xa5);

        assert!(
            request
                .buffer
                .grow_preserving(2 * BUFFER_ALIGNMENT)
                .is_err()
        );
        assert!(
            request
                .buffer
                .prepared(BUFFER_ALIGNMENT)
                .unwrap()
                .iter()
                .all(|byte| *byte == 0xa5)
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.memory_used_bytes, BUFFER_ALIGNMENT as u64);
        assert_eq!(snapshot.memory_peak_bytes, BUFFER_ALIGNMENT as u64);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn aligned_buffer_uses_a_shared_mapping_on_linux() {
        let resources = ResourceController::try_new(limits(BackpressurePolicy::Reject)).unwrap();
        let mut request = resources.begin_read().unwrap();
        request.buffer.prepare(BUFFER_ALIGNMENT).unwrap();
        let address = request.buffer.address();
        let mappings = std::fs::read_to_string("/proc/self/maps").unwrap();
        let permissions = mappings
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                let mut bounds = fields.next()?.split('-');
                let start = usize::from_str_radix(bounds.next()?, 16).ok()?;
                let end = usize::from_str_radix(bounds.next()?, 16).ok()?;
                (start <= address && address < end)
                    .then(|| fields.next())
                    .flatten()
            })
            .expect("aligned buffer mapping must appear in /proc/self/maps");

        assert_eq!(permissions.as_bytes().get(3), Some(&b's'));
    }

    #[test]
    fn write_saturation_does_not_consume_reserved_read_resources() {
        let resources = ResourceController::try_new(limits(BackpressurePolicy::Reject)).unwrap();
        let _write_a = resources.begin_write().unwrap();
        let _write_b = resources.begin_write().unwrap();
        assert_eq!(
            resources.begin_write().err(),
            Some(OverloadReason::WriteQueueFull)
        );
        let mut read = resources.begin_read().unwrap();
        assert_eq!(read.buffer.prepare(4096).unwrap().len(), 4096);
    }

    #[test]
    fn repeated_buffer_overload_is_bounded_and_leak_free() {
        let mut configured = limits(BackpressurePolicy::Reject);
        configured.write_queue_depth = 3;
        let resources = ResourceController::try_new(configured).unwrap();
        let mut first = resources.begin_write().unwrap();
        let mut second = resources.begin_write().unwrap();
        first.buffer.prepare(5000).unwrap();
        second.buffer.prepare(9000).unwrap();
        let memory_after_growth = resources.snapshot().memory_used_bytes;

        for _ in 0..1024 {
            assert_eq!(
                resources.begin_write().err(),
                Some(OverloadReason::WriteBufferUnavailable)
            );
        }
        let saturated = resources.snapshot();
        assert_eq!(saturated.write_queue_depth, 2);
        assert_eq!(saturated.write_buffers_in_use, 2);
        assert_eq!(saturated.queue_rejections, 0);
        assert_eq!(saturated.buffer_rejections, 1024);
        assert_eq!(saturated.memory_used_bytes, memory_after_growth);
        assert!(saturated.memory_peak_bytes <= saturated.memory_budget_bytes);

        drop((first, second));
        let released = resources.snapshot();
        assert_eq!(released.write_queue_depth, 0);
        assert_eq!(released.write_buffers_in_use, 0);
        assert_eq!(released.memory_used_bytes, memory_after_growth);
    }

    #[test]
    fn timeout_is_bounded_and_counted() {
        let resources = ResourceController::try_new(limits(BackpressurePolicy::Timeout(
            Duration::from_millis(1),
        )))
        .unwrap();
        let _first = resources.begin_read().unwrap();
        let _second = resources.begin_read().unwrap();
        assert_eq!(resources.snapshot().backpressure_wait_ns, 0);
        assert_eq!(
            resources.begin_read().err(),
            Some(OverloadReason::ReadTimeout)
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.queue_rejections, 1);
        assert!(snapshot.read_queue_wait_ns > 0);
        assert_eq!(snapshot.write_queue_wait_ns, 0);
        assert_eq!(snapshot.control_queue_wait_ns, 0);
        assert_eq!(snapshot.read_buffer_wait_ns, 0);
        assert_eq!(snapshot.write_buffer_wait_ns, 0);
        assert_eq!(snapshot.control_buffer_wait_ns, 0);
        assert_eq!(snapshot.metadata_buffer_wait_ns, 0);
        assert_eq!(snapshot.backpressure_wait_ns, snapshot.read_queue_wait_ns);
    }

    #[test]
    fn buffer_timeout_is_attributed_without_counting_queue_fast_path() {
        let mut configured = limits(BackpressurePolicy::Timeout(Duration::from_millis(1)));
        configured.read_queue_depth = 3;
        configured.read_buffer_slots = 2;
        let resources = ResourceController::try_new(configured).unwrap();
        let _first = resources.begin_read().unwrap();
        let _second = resources.begin_read().unwrap();

        assert_eq!(
            resources.begin_read().err(),
            Some(OverloadReason::ReadTimeout)
        );
        let snapshot = resources.snapshot();
        assert_eq!(snapshot.read_queue_wait_ns, 0);
        assert!(snapshot.read_buffer_wait_ns > 0);
        assert_eq!(snapshot.backpressure_wait_ns, snapshot.read_buffer_wait_ns);
    }

    #[test]
    fn resource_wait_snapshot_preserves_each_component_and_exact_sum() {
        let resources = ResourceController::try_new(limits(BackpressurePolicy::Reject)).unwrap();
        for (counter, value) in [
            (&resources.read_gate.wait_ns, 1),
            (&resources.write_gate.wait_ns, 2),
            (&resources.control_gate.wait_ns, 3),
            (&resources.read_pool.wait_ns, 4),
            (&resources.write_pool.wait_ns, 5),
            (&resources.control_pool.wait_ns, 6),
            (&resources.metadata_pool.wait_ns, 7),
        ] {
            counter.store(value, Ordering::Relaxed);
        }

        let snapshot = resources.snapshot();
        assert_eq!(snapshot.read_queue_wait_ns, 1);
        assert_eq!(snapshot.write_queue_wait_ns, 2);
        assert_eq!(snapshot.control_queue_wait_ns, 3);
        assert_eq!(snapshot.read_buffer_wait_ns, 4);
        assert_eq!(snapshot.write_buffer_wait_ns, 5);
        assert_eq!(snapshot.control_buffer_wait_ns, 6);
        assert_eq!(snapshot.metadata_buffer_wait_ns, 7);
        assert_eq!(snapshot.backpressure_wait_ns, 28);
    }

    #[test]
    fn block_waits_outside_the_bounded_queue_and_resumes_on_drop() {
        let resources =
            Arc::new(ResourceController::try_new(limits(BackpressurePolicy::Block)).unwrap());
        let first = resources.begin_read().unwrap();
        let _second = resources.begin_read().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
        let worker_resources = Arc::clone(&resources);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _request = worker_resources.begin_read().unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(done_rx.try_recv().is_err());
        assert_eq!(resources.snapshot().read_queue_depth, 2);
        drop(first);
        done_rx.recv().unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn closing_dedicated_pool_wakes_waiters_when_a_slot_cannot_return() {
        let pool = Arc::new(DedicatedBufferPool::try_new(1, BUFFER_ALIGNMENT).unwrap());
        let held = pool.acquire().unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let worker_pool = Arc::clone(&pool);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            worker_pool.acquire().is_none()
        });

        started_rx.recv().unwrap();
        pool.close();
        assert!(worker.join().unwrap());
        assert_eq!(pool.snapshot().rejections, 1);
        drop(held);
    }

    #[test]
    fn high_concurrency_small_buffers_grow_within_the_budget() {
        let mut configured = limits(BackpressurePolicy::Reject);
        configured.max_buffer_bytes = 1024 * 1024;
        configured.read_queue_depth = 16;
        configured.write_queue_depth = 16;
        configured.read_buffer_slots = 16;
        configured.write_buffer_slots = 16;
        configured.memory_budget_bytes = configured.base_memory_bytes + 32 * BUFFER_ALIGNMENT;

        let resources = ResourceController::try_new(configured).unwrap();
        let mut reads = Vec::new();
        let mut writes = Vec::new();
        for _ in 0..16 {
            let mut request = resources.begin_read().unwrap();
            request.buffer.prepare(1).unwrap();
            reads.push(request);
        }
        for _ in 0..16 {
            let mut request = resources.begin_write().unwrap();
            request.buffer.prepare(1).unwrap();
            writes.push(request);
        }

        let snapshot = resources.snapshot();
        assert_eq!(snapshot.read_buffers_in_use, 16);
        assert_eq!(snapshot.write_buffers_in_use, 16);
        assert_eq!(snapshot.memory_used_bytes, snapshot.memory_budget_bytes);
        assert!(snapshot.memory_peak_bytes <= snapshot.memory_budget_bytes);
        drop((reads, writes));
    }

    #[test]
    fn base_memory_over_budget_is_rejected_before_allocating_buffers() {
        let mut too_small = limits(BackpressurePolicy::Reject);
        too_small.memory_budget_bytes = too_small.base_memory_bytes - 1;
        assert!(matches!(
            ResourceController::try_new(too_small),
            Err(ResourceBuildError::Invalid(_))
        ));
    }
}
