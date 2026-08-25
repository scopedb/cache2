//! Bounded write admission and aligned record buffers.
//!
//! Foreground reads allocate one exact-size transient buffer after an L2 index
//! hit. Write waiting uses a separate request gate.

use std::alloc::{Layout, alloc, dealloc};
use std::fmt;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

pub(crate) const BUFFER_ALIGNMENT: usize = 4096;
/// Every cache-owned thread uses an explicit stack reservation so configured
/// topology cannot inherit an environment-dependent `RUST_MIN_STACK` value.
pub(crate) const CACHE_THREAD_STACK_BYTES: usize = 512 * 1024;
pub(crate) const MAX_CONFIG_COUNT: usize = 65_536;
pub(crate) const MAX_BACKPRESSURE_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

/// Behavior when a foreground write gate or fixed write buffer is full.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WriteBackpressure {
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
pub(crate) enum WriteOverloadReason {
    /// The write or reserved control submission gate is at capacity.
    WriteGateBusy,
    /// The fixed append-shard write buffers are full or rotating.
    WriteBufferBusy,
    /// Write/control admission exceeded the configured timeout.
    Timeout,
}

impl fmt::Display for WriteOverloadReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::WriteGateBusy => "write gate is busy",
            Self::WriteBufferBusy => "write buffer is busy",
            Self::Timeout => "write backpressure timeout expired",
        })
    }
}

pub(crate) struct ResourceLimits {
    pub(crate) memory_limit_bytes: usize,
    pub(crate) reserved_memory_bytes: usize,
    pub(crate) waiting_write_limit: usize,
    pub(crate) write_backpressure: WriteBackpressure,
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
    write_gate: Arc<WriteGate>,
    write_backpressure: WriteBackpressure,
}

/// A fixed runtime allocation charged to the same hard memory limit as the
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
        if limits.waiting_write_limit == 0 {
            return Err(ResourceBuildError::Invalid(
                "waiting write limit must be at least 1",
            ));
        }
        if limits.waiting_write_limit > MAX_CONFIG_COUNT {
            return Err(ResourceBuildError::Invalid(
                "waiting write limit exceeds its hard limit",
            ));
        }
        if let WriteBackpressure::Timeout(duration) = limits.write_backpressure {
            if duration > MAX_BACKPRESSURE_TIMEOUT {
                return Err(ResourceBuildError::Invalid(
                    "backpressure timeout must not exceed 24 hours",
                ));
            }
        }
        if limits.reserved_memory_bytes > limits.memory_limit_bytes {
            return Err(ResourceBuildError::Invalid(
                "memory limit cannot hold the cache's reserved memory",
            ));
        }

        let memory = Arc::new(MemoryTracker::new(
            limits.memory_limit_bytes,
            limits.reserved_memory_bytes,
        ));
        Ok(Self {
            write_gate: Arc::new(WriteGate::new(limits.waiting_write_limit)),
            write_backpressure: limits.write_backpressure,
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

    /// Allocates one exact-size foreground read buffer against the cache-wide
    /// hard memory limit. Failure is a cache miss, never caller backpressure.
    pub(crate) fn try_read_buffer(&self, length: usize) -> Option<BufferLease> {
        BufferLease::try_standalone(length, Arc::clone(&self.memory))
    }

    /// Admit a write whose payload already lives in engine-owned storage.
    ///
    /// RegionStore encodes directly into its fixed per-shard write
    /// buffers, so leasing another general-purpose write buffer here would
    /// double-account memory and reduce useful concurrency.
    pub(crate) fn begin_write_permit_until(
        &self,
        deadline: Option<Instant>,
    ) -> Result<WritePermit, WriteOverloadReason> {
        let wait = match self.write_backpressure {
            WriteBackpressure::Reject => WaitMode::Reject,
            WriteBackpressure::Block => WaitMode::Block,
            WriteBackpressure::Timeout(_) => {
                WaitMode::Deadline(deadline.unwrap_or_else(Instant::now))
            }
        };
        self.write_gate.enter(wait)
    }

    pub(crate) fn managed_memory_snapshot(&self) -> ManagedMemorySnapshot {
        let current_bytes = self.memory.current.load(Ordering::Relaxed);
        ManagedMemorySnapshot {
            limit_bytes: self.memory.limit,
            current_bytes,
            peak_bytes: self.memory.peak.load(Ordering::Relaxed).max(current_bytes),
        }
    }

    pub(crate) fn runtime_snapshot(&self) -> ResourceRuntimeSnapshot {
        ResourceRuntimeSnapshot {
            write_requests_in_flight: usize_to_u64(self.write_gate.current.load(Ordering::Relaxed)),
            write_requests_peak: usize_to_u64(self.write_gate.peak.load(Ordering::Relaxed)),
            write_gate_rejections: self.write_gate.rejections.load(Ordering::Relaxed),
            write_gate_wait_ns: self.write_gate.wait_ns.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManagedMemorySnapshot {
    pub(crate) limit_bytes: usize,
    pub(crate) current_bytes: usize,
    pub(crate) peak_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ResourceRuntimeSnapshot {
    pub(crate) write_requests_in_flight: u64,
    pub(crate) write_requests_peak: u64,
    pub(crate) write_gate_rejections: u64,
    pub(crate) write_gate_wait_ns: u64,
}

#[derive(Clone, Copy)]
enum WaitMode {
    Reject,
    Block,
    Deadline(Instant),
}

struct WriteGate {
    limit: usize,
    state: Mutex<usize>,
    available: Condvar,
    current: AtomicUsize,
    peak: AtomicUsize,
    rejections: AtomicU64,
    wait_ns: AtomicU64,
}

impl WriteGate {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            state: Mutex::new(0),
            available: Condvar::new(),
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
            wait_ns: AtomicU64::new(0),
        }
    }

    fn enter(self: &Arc<Self>, wait: WaitMode) -> Result<WritePermit, WriteOverloadReason> {
        let mut wait_started = None;
        let mut state = lock_unpoisoned(&self.state);
        while *state == self.limit {
            if wait_started.is_none() && !matches!(wait, WaitMode::Reject) {
                wait_started = Some(Instant::now());
            }
            state = match wait {
                WaitMode::Reject => {
                    self.rejections.fetch_add(1, Ordering::Relaxed);
                    return Err(WriteOverloadReason::WriteGateBusy);
                }
                WaitMode::Block => wait_unpoisoned(&self.available, state),
                WaitMode::Deadline(deadline) => {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WriteOverloadReason::Timeout);
                    };
                    let (guard, timed_out) =
                        wait_timeout_unpoisoned(&self.available, state, remaining);
                    if timed_out && *guard == self.limit {
                        self.rejections.fetch_add(1, Ordering::Relaxed);
                        add_wait_duration(&self.wait_ns, wait_started);
                        return Err(WriteOverloadReason::Timeout);
                    }
                    guard
                }
            };
        }
        *state += 1;
        let current = *state;
        self.current.store(current, Ordering::Relaxed);
        update_peak(&self.peak, current);
        add_wait_duration(&self.wait_ns, wait_started);
        Ok(WritePermit {
            gate: Arc::clone(self),
        })
    }

    fn release(&self) {
        let mut state = lock_unpoisoned(&self.state);
        debug_assert!(*state != 0);
        if *state != 0 {
            *state -= 1;
        }
        self.current.store(*state, Ordering::Relaxed);
        self.available.notify_one();
    }
}

pub(crate) struct WritePermit {
    gate: Arc<WriteGate>,
}

impl Drop for WritePermit {
    fn drop(&mut self) {
        self.gate.release();
    }
}

struct WriteBufferPoolInner {
    max_buffer_bytes: usize,
    state: Mutex<Vec<AlignedBuffer>>,
    available: Condvar,
    memory: Arc<MemoryTracker>,
    in_use: AtomicUsize,
    rejections: AtomicU64,
}

/// A fixed-slot pool for append shards whose write unit has one constant size.
///
/// Unlike `ResourceController`, this wrapper has no request gate and does not
/// create separate read/write/control pools. Every slot is allocated eagerly,
/// so opening the engine either establishes the complete aligned memory bound
/// or fails before the cache file is modified.
pub(crate) struct WriteBufferPool {
    inner: Arc<WriteBufferPoolInner>,
    buffer_size: usize,
    closed: AtomicBool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WriteBufferPoolSnapshot {
    pub(crate) in_use: u64,
    pub(crate) rejections: u64,
}

impl WriteBufferPool {
    pub(crate) fn try_new(slots: usize, buffer_size: usize) -> Result<Self, ResourceBuildError> {
        if slots == 0 || slots > MAX_CONFIG_COUNT {
            return Err(ResourceBuildError::Invalid(
                "write buffer slots must be within the hard count limit",
            ));
        }
        if buffer_size == 0 || buffer_size % BUFFER_ALIGNMENT != 0 {
            return Err(ResourceBuildError::Invalid(
                "write buffer size must be a non-zero 4096-byte multiple",
            ));
        }
        let budget = slots
            .checked_mul(buffer_size)
            .ok_or(ResourceBuildError::Invalid(
                "write buffer memory size overflow",
            ))?;
        let memory = Arc::new(MemoryTracker::new(budget, 0));
        let inner = Arc::new(WriteBufferPoolInner::try_new(
            slots,
            buffer_size,
            Arc::clone(&memory),
        )?);

        {
            let mut state = lock_unpoisoned(&inner.state);
            for buffer in &mut *state {
                buffer
                    .ensure_capacity(buffer_size, buffer_size, &memory)
                    .map_err(|_| ResourceBuildError::Allocation)?;
            }
        }

        Ok(Self {
            inner,
            buffer_size,
            closed: AtomicBool::new(false),
        })
    }

    pub(crate) fn acquire(&self) -> Option<BufferLease> {
        let mut state = lock_unpoisoned(&self.inner.state);
        while state.is_empty() && !self.closed.load(Ordering::Acquire) {
            state = wait_unpoisoned(&self.inner.available, state);
        }
        if self.closed.load(Ordering::Acquire) {
            self.inner.rejections.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let buffer = state
            .pop()
            .expect("write buffer wake requires a free buffer or closure");
        self.inner.in_use.fetch_add(1, Ordering::Relaxed);
        drop(state);
        let mut lease = BufferLease {
            owner: BufferOwner::WritePool(Arc::clone(&self.inner)),
            buffer: Some(buffer),
        };
        lease
            .prepare(self.buffer_size)
            .expect("write buffers are fully allocated during construction");
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
    pub(crate) fn snapshot(&self) -> WriteBufferPoolSnapshot {
        WriteBufferPoolSnapshot {
            in_use: self.inner.in_use.load(Ordering::Relaxed) as u64,
            rejections: self.inner.rejections.load(Ordering::Relaxed),
        }
    }
}

impl WriteBufferPoolInner {
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
            state: Mutex::new(free),
            available: Condvar::new(),
            memory,
            in_use: AtomicUsize::new(0),
            rejections: AtomicU64::new(0),
        })
    }

    fn release(&self, buffer: AlignedBuffer) {
        let mut state = lock_unpoisoned(&self.state);
        state.push(buffer);
        self.in_use.fetch_sub(1, Ordering::Relaxed);
        self.available.notify_one();
    }
}

impl Drop for WriteBufferPoolInner {
    fn drop(&mut self) {
        let state = lock_unpoisoned(&self.state);
        let allocated = state.iter().map(|buffer| buffer.capacity).sum::<usize>();
        self.memory.release(allocated);
    }
}

pub(crate) struct BufferLease {
    owner: BufferOwner,
    buffer: Option<AlignedBuffer>,
}

enum BufferOwner {
    WritePool(Arc<WriteBufferPoolInner>),
    Standalone {
        memory: Arc<MemoryTracker>,
        maximum: usize,
    },
}

impl BufferLease {
    fn try_standalone(length: usize, memory: Arc<MemoryTracker>) -> Option<Self> {
        let maximum = align_up(length, BUFFER_ALIGNMENT)?;
        if maximum == 0 || maximum > isize::MAX as usize {
            return None;
        }
        let mut buffer = AlignedBuffer::empty();
        buffer.ensure_capacity(maximum, maximum, &memory).ok()?;
        Some(Self {
            owner: BufferOwner::Standalone { memory, maximum },
            buffer: Some(buffer),
        })
    }

    pub(crate) fn prepare(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let (maximum, memory, rejections) = match &self.owner {
            BufferOwner::WritePool(pool) => {
                (pool.max_buffer_bytes, &*pool.memory, Some(&pool.rejections))
            }
            BufferOwner::Standalone { memory, maximum } => (*maximum, &**memory, None),
        };
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if buffer.ensure_capacity(length, maximum, memory).is_err() {
            if let Some(rejections) = rejections {
                rejections.fetch_add(1, Ordering::Relaxed);
            }
            return Err(());
        }
        // Callers encode complete records. Clearing here also fixes padding and
        // prevents bytes from a prior key/value escaping into a later write.
        buffer.prepare_zeroed(length);
        Ok(buffer.prefix_mut(length))
    }

    /// Grow the leased buffer without clearing bytes already in the buffer.
    ///
    /// Fresh capacity is zeroed before it is exposed as initialized bytes.
    #[cfg(test)]
    pub(crate) fn grow_preserving(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let (maximum, memory, rejections) = match &self.owner {
            BufferOwner::WritePool(pool) => {
                (pool.max_buffer_bytes, &*pool.memory, Some(&pool.rejections))
            }
            BufferOwner::Standalone { memory, maximum } => (*maximum, &**memory, None),
        };
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if buffer.ensure_capacity(length, maximum, memory).is_err() {
            if let Some(rejections) = rejections {
                rejections.fetch_add(1, Ordering::Relaxed);
            }
            return Err(());
        }
        buffer.zero_uninitialized_through(length);
        Ok(buffer.prefix_mut(length))
    }

    pub(crate) fn prepared(&self, length: usize) -> Result<&[u8], ()> {
        let buffer = self.buffer.as_ref().ok_or(())?;
        if length > buffer.initialized {
            return Err(());
        }
        // SAFETY: the allocation holds `initialized` initialized bytes and the
        // returned shared slice cannot mutate the exclusively leased buffer.
        Ok(unsafe { std::slice::from_raw_parts(buffer.ptr.as_ptr(), length) })
    }

    pub(crate) fn prepared_mut(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = self.buffer.as_mut().ok_or(())?;
        if length > buffer.initialized {
            return Err(());
        }
        Ok(buffer.prefix_mut(length))
    }

    pub(crate) fn has_capacity(&self, length: usize) -> bool {
        self.buffer
            .as_ref()
            .is_some_and(|buffer| length <= buffer.capacity)
    }

    pub(crate) fn read_target(&self, length: usize) -> Result<*mut u8, ()> {
        let buffer = self.buffer.as_ref().ok_or(())?;
        if length > buffer.capacity {
            return Err(());
        }
        Ok(buffer.ptr.as_ptr())
    }

    pub(crate) fn mark_initialized(&mut self, length: usize) -> Result<(), ()> {
        let buffer = self.buffer.as_mut().ok_or(())?;
        if length > buffer.capacity {
            return Err(());
        }
        buffer.initialized = buffer.initialized.max(length);
        Ok(())
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
            match &self.owner {
                BufferOwner::WritePool(pool) => pool.release(buffer),
                BufferOwner::Standalone { memory, .. } => {
                    let capacity = buffer.capacity;
                    drop(buffer);
                    memory.release(capacity);
                }
            }
        }
    }
}

struct AlignedBuffer {
    ptr: NonNull<u8>,
    capacity: usize,
    initialized: usize,
}

// SAFETY: ownership of the allocation moves with this value. Bytes are only
// exposed through `&mut self`, and a buffer is leased to at most one thread.
unsafe impl Send for AlignedBuffer {}

impl AlignedBuffer {
    const fn empty() -> Self {
        Self {
            ptr: NonNull::dangling(),
            capacity: 0,
            initialized: 0,
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
        let old_initialized = self.initialized;
        // Growth briefly owns both allocations while preserving the prefix. Count
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

        if old_initialized != 0 {
            // SAFETY: both allocations are valid for `old_initialized` bytes and
            // cannot overlap. The old allocation remains live until the copy
            // completes, so a failed growth never destroys buffered data.
            unsafe {
                std::ptr::copy_nonoverlapping(self.ptr.as_ptr(), ptr.as_ptr(), old_initialized);
            }
        }
        if old_capacity != 0 {
            self.deallocate();
            memory.release(old_capacity);
        }
        self.ptr = ptr;
        self.capacity = target;
        self.initialized = old_initialized;
        Ok(())
    }

    fn prepare_zeroed(&mut self, length: usize) {
        debug_assert!(length <= self.capacity);
        // SAFETY: the allocation is valid for `capacity` bytes and this value
        // owns it exclusively. Writing bytes establishes initialization before
        // a Rust reference is created.
        unsafe { self.ptr.as_ptr().write_bytes(0, length) };
        self.initialized = self.initialized.max(length);
    }

    #[cfg(test)]
    fn zero_uninitialized_through(&mut self, length: usize) {
        debug_assert!(length <= self.capacity);
        if length > self.initialized {
            // SAFETY: the uninitialized tail is inside the owned allocation.
            unsafe {
                self.ptr
                    .as_ptr()
                    .add(self.initialized)
                    .write_bytes(0, length - self.initialized);
            }
            self.initialized = length;
        }
    }

    fn prefix_mut(&mut self, length: usize) -> &mut [u8] {
        debug_assert!(length <= self.capacity);
        debug_assert!(length <= self.initialized);
        // SAFETY: the allocation holds `initialized` initialized bytes, this
        // mutable borrow is exclusive, and `length <= initialized`.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), length) }
    }

    fn deallocate(&mut self) {
        if self.capacity == 0 {
            return;
        }
        deallocate_buffer(self.ptr, self.capacity);
        self.ptr = NonNull::dangling();
        self.capacity = 0;
        self.initialized = 0;
    }
}

fn allocate_buffer(capacity: usize) -> Option<NonNull<u8>> {
    let layout = Layout::from_size_align(capacity, BUFFER_ALIGNMENT).ok()?;
    // SAFETY: `layout` has non-zero size and valid power-of-two alignment.
    NonNull::new(unsafe { alloc(layout) })
}

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
    limit: usize,
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl MemoryTracker {
    fn new(limit: usize, reserved: usize) -> Self {
        Self {
            limit,
            current: AtomicUsize::new(reserved),
            peak: AtomicUsize::new(reserved),
        }
    }

    fn try_reserve(&self, bytes: usize) -> bool {
        let mut current = self.current.load(Ordering::Relaxed);
        for _ in 0..MAX_ATOMIC_UPDATE_ATTEMPTS {
            let Some(next) = current.checked_add(bytes) else {
                return false;
            };
            if next > self.limit {
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
        false
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
    peak.fetch_max(candidate, Ordering::Relaxed);
}

fn add_duration(counter: &AtomicU64, duration: Duration) {
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    atomic_saturating_add(counter, nanos);
}

const MAX_ATOMIC_UPDATE_ATTEMPTS: usize = 8;

fn atomic_saturating_add(counter: &AtomicU64, value: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    for _ in 0..MAX_ATOMIC_UPDATE_ATTEMPTS {
        let next = current.saturating_add(value);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
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

    fn limits(policy: WriteBackpressure) -> ResourceLimits {
        ResourceLimits {
            memory_limit_bytes: 128 * 1024,
            reserved_memory_bytes: 16 * 1024,
            waiting_write_limit: 2,
            write_backpressure: policy,
        }
    }

    #[test]
    fn transient_read_buffers_charge_exact_aligned_bytes_and_release_them() {
        let resources = ResourceController::try_new(limits(WriteBackpressure::Reject)).unwrap();
        let first = resources.try_read_buffer(5000).unwrap();
        let second = resources.try_read_buffer(9000).unwrap();
        assert_eq!(first.address() % BUFFER_ALIGNMENT, 0);
        assert_eq!(second.address() % BUFFER_ALIGNMENT, 0);
        assert!(first.prepared(5000).is_err());
        assert!(second.prepared(9000).is_err());
        let snapshot = resources.managed_memory_snapshot();
        assert_eq!(
            snapshot.current_bytes,
            16 * 1024 + 2 * BUFFER_ALIGNMENT + 3 * BUFFER_ALIGNMENT
        );
        assert!(snapshot.peak_bytes <= snapshot.limit_bytes);
        drop(first);
        drop(second);
        assert_eq!(resources.managed_memory_snapshot().current_bytes, 16 * 1024);
    }

    #[test]
    fn preserving_growth_keeps_prefix_and_zeroes_fresh_capacity() {
        let pool = WriteBufferPool::try_new(1, 3 * BUFFER_ALIGNMENT).unwrap();
        let mut buffer = pool.acquire().unwrap();

        let first = buffer.grow_preserving(BUFFER_ALIGNMENT).unwrap();
        assert!(first.iter().all(|byte| *byte == 0));
        first.fill(0x5a);

        let grown = buffer.grow_preserving(2 * BUFFER_ALIGNMENT).unwrap();
        assert!(grown[..BUFFER_ALIGNMENT].iter().all(|byte| *byte == 0x5a));
        assert!(grown[BUFFER_ALIGNMENT..].iter().all(|byte| *byte == 0));
        assert_eq!(buffer.address() % BUFFER_ALIGNMENT, 0);
    }

    #[test]
    fn failed_preserving_growth_keeps_the_existing_buffer() {
        let pool = WriteBufferPool::try_new(1, 2 * BUFFER_ALIGNMENT).unwrap();
        let mut buffer = pool.acquire().unwrap();
        buffer.grow_preserving(BUFFER_ALIGNMENT).unwrap().fill(0xa5);

        assert!(buffer.grow_preserving(3 * BUFFER_ALIGNMENT).is_err());
        assert!(
            buffer
                .prepared(BUFFER_ALIGNMENT)
                .unwrap()
                .iter()
                .all(|byte| *byte == 0xa5)
        );
    }

    #[test]
    fn write_timeout_is_bounded_and_counted() {
        let resources = ResourceController::try_new(limits(WriteBackpressure::Timeout(
            Duration::from_millis(1),
        )))
        .unwrap();
        let deadline = || Some(Instant::now() + Duration::from_millis(20));
        let _first = resources.begin_write_permit_until(deadline()).unwrap();
        let _second = resources.begin_write_permit_until(deadline()).unwrap();
        assert_eq!(
            resources.begin_write_permit_until(deadline()).err(),
            Some(WriteOverloadReason::Timeout)
        );
        let snapshot = resources.runtime_snapshot();
        assert_eq!(snapshot.write_gate_rejections, 1);
        assert!(snapshot.write_gate_wait_ns > 0);
    }

    #[test]
    fn blocking_write_admission_resumes_after_a_permit_drops() {
        let resources =
            Arc::new(ResourceController::try_new(limits(WriteBackpressure::Block)).unwrap());
        let first = resources.begin_write_permit_until(None).unwrap();
        let _second = resources.begin_write_permit_until(None).unwrap();
        let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
        let (done_tx, done_rx) = std::sync::mpsc::sync_channel(0);
        let worker_resources = Arc::clone(&resources);
        let worker = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let _permit = worker_resources.begin_write_permit_until(None).unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(done_rx.try_recv().is_err());
        assert_eq!(resources.runtime_snapshot().write_requests_in_flight, 2);
        drop(first);
        done_rx.recv().unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn closing_write_buffer_pool_wakes_waiters_when_a_slot_cannot_return() {
        let pool = Arc::new(WriteBufferPool::try_new(1, BUFFER_ALIGNMENT).unwrap());
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
    fn transient_read_buffers_stay_within_the_memory_limit() {
        let mut configured = limits(WriteBackpressure::Reject);
        configured.memory_limit_bytes = configured.reserved_memory_bytes + 3 * BUFFER_ALIGNMENT;

        let resources = ResourceController::try_new(configured).unwrap();
        let first = resources.try_read_buffer(BUFFER_ALIGNMENT).unwrap();
        let second = resources.try_read_buffer(2 * BUFFER_ALIGNMENT).unwrap();
        assert!(resources.try_read_buffer(1).is_none());
        let memory = resources.managed_memory_snapshot();
        assert_eq!(memory.current_bytes, memory.limit_bytes);
        assert!(memory.peak_bytes <= memory.limit_bytes);
        drop((first, second));
        assert!(resources.try_read_buffer(3 * BUFFER_ALIGNMENT).is_some());
    }

    #[test]
    fn reserved_memory_over_limit_is_rejected_before_allocating_buffers() {
        let mut too_small = limits(WriteBackpressure::Reject);
        too_small.memory_limit_bytes = too_small.reserved_memory_bytes - 1;
        assert!(matches!(
            ResourceController::try_new(too_small),
            Err(ResourceBuildError::Invalid(_))
        ));
    }
}
