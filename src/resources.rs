//! Bounded write admission and aligned record buffers.
//!
//! Foreground reads allocate one alignment-rounded transient buffer for the
//! exact planned range after an L2 index hit. Write waiting uses a separate
//! request gate.

use std::alloc::{Layout, alloc, dealloc};
use std::fmt;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) const BUFFER_ALIGNMENT: usize = 4096;
/// Every cache-owned thread uses an explicit stack reservation so configured
/// topology cannot inherit an environment-dependent `RUST_MIN_STACK` value.
pub(crate) const CACHE_THREAD_STACK_BYTES: usize = 512 * 1024;
pub(crate) const MAX_CONFIG_COUNT: usize = 65_536;

pub(crate) struct ResourceLimits {
    pub(crate) memory_limit_bytes: usize,
    pub(crate) reserved_memory_bytes: usize,
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
        if limits.reserved_memory_bytes > limits.memory_limit_bytes {
            return Err(ResourceBuildError::Invalid(
                "memory limit cannot hold the cache's reserved memory",
            ));
        }

        let memory = Arc::new(MemoryTracker::new(
            limits.memory_limit_bytes,
            limits.reserved_memory_bytes,
        ));
        Ok(Self { memory })
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

    /// Allocates one alignment-rounded foreground read buffer against the
    /// cache-wide hard memory limit. Failure is a cache miss, never caller
    /// backpressure.
    pub(crate) fn try_read_buffer(&self, length: usize) -> Option<BufferLease> {
        BufferLease::try_standalone(length, Arc::clone(&self.memory))
    }

    pub(crate) fn managed_memory_snapshot(&self) -> ManagedMemorySnapshot {
        let current_bytes = self.memory.current.load(Ordering::Relaxed);
        ManagedMemorySnapshot {
            limit_bytes: self.memory.limit,
            current_bytes,
            peak_bytes: self.memory.peak.load(Ordering::Relaxed).max(current_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ManagedMemorySnapshot {
    pub(crate) limit_bytes: usize,
    pub(crate) current_bytes: usize,
    pub(crate) peak_bytes: usize,
}

pub(crate) struct BufferLease {
    owner: BufferOwner,
    buffer: Option<AlignedBuffer>,
}

enum BufferOwner {
    Fixed,
    Standalone { memory: Arc<MemoryTracker> },
}

impl BufferLease {
    pub(crate) fn try_fixed(length: usize) -> Result<Self, ResourceBuildError> {
        if length == 0 || !length.is_multiple_of(BUFFER_ALIGNMENT) || length > isize::MAX as usize {
            return Err(ResourceBuildError::Invalid(
                "fixed buffer size must be a non-zero 4096-byte multiple",
            ));
        }
        let ptr = allocate_buffer(length).ok_or(ResourceBuildError::Allocation)?;
        let mut buffer = AlignedBuffer {
            ptr,
            capacity: length,
            initialized: 0,
        };
        buffer.prepare_zeroed(length);
        Ok(Self {
            owner: BufferOwner::Fixed,
            buffer: Some(buffer),
        })
    }

    fn try_standalone(length: usize, memory: Arc<MemoryTracker>) -> Option<Self> {
        let maximum = align_up(length, BUFFER_ALIGNMENT)?;
        if maximum == 0 || maximum > isize::MAX as usize {
            return None;
        }
        if !memory.try_reserve(maximum) {
            return None;
        }
        let Some(ptr) = allocate_buffer(maximum) else {
            memory.release(maximum);
            return None;
        };
        Some(Self {
            owner: BufferOwner::Standalone { memory },
            buffer: Some(AlignedBuffer {
                ptr,
                capacity: maximum,
                initialized: 0,
            }),
        })
    }

    #[cfg(test)]
    pub(crate) fn prepare(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if length > buffer.capacity {
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
        let buffer = self.buffer.as_mut().expect("buffer lease owns a buffer");
        if length > buffer.capacity {
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
                BufferOwner::Fixed => drop(buffer),
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

fn update_peak(peak: &AtomicUsize, candidate: usize) {
    peak.fetch_max(candidate, Ordering::Relaxed);
}

const MAX_ATOMIC_UPDATE_ATTEMPTS: usize = 8;

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ResourceLimits {
        ResourceLimits {
            memory_limit_bytes: 128 * 1024,
            reserved_memory_bytes: 16 * 1024,
        }
    }

    #[test]
    fn transient_read_buffers_charge_exact_aligned_bytes_and_release_them() {
        let resources = ResourceController::try_new(limits()).unwrap();
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
        let mut buffer = BufferLease::try_fixed(3 * BUFFER_ALIGNMENT).unwrap();

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
        let mut buffer = BufferLease::try_fixed(2 * BUFFER_ALIGNMENT).unwrap();
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
    fn transient_read_buffers_stay_within_the_memory_limit() {
        let mut configured = limits();
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
        let mut too_small = limits();
        too_small.memory_limit_bytes = too_small.reserved_memory_bytes - 1;
        assert!(matches!(
            ResourceController::try_new(too_small),
            Err(ResourceBuildError::Invalid(_))
        ));
    }
}
