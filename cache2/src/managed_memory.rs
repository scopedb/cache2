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

//! Managed-memory accounting and aligned record buffers.
//!
//! Foreground reads allocate one alignment-rounded transient buffer for the
//! size-class-bounded read range after an L2 index hit. Write waiting uses
//! a separate request gate.

use std::alloc::Layout;
use std::alloc::alloc;
use std::alloc::dealloc;
use std::fmt;
use std::ptr::NonNull;
use std::slice;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

pub const BUFFER_ALIGNMENT: usize = 4096;
/// Every cache-owned thread uses an explicit stack reservation so configured
/// topology cannot inherit an environment-dependent `RUST_MIN_STACK` value.
pub const CACHE_THREAD_STACK_BYTES: usize = 512 * 1024;
pub const MAX_CONFIG_COUNT: usize = 65_536;

pub struct ManagedMemoryLimits {
    pub memory_limit_bytes: usize,
    pub reserved_memory_bytes: usize,
}

#[derive(Debug)]
pub enum ManagedMemoryError {
    Invalid(&'static str),
    Allocation,
}

impl fmt::Display for ManagedMemoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
            Self::Allocation => formatter.write_str("managed memory cannot be allocated"),
        }
    }
}

pub struct ManagedMemory {
    memory: Arc<MemoryTracker>,
}

/// A charge against the cache-wide memory limit. Keep this guard until the
/// associated allocation has been released.
pub struct MemoryReservation {
    memory: Arc<MemoryTracker>,
    bytes: usize,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.memory.release(self.bytes);
    }
}

impl ManagedMemory {
    pub fn try_new(limits: ManagedMemoryLimits) -> Result<Self, ManagedMemoryError> {
        if limits.reserved_memory_bytes > limits.memory_limit_bytes {
            return Err(ManagedMemoryError::Invalid(
                "memory limit cannot hold the cache's reserved memory",
            ));
        }

        let memory = Arc::new(MemoryTracker::new(
            limits.memory_limit_bytes,
            limits.reserved_memory_bytes,
        ));
        Ok(Self { memory })
    }

    pub fn reserve(&self, bytes: usize) -> Result<MemoryReservation, ManagedMemoryError> {
        if !self.memory.try_reserve(bytes) {
            return Err(ManagedMemoryError::Allocation);
        }
        Ok(MemoryReservation {
            memory: Arc::clone(&self.memory),
            bytes,
        })
    }

    /// Allocates one alignment-rounded foreground read buffer against the
    /// cache-wide hard memory limit. The caller maps failure to either a
    /// fail-open miss or an explicit bounded-wait overload.
    pub fn try_read_buffer(&self, length: usize) -> Option<BufferLease> {
        let capacity = align_up(length, BUFFER_ALIGNMENT)?;
        if capacity == 0 || capacity > isize::MAX as usize {
            return None;
        }
        let reservation = self.reserve(capacity).ok()?;
        let buffer = AlignedBuffer::try_new(capacity)?;
        Some(BufferLease {
            buffer,
            _reservation: Some(reservation),
        })
    }

    pub fn snapshot(&self) -> ManagedMemorySnapshot {
        let current_bytes = self.memory.current.load(Ordering::Relaxed);
        ManagedMemorySnapshot {
            limit_bytes: self.memory.limit,
            current_bytes,
            peak_bytes: self.memory.peak.load(Ordering::Relaxed).max(current_bytes),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedMemorySnapshot {
    pub limit_bytes: usize,
    pub current_bytes: usize,
    pub peak_bytes: usize,
}

pub struct BufferLease {
    // Fields drop in declaration order: free the allocation before returning
    // its budget. Fixed staging buffers use their owner's aggregate charge.
    buffer: AlignedBuffer,
    _reservation: Option<MemoryReservation>,
}

impl BufferLease {
    pub fn try_fixed(length: usize) -> Result<Self, ManagedMemoryError> {
        if length == 0 || !length.is_multiple_of(BUFFER_ALIGNMENT) || length > isize::MAX as usize {
            return Err(ManagedMemoryError::Invalid(
                "fixed buffer size must be a non-zero 4096-byte multiple",
            ));
        }
        let mut buffer = AlignedBuffer::try_new(length).ok_or(ManagedMemoryError::Allocation)?;
        buffer.prepare_zeroed(length);
        Ok(Self {
            buffer,
            _reservation: None,
        })
    }

    #[cfg(test)]
    pub fn prepare(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = &mut self.buffer;
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
    fn grow_preserving(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = &mut self.buffer;
        if length > buffer.capacity {
            return Err(());
        }
        buffer.zero_uninitialized_through(length);
        Ok(buffer.prefix_mut(length))
    }

    pub fn prepared(&self, length: usize) -> Result<&[u8], ()> {
        let buffer = &self.buffer;
        if length > buffer.initialized {
            return Err(());
        }
        // SAFETY: the allocation holds `initialized` initialized bytes and the
        // returned shared slice cannot mutate the exclusively leased buffer.
        Ok(unsafe { slice::from_raw_parts(buffer.ptr.as_ptr(), length) })
    }

    pub fn prepared_mut(&mut self, length: usize) -> Result<&mut [u8], ()> {
        let buffer = &mut self.buffer;
        if length > buffer.initialized {
            return Err(());
        }
        Ok(buffer.prefix_mut(length))
    }

    pub fn has_capacity(&self, length: usize) -> bool {
        length <= self.buffer.capacity
    }

    pub fn read_target(&self, length: usize) -> Result<*mut u8, ()> {
        let buffer = &self.buffer;
        if length > buffer.capacity {
            return Err(());
        }
        Ok(buffer.ptr.as_ptr())
    }

    pub fn mark_initialized(&mut self, length: usize) -> Result<(), ()> {
        let buffer = &mut self.buffer;
        if length > buffer.capacity {
            return Err(());
        }
        buffer.initialized = buffer.initialized.max(length);
        Ok(())
    }

    #[cfg(test)]
    fn address(&self) -> usize {
        self.buffer.ptr.as_ptr() as usize
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
    fn try_new(capacity: usize) -> Option<Self> {
        let layout = Layout::from_size_align(capacity, BUFFER_ALIGNMENT).ok()?;
        // SAFETY: both constructors validate that capacity is non-zero; the
        // layout has valid power-of-two alignment and fits in isize.
        let ptr = NonNull::new(unsafe { alloc(layout) })?;
        Some(Self {
            ptr,
            capacity,
            initialized: 0,
        })
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
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), length) }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.capacity, BUFFER_ALIGNMENT)
            .expect("stored aligned-buffer layout is valid");
        // SAFETY: the pointer was allocated with this exact layout and this
        // buffer owns it until drop.
        unsafe { dealloc(self.ptr.as_ptr(), layout) };
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
pub fn aligned_buffer_capacity(value: usize) -> Option<usize> {
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

    fn limits() -> ManagedMemoryLimits {
        ManagedMemoryLimits {
            memory_limit_bytes: 128 * 1024,
            reserved_memory_bytes: 16 * 1024,
        }
    }

    #[test]
    fn transient_read_buffers_charge_exact_aligned_bytes_and_release_them() {
        let managed_memory = ManagedMemory::try_new(limits()).unwrap();
        let first = managed_memory.try_read_buffer(5000).unwrap();
        let second = managed_memory.try_read_buffer(9000).unwrap();
        assert_eq!(first.address() % BUFFER_ALIGNMENT, 0);
        assert_eq!(second.address() % BUFFER_ALIGNMENT, 0);
        assert!(first.prepared(5000).is_err());
        assert!(second.prepared(9000).is_err());
        let snapshot = managed_memory.snapshot();
        assert_eq!(
            snapshot.current_bytes,
            16 * 1024 + 2 * BUFFER_ALIGNMENT + 3 * BUFFER_ALIGNMENT
        );
        assert!(snapshot.peak_bytes <= snapshot.limit_bytes);
        drop(first);
        drop(second);
        assert_eq!(managed_memory.snapshot().current_bytes, 16 * 1024);
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

        let managed_memory = ManagedMemory::try_new(configured).unwrap();
        let first = managed_memory.try_read_buffer(BUFFER_ALIGNMENT).unwrap();
        let second = managed_memory
            .try_read_buffer(2 * BUFFER_ALIGNMENT)
            .unwrap();
        assert!(managed_memory.try_read_buffer(1).is_none());
        let memory = managed_memory.snapshot();
        assert_eq!(memory.current_bytes, memory.limit_bytes);
        assert!(memory.peak_bytes <= memory.limit_bytes);
        drop((first, second));
        assert!(
            managed_memory
                .try_read_buffer(3 * BUFFER_ALIGNMENT)
                .is_some()
        );
    }

    #[test]
    fn reserved_memory_over_limit_is_rejected_before_allocating_buffers() {
        let mut too_small = limits();
        too_small.memory_limit_bytes = too_small.reserved_memory_bytes - 1;
        assert!(matches!(
            ManagedMemory::try_new(too_small),
            Err(ManagedMemoryError::Invalid(_))
        ));
    }
}
