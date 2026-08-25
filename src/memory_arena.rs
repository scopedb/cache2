//! Size-class arena for L1 key/value storage.
//!
//! Allocation is shard-serialized. Final value drops may happen on arbitrary
//! threads and return blocks through bounded lock-free free lists. Empty chunks
//! may be reassigned to another size class so a short-lived class cannot strand
//! the shard's entire arena capacity.

use std::alloc::{Layout, alloc, dealloc};
use std::io;
use std::ops::Deref;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU32, AtomicUsize, Ordering, fence};

use crate::memory::{MEMORY_ENTRY_OVERHEAD_BYTES, MemoryBudget, MemoryCharge};

const KEY_LENGTH_BITS: u32 = 13;
const KEY_LENGTH_MASK: u32 = (1 << KEY_LENGTH_BITS) - 1;
const VALUE_LENGTH_MASK: u32 = u32::MAX >> KEY_LENGTH_BITS;
const MAX_VALUE_REFERENCES: u32 = i32::MAX as u32;
const SMALL_CLASS_LIMIT: usize = 4 * 1024;
const SMALL_CLASS_GRANULARITY: usize = 8;
const LARGE_CLASS_GRANULARITY: usize = 4 * 1024;
const SMALL_CLASS_COUNT: usize = SMALL_CLASS_LIMIT / SMALL_CLASS_GRANULARITY;
const MAX_ITEM_BYTES: usize =
    size_of::<MemoryValueHeader>() + KEY_LENGTH_MASK as usize + VALUE_LENGTH_MASK as usize;
const LARGE_CLASS_COUNT: usize =
    (MAX_ITEM_BYTES - SMALL_CLASS_LIMIT).div_ceil(LARGE_CLASS_GRANULARITY);
const CLASS_COUNT: usize = SMALL_CLASS_COUNT + LARGE_CLASS_COUNT;
const TARGET_CHUNK_BYTES: usize = 256 * 1024;
const TARGET_CHUNKS_PER_ARENA: usize = 16;
const MAX_FREE_LIST_CAS_ATTEMPTS: usize = 8;
const MAX_CHUNK_SCAN: usize = 16;

#[repr(C)]
pub(crate) struct MemoryValueHeader {
    references: AtomicU32,
    lengths: u32,
    chunk: NonNull<ArenaChunk>,
}

#[repr(C)]
struct FreeBlock {
    next: *mut FreeBlock,
}

/// Stable metadata for one independently owned data allocation.
///
/// `references` includes the allocator's owner reference plus one reference
/// for every live block. A retained `MemoryValue` increments only its block's
/// header count, so chunk lifetime bookkeeping stays off the L1 hit path.
struct ArenaChunk {
    references: AtomicU32,
    live_blocks: AtomicU32,
    free: AtomicPtr<FreeBlock>,
    block_bytes: AtomicUsize,
    pointer: NonNull<u8>,
    layout: Layout,
    budget: Arc<MemoryBudget>,
}

// SAFETY: the data allocation is immutable while blocks are live. Free-list
// publication and lifetime counters are atomic, and class reassignment is
// allowed only after an acquire load observes zero live blocks.
unsafe impl Send for ArenaChunk {}
// SAFETY: shared access is limited to the atomic operations described above
// and immutable allocation metadata.
unsafe impl Sync for ArenaChunk {}

impl ArenaChunk {
    fn allocate(data_bytes: usize, budget: Arc<MemoryBudget>) -> Option<NonNull<Self>> {
        let layout = Layout::from_size_align(data_bytes, align_of::<MemoryValueHeader>()).ok()?;
        // SAFETY: the checked layout is non-zero and has the header alignment.
        let pointer = NonNull::new(unsafe { alloc(layout) })?;
        let chunk = Box::new(Self {
            references: AtomicU32::new(1),
            live_blocks: AtomicU32::new(0),
            free: AtomicPtr::new(std::ptr::null_mut()),
            block_bytes: AtomicUsize::new(0),
            pointer,
            layout,
            budget,
        });
        Some(NonNull::from(Box::leak(chunk)))
    }

    fn data_bytes(&self) -> usize {
        self.layout.size()
    }

    fn has_class(&self, block_bytes: usize) -> bool {
        self.block_bytes.load(Ordering::Acquire) == block_bytes
    }

    fn is_empty(&self) -> bool {
        self.live_blocks.load(Ordering::Acquire) == 0
    }

    /// Rebuilds the whole chunk for another class. The unique allocator calls
    /// this only after observing that no live block can still access the data.
    fn reset(&self, block_bytes: usize) -> bool {
        if !self.is_empty() {
            return false;
        }
        let block_count = self.data_bytes() / block_bytes;
        if block_count == 0 {
            return false;
        }
        let mut head = std::ptr::null_mut();
        for ordinal in (0..block_count).rev() {
            // SAFETY: no block is live, every ordinal is within the allocation,
            // and the block size preserves header alignment.
            let block = unsafe {
                self.pointer
                    .as_ptr()
                    .add(ordinal * block_bytes)
                    .cast::<FreeBlock>()
            };
            // SAFETY: the allocator exclusively rebuilds the unpublished list.
            unsafe { block.write(FreeBlock { next: head }) };
            head = block;
        }
        self.block_bytes.store(block_bytes, Ordering::Release);
        self.free.store(head, Ordering::Release);
        true
    }

    fn take(&self, block_bytes: usize) -> Option<NonNull<MemoryValueHeader>> {
        if !self.has_class(block_bytes) {
            return None;
        }
        let mut head = self.free.load(Ordering::Acquire);
        for _ in 0..MAX_FREE_LIST_CAS_ATTEMPTS {
            let block = NonNull::new(head)?;
            // SAFETY: a published free block is chunk-owned and begins with its
            // next pointer.
            let next = unsafe { block.as_ref().next };
            match self
                .free
                .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    let previous = self.references.fetch_add(1, Ordering::Relaxed);
                    if previous == u32::MAX {
                        std::process::abort();
                    }
                    self.live_blocks.fetch_add(1, Ordering::Relaxed);
                    return Some(block.cast());
                }
                Err(observed) => head = observed,
            }
        }
        None
    }

    fn recycle(&self, header: NonNull<MemoryValueHeader>) {
        let block = header.cast::<FreeBlock>();
        let mut head = self.free.load(Ordering::Acquire);
        for _ in 0..MAX_FREE_LIST_CAS_ATTEMPTS {
            // SAFETY: the final value reference exclusively owns this block.
            unsafe { block.as_ptr().write(FreeBlock { next: head }) };
            match self.free.compare_exchange_weak(
                head,
                block.as_ptr(),
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => head = observed,
            }
        }
        // Recycling is optional bookkeeping. If the bounded CAS budget is
        // exhausted, the whole chunk becomes reusable once every block is dead.
        let previous = self.live_blocks.fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0);
    }

    /// Releases either the allocator owner or one live-block reference.
    unsafe fn release_reference(chunk: NonNull<Self>) {
        // SAFETY: the caller owns one reference represented by the counter.
        let previous = unsafe { chunk.as_ref() }
            .references
            .fetch_sub(1, Ordering::Release);
        debug_assert!(previous > 0);
        if previous == 1 {
            fence(Ordering::Acquire);
            // SAFETY: the final reference uniquely owns the leaked Box.
            unsafe { drop(Box::from_raw(chunk.as_ptr())) };
        }
    }
}

impl Drop for ArenaChunk {
    fn drop(&mut self) {
        // SAFETY: this is the final chunk reference and `pointer` was allocated
        // using this exact layout.
        unsafe { dealloc(self.pointer.as_ptr(), self.layout) };
    }
}

pub(crate) struct MemoryArena {
    budget: Arc<MemoryBudget>,
    current: Box<[*mut ArenaChunk]>,
    chunks: Vec<NonNull<ArenaChunk>>,
    capacity_bytes: usize,
    allocated_bytes: usize,
    scan_cursor: usize,
}

// SAFETY: allocation and chunk-vector mutation require `&mut MemoryArena`;
// moving that unique allocator handle between threads does not make them
// concurrent.
unsafe impl Send for MemoryArena {}

impl MemoryArena {
    pub(crate) fn new(budget: Arc<MemoryBudget>) -> io::Result<Self> {
        let mut current = Vec::new();
        current.try_reserve_exact(CLASS_COUNT).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate L1 arena class directory",
            )
        })?;
        current.resize(CLASS_COUNT, std::ptr::null_mut());
        Ok(Self {
            capacity_bytes: budget.capacity_bytes,
            budget,
            current: current.into_boxed_slice(),
            chunks: Vec::new(),
            allocated_bytes: 0,
            scan_cursor: 0,
        })
    }

    pub(crate) fn allocate(
        &mut self,
        key: &[u8],
        value: &[u8],
        charge: MemoryCharge,
    ) -> Result<MemoryValue, MemoryCharge> {
        let Ok(key_length) = u32::try_from(key.len()) else {
            return Err(charge);
        };
        let Ok(value_length) = u32::try_from(value.len()) else {
            return Err(charge);
        };
        if key_length > KEY_LENGTH_MASK || value_length > VALUE_LENGTH_MASK {
            return Err(charge);
        }
        let Some(logical_bytes) = key.len().checked_add(value.len()) else {
            return Err(charge);
        };
        let Some(required_bytes) = size_of::<MemoryValueHeader>().checked_add(logical_bytes) else {
            return Err(charge);
        };
        let Some(class) = class_index(required_bytes) else {
            return Err(charge);
        };
        let block_bytes = class_block_bytes(class);
        let Some((header, chunk)) = self.take(class, block_bytes) else {
            return Err(charge);
        };

        // SAFETY: the selected block is exclusively owned, sufficiently large,
        // and aligned for the fixed header followed by the immutable payload.
        unsafe {
            header.as_ptr().write(MemoryValueHeader {
                references: AtomicU32::new(1),
                lengths: (value_length << KEY_LENGTH_BITS) | key_length,
                chunk,
            });
            let bytes = header.as_ptr().add(1).cast::<u8>();
            std::ptr::copy_nonoverlapping(key.as_ptr(), bytes, key.len());
            std::ptr::copy_nonoverlapping(value.as_ptr(), bytes.add(key.len()), value.len());
        }
        charge.commit(MEMORY_ENTRY_OVERHEAD_BYTES + logical_bytes);
        Ok(MemoryValue(header))
    }

    #[cfg(test)]
    pub(crate) fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }

    fn take(
        &mut self,
        class: usize,
        block_bytes: usize,
    ) -> Option<(NonNull<MemoryValueHeader>, NonNull<ArenaChunk>)> {
        if let Some(chunk) = NonNull::new(self.current[class]) {
            // SAFETY: every current pointer is held by this arena's owner list.
            if let Some(header) = unsafe { chunk.as_ref() }.take(block_bytes) {
                return Some((header, chunk));
            }
        }

        if let Some(result) = self.scan_for_class(block_bytes) {
            self.current[class] = result.1.as_ptr();
            return Some(result);
        }
        if let Some(result) = self.reassign_empty(block_bytes) {
            self.current[class] = result.1.as_ptr();
            return Some(result);
        }
        let result = self.grow(block_bytes)?;
        self.current[class] = result.1.as_ptr();
        Some(result)
    }

    fn scan_for_class(
        &mut self,
        block_bytes: usize,
    ) -> Option<(NonNull<MemoryValueHeader>, NonNull<ArenaChunk>)> {
        let len = self.chunks.len();
        let scan = len.min(MAX_CHUNK_SCAN);
        for offset in 0..scan {
            let index = (self.scan_cursor + offset) % len;
            let chunk = self.chunks[index];
            // SAFETY: the arena owns a reference to every listed chunk.
            if let Some(header) = unsafe { chunk.as_ref() }.take(block_bytes) {
                self.scan_cursor = (index + 1) % len;
                return Some((header, chunk));
            }
        }
        if len != 0 {
            self.scan_cursor = (self.scan_cursor + scan) % len;
        }
        None
    }

    fn reassign_empty(
        &mut self,
        block_bytes: usize,
    ) -> Option<(NonNull<MemoryValueHeader>, NonNull<ArenaChunk>)> {
        let len = self.chunks.len();
        let scan = len.min(MAX_CHUNK_SCAN);
        for offset in 0..scan {
            let index = (self.scan_cursor + offset) % len;
            let chunk = self.chunks[index];
            // SAFETY: the arena owns a reference to every listed chunk.
            let chunk_ref = unsafe { chunk.as_ref() };
            if chunk_ref.data_bytes() >= block_bytes && chunk_ref.reset(block_bytes) {
                self.scan_cursor = (index + 1) % len;
                let header = chunk_ref
                    .take(block_bytes)
                    .expect("a rebuilt arena chunk contains a free block");
                return Some((header, chunk));
            }
        }
        if len != 0 {
            self.scan_cursor = (self.scan_cursor + scan) % len;
        }
        None
    }

    fn grow(
        &mut self,
        block_bytes: usize,
    ) -> Option<(NonNull<MemoryValueHeader>, NonNull<ArenaChunk>)> {
        let remaining = self.capacity_bytes.checked_sub(self.allocated_bytes)?;
        let shard_target = self.capacity_bytes / TARGET_CHUNKS_PER_ARENA;
        let target_bytes = block_bytes
            .max(shard_target.min(TARGET_CHUNK_BYTES))
            .min(remaining);
        let block_count = target_bytes / block_bytes;
        if block_count == 0 {
            return None;
        }
        let chunk_bytes = block_bytes.checked_mul(block_count)?;
        self.chunks.try_reserve(1).ok()?;
        let chunk = ArenaChunk::allocate(chunk_bytes, Arc::clone(&self.budget))?;
        // SAFETY: the new chunk has no live blocks or concurrent users.
        let chunk_ref = unsafe { chunk.as_ref() };
        let rebuilt = chunk_ref.reset(block_bytes);
        debug_assert!(rebuilt);
        let header = chunk_ref
            .take(block_bytes)
            .expect("a new arena chunk contains a free block");
        self.chunks.push(chunk);
        self.allocated_bytes += chunk_bytes;
        Some((header, chunk))
    }
}

impl Drop for MemoryArena {
    fn drop(&mut self) {
        for chunk in self.chunks.drain(..) {
            // SAFETY: draining transfers the arena's one owner reference.
            unsafe { ArenaChunk::release_reference(chunk) };
        }
    }
}

pub(crate) struct MemoryValue(NonNull<MemoryValueHeader>);

// SAFETY: the arena allocation is immutable after publication and references
// are tracked atomically.
unsafe impl Send for MemoryValue {}
// SAFETY: shared access exposes only immutable key/value bytes.
unsafe impl Sync for MemoryValue {}

impl MemoryValue {
    fn header(&self) -> &MemoryValueHeader {
        // SAFETY: every live handle owns a reference to an initialized block.
        unsafe { self.0.as_ref() }
    }

    fn key_length(&self) -> usize {
        (self.header().lengths & KEY_LENGTH_MASK) as usize
    }

    fn value_length(&self) -> usize {
        (self.header().lengths >> KEY_LENGTH_BITS) as usize
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        MEMORY_ENTRY_OVERHEAD_BYTES + self.key_length() + self.value_length()
    }

    fn bytes(&self) -> *const u8 {
        // SAFETY: payload bytes immediately follow the fixed header.
        unsafe { self.0.as_ptr().add(1).cast::<u8>() }
    }

    pub(crate) fn key(&self) -> &[u8] {
        // SAFETY: lengths were checked before the block was initialized.
        unsafe { std::slice::from_raw_parts(self.bytes(), self.key_length()) }
    }

    fn value(&self) -> &[u8] {
        // SAFETY: the immutable value follows the key in the same block.
        unsafe {
            std::slice::from_raw_parts(self.bytes().add(self.key_length()), self.value_length())
        }
    }
}

impl Clone for MemoryValue {
    fn clone(&self) -> Self {
        let previous = self.header().references.fetch_add(1, Ordering::Relaxed);
        if previous >= MAX_VALUE_REFERENCES {
            std::process::abort();
        }
        Self(self.0)
    }
}

impl Drop for MemoryValue {
    fn drop(&mut self) {
        if self.header().references.fetch_sub(1, Ordering::Release) != 1 {
            return;
        }
        fence(Ordering::Acquire);
        let logical_bytes = self.key_length() + self.value_length();
        let chunk = self.header().chunk;
        // SAFETY: this final block reference keeps the chunk alive until the
        // recycle, budget release, and reference release have all completed.
        unsafe {
            chunk.as_ref().recycle(self.0);
            chunk
                .as_ref()
                .budget
                .release(MEMORY_ENTRY_OVERHEAD_BYTES + logical_bytes);
            ArenaChunk::release_reference(chunk);
        }
    }
}

impl Deref for MemoryValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.value()
    }
}

impl AsRef<[u8]> for MemoryValue {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

fn class_index(required_bytes: usize) -> Option<usize> {
    if required_bytes == 0 || required_bytes > MAX_ITEM_BYTES {
        None
    } else if required_bytes <= SMALL_CLASS_LIMIT {
        Some((required_bytes - 1) / SMALL_CLASS_GRANULARITY)
    } else {
        Some(SMALL_CLASS_COUNT + (required_bytes - SMALL_CLASS_LIMIT - 1) / LARGE_CLASS_GRANULARITY)
    }
}

fn class_block_bytes(class: usize) -> usize {
    if class < SMALL_CLASS_COUNT {
        (class + 1) * SMALL_CLASS_GRANULARITY
    } else {
        SMALL_CLASS_LIMIT + (class - SMALL_CLASS_COUNT + 1) * LARGE_CLASS_GRANULARITY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_classes_round_small_and_large_values_directly() {
        assert_eq!(class_block_bytes(class_index(17).unwrap()), 24);
        assert_eq!(class_block_bytes(class_index(4 * 1024).unwrap()), 4 * 1024);
        assert_eq!(
            class_block_bytes(class_index(4 * 1024 + 1).unwrap()),
            8 * 1024
        );
    }
}
