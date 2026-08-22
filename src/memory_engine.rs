//! Bounded, sharded in-memory cache used by the hybrid-cache layer.
//!
//! The engine deliberately owns only L1 policy. Disk promotion/writeback and
//! same-key ordering stay in the hybrid coordinator. Each shard owns a fixed
//! share of the configured byte capacity, so independent shard locks cannot
//! oversubscribe a global budget. LRU is exact within one shard and therefore
//! approximate across the complete cache.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::hybrid_manifest::HybridVersion;

const MAX_SHARDS: usize = 4_096;

/// Logical metadata charged to every retained arena slot.
///
/// This covers the arena node, hash bucket, collision link, LRU links, and
/// retained free-slot bookkeeping with one deliberately simple constant. The
/// full `Vec` capacities of the owned key and shared value buffer are charged
/// separately, so a caller cannot smuggle a mostly unused oversized allocation
/// into L1.
/// Allocator bookkeeping and values cloned into caller-owned return objects
/// are outside the engine budget.
pub(crate) const MEMORY_ENTRY_OVERHEAD_BYTES: usize = 256;

pub(crate) type MemoryResult<T> = Result<T, MemoryError>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MemoryError {
    InvalidConfig(&'static str),
    AllocationFailed,
}

impl fmt::Display for MemoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => {
                write!(formatter, "invalid memory-cache config: {message}")
            }
            Self::AllocationFailed => formatter.write_str("memory-cache allocation failed"),
        }
    }
}

impl std::error::Error for MemoryError {}

/// Complete value and policy metadata transferred between L1 and the hybrid
/// coordinator.
///
/// Expiration is always an absolute Unix timestamp. `disk_clean` means the
/// disk tier already contains this exact logical version; a clean eviction may
/// therefore be discarded, while a dirty eviction must be handled by the
/// coordinator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryEntry {
    pub(crate) namespace: u32,
    pub(crate) key: Vec<u8>,
    /// Shared so an L1 hit only clones this handle while holding the shard lock.
    /// Keeping the original `Vec` behind the handle preserves exact capacity
    /// charging and avoids copying the payload when it is admitted into L1.
    pub(crate) value: Arc<Vec<u8>>,
    pub(crate) expires_at_unix_ms: Option<u64>,
    pub(crate) version: HybridVersion,
    pub(crate) disk_clean: bool,
    /// Namespace charge associated with this version. Dirty entries hold their
    /// conservative pending reservation; clean entries retain the exact
    /// lower-tier receipt for diagnostics and invariant-preserving transfers.
    pub(crate) pending_disk_bytes: u64,
}

impl MemoryEntry {
    #[cfg(test)]
    pub(crate) fn new(
        namespace: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at_unix_ms: Option<u64>,
        disk_clean: bool,
    ) -> Self {
        Self::new_versioned(
            namespace,
            key,
            value,
            expires_at_unix_ms,
            HybridVersion::ZERO,
            disk_clean,
        )
    }

    pub(crate) fn new_versioned(
        namespace: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at_unix_ms: Option<u64>,
        version: HybridVersion,
        disk_clean: bool,
    ) -> Self {
        Self::new_versioned_with_pending_disk_bytes(
            namespace,
            key,
            value,
            expires_at_unix_ms,
            version,
            disk_clean,
            0,
        )
    }

    pub(crate) fn new_versioned_with_pending_disk_bytes(
        namespace: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at_unix_ms: Option<u64>,
        version: HybridVersion,
        disk_clean: bool,
        pending_disk_bytes: u64,
    ) -> Self {
        Self {
            namespace,
            key,
            value: Arc::new(value),
            expires_at_unix_ms,
            version,
            disk_clean,
            pending_disk_bytes,
        }
    }

    pub(crate) fn charged_bytes(&self) -> Option<usize> {
        MEMORY_ENTRY_OVERHEAD_BYTES
            .checked_add(self.key.capacity())?
            .checked_add(self.value.capacity())
    }
}

/// A shared hit plus an identity token for compare-and-mark after asynchronous
/// disk I/O. The coordinator may copy the value after this method releases the
/// shard lock. A disk completion must use the revision it originally observed;
/// otherwise an old completion could mark a newer value clean.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MemoryHit {
    pub(crate) value: Arc<Vec<u8>>,
    pub(crate) version: HybridVersion,
    pub(crate) disk_clean: bool,
    pub(crate) revision: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MemoryLookup {
    Hit(MemoryHit),
    Miss,
    /// The expired value is removed from L1 and returned so the hybrid
    /// coordinator can prevent an older L2 version from becoming visible.
    Expired(MemoryEntry),
    /// The caller declined the destructive expiry transfer while the entry
    /// was still protected by its shard lock.
    ExpiryCommitRejected,
}

type DemoteCallback<'a> = dyn FnMut(&MemoryEntry) -> Option<u64> + 'a;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemoryRejectReason {
    TooLarge,
    AlreadyExpired,
    AllocationFailed,
    RevisionExhausted,
    /// Plain `put` never removes a dirty value. The coordinator may retry with
    /// `put_with_demote` or persist the incoming value directly to L2.
    DirtyVictimBlocked,
    DemotionFailed,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum MemoryPutResult {
    Stored {
        revision: u64,
        /// Live entries evicted after the coordinator either persisted each
        /// dirty value or transferred it to a bounded background owner.
        /// Their owned values are dropped in place instead of being collected
        /// into an unbounded temporary vector.
        evicted_entries: usize,
        evicted_bytes: usize,
    },
    NotStored {
        entry: MemoryEntry,
        reason: MemoryRejectReason,
    },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MemoryStats {
    pub(crate) capacity_bytes: usize,
    pub(crate) charged_bytes: usize,
    pub(crate) entries: usize,
    pub(crate) dirty_entries: usize,
    pub(crate) dirty_bytes: usize,
    pub(crate) hits: u64,
    pub(crate) misses: u64,
    pub(crate) puts: u64,
    pub(crate) rejected: u64,
    pub(crate) remove_requests: u64,
    pub(crate) removed: u64,
    pub(crate) evictions: u64,
    pub(crate) expirations: u64,
    pub(crate) clears: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryEntryUsage {
    pub(crate) disk_clean: bool,
    pub(crate) pending_disk_bytes: u64,
    pub(crate) expired: bool,
}

struct Node {
    entry: MemoryEntry,
    hash: u64,
    hash_next: Option<usize>,
    lru_prev: Option<usize>,
    lru_next: Option<usize>,
    revision: u64,
    charged_bytes: usize,
}

struct EmptyShardStorage {
    buckets: HashMap<u64, usize>,
    nodes: Vec<Option<Node>>,
    free: Vec<usize>,
}

struct Shard {
    capacity_bytes: usize,
    max_retained_slots: usize,
    retained_slots: usize,
    /// Retained-slot metadata plus resident key/value capacities.
    charged_bytes: usize,
    dirty_entries: usize,
    dirty_bytes: usize,
    buckets: HashMap<u64, usize>,
    nodes: Vec<Option<Node>>,
    free: Vec<usize>,
    lru_head: Option<usize>,
    lru_tail: Option<usize>,
    next_revision: u64,
    stats: MemoryStats,
}

impl Shard {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            max_retained_slots: capacity_bytes / MEMORY_ENTRY_OVERHEAD_BYTES,
            retained_slots: 0,
            charged_bytes: 0,
            dirty_entries: 0,
            dirty_bytes: 0,
            buckets: HashMap::new(),
            nodes: Vec::new(),
            free: Vec::new(),
            lru_head: None,
            lru_tail: None,
            next_revision: 1,
            stats: MemoryStats::default(),
        }
    }

    fn find(&self, hash: u64, namespace: u32, key: &[u8]) -> Option<usize> {
        let mut cursor = self.buckets.get(&hash).copied();
        while let Some(index) = cursor {
            let node = self.nodes.get(index)?.as_ref()?;
            if node.entry.namespace == namespace && node.entry.key.as_slice() == key {
                return Some(index);
            }
            cursor = node.hash_next;
        }
        None
    }

    fn resident_entries(&self) -> usize {
        self.nodes.len() - self.free.len()
    }

    #[cfg(test)]
    fn resident_payload_bytes(&self) -> usize {
        self.nodes
            .iter()
            .flatten()
            .map(|node| node.charged_bytes - MEMORY_ENTRY_OVERHEAD_BYTES)
            .sum()
    }

    fn next_slot_target(&self, max_slots_for_entry: usize) -> Option<usize> {
        if !self.free.is_empty() || self.nodes.len() < self.retained_slots {
            return Some(self.retained_slots);
        }
        let limit = self.max_retained_slots.min(max_slots_for_entry);
        if self.retained_slots >= limit {
            return None;
        }
        let doubled = self.retained_slots.saturating_mul(2).max(1);
        Some(doubled.min(limit))
    }

    /// Reserve the arena and removal free-list together. The caller commits
    /// `retained_slots` only after every fallible allocation has succeeded.
    fn reserve_slot_capacity(&mut self, target: usize) -> Result<(), MemoryRejectReason> {
        debug_assert!(target > self.retained_slots);
        debug_assert!(target <= self.max_retained_slots);
        let old_nodes_capacity = self.nodes.capacity();
        let old_free_capacity = self.free.capacity();

        if self.nodes.capacity() < target {
            self.nodes
                .try_reserve_exact(target - self.nodes.len())
                .map_err(|_| MemoryRejectReason::AllocationFailed)?;
        }
        if self.free.capacity() < target
            && self
                .free
                .try_reserve_exact(target - self.free.len())
                .is_err()
        {
            self.nodes.shrink_to(old_nodes_capacity);
            return Err(MemoryRejectReason::AllocationFailed);
        }
        debug_assert!(self.nodes.capacity() >= target);
        debug_assert!(self.free.capacity() >= target);
        debug_assert!(old_free_capacity <= self.free.capacity());
        Ok(())
    }

    fn rollback_slot_capacity(&mut self) {
        self.nodes
            .shrink_to(self.retained_slots.max(self.nodes.len()));
        self.free
            .shrink_to(self.retained_slots.max(self.free.len()));
    }

    fn commit_slot_capacity(&mut self, target: usize) {
        debug_assert!(target >= self.retained_slots);
        let added_slots = target - self.retained_slots;
        self.charged_bytes = self
            .charged_bytes
            .checked_add(added_slots * MEMORY_ENTRY_OVERHEAD_BYTES)
            .expect("validated memory capacity prevents charge overflow");
        self.retained_slots = target;
    }

    fn reset_storage(&mut self) {
        self.charged_bytes = 0;
        self.dirty_entries = 0;
        self.dirty_bytes = 0;
        self.retained_slots = 0;
        self.buckets = HashMap::new();
        self.nodes = Vec::new();
        self.free = Vec::new();
        self.lru_head = None;
        self.lru_tail = None;
    }

    fn reset_storage_if_empty(&mut self) {
        if self.resident_entries() == 0 {
            self.reset_storage();
        }
    }

    fn allocate_empty_storage(
        retained_slots: usize,
    ) -> Result<EmptyShardStorage, MemoryRejectReason> {
        debug_assert!(retained_slots != 0);
        let mut buckets = HashMap::new();
        buckets
            .try_reserve(1)
            .map_err(|_| MemoryRejectReason::AllocationFailed)?;
        let mut nodes = Vec::new();
        nodes
            .try_reserve_exact(retained_slots)
            .map_err(|_| MemoryRejectReason::AllocationFailed)?;
        let mut free = Vec::new();
        free.try_reserve_exact(retained_slots)
            .map_err(|_| MemoryRejectReason::AllocationFailed)?;
        Ok(EmptyShardStorage {
            buckets,
            nodes,
            free,
        })
    }

    fn install_empty_storage(&mut self, storage: EmptyShardStorage, retained_slots: usize) {
        debug_assert_eq!(self.resident_entries(), 0);
        self.buckets = storage.buckets;
        self.nodes = storage.nodes;
        self.free = storage.free;
        self.retained_slots = retained_slots;
        self.charged_bytes = retained_slots * MEMORY_ENTRY_OVERHEAD_BYTES;
        self.lru_head = None;
        self.lru_tail = None;
    }

    fn detach_lru(&mut self, index: usize) {
        let (previous, next) = {
            let node = self.nodes[index]
                .as_ref()
                .expect("LRU index must reference a resident node");
            (node.lru_prev, node.lru_next)
        };
        match previous {
            Some(previous) => {
                self.nodes[previous]
                    .as_mut()
                    .expect("LRU previous node must be resident")
                    .lru_next = next;
            }
            None => self.lru_head = next,
        }
        match next {
            Some(next) => {
                self.nodes[next]
                    .as_mut()
                    .expect("LRU next node must be resident")
                    .lru_prev = previous;
            }
            None => self.lru_tail = previous,
        }
    }

    fn attach_lru_head(&mut self, index: usize) {
        let old_head = self.lru_head;
        {
            let node = self.nodes[index]
                .as_mut()
                .expect("new LRU head must be resident");
            node.lru_prev = None;
            node.lru_next = old_head;
        }
        if let Some(old_head) = old_head {
            self.nodes[old_head]
                .as_mut()
                .expect("old LRU head must be resident")
                .lru_prev = Some(index);
        } else {
            self.lru_tail = Some(index);
        }
        self.lru_head = Some(index);
    }

    fn touch(&mut self, index: usize) {
        if self.lru_head == Some(index) {
            return;
        }
        self.detach_lru(index);
        self.attach_lru_head(index);
    }

    fn remove_index(&mut self, index: usize) -> Node {
        let (hash, hash_next, lru_prev, lru_next, charged_bytes) = {
            let node = self.nodes[index]
                .as_ref()
                .expect("removed index must reference a resident node");
            (
                node.hash,
                node.hash_next,
                node.lru_prev,
                node.lru_next,
                node.charged_bytes,
            )
        };

        let head = self
            .buckets
            .get(&hash)
            .copied()
            .expect("resident node must have a hash-chain head");
        if head == index {
            if let Some(next) = hash_next {
                self.buckets.insert(hash, next);
            } else {
                self.buckets.remove(&hash);
            }
        } else {
            let mut cursor = head;
            loop {
                let next = self.nodes[cursor]
                    .as_ref()
                    .expect("hash chain must reference resident nodes")
                    .hash_next;
                if next == Some(index) {
                    self.nodes[cursor]
                        .as_mut()
                        .expect("hash predecessor must be resident")
                        .hash_next = hash_next;
                    break;
                }
                cursor = next.expect("resident node must be reachable from hash head");
            }
        }

        match lru_prev {
            Some(previous) => {
                self.nodes[previous]
                    .as_mut()
                    .expect("LRU previous node must be resident")
                    .lru_next = lru_next;
            }
            None => self.lru_head = lru_next,
        }
        match lru_next {
            Some(next) => {
                self.nodes[next]
                    .as_mut()
                    .expect("LRU next node must be resident")
                    .lru_prev = lru_prev;
            }
            None => self.lru_tail = lru_prev,
        }

        let node = self.nodes[index]
            .take()
            .expect("removed index must reference a resident node");
        if !node.entry.disk_clean {
            self.dirty_entries = self.dirty_entries.saturating_sub(1);
            self.dirty_bytes = self.dirty_bytes.saturating_sub(node.charged_bytes);
        }
        self.free.push(index);
        self.charged_bytes -= charged_bytes - MEMORY_ENTRY_OVERHEAD_BYTES;
        node
    }

    fn insert_node(&mut self, node: Node) -> usize {
        let hash = node.hash;
        if !node.entry.disk_clean {
            self.dirty_entries += 1;
            self.dirty_bytes += node.charged_bytes;
        }
        let index = if let Some(index) = self.free.pop() {
            debug_assert!(self.nodes[index].is_none());
            self.nodes[index] = Some(node);
            index
        } else {
            debug_assert!(self.nodes.len() < self.retained_slots);
            let index = self.nodes.len();
            self.nodes.push(Some(node));
            index
        };
        let old_head = self.buckets.insert(hash, index);
        self.nodes[index]
            .as_mut()
            .expect("inserted node must be resident")
            .hash_next = old_head;
        self.attach_lru_head(index);
        self.charged_bytes += self.nodes[index]
            .as_ref()
            .expect("inserted node must be resident")
            .charged_bytes
            - MEMORY_ENTRY_OVERHEAD_BYTES;
        debug_assert!(self.charged_bytes <= self.capacity_bytes);
        index
    }

    fn snapshot(&self) -> MemoryStats {
        let mut snapshot = self.stats;
        snapshot.capacity_bytes = self.capacity_bytes;
        snapshot.charged_bytes = self.charged_bytes;
        snapshot.entries = self.resident_entries();
        snapshot.dirty_entries = self.dirty_entries;
        snapshot.dirty_bytes = self.dirty_bytes;
        snapshot
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        assert!(self.charged_bytes <= self.capacity_bytes);
        assert!(self.retained_slots <= self.max_retained_slots);
        assert!(self.nodes.len() <= self.retained_slots);
        assert!(self.nodes.capacity() >= self.retained_slots);
        assert!(self.free.capacity() >= self.retained_slots);
        let resident = self.nodes.iter().filter(|node| node.is_some()).count();
        assert_eq!(resident, self.resident_entries());
        assert_eq!(
            self.charged_bytes,
            self.retained_slots * MEMORY_ENTRY_OVERHEAD_BYTES + self.resident_payload_bytes()
        );
        assert_eq!(
            self.dirty_entries,
            self.nodes
                .iter()
                .flatten()
                .filter(|node| !node.entry.disk_clean)
                .count()
        );
        assert_eq!(
            self.dirty_bytes,
            self.nodes
                .iter()
                .flatten()
                .filter(|node| !node.entry.disk_clean)
                .map(|node| node.charged_bytes)
                .sum()
        );

        let mut lru_count = 0;
        let mut cursor = self.lru_head;
        let mut expected_previous = None;
        while let Some(index) = cursor {
            let node = self.nodes[index].as_ref().unwrap();
            assert_eq!(node.lru_prev, expected_previous);
            expected_previous = Some(index);
            cursor = node.lru_next;
            lru_count += 1;
        }
        assert_eq!(expected_previous, self.lru_tail);
        assert_eq!(lru_count, resident);
    }
}

/// Fixed-capacity, sharded LRU memory tier.
pub(crate) struct MemoryEngine {
    capacity_bytes: usize,
    shard_mask: usize,
    operation_barrier: RwLock<()>,
    shards: Vec<Mutex<Shard>>,
    clears: Mutex<u64>,
}

impl MemoryEngine {
    pub(crate) fn new(capacity_bytes: usize, shard_count: usize) -> MemoryResult<Self> {
        if shard_count == 0 || !shard_count.is_power_of_two() {
            return Err(MemoryError::InvalidConfig(
                "shard_count must be a non-zero power of two",
            ));
        }
        if shard_count > MAX_SHARDS {
            return Err(MemoryError::InvalidConfig(
                "shard_count exceeds the hard limit of 4096",
            ));
        }
        let minimum_capacity = shard_count
            .checked_mul(MEMORY_ENTRY_OVERHEAD_BYTES)
            .ok_or(MemoryError::InvalidConfig("capacity arithmetic overflow"))?;
        if capacity_bytes < minimum_capacity {
            return Err(MemoryError::InvalidConfig(
                "capacity must allow at least one minimum-size entry per shard",
            ));
        }

        let mut shards = Vec::new();
        shards
            .try_reserve_exact(shard_count)
            .map_err(|_| MemoryError::AllocationFailed)?;
        let base = capacity_bytes / shard_count;
        let remainder = capacity_bytes % shard_count;
        for index in 0..shard_count {
            shards.push(Mutex::new(Shard::new(
                base + usize::from(index < remainder),
            )));
        }
        Ok(Self {
            capacity_bytes,
            shard_mask: shard_count - 1,
            operation_barrier: RwLock::new(()),
            shards,
            clears: Mutex::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn get(&self, namespace: u32, key: &[u8]) -> MemoryResult<MemoryLookup> {
        self.get_at_with_reservation(namespace, key, now_unix_ms(), |_| true, || true)
    }

    /// Clone or transfer a hit only after the caller has reserved the exact
    /// temporary bytes that will leave the L1 budget. A live hit reserves its
    /// returned value clone; an expired hit reserves the complete owned entry
    /// before removing it from the shard. The callback runs under the shard
    /// lock and must not call back into this memory engine.
    pub(crate) fn get_with_reservation<F, C>(
        &self,
        namespace: u32,
        key: &[u8],
        reserve: F,
        commit_expiry: C,
    ) -> MemoryResult<MemoryLookup>
    where
        F: FnOnce(usize) -> bool,
        C: FnOnce() -> bool,
    {
        self.get_at_with_reservation(namespace, key, now_unix_ms(), reserve, commit_expiry)
    }

    /// Clone a live hit without transferring expired state. Hybrid uses this
    /// optimistic probe before taking its slower per-key disk-ordering lock;
    /// a miss or expired entry is rechecked under that lock.
    pub(crate) fn get_live_with_reservation<F>(
        &self,
        namespace: u32,
        key: &[u8],
        reserve: F,
    ) -> MemoryResult<Option<MemoryHit>>
    where
        F: FnOnce(usize) -> bool,
    {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(namespace, key);
        let mut shard = self.lock_shard(hash);
        let Some(index) = shard.find(hash, namespace, key) else {
            return Ok(None);
        };
        let entry = &shard.nodes[index]
            .as_ref()
            .expect("found index must be resident")
            .entry;
        if is_expired(entry, now_unix_ms()) {
            return Ok(None);
        }
        if !reserve(entry.value.len()) {
            return Err(MemoryError::AllocationFailed);
        }
        let hit = {
            let node = shard.nodes[index]
                .as_ref()
                .expect("found index must be resident");
            MemoryHit {
                value: Arc::clone(&node.entry.value),
                version: node.entry.version,
                disk_clean: node.entry.disk_clean,
                revision: node.revision,
            }
        };
        shard.touch(index);
        shard.stats.hits = shard.stats.hits.saturating_add(1);
        Ok(Some(hit))
    }

    /// Return allocation-free accounting metadata without changing LRU order
    /// or public hit/miss counters. Hybrid write-back uses this to retire the
    /// logical charge of a replaced dirty value exactly once.
    pub(crate) fn entry_usage(&self, namespace: u32, key: &[u8]) -> Option<MemoryEntryUsage> {
        let now = now_unix_ms();
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(namespace, key);
        let shard = self.lock_shard(hash);
        shard.find(hash, namespace, key).map(|index| {
            let entry = &shard.nodes[index]
                .as_ref()
                .expect("found index must be resident")
                .entry;
            MemoryEntryUsage {
                disk_clean: entry.disk_clean,
                pending_disk_bytes: entry.pending_disk_bytes,
                expired: is_expired(entry, now),
            }
        })
    }

    #[cfg(test)]
    fn get_at(&self, namespace: u32, key: &[u8], now_unix_ms: u64) -> MemoryResult<MemoryLookup> {
        self.get_at_with_reservation(namespace, key, now_unix_ms, |_| true, || true)
    }

    fn get_at_with_reservation<F, C>(
        &self,
        namespace: u32,
        key: &[u8],
        now_unix_ms: u64,
        reserve: F,
        commit_expiry: C,
    ) -> MemoryResult<MemoryLookup>
    where
        F: FnOnce(usize) -> bool,
        C: FnOnce() -> bool,
    {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(namespace, key);
        let mut shard = self.lock_shard(hash);
        let Some(index) = shard.find(hash, namespace, key) else {
            shard.stats.misses = shard.stats.misses.saturating_add(1);
            return Ok(MemoryLookup::Miss);
        };
        let entry = &shard.nodes[index]
            .as_ref()
            .expect("found index must be resident")
            .entry;
        let expired = is_expired(entry, now_unix_ms);
        let temporary_bytes = if expired {
            entry.charged_bytes().ok_or(MemoryError::AllocationFailed)?
        } else {
            entry.value.len()
        };
        if !reserve(temporary_bytes) {
            return Err(MemoryError::AllocationFailed);
        }
        if expired {
            if !commit_expiry() {
                return Ok(MemoryLookup::ExpiryCommitRejected);
            }
            let expired = shard.remove_index(index).entry;
            shard.reset_storage_if_empty();
            shard.stats.expirations = shard.stats.expirations.saturating_add(1);
            shard.stats.misses = shard.stats.misses.saturating_add(1);
            return Ok(MemoryLookup::Expired(expired));
        }

        let hit = {
            let node = shard.nodes[index]
                .as_ref()
                .expect("found index must be resident");
            MemoryHit {
                value: Arc::clone(&node.entry.value),
                version: node.entry.version,
                disk_clean: node.entry.disk_clean,
                revision: node.revision,
            }
        };
        shard.touch(index);
        shard.stats.hits = shard.stats.hits.saturating_add(1);
        Ok(MemoryLookup::Hit(hit))
    }

    pub(crate) fn put(&self, entry: MemoryEntry) -> MemoryPutResult {
        self.put_at_with_demote(entry, now_unix_ms(), None)
    }

    #[cfg(test)]
    fn put_at(&self, entry: MemoryEntry, now_unix_ms: u64) -> MemoryPutResult {
        self.put_at_with_demote(entry, now_unix_ms, None)
    }

    /// Admit an entry while preparing dirty victims under the shard lock. The
    /// callback must either persist the exact supplied entry or transfer it to
    /// a bounded background owner before returning. It returns the exact
    /// lower-tier live bytes for a completed persistence, or zero for an
    /// accepted disposable-cache handoff, and must not call back into this
    /// memory engine. A failed callback leaves the incoming entry out of L1 and
    /// keeps every victim resident. Earlier successful preparations remain
    /// committed, which is safe and avoids rollback I/O.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn put_with_demote<F>(&self, entry: MemoryEntry, mut demote: F) -> MemoryPutResult
    where
        F: FnMut(&MemoryEntry) -> Option<u64>,
    {
        self.put_at_with_demote(entry, now_unix_ms(), Some(&mut demote))
    }

    fn put_at_with_demote(
        &self,
        entry: MemoryEntry,
        now_unix_ms: u64,
        mut demote: Option<&mut DemoteCallback<'_>>,
    ) -> MemoryPutResult {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(entry.namespace, &entry.key);
        let mut shard = self.lock_shard(hash);
        shard.stats.puts = shard.stats.puts.saturating_add(1);
        let Some(charged_bytes) = entry.charged_bytes() else {
            shard.stats.rejected = shard.stats.rejected.saturating_add(1);
            return MemoryPutResult::NotStored {
                entry,
                reason: MemoryRejectReason::TooLarge,
            };
        };
        if is_expired(&entry, now_unix_ms) {
            shard.stats.rejected = shard.stats.rejected.saturating_add(1);
            return MemoryPutResult::NotStored {
                entry,
                reason: MemoryRejectReason::AlreadyExpired,
            };
        }

        if charged_bytes > shard.capacity_bytes {
            shard.stats.rejected = shard.stats.rejected.saturating_add(1);
            return MemoryPutResult::NotStored {
                entry,
                reason: MemoryRejectReason::TooLarge,
            };
        }
        if shard.next_revision == u64::MAX {
            shard.stats.rejected = shard.stats.rejected.saturating_add(1);
            return MemoryPutResult::NotStored {
                entry,
                reason: MemoryRejectReason::RevisionExhausted,
            };
        }

        let incoming_payload_bytes = charged_bytes - MEMORY_ENTRY_OVERHEAD_BYTES;
        let max_slots_for_entry =
            (shard.capacity_bytes - incoming_payload_bytes) / MEMORY_ENTRY_OVERHEAD_BYTES;
        let replaced = shard.find(hash, entry.namespace, &entry.key);
        let replaced_payload_bytes = replaced
            .and_then(|index| shard.nodes[index].as_ref())
            .map_or(0, |node| node.charged_bytes - MEMORY_ENTRY_OVERHEAD_BYTES);
        let after_replacement = shard.charged_bytes - replaced_payload_bytes;
        let slot_target = if replaced.is_some() {
            Some(shard.retained_slots)
        } else {
            shard.next_slot_target(max_slots_for_entry)
        };
        let growth_target = slot_target.filter(|target| *target > shard.retained_slots);
        let growth_bytes = growth_target.map_or(0, |target| {
            (target - shard.retained_slots) * MEMORY_ENTRY_OVERHEAD_BYTES
        });
        let bytes_needed = after_replacement
            .saturating_add(growth_bytes)
            .saturating_add(incoming_payload_bytes)
            .saturating_sub(shard.capacity_bytes);
        let victim_must_supply_slot = slot_target.is_none();

        // Plan against reclaimable payload bytes. Retained arena metadata does
        // not disappear when one node is evicted; if the old high-water mark
        // itself is incompatible with the incoming object, every victim is
        // prepared and the empty shard is rebuilt at one slot.
        let mut victims_needed = 0_usize;
        let mut victim_payload_bytes = 0_usize;
        let mut cursor = shard.lru_tail;
        while victim_payload_bytes < bytes_needed
            || (victim_must_supply_slot && victims_needed == 0)
        {
            let Some(index) = cursor else {
                break;
            };
            let node = shard.nodes[index]
                .as_ref()
                .expect("LRU index must reference a resident node");
            cursor = node.lru_prev;
            if Some(index) == replaced {
                continue;
            }
            victims_needed += 1;
            victim_payload_bytes = victim_payload_bytes
                .saturating_add(node.charged_bytes - MEMORY_ENTRY_OVERHEAD_BYTES);
        }
        let rebuild_storage =
            victim_payload_bytes < bytes_needed || (victim_must_supply_slot && victims_needed == 0);
        if rebuild_storage {
            victims_needed = shard.resident_entries() - usize::from(replaced.is_some());
        }

        // Prepare every dirty victim before changing membership. This closes
        // the L1-miss/L2-stale window and makes failure non-destructive. The
        // hybrid coordinator should use the same shard-ordering domain around
        // this call when L2 reads and writes can race.
        let mut victims_prepared = 0_usize;
        let mut cursor = shard.lru_tail;
        while victims_prepared < victims_needed {
            let index = cursor.expect("planned victims must remain resident");
            let (previous, is_replaced, is_dirty) = {
                let node = shard.nodes[index]
                    .as_ref()
                    .expect("LRU index must reference a resident node");
                (
                    node.lru_prev,
                    Some(index) == replaced,
                    !node.entry.disk_clean,
                )
            };
            cursor = previous;
            if is_replaced {
                continue;
            }
            if is_dirty {
                let Some(callback) = demote.as_deref_mut() else {
                    shard.stats.rejected = shard.stats.rejected.saturating_add(1);
                    return MemoryPutResult::NotStored {
                        entry,
                        reason: MemoryRejectReason::DirtyVictimBlocked,
                    };
                };
                let disk_live_bytes = {
                    let node = shard.nodes[index]
                        .as_ref()
                        .expect("LRU index must reference a resident node");
                    callback(&node.entry)
                };
                let Some(disk_live_bytes) = disk_live_bytes else {
                    shard.stats.rejected = shard.stats.rejected.saturating_add(1);
                    return MemoryPutResult::NotStored {
                        entry,
                        reason: MemoryRejectReason::DemotionFailed,
                    };
                };
                let charged_bytes = shard.nodes[index]
                    .as_ref()
                    .expect("prepared victim must remain resident")
                    .charged_bytes;
                let prepared = &mut shard.nodes[index]
                    .as_mut()
                    .expect("prepared victim must remain resident")
                    .entry;
                prepared.disk_clean = true;
                prepared.pending_disk_bytes = disk_live_bytes;
                shard.dirty_entries = shard.dirty_entries.saturating_sub(1);
                shard.dirty_bytes = shard.dirty_bytes.saturating_sub(charged_bytes);
            }
            victims_prepared += 1;
        }

        let new_hash_bucket = !shard.buckets.contains_key(&hash);
        let replacement_storage = if rebuild_storage {
            match Shard::allocate_empty_storage(1) {
                Ok(storage) => Some(storage),
                Err(reason) => {
                    shard.stats.rejected = shard.stats.rejected.saturating_add(1);
                    return MemoryPutResult::NotStored { entry, reason };
                }
            }
        } else {
            if let Some(target) = growth_target {
                if let Err(reason) = shard.reserve_slot_capacity(target) {
                    shard.stats.rejected = shard.stats.rejected.saturating_add(1);
                    return MemoryPutResult::NotStored { entry, reason };
                }
            }
            if new_hash_bucket && shard.buckets.try_reserve(1).is_err() {
                if growth_target.is_some() {
                    shard.rollback_slot_capacity();
                }
                shard.stats.rejected = shard.stats.rejected.saturating_add(1);
                return MemoryPutResult::NotStored {
                    entry,
                    reason: MemoryRejectReason::AllocationFailed,
                };
            }
            None
        };

        // All fallible allocations are complete. From here the replacement is
        // atomic while this shard lock is held.
        if let Some(index) = replaced {
            let _stale = shard.remove_index(index);
        }
        let mut evicted_entries = 0;
        let mut evicted_bytes = 0_usize;
        for _ in 0..victims_needed {
            let oldest = shard
                .lru_tail
                .expect("planned capacity victim must remain resident");
            let victim = shard.remove_index(oldest);
            if is_expired(&victim.entry, now_unix_ms) {
                shard.stats.expirations = shard.stats.expirations.saturating_add(1);
            } else {
                shard.stats.evictions = shard.stats.evictions.saturating_add(1);
                evicted_entries += 1;
                evicted_bytes = evicted_bytes.saturating_add(victim.charged_bytes);
            }
        }

        if let Some(storage) = replacement_storage {
            debug_assert_eq!(shard.resident_entries(), 0);
            shard.install_empty_storage(storage, 1);
        } else if let Some(target) = growth_target {
            shard.commit_slot_capacity(target);
        }
        debug_assert!(
            shard.charged_bytes.saturating_add(incoming_payload_bytes) <= shard.capacity_bytes
        );
        let revision = shard.next_revision;
        shard.next_revision += 1;
        shard.insert_node(Node {
            entry,
            hash,
            hash_next: None,
            lru_prev: None,
            lru_next: None,
            revision,
            charged_bytes,
        });
        MemoryPutResult::Stored {
            revision,
            evicted_entries,
            evicted_bytes,
        }
    }

    pub(crate) fn remove(&self, namespace: u32, key: &[u8]) -> Option<MemoryEntry> {
        self.remove_at(namespace, key, now_unix_ms())
    }

    fn remove_at(&self, namespace: u32, key: &[u8], now_unix_ms: u64) -> Option<MemoryEntry> {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(namespace, key);
        let mut shard = self.lock_shard(hash);
        shard.stats.remove_requests = shard.stats.remove_requests.saturating_add(1);
        let index = shard.find(hash, namespace, key)?;
        let node = shard.remove_index(index);
        shard.reset_storage_if_empty();
        if is_expired(&node.entry, now_unix_ms) {
            shard.stats.expirations = shard.stats.expirations.saturating_add(1);
        }
        shard.stats.removed = shard.stats.removed.saturating_add(1);
        Some(node.entry)
    }

    /// Mark exactly the revision completed by an asynchronous disk write.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn mark_disk_clean_if(&self, namespace: u32, key: &[u8], revision: u64) -> bool {
        self.set_disk_clean_if(namespace, key, revision, true)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn mark_disk_dirty_if(&self, namespace: u32, key: &[u8], revision: u64) -> bool {
        self.set_disk_clean_if(namespace, key, revision, false)
    }

    fn set_disk_clean_if(
        &self,
        namespace: u32,
        key: &[u8],
        revision: u64,
        disk_clean: bool,
    ) -> bool {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let hash = hash_key(namespace, key);
        let mut shard = self.lock_shard(hash);
        let Some(index) = shard.find(hash, namespace, key) else {
            return false;
        };
        let node = shard.nodes[index]
            .as_ref()
            .expect("found index must be resident");
        if node.revision != revision {
            return false;
        }
        if node.entry.disk_clean == disk_clean {
            return true;
        }
        let charged_bytes = node.charged_bytes;
        if disk_clean {
            shard.dirty_entries = shard.dirty_entries.saturating_sub(1);
            shard.dirty_bytes = shard.dirty_bytes.saturating_sub(charged_bytes);
        } else {
            shard.dirty_entries += 1;
            shard.dirty_bytes += charged_bytes;
        }
        shard.nodes[index]
            .as_mut()
            .expect("found index must be resident")
            .entry
            .disk_clean = disk_clean;
        true
    }

    /// Persist every dirty entry and mark it clean only after the callback
    /// succeeds. Hybrid calls this while its global operation barrier excludes
    /// foreground requests, so callbacks may perform blocking disk I/O.
    pub(crate) fn persist_all_dirty<F, E>(
        &self,
        parallelism: usize,
        persist: F,
    ) -> Result<(usize, usize), E>
    where
        F: Fn(&MemoryEntry) -> Result<u64, E> + Sync,
        E: Send,
    {
        let _operation = write_unpoisoned(&self.operation_barrier);
        let worker_count = parallelism.max(1).min(self.shards.len());
        let next_shard = AtomicUsize::new(0);
        let stopped = AtomicBool::new(false);
        let failure = Mutex::new(None);
        let entries = AtomicUsize::new(0);
        let bytes = AtomicUsize::new(0);
        thread::scope(|scope| {
            for _ in 0..worker_count {
                scope.spawn(|| {
                    loop {
                        if stopped.load(Ordering::Acquire) {
                            break;
                        }
                        let shard_index = next_shard.fetch_add(1, Ordering::Relaxed);
                        let Some(shard) = self.shards.get(shard_index) else {
                            break;
                        };
                        let mut shard = lock_unpoisoned(shard);
                        for index in 0..shard.nodes.len() {
                            if stopped.load(Ordering::Acquire) {
                                break;
                            }
                            let Some(node) = shard.nodes[index].as_ref() else {
                                continue;
                            };
                            if node.entry.disk_clean {
                                continue;
                            }
                            let disk_live_bytes = match persist(&node.entry) {
                                Ok(bytes) => bytes,
                                Err(error) => {
                                    let mut first = lock_unpoisoned(&failure);
                                    if first.is_none() {
                                        *first = Some(error);
                                    }
                                    stopped.store(true, Ordering::Release);
                                    break;
                                }
                            };
                            let charged_bytes = node.charged_bytes;
                            shard.nodes[index]
                                .as_mut()
                                .expect("dirty entry must remain resident during persistence")
                                .entry
                                .disk_clean = true;
                            shard.nodes[index]
                                .as_mut()
                                .expect("dirty entry must remain resident during persistence")
                                .entry
                                .pending_disk_bytes = disk_live_bytes;
                            shard.dirty_entries = shard.dirty_entries.saturating_sub(1);
                            shard.dirty_bytes = shard.dirty_bytes.saturating_sub(charged_bytes);
                            entries.fetch_add(1, Ordering::Relaxed);
                            bytes.fetch_add(charged_bytes, Ordering::Relaxed);
                        }
                    }
                });
            }
        });
        if let Some(error) = lock_unpoisoned(&failure).take() {
            return Err(error);
        }
        Ok((
            entries.load(Ordering::Relaxed),
            bytes.load(Ordering::Relaxed),
        ))
    }

    /// Atomically discard all memory-tier entries relative to other memory
    /// operations. Dirty values are intentionally not returned: hybrid clear
    /// also invalidates the disk tier, so writing them back would be wrong.
    pub(crate) fn clear(&self) -> usize {
        let _operation = write_unpoisoned(&self.operation_barrier);
        let mut removed = 0;
        for shard in &self.shards {
            let mut shard = lock_unpoisoned(shard);
            removed += shard.resident_entries();
            shard.reset_storage();
        }
        let mut clears = lock_unpoisoned(&self.clears);
        *clears = clears.saturating_add(1);
        removed
    }

    /// Bounded-cost operational stats. The shared barrier and per-shard locks
    /// keep each shard internally consistent without stopping unrelated L1
    /// gets/puts across the entire cache merely because metrics are scraped.
    /// Cross-shard totals are therefore an intentionally weak snapshot.
    pub(crate) fn stats(&self) -> MemoryStats {
        let _operation = read_unpoisoned(&self.operation_barrier);
        let mut total = MemoryStats {
            capacity_bytes: self.capacity_bytes,
            clears: *lock_unpoisoned(&self.clears),
            ..MemoryStats::default()
        };
        for shard in &self.shards {
            let snapshot = lock_unpoisoned(shard).snapshot();
            total.charged_bytes += snapshot.charged_bytes;
            total.entries += snapshot.entries;
            total.dirty_entries += snapshot.dirty_entries;
            total.dirty_bytes += snapshot.dirty_bytes;
            total.hits = total.hits.saturating_add(snapshot.hits);
            total.misses = total.misses.saturating_add(snapshot.misses);
            total.puts = total.puts.saturating_add(snapshot.puts);
            total.rejected = total.rejected.saturating_add(snapshot.rejected);
            total.remove_requests = total
                .remove_requests
                .saturating_add(snapshot.remove_requests);
            total.removed = total.removed.saturating_add(snapshot.removed);
            total.evictions = total.evictions.saturating_add(snapshot.evictions);
            total.expirations = total.expirations.saturating_add(snapshot.expirations);
        }
        debug_assert!(total.charged_bytes <= total.capacity_bytes);
        total
    }

    fn lock_shard(&self, hash: u64) -> MutexGuard<'_, Shard> {
        lock_unpoisoned(&self.shards[(hash as usize) & self.shard_mask])
    }

    #[cfg(test)]
    fn assert_invariants(&self) {
        let _operation = write_unpoisoned(&self.operation_barrier);
        let mut capacity = 0;
        let mut charged = 0;
        for shard in &self.shards {
            let shard = lock_unpoisoned(shard);
            shard.assert_invariants();
            capacity += shard.capacity_bytes;
            charged += shard.charged_bytes;
        }
        assert_eq!(capacity, self.capacity_bytes);
        assert!(charged <= capacity);
    }
}

fn is_expired(entry: &MemoryEntry, now_unix_ms: u64) -> bool {
    entry
        .expires_at_unix_ms
        .is_some_and(|expires_at| expires_at <= now_unix_ms)
}

fn hash_key(namespace: u32, key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in namespace.to_le_bytes().iter().chain(key) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100_0000_01b3);
    }
    hash
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_unpoisoned(lock: &RwLock<()>) -> RwLockReadGuard<'_, ()> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_unpoisoned(lock: &RwLock<()>) -> RwLockWriteGuard<'_, ()> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    fn entry(namespace: u32, key: &str, value_len: usize) -> MemoryEntry {
        MemoryEntry::new(
            namespace,
            key.as_bytes().to_vec(),
            vec![key.as_bytes()[0]; value_len],
            None,
            true,
        )
    }

    fn stored(result: MemoryPutResult) -> (u64, usize, usize) {
        match result {
            MemoryPutResult::Stored {
                revision,
                evicted_entries,
                evicted_bytes,
            } => (revision, evicted_entries, evicted_bytes),
            other => panic!("expected stored result, got {other:?}"),
        }
    }

    fn hit(result: MemoryResult<MemoryLookup>) -> MemoryHit {
        match result.unwrap() {
            MemoryLookup::Hit(hit) => hit,
            other => panic!("expected memory hit, got {other:?}"),
        }
    }

    #[test]
    fn configuration_and_per_shard_capacity_are_hard_bounded() {
        assert!(matches!(
            MemoryEngine::new(1_024, 0),
            Err(MemoryError::InvalidConfig(_))
        ));
        assert!(matches!(
            MemoryEngine::new(1_024, 3),
            Err(MemoryError::InvalidConfig(_))
        ));
        assert!(matches!(
            MemoryEngine::new(1_024, 8),
            Err(MemoryError::InvalidConfig(_))
        ));

        let capacity = 4 * MEMORY_ENTRY_OVERHEAD_BYTES + 3;
        let engine = MemoryEngine::new(capacity, 4).unwrap();
        assert_eq!(engine.stats().capacity_bytes, capacity);
        engine.assert_invariants();

        let single_shard = MemoryEngine::new(MEMORY_ENTRY_OVERHEAD_BYTES + 16, 1).unwrap();
        let mut overallocated_key = Vec::with_capacity(32);
        overallocated_key.push(b'k');
        assert!(matches!(
            single_shard.put(MemoryEntry::new(
                0,
                overallocated_key,
                Vec::new(),
                None,
                true
            )),
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::TooLarge,
                ..
            }
        ));
    }

    #[test]
    fn lru_eviction_returns_oldest_live_victims_and_never_exceeds_capacity() {
        let item_bytes = MEMORY_ENTRY_OVERHEAD_BYTES + 1 + 16;
        let engine = MemoryEngine::new(item_bytes * 2, 1).unwrap();
        stored(engine.put(entry(0, "a", 16)));
        stored(engine.put(entry(0, "b", 16)));
        hit(engine.get_at(0, b"a", 10));

        let (_, evicted, evicted_bytes) = stored(engine.put_at(entry(0, "c", 16), 10));
        assert_eq!(evicted, 1);
        assert_eq!(evicted_bytes, item_bytes);
        hit(engine.get_at(0, b"a", 10));
        assert_eq!(engine.get_at(0, b"b", 10).unwrap(), MemoryLookup::Miss);
        hit(engine.get_at(0, b"c", 10));

        let stats = engine.stats();
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.charged_bytes, stats.capacity_bytes);
        assert_eq!(stats.evictions, 1);
        engine.assert_invariants();
    }

    #[test]
    fn rejection_is_non_destructive_and_expiry_releases_the_charge() {
        let capacity = MEMORY_ENTRY_OVERHEAD_BYTES + 3 + 8;
        let engine = MemoryEngine::new(capacity, 1).unwrap();
        stored(engine.put_at(
            MemoryEntry::new(7, b"key".to_vec(), b"original".to_vec(), None, true),
            100,
        ));

        let rejected = engine.put_at(
            MemoryEntry::new(7, b"key".to_vec(), vec![0; 9], None, false),
            100,
        );
        assert!(matches!(
            rejected,
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::TooLarge,
                ..
            }
        ));
        assert_eq!(
            hit(engine.get_at(7, b"key", 100)).value.as_slice(),
            b"original"
        );

        let expired_update = engine.put_at(
            MemoryEntry::new(7, b"key".to_vec(), b"new".to_vec(), Some(100), false),
            100,
        );
        assert!(matches!(
            expired_update,
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::AlreadyExpired,
                ..
            }
        ));
        hit(engine.get_at(7, b"key", 100));

        let (_, evicted, _) = stored(engine.put_at(
            MemoryEntry::new(9, b"x".to_vec(), b"v".to_vec(), Some(110), false),
            100,
        ));
        assert_eq!(evicted, 1);
        assert!(matches!(
            engine.get_at(9, b"x", 110).unwrap(),
            MemoryLookup::Expired(_)
        ));
        assert_eq!(engine.stats().charged_bytes, 0);
        engine.assert_invariants();
    }

    #[test]
    fn revision_fences_stale_disk_completions() {
        let engine = MemoryEngine::new(4 * MEMORY_ENTRY_OVERHEAD_BYTES, 1).unwrap();
        let (first_revision, _, _) = stored(engine.put(entry(0, "a", 1)));
        let (second_revision, _, _) = stored(engine.put(entry(0, "a", 2)));
        assert_ne!(first_revision, second_revision);
        assert!(!engine.mark_disk_clean_if(0, b"a", first_revision));
        assert!(engine.mark_disk_clean_if(0, b"a", second_revision));
        assert!(hit(engine.get(0, b"a")).disk_clean);
        assert!(engine.mark_disk_dirty_if(0, b"a", second_revision));
        assert!(!hit(engine.get(0, b"a")).disk_clean);
    }

    #[test]
    fn hits_share_the_admitted_value_allocation_and_preserve_its_charge() {
        let engine = MemoryEngine::new(MEMORY_ENTRY_OVERHEAD_BYTES + 64, 1).unwrap();
        let mut value = Vec::with_capacity(63);
        value.extend_from_slice(b"shared");
        let entry = MemoryEntry::new(0, b"k".to_vec(), value, None, true);
        let expected_charge = entry.charged_bytes().unwrap();
        let admitted_value = Arc::clone(&entry.value);

        stored(engine.put(entry));
        let first = hit(engine.get(0, b"k"));
        let second = hit(engine.get(0, b"k"));

        assert!(Arc::ptr_eq(&admitted_value, &first.value));
        assert!(Arc::ptr_eq(&first.value, &second.value));
        assert_eq!(first.value.as_slice(), b"shared");
        assert_eq!(engine.stats().charged_bytes, expected_charge);
        engine.assert_invariants();
    }

    #[test]
    fn namespace_remove_and_clear_have_unambiguous_semantics() {
        let engine = MemoryEngine::new(8 * MEMORY_ENTRY_OVERHEAD_BYTES, 1).unwrap();
        stored(engine.put(entry(1, "a", 1)));
        stored(engine.put(entry(2, "a", 1)));
        assert_eq!(engine.remove(1, b"a").unwrap().namespace, 1);
        assert_eq!(engine.get(1, b"a").unwrap(), MemoryLookup::Miss);
        hit(engine.get(2, b"a"));
        assert_eq!(engine.clear(), 1);
        assert_eq!(engine.get(2, b"a").unwrap(), MemoryLookup::Miss);
        assert_eq!(engine.stats().entries, 0);
        assert_eq!(engine.stats().clears, 1);
        engine.assert_invariants();
    }

    #[test]
    fn dirty_victim_requires_successful_synchronous_demotion() {
        let item_bytes = MEMORY_ENTRY_OVERHEAD_BYTES + 1 + 8;
        let engine = MemoryEngine::new(item_bytes, 1).unwrap();
        let mut dirty = entry(0, "a", 8);
        dirty.disk_clean = false;
        stored(engine.put(dirty));

        let blocked = engine.put(entry(0, "b", 8));
        assert!(matches!(
            blocked,
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::DirtyVictimBlocked,
                ..
            }
        ));
        assert!(!hit(engine.get(0, b"a")).disk_clean);

        let failed = engine.put_with_demote(entry(0, "b", 8), |_| None);
        assert!(matches!(
            failed,
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::DemotionFailed,
                ..
            }
        ));
        assert!(!hit(engine.get(0, b"a")).disk_clean);

        let mut demoted = Vec::new();
        let (_, evicted, _) = stored(engine.put_with_demote(entry(0, "b", 8), |victim| {
            demoted.push(victim.key.clone());
            Some(8)
        }));
        assert_eq!(demoted, [b"a".to_vec()]);
        assert_eq!(evicted, 1);
        assert_eq!(engine.get(0, b"a").unwrap(), MemoryLookup::Miss);
        hit(engine.get(0, b"b"));
        engine.assert_invariants();
    }

    #[test]
    fn successful_victim_before_later_demotion_failure_keeps_exact_receipt() {
        let item_bytes = MEMORY_ENTRY_OVERHEAD_BYTES + 1 + 8;
        let engine = MemoryEngine::new(item_bytes * 2, 1).unwrap();
        let mut first = entry(0, "a", 8);
        first.disk_clean = false;
        let mut second = entry(0, "b", 8);
        second.disk_clean = false;
        stored(engine.put(first));
        stored(engine.put(second));

        let mut attempts = 0;
        let failed = engine.put_with_demote(entry(0, "c", item_bytes), |_| {
            attempts += 1;
            (attempts == 1).then_some(777)
        });
        assert!(matches!(
            failed,
            MemoryPutResult::NotStored {
                reason: MemoryRejectReason::DemotionFailed,
                ..
            }
        ));
        let first = engine.entry_usage(0, b"a").unwrap();
        let second = engine.entry_usage(0, b"b").unwrap();
        assert!(first.disk_clean);
        assert_eq!(first.pending_disk_bytes, 777);
        assert!(!second.disk_clean);
        engine.assert_invariants();
    }

    #[test]
    fn dirty_checkpoint_uses_bounded_parallelism_across_shards() {
        let item_bytes = MEMORY_ENTRY_OVERHEAD_BYTES + 16;
        let engine = MemoryEngine::new(item_bytes * 4, 4).unwrap();
        let mut keys = [None, None, None, None];
        for candidate in 0_u64.. {
            let key = format!("dirty-{candidate}").into_bytes();
            let shard = hash_key(0, &key) as usize & 3;
            if keys[shard].is_none() {
                keys[shard] = Some(key);
            }
            if keys.iter().all(Option::is_some) {
                break;
            }
        }
        for key in keys.into_iter().map(Option::unwrap) {
            stored(engine.put(MemoryEntry::new(0, key, Vec::new(), None, false)));
        }

        let rendezvous = Barrier::new(4);
        let (entries, _) = engine
            .persist_all_dirty(4, |_| {
                rendezvous.wait();
                Ok::<u64, ()>(0)
            })
            .unwrap();
        assert_eq!(entries, 4);
        assert_eq!(engine.stats().dirty_entries, 0);
    }

    #[test]
    fn churn_charge_is_bounded_and_clear_releases_retained_containers() {
        let capacity = 32 * (MEMORY_ENTRY_OVERHEAD_BYTES + 8);
        let engine = MemoryEngine::new(capacity, 1).unwrap();
        for round in 0..4 {
            for item in 0..64 {
                let key = format!("key-{round}-{item}");
                stored(engine.put(MemoryEntry::new(
                    0,
                    key.into_bytes(),
                    vec![item as u8; 8],
                    None,
                    true,
                )));
            }
        }

        {
            let shard = lock_unpoisoned(&engine.shards[0]);
            assert!(shard.retained_slots > shard.resident_entries());
            assert!(shard.retained_slots <= shard.max_retained_slots);
            assert!(shard.charged_bytes <= shard.capacity_bytes);
            assert!(shard.nodes.capacity() >= shard.retained_slots);
            assert!(shard.free.capacity() >= shard.retained_slots);
        }

        assert!(engine.clear() != 0);
        let shard = lock_unpoisoned(&engine.shards[0]);
        assert_eq!(shard.retained_slots, 0);
        assert_eq!(shard.charged_bytes, 0);
        assert_eq!(shard.buckets.capacity(), 0);
        assert_eq!(shard.nodes.capacity(), 0);
        assert_eq!(shard.free.capacity(), 0);
        drop(shard);
        engine.assert_invariants();
    }

    #[test]
    fn switching_between_small_and_large_objects_reclaims_old_slot_capacity() {
        let capacity = 8 * MEMORY_ENTRY_OVERHEAD_BYTES + 512;
        let engine = MemoryEngine::new(capacity, 1).unwrap();
        for key in b'a'..=b'h' {
            stored(engine.put(MemoryEntry::new(0, vec![key], Vec::new(), None, true)));
        }

        let large = MemoryEntry::new(0, b"large".to_vec(), vec![7; 2_000], None, true);
        let large_charge = large.charged_bytes().unwrap();
        let (_, evicted, _) = stored(engine.put(large));
        assert_eq!(evicted, 8);
        assert_eq!(hit(engine.get(0, b"large")).value.len(), 2_000);
        {
            let shard = lock_unpoisoned(&engine.shards[0]);
            assert_eq!(shard.retained_slots, 1);
            assert_eq!(shard.charged_bytes, large_charge);
        }

        stored(engine.put(MemoryEntry::new(0, b"x".to_vec(), Vec::new(), None, true)));
        let (_, evicted, evicted_bytes) =
            stored(engine.put(MemoryEntry::new(0, b"y".to_vec(), Vec::new(), None, true)));
        assert_eq!(evicted, 1);
        assert_eq!(evicted_bytes, large_charge);
        assert_eq!(engine.get(0, b"large").unwrap(), MemoryLookup::Miss);
        hit(engine.get(0, b"x"));
        hit(engine.get(0, b"y"));
        assert!(engine.stats().charged_bytes <= capacity);
        engine.assert_invariants();
    }

    #[test]
    fn concurrent_shard_churn_preserves_the_global_bound() {
        let capacity = 128 * (MEMORY_ENTRY_OVERHEAD_BYTES + 32);
        let engine = Arc::new(MemoryEngine::new(capacity, 8).unwrap());
        let mut workers = Vec::new();
        for worker in 0..8 {
            let engine = Arc::clone(&engine);
            workers.push(std::thread::spawn(move || {
                for item in 0..200 {
                    let key = format!("worker-{worker}-item-{item}");
                    stored(engine.put(MemoryEntry::new(
                        worker,
                        key.as_bytes().to_vec(),
                        vec![worker as u8; 32],
                        None,
                        true,
                    )));
                    // Another worker may legally evict this key after `put`
                    // releases the shard lock and before this lookup starts.
                    let _lookup = engine.get(worker, key.as_bytes()).unwrap();
                    if item % 3 == 0 {
                        engine.remove(worker, key.as_bytes());
                    }
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(engine.stats().charged_bytes <= capacity);
        engine.assert_invariants();
    }
}
