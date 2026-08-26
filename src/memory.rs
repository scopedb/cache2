//! Bounded shard-local RAM tier for the HybridCache data path.
//!
//! L1 is process-local and never participates in recovery. Entries are visible
//! immediately, may be discarded at any time, and use a small bounded eviction
//! policy.

use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::eviction::{
    AdmissionHint, EvictionPolicy, EvictionState, MAX_POLICY_SLOT_INDEX, PolicySlot,
    VictimSelection,
};
use crate::expiry::ExpiryClock;

/// Charged resident metadata: the entry slot, eviction-policy slot, and the
/// retained-value ownership. Container buckets and allocator metadata are
/// excluded from managed figures, like the other runtime collections.
pub(crate) const MEMORY_ENTRY_OVERHEAD_BYTES: usize = 64;
/// Full-key collision work must stay bounded even when callers supply many
/// distinct keys with the same 64-bit cache hash.
const MAX_SAME_HASH_ENTRIES: usize = 8;
/// Hash chains use compact slot indices, matching the intrusive compressed
/// links used by the rest of the cache rather than allocating a bucket vector.
const NO_SLOT_INDEX: u32 = u32::MAX;
/// A foreground admission may discard only a small, fixed number of clean
/// entries. Larger compaction work degrades to L1 bypass instead of scaling
/// with shard occupancy.
const MAX_EVICTIONS_PER_INSERT: usize = 16;
const MAX_BUDGET_CAS_ATTEMPTS: usize = 8;

pub(crate) struct MemoryBudget {
    pub(crate) capacity_bytes: usize,
    used_bytes: AtomicUsize,
}

impl MemoryBudget {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            used_bytes: AtomicUsize::new(0),
        }
    }

    fn try_charge(self: &Arc<Self>, bytes: usize) -> MemoryChargeAttempt {
        let mut used = self.used_bytes.load(Ordering::Acquire);
        for _ in 0..MAX_BUDGET_CAS_ATTEMPTS {
            let Some(next) = used.checked_add(bytes) else {
                return MemoryChargeAttempt::Full;
            };
            if next > self.capacity_bytes {
                return MemoryChargeAttempt::Full;
            }
            match self.used_bytes.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return MemoryChargeAttempt::Charged(MemoryCharge {
                        budget: Some(Arc::clone(self)),
                        bytes,
                    });
                }
                Err(observed) => used = observed,
            }
        }
        MemoryChargeAttempt::Contended
    }

    #[cfg(test)]
    fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Acquire)
    }
}

enum MemoryChargeAttempt {
    Charged(MemoryCharge),
    Full,
    Contended,
}

pub(crate) struct MemoryCharge {
    budget: Option<Arc<MemoryBudget>>,
    bytes: usize,
}

struct MemoryValueInner {
    bytes: Box<[u8]>,
    key_length: usize,
    _charge: MemoryCharge,
}

#[derive(Clone)]
pub(crate) struct MemoryValue(Arc<MemoryValueInner>);

impl MemoryValue {
    fn try_new(key: &[u8], value: &[u8], charge: MemoryCharge) -> Result<Self, MemoryCharge> {
        let Some(length) = key.len().checked_add(value.len()) else {
            return Err(charge);
        };
        let mut bytes = Vec::new();
        if bytes.try_reserve_exact(length).is_err() {
            return Err(charge);
        }
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        Ok(Self(Arc::new(MemoryValueInner {
            bytes: bytes.into_boxed_slice(),
            key_length: key.len(),
            _charge: charge,
        })))
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        MEMORY_ENTRY_OVERHEAD_BYTES + self.0.bytes.len()
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.0.bytes[..self.0.key_length]
    }

    fn value(&self) -> &[u8] {
        &self.0.bytes[self.0.key_length..]
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

impl Drop for MemoryCharge {
    fn drop(&mut self) {
        if let Some(budget) = &self.budget {
            let previous = budget.used_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(previous >= self.bytes);
        }
    }
}

struct MemoryEntry {
    value: MemoryValue,
    expires_at_unix_ms: u64,
    seqno: u64,
    namespace_id: u32,
    hash_next: u32,
}

struct MemoryShard {
    budget: Arc<MemoryBudget>,
    eviction: EvictionState,
    directory: HashMap<u64, u32>,
    slots: Vec<Option<MemoryEntry>>,
    policy_slots: Vec<PolicySlot>,
    free_slots: Vec<u32>,
}

impl MemoryShard {
    fn new(capacity_bytes: usize, policy: EvictionPolicy) -> io::Result<Self> {
        let maximum_entries = (capacity_bytes / MEMORY_ENTRY_OVERHEAD_BYTES)
            .min(MAX_POLICY_SLOT_INDEX.saturating_add(1));
        let budget = Arc::new(MemoryBudget::new(capacity_bytes));
        let directory = HashMap::new();
        Ok(Self {
            budget,
            eviction: EvictionState::new(policy, capacity_bytes, maximum_entries)?,
            directory,
            slots: Vec::new(),
            policy_slots: Vec::new(),
            free_slots: Vec::new(),
        })
    }

    fn directory_head(&self, hash: u64) -> Option<u32> {
        self.directory.get(&hash).copied()
    }

    fn directory_can_upsert(&mut self, hash: u64) -> bool {
        self.directory.contains_key(&hash) || self.directory.try_reserve(1).is_ok()
    }

    fn directory_upsert(&mut self, hash: u64, head: u32) -> Option<u32> {
        self.directory.insert(hash, head)
    }

    fn directory_remove(&mut self, hash: u64) -> Option<u32> {
        self.directory.remove(&hash)
    }

    fn find(&self, hash: u64, namespace_id: u32, key: &[u8]) -> Option<usize> {
        let mut cursor = self.directory_head(hash)?;
        for _ in 0..MAX_SAME_HASH_ENTRIES {
            let index = usize::try_from(cursor).ok()?;
            let entry = self.slots.get(index).and_then(Option::as_ref)?;
            if entry.namespace_id == namespace_id && entry.value.key() == key {
                return Some(index);
            }
            if entry.hash_next == NO_SLOT_INDEX {
                return None;
            }
            cursor = entry.hash_next;
        }
        None
    }

    fn hash_chain_is_full(&self, hash: u64) -> bool {
        let Some(mut cursor) = self.directory_head(hash) else {
            return false;
        };
        for depth in 0..MAX_SAME_HASH_ENTRIES {
            let Ok(index) = usize::try_from(cursor) else {
                return true;
            };
            let Some(entry) = self.slots.get(index).and_then(Option::as_ref) else {
                return true;
            };
            if entry.hash_next == NO_SLOT_INDEX {
                return depth + 1 == MAX_SAME_HASH_ENTRIES;
            }
            cursor = entry.hash_next;
        }
        true
    }

    fn remove_slot(&mut self, index: usize) {
        if self.slots.get(index).and_then(Option::as_ref).is_none() {
            return;
        }
        let Some(hash) = self.policy_slots.get(index).map(PolicySlot::hash) else {
            return;
        };
        let Some(mut cursor) = self.directory_head(hash) else {
            return;
        };
        let mut predecessor = None;
        let mut found = false;
        for _ in 0..MAX_SAME_HASH_ENTRIES {
            let Ok(candidate) = usize::try_from(cursor) else {
                break;
            };
            if candidate == index {
                found = true;
                break;
            }
            let Some(entry) = self.slots.get(candidate).and_then(Option::as_ref) else {
                break;
            };
            if entry.hash_next == NO_SLOT_INDEX {
                break;
            }
            predecessor = Some(candidate);
            cursor = entry.hash_next;
        }
        debug_assert!(found, "resident slot must remain in its hash chain");
        if !found {
            return;
        }

        let entry_next = self.slots[index]
            .as_ref()
            .expect("resident slot disappeared")
            .hash_next;
        if let Some(predecessor) = predecessor {
            self.slots[predecessor]
                .as_mut()
                .expect("hash-chain predecessor disappeared")
                .hash_next = entry_next;
        } else if entry_next == NO_SLOT_INDEX {
            let removed = self.directory_remove(hash);
            debug_assert_eq!(removed, u32::try_from(index).ok());
        } else {
            let replaced = self.directory_upsert(hash, entry_next);
            debug_assert!(replaced.is_some());
        }

        let weight = self.slots[index]
            .as_ref()
            .expect("resident slot disappeared")
            .value
            .charged_bytes();
        self.eviction.remove(&mut self.policy_slots, index, weight);
        let _entry = self.slots[index].take().expect("resident slot disappeared");
        self.free_slots
            .push(u32::try_from(index).expect("memory slot index exceeds u32"));
    }

    fn charge_with_eviction(&mut self, required: usize, hash: u64) -> ChargeResult {
        if required > self.budget.capacity_bytes {
            return ChargeResult::Rejected {
                evictions: 0,
                admission: false,
            };
        }
        match self.budget.try_charge(required) {
            MemoryChargeAttempt::Charged(charge) => {
                return ChargeResult::Charged {
                    charge,
                    evictions: 0,
                };
            }
            MemoryChargeAttempt::Contended => {
                return ChargeResult::Rejected {
                    evictions: 0,
                    admission: false,
                };
            }
            MemoryChargeAttempt::Full => {}
        }
        let mut evictions = 0_usize;
        let mut apply_admission = true;
        for _ in 0..MAX_EVICTIONS_PER_INSERT {
            let victim = {
                let slots = &self.slots;
                match self.eviction.select_victim(
                    &mut self.policy_slots,
                    hash,
                    apply_admission,
                    |index| {
                        slots[index]
                            .as_ref()
                            .expect("resident policy slot has no memory entry")
                            .value
                            .charged_bytes()
                    },
                ) {
                    VictimSelection::Victim(index) => index,
                    VictimSelection::Reject => {
                        return ChargeResult::Rejected {
                            evictions,
                            admission: true,
                        };
                    }
                    VictimSelection::None => break,
                }
            };
            self.remove_slot(victim);
            evictions += 1;
            apply_admission = false;
            match self.budget.try_charge(required) {
                MemoryChargeAttempt::Charged(charge) => {
                    return ChargeResult::Charged { charge, evictions };
                }
                MemoryChargeAttempt::Full => {}
                MemoryChargeAttempt::Contended => {
                    return ChargeResult::Rejected {
                        evictions,
                        admission: false,
                    };
                }
            }
        }
        ChargeResult::Rejected {
            evictions,
            admission: false,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn insert(
        &mut self,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at_unix_ms: u64,
        seqno: u64,
        hint: AdmissionHint,
    ) -> MemoryInsertResult {
        if self.hash_chain_is_full(hash) || !self.directory_can_upsert(hash) {
            return MemoryInsertResult::bypassed();
        }
        let Some(charged_bytes) = MEMORY_ENTRY_OVERHEAD_BYTES
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.len()))
        else {
            return MemoryInsertResult::bypassed();
        };
        let (charge, evictions) = match self.charge_with_eviction(charged_bytes, hash) {
            ChargeResult::Charged { charge, evictions } => (charge, evictions),
            ChargeResult::Rejected {
                evictions,
                admission,
            } => return MemoryInsertResult::rejected(evictions, admission),
        };

        let needs_slot = self.free_slots.is_empty();
        if needs_slot
            && (self.slots.try_reserve(1).is_err()
                || self.policy_slots.try_reserve(1).is_err()
                || self.free_slots.try_reserve(1).is_err())
        {
            return MemoryInsertResult::rejected(evictions, false);
        }
        let packed_index = match self.free_slots.last().copied() {
            Some(index) => index,
            None => {
                if self.slots.len() > MAX_POLICY_SLOT_INDEX {
                    return MemoryInsertResult::rejected(evictions, false);
                }
                let Some(index) = u32::try_from(self.slots.len())
                    .ok()
                    .filter(|index| *index != NO_SLOT_INDEX)
                else {
                    return MemoryInsertResult::rejected(evictions, false);
                };
                index
            }
        };
        let Ok(index) = usize::try_from(packed_index) else {
            return MemoryInsertResult::rejected(evictions, false);
        };
        let Ok(memory_value) = MemoryValue::try_new(key, value, charge) else {
            return MemoryInsertResult::rejected(evictions, false);
        };
        let previous_head = self.directory_head(hash);
        let entry = MemoryEntry {
            value: memory_value,
            expires_at_unix_ms,
            seqno,
            namespace_id,
            hash_next: previous_head.unwrap_or(NO_SLOT_INDEX),
        };
        match self.free_slots.pop() {
            Some(free_index) => {
                debug_assert_eq!(free_index, packed_index);
                self.slots[index] = Some(entry);
            }
            None => {
                debug_assert_eq!(index, self.slots.len());
                self.slots.push(Some(entry));
                self.policy_slots.push(PolicySlot::default());
            }
        }
        self.eviction
            .insert(&mut self.policy_slots, index, hash, charged_bytes, hint);
        let previous = self.directory_upsert(hash, packed_index);
        debug_assert_eq!(previous, previous_head);
        MemoryInsertResult {
            inserted: true,
            evictions,
            admission_rejected: false,
        }
    }
}

enum ChargeResult {
    Charged {
        charge: MemoryCharge,
        evictions: usize,
    },
    Rejected {
        evictions: usize,
        admission: bool,
    },
}

#[derive(Clone, Copy, Debug, Default)]
struct MemoryInsertResult {
    inserted: bool,
    evictions: usize,
    admission_rejected: bool,
}

impl MemoryInsertResult {
    const fn bypassed() -> Self {
        Self {
            inserted: false,
            evictions: 0,
            admission_rejected: false,
        }
    }

    const fn rejected(evictions: usize, admission_rejected: bool) -> Self {
        Self {
            inserted: false,
            evictions,
            admission_rejected,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryReadToken {
    shard_id: usize,
    admission: AdmissionHint,
}

pub(crate) enum MemoryLookup {
    Hit(MemoryValue),
    Miss(MemoryReadToken),
}

pub(crate) struct MemoryStore {
    shards: Box<[MemoryShardLock]>,
    metrics: MemoryMetrics,
}

#[repr(align(64))]
struct MemoryShardLock(Mutex<MemoryShard>);

impl Deref for MemoryShardLock {
    type Target = Mutex<MemoryShard>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct MemoryMetrics {
    enabled: bool,
    evictions: AtomicU64,
    bypasses: AtomicU64,
    admission_rejections: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MemoryMetricsSnapshot {
    pub(crate) evictions: u64,
    pub(crate) bypasses: u64,
    pub(crate) admission_rejections: u64,
}

impl MemoryStore {
    pub(crate) fn new(
        capacity_bytes: usize,
        shard_count: usize,
        policy: EvictionPolicy,
        statistics_enabled: bool,
    ) -> io::Result<Self> {
        if shard_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory tier requires at least one shard",
            ));
        }
        let base = capacity_bytes / shard_count;
        let remainder = capacity_bytes % shard_count;
        let mut shards = Vec::new();
        shards.try_reserve_exact(shard_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate memory-tier shards",
            )
        })?;
        for shard_id in 0..shard_count {
            shards.push(MemoryShardLock(Mutex::new(MemoryShard::new(
                base + usize::from(shard_id < remainder),
                policy,
            )?)));
        }
        Ok(Self {
            shards: shards.into_boxed_slice(),
            metrics: MemoryMetrics {
                enabled: statistics_enabled,
                evictions: AtomicU64::new(0),
                bypasses: AtomicU64::new(0),
                admission_rejections: AtomicU64::new(0),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish(
        &self,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at_unix_ms: u64,
        seqno: u64,
    ) -> bool {
        let shard_id = self.route(hash);
        let Ok(Some(mut shard)) = self.try_lock_shard(shard_id) else {
            self.record_insert(MemoryInsertResult::bypassed());
            return false;
        };
        let existing = shard.find(hash, namespace_id, key);
        if existing.is_some_and(|index| {
            shard.slots[index]
                .as_ref()
                .is_some_and(|entry| entry.seqno >= seqno)
        }) {
            return true;
        }
        let admission = shard.eviction.prepare_insert(hash);
        if let Some(index) = existing {
            shard.remove_slot(index);
        }
        let result = shard.insert(
            hash,
            namespace_id,
            key,
            value,
            expires_at_unix_ms,
            seqno,
            admission,
        );
        drop(shard);
        self.record_insert(result);
        result.inserted
    }

    pub(crate) fn lookup(
        &self,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        clock: ExpiryClock,
    ) -> MemoryLookup {
        let shard_id = self.route(hash);
        let Ok(Some(mut shard)) = self.try_lock_shard(shard_id) else {
            return MemoryLookup::Miss(MemoryReadToken {
                shard_id,
                admission: AdmissionHint::default(),
            });
        };
        let Some(index) = shard.find(hash, namespace_id, key) else {
            let admission = shard.eviction.record_miss(hash);
            return MemoryLookup::Miss(MemoryReadToken {
                shard_id,
                admission,
            });
        };
        let expired = shard.slots[index]
            .as_ref()
            .is_some_and(|entry| clock.is_expired(entry.expires_at_unix_ms));
        if expired {
            shard.remove_slot(index);
            let admission = shard.eviction.record_miss(hash);
            return MemoryLookup::Miss(MemoryReadToken {
                shard_id,
                admission,
            });
        }
        let shard = &mut *shard;
        shard.eviction.record_hit(&mut shard.policy_slots, index);
        let entry = shard.slots[index]
            .as_ref()
            .expect("memory directory points to a live slot");
        MemoryLookup::Hit(entry.value.clone())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn promote(
        &self,
        token: MemoryReadToken,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at_unix_ms: u64,
        seqno: u64,
    ) -> Option<MemoryValue> {
        if token.shard_id != self.route(hash) {
            return None;
        }
        let Ok(Some(mut shard)) = self.try_lock_shard(token.shard_id) else {
            return None;
        };
        if shard.find(hash, namespace_id, key).is_some() {
            return None;
        }
        let result = shard.insert(
            hash,
            namespace_id,
            key,
            value,
            expires_at_unix_ms,
            seqno,
            token.admission,
        );
        let promoted = result.inserted.then(|| {
            let index = shard
                .find(hash, namespace_id, key)
                .expect("a successful promotion installs the exact key");
            shard.slots[index]
                .as_ref()
                .expect("memory directory points to the promoted entry")
                .value
                .clone()
        });
        drop(shard);
        self.record_insert(result);
        promoted
    }

    pub(crate) fn metrics_snapshot(&self) -> MemoryMetricsSnapshot {
        MemoryMetricsSnapshot {
            evictions: self.metrics.evictions.load(Ordering::Relaxed),
            bypasses: self.metrics.bypasses.load(Ordering::Relaxed),
            admission_rejections: self.metrics.admission_rejections.load(Ordering::Relaxed),
        }
    }

    fn record_insert(&self, result: MemoryInsertResult) {
        if !self.metrics.enabled {
            return;
        }
        self.metrics.evictions.fetch_add(
            u64::try_from(result.evictions).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if !result.inserted {
            self.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
        }
        if result.admission_rejected {
            self.metrics
                .admission_rejections
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn route(&self, hash: u64) -> usize {
        (hash % self.shards.len() as u64) as usize
    }

    fn try_lock_shard(&self, shard_id: usize) -> io::Result<Option<MutexGuard<'_, MemoryShard>>> {
        match self.shards[shard_id].try_lock() {
            Ok(shard) => Ok(Some(shard)),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Poisoned(_)) => {
                Err(io::Error::other("memory-tier shard is poisoned"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(capacity_bytes: usize, shard_count: usize) -> MemoryStore {
        store_with_policy(capacity_bytes, shard_count, EvictionPolicy::Clock)
    }

    fn store_with_policy(
        capacity_bytes: usize,
        shard_count: usize,
        policy: EvictionPolicy,
    ) -> MemoryStore {
        MemoryStore::new(capacity_bytes, shard_count, policy, true).unwrap()
    }

    fn publish_clean(store: &MemoryStore, hash: u64, key: &[u8], seqno: u64) {
        assert!(store.publish(hash, 0, key, &[hash as u8], 0, seqno));
    }

    fn assert_hit(store: &MemoryStore, hash: u64, key: &[u8]) {
        assert!(matches!(
            store.lookup(hash, 0, key, ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(_)
        ));
    }

    fn assert_miss(store: &MemoryStore, hash: u64, key: &[u8]) {
        assert!(matches!(
            store.lookup(hash, 0, key, ExpiryClock::Fixed(1)),
            MemoryLookup::Miss(_)
        ));
    }

    #[test]
    fn published_value_is_visible_and_immediately_evictable() {
        let store = store(512, 1);
        assert!(store.publish(7, 0, b"a", b"value-a", 0, 1));
        assert!(matches!(
            store.lookup(7, 0, b"a", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"value-a"
        ));

        assert!(store.publish(8, 0, b"b", &[2; 300], 0, 2));
        assert!(matches!(
            store.lookup(8, 0, b"b", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.len() == 300
        ));
    }

    #[test]
    fn l1_contention_bypasses_and_allows_l2_fallback() {
        let store = store(1024, 1);
        assert!(store.publish(7, 0, b"key", b"old", 0, 1));

        let shard = store.shards[0].lock().unwrap();
        assert!(matches!(
            store.lookup(7, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Miss(_)
        ));
        assert!(!store.publish(7, 0, b"key", b"new", 0, 2));
        drop(shard);

        assert!(matches!(
            store.lookup(7, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"old"
        ));
    }

    #[test]
    fn same_hash_chain_disambiguates_namespace_and_full_key() {
        for policy in EvictionPolicy::ALL {
            let store = store_with_policy(2048, 1, policy);
            let collision_hash = 42;
            assert!(store.publish(collision_hash, 7, b"alpha", b"value-alpha-ns7", 0, 1,));
            assert!(store.publish(collision_hash, 7, b"beta", b"value-beta-ns7", 0, 2,));
            assert!(store.publish(collision_hash, 8, b"alpha", b"value-alpha-ns8", 0, 3,));
            assert!(store.publish(collision_hash, 7, b"beta", b"replacement-beta-ns7", 0, 4,));

            for (namespace, key, expected) in [
                (7, b"alpha".as_slice(), b"value-alpha-ns7".as_slice()),
                (7, b"beta".as_slice(), b"replacement-beta-ns7".as_slice()),
                (8, b"alpha".as_slice(), b"value-alpha-ns8".as_slice()),
            ] {
                assert!(matches!(
                    store.lookup(collision_hash, namespace, key, ExpiryClock::Fixed(1)),
                    MemoryLookup::Hit(value) if value.as_ref() == expected
                ));
            }
            assert!(matches!(
                store.lookup(collision_hash, 7, b"foreign", ExpiryClock::Fixed(1)),
                MemoryLookup::Miss(_)
            ));
        }
    }

    #[test]
    fn same_hash_chain_has_a_fixed_admission_limit() {
        let store = store(16 * 1024, 1);
        let collision_hash = 42;
        let keys = (0..=MAX_SAME_HASH_ENTRIES)
            .map(|ordinal| format!("collision-{ordinal}"))
            .collect::<Vec<_>>();

        for (ordinal, key) in keys.iter().take(MAX_SAME_HASH_ENTRIES).enumerate() {
            assert!(store.publish(
                collision_hash,
                0,
                key.as_bytes(),
                &[ordinal as u8],
                0,
                ordinal as u64 + 1,
            ));
        }
        assert!(!store.publish(
            collision_hash,
            0,
            keys[MAX_SAME_HASH_ENTRIES].as_bytes(),
            b"overflow",
            0,
            MAX_SAME_HASH_ENTRIES as u64 + 1,
        ));

        for (ordinal, key) in keys.iter().take(MAX_SAME_HASH_ENTRIES).enumerate() {
            assert!(matches!(
                store.lookup(collision_hash, 0, key.as_bytes(), ExpiryClock::Fixed(1)),
                MemoryLookup::Hit(value) if value.as_ref() == [ordinal as u8]
            ));
        }
        assert_eq!(store.metrics_snapshot().bypasses, 1);
    }

    #[test]
    fn existing_exact_key_blocks_disk_promotion() {
        let store = store(1024, 1);
        let MemoryLookup::Miss(token) = store.lookup(9, 0, b"key", ExpiryClock::Fixed(1)) else {
            panic!("empty memory tier must miss");
        };
        assert!(store.publish(9, 0, b"key", b"new", 0, 2));
        assert!(store.promote(token, 9, 0, b"key", b"old", 0, 1).is_none());
        assert!(matches!(
            store.lookup(9, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_exact_key_publication_suppresses_a_delayed_older_put() {
        let store = store(1024, 1);
        assert!(store.publish(11, 0, b"key", b"new", 0, 2));
        assert!(store.publish(11, 0, b"key", b"old", 0, 1));
        assert!(matches!(
            store.lookup(11, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_unrelated_publication_does_not_suppress_an_older_key() {
        let store = store(1024, 1);
        assert!(store.publish(12, 0, b"newer", b"newer", 0, 2));
        assert!(store.publish(11, 0, b"older", b"older", 0, 1));
        assert_hit(&store, 12, b"newer");
        assert_hit(&store, 11, b"older");
    }

    #[test]
    fn retained_value_keeps_its_capacity_charge_after_eviction() {
        let store = store(512, 1);
        assert!(store.publish(21, 0, b"a", &[1; 300], 0, 1));
        let MemoryLookup::Hit(retained) = store.lookup(21, 0, b"a", ExpiryClock::Fixed(1)) else {
            panic!("published value must be visible");
        };

        assert!(!store.publish(22, 0, b"b", &[2; 300], 0, 2));
        assert_eq!(
            store.shards[0].lock().unwrap().budget.used_bytes(),
            MEMORY_ENTRY_OVERHEAD_BYTES + 301
        );

        drop(retained);
        assert_eq!(store.shards[0].lock().unwrap().budget.used_bytes(), 0);
        assert!(store.publish(22, 0, b"b", &[2; 300], 0, 3));
    }

    #[test]
    fn concurrent_retained_value_drops_release_the_charge_once() {
        let store = store(512, 1);
        assert!(store.publish(21, 0, b"a", &[1; 300], 0, 1));
        let MemoryLookup::Hit(retained) = store.lookup(21, 0, b"a", ExpiryClock::Fixed(1)) else {
            panic!("published value must be visible");
        };
        let clones = (0..8).map(|_| retained.clone()).collect::<Vec<_>>();

        assert!(!store.publish(22, 0, b"b", &[2; 300], 0, 2));
        let barrier = Arc::new(std::sync::Barrier::new(clones.len() + 1));
        std::thread::scope(|scope| {
            for value in clones {
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    drop(value);
                });
            }
            drop(retained);
            barrier.wait();
        });

        assert_eq!(store.shards[0].lock().unwrap().budget.used_bytes(), 0);
        assert!(store.publish(22, 0, b"b", &[2; 300], 0, 3));
    }

    #[test]
    fn skewed_shard_admission_cannot_consume_another_shards_budget() {
        let store = store(1024, 2);
        assert!(store.publish(2, 0, b"even-a", &[1; 300], 0, 1));
        assert!(store.publish(4, 0, b"even-b", &[2; 300], 0, 2));
        assert!(store.publish(3, 0, b"odd", &[3; 300], 0, 3));
        assert_miss(&store, 2, b"even-a");
        assert_hit(&store, 4, b"even-b");

        assert!(
            store
                .shards
                .iter()
                .all(|shard| { shard.lock().unwrap().budget.used_bytes() <= 512 })
        );
    }

    #[test]
    fn lru_and_fifo_have_distinct_hit_ordering() {
        let lru = store_with_policy(2 * MEMORY_ENTRY_OVERHEAD_BYTES + 10, 1, EvictionPolicy::Lru);
        publish_clean(&lru, 1, b"a", 1);
        publish_clean(&lru, 2, b"b", 2);
        assert_hit(&lru, 1, b"a");
        publish_clean(&lru, 3, b"c", 3);
        assert_hit(&lru, 1, b"a");
        assert_miss(&lru, 2, b"b");

        let fifo = store_with_policy(
            2 * MEMORY_ENTRY_OVERHEAD_BYTES + 10,
            1,
            EvictionPolicy::Fifo,
        );
        publish_clean(&fifo, 1, b"a", 1);
        publish_clean(&fifo, 2, b"b", 2);
        assert_hit(&fifo, 1, b"a");
        publish_clean(&fifo, 3, b"c", 3);
        assert_miss(&fifo, 1, b"a");
        assert_hit(&fifo, 2, b"b");
    }

    #[test]
    fn tinylfu_requires_a_candidate_to_outscore_the_lru_victim() {
        let store = store_with_policy(
            2 * MEMORY_ENTRY_OVERHEAD_BYTES + 10,
            1,
            EvictionPolicy::TinyLfu,
        );
        publish_clean(&store, 1, b"hot", 1);
        publish_clean(&store, 2, b"cold", 2);
        assert_hit(&store, 1, b"hot");

        assert!(!store.publish(3, 0, b"cand", b"c", 0, 3));
        assert!(store.publish(3, 0, b"cand", b"c", 0, 4));
        assert_miss(&store, 2, b"cold");
        assert_hit(&store, 3, b"cand");

        let metrics = store.metrics_snapshot();
        assert_eq!(metrics.admission_rejections, 1);
        assert_eq!(metrics.bypasses, 1);
        assert_eq!(metrics.evictions, 1);
    }

    #[test]
    fn sieve_keeps_a_visited_old_entry_and_demotes_an_unvisited_newer_one() {
        let store = store_with_policy(
            2 * MEMORY_ENTRY_OVERHEAD_BYTES + 10,
            1,
            EvictionPolicy::Sieve,
        );
        publish_clean(&store, 1, b"a", 1);
        publish_clean(&store, 2, b"b", 2);
        assert_hit(&store, 1, b"a");
        publish_clean(&store, 3, b"c", 3);

        assert_hit(&store, 1, b"a");
        assert_miss(&store, 2, b"b");
        assert_hit(&store, 3, b"c");
    }
}
