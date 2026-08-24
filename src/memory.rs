//! Bounded shard-local RAM tier for the HybridCache data path.
//!
//! L1 is process-local and never participates in recovery. Pending entries are
//! visible immediately but cannot be evicted until their matching Region write
//! completes; clean entries use a small CLOCK policy and may be discarded at
//! any time.

use std::collections::HashMap;
use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::eviction::{AdmissionHint, EvictionPolicy, EvictionState, PolicySlot, VictimSelection};
use crate::expiry::ExpiryClock;

const MEMORY_ENTRY_OVERHEAD_BYTES: usize = 160;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryState {
    Pending,
    Clean,
}

struct MemoryBudget {
    capacity_bytes: usize,
    used_bytes: AtomicUsize,
}

impl MemoryBudget {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            capacity_bytes,
            used_bytes: AtomicUsize::new(0),
        }
    }

    fn try_charge(self: &Arc<Self>, bytes: usize) -> Option<MemoryCharge> {
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes)
                    .filter(|next| *next <= self.capacity_bytes)
            })
            .ok()?;
        Some(MemoryCharge {
            budget: Arc::clone(self),
            bytes,
        })
    }

    #[cfg(test)]
    fn used_bytes(&self) -> usize {
        self.used_bytes.load(Ordering::Acquire)
    }
}

struct MemoryCharge {
    budget: Arc<MemoryBudget>,
    bytes: usize,
}

impl Drop for MemoryCharge {
    fn drop(&mut self) {
        let previous = self
            .budget
            .used_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        debug_assert!(previous >= self.bytes);
    }
}

struct MemoryValueInner {
    bytes: Box<[u8]>,
    _charge: MemoryCharge,
}

#[derive(Clone)]
pub(crate) struct MemoryValue(Arc<MemoryValueInner>);

impl MemoryValue {
    fn new(bytes: &[u8], charge: MemoryCharge) -> Self {
        Self(Arc::new(MemoryValueInner {
            bytes: Box::from(bytes),
            _charge: charge,
        }))
    }
}

impl Deref for MemoryValue {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0.bytes
    }
}

impl AsRef<[u8]> for MemoryValue {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

struct MemoryEntry {
    hash: u64,
    namespace_id: u32,
    key: Box<[u8]>,
    value: MemoryValue,
    expires_at_unix_ms: u64,
    seqno: u64,
    state: EntryState,
}

struct MemoryShard {
    budget: Arc<MemoryBudget>,
    eviction: EvictionState,
    revision: u64,
    publication_floor_seqno: u64,
    completed_seqno: u64,
    directory: HashMap<u64, Vec<usize>>,
    slots: Vec<Option<MemoryEntry>>,
    policy_slots: Vec<PolicySlot>,
    free_slots: Vec<usize>,
}

impl MemoryShard {
    fn new(capacity_bytes: usize, policy: EvictionPolicy) -> io::Result<Self> {
        let maximum_entries = capacity_bytes / MEMORY_ENTRY_OVERHEAD_BYTES;
        Ok(Self {
            budget: Arc::new(MemoryBudget::new(capacity_bytes)),
            eviction: EvictionState::new(policy, capacity_bytes, maximum_entries)?,
            revision: 0,
            publication_floor_seqno: 0,
            completed_seqno: 0,
            directory: HashMap::new(),
            slots: Vec::new(),
            policy_slots: Vec::new(),
            free_slots: Vec::new(),
        })
    }

    fn next_revision(&mut self) -> Option<u64> {
        self.revision = self.revision.checked_add(1)?;
        Some(self.revision)
    }

    fn find(&self, hash: u64, namespace_id: u32, key: &[u8]) -> Option<usize> {
        self.directory.get(&hash)?.iter().copied().find(|index| {
            self.slots
                .get(*index)
                .and_then(Option::as_ref)
                .is_some_and(|entry| {
                    entry.namespace_id == namespace_id && entry.key.as_ref() == key
                })
        })
    }

    fn remove_key(&mut self, hash: u64, namespace_id: u32, key: &[u8]) {
        if let Some(index) = self.find(hash, namespace_id, key) {
            self.remove_slot(index);
        }
    }

    fn remove_slot(&mut self, index: usize) {
        self.eviction.remove(&mut self.policy_slots, index);
        let Some(entry) = self.slots.get_mut(index).and_then(Option::take) else {
            return;
        };
        let remove_bucket = if let Some(bucket) = self.directory.get_mut(&entry.hash) {
            bucket.retain(|candidate| *candidate != index);
            bucket.is_empty()
        } else {
            false
        };
        if remove_bucket {
            self.directory.remove(&entry.hash);
        }
        self.free_slots.push(index);
    }

    fn charge_with_eviction(&mut self, required: usize, hash: u64) -> ChargeResult {
        if required > self.budget.capacity_bytes {
            return ChargeResult::Rejected {
                evictions: 0,
                admission: false,
            };
        }
        if let Some(charge) = self.budget.try_charge(required) {
            return ChargeResult::Charged {
                charge,
                evictions: 0,
            };
        }
        let maximum_steps = self.slots.len().saturating_add(1);
        let mut evictions = 0_usize;
        let mut apply_admission = true;
        for _ in 0..maximum_steps {
            let victim =
                match self
                    .eviction
                    .select_victim(&mut self.policy_slots, hash, apply_admission)
                {
                    VictimSelection::Victim(index) => index,
                    VictimSelection::Reject => {
                        return ChargeResult::Rejected {
                            evictions,
                            admission: true,
                        };
                    }
                    VictimSelection::None => break,
                };
            self.remove_slot(victim);
            evictions += 1;
            apply_admission = false;
            if let Some(charge) = self.budget.try_charge(required) {
                return ChargeResult::Charged { charge, evictions };
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
        state: EntryState,
        hint: AdmissionHint,
    ) -> MemoryInsertResult {
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
        if !self.directory.contains_key(&hash) && self.directory.try_reserve(1).is_err() {
            return MemoryInsertResult::rejected(evictions, false);
        }
        let bucket = self.directory.entry(hash).or_default();
        if bucket.try_reserve(1).is_err() {
            return MemoryInsertResult::rejected(evictions, false);
        }

        let entry = MemoryEntry {
            hash,
            namespace_id,
            key: Box::from(key),
            value: MemoryValue::new(value, charge),
            expires_at_unix_ms,
            seqno,
            state,
        };
        let index = match self.free_slots.pop() {
            Some(index) => {
                self.slots[index] = Some(entry);
                index
            }
            None => {
                let index = self.slots.len();
                self.slots.push(Some(entry));
                self.policy_slots.push(PolicySlot::default());
                index
            }
        };
        self.eviction.insert(
            &mut self.policy_slots,
            index,
            hash,
            charged_bytes,
            state == EntryState::Clean,
            hint,
        );
        bucket.push(index);
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
    revision: u64,
    admission: AdmissionHint,
}

pub(crate) enum MemoryLookup {
    Hit(MemoryValue),
    Hidden,
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
        stats_enabled: bool,
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
                enabled: stats_enabled,
                evictions: AtomicU64::new(0),
                bypasses: AtomicU64::new(0),
                admission_rejections: AtomicU64::new(0),
            },
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn publish_pending(
        &self,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at_unix_ms: u64,
        seqno: u64,
    ) -> bool {
        let Ok(mut shard) = self.lock_hash(hash) else {
            return false;
        };
        if seqno < shard.publication_floor_seqno {
            return false;
        }
        shard.publication_floor_seqno = seqno;
        if shard.next_revision().is_none() {
            return false;
        }
        let admission = shard.eviction.prepare_insert(hash);
        shard.remove_key(hash, namespace_id, key);
        let state = if seqno <= shard.completed_seqno {
            EntryState::Clean
        } else {
            EntryState::Pending
        };
        let result = shard.insert(
            hash,
            namespace_id,
            key,
            value,
            expires_at_unix_ms,
            seqno,
            state,
            admission,
        );
        drop(shard);
        self.record_insert(result);
        result.inserted
    }

    pub(crate) fn complete(&self, hash: u64, seqno: u64) {
        let Ok(mut shard) = self.lock_hash(hash) else {
            return;
        };
        shard.completed_seqno = shard.completed_seqno.max(seqno);
        let Some(indices) = shard.directory.get(&hash) else {
            return;
        };
        let index = indices.iter().copied().find(|index| {
            shard
                .slots
                .get(*index)
                .and_then(Option::as_ref)
                .is_some_and(|entry| entry.seqno == seqno)
        });
        if let Some(index) = index {
            if let Some(entry) = shard.slots[index].as_mut() {
                entry.state = EntryState::Clean;
                let shard = &mut *shard;
                shard.eviction.set_evictable(&mut shard.policy_slots, index);
            }
        }
    }

    pub(crate) fn lookup(
        &self,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        clock: ExpiryClock,
    ) -> MemoryLookup {
        let Ok(mut shard) = self.lock_hash(hash) else {
            return MemoryLookup::Hidden;
        };
        let Some(index) = shard.find(hash, namespace_id, key) else {
            let admission = shard.eviction.record_miss(hash);
            return MemoryLookup::Miss(MemoryReadToken {
                shard_id: self.route(hash),
                revision: shard.revision,
                admission,
            });
        };
        let expired = shard.slots[index]
            .as_ref()
            .is_some_and(|entry| clock.is_expired(entry.expires_at_unix_ms));
        if expired {
            if shard.slots[index]
                .as_ref()
                .is_some_and(|entry| entry.state == EntryState::Pending)
            {
                let shard = &mut *shard;
                shard.eviction.record_hit(&mut shard.policy_slots, index);
                return MemoryLookup::Hidden;
            }
            shard.remove_slot(index);
            let admission = shard.eviction.record_miss(hash);
            return MemoryLookup::Miss(MemoryReadToken {
                shard_id: self.route(hash),
                revision: shard.revision,
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
    pub(crate) fn promote_clean(
        &self,
        token: MemoryReadToken,
        hash: u64,
        namespace_id: u32,
        key: &[u8],
        value: &[u8],
        expires_at_unix_ms: u64,
        seqno: u64,
    ) -> bool {
        if token.shard_id != self.route(hash) {
            return false;
        }
        let Ok(mut shard) = self.lock_shard(token.shard_id) else {
            return false;
        };
        if shard.revision != token.revision || shard.find(hash, namespace_id, key).is_some() {
            return false;
        }
        let result = shard.insert(
            hash,
            namespace_id,
            key,
            value,
            expires_at_unix_ms,
            seqno,
            EntryState::Clean,
            token.admission,
        );
        drop(shard);
        self.record_insert(result);
        result.inserted
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

    fn lock_hash(&self, hash: u64) -> io::Result<MutexGuard<'_, MemoryShard>> {
        self.lock_shard(self.route(hash))
    }

    fn lock_shard(&self, shard_id: usize) -> io::Result<MutexGuard<'_, MemoryShard>> {
        self.shards[shard_id]
            .lock()
            .map_err(|_| io::Error::other("memory-tier shard is poisoned"))
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
        assert!(store.publish_pending(hash, 0, key, &[hash as u8], 0, seqno));
        store.complete(hash, seqno);
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
    fn pending_value_is_visible_and_matching_completion_makes_it_evictable() {
        let store = store(512, 1);
        assert!(store.publish_pending(7, 0, b"a", b"value-a", 0, 1));
        assert!(matches!(
            store.lookup(7, 0, b"a", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"value-a"
        ));

        store.complete(7, 1);
        assert!(store.publish_pending(8, 0, b"b", &[2; 300], 0, 2));
        assert!(matches!(
            store.lookup(8, 0, b"b", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.len() == 300
        ));
    }

    #[test]
    fn every_policy_pins_pending_entries_until_completion() {
        for policy in EvictionPolicy::ALL {
            let store = store_with_policy(330, 1, policy);
            assert!(store.publish_pending(1, 0, b"a", b"a", 0, 1));
            assert!(store.publish_pending(2, 0, b"b", b"b", 0, 2));
            assert!(!store.publish_pending(3, 0, b"c", b"c", 0, 3));
            assert_hit(&store, 1, b"a");
            assert_hit(&store, 2, b"b");

            store.complete(1, 1);
            let inserted = store.publish_pending(3, 0, b"c", b"c", 0, 4)
                || store.publish_pending(3, 0, b"c", b"c", 0, 5);
            assert!(inserted);
            assert_hit(&store, 3, b"c");
        }
    }

    #[test]
    fn same_hash_bucket_disambiguates_namespace_and_full_key() {
        for policy in EvictionPolicy::ALL {
            let store = store_with_policy(2048, 1, policy);
            let collision_hash = 42;
            assert!(store.publish_pending(collision_hash, 7, b"alpha", b"value-alpha-ns7", 0, 1,));
            assert!(store.publish_pending(collision_hash, 7, b"beta", b"value-beta-ns7", 0, 2,));
            assert!(store.publish_pending(collision_hash, 8, b"alpha", b"value-alpha-ns8", 0, 3,));

            for (namespace, key, expected) in [
                (7, b"alpha".as_slice(), b"value-alpha-ns7".as_slice()),
                (7, b"beta".as_slice(), b"value-beta-ns7".as_slice()),
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
    fn concurrent_put_revision_blocks_stale_disk_promotion() {
        let store = store(1024, 1);
        let MemoryLookup::Miss(token) = store.lookup(9, 0, b"key", ExpiryClock::Fixed(1)) else {
            panic!("empty memory tier must miss");
        };
        assert!(store.publish_pending(9, 0, b"key", b"new", 0, 2));
        store.promote_clean(token, 9, 0, b"key", b"old", 0, 1);
        assert!(matches!(
            store.lookup(9, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_shard_publication_suppresses_a_delayed_older_put() {
        let store = store(1024, 1);
        assert!(store.publish_pending(11, 0, b"key", b"new", 0, 2));
        assert!(!store.publish_pending(11, 0, b"key", b"old", 0, 1));
        assert!(matches!(
            store.lookup(11, 0, b"key", ExpiryClock::Fixed(1)),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn completion_before_publication_installs_a_clean_entry() {
        let store = store(512, 1);
        store.complete(12, 3);
        assert!(store.publish_pending(12, 0, b"a", b"value-a", 0, 3));
        assert!(store.publish_pending(13, 0, b"b", &[4; 300], 0, 4));
    }

    #[test]
    fn retained_value_keeps_its_capacity_charge_after_eviction() {
        let store = store(512, 1);
        assert!(store.publish_pending(21, 0, b"a", &[1; 300], 0, 1));
        store.complete(21, 1);
        let MemoryLookup::Hit(retained) = store.lookup(21, 0, b"a", ExpiryClock::Fixed(1)) else {
            panic!("published value must be visible");
        };

        assert!(!store.publish_pending(22, 0, b"b", &[2; 300], 0, 2));
        assert_eq!(store.shards[0].lock().unwrap().budget.used_bytes(), 461);

        drop(retained);
        assert_eq!(store.shards[0].lock().unwrap().budget.used_bytes(), 0);
        assert!(store.publish_pending(22, 0, b"b", &[2; 300], 0, 3));
    }

    #[test]
    fn skewed_shard_admission_cannot_consume_another_shards_budget() {
        let store = store(1024, 2);
        assert!(store.publish_pending(2, 0, b"even-a", &[1; 300], 0, 1));
        assert!(!store.publish_pending(4, 0, b"even-b", &[2; 300], 0, 2));
        assert!(store.publish_pending(3, 0, b"odd", &[3; 300], 0, 3));

        assert!(
            store
                .shards
                .iter()
                .all(|shard| { shard.lock().unwrap().budget.used_bytes() <= 512 })
        );
    }

    #[test]
    fn lru_and_fifo_have_distinct_hit_ordering() {
        let lru = store_with_policy(330, 1, EvictionPolicy::Lru);
        publish_clean(&lru, 1, b"a", 1);
        publish_clean(&lru, 2, b"b", 2);
        assert_hit(&lru, 1, b"a");
        publish_clean(&lru, 3, b"c", 3);
        assert_hit(&lru, 1, b"a");
        assert_miss(&lru, 2, b"b");

        let fifo = store_with_policy(330, 1, EvictionPolicy::Fifo);
        publish_clean(&fifo, 1, b"a", 1);
        publish_clean(&fifo, 2, b"b", 2);
        assert_hit(&fifo, 1, b"a");
        publish_clean(&fifo, 3, b"c", 3);
        assert_miss(&fifo, 1, b"a");
        assert_hit(&fifo, 2, b"b");
    }

    #[test]
    fn tinylfu_requires_a_candidate_to_outscore_the_lru_victim() {
        let store = store_with_policy(330, 1, EvictionPolicy::TinyLfu);
        publish_clean(&store, 1, b"hot", 1);
        publish_clean(&store, 2, b"cold", 2);
        assert_hit(&store, 1, b"hot");

        assert!(!store.publish_pending(3, 0, b"cand", b"c", 0, 3));
        assert!(store.publish_pending(3, 0, b"cand", b"c", 0, 4));
        assert_miss(&store, 2, b"cold");
        assert_hit(&store, 3, b"cand");

        let metrics = store.metrics_snapshot();
        assert_eq!(metrics.admission_rejections, 1);
        assert_eq!(metrics.bypasses, 1);
        assert_eq!(metrics.evictions, 1);
    }

    #[test]
    fn sieve_keeps_a_visited_old_entry_and_demotes_an_unvisited_newer_one() {
        let store = store_with_policy(330, 1, EvictionPolicy::Sieve);
        publish_clean(&store, 1, b"a", 1);
        publish_clean(&store, 2, b"b", 2);
        assert_hit(&store, 1, b"a");
        publish_clean(&store, 3, b"c", 3);

        assert_hit(&store, 1, b"a");
        assert_miss(&store, 2, b"b");
        assert_hit(&store, 3, b"c");
    }
}
