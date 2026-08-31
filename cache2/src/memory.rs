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

//! Bounded shard-local RAM tier for the Cache data path.
//!
//! L1 is process-local and never participates in recovery. Entries are visible
//! immediately, may be discarded at any time, and use a small bounded eviction
//! policy.

use std::io;
use std::ops::Deref;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};

use crate::eviction::{
    DetachedPolicy, EvictionState, MAX_POLICY_SCAN_STEPS, MAX_POLICY_SLOT_INDEX, PolicySlot,
};
use crate::hashing::{FixedPrehashedMap, route_hash};
use crate::runtime_config::L1EvictionPolicy;
use crate::snapshot::CacheL1Snapshot;

/// Charged retained-value ownership. Fixed entry, policy, and directory
/// storage is planned and allocated separately during open.
pub(crate) const MEMORY_ENTRY_OVERHEAD_BYTES: usize = 64;
/// Keep large Region records out of shard-local L1 critical sections. The
/// complete retained entry, including its key and fixed ownership charge, must
/// fit this bound before foreground publication or L2 promotion takes the lock.
const MAX_L1_ENTRY_BYTES: usize = 256 * 1024;
/// A sequence trailer uses the spare tail of the fixed entry-overhead charge
/// so compact resident slots do not enlarge the hot Arc allocation.
const MEMORY_VALUE_SEQNO_BYTES: usize = std::mem::size_of::<u64>();
/// Full-key collision work stays bounded for entries sharing one directory
/// fingerprint, including distinct keys with the same 64-bit cache hash.
const MAX_SAME_HASH_ENTRIES: usize = 8;
/// Hash chains use compact slot indices, matching the intrusive compressed
/// links used by the rest of the cache rather than allocating a bucket vector.
const NO_SLOT_INDEX: u32 = u32::MAX;
/// A foreground admission may discard only a small, fixed number of clean
/// entries. Larger compaction work degrades to L1 bypass instead of scaling
/// with shard occupancy.
const MAX_EVICTIONS_PER_INSERT: usize = 16;
const MAX_BUDGET_CAS_ATTEMPTS: usize = 8;
/// Bridge a concurrent lookup's short critical section without waiting behind
/// mutation work; exhausted attempts still fall through to L2 immediately.
const MAX_L1_LOOKUP_LOCK_ATTEMPTS: usize = 4;

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

    fn available_bytes(&self) -> usize {
        self.capacity_bytes
            .saturating_sub(self.used_bytes.load(Ordering::Acquire))
    }

    /// Atomically transfers exclusive victim charges to one candidate before
    /// any resident entry is removed. A failed CAS leaves both accounting and
    /// the resident set unchanged.
    fn try_transfer_charge(
        self: &Arc<Self>,
        released: usize,
        required: usize,
    ) -> MemoryChargeAttempt {
        let mut used = self.used_bytes.load(Ordering::Acquire);
        for _ in 0..MAX_BUDGET_CAS_ATTEMPTS {
            let Some(next) = used
                .checked_sub(released)
                .and_then(|retained| retained.checked_add(required))
            else {
                return MemoryChargeAttempt::Full;
            };
            if next > self.capacity_bytes {
                return MemoryChargeAttempt::Full;
            }
            match self
                .used_bytes
                .compare_exchange(used, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return MemoryChargeAttempt::Charged(MemoryCharge {
                        budget: Some(Arc::clone(self)),
                        bytes: required,
                    });
                }
                Err(observed) => used = observed,
            }
        }
        MemoryChargeAttempt::Contended
    }

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
    fn try_new(
        key: &[u8],
        value: &[u8],
        seqno: u64,
        charge: MemoryCharge,
    ) -> Result<Self, MemoryCharge> {
        let Some(length) = key
            .len()
            .checked_add(value.len())
            .and_then(|bytes| bytes.checked_add(MEMORY_VALUE_SEQNO_BYTES))
        else {
            return Err(charge);
        };
        let mut bytes = Vec::new();
        if bytes.try_reserve_exact(length).is_err() {
            return Err(charge);
        }
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value);
        bytes.extend_from_slice(&seqno.to_le_bytes());
        Ok(Self(Arc::new(MemoryValueInner {
            bytes: bytes.into_boxed_slice(),
            key_length: key.len(),
            _charge: charge,
        })))
    }

    pub(crate) fn charged_bytes(&self) -> usize {
        MEMORY_ENTRY_OVERHEAD_BYTES + self.0.bytes.len() - MEMORY_VALUE_SEQNO_BYTES
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.0.bytes[..self.0.key_length]
    }

    fn seqno(&self) -> u64 {
        let mut encoded = [0_u8; MEMORY_VALUE_SEQNO_BYTES];
        encoded.copy_from_slice(&self.0.bytes[self.0.bytes.len() - MEMORY_VALUE_SEQNO_BYTES..]);
        u64::from_le_bytes(encoded)
    }

    fn value(&self) -> &[u8] {
        &self.0.bytes[self.0.key_length..self.0.bytes.len() - MEMORY_VALUE_SEQNO_BYTES]
    }

    fn releases_charge_on_remove(&self) -> bool {
        Arc::strong_count(&self.0) == 1
    }

    fn disarm_exclusive_charge(&mut self) -> usize {
        let inner = Arc::get_mut(&mut self.0).expect("exclusive resident value gained an owner");
        inner._charge.disarm()
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

impl MemoryCharge {
    fn disarm(&mut self) -> usize {
        let was_charged = self.budget.take().is_some();
        debug_assert!(was_charged);
        self.bytes
    }
}

struct MemoryEntry {
    value: MemoryValue,
    hash_next: u32,
}

#[derive(Clone, Copy)]
struct DetachedAdmissionEntry {
    index: usize,
    policy: DetachedPolicy,
    weight: usize,
    transfers_charge: bool,
}

struct AdmissionPlan {
    victims: [Option<DetachedAdmissionEntry>; MAX_EVICTIONS_PER_INSERT],
    len: usize,
    released_bytes: usize,
}

impl AdmissionPlan {
    const fn new() -> Self {
        Self {
            victims: [None; MAX_EVICTIONS_PER_INSERT],
            len: 0,
            released_bytes: 0,
        }
    }

    fn push(&mut self, victim: DetachedAdmissionEntry) {
        let slot = self
            .victims
            .get_mut(self.len)
            .expect("admission plan exceeds its fixed victim budget");
        *slot = Some(victim);
        self.len += 1;
        self.released_bytes = self.released_bytes.saturating_add(victim.weight);
    }

    fn iter(&self) -> impl Iterator<Item = DetachedAdmissionEntry> + '_ {
        self.victims[..self.len]
            .iter()
            .map(|victim| victim.expect("planned victim slot is populated"))
    }

    fn iter_rev(&self) -> impl Iterator<Item = DetachedAdmissionEntry> + '_ {
        self.victims[..self.len]
            .iter()
            .rev()
            .map(|victim| victim.expect("planned victim slot is populated"))
    }
}

struct MemoryShard {
    budget: Arc<MemoryBudget>,
    eviction: EvictionState,
    directory: FixedPrehashedMap,
    slots: Box<[Option<MemoryEntry>]>,
    policy_slots: Box<[PolicySlot]>,
    free_head: u32,
    free_count: usize,
    resident_bytes: usize,
}

impl MemoryShard {
    fn new(
        capacity_bytes: usize,
        maximum_entries: usize,
        eviction_policy: L1EvictionPolicy,
    ) -> io::Result<Self> {
        let budget = Arc::new(MemoryBudget::new(capacity_bytes));
        let directory = FixedPrehashedMap::try_new(maximum_entries)?;
        let mut slots = Vec::new();
        slots.try_reserve_exact(maximum_entries).map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "cannot allocate L1 entry slots")
        })?;
        slots.resize_with(maximum_entries, || None);
        let mut policy_slots = Vec::new();
        policy_slots
            .try_reserve_exact(maximum_entries)
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "cannot allocate L1 policy slots",
                )
            })?;
        policy_slots.resize(maximum_entries, PolicySlot::default());
        for (index, slot) in policy_slots.iter_mut().enumerate() {
            let next = if index + 1 == maximum_entries {
                NO_SLOT_INDEX
            } else {
                u32::try_from(index + 1).expect("validated L1 entry capacity exceeds u32")
            };
            *slot = PolicySlot::new_free(next);
        }
        Ok(Self {
            budget,
            eviction: EvictionState::new(eviction_policy, capacity_bytes, maximum_entries)?,
            directory,
            slots: slots.into_boxed_slice(),
            policy_slots: policy_slots.into_boxed_slice(),
            free_head: if maximum_entries == 0 {
                NO_SLOT_INDEX
            } else {
                0
            },
            free_count: maximum_entries,
            resident_bytes: 0,
        })
    }

    fn directory_head(&self, hash: u64) -> Option<u32> {
        self.directory.get(hash)
    }

    fn directory_can_upsert(&self, hash: u64) -> bool {
        self.directory.can_upsert(hash)
    }

    fn directory_upsert(&mut self, hash: u64, head: u32) -> Option<u32> {
        self.directory
            .insert(hash, head)
            .expect("preflighted fixed L1 directory insertion")
    }

    fn directory_remove(&mut self, hash: u64) -> Option<u32> {
        self.directory.remove(hash)
    }

    fn find(&self, hash: u64, key: &[u8]) -> Option<usize> {
        let mut cursor = self.directory_head(hash)?;
        for _ in 0..MAX_SAME_HASH_ENTRIES {
            let index = usize::try_from(cursor).ok()?;
            let entry = self.slots.get(index).and_then(Option::as_ref)?;
            if self.policy_slots.get(index).map(PolicySlot::hash) == Some(hash)
                && entry.value.key() == key
            {
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
        self.eviction.remove(&mut self.policy_slots, index);
        self.remove_slot_after_policy(index, hash, false);
    }

    fn remove_slot_after_policy(&mut self, index: usize, hash: u64, transfer_charge: bool) {
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

        let resident_bytes = self.slots[index]
            .as_ref()
            .expect("resident slot disappeared")
            .value
            .charged_bytes();
        if transfer_charge {
            let released = self.slots[index]
                .as_mut()
                .expect("resident slot disappeared")
                .value
                .disarm_exclusive_charge();
            debug_assert_eq!(
                released,
                self.slots[index]
                    .as_ref()
                    .expect("resident slot disappeared")
                    .value
                    .charged_bytes()
            );
        }
        let _entry = self.slots[index].take().expect("resident slot disappeared");
        self.resident_bytes = self.resident_bytes.saturating_sub(resident_bytes);
        debug_assert!(self.free_count < self.slots.len());
        let packed = u32::try_from(index).expect("memory slot index exceeds u32");
        self.policy_slots[index] = PolicySlot::new_free(self.free_head);
        self.free_head = packed;
        self.free_count += 1;
    }

    fn detach_for_admission(&mut self, index: usize) -> DetachedAdmissionEntry {
        let entry = self.slots[index]
            .as_ref()
            .expect("resident policy slot has no memory entry");
        let weight = entry.value.charged_bytes();
        let transfers_charge = entry.value.releases_charge_on_remove();
        let policy = self
            .eviction
            .detach_for_admission(&mut self.policy_slots, index, weight);
        debug_assert_eq!(policy.weight(), weight);
        DetachedAdmissionEntry {
            index,
            policy,
            weight,
            transfers_charge,
        }
    }

    fn restore_detached(&mut self, entry: DetachedAdmissionEntry) {
        self.eviction
            .restore_for_admission(&mut self.policy_slots, entry.index, entry.policy);
    }

    fn commit_detached(&mut self, entry: DetachedAdmissionEntry, record_eviction: bool) {
        self.remove_slot_after_policy(entry.index, entry.policy.hash(), entry.transfers_charge);
        if record_eviction {
            self.eviction.commit_eviction(entry.policy);
        }
    }

    fn restore_admission(
        &mut self,
        replacement: Option<DetachedAdmissionEntry>,
        plan: &AdmissionPlan,
    ) {
        for victim in plan.iter_rev() {
            self.restore_detached(victim);
        }
        if let Some(replacement) = replacement {
            self.restore_detached(replacement);
        }
    }

    fn charge_with_eviction(
        &mut self,
        required: usize,
        replacement: Option<usize>,
    ) -> ChargeResult {
        if required > self.budget.capacity_bytes {
            return ChargeResult::Rejected { evictions: 0 };
        }
        let needs_slot = replacement.is_none() && self.free_head == NO_SLOT_INDEX;
        match self.budget.try_charge(required) {
            MemoryChargeAttempt::Charged(charge) => {
                if let Some(replacement) = replacement {
                    self.remove_slot(replacement);
                } else if needs_slot {
                    return self.charge_with_slot_eviction(charge);
                }
                return ChargeResult::Charged {
                    charge,
                    evictions: 0,
                };
            }
            MemoryChargeAttempt::Contended => {
                return ChargeResult::Rejected { evictions: 0 };
            }
            MemoryChargeAttempt::Full => {}
        }

        let replacement = replacement.map(|index| self.detach_for_admission(index));
        let replacement_bytes = replacement
            .filter(|entry| entry.transfers_charge)
            .map_or(0, |entry| entry.weight);
        let mut plan = AdmissionPlan::new();
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;
        while replacement_bytes.saturating_add(plan.released_bytes)
            < required.saturating_sub(self.budget.available_bytes())
            && plan.len < MAX_EVICTIONS_PER_INSERT
            && remaining_steps > 0
        {
            let candidate = {
                let slots = &self.slots;
                self.eviction
                    .select_victim(&mut self.policy_slots, &mut remaining_steps, |index| {
                        slots[index]
                            .as_ref()
                            .expect("resident policy slot has no memory entry")
                            .value
                            .releases_charge_on_remove()
                    })
            };
            let Some(candidate) = candidate else {
                break;
            };
            let detached = self.detach_for_admission(candidate);
            debug_assert!(detached.transfers_charge);
            plan.push(detached);
        }
        let released = replacement_bytes.saturating_add(plan.released_bytes);
        let required_release = required.saturating_sub(self.budget.available_bytes());
        if released < required_release {
            self.restore_admission(replacement, &plan);
            return ChargeResult::Rejected { evictions: 0 };
        }

        let charge = match self.budget.try_transfer_charge(released, required) {
            MemoryChargeAttempt::Charged(charge) => charge,
            MemoryChargeAttempt::Full | MemoryChargeAttempt::Contended => {
                self.restore_admission(replacement, &plan);
                return ChargeResult::Rejected { evictions: 0 };
            }
        };

        if let Some(replacement) = replacement {
            self.commit_detached(replacement, false);
        }
        for victim in plan.iter() {
            self.commit_detached(victim, true);
        }
        ChargeResult::Charged {
            charge,
            evictions: plan.len,
        }
    }

    fn charge_with_slot_eviction(&mut self, charge: MemoryCharge) -> ChargeResult {
        let mut remaining_steps = MAX_POLICY_SCAN_STEPS;
        let candidate = {
            self.eviction
                .select_victim(&mut self.policy_slots, &mut remaining_steps, |_| true)
        };
        let Some(candidate) = candidate else {
            return ChargeResult::Rejected { evictions: 0 };
        };
        let mut victim = self.detach_for_admission(candidate);
        // The candidate was charged independently because byte capacity was
        // available. Let an exclusive victim release its own charge normally.
        victim.transfers_charge = false;
        self.commit_detached(victim, true);
        ChargeResult::Charged {
            charge,
            evictions: 1,
        }
    }

    fn insert(
        &mut self,
        hash: u64,
        key: &[u8],
        value: &[u8],
        seqno: u64,
        charged_bytes: usize,
        replacement: Option<usize>,
    ) -> MemoryInsertResult {
        if (replacement.is_none() && self.hash_chain_is_full(hash))
            || !self.directory_can_upsert(hash)
        {
            return MemoryInsertResult::bypassed();
        }

        let (charge, evictions) = match self.charge_with_eviction(charged_bytes, replacement) {
            ChargeResult::Charged { charge, evictions } => (charge, evictions),
            ChargeResult::Rejected { evictions } => {
                return MemoryInsertResult::rejected(evictions);
            }
        };
        let packed_index = self.free_head;
        if packed_index == NO_SLOT_INDEX {
            return MemoryInsertResult::rejected(evictions);
        }
        let Ok(index) = usize::try_from(packed_index) else {
            return MemoryInsertResult::rejected(evictions);
        };
        let Ok(memory_value) = MemoryValue::try_new(key, value, seqno, charge) else {
            return MemoryInsertResult::rejected(evictions);
        };
        let previous_head = self.directory_head(hash);
        let entry = MemoryEntry {
            value: memory_value,
            hash_next: previous_head.unwrap_or(NO_SLOT_INDEX),
        };
        self.free_head = self.policy_slots[index].free_next();
        debug_assert_ne!(self.free_count, 0);
        self.free_count -= 1;
        self.slots[index] = Some(entry);
        self.resident_bytes = self.resident_bytes.saturating_add(charged_bytes);
        self.eviction
            .insert(&mut self.policy_slots, index, hash, charged_bytes);
        let previous = self.directory_upsert(hash, packed_index);
        debug_assert_eq!(previous, previous_head);
        MemoryInsertResult {
            slot: Some(packed_index),
            evictions,
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
    },
}

#[derive(Clone, Copy, Debug, Default)]
struct MemoryInsertResult {
    slot: Option<u32>,
    evictions: usize,
}

impl MemoryInsertResult {
    const fn bypassed() -> Self {
        Self {
            slot: None,
            evictions: 0,
        }
    }

    const fn rejected(evictions: usize) -> Self {
        Self {
            slot: None,
            evictions,
        }
    }

    const fn inserted(self) -> bool {
        self.slot.is_some()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MemoryReadToken {
    shard_id: usize,
}

pub(crate) enum MemoryLookup {
    Hit(MemoryValue),
    Miss(MemoryReadToken),
}

pub(crate) struct MemoryStore {
    shards: Box<[MemoryShardLock]>,
    metrics: MemoryMetrics,
    entry_capacity: usize,
    metadata_bytes: usize,
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
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct MemoryMetricsSnapshot {
    pub(crate) evictions: u64,
    pub(crate) bypasses: u64,
}

impl MemoryStore {
    pub(crate) fn maximum_entry_capacity(capacity_bytes: usize, shard_count: usize) -> usize {
        if shard_count == 0 {
            return 0;
        }
        (0..shard_count).fold(0_usize, |total, shard_id| {
            total.saturating_add(
                (shard_share(capacity_bytes, shard_count, shard_id) / MEMORY_ENTRY_OVERHEAD_BYTES)
                    .min(MAX_POLICY_SLOT_INDEX.saturating_add(1)),
            )
        })
    }

    pub(crate) fn new(
        capacity_bytes: usize,
        entry_capacity: usize,
        shard_count: usize,
        eviction_policy: L1EvictionPolicy,
        statistics_enabled: bool,
    ) -> io::Result<Self> {
        if shard_count == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory tier requires at least one shard",
            ));
        }
        let metadata_bytes =
            Self::allocation_bytes(capacity_bytes, entry_capacity, shard_count, eviction_policy)?;
        let mut shards = Vec::new();
        shards.try_reserve_exact(shard_count).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate memory-tier shards",
            )
        })?;
        let mut actual_entry_capacity = 0_usize;
        for shard_id in 0..shard_count {
            let shard_capacity = shard_share(capacity_bytes, shard_count, shard_id);
            let shard_entries = shard_share(entry_capacity, shard_count, shard_id)
                .min(shard_capacity / MEMORY_ENTRY_OVERHEAD_BYTES)
                .min(MAX_POLICY_SLOT_INDEX.saturating_add(1));
            actual_entry_capacity = actual_entry_capacity
                .checked_add(shard_entries)
                .ok_or_else(|| invalid_memory_plan("L1 entry capacity overflow"))?;
            shards.push(MemoryShardLock(Mutex::new(MemoryShard::new(
                shard_capacity,
                shard_entries,
                eviction_policy,
            )?)));
        }
        Ok(Self {
            shards: shards.into_boxed_slice(),
            metrics: MemoryMetrics {
                enabled: statistics_enabled,
                evictions: AtomicU64::new(0),
                bypasses: AtomicU64::new(0),
            },
            entry_capacity: actual_entry_capacity,
            metadata_bytes,
        })
    }

    pub(crate) fn allocation_bytes(
        capacity_bytes: usize,
        entry_capacity: usize,
        shard_count: usize,
        eviction_policy: L1EvictionPolicy,
    ) -> io::Result<usize> {
        if shard_count == 0 {
            return Err(invalid_memory_plan(
                "memory tier requires at least one shard",
            ));
        }
        let mut total = 0_usize;
        for shard_id in 0..shard_count {
            let shard_capacity = shard_share(capacity_bytes, shard_count, shard_id);
            let shard_entries = shard_share(entry_capacity, shard_count, shard_id)
                .min(shard_capacity / MEMORY_ENTRY_OVERHEAD_BYTES)
                .min(MAX_POLICY_SLOT_INDEX.saturating_add(1));
            let fixed_slots = shard_entries
                .checked_mul(std::mem::size_of::<Option<MemoryEntry>>())
                .and_then(|bytes| {
                    shard_entries
                        .checked_mul(std::mem::size_of::<PolicySlot>())
                        .and_then(|policy| bytes.checked_add(policy))
                })
                .ok_or_else(|| invalid_memory_plan("L1 slot memory plan overflow"))?;
            let directory = FixedPrehashedMap::allocation_bytes(shard_entries)?;
            let eviction = EvictionState::allocation_bytes(eviction_policy, shard_entries)?;
            total = total
                .checked_add(fixed_slots)
                .and_then(|bytes| bytes.checked_add(directory))
                .and_then(|bytes| bytes.checked_add(eviction))
                .ok_or_else(|| invalid_memory_plan("L1 metadata memory plan overflow"))?;
        }
        Ok(total)
    }

    pub(crate) fn publish(&self, hash: u64, key: &[u8], value: &[u8], seqno: u64) -> bool {
        let Some(charged_bytes) = memory_entry_bytes(key, value) else {
            self.record_insert(MemoryInsertResult::bypassed());
            return false;
        };
        let shard_id = self.route(hash);
        let Some(mut shard) = self.try_lock_shard(shard_id) else {
            self.record_insert(MemoryInsertResult::bypassed());
            return false;
        };
        let existing = shard.find(hash, key);
        if existing.is_some_and(|index| {
            shard.slots[index]
                .as_ref()
                .is_some_and(|entry| entry.value.seqno() >= seqno)
        }) {
            return true;
        }
        if charged_bytes > MAX_L1_ENTRY_BYTES {
            if let Some(index) = existing {
                shard.remove_slot(index);
            }
            drop(shard);
            self.record_insert(MemoryInsertResult::bypassed());
            return false;
        }
        let result = shard.insert(hash, key, value, seqno, charged_bytes, existing);
        drop(shard);
        self.record_insert(result);
        result.inserted()
    }

    /// Best-effort exact-key cleanup for a sequenced delete. Contention
    /// bypasses L1, and a newer resident value is never removed.
    pub(crate) fn delete(&self, hash: u64, key: &[u8], seqno: u64) -> bool {
        let shard_id = self.route(hash);
        let Some(mut shard) = self.try_lock_shard(shard_id) else {
            self.record_insert(MemoryInsertResult::bypassed());
            return false;
        };
        let Some(index) = shard.find(hash, key) else {
            return true;
        };
        if shard.slots[index]
            .as_ref()
            .is_some_and(|entry| entry.value.seqno() > seqno)
        {
            return true;
        }
        shard.remove_slot(index);
        true
    }

    pub(crate) fn lookup(&self, hash: u64, key: &[u8]) -> MemoryLookup {
        let shard_id = self.route(hash);
        let mut attempts = 1;
        let mut shard = loop {
            match self.shards[shard_id].try_lock() {
                Ok(shard) => break shard,
                Err(TryLockError::WouldBlock) if attempts < MAX_L1_LOOKUP_LOCK_ATTEMPTS => {
                    attempts += 1;
                    std::hint::spin_loop();
                }
                Err(TryLockError::WouldBlock | TryLockError::Poisoned(_)) => {
                    return MemoryLookup::Miss(MemoryReadToken { shard_id });
                }
            }
        };
        let Some(index) = shard.find(hash, key) else {
            return MemoryLookup::Miss(MemoryReadToken { shard_id });
        };
        let shard = &mut *shard;
        shard.eviction.record_hit(&mut shard.policy_slots, index);
        let entry = shard.slots[index]
            .as_ref()
            .expect("memory directory points to a live slot");
        MemoryLookup::Hit(entry.value.clone())
    }

    pub(crate) fn promote(
        &self,
        token: MemoryReadToken,
        hash: u64,
        key: &[u8],
        value: &[u8],
        seqno: u64,
    ) -> Option<MemoryValue> {
        if token.shard_id != self.route(hash) {
            return None;
        }
        let Some(charged_bytes) = memory_entry_bytes(key, value) else {
            self.record_insert(MemoryInsertResult::bypassed());
            return None;
        };
        if charged_bytes > MAX_L1_ENTRY_BYTES {
            self.record_insert(MemoryInsertResult::bypassed());
            return None;
        }
        let mut shard = self.try_lock_shard(token.shard_id)?;
        let existing = shard.find(hash, key);
        if let Some(index) = existing
            && shard.slots[index]
                .as_ref()
                .is_some_and(|entry| entry.value.seqno() >= seqno)
        {
            let shard = &mut *shard;
            shard.eviction.record_hit(&mut shard.policy_slots, index);
            return Some(
                shard.slots[index]
                    .as_ref()
                    .expect("memory directory points to a live slot")
                    .value
                    .clone(),
            );
        }
        let result = shard.insert(hash, key, value, seqno, charged_bytes, existing);
        let promoted = result.slot.map(|packed_index| {
            let index = usize::try_from(packed_index).expect("memory slot index exceeds usize");
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
        }
    }

    pub(crate) fn detailed_snapshot(&self) -> io::Result<CacheL1Snapshot> {
        let mut resident_entries = 0_usize;
        let mut resident_bytes = 0_usize;
        let mut charged_bytes = 0_usize;
        for shard in &self.shards {
            let shard = shard
                .lock()
                .map_err(|_| io::Error::other("L1 shard is poisoned"))?;
            resident_entries = resident_entries
                .checked_add(shard.slots.len().saturating_sub(shard.free_count))
                .ok_or_else(|| invalid_memory_plan("L1 resident entry count overflow"))?;
            resident_bytes = resident_bytes
                .checked_add(shard.resident_bytes)
                .ok_or_else(|| invalid_memory_plan("L1 resident byte count overflow"))?;
            charged_bytes = charged_bytes
                .checked_add(shard.budget.used_bytes())
                .ok_or_else(|| invalid_memory_plan("L1 charged byte count overflow"))?;
        }
        Ok(CacheL1Snapshot {
            entry_capacity: self.entry_capacity,
            resident_entries,
            resident_bytes,
            retained_bytes: charged_bytes.saturating_sub(resident_bytes),
            metadata_bytes: self.metadata_bytes,
        })
    }

    fn record_insert(&self, result: MemoryInsertResult) {
        if !self.metrics.enabled {
            return;
        }
        self.metrics.evictions.fetch_add(
            u64::try_from(result.evictions).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        if !result.inserted() {
            self.metrics.bypasses.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn route(&self, hash: u64) -> usize {
        route_hash(hash, self.shards.len())
    }

    fn try_lock_shard(&self, shard_id: usize) -> Option<MutexGuard<'_, MemoryShard>> {
        self.shards[shard_id].try_lock().ok()
    }
}

fn memory_entry_bytes(key: &[u8], value: &[u8]) -> Option<usize> {
    MEMORY_ENTRY_OVERHEAD_BYTES
        .checked_add(key.len())
        .and_then(|bytes| bytes.checked_add(value.len()))
}

fn shard_share(total: usize, shard_count: usize, shard_id: usize) -> usize {
    total / shard_count + usize::from(shard_id < total % shard_count)
}

fn invalid_memory_plan(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(capacity_bytes: usize, shard_count: usize) -> MemoryStore {
        store_with_policy(capacity_bytes, shard_count, L1EvictionPolicy::Clock)
    }

    fn store_with_policy(
        capacity_bytes: usize,
        shard_count: usize,
        policy: L1EvictionPolicy,
    ) -> MemoryStore {
        MemoryStore::new(
            capacity_bytes,
            capacity_bytes / MEMORY_ENTRY_OVERHEAD_BYTES,
            shard_count,
            policy,
            true,
        )
        .unwrap()
    }

    fn assert_hit(store: &MemoryStore, hash: u64, key: &[u8]) {
        assert!(matches!(store.lookup(hash, key), MemoryLookup::Hit(_)));
    }

    fn assert_miss(store: &MemoryStore, hash: u64, key: &[u8]) {
        assert!(matches!(store.lookup(hash, key), MemoryLookup::Miss(_)));
    }

    #[test]
    fn memory_entry_slot_uses_two_machine_words() {
        let expected = 2 * std::mem::size_of::<usize>();
        assert_eq!(std::mem::size_of::<MemoryEntry>(), expected);
        assert_eq!(std::mem::size_of::<Option<MemoryEntry>>(), expected);
    }

    #[test]
    fn published_value_is_visible_and_immediately_evictable() {
        let store = store(512, 1);
        assert!(store.publish(7, b"a", b"value-a", 1));
        assert!(matches!(
            store.lookup(7, b"a"),
            MemoryLookup::Hit(value) if value.as_ref() == b"value-a"
        ));

        assert!(store.publish(8, b"b", &[2; 300], 2));
        assert!(matches!(
            store.lookup(8, b"b"),
            MemoryLookup::Hit(value) if value.len() == 300
        ));
    }

    #[test]
    fn entries_above_the_l1_limit_bypass_and_clean_an_older_exact_key() {
        let store = store(2 * MAX_L1_ENTRY_BYTES, 1);
        assert!(store.publish(7, b"key", b"old", 1));
        let oversized = vec![0xa5; MAX_L1_ENTRY_BYTES - MEMORY_ENTRY_OVERHEAD_BYTES - 3 + 1];

        assert!(!store.publish(7, b"key", &oversized, 2));
        assert_miss(&store, 7, b"key");
        assert_eq!(store.detailed_snapshot().unwrap().resident_entries, 0);
        assert_eq!(store.metrics_snapshot().bypasses, 1);

        let boundary = vec![0xa5; MAX_L1_ENTRY_BYTES - MEMORY_ENTRY_OVERHEAD_BYTES - 4];
        assert!(store.publish(8, b"edge", &boundary, 3));
        assert!(matches!(
            store.lookup(8, b"edge"),
            MemoryLookup::Hit(value) if value.len() == boundary.len()
        ));
    }

    #[test]
    fn l2_promotion_bypasses_above_the_l1_entry_limit() {
        let store = store(2 * MAX_L1_ENTRY_BYTES, 1);
        let MemoryLookup::Miss(token) = store.lookup(9, b"key") else {
            panic!("empty memory tier must miss");
        };
        let oversized = vec![0xa5; MAX_L1_ENTRY_BYTES - MEMORY_ENTRY_OVERHEAD_BYTES - 3 + 1];

        assert!(store.promote(token, 9, b"key", &oversized, 1).is_none());
        assert_miss(&store, 9, b"key");
        assert_eq!(store.detailed_snapshot().unwrap().resident_entries, 0);
        assert_eq!(store.metrics_snapshot().bypasses, 1);
    }

    #[test]
    fn delete_is_exact_and_does_not_remove_a_newer_value() {
        let store = store(4096, 1);
        assert!(store.publish(7, b"a", b"value-a", 10));
        assert!(store.publish(7, b"b", b"value-b", 11));

        assert!(store.delete(7, b"a", 12));
        assert!(matches!(store.lookup(7, b"a"), MemoryLookup::Miss(_)));
        assert!(matches!(
            store.lookup(7, b"b"),
            MemoryLookup::Hit(value) if value.as_ref() == b"value-b"
        ));

        assert!(store.publish(7, b"a", b"newer", 20));
        assert!(store.delete(7, b"a", 19));
        assert!(matches!(
            store.lookup(7, b"a"),
            MemoryLookup::Hit(value) if value.as_ref() == b"newer"
        ));
    }

    #[test]
    fn delete_bypasses_a_contended_l1_shard() {
        let store = store(4096, 1);
        assert!(store.publish(7, b"a", b"value", 10));
        let shard = store.shards[0].lock().unwrap();
        assert!(!store.delete(7, b"a", 11));
        drop(shard);

        assert_hit(&store, 7, b"a");
        assert_eq!(store.metrics_snapshot().bypasses, 1);
    }

    #[test]
    fn resident_reuses_an_unlinked_slot() {
        let store = MemoryStore::new(4096, 2, 1, L1EvictionPolicy::Clock, true).unwrap();
        {
            let shard = store.shards[0].lock().unwrap();
            assert_eq!(shard.free_head, 0);
            assert_eq!(shard.free_count, 2);
            assert_eq!(shard.policy_slots[0].free_next(), 1);
        }

        assert!(store.publish(1, b"a", b"value-a", 1));
        assert!(store.delete(1, b"a", 2));
        assert!(store.publish(2, b"b", b"value-b", 3));

        let shard = store.shards[0].lock().unwrap();
        assert_eq!(shard.free_head, 1);
        assert_eq!(shard.free_count, 1);
        assert_eq!(
            shard.slots[0]
                .as_ref()
                .expect("released slot must be reused")
                .value
                .key(),
            b"b"
        );
    }

    #[test]
    fn fixed_entry_capacity_replaces_without_growing_metadata() {
        let store = MemoryStore::new(4096, 1, 1, L1EvictionPolicy::Clock, true).unwrap();
        let metadata_bytes = store.metadata_bytes;
        let slot_len = store.shards[0].lock().unwrap().slots.len();

        assert!(store.publish(1, b"a", b"a", 1));
        assert!(store.publish(2, b"b", &[2; 1024], 2));

        assert_miss(&store, 1, b"a");
        assert_hit(&store, 2, b"b");
        let snapshot = store.detailed_snapshot().unwrap();
        assert_eq!(snapshot.entry_capacity, 1);
        assert_eq!(snapshot.resident_entries, 1);
        assert_eq!(snapshot.metadata_bytes, metadata_bytes);
        assert_eq!(store.shards[0].lock().unwrap().slots.len(), slot_len);
        assert_eq!(store.metrics_snapshot().evictions, 1);
    }

    #[test]
    fn retained_eviction_is_visible_in_detailed_memory_accounting() {
        let store = MemoryStore::new(4096, 1, 1, L1EvictionPolicy::Clock, true).unwrap();
        assert!(store.publish(1, b"a", &[1; 128], 1));
        let MemoryLookup::Hit(retained) = store.lookup(1, b"a") else {
            panic!("published value must be visible");
        };
        assert!(store.publish(2, b"b", &[2; 128], 2));

        let snapshot = store.detailed_snapshot().unwrap();
        assert_eq!(snapshot.resident_entries, 1);
        assert_eq!(
            snapshot.retained_bytes,
            MEMORY_ENTRY_OVERHEAD_BYTES + 1 + 128
        );
        drop(retained);
        assert_eq!(store.detailed_snapshot().unwrap().retained_bytes, 0);
    }

    #[test]
    fn l1_contention_bypasses_and_allows_l2_fallback() {
        let store = store(1024, 1);
        assert!(store.publish(7, b"key", b"old", 1));

        let shard = store.shards[0].lock().unwrap();
        assert!(matches!(store.lookup(7, b"key"), MemoryLookup::Miss(_)));
        assert!(!store.publish(7, b"key", b"new", 2));
        drop(shard);

        assert!(matches!(
            store.lookup(7, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"old"
        ));
    }

    #[test]
    fn same_hash_chain_disambiguates_full_key() {
        let store = store(2048, 1);
        let collision_hash = 42;
        assert!(store.publish(collision_hash, b"alpha", b"value-alpha", 1));
        assert!(store.publish(collision_hash, b"beta", b"value-beta", 2));
        assert!(store.publish(collision_hash, b"beta", b"replacement-beta", 3));

        for (key, expected) in [
            (b"alpha".as_slice(), b"value-alpha".as_slice()),
            (b"beta".as_slice(), b"replacement-beta".as_slice()),
        ] {
            assert!(matches!(
                store.lookup(collision_hash, key),
                MemoryLookup::Hit(value) if value.as_ref() == expected
            ));
        }
        assert!(matches!(
            store.lookup(collision_hash, b"foreign"),
            MemoryLookup::Miss(_)
        ));
    }

    #[test]
    fn fingerprint_chain_disambiguates_full_hash_before_key() {
        let store = store(2048, 1);
        let first = 1_u64;
        let colliding = 1_u64 << 45;
        assert!(store.publish(first, b"key", b"first", 1));
        assert!(store.publish(colliding, b"key", b"second", 2));

        assert!(matches!(
            store.lookup(first, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"first"
        ));
        assert!(matches!(
            store.lookup(colliding, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"second"
        ));

        assert!(store.delete(first, b"key", 3));
        assert_miss(&store, first, b"key");
        assert_hit(&store, colliding, b"key");
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
                key.as_bytes(),
                &[ordinal as u8],
                ordinal as u64 + 1,
            ));
        }
        assert!(!store.publish(
            collision_hash,
            keys[MAX_SAME_HASH_ENTRIES].as_bytes(),
            b"overflow",
            MAX_SAME_HASH_ENTRIES as u64 + 1,
        ));

        for (ordinal, key) in keys.iter().take(MAX_SAME_HASH_ENTRIES).enumerate() {
            assert!(matches!(
                store.lookup(collision_hash, key.as_bytes()),
                MemoryLookup::Hit(value) if value.as_ref() == [ordinal as u8]
            ));
        }
        assert_eq!(store.metrics_snapshot().bypasses, 1);
    }

    #[test]
    fn newer_exact_key_is_reused_for_a_delayed_older_promotion() {
        let store = store(1024, 1);
        let MemoryLookup::Miss(token) = store.lookup(9, b"key") else {
            panic!("empty memory tier must miss");
        };
        assert!(store.publish(9, b"key", b"new", 2));
        assert!(matches!(
            store.promote(token, 9, b"key", b"old", 1),
            Some(value) if value.as_ref() == b"new"
        ));
        assert!(matches!(
            store.lookup(9, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_l2_promotion_replaces_an_older_concurrent_l1_value() {
        let store = store(1024, 1);
        let MemoryLookup::Miss(token) = store.lookup(9, b"key") else {
            panic!("empty memory tier must miss");
        };
        assert!(store.publish(9, b"key", b"old", 1));

        assert!(matches!(
            store.promote(token, 9, b"key", b"new", 2),
            Some(value) if value.as_ref() == b"new"
        ));
        assert!(matches!(
            store.lookup(9, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_exact_key_publication_suppresses_a_delayed_older_put() {
        let store = store(1024, 1);
        assert!(store.publish(11, b"key", b"new", 2));
        assert!(store.publish(11, b"key", b"old", 1));
        assert!(matches!(
            store.lookup(11, b"key"),
            MemoryLookup::Hit(value) if value.as_ref() == b"new"
        ));
    }

    #[test]
    fn newer_unrelated_publication_does_not_suppress_an_older_key() {
        let store = store(1024, 1);
        assert!(store.publish(12, b"newer", b"newer", 2));
        assert!(store.publish(11, b"older", b"older", 1));
        assert_hit(&store, 12, b"newer");
        assert_hit(&store, 11, b"older");
    }

    #[test]
    fn failed_admission_preserves_a_retained_value_and_its_charge() {
        let store = store(512, 1);
        assert!(store.publish(21, b"a", &[1; 300], 1));
        let MemoryLookup::Hit(retained) = store.lookup(21, b"a") else {
            panic!("published value must be visible");
        };

        assert!(!store.publish(22, b"b", &[2; 300], 2));
        assert_hit(&store, 21, b"a");
        assert_eq!(
            store.shards[0].lock().unwrap().budget.used_bytes(),
            MEMORY_ENTRY_OVERHEAD_BYTES + 301
        );

        drop(retained);
        assert_eq!(
            store.shards[0].lock().unwrap().budget.used_bytes(),
            MEMORY_ENTRY_OVERHEAD_BYTES + 301
        );
        assert!(store.publish(22, b"b", &[2; 300], 3));
    }

    #[test]
    fn concurrent_retained_value_drops_leave_the_resident_charge_intact() {
        let store = store(512, 1);
        assert!(store.publish(21, b"a", &[1; 300], 1));
        let MemoryLookup::Hit(retained) = store.lookup(21, b"a") else {
            panic!("published value must be visible");
        };
        let clones = (0..8).map(|_| retained.clone()).collect::<Vec<_>>();

        assert!(!store.publish(22, b"b", &[2; 300], 2));
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

        assert_hit(&store, 21, b"a");
        assert_eq!(
            store.shards[0].lock().unwrap().budget.used_bytes(),
            MEMORY_ENTRY_OVERHEAD_BYTES + 301
        );
        assert!(store.publish(22, b"b", &[2; 300], 3));
    }

    #[test]
    fn multi_victim_admission_honors_the_fixed_eviction_budget() {
        const VICTIM_VALUE_BYTES: usize = 32;
        const VICTIM_BYTES: usize = MEMORY_ENTRY_OVERHEAD_BYTES + 1 + VICTIM_VALUE_BYTES;

        for victim_count in [MAX_EVICTIONS_PER_INSERT, MAX_EVICTIONS_PER_INSERT + 1] {
            let capacity = victim_count * VICTIM_BYTES;
            let store = store(capacity, 1);
            for hash in 1..=victim_count as u64 {
                assert!(store.publish(hash, b"k", &[hash as u8; VICTIM_VALUE_BYTES], hash));
            }
            let candidate = vec![0xa5; capacity - MEMORY_ENTRY_OVERHEAD_BYTES - 1];
            let within_budget = victim_count == MAX_EVICTIONS_PER_INSERT;

            assert_eq!(store.publish(100, b"c", &candidate, 100), within_budget);
            assert_eq!(
                store.shards[0].lock().unwrap().budget.used_bytes(),
                capacity
            );
            let metrics = store.metrics_snapshot();
            if within_budget {
                assert_hit(&store, 100, b"c");
                for hash in 1..=victim_count as u64 {
                    assert_miss(&store, hash, b"k");
                }
                assert_eq!(metrics.evictions, victim_count as u64);
                assert_eq!(metrics.bypasses, 0);
            } else {
                assert_miss(&store, 100, b"c");
                for hash in 1..=victim_count as u64 {
                    assert_hit(&store, hash, b"k");
                }
                assert_eq!(metrics.evictions, 0);
                assert_eq!(metrics.bypasses, 1);
            }
        }
    }

    #[test]
    fn failed_exact_key_expansion_preserves_the_old_value() {
        const ENTRY_COUNT: usize = MAX_EVICTIONS_PER_INSERT + 2;
        const VALUE_BYTES: usize = 32;
        const ENTRY_BYTES: usize = MEMORY_ENTRY_OVERHEAD_BYTES + 1 + VALUE_BYTES;
        const CAPACITY: usize = ENTRY_COUNT * ENTRY_BYTES;

        let store = store(CAPACITY, 1);
        for hash in 1..=ENTRY_COUNT as u64 {
            assert!(store.publish(hash, b"k", &[hash as u8; VALUE_BYTES], hash));
        }
        let replacement = vec![0xa5; CAPACITY - MEMORY_ENTRY_OVERHEAD_BYTES - 1];

        assert!(!store.publish(1, b"k", &replacement, 100));
        assert!(matches!(
            store.lookup(1, b"k"),
            MemoryLookup::Hit(value) if value.as_ref() == [1; VALUE_BYTES]
        ));
        assert_eq!(
            store.shards[0].lock().unwrap().budget.used_bytes(),
            CAPACITY
        );
        assert_eq!(store.metrics_snapshot().evictions, 0);
    }

    #[test]
    fn retained_exact_key_replacement_keeps_the_old_value_until_reclaimable() {
        let store = store(512, 1);
        assert!(store.publish(21, b"a", &[1; 300], 1));
        let MemoryLookup::Hit(retained) = store.lookup(21, b"a") else {
            panic!("expected retained value");
        };

        assert!(!store.publish(21, b"a", &[2; 300], 2));
        assert!(matches!(
            store.lookup(21, b"a"),
            MemoryLookup::Hit(value) if value.as_ref() == [1; 300]
        ));

        drop(retained);
        assert!(store.publish(21, b"a", &[2; 300], 3));
        assert!(matches!(
            store.lookup(21, b"a"),
            MemoryLookup::Hit(value) if value.as_ref() == [2; 300]
        ));
    }

    #[test]
    fn skewed_shard_admission_cannot_consume_another_shards_budget() {
        let store = store(1024, 2);
        assert!(store.publish(2, b"even-a", &[1; 300], 1));
        assert!(store.publish(4, b"even-b", &[2; 300], 2));
        assert!(store.publish(3, b"odd", &[3; 300], 3));
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
    fn s3fifo_retains_a_reused_hot_set_across_a_one_shot_scan() {
        const ENTRY_BYTES: usize = MEMORY_ENTRY_OVERHEAD_BYTES + 8 + 32;
        const ENTRY_CAPACITY: usize = 100;
        const HOT_ENTRIES: u64 = 60;
        let capacity = ENTRY_BYTES * ENTRY_CAPACITY;
        let clock =
            MemoryStore::new(capacity, ENTRY_CAPACITY, 1, L1EvictionPolicy::Clock, true).unwrap();
        let s3fifo =
            MemoryStore::new(capacity, ENTRY_CAPACITY, 1, L1EvictionPolicy::S3Fifo, true).unwrap();

        for ordinal in 0_u64..ENTRY_CAPACITY as u64 {
            let key = ordinal.to_le_bytes();
            let hash = (ordinal << 32) | ordinal;
            assert!(clock.publish(hash, &key, &[ordinal as u8; 32], ordinal));
            assert!(s3fifo.publish(hash, &key, &[ordinal as u8; 32], ordinal));
        }
        for _ in 0..2 {
            for ordinal in 0_u64..HOT_ENTRIES {
                let key = ordinal.to_le_bytes();
                let hash = (ordinal << 32) | ordinal;
                assert_hit(&clock, hash, &key);
                assert_hit(&s3fifo, hash, &key);
            }
        }
        for ordinal in ENTRY_CAPACITY as u64..1100 {
            let key = ordinal.to_le_bytes();
            let hash = (ordinal << 32) | ordinal;
            assert!(clock.publish(hash, &key, &[ordinal as u8; 32], ordinal));
            assert!(s3fifo.publish(hash, &key, &[ordinal as u8; 32], ordinal));
        }

        let clock_hits = (0_u64..HOT_ENTRIES)
            .filter(|ordinal| {
                matches!(
                    clock.lookup((*ordinal << 32) | *ordinal, &ordinal.to_le_bytes()),
                    MemoryLookup::Hit(_)
                )
            })
            .count();
        let s3fifo_hits = (0_u64..HOT_ENTRIES)
            .filter(|ordinal| {
                matches!(
                    s3fifo.lookup((*ordinal << 32) | *ordinal, &ordinal.to_le_bytes()),
                    MemoryLookup::Hit(_)
                )
            })
            .count();

        assert_eq!(clock_hits, 0);
        assert_eq!(s3fifo_hits, HOT_ENTRIES as usize);
        assert!(
            s3fifo.metadata_bytes > clock.metadata_bytes,
            "S3-FIFO ghost and queue metadata must be included in the memory plan"
        );
    }
}
