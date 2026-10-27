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

use std::collections::HashMap;

use super::*;

const MIXED_OPERATIONS: u64 = 4_000;
const CONCURRENT_OPERATIONS: usize = 12_000;
const CONCURRENT_WORKERS: usize = 8;
const KEY_COUNT: usize = 64;
const VALUE_LENGTHS: [usize; 11] = [0, 1, 63, 64, 127, 128, 193, 257, 511, 1024, 2048];

#[derive(Clone)]
struct RetainedValueSnapshot {
    value: MemoryValue,
    key: Vec<u8>,
    bytes: Vec<u8>,
    seqno: u64,
}

impl RetainedValueSnapshot {
    fn new(value: MemoryValue) -> Self {
        Self {
            key: value.key().to_vec(),
            bytes: value.as_ref().to_vec(),
            seqno: value.seqno(),
            value,
        }
    }

    fn assert_unchanged(&self) {
        assert_eq!(self.value.key(), self.key);
        assert_eq!(self.value.as_ref(), self.bytes);
        assert_eq!(self.value.seqno(), self.seqno);
    }
}

fn next_random(state: &mut u64) -> u64 {
    // Fixed nonzero seeds keep failures reproducible without a new dependency.
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn random_index(state: &mut u64, length: usize) -> usize {
    (next_random(state) % length as u64) as usize
}

fn key_and_hash(id: usize) -> (Vec<u8>, u64) {
    let key = format!("key:{id:02}{}", "x".repeat(id % 9)).into_bytes();
    // Deliberate full-hash collisions also exercise removal from hash chains.
    let hash = (id as u64 % 12).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (key, hash)
}

/// Compare charges with unique live allocations after all writers have stopped.
/// Clones shared by residents and callers must contribute only one charge.
fn assert_accounting(store: &MemoryStore, retained: &[RetainedValueSnapshot]) {
    let mut allocations = HashMap::new();
    let mut budgets = HashMap::new();
    let mut total_resident = 0;
    let mut total_entries = 0;
    let mut record = |value: &MemoryValue| {
        let inner = &value.0;
        assert_eq!(inner._charge.bytes, value.charged_bytes());
        let budget = inner
            ._charge
            .budget
            .as_ref()
            .expect("live allocation must keep its charge armed");
        allocations.insert(
            Arc::as_ptr(inner),
            (Arc::as_ptr(budget), inner._charge.bytes),
        );
    };
    for shard in &store.shards {
        let shard = shard.lock().unwrap();
        let mut resident = 0;
        let mut entries = 0;
        for (index, entry) in shard.slots.iter().enumerate() {
            if let Some(entry) = entry {
                let hash = shard.policy_slots[index].hash();
                assert_eq!(shard.find(hash, entry.value.key()), Some(index));
                assert!(Arc::ptr_eq(
                    entry.value.0._charge.budget.as_ref().unwrap(),
                    &shard.budget,
                ));
                record(&entry.value);
                resident += entry.value.charged_bytes();
                entries += 1;
            }
        }
        assert_eq!(shard.resident_bytes, resident);
        assert_eq!(shard.free_count + entries, shard.slots.len());
        assert!(shard.budget.used_bytes() <= shard.budget.capacity_bytes);
        budgets.insert(Arc::as_ptr(&shard.budget), shard.budget.used_bytes());
        total_resident += resident;
        total_entries += entries;
    }
    for value in retained {
        value.assert_unchanged();
        record(&value.value);
    }
    let mut expected = HashMap::<_, usize>::new();
    for (budget, bytes) in allocations.values() {
        assert!(budgets.contains_key(budget));
        *expected.entry(*budget).or_default() += bytes;
    }
    for (budget, used) in budgets {
        assert_eq!(used, expected.get(&budget).copied().unwrap_or(0));
    }
    let snapshot = store.detailed_snapshot().unwrap();
    assert_eq!(snapshot.resident_entries, total_entries);
    assert_eq!(snapshot.resident_bytes, total_resident);
    assert_eq!(
        snapshot.retained_bytes + total_resident,
        expected.values().sum()
    );
}

fn budgets(store: &MemoryStore) -> Vec<Arc<MemoryBudget>> {
    store
        .shards
        .iter()
        .map(|shard| Arc::clone(&shard.lock().unwrap().budget))
        .collect()
}

#[test]
fn mixed_sizes_and_retained_owners_have_exact_charges() {
    for policy in [L1EvictionPolicy::Clock, L1EvictionPolicy::S3Fifo] {
        let mut saw_multi_victim = false;
        // Cover byte pressure, slot pressure, and uneven per-shard budgets.
        for (capacity, slots, shards) in [(4096, 40, 1), (8192, 7, 3), (8192, 80, 3)] {
            for seed in [1, 42, 777] {
                let store = MemoryStore::new(capacity, slots, shards, policy, true).unwrap();
                let mut retained: Vec<RetainedValueSnapshot> = Vec::new();
                let mut random = seed;
                for seqno in 1..=MIXED_OPERATIONS {
                    let (key, hash) = key_and_hash(random_index(&mut random, KEY_COUNT));
                    let value_len = VALUE_LENGTHS[random_index(&mut random, VALUE_LENGTHS.len())];
                    let value = vec![(seqno % 251) as u8; value_len];
                    let previous_evictions = store.metrics_snapshot().evictions;
                    match next_random(&mut random) % 10 {
                        0..=4 => {
                            store.publish(hash, &key, &value, seqno);
                        }
                        5 => {
                            let result = match store.lookup(hash, &key) {
                                MemoryLookup::Hit(value) => Some(value),
                                MemoryLookup::Miss(token) => {
                                    store.promote(token, hash, &key, &value, seqno)
                                }
                            };
                            if let Some(value) = result {
                                assert_eq!(value.key(), key);
                                retained.push(RetainedValueSnapshot::new(value));
                            }
                        }
                        6 => {
                            store.delete(hash, &key, seqno);
                        }
                        7 => {
                            if !retained.is_empty() {
                                let index = random_index(&mut random, retained.len());
                                retained.push(retained[index].clone());
                            }
                        }
                        8 => {
                            if !retained.is_empty() {
                                let index = random_index(&mut random, retained.len());
                                retained.swap_remove(index);
                            }
                        }
                        _ => {
                            if let MemoryLookup::Hit(value) = store.lookup(hash, &key) {
                                assert_eq!(value.key(), key);
                                retained.push(RetainedValueSnapshot::new(value));
                            }
                        }
                    }
                    saw_multi_victim |= store.metrics_snapshot().evictions - previous_evictions > 1;
                    if retained.len() > 24 {
                        retained.remove(0);
                    }
                    assert_accounting(&store, &retained);
                }
                retained.clear();
                assert_accounting(&store, &retained);
                let budgets = budgets(&store);
                drop(store);
                for budget in budgets {
                    assert_eq!(budget.used_bytes(), 0);
                }
            }
        }
        assert!(
            saw_multi_victim,
            "each policy must transfer charges from multiple victims"
        );
    }
}

#[test]
fn concurrent_publication_clone_and_drop_preserve_old_values() {
    for policy in [L1EvictionPolicy::Clock, L1EvictionPolicy::S3Fifo] {
        for shards in [1, 4] {
            let store = Arc::new(MemoryStore::new(32768, 128, shards, policy, true).unwrap());
            for id in 0..KEY_COUNT {
                let (key, hash) = key_and_hash(id);
                assert!(store.publish(hash, &key, &[1; 64], id as u64 + 1));
            }
            let sequence = Arc::new(AtomicU64::new(KEY_COUNT as u64 + 1));
            let start = Arc::new(Barrier::new(CONCURRENT_WORKERS));
            let mut workers = Vec::new();
            for worker in 0..CONCURRENT_WORKERS {
                let store = Arc::clone(&store);
                let sequence = Arc::clone(&sequence);
                let start = Arc::clone(&start);
                // Every worker owns a known old value before mutation starts,
                // even if scheduling makes its later lookups miss.
                let (key, hash) = key_and_hash(worker);
                let MemoryLookup::Hit(value) = store.lookup(hash, &key) else {
                    panic!("preloaded value must be resident before writers start");
                };
                let initial = RetainedValueSnapshot::new(value);
                workers.push(std::thread::spawn(move || {
                    let mut random = worker as u64 + 1;
                    let mut retained: Vec<RetainedValueSnapshot> = Vec::new();
                    start.wait();
                    for iteration in 0..CONCURRENT_OPERATIONS {
                        let (key, hash) = key_and_hash(random_index(&mut random, KEY_COUNT));
                        let seqno = sequence.fetch_add(1, Ordering::Relaxed);
                        if worker < CONCURRENT_WORKERS / 2 {
                            let value =
                                vec![(seqno % 251) as u8; [64, 128, 512, 2048][iteration % 4]];
                            if iteration % 23 == 0 {
                                store.delete(hash, &key, seqno);
                            } else {
                                store.publish(hash, &key, &value, seqno);
                            }
                        } else if let MemoryLookup::Hit(value) = store.lookup(hash, &key) {
                            assert_eq!(value.key(), key);
                            retained.push(RetainedValueSnapshot::new(value));
                        }
                        if !retained.is_empty() && iteration % 3 == 0 {
                            let index = random_index(&mut random, retained.len());
                            retained.push(retained[index].clone());
                        }
                        initial.assert_unchanged();
                        for value in &retained {
                            value.assert_unchanged();
                        }
                        while retained.len() > 12 {
                            let index = random_index(&mut random, retained.len());
                            retained.swap_remove(index);
                        }
                        if iteration % 31 == 0 {
                            std::thread::yield_now();
                        }
                    }
                    retained.push(initial);
                    retained
                }));
            }
            let mut retained = Vec::new();
            for worker in workers {
                retained.extend(worker.join().unwrap());
            }
            assert_accounting(&store, &retained);
            retained.clear();
            assert_accounting(&store, &retained);
            let budgets = budgets(&store);
            drop(store);
            for budget in budgets {
                assert_eq!(budget.used_bytes(), 0);
            }
        }
    }
}

#[test]
fn last_external_drop_before_reuse_releases_original_charge_once() {
    for policy in [L1EvictionPolicy::Clock, L1EvictionPolicy::S3Fifo] {
        let required = memory_entry_bytes(b"old", &[1; 128]).unwrap();
        let mut shard = MemoryShard::new(2 * required, 2, policy).unwrap();
        let result = shard.insert(1, b"old", &[1; 128], 1, required, None);
        let slot = result.slot.unwrap() as usize;
        let held = shard.slots[slot].as_ref().unwrap().value.clone();
        let address = Arc::as_ptr(&held.0);
        let ChargeResult::Charged {
            charge, recycled, ..
        } = shard.charge_with_eviction(required, Some(slot))
        else {
            panic!("independent new charge should fit");
        };
        assert_eq!(shard.budget.used_bytes(), 2 * required);
        assert!(!recycled.as_ref().unwrap().releases_charge_on_remove());

        // Force the last caller to drop after detachment but before the
        // factory rechecks exclusivity. The original charge is still armed.
        std::thread::spawn(move || drop(held)).join().unwrap();
        let replacement = MemoryValue::try_new(b"new", &[2; 128], 2, charge, recycled)
            .ok()
            .unwrap();
        assert_eq!(Arc::as_ptr(&replacement.0), address);
        assert_eq!(shard.budget.used_bytes(), required);
        assert_eq!(replacement.key(), b"new");
        assert_eq!(replacement.as_ref(), &[2; 128]);
        assert_eq!(replacement.seqno(), 2);
        drop(replacement);
        assert_eq!(shard.budget.used_bytes(), 0);
    }
}
