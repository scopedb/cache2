//! Exact-key visibility fences for detached Hybrid write-back values.
//!
//! The directory contains only bounded background work. Its shard locks are
//! held for short state transitions; device I/O never runs under a directory
//! lock. A directory hit means an older lower-tier value must stay hidden
//! until the detached value is either persisted or explicitly invalidated.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use crate::policy::NamespaceId;

pub(crate) const PENDING_WRITE_OWNED_OVERHEAD_BYTES: usize = 256;
pub(crate) const PENDING_WRITE_SHARD_OVERHEAD_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PendingWriteSnapshot {
    pub(crate) entries: u64,
    pub(crate) entries_peak: u64,
    pub(crate) bytes: u64,
    pub(crate) bytes_peak: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingRegisterError {
    AlreadyPending,
    AllocationFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PendingTerminal {
    Active,
    Finished,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PendingWaitOutcome {
    Finished,
    Failed,
}

pub(crate) struct PendingWriteSlot {
    namespace: NamespaceId,
    key: Vec<u8>,
    hash: u64,
    charged_bytes: usize,
    state: Mutex<PendingTerminal>,
    finished: Condvar,
}

impl PendingWriteSlot {
    pub(crate) fn wait_finished(&self) -> (PendingWaitOutcome, Duration) {
        let started = Instant::now();
        let mut state = lock_mutex(&self.state);
        while *state == PendingTerminal::Active {
            state = self
                .finished
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let outcome = match *state {
            PendingTerminal::Finished => PendingWaitOutcome::Finished,
            PendingTerminal::Failed => PendingWaitOutcome::Failed,
            PendingTerminal::Active => unreachable!("pending wait exits only at a terminal state"),
        };
        (outcome, started.elapsed())
    }

    pub(crate) fn failed(&self) -> bool {
        *lock_mutex(&self.state) == PendingTerminal::Failed
    }

    fn matches(&self, namespace: NamespaceId, key: &[u8]) -> bool {
        self.namespace == namespace && self.key == key
    }
}

pub(crate) struct PendingWriteDirectory {
    shards: Vec<Mutex<HashMap<u64, Vec<Arc<PendingWriteSlot>>>>>,
    shard_mask: usize,
    entries: AtomicU64,
    entries_peak: AtomicU64,
    bytes: AtomicU64,
    bytes_peak: AtomicU64,
}

impl PendingWriteDirectory {
    pub(crate) fn try_new(shards: usize) -> Result<Self, PendingRegisterError> {
        if shards == 0 || !shards.is_power_of_two() {
            return Err(PendingRegisterError::AllocationFailed);
        }
        let mut tables = Vec::new();
        tables
            .try_reserve_exact(shards)
            .map_err(|_| PendingRegisterError::AllocationFailed)?;
        tables.resize_with(shards, || Mutex::new(HashMap::new()));
        Ok(Self {
            shards: tables,
            shard_mask: shards - 1,
            entries: AtomicU64::new(0),
            entries_peak: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            bytes_peak: AtomicU64::new(0),
        })
    }

    pub(crate) fn find(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        hash: u64,
    ) -> Option<Arc<PendingWriteSlot>> {
        let shard = lock_mutex(&self.shards[hash as usize & self.shard_mask]);
        shard.get(&hash).and_then(|slots| {
            slots
                .iter()
                .find(|slot| slot.matches(namespace, key))
                .cloned()
        })
    }

    pub(crate) fn try_register(
        &self,
        namespace: NamespaceId,
        key: &[u8],
        hash: u64,
        charged_bytes: usize,
    ) -> Result<Arc<PendingWriteSlot>, PendingRegisterError> {
        let mut shard = lock_mutex(&self.shards[hash as usize & self.shard_mask]);
        if shard
            .get(&hash)
            .is_some_and(|slots| slots.iter().any(|slot| slot.matches(namespace, key)))
        {
            return Err(PendingRegisterError::AlreadyPending);
        }

        let mut owned_key = Vec::new();
        owned_key
            .try_reserve_exact(key.len())
            .map_err(|_| PendingRegisterError::AllocationFailed)?;
        owned_key.extend_from_slice(key);
        if !shard.contains_key(&hash) {
            shard
                .try_reserve(1)
                .map_err(|_| PendingRegisterError::AllocationFailed)?;
        }
        let slots = shard.entry(hash).or_default();
        slots
            .try_reserve(1)
            .map_err(|_| PendingRegisterError::AllocationFailed)?;
        let slot = Arc::new(PendingWriteSlot {
            namespace,
            key: owned_key,
            hash,
            charged_bytes,
            state: Mutex::new(PendingTerminal::Active),
            finished: Condvar::new(),
        });
        slots.push(Arc::clone(&slot));
        drop(shard);

        let entries = self.entries.fetch_add(1, Ordering::Relaxed) + 1;
        update_peak(&self.entries_peak, entries);
        let bytes = self
            .bytes
            .fetch_add(charged_bytes as u64, Ordering::Relaxed)
            .saturating_add(charged_bytes as u64);
        update_peak(&self.bytes_peak, bytes);
        Ok(slot)
    }

    /// Remove a completed owner from the visibility directory and wake any
    /// same-key mutation that deliberately waited without holding ordering.
    pub(crate) fn finish(&self, slot: &Arc<PendingWriteSlot>) {
        {
            let mut state = lock_mutex(&slot.state);
            if *state != PendingTerminal::Active {
                return;
            }
            *state = PendingTerminal::Finished;
        }
        let mut shard = lock_mutex(&self.shards[slot.hash as usize & self.shard_mask]);
        let mut removed = false;
        if let Some(slots) = shard.get_mut(&slot.hash) {
            if let Some(index) = slots.iter().position(|item| Arc::ptr_eq(item, slot)) {
                slots.swap_remove(index);
                removed = true;
            }
            if slots.is_empty() {
                shard.remove(&slot.hash);
            }
        }
        drop(shard);

        if removed {
            self.entries.fetch_sub(1, Ordering::Relaxed);
            self.bytes
                .fetch_sub(slot.charged_bytes as u64, Ordering::Relaxed);
        }
        slot.finished.notify_all();
        debug_assert!(removed, "active pending owner must be directory-resident");
    }

    /// Keep a failed fence resident. The owning cache is poisoned before this
    /// transition, and a reader that already passed its first health check can
    /// still observe this terminal and refuse the stale lower value.
    pub(crate) fn fail(&self, slot: &Arc<PendingWriteSlot>) {
        let mut state = lock_mutex(&slot.state);
        if *state == PendingTerminal::Active {
            *state = PendingTerminal::Failed;
            slot.finished.notify_all();
        }
    }

    pub(crate) fn snapshot(&self) -> PendingWriteSnapshot {
        let entries = self.entries.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        PendingWriteSnapshot {
            entries,
            entries_peak: self.entries_peak.load(Ordering::Relaxed).max(entries),
            bytes,
            bytes_peak: self.bytes_peak.load(Ordering::Relaxed).max(bytes),
        }
    }
}

pub(crate) fn allocation_bytes(shards: usize) -> Option<usize> {
    shards.checked_mul(PENDING_WRITE_SHARD_OVERHEAD_BYTES)
}

fn update_peak(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (value > current).then_some(value)
    });
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_keys_sharing_a_hash_have_independent_fences() {
        let directory = PendingWriteDirectory::try_new(4).unwrap();
        let first = directory.try_register(7, b"first", 11, 100).unwrap();
        let second = directory.try_register(7, b"second", 11, 200).unwrap();

        assert!(directory.find(7, b"first", 11).is_some());
        assert!(directory.find(7, b"second", 11).is_some());
        assert!(directory.find(8, b"first", 11).is_none());
        assert_eq!(directory.snapshot().entries, 2);

        directory.finish(&first);
        assert!(directory.find(7, b"first", 11).is_none());
        assert!(directory.find(7, b"second", 11).is_some());
        directory.finish(&second);
        assert_eq!(directory.snapshot().entries, 0);
        assert_eq!(directory.snapshot().bytes, 0);
    }

    #[test]
    fn duplicate_exact_key_is_rejected_until_owner_finishes() {
        let directory = PendingWriteDirectory::try_new(2).unwrap();
        let slot = directory.try_register(3, b"key", 5, 64).unwrap();
        assert!(matches!(
            directory.try_register(3, b"key", 5, 64),
            Err(PendingRegisterError::AlreadyPending)
        ));
        directory.finish(&slot);
        assert!(directory.try_register(3, b"key", 5, 64).is_ok());
    }
}
