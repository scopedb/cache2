//! Bounded group commit for Hybrid route-journal intents.
//!
//! Accepted requests retain their complete owned key until one worker has
//! appended and synced the batch. Queue slots and owned-key bytes stay charged
//! while the batch is executing, so draining the queue into the worker cannot
//! temporarily escape the configured bounds.

use std::collections::VecDeque;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::cache::{CacheError, Result};
use crate::hybrid_manifest::{
    HybridManifest, HybridVersion, JOURNAL_COMMIT_SENTINEL_BYTES, JournalIntentInput,
    JournalIntentKind, JournalWaveCommit, journal_intent_record_len,
};
use crate::policy::NamespaceId;
use crate::resources::OverloadReason;

const MAX_GROUP_COMMIT_QUEUE_DEPTH: usize = 65_536;
const MAX_GROUP_COMMIT_BATCH_RECORDS: usize = 4_096;
pub(crate) const MAX_DURABILITY_SYNC_GROUPS: usize = 4;
const REQUEST_ACCOUNTING_BYTES: usize = 256;
// One fixed, non-renewing deadline lets the default 16-way workload converge
// before its durability fence. Compared with the former 200 us window, an
// isolated low-QPS mutation waits at most about 800 us longer.
const GROUP_COMMIT_DELAY: Duration = Duration::from_millis(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct JournalGroupCommitConfig {
    pub(crate) queue_depth: usize,
    /// Total queue ownership budget: conservative fixed storage for every
    /// configured slot plus the complete keys of accepted requests. Bounded
    /// sync-wave scratch is accounted by the enclosing Hybrid memory plan.
    pub(crate) memory_budget_bytes: usize,
    /// Maximum encoded bytes written by one logical group, including its fixed
    /// zero sentinel. The manifest's dirty-slot writes are accounted separately.
    pub(crate) max_batch_bytes: usize,
    pub(crate) max_batch_records: usize,
}

impl JournalGroupCommitConfig {
    pub(crate) fn validate(self) -> Result<()> {
        if !(1..=MAX_GROUP_COMMIT_QUEUE_DEPTH).contains(&self.queue_depth) {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid journal group-commit queue depth must be in 1..={MAX_GROUP_COMMIT_QUEUE_DEPTH}"
            )));
        }
        if !(1..=MAX_GROUP_COMMIT_BATCH_RECORDS).contains(&self.max_batch_records)
            || self.max_batch_records > self.queue_depth
        {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid journal group-commit batch records must be in 1..={} and no greater than queue depth",
                MAX_GROUP_COMMIT_BATCH_RECORDS.min(self.queue_depth)
            )));
        }
        let minimum_memory = self
            .queue_depth
            .checked_mul(REQUEST_ACCOUNTING_BYTES)
            .ok_or_else(|| {
                CacheError::InvalidConfig(
                    "hybrid journal group-commit fixed memory overflow".into(),
                )
            })?;
        if self.memory_budget_bytes < minimum_memory {
            return Err(CacheError::InvalidConfig(format!(
                "hybrid journal group-commit memory budget must be at least {minimum_memory} bytes for the configured queue"
            )));
        }
        if self.max_batch_bytes <= JOURNAL_COMMIT_SENTINEL_BYTES
            || self.max_batch_bytes > self.memory_budget_bytes
        {
            return Err(CacheError::InvalidConfig(
                "hybrid journal group-commit batch bytes must exceed the sentinel and fit the memory budget"
                    .into(),
            ));
        }
        Ok(())
    }
}

/// An accepted intent whose complete key is independent of the submitting
/// request. Construction happens only after queue and byte admission wins.
struct OwnedJournalIntent {
    kind: JournalIntentKind,
    namespace: NamespaceId,
    key_hash: u64,
    bucket_id: Option<u64>,
    key: Box<[u8]>,
    record_bytes: usize,
}

impl OwnedJournalIntent {
    // Retained for the legacy group-commit compatibility tests. The default
    // Hybrid runtime no longer appends per-mutation intents.
    #[allow(dead_code)]
    fn try_clone(input: JournalIntentInput<'_>, record_bytes: usize) -> Result<Self> {
        let mut key = Vec::new();
        key.try_reserve_exact(input.key.len())
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        key.extend_from_slice(input.key);
        Ok(Self {
            kind: input.kind,
            namespace: input.namespace,
            key_hash: input.key_hash,
            bucket_id: input.bucket_id,
            key: key.into_boxed_slice(),
            record_bytes,
        })
    }

    fn borrowed(&self) -> JournalIntentInput<'_> {
        JournalIntentInput {
            kind: self.kind,
            namespace: self.namespace,
            key_hash: self.key_hash,
            bucket_id: self.bucket_id,
            key: &self.key,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct JournalGroupCommitSnapshot {
    pub(crate) queue_capacity: u64,
    pub(crate) memory_capacity_bytes: u64,
    pub(crate) fixed_memory_bytes: u64,
    pub(crate) in_flight: u64,
    pub(crate) in_flight_peak: u64,
    pub(crate) bytes_in_use: u64,
    pub(crate) bytes_peak: u64,
    pub(crate) committed_batches: u64,
    pub(crate) committed_records: u64,
    pub(crate) durability_syncs: u64,
    pub(crate) sync_elapsed_ns_total: u64,
    pub(crate) sync_elapsed_ns_max: u64,
    pub(crate) rejected: u64,
    pub(crate) worker_panics: u64,
    pub(crate) accepting: bool,
}

trait JournalBatchAppender: Send + Sync {
    fn append_wave(
        &self,
        inputs: &[JournalIntentInput<'_>],
        group_lengths: &[usize],
    ) -> Result<JournalWaveCommit>;
}

impl JournalBatchAppender for HybridManifest {
    fn append_wave(
        &self,
        inputs: &[JournalIntentInput<'_>],
        group_lengths: &[usize],
    ) -> Result<JournalWaveCommit> {
        HybridManifest::append_wave(self, inputs, group_lengths)
    }
}

struct Completion {
    value: Mutex<Option<Result<HybridVersion>>>,
    ready: Condvar,
}

impl Completion {
    #[allow(dead_code)]
    fn new() -> Self {
        Self {
            value: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, value: Result<HybridVersion>) {
        let mut current = lock_unpoisoned(&self.value);
        if current.is_none() {
            *current = Some(value);
            self.ready.notify_one();
        }
    }

    #[allow(dead_code)]
    fn wait(&self) -> Result<HybridVersion> {
        let mut value = lock_unpoisoned(&self.value);
        loop {
            if let Some(value) = value.take() {
                return value;
            }
            value = self
                .ready
                .wait(value)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

struct QueuedIntent {
    intent: OwnedJournalIntent,
    slot: usize,
}

type JournalGroup = Vec<QueuedIntent>;
type JournalSyncWave = Vec<JournalGroup>;

struct PendingIntent {
    key_bytes: usize,
    completion: Arc<Completion>,
}

struct QueueState {
    queue: VecDeque<QueuedIntent>,
    pending: Vec<Option<PendingIntent>>,
    free_slots: Vec<usize>,
    in_flight: usize,
    key_bytes_in_use: usize,
    accepting: bool,
    worker_stopped: bool,
    worker_panicked: bool,
}

struct Counters {
    in_flight_peak: AtomicU64,
    bytes_peak: AtomicU64,
    committed_batches: AtomicU64,
    committed_records: AtomicU64,
    durability_syncs: AtomicU64,
    sync_elapsed_ns_total: AtomicU64,
    sync_elapsed_ns_max: AtomicU64,
    rejected: AtomicU64,
    worker_panics: AtomicU64,
}

struct JournalGroupCommitInner {
    appender: Arc<dyn JournalBatchAppender>,
    config: JournalGroupCommitConfig,
    fixed_memory_bytes: usize,
    coalesce_delay: Duration,
    state: Mutex<QueueState>,
    changed: Condvar,
    stopped: Condvar,
    counters: Counters,
}

pub(crate) struct JournalGroupCommit {
    inner: Arc<JournalGroupCommitInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl JournalGroupCommit {
    pub(crate) fn try_new(
        manifest: Arc<HybridManifest>,
        config: JournalGroupCommitConfig,
    ) -> Result<Self> {
        Self::try_new_inner(manifest, config, GROUP_COMMIT_DELAY)
    }

    fn try_new_inner(
        appender: Arc<dyn JournalBatchAppender>,
        config: JournalGroupCommitConfig,
        coalesce_delay: Duration,
    ) -> Result<Self> {
        config.validate()?;
        let fixed_memory_bytes = config
            .queue_depth
            .checked_mul(REQUEST_ACCOUNTING_BYTES)
            .expect("validated group-commit fixed memory");
        let mut queue = VecDeque::new();
        queue
            .try_reserve_exact(config.queue_depth)
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(config.queue_depth)
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        pending.resize_with(config.queue_depth, || None);
        let mut free_slots = Vec::new();
        free_slots
            .try_reserve_exact(config.queue_depth)
            .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
        free_slots.extend(0..config.queue_depth);
        let inner = Arc::new(JournalGroupCommitInner {
            appender,
            config,
            fixed_memory_bytes,
            coalesce_delay,
            state: Mutex::new(QueueState {
                queue,
                pending,
                free_slots,
                in_flight: 0,
                key_bytes_in_use: 0,
                accepting: true,
                worker_stopped: false,
                worker_panicked: false,
            }),
            changed: Condvar::new(),
            stopped: Condvar::new(),
            counters: Counters {
                in_flight_peak: AtomicU64::new(0),
                bytes_peak: AtomicU64::new(fixed_memory_bytes as u64),
                committed_batches: AtomicU64::new(0),
                committed_records: AtomicU64::new(0),
                durability_syncs: AtomicU64::new(0),
                sync_elapsed_ns_total: AtomicU64::new(0),
                sync_elapsed_ns_max: AtomicU64::new(0),
                rejected: AtomicU64::new(0),
                worker_panics: AtomicU64::new(0),
            },
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("cache-rs-hybrid-journal".into())
            .spawn(move || {
                if catch_unwind(AssertUnwindSafe(|| worker_loop(&worker_inner))).is_err() {
                    fail_all_after_panic(&worker_inner);
                }
            })
            .map_err(CacheError::Io)?;
        Ok(Self {
            inner,
            worker: Mutex::new(Some(worker)),
        })
    }

    /// Reserve queue and owned-key bytes before copying the complete input.
    /// Return only after this intent's batch has been durably synced.
    #[allow(dead_code)]
    pub(crate) fn append(&self, input: JournalIntentInput<'_>) -> Result<HybridVersion> {
        let record_bytes = journal_intent_record_len(&input)?;
        let record_with_sentinel = record_bytes
            .checked_add(JOURNAL_COMMIT_SENTINEL_BYTES)
            .ok_or_else(|| {
                CacheError::InvalidConfig("hybrid journal record size overflow".into())
            })?;
        let key_bytes = input.key.len();
        let completion = Arc::new(Completion::new());
        let mut state = lock_unpoisoned(&self.inner.state);
        if state.worker_panicked {
            return Err(CacheError::Poisoned);
        }
        if !state.accepting {
            return Err(CacheError::Closed);
        }
        if record_with_sentinel > self.inner.config.max_batch_bytes
            || key_bytes
                > self
                    .inner
                    .config
                    .memory_budget_bytes
                    .saturating_sub(self.inner.fixed_memory_bytes)
                    .saturating_sub(state.key_bytes_in_use)
        {
            self.inner.counters.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable,
            ));
        }
        if state.in_flight >= self.inner.config.queue_depth {
            self.inner.counters.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(CacheError::Overloaded(OverloadReason::WriteQueueFull));
        }
        state.in_flight += 1;
        state.key_bytes_in_use += key_bytes;
        let slot = state
            .free_slots
            .pop()
            .expect("an admitted journal request has a free completion slot");
        state.pending[slot] = Some(PendingIntent {
            key_bytes,
            completion: Arc::clone(&completion),
        });
        update_peak(&self.inner.counters.in_flight_peak, state.in_flight as u64);
        update_peak(
            &self.inner.counters.bytes_peak,
            self.inner
                .fixed_memory_bytes
                .saturating_add(state.key_bytes_in_use) as u64,
        );
        let intent = match OwnedJournalIntent::try_clone(input, record_bytes) {
            Ok(intent) => intent,
            Err(error) => {
                state.pending[slot] = None;
                state.free_slots.push(slot);
                state.in_flight -= 1;
                state.key_bytes_in_use -= key_bytes;
                self.inner.counters.rejected.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        state.queue.push_back(QueuedIntent { intent, slot });
        self.inner.changed.notify_one();
        drop(state);
        completion.wait()
    }

    pub(crate) fn snapshot(&self) -> JournalGroupCommitSnapshot {
        let state = lock_unpoisoned(&self.inner.state);
        JournalGroupCommitSnapshot {
            queue_capacity: self.inner.config.queue_depth as u64,
            memory_capacity_bytes: self.inner.config.memory_budget_bytes as u64,
            fixed_memory_bytes: self.inner.fixed_memory_bytes as u64,
            in_flight: state.in_flight as u64,
            in_flight_peak: self
                .inner
                .counters
                .in_flight_peak
                .load(Ordering::Relaxed)
                .max(state.in_flight as u64),
            bytes_in_use: self
                .inner
                .fixed_memory_bytes
                .saturating_add(state.key_bytes_in_use) as u64,
            bytes_peak: self.inner.counters.bytes_peak.load(Ordering::Relaxed).max(
                self.inner
                    .fixed_memory_bytes
                    .saturating_add(state.key_bytes_in_use) as u64,
            ),
            committed_batches: self
                .inner
                .counters
                .committed_batches
                .load(Ordering::Relaxed),
            committed_records: self
                .inner
                .counters
                .committed_records
                .load(Ordering::Relaxed),
            durability_syncs: self.inner.counters.durability_syncs.load(Ordering::Relaxed),
            sync_elapsed_ns_total: self
                .inner
                .counters
                .sync_elapsed_ns_total
                .load(Ordering::Relaxed),
            sync_elapsed_ns_max: self
                .inner
                .counters
                .sync_elapsed_ns_max
                .load(Ordering::Relaxed),
            rejected: self.inner.counters.rejected.load(Ordering::Relaxed),
            worker_panics: self.inner.counters.worker_panics.load(Ordering::Relaxed),
            accepting: state.accepting,
        }
    }

    /// Reject new intents, durably drain every accepted intent, and join the
    /// single worker. Concurrent and repeated calls observe the same result.
    pub(crate) fn shutdown(&self) -> Result<()> {
        {
            let mut state = lock_unpoisoned(&self.inner.state);
            state.accepting = false;
            self.inner.changed.notify_all();
        }
        let handle = lock_unpoisoned(&self.worker).take();
        if let Some(handle) = handle {
            if handle.join().is_err() {
                fail_all_after_panic(&self.inner);
            }
        } else {
            let mut state = lock_unpoisoned(&self.inner.state);
            while !state.worker_stopped {
                state = self
                    .inner
                    .stopped
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }
        if lock_unpoisoned(&self.inner.state).worker_panicked {
            Err(CacheError::Poisoned)
        } else {
            Ok(())
        }
    }
}

impl Drop for JournalGroupCommit {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

fn worker_loop(inner: &Arc<JournalGroupCommitInner>) {
    loop {
        let Some(mut wave) = take_sync_wave(inner) else {
            return;
        };
        let record_count = wave_record_count(&wave);
        let outcome = catch_unwind(AssertUnwindSafe(|| commit_sync_wave(inner, &wave)));
        match outcome {
            Ok(Ok(commit)) if commit.versions.len() == record_count => {
                record_wave_commit(inner, wave.len(), record_count, commit.sync_elapsed);
                finish_wave_success(inner, wave, commit.versions);
            }
            Ok(Ok(_)) => {
                fail_all_after_panic(inner);
                return;
            }
            Ok(Err(CacheError::Overloaded(OverloadReason::JournalCapacityFull)))
                if wave.len() > 1 =>
            {
                // The opportunistic wave crossed the journal tail. Retry only
                // its first logical group, exactly matching the old rollover
                // boundary, and keep the untouched suffix ahead of newer work.
                let suffix = wave.split_off(1);
                let first_record_count = wave_record_count(&wave);
                let first_outcome =
                    catch_unwind(AssertUnwindSafe(|| commit_sync_wave(inner, &wave)));
                match first_outcome {
                    Ok(Ok(commit)) if commit.versions.len() == first_record_count => {
                        requeue_wave_front(inner, suffix);
                        record_wave_commit(inner, 1, first_record_count, commit.sync_elapsed);
                        finish_wave_success(inner, wave, commit.versions);
                    }
                    Ok(Ok(_)) => {
                        fail_all_after_panic(inner);
                        return;
                    }
                    Ok(Err(
                        error @ CacheError::Overloaded(OverloadReason::JournalCapacityFull),
                    )) => {
                        requeue_wave_front(inner, suffix);
                        finish_wave_error(inner, wave, &error);
                    }
                    Ok(Err(error)) => {
                        wave.extend(suffix);
                        finish_wave_error(inner, wave, &error);
                    }
                    Err(_) => {
                        fail_all_after_panic(inner);
                        return;
                    }
                }
            }
            Ok(Err(error)) => {
                finish_wave_error(inner, wave, &error);
            }
            Err(_) => {
                fail_all_after_panic(inner);
                return;
            }
        }
    }
}

fn commit_sync_wave(
    inner: &JournalGroupCommitInner,
    wave: &JournalSyncWave,
) -> Result<JournalWaveCommit> {
    let record_count = wave_record_count(wave);
    let mut inputs = Vec::new();
    inputs
        .try_reserve_exact(record_count)
        .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
    let mut group_lengths = Vec::new();
    group_lengths
        .try_reserve_exact(wave.len())
        .map_err(|_| CacheError::Overloaded(OverloadReason::WriteBufferUnavailable))?;
    for group in wave {
        group_lengths.push(group.len());
        inputs.extend(group.iter().map(|request| request.intent.borrowed()));
    }
    inner.appender.append_wave(&inputs, &group_lengths)
}

fn wave_record_count(wave: &JournalSyncWave) -> usize {
    wave.iter().map(Vec::len).sum()
}

fn record_wave_commit(
    inner: &JournalGroupCommitInner,
    groups: usize,
    records: usize,
    sync_elapsed: Duration,
) {
    inner
        .counters
        .committed_batches
        .fetch_add(groups as u64, Ordering::Relaxed);
    inner
        .counters
        .committed_records
        .fetch_add(records as u64, Ordering::Relaxed);
    inner
        .counters
        .durability_syncs
        .fetch_add(1, Ordering::Relaxed);
    let elapsed_ns = u64::try_from(sync_elapsed.as_nanos()).unwrap_or(u64::MAX);
    let _ = inner.counters.sync_elapsed_ns_total.fetch_update(
        Ordering::Relaxed,
        Ordering::Relaxed,
        |current| Some(current.saturating_add(elapsed_ns)),
    );
    update_peak(&inner.counters.sync_elapsed_ns_max, elapsed_ns);
}

fn take_sync_wave(inner: &JournalGroupCommitInner) -> Option<JournalSyncWave> {
    let mut state = lock_unpoisoned(&inner.state);
    loop {
        if !state.queue.is_empty() {
            break;
        }
        if !state.accepting || state.worker_panicked {
            state.worker_stopped = true;
            inner.stopped.notify_all();
            return None;
        }
        state = inner
            .changed
            .wait(state)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
    }

    if state.accepting && inner.coalesce_delay != Duration::ZERO {
        let deadline = Instant::now().checked_add(inner.coalesce_delay);
        let wave_record_limit = inner
            .config
            .max_batch_records
            .saturating_mul(MAX_DURABILITY_SYNC_GROUPS)
            .min(inner.config.queue_depth);
        while state.accepting
            && state.queue.len() < wave_record_limit
            && deadline.is_some_and(|deadline| deadline > Instant::now())
        {
            let remaining = deadline
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .unwrap_or(Duration::ZERO);
            let (next, timed_out) = inner
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = next;
            if timed_out.timed_out() {
                break;
            }
        }
    }

    let mut wave = Vec::with_capacity(MAX_DURABILITY_SYNC_GROUPS);
    while wave.len() < MAX_DURABILITY_SYNC_GROUPS && !state.queue.is_empty() {
        let capacity = state.queue.len().min(inner.config.max_batch_records);
        let mut group = Vec::with_capacity(capacity);
        let mut encoded_bytes = JOURNAL_COMMIT_SENTINEL_BYTES;
        while group.len() < inner.config.max_batch_records {
            let Some(next) = state.queue.front() else {
                break;
            };
            if next.intent.record_bytes > inner.config.max_batch_bytes.saturating_sub(encoded_bytes)
            {
                break;
            }
            encoded_bytes += next.intent.record_bytes;
            group.push(
                state
                    .queue
                    .pop_front()
                    .expect("front intent remains available"),
            );
        }
        if group.is_empty() {
            break;
        }
        wave.push(group);
    }
    debug_assert!(!wave.is_empty());
    Some(wave)
}

fn requeue_wave_front(inner: &JournalGroupCommitInner, wave: JournalSyncWave) {
    let mut state = lock_unpoisoned(&inner.state);
    for group in wave.into_iter().rev() {
        for request in group.into_iter().rev() {
            state.queue.push_front(request);
        }
    }
    inner.changed.notify_one();
}

fn take_pending_completions(
    inner: &JournalGroupCommitInner,
    wave: JournalSyncWave,
) -> Vec<Arc<Completion>> {
    let count = wave_record_count(&wave);
    let mut completions = Vec::with_capacity(count);
    let mut state = lock_unpoisoned(&inner.state);
    let mut bytes = 0_usize;
    for group in &wave {
        for request in group {
            let pending = state.pending[request.slot]
                .take()
                .expect("an executing journal request retains its pending slot");
            bytes = bytes.saturating_add(pending.key_bytes);
            completions.push(pending.completion);
            state.free_slots.push(request.slot);
        }
    }
    // Free every owned key while admission is still serialized, then make its
    // slot and byte charge available to a subsequent caller.
    drop(wave);
    debug_assert!(state.in_flight >= count && state.key_bytes_in_use >= bytes);
    state.in_flight = state.in_flight.saturating_sub(count);
    state.key_bytes_in_use = state.key_bytes_in_use.saturating_sub(bytes);
    if !state.accepting && state.queue.is_empty() && state.in_flight == 0 {
        inner.changed.notify_all();
    }
    completions
}

fn finish_wave_success(
    inner: &JournalGroupCommitInner,
    wave: JournalSyncWave,
    versions: Vec<HybridVersion>,
) {
    let completions = take_pending_completions(inner, wave);
    for (completion, version) in completions.into_iter().zip(versions) {
        completion.complete(Ok(version));
    }
}

fn finish_wave_error(inner: &JournalGroupCommitInner, wave: JournalSyncWave, error: &CacheError) {
    let completions = take_pending_completions(inner, wave);
    for completion in completions {
        completion.complete(Err(clone_cache_error(error)));
    }
}

fn fail_all_after_panic(inner: &JournalGroupCommitInner) {
    let mut state = lock_unpoisoned(&inner.state);
    if !state.worker_panicked {
        inner.counters.worker_panics.fetch_add(1, Ordering::Relaxed);
    }
    state.worker_panicked = true;
    state.accepting = false;
    state.worker_stopped = true;
    state.queue.clear();
    for slot in 0..state.pending.len() {
        if let Some(pending) = state.pending[slot].take() {
            // Keep this failure path allocation-free. Woken callers may
            // briefly wait for the queue lock, but they cannot observe a
            // partially reset admission ledger.
            pending.completion.complete(Err(CacheError::Poisoned));
            state.free_slots.push(slot);
        }
    }
    state.in_flight = 0;
    state.key_bytes_in_use = 0;
    drop(state);
    inner.changed.notify_all();
    inner.stopped.notify_all();
}

fn clone_cache_error(error: &CacheError) -> CacheError {
    match error {
        CacheError::Io(error) => CacheError::Io(match error.raw_os_error() {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(error.kind(), error.to_string()),
        }),
        CacheError::InvalidConfig(message) => CacheError::InvalidConfig(message.clone()),
        CacheError::CorruptMetadata(message) => CacheError::CorruptMetadata(message),
        CacheError::Locked => CacheError::Locked,
        CacheError::Closed => CacheError::Closed,
        CacheError::Poisoned => CacheError::Poisoned,
        CacheError::Cancelled => CacheError::Cancelled,
        CacheError::TimedOut => CacheError::TimedOut,
        CacheError::Overloaded(reason) => CacheError::Overloaded(*reason),
        CacheError::ReclaimBacklog => CacheError::ReclaimBacklog,
    }
}

fn update_peak(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (value > current).then_some(value)
    });
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, mpsc};

    use crate::cache::CacheStatus;
    use crate::hybrid_manifest::DEFAULT_JOURNAL_CAPACITY;
    use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};
    use crate::io_backend::{FileBackend, IoBackend, SyncMode, SyncPoint, WritePoint};

    static TEST_ID: AtomicU64 = AtomicU64::new(1);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(name: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "cache-rs-hybrid-journal-{name}-{}-{}",
                std::process::id(),
                TEST_ID.fetch_add(1, Ordering::Relaxed)
            )))
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn config(queue_depth: usize, memory_budget_bytes: usize) -> JournalGroupCommitConfig {
        JournalGroupCommitConfig {
            queue_depth,
            memory_budget_bytes,
            max_batch_bytes: memory_budget_bytes,
            max_batch_records: queue_depth,
        }
    }

    fn put_region(key: &[u8]) -> JournalIntentInput<'_> {
        JournalIntentInput {
            kind: JournalIntentKind::PutRegion,
            namespace: 1,
            key_hash: key.first().copied().unwrap_or_default() as u64,
            bucket_id: None,
            key,
        }
    }

    fn open_fault_manifest(path: &TestPath) -> (Arc<HybridManifest>, FaultHandle) {
        let (backend, faults) = FaultBackend::open(&path.0).unwrap();
        let (manifest, opened) =
            HybridManifest::open_with_backend(Box::new(backend), 19, DEFAULT_JOURNAL_CAPACITY)
                .unwrap();
        assert!(opened.created);
        (Arc::new(manifest), faults)
    }

    #[derive(Default)]
    struct JournalSyncGateState {
        entered: usize,
        released: bool,
    }

    #[derive(Clone, Default)]
    struct JournalSyncGate {
        shared: Arc<(Mutex<JournalSyncGateState>, Condvar)>,
    }

    impl JournalSyncGate {
        fn block(&self) {
            let (state, changed) = self.shared.as_ref();
            let mut state = lock_unpoisoned(state);
            state.entered += 1;
            changed.notify_all();
            while !state.released {
                state = changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
        }

        fn wait_until_entered(&self, timeout: Duration) -> bool {
            let (state, changed) = self.shared.as_ref();
            let state = lock_unpoisoned(state);
            let (state, _) = changed
                .wait_timeout_while(state, timeout, |state| state.entered == 0)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.entered != 0
        }

        fn release(&self) {
            let (state, changed) = self.shared.as_ref();
            let mut state = lock_unpoisoned(state);
            state.released = true;
            changed.notify_all();
        }

        fn entries(&self) -> usize {
            lock_unpoisoned(&self.shared.0).entered
        }
    }

    struct BlockingJournalSyncBackend {
        inner: FileBackend,
        gate: JournalSyncGate,
    }

    impl BlockingJournalSyncBackend {
        fn open(path: &Path) -> io::Result<(Self, JournalSyncGate)> {
            let gate = JournalSyncGate::default();
            Ok((
                Self {
                    inner: FileBackend::open(path)?,
                    gate: gate.clone(),
                },
                gate,
            ))
        }
    }

    impl IoBackend for BlockingJournalSyncBackend {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }

        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }

        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            self.inner.read_at(buffer, offset)
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            self.inner.write_at(point, buffer, offset)
        }

        fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
            if point == SyncPoint::HybridJournal {
                self.gate.block();
            }
            self.inner.sync(point, mode)
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            self.inner.try_lock_exclusive()
        }

        fn unlock(&self) -> io::Result<()> {
            self.inner.unlock()
        }
    }

    fn runtime_with_delay(
        appender: Arc<dyn JournalBatchAppender>,
        config: JournalGroupCommitConfig,
        delay: Duration,
    ) -> Arc<JournalGroupCommit> {
        Arc::new(JournalGroupCommit::try_new_inner(appender, config, delay).unwrap())
    }

    fn submit_together(
        runtime: &Arc<JournalGroupCommit>,
        count: usize,
    ) -> Vec<Result<HybridVersion>> {
        let barrier = Arc::new(Barrier::new(count + 1));
        let mut workers = Vec::new();
        for index in 0..count {
            let runtime = Arc::clone(runtime);
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                let key = vec![index as u8; 8];
                barrier.wait();
                runtime.append(put_region(&key))
            }));
        }
        barrier.wait();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect()
    }

    #[test]
    fn concurrent_intents_share_one_real_journal_sync_and_adjacent_versions() {
        let path = TestPath::new("single-sync");
        let (manifest, faults) = open_fault_manifest(&path);
        let appender: Arc<dyn JournalBatchAppender> = manifest.clone();
        let runtime =
            runtime_with_delay(appender, config(8, 64 * 1024), Duration::from_millis(250));

        let mut versions = submit_together(&runtime, 8)
            .into_iter()
            .map(Result::unwrap)
            .collect::<Vec<_>>();
        versions.sort_unstable();
        assert!(
            versions.windows(2).all(|pair| {
                pair[0].epoch == pair[1].epoch && pair[0].seqno + 1 == pair[1].seqno
            })
        );
        let events = faults.events();
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == FaultEvent::Sync(SyncPoint::HybridJournal))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == FaultEvent::Write(WritePoint::HybridJournal))
                .count(),
            2,
            "one record-vector write plus one zero-sentinel write"
        );
        assert_eq!(runtime.snapshot().committed_batches, 1);
        runtime.shutdown().unwrap();
        assert_eq!(manifest.status(), CacheStatus::Healthy);
    }

    #[test]
    fn two_logical_groups_share_one_sync_and_never_complete_before_it() {
        const REQUESTS: usize = 4;

        let path = TestPath::new("cross-group-sync-wave");
        let (backend, gate) = BlockingJournalSyncBackend::open(&path.0).unwrap();
        let (manifest, opened) =
            HybridManifest::open_with_backend(Box::new(backend), 23, DEFAULT_JOURNAL_CAPACITY)
                .unwrap();
        assert!(opened.created);
        let manifest = Arc::new(manifest);
        let appender: Arc<dyn JournalBatchAppender> = manifest.clone();
        let runtime = runtime_with_delay(
            appender,
            JournalGroupCommitConfig {
                max_batch_records: 2,
                ..config(REQUESTS, 64 * 1024)
            },
            Duration::from_millis(250),
        );

        let start = Arc::new(Barrier::new(REQUESTS + 1));
        let (result_tx, result_rx) = mpsc::channel();
        let mut workers = Vec::with_capacity(REQUESTS);
        for index in 0..REQUESTS {
            let runtime = Arc::clone(&runtime);
            let start = Arc::clone(&start);
            let result_tx = result_tx.clone();
            workers.push(thread::spawn(move || {
                let key = vec![index as u8; 8];
                start.wait();
                result_tx.send(runtime.append(put_region(&key))).unwrap();
            }));
        }
        drop(result_tx);
        start.wait();

        let sync_entered = gate.wait_until_entered(Duration::from_secs(5));
        let early = result_rx.recv_timeout(Duration::from_millis(30)).ok();
        let completed_before_sync = early.is_some();
        gate.release();

        let mut results = Vec::with_capacity(REQUESTS);
        if let Some(result) = early {
            results.push(result);
        }
        while results.len() < REQUESTS {
            results.push(result_rx.recv_timeout(Duration::from_secs(5)).unwrap());
        }
        for worker in workers {
            worker.join().unwrap();
        }

        assert!(sync_entered, "the durability sync was never reached");
        assert!(
            !completed_before_sync,
            "a logical group completed before the shared durability sync"
        );
        let mut versions = results.into_iter().map(Result::unwrap).collect::<Vec<_>>();
        versions.sort_unstable();
        assert!(
            versions.windows(2).all(|pair| {
                pair[0].epoch == pair[1].epoch && pair[0].seqno + 1 == pair[1].seqno
            })
        );
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.committed_batches, 2);
        assert_eq!(snapshot.committed_records, REQUESTS as u64);
        assert_eq!(snapshot.durability_syncs, 1);
        assert_eq!(gate.entries(), 1);
        assert!(snapshot.sync_elapsed_ns_total >= snapshot.sync_elapsed_ns_max);
        assert!(snapshot.sync_elapsed_ns_max > 0);
        runtime.shutdown().unwrap();
        manifest.close().unwrap();
    }

    #[test]
    fn sync_failure_is_fanned_out_exactly_and_poison_manifest() {
        let path = TestPath::new("sync-error");
        let (manifest, faults) = open_fault_manifest(&path);
        faults.arm(
            FaultEvent::Sync(SyncPoint::HybridJournal),
            1,
            FaultAction::Error(28),
        );
        let appender: Arc<dyn JournalBatchAppender> = manifest.clone();
        let runtime = runtime_with_delay(
            appender,
            JournalGroupCommitConfig {
                max_batch_records: 2,
                ..config(6, 64 * 1024)
            },
            Duration::from_millis(250),
        );

        for result in submit_together(&runtime, 6) {
            match result {
                Err(CacheError::Io(error)) => assert_eq!(error.raw_os_error(), Some(28)),
                other => panic!("expected the journal sync ENOSPC, got {other:?}"),
            }
        }
        assert_eq!(manifest.status(), CacheStatus::Poisoned);
        assert_eq!(runtime.snapshot().committed_batches, 0);
        assert_eq!(runtime.snapshot().durability_syncs, 0);
        runtime.shutdown().unwrap();
    }

    struct BlockingAppender {
        started: AtomicUsize,
        state: Mutex<bool>,
        changed: Condvar,
        next_seqno: AtomicU64,
    }

    impl BlockingAppender {
        fn new() -> Self {
            Self {
                started: AtomicUsize::new(0),
                state: Mutex::new(false),
                changed: Condvar::new(),
                next_seqno: AtomicU64::new(1),
            }
        }

        fn wait_started(&self) {
            while self.started.load(Ordering::Acquire) == 0 {
                thread::yield_now();
            }
        }

        fn release(&self) {
            *lock_unpoisoned(&self.state) = true;
            self.changed.notify_all();
        }
    }

    impl JournalBatchAppender for BlockingAppender {
        fn append_wave(
            &self,
            inputs: &[JournalIntentInput<'_>],
            _group_lengths: &[usize],
        ) -> Result<JournalWaveCommit> {
            self.started.fetch_add(1, Ordering::Release);
            let mut released = lock_unpoisoned(&self.state);
            while !*released {
                released = self
                    .changed
                    .wait(released)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            let first = self
                .next_seqno
                .fetch_add(inputs.len() as u64, Ordering::Relaxed);
            Ok(JournalWaveCommit {
                versions: (0..inputs.len())
                    .map(|offset| HybridVersion {
                        epoch: 1,
                        seqno: first + offset as u64,
                    })
                    .collect(),
                sync_elapsed: Duration::ZERO,
            })
        }
    }

    fn wait_for_in_flight(runtime: &JournalGroupCommit, expected: u64) {
        let deadline = Instant::now() + Duration::from_secs(2);
        while runtime.snapshot().in_flight != expected {
            assert!(
                Instant::now() < deadline,
                "journal requests did not enqueue"
            );
            thread::yield_now();
        }
    }

    #[test]
    fn queue_and_memory_are_hard_bounded_and_shutdown_drains_once() {
        let appender = Arc::new(BlockingAppender::new());
        let runtime = runtime_with_delay(appender.clone(), config(2, 1024), Duration::ZERO);
        let first_runtime = Arc::clone(&runtime);
        let first = thread::spawn(move || first_runtime.append(put_region(b"a")));
        appender.wait_started();
        let second_runtime = Arc::clone(&runtime);
        let second = thread::spawn(move || second_runtime.append(put_region(b"b")));
        wait_for_in_flight(&runtime, 2);
        assert!(matches!(
            runtime.append(put_region(b"c")),
            Err(CacheError::Overloaded(OverloadReason::WriteQueueFull))
        ));

        let shutdown_runtime = Arc::clone(&runtime);
        let shutdown = thread::spawn(move || shutdown_runtime.shutdown());
        while runtime.snapshot().accepting {
            thread::yield_now();
        }
        assert!(matches!(
            runtime.append(put_region(b"d")),
            Err(CacheError::Closed)
        ));
        appender.release();
        first.join().unwrap().unwrap();
        second.join().unwrap().unwrap();
        shutdown.join().unwrap().unwrap();
        runtime.shutdown().unwrap();
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.bytes_in_use, snapshot.fixed_memory_bytes);
        assert_eq!(snapshot.in_flight_peak, 2);
        assert!(snapshot.bytes_peak <= snapshot.memory_capacity_bytes);

        let appender = Arc::new(BlockingAppender::new());
        let fixed_queue_budget = REQUEST_ACCOUNTING_BYTES * 2;
        let one_intent_bytes = fixed_queue_budget + 1;
        let runtime = runtime_with_delay(
            appender.clone(),
            config(2, one_intent_bytes),
            Duration::ZERO,
        );
        let first_runtime = Arc::clone(&runtime);
        let first = thread::spawn(move || first_runtime.append(put_region(b"a")));
        appender.wait_started();
        assert!(matches!(
            runtime.append(put_region(b"b")),
            Err(CacheError::Overloaded(
                OverloadReason::WriteBufferUnavailable
            ))
        ));
        assert_eq!(runtime.snapshot().bytes_peak, one_intent_bytes as u64);
        appender.release();
        first.join().unwrap().unwrap();
        runtime.shutdown().unwrap();

        assert!(matches!(
            config(2, fixed_queue_budget - 1).validate(),
            Err(CacheError::InvalidConfig(_))
        ));
    }

    struct PanicAppender;

    impl JournalBatchAppender for PanicAppender {
        fn append_wave(
            &self,
            _inputs: &[JournalIntentInput<'_>],
            _group_lengths: &[usize],
        ) -> Result<JournalWaveCommit> {
            panic!("injected journal worker panic");
        }
    }

    #[test]
    fn worker_panic_completes_every_waiter_and_latches_poisoned() {
        let runtime = runtime_with_delay(
            Arc::new(PanicAppender),
            config(4, 16 * 1024),
            Duration::from_millis(250),
        );
        for result in submit_together(&runtime, 4) {
            assert!(matches!(result, Err(CacheError::Poisoned)));
        }
        assert!(matches!(runtime.shutdown(), Err(CacheError::Poisoned)));
        assert!(matches!(
            runtime.append(put_region(b"later")),
            Err(CacheError::Poisoned)
        ));
        assert_eq!(runtime.snapshot().worker_panics, 1);
    }
}
