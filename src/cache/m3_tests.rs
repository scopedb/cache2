use super::*;

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Condvar, Mutex};
use std::thread::ThreadId;
use std::time::{Duration, Instant};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new(name: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "cache-rs-m3-{name}-{}-{nonce}.cache",
            std::process::id()
        )))
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Default)]
struct GateState {
    armed: bool,
    released: bool,
    entered: usize,
    active: usize,
    max_active: usize,
}

#[derive(Clone, Default)]
struct ReadGate {
    shared: Arc<(Mutex<GateState>, Condvar)>,
}

impl ReadGate {
    fn arm(&self) {
        let (state, _) = &*self.shared;
        let mut state = state.lock().unwrap();
        *state = GateState {
            armed: true,
            ..GateState::default()
        };
    }

    fn wait_for_entries(&self, target: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        while state.entered < target {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if wait.timed_out() && state.entered < target {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        state.released = true;
        changed.notify_all();
    }

    fn max_active(&self) -> usize {
        let (state, _) = &*self.shared;
        state.lock().unwrap().max_active
    }

    fn enter_read(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        if !state.armed || state.released {
            return;
        }

        state.entered += 1;
        state.active += 1;
        state.max_active = state.max_active.max(state.active);
        changed.notify_all();
        while !state.released {
            state = changed.wait(state).unwrap();
        }
        state.active -= 1;
    }
}

struct GateBackend {
    inner: FileBackend,
    reads: ReadGate,
}

impl GateBackend {
    fn open(path: &Path) -> io::Result<(Self, ReadGate)> {
        let reads = ReadGate::default();
        Ok((
            Self {
                inner: FileBackend::open(path)?,
                reads: reads.clone(),
            },
            reads,
        ))
    }
}

impl IoBackend for GateBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.reads.enter_read();
        self.inner.read_at(buffer, offset)
    }

    fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
        self.inner.write_at(point, buffer, offset)
    }

    fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
        self.inner.sync(point, mode)
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        self.inner.try_lock_exclusive()
    }

    fn unlock(&self) -> io::Result<()> {
        self.inner.unlock()
    }
}

#[derive(Default)]
struct RecordState {
    armed: bool,
    released: bool,
    entered: usize,
    threads: Vec<ThreadId>,
}

#[derive(Clone, Default)]
struct RecordWrites {
    shared: Arc<(Mutex<RecordState>, Condvar)>,
}

impl RecordWrites {
    fn arm(&self) {
        let (state, _) = &*self.shared;
        let mut state = state.lock().unwrap();
        *state = RecordState {
            armed: true,
            ..RecordState::default()
        };
    }

    fn observe(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        state.entered += 1;
        state.threads.push(std::thread::current().id());
        changed.notify_all();
        while state.armed && !state.released {
            state = changed.wait(state).unwrap();
        }
    }

    fn wait_for_entries(&self, target: usize, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        while state.entered < target {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if wait.timed_out() && state.entered < target {
                return false;
            }
        }
        true
    }

    fn release(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        state.released = true;
        changed.notify_all();
    }

    fn threads(&self) -> Vec<ThreadId> {
        let (state, _) = &*self.shared;
        state.lock().unwrap().threads.clone()
    }
}

struct RecordBackend {
    inner: FileBackend,
    records: RecordWrites,
}

struct PanicSyncBackend {
    inner: FileBackend,
    panic_on_checkpoint: Arc<AtomicBool>,
}

impl PanicSyncBackend {
    fn open(path: &Path) -> io::Result<(Self, Arc<AtomicBool>)> {
        let panic_on_checkpoint = Arc::new(AtomicBool::new(false));
        Ok((
            Self {
                inner: FileBackend::open(path)?,
                panic_on_checkpoint: Arc::clone(&panic_on_checkpoint),
            },
            panic_on_checkpoint,
        ))
    }
}

impl IoBackend for PanicSyncBackend {
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
        if point == SyncPoint::CheckpointData
            && self.panic_on_checkpoint.swap(false, Ordering::SeqCst)
        {
            panic!("test append worker panic during checkpoint");
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

#[derive(Clone, Default)]
struct ScheduleLog {
    shared: Arc<(Mutex<Vec<SchedulePoint>>, Condvar)>,
}

impl ScheduleLog {
    fn record(&self, point: SchedulePoint) {
        let (events, changed) = &*self.shared;
        events.lock().unwrap().push(point);
        changed.notify_all();
    }

    fn wait_for(&self, point: SchedulePoint, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        let (events, changed) = &*self.shared;
        let mut events = events.lock().unwrap();
        while !events.contains(&point) {
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next, wait) = changed.wait_timeout(events, remaining).unwrap();
            events = next;
            if wait.timed_out() && !events.contains(&point) {
                return false;
            }
        }
        true
    }

    fn events(&self) -> Vec<SchedulePoint> {
        let (events, _) = &*self.shared;
        events.lock().unwrap().clone()
    }
}

impl RecordBackend {
    fn open(path: &Path) -> io::Result<(Self, RecordWrites)> {
        let records = RecordWrites::default();
        Ok((
            Self {
                inner: FileBackend::open(path)?,
                records: records.clone(),
            },
            records,
        ))
    }
}

impl IoBackend for RecordBackend {
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
        if point == WritePoint::Record {
            self.records.observe();
        }
        self.inner.write_at(point, buffer, offset)
    }

    fn sync(&self, point: SyncPoint, mode: SyncMode) -> io::Result<()> {
        self.inner.sync(point, mode)
    }

    fn try_lock_exclusive(&self) -> io::Result<()> {
        self.inner.try_lock_exclusive()
    }

    fn unlock(&self) -> io::Result<()> {
        self.inner.unlock()
    }
}

fn config(path: &Path) -> CacheConfig {
    CacheConfig::new(path, DATA_OFFSET + 2 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(1024)
        .with_submission_queue_depths(2, 2)
}

fn multi_lane_config(path: &Path) -> CacheConfig {
    CacheConfig::new(path, DATA_OFFSET + 3 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(1024)
        .with_submission_queue_depths(2, 2)
        .with_append_lanes(2)
}

fn fixed_key(id: u64) -> Vec<u8> {
    let mut key = vec![b'k'; 32];
    key[..8].copy_from_slice(&id.to_le_bytes());
    key
}

#[test]
fn public_gets_sharing_an_ordering_stripe_enter_positioned_reads_concurrently() {
    let file = TestFile::new("parallel-gets");
    let config = config(&file.0);
    let first_key = b"first-key".to_vec();
    let stripe = hash_key(config.hash_seed, &first_key) as usize & (KEY_ORDERING_SHARDS - 1);
    let second_key = (0_u64..10_000)
        .map(|id| format!("same-stripe-{id}").into_bytes())
        .find(|key| {
            key != &first_key
                && hash_key(config.hash_seed, key) as usize & (KEY_ORDERING_SHARDS - 1) == stripe
        })
        .expect("a second key in the same ordering stripe");

    let seed = config.clone().open().unwrap();
    seed.put(&first_key, b"first-value", PutOptions::default())
        .unwrap();
    seed.put(&second_key, b"second-value", PutOptions::default())
        .unwrap();
    seed.flush().unwrap();
    seed.close().unwrap();

    let (backend, reads) = GateBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
    // Recovery uses the same positioned-read path, so arm only after open has
    // rebuilt the in-memory index.
    reads.arm();

    let first_cache = cache.clone();
    let first = std::thread::spawn(move || first_cache.get(&first_key));
    let second_cache = cache.clone();
    let second = std::thread::spawn(move || second_cache.get(&second_key));

    let both_entered = reads.wait_for_entries(2, Duration::from_secs(5));
    // Always release before asserting or joining: a failed concurrency
    // regression must not strand the worker threads behind the test gate.
    reads.release();
    let first_result = first.join().unwrap();
    let second_result = second.join().unwrap();

    assert!(
        both_entered,
        "both gets did not reach read_at before watchdog"
    );
    assert_eq!(reads.max_active(), 2);
    assert_eq!(first_result.unwrap(), Some(b"first-value".to_vec()));
    assert_eq!(second_result.unwrap(), Some(b"second-value".to_vec()));
    cache.close().unwrap();
}

#[test]
fn different_append_lanes_overlap_record_io_and_preserve_values() {
    const PUTS: usize = 2;

    let file = TestFile::new("append-worker-thread");
    let config = multi_lane_config(&file.0);
    let stripes = (0..PUTS)
        .map(|index| {
            let key = format!("worker-key-{index}");
            hash_key(config.hash_seed, key.as_bytes()) as usize & (KEY_ORDERING_SHARDS - 1)
        })
        .collect::<HashSet<_>>();
    assert_eq!(stripes.len(), PUTS, "test keys must use distinct locks");
    let lanes = (0..PUTS)
        .map(|index| {
            let key = format!("worker-key-{index}");
            hash_key(config.hash_seed, key.as_bytes()) as usize % config.append_lanes
        })
        .collect::<HashSet<_>>();
    assert_eq!(lanes.len(), PUTS, "test keys must use distinct lanes");

    let (backend, records) = RecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
    records.arm();
    let start = Arc::new(Barrier::new(PUTS + 1));
    let mut callers = Vec::with_capacity(PUTS);
    for index in 0..PUTS {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        callers.push(std::thread::spawn(move || {
            let caller = std::thread::current().id();
            let key = format!("worker-key-{index}");
            let value = format!("worker-value-{index}");
            start.wait();
            let outcome = cache.put(key, value, PutOptions::default());
            (caller, outcome)
        }));
    }
    start.wait();

    let both_entered = records.wait_for_entries(PUTS, Duration::from_secs(5));
    // Always release before assertions or joins so a serialization regression
    // cannot strand the first engine worker behind the test gate.
    records.release();

    let mut caller_threads = Vec::with_capacity(PUTS);
    for caller in callers {
        let (thread, outcome) = caller.join().unwrap();
        caller_threads.push(thread);
        assert_eq!(outcome.unwrap(), PutOutcome::Stored);
    }

    assert!(
        both_entered,
        "different append lanes did not overlap positioned record I/O"
    );
    let record_threads = records.threads();
    assert_eq!(record_threads.len(), PUTS);
    let engine_threads = record_threads.iter().copied().collect::<HashSet<_>>();
    assert!(
        engine_threads.len() <= 4,
        "sync engine worker pool is bounded"
    );
    assert!(
        record_threads
            .iter()
            .all(|thread| !caller_threads.contains(thread)),
        "positioned I/O must not run on caller threads"
    );
    for index in 0..PUTS {
        let key = format!("worker-key-{index}");
        let value = format!("worker-value-{index}").into_bytes();
        assert_eq!(cache.get(key.as_bytes()).unwrap(), Some(value));
    }
    cache.close().unwrap();
}

#[test]
fn different_append_lanes_remove_concurrently_and_recover_tombstones() {
    const REMOVES: usize = 2;

    let file = TestFile::new("parallel-remove-lanes");
    let config = multi_lane_config(&file.0);
    let keys: Vec<Vec<u8>> = (0_u64..10_000).map(fixed_key).fold(
        Vec::<Vec<u8>>::with_capacity(REMOVES),
        |mut keys, key| {
            let hash = hash_key(config.hash_seed, &key) as usize;
            let stripe = hash & (KEY_ORDERING_SHARDS - 1);
            let lane = hash % config.append_lanes;
            if keys.iter().all(|existing| {
                let existing_hash = hash_key(config.hash_seed, existing) as usize;
                existing_hash & (KEY_ORDERING_SHARDS - 1) != stripe
                    && existing_hash % config.append_lanes != lane
            }) {
                keys.push(key);
            }
            keys
        },
    );
    assert_eq!(keys.len(), REMOVES, "test must find one key per lane");

    let (backend, records) = RecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config.clone(), Box::new(backend)).unwrap();
    for (index, key) in keys.iter().enumerate() {
        assert_eq!(
            cache
                .put(key, format!("remove-value-{index}"), PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
    }
    records.arm();

    let start = Arc::new(Barrier::new(REMOVES + 1));
    let callers = keys
        .iter()
        .cloned()
        .map(|key| {
            let cache = cache.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                cache.remove(&key)
            })
        })
        .collect::<Vec<_>>();
    start.wait();

    let both_entered = records.wait_for_entries(REMOVES, Duration::from_secs(5));
    records.release();
    for caller in callers {
        assert_eq!(caller.join().unwrap().unwrap(), RemoveOutcome::Removed);
    }
    assert!(
        both_entered,
        "different append lanes did not overlap tombstone record I/O"
    );
    assert_eq!(cache.stats().control_queue_depth_peak, REMOVES as u64);
    for key in &keys {
        assert_eq!(cache.get(key).unwrap(), None);
    }
    cache.close().unwrap();

    let reopened = config.open().unwrap();
    for key in &keys {
        assert_eq!(reopened.get(key).unwrap(), None);
    }
    reopened.close().unwrap();
}

#[test]
fn one_append_lane_coalesces_queued_small_records_and_recovers_them() {
    const PUTS: usize = 6;

    let file = TestFile::new("coalesced-small-records");
    let config = config(&file.0).with_submission_queue_depths(PUTS, PUTS);
    let (backend, records) = RecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config.clone(), Box::new(backend)).unwrap();
    records.arm();

    let start = Arc::new(Barrier::new(PUTS + 1));
    let mut callers = Vec::with_capacity(PUTS);
    for index in 0..PUTS {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        callers.push(std::thread::spawn(move || {
            let key = format!("batch-key-{index}");
            let value = format!("batch-value-{index}");
            start.wait();
            cache.put(key, value, PutOptions::default())
        }));
    }
    start.wait();

    let first_write_entered = records.wait_for_entries(1, Duration::from_secs(5));
    let deadline = Instant::now() + Duration::from_secs(5);
    let all_admitted = loop {
        if cache.stats().write_queue_depth == PUTS as u64 {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    // Always release the backend before joining, including on a regression.
    records.release();

    for caller in callers {
        assert_eq!(caller.join().unwrap().unwrap(), PutOutcome::Stored);
    }
    assert!(
        first_write_entered,
        "no record batch reached positioned I/O"
    );
    assert!(
        all_admitted,
        "not every put reached the bounded append path"
    );
    let stats = cache.stats();
    assert!(stats.write_batches < PUTS as u64);
    assert_eq!(stats.records_coalesced, PUTS as u64 - stats.write_batches);
    for index in 0..PUTS {
        assert_eq!(
            cache.get(format!("batch-key-{index}").as_bytes()).unwrap(),
            Some(format!("batch-value-{index}").into_bytes())
        );
    }
    cache.close().unwrap();

    let reopened = config.clone().open().unwrap();
    for index in 0..PUTS {
        assert_eq!(
            reopened
                .get(format!("batch-key-{index}").as_bytes())
                .unwrap(),
            Some(format!("batch-value-{index}").into_bytes())
        );
    }
    reopened.close().unwrap();
}

#[test]
fn buffered_append_window_coalesces_staggered_small_records() {
    let file = TestFile::new("staggered-coalescing");
    let config = config(&file.0);
    let first_key = b"staggered-first".to_vec();
    let first_shard = hash_key(config.hash_seed, &first_key) as usize & (KEY_ORDERING_SHARDS - 1);
    let second_key = (0..KEY_ORDERING_SHARDS)
        .map(|index| format!("staggered-second-{index}").into_bytes())
        .find(|key| {
            hash_key(config.hash_seed, key) as usize & (KEY_ORDERING_SHARDS - 1) != first_shard
        })
        .expect("a distinct key-ordering shard");
    let cache = config.open().unwrap();
    cache.set_append_coalesce_delay_for_test(Duration::from_millis(250));

    let schedule = ScheduleLog::default();
    let observed = schedule.clone();
    *cache.inner.schedule_observer.lock().unwrap() =
        Some(Arc::new(move |point| observed.record(point)));

    let first_cache = cache.clone();
    let submitted_first_key = first_key.clone();
    let first = std::thread::spawn(move || {
        first_cache.put(submitted_first_key, b"first", PutOptions::default())
    });
    let first_was_dequeued =
        schedule.wait_for(SchedulePoint::AppendCoalesceWaiting, Duration::from_secs(5));

    let second_cache = cache.clone();
    let submitted_second_key = second_key.clone();
    let second = std::thread::spawn(move || {
        second_cache.put(submitted_second_key, b"second", PutOptions::default())
    });

    assert_eq!(first.join().unwrap().unwrap(), PutOutcome::Stored);
    assert_eq!(second.join().unwrap().unwrap(), PutOutcome::Stored);
    assert!(
        first_was_dequeued,
        "the first put never reached the empty buffered coalescing window"
    );
    let stats = cache.stats();
    assert_eq!(stats.write_batches, 1);
    assert_eq!(stats.records_coalesced, 1);
    assert_eq!(cache.get(&first_key).unwrap(), Some(b"first".to_vec()));
    assert_eq!(cache.get(&second_key).unwrap(), Some(b"second".to_vec()));
    cache.close().unwrap();
}

#[test]
fn buffered_append_window_skips_wait_for_a_full_size_record() {
    let file = TestFile::new("full-size-record-no-coalesce-wait");
    let region_size = 512 * 1024;
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 2 * region_size)
        .with_region_size(region_size)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(MAX_BATCH_BYTES)
        .with_io_mode(IoMode::Buffered);
    let cache = config.open().unwrap();
    cache.set_append_coalesce_delay_for_test(Duration::from_millis(250));

    let schedule = ScheduleLog::default();
    let observed = schedule.clone();
    *cache.inner.schedule_observer.lock().unwrap() =
        Some(Arc::new(move |point| observed.record(point)));

    let key = b"large";
    let value_len = MAX_BATCH_BYTES - RECORD_HEADER_SIZE - key.len();
    assert_eq!(
        RecordHeader::aligned_len(key.len(), value_len),
        Some(MAX_BATCH_BYTES as u32)
    );
    let value = vec![b'x'; value_len];
    assert_eq!(
        cache.put(key, &value, PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );
    assert!(
        !schedule
            .events()
            .contains(&SchedulePoint::AppendCoalesceWaiting),
        "a record at the batch byte limit waited for an impossible follower"
    );
    assert_eq!(cache.get(key).unwrap(), Some(value));
    cache.close().unwrap();
}

#[test]
fn buffered_and_auto_direct_modes_share_format_v1_records() {
    let file = TestFile::new("buffered-direct-compatibility");
    let buffered = config(&file.0).with_io_mode(IoMode::Buffered);
    let cache = buffered.clone().open().unwrap();
    cache
        .put(b"legacy", b"buffered-value", PutOptions::default())
        .unwrap();
    cache.close().unwrap();

    let auto = config(&file.0).with_io_mode(IoMode::Auto);
    let cache = auto.open().unwrap();
    assert_eq!(
        cache.get(b"legacy").unwrap(),
        Some(b"buffered-value".to_vec())
    );
    // The first append realigns the older 32-byte cursor via the buffered
    // compatibility path; the next append is eligible for O_DIRECT.
    cache
        .put(b"bridge", b"auto-value-1", PutOptions::default())
        .unwrap();
    cache
        .put(b"aligned", b"auto-value-2", PutOptions::default())
        .unwrap();
    let stats = cache.stats();
    if stats.direct_io_active {
        assert!(stats.direct_io_operations > 0);
        assert!(stats.buffered_io_operations > 0);
    }
    cache.close().unwrap();

    let reopened = buffered.open().unwrap();
    assert_eq!(
        reopened.get(b"legacy").unwrap(),
        Some(b"buffered-value".to_vec())
    );
    assert_eq!(
        reopened.get(b"bridge").unwrap(),
        Some(b"auto-value-1".to_vec())
    );
    assert_eq!(
        reopened.get(b"aligned").unwrap(),
        Some(b"auto-value-2".to_vec())
    );
    reopened.close().unwrap();
}

#[test]
fn multi_lane_flush_clear_close_and_reopen_preserve_format_v1_semantics() {
    let file = TestFile::new("multi-lane-checkpoint");
    let config = multi_lane_config(&file.0);
    let first_key = b"worker-key-0";
    let second_key = b"worker-key-1";
    let old_only_key = b"old-generation-only";
    assert_ne!(
        hash_key(config.hash_seed, first_key) as usize % config.append_lanes,
        hash_key(config.hash_seed, second_key) as usize % config.append_lanes,
        "test keys must use distinct append lanes"
    );

    let cache = config.clone().open().unwrap();
    {
        let state = cache.inner.state.lock().unwrap();
        assert_eq!(state.active_regions.len(), 2);
        assert_ne!(state.active_regions[0], state.active_regions[1]);
    }
    cache
        .put(first_key, b"first-generation-0", PutOptions::default())
        .unwrap();
    cache
        .put(second_key, b"first-generation-1", PutOptions::default())
        .unwrap();
    cache
        .put(old_only_key, b"removed-by-clear", PutOptions::default())
        .unwrap();
    cache.flush().unwrap();
    cache.close().unwrap();

    let cache = config.clone().open().unwrap();
    assert_eq!(
        cache.get(first_key).unwrap(),
        Some(b"first-generation-0".to_vec())
    );
    assert_eq!(
        cache.get(second_key).unwrap(),
        Some(b"first-generation-1".to_vec())
    );
    cache.clear().unwrap();
    assert_eq!(cache.get(first_key).unwrap(), None);
    assert_eq!(cache.get(second_key).unwrap(), None);
    assert_eq!(cache.get(old_only_key).unwrap(), None);
    cache
        .put(first_key, b"second-generation-0", PutOptions::default())
        .unwrap();
    cache
        .put(second_key, b"second-generation-1", PutOptions::default())
        .unwrap();
    cache.close().unwrap();
    cache.close().unwrap();

    // Lane count is part of the clean-reopen contract even though the record
    // and region bytes remain Format V1 compatible.
    let one_lane = CacheConfig::new(&file.0, DATA_OFFSET + 3 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(1024)
        .with_submission_queue_depths(2, 2);
    assert!(matches!(one_lane.open(), Err(CacheError::InvalidConfig(_))));

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(old_only_key).unwrap(), None);
    assert_eq!(
        reopened.get(first_key).unwrap(),
        Some(b"second-generation-0".to_vec())
    );
    assert_eq!(
        reopened.get(second_key).unwrap(),
        Some(b"second-generation-1".to_vec())
    );
    reopened.close().unwrap();
}

#[test]
fn multi_lane_rotation_never_reclaims_another_active_region() {
    let file = TestFile::new("multi-lane-rotation");
    let config = multi_lane_config(&file.0);
    let keys = [b"worker-key-0".as_slice(), b"worker-key-1".as_slice()];
    assert_ne!(
        hash_key(config.hash_seed, keys[0]) as usize % config.append_lanes,
        hash_key(config.hash_seed, keys[1]) as usize % config.append_lanes
    );

    let cache = config.clone().open().unwrap();
    let mut expected = Vec::new();
    for key in keys {
        let lane = hash_key(config.hash_seed, key) as usize % config.append_lanes;
        let initial = cache.inner.state.lock().unwrap().active_regions[lane];
        let mut rotated = false;
        for generation in 0_u8..64 {
            let mut value = vec![generation; 900];
            value[..8].copy_from_slice(&(u64::from(generation)).to_le_bytes());
            assert_eq!(
                cache.put(key, &value, PutOptions::default()).unwrap(),
                PutOutcome::Stored
            );
            expected.push((key.to_vec(), value));
            if cache.inner.state.lock().unwrap().active_regions[lane] != initial {
                rotated = true;
                break;
            }
        }
        assert!(rotated, "append lane did not rotate before watchdog");
    }

    {
        let state = cache.inner.state.lock().unwrap();
        assert_ne!(state.active_regions[0], state.active_regions[1]);
        for &region_id in &state.active_regions {
            assert_eq!(
                state.regions[region_id as usize].header.state,
                RegionState::Active
            );
        }
        assert_eq!(state.stats.regions_reused, 1);
    }
    for key in keys {
        let value = expected
            .iter()
            .rev()
            .find(|(candidate, _)| candidate.as_slice() == key)
            .unwrap()
            .1
            .clone();
        assert_eq!(cache.get(key).unwrap(), Some(value));
    }
    cache.close().unwrap();

    let reopened = config.open().unwrap();
    for key in keys {
        let value = expected
            .iter()
            .rev()
            .find(|(candidate, _)| candidate.as_slice() == key)
            .unwrap()
            .1
            .clone();
        assert_eq!(reopened.get(key).unwrap(), Some(value));
    }
    reopened.close().unwrap();
}

#[test]
fn multi_lane_reopen_preserves_lane_for_a_newer_active_region_and_its_tombstone() {
    let file = TestFile::new("multi-lane-reopen-tombstone");
    let config = multi_lane_config(&file.0).with_reclaim_mode(ReclaimMode::SecondChance);
    let key_for_lane = |prefix: &str, wanted: usize| {
        (0_u32..10_000)
            .map(|id| format!("{prefix}-{id}"))
            .find(|key| {
                hash_key(config.hash_seed, key.as_bytes()) as usize % config.append_lanes == wanted
            })
            .expect("test must find a key for each append lane")
    };
    let filler = key_for_lane("lane-zero-filler", 0);
    let target = key_for_lane("lane-zero-target", 0);
    assert_ne!(filler, target);

    let cache = config.clone().open().unwrap();
    let (initial_lane_zero, lane_one) = {
        let state = cache.inner.state.lock().unwrap();
        (state.active_regions[0], state.active_regions[1])
    };
    let value = vec![b'x'; 900];
    for generation in 0_u8..64 {
        let mut value = value.clone();
        value[0] = generation;
        assert_eq!(
            cache
                .put(filler.as_bytes(), value, PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        if cache.inner.state.lock().unwrap().active_regions[0] != initial_lane_zero {
            break;
        }
    }
    let rotated_lane_zero = cache.inner.state.lock().unwrap().active_regions[0];
    assert_ne!(rotated_lane_zero, initial_lane_zero);
    assert_eq!(
        cache
            .put(
                target.as_bytes(),
                b"survives-restart",
                PutOptions::default()
            )
            .unwrap(),
        PutOutcome::Stored
    );
    {
        let state = cache.inner.state.lock().unwrap();
        assert_eq!(state.active_regions, vec![rotated_lane_zero, lane_one]);
        assert!(
            state.regions[rotated_lane_zero as usize]
                .header
                .created_seqno
                > state.regions[lane_one as usize].header.created_seqno,
            "created-seqno sorting would swap these lane identities"
        );
    }
    cache.close().unwrap();

    let reopened = config.clone().open().unwrap();
    {
        let state = reopened.inner.state.lock().unwrap();
        assert_eq!(state.active_regions, vec![rotated_lane_zero, lane_one]);
    }
    assert_eq!(
        reopened.remove(target.as_bytes()).unwrap(),
        RemoveOutcome::Removed
    );
    let hash = hash_key(config.hash_seed, target.as_bytes());
    let state = reopened.inner.state.lock().unwrap();
    let tombstone = state
        .index
        .get(hash, state.superblock.epoch_start_seqno)
        .expect("remove must publish a tombstone");
    assert!(tombstone.location.is_tombstone());
    assert_eq!(
        tombstone.location.region_id(),
        rotated_lane_zero,
        "the tombstone must stay in the target key's pre-restart lane"
    );
    drop(state);
    assert_eq!(reopened.get(target.as_bytes()).unwrap(), None);
    reopened.close().unwrap();
}

#[test]
fn append_lanes_equal_to_region_count_is_rejected_before_file_creation() {
    let file = TestFile::new("lanes-need-spare-region");
    let invalid = config(&file.0).with_append_lanes(2);

    assert!(matches!(invalid.open(), Err(CacheError::InvalidConfig(_))));
    assert!(!file.0.exists());
}

#[test]
fn close_drains_an_accepted_put_before_releasing_the_file_lock() {
    let file = TestFile::new("close-drain");
    let config = config(&file.0);
    let (backend, records) = RecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config.clone(), Box::new(backend)).unwrap();
    records.arm();

    let put_cache = cache.clone();
    let put = std::thread::spawn(move || {
        put_cache.put(b"accepted-key", b"accepted-value", PutOptions::default())
    });
    let put_accepted = records.wait_for_entries(1, Duration::from_secs(5));

    let close_cache = cache.clone();
    let (close_started, close_is_started) = std::sync::mpsc::channel();
    let close = std::thread::spawn(move || {
        let _ = close_started.send(());
        close_cache.close()
    });
    let close_started_before_release = close_is_started
        .recv_timeout(Duration::from_secs(5))
        .is_ok();
    let admission_deadline = Instant::now() + Duration::from_secs(5);
    let admission_closed = loop {
        if cache.status() == CacheStatus::Closed {
            break true;
        }
        if Instant::now() >= admission_deadline {
            break false;
        }
        std::thread::yield_now();
    };
    let rejected_during_drain = cache.get(b"accepted-key");

    // Release before any assertion or join so both the accepted put and the
    // close waiter can make progress even when the watchdog detects a bug.
    records.release();
    let put_result = put.join().unwrap();
    let close_result = close.join().unwrap();

    assert!(
        put_accepted,
        "the put did not reach its record persistence point"
    );
    assert!(
        close_started_before_release,
        "the close caller did not start before the gate watchdog"
    );
    assert!(admission_closed, "close did not stop new admission");
    assert!(matches!(rejected_during_drain, Err(CacheError::Closed)));
    assert_eq!(put_result.unwrap(), PutOutcome::Stored);
    close_result.unwrap();
    assert_eq!(cache.status(), CacheStatus::Closed);

    // Keep the closed instance alive while reopening: close(), rather than
    // descriptor destruction, must have drained the lane and released flock.
    let reopened = config.open().unwrap();
    assert_eq!(
        reopened.get(b"accepted-key").unwrap(),
        Some(b"accepted-value".to_vec())
    );
    reopened.close().unwrap();
}

#[test]
fn clear_waits_for_an_accepted_put_and_removes_both_generations() {
    let file = TestFile::new("clear-barrier");
    let config = config(&file.0);
    let (backend, records) = RecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config.clone(), Box::new(backend)).unwrap();
    cache
        .put(b"old-key", b"old-value", PutOptions::default())
        .unwrap();
    records.arm();

    let put_cache = cache.clone();
    let put = std::thread::spawn(move || {
        put_cache.put(b"accepted-key", b"accepted-value", PutOptions::default())
    });
    let put_accepted = records.wait_for_entries(1, Duration::from_secs(5));
    let clear_cache = cache.clone();
    let clear = std::thread::spawn(move || clear_cache.clear());

    records.release();
    let put_result = put.join().unwrap();
    let clear_result = clear.join().unwrap();

    assert!(put_accepted, "the put did not reach its persistence point");
    assert_eq!(put_result.unwrap(), PutOutcome::Stored);
    clear_result.unwrap();
    assert_eq!(cache.get(b"old-key").unwrap(), None);
    assert_eq!(cache.get(b"accepted-key").unwrap(), None);
    cache.close().unwrap();

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"old-key").unwrap(), None);
    assert_eq!(reopened.get(b"accepted-key").unwrap(), None);
    reopened.close().unwrap();
}

#[test]
fn close_contains_a_panicking_backend_and_remains_idempotent() {
    let file = TestFile::new("close-worker-panic");
    let config = config(&file.0);
    let (backend, panic_on_checkpoint) = PanicSyncBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config.clone(), Box::new(backend)).unwrap();
    cache
        .put(b"dirty-key", b"dirty-value", PutOptions::default())
        .unwrap();
    panic_on_checkpoint.store(true, Ordering::SeqCst);

    assert!(matches!(cache.close(), Err(CacheError::Io(_))));
    assert!(!cache.inner.state.is_poisoned());
    assert_eq!(cache.status(), CacheStatus::Closed);
    cache.close().unwrap();

    // The engine converts the panic into an I/O completion and close drains all
    // workers before releasing the file lock.
    let reopened = config.open().unwrap();
    let recovered = reopened.get(b"dirty-key").unwrap();
    assert!(recovered.is_none() || recovered == Some(b"dirty-value".to_vec()));
    reopened.close().unwrap();
}

#[test]
fn region_reuse_waits_for_a_reader_of_the_old_incarnation() {
    let file = TestFile::new("read-vs-reuse");
    let config = config(&file.0).with_index_slots(512);
    let (backend, reads) = GateBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
    let victim_key = b"victim-key";
    let victim_value = b"victim-value";
    cache
        .put(victim_key, victim_value, PutOptions::default())
        .unwrap();

    let filler = vec![7_u8; 900];
    let mut id = 1_u64;
    let first_region = cache.inner.state.lock().unwrap().active_regions[0];
    while cache.inner.state.lock().unwrap().active_regions[0] == first_region {
        cache
            .put(fixed_key(id), &filler, PutOptions::default())
            .unwrap();
        id += 1;
    }

    // Stop one record before the next rotation. With two regions, that next
    // append reuses the first region containing victim-key.
    loop {
        let key = fixed_key(id);
        let record_len = RecordHeader::aligned_len(key.len(), filler.len()).unwrap();
        let remaining = {
            let state = cache.inner.state.lock().unwrap();
            let active = state.active_regions[0] as usize;
            cache.inner.config.region_size - state.regions[active].used
        };
        if u64::from(record_len) > remaining {
            break;
        }
        cache.put(&key, &filler, PutOptions::default()).unwrap();
        id += 1;
    }
    assert_eq!(cache.get(victim_key).unwrap(), Some(victim_value.to_vec()));
    cache.flush().unwrap();

    let schedule = ScheduleLog::default();
    let observed = schedule.clone();
    *cache.inner.schedule_observer.lock().unwrap() =
        Some(Arc::new(move |point| observed.record(point)));
    reads.arm();

    let reader_cache = cache.clone();
    let reader = std::thread::spawn(move || reader_cache.get(victim_key));
    let read_entered = reads.wait_for_entries(1, Duration::from_secs(5));

    let writer_cache = cache.clone();
    let trigger_key = fixed_key(id);
    let expected_trigger_key = trigger_key.clone();
    let writer =
        std::thread::spawn(move || writer_cache.put(trigger_key, filler, PutOptions::default()));
    let reuse_waiting =
        schedule.wait_for(SchedulePoint::RotateBlockedByReader, Duration::from_secs(5));

    // Always release the reader before assertions and joins.
    reads.release();
    let reader_result = reader.join().unwrap();
    let writer_result = writer.join().unwrap();

    assert!(read_entered, "the victim read did not reach positioned I/O");
    assert!(reuse_waiting, "the writer did not reach region reuse");
    assert_eq!(reader_result.unwrap(), Some(victim_value.to_vec()));
    assert_eq!(writer_result.unwrap(), PutOutcome::Stored);
    let events = schedule.events();
    let read_completed = events
        .iter()
        .position(|point| *point == SchedulePoint::ReadCompleted)
        .expect("reader completion event");
    let reuse_acquired = events
        .iter()
        .position(|point| *point == SchedulePoint::RotateReadersDrained)
        .expect("reuse acquisition event");
    assert!(
        read_completed < reuse_acquired,
        "region reuse acquired the write view before the old read completed: {events:?}"
    );
    assert_eq!(cache.get(victim_key).unwrap(), None);
    assert_eq!(
        cache.get(&expected_trigger_key).unwrap(),
        Some(vec![7_u8; 900])
    );
    assert_eq!(cache.stats().regions_reused, 1);
    cache.close().unwrap();
}
