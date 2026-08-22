use super::*;

use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new(name: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "cache-rs-m6-{name}-{}-{nonce}.cache",
            std::process::id()
        )))
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn config(path: &Path) -> CacheConfig {
    CacheConfig::new(path, DATA_OFFSET + 3 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(256)
        .with_max_value_size(2048)
        .with_checkpoint_interval_bytes(0)
}

fn eight_lane_config(path: &Path, region_count: u64) -> CacheConfig {
    CacheConfig::new(path, DATA_OFFSET + region_count * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(512)
        .with_max_key_size(256)
        .with_max_value_size(2048)
        .with_append_lanes(8)
        .with_checkpoint_interval_bytes(0)
}

fn keys_for_append_lanes(lanes: usize) -> Vec<Vec<u8>> {
    let mut keys = vec![None; lanes];
    let mut candidate = 0_u64;
    while keys.iter().any(Option::is_none) {
        let key = format!("lane-key-{candidate}").into_bytes();
        let lane = hash_key(DEFAULT_HASH_SEED, &key) as usize % lanes;
        if keys[lane].is_none() {
            keys[lane] = Some(key);
        }
        candidate += 1;
    }
    keys.into_iter().map(Option::unwrap).collect()
}

// Must stay byte-for-byte compatible with `m1_tests::config`, because the
// subprocess crash worker reopens the file with that helper.
fn crash_worker_config(path: &Path) -> CacheConfig {
    CacheConfig::new(path, DATA_OFFSET + 2 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(256)
        .with_max_value_size(2048)
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    predicate()
}

fn reopen_after_drop(config: &CacheConfig) -> DiskCache {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match config.clone().open() {
            Ok(cache) => return cache,
            Err(CacheError::Locked) if Instant::now() < deadline => {
                // The append worker sends a mutation completion immediately
                // before releasing its last transient `Inner` reference.
                std::thread::yield_now();
            }
            Err(error) => panic!("failed to reopen dropped cache: {error}"),
        }
    }
}

fn verify_interrupted_eight_lane_rotation(name: &str, recovery_mode: RecoveryMode) {
    let file = TestFile::new(name);
    let config = eight_lane_config(&file.0, 12).with_recovery_mode(recovery_mode);
    let cache = config.clone().open().unwrap();
    let keys = keys_for_append_lanes(8);
    let values = (0..8)
        .map(|lane_id| format!("sentinel-{lane_id}").into_bytes())
        .collect::<Vec<_>>();
    for (key, value) in keys.iter().zip(&values) {
        assert_eq!(
            cache.put(key, value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
    }
    cache.flush().unwrap();

    // Reproduce the exact durable state between the two rotation barriers:
    // the checkpoint owner for one lane is Sealed, but its replacement has
    // not yet become Active. Do not update runtime state after the seal sync;
    // dropping the cache models the process disappearing at that boundary.
    {
        let _barrier = cache.inner.operation_barrier.write().unwrap();
        let mut state = cache.inner.state.lock().unwrap();
        cache.mark_dirty(&mut state).unwrap();
        let interrupted_lane = 3;
        let old_active = state.active_regions[interrupted_lane] as usize;
        let mut sealed = state.regions[old_active].header;
        sealed.state = RegionState::Sealed;
        sealed.used = state.regions[old_active].used;
        cache
            .write_region_header(&state.superblock, sealed)
            .unwrap();
        cache
            .engine_sync(SyncPoint::RegionRotation, SyncMode::Data)
            .unwrap();
    }
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert!(
        wait_until(Duration::from_secs(3), || {
            reopened.status() == CacheStatus::Healthy
        }),
        "{name} recovery did not publish a Healthy cache"
    );
    for (key, value) in keys.iter().zip(&values) {
        assert_eq!(reopened.get(key).unwrap(), Some(value.clone()));
    }
    let stats = reopened.stats();
    assert_eq!(stats.checkpoint_loads, 1);
    assert_eq!(stats.checkpoint_fallbacks, 0);
    {
        let state = reopened.inner.state.lock().unwrap();
        assert!(state.superblock.clean);
        assert_eq!(state.active_regions.len(), 8);
        let mut unique = state.active_regions.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), 8);
    }

    for (lane_id, key) in keys.iter().enumerate() {
        let value = format!("after-repair-{lane_id}");
        assert_eq!(
            reopened.put(key, value, PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
    }
    reopened.close().unwrap();

    let final_reopen = config.open().unwrap();
    for (lane_id, key) in keys.iter().enumerate() {
        assert_eq!(
            final_reopen.get(key).unwrap(),
            Some(format!("after-repair-{lane_id}").into_bytes())
        );
    }
    final_reopen.close().unwrap();
}

#[test]
fn dirty_restart_repairs_interrupted_eight_lane_rotation_without_formatting() {
    for (name, recovery_mode) in [
        ("interrupted-eight-lane-blocking", RecoveryMode::Blocking),
        ("interrupted-eight-lane-miss-only", RecoveryMode::MissOnly),
    ] {
        verify_interrupted_eight_lane_rotation(name, recovery_mode);
    }
}

#[test]
fn multiple_empty_replacement_actives_are_mapped_deterministically() {
    let file = TestFile::new("multiple-empty-replacements");
    let config = eight_lane_config(&file.0, 12);
    let cache = config.open().unwrap();
    let (superblock, checkpoint_regions, mut regions) = {
        let state = cache.inner.state.lock().unwrap();
        let checkpoint_regions = state
            .regions
            .iter()
            .map(|region| {
                let lane_id = state
                    .active_regions
                    .iter()
                    .position(|region_id| *region_id == region.header.region_id)
                    .map(|lane_id| lane_id as u8);
                checkpoint_region(region, lane_id)
            })
            .collect::<Vec<_>>();
        (state.superblock, checkpoint_regions, state.regions.clone())
    };

    for region in regions.iter_mut().take(3) {
        region.header.state = RegionState::Sealed;
    }
    for (region_id, created_seqno) in [(9, 101), (10, 102), (8, 103)] {
        regions[region_id] = RegionMeta {
            header: RegionHeader {
                region_id: region_id as u32,
                incarnation: 1,
                state: RegionState::Active,
                created_seqno,
                used: REGION_HEADER_SIZE as u64,
            },
            used: REGION_HEADER_SIZE as u64,
            max_seqno: 0,
        };
    }
    let candidates = [10, 4, 8, 7, 9, 3, 5, 6];
    let restored = restore_active_region_lanes(
        cache.inner.io.as_ref(),
        &superblock,
        &regions,
        &candidates,
        Some(&checkpoint_regions),
        8,
    )
    .unwrap();
    assert_eq!(restored, vec![9, 10, 8, 3, 4, 5, 6, 7]);
    cache.close().unwrap();
}

#[test]
fn dirty_restart_replays_put_and_remove_from_the_checkpoint_tail() {
    let file = TestFile::new("dirty-tail");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    cache
        .put("updated", "before", PutOptions::default())
        .unwrap();
    cache
        .put("removed", "present", PutOptions::default())
        .unwrap();
    cache.flush().unwrap();

    cache
        .put("updated", "after", PutOptions::default())
        .unwrap();
    assert_eq!(cache.remove(b"removed").unwrap(), RemoveOutcome::Removed);
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert_eq!(reopened.get(b"updated").unwrap(), Some(b"after".to_vec()));
    assert_eq!(reopened.get(b"removed").unwrap(), None);
    let stats = reopened.stats();
    assert_eq!(stats.checkpoint_loads, 1);
    assert!(stats.recovery_regions_scanned >= 1);
    assert!(stats.recovery_records_scanned >= 2);
    assert!(stats.recovery_bytes_scanned > 0);
    reopened.close().unwrap();
}

#[test]
fn hybrid_data_fence_recovers_dirty_tail_without_rewriting_full_index_checkpoint() {
    let file = TestFile::new("hybrid-data-fence");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    let baseline_checkpoints = cache.stats().checkpoint_writes;
    cache
        .put("fenced", "durable-tail", PutOptions::default())
        .unwrap();

    cache.sync_mutations_for_hybrid().unwrap();
    assert_eq!(cache.stats().checkpoint_writes, baseline_checkpoints);
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert_eq!(
        reopened.get(b"fenced").unwrap(),
        Some(b"durable-tail".to_vec())
    );
    assert_eq!(reopened.stats().checkpoint_loads, 1);
    assert!(reopened.stats().recovery_records_scanned >= 1);
    reopened.close().unwrap();
}

#[test]
fn fresh_format_publishes_a_baseline_before_the_first_dirty_mutation() {
    let file = TestFile::new("fresh-baseline");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    assert_eq!(cache.stats().checkpoint_writes, 1);

    cache
        .put("first", "survives", PutOptions::default())
        .unwrap();
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert_eq!(reopened.get(b"first").unwrap(), Some(b"survives".to_vec()));
    assert_eq!(reopened.stats().checkpoint_loads, 1);
}

#[test]
fn clear_checkpoint_is_a_barrier_for_later_dirty_recovery() {
    let file = TestFile::new("clear-barrier");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    cache
        .put("old", "must-not-return", PutOptions::default())
        .unwrap();
    cache.flush().unwrap();
    cache.clear().unwrap();
    cache.put("new", "survives", PutOptions::default()).unwrap();
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert_eq!(reopened.get(b"old").unwrap(), None);
    assert_eq!(reopened.get(b"new").unwrap(), Some(b"survives".to_vec()));
    let stats = reopened.stats();
    assert_eq!(stats.checkpoint_loads, 1);
    assert!(stats.recovery_records_scanned >= 1);
    reopened.close().unwrap();
}

fn checkpoint_directory_and_headers(
    path: &Path,
) -> (
    CheckpointDirectory,
    [Option<CheckpointSlotHeader>; CHECKPOINT_SLOT_COUNT],
) {
    let backend = FileBackend::open(path).unwrap();
    let superblock = read_superblock(&backend).unwrap().unwrap();
    let directory = read_checkpoint_directory(&backend, data_file_len(&superblock).unwrap())
        .unwrap()
        .unwrap();
    let mut headers = [None; CHECKPOINT_SLOT_COUNT];
    for (slot, output) in headers.iter_mut().enumerate() {
        let mut encoded = [0_u8; CHECKPOINT_SLOT_HEADER_SIZE];
        read_exact_at(
            &backend,
            &mut encoded,
            directory.slot_header_offset(slot).unwrap(),
        )
        .unwrap();
        *output = CheckpointSlotHeader::decode(&encoded, directory, slot).ok();
    }
    (directory, headers)
}

#[derive(Clone, Copy)]
enum CheckpointCorruption {
    Payload,
    Header,
}

fn corrupt_newest_checkpoint(path: &Path, kind: CheckpointCorruption) {
    let (directory, headers) = checkpoint_directory_and_headers(path);
    let newest = headers
        .into_iter()
        .flatten()
        .max_by_key(|header| header.generation)
        .expect("a flush must publish a checkpoint slot");
    let (offset, point, sync_point) = match kind {
        CheckpointCorruption::Payload => (
            directory
                .slot_payload_offset(usize::from(newest.slot))
                .unwrap(),
            WritePoint::CheckpointPayload,
            SyncPoint::CheckpointPayload,
        ),
        CheckpointCorruption::Header => (
            directory
                .slot_header_offset(usize::from(newest.slot))
                .unwrap(),
            WritePoint::CheckpointHeader,
            SyncPoint::CheckpointHeader,
        ),
    };
    let backend = FileBackend::open(path).unwrap();
    let mut byte = [0_u8; 1];
    read_exact_at(&backend, &mut byte, offset).unwrap();
    byte[0] ^= 0x5a;
    write_all_at(&backend, point, &byte, offset).unwrap();
    backend.sync(sync_point, SyncMode::Data).unwrap();
}

#[test]
fn double_slot_alternates_and_corrupt_newest_never_resurrects_a_tombstone() {
    for (name, corruption) in [
        ("payload", CheckpointCorruption::Payload),
        ("header", CheckpointCorruption::Header),
    ] {
        let file = TestFile::new(name);
        let config = config(&file.0);
        let cache = config.clone().open().unwrap();
        cache.put("key", "old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
        cache.flush().unwrap();
        drop(cache);

        let (_, headers) = checkpoint_directory_and_headers(&file.0);
        let first = headers[0].expect("first checkpoint slot must be committed");
        let second = headers[1].expect("second checkpoint slot must be committed");
        assert_ne!(first.generation, second.generation);
        assert_eq!(first.generation.abs_diff(second.generation), 2);

        corrupt_newest_checkpoint(&file.0, corruption);
        let reopened = reopen_after_drop(&config);
        assert_eq!(reopened.get(b"key").unwrap(), None, "{name}");
        let stats = reopened.stats();
        assert!(
            stats.checkpoint_loads > 0 || stats.checkpoint_fallbacks > 0,
            "{name} neither used the older slot nor reported a full-scan fallback"
        );
        reopened.close().unwrap();
    }
}

struct RecoveryGate {
    released: Mutex<bool>,
    ready: Condvar,
    blocked: AtomicBool,
}

impl RecoveryGate {
    fn new() -> Self {
        Self {
            released: Mutex::new(false),
            ready: Condvar::new(),
            blocked: AtomicBool::new(false),
        }
    }

    fn wait_if_recovery_thread(&self) {
        if std::thread::current().name() != Some("cache-rs-recovery") {
            return;
        }
        self.blocked.store(true, Ordering::Release);
        let released = self.released.lock().unwrap();
        drop(
            self.ready
                .wait_while(released, |released| !*released)
                .unwrap(),
        );
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.ready.notify_all();
    }
}

struct GatedBackend {
    inner: FileBackend,
    gate: Arc<RecoveryGate>,
}

impl GatedBackend {
    fn open(path: &Path, gate: Arc<RecoveryGate>) -> io::Result<Self> {
        Ok(Self {
            inner: FileBackend::open(path)?,
            gate,
        })
    }
}

impl IoBackend for GatedBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }

    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }

    fn preallocate(&self, len: u64) -> io::Result<()> {
        self.inner.preallocate(len)
    }

    fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
        self.gate.wait_if_recovery_thread();
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

    fn direct_io_stats(&self) -> crate::io_backend::DirectIoStats {
        self.inner.direct_io_stats()
    }
}

#[test]
fn miss_only_recovers_behind_a_single_atomic_publication() {
    let file = TestFile::new("miss-only");
    let base = config(&file.0);
    let cache = base.clone().open().unwrap();
    cache.put("baseline", "old", PutOptions::default()).unwrap();
    cache.flush().unwrap();
    cache.put("baseline", "new", PutOptions::default()).unwrap();
    cache.put("tail", "present", PutOptions::default()).unwrap();
    drop(cache);

    let gate = Arc::new(RecoveryGate::new());
    let backend = GatedBackend::open(&file.0, Arc::clone(&gate)).unwrap();
    let miss_only = base.with_recovery_mode(RecoveryMode::MissOnly);
    let reopened = DiskCache::open_with_backend(miss_only, Box::new(backend)).unwrap();
    assert!(wait_until(Duration::from_secs(1), || gate
        .blocked
        .load(Ordering::Acquire)));
    let initial_status = reopened.status();
    let initial_get = reopened.get(b"baseline");
    let initial_put = reopened.put("blocked", "value", PutOptions::default());
    let initial_stats = reopened.stats();
    let initial_region_stats = reopened.region_stats().unwrap();
    gate.release();

    assert_eq!(initial_status, CacheStatus::MissOnly);
    assert_eq!(initial_get.unwrap(), None);
    assert!(matches!(initial_put, Err(CacheError::Poisoned)));
    assert!(initial_stats.recovery_in_progress);
    assert_eq!(initial_stats.region_valid_bytes, 0);
    assert!(
        initial_region_stats
            .iter()
            .all(|region| region.valid_bytes == 0 && region.valid_ratio_bps == 0)
    );
    assert!(wait_until(Duration::from_secs(3), || {
        reopened.status() == CacheStatus::Healthy
    }));
    assert_eq!(reopened.get(b"baseline").unwrap(), Some(b"new".to_vec()));
    assert_eq!(reopened.get(b"tail").unwrap(), Some(b"present".to_vec()));
    let stats = reopened.stats();
    assert!(!stats.recovery_in_progress);
    assert_eq!(stats.checkpoint_loads, 1);
    assert!(stats.recovery_records_scanned >= 2);
    reopened.close().unwrap();
}

#[test]
fn periodic_threshold_publishes_and_close_does_not_deadlock() {
    let file = TestFile::new("periodic");
    let config = config(&file.0).with_checkpoint_interval_bytes(1);
    let cache = config.open().unwrap();
    let baseline_writes = cache.stats().checkpoint_writes;
    let keep_reading = Arc::new(AtomicBool::new(true));
    let readers = (0..4)
        .map(|_| {
            let cache = cache.clone();
            let keep_reading = Arc::clone(&keep_reading);
            std::thread::spawn(move || {
                while keep_reading.load(Ordering::Acquire) {
                    let _ = cache.get(b"reader-pressure");
                }
            })
        })
        .collect::<Vec<_>>();

    for id in 0..32_u8 {
        cache.put([id], [id; 8], PutOptions::default()).unwrap();
        if wait_until(Duration::from_millis(20), || {
            cache.stats().checkpoint_writes > baseline_writes
        }) {
            break;
        }
    }
    assert!(cache.stats().checkpoint_writes > baseline_writes);
    keep_reading.store(false, Ordering::Release);
    for reader in readers {
        reader.join().unwrap();
    }

    let (completed_tx, completed_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = completed_tx.send(cache.close());
    });
    assert!(
        completed_rx
            .recv_timeout(Duration::from_secs(3))
            .expect("close deadlocked after periodic checkpoint")
            .is_ok()
    );
}

#[test]
fn dirty_active_tail_with_only_one_alignment_unit_remaining_recovers() {
    let file = TestFile::new("active-tail-32");
    let region_size = 16 * 1024_u64;
    let record_len = region_size - REGION_HEADER_SIZE as u64 - 32;
    let value_len = record_len as usize - RECORD_HEADER_SIZE - 1;
    assert_eq!(
        u64::from(RecordHeader::aligned_len(1, value_len).unwrap()),
        record_len
    );
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 2 * region_size)
        .with_region_size(region_size)
        .with_index_slots(64)
        .with_max_key_size(1)
        .with_max_value_size(value_len)
        .with_checkpoint_interval_bytes(0);
    let cache = config.clone().open().unwrap();
    cache.flush().unwrap();
    let value = vec![b'x'; value_len];
    cache.put(b"k", &value, PutOptions::default()).unwrap();
    {
        let state = cache.inner.state.lock().unwrap();
        let active = state.active_regions[0] as usize;
        assert_eq!(region_size - state.regions[active].used, 32);
    }
    drop(cache);

    let reopened = reopen_after_drop(&config);
    assert_eq!(reopened.get(b"k").unwrap(), Some(value));
    assert_eq!(reopened.stats().checkpoint_loads, 1);
    assert_eq!(reopened.stats().recovery_records_scanned, 1);
    reopened.close().unwrap();
}

#[derive(Clone, Copy)]
enum TombstoneDamage {
    CorruptHeader,
    TruncateRecord,
}

#[test]
fn damaged_or_truncated_dirty_tombstone_tail_never_revives_the_checkpoint_value() {
    for (name, damage) in [
        ("corrupt-tombstone", TombstoneDamage::CorruptHeader),
        ("truncated-tombstone", TombstoneDamage::TruncateRecord),
    ] {
        let file = TestFile::new(name);
        let config = config(&file.0);
        let cache = config.clone().open().unwrap();
        cache
            .put("victim", "checkpoint-value", PutOptions::default())
            .unwrap();
        cache.flush().unwrap();
        assert_eq!(cache.remove(b"victim").unwrap(), RemoveOutcome::Removed);

        let state = cache.inner.state.lock().unwrap();
        let entry = cache
            .inner
            .index
            .get(
                hash_key(state.superblock.hash_seed, b"victim"),
                state.superblock.epoch_start_seqno,
            )
            .expect("remove must publish its tombstone");
        assert!(entry.location.is_tombstone());
        let absolute = region_base(&state.superblock, entry.location.region_id())
            .unwrap()
            .checked_add(u64::from(entry.location.offset()))
            .unwrap();
        let record_len = u64::from(entry.location.record_len());
        drop(state);
        drop(cache);

        let backend = FileBackend::open(&file.0).unwrap();
        match damage {
            TombstoneDamage::CorruptHeader => {
                let mut byte = [0_u8; 1];
                read_exact_at(&backend, &mut byte, absolute).unwrap();
                byte[0] ^= 0x5a;
                write_all_at(&backend, WritePoint::Record, &byte, absolute).unwrap();
                backend
                    .sync(SyncPoint::CheckpointData, SyncMode::Data)
                    .unwrap();
            }
            TombstoneDamage::TruncateRecord => {
                backend.set_len(absolute + record_len / 2).unwrap();
                backend
                    .sync(SyncPoint::CheckpointData, SyncMode::Data)
                    .unwrap();
            }
        }
        drop(backend);

        let reopened = reopen_after_drop(&config);
        assert_eq!(reopened.status(), CacheStatus::Healthy, "{name}");
        assert_eq!(reopened.get(b"victim").unwrap(), None, "{name}");
        reopened.close().unwrap();
    }
}

fn run_existing_crash_worker(path: &Path, event: &str, timing: &str, operation: &str) {
    let output = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("cache::m1_tests::crash_worker")
        .arg("--ignored")
        .arg("--test-threads=1")
        .env("CACHE_RS_CRASH_PATH", path)
        .env("CACHE_RS_CRASH_EVENT", event)
        .env("CACHE_RS_CRASH_OCCURRENCE", "1")
        .env("CACHE_RS_CRASH_TIMING", timing)
        .env("CACHE_RS_CRASH_OPERATION", operation)
        .output()
        .unwrap();
    assert_eq!(
        output.status.signal(),
        Some(9),
        "crash worker did not reach {event}/{timing}/{operation}: {}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn checkpoint_and_clear_crash_failpoints_do_not_resurrect_removed_values() {
    for event in [
        "checkpoint-payload",
        "checkpoint-payload-sync",
        "checkpoint-header",
        "checkpoint-header-sync",
    ] {
        for timing in ["before", "after"] {
            let file = TestFile::new(&format!("crash-{event}-{timing}"));
            let config = crash_worker_config(&file.0);
            let cache = config.clone().open().unwrap();
            cache.put("key", "old", PutOptions::default()).unwrap();
            cache.flush().unwrap();
            drop(cache);

            run_existing_crash_worker(&file.0, event, timing, "remove");
            let reopened = reopen_after_drop(&config);
            assert_eq!(reopened.get(b"key").unwrap(), None, "{event}/{timing}");
            reopened.close().unwrap();
        }
    }

    for timing in ["before", "after"] {
        let file = TestFile::new(&format!("crash-clear-{timing}"));
        let config = crash_worker_config(&file.0);
        let cache = config.clone().open().unwrap();
        cache.put("key", "old", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        drop(cache);

        run_existing_crash_worker(&file.0, "clear-sync", timing, "clear");
        let reopened = reopen_after_drop(&config);
        assert_eq!(reopened.get(b"key").unwrap(), None, "clear/{timing}");
        reopened.close().unwrap();
    }
}
