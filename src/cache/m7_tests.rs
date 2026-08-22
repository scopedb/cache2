use super::*;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::checkpoint::CHECKPOINT_INDEX_ENTRY_V1_SIZE;
use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

#[derive(Default)]
struct RecordGateState {
    armed: bool,
    released: bool,
    entered: usize,
}

#[derive(Clone, Default)]
struct RecordGate {
    shared: Arc<(Mutex<RecordGateState>, Condvar)>,
}

impl RecordGate {
    fn arm(&self) {
        let (state, _) = &*self.shared;
        *state.lock().unwrap() = RecordGateState {
            armed: true,
            ..RecordGateState::default()
        };
    }

    fn observe(&self) {
        let (state, changed) = &*self.shared;
        let mut state = state.lock().unwrap();
        state.entered += 1;
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
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let (next, timed) = changed.wait_timeout(state, remaining).unwrap();
            state = next;
            if timed.timed_out() && state.entered < target {
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

    fn entered(&self) -> usize {
        let (state, _) = &*self.shared;
        state.lock().unwrap().entered
    }
}

struct GatedRecordBackend {
    inner: FileBackend,
    gate: RecordGate,
}

impl GatedRecordBackend {
    fn open(path: &Path) -> io::Result<(Self, RecordGate)> {
        let gate = RecordGate::default();
        Ok((
            Self {
                inner: FileBackend::open(path)?,
                gate: gate.clone(),
            },
            gate,
        ))
    }
}

impl IoBackend for GatedRecordBackend {
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
            self.gate.observe();
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

struct TestFile(PathBuf);

impl TestFile {
    fn new(name: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "cache-rs-m7-{name}-{}-{nonce}.cache",
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
    CacheConfig::new(path, DATA_OFFSET + 4 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(256)
        .with_max_value_size(8 * 1024)
        .with_submission_queue_depths(2, 2)
        .with_checkpoint_interval_bytes(0)
}

fn reopen(config: &CacheConfig) -> DiskCache {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        match config.clone().open() {
            Ok(cache) => return cache,
            Err(CacheError::Locked) if Instant::now() < deadline => std::thread::yield_now(),
            Err(error) => panic!("failed to reopen cache: {error}"),
        }
    }
}

fn encoded_value_len(namespace: NamespaceId, key: &[u8], value: &[u8]) -> u64 {
    u64::from(
        RecordHeader::aligned_len(encoded_key_len(namespace, key.len()).unwrap(), value.len())
            .unwrap(),
    )
}

fn encoded_tombstone_len(namespace: NamespaceId, key: &[u8]) -> u64 {
    u64::from(RecordHeader::aligned_len(encoded_key_len(namespace, key.len()).unwrap(), 0).unwrap())
}

fn total_valid_bytes(cache: &DiskCache) -> u64 {
    cache
        .region_stats()
        .unwrap()
        .into_iter()
        .map(|region| region.valid_bytes)
        .sum()
}

fn record_write_count(events: &[FaultEvent]) -> usize {
    events
        .iter()
        .filter(|event| **event == FaultEvent::Write(WritePoint::Record))
        .count()
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

fn open_with_sealed_hot(
    path: &Path,
    key: &[u8],
    value: &[u8],
    expires_at_unix_ms: Option<u64>,
) -> (CacheConfig, DiskCache, u64, IndexEntry) {
    let config = config(path)
        .with_max_value_size(11 * 1024)
        .with_reclaim_mode(ReclaimMode::SecondChance);
    let cache = config.clone().open().unwrap();
    cache
        .put(key, value, PutOptions { expires_at_unix_ms })
        .unwrap();
    cache
        .put("fill-a", vec![1_u8; 8_000], PutOptions::default())
        .unwrap();
    cache
        .put("fill-b", vec![2_u8; 3_000], PutOptions::default())
        .unwrap();
    cache
        .put("rotate", vec![3_u8; 2_000], PutOptions::default())
        .unwrap();

    let hash = hash_namespaced_key(cache.inner.config.hash_seed, 0, key);
    let entry = {
        let state = cache.inner.state.lock().unwrap();
        let entry = cache
            .inner
            .index
            .get(hash, state.superblock.epoch_start_seqno)
            .unwrap();
        assert_eq!(
            state.regions[entry.location.region_id() as usize]
                .header
                .state,
            RegionState::Sealed
        );
        entry
    };
    (config, cache, hash, entry)
}

fn complete_second_chance(cache: &DiskCache, key: &[u8], value: &[u8], hash: u64) -> IndexEntry {
    // A worker is deliberately non-blocking and may lose a try-lock race to
    // the get which queued it. Subsequent verified hits are allowed to retry.
    assert!(wait_until(Duration::from_secs(3), || {
        assert_eq!(cache.get(key).unwrap(), Some(value.to_vec()));
        cache.stats().reinsert_completed == 1
    }));
    let state = cache.inner.state.lock().unwrap();
    cache
        .inner
        .index
        .get(hash, state.superblock.epoch_start_seqno)
        .unwrap()
}

fn open_fault_cache(config: CacheConfig) -> (DiskCache, crate::io_backend::testing::FaultHandle) {
    let (backend, handle) = FaultBackend::open(&config.path).unwrap();
    let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
    (cache, handle)
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

/// Re-encode the newest namespace-zero checkpoint with the v0.7 entry shape.
/// This exercises the cache-level migration path, not only the checkpoint codec.
fn rewrite_newest_checkpoint_as_v1(path: &Path) {
    let (directory, headers) = checkpoint_directory_and_headers(path);
    let newest = headers
        .into_iter()
        .flatten()
        .max_by_key(|header| header.generation)
        .expect("close must publish a checkpoint");
    assert_eq!(newest.version, 4);

    let backend = FileBackend::open(path).unwrap();
    let mut current_payload = vec![0_u8; newest.payload_len as usize];
    read_exact_at(
        &backend,
        &mut current_payload,
        directory
            .slot_payload_offset(usize::from(newest.slot))
            .unwrap(),
    )
    .unwrap();

    let region_bytes = newest.region_count as usize * CHECKPOINT_REGION_SNAPSHOT_SIZE;
    let (regions, entries) = current_payload.split_at(region_bytes);
    assert_eq!(
        entries.len(),
        newest.entry_count as usize * CHECKPOINT_INDEX_ENTRY_SIZE
    );
    let mut legacy_payload = Vec::with_capacity(
        region_bytes + newest.entry_count as usize * CHECKPOINT_INDEX_ENTRY_V1_SIZE,
    );
    for region in regions.chunks_exact(CHECKPOINT_REGION_SNAPSHOT_SIZE) {
        let start = legacy_payload.len();
        legacy_payload.extend_from_slice(region);
        // Checkpoint v1 predates the append-lane byte in RegionSnapshot.
        legacy_payload[start + 9] = 0;
    }
    for entry in entries.chunks_exact(CHECKPOINT_INDEX_ENTRY_SIZE) {
        let decoded = decode_checkpoint_index_entry(entry).unwrap();
        assert_eq!(decoded.namespace_id, 0);
        assert_eq!(decoded.flags, 0);
        legacy_payload.extend_from_slice(&entry[..CHECKPOINT_INDEX_ENTRY_V1_SIZE]);
    }

    let legacy_header = CheckpointSlotHeader {
        version: 1,
        payload_len: legacy_payload.len() as u64,
        payload_crc: crc32c(&legacy_payload),
        index_slots: None,
        index_shards: None,
        ..newest
    };
    let slot = usize::from(legacy_header.slot);
    write_all_at(
        &backend,
        WritePoint::CheckpointPayload,
        &legacy_payload,
        directory.slot_payload_offset(slot).unwrap(),
    )
    .unwrap();
    backend
        .sync(SyncPoint::CheckpointPayload, SyncMode::Data)
        .unwrap();
    write_all_at(
        &backend,
        WritePoint::CheckpointHeader,
        &legacy_header.encode(directory).unwrap(),
        directory.slot_header_offset(slot).unwrap(),
    )
    .unwrap();
    backend
        .sync(SyncPoint::CheckpointHeader, SyncMode::Data)
        .unwrap();
}

fn rewrite_newest_checkpoint_source_shards(path: &Path, source_shards: u32) {
    let (directory, headers) = checkpoint_directory_and_headers(path);
    let newest = headers
        .into_iter()
        .flatten()
        .max_by_key(|header| header.generation)
        .expect("close must publish a checkpoint");
    assert_eq!(newest.version, 4);
    assert_ne!(newest.index_shards, Some(source_shards));

    let rewritten = CheckpointSlotHeader {
        index_shards: Some(source_shards),
        ..newest
    };
    let backend = FileBackend::open(path).unwrap();
    write_all_at(
        &backend,
        WritePoint::CheckpointHeader,
        &rewritten.encode(directory).unwrap(),
        directory
            .slot_header_offset(usize::from(rewritten.slot))
            .unwrap(),
    )
    .unwrap();
    backend
        .sync(SyncPoint::CheckpointHeader, SyncMode::Data)
        .unwrap();
}

#[test]
fn namespaces_isolate_the_same_raw_key_across_remove_and_v3_reopen() {
    let file = TestFile::new("namespace-isolation");
    let config = config(&file.0)
        .with_namespace(NamespaceConfig::new(11))
        .with_namespace(NamespaceConfig::new(22));
    let cache = config.clone().open().unwrap();

    assert_eq!(
        cache
            .put("shared", "default", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put_in(11, "shared", "eleven", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put_in(22, "shared", "twenty-two", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache.remove_in(11, b"shared").unwrap(),
        RemoveOutcome::Removed
    );

    assert_eq!(cache.get(b"shared").unwrap(), Some(b"default".to_vec()));
    assert_eq!(cache.get_in(11, b"shared").unwrap(), None);
    assert_eq!(
        cache.get_in(22, b"shared").unwrap(),
        Some(b"twenty-two".to_vec())
    );
    cache.close().unwrap();

    let (_, headers) = checkpoint_directory_and_headers(&file.0);
    assert_eq!(
        headers
            .into_iter()
            .flatten()
            .max_by_key(|header| header.generation)
            .unwrap()
            .version,
        4
    );

    let reopened = reopen(&config);
    assert_eq!(reopened.get(b"shared").unwrap(), Some(b"default".to_vec()));
    assert_eq!(reopened.get_in(11, b"shared").unwrap(), None);
    assert_eq!(
        reopened.get_in(22, b"shared").unwrap(),
        Some(b"twenty-two".to_vec())
    );
    assert_eq!(reopened.namespace_stats(11).unwrap().live_bytes, 0);
    assert!(reopened.namespace_stats(22).unwrap().live_bytes > 0);
    reopened.close().unwrap();
}

#[test]
fn v1_checkpoint_entries_reopen_as_default_namespace() {
    let file = TestFile::new("checkpoint-v1");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    cache
        .put("legacy", "namespace-zero", PutOptions::default())
        .unwrap();
    cache.close().unwrap();

    rewrite_newest_checkpoint_as_v1(&file.0);
    let reopened = reopen(&config);
    assert_eq!(
        reopened.get(b"legacy").unwrap(),
        Some(b"namespace-zero".to_vec())
    );
    assert_eq!(reopened.stats().checkpoint_loads, 1);
    assert_eq!(
        reopened.namespace_stats(0).unwrap().live_bytes,
        encoded_value_len(0, b"legacy", b"namespace-zero")
    );
    reopened.close().unwrap();
}

#[test]
fn checkpoint_with_different_source_sharding_uses_safe_fallback_restore() {
    let file = TestFile::new("checkpoint-shard-fallback");
    let config = config(&file.0);
    let cache = config.clone().open().unwrap();
    cache.put("one", "first", PutOptions::default()).unwrap();
    cache.put("two", "second", PutOptions::default()).unwrap();
    cache.close().unwrap();

    rewrite_newest_checkpoint_source_shards(&file.0, 1);
    let reopened = reopen(&config);
    assert_eq!(reopened.get(b"one").unwrap(), Some(b"first".to_vec()));
    assert_eq!(reopened.get(b"two").unwrap(), Some(b"second".to_vec()));
    assert_eq!(reopened.stats().checkpoint_loads, 1);
    reopened.close().unwrap();
}

#[test]
fn second_hit_rejects_before_dirty_or_record_io_and_large_values_need_three_observations() {
    let file = TestFile::new("second-hit");
    let region_size = 2 * 1024 * 1024_u64;
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 3 * region_size)
        .with_region_size(region_size)
        .with_index_slots(64)
        .with_max_key_size(256)
        .with_max_value_size(crate::policy::LARGE_OBJECT_THRESHOLD_BYTES + 4096)
        .with_submission_queue_depths(1, 1)
        .with_checkpoint_interval_bytes(0)
        .with_admission_mode(AdmissionMode::SecondHit);
    let (cache, handle) = open_fault_cache(config);

    handle.arm(
        FaultEvent::Write(WritePoint::Record),
        usize::MAX,
        FaultAction::Error(5),
    );
    let clean_generation = cache.inner.state.lock().unwrap().superblock.generation;
    assert!(cache.inner.state.lock().unwrap().superblock.clean);
    assert_eq!(
        cache
            .put("ordinary", "value", PutOptions::default())
            .unwrap(),
        PutOutcome::Rejected(RejectReason::AdmissionFiltered)
    );
    {
        let state = cache.inner.state.lock().unwrap();
        assert!(state.superblock.clean);
        assert_eq!(state.superblock.generation, clean_generation);
    }
    assert_eq!(record_write_count(&handle.events()), 0);
    assert_eq!(
        cache
            .put("ordinary", "value", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    cache.flush().unwrap();

    handle.arm(
        FaultEvent::Write(WritePoint::Record),
        usize::MAX,
        FaultAction::Error(5),
    );
    let large = vec![0x5a; crate::policy::LARGE_OBJECT_THRESHOLD_BYTES + 1];
    for _ in 0..2 {
        assert_eq!(
            cache.put("large", &large, PutOptions::default()).unwrap(),
            PutOutcome::Rejected(RejectReason::LargeObjectCold)
        );
        assert!(cache.inner.state.lock().unwrap().superblock.clean);
    }
    assert_eq!(record_write_count(&handle.events()), 0);
    assert_eq!(
        cache.put("large", &large, PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );

    let stats = cache.stats();
    assert_eq!(stats.admission_rejections, 3);
    assert_eq!(stats.large_object_rejections, 2);
    cache.close().unwrap();
}

#[test]
fn namespace_strict_replacement_reservation_isolated_rejection_and_recovery() {
    let file = TestFile::new("namespace-quota");
    let key = b"same";
    let first = vec![1_u8; 64];
    let replacement = vec![2_u8; 64];
    let exact_capacity = encoded_value_len(7, key, &first);
    let config = config(&file.0)
        .with_namespace(
            NamespaceConfig::new(7).with_capacity_bytes(exact_capacity.saturating_mul(2)),
        )
        .with_namespace(NamespaceConfig::new(8).with_capacity_bytes(1024))
        .with_namespace(NamespaceConfig::new(9).with_write_budget(1));
    let (cache, handle) = open_fault_cache(config.clone());

    assert_eq!(
        cache.put_in(7, key, &first, PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put_in(7, key, &replacement, PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.namespace_stats(7).unwrap().live_bytes, exact_capacity);
    cache.flush().unwrap();

    handle.arm(
        FaultEvent::Write(WritePoint::Record),
        usize::MAX,
        FaultAction::Error(5),
    );
    assert_eq!(
        cache
            .put_in(7, "other", vec![3_u8; 88], PutOptions::default())
            .unwrap(),
        PutOutcome::Rejected(RejectReason::NamespaceCapacityExceeded)
    );
    assert_eq!(
        cache.put_in(9, "rate", "x", PutOptions::default()).unwrap(),
        PutOutcome::Rejected(RejectReason::NamespaceWriteBudgetExceeded)
    );
    assert!(cache.inner.state.lock().unwrap().superblock.clean);
    assert_eq!(record_write_count(&handle.events()), 0);

    assert_eq!(
        cache
            .put_in(8, "other", "independent", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.get_in(7, key).unwrap(), Some(replacement.clone()));
    assert_eq!(
        cache.get_in(8, b"other").unwrap(),
        Some(b"independent".to_vec())
    );
    cache.close().unwrap();

    let reopened = reopen(&config);
    assert_eq!(
        reopened.namespace_stats(7).unwrap().live_bytes,
        exact_capacity
    );
    assert_eq!(
        reopened.namespace_stats(8).unwrap().live_bytes,
        encoded_value_len(8, b"other", b"independent")
    );
    assert_eq!(reopened.get_in(7, key).unwrap(), Some(replacement));
    assert_eq!(
        reopened.get_in(8, b"other").unwrap(),
        Some(b"independent".to_vec())
    );
    reopened.close().unwrap();
}

#[test]
fn region_valid_bytes_are_exact_for_overwrite_tombstone_clear_and_reopen() {
    let file = TestFile::new("valid-bytes");
    let namespace = 17;
    let config = config(&file.0).with_namespace(NamespaceConfig::new(namespace));
    let cache = config.clone().open().unwrap();
    let first = vec![1_u8; 10];
    let replacement = vec![2_u8; 100];
    let other = vec![3_u8; 20];

    cache
        .put_in(namespace, "key", &first, PutOptions::default())
        .unwrap();
    assert_eq!(
        total_valid_bytes(&cache),
        encoded_value_len(namespace, b"key", &first)
    );

    cache
        .put_in(namespace, "key", &replacement, PutOptions::default())
        .unwrap();
    let replacement_len = encoded_value_len(namespace, b"key", &replacement);
    assert_eq!(total_valid_bytes(&cache), replacement_len);

    cache
        .put_in(namespace, "other", &other, PutOptions::default())
        .unwrap();
    let other_len = encoded_value_len(namespace, b"other", &other);
    assert_eq!(total_valid_bytes(&cache), replacement_len + other_len);

    assert_eq!(
        cache.remove_in(namespace, b"key").unwrap(),
        RemoveOutcome::Removed
    );
    let tombstone_len = encoded_tombstone_len(namespace, b"key");
    assert_eq!(total_valid_bytes(&cache), tombstone_len + other_len);
    assert_eq!(
        cache.namespace_stats(namespace).unwrap().live_bytes,
        other_len
    );
    cache.close().unwrap();

    let reopened = reopen(&config);
    assert_eq!(
        reopened.inner.index.snapshot_scan_count(),
        0,
        "clean checkpoint accounting must not run a second post-restore index snapshot pass"
    );
    assert_eq!(total_valid_bytes(&reopened), tombstone_len + other_len);
    assert_eq!(
        reopened.namespace_stats(namespace).unwrap().live_bytes,
        other_len
    );
    assert_eq!(reopened.get_in(namespace, b"key").unwrap(), None);
    assert_eq!(reopened.get_in(namespace, b"other").unwrap(), Some(other));

    reopened.clear().unwrap();
    assert_eq!(total_valid_bytes(&reopened), 0);
    assert_eq!(reopened.namespace_stats(namespace).unwrap().live_bytes, 0);
    reopened.close().unwrap();

    let empty = reopen(&config);
    assert_eq!(total_valid_bytes(&empty), 0);
    assert_eq!(empty.namespace_stats(namespace).unwrap().live_bytes, 0);
    assert_eq!(empty.get_in(namespace, b"other").unwrap(), None);
    empty.close().unwrap();
}

#[test]
fn clean_checkpoint_streaming_accounting_tracks_compacted_index_restore() {
    let file = TestFile::new("checkpoint-stream-accounting");
    let namespace = 23;
    let config = config(&file.0)
        .with_index_slots(64)
        .with_namespace(NamespaceConfig::new(namespace));
    let cache = config.clone().open().unwrap();
    let value = vec![0x5a; 73];
    let keys = (0..24)
        .map(|key| format!("key-{key:03}").into_bytes())
        .collect::<Vec<_>>();
    for key in &keys {
        assert_eq!(
            cache
                .put_in(namespace, key, &value, PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
    }
    cache.close().unwrap();

    // Index size is an open-time policy. Restoring into a smaller table can
    // replace entries while streaming the checkpoint, so accounting must
    // follow ApplyResult rather than summing every encoded checkpoint entry.
    let reopened = reopen(&config.with_index_slots(8));
    assert_eq!(reopened.inner.index.snapshot_scan_count(), 0);
    let hit_count = keys
        .iter()
        .filter(|key| reopened.get_in(namespace, key).unwrap().as_deref() == Some(value.as_slice()))
        .count();
    let visible_values = {
        let state = reopened.inner.state.lock().unwrap();
        reopened
            .inner
            .index
            .value_len(state.superblock.epoch_start_seqno)
    };
    assert!(hit_count > 0);
    assert_eq!(hit_count, visible_values);
    let per_entry = encoded_value_len(namespace, &keys[0], &value);
    let expected = (hit_count as u64) * per_entry;
    assert_eq!(total_valid_bytes(&reopened), expected);
    assert_eq!(
        reopened.namespace_stats(namespace).unwrap().live_bytes,
        expected
    );
    reopened.close().unwrap();
}

#[test]
fn sealed_hit_gets_only_one_completed_second_chance_and_preserves_ttl() {
    let file = TestFile::new("second-chance");
    let expires_at = now_unix_ms() + 60_000;
    let hot_value = vec![0xa5; 128];
    let (_config, cache, hash, before) =
        open_with_sealed_hot(&file.0, b"hot", &hot_value, Some(expires_at));
    let after = complete_second_chance(&cache, b"hot", &hot_value, hash);
    assert!(after.seqno > before.seqno);
    assert_ne!(after.location, before.location);
    assert_ne!(after.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    assert_eq!(after.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);

    let queued_after_completion = cache.stats().reinsert_queued;
    for _ in 0..16 {
        assert_eq!(cache.get(b"hot").unwrap(), Some(hot_value.clone()));
    }
    std::thread::sleep(Duration::from_millis(20));
    let stats = cache.stats();
    assert_eq!(stats.reinsert_queued, queued_after_completion);
    assert_eq!(stats.reinsert_completed, 1);

    let (superblock, current) = {
        let state = cache.inner.state.lock().unwrap();
        (
            state.superblock,
            cache
                .inner
                .index
                .get(hash, state.superblock.epoch_start_seqno)
                .unwrap(),
        )
    };
    let mut encoded_header = [0_u8; RECORD_HEADER_SIZE];
    let backend = FileBackend::open(&file.0).unwrap();
    read_exact_at(
        &backend,
        &mut encoded_header,
        region_base(&superblock, current.location.region_id()).unwrap()
            + u64::from(current.location.offset()),
    )
    .unwrap();
    assert_eq!(
        RecordHeader::decode(&encoded_header).unwrap().expires_at,
        expires_at
    );
    cache.close().unwrap();
}

#[test]
fn dirty_reopen_keeps_used_clears_pending_and_does_not_requeue_reinsertion() {
    let file = TestFile::new("second-chance-dirty-reopen");
    let hot_value = vec![0x6b; 128];
    let (config, cache, hash, before) = open_with_sealed_hot(&file.0, b"hot", &hot_value, None);
    let reinserted = complete_second_chance(&cache, b"hot", &hot_value, hash);
    assert!(reinserted.seqno > before.seqno);
    assert_ne!(reinserted.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    assert_eq!(reinserted.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);

    // Seal the reinsertion's Region so a missing USED bit would schedule a
    // second copy after restart, then intentionally leave the cache dirty.
    cache
        .put("post-fill", vec![4_u8; 7_500], PutOptions::default())
        .unwrap();
    cache
        .put("post-rotate", vec![5_u8; 3_000], PutOptions::default())
        .unwrap();
    {
        let state = cache.inner.state.lock().unwrap();
        assert!(!state.superblock.clean);
        assert_eq!(
            state.regions[reinserted.location.region_id() as usize]
                .header
                .state,
            RegionState::Sealed
        );
    }
    drop(cache);

    let reopened = reopen(&config);
    let recovered = {
        let state = reopened.inner.state.lock().unwrap();
        let entry = reopened
            .inner
            .index
            .get(hash, state.superblock.epoch_start_seqno)
            .unwrap();
        assert_eq!(
            state.regions[entry.location.region_id() as usize]
                .header
                .state,
            RegionState::Sealed
        );
        entry
    };
    assert_ne!(recovered.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    assert_eq!(recovered.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);
    let queued = reopened.stats().reinsert_queued;
    for _ in 0..16 {
        assert_eq!(reopened.get(b"hot").unwrap(), Some(hot_value.clone()));
    }
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(reopened.stats().reinsert_queued, queued);
    reopened.close().unwrap();
}

#[test]
fn checkpoint_drops_pending_bit_and_reopen_can_schedule_the_entry_again() {
    let file = TestFile::new("second-chance-pending-checkpoint");
    let hot_value = vec![0x37; 128];
    let (config, cache, hash, entry) = open_with_sealed_hot(&file.0, b"hot", &hot_value, None);
    assert!(cache.inner.index.mark_second_chance_if(
        hash,
        entry.location,
        entry.seqno,
        entry.namespace_id,
    ));
    let marked = {
        let state = cache.inner.state.lock().unwrap();
        cache
            .inner
            .index
            .get(hash, state.superblock.epoch_start_seqno)
            .unwrap()
    };
    assert_ne!(marked.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);
    assert_eq!(marked.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    cache.flush().unwrap();
    drop(cache);

    let reopened = reopen(&config);
    let recovered = {
        let state = reopened.inner.state.lock().unwrap();
        reopened
            .inner
            .index
            .get(hash, state.superblock.epoch_start_seqno)
            .unwrap()
    };
    assert_eq!(recovered.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);
    assert_eq!(recovered.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    let queued = reopened.stats().reinsert_queued;
    let reinserted = complete_second_chance(&reopened, b"hot", &hot_value, hash);
    assert!(reopened.stats().reinsert_queued > queued);
    assert_ne!(reinserted.flags & INDEX_FLAG_SECOND_CHANCE_USED, 0);
    assert_eq!(reinserted.flags & INDEX_FLAG_SECOND_CHANCE_PENDING, 0);
    reopened.close().unwrap();
}

#[test]
fn unconfigured_namespace_can_be_removed_and_stays_deleted_when_reconfigured() {
    let file = TestFile::new("remove-unconfigured-namespace");
    let configured = config(&file.0).with_namespace(NamespaceConfig::new(7));
    let cache = configured.clone().open().unwrap();
    assert_eq!(
        cache
            .put_in(7, "key", "value", PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    cache.close().unwrap();

    let unconfigured = config(&file.0).open().unwrap();
    assert_eq!(unconfigured.get_in(7, b"key").unwrap(), None);
    assert_eq!(
        unconfigured.remove_in(7, b"key").unwrap(),
        RemoveOutcome::Removed
    );
    unconfigured.close().unwrap();

    let reopened = configured.open().unwrap();
    assert_eq!(reopened.get_in(7, b"key").unwrap(), None);
    reopened.close().unwrap();
}

#[test]
fn coalesced_lane_rejects_only_the_namespace_over_capacity() {
    let file = TestFile::new("coalesced-namespace-capacity");
    let config = config(&file.0)
        .with_submission_queue_depths(4, 4)
        .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(1024))
        .with_namespace(NamespaceConfig::new(8).with_capacity_bytes(1));
    let (backend, gate) = GatedRecordBackend::open(&file.0).unwrap();
    let cache = DiskCache::open_with_backend(config, Box::new(backend)).unwrap();
    gate.arm();

    let blocker_cache = cache.clone();
    let blocker =
        std::thread::spawn(move || blocker_cache.put("blocker", "value", PutOptions::default()));
    assert!(gate.wait_for_entries(1, Duration::from_secs(3)));

    let good_cache = cache.clone();
    let good =
        std::thread::spawn(move || good_cache.put_in(7, "good", "value", PutOptions::default()));
    assert!(wait_until(Duration::from_secs(3), || {
        cache.stats().write_queue_depth >= 2
    }));
    // Let the first queued producer finish its tiny buffer/send path before
    // placing the over-capacity request behind it on the same lane.
    for _ in 0..64 {
        std::thread::yield_now();
    }
    let bad_cache = cache.clone();
    let bad =
        std::thread::spawn(move || bad_cache.put_in(8, "bad", "value", PutOptions::default()));
    let all_queued = wait_until(Duration::from_secs(3), || {
        cache.stats().write_queue_depth >= 3
    });

    // Always open the gate before assertions or joins so a regression cannot
    // strand the append/engine workers.
    gate.release();
    let blocker_result = blocker.join().unwrap();
    let good_result = good.join().unwrap();
    let bad_result = bad.join().unwrap();

    assert!(
        all_queued,
        "both namespace puts did not enter the append lane"
    );
    assert_eq!(blocker_result.unwrap(), PutOutcome::Stored);
    assert_eq!(good_result.unwrap(), PutOutcome::Stored);
    assert_eq!(
        bad_result.unwrap(),
        PutOutcome::Rejected(RejectReason::NamespaceCapacityExceeded)
    );
    assert_eq!(
        gate.entered(),
        2,
        "the rejected put must not reach record I/O"
    );
    assert_eq!(cache.get_in(7, b"good").unwrap(), Some(b"value".to_vec()));
    assert_eq!(cache.get_in(8, b"bad").unwrap(), None);
    cache.close().unwrap();
}

#[test]
fn forced_reclaim_allows_large_retry_when_active_is_below_trigger() {
    let file = TestFile::new("forced-reclaim");
    let region_size = 16 * 1024_u64;
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 3 * region_size)
        .with_region_size(region_size)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(11_000)
        .with_submission_queue_depths(2, 2)
        .with_checkpoint_interval_bytes(0)
        .with_reclaim_mode(ReclaimMode::SecondChance);
    let cache = config.open().unwrap();

    cache
        .put("a", vec![1_u8; 8_000], PutOptions::default())
        .unwrap();
    cache
        .put("rotate0", vec![2_u8; 5_000], PutOptions::default())
        .unwrap();
    cache
        .put("b", vec![3_u8; 6_000], PutOptions::default())
        .unwrap();
    cache
        .put("rotate1", vec![4_u8; 2_000], PutOptions::default())
        .unwrap();

    let large = vec![5_u8; 10_800];
    {
        let state = cache.inner.state.lock().unwrap();
        assert!(
            state
                .regions
                .iter()
                .all(|region| region.header.state != RegionState::Free)
        );
        let active = state.active_regions[0] as usize;
        let usable = region_size - REGION_HEADER_SIZE as u64;
        let trigger = REGION_HEADER_SIZE as u64
            + usable * RECLAIM_TRIGGER_NUMERATOR / RECLAIM_TRIGGER_DENOMINATOR;
        assert!(state.regions[active].used < trigger);
        assert!(
            u64::from(RecordHeader::aligned_len(b"large".len(), large.len()).unwrap())
                > region_size - state.regions[active].used
        );
    }

    assert_eq!(
        cache.put("large", &large, PutOptions::default()).unwrap(),
        PutOutcome::Rejected(RejectReason::ReclaimBacklog)
    );
    assert!(wait_until(Duration::from_secs(3), || {
        cache.stats().background_regions_reclaimed >= 1
    }));
    assert_eq!(
        cache.put("large", &large, PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.get(b"large").unwrap(), Some(large));
    cache.close().unwrap();
}

#[test]
fn fresh_format_metadata_counts_against_the_daily_host_write_budget() {
    let file = TestFile::new("fresh-host-write-budget");
    let cache = config(&file.0)
        .with_daily_host_write_budget(1)
        .open()
        .unwrap();
    let region_count = cache.inner.state.lock().unwrap().regions.len() as u64;
    assert!(
        cache.host_write_stats().metadata_bytes >= (region_count + 4) * 4096,
        "preallocation is not a host write, but initialized metadata pages are"
    );
    assert_eq!(
        cache.put("first", "value", PutOptions::default()).unwrap(),
        PutOutcome::Rejected(RejectReason::DailyWriteBudgetExceeded)
    );
    cache.close().unwrap();
}

fn assert_background_reclaim_prepares_fifo_victim(name: &str, reclaim_mode: ReclaimMode) {
    let file = TestFile::new(name);
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 3 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(256)
        .with_max_value_size(9 * 1024)
        .with_submission_queue_depths(2, 2)
        .with_checkpoint_interval_bytes(0)
        .with_reclaim_mode(reclaim_mode);
    let cache = config.open().unwrap();

    assert_eq!(
        cache
            .put("a", vec![1_u8; 8_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("b", vec![2_u8; 4_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("c", vec![3_u8; 8_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("d", vec![4_u8; 2_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("e", vec![5_u8; 8_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("f", vec![6_u8; 2_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert!(wait_until(Duration::from_secs(3), || {
        cache.stats().background_regions_reclaimed == 1
    }));

    assert_eq!(
        cache
            .put("g", vec![7_u8; 2_000], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(
        cache
            .put("h", vec![8_u8; 100], PutOptions::default())
            .unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.get(b"h").unwrap(), Some(vec![8_u8; 100]));
    let stats = cache.stats();
    assert_eq!(stats.reclaim_backlog_rejections, 0);
    assert!(stats.regions_reused >= 1);
    cache.close().unwrap();
}

#[test]
fn second_chance_background_reclaim_prepares_the_strict_fifo_victim() {
    assert_background_reclaim_prepares_fifo_victim(
        "second-chance-background-reclaim",
        ReclaimMode::SecondChance,
    );
}

#[test]
fn fifo_background_reclaim_prepares_a_victim_before_rotation() {
    assert_background_reclaim_prepares_fifo_victim("fifo-background-reclaim", ReclaimMode::Fifo);
}

#[test]
fn minimum_fifo_layout_keeps_its_only_victim_on_the_synchronous_path() {
    let file = TestFile::new("minimum-fifo-background-reserve");
    let config = CacheConfig::new(&file.0, DATA_OFFSET + 2 * 16 * 1024)
        .with_region_size(16 * 1024)
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(2 * 1024)
        .with_submission_queue_depths(2, 2)
        .with_checkpoint_interval_bytes(0);
    let cache = config.clone().open().unwrap();
    let value = vec![b'x'; 1536];
    for index in 0..24 {
        cache
            .put(format!("key-{index:02}"), &value, PutOptions::default())
            .unwrap();
    }
    let stats = cache.stats();
    assert!(stats.regions_reused > 0);
    assert_eq!(stats.background_regions_reclaimed, 0);
    assert_eq!(stats.reclaim_backlog_rejections, 0);
    cache.close().unwrap();

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key-23").unwrap(), Some(value));
    reopened.close().unwrap();
}
