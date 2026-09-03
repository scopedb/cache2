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

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(not(target_os = "linux"))]
use cache2::IoMode;
use cache2::{
    CacheBuilder, CacheHealth, CacheTier, ErrorKind, ErrorOperation, IoEngine, L1EvictionPolicy,
    RuntimeConfig, StartupMode, StaticConfig,
};

static NEXT_FILE: AtomicU64 = AtomicU64::new(1);
type RuntimeConfigCase = (&'static str, fn(RuntimeConfig) -> RuntimeConfig);

fn test_static_config() -> StaticConfig {
    StaticConfig::new(3 * 512 * 1024)
        .with_region_size_bytes(512 * 1024)
        .with_expected_entries(3277)
}

fn test_runtime_config(workers: usize, append_shards: u32) -> RuntimeConfig {
    RuntimeConfig::default()
        .with_io_engine(IoEngine::Posix)
        .with_read_io_workers(workers)
        .with_write_io_workers(workers)
        .with_append_shards(append_shards)
        .with_l1_capacity_bytes(4 * 1024 * 1024)
        .with_managed_memory_limit_bytes(32 * 1024 * 1024)
        .with_write_flush_threshold_bytes(256 * 1024)
        .with_statistics(true)
}

struct TestCache {
    data: PathBuf,
}

impl TestCache {
    fn new(name: &str) -> Self {
        let id = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let data =
            std::env::temp_dir().join(format!("cache2-{name}-{}-{id}.cache", std::process::id()));
        Self { data }
    }

    fn config(&self, workers: usize) -> CacheBuilder {
        self.config_with_static(workers, test_static_config())
    }

    fn config_with_static(&self, workers: usize, static_config: StaticConfig) -> CacheBuilder {
        self.config_with_static_and_shards(workers, static_config, 2)
    }

    fn config_with_static_and_shards(
        &self,
        workers: usize,
        static_config: StaticConfig,
        append_shards: u32,
    ) -> CacheBuilder {
        CacheBuilder::from_static(&self.data, static_config)
            .with_runtime_config(test_runtime_config(workers, append_shards))
    }

    fn sidecar(&self, suffix: &str) -> PathBuf {
        PathBuf::from(format!("{}{suffix}", self.data.display()))
    }

    fn assert_absent(&self) {
        for path in [
            self.data.clone(),
            self.sidecar(".state"),
            self.sidecar(".image"),
            self.sidecar(".image.next"),
        ] {
            assert!(!path.exists(), "unexpected cache file: {}", path.display());
        }
    }
}

impl Drop for TestCache {
    fn drop(&mut self) {
        for path in [
            self.data.clone(),
            self.sidecar(".state"),
            self.sidecar(".image"),
            self.sidecar(".image.next"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn rewrite_page_version(path: &std::path::Path, offset: u64, version: u16) {
    const PAGE_BYTES: usize = 4096;
    const VERSION_OFFSET: usize = 8;
    const CRC_OFFSET: usize = PAGE_BYTES - 4;

    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    let mut page = [0_u8; PAGE_BYTES];
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.read_exact(&mut page).unwrap();
    page[VERSION_OFFSET..VERSION_OFFSET + 2].copy_from_slice(&version.to_le_bytes());
    page[CRC_OFFSET..].fill(0);
    let checksum = crc_fast::crc32_iscsi(&page);
    page[CRC_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&page).unwrap();
    file.sync_all().unwrap();
}

fn eventually_admitted<T>(mut put: impl FnMut() -> cache2::Result<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match put() {
            Ok(value) => return value,
            Err(error) if error.kind() == ErrorKind::Overloaded => {
                assert!(
                    Instant::now() < deadline,
                    "write buffer did not make progress"
                );
                std::thread::yield_now();
            }
            Err(error) => panic!("cache write failed: {error}"),
        }
    }
}

async fn completed_reclaim_snapshot(cache: &cache2::Cache) -> cache2::DetailedCacheSnapshot {
    for ordinal in 0_u64..128 {
        eventually_admitted(|| cache.put(ordinal.to_le_bytes(), vec![ordinal as u8; 8 * 1024]));
    }
    cache.drain().await.unwrap();

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let detailed = cache.detailed_snapshot().unwrap();
        let read = detailed.summary.io.read;
        let write = detailed.summary.io.write;
        let read_terminal = read
            .requests_succeeded
            .saturating_add(read.requests_cancelled)
            .saturating_add(read.requests_failed);
        let write_terminal = write
            .requests_succeeded
            .saturating_add(write.requests_cancelled)
            .saturating_add(write.requests_failed);
        if detailed.summary.reclaim.regions > 0
            && detailed.region.reclaiming_region_count == 0
            && read.requests_in_flight == 0
            && read.requests_submitted == read_terminal
            && write.requests_in_flight == 0
            && write.requests_submitted == write_terminal
        {
            return detailed;
        }
        assert!(Instant::now() < deadline, "reclaim did not make progress");
        thread::yield_now();
    }
}

#[test]
fn explicit_tokio_handle_works_from_a_runtime_without_time_enabled() {
    let files = TestCache::new("explicit-tokio-handle");
    let cache_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    let caller_runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let cache_handle = cache_runtime.handle().clone();

    caller_runtime.block_on(async {
        let cache = files
            .config(1)
            .with_tokio_handle(cache_handle.clone())
            .open()
            .await
            .unwrap();
        cache.put("key", "value").unwrap();
        cache.drain().await.unwrap();
        cache.close_warm().await.unwrap();

        let reopened = files
            .config(1)
            .with_tokio_handle(cache_handle)
            .open()
            .await
            .unwrap();
        let value = reopened.get("key").await.unwrap().unwrap();
        assert_eq!(value.tier(), CacheTier::L2);
        assert_eq!(value.as_ref(), b"value");
        drop(value);
        reopened.close_fast().await.unwrap();
    });
}

#[tokio::test]
async fn fast_close_always_reopens_empty() {
    let files = TestCache::new("fast-close");
    let cache = files.config(1).open().await.unwrap();
    assert_eq!(cache.startup_mode(), StartupMode::Cold);
    cache.put("key", "value").unwrap();
    cache.drain().await.unwrap();
    assert_eq!(cache.get("key").await.unwrap().unwrap().as_ref(), b"value");
    cache.close_fast().await.unwrap();

    let reopened = files.config(7).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Cold);
    assert!(reopened.get("key").await.unwrap().is_none());
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn warm_close_with_retained_arc_fences_operations_and_recovers() {
    let files = TestCache::new("warm-close-retained-arc");
    let cache = Arc::new(files.config(2).open().await.unwrap());
    let retained = Arc::clone(&cache);

    cache.put("key", "value").unwrap();
    cache.close_warm().await.unwrap();

    assert!(cache.get("key").await.unwrap().is_none());
    assert!(retained.get("key").await.unwrap().is_none());
    for error in [
        retained.put("key", "replacement").unwrap_err(),
        retained.put_l2("key", "replacement").unwrap_err(),
        retained.delete("key").unwrap_err(),
    ] {
        assert_eq!(error.kind(), ErrorKind::Unavailable);
    }
    assert_eq!(
        retained.drain().await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        retained.snapshot().unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        retained.detailed_snapshot().unwrap_err().kind(),
        ErrorKind::Unavailable
    );
    assert_eq!(
        retained.close_fast().await.unwrap_err().kind(),
        ErrorKind::Unavailable
    );

    let reopened = files.config(2).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    assert_eq!(
        reopened.get("key").await.unwrap().unwrap().as_ref(),
        b"value"
    );
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn warm_close_fences_concurrent_arc_mutations() {
    let files = TestCache::new("warm-close-concurrent-mutations");
    let cache = Arc::new(files.config(2).open().await.unwrap());
    let writer_cache = Arc::clone(&cache);
    let ready = Arc::new(Barrier::new(2));
    let writer_ready = Arc::clone(&ready);
    let writer = thread::spawn(move || {
        writer_ready.wait();
        let mut accepted = Vec::new();
        for ordinal in 0_u64..256 {
            loop {
                match writer_cache.put(ordinal.to_le_bytes(), ordinal.to_le_bytes()) {
                    Ok(_) => {
                        accepted.push(ordinal);
                        break;
                    }
                    Err(error) if error.kind() == ErrorKind::Overloaded => thread::yield_now(),
                    Err(error) if error.kind() == ErrorKind::Unavailable => return accepted,
                    Err(error) => panic!("concurrent cache write failed: {error}"),
                }
            }
        }
        accepted
    });

    ready.wait();
    cache.close_warm().await.unwrap();
    let accepted = writer.join().unwrap();

    let reopened = files.config(2).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    for ordinal in accepted {
        assert_eq!(
            reopened
                .get(ordinal.to_le_bytes())
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            ordinal.to_le_bytes()
        );
    }
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn concurrent_open_reports_structured_busy_error() {
    let files = TestCache::new("concurrent-open");
    let cache = files.config(1).open().await.unwrap();

    let error = files.config(1).open().await.unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(error.operation(), ErrorOperation::Open);
    assert_eq!(error.io_kind(), std::io::ErrorKind::WouldBlock);

    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn l2_only_put_avoids_l1_until_the_first_demand_read() {
    let files = TestCache::new("l2-only-put");
    let cache = files.config(2).open().await.unwrap();

    let sequence = eventually_admitted(|| cache.put_l2("key", [7_u8; 16 * 1024]));
    assert_ne!(sequence, 0);
    assert_eq!(cache.detailed_snapshot().unwrap().l1.resident_entries, 0);
    cache.drain().await.unwrap();
    assert_eq!(cache.detailed_snapshot().unwrap().l1.resident_entries, 0);

    let first = cache.get("key").await.unwrap().unwrap();
    assert_eq!(first.tier(), CacheTier::L2);
    assert_eq!(first.as_ref(), &[7_u8; 16 * 1024]);
    drop(first);
    let second = cache.get("key").await.unwrap().unwrap();
    assert_eq!(second.tier(), CacheTier::L1);
    assert_eq!(second.as_ref(), &[7_u8; 16 * 1024]);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn l2_only_put_best_effort_invalidates_an_older_l1_value() {
    let files = TestCache::new("l2-only-replacement");
    let cache = files.config(2).open().await.unwrap();

    let old_sequence = eventually_admitted(|| cache.put("key", b"old"));
    let old = cache.get("key").await.unwrap().unwrap();
    assert_eq!(old.tier(), CacheTier::L1);
    drop(old);

    let new_value = vec![9_u8; 16 * 1024];
    let new_sequence = eventually_admitted(|| cache.put_l2("key", &new_value));
    assert!(new_sequence > old_sequence);
    assert_eq!(cache.detailed_snapshot().unwrap().l1.resident_entries, 0);
    cache.drain().await.unwrap();

    let current = cache.get("key").await.unwrap().unwrap();
    assert_eq!(current.tier(), CacheTier::L2);
    assert_eq!(current.as_ref(), new_value);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn l2_only_put_survives_warm_recovery() {
    let files = TestCache::new("l2-only-warm-recovery");
    let cache = files.config(2).open().await.unwrap();
    let expected = vec![7_u8; 16 * 1024];

    let sequence = eventually_admitted(|| cache.put_l2("key", &expected));
    assert_ne!(sequence, 0);
    cache.close_warm().await.unwrap();

    let reopened = files.config(3).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    assert_eq!(reopened.detailed_snapshot().unwrap().l1.resident_entries, 0);
    let value = reopened.get("key").await.unwrap().unwrap();
    assert_eq!(value.tier(), CacheTier::L2);
    assert_eq!(value.as_ref(), expected);
    drop(value);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn write_flush_threshold_does_not_cap_region_sized_staging() {
    let files = TestCache::new("reject-write-buffer-flush");
    let runtime = test_runtime_config(1, 2)
        .with_l1_capacity_bytes(1024 * 1024)
        .with_l1_shards(1);
    let cache = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    let first = vec![1_u8; 200 * 1024];
    let second = vec![2_u8; 200 * 1024];

    let first_seqno = cache.put("key", &first).unwrap();
    let second_seqno = cache.put("key", &second).unwrap();
    assert!(second_seqno > first_seqno);
    assert_eq!(cache.get("key").await.unwrap().unwrap().as_ref(), second);
    assert_eq!(cache.snapshot().unwrap().write_rejections, 0);
    assert_eq!(
        cache.detailed_snapshot().unwrap().write_buffer_rejections,
        0
    );
    cache.drain().await.unwrap();
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn l1_bypass_may_remain_stale_after_region_completion() {
    let files = TestCache::new("l1-bypass-publication");
    let runtime = test_runtime_config(2, 2)
        .with_l1_capacity_bytes(512)
        .with_l1_shards(1)
        .with_write_flush_threshold_bytes(128 * 1024);
    let cache = files
        .config(2)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();

    cache.put("key", "old").unwrap();
    cache.drain().await.unwrap();
    let replacement = vec![9_u8; 1024];
    cache.put("key", &replacement).unwrap();
    assert_eq!(cache.snapshot().unwrap().l1_bypasses, 1);
    let stale = cache.get("key").await.unwrap().unwrap();
    assert_eq!(stale.as_ref(), b"old");
    drop(stale);

    cache.drain().await.unwrap();
    let value = cache.get("key").await.unwrap().unwrap();
    assert!(value.as_ref() == b"old" || value.as_ref() == replacement);
    drop(value);
    cache.close_fast().await.unwrap();
}

#[cfg(not(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
)))]
#[tokio::test]
async fn unavailable_io_engine_is_rejected_before_file_creation() {
    let files = TestCache::new("unavailable-io-engine");
    let runtime = test_runtime_config(1, 2)
        .with_io_engine(IoEngine::IoUring)
        .with_write_flush_threshold_bytes(128 * 1024)
        .with_statistics(false);

    let error = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), ErrorOperation::Open);
    assert_eq!(error.io_kind(), std::io::ErrorKind::Unsupported);
    files.assert_absent();
}

#[cfg(not(target_os = "linux"))]
#[tokio::test]
async fn unavailable_direct_io_is_rejected_before_file_creation() {
    let files = TestCache::new("unavailable-direct-io");
    let runtime = test_runtime_config(1, 2)
        .with_io_mode(IoMode::Direct)
        .with_write_flush_threshold_bytes(128 * 1024)
        .with_statistics(false);

    let error = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Unsupported);
    assert_eq!(error.operation(), ErrorOperation::Open);
    assert_eq!(error.io_kind(), std::io::ErrorKind::Unsupported);
    files.assert_absent();
}

#[tokio::test]
async fn latest_put_survives_warm_recovery() {
    let files = TestCache::new("latest-warm-recovery");
    let cache = files.config(3).open().await.unwrap();
    for version in 0_u8..64 {
        eventually_admitted(|| cache.put("key", [version; 1024]));
    }
    cache.close_warm().await.unwrap();

    let reopened = files.config(5).open().await.unwrap();
    let value = reopened.get("key").await.unwrap().unwrap();
    assert_eq!(value.tier(), CacheTier::L2);
    assert_eq!(value.as_ref(), &[63; 1024]);
    drop(value);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn runtime_config_can_change_across_a_warm_reopen() {
    let files = TestCache::new("warm-close");
    let cache = files.config(1).open().await.unwrap();
    cache.put("key", vec![7_u8; 16 * 1024]).unwrap();
    cache.drain().await.unwrap();
    cache.close_warm().await.unwrap();

    let retuned = test_runtime_config(2, 2)
        .with_read_io_workers(7)
        .with_read_io_wait_timeout(Duration::from_millis(10))
        .with_reclaim_workers(2)
        .with_l1_capacity_bytes(2 * 1024 * 1024)
        .with_l1_eviction_policy(L1EvictionPolicy::S3Fifo)
        .with_l1_shards(7)
        .with_write_flush_threshold_bytes(64 * 1024)
        .with_statistics(false);
    let reopened = files
        .config(7)
        .with_runtime_config(retuned)
        .open()
        .await
        .unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    let first = reopened.get("key").await.unwrap().unwrap();
    assert_eq!(first.tier(), CacheTier::L2);
    assert_eq!(first.as_ref(), vec![7_u8; 16 * 1024]);
    drop(first);
    assert!(!reopened.snapshot().unwrap().statistics_enabled);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn append_shards_rebind_across_warm_reopens() {
    let files = TestCache::new("append-shards");
    let static_config = test_static_config();

    let cache = files
        .config_with_static_and_shards(1, static_config.clone(), 1)
        .open()
        .await
        .unwrap();
    cache.put("old-key", [7_u8; 1024]).unwrap();
    cache.close_warm().await.unwrap();

    let reopened = files
        .config_with_static_and_shards(2, static_config.clone(), 2)
        .open()
        .await
        .unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    assert_eq!(
        reopened.get("old-key").await.unwrap().unwrap().as_ref(),
        &[7_u8; 1024]
    );
    reopened.put("new-key", [9_u8; 1024]).unwrap();
    reopened.close_warm().await.unwrap();

    let recovered = files
        .config_with_static_and_shards(2, static_config.clone(), 2)
        .open()
        .await
        .unwrap();
    assert_eq!(recovered.startup_mode(), StartupMode::Warm);
    assert_eq!(
        recovered.get("new-key").await.unwrap().unwrap().as_ref(),
        &[9_u8; 1024]
    );
    recovered.close_warm().await.unwrap();

    let shrunk = files
        .config_with_static_and_shards(1, static_config.clone(), 1)
        .open()
        .await
        .unwrap();
    assert_eq!(shrunk.startup_mode(), StartupMode::Warm);
    assert_eq!(
        shrunk.get("old-key").await.unwrap().unwrap().as_ref(),
        &[7_u8; 1024]
    );
    assert_eq!(
        shrunk.get("new-key").await.unwrap().unwrap().as_ref(),
        &[9_u8; 1024]
    );
    shrunk.put("shrunk-key", [11_u8; 1024]).unwrap();
    shrunk.close_warm().await.unwrap();

    let stable = files
        .config_with_static_and_shards(1, static_config, 1)
        .open()
        .await
        .unwrap();
    assert_eq!(stable.startup_mode(), StartupMode::Warm);
    assert_eq!(
        stable.get("shrunk-key").await.unwrap().unwrap().as_ref(),
        &[11_u8; 1024]
    );
    stable.close_fast().await.unwrap();
}

#[tokio::test]
async fn delete_is_sequenced_and_warm_recoverable() {
    let files = TestCache::new("delete-warm-recovery");
    let cache = files.config(3).open().await.unwrap();
    let put_sequence = cache.put("key", "one").unwrap();
    cache.drain().await.unwrap();
    let records_before_delete = cache
        .detailed_snapshot()
        .unwrap()
        .region
        .physical_record_count;

    let delete_sequence = cache.delete("key").unwrap();
    assert!(delete_sequence > put_sequence);
    cache.drain().await.unwrap();
    assert!(cache.get("key").await.unwrap().is_none());
    assert_eq!(
        cache
            .detailed_snapshot()
            .unwrap()
            .region
            .physical_record_count,
        records_before_delete
    );
    cache.close_warm().await.unwrap();

    let reopened = files.config(5).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    assert!(reopened.get("key").await.unwrap().is_none());

    let replacement_sequence = reopened.put("key", "new").unwrap();
    assert!(replacement_sequence > delete_sequence);
    reopened.drain().await.unwrap();
    assert_eq!(reopened.get("key").await.unwrap().unwrap().as_ref(), b"new");
    reopened.close_fast().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_mixed_mutations_never_return_wrong_key_or_future_values() {
    const WRITERS: usize = 4;
    const READERS: usize = 4;
    const KEY_COUNT: usize = 64;
    const WRITES_PER_CLIENT: usize = 256;
    const VALUE_SIZES: [usize; 3] = [256, 4 * 1024, 16 * 1024];
    const HEADER_BYTES: usize = 16;

    let files = TestCache::new("concurrent-mixed");
    let cache = files.config(4).open().await.unwrap();
    let keys: Vec<_> = (0..KEY_COUNT)
        .map(|key| {
            format!("mixed-key-{key:04}")
                .into_bytes()
                .into_boxed_slice()
        })
        .collect();
    let announced: Vec<_> = (0..KEY_COUNT).map(|_| AtomicU64::new(0)).collect();
    let writers_left = AtomicUsize::new(WRITERS);
    let start = AtomicBool::new(false);
    let hits = AtomicU64::new(0);
    let runtime = tokio::runtime::Handle::current();

    thread::scope(|scope| {
        for writer in 0..WRITERS {
            let cache = &cache;
            let keys = &keys;
            let announced = &announced;
            let writers_left = &writers_left;
            let start = &start;
            scope.spawn(move || {
                while !start.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                let mut value = vec![0_u8; *VALUE_SIZES.iter().max().unwrap()];
                for ordinal in 0..WRITES_PER_CLIENT {
                    let key_index = writer + (ordinal % (KEY_COUNT / WRITERS)) * WRITERS;
                    let version = u64::try_from(ordinal + 1).unwrap();
                    let value_len = VALUE_SIZES[(ordinal + key_index) % VALUE_SIZES.len()];
                    let pattern = (version ^ key_index as u64) as u8;
                    value[..value_len].fill(pattern);
                    value[..8].copy_from_slice(&version.to_le_bytes());
                    value[8..HEADER_BYTES].copy_from_slice(&(key_index as u64).to_le_bytes());
                    announced[key_index].store(version, Ordering::SeqCst);
                    if ordinal % 2 == 0 {
                        eventually_admitted(|| cache.put(&keys[key_index], &value[..value_len]));
                    } else {
                        eventually_admitted(|| cache.put_l2(&keys[key_index], &value[..value_len]));
                    }
                    if ordinal % 11 == 10 {
                        eventually_admitted(|| cache.delete(&keys[key_index]));
                    }
                }
                writers_left.fetch_sub(1, Ordering::Release);
            });
        }

        for reader in 0..READERS {
            let cache = &cache;
            let keys = &keys;
            let announced = &announced;
            let writers_left = &writers_left;
            let start = &start;
            let hits = &hits;
            let runtime = runtime.clone();
            scope.spawn(move || {
                while !start.load(Ordering::Acquire) {
                    thread::yield_now();
                }
                let mut ordinal = reader;
                while writers_left.load(Ordering::Acquire) != 0 {
                    let key_index = ordinal % KEY_COUNT;
                    ordinal = ordinal.wrapping_add(READERS);
                    let Some(value) = runtime.block_on(cache.get(&keys[key_index])).unwrap() else {
                        continue;
                    };
                    assert!(value.len() >= HEADER_BYTES);
                    let version = u64::from_le_bytes(value[..8].try_into().unwrap());
                    let observed_key =
                        u64::from_le_bytes(value[8..HEADER_BYTES].try_into().unwrap());
                    let latest = announced[key_index].load(Ordering::SeqCst);
                    assert_eq!(observed_key, key_index as u64);
                    assert!(version != 0 && version <= latest);
                    let writer_ordinal = usize::try_from(version - 1).unwrap();
                    let expected_len =
                        VALUE_SIZES[(writer_ordinal + key_index) % VALUE_SIZES.len()];
                    let pattern = (version ^ key_index as u64) as u8;
                    assert_eq!(value.len(), expected_len);
                    assert!(value[HEADER_BYTES..].iter().all(|byte| *byte == pattern));
                    hits.fetch_add(1, Ordering::Relaxed);
                }
            });
        }
        start.store(true, Ordering::Release);
    });

    cache.drain().await.unwrap();
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::Running);
    assert_eq!(snapshot.puts, (WRITERS * WRITES_PER_CLIENT) as u64);
    assert_eq!(
        snapshot.deletes,
        (WRITERS * (WRITES_PER_CLIENT / 11)) as u64
    );
    assert_eq!(snapshot.io_failures, 0);
    assert!(hits.load(Ordering::Relaxed) > 0);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn invalid_runtime_config_is_rejected_before_file_creation() {
    let cases: [RuntimeConfigCase; 14] = [
        ("zero-append-shards", |config| config.with_append_shards(0)),
        ("too-many-append-shards", |config| {
            config.with_append_shards(257)
        }),
        ("zero-reclaim-workers", |config| {
            config.with_reclaim_workers(0)
        }),
        ("too-many-reclaim-workers", |config| {
            config.with_reclaim_workers(3)
        }),
        ("zero-read-workers", |config| config.with_read_io_workers(0)),
        ("zero-write-workers", |config| {
            config.with_write_io_workers(0)
        }),
        ("too-many-read-workers", |config| {
            config.with_read_io_workers(4097)
        }),
        ("too-many-write-workers", |config| {
            config.with_write_io_workers(4097)
        }),
        ("excessive-read-wait", |config| {
            config.with_read_io_wait_timeout(Duration::from_secs(5) + Duration::from_nanos(1))
        }),
        ("l1-exceeds-budget", |config| {
            config
                .with_l1_capacity_bytes(64 * 1024 * 1024)
                .with_managed_memory_limit_bytes(32 * 1024 * 1024)
        }),
        ("fixed-plan-exceeds-budget", |config| {
            config
                .with_io_engine(IoEngine::Posix)
                .with_read_io_workers(2)
                .with_write_io_workers(2)
                .with_l1_capacity_bytes(0)
                .with_managed_memory_limit_bytes(2 * 1024 * 1024)
                .with_write_flush_threshold_bytes(128 * 1024)
        }),
        ("zero-l1-shards", |config| config.with_l1_shards(0)),
        ("unaligned-write-flush-threshold", |config| {
            config.with_write_flush_threshold_bytes(4097)
        }),
        ("oversized-write-flush-threshold", |config| {
            config.with_write_flush_threshold_bytes(4 * 1024 * 1024 + 4096)
        }),
    ];

    for (case, configure) in cases {
        let files = TestCache::new(case);
        let error = files
            .config(2)
            .with_runtime_config(configure(RuntimeConfig::default().with_append_shards(2)))
            .open()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{case}");
        assert_eq!(error.operation(), ErrorOperation::Open, "{case}");
        assert_eq!(error.io_kind(), std::io::ErrorKind::InvalidInput, "{case}");
        files.assert_absent();
    }
}

#[tokio::test]
async fn public_key_and_record_size_limits_are_enforced() {
    let files = TestCache::new("entry-size-errors");
    let cache = files.config(1).open().await.unwrap();
    let oversized_key = vec![0_u8; 4 * 1024 + 1];
    let oversized_value = vec![0_u8; 512 * 1024];

    let error = cache.put(&oversized_key, b"value").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::Put);
    assert_eq!(error.io_kind(), std::io::ErrorKind::InvalidInput);

    let error = cache.put(b"too-large", &oversized_value).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::Put);

    let error = cache.put_l2(&oversized_key, b"value").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::PutL2);

    let error = cache.delete(&oversized_key).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::Delete);
    assert!(cache.get(&oversized_key).await.unwrap().is_none());
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn region_sized_records_remain_l2_only_and_warm_recover() {
    let files = TestCache::new("region-sized-record");
    let cache = files.config(1).open().await.unwrap();
    let large_value = vec![7_u8; 512 * 1024 - 64];

    cache.put(b"large", &large_value).unwrap();
    cache.drain().await.unwrap();
    for _ in 0..2 {
        let value = cache.get(b"large").await.unwrap().unwrap();
        assert_eq!(value.tier(), CacheTier::L2);
        assert_eq!(value.as_ref(), large_value);
    }
    cache.close_warm().await.unwrap();

    let reopened = files.config(1).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    let value = reopened.get(b"large").await.unwrap().unwrap();
    assert_eq!(value.tier(), CacheTier::L2);
    assert_eq!(value.as_ref(), large_value);
    drop(value);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn cold_start_removes_stale_recovery_files() {
    let files = TestCache::new("stale-recovery-files");
    let image = files.sidecar(".image");
    let temporary = files.sidecar(".image.next");
    std::fs::write(&image, b"stale image").unwrap();
    std::fs::write(&temporary, b"stale temporary image").unwrap();

    let cache = files.config(2).open().await.unwrap();
    assert!(!image.exists());
    assert!(!temporary.exists());
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn minimum_region_stores_its_first_record_at_offset_zero_and_recovers() {
    let files = TestCache::new("minimum-region");
    let static_config = StaticConfig::new(2 * 4096)
        .with_region_size_bytes(4096)
        .with_expected_entries(51);
    let value = vec![0x5a; 128];

    let cache = files
        .config_with_static_and_shards(1, static_config.clone(), 1)
        .open()
        .await
        .unwrap();
    cache.put("first", &value).unwrap();
    cache.close_warm().await.unwrap();

    let recovered = files
        .config_with_static_and_shards(1, static_config, 1)
        .open()
        .await
        .unwrap();
    assert_eq!(recovered.startup_mode(), StartupMode::Warm);
    assert_eq!(
        recovered.get("first").await.unwrap().unwrap().as_ref(),
        value
    );
    recovered.close_fast().await.unwrap();
}

#[tokio::test]
async fn reported_peak_disk_bytes_covers_atomic_warm_publication() {
    let files = TestCache::new("disk-bound");
    let static_config = test_static_config();
    let peak_disk_bytes = static_config.peak_disk_bytes().unwrap();

    let cache = files
        .config_with_static(2, static_config)
        .open()
        .await
        .unwrap();
    cache.put("key", vec![7_u8; 16 * 1024]).unwrap();
    cache.close_warm().await.unwrap();

    let image = files.sidecar(".image");
    let temporary = files.sidecar(".image.next");
    std::fs::copy(&image, &temporary).unwrap();
    let logical_bytes = [
        files.data.clone(),
        files.sidecar(".state"),
        image,
        temporary,
    ]
    .into_iter()
    .map(|path| std::fs::metadata(path).unwrap().len())
    .sum::<u64>();

    assert_eq!(logical_bytes, peak_disk_bytes);
}

#[tokio::test]
async fn detailed_snapshot_reports_bounded_resource_state() {
    let files = TestCache::new("resource-snapshot");
    let expected_disk_peak = test_static_config().peak_disk_bytes().unwrap();
    let cache = files.config(4).open().await.unwrap();
    let detailed = completed_reclaim_snapshot(&cache).await;
    let resources = detailed.summary;
    assert_eq!(resources.health, CacheHealth::Running);
    assert!(resources.statistics_enabled);
    assert!(resources.reclaim.regions > 0);
    assert_eq!(resources.managed_memory_limit_bytes, 32 * 1024 * 1024);
    assert!(resources.managed_memory_bytes <= resources.managed_memory_limit_bytes);
    assert!(resources.managed_memory_peak_bytes >= resources.managed_memory_bytes);
    assert!(resources.managed_memory_peak_bytes <= resources.managed_memory_limit_bytes);
    assert_eq!(resources.logical_disk_peak_bytes, expected_disk_peak);

    assert!(detailed.l1.entry_capacity > 0);
    assert!(detailed.l1.resident_entries <= detailed.l1.entry_capacity);
    assert!(detailed.l1.resident_bytes <= 4 * 1024 * 1024);
    assert_eq!(detailed.l1.retained_bytes, 0);
    assert!(detailed.l1.metadata_bytes > 0);
    assert_eq!(detailed.index.slot_capacity, 6554);
    assert_eq!(
        detailed.index.physical_value_slots + detailed.index.empty_slots,
        detailed.index.slot_capacity
    );
    assert_eq!(detailed.region.capacity_bytes, 3 * 512 * 1024);
    assert_eq!(
        detailed.region.active_region_count
            + detailed.region.free_region_count
            + detailed.region.sealed_region_count
            + detailed.region.reclaiming_region_count,
        3
    );
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn detailed_snapshot_reports_reclaim_and_io_activity() {
    let files = TestCache::new("reclaim-snapshot");
    let cache = files.config(4).open().await.unwrap();
    let detailed = completed_reclaim_snapshot(&cache).await;
    let resources = detailed.summary;

    assert!(resources.region_rotations > 0);
    assert!(resources.reclaim.regions > 0);
    assert!(resources.reclaim.bytes_read > 0);
    assert!(resources.reclaim.records_scanned > 0);
    assert!(resources.reclaim.index_entries_removed <= resources.reclaim.records_scanned);
    assert_eq!(resources.io.read.requests_in_flight, 0);
    assert_eq!(resources.io.write.requests_in_flight, 0);
    assert_eq!(
        resources.io.read.requests_submitted,
        resources
            .io
            .read
            .requests_succeeded
            .saturating_add(resources.io.read.requests_cancelled)
            .saturating_add(resources.io.read.requests_failed)
    );
    assert_eq!(
        resources.io.write.requests_submitted,
        resources
            .io
            .write
            .requests_succeeded
            .saturating_add(resources.io.write.requests_cancelled)
            .saturating_add(resources.io.write.requests_failed)
    );
    assert!(
        resources
            .io
            .read
            .direct
            .operations
            .saturating_add(resources.io.read.buffered.operations)
            .saturating_add(resources.io.write.direct.operations)
            .saturating_add(resources.io.write.buffered.operations)
            > 0
    );
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn cache_snapshot_reports_tier_activity_and_resets_on_open() {
    let files = TestCache::new("activity-snapshot");
    let cache = files.config(2).open().await.unwrap();
    cache.put("key", "value").unwrap();
    assert_eq!(
        cache.get("key").await.unwrap().unwrap().tier(),
        CacheTier::L1
    );
    assert!(cache.get("missing").await.unwrap().is_none());
    cache.drain().await.unwrap();

    let before_close = cache.snapshot().unwrap();
    assert_eq!(before_close.health, CacheHealth::Running);
    assert_eq!(before_close.puts, 1);
    assert_eq!(before_close.deletes, 0);
    assert_eq!(before_close.written_bytes, 5);
    assert_eq!(before_close.l1_hits, 1);
    assert_eq!(before_close.l1_misses, 1);
    assert_eq!(before_close.l2_hits, 0);
    assert_eq!(before_close.l2_misses, 1);
    assert_eq!(before_close.l2_read_memory_misses, 0);
    assert_eq!(before_close.l2_read_busy_misses, 0);
    assert_eq!(before_close.l2_read_overloads, 0);
    assert_eq!(before_close.l2_read_wait_ns, 0);
    assert_eq!(before_close.served_bytes, 5);
    assert_eq!(before_close.l1_promotions, 0);
    assert_eq!(before_close.io_failures, 0);
    assert_ne!(before_close.metrics_epoch, 0);
    assert!(before_close.io.write.requests_submitted > 0);
    assert_eq!(
        before_close.io.write.requests_succeeded,
        before_close.io.write.requests_submitted
    );
    assert_eq!(before_close.io.write.requests_cancelled, 0);
    assert_eq!(before_close.io.write.requests_failed, 0);
    assert!(before_close.io.write.buffered.operations > 0);
    assert_eq!(before_close.io.write.direct.operations, 0);
    cache.close_warm().await.unwrap();

    let reopened = files.config(3).open().await.unwrap();
    let fresh = reopened.snapshot().unwrap();
    assert_eq!(fresh.puts, 0);
    assert_eq!(fresh.deletes, 0);
    assert_eq!(fresh.l1_hits, 0);
    assert_eq!(fresh.l2_hits, 0);
    assert_ne!(fresh.metrics_epoch, before_close.metrics_epoch);
    assert_eq!(fresh.io, cache2::CacheIoSnapshot::default());
    assert_eq!(
        reopened.get("key").await.unwrap().unwrap().tier(),
        CacheTier::L2
    );
    assert_eq!(
        reopened.get("key").await.unwrap().unwrap().tier(),
        CacheTier::L1
    );

    let warmed = reopened.snapshot().unwrap();
    assert_eq!(warmed.l1_hits, 1);
    assert_eq!(warmed.l1_misses, 1);
    assert_eq!(warmed.l2_hits, 1);
    assert_eq!(warmed.l2_misses, 0);
    assert_eq!(warmed.l2_read_memory_misses, 0);
    assert_eq!(warmed.l2_read_busy_misses, 0);
    assert_eq!(warmed.l2_read_overloads, 0);
    assert_eq!(warmed.l2_read_wait_ns, 0);
    assert_eq!(warmed.served_bytes, 10);
    assert_eq!(warmed.l1_promotions, 1);
    assert_eq!(warmed.io.read.requests_submitted, 1);
    assert_eq!(warmed.io.read.requests_succeeded, 1);
    assert_eq!(warmed.io.read.requests_cancelled, 0);
    assert_eq!(warmed.io.read.requests_failed, 0);
    assert!(warmed.io.read.buffered.operations > 0);
    assert_eq!(warmed.io.read.direct.operations, 0);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn buffered_l2_read_reports_the_size_class_upper_bound() {
    let files = TestCache::new("buffered-size-class-read");
    let static_config = test_static_config();
    let cache = files
        .config_with_static_and_shards(1, static_config.clone(), 1)
        .open()
        .await
        .unwrap();
    let value = vec![0x5a; 1000];
    cache.put("key-1", &value).unwrap();
    cache.put("key-2", &value).unwrap();
    cache.close_warm().await.unwrap();

    let reopened = files
        .config_with_static_and_shards(1, static_config, 1)
        .open()
        .await
        .unwrap();
    let hit = reopened.get("key-1").await.unwrap().unwrap();
    assert_eq!(hit.tier(), CacheTier::L2);
    assert_eq!(hit.as_ref(), value);
    drop(hit);
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(snapshot.io.read.requests_submitted, 1);
    assert_eq!(snapshot.io.read.buffered.operations, 1);
    assert_eq!(snapshot.io.read.buffered.bytes, 1120);
    assert_eq!(snapshot.io.read.direct.operations, 0);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn read_io_failure_is_counted_and_latches_miss_only() {
    let files = TestCache::new("snapshot-read-failure");
    let runtime = test_runtime_config(1, 2)
        .with_l1_capacity_bytes(0)
        .with_write_flush_threshold_bytes(128 * 1024);
    let cache = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    cache.put("key", vec![9_u8; 16 * 1024]).unwrap();
    cache.drain().await.unwrap();

    std::fs::OpenOptions::new()
        .write(true)
        .open(&files.data)
        .unwrap()
        .set_len(4096)
        .unwrap();
    assert!(cache.get("key").await.unwrap().is_none());
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::MissOnly);
    assert_eq!(snapshot.l1_misses, 1);
    assert_eq!(snapshot.l2_misses, 1);
    assert_eq!(snapshot.io_failures, 1);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn promoted_l2_values_release_transient_read_memory_before_return() {
    let files = TestCache::new("promoted-l2-buffer-release");
    let runtime = test_runtime_config(1, 2)
        .with_l1_capacity_bytes(64 * 1024)
        .with_l1_shards(1)
        .with_write_flush_threshold_bytes(128 * 1024);
    let cache = files
        .config(1)
        .with_runtime_config(runtime.clone())
        .open()
        .await
        .unwrap();
    cache.put("key-1", vec![3_u8; 16 * 1024]).unwrap();
    cache.put("key-2", vec![4_u8; 16 * 1024]).unwrap();
    cache.drain().await.unwrap();
    cache.close_warm().await.unwrap();

    let reopened = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    let baseline = reopened.snapshot().unwrap().managed_memory_bytes;
    let first = reopened.get("key-1").await.unwrap().unwrap();
    assert_eq!(first.tier(), CacheTier::L2);
    assert_eq!(first.as_ref(), vec![3_u8; 16 * 1024]);
    assert_eq!(reopened.snapshot().unwrap().managed_memory_bytes, baseline);

    let second = reopened.get("key-2").await.unwrap().unwrap();
    assert_eq!(second.tier(), CacheTier::L2);
    assert_eq!(second.as_ref(), vec![4_u8; 16 * 1024]);
    assert_eq!(reopened.snapshot().unwrap().managed_memory_bytes, baseline);
    drop((first, second));
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn retained_l2_values_charge_and_release_transient_memory() {
    let files = TestCache::new("retained-read-memory");
    let runtime = test_runtime_config(1, 2)
        .with_l1_capacity_bytes(0)
        .with_write_flush_threshold_bytes(128 * 1024);
    let cache = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    cache.put("key-1", vec![3_u8; 16 * 1024]).unwrap();
    cache.put("key-2", vec![4_u8; 16 * 1024]).unwrap();
    cache.drain().await.unwrap();

    let baseline = cache.snapshot().unwrap().managed_memory_bytes;
    assert!(cache.get("missing").await.unwrap().is_none());
    assert_eq!(cache.snapshot().unwrap().managed_memory_bytes, baseline);
    let first = cache.get("key-1").await.unwrap().unwrap();
    let after_first = cache.snapshot().unwrap().managed_memory_bytes;
    let second = cache.get("key-2").await.unwrap().unwrap();
    let after_second = cache.snapshot().unwrap().managed_memory_bytes;
    assert_eq!(first.as_ref(), vec![3_u8; 16 * 1024]);
    assert_eq!(second.as_ref(), vec![4_u8; 16 * 1024]);
    assert!(after_first > baseline);
    assert!(after_second > after_first);
    drop((first, second));
    assert_eq!(cache.snapshot().unwrap().managed_memory_bytes, baseline);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn static_config_change_discards_the_old_image() {
    let files = TestCache::new("static-change");
    let cache = files.config(2).open().await.unwrap();
    cache.put("key", "old").unwrap();
    cache.drain().await.unwrap();
    cache.close_warm().await.unwrap();

    let changed = StaticConfig::new(3 * 512 * 1024)
        .with_region_size_bytes(512 * 1024)
        .with_expected_entries(6553);
    let reopened = files.config_with_static(5, changed).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Cold);
    assert!(reopened.get("key").await.unwrap().is_none());
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn unsupported_cache_format_versions_cold_start_empty() {
    for target in ["data", "state", "image"] {
        let files = TestCache::new(&format!("unsupported-{target}"));
        let cache = files.config(2).open().await.unwrap();
        cache.put("key", "old").unwrap();
        cache.close_warm().await.unwrap();

        match target {
            "data" => rewrite_page_version(&files.data, 0, 99),
            "state" => {
                rewrite_page_version(&files.sidecar(".state"), 0, 99);
                rewrite_page_version(&files.sidecar(".state"), 4096, 99);
            }
            "image" => rewrite_page_version(&files.sidecar(".image"), 0, 99),
            _ => unreachable!(),
        }

        let reopened = files.config(3).open().await.unwrap();
        assert_eq!(reopened.startup_mode(), StartupMode::Cold, "{target}");
        assert!(reopened.get("key").await.unwrap().is_none(), "{target}");
        reopened.close_fast().await.unwrap();
    }
}
