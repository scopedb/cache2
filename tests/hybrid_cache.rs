use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use cache_rs::{
    CacheHealth, CacheTier, HybridCacheConfig, IoEngine, RegionSetConfig, RuntimeConfig,
    StartupMode, StaticConfig,
};

static NEXT_FILE: AtomicU64 = AtomicU64::new(1);
type RuntimeConfigCase = (&'static str, fn(RuntimeConfig) -> RuntimeConfig);

struct TestCache {
    data: PathBuf,
}

impl TestCache {
    fn new(name: &str) -> Self {
        let id = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        let data =
            std::env::temp_dir().join(format!("cache-rs-{name}-{}-{id}.cache", std::process::id()));
        Self { data }
    }

    fn config(&self, workers: usize) -> HybridCacheConfig {
        let static_config = StaticConfig::new(3 * 512 * 1024)
            .with_region_size(512 * 1024)
            .with_expected_entries(3277)
            .with_write_shards(2);
        self.config_with_static(workers, static_config)
    }

    fn config_with_static(&self, workers: usize, static_config: StaticConfig) -> HybridCacheConfig {
        let runtime_config = RuntimeConfig::default()
            .with_io_engine(IoEngine::Sync)
            .with_io_workers(workers)
            .with_io_concurrency(workers * 4)
            .with_l1_capacity(4 * 1024 * 1024)
            .with_memory_limit(32 * 1024 * 1024)
            .with_write_batch_size(256 * 1024)
            .with_statistics(true);
        HybridCacheConfig::from_static(&self.data, static_config)
            .with_runtime_config(runtime_config)
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
    let checksum = crc32c::crc32c(&page);
    page[CRC_OFFSET..].copy_from_slice(&checksum.to_le_bytes());
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&page).unwrap();
    file.sync_all().unwrap();
}

fn eventually_admitted<T>(mut put: impl FnMut() -> std::io::Result<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match put() {
            Ok(value) => return value,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
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
async fn immediate_l1_publication_is_best_effort() {
    let files = TestCache::new("latest-memory-visible");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(2)
        .with_io_concurrency(8)
        .with_statistics(true);
    let cache = files
        .config(2)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    let mut visible = 0;
    let mut last_sequence = 0;
    for version in 0_u8..64 {
        let bypasses_before = cache.snapshot().unwrap().l1_bypasses;
        let sequence = eventually_admitted(|| cache.put("key", [version; 1024]));
        assert!(sequence > last_sequence);
        last_sequence = sequence;
        match cache.get("key").await.unwrap() {
            Some(value) if value.tier() == CacheTier::L1 => {
                visible += 1;
                assert_eq!(value.as_ref(), &[version; 1024]);
            }
            Some(value) => {
                assert!(value.iter().all(|byte| *byte == value[0]));
                assert!(value[0] <= version);
            }
            None => assert!(cache.snapshot().unwrap().l1_bypasses >= bypasses_before),
        }
    }
    assert!(visible > 0);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn reject_returns_when_the_fixed_write_buffer_needs_a_flush() {
    let files = TestCache::new("reject-write-buffer-flush");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_l1_capacity(1024 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_batch_size(256 * 1024)
        .with_statistics(true);
    let cache = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap();
    let first = vec![1_u8; 200 * 1024];
    let second = vec![2_u8; 200 * 1024];

    cache.put("key", &first).unwrap();
    let error = match cache.put("key", &second) {
        Err(error) => error,
        Ok(_) => panic!("fixed write buffer accepted a second oversized resident batch"),
    };
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(cache.get("key").await.unwrap().unwrap().as_ref(), first);
    assert_eq!(cache.snapshot().unwrap().write_rejections, 1);
    assert_eq!(
        cache.detailed_snapshot().unwrap().write_buffer_rejections,
        1
    );

    cache.drain().await.unwrap();
    eventually_admitted(|| cache.put("key", &second));
    assert_eq!(cache.get("key").await.unwrap().unwrap().as_ref(), second);
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn l1_bypass_may_remain_stale_after_region_completion() {
    let files = TestCache::new("l1-bypass-publication");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(2)
        .with_io_concurrency(8)
        .with_l1_capacity(512)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
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
async fn unavailable_io_engine_fails_during_open_and_releases_the_lock() {
    let files = TestCache::new("unavailable-io-engine");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::IoUring)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_l1_capacity(4 * 1024 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_write_batch_size(128 * 1024);

    let error = files
        .config(1)
        .with_runtime_config(runtime)
        .open()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);

    let reopened = files.config(1).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Cold);
    reopened.close_fast().await.unwrap();
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
async fn embedding_service_can_retune_runtime_policy_and_gracefully_restart() {
    let files = TestCache::new("warm-close");
    let cache = files.config(1).open().await.unwrap();
    cache.put("key", vec![7_u8; 16 * 1024]).unwrap();
    cache.drain().await.unwrap();
    cache.close_warm().await.unwrap();

    let retuned = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(7)
        .with_io_concurrency(35)
        .with_l1_capacity(2 * 1024 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(7)
        .with_write_batch_size(64 * 1024);
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
    assert_eq!(
        reopened.get("key").await.unwrap().unwrap().tier(),
        CacheTier::L1
    );
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::Running);
    assert!(!snapshot.statistics_enabled);
    assert_eq!(snapshot.l1_hits, 0);
    assert!(snapshot.managed_memory_bytes <= snapshot.managed_memory_limit_bytes);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn namespaces_are_independent() {
    let files = TestCache::new("namespaces");
    let cache = files.config(3).open().await.unwrap();
    cache.put_in(1, "key", "one").unwrap();
    cache.put_in(2, "key", "two").unwrap();
    cache.drain().await.unwrap();
    assert_eq!(
        cache.get_in(1, "key").await.unwrap().unwrap().as_ref(),
        b"one"
    );
    assert_eq!(
        cache.get_in(2, "key").await.unwrap().unwrap().as_ref(),
        b"two"
    );
    cache.close_fast().await.unwrap();
}

#[tokio::test]
async fn delete_is_namespaced_sequenced_and_warm_recoverable() {
    let files = TestCache::new("delete-warm-recovery");
    let cache = files.config(3).open().await.unwrap();
    let put_sequence = cache.put_in(1, "key", "one").unwrap();
    cache.put_in(2, "key", "two").unwrap();
    cache.drain().await.unwrap();

    let delete_sequence = cache.delete_in(1, "key").unwrap();
    assert!(delete_sequence > put_sequence);
    cache.drain().await.unwrap();
    assert!(cache.get_in(1, "key").await.unwrap().is_none());
    assert_eq!(
        cache.get_in(2, "key").await.unwrap().unwrap().as_ref(),
        b"two"
    );
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.puts, 2);
    assert_eq!(snapshot.deletes, 1);
    cache.close_warm().await.unwrap();

    let reopened = files.config(5).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    assert!(reopened.get_in(1, "key").await.unwrap().is_none());
    assert_eq!(
        reopened.get_in(2, "key").await.unwrap().unwrap().as_ref(),
        b"two"
    );

    let replacement_sequence = reopened.put_in(1, "key", "new").unwrap();
    assert!(replacement_sequence > delete_sequence);
    reopened.drain().await.unwrap();
    assert_eq!(
        reopened.get_in(1, "key").await.unwrap().unwrap().as_ref(),
        b"new"
    );
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
                    eventually_admitted(|| cache.put(&keys[key_index], &value[..value_len]));
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
async fn namespace_region_sets_rotate_and_recover_independently() {
    const HOT: u32 = 7;
    const BULK: u32 = 9;
    const REGION_BYTES: u64 = 512 * 1024;

    let files = TestCache::new("region-set-isolation");
    let static_config = StaticConfig::new(6 * REGION_BYTES)
        .with_region_size(REGION_BYTES)
        .with_expected_entries(3277)
        .with_write_shards(2)
        .with_region_sets([
            RegionSetConfig::new(0).with_weight(2),
            RegionSetConfig::new(1)
                .with_weight(1)
                .with_namespaces([HOT]),
        ]);

    let cache = files
        .config_with_static(2, static_config.clone())
        .open()
        .await
        .unwrap();
    let value = vec![0x5a; 240 * 1024];

    // Rotate BULK once so its first Region becomes a reclaimable sealed
    // candidate. Repeated HOT rotation must never select that Region.
    eventually_admitted(|| cache.put_in(BULK, "bulk-survivor", &value));
    eventually_admitted(|| cache.put_in(BULK, "bulk-fill-1", &value));
    eventually_admitted(|| cache.put_in(BULK, "bulk-fill-2", &value));
    cache.drain().await.unwrap();

    for ordinal in 0..12 {
        eventually_admitted(|| cache.put_in(HOT, format!("hot-{ordinal}"), &value));
        cache.drain().await.unwrap();
    }

    let detailed = cache.detailed_snapshot().unwrap();
    assert_eq!(detailed.region_sets.len(), 2);
    let bulk = detailed
        .region_sets
        .iter()
        .find(|set| set.id.get() == 0)
        .unwrap();
    let hot = detailed
        .region_sets
        .iter()
        .find(|set| set.id.get() == 1)
        .unwrap();
    assert_eq!(bulk.capacity_bytes, 4 * REGION_BYTES);
    assert_eq!(hot.capacity_bytes, 2 * REGION_BYTES);
    assert_eq!(
        bulk.active_region_count + bulk.free_region_count + bulk.sealed_region_count,
        4
    );
    assert_eq!(
        hot.active_region_count + hot.free_region_count + hot.sealed_region_count,
        2
    );
    assert!(bulk.rotations > 0);
    assert!(hot.rotations > bulk.rotations);
    cache.close_warm().await.unwrap();

    let reopened = files
        .config_with_static(2, static_config)
        .open()
        .await
        .unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    let survivor = reopened
        .get_in(BULK, "bulk-survivor")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(survivor.tier(), CacheTier::L2);
    assert_eq!(survivor.as_ref(), value);
    drop(survivor);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn invalid_runtime_config_is_rejected_before_file_creation() {
    let cases: [RuntimeConfigCase; 6] = [
        ("workers-exceed-queue", |config| {
            config
                .with_io_engine(IoEngine::Sync)
                .with_io_workers(8)
                .with_io_concurrency(4)
        }),
        ("l1-exceeds-budget", |config| {
            config
                .with_l1_capacity(64 * 1024 * 1024)
                .with_memory_limit(32 * 1024 * 1024)
        }),
        ("fixed-plan-exceeds-budget", |config| {
            config
                .with_io_engine(IoEngine::Sync)
                .with_io_workers(2)
                .with_io_concurrency(8)
                .with_l1_capacity(0)
                .with_memory_limit(2 * 1024 * 1024)
                .with_write_batch_size(128 * 1024)
        }),
        ("zero-l1-shards", |config| config.with_l1_shards(0)),
        ("unaligned-write-batch", |config| {
            config.with_write_batch_size(4097)
        }),
        ("oversized-write-batch", |config| {
            config.with_write_batch_size(4 * 1024 * 1024 + 4096)
        }),
    ];

    for (case, configure) in cases {
        let files = TestCache::new(case);
        let error = files
            .config(2)
            .with_runtime_config(configure(RuntimeConfig::default()))
            .open()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{case}");
        files.assert_absent();
    }
}

#[tokio::test]
async fn public_entry_size_limits_are_explicit() {
    let files = TestCache::new("entry-size-limits");
    let cache = files.config(1).open().await.unwrap();
    let oversized_key = vec![0_u8; 4 * 1024 + 1];
    let oversized_value = vec![0_u8; 256 * 1024 + 1];

    assert_eq!(
        cache.put(&oversized_key, b"value").unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        cache.put(b"key", &oversized_value).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert_eq!(
        cache.delete(&oversized_key).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(cache.get(&oversized_key).await.unwrap().is_none());
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.puts, 0);
    assert_eq!(snapshot.deletes, 0);
    cache.close_fast().await.unwrap();
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
        .with_region_size(4096)
        .with_expected_entries(51)
        .with_write_shards(1);
    let value = vec![0x5a; 128];

    let cache = files
        .config_with_static(1, static_config.clone())
        .open()
        .await
        .unwrap();
    cache.put("first", &value).unwrap();
    cache.close_warm().await.unwrap();

    let recovered = files
        .config_with_static(1, static_config)
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
    let static_config = StaticConfig::new(3 * 512 * 1024)
        .with_region_size(512 * 1024)
        .with_expected_entries(3277)
        .with_write_shards(2);
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
async fn cache_snapshot_stays_within_the_configured_bounds() {
    let files = TestCache::new("resource-snapshot");
    let expected_disk_peak = StaticConfig::new(3 * 512 * 1024)
        .with_region_size(512 * 1024)
        .with_expected_entries(3277)
        .with_write_shards(2)
        .peak_disk_bytes()
        .unwrap();
    let cache = files.config(4).open().await.unwrap();

    for ordinal in 0_u64..128 {
        eventually_admitted(|| cache.put(ordinal.to_le_bytes(), vec![ordinal as u8; 8 * 1024]));
    }
    cache.drain().await.unwrap();

    let resources = cache.snapshot().unwrap();
    assert_eq!(resources.health, CacheHealth::Running);
    assert!(resources.statistics_enabled);
    assert_eq!(resources.puts, 128);
    assert_eq!(resources.written_bytes, 128 * 8 * 1024);
    assert!(resources.region_rotations > 0);
    assert_eq!(resources.managed_memory_limit_bytes, 32 * 1024 * 1024);
    assert!(resources.managed_memory_bytes <= resources.managed_memory_limit_bytes);
    assert!(resources.managed_memory_peak_bytes >= resources.managed_memory_bytes);
    assert!(resources.managed_memory_peak_bytes <= resources.managed_memory_limit_bytes);
    assert_eq!(resources.logical_disk_peak_bytes, expected_disk_peak);

    let detailed = cache.detailed_snapshot().unwrap();
    assert_eq!(detailed.summary, resources);
    assert_eq!(detailed.write_buffer_rejections, resources.write_rejections);
    assert_eq!(detailed.io.requests_in_flight, 0);
    assert_eq!(detailed.io.completed, detailed.io.submitted);
    assert!(
        detailed
            .io
            .direct_operations
            .saturating_add(detailed.io.buffered_operations)
            > 0
    );
    assert!(detailed.l1.entry_capacity > 0);
    assert!(detailed.l1.resident_entries <= detailed.l1.entry_capacity);
    assert!(detailed.l1.resident_bytes <= 4 * 1024 * 1024);
    assert_eq!(detailed.l1.retained_bytes, 0);
    assert!(detailed.l1.metadata_bytes > 0);
    assert_eq!(detailed.index.slot_capacity, 4097);
    assert_eq!(
        detailed.index.physical_value_slots
            + detailed.index.deleted_slots
            + detailed.index.empty_slots,
        detailed.index.slot_capacity
    );
    assert_eq!(detailed.region_sets.len(), 1);
    let region_set = detailed.region_sets[0];
    assert_eq!(region_set.capacity_bytes, 3 * 512 * 1024);
    assert_eq!(
        region_set.active_region_count
            + region_set.free_region_count
            + region_set.sealed_region_count,
        3
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
    assert_eq!(before_close.served_bytes, 5);
    assert_eq!(before_close.l1_promotions, 0);
    assert_eq!(before_close.io_failures, 0);
    cache.close_warm().await.unwrap();

    let reopened = files.config(3).open().await.unwrap();
    let fresh = reopened.snapshot().unwrap();
    assert_eq!(fresh.puts, 0);
    assert_eq!(fresh.deletes, 0);
    assert_eq!(fresh.l1_hits, 0);
    assert_eq!(fresh.l2_hits, 0);
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
    assert_eq!(warmed.served_bytes, 10);
    assert_eq!(warmed.l1_promotions, 1);
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn read_io_failure_is_counted_and_latches_miss_only() {
    let files = TestCache::new("snapshot-read-failure");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_l1_capacity(0)
        .with_memory_limit(32 * 1024 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
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
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_l1_capacity(64 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
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
    let snapshot = reopened.detailed_snapshot().unwrap();
    assert_eq!(snapshot.summary.managed_memory_bytes, baseline);
    assert_eq!(snapshot.summary.l1_promotions, 2);
    drop((first, second));
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn retained_l2_values_use_exact_transient_memory_without_slot_saturation() {
    let files = TestCache::new("exact-transient-read-memory");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_l1_capacity(0)
        .with_memory_limit(32 * 1024 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
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
    let first = cache.get("key-1").await.unwrap().unwrap();
    assert!(cache.get("missing").await.unwrap().is_none());
    let after_first = cache.snapshot().unwrap().managed_memory_bytes;
    let second = cache.get("key-2").await.unwrap().unwrap();
    let after_second = cache.snapshot().unwrap().managed_memory_bytes;
    assert_eq!(first.as_ref(), vec![3_u8; 16 * 1024]);
    assert_eq!(second.as_ref(), vec![4_u8; 16 * 1024]);
    assert!(after_first > baseline);
    assert!(after_second > after_first);
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.l2_hits, 2);
    assert_eq!(snapshot.l2_misses, 1);
    assert_eq!(snapshot.write_rejections, 0);
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
        .with_region_size(512 * 1024)
        .with_expected_entries(6553)
        .with_write_shards(2);
    let reopened = files.config_with_static(5, changed).open().await.unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Cold);
    assert!(reopened.get("key").await.unwrap().is_none());
    reopened.close_fast().await.unwrap();
}

#[tokio::test]
async fn unsupported_cache_format_versions_cold_start_empty() {
    for (target, expected_startup) in [
        ("data", StartupMode::Cold),
        ("state", StartupMode::Cold),
        ("image", StartupMode::Cold),
    ] {
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
        assert_eq!(reopened.startup_mode(), expected_startup, "{target}");
        assert!(reopened.get("key").await.unwrap().is_none(), "{target}");
        reopened.close_fast().await.unwrap();
    }
}
