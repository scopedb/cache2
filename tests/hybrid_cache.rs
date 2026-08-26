use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cache_rs::{
    CacheHealth, CacheTier, EvictionPolicy, HybridCacheConfig, IoEngine, RegionSetConfig,
    RuntimeConfig, StartupMode, StaticConfig,
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
            .with_index_slots(4096)
            .with_write_shards(2);
        self.config_with_static(workers, static_config)
    }

    fn config_with_static(&self, workers: usize, static_config: StaticConfig) -> HybridCacheConfig {
        let runtime_config = RuntimeConfig::default()
            .with_io_engine(IoEngine::Sync)
            .with_io_workers(workers)
            .with_io_concurrency(workers * 4)
            .with_waiting_write_limit(16)
            .with_l1_capacity(4 * 1024 * 1024)
            .with_memory_limit(32 * 1024 * 1024)
            .with_write_buffer_size(256 * 1024)
            .with_write_batch_size(128 * 1024)
            .with_write_flush_delay(Duration::from_millis(2))
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

#[test]
fn fast_close_always_reopens_empty() {
    let files = TestCache::new("fast-close");
    let cache = files.config(1).open().unwrap();
    assert_eq!(cache.startup_mode(), StartupMode::Fresh);
    cache.put("key", "value").unwrap();
    cache.drain().unwrap();
    assert_eq!(cache.get("key").unwrap().unwrap().as_ref(), b"value");
    cache.close_fast().unwrap();

    let reopened = files.config(7).open().unwrap();
    assert_eq!(
        reopened.startup_mode(),
        StartupMode::ColdAfterUncleanShutdown
    );
    assert!(reopened.get("key").unwrap().is_none());
    reopened.close_fast().unwrap();
}

#[test]
fn pending_l1_values_are_visible_and_bypasses_hide_l2() {
    let files = TestCache::new("latest-memory-visible");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(2)
        .with_io_concurrency(8)
        .with_write_flush_delay(Duration::from_secs(60))
        .with_statistics(true);
    let cache = files.config(2).with_runtime_config(runtime).open().unwrap();
    let mut visible = 0;
    let mut last_sequence = 0;
    for version in 0_u8..64 {
        let bypasses_before = cache.snapshot().unwrap().l1_bypasses;
        let sequence = eventually_admitted(|| cache.put("key", [version; 1024]));
        assert!(sequence > last_sequence);
        last_sequence = sequence;
        match cache.get("key").unwrap() {
            Some(value) => {
                visible += 1;
                assert_eq!(value.tier(), CacheTier::L1);
                assert_eq!(value.as_ref(), &[version; 1024]);
            }
            None => {
                assert!(cache.snapshot().unwrap().l1_bypasses > bypasses_before);
            }
        }
    }
    assert!(visible > 0);
    cache.close_fast().unwrap();
}

#[test]
fn reject_returns_when_the_fixed_write_buffer_needs_a_flush() {
    let files = TestCache::new("reject-write-buffer-flush");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_waiting_write_limit(4)
        .with_l1_capacity(1024 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_buffer_size(256 * 1024)
        .with_write_batch_size(256 * 1024)
        .with_write_flush_delay(Duration::from_secs(60))
        .with_statistics(true);
    let cache = files.config(1).with_runtime_config(runtime).open().unwrap();
    let first = vec![1_u8; 200 * 1024];
    let second = vec![2_u8; 200 * 1024];

    cache.put("key", &first).unwrap();
    let error = match cache.put("key", &second) {
        Err(error) => error,
        Ok(_) => panic!("fixed write buffer accepted a second oversized resident batch"),
    };
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(cache.get("key").unwrap().unwrap().as_ref(), first);
    assert_eq!(cache.snapshot().unwrap().write_rejections, 1);
    let writes = cache.detailed_snapshot().unwrap().writes;
    assert_eq!(writes.write_buffer_rejections, 1);
    assert_eq!(writes.write_buffer_wait_ns, 0);

    cache.drain().unwrap();
    eventually_admitted(|| cache.put("key", &second));
    assert_eq!(cache.get("key").unwrap().unwrap().as_ref(), second);
    cache.close_fast().unwrap();
}

#[test]
fn l1_bypass_fences_the_old_region_value_without_waiting() {
    let files = TestCache::new("l1-bypass-publication");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(2)
        .with_io_concurrency(8)
        .with_waiting_write_limit(16)
        .with_l1_capacity(512)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_buffer_size(256 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_write_flush_delay(Duration::from_secs(60))
        .with_statistics(true);
    let cache = files.config(2).with_runtime_config(runtime).open().unwrap();

    cache.put("key", "old").unwrap();
    cache.drain().unwrap();
    let replacement = vec![9_u8; 1024];
    cache.put("key", &replacement).unwrap();
    assert_eq!(cache.snapshot().unwrap().l1_bypasses, 1);
    assert!(cache.get("key").unwrap().is_none());

    cache.drain().unwrap();
    let value = cache.get("key").unwrap().unwrap();
    assert_eq!(value.tier(), CacheTier::L2);
    assert_eq!(value.as_ref(), replacement);
    drop(value);
    cache.close_fast().unwrap();
}

#[test]
fn concurrent_first_operations_share_one_runtime() {
    let files = TestCache::new("concurrent-first-operations");
    let cache = Arc::new(files.config(2).open().unwrap());

    std::thread::scope(|scope| {
        for ordinal in 0_u64..8 {
            let cache = Arc::clone(&cache);
            scope.spawn(move || {
                let key = ordinal.to_le_bytes();
                let value = [ordinal as u8; 1024];
                eventually_admitted(|| cache.put(key, value));
                assert_eq!(cache.get(key).unwrap().unwrap().as_ref(), &value);
            });
        }
    });

    let cache = Arc::try_unwrap(cache).expect("worker cache references were released");
    cache.drain().unwrap();
    cache.close_fast().unwrap();
}

#[test]
fn latest_put_survives_warm_recovery() {
    let files = TestCache::new("latest-warm-recovery");
    let cache = files.config(3).open().unwrap();
    for version in 0_u8..64 {
        eventually_admitted(|| cache.put("key", [version; 1024]));
    }
    cache.close_warm().unwrap();

    let reopened = files.config(5).open().unwrap();
    let value = reopened.get("key").unwrap().unwrap();
    assert_eq!(value.tier(), CacheTier::L2);
    assert_eq!(value.as_ref(), &[63; 1024]);
    drop(value);
    reopened.close_fast().unwrap();
}

#[test]
fn expired_pending_value_hides_the_region_tier() {
    let files = TestCache::new("pending-expiry");
    let cache = files.config(2).open().unwrap();
    cache.put_until(7, "key", "value", 10).unwrap();
    assert!(cache.get_in_at(7, "key", 10).unwrap().is_none());
    cache.drain().unwrap();
    assert!(cache.get_in_at(7, "key", 10).unwrap().is_none());
    cache.close_fast().unwrap();
}

#[test]
fn embedding_service_can_retune_runtime_policy_and_gracefully_restart() {
    let files = TestCache::new("warm-close");
    let cache = files.config(1).open().unwrap();
    cache.put("key", vec![7_u8; 16 * 1024]).unwrap();
    cache.drain().unwrap();
    cache.close_warm().unwrap();

    let retuned = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(7)
        .with_io_concurrency(35)
        .with_waiting_write_limit(11)
        .with_l1_capacity(2 * 1024 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(7)
        .with_eviction_policy(EvictionPolicy::S3Fifo)
        .with_write_buffer_size(128 * 1024)
        .with_write_batch_size(64 * 1024)
        .with_write_flush_delay(Duration::from_millis(7));
    let reopened = files.config(7).with_runtime_config(retuned).open().unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    let first = reopened.get("key").unwrap().unwrap();
    assert_eq!(first.tier(), CacheTier::L2);
    assert_eq!(first.as_ref(), vec![7_u8; 16 * 1024]);
    drop(first);
    assert_eq!(reopened.get("key").unwrap().unwrap().tier(), CacheTier::L1);
    let snapshot = reopened.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::Running);
    assert!(!snapshot.statistics_enabled);
    assert_eq!(snapshot.l1_hits, 0);
    assert!(snapshot.managed_memory_bytes <= snapshot.managed_memory_limit_bytes);
    reopened.close_fast().unwrap();
}

#[test]
fn namespaces_are_independent() {
    let files = TestCache::new("namespaces");
    let cache = files.config(3).open().unwrap();
    cache.put_in(1, "key", "one").unwrap();
    cache.put_in(2, "key", "two").unwrap();
    cache.flush().unwrap();
    assert_eq!(cache.get_in(1, "key").unwrap().unwrap().as_ref(), b"one");
    assert_eq!(cache.get_in(2, "key").unwrap().unwrap().as_ref(), b"two");
    cache.close_fast().unwrap();
}

#[test]
fn namespace_region_sets_rotate_and_recover_independently() {
    const HOT: u32 = 7;
    const BULK: u32 = 9;
    const REGION_BYTES: u64 = 512 * 1024;

    let files = TestCache::new("region-set-isolation");
    let static_config = StaticConfig::new(6 * REGION_BYTES)
        .with_region_size(REGION_BYTES)
        .with_index_slots(4096)
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
        .unwrap();
    let value = vec![0x5a; 240 * 1024];

    // Rotate BULK once so its first Region becomes a reclaimable sealed
    // candidate. Repeated HOT rotation must never select that Region.
    eventually_admitted(|| cache.put_in(BULK, "bulk-survivor", &value));
    eventually_admitted(|| cache.put_in(BULK, "bulk-fill-1", &value));
    eventually_admitted(|| cache.put_in(BULK, "bulk-fill-2", &value));
    cache.drain().unwrap();

    for ordinal in 0..12 {
        eventually_admitted(|| cache.put_in(HOT, format!("hot-{ordinal}"), &value));
        cache.drain().unwrap();
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
    cache.close_warm().unwrap();

    let reopened = files.config_with_static(2, static_config).open().unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Warm);
    let survivor = reopened.get_in(BULK, "bulk-survivor").unwrap().unwrap();
    assert_eq!(survivor.tier(), CacheTier::L2);
    assert_eq!(survivor.as_ref(), value);
    drop(survivor);
    reopened.close_fast().unwrap();
}

#[test]
fn invalid_runtime_config_is_rejected_before_file_creation() {
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
                .with_write_buffer_size(256 * 1024)
                .with_write_batch_size(128 * 1024)
        }),
        ("write-queue-out-of-range", |config| {
            config
                .with_io_workers(16)
                .with_io_concurrency(65_536)
                .with_waiting_write_limit(65_537)
        }),
        ("zero-l1-shards", |config| config.with_l1_shards(0)),
        ("write-flush-delay-out-of-range", |config| {
            config.with_write_flush_delay(Duration::from_secs(24 * 60 * 60 + 1))
        }),
    ];

    for (case, configure) in cases {
        let files = TestCache::new(case);
        let error = files
            .config(2)
            .with_runtime_config(configure(RuntimeConfig::default()))
            .open()
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput, "{case}");
        files.assert_absent();
    }
}

#[test]
fn cold_start_removes_stale_recovery_files() {
    let files = TestCache::new("stale-recovery-files");
    let image = files.sidecar(".image");
    let temporary = files.sidecar(".image.next");
    std::fs::write(&image, b"stale image").unwrap();
    std::fs::write(&temporary, b"stale temporary image").unwrap();

    let cache = files.config(2).open().unwrap();
    assert!(!image.exists());
    assert!(!temporary.exists());
    cache.close_fast().unwrap();
}

#[test]
fn minimum_region_stores_its_first_record_at_offset_zero_and_recovers() {
    let files = TestCache::new("minimum-region");
    let static_config = StaticConfig::new(2 * 4096)
        .with_region_size(4096)
        .with_index_slots(64)
        .with_write_shards(1);
    let value = vec![0x5a; 128];

    let cache = files
        .config_with_static(1, static_config.clone())
        .open()
        .unwrap();
    cache.put("first", &value).unwrap();
    cache.close_warm().unwrap();

    let recovered = files.config_with_static(1, static_config).open().unwrap();
    assert_eq!(recovered.startup_mode(), StartupMode::Warm);
    assert_eq!(recovered.get("first").unwrap().unwrap().as_ref(), value);
    recovered.close_fast().unwrap();
}

#[test]
fn reported_peak_disk_bytes_covers_atomic_warm_publication() {
    let files = TestCache::new("disk-bound");
    let static_config = StaticConfig::new(3 * 512 * 1024)
        .with_region_size(512 * 1024)
        .with_index_slots(4096)
        .with_write_shards(2);
    let peak_disk_bytes = static_config.peak_disk_bytes().unwrap();

    let cache = files.config_with_static(2, static_config).open().unwrap();
    cache.put("key", vec![7_u8; 16 * 1024]).unwrap();
    cache.close_warm().unwrap();

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

#[test]
fn cache_snapshot_stays_within_the_configured_bounds() {
    let files = TestCache::new("resource-snapshot");
    let expected_disk_peak = StaticConfig::new(3 * 512 * 1024)
        .with_region_size(512 * 1024)
        .with_index_slots(4096)
        .with_write_shards(2)
        .peak_disk_bytes()
        .unwrap();
    let cache = files.config(4).open().unwrap();

    for ordinal in 0_u64..128 {
        eventually_admitted(|| cache.put(ordinal.to_le_bytes(), vec![ordinal as u8; 8 * 1024]));
    }
    cache.drain().unwrap();

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
    assert_eq!(detailed.writes.write_requests_in_flight, 0);
    assert_eq!(
        detailed.writes.write_requests_peak, 0,
        "reject-mode puts use shard write buffers directly"
    );
    assert_eq!(detailed.io.requests_in_flight, 0);
    assert_eq!(detailed.io.completed, detailed.io.submitted);
    assert!(
        detailed
            .io
            .direct_operations
            .saturating_add(detailed.io.buffered_operations)
            > 0
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
    cache.close_fast().unwrap();
}

#[test]
fn cache_snapshot_reports_tier_activity_and_resets_on_open() {
    let files = TestCache::new("activity-snapshot");
    let cache = files.config(2).open().unwrap();
    cache.put("key", "value").unwrap();
    assert_eq!(cache.get("key").unwrap().unwrap().tier(), CacheTier::L1);
    assert!(cache.get("missing").unwrap().is_none());
    cache.drain().unwrap();

    let before_close = cache.snapshot().unwrap();
    assert_eq!(before_close.health, CacheHealth::Running);
    assert_eq!(before_close.puts, 1);
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
    cache.close_warm().unwrap();

    let reopened = files.config(3).open().unwrap();
    let fresh = reopened.snapshot().unwrap();
    assert_eq!(fresh.puts, 0);
    assert_eq!(fresh.l1_hits, 0);
    assert_eq!(fresh.l2_hits, 0);
    assert_eq!(reopened.get("key").unwrap().unwrap().tier(), CacheTier::L2);
    assert_eq!(reopened.get("key").unwrap().unwrap().tier(), CacheTier::L1);

    let warmed = reopened.snapshot().unwrap();
    assert_eq!(warmed.l1_hits, 1);
    assert_eq!(warmed.l1_misses, 1);
    assert_eq!(warmed.l2_hits, 1);
    assert_eq!(warmed.l2_misses, 0);
    assert_eq!(warmed.l2_read_memory_misses, 0);
    assert_eq!(warmed.l2_read_busy_misses, 0);
    assert_eq!(warmed.served_bytes, 10);
    assert_eq!(warmed.l1_promotions, 1);
    reopened.close_fast().unwrap();
}

#[test]
fn read_io_failure_is_counted_and_latches_miss_only() {
    let files = TestCache::new("snapshot-read-failure");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_waiting_write_limit(16)
        .with_l1_capacity(0)
        .with_memory_limit(32 * 1024 * 1024)
        .with_write_buffer_size(256 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
    let cache = files.config(1).with_runtime_config(runtime).open().unwrap();
    cache.put("key", vec![9_u8; 16 * 1024]).unwrap();
    cache.drain().unwrap();

    std::fs::OpenOptions::new()
        .write(true)
        .open(&files.data)
        .unwrap()
        .set_len(4096)
        .unwrap();
    assert!(cache.get("key").unwrap().is_none());
    let snapshot = cache.snapshot().unwrap();
    assert_eq!(snapshot.health, CacheHealth::MissOnly);
    assert_eq!(snapshot.l1_misses, 1);
    assert_eq!(snapshot.l2_misses, 1);
    assert_eq!(snapshot.io_failures, 1);
    cache.close_fast().unwrap();
}

#[test]
fn promoted_l2_values_release_transient_read_memory_before_return() {
    let files = TestCache::new("promoted-l2-buffer-release");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_waiting_write_limit(4)
        .with_l1_capacity(64 * 1024)
        .with_memory_limit(32 * 1024 * 1024)
        .with_l1_shards(1)
        .with_write_buffer_size(256 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
    let cache = files
        .config(1)
        .with_runtime_config(runtime.clone())
        .open()
        .unwrap();
    cache.put("key-1", vec![3_u8; 16 * 1024]).unwrap();
    cache.put("key-2", vec![4_u8; 16 * 1024]).unwrap();
    cache.drain().unwrap();
    cache.close_warm().unwrap();

    let reopened = files.config(1).with_runtime_config(runtime).open().unwrap();
    let baseline = reopened.snapshot().unwrap().managed_memory_bytes;
    let first = reopened.get("key-1").unwrap().unwrap();
    assert_eq!(first.tier(), CacheTier::L2);
    assert_eq!(first.as_ref(), vec![3_u8; 16 * 1024]);
    assert_eq!(reopened.snapshot().unwrap().managed_memory_bytes, baseline);

    let second = reopened.get("key-2").unwrap().unwrap();
    assert_eq!(second.tier(), CacheTier::L2);
    assert_eq!(second.as_ref(), vec![4_u8; 16 * 1024]);
    let snapshot = reopened.detailed_snapshot().unwrap();
    assert_eq!(snapshot.summary.managed_memory_bytes, baseline);
    assert_eq!(snapshot.summary.l1_promotions, 2);
    drop((first, second));
    reopened.close_fast().unwrap();
}

#[test]
fn retained_l2_values_use_exact_transient_memory_without_slot_saturation() {
    let files = TestCache::new("exact-transient-read-memory");
    let runtime = RuntimeConfig::default()
        .with_io_engine(IoEngine::Sync)
        .with_io_workers(1)
        .with_io_concurrency(4)
        .with_waiting_write_limit(4)
        .with_l1_capacity(0)
        .with_memory_limit(32 * 1024 * 1024)
        .with_write_buffer_size(256 * 1024)
        .with_write_batch_size(128 * 1024)
        .with_statistics(true);
    let cache = files.config(1).with_runtime_config(runtime).open().unwrap();
    cache.put("key-1", vec![3_u8; 16 * 1024]).unwrap();
    cache.put("key-2", vec![4_u8; 16 * 1024]).unwrap();
    cache.drain().unwrap();

    let baseline = cache.snapshot().unwrap().managed_memory_bytes;
    let first = cache.get("key-1").unwrap().unwrap();
    assert!(cache.get("missing").unwrap().is_none());
    let after_first = cache.snapshot().unwrap().managed_memory_bytes;
    let second = cache.get("key-2").unwrap().unwrap();
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
    cache.close_fast().unwrap();
}

#[test]
fn static_config_change_discards_the_old_image() {
    let files = TestCache::new("static-change");
    let cache = files.config(2).open().unwrap();
    cache.put("key", "old").unwrap();
    cache.drain().unwrap();
    cache.close_warm().unwrap();

    let changed = StaticConfig::new(3 * 512 * 1024)
        .with_region_size(512 * 1024)
        .with_index_slots(8192)
        .with_write_shards(2);
    let reopened = files.config_with_static(5, changed).open().unwrap();
    assert_eq!(reopened.startup_mode(), StartupMode::Fresh);
    assert!(reopened.get("key").unwrap().is_none());
    reopened.close_fast().unwrap();
}

#[test]
fn unsupported_cache_format_versions_cold_start_empty() {
    for (target, expected_startup) in [
        ("data", StartupMode::Fresh),
        ("state", StartupMode::ColdAfterUncleanShutdown),
        ("image", StartupMode::ColdAfterUncleanShutdown),
    ] {
        let files = TestCache::new(&format!("unsupported-{target}"));
        let cache = files.config(2).open().unwrap();
        cache.put("key", "old").unwrap();
        cache.close_warm().unwrap();

        match target {
            "data" => rewrite_page_version(&files.data, 0, 99),
            "state" => {
                rewrite_page_version(&files.sidecar(".state"), 0, 99);
                rewrite_page_version(&files.sidecar(".state"), 4096, 99);
            }
            "image" => rewrite_page_version(&files.sidecar(".image"), 0, 99),
            _ => unreachable!(),
        }

        let reopened = files.config(3).open().unwrap();
        assert_eq!(reopened.startup_mode(), expected_startup, "{target}");
        assert!(reopened.get("key").unwrap().is_none(), "{target}");
        reopened.close_fast().unwrap();
    }
}
