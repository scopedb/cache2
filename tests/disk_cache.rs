use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::FileExt;

use cache_rs::{
    BackpressurePolicy, CacheConfig, CacheError, DiskCache, PutOptions, PutOutcome, RejectReason,
    RemoveOutcome,
};

const SUPERBLOCK_AREA_SIZE: u64 = 8 * 1024;
const REGION_HEADER_SIZE: u64 = 4 * 1024;

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new(name: &str) -> Self {
        let nonce = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "cache-rs-{name}-{}-{nonce}.cache",
            std::process::id()
        ));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn config(path: &Path, region_size: u64, regions: u64) -> CacheConfig {
    CacheConfig::new(path, SUPERBLOCK_AREA_SIZE + region_size * regions)
        .with_region_size(region_size)
        .with_index_slots(128)
        .with_max_key_size(256)
        .with_max_value_size(2048)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn sparse_golden(input: &str) -> Vec<u8> {
    let mut output: Option<Vec<u8>> = None;
    for raw_line in input.lines() {
        let line = raw_line.split('#').next().unwrap().trim();
        if line.is_empty() {
            continue;
        }
        let mut fields = line.split_whitespace();
        let first = fields.next().unwrap();
        if first == "length" {
            output = Some(vec![0_u8; fields.next().unwrap().parse().unwrap()]);
            continue;
        }
        let offset = usize::from_str_radix(first, 16).unwrap();
        let hex = fields.next().unwrap().as_bytes();
        let bytes = hex
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect::<Vec<_>>();
        let output = output.as_mut().unwrap();
        output[offset..offset + bytes.len()].copy_from_slice(&bytes);
    }
    output.unwrap()
}

fn index_bucket(key: &[u8], buckets: usize) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64 ^ 0x6a09_e667_f3bc_c909;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % buckets as u64) as usize
}

#[test]
fn put_overwrite_remove_and_recover() {
    let file = TestFile::new("recovery");
    let config = config(file.path(), 16 * 1024, 3);
    let cache = config.clone().open().unwrap();

    assert_eq!(
        cache.put("key", "one", PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.get(b"key").unwrap(), Some(b"one".to_vec()));
    cache.put("key", "two", PutOptions::default()).unwrap();
    cache.put("keep", "value", PutOptions::default()).unwrap();
    assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
    assert_eq!(cache.remove(b"missing").unwrap(), RemoveOutcome::NotFound);
    cache.flush().unwrap();
    drop(cache);

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
    assert_eq!(reopened.get(b"missing").unwrap(), None);
    assert_eq!(reopened.get(b"keep").unwrap(), Some(b"value".to_vec()));
    assert_eq!(reopened.stats().recovered_entries, 1);
}

#[test]
fn async_facade_respects_completed_mutation_order_and_close_drains_accepted_work() {
    let file = TestFile::new("async-drain");
    let config = config(file.path(), 16 * 1024, 4)
        .with_submission_queue_depths(4, 16)
        .with_io_queue_depth(8);
    let cache = config.clone().open().unwrap();
    let async_cache = cache.async_handle().unwrap();

    let first = async_cache.put("same", "one", PutOptions::default());
    assert_eq!(first.wait().unwrap(), PutOutcome::Stored);
    let second = async_cache.put("same", "two", PutOptions::default());
    assert_eq!(second.wait().unwrap(), PutOutcome::Stored);
    assert_eq!(
        async_cache.get("same").wait().unwrap(),
        Some(b"two".to_vec())
    );

    let pending = (0..12)
        .map(|id| {
            async_cache.put(
                format!("async-{id}"),
                format!("value-{id}"),
                PutOptions::default(),
            )
        })
        .collect::<Vec<_>>();
    let close = async_cache.close();
    for request in pending {
        assert_eq!(request.wait().unwrap(), PutOutcome::Stored);
    }
    close.wait().unwrap();
    assert!(matches!(
        async_cache.get("same").wait(),
        Err(CacheError::Closed)
    ));

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"same").unwrap(), Some(b"two".to_vec()));
    for id in 0..12 {
        assert_eq!(
            reopened.get(format!("async-{id}").as_bytes()).unwrap(),
            Some(format!("value-{id}").into_bytes())
        );
    }
}

#[test]
fn concurrent_sync_and_async_close_share_one_successful_shutdown() {
    let file = TestFile::new("concurrent-close");
    let config = config(file.path(), 16 * 1024, 3);
    let cache = config.clone().open().unwrap();
    cache
        .put("durable", "value", PutOptions::default())
        .unwrap();
    let async_cache = cache.async_handle().unwrap();
    let start = Arc::new(Barrier::new(3));

    let sync_closer = {
        let cache = cache.clone();
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            cache.close()
        })
    };
    let async_closer = {
        let async_cache = async_cache.clone();
        let start = Arc::clone(&start);
        thread::spawn(move || {
            start.wait();
            async_cache.close().wait()
        })
    };
    start.wait();

    sync_closer.join().unwrap().unwrap();
    async_closer.join().unwrap().unwrap();
    cache.close().unwrap();

    // Reopen while every old handle remains alive: the one physical close
    // owner must already have drained I/O and released flock.
    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"durable").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn fifo_region_reuse_evicts_old_offsets_without_harming_new_values() {
    let file = TestFile::new("fifo");
    let config = config(file.path(), 8 * 1024, 3);
    let cache = config.clone().open().unwrap();
    let value = vec![7_u8; 900];

    for id in 0..20 {
        cache
            .put(format!("key-{id}"), &value, PutOptions::default())
            .unwrap();
    }

    assert_eq!(cache.get(b"key-0").unwrap(), None);
    assert_eq!(cache.get(b"key-10").unwrap(), Some(value.clone()));
    assert_eq!(cache.get(b"key-19").unwrap(), Some(value.clone()));
    let stats = cache.stats();
    assert_eq!(stats.regions_reused, 2);
    assert!(stats.reclaim_records_scanned > 0);
    assert_eq!(stats.reclaim_index_fallbacks, 0);
    cache.flush().unwrap();
    drop(cache);

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key-0").unwrap(), None);
    assert_eq!(reopened.get(b"key-10").unwrap(), Some(value.clone()));
    assert_eq!(reopened.get(b"key-19").unwrap(), Some(value));
}

#[test]
fn clear_ttl_rejection_and_file_lock_are_enforced() {
    let file = TestFile::new("lifecycle");
    let config = config(file.path(), 16 * 1024, 2);
    let cache = config.clone().open().unwrap();

    assert!(matches!(config.clone().open(), Err(CacheError::Locked)));

    assert_eq!(
        cache
            .put(
                "expired",
                "value",
                PutOptions {
                    expires_at_unix_ms: Some(now_ms().saturating_sub(1)),
                },
            )
            .unwrap(),
        PutOutcome::Rejected(RejectReason::AlreadyExpired)
    );
    cache
        .put(
            "ttl",
            "value",
            PutOptions {
                expires_at_unix_ms: Some(now_ms() + 20),
            },
        )
        .unwrap();
    std::thread::sleep(Duration::from_millis(30));
    assert_eq!(cache.get(b"ttl").unwrap(), None);

    cache
        .put("before-clear", "value", PutOptions::default())
        .unwrap();
    cache.clear().unwrap();
    assert_eq!(cache.get(b"before-clear").unwrap(), None);
    cache
        .put("after-clear", "value", PutOptions::default())
        .unwrap();
    cache.flush().unwrap();
    drop(cache);

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"before-clear").unwrap(), None);
    assert_eq!(
        reopened.get(b"after-clear").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn corrupted_payload_degrades_to_a_miss() {
    let file = TestFile::new("corrupt");
    let config = config(file.path(), 16 * 1024, 2);
    let cache = config.clone().open().unwrap();
    cache.put("key", "payload", PutOptions::default()).unwrap();
    cache.flush().unwrap();
    drop(cache);

    let raw = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file.path())
        .unwrap();
    let value_offset = SUPERBLOCK_AREA_SIZE + REGION_HEADER_SIZE + 64 + 3;
    raw.write_at(&[0xff], value_offset).unwrap();
    raw.sync_data().unwrap();
    drop(raw);

    let reopened = DiskCache::open(config).unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
}

#[test]
fn dirty_restart_replays_a_complete_record_from_the_initial_checkpoint() {
    let file = TestFile::new("unclean");
    let config = config(file.path(), 16 * 1024, 2);
    let cache = config.clone().open().unwrap();
    cache
        .put("not-checkpointed", "value", PutOptions::default())
        .unwrap();
    drop(cache);

    let reopened = config.open().unwrap();
    assert_eq!(
        reopened.get(b"not-checkpointed").unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn corrupted_newer_tombstone_never_resurrects_an_older_value() {
    let file = TestFile::new("corrupt-tombstone");
    let config = config(file.path(), 16 * 1024, 2);
    let cache = config.clone().open().unwrap();
    cache.put("key", "old", PutOptions::default()).unwrap();
    cache.flush().unwrap();
    cache.remove(b"key").unwrap();
    cache.flush().unwrap();
    drop(cache);

    let raw = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file.path())
        .unwrap();
    let old_record_len = 96;
    let tombstone_key_offset = SUPERBLOCK_AREA_SIZE + REGION_HEADER_SIZE + old_record_len + 64;
    raw.write_at(&[0xff], tombstone_key_offset).unwrap();
    raw.sync_data().unwrap();
    drop(raw);

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), None);
    assert_eq!(reopened.stats().recovered_entries, 0);
}

#[test]
fn recovery_replays_wrapped_regions_in_logical_fifo_order() {
    let file = TestFile::new("logical-recovery-order");
    let config = config(file.path(), 8 * 1024, 3).with_index_slots(8);
    let cache = config.clone().open().unwrap();
    let large = vec![7_u8; 900];

    // Four large records fill each 4 KiB payload area. Place the old value in
    // region 2, then wrap its tombstone into the physically earlier region 0.
    for id in 0..8 {
        cache
            .put(format!("fill-{id}"), &large, PutOptions::default())
            .unwrap();
    }
    cache.put("victim", &large, PutOptions::default()).unwrap();
    for id in 0..3 {
        cache
            .put(format!("tail-{id}"), &large, PutOptions::default())
            .unwrap();
    }
    cache.put("pad", "", PutOptions::default()).unwrap();
    cache.remove(b"victim").unwrap();

    // Fill, then replace every index start slot after the tombstone. A physical
    // region-id scan would forget that tombstone before seeing the older value.
    let mut keys_by_bucket: [Vec<String>; 8] = std::array::from_fn(|_| Vec::new());
    for candidate in 0..10_000 {
        let key = format!("pressure-{candidate}");
        let bucket = index_bucket(key.as_bytes(), keys_by_bucket.len());
        if keys_by_bucket[bucket].len() < 3 {
            keys_by_bucket[bucket].push(key);
        }
        if keys_by_bucket.iter().all(|keys| keys.len() == 3) {
            break;
        }
    }
    assert!(keys_by_bucket.iter().all(|keys| keys.len() == 3));
    for round in 0..3 {
        for keys in &keys_by_bucket {
            cache.put(&keys[round], "", PutOptions::default()).unwrap();
        }
    }
    cache.flush().unwrap();
    drop(cache);

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"victim").unwrap(), None);
}

#[test]
fn open_rejects_limits_that_the_disk_format_or_index_cannot_represent() {
    let file = TestFile::new("format-limits");
    let oversized_key = CacheConfig::new(file.path(), SUPERBLOCK_AREA_SIZE + 64 * 1024 * 1024)
        .with_max_key_size(64 * 1024 + 1);
    assert!(matches!(
        oversized_key.open(),
        Err(CacheError::InvalidConfig(_))
    ));

    let oversized_region = 32 * 1024 * 1024 + 4096;
    let unrepresentable_offset =
        CacheConfig::new(file.path(), SUPERBLOCK_AREA_SIZE + oversized_region * 2)
            .with_region_size(oversized_region);
    assert!(matches!(
        unrepresentable_offset.open(),
        Err(CacheError::InvalidConfig(_))
    ));

    let unrepresentable_index = config(file.path(), 16 * 1024, 2).with_index_slots(usize::MAX);
    assert!(matches!(
        unrepresentable_index.open(),
        Err(CacheError::InvalidConfig(_))
    ));

    let insufficient_memory = config(file.path(), 16 * 1024, 2).with_memory_budget(1);
    assert!(matches!(
        insufficient_memory.open(),
        Err(CacheError::InvalidConfig(_))
    ));

    for invalid_resources in [
        config(file.path(), 16 * 1024, 2).with_submission_queue_depths(0, 1),
        config(file.path(), 16 * 1024, 2).with_submission_queue_depths(1, 65_537),
        config(file.path(), 16 * 1024, 2).with_io_queue_depth(0),
        config(file.path(), 16 * 1024, 2).with_io_queue_depth(4097),
        config(file.path(), 16 * 1024, 2).with_write_budget(0),
        config(file.path(), 16 * 1024, 2)
            .with_backpressure(BackpressurePolicy::Timeout(Duration::MAX)),
    ] {
        assert!(matches!(
            invalid_resources.open(),
            Err(CacheError::InvalidConfig(_))
        ));
    }
    assert!(!file.path().exists());
}

#[test]
fn some_zero_is_rejected_as_an_expired_unix_timestamp() {
    let file = TestFile::new("zero-ttl");
    let cache = config(file.path(), 16 * 1024, 2).open().unwrap();

    assert_eq!(
        cache
            .put(
                "key",
                "value",
                PutOptions {
                    expires_at_unix_ms: Some(0),
                },
            )
            .unwrap(),
        PutOutcome::Rejected(RejectReason::AlreadyExpired)
    );
    assert_eq!(cache.get(b"key").unwrap(), None);
    assert_eq!(cache.stats().rejected, 1);
}

#[test]
fn shrunk_runtime_limits_can_still_remove_a_recovered_long_key() {
    let file = TestFile::new("shrunk-limits");
    let original = config(file.path(), 16 * 1024, 2).with_max_value_size(6000);
    let long_key = vec![b'k'; 200];
    let large_value = vec![b'v'; 4000];
    let cache = original.clone().open().unwrap();
    cache
        .put(&long_key, &large_value, PutOptions::default())
        .unwrap();
    cache.flush().unwrap();
    drop(cache);

    let restricted = original
        .clone()
        .with_index_slots(64)
        .with_max_key_size(64)
        .with_max_value_size(1);
    let reopened = restricted.open().unwrap();
    assert_eq!(reopened.get(&long_key).unwrap(), Some(large_value.clone()));
    assert_eq!(
        reopened
            .put("new-large", &large_value, PutOptions::default())
            .unwrap(),
        PutOutcome::Rejected(RejectReason::ValueTooLarge)
    );
    assert_eq!(reopened.remove(&long_key).unwrap(), RemoveOutcome::Removed);
    assert_eq!(reopened.get(&long_key).unwrap(), None);
    reopened.flush().unwrap();
    drop(reopened);

    let final_open = original.open().unwrap();
    assert_eq!(final_open.get(&long_key).unwrap(), None);
}

#[test]
fn persistent_hash_seed_mismatch_is_rejected_without_touching_data() {
    let file = TestFile::new("hash-seed-mismatch");
    let config = config(file.path(), 16 * 1024, 2).with_hash_seed(7);
    let cache = config.clone().open().unwrap();
    cache.put("key", "value", PutOptions::default()).unwrap();
    cache.flush().unwrap();
    drop(cache);

    assert!(matches!(
        config.clone().with_hash_seed(8).open(),
        Err(CacheError::InvalidConfig(_))
    ));
    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn close_is_idempotent_releases_the_lock_and_precedes_argument_validation() {
    let file = TestFile::new("close");
    let config = config(file.path(), 16 * 1024, 2);
    let cache = config.clone().open().unwrap();
    cache.put("key", "value", PutOptions::default()).unwrap();
    cache.close().unwrap();
    cache.close().unwrap();

    assert!(matches!(cache.get(b"key"), Err(CacheError::Closed)));
    assert!(matches!(
        cache.put(
            "key",
            "value",
            PutOptions {
                expires_at_unix_ms: Some(0),
            }
        ),
        Err(CacheError::Closed)
    ));
    assert!(matches!(
        cache.remove(&vec![b'x'; 1024]),
        Err(CacheError::Closed)
    ));
    assert!(matches!(cache.flush(), Err(CacheError::Closed)));
    assert!(matches!(cache.clear(), Err(CacheError::Closed)));

    let reopened = config.open().unwrap();
    assert_eq!(reopened.get(b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn unsupported_or_mixed_format_versions_are_rejected_without_rewrite() {
    let unsupported = sparse_golden(include_str!("fixtures/format_v1/superblock_v2.golden"));
    for rewrite_both_slots in [false, true] {
        let file = TestFile::new(if rewrite_both_slots {
            "unsupported-v2"
        } else {
            "mixed-v1-v2"
        });
        let config = config(file.path(), 16 * 1024, 2);
        let cache = config.clone().open().unwrap();
        cache.put("key", "value", PutOptions::default()).unwrap();
        cache.flush().unwrap();
        drop(cache);

        let raw = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        raw.write_at(&unsupported, 0).unwrap();
        if rewrite_both_slots {
            raw.write_at(&unsupported, 4096).unwrap();
        }
        raw.sync_data().unwrap();
        drop(raw);
        let before = std::fs::read(file.path()).unwrap();

        match config.open() {
            Err(CacheError::InvalidConfig(message)) => {
                assert!(message.contains("unsupported cache format version 2"));
            }
            _ => panic!("unsupported format must be rejected"),
        }
        assert_eq!(std::fs::read(file.path()).unwrap(), before);
    }
}

#[test]
fn an_unrecognized_nonempty_file_is_never_formatted_as_a_cache() {
    let cases = [
        ("foreign-format", b"this is not a cache file".to_vec()),
        ("foreign-after-short-zero-prefix", {
            let mut contents = vec![0_u8; 6144];
            contents[5000] = 0x5a;
            contents
        }),
        ("foreign-after-partial-marker", {
            let mut contents = vec![0_u8; 6144];
            contents[0] = b'C';
            contents[5000] = 0x5a;
            contents
        }),
        ("foreign-in-second-slot", {
            let mut contents = vec![0_u8; SUPERBLOCK_AREA_SIZE as usize];
            contents[0] = b'C';
            contents[4096..4103].copy_from_slice(b"foreign");
            contents
        }),
        ("foreign-after-full-zero-prefix", {
            let mut contents = vec![0_u8; (SUPERBLOCK_AREA_SIZE + 1) as usize];
            *contents.last_mut().unwrap() = 0xa5;
            contents
        }),
    ];

    for (name, contents) in cases {
        let file = TestFile::new(name);
        std::fs::write(file.path(), &contents).unwrap();
        let config = config(file.path(), 16 * 1024, 2);

        assert!(matches!(
            config.open(),
            Err(CacheError::CorruptMetadata(
                "unrecognized non-empty cache file"
            ))
        ));
        assert_eq!(std::fs::read(file.path()).unwrap(), contents);
    }
}

#[test]
fn zero_filled_metadata_extent_is_reserved_for_interrupted_format_v1() {
    for length in [4096, SUPERBLOCK_AREA_SIZE as usize] {
        let file = TestFile::new(&format!("zero-metadata-{length}"));
        std::fs::write(file.path(), vec![0_u8; length]).unwrap();
        let config = config(file.path(), 16 * 1024, 2);

        let cache = config.open().unwrap();
        assert_eq!(cache.get(b"never-written").unwrap(), None);
    }
}

#[test]
fn published_delete_corruption_never_resurrects_an_older_region() {
    let fixture = sparse_golden(include_str!("fixtures/format_v1/cache_deleted.golden"));
    let cases: [(&str, &[u64], bool); 4] = [
        (
            "corrupt-newer-region-after-delete",
            &[SUPERBLOCK_AREA_SIZE + 16 * 1024 + REGION_HEADER_SIZE - 4],
            false,
        ),
        ("corrupt-older-dirty-superblock", &[4096 + 4092], true),
        ("corrupt-latest-clean-superblock", &[4092], false),
        ("corrupt-both-superblocks", &[4092, 4096 + 4092], false),
    ];

    for (name, offsets, preserves_checkpoint) in cases {
        let file = TestFile::new(name);
        std::fs::write(file.path(), &fixture).unwrap();
        let config = config(file.path(), 16 * 1024, 2);
        let raw = OpenOptions::new()
            .read(true)
            .write(true)
            .open(file.path())
            .unwrap();
        for &offset in offsets {
            let mut byte = [0_u8; 1];
            raw.read_at(&mut byte, offset).unwrap();
            byte[0] ^= 0xff;
            raw.write_at(&byte, offset).unwrap();
        }
        raw.sync_data().unwrap();
        drop(raw);

        let reopened = config.open().unwrap();
        assert_eq!(reopened.get(b"victim").unwrap(), None, "{name}");
        if preserves_checkpoint {
            assert_eq!(
                reopened.get(b"canary").unwrap(),
                Some(b"present".to_vec()),
                "{name}"
            );
            assert_eq!(reopened.stats().recovered_entries, 1, "{name}");
        } else {
            assert_eq!(reopened.get(b"canary").unwrap(), None, "{name}");
            assert_eq!(reopened.stats().recovered_entries, 0, "{name}");
        }
    }
}

#[test]
fn committed_format_v1_cache_fixture_opens_without_rewriting_the_v1_data_extent() {
    let file = TestFile::new("format-v1-fixture");
    let fixture = sparse_golden(include_str!("fixtures/format_v1/cache_deleted.golden"));
    std::fs::write(file.path(), &fixture).unwrap();
    let config = config(file.path(), 16 * 1024, 2);

    let cache = config.open().unwrap();
    assert_eq!(cache.get(b"victim").unwrap(), None);
    assert_eq!(cache.get(b"canary").unwrap(), Some(b"present".to_vec()));
    assert_eq!(cache.stats().recovered_entries, 1);
    drop(cache);

    let upgraded = std::fs::read(file.path()).unwrap();
    assert_eq!(&upgraded[..fixture.len()], fixture.as_slice());
    assert!(
        upgraded.len() > fixture.len(),
        "v0.7 should append a recovery checkpoint extension"
    );
}
