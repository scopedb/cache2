use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

struct TestFile(PathBuf);

impl TestFile {
    fn new(name: &str) -> Self {
        let id = NEXT_PATH.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!("cache-rs-m8-{name}-{}-{id}", std::process::id())))
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
        .with_max_key_size(64)
        .with_max_value_size(1024)
        .with_submission_queue_depths(2, 2)
}

#[test]
fn diagnostics_validates_the_resource_plan_without_touching_the_path() {
    let file = TestFile::new("diagnostics");
    let valid_config = config(&file.0);
    let diagnostics = valid_config.diagnostics().unwrap();
    assert!(!file.0.exists());
    assert_eq!(diagnostics.path, file.0);
    assert_eq!(diagnostics.region_count, 3);
    assert_eq!(diagnostics.data_file_len_bytes, DATA_OFFSET + 3 * 16 * 1024);
    assert!(diagnostics.planned_memory_bytes <= diagnostics.memory_budget_bytes);
    assert!(diagnostics.checkpoint_slot_bytes >= CHECKPOINT_SLOT_HEADER_SIZE as u64);
    assert_eq!(
        diagnostics.checkpoint_accounting_bytes,
        u64::from(diagnostics.region_count) * std::mem::size_of::<u64>() as u64
            + std::mem::size_of::<NamespaceUsage>() as u64
    );

    let exact_budget = usize::try_from(diagnostics.planned_memory_bytes).unwrap();
    assert!(
        valid_config
            .clone()
            .with_memory_budget(exact_budget)
            .diagnostics()
            .is_ok()
    );
    assert!(matches!(
        valid_config
            .clone()
            .with_memory_budget(exact_budget - 1)
            .diagnostics(),
        Err(CacheError::InvalidConfig(_))
    ));

    let invalid_path = TestFile::new("invalid-diagnostics");
    let invalid = config(&invalid_path.0).with_origin_fill_protection(OriginFillConfig::new(0, 1));
    assert!(matches!(
        invalid.diagnostics(),
        Err(CacheError::InvalidConfig(_))
    ));
    assert!(!invalid_path.0.exists());

    let invalid_namespace_path = TestFile::new("invalid-namespace-diagnostics");
    let invalid_namespace = config(&invalid_namespace_path.0)
        .with_namespace(NamespaceConfig::new(7).with_capacity_bytes(0));
    assert!(matches!(
        invalid_namespace.diagnostics(),
        Err(CacheError::InvalidConfig(_))
    ));
    assert!(!invalid_namespace_path.0.exists());

    let invalid_daily_path = TestFile::new("invalid-daily-diagnostics");
    let invalid_daily = config(&invalid_daily_path.0).with_daily_host_write_budget(0);
    assert!(matches!(
        invalid_daily.diagnostics(),
        Err(CacheError::InvalidConfig(_))
    ));
    assert!(!invalid_daily_path.0.exists());

    #[cfg(not(target_os = "linux"))]
    {
        let direct_path = TestFile::new("invalid-direct-diagnostics");
        assert!(matches!(
            config(&direct_path.0)
                .with_io_mode(IoMode::Direct)
                .diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!direct_path.0.exists());
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
    {
        let uring_path = TestFile::new("invalid-uring-diagnostics");
        assert!(matches!(
            config(&uring_path.0)
                .with_io_engine(IoEngineKind::IoUring)
                .diagnostics(),
            Err(CacheError::InvalidConfig(_))
        ));
        assert!(!uring_path.0.exists());
    }
}

#[test]
fn explicit_format_only_accepts_a_missing_or_empty_path() {
    let file = TestFile::new("format-empty");
    let cache = config(&file.0).format_empty().unwrap();
    cache
        .put(b"preserved", b"value", PutOptions::default())
        .unwrap();
    cache.close().unwrap();

    assert!(matches!(
        config(&file.0).format_empty(),
        Err(CacheError::InvalidConfig(_))
    ));
    let reopened = config(&file.0).open().unwrap();
    assert_eq!(reopened.get(b"preserved").unwrap(), Some(b"value".to_vec()));
    reopened.close().unwrap();

    let empty = TestFile::new("format-existing-empty");
    std::fs::write(&empty.0, []).unwrap();
    config(&empty.0).format_empty().unwrap().close().unwrap();
}

#[test]
fn reset_requires_an_unlocked_recognized_v1_and_never_restores_old_values() {
    let missing = TestFile::new("reset-missing");
    assert!(matches!(
        config(&missing.0).reset_existing(),
        Err(CacheError::Io(error)) if error.kind() == io::ErrorKind::NotFound
    ));
    assert!(!missing.0.exists());

    let unknown = TestFile::new("reset-unknown");
    let unknown_bytes = b"not a cache-rs file";
    std::fs::write(&unknown.0, unknown_bytes).unwrap();
    assert!(matches!(
        config(&unknown.0).reset_existing(),
        Err(CacheError::InvalidConfig(_))
    ));
    assert_eq!(std::fs::read(&unknown.0).unwrap(), unknown_bytes);

    let file = TestFile::new("reset-recognized");
    let cache = config(&file.0).format_empty().unwrap();
    cache.put(b"old", b"value", PutOptions::default()).unwrap();
    cache.flush().unwrap();
    assert!(matches!(
        config(&file.0).reset_existing(),
        Err(CacheError::Locked)
    ));
    assert_eq!(cache.get(b"old").unwrap(), Some(b"value".to_vec()));
    cache.close().unwrap();

    let reset = config(&file.0).reset_existing().unwrap();
    assert_eq!(reset.get(b"old").unwrap(), None);
    reset.close().unwrap();
    let reopened = config(&file.0).open().unwrap();
    assert_eq!(reopened.get(b"old").unwrap(), None);
    reopened.close().unwrap();
}

#[test]
fn metrics_classify_results_errors_latency_and_lifecycle_once() {
    let file = TestFile::new("metrics");
    let cache = config(&file.0).open().unwrap();

    assert_eq!(cache.get(b"key").unwrap(), None);
    assert_eq!(
        cache.put(b"key", b"value", PutOptions::default()).unwrap(),
        PutOutcome::Stored
    );
    assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
    assert!(matches!(
        cache
            .put(
                b"expired",
                b"value",
                PutOptions {
                    expires_at_unix_ms: Some(0),
                },
            )
            .unwrap(),
        PutOutcome::Rejected(RejectReason::AlreadyExpired)
    ));
    assert_eq!(cache.remove(b"key").unwrap(), RemoveOutcome::Removed);
    cache.flush().unwrap();

    let snapshot = cache.metrics_snapshot();
    let get = &snapshot.operations[CacheOperation::Get as usize];
    assert_eq!(get.result_count(RequestResultClass::Hit), 1);
    assert_eq!(get.result_count(RequestResultClass::Miss), 1);
    assert_eq!(get.latency.count, 2);
    let put = &snapshot.operations[CacheOperation::Put as usize];
    assert_eq!(put.result_count(RequestResultClass::Stored), 1);
    assert_eq!(put.result_count(RequestResultClass::Rejected), 1);
    assert_eq!(put.latency.count, 2);
    assert_eq!(
        snapshot.operations[CacheOperation::Remove as usize]
            .result_count(RequestResultClass::Removed),
        1
    );
    let encoded = snapshot.to_openmetrics();
    assert!(encoded.contains("cache_rs_request_duration_seconds_bucket"));
    assert!(!encoded.contains("expired"));
    assert!(!encoded.contains(file.0.to_string_lossy().as_ref()));
    assert!(cache.health_snapshot().is_ready());

    cache.close().unwrap();
    assert!(matches!(cache.get(b"closed"), Err(CacheError::Closed)));
    let closed = cache.metrics_snapshot();
    assert_eq!(
        closed.operations[CacheOperation::Close as usize].result_count(RequestResultClass::Success),
        1
    );
    assert_eq!(
        closed.operations[CacheOperation::Get as usize].error_count(CacheErrorClass::Closed),
        1
    );
    assert!(
        closed
            .state_transitions
            .iter()
            .any(|event| event.to == CacheStatus::Closed)
    );
    assert!(!cache.health_snapshot().is_ready());
}

#[test]
fn configured_origin_fill_guard_is_bounded_and_reported() {
    let file = TestFile::new("origin-guard");
    let cache = config(&file.0)
        .with_origin_fill_protection(OriginFillConfig::new(2, 1))
        .open()
        .unwrap();
    let first = cache.try_begin_origin_fill().unwrap();
    assert_eq!(
        cache.try_begin_origin_fill().unwrap_err(),
        OriginFillRejectReason::ConcurrencyLimited
    );
    drop(first);
    let second = cache.try_begin_origin_fill().unwrap();
    drop(second);
    assert_eq!(
        cache.try_begin_origin_fill().unwrap_err(),
        OriginFillRejectReason::RateLimited
    );
    let stats = cache.origin_fill_stats();
    assert_eq!(stats.admitted, 2);
    assert_eq!(stats.in_flight, 0);
    assert_eq!(cache.health_snapshot().origin_fills, stats);
    cache.close().unwrap();

    let disabled_file = TestFile::new("origin-disabled");
    let disabled = config(&disabled_file.0).open().unwrap();
    assert_eq!(
        disabled.try_begin_origin_fill().unwrap_err(),
        OriginFillRejectReason::Disabled
    );
    disabled.close().unwrap();
}
