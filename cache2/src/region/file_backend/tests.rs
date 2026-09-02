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

use super::*;
use crate::index::{IndexEntry, PackedLocation};
use crate::index_storage::{INDEX_IMAGE_SLOTS_PER_PAGE, IndexSlot};
use crate::io_backend::testing::{FaultAction, FaultBackend, FaultEvent, FaultHandle};
use crate::io_engine::{BackendIoEngine, IoEngine};
use crate::record_codec::{hash_key, required_record_bytes};
use crate::recovery::{DATA_REGION_AREA_OFFSET, DataGeometry, PersistentId};
use crate::region::core::RegionStageValue;
use crate::region_reader::{ReadCandidate, ReadCompletion, ReadPlan, plan_read};
use crate::region_staging::{RegionStaging, StagedRecord};
use crate::resources::{ResourceController, ResourceLimits};
use crate::snapshot::StartupMode;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;
#[cfg(unix)]
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

fn eventually_admitted<T>(mut put: impl FnMut() -> io::Result<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match put() {
            Ok(value) => return value,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
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

struct TestDirectory {
    root: PathBuf,
    files: RegionFiles,
}

impl TestDirectory {
    fn new() -> Self {
        let ordinal = NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("cache2-region-{}-{ordinal}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir(&root).unwrap();
        let files = RegionFiles::new(
            root.join("data"),
            root.join("state"),
            root.join("recovery.image"),
        );
        Self { root, files }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[cfg(unix)]
#[test]
fn state_page_reads_stop_after_the_interrupted_retry_budget() {
    let directory = TestDirectory::new();
    let (state, faults) = FaultBackend::open(&directory.files.state).unwrap();
    state.set_len(STATE_FILE_SIZE as u64).unwrap();
    faults.arm(FaultEvent::Read, 1, FaultAction::ErrorAlways(libc::EINTR));

    let error = read_state_pages(&state).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(
        faults
            .events()
            .iter()
            .filter(|event| **event == FaultEvent::Read)
            .count(),
        crate::io_backend::MAX_INTERRUPTED_RETRIES + 1
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileSystemFault {
    Rename,
    SyncParent,
}

#[derive(Clone, Default)]
struct FileSystemFaultHandle {
    armed: Arc<Mutex<Option<FileSystemFault>>>,
}

impl FileSystemFaultHandle {
    fn arm(&self, fault: FileSystemFault) {
        *self.armed.lock().unwrap() = Some(fault);
    }

    fn check(&self, fault: FileSystemFault) -> io::Result<()> {
        let mut armed = self.armed.lock().unwrap();
        if *armed == Some(fault) {
            *armed = None;
            Err(io::Error::from_raw_os_error(5))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct FaultRegionFileSystem {
    io: FaultHandle,
    file_system: FileSystemFaultHandle,
}

impl FaultRegionFileSystem {
    fn new() -> (Self, FaultHandle, FileSystemFaultHandle) {
        let io = FaultHandle::default();
        let file_system = FileSystemFaultHandle::default();
        (
            Self {
                io: io.clone(),
                file_system: file_system.clone(),
            },
            io,
            file_system,
        )
    }
}

impl RegionFileSystem for FaultRegionFileSystem {
    type File = FaultBackend;

    fn open(&self, path: &Path, create: bool) -> io::Result<Self::File> {
        if create {
            FaultBackend::open_with_handle(path, self.io.clone())
        } else {
            FaultBackend::open_existing_with_handle(path, self.io.clone())
        }
    }

    fn create_new(&self, path: &Path) -> io::Result<Self::File> {
        FaultBackend::create_new_buffered_with_handle(path, self.io.clone())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn rename(&self, source: &Path, destination: &Path) -> io::Result<()> {
        self.file_system.check(FileSystemFault::Rename)?;
        std::fs::rename(source, destination)
    }

    fn sync_parent(&self, path: &Path) -> io::Result<()> {
        self.file_system.check(FileSystemFault::SyncParent)?;
        SystemRegionFileSystem.sync_parent(path)
    }
}

fn persistent_id(byte: u8) -> PersistentId {
    PersistentId::from_bytes([byte; 16]).unwrap()
}

fn test_data_superblock_with_regions(region_count: u32) -> DataSuperblock {
    let region_size = 2 * RECOVERY_PAGE_SIZE as u64;
    DataSuperblock {
        generation: 1,
        cache_uuid: persistent_id(1),
        data_identity: persistent_id(2),
        geometry: DataGeometry {
            data_file_len: DataGeometry::expected_file_len(region_size, region_count).unwrap(),
            region_size,
            region_count,
        },
        hash_seed: 3,
        config_fingerprint: 4,
    }
}

fn test_data_superblock() -> DataSuperblock {
    test_data_superblock_with_regions(REGION_SHARDS + 1)
}

fn data_path_superblock() -> DataSuperblock {
    let region_size = 8 * 1024 * 1024;
    DataSuperblock {
        geometry: DataGeometry {
            data_file_len: DataGeometry::expected_file_len(region_size, REGION_SHARDS + 1).unwrap(),
            region_size,
            region_count: REGION_SHARDS + 1,
        },
        ..test_data_superblock()
    }
}

fn data_path_resources() -> ResourceController {
    ResourceController::try_new(ResourceLimits {
        memory_limit_bytes: 32 * 1024 * 1024,
        reserved_memory_bytes: 0,
    })
    .unwrap()
}

#[cfg(unix)]
#[test]
#[ignore = "extended recovery qualification; run with `cargo test --package cache2 --lib --all-features -- --ignored`"]
fn external_process_kill_recovery_contract() {
    const CHILD_CASE: &str = "CACHE2_CRASH_CHILD_CASE";
    const CHILD_ROOT: &str = "CACHE2_CRASH_CHILD_ROOT";

    if let Ok(case) = std::env::var(CHILD_CASE) {
        let root = PathBuf::from(std::env::var_os(CHILD_ROOT).expect("child root is set"));
        let files = RegionFiles::new(
            root.join("data"),
            root.join("state"),
            root.join("recovery.image"),
        );
        run_crash_child(&case, files);
    }

    for (case, expect_clean) in [
        ("open", false),
        ("write", false),
        ("drain", false),
        ("warm-data", false),
        ("warm-image", false),
        ("clean-state", true),
    ] {
        let directory = TestDirectory::new();
        let data = data_path_superblock();
        let mut initial =
            RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data)).unwrap();
        eventually_admitted(|| initial.put_value(b"survivor", b"old"));
        initial.drain().unwrap();
        initial.close_warm().unwrap();

        let status = Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("region::file_backend::tests::external_process_kill_recovery_contract")
            .arg("--ignored")
            .arg("--nocapture")
            .env(CHILD_CASE, case)
            .env(CHILD_ROOT, &directory.root)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_eq!(
            status.signal(),
            Some(9),
            "crash case {case} did not SIGKILL"
        );

        let mut reopened =
            RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data)).unwrap();
        if expect_clean {
            assert_eq!(reopened.startup(), StartupMode::Warm, "{case}");
            assert_eq!(
                reopened.get_value(b"survivor").unwrap().unwrap().value(),
                b"old",
                "{case}",
            );
        } else {
            assert_eq!(reopened.startup(), StartupMode::Cold, "{case}");
            assert!(reopened.get_value(b"survivor").unwrap().is_none(), "{case}",);
        }
        reopened.close_fast().unwrap();
    }
}

#[cfg(unix)]
fn run_crash_child(case: &str, files: RegionFiles) -> ! {
    let data = data_path_superblock();
    match case {
        "open" => {
            let _store = RegionStore::open(4096, FileRegionBackend::new(files, data)).unwrap();
            crate::io_backend::testing::kill_process();
        }
        "write" | "drain" => {
            let store = RegionStore::open(4096, FileRegionBackend::new(files, data)).unwrap();
            eventually_admitted(|| store.put_value(b"replacement", b"new"));
            if case == "drain" {
                store.drain().unwrap();
            }
            crate::io_backend::testing::kill_process();
        }
        "warm-data" | "warm-image" | "clean-state" => {
            let (file_system, faults, _) = FaultRegionFileSystem::new();
            let mut store = RegionStore::open(
                4096,
                FileRegionBackend::new_with_file_system(files, data, file_system),
            )
            .unwrap();
            let point = match case {
                "warm-data" => SyncPoint::WarmData,
                "warm-image" => SyncPoint::RecoveryImage,
                "clean-state" => SyncPoint::CleanState,
                _ => unreachable!(),
            };
            faults.arm(FaultEvent::Sync(point), 1, FaultAction::KillAfter);
            let _ = store.close_warm();
            panic!("crash fault did not terminate the child");
        }
        _ => panic!("unknown crash child case: {case}"),
    }
}

fn production_data_superblock(region_size: u64) -> DataSuperblock {
    let region_count = REGION_SHARDS + 1;
    DataSuperblock {
        generation: 1,
        cache_uuid: persistent_id(21),
        data_identity: persistent_id(22),
        geometry: DataGeometry {
            data_file_len: DataGeometry::expected_file_len(region_size, region_count).unwrap(),
            region_size,
            region_count,
        },
        hash_seed: 23,
        config_fingerprint: 24,
    }
}

fn key_for_shard(data: DataSuperblock, shard: u64, ordinal: u64) -> Vec<u8> {
    for attempt in 0_u64..10_000 {
        let key = format!("shard-{shard}-object-{ordinal}-{attempt}").into_bytes();
        if hash_key(data.hash_seed, &key) % u64::from(REGION_SHARDS) == shard {
            return key;
        }
    }
    panic!("could not find a deterministic key for shard {shard}");
}

#[test]
fn configured_read_wait_is_bounded_and_cancel_safe() {
    let directory = TestDirectory::new();
    let data = production_data_superblock(512 * 1024);
    let runtime_config = RuntimeConfig::default()
        .with_read_io_workers(1)
        .with_read_io_wait_timeout(Duration::from_millis(30))
        .with_l1_capacity_bytes(0)
        .with_statistics(true);
    let mut store = RegionStore::open(
        4096,
        FileRegionBackend::new_with_configs(
            directory.files.clone(),
            data,
            REGION_SHARDS,
            runtime_config,
        ),
    )
    .unwrap();
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_time()
        .build()
        .unwrap();
    eventually_admitted(|| store.put_value(b"queued-read", b"local-value"));
    store.drain().unwrap();
    let slot = store
        .runtime()
        .unwrap()
        .data_plane()
        .unwrap()
        .reserve_read_slot_for_test();
    tokio_runtime.block_on(async {
        let mut cancelled = Box::pin(store.get_value_async(b"queued-read", tokio_runtime.handle()));
        std::future::poll_fn(|context| {
            match std::future::Future::poll(cancelled.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(_) => panic!("saturated read must enter the wait queue"),
            }
        })
        .await;
        drop(cancelled);
    });
    let value = tokio_runtime.block_on(async {
        let mut waiting = Box::pin(store.get_value_async(b"queued-read", tokio_runtime.handle()));
        std::future::poll_fn(|context| {
            match std::future::Future::poll(waiting.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(_) => {
                    panic!("a cancelled read must release its wait-queue permit")
                }
            }
        })
        .await;
        drop(slot);
        waiting.await.unwrap().unwrap()
    });
    assert_eq!(value.value(), b"local-value");
    drop(value);
    let blocked = store
        .runtime()
        .unwrap()
        .data_plane()
        .unwrap()
        .reserve_read_slot_for_test();
    tokio_runtime.block_on(async {
        let mut waiting = Box::pin(store.get_value_async(b"queued-read", tokio_runtime.handle()));
        std::future::poll_fn(|context| {
            match std::future::Future::poll(waiting.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(_) => {
                    panic!("saturated read must enter the bounded wait queue")
                }
            }
        })
        .await;
        let queue_full = match store
            .get_value_async(b"queued-read", tokio_runtime.handle())
            .await
        {
            Err(error) => error,
            Ok(_) => panic!("a second saturated read must not enter a full wait queue"),
        };
        assert_eq!(queue_full.kind(), io::ErrorKind::WouldBlock);
        let timed_out = match waiting.await {
            Err(error) => error,
            Ok(_) => panic!("a queued read must honor its configured deadline"),
        };
        assert_eq!(timed_out.kind(), io::ErrorKind::TimedOut);
    });
    drop(blocked);
    let snapshot = store.snapshot().unwrap();
    assert_eq!(snapshot.l2_read_overloads, 2);
    assert!(snapshot.l2_read_wait_ns > 0);
    store.close_fast().unwrap();
}

#[test]
fn queued_l2_read_does_not_pin_warm_close() {
    let directory = TestDirectory::new();
    let data = production_data_superblock(512 * 1024);
    let runtime_config = RuntimeConfig::default()
        .with_read_io_workers(1)
        .with_read_io_wait_timeout(Duration::from_secs(1))
        .with_l1_capacity_bytes(0);
    let mut store = RegionStore::open(
        4096,
        FileRegionBackend::new_with_configs(
            directory.files.clone(),
            data,
            REGION_SHARDS,
            runtime_config.clone(),
        ),
    )
    .unwrap();
    let tokio_runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    eventually_admitted(|| store.put_value(b"queued-close", b"value"));
    store.drain().unwrap();
    let plane = store.data_plane_handle().unwrap();
    let slot = plane.reserve_read_slot_for_test();

    tokio_runtime.block_on(async {
        let mut waiting = Box::pin(plane.get_async(b"queued-close", tokio_runtime.handle()));
        std::future::poll_fn(|context| {
            match std::future::Future::poll(waiting.as_mut(), context) {
                std::task::Poll::Pending => std::task::Poll::Ready(()),
                std::task::Poll::Ready(_) => panic!("saturated read must enter the wait queue"),
            }
        })
        .await;

        store.close_warm().unwrap();
        drop(slot);
        let error = match waiting.await {
            Ok(_) => panic!("queued read must stop after warm close"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    });

    let mut reopened = RegionStore::open(
        4096,
        FileRegionBackend::new_with_configs(
            directory.files.clone(),
            data,
            REGION_SHARDS,
            runtime_config,
        ),
    )
    .unwrap();
    assert_eq!(reopened.startup(), StartupMode::Warm);
    reopened.close_fast().unwrap();
}

#[test]
fn production_data_plane_reads_mixed_chunks_rotates_and_warm_recovers() {
    let directory = TestDirectory::new();
    let data = production_data_superblock(512 * 1024);
    let runtime_config = RuntimeConfig {
        l1_capacity_bytes: 0,
        statistics: true,
        ..RuntimeConfig::default()
    };
    let mut store = RegionStore::open(
        4096,
        FileRegionBackend::new_with_configs(
            directory.files.clone(),
            data,
            REGION_SHARDS,
            runtime_config,
        ),
    )
    .unwrap();

    let mixed = [16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024];
    let mut expected = Vec::new();
    for (ordinal, size) in mixed.into_iter().enumerate() {
        let key = key_for_shard(data, ordinal as u64, ordinal as u64);
        let value = vec![(ordinal as u8) + 1; size];
        eventually_admitted(|| store.put_value(&key, &value));
        expected.push((key, value));
    }
    store.drain().unwrap();
    for (key, value) in &expected {
        assert_eq!(store.get_value(key).unwrap().unwrap().value(), value);
        assert_eq!(store.get_value(key).unwrap().unwrap().value(), value);
    }

    // A 256 KiB record leaves insufficient room for another same-shard
    // record in this test geometry. Repeated writes therefore consume the
    // free Region and then exercise reclaim-before-reuse. With one spare
    // Region, only the newest same-shard record is guaranteed to survive.
    let rotation_value = vec![0xa5; 256 * 1024];
    let mut recent = Vec::new();
    let rotations_before = store.detailed_snapshot().unwrap().region.rotations;
    for ordinal in 0..32 {
        let key = key_for_shard(data, 0, 100 + ordinal);
        eventually_admitted(|| store.put_value(&key, &rotation_value));
        store.drain().unwrap();
        recent.push(key);
    }
    assert_eq!(
        store.detailed_snapshot().unwrap().region.rotations - rotations_before,
        31,
        "one full-Region signal must cause exactly one rotation"
    );
    let reclaim = store.detailed_snapshot().unwrap().summary.reclaim;
    // The explicit drain may fence the optional reinsertion before its
    // reclaimer enters the mutation gate. Heat classification must still
    // offer the candidate, while either admission or a bounded skip is a
    // valid best-effort outcome.
    assert!(
        reclaim.reinsert_records + reclaim.reinsert_skipped > 0,
        "{reclaim:?}"
    );
    assert_eq!(reclaim.reinsert_bytes == 0, reclaim.reinsert_records == 0);
    assert!(reclaim.reinsert_bytes.saturating_mul(8) <= reclaim.bytes_read);
    for key in recent.iter().rev().take(1) {
        assert_eq!(
            store.get_value(key).unwrap().unwrap().value(),
            rotation_value
        );
    }

    let retained_hits: Vec<_> = (0..129)
        .map(|_| store.get_value(recent.last().unwrap()).unwrap().unwrap())
        .collect();
    assert!(
        retained_hits
            .iter()
            .all(|hit| hit.value() == rotation_value)
    );
    assert!(
        store.get_value(b"definite-index-miss").unwrap().is_none(),
        "an index miss must not acquire a retained-hit buffer"
    );

    // Retained zero-copy hits own their transient aligned allocations, but
    // cannot pin the runtime operation barrier or prevent a warm shutdown.
    store.close_warm().unwrap();
    assert_eq!(retained_hits[0].value(), rotation_value);
    drop(retained_hits);
    let mut recovered =
        RegionStore::open(4096, FileRegionBackend::new(directory.files.clone(), data)).unwrap();
    assert_eq!(recovered.startup(), StartupMode::Warm);
    assert_eq!(
        recovered
            .get_value(recent.last().unwrap())
            .unwrap()
            .unwrap()
            .value(),
        rotation_value
    );
    recovered.close_fast().unwrap();
}

#[test]
fn poisoned_runtime_gates_stop_workers_and_reject_warm_close() {
    for case in ["shard", "index"] {
        let directory = TestDirectory::new();
        let data = production_data_superblock(512 * 1024);
        let runtime_config = RuntimeConfig::default()
            .with_io_engine(crate::runtime_config::IoEngine::Posix)
            .with_read_io_workers(1)
            .with_write_io_workers(1)
            .with_l1_capacity_bytes(0)
            .with_managed_memory_limit_bytes(32 * 1024 * 1024)
            .with_write_flush_threshold_bytes(128 * 1024);
        let mut store = RegionStore::open(
            4096,
            FileRegionBackend::new_with_configs(
                directory.files.clone(),
                data,
                REGION_SHARDS,
                runtime_config.clone(),
            ),
        )
        .unwrap();

        let runtime = store.runtime().unwrap();
        match case {
            "shard" => runtime.data_plane().unwrap().poison_shard_for_test(0),
            "index" => runtime
                .core
                .index
                .storage()
                .poison_hash_partition_for_test(0),
            _ => unreachable!(),
        }
        let error = store.close_warm().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{case}");

        let mut reopened = RegionStore::open(
            4096,
            FileRegionBackend::new_with_configs(
                directory.files.clone(),
                data,
                REGION_SHARDS,
                runtime_config,
            ),
        )
        .unwrap();
        assert_eq!(reopened.startup(), StartupMode::Cold, "{case}");
        reopened.close_fast().unwrap();
    }
}

#[test]
fn fresh_metadata_assigns_four_active_shards_and_a_free_victim() {
    let data = test_data_superblock();
    let metadata = empty_region_metadata(data, 64, REGION_SHARDS).unwrap();
    let shards = usize::try_from(REGION_SHARDS).unwrap();

    assert_eq!(metadata.root.shard_count, REGION_SHARDS);
    assert_eq!(metadata.root.active_region_count, REGION_SHARDS);
    assert_eq!(metadata.root.free_region_count, 1);
    assert_eq!(metadata.root.max_seqno, u64::from(REGION_SHARDS));
    for shard_id in 0..REGION_SHARDS {
        let region = metadata.regions[usize::try_from(shard_id).unwrap()];
        assert_eq!(region.state, RegionMetadataState::Active);
        assert_eq!(region.queue_ordinal, shard_id);
        assert_eq!(region.created_seqno, u64::from(shard_id) + 1);
    }
    let free = metadata.regions[shards];
    assert_eq!(free.state, RegionMetadataState::Free);
    assert_eq!(free.queue_ordinal, 0);

    let manager = RegionManager::from_metadata(metadata).unwrap();
    assert_eq!(manager.active_regions(), &[0, 1, 2, 3]);
    assert_eq!(
        manager.free_regions().iter().copied().collect::<Vec<_>>(),
        vec![4]
    );
    assert_eq!(manager.next_seqno(), 5);
}

#[test]
fn four_tib_region_metadata_round_trips_into_runtime_authority() {
    const CAPACITY_BYTES: u64 = 4 * 1024 * 1024 * 1024 * 1024;
    const INDEX_SLOTS: usize = 512 * 1024 * 1024;
    const REGION_SIZE: u64 = 32 * 1024 * 1024;
    const REGION_COUNT: u32 = (CAPACITY_BYTES / REGION_SIZE) as u32;

    let mut data = test_data_superblock_with_regions(REGION_COUNT);
    data.geometry.region_size = REGION_SIZE;
    data.geometry.data_file_len =
        DataGeometry::expected_file_len(REGION_SIZE, REGION_COUNT).unwrap();
    let metadata = empty_region_metadata(data, INDEX_SLOTS, REGION_SHARDS).unwrap();
    let encoded = metadata.encode().unwrap();
    assert_eq!(encoded.len() as u64, metadata.encoded_len().unwrap());

    let recovered = RegionMetadata::decode(&encoded).unwrap();
    let manager = RegionManager::from_metadata(recovered).unwrap();
    let snapshot = manager.region_snapshot().unwrap();
    assert_eq!(snapshot.capacity_bytes, CAPACITY_BYTES);
    assert_eq!(snapshot.active_region_count, REGION_SHARDS);
    assert_eq!(snapshot.free_region_count, REGION_COUNT - REGION_SHARDS);
    assert_eq!(snapshot.sealed_region_count, 0);
    assert_eq!(manager.next_seqno(), u64::from(REGION_SHARDS) + 1);
}

#[test]
fn foreground_stage_bypasses_busy_manager_without_consuming_a_sequence() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let resources = data_path_resources();
    let staging = RegionStaging::try_new(
        1,
        crate::runtime_config::MAX_WRITE_FLUSH_THRESHOLD_BYTES,
        data.geometry.region_size,
        &resources,
    )
    .unwrap();
    let manager = runtime.manager.inner.lock().unwrap();
    let next_seqno = manager.next_seqno();
    let hash = hash_key(data.hash_seed, b"key");
    let record_bytes = required_record_bytes(b"key".len(), b"value".len()).unwrap();

    assert_eq!(
        runtime
            .try_stage_value(&staging, 0, hash, record_bytes, b"key", b"value")
            .unwrap(),
        RegionStageValue::NeedsProgress
    );
    assert_eq!(manager.next_seqno(), next_seqno);
}

#[test]
fn completed_record_publication_does_not_enter_region_manager() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let core = Arc::clone(&runtime.core);
    let manager = core.manager.inner.lock().unwrap();
    let record = StagedRecord::new(
        7,
        IndexEntry {
            location: crate::index::PackedLocation::new(0, 0, 64).unwrap(),
        },
        1,
    );
    let (sender, receiver) = std::sync::mpsc::sync_channel(1);
    let publisher_core = Arc::clone(&core);
    let publisher = std::thread::spawn(move || {
        sender
            .send(publisher_core.publish_completed_records(&[record]))
            .unwrap();
    });

    let published = receiver.recv_timeout(std::time::Duration::from_secs(1));
    drop(manager);
    publisher.join().unwrap();
    published.unwrap().unwrap();
    assert_eq!(runtime.lookup_snapshot(7).unwrap(), Some(record.entry()));
}

#[test]
fn completed_owned_span_publishes_index_without_a_steady_state_sync() {
    let data = data_path_superblock();
    let index_slots = INDEX_IMAGE_SLOTS_PER_PAGE * 4;
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(index_slots).unwrap(),
        empty_region_metadata(data, index_slots, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let resources = data_path_resources();
    let staging = RegionStaging::try_new(
        1,
        crate::runtime_config::MAX_WRITE_FLUSH_THRESHOLD_BYTES,
        data.geometry.region_size,
        &resources,
    )
    .unwrap();
    let directory = TestDirectory::new();
    let (backend, faults) = FaultBackend::open(&directory.files.data).unwrap();
    backend.set_len(data.geometry.data_file_len).unwrap();
    let engine = BackendIoEngine::new(Arc::new(backend), 2).unwrap();
    let value = vec![0x5a; 16 * 1024];
    let mut first = None;
    let mut last = None;
    let mut staged_records = 0_u64;
    loop {
        let key = format!("file/chunk/{staged_records:04}");
        let hash = hash_key(data.hash_seed, key.as_bytes());
        let record_bytes = required_record_bytes(key.len(), value.len()).unwrap();
        match runtime
            .try_stage_value(&staging, 0, hash, record_bytes, key.as_bytes(), &value)
            .unwrap()
        {
            RegionStageValue::Staged(seqno) => {
                if first.is_none() {
                    first = Some((key.clone(), hash, seqno));
                }
                last = Some((key, hash, seqno));
                staged_records += 1;
            }
            RegionStageValue::NeedsProgress => break,
            outcome => panic!("unexpected staging outcome: {outcome:?}"),
        }
    }
    let (first_key, first_hash, _first_seqno) =
        first.expect("4 MiB span must contain target-size records");
    let (last_key, last_hash, last_seqno) = last.expect("4 MiB span must retain its final record");
    assert!(staged_records > 240);
    assert_eq!(runtime.lookup_snapshot(first_hash).unwrap(), None);

    let published = runtime
        .flush_staging_shard(&staging, &engine, 0)
        .unwrap()
        .unwrap();
    assert_eq!(
        (published.end_offset - published.start_offset) % RECOVERY_PAGE_SIZE as u64,
        0
    );
    let Some(entry) = runtime.lookup_snapshot(first_hash).unwrap() else {
        panic!("completed first record must be published");
    };
    let first_exact = PackedLocation::new(
        entry.location.region_id(),
        entry.location.offset(),
        required_record_bytes(first_key.len(), value.len()).unwrap(),
    )
    .unwrap();
    assert!(entry.location.index_equivalent(first_exact));
    assert_ne!(entry.location.record_len() % RECOVERY_PAGE_SIZE as u32, 0);
    let Some(last_entry) = runtime.lookup_snapshot(last_hash).unwrap() else {
        panic!("completed final record must be published");
    };
    assert!(
        last_entry.location.record_len()
            > required_record_bytes(last_key.len(), value.len()).unwrap()
    );
    let last_exact_len =
        u32::try_from(published.end_offset - u64::from(last_entry.location.offset())).unwrap();
    let last_exact = PackedLocation::new(
        last_entry.location.region_id(),
        last_entry.location.offset(),
        last_exact_len,
    )
    .unwrap();
    assert!(last_entry.location.index_equivalent(last_exact));
    let read = runtime
        .begin_point_read(first_hash)
        .expect("completed entry must plan a Region read");
    assert_eq!(read.entry, entry);

    let last_read = runtime
        .begin_point_read(last_hash)
        .expect("completed final entry must plan a Region read");
    let read_buffer_bytes = plan_read(data.geometry, last_hash, last_read, true)
        .unwrap()
        .read_len;
    let memory_before_read = resources.managed_memory_snapshot().current_bytes;
    let hit = runtime
        .read_value(
            &engine,
            data.geometry,
            resources.try_read_buffer(read_buffer_bytes).unwrap(),
            data.hash_seed,
            last_key.as_bytes(),
        )
        .unwrap()
        .expect("completed record must validate as a disk hit");
    assert_eq!(hit.value(), value);
    assert_eq!(hit.seqno(), last_seqno);
    drop(hit);
    assert_eq!(
        resources.managed_memory_snapshot().current_bytes,
        memory_before_read
    );

    assert_eq!(
        faults.events(),
        vec![FaultEvent::Write(WritePoint::Record), FaultEvent::Read]
    );
    assert_eq!(engine.stats().requests.requests_submitted, 2);
    assert_eq!(engine.stats().requests.requests_succeeded, 2);
    engine.shutdown().unwrap();
}

#[test]
fn read_availability_errors_do_not_latch_miss_only() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let entry = IndexEntry {
        location: PackedLocation::new(0, 0, 64).unwrap(),
    };

    for kind in [
        io::ErrorKind::OutOfMemory,
        io::ErrorKind::WouldBlock,
        io::ErrorKind::TimedOut,
        io::ErrorKind::Interrupted,
        io::ErrorKind::BrokenPipe,
    ] {
        let completion = ReadCompletion {
            plan: ReadPlan {
                hash: 7,
                entry,
                region_generation: 1,
                absolute: DATA_REGION_AREA_OFFSET,
                read_len: 64,
                record_range: 0..64,
            },
            result: Err(io::Error::new(kind, "injected read availability error")),
            buffer: None,
        };
        let Err(error) = runtime.finish_value_read(completion, b"key") else {
            panic!("injected read availability error unexpectedly succeeded");
        };
        assert_eq!(error.kind(), kind);
        assert!(runtime.is_healthy(), "{kind:?}");
    }
}

#[test]
fn same_hash_candidate_requires_full_key() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let resources = data_path_resources();
    let staging = RegionStaging::try_new(
        1,
        crate::runtime_config::MAX_WRITE_FLUSH_THRESHOLD_BYTES,
        data.geometry.region_size,
        &resources,
    )
    .unwrap();
    let directory = TestDirectory::new();
    let (backend, _) = FaultBackend::open(&directory.files.data).unwrap();
    backend.set_len(data.geometry.data_file_len).unwrap();
    let engine = BackendIoEngine::new(Arc::new(backend), 1).unwrap();
    let owner_key = b"collision-owner";
    let foreign_key = b"collision-foreign";
    let value = b"owner-value-must-not-leak";
    let owner_hash = hash_key(data.hash_seed, owner_key);
    let owner_record_bytes = required_record_bytes(owner_key.len(), value.len()).unwrap();
    let RegionStageValue::Staged(_) = runtime
        .try_stage_value(
            &staging,
            0,
            owner_hash,
            owner_record_bytes,
            owner_key,
            value,
        )
        .unwrap()
    else {
        panic!("collision owner must stage");
    };
    runtime
        .flush_staging_shard(&staging, &engine, 0)
        .unwrap()
        .expect("collision owner must publish");

    let Some(entry) = runtime.lookup_snapshot(owner_hash).unwrap() else {
        panic!("collision owner must be indexed after publication");
    };
    let read_buffer_bytes =
        (entry.location.record_len() as usize).div_ceil(RECOVERY_PAGE_SIZE) * RECOVERY_PAGE_SIZE;
    let memory_before_read = resources.managed_memory_snapshot().current_bytes;
    let read_buffer = resources.try_read_buffer(read_buffer_bytes).unwrap();
    let entry = runtime
        .begin_point_read(owner_hash)
        .expect("hash lookup must return the collision candidate");
    let plan = plan_read(data.geometry, owner_hash, entry, true).unwrap();

    // Supplying a different key after the hash lookup precisely models a
    // 64-bit collision at the L2 record-validation boundary.
    assert!(
        runtime
            .read_value_from_plan(
                &engine,
                engine.try_reserve_read().unwrap(),
                read_buffer,
                plan,
                foreign_key,
            )
            .unwrap()
            .is_none()
    );
    assert!(runtime.health.is_healthy());
    assert_eq!(
        resources.managed_memory_snapshot().current_bytes,
        memory_before_read
    );

    let current = runtime
        .begin_point_read(owner_hash)
        .expect("owner remains indexed");
    let stale_generation = ReadCandidate {
        region_generation: current.region_generation + 1,
        ..current
    };
    let stale_plan = plan_read(data.geometry, owner_hash, stale_generation, true).unwrap();
    assert!(
        runtime
            .read_value_from_plan(
                &engine,
                engine.try_reserve_read().unwrap(),
                resources.try_read_buffer(read_buffer_bytes).unwrap(),
                stale_plan,
                owner_key,
            )
            .unwrap()
            .is_none()
    );

    let wrong_length_location = PackedLocation::new(
        current.entry.location.region_id(),
        current.entry.location.offset(),
        current.entry.location.record_len() + crate::format::RECORD_ALIGNMENT,
    )
    .unwrap();
    let wrong_length = ReadCandidate {
        entry: IndexEntry {
            location: wrong_length_location,
        },
        ..current
    };
    let wrong_length_plan = plan_read(data.geometry, owner_hash, wrong_length, true).unwrap();
    let wrong_length_read_bytes = wrong_length_plan.read_len;
    assert!(
        runtime
            .read_value_from_plan(
                &engine,
                engine.try_reserve_read().unwrap(),
                resources.try_read_buffer(wrong_length_read_bytes).unwrap(),
                wrong_length_plan,
                owner_key,
            )
            .unwrap()
            .is_none()
    );

    let hit = runtime
        .read_value(
            &engine,
            data.geometry,
            resources.try_read_buffer(read_buffer_bytes).unwrap(),
            data.hash_seed,
            owner_key,
        )
        .unwrap()
        .expect("the owning full key must still hit");
    assert_eq!(hit.value(), value);
    drop(hit);
    assert_eq!(
        resources.managed_memory_snapshot().current_bytes,
        memory_before_read
    );
    engine.shutdown().unwrap();
}

#[test]
fn failed_span_write_never_publishes_and_latches_miss_only() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    let resources = data_path_resources();
    let staging = RegionStaging::try_new(
        1,
        crate::runtime_config::MAX_WRITE_FLUSH_THRESHOLD_BYTES,
        data.geometry.region_size,
        &resources,
    )
    .unwrap();
    let directory = TestDirectory::new();
    let (backend, faults) = FaultBackend::open(&directory.files.data).unwrap();
    backend.set_len(data.geometry.data_file_len).unwrap();
    let engine = BackendIoEngine::new(Arc::new(backend), 1).unwrap();
    let hash = hash_key(data.hash_seed, b"key");
    let record_bytes = required_record_bytes(b"key".len(), 16 * 1024).unwrap();
    let RegionStageValue::Staged(_) = runtime
        .try_stage_value(&staging, 0, hash, record_bytes, b"key", &[7; 16 * 1024])
        .unwrap()
    else {
        panic!("value must stage before the injected write failure");
    };
    faults.arm(
        FaultEvent::Write(WritePoint::Record),
        1,
        FaultAction::ErrorAlways(5),
    );

    assert_eq!(
        runtime
            .flush_staging_shard(&staging, &engine, 0)
            .unwrap_err()
            .raw_os_error(),
        Some(5)
    );
    assert!(!runtime.health.is_healthy());
    assert_eq!(runtime.lookup_snapshot(hash).unwrap(), None);
    assert_eq!(runtime.index.lookup_raw(hash).unwrap(), None);
    assert_eq!(
        runtime.manager.inner.lock().unwrap().regions()[0].completed_used,
        0
    );
    engine.shutdown().unwrap();
}

#[test]
fn rotation_is_committed_without_a_metadata_io_boundary() {
    let data = data_path_superblock();
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(64).unwrap(),
        empty_region_metadata(data, 64, REGION_SHARDS).unwrap(),
    )
    .unwrap();
    runtime
        .manager
        .inner
        .lock()
        .unwrap()
        .request_rotation_for_test(0)
        .unwrap();

    assert!(runtime.rotate_shard(0).unwrap());
    assert!(runtime.health.is_healthy());
    let manager = runtime.manager.lock().unwrap();
    assert_eq!(manager.active_regions()[0], REGION_SHARDS);
    assert_eq!(manager.sealed_regions().back(), Some(&0));
}

fn assert_no_runtime_data_write_during_startup(events: &[FaultEvent]) {
    let running_sync = events
        .iter()
        .rposition(|event| *event == FaultEvent::Sync(SyncPoint::RunningState))
        .expect("startup must make RUNNING durable");
    assert!(
        events[running_sync + 1..]
            .iter()
            .all(|event| !matches!(event, FaultEvent::Write(_))),
        "startup must not materialize recovery-only Region state in the data file"
    );
}

#[test]
fn fresh_and_dirty_startup_do_not_write_runtime_region_metadata() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();

    let (fresh_file_system, fresh_io, _) = FaultRegionFileSystem::new();
    let mut fresh = RegionStore::open(
        config,
        FileRegionBackend::new_with_file_system(directory.files.clone(), data, fresh_file_system),
    )
    .unwrap();
    assert_eq!(fresh.startup(), StartupMode::Cold);
    assert_no_runtime_data_write_during_startup(&fresh_io.events());
    fresh.close_fast().unwrap();

    let (dirty_file_system, dirty_io, _) = FaultRegionFileSystem::new();
    let mut dirty = RegionStore::open(
        config,
        FileRegionBackend::new_with_file_system(directory.files.clone(), data, dirty_file_system),
    )
    .unwrap();
    assert_eq!(dirty.startup(), StartupMode::Cold);
    assert_no_runtime_data_write_during_startup(&dirty_io.events());
    dirty.close_fast().unwrap();
}

fn publish_custom_clean_image(
    directory: &TestDirectory,
    index_slots: usize,
    data: DataSuperblock,
    metadata: RegionMetadata,
) {
    let shard_count = metadata.root.shard_count;
    let runtime_config = RuntimeConfig::default().with_append_shards(shard_count);
    let mut backend = FileRegionBackend::new_with_configs(
        directory.files.clone(),
        data,
        shard_count,
        runtime_config,
    );
    backend.acquire_exclusive().unwrap();
    assert!(matches!(
        backend.inspect_recovery(index_slots).unwrap(),
        RecoveryPlan::Fresh
    ));
    let runtime = FileRegionRuntime::install(
        PartitionedIndexStorage::anonymous(index_slots).unwrap(),
        metadata,
    )
    .unwrap();
    backend.publish_running().unwrap();
    let runtime = backend.start_runtime(runtime).unwrap();
    let frozen = backend.freeze_warm(runtime).unwrap();
    let prepared = backend.persist_frozen(&frozen).unwrap();
    backend.publish_clean(prepared).unwrap();
    backend.release_exclusive().unwrap();
}

#[test]
fn clean_image_rebinds_a_different_append_shard_topology() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();

    let metadata = empty_region_metadata(data, config, 1).unwrap();
    publish_custom_clean_image(&directory, config, data, metadata);

    let mut reopened = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(reopened.startup(), StartupMode::Warm);
    let manager = reopened.runtime().unwrap().manager.lock().unwrap();
    assert_eq!(manager.active_regions(), &[0, 2, 3, 4]);
    assert_eq!(manager.free_regions().len(), 1);
    drop(manager);
    reopened.close_warm().unwrap();

    let mut stable = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(stable.startup(), StartupMode::Warm);
    assert_eq!(
        stable
            .runtime()
            .unwrap()
            .manager
            .lock()
            .unwrap()
            .active_regions(),
        &[0, 2, 3, 4]
    );
    stable.close_fast().unwrap();
}

#[test]
fn append_shard_growth_without_free_regions_cold_starts_safely() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();
    let mut metadata = empty_region_metadata(data, config, 1).unwrap();
    for (ordinal, region) in metadata.regions[1..4].iter_mut().enumerate() {
        region.state = RegionMetadataState::Sealed;
        region.queue_ordinal = u32::try_from(ordinal).unwrap();
        region.created_seqno = u64::try_from(ordinal + 2).unwrap();
    }
    metadata.regions[4].queue_ordinal = 0;
    metadata.root.max_seqno = 4;
    metadata.root.free_region_count = 1;
    metadata.root.sealed_region_count = 3;
    metadata.validate().unwrap();
    publish_custom_clean_image(&directory, config, data, metadata);

    let mut reopened = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(reopened.startup(), StartupMode::Cold);
    let manager = reopened.runtime().unwrap().manager.lock().unwrap();
    assert_eq!(manager.active_regions(), &[0, 1, 2, 3]);
    assert_eq!(manager.free_regions().len(), 1);
    drop(manager);
    reopened.close_fast().unwrap();
}

#[test]
fn dirty_cold_start_discards_stale_region_bytes_without_scanning() {
    use std::os::unix::fs::FileExt;

    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();
    let mut first = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    first.close_fast().unwrap();

    let stale_offset = 2 * RECOVERY_PAGE_SIZE as u64;
    let file = File::options()
        .read(true)
        .write(true)
        .open(&directory.files.data)
        .unwrap();
    file.write_all_at(b"stale-record", stale_offset).unwrap();
    file.sync_data().unwrap();

    let mut cold = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(cold.startup(), StartupMode::Cold);
    let mut observed = [0xff_u8; 12];
    File::open(&directory.files.data)
        .unwrap()
        .read_exact_at(&mut observed, stale_offset)
        .unwrap();
    assert_eq!(observed, [0; 12]);
    cold.close_fast().unwrap();
}

#[test]
fn concrete_recovery_profile_rejects_fewer_than_five_regions_before_file_creation() {
    let directory = TestDirectory::new();
    let opened = RegionStore::open(
        8,
        FileRegionBackend::new(
            directory.files.clone(),
            test_data_superblock_with_regions(REGION_SHARDS),
        ),
    );
    assert!(matches!(
        opened,
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(!directory.files.data.exists());
    assert!(!directory.files.state.exists());
}

#[test]
fn complete_warm_image_maps_without_rebuilding_index_slots() {
    let directory = TestDirectory::new();
    let config = INDEX_IMAGE_SLOTS_PER_PAGE + 8;
    let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
    let value = IndexSlot::from_state(crate::index_storage::IndexSlotState::Value {
        fingerprint: 7,
        displacement: 0,
        entry: IndexEntry {
            location: crate::index::PackedLocation::new(0, 0, 32).unwrap(),
        },
    });

    let mut first = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    let runtime = first.runtime_mut().unwrap();
    assert_eq!(runtime.index.storage().partition_count(), 2);
    runtime
        .index
        .storage()
        .write_slot(config - 1, value)
        .unwrap();
    first.close_warm().unwrap();
    assert!(directory.files.image.exists());

    let mut recovered = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(recovered.startup(), StartupMode::Warm);
    let recovered_runtime = recovered.runtime().unwrap();
    assert_eq!(recovered_runtime.index.storage().partition_count(), 2);
    assert_eq!(
        recovered_runtime
            .index
            .storage()
            .read_slot(config - 1)
            .unwrap(),
        value
    );
    recovered.close_fast().unwrap();
}

#[test]
fn corrupt_region_metadata_rejects_the_complete_clean_image() {
    use std::os::unix::fs::FileExt;

    let directory = TestDirectory::new();
    let config = 130;
    let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
    let mut first = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    first.close_warm().unwrap();

    let image = File::options()
        .read(true)
        .write(true)
        .open(&directory.files.image)
        .unwrap();
    let mut page = [0_u8; RECOVERY_PAGE_SIZE];
    image.read_exact_at(&mut page, 0).unwrap();
    let RecoveryImageHeaderProbe::Valid(header) = RecoveryImageHeader::probe(&page) else {
        panic!("warm recovery image header must be valid");
    };
    image
        .write_all_at(&[0x5a], header.region_table_offset + 100)
        .unwrap();
    image.sync_data().unwrap();

    let mut rejected = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(rejected.startup(), StartupMode::Cold);
    assert_eq!(
        rejected.runtime().unwrap().lookup_snapshot(0).unwrap(),
        None
    );
    rejected.close_fast().unwrap();
}

#[test]
fn one_corrupt_lazy_index_page_rejects_all_pages() {
    use std::os::unix::fs::FileExt;

    let directory = TestDirectory::new();
    let config = INDEX_IMAGE_SLOTS_PER_PAGE + 8;
    let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
    let mut first = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    first.close_warm().unwrap();

    let image = File::options()
        .read(true)
        .write(true)
        .open(&directory.files.image)
        .unwrap();
    image
        .write_all_at(
            &[0x5a],
            RECOVERY_IMAGE_INDEX_OFFSET + RECOVERY_PAGE_SIZE as u64 + 100,
        )
        .unwrap();
    image.sync_data().unwrap();

    let mut recovered = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(recovered.startup(), StartupMode::Warm);
    let runtime = recovered.runtime().unwrap();
    assert_eq!(runtime.lookup_snapshot(0).unwrap(), None);
    assert!(runtime.health.is_healthy());
    assert_eq!(runtime.lookup_snapshot(1).unwrap(), None);
    assert!(!runtime.health.is_healthy());
    assert_eq!(runtime.lookup_snapshot(1).unwrap(), None);
    assert_eq!(runtime.lookup_snapshot(0).unwrap(), None);
    assert!(recovered.close_warm().is_err());

    let mut cold = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert_eq!(cold.startup(), StartupMode::Cold);
    assert_eq!(cold.runtime().unwrap().lookup_snapshot(1).unwrap(), None);
    cold.close_fast().unwrap();
}

#[test]
#[ignore = "extended recovery qualification; run with `cargo test --package cache2 --lib --all-features -- --ignored`"]
fn every_prepublication_failure_leaves_no_selectable_clean_state() {
    let cases = [
        (
            Some((FaultEvent::Sync(SyncPoint::WarmData), FaultAction::Error(5))),
            None,
        ),
        (
            Some((
                FaultEvent::Write(WritePoint::RecoveryImageHeader),
                FaultAction::Torn {
                    bytes: 128,
                    raw_os_error: 5,
                },
            )),
            None,
        ),
        (
            Some((
                FaultEvent::Write(WritePoint::RecoveryImageIndex),
                FaultAction::Error(5),
            )),
            None,
        ),
        (
            Some((
                FaultEvent::Write(WritePoint::RecoveryImageMetadata),
                FaultAction::Torn {
                    bytes: 128,
                    raw_os_error: 5,
                },
            )),
            None,
        ),
        (
            Some((
                FaultEvent::Sync(SyncPoint::RecoveryImage),
                FaultAction::Error(5),
            )),
            None,
        ),
        (None, Some(FileSystemFault::Rename)),
        (None, Some(FileSystemFault::SyncParent)),
        (
            Some((
                FaultEvent::Write(WritePoint::State),
                FaultAction::Torn {
                    bytes: 128,
                    raw_os_error: 5,
                },
            )),
            None,
        ),
    ];

    for (case, (io_fault, file_system_fault)) in cases.into_iter().enumerate() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock_with_regions(REGION_SHARDS + 1);
        let (file_system, io_faults, file_system_faults) = FaultRegionFileSystem::new();
        let backend =
            FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system);
        let mut store = RegionStore::open(config, backend).unwrap();
        if let Some((event, action)) = io_fault {
            io_faults.arm(event, 1, action);
        }
        if let Some(fault) = file_system_fault {
            file_system_faults.arm(fault);
        }
        assert!(store.close_warm().is_err(), "failure case {case}");

        let mut reopened = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert_eq!(reopened.startup(), StartupMode::Cold, "failure case {case}");
        reopened.close_fast().unwrap();
    }
}

#[test]
fn concrete_running_barrier_failures_abort_before_runtime_start() {
    let cases = [
        (
            FaultEvent::Write(WritePoint::State),
            1,
            FaultAction::Error(5),
        ),
        (
            FaultEvent::Write(WritePoint::State),
            2,
            FaultAction::Torn {
                bytes: 128,
                raw_os_error: 5,
            },
        ),
        (
            FaultEvent::Sync(SyncPoint::RunningState),
            1,
            FaultAction::Error(5),
        ),
    ];

    for (case, (event, occurrence, action)) in cases.into_iter().enumerate() {
        let directory = TestDirectory::new();
        let config = 8;
        let data = test_data_superblock();
        let (file_system, faults, _) = FaultRegionFileSystem::new();
        faults.arm(event, occurrence, action);
        let opened = RegionStore::open(
            config,
            FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system),
        );
        assert!(opened.is_err(), "RUNNING barrier case {case}");

        let mut cold = RegionStore::open(
            config,
            FileRegionBackend::new(directory.files.clone(), data),
        )
        .unwrap();
        assert!(matches!(cold.startup(), StartupMode::Cold));
        cold.close_fast().unwrap();
    }
}

#[test]
fn final_clean_sync_failure_reopens_as_safe_clean_or_empty() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();
    let (file_system, faults, _) = FaultRegionFileSystem::new();
    let mut store = RegionStore::open(
        config,
        FileRegionBackend::new_with_file_system(directory.files.clone(), data, file_system),
    )
    .unwrap();
    faults.arm(
        FaultEvent::Sync(SyncPoint::CleanState),
        1,
        FaultAction::Error(5),
    );
    assert!(store.close_warm().is_err());

    let mut reopened = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();
    assert!(matches!(
        reopened.startup(),
        StartupMode::Warm | StartupMode::Cold
    ));
    assert_eq!(
        reopened.runtime().unwrap().lookup_snapshot(0).unwrap(),
        None
    );
    reopened.close_fast().unwrap();
}

#[test]
fn data_and_state_inode_alias_is_rejected_without_truncation() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();
    let marker = b"do-not-truncate";
    std::fs::write(&directory.files.data, marker).unwrap();
    std::fs::hard_link(&directory.files.data, &directory.files.state).unwrap();

    let opened = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    );
    assert!(matches!(
        opened,
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(std::fs::read(&directory.files.data).unwrap(), marker);
}

#[test]
fn recovery_temporary_path_cannot_name_the_data_or_state_file() {
    let directory = TestDirectory::new();
    let marker = b"keep-data";
    let image = directory.root.join("recovery");
    let data_path = directory.root.join("recovery.next");
    std::fs::write(&data_path, marker).unwrap();
    let files = RegionFiles::new(&data_path, directory.root.join("state"), image);

    let opened = RegionStore::open(
        8,
        FileRegionBackend::new(files, test_data_superblock_with_regions(REGION_SHARDS + 1)),
    );
    assert!(matches!(
        opened,
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert_eq!(std::fs::read(data_path).unwrap(), marker);
}

#[test]
fn recovery_sidecars_must_share_one_directory() {
    let directory = TestDirectory::new();
    let other = directory.root.join("other");
    std::fs::create_dir(&other).unwrap();
    let files = RegionFiles::new(
        directory.root.join("data"),
        other.join("state"),
        directory.root.join("image"),
    );
    let opened = RegionStore::open(8, FileRegionBackend::new(files, test_data_superblock()));
    assert!(matches!(
        opened,
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
    assert!(!directory.root.join("data").exists());
    assert!(!other.join("state").exists());
}

#[test]
fn state_sidecar_lock_prevents_cross_data_file_races() {
    let directory = TestDirectory::new();
    let config = 8;
    let data = test_data_superblock();
    let mut first = RegionStore::open(
        config,
        FileRegionBackend::new(directory.files.clone(), data),
    )
    .unwrap();

    let conflicting_files = RegionFiles::new(
        directory.root.join("other-data"),
        directory.files.state.clone(),
        directory.root.join("other-image"),
    );
    let opened = RegionStore::open(config, FileRegionBackend::new(conflicting_files, data));
    assert!(opened.is_err());
    first.close_fast().unwrap();
}
