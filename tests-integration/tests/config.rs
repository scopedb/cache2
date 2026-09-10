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

use std::time::Duration;

use cache2::CacheConfig;
use cache2::ErrorKind;
use cache2::ErrorOperation;
use cache2::IoEngineOptions;
use cache2::L1EvictionPolicy;
use cache2::PosixIoOptions;
use cache2::ReadAdmission;
use cache2::RuntimeOptions;
use cache2::StorageLayout;
use cache2::StorageOptions;

type StorageOptionsCase = (&'static str, fn(&mut StorageOptions));
type RuntimeOptionsCase = (&'static str, fn(&mut RuntimeOptions));

fn test_storage() -> StorageLayout {
    let mut options = StorageOptions::new(3 * 512 * 1024);
    options.region_size_bytes = 512 * 1024;
    options.build().unwrap()
}

#[test]
fn storage_rejects_unrepresentable_inputs() {
    let cases: [StorageOptionsCase; 11] = [
        ("zero-capacity", |options| options.capacity_bytes = 0),
        ("single-region", |options| options.capacity_bytes = 4096),
        ("unaligned-capacity", |options| {
            options.capacity_bytes = 8193
        }),
        ("capacity-overflow", |options| {
            options.capacity_bytes = u64::MAX
        }),
        ("zero-region-size", |options| options.region_size_bytes = 0),
        ("unaligned-region-size", |options| {
            options.region_size_bytes = 4097
        }),
        ("too-many-regions", |options| {
            *options = StorageOptions::new(33_u64 << 40)
        }),
        ("too-many-entries", |options| {
            options.expected_entries = Some(1_usize << 48)
        }),
        ("oversized-region", |options| {
            options.capacity_bytes = 128 * 1024 * 1024;
            options.region_size_bytes = 64 * 1024 * 1024;
        }),
        ("index-size-overflow", |options| {
            options.expected_entries = Some(usize::MAX / 2)
        }),
        ("index-slot-count-overflow", |options| {
            options.expected_entries = Some(usize::MAX)
        }),
    ];
    for (case, configure) in cases {
        let mut options = StorageOptions::new(2 * 4096);
        options.region_size_bytes = 4096;
        configure(&mut options);
        let error = options.build().unwrap_err();
        assert_eq!(error.operation(), ErrorOperation::BuildStorage, "{case}");
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{case}");
    }
}

#[test]
fn large_layout_memory_floor_includes_each_l1_policy() {
    const GIB: usize = 1024 * 1024 * 1024;
    let storage = StorageOptions::new(4_u64 << 40).build().unwrap();
    for policy in [L1EvictionPolicy::Clock, L1EvictionPolicy::S3Fifo] {
        let mut io = PosixIoOptions::default();
        io.reclaim_workers = 2;
        let mut runtime = RuntimeOptions::default();
        runtime.l1_capacity_bytes = 10 * GIB;
        runtime.managed_memory_limit_bytes = 15 * GIB;
        runtime.io_engine = IoEngineOptions::Posix(io);
        runtime.l1_shards = 64;
        runtime.l1_eviction_policy = policy;
        let config = CacheConfig::new(storage.clone(), runtime.clone()).unwrap();
        let floor = config.minimum_memory_bytes();
        assert!(floor > 14 * GIB);
        runtime.managed_memory_limit_bytes = floor;
        CacheConfig::new(storage.clone(), runtime.clone()).unwrap();
        runtime.managed_memory_limit_bytes = floor - 1;
        let error = CacheConfig::new(storage.clone(), runtime).unwrap_err();
        assert_eq!(error.operation(), ErrorOperation::BuildConfig);
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn automatic_wait_capacity_uses_the_selected_engine() {
    let storage = test_storage();
    let mut options = RuntimeOptions::default();
    options.append_shards = 2;
    options.read_admission = ReadAdmission::Wait {
        timeout: Duration::from_millis(1),
        max_waiters: None,
    };
    let mut io = PosixIoOptions::default();
    io.write_workers = 1;
    for workers in [1, 7] {
        io.read_workers = workers;
        options.io_engine = IoEngineOptions::Posix(io);
        let config = CacheConfig::new(storage.clone(), options.clone()).unwrap();
        assert_eq!(
            config.runtime().read_admission,
            ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(workers),
            }
        );
    }
    options.read_admission = ReadAdmission::Wait {
        timeout: Duration::from_millis(1),
        max_waiters: Some(11),
    };
    let config = CacheConfig::new(storage, options).unwrap();
    assert_eq!(
        config.runtime().read_admission,
        ReadAdmission::Wait {
            timeout: Duration::from_millis(1),
            max_waiters: Some(11),
        }
    );
}

#[test]
fn invalid_runtime_options_are_rejected_when_building_configuration() {
    let cases: [RuntimeOptionsCase; 13] = [
        ("zero-memory-budget", |options| {
            options.managed_memory_limit_bytes = 0
        }),
        ("zero-wait-timeout", |options| {
            options.read_admission = ReadAdmission::Wait {
                timeout: Duration::ZERO,
                max_waiters: None,
            };
        }),
        ("zero-append-shards", |options| options.append_shards = 0),
        ("insufficient-regions", |options| options.append_shards = 3),
        ("too-many-append-shards", |options| {
            options.append_shards = 257
        }),
        ("zero-read-wait-capacity", |options| {
            options.read_admission = ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(0),
            };
        }),
        ("too-large-read-wait-capacity", |options| {
            options.read_admission = ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(65_537),
            };
        }),
        ("excessive-read-wait", |options| {
            options.read_admission = ReadAdmission::Wait {
                timeout: Duration::from_secs(5) + Duration::from_nanos(1),
                max_waiters: None,
            };
        }),
        ("l1-exceeds-budget", |options| {
            options.l1_capacity_bytes = 64 * 1024 * 1024;
            options.managed_memory_limit_bytes = 32 * 1024 * 1024;
        }),
        ("fixed-footprint-exceeds-budget", |options| {
            let mut io = PosixIoOptions::default();
            io.read_workers = 2;
            io.write_workers = 2;
            options.io_engine = IoEngineOptions::Posix(io);
            options.l1_capacity_bytes = 0;
            options.managed_memory_limit_bytes = 2 * 1024 * 1024;
            options.write_flush_threshold_bytes = 128 * 1024;
        }),
        ("zero-l1-shards", |options| options.l1_shards = 0),
        ("unaligned-write-flush-threshold", |options| {
            options.write_flush_threshold_bytes = 4097
        }),
        ("oversized-write-flush-threshold", |options| {
            options.write_flush_threshold_bytes = 4 * 1024 * 1024 + 4096
        }),
    ];
    for (case, configure) in cases {
        let mut options = RuntimeOptions::default();
        options.append_shards = 2;
        configure(&mut options);
        let error = CacheConfig::new(test_storage(), options).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{case}");
        assert_eq!(error.operation(), ErrorOperation::BuildConfig, "{case}");
    }
}

#[test]
fn invalid_posix_options_are_rejected_when_building_configuration() {
    for (case, read_workers, write_workers, reclaim_workers) in [
        ("zero-reclaim-workers", 1, 1, 0),
        ("too-many-reclaim-workers", 1, 1, 3),
        ("zero-read-workers", 0, 1, 1),
        ("zero-write-workers", 1, 0, 1),
        ("too-many-read-workers", 4097, 1, 1),
        ("too-many-write-workers", 1, 4097, 1),
    ] {
        let mut io = PosixIoOptions::default();
        io.read_workers = read_workers;
        io.write_workers = write_workers;
        io.reclaim_workers = reclaim_workers;
        let mut options = RuntimeOptions::default();
        options.append_shards = 2;
        options.io_engine = IoEngineOptions::Posix(io);
        let error = CacheConfig::new(test_storage(), options).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{case}");
        assert_eq!(error.operation(), ErrorOperation::BuildConfig, "{case}");
    }
}
