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

type RuntimeOptionsCase = (&'static str, fn(RuntimeOptions) -> RuntimeOptions);

fn test_storage() -> StorageLayout {
    StorageOptions {
        region_size_bytes: 512 * 1024,
        ..StorageOptions::new(3 * 512 * 1024)
    }
    .build()
    .unwrap()
}

#[test]
fn storage_rejects_unrepresentable_inputs() {
    let base = StorageOptions {
        region_size_bytes: 4096,
        ..StorageOptions::new(2 * 4096)
    };
    for options in [
        StorageOptions {
            capacity_bytes: 0,
            ..base.clone()
        },
        StorageOptions {
            capacity_bytes: 4096,
            ..base.clone()
        },
        StorageOptions {
            capacity_bytes: 8193,
            ..base.clone()
        },
        StorageOptions {
            capacity_bytes: u64::MAX,
            ..base.clone()
        },
        StorageOptions {
            region_size_bytes: 0,
            ..base.clone()
        },
        StorageOptions {
            region_size_bytes: 4097,
            ..base.clone()
        },
        StorageOptions::new(33_u64 << 40),
        StorageOptions {
            expected_entries: Some(1_usize << 48),
            ..base.clone()
        },
        StorageOptions {
            capacity_bytes: 128 * 1024 * 1024,
            region_size_bytes: 64 * 1024 * 1024,
            ..base.clone()
        },
        StorageOptions {
            expected_entries: Some(usize::MAX / 2),
            ..base.clone()
        },
        StorageOptions {
            expected_entries: Some(usize::MAX),
            ..base
        },
    ] {
        let error = options.build().unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.operation(), ErrorOperation::BuildStorage);
    }
}

#[test]
fn large_layout_memory_floor_includes_each_l1_policy() {
    const GIB: usize = 1024 * 1024 * 1024;
    let storage = StorageOptions::new(4_u64 << 40).build().unwrap();
    for policy in [L1EvictionPolicy::Clock, L1EvictionPolicy::S3Fifo] {
        let runtime = RuntimeOptions {
            l1_capacity_bytes: 10 * GIB,
            managed_memory_limit_bytes: 15 * GIB,
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(4, 4, 2)),
            l1_shards: 64,
            l1_eviction_policy: policy,
            ..RuntimeOptions::default()
        };
        let config = CacheConfig::new(storage.clone(), runtime.clone()).unwrap();
        let floor = config.minimum_memory_bytes();
        assert!(floor > 14 * GIB);
        CacheConfig::new(
            storage.clone(),
            RuntimeOptions {
                managed_memory_limit_bytes: floor,
                ..runtime.clone()
            },
        )
        .unwrap();
        let error = CacheConfig::new(
            storage.clone(),
            RuntimeOptions {
                managed_memory_limit_bytes: floor - 1,
                ..runtime
            },
        )
        .unwrap_err();
        assert_eq!(error.operation(), ErrorOperation::BuildConfig);
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }
}

#[test]
fn automatic_wait_capacity_uses_the_selected_engine() {
    let storage = test_storage();
    let options = RuntimeOptions {
        append_shards: 2,
        read_admission: ReadAdmission::Wait {
            timeout: Duration::from_millis(1),
            max_waiters: None,
        },
        ..RuntimeOptions::default()
    };
    for workers in [1, 7] {
        let config = CacheConfig::new(
            storage.clone(),
            RuntimeOptions {
                io_engine: IoEngineOptions::Posix(PosixIoOptions::new(workers, 1, 1)),
                ..options.clone()
            },
        )
        .unwrap();
        assert_eq!(
            config.runtime().read_admission,
            ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(workers)
            }
        );
    }
    let config = CacheConfig::new(
        storage,
        RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(11),
            },
            ..options
        },
    )
    .unwrap();
    assert_eq!(
        config.runtime().read_admission,
        ReadAdmission::Wait {
            timeout: Duration::from_millis(1),
            max_waiters: Some(11)
        }
    );
}

#[test]
fn invalid_runtime_options_are_rejected_when_building_configuration() {
    let cases: [RuntimeOptionsCase; 19] = [
        ("zero-memory-budget", |config| RuntimeOptions {
            managed_memory_limit_bytes: 0,
            ..config
        }),
        ("zero-wait-timeout", |config| RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::ZERO,
                max_waiters: None,
            },
            ..config
        }),
        ("zero-append-shards", |config| RuntimeOptions {
            append_shards: 0,
            ..config
        }),
        ("insufficient-regions", |config| RuntimeOptions {
            append_shards: 3,
            ..config
        }),
        ("too-many-append-shards", |config| RuntimeOptions {
            append_shards: 257,
            ..config
        }),
        ("zero-reclaim-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(1, 1, 0)),
            ..config
        }),
        ("too-many-reclaim-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(1, 1, 3)),
            ..config
        }),
        ("zero-read-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(0, 1, 1)),
            ..config
        }),
        ("zero-write-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(1, 0, 1)),
            ..config
        }),
        ("too-many-read-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(4097, 1, 1)),
            ..config
        }),
        ("zero-read-wait-capacity", |config| RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(0),
            },
            ..config
        }),
        ("too-large-read-wait-capacity", |config| RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::from_millis(1),
                max_waiters: Some(65_537),
            },
            ..config
        }),
        ("too-many-write-workers", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(1, 4097, 1)),
            ..config
        }),
        ("excessive-read-wait", |config| RuntimeOptions {
            read_admission: ReadAdmission::Wait {
                timeout: Duration::from_secs(5) + Duration::from_nanos(1),
                max_waiters: None,
            },
            ..config
        }),
        ("l1-exceeds-budget", |config| RuntimeOptions {
            l1_capacity_bytes: 64 * 1024 * 1024,
            managed_memory_limit_bytes: 32 * 1024 * 1024,
            ..config
        }),
        ("fixed-footprint-exceeds-budget", |config| RuntimeOptions {
            io_engine: IoEngineOptions::Posix(PosixIoOptions::new(2, 2, 1)),
            l1_capacity_bytes: 0,
            managed_memory_limit_bytes: 2 * 1024 * 1024,
            write_flush_threshold_bytes: 128 * 1024,
            ..config
        }),
        ("zero-l1-shards", |config| RuntimeOptions {
            l1_shards: 0,
            ..config
        }),
        ("unaligned-write-flush-threshold", |config| RuntimeOptions {
            write_flush_threshold_bytes: 4097,
            ..config
        }),
        ("oversized-write-flush-threshold", |config| RuntimeOptions {
            write_flush_threshold_bytes: 4 * 1024 * 1024 + 4096,
            ..config
        }),
    ];

    for (case, configure) in cases {
        let options = configure(RuntimeOptions {
            append_shards: 2,
            ..RuntimeOptions::default()
        });
        let error = CacheConfig::new(test_storage(), options).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidInput, "{case}");
        assert_eq!(error.operation(), ErrorOperation::BuildConfig, "{case}");
    }
}
