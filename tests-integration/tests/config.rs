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

use cache2::{ErrorKind, ErrorOperation, StorageOptions};

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
