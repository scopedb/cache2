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

use std::error::Error as _;

use cache2::ErrorKind;
use cache2::ErrorOperation;
use cache2::StorageOptions;

#[test]
fn storage_construction_errors_expose_structured_context() {
    let error = StorageOptions::new(1).build().unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::BuildStorage);
    assert_eq!(error.io_kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.raw_os_error().is_none());
    assert!(error.source().is_some());
    assert!(error.to_string().contains("build_storage"));
}

#[test]
fn default_io_conversion_preserves_the_original_error() {
    let error = StorageOptions::new(1).build().unwrap_err();
    let message = error.as_io_error().to_string();
    let error = std::io::Error::from(error);

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), message);
    assert!(
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<cache2::Error>())
            .is_none()
    );
}

#[test]
fn contextual_io_conversion_keeps_structured_source() {
    let error = StorageOptions::new(1)
        .build()
        .unwrap_err()
        .into_io_error_with_context();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    let source = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<cache2::Error>())
        .expect("structured error remains in the source chain");
    assert_eq!(source.operation(), ErrorOperation::BuildStorage);
}
