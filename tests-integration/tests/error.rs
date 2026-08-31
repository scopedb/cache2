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

use cache2::{ErrorKind, ErrorOperation, StaticConfig};

#[test]
fn static_validation_errors_expose_structured_context() {
    let error = StaticConfig::new(1).validate().unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::ValidateConfig);
    assert_eq!(error.io_kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.raw_os_error().is_none());
    assert!(error.source().is_some());
    assert!(error.to_string().contains("validate_config"));
}

#[test]
fn disk_estimation_has_its_own_operation_context() {
    let error = StaticConfig::new(1).peak_disk_bytes().unwrap_err();

    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert_eq!(error.operation(), ErrorOperation::PeakDiskBytes);
}

#[test]
fn default_io_conversion_preserves_the_original_error() {
    let error = StaticConfig::new(1).validate().unwrap_err();
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
    let error = StaticConfig::new(1)
        .validate()
        .unwrap_err()
        .into_io_error_with_context();

    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    let source = error
        .get_ref()
        .and_then(|source| source.downcast_ref::<cache2::Error>())
        .expect("structured error remains in the source chain");
    assert_eq!(source.operation(), ErrorOperation::ValidateConfig);
}
