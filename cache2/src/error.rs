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

#![doc = include_str!("../ERRORS.md")]

use std::error::Error as StdError;
use std::fmt;
use std::io;

/// A result returned by a public C² operation.
pub type Result<T> = std::result::Result<T, Error>;

/// Stable, actionable classification for a C² failure.
///
/// Match this value instead of parsing [`Error`]'s display text or branching
/// directly on [`io::ErrorKind`]. The underlying I/O kind remains available
/// through [`Error::io_kind`] for diagnostics and compatibility.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorKind {
    /// A static configuration, runtime configuration, key, or value is invalid.
    InvalidInput,
    /// The selected engine, I/O mode, or platform capability is unsupported.
    Unsupported,
    /// Another owner or lifecycle operation currently has exclusive access.
    Busy,
    /// A bounded request-path resource is temporarily saturated.
    Overloaded,
    /// A required allocation or fixed resource plan cannot be satisfied.
    ResourceExhausted,
    /// The cache runtime or one of its required services is no longer available.
    Unavailable,
    /// Persisted or in-memory cache data failed structural validation.
    CorruptData,
    /// A filesystem or device operation failed.
    Io,
    /// A worker, synchronization primitive, or internal invariant failed.
    Internal,
}

impl ErrorKind {
    /// Returns the stable snake-case label used in logs and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::Unsupported => "unsupported",
            Self::Busy => "busy",
            Self::Overloaded => "overloaded",
            Self::ResourceExhausted => "resource_exhausted",
            Self::Unavailable => "unavailable",
            Self::CorruptData => "corrupt_data",
            Self::Io => "io",
            Self::Internal => "internal",
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Public operation during which a C² failure occurred.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ErrorOperation {
    /// [`crate::StaticConfig::validate`].
    ValidateConfig,
    /// [`crate::StaticConfig::peak_disk_bytes`].
    PeakDiskBytes,
    /// [`crate::CacheBuilder::open`].
    Open,
    /// [`crate::Cache::put`].
    Put,
    /// [`crate::Cache::put_l2`].
    PutL2,
    /// [`crate::Cache::delete`].
    Delete,
    /// [`crate::Cache::get`].
    Get,
    /// [`crate::Cache::drain`].
    Drain,
    /// [`crate::Cache::snapshot`].
    Snapshot,
    /// [`crate::Cache::detailed_snapshot`].
    DetailedSnapshot,
    /// [`crate::Cache::close_fast`].
    CloseFast,
    /// [`crate::Cache::close_warm`].
    CloseWarm,
}

impl ErrorOperation {
    /// Returns the stable snake-case label used in logs and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValidateConfig => "validate_config",
            Self::PeakDiskBytes => "peak_disk_bytes",
            Self::Open => "open",
            Self::Put => "put",
            Self::PutL2 => "put_l2",
            Self::Delete => "delete",
            Self::Get => "get",
            Self::Drain => "drain",
            Self::Snapshot => "snapshot",
            Self::DetailedSnapshot => "detailed_snapshot",
            Self::CloseFast => "close_fast",
            Self::CloseWarm => "close_warm",
        }
    }
}

impl fmt::Display for ErrorOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Structured failure returned by a public C² operation.
///
/// The classification and operation are stable programmatic fields. The
/// wrapped [`io::Error`] retains the detailed cause, its raw OS error when one
/// exists, and the complete [`StdError::source`] chain.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    operation: ErrorOperation,
    source: io::Error,
}

impl Error {
    pub(crate) fn from_io(operation: ErrorOperation, source: io::Error) -> Self {
        Self {
            kind: classify(operation, &source),
            operation,
            source,
        }
    }

    /// Returns the actionable C² error classification.
    pub const fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the public operation that failed.
    pub const fn operation(&self) -> ErrorOperation {
        self.operation
    }

    /// Returns the underlying standard-library I/O error kind.
    ///
    /// Prefer [`Self::kind`] for application policy. This method is intended
    /// for detailed diagnostics and migration from the former `io::Result`
    /// API.
    pub fn io_kind(&self) -> io::ErrorKind {
        self.source.kind()
    }

    /// Returns the raw OS error code carried by the underlying I/O failure.
    pub fn raw_os_error(&self) -> Option<i32> {
        self.source.raw_os_error()
    }

    /// Borrows the underlying standard-library I/O error.
    pub const fn as_io_error(&self) -> &io::Error {
        &self.source
    }

    /// Removes the structured C² context and returns the underlying I/O error.
    ///
    /// This preserves the original raw OS error code and source chain. The
    /// `From<Error>` conversion uses the same lossless path.
    pub fn into_io_error(self) -> io::Error {
        self.source
    }

    /// Wraps this error in a standard-library I/O error.
    ///
    /// This keeps the C² operation and classification in the source chain, but
    /// the returned outer error does not expose the underlying raw OS error
    /// directly. Use [`Self::into_io_error`] when preserving that code is more
    /// important than retaining the structured context.
    pub fn into_io_error_with_context(self) -> io::Error {
        let kind = self.io_kind();
        io::Error::new(kind, self)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} failed [{}]: {}",
            self.operation, self.kind, self.source
        )
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}

impl From<Error> for io::Error {
    fn from(error: Error) -> Self {
        error.into_io_error()
    }
}

fn classify(operation: ErrorOperation, source: &io::Error) -> ErrorKind {
    use io::ErrorKind as IoKind;

    match source.kind() {
        IoKind::InvalidInput if source.raw_os_error().is_some() => ErrorKind::Io,
        IoKind::InvalidInput if accepts_caller_input(operation) => ErrorKind::InvalidInput,
        IoKind::InvalidInput => ErrorKind::Internal,
        IoKind::Unsupported => ErrorKind::Unsupported,
        IoKind::WouldBlock | IoKind::AlreadyExists if operation == ErrorOperation::Open => {
            ErrorKind::Busy
        }
        IoKind::WouldBlock if source.raw_os_error().is_some() => ErrorKind::Io,
        IoKind::WouldBlock if has_bounded_admission(operation) => ErrorKind::Overloaded,
        IoKind::WouldBlock | IoKind::AlreadyExists => ErrorKind::Internal,
        IoKind::TimedOut if operation == ErrorOperation::Get => ErrorKind::Overloaded,
        IoKind::TimedOut => ErrorKind::Io,
        IoKind::OutOfMemory if operation == ErrorOperation::Get => ErrorKind::Overloaded,
        IoKind::OutOfMemory => ErrorKind::ResourceExhausted,
        IoKind::BrokenPipe | IoKind::NotConnected | IoKind::Interrupted => ErrorKind::Unavailable,
        IoKind::InvalidData | IoKind::UnexpectedEof => ErrorKind::CorruptData,
        IoKind::Other if source.raw_os_error().is_none() => ErrorKind::Internal,
        _ => ErrorKind::Io,
    }
}

const fn accepts_caller_input(operation: ErrorOperation) -> bool {
    matches!(
        operation,
        ErrorOperation::ValidateConfig
            | ErrorOperation::PeakDiskBytes
            | ErrorOperation::Open
            | ErrorOperation::Put
            | ErrorOperation::PutL2
            | ErrorOperation::Delete
    )
}

const fn has_bounded_admission(operation: ErrorOperation) -> bool {
    matches!(
        operation,
        ErrorOperation::Put
            | ErrorOperation::PutL2
            | ErrorOperation::Delete
            | ErrorOperation::Get
            | ErrorOperation::Drain
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn error(operation: ErrorOperation, kind: io::ErrorKind) -> Error {
        Error::from_io(operation, io::Error::new(kind, "injected failure"))
    }

    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn public_error_is_send_and_sync() {
        assert_send_sync::<Error>();
    }

    #[test]
    fn request_pressure_and_open_contention_are_distinct() {
        assert_eq!(
            error(ErrorOperation::Put, io::ErrorKind::WouldBlock).kind(),
            ErrorKind::Overloaded
        );
        assert_eq!(
            error(ErrorOperation::Open, io::ErrorKind::WouldBlock).kind(),
            ErrorKind::Busy
        );
        assert_eq!(
            error(ErrorOperation::Snapshot, io::ErrorKind::WouldBlock).kind(),
            ErrorKind::Internal
        );
    }

    #[test]
    fn request_deadline_and_device_completion_timeout_are_distinct() {
        assert_eq!(
            error(ErrorOperation::Get, io::ErrorKind::TimedOut).kind(),
            ErrorKind::Overloaded
        );
        assert_eq!(
            error(ErrorOperation::Drain, io::ErrorKind::TimedOut).kind(),
            ErrorKind::Io
        );
    }

    #[test]
    fn bounded_get_memory_pressure_is_overload() {
        assert_eq!(
            error(ErrorOperation::Get, io::ErrorKind::OutOfMemory).kind(),
            ErrorKind::Overloaded
        );
        assert_eq!(
            error(ErrorOperation::Open, io::ErrorKind::OutOfMemory).kind(),
            ErrorKind::ResourceExhausted
        );
    }

    #[test]
    fn low_level_failures_map_to_actionable_categories() {
        for (io_kind, expected) in [
            (io::ErrorKind::InvalidInput, ErrorKind::Internal),
            (io::ErrorKind::Unsupported, ErrorKind::Unsupported),
            (io::ErrorKind::BrokenPipe, ErrorKind::Unavailable),
            (io::ErrorKind::InvalidData, ErrorKind::CorruptData),
            (io::ErrorKind::PermissionDenied, ErrorKind::Io),
            (io::ErrorKind::Other, ErrorKind::Internal),
        ] {
            assert_eq!(error(ErrorOperation::Drain, io_kind).kind(), expected);
        }

        assert_eq!(
            error(ErrorOperation::Put, io::ErrorKind::InvalidInput).kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn display_contains_structured_labels_and_cause() {
        let error = error(ErrorOperation::Drain, io::ErrorKind::TimedOut);

        assert_eq!(error.to_string(), "drain failed [io]: injected failure");
    }

    #[test]
    fn default_io_conversion_preserves_raw_os_error() {
        let error = Error::from_io(ErrorOperation::Open, io::Error::from_raw_os_error(13));
        let error = io::Error::from(error);

        assert_eq!(error.raw_os_error(), Some(13));
    }

    #[test]
    fn contextual_io_conversion_retains_structured_error() {
        let error = Error::from_io(ErrorOperation::Open, io::Error::from_raw_os_error(13))
            .into_io_error_with_context();

        assert_eq!(error.raw_os_error(), None);
        let source = error
            .get_ref()
            .and_then(|source| source.downcast_ref::<Error>())
            .expect("structured error remains in the source chain");
        assert_eq!(source.operation(), ErrorOperation::Open);
        assert_eq!(source.raw_os_error(), Some(13));
    }
}
