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

//! One owned-buffer Region span submission for the data path.
//!
//! A successful write completion advances only the written prefix. The
//! disposable-cache protocol establishes durability once, when publishing a
//! CLEAN image, and deliberately has no per-span sync.

use std::fmt;
use std::io;
#[cfg(test)]
use std::time::Duration;

use crate::io::background::BackgroundIoAttempt;
#[cfg(test)]
use crate::io::background::IoRecovery;
use crate::io::engine::BoundedIoRequest;
use crate::io::engine::CACHE_IO_COMPLETION_TIMEOUT;
use crate::io::engine::IoBuffer;
use crate::io::engine::IoEngine;
use crate::io::engine::IoOperation;
use crate::io::engine::OperationKind;
use crate::io::engine::RequestId;
use crate::io::engine::submit_background_io;
use crate::io::file::DIRECT_IO_ALIGNMENT;
use crate::io::file::WritePoint;
use crate::region::manager::RegionWriteSpan;
use crate::region::recovery::DATA_REGION_AREA_OFFSET;
use crate::region::recovery::DataGeometry;

pub struct WriteSubmitError {
    pub error: io::Error,
    pub span: RegionWriteSpan,
    pub buffer: Option<IoBuffer>,
}

impl fmt::Debug for WriteSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriteSubmitError")
            .field("error", &self.error)
            .field("span", &self.span)
            .field("buffer_returned", &self.buffer.is_some())
            .finish()
    }
}

impl fmt::Display for WriteSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for WriteSubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub struct PendingWrite {
    span: RegionWriteSpan,
    expected_len: usize,
    request_id: RequestId,
    request: BoundedIoRequest,
}

pub struct WriteCompletion {
    pub span: RegionWriteSpan,
    pub result: io::Result<()>,
    pub buffer: Option<IoBuffer>,
}

impl PendingWrite {
    pub fn wait(self, engine: &IoEngine, attempt: &mut BackgroundIoAttempt<'_>) -> WriteCompletion {
        let completion = match self.request.wait_background(engine, attempt) {
            Ok(completion) => completion,
            Err(timeout) => {
                let (error, buffer) = timeout.into_buffer();
                return WriteCompletion {
                    span: self.span,
                    result: Err(error),
                    buffer,
                };
            }
        };
        let identity_valid =
            completion.request_id == self.request_id && completion.kind == OperationKind::Write;
        let bytes_transferred = completion.bytes_transferred;
        let (io_result, buffer) = completion.into_io_result();
        let result = io_result.and_then(|completed| {
            if !identity_valid {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Region span completion identity does not match its request",
                ));
            }
            if completed != self.expected_len || bytes_transferred != self.expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Region span write completed with the wrong byte count",
                ));
            }
            let Some(buffer) = buffer.as_ref() else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Region span completion did not return its owned buffer",
                ));
            };
            if buffer.len() != self.expected_len {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Region span completion returned a buffer with the wrong length",
                ));
            }
            Ok(())
        });
        WriteCompletion {
            span: self.span,
            result,
            buffer,
        }
    }
}

// The error returns the owned aligned buffer without another fallible
// allocation; boxing it would violate that overload-path property.
#[allow(clippy::result_large_err)]
pub fn submit_write(
    engine: &IoEngine,
    geometry: DataGeometry,
    span: RegionWriteSpan,
    buffer: IoBuffer,
    absolute: u64,
    attempt: &mut BackgroundIoAttempt<'_>,
) -> Result<PendingWrite, WriteSubmitError> {
    let (expected_len, expected_absolute) = match validate_span(geometry, span) {
        Ok(validated) => validated,
        Err(error) => {
            return Err(WriteSubmitError {
                error,
                span,
                buffer: Some(buffer),
            });
        }
    };
    if buffer.len() != expected_len || absolute != expected_absolute {
        return Err(WriteSubmitError {
            error: io::Error::new(
                io::ErrorKind::InvalidInput,
                "staging job does not match its Region span",
            ),
            span,
            buffer: Some(buffer),
        });
    }
    let buffer_is_direct_aligned = buffer.as_slice().is_ok_and(|bytes| {
        (bytes.as_ptr() as usize).is_multiple_of(DIRECT_IO_ALIGNMENT)
            && bytes.len() % DIRECT_IO_ALIGNMENT == 0
    });
    if !buffer_is_direct_aligned {
        return Err(WriteSubmitError {
            error: io::Error::new(
                io::ErrorKind::InvalidInput,
                "Region span buffer is not direct-I/O aligned",
            ),
            span,
            buffer: Some(buffer),
        });
    }
    let request = match submit_background_io(
        engine,
        IoOperation::write(WritePoint::Record, buffer, absolute),
        CACHE_IO_COMPLETION_TIMEOUT,
        attempt,
    ) {
        Ok(request) => request,
        Err(error) => {
            let (error, buffer) = error.into_buffer();
            return Err(WriteSubmitError {
                error,
                span,
                buffer,
            });
        }
    };
    Ok(PendingWrite {
        span,
        expected_len,
        request_id: request.id(),
        request,
    })
}

fn validate_span(geometry: DataGeometry, span: RegionWriteSpan) -> io::Result<(usize, u64)> {
    if !geometry.is_valid()
        || span.region_id >= geometry.region_count
        || !span.start_offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
        || !span.end_offset.is_multiple_of(DIRECT_IO_ALIGNMENT as u64)
        || span.end_offset <= span.start_offset
        || span.end_offset > geometry.region_size
        || span.record_count == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Region write span",
        ));
    }
    let length = span
        .end_offset
        .checked_sub(span.start_offset)
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length != 0 && *length % DIRECT_IO_ALIGNMENT == 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Region write span length is invalid",
            )
        })?;
    let absolute = u64::from(span.region_id)
        .checked_mul(geometry.region_size)
        .and_then(|offset| offset.checked_add(DATA_REGION_AREA_OFFSET))
        .and_then(|offset| offset.checked_add(span.start_offset))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow"))?;
    let absolute_end = absolute
        .checked_add(length as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write end overflow"))?;
    if absolute % DIRECT_IO_ALIGNMENT as u64 != 0 || absolute_end > geometry.data_file_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region write span exceeds the data file",
        ));
    }
    Ok((length, absolute))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::io::engine::IoEngine;
    use crate::io::file::PositionedIo;
    use crate::managed_memory::BufferLease;

    #[derive(Default)]
    struct RecordingIo {
        writes: Mutex<Vec<(WritePoint, u64, Vec<u8>)>>,
        delay: Duration,
    }

    impl PositionedIo for RecordingIo {
        fn read_at(
            &self,
            #[expect(unused_variables)] buffer: &mut [u8],
            #[expect(unused_variables)] offset: u64,
        ) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "read unused"))
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            std::thread::sleep(self.delay);
            self.writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((point, offset, buffer.to_vec()));
            Ok(buffer.len())
        }
    }

    fn geometry() -> DataGeometry {
        let region_size = 64 * 1024;
        let region_count = 2;
        DataGeometry {
            data_file_len: DataGeometry::expected_file_len(region_size, region_count).unwrap(),
            region_size,
            region_count,
        }
    }

    fn span() -> RegionWriteSpan {
        RegionWriteSpan {
            shard_id: 0,
            region_id: 1,
            start_offset: 0,
            end_offset: 4096,
            record_count: 3,
            max_seqno: 21,
        }
    }

    #[test]
    fn late_span_completion_remains_valid_and_keeps_engine_usable() {
        let io = Arc::new(RecordingIo {
            delay: CACHE_IO_COMPLETION_TIMEOUT + Duration::from_millis(50),
            ..RecordingIo::default()
        });
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let mut lease = BufferLease::try_fixed(4096).unwrap();
        lease.prepared_mut(4096).unwrap().fill(0x5a);
        let absolute = DATA_REGION_AREA_OFFSET + geometry().region_size;
        let io_recovery = IoRecovery::new(Some(Duration::from_secs(5)));
        let mut attempt = BackgroundIoAttempt::new(&io_recovery, None);
        let completion = submit_write(
            &engine,
            geometry(),
            span(),
            IoBuffer::for_write(lease, 4096).unwrap(),
            absolute,
            &mut attempt,
        )
        .unwrap()
        .wait(&engine, &mut attempt);
        assert!(completion.result.is_ok());
        attempt.finish();
        assert!(!io_recovery.is_recovering());
        assert_eq!(completion.span, span());
        assert_eq!(
            completion.buffer.unwrap().as_slice().unwrap(),
            &[0x5a; 4096]
        );
        assert_eq!(io.writes.lock().unwrap().len(), 1);
        assert!(engine.try_reserve_read().is_ok());
        engine.shutdown().unwrap();
    }

    #[test]
    fn span_write_preserves_owned_buffer_and_maps_region_offset_exactly() {
        let io = Arc::new(RecordingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let mut lease = BufferLease::try_fixed(4096).unwrap();
        lease.prepared_mut(4096).unwrap().fill(0x5a);

        let buffer = IoBuffer::for_write(lease, 4096).unwrap();
        let absolute = DATA_REGION_AREA_OFFSET + geometry().region_size;
        let io_recovery = IoRecovery::new(Some(Duration::ZERO));
        let mut attempt = BackgroundIoAttempt::new(&io_recovery, None);
        let completion = submit_write(&engine, geometry(), span(), buffer, absolute, &mut attempt)
            .unwrap()
            .wait(&engine, &mut attempt);
        assert!(completion.result.is_ok());
        assert_eq!(completion.span, span());
        assert!(completion.buffer.is_some());
        drop(completion.buffer);

        let writes = io
            .writes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(writes.len(), 1);
        assert_eq!(writes[0].0, WritePoint::Record);
        assert_eq!(
            writes[0].1,
            DATA_REGION_AREA_OFFSET + geometry().region_size
        );
        assert_eq!(writes[0].2, vec![0x5a; 4096]);
        drop(writes);
        engine.shutdown().unwrap();
    }

    #[test]
    fn invalid_span_returns_the_only_buffer_without_submitting_io() {
        let io = Arc::new(RecordingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let mut invalid = span();
        invalid.end_offset += 1;
        let buffer = IoBuffer::for_write(BufferLease::try_fixed(4096).unwrap(), 4096).unwrap();
        let error = match submit_write(
            &engine,
            geometry(),
            invalid,
            buffer,
            0,
            &mut BackgroundIoAttempt::new(&IoRecovery::new(Some(Duration::ZERO)), None),
        ) {
            Err(error) => error,
            Ok(_) => panic!("unaligned span must not be submitted"),
        };
        assert_eq!(error.error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.buffer.is_some());
        drop(error.buffer);
        assert!(
            io.writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
        engine.shutdown().unwrap();
    }
}
