//! One owned-buffer Region span submission for the data path.
//!
//! A successful write completion advances only the written prefix. The
//! disposable-cache protocol establishes durability once, when publishing a
//! CLEAN image, and deliberately has no per-span sync.

use std::fmt;
use std::io;

use crate::io_backend::{DIRECT_IO_ALIGNMENT, WritePoint};
use crate::io_engine::{
    BoundedIoRequest, IoBuffer, IoEngine, IoOperation, OperationKind, RequestId, submit_cache_io,
};
use crate::recovery::{DATA_REGION_AREA_OFFSET, DataGeometry};
use crate::region_manager::RegionWriteSpan;
use crate::runtime_config::MAX_WRITE_BATCH_BYTES;

pub(crate) struct RegionSpanSubmitError {
    pub(crate) error: io::Error,
    pub(crate) span: RegionWriteSpan,
    pub(crate) buffer: Option<IoBuffer>,
}

impl fmt::Debug for RegionSpanSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionSpanSubmitError")
            .field("error", &self.error)
            .field("span", &self.span)
            .field("buffer_returned", &self.buffer.is_some())
            .finish()
    }
}

impl fmt::Display for RegionSpanSubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for RegionSpanSubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(crate) struct RegionSpanFlight {
    span: RegionWriteSpan,
    expected_len: usize,
    request_id: RequestId,
    request: BoundedIoRequest,
}

pub(crate) struct RegionSpanCompletion {
    pub(crate) span: RegionWriteSpan,
    pub(crate) result: io::Result<()>,
    pub(crate) buffer: Option<IoBuffer>,
}

impl RegionSpanFlight {
    pub(crate) fn wait(self, engine: &dyn IoEngine) -> RegionSpanCompletion {
        let completion = match self.request.wait(engine) {
            Ok(completion) => completion,
            Err(timeout) => {
                let (error, buffer) = timeout.into_buffer();
                return RegionSpanCompletion {
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
        RegionSpanCompletion {
            span: self.span,
            result,
            buffer,
        }
    }
}

// The error returns the owned aligned buffer without another fallible
// allocation; boxing it would violate that overload-path property.
#[allow(clippy::result_large_err)]
pub(crate) fn submit_span(
    engine: &dyn IoEngine,
    geometry: DataGeometry,
    span: RegionWriteSpan,
    buffer: IoBuffer,
    absolute: u64,
) -> Result<RegionSpanFlight, RegionSpanSubmitError> {
    let (expected_len, expected_absolute) = match validate_span(geometry, span) {
        Ok(validated) => validated,
        Err(error) => {
            return Err(RegionSpanSubmitError {
                error,
                span,
                buffer: Some(buffer),
            });
        }
    };
    if buffer.len() != expected_len || absolute != expected_absolute {
        return Err(RegionSpanSubmitError {
            error: io::Error::new(
                io::ErrorKind::InvalidInput,
                "staging job does not match its Region span",
            ),
            span,
            buffer: Some(buffer),
        });
    }
    let buffer_is_direct_aligned = buffer.as_slice().is_ok_and(|bytes| {
        (bytes.as_ptr() as usize) % DIRECT_IO_ALIGNMENT == 0
            && bytes.len() % DIRECT_IO_ALIGNMENT == 0
    });
    if !buffer_is_direct_aligned {
        return Err(RegionSpanSubmitError {
            error: io::Error::new(
                io::ErrorKind::InvalidInput,
                "Region span buffer is not direct-I/O aligned",
            ),
            span,
            buffer: Some(buffer),
        });
    }
    let request = match submit_cache_io(
        engine,
        IoOperation::write(WritePoint::Record, buffer, absolute),
    ) {
        Ok(request) => request,
        Err(error) => {
            let (error, buffer) = error.into_buffer();
            return Err(RegionSpanSubmitError {
                error,
                span,
                buffer,
            });
        }
    };
    Ok(RegionSpanFlight {
        span,
        expected_len,
        request_id: request.id(),
        request,
    })
}

fn validate_span(geometry: DataGeometry, span: RegionWriteSpan) -> io::Result<(usize, u64)> {
    if !geometry.is_valid()
        || span.region_id >= geometry.region_count
        || span.start_offset % DIRECT_IO_ALIGNMENT as u64 != 0
        || span.end_offset % DIRECT_IO_ALIGNMENT as u64 != 0
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
        .filter(|length| {
            *length != 0 && *length <= MAX_WRITE_BATCH_BYTES && *length % DIRECT_IO_ALIGNMENT == 0
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Region write span exceeds the fixed batch size",
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
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::io_backend::{DirectIoStats, IoBackend, SyncMode, SyncPoint};
    use crate::io_engine::BackendIoEngine;
    use crate::resources::BufferLease;

    #[derive(Default)]
    struct RecordingBackend {
        writes: Mutex<Vec<(WritePoint, u64, Vec<u8>)>>,
    }

    impl IoBackend for RecordingBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(u64::MAX)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, _buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "read unused"))
        }

        fn write_at(&self, point: WritePoint, buffer: &[u8], offset: u64) -> io::Result<usize> {
            self.writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((point, offset, buffer.to_vec()));
            Ok(buffer.len())
        }

        fn sync(&self, _point: SyncPoint, _mode: SyncMode) -> io::Result<()> {
            Ok(())
        }

        fn try_lock_exclusive(&self) -> io::Result<()> {
            Ok(())
        }

        fn unlock(&self) -> io::Result<()> {
            Ok(())
        }

        fn direct_io_stats(&self) -> DirectIoStats {
            DirectIoStats::default()
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
            span_id: 7,
            cache_epoch: 3,
            region_id: 1,
            region_incarnation: 9,
            start_offset: 0,
            end_offset: 4096,
            record_count: 3,
            max_seqno: 21,
        }
    }

    #[test]
    fn span_write_preserves_owned_buffer_and_maps_region_offset_exactly() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let mut lease = BufferLease::try_fixed(4096).unwrap();
        lease.prepare(4096).unwrap().fill(0x5a);

        let buffer = IoBuffer::for_write(lease, 4096).unwrap();
        let absolute = DATA_REGION_AREA_OFFSET + geometry().region_size;
        let completion = submit_span(&engine, geometry(), span(), buffer, absolute)
            .unwrap()
            .wait(&engine);
        assert!(completion.result.is_ok());
        assert_eq!(completion.span, span());
        assert!(completion.buffer.is_some());
        drop(completion.buffer);

        let writes = backend
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
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let mut invalid = span();
        invalid.end_offset += 1;
        let buffer = IoBuffer::for_write(BufferLease::try_fixed(4096).unwrap(), 4096).unwrap();
        let error = match submit_span(&engine, geometry(), invalid, buffer, 0) {
            Err(error) => error,
            Ok(_) => panic!("unaligned span must not be submitted"),
        };
        assert_eq!(error.error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.buffer.is_some());
        drop(error.buffer);
        assert!(
            backend
                .writes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
        engine.shutdown().unwrap();
    }
}
