//! Aligned owned-buffer record reads for the RegionStore V2 data path.
//!
//! A packed record may start on a 32-byte boundary, while direct I/O requires
//! a 4 KiB-aligned address, offset, and length. This seam reads one aligned
//! envelope and reports the exact record slice inside the returned buffer.
//! Region generation and record contents are validated by the layers above.

use std::fmt;
use std::io;
use std::ops::Range;

use crate::index::{INDEX_FLAG_VOLATILE, IndexEntry};
use crate::io_engine::{IoBuffer, IoEngine, IoOperation, IoRequest, OperationKind, RequestId};
use crate::recovery_v2::{
    DATA_REGION_AREA_OFFSET_V2, DataGeometryV2, RECORD_ALIGNMENT_V2, REGION_HEADER_SIZE_V2,
};
use crate::resources::BufferLease;

pub(crate) const V2_READ_ALIGNMENT: usize = 4096;
pub(crate) const V2_MAX_READ_ENVELOPE_OVERHEAD: usize = 2 * V2_READ_ALIGNMENT;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RegionRecordReadPlanV2 {
    pub(crate) entry: IndexEntry,
    pub(crate) absolute: u64,
    pub(crate) io_len: usize,
    pub(crate) record_range: Range<usize>,
}

pub(crate) struct RegionRecordReadSubmitErrorV2 {
    pub(crate) error: io::Error,
    pub(crate) entry: IndexEntry,
    pub(crate) buffer: Option<BufferLease>,
}

impl fmt::Debug for RegionRecordReadSubmitErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RegionRecordReadSubmitErrorV2")
            .field("error", &self.error)
            .field("entry", &self.entry)
            .field("buffer_returned", &self.buffer.is_some())
            .finish()
    }
}

impl fmt::Display for RegionRecordReadSubmitErrorV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for RegionRecordReadSubmitErrorV2 {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

pub(crate) struct RegionRecordReadFlightV2 {
    plan: RegionRecordReadPlanV2,
    request_id: RequestId,
    request: IoRequest,
}

pub(crate) struct RegionRecordReadCompletionV2 {
    pub(crate) plan: RegionRecordReadPlanV2,
    pub(crate) result: io::Result<()>,
    pub(crate) buffer: Option<BufferLease>,
}

impl RegionRecordReadCompletionV2 {
    /// Returns the exact packed-record bytes only after every completion
    /// invariant has passed. The surrounding aligned envelope stays private.
    pub(crate) fn record_bytes(&self) -> Option<&[u8]> {
        if self.result.is_err() {
            return None;
        }
        self.buffer
            .as_ref()?
            .prepared(self.plan.io_len)
            .ok()?
            .get(self.plan.record_range.clone())
    }
}

impl RegionRecordReadFlightV2 {
    pub(crate) fn wait(self) -> RegionRecordReadCompletionV2 {
        let completion = self.request.wait();
        let identity_valid =
            completion.request_id == self.request_id && completion.kind == OperationKind::Read;
        let bytes_transferred = completion.bytes_transferred;
        let (io_result, buffer) = completion.into_lease();

        let protocol_error = if !identity_valid {
            Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "Region record completion identity does not match its request",
            ))
        } else if buffer.is_none() {
            Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "Region record completion did not return its owned buffer",
            ))
        } else if buffer
            .as_ref()
            .is_some_and(|buffer| buffer.prepared(self.plan.io_len).is_err())
        {
            Some(io::Error::new(
                io::ErrorKind::InvalidData,
                "Region record completion returned a short buffer",
            ))
        } else {
            None
        };

        let result = match protocol_error {
            Some(error) => Err(error),
            None => io_result.and_then(|completed| {
                if completed != self.plan.io_len || bytes_transferred != self.plan.io_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Region record read completed with the wrong byte count",
                    ));
                }
                Ok(())
            }),
        };
        RegionRecordReadCompletionV2 {
            plan: self.plan,
            result,
            buffer,
        }
    }
}

/// Plans and submits one exact positioned read using an owned pool lease.
///
/// Validation happens before the lease is prepared or submitted. Every local
/// failure returns that lease; engine failures preserve whatever buffer the
/// engine returned with the rejected operation.
pub(crate) fn submit_record_read(
    engine: &dyn IoEngine,
    geometry: DataGeometryV2,
    entry: IndexEntry,
    mut buffer: BufferLease,
) -> Result<RegionRecordReadFlightV2, RegionRecordReadSubmitErrorV2> {
    let plan = match plan_record_read(geometry, entry) {
        Ok(plan) => plan,
        Err(error) => {
            return Err(RegionRecordReadSubmitErrorV2 {
                error,
                entry,
                buffer: Some(buffer),
            });
        }
    };
    // Device reads overwrite the complete envelope. Preserve initialized pool
    // capacity instead of clearing it first; clearing would add one full
    // memory write to every 16--256 KiB cache hit.
    if buffer.grow_preserving(plan.io_len).is_err() {
        return Err(RegionRecordReadSubmitErrorV2 {
            error: io::Error::new(
                io::ErrorKind::OutOfMemory,
                "read buffer cannot hold the aligned Region record envelope",
            ),
            entry,
            buffer: Some(buffer),
        });
    }
    let buffer = match IoBuffer::from_lease(buffer, plan.io_len) {
        Ok(buffer) => buffer,
        Err(error) => {
            return Err(RegionRecordReadSubmitErrorV2 {
                error: error.error,
                entry,
                buffer: Some(error.lease),
            });
        }
    };
    let request = match engine.submit_wait(IoOperation::read(buffer, plan.absolute)) {
        Ok(request) => request,
        Err(error) => {
            let (error, buffer) = error.into_lease();
            return Err(RegionRecordReadSubmitErrorV2 {
                error,
                entry,
                buffer,
            });
        }
    };
    Ok(RegionRecordReadFlightV2 {
        plan,
        request_id: request.id(),
        request,
    })
}

pub(crate) fn plan_record_read(
    geometry: DataGeometryV2,
    entry: IndexEntry,
) -> io::Result<RegionRecordReadPlanV2> {
    let location = entry.location;
    let offset = u64::from(location.offset());
    let record_len = usize::try_from(location.record_len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region record length does not fit this platform",
        )
    })?;
    let record_len_u64 = u64::try_from(record_len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region record length does not fit the data geometry",
        )
    })?;
    let record_end = offset.checked_add(record_len_u64);
    if !geometry.is_valid()
        || entry.seqno == 0
        || entry.flags & INDEX_FLAG_VOLATILE != 0
        || location.is_tombstone()
        || location.region_id() >= geometry.region_count
        || offset < u64::from(REGION_HEADER_SIZE_V2)
        || offset % u64::from(RECORD_ALIGNMENT_V2) != 0
        || record_len == 0
        || record_len % RECORD_ALIGNMENT_V2 as usize != 0
        || record_end.is_none_or(|end| end > geometry.region_size)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid durable V2 Region record location",
        ));
    }

    let record_absolute = u64::from(location.region_id())
        .checked_mul(geometry.region_size)
        .and_then(|base| DATA_REGION_AREA_OFFSET_V2.checked_add(base))
        .and_then(|base| base.checked_add(offset))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow"))?;
    let record_absolute_end = record_absolute
        .checked_add(record_len_u64)
        .filter(|end| *end <= geometry.data_file_len)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "Region record exceeds the data file",
            )
        })?;

    let alignment = V2_READ_ALIGNMENT as u64;
    let absolute = record_absolute / alignment * alignment;
    let io_end = align_up(record_absolute_end, alignment)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read envelope overflow"))?;
    let io_len = io_end
        .checked_sub(absolute)
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length != 0 && *length % V2_READ_ALIGNMENT == 0)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid aligned Region record envelope",
            )
        })?;
    let record_start = record_absolute
        .checked_sub(absolute)
        .and_then(|start| usize::try_from(start).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "record slice overflow"))?;
    let record_range = record_start
        ..record_start.checked_add(record_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "record slice end overflow")
        })?;
    let overhead = io_len.checked_sub(record_len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "aligned Region record envelope is shorter than its record",
        )
    })?;
    if absolute % alignment != 0
        || record_range.end > io_len
        || overhead >= V2_MAX_READ_ENVELOPE_OVERHEAD
        || io_end > geometry.data_file_len
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "aligned Region record envelope exceeds its bounds",
        ));
    }

    Ok(RegionRecordReadPlanV2 {
        entry,
        absolute,
        io_len,
        record_range,
    })
}

fn align_up(value: u64, alignment: u64) -> Option<u64> {
    value
        .checked_add(alignment.checked_sub(1)?)
        .map(|end| end / alignment * alignment)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::index::PackedLocation;
    use crate::io_backend::{DirectIoStats, IoBackend, SyncMode, SyncPoint, WritePoint};
    use crate::io_engine::BackendIoEngine;
    use crate::resources::DedicatedBufferPool;

    #[derive(Default)]
    struct RecordingBackend {
        reads: Mutex<Vec<(u64, usize)>>,
    }

    impl IoBackend for RecordingBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(u64::MAX)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, buffer: &mut [u8], offset: u64) -> io::Result<usize> {
            self.reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((offset, buffer.len()));
            for (index, byte) in buffer.iter_mut().enumerate() {
                *byte = offset.wrapping_add(index as u64) as u8;
            }
            Ok(buffer.len())
        }

        fn write_at(&self, _point: WritePoint, _buffer: &[u8], _offset: u64) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::Unsupported, "write unused"))
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

    fn geometry() -> DataGeometryV2 {
        let region_size = 64 * 1024;
        let region_count = 2;
        DataGeometryV2 {
            data_file_len: DataGeometryV2::expected_file_len(region_size, region_count).unwrap(),
            region_size,
            region_count,
        }
    }

    fn entry(location: crate::index::PackedLocation) -> IndexEntry {
        IndexEntry {
            location,
            seqno: 11,
            namespace_id: 0,
            flags: 0,
        }
    }

    #[test]
    fn unaligned_record_uses_one_aligned_read_and_returns_its_exact_slice() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let pool = DedicatedBufferPool::try_new(1, V2_READ_ALIGNMENT).unwrap();
        let location = PackedLocation::new(1, REGION_HEADER_SIZE_V2 + 32, 64, false).unwrap();
        let entry = entry(location);

        let completion = submit_record_read(&engine, geometry(), entry, pool.acquire().unwrap())
            .unwrap()
            .wait();
        assert!(completion.result.is_ok());
        assert_eq!(completion.plan.record_range, 32..96);
        assert_eq!(completion.record_bytes().unwrap().len(), 64);

        let record_absolute = DATA_REGION_AREA_OFFSET_V2
            + geometry().region_size
            + u64::from(REGION_HEADER_SIZE_V2 + 32);
        let reads = backend
            .reads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(reads.as_slice(), &[(record_absolute - 32, 4096)]);
        let expected = (0..64)
            .map(|index| record_absolute.wrapping_add(index) as u8)
            .collect::<Vec<_>>();
        assert_eq!(completion.record_bytes().unwrap(), expected);
        drop(reads);
        drop(completion.buffer);
        engine.shutdown().unwrap();
        assert_eq!(pool.snapshot().in_use, 0);
    }

    #[test]
    fn invalid_entry_returns_the_buffer_without_issuing_io() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let pool = DedicatedBufferPool::try_new(1, V2_READ_ALIGNMENT).unwrap();
        let invalid = entry(PackedLocation::from_raw(0));

        let error = match submit_record_read(&engine, geometry(), invalid, pool.acquire().unwrap())
        {
            Err(error) => error,
            Ok(_) => panic!("invalid packed entry must not be submitted"),
        };
        assert_eq!(error.error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.buffer.is_some());
        assert!(
            backend
                .reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );
        drop(error.buffer);
        engine.shutdown().unwrap();
        assert_eq!(pool.snapshot().in_use, 0);
    }
}
