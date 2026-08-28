// Copyright 2026 ScopeDB
// SPDX-License-Identifier: Apache-2.0

//! Owned-buffer record reads for the RegionStore data path.
//!
//! A packed record may start on a 32-byte boundary, while direct I/O requires
//! a 4 KiB-aligned address, offset, and length. The index stores a size-class
//! upper bound, so buffered I/O reads that bounded range and direct I/O expands
//! it once to 4 KiB boundaries. Region generation and the exact record envelope
//! are validated above without another lookup or read.

use std::io;
use std::ops::Range;

use crate::format::RECORD_ALIGNMENT;
use crate::index::IndexEntry;
use crate::io_engine::{
    BoundedIoRequest, IoBuffer, IoCompletion, IoDeadlineExceeded, IoEngine, IoOperation,
    OperationKind, ReadSlot, RequestId, submit_cache_read,
};
use crate::recovery::{DATA_REGION_AREA_OFFSET, DataGeometry};
use crate::resources::BufferLease;

pub(crate) const _READ_ALIGNMENT: usize = 4096;
pub(crate) const _MAX_READ_ALIGNMENT_OVERHEAD: usize = 2 * _READ_ALIGNMENT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReadCandidate {
    pub(crate) entry: IndexEntry,
    pub(crate) region_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReadPlan {
    pub(crate) hash: u64,
    pub(crate) entry: IndexEntry,
    pub(crate) region_generation: u64,
    pub(crate) absolute: u64,
    pub(crate) read_len: usize,
    pub(crate) record_range: Range<usize>,
}

pub(crate) struct PendingRead {
    plan: ReadPlan,
    request_id: RequestId,
    request: BoundedIoRequest,
}

pub(crate) struct ReadCompletion {
    pub(crate) plan: ReadPlan,
    pub(crate) result: io::Result<()>,
    pub(crate) buffer: Option<BufferLease>,
}

impl ReadCompletion {
    /// Returns the bounded candidate range only after every completion
    /// invariant has passed. Direct-I/O alignment bytes stay private.
    pub(crate) fn record_bytes(&self) -> Option<&[u8]> {
        if self.result.is_err() {
            return None;
        }
        self.buffer
            .as_ref()?
            .prepared(self.plan.read_len)
            .ok()?
            .get(self.plan.record_range.clone())
    }
}

impl PendingRead {
    #[cfg(test)]
    pub(crate) fn wait(self, engine: &dyn IoEngine) -> ReadCompletion {
        let Self {
            plan,
            request_id,
            request,
        } = self;
        let completion = request.wait(engine);
        Self::finish(plan, request_id, completion)
    }

    pub(crate) async fn wait_async(
        self,
        engine: std::sync::Arc<dyn IoEngine>,
        tokio_handle: &tokio::runtime::Handle,
    ) -> ReadCompletion {
        let Self {
            plan,
            request_id,
            request,
        } = self;
        let completion = request.wait_async(engine, tokio_handle).await;
        Self::finish(plan, request_id, completion)
    }

    fn finish(
        plan: ReadPlan,
        request_id: RequestId,
        completion: Result<IoCompletion, IoDeadlineExceeded>,
    ) -> ReadCompletion {
        let completion = match completion {
            Ok(completion) => completion,
            Err(timeout) => {
                let (error, buffer) = timeout.into_lease();
                return ReadCompletion {
                    plan,
                    result: Err(error),
                    buffer,
                };
            }
        };
        let identity_valid =
            completion.request_id == request_id && completion.kind == OperationKind::Read;
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
            .is_some_and(|buffer| buffer.prepared(plan.read_len).is_err())
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
                if completed != plan.read_len || bytes_transferred != plan.read_len {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "Region record read completed with the wrong byte count",
                    ));
                }
                Ok(())
            }),
        };
        ReadCompletion {
            plan,
            result,
            buffer,
        }
    }
}

/// Submits one bounded planned range using an owned alignment-rounded buffer.
///
/// Validation happens before the lease is prepared or submitted. Any rejected
/// operation drops its lease immediately.
pub(crate) fn submit_read(
    engine: &dyn IoEngine,
    slot: ReadSlot,
    plan: ReadPlan,
    buffer: BufferLease,
) -> io::Result<PendingRead> {
    let buffer = IoBuffer::for_read(buffer, plan.read_len).map_err(|error| error.error)?;
    let request = submit_cache_read(engine, slot, IoOperation::read(buffer, plan.absolute))
        .map_err(|error| error.into_lease().0)?;
    Ok(PendingRead {
        plan,
        request_id: request.id(),
        request,
    })
}

pub(crate) fn plan_read(
    geometry: DataGeometry,
    hash: u64,
    candidate: ReadCandidate,
    align_for_direct_io: bool,
) -> io::Result<ReadPlan> {
    let ReadCandidate {
        entry,
        region_generation,
    } = candidate;
    let location = entry.location;
    let offset = u64::from(location.offset());
    let record_len_upper = usize::try_from(location.record_len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region record length does not fit this platform",
        )
    })?;
    let record_len_upper_u64 = u64::try_from(record_len_upper).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region record length does not fit the data geometry",
        )
    })?;
    if !geometry.is_valid()
        || region_generation == 0
        || location.region_id() >= geometry.region_count
        || offset % u64::from(RECORD_ALIGNMENT) != 0
        || offset >= geometry.region_size
        || record_len_upper == 0
        || record_len_upper % RECORD_ALIGNMENT as usize != 0
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid durable Region record location",
        ));
    }
    // A valid final record may end exactly at the Region boundary while its
    // size class extends beyond it. Never read into the next Region.
    let record_len_u64 = record_len_upper_u64.min(geometry.region_size - offset);
    let record_len = usize::try_from(record_len_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded Region record length does not fit this platform",
        )
    })?;

    let record_absolute = u64::from(location.region_id())
        .checked_mul(geometry.region_size)
        .and_then(|base| DATA_REGION_AREA_OFFSET.checked_add(base))
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

    let alignment = _READ_ALIGNMENT as u64;
    let (absolute, io_end) = if align_for_direct_io {
        (
            record_absolute / alignment * alignment,
            align_up(record_absolute_end, alignment).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "aligned read overflow")
            })?,
        )
    } else {
        (record_absolute, record_absolute_end)
    };
    let read_len = io_end
        .checked_sub(absolute)
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length != 0 && (!align_for_direct_io || *length % _READ_ALIGNMENT == 0))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid Region record read"))?;
    let record_start = record_absolute
        .checked_sub(absolute)
        .and_then(|start| usize::try_from(start).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "record slice overflow"))?;
    let record_range = record_start
        ..record_start.checked_add(record_len).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "record slice end overflow")
        })?;
    let overhead = read_len.checked_sub(record_len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region read is shorter than its candidate range",
        )
    })?;
    if record_range.end > read_len
        || (align_for_direct_io
            && (!absolute.is_multiple_of(alignment) || overhead >= _MAX_READ_ALIGNMENT_OVERHEAD))
        || io_end > geometry.data_file_len
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Region read exceeds its bounds",
        ));
    }

    Ok(ReadPlan {
        hash,
        entry,
        region_generation,
        absolute,
        read_len,
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
    use crate::io_backend::{IoBackend, SyncMode, SyncPoint, WritePoint};
    use crate::io_engine::BackendIoEngine;
    use crate::resources::{ResourceController, ResourceLimits};

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

    fn entry(location: crate::index::PackedLocation) -> IndexEntry {
        IndexEntry { location }
    }

    fn candidate(entry: IndexEntry) -> ReadCandidate {
        ReadCandidate {
            entry,
            region_generation: 1,
        }
    }

    #[test]
    fn unaligned_record_uses_one_aligned_read_and_returns_its_exact_slice() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = ResourceController::try_new(ResourceLimits {
            memory_limit_bytes: _READ_ALIGNMENT,
            reserved_memory_bytes: 0,
        })
        .unwrap();
        let location = PackedLocation::new(1, 32, 64).unwrap();
        let entry = entry(location);
        let plan = plan_read(geometry(), 7, candidate(entry), true).unwrap();

        let slot = engine.try_reserve_read().unwrap();
        let completion = submit_read(
            &engine,
            slot,
            plan,
            resources.try_read_buffer(_READ_ALIGNMENT).unwrap(),
        )
        .unwrap()
        .wait(&engine);
        assert!(completion.result.is_ok());
        assert_eq!(completion.plan.record_range, 32..96);
        assert_eq!(completion.record_bytes().unwrap().len(), 64);

        let record_absolute = DATA_REGION_AREA_OFFSET + geometry().region_size + 32;
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
        assert_eq!(resources.managed_memory_snapshot().current_bytes, 0);
    }

    #[test]
    fn buffered_record_uses_one_size_class_upper_bound_read() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = ResourceController::try_new(ResourceLimits {
            memory_limit_bytes: _READ_ALIGNMENT,
            reserved_memory_bytes: 0,
        })
        .unwrap();
        let entry = entry(PackedLocation::new(1, 32, 1120).unwrap());
        let plan = plan_read(geometry(), 7, candidate(entry), false).unwrap();
        let record_absolute = DATA_REGION_AREA_OFFSET + geometry().region_size + 32;

        let completion = submit_read(
            &engine,
            engine.try_reserve_read().unwrap(),
            plan,
            resources.try_read_buffer(1120).unwrap(),
        )
        .unwrap()
        .wait(&engine);
        assert!(completion.result.is_ok());
        assert_eq!(completion.plan.record_range, 0..1120);
        assert_eq!(completion.record_bytes().unwrap().len(), 1120);
        assert_eq!(
            backend
                .reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[(record_absolute, 1120)]
        );
        drop(completion.buffer);
        engine.shutdown().unwrap();
        assert_eq!(resources.managed_memory_snapshot().current_bytes, 0);
    }

    #[test]
    fn final_record_size_class_is_clamped_to_its_region() {
        let record_len = 1056;
        let offset = geometry().region_size as u32 - record_len;
        let entry = entry(PackedLocation::new(1, offset, 1120).unwrap());
        let plan = plan_read(geometry(), 7, candidate(entry), false).unwrap();

        assert_eq!(plan.record_range, 0..record_len as usize);
        assert_eq!(plan.read_len, record_len as usize);
        assert_eq!(
            plan.absolute,
            DATA_REGION_AREA_OFFSET + geometry().region_size + u64::from(offset)
        );
    }

    #[test]
    fn invalid_entry_is_rejected_before_allocating_or_issuing_io() {
        let backend = Arc::new(RecordingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let invalid = entry(PackedLocation::new(geometry().region_count, 0, 32).unwrap());

        let error = plan_read(geometry(), 7, candidate(invalid), true).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            backend
                .reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );

        let generation_zero = ReadCandidate {
            entry: entry(PackedLocation::new(0, 0, 32).unwrap()),
            region_generation: 0,
        };
        assert_eq!(
            plan_read(geometry(), 7, generation_zero, true)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        engine.shutdown().unwrap();
    }
}
