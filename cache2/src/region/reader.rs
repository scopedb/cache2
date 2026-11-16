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

//! Owned-buffer record reads for the RegionStore data path.
//!
//! A packed record may start on a 32-byte boundary, while direct I/O requires
//! a 4 KiB-aligned address, offset, and length. The index stores a size-class
//! upper bound, so buffered I/O reads that bounded range and direct I/O expands
//! it once to 4 KiB boundaries. Region generation and the exact record envelope
//! are validated above without another lookup or read.

use std::io;
use std::ops::Range;
use std::sync::Arc;

use tokio::runtime::Handle;

use crate::io::engine::BoundedIoRequest;
use crate::io::engine::IoBuffer;
use crate::io::engine::IoCompletion;
use crate::io::engine::IoDeadlineExceeded;
use crate::io::engine::IoEngine;
use crate::io::engine::IoOperation;
use crate::io::engine::ReadSlot;
use crate::io::engine::submit_cache_read;
use crate::io::file::DIRECT_IO_ALIGNMENT;
use crate::managed_memory::BufferLease;
use crate::region::index::packed::IndexEntry;
use crate::region::record::RECORD_ALIGNMENT;
use crate::region::recovery::DataGeometry;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadCandidate {
    pub entry: IndexEntry,
    pub region_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadDesc {
    pub hash: u64,
    pub entry: IndexEntry,
    pub region_generation: u64,
    pub absolute: u64,
    pub read_len: usize,
    pub record_range: Range<usize>,
}

pub struct PendingRead {
    desc: ReadDesc,
    request: BoundedIoRequest,
}

pub struct ReadCompletion {
    pub desc: ReadDesc,
    pub result: io::Result<BufferLease>,
}

impl PendingRead {
    #[cfg(test)]
    pub fn wait(self, engine: &IoEngine) -> ReadCompletion {
        let completion = self.request.wait(engine);
        Self::finish(self.desc, completion)
    }

    pub async fn wait_async(self, engine: Arc<IoEngine>, tokio_handle: &Handle) -> ReadCompletion {
        let completion = self.request.wait_async(engine, tokio_handle).await;
        Self::finish(self.desc, completion)
    }

    fn finish(
        desc: ReadDesc,
        completion: Result<IoCompletion, IoDeadlineExceeded>,
    ) -> ReadCompletion {
        let completion = match completion {
            Ok(completion) => completion,
            Err(timeout) => {
                return ReadCompletion {
                    desc,
                    result: Err(timeout.into_lease().0),
                };
            }
        };
        let (io_result, buffer) = completion.into_lease();
        let result = (|| {
            let transferred = io_result?;
            let buffer = buffer.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Region record completion did not return its owned buffer",
                )
            })?;
            if transferred != desc.read_len {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Region record read completed with the wrong byte count",
                ));
            }
            if buffer.prepared(desc.read_len).is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Region record completion returned a short buffer",
                ));
            }
            Ok(buffer)
        })();
        ReadCompletion { desc, result }
    }
}

/// Submits one bounded read range using an owned alignment-rounded buffer.
///
/// Validation happens before the lease is prepared or submitted. Any rejected
/// operation drops its lease immediately.
pub fn submit_read(
    engine: &IoEngine,
    slot: ReadSlot,
    desc: ReadDesc,
    buffer: BufferLease,
) -> io::Result<PendingRead> {
    let buffer = IoBuffer::for_read(buffer, desc.read_len).map_err(|error| error.error)?;
    let request = submit_cache_read(engine, slot, IoOperation::read(buffer, desc.absolute))
        .map_err(|error| error.into_lease().0)?;
    Ok(PendingRead { desc, request })
}

pub fn describe_read(
    geometry: DataGeometry,
    hash: u64,
    candidate: ReadCandidate,
    align_for_direct_io: bool,
) -> io::Result<ReadDesc> {
    let ReadCandidate {
        entry,
        region_generation,
    } = candidate;
    let location = entry.location;
    let offset = u64::from(location.offset());
    let record_len_upper = u64::from(location.record_len());
    if !geometry.is_valid()
        || region_generation == 0
        || !offset.is_multiple_of(u64::from(RECORD_ALIGNMENT))
        || record_len_upper == 0
        || !record_len_upper.is_multiple_of(u64::from(RECORD_ALIGNMENT))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid durable Region record location",
        ));
    }
    // A valid final record may end exactly at the Region boundary while its
    // size class extends beyond it. Never read into the next Region.
    let Some(record_len_u64) = geometry
        .region_size
        .checked_sub(offset)
        .filter(|remaining| *remaining != 0)
        .map(|remaining| record_len_upper.min(remaining))
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid durable Region record location",
        ));
    };
    let record_len = usize::try_from(record_len_u64).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "bounded Region record length does not fit this platform",
        )
    })?;
    let record_absolute = geometry
        .span_offset(location.region_id(), offset, record_len_u64)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid durable Region record location",
            )
        })?;
    let record_absolute_end = record_absolute + record_len_u64;

    let alignment = DIRECT_IO_ALIGNMENT as u64;
    let (absolute, io_end) = if align_for_direct_io {
        (
            record_absolute / alignment * alignment,
            record_absolute_end
                .checked_next_multiple_of(alignment)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "aligned read overflow")
                })?,
        )
    } else {
        (record_absolute, record_absolute_end)
    };
    let read_len = usize::try_from(io_end - absolute)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid Region record read"))?;
    let record_start = (record_absolute - absolute) as usize;

    Ok(ReadDesc {
        hash,
        entry,
        region_generation,
        absolute,
        read_len,
        record_range: record_start..record_start + record_len,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use super::*;
    use crate::io::engine::IoEngine;
    use crate::io::file::PositionedIo;
    use crate::io::file::WritePoint;
    use crate::managed_memory::ManagedMemory;
    use crate::managed_memory::ManagedMemoryLimits;
    use crate::region::index::packed::PackedLocation;
    use crate::region::record::RECORD_HEADER_SIZE;
    use crate::region::record::codec::required_record_bytes;
    use crate::region::recovery::DATA_REGION_AREA_OFFSET;

    #[derive(Default)]
    struct RecordingIo {
        reads: Mutex<Vec<(u64, usize)>>,
    }

    impl PositionedIo for RecordingIo {
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

    fn entry(location: PackedLocation) -> IndexEntry {
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
        let io = Arc::new(RecordingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let managed_memory = ManagedMemory::try_new(ManagedMemoryLimits {
            memory_limit_bytes: DIRECT_IO_ALIGNMENT,
            reserved_memory_bytes: 0,
        })
        .unwrap();
        let location = PackedLocation::new(1, 32, 64).unwrap();
        let entry = entry(location);
        let desc = describe_read(geometry(), 7, candidate(entry), true).unwrap();

        let slot = engine.try_reserve_read().unwrap();
        let completion = submit_read(
            &engine,
            slot,
            desc,
            managed_memory.try_read_buffer(DIRECT_IO_ALIGNMENT).unwrap(),
        )
        .unwrap()
        .wait(&engine);
        let buffer = completion.result.unwrap();
        let slice = &buffer.prepared(completion.desc.read_len).unwrap()
            [completion.desc.record_range.clone()];
        assert_eq!(completion.desc.record_range, 32..96);
        assert_eq!(slice.len(), 64);

        let record_absolute = DATA_REGION_AREA_OFFSET + geometry().region_size + 32;
        let reads = io
            .reads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(reads.as_slice(), &[(record_absolute - 32, 4096)]);
        let expected = (0..64)
            .map(|index| record_absolute.wrapping_add(index) as u8)
            .collect::<Vec<_>>();
        assert_eq!(slice, expected);
        drop(reads);
        drop(buffer);
        engine.shutdown().unwrap();
        assert_eq!(managed_memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn buffered_record_uses_one_size_class_upper_bound_read() {
        let io = Arc::new(RecordingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let managed_memory = ManagedMemory::try_new(ManagedMemoryLimits {
            memory_limit_bytes: DIRECT_IO_ALIGNMENT,
            reserved_memory_bytes: 0,
        })
        .unwrap();
        let entry = entry(PackedLocation::new(1, 32, 1120).unwrap());
        let desc = describe_read(geometry(), 7, candidate(entry), false).unwrap();
        let record_absolute = DATA_REGION_AREA_OFFSET + geometry().region_size + 32;

        let completion = submit_read(
            &engine,
            engine.try_reserve_read().unwrap(),
            desc,
            managed_memory.try_read_buffer(1120).unwrap(),
        )
        .unwrap()
        .wait(&engine);
        let buffer = completion.result.unwrap();
        let slice = &buffer.prepared(completion.desc.read_len).unwrap()
            [completion.desc.record_range.clone()];
        assert_eq!(completion.desc.record_range, 0..1120);
        assert_eq!(slice.len(), 1120);
        assert_eq!(
            io.reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_slice(),
            &[(record_absolute, 1120)]
        );
        drop(buffer);
        engine.shutdown().unwrap();
        assert_eq!(managed_memory.snapshot().current_bytes, 0);
    }

    #[test]
    fn final_record_size_class_is_clamped_to_its_region() {
        let record_len = 1056;
        let offset = geometry().region_size as u32 - record_len;
        let entry = entry(PackedLocation::new(1, offset, 1120).unwrap());
        let desc = describe_read(geometry(), 7, candidate(entry), false).unwrap();

        assert_eq!(desc.record_range, 0..record_len as usize);
        assert_eq!(desc.read_len, record_len as usize);
        assert_eq!(
            desc.absolute,
            DATA_REGION_AREA_OFFSET + geometry().region_size + u64::from(offset)
        );
    }

    #[test]
    fn invalid_entry_is_rejected_before_allocating_or_issuing_io() {
        let io = Arc::new(RecordingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let invalid = entry(PackedLocation::new(geometry().region_count, 0, 32).unwrap());

        let error = describe_read(geometry(), 7, candidate(invalid), true).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            io.reads
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_empty()
        );

        let generation_zero = ReadCandidate {
            entry: entry(PackedLocation::new(0, 0, 32).unwrap()),
            region_generation: 0,
        };
        assert_eq!(
            describe_read(geometry(), 7, generation_zero, true)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        engine.shutdown().unwrap();
    }

    #[test]
    fn maximum_read_buffer_is_derived_from_runtime_limits() {
        let geometry = DataGeometry {
            data_file_len: DataGeometry::expected_file_len(512 * 1024, 10).unwrap(),
            region_size: 512 * 1024,
            region_count: 10,
        };
        let value_len = geometry.region_size as usize - RECORD_HEADER_SIZE;
        let record_len = required_record_bytes(0, value_len).unwrap();
        assert_eq!(u64::from(record_len), geometry.region_size);
        let entry = IndexEntry {
            location: PackedLocation::new(0, 0, record_len).unwrap(),
        };
        assert_eq!(
            describe_read(
                geometry,
                1,
                ReadCandidate {
                    entry,
                    region_generation: 1,
                },
                true,
            )
            .unwrap()
            .read_len,
            geometry.region_size as usize
        );
    }
}
