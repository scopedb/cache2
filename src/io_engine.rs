//! Owned-buffer I/O runtime.
//!
//! Requests transfer ownership of their aligned buffer to the engine. The
//! buffer is returned only with the target operation's completion, which is
//! the lifetime rule required by both positioned I/O workers and `io_uring`.

use std::fmt;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, TryLockError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use crate::io_backend::DirectIoStatsHandle;
#[cfg(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
use crate::io_backend::RuntimeIoPath;
use crate::io_backend::{
    IoBackend, SyncMode, SyncPoint, WritePoint, read_exact_at_uninit_with_progress,
    write_all_at_with_progress,
};
#[cfg(unix)]
use crate::io_backend::{RuntimeFileBackend, RuntimeFileSet};
use crate::resources::{BufferLease, CACHE_THREAD_STACK_BYTES};
#[cfg(unix)]
use crate::runtime_config::IoEngine as ConfiguredIoEngine;
use crate::snapshot::CacheIoSnapshot;

pub(crate) const IO_BUFFER_ALIGNMENT: usize = 4096;
pub(crate) const MAX_IO_REQUESTS_PER_WORKER: usize = 4096;
/// A stalled cache-device operation must not hold a frontend or shutdown
/// barrier forever. This is intentionally a fixed production guardrail rather
/// than a durability knob: cache contents are disposable.
pub(crate) const CACHE_IO_COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
/// Cancellation is only a request. Give the target operation a short window to
/// publish its own completion, which is the actual buffer-lifetime fence.
pub(crate) const CACHE_IO_CANCEL_GRACE: Duration = Duration::from_millis(100);

/// A logical range of an engine-budgeted aligned buffer lease.
pub(crate) struct IoBuffer {
    lease: BufferLease,
    length: usize,
}

impl IoBuffer {
    pub(crate) fn for_write(mut lease: BufferLease, length: usize) -> Result<Self, IoBufferError> {
        if length > u32::MAX as usize {
            return Err(IoBufferError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "I/O buffer length exceeds the per-request limit",
                ),
                lease,
            });
        }
        if lease.prepared_mut(length).is_err() {
            return Err(IoBufferError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer lease does not contain the requested logical range",
                ),
                lease,
            });
        }
        Ok(Self { lease, length })
    }

    pub(crate) fn for_read(lease: BufferLease, length: usize) -> Result<Self, IoBufferError> {
        if length > u32::MAX as usize {
            return Err(IoBufferError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "I/O buffer length exceeds the per-request limit",
                ),
                lease,
            });
        }
        if !lease.has_capacity(length) {
            return Err(IoBufferError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer lease does not contain the requested logical range",
                ),
                lease,
            });
        }
        Ok(Self { lease, length })
    }

    pub(crate) const fn len(&self) -> usize {
        self.length
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    pub(crate) const fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub(crate) fn as_slice(&self) -> io::Result<&[u8]> {
        self.lease.prepared(self.length).map_err(|()| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer lease lost its prepared range",
            )
        })
    }

    fn read_target(&self) -> io::Result<*mut u8> {
        self.lease.read_target(self.length).map_err(|()| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer lease lost its read target range",
            )
        })
    }

    fn mark_initialized(&mut self, length: usize) -> io::Result<()> {
        self.lease.mark_initialized(length).map_err(|()| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "I/O completion exceeds the owned buffer",
            )
        })
    }

    pub(crate) fn into_lease(self) -> BufferLease {
        self.lease
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    fn as_ptr(&self) -> io::Result<*const u8> {
        Ok(self.as_slice()?.as_ptr())
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    fn as_mut_ptr(&mut self) -> io::Result<*mut u8> {
        self.read_target()
    }
}

impl fmt::Debug for IoBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IoBuffer")
            .field("length", &self.length)
            .field("alignment", &IO_BUFFER_ALIGNMENT)
            .finish_non_exhaustive()
    }
}

pub(crate) struct IoBufferError {
    pub(crate) error: io::Error,
    pub(crate) lease: BufferLease,
}

impl fmt::Debug for IoBufferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IoBufferError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for IoBufferError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for IoBufferError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct RequestId(u64);

impl RequestId {
    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    pub(crate) const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperationKind {
    Read,
    Write,
    Flush,
}

impl OperationKind {
    const fn uses_write_slot(self) -> bool {
        matches!(self, Self::Write | Self::Flush)
    }
}

/// An operation owns its buffer from slot reservation until target completion.
pub(crate) enum IoOperation {
    Read {
        buffer: IoBuffer,
        offset: u64,
    },
    Write {
        point: WritePoint,
        buffer: IoBuffer,
        offset: u64,
    },
    Flush {
        point: SyncPoint,
        mode: SyncMode,
    },
}

impl IoOperation {
    pub(crate) fn read(buffer: IoBuffer, offset: u64) -> Self {
        Self::Read { buffer, offset }
    }

    pub(crate) fn write(point: WritePoint, buffer: IoBuffer, offset: u64) -> Self {
        Self::Write {
            point,
            buffer,
            offset,
        }
    }

    pub(crate) const fn flush(point: SyncPoint, mode: SyncMode) -> Self {
        Self::Flush { point, mode }
    }

    pub(crate) const fn kind(&self) -> OperationKind {
        match self {
            Self::Read { .. } => OperationKind::Read,
            Self::Write { .. } => OperationKind::Write,
            Self::Flush { .. } => OperationKind::Flush,
        }
    }

    fn validate(&self) -> io::Result<()> {
        let (length, offset) = match self {
            Self::Read { buffer, offset } | Self::Write { buffer, offset, .. } => {
                (buffer.len(), *offset)
            }
            Self::Flush { .. } => return Ok(()),
        };
        if length > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "I/O length exceeds u32::MAX",
            ));
        }
        offset
            .checked_add(length as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "I/O offset overflow"))?;
        Ok(())
    }

    fn into_buffer(self) -> Option<IoBuffer> {
        match self {
            Self::Read { buffer, .. } | Self::Write { buffer, .. } => Some(buffer),
            Self::Flush { .. } => None,
        }
    }

    fn mark_completed_read(&mut self, bytes_transferred: usize) -> io::Result<()> {
        match self {
            Self::Read { buffer, .. } if bytes_transferred == buffer.len() => {
                buffer.mark_initialized(bytes_transferred)
            }
            Self::Read { .. } => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "completed read did not initialize its complete buffer",
            )),
            Self::Write { .. } | Self::Flush { .. } => Ok(()),
        }
    }

    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    fn runtime_io_path(
        &self,
        files: &RuntimeFileSet,
        transferred: usize,
    ) -> io::Result<RuntimeIoPath> {
        match self {
            Self::Read { buffer, offset } => {
                let remaining = buffer.len().checked_sub(transferred).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "read progress exceeds buffer")
                })?;
                let offset = offset.checked_add(transferred as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "read offset overflow")
                })?;
                Ok(files.select_path(
                    buffer.read_target()?.wrapping_add(transferred),
                    remaining,
                    offset,
                    true,
                ))
            }
            Self::Write {
                point,
                buffer,
                offset,
            } => {
                let remaining = buffer.len().checked_sub(transferred).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "write progress exceeds buffer")
                })?;
                let offset = offset.checked_add(transferred as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "write offset overflow")
                })?;
                Ok(files.select_path(
                    buffer.as_ptr()?.wrapping_add(transferred),
                    remaining,
                    offset,
                    *point == WritePoint::Record,
                ))
            }
            // Metadata and durability controls remain on the buffered flock
            // descriptor. fsync on either descriptor covers the same inode.
            Self::Flush { .. } => Ok(RuntimeIoPath::Buffered),
        }
    }
}

impl fmt::Debug for IoOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read { buffer, offset } => formatter
                .debug_struct("Read")
                .field("length", &buffer.len())
                .field("offset", offset)
                .finish(),
            Self::Write {
                point,
                buffer,
                offset,
            } => formatter
                .debug_struct("Write")
                .field("point", point)
                .field("length", &buffer.len())
                .field("offset", offset)
                .finish(),
            Self::Flush { point, mode } => formatter
                .debug_struct("Flush")
                .field("point", point)
                .field("mode", mode)
                .finish(),
        }
    }
}

#[derive(Debug)]
pub(crate) enum CompletionStatus {
    Completed,
    Cancelled,
    Failed(io::Error),
}

impl CompletionStatus {
    pub(crate) fn into_io_result(self, bytes_transferred: usize) -> io::Result<usize> {
        match self {
            Self::Completed => Ok(bytes_transferred),
            Self::Cancelled => Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "I/O request was cancelled",
            )),
            Self::Failed(error) => Err(error),
        }
    }
}

#[derive(Debug)]
pub(crate) struct IoCompletion {
    pub(crate) request_id: RequestId,
    pub(crate) kind: OperationKind,
    pub(crate) status: CompletionStatus,
    pub(crate) bytes_transferred: usize,
    pub(crate) buffer: Option<IoBuffer>,
}

impl IoCompletion {
    /// Split the result without ever discarding the owned buffer on failure.
    pub(crate) fn into_io_result(self) -> (io::Result<usize>, Option<IoBuffer>) {
        (
            self.status.into_io_result(self.bytes_transferred),
            self.buffer,
        )
    }

    pub(crate) fn into_lease(self) -> (io::Result<usize>, Option<BufferLease>) {
        let (result, buffer) = self.into_io_result();
        (result, buffer.map(IoBuffer::into_lease))
    }
}

#[derive(Debug)]
pub(crate) struct SubmitError {
    pub(crate) error: io::Error,
    pub(crate) operation: IoOperation,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(formatter)
    }
}

impl std::error::Error for SubmitError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl SubmitError {
    pub(crate) fn into_parts(self) -> (io::Error, IoOperation) {
        (self.error, self.operation)
    }

    pub(crate) fn into_lease(self) -> (io::Error, Option<BufferLease>) {
        let (error, buffer) = self.into_buffer();
        (error, buffer.map(IoBuffer::into_lease))
    }

    pub(crate) fn into_buffer(self) -> (io::Error, Option<IoBuffer>) {
        let (error, operation) = self.into_parts();
        (error, operation.into_buffer())
    }
}

struct CompletionCell {
    completion: Option<IoCompletion>,
    consumer_alive: bool,
}

pub(crate) struct CompletionState {
    cell: Mutex<CompletionCell>,
    ready: Condvar,
    cancel_requested: AtomicBool,
    cancel_notified: AtomicBool,
}

impl CompletionState {
    fn new() -> Self {
        Self {
            cell: Mutex::new(CompletionCell {
                completion: None,
                consumer_alive: true,
            }),
            ready: Condvar::new(),
            cancel_requested: AtomicBool::new(false),
            cancel_notified: AtomicBool::new(false),
        }
    }

    fn complete(&self, completion: IoCompletion) {
        let mut cell = lock_unpoisoned(&self.cell);
        if !cell.consumer_alive {
            drop(cell);
            drop(completion);
            return;
        }
        debug_assert!(cell.completion.is_none());
        cell.completion = Some(completion);
        self.ready.notify_all();
    }

    #[cfg(test)]
    fn wait(&self) -> IoCompletion {
        let mut cell = lock_unpoisoned(&self.cell);
        loop {
            if let Some(completion) = cell.completion.take() {
                return completion;
            }
            cell = self
                .ready
                .wait(cell)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn wait_until(&self, deadline: Instant) -> Option<IoCompletion> {
        let mut cell = lock_unpoisoned(&self.cell);
        loop {
            if let Some(completion) = cell.completion.take() {
                return Some(completion);
            }
            let remaining = deadline.checked_duration_since(Instant::now())?;
            let (next, timeout) = self
                .ready
                .wait_timeout(cell, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            cell = next;
            if timeout.timed_out() && cell.completion.is_none() {
                return None;
            }
        }
    }

    fn detach(&self) {
        let completion = {
            let mut cell = lock_unpoisoned(&self.cell);
            cell.consumer_alive = false;
            cell.completion.take()
        };
        drop(completion);
    }
}

/// A single-consumer completion. Dropping it detaches the consumer; it does not
/// cancel the operation or release an in-flight buffer.
pub(crate) struct IoRequest {
    request_id: RequestId,
    completion: Arc<CompletionState>,
    finished: bool,
}

impl fmt::Debug for IoRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IoRequest")
            .field("request_id", &self.request_id)
            .field("finished", &self.finished)
            .finish_non_exhaustive()
    }
}

impl IoRequest {
    pub(crate) const fn id(&self) -> RequestId {
        self.request_id
    }

    #[cfg(test)]
    pub(crate) fn wait(mut self) -> IoCompletion {
        let completion = self.completion.wait();
        self.finished = true;
        completion
    }

    /// Wait through an absolute deadline without detaching the consumer. On
    /// timeout ownership of the request is returned so the caller can request
    /// cancellation and continue waiting for the target lifetime fence.
    pub(crate) fn wait_until(mut self, deadline: Instant) -> Result<IoCompletion, Self> {
        match self.completion.wait_until(deadline) {
            Some(completion) => {
                self.finished = true;
                Ok(completion)
            }
            None => Err(self),
        }
    }
}

impl Drop for IoRequest {
    fn drop(&mut self) {
        if !self.finished {
            self.completion.detach();
        }
    }
}

/// One cache I/O carrying the same absolute deadline through slot reservation
/// and target completion.
pub(crate) struct BoundedIoRequest {
    request: IoRequest,
    deadline: Instant,
    cancel_grace: Duration,
}

impl BoundedIoRequest {
    pub(crate) const fn id(&self) -> RequestId {
        self.request.id()
    }

    pub(crate) fn wait(self, engine: &dyn IoEngine) -> Result<IoCompletion, IoDeadlineExceeded> {
        let request = match self.request.wait_until(self.deadline) {
            Ok(completion) => return Ok(completion),
            Err(request) => request,
        };
        let cancel_error = engine
            .cancel(request.id(), request.completion.as_ref())
            .err();
        let grace_deadline = Instant::now()
            .checked_add(self.cancel_grace)
            .unwrap_or_else(Instant::now);
        match request.wait_until(grace_deadline) {
            Ok(completion) => Err(IoDeadlineExceeded::new(cancel_error, Some(completion))),
            Err(request) => {
                // Dropping the consumer detaches it but deliberately leaves the
                // operation and owned buffer in the driver. Stop all further
                // requests; close will inspect the exact in-flight write
                // count before deciding whether to retain flock.
                engine.stop_accepting_requests();
                drop(request);
                Err(IoDeadlineExceeded::new(cancel_error, None))
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct IoDeadlineExceeded {
    error: io::Error,
    completion: Option<IoCompletion>,
}

impl IoDeadlineExceeded {
    fn new(cancel_error: Option<io::Error>, completion: Option<IoCompletion>) -> Self {
        let message = cancel_error.map_or_else(
            || "cache I/O completion deadline expired".to_owned(),
            |error| format!("cache I/O completion deadline expired; cancellation failed: {error}"),
        );
        Self {
            error: io::Error::new(io::ErrorKind::TimedOut, message),
            completion,
        }
    }

    pub(crate) fn into_buffer(self) -> (io::Error, Option<IoBuffer>) {
        let buffer = self
            .completion
            .and_then(|completion| completion.into_io_result().1);
        (self.error, buffer)
    }

    pub(crate) fn into_lease(self) -> (io::Error, Option<BufferLease>) {
        let (error, buffer) = self.into_buffer();
        (error, buffer.map(IoBuffer::into_lease))
    }
}

/// Submit one cache-device request with a hard end-to-end deadline.
pub(crate) fn submit_cache_io(
    engine: &dyn IoEngine,
    operation: IoOperation,
) -> Result<BoundedIoRequest, SubmitError> {
    let deadline = Instant::now()
        .checked_add(CACHE_IO_COMPLETION_TIMEOUT)
        .unwrap_or_else(Instant::now);
    submit_cache_io_until(engine, operation, deadline, CACHE_IO_CANCEL_GRACE)
}

/// Submits a read whose engine slot was reserved before allocating its buffer.
pub(crate) fn submit_cache_read(
    engine: &dyn IoEngine,
    slot: ReadSlot,
    operation: IoOperation,
) -> Result<BoundedIoRequest, SubmitError> {
    let deadline = Instant::now()
        .checked_add(CACHE_IO_COMPLETION_TIMEOUT)
        .unwrap_or_else(Instant::now);
    let request = engine.submit_reserved_read(slot, operation)?;
    Ok(BoundedIoRequest {
        request,
        deadline,
        cancel_grace: CACHE_IO_CANCEL_GRACE,
    })
}

fn submit_cache_io_until(
    engine: &dyn IoEngine,
    operation: IoOperation,
    deadline: Instant,
    cancel_grace: Duration,
) -> Result<BoundedIoRequest, SubmitError> {
    let cancelled = AtomicBool::new(false);
    let request = engine.submit_wait_controlled(operation, &cancelled, Some(deadline))?;
    Ok(BoundedIoRequest {
        request,
        deadline,
        cancel_grace,
    })
}

pub(crate) trait IoEngine: Send + Sync {
    fn try_reserve_read(&self) -> io::Result<ReadSlot>;
    fn submit_reserved_read(
        &self,
        slot: ReadSlot,
        operation: IoOperation,
    ) -> Result<IoRequest, SubmitError>;
    #[cfg(test)]
    fn submit_nowait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError>;
    #[cfg(test)]
    fn submit(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
        self.submit_nowait(operation)
    }
    #[cfg(test)]
    fn submit_wait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError>;
    fn submit_wait_controlled(
        &self,
        operation: IoOperation,
        cancelled: &AtomicBool,
        deadline: Option<Instant>,
    ) -> Result<IoRequest, SubmitError>;
    fn wake_slot_waiters(&self);
    fn cancel(&self, request_id: RequestId, state: &CompletionState) -> io::Result<bool>;
    fn shutdown(&self) -> io::Result<()>;
    fn in_flight(&self) -> usize;
    #[cfg(test)]
    fn direct_active(&self) -> bool;
    /// Permanently stop accepting requests after a target operation missed both its
    /// deadline and cancellation grace period.
    fn stop_accepting_requests(&self);
    fn writes_in_flight(&self) -> usize;
    /// True means a failed driver could not fence an issued write or flush.
    /// The cache must retain its exclusive file lock for process lifetime.
    fn has_unfenced_writes(&self) -> bool;
    #[cfg(test)]
    fn mark_unfenced_writes_for_test(&self);
    fn stats(&self) -> CacheIoSnapshot;

    #[cfg(test)]
    fn read_exact_at(&self, buffer: IoBuffer, offset: u64) -> Result<IoRequest, SubmitError> {
        self.submit(IoOperation::read(buffer, offset))
    }

    #[cfg(test)]
    fn write_all_at(
        &self,
        point: WritePoint,
        buffer: IoBuffer,
        offset: u64,
    ) -> Result<IoRequest, SubmitError> {
        self.submit(IoOperation::write(point, buffer, offset))
    }

    #[cfg(test)]
    fn flush(&self, point: SyncPoint, mode: SyncMode) -> Result<IoRequest, SubmitError> {
        self.submit(IoOperation::flush(point, mode))
    }
}

struct IoSlot {
    shared: Arc<RuntimeShared>,
    write: bool,
}

/// One non-waiting read reservation against the engine's complete depth.
/// Dropping it before submission releases the slot immediately.
pub(crate) struct ReadSlot {
    slot: IoSlot,
}

impl Drop for IoSlot {
    fn drop(&mut self) {
        let _slot = lock_unpoisoned(&self.shared.slot_lock);
        self.shared
            .slot_state
            .fetch_sub(slot_delta(self.write, false), Ordering::AcqRel);
        self.shared.slot_available.notify_one();
    }
}

struct WriteSlotWaiter<'a> {
    shared: &'a RuntimeShared,
}

impl<'a> WriteSlotWaiter<'a> {
    fn new(shared: &'a RuntimeShared) -> Self {
        shared
            .slot_state
            .fetch_add(slot_delta(false, true), Ordering::AcqRel);
        Self { shared }
    }
}

impl Drop for WriteSlotWaiter<'_> {
    fn drop(&mut self) {
        self.shared
            .slot_state
            .fetch_sub(slot_delta(false, true), Ordering::AcqRel);
        self.shared.slot_available.notify_all();
    }
}

const SLOT_COUNT_BITS: u32 = 16;
const SLOT_COUNT_MASK: u64 = (1_u64 << SLOT_COUNT_BITS) - 1;
const WRITE_SLOT_SHIFT: u32 = SLOT_COUNT_BITS;
const WRITE_WAITER_SHIFT: u32 = 2 * SLOT_COUNT_BITS;

const fn slot_delta(write: bool, waiter: bool) -> u64 {
    let total = if waiter { 0 } else { 1 };
    total + ((write as u64) << WRITE_SLOT_SHIFT) + ((waiter as u64) << WRITE_WAITER_SHIFT)
}

const fn active_slots(state: u64) -> usize {
    (state & SLOT_COUNT_MASK) as usize
}

const fn active_write_slots(state: u64) -> usize {
    ((state >> WRITE_SLOT_SHIFT) & SLOT_COUNT_MASK) as usize
}

const fn waiting_writes(state: u64) -> usize {
    (state >> WRITE_WAITER_SHIFT) as usize
}

struct RuntimeShared {
    max_in_flight: usize,
    statistics_enabled: bool,
    accepting: AtomicBool,
    /// Packed total, write, and waiting-write counts. A single CAS is the slot
    /// reservation linearization point, including read-to-write handoff.
    slot_state: AtomicU64,
    in_flight_peak: AtomicUsize,
    submitted: AtomicU64,
    completed: AtomicU64,
    cancel_requested: AtomicU64,
    cancelled: AtomicU64,
    errors: AtomicU64,
    slot_wait_ns: AtomicU64,
    completion_ns: AtomicU64,
    cancel_scan_needed: AtomicBool,
    unfenced_writes: AtomicBool,
    slot_lock: Mutex<()>,
    slot_available: Condvar,
}

enum SlotWaitError {
    Shutdown,
    Cancelled,
    TimedOut,
}

impl RuntimeShared {
    fn new(max_in_flight: usize, statistics_enabled: bool) -> Self {
        Self {
            max_in_flight,
            statistics_enabled,
            accepting: AtomicBool::new(true),
            slot_state: AtomicU64::new(0),
            in_flight_peak: AtomicUsize::new(0),
            submitted: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            cancel_requested: AtomicU64::new(0),
            cancelled: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            slot_wait_ns: AtomicU64::new(0),
            completion_ns: AtomicU64::new(0),
            cancel_scan_needed: AtomicBool::new(false),
            unfenced_writes: AtomicBool::new(false),
            slot_lock: Mutex::new(()),
            slot_available: Condvar::new(),
        }
    }

    fn total_in_flight(&self) -> usize {
        active_slots(self.slot_state.load(Ordering::Acquire))
    }

    fn writes_in_flight(&self) -> usize {
        active_write_slots(self.slot_state.load(Ordering::Acquire))
    }

    #[cfg(test)]
    fn waiting_writes_for_test(&self) -> usize {
        waiting_writes(self.slot_state.load(Ordering::Acquire))
    }

    fn try_reserve_slot(self: &Arc<Self>, write: bool) -> Option<IoSlot> {
        const MAX_SLOT_CAS_ATTEMPTS: usize = 8;
        let write_limit = if self.max_in_flight > 1 {
            self.max_in_flight - 1
        } else {
            self.max_in_flight
        };
        // Reads may use the complete engine depth. Once a write is waiting,
        // keep one total slot available for its handoff so a read stream cannot
        // starve accepted write-back or an explicit drain barrier.
        let mut current = self.slot_state.load(Ordering::Acquire);
        for _ in 0..MAX_SLOT_CAS_ATTEMPTS {
            let total = active_slots(current);
            let writes = active_write_slots(current);
            if write && writes >= write_limit {
                return None;
            }
            let reserve_for_waiting_write =
                !write && waiting_writes(current) != 0 && writes < write_limit;
            let slot_limit = if reserve_for_waiting_write {
                self.max_in_flight.saturating_sub(1)
            } else {
                self.max_in_flight
            };
            if total >= slot_limit {
                return None;
            }
            let next = current + slot_delta(write, false);
            match self.slot_state.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if self.statistics_enabled {
                        update_peak(&self.in_flight_peak, total + 1);
                    }
                    return Some(IoSlot {
                        shared: Arc::clone(self),
                        write,
                    });
                }
                Err(observed) => current = observed,
            }
        }
        None
    }

    #[cfg(test)]
    fn reserve_slot_wait(self: &Arc<Self>, write: bool) -> Option<IoSlot> {
        let mut slot = lock_unpoisoned(&self.slot_lock);
        let _write_waiter = write.then(|| WriteSlotWaiter::new(self));
        loop {
            if !self.accepting.load(Ordering::Acquire) {
                return None;
            }
            if let Some(reserved) = self.try_reserve_slot(write) {
                return Some(reserved);
            }
            slot = self
                .slot_available
                .wait(slot)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn reserve_slot_controlled(
        self: &Arc<Self>,
        write: bool,
        cancelled: &AtomicBool,
        deadline: Option<Instant>,
    ) -> Result<IoSlot, SlotWaitError> {
        let mut slot = lock_unpoisoned(&self.slot_lock);
        let _write_waiter = write.then(|| WriteSlotWaiter::new(self));
        loop {
            if !self.accepting.load(Ordering::Acquire) {
                return Err(SlotWaitError::Shutdown);
            }
            if cancelled.load(Ordering::Acquire) {
                self.slot_available.notify_all();
                return Err(SlotWaitError::Cancelled);
            }
            let remaining =
                deadline.map(|deadline| deadline.checked_duration_since(Instant::now()));
            if matches!(remaining, Some(None)) {
                self.slot_available.notify_all();
                return Err(SlotWaitError::TimedOut);
            }
            if let Some(reserved) = self.try_reserve_slot(write) {
                return Ok(reserved);
            }
            slot = match remaining {
                Some(Some(remaining)) => {
                    self.slot_available
                        .wait_timeout(slot, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .0
                }
                Some(None) => unreachable!("expired deadline returned above"),
                None => self
                    .slot_available
                    .wait(slot)
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
            };
        }
    }

    /// Wake controlled waiters after publishing their cancellation flag.
    /// Taking the same mutex used around the check-and-wait transition makes
    /// `cancelled.store(true, Release); wake_slot_waiters()` lossless.
    fn wake_slot_waiters(&self) {
        let _slot = lock_unpoisoned(&self.slot_lock);
        self.slot_available.notify_all();
    }

    fn has_unfenced_writes(&self) -> bool {
        self.unfenced_writes.load(Ordering::Acquire)
    }

    #[cfg(any(
        test,
        all(
            feature = "io-uring",
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            )
        )
    ))]
    fn mark_unfenced_writes(&self) {
        self.unfenced_writes.store(true, Ordering::Release);
    }

    fn finish(&self, task: Task, mut status: CompletionStatus, bytes_transferred: usize) {
        let Task {
            request_id,
            mut operation,
            completion,
            slot,
            submitted_at,
        } = task;
        let kind = operation.kind();
        if matches!(status, CompletionStatus::Completed)
            && let Err(error) = operation.mark_completed_read(bytes_transferred)
        {
            status = CompletionStatus::Failed(error);
        }
        let buffer = operation.into_buffer();
        self.publish_completion(
            request_id,
            kind,
            status,
            bytes_transferred,
            buffer,
            completion,
            slot,
            submitted_at,
        );
    }

    /// Complete a request without releasing a buffer whose kernel lifetime
    /// could not be fenced by its target CQE. Leaking at most the engine's
    /// bounded in-flight limit is preferable to returning an allocation that the
    /// kernel might still access to a reusable pool.
    #[cfg(any(
        test,
        all(
            feature = "io-uring",
            target_os = "linux",
            any(
                target_arch = "x86_64",
                target_arch = "aarch64",
                target_arch = "riscv64",
                target_arch = "loongarch64",
                target_arch = "powerpc64"
            )
        )
    ))]
    fn finish_quarantined(&self, task: Task, status: CompletionStatus, bytes_transferred: usize) {
        let Task {
            request_id,
            operation,
            completion,
            slot,
            submitted_at,
        } = task;
        let kind = operation.kind();
        if let Some(buffer) = operation.into_buffer() {
            std::mem::forget(buffer);
        }
        self.publish_completion(
            request_id,
            kind,
            status,
            bytes_transferred,
            None,
            completion,
            slot,
            submitted_at,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn publish_completion(
        &self,
        request_id: RequestId,
        kind: OperationKind,
        status: CompletionStatus,
        bytes_transferred: usize,
        buffer: Option<IoBuffer>,
        completion: Arc<CompletionState>,
        slot: IoSlot,
        submitted_at: Option<Instant>,
    ) {
        if self.statistics_enabled {
            self.completed.fetch_add(1, Ordering::Relaxed);
            match &status {
                CompletionStatus::Completed => {}
                CompletionStatus::Cancelled => {
                    self.cancelled.fetch_add(1, Ordering::Relaxed);
                }
                CompletionStatus::Failed(_) => {
                    self.errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some(submitted_at) = submitted_at {
                add_duration_ns(&self.completion_ns, submitted_at.elapsed());
            }
        }
        drop(slot);
        completion.complete(IoCompletion {
            request_id,
            kind,
            status,
            bytes_transferred,
            buffer,
        });
    }

    fn snapshot(&self) -> CacheIoSnapshot {
        let in_flight = self.total_in_flight();
        CacheIoSnapshot {
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            cancel_requested: self.cancel_requested.load(Ordering::Relaxed),
            cancelled: self.cancelled.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            requests_in_flight: usize_to_u64(in_flight),
            requests_in_flight_peak: usize_to_u64(
                self.in_flight_peak.load(Ordering::Relaxed).max(in_flight),
            ),
            slot_wait_ns: self.slot_wait_ns.load(Ordering::Relaxed),
            completion_ns: self.completion_ns.load(Ordering::Relaxed),
            direct_operations: 0,
            direct_bytes: 0,
            buffered_operations: 0,
            buffered_bytes: 0,
        }
    }
}

struct Task {
    request_id: RequestId,
    operation: IoOperation,
    completion: Arc<CompletionState>,
    slot: IoSlot,
    submitted_at: Option<Instant>,
}

enum DriverCommand {
    Submit(Task),
    Cancel(RequestId),
    Shutdown,
}

trait DriverWake: Send + Sync {
    fn wake(&self);
}

struct SubmitState {
    accepting: bool,
}

enum ShutdownPhase {
    Running,
    Draining,
    Stopped(Option<StoredIoError>),
}

#[derive(Clone)]
struct StoredIoError {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: String,
}

impl StoredIoError {
    fn capture(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: error.to_string(),
        }
    }

    fn restore(&self) -> io::Error {
        self.raw_os_error.map_or_else(
            || io::Error::new(self.kind, self.message.clone()),
            io::Error::from_raw_os_error,
        )
    }
}

struct ShutdownState {
    phase: Mutex<ShutdownPhase>,
    stopped: Condvar,
}

struct RuntimeInner {
    shared: Arc<RuntimeShared>,
    commands: SyncSender<DriverCommand>,
    submit_state: Arc<RwLock<SubmitState>>,
    next_request_id: AtomicU64,
    wake: Option<Arc<dyn DriverWake>>,
    workers: Mutex<Vec<JoinHandle<io::Result<()>>>>,
    shutdown: ShutdownState,
}

#[derive(Clone, Copy)]
enum SlotMode<'a> {
    #[cfg(test)]
    Try,
    #[cfg(test)]
    Wait,
    Controlled {
        cancelled: &'a AtomicBool,
        deadline: Option<Instant>,
    },
}

impl RuntimeInner {
    fn validate_max_in_flight(max_in_flight: usize) -> io::Result<()> {
        if !(1..=MAX_IO_REQUESTS_PER_WORKER).contains(&max_in_flight) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("I/O requests per worker must be in 1..={MAX_IO_REQUESTS_PER_WORKER}"),
            ));
        }
        Ok(())
    }

    fn next_request_id(&self) -> RequestId {
        // Request IDs never use the top two bits; the uring driver reserves
        // those bits for internal/cancel CQEs. Wrapping is practically
        // unreachable, but skipping zero preserves the invariant.
        const MAX_REQUEST_ID: u64 = (1_u64 << 62) - 1;
        let observed = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let id = observed & MAX_REQUEST_ID;
        if id != 0 {
            return RequestId(id);
        }
        let next = self.next_request_id.fetch_add(1, Ordering::Relaxed) & MAX_REQUEST_ID;
        debug_assert_ne!(next, 0);
        RequestId(next)
    }

    #[cfg(test)]
    fn submit_nowait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
        self.submit_inner(operation, SlotMode::Try)
    }

    fn try_reserve_read(&self) -> io::Result<ReadSlot> {
        if !self.shared.accepting.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "I/O engine is shut down",
            ));
        }
        self.shared
            .try_reserve_slot(false)
            .map(|slot| ReadSlot { slot })
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "no I/O slot is available"))
    }

    fn submit_reserved_read(
        &self,
        slot: ReadSlot,
        operation: IoOperation,
    ) -> Result<IoRequest, SubmitError> {
        let submit_started = self.shared.statistics_enabled.then(Instant::now);
        if operation.kind() != OperationKind::Read || !Arc::ptr_eq(&slot.slot.shared, &self.shared)
        {
            return Err(SubmitError {
                error: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read slot does not match the submitted engine operation",
                ),
                operation,
            });
        }
        if let Err(error) = operation.validate() {
            return Err(SubmitError { error, operation });
        }
        self.submit_with_slot(operation, slot.slot, submit_started, true)
    }

    #[cfg(test)]
    fn submit_wait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
        self.submit_inner(operation, SlotMode::Wait)
    }

    fn submit_wait_controlled(
        &self,
        operation: IoOperation,
        cancelled: &AtomicBool,
        deadline: Option<Instant>,
    ) -> Result<IoRequest, SubmitError> {
        self.submit_inner(
            operation,
            SlotMode::Controlled {
                cancelled,
                deadline,
            },
        )
    }

    fn submit_inner(
        &self,
        operation: IoOperation,
        slot_mode: SlotMode<'_>,
    ) -> Result<IoRequest, SubmitError> {
        let submit_started = self.shared.statistics_enabled.then(Instant::now);
        let nonblocking = match slot_mode {
            #[cfg(test)]
            SlotMode::Try => true,
            #[cfg(test)]
            SlotMode::Wait => false,
            SlotMode::Controlled { .. } => false,
        };
        if let Err(error) = operation.validate() {
            return Err(SubmitError { error, operation });
        }
        let write = operation.kind().uses_write_slot();
        let slot = match slot_mode {
            #[cfg(test)]
            SlotMode::Try if !self.shared.accepting.load(Ordering::Acquire) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "I/O engine is shut down",
            )),
            #[cfg(test)]
            SlotMode::Try => self.shared.try_reserve_slot(write).ok_or_else(|| {
                io::Error::new(io::ErrorKind::WouldBlock, "no I/O slot is available")
            }),
            #[cfg(test)]
            SlotMode::Wait => self.shared.reserve_slot_wait(write).ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "I/O engine is shut down")
            }),
            SlotMode::Controlled {
                cancelled,
                deadline,
            } => self
                .shared
                .reserve_slot_controlled(write, cancelled, deadline)
                .map_err(|error| match error {
                    SlotWaitError::Shutdown => {
                        io::Error::new(io::ErrorKind::BrokenPipe, "I/O engine is shut down")
                    }
                    SlotWaitError::Cancelled => {
                        io::Error::new(io::ErrorKind::Interrupted, "I/O slot wait was cancelled")
                    }
                    SlotWaitError::TimedOut => {
                        io::Error::new(io::ErrorKind::TimedOut, "I/O slot deadline expired")
                    }
                }),
        };
        let slot = match slot {
            Ok(slot) => slot,
            Err(error) => {
                return Err(SubmitError { error, operation });
            }
        };

        self.submit_with_slot(operation, slot, submit_started, nonblocking)
    }

    fn submit_with_slot(
        &self,
        operation: IoOperation,
        slot: IoSlot,
        submit_started: Option<Instant>,
        nonblocking: bool,
    ) -> Result<IoRequest, SubmitError> {
        let submit_state = if nonblocking {
            match self.submit_state.try_read() {
                Ok(state) => state,
                Err(TryLockError::WouldBlock) => {
                    drop(slot);
                    return Err(SubmitError {
                        error: io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "I/O submission fence is busy",
                        ),
                        operation,
                    });
                }
                Err(TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            }
        } else {
            self.submit_state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        };
        if !submit_state.accepting {
            drop(slot);
            return Err(SubmitError {
                error: io::Error::new(io::ErrorKind::BrokenPipe, "I/O engine is shut down"),
                operation,
            });
        }
        let request_id = self.next_request_id();
        let completion = Arc::new(CompletionState::new());
        let task = Task {
            request_id,
            operation,
            completion: Arc::clone(&completion),
            slot,
            submitted_at: submit_started,
        };
        if self.shared.statistics_enabled {
            self.shared.submitted.fetch_add(1, Ordering::Release);
        }
        match self.commands.try_send(DriverCommand::Submit(task)) {
            Ok(()) => {
                if let Some(submit_started) = submit_started {
                    add_duration_ns(&self.shared.slot_wait_ns, submit_started.elapsed());
                }
                if let Some(wake) = &self.wake {
                    wake.wake();
                }
                drop(submit_state);
                Ok(IoRequest {
                    request_id,
                    completion,
                    finished: false,
                })
            }
            Err(TrySendError::Full(DriverCommand::Submit(task))) => {
                if self.shared.statistics_enabled {
                    self.shared.submitted.fetch_sub(1, Ordering::Relaxed);
                }
                let Task {
                    operation, slot, ..
                } = task;
                drop(slot);
                Err(SubmitError {
                    error: io::Error::new(io::ErrorKind::WouldBlock, "I/O command queue is full"),
                    operation,
                })
            }
            Err(TrySendError::Disconnected(DriverCommand::Submit(task))) => {
                if self.shared.statistics_enabled {
                    self.shared.submitted.fetch_sub(1, Ordering::Relaxed);
                }
                let Task {
                    operation, slot, ..
                } = task;
                drop(slot);
                Err(SubmitError {
                    error: io::Error::new(io::ErrorKind::BrokenPipe, "I/O driver stopped"),
                    operation,
                })
            }
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                unreachable!("submit sends only submit commands")
            }
        }
    }

    fn cancel(&self, request_id: RequestId, state: &CompletionState) -> io::Result<bool> {
        if !state.cancel_requested.swap(true, Ordering::AcqRel) && self.shared.statistics_enabled {
            self.shared.cancel_requested.fetch_add(1, Ordering::Relaxed);
        }
        if !state.cancel_notified.swap(true, Ordering::AcqRel) {
            match self.commands.try_send(DriverCommand::Cancel(request_id)) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) => {
                    self.shared
                        .cancel_scan_needed
                        .store(true, Ordering::Release);
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "I/O driver stopped",
                    ));
                }
            }
        }
        if let Some(wake) = &self.wake {
            wake.wake();
        }
        Ok(true)
    }

    fn stop_accepting_requests(&self) {
        let mut submit_state = self
            .submit_state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        submit_state.accepting = false;
        {
            let _slot = lock_unpoisoned(&self.shared.slot_lock);
            self.shared.accepting.store(false, Ordering::Release);
            self.shared.slot_available.notify_all();
        }
        drop(submit_state);
        if let Some(wake) = &self.wake {
            wake.wake();
        }
    }

    fn shutdown(&self) -> io::Result<()> {
        let leader = {
            let mut phase = lock_unpoisoned(&self.shutdown.phase);
            loop {
                match &*phase {
                    ShutdownPhase::Running => {
                        *phase = ShutdownPhase::Draining;
                        break true;
                    }
                    ShutdownPhase::Draining => {
                        phase = self
                            .shutdown
                            .stopped
                            .wait(phase)
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                    }
                    ShutdownPhase::Stopped(error) => {
                        return error.as_ref().map_or(Ok(()), |error| Err(error.restore()));
                    }
                }
            }
        };
        debug_assert!(leader);

        let worker_count = lock_unpoisoned(&self.workers).len();
        let send_error = {
            let mut submit_state = self
                .submit_state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            submit_state.accepting = false;
            {
                let _slot = lock_unpoisoned(&self.shared.slot_lock);
                self.shared.accepting.store(false, Ordering::Release);
                self.shared.slot_available.notify_all();
            }
            let mut error = None;
            for _ in 0..worker_count {
                if self.commands.send(DriverCommand::Shutdown).is_err() {
                    error = Some(io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "I/O driver stopped before shutdown",
                    ));
                    break;
                }
            }
            error
        };
        if let Some(wake) = &self.wake {
            wake.wake();
        }
        let mut worker_result = Ok(());
        for worker in lock_unpoisoned(&self.workers).drain(..) {
            let result = worker
                .join()
                .unwrap_or_else(|_| Err(io::Error::other("I/O driver panicked")));
            if worker_result.is_ok() {
                worker_result = result;
            }
        }
        let result = send_error.map_or(worker_result, Err);
        let stored = result.as_ref().err().map(StoredIoError::capture);
        let mut phase = lock_unpoisoned(&self.shutdown.phase);
        *phase = ShutdownPhase::Stopped(stored);
        self.shutdown.stopped.notify_all();
        result
    }
}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Reference engine: a small fixed worker pool executes exact operations
/// through the existing fault-injectable positioned-I/O backend.
#[derive(Clone)]
pub(crate) struct BackendIoEngine {
    inner: Arc<RuntimeInner>,
    backend: Arc<dyn IoBackend>,
}

impl BackendIoEngine {
    #[cfg(unix)]
    #[cfg(test)]
    pub(crate) fn new_with_files(files: RuntimeFileSet, max_in_flight: usize) -> io::Result<Self> {
        let backend: Arc<dyn IoBackend> = Arc::new(RuntimeFileBackend::new(files));
        Self::new(backend, max_in_flight)
    }

    #[cfg(unix)]
    pub(crate) fn new_with_files_and_workers(
        files: RuntimeFileSet,
        max_in_flight: usize,
        worker_count: usize,
        statistics_enabled: bool,
    ) -> io::Result<Self> {
        files.set_statistics_enabled(statistics_enabled);
        let backend: Arc<dyn IoBackend> = Arc::new(RuntimeFileBackend::new(files));
        Self::new_with_workers_and_statistics(
            backend,
            max_in_flight,
            worker_count,
            statistics_enabled,
        )
    }

    #[cfg(test)]
    pub(crate) fn new(backend: Arc<dyn IoBackend>, max_in_flight: usize) -> io::Result<Self> {
        Self::new_with_workers(backend, max_in_flight, max_in_flight.min(4))
    }

    #[cfg(test)]
    pub(crate) fn new_with_workers(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
        worker_count: usize,
    ) -> io::Result<Self> {
        Self::new_with_workers_and_statistics(backend, max_in_flight, worker_count, true)
    }

    fn new_with_workers_and_statistics(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
        worker_count: usize,
        statistics_enabled: bool,
    ) -> io::Result<Self> {
        RuntimeInner::validate_max_in_flight(max_in_flight)?;
        if worker_count == 0 || worker_count > max_in_flight {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sync I/O worker count must not exceed the request limit",
            ));
        }
        let shared = Arc::new(RuntimeShared::new(max_in_flight, statistics_enabled));
        let command_capacity = max_in_flight
            .checked_mul(2)
            .and_then(|depth| depth.checked_add(1))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "queue size overflow"))?;
        let (commands, receiver) = mpsc::sync_channel(command_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut workers = Vec::with_capacity(worker_count);
        for worker_index in 0..worker_count {
            let worker_backend = Arc::clone(&backend);
            let worker_shared = Arc::clone(&shared);
            let worker_receiver = Arc::clone(&receiver);
            let spawn_result = std::thread::Builder::new()
                .name(format!("cache-rs-sync-io-{worker_index}"))
                .stack_size(CACHE_THREAD_STACK_BYTES)
                .spawn(move || backend_driver(worker_backend, worker_shared, worker_receiver));
            match spawn_result {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    for _ in 0..workers.len() {
                        let _ = commands.send(DriverCommand::Shutdown);
                    }
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            inner: Arc::new(RuntimeInner {
                shared,
                commands,
                submit_state: Arc::new(RwLock::new(SubmitState { accepting: true })),
                next_request_id: AtomicU64::new(1),
                wake: None,
                workers: Mutex::new(workers),
                shutdown: ShutdownState {
                    phase: Mutex::new(ShutdownPhase::Running),
                    stopped: Condvar::new(),
                },
            }),
            backend,
        })
    }
}

impl IoEngine for BackendIoEngine {
    fn try_reserve_read(&self) -> io::Result<ReadSlot> {
        self.inner.try_reserve_read()
    }

    fn submit_reserved_read(
        &self,
        slot: ReadSlot,
        operation: IoOperation,
    ) -> Result<IoRequest, SubmitError> {
        self.inner.submit_reserved_read(slot, operation)
    }

    #[cfg(test)]
    fn submit_nowait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
        self.inner.submit_nowait(operation)
    }

    #[cfg(test)]
    fn submit_wait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
        self.inner.submit_wait(operation)
    }

    fn submit_wait_controlled(
        &self,
        operation: IoOperation,
        cancelled: &AtomicBool,
        deadline: Option<Instant>,
    ) -> Result<IoRequest, SubmitError> {
        self.inner
            .submit_wait_controlled(operation, cancelled, deadline)
    }

    fn wake_slot_waiters(&self) {
        self.inner.shared.wake_slot_waiters();
    }

    fn cancel(&self, request_id: RequestId, state: &CompletionState) -> io::Result<bool> {
        self.inner.cancel(request_id, state)
    }

    fn shutdown(&self) -> io::Result<()> {
        self.inner.shutdown()
    }

    fn in_flight(&self) -> usize {
        self.inner.shared.total_in_flight()
    }

    #[cfg(test)]
    fn direct_active(&self) -> bool {
        self.backend.direct_io_stats().direct_active
    }

    fn stop_accepting_requests(&self) {
        self.inner.stop_accepting_requests();
    }

    fn writes_in_flight(&self) -> usize {
        self.inner.shared.writes_in_flight()
    }

    fn has_unfenced_writes(&self) -> bool {
        self.inner.shared.has_unfenced_writes()
    }

    #[cfg(test)]
    fn mark_unfenced_writes_for_test(&self) {
        self.inner.shared.mark_unfenced_writes();
    }

    fn stats(&self) -> CacheIoSnapshot {
        let mut stats = self.inner.shared.snapshot();
        let direct = self.backend.direct_io_stats();
        stats.direct_operations = direct.direct_operations;
        stats.direct_bytes = direct.direct_bytes;
        stats.buffered_operations = direct.buffered_operations;
        stats.buffered_bytes = direct.buffered_bytes;
        stats
    }
}

fn backend_driver(
    backend: Arc<dyn IoBackend>,
    shared: Arc<RuntimeShared>,
    receiver: Arc<Mutex<Receiver<DriverCommand>>>,
) -> io::Result<()> {
    loop {
        let command = lock_unpoisoned(&receiver).recv();
        let Ok(command) = command else {
            break;
        };
        match command {
            DriverCommand::Submit(mut task) => {
                if task.completion.cancel_requested.load(Ordering::Acquire) {
                    shared.finish(task, CompletionStatus::Cancelled, 0);
                    continue;
                }
                let (status, transferred) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        execute_backend(backend.as_ref(), &mut task.operation)
                    }))
                    .unwrap_or_else(|_| {
                        (
                            CompletionStatus::Failed(io::Error::other("I/O backend panicked")),
                            0,
                        )
                    });
                shared.finish(task, status, transferred);
            }
            DriverCommand::Cancel(request_id) => {
                // The cancel flag is visible directly through CompletionState.
                // A blocking syscall already in progress is allowed to win.
                let _ = request_id;
            }
            DriverCommand::Shutdown => break,
        }
    }
    Ok(())
}

fn execute_backend(
    backend: &dyn IoBackend,
    operation: &mut IoOperation,
) -> (CompletionStatus, usize) {
    match operation {
        IoOperation::Read { buffer, offset } => match buffer.read_target() {
            Ok(buffer_pointer) => {
                let (result, transferred) = read_exact_at_uninit_with_progress(
                    backend,
                    buffer_pointer,
                    buffer.len(),
                    *offset,
                );
                backend_result(result, transferred)
            }
            Err(error) => backend_result(Err(error), 0),
        },
        IoOperation::Write {
            point,
            buffer,
            offset,
        } => match buffer.as_slice() {
            Ok(buffer) => {
                let (result, transferred) =
                    write_all_at_with_progress(backend, *point, buffer, *offset);
                backend_result(result, transferred)
            }
            Err(error) => backend_result(Err(error), 0),
        },
        IoOperation::Flush { point, mode } => exact_backend_result(backend.sync(*point, *mode), 0),
    }
}

fn exact_backend_result(result: io::Result<()>, length: usize) -> (CompletionStatus, usize) {
    backend_result(result, length)
}

fn backend_result(result: io::Result<()>, transferred: usize) -> (CompletionStatus, usize) {
    match result {
        Ok(()) => (CompletionStatus::Completed, transferred),
        Err(error) => (CompletionStatus::Failed(error), transferred),
    }
}

#[cfg(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
mod uring {
    use std::collections::{HashMap, VecDeque};
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use io_uring::{IoUring, Probe, opcode, squeue, types};
    use xxhash_rust::xxh3::Xxh3DefaultBuilder;

    use super::*;

    const CANCEL_CQE_BIT: u64 = 1_u64 << 63;
    const INTERNAL_CQE_BIT: u64 = 1_u64 << 62;
    const WAKE_REQUEST_ID: u64 = INTERNAL_CQE_BIT | 1;
    const WAKE_CANCEL_ID: u64 = CANCEL_CQE_BIT | WAKE_REQUEST_ID;
    const LINUX_EINTR: i32 = 4;
    const LINUX_ECANCELED: i32 = 125;
    const LINUX_POLLIN: u32 = 0x0001;
    const FATAL_DRAIN_ROUNDS: usize = 64;
    const MAX_INTERRUPTED_RETRIES: usize = 4;
    const MAX_WAKE_ATTEMPTS: usize = 4;

    struct SocketWake {
        sender: UnixStream,
    }

    impl DriverWake for SocketWake {
        fn wake(&self) {
            let mut sender = &self.sender;
            for _ in 0..MAX_WAKE_ATTEMPTS {
                match sender.write(&[1]) {
                    Ok(1) => return,
                    Ok(0) => continue,
                    Ok(_) => unreachable!("one-byte wake write cannot write more than one byte"),
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => return,
                    Err(_) => {
                        // Closing or failing the socket also completes the
                        // driver's outstanding read, so it will observe the
                        // command channel instead of treating this as success.
                        return;
                    }
                }
            }
        }
    }

    /// Linux `io_uring` engine. The ring and every raw buffer pointer are owned
    /// by one driver thread; callers communicate only through bounded commands.
    #[derive(Clone)]
    pub(crate) struct UringIoEngine {
        inner: Arc<RuntimeInner>,
        io_stats: DirectIoStatsHandle,
    }

    impl UringIoEngine {
        pub(crate) fn new_with_files(
            files: RuntimeFileSet,
            max_in_flight: usize,
            statistics_enabled: bool,
        ) -> io::Result<Self> {
            RuntimeInner::validate_max_in_flight(max_in_flight)?;
            files.set_statistics_enabled(statistics_enabled);
            let io_stats = files.stats_handle();
            let ring_entries = max_in_flight
                .checked_add(2)
                .and_then(usize::checked_next_power_of_two)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "ring queue size overflow")
                })?;
            let ring_entries = u32::try_from(ring_entries).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "ring queue size exceeds u32")
            })?;
            let completion_entries = ring_entries.checked_mul(2).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "completion queue size overflow",
                )
            })?;
            let mut builder = IoUring::builder();
            builder.setup_cqsize(completion_entries).dontfork();
            let ring = builder.build(ring_entries)?;
            if !ring.params().is_feature_nodrop() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "kernel io_uring can drop completion entries",
                ));
            }
            let mut probe = Probe::new();
            ring.submitter().register_probe(&mut probe)?;
            let required = [
                opcode::Read::CODE,
                opcode::Write::CODE,
                opcode::Fsync::CODE,
                opcode::AsyncCancel::CODE,
                opcode::PollAdd::CODE,
            ];
            if required.iter().any(|opcode| !probe.is_supported(*opcode)) {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "kernel io_uring lacks a required file opcode",
                ));
            }

            let (wake_sender, wake_receiver) = UnixStream::pair()?;
            wake_sender.set_nonblocking(true)?;
            let wake: Arc<dyn DriverWake> = Arc::new(SocketWake {
                sender: wake_sender,
            });
            let shared = Arc::new(RuntimeShared::new(max_in_flight, statistics_enabled));
            let command_capacity = max_in_flight
                .checked_mul(2)
                .and_then(|depth| depth.checked_add(1))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "queue size overflow")
                })?;
            let (commands, receiver) = mpsc::sync_channel(command_capacity);
            let submit_state = Arc::new(RwLock::new(SubmitState { accepting: true }));
            let worker_shared = Arc::clone(&shared);
            let worker_submit_state = Arc::clone(&submit_state);
            let worker = std::thread::Builder::new()
                .name("cache-rs-uring-io".into())
                .stack_size(CACHE_THREAD_STACK_BYTES)
                .spawn(move || {
                    uring_driver(
                        files,
                        ring,
                        wake_receiver,
                        worker_shared,
                        worker_submit_state,
                        receiver,
                    )
                })?;
            Ok(Self {
                inner: Arc::new(RuntimeInner {
                    shared,
                    commands,
                    submit_state,
                    next_request_id: AtomicU64::new(1),
                    wake: Some(wake),
                    workers: Mutex::new(vec![worker]),
                    shutdown: ShutdownState {
                        phase: Mutex::new(ShutdownPhase::Running),
                        stopped: Condvar::new(),
                    },
                }),
                io_stats,
            })
        }

        /// Errors that mean Auto mode may select the synchronous engine before
        /// any request has been accepted. Runtime I/O errors are never eligible.
        pub(crate) fn is_unavailable_error(error: &io::Error) -> bool {
            matches!(
                error.kind(),
                io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
            ) || matches!(error.raw_os_error(), Some(22 | 38 | 95))
        }
    }

    impl IoEngine for UringIoEngine {
        fn try_reserve_read(&self) -> io::Result<ReadSlot> {
            self.inner.try_reserve_read()
        }

        fn submit_reserved_read(
            &self,
            slot: ReadSlot,
            operation: IoOperation,
        ) -> Result<IoRequest, SubmitError> {
            self.inner.submit_reserved_read(slot, operation)
        }

        #[cfg(test)]
        fn submit_nowait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
            self.inner.submit_nowait(operation)
        }

        #[cfg(test)]
        fn submit_wait(&self, operation: IoOperation) -> Result<IoRequest, SubmitError> {
            self.inner.submit_wait(operation)
        }

        fn submit_wait_controlled(
            &self,
            operation: IoOperation,
            cancelled: &AtomicBool,
            deadline: Option<Instant>,
        ) -> Result<IoRequest, SubmitError> {
            self.inner
                .submit_wait_controlled(operation, cancelled, deadline)
        }

        fn wake_slot_waiters(&self) {
            self.inner.shared.wake_slot_waiters();
        }

        fn cancel(&self, request_id: RequestId, state: &CompletionState) -> io::Result<bool> {
            self.inner.cancel(request_id, state)
        }

        fn shutdown(&self) -> io::Result<()> {
            self.inner.shutdown()
        }

        fn in_flight(&self) -> usize {
            self.inner.shared.total_in_flight()
        }

        #[cfg(test)]
        fn direct_active(&self) -> bool {
            self.io_stats.snapshot().direct_active
        }

        fn stop_accepting_requests(&self) {
            self.inner.stop_accepting_requests();
        }

        fn writes_in_flight(&self) -> usize {
            self.inner.shared.writes_in_flight()
        }

        fn has_unfenced_writes(&self) -> bool {
            self.inner.shared.has_unfenced_writes()
        }

        #[cfg(test)]
        fn mark_unfenced_writes_for_test(&self) {
            self.inner.shared.mark_unfenced_writes();
        }

        fn stats(&self) -> CacheIoSnapshot {
            let mut stats = self.inner.shared.snapshot();
            let direct = self.io_stats.snapshot();
            stats.direct_operations = direct.direct_operations;
            stats.direct_bytes = direct.direct_bytes;
            stats.buffered_operations = direct.buffered_operations;
            stats.buffered_bytes = direct.buffered_bytes;
            stats
        }
    }

    struct Flight {
        task: Task,
        transferred: usize,
        active: bool,
        active_path: Option<RuntimeIoPath>,
        force_buffered: bool,
        cancel_submitted: bool,
    }

    #[derive(Clone, Copy)]
    enum PendingEntry {
        Target(RequestId),
        Cancel(RequestId),
        Wake,
        WakeCancel,
    }

    struct UringDriver {
        files: Option<RuntimeFileSet>,
        ring: Option<IoUring>,
        wake_receiver: UnixStream,
        wake_active: bool,
        wake_cancel_submitted: bool,
        wake_cancel_completed: bool,
        shared: Arc<RuntimeShared>,
        submit_state: Arc<RwLock<SubmitState>>,
        receiver: Receiver<DriverCommand>,
        flights: HashMap<RequestId, Flight, Xxh3DefaultBuilder>,
        pending_targets: VecDeque<RequestId>,
        pending_cancels: VecDeque<RequestId>,
        requested_cancels: Vec<RequestId>,
        pending_entries: Vec<PendingEntry>,
        submission_entries: Vec<squeue::Entry>,
        completion_events: Vec<(u64, i32)>,
        shutting_down: bool,
    }

    fn uring_driver(
        files: RuntimeFileSet,
        mut ring: IoUring,
        wake_receiver: UnixStream,
        shared: Arc<RuntimeShared>,
        submit_state: Arc<RwLock<SubmitState>>,
        receiver: Receiver<DriverCommand>,
    ) -> io::Result<()> {
        let max_in_flight = shared.max_in_flight;
        let submission_capacity = ring.submission().capacity();
        let completion_capacity = ring.completion().capacity();
        let mut driver = UringDriver {
            files: Some(files),
            ring: Some(ring),
            wake_receiver,
            wake_active: false,
            wake_cancel_submitted: false,
            wake_cancel_completed: false,
            shared,
            submit_state,
            receiver,
            flights: HashMap::with_capacity_and_hasher(max_in_flight, Xxh3DefaultBuilder::new()),
            pending_targets: VecDeque::with_capacity(max_in_flight),
            pending_cancels: VecDeque::with_capacity(max_in_flight),
            requested_cancels: Vec::with_capacity(max_in_flight),
            pending_entries: Vec::with_capacity(submission_capacity),
            submission_entries: Vec::with_capacity(submission_capacity),
            completion_events: Vec::with_capacity(completion_capacity),
            shutting_down: false,
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| driver.run()))
            .unwrap_or_else(|_| Err(io::Error::other("io_uring driver panicked")));
        if let Err(error) = &result {
            driver.stop_accepting_and_fail_all(error);
        }
        result
    }

    impl UringDriver {
        fn ring_mut(&mut self) -> &mut IoUring {
            self.ring.as_mut().expect("ring exists while driver runs")
        }

        fn run(&mut self) -> io::Result<()> {
            loop {
                self.drain_completions();
                self.drain_commands();
                self.queue_requested_cancels();

                if self.shutting_down && self.flights.is_empty() {
                    if self.wake_active && !self.wake_cancel_submitted {
                        self.push_entries(&[PendingEntry::WakeCancel])?;
                        self.wake_cancel_submitted = true;
                    }
                    if !self.wake_active
                        && (!self.wake_cancel_submitted || self.wake_cancel_completed)
                    {
                        return Ok(());
                    }
                }

                let submitted = self.submit_pending()?;
                if self.shutting_down
                    && self.flights.is_empty()
                    && !self.wake_active
                    && (!self.wake_cancel_submitted || self.wake_cancel_completed)
                {
                    return Ok(());
                }
                if submitted == 0 {
                    self.wait_for_completion()?;
                }
            }
        }

        fn drain_commands(&mut self) {
            loop {
                match self.receiver.try_recv() {
                    Ok(DriverCommand::Submit(task)) => {
                        if task.completion.cancel_requested.load(Ordering::Acquire) {
                            self.shared.finish(task, CompletionStatus::Cancelled, 0);
                        } else if task.operation_is_empty() {
                            self.shared.finish(task, CompletionStatus::Completed, 0);
                        } else {
                            let request_id = task.request_id;
                            self.flights.insert(
                                request_id,
                                Flight {
                                    task,
                                    transferred: 0,
                                    active: false,
                                    active_path: None,
                                    force_buffered: false,
                                    cancel_submitted: false,
                                },
                            );
                            self.pending_targets.push_back(request_id);
                        }
                    }
                    Ok(DriverCommand::Cancel(request_id)) => {
                        self.queue_cancel_if_active(request_id);
                    }
                    Ok(DriverCommand::Shutdown) => self.shutting_down = true,
                    Err(mpsc::TryRecvError::Empty) => break,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        self.shutting_down = true;
                        break;
                    }
                }
            }
        }

        fn queue_requested_cancels(&mut self) {
            if !self.shared.cancel_scan_needed.swap(false, Ordering::AcqRel) {
                return;
            }
            self.requested_cancels.clear();
            self.requested_cancels
                .extend(self.flights.iter().filter_map(|(request_id, flight)| {
                    (flight
                        .task
                        .completion
                        .cancel_requested
                        .load(Ordering::Acquire)
                        && flight.active
                        && !flight.cancel_submitted)
                        .then_some(*request_id)
                }));
            for index in 0..self.requested_cancels.len() {
                let request_id = self.requested_cancels[index];
                self.queue_cancel_if_active(request_id);
            }
        }

        fn queue_cancel_if_active(&mut self, request_id: RequestId) {
            let Some(flight) = self.flights.get_mut(&request_id) else {
                return;
            };
            if flight.active && !flight.cancel_submitted {
                flight.cancel_submitted = true;
                self.pending_cancels.push_back(request_id);
            }
        }

        fn submit_pending(&mut self) -> io::Result<usize> {
            let available = {
                let sq = self.ring_mut().submission();
                sq.capacity() - sq.len()
            };
            if available == 0 {
                return self.submit_ring();
            }
            self.pending_entries.clear();
            if !self.shutting_down && !self.wake_active {
                self.pending_entries.push(PendingEntry::Wake);
            }
            while self.pending_entries.len() < available {
                let Some(request_id) = self.pending_cancels.pop_front() else {
                    break;
                };
                if self.flights.contains_key(&request_id) {
                    self.pending_entries.push(PendingEntry::Cancel(request_id));
                }
            }
            while self.pending_entries.len() < available {
                let Some(request_id) = self.pending_targets.pop_front() else {
                    break;
                };
                let Some(flight) = self.flights.get(&request_id) else {
                    continue;
                };
                if flight
                    .task
                    .completion
                    .cancel_requested
                    .load(Ordering::Acquire)
                    && !flight.active
                {
                    let flight = self
                        .flights
                        .remove(&request_id)
                        .expect("checked flight exists");
                    let status = cancelled_before_resubmit_status(&flight);
                    self.shared.finish(flight.task, status, flight.transferred);
                    continue;
                }
                self.pending_entries.push(PendingEntry::Target(request_id));
            }
            if self.pending_entries.is_empty() {
                return self.submit_ring();
            }
            self.push_pending_entries()?;
            self.submit_ring()
        }

        fn push_pending_entries(&mut self) -> io::Result<()> {
            self.submission_entries.clear();
            for index in 0..self.pending_entries.len() {
                let pending = self.pending_entries[index];
                let entry = self.build_submission_entry(pending)?;
                self.submission_entries.push(entry);
            }
            self.push_submission_entries()
        }

        fn push_entries(&mut self, pending: &[PendingEntry]) -> io::Result<()> {
            self.submission_entries.clear();
            for pending_entry in pending {
                let entry = self.build_submission_entry(*pending_entry)?;
                self.submission_entries.push(entry);
            }
            self.push_submission_entries()
        }

        fn build_submission_entry(&mut self, pending: PendingEntry) -> io::Result<squeue::Entry> {
            let wake_fd = self.wake_receiver.as_raw_fd();
            Ok(match pending {
                PendingEntry::Target(request_id) => {
                    let path = {
                        let flight = self
                            .flights
                            .get(&request_id)
                            .expect("pending target has a flight");
                        if flight.force_buffered {
                            RuntimeIoPath::Buffered
                        } else {
                            flight.task.operation.runtime_io_path(
                                self.files
                                    .as_ref()
                                    .expect("runtime files exist while driver runs"),
                                flight.transferred,
                            )?
                        }
                    };
                    let file_fd = self
                        .files
                        .as_ref()
                        .expect("runtime files exist while driver runs")
                        .file_for(path)
                        .as_raw_fd();
                    let flight = self
                        .flights
                        .get_mut(&request_id)
                        .expect("pending target has a flight");
                    flight.active = true;
                    flight.active_path = Some(path);
                    build_target_entry(file_fd, request_id, flight)?
                }
                PendingEntry::Cancel(request_id) => opcode::AsyncCancel::new(request_id.get())
                    .build()
                    .user_data(CANCEL_CQE_BIT | request_id.get()),
                PendingEntry::Wake => {
                    self.wake_active = true;
                    opcode::PollAdd::new(types::Fd(wake_fd), LINUX_POLLIN)
                        .build()
                        .user_data(WAKE_REQUEST_ID)
                }
                PendingEntry::WakeCancel => opcode::AsyncCancel::new(WAKE_REQUEST_ID)
                    .build()
                    .user_data(WAKE_CANCEL_ID),
            })
        }

        fn push_submission_entries(&mut self) -> io::Result<()> {
            let UringDriver {
                ring,
                submission_entries,
                ..
            } = self;
            let mut sq = ring
                .as_mut()
                .expect("ring exists while driver runs")
                .submission();
            // SAFETY: every file descriptor and data buffer is owned by this
            // driver. Target buffers remain in `flights` until their target
            // CQE; the wake entry is a pointer-free poll operation.
            unsafe {
                sq.push_multiple(submission_entries).map_err(|_| {
                    io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ is full")
                })?;
            }
            Ok(())
        }

        fn submit_ring(&mut self) -> io::Result<usize> {
            retry_interrupted(|| self.ring_mut().submit())
        }

        fn wait_for_completion(&mut self) -> io::Result<()> {
            let wake_cancel_pending = self.wake_cancel_submitted && !self.wake_cancel_completed;
            if !self.wake_active && self.flights.is_empty() && !wake_cancel_pending {
                return Ok(());
            }
            retry_interrupted(|| self.ring_mut().submit_and_wait(1)).map(|_| ())
        }

        fn drain_completions(&mut self) {
            self.completion_events.clear();
            {
                let UringDriver {
                    ring,
                    completion_events,
                    ..
                } = self;
                let mut cq = ring
                    .as_mut()
                    .expect("ring exists while driver runs")
                    .completion();
                completion_events
                    .extend(cq.by_ref().map(|entry| (entry.user_data(), entry.result())));
            }
            for index in 0..self.completion_events.len() {
                let (user_data, result) = self.completion_events[index];
                if user_data == WAKE_REQUEST_ID {
                    self.wake_active = false;
                    if !self.shutting_down && result >= 0 {
                        self.drain_wake_bytes();
                    }
                } else if user_data == WAKE_CANCEL_ID {
                    self.wake_cancel_completed = true;
                } else if user_data & CANCEL_CQE_BIT != 0 {
                    // A cancel CQE is only the cancel request's outcome. The
                    // target flight and buffer remain alive until target CQE.
                } else if user_data & INTERNAL_CQE_BIT == 0 {
                    self.complete_target(RequestId(user_data), result);
                }
            }
        }

        fn drain_wake_bytes(&mut self) {
            if self.wake_receiver.set_nonblocking(true).is_err() {
                return;
            }
            let mut wake_buffer = [0_u8; 64];
            loop {
                match retry_interrupted(|| self.wake_receiver.read(&mut wake_buffer)) {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
            let _ = self.wake_receiver.set_nonblocking(false);
        }

        fn complete_target(&mut self, request_id: RequestId, result: i32) {
            let Some(mut flight) = self.flights.remove(&request_id) else {
                return;
            };
            flight.active = false;
            let active_path = flight.active_path.take().unwrap_or(RuntimeIoPath::Buffered);
            if result < 0 {
                let raw_error = result.saturating_neg();
                let error = io::Error::from_raw_os_error(raw_error);
                if !flight
                    .task
                    .completion
                    .cancel_requested
                    .load(Ordering::Acquire)
                    && self
                        .files
                        .as_ref()
                        .is_some_and(|files| files.should_fallback(active_path, &error))
                {
                    flight.force_buffered = true;
                    flight.cancel_submitted = false;
                    self.flights.insert(request_id, flight);
                    self.pending_targets.push_back(request_id);
                    return;
                }
                let cancelled = flight
                    .task
                    .completion
                    .cancel_requested
                    .load(Ordering::Acquire)
                    && matches!(raw_error, LINUX_EINTR | LINUX_ECANCELED);
                let status = if cancelled
                    && (flight.task.operation.kind() == OperationKind::Read
                        || (raw_error == LINUX_ECANCELED && flight.transferred == 0))
                {
                    CompletionStatus::Cancelled
                } else {
                    CompletionStatus::Failed(error)
                };
                self.shared.finish(flight.task, status, flight.transferred);
                return;
            }

            match flight.task.operation.kind() {
                OperationKind::Flush => {
                    self.shared
                        .finish(flight.task, CompletionStatus::Completed, 0);
                }
                OperationKind::Read | OperationKind::Write => {
                    let remaining =
                        operation_length(&flight.task.operation).saturating_sub(flight.transferred);
                    let completed = result as usize;
                    if completed <= remaining && completed != 0 {
                        self.files
                            .as_ref()
                            .expect("runtime files exist while driver runs")
                            .record(active_path, completed);
                    }
                    if completed == 0 && remaining != 0 {
                        let kind = if flight.task.operation.kind() == OperationKind::Read {
                            io::ErrorKind::UnexpectedEof
                        } else {
                            io::ErrorKind::WriteZero
                        };
                        self.shared.finish(
                            flight.task,
                            CompletionStatus::Failed(io::Error::new(kind, "short io_uring I/O")),
                            flight.transferred,
                        );
                    } else if completed > remaining {
                        self.shared.finish(
                            flight.task,
                            CompletionStatus::Failed(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "io_uring completion exceeds submitted length",
                            )),
                            flight.transferred,
                        );
                    } else {
                        flight.transferred += completed;
                        if flight.transferred == operation_length(&flight.task.operation) {
                            self.shared.finish(
                                flight.task,
                                CompletionStatus::Completed,
                                flight.transferred,
                            );
                        } else if flight
                            .task
                            .completion
                            .cancel_requested
                            .load(Ordering::Acquire)
                        {
                            let status = cancelled_before_resubmit_status(&flight);
                            self.shared.finish(flight.task, status, flight.transferred);
                        } else {
                            self.flights.insert(request_id, flight);
                            self.pending_targets.push_back(request_id);
                        }
                    }
                }
            }
        }

        fn stop_accepting_and_fail_all(&mut self, error: &io::Error) {
            // Submission holds a shared fence through the bounded channel send.
            // Once this exclusive guard is acquired, no task can appear after
            // the drain.
            let mut submit_state = self
                .submit_state
                .write()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            submit_state.accepting = false;
            {
                let _slot = lock_unpoisoned(&self.shared.slot_lock);
                self.shared.accepting.store(false, Ordering::Release);
                self.shared.slot_available.notify_all();
            }
            // The false state is permanent. Release the fence before waking
            // completion consumers so a custom waker may safely re-enter the
            // engine and receive BrokenPipe instead of deadlocking.
            drop(submit_state);
            let message = error.to_string();

            // io_uring_cancelation(7) explicitly says that closing the ring
            // cancels requests but is not a lifetime fence for associated
            // resources. Make a bounded best-effort cancel/drain pass. A
            // target CQE, never its cancel CQE, is the release fence.
            self.try_cancel_and_drain_fatal(error, &message);

            let unfenced_writes = self
                .flights
                .values()
                .any(|flight| flight.active && flight.task.operation.kind().uses_write_slot());
            if unfenced_writes {
                // A ring close does not wait for hardware writes. Preserve the
                // duplicated open-file description (and therefore its flock)
                // for process lifetime. The cache must also observe the flag
                // and never issue LOCK_UN on another duplicate.
                self.shared.mark_unfenced_writes();
                if let Some(files) = self.files.take() {
                    std::mem::forget(files);
                }
            }

            for (_, flight) in self.flights.drain() {
                let status = CompletionStatus::Failed(copy_io_error(error, &message));
                if flight.active && flight.task.operation.kind() != OperationKind::Flush {
                    self.shared
                        .finish_quarantined(flight.task, status, flight.transferred);
                } else {
                    self.shared.finish(flight.task, status, flight.transferred);
                }
            }
            while let Ok(command) = self.receiver.try_recv() {
                if let DriverCommand::Submit(task) = command {
                    self.shared.finish(
                        task,
                        CompletionStatus::Failed(copy_io_error(error, &message)),
                        0,
                    );
                }
            }

            drop(self.ring.take());
        }

        fn try_cancel_and_drain_fatal(&mut self, error: &io::Error, message: &str) {
            let mut cancel_targets = VecDeque::with_capacity(self.flights.len() + 1);
            for (request_id, flight) in &self.flights {
                if flight.active {
                    cancel_targets.push_back((request_id.get(), CANCEL_CQE_BIT | request_id.get()));
                }
            }
            if self.wake_active {
                cancel_targets.push_back((WAKE_REQUEST_ID, WAKE_CANCEL_ID));
            }

            for _ in 0..FATAL_DRAIN_ROUNDS {
                self.drain_fatal_completions(error, message);
                if !self.has_active_target() {
                    return;
                }

                let available = {
                    let sq = self.ring_mut().submission();
                    sq.capacity() - sq.len()
                };
                if available != 0 && !cancel_targets.is_empty() {
                    let mut entries = Vec::with_capacity(available.min(cancel_targets.len()));
                    while entries.len() < available {
                        let Some((target_user_data, cancel_user_data)) = cancel_targets.pop_front()
                        else {
                            break;
                        };
                        let target_still_active = if target_user_data == WAKE_REQUEST_ID {
                            self.wake_active
                        } else {
                            self.flights
                                .get(&RequestId(target_user_data))
                                .is_some_and(|flight| flight.active)
                        };
                        if target_still_active {
                            entries.push(
                                opcode::AsyncCancel::new(target_user_data)
                                    .build()
                                    .user_data(cancel_user_data),
                            );
                        }
                    }
                    if !entries.is_empty() {
                        let mut sq = self.ring_mut().submission();
                        // SAFETY: entries contain no borrowed userspace
                        // pointers, and capacity was checked above.
                        if unsafe { sq.push_multiple(&entries) }.is_err() {
                            return;
                        }
                    }
                }

                if self.submit_ring().is_err() {
                    return;
                }
                self.drain_fatal_completions(error, message);
                if !self.has_active_target() {
                    return;
                }
                std::thread::yield_now();
            }
        }

        fn has_active_target(&self) -> bool {
            self.wake_active || self.flights.values().any(|flight| flight.active)
        }

        fn drain_fatal_completions(&mut self, error: &io::Error, message: &str) {
            let completions: Vec<u64> = {
                let mut cq = self.ring_mut().completion();
                cq.by_ref().map(|entry| entry.user_data()).collect()
            };
            for user_data in completions {
                if user_data == WAKE_REQUEST_ID {
                    self.wake_active = false;
                } else if user_data == WAKE_CANCEL_ID || user_data & CANCEL_CQE_BIT != 0 {
                    // A cancel CQE is not a target lifetime fence.
                } else if user_data & INTERNAL_CQE_BIT == 0
                    && let Some(flight) = self.flights.remove(&RequestId(user_data))
                {
                    self.shared.finish(
                        flight.task,
                        CompletionStatus::Failed(copy_io_error(error, message)),
                        flight.transferred,
                    );
                }
            }
        }
    }

    fn retry_interrupted<T>(mut operation: impl FnMut() -> io::Result<T>) -> io::Result<T> {
        let mut retries = 0_usize;
        loop {
            match operation() {
                Err(error)
                    if error.kind() == io::ErrorKind::Interrupted
                        && retries < MAX_INTERRUPTED_RETRIES =>
                {
                    retries += 1;
                }
                result => return result,
            }
        }
    }

    impl Task {
        fn operation_is_empty(&self) -> bool {
            match &self.operation {
                IoOperation::Read { buffer, .. } | IoOperation::Write { buffer, .. } => {
                    buffer.is_empty()
                }
                IoOperation::Flush { .. } => false,
            }
        }
    }

    fn copy_io_error(error: &io::Error, message: &str) -> io::Error {
        error.raw_os_error().map_or_else(
            || io::Error::new(error.kind(), message.to_owned()),
            io::Error::from_raw_os_error,
        )
    }

    fn operation_length(operation: &IoOperation) -> usize {
        match operation {
            IoOperation::Read { buffer, .. } | IoOperation::Write { buffer, .. } => buffer.len(),
            IoOperation::Flush { .. } => 0,
        }
    }

    fn cancelled_before_resubmit_status(flight: &Flight) -> CompletionStatus {
        if flight.task.operation.kind() == OperationKind::Read || flight.transferred == 0 {
            CompletionStatus::Cancelled
        } else {
            CompletionStatus::Failed(io::Error::new(
                io::ErrorKind::Interrupted,
                "write cancellation raced with a partial completion",
            ))
        }
    }

    fn build_target_entry(
        file_fd: i32,
        request_id: RequestId,
        flight: &mut Flight,
    ) -> io::Result<squeue::Entry> {
        let transferred = flight.transferred;
        match &mut flight.task.operation {
            IoOperation::Read { buffer, offset } => {
                let remaining = buffer.len() - transferred;
                let length = u32::try_from(remaining).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "read length exceeds u32")
                })?;
                // SAFETY: `transferred <= buffer.len()` and the allocation is
                // kept in this flight until the target CQE.
                let pointer = unsafe { buffer.as_mut_ptr()?.add(transferred) };
                Ok(opcode::Read::new(types::Fd(file_fd), pointer, length)
                    .offset(*offset + transferred as u64)
                    .build()
                    .user_data(request_id.get()))
            }
            IoOperation::Write { buffer, offset, .. } => {
                let remaining = buffer.len() - transferred;
                let length = u32::try_from(remaining).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "write length exceeds u32")
                })?;
                // SAFETY: `transferred <= buffer.len()` and the allocation is
                // kept in this flight until the target CQE.
                let pointer = unsafe { buffer.as_ptr()?.add(transferred) };
                Ok(opcode::Write::new(types::Fd(file_fd), pointer, length)
                    .offset(*offset + transferred as u64)
                    .build()
                    .user_data(request_id.get()))
            }
            IoOperation::Flush { mode, .. } => {
                let flags = match mode {
                    SyncMode::Data => types::FsyncFlags::DATASYNC,
                    SyncMode::All => types::FsyncFlags::empty(),
                };
                Ok(opcode::Fsync::new(types::Fd(file_fd))
                    .flags(flags)
                    .build()
                    .user_data(request_id.get()))
            }
        }
    }
}

#[cfg(all(
    feature = "io-uring",
    target_os = "linux",
    any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "powerpc64"
    )
))]
pub(crate) use uring::UringIoEngine;

/// Builds the fastest available native file engine while preserving the
/// positioned-I/O backend as a behaviorally identical fallback.
#[cfg(unix)]
pub(crate) fn build_auto_file_engine(
    files: RuntimeFileSet,
    max_in_flight: usize,
    statistics_enabled: bool,
) -> io::Result<Arc<dyn IoEngine>> {
    #[cfg(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    ))]
    {
        let fallback = files.try_clone()?;
        match UringIoEngine::new_with_files(files, max_in_flight, statistics_enabled) {
            Ok(engine) => Ok(Arc::new(engine)),
            Err(error) if UringIoEngine::is_unavailable_error(&error) => {
                BackendIoEngine::new_with_files_and_workers(fallback, 1, 1, statistics_enabled)
                    .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(not(all(
        feature = "io-uring",
        target_os = "linux",
        any(
            target_arch = "x86_64",
            target_arch = "aarch64",
            target_arch = "riscv64",
            target_arch = "loongarch64",
            target_arch = "powerpc64"
        )
    )))]
    {
        let _ = max_in_flight;
        BackendIoEngine::new_with_files_and_workers(files, 1, 1, statistics_enabled)
            .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
    }
}

#[cfg(unix)]
pub(crate) fn build_file_engine(
    files: RuntimeFileSet,
    max_in_flight: usize,
    kind: ConfiguredIoEngine,
    statistics_enabled: bool,
) -> io::Result<Arc<dyn IoEngine>> {
    match kind {
        ConfiguredIoEngine::Sync => {
            BackendIoEngine::new_with_files_and_workers(files, 1, 1, statistics_enabled)
                .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
        }
        ConfiguredIoEngine::Auto => {
            build_auto_file_engine(files, max_in_flight, statistics_enabled)
        }
        ConfiguredIoEngine::IoUring => {
            #[cfg(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            ))]
            {
                UringIoEngine::new_with_files(files, max_in_flight, statistics_enabled)
                    .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>)
            }
            #[cfg(not(all(
                feature = "io-uring",
                target_os = "linux",
                any(
                    target_arch = "x86_64",
                    target_arch = "aarch64",
                    target_arch = "riscv64",
                    target_arch = "loongarch64",
                    target_arch = "powerpc64"
                )
            )))]
            {
                let _ = files;
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "io_uring is unavailable on this build or platform",
                ))
            }
        }
    }
}

fn update_peak(peak: &AtomicUsize, value: usize) {
    peak.fetch_max(value, Ordering::Relaxed);
}

fn add_duration_ns(counter: &AtomicU64, duration: std::time::Duration) {
    const MAX_DURATION_CAS_ATTEMPTS: usize = 8;
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    let mut current = counter.load(Ordering::Relaxed);
    for _ in 0..MAX_DURATION_CAS_ATTEMPTS {
        let next = current.saturating_add(nanos);
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{File, OpenOptions};
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use crate::io_backend::FileBackend;
    use crate::resources::{ResourceController, ResourceLimits, aligned_buffer_capacity};

    static FILE_ID: AtomicU64 = AtomicU64::new(1);

    struct TestFile {
        path: PathBuf,
    }

    impl TestFile {
        fn new() -> Self {
            let id = FILE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cache-rs-io-engine-{}-{id}.bin",
                std::process::id()
            ));
            Self { path }
        }

        fn backend(&self) -> Arc<dyn IoBackend> {
            Arc::new(FileBackend::open(&self.path).unwrap())
        }

        fn file(&self) -> File {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&self.path)
                .unwrap()
        }
    }

    impl Drop for TestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[derive(Default)]
    struct BlockingState {
        entered: usize,
        active: usize,
        maximum_active: usize,
        released: bool,
    }

    #[derive(Default)]
    struct BlockingBackend {
        state: Mutex<BlockingState>,
        changed: Condvar,
    }

    impl BlockingBackend {
        fn wait_for_entered(&self, expected: usize) -> bool {
            let state = lock_unpoisoned(&self.state);
            let (state, _) = self
                .changed
                .wait_timeout_while(state, Duration::from_secs(1), |state| {
                    state.entered < expected
                })
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.entered >= expected
        }

        fn release(&self) {
            let mut state = lock_unpoisoned(&self.state);
            state.released = true;
            self.changed.notify_all();
        }

        fn maximum_active(&self) -> usize {
            lock_unpoisoned(&self.state).maximum_active
        }

        fn enter_and_wait(&self) {
            let mut state = lock_unpoisoned(&self.state);
            state.entered += 1;
            state.active += 1;
            state.maximum_active = state.maximum_active.max(state.active);
            self.changed.notify_all();
            while !state.released {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            state.active -= 1;
        }
    }

    impl IoBackend for BlockingBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(1024 * 1024)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
            self.enter_and_wait();
            buffer.fill(0);
            Ok(buffer.len())
        }

        fn write_at(&self, _point: WritePoint, buffer: &[u8], _offset: u64) -> io::Result<usize> {
            self.enter_and_wait();
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
    }

    struct PanicOnceBackend {
        panic_next_read: AtomicBool,
    }

    #[derive(Default)]
    struct ShortThenErrorBackend {
        read_calls: AtomicUsize,
        write_calls: AtomicUsize,
    }

    impl PanicOnceBackend {
        fn new() -> Self {
            Self {
                panic_next_read: AtomicBool::new(true),
            }
        }
    }

    impl IoBackend for PanicOnceBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(1024 * 1024)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
            if self.panic_next_read.swap(false, Ordering::AcqRel) {
                panic!("injected backend panic");
            }
            buffer.fill(0);
            Ok(buffer.len())
        }

        fn write_at(&self, _point: WritePoint, buffer: &[u8], _offset: u64) -> io::Result<usize> {
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
    }

    impl IoBackend for ShortThenErrorBackend {
        fn len(&self) -> io::Result<u64> {
            Ok(1024 * 1024)
        }

        fn set_len(&self, _len: u64) -> io::Result<()> {
            Ok(())
        }

        fn read_at(&self, buffer: &mut [u8], _offset: u64) -> io::Result<usize> {
            if self.read_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                let transferred = 3.min(buffer.len());
                buffer[..transferred].fill(0x5a);
                Ok(transferred)
            } else {
                Err(io::Error::from_raw_os_error(5))
            }
        }

        fn write_at(&self, _point: WritePoint, buffer: &[u8], _offset: u64) -> io::Result<usize> {
            if self.write_calls.fetch_add(1, Ordering::Relaxed) == 0 {
                Ok(3.min(buffer.len()))
            } else {
                Err(io::Error::from_raw_os_error(5))
            }
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

    fn resources(_maximum: usize) -> Arc<ResourceController> {
        Arc::new(
            ResourceController::try_new(ResourceLimits {
                memory_limit_bytes: 1024 * 1024,
                reserved_memory_bytes: 0,
                waiting_write_limit: 4,
            })
            .unwrap(),
        )
    }

    fn read_buffer(resources: &Arc<ResourceController>, length: usize) -> IoBuffer {
        let lease = resources.try_read_buffer(length).unwrap();
        IoBuffer::for_read(lease, length).unwrap()
    }

    fn write_buffer(resources: &Arc<ResourceController>, bytes: &[u8]) -> IoBuffer {
        let mut lease = resources.try_read_buffer(bytes.len()).unwrap();
        lease.prepare(bytes.len()).unwrap().copy_from_slice(bytes);
        IoBuffer::for_write(lease, bytes.len()).unwrap()
    }

    #[test]
    fn aligned_buffer_has_stable_alignment() {
        let resources = resources(8193);
        let buffer = read_buffer(&resources, 8193);
        assert_eq!(
            buffer.read_target().unwrap() as usize % IO_BUFFER_ALIGNMENT,
            0
        );
        assert_eq!(buffer.len(), 8193);
    }

    #[test]
    fn sync_engine_round_trips_owned_buffers_and_drains() {
        let file = TestFile::new();
        let engine = BackendIoEngine::new(file.backend(), 4).unwrap();
        let resources = resources(4096);
        let input = b"owned async positioned I/O";
        let write = engine
            .write_all_at(WritePoint::Record, write_buffer(&resources, input), 4096)
            .unwrap()
            .wait();
        assert!(matches!(write.status, CompletionStatus::Completed));
        assert_eq!(write.bytes_transferred, input.len());

        engine
            .flush(SyncPoint::ExplicitFlush, SyncMode::Data)
            .unwrap()
            .wait();
        let read = engine
            .read_exact_at(read_buffer(&resources, input.len()), 4096)
            .unwrap()
            .wait();
        assert!(matches!(read.status, CompletionStatus::Completed));
        assert_eq!(read.buffer.unwrap().as_slice().unwrap(), input);
        engine.shutdown().unwrap();
        assert_eq!(engine.in_flight(), 0);
        let stats = engine.stats();
        assert_eq!(stats.submitted, 3);
        assert_eq!(stats.completed, 3);
        assert_eq!(stats.errors, 0);
        assert!(stats.requests_in_flight_peak >= 1);
        assert!(
            engine
                .flush(SyncPoint::ExplicitFlush, SyncMode::Data)
                .unwrap_err()
                .error
                .kind()
                .eq(&io::ErrorKind::BrokenPipe)
        );
    }

    #[test]
    fn sync_engine_reports_progress_before_a_terminal_short_io_error() {
        let engine = BackendIoEngine::new(Arc::new(ShortThenErrorBackend::default()), 2).unwrap();
        let resources = resources(4096);

        let read = engine
            .read_exact_at(read_buffer(&resources, 8), 0)
            .unwrap()
            .wait();
        assert!(matches!(read.status, CompletionStatus::Failed(_)));
        assert_eq!(read.bytes_transferred, 3);

        let write = engine
            .write_all_at(WritePoint::Record, write_buffer(&resources, &[0x33; 8]), 0)
            .unwrap()
            .wait();
        assert!(matches!(write.status, CompletionStatus::Failed(_)));
        assert_eq!(write.bytes_transferred, 3);
        engine.shutdown().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn sync_engine_routes_only_aligned_record_io_to_direct() {
        let buffered = TestFile::new();
        let direct = TestFile::new();
        let buffered_file = buffered.file();
        let direct_file = direct.file();
        buffered_file.set_len(8192).unwrap();
        direct_file.set_len(8192).unwrap();
        let engine = BackendIoEngine::new_with_files(
            RuntimeFileSet::new(buffered_file, Some(direct_file)),
            2,
        )
        .unwrap();
        let resources = resources(4096);

        let aligned = vec![0x5a; 4096];
        assert!(matches!(
            engine
                .write_all_at(WritePoint::Record, write_buffer(&resources, &aligned), 0,)
                .unwrap()
                .wait()
                .status,
            CompletionStatus::Completed
        ));
        assert!(matches!(
            engine
                .write_all_at(
                    WritePoint::Record,
                    write_buffer(&resources, &[0x33; 32]),
                    4096,
                )
                .unwrap()
                .wait()
                .status,
            CompletionStatus::Completed
        ));

        assert!(engine.direct_active());
        let stats = engine.stats();
        assert_eq!(stats.direct_operations, 1);
        assert_eq!(stats.direct_bytes, 4096);
        assert_eq!(stats.buffered_operations, 1);
        assert_eq!(stats.buffered_bytes, 32);
        engine.shutdown().unwrap();
    }

    #[test]
    fn unfenced_write_state_remains_unsafe_after_shutdown() {
        let file = TestFile::new();
        let engine = BackendIoEngine::new(file.backend(), 1).unwrap();
        assert!(!engine.has_unfenced_writes());

        engine.mark_unfenced_writes_for_test();
        engine.shutdown().unwrap();

        assert!(engine.has_unfenced_writes());
        assert_eq!(engine.in_flight(), 0);
    }

    #[test]
    fn completion_deadline_stops_requests_without_releasing_an_unfenced_target() {
        let backend = Arc::new(BlockingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = resources(4096);
        let deadline = Instant::now() + Duration::from_millis(20);
        let request = submit_cache_io_until(
            &engine,
            IoOperation::read(read_buffer(&resources, 4096), 0),
            deadline,
            Duration::from_millis(10),
        )
        .unwrap();
        assert!(backend.wait_for_entered(1));

        let timeout = request.wait(&engine).unwrap_err();
        let pending = engine.in_flight();
        let pending_writes = engine.writes_in_flight();
        backend.release();
        let (error, buffer) = timeout.into_buffer();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(buffer.is_none());
        assert_eq!(pending, 1);
        assert_eq!(pending_writes, 0);

        let rejected = engine
            .submit(IoOperation::read(read_buffer(&resources, 1), 0))
            .unwrap_err();
        assert_eq!(rejected.error.kind(), io::ErrorKind::BrokenPipe);

        engine.shutdown().unwrap();
        assert_eq!(engine.in_flight(), 0);
    }

    #[test]
    fn completion_deadline_keeps_an_issued_write_counted_until_target_completion() {
        let backend = Arc::new(BlockingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = resources(4096);
        let request = submit_cache_io_until(
            &engine,
            IoOperation::write(
                WritePoint::Record,
                write_buffer(&resources, &[0x5a; 4096]),
                0,
            ),
            Instant::now() + Duration::from_millis(20),
            Duration::from_millis(10),
        )
        .unwrap();
        assert!(backend.wait_for_entered(1));

        let timeout = request.wait(&engine).unwrap_err();
        let pending = engine.in_flight();
        let pending_writes = engine.writes_in_flight();
        backend.release();
        let (error, buffer) = timeout.into_buffer();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(buffer.is_none());
        assert_eq!(pending, 1);
        assert_eq!(pending_writes, 1);

        engine.shutdown().unwrap();
        assert_eq!(engine.in_flight(), 0);
        assert_eq!(engine.writes_in_flight(), 0);
    }

    #[test]
    fn io_concurrency_is_hard_bounded() {
        let file = TestFile::new();
        assert!(matches!(
            BackendIoEngine::new(file.backend(), MAX_IO_REQUESTS_PER_WORKER + 1),
            Err(error) if error.kind() == io::ErrorKind::InvalidInput
        ));
    }

    #[cfg(unix)]
    #[test]
    fn configured_sync_engine_exposes_only_its_executable_slot() {
        let file = TestFile::new();
        let files = RuntimeFileSet::new(file.file(), None);
        let engine = build_file_engine(files, 32, ConfiguredIoEngine::Sync, false).unwrap();

        let reserved = engine.try_reserve_read().unwrap();
        assert_eq!(
            engine.try_reserve_read().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(reserved);
        drop(engine.try_reserve_read().unwrap());
        engine.shutdown().unwrap();
    }

    #[test]
    fn disabled_io_statistics_skip_cumulative_engine_counters() {
        let file = TestFile::new();
        let engine =
            BackendIoEngine::new_with_workers_and_statistics(file.backend(), 1, 1, false).unwrap();
        let resources = resources(1);
        let completion = engine
            .write_all_at(WritePoint::Record, write_buffer(&resources, &[0x5a]), 0)
            .unwrap()
            .wait();
        assert!(matches!(completion.status, CompletionStatus::Completed));
        assert_eq!(engine.stats(), CacheIoSnapshot::default());
        engine.shutdown().unwrap();
    }

    #[test]
    fn writes_reserve_the_final_engine_slot_for_reads() {
        let shared = Arc::new(RuntimeShared::new(2, true));
        let write = shared.try_reserve_slot(true).unwrap();
        assert!(shared.try_reserve_slot(true).is_none());
        let read = shared.try_reserve_slot(false).unwrap();
        assert_eq!(shared.total_in_flight(), 2);
        drop((write, read));

        let read = shared.try_reserve_slot(false).unwrap();
        let write = shared.try_reserve_slot(true).unwrap();
        assert_eq!(shared.total_in_flight(), 2);
        assert_eq!(shared.writes_in_flight(), 1);
        drop((read, write));

        let depth_one = Arc::new(RuntimeShared::new(1, true));
        assert!(depth_one.try_reserve_slot(true).is_some());
    }

    #[test]
    fn waiting_write_gets_the_next_slot_under_read_pressure() {
        let shared = Arc::new(RuntimeShared::new(2, true));
        let first = shared.try_reserve_slot(false).unwrap();
        let second = shared.try_reserve_slot(false).unwrap();
        let waiting = Arc::clone(&shared);
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let slot = waiting.reserve_slot_wait(true).unwrap();
            admitted_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            drop(slot);
        });

        while shared.waiting_writes_for_test() == 0 {
            std::thread::yield_now();
        }
        drop(first);
        assert!(shared.try_reserve_slot(false).is_none());
        admitted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_tx.send(()).unwrap();
        drop(second);
        writer.join().unwrap();
        assert_eq!(shared.total_in_flight(), 0);
        assert_eq!(shared.writes_in_flight(), 0);
    }

    #[test]
    fn write_waiter_does_not_consume_the_read_reserved_slot() {
        let shared = Arc::new(RuntimeShared::new(2, true));
        let first_write = shared.try_reserve_slot(true).unwrap();
        let waiting = Arc::clone(&shared);
        let (admitted_tx, admitted_rx) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let slot = waiting.reserve_slot_wait(true).unwrap();
            admitted_tx.send(()).unwrap();
            drop(slot);
        });

        while shared.waiting_writes_for_test() == 0 {
            std::thread::yield_now();
        }
        let read = shared.try_reserve_slot(false).unwrap();
        assert_eq!(shared.total_in_flight(), 2);
        drop(read);
        drop(first_write);
        admitted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        writer.join().unwrap();
    }

    #[test]
    fn unused_read_reservation_releases_its_engine_slot() {
        let file = TestFile::new();
        let engine = BackendIoEngine::new(file.backend(), 1).unwrap();
        let slot = engine.try_reserve_read().unwrap();
        assert_eq!(engine.in_flight(), 1);
        assert_eq!(
            engine.try_reserve_read().err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(slot);
        assert_eq!(engine.in_flight(), 0);
        drop(engine.try_reserve_read().unwrap());
        engine.shutdown().unwrap();
    }

    #[test]
    fn nowait_submission_does_not_wait_for_the_shutdown_fence() {
        let file = TestFile::new();
        let engine = BackendIoEngine::new(file.backend(), 1).unwrap();
        let resources = resources(1);
        let fence = engine
            .inner
            .submit_state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let rejected = engine
            .submit(IoOperation::read(read_buffer(&resources, 1), 0))
            .unwrap_err();
        assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(engine.in_flight(), 0);

        drop(fence);
        engine.shutdown().unwrap();
    }

    #[test]
    fn backend_workers_execute_independent_reads_concurrently() {
        let backend = Arc::new(BlockingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 2).unwrap();
        let resources = resources(1);
        let first = engine.read_exact_at(read_buffer(&resources, 1), 0).unwrap();
        let second = engine.read_exact_at(read_buffer(&resources, 1), 1).unwrap();

        let both_entered = backend.wait_for_entered(2);
        backend.release();
        assert!(matches!(first.wait().status, CompletionStatus::Completed));
        assert!(matches!(second.wait().status, CompletionStatus::Completed));
        assert!(
            both_entered,
            "both workers must enter before either is released"
        );
        assert_eq!(backend.maximum_active(), 2);
        engine.shutdown().unwrap();
    }

    #[test]
    fn submit_wait_blocks_at_the_io_concurrency_limit_and_resumes() {
        let backend = Arc::new(BlockingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = resources(1);
        let first = engine.read_exact_at(read_buffer(&resources, 1), 0).unwrap();
        assert!(backend.wait_for_entered(1));

        let waiting_engine = engine.clone();
        let waiting_buffer = read_buffer(&resources, 1);
        let rejected = engine
            .submit(IoOperation::read(waiting_buffer, 1))
            .unwrap_err();
        assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);
        let (_, waiting_operation) = rejected.into_parts();
        let (sender, receiver) = mpsc::sync_channel(1);
        let submitter = std::thread::spawn(move || {
            sender
                .send(waiting_engine.submit_wait(waiting_operation))
                .unwrap();
        });
        let early = receiver.recv_timeout(Duration::from_millis(30));
        let was_blocked = matches!(&early, Err(mpsc::RecvTimeoutError::Timeout));

        backend.release();
        assert!(matches!(first.wait().status, CompletionStatus::Completed));
        let second = match early {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                receiver.recv_timeout(Duration::from_secs(1)).unwrap()
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => panic!("submitter disconnected"),
        }
        .unwrap();
        assert!(matches!(second.wait().status, CompletionStatus::Completed));
        submitter.join().unwrap();
        assert!(was_blocked);
        assert_eq!(engine.stats().requests_in_flight_peak, 1);
        engine.shutdown().unwrap();
    }

    #[test]
    fn controlled_slot_wait_observes_cancel_wake_and_absolute_deadline() {
        let backend = Arc::new(BlockingBackend::default());
        let engine = BackendIoEngine::new(backend.clone(), 1).unwrap();
        let resources = resources(1);
        let first = engine.read_exact_at(read_buffer(&resources, 1), 0).unwrap();
        assert!(backend.wait_for_entered(1));

        let cancelled = Arc::new(AtomicBool::new(false));
        let waiting_engine = engine.clone();
        let waiting_cancelled = Arc::clone(&cancelled);
        let waiting_operation = IoOperation::read(read_buffer(&resources, 1), 1);
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (result_sender, result_receiver) = mpsc::sync_channel(1);
        let submitter = std::thread::spawn(move || {
            started_sender.send(()).unwrap();
            result_sender
                .send(waiting_engine.submit_wait_controlled(
                    waiting_operation,
                    waiting_cancelled.as_ref(),
                    None,
                ))
                .unwrap();
        });
        started_receiver.recv().unwrap();
        assert!(matches!(
            result_receiver.recv_timeout(Duration::from_millis(30)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        cancelled.store(true, Ordering::Release);
        engine.wake_slot_waiters();
        let cancelled_error = result_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap_err();
        assert_eq!(cancelled_error.error.kind(), io::ErrorKind::Interrupted);
        drop(cancelled_error);
        submitter.join().unwrap();

        let deadline_cancelled = AtomicBool::new(false);
        let timed_out = engine
            .submit_wait_controlled(
                IoOperation::read(read_buffer(&resources, 1), 2),
                &deadline_cancelled,
                Some(Instant::now()),
            )
            .unwrap_err();
        assert_eq!(timed_out.error.kind(), io::ErrorKind::TimedOut);
        drop(timed_out);

        backend.release();
        assert!(matches!(first.wait().status, CompletionStatus::Completed));
        engine.shutdown().unwrap();
    }

    #[test]
    fn backend_panic_completes_the_request_and_worker_survives() {
        let engine = BackendIoEngine::new(Arc::new(PanicOnceBackend::new()), 1).unwrap();
        let resources = resources(1);
        let failed = engine
            .read_exact_at(read_buffer(&resources, 1), 0)
            .unwrap()
            .wait();
        let (result, lease) = failed.into_lease();
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
        assert!(lease.is_some(), "failed completion must return its lease");
        drop(lease);

        let succeeded = engine
            .read_exact_at(read_buffer(&resources, 1), 1)
            .unwrap()
            .wait();
        assert!(matches!(succeeded.status, CompletionStatus::Completed));
        let stats = engine.stats();
        assert_eq!(stats.submitted, 2);
        assert_eq!(stats.completed, 2);
        assert_eq!(stats.errors, 1);
        engine.shutdown().unwrap();
    }

    #[test]
    fn quarantined_completion_does_not_return_a_potentially_live_buffer() {
        let shared = Arc::new(RuntimeShared::new(1, true));
        let slot = shared.try_reserve_slot(false).unwrap();
        let request_id = RequestId(1);
        let completion = Arc::new(CompletionState::new());
        shared.submitted.fetch_add(1, Ordering::Relaxed);

        let resources = resources(1);
        let task = Task {
            request_id,
            operation: IoOperation::read(read_buffer(&resources, 1), 0),
            completion: Arc::clone(&completion),
            slot,
            submitted_at: Some(Instant::now()),
        };
        shared.finish_quarantined(
            task,
            CompletionStatus::Failed(io::Error::other("uncertain kernel lifetime")),
            0,
        );

        let completed = completion.wait();
        assert!(matches!(completed.status, CompletionStatus::Failed(_)));
        assert!(completed.buffer.is_none());
        assert_eq!(shared.snapshot().requests_in_flight, 0);

        // The uncertain buffer remains charged instead of being reused while
        // the kernel may still own its address.
        assert_eq!(
            resources.managed_memory_snapshot().current_bytes,
            aligned_buffer_capacity(1).unwrap()
        );
        assert!(resources.try_read_buffer(1).is_some());
    }
}
