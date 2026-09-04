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

//! Owned-buffer I/O runtime.
//!
//! Requests transfer ownership of their aligned buffer to the engine. The
//! buffer is returned only with the target operation's completion, which is
//! the lifetime rule required by both positioned I/O workers and `io_uring`.

use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock, TryLockError};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tokio::sync::{
    OwnedSemaphorePermit as TokioSemaphorePermit, Semaphore as TokioSemaphore, TryAcquireError,
};

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
use crate::io_backend::RuntimeIoStatsHandle;
use crate::io_backend::{
    IoBackend, RuntimeIoStats, WritePoint, read_exact_at_uninit_with_progress,
    write_all_at_with_progress,
};
#[cfg(unix)]
use crate::io_backend::{RuntimeFileBackend, RuntimeFileSet};
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
use crate::io_backend::{RuntimeIoDirection, RuntimeIoPath};
use crate::resources::{BufferLease, CACHE_THREAD_STACK_BYTES};
#[cfg(unix)]
use crate::runtime_config::{IoEngine as ConfiguredIoEngine, IoUringPoolConfig};
use crate::snapshot::CacheIoDirectionSnapshot;

pub(crate) const IO_BUFFER_ALIGNMENT: usize = 4096;
pub(crate) const MAX_IO_REQUESTS_PER_ENGINE: usize = 4096;
/// A stalled cache-device operation must not hold a frontend or shutdown
/// barrier forever. This is intentionally a fixed production guardrail rather
/// than a durability knob: cache contents are disposable.
pub(crate) const CACHE_IO_COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);
/// Cancellation is only a request. Give the target operation a short window to
/// publish its own completion, which is the actual buffer-lifetime fence.
pub(crate) const CACHE_IO_CANCEL_GRACE: Duration = Duration::from_millis(100);

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct EngineIoSnapshot {
    pub(crate) requests: CacheIoDirectionSnapshot,
    pub(crate) runtime: RuntimeIoStats,
}

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
}

impl OperationKind {
    const fn uses_write_slot(self) -> bool {
        matches!(self, Self::Write)
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
    const fn io_direction(self) -> RuntimeIoDirection {
        match self {
            Self::Read => RuntimeIoDirection::Read,
            Self::Write => RuntimeIoDirection::Write,
        }
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

    pub(crate) const fn kind(&self) -> OperationKind {
        match self {
            Self::Read { .. } => OperationKind::Read,
            Self::Write { .. } => OperationKind::Write,
        }
    }

    fn validate(&self) -> io::Result<()> {
        let (length, offset) = match self {
            Self::Read { buffer, offset } | Self::Write { buffer, offset, .. } => {
                (buffer.len(), *offset)
            }
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
            Self::Write { .. } => Ok(()),
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
    waker: Option<Waker>,
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
                waker: None,
            }),
            ready: Condvar::new(),
            cancel_requested: AtomicBool::new(false),
            cancel_notified: AtomicBool::new(false),
        }
    }

    fn complete(&self, completion: IoCompletion) {
        let waker = {
            let mut cell = lock_unpoisoned(&self.cell);
            if !cell.consumer_alive {
                drop(cell);
                drop(completion);
                return;
            }
            debug_assert!(cell.completion.is_none());
            cell.completion = Some(completion);
            cell.waker.take()
        };
        self.ready.notify_all();
        if let Some(waker) = waker {
            waker.wake();
        }
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
            cell.waker = None;
            cell.completion.take()
        };
        drop(completion);
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<IoCompletion> {
        let mut cell = lock_unpoisoned(&self.cell);
        if let Some(completion) = cell.completion.take() {
            return Poll::Ready(completion);
        }
        debug_assert!(cell.consumer_alive);
        if cell
            .waker
            .as_ref()
            .is_none_or(|waker| !waker.will_wake(context.waker()))
        {
            cell.waker = Some(context.waker().clone());
        }
        Poll::Pending
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

impl Future for IoRequest {
    type Output = IoCompletion;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let request = self.get_mut();
        match request.completion.poll(context) {
            Poll::Ready(completion) => {
                request.finished = true;
                Poll::Ready(completion)
            }
            Poll::Pending => Poll::Pending,
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
    stop_engine_on_deadline: bool,
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
                // operation and owned buffer in the driver. A stuck read keeps
                // only its own bounded slot; other read workers may continue.
                // A write timeout stops its engine so close can retain the
                // exact unfenced-write boundary.
                if self.stop_engine_on_deadline {
                    engine.stop_accepting_requests();
                }
                drop(request);
                Err(IoDeadlineExceeded::new(cancel_error, None))
            }
        }
    }

    pub(crate) async fn wait_async(
        self,
        engine: Arc<dyn IoEngine>,
        tokio_handle: &tokio::runtime::Handle,
    ) -> Result<IoCompletion, IoDeadlineExceeded> {
        let mut request = AsyncRequestGuard::new(self.request, engine);
        let deadline = tokio::time::Instant::from_std(self.deadline);
        let completion = {
            let _entered = tokio_handle.enter();
            tokio::time::timeout_at(deadline, request.request_mut())
        }
        .await;
        if let Ok(completion) = completion {
            request.disarm();
            return Ok(completion);
        }

        let cancel_error = request.cancel().err();
        let completion = {
            let _entered = tokio_handle.enter();
            tokio::time::timeout(self.cancel_grace, request.request_mut())
        }
        .await;
        match completion {
            Ok(completion) => {
                request.disarm();
                Err(IoDeadlineExceeded::new(cancel_error, Some(completion)))
            }
            Err(_) => {
                request.detach(self.stop_engine_on_deadline);
                Err(IoDeadlineExceeded::new(cancel_error, None))
            }
        }
    }
}

struct AsyncRequestGuard {
    request: Option<IoRequest>,
    engine: Arc<dyn IoEngine>,
}

impl AsyncRequestGuard {
    fn new(request: IoRequest, engine: Arc<dyn IoEngine>) -> Self {
        Self {
            request: Some(request),
            engine,
        }
    }

    fn request_mut(&mut self) -> &mut IoRequest {
        self.request.as_mut().expect("async request is armed")
    }

    fn cancel(&self) -> io::Result<bool> {
        let request = self.request.as_ref().expect("async request is armed");
        self.engine
            .cancel(request.id(), request.completion.as_ref())
    }

    fn disarm(&mut self) {
        let request = self.request.take().expect("async request is armed");
        debug_assert!(request.finished);
        drop(request);
    }

    fn detach(&mut self, stop_engine: bool) {
        if stop_engine {
            self.engine.stop_accepting_requests();
        }
        drop(self.request.take());
    }
}

impl Drop for AsyncRequestGuard {
    fn drop(&mut self) {
        if self.request.is_some() {
            let _ = self.cancel();
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
        stop_engine_on_deadline: false,
    })
}

fn submit_cache_io_until(
    engine: &dyn IoEngine,
    operation: IoOperation,
    deadline: Instant,
    cancel_grace: Duration,
) -> Result<BoundedIoRequest, SubmitError> {
    let cancelled = AtomicBool::new(false);
    let stop_engine_on_deadline = operation.kind().uses_write_slot();
    let request = engine.submit_wait_controlled(operation, &cancelled, Some(deadline))?;
    Ok(BoundedIoRequest {
        request,
        deadline,
        cancel_grace,
        stop_engine_on_deadline,
    })
}

pub(crate) trait IoEngine: Send + Sync {
    fn try_reserve_read(&self) -> io::Result<ReadSlot>;
    fn read_slot_waiter(&self) -> ReadSlotWaiter;
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
    /// True means a failed driver could not fence an issued write.
    /// The cache must retain its exclusive file lock for process lifetime.
    fn has_unfenced_writes(&self) -> bool;
    #[cfg(test)]
    fn mark_unfenced_writes_for_test(&self);
    fn stats(&self) -> EngineIoSnapshot;

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
}

struct IoSlot {
    shared: Arc<RuntimeShared>,
    write: bool,
    // This permit drops only after `IoSlot::drop` publishes physical capacity.
    read_permit: Option<TokioSemaphorePermit>,
}

/// One read reservation against the engine's complete depth.
/// Dropping it before submission releases the slot immediately.
pub(crate) struct ReadSlot {
    slot: IoSlot,
}

/// An async reservation handle backed by the engine's physical slot state.
pub(crate) struct ReadSlotWaiter {
    shared: Arc<RuntimeShared>,
}

// The physical completion path must not allocate. Both Tokio and asyncband 0.7
// keep waiter nodes in acquisition futures and wake from fixed-size batches,
// but Tokio also provides the close operation used to wake pending reads during
// engine shutdown. The outer bounded read-wait capacity uses asyncband in
// `region_runtime`.
struct ReadSlotAdmission {
    slots: Arc<TokioSemaphore>,
    waiters: AtomicUsize,
}

impl ReadSlotAdmission {
    fn new(max_in_flight: usize) -> Self {
        Self {
            slots: Arc::new(TokioSemaphore::new(max_in_flight)),
            waiters: AtomicUsize::new(0),
        }
    }

    fn register_waiter(&self) -> ReadWaiterGuard<'_> {
        self.waiters.fetch_add(1, Ordering::AcqRel);
        ReadWaiterGuard { admission: self }
    }

    fn has_waiters(&self) -> bool {
        self.waiters.load(Ordering::Acquire) != 0
    }

    fn try_acquire(&self) -> io::Result<TokioSemaphorePermit> {
        if self.has_waiters() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a queued read has priority",
            ));
        }
        let permit = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|error| match error {
                TryAcquireError::Closed => {
                    io::Error::new(io::ErrorKind::BrokenPipe, "I/O engine is shut down")
                }
                TryAcquireError::NoPermits => {
                    io::Error::new(io::ErrorKind::WouldBlock, "no I/O slot is available")
                }
            })?;
        // Registration happens before an async acquire is polled. Rechecking
        // closes the race with a waiter that arrived during the try-acquire.
        if self.has_waiters() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "a queued read has priority",
            ));
        }
        Ok(permit)
    }

    async fn acquire_until(
        &self,
        deadline: Instant,
        tokio_handle: &tokio::runtime::Handle,
    ) -> io::Result<TokioSemaphorePermit> {
        let acquire = Arc::clone(&self.slots).acquire_owned();
        {
            let _entered = tokio_handle.enter();
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), acquire)
        }
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "L2 read wait deadline expired"))?
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "I/O engine is shut down"))
    }

    fn close(&self) {
        self.slots.close();
    }
}

struct ReadWaiterGuard<'a> {
    admission: &'a ReadSlotAdmission,
}

impl Drop for ReadWaiterGuard<'_> {
    fn drop(&mut self) {
        self.admission.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

impl ReadSlotWaiter {
    pub(crate) async fn reserve_until(
        self,
        deadline: Instant,
        tokio_handle: &tokio::runtime::Handle,
    ) -> io::Result<ReadSlot> {
        let admission = self
            .shared
            .read_slot_admission
            .as_ref()
            .ok_or_else(|| io::Error::other("async read admission is disabled"))?;
        let _waiter = admission.register_waiter();
        self.shared.ensure_accepting()?;
        let permit = admission.acquire_until(deadline, tokio_handle).await?;
        self.shared.try_reserve_read_slot(Some(permit))
    }
}

impl Drop for IoSlot {
    fn drop(&mut self) {
        {
            let _slot = lock_unpoisoned(&self.shared.slot_lock);
            self.shared
                .slot_state
                .fetch_sub(slot_delta(self.write), Ordering::AcqRel);
            self.shared.slot_available.notify_one();
        }
        drop(self.read_permit.take());
    }
}

const SLOT_COUNT_BITS: u32 = 16;
const SLOT_COUNT_MASK: u64 = (1_u64 << SLOT_COUNT_BITS) - 1;
const WRITE_SLOT_SHIFT: u32 = SLOT_COUNT_BITS;

const fn slot_delta(write: bool) -> u64 {
    1 + ((write as u64) << WRITE_SLOT_SHIFT)
}

const fn active_slots(state: u64) -> usize {
    (state & SLOT_COUNT_MASK) as usize
}

const fn active_write_slots(state: u64) -> usize {
    ((state >> WRITE_SLOT_SHIFT) & SLOT_COUNT_MASK) as usize
}

struct RuntimeShared {
    max_in_flight: usize,
    statistics_enabled: bool,
    accepting: AtomicBool,
    /// Packed total and write counts. A single CAS is the slot reservation
    /// linearization point.
    slot_state: AtomicU64,
    in_flight_peak: AtomicUsize,
    requests_submitted: AtomicU64,
    requests_succeeded: AtomicU64,
    requests_cancelled: AtomicU64,
    requests_failed: AtomicU64,
    slot_wait_ns: AtomicU64,
    request_time_ns: AtomicU64,
    cancel_scan_needed: AtomicBool,
    unfenced_writes: AtomicBool,
    slot_lock: Mutex<()>,
    slot_available: Condvar,
    read_slot_admission: Option<ReadSlotAdmission>,
    #[cfg(test)]
    quarantined_buffers: Mutex<Vec<IoBuffer>>,
}

enum SlotWaitError {
    Shutdown,
    Cancelled,
    TimedOut,
}

impl RuntimeShared {
    fn new(max_in_flight: usize, statistics_enabled: bool, read_wait_enabled: bool) -> Self {
        Self {
            max_in_flight,
            statistics_enabled,
            accepting: AtomicBool::new(true),
            slot_state: AtomicU64::new(0),
            in_flight_peak: AtomicUsize::new(0),
            requests_submitted: AtomicU64::new(0),
            requests_succeeded: AtomicU64::new(0),
            requests_cancelled: AtomicU64::new(0),
            requests_failed: AtomicU64::new(0),
            slot_wait_ns: AtomicU64::new(0),
            request_time_ns: AtomicU64::new(0),
            cancel_scan_needed: AtomicBool::new(false),
            unfenced_writes: AtomicBool::new(false),
            slot_lock: Mutex::new(()),
            slot_available: Condvar::new(),
            read_slot_admission: read_wait_enabled.then(|| ReadSlotAdmission::new(max_in_flight)),
            #[cfg(test)]
            quarantined_buffers: Mutex::new(Vec::new()),
        }
    }

    fn total_in_flight(&self) -> usize {
        active_slots(self.slot_state.load(Ordering::Acquire))
    }

    fn writes_in_flight(&self) -> usize {
        active_write_slots(self.slot_state.load(Ordering::Acquire))
    }

    fn ensure_accepting(&self) -> io::Result<()> {
        if self.accepting.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "I/O engine is shut down",
            ))
        }
    }

    fn try_reserve_slot(self: &Arc<Self>, write: bool) -> Option<IoSlot> {
        const MAX_SLOT_CAS_ATTEMPTS: usize = 8;
        let mut current = self.slot_state.load(Ordering::Acquire);
        for _ in 0..MAX_SLOT_CAS_ATTEMPTS {
            let total = active_slots(current);
            if total >= self.max_in_flight {
                return None;
            }
            let next = current + slot_delta(write);
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
                        read_permit: None,
                    });
                }
                Err(observed) => current = observed,
            }
        }
        None
    }

    fn try_reserve_read_slot(
        self: &Arc<Self>,
        permit: Option<TokioSemaphorePermit>,
    ) -> io::Result<ReadSlot> {
        self.ensure_accepting()?;
        let mut slot = self
            .try_reserve_slot(false)
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "no I/O slot is available"))?;
        slot.read_permit = permit;
        Ok(ReadSlot { slot })
    }

    fn stop_accepting_slots(&self) {
        {
            let _slot = lock_unpoisoned(&self.slot_lock);
            self.accepting.store(false, Ordering::Release);
            self.slot_available.notify_all();
        }
        if let Some(admission) = &self.read_slot_admission {
            admission.close();
        }
    }

    #[cfg(test)]
    fn reserve_slot_wait(self: &Arc<Self>, write: bool) -> Option<IoSlot> {
        let mut slot = lock_unpoisoned(&self.slot_lock);
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
            #[cfg(test)]
            // Keep it unreachable until fixture teardown without creating an
            // intentional LeakSanitizer finding.
            lock_unpoisoned(&self.quarantined_buffers).push(buffer);
            #[cfg(not(test))]
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
            match &status {
                CompletionStatus::Completed => {
                    self.requests_succeeded.fetch_add(1, Ordering::Relaxed);
                }
                CompletionStatus::Cancelled => {
                    self.requests_cancelled.fetch_add(1, Ordering::Relaxed);
                }
                CompletionStatus::Failed(_) => {
                    self.requests_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
            if let Some(submitted_at) = submitted_at {
                add_duration_ns(&self.request_time_ns, submitted_at.elapsed());
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

    fn snapshot(&self) -> CacheIoDirectionSnapshot {
        let requests_in_flight = self.total_in_flight();
        CacheIoDirectionSnapshot {
            requests_submitted: self.requests_submitted.load(Ordering::Relaxed),
            requests_succeeded: self.requests_succeeded.load(Ordering::Relaxed),
            requests_cancelled: self.requests_cancelled.load(Ordering::Relaxed),
            requests_failed: self.requests_failed.load(Ordering::Relaxed),
            requests_in_flight: usize_to_u64(requests_in_flight),
            requests_in_flight_peak: usize_to_u64(
                self.in_flight_peak
                    .load(Ordering::Relaxed)
                    .max(requests_in_flight),
            ),
            slot_wait_ns: self.slot_wait_ns.load(Ordering::Relaxed),
            request_time_ns: self.request_time_ns.load(Ordering::Relaxed),
            buffered: Default::default(),
            direct: Default::default(),
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
        if !(1..=MAX_IO_REQUESTS_PER_ENGINE).contains(&max_in_flight) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("I/O requests per engine must be in 1..={MAX_IO_REQUESTS_PER_ENGINE}"),
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
        let permit = self
            .shared
            .read_slot_admission
            .as_ref()
            .map(ReadSlotAdmission::try_acquire)
            .transpose()?;
        self.shared.try_reserve_read_slot(permit)
    }

    fn read_slot_waiter(&self) -> ReadSlotWaiter {
        ReadSlotWaiter {
            shared: Arc::clone(&self.shared),
        }
    }

    fn submit_reserved_read(
        &self,
        slot: ReadSlot,
        operation: IoOperation,
    ) -> Result<IoRequest, SubmitError> {
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
        let request_started = self.shared.statistics_enabled.then(Instant::now);
        self.submit_with_slot(operation, slot.slot, request_started, true)
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
        let slot_wait_started = self.shared.statistics_enabled.then(Instant::now);
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
        if let Some(slot_wait_started) = slot_wait_started {
            add_duration_ns(&self.shared.slot_wait_ns, slot_wait_started.elapsed());
        }

        let request_started = self.shared.statistics_enabled.then(Instant::now);
        self.submit_with_slot(operation, slot, request_started, nonblocking)
    }

    fn submit_with_slot(
        &self,
        operation: IoOperation,
        slot: IoSlot,
        request_started: Option<Instant>,
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
            submitted_at: request_started,
        };
        if self.shared.statistics_enabled {
            self.shared
                .requests_submitted
                .fetch_add(1, Ordering::Release);
        }
        match self.commands.try_send(DriverCommand::Submit(task)) {
            Ok(()) => {
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
                    self.shared
                        .requests_submitted
                        .fetch_sub(1, Ordering::Relaxed);
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
                    self.shared
                        .requests_submitted
                        .fetch_sub(1, Ordering::Relaxed);
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
        state.cancel_requested.store(true, Ordering::Release);
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
        self.shared.stop_accepting_slots();
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
            self.shared.stop_accepting_slots();
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

mod posix;
pub(crate) use posix::BackendIoEngine;

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
mod uring;

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

#[cfg(unix)]
pub(crate) fn build_file_engine(
    files: RuntimeFileSet,
    max_in_flight: usize,
    posix_workers: usize,
    kind: ConfiguredIoEngine,
    io_uring_config: Option<IoUringPoolConfig>,
    statistics_enabled: bool,
    read_wait_enabled: bool,
) -> io::Result<Arc<dyn IoEngine>> {
    match kind {
        ConfiguredIoEngine::Posix(_) => BackendIoEngine::new_with_files_and_workers(
            files,
            max_in_flight,
            posix_workers,
            statistics_enabled,
            read_wait_enabled,
        )
        .map(|engine| Arc::new(engine) as Arc<dyn IoEngine>),
        ConfiguredIoEngine::IoUring(_) => {
            let _ = posix_workers;
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
                let io_uring_config = io_uring_config.ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "io_uring pool configuration is missing",
                    )
                })?;
                UringIoEngine::new_with_files(
                    files,
                    max_in_flight,
                    io_uring_config,
                    statistics_enabled,
                    read_wait_enabled,
                )
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
                let _ = io_uring_config;
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
mod tests;
