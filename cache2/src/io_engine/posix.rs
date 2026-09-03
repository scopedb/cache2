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

use super::*;

/// Reference engine: a small fixed worker pool executes exact operations
/// through the existing fault-injectable positioned-I/O backend.
#[derive(Clone)]
pub(crate) struct BackendIoEngine {
    pub(super) inner: Arc<RuntimeInner>,
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
        read_wait_enabled: bool,
    ) -> io::Result<Self> {
        files.set_statistics_enabled(statistics_enabled);
        let backend: Arc<dyn IoBackend> = Arc::new(RuntimeFileBackend::new(files));
        Self::new_with_workers_and_statistics(
            backend,
            max_in_flight,
            worker_count,
            statistics_enabled,
            read_wait_enabled,
        )
    }

    #[cfg(test)]
    pub(crate) fn new(backend: Arc<dyn IoBackend>, max_in_flight: usize) -> io::Result<Self> {
        Self::new_with_workers(backend, max_in_flight, max_in_flight.min(4))
    }

    #[cfg(test)]
    pub(crate) fn new_with_read_wait(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
    ) -> io::Result<Self> {
        Self::new_with_workers_and_statistics(
            backend,
            max_in_flight,
            max_in_flight.min(4),
            true,
            true,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_workers(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
        worker_count: usize,
    ) -> io::Result<Self> {
        Self::new_with_workers_and_statistics(backend, max_in_flight, worker_count, true, false)
    }

    pub(super) fn new_with_workers_and_statistics(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
        worker_count: usize,
        statistics_enabled: bool,
        read_wait_enabled: bool,
    ) -> io::Result<Self> {
        RuntimeInner::validate_max_in_flight(max_in_flight)?;
        if worker_count == 0 || worker_count > max_in_flight {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "POSIX I/O worker count must not exceed the request limit",
            ));
        }
        let shared = Arc::new(RuntimeShared::new(
            max_in_flight,
            statistics_enabled,
            read_wait_enabled,
        ));
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
                .name(format!("cache2-sync-io-{worker_index}"))
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

    fn read_slot_waiter(&self) -> ReadSlotWaiter {
        self.inner.read_slot_waiter()
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
        self.backend.runtime_io_stats().direct_active
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

    fn stats(&self) -> EngineIoSnapshot {
        EngineIoSnapshot {
            requests: self.inner.shared.snapshot(),
            runtime: self.backend.runtime_io_stats(),
        }
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
    }
}

fn backend_result(result: io::Result<()>, transferred: usize) -> (CompletionStatus, usize) {
    match result {
        Ok(()) => (CompletionStatus::Completed, transferred),
        Err(error) => (CompletionStatus::Failed(error), transferred),
    }
}
