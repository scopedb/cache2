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

use std::io;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::Ordering;
use std::sync::mpsc::Receiver;

use crate::io::backend::IoBackend;
#[cfg(unix)]
use crate::io::backend::RuntimeFileBackend;
#[cfg(unix)]
use crate::io::backend::RuntimeFileSet;
use crate::io::backend::RuntimeIoStatsHandle;
use crate::io::backend::read_exact_at_uninit_with_progress;
use crate::io::backend::write_all_at_with_progress;
use crate::io::engine::CompletionStatus;
use crate::io::engine::DriverCommand;
use crate::io::engine::IoEngine;
use crate::io::engine::IoOperation;
use crate::io::engine::RuntimeShared;
use crate::io::engine::lock_unpoisoned;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;

#[cfg(unix)]
pub fn start(
    files: RuntimeFileSet,
    max_in_flight: usize,
    worker_count: usize,
    activity_counters_enabled: bool,
    read_wait_enabled: bool,
) -> io::Result<IoEngine> {
    let io_stats = files.stats_handle();
    let backend = Arc::new(RuntimeFileBackend::new(files));
    start_backend(
        backend,
        io_stats,
        max_in_flight,
        worker_count,
        activity_counters_enabled,
        read_wait_enabled,
    )
}

#[cfg(test)]
impl IoEngine {
    pub fn for_test(backend: Arc<dyn IoBackend>, max_in_flight: usize) -> io::Result<Self> {
        Self::for_test_with_options(backend, max_in_flight, max_in_flight.min(4), true, false)
    }

    pub fn for_test_with_read_wait(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
    ) -> io::Result<Self> {
        Self::for_test_with_options(backend, max_in_flight, max_in_flight.min(4), true, true)
    }

    pub fn for_test_with_options(
        backend: Arc<dyn IoBackend>,
        max_in_flight: usize,
        worker_count: usize,
        activity_counters_enabled: bool,
        read_wait_enabled: bool,
    ) -> io::Result<Self> {
        start_backend(
            backend,
            RuntimeIoStatsHandle::new(false),
            max_in_flight,
            worker_count,
            activity_counters_enabled,
            read_wait_enabled,
        )
    }
}

fn start_backend(
    backend: Arc<dyn IoBackend>,
    io_stats: RuntimeIoStatsHandle,
    max_in_flight: usize,
    worker_count: usize,
    activity_counters_enabled: bool,
    read_wait_enabled: bool,
) -> io::Result<IoEngine> {
    let (mut engine, receiver) = IoEngine::with_command_channel(
        max_in_flight,
        activity_counters_enabled,
        read_wait_enabled,
        io_stats,
    )?;
    if worker_count == 0 || worker_count > max_in_flight {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "POSIX I/O worker count must not exceed the request limit",
        ));
    }
    let receiver = Arc::new(Mutex::new(receiver));
    engine
        .workers
        .get_mut()
        .unwrap()
        .reserve_exact(worker_count);
    for worker_index in 0..worker_count {
        let worker_backend = Arc::clone(&backend);
        let worker_shared = Arc::clone(&engine.shared);
        let worker_receiver = Arc::clone(&receiver);
        let worker = std::thread::Builder::new()
            .name(format!("cache2-sync-io-{worker_index}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || backend_driver(worker_backend, worker_shared, worker_receiver))?;
        // Retain each worker immediately so engine Drop joins it if a later spawn fails.
        engine.workers.get_mut().unwrap().push(worker);
    }
    Ok(engine)
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
                let (status, transferred) = panic::catch_unwind(AssertUnwindSafe(|| {
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
