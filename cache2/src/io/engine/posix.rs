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

use crate::io::engine::CompletionStatus;
use crate::io::engine::DriverCommand;
use crate::io::engine::EngineState;
use crate::io::engine::IoEngine;
use crate::io::engine::IoOperation;
use crate::io::engine::lock_unpoisoned;
#[cfg(unix)]
#[cfg(unix)]
use crate::io::file::DataFileHandles;
use crate::io::file::FileIoStatsHandle;
use crate::io::file::PositionedIo;
use crate::io::file::read_exact_at_uninit_with_progress;
use crate::io::file::write_all_at_with_progress;
use crate::managed_memory::CACHE_THREAD_STACK_BYTES;

#[cfg(unix)]
pub fn start(
    handles: DataFileHandles,
    max_in_flight: usize,
    worker_count: usize,
    activity_counters_enabled: bool,
    read_wait_enabled: bool,
) -> io::Result<IoEngine> {
    let io_stats = handles.stats_handle();
    let io = Arc::new(handles);
    start_workers(
        io,
        io_stats,
        max_in_flight,
        worker_count,
        activity_counters_enabled,
        read_wait_enabled,
    )
}

#[cfg(test)]
impl IoEngine {
    pub fn for_test(io: Arc<dyn PositionedIo>, max_in_flight: usize) -> io::Result<Self> {
        Self::for_test_with_options(io, max_in_flight, max_in_flight.min(4), true, false)
    }

    pub fn for_test_with_read_wait(
        io: Arc<dyn PositionedIo>,
        max_in_flight: usize,
    ) -> io::Result<Self> {
        Self::for_test_with_options(io, max_in_flight, max_in_flight.min(4), true, true)
    }

    pub fn for_test_with_options(
        io: Arc<dyn PositionedIo>,
        max_in_flight: usize,
        worker_count: usize,
        activity_counters_enabled: bool,
        read_wait_enabled: bool,
    ) -> io::Result<Self> {
        start_workers(
            io,
            FileIoStatsHandle::new(false),
            max_in_flight,
            worker_count,
            activity_counters_enabled,
            read_wait_enabled,
        )
    }
}

fn start_workers(
    io: Arc<dyn PositionedIo>,
    io_stats: FileIoStatsHandle,
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
        let worker_io = Arc::clone(&io);
        let worker_state = Arc::clone(&engine.state);
        let worker_receiver = Arc::clone(&receiver);
        let worker = std::thread::Builder::new()
            .name(format!("cache2-sync-io-{worker_index}"))
            .stack_size(CACHE_THREAD_STACK_BYTES)
            .spawn(move || posix_worker(worker_io, worker_state, worker_receiver))?;
        // Retain each worker immediately so engine Drop joins it if a later spawn fails.
        engine.workers.get_mut().unwrap().push(worker);
    }
    Ok(engine)
}

fn posix_worker(
    io: Arc<dyn PositionedIo>,
    state: Arc<EngineState>,
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
                    state.finish(task, CompletionStatus::Cancelled, 0);
                    continue;
                }
                let (status, transferred) = panic::catch_unwind(AssertUnwindSafe(|| {
                    execute_operation(io.as_ref(), &mut task.operation)
                }))
                .unwrap_or_else(|_| {
                    (
                        CompletionStatus::Failed(io::Error::other("positioned I/O panicked")),
                        0,
                    )
                });
                state.finish(task, status, transferred);
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

fn execute_operation(
    io: &dyn PositionedIo,
    operation: &mut IoOperation,
) -> (CompletionStatus, usize) {
    match operation {
        IoOperation::Read { buffer, offset } => match buffer.read_target() {
            Ok(buffer_pointer) => {
                let (result, transferred) =
                    read_exact_at_uninit_with_progress(io, buffer_pointer, buffer.len(), *offset);
                completion_result(result, transferred)
            }
            Err(error) => completion_result(Err(error), 0),
        },
        IoOperation::Write {
            point,
            buffer,
            offset,
        } => match buffer.as_slice() {
            Ok(buffer) => {
                let (result, transferred) = write_all_at_with_progress(io, *point, buffer, *offset);
                completion_result(result, transferred)
            }
            Err(error) => completion_result(Err(error), 0),
        },
    }
}

fn completion_result(result: io::Result<()>, transferred: usize) -> (CompletionStatus, usize) {
    match result {
        Ok(()) => (CompletionStatus::Completed, transferred),
        Err(error) => (CompletionStatus::Failed(error), transferred),
    }
}
