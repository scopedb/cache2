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

use std::sync::mpsc;
use std::time::Duration;

use super::*;
use crate::IoOutcome;
use crate::IoRole;
use crate::StatsOptions;
use crate::fixtures::TestFile;
use crate::io::background::BackgroundIoAttempt;
use crate::io::background::IoRecovery;
use crate::io::file::PositionedIo;
use crate::managed_memory::ManagedMemory;
use crate::managed_memory::ManagedMemoryLimits;
use crate::managed_memory::aligned_buffer_capacity;
use crate::stats::recording::Recorder;

async fn wait_for_registered_read_waiters(engine: &IoEngine, expected: usize) {
    for _ in 0..100 {
        let actual = engine
            .state
            .read_slot_admission
            .as_ref()
            .map_or(0, |admission| admission.waiters.load(Ordering::Acquire));
        if actual == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("expected {expected} registered read waiters");
}

async fn spawn_registered_read_slot_waiter(
    engine: &IoEngine,
    timeout: Duration,
    expected_waiters: usize,
) -> tokio::task::JoinHandle<io::Result<ReadSlot>> {
    let slot_waiter = engine.read_slot_waiter();
    let waiter = tokio::spawn(async move {
        slot_waiter
            .reserve_until(Instant::now() + timeout, &tokio::runtime::Handle::current())
            .await
    });
    wait_for_registered_read_waiters(engine, expected_waiters).await;
    waiter
}

async fn read_wait_error(waiter: tokio::task::JoinHandle<io::Result<ReadSlot>>) -> io::Error {
    match waiter.await.unwrap() {
        Ok(_) => panic!("read waiter unexpectedly reserved a slot"),
        Err(error) => error,
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
struct BlockingIo {
    state: Mutex<BlockingState>,
    changed: Condvar,
}

impl BlockingIo {
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

impl PositionedIo for BlockingIo {
    fn read_at(
        &self,
        buffer: &mut [u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        self.enter_and_wait();
        buffer.fill(0);
        Ok(buffer.len())
    }

    fn write_at(
        &self,
        #[expect(unused_variables)] point: WritePoint,
        buffer: &[u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        self.enter_and_wait();
        Ok(buffer.len())
    }
}

struct PanicOnceIo {
    panic_next_read: AtomicBool,
}

#[derive(Default)]
struct ShortThenErrorIo {
    read_calls: AtomicUsize,
    write_calls: AtomicUsize,
}

impl PanicOnceIo {
    fn new() -> Self {
        Self {
            panic_next_read: AtomicBool::new(true),
        }
    }
}

impl PositionedIo for PanicOnceIo {
    fn read_at(
        &self,
        buffer: &mut [u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        if self.panic_next_read.swap(false, Ordering::AcqRel) {
            panic!("injected io panic");
        }
        buffer.fill(0);
        Ok(buffer.len())
    }

    fn write_at(
        &self,
        #[expect(unused_variables)] point: WritePoint,
        buffer: &[u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        Ok(buffer.len())
    }
}

impl PositionedIo for ShortThenErrorIo {
    fn read_at(
        &self,
        buffer: &mut [u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        if self.read_calls.fetch_add(1, Ordering::Relaxed) == 0 {
            let transferred = 3.min(buffer.len());
            buffer[..transferred].fill(0x5a);
            Ok(transferred)
        } else {
            Err(io::Error::from_raw_os_error(5))
        }
    }

    fn write_at(
        &self,
        #[expect(unused_variables)] point: WritePoint,
        buffer: &[u8],
        #[expect(unused_variables)] offset: u64,
    ) -> io::Result<usize> {
        if self.write_calls.fetch_add(1, Ordering::Relaxed) == 0 {
            Ok(3.min(buffer.len()))
        } else {
            Err(io::Error::from_raw_os_error(5))
        }
    }
}

fn managed_memory() -> Arc<ManagedMemory> {
    Arc::new(
        ManagedMemory::try_new(ManagedMemoryLimits {
            memory_limit_bytes: 1024 * 1024,
            reserved_memory_bytes: 0,
        })
        .unwrap(),
    )
}

fn read_buffer(managed_memory: &Arc<ManagedMemory>, length: usize) -> IoBuffer {
    let lease = managed_memory.try_read_buffer(length).unwrap();
    IoBuffer::for_read(lease, length).unwrap()
}

fn write_buffer(managed_memory: &Arc<ManagedMemory>, bytes: &[u8]) -> IoBuffer {
    let mut lease = managed_memory.try_read_buffer(bytes.len()).unwrap();
    let target = lease.read_target(bytes.len()).unwrap();
    // SAFETY: the exclusively owned target fits the source, and the allocations
    // do not overlap. Publish the initialized range only after copying it.
    unsafe { target.copy_from_nonoverlapping(bytes.as_ptr(), bytes.len()) };
    lease.mark_initialized(bytes.len()).unwrap();
    IoBuffer::for_write(lease, bytes.len()).unwrap()
}

#[test]
fn aligned_buffer_has_stable_alignment() {
    let managed_memory = managed_memory();
    let buffer = read_buffer(&managed_memory, 8193);
    assert_eq!(
        buffer.read_target().unwrap() as usize % IO_BUFFER_ALIGNMENT,
        0
    );
    assert_eq!(buffer.len(), 8193);
}

#[test]
fn posix_engine_round_trips_owned_buffers_and_drains() {
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test(file.io(), 4).unwrap();
    let managed_memory = managed_memory();
    let input = b"owned async positioned I/O";
    let write = engine
        .write_all_at(
            WritePoint::Record,
            write_buffer(&managed_memory, input),
            4096,
        )
        .unwrap()
        .wait();
    assert!(matches!(write.status, CompletionStatus::Completed));
    assert_eq!(write.bytes_transferred, input.len());

    let read = engine
        .read_exact_at(read_buffer(&managed_memory, input.len()), 4096)
        .unwrap()
        .wait();
    assert!(matches!(read.status, CompletionStatus::Completed));
    assert_eq!(read.buffer.unwrap().as_slice().unwrap(), input);
    engine.shutdown().unwrap();
    assert_eq!(engine.in_flight(), 0);
    let stats = engine.stats();
    assert_eq!(stats.requests.requests_submitted, 2);
    assert_eq!(stats.requests.requests_succeeded, 2);
    assert_eq!(stats.requests.requests_failed, 0);
    assert!(stats.requests.requests_in_flight_peak >= 1);
    assert!(
        engine
            .read_exact_at(read_buffer(&managed_memory, input.len()), 4096)
            .unwrap_err()
            .error
            .kind()
            .eq(&io::ErrorKind::BrokenPipe)
    );
}

#[test]
fn posix_engine_reports_progress_before_a_terminal_short_io_error() {
    let engine = IoEngine::for_test(Arc::new(ShortThenErrorIo::default()), 2).unwrap();
    let managed_memory = managed_memory();

    let read = engine
        .read_exact_at(read_buffer(&managed_memory, 8), 0)
        .unwrap()
        .wait();
    assert!(matches!(read.status, CompletionStatus::Failed(_)));
    assert_eq!(read.bytes_transferred, 3);

    let write = engine
        .write_all_at(
            WritePoint::Record,
            write_buffer(&managed_memory, &[0x33; 8]),
            0,
        )
        .unwrap()
        .wait();
    assert!(matches!(write.status, CompletionStatus::Failed(_)));
    assert_eq!(write.bytes_transferred, 3);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn async_request_is_woken_by_driver_completion() {
    let file = TestFile::new("io-engine");
    file.open().set_len(4096).unwrap();
    let engine = Arc::new(IoEngine::for_test(file.io(), 2).unwrap());
    let managed_memory = managed_memory();
    let request = submit_cache_io(
        engine.as_ref(),
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
    )
    .unwrap();

    let completion = request
        .wait_async(Arc::clone(&engine), &tokio::runtime::Handle::current())
        .await
        .unwrap();

    assert!(matches!(completion.status, CompletionStatus::Completed));
    assert_eq!(completion.bytes_transferred, 4096);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn dropping_async_wait_requests_bounded_cancellation() {
    let io = Arc::new(BlockingIo::default());
    let engine = Arc::new(IoEngine::for_test(io.clone(), 1).unwrap());
    let recorder = Arc::new(
        Recorder::new(StatsOptions {
            io_latency: true,
            ..StatsOptions::default()
        })
        .unwrap(),
    );
    engine.set_latency_recorder(recorder.io_timing(IoRole::Read).unwrap());
    let managed_memory = managed_memory();
    let request = submit_cache_io(
        engine.as_ref(),
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
    )
    .unwrap();
    let waiter_engine = Arc::clone(&engine);
    let waiter = tokio::spawn(async move {
        request
            .wait_async(waiter_engine, &tokio::runtime::Handle::current())
            .await
    });
    tokio::task::yield_now().await;
    assert!(io.wait_for_entered(1));

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        recorder
            .io_snapshot()
            .iter()
            .all(|row| row.latency.count == 0)
    );
    io.release();
    engine.shutdown().unwrap();
    assert_eq!(engine.in_flight(), 0);
    assert_eq!(
        recorder
            .io_snapshot()
            .iter()
            .map(|row| row.latency.count)
            .sum::<u64>(),
        1
    );
}

#[tokio::test]
async fn reserved_read_latency_includes_time_before_submission() {
    for activity_counters_enabled in [false, true] {
        let file = TestFile::new("io-engine");
        file.open().set_len(4096).unwrap();
        let engine =
            IoEngine::for_test_with_options(file.io(), 1, 1, activity_counters_enabled, true)
                .unwrap();
        let recorder = Arc::new(
            Recorder::new(StatsOptions {
                io_latency: true,
                ..StatsOptions::default()
            })
            .unwrap(),
        );
        engine.set_latency_recorder(recorder.io_timing(IoRole::Read).unwrap());
        drop(engine.try_reserve_read().unwrap());
        let slot = engine.try_reserve_read().unwrap();
        let reserved_at = slot.reserved_at.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        let before_submit = reserved_at.elapsed();
        assert!(
            recorder
                .io_snapshot()
                .iter()
                .all(|row| row.latency.count == 0)
        );
        let completion = submit_cache_read(
            &engine,
            slot,
            IoOperation::read(read_buffer(&managed_memory(), 4096), 0),
        )
        .unwrap()
        .wait(&engine)
        .unwrap();
        assert!(matches!(completion.status, CompletionStatus::Completed));
        engine.shutdown().unwrap();
        let snapshots = recorder.io_snapshot();
        let completed = snapshots
            .iter()
            .find(|row| row.role == IoRole::Read && row.outcome == IoOutcome::Completed)
            .unwrap();
        assert_eq!(completed.latency.count, 1);
        assert!(completed.latency.sum_ns >= before_submit.as_nanos());
        if activity_counters_enabled {
            assert!(
                u128::from(engine.stats().requests.request_time_ns) >= before_submit.as_nanos()
            );
        }
    }
}

#[tokio::test]
async fn read_slot_waits_for_cancelled_request_to_release_physical_capacity() {
    let io = Arc::new(BlockingIo::default());
    let engine = Arc::new(IoEngine::for_test_with_read_wait(io.clone(), 1).unwrap());
    let managed_memory = managed_memory();
    let slot = engine.try_reserve_read().unwrap();
    let request = submit_cache_read(
        engine.as_ref(),
        slot,
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
    )
    .unwrap();
    let request_engine = Arc::clone(&engine);
    let request_waiter = tokio::spawn(async move {
        request
            .wait_async(request_engine, &tokio::runtime::Handle::current())
            .await
    });
    tokio::task::yield_now().await;
    assert!(io.wait_for_entered(1));

    request_waiter.abort();
    assert!(request_waiter.await.unwrap_err().is_cancelled());
    assert_eq!(engine.in_flight(), 1);

    let slot_waiter = engine.read_slot_waiter();
    let deadline = Instant::now() + Duration::from_secs(1);
    let tokio_handle = tokio::runtime::Handle::current();
    let mut reservation = Box::pin(slot_waiter.reserve_until(deadline, &tokio_handle));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), reservation.as_mut())
            .await
            .is_err(),
        "caller cancellation must not publish physical capacity"
    );

    io.release();
    let slot = reservation.await.unwrap();
    drop(slot);
    engine.shutdown().unwrap();
    assert_eq!(engine.in_flight(), 0);
}

#[tokio::test]
async fn read_slot_wait_is_woken_by_engine_shutdown() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 1).unwrap());
    let slot = engine.try_reserve_read().unwrap();
    let mut waiters = Vec::new();
    for expected in 1..=3 {
        waiters.push(
            spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), expected).await,
        );
    }

    engine.stop_accepting_requests();
    for waiter in waiters {
        assert_eq!(
            read_wait_error(waiter).await.kind(),
            io::ErrorKind::BrokenPipe
        );
    }
    drop(slot);
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn queued_read_reservation_precedes_new_immediate_read() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 1).unwrap());
    let held = engine.try_reserve_read().unwrap();
    let queued = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 1).await;

    drop(held);
    let error = match engine.try_reserve_read() {
        Ok(_) => panic!("a new immediate read must not bypass a queued read"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

    let slot = queued.await.unwrap().unwrap();
    drop(slot);
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn queued_read_reservations_are_fifo() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 1).unwrap());
    let held = engine.try_reserve_read().unwrap();

    let first = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 1).await;
    let second = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 2).await;

    drop(held);
    let first_slot = first.await.unwrap().unwrap();
    tokio::task::yield_now().await;
    assert!(
        !second.is_finished(),
        "the second waiter bypassed the first"
    );

    drop(first_slot);
    let second_slot = second.await.unwrap().unwrap();
    drop(second_slot);
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn queued_reads_use_every_released_engine_slot() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 2).unwrap());
    let held: Vec<_> = (0..2).map(|_| engine.try_reserve_read().unwrap()).collect();
    let first = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 1).await;
    let second = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 2).await;

    drop(held);
    let first_slot = first.await.unwrap().unwrap();
    let second_slot = tokio::time::timeout(Duration::from_millis(20), second)
        .await
        .expect("an idle second engine slot was blocked by the queue head")
        .unwrap()
        .unwrap();
    drop((first_slot, second_slot));
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_queue_head_passes_priority_to_next_read() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 1).unwrap());
    let held = engine.try_reserve_read().unwrap();

    let first = spawn_registered_read_slot_waiter(&engine, Duration::from_millis(20), 1).await;
    let second = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 2).await;

    assert_eq!(read_wait_error(first).await.kind(), io::ErrorKind::TimedOut);
    wait_for_registered_read_waiters(&engine, 1).await;

    drop(held);
    let second_slot = second.await.unwrap().unwrap();
    drop(second_slot);
    engine.shutdown().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_queue_head_passes_priority_to_next_read() {
    let file = TestFile::new("io-engine");
    let engine = Arc::new(IoEngine::for_test_with_read_wait(file.io(), 1).unwrap());
    let held = engine.try_reserve_read().unwrap();

    let first = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 1).await;
    let second = spawn_registered_read_slot_waiter(&engine, Duration::from_secs(1), 2).await;

    first.abort();
    match first.await {
        Ok(_) => panic!("aborted queue head completed"),
        Err(error) => assert!(error.is_cancelled()),
    }
    wait_for_registered_read_waiters(&engine, 1).await;

    drop(held);
    let second_slot = second.await.unwrap().unwrap();
    drop(second_slot);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn async_read_deadline_keeps_other_slots_available() {
    let io = Arc::new(BlockingIo::default());
    let engine = Arc::new(IoEngine::for_test(io.clone(), 2).unwrap());
    let managed_memory = managed_memory();
    let request = submit_cache_io_until(
        engine.as_ref(),
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
        Instant::now() + Duration::from_millis(20),
        Duration::from_millis(10),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));

    let timeout = request
        .wait_async(Arc::clone(&engine), &tokio::runtime::Handle::current())
        .await
        .unwrap_err();
    let (error, buffer) = timeout.into_buffer();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
    assert_eq!(engine.in_flight(), 1);
    drop(engine.try_reserve_read().unwrap());

    io.release();
    engine.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn posix_engine_routes_only_aligned_record_io_to_direct() {
    let buffered = TestFile::new("io-engine");
    let direct = TestFile::new("io-engine");
    let buffered_file = buffered.open();
    let direct_file = direct.open();
    buffered_file.set_len(8192).unwrap();
    direct_file.set_len(8192).unwrap();
    let engine = posix::start(
        DataFileHandles::new(buffered_file, Some(direct_file)),
        2,
        2,
        true,
        false,
    )
    .unwrap();
    let managed_memory = managed_memory();

    let aligned = vec![0x5a; 4096];
    assert!(matches!(
        engine
            .write_all_at(
                WritePoint::Record,
                write_buffer(&managed_memory, &aligned),
                0,
            )
            .unwrap()
            .wait()
            .status,
        CompletionStatus::Completed
    ));
    assert!(matches!(
        engine
            .write_all_at(
                WritePoint::Record,
                write_buffer(&managed_memory, &[0x33; 32]),
                4096,
            )
            .unwrap()
            .wait()
            .status,
        CompletionStatus::Completed
    ));

    assert!(engine.stats().file_io.direct_active);
    let stats = engine.stats();
    assert_eq!(stats.file_io.write.direct.operations, 1);
    assert_eq!(stats.file_io.write.direct.bytes, 4096);
    assert_eq!(stats.file_io.write.buffered.operations, 1);
    assert_eq!(stats.file_io.write.buffered.bytes, 32);
    engine.shutdown().unwrap();
}

#[test]
fn unfenced_write_state_remains_unsafe_after_shutdown() {
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test(file.io(), 1).unwrap();
    assert!(!engine.has_unfenced_writes());

    engine.mark_unfenced_writes_for_test();
    engine.shutdown().unwrap();

    assert!(engine.has_unfenced_writes());
    assert_eq!(engine.in_flight(), 0);
}

#[test]
fn read_completion_deadline_retains_only_its_bounded_slot() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let deadline = Instant::now() + Duration::from_millis(20);
    let request = submit_cache_io_until(
        &engine,
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
        deadline,
        Duration::from_millis(10),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));

    let timeout = request.wait(&engine).unwrap_err();
    let pending = engine.in_flight();
    let pending_writes = engine.writes_in_flight();
    let (error, buffer) = timeout.into_buffer();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
    assert_eq!(pending, 1);
    assert_eq!(pending_writes, 0);

    let rejected = engine
        .submit(IoOperation::read(read_buffer(&managed_memory, 1), 0))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);

    io.release();
    engine.shutdown().unwrap();
    assert_eq!(engine.in_flight(), 0);
}

#[test]
fn completion_deadline_keeps_an_issued_write_counted_until_target_completion() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let request = submit_cache_io_until(
        &engine,
        IoOperation::write(
            WritePoint::Record,
            write_buffer(&managed_memory, &[0x5a; 4096]),
            0,
        ),
        Instant::now() + Duration::from_millis(20),
        Duration::from_millis(10),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));

    let timeout = request.wait(&engine).unwrap_err();
    let pending = engine.in_flight();
    let pending_writes = engine.writes_in_flight();
    let rejected = engine
        .submit(IoOperation::write(
            WritePoint::Record,
            write_buffer(&managed_memory, &[0x33; 4096]),
            4096,
        ))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::BrokenPipe);
    io.release();
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
fn engine_request_capacity_is_hard_bounded() {
    let file = TestFile::new("io-engine");
    assert!(matches!(
        IoEngine::for_test(file.io(), MAX_IO_REQUESTS_PER_ENGINE + 1),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
}

#[cfg(unix)]
#[test]
fn configured_posix_engine_shares_its_worker_capacity() {
    let file = TestFile::new("io-engine");
    let handles = DataFileHandles::new(file.open(), None);
    let engine =
        build_file_engine(handles, IoEngineConfig::Posix { workers: 4 }, false, false).unwrap();

    let reserved: Vec<_> = (0..4).map(|_| engine.try_reserve_read().unwrap()).collect();
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
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test_with_options(file.io(), 1, 1, false, false).unwrap();
    let managed_memory = managed_memory();
    let completion = engine
        .write_all_at(
            WritePoint::Record,
            write_buffer(&managed_memory, &[0x5a]),
            0,
        )
        .unwrap()
        .wait();
    assert!(matches!(completion.status, CompletionStatus::Completed));
    assert_eq!(engine.stats(), EngineIoSnapshot::default());
    engine.shutdown().unwrap();
}

#[test]
fn slot_state_tracks_full_write_capacity() {
    let state = Arc::new(EngineState::new(2, true, false));
    let first = state.try_reserve_slot(true).unwrap();
    let second = state.try_reserve_slot(true).unwrap();
    assert!(state.try_reserve_slot(true).is_none());
    assert_eq!(state.total_in_flight(), 2);
    assert_eq!(state.writes_in_flight(), 2);
    drop((first, second));
    assert_eq!(state.total_in_flight(), 0);
    assert_eq!(state.writes_in_flight(), 0);
}

#[test]
fn unused_read_reservation_releases_its_engine_slot() {
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test(file.io(), 1).unwrap();
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
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test(file.io(), 1).unwrap();
    let managed_memory = managed_memory();
    let fence = engine
        .submit_state
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let rejected = engine
        .submit(IoOperation::read(read_buffer(&managed_memory, 1), 0))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(engine.in_flight(), 0);

    drop(fence);
    engine.shutdown().unwrap();
}

#[test]
fn posix_workers_execute_independent_reads_concurrently() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 2).unwrap();
    let managed_memory = managed_memory();
    let first = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 0)
        .unwrap();
    let second = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 1)
        .unwrap();

    let both_entered = io.wait_for_entered(2);
    io.release();
    assert!(matches!(first.wait().status, CompletionStatus::Completed));
    assert!(matches!(second.wait().status, CompletionStatus::Completed));
    assert!(
        both_entered,
        "both workers must enter before either is released"
    );
    assert_eq!(io.maximum_active(), 2);
    engine.shutdown().unwrap();
}

#[test]
fn submit_wait_blocks_at_engine_capacity_and_resumes() {
    let io = Arc::new(BlockingIo::default());
    let engine = Arc::new(IoEngine::for_test(io.clone(), 1).unwrap());
    let managed_memory = managed_memory();
    let first = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 0)
        .unwrap();
    assert!(io.wait_for_entered(1));

    let waiting_engine = engine.clone();
    let waiting_buffer = read_buffer(&managed_memory, 1);
    let rejected = engine
        .submit(IoOperation::read(waiting_buffer, 1))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);
    let (_, waiting_operation) = rejected.into_parts();
    let (started_sender, started_receiver) = mpsc::sync_channel(1);
    let (sender, receiver) = mpsc::sync_channel(1);
    let submitter = std::thread::spawn(move || {
        started_sender.send(()).unwrap();
        sender
            .send(waiting_engine.submit_wait(waiting_operation))
            .unwrap();
    });
    started_receiver.recv().unwrap();
    let early = receiver.recv_timeout(Duration::from_millis(30));
    let was_blocked = matches!(&early, Err(mpsc::RecvTimeoutError::Timeout));

    io.release();
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
    let stats = engine.stats();
    assert_eq!(stats.requests.requests_in_flight_peak, 1);
    assert!(stats.requests.slot_wait_ns > 0);
    assert!(stats.requests.request_time_ns > 0);
    engine.shutdown().unwrap();
}

#[test]
fn controlled_slot_wait_observes_cancel_wake_and_absolute_deadline() {
    let io = Arc::new(BlockingIo::default());
    let engine = Arc::new(IoEngine::for_test(io.clone(), 1).unwrap());
    let managed_memory = managed_memory();
    let first = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 0)
        .unwrap();
    assert!(io.wait_for_entered(1));

    let cancelled = Arc::new(AtomicBool::new(false));
    let waiting_engine = engine.clone();
    let waiting_cancelled = Arc::clone(&cancelled);
    let waiting_operation = IoOperation::read(read_buffer(&managed_memory, 1), 1);
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
            IoOperation::read(read_buffer(&managed_memory, 1), 2),
            &deadline_cancelled,
            Some(Instant::now()),
        )
        .unwrap_err();
    assert_eq!(timed_out.error.kind(), io::ErrorKind::TimedOut);
    drop(timed_out);

    io.release();
    assert!(matches!(first.wait().status, CompletionStatus::Completed));
    engine.shutdown().unwrap();
}

#[test]
fn positioned_io_panic_completes_the_request_and_worker_survives() {
    let engine = IoEngine::for_test(Arc::new(PanicOnceIo::new()), 1).unwrap();
    let managed_memory = managed_memory();
    let failed = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 0)
        .unwrap()
        .wait();
    let (result, lease) = failed.into_lease();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Other);
    assert!(lease.is_some(), "failed completion must return its lease");
    drop(lease);

    let succeeded = engine
        .read_exact_at(read_buffer(&managed_memory, 1), 1)
        .unwrap()
        .wait();
    assert!(matches!(succeeded.status, CompletionStatus::Completed));
    let stats = engine.stats();
    assert_eq!(stats.requests.requests_submitted, 2);
    assert_eq!(stats.requests.requests_succeeded, 1);
    assert_eq!(stats.requests.requests_failed, 1);
    assert_eq!(stats.requests.requests_cancelled, 0);
    assert_eq!(
        stats.requests.requests_succeeded
            + stats.requests.requests_cancelled
            + stats.requests.requests_failed,
        stats.requests.requests_submitted
    );
    engine.shutdown().unwrap();
}

#[test]
fn quarantined_completion_does_not_return_a_potentially_live_buffer() {
    let state = Arc::new(EngineState::new(1, true, false));
    let slot = state.try_reserve_slot(false).unwrap();
    let request_id = RequestId(1);
    let completion = Arc::new(CompletionState::new());
    state.requests_submitted.fetch_add(1, Ordering::Relaxed);

    let managed_memory = managed_memory();
    let task = Task {
        request_id,
        operation: IoOperation::read(read_buffer(&managed_memory, 1), 0),
        completion: Arc::clone(&completion),
        slot,
        submitted_at: Some(Instant::now()),
    };
    state.finish_quarantined(
        task,
        CompletionStatus::Failed(io::Error::other("uncertain kernel lifetime")),
        0,
    );

    let completed = completion.wait();
    assert!(matches!(completed.status, CompletionStatus::Failed(_)));
    assert!(completed.buffer.is_none());
    assert_eq!(state.snapshot().requests_in_flight, 0);

    // The uncertain buffer remains charged instead of being reused while
    // the kernel may still own its address.
    assert_eq!(
        managed_memory.snapshot().current_bytes,
        aligned_buffer_capacity(1).unwrap()
    );
    assert!(managed_memory.try_read_buffer(1).is_some());

    drop(completed);
    drop(completion);
    drop(state);
    assert_eq!(managed_memory.snapshot().current_bytes, 0);
}

#[test]
fn io_histograms_include_failures_when_activity_counters_are_disabled() {
    let file = TestFile::new("io-engine");
    let engine = IoEngine::for_test_with_options(file.io(), 1, 1, false, false).unwrap();
    let recorder = Arc::new(
        Recorder::new(StatsOptions {
            io_latency: true,
            ..StatsOptions::default()
        })
        .unwrap(),
    );
    engine.set_latency_recorder(recorder.io_timing(IoRole::Read).unwrap());
    let managed_memory = managed_memory();
    let completion = engine
        .read_exact_at(read_buffer(&managed_memory, 4096), 0)
        .unwrap()
        .wait();
    assert!(matches!(completion.status, CompletionStatus::Failed(_)));
    assert_eq!(engine.stats(), EngineIoSnapshot::default());
    let snapshot = recorder.io_snapshot();
    let failed = snapshot
        .iter()
        .find(|row| row.role == IoRole::Read && row.outcome == IoOutcome::Failed)
        .unwrap();
    assert_eq!(failed.latency.count, 1);
    assert!(failed.latency.valid);
    engine.shutdown().unwrap();
}

#[test]
fn configured_background_read_deadline_expires_and_retains_owned_buffer() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 2).unwrap();
    let managed_memory = managed_memory();
    let request = submit_cache_io_with_timeout(
        &engine,
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
        Duration::from_millis(20),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));
    let (error, buffer) = request.wait(&engine).unwrap_err().into_buffer();
    io.release();
    engine.shutdown().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
}

#[test]
fn background_read_can_complete_after_default_deadline() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let request = submit_cache_io_with_timeout(
        &engine,
        IoOperation::read(read_buffer(&managed_memory, 4096), 0),
        Duration::from_secs(30),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));
    let release = std::thread::spawn(move || {
        std::thread::sleep(CACHE_IO_COMPLETION_TIMEOUT + Duration::from_millis(200));
        io.release();
    });
    let completion = request.wait(&engine);
    release.join().unwrap();
    engine.shutdown().unwrap();
    assert!(completion.is_ok());
}

#[test]
fn background_recovery_keeps_the_original_request_and_accepts_late_completion() {
    for write in [false, true] {
        let io = Arc::new(BlockingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let managed_memory = managed_memory();
        let operation = if write {
            IoOperation::write(
                WritePoint::Record,
                write_buffer(&managed_memory, &[7; 4096]),
                0,
            )
        } else {
            IoOperation::read(read_buffer(&managed_memory, 4096), 0)
        };
        let mut request = submit_cache_io(&engine, operation).unwrap();
        assert!(io.wait_for_entered(1));
        // Force the normal deadline to expire while the io still owns I/O.
        request.deadline = Instant::now();
        let id = request.id();
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let engine = &engine;
            scope.spawn(move || {
                tx.send(request.wait_background(
                    engine,
                    &mut BackgroundIoAttempt::new(
                        &IoRecovery::new(Some(Duration::from_secs(2))),
                        None,
                    ),
                ))
                .unwrap();
            });
            let early = rx.recv_timeout(Duration::from_millis(30));
            let in_flight = engine.in_flight();
            let charged = managed_memory.snapshot().current_bytes;
            io.release();
            assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
            assert_eq!(in_flight, 1);
            assert!(charged >= 4096);
            let completion = rx.recv_timeout(Duration::from_secs(2)).unwrap().unwrap();
            assert_eq!(completion.request_id, id);
            assert_eq!(completion.bytes_transferred, 4096);
            assert!(completion.into_io_result().0.is_ok());
        });
        assert_eq!(lock_unpoisoned(&io.state).entered, 1);
        // Recovery does not poison admission: the next request also completes.
        let next = engine
            .read_exact_at(read_buffer(&managed_memory, 4096), 0)
            .unwrap()
            .wait();
        assert!(next.into_io_result().0.is_ok());
        engine.shutdown().unwrap();
        assert_eq!(managed_memory.snapshot().current_bytes, 0);
    }
}

#[test]
fn exhausted_background_recovery_still_fences_unfinished_writes() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let mut request = submit_cache_io(
        &engine,
        IoOperation::write(
            WritePoint::Record,
            write_buffer(&managed_memory, &[7; 4096]),
            0,
        ),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));
    request.deadline = Instant::now();
    request.cancel_grace = Duration::from_millis(10);
    let result = request.wait_background(
        &engine,
        &mut BackgroundIoAttempt::new(&IoRecovery::new(Some(Duration::from_millis(20))), None),
    );
    let pending = engine.writes_in_flight();
    let rejected = engine.submit(IoOperation::write(
        WritePoint::Record,
        write_buffer(&managed_memory, &[8; 4096]),
        4096,
    ));
    io.release();
    engine.shutdown().unwrap();
    let (error, buffer) = result.unwrap_err().into_buffer();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
    assert_eq!(pending, 1);
    assert_eq!(
        rejected.unwrap_err().error.kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[test]
fn background_admission_recovers_without_duplicate_submission() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let first = engine
        .read_exact_at(read_buffer(&managed_memory, 4096), 0)
        .unwrap();
    assert!(io.wait_for_entered(1));
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        let engine = &engine;
        let managed_memory = &managed_memory;
        scope.spawn(move || {
            let io_recovery = IoRecovery::new(Some(Duration::from_secs(2)));
            let mut attempt = BackgroundIoAttempt::new(&io_recovery, None);
            let result = submit_background_io(
                engine,
                IoOperation::read(read_buffer(managed_memory, 4096), 0),
                Duration::from_millis(10),
                &mut attempt,
            )
            .map(|request| request.wait_background(engine, &mut attempt));
            tx.send(result).unwrap();
        });
        let early = rx.recv_timeout(Duration::from_millis(50));
        io.release();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(
            rx.recv_timeout(Duration::from_secs(2))
                .unwrap()
                .unwrap()
                .unwrap()
                .into_io_result()
                .0
                .is_ok()
        );
    });
    assert!(first.wait().into_io_result().0.is_ok());
    engine.shutdown().unwrap();
    assert_eq!(lock_unpoisoned(&io.state).entered, 2);
}

#[test]
fn unlimited_recovery_keeps_admission_paused_until_validation() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let io_recovery = IoRecovery::new(None);
    let mut request = submit_cache_io(
        &engine,
        IoOperation::write(
            WritePoint::Record,
            write_buffer(&managed_memory, &[9; 4096]),
            0,
        ),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));
    request.deadline = Instant::now();
    std::thread::scope(|scope| {
        let (completed_tx, completed_rx) = mpsc::channel();
        let (validate_tx, validate_rx) = mpsc::channel();
        let io_recovery = &io_recovery;
        let engine = &engine;
        scope.spawn(move || {
            let mut attempt = BackgroundIoAttempt::new(io_recovery, None);
            let completion = request.wait_background(engine, &mut attempt).unwrap();
            completed_tx.send(completion).unwrap();
            validate_rx.recv().unwrap();
            attempt.finish();
        });
        // Cross more than one polling interval while preserving the same I/O.
        let early = completed_rx.recv_timeout(Duration::from_millis(1100));
        let paused = io_recovery.is_recovering();
        io.release();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(paused);
        let completion = completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(
            io_recovery.is_recovering(),
            "completion delivery alone must not resume fills"
        );
        assert!(completion.into_io_result().0.is_ok());
        validate_tx.send(()).unwrap();
    });
    assert!(!io_recovery.is_recovering());
    assert_eq!(lock_unpoisoned(&io.state).entered, 1);
    engine.shutdown().unwrap();
}

#[test]
fn shutdown_interrupts_unlimited_recovery_without_releasing_pending_write() {
    let io = Arc::new(BlockingIo::default());
    let engine = IoEngine::for_test(io.clone(), 1).unwrap();
    let managed_memory = managed_memory();
    let io_recovery = IoRecovery::new(None);
    let mut request = submit_cache_io(
        &engine,
        IoOperation::write(
            WritePoint::Record,
            write_buffer(&managed_memory, &[9; 4096]),
            0,
        ),
    )
    .unwrap();
    assert!(io.wait_for_entered(1));
    request.deadline = Instant::now();
    std::thread::scope(|scope| {
        let (tx, rx) = mpsc::channel();
        let io_recovery = &io_recovery;
        let engine = &engine;
        scope.spawn(move || {
            let mut attempt = BackgroundIoAttempt::new(io_recovery, None);
            tx.send(request.wait_background(engine, &mut attempt))
                .unwrap();
        });
        let early = rx.recv_timeout(Duration::from_millis(30));
        io_recovery.stop();
        let stopped = rx.recv_timeout(Duration::from_secs(2));
        let pending = engine.writes_in_flight();
        let charged = managed_memory.snapshot().current_bytes;
        io.release();
        assert!(matches!(early, Err(mpsc::RecvTimeoutError::Timeout)));
        let (error, buffer) = stopped.unwrap().unwrap_err().into_buffer();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(buffer.is_none());
        assert_eq!(pending, 1);
        assert!(charged >= 4096);
    });
    engine.shutdown().unwrap();
}

#[test]
fn adaptive_pressure_pauses_before_real_engine_timeout_and_resumes_after_io_completes() {
    use crate::FillControlOptions;
    use crate::FillLimits;
    use crate::FillPressure;
    use crate::io::fill_control::FillController;
    for enforcing in [false, true] {
        let settings = FillLimits::new(1024 * 1024, 1000);
        let mode = if enforcing {
            FillControlOptions::Adaptive(settings)
        } else {
            FillControlOptions::Observe(settings)
        };
        let control = FillController::new(mode, 1, 4096).unwrap().unwrap();
        let io_recovery = IoRecovery::new(None);
        let io = Arc::new(BlockingIo::default());
        let engine = IoEngine::for_test(io.clone(), 1).unwrap();
        let memory = managed_memory();
        std::thread::scope(|scope| {
            let (returned_tx, returned_rx) = mpsc::channel();
            let (validate_tx, validate_rx) = mpsc::channel();
            let io_recovery = &io_recovery;
            let control = &control;
            let engine = &engine;
            let memory = &memory;
            scope.spawn(move || {
                let mut attempt = BackgroundIoAttempt::new(io_recovery, Some(control));
                let request = submit_background_io(
                    engine,
                    IoOperation::write(WritePoint::Record, write_buffer(memory, &[5; 4096]), 0),
                    Duration::from_secs(4),
                    &mut attempt,
                )
                .unwrap();
                let completion = request.wait_background(engine, &mut attempt).unwrap();
                returned_tx.send(completion).unwrap();
                validate_rx.recv().unwrap();
                attempt.finish();
            });
            assert!(io.wait_for_entered(1));
            let deadline = Instant::now() + Duration::from_secs(2);
            while control.snapshot().pressure != FillPressure::Paused && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
            let paused = control.snapshot();
            let recovered = io_recovery.is_recovering();
            let admission = control.try_admit_fill();
            io.release();
            let completion = returned_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert_eq!(paused.pressure, FillPressure::Paused);
            assert!(!recovered, "must react before the normal I/O timeout");
            assert_eq!(admission, !enforcing);
            assert_eq!(paused.outstanding_bytes, 4096);
            assert!(completion.into_io_result().0.is_ok());
            assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
            assert!(control.try_admit_fill());
            validate_tx.send(()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while control.snapshot().pressure == FillPressure::Paused && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(control.snapshot().pressure, FillPressure::Healthy);
        assert!(control.try_admit_fill());
        assert_eq!(lock_unpoisoned(&io.state).entered, 1);
        engine.shutdown().unwrap();
    }
}
