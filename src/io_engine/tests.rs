use super::*;
use crate::io_backend::{SyncMode, SyncPoint};
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
        let path =
            std::env::temp_dir().join(format!("cache2-io-engine-{}-{id}.bin", std::process::id()));
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
fn posix_engine_round_trips_owned_buffers_and_drains() {
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

    let read = engine
        .read_exact_at(read_buffer(&resources, input.len()), 4096)
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
            .read_exact_at(read_buffer(&resources, input.len()), 4096)
            .unwrap_err()
            .error
            .kind()
            .eq(&io::ErrorKind::BrokenPipe)
    );
}

#[test]
fn posix_engine_reports_progress_before_a_terminal_short_io_error() {
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

#[tokio::test]
async fn async_request_is_woken_by_driver_completion() {
    let file = TestFile::new();
    file.file().set_len(4096).unwrap();
    let engine: Arc<dyn IoEngine> = Arc::new(BackendIoEngine::new(file.backend(), 2).unwrap());
    let resources = resources(4096);
    let request = submit_cache_io(
        engine.as_ref(),
        IoOperation::read(read_buffer(&resources, 4096), 0),
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
    let backend = Arc::new(BlockingBackend::default());
    let engine: Arc<dyn IoEngine> = Arc::new(BackendIoEngine::new(backend.clone(), 1).unwrap());
    let resources = resources(4096);
    let request = submit_cache_io(
        engine.as_ref(),
        IoOperation::read(read_buffer(&resources, 4096), 0),
    )
    .unwrap();
    let waiter_engine = Arc::clone(&engine);
    let waiter = tokio::spawn(async move {
        request
            .wait_async(waiter_engine, &tokio::runtime::Handle::current())
            .await
    });
    tokio::task::yield_now().await;
    assert!(backend.wait_for_entered(1));

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    backend.release();
    engine.shutdown().unwrap();
    assert_eq!(engine.in_flight(), 0);
}

#[tokio::test]
async fn async_read_deadline_keeps_other_slots_available() {
    let backend = Arc::new(BlockingBackend::default());
    let engine: Arc<dyn IoEngine> = Arc::new(BackendIoEngine::new(backend.clone(), 2).unwrap());
    let resources = resources(4096);
    let request = submit_cache_io_until(
        engine.as_ref(),
        IoOperation::read(read_buffer(&resources, 4096), 0),
        Instant::now() + Duration::from_millis(20),
        Duration::from_millis(10),
    )
    .unwrap();
    assert!(backend.wait_for_entered(1));

    let timeout = request
        .wait_async(Arc::clone(&engine), &tokio::runtime::Handle::current())
        .await
        .unwrap_err();
    let (error, buffer) = timeout.into_buffer();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
    assert_eq!(engine.in_flight(), 1);
    drop(engine.try_reserve_read().unwrap());

    backend.release();
    engine.shutdown().unwrap();
}

#[cfg(unix)]
#[test]
fn posix_engine_routes_only_aligned_record_io_to_direct() {
    let buffered = TestFile::new();
    let direct = TestFile::new();
    let buffered_file = buffered.file();
    let direct_file = direct.file();
    buffered_file.set_len(8192).unwrap();
    direct_file.set_len(8192).unwrap();
    let engine =
        BackendIoEngine::new_with_files(RuntimeFileSet::new(buffered_file, Some(direct_file)), 2)
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
    assert_eq!(stats.runtime.write.direct.operations, 1);
    assert_eq!(stats.runtime.write.direct.bytes, 4096);
    assert_eq!(stats.runtime.write.buffered.operations, 1);
    assert_eq!(stats.runtime.write.buffered.bytes, 32);
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
fn read_completion_deadline_retains_only_its_bounded_slot() {
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
    let (error, buffer) = timeout.into_buffer();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(buffer.is_none());
    assert_eq!(pending, 1);
    assert_eq!(pending_writes, 0);

    let rejected = engine
        .submit(IoOperation::read(read_buffer(&resources, 1), 0))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::WouldBlock);

    backend.release();
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
    let rejected = engine
        .submit(IoOperation::write(
            WritePoint::Record,
            write_buffer(&resources, &[0x33; 4096]),
            4096,
        ))
        .unwrap_err();
    assert_eq!(rejected.error.kind(), io::ErrorKind::BrokenPipe);
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
fn engine_request_capacity_is_hard_bounded() {
    let file = TestFile::new();
    assert!(matches!(
        BackendIoEngine::new(file.backend(), MAX_IO_REQUESTS_PER_ENGINE + 1),
        Err(error) if error.kind() == io::ErrorKind::InvalidInput
    ));
}

#[cfg(unix)]
#[test]
fn configured_posix_engine_shares_its_worker_capacity() {
    let file = TestFile::new();
    let files = RuntimeFileSet::new(file.file(), None);
    let engine = build_file_engine(files, 4, 4, ConfiguredIoEngine::Posix, false).unwrap();

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
    let file = TestFile::new();
    let engine =
        BackendIoEngine::new_with_workers_and_statistics(file.backend(), 1, 1, false).unwrap();
    let resources = resources(1);
    let completion = engine
        .write_all_at(WritePoint::Record, write_buffer(&resources, &[0x5a]), 0)
        .unwrap()
        .wait();
    assert!(matches!(completion.status, CompletionStatus::Completed));
    assert_eq!(engine.stats(), EngineIoSnapshot::default());
    engine.shutdown().unwrap();
}

#[test]
fn slot_state_tracks_full_write_capacity() {
    let shared = Arc::new(RuntimeShared::new(2, true));
    let first = shared.try_reserve_slot(true).unwrap();
    let second = shared.try_reserve_slot(true).unwrap();
    assert!(shared.try_reserve_slot(true).is_none());
    assert_eq!(shared.total_in_flight(), 2);
    assert_eq!(shared.writes_in_flight(), 2);
    drop((first, second));
    assert_eq!(shared.total_in_flight(), 0);
    assert_eq!(shared.writes_in_flight(), 0);
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
fn submit_wait_blocks_at_engine_capacity_and_resumes() {
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
    let stats = engine.stats();
    assert_eq!(stats.requests.requests_in_flight_peak, 1);
    assert!(stats.requests.slot_wait_ns >= 20_000_000);
    assert!(stats.requests.request_time_ns > 0);
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
    let shared = Arc::new(RuntimeShared::new(1, true));
    let slot = shared.try_reserve_slot(false).unwrap();
    let request_id = RequestId(1);
    let completion = Arc::new(CompletionState::new());
    shared.requests_submitted.fetch_add(1, Ordering::Relaxed);

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

    drop(completed);
    drop(completion);
    drop(shared);
    assert_eq!(resources.managed_memory_snapshot().current_bytes, 0);
}
