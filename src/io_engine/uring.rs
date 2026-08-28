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
    io_stats: RuntimeIoStatsHandle,
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
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "queue size overflow"))?;
        let (commands, receiver) = mpsc::sync_channel(command_capacity);
        let submit_state = Arc::new(RwLock::new(SubmitState { accepting: true }));
        let worker_shared = Arc::clone(&shared);
        let worker_submit_state = Arc::clone(&submit_state);
        let worker = std::thread::Builder::new()
            .name("cache2-uring-io".into())
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

    fn stats(&self) -> EngineIoSnapshot {
        EngineIoSnapshot {
            requests: self.inner.shared.snapshot(),
            runtime: self.io_stats.snapshot(),
        }
    }
}

struct Flight {
    task: Task,
    transferred: usize,
    active: bool,
    active_path: Option<RuntimeIoPath>,
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
                if !self.wake_active && (!self.wake_cancel_submitted || self.wake_cancel_completed)
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
                    flight.task.operation.runtime_io_path(
                        self.files
                            .as_ref()
                            .expect("runtime files exist while driver runs"),
                        flight.transferred,
                    )?
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
            sq.push_multiple(submission_entries)
                .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "io_uring SQ is full"))?;
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
            completion_events.extend(cq.by_ref().map(|entry| (entry.user_data(), entry.result())));
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
            OperationKind::Read | OperationKind::Write => {
                let remaining =
                    operation_length(&flight.task.operation).saturating_sub(flight.transferred);
                let completed = result as usize;
                if completed <= remaining && completed != 0 {
                    self.files
                        .as_ref()
                        .expect("runtime files exist while driver runs")
                        .record(
                            flight.task.operation.kind().io_direction(),
                            active_path,
                            completed,
                        );
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
            if flight.active {
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
    }
}
