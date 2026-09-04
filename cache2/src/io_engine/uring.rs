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

use std::collections::{HashMap, VecDeque};
use std::hash::BuildHasherDefault;
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;

use io_uring::{IoUring, Probe, opcode, squeue, types};
use twox_hash::XxHash3_64;

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
const MAX_COMMANDS_PER_DRAIN: usize = 64;

struct SocketWake {
    sender: UnixStream,
    pending: Arc<AtomicBool>,
}

impl DriverWake for SocketWake {
    fn wake(&self) {
        if self.pending.load(Ordering::Acquire) || self.pending.swap(true, Ordering::AcqRel) {
            return;
        }
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
                    self.pending.store(false, Ordering::Release);
                    return;
                }
            }
        }
        self.pending.store(false, Ordering::Release);
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
        config: crate::runtime_config::IoUringPoolConfig,
        statistics_enabled: bool,
        read_wait_enabled: bool,
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
        if config.io_poll() {
            builder.setup_iopoll();
        }
        if let Some(sq_poll) = config.sq_poll() {
            builder.setup_sqpoll(sq_poll.idle_millis());
            if let Some(cpu) = sq_poll.cpu() {
                builder.setup_sqpoll_cpu(cpu);
            }
        }
        let ring = builder.build(ring_entries)?;
        if !ring.params().is_feature_nodrop() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel io_uring can drop completion entries",
            ));
        }
        if config.sq_poll().is_some() && !ring.params().is_feature_sqpoll_nonfixed() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "kernel io_uring SQPOLL requires registered files",
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
        wake_receiver.set_nonblocking(true)?;
        let wake_pending = Arc::new(AtomicBool::new(false));
        let wake: Arc<dyn DriverWake> = Arc::new(SocketWake {
            sender: wake_sender,
            pending: Arc::clone(&wake_pending),
        });
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
                    wake_pending,
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
    wake_pending: Arc<AtomicBool>,
    wake_active: bool,
    wake_cancel_submitted: bool,
    wake_cancel_completed: bool,
    shared: Arc<RuntimeShared>,
    submit_state: Arc<RwLock<SubmitState>>,
    receiver: Receiver<DriverCommand>,
    flights: HashMap<RequestId, Flight, BuildHasherDefault<XxHash3_64>>,
    pending_targets: VecDeque<RequestId>,
    pending_cancels: VecDeque<RequestId>,
    requested_cancels: Vec<RequestId>,
    pending_entries: Vec<PendingEntry>,
    submission_entries: Vec<squeue::Entry>,
    completion_events: Vec<(u64, i32)>,
    io_poll: bool,
    shutting_down: bool,
}

fn uring_driver(
    files: RuntimeFileSet,
    mut ring: IoUring,
    wake_receiver: UnixStream,
    wake_pending: Arc<AtomicBool>,
    shared: Arc<RuntimeShared>,
    submit_state: Arc<RwLock<SubmitState>>,
    receiver: Receiver<DriverCommand>,
) -> io::Result<()> {
    let max_in_flight = shared.max_in_flight;
    let submission_capacity = ring.submission().capacity();
    let completion_capacity = ring.completion().capacity();
    let io_poll = ring.params().is_setup_iopoll();
    let mut driver = UringDriver {
        files: Some(files),
        ring: Some(ring),
        wake_receiver,
        wake_pending,
        wake_active: false,
        wake_cancel_submitted: false,
        wake_cancel_completed: false,
        shared,
        submit_state,
        receiver,
        flights: HashMap::with_capacity_and_hasher(max_in_flight, BuildHasherDefault::default()),
        pending_targets: VecDeque::with_capacity(max_in_flight),
        pending_cancels: VecDeque::with_capacity(max_in_flight),
        requested_cancels: Vec::with_capacity(max_in_flight),
        pending_entries: Vec::with_capacity(submission_capacity),
        submission_entries: Vec::with_capacity(submission_capacity),
        completion_events: Vec::with_capacity(completion_capacity),
        io_poll,
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
            let woke = self.drain_completions()?;
            let mut commands_pending = self.drain_commands();
            if woke || self.io_poll {
                // Producers may skip the socket write while `pending` is set.
                // Clear only after one command drain, then drain once more to
                // close the enqueue-before-clear race. Enqueues after the
                // clear emit a fresh wake byte. IOPOLL rings cannot arm the
                // in-ring wake poll, so every iteration clears and drains
                // twice instead of synchronizing on a wake CQE.
                self.wake_pending.store(false, Ordering::Release);
                commands_pending = self.drain_commands();
            }
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
            if submitted == 0 && !commands_pending {
                self.wait_for_completion()?;
            }
        }
    }

    /// Returns true when the budget is exhausted: commands may remain without
    /// another socket wake, so the driver must make another pass before parking.
    fn drain_commands(&mut self) -> bool {
        for _ in 0..MAX_COMMANDS_PER_DRAIN {
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
                Err(mpsc::TryRecvError::Empty) => return false,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.shutting_down = true;
                    return false;
                }
            }
        }
        true
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
            if !self.io_poll {
                // IOPOLL rings cannot cancel with an in-ring async-cancel
                // request; the polled device completes quickly and
                // `complete_target` treats `cancel_requested` as advisory.
                self.pending_cancels.push_back(request_id);
            }
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
        if !self.shutting_down && !self.wake_active && !self.io_poll {
            // IOPOLL rings cannot carry a poll-on-wake-fd request; the driver
            // instead blocks in userspace `poll` while the ring is idle.
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
                if self.io_poll && path != RuntimeIoPath::Direct {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "io_uring IOPOLL operation is not direct-I/O aligned",
                    ));
                }
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
        if self.io_poll {
            return self.wait_for_polled_completion();
        }
        let wake_cancel_pending = self.wake_cancel_submitted && !self.wake_cancel_completed;
        if !self.wake_active && self.flights.is_empty() && !wake_cancel_pending {
            return Ok(());
        }
        retry_interrupted(|| self.ring_mut().submit_and_wait(1)).map(|_| ())
    }

    /// IOPOLL completion reaping. Polled rings publish completions only while
    /// the kernel busy-polls inside an enter-with-GETEVENTS call, so an idle
    /// ring must never reach `submit_and_wait` and an active ring reaps there.
    fn wait_for_polled_completion(&mut self) -> io::Result<()> {
        if self.flights.is_empty() {
            self.block_on_wake();
            return Ok(());
        }
        retry_interrupted(|| self.ring_mut().submit_and_wait(1)).map(|_| ())
    }

    /// Blocks the driver on the wake socket instead of an in-ring poll.
    ///
    /// Producers write one byte per submission burst (coalesced through
    /// `wake_pending`), and shutdown always writes a final byte after its
    /// `DriverCommand::Shutdown`. The socket is never drained before parking:
    /// a byte whose commands have not been drained yet must keep `poll`
    /// from blocking, so the driver first waits for readability and only then
    /// drains bytes and lets the main loop drain commands. A byte left over
    /// from an already-drained burst costs one extra loop iteration, never a
    /// lost wake. The bounded timeout additionally re-scans the command
    /// channel, which also covers a producer-channel disconnect.
    fn block_on_wake(&mut self) {
        let wake_fd = self.wake_receiver.as_raw_fd();
        loop {
            let mut pollfd = libc::pollfd {
                fd: wake_fd,
                events: libc::POLLIN,
                revents: 0,
            };
            let ready = unsafe { libc::poll(&mut pollfd, 1, 100) };
            if ready < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return;
                }
                continue;
            }
            if ready == 0 {
                return;
            }
            if pollfd.revents & libc::POLLIN != 0 {
                self.drain_wake_bytes();
                return;
            }
        }
    }

    fn drain_completions(&mut self) -> io::Result<bool> {
        let mut woke = false;
        let mut wake_error = None;
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
                woke = true;
                self.wake_active = false;
                if !self.shutting_down {
                    if result >= 0 {
                        self.drain_wake_bytes();
                    } else {
                        wake_error = Some(io::Error::from_raw_os_error(result.saturating_neg()));
                    }
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
        wake_error.map_or(Ok(woke), Err)
    }

    fn drain_wake_bytes(&mut self) {
        let mut wake_buffer = [0_u8; 64];
        loop {
            match retry_interrupted(|| self.wake_receiver.read(&mut wake_buffer)) {
                Ok(0) => break,
                Ok(_) => continue,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
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
        self.shared.stop_accepting_slots();
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::{ResourceController, ResourceLimits};
    use std::task::{Wake, Waker};

    struct CancelledCommandProducer {
        shared: Arc<RuntimeShared>,
        commands: mpsc::SyncSender<DriverCommand>,
        resources: ResourceController,
        next: AtomicU64,
        completed: AtomicUsize,
    }

    impl CancelledCommandProducer {
        fn enqueue(self: &Arc<Self>, cancelled: bool) {
            let completion = Arc::new(CompletionState::new());
            completion
                .cancel_requested
                .store(cancelled, Ordering::Release);
            if cancelled {
                completion.cell.lock().unwrap().waker = Some(Waker::from(Arc::clone(self)));
            }
            let buffer =
                IoBuffer::for_read(self.resources.try_read_buffer(4096).unwrap(), 1).unwrap();
            let task = Task {
                request_id: RequestId(self.next.fetch_add(1, Ordering::Relaxed)),
                operation: IoOperation::read(buffer, 0),
                completion,
                slot: self.shared.try_reserve_slot(false).unwrap(),
                submitted_at: None,
            };
            assert!(self.commands.try_send(DriverCommand::Submit(task)).is_ok());
        }
    }

    impl Wake for CancelledCommandProducer {
        fn wake(self: Arc<Self>) {
            if self.completed.fetch_add(1, Ordering::Relaxed) + 1 < 100 {
                self.enqueue(true);
            }
        }
    }

    #[test]
    fn cancelled_command_refills_leave_room_for_io_progress() {
        let depth = 2;
        let shared = Arc::new(RuntimeShared::new(depth, false, false));
        let (commands, receiver) = mpsc::sync_channel(depth * 2 + 1);
        let (_wake_sender, wake_receiver) = UnixStream::pair().unwrap();
        let producer = Arc::new(CancelledCommandProducer {
            shared: Arc::clone(&shared),
            commands,
            resources: ResourceController::try_new(ResourceLimits {
                memory_limit_bytes: 16 * 1024,
                reserved_memory_bytes: 0,
            })
            .unwrap(),
            next: AtomicU64::new(1),
            completed: AtomicUsize::new(0),
        });
        // Command processing needs neither a kernel ring nor file descriptors.
        let mut driver = UringDriver {
            files: None,
            ring: None,
            wake_receiver,
            wake_pending: Arc::new(AtomicBool::new(false)),
            wake_active: false,
            wake_cancel_submitted: false,
            wake_cancel_completed: false,
            shared,
            submit_state: Arc::new(RwLock::new(SubmitState { accepting: true })),
            receiver,
            flights: HashMap::with_capacity_and_hasher(depth, BuildHasherDefault::default()),
            pending_targets: VecDeque::with_capacity(depth),
            pending_cancels: VecDeque::with_capacity(depth),
            requested_cancels: Vec::with_capacity(depth),
            pending_entries: Vec::new(),
            submission_entries: Vec::new(),
            completion_events: Vec::new(),
            io_poll: false,
            shutting_down: false,
        };
        producer.enqueue(false);
        producer.enqueue(true);

        assert!(
            driver.drain_commands(),
            "remaining commands must prevent parking"
        );
        assert_eq!(driver.pending_targets.len(), 1);
        assert!(producer.completed.load(Ordering::Relaxed) < 100);

        assert!(
            !driver.drain_commands(),
            "an empty queue allows parking again"
        );
        assert_eq!(producer.completed.load(Ordering::Relaxed), 100);
        assert_eq!(driver.pending_targets.len(), 1);
    }

    #[test]
    fn io_uring_memory_reservation_covers_allocations() {
        // Include both SQ and hash-table growth boundaries, including the
        // largest admitted depth. This needs no kernel io_uring support.
        let maximum = MAX_IO_REQUESTS_PER_ENGINE;
        for depth in [1, 28, 29, 2046, 2047, maximum - 2, maximum] {
            let sq = (depth + 2).next_power_of_two();
            let cq = sq * 2;
            let flights = HashMap::<RequestId, Flight, BuildHasherDefault<XxHash3_64>>::
                with_capacity_and_hasher(depth, BuildHasherDefault::default());
            // HashMap capacity excludes its unused buckets; include those
            // buckets, control bytes, and the trailing SIMD control group.
            let buckets = flights.capacity().next_power_of_two();
            let flight_bytes = buckets * (size_of::<(RequestId, Flight)>() + 1) + 16;
            // Each bounded channel slot also carries an atomic stamp, and
            // completion state is held in a separately allocated Arc.
            let commands = (2 * depth + 1) * (size_of::<DriverCommand>() + size_of::<usize>());
            let completions = depth * (size_of::<CompletionState>() + 2 * size_of::<usize>());
            let request_ids = 3 * depth * size_of::<RequestId>();
            let batches = sq * (size_of::<PendingEntry>() + size_of::<squeue::Entry>())
                + cq * size_of::<(u64, i32)>();
            let fatal_drain = (depth + 1) * (size_of::<(u64, u64)>() + size_of::<squeue::Entry>())
                + cq * size_of::<u64>();
            // SQ indices, SQEs and CQEs, with one 64 KiB page of header /
            // alignment allowance for each of the three possible mappings.
            let mappings = sq * (size_of::<u32>() + size_of::<squeue::Entry>())
                + cq * size_of::<io_uring::cqueue::Entry>()
                + 3 * 64 * 1024;
            let required = flight_bytes
                + commands
                + completions
                + request_ids
                + batches
                + fatal_drain
                + mappings;
            let reserved = depth * IO_QUEUE_ENTRY_RESERVATION_BYTES
                + io_uring_extra_memory_bytes(depth, 1).unwrap();
            assert!(
                reserved >= required,
                "depth {depth}: {reserved} < {required}"
            );
        }
    }

    #[test]
    fn socket_wake_coalesces_until_driver_clears_pending() {
        let (sender, mut receiver) = UnixStream::pair().unwrap();
        sender.set_nonblocking(true).unwrap();
        receiver.set_nonblocking(true).unwrap();
        let pending = Arc::new(AtomicBool::new(false));
        let wake = SocketWake {
            sender,
            pending: Arc::clone(&pending),
        };

        wake.wake();
        wake.wake();
        let mut byte = [0_u8; 1];
        assert_eq!(receiver.read(&mut byte).unwrap(), 1);
        assert_eq!(
            receiver.read(&mut byte).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        pending.store(false, Ordering::Release);
        wake.wake();
        assert_eq!(receiver.read(&mut byte).unwrap(), 1);
    }
}
