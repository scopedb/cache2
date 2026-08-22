//! Runtime-neutral bounded executor for the asynchronous cache facade.
//!
//! The executor deliberately does not know about `DiskCache`. Callers submit
//! closures whose output is already the public cache result type and provide a
//! small mapper for dispatch failures. This keeps the compatibility layer
//! usable while the synchronous cache remains the source of API semantics.
//!
//! Read work is cancelable after it starts. Mutation work becomes committed
//! when its worker starts: cancellation and deadlines are then advisory and
//! the real mutation result must be delivered. This avoids reporting a
//! timeout and subsequently publishing a write behind the caller's back.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::io;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll, Waker};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::cache::{
    CacheError, CacheStatus, DiskCache, Inner, PutOptions, PutOutcome, RejectReason, RemoveOutcome,
    Result as CacheResult,
};
use crate::format::MAX_KEY_SIZE;
use crate::resources::OverloadReason;

const MAX_ASYNC_READ_WORKERS: usize = 128;

pub(crate) fn async_read_worker_count(read_queue_depth: usize, io_queue_depth: usize) -> usize {
    MAX_ASYNC_READ_WORKERS
        .min(read_queue_depth)
        .min(io_queue_depth)
}

const PHASE_QUEUED: u8 = 0;
const PHASE_RUNNING_CANCELABLE: u8 = 1;
const PHASE_COMMITTED: u8 = 2;
const PHASE_DONE: u8 = 3;

const STOP_NONE: u8 = 0;
const STOP_CANCELLED: u8 = 1;
const STOP_TIMED_OUT: u8 = 2;

const CLOSE_OPEN: u8 = 0;
const CLOSE_RUNNING: u8 = 1;
const CLOSE_DONE: u8 = 2;

const CLOSE_PENDING: u8 = 0;
const CLOSE_SUCCEEDED: u8 = 1;
const CLOSE_POISONED: u8 = 2;

const CONTROL_QUEUE_RESERVE: usize = 2;

/// A failure produced by the facade before the cache operation returns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AsyncFailure {
    Closed,
    QueueFull,
    Cancelled,
    TimedOut,
    WorkerPanicked,
}

/// Result of requesting cancellation on a cache future.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelOutcome {
    /// The request was queued or executing on the cancelable read path.
    Requested,
    /// A mutation has started and must return its real commit result.
    TooLate,
    /// The request had already completed.
    Completed,
}

/// Per-request scheduling options.
#[derive(Clone, Copy, Debug, Default)]
pub struct AsyncRequestOptions {
    deadline: Option<Instant>,
}

impl AsyncRequestOptions {
    pub const fn new() -> Self {
        Self { deadline: None }
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            deadline: Some(now.checked_add(timeout).unwrap_or(now)),
        }
    }

    pub const fn with_deadline(deadline: Instant) -> Self {
        Self {
            deadline: Some(deadline),
        }
    }

    pub const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

/// Cancellation state visible to an executing read task.
#[derive(Clone)]
pub(crate) struct TaskContext {
    stop: Arc<StopToken>,
    phase: Arc<AtomicU8>,
    deadline: Option<Instant>,
}

impl TaskContext {
    pub(crate) fn stop_reason(&self) -> Option<AsyncFailure> {
        match self.stop.reason.load(Ordering::Acquire) {
            STOP_CANCELLED => Some(AsyncFailure::Cancelled),
            STOP_TIMED_OUT => Some(AsyncFailure::TimedOut),
            _ => None,
        }
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stop.reason.load(Ordering::Acquire) != STOP_NONE
    }

    /// Absolute scheduling deadline supplied by the caller. Engines use the
    /// same instant for their own bounded admission waits so a task cannot
    /// outlive the facade deadline while blocked below the executor.
    pub(crate) const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Install the best-effort cancellation hook for the currently pending
    /// engine request. If cancellation already won the race, the hook is
    /// invoked immediately on this thread.
    pub(crate) fn set_stop_hook(&self, hook: impl Fn(AsyncFailure) + Send + Sync + 'static) {
        self.stop.set_hook(Arc::new(hook));
    }

    /// Run a small read-side publication only if cancellation has not won.
    /// Stop publication and stop requests share the hook mutex, so once a
    /// cancel/timeout call returns no later publication can slip through.
    pub(crate) fn run_if_active<T>(&self, publish: impl FnOnce() -> T) -> Option<T> {
        let _serialized = lock_unpoisoned(&self.stop.hook);
        (self.stop.reason.load(Ordering::Acquire) == STOP_NONE).then(publish)
    }

    /// Atomically turn a cancelable read into a committed mutation. A caller
    /// must invoke this before its first irreversible state change. If stop
    /// already won, no mutation may follow; if this wins, later cancellation
    /// reports `TooLate` and the task publishes its real completion.
    pub(crate) fn try_commit(&self) -> bool {
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            match phase {
                PHASE_RUNNING_CANCELABLE => match self.phase.compare_exchange_weak(
                    PHASE_RUNNING_CANCELABLE,
                    PHASE_COMMITTED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(observed) => phase = observed,
                },
                PHASE_COMMITTED => return true,
                _ => return false,
            }
        }
    }
}

struct StopToken {
    reason: AtomicU8,
    hook: Mutex<Option<StopHook>>,
}

type StopHook = Arc<dyn Fn(AsyncFailure) + Send + Sync>;

impl StopToken {
    fn new() -> Self {
        Self {
            reason: AtomicU8::new(STOP_NONE),
            hook: Mutex::new(None),
        }
    }

    fn set(&self, failure: AsyncFailure) {
        let reason = match failure {
            AsyncFailure::Cancelled => STOP_CANCELLED,
            AsyncFailure::TimedOut => STOP_TIMED_OUT,
            _ => return,
        };
        let hook = {
            let mut registered = lock_unpoisoned(&self.hook);
            if self
                .reason
                .compare_exchange(STOP_NONE, reason, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            registered.take()
        };
        if let Some(hook) = hook {
            let _ = catch_unwind(AssertUnwindSafe(|| hook(failure)));
        }
    }

    fn set_hook(&self, hook: StopHook) {
        let mut registered = lock_unpoisoned(&self.hook);
        let reason = self.reason.load(Ordering::Acquire);
        if reason == STOP_NONE {
            *registered = Some(hook);
            return;
        }
        drop(registered);
        let failure = match reason {
            STOP_TIMED_OUT => AsyncFailure::TimedOut,
            _ => AsyncFailure::Cancelled,
        };
        let _ = catch_unwind(AssertUnwindSafe(|| hook(failure)));
    }
}

/// A runtime-neutral request that can be awaited or waited synchronously.
#[must_use = "cache requests do nothing useful unless awaited, waited, or cancelled"]
pub struct CacheFuture<T> {
    core: Arc<RequestCore<T>>,
}

impl<T> CacheFuture<T> {
    pub fn cancel(&self) -> CancelOutcome {
        self.core.request_stop(AsyncFailure::Cancelled)
    }

    pub fn wait(self) -> T {
        self.core.completion.wait()
    }
}

impl<T> Future for CacheFuture<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.core.completion.poll(context)
    }
}

impl<T> Drop for CacheFuture<T> {
    fn drop(&mut self) {
        let _ = self.core.request_stop(AsyncFailure::Cancelled);
    }
}

struct Completion<T> {
    state: Mutex<CompletionState<T>>,
    ready: Condvar,
}

struct CompletionState<T> {
    value: Option<T>,
    waker: Option<Waker>,
}

impl<T> Completion<T> {
    fn new() -> Self {
        Self {
            state: Mutex::new(CompletionState {
                value: None,
                waker: None,
            }),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, value: T) {
        let waker = {
            let mut state = lock_unpoisoned(&self.state);
            if state.value.is_some() {
                return;
            }
            state.value = Some(value);
            state.waker.take()
        };
        self.ready.notify_all();
        if let Some(waker) = waker {
            safe_wake(waker);
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<T> {
        let mut state = lock_unpoisoned(&self.state);
        if let Some(value) = state.value.take() {
            return Poll::Ready(value);
        }
        let replace = state
            .waker
            .as_ref()
            .is_none_or(|registered| !wakers_match(registered, context.waker()));
        if replace {
            state.waker = try_clone_waker(context.waker());
        }
        Poll::Pending
    }

    fn wait(&self) -> T {
        let mut state = lock_unpoisoned(&self.state);
        loop {
            if let Some(value) = state.value.take() {
                return value;
            }
            state = wait_unpoisoned(&self.ready, state);
        }
    }
}

struct RequestCore<T> {
    phase: Arc<AtomicU8>,
    stop: Arc<StopToken>,
    completion: Completion<T>,
    failure: Arc<dyn Fn(AsyncFailure) -> T + Send + Sync>,
    timer: Weak<TimerInner>,
    timer_key: Mutex<Option<TimerKey>>,
    queued_in: Mutex<Option<Weak<PoolInner>>>,
}

impl<T> RequestCore<T> {
    fn new(failure: Arc<dyn Fn(AsyncFailure) -> T + Send + Sync>, timer: Weak<TimerInner>) -> Self {
        Self {
            phase: Arc::new(AtomicU8::new(PHASE_QUEUED)),
            stop: Arc::new(StopToken::new()),
            completion: Completion::new(),
            failure,
            timer,
            timer_key: Mutex::new(None),
            queued_in: Mutex::new(None),
        }
    }

    fn start(&self, kind: TaskKind) -> bool {
        let phase = match kind {
            TaskKind::CancelableRead => PHASE_RUNNING_CANCELABLE,
            TaskKind::CommittedMutation => PHASE_COMMITTED,
        };
        let started = self
            .phase
            .compare_exchange(PHASE_QUEUED, phase, Ordering::AcqRel, Ordering::Acquire)
            .is_ok();
        if started {
            lock_unpoisoned(&self.queued_in).take();
        }
        started
    }

    fn finish(&self, value: T) {
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            match phase {
                PHASE_RUNNING_CANCELABLE | PHASE_COMMITTED => {
                    match self.phase.compare_exchange_weak(
                        phase,
                        PHASE_DONE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.unregister_timer();
                            self.completion.complete(value);
                            return;
                        }
                        Err(observed) => phase = observed,
                    }
                }
                PHASE_DONE => return,
                _ => return,
            }
        }
    }

    fn fail_before_or_during_task(&self, failure: AsyncFailure) {
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            match phase {
                PHASE_QUEUED | PHASE_RUNNING_CANCELABLE | PHASE_COMMITTED => {
                    match self.phase.compare_exchange_weak(
                        phase,
                        PHASE_DONE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.stop.set(failure);
                            self.unregister_timer();
                            self.remove_from_queue();
                            self.completion.complete((self.failure)(failure));
                            return;
                        }
                        Err(observed) => phase = observed,
                    }
                }
                PHASE_DONE => return,
                _ => return,
            }
        }
    }

    fn request_stop(&self, failure: AsyncFailure) -> CancelOutcome {
        debug_assert!(matches!(
            failure,
            AsyncFailure::Cancelled | AsyncFailure::TimedOut
        ));
        let mut phase = self.phase.load(Ordering::Acquire);
        loop {
            match phase {
                PHASE_QUEUED | PHASE_RUNNING_CANCELABLE => {
                    match self.phase.compare_exchange_weak(
                        phase,
                        PHASE_DONE,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            self.stop.set(failure);
                            self.unregister_timer();
                            self.remove_from_queue();
                            self.completion.complete((self.failure)(failure));
                            return CancelOutcome::Requested;
                        }
                        Err(observed) => phase = observed,
                    }
                }
                PHASE_COMMITTED => return CancelOutcome::TooLate,
                PHASE_DONE => return CancelOutcome::Completed,
                _ => return CancelOutcome::Completed,
            }
        }
    }

    fn unregister_timer(&self) {
        let Some(key) = lock_unpoisoned(&self.timer_key).take() else {
            return;
        };
        if let Some(timer) = self.timer.upgrade() {
            timer.unregister(key);
        }
    }

    fn attach_queue(&self, pool: &Arc<PoolInner>) {
        let mut queued_in = lock_unpoisoned(&self.queued_in);
        if self.phase.load(Ordering::Acquire) == PHASE_QUEUED {
            *queued_in = Some(Arc::downgrade(pool));
            return;
        }
        drop(queued_in);
        pool.remove_cancelled();
    }

    fn remove_from_queue(&self) {
        let pool = lock_unpoisoned(&self.queued_in)
            .take()
            .and_then(|pool| pool.upgrade());
        if let Some(pool) = pool {
            pool.remove_cancelled();
        }
    }
}

impl<T: Send + 'static> TimerTarget for RequestCore<T> {
    fn fire_timeout(&self) {
        let _ = self.request_stop(AsyncFailure::TimedOut);
    }
}

#[derive(Clone, Copy)]
enum TaskKind {
    CancelableRead,
    CommittedMutation,
}

trait Runnable: Send {
    fn run(self: Box<Self>);

    fn is_queued(&self) -> bool;
}

struct RunnableTask<T, F>
where
    T: Send + 'static,
    F: FnOnce(TaskContext) -> T + Send + 'static,
{
    core: Arc<RequestCore<T>>,
    kind: TaskKind,
    deadline: Option<Instant>,
    task: Option<F>,
}

impl<T, F> Runnable for RunnableTask<T, F>
where
    T: Send + 'static,
    F: FnOnce(TaskContext) -> T + Send + 'static,
{
    fn run(mut self: Box<Self>) {
        if !self.core.start(self.kind) {
            return;
        }
        let task = self.task.take().expect("async task is executed once");
        let context = TaskContext {
            stop: Arc::clone(&self.core.stop),
            phase: Arc::clone(&self.core.phase),
            deadline: self.deadline,
        };
        match catch_unwind(AssertUnwindSafe(|| task(context))) {
            Ok(value) => self.core.finish(value),
            Err(_) => self
                .core
                .fail_before_or_during_task(AsyncFailure::WorkerPanicked),
        }
    }

    fn is_queued(&self) -> bool {
        self.core.phase.load(Ordering::Acquire) == PHASE_QUEUED
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueueClass {
    Ordinary,
    // Uses the admission slots protected from puts, but participates in the
    // concurrent prefix. This keeps remove available under put saturation
    // without turning every remove into a global mutation barrier.
    ReservedOrdinary,
    Control,
}

struct QueuedTask {
    class: QueueClass,
    runnable: Box<dyn Runnable>,
}

struct QueueState {
    accepting: bool,
    pending: usize,
    ordinary_in_flight: usize,
    control_in_flight: bool,
    ordinary_reserved: usize,
    control_reserved: usize,
    ordinary_queued: usize,
    control_queued: usize,
    rejected: u64,
    queue: VecDeque<QueuedTask>,
    drain_waker: Option<Waker>,
}

struct PoolInner {
    ordinary_capacity: usize,
    total_capacity: usize,
    state: Mutex<QueueState>,
    changed: Condvar,
}

impl PoolInner {
    fn try_reserve(self: &Arc<Self>, class: QueueClass) -> Result<QueueReservation, AsyncFailure> {
        self.remove_cancelled();
        let mut state = lock_unpoisoned(&self.state);
        if !state.accepting {
            return Err(AsyncFailure::Closed);
        }
        let ordinary_full = class == QueueClass::Ordinary
            && state
                .ordinary_queued
                .saturating_add(state.ordinary_reserved)
                >= self.ordinary_capacity;
        let reserved = state
            .ordinary_reserved
            .saturating_add(state.control_reserved);
        if ordinary_full || state.queue.len().saturating_add(reserved) >= self.total_capacity {
            state.rejected = state.rejected.saturating_add(1);
            return Err(AsyncFailure::QueueFull);
        }
        match class {
            QueueClass::Ordinary => state.ordinary_reserved += 1,
            QueueClass::ReservedOrdinary | QueueClass::Control => state.control_reserved += 1,
        }
        Ok(QueueReservation {
            pool: Arc::clone(self),
            class,
            active: true,
        })
    }

    fn enqueue_reserved(
        &self,
        class: QueueClass,
        runnable: Box<dyn Runnable>,
    ) -> Result<(), AsyncFailure> {
        let mut state = lock_unpoisoned(&self.state);
        release_reservation_locked(&mut state, class);
        if !state.accepting {
            return Err(AsyncFailure::Closed);
        }
        match class {
            QueueClass::Ordinary => state.ordinary_queued += 1,
            QueueClass::ReservedOrdinary | QueueClass::Control => state.control_queued += 1,
        }
        state.queue.push_back(QueuedTask { class, runnable });
        state.pending += 1;
        self.changed.notify_one();
        Ok(())
    }

    fn release_reservation(&self, class: QueueClass) {
        let mut state = lock_unpoisoned(&self.state);
        release_reservation_locked(&mut state, class);
        self.changed.notify_all();
    }

    fn begin_drain(&self) {
        let waker = {
            let mut state = lock_unpoisoned(&self.state);
            state.accepting = false;
            self.changed.notify_all();
            if state.pending == 0 {
                state.drain_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            safe_wake(waker);
        }
    }

    fn task_done(&self, class: QueueClass) {
        let waker = {
            let mut state = lock_unpoisoned(&self.state);
            debug_assert!(state.pending != 0);
            let release_control;
            match class {
                QueueClass::Ordinary | QueueClass::ReservedOrdinary => {
                    debug_assert!(state.ordinary_in_flight != 0);
                    state.ordinary_in_flight = state.ordinary_in_flight.saturating_sub(1);
                    release_control = state.ordinary_in_flight == 0
                        && matches!(
                            state.queue.front().map(|task| task.class),
                            Some(QueueClass::Control)
                        );
                }
                QueueClass::Control => {
                    debug_assert!(state.control_in_flight);
                    state.control_in_flight = false;
                    release_control = false;
                }
            }
            state.pending = state.pending.saturating_sub(1);
            if state.pending == 0 || class == QueueClass::Control {
                // Draining workers must all observe an empty stopped pool. A
                // completed control releases a concurrent ordinary prefix.
                self.changed.notify_all();
            } else if release_control {
                // Only one worker may claim the newly runnable control.
                self.changed.notify_one();
            }
            if state.pending == 0 {
                state.drain_waker.take()
            } else {
                None
            }
        };
        if let Some(waker) = waker {
            safe_wake(waker);
        }
    }

    fn remove_cancelled(&self) {
        let waker = {
            let mut state = lock_unpoisoned(&self.state);
            let discarded = discard_cancelled_locked(&mut state);
            if discarded == 0 {
                return;
            }
            state.pending = state.pending.saturating_sub(discarded);
            self.changed.notify_all();
            (state.pending == 0)
                .then(|| state.drain_waker.take())
                .flatten()
        };
        if let Some(waker) = waker {
            safe_wake(waker);
        }
    }

    fn snapshot(&self) -> PoolSnapshot {
        self.remove_cancelled();
        let state = lock_unpoisoned(&self.state);
        PoolSnapshot {
            queued: state.queue.len(),
            in_flight: state.pending.saturating_sub(state.queue.len()),
            reserved: state
                .ordinary_reserved
                .saturating_add(state.control_reserved),
            ordinary_queued: state.ordinary_queued,
            control_queued: state.control_queued,
            rejected: state.rejected,
            ordinary_capacity: self.ordinary_capacity,
            total_capacity: self.total_capacity,
        }
    }

    fn poll_drained(&self, context: &mut Context<'_>) -> Poll<()> {
        let mut state = lock_unpoisoned(&self.state);
        if state.pending == 0 {
            return Poll::Ready(());
        }
        let replace = state
            .drain_waker
            .as_ref()
            .is_none_or(|registered| !wakers_match(registered, context.waker()));
        if replace {
            state.drain_waker = try_clone_waker(context.waker());
        }
        Poll::Pending
    }

    fn wait_drained(&self) {
        let mut state = lock_unpoisoned(&self.state);
        while state.pending != 0 {
            state = wait_unpoisoned(&self.changed, state);
        }
    }
}

fn release_reservation_locked(state: &mut QueueState, class: QueueClass) {
    match class {
        QueueClass::Ordinary => {
            debug_assert!(state.ordinary_reserved != 0);
            state.ordinary_reserved = state.ordinary_reserved.saturating_sub(1);
        }
        QueueClass::ReservedOrdinary | QueueClass::Control => {
            debug_assert!(state.control_reserved != 0);
            state.control_reserved = state.control_reserved.saturating_sub(1);
        }
    }
}

pub(crate) struct QueueReservation {
    pool: Arc<PoolInner>,
    class: QueueClass,
    active: bool,
}

impl QueueReservation {
    fn enqueue(mut self, runnable: Box<dyn Runnable>) -> Result<(), AsyncFailure> {
        let result = self.pool.enqueue_reserved(self.class, runnable);
        self.active = false;
        result
    }
}

impl Drop for QueueReservation {
    fn drop(&mut self) {
        if self.active {
            self.pool.release_reservation(self.class);
        }
    }
}

fn discard_cancelled_locked(state: &mut QueueState) -> usize {
    let mut discarded = 0usize;
    let mut ordinary = 0usize;
    let mut control = 0usize;
    state.queue.retain(|task| {
        if task.runnable.is_queued() {
            return true;
        }
        discarded += 1;
        match task.class {
            QueueClass::Ordinary => ordinary += 1,
            QueueClass::ReservedOrdinary | QueueClass::Control => control += 1,
        }
        false
    });
    state.ordinary_queued = state.ordinary_queued.saturating_sub(ordinary);
    state.control_queued = state.control_queued.saturating_sub(control);
    discarded
}

#[derive(Clone, Copy)]
struct PoolSnapshot {
    queued: usize,
    in_flight: usize,
    reserved: usize,
    ordinary_queued: usize,
    control_queued: usize,
    rejected: u64,
    ordinary_capacity: usize,
    total_capacity: usize,
}

struct WorkerPool {
    inner: Arc<PoolInner>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    worker_panicked: AtomicBool,
}

impl WorkerPool {
    fn try_new(
        name: &str,
        workers: usize,
        ordinary_capacity: usize,
        control_reserve: usize,
    ) -> io::Result<Self> {
        if workers == 0 || ordinary_capacity == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "async worker count and queue capacity must be non-zero",
            ));
        }
        let total_capacity = ordinary_capacity
            .checked_add(control_reserve)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "async queue capacity overflow")
            })?;
        let mut queue = VecDeque::new();
        queue.try_reserve_exact(total_capacity).map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "async task queue allocation")
        })?;
        let inner = Arc::new(PoolInner {
            ordinary_capacity,
            total_capacity,
            state: Mutex::new(QueueState {
                accepting: true,
                pending: 0,
                ordinary_in_flight: 0,
                control_in_flight: false,
                ordinary_reserved: 0,
                control_reserved: 0,
                ordinary_queued: 0,
                control_queued: 0,
                rejected: 0,
                queue,
                drain_waker: None,
            }),
            changed: Condvar::new(),
        });
        let mut handles = Vec::new();
        handles.try_reserve_exact(workers).map_err(|_| {
            io::Error::new(io::ErrorKind::OutOfMemory, "async worker table allocation")
        })?;
        for index in 0..workers {
            let worker_inner = Arc::clone(&inner);
            let thread_name = format!("cache-rs-{name}-{index}");
            match thread::Builder::new()
                .name(thread_name)
                .spawn(move || worker_loop(worker_inner))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    inner.begin_drain();
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            inner,
            workers: Mutex::new(handles),
            worker_panicked: AtomicBool::new(false),
        })
    }

    fn join(&self) -> bool {
        // Keep the join mutex held until all handles finish. Concurrent close
        // callers must not observe an empty table while another caller is
        // still waiting for a worker that may panic.
        let mut workers = lock_unpoisoned(&self.workers);
        for worker in workers.drain(..) {
            if worker.join().is_err() {
                self.worker_panicked.store(true, Ordering::Release);
            }
        }
        self.worker_panicked.load(Ordering::Acquire)
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.inner.begin_drain();
        self.inner.wait_drained();
        let _ = self.join();
    }
}

fn worker_loop(inner: Arc<PoolInner>) {
    loop {
        let task = {
            let mut state = lock_unpoisoned(&inner.state);
            loop {
                let discarded = discard_cancelled_locked(&mut state);
                if discarded != 0 {
                    state.pending = state.pending.saturating_sub(discarded);
                    inner.changed.notify_all();
                    if state.pending == 0 {
                        if let Some(waker) = state.drain_waker.take() {
                            drop(state);
                            safe_wake(waker);
                            state = lock_unpoisoned(&inner.state);
                            continue;
                        }
                    }
                }
                let runnable = match state.queue.front().map(|task| task.class) {
                    Some(QueueClass::Ordinary | QueueClass::ReservedOrdinary)
                        if !state.control_in_flight =>
                    {
                        let task = state.queue.pop_front().expect("front task exists");
                        match task.class {
                            QueueClass::Ordinary => {
                                state.ordinary_queued = state.ordinary_queued.saturating_sub(1)
                            }
                            QueueClass::ReservedOrdinary => {
                                state.control_queued = state.control_queued.saturating_sub(1)
                            }
                            QueueClass::Control => unreachable!("matched ordinary task"),
                        }
                        state.ordinary_in_flight += 1;
                        Some(task)
                    }
                    Some(QueueClass::Control)
                        if state.ordinary_in_flight == 0 && !state.control_in_flight =>
                    {
                        let task = state.queue.pop_front().expect("front task exists");
                        state.control_queued = state.control_queued.saturating_sub(1);
                        state.control_in_flight = true;
                        Some(task)
                    }
                    _ => None,
                };
                if let Some(task) = runnable {
                    break task;
                }
                if !state.accepting && state.queue.is_empty() {
                    return;
                }
                state = wait_unpoisoned(&inner.changed, state);
            }
        };
        let _done = TaskDoneGuard {
            inner: Arc::clone(&inner),
            class: task.class,
        };
        // RunnableTask catches user code. This outer boundary also protects
        // the lane from an unexpected panic in completion bookkeeping.
        let _ = catch_unwind(AssertUnwindSafe(|| task.runnable.run()));
    }
}

struct TaskDoneGuard {
    inner: Arc<PoolInner>,
    class: QueueClass,
}

impl Drop for TaskDoneGuard {
    fn drop(&mut self) {
        self.inner.task_done(self.class);
    }
}

/// Fixed, bounded compatibility executor for `AsyncDiskCache`.
pub(crate) struct AsyncExecutor {
    reads: WorkerPool,
    mutations: WorkerPool,
    timer: TimerDriver,
}

impl AsyncExecutor {
    pub(crate) fn try_new(
        read_queue_depth: usize,
        write_queue_depth: usize,
        io_queue_depth: usize,
        mutation_workers: usize,
    ) -> io::Result<Self> {
        if read_queue_depth == 0
            || write_queue_depth == 0
            || io_queue_depth == 0
            || mutation_workers == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "async queue depths and mutation worker count must be non-zero",
            ));
        }
        let timer = TimerDriver::try_new()?;
        let read_workers = async_read_worker_count(read_queue_depth, io_queue_depth);
        let reads = WorkerPool::try_new("async-read", read_workers, read_queue_depth, 0)?;
        let mutations = WorkerPool::try_new(
            "async-mutate",
            mutation_workers,
            write_queue_depth,
            CONTROL_QUEUE_RESERVE,
        )?;
        Ok(Self {
            reads,
            mutations,
            timer,
        })
    }

    #[cfg(test)]
    fn submit_read<T, F, M>(
        &self,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit(
            self.reserve_read(),
            TaskKind::CancelableRead,
            options,
            map_failure,
            task,
        )
    }

    #[cfg(test)]
    fn submit_mutation<T, F, M>(
        &self,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit(
            self.reserve_mutation(),
            TaskKind::CommittedMutation,
            options,
            map_failure,
            task,
        )
    }

    pub(crate) fn submit_control<T, F, M>(
        &self,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit(
            self.reserve_control(),
            TaskKind::CommittedMutation,
            options,
            map_failure,
            task,
        )
    }

    #[cfg(test)]
    fn submit_reserved_mutation<T, F, M>(
        &self,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit(
            self.reserve_reserved_mutation(),
            TaskKind::CommittedMutation,
            options,
            map_failure,
            task,
        )
    }

    pub(crate) fn reserve_read(&self) -> Result<QueueReservation, AsyncFailure> {
        self.reads.inner.try_reserve(QueueClass::Ordinary)
    }

    pub(crate) fn reserve_mutation(&self) -> Result<QueueReservation, AsyncFailure> {
        self.mutations.inner.try_reserve(QueueClass::Ordinary)
    }

    fn reserve_reserved_mutation(&self) -> Result<QueueReservation, AsyncFailure> {
        self.mutations
            .inner
            .try_reserve(QueueClass::ReservedOrdinary)
    }

    fn reserve_control(&self) -> Result<QueueReservation, AsyncFailure> {
        self.mutations.inner.try_reserve(QueueClass::Control)
    }

    pub(crate) fn submit_read_reserved<T, F, M>(
        &self,
        reservation: QueueReservation,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit_reserved(
            reservation,
            TaskKind::CancelableRead,
            options,
            map_failure,
            task,
        )
    }

    pub(crate) fn submit_mutation_reserved<T, F, M>(
        &self,
        reservation: QueueReservation,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        self.submit_reserved(
            reservation,
            TaskKind::CommittedMutation,
            options,
            map_failure,
            task,
        )
    }

    fn submit<T, F, M>(
        &self,
        reservation: Result<QueueReservation, AsyncFailure>,
        kind: TaskKind,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        let reservation = match reservation {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_failure(failure)),
        };
        self.submit_reserved(reservation, kind, options, map_failure, task)
    }

    fn submit_reserved<T, F, M>(
        &self,
        reservation: QueueReservation,
        kind: TaskKind,
        options: AsyncRequestOptions,
        map_failure: M,
        task: F,
    ) -> CacheFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(TaskContext) -> T + Send + 'static,
        M: Fn(AsyncFailure) -> T + Send + Sync + 'static,
    {
        let pool = Arc::clone(&reservation.pool);
        let core = Arc::new(RequestCore::new(
            Arc::new(map_failure),
            Arc::downgrade(&self.timer.inner),
        ));
        let runnable: Box<dyn Runnable> = Box::new(RunnableTask {
            core: Arc::clone(&core),
            kind,
            deadline: options.deadline,
            task: Some(task),
        });
        if let Err(failure) = reservation.enqueue(runnable) {
            core.fail_before_or_during_task(failure);
        } else {
            core.attach_queue(&pool);
            if let Some(deadline) = options.deadline {
                self.timer.register(&core, deadline);
            }
        }
        CacheFuture { core }
    }

    /// Stop admission and return a future for all already accepted work.
    pub(crate) fn begin_drain(&self) -> DrainHandle {
        self.reads.inner.begin_drain();
        self.mutations.inner.begin_drain();
        DrainHandle {
            reads: Arc::clone(&self.reads.inner),
            mutations: Arc::clone(&self.mutations.inner),
        }
    }

    /// Join worker and timer threads after the drain handle becomes ready.
    pub(crate) fn join(&self) -> bool {
        let read_panicked = self.reads.join();
        let mutation_panicked = self.mutations.join();
        self.timer.shutdown();
        read_panicked || mutation_panicked
    }

    pub(crate) fn snapshot(&self) -> AsyncQueueStats {
        let reads = self.reads.inner.snapshot();
        let mutations = self.mutations.inner.snapshot();
        AsyncQueueStats {
            read_queued: usize_to_u64(reads.queued),
            read_in_flight: usize_to_u64(reads.in_flight),
            read_reserved: usize_to_u64(reads.reserved),
            mutation_queued: usize_to_u64(mutations.queued),
            mutation_in_flight: usize_to_u64(mutations.in_flight),
            mutation_reserved: usize_to_u64(mutations.reserved),
            ordinary_mutation_queued: usize_to_u64(mutations.ordinary_queued),
            control_mutation_queued: usize_to_u64(mutations.control_queued),
            read_queue_capacity: usize_to_u64(reads.ordinary_capacity),
            write_queue_capacity: usize_to_u64(mutations.ordinary_capacity),
            control_queue_reserve: usize_to_u64(
                mutations
                    .total_capacity
                    .saturating_sub(mutations.ordinary_capacity),
            ),
            queue_rejections: reads.rejected.saturating_add(mutations.rejected),
        }
    }
}

/// Snapshot of the bounded queues owned by the asynchronous facade.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AsyncQueueStats {
    pub read_queued: u64,
    pub read_in_flight: u64,
    pub read_reserved: u64,
    pub mutation_queued: u64,
    pub mutation_in_flight: u64,
    pub mutation_reserved: u64,
    pub ordinary_mutation_queued: u64,
    pub control_mutation_queued: u64,
    pub read_queue_capacity: u64,
    pub write_queue_capacity: u64,
    pub control_queue_reserve: u64,
    pub queue_rejections: u64,
}

/// Shared, uncancellable completion of asynchronous cache shutdown.
///
/// Every call to [`AsyncDiskCache::close`] returns a distinct waiter over the
/// same result. Cancelling or dropping one waiter never interrupts shutdown.
#[must_use = "cache close must be awaited or waited for completion"]
pub struct AsyncCloseFuture {
    completion: Arc<CloseCompletion>,
}

impl AsyncCloseFuture {
    pub fn cancel(&self) -> CancelOutcome {
        if self.completion.is_ready() {
            CancelOutcome::Completed
        } else {
            CancelOutcome::TooLate
        }
    }

    pub fn wait(self) -> CacheResult<()> {
        self.completion.wait()
    }
}

impl Future for AsyncCloseFuture {
    type Output = CacheResult<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        self.completion.poll(context)
    }
}

struct CloseCompletion {
    outcome: AtomicU8,
    waiters: Mutex<Vec<Waker>>,
    ready: Condvar,
}

impl CloseCompletion {
    fn new() -> Self {
        Self {
            outcome: AtomicU8::new(CLOSE_PENDING),
            waiters: Mutex::new(Vec::new()),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, succeeded: bool) {
        let outcome = if succeeded {
            CLOSE_SUCCEEDED
        } else {
            CLOSE_POISONED
        };
        let waiters = {
            let mut waiters = lock_unpoisoned(&self.waiters);
            if self
                .outcome
                .compare_exchange(CLOSE_PENDING, outcome, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }
            std::mem::take(&mut *waiters)
        };
        self.ready.notify_all();
        for waker in waiters {
            safe_wake(waker);
        }
    }

    fn is_ready(&self) -> bool {
        self.outcome.load(Ordering::Acquire) != CLOSE_PENDING
    }

    fn result(&self) -> Option<CacheResult<()>> {
        match self.outcome.load(Ordering::Acquire) {
            CLOSE_SUCCEEDED => Some(Ok(())),
            CLOSE_POISONED => Some(Err(CacheError::Poisoned)),
            _ => None,
        }
    }

    fn poll(&self, context: &mut Context<'_>) -> Poll<CacheResult<()>> {
        if let Some(result) = self.result() {
            return Poll::Ready(result);
        }
        let mut waiters = lock_unpoisoned(&self.waiters);
        if let Some(result) = self.result() {
            return Poll::Ready(result);
        }
        if !waiters
            .iter()
            .any(|registered| wakers_match(registered, context.waker()))
        {
            // Failing to retain a waker cannot make shutdown unsafe. The
            // future remains pollable and synchronous waiters still work.
            if waiters.try_reserve(1).is_ok() {
                if let Some(waker) = try_clone_waker(context.waker()) {
                    waiters.push(waker);
                }
            }
        }
        Poll::Pending
    }

    fn wait(&self) -> CacheResult<()> {
        let mut waiters = lock_unpoisoned(&self.waiters);
        loop {
            if let Some(result) = self.result() {
                return result;
            }
            waiters = wait_unpoisoned(&self.ready, waiters);
        }
    }
}

/// Future-based compatibility facade for [`DiskCache`].
///
/// Clones share one bounded executor. Reads use a fixed worker set. Ordinary
/// mutations may execute concurrently and overlapping calls can linearize in
/// either worker-acquisition order; await an earlier mutation when its order is
/// required. Control mutations form FIFO barriers that run only after every
/// earlier mutation completes. The cache's synchronous API remains the source
/// of commit and recovery semantics.
#[derive(Clone)]
pub struct AsyncDiskCache {
    inner: Arc<AsyncInner>,
}

pub(crate) struct AsyncInner {
    cache: Weak<Inner>,
    executor: Arc<AsyncExecutor>,
    close_state: AtomicU8,
    close_completion: Arc<CloseCompletion>,
}

impl AsyncDiskCache {
    pub(crate) fn try_new(
        cache: Weak<Inner>,
        read_queue_depth: usize,
        write_queue_depth: usize,
        io_queue_depth: usize,
        mutation_workers: usize,
    ) -> CacheResult<Self> {
        let executor = AsyncExecutor::try_new(
            read_queue_depth,
            write_queue_depth,
            io_queue_depth,
            mutation_workers,
        )
        .map_err(CacheError::Io)?;
        Ok(Self {
            inner: Arc::new(AsyncInner {
                cache,
                executor: Arc::new(executor),
                close_state: AtomicU8::new(CLOSE_OPEN),
                close_completion: Arc::new(CloseCompletion::new()),
            }),
        })
    }

    pub(crate) fn from_inner(inner: Arc<AsyncInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn shared_inner(&self) -> &Arc<AsyncInner> {
        &self.inner
    }

    /// Return a point-in-time view of facade queue occupancy and rejection
    /// counters. Reading this snapshot also reclaims cancelled queued tasks.
    pub fn queue_stats(&self) -> AsyncQueueStats {
        self.inner.executor.snapshot()
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> CacheFuture<CacheResult<Option<Vec<u8>>>> {
        self.get_in(0, key)
    }

    pub fn get_with_options(
        &self,
        key: impl AsRef<[u8]>,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<Option<Vec<u8>>>> {
        self.get_in_with_options(0, key, request_options)
    }

    pub fn get_in(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
    ) -> CacheFuture<CacheResult<Option<Vec<u8>>>> {
        self.get_in_with_options(namespace, key, AsyncRequestOptions::default())
    }

    pub fn get_in_with_options(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<Option<Vec<u8>>>> {
        if !self.is_open() {
            return ready_future(Err(CacheError::Closed));
        }
        let cache = match self.inner.cache() {
            Some(cache) => cache,
            None => return ready_future(Err(CacheError::Closed)),
        };
        match cache.status() {
            CacheStatus::Healthy => {}
            CacheStatus::MissOnly => {
                cache.record_async_miss();
                return ready_future(Ok(None));
            }
            CacheStatus::Poisoned => return ready_future(Err(CacheError::Poisoned)),
            CacheStatus::Closed => return ready_future(Err(CacheError::Closed)),
        }
        let key = key.as_ref();
        if key.len() > MAX_KEY_SIZE {
            if !self.is_open() {
                return ready_future(Err(CacheError::Closed));
            }
            return ready_future(Ok(None));
        }
        let reservation = match self.inner.executor.reserve_read() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_read_failure(failure)),
        };
        let key = match copy_input(key, OverloadReason::ReadBufferUnavailable) {
            Ok(key) => key,
            Err(error) => return ready_future(Err(error)),
        };
        self.inner.executor.submit_read_reserved(
            reservation,
            request_options,
            map_read_failure,
            move |context| cache.get_in_with_task_context(namespace, &key, &context),
        )
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
    ) -> CacheFuture<CacheResult<PutOutcome>> {
        self.put_in(0, key, value, put_options)
    }

    pub fn put_with_options(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<PutOutcome>> {
        self.put_in_with_options(0, key, value, put_options, request_options)
    }

    pub fn put_in(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
    ) -> CacheFuture<CacheResult<PutOutcome>> {
        self.put_in_with_options(
            namespace,
            key,
            value,
            put_options,
            AsyncRequestOptions::default(),
        )
    }

    pub fn put_in_with_options(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<PutOutcome>> {
        if !self.is_open() {
            return ready_future(Err(CacheError::Closed));
        }
        let cache = match self.inner.cache() {
            Some(cache) => cache,
            None => return ready_future(Err(CacheError::Closed)),
        };
        if let Some(error) = write_status_error(cache.status()) {
            return ready_future(Err(error));
        }
        let key = key.as_ref();
        let value = value.as_ref();
        let (max_key_size, max_value_size) = cache.async_put_input_limits();
        if key.len() > max_key_size {
            if !self.is_open() {
                return ready_future(Err(CacheError::Closed));
            }
            cache.record_async_put_rejection();
            return ready_future(Ok(PutOutcome::Rejected(RejectReason::KeyTooLarge)));
        }
        if value.len() > max_value_size {
            if !self.is_open() {
                return ready_future(Err(CacheError::Closed));
            }
            cache.record_async_put_rejection();
            return ready_future(Ok(PutOutcome::Rejected(RejectReason::ValueTooLarge)));
        }
        let reservation = match self.inner.executor.reserve_mutation() {
            Ok(reservation) => reservation,
            Err(AsyncFailure::QueueFull) => {
                cache.record_async_put_rejection();
                return ready_future(Ok(PutOutcome::Rejected(RejectReason::SubmissionFull)));
            }
            Err(failure) => return ready_future(map_put_failure(failure)),
        };
        let key = match copy_input(key, OverloadReason::WriteBufferUnavailable) {
            Ok(key) => key,
            Err(_) => {
                cache.record_async_put_rejection();
                return ready_future(Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)));
            }
        };
        let value = match copy_input(value, OverloadReason::WriteBufferUnavailable) {
            Ok(value) => value,
            Err(_) => {
                cache.record_async_put_rejection();
                return ready_future(Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)));
            }
        };
        let failure_cache = cache.clone();
        self.inner.executor.submit_mutation_reserved(
            reservation,
            request_options,
            move |failure| {
                if failure == AsyncFailure::QueueFull {
                    failure_cache.record_async_put_rejection();
                }
                map_put_failure(failure)
            },
            move |_| cache.put_in(namespace, key, value, put_options),
        )
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> CacheFuture<CacheResult<RemoveOutcome>> {
        self.remove_in(0, key)
    }

    pub fn remove_with_options(
        &self,
        key: impl AsRef<[u8]>,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<RemoveOutcome>> {
        self.remove_in_with_options(0, key, request_options)
    }

    pub fn remove_in(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
    ) -> CacheFuture<CacheResult<RemoveOutcome>> {
        self.remove_in_with_options(namespace, key, AsyncRequestOptions::default())
    }

    pub fn remove_in_with_options(
        &self,
        namespace: u32,
        key: impl AsRef<[u8]>,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<RemoveOutcome>> {
        if !self.is_open() {
            return ready_future(Err(CacheError::Closed));
        }
        let cache = match self.inner.cache() {
            Some(cache) => cache,
            None => return ready_future(Err(CacheError::Closed)),
        };
        if let Some(error) = write_status_error(cache.status()) {
            return ready_future(Err(error));
        }
        let key = key.as_ref();
        if key.len() > MAX_KEY_SIZE {
            if !self.is_open() {
                return ready_future(Err(CacheError::Closed));
            }
            return ready_future(Ok(RemoveOutcome::NotFound));
        }
        let reservation = match self.inner.executor.reserve_reserved_mutation() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_write_failure(failure)),
        };
        let key = match copy_input(key, OverloadReason::WriteBufferUnavailable) {
            Ok(key) => key,
            Err(error) => return ready_future(Err(error)),
        };
        self.inner.executor.submit_mutation_reserved(
            reservation,
            request_options,
            map_write_failure,
            move |_| cache.remove_in(namespace, &key),
        )
    }

    pub fn flush(&self) -> CacheFuture<CacheResult<()>> {
        self.flush_with_options(AsyncRequestOptions::default())
    }

    pub fn flush_with_options(
        &self,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<()>> {
        if !self.is_open() {
            return ready_future(Err(CacheError::Closed));
        }
        let cache = match self.inner.cache() {
            Some(cache) => cache,
            None => return ready_future(Err(CacheError::Closed)),
        };
        if let Some(error) = write_status_error(cache.status()) {
            return ready_future(Err(error));
        }
        let reservation = match self.inner.executor.reserve_control() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_write_failure(failure)),
        };
        self.inner.executor.submit_mutation_reserved(
            reservation,
            request_options,
            map_write_failure,
            move |_| cache.flush(),
        )
    }

    pub fn clear(&self) -> CacheFuture<CacheResult<()>> {
        self.clear_with_options(AsyncRequestOptions::default())
    }

    pub fn clear_with_options(
        &self,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<CacheResult<()>> {
        if !self.is_open() {
            return ready_future(Err(CacheError::Closed));
        }
        let cache = match self.inner.cache() {
            Some(cache) => cache,
            None => return ready_future(Err(CacheError::Closed)),
        };
        if let Some(error) = write_status_error(cache.status()) {
            return ready_future(Err(error));
        }
        let reservation = match self.inner.executor.reserve_control() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_write_failure(failure)),
        };
        self.inner.executor.submit_mutation_reserved(
            reservation,
            request_options,
            map_write_failure,
            move |_| cache.clear(),
        )
    }

    /// Drain every accepted facade request, stop its workers, and then close
    /// the synchronous cache. The request is committed immediately and cannot
    /// be cancelled halfway through shutdown.
    pub fn close(&self) -> AsyncCloseFuture {
        let future = AsyncCloseFuture {
            completion: Arc::clone(&self.inner.close_completion),
        };
        if self.inner.begin_close() {
            self.start_close_coordinator();
        }
        future
    }

    fn is_open(&self) -> bool {
        self.inner.close_state.load(Ordering::Acquire) == CLOSE_OPEN
    }

    fn start_close_coordinator(&self) {
        let inner = Arc::clone(&self.inner);
        let spawn = thread::Builder::new()
            .name("cache-rs-async-close".into())
            .spawn(move || {
                let worker_panicked = inner.drain_and_join();
                let Some(cache) = inner.cache() else {
                    inner.finish_close(false);
                    return;
                };
                if catch_unwind(AssertUnwindSafe(|| {
                    cache.close_after_async_drain(&inner, worker_panicked)
                }))
                .is_err()
                {
                    inner.finish_close(false);
                }
            });
        if spawn.is_err() {
            // Thread creation failure must not strand the file lock. The
            // caller pays the synchronous drain cost, but observes the same
            // shared close result as the normal coordinator path.
            let worker_panicked = self.inner.drain_and_join();
            let Some(cache) = self.inner.cache() else {
                self.inner.finish_close(false);
                return;
            };
            if catch_unwind(AssertUnwindSafe(|| {
                cache.close_after_async_drain(&self.inner, worker_panicked)
            }))
            .is_err()
            {
                self.inner.finish_close(false);
            }
        }
    }
}

impl AsyncInner {
    fn cache(&self) -> Option<DiskCache> {
        self.cache.upgrade().map(DiskCache::from_inner)
    }

    pub(crate) fn queue_stats(&self) -> AsyncQueueStats {
        self.executor.snapshot()
    }

    /// Fence facade admission before synchronous close starts.
    ///
    /// The winner returns `true`; other close callers join the same shared
    /// completion instead of receiving a transient `Closed` result.
    pub(crate) fn begin_close(&self) -> bool {
        if self
            .close_state
            .compare_exchange(
                CLOSE_OPEN,
                CLOSE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.executor.begin_drain();
            true
        } else {
            false
        }
    }

    /// Drain accepted facade work and join every worker exactly once.
    pub(crate) fn drain_and_join(&self) -> bool {
        self.executor.begin_drain().wait();
        self.executor.join()
    }

    /// Publish the one shared async-close result.
    ///
    /// Any close-path failure is deliberately normalized to `Poisoned`: the
    /// original non-cloneable `io::Error` remains available to the initiating
    /// synchronous caller while every async waiter observes the same outcome.
    pub(crate) fn finish_close(&self, succeeded: bool) {
        self.close_state.store(CLOSE_DONE, Ordering::Release);
        self.close_completion.complete(succeeded);
    }

    /// Wait for the close owner chosen by [`Self::begin_close`].
    pub(crate) fn wait_close_result(&self) -> CacheResult<()> {
        self.close_completion.wait()
    }
}

fn map_read_failure<T>(failure: AsyncFailure) -> CacheResult<T> {
    Err(map_failure(failure, OverloadReason::ReadQueueFull))
}

fn write_status_error(status: CacheStatus) -> Option<CacheError> {
    match status {
        CacheStatus::Healthy => None,
        CacheStatus::MissOnly | CacheStatus::Poisoned => Some(CacheError::Poisoned),
        CacheStatus::Closed => Some(CacheError::Closed),
    }
}

fn map_put_failure(failure: AsyncFailure) -> CacheResult<PutOutcome> {
    match failure {
        AsyncFailure::QueueFull => Ok(PutOutcome::Rejected(RejectReason::SubmissionFull)),
        failure => Err(map_failure(failure, OverloadReason::WriteQueueFull)),
    }
}

fn map_write_failure<T>(failure: AsyncFailure) -> CacheResult<T> {
    Err(map_failure(failure, OverloadReason::WriteQueueFull))
}

fn map_failure(failure: AsyncFailure, queue_full: OverloadReason) -> CacheError {
    match failure {
        AsyncFailure::Closed => CacheError::Closed,
        AsyncFailure::QueueFull => CacheError::Overloaded(queue_full),
        AsyncFailure::Cancelled => CacheError::Cancelled,
        AsyncFailure::TimedOut => CacheError::TimedOut,
        AsyncFailure::WorkerPanicked => CacheError::Poisoned,
    }
}

fn copy_input(input: &[u8], allocation_failure: OverloadReason) -> CacheResult<Vec<u8>> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(input.len())
        .map_err(|_| CacheError::Overloaded(allocation_failure))?;
    owned.extend_from_slice(input);
    Ok(owned)
}

pub(crate) fn ready_future<T: Send + 'static>(value: T) -> CacheFuture<T> {
    let core = Arc::new(RequestCore::new(
        Arc::new(|_| panic!("a ready future cannot fail dispatch")),
        Weak::new(),
    ));
    debug_assert!(core.start(TaskKind::CancelableRead));
    core.finish(value);
    CacheFuture { core }
}

/// Completion handle used before invoking `DiskCache::close`.
pub(crate) struct DrainHandle {
    reads: Arc<PoolInner>,
    mutations: Arc<PoolInner>,
}

impl DrainHandle {
    pub(crate) fn wait(self) {
        self.reads.wait_drained();
        self.mutations.wait_drained();
    }
}

impl Future for DrainHandle {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let reads = self.reads.poll_drained(context).is_ready();
        let mutations = self.mutations.poll_drained(context).is_ready();
        if reads && mutations {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

trait TimerTarget: Send + Sync {
    fn fire_timeout(&self);
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TimerKey {
    deadline: Instant,
    sequence: u64,
}

struct TimerState {
    stopped: bool,
    deadlines: BTreeMap<TimerKey, Weak<dyn TimerTarget>>,
}

struct TimerInner {
    next_sequence: AtomicU64,
    state: Mutex<TimerState>,
    changed: Condvar,
}

impl TimerInner {
    fn unregister(&self, key: TimerKey) {
        lock_unpoisoned(&self.state).deadlines.remove(&key);
        self.changed.notify_one();
    }
}

struct TimerDriver {
    inner: Arc<TimerInner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

impl TimerDriver {
    fn try_new() -> io::Result<Self> {
        let inner = Arc::new(TimerInner {
            next_sequence: AtomicU64::new(0),
            state: Mutex::new(TimerState {
                stopped: false,
                deadlines: BTreeMap::new(),
            }),
            changed: Condvar::new(),
        });
        let worker_inner = Arc::clone(&inner);
        let worker = thread::Builder::new()
            .name("cache-rs-async-timer".into())
            .spawn(move || timer_loop(worker_inner))?;
        Ok(Self {
            inner,
            worker: Mutex::new(Some(worker)),
        })
    }

    fn register<T: Send + 'static>(&self, target: &Arc<RequestCore<T>>, deadline: Instant) {
        if deadline <= Instant::now() {
            let _ = catch_unwind(AssertUnwindSafe(|| target.fire_timeout()));
            return;
        }
        let sequence = self.inner.next_sequence.fetch_add(1, Ordering::Relaxed);
        let key = TimerKey { deadline, sequence };
        let timer_target: Arc<dyn TimerTarget> = target.clone();
        // Register under the same core -> timer lock order used by
        // `unregister_timer`. Otherwise a completion between attaching the key
        // and inserting the map entry could leave a stale long-lived timer.
        let mut timer_key = lock_unpoisoned(&target.timer_key);
        if target.phase.load(Ordering::Acquire) == PHASE_DONE {
            return;
        }
        let mut state = lock_unpoisoned(&self.inner.state);
        if state.stopped {
            drop(state);
            drop(timer_key);
            let _ = catch_unwind(AssertUnwindSafe(|| timer_target.fire_timeout()));
            return;
        }
        state.deadlines.insert(key, Arc::downgrade(&timer_target));
        *timer_key = Some(key);
        self.inner.changed.notify_one();
    }

    fn shutdown(&self) {
        {
            let mut state = lock_unpoisoned(&self.inner.state);
            state.stopped = true;
            state.deadlines.clear();
            self.inner.changed.notify_all();
        }
        let mut worker = lock_unpoisoned(&self.worker);
        if let Some(handle) = worker.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TimerDriver {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn timer_loop(inner: Arc<TimerInner>) {
    loop {
        let target = {
            let mut state = lock_unpoisoned(&inner.state);
            loop {
                if state.stopped {
                    return;
                }
                let Some((&key, _)) = state.deadlines.first_key_value() else {
                    state = wait_unpoisoned(&inner.changed, state);
                    continue;
                };
                let now = Instant::now();
                if key.deadline <= now {
                    break state
                        .deadlines
                        .remove(&key)
                        .and_then(|target| target.upgrade());
                }
                let timeout = key.deadline.saturating_duration_since(now);
                let (next, _) = wait_timeout_unpoisoned(&inner.changed, state, timeout);
                state = next;
            }
        };
        if let Some(target) = target {
            let _ = catch_unwind(AssertUnwindSafe(|| target.fire_timeout()));
        }
    }
}

fn safe_wake(waker: Waker) {
    let _ = catch_unwind(AssertUnwindSafe(|| waker.wake()));
}

fn wakers_match(left: &Waker, right: &Waker) -> bool {
    catch_unwind(AssertUnwindSafe(|| left.will_wake(right))).unwrap_or(false)
}

fn try_clone_waker(waker: &Waker) -> Option<Waker> {
    catch_unwind(AssertUnwindSafe(|| waker.clone())).ok()
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_unpoisoned<'a, T>(condvar: &Condvar, guard: MutexGuard<'a, T>) -> MutexGuard<'a, T> {
    condvar
        .wait(guard)
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn wait_timeout_unpoisoned<'a, T>(
    condvar: &Condvar,
    guard: MutexGuard<'a, T>,
    timeout: Duration,
) -> (MutexGuard<'a, T>, bool) {
    match condvar.wait_timeout(guard, timeout) {
        Ok((guard, result)) => (guard, result.timed_out()),
        Err(poisoned) => {
            let (guard, result) = poisoned.into_inner();
            (guard, result.timed_out())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::task::Wake;

    use crate::cache::CacheConfig;
    use crate::format::SUPERBLOCK_AREA_SIZE;
    use crate::policy::NamespaceConfig;

    type TestResult = Result<u64, AsyncFailure>;

    fn failure(error: AsyncFailure) -> TestResult {
        Err(error)
    }

    static NEXT_CACHE_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestCacheFile(PathBuf);

    impl TestCacheFile {
        fn new() -> Self {
            let nonce = NEXT_CACHE_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "cache-rs-async-namespace-{}-{nonce}.cache",
                std::process::id()
            )))
        }
    }

    impl Drop for TestCacheFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    struct ChannelWake(mpsc::Sender<()>);

    impl Wake for ChannelWake {
        fn wake(self: Arc<Self>) {
            let _ = self.0.send(());
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _ = self.0.send(());
        }
    }

    struct PanicWake;

    impl Wake for PanicWake {
        fn wake(self: Arc<Self>) {
            panic!("expected waker panic");
        }

        fn wake_by_ref(self: &Arc<Self>) {
            panic!("expected waker panic");
        }
    }

    #[test]
    fn read_worker_count_tracks_queue_depths_up_to_the_hard_cap() {
        assert_eq!(async_read_worker_count(1, 128), 1);
        assert_eq!(async_read_worker_count(16, 8), 8);
        assert_eq!(async_read_worker_count(64, 128), 64);
        assert_eq!(async_read_worker_count(256, 256), 128);
    }

    #[test]
    fn async_namespaces_are_isolated_and_legacy_methods_route_to_zero() {
        let file = TestCacheFile::new();
        let region_size = 16 * 1024;
        let cache = CacheConfig::new(&file.0, SUPERBLOCK_AREA_SIZE + 3 * region_size)
            .with_region_size(region_size)
            .with_index_slots(64)
            .with_max_key_size(256)
            .with_max_value_size(2048)
            .with_namespace(NamespaceConfig::new(1))
            .with_namespace(NamespaceConfig::new(2))
            .open()
            .unwrap();
        let asynchronous = cache.async_handle().unwrap();

        assert_eq!(
            asynchronous
                .put("shared", "zero", PutOptions::default())
                .wait()
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            asynchronous
                .put_in_with_options(
                    1,
                    "shared",
                    "one",
                    PutOptions::default(),
                    AsyncRequestOptions::default(),
                )
                .wait()
                .unwrap(),
            PutOutcome::Stored
        );
        assert_eq!(
            asynchronous
                .put_in(2, "shared", "two", PutOptions::default())
                .wait()
                .unwrap(),
            PutOutcome::Stored
        );

        assert_eq!(
            asynchronous
                .get_with_options("shared", AsyncRequestOptions::default())
                .wait()
                .unwrap(),
            Some(b"zero".to_vec())
        );
        assert_eq!(
            asynchronous
                .get_in_with_options(1, "shared", AsyncRequestOptions::default())
                .wait()
                .unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(
            asynchronous.get_in(2, "shared").wait().unwrap(),
            Some(b"two".to_vec())
        );

        assert_eq!(
            asynchronous
                .remove_in_with_options(1, "shared", AsyncRequestOptions::default())
                .wait()
                .unwrap(),
            RemoveOutcome::Removed
        );
        assert_eq!(asynchronous.get_in(1, "shared").wait().unwrap(), None);
        assert_eq!(
            asynchronous.get("shared").wait().unwrap(),
            Some(b"zero".to_vec())
        );
        assert_eq!(
            asynchronous.remove_in(2, "shared").wait().unwrap(),
            RemoveOutcome::Removed
        );

        asynchronous.close().wait().unwrap();
    }

    #[test]
    fn cache_request_future_registers_and_wakes_its_waker() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let mut request =
            executor.submit_read(AsyncRequestOptions::default(), failure, move |_| {
                release_rx.recv().unwrap();
                Ok(5)
            });
        let (wake_tx, wake_rx) = mpsc::channel();
        let waker = Waker::from(Arc::new(ChannelWake(wake_tx)));
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut request).poll(&mut context).is_pending());

        release_tx.send(()).unwrap();
        wake_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(
            Pin::new(&mut request).poll(&mut context),
            Poll::Ready(Ok(5))
        );

        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn read_queue_is_bounded_and_drains_accepted_work() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::channel();

        let first_gate = Arc::clone(&gate);
        let first = executor.submit_read(AsyncRequestOptions::default(), failure, move |_| {
            started_tx.send(()).unwrap();
            let (released, changed) = &*first_gate;
            let mut released = lock_unpoisoned(released);
            while !*released {
                released = wait_unpoisoned(changed, released);
            }
            Ok(1)
        });
        started_rx.recv().unwrap();

        let second = executor.submit_read(AsyncRequestOptions::default(), failure, |_| Ok(2));
        let rejected = executor.submit_read(AsyncRequestOptions::default(), failure, |_| Ok(3));
        assert_eq!(rejected.wait(), Err(AsyncFailure::QueueFull));

        let drain = executor.begin_drain();
        assert_eq!(
            executor
                .submit_read(AsyncRequestOptions::default(), failure, |_| Ok(4))
                .wait(),
            Err(AsyncFailure::Closed)
        );
        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        assert_eq!(first.wait(), Ok(1));
        assert_eq!(second.wait(), Ok(2));
        drain.wait();
        assert!(!executor.join());
    }

    #[test]
    fn running_read_cancel_wakes_caller_and_invokes_engine_hook() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_called = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = mpsc::channel();

        let task_gate = Arc::clone(&gate);
        let task_hook_called = Arc::clone(&hook_called);
        let request =
            executor.submit_read(AsyncRequestOptions::default(), failure, move |context| {
                context.set_stop_hook(move |_| {
                    task_hook_called.store(true, Ordering::Release);
                });
                started_tx.send(()).unwrap();
                let (released, changed) = &*task_gate;
                let mut released = lock_unpoisoned(released);
                while !*released {
                    released = wait_unpoisoned(changed, released);
                }
                assert_eq!(context.stop_reason(), Some(AsyncFailure::Cancelled));
                Ok(1)
            });
        started_rx.recv().unwrap();

        assert_eq!(request.cancel(), CancelOutcome::Requested);
        assert_eq!(request.wait(), Err(AsyncFailure::Cancelled));
        assert!(hook_called.load(Ordering::Acquire));

        let drain = executor.begin_drain();
        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        drain.wait();
        assert!(!executor.join());
    }

    #[test]
    fn timeout_returns_while_read_task_safely_drains() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let hook_called = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = mpsc::channel();

        let task_gate = Arc::clone(&gate);
        let task_hook_called = Arc::clone(&hook_called);
        let request = executor.submit_read(
            AsyncRequestOptions::with_timeout(Duration::from_millis(20)),
            failure,
            move |context| {
                context.set_stop_hook(move |reason| {
                    assert_eq!(reason, AsyncFailure::TimedOut);
                    task_hook_called.store(true, Ordering::Release);
                });
                started_tx.send(()).unwrap();
                let (released, changed) = &*task_gate;
                let mut released = lock_unpoisoned(released);
                while !*released {
                    released = wait_unpoisoned(changed, released);
                }
                assert!(context.is_stopped());
                Ok(1)
            },
        );
        started_rx.recv().unwrap();

        assert_eq!(request.wait(), Err(AsyncFailure::TimedOut));
        assert!(hook_called.load(Ordering::Acquire));

        let drain = executor.begin_drain();
        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        drain.wait();
        assert!(!executor.join());
    }

    #[test]
    fn committed_mutation_ignores_cancel_and_returns_real_result() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::channel();

        let task_gate = Arc::clone(&gate);
        let request = executor.submit_mutation(
            AsyncRequestOptions::with_timeout(Duration::from_millis(20)),
            failure,
            move |_| {
                started_tx.send(()).unwrap();
                let (released, changed) = &*task_gate;
                let mut released = lock_unpoisoned(released);
                while !*released {
                    released = wait_unpoisoned(changed, released);
                }
                Ok(7)
            },
        );
        started_rx.recv().unwrap();
        assert_eq!(request.cancel(), CancelOutcome::TooLate);

        std::thread::sleep(Duration::from_millis(30));
        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        assert_eq!(request.wait(), Ok(7));

        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn mutations_run_concurrently_without_passing_control_barriers() {
        let executor = AsyncExecutor::try_new(1, 8, 1, 3).unwrap();
        let ordinary_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (ordinary_started_tx, ordinary_started_rx) = mpsc::channel();

        let first_gate = Arc::clone(&ordinary_gate);
        let first_started = ordinary_started_tx.clone();
        let first = executor.submit_mutation(AsyncRequestOptions::default(), failure, move |_| {
            first_started.send(()).unwrap();
            let (released, changed) = &*first_gate;
            let mut released = lock_unpoisoned(released);
            while !*released {
                released = wait_unpoisoned(changed, released);
            }
            Ok(1)
        });
        let second_gate = Arc::clone(&ordinary_gate);
        let second =
            executor.submit_reserved_mutation(AsyncRequestOptions::default(), failure, move |_| {
                ordinary_started_tx.send(()).unwrap();
                let (released, changed) = &*second_gate;
                let mut released = lock_unpoisoned(released);
                while !*released {
                    released = wait_unpoisoned(changed, released);
                }
                Ok(2)
            });

        let first_ordinary_started = ordinary_started_rx
            .recv_timeout(Duration::from_secs(1))
            .is_ok();
        let second_ordinary_started = ordinary_started_rx
            .recv_timeout(Duration::from_secs(1))
            .is_ok();

        let control_gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (control_started_tx, control_started_rx) = mpsc::channel();
        let task_control_gate = Arc::clone(&control_gate);
        let control = executor.submit_control(AsyncRequestOptions::default(), failure, move |_| {
            control_started_tx.send(()).unwrap();
            let (released, changed) = &*task_control_gate;
            let mut released = lock_unpoisoned(released);
            while !*released {
                released = wait_unpoisoned(changed, released);
            }
            Ok(3)
        });
        let (later_started_tx, later_started_rx) = mpsc::channel();
        let later = executor.submit_mutation(AsyncRequestOptions::default(), failure, move |_| {
            later_started_tx.send(()).unwrap();
            Ok(4)
        });

        let control_started_early = control_started_rx
            .recv_timeout(Duration::from_millis(30))
            .is_ok();
        let later_started_early = later_started_rx
            .recv_timeout(Duration::from_millis(30))
            .is_ok();
        {
            let (released, changed) = &*ordinary_gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }

        let control_started = control_started_early
            || control_started_rx
                .recv_timeout(Duration::from_secs(1))
                .is_ok();
        let later_started_before_control = later_started_early
            || later_started_rx
                .recv_timeout(Duration::from_millis(30))
                .is_ok();
        {
            let (released, changed) = &*control_gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        let later_started = later_started_before_control
            || later_started_rx
                .recv_timeout(Duration::from_secs(1))
                .is_ok();

        assert_eq!(first.wait(), Ok(1));
        assert_eq!(second.wait(), Ok(2));
        assert_eq!(control.wait(), Ok(3));
        assert_eq!(later.wait(), Ok(4));
        assert!(first_ordinary_started && second_ordinary_started);
        assert!(!control_started_early);
        assert!(control_started);
        assert!(!later_started_before_control);
        assert!(later_started);
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn control_completion_wakes_the_concurrent_ordinary_prefix() {
        let executor = AsyncExecutor::try_new(1, 8, 1, 3).unwrap();
        let (control_release_tx, control_release_rx) = mpsc::channel();
        let (control_started_tx, control_started_rx) = mpsc::channel();
        let control = executor.submit_control(AsyncRequestOptions::default(), failure, move |_| {
            control_started_tx.send(()).unwrap();
            control_release_rx.recv().unwrap();
            Ok(0)
        });
        control_started_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::channel();
        let mut requests = Vec::new();
        for value in 1..=3 {
            let gate = Arc::clone(&gate);
            let started_tx = started_tx.clone();
            requests.push(executor.submit_mutation(
                AsyncRequestOptions::default(),
                failure,
                move |_| {
                    started_tx.send(()).unwrap();
                    let (released, changed) = &*gate;
                    let mut released = lock_unpoisoned(released);
                    while !*released {
                        released = wait_unpoisoned(changed, released);
                    }
                    Ok(value)
                },
            ));
        }

        control_release_tx.send(()).unwrap();
        for _ in 0..3 {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }
        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        assert_eq!(control.wait(), Ok(0));
        for (request, expected) in requests.into_iter().zip(1..=3) {
            assert_eq!(request.wait(), Ok(expected));
        }
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn worker_panic_completes_request_and_does_not_kill_lane() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let panicked =
            executor.submit_mutation(AsyncRequestOptions::default(), failure, |_| -> TestResult {
                panic!("expected task panic")
            });
        assert_eq!(panicked.wait(), Err(AsyncFailure::WorkerPanicked));

        let next = executor.submit_mutation(AsyncRequestOptions::default(), failure, |_| Ok(9));
        assert_eq!(next.wait(), Ok(9));
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn cancelling_a_queued_request_releases_capacity_immediately() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::channel();

        let first_gate = Arc::clone(&gate);
        let first = executor.submit_read(AsyncRequestOptions::default(), failure, move |_| {
            started_tx.send(()).unwrap();
            let (released, changed) = &*first_gate;
            let mut released = lock_unpoisoned(released);
            while !*released {
                released = wait_unpoisoned(changed, released);
            }
            Ok(1)
        });
        started_rx.recv().unwrap();

        let cancelled = executor.submit_read(AsyncRequestOptions::default(), failure, |_| Ok(2));
        assert_eq!(cancelled.cancel(), CancelOutcome::Requested);
        assert_eq!(cancelled.wait(), Err(AsyncFailure::Cancelled));
        let replacement = executor.submit_read(AsyncRequestOptions::default(), failure, |_| Ok(3));
        assert_eq!(executor.snapshot().read_queued, 1);

        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        assert_eq!(first.wait(), Ok(1));
        assert_eq!(replacement.wait(), Ok(3));
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn control_requests_use_two_slots_reserved_from_puts() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let (started_tx, started_rx) = mpsc::channel();

        let first_gate = Arc::clone(&gate);
        let first = executor.submit_mutation(AsyncRequestOptions::default(), failure, move |_| {
            started_tx.send(()).unwrap();
            let (released, changed) = &*first_gate;
            let mut released = lock_unpoisoned(released);
            while !*released {
                released = wait_unpoisoned(changed, released);
            }
            Ok(1)
        });
        started_rx.recv().unwrap();

        let queued_put =
            executor.submit_mutation(AsyncRequestOptions::default(), failure, |_| Ok(2));
        let rejected_put =
            executor.submit_mutation(AsyncRequestOptions::default(), failure, |_| Ok(3));
        assert_eq!(rejected_put.wait(), Err(AsyncFailure::QueueFull));

        let control_one =
            executor.submit_control(AsyncRequestOptions::default(), failure, |_| Ok(4));
        let control_two =
            executor.submit_control(AsyncRequestOptions::default(), failure, |_| Ok(5));
        let rejected_control =
            executor.submit_control(AsyncRequestOptions::default(), failure, |_| Ok(6));
        assert_eq!(rejected_control.wait(), Err(AsyncFailure::QueueFull));
        let stats = executor.snapshot();
        assert_eq!(stats.ordinary_mutation_queued, 1);
        assert_eq!(stats.control_mutation_queued, 2);
        assert_eq!(stats.control_queue_reserve, 2);

        {
            let (released, changed) = &*gate;
            *lock_unpoisoned(released) = true;
            changed.notify_all();
        }
        assert_eq!(first.wait(), Ok(1));
        assert_eq!(queued_put.wait(), Ok(2));
        assert_eq!(control_one.wait(), Ok(4));
        assert_eq!(control_two.wait(), Ok(5));
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn panicking_completion_waker_does_not_kill_worker_or_stall_drain() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let mut request =
            executor.submit_read(AsyncRequestOptions::default(), failure, move |_| {
                release_rx.recv().unwrap();
                Ok(1)
            });
        let waker = Waker::from(Arc::new(PanicWake));
        let mut context = Context::from_waker(&waker);
        assert!(Pin::new(&mut request).poll(&mut context).is_pending());
        release_tx.send(()).unwrap();
        assert_eq!(request.wait(), Ok(1));

        let next = executor.submit_read(AsyncRequestOptions::default(), failure, |_| Ok(2));
        assert_eq!(next.wait(), Ok(2));
        executor.begin_drain().wait();
        assert!(!executor.join());
    }

    #[test]
    fn close_completion_wakes_all_waiters_and_is_uncancellable() {
        let completion = Arc::new(CloseCompletion::new());
        let first = AsyncCloseFuture {
            completion: Arc::clone(&completion),
        };
        let second = AsyncCloseFuture {
            completion: Arc::clone(&completion),
        };
        assert_eq!(first.cancel(), CancelOutcome::TooLate);

        completion.complete(false);
        assert!(matches!(first.wait(), Err(CacheError::Poisoned)));
        assert!(matches!(second.wait(), Err(CacheError::Poisoned)));
        let completed = AsyncCloseFuture { completion };
        assert_eq!(completed.cancel(), CancelOutcome::Completed);
        assert!(matches!(completed.wait(), Err(CacheError::Poisoned)));
    }

    #[test]
    fn drain_rejects_a_slot_reserved_before_formal_enqueue() {
        let executor = AsyncExecutor::try_new(1, 1, 1, 1).unwrap();
        let reservation = executor.reserve_read().unwrap();
        assert_eq!(executor.snapshot().read_reserved, 1);

        let drain = executor.begin_drain();
        let request = executor.submit_read_reserved(
            reservation,
            AsyncRequestOptions::default(),
            failure,
            |_| Ok(1),
        );
        assert_eq!(request.wait(), Err(AsyncFailure::Closed));
        assert_eq!(executor.snapshot().read_reserved, 0);
        drain.wait();
        assert!(!executor.join());
    }

    #[test]
    fn put_queue_full_is_an_explicit_rejection() {
        assert!(matches!(
            map_put_failure(AsyncFailure::QueueFull),
            Ok(PutOutcome::Rejected(RejectReason::SubmissionFull))
        ));
    }

    #[test]
    fn one_close_owner_runs_while_all_other_callers_wait_for_its_result() {
        let inner = Arc::new(AsyncInner {
            cache: Weak::new(),
            executor: Arc::new(AsyncExecutor::try_new(1, 1, 1, 1).unwrap()),
            close_state: AtomicU8::new(CLOSE_OPEN),
            close_completion: Arc::new(CloseCompletion::new()),
        });
        let owners = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::new();
        for _ in 0..4 {
            let inner = Arc::clone(&inner);
            let owners = Arc::clone(&owners);
            callers.push(thread::spawn(move || {
                if inner.begin_close() {
                    owners.fetch_add(1, Ordering::AcqRel);
                    let worker_panicked = inner.drain_and_join();
                    inner.finish_close(!worker_panicked);
                    Ok(())
                } else {
                    inner.wait_close_result()
                }
            }));
        }
        for caller in callers {
            assert!(caller.join().unwrap().is_ok());
        }
        assert_eq!(owners.load(Ordering::Acquire), 1);
    }
}
