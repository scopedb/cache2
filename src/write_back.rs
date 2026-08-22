//! Bounded executor for Hybrid dirty-entry persistence.
//!
//! A caller reserves both one submission slot and the complete owned-copy
//! charge before cloning a dirty L1 entry. Foreground eviction waits for its
//! positioned-I/O task; best-effort proactive work uses only the background
//! share. This keeps the L1-miss/L2-stale window closed while allowing
//! independent memory shards to drive several NVMe writes concurrently.

use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::resources::{BackpressurePolicy, OverloadReason};

type Task = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct WriteBackSnapshot {
    pub(crate) queue_capacity: u64,
    pub(crate) memory_capacity_bytes: u64,
    pub(crate) in_flight: u64,
    pub(crate) in_flight_peak: u64,
    pub(crate) bytes_in_use: u64,
    pub(crate) bytes_peak: u64,
    pub(crate) submitted: u64,
    pub(crate) completed: u64,
    pub(crate) rejected: u64,
    pub(crate) worker_panics: u64,
    pub(crate) wait_ns: u64,
}

#[derive(Debug)]
pub(crate) enum WriteBackRunError {
    Overloaded(OverloadReason),
    Closed,
    WorkerPanicked,
}

impl fmt::Display for WriteBackRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overloaded(reason) => write!(formatter, "write-back executor: {reason}"),
            Self::Closed => formatter.write_str("write-back executor is closed"),
            Self::WorkerPanicked => formatter.write_str("write-back worker panicked"),
        }
    }
}

struct GateState {
    in_flight: usize,
    bytes_in_use: usize,
    background_in_flight: usize,
    background_bytes_in_use: usize,
    background_pauses: usize,
    closed: bool,
}

struct WriteBackGate {
    max_in_flight: usize,
    max_bytes: usize,
    backpressure: BackpressurePolicy,
    state: Mutex<GateState>,
    available: Condvar,
    in_flight_peak: AtomicU64,
    bytes_peak: AtomicU64,
    submitted: AtomicU64,
    completed: AtomicU64,
    rejected: AtomicU64,
    worker_panics: AtomicU64,
    wait_ns: AtomicU64,
}

pub(crate) struct WriteBackReservation {
    gate: Arc<WriteBackGate>,
    bytes: usize,
    background: bool,
    active: bool,
}

pub(crate) struct WriteBackBackgroundPause {
    gate: Arc<WriteBackGate>,
    active: bool,
}

pub(crate) struct WriteBackExecutor {
    sender: Mutex<Option<SyncSender<Task>>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
    gate: Arc<WriteBackGate>,
}

impl WriteBackExecutor {
    pub(crate) fn try_new(
        queue_depth: usize,
        workers: usize,
        memory_bytes: usize,
        backpressure: BackpressurePolicy,
    ) -> std::io::Result<Self> {
        let (sender, receiver) = sync_channel(queue_depth);
        let receiver = Arc::new(Mutex::new(receiver));
        let mut handles = Vec::new();
        handles.try_reserve_exact(workers).map_err(|_| {
            std::io::Error::other("write-back worker bookkeeping allocation failed")
        })?;
        for index in 0..workers {
            let receiver = Arc::clone(&receiver);
            match thread::Builder::new()
                .name(format!("cache-rs-hybrid-writeback-{index}"))
                .spawn(move || worker_loop(receiver))
            {
                Ok(handle) => handles.push(handle),
                Err(error) => {
                    drop(sender);
                    for handle in handles {
                        let _ = handle.join();
                    }
                    return Err(error);
                }
            }
        }
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            workers: Mutex::new(handles),
            gate: Arc::new(WriteBackGate {
                max_in_flight: queue_depth,
                max_bytes: memory_bytes,
                backpressure,
                state: Mutex::new(GateState {
                    in_flight: 0,
                    bytes_in_use: 0,
                    background_in_flight: 0,
                    background_bytes_in_use: 0,
                    background_pauses: 0,
                    closed: false,
                }),
                available: Condvar::new(),
                in_flight_peak: AtomicU64::new(0),
                bytes_peak: AtomicU64::new(0),
                submitted: AtomicU64::new(0),
                completed: AtomicU64::new(0),
                rejected: AtomicU64::new(0),
                worker_panics: AtomicU64::new(0),
                wait_ns: AtomicU64::new(0),
            }),
        })
    }

    /// Reserve the complete owned-copy charge before allocating it.
    pub(crate) fn reserve(&self, bytes: usize) -> Result<WriteBackReservation, WriteBackRunError> {
        self.gate.reserve(bytes)
    }

    /// Reserve one bounded background-cleaner task without waiting behind
    /// foreground pressure. Failure deliberately leaves the L1 entry dirty so
    /// the existing synchronous eviction fallback can preserve correctness.
    pub(crate) fn try_reserve_background(&self, bytes: usize) -> Option<WriteBackReservation> {
        self.gate.try_reserve_background(bytes)
    }

    /// Stop proactive admission and drain every already accepted background
    /// task. Regular foreground demotion remains available while paused.
    pub(crate) fn pause_background(&self) -> WriteBackBackgroundPause {
        self.gate.pause_background();
        WriteBackBackgroundPause {
            gate: Arc::clone(&self.gate),
            active: true,
        }
    }

    pub(crate) fn memory_capacity_bytes(&self) -> usize {
        self.gate.max_bytes
    }

    pub(crate) fn parallelism(&self) -> usize {
        lock_unpoisoned(&self.workers).len().max(1)
    }

    pub(crate) fn run<T, F>(
        &self,
        reservation: WriteBackReservation,
        operation: F,
    ) -> Result<T, WriteBackRunError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let completion = Arc::new(Completion::new());
        let worker_completion = Arc::clone(&completion);
        let gate = Arc::clone(&self.gate);
        let task: Task = Box::new(move || {
            let outcome = catch_unwind(AssertUnwindSafe(operation))
                .map_err(|_| WriteBackRunError::WorkerPanicked);
            if outcome.is_err() {
                gate.worker_panics.fetch_add(1, Ordering::Relaxed);
            } else {
                gate.completed.fetch_add(1, Ordering::Relaxed);
            }
            worker_completion.complete(outcome);
            drop(reservation);
        });
        let sender_guard = lock_unpoisoned(&self.sender);
        let Some(sender) = sender_guard.as_ref() else {
            drop(task);
            return Err(WriteBackRunError::Closed);
        };
        match sender.try_send(task) {
            Ok(()) => {
                self.gate.submitted.fetch_add(1, Ordering::Relaxed);
                drop(sender_guard);
                completion.wait()
            }
            Err(TrySendError::Full(task)) => {
                // The gate covers queued and executing jobs, so a full channel
                // here can only be a shutdown/race invariant violation. Fail
                // closed and keep the dirty victim resident.
                drop(task);
                self.gate.rejected.fetch_add(1, Ordering::Relaxed);
                Err(WriteBackRunError::Overloaded(
                    OverloadReason::WriteQueueFull,
                ))
            }
            Err(TrySendError::Disconnected(task)) => {
                drop(task);
                Err(WriteBackRunError::Closed)
            }
        }
    }

    pub(crate) fn submit_background<F, P>(
        &self,
        reservation: WriteBackReservation,
        operation: F,
        on_panic: P,
    ) -> Result<(), WriteBackRunError>
    where
        F: FnOnce() + Send + 'static,
        P: FnOnce() + Send + 'static,
    {
        debug_assert!(reservation.background);
        let gate = Arc::clone(&self.gate);
        let task: Task = Box::new(move || {
            if catch_unwind(AssertUnwindSafe(operation)).is_err() {
                gate.worker_panics.fetch_add(1, Ordering::Relaxed);
                on_panic();
            } else {
                gate.completed.fetch_add(1, Ordering::Relaxed);
            }
            drop(reservation);
        });
        let sender_guard = lock_unpoisoned(&self.sender);
        let Some(sender) = sender_guard.as_ref() else {
            drop(task);
            return Err(WriteBackRunError::Closed);
        };
        match sender.try_send(task) {
            Ok(()) => {
                self.gate.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(task)) => {
                // A background reservation consumes one of the same bounded
                // slots as the queue. Full is therefore only a shutdown race or
                // invariant failure, never ordinary backpressure.
                drop(task);
                Err(WriteBackRunError::Overloaded(
                    OverloadReason::WriteQueueFull,
                ))
            }
            Err(TrySendError::Disconnected(task)) => {
                drop(task);
                Err(WriteBackRunError::Closed)
            }
        }
    }

    pub(crate) fn snapshot(&self) -> WriteBackSnapshot {
        self.gate.snapshot()
    }

    /// Stop new reservations, close the channel, and join every worker.
    /// Returns false if a worker escaped the per-task panic boundary.
    pub(crate) fn shutdown(&self) -> bool {
        self.gate.close();
        drop(lock_unpoisoned(&self.sender).take());
        let handles = std::mem::take(&mut *lock_unpoisoned(&self.workers));
        let mut healthy = true;
        for handle in handles {
            healthy &= handle.join().is_ok();
        }
        healthy
    }
}

impl Drop for WriteBackExecutor {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

impl WriteBackGate {
    fn reserve(self: &Arc<Self>, bytes: usize) -> Result<WriteBackReservation, WriteBackRunError> {
        let started = Instant::now();
        if bytes > self.max_bytes {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(WriteBackRunError::Overloaded(
                OverloadReason::WriteBufferUnavailable,
            ));
        }
        let deadline = match self.backpressure {
            BackpressurePolicy::Timeout(timeout) => Instant::now().checked_add(timeout),
            BackpressurePolicy::Reject | BackpressurePolicy::Block => None,
        };
        let mut state = lock_unpoisoned(&self.state);
        loop {
            if state.closed {
                add_duration(&self.wait_ns, started.elapsed());
                return Err(WriteBackRunError::Closed);
            }
            let slots_full = state.in_flight >= self.max_in_flight;
            let bytes_full = bytes > self.max_bytes.saturating_sub(state.bytes_in_use);
            if !slots_full && !bytes_full {
                break;
            }
            state = match self.backpressure {
                BackpressurePolicy::Reject => {
                    self.rejected.fetch_add(1, Ordering::Relaxed);
                    add_duration(&self.wait_ns, started.elapsed());
                    return Err(WriteBackRunError::Overloaded(if bytes_full {
                        OverloadReason::WriteBufferUnavailable
                    } else {
                        OverloadReason::WriteQueueFull
                    }));
                }
                BackpressurePolicy::Block => self
                    .available
                    .wait(state)
                    .unwrap_or_else(|poisoned| poisoned.into_inner()),
                BackpressurePolicy::Timeout(_) => {
                    let Some(remaining) = deadline
                        .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                    else {
                        self.rejected.fetch_add(1, Ordering::Relaxed);
                        add_duration(&self.wait_ns, started.elapsed());
                        return Err(WriteBackRunError::Overloaded(OverloadReason::WriteTimeout));
                    };
                    let (next, timed_out) = self
                        .available
                        .wait_timeout(state, remaining)
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if timed_out.timed_out()
                        && (next.in_flight >= self.max_in_flight
                            || bytes > self.max_bytes.saturating_sub(next.bytes_in_use))
                    {
                        self.rejected.fetch_add(1, Ordering::Relaxed);
                        add_duration(&self.wait_ns, started.elapsed());
                        return Err(WriteBackRunError::Overloaded(OverloadReason::WriteTimeout));
                    }
                    next
                }
            };
        }
        state.in_flight += 1;
        state.bytes_in_use += bytes;
        update_peak(&self.in_flight_peak, state.in_flight as u64);
        update_peak(&self.bytes_peak, state.bytes_in_use as u64);
        add_duration(&self.wait_ns, started.elapsed());
        Ok(WriteBackReservation {
            gate: Arc::clone(self),
            bytes,
            background: false,
            active: true,
        })
    }

    fn try_reserve_background(self: &Arc<Self>, bytes: usize) -> Option<WriteBackReservation> {
        if bytes > self.max_bytes {
            return None;
        }
        let mut state = lock_unpoisoned(&self.state);
        // Best-effort cleaning may use at most half the copy budget and must
        // leave one submission slot for mandatory eviction demotion. This
        // bounds optional interference; a depth of one disables proactive work.
        let background_slot_limit = self.max_in_flight.saturating_sub(1);
        let background_byte_limit = self.max_bytes / 2;
        let slots_full = state.in_flight >= self.max_in_flight;
        let bytes_full = bytes > self.max_bytes.saturating_sub(state.bytes_in_use);
        let background_slots_full = state.background_in_flight >= background_slot_limit;
        let background_bytes_full =
            bytes > background_byte_limit.saturating_sub(state.background_bytes_in_use);
        if state.closed
            || state.background_pauses != 0
            || slots_full
            || bytes_full
            || background_slots_full
            || background_bytes_full
        {
            return None;
        }
        state.in_flight += 1;
        state.background_in_flight += 1;
        state.bytes_in_use += bytes;
        state.background_bytes_in_use += bytes;
        update_peak(&self.in_flight_peak, state.in_flight as u64);
        update_peak(&self.bytes_peak, state.bytes_in_use as u64);
        Some(WriteBackReservation {
            gate: Arc::clone(self),
            bytes,
            background: true,
            active: true,
        })
    }

    fn release(&self, bytes: usize, background: bool) {
        let mut state = lock_unpoisoned(&self.state);
        debug_assert!(state.in_flight != 0 && state.bytes_in_use >= bytes);
        state.in_flight = state.in_flight.saturating_sub(1);
        state.bytes_in_use = state.bytes_in_use.saturating_sub(bytes);
        if background {
            debug_assert!(
                state.background_in_flight != 0 && state.background_bytes_in_use >= bytes
            );
            state.background_in_flight = state.background_in_flight.saturating_sub(1);
            state.background_bytes_in_use = state.background_bytes_in_use.saturating_sub(bytes);
        }
        self.available.notify_all();
    }

    fn pause_background(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.background_pauses = state.background_pauses.saturating_add(1);
        while state.background_in_flight != 0 {
            state = self
                .available
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn resume_background(&self) {
        let mut state = lock_unpoisoned(&self.state);
        debug_assert!(state.background_pauses != 0);
        state.background_pauses = state.background_pauses.saturating_sub(1);
        self.available.notify_all();
    }

    fn close(&self) {
        let mut state = lock_unpoisoned(&self.state);
        state.closed = true;
        self.available.notify_all();
    }

    fn snapshot(&self) -> WriteBackSnapshot {
        let state = lock_unpoisoned(&self.state);
        WriteBackSnapshot {
            queue_capacity: self.max_in_flight as u64,
            memory_capacity_bytes: self.max_bytes as u64,
            in_flight: state.in_flight as u64,
            in_flight_peak: self
                .in_flight_peak
                .load(Ordering::Relaxed)
                .max(state.in_flight as u64),
            bytes_in_use: state.bytes_in_use as u64,
            bytes_peak: self
                .bytes_peak
                .load(Ordering::Relaxed)
                .max(state.bytes_in_use as u64),
            submitted: self.submitted.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            worker_panics: self.worker_panics.load(Ordering::Relaxed),
            wait_ns: self.wait_ns.load(Ordering::Relaxed),
        }
    }
}

impl Drop for WriteBackReservation {
    fn drop(&mut self) {
        if self.active {
            self.gate.release(self.bytes, self.background);
            self.active = false;
        }
    }
}

impl Drop for WriteBackBackgroundPause {
    fn drop(&mut self) {
        if self.active {
            self.gate.resume_background();
            self.active = false;
        }
    }
}

struct Completion<T> {
    value: Mutex<Option<T>>,
    ready: Condvar,
}

impl<T> Completion<T> {
    fn new() -> Self {
        Self {
            value: Mutex::new(None),
            ready: Condvar::new(),
        }
    }

    fn complete(&self, value: T) {
        *lock_unpoisoned(&self.value) = Some(value);
        self.ready.notify_one();
    }

    fn wait(&self) -> T {
        let mut value = lock_unpoisoned(&self.value);
        loop {
            if let Some(value) = value.take() {
                return value;
            }
            value = self
                .ready
                .wait(value)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

fn worker_loop(receiver: Arc<Mutex<Receiver<Task>>>) {
    loop {
        let task = lock_unpoisoned(&receiver).recv();
        match task {
            Ok(task) => task(),
            Err(_) => return,
        }
    }
}

fn add_duration(counter: &AtomicU64, duration: Duration) {
    let nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(nanos))
    });
}

fn update_peak(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        (value > current).then_some(value)
    });
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn reservations_bound_owned_bytes_and_shutdown_joins_workers() {
        let executor = WriteBackExecutor::try_new(2, 2, 16, BackpressurePolicy::Reject).unwrap();
        let first = executor.reserve(12).unwrap();
        assert!(matches!(
            executor.reserve(5),
            Err(WriteBackRunError::Overloaded(
                OverloadReason::WriteBufferUnavailable
            ))
        ));
        let completed = Arc::new(AtomicUsize::new(0));
        let worker_completed = Arc::clone(&completed);
        assert_eq!(
            executor
                .run(first, move || {
                    worker_completed.fetch_add(1, Ordering::Relaxed);
                    7
                })
                .unwrap(),
            7
        );
        assert_eq!(completed.load(Ordering::Relaxed), 1);
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.in_flight, 0);
        assert_eq!(snapshot.bytes_in_use, 0);
        assert_eq!(snapshot.submitted, 1);
        assert_eq!(snapshot.completed, 1);
        assert_eq!(snapshot.rejected, 1);
        assert!(executor.shutdown());
        assert!(matches!(
            executor.reserve(1),
            Err(WriteBackRunError::Closed)
        ));
    }

    #[test]
    fn background_admission_reserves_foreground_slot_and_memory() {
        let executor = WriteBackExecutor::try_new(2, 1, 16, BackpressurePolicy::Reject).unwrap();
        let background = executor.try_reserve_background(8).unwrap();
        assert!(executor.try_reserve_background(1).is_none());

        let foreground = executor.reserve(8).unwrap();
        let snapshot = executor.snapshot();
        assert_eq!(snapshot.in_flight, 2);
        assert_eq!(snapshot.bytes_in_use, 16);
        assert_eq!(snapshot.rejected, 0);

        drop(foreground);
        drop(background);
        assert!(executor.shutdown());
    }
}
