//! Single bounded asynchronous facade for the mixed-object Hybrid cache.
//!
//! Slow-path queue reservations and the Hybrid byte gate are both acquired
//! before caller inputs are copied. A no-deadline live L1 hit completes on the
//! caller without consuming a read-worker slot. Mutation cancellation becomes
//! advisory once a worker starts, matching the synchronous commit contract.

use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::{Duration, Instant};

use crate::async_cache::{
    AsyncExecutor, AsyncQueueStats, AsyncRequestOptions, CacheFuture, CancelOutcome, copy_input,
    map_put_failure, map_read_failure, map_write_failure, ready_future,
};
use crate::cache::{
    CacheError, CacheStatus, PutOptions, PutOutcome, RejectReason, RemoveOutcome, Result,
};
use crate::hybrid::{HybridAsyncConfig, HybridCache, HybridInner, HybridLookupOutcome};
use crate::policy::NamespaceId;
use crate::resources::OverloadReason;

const CLOSE_OPEN: u8 = 0;
const CLOSE_RUNNING: u8 = 1;
const CLOSE_DONE: u8 = 2;
const COMPLETION_PENDING: u8 = 0;
const COMPLETION_SUCCEEDED: u8 = 1;
const COMPLETION_FAILED: u8 = 2;
const MAX_CLOSE_FUTURE_WAITERS: usize = 64;

/// Future-based facade over one [`HybridCache`]. Every handle shares the same
/// bounded executor and shutdown barrier.
#[derive(Clone)]
pub struct AsyncHybridCache {
    inner: Arc<AsyncHybridInner>,
}

/// Point-in-time state of the bounded Hybrid shutdown completion.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AsyncHybridCloseStats {
    pub draining: bool,
    pub completed: bool,
    pub succeeded: bool,
    pub registered_waiters: u64,
    pub registered_waiters_peak: u64,
    pub waiter_rejections: u64,
    pub timed_out_waits: u64,
    pub drain_duration_ns: u64,
}

pub(crate) struct AsyncHybridInner {
    cache: Arc<HybridInner>,
    executor: Arc<AsyncExecutor>,
    close_state: AtomicU8,
    close_completion: Arc<HybridCloseCompletion>,
    close_coordinator: SyncSender<Arc<AsyncHybridInner>>,
}

impl AsyncHybridCache {
    pub(crate) fn try_new(cache: Arc<HybridInner>, config: HybridAsyncConfig) -> Result<Self> {
        let executor = AsyncExecutor::try_new(
            config.read_queue_depth,
            config.write_queue_depth,
            config.io_concurrency,
            config.mutation_workers,
        )
        .map_err(CacheError::Io)?;
        let (close_coordinator, close_request) = sync_channel(1);
        let close_completion = Arc::clone(&cache.async_close_completion);
        let inner = Arc::new(AsyncHybridInner {
            cache,
            executor: Arc::new(executor),
            close_state: AtomicU8::new(CLOSE_OPEN),
            close_completion,
            close_coordinator,
        });
        thread::Builder::new()
            .name("cache-rs-hybrid-close".into())
            .spawn(move || {
                if let Ok(inner) = close_request.recv() {
                    inner.drain_and_close();
                }
            })
            .map_err(CacheError::Io)?;
        Ok(Self { inner })
    }

    pub(crate) fn from_inner(inner: Arc<AsyncHybridInner>) -> Self {
        Self { inner }
    }

    pub(crate) fn shared_inner(&self) -> &Arc<AsyncHybridInner> {
        &self.inner
    }

    pub fn queue_stats(&self) -> AsyncQueueStats {
        self.inner.executor.snapshot()
    }

    /// Return bounded close-waiter occupancy and the last completed drain
    /// duration. The snapshot remains retained by the owning Hybrid cache even
    /// after this facade and its coordinator have exited.
    pub fn close_stats(&self) -> AsyncHybridCloseStats {
        self.inner.close_completion.snapshot()
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> CacheFuture<Result<Option<Vec<u8>>>> {
        self.get_in_with_options(0, key, AsyncRequestOptions::default())
    }

    pub fn get_with_options(
        &self,
        key: impl AsRef<[u8]>,
        options: AsyncRequestOptions,
    ) -> CacheFuture<Result<Option<Vec<u8>>>> {
        self.get_in_with_options(0, key, options)
    }

    pub fn get_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
    ) -> CacheFuture<Result<Option<Vec<u8>>>> {
        self.get_in_with_options(namespace, key, AsyncRequestOptions::default())
    }

    pub fn get_in_with_options(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        options: AsyncRequestOptions,
    ) -> CacheFuture<Result<Option<Vec<u8>>>> {
        self.read_in_with_options(namespace, key, options, |outcome| match outcome {
            HybridLookupOutcome::Hit { value, .. } => Some(value),
            HybridLookupOutcome::Miss(_) => None,
        })
    }

    pub fn lookup(&self, key: impl AsRef<[u8]>) -> CacheFuture<Result<HybridLookupOutcome>> {
        self.lookup_in_with_options(0, key, AsyncRequestOptions::default())
    }

    pub fn lookup_in_with_options(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        options: AsyncRequestOptions,
    ) -> CacheFuture<Result<HybridLookupOutcome>> {
        self.read_in_with_options(namespace, key, options, |outcome| outcome)
    }

    fn read_in_with_options<T, F>(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        options: AsyncRequestOptions,
        map: F,
    ) -> CacheFuture<Result<T>>
    where
        T: Send + 'static,
        F: FnOnce(HybridLookupOutcome) -> T + Send + 'static,
    {
        let cache = match self.cache_for_read() {
            Ok(cache) => cache,
            Err(error) => return ready_future(Err(error)),
        };
        let key = key.as_ref();
        let permit = match cache.try_reserve_async_read(key.len()) {
            Ok(permit) => permit,
            Err(error) => return ready_future(Err(error)),
        };
        let mut permit = permit;
        if options.deadline().is_none() {
            match cache.lookup_memory_in_admitted(namespace, key, &mut permit) {
                Ok(Some(outcome)) => return ready_future(Ok(map(outcome))),
                Ok(None) => {}
                Err(error) => return ready_future(Err(error)),
            }
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
            options,
            map_read_failure,
            move |context| {
                cache
                    .lookup_in_admitted_with_task_context(namespace, &key, &mut permit, &context)
                    .map(map)
            },
        )
    }

    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
    ) -> CacheFuture<Result<PutOutcome>> {
        self.put_in_with_options(0, key, value, put_options, AsyncRequestOptions::default())
    }

    pub fn put_with_options(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<Result<PutOutcome>> {
        self.put_in_with_options(0, key, value, put_options, request_options)
    }

    pub fn put_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
    ) -> CacheFuture<Result<PutOutcome>> {
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
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        put_options: PutOptions,
        request_options: AsyncRequestOptions,
    ) -> CacheFuture<Result<PutOutcome>> {
        let cache = match self.cache_for_write() {
            Ok(cache) => cache,
            Err(error) => return ready_future(Err(error)),
        };
        let key = key.as_ref();
        let value = value.as_ref();
        let permit = match cache.try_reserve_async_put(key.len(), value.len()) {
            Ok(permit) => permit,
            Err(CacheError::Overloaded(reason)) => {
                return ready_future(Ok(PutOutcome::Rejected(put_reject_reason(reason))));
            }
            Err(error) => return ready_future(Err(error)),
        };
        let reservation = match self.inner.executor.reserve_mutation() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_put_failure(failure)),
        };
        let key = match copy_input(key, OverloadReason::WriteBufferUnavailable) {
            Ok(key) => key,
            Err(_) => {
                return ready_future(Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)));
            }
        };
        let value = match copy_input(value, OverloadReason::WriteBufferUnavailable) {
            Ok(value) => value,
            Err(_) => {
                return ready_future(Ok(PutOutcome::Rejected(RejectReason::BufferUnavailable)));
            }
        };
        self.inner.executor.submit_mutation_reserved(
            reservation,
            request_options,
            map_put_failure,
            move |_| {
                let _permit = permit;
                cache.put_in_admitted(namespace, &key, &value, put_options)
            },
        )
    }

    pub fn remove(&self, key: impl AsRef<[u8]>) -> CacheFuture<Result<RemoveOutcome>> {
        self.remove_in_with_options(0, key, AsyncRequestOptions::default())
    }

    pub fn remove_in(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
    ) -> CacheFuture<Result<RemoveOutcome>> {
        self.remove_in_with_options(namespace, key, AsyncRequestOptions::default())
    }

    pub fn remove_in_with_options(
        &self,
        namespace: NamespaceId,
        key: impl AsRef<[u8]>,
        options: AsyncRequestOptions,
    ) -> CacheFuture<Result<RemoveOutcome>> {
        let cache = match self.cache_for_write() {
            Ok(cache) => cache,
            Err(error) => return ready_future(Err(error)),
        };
        let key = key.as_ref();
        let permit = match cache.try_reserve_async_remove(key.len()) {
            Ok(permit) => permit,
            Err(error) => return ready_future(Err(error)),
        };
        let reservation = match self.inner.executor.reserve_mutation() {
            Ok(reservation) => reservation,
            Err(failure) => return ready_future(map_write_failure(failure)),
        };
        let key = match copy_input(key, OverloadReason::WriteBufferUnavailable) {
            Ok(key) => key,
            Err(error) => return ready_future(Err(error)),
        };
        self.inner.executor.submit_mutation_reserved(
            reservation,
            options,
            map_write_failure,
            move |_| {
                let _permit = permit;
                cache.remove_in_admitted(namespace, &key)
            },
        )
    }

    pub fn flush(&self) -> CacheFuture<Result<()>> {
        self.flush_with_options(AsyncRequestOptions::default())
    }

    pub fn flush_with_options(&self, options: AsyncRequestOptions) -> CacheFuture<Result<()>> {
        let cache = match self.cache_for_write() {
            Ok(cache) => cache,
            Err(error) => return ready_future(Err(error)),
        };
        self.inner
            .executor
            .submit_control(options, map_write_failure, move |_| cache.flush())
    }

    pub fn clear(&self) -> CacheFuture<Result<()>> {
        self.clear_with_options(AsyncRequestOptions::default())
    }

    pub fn clear_with_options(&self, options: AsyncRequestOptions) -> CacheFuture<Result<()>> {
        let cache = match self.cache_for_write() {
            Ok(cache) => cache,
            Err(error) => return ready_future(Err(error)),
        };
        self.inner
            .executor
            .submit_control(options, map_write_failure, move |_| cache.clear())
    }

    /// Stop admission, drain accepted requests, close all three files, and
    /// release their locks. Every caller observes the same completion.
    pub fn close(&self) -> AsyncHybridCloseFuture {
        let future = AsyncHybridCloseFuture {
            completion: Arc::clone(&self.inner.close_completion),
            registration: None,
        };
        if self
            .inner
            .close_state
            .compare_exchange(
                CLOSE_OPEN,
                CLOSE_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            HybridCache::from_inner(Arc::clone(&self.inner.cache)).stop_admission_for_close();
            self.inner.close_completion.start();
            self.start_close_coordinator();
        }
        future
    }

    fn cache_for_read(&self) -> Result<HybridCache> {
        if self.inner.close_state.load(Ordering::Acquire) != CLOSE_OPEN {
            return Err(CacheError::Closed);
        }
        let cache = HybridCache::from_inner(Arc::clone(&self.inner.cache));
        match cache.status() {
            CacheStatus::Healthy | CacheStatus::MissOnly => Ok(cache),
            CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Err(CacheError::Closed),
        }
    }

    fn cache_for_write(&self) -> Result<HybridCache> {
        if self.inner.close_state.load(Ordering::Acquire) != CLOSE_OPEN {
            return Err(CacheError::Closed);
        }
        let cache = HybridCache::from_inner(Arc::clone(&self.inner.cache));
        match cache.status() {
            CacheStatus::Healthy => Ok(cache),
            CacheStatus::MissOnly | CacheStatus::Poisoned => Err(CacheError::Poisoned),
            CacheStatus::Closed => Err(CacheError::Closed),
        }
    }

    fn start_close_coordinator(&self) {
        // The coordinator thread is reserved when the facade is constructed,
        // before admission is stopped. With one close owner and a capacity-one
        // channel this send cannot wait for drain work or grow memory.
        let _ = self.inner.close_coordinator.send(Arc::clone(&self.inner));
    }
}

impl AsyncHybridInner {
    fn drain_and_close(&self) {
        let started = Instant::now();
        let drain = self.executor.begin_drain();
        drain.wait();
        let worker_panicked = self.executor.join();
        let cache = HybridCache::from_inner(Arc::clone(&self.cache));
        let closed = catch_unwind(AssertUnwindSafe(|| cache.close_after_async_drain()))
            .ok()
            .is_some_and(|result| result.is_ok());
        self.close_state.store(CLOSE_DONE, Ordering::Release);
        self.close_completion
            .complete(closed && !worker_panicked, started.elapsed());
    }
}

/// Shared, uncancellable completion for asynchronous Hybrid shutdown.
#[must_use = "Hybrid close must be awaited or synchronously waited"]
pub struct AsyncHybridCloseFuture {
    completion: Arc<HybridCloseCompletion>,
    registration: Option<u64>,
}

impl AsyncHybridCloseFuture {
    pub fn cancel(&self) -> CancelOutcome {
        if self.completion.is_ready() {
            CancelOutcome::Completed
        } else {
            CancelOutcome::TooLate
        }
    }

    pub fn wait(mut self) -> Result<()> {
        self.unregister();
        self.completion.wait()
    }

    /// Wait for at most `timeout` while the uncancellable shutdown continues
    /// in its reserved coordinator thread. A timeout never releases file locks
    /// or marks the cache closed; a later `close()` waiter can observe the same
    /// eventual result.
    pub fn wait_timeout(mut self, timeout: Duration) -> Result<()> {
        self.unregister();
        self.completion.wait_timeout(timeout)
    }

    fn unregister(&mut self) {
        if let Some(registration) = self.registration.take() {
            self.completion.unregister(registration);
        }
    }
}

impl Future for AsyncHybridCloseFuture {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.completion.poll(context, &mut this.registration)
    }
}

impl Drop for AsyncHybridCloseFuture {
    fn drop(&mut self) {
        self.unregister();
    }
}

pub(crate) struct HybridCloseCompletion {
    phase: AtomicU8,
    outcome: AtomicU8,
    waiters: Mutex<HybridCloseWaiters>,
    ready: Condvar,
    drain_duration_ns: AtomicU64,
}

struct HybridCloseWaiters {
    next_id: u64,
    registered: Vec<HybridCloseWaiter>,
    peak: u64,
    rejections: u64,
    timed_out: u64,
}

struct HybridCloseWaiter {
    id: u64,
    waker: Waker,
}

impl HybridCloseCompletion {
    pub(crate) fn new() -> Self {
        Self {
            phase: AtomicU8::new(CLOSE_OPEN),
            outcome: AtomicU8::new(COMPLETION_PENDING),
            waiters: Mutex::new(HybridCloseWaiters {
                next_id: 1,
                registered: Vec::new(),
                peak: 0,
                rejections: 0,
                timed_out: 0,
            }),
            ready: Condvar::new(),
            drain_duration_ns: AtomicU64::new(0),
        }
    }

    pub(crate) fn start(&self) {
        let _ = self.phase.compare_exchange(
            CLOSE_OPEN,
            CLOSE_RUNNING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn complete(&self, succeeded: bool, elapsed: Duration) {
        let outcome = if succeeded {
            COMPLETION_SUCCEEDED
        } else {
            COMPLETION_FAILED
        };
        let waiters = {
            let mut waiters = lock_mutex(&self.waiters);
            if self
                .outcome
                .compare_exchange(
                    COMPLETION_PENDING,
                    outcome,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return;
            }
            self.drain_duration_ns.store(
                elapsed.as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Release,
            );
            std::mem::take(&mut waiters.registered)
        };
        self.phase.store(CLOSE_DONE, Ordering::Release);
        self.ready.notify_all();
        for waiter in waiters {
            let _ = catch_unwind(AssertUnwindSafe(|| waiter.waker.wake()));
        }
    }

    fn is_ready(&self) -> bool {
        self.outcome.load(Ordering::Acquire) != COMPLETION_PENDING
    }

    fn result(&self) -> Option<Result<()>> {
        match self.outcome.load(Ordering::Acquire) {
            COMPLETION_SUCCEEDED => Some(Ok(())),
            COMPLETION_FAILED => Some(Err(CacheError::Poisoned)),
            _ => None,
        }
    }

    fn poll(&self, context: &mut Context<'_>, registration: &mut Option<u64>) -> Poll<Result<()>> {
        if let Some(result) = self.result() {
            return Poll::Ready(result);
        }
        let mut waiters = lock_mutex(&self.waiters);
        if let Some(result) = self.result() {
            return Poll::Ready(result);
        }

        if let Some(id) = *registration {
            if let Some(waiter) = waiters.registered.iter_mut().find(|waiter| waiter.id == id) {
                let matches =
                    catch_unwind(AssertUnwindSafe(|| waiter.waker.will_wake(context.waker())))
                        .unwrap_or(false);
                if !matches {
                    let Ok(waker) = catch_unwind(AssertUnwindSafe(|| context.waker().clone()))
                    else {
                        waiters.rejections = waiters.rejections.saturating_add(1);
                        waiters.registered.retain(|waiter| waiter.id != id);
                        *registration = None;
                        return Poll::Ready(Err(CacheError::Overloaded(
                            OverloadReason::CloseWaitersFull,
                        )));
                    };
                    waiter.waker = waker;
                }
                return Poll::Pending;
            }
            *registration = None;
        }

        if waiters.registered.len() >= MAX_CLOSE_FUTURE_WAITERS
            || waiters.registered.try_reserve_exact(1).is_err()
        {
            waiters.rejections = waiters.rejections.saturating_add(1);
            return Poll::Ready(Err(CacheError::Overloaded(
                OverloadReason::CloseWaitersFull,
            )));
        }
        let Ok(waker) = catch_unwind(AssertUnwindSafe(|| context.waker().clone())) else {
            waiters.rejections = waiters.rejections.saturating_add(1);
            return Poll::Ready(Err(CacheError::Overloaded(
                OverloadReason::CloseWaitersFull,
            )));
        };
        let id = waiters.next_waiter_id();
        waiters.registered.push(HybridCloseWaiter { id, waker });
        waiters.peak = waiters.peak.max(waiters.registered.len() as u64);
        *registration = Some(id);
        Poll::Pending
    }

    fn wait(&self) -> Result<()> {
        let mut waiters = lock_mutex(&self.waiters);
        loop {
            if let Some(result) = self.result() {
                return result;
            }
            waiters = self
                .ready
                .wait(waiters)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn wait_timeout(&self, timeout: Duration) -> Result<()> {
        let started = Instant::now();
        let deadline = started.checked_add(timeout);
        let mut waiters = lock_mutex(&self.waiters);
        loop {
            if let Some(result) = self.result() {
                return result;
            }
            let Some(remaining) =
                deadline.and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            else {
                waiters.timed_out = waiters.timed_out.saturating_add(1);
                return Err(CacheError::TimedOut);
            };
            let (next, result) = self
                .ready
                .wait_timeout(waiters, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            waiters = next;
            if result.timed_out() && self.result().is_none() {
                waiters.timed_out = waiters.timed_out.saturating_add(1);
                return Err(CacheError::TimedOut);
            }
        }
    }

    fn unregister(&self, id: u64) {
        let mut waiters = lock_mutex(&self.waiters);
        if let Some(index) = waiters.registered.iter().position(|waiter| waiter.id == id) {
            waiters.registered.swap_remove(index);
        }
    }

    pub(crate) fn snapshot(&self) -> AsyncHybridCloseStats {
        let waiters = lock_mutex(&self.waiters);
        let outcome = self.outcome.load(Ordering::Acquire);
        AsyncHybridCloseStats {
            draining: self.phase.load(Ordering::Acquire) == CLOSE_RUNNING,
            completed: outcome != COMPLETION_PENDING,
            succeeded: outcome == COMPLETION_SUCCEEDED,
            registered_waiters: waiters.registered.len() as u64,
            registered_waiters_peak: waiters.peak,
            waiter_rejections: waiters.rejections,
            timed_out_waits: waiters.timed_out,
            drain_duration_ns: self.drain_duration_ns.load(Ordering::Acquire),
        }
    }
}

impl HybridCloseWaiters {
    fn next_waiter_id(&mut self) -> u64 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1).max(1);
            if self.registered.iter().all(|waiter| waiter.id != id) {
                return id;
            }
        }
    }
}

fn put_reject_reason(reason: OverloadReason) -> RejectReason {
    match reason {
        OverloadReason::ReadQueueFull
        | OverloadReason::WriteQueueFull
        | OverloadReason::JournalCapacityFull
        | OverloadReason::CloseWaitersFull => RejectReason::SubmissionFull,
        OverloadReason::ReadBufferUnavailable | OverloadReason::WriteBufferUnavailable => {
            RejectReason::BufferUnavailable
        }
        OverloadReason::ReadTimeout | OverloadReason::WriteTimeout => {
            RejectReason::SubmissionTimeout
        }
    }
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::task::Wake;

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn close_future(completion: &Arc<HybridCloseCompletion>) -> AsyncHybridCloseFuture {
        AsyncHybridCloseFuture {
            completion: Arc::clone(completion),
            registration: None,
        }
    }

    #[test]
    fn close_future_waiters_are_bounded_and_removed_when_dropped() {
        let completion = Arc::new(HybridCloseCompletion::new());
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);

        for _ in 0..MAX_CLOSE_FUTURE_WAITERS * 2 {
            let mut future = close_future(&completion);
            assert!(Pin::new(&mut future).poll(&mut context).is_pending());
            assert!(Pin::new(&mut future).poll(&mut context).is_pending());
            assert_eq!(completion.snapshot().registered_waiters, 1);
            drop(future);
            assert_eq!(completion.snapshot().registered_waiters, 0);
        }

        let mut futures = Vec::new();
        for _ in 0..MAX_CLOSE_FUTURE_WAITERS {
            let mut future = close_future(&completion);
            assert!(Pin::new(&mut future).poll(&mut context).is_pending());
            futures.push(future);
        }
        let mut rejected = close_future(&completion);
        assert!(matches!(
            Pin::new(&mut rejected).poll(&mut context),
            Poll::Ready(Err(CacheError::Overloaded(
                OverloadReason::CloseWaitersFull
            )))
        ));
        let stats = completion.snapshot();
        assert_eq!(stats.registered_waiters, MAX_CLOSE_FUTURE_WAITERS as u64);
        assert_eq!(
            stats.registered_waiters_peak,
            MAX_CLOSE_FUTURE_WAITERS as u64
        );
        assert_eq!(stats.waiter_rejections, 1);

        drop(futures);
        assert_eq!(completion.snapshot().registered_waiters, 0);
        completion.complete(true, Duration::from_millis(1));
        assert!(close_future(&completion).wait().is_ok());
    }
}
