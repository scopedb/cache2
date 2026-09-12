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

use std::cell::Cell;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;
use std::time::Instant;

use super::*;
use crate::Error;
use crate::ErrorKind;

const BUCKETS: usize = LATENCY_BUCKET_UPPER_BOUNDS_NS.len() + 1;
const REQUEST_SERIES: &[(RequestOperation, RequestOutcome)] = &[
    (RequestOperation::Get, RequestOutcome::L1Hit),
    (RequestOperation::Get, RequestOutcome::L2Hit),
    (RequestOperation::Get, RequestOutcome::Miss),
    (RequestOperation::Get, RequestOutcome::Overloaded),
    (RequestOperation::Get, RequestOutcome::Error),
    (RequestOperation::Get, RequestOutcome::Cancelled),
    (RequestOperation::Put, RequestOutcome::Accepted),
    (RequestOperation::Put, RequestOutcome::Overloaded),
    (RequestOperation::Put, RequestOutcome::InvalidInput),
    (RequestOperation::Put, RequestOutcome::Unavailable),
    (RequestOperation::Put, RequestOutcome::Error),
    (RequestOperation::PutL2, RequestOutcome::Accepted),
    (RequestOperation::PutL2, RequestOutcome::Overloaded),
    (RequestOperation::PutL2, RequestOutcome::InvalidInput),
    (RequestOperation::PutL2, RequestOutcome::Unavailable),
    (RequestOperation::PutL2, RequestOutcome::Error),
    (RequestOperation::Delete, RequestOutcome::Accepted),
    (RequestOperation::Delete, RequestOutcome::Overloaded),
    (RequestOperation::Delete, RequestOutcome::InvalidInput),
    (RequestOperation::Delete, RequestOutcome::Unavailable),
    (RequestOperation::Delete, RequestOutcome::Error),
];
const REQUEST_INDEX: [[usize; 9]; 4] = {
    let mut indices = [[usize::MAX; 9]; 4];
    let mut index = 0;
    while index < REQUEST_SERIES.len() {
        let (operation, outcome) = REQUEST_SERIES[index];
        indices[operation as usize][outcome as usize] = index;
        index += 1;
    }
    indices
};
const IO_ROLES: [IoRole; 3] = [IoRole::Read, IoRole::Write, IoRole::Reclaim];
const IO_OUTCOMES: [IoOutcome; 3] = [
    IoOutcome::Completed,
    IoOutcome::Cancelled,
    IoOutcome::Failed,
];
const IO_SERIES: usize = IO_ROLES.len() * IO_OUTCOMES.len();

// Keep independent writers' counters off the same cache line. Histograms group
// all outcomes of a stripe, rather than routing hot keys to the same recorder.
#[repr(align(128))]
struct Counters([AtomicU64; REQUEST_SERIES.len()]);

#[repr(align(128))]
struct Histogram {
    buckets: [AtomicU64; BUCKETS],
    sum_ns: AtomicU64,
    invalid: AtomicBool,
}

impl Histogram {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            sum_ns: AtomicU64::new(0),
            invalid: AtomicBool::new(false),
        }
    }

    fn record(&self, duration: Duration) {
        let Ok(ns) = u64::try_from(duration.as_nanos()) else {
            self.invalid.store(true, Relaxed);
            return;
        };
        let bucket = LATENCY_BUCKET_UPPER_BOUNDS_NS.partition_point(|bound| *bound < ns);
        let old_count = self.buckets[bucket].fetch_add(1, Relaxed);
        let old_sum = self.sum_ns.fetch_add(ns, Relaxed);
        if old_count == u64::MAX || old_sum.checked_add(ns).is_none() {
            self.invalid.store(true, Relaxed);
        }
    }
}

static NEXT_THREAD: AtomicU64 = AtomicU64::new(1);
thread_local! {
    // A bounded scalar per thread, not a map of cache instances. A different
    // sampling rate uses the same uniform random stream without reinitializing.
    static THREAD: Cell<(u64, u64)> = {
        let id = NEXT_THREAD.fetch_add(1, Relaxed);
        Cell::new((id, id.wrapping_mul(0x9e3779b97f4a7c15).max(1)))
    };
}

fn thread_sample(mode: LatencyMode) -> (usize, bool) {
    THREAD.with(|cell| {
        let (id, mut state) = cell.get();
        let sampled = match mode {
            LatencyMode::Off => false,
            LatencyMode::Full => true,
            LatencyMode::Sampled { interval } if interval.get() == 1 => true,
            LatencyMode::Sampled { interval } => {
                state ^= state >> 12;
                state ^= state << 25;
                state ^= state >> 27;
                cell.set((id, state));
                state.wrapping_mul(0x2545f4914f6cdd1d) <= u64::MAX / u64::from(interval.get())
            }
        };
        (id as usize, sampled)
    })
}

pub struct Recorder {
    options: StatsOptions,
    counters: Box<[Counters]>,
    request_histograms: Box<[Histogram]>,
    histogram_indices: [usize; REQUEST_SERIES.len()],
    histogram_series: usize,
    io_histograms: Box<[Histogram]>,
}

impl Recorder {
    pub fn allocation_bytes(options: StatsOptions) -> io::Result<usize> {
        if !options.shards.is_power_of_two() || options.shards > 64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stats shards must be a power of two in 1..=64",
            ));
        }
        let counters = usize::from(options.request_counters) * size_of::<Counters>();
        let request = REQUEST_SERIES
            .iter()
            .filter(|&&(operation, outcome)| {
                options.latency_mode(RequestLatencyScope::for_request(operation, outcome))
                    != LatencyMode::Off
            })
            .count();
        let io = usize::from(options.io_latency) * IO_SERIES;
        Ok(options.shards * (counters + (request + io) * size_of::<Histogram>()))
    }

    pub fn new(options: StatsOptions) -> io::Result<Self> {
        Self::allocation_bytes(options)?;
        let counters = allocate(
            usize::from(options.request_counters) * options.shards,
            || Counters(std::array::from_fn(|_| AtomicU64::new(0))),
        )?;
        let mut histogram_indices = [usize::MAX; REQUEST_SERIES.len()];
        let mut histogram_series = 0;
        for (index, &(operation, outcome)) in REQUEST_SERIES.iter().enumerate() {
            if options.latency_mode(RequestLatencyScope::for_request(operation, outcome))
                != LatencyMode::Off
            {
                histogram_indices[index] = histogram_series;
                histogram_series += 1;
            }
        }
        Ok(Self {
            options,
            counters,
            request_histograms: allocate(options.shards * histogram_series, Histogram::new)?,
            histogram_indices,
            histogram_series,
            io_histograms: allocate(
                usize::from(options.io_latency) * options.shards * IO_SERIES,
                Histogram::new,
            )?,
        })
    }

    #[inline]
    pub fn begin(&self, operation: RequestOperation) -> RequestGuard<'_> {
        if !self.options.requests_enabled() {
            return RequestGuard {
                recorder: None,
                operation,
                stripe: 0,
                timer: None,
            };
        }
        let scope = if operation == RequestOperation::Get {
            RequestLatencyScope::L1Hit
        } else {
            RequestLatencyScope::Mutation
        };
        let mode = self.options.latency_mode(scope);
        let (thread, sampled) = if mode == LatencyMode::Off && !self.options.request_counters {
            (0, false)
        } else {
            thread_sample(mode)
        };
        RequestGuard {
            recorder: Some(self),
            operation,
            stripe: thread & (self.options.shards - 1),
            timer: sampled.then(|| (scope, Instant::now())),
        }
    }

    pub fn io_timing(self: &Arc<Self>, role: IoRole) -> Option<IoTiming> {
        self.options.io_latency.then(|| IoTiming {
            recorder: Arc::clone(self),
            role,
        })
    }

    pub fn snapshot(&self, summary: CacheSnapshot) -> CacheStatsSnapshot {
        let mut requests = Vec::new();
        if self.options.requests_enabled() {
            for (index, &(operation, outcome)) in REQUEST_SERIES.iter().enumerate() {
                let scope = RequestLatencyScope::for_request(operation, outcome);
                requests.push(RequestStatsSnapshot {
                    operation,
                    outcome,
                    count: self.options.request_counters.then(|| {
                        self.counters.iter().fold(0u64, |sum, stripe| {
                            sum.saturating_add(stripe.0[index].load(Relaxed))
                        })
                    }),
                    latency_scope: scope,
                    latency_mode: self.options.latency_mode(scope),
                    latency: (self.histogram_indices[index] != usize::MAX).then(|| {
                        histogram_snapshot(
                            &self.request_histograms,
                            self.histogram_series,
                            self.histogram_indices[index],
                        )
                    }),
                });
            }
        }
        CacheStatsSnapshot {
            summary,
            options: self.options,
            recorder_bytes: self.counters.len() * size_of::<Counters>()
                + (self.request_histograms.len() + self.io_histograms.len())
                    * size_of::<Histogram>(),
            requests,
            io_latency: self.io_snapshot(),
        }
    }
    pub fn io_snapshot(&self) -> Vec<IoLatencySnapshot> {
        let mut io_latency = Vec::new();
        if self.options.io_latency {
            for role in IO_ROLES {
                for outcome in IO_OUTCOMES {
                    io_latency.push(IoLatencySnapshot {
                        role,
                        outcome,
                        latency: histogram_snapshot(
                            &self.io_histograms,
                            IO_SERIES,
                            io_index(role, outcome),
                        ),
                    });
                }
            }
        }
        io_latency
    }
}

fn allocate<T>(count: usize, init: impl FnMut() -> T) -> io::Result<Box<[T]>> {
    let mut items = Vec::new();
    items.try_reserve_exact(count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot allocate statistics recorders",
        )
    })?;
    items.resize_with(count, init);
    Ok(items.into_boxed_slice())
}

fn histogram_snapshot(histograms: &[Histogram], series: usize, index: usize) -> LatencySnapshot {
    let mut snapshot = LatencySnapshot {
        bucket_counts: vec![0u64; BUCKETS].into_boxed_slice(),
        count: 0,
        sum_ns: 0,
        valid: true,
    };
    for histogram in histograms.iter().skip(index).step_by(series) {
        for (sum, bucket) in snapshot.bucket_counts.iter_mut().zip(&histogram.buckets) {
            match sum.checked_add(bucket.load(Relaxed)) {
                Some(value) => *sum = value,
                None => {
                    snapshot.valid = false;
                    *sum = u64::MAX;
                }
            }
        }
        snapshot.sum_ns += u128::from(histogram.sum_ns.load(Relaxed));
        snapshot.valid &= !histogram.invalid.load(Relaxed);
    }
    for count in &snapshot.bucket_counts {
        match snapshot.count.checked_add(*count) {
            Some(value) => snapshot.count = value,
            None => {
                snapshot.valid = false;
                snapshot.count = u64::MAX;
            }
        }
    }
    snapshot
}

pub struct RequestGuard<'a> {
    recorder: Option<&'a Recorder>,
    operation: RequestOperation,
    stripe: usize,
    timer: Option<(RequestLatencyScope, Instant)>,
}

impl RequestGuard<'_> {
    pub fn enter_l2(&mut self) {
        let Some(recorder) = self.recorder else {
            return;
        };
        let (thread, sampled) = thread_sample(recorder.options.l2_latency);
        self.stripe = thread & (recorder.options.shards - 1);
        self.timer = sampled.then(|| (RequestLatencyScope::L2Lookup, Instant::now()));
    }

    #[inline]
    pub fn finish<T>(mut self, result: &Result<T, Error>, success: RequestOutcome) {
        if self.recorder.is_none() {
            return;
        }
        let outcome = match result {
            Ok(_) => success,
            Err(error) => match error.kind() {
                ErrorKind::Overloaded => RequestOutcome::Overloaded,
                ErrorKind::InvalidInput if self.operation != RequestOperation::Get => {
                    RequestOutcome::InvalidInput
                }
                ErrorKind::Unavailable if self.operation != RequestOperation::Get => {
                    RequestOutcome::Unavailable
                }
                _ => RequestOutcome::Error,
            },
        };
        self.record(outcome);
    }

    fn record(&mut self, outcome: RequestOutcome) {
        let Some(recorder) = self.recorder.take() else {
            return;
        };
        let index = REQUEST_INDEX[self.operation as usize][outcome as usize];
        // Stop the clock before recorder work; caller-side observers measure its
        // overhead separately. Count once at the same terminal boundary.
        let scope = RequestLatencyScope::for_request(self.operation, outcome);
        let elapsed = self
            .timer
            .filter(|(timed_scope, _)| *timed_scope == scope)
            .map(|(_, start)| start.elapsed());
        if recorder.options.request_counters {
            recorder.counters[self.stripe].0[index].fetch_add(1, Relaxed);
        }
        if let Some(elapsed) = elapsed {
            recorder.request_histograms
                [self.stripe * recorder.histogram_series + recorder.histogram_indices[index]]
                .record(elapsed);
        }
    }
}

impl Drop for RequestGuard<'_> {
    fn drop(&mut self) {
        if self.operation == RequestOperation::Get && !std::thread::panicking() {
            self.record(RequestOutcome::Cancelled);
        }
    }
}

#[derive(Clone)]
pub struct IoTiming {
    recorder: Arc<Recorder>,
    role: IoRole,
}

impl IoTiming {
    pub fn record(&self, duration: Duration, outcome: IoOutcome) {
        let index = io_index(self.role, outcome);
        let stripe = thread_sample(LatencyMode::Off).0 & (self.recorder.options.shards - 1);
        self.recorder.io_histograms[stripe * IO_SERIES + index].record(duration);
    }
}

fn io_index(role: IoRole, outcome: IoOutcome) -> usize {
    role as usize * IO_OUTCOMES.len() + outcome as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_boundaries_sum_overflow_and_invalidity() {
        let histogram = Histogram::new();
        let mut expected_sum = 0u128;
        for &bound in LATENCY_BUCKET_UPPER_BOUNDS_NS {
            histogram.record(Duration::from_nanos(bound));
            expected_sum += u128::from(bound);
        }
        histogram.record(Duration::from_nanos(60_000_000_001));
        expected_sum += 60_000_000_001;
        let snapshot = histogram_snapshot(std::slice::from_ref(&histogram), 1, 0);
        assert!(snapshot.valid);
        assert_eq!(snapshot.bucket_counts.as_ref(), &[1; BUCKETS]);
        assert_eq!(snapshot.count, BUCKETS as u64);
        assert_eq!(snapshot.sum_ns, expected_sum);
        histogram.sum_ns.store(u64::MAX, Relaxed);
        histogram.record(Duration::from_nanos(1));
        assert!(!histogram_snapshot(&[histogram], 1, 0).valid);
    }

    #[test]
    fn guards_count_once_and_cancellation_is_a_separate_population() {
        let recorder = Recorder::new(StatsOptions {
            request_counters: true,
            l1_latency: LatencyMode::Full,
            l2_latency: LatencyMode::Full,
            mutation_latency: LatencyMode::Full,
            shards: 1,
            ..StatsOptions::default()
        })
        .unwrap();
        let mut hit = recorder.begin(RequestOperation::Get);
        hit.enter_l2();
        hit.finish(&Ok::<_, Error>(()), RequestOutcome::L2Hit);
        let mut cancelled = recorder.begin(RequestOperation::Get);
        cancelled.enter_l2();
        drop(cancelled);
        assert_eq!(recorder.counters[0].0[1].load(Relaxed), 1);
        assert_eq!(recorder.counters[0].0[5].load(Relaxed), 1);
        assert_eq!(
            histogram_snapshot(&recorder.request_histograms, REQUEST_SERIES.len(), 1).count,
            1
        );
        assert_eq!(
            histogram_snapshot(&recorder.request_histograms, REQUEST_SERIES.len(), 5).count,
            1
        );
        assert_eq!(
            recorder.counters[0]
                .0
                .iter()
                .map(|count| count.load(Relaxed))
                .sum::<u64>(),
            2
        );
    }

    #[test]
    fn l2_timing_starts_at_the_l1_miss() {
        let recorder = Recorder::new(StatsOptions {
            l2_latency: LatencyMode::Full,
            shards: 1,
            ..StatsOptions::default()
        })
        .unwrap();
        let mut guard = recorder.begin(RequestOperation::Get);
        assert!(
            guard.timer.is_none(),
            "L2-only timing must not start an L1 clock"
        );
        let transition = Instant::now();
        guard.enter_l2();
        assert!(guard.timer.unwrap().1 >= transition);
        drop(guard);
        let recorder = Recorder::new(StatsOptions {
            l1_latency: LatencyMode::Full,
            l2_latency: LatencyMode::Full,
            ..StatsOptions::default()
        })
        .unwrap();
        let mut guard = recorder.begin(RequestOperation::Get);
        guard.timer = Some((
            RequestLatencyScope::L1Hit,
            Instant::now() - Duration::from_secs(3600),
        ));
        let transition = Instant::now();
        guard.enter_l2();
        assert!(
            guard.timer.unwrap().1 >= transition,
            "L2 must discard the L1 timer"
        );
        guard.finish(&Ok::<_, Error>(()), RequestOutcome::Miss);
    }

    #[test]
    fn sampling_is_independent_of_callers_switching_rates() {
        THREAD.with(|thread| thread.set((1, 12345)));
        let mut sampled = [0usize; 2];
        for _ in 0..100_000 {
            for (index, interval) in [16, 64].into_iter().enumerate() {
                sampled[index] += usize::from(
                    thread_sample(LatencyMode::Sampled {
                        interval: NonZeroU32::new(interval).unwrap(),
                    })
                    .1,
                );
            }
        }
        assert!((5900..6600).contains(&sampled[0]), "{sampled:?}");
        assert!((1400..1750).contains(&sampled[1]), "{sampled:?}");
    }

    #[test]
    fn concurrent_readers_do_not_consume_or_lose_observations() {
        let recorder = Recorder::new(StatsOptions {
            request_counters: true,
            l1_latency: LatencyMode::Full,
            l2_latency: LatencyMode::Full,
            mutation_latency: LatencyMode::Full,
            shards: 1,
            ..StatsOptions::default()
        })
        .unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let recorder = &recorder;
                scope.spawn(move || {
                    for _ in 0..10_000 {
                        recorder
                            .begin(RequestOperation::Get)
                            .finish(&Ok::<_, Error>(()), RequestOutcome::L1Hit);
                    }
                });
            }
            for _ in 0..2 {
                let recorder = &recorder;
                scope.spawn(move || {
                    let mut previous = 0;
                    for _ in 0..1000 {
                        let snapshot = histogram_snapshot(
                            &recorder.request_histograms,
                            REQUEST_SERIES.len(),
                            0,
                        );
                        assert!(snapshot.valid);
                        assert!(snapshot.count >= previous);
                        previous = snapshot.count;
                    }
                });
            }
        });
        assert_eq!(recorder.counters[0].0[0].load(Relaxed), 80_000);
        let first = histogram_snapshot(&recorder.request_histograms, REQUEST_SERIES.len(), 0);
        assert_eq!(first.count, 80_000);
        assert_eq!(
            first,
            histogram_snapshot(&recorder.request_histograms, REQUEST_SERIES.len(), 0)
        );
    }
}
