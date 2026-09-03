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

//! Bounded benchmark measurements and fio-style reporting.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use cache2::{CacheIoDirectionSnapshot, DetailedCacheSnapshot};

const LATENCY_BUCKETS: usize = 65;

/// A fixed-size log2 latency histogram.
///
/// Percentiles are reported as bucket upper bounds. Mean and standard
/// deviation are estimates derived from bucket midpoints.
#[derive(Clone)]
pub struct LatencyHistogram {
    buckets: [u64; LATENCY_BUCKETS],
    samples: u64,
    minimum_ns: u64,
    maximum_ns: u64,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: [0; LATENCY_BUCKETS],
            samples: 0,
            minimum_ns: u64::MAX,
            maximum_ns: 0,
        }
    }
}

impl LatencyHistogram {
    /// Records one latency sample.
    pub fn record(&mut self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        let bucket = latency_bucket(nanos);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
        self.minimum_ns = self.minimum_ns.min(nanos);
        self.maximum_ns = self.maximum_ns.max(nanos);
    }

    /// Merges another bounded histogram.
    pub fn merge(&mut self, other: Self) {
        for (target, value) in self.buckets.iter_mut().zip(other.buckets) {
            *target = target.saturating_add(value);
        }
        self.samples = self.samples.saturating_add(other.samples);
        if other.samples != 0 {
            self.minimum_ns = self.minimum_ns.min(other.minimum_ns);
            self.maximum_ns = self.maximum_ns.max(other.maximum_ns);
        }
    }

    /// Returns the number of recorded samples.
    pub const fn samples(&self) -> u64 {
        self.samples
    }

    /// Returns a complete summary of the bounded histogram.
    pub fn summary(&self) -> LatencySummary {
        if self.samples == 0 {
            return LatencySummary::default();
        }
        let mean_ns = self.estimated_mean_ns();
        let variance = self
            .buckets
            .iter()
            .enumerate()
            .map(|(bucket, count)| {
                let difference = bucket_midpoint_ns(bucket) - mean_ns;
                difference * difference * *count as f64
            })
            .sum::<f64>()
            / self.samples as f64;
        LatencySummary {
            samples: self.samples,
            minimum_ns: self.minimum_ns,
            mean_ns,
            standard_deviation_ns: variance.sqrt(),
            p50_upper_ns: self.percentile_upper_bound_ns(50, 100),
            p90_upper_ns: self.percentile_upper_bound_ns(90, 100),
            p95_upper_ns: self.percentile_upper_bound_ns(95, 100),
            p99_upper_ns: self.percentile_upper_bound_ns(99, 100),
            p999_upper_ns: self.percentile_upper_bound_ns(999, 1_000),
            maximum_ns: self.maximum_ns,
        }
    }

    fn estimated_mean_ns(&self) -> f64 {
        self.buckets
            .iter()
            .enumerate()
            .map(|(bucket, count)| bucket_midpoint_ns(bucket) * *count as f64)
            .sum::<f64>()
            / self.samples as f64
    }

    fn percentile_upper_bound_ns(&self, numerator: u64, denominator: u64) -> u64 {
        let target = self
            .samples
            .saturating_mul(numerator)
            .saturating_add(denominator - 1)
            / denominator;
        let mut seen = 0_u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target {
                return bucket_upper_bound_ns(index);
            }
        }
        self.maximum_ns
    }
}

/// A lock-free latency histogram for multi-threaded soak measurements.
pub struct AtomicLatencyHistogram {
    buckets: [AtomicU64; LATENCY_BUCKETS],
    minimum_ns: AtomicU64,
    maximum_ns: AtomicU64,
}

impl Default for AtomicLatencyHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            minimum_ns: AtomicU64::new(u64::MAX),
            maximum_ns: AtomicU64::new(0),
        }
    }
}

impl AtomicLatencyHistogram {
    /// Records one latency sample with relaxed atomics.
    pub fn record(&self, elapsed: Duration) {
        let nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.buckets[latency_bucket(nanos)].fetch_add(1, Ordering::Relaxed);
        self.minimum_ns.fetch_min(nanos, Ordering::Relaxed);
        self.maximum_ns.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Takes a non-transactional snapshot suitable for periodic reporting.
    pub fn snapshot(&self) -> LatencyHistogram {
        let buckets = std::array::from_fn(|index| self.buckets[index].load(Ordering::Relaxed));
        let samples = buckets.iter().copied().sum();
        LatencyHistogram {
            buckets,
            samples,
            minimum_ns: if samples == 0 {
                u64::MAX
            } else {
                self.minimum_ns.load(Ordering::Relaxed)
            },
            maximum_ns: self.maximum_ns.load(Ordering::Relaxed),
        }
    }
}

/// Aggregated latency statistics.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct LatencySummary {
    /// Number of sampled operations.
    pub samples: u64,
    /// Smallest observed latency.
    pub minimum_ns: u64,
    /// Mean estimated from log2 bucket midpoints.
    pub mean_ns: f64,
    /// Standard deviation estimated from log2 bucket midpoints.
    pub standard_deviation_ns: f64,
    /// 50th-percentile bucket upper bound.
    pub p50_upper_ns: u64,
    /// 90th-percentile bucket upper bound.
    pub p90_upper_ns: u64,
    /// 95th-percentile bucket upper bound.
    pub p95_upper_ns: u64,
    /// 99th-percentile bucket upper bound.
    pub p99_upper_ns: u64,
    /// 99.9th-percentile bucket upper bound.
    pub p999_upper_ns: u64,
    /// Largest observed latency.
    pub maximum_ns: u64,
}

/// One fio-style benchmark job.
pub struct JobReport<'a> {
    benchmark: &'a str,
    scenario: &'a str,
    name: &'a str,
    operation: &'a str,
    workers: usize,
    elapsed: Duration,
    operations: u64,
    bytes: u128,
    errors: u64,
    latency_sample_interval: usize,
    latency: Option<&'a LatencyHistogram>,
}

impl<'a> JobReport<'a> {
    /// Creates one report job from its successfully completed operations.
    pub fn new(
        benchmark: &'a str,
        scenario: Option<&'a str>,
        name: &'a str,
        operation: &'a str,
        elapsed: Duration,
        operations: u64,
    ) -> Self {
        Self {
            benchmark,
            scenario: scenario.unwrap_or("none"),
            name,
            operation,
            workers: 1,
            elapsed,
            operations,
            bytes: 0,
            errors: 0,
            latency_sample_interval: 0,
            latency: None,
        }
    }

    /// Sets the number of contributing workers or clients.
    pub const fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    /// Sets the successfully processed payload bytes.
    pub const fn bytes(mut self, bytes: u128) -> Self {
        self.bytes = bytes;
        self
    }

    /// Sets the failed or rejected operation count.
    pub const fn errors(mut self, errors: u64) -> Self {
        self.errors = errors;
        self
    }

    /// Attaches a sampled latency distribution and its sampling interval.
    pub const fn latency(
        mut self,
        histogram: &'a LatencyHistogram,
        sample_interval: usize,
    ) -> Self {
        self.latency = Some(histogram);
        self.latency_sample_interval = sample_interval;
        self
    }

    /// Emits human-readable fio-style lines and one stable machine record.
    pub fn emit(&self) {
        let seconds = self.elapsed.as_secs_f64();
        let attempts = self.operations.saturating_add(self.errors);
        let iops = rate(self.operations as f64, seconds);
        let attempt_iops = rate(attempts as f64, seconds);
        let bandwidth = rate(self.bytes as f64, seconds);
        let latency = self
            .latency
            .map_or_else(LatencySummary::default, |value| value.summary());
        println!(
            "{}/{}: (groupid=0, jobs={})",
            self.benchmark, self.name, self.workers
        );
        println!(
            "  {}: IOPS={}, BW={} ({:.0} B/s) ({}/{}), attempts={}, errors={}",
            self.operation,
            format_count(iops),
            format_rate_bytes(bandwidth),
            bandwidth,
            format_bytes(self.bytes as f64),
            format_duration(self.elapsed),
            attempts,
            self.errors,
        );
        if latency.samples == 0 {
            println!("    lat (nsec): samples=0");
        } else {
            println!(
                "    lat (nsec): samples={}, min={}, avg~={:.1}, stdev~={:.1}, max={}",
                latency.samples,
                latency.minimum_ns,
                latency.mean_ns,
                latency.standard_deviation_ns,
                latency.maximum_ns,
            );
            println!(
                "    lat percentiles (nsec): 50.00th=[{}], 90.00th=[{}], 95.00th=[{}], 99.00th=[{}], 99.90th=[{}]",
                latency.p50_upper_ns,
                latency.p90_upper_ns,
                latency.p95_upper_ns,
                latency.p99_upper_ns,
                latency.p999_upper_ns,
            );
        }
        println!(
            "report version=1 type=job benchmark={} scenario={} job={} operation={} jobs={} runtime_ns={} attempts={} operations={} errors={} bytes={} attempt_iops={:.3} iops={:.3} bw_bytes_per_sec={:.3} latency_sample_interval={} latency_samples={} latency_min_ns={} latency_mean_estimate_ns={:.3} latency_stddev_estimate_ns={:.3} latency_p50_upper_ns={} latency_p90_upper_ns={} latency_p95_upper_ns={} latency_p99_upper_ns={} latency_p999_upper_ns={} latency_max_ns={}",
            self.benchmark,
            self.scenario,
            self.name,
            self.operation,
            self.workers,
            self.elapsed.as_nanos(),
            attempts,
            self.operations,
            self.errors,
            self.bytes,
            attempt_iops,
            iops,
            bandwidth,
            self.latency_sample_interval,
            latency.samples,
            latency.minimum_ns,
            latency.mean_ns,
            latency.standard_deviation_ns,
            latency.p50_upper_ns,
            latency.p90_upper_ns,
            latency.p95_upper_ns,
            latency.p99_upper_ns,
            latency.p999_upper_ns,
            latency.maximum_ns,
        );
    }
}

/// Tracks wall-clock and process-resource usage for one benchmark run.
pub struct RunReporter {
    benchmark: &'static str,
    scenario: &'static str,
    started: Instant,
    usage: ProcessUsage,
}

impl RunReporter {
    /// Starts one report and emits its common header.
    pub fn start(benchmark: &'static str, scenario: Option<&'static str>) -> Self {
        let scenario = scenario.unwrap_or("none");
        println!("C² benchmark report");
        println!(
            "  benchmark={}, scenario={}, os={}, arch={}",
            benchmark,
            scenario,
            std::env::consts::OS,
            std::env::consts::ARCH,
        );
        println!(
            "report version=1 type=header benchmark={benchmark} scenario={scenario} os={} arch={}",
            std::env::consts::OS,
            std::env::consts::ARCH,
        );
        Self {
            benchmark,
            scenario,
            started: Instant::now(),
            usage: ProcessUsage::capture(),
        }
    }

    /// Finishes the report with CPU, scheduler, memory, and status metrics.
    pub fn finish(self, error: Option<&dyn fmt::Display>) {
        let elapsed = self.started.elapsed();
        let usage = ProcessUsage::capture().subtract(self.usage);
        let seconds = elapsed.as_secs_f64();
        let user_percent = rate(usage.user_cpu.as_secs_f64() * 100.0, seconds);
        let system_percent = rate(usage.system_cpu.as_secs_f64() * 100.0, seconds);
        let status = if error.is_some() { "error" } else { "pass" };
        println!(
            "Run status group 0: ({status}), runtime={}, errors={}",
            format_duration(elapsed),
            u8::from(error.is_some()),
        );
        println!(
            "  cpu: usr={user_percent:.2}%, sys={system_percent:.2}%, ctx={}, majf={}, minf={}",
            usage
                .voluntary_context_switches
                .saturating_add(usage.involuntary_context_switches),
            usage.major_faults,
            usage.minor_faults,
        );
        println!(
            "  memory: maxrss={}",
            format_bytes(usage.maximum_rss_bytes as f64)
        );
        if let Some(error) = error {
            println!("  error: {error}");
        }
        println!(
            "report version=1 type=run benchmark={} scenario={} status={} runtime_ns={} errors={} cpu_user_ns={} cpu_system_ns={} voluntary_context_switches={} involuntary_context_switches={} major_faults={} minor_faults={} max_rss_bytes={}",
            self.benchmark,
            self.scenario,
            status,
            elapsed.as_nanos(),
            u8::from(error.is_some()),
            usage.user_cpu.as_nanos(),
            usage.system_cpu.as_nanos(),
            usage.voluntary_context_switches,
            usage.involuntary_context_switches,
            usage.major_faults,
            usage.minor_faults,
            usage.maximum_rss_bytes,
        );
    }
}

/// Emits cache, I/O-path, index, Region, and managed-resource counters.
pub fn emit_cache_report(
    benchmark: &str,
    scenario: Option<&str>,
    phase: &str,
    elapsed: Duration,
    detailed: &DetailedCacheSnapshot,
) {
    let scenario = scenario.unwrap_or("none");
    let cache = detailed.summary;
    let process = ProcessUsage::capture();
    println!("Cache statistics ({phase}):");
    println!(
        "  requests: puts={}, deletes={}, l1_hits={}, l2_hits={}, l2_misses={}, overloads={}, io_failures={}",
        cache.puts,
        cache.deletes,
        cache.l1_hits,
        cache.l2_hits,
        cache.l2_misses,
        cache.l2_read_overloads,
        cache.io_failures,
    );
    emit_io_direction(benchmark, scenario, phase, "read", elapsed, cache.io.read);
    emit_io_direction(benchmark, scenario, phase, "write", elapsed, cache.io.write);
    println!(
        "  resources: managed={}/{}, peak={}, maxrss={}, index={}/{}, regions={}, logical_disk_peak={}",
        format_bytes(cache.managed_memory_bytes as f64),
        format_bytes(cache.managed_memory_limit_bytes as f64),
        format_bytes(cache.managed_memory_peak_bytes as f64),
        format_bytes(process.maximum_rss_bytes as f64),
        detailed.index.physical_value_slots,
        detailed.index.slot_capacity,
        detailed.region.physical_record_count,
        format_bytes(cache.logical_disk_peak_bytes as f64),
    );
    println!(
        "report version=1 type=cache benchmark={} scenario={} phase={} health={:?} statistics_enabled={} puts={} deletes={} written_bytes={} served_bytes={} l1_hits={} l1_misses={} l2_hits={} l2_misses={} l2_read_memory_misses={} l2_read_busy_misses={} l2_read_overloads={} l2_read_wait_ns={} promotions={} l1_evictions={} l1_bypasses={} write_rejections={} io_failures={} rotations={} reclaimed_regions={} reclaim_bytes={} reclaim_records={} reinsert_records={} reinsert_bytes={} reinsert_skipped={} reinsert_budget_skipped={}",
        benchmark,
        scenario,
        phase,
        cache.health,
        cache.statistics_enabled,
        cache.puts,
        cache.deletes,
        cache.written_bytes,
        cache.served_bytes,
        cache.l1_hits,
        cache.l1_misses,
        cache.l2_hits,
        cache.l2_misses,
        cache.l2_read_memory_misses,
        cache.l2_read_busy_misses,
        cache.l2_read_overloads,
        cache.l2_read_wait_ns,
        cache.l1_promotions,
        cache.l1_evictions,
        cache.l1_bypasses,
        cache.write_rejections,
        cache.io_failures,
        cache.region_rotations,
        cache.reclaim.regions,
        cache.reclaim.bytes_read,
        cache.reclaim.records_scanned,
        cache.reclaim.reinsert_records,
        cache.reclaim.reinsert_bytes,
        cache.reclaim.reinsert_skipped,
        cache.reclaim.reinsert_budget_skipped,
    );
    println!(
        "report version=1 type=resources benchmark={} scenario={} phase={} managed_bytes={} managed_peak_bytes={} managed_limit_bytes={} max_rss_bytes={} logical_disk_peak_bytes={} l1_entries={} l1_entry_capacity={} l1_resident_bytes={} l1_retained_bytes={} l1_metadata_bytes={} index_values={} index_slots={} index_relocations={} index_overflow_evictions={} index_conditional_remove_misses={} index_conditional_replace_misses={} region_records={} region_bytes={}",
        benchmark,
        scenario,
        phase,
        cache.managed_memory_bytes,
        cache.managed_memory_peak_bytes,
        cache.managed_memory_limit_bytes,
        process.maximum_rss_bytes,
        cache.logical_disk_peak_bytes,
        detailed.l1.resident_entries,
        detailed.l1.entry_capacity,
        detailed.l1.resident_bytes,
        detailed.l1.retained_bytes,
        detailed.l1.metadata_bytes,
        detailed.index.physical_value_slots,
        detailed.index.slot_capacity,
        detailed.index.relocations,
        detailed.index.overflow_evictions,
        detailed.index.conditional_remove_misses,
        detailed.index.conditional_replace_misses,
        detailed.region.physical_record_count,
        detailed.region.physical_bytes,
    );
}

fn emit_io_direction(
    benchmark: &str,
    scenario: &str,
    phase: &str,
    direction: &str,
    elapsed: Duration,
    io: CacheIoDirectionSnapshot,
) {
    let completed = io
        .requests_succeeded
        .saturating_add(io.requests_cancelled)
        .saturating_add(io.requests_failed);
    let operations = io.buffered.operations.saturating_add(io.direct.operations);
    let bytes = io.buffered.bytes.saturating_add(io.direct.bytes);
    let seconds = elapsed.as_secs_f64();
    let iops = rate(operations as f64, seconds);
    let bandwidth = rate(bytes as f64, seconds);
    let average_slot_wait_ns = average(io.slot_wait_ns, io.requests_submitted);
    let average_request_ns = average(io.request_time_ns, completed);
    println!(
        "  {direction}: IOPS={}, BW={}, submitted={}, completed={}, errors={}, cancelled={}, depth_peak={}, slat_avg={}ns, clat_avg={}ns, buffered={}/{}, direct={}/{}",
        format_count(iops),
        format_rate_bytes(bandwidth),
        io.requests_submitted,
        completed,
        io.requests_failed,
        io.requests_cancelled,
        io.requests_in_flight_peak,
        average_slot_wait_ns,
        average_request_ns,
        io.buffered.operations,
        format_bytes(io.buffered.bytes as f64),
        io.direct.operations,
        format_bytes(io.direct.bytes as f64),
    );
    println!(
        "report version=1 type=io benchmark={} scenario={} phase={} direction={} runtime_ns={} requests_submitted={} requests_succeeded={} requests_failed={} requests_cancelled={} in_flight={} in_flight_peak={} slot_wait_ns={} request_time_ns={} average_slot_wait_ns={} average_request_time_ns={} buffered_operations={} buffered_bytes={} direct_operations={} direct_bytes={} operations={} bytes={} iops={:.3} bw_bytes_per_sec={:.3}",
        benchmark,
        scenario,
        phase,
        direction,
        elapsed.as_nanos(),
        io.requests_submitted,
        io.requests_succeeded,
        io.requests_failed,
        io.requests_cancelled,
        io.requests_in_flight,
        io.requests_in_flight_peak,
        io.slot_wait_ns,
        io.request_time_ns,
        average_slot_wait_ns,
        average_request_ns,
        io.buffered.operations,
        io.buffered.bytes,
        io.direct.operations,
        io.direct.bytes,
        operations,
        bytes,
        iops,
        bandwidth,
    );
}

#[derive(Clone, Copy, Default)]
struct ProcessUsage {
    user_cpu: Duration,
    system_cpu: Duration,
    maximum_rss_bytes: u64,
    minor_faults: u64,
    major_faults: u64,
    voluntary_context_switches: u64,
    involuntary_context_switches: u64,
}

impl ProcessUsage {
    #[cfg(unix)]
    fn capture() -> Self {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
        // SAFETY: `usage` points to writable storage for one `rusage` value.
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
            return Self::default();
        }
        // SAFETY: a successful `getrusage` initialized the complete value.
        let usage = unsafe { usage.assume_init() };
        Self {
            user_cpu: timeval_duration(usage.ru_utime),
            system_cpu: timeval_duration(usage.ru_stime),
            maximum_rss_bytes: maximum_rss_bytes(usage.ru_maxrss),
            minor_faults: nonnegative_u64(usage.ru_minflt),
            major_faults: nonnegative_u64(usage.ru_majflt),
            voluntary_context_switches: nonnegative_u64(usage.ru_nvcsw),
            involuntary_context_switches: nonnegative_u64(usage.ru_nivcsw),
        }
    }

    #[cfg(not(unix))]
    fn capture() -> Self {
        Self::default()
    }

    fn subtract(self, earlier: Self) -> Self {
        Self {
            user_cpu: self.user_cpu.saturating_sub(earlier.user_cpu),
            system_cpu: self.system_cpu.saturating_sub(earlier.system_cpu),
            maximum_rss_bytes: self.maximum_rss_bytes,
            minor_faults: self.minor_faults.saturating_sub(earlier.minor_faults),
            major_faults: self.major_faults.saturating_sub(earlier.major_faults),
            voluntary_context_switches: self
                .voluntary_context_switches
                .saturating_sub(earlier.voluntary_context_switches),
            involuntary_context_switches: self
                .involuntary_context_switches
                .saturating_sub(earlier.involuntary_context_switches),
        }
    }
}

fn latency_bucket(nanos: u64) -> usize {
    if nanos <= 1 {
        0
    } else {
        usize::try_from(u64::BITS - (nanos - 1).leading_zeros()).unwrap_or(64)
    }
}

fn bucket_upper_bound_ns(index: usize) -> u64 {
    if index >= 64 {
        u64::MAX
    } else {
        1_u64 << index
    }
}

fn bucket_midpoint_ns(index: usize) -> f64 {
    if index == 0 {
        return 1.0;
    }
    if index >= 64 {
        return u64::MAX as f64;
    }
    let lower = (1_u64 << (index - 1)).saturating_add(1);
    let upper = 1_u64 << index;
    lower as f64 + upper.saturating_sub(lower) as f64 / 2.0
}

fn rate(value: f64, seconds: f64) -> f64 {
    if seconds > 0.0 { value / seconds } else { 0.0 }
}

fn average(total: u64, count: u64) -> u64 {
    total.checked_div(count).unwrap_or(0)
}

fn format_count(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.2}k", value / 1_000.0)
    } else {
        format!("{value:.0}")
    }
}

fn format_rate_bytes(value: f64) -> String {
    format!("{}/s", format_bytes(value))
}

fn format_bytes(value: f64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    if value >= GIB {
        format!("{:.2}GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2}MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.2}KiB", value / KIB)
    } else {
        format!("{value:.0}B")
    }
}

fn format_duration(duration: Duration) -> String {
    if duration.as_secs() != 0 {
        format!("{:.3}s", duration.as_secs_f64())
    } else if duration.as_millis() != 0 {
        format!("{:.3}ms", duration.as_secs_f64() * 1_000.0)
    } else {
        format!("{:.3}us", duration.as_secs_f64() * 1_000_000.0)
    }
}

#[cfg(unix)]
fn timeval_duration(value: libc::timeval) -> Duration {
    Duration::from_secs(nonnegative_u64(value.tv_sec))
        .saturating_add(Duration::from_micros(nonnegative_u64(value.tv_usec)))
}

#[cfg(all(unix, any(target_os = "macos", target_os = "ios")))]
fn maximum_rss_bytes(value: libc::c_long) -> u64 {
    nonnegative_u64(value)
}

#[cfg(all(unix, not(any(target_os = "macos", target_os = "ios"))))]
fn maximum_rss_bytes(value: libc::c_long) -> u64 {
    nonnegative_u64(value).saturating_mul(1024)
}

#[cfg(unix)]
fn nonnegative_u64<T>(value: T) -> u64
where
    T: TryInto<u64>,
{
    value.try_into().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_merges_and_reports_complete_percentiles() {
        let mut first = LatencyHistogram::default();
        first.record(Duration::from_nanos(10));
        first.record(Duration::from_nanos(100));
        let mut second = LatencyHistogram::default();
        second.record(Duration::from_nanos(1_000));
        first.merge(second);

        let summary = first.summary();
        assert_eq!(summary.samples, 3);
        assert_eq!(summary.minimum_ns, 10);
        assert_eq!(summary.maximum_ns, 1_000);
        assert_eq!(summary.p50_upper_ns, 128);
        assert_eq!(summary.p90_upper_ns, 1_024);
        assert_eq!(summary.p999_upper_ns, 1_024);
        assert!(summary.mean_ns > 0.0);
        assert!(summary.standard_deviation_ns > 0.0);
    }

    #[test]
    fn atomic_histogram_preserves_samples_and_extrema() {
        let histogram = AtomicLatencyHistogram::default();
        histogram.record(Duration::from_nanos(7));
        histogram.record(Duration::from_nanos(70));

        let summary = histogram.snapshot().summary();
        assert_eq!(summary.samples, 2);
        assert_eq!(summary.minimum_ns, 7);
        assert_eq!(summary.maximum_ns, 70);
        assert_eq!(summary.p50_upper_ns, 8);
        assert_eq!(summary.p99_upper_ns, 128);
    }
}
