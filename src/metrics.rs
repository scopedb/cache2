//! Dependency-free production metrics for cache-rs.
//!
//! The engine keeps fixed-size request latency histograms and exposes an
//! OpenMetrics text snapshot. OpenTelemetry deployments can scrape the same
//! endpoint with the Collector's Prometheus receiver, avoiding an SDK and a
//! telemetry worker inside the cache process.

use std::fmt;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::cache::{CacheStats, CacheStatus};
use crate::miss_guard::OriginFillStats;

pub const LATENCY_BUCKET_UPPER_US: [u64; 24] = [
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536,
    131_072, 262_144, 524_288, 1_048_576, 2_097_152, 4_194_304, 8_388_608,
];
pub const LATENCY_BUCKET_COUNT: usize = LATENCY_BUCKET_UPPER_US.len() + 1;
const OPERATION_COUNT: usize = 6;
const RESULT_CLASS_COUNT: usize = 12;
const ERROR_CLASS_COUNT: usize = 12;
const STATE_HISTORY_CAPACITY: usize = 32;

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CacheOperation {
    Get,
    Put,
    Remove,
    Flush,
    Clear,
    Close,
}

impl CacheOperation {
    pub const ALL: [Self; OPERATION_COUNT] = [
        Self::Get,
        Self::Put,
        Self::Remove,
        Self::Flush,
        Self::Clear,
        Self::Close,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::Remove => "remove",
            Self::Flush => "flush",
            Self::Clear => "clear",
            Self::Close => "close",
        }
    }
}

/// Stable, low-cardinality classification for public API errors.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum CacheErrorClass {
    InvalidConfig,
    Locked,
    CorruptMetadata,
    NoSpace,
    Permission,
    DeviceIo,
    Overloaded,
    ReclaimBacklog,
    TimedOut,
    Cancelled,
    Poisoned,
    Closed,
}

impl CacheErrorClass {
    pub const ALL: [Self; ERROR_CLASS_COUNT] = [
        Self::InvalidConfig,
        Self::Locked,
        Self::CorruptMetadata,
        Self::NoSpace,
        Self::Permission,
        Self::DeviceIo,
        Self::Overloaded,
        Self::ReclaimBacklog,
        Self::TimedOut,
        Self::Cancelled,
        Self::Poisoned,
        Self::Closed,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidConfig => "invalid_config",
            Self::Locked => "locked",
            Self::CorruptMetadata => "corrupt_metadata",
            Self::NoSpace => "no_space",
            Self::Permission => "permission",
            Self::DeviceIo => "device_io",
            Self::Overloaded => "overloaded",
            Self::ReclaimBacklog => "reclaim_backlog",
            Self::TimedOut => "timed_out",
            Self::Cancelled => "cancelled",
            Self::Poisoned => "poisoned",
            Self::Closed => "closed",
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RequestResultClass {
    Success,
    Hit,
    Miss,
    Stored,
    Removed,
    NotFound,
    Rejected,
    IoError,
    Corrupt,
    Overloaded,
    Unavailable,
    Cancelled,
}

impl RequestResultClass {
    pub const ALL: [Self; RESULT_CLASS_COUNT] = [
        Self::Success,
        Self::Hit,
        Self::Miss,
        Self::Stored,
        Self::Removed,
        Self::NotFound,
        Self::Rejected,
        Self::IoError,
        Self::Corrupt,
        Self::Overloaded,
        Self::Unavailable,
        Self::Cancelled,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Hit => "hit",
            Self::Miss => "miss",
            Self::Stored => "stored",
            Self::Removed => "removed",
            Self::NotFound => "not_found",
            Self::Rejected => "rejected",
            Self::IoError => "io_error",
            Self::Corrupt => "corrupt",
            Self::Overloaded => "overloaded",
            Self::Unavailable => "unavailable",
            Self::Cancelled => "cancelled",
        }
    }
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StateChangeReason {
    Opened,
    RecoveryCompleted,
    IoFailure,
    MetadataFailure,
    WorkerFailure,
    Closing,
}

impl StateChangeReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::RecoveryCompleted => "recovery_completed",
            Self::IoFailure => "io_failure",
            Self::MetadataFailure => "metadata_failure",
            Self::WorkerFailure => "worker_failure",
            Self::Closing => "closing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateTransition {
    pub sequence: u64,
    pub unix_ms: u64,
    pub from: CacheStatus,
    pub to: CacheStatus,
    pub reason: StateChangeReason,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LatencyHistogramSnapshot {
    /// Non-cumulative counts. The last element is the `+Inf` bucket.
    pub bucket_counts: [u64; LATENCY_BUCKET_COUNT],
    pub count: u64,
    pub sum_us: u64,
    pub max_us: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationMetricsSnapshot {
    pub operation: CacheOperation,
    pub results: [u64; RESULT_CLASS_COUNT],
    pub errors: [u64; ERROR_CLASS_COUNT],
    pub latency: LatencyHistogramSnapshot,
}

impl OperationMetricsSnapshot {
    pub fn result_count(&self, class: RequestResultClass) -> u64 {
        self.results[class as usize]
    }

    pub fn error_count(&self, class: CacheErrorClass) -> u64 {
        self.errors[class as usize]
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub status: CacheStatus,
    pub stats: CacheStats,
    pub origin_fills: OriginFillStats,
    pub operations: [OperationMetricsSnapshot; OPERATION_COUNT],
    /// Oldest-to-newest bounded lifecycle history.
    pub state_transitions: Vec<StateTransition>,
}

impl MetricsSnapshot {
    /// Encode one self-contained OpenMetrics exposition.
    pub fn to_openmetrics(&self) -> String {
        let mut output = String::new();
        // Writing to String is infallible.
        self.write_openmetrics(&mut output)
            .expect("writing OpenMetrics to a String cannot fail");
        output
    }

    pub fn write_openmetrics(&self, output: &mut impl fmt::Write) -> fmt::Result {
        output.write_str("# HELP cache_rs_up Whether the cache can serve normal traffic.\n")?;
        output.write_str("# TYPE cache_rs_up gauge\n")?;
        writeln!(
            output,
            "cache_rs_up {}",
            u8::from(self.status == CacheStatus::Healthy)
        )?;
        output.write_str("# HELP cache_rs_status Current cache lifecycle state.\n")?;
        output.write_str("# TYPE cache_rs_status gauge\n")?;
        for status in [
            CacheStatus::Healthy,
            CacheStatus::MissOnly,
            CacheStatus::Poisoned,
            CacheStatus::Closed,
        ] {
            writeln!(
                output,
                "cache_rs_status{{state=\"{}\"}} {}",
                status_name(status),
                u8::from(status == self.status)
            )?;
        }

        output.write_str("# HELP cache_rs_requests_total Cache API results by operation.\n")?;
        output.write_str("# TYPE cache_rs_requests_total counter\n")?;
        for operation in &self.operations {
            for class in RequestResultClass::ALL {
                let value = operation.result_count(class);
                if value != 0 {
                    writeln!(
                        output,
                        "cache_rs_requests_total{{operation=\"{}\",result=\"{}\"}} {value}",
                        operation.operation.as_str(),
                        class.as_str()
                    )?;
                }
            }
        }

        output.write_str("# HELP cache_rs_request_errors_total Cache API errors by class.\n")?;
        output.write_str("# TYPE cache_rs_request_errors_total counter\n")?;
        for operation in &self.operations {
            for class in CacheErrorClass::ALL {
                let value = operation.error_count(class);
                if value != 0 {
                    writeln!(
                        output,
                        "cache_rs_request_errors_total{{operation=\"{}\",class=\"{}\"}} {value}",
                        operation.operation.as_str(),
                        class.as_str()
                    )?;
                }
            }
        }

        output.write_str(
            "# HELP cache_rs_request_duration_seconds Cache API latency by operation.\n",
        )?;
        output.write_str("# TYPE cache_rs_request_duration_seconds histogram\n")?;
        for operation in &self.operations {
            let mut cumulative = 0_u64;
            for (index, upper_us) in LATENCY_BUCKET_UPPER_US.iter().enumerate() {
                cumulative = cumulative.saturating_add(operation.latency.bucket_counts[index]);
                writeln!(
                    output,
                    "cache_rs_request_duration_seconds_bucket{{operation=\"{}\",le=\"{}\"}} {cumulative}",
                    operation.operation.as_str(),
                    seconds_label(*upper_us)
                )?;
            }
            cumulative = cumulative
                .saturating_add(operation.latency.bucket_counts[LATENCY_BUCKET_COUNT - 1]);
            writeln!(
                output,
                "cache_rs_request_duration_seconds_bucket{{operation=\"{}\",le=\"+Inf\"}} {cumulative}",
                operation.operation.as_str()
            )?;
            writeln!(
                output,
                "cache_rs_request_duration_seconds_sum{{operation=\"{}\"}} {:.6}",
                operation.operation.as_str(),
                operation.latency.sum_us as f64 / 1_000_000.0
            )?;
            writeln!(
                output,
                "cache_rs_request_duration_seconds_count{{operation=\"{}\"}} {}",
                operation.operation.as_str(),
                operation.latency.count
            )?;
        }

        write_counter(output, "cache_rs_hits_total", self.stats.hits)?;
        write_counter(output, "cache_rs_misses_total", self.stats.misses)?;
        write_counter(output, "cache_rs_puts_total", self.stats.puts)?;
        write_counter(output, "cache_rs_removes_total", self.stats.removes)?;
        write_counter(output, "cache_rs_rejected_total", self.stats.rejected)?;
        write_counter(output, "cache_rs_io_errors_total", self.stats.io_errors)?;
        write_counter(
            output,
            "cache_rs_corrupt_records_total",
            self.stats.corrupt_records,
        )?;
        write_counter(
            output,
            "cache_rs_checkpoint_errors_total",
            self.stats.checkpoint_errors,
        )?;
        write_counter(
            output,
            "cache_rs_host_write_bytes_total",
            self.stats.host_write_bytes,
        )?;
        write_counter(output, "cache_rs_bytes_read_total", self.stats.bytes_read)?;
        write_counter(
            output,
            "cache_rs_foreground_record_bytes_total",
            self.stats.foreground_record_bytes,
        )?;
        write_counter(
            output,
            "cache_rs_reinsertion_bytes_total",
            self.stats.reinsertion_bytes,
        )?;
        write_counter(
            output,
            "cache_rs_checkpoint_write_bytes_total",
            self.stats.checkpoint_write_bytes,
        )?;
        write_counter(
            output,
            "cache_rs_queue_rejections_total",
            self.stats.queue_rejections,
        )?;
        write_counter(
            output,
            "cache_rs_buffer_rejections_total",
            self.stats.buffer_rejections,
        )?;
        output.write_str("# TYPE cache_rs_backpressure_wait_ns_total counter\n")?;
        for (resource, wait_ns) in [
            ("read_queue", self.stats.read_queue_wait_ns),
            ("write_queue", self.stats.write_queue_wait_ns),
            ("control_queue", self.stats.control_queue_wait_ns),
            ("read_buffer", self.stats.read_buffer_wait_ns),
            ("write_buffer", self.stats.write_buffer_wait_ns),
            ("control_buffer", self.stats.control_buffer_wait_ns),
            ("metadata_buffer", self.stats.metadata_buffer_wait_ns),
        ] {
            writeln!(
                output,
                "cache_rs_backpressure_wait_ns_total{{resource=\"{resource}\"}} {wait_ns}"
            )?;
        }
        write_counter(
            output,
            "cache_rs_reclaim_backlog_rejections_total",
            self.stats.reclaim_backlog_rejections,
        )?;
        write_counter(
            output,
            "cache_rs_background_regions_reclaimed_total",
            self.stats.background_regions_reclaimed,
        )?;
        write_counter(
            output,
            "cache_rs_reclaim_records_scanned_total",
            self.stats.reclaim_records_scanned,
        )?;
        write_counter(
            output,
            "cache_rs_reclaim_index_fallbacks_total",
            self.stats.reclaim_index_fallbacks,
        )?;
        write_counter(
            output,
            "cache_rs_origin_fill_attempts_total",
            self.origin_fills.attempts,
        )?;
        write_counter(
            output,
            "cache_rs_origin_fill_admitted_total",
            self.origin_fills.admitted,
        )?;
        write_counter(
            output,
            "cache_rs_origin_fill_rate_limited_total",
            self.origin_fills.rate_limited,
        )?;
        write_counter(
            output,
            "cache_rs_origin_fill_concurrency_limited_total",
            self.origin_fills.concurrency_limited,
        )?;
        write_gauge(output, "cache_rs_entries", self.stats.entries)?;
        write_gauge(
            output,
            "cache_rs_memory_used_bytes",
            self.stats.memory_used_bytes,
        )?;
        write_gauge(
            output,
            "cache_rs_memory_budget_bytes",
            self.stats.memory_budget_bytes,
        )?;
        write_gauge(
            output,
            "cache_rs_read_queue_depth",
            self.stats.read_queue_depth,
        )?;
        write_gauge(
            output,
            "cache_rs_write_queue_depth",
            self.stats.write_queue_depth,
        )?;
        write_gauge(output, "cache_rs_io_in_flight", self.stats.io_in_flight)?;
        write_gauge(
            output,
            "cache_rs_io_in_flight_peak",
            self.stats.io_in_flight_peak,
        )?;
        write_gauge(
            output,
            "cache_rs_origin_fills_in_flight",
            self.origin_fills.in_flight,
        )?;
        write_gauge(
            output,
            "cache_rs_recovery_in_progress",
            u64::from(self.stats.recovery_in_progress),
        )?;
        write_gauge(
            output,
            "cache_rs_recovery_regions_completed",
            self.stats.recovery_regions_completed,
        )?;
        write_gauge(
            output,
            "cache_rs_recovery_regions_total",
            self.stats.recovery_regions_total,
        )?;
        write_gauge(
            output,
            "cache_rs_region_valid_ratio_basis_points",
            self.stats.minimum_region_valid_ratio_bps,
        )?;
        write_gauge(
            output,
            "cache_rs_write_amplification_milli",
            self.stats.write_amplification_milli,
        )?;
        write_gauge(
            output,
            "cache_rs_nvme_health_critical",
            u64::from(self.stats.nvme_health_critical),
        )?;
        write_counter(
            output,
            "cache_rs_state_transitions_total",
            self.state_transitions
                .last()
                .map_or(0, |event| event.sequence),
        )?;
        output.write_str("# EOF\n")
    }

    /// Write the bounded lifecycle history as one JSON object per line.
    ///
    /// Field values are fixed engine enums; cache paths, keys, and tenant
    /// supplied strings are deliberately absent.
    pub fn write_state_log_json(&self, output: &mut impl fmt::Write) -> fmt::Result {
        for event in &self.state_transitions {
            writeln!(
                output,
                concat!(
                    "{{\"schema_version\":1,\"event\":\"cache_state_change\",",
                    "\"sequence\":{},\"unix_ms\":{},\"from\":\"{}\",",
                    "\"to\":\"{}\",\"reason\":\"{}\"}}"
                ),
                event.sequence,
                event.unix_ms,
                status_name(event.from),
                status_name(event.to),
                event.reason.as_str(),
            )?;
        }
        Ok(())
    }

    pub fn state_log_json(&self) -> String {
        let mut output = String::new();
        self.write_state_log_json(&mut output)
            .expect("writing state events to a String cannot fail");
        output
    }
}

pub(crate) struct RequestTelemetry {
    operations: [OperationTelemetry; OPERATION_COUNT],
    history: Mutex<StateHistory>,
}

impl RequestTelemetry {
    pub(crate) fn new(initial_status: CacheStatus) -> Self {
        let telemetry = Self {
            operations: std::array::from_fn(|_| OperationTelemetry::new()),
            history: Mutex::new(StateHistory::new()),
        };
        telemetry.record_transition(initial_status, initial_status, StateChangeReason::Opened);
        telemetry
    }

    pub(crate) fn observe(
        &self,
        operation: CacheOperation,
        class: RequestResultClass,
        error: Option<CacheErrorClass>,
        elapsed: Duration,
    ) {
        self.operations[operation as usize].observe(class, error, elapsed);
    }

    pub(crate) fn record_transition(
        &self,
        from: CacheStatus,
        to: CacheStatus,
        reason: StateChangeReason,
    ) {
        let mut history = self
            .history
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        history.push(from, to, reason);
    }

    pub(crate) fn snapshot(
        &self,
        status: CacheStatus,
        stats: CacheStats,
        origin_fills: OriginFillStats,
    ) -> MetricsSnapshot {
        MetricsSnapshot {
            status,
            stats,
            origin_fills,
            operations: std::array::from_fn(|index| {
                self.operations[index].snapshot(CacheOperation::ALL[index])
            }),
            state_transitions: self
                .history
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .snapshot(),
        }
    }

    pub(crate) fn operation_snapshots(&self) -> [OperationMetricsSnapshot; OPERATION_COUNT] {
        std::array::from_fn(|index| self.operations[index].snapshot(CacheOperation::ALL[index]))
    }

    pub(crate) fn state_transitions(&self) -> Vec<StateTransition> {
        self.history
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .snapshot()
    }
}

struct OperationTelemetry {
    results: [AtomicU64; RESULT_CLASS_COUNT],
    errors: [AtomicU64; ERROR_CLASS_COUNT],
    buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    count: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
}

impl OperationTelemetry {
    fn new() -> Self {
        Self {
            results: std::array::from_fn(|_| AtomicU64::new(0)),
            errors: std::array::from_fn(|_| AtomicU64::new(0)),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    fn observe(
        &self,
        class: RequestResultClass,
        error: Option<CacheErrorClass>,
        elapsed: Duration,
    ) {
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        let bucket = LATENCY_BUCKET_UPPER_US
            .iter()
            .position(|upper| elapsed_us <= *upper)
            .unwrap_or(LATENCY_BUCKET_COUNT - 1);
        self.results[class as usize].fetch_add(1, Ordering::Relaxed);
        if let Some(error) = error {
            self.errors[error as usize].fetch_add(1, Ordering::Relaxed);
        }
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(elapsed_us, Ordering::Relaxed);
        self.max_us.fetch_max(elapsed_us, Ordering::Relaxed);
    }

    fn snapshot(&self, operation: CacheOperation) -> OperationMetricsSnapshot {
        OperationMetricsSnapshot {
            operation,
            results: std::array::from_fn(|index| self.results[index].load(Ordering::Relaxed)),
            errors: std::array::from_fn(|index| self.errors[index].load(Ordering::Relaxed)),
            latency: LatencyHistogramSnapshot {
                bucket_counts: std::array::from_fn(|index| {
                    self.buckets[index].load(Ordering::Relaxed)
                }),
                count: self.count.load(Ordering::Relaxed),
                sum_us: self.sum_us.load(Ordering::Relaxed),
                max_us: self.max_us.load(Ordering::Relaxed),
            },
        }
    }
}

struct StateHistory {
    sequence: u64,
    next: usize,
    len: usize,
    entries: [Option<StateTransition>; STATE_HISTORY_CAPACITY],
}

impl StateHistory {
    fn new() -> Self {
        Self {
            sequence: 0,
            next: 0,
            len: 0,
            entries: [None; STATE_HISTORY_CAPACITY],
        }
    }

    fn push(&mut self, from: CacheStatus, to: CacheStatus, reason: StateChangeReason) {
        self.sequence = self.sequence.saturating_add(1);
        self.entries[self.next] = Some(StateTransition {
            sequence: self.sequence,
            unix_ms: now_unix_ms(),
            from,
            to,
            reason,
        });
        self.next = (self.next + 1) % STATE_HISTORY_CAPACITY;
        self.len = self.len.saturating_add(1).min(STATE_HISTORY_CAPACITY);
    }

    fn snapshot(&self) -> Vec<StateTransition> {
        let mut output = Vec::with_capacity(self.len);
        let start = (self.next + STATE_HISTORY_CAPACITY - self.len) % STATE_HISTORY_CAPACITY;
        for offset in 0..self.len {
            if let Some(event) = self.entries[(start + offset) % STATE_HISTORY_CAPACITY] {
                output.push(event);
            }
        }
        output
    }
}

fn write_counter(output: &mut impl fmt::Write, name: &str, value: u64) -> fmt::Result {
    writeln!(output, "# TYPE {name} counter")?;
    writeln!(output, "{name} {value}")
}

fn write_gauge(output: &mut impl fmt::Write, name: &str, value: u64) -> fmt::Result {
    writeln!(output, "# TYPE {name} gauge")?;
    writeln!(output, "{name} {value}")
}

fn seconds_label(microseconds: u64) -> String {
    if microseconds < 1_000_000 {
        format!("0.{microseconds:06}")
    } else {
        format!(
            "{}.{:06}",
            microseconds / 1_000_000,
            microseconds % 1_000_000
        )
    }
}

fn status_name(status: CacheStatus) -> &'static str {
    match status {
        CacheStatus::Healthy => "healthy",
        CacheStatus::MissOnly => "miss_only",
        CacheStatus::Poisoned => "poisoned",
        CacheStatus::Closed => "closed",
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_and_openmetrics_have_stable_cumulative_counts() {
        let telemetry = RequestTelemetry::new(CacheStatus::Healthy);
        telemetry.observe(
            CacheOperation::Get,
            RequestResultClass::Hit,
            None,
            Duration::from_micros(1),
        );
        telemetry.observe(
            CacheOperation::Get,
            RequestResultClass::Miss,
            None,
            Duration::from_micros(3),
        );
        let snapshot = telemetry.snapshot(
            CacheStatus::Healthy,
            CacheStats {
                reclaim_records_scanned: 17,
                reclaim_index_fallbacks: 2,
                control_queue_wait_ns: 19,
                ..CacheStats::default()
            },
            OriginFillStats::default(),
        );
        let get = &snapshot.operations[CacheOperation::Get as usize];
        assert_eq!(get.latency.count, 2);
        assert_eq!(get.latency.bucket_counts[0], 1);
        assert_eq!(get.latency.bucket_counts[2], 1);
        let encoded = snapshot.to_openmetrics();
        assert!(encoded.contains(
            "cache_rs_request_duration_seconds_bucket{operation=\"get\",le=\"0.000004\"} 2"
        ));
        assert!(encoded.contains("cache_rs_requests_total{operation=\"get\",result=\"hit\"} 1"));
        assert!(encoded.contains("cache_rs_reclaim_records_scanned_total 17"));
        assert!(encoded.contains("cache_rs_reclaim_index_fallbacks_total 2"));
        assert!(
            encoded.contains("cache_rs_backpressure_wait_ns_total{resource=\"control_queue\"} 19")
        );
        assert!(encoded.ends_with("# EOF\n"));
    }

    #[test]
    fn state_history_is_bounded_and_chronological() {
        let telemetry = RequestTelemetry::new(CacheStatus::Healthy);
        for index in 0..(STATE_HISTORY_CAPACITY + 5) {
            telemetry.record_transition(
                CacheStatus::Healthy,
                if index % 2 == 0 {
                    CacheStatus::MissOnly
                } else {
                    CacheStatus::Healthy
                },
                StateChangeReason::IoFailure,
            );
        }
        let snapshot = telemetry.snapshot(
            CacheStatus::Healthy,
            CacheStats::default(),
            OriginFillStats::default(),
        );
        assert_eq!(snapshot.state_transitions.len(), STATE_HISTORY_CAPACITY);
        assert!(
            snapshot
                .state_transitions
                .windows(2)
                .all(|pair| pair[0].sequence < pair[1].sequence)
        );
        let log = snapshot.state_log_json();
        assert_eq!(log.lines().count(), STATE_HISTORY_CAPACITY);
        assert!(log.contains("\"event\":\"cache_state_change\""));
        assert!(log.contains("\"reason\":\"io_failure\""));
    }
}
