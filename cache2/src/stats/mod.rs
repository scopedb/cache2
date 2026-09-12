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

//! Optional request accounting and cumulative latency distributions.
//!
//! The legacy `RuntimeOptions::statistics` switch retains its existing behavior.
//! These options add public request outcomes and timing independently. Sampled
//! durations describe observed requests only; they are not full-population SLOs.

use std::num::NonZeroU32;

use crate::snapshot::CacheSnapshot;

pub(crate) mod recording;

/// Duration collection for one timing scope, selected before its outcome is known.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LatencyMode {
    /// No clocks or histogram updates for this scope.
    #[default]
    Off,
    /// Observe every terminal operation in this scope; no sampling RNG work.
    Full,
    /// Observe each request with probability approximately `1 / interval`.
    /// Counts and sums retain sampled semantics. One is equivalent to full.
    Sampled {
        /// Mean number of requests per observation.
        interval: NonZeroU32,
    },
}

/// Additional statistics allocated once per open, independent of legacy counters.
///
/// Defaults allocate no counter or histogram stripes. Enable `request_counters` for complete
/// terminal accounting even when durations are sampled. All enabled storage is
/// included in the managed-memory plan. Configuration cannot change while open.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatsOptions {
    /// Count every terminal get, put, put_l2 and delete by exclusive result.
    pub request_counters: bool,
    /// Time L1 hits from the first poll to return. L1 misses discard this timer.
    pub l1_latency: LatencyMode,
    /// Time reads from L1 miss through L2 lookup, waiting, I/O and promotion.
    /// Excludes the initial L1 lookup. Early misses before L1 lookup are not timed.
    pub l2_latency: LatencyMode,
    /// Time put, put_l2 and delete from entry to their terminal result.
    pub mutation_latency: LatencyMode,
    /// Fully observe engine requests from slot reservation to terminal completion.
    /// Split by foreground read, append write and reclaim read; not device latency.
    pub io_latency: bool,
    /// Fixed recorder stripes. Must be a power of two in 1..=64; defaults to 16.
    /// Colliding threads use relaxed atomic updates, never a recorder lock.
    pub shards: usize,
}

impl Default for StatsOptions {
    fn default() -> Self {
        Self {
            request_counters: false,
            l1_latency: LatencyMode::Off,
            l2_latency: LatencyMode::Off,
            mutation_latency: LatencyMode::Off,
            io_latency: false,
            shards: 16,
        }
    }
}

impl StatsOptions {
    pub(crate) fn requests_enabled(self) -> bool {
        self.request_counters
            || self.l1_latency != LatencyMode::Off
            || self.l2_latency != LatencyMode::Off
            || self.mutation_latency != LatencyMode::Off
    }

    pub(crate) fn latency_mode(self, scope: RequestLatencyScope) -> LatencyMode {
        match scope {
            RequestLatencyScope::L1Hit => self.l1_latency,
            RequestLatencyScope::L2Lookup => self.l2_latency,
            RequestLatencyScope::Mutation => self.mutation_latency,
        }
    }
}

/// Public operation whose terminal outcome is recorded.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOperation {
    /// Lookup, starting at first poll; an unpolled future is not a request.
    Get,
    /// Acceptance into staging with best-effort L1 admission.
    Put,
    /// Acceptance into staging without L1 admission.
    PutL2,
    /// Bounded index deletion and best-effort L1 cleanup.
    Delete,
}

impl RequestOperation {
    /// Stable low-cardinality metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "get",
            Self::Put => "put",
            Self::PutL2 => "put_l2",
            Self::Delete => "delete",
        }
    }
}

/// Mutually exclusive terminal result; internal events may overlap these counts.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestOutcome {
    /// A value found in L1 at lookup, not an L2 value subsequently promoted.
    L1Hit,
    /// A validated L2 value, including values promoted to L1 before return.
    L2Hit,
    /// No value returned, including fail-open and post-close gets.
    Miss,
    /// Mutation accepted; does not imply flush or durability.
    Accepted,
    /// Explicit bounded admission or deadline rejection.
    Overloaded,
    /// Invalid public mutation input.
    InvalidInput,
    /// Mutation attempted after shutdown or loss of availability.
    Unavailable,
    /// Another explicit public error; safe fail-open reads remain misses.
    Error,
    /// A polled get future dropped before returning; duration is partial lifetime.
    Cancelled,
}

impl RequestOutcome {
    /// Stable low-cardinality metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::L1Hit => "l1_hit",
            Self::L2Hit => "l2_hit",
            Self::Miss => "miss",
            Self::Accepted => "accepted",
            Self::Overloaded => "overloaded",
            Self::InvalidInput => "invalid_input",
            Self::Unavailable => "unavailable",
            Self::Error => "error",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Engine ownership, independent of buffered/direct file-operation accounting.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoRole {
    /// Foreground L2 lookups.
    Read,
    /// Foreground and reinsertion append batches.
    Write,
    /// Source Region reads by reclaim workers.
    Reclaim,
}

impl IoRole {
    /// Stable metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Reclaim => "reclaim",
        }
    }
}

/// Terminal engine result, distinct from caller cancellation or timeout.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoOutcome {
    /// Operation completed successfully.
    Completed,
    /// Engine completed the operation through cancellation.
    Cancelled,
    /// Engine reported a failure.
    Failed,
}

impl IoOutcome {
    /// Stable metric label.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }
}

/// Version-one explicit duration bucket boundaries, in nanoseconds, inclusive.
///
/// A final unbounded bucket follows these finite bounds. The layout includes
/// common SLO thresholds and is shared by all instances. Quantiles have bucket
/// resolution, not an exact-value or relative-error guarantee.
pub const LATENCY_BUCKET_UPPER_BOUNDS_NS: &[u64] = &[
    0,
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    25_000_000,
    50_000_000,
    100_000_000,
    250_000_000,
    500_000_000,
    1_000_000_000,
    2_500_000_000,
    5_000_000_000,
    10_000_000_000,
    30_000_000_000,
    60_000_000_000,
];

/// Cumulative observations for one population since open.
///
/// Snapshots never reset or consume counts. Concurrent updates can appear at
/// slightly different times in buckets and sum; quiescent snapshots are exact.
/// The sum covers actual observed durations, including overflow-bucket values.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LatencySnapshot {
    /// Noncumulative bucket counts: one per finite bound plus the unbounded bucket.
    pub bucket_counts: Box<[u64]>,
    /// Sum of these bucket counts, not an estimate of full request volume.
    pub count: u64,
    /// Sum of observed durations in nanoseconds, widened when stripes are merged.
    pub sum_ns: u128,
    /// False after recorder arithmetic overflow. Do not export invalid distributions.
    pub valid: bool,
}

/// Interval measured by a request duration distribution.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestLatencyScope {
    /// First public get poll through return of an L1 hit.
    L1Hit,
    /// L1 miss through terminal result, including index lookup, waiting and promotion.
    /// This is not the full public get duration.
    L2Lookup,
    /// Public put, put_l2 or delete entry through its terminal result.
    Mutation,
}

impl RequestLatencyScope {
    pub(crate) fn for_request(operation: RequestOperation, outcome: RequestOutcome) -> Self {
        match operation {
            RequestOperation::Get if outcome == RequestOutcome::L1Hit => Self::L1Hit,
            RequestOperation::Get => Self::L2Lookup,
            _ => Self::Mutation,
        }
    }
}

/// One allowed public-operation/result combination.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestStatsSnapshot {
    /// Public operation.
    pub operation: RequestOperation,
    /// Exclusive terminal result.
    pub outcome: RequestOutcome,
    /// Full event count, or None when request counters are disabled.
    pub count: Option<u64>,
    /// Interval covered by this row's optional duration distribution.
    pub latency_scope: RequestLatencyScope,
    /// Collection mode for this row, independent of other tiers and mutations.
    pub latency_mode: LatencyMode,
    /// Full or sampled duration distribution, or None when timing is disabled.
    /// L1 hits cover the public lookup; other get rows start at L1 miss.
    /// Early misses before L1 lookup contribute only to the request counter.
    pub latency: Option<LatencySnapshot>,
}

/// Fully observed engine durations for one role/result combination.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IoLatencySnapshot {
    /// Engine ownership.
    pub role: IoRole,
    /// Engine terminal result.
    pub outcome: IoOutcome,
    /// Full duration observations, irrespective of public-request sampling.
    pub latency: LatencySnapshot,
}

/// Cumulative monitoring view without metadata scans or recorder locks.
///
/// Legacy activity availability is given by summary.statistics_enabled; new
/// families use explicit options and optional values. The application owns
/// collection scheduling, timestamp assignment and export to its monitoring SDK. Empty vectors
/// indicate a disabled family, not a zero-event population. Returned allocations belong to
/// the caller; cache-owned recorder storage is fixed and charged at open.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CacheStatsSnapshot {
    /// Existing health, resource, activity, I/O and reclaim metrics and reset epoch.
    pub summary: CacheSnapshot,
    /// Exact collection settings for this open.
    pub options: StatsOptions,
    /// Bytes reserved for counter/histogram stripes, excluding this snapshot.
    /// Recorder controls are covered by the fixed runtime control reservation.
    pub recorder_bytes: usize,
    /// Finite operation/result series, independent of user keys and error strings.
    pub requests: Vec<RequestStatsSnapshot>,
    /// Full I/O duration distributions; empty when I/O latency is disabled.
    pub io_latency: Vec<IoLatencySnapshot>,
}
