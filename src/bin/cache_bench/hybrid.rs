//! Fixed- and mixed-size benchmark path for the complete Hybrid cache.

use super::{
    ApiArg as Api, EngineArg as Engine, IoModeArg as Mode, XorShift64, json_escape, parse_number,
    parse_seed, percentage_count, rate, validate_range,
};

use std::fmt::Write as _;
use std::mem::size_of;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache_rs::{
    AsyncHybridCache, BackpressurePolicy, BucketCacheConfig, CacheConfig, CacheError, CacheTier,
    HybridCache, HybridCacheConfig, HybridCacheStats, HybridLookupOutcome, HybridWriteMode,
    PutOptions, PutOutcome, RegionStagingStats, RejectReason, RemoveOutcome,
};

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_KEYS: usize = 100_000;
const DEFAULT_READ_PERCENT: u8 = 80;
const DEFAULT_PREFILL_PERCENT: u8 = 100;
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_QUEUE_DEPTH: usize = 128;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_WARMUP_SECS: u64 = 5;
const DEFAULT_STEADY_STATE_FILL_MAX_SECS: u64 = 60 * 60;
const STEADY_STATE_FILL_POLL_SECS: u64 = 10;
const DEFAULT_SEED: u64 = 0x243f_6a88_85a3_08d3;
const DEFAULT_GENERATOR_MEMORY_BYTES: usize = 2 * 1024 * 1024 * 1024;
const DEFAULT_VERIFY_SAMPLES: usize = 10_000;
const DEFAULT_TTL_MS: u64 = 100;
const DEFAULT_JOURNAL_CAPACITY: u64 = 16 * 1024 * 1024;
const DEFAULT_TEMPORAL_WINDOW_PERCENT: u8 = 5;
const DEFAULT_TEMPORAL_HOT_READ_PERCENT: u8 = 90;
const MAX_KEYS: usize = 100_000_000;
const MAX_CONCURRENCY: usize = 4096;
const MAX_QUEUE_DEPTH: usize = 4096;
const MAX_WORKERS: usize = 128;
const MAX_SIZE_CLASSES: usize = 16;
const MAX_OBJECT_SIZE: usize = 64 * 1024 * 1024;
const MAX_SIZE_WEIGHT: u64 = 1_000_000_000;
const MAX_CACHE_CAPACITY: u64 = 64 * 1024_u64.pow(4);
const MAX_TOOL_MEMORY_BYTES: u64 = 16 * 1024_u64.pow(4);
const MAX_VERIFY_SAMPLES: usize = 1_000_000;
const MAX_TTL_MS: u64 = 60_000;
const MAX_STATE_LOCKS: usize = 65_536;
const MAX_ARGUMENTS: usize = 128;
const MAX_ARGUMENT_BYTES: usize = 64 * 1024;
const WORKER_STACK_BYTES: usize = 512 * 1024;
const KEY_BYTES: usize = 32;
const VALUE_HEADER_BYTES: usize = 32;
const VALUE_MAGIC: [u8; 8] = *b"CRHBVAL1";
const VALUE_PATTERN_SEED: u64 = 0x7bf8_4a91_2d63_c5e0;
const HISTOGRAM_BUCKETS: usize = 512;

pub(super) fn run(arguments: impl IntoIterator<Item = String>) -> Result<(), String> {
    let options = match Options::parse(arguments)? {
        ParseOutcome::Help => {
            print_help();
            return Ok(());
        }
        ParseOutcome::Run(options) => options,
    };
    ensure_empty_targets(&options)?;
    let config = build_config(&options)?;
    let diagnostics = config
        .diagnostics()
        .map_err(|error| format!("Hybrid configuration is invalid: {error}"))?;
    eprintln!(
        "cache-bench hybrid: planned memory={} B, Bucket={} B, Region={} B",
        diagnostics.planned_memory_bytes,
        diagnostics.bucket.file_len_bytes,
        diagnostics.region.data_file_len_bytes
    );
    let required_region_reuses = if options.mix.uses_region(options.small_object_max) {
        u64::from(diagnostics.region.region_count)
    } else {
        0
    };
    let keys = Arc::new(KeySpace::new(options.keys, options.seed));
    let states = Arc::new(KeyStateTable::try_new(
        options.keys,
        options.concurrency.max(options.prefill_concurrency),
        options.generator_memory_budget,
        options.mix.maximum_bytes(),
    )?);
    let generator_planned_memory = states.planned_memory_bytes();
    let cache = Arc::new(
        config
            .clone()
            .open()
            .map_err(|error| format!("cannot format/open Hybrid cache: {error}"))?,
    );
    let cache = BenchCache::new(cache, options.api)?;
    let initial_stats = cache.stats();
    let prefill_count = percentage_count(options.keys, options.prefill_percent);
    eprintln!(
        "cache-bench hybrid: generator={} B, preloading {prefill_count}/{} objects with {} bounded workers",
        states.planned_memory_bytes(),
        options.keys,
        options.prefill_concurrency,
    );
    let prefill_started = Instant::now();
    prefill(
        &cache,
        Arc::clone(&keys),
        Arc::clone(&states),
        prefill_count,
        options.prefill_concurrency,
        &options.mix,
    )?;
    let prefill_elapsed = prefill_started.elapsed();
    cache
        .flush()
        .map_err(|error| format!("prefill flush failed: {error}"))?;
    verify_prefill(
        &cache,
        &keys,
        &states,
        prefill_count,
        options.verify_samples,
        &options.mix,
    )?;
    exercise_semantics(&cache, &keys, &states, &options)?;
    cache
        .flush()
        .map_err(|error| format!("semantic gate flush failed: {error}"))?;
    let temporal = (options.access_pattern == AccessPattern::Temporal).then(|| {
        TemporalAccess::new(
            options.keys,
            percentage_count(options.keys, options.temporal_window_percent).max(1),
            options.temporal_hot_read_percent,
            prefill_count as u64,
        )
    });
    eprintln!(
        "cache-bench hybrid: access pattern={} temporal window={} keys hot reads={}%",
        options.access_pattern.as_str(),
        temporal.as_ref().map_or(0, TemporalAccess::window_keys),
        options.temporal_hot_read_percent,
    );

    let premeasure_before = cache.stats();
    let premeasure_started = Instant::now();
    if !options.warmup.is_zero() {
        eprintln!(
            "cache-bench hybrid: warmup {:.3}s",
            options.warmup.as_secs_f64()
        );
        let warmup_phase = run_phase(
            &cache,
            PhaseWorkload {
                keys: Arc::clone(&keys),
                states: Arc::clone(&states),
                mix: options.mix.clone(),
                read_percent: options.read_percent,
                remove_percent: options.remove_percent,
                ttl_percent: options.ttl_percent,
                cross_tier_percent: options.cross_tier_percent,
                small_object_max: options.small_object_max,
                ttl_ms: options.ttl_ms,
                concurrency: options.concurrency,
                duration: options.warmup,
                seed: options.seed ^ 0xa409_3822_299f_31d0,
                temporal: temporal.clone(),
            },
        )?;
        if warmup_phase.errors != 0 || warmup_phase.stale_values != 0 || warmup_phase.rejected != 0
        {
            return Err(format!(
                "warmup failed: errors={} stale={} rejected={} first_rejection={:?} first_error={} {}",
                warmup_phase.errors,
                warmup_phase.stale_values,
                warmup_phase.rejected,
                warmup_phase.first_rejection,
                warmup_phase.first_error.as_deref().unwrap_or("none"),
                overload_diagnostics(&cache),
            ));
        }
    }

    let mut steady_state_fill_phase = Phase::default();
    if options.steady_state_fill_turnovers > 0.0 {
        eprintln!(
            "cache-bench hybrid: filling to steady state {:.3}x physical turnover and {} Region reuses (max {:.3}s)",
            options.steady_state_fill_turnovers,
            required_region_reuses,
            options.steady_state_fill_max.as_secs_f64(),
        );
        let deadline = Instant::now()
            .checked_add(options.steady_state_fill_max)
            .unwrap_or_else(Instant::now);
        let mut round = 0_u64;
        loop {
            let progress = StatsDelta::between(premeasure_before, cache.stats());
            if steady_state_gate_ready(&options, &progress, required_region_reuses) {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                return Err(format!(
                    "steady-state fill timed out after {:.3}s: physical turnover {:.3}x/{:.3}x, Region reuse={}/{} {}",
                    options.steady_state_fill_max.as_secs_f64(),
                    capacity_turnovers_for(&options, progress.host_write_bytes),
                    options.steady_state_fill_turnovers,
                    progress.region_reuses,
                    required_region_reuses,
                    overload_diagnostics(&cache),
                ));
            }
            let remaining = deadline.saturating_duration_since(now);
            let duration = remaining.min(Duration::from_secs(STEADY_STATE_FILL_POLL_SECS));
            eprintln!(
                "cache-bench hybrid: steady-state fill {:.3}x/{:.3}x Region reuse={}/{} elapsed={:.1}s",
                capacity_turnovers_for(&options, progress.host_write_bytes),
                options.steady_state_fill_turnovers,
                progress.region_reuses,
                required_region_reuses,
                steady_state_fill_phase.elapsed.as_secs_f64(),
            );
            let fill = run_phase(
                &cache,
                PhaseWorkload {
                    keys: Arc::clone(&keys),
                    states: Arc::clone(&states),
                    mix: options.mix.clone(),
                    read_percent: options.read_percent,
                    remove_percent: options.remove_percent,
                    ttl_percent: options.ttl_percent,
                    cross_tier_percent: options.cross_tier_percent,
                    small_object_max: options.small_object_max,
                    ttl_ms: options.ttl_ms,
                    concurrency: options.concurrency,
                    duration,
                    seed: options.seed ^ 0x1319_8a2e_0370_7344 ^ mix64(round),
                    temporal: temporal.clone(),
                },
            )?;
            if fill.errors != 0 || fill.stale_values != 0 || fill.rejected != 0 {
                return Err(format!(
                    "steady-state fill failed: errors={} stale={} rejected={} first_rejection={:?} first_error={} {}",
                    fill.errors,
                    fill.stale_values,
                    fill.rejected,
                    fill.first_rejection,
                    fill.first_error.as_deref().unwrap_or("none"),
                    overload_diagnostics(&cache),
                ));
            }
            steady_state_fill_phase.merge_sequential(&fill);
            round = round.saturating_add(1);
        }
    }
    cache
        .flush()
        .map_err(|error| format!("pre-measure drain/flush failed: {error}"))?;
    let before = cache.stats();
    let premeasure_stats = StatsDelta::between(premeasure_before, before);
    if !steady_state_gate_ready(&options, &premeasure_stats, required_region_reuses) {
        return Err(format!(
            "steady-state fill boundary did not satisfy its gate: physical turnover {:.3}x/{:.3}x, Region reuse={}/{} {}",
            capacity_turnovers_for(&options, premeasure_stats.host_write_bytes),
            options.steady_state_fill_turnovers,
            premeasure_stats.region_reuses,
            required_region_reuses,
            overload_diagnostics(&cache),
        ));
    }
    let premeasure_elapsed = premeasure_started.elapsed();
    eprintln!(
        "cache-bench hybrid: measuring {:.3}s concurrency={} Region/Bucket QD={}",
        options.duration.as_secs_f64(),
        options.concurrency,
        options.queue_depth
    );
    let phase = run_phase(
        &cache,
        PhaseWorkload {
            keys: Arc::clone(&keys),
            states: Arc::clone(&states),
            mix: options.mix.clone(),
            read_percent: options.read_percent,
            remove_percent: options.remove_percent,
            ttl_percent: options.ttl_percent,
            cross_tier_percent: options.cross_tier_percent,
            small_object_max: options.small_object_max,
            ttl_ms: options.ttl_ms,
            concurrency: options.concurrency,
            duration: options.duration,
            seed: options.seed,
            temporal,
        },
    )?;
    if let Some(error) = &phase.first_error {
        eprintln!("cache-bench hybrid: first measured error: {error}");
    }
    let measured_after = cache.stats();
    let drain_start = Instant::now();
    cache
        .close()
        .map_err(|error| format!("Hybrid drain/close failed: {error}"))?;
    let drain = drain_start.elapsed();
    let after = cache.stats();
    eprintln!(
        "cache-bench hybrid: reopening clean cache and verifying {} samples",
        options.verify_samples.min(options.keys)
    );
    let reopen_started = Instant::now();
    let reopened = Arc::new(
        config
            .open()
            .map_err(|error| format!("clean Hybrid reopen failed: {error}"))?,
    );
    let reopened = BenchCache::new(reopened, options.api)?;
    let reopen = reopen_started.elapsed();
    let reopen_verify_started = Instant::now();
    let reopen_verification = verify_reopen(
        &reopened,
        &keys,
        &states,
        options.verify_samples,
        &options.mix,
    )?;
    let reopen_verify = reopen_verify_started.elapsed();
    let reopen_close_started = Instant::now();
    reopened
        .close()
        .map_err(|error| format!("reopened Hybrid close failed: {error}"))?;
    let reopen_close = reopen_close_started.elapsed();
    let report = Report {
        options: &options,
        phase,
        stats: StatsDelta::between(before, measured_after),
        drain_stats: StatsDelta::between(measured_after, after),
        total_stats: StatsDelta::between(initial_stats, after),
        premeasure_stats,
        final_stats: after.hybrid,
        generator_planned_memory,
        prefill_elapsed,
        premeasure_elapsed,
        steady_state_fill_phase,
        required_region_reuses,
        drain,
        reopen,
        reopen_verify,
        reopen_close,
        reopen_verification,
    };
    match options.output {
        Output::Human => report.print_human(),
        Output::Json => println!("{}", report.to_json()),
        Output::OpenMetrics => print!("{}", report.to_openmetrics()),
    }
    let failures = report.acceptance_failures();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(format!("acceptance failed: {}", failures.join("; ")))
    }
}

fn overload_diagnostics(cache: &BenchCache) -> String {
    let stats = cache.stats().hybrid;
    format!(
        "request_rejections={} memory={}/{} write_back_failures={} lower_candidates={} skipped={} queue={}/{}/{} write_back_memory={}/{}/{} Bucket_buffer_rejections={} Region_rejected={} waits_ns(req/WB/Bbuf/Rbp)={}/{}/{}/{} io_submitted(B/R)={}/{} io_completion_ns(B/R)={}/{}",
        stats.request_rejections,
        stats.memory_charged_bytes,
        stats.memory_capacity_bytes,
        stats.write_back.demotion_failures,
        stats.write_back.lower_candidate_evictions,
        stats.write_back.proactive_skipped,
        stats.write_back.queue_in_flight,
        stats.write_back.queue_in_flight_peak,
        stats.write_back.queue_capacity,
        stats.write_back.memory_in_use_bytes,
        stats.write_back.memory_peak_bytes,
        stats.write_back.memory_capacity_bytes,
        stats.bucket.page_buffer_rejections,
        stats.region.rejected,
        stats.request_wait_ns,
        stats.write_back.queue_wait_ns,
        stats.bucket.page_buffer_wait_ns,
        stats.region.backpressure_wait_ns,
        stats.bucket.io_submitted,
        stats.region.io_submitted,
        stats.bucket.io_completion_ns,
        stats.region.io_completion_ns,
    )
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum AccessPattern {
    #[default]
    Uniform,
    Temporal,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum Backpressure {
    Reject,
    #[default]
    Block,
}

impl Backpressure {
    const fn cache(self) -> BackpressurePolicy {
        match self {
            Self::Reject => BackpressurePolicy::Reject,
            Self::Block => BackpressurePolicy::Block,
        }
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::Block => "block",
        }
    }
}

impl FromStr for Backpressure {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "reject" => Ok(Self::Reject),
            "block" => Ok(Self::Block),
            _ => Err("--backpressure must be reject or block".into()),
        }
    }
}

impl AccessPattern {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Temporal => "temporal",
        }
    }
}

impl FromStr for AccessPattern {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "uniform" => Ok(Self::Uniform),
            "temporal" => Ok(Self::Temporal),
            _ => Err("--access-pattern must be uniform or temporal".into()),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Output {
    Human,
    Json,
    OpenMetrics,
}

impl FromStr for Output {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "human" => Ok(Self::Human),
            "json" => Ok(Self::Json),
            "openmetrics" => Ok(Self::OpenMetrics),
            _ => Err("--output must be human, json, or openmetrics".into()),
        }
    }
}

#[derive(Clone, Debug)]
struct SizeClass {
    bytes: usize,
    cumulative_weight: u64,
}

#[derive(Clone, Debug)]
struct SizeMix {
    classes: Vec<SizeClass>,
    total_weight: u64,
}

impl SizeMix {
    fn parse(value: &str) -> Result<Self, String> {
        let mut classes = Vec::new();
        let mut total = 0_u64;
        for item in value.split(',') {
            let (size, weight) = item
                .split_once(':')
                .ok_or_else(|| format!("invalid size class {item:?}; expected SIZE:WEIGHT"))?;
            let bytes = parse_usize_bytes(size, "--sizes")?;
            if !(VALUE_HEADER_BYTES..=MAX_OBJECT_SIZE).contains(&bytes) {
                return Err(format!(
                    "each --sizes object must be in {VALUE_HEADER_BYTES}..={MAX_OBJECT_SIZE} bytes so values carry the key/version integrity header"
                ));
            }
            let weight = parse_number::<u64>(weight, "--sizes weight")?;
            if weight == 0 || weight > MAX_SIZE_WEIGHT {
                return Err(format!("--sizes weights must be in 1..={MAX_SIZE_WEIGHT}"));
            }
            total = total
                .checked_add(weight)
                .ok_or_else(|| "--sizes total weight is too large".to_owned())?;
            classes.push(SizeClass {
                bytes,
                cumulative_weight: total,
            });
            if classes.len() > MAX_SIZE_CLASSES {
                return Err(format!(
                    "--sizes accepts at most {MAX_SIZE_CLASSES} classes"
                ));
            }
        }
        if classes.is_empty() {
            return Err("--sizes requires at least one class".into());
        }
        Ok(Self {
            classes,
            total_weight: total,
        })
    }

    fn class_for_key(&self, key_index: usize) -> usize {
        self.class_for_sample(mix64(key_index as u64))
    }

    fn class_for_version(&self, key_index: usize, version: u32) -> usize {
        self.class_for_sample(mix64((key_index as u64) ^ (u64::from(version) << 32)))
    }

    fn class_for_sample(&self, sample: u64) -> usize {
        let sample = sample % self.total_weight;
        self.classes
            .iter()
            .position(|class| sample < class.cumulative_weight)
            .unwrap_or(self.classes.len() - 1)
    }

    fn routing_classes(&self, small_object_max: usize) -> Option<(usize, usize)> {
        let small = self
            .classes
            .iter()
            .position(|class| KEY_BYTES.saturating_add(class.bytes) <= small_object_max)?;
        let large = self
            .classes
            .iter()
            .position(|class| KEY_BYTES.saturating_add(class.bytes) > small_object_max)?;
        Some((small, large))
    }

    fn uses_bucket(&self, small_object_max: usize) -> bool {
        self.classes
            .iter()
            .any(|class| KEY_BYTES.saturating_add(class.bytes) <= small_object_max)
    }

    fn uses_region(&self, small_object_max: usize) -> bool {
        self.classes
            .iter()
            .any(|class| KEY_BYTES.saturating_add(class.bytes) > small_object_max)
    }

    fn maximum_bytes(&self) -> usize {
        self.classes
            .iter()
            .map(|class| class.bytes)
            .max()
            .unwrap_or(1)
    }

    fn as_spec(&self) -> String {
        let mut previous = 0_u64;
        self.classes
            .iter()
            .map(|class| {
                let weight = class.cumulative_weight - previous;
                previous = class.cumulative_weight;
                format!("{}:{}", class.bytes, weight)
            })
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Clone, Debug)]
struct Options {
    bucket_path: PathBuf,
    region_path: PathBuf,
    manifest_path: PathBuf,
    bucket_capacity: u64,
    region_capacity: u64,
    memory_capacity: usize,
    bucket_size: usize,
    region_size: u64,
    bucket_memory_budget: usize,
    region_memory_budget: usize,
    aggregate_memory_budget: Option<usize>,
    small_object_max: usize,
    mix: SizeMix,
    keys: usize,
    read_percent: u8,
    access_pattern: AccessPattern,
    temporal_window_percent: u8,
    temporal_hot_read_percent: u8,
    prefill_percent: u8,
    prefill_concurrency: usize,
    verify_samples: usize,
    concurrency: usize,
    queue_depth: usize,
    backpressure: Backpressure,
    append_lanes: usize,
    write_mode: HybridWriteMode,
    write_back_queue_depth: usize,
    write_back_workers: usize,
    write_back_memory: usize,
    generator_memory_budget: usize,
    journal_capacity: u64,
    remove_percent: u8,
    ttl_percent: u8,
    cross_tier_percent: u8,
    ttl_ms: u64,
    api: Api,
    engine: Engine,
    mode: Mode,
    warmup: Duration,
    duration: Duration,
    seed: u64,
    output: Output,
    min_ops_per_sec: Option<f64>,
    max_p99_us: Option<f64>,
    min_hit_percent: Option<f64>,
    min_journal_rollovers: u64,
    steady_state_fill_turnovers: f64,
    steady_state_fill_max: Duration,
    min_capacity_turnovers: f64,
    min_logical_keyspace_turnovers: f64,
    min_disk_qd_peak: u64,
    min_write_back_qd_peak: u64,
    max_journal_rollover_ms: Option<f64>,
    max_close_ms: Option<f64>,
}

enum ParseOutcome {
    Help,
    Run(Box<Options>),
}

impl Options {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<ParseOutcome, String> {
        let mut bucket_path = None;
        let mut region_path = None;
        let mut manifest_path = None;
        let mut bucket_capacity = None;
        let mut region_capacity = None;
        let mut memory_capacity = None;
        let mut bucket_size = 4096;
        let mut region_size = DEFAULT_REGION_SIZE;
        let mut bucket_memory_budget = 1024 * 1024 * 1024;
        let mut region_memory_budget = 1024 * 1024 * 1024;
        let mut aggregate_memory_budget = None;
        let mut small_object_max = 1024;
        let mut mix = SizeMix::parse("256:50,4KiB:30,64KiB:20")?;
        let mut keys = DEFAULT_KEYS;
        let mut read_percent = DEFAULT_READ_PERCENT;
        let mut access_pattern = AccessPattern::Uniform;
        let mut temporal_window_percent = DEFAULT_TEMPORAL_WINDOW_PERCENT;
        let mut temporal_hot_read_percent = DEFAULT_TEMPORAL_HOT_READ_PERCENT;
        let mut prefill_percent = DEFAULT_PREFILL_PERCENT;
        let mut prefill_concurrency = None;
        let mut verify_samples = DEFAULT_VERIFY_SAMPLES;
        let mut concurrency = DEFAULT_CONCURRENCY;
        let mut queue_depth = DEFAULT_QUEUE_DEPTH;
        let mut backpressure = Backpressure::Block;
        let mut append_lanes = 2;
        let mut write_mode = HybridWriteMode::WriteBack;
        let mut write_back_queue_depth = 64;
        let mut write_back_workers = 4;
        let mut write_back_memory = 32 * 1024 * 1024;
        let mut generator_memory_budget = DEFAULT_GENERATOR_MEMORY_BYTES;
        let mut journal_capacity = DEFAULT_JOURNAL_CAPACITY;
        let mut remove_percent = 10;
        let mut ttl_percent = 5;
        let mut cross_tier_percent = 20;
        let mut ttl_ms = DEFAULT_TTL_MS;
        let mut api = Api::Async;
        let mut engine = Engine::Auto;
        let mut mode = Mode::Buffered;
        let mut warmup = Duration::from_secs(DEFAULT_WARMUP_SECS);
        let mut duration = Duration::from_secs(DEFAULT_DURATION_SECS);
        let mut seed = DEFAULT_SEED;
        let mut output = Output::Json;
        let mut min_ops_per_sec = None;
        let mut max_p99_us = None;
        let mut min_hit_percent = None;
        let mut min_journal_rollovers = 0;
        let mut steady_state_fill_turnovers = 0.0;
        let mut steady_state_fill_max = Duration::from_secs(DEFAULT_STEADY_STATE_FILL_MAX_SECS);
        let mut min_capacity_turnovers = 0.0;
        let mut min_logical_keyspace_turnovers = 0.0;
        let mut min_disk_qd_peak = 1;
        let mut min_write_back_qd_peak = 1;
        let mut max_journal_rollover_ms = None;
        let mut max_close_ms = None;
        let mut confirmed = false;
        let mut bounded_arguments = Vec::new();
        let mut argument_bytes = 0_usize;
        for argument in arguments {
            if bounded_arguments.len() == MAX_ARGUMENTS {
                return Err(format!(
                    "Hybrid benchmark accepts at most {MAX_ARGUMENTS} arguments"
                ));
            }
            argument_bytes = argument_bytes
                .checked_add(argument.len())
                .ok_or_else(|| "Hybrid benchmark argument bytes overflow".to_owned())?;
            if argument_bytes > MAX_ARGUMENT_BYTES {
                return Err(format!(
                    "Hybrid benchmark accepts at most {MAX_ARGUMENT_BYTES} argument bytes"
                ));
            }
            bounded_arguments.push(argument);
        }
        let arguments = bounded_arguments;
        let mut index = 0;
        while index < arguments.len() {
            let argument = &arguments[index];
            if matches!(argument.as_str(), "--help" | "-h") {
                return Ok(ParseOutcome::Help);
            }
            let (name, inline) = argument
                .split_once('=')
                .map_or((argument.as_str(), None), |(name, value)| {
                    (name, Some(value))
                });
            if name == "--yes" {
                if inline.is_some() {
                    return Err("--yes does not take a value".into());
                }
                confirmed = true;
                index += 1;
                continue;
            }
            let value = match inline {
                Some(value) => value,
                None => {
                    index += 1;
                    arguments
                        .get(index)
                        .filter(|value| !value.starts_with('-'))
                        .map(String::as_str)
                        .ok_or_else(|| format!("missing value for {name}"))?
                }
            };
            match name {
                "--bucket-path" => bucket_path = Some(PathBuf::from(value)),
                "--region-path" => region_path = Some(PathBuf::from(value)),
                "--manifest-path" => manifest_path = Some(PathBuf::from(value)),
                "--bucket-capacity" => bucket_capacity = Some(parse_bytes(value, name)?),
                "--region-capacity" => region_capacity = Some(parse_bytes(value, name)?),
                "--memory-capacity" => memory_capacity = Some(parse_usize_bytes(value, name)?),
                "--bucket-size" => bucket_size = parse_usize_bytes(value, name)?,
                "--region-size" => region_size = parse_bytes(value, name)?,
                "--bucket-memory-budget" => bucket_memory_budget = parse_usize_bytes(value, name)?,
                "--region-memory-budget" => region_memory_budget = parse_usize_bytes(value, name)?,
                "--hybrid-memory-budget" => {
                    aggregate_memory_budget = Some(parse_usize_bytes(value, name)?)
                }
                "--small-object-max" => small_object_max = parse_usize_bytes(value, name)?,
                "--sizes" => mix = SizeMix::parse(value)?,
                "--keys" => keys = parse_number(value, name)?,
                "--read-percent" => read_percent = parse_number(value, name)?,
                "--access-pattern" => access_pattern = value.parse()?,
                "--temporal-window-percent" => temporal_window_percent = parse_number(value, name)?,
                "--temporal-hot-read-percent" => {
                    temporal_hot_read_percent = parse_number(value, name)?
                }
                "--prefill-percent" => prefill_percent = parse_number(value, name)?,
                "--prefill-concurrency" => prefill_concurrency = Some(parse_number(value, name)?),
                "--verify-samples" => verify_samples = parse_number(value, name)?,
                "--concurrency" => concurrency = parse_number(value, name)?,
                "--queue-depth" => queue_depth = parse_number(value, name)?,
                "--backpressure" => backpressure = value.parse()?,
                "--append-lanes" => append_lanes = parse_number(value, name)?,
                "--write-mode" => {
                    write_mode = match value {
                        "write-through" | "write_through" | "through" => {
                            HybridWriteMode::WriteThrough
                        }
                        "write-back" | "write_back" | "back" => HybridWriteMode::WriteBack,
                        _ => return Err("--write-mode must be write-through or write-back".into()),
                    }
                }
                "--write-back-queue-depth" => write_back_queue_depth = parse_number(value, name)?,
                "--write-back-workers" => write_back_workers = parse_number(value, name)?,
                "--write-back-memory" => write_back_memory = parse_usize_bytes(value, name)?,
                "--generator-memory-budget" => {
                    generator_memory_budget = parse_usize_bytes(value, name)?
                }
                "--journal-capacity" => journal_capacity = parse_bytes(value, name)?,
                "--remove-percent" => remove_percent = parse_number(value, name)?,
                "--ttl-percent" => ttl_percent = parse_number(value, name)?,
                "--cross-tier-percent" => cross_tier_percent = parse_number(value, name)?,
                "--ttl-ms" => ttl_ms = parse_number(value, name)?,
                "--api" => api = value.parse()?,
                "--engine" => engine = value.parse()?,
                "--mode" => mode = value.parse()?,
                "--warmup-secs" => warmup = parse_duration(value, name, true)?,
                "--duration-secs" => duration = parse_duration(value, name, false)?,
                "--seed" => seed = parse_seed(value)?,
                "--output" => output = value.parse()?,
                "--min-ops-per-sec" => min_ops_per_sec = Some(parse_positive(value, name)?),
                "--max-p99-us" => max_p99_us = Some(parse_positive(value, name)?),
                "--min-hit-percent" => min_hit_percent = Some(parse_percent(value, name)?),
                "--min-journal-rollovers" => min_journal_rollovers = parse_number(value, name)?,
                "--steady-state-fill-turnovers" => {
                    steady_state_fill_turnovers = parse_non_negative(value, name)?
                }
                "--steady-state-fill-max-secs" => {
                    steady_state_fill_max = parse_duration(value, name, false)?
                }
                "--min-capacity-turnovers" => {
                    min_capacity_turnovers = parse_non_negative(value, name)?
                }
                "--min-logical-keyspace-turnovers" => {
                    min_logical_keyspace_turnovers = parse_non_negative(value, name)?
                }
                "--min-disk-qd-peak" => min_disk_qd_peak = parse_number(value, name)?,
                "--min-write-back-qd-peak" => min_write_back_qd_peak = parse_number(value, name)?,
                "--max-journal-rollover-ms" => {
                    max_journal_rollover_ms = Some(parse_positive(value, name)?)
                }
                "--max-close-ms" => max_close_ms = Some(parse_positive(value, name)?),
                _ => return Err(format!("unknown Hybrid benchmark option {name}")),
            }
            index += 1;
        }
        if !confirmed {
            return Err("Hybrid benchmark requires --yes and three empty dedicated paths".into());
        }
        let bucket_path = required(bucket_path, "--bucket-path")?;
        let region_path = required(region_path, "--region-path")?;
        let manifest_path = required(manifest_path, "--manifest-path")?;
        if bucket_path == region_path
            || bucket_path == manifest_path
            || region_path == manifest_path
        {
            return Err("Bucket, Region, and manifest paths must be distinct".into());
        }
        let bucket_capacity = required(bucket_capacity, "--bucket-capacity")?;
        let region_capacity = required(region_capacity, "--region-capacity")?;
        let memory_capacity = required(memory_capacity, "--memory-capacity")?;
        validate_range("--bucket-capacity", bucket_capacity, 1, MAX_CACHE_CAPACITY)?;
        validate_range("--region-capacity", region_capacity, 1, MAX_CACHE_CAPACITY)?;
        validate_memory("--memory-capacity", memory_capacity)?;
        validate_memory("--bucket-memory-budget", bucket_memory_budget)?;
        validate_memory("--region-memory-budget", region_memory_budget)?;
        if let Some(bytes) = aggregate_memory_budget {
            validate_memory("--hybrid-memory-budget", bytes)?;
        }
        validate_range("--keys", keys, 1, MAX_KEYS)?;
        validate_range("--read-percent", read_percent, 0, 100)?;
        validate_range("--temporal-window-percent", temporal_window_percent, 1, 100)?;
        validate_range(
            "--temporal-hot-read-percent",
            temporal_hot_read_percent,
            0,
            100,
        )?;
        if access_pattern == AccessPattern::Uniform && min_logical_keyspace_turnovers > 0.0 {
            return Err(
                "--min-logical-keyspace-turnovers requires --access-pattern temporal".into(),
            );
        }
        validate_range("--prefill-percent", prefill_percent, 0, 100)?;
        let prefill_concurrency = prefill_concurrency.unwrap_or(concurrency.min(MAX_WORKERS));
        validate_range("--prefill-concurrency", prefill_concurrency, 1, MAX_WORKERS)?;
        validate_range("--verify-samples", verify_samples, 1, MAX_VERIFY_SAMPLES)?;
        validate_range("--concurrency", concurrency, 1, MAX_CONCURRENCY)?;
        if concurrency > keys {
            return Err("--concurrency must not exceed --keys".into());
        }
        validate_range("--queue-depth", queue_depth, 1, MAX_QUEUE_DEPTH)?;
        validate_range("--append-lanes", append_lanes, 1, 8)?;
        validate_range(
            "--write-back-queue-depth",
            write_back_queue_depth,
            1,
            MAX_QUEUE_DEPTH,
        )?;
        validate_range("--write-back-workers", write_back_workers, 1, MAX_WORKERS)?;
        if write_back_workers > write_back_queue_depth {
            return Err("--write-back-workers must not exceed --write-back-queue-depth".into());
        }
        validate_memory("--write-back-memory", write_back_memory)?;
        validate_memory("--generator-memory-budget", generator_memory_budget)?;
        if !(64 * 1024..=MAX_CACHE_CAPACITY).contains(&journal_capacity)
            || journal_capacity % 4096 != 0
        {
            return Err("--journal-capacity must be a 4096-byte multiple in 64KiB..=64TiB".into());
        }
        validate_range("--remove-percent", remove_percent, 0, 100)?;
        validate_range("--ttl-percent", ttl_percent, 0, 100)?;
        validate_range("--cross-tier-percent", cross_tier_percent, 0, 100)?;
        if u16::from(remove_percent) + u16::from(ttl_percent) + u16::from(cross_tier_percent) > 100
        {
            return Err(
                "--remove-percent + --ttl-percent + --cross-tier-percent must not exceed 100"
                    .into(),
            );
        }
        validate_range("--ttl-ms", ttl_ms, 1, MAX_TTL_MS)?;
        validate_range(
            "--min-disk-qd-peak",
            min_disk_qd_peak,
            1,
            MAX_QUEUE_DEPTH as u64,
        )?;
        validate_range(
            "--min-write-back-qd-peak",
            min_write_back_qd_peak,
            1,
            MAX_QUEUE_DEPTH as u64,
        )?;
        if small_object_max == 0 || small_object_max > MAX_OBJECT_SIZE {
            return Err(format!(
                "--small-object-max must be in 1..={MAX_OBJECT_SIZE}"
            ));
        }
        if cross_tier_percent != 0 && mix.routing_classes(small_object_max).is_none() {
            return Err(
                "--cross-tier-percent requires --sizes to include at least one Bucket-routed and one Region-routed value class after accounting for the generated key; use --cross-tier-percent=0 for fixed-tier workloads"
                    .into(),
            );
        }
        let generator_memory = generator_memory_plan(
            keys,
            concurrency.max(prefill_concurrency),
            mix.maximum_bytes(),
        )?;
        if generator_memory > generator_memory_budget {
            return Err(format!(
                "benchmark generator needs {generator_memory} bytes, exceeding --generator-memory-budget {generator_memory_budget}"
            ));
        }
        Ok(ParseOutcome::Run(Box::new(Self {
            bucket_path,
            region_path,
            manifest_path,
            bucket_capacity,
            region_capacity,
            memory_capacity,
            bucket_size,
            region_size,
            bucket_memory_budget,
            region_memory_budget,
            aggregate_memory_budget,
            small_object_max,
            mix,
            keys,
            read_percent,
            access_pattern,
            temporal_window_percent,
            temporal_hot_read_percent,
            prefill_percent,
            prefill_concurrency,
            verify_samples,
            concurrency,
            queue_depth,
            backpressure,
            append_lanes,
            write_mode,
            write_back_queue_depth,
            write_back_workers,
            write_back_memory,
            generator_memory_budget,
            journal_capacity,
            remove_percent,
            ttl_percent,
            cross_tier_percent,
            ttl_ms,
            api,
            engine,
            mode,
            warmup,
            duration,
            seed,
            output,
            min_ops_per_sec,
            max_p99_us,
            min_hit_percent,
            min_journal_rollovers,
            steady_state_fill_turnovers,
            steady_state_fill_max,
            min_capacity_turnovers,
            min_logical_keyspace_turnovers,
            min_disk_qd_peak,
            min_write_back_qd_peak,
            max_journal_rollover_ms,
            max_close_ms,
        })))
    }
}

fn build_config(options: &Options) -> Result<HybridCacheConfig, String> {
    let maximum_value = options
        .mix
        .maximum_bytes()
        .checked_add(256)
        .ok_or_else(|| "maximum Hybrid value size overflow".to_owned())?;
    let index_slots = if options.mix.uses_region(options.small_object_max) {
        default_index_slots(options.keys)?
    } else {
        default_index_slots(1)?
    };
    let bucket = BucketCacheConfig::new(&options.bucket_path, options.bucket_capacity)
        .with_bucket_size(options.bucket_size)
        .with_memory_budget(options.bucket_memory_budget)
        .with_buffer_slots(options.concurrency.min(128))
        .with_io_engine(options.engine.cache())
        .with_io_mode(options.mode.cache())
        .with_io_queue_depth(options.queue_depth);
    let region = CacheConfig::new(&options.region_path, options.region_capacity)
        .with_region_size(options.region_size)
        .with_index_slots(index_slots)
        .with_max_key_size(64)
        .with_max_value_size(maximum_value)
        .with_append_lanes(options.append_lanes)
        .with_memory_budget(options.region_memory_budget)
        .with_submission_queue_depths(options.queue_depth, options.queue_depth)
        .with_backpressure(options.backpressure.cache())
        .with_io_queue_depth(options.queue_depth)
        .with_io_engine(options.engine.cache())
        .with_io_mode(options.mode.cache());
    let request_memory = options
        .mix
        .maximum_bytes()
        .checked_add(512)
        .and_then(|bytes| bytes.checked_mul(options.concurrency))
        .map(|bytes| bytes.max(64 * 1024 * 1024))
        .ok_or_else(|| "Hybrid request-memory plan overflow".to_owned())?;
    let mut hybrid = HybridCacheConfig::new(options.memory_capacity, bucket, region)
        .with_manifest_path(&options.manifest_path)
        .with_journal_capacity(options.journal_capacity)
        .with_small_object_max(options.small_object_max)
        .with_request_slots(options.concurrency.min(MAX_QUEUE_DEPTH))
        .with_request_memory(request_memory)
        .with_async_queue_depths(options.queue_depth, options.queue_depth)
        .with_async_workers(options.concurrency.min(128), options.concurrency.min(128))
        .with_backpressure(options.backpressure.cache())
        .with_write_mode(options.write_mode)
        .with_write_back_resources(
            options.write_back_queue_depth,
            options.write_back_workers,
            options.write_back_memory,
        );
    if let Some(bytes) = options.aggregate_memory_budget {
        hybrid = hybrid.with_memory_budget(bytes);
    }
    Ok(hybrid)
}

fn ensure_empty_targets(options: &Options) -> Result<(), String> {
    for (name, path) in [
        ("Bucket", &options.bucket_path),
        ("Region", &options.region_path),
        ("manifest", &options.manifest_path),
    ] {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(format!("{name} path {} is a symlink", path.display()));
            }
            Ok(metadata) if !metadata.is_file() => {
                return Err(format!(
                    "{name} path {} is not a regular file",
                    path.display()
                ));
            }
            Ok(metadata) if metadata.len() != 0 => {
                return Err(format!(
                    "Hybrid benchmark refuses non-empty {name} path {}; use three dedicated empty paths",
                    path.display()
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(format!("cannot inspect {}: {error}", path.display())),
        }
    }
    Ok(())
}

#[derive(Clone)]
enum BenchCache {
    Sync(Arc<HybridCache>),
    Async {
        cache: Arc<HybridCache>,
        facade: AsyncHybridCache,
    },
}

#[derive(Clone, Copy, Default)]
struct ObservationStats {
    hybrid: HybridCacheStats,
    region_staging: RegionStagingStats,
}

impl BenchCache {
    fn new(cache: Arc<HybridCache>, api: Api) -> Result<Self, String> {
        match api {
            Api::Sync => Ok(Self::Sync(cache)),
            Api::Async => {
                let facade = cache
                    .async_handle()
                    .map_err(|error| format!("cannot create Hybrid async handle: {error}"))?;
                Ok(Self::Async { cache, facade })
            }
        }
    }

    fn cache(&self) -> &HybridCache {
        match self {
            Self::Sync(cache) | Self::Async { cache, .. } => cache,
        }
    }

    fn stats(&self) -> ObservationStats {
        let cache = self.cache();
        ObservationStats {
            hybrid: cache.stats(),
            region_staging: cache.region_staging_stats(),
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
        match self {
            Self::Sync(cache) => cache.get(key),
            Self::Async { facade, .. } => facade.get(key).wait(),
        }
    }

    fn lookup(&self, key: &[u8]) -> Result<HybridLookupOutcome, CacheError> {
        match self {
            Self::Sync(cache) => cache.lookup(key),
            Self::Async { facade, .. } => facade.lookup(key).wait(),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<PutOutcome, CacheError> {
        self.put_with_options(key, value, PutOptions::default())
    }

    fn put_with_options(
        &self,
        key: &[u8],
        value: &[u8],
        options: PutOptions,
    ) -> Result<PutOutcome, CacheError> {
        match self {
            Self::Sync(cache) => cache.put(key, value, options),
            Self::Async { facade, .. } => facade.put(key, value, options).wait(),
        }
    }

    fn remove(&self, key: &[u8]) -> Result<RemoveOutcome, CacheError> {
        match self {
            Self::Sync(cache) => cache.remove(key),
            Self::Async { facade, .. } => facade.remove(key).wait(),
        }
    }

    fn flush(&self) -> Result<(), CacheError> {
        match self {
            Self::Sync(cache) => cache.flush(),
            Self::Async { facade, .. } => facade.flush().wait(),
        }
    }

    fn close(&self) -> Result<(), CacheError> {
        match self {
            Self::Sync(cache) => cache.close(),
            Self::Async { facade, .. } => facade.close().wait(),
        }
    }
}

struct KeySpace {
    count: usize,
    seed: u64,
}

impl KeySpace {
    const fn new(count: usize, seed: u64) -> Self {
        Self { count, seed }
    }

    fn key(&self, index: usize) -> [u8; KEY_BYTES] {
        debug_assert!(index < self.count);
        let mut key = [0_u8; KEY_BYTES];
        key[..8].copy_from_slice(b"CRHBKEY1");
        key[8..16].copy_from_slice(&(index as u64).to_le_bytes());
        key[16..24].copy_from_slice(&mix64((index as u64) ^ self.seed).to_le_bytes());
        key[24..32]
            .copy_from_slice(&mix64((index as u64).rotate_left(17) ^ !self.seed).to_le_bytes());
        key
    }
}

const STATE_PRESENT: u64 = 1_u64 << 63;
const STATE_VERSION_MASK: u64 = u32::MAX as u64;
const STATE_CLASS_SHIFT: u32 = 32;
const STATE_CLASS_MASK: u64 = 0x0f;
const STATE_EXPIRY_SHIFT: u32 = 36;
const STATE_EXPIRY_MASK: u64 = (1_u64 << 27) - 1;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct ExpectedState {
    present: bool,
    version: u32,
    class: usize,
    expiry_delta_ms: u32,
}

impl ExpectedState {
    fn decode(encoded: u64) -> Self {
        Self {
            present: encoded & STATE_PRESENT != 0,
            version: (encoded & STATE_VERSION_MASK) as u32,
            class: ((encoded >> STATE_CLASS_SHIFT) & STATE_CLASS_MASK) as usize,
            expiry_delta_ms: ((encoded >> STATE_EXPIRY_SHIFT) & STATE_EXPIRY_MASK) as u32,
        }
    }

    fn encode(self) -> u64 {
        u64::from(self.version)
            | ((self.class as u64 & STATE_CLASS_MASK) << STATE_CLASS_SHIFT)
            | ((u64::from(self.expiry_delta_ms) & STATE_EXPIRY_MASK) << STATE_EXPIRY_SHIFT)
            | if self.present { STATE_PRESENT } else { 0 }
    }

    fn next_version(self) -> Result<u32, String> {
        self.version
            .checked_add(1)
            .ok_or_else(|| "per-key benchmark version exhausted u32".to_owned())
    }

    fn without_value(self) -> Self {
        Self {
            present: false,
            expiry_delta_ms: 0,
            ..self
        }
    }
}

struct KeyStateTable {
    states: Box<[AtomicU64]>,
    locks: Box<[Mutex<()>]>,
    base_unix_ms: u64,
    planned_memory_bytes: usize,
}

impl KeyStateTable {
    fn try_new(
        keys: usize,
        concurrency: usize,
        memory_budget: usize,
        maximum_value_bytes: usize,
    ) -> Result<Self, String> {
        let planned_memory_bytes = generator_memory_plan(keys, concurrency, maximum_value_bytes)?;
        if planned_memory_bytes > memory_budget {
            return Err(format!(
                "benchmark generator needs {planned_memory_bytes} bytes, exceeding budget {memory_budget}"
            ));
        }
        let mut states = Vec::new();
        states
            .try_reserve_exact(keys)
            .map_err(|_| "cannot allocate bounded per-key version table".to_owned())?;
        states.resize_with(keys, || AtomicU64::new(0));
        let lock_count = state_lock_count(concurrency);
        let mut locks = Vec::new();
        locks
            .try_reserve_exact(lock_count)
            .map_err(|_| "cannot allocate bounded key ordering locks".to_owned())?;
        locks.resize_with(lock_count, || Mutex::new(()));
        Ok(Self {
            states: states.into_boxed_slice(),
            locks: locks.into_boxed_slice(),
            base_unix_ms: now_unix_ms(),
            planned_memory_bytes,
        })
    }

    const fn planned_memory_bytes(&self) -> usize {
        self.planned_memory_bytes
    }

    fn lock(&self, index: usize) -> MutexGuard<'_, ()> {
        lock_mutex(&self.locks[index & (self.locks.len() - 1)])
    }

    fn load(&self, index: usize) -> ExpectedState {
        ExpectedState::decode(self.states[index].load(Ordering::Acquire))
    }

    fn store(&self, index: usize, state: ExpectedState) {
        self.states[index].store(state.encode(), Ordering::Release);
    }

    fn expiry_delta(&self, expires_at_unix_ms: u64) -> Result<u32, String> {
        let delta = expires_at_unix_ms
            .saturating_sub(self.base_unix_ms)
            .saturating_add(1);
        if delta > STATE_EXPIRY_MASK {
            return Err("TTL deadline exceeds packed generator state range".into());
        }
        Ok(delta as u32)
    }

    fn normalize_expiry(&self, index: usize, mut state: ExpectedState) -> ExpectedState {
        if state.present
            && state.expiry_delta_ms != 0
            && now_unix_ms()
                .saturating_sub(self.base_unix_ms)
                .saturating_add(1)
                >= u64::from(state.expiry_delta_ms)
        {
            state = state.without_value();
            self.store(index, state);
        }
        state
    }
}

fn state_lock_count(concurrency: usize) -> usize {
    concurrency
        .saturating_mul(16)
        .clamp(1, MAX_STATE_LOCKS)
        .next_power_of_two()
        .min(MAX_STATE_LOCKS)
}

fn generator_memory_plan(
    keys: usize,
    concurrency: usize,
    maximum_value_bytes: usize,
) -> Result<usize, String> {
    keys.checked_mul(size_of::<AtomicU64>())
        .and_then(|bytes| {
            state_lock_count(concurrency)
                .checked_mul(size_of::<Mutex<()>>())
                .and_then(|locks| bytes.checked_add(locks))
        })
        .and_then(|bytes| {
            maximum_value_bytes
                .checked_add(WORKER_STACK_BYTES)
                .and_then(|per_worker| per_worker.checked_add(size_of::<Phase>()))
                .and_then(|per_worker| per_worker.checked_mul(concurrency))
                .and_then(|workers| bytes.checked_add(workers))
        })
        .and_then(|bytes| bytes.checked_add(1024 * 1024))
        .ok_or_else(|| "benchmark generator memory plan overflow".to_owned())
}

fn fill_value(
    output: &mut Vec<u8>,
    mix: &SizeMix,
    class: usize,
    key_index: usize,
    version: u32,
) -> Result<usize, String> {
    let length = mix.classes[class].bytes;
    prepare_value_buffer(output, length)?;
    output[..8].copy_from_slice(&VALUE_MAGIC);
    output[8..16].copy_from_slice(&(key_index as u64).to_le_bytes());
    output[16..20].copy_from_slice(&version.to_le_bytes());
    output[20..22].copy_from_slice(&(class as u16).to_le_bytes());
    output[22..24].copy_from_slice(&(VALUE_HEADER_BYTES as u16).to_le_bytes());
    output[24..28].copy_from_slice(&(length as u32).to_le_bytes());
    output[28..32].copy_from_slice(&(VALUE_PATTERN_SEED as u32).to_le_bytes());
    Ok(length)
}

fn prepare_value_buffer(output: &mut Vec<u8>, length: usize) -> Result<(), String> {
    if output.len() >= length {
        return Ok(());
    }
    if output.capacity() < length {
        output
            .try_reserve_exact(length - output.len())
            .map_err(|_| format!("cannot allocate {length} byte benchmark scratch value"))?;
    }
    output.resize(length, 0);
    for (block_index, block) in output[VALUE_HEADER_BYTES..].chunks_mut(8).enumerate() {
        let pattern = mix64(VALUE_PATTERN_SEED ^ block_index as u64).to_le_bytes();
        block.copy_from_slice(&pattern[..block.len()]);
    }
    Ok(())
}

fn validate_value(
    value: &[u8],
    mix: &SizeMix,
    class: usize,
    key_index: usize,
    version: u32,
) -> bool {
    let expected_len = mix.classes[class].bytes;
    if value.len() != expected_len
        || value.get(..8) != Some(VALUE_MAGIC.as_slice())
        || value.get(8..16) != Some((key_index as u64).to_le_bytes().as_slice())
        || value.get(16..20) != Some(version.to_le_bytes().as_slice())
        || value.get(20..22) != Some((class as u16).to_le_bytes().as_slice())
        || value.get(22..24) != Some((VALUE_HEADER_BYTES as u16).to_le_bytes().as_slice())
        || value.get(24..28) != Some((expected_len as u32).to_le_bytes().as_slice())
    {
        return false;
    }
    if value.get(28..32) != Some((VALUE_PATTERN_SEED as u32).to_le_bytes().as_slice()) {
        return false;
    }
    let payload_len = value.len() - VALUE_HEADER_BYTES;
    if payload_len == 0 {
        return true;
    }
    [0, payload_len / 2, payload_len - 1]
        .into_iter()
        .all(|offset| {
            value[VALUE_HEADER_BYTES + offset] == value_pattern_byte(VALUE_PATTERN_SEED, offset)
        })
}

fn value_pattern_byte(seed: u64, offset: usize) -> u8 {
    mix64(seed ^ (offset / 8) as u64).to_le_bytes()[offset % 8]
}

fn prefill(
    cache: &BenchCache,
    keys: Arc<KeySpace>,
    states: Arc<KeyStateTable>,
    count: usize,
    concurrency: usize,
    mix: &SizeMix,
) -> Result<(), String> {
    let next = Arc::new(AtomicUsize::new(0));
    let abort = Arc::new(AtomicBool::new(false));
    let error = Arc::new(Mutex::new(None));
    let mut workers: Vec<thread::JoinHandle<()>> = Vec::new();
    workers
        .try_reserve_exact(concurrency)
        .map_err(|_| "cannot allocate bounded prefill worker table".to_owned())?;
    for worker_id in 0..concurrency {
        let worker_cache = cache.clone();
        let worker_keys = Arc::clone(&keys);
        let worker_states = Arc::clone(&states);
        let worker_next = Arc::clone(&next);
        let worker_abort = Arc::clone(&abort);
        let worker_error = Arc::clone(&error);
        let worker_mix = mix.clone();
        let worker = thread::Builder::new()
            .name(format!("hybrid-prefill-{worker_id}"))
            .stack_size(WORKER_STACK_BYTES)
            .spawn(move || {
                let mut value = Vec::new();
                while !worker_abort.load(Ordering::Acquire) {
                    let start = worker_next.fetch_add(64, Ordering::Relaxed);
                    if start >= count {
                        break;
                    }
                    for index in start..start.saturating_add(64).min(count) {
                        let class = worker_mix.class_for_key(index);
                        let length = match fill_value(&mut value, &worker_mix, class, index, 1) {
                            Ok(length) => length,
                            Err(message) => {
                                *lock_mutex(&worker_error) = Some(message);
                                worker_abort.store(true, Ordering::Release);
                                return;
                            }
                        };
                        let key = worker_keys.key(index);
                        match worker_cache.put(&key, &value[..length]) {
                            Ok(PutOutcome::Stored) => worker_states.store(
                                index,
                                ExpectedState {
                                    present: true,
                                    version: 1,
                                    class,
                                    expiry_delta_ms: 0,
                                },
                            ),
                            Ok(PutOutcome::Rejected(reason)) => {
                                *lock_mutex(&worker_error) =
                                    Some(format!("prefill rejected key {index}: {reason:?}"));
                                worker_abort.store(true, Ordering::Release);
                                return;
                            }
                            Err(error) => {
                                *lock_mutex(&worker_error) =
                                    Some(format!("prefill failed for key {index}: {error}"));
                                worker_abort.store(true, Ordering::Release);
                                return;
                            }
                        }
                    }
                }
            });
        match worker {
            Ok(worker) => workers.push(worker),
            Err(spawn_error) => {
                abort.store(true, Ordering::Release);
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(format!("cannot spawn prefill worker: {spawn_error}"));
            }
        }
    }
    for worker in workers {
        worker
            .join()
            .map_err(|_| "a Hybrid prefill worker panicked".to_owned())?;
    }
    lock_mutex(&error).take().map_or(Ok(()), Err)
}

fn verify_prefill(
    cache: &BenchCache,
    keys: &KeySpace,
    states: &KeyStateTable,
    prefill_count: usize,
    samples: usize,
    mix: &SizeMix,
) -> Result<(), String> {
    let samples = samples.min(prefill_count);
    for sample in 0..samples {
        let index = ((sample as u128 * prefill_count as u128) / samples as u128) as usize;
        let _ordering = states.lock(index);
        let state = states.normalize_expiry(index, states.load(index));
        let key = keys.key(index);
        if let Some(value) = cache
            .get(&key)
            .map_err(|error| format!("prefill sample read failed for key {index}: {error}"))?
        {
            if !state.present || !validate_value(&value, mix, state.class, index, state.version) {
                return Err(format!("stale/corrupt prefill sample for key {index}"));
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
struct ReopenVerification {
    samples: usize,
    expected_live: usize,
    live_hits: usize,
    live_misses: usize,
    absent_verified: usize,
}

fn verify_reopen(
    cache: &BenchCache,
    keys: &KeySpace,
    states: &KeyStateTable,
    requested_samples: usize,
    mix: &SizeMix,
) -> Result<ReopenVerification, String> {
    let samples = requested_samples.min(keys.count);
    let mut verification = ReopenVerification {
        samples,
        ..ReopenVerification::default()
    };
    for sample in 0..samples {
        let index = ((sample as u128 * keys.count as u128) / samples as u128) as usize;
        let _ordering = states.lock(index);
        let state = states.normalize_expiry(index, states.load(index));
        let key = keys.key(index);
        let found = cache
            .get(&key)
            .map_err(|error| format!("reopen sample read failed for key {index}: {error}"))?;
        match (state.present, found) {
            (true, Some(value)) => {
                verification.expected_live = verification.expected_live.saturating_add(1);
                if !validate_value(&value, mix, state.class, index, state.version) {
                    return Err(format!(
                        "stale/corrupt value after clean reopen for key {index}"
                    ));
                }
                verification.live_hits = verification.live_hits.saturating_add(1);
            }
            (true, None) => {
                verification.expected_live = verification.expected_live.saturating_add(1);
                verification.live_misses = verification.live_misses.saturating_add(1);
            }
            (false, Some(_)) => {
                return Err(format!(
                    "removed/expired value resurrected after clean reopen for key {index}"
                ));
            }
            (false, None) => {
                verification.absent_verified = verification.absent_verified.saturating_add(1);
            }
        }
    }
    Ok(verification)
}

fn exercise_semantics(
    cache: &BenchCache,
    keys: &KeySpace,
    states: &KeyStateTable,
    options: &Options,
) -> Result<(), String> {
    let index = keys.count - 1;
    let _ordering = states.lock(index);
    let key = keys.key(index);
    let routes = options.mix.routing_classes(options.small_object_max);
    let classes = routes.map_or_else(|| vec![0, 0], |(small, large)| vec![small, large, small]);
    let ttl_class = routes.map_or(0, |(small, _)| small);
    let mut state = states.normalize_expiry(index, states.load(index));
    let mut value = Vec::new();
    for class in classes {
        let version = state.next_version()?;
        let length = fill_value(&mut value, &options.mix, class, index, version)?;
        match cache.put(&key, &value[..length]) {
            Ok(PutOutcome::Stored) => {
                state = ExpectedState {
                    present: true,
                    version,
                    class,
                    expiry_delta_ms: 0,
                };
                states.store(index, state);
            }
            Ok(PutOutcome::Rejected(reason)) => {
                return Err(format!("semantic put rejected: {reason:?}"));
            }
            Err(error) => return Err(format!("semantic put failed: {error}")),
        }
        cache
            .flush()
            .map_err(|error| format!("semantic flush failed: {error}"))?;
        require_current_hit(cache, &key, &options.mix, index, state)?;
    }
    cache
        .remove(&key)
        .map_err(|error| format!("semantic remove failed: {error}"))?;
    state = state.without_value();
    states.store(index, state);
    require_absent(cache, &key, "remove")?;

    let version = state.next_version()?;
    let expires_at = now_unix_ms().saturating_add(options.ttl_ms);
    let length = fill_value(&mut value, &options.mix, ttl_class, index, version)?;
    match cache.put_with_options(
        &key,
        &value[..length],
        PutOptions {
            expires_at_unix_ms: Some(expires_at),
        },
    ) {
        Ok(PutOutcome::Stored) => {}
        Ok(PutOutcome::Rejected(reason)) => {
            return Err(format!("semantic TTL put rejected: {reason:?}"));
        }
        Err(error) => return Err(format!("semantic TTL put failed: {error}")),
    }
    state = ExpectedState {
        present: true,
        version,
        class: ttl_class,
        expiry_delta_ms: states.expiry_delta(expires_at)?,
    };
    states.store(index, state);
    cache
        .flush()
        .map_err(|error| format!("semantic TTL flush failed: {error}"))?;
    thread::sleep(Duration::from_millis(options.ttl_ms.saturating_add(10)));
    states.store(index, state.without_value());
    require_absent(cache, &key, "TTL expiry")
}

fn require_current_hit(
    cache: &BenchCache,
    key: &[u8],
    mix: &SizeMix,
    index: usize,
    state: ExpectedState,
) -> Result<(), String> {
    match cache.get(key) {
        Ok(Some(value)) if validate_value(&value, mix, state.class, index, state.version) => Ok(()),
        Ok(Some(_)) => Err(format!(
            "stale value after cross-tier update for key {index}"
        )),
        Ok(None) => Err(format!("immediate cross-tier update miss for key {index}")),
        Err(error) => Err(format!("cross-tier verification failed: {error}")),
    }
}

fn require_absent(cache: &BenchCache, key: &[u8], operation: &str) -> Result<(), String> {
    match cache.get(key) {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err(format!("{operation} revived a stale value")),
        Err(error) => Err(format!("{operation} verification failed: {error}")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TemporalBand {
    Recent,
    Historical,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReadLatencyTier {
    Memory,
    Bucket,
    Region,
    Miss,
}

impl ReadLatencyTier {
    fn from_cache_tier(tier: CacheTier) -> Option<Self> {
        match tier {
            CacheTier::Memory => Some(Self::Memory),
            CacheTier::SmallObjectDisk => Some(Self::Bucket),
            CacheTier::RegionLogDisk => Some(Self::Region),
            _ => None,
        }
    }
}

#[derive(Clone)]
struct TemporalAccess {
    head: Arc<AtomicU64>,
    key_count: usize,
    window_keys: usize,
    hot_read_percent: u8,
}

impl TemporalAccess {
    fn new(key_count: usize, window_keys: usize, hot_read_percent: u8, head: u64) -> Self {
        debug_assert!(key_count > 0);
        debug_assert!((1..=key_count).contains(&window_keys));
        Self {
            head: Arc::new(AtomicU64::new(head)),
            key_count,
            window_keys,
            hot_read_percent,
        }
    }

    const fn window_keys(&self) -> usize {
        self.window_keys
    }

    fn head(&self) -> u64 {
        self.head.load(Ordering::Acquire)
    }

    fn next_write(&self) -> usize {
        let sequence = self.head.fetch_add(1, Ordering::AcqRel);
        (sequence % self.key_count as u64) as usize
    }

    fn select_read(&self, random: &mut XorShift64) -> (usize, TemporalBand) {
        let head = self.head();
        let populated = usize::try_from(head.min(self.key_count as u64)).unwrap_or(self.key_count);
        if populated == 0 {
            return (
                (random.next() % self.key_count as u64) as usize,
                TemporalBand::Historical,
            );
        }
        let recent = self.window_keys.min(populated);
        let historical = populated - recent;
        let choose_recent =
            historical == 0 || random.next() % 100 < u64::from(self.hot_read_percent);
        let (age, band) = if choose_recent {
            (
                (random.next() % recent as u64) as usize,
                TemporalBand::Recent,
            )
        } else {
            (
                recent + (random.next() % historical as u64) as usize,
                TemporalBand::Historical,
            )
        };
        let sequence = head - 1 - age as u64;
        ((sequence % self.key_count as u64) as usize, band)
    }
}

struct PhaseWorkload {
    keys: Arc<KeySpace>,
    states: Arc<KeyStateTable>,
    mix: SizeMix,
    read_percent: u8,
    remove_percent: u8,
    ttl_percent: u8,
    cross_tier_percent: u8,
    small_object_max: usize,
    ttl_ms: u64,
    concurrency: usize,
    duration: Duration,
    seed: u64,
    temporal: Option<TemporalAccess>,
}

fn run_phase(cache: &BenchCache, workload: PhaseWorkload) -> Result<Phase, String> {
    let PhaseWorkload {
        keys,
        states,
        mix,
        read_percent,
        remove_percent,
        ttl_percent,
        cross_tier_percent,
        small_object_max,
        ttl_ms,
        concurrency,
        duration,
        seed,
        temporal,
    } = workload;
    let timeline_start = temporal.as_ref().map_or(0, TemporalAccess::head);
    let start = Arc::new(AtomicBool::new(false));
    let abort = Arc::new(AtomicBool::new(false));
    let (ready_tx, ready_rx) = mpsc::sync_channel(concurrency);
    let mut workers: Vec<thread::JoinHandle<Phase>> = Vec::new();
    workers
        .try_reserve_exact(concurrency)
        .map_err(|_| "cannot allocate Hybrid benchmark worker table".to_owned())?;
    for worker_id in 0..concurrency {
        let worker_cache = cache.clone();
        let worker_keys = Arc::clone(&keys);
        let worker_states = Arc::clone(&states);
        let worker_mix = mix.clone();
        let worker_start = Arc::clone(&start);
        let worker_abort = Arc::clone(&abort);
        let worker_ready = ready_tx.clone();
        let worker_temporal = temporal.clone();
        let worker = thread::Builder::new()
            .name(format!("hybrid-bench-{worker_id}"))
            .stack_size(WORKER_STACK_BYTES)
            .spawn(move || {
                let mut phase = Phase::default();
                let mut value = Vec::new();
                if let Err(error) = prepare_value_buffer(&mut value, worker_mix.maximum_bytes()) {
                    phase.record_error(error);
                    worker_abort.store(true, Ordering::Release);
                }
                if worker_ready.send(()).is_err() {
                    return phase;
                }
                while !worker_start.load(Ordering::Acquire) {
                    if worker_abort.load(Ordering::Acquire) {
                        return phase;
                    }
                    thread::park_timeout(Duration::from_millis(1));
                }
                let deadline = Instant::now()
                    .checked_add(duration)
                    .unwrap_or_else(Instant::now);
                let mut random = XorShift64::new(seed ^ mix64(worker_id as u64));
                let owned_keys = (worker_keys.count - worker_id).div_ceil(concurrency);
                while !worker_abort.load(Ordering::Acquire) && Instant::now() < deadline {
                    let is_read = random.next() % 100 < u64::from(read_percent);
                    let mutation = (!is_read).then(|| random.next() % 100);
                    let (key_index, temporal_band) = match &worker_temporal {
                        Some(temporal) if is_read => {
                            let (index, band) = temporal.select_read(&mut random);
                            (index, Some(band))
                        }
                        Some(temporal)
                            if mutation.expect("non-read operation has a mutation class")
                                < u64::from(remove_percent) =>
                        {
                            let (index, _) = temporal.select_read(&mut random);
                            (index, None)
                        }
                        Some(temporal) => (temporal.next_write(), None),
                        None => {
                            let local = (random.next() as usize) % owned_keys;
                            (worker_id + local * concurrency, None)
                        }
                    };
                    let key = worker_keys.key(key_index);
                    let _ordering = worker_states.lock(key_index);
                    let mut state =
                        worker_states.normalize_expiry(key_index, worker_states.load(key_index));
                    if is_read {
                        phase.reads = phase.reads.saturating_add(1);
                        phase.record_temporal_read(temporal_band);
                        let started = Instant::now();
                        let outcome = worker_cache.lookup(&key);
                        let elapsed = started.elapsed();
                        let latency_tier = match outcome {
                            Ok(HybridLookupOutcome::Hit { value: found, tier })
                                if state.present
                                    && validate_value(
                                        &found,
                                        &worker_mix,
                                        state.class,
                                        key_index,
                                        state.version,
                                    ) =>
                            {
                                phase.hits = phase.hits.saturating_add(1);
                                phase.read_bytes =
                                    phase.read_bytes.saturating_add(found.len() as u64);
                                phase.record_temporal_hit(temporal_band, tier);
                                ReadLatencyTier::from_cache_tier(tier)
                            }
                            Ok(HybridLookupOutcome::Hit { tier, .. }) => {
                                phase.record_error(format!(
                                    "stale/corrupt value returned for key {key_index}"
                                ));
                                phase.stale_values = phase.stale_values.saturating_add(1);
                                worker_abort.store(true, Ordering::Release);
                                ReadLatencyTier::from_cache_tier(tier)
                            }
                            Ok(HybridLookupOutcome::Miss(_)) => {
                                phase.misses = phase.misses.saturating_add(1);
                                phase.record_temporal_miss(temporal_band);
                                Some(ReadLatencyTier::Miss)
                            }
                            Err(error) => {
                                phase.record_error(format!(
                                    "lookup failed for key {key_index}: {error}"
                                ));
                                None
                            }
                        };
                        phase.read_latency.record(elapsed);
                        phase.latency.record(elapsed);
                        phase.record_temporal_latency(temporal_band, elapsed);
                        if let Some(tier) = latency_tier {
                            phase.record_read_tier_latency(tier, elapsed);
                        }
                    } else {
                        let mutation = mutation.expect("non-read operation has a mutation class");
                        if mutation < u64::from(remove_percent) {
                            phase.removes = phase.removes.saturating_add(1);
                            let started = Instant::now();
                            let outcome = worker_cache.remove(&key);
                            let elapsed = started.elapsed();
                            phase.write_latency.record(elapsed);
                            phase.latency.record(elapsed);
                            match outcome {
                                Ok(_) => {
                                    state = state.without_value();
                                    worker_states.store(key_index, state);
                                }
                                Err(error) => phase.record_error(format!(
                                    "remove failed for key {key_index}: {error}"
                                )),
                            }
                        } else if mutation < u64::from(remove_percent) + u64::from(ttl_percent) {
                            phase.ttl_puts = phase.ttl_puts.saturating_add(1);
                            phase.writes = phase.writes.saturating_add(1);
                            if perform_put(
                                &worker_cache,
                                &worker_keys,
                                &worker_states,
                                &worker_mix,
                                key_index,
                                worker_mix
                                    .class_for_version(key_index, state.version.saturating_add(1)),
                                Some(now_unix_ms().saturating_add(ttl_ms)),
                                &mut state,
                                &mut value,
                                &mut phase,
                            )
                            .is_err()
                            {
                                worker_abort.store(true, Ordering::Release);
                            }
                        } else if mutation
                            < u64::from(remove_percent)
                                + u64::from(ttl_percent)
                                + u64::from(cross_tier_percent)
                        {
                            phase.cross_tier_updates = phase.cross_tier_updates.saturating_add(1);
                            let (small, large) = worker_mix
                                .routing_classes(small_object_max)
                                .expect("validated mixed routing classes");
                            let routes = if state.version & 1 == 0 {
                                [small, large]
                            } else {
                                [large, small]
                            };
                            for class in routes {
                                phase.writes = phase.writes.saturating_add(1);
                                if perform_put(
                                    &worker_cache,
                                    &worker_keys,
                                    &worker_states,
                                    &worker_mix,
                                    key_index,
                                    class,
                                    None,
                                    &mut state,
                                    &mut value,
                                    &mut phase,
                                )
                                .is_err()
                                {
                                    worker_abort.store(true, Ordering::Release);
                                    break;
                                }
                            }
                        } else {
                            phase.writes = phase.writes.saturating_add(1);
                            let class = worker_mix
                                .class_for_version(key_index, state.version.saturating_add(1));
                            if perform_put(
                                &worker_cache,
                                &worker_keys,
                                &worker_states,
                                &worker_mix,
                                key_index,
                                class,
                                None,
                                &mut state,
                                &mut value,
                                &mut phase,
                            )
                            .is_err()
                            {
                                worker_abort.store(true, Ordering::Release);
                            }
                        }
                    }
                }
                phase
            });
        match worker {
            Ok(worker) => workers.push(worker),
            Err(spawn_error) => {
                abort.store(true, Ordering::Release);
                start.store(true, Ordering::Release);
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(format!(
                    "cannot spawn Hybrid benchmark worker: {spawn_error}"
                ));
            }
        }
    }
    drop(ready_tx);
    for _ in 0..concurrency {
        if ready_rx.recv().is_err() {
            abort.store(true, Ordering::Release);
            start.store(true, Ordering::Release);
            for worker in workers {
                let _ = worker.join();
            }
            return Err("a Hybrid benchmark worker failed during startup".to_owned());
        }
    }
    let started = Instant::now();
    start.store(true, Ordering::Release);
    let mut phase = Phase::default();
    for worker in workers {
        let worker = worker
            .join()
            .map_err(|_| "a Hybrid benchmark worker panicked".to_owned())?;
        phase.merge(&worker);
    }
    phase.elapsed = started.elapsed();
    phase.timeline_start = timeline_start;
    phase.timeline_end = temporal.as_ref().map_or(0, TemporalAccess::head);
    Ok(phase)
}

#[allow(clippy::too_many_arguments)]
fn perform_put(
    cache: &BenchCache,
    keys: &KeySpace,
    states: &KeyStateTable,
    mix: &SizeMix,
    key_index: usize,
    class: usize,
    expires_at: Option<u64>,
    state: &mut ExpectedState,
    value: &mut Vec<u8>,
    phase: &mut Phase,
) -> Result<(), ()> {
    let version = match state.next_version() {
        Ok(version) => version,
        Err(_) => {
            phase.record_error(format!("version exhausted for key {key_index}"));
            return Err(());
        }
    };
    let length = match fill_value(value, mix, class, key_index, version) {
        Ok(length) => length,
        Err(_) => {
            phase.record_error(format!("cannot generate value for key {key_index}"));
            return Err(());
        }
    };
    let key = keys.key(key_index);
    let started = Instant::now();
    let outcome = cache.put_with_options(
        &key,
        &value[..length],
        PutOptions {
            expires_at_unix_ms: expires_at,
        },
    );
    let elapsed = started.elapsed();
    phase.write_latency.record(elapsed);
    phase.latency.record(elapsed);
    match outcome {
        Ok(PutOutcome::Stored) => {
            let expiry_delta_ms = match expires_at {
                Some(expires_at) => match states.expiry_delta(expires_at) {
                    Ok(delta) => delta,
                    Err(_) => {
                        phase.record_error(format!("TTL delta overflow for key {key_index}"));
                        return Err(());
                    }
                },
                None => 0,
            };
            *state = ExpectedState {
                present: true,
                version,
                class,
                expiry_delta_ms,
            };
            states.store(key_index, *state);
            phase.record_stored_write(length);
            Ok(())
        }
        Ok(PutOutcome::Rejected(reason)) => {
            phase.record_rejection(reason);
            Ok(())
        }
        Err(error) => {
            phase.record_error(format!("put failed for key {key_index}: {error}"));
            Err(())
        }
    }
}

#[derive(Default)]
struct Phase {
    elapsed: Duration,
    first_error: Option<String>,
    first_rejection: Option<RejectReason>,
    timeline_start: u64,
    timeline_end: u64,
    reads: u64,
    writes: u64,
    removes: u64,
    ttl_puts: u64,
    cross_tier_updates: u64,
    hits: u64,
    misses: u64,
    stored: u64,
    rejected: u64,
    errors: u64,
    stale_values: u64,
    read_bytes: u64,
    write_bytes: u64,
    recent_reads: u64,
    recent_hits: u64,
    recent_misses: u64,
    recent_memory_hits: u64,
    recent_bucket_hits: u64,
    recent_region_hits: u64,
    historical_reads: u64,
    historical_hits: u64,
    historical_misses: u64,
    historical_memory_hits: u64,
    historical_bucket_hits: u64,
    historical_region_hits: u64,
    latency: Histogram,
    read_latency: Histogram,
    write_latency: Histogram,
    memory_read_latency: Histogram,
    bucket_read_latency: Histogram,
    region_read_latency: Histogram,
    miss_read_latency: Histogram,
    recent_read_latency: Histogram,
    historical_read_latency: Histogram,
}

impl Phase {
    fn operations(&self) -> u64 {
        self.reads
            .saturating_add(self.writes)
            .saturating_add(self.removes)
    }

    fn record_stored_write(&mut self, bytes: usize) {
        self.stored = self.stored.saturating_add(1);
        self.write_bytes = self.write_bytes.saturating_add(bytes as u64);
    }

    fn timeline_advances(&self) -> u64 {
        self.timeline_end.saturating_sub(self.timeline_start)
    }

    fn record_error(&mut self, error: String) {
        self.errors = self.errors.saturating_add(1);
        if self.first_error.is_none() {
            self.first_error = Some(error);
        }
    }

    fn record_rejection(&mut self, reason: RejectReason) {
        self.rejected = self.rejected.saturating_add(1);
        self.first_rejection.get_or_insert(reason);
    }

    fn record_temporal_read(&mut self, band: Option<TemporalBand>) {
        match band {
            Some(TemporalBand::Recent) => self.recent_reads = self.recent_reads.saturating_add(1),
            Some(TemporalBand::Historical) => {
                self.historical_reads = self.historical_reads.saturating_add(1)
            }
            None => {}
        }
    }

    fn record_temporal_hit(&mut self, band: Option<TemporalBand>, tier: CacheTier) {
        let (hits, memory, bucket, region) = match band {
            Some(TemporalBand::Recent) => (
                &mut self.recent_hits,
                &mut self.recent_memory_hits,
                &mut self.recent_bucket_hits,
                &mut self.recent_region_hits,
            ),
            Some(TemporalBand::Historical) => (
                &mut self.historical_hits,
                &mut self.historical_memory_hits,
                &mut self.historical_bucket_hits,
                &mut self.historical_region_hits,
            ),
            None => return,
        };
        *hits = hits.saturating_add(1);
        let tier_hits = match tier {
            CacheTier::Memory => memory,
            CacheTier::SmallObjectDisk => bucket,
            CacheTier::RegionLogDisk => region,
            _ => return,
        };
        *tier_hits = tier_hits.saturating_add(1);
    }

    fn record_temporal_miss(&mut self, band: Option<TemporalBand>) {
        match band {
            Some(TemporalBand::Recent) => self.recent_misses = self.recent_misses.saturating_add(1),
            Some(TemporalBand::Historical) => {
                self.historical_misses = self.historical_misses.saturating_add(1)
            }
            None => {}
        }
    }

    fn record_temporal_latency(&mut self, band: Option<TemporalBand>, elapsed: Duration) {
        match band {
            Some(TemporalBand::Recent) => self.recent_read_latency.record(elapsed),
            Some(TemporalBand::Historical) => self.historical_read_latency.record(elapsed),
            None => {}
        }
    }

    fn record_read_tier_latency(&mut self, tier: ReadLatencyTier, elapsed: Duration) {
        match tier {
            ReadLatencyTier::Memory => self.memory_read_latency.record(elapsed),
            ReadLatencyTier::Bucket => self.bucket_read_latency.record(elapsed),
            ReadLatencyTier::Region => self.region_read_latency.record(elapsed),
            ReadLatencyTier::Miss => self.miss_read_latency.record(elapsed),
        }
    }

    fn merge(&mut self, other: &Self) {
        if self.first_error.is_none() {
            self.first_error.clone_from(&other.first_error);
        }
        if self.first_rejection.is_none() {
            self.first_rejection = other.first_rejection;
        }
        self.reads = self.reads.saturating_add(other.reads);
        self.writes = self.writes.saturating_add(other.writes);
        self.removes = self.removes.saturating_add(other.removes);
        self.ttl_puts = self.ttl_puts.saturating_add(other.ttl_puts);
        self.cross_tier_updates = self
            .cross_tier_updates
            .saturating_add(other.cross_tier_updates);
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.stored = self.stored.saturating_add(other.stored);
        self.rejected = self.rejected.saturating_add(other.rejected);
        self.errors = self.errors.saturating_add(other.errors);
        self.stale_values = self.stale_values.saturating_add(other.stale_values);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.write_bytes = self.write_bytes.saturating_add(other.write_bytes);
        self.recent_reads = self.recent_reads.saturating_add(other.recent_reads);
        self.recent_hits = self.recent_hits.saturating_add(other.recent_hits);
        self.recent_misses = self.recent_misses.saturating_add(other.recent_misses);
        self.recent_memory_hits = self
            .recent_memory_hits
            .saturating_add(other.recent_memory_hits);
        self.recent_bucket_hits = self
            .recent_bucket_hits
            .saturating_add(other.recent_bucket_hits);
        self.recent_region_hits = self
            .recent_region_hits
            .saturating_add(other.recent_region_hits);
        self.historical_reads = self.historical_reads.saturating_add(other.historical_reads);
        self.historical_hits = self.historical_hits.saturating_add(other.historical_hits);
        self.historical_misses = self
            .historical_misses
            .saturating_add(other.historical_misses);
        self.historical_memory_hits = self
            .historical_memory_hits
            .saturating_add(other.historical_memory_hits);
        self.historical_bucket_hits = self
            .historical_bucket_hits
            .saturating_add(other.historical_bucket_hits);
        self.historical_region_hits = self
            .historical_region_hits
            .saturating_add(other.historical_region_hits);
        self.latency.merge(&other.latency);
        self.read_latency.merge(&other.read_latency);
        self.write_latency.merge(&other.write_latency);
        self.memory_read_latency.merge(&other.memory_read_latency);
        self.bucket_read_latency.merge(&other.bucket_read_latency);
        self.region_read_latency.merge(&other.region_read_latency);
        self.miss_read_latency.merge(&other.miss_read_latency);
        self.recent_read_latency.merge(&other.recent_read_latency);
        self.historical_read_latency
            .merge(&other.historical_read_latency);
    }

    fn merge_sequential(&mut self, other: &Self) {
        if self.elapsed.is_zero() {
            self.timeline_start = other.timeline_start;
        }
        self.merge(other);
        self.elapsed = self.elapsed.saturating_add(other.elapsed);
        self.timeline_end = other.timeline_end;
    }
}

struct Histogram {
    buckets: [u64; HISTOGRAM_BUCKETS],
    count: u64,
    maximum_ns: u64,
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; HISTOGRAM_BUCKETS],
            count: 0,
            maximum_ns: 0,
        }
    }
}

impl Histogram {
    fn record(&mut self, duration: Duration) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let exponent = if ns <= 1 {
            0
        } else {
            (u64::BITS - 1 - ns.leading_zeros()) as usize
        };
        let base = 1_u64 << exponent;
        let sub = if ns <= 1 {
            0
        } else {
            (((ns - base) as u128 * 8) / base as u128).min(7) as usize
        };
        let bucket = (exponent * 8 + sub).min(HISTOGRAM_BUCKETS - 1);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.maximum_ns = self.maximum_ns.max(ns);
    }

    fn merge(&mut self, other: &Self) {
        for (target, source) in self.buckets.iter_mut().zip(other.buckets) {
            *target = target.saturating_add(source);
        }
        self.count = self.count.saturating_add(other.count);
        self.maximum_ns = self.maximum_ns.max(other.maximum_ns);
    }

    fn percentile(&self, permille: u64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = self.count.saturating_mul(permille).saturating_add(999) / 1000;
        let mut seen = 0_u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target.max(1) {
                let exponent = index / 8;
                let sub = index % 8;
                let base = 1_u128 << exponent;
                let upper = base + ((sub + 1) as u128 * base).div_ceil(8);
                return u64::try_from(upper.saturating_sub(1))
                    .unwrap_or(u64::MAX)
                    .min(self.maximum_ns.max(1));
            }
        }
        self.maximum_ns
    }
}

#[derive(Clone, Copy, Default)]
struct StatsDelta {
    memory_hits: u64,
    bucket_hits: u64,
    region_hits: u64,
    misses: u64,
    promotions: u64,
    request_rejections: u64,
    request_wait_ns: u64,
    bucket_bytes_read: u64,
    bucket_bytes_written: u64,
    bucket_io_submitted: u64,
    bucket_io_completed: u64,
    bucket_io_errors: u64,
    bucket_io_submit_wait_ns: u64,
    bucket_io_completion_ns: u64,
    bucket_page_buffer_wait_ns: u64,
    region_bytes_read: u64,
    region_bytes_written: u64,
    region_write_batches: u64,
    region_records_coalesced: u64,
    region_staging_chunk_bytes: u64,
    region_staging_resident_bytes: u64,
    region_staging_flushing_bytes: u64,
    region_staging_sealed_spans: u64,
    region_staging_sealed_bytes: u64,
    region_staging_completion_live_records: u64,
    region_staging_completion_live_bytes: u64,
    region_staging_completion_obsolete_records: u64,
    region_staging_completion_obsolete_bytes: u64,
    region_io_submitted: u64,
    region_io_completed: u64,
    region_io_errors: u64,
    region_backpressure_wait_ns: u64,
    region_read_queue_wait_ns: u64,
    region_write_queue_wait_ns: u64,
    region_control_queue_wait_ns: u64,
    region_read_buffer_wait_ns: u64,
    region_write_buffer_wait_ns: u64,
    region_control_buffer_wait_ns: u64,
    region_metadata_buffer_wait_ns: u64,
    region_io_submit_wait_ns: u64,
    region_io_completion_ns: u64,
    region_reuses: u64,
    region_background_reclaims: u64,
    region_reclaim_records_scanned: u64,
    region_reclaim_index_fallbacks: u64,
    host_write_bytes: u64,
    admitted_value_bytes: u64,
    write_amplification_milli: u64,
    bucket_io_peak: u64,
    region_io_peak: u64,
    journal_rollovers: u64,
    journal_rollover_max_ns: u64,
    journal_commit_batches: u64,
    journal_commit_records: u64,
    journal_durability_syncs: u64,
    journal_sync_elapsed_ns_total: u64,
    journal_sync_elapsed_ns_max: u64,
    journal_commit_rejected: u64,
    journal_commit_worker_panics: u64,
    journal_commit_queue_peak: u64,
    write_back_memory_only_puts: u64,
    write_back_fallbacks: u64,
    write_back_demoted_entries: u64,
    write_back_demoted_bytes: u64,
    write_back_demotion_failures: u64,
    write_back_lower_absent_evictions: u64,
    write_back_lower_candidate_evictions: u64,
    write_back_synchronous_demotions: u64,
    write_back_dropped_evictions: u64,
    write_back_proactive_scheduled: u64,
    write_back_proactive_skipped: u64,
    write_back_proactive_persisted: u64,
    write_back_proactive_rejected: u64,
    write_back_proactive_fatal: u64,
    write_back_proactive_invalidated: u64,
    write_back_volatile_loss_pending: bool,
    write_back_pending_entries: u64,
    write_back_pending_entries_peak: u64,
    write_back_pending_bytes: u64,
    write_back_pending_bytes_peak: u64,
    write_back_pending_lookup_misses: u64,
    write_back_pending_same_key_waits: u64,
    write_back_pending_same_key_wait_ns: u64,
    write_back_queue_rejections: u64,
    write_back_worker_panics: u64,
    write_back_queue_peak: u64,
    write_back_queue_submitted: u64,
    write_back_queue_completed: u64,
    write_back_queue_wait_ns: u64,
}

impl StatsDelta {
    fn between(before: ObservationStats, after: ObservationStats) -> Self {
        let before_staging = before.region_staging;
        let after_staging = after.region_staging;
        let before = before.hybrid;
        let after = after.hybrid;
        Self {
            memory_hits: after.memory_hits.saturating_sub(before.memory_hits),
            bucket_hits: after.small_disk_hits.saturating_sub(before.small_disk_hits),
            region_hits: after
                .region_disk_hits
                .saturating_sub(before.region_disk_hits),
            misses: after.misses.saturating_sub(before.misses),
            promotions: after.promotions.saturating_sub(before.promotions),
            request_rejections: after
                .request_rejections
                .saturating_sub(before.request_rejections),
            request_wait_ns: after.request_wait_ns.saturating_sub(before.request_wait_ns),
            bucket_bytes_read: after
                .bucket
                .bytes_read
                .saturating_sub(before.bucket.bytes_read),
            bucket_bytes_written: after
                .bucket
                .bytes_written
                .saturating_sub(before.bucket.bytes_written),
            bucket_io_submitted: after
                .bucket
                .io_submitted
                .saturating_sub(before.bucket.io_submitted),
            bucket_io_completed: after
                .bucket
                .io_completed
                .saturating_sub(before.bucket.io_completed),
            bucket_io_errors: after
                .bucket
                .io_errors
                .saturating_sub(before.bucket.io_errors),
            bucket_io_submit_wait_ns: after
                .bucket
                .io_submit_wait_ns
                .saturating_sub(before.bucket.io_submit_wait_ns),
            bucket_io_completion_ns: after
                .bucket
                .io_completion_ns
                .saturating_sub(before.bucket.io_completion_ns),
            bucket_page_buffer_wait_ns: after
                .bucket
                .page_buffer_wait_ns
                .saturating_sub(before.bucket.page_buffer_wait_ns),
            region_bytes_read: after
                .region
                .bytes_read
                .saturating_sub(before.region.bytes_read),
            region_bytes_written: after
                .region
                .bytes_written
                .saturating_sub(before.region.bytes_written),
            region_write_batches: after
                .region
                .write_batches
                .saturating_sub(before.region.write_batches),
            region_records_coalesced: after
                .region
                .records_coalesced
                .saturating_sub(before.region.records_coalesced),
            region_staging_chunk_bytes: after_staging.chunk_bytes,
            region_staging_resident_bytes: after_staging.resident_bytes,
            region_staging_flushing_bytes: after_staging.flushing_bytes,
            region_staging_sealed_spans: after_staging
                .sealed_spans
                .saturating_sub(before_staging.sealed_spans),
            region_staging_sealed_bytes: after_staging
                .sealed_bytes
                .saturating_sub(before_staging.sealed_bytes),
            region_staging_completion_live_records: after_staging
                .completion_live_records
                .saturating_sub(before_staging.completion_live_records),
            region_staging_completion_live_bytes: after_staging
                .completion_live_bytes
                .saturating_sub(before_staging.completion_live_bytes),
            region_staging_completion_obsolete_records: after_staging
                .completion_obsolete_records
                .saturating_sub(before_staging.completion_obsolete_records),
            region_staging_completion_obsolete_bytes: after_staging
                .completion_obsolete_bytes
                .saturating_sub(before_staging.completion_obsolete_bytes),
            region_io_submitted: after
                .region
                .io_submitted
                .saturating_sub(before.region.io_submitted),
            region_io_completed: after
                .region
                .io_completed
                .saturating_sub(before.region.io_completed),
            region_io_errors: after
                .region
                .io_errors
                .saturating_sub(before.region.io_errors),
            region_backpressure_wait_ns: after
                .region
                .backpressure_wait_ns
                .saturating_sub(before.region.backpressure_wait_ns),
            region_read_queue_wait_ns: after
                .region
                .read_queue_wait_ns
                .saturating_sub(before.region.read_queue_wait_ns),
            region_write_queue_wait_ns: after
                .region
                .write_queue_wait_ns
                .saturating_sub(before.region.write_queue_wait_ns),
            region_control_queue_wait_ns: after
                .region
                .control_queue_wait_ns
                .saturating_sub(before.region.control_queue_wait_ns),
            region_read_buffer_wait_ns: after
                .region
                .read_buffer_wait_ns
                .saturating_sub(before.region.read_buffer_wait_ns),
            region_write_buffer_wait_ns: after
                .region
                .write_buffer_wait_ns
                .saturating_sub(before.region.write_buffer_wait_ns),
            region_control_buffer_wait_ns: after
                .region
                .control_buffer_wait_ns
                .saturating_sub(before.region.control_buffer_wait_ns),
            region_metadata_buffer_wait_ns: after
                .region
                .metadata_buffer_wait_ns
                .saturating_sub(before.region.metadata_buffer_wait_ns),
            region_io_submit_wait_ns: after
                .region
                .io_submit_wait_ns
                .saturating_sub(before.region.io_submit_wait_ns),
            region_io_completion_ns: after
                .region
                .io_completion_ns
                .saturating_sub(before.region.io_completion_ns),
            region_reuses: after
                .region
                .regions_reused
                .saturating_sub(before.region.regions_reused),
            region_background_reclaims: after
                .region
                .background_regions_reclaimed
                .saturating_sub(before.region.background_regions_reclaimed),
            region_reclaim_records_scanned: after
                .region
                .reclaim_records_scanned
                .saturating_sub(before.region.reclaim_records_scanned),
            region_reclaim_index_fallbacks: after
                .region
                .reclaim_index_fallbacks
                .saturating_sub(before.region.reclaim_index_fallbacks),
            host_write_bytes: after
                .host_writes
                .host_write_bytes
                .saturating_sub(before.host_writes.host_write_bytes),
            admitted_value_bytes: after
                .host_writes
                .admitted_value_bytes
                .saturating_sub(before.host_writes.admitted_value_bytes),
            write_amplification_milli: amplification_milli(
                after
                    .host_writes
                    .host_write_bytes
                    .saturating_sub(before.host_writes.host_write_bytes),
                after
                    .host_writes
                    .admitted_value_bytes
                    .saturating_sub(before.host_writes.admitted_value_bytes),
            ),
            bucket_io_peak: after.bucket.io_in_flight_peak,
            region_io_peak: after.region.io_in_flight_peak,
            journal_rollovers: after
                .journal_rollovers
                .saturating_sub(before.journal_rollovers),
            journal_rollover_max_ns: after.journal_rollover_max_ns,
            journal_commit_batches: after
                .journal_group_commit
                .committed_batches
                .saturating_sub(before.journal_group_commit.committed_batches),
            journal_commit_records: after
                .journal_group_commit
                .committed_records
                .saturating_sub(before.journal_group_commit.committed_records),
            journal_durability_syncs: after
                .journal_group_commit
                .durability_syncs
                .saturating_sub(before.journal_group_commit.durability_syncs),
            journal_sync_elapsed_ns_total: after
                .journal_group_commit
                .sync_elapsed_ns_total
                .saturating_sub(before.journal_group_commit.sync_elapsed_ns_total),
            journal_sync_elapsed_ns_max: after.journal_group_commit.sync_elapsed_ns_max,
            journal_commit_rejected: after
                .journal_group_commit
                .rejected
                .saturating_sub(before.journal_group_commit.rejected),
            journal_commit_worker_panics: after
                .journal_group_commit
                .worker_panics
                .saturating_sub(before.journal_group_commit.worker_panics),
            journal_commit_queue_peak: after.journal_group_commit.in_flight_peak,
            write_back_memory_only_puts: after
                .write_back
                .memory_only_puts
                .saturating_sub(before.write_back.memory_only_puts),
            write_back_fallbacks: after
                .write_back
                .write_through_fallbacks
                .saturating_sub(before.write_back.write_through_fallbacks),
            write_back_demoted_entries: after
                .write_back
                .demoted_entries
                .saturating_sub(before.write_back.demoted_entries),
            write_back_demoted_bytes: after
                .write_back
                .demoted_bytes
                .saturating_sub(before.write_back.demoted_bytes),
            write_back_demotion_failures: after
                .write_back
                .demotion_failures
                .saturating_sub(before.write_back.demotion_failures),
            write_back_lower_absent_evictions: after
                .write_back
                .lower_absent_evictions
                .saturating_sub(before.write_back.lower_absent_evictions),
            write_back_lower_candidate_evictions: after
                .write_back
                .lower_candidate_evictions
                .saturating_sub(before.write_back.lower_candidate_evictions),
            write_back_synchronous_demotions: after
                .write_back
                .synchronous_demotions
                .saturating_sub(before.write_back.synchronous_demotions),
            write_back_dropped_evictions: after
                .write_back
                .dropped_evictions
                .saturating_sub(before.write_back.dropped_evictions),
            write_back_proactive_scheduled: after
                .write_back
                .proactive_scheduled
                .saturating_sub(before.write_back.proactive_scheduled),
            write_back_proactive_skipped: after
                .write_back
                .proactive_skipped
                .saturating_sub(before.write_back.proactive_skipped),
            write_back_proactive_persisted: after
                .write_back
                .proactive_persisted
                .saturating_sub(before.write_back.proactive_persisted),
            write_back_proactive_rejected: after
                .write_back
                .proactive_rejected
                .saturating_sub(before.write_back.proactive_rejected),
            write_back_proactive_fatal: after
                .write_back
                .proactive_fatal
                .saturating_sub(before.write_back.proactive_fatal),
            write_back_proactive_invalidated: after
                .write_back
                .proactive_invalidated
                .saturating_sub(before.write_back.proactive_invalidated),
            write_back_volatile_loss_pending: after.write_back.volatile_loss_pending,
            write_back_pending_entries: after.write_back.pending_entries,
            write_back_pending_entries_peak: after.write_back.pending_entries_peak,
            write_back_pending_bytes: after.write_back.pending_bytes,
            write_back_pending_bytes_peak: after.write_back.pending_bytes_peak,
            write_back_pending_lookup_misses: after
                .write_back
                .pending_lookup_misses
                .saturating_sub(before.write_back.pending_lookup_misses),
            write_back_pending_same_key_waits: after
                .write_back
                .pending_same_key_waits
                .saturating_sub(before.write_back.pending_same_key_waits),
            write_back_pending_same_key_wait_ns: after
                .write_back
                .pending_same_key_wait_ns
                .saturating_sub(before.write_back.pending_same_key_wait_ns),
            write_back_queue_rejections: after
                .write_back
                .queue_rejections
                .saturating_sub(before.write_back.queue_rejections),
            write_back_worker_panics: after
                .write_back
                .worker_panics
                .saturating_sub(before.write_back.worker_panics),
            write_back_queue_peak: after.write_back.queue_in_flight_peak,
            write_back_queue_submitted: after
                .write_back
                .queue_submitted
                .saturating_sub(before.write_back.queue_submitted),
            write_back_queue_completed: after
                .write_back
                .queue_completed
                .saturating_sub(before.write_back.queue_completed),
            write_back_queue_wait_ns: after
                .write_back
                .queue_wait_ns
                .saturating_sub(before.write_back.queue_wait_ns),
        }
    }
}

fn capacity_turnovers_for(options: &Options, host_write_bytes: u64) -> f64 {
    let capacity = active_disk_capacity(options);
    if capacity == 0 {
        0.0
    } else {
        host_write_bytes as f64 / capacity as f64
    }
}

fn active_disk_capacity(options: &Options) -> u64 {
    let bucket = if options.mix.uses_bucket(options.small_object_max) {
        options.bucket_capacity
    } else {
        0
    };
    let region = if options.mix.uses_region(options.small_object_max) {
        options.region_capacity
    } else {
        0
    };
    bucket.saturating_add(region)
}

fn steady_state_gate_ready(
    options: &Options,
    progress: &StatsDelta,
    required_region_reuses: u64,
) -> bool {
    options.steady_state_fill_turnovers == 0.0
        || (capacity_turnovers_for(options, progress.host_write_bytes)
            >= options.steady_state_fill_turnovers
            && progress.region_reuses >= required_region_reuses)
}

struct Report<'a> {
    options: &'a Options,
    phase: Phase,
    stats: StatsDelta,
    drain_stats: StatsDelta,
    total_stats: StatsDelta,
    premeasure_stats: StatsDelta,
    final_stats: HybridCacheStats,
    generator_planned_memory: usize,
    prefill_elapsed: Duration,
    premeasure_elapsed: Duration,
    steady_state_fill_phase: Phase,
    required_region_reuses: u64,
    drain: Duration,
    reopen: Duration,
    reopen_verify: Duration,
    reopen_close: Duration,
    reopen_verification: ReopenVerification,
}

impl Report<'_> {
    fn ops_per_sec(&self) -> f64 {
        rate(self.phase.operations(), self.phase.elapsed)
    }

    fn hit_percent(&self) -> f64 {
        let lookups = self.phase.hits.saturating_add(self.phase.misses);
        if lookups == 0 {
            0.0
        } else {
            self.phase.hits as f64 * 100.0 / lookups as f64
        }
    }

    fn p99_us(&self) -> f64 {
        self.phase.latency.percentile(990) as f64 / 1000.0
    }

    fn logical_keyspace_turnovers(&self) -> f64 {
        self.phase.timeline_advances() as f64 / self.options.keys as f64
    }

    fn timeline_wraps_crossed(&self) -> u64 {
        let keys = self.options.keys as u64;
        self.phase
            .timeline_end
            .checked_div(keys)
            .unwrap_or(0)
            .saturating_sub(self.phase.timeline_start.checked_div(keys).unwrap_or(0))
    }

    fn logical_ingest_turnovers(&self) -> f64 {
        self.bytes_per_capacity(self.phase.write_bytes)
    }

    fn admitted_disk_turnovers(&self) -> f64 {
        self.bytes_per_capacity(self.stats.admitted_value_bytes)
    }

    fn bytes_per_capacity(&self, bytes: u64) -> f64 {
        let capacity = active_disk_capacity(self.options);
        if capacity == 0 {
            0.0
        } else {
            bytes as f64 / capacity as f64
        }
    }

    fn recent_hit_percent(&self) -> f64 {
        hit_percent(self.phase.recent_hits, self.phase.recent_misses)
    }

    fn historical_hit_percent(&self) -> f64 {
        hit_percent(self.phase.historical_hits, self.phase.historical_misses)
    }

    fn recent_memory_read_percent(&self) -> f64 {
        percent_of(self.phase.recent_memory_hits, self.phase.recent_reads)
    }

    fn historical_memory_read_percent(&self) -> f64 {
        percent_of(
            self.phase.historical_memory_hits,
            self.phase.historical_reads,
        )
    }

    fn capacity_turnovers(&self) -> f64 {
        capacity_turnovers_for(self.options, self.stats.host_write_bytes)
    }

    fn total_capacity_turnovers(&self) -> f64 {
        capacity_turnovers_for(self.options, self.total_stats.host_write_bytes)
    }

    fn premeasure_capacity_turnovers(&self) -> f64 {
        capacity_turnovers_for(self.options, self.premeasure_stats.host_write_bytes)
    }

    fn acceptance_failures(&self) -> Vec<String> {
        let mut failures = Vec::new();
        if !steady_state_gate_ready(
            self.options,
            &self.premeasure_stats,
            self.required_region_reuses,
        ) {
            failures.push(format!(
                "steady-state pre-measure gate {:.3}x/{:.3}x with {} Region reuses",
                self.premeasure_capacity_turnovers(),
                self.options.steady_state_fill_turnovers,
                self.premeasure_stats.region_reuses
            ));
        }
        if self.phase.errors != 0 {
            failures.push(format!("{} correctness/I/O errors", self.phase.errors));
        }
        if self.phase.latency.count != self.phase.operations()
            || self.phase.read_latency.count != self.phase.reads
            || self.phase.write_latency.count
                != self.phase.writes.saturating_add(self.phase.removes)
        {
            failures.push(format!(
                "latency sample mismatch overall={}/{} read={}/{} write={}/{}",
                self.phase.latency.count,
                self.phase.operations(),
                self.phase.read_latency.count,
                self.phase.reads,
                self.phase.write_latency.count,
                self.phase.writes.saturating_add(self.phase.removes)
            ));
        }
        if self.phase.stale_values != 0 {
            failures.push(format!(
                "{} stale or corrupt versioned values",
                self.phase.stale_values
            ));
        }
        if self.phase.rejected != 0 {
            failures.push(format!(
                "{} puts were rejected by bounded admission/resources",
                self.phase.rejected
            ));
        }
        if self.total_stats.write_back_demotion_failures != 0
            || self.total_stats.write_back_queue_rejections != 0
            || self.total_stats.write_back_worker_panics != 0
        {
            failures.push(format!(
                "write-back demotion failures/rejections/panics = {}/{}/{}",
                self.total_stats.write_back_demotion_failures,
                self.total_stats.write_back_queue_rejections,
                self.total_stats.write_back_worker_panics
            ));
        }
        if self.total_stats.bucket_io_errors != 0 || self.total_stats.region_io_errors != 0 {
            failures.push(format!(
                "Bucket/Region I/O errors = {}/{}",
                self.total_stats.bucket_io_errors, self.total_stats.region_io_errors
            ));
        }
        let uses_bucket = self.options.mix.uses_bucket(self.options.small_object_max);
        let uses_region = self.options.mix.uses_region(self.options.small_object_max);
        if uses_bucket && self.stats.bucket_io_submitted == 0 {
            failures.push(format!(
                "measurement must submit active Bucket I/O; submitted={}",
                self.stats.bucket_io_submitted
            ));
        }
        if uses_region && self.stats.region_io_submitted == 0 {
            failures.push(format!(
                "measurement must submit active Region I/O; submitted={}",
                self.stats.region_io_submitted
            ));
        }
        if uses_bucket && self.total_stats.bucket_io_peak < self.options.min_disk_qd_peak {
            failures.push(format!(
                "active Bucket I/O QD peak {} below required {}",
                self.total_stats.bucket_io_peak, self.options.min_disk_qd_peak
            ));
        }
        if uses_region && self.total_stats.region_io_peak < self.options.min_disk_qd_peak {
            failures.push(format!(
                "active Region I/O QD peak {} below required {}",
                self.total_stats.region_io_peak, self.options.min_disk_qd_peak
            ));
        }
        if uses_bucket
            && self.options.steady_state_fill_turnovers > 0.0
            && self.final_stats.bucket.evictions == 0
        {
            failures.push("steady-state Bucket workload observed no entry eviction".into());
        }
        if self.options.write_mode == HybridWriteMode::WriteBack {
            if self.total_stats.write_back_demoted_entries == 0 {
                failures.push("write-back run observed no completed demotion".into());
            }
            if self.total_stats.write_back_queue_peak < self.options.min_write_back_qd_peak {
                failures.push(format!(
                    "write-back QD peak {} below required {}",
                    self.total_stats.write_back_queue_peak, self.options.min_write_back_qd_peak
                ));
            }
        }
        if self.total_stats.journal_commit_rejected != 0
            || self.total_stats.journal_commit_worker_panics != 0
        {
            failures.push(format!(
                "journal group-commit rejections/panics = {}/{}",
                self.total_stats.journal_commit_rejected,
                self.total_stats.journal_commit_worker_panics
            ));
        }
        if self.total_stats.journal_rollovers < self.options.min_journal_rollovers {
            failures.push(format!(
                "journal rollovers {} below required {}",
                self.total_stats.journal_rollovers, self.options.min_journal_rollovers
            ));
        }
        if self.capacity_turnovers() < self.options.min_capacity_turnovers {
            failures.push(format!(
                "capacity turnover {:.3}x below required {:.3}x",
                self.capacity_turnovers(),
                self.options.min_capacity_turnovers
            ));
        }
        if self.logical_keyspace_turnovers() < self.options.min_logical_keyspace_turnovers {
            failures.push(format!(
                "logical keyspace turnover {:.3}x below required {:.3}x",
                self.logical_keyspace_turnovers(),
                self.options.min_logical_keyspace_turnovers
            ));
        }
        if let Some(maximum) = self.options.max_journal_rollover_ms {
            let observed = self.total_stats.journal_rollover_max_ns as f64 / 1_000_000.0;
            if observed > maximum {
                failures.push(format!(
                    "journal rollover max {observed:.3} ms exceeds {maximum:.3} ms"
                ));
            }
        }
        if let Some(maximum) = self.options.max_close_ms {
            let observed = self.drain.as_secs_f64() * 1000.0;
            if observed > maximum {
                failures.push(format!(
                    "drain/close {observed:.3} ms exceeds {maximum:.3} ms"
                ));
            }
        }
        if self.final_stats.memory_dirty_entries != 0
            || self.final_stats.write_back.queue_in_flight != 0
        {
            failures.push(format!(
                "close left dirty entries/in-flight demotions = {}/{}",
                self.final_stats.memory_dirty_entries, self.final_stats.write_back.queue_in_flight
            ));
        }
        if let Some(minimum) = self.options.min_ops_per_sec {
            if self.ops_per_sec() < minimum {
                failures.push(format!(
                    "ops/s {:.3} below {minimum:.3}",
                    self.ops_per_sec()
                ));
            }
        }
        if let Some(maximum) = self.options.max_p99_us {
            if self.p99_us() > maximum {
                failures.push(format!("p99 {:.3} us exceeds {maximum:.3}", self.p99_us()));
            }
        }
        if let Some(minimum) = self.options.min_hit_percent {
            if self.hit_percent() < minimum {
                failures.push(format!(
                    "hit rate {:.3}% below {minimum:.3}%",
                    self.hit_percent()
                ));
            }
        }
        failures
    }

    fn to_json(&self) -> String {
        let mut output = String::with_capacity(8192);
        output.push('{');
        macro_rules! number_field {
            ($name:literal, $value:expr) => {
                write!(output, "\"{}\":{},", $name, $value)
                    .expect("writing JSON into a String cannot fail")
            };
        }
        macro_rules! string_field {
            ($name:literal, $value:expr) => {
                write!(output, "\"{}\":\"{}\",", $name, json_escape($value))
                    .expect("writing JSON into a String cannot fail")
            };
        }
        macro_rules! raw_field {
            ($name:literal, $value:expr) => {
                write!(output, "\"{}\":{},", $name, $value)
                    .expect("writing JSON into a String cannot fail")
            };
        }

        number_field!("schema_version", 9);
        string_field!("cache", "hybrid");
        string_field!("latency_scope", "individual_cache_api_calls");
        string_field!("write_value_generation", "prebuilt_worker_template");
        raw_field!("hardware_qualification", false);
        raw_field!("external_hardware_signoff_required", true);
        raw_field!("target_nvme_matrix_passed", false);
        raw_field!("external_nvme_soak_passed", false);
        raw_field!("external_power_loss_passed", false);
        raw_field!("external_thermal_passed", false);
        string_field!("qualification_scope", "software_scale_gate_single_run");
        string_field!(
            "bucket_path",
            self.options.bucket_path.to_string_lossy().as_ref()
        );
        string_field!(
            "region_path",
            self.options.region_path.to_string_lossy().as_ref()
        );
        string_field!(
            "manifest_path",
            self.options.manifest_path.to_string_lossy().as_ref()
        );
        number_field!("bucket_capacity_bytes", self.options.bucket_capacity);
        number_field!("region_capacity_bytes", self.options.region_capacity);
        number_field!(
            "active_disk_capacity_bytes",
            active_disk_capacity(self.options)
        );
        number_field!("memory_capacity_bytes", self.options.memory_capacity);
        string_field!("size_mix", &self.options.mix.as_spec());
        number_field!("small_object_max_bytes", self.options.small_object_max);
        number_field!("keys", self.options.keys);
        number_field!(
            "generator_memory_budget_bytes",
            self.options.generator_memory_budget
        );
        number_field!(
            "generator_planned_memory_bytes",
            self.generator_planned_memory
        );
        number_field!("prefill_concurrency", self.options.prefill_concurrency);
        number_field!("verify_samples", self.options.verify_samples);
        number_field!("journal_capacity_bytes", self.options.journal_capacity);
        number_field!("read_percent", self.options.read_percent);
        string_field!("access_pattern", self.options.access_pattern.as_str());
        number_field!(
            "temporal_window_percent",
            self.options.temporal_window_percent
        );
        number_field!(
            "temporal_window_keys",
            percentage_count(self.options.keys, self.options.temporal_window_percent).max(1)
        );
        number_field!(
            "temporal_hot_read_percent",
            self.options.temporal_hot_read_percent
        );
        number_field!("prefill_percent", self.options.prefill_percent);
        number_field!("remove_percent_of_mutations", self.options.remove_percent);
        number_field!("ttl_percent_of_mutations", self.options.ttl_percent);
        number_field!(
            "cross_tier_percent_of_mutations",
            self.options.cross_tier_percent
        );
        number_field!("ttl_ms", self.options.ttl_ms);
        number_field!("concurrency", self.options.concurrency);
        number_field!("queue_depth", self.options.queue_depth);
        string_field!("backpressure", self.options.backpressure.as_str());
        string_field!("api", self.options.api.as_str());
        string_field!(
            "client_completion_model",
            self.options.api.client_completion_model()
        );
        string_field!("engine_requested", self.options.engine.as_str());
        string_field!("io_mode_requested", self.options.mode.as_str());
        string_field!("write_mode", write_mode_name(self.options.write_mode));
        number_field!(
            "write_back_queue_depth",
            self.options.write_back_queue_depth
        );
        number_field!("write_back_workers", self.options.write_back_workers);
        number_field!("write_back_memory_bytes", self.options.write_back_memory);
        number_field!("prefill_seconds", self.prefill_elapsed.as_secs_f64());
        number_field!(
            "steady_state_fill_target_turnovers",
            self.options.steady_state_fill_turnovers
        );
        number_field!(
            "steady_state_fill_max_seconds",
            self.options.steady_state_fill_max.as_secs_f64()
        );
        number_field!(
            "steady_state_fill_seconds",
            self.steady_state_fill_phase.elapsed.as_secs_f64()
        );
        number_field!(
            "steady_state_fill_operations",
            self.steady_state_fill_phase.operations()
        );
        number_field!("premeasure_seconds", self.premeasure_elapsed.as_secs_f64());
        number_field!(
            "premeasure_host_write_bytes",
            self.premeasure_stats.host_write_bytes
        );
        number_field!(
            "premeasure_capacity_turnovers",
            self.premeasure_capacity_turnovers()
        );
        number_field!(
            "premeasure_region_reuses",
            self.premeasure_stats.region_reuses
        );
        number_field!(
            "steady_state_required_region_reuses",
            self.required_region_reuses
        );
        raw_field!(
            "steady_state_gate_passed",
            steady_state_gate_ready(
                self.options,
                &self.premeasure_stats,
                self.required_region_reuses,
            )
        );
        number_field!("elapsed_seconds", self.phase.elapsed.as_secs_f64());
        number_field!("operations", self.phase.operations());
        number_field!("operations_per_second", self.ops_per_sec());
        number_field!("reads", self.phase.reads);
        number_field!("writes", self.phase.writes);
        number_field!("removes", self.phase.removes);
        number_field!("ttl_puts", self.phase.ttl_puts);
        number_field!("cross_tier_updates", self.phase.cross_tier_updates);
        number_field!("hits", self.phase.hits);
        number_field!("misses", self.phase.misses);
        number_field!("hit_percent", self.hit_percent());
        number_field!("stored", self.phase.stored);
        number_field!("rejected", self.phase.rejected);
        match self.phase.first_rejection {
            Some(reason) => string_field!("first_rejection", &format!("{reason:?}")),
            None => raw_field!("first_rejection", "null"),
        }
        number_field!("errors", self.phase.errors);
        match &self.phase.first_error {
            Some(error) => string_field!("first_error", error),
            None => raw_field!("first_error", "null"),
        }
        number_field!("stale_values", self.phase.stale_values);
        number_field!("read_bytes", self.phase.read_bytes);
        number_field!("write_bytes", self.phase.write_bytes);
        number_field!(
            "read_mib_per_second",
            rate(self.phase.read_bytes, self.phase.elapsed) / (1024.0 * 1024.0)
        );
        number_field!(
            "write_mib_per_second",
            rate(self.phase.write_bytes, self.phase.elapsed) / (1024.0 * 1024.0)
        );
        number_field!("timeline_start", self.phase.timeline_start);
        number_field!("timeline_end", self.phase.timeline_end);
        number_field!("timeline_advances", self.phase.timeline_advances());
        number_field!("timeline_wraps_crossed", self.timeline_wraps_crossed());
        number_field!(
            "logical_keyspace_turnovers",
            self.logical_keyspace_turnovers()
        );
        number_field!("logical_ingest_turnovers", self.logical_ingest_turnovers());
        number_field!("recent_reads", self.phase.recent_reads);
        number_field!(
            "observed_recent_read_percent",
            percent_of(
                self.phase.recent_reads,
                self.phase
                    .recent_reads
                    .saturating_add(self.phase.historical_reads)
            )
        );
        number_field!("recent_hits", self.phase.recent_hits);
        number_field!("recent_misses", self.phase.recent_misses);
        number_field!("recent_hit_percent", self.recent_hit_percent());
        number_field!("recent_memory_hits", self.phase.recent_memory_hits);
        number_field!("recent_bucket_hits", self.phase.recent_bucket_hits);
        number_field!("recent_region_hits", self.phase.recent_region_hits);
        number_field!(
            "recent_memory_read_percent",
            self.recent_memory_read_percent()
        );
        number_field!("historical_reads", self.phase.historical_reads);
        number_field!("historical_hits", self.phase.historical_hits);
        number_field!("historical_misses", self.phase.historical_misses);
        number_field!("historical_hit_percent", self.historical_hit_percent());
        number_field!("historical_memory_hits", self.phase.historical_memory_hits);
        number_field!("historical_bucket_hits", self.phase.historical_bucket_hits);
        number_field!("historical_region_hits", self.phase.historical_region_hits);
        number_field!(
            "historical_memory_read_percent",
            self.historical_memory_read_percent()
        );
        number_field!(
            "latency_p50_us",
            self.phase.latency.percentile(500) as f64 / 1000.0
        );
        number_field!("latency_p99_us", self.p99_us());
        number_field!(
            "latency_p999_us",
            self.phase.latency.percentile(999) as f64 / 1000.0
        );
        number_field!(
            "latency_max_us",
            self.phase.latency.maximum_ns as f64 / 1000.0
        );
        number_field!("latency_samples", self.phase.latency.count);
        number_field!("read_latency_samples", self.phase.read_latency.count);
        number_field!("write_latency_samples", self.phase.write_latency.count);
        number_field!(
            "read_latency_p99_us",
            self.phase.read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "write_latency_p99_us",
            self.phase.write_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "memory_read_latency_p99_us",
            self.phase.memory_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "bucket_read_latency_p99_us",
            self.phase.bucket_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "region_read_latency_p99_us",
            self.phase.region_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "miss_read_latency_p99_us",
            self.phase.miss_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "recent_read_latency_p99_us",
            self.phase.recent_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!(
            "historical_read_latency_p99_us",
            self.phase.historical_read_latency.percentile(990) as f64 / 1000.0
        );
        number_field!("memory_hits", self.stats.memory_hits);
        number_field!("bucket_hits", self.stats.bucket_hits);
        number_field!("region_hits", self.stats.region_hits);
        number_field!("hybrid_misses", self.stats.misses);
        number_field!("promotions", self.stats.promotions);
        number_field!("request_rejections", self.stats.request_rejections);
        number_field!("request_wait_ns", self.stats.request_wait_ns);
        number_field!("bucket_bytes_read", self.stats.bucket_bytes_read);
        number_field!("bucket_bytes_written", self.stats.bucket_bytes_written);
        number_field!("bucket_io_submitted", self.stats.bucket_io_submitted);
        number_field!("bucket_io_completed", self.stats.bucket_io_completed);
        number_field!(
            "bucket_io_submit_wait_ns",
            self.stats.bucket_io_submit_wait_ns
        );
        number_field!(
            "bucket_io_completion_ns",
            self.stats.bucket_io_completion_ns
        );
        number_field!(
            "bucket_io_completion_avg_us",
            average_us(
                self.stats.bucket_io_completion_ns,
                self.stats.bucket_io_completed
            )
        );
        number_field!(
            "bucket_page_buffer_wait_ns",
            self.stats.bucket_page_buffer_wait_ns
        );
        number_field!("region_bytes_read", self.stats.region_bytes_read);
        number_field!("region_bytes_written", self.stats.region_bytes_written);
        number_field!("region_write_batches", self.stats.region_write_batches);
        number_field!(
            "region_records_coalesced",
            self.stats.region_records_coalesced
        );
        number_field!(
            "region_staging_chunk_bytes",
            self.stats.region_staging_chunk_bytes
        );
        number_field!(
            "region_staging_resident_bytes",
            self.stats.region_staging_resident_bytes
        );
        number_field!(
            "region_staging_flushing_bytes",
            self.stats.region_staging_flushing_bytes
        );
        number_field!(
            "region_staging_sealed_spans",
            self.stats.region_staging_sealed_spans
        );
        number_field!(
            "region_staging_sealed_bytes",
            self.stats.region_staging_sealed_bytes
        );
        number_field!(
            "region_staging_completion_live_records",
            self.stats.region_staging_completion_live_records
        );
        number_field!(
            "region_staging_completion_live_bytes",
            self.stats.region_staging_completion_live_bytes
        );
        number_field!(
            "region_staging_completion_obsolete_records",
            self.stats.region_staging_completion_obsolete_records
        );
        number_field!(
            "region_staging_completion_obsolete_bytes",
            self.stats.region_staging_completion_obsolete_bytes
        );
        number_field!(
            "region_staging_seal_fill_percent",
            percent_of(
                self.stats.region_staging_sealed_bytes,
                self.stats
                    .region_staging_sealed_spans
                    .saturating_mul(self.stats.region_staging_chunk_bytes),
            )
        );
        number_field!(
            "region_staging_obsolete_record_percent",
            percent_of(
                self.stats.region_staging_completion_obsolete_records,
                self.stats
                    .region_staging_completion_live_records
                    .saturating_add(self.stats.region_staging_completion_obsolete_records),
            )
        );
        number_field!(
            "region_staging_obsolete_byte_percent",
            percent_of(
                self.stats.region_staging_completion_obsolete_bytes,
                self.stats
                    .region_staging_completion_live_bytes
                    .saturating_add(self.stats.region_staging_completion_obsolete_bytes),
            )
        );
        number_field!("region_io_submitted", self.stats.region_io_submitted);
        number_field!("region_io_completed", self.stats.region_io_completed);
        number_field!(
            "region_backpressure_wait_ns",
            self.stats.region_backpressure_wait_ns
        );
        number_field!(
            "region_read_queue_wait_ns",
            self.stats.region_read_queue_wait_ns
        );
        number_field!(
            "region_write_queue_wait_ns",
            self.stats.region_write_queue_wait_ns
        );
        number_field!(
            "region_control_queue_wait_ns",
            self.stats.region_control_queue_wait_ns
        );
        number_field!(
            "region_read_buffer_wait_ns",
            self.stats.region_read_buffer_wait_ns
        );
        number_field!(
            "region_write_buffer_wait_ns",
            self.stats.region_write_buffer_wait_ns
        );
        number_field!(
            "region_control_buffer_wait_ns",
            self.stats.region_control_buffer_wait_ns
        );
        number_field!(
            "region_metadata_buffer_wait_ns",
            self.stats.region_metadata_buffer_wait_ns
        );
        number_field!(
            "region_io_submit_wait_ns",
            self.stats.region_io_submit_wait_ns
        );
        number_field!(
            "region_io_completion_ns",
            self.stats.region_io_completion_ns
        );
        number_field!(
            "region_io_completion_avg_us",
            average_us(
                self.stats.region_io_completion_ns,
                self.stats.region_io_completed
            )
        );
        number_field!("region_reuses", self.stats.region_reuses);
        number_field!(
            "region_background_reclaims",
            self.stats.region_background_reclaims
        );
        number_field!(
            "region_reclaim_records_scanned",
            self.stats.region_reclaim_records_scanned
        );
        number_field!(
            "region_reclaim_index_fallbacks",
            self.stats.region_reclaim_index_fallbacks
        );
        number_field!("host_write_bytes", self.stats.host_write_bytes);
        number_field!("admitted_value_bytes", self.stats.admitted_value_bytes);
        number_field!("admitted_disk_turnovers", self.admitted_disk_turnovers());
        number_field!(
            "write_amplification_milli",
            self.stats.write_amplification_milli
        );
        number_field!("bucket_io_in_flight_peak", self.stats.bucket_io_peak);
        number_field!("region_io_in_flight_peak", self.stats.region_io_peak);
        number_field!(
            "write_back_memory_only_puts",
            self.stats.write_back_memory_only_puts
        );
        number_field!("write_back_fallbacks", self.stats.write_back_fallbacks);
        number_field!(
            "write_back_demoted_entries",
            self.stats.write_back_demoted_entries
        );
        number_field!(
            "write_back_demoted_bytes",
            self.stats.write_back_demoted_bytes
        );
        number_field!(
            "write_back_demotion_failures",
            self.stats.write_back_demotion_failures
        );
        number_field!(
            "write_back_lower_absent_evictions",
            self.stats.write_back_lower_absent_evictions
        );
        number_field!(
            "write_back_lower_candidate_evictions",
            self.stats.write_back_lower_candidate_evictions
        );
        number_field!(
            "write_back_synchronous_demotions",
            self.stats.write_back_synchronous_demotions
        );
        number_field!(
            "write_back_dropped_evictions",
            self.stats.write_back_dropped_evictions
        );
        number_field!(
            "write_back_proactive_scheduled",
            self.stats.write_back_proactive_scheduled
        );
        number_field!(
            "write_back_proactive_skipped",
            self.stats.write_back_proactive_skipped
        );
        number_field!(
            "write_back_proactive_persisted",
            self.stats.write_back_proactive_persisted
        );
        number_field!(
            "write_back_proactive_rejected",
            self.stats.write_back_proactive_rejected
        );
        number_field!(
            "write_back_proactive_fatal",
            self.stats.write_back_proactive_fatal
        );
        number_field!(
            "write_back_proactive_invalidated",
            self.stats.write_back_proactive_invalidated
        );
        raw_field!(
            "write_back_volatile_loss_pending",
            self.stats.write_back_volatile_loss_pending
        );
        number_field!(
            "write_back_pending_entries",
            self.stats.write_back_pending_entries
        );
        number_field!(
            "write_back_pending_entries_peak",
            self.stats.write_back_pending_entries_peak
        );
        number_field!(
            "write_back_pending_bytes",
            self.stats.write_back_pending_bytes
        );
        number_field!(
            "write_back_pending_bytes_peak",
            self.stats.write_back_pending_bytes_peak
        );
        number_field!(
            "write_back_pending_lookup_misses",
            self.stats.write_back_pending_lookup_misses
        );
        number_field!(
            "write_back_pending_same_key_waits",
            self.stats.write_back_pending_same_key_waits
        );
        number_field!(
            "write_back_pending_same_key_wait_ns",
            self.stats.write_back_pending_same_key_wait_ns
        );
        number_field!(
            "write_back_queue_rejections",
            self.stats.write_back_queue_rejections
        );
        number_field!(
            "write_back_worker_panics",
            self.stats.write_back_worker_panics
        );
        number_field!(
            "write_back_queue_in_flight_peak",
            self.stats.write_back_queue_peak
        );
        number_field!(
            "write_back_queue_submitted",
            self.stats.write_back_queue_submitted
        );
        number_field!(
            "write_back_queue_completed",
            self.stats.write_back_queue_completed
        );
        number_field!(
            "write_back_queue_wait_ns",
            self.stats.write_back_queue_wait_ns
        );
        number_field!(
            "total_bucket_io_submitted",
            self.total_stats.bucket_io_submitted
        );
        number_field!(
            "total_bucket_io_completed",
            self.total_stats.bucket_io_completed
        );
        number_field!(
            "total_region_io_submitted",
            self.total_stats.region_io_submitted
        );
        number_field!(
            "total_region_io_completed",
            self.total_stats.region_io_completed
        );
        number_field!(
            "total_region_staging_chunk_bytes",
            self.total_stats.region_staging_chunk_bytes
        );
        number_field!(
            "total_region_staging_resident_bytes",
            self.total_stats.region_staging_resident_bytes
        );
        number_field!(
            "total_region_staging_flushing_bytes",
            self.total_stats.region_staging_flushing_bytes
        );
        number_field!(
            "total_region_staging_sealed_spans",
            self.total_stats.region_staging_sealed_spans
        );
        number_field!(
            "total_region_staging_sealed_bytes",
            self.total_stats.region_staging_sealed_bytes
        );
        number_field!(
            "total_region_staging_completion_live_records",
            self.total_stats.region_staging_completion_live_records
        );
        number_field!(
            "total_region_staging_completion_live_bytes",
            self.total_stats.region_staging_completion_live_bytes
        );
        number_field!(
            "total_region_staging_completion_obsolete_records",
            self.total_stats.region_staging_completion_obsolete_records
        );
        number_field!(
            "total_region_staging_completion_obsolete_bytes",
            self.total_stats.region_staging_completion_obsolete_bytes
        );
        number_field!("total_bucket_io_errors", self.total_stats.bucket_io_errors);
        number_field!("total_region_io_errors", self.total_stats.region_io_errors);
        number_field!("total_request_wait_ns", self.total_stats.request_wait_ns);
        number_field!(
            "total_bucket_io_submit_wait_ns",
            self.total_stats.bucket_io_submit_wait_ns
        );
        number_field!(
            "total_bucket_io_completion_ns",
            self.total_stats.bucket_io_completion_ns
        );
        number_field!(
            "total_bucket_io_completion_avg_us",
            average_us(
                self.total_stats.bucket_io_completion_ns,
                self.total_stats.bucket_io_completed
            )
        );
        number_field!(
            "total_bucket_page_buffer_wait_ns",
            self.total_stats.bucket_page_buffer_wait_ns
        );
        number_field!(
            "total_region_backpressure_wait_ns",
            self.total_stats.region_backpressure_wait_ns
        );
        number_field!(
            "total_region_read_queue_wait_ns",
            self.total_stats.region_read_queue_wait_ns
        );
        number_field!(
            "total_region_write_queue_wait_ns",
            self.total_stats.region_write_queue_wait_ns
        );
        number_field!(
            "total_region_control_queue_wait_ns",
            self.total_stats.region_control_queue_wait_ns
        );
        number_field!(
            "total_region_read_buffer_wait_ns",
            self.total_stats.region_read_buffer_wait_ns
        );
        number_field!(
            "total_region_write_buffer_wait_ns",
            self.total_stats.region_write_buffer_wait_ns
        );
        number_field!(
            "total_region_control_buffer_wait_ns",
            self.total_stats.region_control_buffer_wait_ns
        );
        number_field!(
            "total_region_metadata_buffer_wait_ns",
            self.total_stats.region_metadata_buffer_wait_ns
        );
        number_field!(
            "total_region_io_submit_wait_ns",
            self.total_stats.region_io_submit_wait_ns
        );
        number_field!(
            "total_region_io_completion_ns",
            self.total_stats.region_io_completion_ns
        );
        number_field!(
            "total_region_io_completion_avg_us",
            average_us(
                self.total_stats.region_io_completion_ns,
                self.total_stats.region_io_completed
            )
        );
        number_field!(
            "total_write_back_queue_submitted",
            self.total_stats.write_back_queue_submitted
        );
        number_field!(
            "total_write_back_queue_completed",
            self.total_stats.write_back_queue_completed
        );
        number_field!(
            "total_write_back_queue_wait_ns",
            self.total_stats.write_back_queue_wait_ns
        );
        number_field!("total_region_reuses", self.total_stats.region_reuses);
        number_field!(
            "total_region_background_reclaims",
            self.total_stats.region_background_reclaims
        );
        number_field!(
            "total_region_reclaim_records_scanned",
            self.total_stats.region_reclaim_records_scanned
        );
        number_field!(
            "total_region_reclaim_index_fallbacks",
            self.total_stats.region_reclaim_index_fallbacks
        );
        number_field!("total_bucket_io_qd_peak", self.total_stats.bucket_io_peak);
        number_field!("total_region_io_qd_peak", self.total_stats.region_io_peak);
        number_field!(
            "total_write_back_demoted_entries",
            self.total_stats.write_back_demoted_entries
        );
        number_field!(
            "total_write_back_demoted_bytes",
            self.total_stats.write_back_demoted_bytes
        );
        number_field!(
            "total_write_back_queue_qd_peak",
            self.total_stats.write_back_queue_peak
        );
        number_field!("total_host_write_bytes", self.total_stats.host_write_bytes);
        number_field!("capacity_turnovers", self.capacity_turnovers());
        number_field!("total_capacity_turnovers", self.total_capacity_turnovers());
        number_field!("journal_rollovers", self.total_stats.journal_rollovers);
        number_field!(
            "journal_rollover_max_ms",
            self.total_stats.journal_rollover_max_ns as f64 / 1_000_000.0
        );
        number_field!(
            "journal_group_commit_batches",
            self.total_stats.journal_commit_batches
        );
        number_field!(
            "journal_group_commit_records",
            self.total_stats.journal_commit_records
        );
        number_field!(
            "journal_durability_syncs",
            self.total_stats.journal_durability_syncs
        );
        number_field!(
            "journal_sync_elapsed_ms_total",
            self.total_stats.journal_sync_elapsed_ns_total as f64 / 1_000_000.0
        );
        number_field!(
            "journal_sync_elapsed_ms_max",
            self.total_stats.journal_sync_elapsed_ns_max as f64 / 1_000_000.0
        );
        number_field!(
            "journal_group_commit_rejected",
            self.total_stats.journal_commit_rejected
        );
        number_field!(
            "journal_group_commit_worker_panics",
            self.total_stats.journal_commit_worker_panics
        );
        number_field!(
            "journal_group_commit_qd_peak",
            self.total_stats.journal_commit_queue_peak
        );
        number_field!("final_dirty_entries", self.final_stats.memory_dirty_entries);
        number_field!("final_dirty_bytes", self.final_stats.memory_dirty_bytes);
        number_field!(
            "final_journal_used_bytes",
            self.final_stats.journal_used_bytes
        );
        number_field!("drain_host_write_bytes", self.drain_stats.host_write_bytes);
        number_field!(
            "drain_region_bytes_written",
            self.drain_stats.region_bytes_written
        );
        number_field!(
            "drain_write_back_demoted_entries",
            self.drain_stats.write_back_demoted_entries
        );
        number_field!(
            "drain_synchronous_demotions",
            self.drain_stats.write_back_synchronous_demotions
        );
        number_field!("drain_close_ms", self.drain.as_secs_f64() * 1000.0);
        number_field!("clean_reopen_ms", self.reopen.as_secs_f64() * 1000.0);
        number_field!(
            "clean_reopen_verify_ms",
            self.reopen_verify.as_secs_f64() * 1000.0
        );
        number_field!(
            "clean_reopen_close_ms",
            self.reopen_close.as_secs_f64() * 1000.0
        );
        number_field!("clean_reopen_samples", self.reopen_verification.samples);
        number_field!(
            "clean_reopen_expected_live",
            self.reopen_verification.expected_live
        );
        number_field!("clean_reopen_live_hits", self.reopen_verification.live_hits);
        number_field!(
            "clean_reopen_live_misses",
            self.reopen_verification.live_misses
        );
        number_field!(
            "clean_reopen_absent_verified",
            self.reopen_verification.absent_verified
        );
        raw_field!(
            "min_ops_per_sec",
            optional_f64(self.options.min_ops_per_sec)
        );
        raw_field!("max_p99_us", optional_f64(self.options.max_p99_us));
        raw_field!(
            "min_hit_percent",
            optional_f64(self.options.min_hit_percent)
        );
        number_field!("min_journal_rollovers", self.options.min_journal_rollovers);
        number_field!(
            "steady_state_fill_turnovers",
            self.options.steady_state_fill_turnovers
        );
        number_field!(
            "min_capacity_turnovers",
            self.options.min_capacity_turnovers
        );
        number_field!(
            "min_logical_keyspace_turnovers",
            self.options.min_logical_keyspace_turnovers
        );
        number_field!("min_disk_qd_peak", self.options.min_disk_qd_peak);
        number_field!(
            "min_write_back_qd_peak",
            self.options.min_write_back_qd_peak
        );
        raw_field!(
            "max_journal_rollover_ms",
            optional_f64(self.options.max_journal_rollover_ms)
        );
        raw_field!("max_close_ms", optional_f64(self.options.max_close_ms));
        write!(
            output,
            "\"acceptance_passed\":{}}}",
            self.acceptance_failures().is_empty()
        )
        .expect("writing JSON into a String cannot fail");
        output
    }

    fn to_openmetrics(&self) -> String {
        let mut output = self.final_stats.to_openmetrics();
        if let Some(prefix) = output.strip_suffix("# EOF\n") {
            output.truncate(prefix.len());
        }
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_acceptance_passed gauge\ncache_rs_hybrid_bench_acceptance_passed {}",
            u8::from(self.acceptance_failures().is_empty())
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_drain_host_write_bytes gauge\ncache_rs_hybrid_bench_drain_host_write_bytes {}",
            self.drain_stats.host_write_bytes
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_drain_synchronous_demotions gauge\ncache_rs_hybrid_bench_drain_synchronous_demotions {}",
            self.drain_stats.write_back_synchronous_demotions
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_capacity_turnovers gauge\ncache_rs_hybrid_bench_capacity_turnovers {:.6}",
            self.capacity_turnovers()
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_premeasure_capacity_turnovers gauge\ncache_rs_hybrid_bench_premeasure_capacity_turnovers {:.6}",
            self.premeasure_capacity_turnovers()
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_premeasure_region_reuses gauge\ncache_rs_hybrid_bench_premeasure_region_reuses {}",
            self.premeasure_stats.region_reuses
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_steady_state_required_region_reuses gauge\ncache_rs_hybrid_bench_steady_state_required_region_reuses {}",
            self.required_region_reuses
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_steady_state_gate_passed gauge\ncache_rs_hybrid_bench_steady_state_gate_passed {}",
            u8::from(steady_state_gate_ready(
                self.options,
                &self.premeasure_stats,
                self.required_region_reuses,
            ))
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_logical_keyspace_turnovers gauge\ncache_rs_hybrid_bench_logical_keyspace_turnovers {:.6}",
            self.logical_keyspace_turnovers()
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_recent_memory_read_percent gauge\ncache_rs_hybrid_bench_recent_memory_read_percent {:.6}",
            self.recent_memory_read_percent()
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_historical_memory_read_percent gauge\ncache_rs_hybrid_bench_historical_memory_read_percent {:.6}",
            self.historical_memory_read_percent()
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_journal_rollover_max_milliseconds gauge\ncache_rs_hybrid_bench_journal_rollover_max_milliseconds {:.6}",
            self.total_stats.journal_rollover_max_ns as f64 / 1_000_000.0
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_drain_close_milliseconds gauge\ncache_rs_hybrid_bench_drain_close_milliseconds {:.6}",
            self.drain.as_secs_f64() * 1000.0
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_clean_reopen_milliseconds gauge\ncache_rs_hybrid_bench_clean_reopen_milliseconds {:.6}",
            self.reopen.as_secs_f64() * 1000.0
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_clean_reopen_samples gauge\ncache_rs_hybrid_bench_clean_reopen_samples {}",
            self.reopen_verification.samples
        )
        .expect("writing OpenMetrics into a String cannot fail");
        writeln!(
            output,
            "# TYPE cache_rs_hybrid_bench_clean_reopen_live_hits gauge\ncache_rs_hybrid_bench_clean_reopen_live_hits {}",
            self.reopen_verification.live_hits
        )
        .expect("writing OpenMetrics into a String cannot fail");
        output.push_str(
            "# TYPE cache_rs_hybrid_bench_hardware_qualification gauge\ncache_rs_hybrid_bench_hardware_qualification 0\n# EOF\n",
        );
        output
    }

    fn print_human(&self) {
        println!("cache-rs Hybrid fixed/mixed-object benchmark");
        println!("  hardware qualification: no (single run; target sign-off external)");
        println!("  size mix:               {}", self.options.mix.as_spec());
        println!(
            "  access/window/hot reads: {}/{} keys/{}%",
            self.options.access_pattern.as_str(),
            percentage_count(self.options.keys, self.options.temporal_window_percent).max(1),
            self.options.temporal_hot_read_percent
        );
        println!(
            "  keys/generator memory:  {} / {}/{} B",
            self.options.keys, self.generator_planned_memory, self.options.generator_memory_budget
        );
        println!(
            "  prefill workers/time:   {} / {:.3}s",
            self.options.prefill_concurrency,
            self.prefill_elapsed.as_secs_f64()
        );
        println!(
            "  premeasure fill/gate:    {:.3}s + {:.3}s / {:.3}x target, {:.3}x observed, Region reuse={}/{} ({})",
            self.options.warmup.as_secs_f64(),
            self.steady_state_fill_phase.elapsed.as_secs_f64(),
            self.options.steady_state_fill_turnovers,
            self.premeasure_capacity_turnovers(),
            self.premeasure_stats.region_reuses,
            self.required_region_reuses,
            if steady_state_gate_ready(
                self.options,
                &self.premeasure_stats,
                self.required_region_reuses,
            ) {
                "pass"
            } else {
                "FAIL"
            }
        );
        println!(
            "  read/concurrency/QD/BP:  {}% / {} / {} / {}",
            self.options.read_percent,
            self.options.concurrency,
            self.options.queue_depth,
            self.options.backpressure.as_str()
        );
        println!(
            "  API/engine/mode/write:   {}/{}/{}/{}",
            self.options.api.as_str(),
            self.options.engine.as_str(),
            self.options.mode.as_str(),
            write_mode_name(self.options.write_mode)
        );
        println!(
            "  client completion:       {}",
            self.options.api.client_completion_model()
        );
        println!("  throughput:              {:.0} ops/s", self.ops_per_sec());
        println!(
            "  latency p50/p99/p99.9:   {:.1}/{:.1}/{:.1} us",
            self.phase.latency.percentile(500) as f64 / 1000.0,
            self.p99_us(),
            self.phase.latency.percentile(999) as f64 / 1000.0
        );
        println!(
            "  read/write p99:          {:.1}/{:.1} us; {:.1}/{:.1} MiB/s",
            self.phase.read_latency.percentile(990) as f64 / 1000.0,
            self.phase.write_latency.percentile(990) as f64 / 1000.0,
            rate(self.phase.read_bytes, self.phase.elapsed) / (1024.0 * 1024.0),
            rate(self.phase.write_bytes, self.phase.elapsed) / (1024.0 * 1024.0)
        );
        println!(
            "  tier read p99 M/B/R/miss:{:.1}/{:.1}/{:.1}/{:.1} us",
            self.phase.memory_read_latency.percentile(990) as f64 / 1000.0,
            self.phase.bucket_read_latency.percentile(990) as f64 / 1000.0,
            self.phase.region_read_latency.percentile(990) as f64 / 1000.0,
            self.phase.miss_read_latency.percentile(990) as f64 / 1000.0
        );
        println!(
            "  hit rate/tier hits:      {:.3}% / memory={} bucket={} Region={}",
            self.hit_percent(),
            self.stats.memory_hits,
            self.stats.bucket_hits,
            self.stats.region_hits
        );
        if self.options.access_pattern == AccessPattern::Temporal {
            println!(
                "  recent hit/memory read:  {:.3}%/{:.3}% (reads={})",
                self.recent_hit_percent(),
                self.recent_memory_read_percent(),
                self.phase.recent_reads
            );
            println!(
                "  history hit/memory read: {:.3}%/{:.3}% (reads={})",
                self.historical_hit_percent(),
                self.historical_memory_read_percent(),
                self.phase.historical_reads
            );
            println!(
                "  timeline start/end/turn: {}/{} / {:.3}x (wraps={})",
                self.phase.timeline_start,
                self.phase.timeline_end,
                self.logical_keyspace_turnovers(),
                self.timeline_wraps_crossed()
            );
        }
        println!(
            "  outcomes:                stored={} remove={} ttl={} cross={} rejected={} stale={} errors={}",
            self.phase.stored,
            self.phase.removes,
            self.phase.ttl_puts,
            self.phase.cross_tier_updates,
            self.phase.rejected,
            self.phase.stale_values,
            self.phase.errors
        );
        if let Some(error) = &self.phase.first_error {
            println!("  first error:             {error}");
        }
        println!(
            "  host write/value/WA:     {}/{} B / {:.3}x",
            self.stats.host_write_bytes,
            self.stats.admitted_value_bytes,
            self.stats.write_amplification_milli as f64 / 1000.0
        );
        println!(
            "  total I/O sub/complete:  Bucket {}/{}; Region {}/{}; QD peak {}/{}",
            self.total_stats.bucket_io_submitted,
            self.total_stats.bucket_io_completed,
            self.total_stats.region_io_submitted,
            self.total_stats.region_io_completed,
            self.total_stats.bucket_io_peak,
            self.total_stats.region_io_peak
        );
        println!(
            "  wait measure req/WB/Bbuf/Rbp: {:.3}/{:.3}/{:.3}/{:.3} ms",
            self.stats.request_wait_ns as f64 / 1_000_000.0,
            self.stats.write_back_queue_wait_ns as f64 / 1_000_000.0,
            self.stats.bucket_page_buffer_wait_ns as f64 / 1_000_000.0,
            self.stats.region_backpressure_wait_ns as f64 / 1_000_000.0,
        );
        println!(
            "  Region wait queue R/W/C: {:.3}/{:.3}/{:.3} ms; buffer R/W/C/M: {:.3}/{:.3}/{:.3}/{:.3} ms",
            self.stats.region_read_queue_wait_ns as f64 / 1_000_000.0,
            self.stats.region_write_queue_wait_ns as f64 / 1_000_000.0,
            self.stats.region_control_queue_wait_ns as f64 / 1_000_000.0,
            self.stats.region_read_buffer_wait_ns as f64 / 1_000_000.0,
            self.stats.region_write_buffer_wait_ns as f64 / 1_000_000.0,
            self.stats.region_control_buffer_wait_ns as f64 / 1_000_000.0,
            self.stats.region_metadata_buffer_wait_ns as f64 / 1_000_000.0,
        );
        println!(
            "  I/O measure submit/complete: Bucket {:.1}/{:.1} us; Region {:.1}/{:.1} us",
            average_us(
                self.stats.bucket_io_submit_wait_ns,
                self.stats.bucket_io_submitted,
            ),
            average_us(
                self.stats.bucket_io_completion_ns,
                self.stats.bucket_io_completed,
            ),
            average_us(
                self.stats.region_io_submit_wait_ns,
                self.stats.region_io_submitted,
            ),
            average_us(
                self.stats.region_io_completion_ns,
                self.stats.region_io_completed,
            ),
        );
        println!(
            "  write-back put/fallback/demotion/reject: {}/{}/{}/{}",
            self.stats.write_back_memory_only_puts,
            self.stats.write_back_fallbacks,
            self.stats.write_back_demoted_entries,
            self.stats.write_back_queue_rejections
        );
        println!(
            "  proactive scheduled/skipped/persisted/rejected/fatal/invalidate: {}/{}/{}/{}/{}/{}",
            self.stats.write_back_proactive_scheduled,
            self.stats.write_back_proactive_skipped,
            self.stats.write_back_proactive_persisted,
            self.stats.write_back_proactive_rejected,
            self.stats.write_back_proactive_fatal,
            self.stats.write_back_proactive_invalidated
        );
        println!(
            "  volatile loss pending: {}",
            self.stats.write_back_volatile_loss_pending
        );
        println!(
            "  eviction absent/candidate/sync/drop: {}/{}/{}/{}",
            self.stats.write_back_lower_absent_evictions,
            self.stats.write_back_lower_candidate_evictions,
            self.stats.write_back_synchronous_demotions,
            self.stats.write_back_dropped_evictions
        );
        println!(
            "  pending current/peak/masked/waits: {}/{} entries; {} misses; {} waits ({:.3} ms)",
            self.stats.write_back_pending_entries,
            self.stats.write_back_pending_entries_peak,
            self.stats.write_back_pending_lookup_misses,
            self.stats.write_back_pending_same_key_waits,
            self.stats.write_back_pending_same_key_wait_ns as f64 / 1_000_000.0
        );
        let region_records = self
            .stats
            .region_write_batches
            .saturating_add(self.stats.region_records_coalesced);
        println!(
            "  Region batches/records/avg: {}/{} / {:.1} KiB per write",
            self.stats.region_write_batches,
            region_records,
            if self.stats.region_write_batches == 0 {
                0.0
            } else {
                self.stats.region_bytes_written as f64
                    / self.stats.region_write_batches as f64
                    / 1024.0
            }
        );
        println!(
            "  Region staging spans/fill/obsolete: {} / {:.1}% / {:.1}% records, {:.1}% bytes (resident/flushing={}/{} KiB)",
            self.stats.region_staging_sealed_spans,
            percent_of(
                self.stats.region_staging_sealed_bytes,
                self.stats
                    .region_staging_sealed_spans
                    .saturating_mul(self.stats.region_staging_chunk_bytes),
            ),
            percent_of(
                self.stats.region_staging_completion_obsolete_records,
                self.stats
                    .region_staging_completion_live_records
                    .saturating_add(self.stats.region_staging_completion_obsolete_records),
            ),
            percent_of(
                self.stats.region_staging_completion_obsolete_bytes,
                self.stats
                    .region_staging_completion_live_bytes
                    .saturating_add(self.stats.region_staging_completion_obsolete_bytes),
            ),
            self.stats.region_staging_resident_bytes / 1024,
            self.stats.region_staging_flushing_bytes / 1024,
        );
        println!(
            "  total WB demotion/QD:    {} entries / {}",
            self.total_stats.write_back_demoted_entries, self.total_stats.write_back_queue_peak
        );
        println!(
            "  journal rollover/max:    {} / {:.3} ms (groups/sync={}/{} records={} QD={})",
            self.total_stats.journal_rollovers,
            self.total_stats.journal_rollover_max_ns as f64 / 1_000_000.0,
            self.total_stats.journal_commit_batches,
            self.total_stats.journal_durability_syncs,
            self.total_stats.journal_commit_records,
            self.total_stats.journal_commit_queue_peak
        );
        println!(
            "  physical measured/total: {:.3}x / {:.3}x",
            self.capacity_turnovers(),
            self.total_capacity_turnovers()
        );
        println!(
            "  Region reuse measure/total: {} / {}",
            self.stats.region_reuses, self.total_stats.region_reuses
        );
        println!(
            "  Region background reclaim: {} / {} (records={} fallback={})",
            self.stats.region_background_reclaims,
            self.total_stats.region_background_reclaims,
            self.stats.region_reclaim_records_scanned,
            self.stats.region_reclaim_index_fallbacks,
        );
        println!(
            "  logical/admitted turn:   {:.3}x / {:.3}x",
            self.logical_ingest_turnovers(),
            self.admitted_disk_turnovers()
        );
        println!(
            "  drain/close:             {:.3} ms; {} MiB host writes; {} sync demotions",
            self.drain.as_secs_f64() * 1000.0,
            self.drain_stats.host_write_bytes / (1024 * 1024),
            self.drain_stats.write_back_synchronous_demotions,
        );
        println!(
            "  reopen/verify/close:     {:.3}/{:.3}/{:.3} ms; samples={} live hit/miss={}/{} absent={}",
            self.reopen.as_secs_f64() * 1000.0,
            self.reopen_verify.as_secs_f64() * 1000.0,
            self.reopen_close.as_secs_f64() * 1000.0,
            self.reopen_verification.samples,
            self.reopen_verification.live_hits,
            self.reopen_verification.live_misses,
            self.reopen_verification.absent_verified,
        );
        println!(
            "  acceptance:              {}",
            if self.acceptance_failures().is_empty() {
                "pass"
            } else {
                "FAIL"
            }
        );
    }
}

fn required<T>(value: Option<T>, name: &str) -> Result<T, String> {
    value.ok_or_else(|| format!("{name} is required"))
}

fn validate_memory(name: &str, value: usize) -> Result<(), String> {
    let value = u64::try_from(value).map_err(|_| format!("{name} does not fit u64"))?;
    validate_range(name, value, 1, MAX_TOOL_MEMORY_BYTES)
}

fn parse_bytes(value: &str, name: &str) -> Result<u64, String> {
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '_')
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    if number.is_empty() {
        return Err(format!("invalid byte value {value:?} for {name}"));
    }
    let number = number
        .replace('_', "")
        .parse::<u64>()
        .map_err(|_| format!("invalid byte value {value:?} for {name}"))?;
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "k" | "kb" | "kib" => 1024,
        "m" | "mb" | "mib" => 1024 * 1024,
        "g" | "gb" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" | "tib" => 1024_u64 * 1024 * 1024 * 1024,
        _ => return Err(format!("invalid byte suffix in {value:?} for {name}")),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte value for {name} is too large"))
}

fn parse_usize_bytes(value: &str, name: &str) -> Result<usize, String> {
    usize::try_from(parse_bytes(value, name)?)
        .map_err(|_| format!("byte value for {name} does not fit this platform"))
}

fn parse_duration(value: &str, name: &str, allow_zero: bool) -> Result<Duration, String> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| format!("invalid seconds {value:?} for {name}"))?;
    if !seconds.is_finite() || seconds < 0.0 || (!allow_zero && seconds == 0.0) {
        return Err(format!(
            "{name} must be {}",
            if allow_zero {
                "non-negative"
            } else {
                "positive"
            }
        ));
    }
    if seconds > 24.0 * 60.0 * 60.0 {
        return Err(format!("{name} must not exceed 24 hours"));
    }
    Ok(Duration::from_secs_f64(seconds))
}

fn parse_positive(value: &str, name: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|_| format!("invalid value for {name}"))?;
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(format!("{name} must be a finite positive number"))
    }
}

fn parse_non_negative(value: &str, name: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|_| format!("invalid value for {name}"))?;
    if value.is_finite() && value >= 0.0 {
        Ok(value)
    } else {
        Err(format!("{name} must be a finite non-negative number"))
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn lock_mutex<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn parse_percent(value: &str, name: &str) -> Result<f64, String> {
    let value = value
        .parse::<f64>()
        .map_err(|_| format!("invalid value for {name}"))?;
    if value.is_finite() && (0.0..=100.0).contains(&value) {
        Ok(value)
    } else {
        Err(format!("{name} must be in 0..=100"))
    }
}

fn default_index_slots(keys: usize) -> Result<usize, String> {
    let doubled = keys
        .checked_mul(2)
        .ok_or_else(|| "--keys is too large to size the Region index".to_owned())?;
    Ok(doubled
        .checked_next_power_of_two()
        .unwrap_or(256 * 1024 * 1024)
        .clamp(1024, 256 * 1024 * 1024))
}

fn average_us(total_ns: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total_ns as f64 / count as f64 / 1000.0
    }
}

fn percent_of(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 * 100.0 / denominator as f64
    }
}

fn hit_percent(hits: u64, misses: u64) -> f64 {
    percent_of(hits, hits.saturating_add(misses))
}

fn amplification_milli(host_bytes: u64, admitted_bytes: u64) -> u64 {
    if admitted_bytes == 0 {
        return 0;
    }
    let scaled = u128::from(host_bytes).saturating_mul(1000) / u128::from(admitted_bytes);
    u64::try_from(scaled).unwrap_or(u64::MAX)
}

fn write_mode_name(mode: HybridWriteMode) -> &'static str {
    match mode {
        HybridWriteMode::WriteThrough => "write_through",
        HybridWriteMode::WriteBack => "write_back",
    }
}

fn optional_f64(value: Option<f64>) -> String {
    value.map_or_else(|| "null".into(), |value| format!("{value:.3}"))
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn print_help() {
    println!(
        r#"cache-bench hybrid — fixed/mixed-size Memory + Bucket + Region benchmark

Usage:
  cache-bench hybrid --bucket-path PATH --bucket-capacity BYTES \
    --region-path PATH --region-capacity BYTES --manifest-path PATH \
    --memory-capacity BYTES --yes [options]

Safety: all three paths must be distinct, missing or empty regular files.
The benchmark never resets an existing cache. --yes is mandatory.

Workload:
  --sizes SIZE:WEIGHT,...     One fixed class or up to 16 mixed classes
                             (default 256:50,4KiB:30,64KiB:20)
  --small-object-max BYTES    Bucket routing threshold (default 1KiB)
  --keys 1..100000000        Streamed key population (default 100000)
  --generator-memory-budget B Hard bound for version state, scratch, worker stacks (default 2GiB)
  --read-percent 0..100      Read ratio (default 80)
  --access-pattern uniform|temporal (default uniform)
  --temporal-window-percent 1..100 Recent key window (default 5)
  --temporal-hot-read-percent 0..100 Reads aimed at recent window (default 90)
  --prefill-percent 0..100   Initial population (default 100)
  --prefill-concurrency 1..128 Bounded prefill workers (default min(concurrency,128))
  --verify-samples 1..1000000 Sampled prefill version checks (default 10000)
  --remove-percent 0..100    Share of mutations (default 10)
  --ttl-percent 0..100       Share of mutations (default 5)
  --cross-tier-percent 0..100 Same-key small/large update share (default 20;
                             must be 0 for a single-tier size set)
  --ttl-ms 1..60000          TTL mutation lifetime (default 100)
  --concurrency 1..4096      Caller concurrency (default 16)
  --queue-depth 1..4096      Both lower I/O queue bounds (default 128)
  --backpressure block|reject Bounded saturation policy (default block)
  --warmup-secs SECONDS      0..86400 (default 5)
  --steady-state-fill-turnovers N Continue the workload before measurement until
                             pre-measure host writes / disk capacity reach N and
                             one complete Region reuse cycle is observed
                             (default 0/off)
  --steady-state-fill-max-secs S Maximum added fill time (default 3600)
  --duration-secs SECONDS    >0..86400 (default 30)

Engine/layout:
  --bucket-size BYTES        4KiB..64KiB power of two
  --region-size BYTES        Region size (default 32MiB)
  --bucket-memory-budget B   Default 1GiB
  --region-memory-budget B   Default 1GiB
  --hybrid-memory-budget B   Optional aggregate bound
  --journal-capacity BYTES   4KiB-aligned, 64KiB..64TiB (default 16MiB)
  --append-lanes 1..8        Default 2
  --write-mode write-through|write-back (default write-back)
  --write-back-queue-depth 1..4096 (default 64)
  --write-back-workers 1..128 (default 4; must not exceed queue depth)
  --write-back-memory BYTES  Reserved demotion bytes (default 32MiB)
  --api sync|async           Default async; one blocking request/client
  --engine sync|auto|uring   Applied to both disk tiers (default auto)
  --mode buffered|auto|direct Applied to both disk tiers (default buffered)

Output/gates:
  --output human|json|openmetrics (default json)
  --min-ops-per-sec N
  --max-p99-us N
  --min-hit-percent N
  --min-journal-rollovers N  Require journal rollover activity
  --min-capacity-turnovers N Require measured host writes / disk capacity
  --min-logical-keyspace-turnovers N Require temporal write-head advances / key count
  --min-disk-qd-peak N       Required on each active disk tier (default 1)
  --min-write-back-qd-peak N Required for write-back runs (default 1)
  --max-journal-rollover-ms N
  --max-close-ms N

Temporal mode treats the key population as a ring: successful put classes advance
a shared write head while reads select the recent or historical generation window.
All outputs record version-stale checks, age-window/tier hits, logical and physical
turnover, pre-measure steady-state gate/Region reuse, disk-tier I/O,
write-back demotion/QD, journal rollover/max latency, and
drain/close latency. JSON
deliberately records hardware_qualification=false and every external hardware
sign-off field false. A passing run is not NVMe soak, thermal, DWPD, power-loss,
or canary sign-off.
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_mix_is_deterministic_and_bounded() {
        let mix = SizeMix::parse("128:1,4KiB:2,64KiB:3").unwrap();
        assert_eq!(mix.maximum_bytes(), 64 * 1024);
        assert_eq!(mix.class_for_key(42), mix.class_for_key(42));
        assert_eq!(mix.routing_classes(1024), Some((0, 1)));
        assert!(mix.uses_bucket(1024));
        assert!(mix.uses_region(1024));
        let fixed = SizeMix::parse("256:1").unwrap();
        assert!(fixed.uses_bucket(1024));
        assert!(!fixed.uses_region(1024));
        assert!(SizeMix::parse("0:1").is_err());
        assert!(SizeMix::parse("1:0").is_err());
    }

    #[test]
    fn streamed_keys_and_versioned_values_detect_stale_data() {
        let keys = KeySpace::new(MAX_KEYS, DEFAULT_SEED);
        assert_eq!(keys.key(99_999_999), keys.key(99_999_999));
        assert_ne!(keys.key(0), keys.key(99_999_999));
        assert!(size_of::<KeySpace>() <= 2 * size_of::<u64>());

        let mix = SizeMix::parse("256:1,4KiB:1").unwrap();
        let mut value = Vec::new();
        prepare_value_buffer(&mut value, mix.maximum_bytes()).unwrap();
        let length = fill_value(&mut value, &mix, 1, 41, 7).unwrap();
        assert!(validate_value(&value[..length], &mix, 1, 41, 7));
        assert!(!validate_value(&value[..length], &mix, 1, 41, 6));
        assert!(!validate_value(&value[..length], &mix, 1, 42, 7));
        value[VALUE_HEADER_BYTES] ^= 1;
        assert!(!validate_value(&value[..length], &mix, 1, 41, 7));
    }

    #[test]
    fn prepared_value_template_preserves_the_pattern_through_a_partial_tail() {
        let mix = SizeMix::parse("53:1").unwrap();
        let mut value = Vec::new();
        let length = fill_value(&mut value, &mix, 0, 41, 7).unwrap();

        for (offset, byte) in value[VALUE_HEADER_BYTES..length]
            .iter()
            .copied()
            .enumerate()
        {
            assert_eq!(byte, value_pattern_byte(VALUE_PATTERN_SEED, offset));
        }
        assert!(validate_value(&value[..length], &mix, 0, 41, 7));
    }

    #[test]
    fn mixed_size_write_bytes_use_the_selected_prefix_not_the_template() {
        let mix = SizeMix::parse("256:1,4KiB:1").unwrap();
        let mut value = Vec::new();
        prepare_value_buffer(&mut value, mix.maximum_bytes()).unwrap();
        let small = fill_value(&mut value, &mix, 0, 1, 1).unwrap();
        assert_eq!(small, 256);
        assert_eq!(value.len(), 4 * 1024);

        let mut phase = Phase::default();
        phase.record_stored_write(small);
        let large = fill_value(&mut value, &mix, 1, 2, 1).unwrap();
        phase.record_stored_write(large);

        assert_eq!(phase.stored, 2);
        assert_eq!(phase.write_bytes, (256 + 4 * 1024) as u64);
    }

    #[test]
    fn stats_delta_preserves_every_wait_path_as_a_counter() {
        let before_hybrid = HybridCacheStats {
            request_wait_ns: 10,
            write_back: cache_rs::HybridWriteBackStats {
                queue_submitted: 10,
                queue_completed: 10,
                queue_wait_ns: 10,
                ..cache_rs::HybridWriteBackStats::default()
            },
            bucket: cache_rs::BucketCacheStats {
                io_completed: 10,
                io_submit_wait_ns: 10,
                io_completion_ns: 10,
                page_buffer_wait_ns: 10,
                ..cache_rs::BucketCacheStats::default()
            },
            region: cache_rs::CacheStats {
                read_queue_wait_ns: 10,
                write_queue_wait_ns: 20,
                control_queue_wait_ns: 30,
                read_buffer_wait_ns: 40,
                write_buffer_wait_ns: 50,
                control_buffer_wait_ns: 60,
                metadata_buffer_wait_ns: 70,
                backpressure_wait_ns: 280,
                io_completed: 10,
                io_submit_wait_ns: 10,
                io_completion_ns: 10,
                background_regions_reclaimed: 10,
                reclaim_records_scanned: 10,
                reclaim_index_fallbacks: 10,
                ..cache_rs::CacheStats::default()
            },
            ..HybridCacheStats::default()
        };
        let after_hybrid = HybridCacheStats {
            request_wait_ns: 17,
            write_back: cache_rs::HybridWriteBackStats {
                queue_submitted: 17,
                queue_completed: 17,
                queue_wait_ns: 17,
                ..before_hybrid.write_back
            },
            bucket: cache_rs::BucketCacheStats {
                io_completed: 17,
                io_submit_wait_ns: 17,
                io_completion_ns: 17,
                page_buffer_wait_ns: 17,
                ..before_hybrid.bucket
            },
            region: cache_rs::CacheStats {
                read_queue_wait_ns: 11,
                write_queue_wait_ns: 22,
                control_queue_wait_ns: 33,
                read_buffer_wait_ns: 44,
                write_buffer_wait_ns: 55,
                control_buffer_wait_ns: 66,
                metadata_buffer_wait_ns: 77,
                backpressure_wait_ns: 308,
                io_completed: 17,
                io_submit_wait_ns: 17,
                io_completion_ns: 17,
                background_regions_reclaimed: 17,
                reclaim_records_scanned: 17,
                reclaim_index_fallbacks: 17,
                ..before_hybrid.region
            },
            ..before_hybrid
        };
        let mut before_staging = RegionStagingStats::default();
        before_staging.chunk_bytes = 4_096;
        before_staging.resident_bytes = 100;
        before_staging.flushing_bytes = 50;
        before_staging.sealed_spans = 10;
        before_staging.sealed_bytes = 1_000;
        before_staging.completion_live_records = 10;
        before_staging.completion_live_bytes = 500;
        before_staging.completion_obsolete_records = 10;
        before_staging.completion_obsolete_bytes = 500;
        let mut after_staging = RegionStagingStats::default();
        after_staging.chunk_bytes = 8_192;
        after_staging.resident_bytes = 300;
        after_staging.flushing_bytes = 200;
        after_staging.sealed_spans = 17;
        after_staging.sealed_bytes = 1_700;
        after_staging.completion_live_records = 17;
        after_staging.completion_live_bytes = 1_200;
        after_staging.completion_obsolete_records = 13;
        after_staging.completion_obsolete_bytes = 900;
        let before = ObservationStats {
            hybrid: before_hybrid,
            region_staging: before_staging,
        };
        let after = ObservationStats {
            hybrid: after_hybrid,
            region_staging: after_staging,
        };

        let delta = StatsDelta::between(before, after);
        assert_eq!(delta.request_wait_ns, 7);
        assert_eq!(delta.write_back_queue_submitted, 7);
        assert_eq!(delta.write_back_queue_completed, 7);
        assert_eq!(delta.write_back_queue_wait_ns, 7);
        assert_eq!(delta.bucket_io_completed, 7);
        assert_eq!(delta.bucket_io_submit_wait_ns, 7);
        assert_eq!(delta.bucket_io_completion_ns, 7);
        assert_eq!(delta.bucket_page_buffer_wait_ns, 7);
        assert_eq!(delta.region_read_queue_wait_ns, 1);
        assert_eq!(delta.region_write_queue_wait_ns, 2);
        assert_eq!(delta.region_control_queue_wait_ns, 3);
        assert_eq!(delta.region_read_buffer_wait_ns, 4);
        assert_eq!(delta.region_write_buffer_wait_ns, 5);
        assert_eq!(delta.region_control_buffer_wait_ns, 6);
        assert_eq!(delta.region_metadata_buffer_wait_ns, 7);
        assert_eq!(delta.region_backpressure_wait_ns, 28);
        assert_eq!(delta.region_io_completed, 7);
        assert_eq!(delta.region_io_submit_wait_ns, 7);
        assert_eq!(delta.region_io_completion_ns, 7);
        assert_eq!(delta.region_background_reclaims, 7);
        assert_eq!(delta.region_reclaim_records_scanned, 7);
        assert_eq!(delta.region_reclaim_index_fallbacks, 7);
        assert_eq!(delta.region_staging_chunk_bytes, 8_192);
        assert_eq!(delta.region_staging_resident_bytes, 300);
        assert_eq!(delta.region_staging_flushing_bytes, 200);
        assert_eq!(delta.region_staging_sealed_spans, 7);
        assert_eq!(delta.region_staging_sealed_bytes, 700);
        assert_eq!(delta.region_staging_completion_live_records, 7);
        assert_eq!(delta.region_staging_completion_live_bytes, 700);
        assert_eq!(delta.region_staging_completion_obsolete_records, 3);
        assert_eq!(delta.region_staging_completion_obsolete_bytes, 400);
    }

    #[test]
    fn temporal_access_moves_the_recent_window_and_wraps_the_ring() {
        let access = TemporalAccess::new(10, 2, 100, 5);
        let mut random = XorShift64::new(7);
        for _ in 0..32 {
            let (index, band) = access.select_read(&mut random);
            assert_eq!(band, TemporalBand::Recent);
            assert!([3, 4].contains(&index));
        }

        assert_eq!(access.next_write(), 5);
        assert_eq!(access.next_write(), 6);
        assert_eq!(access.head(), 7);
        for _ in 0..8 {
            let _ = access.next_write();
        }
        assert_eq!(access.head(), 15);
        assert_eq!(access.next_write(), 5);

        let (index, band) = access.select_read(&mut random);
        assert_eq!(band, TemporalBand::Recent);
        assert!([4, 5].contains(&index));
    }

    #[test]
    fn temporal_access_keeps_historical_reads_outside_the_recent_window() {
        let access = TemporalAccess::new(10, 2, 0, 10);
        let mut random = XorShift64::new(11);
        for _ in 0..64 {
            let (index, band) = access.select_read(&mut random);
            assert_eq!(band, TemporalBand::Historical);
            assert!(index < 8);
        }
    }

    #[test]
    fn parser_accepts_temporal_workload_and_requires_it_for_logical_turnover_gate() {
        let base = [
            "--bucket-path=bucket",
            "--bucket-capacity=16KiB",
            "--region-path=region",
            "--region-capacity=1GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--access-pattern=temporal",
            "--temporal-window-percent=2",
            "--temporal-hot-read-percent=85",
            "--backpressure=block",
            "--steady-state-fill-turnovers=2",
            "--steady-state-fill-max-secs=900",
            "--min-logical-keyspace-turnovers=1.5",
            "--yes",
        ];
        let ParseOutcome::Run(options) =
            Options::parse(base.iter().map(|value| (*value).to_owned())).unwrap()
        else {
            panic!("expected runnable options");
        };
        assert_eq!(options.access_pattern, AccessPattern::Temporal);
        assert_eq!(options.temporal_window_percent, 2);
        assert_eq!(options.temporal_hot_read_percent, 85);
        assert_eq!(options.backpressure, Backpressure::Block);
        assert_eq!(options.steady_state_fill_turnovers, 2.0);
        assert_eq!(options.steady_state_fill_max, Duration::from_secs(900));
        assert_eq!(options.min_logical_keyspace_turnovers, 1.5);

        let uniform = base
            .iter()
            .filter(|value| !value.starts_with("--access-pattern="))
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        assert!(Options::parse(uniform).is_err());
    }

    #[test]
    fn parser_accepts_fixed_tier_workloads_with_route_aware_capacity() {
        let base = [
            "--bucket-path=bucket",
            "--bucket-capacity=2GiB",
            "--region-path=region",
            "--region-capacity=3GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--sizes=256:1",
            "--cross-tier-percent=0",
            "--write-mode=write-through",
            "--yes",
        ];
        let ParseOutcome::Run(small) =
            Options::parse(base.iter().map(|value| (*value).to_owned())).unwrap()
        else {
            panic!("expected runnable options");
        };
        assert_eq!(active_disk_capacity(&small), 2 * 1024 * 1024 * 1024);
        assert!(small.mix.uses_bucket(small.small_object_max));
        assert!(!small.mix.uses_region(small.small_object_max));
        assert_eq!(
            build_config(&small)
                .unwrap()
                .diagnostics()
                .unwrap()
                .region
                .index_slots,
            1024
        );

        let large_arguments = base
            .iter()
            .map(|value| {
                if value.starts_with("--sizes=") {
                    "--sizes=64KiB:1".to_owned()
                } else {
                    (*value).to_owned()
                }
            })
            .collect::<Vec<_>>();
        let ParseOutcome::Run(large) = Options::parse(large_arguments).unwrap() else {
            panic!("expected runnable options");
        };
        assert_eq!(active_disk_capacity(&large), 3 * 1024 * 1024 * 1024);
        assert!(!large.mix.uses_bucket(large.small_object_max));
        assert!(large.mix.uses_region(large.small_object_max));

        let missing_cross_tier_disable = base
            .iter()
            .filter(|value| !value.starts_with("--cross-tier-percent="))
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        assert!(Options::parse(missing_cross_tier_disable).is_err());
    }

    #[test]
    fn fixed_tier_acceptance_ignores_inactive_disk_tier() {
        let arguments = [
            "--bucket-path=bucket",
            "--bucket-capacity=2GiB",
            "--region-path=region",
            "--region-capacity=3GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--sizes=256:1",
            "--cross-tier-percent=0",
            "--write-mode=write-through",
            "--yes",
        ];
        let ParseOutcome::Run(options) =
            Options::parse(arguments.iter().map(|value| (*value).to_owned())).unwrap()
        else {
            panic!("expected runnable options");
        };
        let report = Report {
            options: &options,
            phase: Phase::default(),
            stats: StatsDelta {
                bucket_io_submitted: 1,
                ..StatsDelta::default()
            },
            drain_stats: StatsDelta::default(),
            total_stats: StatsDelta {
                bucket_io_submitted: 1,
                bucket_io_peak: 1,
                ..StatsDelta::default()
            },
            premeasure_stats: StatsDelta::default(),
            final_stats: HybridCacheStats::default(),
            generator_planned_memory: 1,
            prefill_elapsed: Duration::ZERO,
            premeasure_elapsed: Duration::ZERO,
            steady_state_fill_phase: Phase::default(),
            required_region_reuses: 0,
            drain: Duration::ZERO,
            reopen: Duration::ZERO,
            reopen_verify: Duration::ZERO,
            reopen_close: Duration::ZERO,
            reopen_verification: ReopenVerification::default(),
        };
        assert!(report.acceptance_failures().is_empty());
    }

    #[test]
    fn steady_state_gate_requires_physical_turnover_and_region_reuse() {
        let arguments = [
            "--bucket-path=bucket",
            "--bucket-capacity=16KiB",
            "--region-path=region",
            "--region-capacity=96MiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--append-lanes=1",
            "--steady-state-fill-turnovers=2",
            "--yes",
        ];
        let ParseOutcome::Run(options) =
            Options::parse(arguments.iter().map(|value| (*value).to_owned())).unwrap()
        else {
            panic!("expected runnable options");
        };
        let capacity = options
            .bucket_capacity
            .saturating_add(options.region_capacity);
        let required_reuses = u64::from(
            build_config(&options)
                .unwrap()
                .diagnostics()
                .unwrap()
                .region
                .region_count,
        );
        assert_eq!(required_reuses, 2);
        assert!(!steady_state_gate_ready(
            &options,
            &StatsDelta {
                host_write_bytes: capacity.saturating_mul(2),
                ..StatsDelta::default()
            },
            required_reuses,
        ));
        assert!(!steady_state_gate_ready(
            &options,
            &StatsDelta {
                host_write_bytes: capacity,
                region_reuses: required_reuses,
                ..StatsDelta::default()
            },
            required_reuses,
        ));
        assert!(!steady_state_gate_ready(
            &options,
            &StatsDelta {
                host_write_bytes: capacity.saturating_mul(2),
                region_reuses: required_reuses - 1,
                ..StatsDelta::default()
            },
            required_reuses,
        ));
        assert!(steady_state_gate_ready(
            &options,
            &StatsDelta {
                host_write_bytes: capacity.saturating_mul(2),
                region_reuses: required_reuses,
                ..StatsDelta::default()
            },
            required_reuses,
        ));
    }

    #[test]
    fn parser_supports_one_hundred_million_keys_with_a_hard_generator_budget() {
        let base = [
            "--bucket-path=bucket",
            "--bucket-capacity=16KiB",
            "--region-path=region",
            "--region-capacity=1GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--yes",
            "--keys=100000000",
        ];
        let parsed = Options::parse(base.iter().map(|value| (*value).to_owned())).unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        assert_eq!(options.keys, MAX_KEYS);
        assert!(
            generator_memory_plan(
                options.keys,
                options.concurrency.max(options.prefill_concurrency),
                options.mix.maximum_bytes(),
            )
            .unwrap()
                <= options.generator_memory_budget
        );

        let mut under_budget = base
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        under_budget.push("--generator-memory-budget=64MiB".into());
        assert!(Options::parse(under_budget).is_err());
    }

    #[test]
    fn production_acceptance_enforces_tier_writeback_rollover_and_turnover_gates() {
        let arguments = [
            "--bucket-path=bucket",
            "--bucket-capacity=16KiB",
            "--region-path=region",
            "--region-capacity=1GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
            "--write-mode=write-back",
            "--min-journal-rollovers=1",
            "--steady-state-fill-turnovers=2",
            "--min-capacity-turnovers=2",
            "--min-disk-qd-peak=2",
            "--min-write-back-qd-peak=2",
            "--max-journal-rollover-ms=10",
            "--max-close-ms=10",
            "--yes",
        ];
        let ParseOutcome::Run(options) =
            Options::parse(arguments.iter().map(|value| (*value).to_owned())).unwrap()
        else {
            panic!("expected runnable options");
        };
        let capacity = options
            .bucket_capacity
            .saturating_add(options.region_capacity);
        let required_reuses = u64::from(
            build_config(&options)
                .unwrap()
                .diagnostics()
                .unwrap()
                .region
                .region_count,
        );
        let measured = StatsDelta {
            host_write_bytes: capacity.saturating_mul(2),
            bucket_io_submitted: 1,
            region_io_submitted: 1,
            ..StatsDelta::default()
        };
        let qualifying_premeasure = StatsDelta {
            host_write_bytes: capacity.saturating_mul(2),
            region_reuses: required_reuses,
            ..StatsDelta::default()
        };
        let qualifying_total = StatsDelta {
            bucket_io_submitted: 1,
            region_io_submitted: 1,
            bucket_io_peak: 2,
            region_io_peak: 2,
            host_write_bytes: capacity.saturating_mul(3),
            journal_rollovers: 1,
            journal_rollover_max_ns: 1_000_000,
            journal_commit_records: 1,
            write_back_demoted_entries: 1,
            write_back_queue_peak: 2,
            ..StatsDelta::default()
        };
        let mut phase = Phase::default();
        phase.record_read_tier_latency(ReadLatencyTier::Memory, Duration::from_micros(11));
        phase.record_read_tier_latency(ReadLatencyTier::Bucket, Duration::from_micros(22));
        phase.record_read_tier_latency(ReadLatencyTier::Region, Duration::from_micros(33));
        phase.record_read_tier_latency(ReadLatencyTier::Miss, Duration::from_micros(44));
        let report = Report {
            options: &options,
            phase,
            stats: measured,
            drain_stats: StatsDelta::default(),
            total_stats: qualifying_total,
            premeasure_stats: qualifying_premeasure,
            final_stats: HybridCacheStats {
                bucket: cache_rs::BucketCacheStats {
                    evictions: 1,
                    ..cache_rs::BucketCacheStats::default()
                },
                ..HybridCacheStats::default()
            },
            generator_planned_memory: 1,
            prefill_elapsed: Duration::ZERO,
            premeasure_elapsed: Duration::ZERO,
            steady_state_fill_phase: Phase::default(),
            required_region_reuses: required_reuses,
            drain: Duration::from_millis(1),
            reopen: Duration::ZERO,
            reopen_verify: Duration::ZERO,
            reopen_close: Duration::ZERO,
            reopen_verification: ReopenVerification::default(),
        };
        assert!(report.acceptance_failures().is_empty());
        assert_eq!(report.capacity_turnovers(), 2.0);
        let json = report.to_json();
        assert!(json.contains("\"schema_version\":9"));
        assert!(json.contains("\"client_completion_model\":\"blocking_wait_one_outstanding\""));
        assert!(json.contains("\"latency_scope\":\"individual_cache_api_calls\""));
        assert!(json.contains("\"write_value_generation\":\"prebuilt_worker_template\""));
        assert!(json.contains("\"latency_samples\":0"));
        assert!(json.contains("\"request_wait_ns\":0"));
        assert!(json.contains("\"bucket_io_completion_avg_us\":0"));
        assert!(json.contains("\"region_io_completion_avg_us\":0"));
        assert!(json.contains("\"total_write_back_queue_wait_ns\":0"));
        for field in [
            "region_read_queue_wait_ns",
            "region_write_queue_wait_ns",
            "region_control_queue_wait_ns",
            "region_read_buffer_wait_ns",
            "region_write_buffer_wait_ns",
            "region_control_buffer_wait_ns",
            "region_metadata_buffer_wait_ns",
            "total_region_read_queue_wait_ns",
            "total_region_write_queue_wait_ns",
            "total_region_control_queue_wait_ns",
            "total_region_read_buffer_wait_ns",
            "total_region_write_buffer_wait_ns",
            "total_region_control_buffer_wait_ns",
            "total_region_metadata_buffer_wait_ns",
            "region_staging_chunk_bytes",
            "region_staging_resident_bytes",
            "region_staging_flushing_bytes",
            "region_staging_sealed_spans",
            "region_staging_sealed_bytes",
            "region_staging_completion_live_records",
            "region_staging_completion_live_bytes",
            "region_staging_completion_obsolete_records",
            "region_staging_completion_obsolete_bytes",
            "region_staging_seal_fill_percent",
            "region_staging_obsolete_record_percent",
            "region_staging_obsolete_byte_percent",
            "total_region_staging_chunk_bytes",
            "total_region_staging_resident_bytes",
            "total_region_staging_flushing_bytes",
            "total_region_staging_sealed_spans",
            "total_region_staging_sealed_bytes",
            "total_region_staging_completion_live_records",
            "total_region_staging_completion_live_bytes",
            "total_region_staging_completion_obsolete_records",
            "total_region_staging_completion_obsolete_bytes",
        ] {
            assert!(
                json.contains(&format!("\"{field}\":0")),
                "missing zero-valued metric {field}"
            );
        }
        assert!(json.contains("\"hardware_qualification\":false"));
        assert!(json.contains("\"target_nvme_matrix_passed\":false"));
        assert!(json.contains("\"journal_rollover_max_ms\":1"));
        assert!(json.contains("\"memory_read_latency_p99_us\":11"));
        assert!(json.contains("\"bucket_read_latency_p99_us\":22"));
        assert!(json.contains("\"region_read_latency_p99_us\":33"));
        assert!(json.contains("\"miss_read_latency_p99_us\":44"));
        assert!(json.contains("\"steady_state_gate_passed\":true"));
        assert!(json.contains("\"premeasure_capacity_turnovers\":2"));

        let failed = Report {
            options: &options,
            phase: Phase {
                stale_values: 1,
                ..Phase::default()
            },
            stats: StatsDelta {
                bucket_io_submitted: 0,
                ..measured
            },
            drain_stats: StatsDelta::default(),
            total_stats: StatsDelta {
                bucket_io_submitted: 0,
                ..qualifying_total
            },
            premeasure_stats: StatsDelta::default(),
            final_stats: HybridCacheStats::default(),
            generator_planned_memory: 1,
            prefill_elapsed: Duration::ZERO,
            premeasure_elapsed: Duration::ZERO,
            steady_state_fill_phase: Phase::default(),
            required_region_reuses: required_reuses,
            drain: Duration::from_millis(20),
            reopen: Duration::ZERO,
            reopen_verify: Duration::ZERO,
            reopen_close: Duration::ZERO,
            reopen_verification: ReopenVerification::default(),
        };
        let failures = failed.acceptance_failures().join("; ");
        assert!(failures.contains("stale or corrupt"));
        assert!(failures.contains("measurement must submit active Bucket"));
        assert!(failures.contains("drain/close"));
        assert!(failures.contains("steady-state pre-measure gate"));
    }

    #[test]
    fn parser_requires_confirmation_empty_path_contract_and_hard_bounds() {
        let base = [
            "--bucket-path=bucket",
            "--bucket-capacity=16KiB",
            "--region-path=region",
            "--region-capacity=1GiB",
            "--manifest-path=manifest",
            "--memory-capacity=1MiB",
        ];
        assert!(Options::parse(base.iter().map(|value| (*value).to_owned())).is_err());
        let mut confirmed = base
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        confirmed.extend(["--yes".into(), "--concurrency=4097".into()]);
        assert!(Options::parse(confirmed).is_err());

        let mut write_back = base
            .iter()
            .map(|value| (*value).to_owned())
            .collect::<Vec<_>>();
        write_back.extend([
            "--yes".into(),
            "--write-mode=write-back".into(),
            "--write-back-queue-depth=2".into(),
            "--write-back-workers=3".into(),
        ]);
        assert!(Options::parse(write_back).is_err());
    }

    #[test]
    fn histogram_percentiles_are_monotonic() {
        let mut histogram = Histogram::default();
        for micros in [1, 2, 10, 100, 1000] {
            histogram.record(Duration::from_micros(micros));
        }
        assert!(histogram.percentile(500) <= histogram.percentile(990));
        assert!(histogram.percentile(990) <= histogram.percentile(999));
    }

    #[test]
    fn tier_read_latency_histograms_classify_and_merge_worker_results() {
        assert_eq!(
            ReadLatencyTier::from_cache_tier(CacheTier::Memory),
            Some(ReadLatencyTier::Memory)
        );
        assert_eq!(
            ReadLatencyTier::from_cache_tier(CacheTier::SmallObjectDisk),
            Some(ReadLatencyTier::Bucket)
        );
        assert_eq!(
            ReadLatencyTier::from_cache_tier(CacheTier::RegionLogDisk),
            Some(ReadLatencyTier::Region)
        );

        let mut first = Phase::default();
        first.record_read_tier_latency(ReadLatencyTier::Memory, Duration::from_micros(10));
        first.record_read_tier_latency(ReadLatencyTier::Bucket, Duration::from_micros(20));
        first.record_read_tier_latency(ReadLatencyTier::Region, Duration::from_micros(30));
        first.record_read_tier_latency(ReadLatencyTier::Miss, Duration::from_micros(40));
        let mut second = Phase::default();
        second.record_read_tier_latency(ReadLatencyTier::Memory, Duration::from_micros(50));
        second.record_read_tier_latency(ReadLatencyTier::Miss, Duration::from_micros(60));

        first.merge(&second);

        assert_eq!(first.memory_read_latency.count, 2);
        assert_eq!(first.bucket_read_latency.count, 1);
        assert_eq!(first.region_read_latency.count, 1);
        assert_eq!(first.miss_read_latency.count, 2);
        assert_eq!(first.memory_read_latency.percentile(990), 50_000);
        assert_eq!(first.bucket_read_latency.percentile(990), 20_000);
        assert_eq!(first.region_read_latency.percentile(990), 30_000);
        assert_eq!(first.miss_read_latency.percentile(990), 60_000);
    }
}
