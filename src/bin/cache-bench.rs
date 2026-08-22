//! Real-file workload harness for cache-rs.
//!
//! The target path must be dedicated to cache-rs. Opening and format safety
//! must be a regular file and cannot be a symbolic link. Format safety is
//! delegated to `DiskCache`: an unrecognized non-empty file is rejected and
//! is never silently overwritten by this tool.

use std::env;
use std::fmt::Write as _;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use cache_rs::{
    AdmissionMode, AsyncDiskCache, BackpressurePolicy, CacheConfig, CacheError, CacheStats,
    DiskCache, HostWriteSnapshot, IoEngineKind, IoMode, PutOptions, PutOutcome, ReclaimMode,
    RejectReason,
};

#[path = "cache_bench/hybrid.rs"]
mod hybrid_bench;

const DEFAULT_REGION_SIZE: u64 = 32 * 1024 * 1024;
const DEFAULT_OBJECT_SIZE: usize = 4096;
const DEFAULT_KEYS: usize = 100_000;
const DEFAULT_READ_PERCENT: u8 = 80;
const DEFAULT_PREFILL_PERCENT: u8 = 100;
const DEFAULT_HOTSET_PERCENT: u8 = 100;
const DEFAULT_HOT_ACCESS_PERCENT: u8 = 100;
const DEFAULT_CONCURRENCY: usize = 16;
const DEFAULT_QUEUE_DEPTH: usize = 128;
const DEFAULT_APPEND_LANES: usize = 2;
const DEFAULT_MEMORY_BUDGET: usize = 1024 * 1024 * 1024;
const DEFAULT_WARMUP_SECS: u64 = 5;
const DEFAULT_DURATION_SECS: u64 = 30;
const DEFAULT_SEED: u64 = 0x243f_6a88_85a3_08d3;
const MAX_CONCURRENCY: usize = 65_536;
const MAX_QUEUE_DEPTH: usize = 4096;
const MAX_APPEND_LANES: usize = 8;
const MAX_READ_PERCENT: u8 = 100;
const HISTOGRAM_SUB_BITS: usize = 3;
const HISTOGRAM_SUB_BUCKETS: usize = 1 << HISTOGRAM_SUB_BITS;
const HISTOGRAM_BUCKETS: usize = 64 * HISTOGRAM_SUB_BUCKETS;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("cache-bench: {error}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<(), String> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "hybrid")
    {
        return hybrid_bench::run(arguments.into_iter().skip(1));
    }
    let options = match Options::parse(arguments)? {
        ParseOutcome::Run(options) => options,
        ParseOutcome::Help => {
            print_help();
            return Ok(());
        }
    };
    validate_benchmark_target(&options.path)?;
    // Validate an explicitly supplied Linux counter source before formatting
    // or preloading a potentially large cache file.
    let _ = read_device_snapshot(options.device_stat.as_deref())?;

    let config = CacheConfig::new(&options.path, options.capacity)
        .with_region_size(options.region_size)
        .with_index_slots(options.index_slots)
        .with_max_key_size(64)
        .with_max_value_size(options.object_size)
        .with_memory_budget(options.memory_budget)
        .with_submission_queue_depths(options.queue_depth, options.queue_depth)
        .with_append_lanes(options.append_lanes)
        .with_io_engine(options.engine.into_cache_engine())
        .with_io_queue_depth(options.queue_depth)
        .with_backpressure(BackpressurePolicy::Block)
        .with_admission_mode(options.admission.into_cache_mode())
        .with_reclaim_mode(options.reclaim.into_cache_mode())
        .with_io_mode(cache_io_mode(options.io_mode));

    let keys = Arc::new(build_keys(options.keys)?);
    let value = Arc::new(build_value(options.object_size)?);
    let prefill_count = percentage_count(options.keys, options.prefill_percent);
    let access = AccessPattern {
        read_percent: options.read_percent,
        hotset_keys: percentage_count(options.keys, options.hotset_percent).max(1),
        hot_access_percent: options.hot_access_percent,
    };

    // Every invocation starts from freshly formatted Format V1 bytes. Existing
    // recognized cache contents are deliberately reset; unrelated files remain
    // protected by reset_existing's format recognition.
    let prefill_cache = Arc::new(open_fresh_benchmark_cache(
        config
            .clone()
            .with_admission_mode(AdmissionMode::Always)
            .with_reclaim_mode(ReclaimMode::Fifo),
        &options.path,
    )?);
    let prefill_cache = BenchmarkCache::new(prefill_cache, options.api)?;

    eprintln!(
        "cache-bench: preloading {} of {} objects ({} bytes each)",
        prefill_count, options.keys, options.object_size
    );
    prefill(&prefill_cache, &keys[..prefill_count], &value)?;
    prefill_cache
        .flush()
        .map_err(|error| format!("prefill flush failed: {error}"))?;
    eprintln!("cache-bench: verifying preloaded objects");
    verify_prefill(&prefill_cache, &keys[..prefill_count], &value)?;
    prefill_cache
        .close()
        .map_err(|error| format!("prefill close failed: {error}"))?;

    if !options.warmup.is_zero() {
        let warmup_cache = Arc::new(
            config
                .clone()
                .open()
                .map_err(|error| format!("cannot open warmup cache: {error}"))?,
        );
        let warmup_cache = BenchmarkCache::new(warmup_cache, options.api)?;
        eprintln!(
            "cache-bench: warming up for {:.3}s",
            options.warmup.as_secs_f64()
        );
        let _ = run_phase(
            &warmup_cache,
            Arc::clone(&keys),
            Arc::clone(&value),
            access,
            options.concurrency,
            options.warmup,
            options.seed ^ 0xa409_3822_299f_31d0,
        )?;
        warmup_cache
            .close()
            .map_err(|error| format!("warmup drain/close failed: {error}"))?;
    }

    // Reopening after warmup gives the measurement window its own counters and
    // policy observations. close() below drains append, reinsertion, and
    // maintenance workers before the final stats/device snapshots are taken.
    let cache = Arc::new(
        config
            .open()
            .map_err(|error| format!("cannot open measurement cache: {error}"))?,
    );
    let cache = BenchmarkCache::new(cache, options.api)?;
    let stats_before = cache.disk_cache().stats();
    let host_writes_before = cache.disk_cache().host_write_stats();
    eprintln!(
        "cache-bench: measuring for {:.3}s with concurrency={} queue_depth={}",
        options.duration.as_secs_f64(),
        options.concurrency,
        options.queue_depth
    );
    let device_before = read_device_snapshot(options.device_stat.as_deref())?;
    let cpu_before = ProcessCpuSnapshot::read();
    let result = run_phase(
        &cache,
        keys,
        value,
        access,
        options.concurrency,
        options.duration,
        options.seed,
    )?;
    let cpu = ProcessCpuMeasurement::between(cpu_before, ProcessCpuSnapshot::read());
    let drain_start = Instant::now();
    cache
        .close()
        .map_err(|error| format!("measurement drain/close failed: {error}"))?;
    let drain_elapsed = drain_start.elapsed();
    let stats_after = cache.disk_cache().stats();
    let host_writes_after = cache.disk_cache().host_write_stats();
    // Read device counters only after close has drained all background work and
    // published the clean checkpoint. Workload latency remains independent of
    // that final durability barrier.
    let device_after = read_device_snapshot(options.device_stat.as_deref())?;
    let device = DeviceMeasurement::between(device_before, device_after);

    let report = Report::new(
        &options,
        result,
        stats_delta(
            stats_before,
            stats_after,
            host_writes_before,
            host_writes_after,
        ),
        cpu,
        device,
        drain_elapsed,
    );
    match options.output {
        OutputKind::Json => println!("{}", report.to_json()),
        OutputKind::Human => report.print_human(),
    }
    let failures = report.acceptance_failures();
    if !failures.is_empty() {
        return Err(format!("acceptance failed: {}", failures.join("; ")));
    }
    Ok(())
}

#[derive(Clone)]
enum BenchmarkCache {
    Sync(Arc<DiskCache>),
    Async {
        cache: Arc<DiskCache>,
        facade: AsyncDiskCache,
    },
}

impl BenchmarkCache {
    fn new(cache: Arc<DiskCache>, api: ApiArg) -> Result<Self, String> {
        match api {
            ApiArg::Sync => Ok(Self::Sync(cache)),
            ApiArg::Async => {
                let facade = cache
                    .async_handle()
                    .map_err(|error| format!("cannot create async cache handle: {error}"))?;
                Ok(Self::Async { cache, facade })
            }
        }
    }

    fn disk_cache(&self) -> &DiskCache {
        match self {
            Self::Sync(cache) | Self::Async { cache, .. } => cache,
        }
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, CacheError> {
        match self {
            Self::Sync(cache) => cache.get(key),
            Self::Async { facade, .. } => block_on(facade.get(key)),
        }
    }

    fn put(&self, key: &[u8], value: &[u8], options: PutOptions) -> Result<PutOutcome, CacheError> {
        match self {
            Self::Sync(cache) => cache.put(key, value, options),
            Self::Async { facade, .. } => block_on(facade.put(key, value, options)),
        }
    }

    fn flush(&self) -> Result<(), CacheError> {
        match self {
            Self::Sync(cache) => cache.flush(),
            Self::Async { facade, .. } => block_on(facade.flush()),
        }
    }

    fn close(&self) -> Result<(), CacheError> {
        match self {
            Self::Sync(cache) => cache.close(),
            Self::Async { facade, .. } => block_on(facade.close()),
        }
    }
}

struct ThreadWaker {
    thread: thread::Thread,
    notified: AtomicBool,
}

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notified.store(true, Ordering::Release);
        self.thread.unpark();
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let notifier = Arc::new(ThreadWaker {
        thread: thread::current(),
        notified: AtomicBool::new(false),
    });
    let waker = Waker::from(Arc::clone(&notifier));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match Pin::as_mut(&mut future).poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => {
                while !notifier.notified.swap(false, Ordering::Acquire) {
                    thread::park();
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Options {
    path: PathBuf,
    capacity: u64,
    region_size: u64,
    object_size: usize,
    keys: usize,
    read_percent: u8,
    prefill_percent: u8,
    hotset_percent: u8,
    hot_access_percent: u8,
    concurrency: usize,
    queue_depth: usize,
    append_lanes: usize,
    memory_budget: usize,
    index_slots: usize,
    api: ApiArg,
    engine: EngineArg,
    io_mode: IoModeArg,
    admission: AdmissionArg,
    reclaim: ReclaimArg,
    warmup: Duration,
    duration: Duration,
    seed: u64,
    output: OutputKind,
    min_ops_per_sec: Option<f64>,
    max_p99_us: Option<f64>,
    min_hit_percent: Option<f64>,
    require_policy_activity: bool,
    device_stat: Option<PathBuf>,
}

enum ParseOutcome {
    Run(Box<Options>),
    Help,
}

impl Options {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<ParseOutcome, String> {
        let mut parser = ArgParser::new(arguments);
        let mut path = None;
        let mut capacity = None;
        let mut region_size = DEFAULT_REGION_SIZE;
        let mut object_size = DEFAULT_OBJECT_SIZE;
        let mut keys = DEFAULT_KEYS;
        let mut read_percent = DEFAULT_READ_PERCENT;
        let mut prefill_percent = DEFAULT_PREFILL_PERCENT;
        let mut hotset_percent = DEFAULT_HOTSET_PERCENT;
        let mut hot_access_percent = DEFAULT_HOT_ACCESS_PERCENT;
        let mut concurrency = DEFAULT_CONCURRENCY;
        let mut queue_depth = DEFAULT_QUEUE_DEPTH;
        let mut append_lanes = DEFAULT_APPEND_LANES;
        let mut memory_budget = DEFAULT_MEMORY_BUDGET;
        let mut index_slots = None;
        let mut api = ApiArg::Sync;
        let mut engine = EngineArg::Auto;
        let mut io_mode = IoModeArg::Buffered;
        let mut admission = AdmissionArg::Always;
        let mut reclaim = ReclaimArg::Fifo;
        let mut warmup = Duration::from_secs(DEFAULT_WARMUP_SECS);
        let mut duration = Duration::from_secs(DEFAULT_DURATION_SECS);
        let mut seed = DEFAULT_SEED;
        let mut output = OutputKind::Json;
        let mut min_ops_per_sec = None;
        let mut max_p99_us = None;
        let mut min_hit_percent = None;
        let mut require_policy_activity = false;
        let mut device_stat = None;

        while let Some(argument) = parser.next() {
            let (name, inline_value) = split_argument(&argument)?;
            if name == "--help" || name == "-h" {
                return Ok(ParseOutcome::Help);
            }
            if name == "--require-policy-activity" {
                if inline_value.is_some() {
                    return Err("--require-policy-activity does not take a value".to_owned());
                }
                require_policy_activity = true;
                continue;
            }
            let value = match inline_value {
                Some(value) => value.to_owned(),
                None => parser.next_value(name)?,
            };
            match name {
                "--path" => path = Some(PathBuf::from(value)),
                "--capacity" => capacity = Some(parse_bytes(&value, name)?),
                "--region-size" => region_size = parse_bytes(&value, name)?,
                "--object-size" => object_size = parse_usize_bytes(&value, name)?,
                "--keys" => keys = parse_number(&value, name)?,
                "--read-percent" => read_percent = parse_number(&value, name)?,
                "--prefill-percent" => prefill_percent = parse_number(&value, name)?,
                "--hotset-percent" => hotset_percent = parse_number(&value, name)?,
                "--hot-access-percent" => hot_access_percent = parse_number(&value, name)?,
                "--concurrency" => concurrency = parse_number(&value, name)?,
                "--queue-depth" => queue_depth = parse_number(&value, name)?,
                "--append-lanes" => append_lanes = parse_number(&value, name)?,
                "--memory-budget" => memory_budget = parse_usize_bytes(&value, name)?,
                "--index-slots" => index_slots = Some(parse_number(&value, name)?),
                "--api" => api = ApiArg::from_str(&value)?,
                "--engine" => engine = EngineArg::from_str(&value)?,
                "--mode" => io_mode = IoModeArg::from_str(&value)?,
                "--admission" => admission = AdmissionArg::from_str(&value)?,
                "--reclaim" => reclaim = ReclaimArg::from_str(&value)?,
                "--warmup-secs" => warmup = parse_duration(&value, name, true)?,
                "--duration-secs" => duration = parse_duration(&value, name, false)?,
                "--seed" => seed = parse_seed(&value)?,
                "--output" => output = OutputKind::from_str(&value)?,
                "--min-ops-per-sec" => min_ops_per_sec = Some(parse_positive_f64(&value, name)?),
                "--max-p99-us" => max_p99_us = Some(parse_positive_f64(&value, name)?),
                "--min-hit-percent" => min_hit_percent = Some(parse_percent_f64(&value, name)?),
                "--device-stat" => device_stat = Some(PathBuf::from(value)),
                _ => return Err(format!("unknown option {name}; use --help")),
            }
        }

        let path = path.ok_or_else(|| "--path is required".to_owned())?;
        let capacity = capacity.ok_or_else(|| "--capacity is required".to_owned())?;
        validate_nonzero("--capacity", capacity)?;
        validate_nonzero("--region-size", region_size)?;
        validate_nonzero("--object-size", object_size)?;
        validate_nonzero("--keys", keys)?;
        validate_range("--read-percent", read_percent, 0, MAX_READ_PERCENT)?;
        validate_range("--prefill-percent", prefill_percent, 0, 100)?;
        validate_range("--hotset-percent", hotset_percent, 1, 100)?;
        validate_range("--hot-access-percent", hot_access_percent, 0, 100)?;
        validate_range("--concurrency", concurrency, 1, MAX_CONCURRENCY)?;
        validate_range("--queue-depth", queue_depth, 1, MAX_QUEUE_DEPTH)?;
        validate_range("--append-lanes", append_lanes, 1, MAX_APPEND_LANES)?;
        validate_nonzero("--memory-budget", memory_budget)?;
        if path.as_os_str().is_empty() {
            return Err("--path must not be empty".into());
        }
        let index_slots = match index_slots {
            Some(slots) => slots,
            None => default_index_slots(keys)?,
        };
        validate_nonzero("--index-slots", index_slots)?;

        Ok(ParseOutcome::Run(Box::new(Self {
            path,
            capacity,
            region_size,
            object_size,
            keys,
            read_percent,
            prefill_percent,
            hotset_percent,
            hot_access_percent,
            concurrency,
            queue_depth,
            append_lanes,
            memory_budget,
            index_slots,
            api,
            engine,
            io_mode,
            admission,
            reclaim,
            warmup,
            duration,
            seed,
            output,
            min_ops_per_sec,
            max_p99_us,
            min_hit_percent,
            require_policy_activity,
            device_stat,
        })))
    }
}

struct ArgParser {
    arguments: std::iter::Peekable<std::vec::IntoIter<String>>,
}

impl ArgParser {
    fn new(arguments: impl IntoIterator<Item = String>) -> Self {
        Self {
            arguments: arguments
                .into_iter()
                .collect::<Vec<_>>()
                .into_iter()
                .peekable(),
        }
    }

    fn next(&mut self) -> Option<String> {
        self.arguments.next()
    }

    fn next_value(&mut self, name: &str) -> Result<String, String> {
        match self.arguments.peek() {
            Some(argument) if argument.starts_with('-') => Err(format!("missing value for {name}")),
            Some(_) => self
                .arguments
                .next()
                .ok_or_else(|| format!("missing value for {name}")),
            None => Err(format!("missing value for {name}")),
        }
    }
}

fn split_argument(argument: &str) -> Result<(&str, Option<&str>), String> {
    if !argument.starts_with('-') {
        return Err(format!(
            "unexpected positional argument {argument:?}; use --help"
        ));
    }
    Ok(argument
        .split_once('=')
        .map_or((argument, None), |(name, value)| (name, Some(value))))
}

fn validate_benchmark_target(path: &Path) -> Result<(), String> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(format!(
                "cannot inspect benchmark cache {}: {error}",
                path.display()
            ));
        }
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Err(format!(
            "benchmark cache path {} must not be a symbolic link",
            path.display()
        ));
    }
    if !file_type.is_file() {
        return Err(format!(
            "benchmark cache path {} is not a regular file",
            path.display()
        ));
    }
    Ok(())
}

fn open_fresh_benchmark_cache(config: CacheConfig, path: &Path) -> Result<DiskCache, String> {
    validate_benchmark_target(path)?;
    let existing_bytes = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata.len() != 0,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(format!(
                "cannot inspect benchmark cache {}: {error}",
                path.display()
            ));
        }
    };
    if existing_bytes {
        eprintln!(
            "cache-bench: resetting existing recognized Format V1 cache {}",
            path.display()
        );
        config
            .reset_existing()
            .map_err(|error| format!("cannot reset benchmark cache: {error}"))
    } else {
        eprintln!(
            "cache-bench: formatting fresh Format V1 cache {}",
            path.display()
        );
        config
            .format_empty()
            .map_err(|error| format!("cannot format benchmark cache: {error}"))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApiArg {
    Sync,
    Async,
}

impl ApiArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Async => "async",
        }
    }
}

impl FromStr for ApiArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sync" => Ok(Self::Sync),
            "async" => Ok(Self::Async),
            _ => Err(format!("invalid --api {value:?}; expected sync or async")),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum EngineArg {
    Sync,
    Auto,
    IoUring,
}

impl EngineArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sync => "sync",
            Self::Auto => "auto",
            Self::IoUring => "io_uring",
        }
    }

    const fn into_cache_engine(self) -> IoEngineKind {
        match self {
            Self::Sync => IoEngineKind::Sync,
            Self::Auto => IoEngineKind::Auto,
            Self::IoUring => IoEngineKind::IoUring,
        }
    }
}

impl FromStr for EngineArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "sync" => Ok(Self::Sync),
            "auto" => Ok(Self::Auto),
            "uring" | "io_uring" | "io-uring" => Ok(Self::IoUring),
            _ => Err(format!(
                "invalid --engine {value:?}; expected sync, auto, or uring"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IoModeArg {
    Buffered,
    Direct,
    Auto,
}

impl IoModeArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Direct => "direct",
            Self::Auto => "auto",
        }
    }
}

impl FromStr for IoModeArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "buffered" => Ok(Self::Buffered),
            "direct" => Ok(Self::Direct),
            "auto" => Ok(Self::Auto),
            _ => Err(format!(
                "invalid --mode {value:?}; expected buffered, direct, or auto"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AdmissionArg {
    Always,
    SecondHit,
}

impl AdmissionArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::SecondHit => "second-hit",
        }
    }

    const fn into_cache_mode(self) -> AdmissionMode {
        match self {
            Self::Always => AdmissionMode::Always,
            Self::SecondHit => AdmissionMode::SecondHit,
        }
    }
}

impl FromStr for AdmissionArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "always" => Ok(Self::Always),
            "second-hit" | "second_hit" => Ok(Self::SecondHit),
            _ => Err(format!(
                "invalid --admission {value:?}; expected always or second-hit"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReclaimArg {
    Fifo,
    SecondChance,
}

impl ReclaimArg {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Fifo => "fifo",
            Self::SecondChance => "second-chance",
        }
    }

    const fn into_cache_mode(self) -> ReclaimMode {
        match self {
            Self::Fifo => ReclaimMode::Fifo,
            Self::SecondChance => ReclaimMode::SecondChance,
        }
    }
}

impl FromStr for ReclaimArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "fifo" => Ok(Self::Fifo),
            "second-chance" | "second_chance" => Ok(Self::SecondChance),
            _ => Err(format!(
                "invalid --reclaim {value:?}; expected fifo or second-chance"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum OutputKind {
    Json,
    Human,
}

impl FromStr for OutputKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "json" => Ok(Self::Json),
            "human" => Ok(Self::Human),
            _ => Err(format!(
                "invalid --output {value:?}; expected json or human"
            )),
        }
    }
}

fn cache_io_mode(mode: IoModeArg) -> IoMode {
    match mode {
        IoModeArg::Buffered => IoMode::Buffered,
        IoModeArg::Auto => IoMode::Auto,
        IoModeArg::Direct => IoMode::Direct,
    }
}

fn parse_bytes(value: &str, name: &str) -> Result<u64, String> {
    let value = value.trim();
    let split = value
        .find(|character: char| !character.is_ascii_digit() && character != '_')
        .unwrap_or(value.len());
    let (number, suffix) = value.split_at(split);
    if number.is_empty() {
        return Err(format!("invalid byte value {value:?} for {name}"));
    }
    let number = number.replace('_', "");
    let number = number
        .parse::<u64>()
        .map_err(|_| format!("invalid byte value {value:?} for {name}"))?;
    let suffix = suffix.to_ascii_lowercase();
    let multiplier = match suffix.as_str() {
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

fn parse_number<T>(value: &str, name: &str) -> Result<T, String>
where
    T: FromStr,
{
    value
        .replace('_', "")
        .parse::<T>()
        .map_err(|_| format!("invalid value {value:?} for {name}"))
}

fn parse_seed(value: &str) -> Result<u64, String> {
    let compact = value.replace('_', "");
    if let Some(hex) = compact
        .strip_prefix("0x")
        .or_else(|| compact.strip_prefix("0X"))
    {
        u64::from_str_radix(hex, 16).map_err(|_| format!("invalid --seed {value:?}"))
    } else {
        compact
            .parse::<u64>()
            .map_err(|_| format!("invalid --seed {value:?}"))
    }
}

fn parse_duration(value: &str, name: &str, allow_zero: bool) -> Result<Duration, String> {
    let seconds = value
        .parse::<f64>()
        .map_err(|_| format!("invalid seconds value {value:?} for {name}"))?;
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

fn parse_positive_f64(value: &str, name: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("invalid value {value:?} for {name}"))?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(format!("{name} must be a finite positive number"))
    }
}

fn parse_percent_f64(value: &str, name: &str) -> Result<f64, String> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| format!("invalid value {value:?} for {name}"))?;
    if parsed.is_finite() && (0.0..=100.0).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(format!("{name} must be a finite number in 0..=100"))
    }
}

fn validate_nonzero<T>(name: &str, value: T) -> Result<(), String>
where
    T: Copy + Default + PartialEq,
{
    if value == T::default() {
        Err(format!("{name} must be non-zero"))
    } else {
        Ok(())
    }
}

fn validate_range<T>(name: &str, value: T, minimum: T, maximum: T) -> Result<(), String>
where
    T: Copy + std::fmt::Display + PartialOrd,
{
    if value < minimum || value > maximum {
        Err(format!("{name} must be in {minimum}..={maximum}"))
    } else {
        Ok(())
    }
}

fn default_index_slots(keys: usize) -> Result<usize, String> {
    const MAX_INDEX_SLOTS: usize = 256 * 1024 * 1024;
    let doubled = keys
        .checked_mul(2)
        .ok_or_else(|| "--keys is too large to size the index".to_owned())?;
    Ok(doubled
        .checked_next_power_of_two()
        .unwrap_or(MAX_INDEX_SLOTS)
        .clamp(1024, MAX_INDEX_SLOTS))
}

fn percentage_count(total: usize, percent: u8) -> usize {
    ((total as u128 * u128::from(percent)) / 100) as usize
}

fn build_keys(count: usize) -> Result<Vec<Vec<u8>>, String> {
    let mut keys = Vec::new();
    keys.try_reserve_exact(count)
        .map_err(|_| "cannot allocate benchmark key table".to_owned())?;
    for id in 0..count {
        keys.push(format!("cache-bench-{id:016x}").into_bytes());
    }
    Ok(keys)
}

fn build_value(size: usize) -> Result<Vec<u8>, String> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(size)
        .map_err(|_| "cannot allocate benchmark value".to_owned())?;
    value.resize(size, 0x5a);
    Ok(value)
}

fn prefill(cache: &BenchmarkCache, keys: &[Vec<u8>], value: &[u8]) -> Result<(), String> {
    for key in keys {
        match cache.put(key, value, PutOptions::default()) {
            Ok(PutOutcome::Stored) => {}
            Ok(PutOutcome::Rejected(reason)) => {
                return Err(format!("prefill put was rejected: {reason:?}"));
            }
            Err(error) => return Err(format!("prefill put failed: {error}")),
        }
    }
    Ok(())
}

fn verify_prefill(cache: &BenchmarkCache, keys: &[Vec<u8>], value: &[u8]) -> Result<(), String> {
    for (index, key) in keys.iter().enumerate() {
        match cache.get(key) {
            Ok(Some(found)) if found == value => {}
            Ok(Some(_)) => return Err(format!("prefill verification failed for key {index}")),
            Ok(None) => return Err(format!("prefill key {index} is missing after flush")),
            Err(error) => {
                return Err(format!(
                    "prefill verification read failed for key {index}: {error}"
                ));
            }
        }
    }
    Ok(())
}

fn run_phase(
    cache: &BenchmarkCache,
    keys: Arc<Vec<Vec<u8>>>,
    value: Arc<Vec<u8>>,
    access: AccessPattern,
    concurrency: usize,
    duration: Duration,
    seed: u64,
) -> Result<PhaseResult, String> {
    let gate = Arc::new(StartGate::new());
    let abort = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    workers
        .try_reserve_exact(concurrency)
        .map_err(|_| "cannot allocate benchmark worker table".to_owned())?;

    for worker_id in 0..concurrency {
        let worker_gate = Arc::clone(&gate);
        let context = WorkerContext {
            cache: cache.clone(),
            keys: Arc::clone(&keys),
            value: Arc::clone(&value),
            access,
            duration,
            seed: mix_seed(seed, worker_id),
            abort: Arc::clone(&abort),
        };
        let spawn = thread::Builder::new()
            .name(format!("cache-bench-{worker_id}"))
            .spawn(move || {
                let start = worker_gate.wait();
                worker_loop(context, start)
            });
        match spawn {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                abort.store(true, Ordering::Release);
                gate.start(Instant::now());
                for worker in workers {
                    let _ = worker.join();
                }
                return Err(format!("cannot spawn benchmark worker: {error}"));
            }
        }
    }

    gate.wait_for_workers(concurrency);
    let start = Instant::now();
    gate.start(start);
    let mut result = PhaseResult::new();
    let mut worker_panicked = false;
    for worker in workers {
        match worker.join() {
            Ok(worker) => result.merge(&worker),
            Err(_) => worker_panicked = true,
        }
    }
    result.elapsed = start.elapsed();
    if worker_panicked {
        Err("a benchmark worker panicked".to_owned())
    } else {
        Ok(result)
    }
}

struct StartGate {
    state: Mutex<StartGateState>,
    changed: Condvar,
}

struct StartGateState {
    start: Option<Instant>,
    waiting_workers: usize,
}

impl StartGate {
    fn new() -> Self {
        Self {
            state: Mutex::new(StartGateState {
                start: None,
                waiting_workers: 0,
            }),
            changed: Condvar::new(),
        }
    }

    fn start(&self, start: Instant) {
        lock_unpoisoned(&self.state).start = Some(start);
        self.changed.notify_all();
    }

    fn wait_for_workers(&self, expected: usize) {
        let mut state = lock_unpoisoned(&self.state);
        while state.waiting_workers < expected {
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn wait(&self) -> Instant {
        let mut state = lock_unpoisoned(&self.state);
        state.waiting_workers = state.waiting_workers.saturating_add(1);
        self.changed.notify_all();
        loop {
            if let Some(start) = state.start {
                return start;
            }
            state = self
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct WorkerContext {
    cache: BenchmarkCache,
    keys: Arc<Vec<Vec<u8>>>,
    value: Arc<Vec<u8>>,
    access: AccessPattern,
    duration: Duration,
    seed: u64,
    abort: Arc<AtomicBool>,
}

#[derive(Clone, Copy)]
struct AccessPattern {
    read_percent: u8,
    hotset_keys: usize,
    hot_access_percent: u8,
}

fn worker_loop(context: WorkerContext, start: Instant) -> PhaseResult {
    let deadline = start.checked_add(context.duration).unwrap_or(start);
    let mut random = XorShift64::new(context.seed);
    let mut result = PhaseResult::new();
    while !context.abort.load(Ordering::Acquire) && Instant::now() < deadline {
        let is_read = random.next_u64() % 100 < u64::from(context.access.read_percent);
        let key_index = select_key_index(&mut random, context.keys.len(), context.access);
        let operation_start = Instant::now();
        if is_read {
            result.reads = result.reads.saturating_add(1);
            match context.cache.get(&context.keys[key_index]) {
                Ok(Some(found)) if found.as_slice() == context.value.as_slice() => {
                    result.hits = result.hits.saturating_add(1);
                    result.read_bytes = result.read_bytes.saturating_add(usize_to_u64(found.len()));
                }
                // A cache miss is allowed; returning the wrong value is not.
                // Stop the phase promptly so long-running soak jobs surface
                // the first correctness failure instead of hiding it in an
                // aggregate error count.
                Ok(Some(_)) => {
                    result.errors = result.errors.saturating_add(1);
                    context.abort.store(true, Ordering::Release);
                }
                Ok(None) => result.misses = result.misses.saturating_add(1),
                Err(error) if is_rejection(&error) => {
                    result.rejected = result.rejected.saturating_add(1)
                }
                Err(_) => result.errors = result.errors.saturating_add(1),
            }
            let latency = operation_start.elapsed();
            result.latency.record(latency);
            result.read_latency.record(latency);
        } else {
            result.writes = result.writes.saturating_add(1);
            match context.cache.put(
                &context.keys[key_index],
                context.value.as_slice(),
                PutOptions::default(),
            ) {
                Ok(PutOutcome::Stored) => {
                    result.stored = result.stored.saturating_add(1);
                    result.write_bytes = result
                        .write_bytes
                        .saturating_add(usize_to_u64(context.value.len()));
                }
                Ok(PutOutcome::Rejected(reason)) => {
                    result.rejected = result.rejected.saturating_add(1);
                    result.write_rejections = result.write_rejections.saturating_add(1);
                    if matches!(
                        reason,
                        RejectReason::AdmissionFiltered | RejectReason::LargeObjectCold
                    ) {
                        result.policy_rejections = result.policy_rejections.saturating_add(1);
                    }
                }
                Err(error) if is_rejection(&error) => {
                    result.rejected = result.rejected.saturating_add(1);
                    result.write_rejections = result.write_rejections.saturating_add(1);
                }
                Err(_) => result.errors = result.errors.saturating_add(1),
            }
            let latency = operation_start.elapsed();
            result.latency.record(latency);
            result.write_latency.record(latency);
        }
    }
    result
}

fn select_key_index(random: &mut XorShift64, key_count: usize, access: AccessPattern) -> usize {
    let hotset_keys = access.hotset_keys.min(key_count).max(1);
    if hotset_keys == key_count {
        return (random.next_u64() as usize) % key_count;
    }
    let use_hot = random.next_u64() % 100 < u64::from(access.hot_access_percent);
    if use_hot {
        (random.next_u64() as usize) % hotset_keys
    } else {
        hotset_keys + (random.next_u64() as usize) % (key_count - hotset_keys)
    }
}

fn is_rejection(error: &CacheError) -> bool {
    matches!(
        error,
        CacheError::Cancelled | CacheError::TimedOut | CacheError::Overloaded(_)
    )
}

fn mix_seed(seed: u64, worker: usize) -> u64 {
    let mut value = seed ^ (usize_to_u64(worker).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    let mixed = value ^ (value >> 31);
    if mixed == 0 { DEFAULT_SEED } else { mixed }
}

struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 { DEFAULT_SEED } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}

struct PhaseResult {
    elapsed: Duration,
    reads: u64,
    writes: u64,
    hits: u64,
    misses: u64,
    stored: u64,
    rejected: u64,
    write_rejections: u64,
    policy_rejections: u64,
    errors: u64,
    read_bytes: u64,
    write_bytes: u64,
    latency: LatencyHistogram,
    read_latency: LatencyHistogram,
    write_latency: LatencyHistogram,
}

impl PhaseResult {
    fn new() -> Self {
        Self {
            elapsed: Duration::ZERO,
            reads: 0,
            writes: 0,
            hits: 0,
            misses: 0,
            stored: 0,
            rejected: 0,
            write_rejections: 0,
            policy_rejections: 0,
            errors: 0,
            read_bytes: 0,
            write_bytes: 0,
            latency: LatencyHistogram::new(),
            read_latency: LatencyHistogram::new(),
            write_latency: LatencyHistogram::new(),
        }
    }

    fn operations(&self) -> u64 {
        self.reads.saturating_add(self.writes)
    }

    fn merge(&mut self, other: &Self) {
        self.reads = self.reads.saturating_add(other.reads);
        self.writes = self.writes.saturating_add(other.writes);
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.stored = self.stored.saturating_add(other.stored);
        self.rejected = self.rejected.saturating_add(other.rejected);
        self.write_rejections = self.write_rejections.saturating_add(other.write_rejections);
        self.policy_rejections = self
            .policy_rejections
            .saturating_add(other.policy_rejections);
        self.errors = self.errors.saturating_add(other.errors);
        self.read_bytes = self.read_bytes.saturating_add(other.read_bytes);
        self.write_bytes = self.write_bytes.saturating_add(other.write_bytes);
        self.latency.merge(&other.latency);
        self.read_latency.merge(&other.read_latency);
        self.write_latency.merge(&other.write_latency);
    }
}

struct LatencyHistogram {
    buckets: [u64; HISTOGRAM_BUCKETS],
    count: u64,
    maximum_ns: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: [0; HISTOGRAM_BUCKETS],
            count: 0,
            maximum_ns: 0,
        }
    }

    fn record(&mut self, duration: Duration) {
        let nanoseconds = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let bucket = histogram_bucket(nanoseconds);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.count = self.count.saturating_add(1);
        self.maximum_ns = self.maximum_ns.max(nanoseconds);
    }

    fn merge(&mut self, other: &Self) {
        for (target, source) in self.buckets.iter_mut().zip(&other.buckets) {
            *target = target.saturating_add(*source);
        }
        self.count = self.count.saturating_add(other.count);
        self.maximum_ns = self.maximum_ns.max(other.maximum_ns);
    }

    fn percentile_ns(&self, permille: u64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = self.count.saturating_mul(permille).saturating_add(999) / 1000;
        let mut seen = 0u64;
        for (index, count) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*count);
            if seen >= target.max(1) {
                return histogram_bucket_upper(index).min(self.maximum_ns.max(1));
            }
        }
        self.maximum_ns
    }
}

fn histogram_bucket(nanoseconds: u64) -> usize {
    if nanoseconds <= 1 {
        return 0;
    }
    let exponent = (u64::BITS - 1 - nanoseconds.leading_zeros()) as usize;
    let base = 1u64 << exponent;
    let offset = nanoseconds - base;
    let sub_bucket = ((u128::from(offset) * HISTOGRAM_SUB_BUCKETS as u128) / u128::from(base))
        .min((HISTOGRAM_SUB_BUCKETS - 1) as u128) as usize;
    exponent * HISTOGRAM_SUB_BUCKETS + sub_bucket
}

fn histogram_bucket_upper(index: usize) -> u64 {
    let exponent = index / HISTOGRAM_SUB_BUCKETS;
    let sub_bucket = index % HISTOGRAM_SUB_BUCKETS;
    let base = 1u128 << exponent;
    let numerator = (sub_bucket + 1) as u128 * base;
    let width =
        numerator.saturating_add(HISTOGRAM_SUB_BUCKETS as u128 - 1) / HISTOGRAM_SUB_BUCKETS as u128;
    let upper = base.saturating_add(width);
    u64::try_from(upper.saturating_sub(1)).unwrap_or(u64::MAX)
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Clone, Copy)]
struct ProcessCpuSnapshot {
    user_ticks: u64,
    system_ticks: u64,
    ticks_per_second: u64,
}

impl ProcessCpuSnapshot {
    fn read() -> Option<Self> {
        read_process_cpu_snapshot()
    }
}

#[derive(Clone, Copy, Default)]
struct ProcessCpuMeasurement {
    available: bool,
    user_seconds: f64,
    system_seconds: f64,
}

impl ProcessCpuMeasurement {
    fn between(before: Option<ProcessCpuSnapshot>, after: Option<ProcessCpuSnapshot>) -> Self {
        let (Some(before), Some(after)) = (before, after) else {
            return Self::default();
        };
        if before.ticks_per_second == 0 || before.ticks_per_second != after.ticks_per_second {
            return Self::default();
        }
        let ticks = before.ticks_per_second as f64;
        Self {
            available: true,
            user_seconds: after.user_ticks.saturating_sub(before.user_ticks) as f64 / ticks,
            system_seconds: after.system_ticks.saturating_sub(before.system_ticks) as f64 / ticks,
        }
    }

    fn total_seconds(self) -> f64 {
        self.user_seconds + self.system_seconds
    }
}

#[cfg(target_os = "linux")]
fn read_process_cpu_snapshot() -> Option<ProcessCpuSnapshot> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    parse_process_stat(&stat, linux_clock_ticks_per_second()?)
}

#[cfg(not(target_os = "linux"))]
fn read_process_cpu_snapshot() -> Option<ProcessCpuSnapshot> {
    None
}

#[cfg(any(target_os = "linux", test))]
fn parse_process_stat(stat: &str, ticks_per_second: u64) -> Option<ProcessCpuSnapshot> {
    let command_end = stat.rfind(')')?;
    let mut fields = stat.get(command_end + 1..)?.split_whitespace();
    let user_ticks = fields.nth(11)?.parse().ok()?;
    let system_ticks = fields.next()?.parse().ok()?;
    Some(ProcessCpuSnapshot {
        user_ticks,
        system_ticks,
        ticks_per_second,
    })
}

#[cfg(target_os = "linux")]
fn linux_clock_ticks_per_second() -> Option<u64> {
    const SC_CLK_TCK: i32 = 2;
    unsafe extern "C" {
        fn sysconf(name: i32) -> isize;
    }
    // SAFETY: `sysconf` is called with Linux's constant `_SC_CLK_TCK` and has
    // no pointer arguments or caller-owned memory lifetime requirements.
    let ticks = unsafe { sysconf(SC_CLK_TCK) };
    u64::try_from(ticks).ok().filter(|ticks| *ticks != 0)
}

#[derive(Clone, Copy)]
struct DeviceSnapshot {
    sectors_read: u64,
    sectors_written: u64,
    io_ticks_ms: u64,
}

#[derive(Clone, Copy, Default)]
struct DeviceMeasurement {
    available: bool,
    sectors_read: u64,
    sectors_written: u64,
    io_ticks_ms: u64,
}

impl DeviceMeasurement {
    fn between(before: Option<DeviceSnapshot>, after: Option<DeviceSnapshot>) -> Self {
        let (Some(before), Some(after)) = (before, after) else {
            return Self::default();
        };
        Self {
            available: true,
            sectors_read: after.sectors_read.saturating_sub(before.sectors_read),
            sectors_written: after.sectors_written.saturating_sub(before.sectors_written),
            io_ticks_ms: after.io_ticks_ms.saturating_sub(before.io_ticks_ms),
        }
    }

    fn bytes_read(self) -> u64 {
        self.sectors_read.saturating_mul(512)
    }

    fn bytes_written(self) -> u64 {
        self.sectors_written.saturating_mul(512)
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_device_stat(stat: &str) -> Option<DeviceSnapshot> {
    let fields = stat
        .split_whitespace()
        .map(str::parse::<u64>)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    if fields.len() < 11 {
        return None;
    }
    Some(DeviceSnapshot {
        sectors_read: fields[2],
        sectors_written: fields[6],
        io_ticks_ms: fields[9],
    })
}

#[cfg(target_os = "linux")]
fn read_device_snapshot(path: Option<&Path>) -> Result<Option<DeviceSnapshot>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let stat = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read --device-stat {}: {error}", path.display()))?;
    parse_device_stat(&stat)
        .map(Some)
        .ok_or_else(|| format!("invalid Linux block stat file: {}", path.display()))
}

#[cfg(not(target_os = "linux"))]
fn read_device_snapshot(path: Option<&Path>) -> Result<Option<DeviceSnapshot>, String> {
    let _ = path;
    Ok(None)
}

#[derive(Clone, Copy, Default)]
struct StatsDelta {
    cache_bytes_read: u64,
    cache_bytes_written: u64,
    write_batches: u64,
    records_coalesced: u64,
    regions_reused: u64,
    cache_rejected: u64,
    admission_observations: u64,
    admission_rejections: u64,
    large_object_rejections: u64,
    namespace_capacity_rejections: u64,
    namespace_write_budget_rejections: u64,
    host_write_operations: u64,
    host_write_bytes: u64,
    foreground_record_bytes: u64,
    reinsertion_bytes: u64,
    metadata_write_bytes: u64,
    checkpoint_write_bytes: u64,
    admitted_value_bytes: u64,
    daily_utc_day: u64,
    daily_window_crossed: bool,
    daily_host_write_bytes_current: u64,
    daily_write_budget_rejections_current: u64,
    reinsert_queued: u64,
    reinsert_dropped: u64,
    reinsert_stale: u64,
    reinsert_completed: u64,
    background_regions_reclaimed: u64,
    reclaim_backlog_rejections: u64,
    reclaim_records_scanned: u64,
    reclaim_index_fallbacks: u64,
    region_used_bytes: u64,
    region_valid_bytes: u64,
    minimum_region_valid_ratio_bps: u64,
    memory_budget_bytes: u64,
    memory_used_bytes: u64,
    memory_peak_bytes: u64,
    async_queue_rejections: u64,
    io_submitted: u64,
    io_completed: u64,
    io_errors: u64,
    io_cancel_requested: u64,
    io_cancelled: u64,
    io_submit_wait_ns: u64,
    io_completion_ns: u64,
    io_in_flight_peak: u64,
    direct_io_operations: u64,
    direct_io_bytes: u64,
    buffered_io_operations: u64,
    buffered_io_bytes: u64,
    io_uring_active: bool,
    direct_io_active: bool,
}

fn stats_delta(
    before: CacheStats,
    after: CacheStats,
    daily_before: HostWriteSnapshot,
    daily_after: HostWriteSnapshot,
) -> StatsDelta {
    StatsDelta {
        cache_bytes_read: after.bytes_read.saturating_sub(before.bytes_read),
        cache_bytes_written: after.bytes_written.saturating_sub(before.bytes_written),
        write_batches: after.write_batches.saturating_sub(before.write_batches),
        records_coalesced: after
            .records_coalesced
            .saturating_sub(before.records_coalesced),
        regions_reused: after.regions_reused.saturating_sub(before.regions_reused),
        cache_rejected: after.rejected.saturating_sub(before.rejected),
        admission_observations: after
            .admission_observations
            .saturating_sub(before.admission_observations),
        admission_rejections: after
            .admission_rejections
            .saturating_sub(before.admission_rejections),
        large_object_rejections: after
            .large_object_rejections
            .saturating_sub(before.large_object_rejections),
        namespace_capacity_rejections: after
            .namespace_capacity_rejections
            .saturating_sub(before.namespace_capacity_rejections),
        namespace_write_budget_rejections: after
            .namespace_write_budget_rejections
            .saturating_sub(before.namespace_write_budget_rejections),
        host_write_operations: after
            .host_write_operations
            .saturating_sub(before.host_write_operations),
        host_write_bytes: after
            .host_write_bytes
            .saturating_sub(before.host_write_bytes),
        foreground_record_bytes: after
            .foreground_record_bytes
            .saturating_sub(before.foreground_record_bytes),
        reinsertion_bytes: after
            .reinsertion_bytes
            .saturating_sub(before.reinsertion_bytes),
        metadata_write_bytes: after
            .metadata_write_bytes
            .saturating_sub(before.metadata_write_bytes),
        checkpoint_write_bytes: after
            .checkpoint_write_bytes
            .saturating_sub(before.checkpoint_write_bytes),
        admitted_value_bytes: after
            .admitted_value_bytes
            .saturating_sub(before.admitted_value_bytes),
        daily_utc_day: daily_after.utc_day,
        daily_window_crossed: daily_before.utc_day != daily_after.utc_day,
        daily_host_write_bytes_current: daily_after.daily_host_write_bytes,
        daily_write_budget_rejections_current: daily_after.daily_budget_rejections,
        reinsert_queued: after.reinsert_queued.saturating_sub(before.reinsert_queued),
        reinsert_dropped: after
            .reinsert_dropped
            .saturating_sub(before.reinsert_dropped),
        reinsert_stale: after.reinsert_stale.saturating_sub(before.reinsert_stale),
        reinsert_completed: after
            .reinsert_completed
            .saturating_sub(before.reinsert_completed),
        background_regions_reclaimed: after
            .background_regions_reclaimed
            .saturating_sub(before.background_regions_reclaimed),
        reclaim_backlog_rejections: after
            .reclaim_backlog_rejections
            .saturating_sub(before.reclaim_backlog_rejections),
        reclaim_records_scanned: after
            .reclaim_records_scanned
            .saturating_sub(before.reclaim_records_scanned),
        reclaim_index_fallbacks: after
            .reclaim_index_fallbacks
            .saturating_sub(before.reclaim_index_fallbacks),
        region_used_bytes: after.region_used_bytes,
        region_valid_bytes: after.region_valid_bytes,
        minimum_region_valid_ratio_bps: after.minimum_region_valid_ratio_bps,
        memory_budget_bytes: after.memory_budget_bytes,
        memory_used_bytes: after.memory_used_bytes,
        memory_peak_bytes: after.memory_peak_bytes,
        async_queue_rejections: after
            .async_queue_rejections
            .saturating_sub(before.async_queue_rejections),
        io_submitted: after.io_submitted.saturating_sub(before.io_submitted),
        io_completed: after.io_completed.saturating_sub(before.io_completed),
        io_errors: after.io_errors.saturating_sub(before.io_errors),
        io_cancel_requested: after
            .io_cancel_requested
            .saturating_sub(before.io_cancel_requested),
        io_cancelled: after.io_cancelled.saturating_sub(before.io_cancelled),
        io_submit_wait_ns: after
            .io_submit_wait_ns
            .saturating_sub(before.io_submit_wait_ns),
        io_completion_ns: after
            .io_completion_ns
            .saturating_sub(before.io_completion_ns),
        io_in_flight_peak: after.io_in_flight_peak,
        direct_io_operations: after
            .direct_io_operations
            .saturating_sub(before.direct_io_operations),
        direct_io_bytes: after.direct_io_bytes.saturating_sub(before.direct_io_bytes),
        buffered_io_operations: after
            .buffered_io_operations
            .saturating_sub(before.buffered_io_operations),
        buffered_io_bytes: after
            .buffered_io_bytes
            .saturating_sub(before.buffered_io_bytes),
        io_uring_active: after.io_uring_active,
        direct_io_active: after.direct_io_active,
    }
}

struct Report<'a> {
    options: &'a Options,
    phase: PhaseResult,
    stats: StatsDelta,
    cpu: ProcessCpuMeasurement,
    device: DeviceMeasurement,
    drain_elapsed: Duration,
}

impl<'a> Report<'a> {
    fn new(
        options: &'a Options,
        phase: PhaseResult,
        stats: StatsDelta,
        cpu: ProcessCpuMeasurement,
        device: DeviceMeasurement,
        drain_elapsed: Duration,
    ) -> Self {
        Self {
            options,
            phase,
            stats,
            cpu,
            device,
            drain_elapsed,
        }
    }

    fn operations_per_second(&self) -> f64 {
        rate(self.phase.operations(), self.phase.elapsed)
    }

    fn logical_mib_per_second(&self) -> f64 {
        let bytes = self.phase.read_bytes.saturating_add(self.phase.write_bytes);
        rate(bytes, self.phase.elapsed) / (1024.0 * 1024.0)
    }

    fn actual_engine(&self) -> &'static str {
        if self.stats.io_uring_active {
            "io_uring"
        } else {
            "sync"
        }
    }

    fn latency_p99_us(&self) -> f64 {
        ns_to_us(self.phase.latency.percentile_ns(990))
    }

    fn hit_percent(&self) -> f64 {
        let lookups = self.phase.hits.saturating_add(self.phase.misses);
        if lookups == 0 {
            0.0
        } else {
            self.phase.hits as f64 / lookups as f64 * 100.0
        }
    }

    fn cache_write_amplification(&self) -> Option<f64> {
        amplification(self.stats.cache_bytes_written, self.phase.write_bytes)
    }

    fn engine_write_amplification(&self) -> Option<f64> {
        amplification(self.stats.host_write_bytes, self.stats.admitted_value_bytes)
    }

    fn engine_write_amplification_milli(&self) -> Option<u64> {
        if self.stats.admitted_value_bytes == 0 {
            return None;
        }
        let milli = u128::from(self.stats.host_write_bytes).saturating_mul(1_000)
            / u128::from(self.stats.admitted_value_bytes);
        Some(u64::try_from(milli).unwrap_or(u64::MAX))
    }

    fn device_write_amplification(&self) -> Option<f64> {
        if self.device.available {
            amplification(self.device.bytes_written(), self.phase.write_bytes)
        } else {
            None
        }
    }

    fn process_cpu_percent(&self) -> Option<f64> {
        if !self.cpu.available || self.phase.elapsed.is_zero() {
            return None;
        }
        Some(self.cpu.total_seconds() / self.phase.elapsed.as_secs_f64() * 100.0)
    }

    fn device_stats_status(&self) -> &'static str {
        if self.device.available {
            "available"
        } else if self.options.device_stat.is_none() {
            "not_requested"
        } else if cfg!(target_os = "linux") {
            "unavailable"
        } else {
            "unsupported_platform"
        }
    }

    fn acceptance_passed(&self) -> bool {
        self.acceptance_failures().is_empty()
    }

    fn policy_activity_failures(&self) -> Vec<String> {
        let mut failures = Vec::with_capacity(3);
        if self.stats.regions_reused == 0 {
            failures.push(
                "required steady-state activity was not observed (regions_reused=0)".to_owned(),
            );
        }
        if self.options.admission == AdmissionArg::SecondHit && self.stats.admission_rejections == 0
        {
            failures.push(
                "required SecondHit activity was not observed (admission_rejections=0)".to_owned(),
            );
        }
        if self.options.reclaim == ReclaimArg::SecondChance
            && (self.stats.reinsert_queued == 0 || self.stats.reinsert_completed == 0)
        {
            failures.push(format!(
                "required SecondChance activity was not observed (reinsert_queued={}, reinsert_completed={})",
                self.stats.reinsert_queued,
                self.stats.reinsert_completed
            ));
        }
        failures
    }

    fn policy_activity_passed(&self) -> bool {
        !self.options.require_policy_activity || self.policy_activity_failures().is_empty()
    }

    fn acceptance_failures(&self) -> Vec<String> {
        let mut failures = Vec::with_capacity(8);
        if self.phase.operations() == 0 {
            failures.push("no operations completed".to_owned());
        }
        if self.phase.errors != 0 || self.stats.io_errors != 0 {
            failures.push(format!(
                "errors were observed (operations={}, io={})",
                self.phase.errors, self.stats.io_errors
            ));
        }
        let unexpected_rejections = self
            .phase
            .rejected
            .saturating_sub(self.phase.policy_rejections);
        let unexpected_write_rejections = self
            .phase
            .write_rejections
            .saturating_sub(self.phase.policy_rejections);
        let unexpected_cache_rejections = self
            .stats
            .cache_rejected
            .saturating_sub(self.stats.admission_rejections);
        if unexpected_rejections != 0
            || unexpected_write_rejections != 0
            || unexpected_cache_rejections != 0
        {
            failures.push(format!(
                "rejections were observed (operations={}, writes={}, cache={})",
                unexpected_rejections, unexpected_write_rejections, unexpected_cache_rejections
            ));
        }
        if self.stats.memory_used_bytes > self.stats.memory_budget_bytes {
            failures.push(format!(
                "memory_used_bytes {} exceeds memory_budget_bytes {}",
                self.stats.memory_used_bytes, self.stats.memory_budget_bytes
            ));
        }
        if self.stats.memory_peak_bytes > self.stats.memory_budget_bytes {
            failures.push(format!(
                "memory_peak_bytes {} exceeds memory_budget_bytes {}",
                self.stats.memory_peak_bytes, self.stats.memory_budget_bytes
            ));
        }
        if self.options.io_mode == IoModeArg::Direct {
            if !self.stats.direct_io_active {
                failures.push("required direct I/O is not active".to_owned());
            } else if self.stats.direct_io_operations == 0 {
                failures.push("required direct mode completed no O_DIRECT operations".to_owned());
            }
        }
        let operations_per_second = self.operations_per_second();
        if let Some(minimum) = self.options.min_ops_per_sec {
            if operations_per_second < minimum {
                failures.push(format!(
                    "operations_per_second {operations_per_second:.3} is below {minimum:.3}"
                ));
            }
        }
        let latency_p99_us = self.latency_p99_us();
        if let Some(maximum) = self.options.max_p99_us {
            if latency_p99_us > maximum {
                failures.push(format!(
                    "latency_p99_us {latency_p99_us:.3} exceeds {maximum:.3}"
                ));
            }
        }
        let hit_percent = self.hit_percent();
        if let Some(minimum) = self.options.min_hit_percent {
            if hit_percent < minimum {
                failures.push(format!(
                    "hit_percent {hit_percent:.3} is below {minimum:.3}"
                ));
            }
        }
        if self.options.require_policy_activity {
            failures.extend(self.policy_activity_failures());
        }
        failures
    }

    fn to_json(&self) -> String {
        let cpu_user_seconds = self.cpu.available.then_some(self.cpu.user_seconds);
        let cpu_system_seconds = self.cpu.available.then_some(self.cpu.system_seconds);
        let cpu_total_seconds = self.cpu.available.then_some(self.cpu.total_seconds());
        let device_sectors_read = self.device.available.then_some(self.device.sectors_read);
        let device_sectors_written = self.device.available.then_some(self.device.sectors_written);
        let device_io_ticks_ms = self.device.available.then_some(self.device.io_ticks_ms);
        let device_bytes_read = self.device.available.then_some(self.device.bytes_read());
        let device_bytes_written = self.device.available.then_some(self.device.bytes_written());
        let device_stat_path = self
            .options
            .device_stat
            .as_ref()
            .map(|path| path.to_string_lossy());
        let mut output = String::with_capacity(3072);
        write!(
            output,
            concat!(
                "{{\"schema_version\":2,",
                "\"path\":\"{}\",",
                "\"capacity_bytes\":{},\"region_size_bytes\":{},",
                "\"object_size_bytes\":{},\"keys\":{},",
                "\"prefill_percent\":{},\"hotset_percent\":{},",
                "\"hot_access_percent\":{},",
                "\"read_percent\":{},\"concurrency\":{},\"queue_depth\":{},",
                "\"append_lanes\":{},",
                "\"admission\":\"{}\",\"reclaim\":\"{}\",",
                "\"require_policy_activity\":{},",
                "\"api\":\"{}\",",
                "\"engine_requested\":\"{}\",\"engine_actual\":\"{}\",",
                "\"io_mode_requested\":\"{}\",\"direct_io_active\":{},",
                "\"elapsed_seconds\":{:.6},\"operations\":{},",
                "\"reads\":{},\"writes\":{},\"hits\":{},\"misses\":{},",
                "\"hit_percent\":{:.3},",
                "\"stored\":{},\"rejected\":{},\"write_rejections\":{},",
                "\"policy_rejections\":{},\"errors\":{},",
                "\"operations_per_second\":{:.3},",
                "\"logical_bytes_read\":{},\"logical_bytes_written\":{},",
                "\"logical_mib_per_second\":{:.3},",
                "\"latency_p50_us\":{:.3},\"latency_p99_us\":{:.3},",
                "\"latency_p999_us\":{:.3},\"latency_max_us\":{:.3},",
                "\"read_latency_p50_us\":{:.3},\"read_latency_p99_us\":{:.3},",
                "\"write_latency_p50_us\":{:.3},\"write_latency_p99_us\":{:.3}"
            ),
            json_escape(&self.options.path.to_string_lossy()),
            self.options.capacity,
            self.options.region_size,
            self.options.object_size,
            self.options.keys,
            self.options.prefill_percent,
            self.options.hotset_percent,
            self.options.hot_access_percent,
            self.options.read_percent,
            self.options.concurrency,
            self.options.queue_depth,
            self.options.append_lanes,
            self.options.admission.as_str(),
            self.options.reclaim.as_str(),
            self.options.require_policy_activity,
            self.options.api.as_str(),
            self.options.engine.as_str(),
            self.actual_engine(),
            self.options.io_mode.as_str(),
            self.stats.direct_io_active,
            self.phase.elapsed.as_secs_f64(),
            self.phase.operations(),
            self.phase.reads,
            self.phase.writes,
            self.phase.hits,
            self.phase.misses,
            self.hit_percent(),
            self.phase.stored,
            self.phase.rejected,
            self.phase.write_rejections,
            self.phase.policy_rejections,
            self.phase.errors,
            self.operations_per_second(),
            self.phase.read_bytes,
            self.phase.write_bytes,
            self.logical_mib_per_second(),
            ns_to_us(self.phase.latency.percentile_ns(500)),
            self.latency_p99_us(),
            ns_to_us(self.phase.latency.percentile_ns(999)),
            ns_to_us(self.phase.latency.maximum_ns),
            ns_to_us(self.phase.read_latency.percentile_ns(500)),
            ns_to_us(self.phase.read_latency.percentile_ns(990)),
            ns_to_us(self.phase.write_latency.percentile_ns(500)),
            ns_to_us(self.phase.write_latency.percentile_ns(990)),
        )
        .expect("writing JSON into a String cannot fail");
        write!(
            output,
            concat!(
                ",\"min_ops_per_sec\":{},\"max_p99_us\":{},",
                "\"min_hit_percent\":{},",
                "\"acceptance_passed\":{},\"policy_activity_passed\":{},",
                "\"cache_bytes_read\":{},\"cache_bytes_written\":{},",
                "\"write_batches\":{},\"records_coalesced\":{},",
                "\"regions_reused\":{},",
                "\"cache_write_amplification\":{},",
                "\"engine_write_amplification\":{},",
                "\"write_amplification_milli\":{},",
                "\"cache_rejected\":{},\"admission_observations\":{},",
                "\"admission_rejections\":{},\"large_object_rejections\":{},",
                "\"namespace_capacity_rejections\":{},",
                "\"namespace_write_budget_rejections\":{},",
                "\"host_write_operations\":{},\"host_write_bytes\":{},",
                "\"foreground_record_bytes\":{},\"reinsertion_bytes\":{},",
                "\"metadata_write_bytes\":{},\"checkpoint_write_bytes\":{},",
                "\"admitted_value_bytes\":{},",
                "\"daily_utc_day\":{},\"daily_window_crossed\":{},",
                "\"daily_host_write_bytes_current\":{},",
                "\"daily_write_budget_rejections_current\":{},",
                "\"reinsert_queued\":{},\"reinsert_dropped\":{},",
                "\"reinsert_stale\":{},\"reinsert_completed\":{},",
                "\"background_regions_reclaimed\":{},",
                "\"reclaim_backlog_rejections\":{},",
                "\"reclaim_records_scanned\":{},",
                "\"reclaim_index_fallbacks\":{},",
                "\"region_used_bytes\":{},\"region_valid_bytes\":{},",
                "\"minimum_region_valid_ratio_bps\":{},",
                "\"async_queue_rejections\":{},",
                "\"memory_budget_bytes\":{},\"memory_used_bytes\":{},",
                "\"memory_peak_bytes\":{},",
                "\"io_submitted\":{},\"io_completed\":{},\"io_errors\":{},",
                "\"io_cancel_requested\":{},\"io_cancelled\":{},",
                "\"io_in_flight_peak\":{},\"io_submit_wait_ns\":{},",
                "\"io_completion_ns\":{},",
                "\"direct_io_operations\":{},\"direct_io_bytes\":{},",
                "\"buffered_io_operations\":{},\"buffered_io_bytes\":{}"
            ),
            json_optional_f64(self.options.min_ops_per_sec),
            json_optional_f64(self.options.max_p99_us),
            json_optional_f64(self.options.min_hit_percent),
            self.acceptance_passed(),
            self.policy_activity_passed(),
            self.stats.cache_bytes_read,
            self.stats.cache_bytes_written,
            self.stats.write_batches,
            self.stats.records_coalesced,
            self.stats.regions_reused,
            json_optional_f64(self.cache_write_amplification()),
            json_optional_f64(self.engine_write_amplification()),
            json_optional_u64(self.engine_write_amplification_milli()),
            self.stats.cache_rejected,
            self.stats.admission_observations,
            self.stats.admission_rejections,
            self.stats.large_object_rejections,
            self.stats.namespace_capacity_rejections,
            self.stats.namespace_write_budget_rejections,
            self.stats.host_write_operations,
            self.stats.host_write_bytes,
            self.stats.foreground_record_bytes,
            self.stats.reinsertion_bytes,
            self.stats.metadata_write_bytes,
            self.stats.checkpoint_write_bytes,
            self.stats.admitted_value_bytes,
            self.stats.daily_utc_day,
            self.stats.daily_window_crossed,
            self.stats.daily_host_write_bytes_current,
            self.stats.daily_write_budget_rejections_current,
            self.stats.reinsert_queued,
            self.stats.reinsert_dropped,
            self.stats.reinsert_stale,
            self.stats.reinsert_completed,
            self.stats.background_regions_reclaimed,
            self.stats.reclaim_backlog_rejections,
            self.stats.reclaim_records_scanned,
            self.stats.reclaim_index_fallbacks,
            self.stats.region_used_bytes,
            self.stats.region_valid_bytes,
            self.stats.minimum_region_valid_ratio_bps,
            self.stats.async_queue_rejections,
            self.stats.memory_budget_bytes,
            self.stats.memory_used_bytes,
            self.stats.memory_peak_bytes,
            self.stats.io_submitted,
            self.stats.io_completed,
            self.stats.io_errors,
            self.stats.io_cancel_requested,
            self.stats.io_cancelled,
            self.stats.io_in_flight_peak,
            self.stats.io_submit_wait_ns,
            self.stats.io_completion_ns,
            self.stats.direct_io_operations,
            self.stats.direct_io_bytes,
            self.stats.buffered_io_operations,
            self.stats.buffered_io_bytes,
        )
        .expect("writing JSON into a String cannot fail");
        write!(
            output,
            concat!(
                ",\"process_cpu_available\":{},",
                "\"process_cpu_user_seconds\":{},",
                "\"process_cpu_system_seconds\":{},",
                "\"process_cpu_total_seconds\":{},",
                "\"process_cpu_percent\":{},",
                "\"device_stat_path\":{},",
                "\"device_stats_status\":\"{}\",",
                "\"device_stats_available\":{},\"device_sector_bytes\":512,",
                "\"device_sectors_read\":{},\"device_sectors_written\":{},",
                "\"device_io_ticks_ms\":{},",
                "\"device_bytes_read\":{},\"device_bytes_written\":{},",
                "\"device_write_amplification\":{},",
                "\"drain_close_ms\":{:.3}}}"
            ),
            self.cpu.available,
            json_optional_f64(cpu_user_seconds),
            json_optional_f64(cpu_system_seconds),
            json_optional_f64(cpu_total_seconds),
            json_optional_f64(self.process_cpu_percent()),
            json_optional_string(device_stat_path.as_deref()),
            self.device_stats_status(),
            self.device.available,
            json_optional_u64(device_sectors_read),
            json_optional_u64(device_sectors_written),
            json_optional_u64(device_io_ticks_ms),
            json_optional_u64(device_bytes_read),
            json_optional_u64(device_bytes_written),
            json_optional_f64(self.device_write_amplification()),
            self.drain_elapsed.as_secs_f64() * 1000.0,
        )
        .expect("writing JSON into a String cannot fail");
        output
    }

    fn print_human(&self) {
        println!("cache-rs real-file benchmark");
        println!("  path:              {}", self.options.path.display());
        println!(
            "  API/engine/mode:   {}/{}/{} (actual engine: {}, direct: {})",
            self.options.api.as_str(),
            self.options.engine.as_str(),
            self.options.io_mode.as_str(),
            self.actual_engine(),
            self.stats.direct_io_active
        );
        println!(
            "  workload:          {} byte objects, {} keys, {}% reads",
            self.options.object_size, self.options.keys, self.options.read_percent
        );
        println!(
            "  workload shape:    {}% prefill, {}% hot keys receive {}% accesses",
            self.options.prefill_percent,
            self.options.hotset_percent,
            self.options.hot_access_percent
        );
        println!(
            "  concurrency/QD:    {}/{} (append lanes: {})",
            self.options.concurrency, self.options.queue_depth, self.options.append_lanes
        );
        println!(
            "  cache policy:      admission={} reclaim={}",
            self.options.admission.as_str(),
            self.options.reclaim.as_str()
        );
        println!(
            "  elapsed:           {:.3} s",
            self.phase.elapsed.as_secs_f64()
        );
        println!(
            "  throughput:        {:.0} ops/s",
            self.operations_per_second()
        );
        println!(
            "  logical bandwidth: {:.2} MiB/s (read {} B, write {} B)",
            self.logical_mib_per_second(),
            self.phase.read_bytes,
            self.phase.write_bytes
        );
        println!(
            "  outcomes:          reads={} writes={} hits={} misses={} hit={:.3}% stored={} rejected={} policy_rejected={} errors={}",
            self.phase.reads,
            self.phase.writes,
            self.phase.hits,
            self.phase.misses,
            self.hit_percent(),
            self.phase.stored,
            self.phase.rejected,
            self.phase.policy_rejections,
            self.phase.errors
        );
        println!(
            "  latency all:       p50={} p99={} p99.9={} max={}",
            format_latency(self.phase.latency.percentile_ns(500)),
            format_latency(self.phase.latency.percentile_ns(990)),
            format_latency(self.phase.latency.percentile_ns(999)),
            format_latency(self.phase.latency.maximum_ns)
        );
        println!(
            "  latency read:      p50={} p99={}",
            format_latency(self.phase.read_latency.percentile_ns(500)),
            format_latency(self.phase.read_latency.percentile_ns(990))
        );
        println!(
            "  latency write:     p50={} p99={}",
            format_latency(self.phase.write_latency.percentile_ns(500)),
            format_latency(self.phase.write_latency.percentile_ns(990))
        );
        println!(
            "  cache bytes:       read={} write={}",
            self.stats.cache_bytes_read, self.stats.cache_bytes_written
        );
        println!(
            "  memory:            used={} B peak={} B budget={} B",
            self.stats.memory_used_bytes,
            self.stats.memory_peak_bytes,
            self.stats.memory_budget_bytes
        );
        println!(
            "  write amp:         cache={} engine={} device={}",
            format_amplification(self.cache_write_amplification()),
            format_amplification(self.engine_write_amplification()),
            format_amplification(self.device_write_amplification())
        );
        println!(
            "  M7 policy:         admission reject={} Region reuse={} reinsertion={} B (queued={}, complete={}), background reclaim={} backlog_reject={}",
            self.stats.admission_rejections,
            self.stats.regions_reused,
            self.stats.reinsertion_bytes,
            self.stats.reinsert_queued,
            self.stats.reinsert_completed,
            self.stats.background_regions_reclaimed,
            self.stats.reclaim_backlog_rejections
        );
        println!(
            "  reclaim scan:      records={} full_index_fallbacks={}",
            self.stats.reclaim_records_scanned, self.stats.reclaim_index_fallbacks
        );
        println!(
            "  policy activity:   required={} passed={}",
            self.options.require_policy_activity,
            self.policy_activity_passed()
        );
        println!(
            "  daily writes:      UTC day={} current={} B budget_reject={} window_crossed={}",
            self.stats.daily_utc_day,
            self.stats.daily_host_write_bytes_current,
            self.stats.daily_write_budget_rejections_current,
            self.stats.daily_window_crossed
        );
        println!(
            "  Region validity:   {}/{} B (minimum {} bps)",
            self.stats.region_valid_bytes,
            self.stats.region_used_bytes,
            self.stats.minimum_region_valid_ratio_bps
        );
        if self.cpu.available {
            println!(
                "  process CPU:       user={:.3}s system={:.3}s total={:.3}s ({:.1}%)",
                self.cpu.user_seconds,
                self.cpu.system_seconds,
                self.cpu.total_seconds(),
                self.process_cpu_percent().unwrap_or(0.0)
            );
        } else {
            println!("  process CPU:       unavailable (Linux /proc/self/stat only)");
        }
        if self.device.available {
            println!(
                "  block device:      sectors read={} written={} io_ticks={}ms (512 B/sector)",
                self.device.sectors_read, self.device.sectors_written, self.device.io_ticks_ms
            );
        } else if self.options.device_stat.is_none() {
            println!("  block device:      unavailable (no --device-stat supplied)");
        } else if cfg!(target_os = "linux") {
            println!("  block device:      unavailable");
        } else {
            println!("  block device:      unavailable (--device-stat is Linux-only)");
        }
        println!(
            "  device requests:   submitted={} completed={} errors={} peak_in_flight={}",
            self.stats.io_submitted,
            self.stats.io_completed,
            self.stats.io_errors,
            self.stats.io_in_flight_peak
        );
        println!(
            "  I/O path:          direct={} ops/{} B, buffered={} ops/{} B (active={})",
            self.stats.direct_io_operations,
            self.stats.direct_io_bytes,
            self.stats.buffered_io_operations,
            self.stats.buffered_io_bytes,
            self.stats.direct_io_active
        );
        println!(
            "  write batching:    batches={} coalesced_records={}",
            self.stats.write_batches, self.stats.records_coalesced
        );
        println!(
            "  final drain/close: {:.3} ms",
            self.drain_elapsed.as_secs_f64() * 1000.0
        );
        println!(
            "  acceptance:        {} (min ops/s={}, max p99 us={}, min hit %={})",
            if self.acceptance_passed() {
                "pass"
            } else {
                "FAIL"
            },
            format_optional_number(self.options.min_ops_per_sec),
            format_optional_number(self.options.max_p99_us),
            format_optional_number(self.options.min_hit_percent)
        );
    }
}

fn rate(value: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        0.0
    } else {
        value as f64 / seconds
    }
}

fn amplification(physical_bytes: u64, logical_write_bytes: u64) -> Option<f64> {
    if logical_write_bytes == 0 {
        None
    } else {
        Some(physical_bytes as f64 / logical_write_bytes as f64)
    }
}

fn format_amplification(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.3}x"))
}

fn format_optional_number(value: Option<f64>) -> String {
    value.map_or_else(|| "n/a".to_owned(), |value| format!("{value:.3}"))
}

fn json_optional_f64(value: Option<f64>) -> String {
    match value {
        Some(value) if value.is_finite() => format!("{value:.6}"),
        _ => "null".to_owned(),
    }
}

fn json_optional_u64(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn json_optional_string(value: Option<&str>) -> String {
    value.map_or_else(
        || "null".to_owned(),
        |value| format!("\"{}\"", json_escape(value)),
    )
}

fn ns_to_us(nanoseconds: u64) -> f64 {
    nanoseconds as f64 / 1000.0
}

fn format_latency(nanoseconds: u64) -> String {
    if nanoseconds >= 1_000_000 {
        format!("{:.3} ms", nanoseconds as f64 / 1_000_000.0)
    } else {
        format!("{:.3} us", ns_to_us(nanoseconds))
    }
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                write!(escaped, "\\u{:04x}", u32::from(character))
                    .expect("writing JSON escape into a String cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}

fn print_help() {
    println!(
        r#"cache-bench — real-file cache-rs workload harness

The path must be dedicated to cache-rs. Every run destructively resets an
existing recognized Format V1 cache or formats a missing/empty path. Existing
non-cache contents, non-regular files, and symbolic links are rejected.

Usage:
  cargo run --release --bin cache-bench -- \
    --path /mnt/nvme/cache-rs.bench --capacity 64GiB [options]

Required:
  --path PATH                 Dedicated cache file
  --capacity BYTES            Cache capacity (supports KiB/MiB/GiB/TiB)

Workload:
  --object-size BYTES         Value size (default: 4KiB)
  --keys COUNT                Working-set objects (default: 100000)
  --read-percent 0..100       Read share (default: 80)
  --prefill-percent 0..100    Initial key-set fill (default: 100)
  --hotset-percent 1..100     Fraction of keys in hot set (default: 100)
  --hot-access-percent 0..100 Accesses routed to hot set (default: 100)
  --concurrency COUNT         Client worker threads (default: 16)
  --warmup-secs SECONDS       Warmup duration; 0 disables (default: 5)
  --duration-secs SECONDS     Measurement duration (default: 30)
  --seed U64                  Decimal or 0x-prefixed deterministic seed

Cache and device:
  --region-size BYTES         Region size (default: 32MiB)
  --queue-depth 1..4096       Cache and device queue depth (default: 128)
  --append-lanes 1..8         Independent Active Regions (default: 2)
  --api sync|async           Public API exercised (default: sync)
  --engine sync|auto|uring    I/O engine (default: auto)
  --mode buffered|auto|direct File I/O mode (default: buffered)
  --admission always|second-hit
                              New-object admission (default: always)
  --reclaim fifo|second-chance
                              Region reclaim policy (default: fifo)
  --memory-budget BYTES       Engine logical memory budget (default: 1GiB)
  --index-slots COUNT         Compact-index slots (default: next pow2 of 2*keys)
  --device-stat PATH          Linux block stat file, e.g. /sys/block/nvme0n1/stat

Acceptance:
  --min-ops-per-sec NUMBER    Fail with exit code 2 below this throughput
  --max-p99-us NUMBER         Fail with exit code 2 above this overall p99
  --min-hit-percent 0..100    Fail with exit code 2 below this hit percentage
  --require-policy-activity  Require steady-state M7 policy activity

Output:
  --output json|human         Single-line JSON or readable text (default: json)
  -h, --help                  Show this help

Each client thread calls the selected public API. Async mode reuses one
AsyncDiskCache handle per benchmark phase and waits each request to completion.
Measured latency spans the complete cache call, including device I/O.
Process CPU uses Linux /proc/self/stat. Linux block stat sectors are reported
using the kernel ABI's 512-byte sector unit; unavailable metrics are explicit.
Admission-filter rejects are expected policy outcomes and are reported
separately; overload, budget, queue, and I/O rejects still fail acceptance.
Every policy-activity-gated run requires Region reuse. SecondHit additionally
requires at least one admission rejection; SecondChance additionally requires
at least one queued and completed reinsertion.
"#
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(0);

    struct TestPath(PathBuf);

    impl TestPath {
        fn new(name: &str) -> Self {
            let nonce = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(
                std::env::temp_dir()
                    .join(format!("cache-bench-{name}-{}-{nonce}", std::process::id())),
            )
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestPath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
            let _ = std::fs::remove_dir(&self.0);
        }
    }

    #[test]
    fn byte_parser_accepts_binary_units_and_rejects_unknown_suffixes() {
        assert_eq!(parse_bytes("4KiB", "size").unwrap(), 4096);
        assert_eq!(parse_bytes("2_mib", "size").unwrap(), 2 * 1024 * 1024);
        assert_eq!(parse_bytes("3G", "size").unwrap(), 3 * 1024 * 1024 * 1024);
        assert!(parse_bytes("4watts", "size").is_err());
    }

    #[test]
    fn missing_value_does_not_consume_the_next_option() {
        let mut parser = ArgParser::new(
            ["--path", "--capacity=128MiB"]
                .into_iter()
                .map(str::to_owned),
        );
        assert_eq!(parser.next().as_deref(), Some("--path"));
        assert_eq!(
            parser.next_value("--path").unwrap_err(),
            "missing value for --path"
        );
        assert_eq!(parser.next().as_deref(), Some("--capacity=128MiB"));
    }

    #[test]
    fn benchmark_target_must_be_missing_or_a_regular_file() {
        let target = TestPath::new("target-validation");
        assert!(validate_benchmark_target(target.path()).is_ok());

        std::fs::write(target.path(), b"existing cache").unwrap();
        assert!(validate_benchmark_target(target.path()).is_ok());
        std::fs::remove_file(target.path()).unwrap();

        std::fs::create_dir(target.path()).unwrap();
        assert!(
            validate_benchmark_target(target.path())
                .unwrap_err()
                .contains("not a regular file")
        );
        std::fs::remove_dir(target.path()).unwrap();

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("missing-target", target.path()).unwrap();
            assert!(
                validate_benchmark_target(target.path())
                    .unwrap_err()
                    .contains("symbolic link")
            );
        }
    }

    #[test]
    fn fresh_start_resets_recognized_cache_and_preserves_unrecognized_bytes() {
        let target = TestPath::new("fresh-start");
        let config = CacheConfig::new(target.path(), 8 * 1024 + 3 * 16 * 1024)
            .with_region_size(16 * 1024)
            .with_index_slots(128)
            .with_max_key_size(64)
            .with_max_value_size(64);

        let first = open_fresh_benchmark_cache(config.clone(), target.path()).unwrap();
        assert_eq!(
            first
                .put(b"stale", b"value", PutOptions::default())
                .unwrap(),
            PutOutcome::Stored
        );
        first.close().unwrap();

        let second = open_fresh_benchmark_cache(config.clone(), target.path()).unwrap();
        assert_eq!(second.stats().entries, 0);
        assert_eq!(second.get(b"stale").unwrap(), None);
        second.close().unwrap();

        let unrelated = b"not a cache-rs Format V1 file";
        std::fs::write(target.path(), unrelated).unwrap();
        let error = match open_fresh_benchmark_cache(config, target.path()) {
            Ok(cache) => {
                let _ = cache.close();
                panic!("unrecognized bytes must not be formatted")
            }
            Err(error) => error,
        };
        assert!(error.contains("reset requires an existing recognized Format V1 cache"));
        assert_eq!(std::fs::read(target.path()).unwrap(), unrelated);
    }

    #[test]
    fn prefill_verification_checks_every_value_after_flush() {
        let target = TestPath::new("prefill-verification");
        let cache = Arc::new(
            CacheConfig::new(target.path(), 8 * 1024 + 3 * 16 * 1024)
                .with_region_size(16 * 1024)
                .with_index_slots(128)
                .with_max_key_size(64)
                .with_max_value_size(64)
                .open()
                .unwrap(),
        );
        let cache = BenchmarkCache::new(cache, ApiArg::Sync).unwrap();
        let keys = vec![b"first".to_vec(), b"last".to_vec()];
        let value = b"expected".to_vec();

        prefill(&cache, &keys, &value).unwrap();
        cache.flush().unwrap();
        verify_prefill(&cache, &keys, &value).unwrap();

        cache
            .put(&keys[1], b"wrong", PutOptions::default())
            .unwrap();
        assert!(
            verify_prefill(&cache, &keys, &value)
                .unwrap_err()
                .contains("key 1")
        );
        cache.close().unwrap();
    }

    #[test]
    fn async_benchmark_client_uses_facade_and_drains_close() {
        let target = TestPath::new("async-client");
        let cache = Arc::new(
            CacheConfig::new(target.path(), 8 * 1024 + 3 * 16 * 1024)
                .with_region_size(16 * 1024)
                .with_index_slots(128)
                .with_max_key_size(64)
                .with_max_value_size(64)
                .with_submission_queue_depths(8, 8)
                .open()
                .unwrap(),
        );
        let cache = BenchmarkCache::new(cache, ApiArg::Async).unwrap();
        assert_eq!(
            cache.put(b"key", b"value", PutOptions::default()).unwrap(),
            PutOutcome::Stored
        );
        cache.flush().unwrap();
        assert_eq!(cache.get(b"key").unwrap(), Some(b"value".to_vec()));
        cache.close().unwrap();
        assert!(matches!(cache.get(b"key"), Err(CacheError::Closed)));
    }

    #[test]
    fn default_index_sizing_caps_at_256_million_slots() {
        assert_eq!(default_index_slots(100).unwrap(), 1024);
        assert_eq!(default_index_slots(200_000_000).unwrap(), 256 * 1024 * 1024);
    }

    #[test]
    fn command_line_covers_every_workload_dimension() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=8GiB",
                "--region-size=16MiB",
                "--object-size=32KiB",
                "--keys=4096",
                "--read-percent=65",
                "--prefill-percent=25",
                "--hotset-percent=10",
                "--hot-access-percent=90",
                "--concurrency=24",
                "--queue-depth=64",
                "--append-lanes=2",
                "--memory-budget=2GiB",
                "--index-slots=8192",
                "--api=async",
                "--engine=sync",
                "--mode=auto",
                "--admission=second-hit",
                "--reclaim=second-chance",
                "--warmup-secs=0.5",
                "--duration-secs=2",
                "--seed=0x1234",
                "--output=human",
                "--min-ops-per-sec=12345.5",
                "--max-p99-us=900.25",
                "--min-hit-percent=99.5",
                "--require-policy-activity",
                "--device-stat=/sys/block/nvme0n1/stat",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        assert_eq!(options.capacity, 8 * 1024 * 1024 * 1024);
        assert_eq!(options.object_size, 32 * 1024);
        assert_eq!(options.keys, 4096);
        assert_eq!(options.read_percent, 65);
        assert_eq!(options.prefill_percent, 25);
        assert_eq!(options.hotset_percent, 10);
        assert_eq!(options.hot_access_percent, 90);
        assert_eq!(options.concurrency, 24);
        assert_eq!(options.queue_depth, 64);
        assert_eq!(options.append_lanes, 2);
        assert_eq!(options.api, ApiArg::Async);
        assert_eq!(options.seed, 0x1234);
        assert_eq!(options.io_mode, IoModeArg::Auto);
        assert_eq!(options.admission, AdmissionArg::SecondHit);
        assert_eq!(options.reclaim, ReclaimArg::SecondChance);
        assert_eq!(options.warmup, Duration::from_millis(500));
        assert_eq!(options.min_ops_per_sec, Some(12_345.5));
        assert_eq!(options.max_p99_us, Some(900.25));
        assert_eq!(options.min_hit_percent, Some(99.5));
        assert!(options.require_policy_activity);
        assert_eq!(
            options.device_stat,
            Some(PathBuf::from("/sys/block/nvme0n1/stat"))
        );
    }

    #[test]
    fn hot_cold_selector_and_partial_prefill_are_deterministic_and_bounded() {
        assert_eq!(percentage_count(1_000, 25), 250);
        let mut hot_random = XorShift64::new(1);
        let hot = AccessPattern {
            read_percent: 80,
            hotset_keys: 10,
            hot_access_percent: 100,
        };
        assert!((0..100).all(|_| select_key_index(&mut hot_random, 100, hot) < 10));

        let mut cold_random = XorShift64::new(1);
        let cold = AccessPattern {
            hot_access_percent: 0,
            ..hot
        };
        assert!((0..100).all(|_| select_key_index(&mut cold_random, 100, cold) >= 10));
    }

    #[test]
    fn latency_histogram_is_fixed_size_and_reports_monotonic_quantiles() {
        let mut histogram = LatencyHistogram::new();
        for nanoseconds in [10, 100, 1_000, 10_000] {
            histogram.record(Duration::from_nanos(nanoseconds));
        }
        let p50 = histogram.percentile_ns(500);
        let p99 = histogram.percentile_ns(990);
        assert!((100..=125).contains(&p50));
        assert!(p99 >= p50);
        assert_eq!(p99, 10_000);
        assert!(std::mem::size_of::<LatencyHistogram>() <= 5 * 1024);
    }

    #[test]
    fn json_escape_keeps_single_line_output_valid() {
        assert_eq!(json_escape("a\n\"b\\c"), "a\\n\\\"b\\\\c");
    }

    #[test]
    fn linux_process_and_block_stats_are_parsed_by_abi_field_number() {
        let process = parse_process_stat(
            "42 (cache bench worker) R 1 2 3 4 5 6 7 8 9 10 123 45 0 0",
            100,
        )
        .unwrap();
        assert_eq!(process.user_ticks, 123);
        assert_eq!(process.system_ticks, 45);
        assert_eq!(process.ticks_per_second, 100);

        let device = parse_device_stat("1 2 3 4 5 6 7 8 9 10 11").unwrap();
        assert_eq!(device.sectors_read, 3);
        assert_eq!(device.sectors_written, 7);
        assert_eq!(device.io_ticks_ms, 10);
    }

    #[test]
    fn acceptance_thresholds_report_every_failure() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=128MiB",
                "--duration-secs=1",
                "--min-ops-per-sec=101",
                "--max-p99-us=1",
                "--min-hit-percent=90",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        let mut phase = PhaseResult::new();
        phase.elapsed = Duration::from_secs(1);
        phase.reads = 100;
        phase.hits = 80;
        phase.misses = 20;
        phase.latency.record(Duration::from_micros(50));
        let stats = StatsDelta {
            memory_budget_bytes: 1024,
            memory_used_bytes: 512,
            memory_peak_bytes: 768,
            ..StatsDelta::default()
        };
        let report = Report::new(
            &options,
            phase,
            stats,
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert_eq!(report.acceptance_failures().len(), 3);
        assert!(!report.acceptance_passed());
        let json = report.to_json();
        assert!(json.contains("\"schema_version\":2"));
        assert!(json.contains("\"api\":\"sync\""));
        assert!(json.contains("\"acceptance_passed\":false"));
        assert!(json.contains("\"hit_percent\":80.000"));
        assert!(json.contains("\"memory_budget_bytes\":1024"));
        assert!(json.contains("\"memory_used_bytes\":512"));
        assert!(json.contains("\"memory_peak_bytes\":768"));
        assert!(json.contains("\"write_batches\":0"));
        assert!(json.contains("\"records_coalesced\":0"));
        assert!(json.contains("\"reclaim_records_scanned\":0"));
        assert!(json.contains("\"reclaim_index_fallbacks\":0"));
        assert!(json.contains("\"daily_host_write_bytes_current\":0"));
        assert!(json.contains("\"daily_write_budget_rejections_current\":0"));
        assert!(!json.contains("\"daily_host_write_bytes\":"));
        assert!(json.contains("\"device_stats_status\":\"not_requested\""));
    }

    #[test]
    fn acceptance_rejects_empty_erroneous_rejected_and_over_budget_runs() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=128MiB",
                "--duration-secs=1",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        let empty_report = Report::new(
            &options,
            PhaseResult::new(),
            StatsDelta::default(),
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert_eq!(
            empty_report.acceptance_failures(),
            vec!["no operations completed"]
        );

        let mut phase = PhaseResult::new();
        phase.elapsed = Duration::from_secs(1);
        phase.writes = 1;
        phase.rejected = 1;
        phase.write_rejections = 1;
        phase.errors = 2;
        let stats = StatsDelta {
            memory_budget_bytes: 100,
            memory_used_bytes: 101,
            memory_peak_bytes: 102,
            io_errors: 1,
            ..StatsDelta::default()
        };
        let report = Report::new(
            &options,
            phase,
            stats,
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        let failures = report.acceptance_failures().join("; ");
        assert!(failures.contains("errors were observed"));
        assert!(failures.contains("writes=1"));
        assert!(failures.contains("memory_used_bytes 101 exceeds"));
        assert!(failures.contains("memory_peak_bytes 102 exceeds"));
    }

    #[test]
    fn admission_filtering_is_reported_but_not_treated_as_overload() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=128MiB",
                "--duration-secs=1",
                "--admission=second-hit",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        let mut phase = PhaseResult::new();
        phase.elapsed = Duration::from_secs(1);
        phase.writes = 5;
        phase.rejected = 5;
        phase.write_rejections = 5;
        phase.policy_rejections = 5;
        let stats = StatsDelta {
            cache_rejected: 5,
            admission_rejections: 5,
            memory_budget_bytes: 1024,
            memory_used_bytes: 512,
            memory_peak_bytes: 512,
            ..StatsDelta::default()
        };
        let report = Report::new(
            &options,
            phase,
            stats,
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert!(report.acceptance_passed());
        assert!(report.to_json().contains("\"policy_rejections\":5"));
    }

    #[test]
    fn required_policy_activity_gates_second_hit_and_second_chance() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=128MiB",
                "--duration-secs=1",
                "--admission=second-hit",
                "--reclaim=second-chance",
                "--require-policy-activity",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        let mut inactive_phase = PhaseResult::new();
        inactive_phase.elapsed = Duration::from_secs(1);
        inactive_phase.reads = 1;
        inactive_phase.hits = 1;
        let inactive = Report::new(
            &options,
            inactive_phase,
            StatsDelta::default(),
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        let failures = inactive.acceptance_failures().join("; ");
        assert!(failures.contains("regions_reused=0"));
        assert!(failures.contains("SecondHit activity"));
        assert!(failures.contains("SecondChance activity"));
        assert!(!inactive.policy_activity_passed());

        let mut active_phase = PhaseResult::new();
        active_phase.elapsed = Duration::from_secs(1);
        active_phase.reads = 1;
        active_phase.hits = 1;
        let active = Report::new(
            &options,
            active_phase,
            StatsDelta {
                admission_rejections: 1,
                regions_reused: 2,
                reinsert_queued: 3,
                reinsert_completed: 2,
                ..StatsDelta::default()
            },
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert!(active.acceptance_passed());
        let json = active.to_json();
        assert!(json.contains("\"require_policy_activity\":true"));
        assert!(json.contains("\"policy_activity_passed\":true"));
        assert!(json.contains("\"regions_reused\":2"));
    }

    #[test]
    fn stats_delta_preserves_memory_gauges() {
        let before = CacheStats {
            memory_budget_bytes: 1024,
            memory_used_bytes: 128,
            memory_peak_bytes: 256,
            write_batches: 3,
            records_coalesced: 5,
            reclaim_records_scanned: 7,
            reclaim_index_fallbacks: 1,
            ..CacheStats::default()
        };
        let after = CacheStats {
            memory_budget_bytes: 1024,
            memory_used_bytes: 512,
            memory_peak_bytes: 768,
            write_batches: 11,
            records_coalesced: 19,
            regions_reused: 4,
            reclaim_records_scanned: 31,
            reclaim_index_fallbacks: 3,
            ..CacheStats::default()
        };
        let delta = stats_delta(
            before,
            after,
            HostWriteSnapshot::default(),
            HostWriteSnapshot::default(),
        );
        assert_eq!(delta.memory_budget_bytes, 1024);
        assert_eq!(delta.memory_used_bytes, 512);
        assert_eq!(delta.memory_peak_bytes, 768);
        assert_eq!(delta.write_batches, 8);
        assert_eq!(delta.records_coalesced, 14);
        assert_eq!(delta.regions_reused, 4);
        assert_eq!(delta.reclaim_records_scanned, 24);
        assert_eq!(delta.reclaim_index_fallbacks, 2);
    }

    #[test]
    fn daily_stats_are_current_gauges_and_report_utc_rollover() {
        let before = HostWriteSnapshot {
            utc_day: 10,
            daily_host_write_bytes: 1_000,
            daily_budget_rejections: 9,
            ..HostWriteSnapshot::default()
        };
        let after = HostWriteSnapshot {
            utc_day: 11,
            daily_host_write_bytes: 7,
            daily_budget_rejections: 1,
            ..HostWriteSnapshot::default()
        };
        let delta = stats_delta(CacheStats::default(), CacheStats::default(), before, after);
        assert_eq!(delta.daily_utc_day, 11);
        assert!(delta.daily_window_crossed);
        assert_eq!(delta.daily_host_write_bytes_current, 7);
        assert_eq!(delta.daily_write_budget_rejections_current, 1);

        let same_day = stats_delta(
            CacheStats::default(),
            CacheStats::default(),
            after,
            HostWriteSnapshot {
                daily_host_write_bytes: 12,
                daily_budget_rejections: 2,
                ..after
            },
        );
        assert!(!same_day.daily_window_crossed);
        assert_eq!(same_day.daily_host_write_bytes_current, 12);
        assert_eq!(same_day.daily_write_budget_rejections_current, 2);
    }

    #[test]
    fn direct_mode_maps_to_required_direct_io() {
        assert_eq!(cache_io_mode(IoModeArg::Direct), IoMode::Direct);
    }

    #[test]
    fn direct_acceptance_requires_observed_direct_operations() {
        let parsed = Options::parse(
            [
                "--path=/tmp/cache-rs-bench",
                "--capacity=128MiB",
                "--duration-secs=1",
                "--mode=direct",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap();
        let ParseOutcome::Run(options) = parsed else {
            panic!("expected runnable options");
        };
        let mut phase = PhaseResult::new();
        phase.elapsed = Duration::from_secs(1);
        phase.reads = 1;
        phase.hits = 1;
        let inactive = Report::new(
            &options,
            phase,
            StatsDelta::default(),
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert!(
            inactive
                .acceptance_failures()
                .contains(&"required direct I/O is not active".to_owned())
        );

        let mut active_phase = PhaseResult::new();
        active_phase.elapsed = Duration::from_secs(1);
        active_phase.reads = 1;
        active_phase.hits = 1;
        let active_without_io = Report::new(
            &options,
            active_phase,
            StatsDelta {
                direct_io_active: true,
                ..StatsDelta::default()
            },
            ProcessCpuMeasurement::default(),
            DeviceMeasurement::default(),
            Duration::ZERO,
        );
        assert!(
            active_without_io
                .acceptance_failures()
                .contains(&"required direct mode completed no O_DIRECT operations".to_owned())
        );
    }
}
