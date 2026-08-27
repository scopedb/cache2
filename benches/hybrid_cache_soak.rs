use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache_rs::{
    CacheHealth, DetailedCacheSnapshot, HybridCache, HybridCacheConfig, IoEngine, IoMode,
    RuntimeConfig, StartupMode, StaticConfig,
};

const MIB: usize = 1024 * 1024;
const REGION_BYTES: usize = 32 * MIB;
const VALUE_HEADER_BYTES: usize = 16;
const MAX_VALUE_BYTES: usize = REGION_BYTES - 64;
const DELETE_INTERVAL: u64 = 64;
const OVERLOAD_DELAY: Duration = Duration::from_micros(50);
const DEFAULT_VALUE_BYTES: [usize; 4] = [256, 4 * 1024, 16 * 1024, 256 * 1024];

struct SoakConfig {
    duration: Duration,
    sample_period: Duration,
    capacity_bytes: u64,
    memory_bytes: usize,
    memory_limit_bytes: usize,
    value_bytes: Box<[usize]>,
    rss_slack_bytes: usize,
    key_count: usize,
    shards: u32,
    read_io_workers: usize,
    write_io_workers: usize,
    writers: usize,
    readers: usize,
    warm_reopen: bool,
    io_engine: IoEngine,
    io_mode: IoMode,
    directory: PathBuf,
}

impl SoakConfig {
    fn from_env() -> io::Result<Self> {
        let duration = Duration::from_secs(env_u64("CACHE_SOAK_SECONDS", 60)?);
        let sample_period = Duration::from_secs(env_u64("CACHE_SOAK_SAMPLE_SECONDS", 10)?);
        let capacity_bytes = env_u64("CACHE_SOAK_CAPACITY_MIB", 256)?
            .checked_mul(MIB as u64)
            .ok_or_else(|| invalid("soak capacity is too large"))?;
        let memory_mib = env_usize("CACHE_SOAK_MEMORY_MIB", 64)?;
        let memory_bytes = memory_mib
            .checked_mul(MIB)
            .ok_or_else(|| invalid("soak memory capacity is too large"))?;
        let shards = env_u32("CACHE_SOAK_SHARDS", 4)?;
        let shard_count =
            usize::try_from(shards).map_err(|_| invalid("soak shard count does not fit usize"))?;
        let default_memory_limit_mib = shard_count
            .checked_mul(2 * REGION_BYTES)
            .and_then(|bytes| bytes.checked_add(2 * REGION_BYTES))
            .and_then(|bytes| bytes.checked_add(memory_bytes))
            .ok_or_else(|| invalid("soak default memory limit is too large"))?
            .div_ceil(MIB);
        let memory_limit_bytes =
            env_usize("CACHE_SOAK_MEMORY_LIMIT_MIB", default_memory_limit_mib)?
                .checked_mul(MIB)
                .ok_or_else(|| invalid("soak memory limit is too large"))?;
        let value_bytes = env_usize_list("CACHE_SOAK_VALUE_BYTES", &DEFAULT_VALUE_BYTES)?;
        let rss_slack_bytes = env_usize("CACHE_SOAK_RSS_SLACK_MIB", 128)?
            .checked_mul(MIB)
            .ok_or_else(|| invalid("soak RSS slack is too large"))?;
        let key_count = env_usize("CACHE_SOAK_KEYS", 32_768)?;
        let read_io_workers = env_usize("CACHE_SOAK_READ_IO_WORKERS", 4)?;
        let write_io_workers = env_usize("CACHE_SOAK_WRITE_IO_WORKERS", 4)?;
        let writers = env_usize("CACHE_SOAK_WRITERS", 4)?;
        let readers = env_usize("CACHE_SOAK_READERS", 4)?;
        let warm_reopen = env_bool("CACHE_SOAK_WARM_REOPEN", false)?;
        let io_engine = parse_io_engine("CACHE_SOAK_IO_ENGINE")?;
        let io_mode = parse_io_mode("CACHE_SOAK_IO_MODE")?;
        let directory = env::var_os("CACHE_SOAK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);
        if duration.is_zero()
            || sample_period.is_zero()
            || value_bytes.is_empty()
            || value_bytes
                .iter()
                .any(|bytes| !(VALUE_HEADER_BYTES..=MAX_VALUE_BYTES).contains(bytes))
            || key_count == 0
            || shards == 0
            || read_io_workers == 0
            || write_io_workers == 0
            || writers == 0
            || readers == 0
            || !directory.is_dir()
        {
            return Err(invalid(
                "invalid soak duration, topology, value, or directory",
            ));
        }
        Ok(Self {
            duration,
            sample_period,
            capacity_bytes,
            memory_bytes,
            memory_limit_bytes,
            value_bytes,
            rss_slack_bytes,
            key_count,
            shards,
            read_io_workers,
            write_io_workers,
            writers,
            readers,
            warm_reopen,
            io_engine,
            io_mode,
            directory,
        })
    }

    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size(REGION_BYTES as u64)
            .with_expected_entries(self.key_count)
    }

    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_read_io_workers(self.read_io_workers)
            .with_write_io_workers(self.write_io_workers)
            .with_write_shards(self.shards)
            .with_l1_capacity(self.memory_bytes)
            .with_memory_limit(self.memory_limit_bytes)
            .with_statistics(true)
    }
}

struct SoakFiles {
    data: PathBuf,
}

impl SoakFiles {
    fn new(directory: &Path) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache-rs-hybrid-soak-{}-{timestamp}.cache",
                std::process::id()
            )),
        }
    }

    fn logical_bytes(&self) -> io::Result<u64> {
        [
            self.data.clone(),
            sidecar(&self.data, ".state"),
            sidecar(&self.data, ".image"),
            sidecar(&self.data, ".image.next"),
        ]
        .into_iter()
        .try_fold(0_u64, |total, path| match std::fs::metadata(path) {
            Ok(metadata) => total
                .checked_add(metadata.len())
                .ok_or_else(|| invalid("logical disk byte count overflow")),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
            Err(error) => Err(error),
        })
    }
}

impl Drop for SoakFiles {
    fn drop(&mut self) {
        for path in [
            self.data.clone(),
            sidecar(&self.data, ".state"),
            sidecar(&self.data, ".image"),
            sidecar(&self.data, ".image.next"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[derive(Default)]
struct SoakCounters {
    writes: AtomicU64,
    deletes: AtomicU64,
    write_rejections: AtomicU64,
    delete_rejections: AtomicU64,
    hits: AtomicU64,
    stale_hits: AtomicU64,
    misses: AtomicU64,
    max_put_ns: AtomicU64,
    max_delete_ns: AtomicU64,
    max_get_ns: AtomicU64,
}

#[derive(Clone, Copy)]
struct CounterSnapshot {
    writes: u64,
    deletes: u64,
    write_rejections: u64,
    delete_rejections: u64,
    hits: u64,
    stale_hits: u64,
    misses: u64,
    max_put_ns: u64,
    max_delete_ns: u64,
    max_get_ns: u64,
}

impl SoakCounters {
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            writes: self.writes.load(Ordering::Relaxed),
            deletes: self.deletes.load(Ordering::Relaxed),
            write_rejections: self.write_rejections.load(Ordering::Relaxed),
            delete_rejections: self.delete_rejections.load(Ordering::Relaxed),
            hits: self.hits.load(Ordering::Relaxed),
            stale_hits: self.stale_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            max_put_ns: self.max_put_ns.load(Ordering::Relaxed),
            max_delete_ns: self.max_delete_ns.load(Ordering::Relaxed),
            max_get_ns: self.max_get_ns.load(Ordering::Relaxed),
        }
    }
}

struct ResourceSample {
    detailed: DetailedCacheSnapshot,
    logical_bytes: u64,
    current_rss: u64,
    rss_limit: u64,
}

fn main() -> io::Result<()> {
    let config = SoakConfig::from_env()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.readers.max(2))
        .thread_name("cache-rs-soak")
        .enable_time()
        .build()?;
    let files = SoakFiles::new(&config.directory);
    let static_config = config.static_config();
    let peak_disk_bytes = static_config.peak_disk_bytes()?;
    let mut cache = open_cache(&runtime, &files, &config)?;
    let keys: Vec<Box<[u8]>> = (0..config.key_count)
        .map(|ordinal| {
            format!("soak-key-{ordinal:016x}")
                .into_bytes()
                .into_boxed_slice()
        })
        .collect();
    let key_count = u64::try_from(keys.len()).map_err(|_| invalid("soak key count exceeds u64"))?;
    let value_size_count = u64::try_from(config.value_bytes.len())
        .map_err(|_| invalid("soak value-size count exceeds u64"))?;
    let expected: Vec<_> = (0..config.key_count).map(|_| AtomicU64::new(0)).collect();
    let first_write = if config.warm_reopen {
        let next = populate_for_warm_reopen(
            &cache,
            &config,
            &keys,
            &expected,
            key_count,
            value_size_count,
        )?;
        runtime.block_on(cache.drain())?;
        runtime.block_on(cache.close_warm())?;
        cache = open_cache(&runtime, &files, &config)?;
        if cache.startup_mode() != StartupMode::Warm {
            return Err(io::Error::other(
                "soak warm-reopen preparation did not recover a clean image",
            ));
        }
        next
    } else {
        0
    };
    let next_write = AtomicU64::new(first_write);
    let next_read = AtomicU64::new(0);
    let stop = AtomicBool::new(false);
    let client_count = config
        .writers
        .checked_add(config.readers)
        .ok_or_else(|| invalid("soak client count is too large"))?;
    let counters = SoakCounters::default();
    let started = Instant::now();
    let deadline = started
        .checked_add(config.duration)
        .ok_or_else(|| invalid("soak deadline is too far in the future"))?;
    let mut max_managed_memory = 0_usize;

    println!(
        "cache-rs soak duration={}s capacity={:.1}MiB memory={:.1}MiB memory_limit={:.1}MiB values={} keys={} shards={} read_io_workers={} write_io_workers={} writers={} readers={} warm_reopen={} delete_interval={} engine={:?} mode={:?} peak_disk={} rss_slack={}",
        config.duration.as_secs(),
        config.capacity_bytes as f64 / MIB as f64,
        config.memory_bytes as f64 / MIB as f64,
        config.memory_limit_bytes as f64 / MIB as f64,
        config
            .value_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(","),
        config.key_count,
        config.shards,
        config.read_io_workers,
        config.write_io_workers,
        config.writers,
        config.readers,
        config.warm_reopen,
        DELETE_INTERVAL,
        config.io_engine,
        config.io_mode,
        peak_disk_bytes,
        config.rss_slack_bytes,
    );

    thread::scope(|scope| -> io::Result<()> {
        let mut workers = Vec::with_capacity(client_count);
        for _ in 0..config.writers {
            workers.push(scope.spawn(|| {
                let result = run_writer(
                    &cache,
                    &config,
                    &keys,
                    &expected,
                    key_count,
                    value_size_count,
                    &next_write,
                    &stop,
                    &counters,
                );
                if result.is_err() {
                    stop.store(true, Ordering::Release);
                }
                result
            }));
        }
        for reader_id in 0..config.readers {
            let cache = &cache;
            let config = &config;
            let keys = &keys;
            let expected = &expected;
            let next_read = &next_read;
            let stop = &stop;
            let counters = &counters;
            let runtime = runtime.handle().clone();
            workers.push(scope.spawn(move || {
                let result = run_reader(
                    cache,
                    config,
                    keys,
                    expected,
                    key_count,
                    value_size_count,
                    reader_id,
                    next_read,
                    stop,
                    counters,
                    &runtime,
                );
                if result.is_err() {
                    stop.store(true, Ordering::Release);
                }
                result
            }));
        }

        let mut next_sample = started
            .checked_add(config.sample_period)
            .ok_or_else(|| invalid("soak sample deadline is too far in the future"))?;
        let mut sample_error = None;
        while Instant::now() < deadline && !stop.load(Ordering::Acquire) {
            let wake_at = std::cmp::min(next_sample, deadline);
            if let Some(remaining) = wake_at.checked_duration_since(Instant::now()) {
                thread::sleep(remaining);
            }
            let now = Instant::now();
            if now >= next_sample && now < deadline {
                match resource_sample(
                    &cache,
                    &files,
                    peak_disk_bytes,
                    config.rss_slack_bytes,
                    true,
                    runtime.handle(),
                ) {
                    Ok(sample) => {
                        max_managed_memory = max_managed_memory
                            .max(sample.detailed.summary.managed_memory_peak_bytes);
                        report_sample(
                            false,
                            started.elapsed(),
                            counters.snapshot(),
                            &sample,
                            max_managed_memory,
                        );
                    }
                    Err(error) => {
                        sample_error = Some(error);
                        stop.store(true, Ordering::Release);
                    }
                }
                next_sample = now.checked_add(config.sample_period).unwrap_or(deadline);
            }
        }
        stop.store(true, Ordering::Release);

        let mut worker_error = None;
        for worker in workers {
            match worker.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) if worker_error.is_none() => worker_error = Some(error),
                Ok(Err(_)) => {}
                Err(_) if worker_error.is_none() => {
                    worker_error = Some(io::Error::other("soak worker panicked"));
                }
                Err(_) => {}
            }
        }
        match sample_error.or(worker_error) {
            Some(error) => Err(error),
            None => Ok(()),
        }
    })?;

    runtime.block_on(cache.drain())?;
    let sample = resource_sample(
        &cache,
        &files,
        peak_disk_bytes,
        config.rss_slack_bytes,
        false,
        runtime.handle(),
    )?;
    max_managed_memory = max_managed_memory.max(sample.detailed.summary.managed_memory_peak_bytes);
    runtime.block_on(async { cache.close_fast().await })?;
    report_sample(
        true,
        started.elapsed(),
        counters.snapshot(),
        &sample,
        max_managed_memory,
    );
    Ok(())
}

fn open_cache(
    runtime: &tokio::runtime::Runtime,
    files: &SoakFiles,
    config: &SoakConfig,
) -> io::Result<HybridCache> {
    runtime.block_on(async {
        HybridCacheConfig::from_static(&files.data, config.static_config())
            .with_runtime_config(config.runtime_config())
            .open()
            .await
    })
}

fn populate_for_warm_reopen(
    cache: &HybridCache,
    config: &SoakConfig,
    keys: &[Box<[u8]>],
    expected: &[AtomicU64],
    key_count: u64,
    value_size_count: u64,
) -> io::Result<u64> {
    let total = key_count
        .checked_mul(value_size_count)
        .ok_or_else(|| invalid("warm-reopen population count overflow"))?;
    let maximum_value_bytes = config.value_bytes.iter().copied().max().unwrap_or(0);
    let mut value = vec![0_u8; maximum_value_bytes];
    for ordinal in 0..total {
        let announced = ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("warm-reopen write ordinal exhausted"))?;
        let key_index = usize::try_from(ordinal % key_count)
            .map_err(|_| invalid("warm-reopen key index exceeds usize"))?;
        let value_index = usize::try_from(ordinal / key_count % value_size_count)
            .map_err(|_| invalid("warm-reopen value-size index exceeds usize"))?;
        let value_bytes = config.value_bytes[value_index];
        let pattern = (ordinal ^ key_index as u64) as u8;
        value[..value_bytes].fill(pattern);
        value[..8].copy_from_slice(&ordinal.to_le_bytes());
        value[8..VALUE_HEADER_BYTES].copy_from_slice(&(key_index as u64).to_le_bytes());
        expected[key_index].fetch_max(announced, Ordering::SeqCst);
        loop {
            match cache.put(&keys[key_index], &value[..value_bytes]) {
                Ok(_) => break,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(OVERLOAD_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(total)
}

#[allow(clippy::too_many_arguments)]
fn run_writer(
    cache: &HybridCache,
    config: &SoakConfig,
    keys: &[Box<[u8]>],
    expected: &[AtomicU64],
    key_count: u64,
    value_size_count: u64,
    next_write: &AtomicU64,
    stop: &AtomicBool,
    counters: &SoakCounters,
) -> io::Result<()> {
    let maximum_value_bytes = config.value_bytes.iter().copied().max().unwrap_or(0);
    let mut value = vec![0_u8; maximum_value_bytes];
    while !stop.load(Ordering::Acquire) {
        let ordinal = next_write.fetch_add(1, Ordering::Relaxed);
        let announced = ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("soak write ordinal exhausted"))?;
        let key_index = usize::try_from(ordinal % key_count)
            .map_err(|_| invalid("soak key index exceeds usize"))?;
        let value_index = usize::try_from(ordinal / key_count % value_size_count)
            .map_err(|_| invalid("soak value-size index exceeds usize"))?;
        let value_bytes = config.value_bytes[value_index];
        let pattern = (ordinal ^ key_index as u64) as u8;
        value[..value_bytes].fill(pattern);
        value[..8].copy_from_slice(&ordinal.to_le_bytes());
        value[8..VALUE_HEADER_BYTES].copy_from_slice(&(key_index as u64).to_le_bytes());

        // Announce before publication so a reader can never classify a newly
        // visible value as future. Rejected attempts only make validation more
        // conservative; stale and missing values are permitted.
        expected[key_index].fetch_max(announced, Ordering::SeqCst);
        let put_started = Instant::now();
        match cache.put(&keys[key_index], &value[..value_bytes]) {
            Ok(_) => {
                record_latency(&counters.max_put_ns, put_started.elapsed());
                counters.writes.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                record_latency(&counters.max_put_ns, put_started.elapsed());
                counters.write_rejections.fetch_add(1, Ordering::Relaxed);
                thread::sleep(OVERLOAD_DELAY);
                continue;
            }
            Err(error) => return Err(error),
        }

        if announced.is_multiple_of(DELETE_INTERVAL) {
            let delete_started = Instant::now();
            match cache.delete(&keys[key_index]) {
                Ok(_) => {
                    record_latency(&counters.max_delete_ns, delete_started.elapsed());
                    counters.deletes.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    record_latency(&counters.max_delete_ns, delete_started.elapsed());
                    counters.delete_rejections.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(OVERLOAD_DELAY);
                }
                Err(error) => return Err(error),
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_reader(
    cache: &HybridCache,
    config: &SoakConfig,
    keys: &[Box<[u8]>],
    expected: &[AtomicU64],
    key_count: u64,
    value_size_count: u64,
    reader_id: usize,
    next_read: &AtomicU64,
    stop: &AtomicBool,
    counters: &SoakCounters,
    runtime: &tokio::runtime::Handle,
) -> io::Result<()> {
    let reader_id = u64::try_from(reader_id).map_err(|_| invalid("reader id exceeds u64"))?;
    while !stop.load(Ordering::Acquire) {
        let ordinal = next_read.fetch_add(1, Ordering::Relaxed);
        let sampled = usize::try_from(ordinal.wrapping_mul(17).wrapping_add(reader_id) % key_count)
            .map_err(|_| invalid("soak sampled key exceeds usize"))?;
        let get_started = Instant::now();
        match runtime.block_on(cache.get(&keys[sampled]))? {
            Some(observed) => {
                record_latency(&counters.max_get_ns, get_started.elapsed());
                let latest = expected[sampled].load(Ordering::SeqCst);
                let stale = validate_observed(
                    sampled,
                    &observed,
                    latest,
                    key_count,
                    value_size_count,
                    &config.value_bytes,
                )?;
                counters.hits.fetch_add(1, Ordering::Relaxed);
                if stale {
                    counters.stale_hits.fetch_add(1, Ordering::Relaxed);
                }
            }
            None => {
                record_latency(&counters.max_get_ns, get_started.elapsed());
                counters.misses.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

fn validate_observed(
    sampled: usize,
    observed: &[u8],
    latest: u64,
    key_count: u64,
    value_size_count: u64,
    value_bytes: &[usize],
) -> io::Result<bool> {
    if observed.len() < VALUE_HEADER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "soak read returned a truncated value",
        ));
    }
    let sequence = u64::from_le_bytes(observed[..8].try_into().unwrap());
    let observed_key = u64::from_le_bytes(observed[8..VALUE_HEADER_BYTES].try_into().unwrap());
    let observed_version = sequence
        .checked_add(1)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "soak value version overflow"))?;
    if latest == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "soak read returned a value for an unwritten key",
        ));
    }
    if observed_key != sampled as u64 || observed_version > latest {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "soak read returned a wrong-key or future value",
        ));
    }
    let value_index = usize::try_from(sequence / key_count % value_size_count)
        .map_err(|_| invalid("observed value-size index exceeds usize"))?;
    let expected_length = value_bytes[value_index];
    let expected_pattern = (sequence ^ sampled as u64) as u8;
    if observed.len() != expected_length
        || observed[VALUE_HEADER_BYTES..]
            .iter()
            .any(|byte| *byte != expected_pattern)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "soak read returned a malformed value",
        ));
    }
    Ok(observed_version < latest)
}

fn resource_sample(
    cache: &HybridCache,
    files: &SoakFiles,
    peak_disk_bytes: u64,
    rss_slack_bytes: usize,
    drain: bool,
    runtime: &tokio::runtime::Handle,
) -> io::Result<ResourceSample> {
    if drain {
        runtime.block_on(cache.drain())?;
    }
    let logical_bytes = files.logical_bytes()?;
    if logical_bytes > peak_disk_bytes {
        return Err(io::Error::other("soak exceeded the logical disk bound"));
    }
    let detailed = cache.detailed_snapshot()?;
    let resources = detailed.summary;
    if resources.managed_memory_bytes > resources.managed_memory_limit_bytes
        || resources.managed_memory_peak_bytes > resources.managed_memory_limit_bytes
    {
        return Err(io::Error::other("soak exceeded the managed memory bound"));
    }
    if resources.health != CacheHealth::Running || resources.io_failures != 0 {
        return Err(io::Error::other("soak observed a cache runtime failure"));
    }
    let current_rss = current_rss_bytes()?;
    let rss_limit =
        (resources.managed_memory_limit_bytes as u64).saturating_add(rss_slack_bytes as u64);
    if current_rss != 0 && current_rss > rss_limit {
        return Err(io::Error::other("soak exceeded the process RSS bound"));
    }
    Ok(ResourceSample {
        detailed,
        logical_bytes,
        current_rss,
        rss_limit,
    })
}

fn report_sample(
    complete: bool,
    elapsed: Duration,
    counters: CounterSnapshot,
    sample: &ResourceSample,
    max_managed_memory: usize,
) {
    let prefix = if complete { "complete " } else { "" };
    let detailed = &sample.detailed;
    let resources = detailed.summary;
    println!(
        "{prefix}elapsed={:.1}s writes={} deletes={} write_rejections={} delete_rejections={} hits={} stale_hits={} misses={} errors={} cache_puts={} cache_deletes={} l1_hits={} l2_hits={} l2_misses={} l2_read_memory_misses={} l2_read_busy_misses={} promotions={} l1_evictions={} l1_bypasses={} cache_write_rejections={} rotations={} l1_entries={} l1_entry_capacity={} l1_resident={} l1_retained={} l1_metadata={} index_values={} index_deleted={} index_deleted_reuses={} index_stale_reuses={} index_live_replacements={} io_submitted={} io_completed={} io_errors={} io_in_flight_peak={} managed={} managed_peak={} managed_limit={} logical_disk={} current_rss={} rss_limit={} peak_rss={} max_put_us={} max_delete_us={} max_get_us={}",
        elapsed.as_secs_f64(),
        counters.writes,
        counters.deletes,
        counters.write_rejections,
        counters.delete_rejections,
        counters.hits,
        counters.stale_hits,
        counters.misses,
        0,
        resources.puts,
        resources.deletes,
        resources.l1_hits,
        resources.l2_hits,
        resources.l2_misses,
        resources.l2_read_memory_misses,
        resources.l2_read_busy_misses,
        resources.l1_promotions,
        resources.l1_evictions,
        resources.l1_bypasses,
        resources.write_rejections,
        resources.region_rotations,
        detailed.l1.resident_entries,
        detailed.l1.entry_capacity,
        detailed.l1.resident_bytes,
        detailed.l1.retained_bytes,
        detailed.l1.metadata_bytes,
        detailed.index.physical_value_slots,
        detailed.index.deleted_slots,
        detailed.index.deleted_slot_reuses,
        detailed.index.stale_slot_reuses,
        detailed.index.live_slot_replacements,
        detailed.io.submitted,
        detailed.io.completed,
        detailed.io.errors,
        detailed.io.requests_in_flight_peak,
        resources.managed_memory_bytes,
        max_managed_memory,
        resources.managed_memory_limit_bytes,
        sample.logical_bytes,
        sample.current_rss,
        sample.rss_limit,
        peak_rss_bytes(),
        counters.max_put_ns / 1_000,
        counters.max_delete_ns / 1_000,
        counters.max_get_ns / 1_000,
    );
}

fn record_latency(counter: &AtomicU64, elapsed: Duration) {
    let nanos = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
    counter.fetch_max(nanos, Ordering::Relaxed);
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `usage` points to writable storage for one `rusage` value.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    // SAFETY: a successful getrusage initialized the complete value.
    let usage = unsafe { usage.assume_init() };
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        u64::try_from(usage.ru_maxrss).unwrap_or(0)
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        u64::try_from(usage.ru_maxrss)
            .unwrap_or(0)
            .saturating_mul(1024)
    }
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> io::Result<u64> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let kib = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_ascii_whitespace().next())
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| io::Error::other("cannot read current RSS from /proc/self/status"))?;
    kib.checked_mul(1024)
        .ok_or_else(|| io::Error::other("current RSS byte count overflow"))
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> io::Result<u64> {
    Ok(0)
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

fn env_u64(name: &str, default: u64) -> io::Result<u64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(format!("{name} must be an unsigned integer"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    env_u64(name, default as u64).and_then(|value| {
        usize::try_from(value).map_err(|_| invalid(format!("{name} does not fit usize")))
    })
}

fn env_usize_list(name: &str, default: &[usize]) -> io::Result<Box<[usize]>> {
    match env::var(name) {
        Ok(value) => value
            .split(',')
            .map(|item| {
                item.parse::<usize>()
                    .map_err(|_| invalid(format!("{name} must be comma-separated integers")))
            })
            .collect::<io::Result<Vec<_>>>()
            .map(Vec::into_boxed_slice),
        Err(env::VarError::NotPresent) => Ok(default.to_vec().into_boxed_slice()),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn env_u32(name: &str, default: u32) -> io::Result<u32> {
    env_u64(name, u64::from(default))
        .and_then(|value| u32::try_from(value).map_err(|_| invalid(format!("{name} exceeds u32"))))
}

fn env_bool(name: &str, default: bool) -> io::Result<bool> {
    match env::var(name) {
        Ok(value) if value == "true" || value == "1" => Ok(true),
        Ok(value) if value == "false" || value == "0" => Ok(false),
        Ok(_) => Err(invalid(format!("{name} must be true, false, 1, or 0"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn parse_io_engine(name: &str) -> io::Result<IoEngine> {
    match env::var(name)
        .unwrap_or_else(|_| "posix".to_owned())
        .as_str()
    {
        "posix" => Ok(IoEngine::Posix),
        "io-uring" => Ok(IoEngine::IoUring),
        value => Err(invalid(format!("unsupported I/O engine: {value}"))),
    }
}

fn parse_io_mode(name: &str) -> io::Result<IoMode> {
    match env::var(name)
        .unwrap_or_else(|_| "auto".to_owned())
        .as_str()
    {
        "buffered" => Ok(IoMode::Buffered),
        "auto" => Ok(IoMode::Auto),
        "direct" => Ok(IoMode::Direct),
        value => Err(invalid(format!("unsupported I/O mode: {value}"))),
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
