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

use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache2::{
    Cache, CacheBuilder, CacheHealth, DetailedCacheSnapshot, ErrorKind as CacheErrorKind, IoEngine,
    IoMode, IoUringConfig, IoUringPoolConfig, L1EvictionPolicy, PosixIoConfig, RuntimeConfig,
    StartupMode, StaticConfig,
};
use logforth::append::Stderr;
use logforth::bridge::log::LogBridge;
use logforth::filter::rustlog::RustLogFilterBuilder;
use logforth::layout::JsonLayout;

const MIB: usize = 1024 * 1024;
const REGION_BYTES: usize = 32 * MIB;
const KEY_BYTES: usize = 25;
const KEY_PREFIX: &[u8; 17] = b"cache2-soak-key-v";
const VALUE_HEADER_BYTES: usize = 16;
// The generated key is 25 bytes; reserve it and the 48-byte v1 record header.
const MAX_VALUE_BYTES: usize = REGION_BYTES - KEY_BYTES - 48;
const DELETE_INTERVAL: u64 = 64;
const OVERLOAD_DELAY: Duration = Duration::from_micros(50);
const DEFAULT_VALUE_BYTES: [usize; 4] = [256, 4 * 1024, 16 * 1024, 256 * 1024];

struct SoakConfig {
    duration: Duration,
    sample_period: Duration,
    capacity_bytes: u64,
    memory_bytes: usize,
    managed_memory_limit_bytes: usize,
    value_bytes: Box<[usize]>,
    rss_slack_bytes: usize,
    rss_reopen_allowance_bytes: usize,
    key_count: usize,
    append_shards: u32,
    read_io_workers: usize,
    write_io_workers: usize,
    reclaim_workers: usize,
    writers: usize,
    readers: usize,
    operation_interval: Duration,
    warm_reopen: bool,
    final_warm_verify: bool,
    require_path_coverage: bool,
    require_reinsert_coverage: bool,
    io_engine: IoEngine,
    io_mode: IoMode,
    l1_eviction_policy: L1EvictionPolicy,
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
        let append_shards = env_u32("CACHE_SOAK_APPEND_SHARDS", 4)?;
        let reclaim_workers = env_usize("CACHE_SOAK_RECLAIM_WORKERS", 1)?;
        let shard_count = usize::try_from(append_shards)
            .map_err(|_| invalid("soak shard count does not fit usize"))?;
        let read_reservation_bytes = reclaim_workers
            .checked_add(1)
            .and_then(|buffers| buffers.checked_mul(REGION_BYTES))
            .ok_or_else(|| invalid("soak read reservation is too large"))?;
        let default_managed_memory_limit_mib = shard_count
            .checked_mul(2 * REGION_BYTES)
            .and_then(|bytes| bytes.checked_add(read_reservation_bytes))
            .and_then(|bytes| bytes.checked_add(memory_bytes))
            // Cover the fixed index/L1 metadata and recovery scratch for the
            // default harness. Large custom plans should set an explicit limit.
            .and_then(|bytes| bytes.checked_add(REGION_BYTES))
            .ok_or_else(|| invalid("soak default managed memory limit is too large"))?
            .div_ceil(MIB);
        let managed_memory_limit_bytes = env_usize(
            "CACHE_SOAK_MANAGED_MEMORY_LIMIT_MIB",
            default_managed_memory_limit_mib,
        )?
        .checked_mul(MIB)
        .ok_or_else(|| invalid("soak managed memory limit is too large"))?;
        let value_bytes = env_usize_list("CACHE_SOAK_VALUE_BYTES", &DEFAULT_VALUE_BYTES)?;
        let rss_slack_bytes = env_usize("CACHE_SOAK_RSS_SLACK_MIB", 128)?
            .checked_mul(MIB)
            .ok_or_else(|| invalid("soak RSS slack is too large"))?;
        let key_count = env_usize("CACHE_SOAK_KEYS", 32_768)?;
        let read_io_workers = env_usize("CACHE_SOAK_READ_IO_WORKERS", 4)?;
        let write_io_workers = env_usize("CACHE_SOAK_WRITE_IO_WORKERS", 4)?;
        let writers = env_usize("CACHE_SOAK_WRITERS", 4)?;
        let readers = env_usize("CACHE_SOAK_READERS", 4)?;
        let operation_interval =
            Duration::from_micros(env_u64("CACHE_SOAK_OPERATION_INTERVAL_US", 0)?);
        let warm_reopen = env_bool("CACHE_SOAK_WARM_REOPEN", false)?;
        let final_warm_verify = env_bool("CACHE_SOAK_FINAL_WARM_VERIFY", false)?;
        // A same-process reopen can leave the retired L1's freed allocations in
        // libc arenas while the replacement L1 fills. This is allocator RSS,
        // not cache-owned managed memory, and is bounded by one L1 capacity for
        // this harness's fixed reopen topology.
        let rss_reopen_allowance_bytes = if warm_reopen || final_warm_verify {
            memory_bytes
        } else {
            0
        };
        let require_path_coverage = env_bool("CACHE_SOAK_REQUIRE_PATH_COVERAGE", false)?;
        let require_reinsert_coverage = env_bool("CACHE_SOAK_REQUIRE_REINSERT_COVERAGE", false)?;
        let io_engine = parse_io_engine(
            "CACHE_SOAK_IO_ENGINE",
            read_io_workers,
            write_io_workers,
            reclaim_workers,
        )?;
        let io_mode = parse_io_mode("CACHE_SOAK_IO_MODE")?;
        let l1_eviction_policy = parse_l1_eviction_policy("CACHE_SOAK_L1_EVICTION")?;
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
            || append_shards == 0
            || reclaim_workers == 0
            || reclaim_workers > shard_count
            || read_io_workers == 0
            || write_io_workers == 0
            || writers == 0
            || readers == 0
            || operation_interval > Duration::from_secs(1)
            || !directory.is_dir()
        {
            return Err(invalid(
                "invalid soak duration, pacing, topology, value, or directory",
            ));
        }
        Ok(Self {
            duration,
            sample_period,
            capacity_bytes,
            memory_bytes,
            managed_memory_limit_bytes,
            value_bytes,
            rss_slack_bytes,
            rss_reopen_allowance_bytes,
            key_count,
            append_shards,
            read_io_workers,
            write_io_workers,
            reclaim_workers,
            writers,
            readers,
            operation_interval,
            warm_reopen,
            final_warm_verify,
            require_path_coverage,
            require_reinsert_coverage,
            io_engine,
            io_mode,
            l1_eviction_policy,
            directory,
        })
    }

    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size_bytes(REGION_BYTES as u64)
            .with_expected_entries(self.key_count)
    }

    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_append_shards(self.append_shards)
            .with_l1_capacity_bytes(self.memory_bytes)
            .with_l1_eviction_policy(self.l1_eviction_policy)
            .with_managed_memory_limit_bytes(self.managed_memory_limit_bytes)
            .with_statistics(true)
    }
}

struct SoakFiles {
    data: PathBuf,
    cleanup_on_drop: AtomicBool,
}

impl SoakFiles {
    fn new(directory: &Path) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache2-soak-{}-{timestamp}.cache",
                std::process::id()
            )),
            cleanup_on_drop: AtomicBool::new(false),
        }
    }

    fn mark_success(&self) {
        self.cleanup_on_drop.store(true, Ordering::Release);
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
        if !self.cleanup_on_drop.load(Ordering::Acquire) {
            eprintln!(
                "soak artifacts preserved after failure: data={}",
                self.data.display()
            );
            return;
        }
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

#[derive(Default)]
struct WarmVerification {
    hits: u64,
    stale_hits: u64,
    misses: u64,
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
    init_logforth()?;
    let config = SoakConfig::from_env()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.readers.max(2))
        .thread_name("cache2-soak")
        .enable_time()
        .build()?;
    let files = SoakFiles::new(&config.directory);
    let static_config = config.static_config();
    let peak_disk_bytes = static_config.peak_disk_bytes()?;
    let mut cache = open_cache(&runtime, &files, &config)?;
    let key_count =
        u64::try_from(config.key_count).map_err(|_| invalid("soak key count exceeds u64"))?;
    let value_size_count = u64::try_from(config.value_bytes.len())
        .map_err(|_| invalid("soak value-size count exceeds u64"))?;
    let expected: Vec<_> = (0..config.key_count).map(|_| AtomicU64::new(0)).collect();
    let first_write = if config.warm_reopen {
        let next =
            populate_for_warm_reopen(&cache, &config, &expected, key_count, value_size_count)?;
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
        "C² soak duration={}s capacity={:.1}MiB memory={:.1}MiB managed_memory_limit={:.1}MiB values={} keys={} append_shards={} read_io_workers={} write_io_workers={} reclaim_workers={} writers={} readers={} operation_interval_us={} warm_reopen={} final_warm_verify={} require_path_coverage={} require_reinsert_coverage={} delete_interval={} l1_eviction={:?} engine={:?} mode={:?} peak_disk={} rss_slack={} rss_reopen_allowance={} data={}",
        config.duration.as_secs(),
        config.capacity_bytes as f64 / MIB as f64,
        config.memory_bytes as f64 / MIB as f64,
        config.managed_memory_limit_bytes as f64 / MIB as f64,
        config
            .value_bytes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(","),
        config.key_count,
        config.append_shards,
        config.read_io_workers,
        config.write_io_workers,
        config.reclaim_workers,
        config.writers,
        config.readers,
        config.operation_interval.as_micros(),
        config.warm_reopen,
        config.final_warm_verify,
        config.require_path_coverage,
        config.require_reinsert_coverage,
        DELETE_INTERVAL,
        config.l1_eviction_policy,
        config.io_engine,
        config.io_mode,
        peak_disk_bytes,
        config.rss_slack_bytes,
        config.rss_reopen_allowance_bytes,
        files.data.display(),
    );

    thread::scope(|scope| -> io::Result<()> {
        let mut workers = Vec::with_capacity(client_count);
        for _ in 0..config.writers {
            workers.push(scope.spawn(|| {
                let result = run_writer(
                    &cache,
                    &config,
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
            let expected = &expected;
            let next_read = &next_read;
            let stop = &stop;
            let counters = &counters;
            let runtime = runtime.handle().clone();
            workers.push(scope.spawn(move || {
                let result = run_reader(
                    cache,
                    config,
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
                    config.rss_reopen_allowance_bytes,
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
        config.rss_reopen_allowance_bytes,
    )?;
    max_managed_memory = max_managed_memory.max(sample.detailed.summary.managed_memory_peak_bytes);
    let final_counters = counters.snapshot();
    if config.require_path_coverage {
        validate_measured_path_coverage(final_counters, &sample)?;
    }
    if config.require_reinsert_coverage {
        validate_reinsert_coverage(&sample)?;
    }
    if config.final_warm_verify {
        runtime.block_on(cache.close_warm())?;
        cache = open_cache(&runtime, &files, &config)?;
        if cache.startup_mode() != StartupMode::Warm {
            return Err(io::Error::other(
                "soak final verification did not recover a clean image",
            ));
        }
        let verification = verify_warm_reopen(
            &runtime,
            &cache,
            &expected,
            value_size_count,
            &config.value_bytes,
        )?;
        let verification_sample = resource_sample(
            &cache,
            &files,
            peak_disk_bytes,
            config.rss_slack_bytes,
            config.rss_reopen_allowance_bytes,
        )?;
        if config.require_path_coverage {
            validate_warm_path_coverage(&verification, &verification_sample)?;
        }
        let verification_resources = verification_sample.detailed.summary;
        max_managed_memory =
            max_managed_memory.max(verification_resources.managed_memory_peak_bytes);
        runtime.block_on(cache.close_fast())?;
        println!(
            "warm_verification keys={} hits={} stale_hits={} misses={} l2_hits={} l2_misses={} managed={} managed_peak={} errors=0",
            config.key_count,
            verification.hits,
            verification.stale_hits,
            verification.misses,
            verification_resources.l2_hits,
            verification_resources.l2_misses,
            verification_resources.managed_memory_bytes,
            verification_resources.managed_memory_peak_bytes,
        );
    } else {
        runtime.block_on(cache.close_fast())?;
    }
    report_sample(
        true,
        started.elapsed(),
        final_counters,
        &sample,
        max_managed_memory,
    );
    files.mark_success();
    Ok(())
}

fn init_logforth() -> io::Result<()> {
    let logger = logforth::core::builder()
        .dispatch(|dispatch| {
            dispatch
                .filter(RustLogFilterBuilder::from_default_env().build())
                .append(Stderr::default().with_layout(JsonLayout::default()))
        })
        .build();
    log::set_boxed_logger(Box::new(LogBridge::new(logger)))
        .map_err(|error| io::Error::other(error.to_string()))?;
    log::set_max_level(log::LevelFilter::Trace);
    Ok(())
}

fn open_cache(
    runtime: &tokio::runtime::Runtime,
    files: &SoakFiles,
    config: &SoakConfig,
) -> io::Result<Cache> {
    Ok(runtime.block_on(async {
        CacheBuilder::from_static(&files.data, config.static_config())
            .with_runtime_config(config.runtime_config())
            .open()
            .await
    })?)
}

fn populate_for_warm_reopen(
    cache: &Cache,
    config: &SoakConfig,
    expected: &[AtomicU64],
    key_count: u64,
    value_size_count: u64,
) -> io::Result<u64> {
    let total = key_count;
    let maximum_value_bytes = config.value_bytes.iter().copied().max().unwrap_or(0);
    let mut value = vec![0_u8; maximum_value_bytes];
    for ordinal in 0..total {
        let announced = ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("warm-reopen write ordinal exhausted"))?;
        let key_index = usize::try_from(ordinal % key_count)
            .map_err(|_| invalid("warm-reopen key index exceeds usize"))?;
        let value_index = mixed_value_index(ordinal, value_size_count)?;
        let value_bytes = config.value_bytes[value_index];
        let pattern = (ordinal ^ key_index as u64) as u8;
        value[..value_bytes].fill(pattern);
        value[..8].copy_from_slice(&ordinal.to_le_bytes());
        value[8..VALUE_HEADER_BYTES].copy_from_slice(&(key_index as u64).to_le_bytes());
        expected[key_index].fetch_max(announced, Ordering::SeqCst);
        let key = soak_key(key_index as u64);
        loop {
            match cache.put(key, &value[..value_bytes]) {
                Ok(_) => break,
                Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                    thread::sleep(OVERLOAD_DELAY);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(total)
}

#[allow(clippy::too_many_arguments)]
fn run_writer(
    cache: &Cache,
    config: &SoakConfig,
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
        pace(config.operation_interval);
        if stop.load(Ordering::Acquire) {
            break;
        }
        let ordinal = next_write.fetch_add(1, Ordering::Relaxed);
        let announced = ordinal
            .checked_add(1)
            .ok_or_else(|| invalid("soak write ordinal exhausted"))?;
        let key_index = usize::try_from(ordinal % key_count)
            .map_err(|_| invalid("soak key index exceeds usize"))?;
        let value_index = mixed_value_index(ordinal, value_size_count)?;
        let value_bytes = config.value_bytes[value_index];
        let pattern = (ordinal ^ key_index as u64) as u8;
        value[..value_bytes].fill(pattern);
        value[..8].copy_from_slice(&ordinal.to_le_bytes());
        value[8..VALUE_HEADER_BYTES].copy_from_slice(&(key_index as u64).to_le_bytes());

        // Announce before publication so a reader can never classify a newly
        // visible value as future. Rejected attempts only make validation more
        // conservative; stale and missing values are permitted.
        expected[key_index].fetch_max(announced, Ordering::SeqCst);
        let key = soak_key(key_index as u64);
        let put_started = Instant::now();
        match cache.put(key, &value[..value_bytes]) {
            Ok(_) => {
                record_latency(&counters.max_put_ns, put_started.elapsed());
                counters.writes.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                record_latency(&counters.max_put_ns, put_started.elapsed());
                counters.write_rejections.fetch_add(1, Ordering::Relaxed);
                thread::sleep(OVERLOAD_DELAY);
                continue;
            }
            Err(error) => return Err(error.into()),
        }

        if announced.is_multiple_of(DELETE_INTERVAL) {
            let delete_started = Instant::now();
            match cache.delete(key) {
                Ok(_) => {
                    record_latency(&counters.max_delete_ns, delete_started.elapsed());
                    counters.deletes.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                    record_latency(&counters.max_delete_ns, delete_started.elapsed());
                    counters.delete_rejections.fetch_add(1, Ordering::Relaxed);
                    thread::sleep(OVERLOAD_DELAY);
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_reader(
    cache: &Cache,
    config: &SoakConfig,
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
        pace(config.operation_interval);
        if stop.load(Ordering::Acquire) {
            break;
        }
        let ordinal = next_read.fetch_add(1, Ordering::Relaxed);
        let sampled = usize::try_from(ordinal.wrapping_mul(17).wrapping_add(reader_id) % key_count)
            .map_err(|_| invalid("soak sampled key exceeds usize"))?;
        let key = soak_key(sampled as u64);
        let get_started = Instant::now();
        match runtime.block_on(cache.get(&key))? {
            Some(observed) => {
                record_latency(&counters.max_get_ns, get_started.elapsed());
                let latest = expected[sampled].load(Ordering::SeqCst);
                let stale = validate_observed(
                    sampled,
                    &observed,
                    latest,
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

fn verify_warm_reopen(
    runtime: &tokio::runtime::Runtime,
    cache: &Cache,
    expected: &[AtomicU64],
    value_size_count: u64,
    value_bytes: &[usize],
) -> io::Result<WarmVerification> {
    let mut verification = WarmVerification::default();
    for (sampled, expected) in expected.iter().enumerate() {
        let key = soak_key(sampled as u64);
        match runtime.block_on(cache.get(&key))? {
            Some(observed) => {
                let latest = expected.load(Ordering::SeqCst);
                if validate_observed(sampled, &observed, latest, value_size_count, value_bytes)? {
                    verification.stale_hits = verification.stale_hits.saturating_add(1);
                }
                verification.hits = verification.hits.saturating_add(1);
            }
            None => verification.misses = verification.misses.saturating_add(1),
        }
    }
    Ok(verification)
}

fn soak_key(ordinal: u64) -> [u8; KEY_BYTES] {
    let mut key = [0_u8; KEY_BYTES];
    key[..KEY_PREFIX.len()].copy_from_slice(KEY_PREFIX);
    key[KEY_PREFIX.len()..].copy_from_slice(&ordinal.to_le_bytes());
    key
}

fn validate_observed(
    sampled: usize,
    observed: &[u8],
    latest: u64,
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
    let value_index = mixed_value_index(sequence, value_size_count)?;
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

fn mixed_value_index(sequence: u64, value_size_count: u64) -> io::Result<usize> {
    let mut mixed = sequence.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    mixed ^= mixed >> 31;
    usize::try_from(mixed % value_size_count).map_err(|_| invalid("value-size index exceeds usize"))
}

fn resource_sample(
    cache: &Cache,
    files: &SoakFiles,
    peak_disk_bytes: u64,
    rss_slack_bytes: usize,
    rss_reopen_allowance_bytes: usize,
) -> io::Result<ResourceSample> {
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
    let rss_limit = (resources.managed_memory_limit_bytes as u64)
        .saturating_add(rss_slack_bytes as u64)
        .saturating_add(rss_reopen_allowance_bytes as u64);
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

fn validate_measured_path_coverage(
    counters: CounterSnapshot,
    sample: &ResourceSample,
) -> io::Result<()> {
    let resources = sample.detailed.summary;
    let checks = [
        (counters.writes != 0, "no write completed"),
        (counters.deletes != 0, "no delete completed"),
        (
            counters.hits.saturating_add(counters.misses) != 0,
            "no read completed",
        ),
        (resources.l2_hits != 0, "no L2 hit completed"),
        (resources.region_rotations != 0, "no Region rotated"),
        (resources.reclaim.regions != 0, "no Region was reclaimed"),
    ];
    match checks.into_iter().find(|(passed, _)| !passed) {
        Some((_, missing)) => Err(io::Error::other(format!(
            "soak path coverage failed: {missing}"
        ))),
        None => Ok(()),
    }
}

fn validate_warm_path_coverage(
    verification: &WarmVerification,
    sample: &ResourceSample,
) -> io::Result<()> {
    if verification.hits == 0 {
        return Err(io::Error::other(
            "soak path coverage failed: warm verification recovered no value",
        ));
    }
    if sample.detailed.summary.l2_hits == 0 {
        return Err(io::Error::other(
            "soak path coverage failed: warm verification completed no L2 hit",
        ));
    }
    Ok(())
}

fn validate_reinsert_coverage(sample: &ResourceSample) -> io::Result<()> {
    let reclaim = sample.detailed.summary.reclaim;
    if reclaim.reinsert_records == 0 {
        return Err(io::Error::other(
            "soak reinsert coverage failed: no hot record was reinserted",
        ));
    }
    if reclaim.reinsert_budget_skipped == 0 {
        return Err(io::Error::other(
            "soak reinsert coverage failed: no hot record exhausted the byte budget",
        ));
    }
    Ok(())
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
    let io = resources.io;
    let io_submitted = io
        .read
        .requests_submitted
        .saturating_add(io.write.requests_submitted);
    let io_completed = io
        .read
        .requests_succeeded
        .saturating_add(io.read.requests_cancelled)
        .saturating_add(io.read.requests_failed)
        .saturating_add(io.write.requests_succeeded)
        .saturating_add(io.write.requests_cancelled)
        .saturating_add(io.write.requests_failed);
    println!(
        "{prefix}elapsed={:.1}s writes={} deletes={} write_rejections={} delete_rejections={} hits={} stale_hits={} misses={} errors={} cache_puts={} cache_deletes={} l1_hits={} l2_hits={} l2_misses={} l2_read_memory_misses={} l2_read_busy_misses={} promotions={} l1_evictions={} l1_bypasses={} cache_write_rejections={} rotations={} reclaimed_regions={} reclaim_reinsert_records={} reclaim_reinsert_bytes={} reclaim_reinsert_skipped={} reclaim_reinsert_budget_skipped={} reclaim_bytes={} reclaim_records={} reclaim_index_removed={} l1_entries={} l1_entry_capacity={} l1_resident={} l1_retained={} l1_metadata={} index_values={} index_relocations={} index_overflow_evictions={} index_conditional_remove_misses={} index_conditional_replace_misses={} io_submitted={} io_completed={} io_errors={} io_in_flight_peak={} managed={} managed_peak={} managed_limit={} logical_disk={} current_rss={} rss_limit={} peak_rss={} max_put_us={} max_delete_us={} max_get_us={}",
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
        resources.reclaim.regions,
        resources.reclaim.reinsert_records,
        resources.reclaim.reinsert_bytes,
        resources.reclaim.reinsert_skipped,
        resources.reclaim.reinsert_budget_skipped,
        resources.reclaim.bytes_read,
        resources.reclaim.records_scanned,
        resources.reclaim.index_entries_removed,
        detailed.l1.resident_entries,
        detailed.l1.entry_capacity,
        detailed.l1.resident_bytes,
        detailed.l1.retained_bytes,
        detailed.l1.metadata_bytes,
        detailed.index.physical_value_slots,
        detailed.index.relocations,
        detailed.index.overflow_evictions,
        detailed.index.conditional_remove_misses,
        detailed.index.conditional_replace_misses,
        io_submitted,
        io_completed,
        io.read
            .requests_failed
            .saturating_add(io.write.requests_failed),
        io.read
            .requests_in_flight_peak
            .saturating_add(io.write.requests_in_flight_peak),
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

fn pace(interval: Duration) {
    if !interval.is_zero() {
        thread::sleep(interval);
    }
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

fn parse_io_engine(
    name: &str,
    read_workers: usize,
    write_workers: usize,
    reclaim_workers: usize,
) -> io::Result<IoEngine> {
    match env::var(name)
        .unwrap_or_else(|_| "posix".to_owned())
        .as_str()
    {
        "posix" => Ok(IoEngine::Posix(PosixIoConfig::new(
            read_workers,
            write_workers,
            reclaim_workers,
        ))),
        "io-uring" => Ok(IoEngine::IoUring(IoUringConfig::new(
            IoUringPoolConfig::new(
                read_workers,
                read_workers
                    .checked_mul(64)
                    .ok_or_else(|| invalid("read io_uring depth is too large"))?,
            ),
            IoUringPoolConfig::new(
                write_workers,
                write_workers
                    .checked_mul(64)
                    .ok_or_else(|| invalid("write io_uring depth is too large"))?,
            ),
            IoUringPoolConfig::new(reclaim_workers, reclaim_workers),
        ))),
        value => Err(invalid(format!("unsupported I/O engine: {value}"))),
    }
}

fn parse_io_mode(name: &str) -> io::Result<IoMode> {
    match env::var(name)
        .unwrap_or_else(|_| "buffered".to_owned())
        .as_str()
    {
        "buffered" => Ok(IoMode::Buffered),
        "direct" => Ok(IoMode::Direct),
        value => Err(invalid(format!("unsupported I/O mode: {value}"))),
    }
}

fn parse_l1_eviction_policy(name: &str) -> io::Result<L1EvictionPolicy> {
    match env::var(name)
        .unwrap_or_else(|_| "clock".to_owned())
        .as_str()
    {
        "clock" => Ok(L1EvictionPolicy::Clock),
        "s3-fifo" => Ok(L1EvictionPolicy::S3Fifo),
        value => Err(invalid(format!("unsupported L1 eviction policy: {value}"))),
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
