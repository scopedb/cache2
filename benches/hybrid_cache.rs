use std::env;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Barrier;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache_rs::{
    CacheTier, HybridCache, HybridCacheConfig, IoEngine, IoMode, RuntimeConfig, StartupMode,
    StaticConfig,
};

const MIB: usize = 1024 * 1024;
const MAX_VALUE_BYTES: usize = 256 * 1024;
const READ_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const WRITE_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_micros(50);
const WRITE_YIELD_RETRIES: usize = 8;

struct BenchConfig {
    entries: usize,
    resident_entries: usize,
    value_bytes: usize,
    read_ops: usize,
    capacity_bytes: u64,
    memory_bytes: usize,
    memory_limit_bytes: usize,
    shards: u32,
    io_workers: usize,
    clients: usize,
    io_engine: IoEngine,
    io_mode: IoMode,
    statistics_enabled: bool,
    directory: PathBuf,
}

impl BenchConfig {
    fn from_env() -> io::Result<Self> {
        let entries = env_usize("CACHE_BENCH_ENTRIES", 8_192)?;
        let value_bytes = env_usize("CACHE_BENCH_VALUE_BYTES", 16 * 1024)?;
        let read_ops = env_usize("CACHE_BENCH_READ_OPS", 1_048_576)?;
        let capacity_mib = env_usize("CACHE_BENCH_CAPACITY_MIB", 512)?;
        let memory_mib = env_usize("CACHE_BENCH_MEMORY_MIB", 256)?;
        let shards = env_u32("CACHE_BENCH_SHARDS", 4)?;
        let io_workers = env_usize("CACHE_BENCH_IO_WORKERS", 4)?;
        let clients = env_usize("CACHE_BENCH_CLIENTS", 8)?;
        let io_engine = match env::var("CACHE_BENCH_IO_ENGINE")
            .unwrap_or_else(|_| "auto".to_owned())
            .as_str()
        {
            "sync" => IoEngine::Sync,
            "auto" => IoEngine::Auto,
            "io-uring" => IoEngine::IoUring,
            value => return Err(invalid(format!("unsupported I/O engine: {value}"))),
        };
        let io_mode = match env::var("CACHE_BENCH_IO_MODE")
            .unwrap_or_else(|_| "auto".to_owned())
            .as_str()
        {
            "buffered" => IoMode::Buffered,
            "auto" => IoMode::Auto,
            "direct" => IoMode::Direct,
            value => return Err(invalid(format!("unsupported I/O mode: {value}"))),
        };
        let statistics_enabled = env_bool("CACHE_BENCH_STATS", false)?;
        let directory = env::var_os("CACHE_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);

        if entries == 0 || read_ops == 0 || io_workers == 0 || clients == 0 || shards == 0 {
            return Err(invalid(
                "entry, operation, worker, client, and shard counts must be positive",
            ));
        }
        if !(8..=MAX_VALUE_BYTES).contains(&value_bytes) {
            return Err(invalid("value size must be in 8..=262144 bytes"));
        }
        if !directory.is_dir() {
            return Err(invalid(format!(
                "benchmark directory does not exist: {}",
                directory.display()
            )));
        }

        let capacity_bytes = u64::try_from(capacity_mib)
            .ok()
            .and_then(|value| value.checked_mul(MIB as u64))
            .ok_or_else(|| invalid("benchmark capacity is too large"))?;
        let memory_bytes = memory_mib
            .checked_mul(MIB)
            .ok_or_else(|| invalid("benchmark memory capacity is too large"))?;
        let estimated_index_bytes = entries
            .checked_mul(160)
            .ok_or_else(|| invalid("benchmark index estimate is too large"))?;
        let default_memory_limit_mib = memory_bytes
            .checked_add(estimated_index_bytes)
            .and_then(|bytes| bytes.checked_add(512 * MIB))
            .ok_or_else(|| invalid("benchmark memory limit is too large"))?
            .div_ceil(MIB);
        let memory_limit_mib = env_usize("CACHE_BENCH_MEMORY_LIMIT_MIB", default_memory_limit_mib)?;
        let memory_limit_bytes = memory_limit_mib
            .checked_mul(MIB)
            .ok_or_else(|| invalid("benchmark memory limit is too large"))?;
        let maximum_resident_entries = memory_bytes
            .saturating_mul(3)
            .saturating_div(4)
            .saturating_div(value_bytes.saturating_add(256));
        let resident_entries = env_usize(
            "CACHE_BENCH_RESIDENT_ENTRIES",
            entries.min(maximum_resident_entries),
        )?;
        let resident_bytes = resident_entries
            .checked_mul(value_bytes.saturating_add(256))
            .ok_or_else(|| invalid("resident benchmark set is too large"))?;
        if resident_entries == 0
            || resident_entries > entries
            || resident_bytes > memory_bytes.saturating_mul(3).saturating_div(4)
        {
            return Err(invalid(format!(
                "resident set must fit within 75% of RAM tier (maximum {maximum_resident_entries} entries)"
            )));
        }
        let data_bytes = (entries as u128) * (value_bytes as u128);
        if data_bytes > u128::from(capacity_bytes / 2) {
            return Err(invalid(
                "benchmark data set must not exceed half of Region capacity",
            ));
        }

        Ok(Self {
            entries,
            resident_entries,
            value_bytes,
            read_ops,
            capacity_bytes,
            memory_bytes,
            memory_limit_bytes,
            shards,
            io_workers,
            clients,
            io_engine,
            io_mode,
            statistics_enabled,
            directory,
        })
    }

    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size(32 * MIB as u64)
            // Keep the benchmark's complete L2 working set comfortably below
            // every page-aligned index partition's bounded-probe capacity.
            .with_expected_entries(self.entries.saturating_mul(4))
            .with_write_shards(self.shards)
    }

    fn runtime_config(&self) -> RuntimeConfig {
        let io_concurrency = self.io_workers.saturating_mul(64).max(self.io_workers);
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_io_workers(self.io_workers)
            .with_io_concurrency(io_concurrency)
            .with_l1_capacity(self.memory_bytes)
            .with_memory_limit(self.memory_limit_bytes)
            .with_statistics(self.statistics_enabled)
    }
}

struct BenchFiles {
    data: PathBuf,
}

impl BenchFiles {
    fn new(directory: &Path) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache-rs-hybrid-bench-{}-{timestamp}.cache",
                std::process::id()
            )),
        }
    }

    fn config(&self, config: &BenchConfig) -> HybridCacheConfig {
        HybridCacheConfig::from_static(&self.data, config.static_config())
            .with_runtime_config(config.runtime_config())
    }
}

impl Drop for BenchFiles {
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

struct Measurement {
    elapsed: Duration,
    operations: usize,
    bytes: u128,
    checksum: u64,
}

fn main() -> io::Result<()> {
    let config = BenchConfig::from_env()?;
    let files = BenchFiles::new(&config.directory);
    let mut value = vec![0xa5; config.value_bytes];

    println!("cache-rs HybridCache benchmark");
    println!(
        "entries={} resident_entries={} value={} B data={:.1} MiB memory={:.1} MiB memory_limit={:.1} MiB shards={} workers={} clients={} engine={:?} mode={:?} statistics={}",
        config.entries,
        config.resident_entries,
        config.value_bytes,
        (config.entries as f64 * config.value_bytes as f64) / MIB as f64,
        config.memory_bytes as f64 / MIB as f64,
        config.memory_limit_bytes as f64 / MIB as f64,
        config.shards,
        config.io_workers,
        config.clients,
        config.io_engine,
        config.io_mode,
        config.statistics_enabled,
    );
    println!("file={}", files.data.display());

    let cache = files.config(&config).open()?;
    let started = Instant::now();
    let mut write_checksum = 0_u64;
    let mut write_attempts = 0_usize;
    let mut throttled_writes = 0_usize;
    for ordinal in 0..config.entries {
        let key = benchmark_key(ordinal);
        value[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
        let (receipt, attempts) = put_eventually(&cache, black_box(&key), black_box(&value))?;
        write_attempts = write_attempts.saturating_add(attempts);
        throttled_writes = throttled_writes.saturating_add(usize::from(attempts > 1));
        write_checksum = write_checksum.wrapping_add(receipt.rotate_left((ordinal % 64) as u32));
    }
    cache.drain()?;
    let put = Measurement {
        elapsed: started.elapsed(),
        operations: config.entries,
        bytes: (config.entries as u128) * (config.value_bytes as u128),
        checksum: write_checksum,
    };
    report("put_drain", "put + drain", &put);
    println!(
        "result phase=put_admission attempts={write_attempts} retries={} throttled={throttled_writes}",
        write_attempts.saturating_sub(config.entries),
    );

    let resident_start = config.entries - config.resident_entries;
    for ordinal in resident_start..config.entries {
        let key = benchmark_key(ordinal);
        let observed = cache
            .get(black_box(key))?
            .ok_or_else(|| io::Error::other("resident L1 preparation missed"))?;
        verify_value(ordinal, &observed)?;
    }
    let l1 = concurrent_reads(
        &cache,
        resident_start,
        config.resident_entries,
        config.read_ops,
        config.clients,
        CacheTier::L1,
    )?;
    report("resident_l1", "resident L1 get", &l1);

    let started = Instant::now();
    cache.close_warm()?;
    let warm_close = started.elapsed();
    report_latency("warm_close", "warm close", warm_close);

    let cache = files.config(&config).open()?;
    if cache.startup_mode() != StartupMode::Warm {
        return Err(io::Error::other(
            "benchmark did not reopen from a clean image",
        ));
    }
    let promotion = concurrent_reads(
        &cache,
        0,
        config.entries,
        config.entries,
        config.clients,
        CacheTier::L2,
    )?;
    report("l2_promote", "L2 get + promote", &promotion);

    for ordinal in resident_start..config.entries {
        let key = benchmark_key(ordinal);
        let observed = cache
            .get(black_box(key))?
            .ok_or_else(|| io::Error::other("promoted L1 preparation missed"))?;
        verify_value(ordinal, &observed)?;
    }

    let promoted_l1 = concurrent_reads(
        &cache,
        resident_start,
        config.resident_entries,
        config.read_ops,
        config.clients,
        CacheTier::L1,
    )?;
    report("promoted_l1", "promoted L1 get", &promoted_l1);
    if config.statistics_enabled {
        let snapshot = cache.snapshot()?;
        println!(
            "result phase=l1_clock evictions={} bypasses={} promotions={} l1_hits={} l1_misses={}",
            snapshot.l1_evictions,
            snapshot.l1_bypasses,
            snapshot.l1_promotions,
            snapshot.l1_hits,
            snapshot.l1_misses,
        );
    }
    cache.close_fast()?;

    enforce_thresholds(&put, &l1, warm_close, &promotion, &promoted_l1)?;

    Ok(())
}

fn concurrent_reads(
    cache: &HybridCache,
    first_key: usize,
    key_count: usize,
    operations: usize,
    clients: usize,
    expected_tier: CacheTier,
) -> io::Result<Measurement> {
    let barrier = Barrier::new(clients + 1);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(clients);
        for client in 0..clients {
            let barrier = &barrier;
            handles.push(scope.spawn(move || -> io::Result<(u128, u64)> {
                barrier.wait();
                let mut bytes = 0_u128;
                let mut checksum = 0_u64;
                for ordinal in (client..operations).step_by(clients) {
                    let key_ordinal = first_key + ordinal % key_count;
                    let key = benchmark_key(key_ordinal);
                    let mut attempts = 1;
                    let mut retry_deadline = None;
                    let value = loop {
                        if let Some(value) = cache.get(black_box(key))? {
                            verify_value(key_ordinal, &value)?;
                            if value.tier() == expected_tier {
                                break value;
                            }
                            if expected_tier != CacheTier::L1 {
                                return Err(io::Error::other(format!(
                                    "expected {expected_tier:?} hit, observed {:?}",
                                    value.tier()
                                )));
                            }
                            black_box(value.as_ref());
                        }
                        let deadline = retry_deadline
                            .get_or_insert_with(|| Instant::now() + READ_RETRY_TIMEOUT);
                        if Instant::now() >= *deadline {
                            return Err(io::Error::other(format!(
                                "benchmark key {key_ordinal} did not produce an {expected_tier:?} hit on client {client} after {attempts} attempts",
                            )));
                        }
                        attempts += 1;
                        thread::sleep(RETRY_DELAY);
                    };
                    bytes += value.len() as u128;
                    checksum = checksum.wrapping_add(
                        (ordinal as u64).rotate_left(17) ^ u64::from(value[ordinal % value.len()]),
                    );
                    black_box(value.as_ref());
                }
                Ok((bytes, checksum))
            }));
        }
        barrier.wait();
        let started = Instant::now();
        let mut bytes = 0_u128;
        let mut checksum = 0_u64;
        for handle in handles {
            let (thread_bytes, thread_checksum) = handle
                .join()
                .map_err(|_| io::Error::other("benchmark reader panicked"))??;
            bytes += thread_bytes;
            checksum = checksum.wrapping_add(thread_checksum);
        }
        Ok(Measurement {
            elapsed: started.elapsed(),
            operations,
            bytes,
            checksum,
        })
    })
}

fn put_eventually(cache: &HybridCache, key: &[u8], value: &[u8]) -> io::Result<(u64, usize)> {
    let deadline = Instant::now() + WRITE_RETRY_TIMEOUT;
    let mut attempts = 0_usize;
    loop {
        attempts = attempts.saturating_add(1);
        match cache.put(key, value) {
            Ok(receipt) => return Ok((receipt, attempts)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "benchmark write did not enter bounded staging",
                    ));
                }
                if attempts <= WRITE_YIELD_RETRIES {
                    thread::yield_now();
                } else {
                    thread::sleep(RETRY_DELAY);
                }
            }
            Err(error) => return Err(error),
        }
    }
}

fn benchmark_key(ordinal: usize) -> [u8; 16] {
    let mut key = [0_u8; 16];
    key[..8].copy_from_slice(b"cache-rs");
    key[8..].copy_from_slice(&(ordinal as u64).to_le_bytes());
    key
}

fn verify_value(ordinal: usize, value: &[u8]) -> io::Result<()> {
    let observed =
        u64::from_le_bytes(value[..8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "benchmark value is truncated")
        })?);
    if observed != ordinal as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "benchmark returned a value for the wrong key",
        ));
    }
    Ok(())
}

fn report(phase: &str, name: &str, measurement: &Measurement) {
    let seconds = measurement.elapsed.as_secs_f64();
    let operations_per_second = measurement.operations as f64 / seconds;
    let mib_per_second = measurement.bytes as f64 / MIB as f64 / seconds;
    println!(
        "{name:<20} {:>9.3} ms  {:>12.0} ops/s  {:>10.1} MiB/s  checksum={:016x}",
        seconds * 1_000.0,
        operations_per_second,
        mib_per_second,
        measurement.checksum,
    );
    println!(
        "result phase={phase} elapsed_ns={} operations={} bytes={} ops_per_sec={operations_per_second:.3} mib_per_sec={mib_per_second:.3} checksum={:016x}",
        measurement.elapsed.as_nanos(),
        measurement.operations,
        measurement.bytes,
        measurement.checksum,
    );
}

fn report_latency(phase: &str, name: &str, elapsed: Duration) {
    println!("{name:<20} {:>9.3} ms", elapsed.as_secs_f64() * 1_000.0);
    println!(
        "result phase={phase} elapsed_ns={} operations=0 bytes=0 ops_per_sec=0.000 mib_per_sec=0.000 checksum=0000000000000000",
        elapsed.as_nanos(),
    );
}

fn enforce_thresholds(
    put: &Measurement,
    resident_l1: &Measurement,
    warm_close: Duration,
    l2_promote: &Measurement,
    promoted_l1: &Measurement,
) -> io::Result<()> {
    require_minimum_rate("CACHE_BENCH_MIN_PUT_OPS", put)?;
    require_minimum_rate("CACHE_BENCH_MIN_RESIDENT_L1_OPS", resident_l1)?;
    require_minimum_rate("CACHE_BENCH_MIN_L2_OPS", l2_promote)?;
    require_minimum_rate("CACHE_BENCH_MIN_PROMOTED_L1_OPS", promoted_l1)?;
    if let Some(maximum_ms) = env_optional_f64("CACHE_BENCH_MAX_WARM_CLOSE_MS")?
        && warm_close.as_secs_f64() * 1_000.0 > maximum_ms
    {
        return Err(io::Error::other(format!(
            "warm close exceeded CACHE_BENCH_MAX_WARM_CLOSE_MS={maximum_ms}"
        )));
    }
    Ok(())
}

fn require_minimum_rate(name: &str, measurement: &Measurement) -> io::Result<()> {
    let Some(minimum) = env_optional_f64(name)? else {
        return Ok(());
    };
    let actual = measurement.operations as f64 / measurement.elapsed.as_secs_f64();
    if actual < minimum {
        return Err(io::Error::other(format!(
            "phase throughput {actual:.3} ops/s is below {name}={minimum}"
        )));
    }
    Ok(())
}

fn env_optional_f64(name: &str) -> io::Result<Option<f64>> {
    match env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<f64>()
                .map_err(|_| invalid(format!("{name} must be a finite non-negative number")))?;
            if !parsed.is_finite() || parsed < 0.0 {
                return Err(invalid(format!(
                    "{name} must be a finite non-negative number"
                )));
            }
            Ok(Some(parsed))
        }
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn env_usize(name: &str, default: usize) -> io::Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(format!("{name} must be an unsigned integer"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
}

fn env_u32(name: &str, default: u32) -> io::Result<u32> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| invalid(format!("{name} must be an unsigned integer"))),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
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

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
