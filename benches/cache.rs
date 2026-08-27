use std::env;
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache2::{
    Cache, CacheBuilder, CacheTier, IoEngine, IoMode, RuntimeConfig, StartupMode, StaticConfig,
    Value,
};

const MIB: usize = 1024 * 1024;
const REGION_BYTES: usize = 32 * MIB;
const L1_ENTRY_BYTES: usize = 256 * 1024;
const L1_ENTRY_OVERHEAD_BYTES: usize = 64;
const BENCHMARK_KEY_BYTES: usize = 16;
// The benchmark uses a fixed 16-byte key. Leave one 64-byte format envelope so
// every accepted benchmark value fits its configured Region.
const MAX_VALUE_BYTES: usize = REGION_BYTES - 64;
const READ_RETRY_TIMEOUT: Duration = Duration::from_secs(1);
const WRITE_RETRY_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_micros(50);
const WRITE_YIELD_RETRIES: usize = 8;

struct BenchConfig {
    entries: usize,
    resident_entries: usize,
    value_bytes: usize,
    read_ops: usize,
    hot_entries: usize,
    hot_read_interval: usize,
    capacity_bytes: u64,
    memory_bytes: usize,
    managed_memory_limit_bytes: usize,
    append_shards: u32,
    read_io_workers: usize,
    write_io_workers: usize,
    write_clients: usize,
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
        let hot_entries = env_usize("CACHE_BENCH_HOT_ENTRIES", 0)?;
        let hot_read_interval = env_usize("CACHE_BENCH_HOT_READ_INTERVAL", 8)?;
        let capacity_mib = env_usize("CACHE_BENCH_CAPACITY_MIB", 512)?;
        let memory_mib = env_usize("CACHE_BENCH_MEMORY_MIB", 256)?;
        let append_shards = env_u32("CACHE_BENCH_APPEND_SHARDS", 4)?;
        let read_io_workers = env_usize("CACHE_BENCH_READ_IO_WORKERS", 4)?;
        let write_io_workers = env_usize("CACHE_BENCH_WRITE_IO_WORKERS", 4)?;
        let clients = env_usize("CACHE_BENCH_CLIENTS", 8)?;
        let write_clients = env_usize("CACHE_BENCH_WRITE_CLIENTS", 4)?;
        let io_engine = match env::var("CACHE_BENCH_IO_ENGINE")
            .unwrap_or_else(|_| "posix".to_owned())
            .as_str()
        {
            "posix" => IoEngine::Posix,
            "io-uring" => IoEngine::IoUring,
            value => return Err(invalid(format!("unsupported I/O engine: {value}"))),
        };
        let io_mode = match env::var("CACHE_BENCH_IO_MODE")
            .unwrap_or_else(|_| "buffered".to_owned())
            .as_str()
        {
            "buffered" => IoMode::Buffered,
            "direct" => IoMode::Direct,
            value => return Err(invalid(format!("unsupported I/O mode: {value}"))),
        };
        let statistics_enabled = env_bool("CACHE_BENCH_STATS", false)?;
        let directory = env::var_os("CACHE_BENCH_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);

        if entries == 0
            || read_ops == 0
            || read_io_workers == 0
            || write_io_workers == 0
            || write_clients == 0
            || clients == 0
            || append_shards == 0
        {
            return Err(invalid(
                "entry, operation, worker, client, and shard counts must be positive",
            ));
        }
        if !(8..=MAX_VALUE_BYTES).contains(&value_bytes) {
            return Err(invalid(format!(
                "value size must be in 8..={MAX_VALUE_BYTES} bytes"
            )));
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
        let default_managed_memory_limit_mib = memory_bytes
            .checked_add(estimated_index_bytes)
            .and_then(|bytes| bytes.checked_add(512 * MIB))
            .ok_or_else(|| invalid("benchmark managed memory limit is too large"))?
            .div_ceil(MIB);
        let managed_memory_limit_mib = env_usize(
            "CACHE_BENCH_MANAGED_MEMORY_LIMIT_MIB",
            default_managed_memory_limit_mib,
        )?;
        let managed_memory_limit_bytes = managed_memory_limit_mib
            .checked_mul(MIB)
            .ok_or_else(|| invalid("benchmark managed memory limit is too large"))?;
        let maximum_resident_entries = if benchmark_entry_is_l1_eligible(value_bytes) {
            memory_bytes
                .saturating_mul(3)
                .saturating_div(4)
                .saturating_div(value_bytes.saturating_add(256))
                .min(entries)
        } else {
            entries
        };
        let resident_entries = env_usize("CACHE_BENCH_RESIDENT_ENTRIES", maximum_resident_entries)?;
        if resident_entries == 0 || resident_entries > maximum_resident_entries {
            return Err(invalid(format!(
                "resident set exceeds the benchmark maximum of {maximum_resident_entries} entries"
            )));
        }
        if hot_read_interval == 0 {
            return Err(invalid("hot read interval must be non-zero"));
        }
        if hot_entries > 0
            && (!benchmark_entry_is_l1_eligible(value_bytes)
                || hot_entries > maximum_resident_entries
                || hot_entries >= entries)
        {
            return Err(invalid(format!(
                "hot set must be L1-eligible, smaller than the data set, and contain at most {maximum_resident_entries} entries"
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
            hot_entries,
            hot_read_interval,
            capacity_bytes,
            memory_bytes,
            managed_memory_limit_bytes,
            append_shards,
            read_io_workers,
            write_io_workers,
            write_clients,
            clients,
            io_engine,
            io_mode,
            statistics_enabled,
            directory,
        })
    }

    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size_bytes(REGION_BYTES as u64)
            // Keep every planned request an L2 hit so this harness measures
            // storage rather than a mixture of hits and fast index misses.
            // Production-load index behavior has its own turnover benchmark.
            .with_expected_entries(self.entries.saturating_mul(4))
    }

    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_read_io_workers(self.read_io_workers)
            .with_write_io_workers(self.write_io_workers)
            .with_append_shards(self.append_shards)
            .with_l1_capacity_bytes(self.memory_bytes)
            .with_managed_memory_limit_bytes(self.managed_memory_limit_bytes)
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
                "cache2-bench-{}-{timestamp}.cache",
                std::process::id()
            )),
        }
    }

    fn config(&self, config: &BenchConfig) -> CacheBuilder {
        CacheBuilder::from_static(&self.data, config.static_config())
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

struct WriteAdmission {
    measurement: Measurement,
    attempts: usize,
    throttled_writes: usize,
}

#[derive(Clone, Copy)]
struct HotReadPlan {
    entries: usize,
    interval: usize,
}

#[derive(Default)]
struct TierCounts {
    operations: usize,
    bytes: u128,
    checksum: u64,
    l1_hits: usize,
    l2_hits: usize,
    misses: usize,
}

struct ReadMeasurement {
    measurement: Measurement,
    sampled: TierCounts,
}

fn main() -> io::Result<()> {
    let config = BenchConfig::from_env()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.clients.max(2))
        .thread_name("cache2-benchmark")
        .enable_time()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: BenchConfig) -> io::Result<()> {
    let files = BenchFiles::new(&config.directory);
    let l1_entry_eligible = benchmark_entry_is_l1_eligible(config.value_bytes);
    let index_slots = config.static_config().index_slots();
    let index_load = config.entries as f64 * 100.0 / index_slots as f64;

    println!("C² cache benchmark");
    println!(
        "entries={} index_slots={} index_load={:.1}% resident_entries={} hot_entries={} hot_read_interval={} value={} B data={:.1} MiB memory={:.1} MiB managed_memory_limit={:.1} MiB append_shards={} read_workers={} write_workers={} write_clients={} read_clients={} l1_entry_eligible={} engine={:?} mode={:?} statistics={}",
        config.entries,
        index_slots,
        index_load,
        config.resident_entries,
        config.hot_entries,
        config.hot_read_interval,
        config.value_bytes,
        (config.entries as f64 * config.value_bytes as f64) / MIB as f64,
        config.memory_bytes as f64 / MIB as f64,
        config.managed_memory_limit_bytes as f64 / MIB as f64,
        config.append_shards,
        config.read_io_workers,
        config.write_io_workers,
        config.write_clients,
        config.clients,
        l1_entry_eligible,
        config.io_engine,
        config.io_mode,
        config.statistics_enabled,
    );
    println!("file={}", files.data.display());

    let cache = Arc::new(files.config(&config).open().await?);
    let mut write = concurrent_writes(
        Arc::clone(&cache),
        config.entries,
        config.value_bytes,
        config.write_clients,
    )?;
    let drain_started = Instant::now();
    cache.drain().await?;
    write.measurement.elapsed += drain_started.elapsed();
    report("put_drain", "put + drain", &write.measurement);
    println!(
        "result phase=put_admission clients={} attempts={} retries={} throttled={}",
        config.write_clients,
        write.attempts,
        write.attempts.saturating_sub(config.entries),
        write.throttled_writes,
    );

    let started = Instant::now();
    Arc::try_unwrap(cache)
        .map_err(|_| io::Error::other("benchmark retained a cache reader"))?
        .close_warm()
        .await?;
    let warm_close = started.elapsed();
    report_latency("warm_close", "warm close", warm_close);

    let cache = Arc::new(files.config(&config).open().await?);
    if cache.startup_mode() != StartupMode::Warm {
        return Err(io::Error::other(
            "benchmark did not reopen from a clean image",
        ));
    }
    let l2_read = if config.hot_entries == 0 {
        let l2_read = concurrent_reads(
            Arc::clone(&cache),
            0,
            config.entries,
            config.entries,
            config.clients,
            CacheTier::L2,
            None,
        )
        .await?;
        if l1_entry_eligible {
            report("l2_promote", "L2 get + promote", &l2_read.measurement);
        } else {
            report("l2_read", "L2 get", &l2_read.measurement);
        }
        l2_read.measurement
    } else {
        let _ = concurrent_writes(
            Arc::clone(&cache),
            config.hot_entries,
            config.value_bytes,
            1,
        )?;
        cache.drain().await?;
        let (before_elapsed, hot_before) = tier_reads(&cache, config.hot_entries).await?;
        report_tiers(
            "hot_before_scan",
            "hot before scan",
            before_elapsed,
            &hot_before,
        );
        let scan_start = config
            .statistics_enabled
            .then(|| cache.snapshot())
            .transpose()?;
        let cold_scan = concurrent_reads(
            Arc::clone(&cache),
            config.hot_entries,
            config.entries - config.hot_entries,
            config.entries - config.hot_entries,
            config.clients,
            CacheTier::L2,
            Some(HotReadPlan {
                entries: config.hot_entries,
                interval: config.hot_read_interval,
            }),
        )
        .await?;
        report("l2_hot_scan", "L2 cold scan", &cold_scan.measurement);
        report_tiers(
            "hot_during_scan",
            "hot during scan",
            cold_scan.measurement.elapsed,
            &cold_scan.sampled,
        );
        if let Some(before) = scan_start {
            let after = cache.snapshot()?;
            println!(
                "result phase=hot_scan_clock evictions={} bypasses={} promotions={} l1_hits={} l1_misses={} l2_hits={} l2_misses={}",
                after.l1_evictions.saturating_sub(before.l1_evictions),
                after.l1_bypasses.saturating_sub(before.l1_bypasses),
                after.l1_promotions.saturating_sub(before.l1_promotions),
                after.l1_hits.saturating_sub(before.l1_hits),
                after.l1_misses.saturating_sub(before.l1_misses),
                after.l2_hits.saturating_sub(before.l2_hits),
                after.l2_misses.saturating_sub(before.l2_misses),
            );
        }
        let (after_elapsed, hot_after) = tier_reads(&cache, config.hot_entries).await?;
        report_tiers(
            "hot_after_scan",
            "hot after scan",
            after_elapsed,
            &hot_after,
        );
        cold_scan.measurement
    };

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
        println!(
            "result phase=read_io requests={} buffered_operations={} buffered_bytes={} direct_operations={} direct_bytes={}",
            snapshot.io.read.requests_succeeded,
            snapshot.io.read.buffered.operations,
            snapshot.io.read.buffered.bytes,
            snapshot.io.read.direct.operations,
            snapshot.io.read.direct.bytes,
        );
        println!(
            "result phase=memory managed_bytes={} managed_peak_bytes={} managed_limit_bytes={} logical_disk_peak_bytes={}",
            snapshot.managed_memory_bytes,
            snapshot.managed_memory_peak_bytes,
            snapshot.managed_memory_limit_bytes,
            snapshot.logical_disk_peak_bytes,
        );
    }
    Arc::try_unwrap(cache)
        .map_err(|_| io::Error::other("benchmark retained a cache reader"))?
        .close_fast()
        .await?;

    let resident = if l1_entry_eligible {
        let cache = Arc::new(files.config(&config).open().await?);
        if cache.startup_mode() != StartupMode::Cold {
            return Err(io::Error::other(
                "fast-closed benchmark did not reopen empty",
            ));
        }
        let _ = concurrent_writes(
            Arc::clone(&cache),
            config.resident_entries,
            config.value_bytes,
            1,
        )?;
        cache.drain().await?;
        let resident = concurrent_reads(
            Arc::clone(&cache),
            0,
            config.resident_entries,
            config.read_ops,
            config.clients,
            CacheTier::L1,
            None,
        )
        .await?;
        report("resident_l1", "resident L1 get", &resident.measurement);
        Arc::try_unwrap(cache)
            .map_err(|_| io::Error::other("benchmark retained a cache reader"))?
            .close_fast()
            .await?;
        Some(resident.measurement)
    } else {
        None
    };

    enforce_thresholds(&write.measurement, warm_close, &l2_read, resident.as_ref())?;

    Ok(())
}

fn concurrent_writes(
    cache: Arc<Cache>,
    entries: usize,
    value_bytes: usize,
    clients: usize,
) -> io::Result<WriteAdmission> {
    let barrier = Arc::new(std::sync::Barrier::new(clients + 1));
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(clients);
        for client in 0..clients {
            let cache = Arc::clone(&cache);
            let barrier = Arc::clone(&barrier);
            handles.push(scope.spawn(move || {
                let mut value = vec![0xa5; value_bytes];
                barrier.wait();
                let mut checksum = 0_u64;
                let mut attempts = 0_usize;
                let mut throttled_writes = 0_usize;
                for ordinal in (client..entries).step_by(clients) {
                    let key = benchmark_key(ordinal);
                    value[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
                    let (receipt, operation_attempts) =
                        put_eventually(&cache, black_box(&key), black_box(&value))?;
                    attempts = attempts.saturating_add(operation_attempts);
                    throttled_writes =
                        throttled_writes.saturating_add(usize::from(operation_attempts > 1));
                    checksum = checksum.wrapping_add(receipt.rotate_left((ordinal % 64) as u32));
                }
                Ok::<_, io::Error>((checksum, attempts, throttled_writes))
            }));
        }
        barrier.wait();
        let started = Instant::now();
        let mut checksum = 0_u64;
        let mut attempts = 0_usize;
        let mut throttled_writes = 0_usize;
        for handle in handles {
            let (client_checksum, client_attempts, client_throttled_writes) = handle
                .join()
                .map_err(|_| io::Error::other("benchmark writer panicked"))??;
            checksum = checksum.wrapping_add(client_checksum);
            attempts = attempts.saturating_add(client_attempts);
            throttled_writes = throttled_writes.saturating_add(client_throttled_writes);
        }
        Ok(WriteAdmission {
            measurement: Measurement {
                elapsed: started.elapsed(),
                operations: entries,
                bytes: (entries as u128) * (value_bytes as u128),
                checksum,
            },
            attempts,
            throttled_writes,
        })
    })
}

async fn concurrent_reads(
    cache: Arc<Cache>,
    first_key: usize,
    key_count: usize,
    operations: usize,
    clients: usize,
    expected_tier: CacheTier,
    hot_reads: Option<HotReadPlan>,
) -> io::Result<ReadMeasurement> {
    let barrier = Arc::new(tokio::sync::Barrier::new(clients + 1));
    let mut handles = Vec::with_capacity(clients);
    for client in 0..clients {
        let cache = Arc::clone(&cache);
        let barrier = Arc::clone(&barrier);
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut bytes = 0_u128;
            let mut checksum = 0_u64;
            let mut sampled = TierCounts::default();
            for ordinal in (client..operations).step_by(clients) {
                let key_ordinal = first_key + ordinal % key_count;
                let value = read_expected(&cache, key_ordinal, expected_tier, client).await?;
                bytes += value.len() as u128;
                checksum = checksum.wrapping_add(
                    (ordinal as u64).rotate_left(17) ^ u64::from(value[ordinal % value.len()]),
                );
                black_box(value.as_ref());
                if let Some(plan) = hot_reads
                    && ordinal.is_multiple_of(plan.interval)
                {
                    let hot_key = ordinal / plan.interval % plan.entries;
                    sample_tier(&cache, hot_key, ordinal, &mut sampled).await?;
                }
            }
            Ok::<_, io::Error>((bytes, checksum, sampled))
        }));
    }
    barrier.wait().await;
    let started = Instant::now();
    let mut bytes = 0_u128;
    let mut checksum = 0_u64;
    let mut sampled = TierCounts::default();
    for handle in handles {
        let (task_bytes, task_checksum, task_sampled) = handle
            .await
            .map_err(|_| io::Error::other("benchmark reader panicked"))??;
        bytes += task_bytes;
        checksum = checksum.wrapping_add(task_checksum);
        merge_tiers(&mut sampled, task_sampled);
    }
    Ok(ReadMeasurement {
        measurement: Measurement {
            elapsed: started.elapsed(),
            operations,
            bytes,
            checksum,
        },
        sampled,
    })
}

async fn tier_reads(cache: &Cache, entries: usize) -> io::Result<(Duration, TierCounts)> {
    let started = Instant::now();
    let mut tiers = TierCounts::default();
    for ordinal in 0..entries {
        sample_tier(cache, ordinal, ordinal, &mut tiers).await?;
    }
    Ok((started.elapsed(), tiers))
}

async fn sample_tier(
    cache: &Cache,
    key_ordinal: usize,
    checksum_ordinal: usize,
    tiers: &mut TierCounts,
) -> io::Result<()> {
    tiers.operations += 1;
    match cache.get(black_box(benchmark_key(key_ordinal))).await? {
        Some(value) => {
            verify_value(key_ordinal, &value)?;
            match value.tier() {
                CacheTier::L1 => tiers.l1_hits += 1,
                CacheTier::L2 => tiers.l2_hits += 1,
            }
            tiers.bytes += value.len() as u128;
            tiers.checksum = tiers.checksum.wrapping_add(
                (checksum_ordinal as u64).rotate_left(17)
                    ^ u64::from(value[checksum_ordinal % value.len()]),
            );
            black_box(value.as_ref());
        }
        None => tiers.misses += 1,
    }
    Ok(())
}

fn merge_tiers(total: &mut TierCounts, sampled: TierCounts) {
    total.operations += sampled.operations;
    total.bytes += sampled.bytes;
    total.checksum = total.checksum.wrapping_add(sampled.checksum);
    total.l1_hits += sampled.l1_hits;
    total.l2_hits += sampled.l2_hits;
    total.misses += sampled.misses;
}

async fn read_expected(
    cache: &Cache,
    key_ordinal: usize,
    expected_tier: CacheTier,
    client: usize,
) -> io::Result<Value> {
    let key = benchmark_key(key_ordinal);
    let mut attempts = 1;
    let mut retry_deadline = None;
    loop {
        if let Some(value) = cache.get(black_box(key)).await? {
            verify_value(key_ordinal, &value)?;
            if value.tier() == expected_tier {
                return Ok(value);
            }
            if expected_tier != CacheTier::L1 {
                return Err(io::Error::other(format!(
                    "expected {expected_tier:?} hit, observed {:?}",
                    value.tier()
                )));
            }
            black_box(value.as_ref());
        }
        let deadline = retry_deadline.get_or_insert_with(|| Instant::now() + READ_RETRY_TIMEOUT);
        if Instant::now() >= *deadline {
            return Err(io::Error::other(format!(
                "benchmark key {key_ordinal} did not produce an {expected_tier:?} hit on client {client} after {attempts} attempts",
            )));
        }
        attempts += 1;
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

fn put_eventually(cache: &Cache, key: &[u8], value: &[u8]) -> io::Result<(u64, usize)> {
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
    key[..8].copy_from_slice(b"cache2::");
    key[8..].copy_from_slice(&(ordinal as u64).to_le_bytes());
    key
}

fn benchmark_entry_is_l1_eligible(value_bytes: usize) -> bool {
    L1_ENTRY_OVERHEAD_BYTES
        .checked_add(BENCHMARK_KEY_BYTES)
        .and_then(|bytes| bytes.checked_add(value_bytes))
        .is_some_and(|bytes| bytes <= L1_ENTRY_BYTES)
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

fn report_tiers(phase: &str, name: &str, elapsed: Duration, tiers: &TierCounts) {
    let seconds = elapsed.as_secs_f64();
    let operations_per_second = tiers.operations as f64 / seconds;
    let l1_hit_rate = if tiers.operations == 0 {
        0.0
    } else {
        tiers.l1_hits as f64 * 100.0 / tiers.operations as f64
    };
    println!(
        "{name:<20} {:>9.3} ms  {:>12.0} ops/s  l1={l1_hit_rate:>7.3}% l2={} miss={} checksum={:016x}",
        seconds * 1_000.0,
        operations_per_second,
        tiers.l2_hits,
        tiers.misses,
        tiers.checksum,
    );
    println!(
        "result phase={phase} elapsed_ns={} operations={} bytes={} ops_per_sec={operations_per_second:.3} l1_hits={} l2_hits={} misses={} l1_hit_rate={l1_hit_rate:.6} checksum={:016x}",
        elapsed.as_nanos(),
        tiers.operations,
        tiers.bytes,
        tiers.l1_hits,
        tiers.l2_hits,
        tiers.misses,
        tiers.checksum,
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
    warm_close: Duration,
    l2_read: &Measurement,
    resident: Option<&Measurement>,
) -> io::Result<()> {
    require_minimum_rate("CACHE_BENCH_MIN_PUT_OPS", put)?;
    require_minimum_rate("CACHE_BENCH_MIN_L2_OPS", l2_read)?;
    if let Some(resident) = resident {
        require_minimum_rate("CACHE_BENCH_MIN_RESIDENT_L1_OPS", resident)?;
    }
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
