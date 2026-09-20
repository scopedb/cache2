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
use std::fmt;
use std::io;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;

use benchmarks::config::env_u64;
use benchmarks::config::env_usize;
use benchmarks::config::invalid;
use benchmarks::config::reject_renamed_env;
use benchmarks::harness::BenchFiles;
use benchmarks::harness::current_rss_bytes;
use benchmarks::harness::peak_rss_bytes;
use benchmarks::harness::put_eventually;
use benchmarks::report::JobReport;
use benchmarks::report::RunReporter;
use cache2::Cache;
use cache2::CacheConfig;
use cache2::IoEngineOptions;
use cache2::IoMode;
use cache2::PosixIoOptions;
use cache2::RuntimeOptions;
use cache2::StartupMode;
use cache2::StorageOptions;

const MIB: usize = 1024 * 1024;
const WRITE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

struct ScaleConfig {
    expected_entries: usize,
    capacity_bytes: u64,
    l1_capacity_bytes: usize,
    managed_memory_limit_bytes: usize,
    sentinel_count: usize,
    value_bytes: usize,
    directory: PathBuf,
}

impl ScaleConfig {
    fn from_env() -> io::Result<Self> {
        reject_renamed_env("CACHE_RECOVERY")?;
        let expected_entries = env_usize("CACHE_RECOVERY_EXPECTED_ENTRIES", 1_000_000)?;
        let capacity_bytes = env_u64("CACHE_RECOVERY_CAPACITY_MIB", 256)?
            .checked_mul(MIB as u64)
            .ok_or_else(|| invalid("recovery benchmark capacity is too large"))?;
        let l1_capacity_bytes = env_usize("CACHE_RECOVERY_L1_CAPACITY_MIB", 16)?
            .checked_mul(MIB)
            .ok_or_else(|| invalid("recovery benchmark RAM tier is too large"))?;
        let managed_memory_limit_bytes =
            env_usize("CACHE_RECOVERY_MANAGED_MEMORY_LIMIT_MIB", 1_024)?
                .checked_mul(MIB)
                .ok_or_else(|| invalid("recovery benchmark managed memory limit is too large"))?;
        let sentinel_count = env_usize("CACHE_RECOVERY_SENTINELS", 1_024)?;
        let value_bytes = env_usize("CACHE_RECOVERY_VALUE_BYTES", 1_024)?;
        let directory = env::var_os("CACHE_RECOVERY_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);
        if expected_entries == 0 || sentinel_count == 0 || value_bytes < 8 || !directory.is_dir() {
            return Err(invalid(
                "expected entries and sentinels must be positive, values must be at least 8 bytes, and the benchmark directory must exist",
            ));
        }
        Ok(Self {
            expected_entries,
            capacity_bytes,
            l1_capacity_bytes,
            managed_memory_limit_bytes,
            sentinel_count,
            value_bytes,
            directory,
        })
    }

    fn storage_options(&self) -> StorageOptions {
        let mut options = StorageOptions::new(self.capacity_bytes);
        options.region_size_bytes = 32 * MIB as u64;
        options.expected_entries = Some(self.expected_entries);
        options
    }

    fn runtime_options(&self) -> RuntimeOptions {
        let mut io = PosixIoOptions::default();
        io.read_workers = 1;
        io.write_workers = 1;
        io.reclaim_workers = 1;
        let mut options = RuntimeOptions::default();
        options.io_engine = IoEngineOptions::Posix(io);
        options.io_mode = IoMode::Buffered;
        options.append_shards = 4;
        options.l1_capacity_bytes = self.l1_capacity_bytes;
        options.managed_memory_limit_bytes = self.managed_memory_limit_bytes;
        options.stats.activity_counters = false;
        options
    }
}

fn main() -> io::Result<()> {
    let reporter = RunReporter::start("recovery_scale", None);
    let result = run_benchmark();
    reporter.finish(
        result
            .as_ref()
            .err()
            .map(|error| error as &dyn fmt::Display),
    );
    result
}

fn run_benchmark() -> io::Result<()> {
    let config = ScaleConfig::from_env()?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?;
    runtime.block_on(run(config))
}

async fn run(config: ScaleConfig) -> io::Result<()> {
    let mut files = BenchFiles::preserved_on_failure(&config.directory, "recovery-scale");
    let storage = config.storage_options().build()?;
    let cache_config = CacheConfig::new(storage.clone(), config.runtime_options())?;
    let peak_disk_bytes = storage.peak_disk_bytes();
    println!(
        "config expected_entries={} index_slots={} capacity_bytes={} l1_capacity_bytes={} managed_memory_limit_bytes={} sentinels={} value_bytes={} peak_disk_bytes={} directory={}",
        config.expected_entries,
        storage.index_slots(),
        config.capacity_bytes,
        config.l1_capacity_bytes,
        config.managed_memory_limit_bytes,
        config.sentinel_count,
        config.value_bytes,
        peak_disk_bytes,
        config.directory.display(),
    );

    let opened = Instant::now();
    let cache = Cache::open(files.data(), cache_config.clone()).await?;
    emit("fresh_open", "control", opened.elapsed(), 1, 0);
    require_startup(cache.startup_mode(), StartupMode::Cold)?;
    let resources = cache.snapshot()?;
    println!(
        "resources managed_bytes={} managed_peak_bytes={} managed_limit_bytes={}",
        resources.managed_memory_bytes,
        resources.managed_memory_peak_bytes,
        resources.managed_memory_limit_bytes,
    );

    let keys: Vec<[u8; 16]> = (0..config.sentinel_count).map(sentinel_key).collect();
    let mut value = vec![0xa5; config.value_bytes];
    let populated = Instant::now();
    for (ordinal, key) in keys.iter().enumerate() {
        value[..8].copy_from_slice(&(ordinal as u64).to_le_bytes());
        put_eventually(&cache, key, &value, WRITE_RETRY_TIMEOUT)?;
    }
    cache.drain().await?;
    emit(
        "populate_and_drain",
        "write",
        populated.elapsed(),
        keys.len() as u64,
        keys.len() as u128 * config.value_bytes as u128,
    );

    let closed = Instant::now();
    cache.close_warm().await?;
    emit("initial_close_warm", "control", closed.elapsed(), 1, 0);
    emit_sizes(&files, peak_disk_bytes)?;

    let reopened = Instant::now();
    let cache = Cache::open(files.data(), cache_config.clone()).await?;
    emit("warm_open", "control", reopened.elapsed(), 1, 0);
    require_startup(cache.startup_mode(), StartupMode::Warm)?;
    verify_sentinels(&cache, &keys, config.value_bytes).await?;

    let closed = Instant::now();
    cache.close_warm().await?;
    emit("recovered_close_warm", "control", closed.elapsed(), 1, 0);
    emit_sizes(&files, peak_disk_bytes)?;

    let reopened = Instant::now();
    let cache = Cache::open(files.data(), cache_config.clone()).await?;
    emit("second_warm_open", "control", reopened.elapsed(), 1, 0);
    require_startup(cache.startup_mode(), StartupMode::Warm)?;
    verify_sentinels(&cache, &keys, config.value_bytes).await?;

    let closed = Instant::now();
    cache.close_fast().await?;
    emit("close_fast", "control", closed.elapsed(), 1, 0);
    files.mark_success();
    println!("complete status=pass");
    Ok(())
}

async fn verify_sentinels(cache: &Cache, keys: &[[u8; 16]], value_bytes: usize) -> io::Result<()> {
    let started = Instant::now();
    for (ordinal, key) in keys.iter().enumerate() {
        let observed = cache
            .get(key)
            .await?
            .ok_or_else(|| io::Error::other("recovered sentinel is missing"))?;
        if observed.len() != value_bytes
            || observed[..8] != (ordinal as u64).to_le_bytes()
            || observed[8..].iter().any(|byte| *byte != 0xa5)
        {
            return Err(io::Error::other("recovered sentinel value is incorrect"));
        }
    }
    emit(
        "verify_sentinels",
        "read",
        started.elapsed(),
        keys.len() as u64,
        keys.len() as u128 * value_bytes as u128,
    );
    Ok(())
}

fn require_startup(observed: StartupMode, expected: StartupMode) -> io::Result<()> {
    if observed != expected {
        return Err(io::Error::other(format!(
            "unexpected startup mode: expected {expected:?}, observed {observed:?}"
        )));
    }
    Ok(())
}

fn emit(phase: &str, operation: &str, elapsed: Duration, operations: u64, bytes: u128) {
    JobReport::new(
        "recovery_scale",
        None,
        phase,
        operation,
        elapsed,
        operations,
    )
    .bytes(bytes)
    .emit();
    println!(
        "result phase={phase} elapsed_ns={} elapsed_seconds={:.6} current_rss_bytes={} peak_rss_bytes={}",
        elapsed.as_nanos(),
        elapsed.as_secs_f64(),
        current_rss_bytes().unwrap_or(0),
        peak_rss_bytes(),
    );
}

fn emit_sizes(files: &BenchFiles, peak_disk_bytes: u64) -> io::Result<()> {
    let logical_bytes = files.logical_bytes()?;
    let allocated_bytes = files.allocated_bytes()?;
    if logical_bytes > peak_disk_bytes {
        return Err(io::Error::other(
            "recovery benchmark exceeded the logical disk bound",
        ));
    }
    println!(
        "files logical_bytes={logical_bytes} allocated_bytes={allocated_bytes} peak_disk_bytes={peak_disk_bytes}"
    );
    Ok(())
}

fn sentinel_key(ordinal: usize) -> [u8; 16] {
    let mut key = *b"recovery-scale!!";
    key[8..].copy_from_slice(&(ordinal as u64).to_le_bytes());
    key
}
