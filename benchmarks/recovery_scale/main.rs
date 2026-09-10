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
use std::fs;
use std::io;
use std::mem::MaybeUninit;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use benchmarks::report::JobReport;
use benchmarks::report::RunReporter;
use cache2::Cache;
use cache2::CacheConfig;
use cache2::IoEngineConfig;
use cache2::IoMode;
use cache2::PosixIoConfig;
use cache2::RuntimeOptions;
use cache2::StartupMode;
use cache2::StorageOptions;

const MIB: usize = 1024 * 1024;
const WRITE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);

struct ScaleConfig {
    expected_entries: usize,
    capacity_bytes: u64,
    memory_bytes: usize,
    managed_memory_limit_bytes: usize,
    sentinel_count: usize,
    value_bytes: usize,
    directory: PathBuf,
}

impl ScaleConfig {
    fn from_env() -> io::Result<Self> {
        let expected_entries = env_usize("CACHE_RECOVERY_EXPECTED_ENTRIES", 1_000_000)?;
        let capacity_bytes = env_u64("CACHE_RECOVERY_CAPACITY_MIB", 256)?
            .checked_mul(MIB as u64)
            .ok_or_else(|| invalid("recovery benchmark capacity is too large"))?;
        let memory_bytes = env_usize("CACHE_RECOVERY_MEMORY_MIB", 16)?
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
            memory_bytes,
            managed_memory_limit_bytes,
            sentinel_count,
            value_bytes,
            directory,
        })
    }

    fn storage_options(&self) -> StorageOptions {
        StorageOptions {
            region_size_bytes: 32 * MIB as u64,
            expected_entries: Some(self.expected_entries),
            ..StorageOptions::new(self.capacity_bytes)
        }
    }

    fn runtime_options(&self) -> RuntimeOptions {
        RuntimeOptions {
            io_engine: IoEngineConfig::Posix(PosixIoConfig::new(1, 1, 1)),
            io_mode: IoMode::Buffered,
            append_shards: 4,
            l1_capacity_bytes: self.memory_bytes,
            managed_memory_limit_bytes: self.managed_memory_limit_bytes,
            statistics: false,
            ..RuntimeOptions::default()
        }
    }
}

struct ScaleFiles {
    data: PathBuf,
    cleanup_on_drop: bool,
}

impl ScaleFiles {
    fn new(directory: &Path) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache2-recovery-scale-{}-{timestamp}.cache",
                std::process::id()
            )),
            cleanup_on_drop: false,
        }
    }

    fn mark_success(&mut self) {
        self.cleanup_on_drop = true;
    }

    fn logical_bytes(&self) -> io::Result<u64> {
        self.paths()
            .into_iter()
            .try_fold(0_u64, |total, path| match fs::metadata(path) {
                Ok(metadata) => total
                    .checked_add(metadata.len())
                    .ok_or_else(|| invalid("logical file size overflow")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
                Err(error) => Err(error),
            })
    }

    #[cfg(unix)]
    fn allocated_bytes(&self) -> io::Result<u64> {
        use std::os::unix::fs::MetadataExt;

        self.paths()
            .into_iter()
            .try_fold(0_u64, |total, path| match fs::metadata(path) {
                Ok(metadata) => total
                    .checked_add(metadata.blocks().saturating_mul(512))
                    .ok_or_else(|| invalid("allocated file size overflow")),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(total),
                Err(error) => Err(error),
            })
    }

    #[cfg(not(unix))]
    fn allocated_bytes(&self) -> io::Result<u64> {
        self.logical_bytes()
    }

    fn paths(&self) -> [PathBuf; 4] {
        [
            self.data.clone(),
            sidecar(&self.data, ".state"),
            sidecar(&self.data, ".image"),
            sidecar(&self.data, ".image.next"),
        ]
    }
}

impl Drop for ScaleFiles {
    fn drop(&mut self) {
        if !self.cleanup_on_drop {
            eprintln!(
                "recovery-scale artifacts preserved after failure: data={}",
                self.data.display()
            );
            return;
        }
        for path in self.paths() {
            let _ = fs::remove_file(path);
        }
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
    let mut files = ScaleFiles::new(&config.directory);
    let storage = config.storage_options().build()?;
    let cache_config = CacheConfig::new(storage.clone(), config.runtime_options())?;
    let peak_disk_bytes = storage.peak_disk_bytes();
    println!(
        "config expected_entries={} index_slots={} capacity_bytes={} memory_bytes={} managed_memory_limit_bytes={} sentinels={} value_bytes={} peak_disk_bytes={} directory={}",
        config.expected_entries,
        storage.index_slots(),
        config.capacity_bytes,
        config.memory_bytes,
        config.managed_memory_limit_bytes,
        config.sentinel_count,
        config.value_bytes,
        peak_disk_bytes,
        config.directory.display(),
    );

    let opened = Instant::now();
    let cache = Cache::open(&files.data, cache_config.clone()).await?;
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
        put_eventually(&cache, key, &value)?;
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
    let cache = Cache::open(&files.data, cache_config.clone()).await?;
    emit("warm_open", "control", reopened.elapsed(), 1, 0);
    require_startup(cache.startup_mode(), StartupMode::Warm)?;
    verify_sentinels(&cache, &keys, config.value_bytes).await?;

    let closed = Instant::now();
    cache.close_warm().await?;
    emit("recovered_close_warm", "control", closed.elapsed(), 1, 0);
    emit_sizes(&files, peak_disk_bytes)?;

    let reopened = Instant::now();
    let cache = Cache::open(&files.data, cache_config.clone()).await?;
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

fn put_eventually(cache: &Cache, key: &[u8], value: &[u8]) -> io::Result<()> {
    let deadline = Instant::now() + WRITE_RETRY_TIMEOUT;
    loop {
        match cache.put(key, value) {
            Ok(_) => return Ok(()),
            Err(error) if error.kind() == cache2::ErrorKind::Overloaded => {
                if Instant::now() >= deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "recovery benchmark write did not enter bounded staging",
                    ));
                }
                std::thread::sleep(Duration::from_micros(50));
            }
            Err(error) => return Err(error.into()),
        }
    }
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
        current_rss_bytes(),
        peak_rss_bytes(),
    );
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
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

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

#[cfg(target_os = "linux")]
fn current_rss_bytes() -> u64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                line.strip_prefix("VmRSS:")
                    .and_then(|value| value.split_ascii_whitespace().next())
                    .and_then(|value| value.parse::<u64>().ok())
            })
        })
        .unwrap_or(0)
        .saturating_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn current_rss_bytes() -> u64 {
    0
}

fn emit_sizes(files: &ScaleFiles, peak_disk_bytes: u64) -> io::Result<()> {
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

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
