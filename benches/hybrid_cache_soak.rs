use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cache_rs::{EvictionPolicy, HybridCacheConfig, IoEngine, IoMode, RuntimeConfig, StaticConfig};

const MIB: usize = 1024 * 1024;

struct SoakConfig {
    duration: Duration,
    sample_period: Duration,
    capacity_bytes: u64,
    memory_bytes: usize,
    value_bytes: usize,
    key_count: usize,
    shards: u32,
    io_workers: usize,
    io_engine: IoEngine,
    io_mode: IoMode,
    eviction_policy: EvictionPolicy,
    directory: PathBuf,
}

impl SoakConfig {
    fn from_env() -> io::Result<Self> {
        let duration = Duration::from_secs(env_u64("CACHE_SOAK_SECONDS", 60)?);
        let sample_period = Duration::from_secs(env_u64("CACHE_SOAK_SAMPLE_SECONDS", 10)?);
        let capacity_bytes = env_u64("CACHE_SOAK_CAPACITY_MIB", 256)?
            .checked_mul(MIB as u64)
            .ok_or_else(|| invalid("soak capacity is too large"))?;
        let memory_bytes = env_usize("CACHE_SOAK_MEMORY_MIB", 64)?
            .checked_mul(MIB)
            .ok_or_else(|| invalid("soak memory capacity is too large"))?;
        let value_bytes = env_usize("CACHE_SOAK_VALUE_BYTES", 16 * 1024)?;
        let key_count = env_usize("CACHE_SOAK_KEYS", 32_768)?;
        let shards = env_u32("CACHE_SOAK_SHARDS", 4)?;
        let io_workers = env_usize("CACHE_SOAK_IO_WORKERS", 4)?;
        let io_engine = parse_io_engine("CACHE_SOAK_IO_ENGINE")?;
        let io_mode = parse_io_mode("CACHE_SOAK_IO_MODE")?;
        let eviction_policy = parse_eviction_policy("CACHE_SOAK_EVICTION")?;
        let directory = env::var_os("CACHE_SOAK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);
        if duration.is_zero()
            || sample_period.is_zero()
            || value_bytes < 8
            || key_count == 0
            || shards == 0
            || io_workers == 0
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
            value_bytes,
            key_count,
            shards,
            io_workers,
            io_engine,
            io_mode,
            eviction_policy,
            directory,
        })
    }

    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size(32 * MIB as u64)
            .with_expected_entries(self.key_count)
            .with_write_shards(self.shards)
    }

    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_io_workers(self.io_workers)
            .with_io_concurrency(self.io_workers.saturating_mul(64))
            .with_waiting_write_limit(256)
            .with_l1_capacity(self.memory_bytes)
            .with_memory_limit(self.memory_bytes.saturating_add(256 * MIB))
            .with_eviction_policy(self.eviction_policy)
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

fn main() -> io::Result<()> {
    let config = SoakConfig::from_env()?;
    let files = SoakFiles::new(&config.directory);
    let static_config = config.static_config();
    let peak_disk_bytes = static_config.peak_disk_bytes()?;
    let cache = HybridCacheConfig::from_static(&files.data, static_config)
        .with_runtime_config(config.runtime_config())
        .open()?;
    let keys: Vec<Box<[u8]>> = (0..config.key_count)
        .map(|ordinal| {
            format!("soak-key-{ordinal:016x}")
                .into_bytes()
                .into_boxed_slice()
        })
        .collect();
    let mut expected = vec![None; config.key_count];
    let mut value = vec![0xa5_u8; config.value_bytes];
    let started = Instant::now();
    let deadline = started + config.duration;
    let mut next_sample = started + config.sample_period;
    let mut writes = 0_u64;
    let mut write_rejections = 0_u64;
    let mut hits = 0_u64;
    let mut misses = 0_u64;
    let mut max_put = Duration::ZERO;
    let mut max_get = Duration::ZERO;
    let mut max_managed_memory = 0_usize;

    println!(
        "cache-rs soak duration={}s capacity={:.1}MiB memory={:.1}MiB value={} keys={} shards={} workers={} engine={:?} mode={:?} eviction={:?} peak_disk={}",
        config.duration.as_secs(),
        config.capacity_bytes as f64 / MIB as f64,
        config.memory_bytes as f64 / MIB as f64,
        config.value_bytes,
        config.key_count,
        config.shards,
        config.io_workers,
        config.io_engine,
        config.io_mode,
        config.eviction_policy,
        peak_disk_bytes,
    );

    while Instant::now() < deadline {
        let key_index = writes as usize % keys.len();
        value[..8].copy_from_slice(&writes.to_le_bytes());
        let put_started = Instant::now();
        match cache.put(&keys[key_index], &value) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                max_put = max_put.max(put_started.elapsed());
                write_rejections = write_rejections.saturating_add(1);
                std::thread::sleep(Duration::from_micros(50));
                continue;
            }
            Err(error) => return Err(error),
        }
        max_put = max_put.max(put_started.elapsed());
        expected[key_index] = Some(writes);
        writes = writes.saturating_add(1);

        if writes.is_multiple_of(256) {
            cache.drain()?;
        }
        if writes.is_multiple_of(4) {
            let read_ordinal = writes / 4;
            let distance = if read_ordinal.is_multiple_of(2) {
                17
            } else {
                u64::try_from(keys.len() / 4).unwrap_or(u64::MAX)
            };
            let sampled = writes.saturating_sub(distance) as usize % keys.len();
            let get_started = Instant::now();
            match cache.get(&keys[sampled])? {
                Some(observed) => {
                    max_get = max_get.max(get_started.elapsed());
                    let sequence = u64::from_le_bytes(observed[..8].try_into().unwrap());
                    if Some(sequence) != expected[sampled] {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "soak read returned a stale value",
                        ));
                    }
                    hits = hits.saturating_add(1);
                }
                None => {
                    max_get = max_get.max(get_started.elapsed());
                    misses = misses.saturating_add(1);
                }
            }
        }

        let now = Instant::now();
        if now >= next_sample {
            cache.flush()?;
            let logical_bytes = files.logical_bytes()?;
            if logical_bytes > peak_disk_bytes {
                return Err(io::Error::other("soak exceeded the logical disk bound"));
            }
            let resources = cache.snapshot()?;
            if resources.managed_memory_bytes > resources.managed_memory_limit_bytes
                || resources.managed_memory_peak_bytes > resources.managed_memory_limit_bytes
            {
                return Err(io::Error::other("soak exceeded the managed memory bound"));
            }
            if resources.io_failures != 0 {
                return Err(io::Error::other("soak observed a cache runtime failure"));
            }
            max_managed_memory = max_managed_memory.max(resources.managed_memory_peak_bytes);
            println!(
                "elapsed={:.1}s writes={} write_rejections={} hits={} misses={} errors={} l1_hits={} l2_hits={} l2_misses={} promotions={} l1_evictions={} l1_bypasses={} admission_rejections={} cache_write_rejections={} rotations={} managed={} managed_peak={} managed_limit={} logical_disk={} peak_rss={} max_put_us={} max_get_us={}",
                started.elapsed().as_secs_f64(),
                writes,
                write_rejections,
                hits,
                misses,
                0,
                resources.l1_hits,
                resources.l2_hits,
                resources.l2_misses,
                resources.l1_promotions,
                resources.l1_evictions,
                resources.l1_bypasses,
                resources.l1_admission_rejections,
                resources.write_rejections,
                resources.region_rotations,
                resources.managed_memory_bytes,
                max_managed_memory,
                resources.managed_memory_limit_bytes,
                logical_bytes,
                peak_rss_bytes(),
                max_put.as_micros(),
                max_get.as_micros(),
            );
            next_sample = now + config.sample_period;
        }
    }

    cache.drain()?;
    let logical_bytes = files.logical_bytes()?;
    if logical_bytes > peak_disk_bytes {
        return Err(io::Error::other("soak exceeded the logical disk bound"));
    }
    let resources = cache.snapshot()?;
    if resources.managed_memory_bytes > resources.managed_memory_limit_bytes
        || resources.managed_memory_peak_bytes > resources.managed_memory_limit_bytes
    {
        return Err(io::Error::other("soak exceeded the managed memory bound"));
    }
    if resources.io_failures != 0 {
        return Err(io::Error::other("soak observed a cache runtime failure"));
    }
    max_managed_memory = max_managed_memory.max(resources.managed_memory_peak_bytes);
    cache.close_fast()?;
    println!(
        "complete elapsed={:.1}s writes={} write_rejections={} hits={} misses={} errors={} l1_hits={} l2_hits={} l2_misses={} promotions={} l1_evictions={} l1_bypasses={} admission_rejections={} cache_write_rejections={} rotations={} managed_peak={} managed_limit={} logical_disk={} peak_rss={} max_put_us={} max_get_us={}",
        started.elapsed().as_secs_f64(),
        writes,
        write_rejections,
        hits,
        misses,
        0,
        resources.l1_hits,
        resources.l2_hits,
        resources.l2_misses,
        resources.l1_promotions,
        resources.l1_evictions,
        resources.l1_bypasses,
        resources.l1_admission_rejections,
        resources.write_rejections,
        resources.region_rotations,
        max_managed_memory,
        resources.managed_memory_limit_bytes,
        logical_bytes,
        peak_rss_bytes(),
        max_put.as_micros(),
        max_get.as_micros(),
    );
    Ok(())
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

fn env_u32(name: &str, default: u32) -> io::Result<u32> {
    env_u64(name, u64::from(default))
        .and_then(|value| u32::try_from(value).map_err(|_| invalid(format!("{name} exceeds u32"))))
}

fn parse_io_engine(name: &str) -> io::Result<IoEngine> {
    match env::var(name)
        .unwrap_or_else(|_| "auto".to_owned())
        .as_str()
    {
        "sync" => Ok(IoEngine::Sync),
        "auto" => Ok(IoEngine::Auto),
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

fn parse_eviction_policy(name: &str) -> io::Result<EvictionPolicy> {
    match env::var(name)
        .unwrap_or_else(|_| "clock".to_owned())
        .as_str()
    {
        "clock" => Ok(EvictionPolicy::Clock),
        "lru" => Ok(EvictionPolicy::Lru),
        "tinylfu" | "tiny-lfu" => Ok(EvictionPolicy::TinyLfu),
        "sieve" => Ok(EvictionPolicy::Sieve),
        "fifo" => Ok(EvictionPolicy::Fifo),
        "s3fifo" | "s3-fifo" => Ok(EvictionPolicy::S3Fifo),
        value => Err(invalid(format!("unsupported eviction policy: {value}"))),
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
