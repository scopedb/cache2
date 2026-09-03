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
use std::hint::black_box;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use benchmarks::report::{JobReport, LatencyHistogram, RunReporter, emit_cache_report};
use cache2::{
    Cache, CacheBuilder, CacheHealth, ErrorKind as CacheErrorKind, IoEngine, IoMode, IoUringConfig,
    IoUringPoolConfig, L1EvictionPolicy, PosixIoConfig, RuntimeConfig, StaticConfig,
};
use tokio::sync::Barrier;

const MIB: usize = 1024 * 1024;
const MAX_KEY_BYTES: usize = 64;
const NEGATIVE_LOOKUP_KEY_BYTES: usize = 24;
const VALUE_HEADER_BYTES: usize = 24;
const VALUE_MAGIC: u32 = u32::from_le_bytes(*b"C2WL");
const RECORD_OVERHEAD_BYTES: usize = 48;
const MANAGED_MEMORY_SLACK_BYTES: usize = 64 * MIB;
const NORMAL_SAMPLE_ATTEMPTS: usize = 32;
const DEFAULT_SEED: u64 = 0x243f_6a88_85a3_08d3;
const RNG_GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;
const MIXED_KEY_BOUNDS: [usize; 3] = [1, 8, 64];
const MIXED_KEY_WEIGHTS: [u64; 2] = [3, 7];
const MIXED_VALUE_BOUNDS: [usize; 4] = [1, 32, 10_240, 409_200];
const MIXED_VALUE_WEIGHTS: [u64; 3] = [1, 2, 7];
const REINSERTION_KEY_BOUNDS: [usize; 2] = [1, 8];
const REINSERTION_KEY_WEIGHTS: [u64; 1] = [1];
const REINSERTION_VALUE_BOUNDS: [usize; 2] = [1_024, 10_240];
const REINSERTION_VALUE_WEIGHTS: [u64; 1] = [1];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    Mixed,
    Reinsertion,
    NegativeLookup,
}

impl Scenario {
    const ALL: [Self; 3] = [Self::Mixed, Self::Reinsertion, Self::NegativeLookup];

    const fn slug(self) -> &'static str {
        match self {
            Self::Mixed => "mixed",
            Self::Reinsertion => "reinsertion",
            Self::NegativeLookup => "negative-lookup",
        }
    }

    const fn default_threads(self) -> usize {
        match self {
            Self::Mixed => 2,
            Self::Reinsertion | Self::NegativeLookup => 8,
        }
    }

    const fn default_operations_per_thread(self) -> usize {
        match self {
            Self::Mixed => 1_000,
            Self::Reinsertion => 5_000,
            Self::NegativeLookup => 25_000,
        }
    }

    const fn default_keys(self) -> usize {
        match self {
            Self::Mixed => 625,
            Self::Reinsertion | Self::NegativeLookup => 1_000,
        }
    }

    const fn default_l1_mib(self) -> usize {
        match self {
            Self::Mixed => 32,
            Self::Reinsertion | Self::NegativeLookup => 1,
        }
    }

    const fn default_l2_mib(self) -> usize {
        match self {
            Self::Mixed => 64,
            Self::Reinsertion => 8,
            Self::NegativeLookup => 5,
        }
    }

    const fn default_region_mib(self) -> usize {
        match self {
            Self::Mixed => 4,
            Self::Reinsertion | Self::NegativeLookup => 1,
        }
    }

    const fn seed_salt(self) -> u64 {
        match self {
            Self::Mixed => 0xa409_3822_299f_31d0,
            Self::Reinsertion => 0x082e_fa98_ec4e_6c89,
            Self::NegativeLookup => 0x4528_21e6_38d0_1377,
        }
    }

    const fn maximum_value_bytes(self) -> usize {
        match self {
            Self::Mixed => MIXED_VALUE_BOUNDS[3] - 1,
            Self::Reinsertion => REINSERTION_VALUE_BOUNDS[1] - 1,
            Self::NegativeLookup => VALUE_HEADER_BYTES,
        }
    }

    fn operation(self, random: u64) -> Operation {
        let sample = random % 100;
        match self {
            Self::Mixed if sample < 15 => Operation::Get,
            Self::Mixed if sample < 95 => Operation::Set,
            Self::Mixed => Operation::Delete,
            Self::Reinsertion if sample < 50 => Operation::Get,
            Self::Reinsertion => Operation::Set,
            Self::NegativeLookup => Operation::Get,
        }
    }

    fn key_size(self, seed: u64, key_index: usize) -> usize {
        let first = mixed(seed ^ key_index as u64 ^ 0x1319_8a2e_0370_7344);
        let second = mixed(first);
        let sampled = match self {
            Self::Mixed | Self::NegativeLookup => {
                sample_piecewise(&MIXED_KEY_BOUNDS, &MIXED_KEY_WEIGHTS, first, second)
            }
            Self::Reinsertion => sample_piecewise(
                &REINSERTION_KEY_BOUNDS,
                &REINSERTION_KEY_WEIGHTS,
                first,
                second,
            ),
        };
        sampled.max(size_of::<usize>())
    }

    fn value_size(self, seed: u64, key_index: usize) -> usize {
        let first = mixed(seed ^ key_index as u64 ^ 0xbe54_66cf_34e9_0c6c);
        let second = mixed(first);
        let sampled = match self {
            Self::Mixed | Self::NegativeLookup => {
                sample_piecewise(&MIXED_VALUE_BOUNDS, &MIXED_VALUE_WEIGHTS, first, second)
            }
            Self::Reinsertion => sample_piecewise(
                &REINSERTION_VALUE_BOUNDS,
                &REINSERTION_VALUE_WEIGHTS,
                first,
                second,
            ),
        };
        sampled.max(VALUE_HEADER_BYTES)
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Get,
    Set,
    Delete,
}

struct HarnessConfig {
    scenarios: Box<[Scenario]>,
    operations_per_thread: Option<usize>,
    threads: Option<usize>,
    key_count: Option<usize>,
    l1_mib: Option<usize>,
    l2_mib: Option<usize>,
    managed_memory_limit_mib: Option<usize>,
    region_mib: Option<usize>,
    append_shards: u32,
    read_io_workers: usize,
    write_io_workers: usize,
    reclaim_workers: usize,
    latency_sample_interval: usize,
    seed: u64,
    io_engine: IoEngine,
    io_mode: IoMode,
    l1_eviction_policy: L1EvictionPolicy,
    directory: PathBuf,
}

impl HarnessConfig {
    fn from_env() -> io::Result<Self> {
        let scenarios = parse_scenarios()?;
        let operations_per_thread = env_optional_usize("CACHE_WORKLOAD_OPS_PER_THREAD")?;
        let threads = env_optional_usize("CACHE_WORKLOAD_THREADS")?;
        let key_count = env_optional_usize("CACHE_WORKLOAD_KEYS")?;
        let l1_mib = env_optional_usize("CACHE_WORKLOAD_L1_MIB")?;
        let l2_mib = env_optional_usize("CACHE_WORKLOAD_L2_MIB")?;
        let managed_memory_limit_mib =
            env_optional_usize("CACHE_WORKLOAD_MANAGED_MEMORY_LIMIT_MIB")?;
        let region_mib = env_optional_usize("CACHE_WORKLOAD_REGION_MIB")?;
        let append_shards = env_u32("CACHE_WORKLOAD_APPEND_SHARDS", 4)?;
        let read_io_workers = env_usize("CACHE_WORKLOAD_READ_IO_WORKERS", 4)?;
        let write_io_workers = env_usize("CACHE_WORKLOAD_WRITE_IO_WORKERS", 4)?;
        let reclaim_workers = env_usize("CACHE_WORKLOAD_RECLAIM_WORKERS", 1)?;
        let latency_sample_interval = env_usize("CACHE_WORKLOAD_LATENCY_SAMPLE_INTERVAL", 16)?;
        let seed = env_u64("CACHE_WORKLOAD_SEED", DEFAULT_SEED)?;
        let io_engine = match env::var("CACHE_WORKLOAD_IO_ENGINE")
            .unwrap_or_else(|_| "posix".to_owned())
            .as_str()
        {
            "posix" => IoEngine::Posix(PosixIoConfig::new(
                read_io_workers,
                write_io_workers,
                reclaim_workers,
            )),
            "io-uring" => IoEngine::IoUring(IoUringConfig::new(
                IoUringPoolConfig::new(
                    read_io_workers,
                    read_io_workers
                        .checked_mul(64)
                        .ok_or_else(|| invalid("read io_uring depth is too large"))?,
                ),
                IoUringPoolConfig::new(
                    write_io_workers,
                    write_io_workers
                        .checked_mul(64)
                        .ok_or_else(|| invalid("write io_uring depth is too large"))?,
                ),
                IoUringPoolConfig::new(reclaim_workers, reclaim_workers),
            )),
            value => return Err(invalid(format!("unsupported I/O engine: {value}"))),
        };
        let io_mode = match env::var("CACHE_WORKLOAD_IO_MODE")
            .unwrap_or_else(|_| "buffered".to_owned())
            .as_str()
        {
            "buffered" => IoMode::Buffered,
            "direct" => IoMode::Direct,
            value => return Err(invalid(format!("unsupported I/O mode: {value}"))),
        };
        let l1_eviction_policy = match env::var("CACHE_WORKLOAD_L1_EVICTION")
            .unwrap_or_else(|_| "clock".to_owned())
            .as_str()
        {
            "clock" => L1EvictionPolicy::Clock,
            "s3-fifo" => L1EvictionPolicy::S3Fifo,
            value => return Err(invalid(format!("unsupported L1 eviction policy: {value}"))),
        };
        let directory = env::var_os("CACHE_WORKLOAD_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);

        if operations_per_thread == Some(0)
            || threads == Some(0)
            || key_count == Some(0)
            || region_mib.is_some_and(|value| value == 0 || value > 32)
            || append_shards == 0
            || read_io_workers == 0
            || write_io_workers == 0
            || reclaim_workers == 0
            || reclaim_workers > append_shards as usize
            || !directory.is_dir()
        {
            return Err(invalid(
                "invalid workload size, Region size, worker topology, or directory",
            ));
        }

        Ok(Self {
            scenarios,
            operations_per_thread,
            threads,
            key_count,
            l1_mib,
            l2_mib,
            managed_memory_limit_mib,
            region_mib,
            append_shards,
            read_io_workers,
            write_io_workers,
            reclaim_workers,
            latency_sample_interval,
            seed,
            io_engine,
            io_mode,
            l1_eviction_policy,
            directory,
        })
    }

    fn effective(&self, scenario: Scenario) -> io::Result<EffectiveConfig> {
        let operations_per_thread = self
            .operations_per_thread
            .unwrap_or_else(|| scenario.default_operations_per_thread());
        let threads = self.threads.unwrap_or_else(|| scenario.default_threads());
        let key_count = self.key_count.unwrap_or_else(|| scenario.default_keys());
        let l1_mib = self.l1_mib.unwrap_or_else(|| scenario.default_l1_mib());
        let l2_mib = self.l2_mib.unwrap_or_else(|| scenario.default_l2_mib());
        let region_mib = self
            .region_mib
            .unwrap_or_else(|| scenario.default_region_mib());
        if operations_per_thread == 0 || threads == 0 || key_count == 0 {
            return Err(invalid("effective workload counts must be positive"));
        }
        if key_count > u32::MAX as usize {
            return Err(invalid("CACHE_WORKLOAD_KEYS must not exceed 2^32 - 1"));
        }
        let total_operations = operations_per_thread
            .checked_mul(threads)
            .ok_or_else(|| invalid("total operation count is too large"))?;
        let region_bytes = mib_to_usize("CACHE_WORKLOAD_REGION_MIB", region_mib)?;
        let capacity_bytes = mib_to_u64("CACHE_WORKLOAD_L2_MIB", l2_mib)?;
        let l1_bytes = mib_to_usize("CACHE_WORKLOAD_L1_MIB", l1_mib)?;
        if capacity_bytes % region_bytes as u64 != 0
            || capacity_bytes / region_bytes as u64 <= u64::from(self.append_shards)
        {
            return Err(invalid(
                "L2 capacity must be a Region-size multiple with more Regions than append shards",
            ));
        }
        let maximum_record_bytes = scenario
            .maximum_value_bytes()
            .checked_add(MAX_KEY_BYTES + RECORD_OVERHEAD_BYTES)
            .ok_or_else(|| invalid("maximum record size overflow"))?;
        if maximum_record_bytes > region_bytes {
            return Err(invalid(format!(
                "{} workload records do not fit CACHE_WORKLOAD_REGION_MIB={}",
                scenario.slug(),
                region_mib
            )));
        }

        let managed_memory_limit_bytes = match self.managed_memory_limit_mib {
            Some(value) => mib_to_usize("CACHE_WORKLOAD_MANAGED_MEMORY_LIMIT_MIB", value)?,
            None => default_managed_memory_limit(
                l1_bytes,
                region_bytes,
                self.append_shards,
                self.read_io_workers,
                self.reclaim_workers,
                key_count,
            )?,
        };

        Ok(EffectiveConfig {
            scenario,
            operations_per_thread,
            threads,
            key_count,
            total_operations,
            capacity_bytes,
            l1_bytes,
            managed_memory_limit_bytes,
            region_bytes,
            append_shards: self.append_shards,
            read_io_workers: self.read_io_workers,
            write_io_workers: self.write_io_workers,
            reclaim_workers: self.reclaim_workers,
            latency_sample_interval: self.latency_sample_interval,
            seed: self.seed,
            io_engine: self.io_engine,
            io_mode: self.io_mode,
            l1_eviction_policy: self.l1_eviction_policy,
            directory: self.directory.clone(),
        })
    }
}

struct EffectiveConfig {
    scenario: Scenario,
    operations_per_thread: usize,
    threads: usize,
    key_count: usize,
    total_operations: usize,
    capacity_bytes: u64,
    l1_bytes: usize,
    managed_memory_limit_bytes: usize,
    region_bytes: usize,
    append_shards: u32,
    read_io_workers: usize,
    write_io_workers: usize,
    reclaim_workers: usize,
    latency_sample_interval: usize,
    seed: u64,
    io_engine: IoEngine,
    io_mode: IoMode,
    l1_eviction_policy: L1EvictionPolicy,
    directory: PathBuf,
}

impl EffectiveConfig {
    fn static_config(&self) -> StaticConfig {
        StaticConfig::new(self.capacity_bytes)
            .with_region_size_bytes(self.region_bytes as u64)
            .with_expected_entries(self.key_count)
    }

    fn runtime_config(&self) -> RuntimeConfig {
        RuntimeConfig::default()
            .with_io_engine(self.io_engine)
            .with_io_mode(self.io_mode)
            .with_append_shards(self.append_shards)
            .with_l1_capacity_bytes(self.l1_bytes)
            .with_l1_eviction_policy(self.l1_eviction_policy)
            .with_managed_memory_limit_bytes(self.managed_memory_limit_bytes)
            .with_statistics(true)
    }
}

struct BenchFiles {
    data: PathBuf,
}

impl BenchFiles {
    fn new(directory: &Path, scenario: Scenario) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        Self {
            data: directory.join(format!(
                "cache2-mixed-workload-{}-{}-{timestamp}.cache",
                scenario.slug(),
                std::process::id()
            )),
        }
    }
}

impl Drop for BenchFiles {
    fn drop(&mut self) {
        for file in [
            self.data.clone(),
            sidecar(&self.data, ".state"),
            sidecar(&self.data, ".image"),
            sidecar(&self.data, ".image.next"),
        ] {
            let _ = std::fs::remove_file(file);
        }
    }
}

#[derive(Default)]
struct WorkloadResult {
    gets: u64,
    sets: u64,
    deletes: u64,
    hits: u64,
    misses: u64,
    stale_hits: u64,
    set_accepted: u64,
    set_overloaded: u64,
    delete_accepted: u64,
    delete_overloaded: u64,
    get_overloaded: u64,
    attempted_value_bytes: u64,
    accepted_value_bytes: u64,
    served_value_bytes: u64,
    checksum: u64,
    get_latency: LatencyHistogram,
    set_latency: LatencyHistogram,
    delete_latency: LatencyHistogram,
}

impl WorkloadResult {
    fn operations(&self) -> u64 {
        self.gets
            .saturating_add(self.sets)
            .saturating_add(self.deletes)
    }

    fn non_overloaded_operations(&self) -> u64 {
        self.gets
            .saturating_sub(self.get_overloaded)
            .saturating_add(self.set_accepted)
            .saturating_add(self.delete_accepted)
    }

    fn overloads(&self) -> u64 {
        self.get_overloaded
            .saturating_add(self.set_overloaded)
            .saturating_add(self.delete_overloaded)
    }

    fn merge(&mut self, other: Self) {
        self.gets = self.gets.saturating_add(other.gets);
        self.sets = self.sets.saturating_add(other.sets);
        self.deletes = self.deletes.saturating_add(other.deletes);
        self.hits = self.hits.saturating_add(other.hits);
        self.misses = self.misses.saturating_add(other.misses);
        self.stale_hits = self.stale_hits.saturating_add(other.stale_hits);
        self.set_accepted = self.set_accepted.saturating_add(other.set_accepted);
        self.set_overloaded = self.set_overloaded.saturating_add(other.set_overloaded);
        self.delete_accepted = self.delete_accepted.saturating_add(other.delete_accepted);
        self.delete_overloaded = self
            .delete_overloaded
            .saturating_add(other.delete_overloaded);
        self.get_overloaded = self.get_overloaded.saturating_add(other.get_overloaded);
        self.attempted_value_bytes = self
            .attempted_value_bytes
            .saturating_add(other.attempted_value_bytes);
        self.accepted_value_bytes = self
            .accepted_value_bytes
            .saturating_add(other.accepted_value_bytes);
        self.served_value_bytes = self
            .served_value_bytes
            .saturating_add(other.served_value_bytes);
        self.checksum ^= other.checksum;
        self.get_latency.merge(other.get_latency);
        self.set_latency.merge(other.set_latency);
        self.delete_latency.merge(other.delete_latency);
    }
}

struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(RNG_GAMMA);
        mixed(self.state)
    }

    fn bounded(&mut self, upper: u64) -> u64 {
        ((u128::from(self.next_u64()) * u128::from(upper)) >> 64) as u64
    }

    fn open_unit_f64(&mut self) -> f64 {
        const DENOMINATOR: f64 = ((1_u64 << 53) + 1) as f64;
        ((self.next_u64() >> 11) + 1) as f64 / DENOMINATOR
    }
}

fn main() -> io::Result<()> {
    let harness = HarnessConfig::from_env()?;
    let configs = harness
        .scenarios
        .iter()
        .copied()
        .map(|scenario| harness.effective(scenario))
        .collect::<io::Result<Vec<_>>>()?;
    let runtime_threads = configs
        .iter()
        .map(|config| config.threads)
        .max()
        .unwrap_or(2)
        .max(2);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_threads)
        .thread_name("cache2-mixed-workload")
        .enable_time()
        .build()?;
    runtime.block_on(async {
        for config in configs {
            run_scenario(config).await?;
        }
        Ok(())
    })
}

async fn run_scenario(config: EffectiveConfig) -> io::Result<()> {
    let reporter = RunReporter::start("mixed_workloads", Some(config.scenario.slug()));
    let result = run_scenario_inner(config).await;
    reporter.finish(
        result
            .as_ref()
            .err()
            .map(|error| error as &dyn std::fmt::Display),
    );
    result
}

async fn run_scenario_inner(config: EffectiveConfig) -> io::Result<()> {
    let scenario = config.scenario;
    println!("C² mixed workload: {}", scenario.slug());
    println!(
        "effective threads={} ops_per_thread={} total_ops={} keys={} l1_mib={:.1} l2_mib={:.1} region_mib={:.1} managed_limit_mib={:.1} append_shards={} read_workers={} write_workers={} reclaim_workers={} latency_sample_interval={} seed={} l1_eviction={:?} engine={:?} mode={:?}",
        config.threads,
        config.operations_per_thread,
        config.total_operations,
        config.key_count,
        config.l1_bytes as f64 / MIB as f64,
        config.capacity_bytes as f64 / MIB as f64,
        config.region_bytes as f64 / MIB as f64,
        config.managed_memory_limit_bytes as f64 / MIB as f64,
        config.append_shards,
        config.read_io_workers,
        config.write_io_workers,
        config.reclaim_workers,
        config.latency_sample_interval,
        config.seed,
        config.l1_eviction_policy,
        config.io_engine,
        config.io_mode,
    );

    let files = BenchFiles::new(&config.directory, scenario);
    let static_config = config.static_config();
    static_config.validate()?;
    let cache = Arc::new(
        CacheBuilder::from_static(&files.data, static_config)
            .with_runtime_config(config.runtime_config())
            .open()
            .await?,
    );
    let expected: Arc<[AtomicU64]> = (0..config.key_count)
        .map(|_| AtomicU64::new(0))
        .collect::<Vec<_>>()
        .into();
    let config = Arc::new(config);
    let ready = Arc::new(Barrier::new(config.threads + 1));
    let start = Arc::new(Barrier::new(config.threads + 1));
    let mut workers = Vec::with_capacity(config.threads);
    for worker_id in 0..config.threads {
        let cache = Arc::clone(&cache);
        let expected = Arc::clone(&expected);
        let config = Arc::clone(&config);
        let ready = Arc::clone(&ready);
        let start = Arc::clone(&start);
        workers.push(tokio::spawn(async move {
            ready.wait().await;
            start.wait().await;
            run_worker(worker_id, &cache, &expected, &config).await
        }));
    }

    ready.wait().await;
    let workload_started = Instant::now();
    start.wait().await;
    let mut result = WorkloadResult::default();
    let mut first_error = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(worker_result)) => result.merge(worker_result),
            Ok(Err(error)) if first_error.is_none() => first_error = Some(error),
            Ok(Err(_)) => {}
            Err(error) if first_error.is_none() => {
                first_error = Some(io::Error::other(format!(
                    "Mixed workload worker failed: {error}"
                )));
            }
            Err(_) => {}
        }
    }
    let workload_elapsed = workload_started.elapsed();
    if let Some(error) = first_error {
        return Err(error);
    }
    if result.operations() != config.total_operations as u64 {
        return Err(io::Error::other("workload operation count mismatch"));
    }

    let drain_started = Instant::now();
    cache.drain().await?;
    let drain_elapsed = drain_started.elapsed();
    let detailed = cache.detailed_snapshot()?;
    validate_snapshot(&result, &detailed.summary)?;
    report(&config, &result, workload_elapsed, drain_elapsed, &detailed);

    drop(expected);
    drop(config);
    cache.close_fast().await?;
    drop(cache);
    Ok(())
}

async fn run_worker(
    worker_id: usize,
    cache: &Cache,
    expected: &[AtomicU64],
    config: &EffectiveConfig,
) -> io::Result<WorkloadResult> {
    let worker_seed = mixed(
        config.seed ^ config.scenario.seed_salt() ^ (worker_id as u64).wrapping_mul(RNG_GAMMA),
    );
    let mut rng = DeterministicRng::new(worker_seed);
    let mut result = WorkloadResult::default();
    let mut key_buffer = [0_u8; MAX_KEY_BYTES];
    let mut miss_key = [0_u8; NEGATIVE_LOOKUP_KEY_BYTES];
    let mut value = vec![
        0_u8;
        config
            .scenario
            .maximum_value_bytes()
            .max(VALUE_HEADER_BYTES)
    ];

    for operation_index in 0..config.operations_per_thread {
        let global_index = worker_id
            .checked_mul(config.operations_per_thread)
            .and_then(|base| base.checked_add(operation_index))
            .ok_or_else(|| invalid("worker operation ordinal overflow"))?;
        let operation = config.scenario.operation(rng.bounded(100));
        let sample_latency = should_sample(operation, &result, config.latency_sample_interval);

        if config.scenario == Scenario::NegativeLookup {
            write_negative_lookup_key(&mut miss_key, config.seed, global_index as u64);
            result.gets = result.gets.saturating_add(1);
            let started = sample_latency.then(Instant::now);
            let outcome = cache.get(miss_key).await;
            record_sample(&mut result.get_latency, started);
            match outcome {
                Ok(Some(_)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "negative-lookup workload returned a hit for a unique key",
                    ));
                }
                Ok(None) => result.misses = result.misses.saturating_add(1),
                Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                    result.get_overloaded = result.get_overloaded.saturating_add(1);
                }
                Err(error) => return Err(error.into()),
            }
            continue;
        }

        let key_index = sample_key(config.scenario, config.key_count, &mut rng);
        let key_size = config.scenario.key_size(config.seed, key_index);
        write_regular_key(&mut key_buffer[..key_size], key_index as u64);
        let key = &key_buffer[..key_size];

        match operation {
            Operation::Get => {
                result.gets = result.gets.saturating_add(1);
                let started = sample_latency.then(Instant::now);
                let outcome = cache.get(key).await;
                record_sample(&mut result.get_latency, started);
                match outcome {
                    Ok(Some(observed)) => {
                        let latest = expected[key_index].load(Ordering::SeqCst);
                        let observed_version = validate_value(key_index, &observed, latest)?;
                        result.hits = result.hits.saturating_add(1);
                        if observed_version < latest {
                            result.stale_hits = result.stale_hits.saturating_add(1);
                        }
                        result.served_value_bytes = result
                            .served_value_bytes
                            .saturating_add(observed.len() as u64);
                        result.checksum ^= black_box(mixed(
                            key_index as u64
                                ^ observed_version.rotate_left(17)
                                ^ observed.len() as u64,
                        ));
                    }
                    Ok(None) => result.misses = result.misses.saturating_add(1),
                    Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                        result.get_overloaded = result.get_overloaded.saturating_add(1);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Operation::Set => {
                result.sets = result.sets.saturating_add(1);
                let previous = expected[key_index].fetch_add(1, Ordering::SeqCst);
                let version = previous
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("workload value version exhausted"))?;
                let value_size = config.scenario.value_size(config.seed, key_index);
                fill_value(&mut value[..value_size], key_index as u64, version)?;
                result.attempted_value_bytes = result
                    .attempted_value_bytes
                    .saturating_add(value_size as u64);
                let started = sample_latency.then(Instant::now);
                let outcome = cache.put(key, &value[..value_size]);
                record_sample(&mut result.set_latency, started);
                match outcome {
                    Ok(_) => {
                        result.set_accepted = result.set_accepted.saturating_add(1);
                        result.accepted_value_bytes = result
                            .accepted_value_bytes
                            .saturating_add(value_size as u64);
                    }
                    Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                        result.set_overloaded = result.set_overloaded.saturating_add(1);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            Operation::Delete => {
                result.deletes = result.deletes.saturating_add(1);
                let started = sample_latency.then(Instant::now);
                let outcome = cache.delete(key);
                record_sample(&mut result.delete_latency, started);
                match outcome {
                    Ok(_) => {
                        result.delete_accepted = result.delete_accepted.saturating_add(1);
                    }
                    Err(error) if error.kind() == CacheErrorKind::Overloaded => {
                        result.delete_overloaded = result.delete_overloaded.saturating_add(1);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
    }
    Ok(result)
}

fn sample_key(scenario: Scenario, key_count: usize, rng: &mut DeterministicRng) -> usize {
    if scenario != Scenario::Mixed || key_count < 2 {
        return sample_normal_key(0, key_count - 1, rng);
    }
    let split = (key_count.saturating_mul(2) / 5).clamp(1, key_count - 1);
    if rng.bounded(2) == 0 {
        sample_normal_key(0, split - 1, rng)
    } else {
        sample_normal_key(split, key_count - 1, rng)
    }
}

fn sample_normal_key(left: usize, right: usize, rng: &mut DeterministicRng) -> usize {
    if left == right {
        return left;
    }
    let mean = (left as f64 + right as f64) * 0.5;
    let standard_deviation = (right - left) as f64 * 0.25;
    for _ in 0..NORMAL_SAMPLE_ATTEMPTS {
        let radius = (-2.0 * rng.open_unit_f64().ln()).sqrt();
        let angle = std::f64::consts::TAU * rng.open_unit_f64();
        let sampled = (mean + standard_deviation * radius * angle.cos()).round();
        if sampled >= left as f64 && sampled <= right as f64 {
            return sampled as usize;
        }
    }
    mean.round() as usize
}

fn sample_piecewise(
    bounds: &[usize],
    weights: &[u64],
    interval_random: u64,
    value_random: u64,
) -> usize {
    debug_assert_eq!(bounds.len(), weights.len() + 1);
    let total_mass = weights
        .iter()
        .enumerate()
        .map(|(index, weight)| weight.saturating_mul((bounds[index + 1] - bounds[index]) as u64))
        .sum::<u64>();
    let mut ticket = interval_random % total_mass;
    for (index, weight) in weights.iter().enumerate() {
        let width = (bounds[index + 1] - bounds[index]) as u64;
        let mass = weight.saturating_mul(width);
        if ticket < mass {
            return bounds[index] + (value_random % width) as usize;
        }
        ticket -= mass;
    }
    bounds[bounds.len() - 1] - 1
}

fn write_regular_key(key: &mut [u8], key_index: u64) {
    key[..8].copy_from_slice(&key_index.to_le_bytes());
    for (offset, byte) in key[8..].iter_mut().enumerate() {
        *byte = b'a' + ((key_index.wrapping_add(offset as u64) % 26) as u8);
    }
}

fn write_negative_lookup_key(key: &mut [u8; NEGATIVE_LOOKUP_KEY_BYTES], seed: u64, ordinal: u64) {
    key[..8].copy_from_slice(b"c2-miss-");
    key[8..16].copy_from_slice(&seed.to_le_bytes());
    key[16..].copy_from_slice(&ordinal.to_le_bytes());
}

fn fill_value(value: &mut [u8], key_index: u64, version: u64) -> io::Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| invalid("value length exceeds u32"))?;
    let pattern = value_pattern(key_index, version, value.len());
    value.fill(pattern);
    value[..4].copy_from_slice(&VALUE_MAGIC.to_le_bytes());
    value[4..8].copy_from_slice(&length.to_le_bytes());
    value[8..16].copy_from_slice(&key_index.to_le_bytes());
    value[16..VALUE_HEADER_BYTES].copy_from_slice(&version.to_le_bytes());
    Ok(())
}

fn validate_value(key_index: usize, value: &[u8], latest: u64) -> io::Result<u64> {
    if value.len() < VALUE_HEADER_BYTES {
        return Err(invalid_data("Mixed workload returned a truncated value"));
    }
    let magic = u32::from_le_bytes(value[..4].try_into().unwrap());
    let encoded_length = u32::from_le_bytes(value[4..8].try_into().unwrap()) as usize;
    let encoded_key = u64::from_le_bytes(value[8..16].try_into().unwrap());
    let version = u64::from_le_bytes(value[16..VALUE_HEADER_BYTES].try_into().unwrap());
    if magic != VALUE_MAGIC
        || encoded_length != value.len()
        || encoded_key != key_index as u64
        || latest == 0
        || version == 0
        || version > latest
    {
        return Err(invalid_data(
            "Mixed workload returned a wrong-key, malformed, or future value",
        ));
    }
    let pattern = value_pattern(encoded_key, version, value.len());
    if value[VALUE_HEADER_BYTES..]
        .iter()
        .any(|byte| *byte != pattern)
    {
        return Err(invalid_data(
            "Mixed workload returned a value with a malformed payload",
        ));
    }
    Ok(version)
}

fn value_pattern(key_index: u64, version: u64, length: usize) -> u8 {
    mixed(key_index ^ version.rotate_left(29) ^ length as u64) as u8
}

fn record_sample(histogram: &mut LatencyHistogram, started: Option<Instant>) {
    if let Some(started) = started {
        histogram.record(started.elapsed());
    }
}

fn should_sample(operation: Operation, result: &WorkloadResult, interval: usize) -> bool {
    interval != 0
        && match operation {
            Operation::Get => result.gets.is_multiple_of(interval as u64),
            Operation::Set => result.sets.is_multiple_of(interval as u64),
            Operation::Delete => result.deletes.is_multiple_of(interval as u64),
        }
}

fn validate_snapshot(result: &WorkloadResult, snapshot: &cache2::CacheSnapshot) -> io::Result<()> {
    let cache_hits = snapshot.l1_hits.saturating_add(snapshot.l2_hits);
    if snapshot.health != CacheHealth::Running || snapshot.io_failures != 0 {
        return Err(io::Error::other(
            "Mixed workload observed a cache runtime failure",
        ));
    }
    if snapshot.managed_memory_bytes > snapshot.managed_memory_limit_bytes
        || snapshot.managed_memory_peak_bytes > snapshot.managed_memory_limit_bytes
    {
        return Err(io::Error::other(
            "Mixed workload exceeded the managed-memory limit",
        ));
    }
    if snapshot.puts != result.set_accepted
        || snapshot.deletes != result.delete_accepted
        || cache_hits != result.hits
        || snapshot.l2_misses != result.misses
        || snapshot.l2_read_overloads != result.get_overloaded
    {
        return Err(io::Error::other(
            "Mixed workload counters disagree with the cache snapshot",
        ));
    }
    Ok(())
}

fn report(
    config: &EffectiveConfig,
    result: &WorkloadResult,
    workload_elapsed: Duration,
    drain_elapsed: Duration,
    detailed: &cache2::DetailedCacheSnapshot,
) {
    let snapshot = detailed.summary;
    let seconds = workload_elapsed.as_secs_f64();
    let operations_per_second = result.operations() as f64 / seconds;
    let non_overloaded_operations_per_second = result.non_overloaded_operations() as f64 / seconds;
    let overload_rate = result.overloads() as f64 * 100.0 / result.operations() as f64;
    let hit_rate = if result.gets == 0 {
        0.0
    } else {
        result.hits as f64 * 100.0 / result.gets as f64
    };
    let scenario = config.scenario.slug();
    JobReport::new(
        "mixed_workloads",
        Some(scenario),
        "workload",
        "mixed",
        workload_elapsed,
        result.non_overloaded_operations(),
    )
    .workers(config.threads)
    .bytes(u128::from(
        result
            .accepted_value_bytes
            .saturating_add(result.served_value_bytes),
    ))
    .errors(result.overloads())
    .emit();
    JobReport::new(
        "mixed_workloads",
        Some(scenario),
        "get",
        "read",
        workload_elapsed,
        result.hits.saturating_add(result.misses),
    )
    .workers(config.threads)
    .bytes(u128::from(result.served_value_bytes))
    .errors(result.get_overloaded)
    .latency(&result.get_latency, config.latency_sample_interval)
    .emit();
    JobReport::new(
        "mixed_workloads",
        Some(scenario),
        "set",
        "write",
        workload_elapsed,
        result.set_accepted,
    )
    .workers(config.threads)
    .bytes(u128::from(result.accepted_value_bytes))
    .errors(result.set_overloaded)
    .latency(&result.set_latency, config.latency_sample_interval)
    .emit();
    JobReport::new(
        "mixed_workloads",
        Some(scenario),
        "delete",
        "trim",
        workload_elapsed,
        result.delete_accepted,
    )
    .workers(config.threads)
    .errors(result.delete_overloaded)
    .latency(&result.delete_latency, config.latency_sample_interval)
    .emit();
    JobReport::new(
        "mixed_workloads",
        Some(scenario),
        "drain",
        "control",
        drain_elapsed,
        1,
    )
    .emit();
    emit_cache_report(
        "mixed_workloads",
        Some(scenario),
        "workload",
        workload_elapsed.saturating_add(drain_elapsed),
        detailed,
    );
    println!(
        "{}: {:.3} ms, {:.0} attempted ops/s, {:.0} non-overloaded ops/s, overload={:.3}%, hit={:.3}%",
        config.scenario.slug(),
        seconds * 1_000.0,
        operations_per_second,
        non_overloaded_operations_per_second,
        overload_rate,
        hit_rate,
    );
    println!(
        "result benchmark=mixed_workloads scenario={} elapsed_ns={} drain_ns={} operations={} ops_per_sec={operations_per_second:.3} non_overloaded_operations={} non_overloaded_ops_per_sec={non_overloaded_operations_per_second:.3} overloads={} overload_rate={overload_rate:.6} gets={} sets={} deletes={} hits={} misses={} hit_rate={hit_rate:.6} stale_hits={} set_accepted={} set_overloaded={} delete_accepted={} delete_overloaded={} get_overloaded={} attempted_value_bytes={} accepted_value_bytes={} served_value_bytes={} checksum={:016x}",
        config.scenario.slug(),
        workload_elapsed.as_nanos(),
        drain_elapsed.as_nanos(),
        result.operations(),
        result.non_overloaded_operations(),
        result.overloads(),
        result.gets,
        result.sets,
        result.deletes,
        result.hits,
        result.misses,
        result.stale_hits,
        result.set_accepted,
        result.set_overloaded,
        result.delete_accepted,
        result.delete_overloaded,
        result.get_overloaded,
        result.attempted_value_bytes,
        result.accepted_value_bytes,
        result.served_value_bytes,
        result.checksum,
    );
    report_latency(config.scenario, "get", &result.get_latency);
    report_latency(config.scenario, "set", &result.set_latency);
    report_latency(config.scenario, "delete", &result.delete_latency);
    println!(
        "result benchmark=mixed_workloads scenario={} phase=cache puts={} deletes={} written_bytes={} l1_hits={} l1_misses={} l2_hits={} l2_misses={} l2_busy_misses={} l2_memory_misses={} l2_overloads={} promotions={} l1_evictions={} l1_bypasses={} write_rejections={} rotations={} reclaimed_regions={} reinsert_records={} reinsert_bytes={} reinsert_skipped={} reinsert_budget_skipped={} index_values={} index_relocations={} index_overflow_evictions={} read_requests={} write_requests={} managed_bytes={} managed_peak_bytes={} managed_limit_bytes={} logical_disk_peak_bytes={}",
        config.scenario.slug(),
        snapshot.puts,
        snapshot.deletes,
        snapshot.written_bytes,
        snapshot.l1_hits,
        snapshot.l1_misses,
        snapshot.l2_hits,
        snapshot.l2_misses,
        snapshot.l2_read_busy_misses,
        snapshot.l2_read_memory_misses,
        snapshot.l2_read_overloads,
        snapshot.l1_promotions,
        snapshot.l1_evictions,
        snapshot.l1_bypasses,
        snapshot.write_rejections,
        snapshot.region_rotations,
        snapshot.reclaim.regions,
        snapshot.reclaim.reinsert_records,
        snapshot.reclaim.reinsert_bytes,
        snapshot.reclaim.reinsert_skipped,
        snapshot.reclaim.reinsert_budget_skipped,
        detailed.index.physical_value_slots,
        detailed.index.relocations,
        detailed.index.overflow_evictions,
        snapshot.io.read.requests_succeeded,
        snapshot.io.write.requests_succeeded,
        snapshot.managed_memory_bytes,
        snapshot.managed_memory_peak_bytes,
        snapshot.managed_memory_limit_bytes,
        snapshot.logical_disk_peak_bytes,
    );
}

fn report_latency(scenario: Scenario, operation: &str, latency: &LatencyHistogram) {
    let summary = latency.summary();
    println!(
        "result benchmark=mixed_workloads scenario={} phase=latency operation={} samples={} min_ns={} mean_estimate_ns={:.3} stddev_estimate_ns={:.3} p50_upper_ns={} p90_upper_ns={} p95_upper_ns={} p99_upper_ns={} p999_upper_ns={} max_ns={}",
        scenario.slug(),
        operation,
        summary.samples,
        summary.minimum_ns,
        summary.mean_ns,
        summary.standard_deviation_ns,
        summary.p50_upper_ns,
        summary.p90_upper_ns,
        summary.p95_upper_ns,
        summary.p99_upper_ns,
        summary.p999_upper_ns,
        summary.maximum_ns,
    );
}

fn parse_scenarios() -> io::Result<Box<[Scenario]>> {
    let value = env::var("CACHE_WORKLOAD_SCENARIO").unwrap_or_else(|_| "all".to_owned());
    if value == "all" {
        return Ok(Scenario::ALL.into());
    }
    let mut scenarios = Vec::new();
    for name in value.split(',') {
        let scenario = match name {
            "mixed" => Scenario::Mixed,
            "reinsertion" => Scenario::Reinsertion,
            "negative-lookup" => Scenario::NegativeLookup,
            _ => {
                return Err(invalid(format!(
                    "unsupported CACHE_WORKLOAD_SCENARIO entry: {name}"
                )));
            }
        };
        if !scenarios.contains(&scenario) {
            scenarios.push(scenario);
        }
    }
    if scenarios.is_empty() {
        return Err(invalid("CACHE_WORKLOAD_SCENARIO must not be empty"));
    }
    Ok(scenarios.into_boxed_slice())
}

fn default_managed_memory_limit(
    l1_bytes: usize,
    region_bytes: usize,
    append_shards: u32,
    read_io_workers: usize,
    reclaim_workers: usize,
    key_count: usize,
) -> io::Result<usize> {
    let staging_bytes = usize::try_from(append_shards)
        .ok()
        .and_then(|shards| shards.checked_mul(2))
        .and_then(|buffers| buffers.checked_mul(region_bytes))
        .ok_or_else(|| invalid("append staging estimate is too large"))?;
    let io_buffer_bytes = read_io_workers
        .checked_add(reclaim_workers)
        .and_then(|buffers| buffers.checked_mul(region_bytes))
        .ok_or_else(|| invalid("I/O buffer estimate is too large"))?;
    let index_bytes = key_count
        .checked_mul(160)
        .ok_or_else(|| invalid("index memory estimate is too large"))?;
    let bytes = l1_bytes
        .checked_add(staging_bytes)
        .and_then(|total| total.checked_add(io_buffer_bytes))
        .and_then(|total| total.checked_add(index_bytes))
        .and_then(|total| total.checked_add(MANAGED_MEMORY_SLACK_BYTES))
        .ok_or_else(|| invalid("managed-memory estimate is too large"))?;
    bytes
        .div_ceil(MIB)
        .checked_mul(MIB)
        .ok_or_else(|| invalid("managed-memory estimate rounding overflow"))
}

fn mixed(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn env_optional_usize(name: &str) -> io::Result<Option<usize>> {
    match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| invalid(format!("{name} must be an unsigned integer"))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(invalid(format!("cannot read {name}: {error}"))),
    }
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

fn mib_to_usize(name: &str, value: usize) -> io::Result<usize> {
    value
        .checked_mul(MIB)
        .ok_or_else(|| invalid(format!("{name} is too large")))
}

fn mib_to_u64(name: &str, value: usize) -> io::Result<u64> {
    u64::try_from(value)
        .ok()
        .and_then(|mib| mib.checked_mul(MIB as u64))
        .ok_or_else(|| invalid(format!("{name} is too large")))
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
