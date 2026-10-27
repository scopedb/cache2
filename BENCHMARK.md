# C² validation

Compare revisions only on the same host, filesystem, device, toolchain, and workload. Keep raw output with the tested commit; this repository intentionally does not publish machine-specific historical numbers.

## Ablation experiments

Start from a saved source tree and binary, including any uncommitted changes. Change one mechanism per experimental build, record its patch and binary checksum, and keep routing, workload, memory limits, admission ceilings, statistics, and I/O topology fixed. Run builds and timed workloads separately. Retain every sample, including regressions and failed runs; a correctness or memory-bound failure disqualifies a candidate regardless of throughput.

Use isolated measurements to explain a cost before changing the architecture. For L1 accounting, compare same-key replacement with new-key eviction at a full fixed byte budget: replacement has no eviction-counter increment, while both paths transfer an exclusive memory charge. Measure each change separately and together to expose interactions. Check small and 16 KiB values, and retain data-validation and exact memory-accounting checks. An isolated speedup does not establish a whole-cache speedup.

Before editing the measured production path, qualify the measurement with byte-identical A/A runs. Use the comparison procedure below, and stop if its uncertainty or drift gate fails. Qualify the chosen implementation with balanced baseline/candidate runs using the batch-plus-drain and mixed-turnover workloads below. Compare effective hit throughput, accepted writes, rejections, latency, CPU, and RSS together. Include a single-writer case and a longer sampled Adaptive run when the changed mechanism affects contention or allocation. Treat variation in an unaffected path as a noise control; repeat or lengthen measurements before attributing small differences to a change. Keep experimental switches and rejected candidates out of the production request path.

## Noise control and paired comparisons

Build and save each binary before starting measurements. Save its source snapshot or patch, toolchain, and workload environment alongside the output. Do not compile, run tests, or profile concurrently with a timed comparison. Keep the host, filesystem, I/O topology, statistics, memory limits, and power state fixed. On macOS laptops, connected AC power alone is insufficient: a charger can be connected while the battery continues discharging.

The request benchmark retains the cold single-batch `put_drain` result. For steadier write measurements, set `CACHE_BENCH_WRITE_ROUNDS` to a positive fixed count and optionally set `CACHE_BENCH_WRITE_WARMUP_ROUNDS` (default 2). The additional `put_steady` phase overwrites the same key set after the cold batch, reuses the same writer threads and payload buffers, assigns work in fixed 64-operation chunks, and drains every round. Chunk assignment prevents one slowly scheduled worker from holding an entire static share of a short batch. Only the configured measurement rounds contribute to its elapsed time, operations, retries, and bytes. Thread creation, warmup, final payload checks, and close are outside that interval. Dispatch, admission, and drain are inside it. This measures repeated overwrite and, when enough bytes are written, Region turnover; it is a different workload from cold population. Both phases start their clock before releasing any writer. `CACHE_BENCH_WRITE_ONLY=true` skips the subsequent read/recovery phases so comparisons can focus on writing; use the full default benchmark and soak warm verification separately for qualification.

Calibrate the round count in a pilot so the selected measured phase lasts at least five seconds, then freeze it across all builds. Increase warmup until startup allocation and page faults no longer dominate; do not silently substitute longer single batches for repeated short batches. A longer interval alone does not establish stability. Keep the original cold results, but do not infer small improvements from a single millisecond-scale batch.

`scripts/compare-benchmarks.py` requires Python 3 and an environment JSON object with string keys and values. It runs saved binaries serially in alternating ABBA and BAAB blocks, saves every raw log, binary hash, power sample, and parsed result, and reports paired geometric changes with a deterministic 95% bootstrap interval over whole blocks. The default is eight blocks, or sixteen runs of each binary. The point estimate and interval use the paired blocks; marginal medians are also reported and need not imply exactly the same change. This interval describes repeated runs on the tested host, not uncertainty across hardware or workloads.

```sh
python3 scripts/test_compare_benchmarks.py
python3 scripts/compare-benchmarks.py \
  --baseline /tmp/before/cache --candidate /tmp/before/cache \
  --env /tmp/write-environment.json --output /tmp/write-aa \
  --metric put_steady --aa
python3 scripts/compare-benchmarks.py \
  --baseline /tmp/before/cache --candidate /tmp/after/cache \
  --env /tmp/write-environment.json --output /tmp/write-ab \
  --metric put_steady
```

A/A requires byte-identical binaries. By default its entire interval must fit within ±3%, and the spread of the ABBA block centers must stay within 5%; excessive absolute drift also fails A/B. Selected phases shorter than five seconds, benchmark failures, and missing metrics stop the run and retain evidence. On macOS, the runner records power state before and after each process and stops on battery power, discharging, or battery below 20%. Desktop Macs do not need a battery. Explicit thresholds are recorded in the manifest; select them before measuring and do not relax them after seeing a candidate's result. Run the same A/A control for each relevant workload, and repeat controls if the environment changes. Do not discard outliers or rerun only unfavorable samples to manufacture a pass.

An external affinity or scheduling launcher can be supplied with repeated `--prefix` arguments, one argument at a time, including `--prefix=-a` for an option beginning with a dash. The runner applies the same prefix to both binaries and records it in the manifest. A launcher does not establish stability; its runs still need the same A/A qualification.

For mixed turnover, pass `cache_soak` binaries and select `--metric get --metric hit --metric put`. `hit` counts validated application-visible hits per second, independently of library statistics, so fast misses cannot masquerade as useful read throughput. Set `CACHE_SOAK_ACTIVITY_COUNTERS=false` to measure the default library path; the harness still validates every hit, counts outcomes, samples latency, checks memory bounds, and can perform final warm verification. Keep those observer settings constant. Compare CPU, retries/rejections, latency, hit population, and RSS from the saved logs alongside throughput. Retain an optimization only after fresh confirmation, with a gain larger than the measured noise and no unexplained loss in the other qualification workloads.

## Unified report format

All benchmark targets emit a common fio-style report in addition to their existing `result ...` records. The human-readable section groups work into jobs and reports completed-operation rate, attempts, payload bandwidth, bytes, runtime, worker count, errors or overloads, and sampled latency. Every run ends with process CPU time, context switches, page faults, peak RSS, and pass or error status.

Lines beginning with `report version=2` are the stable machine-readable form. They are whitespace-separated `key=value` records with these types:

| Type             | Contents                                                                                 |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `header`         | Benchmark identity, scenario, operating system, and architecture                         |
| `job`            | IOPS, bandwidth, bytes, runtime, workers, errors, and latency distribution               |
| `io`             | Read/write submissions, completions, failures, cancellation, depth, timing, and I/O path |
| `cache`          | Request outcomes, hits/misses, overload, promotion, rotation, and reclaim                |
| `resources`      | Managed memory, peak RSS, logical disk use, and L1/index/Region occupancy                |
| `process_memory` | Soak phase, current RSS, and process peak RSS                                            |
| `run`            | Overall status, runtime, CPU, scheduler, faults, and peak RSS                            |

`mixed_workloads` and `cache_soak` emit the cache-specific records by default; soak records remain available with activity counters disabled, but disabled activity fields are zero; `cache` emits them when `CACHE_BENCH_ACTIVITY_COUNTERS=true`. The `phase` field distinguishes independently reset cache instances, such as the initial write and warm-reopened L2 phases. Existing `result ...` lines are kept for qualification scripts and compatibility.

The `io` records count logical engine requests and positive runtime file operations. They distinguish buffered and direct paths, but they are not physical-device I/O counters; use OS or device telemetry when that distinction matters.

Latency uses a fixed 65-bucket log2 histogram, so measurement memory is bounded and independent of operation count. Percentiles are bucket upper bounds; `avg~` and `stdev~` are estimates from bucket midpoints. The default sampling interval is 16 operations. Set the target's latency sampling interval to zero to disable it. Non-soak workers update thread-local histograms; each soak worker owns separate counters and histograms, with three uncontended relaxed atomic updates per sampled operation. Periodic snapshots merge the bounded worker records, and final bandwidth uses payload bytes accumulated locally by each worker, independently of library statistics. Sampling otherwise adds one timer start and one elapsed-time read to each selected operation.

## Request-path benchmark

```sh
cargo +1.98.0 x bench --locked --bench cache
```

The benchmark measures accepted puts plus drain, resident L1 reads, warm close, and successful L2 reads with best-effort promotion. The `put_admission` result separately reports `admission_elapsed_ns` and `drain_elapsed_ns`, while `put_drain` retains their combined throughput. Values encode their key ordinal, so a wrong-key result fails the run. Harness retries of `ErrorKind::Overloaded` are workload setup, not library behavior.

The main controls are grouped below. See `benchmarks/cache/main.rs` for defaults and validation rules.

| Purpose        | Variables                                                                                                                                                                                                                                                             |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data shape     | `CACHE_BENCH_ENTRIES`, `CACHE_BENCH_VALUE_BYTES`, `CACHE_BENCH_RESIDENT_ENTRIES`                                                                                                                                                                                      |
| Capacity       | `CACHE_BENCH_CAPACITY_MIB`, `CACHE_BENCH_L1_CAPACITY_MIB`, `CACHE_BENCH_MANAGED_MEMORY_LIMIT_MIB`                                                                                                                                                                     |
| Concurrency    | `CACHE_BENCH_CLIENTS`, `CACHE_BENCH_WRITE_CLIENTS`, `CACHE_BENCH_APPEND_SHARDS`, `CACHE_BENCH_POSIX_READ_WORKERS`, `CACHE_BENCH_READ_IO_WAIT_CAPACITY`, `CACHE_BENCH_READ_IO_WAIT_TIMEOUT_US`, `CACHE_BENCH_POSIX_WRITE_WORKERS`, `CACHE_BENCH_POSIX_RECLAIM_WORKERS` |
| I/O path       | `CACHE_BENCH_IO_ENGINE`, `CACHE_BENCH_IO_MODE`, `CACHE_BENCH_DIR`                                                                                                                                                                                                     |
| Cache policy   | `CACHE_BENCH_L1_EVICTION=clock\|s3-fifo`, `CACHE_BENCH_HOT_ENTRIES`, `CACHE_BENCH_HOT_READ_INTERVAL`                                                                                                                                                                  |
| Batching       | `CACHE_BENCH_WRITE_FLUSH_THRESHOLD_BYTES` (defaults to the library threshold; the effective byte value is printed)                                                                                                                                                    |
| Measurement    | `CACHE_BENCH_READ_LATENCY_SAMPLE_INTERVAL` (default 16; zero disables), `CACHE_BENCH_ACTIVITY_COUNTERS` (default false), `CACHE_BENCH_WRITE_ROUNDS` (default 0), `CACHE_BENCH_WRITE_WARMUP_ROUNDS` (default 2), `CACHE_BENCH_WRITE_ONLY` (default false)              |
| Gates          | `CACHE_BENCH_MIN_PUT_OPS`, `CACHE_BENCH_MIN_RESIDENT_L1_OPS`, `CACHE_BENCH_MIN_L2_OPS`, `CACHE_BENCH_MAX_WARM_CLOSE_MS`                                                                                                                                               |

Request and latency collection are controlled independently by `CACHE_BENCH_REQUEST_STATS` (complete request counters, default false), `CACHE_BENCH_L1_LATENCY_SAMPLE_INTERVAL`, `CACHE_BENCH_L2_LATENCY_SAMPLE_INTERVAL`, and `CACHE_BENCH_MUTATION_LATENCY_SAMPLE_INTERVAL` (independent: 0 off, 1 full, greater values sample with that mean interval; default 0), `CACHE_BENCH_IO_LATENCY` (full engine latency, default false), and `CACHE_BENCH_STATS_SHARDS` (default 16). These instrument the library itself. `CACHE_BENCH_READ_LATENCY_SAMPLE_INTERVAL` remains the independent benchmark observer and should be held constant across comparisons. The effective statistics settings are printed with each run. Compare disabled, counters-only, full and sampled modes on the same workload, keeping actual hit/overload populations in view.

`CACHE_BENCH_ACTIVITY_COUNTERS=true` adds cache accounting on the measured request path. Use the same setting for baseline and candidate runs. The `result phase=read_io` line then also reports `in_flight_peak`, `configured_in_flight`, `slot_wait_ns`, `payload_bytes`, and `read_amp` (engine read bytes divided by served payload bytes). The cache report also includes `write_buffer_rejections` alongside total `write_rejections`.

For device measurements, use a data set larger than host RAM and no larger than half of L2 capacity. Run baseline and candidate in alternating order at least five times, compare medians, and retain every sample. Throughput does not replace correctness, overload, memory, or latency checks.

## Mixed workload benchmark

```sh
cargo +1.98.0 x bench --locked --bench mixed_workloads
```

This harness runs three deterministic request profiles:

| Scenario            | Request semantics                                                                    | Scaled default                                                     |
| ------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------ |
| `mixed`             | 15% get, 80% set, 5% delete; two key groups; piecewise key and value sizes           | 2 × 1,000 operations, 625 keys, 32 MiB L1, 64 MiB L2               |
| `reinsertion`       | 50% get, 50% set; truncated-normal popularity; 1–10 KiB values; version validation   | 8 × 5,000 operations, 1,000 keys, 1 MiB L1, 8 MiB L2               |
| `negative-lookup`   | Every lookup uses a new key that cannot already exist                                | 8 × 25,000 operations, 1,000 configured keys, 1 MiB L1, 5 MiB L2   |

Operation counts are per thread. The scaled `mixed` and `reinsertion` defaults use total-operation-to-key ratios of 3.2 and 40. Select one or several scenarios with `CACHE_WORKLOAD_SCENARIO=mixed`, `reinsertion`, `negative-lookup`, or a comma-separated list; the default is `all`. The scaled Region size is 4 MiB for `mixed` and 1 MiB for the other scenarios.

Use the following controls to scale a run:

| Purpose          | Variables                                                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| Request stream   | `CACHE_WORKLOAD_OPS_PER_THREAD`, `CACHE_WORKLOAD_THREADS`, `CACHE_WORKLOAD_KEYS`, `CACHE_WORKLOAD_SEED`                                           |
| Capacity         | `CACHE_WORKLOAD_L1_CAPACITY_MIB`, `CACHE_WORKLOAD_CAPACITY_MIB`, `CACHE_WORKLOAD_REGION_SIZE_MIB`, `CACHE_WORKLOAD_MANAGED_MEMORY_LIMIT_MIB`      |
| Concurrency      | `CACHE_WORKLOAD_APPEND_SHARDS`, `CACHE_WORKLOAD_POSIX_READ_WORKERS`, `CACHE_WORKLOAD_POSIX_WRITE_WORKERS`, `CACHE_WORKLOAD_POSIX_RECLAIM_WORKERS` |
| I/O and policy   | `CACHE_WORKLOAD_IO_ENGINE`, `CACHE_WORKLOAD_IO_MODE`, `CACHE_WORKLOAD_L1_EVICTION`, `CACHE_WORKLOAD_DIR`                                          |
| Measurement      | `CACHE_WORKLOAD_LATENCY_SAMPLE_INTERVAL` (default 16; zero disables)                                                                              |

The harness uses a fixed seed, truncated-normal popularity, and piecewise-constant key and value sizes. Keys are at least eight bytes so their identity is verifiable. Values are at least 24 bytes so every hit can be checked for key, length, version, and payload. `negative-lookup` uses the fixed seed and operation ordinal for guaranteed-unique deterministic keys.

Request throughput includes generation, value population, cache calls, and hit validation. Sampled latency covers only the public cache call and is reported as bounded log2 histogram quantiles. Foreground overload is counted as an outcome and is never retried. Drain time is reported separately, followed by C² L1/L2, Region rotation, reclaim, reinsertion, I/O, and managed-memory counters.

## Mixed turnover

```sh
cargo +1.98.0 x bench --locked --bench cache_soak
```

The default short run mixes reads, writes, deletes, Region rotation, reclaim, and 256 B, 4 KiB, 16 KiB, and 256 KiB values. The read stream retains its shared ordinal and cyclic key permutation. Reader and writer ordinals are isolated from each other and from the stop flag, and each worker owns its outcome counters and latency histogram. Compare revisions using the same harness version, since observer overhead also consumes CPU. Each hit is checked for key, version, length, and payload. Older valid values are counted as stale; future, wrong-key, malformed, I/O-error, or managed-memory/RSS bound violations fail the run.

Use `CACHE_SOAK_*` to select duration, sample interval, capacity, L1 and managed memory, key count, mixed value sizes, client counts, worker topology, I/O path, and output directory. Important qualification switches are:

- `CACHE_SOAK_WARM_REOPEN=true` to recover before measurement;
- `CACHE_SOAK_WRITERS=0` with warm reopen to measure a pre-populated, read-only mixed-size data set;
- `CACHE_SOAK_FINAL_WARM_VERIFY=true` to scan every key after recovery and validate every hit;
- `CACHE_SOAK_REQUIRE_PATH_COVERAGE=true` to require writes, deletes, L2 reads, rotation, reclaim, and warm recovery;
- `CACHE_SOAK_REQUIRE_REINSERT_COVERAGE=true` for a focused read-heavy run that must both reinsert a hot record and exhaust the bounded reinsert budget.

`CACHE_SOAK_ACTIVITY_COUNTERS` defaults to true for diagnostic coverage. Set it to false for the library default path. `CACHE_SOAK_REQUIRE_PATH_COVERAGE` and `CACHE_SOAK_REQUIRE_REINSERT_COVERAGE` require activity counters and are rejected when counters are disabled; value validation, outcome counting, final warm verification, and memory checks remain active. Workers use a common start gate, with timing beginning before they are released.

`CACHE_SOAK_LATENCY_SAMPLE_INTERVAL` controls put, get, and delete latency sampling. It defaults to 16; zero disables latency sampling.

The soak samples current RSS on Linux and macOS and enforces the existing memory allowance during the workload. Other platforms report zero when current RSS is unavailable. `process_memory` records expose `current_rss_bytes` and `peak_rss_bytes` at open, periodic samples, and warm-close/drop/reopen boundaries. Reopen explicitly drops the closed cache before allocating its replacement, so the reported peak does not include two simultaneously retained cache instances. Allocator-retained pages still count toward RSS even after the cache's managed charges are released.

For concurrent 16 KiB turnover, compare eight writers and four readers with `CACHE_SOAK_VALUE_BYTES=16384`, `CACHE_SOAK_KEYS=16384`, `CACHE_SOAK_CAPACITY_MIB=256`, `CACHE_SOAK_L1_CAPACITY_MIB=64`, `CACHE_SOAK_MANAGED_MEMORY_LIMIT_MIB=768`, and `CACHE_SOAK_APPEND_SHARDS=4`. Hold the duration, sampling interval, I/O topology, and final warm verification constant. Run Disabled and Adaptive admission separately in alternating revision order at least five times. Compare get throughput together with L1 hits, L2 hits, misses, accepted puts, overloads, latency, CPU, managed peak, and phase RSS: faster fills can change the read-hit population, and fast misses alone are not a throughput improvement. Pair this with `cache` runs using 8,192 and 65,536 entries, 16 KiB values, eight write clients, four append shards, and the default flush threshold to check that batch-write-plus-drain gains remain. A longer sampled Adaptive run should distinguish a stable resident footprint from continuing allocator retention.

The I/O topology uses the shared backend-specific controls below, with `CACHE_SOAK` as the prefix.

Repeat sizes in `CACHE_SOAK_VALUE_BYTES` to weight a production distribution. Use a short matrix rather than one oversized run: small-capacity turnover, high-cardinality mixed sizes, read-heavy reinsertion, and CLOCK/S3-FIFO A/B. Longer soaks are optional follow-up evidence, not a default gate.

## Shared I/O and capacity controls

`cache`, `cache_soak`, and `mixed_workloads` share the same pool parser, using the `CACHE_BENCH`, `CACHE_SOAK`, and `CACHE_WORKLOAD` prefixes respectively. `<ROLE>` is `READ`, `WRITE`, or `RECLAIM`.

| Backend   | Variable suffix                  | Default (read / write / reclaim)  |
| --------- | -------------------------------- | --------------------------------- |
| POSIX     | `_POSIX_<ROLE>_WORKERS`          | 4 / 4 / 1                         |
| io_uring  | `_IO_URING_<ROLE>_RINGS`         | 1 / 1 / 1                         |
| io_uring  | `_IO_URING_<ROLE>_MAX_IN_FLIGHT` | 64 / 64 / 1                       |
| io_uring  | `_IO_URING_<ROLE>_IOPOLL`        | false                             |
| io_uring  | `_IO_URING_<ROLE>_SQPOLL_MS`     | absent (disabled)                 |
| io_uring  | `_IO_URING_<ROLE>_SQPOLL_CPU`    | absent (unpinned)                 |

Optional fill admission uses `_FILL_CONTROL=disabled|observe|adaptive` under the same three prefixes. Enabled modes require explicit `_FILL_BYTES_PER_SECOND` and `_FILL_OPERATIONS_PER_SECOND` ceilings. The harness prints the effective options and adds a `type=fill_control` report beside cache records. Compare the modes in alternating order with identical ceilings and traffic; record rejected and hypothetical fills alongside completed throughput. High ceilings help measure instrumentation overhead, while lower ceilings and injected storage stalls exercise admission behavior. Buffered macOS results do not qualify Linux NVMe or cgroup throttling.

Select the backend with `_IO_ENGINE=posix|io-uring`. POSIX workers bound concurrent operations. io_uring ring count and aggregate in-flight limit are independent; changing one does not rewrite the other. Start with one ring per pool and set `_IO_URING_READ_MAX_IN_FLIGHT` to the concurrent L2 get count (`CACHE_BENCH_CLIENTS` in the request benchmark). Leave write and reclaim unset so they follow library defaults. Extra rings split the same depth and do not add slots. Ring count must not exceed the in-flight limit. SQPOLL and IOPOLL environment variables are advanced follow-ups with different requirements: IOPOLL requires `_IO_MODE=direct`; SQPOLL CPU requires an idle timeout. Do not enable them in the same sweep as rings or in-flight depth, and do not enable both as a pair. Only the selected backend's settings are read. Each harness prints the resulting `IoEngineOptions`, and buffer estimates use its actual concurrency. The request benchmark's default read-wait capacity follows the selected read pool's in-flight limit. POSIX Immediate L2 reads still cap `CACHE_BENCH_CLIENTS` at the POSIX read-worker count, so a worker sweep must set clients at least as high as the worker count.

`CACHE_BENCH_STATS` is now `CACHE_BENCH_ACTIVITY_COUNTERS`. Machine-readable reports use `version=2`, renaming the cache record field `statistics_enabled` to `activity_counters_enabled`; the counter population is unchanged.

The previous `_READ_IO_WORKERS`, `_WRITE_IO_WORKERS`, and `_RECLAIM_WORKERS` variables are rejected with a migration error. For POSIX, use `_POSIX_<ROLE>_WORKERS`. For io_uring, specify ring count and total in-flight depth separately. Unset io_uring knobs follow `IoUringOptions::default()`: one ring and 64 in-flight requests for read and write, and one ring and one in-flight request for reclaim. To reproduce a previous POSIX-shaped default of 4 workers, set 4 rings and 256 in-flight requests.

Capacity variables follow the library's terms: `_CAPACITY_MIB` is L2, `_L1_CAPACITY_MIB` is L1, `_MANAGED_MEMORY_LIMIT_MIB` is the overall managed-memory budget, and `_REGION_SIZE_MIB` selects Region size where supported. The former `_MEMORY_MIB`, `_L1_MIB`, `_L2_MIB`, and `_REGION_MIB` names are rejected with their replacements. `recovery_scale` uses the same L1 naming under `CACHE_RECOVERY`.

## Linux NVMe qualification

```sh
./scripts/qualify-linux-nvme.sh \
  /mnt/nvme \
  /var/tmp/cache2-qualification
```

The runner records the revision, machine, filesystem, block device, complete configuration, raw logs, medians, and checksums. It exercises buffered and Direct POSIX I/O, a POSIX Direct worker sweep with matching client depth, an experimental io_uring Direct in-flight sweep (one ring at 16/32/64 concurrent gets), mixed turnover, and final warm recovery. Qualification builds with `--features io-uring` and sets `CACHE_BENCH_ACTIVITY_COUNTERS=true` so `result phase=read_io` records in-flight peak, slot wait, payload bytes, and read amplification. SQPOLL and IOPOLL are advanced host follow-ups, not part of the default matrix; IOPOLL still needs `CACHE_BENCH_IO_URING_READ_IOPOLL=true` with Direct I/O.

A release pass requires:

- a clean worktree and exact Rust 1.98.0;
- a verified non-rotational NVMe device;
- an L2 data set larger than physical RAM;
- all four performance gates configured;
- five benchmark samples and at least a 30-minute turnover run.

Relaxing hardware, worktree, sample, or duration checks produces preflight evidence only. Set the `CACHE_BENCH_*` shape and gate variables for the target machine before running the script.

## Focused checks

Exercise a production-load-factor index without storage I/O:

```sh
cargo +1.98.0 x bench --locked --bench region_index_turnover
```

Measure cold open, warm image publication, mmap recovery, and RSS at a chosen index scale:

```sh
cargo +1.98.0 x bench --locked --bench recovery_scale
```

Run the 10,000-case property tests for persistent decoders, record round trips, and the two bounded index state machines:

```sh
cargo test --package cache2 --lib property_tests::
```

Treat any correctness failure, unexpected unbounded growth, missed required path, or recovery mismatch as a failed result regardless of throughput.
