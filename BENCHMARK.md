# C² validation

Compare revisions only on the same host, filesystem, device, toolchain, and
workload. Keep raw output with the tested commit; this repository intentionally
does not publish machine-specific historical numbers.

## Request-path benchmark

```sh
cargo +1.98.0 bench --locked --package benchmarks --bench cache
```

The benchmark measures accepted puts plus drain, resident L1 reads, warm close,
and successful L2 reads with best-effort promotion. Values encode their key
ordinal, so a wrong-key result fails the run. Harness retries of
`ErrorKind::Overloaded` are workload setup, not library behavior.

The main controls are grouped below. See `benchmarks/cache/main.rs` for
defaults and validation rules.

| Purpose | Variables |
| --- | --- |
| Data shape | `CACHE_BENCH_ENTRIES`, `CACHE_BENCH_VALUE_BYTES`, `CACHE_BENCH_RESIDENT_ENTRIES` |
| Capacity | `CACHE_BENCH_CAPACITY_MIB`, `CACHE_BENCH_MEMORY_MIB`, `CACHE_BENCH_MANAGED_MEMORY_LIMIT_MIB` |
| Concurrency | `CACHE_BENCH_CLIENTS`, `CACHE_BENCH_WRITE_CLIENTS`, `CACHE_BENCH_APPEND_SHARDS`, `CACHE_BENCH_READ_IO_WORKERS`, `CACHE_BENCH_WRITE_IO_WORKERS`, `CACHE_BENCH_RECLAIM_WORKERS` |
| I/O path | `CACHE_BENCH_IO_ENGINE`, `CACHE_BENCH_IO_MODE`, `CACHE_BENCH_DIR` |
| Cache policy | `CACHE_BENCH_L1_EVICTION=clock|s3-fifo`, `CACHE_BENCH_HOT_ENTRIES`, `CACHE_BENCH_HOT_READ_INTERVAL` |
| Gates | `CACHE_BENCH_MIN_PUT_OPS`, `CACHE_BENCH_MIN_RESIDENT_L1_OPS`, `CACHE_BENCH_MIN_L2_OPS`, `CACHE_BENCH_MAX_WARM_CLOSE_MS` |

For device measurements, use a data set larger than host RAM and no larger than
half of L2 capacity. Run baseline and candidate in alternating order at least
five times, compare medians, and retain every sample. Throughput does not
replace correctness, overload, memory, or latency checks.

## Mixed workload benchmark

```sh
cargo +1.98.0 bench --locked --package benchmarks --bench mixed_workloads
```

This harness runs three deterministic request profiles:

| Scenario | Request semantics | Scaled default |
| --- | --- | --- |
| `mixed` | 15% get, 80% set, 5% delete; two key groups; piecewise key and value sizes | 2 × 1,000 operations, 625 keys, 32 MiB L1, 64 MiB L2 |
| `reinsertion` | 50% get, 50% set; truncated-normal popularity; 1–10 KiB values; version validation | 8 × 5,000 operations, 1,000 keys, 1 MiB L1, 8 MiB L2 |
| `negative-lookup` | Every lookup uses a new key that cannot already exist | 8 × 25,000 operations, 1,000 configured keys, 1 MiB L1, 5 MiB L2 |

Operation counts are per thread. The scaled `mixed` and `reinsertion` defaults
use total-operation-to-key ratios of 3.2 and 40. Select one or several scenarios
with `CACHE_WORKLOAD_SCENARIO=mixed`, `reinsertion`, `negative-lookup`, or a
comma-separated list; the default is `all`. The scaled Region size is 4 MiB for
`mixed` and 1 MiB for the other scenarios.

Use the following controls to scale a run:

| Purpose | Variables |
| --- | --- |
| Request stream | `CACHE_WORKLOAD_OPS_PER_THREAD`, `CACHE_WORKLOAD_THREADS`, `CACHE_WORKLOAD_KEYS`, `CACHE_WORKLOAD_SEED` |
| Capacity | `CACHE_WORKLOAD_L1_MIB`, `CACHE_WORKLOAD_L2_MIB`, `CACHE_WORKLOAD_REGION_MIB`, `CACHE_WORKLOAD_MANAGED_MEMORY_LIMIT_MIB` |
| Concurrency | `CACHE_WORKLOAD_APPEND_SHARDS`, `CACHE_WORKLOAD_READ_IO_WORKERS`, `CACHE_WORKLOAD_WRITE_IO_WORKERS`, `CACHE_WORKLOAD_RECLAIM_WORKERS` |
| I/O and policy | `CACHE_WORKLOAD_IO_ENGINE`, `CACHE_WORKLOAD_IO_MODE`, `CACHE_WORKLOAD_L1_EVICTION`, `CACHE_WORKLOAD_DIR` |
| Measurement | `CACHE_WORKLOAD_LATENCY_SAMPLE_INTERVAL` |

The harness uses a fixed seed, truncated-normal popularity, and
piecewise-constant key and value sizes. Keys are at least eight bytes so their
identity is verifiable. Values are at least 24 bytes so every hit can be checked
for key, length, version, and payload. `negative-lookup` uses the fixed seed and
operation ordinal for guaranteed-unique deterministic keys.

Request throughput includes generation, value population, cache calls, and hit
validation. Sampled latency covers only the public cache call and is reported
as bounded log2 histogram quantiles. Foreground overload is counted as an
outcome and is never retried. Drain time is reported separately, followed by
C² L1/L2, Region rotation, reclaim, reinsertion, I/O, and managed-memory
counters.

## Mixed turnover

```sh
cargo +1.98.0 bench --locked --package benchmarks --bench cache_soak
```

The default short run mixes reads, writes, deletes, Region rotation, reclaim,
and 256 B, 4 KiB, 16 KiB, and 256 KiB values. Each hit is checked for key,
version, length, and payload. Older valid values are counted as stale; future,
wrong-key, malformed, I/O-error, or managed-memory/RSS bound violations fail
the run.

Use `CACHE_SOAK_*` to select duration, sample interval, capacity, L1 and managed
memory, key count, mixed value sizes, client counts, worker topology, I/O path,
and output directory. Important qualification switches are:

- `CACHE_SOAK_WARM_REOPEN=true` to recover before measurement;
- `CACHE_SOAK_FINAL_WARM_VERIFY=true` to scan every key after recovery and
  validate every hit;
- `CACHE_SOAK_REQUIRE_PATH_COVERAGE=true` to require writes, deletes, L2 reads,
  rotation, reclaim, and warm recovery;
- `CACHE_SOAK_REQUIRE_REINSERT_COVERAGE=true` for a focused read-heavy run that
  must both reinsert a hot record and exhaust the bounded reinsert budget.

Repeat sizes in `CACHE_SOAK_VALUE_BYTES` to weight a production distribution.
Use a short matrix rather than one oversized run: small-capacity turnover,
high-cardinality mixed sizes, read-heavy reinsertion, and CLOCK/S3-FIFO A/B.
Longer soaks are optional follow-up evidence, not a default gate.

## Linux NVMe qualification

```sh
./scripts/qualify-linux-nvme.sh \
  /mnt/nvme \
  /var/tmp/cache2-qualification
```

The runner records the revision, machine, filesystem, block device, complete
configuration, raw logs, medians, and checksums. It exercises buffered and
direct POSIX I/O, a worker-count sweep, mixed turnover, and final warm recovery.

A release pass requires:

- a clean worktree and exact Rust 1.98.0;
- a verified non-rotational NVMe device;
- an L2 data set larger than physical RAM;
- all four performance gates configured;
- five benchmark samples and at least a 30-minute turnover run.

Relaxing hardware, worktree, sample, or duration checks produces preflight
evidence only. Set the `CACHE_BENCH_*` shape and gate variables for the target
machine before running the script.

## Focused checks

Exercise a production-load-factor index without storage I/O:

```sh
cargo +1.98.0 bench --locked --package benchmarks \
  --bench region_index_turnover
```

Measure cold open, warm image publication, mmap recovery, and RSS at a chosen
index scale:

```sh
cargo +1.98.0 bench --locked --package benchmarks --bench recovery_scale
```

Fuzz persistent decoders and bounded index probes:

```sh
cargo fuzz run persistent_decoders -- \
  -runs=10000 -max_len=16384 -print_final_stats=1
```

Treat any correctness failure, unexpected unbounded growth, missed required
path, or recovery mismatch as a failed result regardless of throughput.
