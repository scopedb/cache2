# C² performance validation

Performance results are meaningful only when the baseline and candidate use the
same host, toolchain, filesystem, device, configuration, and workload. Stored
historical numbers are intentionally omitted; keep raw results with the tested
revision instead.

## Request-path benchmark

```sh
cargo +1.98.0 bench --locked --bench cache
```

The benchmark covers bounded concurrent puts plus drain, warm close, L2 reads
with best-effort promotion, and resident L1 reads populated in a fresh RAM tier.
Values contain their key ordinal, so a wrong-key result is fatal. Writes retry
`WouldBlock` only in the harness; this is not library behavior.

The request-path workload uses 20% physical index load so every planned L2
operation remains a storage hit. Use `region_index_turnover` below for the
production 80% index load factor, bounded-probe cost, and replacement rate.

The main controls are:

- `CACHE_BENCH_ENTRIES`, `CACHE_BENCH_VALUE_BYTES`, and
  `CACHE_BENCH_RESIDENT_ENTRIES` for the data set;
- `CACHE_BENCH_CAPACITY_MIB`, `CACHE_BENCH_MEMORY_MIB`, and
  `CACHE_BENCH_MANAGED_MEMORY_LIMIT_MIB` for capacity;
- `CACHE_BENCH_APPEND_SHARDS`, `CACHE_BENCH_READ_IO_WORKERS`, and
  `CACHE_BENCH_WRITE_IO_WORKERS` for topology;
- `CACHE_BENCH_WRITE_CLIENTS` and `CACHE_BENCH_CLIENTS` for concurrency;
- `CACHE_BENCH_HOT_ENTRIES` enables the focused hot-set/cold-scan workload;
  `CACHE_BENCH_HOT_READ_INTERVAL` controls one hot read per N cold reads;
- `CACHE_BENCH_IO_ENGINE`, `CACHE_BENCH_IO_MODE`, `CACHE_BENCH_STATS`, and
  `CACHE_BENCH_DIR` for the execution path.

The optional hot-scan mode rewrites a bounded hot set into L1 after warm open,
then interleaves one-shot hot reads with a single pass over the remaining cold
keys. It reports L1/L2/miss counts before, during, and after the scan without
requiring best-effort admission to succeed. Its discarded initial fill disables
L1; the measured warm-open scan uses the configured L1 capacity. This avoids
retaining a second allocator copy of the complete RAM tier in one process.

Use a data set larger than host RAM for L2 device measurements. Run each
revision at least five times in alternating order, report medians, and retain
all samples. Investigate correctness failures, bound violations, and latency
regressions independently; one aggregate throughput number is insufficient.

Optional `CACHE_BENCH_MIN_*` and `CACHE_BENCH_MAX_WARM_CLOSE_MS` variables turn
the harness measurements into explicit regression gates.

## Device qualification

The checked-in runner captures the Linux NVMe matrix, machine identity,
configuration, raw output, checksums, and turnover soak:

```sh
./scripts/qualify-linux-nvme.sh \
  /mnt/nvme \
  /var/tmp/cache2-qualification
```

A release-quality run requires a clean worktree, exact Rust 1.98.0, a data set
larger than physical RAM, a non-rotational NVMe device, five configured
performance gates, and the full four-hour soak. Shorter or relaxed runs are
preflight evidence only; the script records that distinction.

## Turnover soak

```sh
CACHE_SOAK_SECONDS=14400 \
CACHE_SOAK_SAMPLE_SECONDS=10 \
CACHE_SOAK_DIR=/mnt/nvme \
CACHE_SOAK_IO_ENGINE=posix \
CACHE_SOAK_IO_MODE=buffered \
cargo +1.98.0 bench --locked --bench cache_soak
```

The soak continuously mixes writes, reads, deletes, Region rotation, and 256 B,
4 KiB, 16 KiB, and 256 KiB values. It checks key, version, length, payload,
managed memory, RSS, logical disk use, and latency. Valid older values count as
`stale_hits`; future, wrong-key, or malformed values fail the run.

Use `CACHE_SOAK_WARM_REOPEN=true` to publish and reopen a clean image before the
measured phase. `CACHE_SOAK_*` variables control capacity, managed memory, keys,
append-shard and worker counts, clients, value sizes, and RSS slack.

## Focused diagnostics

Exercise long-turnover index behavior without storage I/O:

```sh
cargo +1.98.0 bench --locked --features benchmarking \
  --bench region_index_turnover
```

Measure fresh/warm open and close costs at a configurable index scale:

```sh
cargo +1.98.0 bench --locked --bench recovery_scale
```

Fuzz persistent decoders and bounded index probes when `cargo-fuzz` is
available:

```sh
cargo fuzz run persistent_decoders -- \
  -runs=10000 -max_len=16384 -print_final_stats=1
```
