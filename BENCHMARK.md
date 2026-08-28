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

The request-path workload overprovisions the index so replacement is rare. It
reports attempted reads, L2 hits, misses, and the successful-hit rate; a legal
bounded-index eviction does not abort the run. The `l2_promote`/`l2_read`
throughput is successful L2 hits per elapsed second, so fast misses cannot
inflate the storage result. Use `region_index_turnover` below for the production
50% index load factor, bounded-candidate cost, and replacement rate.
For POSIX, the harness caps measured L2 concurrency at the configured read
worker count so execution-admission misses do not obscure the storage rate;
the turnover soak deliberately exercises overload behavior separately.

The main controls are:

- `CACHE_BENCH_ENTRIES`, `CACHE_BENCH_VALUE_BYTES`, and
  `CACHE_BENCH_RESIDENT_ENTRIES` for the data set;
- `CACHE_BENCH_CAPACITY_MIB`, `CACHE_BENCH_MEMORY_MIB`, and
  `CACHE_BENCH_MANAGED_MEMORY_LIMIT_MIB` for capacity;
- `CACHE_BENCH_APPEND_SHARDS`, `CACHE_BENCH_READ_IO_WORKERS`, and
  `CACHE_BENCH_WRITE_IO_WORKERS` for foreground topology;
- `CACHE_BENCH_RECLAIM_WORKERS` for concurrent Region reclaim;
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
larger than physical RAM, a non-rotational NVMe device, four configured
performance gates, and the full four-hour soak. The soak forces an initial warm
reopen, measured-path coverage, and a final all-key warm-recovery verification.
Shorter or relaxed runs are preflight evidence only; the script records that
distinction.

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

Reclaim samples also report reinsert rewrite records and bytes, skipped hot
records, and conditional replacement misses. A turnover run with L2 hits should
exercise reinsertion without allowing cumulative rewritten bytes to exceed the
bounded reclaim budget.
Set `CACHE_SOAK_REQUIRE_REINSERT_COVERAGE=true` for a focused read-heavy run;
the harness then requires both an accepted hot-record rewrite and a hot record
skipped specifically because the bounded byte budget was exhausted.

Value sizes are deterministically mixed on every write instead of running in
size phases. Repeat a size in `CACHE_SOAK_VALUE_BYTES` to give it proportionally
more weight in a production-shaped distribution.

Use `CACHE_SOAK_WARM_REOPEN=true` to publish and reopen a clean image before the
measured phase. `CACHE_SOAK_*` variables control capacity, managed memory, keys,
append-shard and worker counts, clients, value sizes, and RSS slack.
For same-process warm reopen, the RSS bound automatically allows one additional
L1 capacity for freed allocations retained by libc; configured RSS slack covers
the runtime, harness, and other non-cache-managed memory.

## Long correctness and stability run

Use a per-client interval to keep a long run below saturation. The following
configuration limits two writers to at most roughly 1,000 combined operations
per second and four readers to at most roughly 2,000 reads per second. Actual
rates are lower because operation time is not subtracted from the interval.

```sh
CACHE_SOAK_SECONDS=86400 \
CACHE_SOAK_SAMPLE_SECONDS=60 \
CACHE_SOAK_DIR=/mnt/nvme \
CACHE_SOAK_CAPACITY_MIB=65536 \
CACHE_SOAK_MEMORY_MIB=4096 \
CACHE_SOAK_MANAGED_MEMORY_LIMIT_MIB=6144 \
CACHE_SOAK_RSS_SLACK_MIB=256 \
CACHE_SOAK_KEYS=1048576 \
CACHE_SOAK_VALUE_BYTES=256,256,4096,4096,4096,4096,16384,16384,16384,16384,16384,16384,16384,16384,65536,262144 \
CACHE_SOAK_APPEND_SHARDS=4 \
CACHE_SOAK_RECLAIM_WORKERS=1 \
CACHE_SOAK_READ_IO_WORKERS=4 \
CACHE_SOAK_WRITE_IO_WORKERS=4 \
CACHE_SOAK_WRITERS=2 \
CACHE_SOAK_READERS=4 \
CACHE_SOAK_OPERATION_INTERVAL_US=2000 \
CACHE_SOAK_WARM_REOPEN=true \
CACHE_SOAK_FINAL_WARM_VERIFY=true \
CACHE_SOAK_REQUIRE_PATH_COVERAGE=true \
CACHE_SOAK_IO_ENGINE=posix \
CACHE_SOAK_IO_MODE=buffered \
cargo +1.98.0 bench --locked --bench cache_soak
```

The operation interval applies to every measured foreground client. Warm
prefill is intentionally unpaced and retries bounded `WouldBlock` results so a
large key space does not add hours before measurement. Intervals above one
second are rejected so shutdown remains responsive.

Periodic samples are passive and do not drain accepted writes, so backlog and
resource peaks remain visible across the complete measured phase. Path coverage
requires accepted writes and deletes, completed reads and L2 hits, Region
rotation and reclaim, plus a recovered L2 hit during final warm verification.
Failed runs preserve their data and recovery sidecars; successful runs remove
them.

Final warm verification publishes the churned state, reopens it, and scans the
complete key space sequentially. Every recovered hit receives the same key,
version, length, and payload validation as the measured readers. A successful
run emits `warm_verification ... errors=0` followed by
`complete ... errors=0 ... io_errors=0`. The configured measured duration does
not include the unpaced prefill or final verification scan; the final reported
elapsed time does include the verification work.

## Focused diagnostics

Exercise long-turnover index behavior without storage I/O:

```sh
cargo +1.98.0 bench --locked --features benchmarking \
  --bench region_index_turnover
```

The default models one production-sized partition from a 4 TiB cache averaging
16 KiB entries: 65,536 logical live entries in 131,072 slots. This preserves
the 50% load and candidate shape without allocating the complete 4.06 GiB
index or its 128 MiB volatile heat bitmaps.

Measure fresh/warm open and close costs at a configurable index scale:

```sh
cargo +1.98.0 bench --locked --bench recovery_scale
```

The harness reports current and peak process RSS after every phase. This shape
uses a 512 GiB data file while exercising the exact 536,870,912-slot index for
a 4 TiB cache averaging 16 KiB; it is an index/recovery-scale test, not a
physical 4 TiB data test:

```sh
CACHE_RECOVERY_EXPECTED_ENTRIES=268435456 \
CACHE_RECOVERY_CAPACITY_MIB=524288 \
CACHE_RECOVERY_MEMORY_MIB=64 \
CACHE_RECOVERY_MANAGED_MEMORY_LIMIT_MIB=8192 \
CACHE_RECOVERY_SENTINELS=65536 \
CACHE_RECOVERY_VALUE_BYTES=16384 \
CACHE_RECOVERY_DIR=/mnt/nvme \
cargo +1.98.0 bench --locked --bench recovery_scale
```

Fuzz persistent decoders and bounded index probes when `cargo-fuzz` is
available:

```sh
cargo fuzz run persistent_decoders -- \
  -runs=10000 -max_len=16384 -print_final_stats=1
```
