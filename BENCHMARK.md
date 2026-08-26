# cache-rs benchmark baseline

This file records the reproducible developer baseline. It is a comparison aid,
not an NVMe claim; device-qualified profiles belong to M2.

The current harness gives its fixed index extra headroom for the complete L2
working set and retries rejected writes. A write yields for up to eight brief
admission conflicts before using a short delay for sustained pressure; the
reported admission totals expose both retry attempts and affected writes.
Every measured read retries pressure misses for at most one second with a short
delay. The L1 phases also retry a valid L2 fallback caused by best-effort
shard-lock contention; that extra work remains inside the measured latency.
Rerun the baseline before treating historical thresholds as release gates.

## Best-effort request-path baseline — 2026-08-26

Five consecutive release runs used an Apple M4 Max with 16 CPU cores and
64 GB RAM, macOS 26.5.2, and `rustc 1.98.0 (88d9e12ae 2026-08-18)`.
The then-default automatic/buffered workload used 8,192 entries × 16 KiB, 4
write shards, 4 I/O workers, and 8 read clients. Medians were:

| Phase | Median latency | Median throughput |
|---|---:|---:|
| put + drain | 30.107 ms | 272,098 ops/s |
| resident L1 get | 68.949 ms | 15,207,994 ops/s |
| warm close | 73.027 ms | — |
| L2 get + promote | 22.877 ms | 358,084 ops/s |
| promoted L1 get | 86.159 ms | 12,170,314 ops/s |

The median admission sample made 8,227 attempts for 8,192 accepted writes:
35 retries across 7 throttled writes. Internal maps consume the already
computed seeded XXH3 key hash directly, power-of-two request routing uses a
mask, and completed Region writes publish to the bounded index partition
without reacquiring the global Region manager. Recovery retains only physical
Region/FIFO state; stale index locations remain safe misses because every L2
record is validated locally.

To separate code effects from run-to-run machine drift, five old-revision runs
at `7eadb93` were collected immediately after the five current runs. Medians
from that same-session A/B were:

| Phase | `7eadb93` | Current | Latency change |
|---|---:|---:|---:|
| put + drain | 34.027 ms | 30.107 ms | -11.5% |
| resident L1 get | 76.787 ms | 68.949 ms | -10.2% |
| warm close | 73.557 ms | 73.027 ms | -0.7% |
| L2 get + promote | 23.434 ms | 22.877 ms | -2.4% |
| promoted L1 get | 88.003 ms | 86.159 ms | -2.1% |

The old control needed a median 767 retries across 177 throttled writes. This
A/B is the relevant regression check; absolute hot-read latency varies enough
between sessions that historical numbers should not be compared in isolation.

## Fixed L1 metadata A/B — 2026-08-26

The fixed-capacity L1 directory, slot arrays, policy metadata, and mixed-size
entry plan were compared against `fdc3f83` in the same session with Rust
1.98.0. Each side used five default release runs with statistics disabled. The
absolute L2 latency was higher than the earlier session above on both revisions,
so this table compares only the paired revisions:

| Phase | `fdc3f83` | Fixed metadata | Latency change |
|---|---:|---:|---:|
| put + drain | 28.436 ms | 29.074 ms | +2.2% |
| resident L1 get | 60.352 ms | 55.928 ms | -7.3% |
| warm close | 82.950 ms | 70.936 ms | -14.5% |
| L2 get + promote | 40.664 ms | 41.086 ms | +1.0% |
| promoted L1 get | 65.493 ms | 60.584 ms | -7.5% |

The two foreground regressions remain below the 10% investigation threshold;
both hot-L1 phases improved. A three-second mixed-size smoke soak additionally
completed 208,697 accepted writes and 441 Region rotations with zero malformed,
wrong-key, or future reads. It observed 156 valid stale hits, which are accepted
by the cache contract.

## M1 local baseline — 2026-08-24

Environment:

- Apple M4 Max, 16 CPU cores, 64 GB RAM;
- macOS 26.5.2, APFS temporary directory;
- `rustc 1.96.0-nightly (55e86c996 2026-04-02)`;
- POSIX engine and Auto I/O mode, which use buffered I/O on this platform;
- 8,192 entries × 16 KiB, 4 shards, 4 I/O workers, 8 read clients;
- 1,048,576 L1 read operations per measured hot-read phase.

Five consecutive release runs were collected with:

```sh
for run in 1 2 3 4 5; do
  cargo bench --bench hybrid_cache --quiet
done
```

The median result is the baseline:

| Phase | Median latency | Median throughput |
|---|---:|---:|
| put + drain | 31.927 ms | 256,585 ops/s |
| resident L1 get | 323.513 ms | 3,241,215 ops/s |
| warm close | 71.335 ms | — |
| L2 get + promote | 28.802 ms | 284,425 ops/s |
| promoted L1 get | 308.153 ms | 3,402,779 ops/s |

## M1 L1 shard scaling — 2026-08-24

An Ubuntu arm64 OrbStack VM ran the historical comparison entirely from
`/dev/shm` with 16 virtual CPUs and `rustc 1.94.1`. Both implementations used
16,384 eight-byte keys, 256-byte values, a 32 MiB L1, XXH3, four static
append/index shards, 4,194,304 reads per sample, and three-run medians. This
predates the current CLOCK-only API and Rust 1.98.0 qualification contract.
Reader-thread and runtime L1-shard counts varied while persistent topology
stayed fixed; every returned value's embedded key ordinal was verified.

With 64 L1 shards, cache-rs scaled through four reader threads before the VM's
shared-cache and scheduling costs dominated:

| Reader threads | cache-rs | Foyer 0.22.3 |
|---:|---:|---:|
| 1 | 16.79 Mops/s | 9.12 Mops/s |
| 2 | 23.90 Mops/s | 5.28 Mops/s |
| 4 | 30.70 Mops/s | 6.66 Mops/s |
| 8 | 27.56 Mops/s | 5.46 Mops/s |
| 16 | 25.41 Mops/s | 4.56 Mops/s |

At 16 reader threads, increasing only L1 shards exposed the lock-contention
curve while leaving the persistent topology unchanged:

| L1 shards | cache-rs | Foyer 0.22.3 |
|---:|---:|---:|
| 1 | 2.09 Mops/s | 0.48 Mops/s |
| 2 | 4.05 Mops/s | 1.16 Mops/s |
| 4 | 6.15 Mops/s | 3.93 Mops/s |
| 8 | 9.01 Mops/s | 4.63 Mops/s |
| 16 | 13.63 Mops/s | 4.21 Mops/s |
| 32 | 18.14 Mops/s | 4.57 Mops/s |
| 64 | 25.41 Mops/s | 4.56 Mops/s |

This measurement introduced runtime-only `memory_shards` with a default of 32
and cache-line-isolated shard locks. The benchmark source is kept outside the
crate so it cannot become an accidental release test or add Foyer to production
dependencies.

## M3 observability cost — 2026-08-24

After adding `HybridCache::snapshot()`, the same five-run procedure was repeated
with data-path counters disabled (the default) and with
`CACHE_BENCH_STATS=true`. Latencies are medians:

| Phase | M1 baseline | Counters off | Counters on |
|---|---:|---:|---:|
| put + drain | 31.927 ms | 32.410 ms | 33.614 ms |
| resident L1 get | 323.513 ms | 328.393 ms | 391.830 ms |
| warm close | 71.335 ms | 81.589 ms | 70.830 ms |
| L2 get + promote | 28.802 ms | 29.648 ms | 29.590 ms |
| promoted L1 get | 308.153 ms | 324.075 ms | 373.604 ms |

The default path remains inside the existing 10% data-path and 15% warm-close
investigation thresholds. Exact per-operation counters cost roughly 16–19% on
the contended hot-L1 phases on this host, so they are an explicit runtime
choice. Health and resource bounds remain available with counters disabled.

## M2 bounded-generator preflight — 2026-08-24

The qualification-capable harness generates fixed-size keys on demand, embeds
the key ordinal in every value, and separates the L1 resident subset from the
complete L2 data set. This removes per-key generator allocation, turns a wrong
key/value association into a hard failure, and permits device runs whose L2
data set exceeds host RAM. Five default local runs with counters off produced:

| Phase | Median latency | Median throughput |
|---|---:|---:|
| put + drain | 32.127 ms | 254,991 ops/s |
| resident L1 get | 351.165 ms | 2,985,995 ops/s |
| warm close | 73.477 ms | — |
| L2 get + promote | 26.791 ms | 305,774 ops/s |
| promoted L1 get | 323.232 ms | 3,244,036 ops/s |

The resident phase now verifies the embedded ordinal on every hit. Relative to
the previous counters-off run, this deliberate correctness work adds 6.9% to
resident-L1 latency; all other phases improved or stayed effectively flat. The
current harness remains inside the 10% data-path and 15% warm-close local
investigation thresholds.

The harness now also reads and verifies the bounded resident subset once before
timing it. This removes an invalid assumption that CLOCK admission always
retains the final written keys under every completion schedule. Historical
resident-L1 numbers above predate this preparation step and need a fresh
baseline before direct comparison; the other phase definitions are unchanged.

## M2 OrbStack Linux I/O preflight — 2026-08-24

An Ubuntu arm64 OrbStack VM ran from a guest-RAM-only setup: source, Cargo home,
build target, and reports were placed in `/dev/shm`; cache files used a 640 MiB
loop/ext4 filesystem whose backing image was also in `/dev/shm`. Guest swap
remained at zero. This avoids sustained host-SSD writes and is functional I/O
evidence, not an NVMe performance result.

Environment: Linux `7.0.14-orbstack-00380`, 16 virtual CPUs, 15 GiB RAM,
rustc `1.94.1`, one benchmark repetition, 512 × 16 KiB entries, and a 64-entry
resident subset. Default and no-default test profiles each passed 170 unit and
15 integration tests. The run exposed and fixed an arm64 bug caused by a
hard-coded x86 `O_NOFOLLOW` value; cache opens now use the target libc constants.

| I/O path | put + drain | L2 + promote | warm close |
|---|---:|---:|---:|
| POSIX / buffered | 67,196 ops/s | 75,658 ops/s | 2.726 ms |
| POSIX / direct | 63,603 ops/s | 60,766 ops/s | 2.492 ms |
| io_uring / direct | 64,855 ops/s | 62,016 ops/s | 2.119 ms |

An optional 15-second io_uring/direct turnover smoke completed 1,156,081
writes and 572 Region rotations with zero stale reads or errors. Managed memory
peaked at 110,659,840 of 335,544,320 bytes; logical disk use was 268,447,744 of
271,183,872 bytes. A separate live `/proc/<pid>/fdinfo` sample observed four
active io_uring descriptors and direct cache descriptors with the target
`O_DIRECT` bit set. The checksummed qualification result correctly reported
`preflight_pass`, because the data set was memory-sized, the device was a RAM
loop rather than NVMe, the source tree was dirty, and the soak was shortened.

## 100M-key recovery/shutdown scale — 2026-08-24

The fixed index was configured for 100,000,000 expected keys, which produces
125,000,000 slots and a 4,063,776,768-byte recovery image. The workload writes
1,024 sentinel values and verifies them after each reopen. This isolates the
scale-sensitive path accurately: warm close encodes every configured index
slot, while warm recovery maps the complete image lazily and does not scan the
number of occupied keys.

The Ubuntu arm64 OrbStack VM used `/tmp` tmpfs for cache files and `/dev/shm`
for source, Cargo state, binaries, and reports. The clean monitored run sampled
resources every 200 ms, aborted below 2 GiB available RAM or on any guest swap,
and completed with 7,126,048,768 bytes minimum available RAM and zero guest
swap. Host swap usage did not increase. The temporary files were removed after
each run.

| Phase | Clean monitored run | Corroborating run |
|---|---:|---:|
| fresh open | 19.828 ms | 18.398 ms |
| initial `close_warm` | 1.411 s | 1.539 s |
| first warm open | 0.534 ms | 0.512 ms |
| recovered `close_warm` | 1.761 s | 1.974 s |
| second warm open | 0.523 ms | 0.573 ms |
| `close_fast` | 1.971 ms | 2.382 ms |

Initial warm-image publication sustained 2.64–2.88 GB/s; publication from a
recovered private mapping sustained 2.06–2.31 GB/s. Stable logical file use was
4,332,224,512 bytes. Atomic replacement reached the exact configured
8,396,001,280-byte bound plus the pre-existing 4 KiB tmpfs directory charge.
Managed cache memory was 4,119,664,896 of the configured 6,442,450,944 bytes.
Both warm reopens returned `StartupMode::Warm`, and every sentinel matched.

The checked-in scale benchmark has a smaller safe default. Reproduce the 100M
configuration only on a filesystem with at least the reported peak capacity:

```sh
CACHE_RECOVERY_DIR=/path/to/ram-backed-or-test-filesystem \
CACHE_RECOVERY_EXPECTED_ENTRIES=100000000 \
CACHE_RECOVERY_MEMORY_LIMIT_MIB=6144 \
cargo bench --bench recovery_scale
```

These numbers are a RAM-backed control-path ceiling, not an NVMe shutdown
claim. The M2 device qualification must repeat the shutdown measurement on the
target NVMe without risking the developer host.

## Comparison procedure

1. Use the same machine, power mode, toolchain, environment variables, and an
   otherwise idle host.
2. Run the command above five times for the baseline revision and candidate
   revision; discard neither cold runs nor outliers.
3. Compare medians by phase. Investigate a data-path latency increase above 10%
   or a warm-close increase above 15%.
4. Treat checksums as anti-optimization guards, not cross-run identities:
   shard rotation may allocate sequence numbers in a different order.
5. Record workload or hardware changes as a new named baseline instead of
   comparing unlike configurations.

Linux direct-I/O and io_uring results must identify the device, filesystem,
mount options, kernel, CPU, queue topology, dataset size, and whether the
dataset exceeds available RAM.

Every benchmark phase emits a stable `result phase=... key=value` line in
addition to the human-readable table. The following optional environment
variables turn an individual run into a fail-fast regression gate:

- `CACHE_BENCH_MIN_PUT_OPS`;
- `CACHE_BENCH_MIN_RESIDENT_L1_OPS`;
- `CACHE_BENCH_MIN_L2_OPS`;
- `CACHE_BENCH_MIN_PROMOTED_L1_OPS`;
- `CACHE_BENCH_MAX_WARM_CLOSE_MS`.

Thresholds must be finite non-negative numbers. When present, a missed gate
exits non-zero after correctness and cleanup have completed.

The checked-in qualification runner captures the complete Linux NVMe evidence
set in a new caller-owned report directory:

```sh
./scripts/qualify-linux-nvme.sh \
  /mnt/nvme \
  /var/tmp/cache-rs-qualification-2026-08-24
```

It requires Linux, a writable cache directory, a new report path under an
existing directory outside the source tree, and a clean worktree. It records
the exact revision, Rust 1.98.0 toolchain, kernel, CPU, block
device, filesystem and mount information; runs five repetitions of POSIX
buffered and direct I/O; measures POSIX direct I/O with 1/2/4/8/16 workers;
calculates phase medians; then runs a four-hour POSIX/direct turnover soak. For
an M2 result, the benchmark data set must
exceed physical RAM and the mount must resolve to a non-rotational NVMe block
device. `qualification.status` is written with `status=m2_pass` only after
those preconditions, all five performance gates being configured, every
command, and the final zero-error soak record succeed. `SHA256SUMS` binds the
retained evidence.

`CACHE_QUAL_BENCH_RUNS`, `CACHE_QUAL_SOAK_SECONDS`, and
`CACHE_QUAL_SAMPLE_SECONDS` tune preflight duration. The four-hour default is
the M2 sign-off value; shorter runs are preflight only. A dirty tree can be used
for preflight with `CACHE_QUAL_ALLOW_DIRTY=1`, but is not release evidence.
`CACHE_QUAL_ALLOW_MEMORY_SIZED_DATASET=1` and
`CACHE_QUAL_ALLOW_UNVERIFIED_DEVICE=1` likewise permit development runs, but
force `status=preflight_pass`. Set `CACHE_BENCH_ENTRIES` and
`CACHE_BENCH_CAPACITY_MIB` so the data set exceeds host RAM while remaining no
larger than half the Region capacity.
Git-less development VMs may additionally provide a hexadecimal
`CACHE_QUAL_SOURCE_REVISION` with `CACHE_QUAL_ALLOW_DIRTY=1`; this is always
preflight evidence and never permits `m2_pass`.
The `CACHE_BENCH_*` regression gates above are inherited by every benchmark
profile. Agree and record thresholds before the release run; an ungated result
is measurement evidence, not M4 performance acceptance. The qualification
runner requires all five values to be positive and records their exact values
in `environment.txt`; missing gates downgrade the result to preflight.

## M2 worker-scaling preflight — 2026-08-24

The same macOS workload was run once per worker count as a topology preflight.
These single runs are not the device-qualified M2 result:

| I/O workers | put + drain | resident L1 | L2 + promote | promoted L1 |
|---:|---:|---:|---:|---:|
| 1 | 242,146 ops/s | 2,968,152 ops/s | 280,508 ops/s | 2,840,160 ops/s |
| 2 | 240,302 ops/s | 3,077,932 ops/s | 285,681 ops/s | 3,213,154 ops/s |
| 4 | 253,585 ops/s | 3,328,880 ops/s | 279,985 ops/s | 3,169,937 ops/s |
| 8 | 256,329 ops/s | 2,863,649 ops/s | 255,146 ops/s | 3,230,807 ops/s |
| 16 | 256,641 ops/s | 3,285,497 ops/s | 242,913 ops/s | 3,285,725 ops/s |

The buffered developer path plateaus around 4–8 workers and L2 promotion drops
after 4 workers. Linux NVMe measurement must determine whether this is host
scheduling/page-cache contention or a device-path limit.

## Turnover soak

The soak workload runs four writers and four readers by default while
continuously overwriting a key ring larger than L1. Every 64th announced write
also attempts a delete, and values cycle through 256 B, 4 KiB, 16 KiB, and
256 KiB sizes. Periodic samples drain accepted batches, validate the embedded
version, key, length, and payload of every hit, and report an older valid
version as `stale_hits` because freshness is best effort. A future version,
wrong key, or malformed record is fatal. Samples
cover managed-memory current/peak/budget, L1 and index pressure, current and
peak RSS, logical disk use, and latency. The Linux run fails when current RSS
exceeds the managed-memory limit plus a fixed slack, or when a cache-owned bound
is exceeded. Every periodic `errors=0` sample means no earlier error was
ignored.

Run a four-hour device soak with:

```sh
CACHE_SOAK_SECONDS=14400 \
CACHE_SOAK_SAMPLE_SECONDS=10 \
CACHE_SOAK_DIR=/mnt/nvme \
CACHE_SOAK_IO_ENGINE=posix \
CACHE_SOAK_IO_MODE=direct \
cargo +1.98.0 bench --locked --bench hybrid_cache_soak
```

`CACHE_SOAK_CAPACITY_MIB`, `CACHE_SOAK_MEMORY_MIB`,
`CACHE_SOAK_MEMORY_LIMIT_MIB`, comma-separated `CACHE_SOAK_VALUE_BYTES`,
`CACHE_SOAK_KEYS`, `CACHE_SOAK_SHARDS`,
`CACHE_SOAK_RSS_SLACK_MIB`, `CACHE_SOAK_IO_WORKERS`,
`CACHE_SOAK_READ_IO_RESERVE`, `CACHE_SOAK_WRITERS`, and `CACHE_SOAK_READERS`
control the workload. The read reserve must be smaller than the POSIX worker
count and defaults to one, except for a single-worker pool where it is zero.
`CACHE_SOAK_WARM_REOPEN=true`
populates one complete pass at every configured value size, publishes a clean
image, reopens it, and runs the measured turnover against the recovered private
index mapping.
`CACHE_SOAK_IO_ENGINE` and
`CACHE_SOAK_IO_MODE` select the device path. M2 evidence must retain the
complete output and confirm zero errors, no future/wrong-key/malformed reads,
bounded managed memory and current RSS, and bounded logical disk use. Stale-hit
counts are workload evidence, not a correctness failure.

## M2 decoder and unsafe-memory preflight — 2026-08-24

The checked-in libFuzzer target sends arbitrary bytes through the persistent
data/state/image, Region header, record header, Region metadata, and index-slot
decoders, then drives bounded canonical index upserts and probes. The local
coverage-instrumented AddressSanitizer run completed 10,000 inputs without a
crash (`190` edges, `336` features):

```sh
cargo fuzz run persistent_decoders -- \
  -runs=10000 -max_len=16384 -print_final_stats=1
```

The mmap/index storage tests also completed under AddressSanitizer:

```sh
RUSTFLAGS='-Zsanitizer=address' \
cargo test --lib index_storage::tests:: \
  --target aarch64-apple-darwin -- --test-threads=1
```

CI repeats both checks on Linux nightly. Longer fuzz campaigns should retain
their corpus externally and use the same checked-in target.
