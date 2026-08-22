# NVMe staging benchmark

`cache-bench` is the reproducible acceptance harness for the v1.1 single-device
large-capacity, high-entry-count data path and M7 policy A/B tests.
It uses a dedicated cache file, destructively creates a fresh Format V1 layout,
optionally preloads a fixed fraction of the key universe under `Always/FIFO`,
reopens with the selected policy, runs a warm-up, drains it with `close`, then
reopens for an isolated measurement window. A configurable
hot/cold distribution supplies real cold-key churn instead of updating only
pre-existing keys. `--api sync` makes every client thread call `DiskCache`
directly; `--api async` creates and reuses one `AsyncDiskCache` handle per
phase and waits each request to completion. Both modes exercise append-lane
routing and the configured `IoEngine` queue depth.

M5 implementation and behavior testing are complete, as are the M7 policy
switches and counters. Target-NVMe throughput/p99/profile, target-workload
hit-rate, device DWPD, TB-scale recovery SLA, and M8 soak/canary sign-offs are
intentionally still pending; a developer machine or source-tree test cannot
substitute for those deployment artifacts.

Every invocation is destructive for an existing recognized Format V1 cache:
the harness holds the cache lock, validates the format, resets it, and starts
from fresh bytes. It never treats an unrecognized non-empty file as disposable.
Use a regular-file path dedicated to cache-rs and place it on the exact NVMe
filesystem under test. Formatting establishes the configured whole-Region
extent and, on 64-bit Linux, requests physical allocation with
`posix_fallocate`, so verify free space before starting. Do not point `--path`
at a directory, symlink, shared application file, or raw block device.

## Build

```text
cargo +1.85.0 build --release --bin cache-bench
```

Linux direct I/O requires a filesystem and kernel that support `O_DIRECT`.
`--mode direct` requires that capability: opening fails when it is unavailable,
and an aligned direct-request error is returned instead of silently retrying
that request as buffered. `--mode auto`
may disable direct I/O only when the system reports the capability unavailable.
In both modes, metadata/recovery, legacy unaligned Format V1 records, and an
unaligned remainder after a positive short completion deliberately use the
buffered compatibility descriptor. Every actual direct submission has a 4 KiB
aligned buffer address, offset, and length. Consequently, a valid `direct` run
can report some buffered operations; required direct means capability plus
strict aligned-request error semantics, not “every byte uses `O_DIRECT`.”

## Acceptance command

Choose throughput, p99, and (for workloads containing reads) hit-rate limits
before starting the run. The harness exits with code 2 if a supplied threshold
is missed or if it observes operational errors, non-policy rejections, a
memory-budget violation, or a required-direct run with no actual direct
operation. Admission-filter rejections are expected policy outcomes and remain
explicit in the report. `--require-policy-activity` additionally requires
Region reuse for every row. A selected SecondHit policy must also reject at
least one admission; a selected SecondChance policy must also queue and
complete at least one reinsertion.

```text
target/release/cache-bench \
  --path /mnt/nvme/cache-rs.bench \
  --capacity 64GiB \
  --region-size 32MiB \
  --object-size 4KiB \
  --keys 1000000 \
  --read-percent 80 \
  --api async \
  --concurrency 32 \
  --queue-depth 128 \
  --append-lanes 2 \
  --admission always \
  --reclaim fifo \
  --engine io-uring \
  --mode direct \
  --memory-budget 4GiB \
  --warmup-secs 30 \
  --duration-secs 300 \
  --min-ops-per-sec <agreed-throughput> \
  --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> \
  --device-stat /sys/block/nvme0n1/stat \
  --output json
```

Omit `--min-hit-percent` for the 0/100 row, which has no lookups and therefore
reports a zero hit percentage. The harness defaults to the synchronous API and
two append lanes. Always pass both `--api` and `--append-lanes` explicitly in
recorded acceptance commands.
Always pass `--admission` and `--reclaim` as well; their conservative defaults
are `always` and `fifo`, but an implicit default makes an A/B artifact
ambiguous.

Do not reuse a threshold from another device class. Record the exact SSD,
filesystem, mount options, kernel, CPU governor, NUMA placement, public API,
engine, I/O mode, queue depth, lane count, cache capacity, index slots, and
workload beside each result.

## Required matrix

Run both `sync` and `async` public APIs and both `buffered` and `direct` I/O
modes for every row. Run queue depths 1, 8, 32, 64, and 128 for the 4 KiB rows,
then retain the lowest depth that reaches the agreed throughput without
worsening p99. Compare append lanes 1, 2, 4, and 8 for write mixes.
Queued puts on each lane are coalesced into at most 64 records and 128 KiB per
multi-record write; a single larger record remains a valid one-record batch.

Each measured row must reach steady-state reuse rather than stop after filling
an empty cache. Run until measurement-window admitted foreground record bytes
are at least `2 * capacity`; if policy admission makes that impractical, use
device/cache byte counters and Region-reuse activity to document an equivalent
two full-capacity churn interval. Increase `--duration-secs` rather than
counting prefill or warm-up bytes toward this requirement.

| Object | Read/write | Concurrency | Purpose |
| --- | --- | --- | --- |
| 4 KiB | 100/0 | 1, 32 | hit latency and random-read scaling |
| 4 KiB | 80/20 | 32 | primary staging workload |
| 4 KiB | 0/100 | 32 | batching, lane scaling, device write cost |
| 64 KiB | 50/50 | 16 | medium-object copy and bandwidth cost |
| 1 MiB | 90/10 | 8 | large-object bandwidth and memory pressure |

For format compatibility, also write and flush in each mode, reopen in the
other mode, and validate the preloaded keys before timing.

## Large-scale acceptance

In addition to the throughput matrix, run a capacity/entry-count row with the
largest production configuration intended for staging. With the default
32 MiB Region, Format V1 permits a single-cache layout just below 64 TiB; the
v1.1 scale qualification must cover at least 100 million live entries. Size
`--index-slots` for the declared maximum load factor, reserve the corresponding
memory budget, and record both values. A run is acceptable only when:

- open, prefill, steady-state operation, final checkpoint, close, and reopen
  complete without panic, OOM, or unbounded queue growth;
- the measurement churns at least twice the configured capacity and observes
  Region reuse while preserving value correctness;
- throughput scales across the selected API, queue-depth, concurrency, and
  lane matrix without violating the predeclared p99 target;
- `memory_used_bytes` and `memory_peak_bytes` stay within the configured budget,
  and no non-policy rejection or I/O error is reported;
- reopen preserves the sampled live entries and never returns an incorrect or
  superseded value.

Run smaller representative capacity rows when the intended production-size
device is unavailable, but label them as scale-model evidence rather than a
production-capacity staging sign-off.

## M7 policy A/B

Compare all four policy combinations on the same host, device, filesystem,
cache size, seed, workload shape, and prefilled fraction. The key universe must
be larger than effective cache capacity so Region reuse actually occurs. Each
command resets its dedicated recognized Format V1 cache before prefill; an
unrelated non-empty file is rejected without modification.
The harness establishes and verifies the same partial prefill under
`Always/FIFO`, closes it, and only then opens the measured policy. It performs
no verification reads on that measured instance. Warm-up is also closed and
drained before a fresh measurement reopen, so both measurement counters and
bounded policy state have a precise boundary. With `--prefill-percent 10`,
`--hotset-percent 10`, and `--hot-access-percent 90`, the complete hot set
starts resident while the cold
90% supplies new-key admission pressure. A 100% prefill is not a valid
admission A/B because all writes become existing-key updates.

The following commands use separate files to make cross-row contamination
obvious. Ensure the filesystem has room for all files, or run one row at a time
and safely reset the dedicated file. Replace the three threshold placeholders
with values declared before the first run.

```text
target/release/cache-bench --path /mnt/nvme/cache-rs.always-fifo \
  --capacity 8GiB --region-size 32MiB --object-size 4KiB --keys 4000000 \
  --prefill-percent 10 --hotset-percent 10 --hot-access-percent 90 \
  --read-percent 80 --concurrency 32 --queue-depth 128 --append-lanes 2 \
  --engine io-uring --mode direct --memory-budget 4GiB \
  --admission always --reclaim fifo --warmup-secs 30 --duration-secs 300 \
  --require-policy-activity \
  --min-ops-per-sec <agreed-throughput> --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> --device-stat /sys/block/nvme0n1/stat \
  --output json

target/release/cache-bench --path /mnt/nvme/cache-rs.second-hit-fifo \
  --capacity 8GiB --region-size 32MiB --object-size 4KiB --keys 4000000 \
  --prefill-percent 10 --hotset-percent 10 --hot-access-percent 90 \
  --read-percent 80 --concurrency 32 --queue-depth 128 --append-lanes 2 \
  --engine io-uring --mode direct --memory-budget 4GiB \
  --admission second-hit --reclaim fifo --warmup-secs 30 --duration-secs 300 \
  --require-policy-activity \
  --min-ops-per-sec <agreed-throughput> --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> --device-stat /sys/block/nvme0n1/stat \
  --output json

target/release/cache-bench --path /mnt/nvme/cache-rs.always-second-chance \
  --capacity 8GiB --region-size 32MiB --object-size 4KiB --keys 4000000 \
  --prefill-percent 10 --hotset-percent 10 --hot-access-percent 90 \
  --read-percent 80 --concurrency 32 --queue-depth 128 --append-lanes 2 \
  --engine io-uring --mode direct --memory-budget 4GiB \
  --admission always --reclaim second-chance --warmup-secs 30 --duration-secs 300 \
  --require-policy-activity \
  --min-ops-per-sec <agreed-throughput> --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> --device-stat /sys/block/nvme0n1/stat \
  --output json

target/release/cache-bench --path /mnt/nvme/cache-rs.second-hit-second-chance \
  --capacity 8GiB --region-size 32MiB --object-size 4KiB --keys 4000000 \
  --prefill-percent 10 --hotset-percent 10 --hot-access-percent 90 \
  --read-percent 80 --concurrency 32 --queue-depth 128 --append-lanes 2 \
  --engine io-uring --mode direct --memory-budget 4GiB \
  --admission second-hit --reclaim second-chance --warmup-secs 30 --duration-secs 300 \
  --require-policy-activity \
  --min-ops-per-sec <agreed-throughput> --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> --device-stat /sys/block/nvme0n1/stat \
  --output json
```

The JSON records the selected public API, workload shape, both policy names, the policy-activity
gate, admission observations and rejections, Region reuse, reinsertion
queued/completed counts, reclaim/backlog counters, Region validity,
host-write classes, and measurement-window `write_amplification_milli`.
The final measurement `close` drains background workers before cache and block
device snapshots, so host/device bytes and write amplification share the same
window. `daily_host_write_bytes_current` and
`daily_write_budget_rejections_current` are current UTC-day gauges rather than
deltas; `daily_utc_day` and `daily_window_crossed` make a midnight rollover
visible. `daily_host_write_bytes` is actual submitted I/O;
`daily_budget_used_bytes`/`reserved_bytes` are admission state. Retain the
OpenMetrics and policy snapshot with the JSON, because the submission gauge
alone cannot prove capacity or reservation reconciliation.
Compare those with hit percentage, operations/s,
p99, cache/device bytes, and SMART `data_units_written` deltas. Retain a matching
OpenMetrics snapshot as a companion artifact.
Run long enough to reach steady-state Region reuse: an empty-cache warm-up
cannot establish second-chance value or SSD wear. Measurement-window churn must
also satisfy the two-capacity rule above. The M7 gate is workload
specific: hit rate must not regress against `always/fifo`, and daily submitted
host bytes must remain inside the budget derived from the actual SSD capacity,
warranty period, and DWPD rating.

## Hybrid mixed-object acceptance

The Region-only matrix above isolates the append/index/reclaim path. It does
not qualify the production composition where DRAM, fixed Bucket pages, and
RegionLog share admission, host-write accounting, and device queue capacity.
Run the Hybrid mode separately with three new empty files:

```text
target/release/cache-bench hybrid \
  --bucket-path /mnt/nvme/cache-rs.hybrid-small \
  --bucket-capacity 64GiB \
  --region-path /mnt/nvme/cache-rs.hybrid-large \
  --region-capacity 512GiB \
  --manifest-path /mnt/nvme/cache-rs.hybrid-manifest \
  --memory-capacity 16GiB \
  --bucket-memory-budget 2GiB \
  --region-memory-budget 8GiB \
  --hybrid-memory-budget 32GiB \
  --generator-memory-budget 2GiB \
  --sizes 256:50,4KiB:30,64KiB:20 \
  --small-object-max 1KiB \
  --keys 100000000 --prefill-percent 80 --prefill-concurrency 64 \
  --verify-samples 100000 --read-percent 80 \
  --remove-percent 10 --ttl-percent 5 --cross-tier-percent 20 --ttl-ms 250 \
  --api async --concurrency 64 --queue-depth 128 --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --journal-capacity 64MiB \
  --engine uring --mode direct --warmup-secs 60 \
  --steady-state-fill-turnovers 2 --steady-state-fill-max-secs 3600 \
  --duration-secs 600 \
  --min-ops-per-sec <agreed-throughput> \
  --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> \
  --min-journal-rollovers 1 \
  --min-capacity-turnovers 2 \
  --min-disk-qd-peak 8 \
  --min-write-back-qd-peak 8 \
  --max-journal-rollover-ms <agreed-rollover-ms> \
  --max-close-ms <agreed-close-ms> \
  --yes --output json
```

`--sizes` accepts at most 16 `SIZE:WEIGHT` classes and caps each object at
64 MiB. The generator derives fixed-size keys on demand instead of retaining a
key vector. Its bounded 8-byte-per-key state records the latest version, route
class, presence, and TTL deadline; scratch values and explicit 512 KiB worker
stacks are included in `--generator-memory-budget`. Key count (up to 100
million), command input, concurrency, queue depth, worker counts, durations,
and every engine configuration retain hard validation bounds. Prefill uses at
most `--prefill-concurrency` workers and verification samples do not scale with
the key count. The command refuses all non-empty targets, including valid old
caches; this prevents benchmark automation from erasing a warm production
Hybrid cache.

At minimum run these distributions under both synchronous and asynchronous
public APIs, write-through/write-back policies, buffered/direct modes, and
queue depths 1/8/32/64/128:

| Size weights | Read/write | Purpose |
| --- | --- | --- |
| `256:80,4KiB:15,64KiB:5` | 95/5 | L1 and Bucket lookup latency, high entry count |
| `256:50,4KiB:30,64KiB:20` | 80/20 | primary mixed workload and cross-tier promotion |
| `256:40,4KiB:40,1MiB:20` | 50/50 | Bucket RMW plus Region bandwidth/write amplification |
| `4KiB:20,64KiB:50,1MiB:30` | 0/100 | lower-tier queue saturation and shutdown drain |

Add a tight-quota Bucket TTL race: cancellation before cleanup commit must
produce no page rewrite/refund; cancellation after commit must return
`TooLate`, followed by the real miss, one complete-page host write, and the
exact namespace refund. Also hold the daily budget exhausted to prove logical
expiry does not release physical quota early or corrupt the Bloom fast-miss.

Retain tier hit counts, combined host-write bytes, Bucket/Region physical I/O,
request rejections, queue-depth peaks, journal rollover count/max latency,
capacity turnovers, final drain time, process/device/SMART profiles, and an
offline `cachectl hybrid-verify` report. The gate fails on a stale per-key
version, a missing Bucket or Region I/O submission, an insufficient QD peak,
or (in write-back mode) no completed demotion. The configured rollover,
rollover-latency, and close-latency thresholds are also enforced;
`--steady-state-fill-turnovers` runs the same workload before measurement until
host writes since the pre-measure baseline reach the requested combined disk
capacity multiple and the Region reuse count reaches the configured Region
count, proving one complete reuse cycle. The fill has a separate bounded
deadline and its latency samples are excluded from reported throughput and
percentiles. `--min-capacity-turnovers` uses only host bytes submitted before
the final close begins; drain host bytes, synchronous demotions, and close time
are reported separately, so neither prefill nor lifecycle work can satisfy the
measurement gate. This is a wall-clock completion boundary: up to the bounded
set of background tasks still in flight at the snapshot can finish in the drain
window. A
run that never reaches the Bucket tier, Region tier, and steady-state
eviction/reclaim is not a mixed Hybrid qualification even if its aggregate
throughput is high.
Retain namespace live/reserved usage, expiry-compaction page writes, exact
removal refunds, and `Requested`/`TooLate` cancellation counts with the same
run evidence.

For recovery qualification, run `SIGKILL/reopen` at several admitted-write
distances since the last explicit `HybridCache::flush()`, including the maximum
declared cadence. Record first-service/full-recovery latency and RSS peak with
`journal_recovery_memory_bytes` and
`region_checkpoint_accounting_bytes`. A journal rollover is not a full Region
compact-index checkpoint; the benchmark's final `close` is. The matrix must
therefore exercise both rollover-only dirty tails and explicit-flush clean
starts instead of treating rollover count as recovery coverage.
Add crash points at the first managed-manifest dirty slot, the pre-lower open
fence, each lower open/recovery boundary, and after an autonomous owner-dirty
mutation. Dirty plus an empty journal must reopen as a safe-cleared two-tier
miss, never by trusting the previous clean usage snapshot.

The Hybrid JSON intentionally includes `hardware_qualification=false`,
`qualification_scope=software_scale_gate_single_run`,
`external_hardware_signoff_required=true`, and false values for
`target_nvme_matrix_passed`, `external_nvme_soak_passed`,
`external_power_loss_passed`, and `external_thermal_passed`. Passing CLI
thresholds proves only that one software run met its predeclared gates. The
target-device thermal/DWPD window, 24–72 hour soak, TB-scale restart SLA, real
power-loss tests, origin miss-storm behavior, and canary/rollback decision
remain external release evidence and cannot be inferred from a developer
machine.

## What the report proves

The single-line JSON contains:

- operation/read/write throughput and bounded latency histogram percentiles;
- selected `sync` or `async` public API;
- measured hits/misses/hit percentage and all three requested acceptance gates;
- selected admission/reclaim policy, rejection/reinsertion activity, and
  victim-scan/full-index-fallback counters;
- logical bytes, cache record bytes, direct/buffered operation and byte counts;
- write-batch count and the number of records coalesced behind their first record;
- cache and device write amplification;
- configured and peak I/O queue depth, completion time, errors, and rejects;
- process user/system CPU and wall-normalized CPU percentage;
- optional Linux block-device sectors and I/O busy ticks;
- final drain/close latency and the explicit threshold verdict;
- M7 policy activity, Region reuse, and current UTC-day write gauges.

The report includes `reclaim_records_scanned` and
`reclaim_index_fallbacks`, which validate that normal Region reuse remains
proportional to victim-Region contents rather than total index size. A non-zero
fallback count requires correlating the run with cache I/O/corruption metrics;
missing reclaim-counter evidence makes a large-scale artifact incomplete.

Memory correctness is checked separately by the bounded-resource tests and by
`memory_used_bytes <= memory_budget_bytes` in every report. For a full CPU and
copy profile, capture `perf record`/`perf stat` around the same command; for a
device profile, retain the JSON block counters together with `iostat -x` or an
equivalent NVMe telemetry capture. External profilers are observations, not a
cache correctness dependency.

No hardware performance or endurance result is asserted by the source-tree
test suite. A staging sign-off consists of every required-matrix and M7 A/B
command, its predeclared throughput/p99/hit-rate thresholds, the JSON result,
and matching CPU/device/SMART captures from the target host. Until those
artifacts exist and pass, M5 and M7 remain code-complete but their target-device
sign-offs remain pending. TB-scale recovery, 24--72 hour soak, canary, and real
power-loss results are separate M6/M8 production-environment gates.
