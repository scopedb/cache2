# Production operations

This runbook treats the cache as disposable acceleration. The source of truth
must remain outside cache-rs. When recovery is uncertain, prefer a miss, a new
cache path, or a controlled reset over in-place repair.

## Before a canary

Record workload-specific limits before deployment:

- cache and origin request rate, origin concurrency, and timeout budget;
- cache hit-rate floor after warm-up;
- cache p99 and origin p99 ceilings;
- memory budget, queue depths, and expected object-size distribution;
- expected maximum live entries, index slots (target load at most 80%), and
  full-checkpoint pause/write budget;
- Hybrid `flush()` cadence, maximum clean-boundary age, and tolerated
  cold-start/miss-storm exposure; managed Region periodic checkpoints are
  disabled and journal rollover is not a full Region index checkpoint;
- maximum daily host writes derived from the SSD DWPD allowance;
- recovery-time objective for the configured capacity;
- maximum reclaim backlog, I/O error, and checkpoint error counts.

Run configuration diagnostics before creating the file, for example:

```sh
cachectl diagnose --path /mnt/nvme/cache-rs.data --capacity 1TiB \
  --region-size 32MiB --index-slots 125000000 \
  --append-lanes 4 --memory-budget 8GiB --output json
```

For the complete Hybrid cache, diagnose all three persistent files and the
aggregate L1/request/policy budget in one command. Diagnostics never create or
open any path:

```sh
cachectl hybrid-diagnose \
  --bucket-path /mnt/nvme/cache.small --bucket-capacity 64GiB \
  --region-path /mnt/nvme/cache.large --region-capacity 1TiB \
  --manifest-path /mnt/nvme/cache.hybrid-manifest \
  --memory-capacity 16GiB --small-object-max 1KiB \
  --bucket-engine auto --bucket-mode auto --bucket-io-queue-depth 128 \
  --region-engine auto --region-mode auto --region-io-queue-depth 128 \
  --index-slots 125000000 --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --bucket-memory-budget 2GiB --region-memory-budget 8GiB \
  --hybrid-memory-budget 32GiB --output json
```

Archive `journal_recovery_memory_bytes`,
`region_checkpoint_accounting_bytes`, and `checkpoint_slot_bytes` from this
report. They are enforced parts of the open-time memory/storage plan, not
optional estimates. For Hybrid, schedule application-level `flush()` from the
measured recovery RTO and the O(index-slots) pause/write budget. Normally a
flush drains dirty L1, fences both lower tiers, and publishes matching global
namespace usage. If `write_back_volatile_loss_pending` is set, it instead clears
L1 and both lower tiers, resets namespace usage, and publishes a clean-empty
boundary. The nested Region's checkpoint interval does not provide either
global boundary.
The managed manifest starts dirty/unbound and every open fences it dirty before
touching either lower tier. TTL/corruption cleanup and SecondChance reinsertion
can also dirty the owner without a foreground write. If the process exits after
such a fence but before a matching global clean boundary, the next open
deliberately safe-clears both lower tiers. Treat that outcome as a
cold-cache/miss-storm event, not as recovered source data, and keep the explicit
Hybrid flush cadence even during read-heavy periods.

Then run the required matrix
from [NVME_BENCHMARK.md](NVME_BENCHMARK.md) on the target NVMe. Source tests do
not substitute for a target-device throughput, latency, thermal, or endurance
sign-off.

Use one dedicated regular-file path per cache instance. Do not use a symlink,
shared writer path, or an application data file. Keep enough free filesystem
space for exact preallocation plus the two-slot checkpoint tail. At large index
sizes the tail itself is several GiB; use the `checkpoint_slot_bytes` diagnostic
instead of estimating it from cache capacity.

## Metrics and health

`DiskCache::metrics_snapshot().to_openmetrics()` returns a complete bounded
OpenMetrics exposition. Serve it from the owning process; do not open the cache
file from a sidecar merely to obtain live statistics. Prometheus can scrape it
directly. For OpenTelemetry, configure an OpenTelemetry Collector Prometheus
receiver against the same endpoint.

`HybridCache::openmetrics_snapshot()` exposes the coordinator plus Memory,
Bucket, Region, admission, request-gate, combined host-write counters, component
health, dirty L1 bytes/entries, and every bounded write-back queue/memory gauge.
`HybridCacheStats::to_openmetrics()` remains available after close for the
final drained snapshot. Export these from the owning service; `cachectl` does
not open a live Hybrid cache merely to scrape metrics.

For write-back pressure, compare current and peak queue slots and bytes against
their capacities independently. `proactive_persisted` preserves a dirty value.
`proactive_invalidated` together with `dropped_evictions` means the engine
intentionally converted an update to a miss after synchronously hiding its
Region/Bucket candidates in memory; it consumes no queue slot or device I/O.
This is cache loss, not stale data or corruption. While
`volatile_loss_pending=1`, the next flush/close clears the complete cache before
publishing a clean empty boundary. Correlate it with hit rate and origin QPS.
Bucket namespace usage remains conservatively charged until that boundary, so
tight quotas may reject early. `demotion_failures` or put rejections still mean
the overload path could not safely complete. `proactive_skipped` alone can be an
expected lower-absent drop.

Interpret write governance by both views: `daily_host_write_bytes` is actual
submitted I/O, while `daily_budget_used_bytes` and
`daily_budget_reserved_bytes` are admission accounting. Bucket RMW submits a
full page, Region live capacity uses the receipt's packed physical length, and
SecondChance reinsertion consumes the shared daily budget. Preserve the policy
snapshot/OpenMetrics alongside benchmark JSON; one daily gauge alone cannot
prove reservation correctness.

An expired Bucket hit can be a logical miss while its aligned physical charge
still occupies namespace quota. A managed access refunds it only after the
complete page rewrite and exact receipt; daily-budget rejection, cancellation
before the cleanup commit point, and write failure retain both quota and the
conservative Bloom state. A namespace filled by untouched expired pages can
therefore continue rejecting puts until those pages are read/removed, the tier
is cleared, or a future scavenger is deployed. Provision quota headroom and
alert on sustained capacity rejection plus expiry-compaction page writes.

For async reads, `CancelOutcome::TooLate` means cleanup crossed its commit
point; it is not a successful cancellation. Continue observing the request's
real completion, host-page write, health transition, and exact refund.

`metrics_snapshot().state_log_json()` emits the latest bounded lifecycle
history as newline-delimited JSON (`sequence`, `unix_ms`, `from`, `to`, and a
fixed `reason`). Forward newly observed sequence numbers through the service's
normal logger; cache keys, paths, and tenant-controlled strings are absent.

Readiness is strict:

- `Healthy`: ready for normal traffic;
- `MissOnly`: not ready for a full cutover; reads deliberately miss while
  recovery or I/O degradation is active;
- `Poisoned`: not ready; stop routing and replace/reopen the instance;
- `Closed`: not ready.

At minimum alert on:

- `cache_rs_up == 0`;
- any increase in I/O, checkpoint, or corrupt-record errors;
- recovery progress stalled beyond its declared SLA;
- sustained write/reclaim backlog or queue saturation;
- any increase in `cache_rs_reclaim_index_fallbacks_total`, and victim record
  scan growth that correlates with index size rather than Region contents;
- memory peak approaching the configured hard budget;
- hit rate below the declared post-warm-up floor;
- p99 above the declared ceiling;
- write amplification or daily host bytes above the DWPD plan;
- critical NVMe health observations.

Metrics use only fixed operation/result labels. Never add cache keys, namespace
names supplied by tenants, or file paths as labels.

## Miss-storm protection

The cache cannot rate-limit an origin it does not own. Configure the explicit
origin-fill limiter from the origin capacity plan and acquire a permit only
after a cache miss and immediately before contacting the origin:

```rust
let cache = config
    .with_origin_fill_protection(cache_rs::OriginFillConfig::new(2_000, 64))
    .open()?;

if let Some(value) = cache.get(key)? {
    return Ok(value);
}
let _permit = cache.try_begin_origin_fill()?;
let value = load_from_origin(key)?;
let _ = cache.put(key, &value, cache_rs::PutOptions::default());
Ok(value)
```

Acquisition never queues: rate or concurrency saturation is returned
immediately. The service decides whether to shed, serve stale source data, or
retry within its own deadline. Coalesce same-key fills above this layer when an
origin request is expensive.

## Canary and rollback

Use a new cache path for a new release or capacity. Retain the old process and
old path for the rollback window. Increase traffic through 1%, 5%, 25%, 50%,
and 100%; at every step hold long enough to reach a representative reclaim
state and check:

- hit rate and origin QPS/concurrency;
- request p50/p95/p99 and timeout/error classes;
- queue depth, rejection, and origin-fill rejection;
- write-back persist/invalidate/drop rates and the resulting origin QPS;
- recovery progress and checkpoint failures;
- region valid ratio, reclaim backlog, victim scan records, and full-index fallback;
- host writes, write amplification, and NVMe health.

On a hard-threshold breach, route traffic back to the old process/path. Do not
attempt an in-place downgrade of a file already written by the new binary.

## Failure actions

| Signal | Immediate action | Follow-up |
| --- | --- | --- |
| `MissOnly` during startup | Keep canary traffic low and origin guarded | Wait for monotonic recovery progress; replace the path if the SLA expires |
| terminal `MissOnly` after I/O error | Stop new traffic, preserve device evidence | Check filesystem/NVMe health, then reopen or create a new path |
| `Poisoned` | Stop traffic and close | Capture metrics/events, replace the process and cache path |
| checkpoint fallback increases | Continue only if latency/origin budgets hold | Run offline `cachectl verify` after the owner closes |
| reclaim index fallback increases | Keep reads available and watch p99 | Correlate with short read/corrupt header/device errors; verify offline after close |
| `ReclaimBacklog` or queue rejects | Shed/defer writes; reads retain reserved capacity | Reduce write traffic or tune only within the validated memory/device plan |
| sustained proactive invalidation without rejects | Keep serving if origin and latency budgets hold | Reduce churn or add write-back/device throughput; validate the expected miss-rate tradeoff |
| write-back demotion failures or put rejects | Shed/defer cache writes | Inspect both slot and byte hard caps plus pending allocation/lifecycle failures |
| NVMe critical | Stop new cache writes according to configured policy | Drain/replace the device; the authoritative store remains the source |
| `ENOSPC` | Stop cache writes and traffic to the instance | Free space or provision a new path; never truncate a live cache |

`cachectl inspect` and `cachectl verify` are offline commands. They take the
exclusive writer lock and refuse to inspect an active instance, avoiding a
misleading mixed-generation report.

For Hybrid, use `hybrid-inspect` or `hybrid-verify` with all three paths. The
tool locks in manifest/Bucket/Region order and retains every lock until it has
captured one coherent view. `hybrid-verify` streams all Bucket pages, Region
records/checkpoints, and the bounded transition journal:

```sh
cachectl hybrid-verify \
  --bucket-path /mnt/nvme/cache.small \
  --region-path /mnt/nvme/cache.large \
  --manifest-path /mnt/nvme/cache.hybrid-manifest --output json
```

## Format, reset, and capacity changes

`cachectl format` may create only a missing or empty dedicated path and requires
explicit confirmation. `cachectl reset` is destructive and may operate only on
a recognized Format V1 file after obtaining its exclusive lock; it refuses
symlinks, unknown formats, and unsupported versions.

```sh
cachectl format --path /mnt/nvme/cache-rs.data --capacity 1TiB \
  --region-size 32MiB --append-lanes 2 --memory-budget 8GiB --yes

cachectl reset --path /mnt/nvme/cache-rs.data --capacity 1TiB \
  --region-size 32MiB --append-lanes 2 --memory-budget 8GiB --yes
```

Reset holds one `flock` and one inode across Format V1 recognition, durable
truncate, and fresh formatting. If reset reports an I/O error after truncation,
run `inspect`; an empty file can be retried with `format`. Never script `--yes`
against a path that is not dedicated cache storage.

Hybrid initialization deliberately has no reset command. `hybrid-format`
requires `--yes`, three distinct paths, and verifies that every path is missing
or an empty regular file before opening any engine. It refuses recognized but
non-empty cache files as well as symlinks:

```sh
cachectl hybrid-format \
  --bucket-path /mnt/nvme/cache.small --bucket-capacity 64GiB \
  --region-path /mnt/nvme/cache.large --region-capacity 1TiB \
  --manifest-path /mnt/nvme/cache.hybrid-manifest \
  --memory-capacity 16GiB --small-object-max 1KiB \
  --bucket-memory-budget 2GiB --region-memory-budget 8GiB \
  --hybrid-memory-budget 32GiB --yes --output json
```

There is no in-place resize. To change capacity:

1. provision a new path and run configuration diagnostics;
2. open and warm the new cache under canary traffic;
3. shift traffic using the canary gates above;
4. retain the old path for rollback;
5. remove the old file only after the rollback window and explicit operator
   acknowledgement.

## Soak and evidence

`cache-bench` validates every returned hit against its deterministic value and
exits on the first wrong value. A typical staging soak repeatedly reopens the
same dedicated path:

```sh
cargo run --release --bin cache-bench -- \
  --path /mnt/nvme/cache-rs.soak --capacity 107374182400 \
  --duration-secs 3600 --warmup-secs 60 --output json
```

Run it for the declared 24–72 hour window with an external supervisor, saving
each JSON line together with process RSS, CPU profile, block-device counters,
SMART/NVMe telemetry, kernel logs, and the exact binary/configuration. Add the
existing crash/failpoint test suite and real power-loss validation where the
deployment risk requires it.

The mixed-object Hybrid harness uses a separate, safer invocation that also
requires three empty paths and `--yes`:

```sh
cache-bench hybrid \
  --bucket-path /mnt/nvme/bench.small --bucket-capacity 64GiB \
  --region-path /mnt/nvme/bench.large --region-capacity 1TiB \
  --manifest-path /mnt/nvme/bench.hybrid-manifest \
  --memory-capacity 16GiB --sizes 256:50,4KiB:30,64KiB:20 \
  --small-object-max 1KiB --keys 100000000 \
  --generator-memory-budget 2GiB --prefill-concurrency 64 \
  --verify-samples 100000 --read-percent 80 --concurrency 64 \
  --remove-percent 10 --ttl-percent 5 --cross-tier-percent 20 --ttl-ms 250 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --journal-capacity 64MiB --min-journal-rollovers 1 \
  --warmup-secs 60 --steady-state-fill-turnovers 2 \
  --steady-state-fill-max-secs 3600 \
  --min-capacity-turnovers 2 --min-disk-qd-peak 8 \
  --min-write-back-qd-peak 8 \
  --queue-depth 128 --engine uring --mode direct \
  --duration-secs <long-enough-for-two-turnovers> \
  --max-journal-rollover-ms <agreed-rollover-ms> \
  --max-close-ms <agreed-close-ms> --yes --output json
```

Its JSON always states `hardware_qualification=false`, requires external
hardware sign-off, and leaves the target-NVMe, soak, power-loss, and thermal
fields false. The line is one software scale-gate artifact, never an automatic
production, endurance, or soak certification.
The steady-state fill is a bounded pre-measure phase: it must observe both the
requested physical turnover and one complete Region reuse cycle before
latency/throughput samples are retained. `--min-capacity-turnovers`
independently gates the subsequent measurement window.
