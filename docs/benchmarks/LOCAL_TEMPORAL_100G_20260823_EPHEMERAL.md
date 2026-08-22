# 100 GiB performance-first Hybrid validation

Date: 2026-08-23

## Result

The 12 GiB Bucket + 88 GiB Region + 8 GiB L1 configuration completed a
120-second mixed-size temporal run with no request error, rejection, stale
read, deadlock, or worker panic. Normal Hybrid mutations issued no route
journal record and no durability sync.

This is a local software scale result on the internal APFS SSD with buffered
I/O. It is not target-NVMe, power-loss, thermal, or production sign-off.
It is also a fresh-cache throughput result: total physical turnover was only
0.349x and no complete Region-reuse cycle was required before measurement.

| Metric | Previous 120 s check | Performance-first run |
| --- | ---: | ---: |
| Operations | 523,577 | 3,278,496 |
| Throughput | 4,362 ops/s | 27,306 ops/s |
| p99 | 27.263 ms | 3.146 ms |
| Read p99 | 5.243 ms | 1.966 ms |
| Write p99 | 46.137 ms | 10.486 ms |
| Errors / rejects / stale reads | 0 / 0 / 0 | 0 / 0 / 0 |
| Journal records / durability syncs | 202,982 / 16,593 | 0 / 0 |
| Close/drain/checkpoint | 1.303 s | 31.176 s |

Throughput increased by 6.26x and overall p99 fell by 88.5%. The long close is
now the main lifecycle bottleneck: the larger run drains remaining dirty L1
state and publishes complete lower/global checkpoints. It must not be counted
as a steady-state throughput regression, but it remains a production gap.

## Workload and resource evidence

- One million keys in a circular temporal timeline; newest 2% receives 85% of
  reads.
- Sizes: 256 B (45%), 4 KiB (25%), 64 KiB (20%), 1 MiB (10%).
- 70% reads, 27% puts, 3% removes; 3% TTL and 8% cross-tier updates.
- Sixteen clients, async API, device QD 64, four append lanes, eight bounded
  write-back workers, and write-back queue depth 128.
- Measured read/value throughput: 1,783.1 / 812.1 MiB/s.
- Overall hit rate: 91.27%. Recent hit rate was 98.53%, with all but one recent
  hit served by L1. Historical hit rate was 50.22%; Bucket and Region served
  49,965 and 71,550 historical reads respectively.
- Host writes were 37.52 GB for 102.24 GB admitted values, an observed 0.366x
  ratio from write coalescing, supersession, and disposable background drops.
- Peak write-back depth reached its configured 128 slots without rejection.
  Bucket/Region device queue peaks were 16/11.
- `/usr/bin/time -l` reported 9,591,848,960 B (8.93 GiB) maximum RSS versus
  9,191,222,796 B (8.56 GiB) planned and a 16 GiB aggregate hard budget.
- p99.9 was 46.14 ms and the single maximum was 9.95 s. This rare tail remains
  an explicit profiling target.

The exact command was:

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-100g-20260823-ephemeral/bucket.cache \
  --bucket-capacity 12GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-100g-20260823-ephemeral/region.cache \
  --region-capacity 88GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-100g-20260823-ephemeral/manifest.cache \
  --memory-capacity 8GiB --bucket-memory-budget 512MiB \
  --region-memory-budget 2GiB --hybrid-memory-budget 16GiB \
  --generator-memory-budget 512MiB --small-object-max 1KiB \
  --sizes 256:45,4KiB:25,64KiB:20,1MiB:10 \
  --keys 1000000 --prefill-percent 2 --prefill-concurrency 16 \
  --verify-samples 5000 --read-percent 70 \
  --access-pattern temporal --temporal-window-percent 2 \
  --temporal-hot-read-percent 85 --remove-percent 3 --ttl-percent 3 \
  --cross-tier-percent 8 --ttl-ms 5000 --concurrency 16 \
  --queue-depth 64 --backpressure block --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --journal-capacity 64MiB --api async --engine auto --mode buffered \
  --warmup-secs 10 --duration-secs 120 --yes --output json
```

## Lock-cycle finding and correction

The first attempt exposed a real architecture bug rather than merely a slow
close. A foreground put held its Hybrid ordering stripe and Memory shard while
waiting for mandatory demotion. All write-back workers could simultaneously
block acquiring those coarse ordering stripes for detached background
evictions, leaving no worker to run the mandatory task.

Detached workers now avoid the coarse ordering mutex. They hold a 65,536-way
latest-version fence across their lower-tier publication; foreground mutations
hold that fine fence only while publishing a version and release it before any
Memory eviction can wait for a worker. A deterministic one-worker regression
test recreates the old starvation topology and proves both background and
mandatory persistence complete while the coarse stripe remains occupied.

## Offline verification

Release-mode `cachectl hybrid-verify` passed the closed files:

- Manifest clean and valid; zero journal records and no recovery required.
- 786,432 Bucket pages and 159,931 entries checked; zero invalid pages.
- 2,815 Region headers and 304,865 records checked; zero issues.
- Region disposition `clean_checkpoint`; `safe_to_open=true`.
- Logical files total 100.071 GiB and consumed 49 GiB of APFS blocks.

## Remaining gates

The next architecture work should target the 31.2-second close/checkpoint, the
9.95-second maximum request tail, and a true Active Region write buffer. The
coarse 65,536-way detached-version table from this recorded run has since been
replaced by a bounded exact-key pending directory.
Target Linux NVMe qualification still requires `io_uring`, `O_DIRECT`, at least
one complete Region reuse cycle, device latency/utilization telemetry, and a
long soak.

The next comparable 100 GiB run must add:

```text
--steady-state-fill-turnovers 2 --steady-state-fill-max-secs 3600
```

The CLI will apply the same temporal mixed workload before measurement, require
both 2x combined lower-tier host-write turnover and a reuse count equal to the
configured Region count, drain the phase boundary, and only then reset the
measurement baseline. The
JSON report records pre-measure time, operations, host bytes, turnover, Region
reuse, and `steady_state_gate_passed`; a timeout exits non-zero without
publishing a misleading steady-state result.
