# 100 GiB local temporal hybrid-cache validation

Date: 2026-08-22

## Result

The 100 GiB local baseline completed 10 minutes of sustained mixed-size temporal traffic with zero request errors, zero overload rejections, and zero stale reads. Memory, Bucket, and Region tiers all served reads, and recent objects showed the expected memory residency and latency advantage over historical objects.

This run is a baseline, not a production qualification. It advanced through 42.97% of the logical key ring and therefore did not prove a complete overwrite/reuse cycle. It also ran on the internal APFS SSD with buffered positioned I/O, not Linux `io_uring` plus `O_DIRECT` on the target NVMe device.

## Host and cache layout

| Item | Value |
| --- | ---: |
| Host | Apple M4 Max, 16 cores, 64 GiB RAM |
| Filesystem | APFS, internal SSD |
| Bucket capacity | 12 GiB |
| Region capacity | 88 GiB |
| Logical cache-file size after close | 100.071 GiB |
| APFS blocks allocated after close | 52.553 GiB |
| L1 memory capacity | 8 GiB |
| Configured aggregate memory ceiling | 16 GiB |
| Planned benchmark memory | 8.558 GiB |
| Sampled peak RSS | 13.232 GiB |
| I/O mode | `engine=auto`, buffered |

APFS sparse/preallocated accounting explains why allocated blocks were lower than logical file size. The logical file lengths, rather than `du`, are the capacity evidence.

## Workload

- One million logical keys in a circular timeline.
- Successful put-class mutations advance a shared write head; removes sample the timeline without advancing it.
- Reads target the newest 2% window 85% of the time and older keys 15% of the time.
- Object mix: 256 B (45%), 4 KiB (25%), 64 KiB (20%), and 1 MiB (10%). The weighted logical working set is approximately 110.9 GiB.
- Operation mix: 70% reads, 27% puts, and 3% removes, with 3% TTL and 8% cross-tier updates.
- Sixteen clients, queue depth 64, four append lanes, write-back queue depth 128, and eight demotion workers.
- Thirty-second warm-up followed by a 600-second measurement.
- Bounded `Block` backpressure was used for the accepted baseline.

The exact command was:

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-100g-20260822/bucket.cache \
  --bucket-capacity 12GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-100g-20260822/region.cache \
  --region-capacity 88GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-100g-20260822/manifest.cache \
  --memory-capacity 8GiB --bucket-memory-budget 512MiB \
  --region-memory-budget 2GiB --hybrid-memory-budget 16GiB \
  --generator-memory-budget 512MiB --small-object-max 1KiB \
  --sizes 256:45,4KiB:25,64KiB:20,1MiB:10 \
  --keys 1000000 --prefill-percent 2 --prefill-concurrency 16 \
  --verify-samples 20000 --read-percent 70 \
  --access-pattern temporal --temporal-window-percent 2 \
  --temporal-hot-read-percent 85 --remove-percent 3 --ttl-percent 3 \
  --cross-tier-percent 8 --ttl-ms 5000 --concurrency 16 \
  --queue-depth 64 --backpressure block --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --journal-capacity 64MiB --api async --engine auto --mode buffered \
  --warmup-secs 30 --duration-secs 600 --yes --output json
```

## Measurement

| Metric | Result |
| --- | ---: |
| Duration | 600.021 s |
| Operations | 1,510,462 |
| Throughput | 2,517.35 ops/s |
| Reads / puts / removes | 1,032,040 / 465,292 / 13,130 |
| Request errors / rejections / stale reads | 0 / 0 / 0 |
| Overall hit ratio | 95.958% |
| Read throughput | 171.870 MiB/s |
| Admitted-value throughput | 75.041 MiB/s |
| p50 / p99 / p99.9 | 4.719 / 62.915 / 167.772 ms |
| Read p99 / write p99 | 14.680 / 109.052 ms |
| Maximum request latency | 2.873 s |
| Measurement host writes | 43.978 GiB |
| Measurement admitted values | 43.971 GiB |
| Observed write amplification | 1.000x |
| Logical keyspace turnover | 0.429685x |
| Logical ingest turnover | 0.439709x |
| Complete write-head wraps | 0 |
| Close and write-back drain | 17.826 s |

Queue peaks stayed bounded and below their configured limits: Bucket 10, Region 8, and write-back 11. All 465,292 measured writes entered the memory-first write-back path; there were no fallbacks, demotion failures, demotion rejections, or worker panics. The measurement completed 405,456 demotions.

## Temporal behavior

| Read band | Requests | Hit ratio | Memory hits | Bucket hits | Region hits | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Recent 2% | 876,974 | 96.236% | 843,962 | 0 | 0 | 11.534 ms |
| Historical 98% | 155,066 | 94.390% | 54,567 | 37,286 | 54,514 | 31.457 ms |

The observed recent-read share was 84.975%, matching the configured 85%. Recent reads were served from memory 96.236% of the time versus 35.190% for historical reads, a 61.046 percentage-point separation. Historical p99 was 2.73 times recent p99. The small hit-ratio gap is expected because Bucket and Region recover most historical memory misses.

## Offline integrity verification

After clean close, release-mode `cachectl hybrid-verify` scanned the original files and passed:

- Manifest checkpoint clean and valid.
- 786,432 Bucket buckets checked; 190,870 entries checked; no invalid buckets.
- 2,815 Region headers and 302,745 records checked; no invalid headers or records.
- Region reopen disposition: `clean_checkpoint`.
- `safe_to_open=true`.

The first verification attempt exposed a verifier defect rather than file damage: runtime accepts eight append lanes, while management verification had a stale hard-coded limit of two. The verifier now imports the runtime limit, safely computes the full eight-lane mask, and has a regression test that writes and verifies a clean checkpoint using every supported lane. The unchanged workload files then passed verification.

## Acceptance status

| Requirement | Status | Evidence |
| --- | --- | --- |
| 100 GiB logical cache layout | Pass | 100.071 GiB closed files |
| Sustained mixed-size hybrid traffic | Pass | 600 s, all three tiers active |
| Correctness under accepted load | Pass | zero errors, rejects, and stale reads; offline verify passed |
| Temporal hot/cold separation | Pass | 61.046 pp memory-share gap; 2.73x p99 gap |
| Bounded queues | Pass | observed peaks 10 / 8 / 11 |
| At least one full logical rotation | Incomplete | 0.429685x, zero wraps |
| Planned-memory accounting | Warning | peak RSS 13.232 GiB vs 8.558 GiB plan, but below 16 GiB aggregate ceiling |
| Fast deterministic shutdown | Warning | close/drain took 17.826 s |
| Fail-fast overload mode | Warning | calibration with `Reject` exposed a hang/poison path; `Block` completed cleanly |
| Target NVMe data path | Not tested locally | macOS buffered I/O; no `io_uring` or `O_DIRECT` |

## Follow-up gates

At the observed write-head rate, a fresh run needs approximately 1,400 measured seconds to cross one complete keyspace wrap. The next local acceptance run should set `--duration-secs 1400 --min-logical-keyspace-turnovers 1`; a stronger two-wrap soak needs approximately 47 minutes. It should also collect windowed RSS rather than process snapshots and enforce a close-time SLA.

Target-hardware qualification remains separate: Linux, external NVMe, `io_uring`, `O_DIRECT`, at least one complete Region-reuse cycle, declared throughput and p99 targets, and device-level latency/write-amplification telemetry.

## Post-optimization 100 GiB verification

A 120-second measurement was run against the same 12 GiB Bucket + 88 GiB
Region + 8 GiB L1 layout after the architecture fixes. The workload dimensions
were unchanged except for a 10-second warm-up, 5,000 verification samples, and
the shorter measurement window. This is a scale regression check, not a
replacement for the 600-second baseline or the target-NVMe qualification.

The implementation under test added proactive bounded write-back, an
ordering-free live L1-hit path, a fixed 1 ms journal coalescing deadline,
four-logical-group durability sync waves, buffered append coalescing, and a
lower-cost deterministic value generator.

| Metric | Original 600 s baseline | Post-optimization 120 s check |
| --- | ---: | ---: |
| Throughput | 2,517 ops/s | 4,362 ops/s |
| p99 | 62.915 ms | 27.263 ms |
| Read p99 | 14.680 ms | 5.243 ms |
| Write p99 | 109.052 ms | 46.137 ms |
| Memory-tier p99 | not recorded | 0.786 ms |
| Foreground demotions / puts | 87.140% | 4.375% |
| Close and write-back drain | 17.826 s | 1.303 s |
| Request errors / rejections / stale reads | 0 / 0 / 0 | 0 / 0 / 0 |

The post-optimization check completed 523,577 operations, including 161,251
puts. Proactive persistence completed 146,507 entries; 14,744 conflicted or
were skipped and remained eligible for normal eviction/flush fallback. The
journal committed 202,982 records with 16,593 durability syncs, or 12.233
records per sync. No write-back rejection, demotion failure, or worker panic
was observed. Peak Bucket/Region queue depth was 14/9 and peak write-back depth
was 127 of 128; the executor always retained its foreground slot.

The logical files totaled 100.071 GiB and occupied 21 GiB of APFS blocks after
the shorter run. Release-mode `cachectl hybrid-verify` passed the closed files:
786,432 Bucket pages, 72,716 Bucket entries, 2,815 Region headers, and 112,980
Region records were checked with zero invalid pages, headers, or records;
`safe_to_open=true` and the Region disposition was `clean_checkpoint`.
