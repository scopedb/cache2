# Local 10 GiB temporal Hybrid benchmark: volatile Region remove

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `482ace6d8206902157f515c61efa44a068437d76`  
Release `cache-bench` SHA-256: `f97cd0b44865b81681c8ccce1060b0b93c7a97a4ebcc1f8abfdc1f17c6196592`  
Release `cachectl` SHA-256: `ca2c10494935eb09dc6742d186a9f5292edb63fa0040510cecd9ff6b8a4fd36e`

## Result and decision

Keep the managed-Hybrid Region index-only removal path. Runtime Hybrid removes
now validate the exact stored key, establish the owner and Region dirty fence,
retire the current index entry, and avoid appending a Region tombstone. Clean
`flush`/`close` persists the retired index; a dirty Hybrid reopen continues to
use the existing safe cache-loss recovery policy. Standalone Region removal and
Hybrid recovery still append tombstones where a persistent deletion fence is
required.

Against the `1b075eb` true-wait baseline, the same mixed-size temporal workload
reached **61,721.2 ops/s**, up 0.80%. Region control-queue waiting fell from
41.540 s to exactly zero. Region I/O submissions fell 31.79% and Region write
batches fell 36.13%. p99 improved 22.22% and p99.9 improved 15.38%. There were
no errors, overload rejections, or stale reads, and offline verification passed.

This single local run does not establish a write-amplification improvement.
The candidate processed 0.80% more operations and 0.87% more removes, while
Region bytes written per operation rose 1.46% and host bytes written per
operation rose 1.54%. Its maximum latency and average Region I/O completion
time also regressed. The retained value is removal of an architectural control
wait and approximately one third of small Region submissions; large buffered
staging remains necessary to reduce bytes and device latency consistently.

| Metric | True-wait baseline | Volatile remove | Change |
|---|---:|---:|---:|
| Revision | `1b075eb` | `482ace6` | — |
| Throughput | 61,230.1 ops/s | 61,721.2 ops/s | +0.80% |
| Operations | 7,347,786 | 7,406,750 | +0.80% |
| Hit rate | 87.932% | 88.002% | +0.070 pp |
| p50 | 114.687 us | 114.687 us | unchanged |
| p99 | 2.359 ms | 1.835 ms | -22.22% |
| p99.9 | 6.816 ms | 5.767 ms | -15.38% |
| Maximum latency | 3.142 s | 5.879 s | +87.09% |
| Region control-queue wait | 41.540 s | 0 | -100% |
| Region control wait per operation | 5.653 us | 0 | -100% |
| Region I/O submitted/completed | 437,722 | 298,553 | -31.79% |
| Region I/O per 1,000 operations | 59.572 | 40.308 | -32.34% |
| Region write batches | 305,893 | 195,359 | -36.13% |
| Region write batches per 1,000 operations | 41.631 | 26.376 | -36.64% |
| Records coalesced | 84,708 | 94,969 | +12.11% |
| Region I/O submit wait | 4.379 s | 1.980 s | -54.79% |
| Region I/O completion average | 89.36 us | 177.76 us | +98.93% |
| Region bytes written | 55,149,453,856 | 56,403,103,488 | +2.27% |
| Region bytes written per operation | 7,505.6 B | 7,615.1 B | +1.46% |
| Host bytes written | 59,489,264,160 | 60,887,912,192 | +2.35% |
| Host bytes written per operation | 8,096.2 B | 8,220.6 B | +1.54% |
| Admitted value bytes | 229,327,083,008 | 230,293,547,008 | +0.42% |
| Software write amplification | 0.259 | 0.264 | +0.005 |
| Region reuses | 1,668 | 1,706 | +2.28% |

The measurement window completed 2,278,423 writes and 64,917 explicit removes.
All read/write/control queue and buffer wait counters were zero. Region and
Bucket I/O queue-depth peaks were 13 and 15. The run reached 2.506 physical
turnovers and 476 Region reuses before measurement, so its counters exclude
first-allocation behavior.

## Command

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-volatile-remove/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-volatile-remove/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-volatile-remove/manifest.cache \
  --memory-capacity 1GiB --bucket-memory-budget 128MiB \
  --region-memory-budget 512MiB --hybrid-memory-budget 4GiB \
  --generator-memory-budget 128MiB --small-object-max 1KiB \
  --sizes '256:45,4KiB:25,64KiB:20,1MiB:10' \
  --keys 100000 --prefill-percent 2 --prefill-concurrency 16 \
  --verify-samples 2000 --read-percent 70 \
  --access-pattern temporal --temporal-window-percent 2 \
  --temporal-hot-read-percent 85 --remove-percent 3 --ttl-percent 3 \
  --cross-tier-percent 8 --ttl-ms 5000 --concurrency 16 \
  --queue-depth 64 --backpressure block --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 128MiB \
  --journal-capacity 64MiB --api async --engine auto --mode buffered \
  --warmup-secs 2 --steady-state-fill-turnovers 2 \
  --steady-state-fill-max-secs 600 --duration-secs 120 --yes --output json
```

The complete 10 GiB artifact remains at
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-volatile-remove`.

## Correctness evidence

The release benchmark reported `acceptance_passed=true`, zero errors, zero
request or write-back rejection, and zero stale values. After clean close,
release-mode `cachectl hybrid-verify` reported:

- clean manifest generation 36, empty journal, and no recovery required;
- all 65,536 Bucket pages valid, with zero invalid pages;
- all 287 Region headers valid and 48,462 records verified;
- zero Region issues, `clean_checkpoint`, and `safe_to_open=true`.

Before the benchmark, the implementation passed
`cargo test --all-targets --all-features`,
`cargo clippy --all-targets --all-features -- -D warnings`, and
`cargo +1.85 check --all-targets --all-features`. Regression coverage includes
exact-key collision validation, tombstone-fence preservation, reinsertion/remove
ordering, clean-checkpoint reopen, and Region-to-Bucket retirement without a
Region record write.

## Scope and next step

This is a software-scale gate on local APFS buffered I/O. It is not target-NVMe,
thermal, soak, or power-loss qualification. The next structural experiment is a
bounded, lane-local Region staging path for managed Hybrid writes. Foreground
requests should publish from DRAM while large sequential chunks flush in the
background; flush, clear, close, rotation, and read routing must explicitly
fence resident and flushing records.
