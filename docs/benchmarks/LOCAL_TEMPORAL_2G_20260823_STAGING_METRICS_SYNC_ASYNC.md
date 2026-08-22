# Local 2 GiB temporal Hybrid benchmark: staging metrics and sync/async

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `f0fd8338699cb647689acaaf35c233fba5067895`  
Release `cache-bench` SHA-256: `537d2b2826c28a1798e58b5e18a0e7f8a2a8f213e6d6f8106be3fcd39313bf8a`  
Release `cachectl` SHA-256: `a8dca2e435695d215455bb99a8d79f5b2e34c2d3c848ee930efd5a76be020540`

## Result and decision

Do not implement sparse Region staging writes for this workload. The new
completion accounting measured only **0.112%** obsolete staged bytes in the
sync run and **0.123%** in the async run. Staging spans were already **78.43%**
and **78.53%** full. A `live runs + tombstone filler` path would add branching,
extra SQEs, and recovery surface to avoid roughly one byte per thousand.

The next optimization target is the async facade. With otherwise identical
configuration, the sync API reached **46,816.6 ops/s** and the async API reached
**38,160.3 ops/s**. Sync was **22.68% faster**; equivalently, the current async
path lost 18.49% of sync throughput. The async memory-hit p99 was 458.8 us,
22.4x the sync result of 20.5 us, even though a memory hit requires no disk I/O.
This is strong evidence of per-request dispatch, wake/park, or queueing cost
above the cache engines. A CPU profile and direct async hot-path simplification
should precede further Region chunk tuning.

Both single runs passed acceptance with zero errors, overload rejections, or
stale values. Because the sync run completed more work in the same 30 seconds,
it also advanced farther through the temporal timeline and produced more disk
turnover. The large facade-sensitive latency difference is more diagnostic
than comparing absolute I/O byte totals between the two fixed-duration runs.

| Metric | Sync | Async | Difference |
|---|---:|---:|---:|
| Operations | 1,404,774 | 1,145,011 | -18.49% |
| Throughput | 46,816.6 ops/s | 38,160.3 ops/s | -18.49% |
| Hit rate | 89.100% | 89.621% | +0.522 pp |
| p50 | 1.919 us | 57.343 us | 29.88x |
| p99 | 4.719 ms | 5.243 ms | +11.11% |
| p99.9 | 10.486 ms | 10.486 ms | unchanged |
| Memory-hit p99 | 20.479 us | 458.751 us | 22.40x |
| Write throughput | 1,396.4 MiB/s | 1,141.2 MiB/s | -18.27% |
| Region staging span fill | 78.430% | 78.533% | +0.102 pp |
| Staged records per span | 10.33 | 11.73 | +13.65% |
| Obsolete completion records | 0.0355% | 0.0344% | -0.0011 pp |
| Obsolete completion bytes | 0.1120% | 0.1229% | +0.0109 pp |
| Region queue/buffer waits | 0 | 0 | unchanged |
| Region I/O QD peak | 16 | 14 | -2 |
| Request-gate wait | 223.7 ms | 286.7 ms | +28.13% |
| Drain/close | 539.0 ms | 542.1 ms | +0.58% |

The sync measurement sealed 12,545 spans containing 41,268,129,152 bytes and
classified 46 records / 46,214,336 bytes as obsolete at completion. The async
measurement sealed 10,392 spans containing 34,230,208,576 bytes and classified
42 records / 42,080,832 bytes as obsolete. Both had about 10 MiB resident at
the measurement boundary and zero resident/flushing bytes after close.

## Matched command

The runs used separate new cache files and differed only in `--api sync` versus
`--api async` and their path suffix.

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-staging-metrics-API/bucket.cache \
  --bucket-capacity 256MiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-staging-metrics-API/region.cache \
  --region-capacity 1792MiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-staging-metrics-API/manifest.cache \
  --memory-capacity 256MiB --bucket-memory-budget 64MiB \
  --region-memory-budget 256MiB --hybrid-memory-budget 1GiB \
  --generator-memory-budget 128MiB --small-object-max 1KiB \
  --sizes '256:45,4KiB:25,64KiB:20,1MiB:10' \
  --keys 100000 --prefill-percent 2 --prefill-concurrency 16 \
  --verify-samples 1000 --read-percent 70 \
  --access-pattern temporal --temporal-window-percent 2 \
  --temporal-hot-read-percent 85 --remove-percent 3 --ttl-percent 3 \
  --cross-tier-percent 8 --ttl-ms 5000 --concurrency 16 \
  --queue-depth 64 --backpressure block --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 64MiB \
  --journal-capacity 64MiB --api API --engine auto --mode buffered \
  --warmup-secs 1 --steady-state-fill-turnovers 1 \
  --steady-state-fill-max-secs 180 --duration-secs 30 --yes --output json
```

Replace `API` consistently with `sync` or `async`. The complete artifacts are:

- `/Users/leiysky/cache-rs-bench/temporal-2g-20260823-staging-metrics-sync`
- `/Users/leiysky/cache-rs-bench/temporal-2g-20260823-staging-metrics-async`

## Correctness and lifecycle evidence

Release-mode `cachectl hybrid-verify` passed both closed artifacts. Each had a
clean generation-36 manifest, empty journal, 16,384 valid Bucket pages, 55
valid Region headers, zero Region issues, `clean_checkpoint`, and
`safe_to_open=true`. The verifier scanned 5,811 Region records for sync and
6,992 for async.

The implementation passed the active-toolchain full test suite: 321 library
tests (318 passed, 3 ignored subprocess cases), 34 benchmark tests, 6 cachectl
tests, and 19 integration tests. All-target clippy was clean. Rust 1.85 passed
all-target, all-feature check and clippy.

## Scope

This is a short software-scale diagnostic on local APFS. It does not replace
the existing 10 GiB sustained gate, the requested 100 GiB local qualification,
or target-NVMe queue-depth, thermal, endurance, soak, and power-loss testing.
It is sufficient to reject sparse staging writes for the measured workload and
to select the async facade as the next local optimization target.
