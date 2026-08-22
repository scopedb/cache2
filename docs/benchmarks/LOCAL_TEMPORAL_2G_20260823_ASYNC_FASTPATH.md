# Local 2 GiB temporal Hybrid benchmark: async L1 fast path

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `5adb66ce3cd8e9f1b409544dec778204ba204dfe`  
Release `cache-bench` SHA-256: `e0ed7c817cd661da2b288f7a52ad804ee21d99050ba59dce89023b4c2c771566`  
Release `cachectl` SHA-256: `5d41578e34ee37c6b1b2dd1186f9c47e4bf93fdfe00c34be58b57d026b1dfe90`

## Result and decision

The async-facade gap is no longer an architectural blocker for this workload.
The candidate reached **45,892.3 ops/s**, up **20.26%** from the matched async
baseline of 38,160.3 ops/s. It delivered **98.03%** of the previously measured
sync throughput of 46,816.6 ops/s; the async deficit fell from 18.49% to 1.97%.

The strongest signal is the L1 path. Memory-hit p99 fell from **458.751 us** to
**24.575 us** (-94.64%), only 20.0% above the old sync result of 20.479 us.
Overall p50 fell from 57.343 us to 6.655 us (-88.39%), while p99 improved by
10.0%. The candidate completed 20.26% more operations in the same 30-second
window with zero errors, overload rejections, or stale values.

This validates the combined async-facade changes:

- no-deadline live L1 hits retain the bounded Hybrid request permit but bypass
  key copy, the slow read queue, runnable allocation, and worker wakeup;
- L1 misses, expired values, operation-lock contention, and deadline requests
  retain the old bounded/cancelable worker path;
- ordinary task completion no longer broadcasts to the complete worker pool;
  control completion and final drain still wake every worker they release;
- submission no longer scans the complete queued `VecDeque` on every reserve
  or on the worker-won attach race;
- mutation queue capacity is reserved before copying caller key/value bytes.

The workload still spent 53.64% of all operations on memory hits, so a direct
L1 completion lane is the correct structural split. Further Region staging
tuning remains lower priority than eliminating the allocations still retained
by the ready-Future representation and by the benchmark's per-operation waker.

| Metric | Old async | Candidate async | Change |
|---|---:|---:|---:|
| Operations | 1,145,011 | 1,376,957 | +20.26% |
| Throughput | 38,160.3 ops/s | 45,892.3 ops/s | +20.26% |
| Hit rate | 89.621% | 89.211% | -0.410 pp |
| p50 | 57.343 us | 6.655 us | -88.39% |
| p99 | 5.243 ms | 4.719 ms | -10.00% |
| p99.9 | 10.486 ms | 10.486 ms | unchanged |
| Maximum latency | 23.560 ms | 21.433 ms | -9.03% |
| Memory-hit p99 | 458.751 us | 24.575 us | -94.64% |
| Read throughput | 2,349.6 MiB/s | 2,838.2 MiB/s | +20.80% |
| Write throughput | 1,141.2 MiB/s | 1,370.7 MiB/s | +20.11% |
| Request-gate wait | 286.7 ms | 246.7 ms | -13.94% |
| Region staging span fill | 78.533% | 78.521% | -0.012 pp |
| Obsolete staged bytes | 0.1229% | 0.0982% | -0.0247 pp |
| Region I/O operations | 76,599 | 91,196 | +19.06% |
| Drain/close | 542.1 ms | 547.1 ms | +0.93% |

The small hit-rate difference is expected in fixed-duration temporal runs: the
faster candidate advanced 3.92 logical keyspace turnovers and performed more
historical reads. It does not explain the L1 latency collapse, because the
tier-specific histogram compares only verified memory hits.

## Profile basis

Before the change, a 4.543-second macOS stack sample attributed 1,838 of 1,970
condition-variable broadcast samples (93.3%) to
`TaskDoneGuard -> task_done -> notify_all`. The sampled workload also reported
842,463 L1 hits out of 1,552,130 total operations (54.3%). A benchmark caller
additionally spent 204 of 4,543 wall-stack samples contending in queue reserve's
cancelled-task scan. These observations selected the facade rather than Region
I/O as the next optimization target.

## Release-only issue caught during validation

The first release candidate stalled on its first direct completion even though
all executor workers were idle. Isolation showed that `ready_future()` invoked
`RequestCore::start()` only inside `debug_assert!`; optimized builds removed the
call and left the Future permanently queued. The final revision starts the core
unconditionally and asserts only the returned boolean. A dedicated behavior
test now covers immediate completion, and the release-mode smoke and full
measurement both completed normally.

This was a pre-existing cold-path bug for immediate async errors/rejections;
the new successful L1 ready path made it deterministic and therefore visible.

## Matched command

The candidate used the same configuration as the old sync/async comparison;
only the revision and path suffix changed.

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-async-fastpath-fixed/bucket.cache \
  --bucket-capacity 256MiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-async-fastpath-fixed/region.cache \
  --region-capacity 1792MiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-async-fastpath-fixed/manifest.cache \
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
  --journal-capacity 64MiB --api async --engine auto --mode buffered \
  --warmup-secs 1 --steady-state-fill-turnovers 1 \
  --steady-state-fill-max-secs 180 --duration-secs 30 --yes --output json
```

The retained artifact is
`/Users/leiysky/cache-rs-bench/temporal-2g-20260823-async-fastpath-fixed`.

## Correctness and lifecycle evidence

The run passed its software-scale acceptance gate with 1,376,957 operations,
zero errors, zero rejections, and zero stale values. It completed 404 Region
reuses before measurement and another 1,230 during measurement. Region and
Bucket buffer/backpressure wait counters remained zero, and no device I/O error
occurred.

Release-mode `cachectl hybrid-verify` passed the closed artifact. The manifest
was clean at generation 36 with an empty journal. Verification checked all
16,384 Bucket pages and 5,924 Region records, found zero invalid Bucket pages,
zero invalid/truncated Region headers, and zero Region issues, and reported
`clean_checkpoint` with `safe_to_open=true`.

At the final revision, the active toolchain passed 324 library tests (321
passed, 3 ignored subprocess workers), 34 benchmark tests, 6 cachectl tests,
and 19 integration tests. Rust 1.85 passed all-feature/all-target check and
clippy with warnings denied.

## Scope and next step

This remains a single local APFS software-scale run, not a target-NVMe or soak
qualification. The next measured optimization should make ready completions
allocation-free, then change the benchmark async client to reuse a runtime
waker and keep multiple requests outstanding. Those changes must be measured
separately so benchmark-runtime overhead is not mistaken for engine overhead.
