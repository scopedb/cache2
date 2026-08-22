# Local 2 GiB temporal Hybrid benchmark: inline ready + native wait

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `3d58c0e32009b1bf5879dd5479d4634e5fd4daa9`  
Release `cache-bench` SHA-256: `69cbaef4c21f44ad6c764fd7275731e905b80f136cf17b509101dae5418748a0`  
Release `cachectl` SHA-256: `cbeb31b814f462b19c2f2d6d08873c58391c2e32c12a946ed1c97506cbca785e`

## Result and decision

The allocation-free ready representation and native blocking benchmark client
improve the already-fast L1 completion latency, but do not raise aggregate
throughput materially on this workload. The run reached **45,912.7 ops/s**, only
**0.04%** above the preceding async-fast-path candidate. Overall p50 improved
from **6.655 us** to **5.631 us** (-15.39%) and memory-hit p99 improved from
**24.575 us** to **22.527 us** (-8.33%). Overall p99 remained **4.719 ms**.

This rules out ready-Future and benchmark-waker allocation as the next
throughput-scale architectural blocker. The workload already drives about
2.83 GiB/s of logical reads and 1.37 GiB/s of logical writes through local
buffered storage. Its remaining tail is dominated by lower-tier requests:
Region-hit p99 is 4.719 ms and write p99 is 7.340 ms, while every bounded queue
reported zero rejection and the Region staging path retained 78.52% fill.
The next optimization should therefore be selected from a fresh profile of the
lower-tier read/write path, not by adding more completion machinery.

The two changes under validation are:

- a completed `CacheFuture<T>` stores its result inline without allocating a
  `RequestCore`; a `Mutex<Option<T>>` preserves the previous public
  `T: Send => CacheFuture<T>: Send + Sync` auto-trait contract while mutable
  access keeps the ready path allocation-free and lock-free;
- both benchmark entry points call the facade's native `.wait()` instead of
  allocating an `Arc<Waker>`, boxing and polling each request, then adding a
  caller-side park/unpark round trip.

The benchmark change is intentionally explicit in schema v9 as
`client_completion_model=blocking_wait_one_outstanding`. The comparison below
spans both an engine representation change and a harness completion change; it
is not an engine-only A/B result.

| Metric | Previous candidate | Inline ready + native wait | Change |
|---|---:|---:|---:|
| Operations | 1,376,957 | 1,377,870 | +0.07% |
| Throughput | 45,892.3 ops/s | 45,912.7 ops/s | +0.04% |
| Hit rate | 89.211% | 89.253% | +0.042 pp |
| p50 | 6.655 us | 5.631 us | -15.39% |
| p99 | 4.719 ms | 4.719 ms | unchanged |
| p99.9 | 10.486 ms | 9.437 ms | -10.00% |
| Maximum latency | 21.433 ms | 24.434 ms | +14.00% |
| Memory-hit p99 | 24.575 us | 22.527 us | -8.33% |
| Read throughput | 2,838.2 MiB/s | 2,834.9 MiB/s | -0.12% |
| Write throughput | 1,370.7 MiB/s | 1,371.3 MiB/s | +0.04% |
| Request-gate wait | 246.7 ms | 238.6 ms | -3.29% |
| Region staging span fill | 78.521% | 78.518% | -0.003 pp |
| Region I/O operations | 91,196 | 90,987 | -0.23% |
| Drain/close | 547.1 ms | 520.4 ms | -4.88% |

The small throughput and maximum-latency differences are within normal
single-run local-storage variation. The tier-specific memory histogram is the
stronger evidence that the intended fast path improved.

## Matched command

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-native-wait/bucket.cache \
  --bucket-capacity 256MiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-native-wait/region.cache \
  --region-capacity 1792MiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-2g-20260823-native-wait/manifest.cache \
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
`/Users/leiysky/cache-rs-bench/temporal-2g-20260823-native-wait`.

## Correctness and lifecycle evidence

The software-scale run passed with 1,377,870 operations, zero errors, zero
rejections, and zero stale values. It completed 384 Region reuses before
measurement and another 1,233 during measurement. Bucket/Region buffer and
backpressure waits remained zero; no device I/O error occurred.

Release-mode `cachectl hybrid-verify` passed the closed artifact. The manifest
was clean at generation 36 with an empty journal. Verification checked all
16,384 Bucket pages and 6,169 Region records, found zero invalid Bucket pages,
zero invalid/truncated Region headers, and zero Region issues, and reported
`clean_checkpoint` with `safe_to_open=true`.

The final revision passed all-feature tests: 324 library tests (321 passed and
3 ignored subprocess workers), 34 benchmark tests, 6 cachectl tests, and 19
integration tests. Rust 1.85 passed all-feature/all-target check and clippy with
warnings denied.

## Scope

This is one local APFS software-scale run. It validates behavior and informs
the next profile target, but it is not target-NVMe throughput, latency, thermal,
DWPD, power-loss, soak, or production qualification.
