# Local 10 GiB temporal Hybrid benchmark: managed Region staging

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `8839c7ee30e5455d17cbf96fa3c344e8da9132ac`  
Release `cache-bench` SHA-256: `b5e3d21e85955702042edb26960940aadc1bcf48401529d933081c6571e39228`  
Release `cachectl` SHA-256: `ddb6b5d785516e8cafaa8ea0910847b7272884ae1eb6b66b4876ecd46fd39b1d`

## Result and decision

Keep the bounded managed-Hybrid Region staging path. It changes buffered
Region demotion from one synchronous device completion per small append group
to early publication from bounded DRAM followed by lane-local sequential
flushes of up to 4 MiB. Each lane has fixed Active and Flushing resident
chunks plus one aligned I/O buffer, all charged to the Region memory budget.
Reads resolve volatile index entries from resident bytes. A successful CQE
only clears the exact entry's runtime flag; it never republishes an entry that
was removed, replaced, or cleared.

Against the immediately preceding volatile-remove revision (`482ace6`), the
same 10 GiB temporal workload reduced Region write batches from 195,359 to
18,147 (**-90.71%**) and Region I/O submissions from 298,553 to 130,838
(**-56.18%**). The average number of Region records represented by one physical
write batch rose from 1.49 to 17.58, while the average logical record bytes per
batch rose from 282 KiB to 3.27 MiB. Region I/O submission wait fell 57.12%.

Throughput reached **62,086.0 ops/s**, a modest 0.59% improvement. p99 remained
1.835 ms, p99.9 improved 9.09%, and maximum observed latency improved 30.38%.
The local APFS run is therefore strong evidence that the former small-write
architecture has been removed, but not evidence that Region device I/O was the
remaining throughput limiter on this machine.

The candidate wrote 10.29% more Region bytes and 10.31% more host bytes in this
fixed-duration run, while admitted value bytes rose only 0.68%. Software write
amplification moved from 0.264 to 0.289. The candidate also performed 10.26%
more Region reuses, and its premeasurement cache state differed from the prior
single run (2.200 versus 2.506 physical turnovers). This is not a fixed-work
write-amplification comparison. It does show that the next optimization needs
visibility into staged live/obsolete bytes and fill ratio; batching alone does
not reduce logical churn.

| Metric | Volatile remove | Region staging | Change |
|---|---:|---:|---:|
| Revision | `482ace6` | `8839c7e` | — |
| Throughput | 61,721.2 ops/s | 62,086.0 ops/s | +0.59% |
| Operations | 7,406,750 | 7,450,416 | +0.59% |
| Hit rate | 88.002% | 88.307% | +0.305 pp |
| p50 | 114.687 us | 114.687 us | unchanged |
| p99 | 1.835 ms | 1.835 ms | unchanged |
| p99.9 | 5.767 ms | 5.243 ms | -9.09% |
| Maximum latency | 5.879 s | 4.093 s | -30.38% |
| Region control-queue wait | 0 | 0 | unchanged |
| Region I/O submitted/completed | 298,553 | 130,838 | -56.18% |
| Region I/O per 1,000 operations | 40.308 | 17.561 | -56.44% |
| Region write batches | 195,359 | 18,147 | -90.71% |
| Region write batches per 1,000 operations | 26.376 | 2.436 | -90.76% |
| Records coalesced | 94,969 | 300,927 | +216.87% |
| Average records per write batch | 1.49 | 17.58 | 11.83x |
| Average logical Region bytes per batch | 282 KiB | 3.27 MiB | 11.87x |
| Region I/O submit wait | 1.980 s | 0.849 s | -57.12% |
| Region I/O completion average | 177.76 us | 601.18 us | +238.19% |
| Region bytes written | 56,403,103,488 | 62,208,962,720 | +10.29% |
| Region bytes written per operation | 7,615.1 B | 8,349.7 B | +9.65% |
| Host bytes written | 60,887,912,192 | 67,162,496,320 | +10.31% |
| Host bytes written per operation | 8,220.6 B | 9,014.6 B | +9.66% |
| Admitted value bytes | 230,293,547,008 | 231,863,354,880 | +0.68% |
| Software write amplification | 0.264 | 0.289 | +0.025 |
| Region reuses | 1,706 | 1,881 | +10.26% |

The measurement completed 2,291,850 writes and 65,318 explicit removes. It
reported zero request errors, overload rejections, write-back failures, stale
values, or Region admission queue/buffer waits. Region and Bucket I/O queue-depth
peaks were 13. The steady-state gate completed in 40.013 seconds and reached
2.200 premeasurement physical turnovers and 384 Region reuses.

## Command

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-region-staging/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-region-staging/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-region-staging/manifest.cache \
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

The complete cache artifact remains at
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-region-staging`.

## Correctness and lifecycle evidence

The release benchmark reported `acceptance_passed=true`, zero errors, zero
rejections, and zero stale values. Its final drain left no dirty write-back
entries and an empty Hybrid journal. Release-mode `cachectl hybrid-verify`
reported:

- clean manifest generation 36, empty journal, and no recovery required;
- all 65,536 Bucket pages valid, with zero invalid pages;
- all 287 Region headers valid and 48,560 records verified;
- zero Region issues, `clean_checkpoint`, and `safe_to_open=true`.

Before the benchmark, the implementation passed both the active toolchain and
Rust 1.85 versions of `test --all-targets --all-features` and
`clippy --all-targets --all-features -- -D warnings`. The executed suites
contained 320 library tests (317 passed, 3 ignored subprocess cases), 34
benchmark tests, 6 cachectl tests, and 19 integration tests.

New behavior coverage proves that managed puts remain readable without any
Record I/O, multiple resident puts become one physical flush, an in-memory
remove cannot be republished by the CQE, a staging EIO clears the runtime index
and keeps Region in MissOnly, degraded Hybrid lower tiers fail closed, and
managed `close()` remains idempotent. The checkpoint codec independently
rejects a volatile runtime entry.

## Scope and next work

This is a software-scale gate on local APFS buffered I/O. It is not target-NVMe,
thermal, soak, or power-loss qualification. The next local step is to expose
staging fill ratio, resident/flushing bytes, live bytes at seal, and obsolete
bytes at CQE. Those metrics will distinguish workload churn from avoidable
staged writes before changing the physical format or adding sparse live-run
writes. The same implementation must then be measured on target NVMe, where
the 90.71% reduction in write submissions should have a materially different
throughput and CPU profile.
