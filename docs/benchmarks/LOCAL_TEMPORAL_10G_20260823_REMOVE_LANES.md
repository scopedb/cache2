# 10 GiB temporal Hybrid remove-lane A/B

Date: 2026-08-23

## Result

A 10 GiB steady-state mixed-size temporal workload identified the Region
remove path as an architecture-level serialization point. The baseline made
every Region remove acquire one global reserved control permit before routing
the command to one of four append lanes. Explicit removes and Hybrid
cross-tier cleanup therefore serialized even when their keys belonged to
different lanes.

The candidate sizes the reserved control permits to `append_lanes` and keeps
two bounded scratch buffers per admitted control request. The existing shared
operation barrier remains held for the complete remove, so `flush`, `clear`,
and `close` retain exclusive fencing semantics. Memory allocation remains
subject to the configured `MemoryTracker` hard limit.

With identical workload and cache parameters, the observed throughput rose by
27.1%, aggregate Region backpressure wait fell by 93.8%, overall p99 fell by
35.7%, and write/remove p99 fell by 53.3%. Both runs passed their software
acceptance gates with zero errors, rejections, or stale values.

| Metric | Global control permit | Per-lane control permits | Change |
| --- | ---: | ---: | ---: |
| Operations | 5,338,049 | 6,786,985 | +27.1% |
| Throughput | 44,482.9 ops/s | 56,557.4 ops/s | +27.1% |
| Overall p99 | 3.670 ms | 2.359 ms | -35.7% |
| Overall p99.9 | 11.534 ms | 7.340 ms | -36.4% |
| Read p99 | 1.049 ms | 1.442 ms | +37.4% |
| Write/remove p99 | 7.864 ms | 3.670 ms | -53.3% |
| Region backpressure wait | 600.526 s | 37.007 s | -93.8% |
| Region I/O completion average | 156.79 us | 185.78 us | +18.5% |
| Region I/O QD peak | 9 | 11 | +2 |
| Records per Region write batch | 1.248 | 1.263 | +1.2% |
| Host writes | 48.788 GB | 51.588 GB | +5.7% |
| Admitted-value write ratio | 0.293x | 0.244x | -16.7% |
| Hit rate | 88.254% | 87.751% | -0.503 pp |
| Close/drain/checkpoint | 1.506 s | 2.652 s | +76.1% |

The read-p99 and close regressions remain important. This is one local A/B,
not a statistical or target-NVMe qualification. The candidate completed 27%
more work in the same interval and drove Region I/O to a higher queue depth,
so latency rows do not represent a fixed offered-load comparison. A later
throughput-capped latency run should separate queueing effects from intrinsic
read latency.

## Workload

- 1 GiB Bucket, 9 GiB Region, 1 GiB Memory tier.
- 100,000 keys with a circular temporal timeline; the newest 2% receives 85%
  of reads.
- Sizes: 256 B (45%), 4 KiB (25%), 64 KiB (20%), 1 MiB (10%).
- 70% reads; 3% of mutations are removes, 3% use TTL, and 8% are cross-tier
  updates.
- Sixteen async clients, queue depth 64, four append lanes, eight write-back
  workers, and a 128-entry write-back queue.
- Buffered I/O on the local APFS system SSD.
- Two physical lower-tier turnovers and at least 287 Region reuses before the
  120-second measurement window.
- Individual cache API calls are timed; value generation uses prebuilt worker
  templates and is outside the latency sample.

The exact candidate command was:

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-remove-lanes/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-remove-lanes/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-remove-lanes/manifest.cache \
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

The baseline artifact is
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-owner-fenced` at revision
`7038a24`. The candidate artifact is
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-remove-lanes`; its binary
contained the performance code in `86bd602`, subsequently amended as
`522aa90` only to correct an API documentation comment.

## Correctness evidence

The change includes a deterministic M3 regression test. It selects two keys
in distinct ordering stripes and append lanes, blocks both tombstone record
writes in the backend, proves that both enter concurrently, then closes and
reopens the cache and verifies both keys remain misses.

Release-mode `cachectl hybrid-verify` passed the candidate files:

- Manifest clean and valid; generation 36, empty journal, no recovery needed.
- 65,536 Bucket pages checked; zero invalid pages.
- 287 Region headers and 67,991 records checked; zero issues.
- Region disposition `clean_checkpoint`; `safe_to_open=true`.

## Remaining architecture work

The remaining 37 seconds of aggregate Region backpressure is not attributable
to one resource because the current counter combines seven gates and buffer
pools. Split wait accounting by resource before tuning another concurrency
constant.

At the time of this run, Region reuse also performed a synchronous record/index
scrub while holding the global state mutex and the victim Region write guard.
It did not zero or rewrite the Region body, but an offline analysis of the
baseline artifact estimated about 8.6 GiB of unreported scrub reads during its
measurement window. Eager background preclean in `51fce50` improved the local
benchmark but violated strict FIFO capacity semantics and is retained only as a
rejected experiment in `LOCAL_TEMPORAL_10G_20260823_FIFO_PRECLEAN.md`. The
accepted append-demand generation reclaim result is recorded in
`LOCAL_TEMPORAL_10G_20260823_GENERATION_RECLAIM.md`.

Do not increase write-back workers from 8 to 32 yet: the candidate had zero
write-back queue wait or rejection, while Region I/O QD already rose to 11.
Additional consumers would not address the remaining measured bottleneck.
