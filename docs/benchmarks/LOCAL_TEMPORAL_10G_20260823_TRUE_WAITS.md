# Local 10 GiB temporal Hybrid benchmark: true Region waits

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification  
Revision: `1b075eb5395c485134898ad2e10026d939f07d54`  
Release `cache-bench` SHA-256: `596c8c38b8bb15d8aa27494f87c4bfe57a3e9e8c6107ceba7ad4969dd14e6019`

## Result

This run changes Region wait accounting so an uncontended gate or buffer
acquisition contributes zero. It also removes the unconditional clock reads and
atomic additions from those fast paths. The same mixed-size temporal workload
reached **61,230.1 ops/s**, 4.03% above the generation-reclaim run at 58,859.7
ops/s. There were no errors, overload rejections, or stale reads, and offline
verification passed.

The split counters establish one unambiguous next bottleneck: all **41.540 s**
of measured Region resource waiting came from the four-slot control queue.
Read queue, write queue, read buffer, write buffer, control buffer, and metadata
buffer waits were all exactly zero. The old 28.716 s aggregate is not directly
comparable because it included uncontended acquisition overhead.

| Metric | Generation reclaim | True-wait run | Change |
|---|---:|---:|---:|
| Throughput | 58,859.7 ops/s | 61,230.1 ops/s | +4.03% |
| Operations | 7,085,833 | 7,347,786 | +3.70% |
| Hit rate | 87.834% | 87.932% | +0.099 pp |
| p50 | 114.687 us | 114.687 us | unchanged |
| p99 | 2.359 ms | 2.359 ms | unchanged |
| p99.9 | 6.816 ms | 6.816 ms | unchanged |
| Maximum latency | 3.209 s | 3.142 s | -2.07% |
| Region I/O completion average | 217.47 us | 89.36 us | -58.91% |
| Region I/O submitted/completed | 411,304 | 437,722 | +6.42% |
| Region reuses | 1,558 | 1,668 | +7.06% |

Measured Region waits:

| Resource | Wait |
|---|---:|
| read queue | 0 |
| write queue | 0 |
| control queue | 41.540 s |
| read buffer | 0 |
| write buffer | 0 |
| control buffer | 0 |
| metadata buffer | 0 |

The control-queue wait was 5.653 us per completed cache operation. The workload
issued 64,359 explicit removes; a remove holds one control permit while it
reads the prior Region value, waits for its append lane, and publishes the
tombstone. A shared four-permit gate therefore permits same-lane requests to
occupy capacity needed by otherwise idle lanes, creating head-of-line blocking.

Other measured evidence:

- Region write batches: 305,893; records coalesced: 84,708.
- Region bytes read/written: 18,885,511,344 / 55,149,453,856.
- Region I/O submit wait: 4.379 s; Region completion time: 39.113 s.
- Region and Bucket I/O queue-depth peaks: 13 / 11.
- Measurement Region victim scans and index fallbacks: 0 / 0.
- Host writes: 59,489,264,160 bytes; admitted values: 229,327,083,008 bytes;
  software write amplification: 0.259.
- Premeasurement reached 2.352 physical turnovers and 430 Region reuses in
  53.089 s.

## Command

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-true-waits/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-true-waits/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-true-waits/manifest.cache \
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
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-true-waits`.

## Correctness evidence

Release-mode `cachectl hybrid-verify` passed after clean close:

- Manifest generation 36 was clean, with an empty journal and no recovery
  required.
- All 65,536 Bucket pages were valid.
- All 287 Region headers and 66,861 records were valid; zero issues.
- Region disposition was `clean_checkpoint`; `safe_to_open=true`.

The implementation also passed `cargo test --all-targets --all-features`,
`cargo clippy --all-targets --all-features -- -D warnings`, and
`cargo +1.85 check --all-targets --all-features`.

## Next target

Replace the shared control gate with lane-affine admission so a request can
reserve only its actual append lane and cannot consume another lane's permit.
This preserves the total four-request/eight-buffer hard bounds. After that
local fix, the larger throughput milestone remains Navy-style lane-local Region
staging buffers: publish from bounded DRAM and flush large sequential chunks in
the background instead of waiting for every small write batch.

