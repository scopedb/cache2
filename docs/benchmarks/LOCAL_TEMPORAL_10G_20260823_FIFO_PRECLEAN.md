# Rejected experiment: 10 GiB temporal Hybrid FIFO preclean A/B

Date: 2026-08-23

## Disposition

This experiment is rejected and was never an acceptable FIFO implementation.
It prepared the oldest sealed victim before a concrete append required more
space, advancing the generation floor and evicting live values early. That
reduced effective cache capacity and violated the strict FIFO behavior test:
a value that should have survived until the next append-driven rotation became
a miss. The artifact remains useful only to quantify the upper bound from
overlapping victim scrub with foreground work.

On the same 10 GiB mixed-size temporal workload, the experiment added 8.6%
throughput over the remove-lane candidate and brought the cumulative gain over
the original global-remove baseline to 38.0%. Overall p99 and p99.9 stayed at
the same histogram buckets. There were no request errors, rejections, stale
values, reclaim fallbacks, or journal durability operations, but those gates do
not detect premature cache eviction. These numbers must not be used as an
accepted performance baseline.

| Metric | Remove-lane candidate | FIFO preclean | Change |
| --- | ---: | ---: | ---: |
| Operations | 6,786,985 | 7,368,263 | +8.6% |
| Throughput | 56,557.4 ops/s | 61,400.4 ops/s | +8.6% |
| Overall p99 | 2.359 ms | 2.359 ms | unchanged |
| Overall p99.9 | 7.340 ms | 7.340 ms | unchanged |
| Maximum API latency | 6.905 s | 3.216 s | -53.4% |
| Region backpressure wait | 37.007 s | 30.957 s | -16.3% |
| Region backpressure per operation | 5.453 us | 4.201 us | -22.9% |
| Region I/O completion average | 185.78 us | 110.38 us | -40.6% |
| Region I/O QD peak | 11 | 12 | +1 |
| Hit rate | 87.751% | 87.836% | +0.084 pp |
| Host writes | 51.588 GB | 57.400 GB | +11.3% |
| Admitted-value write ratio | 0.244x | 0.250x | +2.5% |
| Close/drain/checkpoint | 2.652 s | 3.706 s | +39.7% |

The run completed more work and host writes in the fixed interval, so the
close result is not a fixed-dirty-set comparison. Close latency remains a
separate optimization target.

## Reclaim evidence

- Measurement Region reuses: 1,606.
- Background victims prepared: 1,565, or 97.45% of reuses.
- Synchronous FIFO fallbacks: at most 41, inferred from the difference.
- Victim record headers scanned: 380,302, or 236.8 per reuse.
- Full-index corruption fallbacks: 0.
- Region I/O submitted/completed: 426,215 / 426,214 in the measurement
  snapshot and 608,992 / 608,992 after drain. One completion crossed the phase
  boundary; no I/O error was recorded.

The experiment deliberately does not remove scrub reads. It moves normal
scrub work outside the global state critical section while preserving exact
namespace retirement. A failed retirement, a non-zero index generation count,
or residual Region valid-byte accounting now prevents ready publication and
poisons the cache instead of silently publishing inconsistent accounting.

## Workload and command

The cache layout and workload are identical to
`LOCAL_TEMPORAL_10G_20260823_REMOVE_LANES.md`: 1 GiB Bucket, 9 GiB Region,
1 GiB Memory, 100,000 temporal keys, the
`256 B:45% / 4 KiB:25% / 64 KiB:20% / 1 MiB:10%` size mix, 70% reads,
16 async clients, four append lanes, and eight bounded write-back workers.
The run reached two physical lower-tier turnovers and 287 Region reuses before
the 120-second measurement window.

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-fifo-preclean/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-fifo-preclean/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-fifo-preclean/manifest.cache \
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

The artifact is
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-fifo-preclean`.
The measured binary contains `51fce50` and the schema-v6 metrics from
`150472c`. The eager-preclean design was superseded by append-demand rotation
in `1402508` and by O(1) single-namespace generation reclaim in `17b2f19`.

## Structural verification and semantic failure

Release-mode `cachectl hybrid-verify` passed:

- Manifest clean and valid; generation 36, empty journal, no recovery needed.
- 65,536 Bucket pages checked; zero invalid pages.
- 287 Region headers and 67,331 records checked; zero issues.
- Region disposition `clean_checkpoint`; `safe_to_open=true`.

Offline verification proves that the files are structurally consistent; it
does not make the implementation semantically valid. The strict FIFO
integration test demonstrated premature eviction, so the code was reverted.
See `LOCAL_TEMPORAL_10G_20260823_GENERATION_RECLAIM.md` for the accepted
append-demand design and its replacement A/B result.

This remains a local APFS buffered-I/O software scale result, not target-NVMe,
power-loss, thermal, soak, or production sign-off.
