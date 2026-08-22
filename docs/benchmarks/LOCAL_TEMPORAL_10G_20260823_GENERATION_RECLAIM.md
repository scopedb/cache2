# 10 GiB temporal Hybrid FIFO generation-reclaim A/B

Date: 2026-08-23

Behavior revision: `17b2f19`

Measured binary revision: `46a45fa`

## Result

The accepted FIFO path now waits for a concrete append to exhaust its Active
Region, selects the exact oldest victim, drains its readers, writes the ordered
Region headers, and retires a single-effective-namespace victim with one index
generation flip. The compact index maintains exact per-Region record bytes and
exact bytes for the effective namespace, so the common Hybrid configuration no
longer reads or parses the victim body. Multiple effective namespaces retain
the victim-local streaming scrub for exact attribution.

On the same 10 GiB mixed-size temporal workload, generation reclaim improved
throughput by 5.8% over the semantically valid on-demand scrub implementation
and by 4.1% over the earlier remove-lane baseline. Aggregate Region
backpressure fell by 19.4% from on-demand scrub, or 24.1% per operation. The
measurement completed 1,558 Region reuses with zero victim records scanned and
zero full-index fallbacks. There were no errors, rejections, or stale values.

| Metric | Remove-lane baseline | On-demand scrub | Generation reclaim | Change vs on-demand |
| --- | ---: | ---: | ---: | ---: |
| Operations | 6,786,985 | 6,673,947 | 7,085,833 | +6.2% |
| Throughput | 56,557.4 ops/s | 55,615.4 ops/s | 58,859.7 ops/s | +5.8% |
| Overall p99 | 2.359 ms | 2.359 ms | 2.359 ms | unchanged |
| Overall p99.9 | 7.340 ms | 7.340 ms | 6.816 ms | -7.1% |
| Maximum API latency | 6.905 s | 6.777 s | 3.209 s | -52.7% |
| Region backpressure wait | 37.007 s | 35.638 s | 28.716 s | -19.4% |
| Region backpressure per operation | 5.453 us | 5.340 us | 4.053 us | -24.1% |
| Region I/O completion average | 185.78 us | 311.18 us | 217.47 us | -30.1% |
| Region I/O QD peak | 11 | 10 | 13 | +3 |
| Measurement Region reuses | not recorded | 1,453 | 1,558 | +7.2% |
| Victim records scanned | not recorded | 342,019 | 0 | -100% |
| Hit rate | 87.751% | 87.801% | 87.834% | +0.033 pp |
| Host writes | 51.588 GB | 51.903 GB | 55.636 GB | +7.2% |
| Close/drain/checkpoint | 2.652 s | 1.784 s | 0.516 s | -71.1% |

The fixed-duration candidate completed more mutations, demotions, and Region
reuses, so host-write and close rows are not fixed-work comparisons. The local
APFS device also produced materially different Region I/O completion averages
across the three runs. The zero-scan invariant is deterministic; the measured
throughput delta still requires repeated target-NVMe confirmation.

The earlier eager-preclean artifact reached 61,400 ops/s, but it evicted the
oldest Region before append demand and violated strict FIFO capacity semantics.
It is documented only as a rejected experiment in
`LOCAL_TEMPORAL_10G_20260823_FIFO_PRECLEAN.md`, not as an accepted baseline.

## Reclaim evidence

- Measurement Region reuses: 1,558; total including premeasurement: 1,977.
- Background victims prepared: 0.
- Victim record headers scanned: 0 in measurement and 0 total.
- Full-index corruption fallbacks: 0.
- Measurement Region I/O submitted/completed: 411,304 / 411,304.
- Measurement Region backpressure: 28.716 s, or 4.053 us per operation.
- Measurement Region I/O QD peak: 13.
- Premeasurement reached 2.307 physical turnovers and 418 Region reuses.
- Software acceptance passed with zero request errors, rejects, or stale reads.

The index generation is the linearization point for victim visibility and byte
accounting. Concurrent replacement or expiry either removes its entry before
the flip and retires it separately, or loses to the flip and cannot retire it
again. Namespace retirement follows the flip and may briefly leave conservative
overcharge, but never undercharge. A failed retirement prevents topology
publication and enters the existing terminal failure path.

The tracked namespace is frozen from the effective controller at open. Hybrid
uses its delegated controller, not the Region engine's local default policy.
Entries restored from a namespace removed from the current configuration are
hidden by the generation flip but are not incorrectly charged to namespace 0.

## Workload and command

- 1 GiB Bucket, 9 GiB Region, and 1 GiB Memory tier.
- 100,000 circular temporal keys; the newest 2% receives 85% of reads.
- Sizes: 256 B (45%), 4 KiB (25%), 64 KiB (20%), 1 MiB (10%).
- 70% reads; 3% of mutations remove, 3% use TTL, and 8% cross tiers.
- Sixteen async clients, queue depth 64, four append lanes, and eight bounded
  write-back workers.
- Buffered I/O on the local APFS system SSD.
- Two lower-tier turnovers and at least 287 Region reuses before the 120-second
  measurement.
- Value templates are prebuilt outside individual cache API latency samples.

```sh
target/release/cache-bench hybrid \
  --bucket-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-generation-reclaim/bucket.cache \
  --bucket-capacity 1GiB --bucket-size 16KiB \
  --region-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-generation-reclaim/region.cache \
  --region-capacity 9GiB --region-size 32MiB \
  --manifest-path /Users/leiysky/cache-rs-bench/temporal-10g-20260823-generation-reclaim/manifest.cache \
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
`/Users/leiysky/cache-rs-bench/temporal-10g-20260823-generation-reclaim`.
The release binary SHA-256 is
`7254e17339e4116203ce80bad938b58e4c7c522cef5dfd35029980666eebb9fa`.

## Correctness evidence

Release-mode `cachectl hybrid-verify` passed:

- Manifest clean and valid; generation 36, empty journal, no recovery needed.
- 65,536 Bucket pages checked; zero invalid pages.
- 287 Region headers and 66,289 records checked; zero issues.
- Region disposition `clean_checkpoint`; `safe_to_open=true`.

The implementation passed all local checks:

- 307 lib tests passed and 3 subprocess workers were intentionally ignored;
  the executed suite includes the Region and combined Hybrid kill/restart
  harnesses.
- 34 benchmark tests, 6 cachectl tests, and 19 integration tests passed.
- `cargo clippy --all-targets --all-features -- -D warnings` passed.
- Regressions cover strict FIFO survival, single-vs-multiple effective
  namespaces, historical foreign namespace reopen, exact Region bytes across
  overwrite/tombstone/clear/recovery, caller-floor replacement, and MissOnly
  recovery metrics.

## Remaining work

This is a local buffered-APFS software scale gate, not target-NVMe, power-loss,
thermal, soak, or production sign-off. The benchmark JSON correctly reports
`hardware_qualification=false` and every external qualification flag false.

The next performance work should split the remaining 28.716 seconds of Region
backpressure by resource and repeat this exact workload on the target NVMe.
The Region I/O average remains 17.1% slower than the remove-lane run despite
being 30.1% faster than the noisy on-demand run, so one additional local sample
would not resolve device variance. Large-region-count observability can also
batch per-Region index-counter reads under one visibility lock, but that is not
currently established as a foreground bottleneck.
