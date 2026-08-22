# 10 GiB temporal Hybrid lane-admission experiments

Date: 2026-08-23  
Platform: local macOS/APFS buffered I/O; not target-NVMe qualification

## Decision

Keep the shared Region control gate sized to `append_lanes`. Two lane-partitioned
admission variants were correct but slower under the same mixed-size temporal
workload:

- one permit per lane removed the existing per-lane command pipeline and lost
  6.21% throughput;
- two permits per lane restored one queued request, but its statically
  partitioned spare capacity still lost 4.43% throughput.

The result falsifies the earlier hypothesis that cross-lane head-of-line
blocking in the shared control gate was the dominant remaining bottleneck. The
shared gate lets whichever append lane is ready consume the bounded global
capacity, which is more valuable for this uniformly hashed workload than strict
lane isolation.

| Metric | Shared: 4 total | Strict: 1/lane | Pipelined: 2/lane |
| --- | ---: | ---: | ---: |
| Revision | `1b075eb` | `4df2185` | `7492aff` |
| Throughput | 61,230.1 ops/s | 57,426.5 ops/s | 58,520.4 ops/s |
| Change from shared | baseline | -6.21% | -4.43% |
| Operations | 7,347,786 | 6,891,347 | 7,022,558 |
| Hit rate | 87.932% | — | 87.926% |
| p50 | 114.687 us | 114.687 us | 122.879 us |
| p99 | 2.359 ms | 2.359 ms | 2.359 ms |
| p99.9 | 6.816 ms | 9.437 ms | 7.340 ms |
| Maximum latency | 3.142 s | 3.484 s | 3.488 s |
| Control-gate wait | 41.540 s | 189.717 s | 55.977 s |
| Control-gate wait/op | 5.653 us | 27.530 us | 7.971 us |
| Region I/O completion average | 89.36 us | 243.04 us | 170.14 us |
| Region I/O completions | 437,722 | 420,378 | 419,029 |
| Region reuses | 1,668 | 1,611 | 1,586 |

All three runs completed with zero errors, overload rejections, and stale
reads. Release-mode offline Hybrid verification passed on every artifact with a
clean manifest and zero Bucket or Region issues. The I/O timing varied
substantially between these local APFS runs, so it is supporting evidence rather
than a fixed device comparison; both lane-partitioned candidates nevertheless
regressed the end-to-end result they were intended to improve.

## Artifacts

- Shared baseline:
  `/Users/leiysky/cache-rs-bench/temporal-10g-20260823-true-waits`
- Strict one-per-lane candidate:
  `/Users/leiysky/cache-rs-bench/temporal-10g-20260823-lane-affine`
- Two-per-lane candidate:
  `/Users/leiysky/cache-rs-bench/temporal-10g-20260823-lane-pipeline`

The workload and exact command are recorded in
`LOCAL_TEMPORAL_10G_20260823_TRUE_WAITS.md`; only the output paths and tested
revisions changed.

## Next architecture target

Do not tune the control-gate constant again. The remaining structural cost is
that a foreground Region mutation owns bounded request buffers and waits for a
small append batch to reach device completion before publishing its result.
The next experiment should introduce bounded lane-local Region staging:
foreground callers encode into DRAM, full chunks flush sequentially in the
background, and reads route explicitly between resident, flushing, and
on-device locations. Cache contents may be lost after a crash, but the design
must never expose a stale value within a running process.
