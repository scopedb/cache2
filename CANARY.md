# Workload canary evidence

The M3/M4 canary validates an embedding service, not an isolated cache
microbenchmark. Complete this record before calling a deployment production
ready. Thresholds are chosen before the run and cannot be retrofitted from the
observed result.

## Identity

- cache-rs revision:
- service revision:
- environment and host class:
- cache device and filesystem:
- static configuration:
- runtime configuration, including whether statistics are enabled:
- baseline window:
- canary window and traffic percentage:

## Predeclared gates

| Signal | Baseline | Required canary result | Observed | Pass |
|---|---:|---:|---:|:---:|
| service p50 latency | | | | |
| service p95 latency | | | | |
| service p99 latency | | | | |
| service error rate | | | | |
| origin bytes/request rate | | | | |
| cache hit rate after warm-up | | | | |
| cache queue saturation | | | | |
| cache buffer saturation | | | | |
| cache I/O failures | | 0 | | |
| managed-memory peak/budget | | <= 100% | | |
| logical-disk peak/bound | | <= 100% | | |

## Required scenarios

1. Establish the cache-disabled or previous-release baseline on comparable
   traffic and hardware.
2. Cold-start the candidate and observe origin load until hit rate stabilizes.
3. Hold steady canary traffic for the predeclared duration, including normal
   capacity turnover.
4. Exercise the configured overload policy and verify the service remains
   correct when cache operations reject, time out, or miss.
5. Stop admission, execute `close_warm`, restart with a permitted runtime-only
   configuration change, and verify warm recovery plus continued correctness.
6. Execute one fast/unclean restart and verify cold-empty recovery without stale
   values or an origin failure storm outside the declared service gate.

## Decision

- all predeclared gates passed:
- stale or incorrect cache values observed:
- unexplained memory, RSS, disk, or latency growth:
- unresolved cache-related findings:
- owner and approval date:

Attach raw service telemetry and `HybridCache::snapshot()` samples for the full
baseline and canary windows. A missing signal or shortened scenario is an
incomplete canary, not a pass.
