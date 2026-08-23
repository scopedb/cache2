# Local fixed-size Hybrid scale baseline — 2026-08-23

## Scope

- Revision: `955473b`
- Host: Apple M4 Max, 64 GiB RAM
- OS: macOS 26.5.2 (25F84), internal APFS
- I/O: `engine=auto`, `mode=buffered`
- API: async facade with one blocking outstanding request per client
- Write policy: write-through, to isolate each lower data path
- Workload: uniform 80/20 read/write, no measured remove/TTL/cross-tier
- Fixed Bucket value: 992 B; 32 B generated key makes the complete route size
  exactly 1 KiB
- Fixed Region value: 1 MiB

This is a local software-scale regression, not target-NVMe qualification. Each
row used a fresh three-file Hybrid cache, completed the built-in semantic gate,
closed cleanly, reopened with the same configuration, sampled the current
version state, closed again, and passed offline `cachectl hybrid-verify`.

## Results

| Active tier / capacity | Keys | Prefill | Ops/s | p50 | p99 | p99.9 | Max | Read / write MiB/s | Hit | Active QD | Measured turnover | Close |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Bucket 256 MiB | 400,000 | 5.84 s | 88,191 | 197 us | 459 us | 655 us | 8.92 ms | 38 / 17 | 57.2% | 16 | 16.15x | 147 ms |
| Region 256 MiB | 512 | 0.55 s | 4,427 | 20 us | 12.58 ms | 16.78 ms | 22.62 ms | 1,095 / 875 | 30.8% | 7 | 51.33x | 116 ms |
| Bucket 1 GiB | 1,500,000 | 19.77 s | 97,029 | 328 us | 1.18 ms | 2.36 ms | 125 ms | 45 / 18 | 61.0% | 32 | 8.89x | 311 ms |
| Region 1 GiB | 2,048 | 1.32 s | 5,967 | 53 us | 6.82 ms | 10.49 ms | 16.45 ms | 1,704 / 1,195 | 35.7% | 7 | 35.01x | 273 ms |
| Bucket 10 GiB | 15,000,000 | 575.02 s | 71,588 | 328 us | 1.18 ms | 2.62 ms | 1.24 s | 33 / 14 | 61.0% | 32 | 1.31x | 340 ms |
| Region 10 GiB | 20,480 | 23.34 s | 4,006 | 27 us | 6.82 ms | 10.49 ms | 7.37 s | 1,218 / 803 | 38.0% | 11 | 4.71x | 888 ms |

The 256 MiB rows used a 15 second measurement and a 0.25x pre-measure target.
The 1 GiB rows used 30 seconds and a 1x target. The 10 GiB rows used 60 seconds
and a 1x target. The Region rows also exceeded a complete Region reuse cycle;
the measured reuse counts at 256 MiB, 1 GiB, and 10 GiB were 423, 1,156, and
1,554 respectively.

## Correctness and lifecycle

All six rows reported:

- `acceptance_passed=true`
- zero errors, stale values, rejected writes, request rejections, I/O errors,
  and write-back/journal failures
- exact latency-sample accounting
- zero final dirty entries and bytes
- clean manifest and lower tiers after the second close
- `safe_to_open=true`, `region_issues_total=0`, and
  `region_reopen_disposition=clean_checkpoint`

Route isolation was exact: Bucket rows recorded no Region data read/write, and
Region rows recorded no Bucket data read/write. Offline verification covered:

| Row | Bucket pages / entries | Region records | Invalid pages / Region issues |
| --- | ---: | ---: | ---: |
| Bucket 256 MiB | 16,384 / 229,102 | 0 | 0 / 0 |
| Region 256 MiB | 2,048 / 0 | 212 | 0 / 0 |
| Bucket 1 GiB | 65,536 / 915,107 | 0 | 0 / 0 |
| Region 1 GiB | 2,048 / 0 | 923 | 0 / 0 |
| Bucket 10 GiB | 655,360 / 9,151,113 | 0 | 0 / 0 |
| Region 10 GiB | 2,048 / 0 | 9,844 | 0 / 0 |

Clean-reopen verification sampled 10,000 / 512 / 50,000 / 2,048 / 100,000 /
20,000 keys for the rows in table order. Every hit contained the current
key/version payload; an expected live entry was allowed to be a miss because
the cache is disposable. No removed or expired value was resurrected.

## Observations

1. Fixed routing and lifecycle behavior are correct through 10 GiB, including
   full Region reuse and full offline scans.
2. Bucket steady-state throughput fell about 26% from 1 GiB to 10 GiB, while
   p99 stayed at 1.18 ms. The 10 GiB first prefill took 575 seconds, but its
   separate 1x steady-state preparation took only 41 seconds. Initial
   high-entry Bucket population is therefore a distinct scaling cost from the
   steady-state update path.
3. Region throughput fell from 5,967 to 4,006 ops/s between 1 GiB and 10 GiB;
   combined value bandwidth fell from about 2.90 GiB/s to 2.02 GiB/s while p99
   stayed at 6.82 ms.
4. The 10 GiB rows were executed with the APFS data volume at 98–99% usage.
   Their 1.24 s and 7.37 s maximum-latency outliers must be rerun after freeing
   space before they are attributed to cache architecture.
5. Before a 100 GiB fixed-size run, profile the Bucket prefill path and retain
   full JSON output. The next capacity rows must run with enough free space to
   avoid filesystem-pressure contamination.
