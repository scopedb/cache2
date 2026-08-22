# Upgrade and rollback

cache-rs stores disposable cache data, but an upgrade must still never return a
wrong or deleted value. This document defines the v1.1 single-device policy.

## Compatibility boundary

The base data layout remains Format V1:

- two Superblocks;
- fixed Region Headers;
- append-only records with checksummed headers and payloads.

The checkpoint tail has its own codec version. v1.1 reads checkpoint versions
1 through 4. Version 3 persists append-lane identity in a previously reserved
Region snapshot byte; this closes multi-lane tombstone ordering ambiguity while
leaving the base Format V1 extent unchanged. Version 4 additionally records the
source index capacity, shard count, and physical slot of each visible entry, so
reopening with the same `index_slots` and `index_shards` reproduces a
bounded-probe table without order-dependent extra eviction. Older v1--v3
checkpoints remain readable through their legacy
safe insertion path. A missing or invalid clean
checkpoint falls back to validated Format V1 scanning. An unusable dirty
baseline is rebuilt empty.

Current record codecs are:

| Codec | Meaning |
| --- | --- |
| 0 | default namespace value/tombstone |
| 1 | namespaced value/tombstone |
| 2 | durable second-chance default-namespace record |
| 3 | durable second-chance namespaced record |

New binaries must continue reading all earlier supported V1/checkpoint/record
variants. Older binaries are not guaranteed to understand records or checkpoint
metadata written by a newer binary. They may reject or safely clear the cache;
they must not be used as an in-place rollback plan.

In particular, a v1.0 binary is not a supported downgrade target after v1.1
has written a v4 checkpoint, used more than two append lanes, or created a
checkpoint tail larger than that binary's former limit. Use the retained old
path or a new empty path.

Hybrid identity spans three files: the fixed-Bucket file, Region Format V1
file, and global manifest/journal. Treat them as one upgrade and rollback unit.
Never copy, restore, replace, or reuse only one member. A missing/recreated
manifest deliberately fences pre-existing lower-tier state instead of guessing
that the files still belong together.

The Hybrid manifest remains version 1. New writers may populate a separately
checksummed, fixed-size namespace-usage extension in bytes that older V1 files
left zero. New readers accept the all-zero legacy representation, perform one
lower-tier usage scan, and publish the extension. A clean checkpoint whose
namespace-id set exactly matches configuration reopens without a data scan;
dirty recovery always reconstructs usage from the reconciled lower tiers.

## Rolling upgrade

1. Build and run Rust 1.85 `test` and `clippy` for the exact release artifact.
2. Run `cachectl inspect` and `cachectl verify` after closing a representative
   old cache.
   For Hybrid, run `cachectl hybrid-inspect` and `cachectl hybrid-verify` on the
   complete three-file set.
3. Provision a new dedicated path for the new binary. Keep the old binary and
   old path unchanged.
4. Run configuration diagnostics and the target-NVMe benchmark gates.
5. Start the new process at 1% traffic, then use the canary sequence in
   [OPERATIONS.md](OPERATIONS.md).
6. Retain the old path until the rollback window expires.

Opening an old path directly is supported for forward upgrade, but a separate
path is preferred because it makes rollback immediate and does not couple the
release to cache warm-up state.

## Rollback

Route traffic to the retained old process and old cache path. If that path is
not available, start the old binary with a new empty path. Never point an older
binary at a path that a newer binary has mutated unless that exact downgrade
pair has a committed compatibility test.

Cache contents are never copied back as authoritative data. If an upgrade
fails, preserving origin correctness and controlling the miss storm take
priority over retaining cache hit rate.

## Configuration and capacity

Persistent reopen settings are effective Region count, Region size, hash seed,
and append-lane count. Runtime settings such as index slots, object admission
limits, queue depths, memory budget, I/O engine/mode, recovery mode, and policy
budgets may change subject to validation.

Changing `index_slots` or `index_shards` intentionally gives up exact
physical-slot replay: the v4 entries are safely reinserted into the new table
and capacity/probe pressure may turn some cache hits into misses. It cannot make
a wrong or deleted value visible. Keep both values unchanged when hit-rate
continuity matters.

Capacity changes use a new file. No release performs in-place Region-count
growth, shrink, or data migration. This keeps format rollback and crash
reasoning simple.

For Hybrid, capacity, Bucket page size, routing threshold, layout identity, or
journal changes use three new empty paths. Run `hybrid-diagnose`, then
`hybrid-format --yes`; there is intentionally no Hybrid reset command. Retain
the previous three-file set through the rollback window.

## Release evidence

Archive for each production candidate:

- source revision, Rust version, target triple, Cargo feature set, and binary
  checksum;
- unit/integration/crash/failpoint results;
- old-format fixture and forward-upgrade results;
- `cachectl inspect/verify` or complete `hybrid-inspect/hybrid-verify` reports;
- target-NVMe benchmark and 24–72 hour soak output;
- CPU/RSS/block-device/SMART profiles;
- declared SLO, recovery SLA, origin guard, and DWPD thresholds;
- canary timeline, alerts, and rollback decision record.

Real device throughput, p99, power-loss behavior, DWPD, and production-origin
miss-storm limits are deployment sign-offs. They cannot be certified by source
tests on an unrelated workstation.
