# cache-rs milestones

Milestones describe maturity, not file-format or crate versions. Promotion is
based on exit criteria rather than dates.

- milestone labels are `M0` through `M4`;
- the crate follows normal `0.x` development until the `M4` API freeze;
- the disk format is independently versioned and is currently format `1`.

Before the first compatibility commitment, format-1 golden fixtures are the
source of truth and may be updated with the implementation. After `M4`, an
incompatible persisted-layout change must use a new format version. Runtime
configuration never participates in format versioning.

## M0 — HybridCache baseline

**Status:** complete

Deliver one simple bounded RAM + Region SSD architecture:

- [x] one public `HybridCache` API and one Region format;
- [x] shard-local L1, staging, ordering, and L2 ownership;
- [x] default POSIX positioned-I/O engine, explicit optional io_uring engine,
  and independently configurable read/write I/O worker counts;
- [x] static file-layout configuration separated from runtime tuning;
- [x] bounded L1 admission, I/O queues, buffers, staging, and logical disk use;
- [x] cold-empty recovery after crash or fast close;
- [x] clean recovery only after successful `close_warm`;
- [x] release-mode benchmark for put, L1, L2 promotion, and warm close;
- [x] format-1 golden fixtures and behavioral recovery tests.

Exit evidence: all unit and integration tests pass, the benchmark runs without
leaving files behind, and `peak_disk_bytes()` covers atomic image publication.

## M1 — Bounded alpha

**Status:** complete

**Goal:** make the current core small, explicit, and warning-free.

- [x] remove unused internal APIs and reach a warning-free
  `cargo check --all-targets`;
- [x] keep tunable defaults in one configuration layer and direct callers to
  general-purpose constants;
- [x] audit every cache-owned bulk allocation against the managed-memory plan;
- [x] explicitly account for configured worker/shard thread stack reservations,
  or expose them as a separate deterministic bound;
- [x] add boundary tests for skewed shards, externally retained values, maximum
  queue topology, and repeated Region turnover;
- [x] record a reproducible local benchmark baseline and comparison procedure;
- [x] keep README, architecture, and public API examples synchronized.

Exit criteria:

1. format, check, tests, and benchmark complete without warnings or leaked files;
2. managed-memory and logical-disk limits have executable regression tests;
3. changing runtime worker and queue settings never invalidates a clean image;
4. the production library contains no unused legacy path.

## M2 — Device-validated beta

**Status:** in progress

**Goal:** validate the implementation on the operating systems and devices it
is intended to serve.

- [x] add Linux and macOS CI for format, check, clippy, tests, the default POSIX
  build, and the optional io_uring feature;
- [x] provide one Linux NVMe qualification runner that captures hardware,
  POSIX buffered/direct medians, worker scaling, a bounded soak, and checksummed
  evidence;
- [x] run a RAM-backed Linux arm64 functional preflight for POSIX buffered and
  direct modes, optional io_uring/direct compatibility, worker scaling, and
  Region turnover;
- [x] validate warm recovery and both shutdown modes with a 100M-key fixed
  index in a swap-free, RAM-backed Linux VM;
- [ ] benchmark Linux NVMe with POSIX buffered and direct I/O;
- [x] measure worker scaling at 1, 2, 4, 8, and 16 workers with fixed workloads;
- [x] run external-process kill tests at open, write, drain, and warm-close
  publication boundaries;
- [ ] run capacity-turnover and mixed read/write soak tests while sampling RSS,
  managed bytes, logical disk bytes, latency, and error counts;
- [x] fuzz persistent decoders, record boundaries, and index probing;
- [x] run Miri or an equivalent checker over isolated unsafe mmap/index code.

Exit criteria:

1. restart returns either the last completely published clean image or an empty
   cache, never partially published data;
2. no workload causes managed memory, queues, or logical files to exceed their
   documented limits;
3. each supported I/O mode has a published reproducible benchmark result;
4. a multi-hour turnover soak has no unchecked or wrong-key read, hang, or
   monotonic resource growth.

## M3 — Operable release candidate

**Status:** implementation complete; workload canary pending

**Goal:** let an embedding service tune and operate the cache without depending
on internal implementation details.

- [x] expose a small snapshot API for L1/L2 hits and misses, promotions, bytes,
  queue saturation, I/O failures, and Region rotations;
- [x] expose managed-memory current/peak use and configured logical-disk peak;
- [x] expose on-demand queue, buffer, worker-I/O, L1/index pressure, and
  aggregate Region occupancy detail without adding latency instrumentation to
  the request path;
- [x] expose a compact lifecycle/health state for running, draining, miss-only,
  and terminal failure;
- [x] expose sequenced point deletes through the bounded mutation path with
  best-effort L1 cleanup and warm-recoverable L2 tombstones;
- [x] document overload behavior, shutdown choices, capacity planning, and
  benchmark reproduction;
- [x] validate configuration changes and graceful restart in an embedding-service
  integration test;
- [x] define a predeclared workload-canary evidence and pass/fail contract;
- [x] keep batching and async wrappers out until an integration or benchmark
  demonstrates that the core API is insufficient;
- [ ] complete a real workload canary with agreed latency and hit-rate targets.

Exit criteria:

1. an embedding service can detect saturation and failures using only public API;
2. operators can select shard, worker, queue, RAM, and disk settings from a
   documented capacity plan;
3. the release candidate completes a real workload canary with agreed workload
   latency and hit-rate targets.

## M4 — 1.0 compatibility freeze

**Status:** planned

**Goal:** make a narrow production commitment around the validated architecture.

- [x] audit and document the candidate public surface and compatibility policy;
- [ ] freeze the minimal public API and document its compatibility policy;
- [ ] freeze disk format 1 and require a format bump for incompatible changes;
- [x] document and test that unsupported or mismatched cache formats cold-start
  empty;
- [x] add release metadata, license file, changelog, and supported-platform list;
- [ ] publish reference hardware results and regression thresholds;
- [ ] complete at least one production canary and resolve release-blocking
  correctness, resource-bound, and operability findings.

Exit criteria: the crate can be upgraded, configured, monitored, gracefully
restarted, and safely discarded using only documented behavior.

## Non-goals

The roadmap does not include dirty-record replay, a write-ahead journal,
write-back durability, persistent L1 state, multiple storage engines, online
format migration, or a general-purpose database API. Defensive and security
hardening remain useful follow-up work, but do not replace the correctness,
resource, device-validation, and operability criteria above.
