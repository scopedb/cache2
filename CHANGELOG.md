# Changelog

All notable changes to cache-rs are recorded here. The crate follows semantic
versioning for its Rust API. The disposable disk format has an independent
version number documented in `MILESTONES.md`.

## Unreleased

### Added

- bounded RAM + Region SSD `HybridCache` with shard-local staging and L1;
- POSIX positioned-I/O and optional Linux io_uring engines with runtime worker
  tuning;
- fast cold restart and graceful warm-image recovery;
- deterministic managed-memory and logical-disk bounds;
- opt-in operational snapshots, release benchmarks, turnover soak, persistent
  format fuzzing, and external-process crash tests.
- stable machine-readable benchmark results, explicit performance gates, a
  Linux NVMe qualification runner, and a workload-canary evidence contract.
- a bounded benchmark generator with signed values and independent L1-resident
  and larger-than-RAM L2 data sets.
- a candidate API/disk-format compatibility policy and extensible public
  operational enums before the M4 freeze.
- a bounded recovery-scale benchmark covering fresh/warm open, initial and
  recovered warm close, sentinel validation, file bounds, and fast close.
- shard-local CLOCK RAM eviction with shared byte bounds, immediate eviction
  eligibility, fixed scan budgets, and collision-safe lookup.
- static namespace-to-RegionSet routing with weighted physical Region ranges,
  independent FIFO rotation, deterministic append-shard assignment, and warm
  recovery without a second disk format.
- on-demand detailed snapshots for buffer pressure, aggregate worker
  I/O, fixed/resident/retained L1 memory, index occupancy/replacement pressure,
  and per-RegionSet capacity, occupancy, and process-local rotations.
- sequenced point deletion through the existing bounded mutation path, with
  best-effort exact-key L1 cleanup and warm-recoverable 24-byte index
  tombstones.
- optional explicit Tokio runtime-handle binding for cache lifecycle work and
  L2 read deadlines.

### Changed

- made POSIX positioned I/O the only default engine, renamed `IoEngine::Sync`
  to `IoEngine::Posix`, removed implicit `IoEngine::Auto` selection, and made
  io_uring an explicit opt-in crate feature;
- removed the public `io_concurrency` setting; read and write worker counts now
  independently bound dedicated POSIX pools, while optional io_uring uses a
  fixed 64-request depth per configured worker in each pool;
- made `get` and `get_in` native Tokio async operations; L1/index-miss paths
  remain immediate, while an admitted L2 read wakes the caller task directly;
- made `open`, `drain`, and explicit close Tokio-friendly: drain uses native
  shard notifications and blocking recovery/filesystem work stays off runtime
  workers;
- separated read and write submissions into dedicated bounded worker pools so
  write pressure cannot consume read slots; optional multi-ring io_uring reads
  retain one bounded alternate-lane probe;
- stopped requiring the unused io_uring `fsync` opcode after the public data-sync
  operation was removed;
- replaced FNV-1a key hashing with seeded XXH3-64 and bound the algorithm
  identity into static recovery compatibility;
- reset the green-field disk layout to format 1 and removed legacy engines,
  journals, manifests, metrics stacks, versioned duplicates, and migration
  paths;
- renamed lane ownership to shard ownership throughout the public architecture;
- made intact unknown cache-format versions cold-start empty because cached
  data is disposable and never authoritative.
- made the resident-L1 benchmark explicitly prepare its measured key subset so
  CLOCK eviction schedules cannot make the tier assertion nondeterministic.
- removed the non-observable `OverloadReason` export; public overload remains
  `std::io::ErrorKind::WouldBlock` plus snapshot saturation counters.
- fixed Linux arm64 symbolic-link rejection by using architecture-correct libc
  values for `O_NOFOLLOW` and `O_NONBLOCK` instead of x86 constants.
- removed time-based expiration from the API, memory tier, request path, and
  record format; cache lifetime is governed only by eviction and explicit delete.
- replaced the capacity-overwriting cache builder path with
  `HybridCacheConfig::from_static`, exposed validated RegionSet allocations,
  and canonicalized physically equivalent default RegionSet configurations.
- renamed the ambiguous snapshot `read_bytes` counter to `served_bytes` to
  distinguish bytes returned to callers from underlying device I/O bytes.
- replaced the fixed maximum-size foreground read pool with exact-size
  transient aligned allocations charged to the aggregate memory limit;
  removed read-buffer admission configuration and read-side `WouldBlock`, and
  made read-engine pressure a fail-open cache miss.
- removed `ReadBufferPolicy`, `with_read_buffer_slots`,
  `with_read_buffer_policy`, the legacy split-submission alias, and read-pool
  fields from detailed snapshots.
- simplified write overload reporting to the `write_rejections` summary counter
  with shard-buffer detail in `detailed_snapshot()`.
- reserve the read engine slot before allocating its exact aligned range, let the
  device initialize transient read storage without a userspace pre-clear,
  derive the maximum range from the runtime record limits, and expose
  `l2_read_memory_misses` and `l2_read_busy_misses` counters.
- renamed runtime settings around their actual boundaries: L1 capacity/shards,
  aggregate memory limit, separate read/write I/O workers, and one per-shard
  write-batch capacity.
- made `put` and `put_in` return their mutation sequence directly
  instead of wrapping it in a single-field `PutReceipt`.
- replaced runtime-growing L1 maps and slot vectors with fixed startup
  allocations keyed directly by seeded XXH3;
  L1 metadata now participates in aggregate memory-plan validation.
- collapsed the green-field RAM policy surface to CLOCK, write admission to
  immediate `WouldBlock`, startup reporting to `Cold`/`Warm`, and write staging
  to one capacity setting with a fixed internal flush age; removed the public
  data-sync operation and low-level index/hash setters.
- raised the bounded index ceiling to 512 million slots for TB-scale,
  small-entry deployments and exposed deleted/stale/live slot reuse pressure.
- changed the turnover soak to cycle mixed entry sizes, accept and count valid
  stale hits, reject future/wrong-key/malformed values, and gate Linux current
  RSS; production qualification now requires exact Rust 1.98.0 and all five
  performance thresholds.
- made the turnover soak concurrently exercise multiple writers and readers,
  including periodic deletes and independent mutation/read latency reporting.

### Compatibility

- no public Rust API or disk-format compatibility promise is made before the
  M4/1.0 freeze;
- runtime configuration may change across a clean warm restart without
  invalidating the image;
- static layout changes and unsupported formats intentionally start empty.
