# Changelog

All notable changes to cache-rs are recorded here. The crate follows semantic
versioning for its Rust API. The disposable disk format has an independent
version number documented in `MILESTONES.md`.

## Unreleased

### Added

- bounded RAM + Region SSD `HybridCache` with shard-local staging and L1;
- sync, automatic, and Linux io_uring engines with runtime worker tuning;
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
- runtime-selectable CLOCK, LRU, TinyLFU, SIEVE, FIFO, and S3-FIFO RAM eviction
  with shared byte bounds, immediate eviction eligibility, and collision-safe
  lookup.
- static namespace-to-RegionSet routing with weighted physical Region ranges,
  independent FIFO rotation, deterministic append-shard assignment, and warm
  recovery without a second disk format.
- on-demand detailed snapshots for queue and buffer pressure, aggregate worker
  I/O, fixed/resident/retained L1 memory, index occupancy/replacement pressure,
  and per-RegionSet capacity, occupancy, and process-local rotations.
- sequenced point deletion through the existing bounded mutation path, with
  best-effort exact-key L1 cleanup and warm-recoverable 24-byte index
  tombstones.

### Changed

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
- separated namespace-only `put_in`/`get_in` from explicit `put_until` and
  `get_in_at` expiration APIs; TTL-free hits no longer sample the system clock.
- replaced the capacity-overwriting cache builder path with
  `HybridCacheConfig::from_static`, exposed validated RegionSet allocations,
  and canonicalized physically equivalent default RegionSet configurations.
- renamed the ambiguous snapshot `read_bytes` counter to `served_bytes` to
  distinguish bytes returned to callers from underlying device I/O bytes.
- replaced the fixed maximum-size foreground read pool with exact-size
  transient aligned allocations charged to the aggregate memory limit;
  removed read-buffer admission configuration and read-side `WouldBlock`, and
  moved I/O availability protection to write-side slot reservation.
- removed `ReadBufferPolicy`, `with_read_buffer_slots`,
  `with_read_buffer_policy`, the legacy split-submission alias, and read-pool
  fields from detailed snapshots.
- simplified write overload reporting to the `write_rejections` summary counter
  with gate/buffer detail in `detailed_snapshot()`.
- corrected engine slot reservation to cap write occupancy rather than total
  occupancy, and added bounded write-waiter handoff under sustained reads.
- reserve the read engine slot before allocating its exact aligned range, let the
  device initialize transient read storage without a userspace pre-clear,
  derive the maximum range from the runtime record limits, and expose
  `l2_read_memory_misses` and `l2_read_busy_misses` counters.
- renamed runtime settings around their actual boundaries: L1 capacity/shards,
  aggregate memory limit, I/O concurrency, and per-shard write buffering.
- made `put`, `put_in`, and `put_until` return their mutation sequence directly
  instead of wrapping it in a single-field `PutReceipt`.
- replaced runtime-growing L1 maps, slot vectors, policy sketches, and S3-FIFO
  ghost state with fixed startup allocations keyed directly by seeded XXH3;
  L1 metadata now participates in aggregate memory-plan validation.
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
