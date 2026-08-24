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
  with shared byte bounds, pending-write pinning, and collision-safe lookup.
- static namespace-to-RegionSet routing with weighted physical Region ranges,
  independent FIFO rotation, deterministic append-shard assignment, and warm
  recovery without a second disk format.
- on-demand detailed snapshots for queue and buffer pressure, aggregate worker
  I/O, and per-RegionSet capacity, occupancy, and process-local rotations.

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

### Compatibility

- no public Rust API or disk-format compatibility promise is made before the
  M4/1.0 freeze;
- runtime configuration may change across a clean warm restart without
  invalidating the image;
- static layout changes and unsupported formats intentionally start empty.
