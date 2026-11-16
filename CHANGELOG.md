# Changelog

## Unreleased

### Improvements

- Implement `Debug` for `Value`, `Eq` and `PartialEq` for `CacheConfig` and `RuntimeOptions`, `Copy` for `DetailedCacheSnapshot`, `Hash` for public configuration, health, tier, and statistics enums, and `Display` for `RequestOperation`, `RequestOutcome`, `IoRole`, and `IoOutcome`.

### Performance improvements

- Reuse exclusively owned L1 allocations of matching size during CLOCK and S3-FIFO replacement and eviction, reducing allocator churn under fixed-size turnover.
- Reuse validated payload checksums when rewriting hot records during reclaim, and skip payload checksum scans for within-budget records once reinsertion stops for a Region.
- Bypass L1 shard locking and directory lookups across reads, fills, promotions, and deletes when `l1_capacity_bytes` is zero.
- Skip no-op atomic updates for unchanged managed-memory peaks, net-zero L1 byte transfers, already-clear L2 heat bits, and validated index pages.

### Bug fixes

- Return `ErrorKind::Overloaded` on foreground staging lock contention without forcing partial Region flushes or incrementing `DetailedCacheSnapshot::write_buffer_rejections`; `CacheSnapshot::write_rejections` still counts all rejected mutations.
- Retain a full Region's rotation request across its final write so the shard rotates immediately upon completion without waiting for another rejected put or rotating the replacement empty Region.
- Count the payload size actually returned in `CacheSnapshot::served_bytes` when L2 promotion selects a newer L1 entry of a different size.
- Propagate drain and background worker failures during `close_fast` and `close_warm` when foreground L2 reads remain in flight instead of treating shutdown as quiescent.

## v0.6.0 (2026-09-21)

### Breaking changes

- Move optional statistics settings under `RuntimeOptions::stats`: replace `runtime.statistics` with `runtime.stats.activity_counters` and `CacheSnapshot::statistics_enabled` with `CacheSnapshot::activity_counters_enabled`.
- Rename `RegionIndexTurnoverReport::config` to `options` in the benchmarking API to match the `RegionIndexTurnoverOptions` input it carries.

### New features

- Add `RuntimeOptions::fill_control` (`Disabled`, `Observe`, `Adaptive`) and `CacheSnapshot::fill_control` to track pre-timeout background I/O pressure, pause new `put` and `put_l2` fills once background I/O crosses one quarter of its deadline (capped at 500 ms) or enters timeout recovery, pace non-essential background flushes against instance-wide byte and record ceilings, and suppress optional reinsertion while paused. See [adaptive fill admission](CONFIGURATION.md#adaptive-fill-admission) for ceilings and measurement limits.
- Add `CacheHealth::Recovering` and `RuntimeOptions::io_recovery_timeout` (defaulting to `None`) so background write and reclaim timeouts pause new fills while keeping reads and deletes available until affected work finishes validation and publication or close interrupts recovery; actual I/O errors, invalid completions, and exhausted finite recovery budgets remain terminal.

### Improvements

- Identify the missing crate feature, operating system, or architecture in the `Error` source when `IoEngineOptions::IoUring` is unavailable.

## v0.5.0 (2026-09-16)

### Breaking changes

- Rename unchecked I/O and benchmarking option types to use `Options` suffixes: replace `IoEngineConfig`, `PosixIoConfig`, `IoUringConfig`, `IoUringPoolConfig`, `IoUringSqPollConfig`, and `RegionIndexTurnoverConfig` with their `*Options` counterparts.
- Make all option structs `#[non_exhaustive]` with public fields and remove I/O option getters, positional constructors, and `with_*` methods except `IoUringSqPollOptions::new(idle_millis)`; construct options with `new(required_input)` or `Default::default()` followed by field assignments.

### New features

- Add `RuntimeOptions::stats` and `Cache::stats_snapshot()` for terminal public request outcomes, independently configured full or sampled L1-hit, L2-lookup, and mutation latency histograms, and full read/write/reclaim I/O latency within bounded managed memory.

### Improvements

- Add `RuntimeOptions::reclaim_io_timeout` to configure the background reclaim read deadline independently of foreground I/O, retaining the five-second default.

### Bug fixes

- Include buffer preparation and engine scheduling between slot reservation and submission in read I/O duration accounting.

## v0.4.0 (2026-09-10)

### Breaking changes

- Rename the public I/O configuration enum from `IoEngine` to `IoEngineConfig`.
- Export error types only from the crate root (`cache2::{Error, ErrorKind, ErrorOperation}`) and remove the `cache2::Result` alias; replace `cache2::error` imports and use `Result<T, Error>`.
- Separate editable `StorageOptions` and `RuntimeOptions` from immutable `StorageLayout` and `CacheConfig`: replace `StaticConfig`, `RuntimeConfig`, and `CacheBuilder` with `StorageOptions::build()`, `CacheConfig::new(layout, options)`, `Cache::open(path, config)`, and `Cache::open_with_handle(path, config, handle)`, removing the standalone `validate` method.
- Replace separate read-wait setters with `ReadAdmission::Immediate` and `ReadAdmission::Wait { timeout, max_waiters }`, requiring a positive timeout.
- Report configuration failures under `ErrorOperation::BuildStorage` or `BuildConfig` instead of `ValidateConfig` or `PeakDiskBytes`, and replace fallible `StaticConfig::peak_disk_bytes()` calls with `StorageLayout::peak_disk_bytes()`.

## v0.3.0 (2026-09-04)

### Breaking changes

- Replace backend-ambiguous `with_read_io_workers`, `with_write_io_workers`, and `with_reclaim_workers` methods on `IoEngine` with backend-specific `PosixIoConfig`, `IoUringConfig`, and `IoUringPoolConfig` topologies.

### Improvements

- Decouple read execution concurrency from bounded asynchronous wait capacity, serving queued reads in FIFO order within the selected engine ahead of later immediate admissions.
- Add independent ring and aggregate in-flight bounds for read, write, and reclaim pools plus opt-in SQPOLL idle and CPU affinity in the experimental `io_uring` engine.
- Support per-pool `io_uring` IOPOLL completion polling through `IoUringPoolConfig::with_io_poll`, parking idle drivers on the wake socket and reaping completions via `enter` with `GETEVENTS`; requires direct I/O mode and a polling-capable filesystem and block device, and treats cancellation as advisory until the polled operation completes.

### Performance improvements

- Compute payload checksums before taking the append-shard mutation gate, and reject contended shard admission with `ErrorKind::Overloaded` instead of blocking on competing mutations.
- Rotate the single pressure-only fallback lane for hot read routes across all physical lanes while keeping primary affinity stable.
- Coalesce append-worker and `io_uring` command-socket wakeups so repeated writes and submissions share pending notifications.

### Bug fixes

- Fence I/O submission before checking for idle engines during close, preventing concurrent reads from stalling shutdown on device I/O.
- Report `ErrorKind::Overloaded` and update overload counters instead of busy-miss counters when bounded read waiting times out.
- Cap `io_uring` command processing per driver pass so cancelled request refills cannot starve I/O submission or completion reaping.
- Reject drain requests sent to stopped append workers during a concurrent close instead of waiting indefinitely for an unreachable completion generation.
- Reserve `io_uring` flight tables, rounded SQ/CQ buffers, ring mappings, and failure cleanup scratch in managed-memory planning for every configured ring.

## v0.2.3 (2026-09-03)

### Improvements

- Accept shared `&self` ownership in `Cache::close_fast` and `Cache::close_warm` so caches held in `Arc` can close without `Arc::try_unwrap`, immediately rejecting new operations on all retained handles and fencing accepted persistent writes before `close_warm` publishes `CLEAN`.

## v0.2.2 (2026-09-01)

### Performance improvements

- Rotate hot-record reinsertion across disjoint append-shard subsets, avoiding sustained concentration on a single shard.
- Skip hot-record reinsertion when no Free Region remains so reclaim restores foreground write capacity first, increasing accepted writes by 20.2% in the saturated soak workload with effectively flat hit ratio and zero runtime errors.

## v0.2.1 (2026-08-30)

### Improvements

- Remove the fixed 536,870,912-slot static index cap, accepting index capacities whenever their complete page, mapping, and recovery-image layout is representable and satisfies the managed-memory plan.
- Include the index page-validation bitmap in managed-memory planning.

## v0.2.0 (2026-08-29)

### Breaking changes

- Return structured `cache2::Error` values (`ErrorKind`, `ErrorOperation`, and the original `std::io::Error`) from fallible public operations via `cache2::Result`, preserving the underlying I/O error on `From<Error> for io::Error` with an explicit `into_io_error_with_context()` wrapper.
- Classify request-path saturation as `ErrorKind::Overloaded` and exclusive cache-file contention during `open` as `ErrorKind::Busy`.
- Mark `CacheTier` as `#[non_exhaustive]`, requiring a wildcard arm in downstream matches.

### Performance improvements

- Use bounded contention retries for L1 lookups, reuse already-published exact-key values across concurrent promotions, and scan resident CLOCK entries instead of sparse capacity slots.
- Use compact fixed-width storage for L1 free lists, hash directories, fixed entry slots, and S3-FIFO ghost state, reducing metadata in the 4 TiB L2, 10 GiB L1, 64-shard reference plan from 184 MiB to 130 MiB (CLOCK) and from 338 MiB to 240 MiB (S3-FIFO).

## v0.1.2 (2026-08-29)

### Improvements

- Replace the BSL-1.0-licensed `xxhash-rust` dependency with the MIT-licensed `twox-hash` implementation while preserving seeded XXH3-64 key hashes.

## v0.1.1 (2026-08-29)

### Improvements

- Rebind changed append-shard counts during warm recovery, activating Free Regions when growing capacity is available or falling back to an empty cache otherwise.
- Identify index backing, mapping size, validation mode, and cold-fallback or miss-only causes in recovery logs.

### Performance improvements

- Compute CRC32C checksums with `crc-fast`.

### Bug fixes

- Fall back to a cold start instead of failing `open` when creating a private mmap for a warm index fails.

## v0.1.0 (2026-08-29)

- Initial release of the bounded RAM and NVMe hybrid cache.
