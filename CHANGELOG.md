# Changelog

## Unreleased

## v0.6.1 (2026-11-16)

### Improvements

- Compare `CacheConfig` and `RuntimeOptions` directly with `==` and `!=` through their new `Eq` and `PartialEq` implementations.
- Log `Value` with `Debug` without printing its payload, and format `RequestOperation`, `RequestOutcome`, `IoRole`, and `IoOutcome` with `Display`.
- Use public configuration, health, tier, and statistics enums as hash-map keys through their new `Hash` implementations.

### Performance improvements

- Reduce allocation and memory-accounting overhead when replacing or evicting fixed-size L1 entries, for both CLOCK and S3-FIFO.
- Reduce checksum and index-bookkeeping work during L2 space reclamation, including when preserving hot records under sustained writes.
- Reduce read and mutation overhead when L1 is disabled (`RuntimeOptions::l1_capacity_bytes = 0`).

### Bug fixes

- Contended `put` and `put_l2` calls no longer trigger partial L2 flushes or count as exhausted write-buffer capacity in `DetailedCacheSnapshot::write_buffer_rejections`. They return `ErrorKind::Overloaded` and remain included in `CacheSnapshot::write_rejections`.
- Avoid write stalls when an L2 region fills: finish its pending rotation after the last write completes, without requiring another rejected write to restart progress or unnecessarily rotating the empty replacement region.
- Fix `CacheSnapshot::served_bytes` when an L2 read selects a newer L1 value of a different size during promotion. The counter now reflects the bytes actually returned to the caller.
- Make `close_fast` and `close_warm` report drain and background-worker failures even when L2 reads remain in flight, so applications can detect failed shutdowns.
- Prevent `close_fast` and `close_warm` from hanging when drain bookkeeping cannot be allocated or its counter is exhausted. Idle append workers also stop on terminal cache failures without waiting for another request.

## v0.6.0 (2026-09-21)

### Breaking changes

- Move the activity-counter switch from `RuntimeOptions::statistics` to `RuntimeOptions::stats.activity_counters`, and rename `CacheSnapshot::statistics_enabled` to `activity_counters_enabled`. Update configuration and metrics code to use the new fields.
- Rename `RegionIndexTurnoverReport::config` to `options` in the benchmarking API. Update code that reads the workload options from a report.

### New features

- Add optional fill-pressure control through `RuntimeOptions::fill_control`. `Observe` reports pressure through `CacheSnapshot::fill_control`; `Adaptive` also pauses new `put` and `put_l2` fills during slow background I/O, limits non-essential background flush rates, and suppresses optional hot-record reinsertion while paused. The default is `Disabled`. See [adaptive fill admission](CONFIGURATION.md#adaptive-fill-admission) for configuration and tuning.
- Allow background I/O timeouts to recover instead of immediately disabling the cache. `CacheHealth::Recovering` reports this state; new fills pause while reads and deletes remain available. `RuntimeOptions::io_recovery_timeout` bounds the recovery period, with `None` allowing unlimited recovery by default. Close interrupts recovery; actual I/O errors, invalid completions, or an exhausted recovery timeout remain terminal failures.

### Improvements

- Configuration errors for an unavailable `IoEngineOptions::IoUring` now identify the missing crate feature, operating system, or architecture.

## v0.5.0 (2026-09-16)

### Breaking changes

- Rename `IoEngineConfig`, `PosixIoConfig`, `IoUringConfig`, `IoUringPoolConfig`, `IoUringSqPollConfig`, and `RegionIndexTurnoverConfig` to their `*Options` equivalents. Update imports and type annotations.
- Make option structs `#[non_exhaustive]` with public editable fields. Replace getters with field access, and construct options with `Default::default()` or `new(required_input)` followed by field assignments instead of positional constructors and `with_*` setters. `IoUringSqPollOptions::new(idle_millis)` remains available.

### New features

- Add optional request outcome counters and latency distributions through `RuntimeOptions::stats` and `Cache::stats_snapshot()`. L1 hits, L2 lookups, and mutations can be timed independently with full or sampled recording; engine I/O timing separates foreground reads, append writes, and reclaim reads. Collection is disabled by default.

### Improvements

- Add `RuntimeOptions::reclaim_io_timeout` to configure the background reclaim-read deadline independently of foreground I/O. The default remains five seconds.

### Bug fixes

- Make read I/O latency include buffer preparation and engine scheduling after slot reservation, so reported durations cover the full engine request.

## v0.4.0 (2026-09-10)

### Breaking changes

- Rename the public I/O configuration enum from `IoEngine` to `IoEngineConfig`. Update imports and type annotations.
- Move error imports to `cache2::{Error, ErrorKind, ErrorOperation}` and remove the `cache2::error` module and `cache2::Result` alias. Use `Result<T, cache2::Error>` in signatures that previously used the alias.
- Replace `StaticConfig`, `RuntimeConfig`, and `CacheBuilder` with `StorageOptions`, `StorageLayout`, `RuntimeOptions`, and `CacheConfig`. Build the storage layout with `StorageOptions::build()`, combine it with runtime options using `CacheConfig::new(layout, options)`, then call `Cache::open(path, config)` or `Cache::open_with_handle(path, config, handle)`. Configuration construction replaces the standalone `validate` call.
- Replace separate read-wait setters with `ReadAdmission::Immediate` or `ReadAdmission::Wait { timeout, max_waiters }`. Waiting requires a positive timeout.
- Configuration errors now identify `ErrorOperation::BuildStorage` or `BuildConfig` instead of `ValidateConfig` or `PeakDiskBytes`; update matches on those operations. Replace fallible `StaticConfig::peak_disk_bytes()` calls with `StorageLayout::peak_disk_bytes()`.

## v0.3.0 (2026-09-04)

### Breaking changes

- Replace `IoEngine`'s `with_read_io_workers`, `with_write_io_workers`, and `with_reclaim_workers` setters with backend-specific options in `PosixIoConfig`, `IoUringConfig`, and `IoUringPoolConfig`. Configure the read, write, and reclaim pools for the selected backend.

### Improvements

- Configure queued L2 reads separately from executing reads. Within each I/O engine, queued reads receive capacity in FIFO order ahead of later immediate requests.
- Configure the experimental `io_uring` read, write, and reclaim pools independently, including ring counts, total in-flight limits, and optional SQPOLL idle time and CPU affinity.
- Add per-pool IOPOLL support through `IoUringPoolConfig::with_io_poll`. This requires direct I/O and a filesystem and block device that support polling. Cancellation remains advisory until the submitted operation completes.

### Performance improvements

- Reduce write-lock hold time by checksumming values before admission. Concurrent writers that encounter a contended append shard receive `ErrorKind::Overloaded` without waiting for the lock.
- Use spare capacity across the read I/O pool when frequently accessed keys saturate their preferred lane.
- Reduce redundant worker wakeups during write-heavy workloads and `io_uring` submissions.

### Bug fixes

- Prevent concurrent L2 reads from making cache close wait indefinitely on device I/O after shutdown has started.
- Report `ErrorKind::Overloaded` and increment overload counters when a queued read times out, instead of counting it as a busy cache miss.
- Keep `io_uring` I/O and completions progressing during sustained request submissions and cancellations.
- Make `drain` return an error when a concurrent close has already stopped an append worker, instead of waiting indefinitely for that worker.
- Include per-ring `io_uring` queues, mappings, and request tracking in managed-memory requirements, so configuration budgets account for the selected ring topology.

## v0.2.3 (2026-09-03)

### Improvements

- Call `Cache::close_fast` and `Cache::close_warm` through shared `&self` or `Arc<Cache>` handles without `Arc::try_unwrap`. Starting close rejects new operations through all retained handles; warm close waits for accepted persistent writes before publishing recovery state.

## v0.2.2 (2026-09-01)

### Performance improvements

- Distribute hot-record preservation across append shards during L2 reclamation, reducing contention on a single shard.
- Prioritize freeing L2 capacity over preserving hot records when no free regions remain. A saturated soak benchmark accepted 20.2% more writes with approximately unchanged hit rate and no runtime errors.

## v0.2.1 (2026-08-30)

### Improvements

- Support L2 indexes above the previous 536,870,912-slot limit when their storage layout is representable and fits the managed-memory budget.
- Account for the memory used to validate recovered index pages in managed-memory requirements.

## v0.2.0 (2026-08-29)

### Breaking changes

- Fallible cache operations now return `cache2::Result<T>` with a structured `cache2::Error` instead of `std::io::Error`. Use `kind()` and `operation()` for application policy. Converting back to `io::Error` preserves the original I/O error; use `into_io_error_with_context()` when the conversion must retain C²'s classification and operation.
- Classify request-path saturation as `ErrorKind::Overloaded` and exclusive cache-file contention during `open` as `ErrorKind::Busy`. Handle these cases separately from device I/O failures.
- Mark `CacheTier` as `#[non_exhaustive]`. Add a wildcard arm to downstream matches.

### Performance improvements

- Reduce L1 lookup and promotion overhead under contention, and avoid scanning unused directory slots during CLOCK eviction.
- Reduce L1 metadata memory: the 4 TiB L2, 10 GiB L1, 64-shard reference configuration uses 130 MiB instead of 184 MiB with CLOCK, and 240 MiB instead of 338 MiB with S3-FIFO.

## v0.1.2 (2026-08-29)

### Improvements

- Replace the BSL-1.0-licensed `xxhash-rust` dependency with the MIT-licensed `twox-hash` implementation while preserving seeded XXH3-64 key hashes.

## v0.1.1 (2026-08-29)

### Improvements

- Allow warm recovery after changing the append-shard count. The cache reuses recovered regions for the new configuration when capacity permits, otherwise it starts cold with an empty cache.
- Add recovery logs that identify the index backing, mapping size, validation mode, and reasons for a cold start or miss-only state.

### Performance improvements

- Reduce CRC32C checksum overhead.

### Bug fixes

- Start cold with an empty cache instead of failing `open` when a recovered index cannot be privately memory-mapped.

## v0.1.0 (2026-08-29)

- Initial release of the bounded RAM and NVMe hybrid cache.
