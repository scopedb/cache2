# Changelog

## Unreleased

### Bug Fixes

- A drain racing with close now rejects requests to stopped append workers
  instead of waiting indefinitely for an unreachable completion generation.
- Managed-memory planning now reserves io_uring flight tables, rounded SQ/CQ
  buffers, ring mappings, and failure cleanup scratch for every configured ring.

### Breaking Changes

- `IoEngine` now carries backend-specific topology. Configure POSIX worker
  counts with `PosixIoConfig`; configure independent io_uring pools with
  `IoUringConfig` and `IoUringPoolConfig`. The backend-ambiguous
  `with_read_io_workers`, `with_write_io_workers`, and
  `with_reclaim_workers` methods were removed.

### Improvements

- Read execution concurrency and bounded asynchronous wait capacity can now be
  configured independently. Queued reads receive FIFO priority within their
  selected engine, use its full physical depth, and cannot be bypassed by later
  immediate admissions.
- The experimental io_uring engine exposes independent ring and aggregate
  in-flight bounds for read, write, and reclaim pools, plus opt-in SQPOLL
  idle/CPU affinity.

- The experimental io_uring engine's per-pool IOPOLL completion polling is
  available again through `IoUringPoolConfig::with_io_poll`. The driver parks
  on the wake socket while idle and reaps polled completions through
  enter-with-GETEVENTS instead of relying on wake or cancel requests that
  IOPOLL rings cannot carry. IOPOLL requires direct I/O mode and a
  polling-capable filesystem and block device; cancellation remains advisory
  until the polled operation completes.

### Performance

- Foreground appends compute payload checksums before taking the shard mutation
  gate, shortening the critical section while waiting for competing shard mutations.
- Hot read routes retain stable primary affinity but rotate their single
  pressure-only fallback across all physical lanes.
- Append workers are notified only for a new batch, a flush-threshold crossing,
  or an urgent lifecycle event; repeated writes coalesce into the pending wake.
- io_uring command wakeups coalesce while the driver has a pending socket
  notification, avoiding one socket write per submitted request.

## v0.2.3 (2026-09-03)

This release keeps the version 1 on-disk format and requires no disk
migration. It contains no source-breaking public API changes.

### Improvements

- `Cache::close_fast` and `Cache::close_warm` now accept shared ownership, so
  caches held in `Arc` can close without `Arc::try_unwrap`. Close immediately
  rejects new operations on every retained handle, and warm close fences
  accepted mutations and their submitted writes before publishing `CLEAN`.

## v0.2.2 (2026-09-01)

This release keeps the version 1 on-disk format and requires no disk
migration. It contains no source-breaking public API changes.

### Performance

- Reclaim workers rotate reinsertion across disjoint append-shard subsets,
  avoiding sustained concentration on a single shard.
- When no Free Region remains, reclaim skips hot-record reinsertion for that
  pass and restores foreground write capacity first. In the saturated soak
  workload, accepted writes increased by 20.2%, while the hit ratio remained
  effectively flat and runtime errors remained at zero.

### Testing and Tooling

- The repository now uses a Cargo workspace that separates the publishable
  library from integration tests, benchmarks, examples, and development tools.
- Bounded property tests now cover persistent decoders, record encoding,
  fixed-map operations, and Region-index operations across 10,000 generated
  inputs per property.
- Integration-test fixtures were consolidated and fragile timing assertions
  were removed.

## v0.2.1 (2026-08-30)

This release keeps the version 1 on-disk format and requires no disk
migration. It contains no source-breaking public API changes.

### Improvements

- Static index sizing is no longer capped at 536,870,912 slots. Index capacity
  is accepted when its complete page, mapping, and recovery-image layout is
  representable; runtime opening still enforces the configured managed-memory
  plan.
- Managed-memory planning includes the index page-validation bitmap.

### Benchmarking

- Added deterministic mixed, reinsertion, and negative-lookup workload
  profiles with throughput, sampled latency, overload, tier, reclaim,
  reinsertion, I/O, and memory reporting.

### Documentation

- Added a configuration guide covering managed memory, coupled settings,
  workload-oriented profiles, diagnostics, tuning order, and hard bounds.
- Expanded the architecture and recovery documentation with the consistency
  model, bounded request paths, persistent artifacts, open state machine, and
  warm-close publication ordering.

## v0.2.0 (2026-08-29)

This release keeps the version 1 on-disk format and requires no disk
migration. It contains source-breaking public API changes described below.

### Breaking Changes

- Public fallible operations now return `cache2::Error` through
  `cache2::Result`. Errors expose an actionable `ErrorKind`, the failed
  `ErrorOperation`, and the original `std::io::Error`; conversion back to
  `std::io::Error` preserves the original error by default, with an explicit
  contextual wrapper available when structured fields should remain in the
  source chain.
- Request-path saturation is classified as `ErrorKind::Overloaded`, while
  exclusive cache-file contention during `open` is classified as
  `ErrorKind::Busy`.
- `CacheTier` is non-exhaustive so future backing tiers can be added without
  another source-breaking enum change. Downstream matches require a wildcard
  arm.

### Improvements

- L1 lookups use bounded contention retries, concurrent promotions reuse an
  already-published exact-key value, and CLOCK victim scans visit resident
  entries instead of sparse capacity slots.
- L1 free lists, hash directories, fixed entry slots, and S3-FIFO ghost state
  use compact fixed-width storage. In the 4 TiB L2, 10 GiB L1, 64-shard
  reference plan, CLOCK metadata falls from 184 MiB to 130 MiB and S3-FIFO
  metadata falls from 338 MiB to 240 MiB.
- Full hashes, full-key validation, bounded probes, and bounded victim work
  remain in force for both L1 policies.

### Documentation and Validation

- Public cache values, status enums, snapshots, and every metrics field now
  document their semantics, including L2 values promoted before return.
- Supported platform and I/O combinations are explicit. CI now rejects
  missing public documentation, verifies the publishable package, and runs
  the extended crash and recovery contracts.

## v0.1.2 (2026-08-29)

This release keeps the version 1 on-disk format and requires no migration.

### Dependency Licensing

- Replaced the BSL-1.0-licensed `xxhash-rust` dependency with the MIT-licensed
  `twox-hash` implementation while preserving seeded XXH3-64 key hashes, and
  removed BSL-1.0 from the allowed dependency licenses.

## v0.1.1 (2026-08-29)

This release keeps the version 1 on-disk format and requires no migration.

### Improvements

- Warm recovery can rebind a changed append-shard count. Growth activates Free
  Regions when capacity is available; otherwise the disposable cache safely
  starts empty.
- CRC32C validation now uses `crc-fast`.
- Storage, I/O, and Region internals were split into smaller modules without
  changing the public request-path contract.

### Bug Fixes

- Failure to create a private mmap for a warm index now falls back to a cold
  start instead of failing cache open.
- Recovery logs now identify index backing, mapping size, validation mode, and
  the reason for cold fallback or miss-only operation.

## v0.1.0 (2026-08-29)

- Initial release of the bounded RAM and NVMe hybrid cache.
