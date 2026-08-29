# Changelog

## Unreleased

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
