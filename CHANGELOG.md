# Changelog

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
