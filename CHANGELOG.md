# Changelog

This file records user-visible changes to C².

## 0.1.1 - 2026-08-29

This release keeps the version 1 on-disk format and requires no migration.

### Changed

- Warm recovery can rebind a changed append-shard count. Growth activates Free
  Regions when capacity is available; otherwise the disposable cache safely
  starts empty.
- CRC32C validation now uses `crc-fast`.
- Storage, I/O, and Region internals were split into smaller modules without
  changing the public request-path contract.

### Fixed

- Failure to create a private mmap for a warm index now falls back to a cold
  start instead of failing cache open.
- Recovery logs now identify index backing, mapping size, validation mode, and
  the reason for cold fallback or miss-only operation.

## 0.1.0 - 2026-08-29

- Initial release of the bounded RAM and NVMe hybrid cache.
