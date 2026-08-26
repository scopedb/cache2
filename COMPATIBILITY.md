# Compatibility policy

cache-rs is currently `0.1.x` and has not reached the M4 compatibility freeze.
This document describes the candidate contract; it becomes a release promise
only when M4 is complete.

## Rust API

The intended 1.0 surface is deliberately small:

- `HybridCacheConfig`, `StaticConfig`, and `RuntimeConfig` configure and open a
  cache;
- `HybridCache` provides synchronous point and namespace mutations plus native
  Tokio-async `get` operations,
  a completion fence, snapshots, and explicit fast or warm close;
- `Value`, `CacheTier`, `StartupMode`, and `CacheHealth` describe results;
- `IoEngine` and `IoMode` select runtime I/O behavior.
- `RegionSetConfig`, `RegionSetId`, and `RegionSetAllocation` define and inspect
  optional physical namespace retention partitions.

Before 1.0, source-breaking changes may occur in any `0.x` release and are
recorded in `CHANGELOG.md`. After 1.0, backwards-compatible additions use a
minor release and source-breaking changes require a new major release. Public
enums or records that are likely to gain operational variants are
`non_exhaustive`; callers must retain a fallback arm and must not construct
returned records directly.

Public operations use `std::io::Result`. Write admission saturation is
`std::io::ErrorKind::WouldBlock`; read allocation and I/O pressure fail open as
cache misses. Persistent runtime health and write-saturation counters are
available through `HybridCache::snapshot()`.

## Disk format

The disk format is versioned independently from the Rust crate and is currently
format `1`. Until M4, committed format-1 golden fixtures may change with the
green-field implementation. Once M4 freezes format 1:

- incompatible envelope, record, index, Region metadata, or publication changes
  require a new format number;
- compatible decoder hardening may ship without a format bump;
- there is no online migration promise;
- unsupported, corrupt, stale, or static-config-mismatched cache state safely
  cold-starts empty.

Cached bytes are disposable and never authoritative, so rejecting recovery does
not imply recovering or preserving the previous cache contents.

## Configuration and restart

Static configuration defines disk identity: capacity, Region geometry, index
slots, shard count, RegionSet capacity layout and namespace ownership, seeded
XXH3-64 algorithm identity, and hash seed. A change intentionally cold-starts
empty.
Runtime configuration—including workers, I/O concurrency, write-batch
capacity, L1 capacity, and statistics—may change across a successful warm restart
without invalidating the clean image.

Only `close_warm` publishes recoverable state. Fast close, process failure, and
every unclean boundary reopen empty. This recovery contract is part of format 1
and is not a durability guarantee for ordinary cache mutations.
