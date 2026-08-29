# C²

[![CI](https://github.com/scopedb/cache2/actions/workflows/ci.yml/badge.svg)](https://github.com/scopedb/cache2/actions/workflows/ci.yml)

**A bounded RAM + SSD cache for Tokio applications.**

C² (`cache2`) provides bounded, disposable acceleration for large file chunks.
It keeps request paths short and cache-owned resources fixed.

- Sharded CLOCK or S3-FIFO L1 with a compact, fixed-size L2 index.
- Batched Region writes and independent read, write, and reclaim I/O capacity.
- Best-effort consistency with stale, fully validated hits.

## Quick start

```rust
use cache2::{CacheBuilder, ErrorKind, Result};

async fn run() -> Result<()> {
    let cache = CacheBuilder::new("/var/tmp/cache2.data", 1024 * 1024 * 1024)
        .open()
        .await?;

    match cache.put(b"chunk:42", b"cached bytes") {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::Overloaded => {
            // Cache admission is full. Continue through the authoritative path.
        }
        Err(error) => return Err(error),
    }

    if let Some(value) = cache.get(b"chunk:42").await? {
        println!("cache hit: {} bytes", value.len());
    }

    cache.close_warm().await
}
```

`open` uses the current Tokio runtime. Use
`CacheBuilder::with_tokio_handle` to bind C² to another runtime; it must have
time enabled and outlive the cache.

## Semantics

C² returns fully validated, potentially stale values. Misses, eviction, L1
bypass, and bounded overload are normal cache outcomes.

- Keys are raw bytes in one key space, up to 4 KiB. Each encoded record fits in
  one Region.
- Resource pressure returns a miss, bypass, or `ErrorKind::Overloaded`
  according to the operation.
- Sequence numbers provide advisory ordering for internal updates.

Public failures are `cache2::Error` values with an actionable `ErrorKind`, the
failed `ErrorOperation`, and the original `std::io::Error` source. See
[Error handling](ERRORS.md) for the classification table, retry policy,
diagnostic fields, and migration from the former `io::Result` API.

### Operations

| Operation | Behavior |
| --- | --- |
| `put` | Attempts immediate L1 admission and stages the value for L2. It returns after bounded in-memory admission. |
| `put_l2` | Stages the value for L2 and applies best-effort L1 cleanup. The value appears after its Region write publishes. |
| `get` | Checks L1, then performs at most one bounded, locally validated L2 record read. |
| `delete` | Removes the current L2 mapping and applies best-effort L1 cleanup with bounded in-memory work. |
| `drain` | Waits for accepted Region writes and L2 index publication. |

### Lifecycle

`close_fast`, drop, and an unclean exit make the next open a cold start.
`close_warm` publishes a clean recovery image for a warm start. Prefer explicit
async close because drop closes the cache synchronously.

## Configuration

`StaticConfig` defines the persistent L2 layout. `RuntimeConfig` selects the
process-local topology for each open.

### Persistent layout

- `StaticConfig::new(capacity_bytes)` uses 32 MiB Regions and assumes 16 KiB
  average live entries when sizing the fixed index.
- `with_region_size_bytes` changes Region geometry.
- `with_expected_entries` sizes the index for the expected simultaneously live
  key count.
- `peak_disk_bytes` reports the maximum cache-owned logical disk space.

Changing the persistent layout starts with an empty cache. Open validates the
complete geometry and memory plan before creating cache files.

### Runtime tuning

| Area | Controls | Default and behavior |
| --- | --- | --- |
| L1 | `with_l1_capacity_bytes`, `with_l1_shards`, `with_l1_eviction_policy` | 256 MiB, 32 shards, CLOCK. Zero capacity disables L1; entries charged above 256 KiB use L2. |
| Reads | `with_read_io_workers`, `with_read_io_wait_timeout` | Four workers and immediate admission. |
| Writes | `with_write_io_workers`, `with_append_shards`, `with_write_flush_threshold_bytes` | Four workers, four append shards, 4 MiB flush threshold. |
| Reclaim | `with_reclaim_workers` | One worker with its own Region-sized scan buffer and read lane. |
| Memory | `with_managed_memory_limit_bytes` | 1 GiB across cache-managed allocations. |
| I/O | `with_io_engine`, `with_io_mode` | Buffered POSIX I/O. |
| Metrics | `with_statistics` | Health and resource gauges enabled; cumulative activity counters opt in. |

Changing the append-shard count rebinds recovered Active Regions during a warm
open. Growth uses available Free Regions; when there are not enough, the
disposable cache safely starts empty.

The default read-wait timeout is zero, so read-engine or buffer pressure returns
a miss. A positive timeout enables a short queue with at most one waiter per
read worker. Queued requests retain their read plan and allocate a buffer after
admission. Queue saturation, memory pressure, and timeout return explicit
overload.

Buffered POSIX I/O is the production path. Direct I/O is an explicit Linux
mode. io_uring requires the `io-uring` feature and remains experimental in
0.2.

### Platform support

C² supports 64-bit Linux and macOS. Buffered positioned I/O is available on
both platforms. Direct I/O and io_uring are Linux-only; io_uring is limited to
the architectures listed by the optional `io-uring` feature. Other Unix
targets may compile, but cache open returns `ErrorKind::Unsupported` when the
platform cannot provide physical file preallocation. Windows is not supported.

## Deployment

C² accepts one data-file path. For multiple homogeneous SSDs, expose RAID0 or
an equivalent striped block device below the filesystem. Losing any member
discards the complete cache.

The managed-memory limit covers the index, L1, append and reclaim buffers,
metadata, cache-owned threads, recovery scratch, and transient reads. Total
deployment memory additionally includes allocator metadata, Tokio, process
overhead, and the kernel page cache.

The on-disk format is versioned. During 0.x, deployments should expect cold
starts across releases and monitor `Cache::startup_mode()`.

## Observability

### Metrics

`Cache::snapshot()` provides lock-free health and resource gauges.
`RuntimeConfig::with_statistics(true)` adds cumulative cache and I/O counters.
`Cache::detailed_snapshot()` samples L1, index, write-buffer pressure, and
Region metadata for periodic diagnostics.

C² exposes snapshots for integration with the application's metrics SDK. An
OpenTelemetry or Prometheus adapter can export:

- get outcomes from `l1_hits`, `l2_hits`, `l2_misses`, and
  `l2_read_overloads`;
- mutation volume and `write_rejections`;
- I/O requests, operations, bytes, request time, and slot-wait time;
- reclaim progress, managed memory, and cache health.

Export counters cumulatively and derive rates in the backend. Use fixed labels
such as direction, path, and outcome. Treat `metrics_epoch` as a reset marker.
Report `l1_misses` separately because it overlaps L2 outcomes.

Runtime file-operation counters describe application-level operations; system
telemetry supplies physical device IOPS. Convert nanoseconds to seconds before
export. Activity series correspond to statistics-enabled opens.

### Logs

Lifecycle, recovery, reclaim, and terminal failure events use the `log` facade
under `cache2::*`. Applications own the global logger. The included example
uses logforth:

```sh
RUST_LOG=cache2=info cargo run --example logforth -- /tmp/cache2.data
```

`cache_opened` reports the index backing, mapping extent, validation mode, and
whether warm mutations use copy-on-write. `cache_recovery_cold` records why a
clean image was rejected or why private mapping fell back to a cold start.
`cache_miss_only` records the first terminal index-validation or I/O failure.

## Development

C² requires Rust 1.98.0.

```sh
cargo +1.98.0 fmt --all -- --check
cargo +1.98.0 test --all-features
cargo +1.98.0 clippy --all-targets --all-features -- -D warnings
```

## Further reading

- [Architecture](ARCHITECTURE.md) — data structures, request paths, reclaim,
  and recovery.
- [Error handling](ERRORS.md) — structured classifications, operation context,
  overload policy, and standard I/O interoperability.
- [Validation](BENCHMARK.md) — benchmarks, mixed turnover, and Linux NVMe
  qualification.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
