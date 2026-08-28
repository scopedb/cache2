# C²

C² (`cache2`) is a bounded RAM and SSD cache for Tokio applications. It is
designed for large, disposable file-chunk caches where request-path simplicity
matters more than freshness or durability.

## Semantics

- C² is not a source of truth. A hit may be stale, and a miss, eviction, bypass,
  or overload is a valid result.
- Keys are raw bytes in one key space. There are no namespaces or TTLs. Keys may
  be at most 4 KiB; one encoded record must fit in a Region.
- `put`, `put_l2`, and `delete` use bounded in-memory admission. They may return
  `WouldBlock`, but never wait for device I/O.
- `get` checks L1 and then L2. A true index miss returns immediately. An admitted
  L2 hit performs one bounded read and validates the complete record locally.
- Sequence numbers help order bounded internal updates. They are not freshness
  tokens and do not prevent stale reads.
- `drain` waits for accepted mutations to finish; it does not make them durable.
  A crash or fast close reopens empty. Only `close_warm` creates a recoverable
  clean image.

## Quick start

```rust
use cache2::CacheBuilder;

# async fn example() -> std::io::Result<()> {
let cache = CacheBuilder::new("/mnt/nvme/cache2.data", 64 * 1024 * 1024 * 1024)
    .open()
    .await?;

cache.put(b"chunk:42", b"cached bytes")?;

if let Some(value) = cache.get(b"chunk:42").await? {
    consume(value.as_ref());
}

cache.delete(b"chunk:42")?;
cache.close_warm().await?;
# Ok(())
# }
# fn consume(_: &[u8]) {}
```

Treat `WouldBlock` as a cache admission rejection and continue through the
authoritative path. `put_l2` is useful for prefetch: it skips L1 admission and
becomes visible after its Region write publishes to the L2 index. Call `drain`
when a caller must wait for all accepted prefetches to become visible.

`open` uses the current Tokio runtime. `CacheBuilder::with_tokio_handle` binds
the cache to another runtime, which must have time enabled and outlive the
cache. Prefer an explicit async close; dropping an open cache performs a
synchronous fast close.

## Configuration

Defaults are intended to be safe starting points. Tune from measurements, not
from device count alone.

| Area | Controls | Notes |
| --- | --- | --- |
| L2 layout | `StaticConfig` capacity, Region size, expected entries | The default assumes 16 KiB average entries and allocates about two index slots per live key. Layout changes cold-start the cache. |
| L1 | capacity, shard count, CLOCK or S3-FIFO | Set capacity to zero to disable L1. Entries charged above 256 KiB bypass L1 but remain valid in L2. |
| Writes | append shards, write workers, flush threshold | Each append shard owns one Active Region, two Region-sized buffers, and one ordered worker. |
| Reads | read workers, optional wait timeout | Read and write execution capacity are independent. Zero wait is the default. |
| Reclaim | reclaim workers | Each worker owns one Region-sized scan buffer and an independent read lane. |
| Memory | managed-memory limit | Covers cache-owned index, L1, staging, metadata, threads, reclaim, and transient reads; it does not bound allocator, Tokio, or kernel memory. |
| I/O | POSIX or io_uring; buffered or direct | Buffered POSIX I/O is the default and production path. io_uring is an optional experimental feature in 0.1. |
| Metrics | statistics on or off | Health and resource gauges are always available; activity counters are opt-in. |

A non-zero read wait is for deployments where a short local queue is cheaper
than an origin fetch:

```rust
use cache2::RuntimeConfig;
use std::time::Duration;

let runtime = RuntimeConfig::default()
    .with_read_io_wait_timeout(Duration::from_millis(2));
```

Only read-engine capacity is queued, with at most one waiter per read worker.
No record buffer is held while waiting. Queue saturation, memory pressure, or
deadline expiry is returned as overload; with the default zero timeout the same
resource pressure fails open as a miss.

C² accepts one data-file path. Put multiple homogeneous SSDs behind RAID0 or an
equivalent striped block device instead of adding a second striping layer to the
cache. RAID loss discards the whole cache, which matches the cache contract.

`StaticConfig::peak_disk_bytes()` reports the maximum cache-owned logical disk
space. Opening validates the complete static geometry and managed-memory plan
before creating files. A static mismatch, incompatible format, or changed
append-shard topology safely starts empty.

## Observability

`Cache::snapshot()` is lock-free. With statistics enabled it returns cumulative
cache and I/O counters for the current open; health and resource gauges are
always populated. `Cache::detailed_snapshot()` also samples L1, index,
write-buffer pressure, and Region metadata, so use it slowly or on demand.

C² deliberately has no metrics SDK dependency. An OpenTelemetry or Prometheus
adapter should sample snapshots and export:

- get outcomes from `l1_hits`, `l2_hits`, `l2_misses`, and
  `l2_read_overloads`;
- mutation volume and `write_rejections`;
- request counts, file-operation counts, bytes, request time, and slot-wait
  time from `io`;
- reclaim progress, managed memory, and cache health.

Export cumulative values and derive rates in the backend. Convert nanoseconds
to seconds. Use only fixed labels such as direction, path, and outcome; never
label by key, file, Region, shard, or worker. `metrics_epoch` marks a reset and
must not become a label. Runtime file-operation counters are application-level
operations, not physical device IOPS.

`l1_misses` overlaps the L2 outcome counters and must not be added to them. When
statistics are disabled, omit activity series instead of exporting zero
traffic.

Lifecycle, recovery, reclaim, and terminal failure events use the `log` facade
under `cache2::*`. Applications own the global logger. The example uses
logforth:

```sh
RUST_LOG=cache2=info cargo run --example logforth -- /tmp/cache2.data
```

## Development

C² requires Rust 1.98.0.

```sh
cargo +1.98.0 fmt --all -- --check
cargo +1.98.0 test --all-features
cargo +1.98.0 clippy --all-targets --all-features -- -D warnings
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for implementation boundaries and
[BENCHMARK.md](BENCHMARK.md) for performance and device qualification.
