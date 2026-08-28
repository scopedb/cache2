# C²

[![CI](https://github.com/leiysky/cache-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/leiysky/cache-rs/actions/workflows/ci.yml)

**A bounded RAM + SSD cache for Tokio applications.**

C² (`cache2`) is disposable acceleration for large file chunks. It keeps the
request path short, bounds every cache-owned resource, and treats cache loss as
an availability event rather than a data-loss event.

- Sharded CLOCK or S3-FIFO L1 with a compact, fixed-size L2 index.
- Batched Region writes and independent read, write, and reclaim I/O capacity.
- Best-effort consistency: stale hits are allowed; corrupt or wrong-key values
  are never returned.

## Quick start

```rust
use cache2::CacheBuilder;
use std::io::ErrorKind;

# async fn example() -> std::io::Result<()> {
let cache = CacheBuilder::new("/var/tmp/cache2.data", 1024 * 1024 * 1024)
    .open()
    .await?;

match cache.put(b"chunk:42", b"cached bytes") {
    Ok(_) => {}
    Err(error) if error.kind() == ErrorKind::WouldBlock => {
        // Cache admission is full. Continue through the authoritative path.
    }
    Err(error) => return Err(error),
}

if let Some(value) = cache.get(b"chunk:42").await? {
    consume(value.as_ref());
}

cache.close_warm().await?;
# Ok(())
# }
# fn consume(_: &[u8]) {}
```

`open` uses the current Tokio runtime. Use
`CacheBuilder::with_tokio_handle` to bind C² to another runtime; it must have
time enabled and outlive the cache.

## Contract

C² is a cache, not a source of truth:

- Hits may be stale. Misses, eviction, L1 bypass, and bounded overload are
  normal outcomes.
- Keys are raw bytes in one key space. There are no namespaces or TTLs.
- Keys may be at most 4 KiB. A complete encoded record must fit in one Region.
- Sequence numbers order cheap internal updates; they are not freshness tokens.

The public operations have deliberately different completion points:

- **`put`** attempts immediate L1 admission and stages the same value for L2.
- **`put_l2`** stages a prefetch without admitting its payload to L1. It becomes
  visible after the completed Region write publishes to the index.
- **`delete`** removes the current L2 mapping and cleans L1 best effort. It
  appends no record.
- **`get`** checks L1 and then L2. A true index miss returns immediately; an
  admitted L2 candidate performs one bounded record read and local validation.

`put`, `put_l2`, and `delete` may return `WouldBlock`, but they never wait for
device I/O. `drain` waits for accepted mutations to complete; it is not a
durability sync.

A crash, drop, or `close_fast` reopens empty. `close_warm` is the only shutdown
that publishes a recoverable clean image. Prefer explicit async close because
dropping an open cache performs a synchronous fast close.

## Configuration

`StaticConfig` defines the persistent L2 layout. `RuntimeConfig` controls the
process-local topology and may normally change between opens.

### Static layout

- `StaticConfig::new(capacity_bytes)` uses 32 MiB Regions and assumes 16 KiB
  average live entries when sizing the fixed index.
- `with_region_size_bytes` changes Region geometry.
- `with_expected_entries` sizes the index for the expected simultaneously live
  key count, not lifetime writes.
- `peak_disk_bytes` reports the maximum cache-owned logical disk space.

Changing static layout safely cold-starts the cache. The complete geometry and
memory plan are validated before cache files are created.

### Runtime tuning

- **L1:** `with_l1_capacity_bytes`, `with_l1_shards`, and
  `with_l1_eviction_policy`. Zero capacity disables L1. Entries charged above
  256 KiB bypass L1 but remain valid in L2.
- **Reads:** `with_read_io_workers` and `with_read_io_wait_timeout`.
- **Writes:** `with_write_io_workers`, `with_append_shards`, and
  `with_write_flush_threshold_bytes`.
- **Reclaim:** `with_reclaim_workers`.
- **Memory:** `with_managed_memory_limit_bytes`.
- **I/O:** `with_io_engine` and `with_io_mode`.
- **Metrics:** `with_statistics`.

The defaults use buffered POSIX I/O, CLOCK, four read workers, four write
workers, four append shards, one reclaim worker, a 256 MiB L1, and a 1 GiB
managed-memory limit. Changing the append-shard count makes an existing clean
image ineligible and safely cold-starts the cache.

Read waiting is disabled by default, so read-engine or buffer pressure fails
open as a miss. A non-zero `with_read_io_wait_timeout` lets an L2 candidate wait
briefly for execution capacity. The queue allows at most one waiter per read
worker and holds no record buffer while waiting. Queue saturation, memory
pressure, or timeout is returned as explicit overload.

Buffered POSIX I/O is the production path. Direct I/O is an explicit Linux
mode. io_uring requires the `io-uring` feature and is experimental in 0.1.

## Deployment

C² accepts one data-file path. For multiple homogeneous SSDs, expose RAID0 or
an equivalent striped block device below the filesystem. Losing any member
discards the complete cache, which matches C²'s failure contract.

The managed-memory limit covers the index, L1, append and reclaim buffers,
metadata, cache-owned threads, recovery scratch, and transient reads. It does
not bound allocator metadata, Tokio, process overhead, or the kernel page cache.

The on-disk format is versioned, but 0.x releases do not promise cache-data
compatibility. Deployments must tolerate a cold start and should monitor
`Cache::startup_mode()`.

## Observability

### Metrics

`Cache::snapshot()` is lock-free. It always reports health and resource gauges;
`RuntimeConfig::with_statistics(true)` also enables cumulative cache and I/O
counters. `Cache::detailed_snapshot()` samples L1, index, write-buffer pressure,
and Region metadata, so call it slowly or on demand.

C² has no metrics SDK dependency. An OpenTelemetry or Prometheus adapter should
export cumulative values and derive rates in the backend:

- get outcomes from `l1_hits`, `l2_hits`, `l2_misses`, and
  `l2_read_overloads`;
- mutation volume and `write_rejections`;
- I/O requests, operations, bytes, request time, and slot-wait time;
- reclaim progress, managed memory, and cache health.

Use only fixed labels such as direction, path, and outcome. Never label by key,
file, Region, shard, or worker. Treat `metrics_epoch` as a reset marker, not a
label. `l1_misses` overlaps the L2 outcomes and must not be added to them.

Runtime file-operation counters describe application-level operations, not
physical device IOPS. Convert nanoseconds to seconds before export, and omit
activity series when statistics are disabled.

### Logs

Lifecycle, recovery, reclaim, and terminal failure events use the `log` facade
under `cache2::*`. Applications own the global logger. The included example
uses logforth:

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

## Further reading

- [Architecture](ARCHITECTURE.md) — data structures, request paths, reclaim,
  and recovery.
- [Validation](BENCHMARK.md) — benchmarks, mixed turnover, and Linux NVMe
  qualification.
