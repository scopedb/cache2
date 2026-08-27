# C²

**C²** (`cache2`) is a bounded, performance-first RAM + SSD cache for file
chunks. It is disposable acceleration, not durable storage.

## Contract

- Cache contents are never authoritative. Hits may be stale; misses, eviction,
  bypass, and overload are valid outcomes.
- `put` and `delete` use bounded in-memory admission and may return
  `WouldBlock`. They never wait for device I/O.
- An L1 miss consults L2. An admitted L2 lookup performs one bounded record
  read; memory or I/O pressure fails open as a miss.
- Full keys, lengths, sequence numbers, locations, and checksums are validated
  before an L2 value is returned.
- There is one raw byte-key space and no TTL. Encode any logical namespace into
  the key.
- `drain` waits for accepted writes to complete but is not a durability sync.
  Crashes, drop, and `close_fast` reopen empty. Only `close_warm` publishes a
  recoverable clean image.

## Usage

```rust
use cache2::{CacheBuilder, RuntimeConfig, StaticConfig};
# async fn run() -> std::io::Result<()> {

let static_config = StaticConfig::new(64 * 1024 * 1024 * 1024)
    .with_region_size_bytes(32 * 1024 * 1024)
    .with_expected_entries(1_000_000);

let runtime_config = RuntimeConfig::default()
    .with_l1_capacity_bytes(8 * 1024 * 1024 * 1024)
    .with_managed_memory_limit_bytes(10 * 1024 * 1024 * 1024)
    .with_l1_shards(64)
    .with_read_io_workers(16)
    .with_write_io_workers(4)
    .with_append_shards(8);

let cache = CacheBuilder::from_static("/mnt/nvme/chunks.cache", static_config)
    .with_runtime_config(runtime_config)
    .open()
    .await?;

cache.put("chunk-key", b"chunk bytes")?;
let value = cache.get("chunk-key").await?.expect("cache hit");
assert_eq!(value.as_ref(), b"chunk bytes");

cache.delete("chunk-key")?;
cache.drain().await?;
cache.close_warm().await?;
# Ok(())
# }
```

`open()` captures the current Tokio runtime. Use
`CacheBuilder::with_tokio_handle` to bind the cache to another runtime; that
runtime must have time enabled and outlive the cache.

## Configuration

`StaticConfig` defines disk identity: capacity, Region size, index slots derived
from expected entries, and the key-hash identity. A mismatch safely opens an
empty cache.

`RuntimeConfig` selects L1 capacity and shards, aggregate managed-memory limit,
append shards, independent read/write I/O workers, I/O mode, write flush
threshold, and optional statistics. Runtime tuning may change across opens,
except that an append-shard count different from the clean image cold-starts
empty.

POSIX positioned I/O is the default production path. `IoMode::PreferDirect`
prefers Linux `O_DIRECT` for aligned runtime record I/O and falls back to
buffered I/O when unavailable; `Buffered` and strict `Direct` modes are also
available. Control and recovery I/O remain buffered. Linux io_uring is an
explicit `io-uring` crate feature and runtime selection.

Each append shard owns one Active Region, two Region-sized staging buffers, and
one ordered append worker. Read and write I/O workers bound separate execution
pools; increasing append shards can raise write parallelism but has a direct
fixed-memory cost.

Keys are limited to 4 KiB. A complete encoded record must fit in one Region.
Entries charged above 256 KiB bypass L1 but remain valid in L2.

The fixed L2 index uses roughly 1.25 slots per expected entry and 24 bytes per
slot, plus page headers. A 4 TiB cache averaging 16 KiB per entry therefore
needs about 7.62 GiB for the index. The aggregate managed-memory limit must also
cover L1, two staging buffers per append shard, metadata, transient reads, and
cache thread stacks. Invalid plans fail before cache files are created.
`StaticConfig::peak_disk_bytes()` reports the cache-owned logical disk bound.

## Operations

`Cache::snapshot()` exposes health, resource bounds, and optional activity
counters. Enable request-path counters with
`RuntimeConfig::with_statistics(true)`. `Cache::detailed_snapshot()` also
reports L1/index pressure, staging, worker I/O, and Region occupancy; it is an
on-demand diagnostic and should not run on the request hot path.

Structural or device failure moves reads to fail-open misses when safe operation
cannot continue. Writes remain explicit errors. Normal cache misses and request
paths are not logged.

Lifecycle and recovery events use the `log` facade under `cache2::*`. The
application owns the global logger; the included example uses logforth:

```sh
RUST_LOG=cache2=info cargo run --example logforth -- /tmp/cache2-logforth.data
```

## Development

The project requires Rust 1.98.0.

```sh
cargo +1.98.0 fmt --all -- --check
cargo +1.98.0 test --all-features
cargo +1.98.0 clippy --all-targets --all-features -- -D warnings
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the request and recovery paths and
[BENCHMARK.md](BENCHMARK.md) for performance and device validation.
