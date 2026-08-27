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
- Full keys, exact record lengths, Region generations, hashes, locations, and
  checksums are validated before an L2 value is returned.
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
    .with_reclaim_workers(2)
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

POSIX positioned I/O with `IoMode::Buffered` is the default production path.
`IoMode::Direct` explicitly enables Linux `O_DIRECT` for aligned runtime record
I/O and reports direct-I/O errors without retrying through buffered I/O. Control,
recovery, and necessarily unaligned remainder I/O stay buffered. Linux io_uring
is an explicit `io-uring` crate feature and runtime selection.

Each append shard owns one Active Region, two Region-sized staging buffers, and
one ordered append worker. Read and write I/O workers bound separate execution
pools; increasing append shards can raise write parallelism but has a direct
fixed-memory cost.

Keys are limited to 4 KiB. A complete encoded record must fit in one Region.
Entries charged above 256 KiB bypass L1 but remain valid in L2.

The fixed L2 index uses roughly two slots per expected entry and 10 bytes per
slot, plus page headers. A 4 TiB cache averaging 16 KiB per entry therefore
needs about 5.08 GiB for the index. The aggregate managed-memory limit must also
cover L1, two staging buffers per append shard, one Region reclaim buffer,
metadata, transient reads, and cache thread stacks. Invalid plans fail before
cache files are created.
`StaticConfig::peak_disk_bytes()` reports the cache-owned logical disk bound.

## Operations

`Cache::snapshot()` is lock-free and exposes health, resource bounds, optional
cache activity, and read/write I/O counters split by buffered/direct path.
Enable activity counters with `RuntimeConfig::with_statistics(true)`. Runtime
file-operation counts are application-observed I/O, not physical device IOPS;
buffer cache, readahead, and the block layer remain outside C².

Export cumulative values rather than calculating rates inside C². An upper
OpenTelemetry adapter can sample the snapshot on its Tokio runtime and use the
following instruments. The same instruments translate directly to the shown
Prometheus names:

| Meaning | OpenTelemetry | Prometheus |
| --- | --- | --- |
| Get outcomes | `cache2.get.operations`, unit `{operation}` | `cache2_get_operations_total` |
| Accepted mutations | `cache2.mutations`, unit `{operation}` | `cache2_mutations_total` |
| Mutation rejections | `cache2.mutation.rejections`, unit `{operation}` | `cache2_mutation_rejections_total` |
| Logical payload bytes | `cache2.data.io`, unit `By` | `cache2_data_io_bytes_total` |
| Runtime file operations | `cache2.io.operations`, unit `{operation}` | `cache2_io_operations_total` |
| Runtime file bytes | `cache2.io`, unit `By` | `cache2_io_bytes_total` |
| Engine submissions | `cache2.io.requests`, unit `{request}` | `cache2_io_requests_total` |
| Terminal outcomes | `cache2.io.request.completions`, unit `{request}` | `cache2_io_request_completions_total` |
| Request time | `cache2.io.request.time`, unit `s` | `cache2_io_request_time_seconds_total` |
| Slot-wait time | `cache2.io.slot.wait`, unit `s` | `cache2_io_slot_wait_seconds_total` |
| Requests in flight | `cache2.io.request.in_flight`, unit `{request}` | `cache2_io_requests_in_flight` |
| Region reclaim | `cache2.reclaim`, unit `{operation}` | `cache2_reclaim_total` |
| Reclaim bytes | `cache2.reclaim.io`, unit `By` | `cache2_reclaim_io_bytes_total` |
| Managed memory | `cache2.memory.usage`, unit `By` | `cache2_memory_usage_bytes` |

Use fixed `direction=read|write`, `path=buffered|direct`, and
`outcome=success|cancelled|error` attributes. Cache keys, paths, Regions,
shards, and workers must not become labels. `metrics_epoch` is a reset marker
for the adapter, not a label: when it changes, use the current cumulative value
as the first delta for the new open.

The read direction includes the dedicated reclaim lane so request and runtime
file counters remain additive. `reclaim` separately exposes Regions completed,
second chances, sequential bytes, records scanned, and index entries removed.

For get outcomes, export `l1_hits`, `l2_hits`, and `l2_misses` as the disjoint
`result=l1_hit|l2_hit|miss` series; `l1_misses` overlaps the latter two and must
not be summed into that result set. If `statistics_enabled` is false, omit
activity series rather than exporting misleading zero traffic; health and
resource gauges remain available.

Prometheus derives runtime IOPS with
`rate(cache2_io_operations_total[5m])`, throughput with
`rate(cache2_io_bytes_total[5m])`, and average operation size by dividing the
byte rate by the operation rate. The current counters support average request
time, not latency percentiles.

`Cache::detailed_snapshot()` additionally reports L1/index pressure, staging,
and Region occupancy. It briefly locks and scans Region metadata, so sample it
slowly or on demand rather than from a scrape callback.

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
