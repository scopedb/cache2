# C²

**A bounded RAM + SSD cache.**

C² (`cache2`) provides bounded, disposable acceleration for large file chunks. It keeps request paths short and cache-owned resources fixed.

- Sharded CLOCK or S3-FIFO L1 with a compact, fixed-size L2 index.
- Batched Region writes and independent read, write, and reclaim I/O capacity.
- Best-effort consistency with stale, fully validated hits.

## Quick start

```rust
use cache2::{Cache, CacheConfig, Error, ErrorKind, RuntimeOptions, StorageOptions};

async fn run() -> Result<(), Error> {
    let storage = StorageOptions::new(1024 * 1024 * 1024).build()?;
    let config = CacheConfig::new(storage, RuntimeOptions::default())?;
    let cache = Cache::open("/var/tmp/cache2.data", config).await?;

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

`open` uses the current Tokio runtime. Use `Cache::open_with_handle(path, config, handle)` to bind C² to another runtime; it must have time enabled and outlive the cache.

## Semantics

C² returns fully validated, potentially stale values. Misses, eviction, L1 bypass, and bounded overload are normal cache outcomes.

- Keys are raw bytes in one key space, up to 4 KiB. Each encoded record fits in one Region.
- Resource pressure returns a miss, bypass, or `ErrorKind::Overloaded` according to the operation.
- Sequence numbers provide advisory ordering for internal updates.

Public failures are `cache2::Error` values with an actionable `ErrorKind`, the failed `ErrorOperation`, and the original `std::io::Error` source. See [Error handling](cache2/ERRORS.md) for the classification table, retry policy, diagnostic fields, and migration from the former `io::Result` API.

### Operations

| Operation | Behavior                                                                                                        |
|-----------|-----------------------------------------------------------------------------------------------------------------|
| `put`     | Attempts immediate L1 admission and stages the value for L2. It returns after bounded in-memory admission.      |
| `put_l2`  | Stages the value for L2 and applies best-effort L1 cleanup. The value appears after its Region write publishes. |
| `get`     | Checks L1, then performs at most one bounded, locally validated L2 record read.                                 |
| `delete`  | Removes the current L2 mapping and applies best-effort L1 cleanup with bounded in-memory work.                  |
| `drain`   | Waits for accepted Region writes and L2 index publication.                                                      |

### Lifecycle

`close_fast`, drop, and an unclean exit make the next open a cold start. `close_warm` publishes a clean recovery image for a warm start. Both close methods work through `Arc<Cache>` without `Arc::try_unwrap`: the first close call immediately makes every shared handle inert, then fences accepted persistent work on Tokio's blocking pool. Prefer explicit async close because drop closes the cache synchronously. Retained handles may keep bounded in-memory resources allocated until they are dropped, but no longer admit public operations.

## Configuration

`StorageOptions` and `RuntimeOptions` are editable inputs. Build the storage options into a `StorageLayout`, then combine it with runtime options using `CacheConfig::new`. The resulting configuration is immutable and ready for `Cache::open(path, config)`.

Construction requires neither file access nor Tokio. Inspect `config.storage().peak_disk_bytes()` and `config.minimum_memory_bytes()` before opening; both queries reuse computed values. Clone a configuration to reuse it across paths or successive opens. File locks, device support, recovery, and actual allocations are checked when opening each instance.

### Persistent layout

`StorageOptions::new(capacity_bytes)` selects the total Region capacity, excluding headers, state, and recovery images. Its public fields allow explicit `region_size_bytes` and `expected_entries` values. Defaults are 32 MiB Regions and an index sized for 16 KiB average live entries. Build candidate layouts and compare `StorageLayout::peak_disk_bytes()` with the disk budget before choosing runtime tuning. Changing the persistent layout starts with an empty cache.

See the [configuration guide](CONFIGURATION.md#configuration-lifecycle) for examples, budget accounting, error boundaries, and migration from the builder API.

### Runtime tuning

| Area      | Controls                                                                   | Default and behavior                                                                        |
|-----------|----------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| L1        | `l1_capacity_bytes`, `l1_shards`, `l1_eviction_policy`                     | 256 MiB, 32 shards, CLOCK. Zero capacity disables L1; entries charged above 256 KiB use L2. |
| I/O pools | `io_engine: IoEngine::Posix(...)` or `IoEngine::IoUring(...)`              | Four POSIX read workers, four write workers, and one reclaimer; io_uring is experimental.   |
| Read wait | `read_admission: ReadAdmission::Immediate` or `ReadAdmission::Wait { .. }` | Immediate admission; wait capacity defaults to aggregate read capacity.                     |
| Writes    | `append_shards`, `write_flush_threshold_bytes`                             | Four append shards and a 4 MiB flush threshold.                                             |
| Memory    | `managed_memory_limit_bytes`                                               | 1 GiB across cache-managed allocations.                                                     |
| I/O mode  | `io_mode`                                                                  | Buffered I/O.                                                                               |
| Metrics   | `statistics`                                                               | Health and resource gauges enabled; cumulative activity counters opt in.                    |

Changing the append-shard count rebinds recovered Active Regions during a warm open. Growth uses available Free Regions; when there are not enough, the disposable cache safely starts empty.

The default `ReadAdmission::Immediate` returns a miss under read-engine or buffer pressure. `ReadAdmission::Wait` enables a queue bounded by `max_waiters` and a positive `timeout`. Queued requests retain their read plan and allocate a buffer after admission. Queue saturation, memory pressure, and timeout return explicit overload.

Buffered POSIX I/O is the production path. Direct I/O is an explicit Linux mode. io_uring requires the `io-uring` feature and remains experimental; its ring count and aggregate in-flight limit are independent. SQPOLL and IOPOLL are explicit per-pool opt-ins: SQPOLL adds kernel submission polling with configurable idle time and optional CPU affinity, while IOPOLL adds completion polling and requires direct I/O on polling-capable storage.

### Platform support

C² supports 64-bit Linux and macOS. Buffered positioned I/O is available on both platforms. Direct I/O and io_uring are Linux-only; io_uring is limited to the architectures listed by the optional `io-uring` feature. Other Unix targets may compile, but cache open returns `ErrorKind::Unsupported` when the platform cannot provide physical file preallocation. Windows is not supported.

## Deployment

C² accepts one data-file path. For multiple homogeneous SSDs, expose RAID0 or an equivalent striped block device below the filesystem. Losing any member discards the complete cache.

The managed-memory limit covers the index, L1, append and reclaim buffers, metadata, cache-owned threads, recovery scratch, and transient reads. Total deployment memory additionally includes allocator metadata, Tokio, process overhead, and the kernel page cache.

The on-disk format is versioned. During 0.x, deployments should expect cold starts across releases and monitor `Cache::startup_mode()`.

## Observability

### Metrics

`Cache::snapshot()` provides lock-free health and resource gauges. `RuntimeOptions { statistics: true, .. }` adds cumulative cache and I/O counters. `Cache::detailed_snapshot()` samples L1, index, write-buffer pressure, and Region metadata for periodic diagnostics.

C² exposes snapshots for integration with the application's metrics SDK. An OpenTelemetry or Prometheus adapter can export:

- get outcomes from `l1_hits`, `l2_hits`, `l2_misses`, and `l2_read_overloads`;
- mutation volume and `write_rejections`;
- I/O requests, operations, bytes, request time, and slot-wait time;
- reclaim progress, managed memory, and cache health.

Export counters cumulatively and derive rates in the backend. Use fixed labels such as direction, path, and outcome. Treat `metrics_epoch` as a reset marker. Report `l1_misses` separately because it overlaps L2 outcomes.

Runtime file-operation counters describe application-level operations; system telemetry supplies physical device IOPS. Convert nanoseconds to seconds before export. Activity series correspond to statistics-enabled opens.

### Logs

Lifecycle, recovery, reclaim, and terminal failure events use the `log` facade under `cache2::*`. Applications own the global logger. The included example uses logforth:

```sh
RUST_LOG=cache2=info cargo run --package examples --example logforth -- /tmp/cache2.data
```

`cache_opened` reports the index backing, mapping extent, validation mode, and whether warm mutations use copy-on-write. `cache_recovery_cold` records why a clean image was rejected or why private mapping fell back to a cold start. `cache_miss_only` records the first terminal index-validation or I/O failure.

## Development

C² requires Rust 1.98.0.

```sh
cargo x check
cargo x test
cargo x lint
```

The root workspace keeps the publishable crate, integration tests, benchmarks, examples, and repository tooling in separate members. See [Contributing](CONTRIBUTING.md) for the layout and repository workflows.

## Further reading

- [Configuration guide](CONFIGURATION.md) — parameter interactions, resource tradeoffs, goal-oriented profiles, and diagnostic tuning.
- [Architecture](ARCHITECTURE.md) — data structures, request paths, reclaim, and recovery.
- [Error handling](cache2/ERRORS.md) — structured classifications, operation context, overload policy, and standard I/O interoperability.
- [Validation](BENCHMARK.md) — benchmarks, mixed turnover, and Linux NVMe qualification.
- [Contributing](CONTRIBUTING.md) — workspace layout and development workflows.

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
