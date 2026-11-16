# C²

**A bounded RAM + SSD cache for large file chunks.**

C² (`cache2`) keeps frequently reused values in memory (L1) and batches writes to a file-backed cache (L2). Cache-owned memory, queues, and I/O concurrency have explicit bounds. Use it as disposable acceleration in front of an authoritative data source: entries may be evicted, reads may miss under pressure, and a validated hit may still be stale.

## Quick start

Add the cache and a Tokio runtime to your application:

```toml
[dependencies]
cache2 = "0.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Build a storage layout, combine it with runtime options, and open a cache file. This example reserves 1 GiB of Region capacity plus metadata and recovery space. The parent directory must exist, and the filesystem must support physical file preallocation.

```rust
use cache2::Cache;
use cache2::CacheConfig;
use cache2::Error;
use cache2::ErrorKind;
use cache2::RuntimeOptions;
use cache2::StorageOptions;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let storage = StorageOptions::new(1024 * 1024 * 1024).build()?;
    let config = CacheConfig::new(storage, RuntimeOptions::default())?;
    let cache = Cache::open("cache.data", config).await?;

    // Fill with bytes already obtained from the authoritative source.
    match cache.put(b"chunk:42", b"cached bytes") {
        Ok(_) => {}
        Err(error) if error.kind() == ErrorKind::Overloaded => {
            // Skipping a cache fill must not fail the application request.
        }
        Err(error) => return Err(error),
    }

    match cache.get(b"chunk:42").await? {
        Some(value) => println!("cache hit: {} bytes", value.len()),
        None => println!("cache miss: read from the authoritative source"),
    }

    cache.close_warm().await
}
```

`Cache::open` uses the current Tokio runtime, which must have time enabled and outlive the cache. Use `Cache::open_with_handle` to select another runtime. Share an open cache through `Arc<Cache>`.

## Integration contract

- Keys and values are bytes. Keys may be at most 4 KiB, and each encoded key/value record must fit in one Region. Use keys that identify the version of the source data when freshness matters.
- Misses, eviction, L1 bypass, and overload are normal cache outcomes. Keep the authoritative path available and bound retries. See [Error handling](ERRORS.md) for the complete policy.
- Successful `put` and `put_l2` calls admit work into bounded memory. `drain` waits for accepted Region writes and L2 publication; it does not publish a restart image.
- `close_warm` publishes a clean recovery image. `close_fast`, drop, and an unclean exit make the next open a cold start. Prefer explicit async close because drop can block. See [Operating a cache](OPERATIONS.md#shutdown-and-restart).
- Configuration construction performs no file access and needs no runtime. Inspect `StorageLayout::peak_disk_bytes()` and `CacheConfig::minimum_memory_bytes()` before opening, then leave memory headroom for concurrent reads and retained values. See [Configuration](CONFIGURATION.md).

## Platform support

C² requires Rust 1.98.0 and supports 64-bit Linux and macOS. Buffered POSIX I/O is the default. Direct I/O is Linux-only; the optional `io-uring` feature makes the experimental backend available on supported Linux architectures, but selecting it also requires `IoEngineOptions::IoUring`. See [I/O engine and mode](CONFIGURATION.md#io-engine-and-mode) for configuration and platform requirements. Windows is not supported.

## Next steps

| Task                         | Guide                                                                                    |
| ---------------------------- | ---------------------------------------------------------------------------------------- |
| Budget and tune a cache      | [Configuration](CONFIGURATION.md) — storage, memory, concurrency, and workload profiles. |
| Deploy and operate           | [Operating a cache](OPERATIONS.md) — shutdown, recovery, metrics, and logs.              |
| Handle errors                | [Error handling](ERRORS.md) — overload, diagnostics, and standard I/O interoperability.  |
| Look up public contracts     | [API reference](https://docs.rs/cache2) — types, methods, and examples.                  |
| Understand design invariants | [Architecture](ARCHITECTURE.md) — request paths, reclamation, and persistence.           |
| Qualify a workload and host  | [Validation](BENCHMARK.md) — benchmark methodology and Linux NVMe qualification.         |

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
