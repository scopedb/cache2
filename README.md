# C²

**A bounded RAM + SSD cache for large file chunks.**

C² (`cache2`) provides disposable acceleration in front of an authoritative data source. It combines a sharded in-memory cache with batched SSD writes, a fixed-size L2 index, and independent read, write, and reclaim I/O capacity. Cache-owned memory and request admission have explicit bounds.

A validated hit may be stale, and pressure may produce a miss or overload. Use versioned keys when freshness matters and keep the authoritative path available.

## Getting started

```toml
[dependencies]
cache2 = "0.6"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "time"] }
```

Start with the [integration example](cache2/README.md#quick-start) or the [crate documentation](https://docs.rs/cache2). Both cover opening a cache, filling and reading values, and shutting down. C² requires Rust 1.98.0 and supports 64-bit Linux and macOS; buffered POSIX I/O is the default, with Linux Direct I/O and experimental io_uring available explicitly.

## Documentation

| I want to…                            | Read                                                                                 |
| ------------------------------------- | ------------------------------------------------------------------------------------ |
| Integrate a cache                     | [Getting started](cache2/README.md) — example, cache semantics, and runtime binding. |
| Size storage, memory, and concurrency | [Configuration](cache2/CONFIGURATION.md) — budgets, interactions, and tuning.        |
| Deploy, restart, and monitor          | [Operating a cache](cache2/OPERATIONS.md) — lifecycle, health, metrics, and logs.    |
| Handle pressure and failures          | [Error handling](cache2/ERRORS.md) — classifications, policies, and diagnostics.     |
| Look up a type or method              | [API reference](https://docs.rs/cache2) — contracts and examples.                    |
| Understand the implementation         | [Architecture](cache2/ARCHITECTURE.md) — data structures and invariants.             |
| Qualify performance                   | [Validation](cache2/BENCHMARK.md) — workloads and Linux NVMe qualification.          |
| Upgrade a deployment                  | [Changelog](CHANGELOG.md) — release changes and migration guidance.                  |

The guides live with the library so that they are also available in its published source package. The API reference includes a self-contained introduction and links to the relevant public contracts.

## Development

The workspace separates the publishable library, integration tests, benchmarks, examples, and repository tooling. See [Contributing](CONTRIBUTING.md) for setup and workflows.

```sh
cargo x check
cargo x test
cargo x lint
```

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
