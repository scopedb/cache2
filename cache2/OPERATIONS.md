# Operating a cache

This guide covers deployment, shutdown, recovery, and monitoring after the [first integration](README.md). Use [Configuration](CONFIGURATION.md) to choose budgets and [Error handling](ERRORS.md) for caller-side failure policies.

## Deployment budgets

C² accepts one data-file path and creates associated state and recovery files. Reserve space using `StorageLayout::peak_disk_bytes()`, which includes more than the requested Region capacity. One open instance exclusively owns those files. For multiple homogeneous SSDs, expose RAID0 or an equivalent striped block device below the filesystem; losing any member discards the complete cache.

The managed-memory limit covers cache-managed allocations, including the index, L1, append and reclaim buffers, metadata, cache-owned threads, recovery scratch, and transient reads. It is not an RSS limit: allocator metadata, Tokio, application overhead, mapped-file residency, and the kernel page cache are outside it. Leave dynamic headroom above `CacheConfig::minimum_memory_bytes()` for concurrent reads and values retained by callers. See the [resource model](CONFIGURATION.md#resource-model) for sizing interactions.

## Shutdown and restart

Choose the shutdown operation according to whether the next open should reuse cached data:

| Operation    | Completion and restart behavior                                                                                                   |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------- |
| `drain`      | Waits for accepted Region writes and L2 index publication; keeps the cache open and does not publish a recovery image.            |
| `close_warm` | Fences accepted persistent work and publishes a clean recovery image for the next open. A failed close publishes no usable image. |
| `close_fast` | Fences accepted persistent work and makes the next open start empty.                                                              |
| Drop         | Performs a synchronous fast close and can block the dropping thread.                                                              |
| Unclean exit | The next open starts empty. Mutations do not provide crash durability.                                                            |

Prefer explicit async close and await its result before shutting down the Tokio runtime. Both close methods work through `Arc<Cache>` without `Arc::try_unwrap`. The first close call immediately makes every shared handle inert: gets miss, and mutations and snapshots return unavailable. Shutdown continues even if its returned future is dropped. Retained handles and returned values may keep bounded in-memory resources allocated until they are dropped.

Warm recovery validates the saved image and may safely fall back to an empty cache. Changing the persistent layout prevents reuse. Changing append-shard count rebinds recovered Active Regions; growth uses Free Regions and falls back to cold when there are too few. Inspect `Cache::startup_mode()` after opening instead of assuming that a previous warm close guarantees reuse.

The on-disk format is versioned. During 0.x, deployments should expect cold starts across releases. Keep the authoritative data path ready to absorb a cold cache.

## Health and background pressure

`Cache::snapshot()` reports `CacheHealth`. Treat health separately from request outcomes: overload can occur while the cache is healthy, and a terminal cache failure can become a read miss instead of an application error.

A timed-out background write or reclaim enters `CacheHealth::Recovering`: new fills return overload while reads and deletes continue. Fills resume when the original operations finish. `RuntimeOptions::io_recovery_timeout` defaults to `None`, waiting for completion or close; a finite recovery budget can terminate the instance. See [background I/O recovery](CONFIGURATION.md#background-io-recovery) and [Error handling](ERRORS.md#transient-background-timeouts) for the boundaries.

Optional `FillControlOptions::Observe` reports pre-timeout fill pressure without rejecting fills. `Adaptive` also pauses new fills and paces non-essential flushes. These pauses do not change `CacheHealth::Running`. Inspect `CacheSnapshot::fill_control` to distinguish a controller pause from staging pressure. See [adaptive fill admission](CONFIGURATION.md#adaptive-fill-admission) for tuning.

## Metrics

Choose the snapshot API according to the information and collection cost:

| API                        | Information                                                                                                      | Collection policy                                                                                  |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `Cache::snapshot`          | Health and resource gauges; cumulative activity and I/O counters when enabled.                                   | Gauges are always available; enable `RuntimeOptions::stats.activity_counters` for activity series. |
| `Cache::stats_snapshot`    | The summary plus optional public request outcomes and request/I/O latency histograms, without scanning metadata. | Configure `StatsOptions` before opening; repeated readers do not reset counters.                   |
| `Cache::detailed_snapshot` | L1, index, write-buffer pressure, I/O, and Region metadata in addition to the summary.                           | Use for periodic diagnostics; it scans metadata and should stay off the request path.              |

`StatsOptions` independently enables request outcomes, L1-hit, L2-lookup and mutation latency (`Off`, `Full`, or `Sampled`), and full I/O latency by read/write/reclaim role. Structured request rows include their timing scope and collection mode. Full timing avoids sampling work; sampled histograms retain actual sample counts and cannot guarantee observation of rare tail events. Recorder storage is preallocated, bounded, and charged to managed memory.

Applications own metric conversion, timestamps, scheduling, and transport. From a repository checkout, run the integration example with:

```sh
cargo run --package examples --example stats -- <cache-data-path>
```

An OpenTelemetry or Prometheus adapter can export:

- Get outcomes from `l1_hits`, `l2_hits`, `l2_misses`, and `l2_read_overloads`.
- Mutation volume and `write_rejections`.
- I/O requests, operations, bytes, request time, and slot-wait time.
- Reclaim progress, managed memory, and cache health.

Export counters cumulatively and derive rates in the backend. Use fixed labels such as direction, path, and outcome. Treat `metrics_epoch` as a reset marker. Report `l1_misses` separately because it overlaps L2 outcomes. Convert nanoseconds to seconds before export.

Runtime file-operation counters describe application-level operations; system telemetry supplies physical device IOPS. Use the [diagnostic map](CONFIGURATION.md#diagnostic-map) to connect observed pressure to configuration changes.

## Logs

Lifecycle, recovery, reclaim, and terminal failure events use the `log` facade under `cache2::*`. Applications own the global logger. From a repository checkout, run the logforth integration example with:

```sh
RUST_LOG=cache2=info cargo run --package examples --example logforth -- /tmp/cache2.data
```

`cache_opened` reports the index backing, mapping extent, validation mode, and whether warm mutations use copy-on-write. `cache_recovery_cold` records why a clean image was rejected or why private mapping fell back to a cold start. `cache_miss_only` records the first terminal index-validation or I/O failure. Log messages are diagnostic text; use structured error classifications and snapshots for application policy and metric labels.
