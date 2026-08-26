# cache-rs

`cache-rs` is a bounded, performance-first RAM + Region SSD HybridCache for file
chunks. It is disposable acceleration, not durable storage.

The persistence contract is intentionally narrow:

- ordinary `put` operations do not publish recoverable state;
- `close_fast` and process crashes reopen as an empty cache;
- only a successful `close_warm` publishes a clean recovery image;
- open never scans the data extent or replays dirty records;
- corrupt, stale, or static-config-mismatched recovery state is discarded.
- intact but unsupported cache-format versions are also discarded and reopened
  empty; cache data is never migrated or treated as authoritative.

## Usage

```rust
use std::time::Duration;
use cache_rs::{EvictionPolicy, HybridCacheConfig, IoEngine, RuntimeConfig, StaticConfig};

let static_config = StaticConfig::new(64 * 1024 * 1024 * 1024)
    .with_region_size(32 * 1024 * 1024)
    .with_expected_entries(1_000_000)
    .with_write_shards(8);
let peak_disk_bytes = static_config.peak_disk_bytes()?;

let runtime_config = RuntimeConfig::default()
    .with_l1_capacity(8 * 1024 * 1024 * 1024)
    .with_memory_limit(10 * 1024 * 1024 * 1024)
    .with_l1_shards(64)
    .with_eviction_policy(EvictionPolicy::Sieve)
    .with_io_engine(IoEngine::Auto)
    .with_io_workers(16)
    .with_io_concurrency(512)
    .with_waiting_write_limit(256)
    .with_write_flush_delay(Duration::from_millis(1));

let cache = HybridCacheConfig::from_static("/mnt/nvme/chunks.cache", static_config)
    .with_runtime_config(runtime_config)
    .open()?;

cache.put("chunk-key", b"chunk bytes")?;
let value = cache.get("chunk-key")?.expect("cache hit");
assert_eq!(value.as_ref(), b"chunk bytes");

// Waits for the async Region write and index publication.
cache.drain()?;

// Publishes the only state that a later process is allowed to recover.
cache.close_warm()?;
# Ok::<(), std::io::Error>(())
```

The default path uses namespace zero and never expires, so reads do not sample
the system clock. Namespaces and expiration are explicit extensions:

```rust
let namespace = 42;
cache.put_in(namespace, "stable-key", b"no expiration")?;
cache.put_until(namespace, "leased-key", b"expires", expires_at_unix_ms)?;

let stable = cache.get_in(namespace, "stable-key")?;
let leased = cache.get_in(namespace, "leased-key")?;

// Deterministic tests and batch callers may supply one shared clock sample.
let leased_at_sample = cache.get_in_at(namespace, "leased-key", now_unix_ms)?;
# Ok::<(), std::io::Error>(())
```

Expiration is checked lazily. A zero expiration never expires; otherwise the
entry is a miss when `expires_at_unix_ms <= now_unix_ms`. The system clock is
read only after a matching L1 or L2 record is found with a nonzero expiration.
Expired disk records consume space until normal Region rotation reclaims them.

Namespaces may also be assigned to physical RegionSets when workloads need
different SSD retention. Capacity weights divide the fixed Region count; each
set owns its active Regions and its own free/sealed FIFO, so rotation in one set
cannot reclaim another set. The append-shard count remains the global
`with_write_shards` value and is distributed evenly across sets in deterministic ID
order:

```rust
use cache_rs::{RegionSetConfig, StaticConfig};

let static_config = StaticConfig::new(96 * 1024 * 1024 * 1024)
    .with_region_size(32 * 1024 * 1024)
    .with_write_shards(8)
    .with_region_sets([
        RegionSetConfig::new(0).with_weight(3), // default, larger cold set
        RegionSetConfig::new(1)
            .with_weight(1)
            .with_namespaces([7]), // compact hot set
    ]);
let allocations = static_config.region_set_allocations()?;
# let _ = allocations;
# Ok::<(), std::io::Error>(())
```

The L1, fixed L2 index, I/O engines, memory limit, and statistics
remain shared. RegionSets are therefore physical SSD-retention partitions, not
independent cache instances or runtime quotas. Weight rounding is deterministic
at whole-Region granularity. Each set must receive one Region per assigned
append shard plus at least one spare Region; invalid layouts fail before files
are created. RegionSet zero is required and receives every namespace not listed
by another set. Layout and namespace ownership are static identity, so changing
either intentionally cold-starts empty.
`StaticConfig::validate()` checks the layout without touching cache files, and
`region_set_allocations()` reports the resolved Region count, capacity bytes,
and append-shard count for capacity planning.

`put` publishes an admitted RAM value immediately and encodes the same sequence
into the bounded per-shard write buffer before returning. The value is readable
from L1 while its device write is still in flight and is immediately eligible
for normal eviction. Values that do not fit their RAM shard bypass L1 and
continue through L2. Successful completion publishes the L2 index entry. The
default write backpressure policy rejects immediately when the fixed shard
write buffer is saturated; it has no global write-admission gate. Only an
explicitly selected `Block` or `Timeout` policy may wait for write-side
capacity. Any write may briefly wait for the shard mutation gate. Region
manager contention follows the selected backpressure policy after one
non-waiting probe. Neither path holds a lock across device I/O.

After an L1 miss, `get` always asks L2 to decide the result. A true L2 index
miss returns immediately without allocating a read buffer. An L2 candidate
plans one exact aligned read range, reserves an immediately available engine
slot, allocates that range, and performs one record read. The result is checked
locally against the planned location, sequence number, hash, namespace, full
key, lengths, expiry, and checksum. There is no second index lookup or global
freshness check; a concurrently superseded but otherwise valid record may be
returned. There is no read admission policy, fixed foreground read pool,
separate read queue, or background-ready state. The transient allocation is
charged to the aggregate hard memory limit; allocation or I/O submission
pressure fails open as a cache miss instead of returning read backpressure.
Write-side slot reservation limits write occupancy so the final slot of a
multi-entry I/O engine remains available to reads; reads may use the complete
engine depth. A waiting write receives the next eligible slot instead of
starving behind a read stream. A successful L1 promotion returns a bounded
L1-backed value and releases the transient read buffer before `get` returns
while preserving `CacheTier::L2` as the source of that hit. If promotion
bypasses, the zero-copy Region value retains only its exact-size allocation
until the caller drops it. I/O submission contention follows the same fail-open
rule. Point hashes and record sizes are
computed once per operation, and L1 admits at most eight distinct full keys for
one 64-bit hash so collision work remains constant-bounded.

Use `drain` when the caller needs all accepted Region writes completed, and
`flush` when completed data should also be issued through the device data-sync
primitive. Neither operation creates a recovery image.

## Configuration boundary

`StaticConfig` defines file-layout identity:

- capacity and Region size;
- index slot count;
- write-shard count;
- RegionSet capacity layout and namespace ownership;
- seeded XXH3-64 key hashing and its hash seed.

Changing one of these values safely formats an empty cache. `RuntimeConfig`
may change on every open without invalidating a clean image:

- sync/automatic/io_uring engine selection;
- buffered/automatic/direct I/O mode;
- any positive I/O worker count within the configured total I/O concurrency;
- total I/O concurrency and the waiting-policy write limit;
- L1 capacity, L1 shard count, eviction policy, aggregate memory
  limit, write-buffer size, batch size, flush delay, backpressure, and opt-in
  operational counters.

The RAM tier supports `Clock` (default), `Lru`, `TinyLfu`, `Sieve`, `Fifo`, and
`S3Fifo`. Victim search, multi-entry eviction, and frequency aging use fixed
per-operation work budgets; budget exhaustion bypasses L1. TinyLFU uses a
compact incrementally aged frequency sketch to admit candidates
against an LRU victim. S3-FIFO uses byte-targeted small/main queues and a
bounded hash-only ghost queue; a ghost collision can affect admission but never
key/value correctness because resident hits still verify namespace and the
complete key. The selected policy is runtime-only and may change on a warm
reopen without invalidating the Region image.

RAM is never recovered. A warm reopen maps the clean Region index and repopulates
L1 through read promotion.

`RuntimeConfig::l1_capacity_bytes()` is the charged L1 payload and entry
bound. A returned L1 `Value` retains its charge until the caller drops it, even
after CLOCK eviction, so slow consumers cannot push new L1 allocations past the
configured capacity. `RuntimeConfig::with_l1_shards()` controls only the
volatile L1 lock topology; the default is 32 and it may change on a warm reopen.
L1 entry slots, eviction metadata, free lists, and the open-addressed directory
are sized and allocated once during open. The directory consumes the already
computed seeded XXH3 key hash directly; it never invokes SipHash, reallocates,
or rehashes under a shard lock. Compact intrusive same-hash chains retain
namespace and full-key validation. The fixed entry count is derived from the
static expected-key/index plan and the L1/L2 capacity ratio, with enough slots
for a 4 KiB average L1 entry when the static key plan permits it. Smaller-entry
workloads may therefore reach the entry bound before the byte bound and bypass
or evict from L1 while continuing normally through L2.
The aggregate memory limit also reserves fixed 512 KiB stacks for append-shard,
I/O, and bounded shutdown-reaper threads plus conservative L1 shard, I/O command,
fixed L1 directory/policy, and recovery encoding metadata.
`StaticConfig::peak_disk_bytes()` reports the cache-owned logical disk bound,
including the data/state files and both recovery images that may coexist during
atomic warm publication.

Invalid runtime topology is rejected before cache files are opened or created.
`HybridCache::snapshot()` reports tier hits/misses, promotions, L1 evictions,
bypasses and TinyLFU admission rejections, served and written bytes,
overload/failure/rotation counters, lifecycle health, current and peak managed
memory against its configured limit, and the configured logical-disk peak.
Managed figures deliberately exclude allocator metadata, buffered-I/O page
cache, and filesystem metadata. Health and resource fields are always active.
Data-path counters reset on every open and are enabled with
`RuntimeConfig::with_statistics(true)`; they default off to keep atomic writes out of
the performance-first path. Index reuse/replacement counters follow the same
switch; fixed capacity and physical occupancy remain available when it is off.

`HybridCache::detailed_snapshot()` adds current/peak write admission,
cumulative write wait and rejection counters, aggregate worker I/O activity,
fixed/resident/retained L1 accounting, physical index occupancy and bounded
slot-reuse/replacement counters, and per-RegionSet capacity, occupancy, queue
state, and process-local rotations.
It adds no request latency instrumentation. Because it briefly locks Region
metadata and scans all Regions, use it for periodic diagnostics rather than on
the request hot path.
`l2_read_memory_misses` and `l2_read_busy_misses` identify the resource
subsets of `l2_misses` without changing the fail-open `get` result.

## Overload and health

Fixed per-shard write buffers bound write admission. A true L2 miss does not allocate a
buffer. An L2 candidate first reserves an engine execution slot and then
allocates only its actual aligned read range against the aggregate memory
limit; allocation or I/O-engine pressure is an observable cache miss. Write
occupancy cannot consume the final slot of a multi-entry I/O engine.
Promoted hits release the temporary allocation before return;
unpromoted zero-copy Region values retain it. For writes,
`WriteBackpressure::Reject` returns `WouldBlock` immediately from the shard
write buffer; `Block` waits for capacity and `Timeout` waits only for the configured
duration. `CacheSnapshot::write_rejections` counts all write-side overloads;
`detailed_snapshot()` separates write-gate and write-buffer pressure. A
device or structural failure increments
`io_failures` and moves health to `MissOnly` or `Failed`; reads then fail open
as misses, while writes remain explicit errors.

Use `close_fast` (or ordinary drop) when restart warmth is not worth an
O(index) image write. The next open is empty. Use `close_warm` only after the
service has stopped admitting application work and wants a recoverable clean
image. `drain` is an I/O completion fence and `flush` additionally asks the
device to data-sync, but neither makes the cache recoverable.

## Capacity planning

1. Choose a static capacity that is an exact Region-size multiple and provides
   at least one more Region than the shard count. With RegionSets, apply that
   rule independently to every weighted assignment: each set needs one active
   Region per assigned shard plus one spare. More Regions reduce turnover.
2. Set expected entries from the intended live key count; the fixed mmap index
   uses approximately 1.25 slots per entry and 32 bytes per slot. Up to
   512 million slots (16 GiB) supports a 1 TiB cache planned near 4 KiB per
   entry with the normal probe headroom.
3. Choose L1 capacity from the useful resident payload. The aggregate managed
   limit must additionally hold the fixed L1 directory/policy plan, index/Region
   metadata, two write buffers per shard, at least one maximum aligned record
   read, I/O command metadata, recovery scratch, and explicit cache-thread
   stacks. Concurrent reads consume only their actual aligned read sizes.
   Invalid plans fail before creating files; inspect `detailed_snapshot().l1`
   and `.index` to confirm the resulting entry and slot pressure.
4. Reserve at least `StaticConfig::peak_disk_bytes()` logical bytes on the cache
   device. Provision extra filesystem space for allocation granularity and
   metadata, which are outside the logical bound.
5. Start with four workers and measure 1/2/4/8/16 on the target device. Worker
   count, I/O concurrency, memory, write buffering, and backpressure are runtime settings and
   may change across a warm restart.

The reproducible baseline, Linux NVMe matrix, regression thresholds, and soak
procedure are in `BENCHMARK.md`.

## Benchmark

Run the release benchmark with:

```sh
cargo bench --bench hybrid_cache
```

It measures non-blocking `put` retries plus `drain`, resident L1 reads, warm
close, direct L2 reads with L1 promotion where admitted, and reads from the
promoted L1 set. The default data set is 8,192 × 16 KiB.
`CACHE_BENCH_ENTRIES`, `CACHE_BENCH_VALUE_BYTES`,
`CACHE_BENCH_RESIDENT_ENTRIES`, `CACHE_BENCH_READ_OPS`,
`CACHE_BENCH_CAPACITY_MIB`, `CACHE_BENCH_MEMORY_MIB`,
`CACHE_BENCH_MEMORY_LIMIT_MIB`, `CACHE_BENCH_SHARDS`,
`CACHE_BENCH_IO_WORKERS`, `CACHE_BENCH_CLIENTS`,
`CACHE_BENCH_IO_ENGINE`, `CACHE_BENCH_IO_MODE`, `CACHE_BENCH_STATS`, and
`CACHE_BENCH_EVICTION`, and `CACHE_BENCH_DIR` adjust the workload and device
configuration. Set
`CACHE_BENCH_DIR` to a mounted cache device when measuring device I/O rather
than the system temporary directory.
Keys are generated on demand and values contain their key ordinal, so a large
L2 data set does not allocate one generator object per key and a wrong-key read
is a hard failure. The resident L1 phase uses only the configured bounded
subset and explicitly warms that subset before measurement; target-device
qualification requires an L2 data set larger than host RAM.
The configurable turnover soak is available through
`cargo bench --bench hybrid_cache_soak`; use the multi-hour device command in
`BENCHMARK.md` for M2 validation. Its engine and mode are independently
selectable with `CACHE_SOAK_IO_ENGINE` and `CACHE_SOAK_IO_MODE`. By default it
cycles 256 B, 4 KiB, 16 KiB, and 256 KiB values. A validated older version is
counted as `stale_hits`; a future version, wrong key, malformed value, resource
bound violation, or runtime failure terminates the run.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the data and recovery paths,
[BENCHMARK.md](BENCHMARK.md) for the current reproducible baseline, and
[MILESTONES.md](MILESTONES.md) for maturity stages and release criteria.
[COMPATIBILITY.md](COMPATIBILITY.md) records the candidate API and format policy.
[SUPPORTED_PLATFORMS.md](SUPPORTED_PLATFORMS.md) separates validated and pending
platform paths, [CANARY.md](CANARY.md) defines the workload sign-off evidence,
and [CHANGELOG.md](CHANGELOG.md) records compatibility changes.
