# cache-rs

`cache-rs` provides a bounded RegionLog disk cache and a production-candidate
Hybrid cache for mixed small/large objects. The Hybrid path combines a sharded
DRAM LRU, a fixed-bucket small-object SSD engine, and the RegionLog large-object
SSD engine under one session manifest, policy controller, resource budget,
async facade, and operations surface. Version 1.1 is source-complete for the
single-device implementation; deployment readiness still depends on the
target-environment evidence stated below.

Production qualification is deliberately separate from source completion.
Target-NVMe throughput/p99/profile, TB-scale recovery SLA, workload
hit-rate/DWPD, 24--72 hour soak, canary, and real-power-loss sign-offs must still
be run in the deployment environment; this repository claims no hardware
result.

## Hybrid cache baseline

```text
HybridCache
├── MemoryEngine          bounded sharded LRU (L1)
└── DiskPair              size-routed SSD tier (L2)
    ├── BucketCache       small objects; fixed bucket, no per-entry DRAM index
    └── DiskCache         large objects; append-only RegionLog + compact index
```

The default is the performance-first **write-back** mode: a `put` publishes a
dirty L1 value after allocating a process-local version. `flush`/`close` drain
resident dirty values; eviction normally persists one in the selected SSD
engine, but may deliberately lose it to a miss under bounded executor pressure
after durably deleting any older L2 candidate. Steady-state mutations do not
append a route-journal record or issue a durability sync. Queue slots, workers,
and owned task bytes are hard-bounded. Oversize/nonresident L1 paths retain
explicit backpressure or write-through fallback semantics independently of
eviction pressure. `HybridWriteMode::WriteThrough` remains available when
disk-first publication is required.

L1 values use shared immutable `Arc` storage. `get_handle` returns a
`HybridValueHandle`, so an L1 hit only increments a reference count while the
shard is locked and does not copy the payload. The compatibility `get`/`lookup`
APIs still return an owned `Vec<u8>` and perform that copy after releasing the
L1 shard lock.

The shared Hybrid request-byte gate charges compatibility-API L1 return clones
and both Bucket/Region candidates during a cross-size read. It reserves the
current record sizes after same-key ordering;
`HybridConfigDiagnostics::maximum_read_temporary_bytes` reports the one-request
worst case for capacity planning.

Same-key mutations and lower-tier lookup/publication are linearized by a Hybrid
ordering stripe. The L1 fast path uses only its memory shard; after an L1 miss,
the exact-key pending directory masks an in-flight older lower candidate.
`lookup` reports whether a hit came from memory, the small-object disk engine,
or RegionLog, and absolute TTL is preserved during promotion/demotion.

Small and large disk data use distinct dedicated files plus a third global
manifest file. Open persists one session-level dirty fence before traffic; a
successful `flush` publishes both lower checkpoints and the matching clean
manifest, then re-arms that dirty fence before returning. `close` drains dirty
L1 values and publishes the final clean lower/global checkpoint. Consequently,
an unclean session may cold-start the disposable lower tiers instead of replaying
each mutation, and an older disk value is never exposed as the result of a lost
dirty L1 value. The bounded route-journal decoder remains for compatible recovery
of existing non-empty journal generations, but it is not on the steady-state
mutation path.

A dirty victim may leave L1 before its SSD write completes after it registers in
the bounded exact-key pending directory. While pending, reads mask any older
lower value and same-key mutations wait without holding the coarse ordering
lock. Lower-absent writes use at most 75% of the executor and may be dropped to
a miss under pressure. Lower-candidate writes have priority: when admitting the
full value keeps projected slot/byte occupancy at or below 75% they persist it;
at higher pressure they schedule only a smaller durable deletion of the stale
L2 value and drop the
dirty value to a miss. The exact-key fence remains until deletion completes. If
even that work cannot be allocated, registered, or admitted, the dirty victim
remains resident and the incoming cache put is rejected rather than performing
foreground device I/O.

Compatible non-empty journal recovery is two-pass and hard-bounded. A first
streaming pass uses at most 64 KiB scratch to validate structure, CRC, generation, versions,
and density. Only a normal recoverable journal receives a second pass, stored
as one exact encoded prefix plus one `u32` offset per intent—never one `Vec` per
key or geometric growth. The conservative retained bound is
`journal_capacity + 4 × floor(journal_capacity / 96)` and is exposed as
`HybridConfigDiagnostics::journal_recovery_memory_bytes`. A valid non-empty
journal invalidates its touched routes before usage is rebuilt. Dirty plus an
empty journal, a Clear boundary, or a torn/corrupt/untrusted journal instead
safe-clears both lower tiers; an unaddressable/overflowing journal returns an
explicit error. None of those fallback paths retains the raw prefix.

Both disk tiers offer bounded sync/`io_uring` I/O and buffered/auto/required
`O_DIRECT` modes, while `AsyncHybridCache` provides one shared bounded facade
with cancellation/deadline and drain-close semantics. Hybrid shutdown reserves
its coordinator thread before admission can stop; `close_with_timeout` and
`AsyncHybridCloseFuture::wait_timeout` return `TimedOut` without cancelling the
drain or releasing any file lock. A later close joins the same owner. Future
waker registration is capped at 64 removable slots and overload is explicit;
synchronous waiters do not consume those slots. `HybridHealthSnapshot` keeps
manifest, Bucket, and Region degradation visible independently.

An async read remains cancellable only until it needs an irreversible cleanup.
Dirty-L1 expiry, Bucket expiry/corruption, and Region expiry/corruption first
atomically claim a read-side commit point. If cancellation wins, no mutation or
quota refund follows; if cleanup wins, `cancel()` returns `TooLate` and the
request delivers its real completion after the dirty fence and cleanup finish.

## What v1.1 includes

- fixed-size regions with append-only records and selectable FIFO or bounded
  second-chance region reuse;
- a fixed-capacity 32-byte-per-slot in-memory hash index split across at most
  4,096 independently locked shards, with up to 268,435,456 slots;
- explicit little-endian superblock, region, and record codecs;
- CRC32C for metadata and payloads, plus full-key verification on reads;
- `put`, `get`, `remove`, TTL, `clear`, `flush`, and statistics;
- alternating index-checkpoint slots containing Region incarnation/used/max
  seqno snapshots and compact index entries, protected by header and payload
  CRC32C;
- checkpoint payload v4 persists every Active Region's owning lane plus the
  compact-index layout and physical slots; the reader remains compatible with
  v1/v2/v3 checkpoints and reconstructs legacy lane identity conservatively
  before opening traffic;
- payload-first, commit-header-last checkpoint publication followed by an
  exactly paired clean Superblock, so an interrupted generation is never
  selected as clean;
- clean restart from the matching checkpoint without record replay, and dirty
  restart from the previous clean checkpoint plus a bounded scan of only
  changed Region tails/incarnations;
- tombstones and epoch barriers that prevent removed or pre-`clear` values from
  being resurrected by checkpoint fallback or incremental recovery;
- `RecoveryMode::Blocking` and `RecoveryMode::MissOnly`; the latter returns
  stable misses and rejects mutations while recovery runs, then atomically
  opens normal traffic after publishing a new clean checkpoint;
- standalone `DiskCache` coalesces periodic checkpoints after at least 256 MiB
  of admitted record bytes; the implicit threshold grows to 16 times the
  maximum index snapshot size for a large index, while an explicitly
  configured value is exact and `0` disables periodic publication. A Region
  managed by `HybridCache` deliberately disables autonomous publication:
  `HybridCache::flush()`/`close()` own the matching Region, Bucket, and global
  manifest/namespace-usage boundary;
- non-blocking exclusive file locking so two instances cannot write the same
  cache file;
- bounded short-I/O loops and checked offset/length arithmetic;
- an internal synchronous `IoBackend` with named record, Region Header,
  Superblock, and durability-barrier persistence points;
- deterministic short/torn write, `EIO`, `ENOSPC`, and sync-failure injection,
  plus real subprocess `SIGKILL/restart` coverage at record, Region rotation,
  clear, checkpoint payload/header, and Superblock persistence boundaries;
- committed Format V1 golden fixtures and explicit rejection of unsupported or
  unrecognized non-empty formats without modifying them;
- an observable `Healthy` / `MissOnly` / `Poisoned` / `Closed` runtime state;
- independent bounded read, write, and reserved control admission gates;
- dynamically sized, lazily allocated 4 KiB-aligned data-buffer pools, capped
  at 128 read and 128 write slots, plus fixed control and metadata reserves;
- Linux `MAP_SHARED | MAP_ANONYMOUS` mappings for aligned buffers, with both
  old and new mappings charged during growth so transient allocation stays
  under the hard logical memory budget;
- a unified logical memory budget for the index, region/recovery metadata,
  scratch buffers, I/O queue bookkeeping, and bounded async request inputs;
- explicit reject, caller-blocking, and timeout backpressure policies;
- an optional token-bucket write budget that rejects a `put` before dirtying
  metadata, changing the index, or issuing I/O;
- queue, buffer, rejection, wait-time, and memory current/peak statistics;
- 256 key-ordering stripes for mutations, optimistic lock-free-by-key reads,
  and location/seqno/incarnation revalidation;
- per-Region read pins, so reuse waits only for readers of the victim Region
  while unrelated `get` I/O continues;
- generation floors and per-Region counters that make entry statistics
  constant-time, `clear` independent of index-slot count, and Region reuse
  proportional to records in the victim instead of total index capacity;
- one hash-selected append lane by default, configurable up to eight; each lane
  owns one Active Region and batches an already queued put prefix into at most
  64 records and 128 KiB of coalesced data per positioned write;
- an owned-buffer internal `IoEngine` with hard-capped queue depth, cancellation,
  Future/blocking completions, shutdown drain, and a synchronous reference backend;
- a Linux `io_uring` backend that batches submissions/completions and safely
  fences or quarantines kernel-owned buffers on fatal paths;
- a shared `AsyncDiskCache` facade with up to 128 read workers and up to 64
  ordinary mutation workers (eight per append lane, bounded by write depth), two
  control-reserve slots, cancellation/deadlines, and FIFO exclusive barriers
  for `flush`, `clear`, and shutdown;
- I/O and async queue occupancy, rejection, cancellation, error, and latency
  counters in runtime statistics, plus checkpoint load/write/fallback/error and
  bounded recovery progress/scan/time counters;
- Linux `O_DIRECT` runtime data I/O with strict 4 KiB buffer/offset/length
  submission checks, exact file preallocation during formatting, and
  direct-versus-buffered operation/byte counters;
- fixed-memory `Always` / `SecondHit` admission, large-object protection,
  per-namespace capacity and write quotas, UTC-day host-write budgets, Region
  valid-ratio accounting, and asynchronous one-shot second-chance reinsertion;
- submitted host-write categories and write-amplification counters, plus
  operator-supplied NVMe SMART/health samples (`data_units_written`, spare,
  wear, media errors, and critical warnings); health is advisory by default;
- dependency-free bounded request telemetry: 24 finite latency buckets plus
  overflow, stable result/error classes, and the latest 32 lifecycle events;
- `MetricsSnapshot` OpenMetrics exposition suitable for Prometheus directly or
  OpenTelemetry Collector's Prometheus receiver, without an exporter worker in
  the cache process;
- preflight `CacheConfig::diagnostics`, `open_with_diagnostics`, health
  snapshots, and an explicit rate/concurrency-bounded origin-fill permit for
  miss-storm protection;
- the `cachectl inspect`, `verify`, `format`, `reset`, and `diagnose`
  workflows, with destructive operations requiring explicit acknowledgement.

Version 1.1 keeps `IoEngineKind::Sync`, `IoMode::Buffered`,
`AdmissionMode::Always`, and `ReclaimMode::Fifo` as conservative defaults.
Engine selection and file-I/O policy are independent:
`IoEngineKind::Auto` may select `io_uring`, while `IoUring` requires it;
`IoMode::Auto` tries `O_DIRECT` and disables it only when the filesystem reports
that capability unavailable, while `IoMode::Direct` requires the direct
descriptor and never retries an aligned direct-I/O error through the buffered
descriptor. Metadata, recovery, legacy unaligned Format V1 records, and an
unaligned remainder after a positive short completion still use the buffered
descriptor in every mode. Thus `Direct` means required direct capability, not
that every byte of a compatible cache file is submitted with `O_DIRECT`.
Multi-device striping, raw devices, FDP placement, and SPDK remain v1.x work.
Milestone status and the remaining deployment sign-offs are tracked in
[ROADMAP.md](ROADMAP.md).

## Example

```rust
use cache_rs::{CacheConfig, PutOptions};

fn main() -> cache_rs::Result<()> {
    let cache = CacheConfig::new("/var/tmp/example.cache", 512 * 1024 * 1024)
        .open()?;

    cache.put("answer", "42", PutOptions::default())?;
    assert_eq!(cache.get(b"answer")?, Some(b"42".to_vec()));

    cache.flush()?;
    Ok(())
}
```

For mixed objects, construct the two dedicated disk engines and place the
bounded memory tier above them:

```rust
use cache_rs::{
    BucketCacheConfig, CacheConfig, HybridCacheConfig, IoEngineKind, IoMode, PutOptions,
};

fn main() -> cache_rs::Result<()> {
    let small = BucketCacheConfig::new("/mnt/nvme/cache.small", 8 * 1024_u64.pow(3))
        .with_buffer_slots(64)
        .with_io_queue_depth(128)
        .with_io_engine(IoEngineKind::Auto)
        .with_io_mode(IoMode::Auto);
    let large = CacheConfig::new("/mnt/nvme/cache.large", 512 * 1024_u64.pow(3))
        .with_expected_entries(20_000_000)
        .with_memory_budget(2 * 1024_usize.pow(3));
    let cache = HybridCacheConfig::new(8 * 1024_usize.pow(3), small, large)
        .with_small_object_max(1024)
        .open()?;

    cache.put("small", "value", PutOptions::default())?;
    cache.put("large", vec![7_u8; 64 * 1024], PutOptions::default())?;
    assert_eq!(cache.get(b"small")?, Some(b"value".to_vec()));
    cache.flush()?;
    Ok(())
}
```

For a large deployment, size the index from the expected live population and
validate the complete memory/checkpoint plan before opening the dedicated
file. For example, 100 million live entries target 125 million slots, or about
3.73 GiB of index memory before Region metadata, queues, and buffers:

```rust
use cache_rs::{CacheConfig, IoEngineKind, IoMode};

fn main() -> cache_rs::Result<()> {
    let config = CacheConfig::new("/mnt/nvme/cache-rs.data", 8 * 1024_u64.pow(4))
        .with_expected_entries(100_000_000)
        .with_memory_budget(6 * 1024_usize.pow(3))
        .with_max_key_size(1024)
        .with_max_value_size(256 * 1024)
        .with_append_lanes(4)
        .with_submission_queue_depths(128, 128)
        .with_io_queue_depth(128)
        .with_io_engine(IoEngineKind::Auto)
        .with_io_mode(IoMode::Auto);

    let plan = config.diagnostics()?;
    assert_eq!(plan.index_slots, 125_000_000);
    let _cache = config.open()?;
    Ok(())
}
```

The data format supports fewer than 2^21 Regions (just under 64 TiB with the
default 32 MiB Region). For standalone `DiskCache`, the 256 MiB default
periodic-checkpoint floor scales up automatically for a large index. A managed
Region does not publish independently: `HybridCache::flush()` drains dirty L1,
durably marks the global manifest dirty, freezes lower mutations, publishes
both lower boundaries, and only then publishes matching namespace usage as
globally clean; before normal service resumes, `flush()` re-arms the single
dirty-session fence. `close()` provides the final clean boundary. Managed
Hybrid disables periodic Region checkpoints, so operators choose an explicit
flush cadence only when warm clean restart is worth the full Region checkpoint
cost. An unclean session is allowed to restart empty. Full Region checkpoints
remain O(index slots); plan their pause and write budget on the target host.
The data path and reclaim path no longer perform a full-index scan per Region
rotation.

The scalable fast path is steady-state record I/O: independent reads and
hash-selected append lanes overlap. Two turnover boundaries remain explicit in
v1.1. FIFO rotation still performs its header barriers and victim scrub while
holding the global Region-manager state lock, so all append lanes pause at that
boundary; `SecondChance` moves the scrub off the foreground path but currently
maintains only one ready Region and can return `ReclaimBacklog` under sustained
full-cache writes. Checkpoint publication is also an exclusive O(index slots)
operation. Payload I/O is aggregated into fixed 256 KiB writes, but a
100M-entry checkpoint is still a planned maintenance pause, not a transparent
background snapshot.

The path must be dedicated to the cache. A completed Hybrid `put` is immediately
visible in the running process but, in default write-back mode, may exist only in
L1. Hybrid `flush`/`close` publish a clean restart boundary; `clear` removes the
current contents but leaves the session dirty. Open and post-flush re-arming
persist the dirty fence, so later steady-state mutations allocate versions only
in memory and do not write journal records or issue metadata syncs. A dirty
Hybrid reopen may safely clear both lower tiers. Standalone `DiskCache` retains
its checkpoint-plus-incremental-tail recovery behavior described below.

Clean checkpoint loading derives per-Region valid bytes and standalone
namespace live bytes during the same streamed entry-decode pass. It accounts
only entries actually applied after compact-index collision/replacement rules;
tombstones remain Region-valid physical data but are not namespace live data.
Clean and initial `MissOnly` loads therefore do not perform a second full-index
snapshot scan. The bounded Region and namespace accounting workspace is
reported as `ConfigDiagnostics::checkpoint_accounting_bytes` (and
`region_checkpoint_accounting_bytes` by Hybrid diagnostics).

The checkpoint extension starts immediately after the original Format V1 data
extent: one 4 KiB directory and two independently checksummed slots. Each slot
has a 4 KiB commit header and a streamed payload. Payload pages are written and
synced before the header is written and synced; only then is the matching clean
Superblock published. Older Format V1 readers use the original Superblock and
Region extent and ignore this tail, so the record/Region format is unchanged.
Opening a legacy fixture leaves every byte of that data extent unchanged and
appends only the compatible checkpoint extension. The current writer emits
checkpoint payload v4. It retains the v3 Active Region lane identity and M7
namespace/index flags, and also records the source index capacity, shard count,
and physical slot of every visible entry. Reopening with the same index layout
therefore reconstructs the bounded-probe table exactly; changing the runtime
index layout uses safe reinsertion, where pressure may cause extra misses but
cannot expose a wrong value. v1/v2/v3 payloads remain readable.
Checkpoint encoding, loading, and recovery use fixed pages plus workspaces
charged to the configured logical memory budget rather than allocating in
proportion to payload size.

`close` is idempotent and immediately releases the writer lock, even while the
closed `DiskCache` object or one of its clones remains alive. Fallible APIs
return `Closed` after that point; `stats` remains available as a final snapshot.
The sole exception is an `io_uring` fatal path with an active write/flush whose
target CQE cannot be observed: that instance returns an I/O error and retains
the lock so a new writer cannot race a still-live kernel request into the same
inode.

`DiskCache::async_handle()` returns a shared `AsyncDiskCache`. Its operations
return standard Rust `Future`s that can also be completed with `.wait()` when no
executor is available. Dropping or cancelling a queued request, or a read before
its cleanup commit point, releases its bounded slot. A committed read cleanup
and a mutation whose ordered worker has started are uncancellable, return
`TooLate` to cancellation, and deliver their real result. Sync and async `close` calls
elect one physical close owner and all other callers observe the shared result.
Ordinary async mutations run on up to eight workers per append lane (64 total,
also bounded by write depth), retain same-key ordering through the cache
ordering stripes, and let each append worker collect a queued batch while
different lanes overlap writes. Overlapping ordinary mutations may linearize
in either worker-acquisition order; callers that require call order await the
earlier future before submitting the dependent mutation. `flush`, `clear`, and close are FIFO exclusive barriers: each
waits for all earlier mutations, excludes later mutations while it runs, and
then releases the ordinary worker pool.

## Runtime failure states

`status()` exposes the instance state without performing I/O:

| State | `get` | Mutation / `flush` / `clear` | First `close` |
| --- | --- | --- | --- |
| `Healthy` | normal | normal | publishes clean checkpoint, unlocks |
| `MissOnly` | `Ok(None)`, no more device reads | `Err(Poisoned)` | skips checkpoint, unlocks, returns `Poisoned` |
| `Poisoned` | `Err(Poisoned)` | `Err(Poisoned)` | skips checkpoint, unlocks, returns `Poisoned` |
| `Closed` | `Err(Closed)` | `Err(Closed)` | idempotent `Ok(())` |

During `RecoveryMode::MissOnly`, `MissOnly` is temporary: validated checkpoint
state remains hidden, reads return misses, mutations are rejected, progress is
reported in statistics, and one atomic publication moves the instance to
`Healthy`. A terminal runtime backend error also moves the instance to
`MissOnly` and clears its in-memory index, but that failure state remains until
reopen. The operation that observes the write or sync failure returns the
original I/O error; later mutations fail consistently.

## Configuration contract

| Setting | Reopen behavior |
| --- | --- |
| `capacity` | The effective whole-region count must match the existing file. |
| `region_size` | Persistent layout; must match. |
| `hash_seed` | Persistent key identity; must match. |
| `index_slots` | Runtime-only; may change, with a hard limit of 268,435,456 slots (32 bytes each). `with_expected_entries(n)` targets at most 80% occupancy. |
| `max_key_size` | Runtime `put` admission only; may change. Old keys remain readable/removable. |
| `max_value_size` | Runtime `put` admission only; may change. Old values remain readable/removable. |
| `memory_budget_bytes` | Runtime-only logical engine-memory cap, default 1 GiB; open fails before touching the path if the complete resource plan cannot fit. |
| `append_lanes` | Persistent clean-checkpoint layout; 1–8, default 1, and must match on reopen. Each lane owns an Active Region. |
| read/write submission depths | Runtime-only; each must be 1–65,536 and defaults to 2/2. Data-buffer slots scale from these depths, capped at 128 per class; control admission has a separate fixed one-slot reserve. |
| I/O engine | Runtime-only `Sync`, `Auto`, or required `IoUring`; does not change Format V1. |
| I/O mode | Runtime-only `Buffered`, `Auto`, or required-capability `Direct`; may change on reopen without changing Format V1. |
| I/O queue depth | Runtime-only, hard limited to 1–4,096; default 128. |
| backpressure policy | Runtime-only; defaults to immediate `Reject`, with explicit `Block` and `Timeout` alternatives. |
| write budget | Runtime-only bytes/second token bucket with a one-second burst; disabled by default. |
| checkpoint interval | Runtime-only standalone `DiskCache` admitted-record threshold. The implicit default is `max(256 MiB, 16 × index_slots × 40 B)`; an explicit value is exact, and `0` disables periodic checkpoints. Hybrid forces its managed Region interval to `0`; explicit Hybrid `flush`/`close` own clean publication. |
| recovery mode | Runtime-only `Blocking` (default) or `MissOnly`; applies to dirty startup with a usable checkpoint. |
| admission mode | Runtime-only `Always` (default) or fixed-memory `SecondHit`; may change on reopen. Existing-key updates bypass the frequency threshold. |
| reclaim mode | Runtime-only `Fifo` (default) or bounded asynchronous `SecondChance`; may change on reopen. |
| namespace policy | Runtime-only capacity and bytes/second write limits for configured namespace IDs; namespace zero remains the legacy API. |
| daily host-write budget | Runtime-only UTC-day host-byte limit; use an externally durable baseline to enforce it across restarts. |
| device-health policy | Runtime-only advisory SMART observation by default; optionally reject only new puts after a critical sample. |
| origin-fill protection | Runtime-only rate and concurrency limiter acquired explicitly after a miss; disabled by default. |

An empty file is formatted as V1. A non-empty file with cache magic and an
unsupported version, or an unrecognized non-empty file, is rejected without
being rewritten. Formatting writes and syncs a V1 ownership marker before
extending the file, so interrupted initialization remains recognizable.
Every non-empty leading prefix of `CACHERS\0`, plus a zero-filled file no larger
than the 8 KiB Superblock area, is therefore reserved for interrupted Format V1:
a torn marker or persisted length update is byte-for-byte indistinguishable from
an unrelated file with those contents. The configured path must be dedicated to
this cache. A larger file without a recognized V1 header is rejected. Recognized
but corrupt V1 metadata is disposable and is safely rebuilt empty.

Formatting establishes the exact whole-region Format V1 data extent before
initializing Region Headers. On 64-bit Linux it also requests physical
allocation with `posix_fallocate`; filesystems that do not implement that
primitive retain the exact `set_len` extent and continue through the compatible
path. The checkpoint tail is created after that extent on first checkpoint;
safe reformat truncates any stale extension before publishing the new format.

For TTL, `None` means no expiration. `Some(ts)` is an absolute Unix timestamp
in milliseconds, so `Some(0)` and any timestamp at or before validation time
are rejected as already expired.

## Bounded-resource contract

`CacheConfig::with_memory_budget` accounts for engine-owned logical heap:
the fixed-capacity compact index, Region and checkpoint snapshot metadata, the
recovery ordering/workspace and read-plane mirror, sharded index/key-ordering
metadata, the bounded
append/async/I/O queues and completion allowance, copied inputs retained by the
async facade, a small fixed overhead allowance, and the maximum growth of the
dynamic read/write data pools (at most 128 slots each), two control buffers, and
one metadata buffer. Read slots are
`min(read_submission_depth, io_queue_depth, 128)`; write slots are
`min(write_submission_depth, 128)`. Pool slots are created eagerly, while their
backing allocations grow lazily and never beyond the validated plan. On Linux,
growth reserves the entire replacement mapping before allocation and keeps the
old mapping charged until its contents have been copied and unmapped, so the
reported peak also covers transient overlap.
The reported `memory_used_bytes` and `memory_peak_bytes` use this conservative
accounting and never exceed `memory_budget_bytes`.

Hybrid diagnostics additionally expose `journal_recovery_memory_bytes`, and
Region diagnostics expose `checkpoint_accounting_bytes`; both are included in
the aggregate open-time plan rather than treated as untracked recovery memory.

Caller-owned input keys/values, returned value `Vec`s, thread stacks, OS page
cache, and allocator metadata are outside that logical budget. Direct-eligible
runtime requests are submitted only when buffer address, offset, and length are
all 4 KiB aligned. Queued puts are coalesced without changing Format V1; the
final record carries any direct-I/O tail padding. A single record larger than
128 KiB remains a valid one-record batch.

On saturation, `Reject` returns immediately and `Timeout` shares one deadline
across gate and buffer acquisition. `Block` intentionally waits in the calling
thread until capacity is returned; no request object is enqueued or retained by
the engine while it waits. In v1.1, close first fences the async facade and
drains accepted facade work, then closes synchronous admission and drains the
shared operation barrier; later calls observe `Closed`.
Ordinary writes cannot consume the resources reserved for reads or remove/control
operations, so admission pressure in one class does not exhaust another class.
The writer state mutex is confined to append-lane reservation/publication and
failure transitions; ordinary cache-hit reads do not acquire it. Reads hold
only the target Region's read guard across I/O, and rotation waits only on its
victim Region.

`put` reports overload as `PutOutcome::Rejected(SubmissionFull /
SubmissionTimeout / BufferUnavailable / WriteBudgetExceeded)`. Other operations
return `CacheError::Overloaded(OverloadReason)`. Resource and budget rejection
happens before a dirty marker, sequence allocation, append, or index publication
and does not change `CacheStatus`.

`CacheStats` additionally exposes `write_batches`, `records_coalesced`, direct
and buffered operation/byte totals, whether direct I/O and `io_uring` are
active, and whether an unfenced mutation forced lock retention. These counters
make compatibility-path traffic visible instead of treating it as a silent
direct-mode fallback. M6 adds `checkpoint_writes`, `checkpoint_loads`,
`checkpoint_fallbacks`, `checkpoint_errors`, `recovery_regions_scanned`,
`recovery_records_scanned`, `recovery_bytes_scanned`, `recovery_elapsed_us`,
`recovery_regions_completed`, `recovery_regions_total`, and
`recovery_in_progress`. `recovery_regions_scanned` counts Regions whose record
data was actually scanned, while `recovery_bytes_scanned` includes every Region
Header read plus scanned record bytes. `completed/total` is monotonic progress
over all Regions, including those that required no record scan. v1.1 adds
`reclaim_records_scanned` and `reclaim_index_fallbacks`: normal reuse scans only
the victim's record headers; the full-index path is reserved for malformed
victim metadata and is explicitly observable.

Nine focused M6 behavior tests cover the pre-traffic baseline, dirty put/remove
replay, `clear` as an epoch barrier, slot alternation and newest-slot corruption,
temporary miss-only service, periodic checkpoint shutdown, the minimum Active
tail boundary, damaged/truncated tombstones, and checkpoint/clear crash failpoints. The last
test drives the real subprocess `SIGKILL/restart` harness before and after each
checkpoint payload/header barrier and the clear barrier; restart is allowed to
produce only a correct value or a miss, never a resurrected value.

## M7 cache value and SSD governance

`AdmissionMode::SecondHit` uses a fixed 64 KiB approximate-frequency table:
ordinary new objects require two observations and objects larger than 1 MiB
require three. Updates to an existing key remain admissible. `ReclaimMode::SecondChance`
marks verified hits and gives an eligible record one bounded asynchronous
reinsertion; FIFO remains the comparison baseline. Reinsertion capacity,
pending work, stale work, Region valid bytes/ratio, and reclaim backlog are all
bounded and observable.

The namespace APIs (`get_in`, `put_in`, and `remove_in`) isolate key identity
and enforce configured live-byte and write-rate quotas. The legacy API is
namespace zero. Hybrid clean checkpoints persist at most 240 sorted namespace
live-byte counters in the reserved area of each Format V1 manifest slot, so a
clean reopen is metadata-only even when the Bucket and Region files are very
large. Legacy all-zero extensions and a changed namespace set scan the lower
engines once and republish a bounded snapshot. Recoverable non-empty dirty
journals do the same after invalidating touched routes; unsafe or empty dirty
journals safe-clear first. A damaged extension invalidates that slot instead
of restoring a possibly low quota.
Host-write accounting separates foreground records,
reinsertion, reclaim, forced tombstones, metadata, and checkpoints, and reports
write amplification plus an optional UTC-day byte budget. Bucket capacity is
charged by aligned encoded entry while its RMW submission is a complete fixed
page. Region capacity uses the durable receipt's actual packed record length,
including direct-I/O padding; Hybrid reserves the aligned worst case before
I/O and reconciles write-back pending usage to that receipt without an
undercount window. SecondChance replacements reserve namespace and daily
budgets before writing and record their submissions as reinsertion traffic.
`daily_host_write_bytes` is the submitted-I/O counter, while
`daily_budget_used_bytes`/`reserved_bytes` describe admission state; accepted
metadata/fence writes may still make the former exceed its advisory limit.
An expired Bucket entry remains an aligned physical namespace charge and keeps
its conservative membership hint until a managed access successfully compacts
and rewrites the complete page. Daily-budget rejection, pre-commit
cancellation, or write failure does not refund it; only the durable exact
removal receipt does. A receipt underflow poisons the tier and forbids a clean
global checkpoint. Consequently, a TTL-heavy namespace at its hard quota may
need reads/removes of the affected pages, `clear`, or future scavenging before
new admission succeeds.
Because the cache
file cannot reliably discover its backing controller, callers feed SMART data
through `observe_nvme_health`; the default policy only reports health, while an
explicit policy may reject new puts on a critical sample. Translating device
DWPD into the daily host-write budget, and proving hit-rate and wear targets,
remain workload/device sign-offs.

## M8 observability and operations

`metrics_snapshot()` returns bounded, low-cardinality operation metrics and the
latest 32 lifecycle transitions. `write_openmetrics()` emits a complete
OpenMetrics snapshot with no key, namespace, or path labels. A Prometheus
endpoint can expose that string directly; OpenTelemetry deployments should use
the Collector Prometheus receiver. `state_log_json()` writes those lifecycle
events as newline-delimited structured JSON for the owning service's logger.
Request latency covers the synchronous cache
operation (or async work after dispatch); async queue wait and I/O timing remain
separate counters.

`CacheConfig::diagnostics()` validates layout and the full logical-memory plan
without creating or modifying the path. `open_with_diagnostics()` also returns
the selected recovery and I/O outcome. `health_snapshot()` provides a compact
readiness/degradation view. After a miss, applications can call
`try_begin_origin_fill()` and hold its RAII permit across the authoritative
origin request; configured rate and in-flight caps prevent a cold cache from
creating an unbounded fill storm.

The management tool is intentionally explicit; inspect/verify are offline and
read-only, while format/reset are acknowledged mutations:

```text
cachectl inspect  --path /var/lib/service/cache-rs.data --output human
cachectl verify   --path /var/lib/service/cache-rs.data --output json
cachectl diagnose --path /var/lib/service/cache-rs.data --capacity 512GiB
cachectl format   --path /var/lib/service/cache-rs.data --capacity 512GiB --yes
cachectl reset    --path /var/lib/service/cache-rs.data --capacity 512GiB --yes
```

`inspect` and `verify` are read-only and do not open the cache runtime.
`format` accepts only a missing or empty dedicated file, while `reset` requires
a recognized Format V1 cache and explicit `--yes`; neither command is an
automatic recovery reaction. See [docs/OPERATIONS.md](docs/OPERATIONS.md) and
[docs/UPGRADE.md](docs/UPGRADE.md) for readiness, canary, reset, and rollback
procedures.

The complete Hybrid surface keeps the three-file identity explicit:

```text
cachectl hybrid-diagnose --bucket-path cache.small --bucket-capacity 64GiB \
  --region-path cache.large --region-capacity 512GiB \
  --manifest-path cache.manifest --memory-capacity 16GiB \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB
cachectl hybrid-verify --bucket-path cache.small --region-path cache.large \
  --manifest-path cache.manifest --output json
```

`hybrid-inspect`/`hybrid-verify` hold all three offline locks and never run
recovery. `hybrid-format` requires `--yes` and three missing or empty paths;
there is no destructive Hybrid reset shortcut. At runtime,
`HybridCache::openmetrics_snapshot()` reports Memory/Bucket/Region hits,
request bounds, admission, combined host writes, I/O, component health, dirty
L1 state, and bounded write-back queue/memory state with fixed label cardinality.
`proactive_persisted` is value-preserving demotion;
`proactive_invalidated` plus `dropped_evictions` is intentional cache loss after
durable stale-L2 deletion; `demotion_failures` identifies demotion work that
could not safely complete.

## NVMe staging acceptance

Use the dedicated-path procedure in
[docs/NVME_BENCHMARK.md](docs/NVME_BENCHMARK.md). The checked-in harness accepts
predeclared throughput, p99, and hit-rate gates through `--min-ops-per-sec`,
`--max-p99-us`, and `--min-hit-percent`, and exits non-zero when a gate or an
integrity/resource check fails. Every invocation destructively resets an
existing recognized Format V1 cache at `--path`; unrelated non-empty files are
rejected. `--api sync|async` measures both public paths; scale runs cover queue
depth 1/8/32/64/128, append lanes 1/2/4/8, and at least two cache-capacity
turnovers in the measurement window. M7 A/B runs use
`--require-policy-activity` to require steady-state
Region reuse and the selected admission/reinsertion activity. M5 implementation
work is complete; staging
sign-off remains pending until that matrix and matching CPU/device profiles are
captured on the target NVMe host.

`cache-bench hybrid` adds deterministic weighted object sizes such as
`--sizes 256:50,4KiB:30,64KiB:20` and exercises both SSD routes behind the
bounded memory engine. Keys are generated on demand (no `Vec<Vec<u8>>`), while
an 8-byte-per-key bounded state table tracks the current value version so a
superseded small/large update is a hard correctness failure. A production-scale
gate starts from the following target-host command (replace every agreed
threshold with the deployment value):

```text
target/release/cache-bench hybrid \
  --bucket-path /mnt/nvme/cache-rs.hybrid-small --bucket-capacity 64GiB \
  --region-path /mnt/nvme/cache-rs.hybrid-large --region-capacity 512GiB \
  --manifest-path /mnt/nvme/cache-rs.hybrid-manifest --memory-capacity 16GiB \
  --bucket-memory-budget 2GiB --region-memory-budget 8GiB \
  --hybrid-memory-budget 32GiB --generator-memory-budget 2GiB \
  --sizes 256:50,4KiB:30,64KiB:20 --small-object-max 1KiB \
  --keys 100000000 --prefill-percent 80 --prefill-concurrency 64 \
  --verify-samples 100000 --read-percent 80 \
  --remove-percent 10 --ttl-percent 5 --cross-tier-percent 20 --ttl-ms 250 \
  --api async --concurrency 64 --queue-depth 128 --append-lanes 4 \
  --write-mode write-back --write-back-queue-depth 128 \
  --write-back-workers 8 --write-back-memory 256MiB \
  --journal-capacity 64MiB --engine uring --mode direct \
  --warmup-secs 60 --steady-state-fill-turnovers 2 \
  --steady-state-fill-max-secs 3600 --duration-secs 600 \
  --min-ops-per-sec <agreed-throughput> --max-p99-us <agreed-p99> \
  --min-hit-percent <agreed-hit-percent> \
  --min-capacity-turnovers 2 --min-disk-qd-peak 8 \
  --min-write-back-qd-peak 8 \
  --max-close-ms <agreed-close-ms> --yes --output json
```

The qualification matrix covers both
`--write-mode write-through` and `--write-mode write-back`; it reports tier
hits, global write amplification, version-stale failures, bounded prefill,
Bucket/Region submitted I/O and QD peaks, write-back persist/invalidate/drop and
hard-cap failures, capacity turnover, request rejects, latency, and final
clean-checkpoint drain time. Steady-state
mutations are expected to leave journal rollover counters unchanged. The run
requires three empty paths plus `--yes`. JSON always records
`hardware_qualification=false`, `target_nvme_matrix_passed=false`, and the
external soak/power-loss/thermal fields as false: meeting the software gate is
evidence for that run, not a substitute for the target-NVMe release matrix.

`--steady-state-fill-turnovers 2` applies the same mixed temporal workload
before latency/throughput accounting starts. It advances until pre-measure host
writes reach two combined lower-tier capacities and the Region reuse count
reaches the configured Region count (a complete reuse cycle), then drains
the phase boundary and snapshots fresh measurement counters.
`--steady-state-fill-max-secs` bounds this preparation; failure to reach either
condition exits non-zero instead of publishing a fresh-cache
result. `--min-capacity-turnovers` remains a separate requirement on the
measurement window itself.

For v1.1 production qualification, also retain the M6 TB-scale recovery SLA
report, the M7 FIFO/second-chance and always/second-hit A/B results with the
device DWPD calculation, and M8 24--72 hour soak, canary, miss-storm, and real
power-loss evidence. Source completion makes this tree a production candidate;
those environment-specific artifacts decide whether a particular deployment
is production-ready.

## Development

```text
cargo test
cargo clippy --all-targets -- -D warnings
cargo +1.85.0 test --all-targets
cargo +1.85.0 clippy --all-targets -- -D warnings
```
