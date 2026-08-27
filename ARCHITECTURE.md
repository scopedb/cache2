# cache-rs architecture

## Contract

cache-rs optimizes the cache data path ahead of durability. The authoritative
copy of every value must live elsewhere.

```text
HybridCache
├── shared bounded RAM L1
├── fixed-capacity partitioned mmap index
├── append shards + one global Region free/sealed FIFO
├── bounded read I/O-engine pool
├── bounded write I/O-engine pool
└── data, state, and clean-image files
```

RAM is a process-local L1 and the Region file is L2. Both tiers use one seeded
XXH3-64 hash of the complete raw key. L1 disambiguates a hash bucket with the
complete key, while L2 validates the complete key after reading a hash-selected
record. A collision may therefore become a conservative miss but can never
return another key's value. Logical namespaces, when needed, are encoded by the
caller into that raw key.

Every append shard has one Active Region, two fixed write buffers, and one
ordered worker. All remaining Regions share one global free/sealed FIFO, so any
shard can consume free capacity and reclaim the oldest sealed Region. Runtime
L1 shards and append shards are independent. Changing the append-shard count
does not change physical Region capacity, but it discards a clean image whose
active-Region topology has a different shard count. There is one SSD format and
no Bucket engine, journal, or separate write-back executor.

## Internal boundaries

- `cache` owns the public configuration and API translation;
- `region_store` is the backend-independent open/shutdown state machine;
- `region` owns the concrete Region/index authority and recovery-image I/O;
- `region_runtime` owns steady-state workers and data-plane admission;
- focused codec, index, staging, read, I/O, memory, eviction, and resource
  modules each enforce one bounded mechanism.

The lifecycle state machine depends only on its backend trait. Concrete Region
types implement that trait and keep file-format or worker details out of
lifecycle tests.

## I/O path

A `put` follows a direct bounded path:

1. hash the complete raw key once;
2. route that hash to one append shard;
3. reserve bytes and sequence authority from the Region manager;
4. encode directly into that shard's aligned staging buffer;
5. publish the same sequence to RAM when it fits, before returning;
6. seal a batch on size, age, or Region rotation;
7. submit the owned buffer to one configured write I/O engine;
8. publish the index entry directly to its index partition only after successful
   completion, without reacquiring the global Region manager.

Each shard has exactly two staging buffers, allowing one submitted batch and one
fill batch without allocating per request. RAM entries are byte-bounded per
shard and immediately eligible for eviction. Victim order is selected at
runtime by shard-local CLOCK with a fixed scan budget. Capacity charging and
full-key validation remain separate from its visited-bit metadata. Values that
do not fit L1 continue through L2 without an additional queue. FIFO Region
reuse remains the only SSD replacement policy.
L1 capacity remains charged through the last returned value handle, so eviction
cannot hide memory retained by a slow caller.

A `delete` allocates one sequence and performs a non-waiting bounded L2 index
probe plus a best-effort exact-key L1 removal. It does not enter append staging,
reserve Region bytes, or submit I/O. Manager or index contention rejects it as
write overload, and an already missing delete consumes no index slot. Because
the open-addressing deleted marker carries no sequence, a delayed older put may
reappear. This is permitted by the stale-cache contract; reads do not retry or
validate freshness a second time.

Each L1 shard receives fixed entry slots, policy slots, a free list, and an
open-addressed directory during open. The directory routes the already seeded
XXH3 hash directly and has a fixed 64-probe ceiling; pressure bypasses optional
L1 insertion. No resident-set change reallocates or rehashes metadata under the
shard lock. The entry plan is capped by the static expected-key plan, scales
with the L1/L2 capacity ratio, and has a 4 KiB-density floor so deliberate L2
headroom does not make L1 unusably sparse. Fixed metadata is reserved
separately in the aggregate memory plan.

CLOCK keeps one resident/visited bit per fixed policy slot. Lookup marks the
slot visited; insertion advances one shard-local hand for at most 64 slots,
clearing visited bits before selecting a victim. Exhausting that fixed budget
bypasses L1 insertion.

A `get` first checks the shared RAM tier. On an L1 miss it probes the fixed-size
L2 index once, reserves one non-waiting read-engine slot, allocates the exact
aligned read range, and reads one record. POSIX uses one dedicated bounded read
engine, so one reservation observes the complete read-worker capacity. If an
io_uring hash-selected read lane is full, admission may probe one hash-derived
alternate lane; it never scans or waits across the pool. Local validation
checks the planned location, sequence, hash, complete key, lengths, and
checksums.
There is no retry or second freshness check; a concurrently superseded but
otherwise valid value may be returned or promoted.

Read and write I/O use independent bounded pools. The default POSIX path uses
one engine per direction with one positioned-I/O worker and execution slot per
configured worker. Explicit io_uring builds use one ring with a fixed 64-slot
depth per configured worker in each direction. Reads never wait for a slot and
may use the complete read-pool depth; write submissions cannot consume it.
Write-slot waits occur only in background append-shard workers; explicit
completion barriers may wait for those workers.

## Recovery path

The state file is a small alternating generation fence. Normal open publishes
`RUNNING` before exposing the cache. Therefore a crash, kill, dropped owner, or
`close_fast` leaves no recovery authority and the next open creates an anonymous
empty index immediately.

`close_warm` is the only recoverable shutdown:

1. stop admission and drain shards and I/O engines;
2. freeze Region and index authority;
3. sync the completed data writes;
4. write a new checksummed recovery image sequentially;
5. sync and atomically install that image;
6. publish matching `CLEAN` state last.

On the next open, a matching clean image is mapped private and serves as the
initial index. Missing, corrupt, truncated, stale, or configuration-mismatched
state becomes an empty cache; intact unsupported format versions also cold-start
empty. The data extent is never scanned. L1 is always created empty and warms through L2
reads; it is never serialized into the clean image.

## Configuration identity

Static configuration is included in the data superblock fingerprint and clean
image binding. It consists of capacity, Region size/count, index slots,
hash-algorithm identity, and hash seed. A mismatch reformats disposable cache
state rather than trying to migrate it.

Runtime configuration is not part of the data-superblock identity and is
selected on each open. It includes I/O engine and mode, independent read and
write worker counts, append-shard count, L1 capacity and shard count, one
write-batch capacity, aggregate memory limit, and optional statistics. The
clean image records its append-shard topology; a count mismatch cold-starts
empty instead of migrating it. Other runtime tuning can retain the image.
Foreground L2 reads
reserve one immediately available read-engine execution slot after an index hit,
then allocate one actual-size aligned range; they have no separate public
admission policy. The dedicated write pool cannot affect read-slot availability.
Runtime configuration is validated before filesystem mutation.
Keys are bounded at 4 KiB and values at 256 KiB. Device operations use a fixed
five-second completion guardrail so a stalled cache device cannot retain a
frontend or shutdown barrier indefinitely.

The managed logical disk peak is deterministic: the fixed data file and 8 KiB
state file plus two fixed-size recovery images. The second image is the bounded
cost of atomic warm publication. Cold start removes stale images and abandoned
temporary images before exposing an anonymous runtime. Every cache thread uses
an explicit 512 KiB stack and the worst-case shard, I/O, and shutdown-reaper
stack topology participates in the aggregate memory plan. The operating system
still controls physical stack commitment. Buffered-I/O page cache, filesystem
metadata, and allocator internals remain outside the cache-owned byte budgets.
The public resource snapshot exposes current/peak managed reservations, their
hard limit, and the configured logical-disk peak. The on-demand detailed
snapshot also exposes L1 entry capacity, resident/retained/fixed-metadata bytes,
physical index occupancy, and counts of deleted, stale-generation, and live
slot replacement, plus aggregate Region capacity, occupancy, FIFO state, and
rotations. The replacement counters follow the optional statistics switch;
occupancy remains always available. Optional activity statistics separately
classify read-memory and busy-engine misses.

`HybridCache::snapshot()` also reads relaxed process-local counters for puts,
deletes, L1/L2 hits and misses, read-memory and busy-engine misses, L1 promotions,
evictions/bypasses, logical bytes, write rejections, failures,
and Region rotations. Health is `Running`,
transiently `Draining`, one-way `MissOnly` for
fail-open reads, or `Failed` after a shard worker terminates. None of this state
is persisted or participates in format identity. Activity counters are
cache-line-aligned per shard and opt-in at runtime; health and resource bounds
remain observable when counters are disabled.

## Safety boundary

HybridCache loss and stale valid values are acceptable; unchecked, wrong-key,
out-of-bounds, or corrupt values are not.
All sizes and offsets are checked, persistent structures are field-encoded and
checksummed, reads verify full keys, Region reuse uses incarnation authority,
and bounded buffers retain ownership through I/O completion. Device or
structural failures may reject work or turn reads into misses.
