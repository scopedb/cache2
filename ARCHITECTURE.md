# cache-rs architecture

## Contract

cache-rs optimizes the cache data path ahead of durability. The authoritative
copy of every value must live elsewhere.

```text
HybridCache
├── shared bounded RAM L1 + visibility fences
├── fixed-capacity partitioned mmap index
├── namespace → RegionSet router
│ ├── RegionSet 0: append shards + private Region range + free/sealed FIFO
│ └── RegionSet N: append shards + private Region range + free/sealed FIFO
├── configurable I/O-engine pool
└── data, state, and clean-image files
```

RAM is a process-local L1 and the Region file is L2. Both tiers use the same key
hash. The hash is seeded XXH3-64; L1 disambiguates a hash
bucket with the namespace and complete key, while L2 validates the complete
encoded key after reading a hash-selected record. A collision may therefore
become a conservative miss but can never return another key's value. There is
one SSD format and no Bucket engine, journal, or separate write-back executor.

A RegionSet is an L2 retention boundary. Static capacity weights assign each
set a contiguous Region range; append shards are assigned contiguous ranges
evenly across sets. Every append shard has one Active Region, two fixed write
buffers, and one ordered worker. Free and sealed FIFOs are private to their set,
and Region rotation never borrows across sets. L1, the global index, I/O pool,
I/O concurrency, memory limit, and statistics stay shared. Runtime-only L1 shards are
independent of static append shards, so RAM concurrency can be retuned without
changing Region assignment or recovery identity.

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

1. hash the namespace and full key;
2. route the namespace to a RegionSet, then select one of its append shards;
3. reserve bytes and sequence authority from the Region manager;
4. encode directly into that shard's aligned staging buffer;
5. publish the same sequence as a pending RAM entry when it fits, before
   returning;
6. seal a batch on size, age, or Region rotation;
7. submit the owned buffer to one configured I/O engine;
8. publish the index entry and mark the RAM entry clean only from successful
   completion.

Each shard has exactly two staging buffers, allowing one submitted batch and one
fill batch without allocating per request. RAM entries are byte-bounded per
shard; pending entries stay resident until their matching completion, while
clean victim order is selected at runtime from CLOCK, LRU, TinyLFU admission
over LRU, SIEVE, FIFO, or S3-FIFO. The policy owns only slot-order metadata:
capacity charging, pending eligibility, TTL, and full-key validation remain one
common mechanism. Values that do not fit L1 continue through L2 without an
additional queue. FIFO Region reuse remains the only SSD replacement policy.
L1 capacity remains charged through the last returned value handle, so eviction
cannot hide memory retained by a slow caller.

TinyLFU uses an aged four-row 4-bit frequency sketch sized from the maximum
charged entry count. S3-FIFO divides resident bytes into 10% small and 90% main
targets and bounds its non-resident hash-only ghost queue by the corresponding
maximum entry count. Policy metadata is covered by the fixed per-entry L1
overhead estimate; it cannot increase the configured payload/entry charge.

A `get` first checks the shared RAM tier. On an L1 miss it snapshots the relevant
revision, looks up the fixed-size L2 index, reserves one bounded engine slot,
allocates the exact aligned read range, reads and validates the record, then
revalidates Region, index, append-shard authority, and that the Region belongs
to the namespace's configured set before promoting a clean RAM copy. A
concurrent newer `put` prevents an older L2 result from being promoted.
Expiration is lazy: the wall clock is sampled only after a matching entry with
a nonzero expiration is found, so namespace-only and default hits do not pay
for timekeeping.

The I/O pool contains the requested number of workers. In sync mode these are
positioned-I/O workers; with io_uring they are independent rings. Hash routing
keeps work distributed while total in-flight I/O remains explicitly bounded.

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

`flush` drains and data-syncs current writes but deliberately does not publish
`CLEAN`; only graceful ownership termination may create recoverable state.

## Configuration identity

Static configuration is included in the data superblock fingerprint and clean
image binding. It consists of capacity, Region size/count, index slots, data
shards, RegionSet ranges, namespace ownership, hash-algorithm
identity, and hash seed. A mismatch reformats disposable cache state rather
than trying to migrate it. The single implicit set preserves the original
single-pool layout identity.

RegionSet weights are resolved once at open by largest remainder at whole-Region
granularity, with stable RegionSet ID as the tie breaker. Append shards are
distributed evenly in the same stable order and are deliberately independent
of capacity weights. RegionSet zero owns every namespace not explicitly listed
by another set. A recovered global queue order is partitioned back into per-set
queues; warm publication flattens them again, so format 1 needs no per-record
RegionSet field. Runtime reweighting, cross-set borrowing, migration, and
per-set L1/index/I/O quotas are outside this first boundary.
Routes that explicitly name RegionSet zero and an explicit single-set-zero
layout are canonicalized to the implicit default, avoiding cold starts for
configurations with identical physical behavior.

Runtime configuration is not persisted and may change between opens. It
includes I/O engine and mode, worker count, total I/O concurrency,
L1 capacity and eviction policy, write-buffer and batch sizes, flush delay,
aggregate memory limit, waiting-policy write limit, and write backpressure. Foreground L2 reads
reserve one immediately available engine execution slot after an index hit,
then allocate one actual-size aligned range; they have no separate public
admission policy. Write occupancy leaves the final slot of a multi-entry I/O
engine available to reads, while a waiting write receives a bounded handoff.
Runtime configuration is validated before filesystem mutation.

The managed logical disk peak is deterministic: the fixed data file and 8 KiB
state file plus two fixed-size recovery images. The second image is the bounded
cost of atomic warm publication. Cold start removes stale images and abandoned
temporary images before exposing an anonymous runtime. Every cache thread uses
an explicit 512 KiB stack and the worst-case shard, I/O, and shutdown-reaper
stack topology participates in the aggregate memory plan. The operating system
still controls physical stack commitment. Buffered-I/O page cache, filesystem
metadata, and allocator internals remain outside the cache-owned byte budgets.
The public resource snapshot exposes current/peak managed reservations, their
hard limit, and the configured logical-disk peak. Optional activity statistics
separately classify read-memory and busy-engine misses.

`HybridCache::snapshot()` also reads relaxed process-local counters for L1/L2
hits and misses, read-memory and busy-engine misses, L1 promotions,
evictions/bypasses/admission rejections, logical bytes, write rejections, failures,
and Region rotations. Health is `Running`,
transiently `Draining`, one-way `MissOnly` for
fail-open reads, or `Failed` after a shard worker terminates. None of this state
is persisted or participates in format identity. Activity counters are
cache-line-aligned per shard and opt-in at runtime; health and resource bounds
remain observable when counters are disabled.

## Safety boundary

HybridCache loss is acceptable; returning unchecked or stale physical data is not.
All sizes and offsets are checked, persistent structures are field-encoded and
checksummed, reads verify full keys, Region reuse uses incarnation authority,
and bounded buffers retain ownership through I/O completion. Device or
structural failures may reject work or turn reads into misses.
