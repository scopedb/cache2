# cache-rs architecture

## Contract

cache-rs optimizes the cache data path ahead of durability. The authoritative
copy of every value must live elsewhere.

```text
HybridCache
├── shared bounded RAM L1
├── fixed-capacity partitioned mmap index
├── namespace → RegionSet router
│ ├── RegionSet 0: append shards + private Region range + free/sealed FIFO
│ └── RegionSet N: append shards + private Region range + free/sealed FIFO
├── configurable I/O-engine pool
└── data, state, and clean-image files
```

RAM is a process-local L1 and the Region file is L2. Both tiers use the same key
hash. The hash is seeded XXH3-64; L1 disambiguates a hash
bucket with the namespace and complete key, while L2 validates the header
namespace and complete raw key after reading a hash-selected record. A
collision may therefore become a conservative miss but can never return
another key's value. There is one SSD format and no Bucket engine, journal, or
separate write-back executor.

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
5. publish the same sequence to RAM when it fits, before returning;
6. seal a batch on size, age, or Region rotation;
7. submit the owned buffer to one configured I/O engine;
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

A `delete` follows the same append shard, sequence allocation, fixed staging,
batch completion, and immediate-rejection admission path. Its foreground work adds only one
best-effort exact-key L1 removal. Completion replaces an older same-hash index
value with a sequenced tombstone encoded in the existing 24-byte slot; a newer
put may replace it, and an already missing delete consumes no index slot. Reads
stop at a matching tombstone as an index miss. They do not retry or validate
freshness a second time, so stale L1 values remain permitted by the cache
contract.

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
L2 index once, reserves one non-waiting engine slot, allocates the exact aligned
read range, and reads one record. If the hash-selected lane is full, admission
may probe one hash-derived alternate lane; it never scans or waits across the
pool. Local validation checks the planned location, sequence, hash, namespace,
complete key, lengths, and checksums.
There is no retry or second freshness check; a concurrently superseded but
otherwise valid value may be returned or promoted.

The I/O pool contains the requested number of workers. In sync mode these are
positioned-I/O workers; with io_uring they are independent rings. The sum of
fixed lane depths never exceeds the configured global in-flight bound.
Two-choice read routing reduces idle-lane fragmentation without adding a shared
admission counter to every I/O.

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
L1 capacity and shard count, one write-batch capacity, aggregate memory limit,
and optional statistics. Foreground L2 reads
reserve one immediately available engine execution slot after an index hit,
then allocate one actual-size aligned range; they have no separate public
admission policy. Write occupancy leaves the final slot of a multi-entry I/O
engine available to reads, while a waiting write receives a bounded handoff.
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
slot replacement. The replacement counters follow the optional statistics
switch; occupancy remains always available. Optional activity statistics
separately classify read-memory and busy-engine misses.

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
