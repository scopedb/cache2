# C² architecture

C² is a best-effort lookaside cache. The source of truth lives elsewhere, so
the design favors a short bounded request path over freshness or durability.

```text
Cache
├── bounded shard-local CLOCK or S3-FIFO L1
├── fixed partitioned mmap L2 index
├── append shards + global Region FIFO
├── bounded read I/O pool
├── bounded write I/O pool
├── bounded one-depth background reclaim I/O lanes
└── data, state, and clean-image files
```

Both tiers use one seeded XXH3-64 hash computed from the complete raw key. Hash
lookup is never sufficient by itself: L1 compares the full key, and L2 validates
the full record after I/O.

## Request paths

### Put

1. Compute the complete-key hash and encoded record size once.
2. Reserve the Region tail and open-span accounting under one short manager
   try-lock.
3. Encode into the selected append shard's existing aligned buffer.
4. Publish to L1 immediately when bounded admission succeeds.
5. Return without waiting for device I/O.
6. The shard worker writes sealed batches and publishes L2 index entries only
   after completion.

Each append shard has two Region-sized buffers, so it may fill one while the
other is in flight. Saturation or short-path contention returns `WouldBlock`;
there is no foreground retry protocol or global admission queue.

### Get

1. Probe one L1 shard under a short critical section.
2. On miss, probe one L2 index partition without waiting for contention.
3. For a candidate, reserve one immediately available read-engine slot and
   allocate its bounded size-class range against the managed-memory limit.
4. Perform one record read and validate location, Region generation, hash,
   full key, exact length, and checksum locally.
5. Promote to L1 when bounded admission succeeds; otherwise return the
   zero-copy Region-backed value with its bounded read allocation.

An index miss allocates nothing. Read-memory, index, or engine pressure is a
miss. There is no read queue, retry, second index lookup, or freshness fence, so
a concurrently superseded but otherwise valid value may be returned.

### Delete

Delete allocates a sequence, performs one non-waiting bounded index removal,
and attempts exact-key L1 cleanup. It appends no Region record and submits no
I/O. A delayed older put may republish a valid stale value; this is accepted by
the cache contract.

## Reclamation and bounds

Every append shard owns one Active Region. All other Regions share one global
free/sealed FIFO, allowing any shard to consume clean capacity. A sealed Region
is never reused directly. When the free reserve falls below one Region per
append shard, a background worker reads the oldest sealed prefix sequentially
and walks its self-sized records.

Two volatile bitmaps track `seen` and `hot` for each exact index slot during the
existing index pass. The first L2 candidate access marks `seen`; a later access
marks `hot`. Reclaim consumes both bits and offers only a hot, still-current
record for reinsertion. Other current records are removed only when the index
still points at their exact old address. Reinsertion preserves the record's
logical sequence number, writes through an existing append shard, and
conditionally replaces the old address only after the batch completes.

The source Region stays pinned until accepted replacement batches finish. A
reclaim rewrites at most roughly one eighth of its used bytes; staging pressure
skips the remaining hot records instead of waiting. Only then does the source
Region enter the free FIFO. This keeps victim selection strict FIFO and adds no
foreground queue, retry, lock, or I/O.

The heat bitmaps cost two bits per index slot and reset on restart. They are a
two-access signal rather than a hit count: a reinserted entry needs two later L2
candidate accesses to become eligible for another reclaim cycle. Every record
also carries its exact Region generation. The read plan captures that
generation from a per-Region atomic, and completion compares it locally before
returning the record. The logical sequence remains in the record and L1, where
it orders bounded publication and promotion; the compact L2 read token does not
carry it or use it as a freshness fence.

The fixed index stores 8-byte slots with 20-bit Region and offset fields, an
8-bit record-size upper class, a 14-bit fingerprint, and a 2-bit displacement.
It uses four deterministic candidates, a fixed relocation search of depth two,
and no tombstones or generation table. Point work is therefore capped at four
primary probes; mutations may additionally move at most two occupants before a
bounded eviction. The exact record envelope is recovered and validated from
the one bounded read. Index-partition contention is a miss or mutation
overload, never a wait.

L1 uses fixed startup allocations for entries, eviction metadata, free lists,
and its prehashed directory. Each same-hash chain is capped at eight full keys.
CLOCK and S3-FIFO selection plus multi-victim admission have fixed work budgets;
exhaustion bypasses L1. Returned values retain their memory charge until dropped.

Read and write submissions use independent bounded pools. POSIX uses one engine
per direction with one execution slot per configured worker. Optional io_uring
uses a fixed-depth ring per configured worker. Reads never consume write slots,
and no lock is held across device I/O.

Physical striping is deliberately outside C². Multiple homogeneous devices
should be combined into one RAID0 or equivalent block device, while C² retains
one data file, one persistent identity, and one bounded set of I/O engines.
Native file-level striping would duplicate block-layer scheduling, tie recovery
format to path order, multiply file descriptors, and still provide no degraded
mode under the whole-cache failure contract.

## Recovery

Normal open publishes a `RUNNING` state fence before exposing the cache. A
crash, drop, or fast close therefore has no recovery authority and the next
open starts empty without scanning the data extent.

`close_warm` is the only recoverable shutdown:

1. stop admission and drain accepted work;
2. freeze Region and index authority;
3. sync completed data writes;
4. write, sync, and atomically install a checksummed recovery image;
5. publish matching `CLEAN` state last.

A matching clean image is mapped private on the next open. Missing, corrupt,
stale, unsupported, or configuration-mismatched state is discarded. L1 is
always rebuilt empty and warms through normal L2 reads.

## Failure boundary

Stale values, misses, overload, and cache loss are acceptable. Wrong-key,
out-of-bounds, or corrupt values are not. Persistent structures are explicitly
encoded and checksummed, all offsets and sizes are checked, and in-flight
buffers retain ownership until completion. A structural or device failure
moves the cache to miss-only operation when reads can still fail open safely.
