# C² architecture

C² is a best-effort lookaside cache. The source of truth lives elsewhere, so
the design favors a short bounded request path over freshness or durability.

```text
Cache
├── bounded shard-local CLOCK L1
├── fixed partitioned mmap L2 index
├── append shards + global Region FIFO
├── bounded read I/O pool
├── bounded write I/O pool
├── one-depth background reclaim I/O lane
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
   allocate its exact aligned range against the managed-memory limit.
4. Perform one record read and validate location, Region generation, hash,
   full key, exact length, and checksum locally.
5. Promote to L1 when bounded admission succeeds; otherwise return the
   exact-size Region-backed value.

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
append shard, the background worker reads one sealed prefix sequentially,
walks its self-sized records, and removes each index mapping only if it still
points at that exact address. Only then does the Region enter the free FIFO.

An L2 candidate marks its Region with one relaxed reference bit during the
existing index pass. Reclaim gives referenced FIFO heads one bounded second
chance, inspecting at most eight alternatives; this adds no request-path lock
or I/O. Every record also carries its exact Region generation. The read plan
captures that generation from a per-Region atomic, and completion compares it
locally before returning the record.

Per-record hit counters and reclaim-time reinsertion are intentionally absent.
They would turn L2 hits into random index or shadow-table mutations and require
conditional publication after an asynchronous rewrite. Region second chance
keeps the heat signal bounded and off the index write path; entry reinsertion
should be added only if production-shaped traces show a material hit-rate gap.

The fixed index stores 10-byte slots with four deterministic candidates, at
most one relocation, and no tombstones or generation table. Point work is
therefore capped at four primary probes plus the bounded one-hop relocation
work on mutation. Index-partition contention is a miss or mutation overload,
never a wait.

L1 uses fixed startup allocations for entries, CLOCK metadata, free lists, and
its prehashed directory. Each same-hash chain is capped at eight full keys.
CLOCK selection and multi-victim admission have fixed work budgets; exhaustion
bypasses L1. Returned values retain their memory charge until dropped.

Read and write submissions use independent bounded pools. POSIX uses one engine
per direction with one execution slot per configured worker. Optional io_uring
uses a fixed-depth ring per configured worker. Reads never consume write slots,
and no lock is held across device I/O.

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
