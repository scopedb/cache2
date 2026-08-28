# C² architecture

C² is a best-effort lookaside cache. The design keeps all foreground work
bounded and accepts stale values when avoiding a fence, retry, or second lookup
makes the request path simpler.

```text
Cache
├── sharded in-memory L1
├── fixed partitioned L2 index
├── hash-routed append shards
├── global FIFO of sealed and free Regions
├── separate bounded read, write, and reclaim I/O lanes
└── data file, state file, and optional clean image
```

The complete raw key is hashed once with seeded XXH3-64 at the public operation
boundary. L1 still compares the full key, and an L2 hit still validates the key
stored in the record. A hash match alone never returns a value.

## Core structures

L1 is split into independently locked shards. Entry slots, directories, free
lists, and CLOCK or S3-FIFO metadata are allocated at open. Retained key/value
bytes are charged to a fixed capacity. Same-hash chains and victim searches
have constant work limits; pressure bypasses L1.

L2 uses a fixed index sized at roughly two slots per expected live key. Each
stable slot is eight bytes: Region, offset, record-size class, fingerprint, and
candidate displacement. Four deterministic candidates and relocation depth two
bound every operation. The index has no tombstones and does not store keys or
sequence numbers; the record supplies both after I/O.

The index image stores 504 slots plus a checksummed header in each 4 KiB page,
or about 8.13 bytes per slot. Two volatile heat bits add 0.25 bytes per slot. A
4 TiB cache averaging 16 KiB per entry therefore uses about 4.06 GiB for its
536,870,912-slot index image and 128 MiB for heat.

The data file is divided into fixed Regions. Each append shard owns one Active
Region, two Region-sized staging buffers, and one ordered worker. Sealed Regions
join one global FIFO, so clean capacity is shared by all shards. Regions are
extents in one file, not separate files; descriptor use does not grow with
Region count.

## Request paths

### Put

1. Compute the key hash and encoded size once.
2. Enter bounded mutation admission and reserve an append position under one
   short manager try-lock.
3. Encode into the selected shard's current buffer.
4. For `put`, attempt bounded L1 admission. For `put_l2`, skip value admission
   and remove an older exact-key L1 entry best effort.
5. Return without waiting for I/O.
6. The shard worker writes sealed batches and publishes their L2 mappings only
   after write completion.

Full staging or short-path contention returns `WouldBlock`. There is no global
admission queue. A successful mutation is accepted, not durable; `put_l2` also
is not L2-visible until publication completes.

### Get

1. Probe one L1 shard under a short critical section.
2. On miss, try one L2 index partition. Contention or an absent candidate is a
   miss and allocates no read buffer.
3. Build one Region-bounded read plan from the candidate.
4. Reserve read execution. By default, unavailable capacity is a miss. With a
   configured timeout, at most one waiter per read worker may wait for a slot;
   it holds the plan but no data buffer.
5. Allocate one managed, alignment-rounded buffer and submit one record read.
6. Validate the planned address, Region generation, size class, hash, full key,
   lengths, sequence structure, and checksums.
7. Attempt bounded L1 promotion. Otherwise return a Region-backed value that
   owns the read allocation until dropped.

With read waiting enabled, a full wait queue, unavailable buffer, or expired
deadline is an explicit overload. There is still no retry, second index lookup,
or freshness check. A valid record superseded concurrently may therefore be
returned.

### Delete

Delete allocates a sequence number, performs one bounded non-waiting index
removal, and attempts exact-key L1 cleanup. It appends no record and submits no
I/O. A delayed older publication can make a stale value visible again; this is
within the cache contract.

## Reclamation

When free capacity is low, a reclaim worker takes the oldest sealed Region,
reads its used prefix sequentially, and walks the self-sized records. A mapping
is changed only if the index still points at that exact Region address.

The first L2 candidate access sets `seen`; a later access sets `hot`. Reclaim
clears these volatile bits. Cold current mappings are removed. Hot current
records may be rewritten through an existing append shard while preserving
their logical sequence number. Reinsertion is capped at roughly one eighth of
the reclaimed bytes; staging or budget pressure drops the remaining candidates.

The source Region stays pinned until accepted replacement writes finish.
Conditional index replacement lets a newer foreground put or delete win over a
delayed reinsert. Only then is the Region returned to the free FIFO. Reclaim
does not add foreground I/O, waiting, or retries.

## I/O and memory bounds

Reads and writes use independent bounded engine pools. Reclaim has separate
read lanes. POSIX uses positioned worker I/O; optional io_uring uses fixed-depth
rings. Buffered I/O is the default. Direct mode aligns runtime record I/O but
keeps control, recovery, and unavoidable remainder operations buffered. No lock
is held across device I/O.

The managed-memory limit covers the index mapping, heat bits, L1, append
buffers, reclaim buffers, metadata, cache-owned thread stacks, recovery scratch,
and transient reads. It does not cover allocator metadata, Tokio, the process,
or the kernel page cache. Invalid plans fail during open.

C² owns one logical data path. Multi-device deployments should stripe below the
filesystem with RAID0 or an equivalent layer. This keeps request routing,
recovery identity, and descriptor count independent of device topology.

## Recovery and failure

Opening marks the cache `RUNNING` before serving requests. A crash, drop, or
fast close leaves no recovery authority, so the next open starts empty without
scanning the data extent.

`close_warm` stops admission, drains accepted work, syncs completed data,
writes and syncs a checksummed image, installs it atomically, and publishes a
matching `CLEAN` state last. A warm open maps the image privately and validates
pages lazily. L1 always starts empty. Missing, corrupt, stale, unsupported, or
configuration-mismatched state is discarded.

Stale data, overload, and total cache loss are acceptable. Wrong-key,
out-of-bounds, or corrupt data are not. A structural or device fault moves the
cache to miss-only operation when reads can still fail open safely; mutations
continue to report errors.
