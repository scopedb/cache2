# C² architecture

C² is a bounded, best-effort lookaside cache. Each L2 attempt uses one index
lookup and one validation pass and may return a stale value.

## Topology

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
boundary. L1 compares the full key, and an L2 hit validates the key stored in
the record. Returned values always match the complete key.

## Storage layout

### L1

L1 is split into independently locked shards. Entry slots, directories, free
lists, and CLOCK or S3-FIFO metadata are allocated at open. Retained key/value
bytes are charged to a fixed capacity. Same-hash chains and victim searches
have constant work limits; pressure bypasses L1.

### L2 index

L2 uses a fixed index sized at roughly two slots per expected live key. Each
stable slot is eight bytes: Region, offset, record-size class, fingerprint, and
candidate displacement. Four deterministic candidates and relocation depth two
bound every operation. The index stores compact location metadata; the record
supplies keys and sequence numbers after I/O.

The index image stores 504 slots plus a checksummed header in each 4 KiB page,
or about 8.13 bytes per slot. Two volatile heat bits add 0.25 bytes per slot. A
4 TiB cache averaging 16 KiB per entry therefore uses about 4.06 GiB for its
536,870,912-slot index image and 128 MiB for heat.

### Region store

The data file is divided into fixed Region extents. Each append shard owns one
Active Region, two Region-sized staging buffers, and one ordered worker. Sealed
Regions join one global FIFO, so clean capacity is shared by all shards while
descriptor use stays constant.

## Request paths

### `put` and `put_l2`

1. Compute the key hash and encoded size once.
2. Enter bounded mutation admission and reserve an append position under one
   short manager try-lock.
3. Encode into the selected shard's current buffer.
4. For `put`, attempt bounded L1 admission. For `put_l2`, stage through Region
   and remove an older exact-key L1 entry best effort.
5. Return after bounded in-memory admission.
6. The shard worker writes sealed batches and publishes their L2 mappings only
   after write completion.

Full staging or short-path contention returns `WouldBlock`. Admission is
shard-local. Success means accepted staging; `put_l2` becomes visible when
publication completes.

### `get`

1. Probe one L1 shard under a short critical section.
2. On miss, try one L2 index partition. Contention or an absent candidate
   returns a miss before buffer allocation.
3. Build one Region-bounded read plan from the candidate.
4. Reserve read execution. By default, unavailable capacity is a miss. With a
   configured timeout, at most one waiter per read worker may wait for a slot;
   it retains the plan and acquires the data buffer after admission.
5. Allocate one managed, alignment-rounded buffer and submit one record read.
6. Validate the planned address, Region generation, size class, hash, full key,
   lengths, sequence structure, and checksums.
7. Attempt bounded L1 promotion. Otherwise return a Region-backed value that
   owns the read allocation until dropped.

With read waiting enabled, a full wait queue, unavailable buffer, or expired
deadline is an explicit overload. Each request uses one index lookup and one
record read, so a concurrently superseded valid record may be returned.

### `delete`

Delete allocates a sequence number, performs one bounded in-memory index
removal, and attempts exact-key L1 cleanup. A delayed older publication may
make a stale value visible again.

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
uses background I/O lanes and bounded single-pass work.

## Resource bounds

### I/O

Reads and writes use independent bounded engine pools. Reclaim has separate
read lanes. POSIX uses positioned worker I/O; optional io_uring uses fixed-depth
rings. Buffered I/O is the default. Direct mode aligns runtime record I/O and
keeps control, recovery, and unavoidable remainder operations buffered. Locks
cover bounded in-memory work and release before device I/O.

### Memory

The managed-memory limit covers the index mapping, heat bits, L1, append
buffers, reclaim buffers, metadata, cache-owned thread stacks, recovery scratch,
and transient reads. Total deployment memory additionally includes allocator
metadata, Tokio, process overhead, and the kernel page cache. Invalid plans fail
during open.

### Storage path

C² owns one logical data path. Multi-device deployments stripe below the
filesystem with RAID0 or an equivalent layer. Request routing, recovery
identity, and descriptor count stay independent of device topology.

## Recovery and failures

Opening marks the cache `RUNNING` before serving requests. After a crash, drop,
or fast close, the next open starts empty in constant time.

`close_warm` stops admission, drains accepted work, syncs completed data,
writes and syncs a checksummed image, installs it atomically, and publishes a
matching `CLEAN` state last. A warm open maps the image privately and validates
pages lazily. L1 always starts empty. Missing, corrupt, stale, unsupported, or
configuration-mismatched state is discarded.

Stale data, overload, and cache loss are valid outcomes. Every returned value
passes address, key, and checksum validation. Structural or device faults move
the cache to miss-only when reads can fail open safely; mutations still report
errors.
