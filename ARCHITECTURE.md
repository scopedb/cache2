# C² architecture

C² is a bounded, best-effort lookaside cache. Each L2 attempt uses one index
lookup and one validation pass and may return a stale value.

It is not an authoritative store. A miss, bypass, eviction, stale hit, rejected
recovery image, or complete cache loss must leave the caller able to continue
through its authoritative path.

## Design priorities

When goals compete, C² keeps this order:

1. Keep request-path work short, simple, and bounded.
2. Preserve hit rate within fixed memory and work budgets.
3. Prefer newer values when an already available sequence number makes that
   cheap.

No request path grows a cache-owned allocation, queue, probe chain, retry loop,
or eviction scan without a fixed bound. Saturation therefore degrades to L1
bypass, a cache miss, mutation overload, or a bounded index eviction rather
than hidden waiting or unbounded work. The cache does not provide linearizable
reads or make accepted writes durable; `close_warm` is the only operation that
publishes recoverable state.

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

## Consistency model

Each accepted mutation receives a logical sequence number. L1 uses that number
to avoid replacing a newer exact-key resident value with an older publication
or promotion, and a sequenced delete does not remove a newer L1 value. Reclaim
preserves the original logical sequence when it rewrites a live record.

The compact L2 index deliberately stores no sequence number or full key. A
delayed write publication may replace a newer mapping, and a delayed
publication may make a deleted key visible again. Reclaim changes a mapping
only while it still names the exact source Region address, so a foreground put
or delete that has already changed the mapping wins. These rules improve
freshness cheaply but do not establish a newest-value guarantee.

`put` and `put_l2` success means that bounded staging accepted the mutation.
`drain` waits for accepted Region writes and index publication, but it is an I/O
completion barrier rather than a durability or recovery boundary. Every value
returned by `get` has passed full-key and record validation even when it is
stale.

## Storage layout

### L1

L1 is process-local and always starts empty. It is split into independently
locked shards so each operation touches one short critical section. Lookups use
a small try-lock retry budget and then continue to L2 on contention. Admission,
promotion, and cleanup also use bounded try-lock work; contention bypasses L1
instead of delaying the foreground operation.

Entry slots, directories, free lists, and eviction metadata are allocated at
open. Retained key/value bytes plus a fixed ownership charge consume a fixed
byte capacity, while the fixed metadata is charged separately to the managed
memory plan. A complete charged entry above 256 KiB bypasses L1 so a large
Region record cannot dominate a shard lock or require a large in-lock copy.
Same-hash chains, compare-exchange attempts, victim scans, and the number of
victims per admission all have small fixed limits. If those limits cannot
produce a slot and enough bytes, the candidate continues through Region without
an L1 copy.

The eviction policies share those bounds but spend metadata differently:

| Policy | Design |
| --- | --- |
| CLOCK | The default uses one visited bit and a bounded shard-local hand. It keeps policy metadata and hit work small. |
| S3-FIFO | Uses small and main resident FIFOs, a metadata-only ghost FIFO, and a saturating two-bit frequency. Hits update frequency without moving queue nodes; repeated small-queue entries and ghost hits can enter the main queue. It spends additional fixed metadata to distinguish reused entries from one-hit traffic. |

Evicted values may remain alive while a caller owns them. Admission never waits
for such retained values to release their memory charge; it selects another
bounded victim or bypasses L1.

### L2 index

L2 uses a fixed index sized by default at roughly two slots per expected live
key. The resulting nominal load target leaves room for bounded placement
without making the index dynamically grow. Each stable slot is eight bytes: a
14-bit fingerprint, two-bit candidate displacement, Region and offset, and a
record-size class. The size class is an upper bound that permits one bounded
record read; the record supplies the exact envelope, full key, and sequence
number after I/O.

Each hash has four deterministic candidates in one canonical partition. An
upsert replaces a matching fingerprint candidate, uses an empty candidate, or
performs at most two relocation hops. If all bounded placements fail, it
replaces one deterministic candidate and records an overflow eviction. The
index never resizes, follows an unbounded cuckoo path, or adds an overflow
chain. Saturation can reduce hit rate, but it cannot make mutation latency grow
with index occupancy.

Lookups take one partition try-read guard and inspect at most the four
candidates. Partition contention, a page currently being validated, or an
absent fingerprint returns a miss before read-buffer allocation. A fingerprint
match is only a read candidate: Region generation, hash, full key, lengths, and
checksums remain the correctness authority.

The index image stores 504 slots plus a checksummed header in each 4 KiB page,
or about 8.13 bytes per slot. Two volatile heat bits add 0.25 bytes per slot. A
4 TiB cache averaging 16 KiB per entry therefore uses about 4.06 GiB for its
536,870,912-slot index image and 128 MiB for heat.

### Region store

The data file is divided into fixed Region extents. Each append shard owns one
Active Region, two Region-sized staging buffers, and one ordered worker. Sealed
Regions join one global FIFO, so clean capacity is shared by all shards while
descriptor use stays constant.

This layout converts small foreground mutations into ordered, batched Region
writes without allocating a queue entry per value. Hash-routed append shards
keep mutation gates local, while the global sealed/free FIFO prevents capacity
from becoming permanently stranded behind one shard. Two staging buffers let a
worker write one sealed batch while foreground mutations fill the other; if
both are unavailable, admission is throttled instead of allocating a third.

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

Full staging or short-path contention returns structured
`ErrorKind::Overloaded`. Admission is shard-local. Success means accepted
staging; `put_l2` becomes visible when publication completes. `drain` fences
all mutations accepted before its operation barrier and waits for their Region
writes and L2 publication, but does not issue the recovery durability syncs.

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

### Persistent artifacts

C² owns three distinct files in one directory:

| File | Authority |
| --- | --- |
| Data | A checksummed 4 KiB superblock followed by fixed Region extents. The superblock fixes cache/data identities, geometry, hash seed, record format, and static-configuration fingerprint. It has no session-state bit. |
| State | Two checksummed 4 KiB slots. The newest valid generation is the sole authority for `EMPTY`, `RUNNING`, or `CLEAN` and binds an exact data identity; `CLEAN` additionally binds an exact image identity, generation, and length. |
| Clean image | An immutable 4 KiB header followed by checksummed L2 index pages and mandatory Region metadata. It is produced only by a successful warm close. |

The two state slots tolerate one torn page. Before serving requests, open writes
fresh `RUNNING` generations into both slots and syncs them with one barrier.
Replacing both slots prevents a torn future state write from reviving an older
`CLEAN` record after Region reuse has begun.

Recovery restores the L2 index and Region ownership, queue, generation, and
written-prefix metadata. L1 entries, L2 heat bits, metrics, in-flight work,
queues, and worker resources are process-local and start empty. Runtime tuning
may change across opens. A changed append-shard count reuses recovered Active
Regions, activates Free Regions when growing, or seals surplus Active Regions
when shrinking; growth without enough Free Regions selects a cold start.

### Open state machine

Open first acquires exclusive ownership of the data and state files, then
inspects the data superblock and the two state slots without scanning Region
extents or the complete index:

| Latest usable state | Open result |
| --- | --- |
| Fresh or `EMPTY` | Construct an empty index and Region topology. |
| `RUNNING`, missing state, or rejected state | Discard prior cache contents and construct an empty runtime without scanning Region records. |
| Matching `CLEAN` | Validate the image header and complete Region metadata, then attempt a warm index mapping. |

Before workers start or the cache becomes observable, every successful open
publishes and syncs `RUNNING`. Consequently a crash, process kill, drop, or fast
close leaves an unclean state whose next open is cold.

For a warm open, the state, data superblock, image header, image length, index
layout, and Region metadata must describe one identity and generation. The
checks include the cache UUID, data and image identities, generations, hash
seed, static-configuration fingerprint, and expected geometry. A mismatch,
unsupported format, missing/truncated image, invalid Region metadata, failed
append-shard rebind, or unavailable private mapping rejects the complete image
and starts cold.

The index image is mapped writable and private. Runtime mutations therefore use
copy-on-write pages and never modify the immutable clean image. Index pages are
validated lazily on first read or mutation to avoid an O(slot-count) startup
scan; the header and Region metadata are validated eagerly. A page CRC,
identity, layout, or slot-semantics failure makes the shared image unusable and
moves safe reads to miss-only rather than returning unvalidated data.

### Warm close publication

`close_warm` establishes one recoverable snapshot in this order:

1. Stop admission, fence accepted work, stop workers, and require a healthy,
   fully quiescent runtime.
2. Freeze the index and Region metadata as one authority.
3. Sync completed Region data.
4. Write a new header, complete index, and Region metadata to a temporary image;
   sync it, atomically rename it over the image path, and sync the parent
   directory.
5. Write and sync a new `CLEAN` state that binds the exact data and installed
   image. `CLEAN` is always the final publication.

`drain` covers only the completion part of step 1. It neither syncs Region data
nor writes an image or `CLEAN` state.

The ordering makes crash outcomes unambiguous:

| Interruption point | Next open |
| --- | --- |
| During normal operation, drop, or fast close | `RUNNING` remains authoritative; start cold. |
| Before the temporary image is installed | Ignore/remove the temporary image; start cold. |
| After image rename but before `CLEAN` sync | The image is not named by state and is ignored; start cold. |
| After matching `CLEAN` sync | Warm recovery is eligible, subject to full identity and validation checks. |

There is no in-place repair or Region scan after rejected recovery. Cache data
is disposable, so cold fallback is safer and keeps startup work bounded.

Stale data, overload, and cache loss are valid outcomes. Every returned value
passes address, key, and checksum validation. Structural or device faults move
the cache to miss-only when reads can fail open safely; mutations still report
errors. Public failures carry an `ErrorKind`, `ErrorOperation`, and the original
`std::io::Error`; the complete policy is documented in [Error
handling](ERRORS.md).
