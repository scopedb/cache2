# cache-rs Engineering Constraints

## Decision Priority

Use this order whenever correctness and performance goals conflict:

1. Do not wait for internal capacity unless the caller explicitly selected the
   bounded read-buffer wait policy or a waiting write policy.
2. Do not return a stale value.
3. Preserve or improve hit rate.

Prefer a cache miss over waiting for internal capacity or serving an obsolete
version. Explicit bounded read-buffer waiting is the only foreground-read
exception. An admitted L2 lookup may complete its single bounded record read so
L2 can decide the result.

## Performance Contract

- Steady-state `put` must not wait for device I/O, batch completion, data
  synchronization, or recovery publication.
- Waiting is allowed only for an explicit barrier such as `drain`, `flush`, or
  graceful warm close, when the caller explicitly selects a waiting
  write-backpressure policy, or when the caller selects the bounded
  read-buffer wait policy. An admitted L2 hit may otherwise wait only for its
  own single bounded record read.
- Keep memory, queues, staging buffers, index probes, and eviction work strictly
  bounded. Never introduce an unbounded allocation, scan, retry loop, or queue.
- On saturation, bypass L1, return a cache miss, or reject with an overload
  result. Only the explicit bounded read-buffer policy and caller-selected
  write policies may wait for capacity.
- Keep the admitted L1 path limited to shard routing and short, bounded
  in-memory critical sections. Do not add work needed only by bypass or failure
  cases to the normal hit path.
- L1 lookup and pending publication retain only their short shard-local
  critical sections so an admitted `Pending` value remains immediately visible
  and an older L1 value cannot escape a newer Region mask. Optional promotion
  bypasses on lock contention.
- Give every eviction selection, multi-victim admission, and frequency-aging
  operation a small constant work budget. Exhausting that budget bypasses L1.
- Never hold a lock across device I/O. Keep mutation critical sections short
  and shard-local whenever possible.
- Publish Region writes in batches. Never add per-record flushes, syncs, or
  completion waits.
- An L1 miss must always consult L2. A true L2 index miss returns immediately
  without acquiring a read buffer.
- L2 owns the final result. By default, an L2 candidate either acquires one read
  buffer immediately or returns `WouldBlock` when the pool is full. An explicit
  bounded-wait policy must release the Region pin before sleeping and re-probe
  L2 exactly once after buffer admission. After buffer admission, L2 attempts
  one non-waiting I/O submission and owns the result: one aligned read plus
  exact revalidation, or a miss when the engine cannot admit that read
  immediately.
- Read-buffer exhaustion is the only `get` throttle boundary. Do not add a read
  admission queue, background read worker, pending/ready table, retry protocol,
  or another throttle resource to `get`. Read waiting must be bounded; never
  offer an unlimited read-buffer block because a returned Region value may own
  the capacity needed to wake the same caller.
- Distinguish an observed empty read-buffer pool from pool-lock contention.
  Only observed exhaustion may reject or wait; bookkeeping contention is an L2
  miss. Foreground I/O submission-fence contention is also an L2 miss.
- Allocate every foreground read buffer to its maximum size during open. A
  successful buffer admission must not encounter lazy allocation or growth.
- Treat the read-buffer slot count as the sole foreground L2 read budget. A
  successful L1 promotion returns an L1-backed value and releases the transient
  buffer before `get` returns while preserving Region as the hit source. If
  promotion bypasses, the zero-copy Region value may retain its slot until the
  caller drops it.
- Under the default reject policy, shard staging is the only write admission
  boundary. Do not put a global request gate in front of it.
- Preflight shard capacity before allocating an append receipt. Reserve the
  Region tail and commit its open-span accounting under one manager try-lock;
  the shard mutation gate then protects encoding without holding that manager.
- Index-partition mutation, warm-page validation, and Region-generation
  contention are misses. Never wait or spin behind another request before
  deciding an L2 candidate.
- Bound every L1 same-hash bucket to a small constant and bypass L1 when that
  bucket is full. Full-key validation remains mandatory within the bound.
- Compute the namespaced key hash and planned record size once at the public
  operation boundary and pass them down unchanged.
- Give compare-exchange bookkeeping a small constant retry budget. Exhausting
  that budget bypasses optional work or returns a miss instead of spinning.
- Statistics must remain optional and use low-cost atomic accounting when
  enabled.

## Visibility Semantics

- A value admitted to L1 is immediately readable as `Pending` while its Region
  write is still in flight.
- A `Pending` L1 value is not evictable. It becomes `Clean` only after its
  matching Region record completes.
- If a value cannot enter L1, do not wait for Region completion. Install a
  transient Region-index mask and return immediately.
- While that mask is active, reads must return a miss instead of falling back
  to the older Region value.
- The completed record replaces the matching mask at the same sequence number.
- The required degradation order is `new value -> miss`, never
  `new value -> stale value`.
- Expired pending values hide the Region tier. Expired clean values may be
  removed and treated as misses.
- Namespace and full-key validation remain mandatory after hash lookup so hash
  collisions cannot return another entry.

## Concurrency and Versioning

- Every mutation has a monotonic sequence number. A newer sequence always wins.
- A delayed older publication must never overwrite, expose, or evict a newer
  version of the same key.
- Scope completion watermarks to append shards. A memory-shard-wide version
  floor must not suppress unrelated keys.
- Concurrent operations may be linearized in any order allowed by their
  overlap, but the final visible state must respect sequence ordering.
- After Region I/O, revalidate the exact index record identity, Region
  generation, cache epoch, and clear floor before returning the value.
- Transient index masks are runtime-only and must be resolved before publishing
  a warm recovery image.

## Cache and Durability Semantics

- cache-rs is a best-effort cache, not a database. Eviction, bypass, rejection,
  and misses are valid outcomes; stale reads are not.
- Publish an L2 index entry only after the corresponding data write completes.
- `drain` is an accepted-write completion barrier, not a durability sync.
- `flush` drains accepted writes and then issues the device data-sync operation.
- Warm close publishes a clean recovery image. Fast close or an unclean exit
  must recover safely as an empty cache rather than trust incomplete state.

## Failure Semantics

- Internal I/O, index, or metadata failures should move the cache to miss-only
  when safe operation can no longer be guaranteed.
- Reads in miss-only mode fail open as cache misses rather than application data
  errors.
- Resource overload remains explicit, bounded, and observable.
- If freshness and bounded execution cannot both be guaranteed, return a miss.
