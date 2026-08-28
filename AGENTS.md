# C² Engineering Constraints

## Decision Priority

Use this order whenever correctness and performance goals conflict:

1. Keep the request path simple and fast.
2. Preserve or improve hit rate.
3. Prefer a newer value when sequence information makes that cheap.

Consistency is best effort. A cache hit may be stale. Sequence numbers may
help prefer newer records, but must not add a fence, retry protocol, or second
validation pass to a read. Writes and deletes may throttle on bounded internal
capacity; this is expected to be uncommon in normal operation.

## Performance Contract

- Steady-state `put` and delete operations may wait for bounded in-memory
  admission, but must not wait for device I/O, batch completion, data
  synchronization, or recovery publication.
- An admitted L2 hit may wait only for its own single bounded record read.
- Keep memory, queues, staging buffers, index probes, and eviction work strictly
  bounded. Never introduce an unbounded allocation, scan, retry loop, or queue.
- On saturation, bypass L1, return a cache miss, throttle a mutation, or reject
  it with an overload result.
- Keep the admitted L1 path limited to shard routing and short, bounded
  in-memory critical sections. Do not add work needed only by bypass or failure
  cases to the normal hit path.
- L1 lookup and publication retain only short shard-local critical sections.
  Optional insertion and promotion may bypass on pressure.
- Give every L1 victim selection and multi-victim admission a small constant
  work budget. Exhausting that budget bypasses L1.
- Never hold a lock across device I/O. Keep mutation critical sections short
  and shard-local whenever possible.
- Publish Region writes in batches. Never add per-record flushes, syncs, or
  completion waits.
- Read and write submissions use separate bounded I/O-engine pools. Writes must
  never consume read-engine slots. Reads may use the complete read-pool depth
  but never wait for a slot. Write-slot waits remain confined to background
  shard workers; explicit completion barriers may wait for those workers.
- An L1 miss must always consult L2. A true L2 index miss returns immediately
  without acquiring a read buffer.
- L2 owns the final result. An L2 candidate plans one size-class-bounded record
  read, truncating the upper bound at its Region end and expanding it to 4 KiB
  boundaries only for direct I/O. It reserves one non-waiting engine execution
  slot, charges its owned buffer against the aggregate memory limit, and submits
  under the reservation.
  Allocation or engine pressure is a miss, not public read backpressure. An
  admitted request completes one read plus local exact-envelope validation.
- Do not add a read admission policy, read-buffer pool, read wait, background
  ready table, or retry protocol to `get`. The bounded I/O engine and aggregate
  memory limit are execution safety boundaries, not public read admission.
- A successful L1 promotion returns an L1-backed value and releases the
  transient aligned buffer before `get` returns while preserving Region as the
  hit source. If promotion bypasses, the zero-copy Region value may retain its
  alignment-rounded allocation until the caller drops it.
- Preflight shard capacity before allocating an append receipt. Reserve the
  Region tail and commit its open-span accounting under one manager try-lock;
  the shard mutation gate then protects encoding without holding that manager.
- Index-partition and warm-page contention are misses. Never wait or spin
  behind another request before deciding an L2 candidate.
- An L2 candidate may perform one bounded, lossy relaxed-atomic heat update.
  Heat bookkeeping must never acquire an exclusive index lock, wait, retry,
  allocate, perform another index lookup, or change the read outcome.
- Bound every L1 same-hash bucket to a small constant and bypass L1 when that
  bucket is full. Full-key validation remains mandatory within the bound.
- Compute the complete-key hash and planned record size once at the public
  operation boundary and pass them down unchanged.
- Give compare-exchange bookkeeping a small constant retry budget. Exhausting
  that budget bypasses optional work or returns a miss instead of spinning.
- Statistics must remain optional and use low-cost atomic accounting when
  enabled.

## Visibility Semantics

- A value admitted to L1 is immediately readable and immediately evictable.
- If a value cannot enter L1, the mutation may still complete through Region.
- L1 contention, eviction, delayed publication, and L2 promotion may expose an
  older valid value. This is an accepted best-effort outcome.
- Full-key validation remains mandatory after hash lookup so hash collisions
  cannot return another entry.

## Concurrency and Versioning

- Every Region record has a logical sequence number. Preserve it across
  reinsertion and use it to prefer a newer value where this is already part of
  a bounded L1 mutation. The compact L2 index does not retain it, so it is not
  a read token and does not guarantee freshness.
- Concurrent and delayed operations may expose an older valid version.
- After Region I/O, validate the returned record locally against the planned
  physical address, Region generation, size class, hash, full key, lengths,
  and header and payload checksums. Require a structurally valid logical
  sequence for L1 promotion, but do not perform a second index lookup or a
  global freshness check.

## Cache and Durability Semantics

- C² is a best-effort cache, not a database. Eviction, bypass, rejection,
  throttling, misses, and stale reads are valid outcomes.
- Publish an L2 index entry only after the corresponding data write completes.
- `drain` is an accepted-write completion barrier, not a durability sync.
- Warm close publishes a clean recovery image. Fast close or an unclean exit
  must recover safely as an empty cache rather than trust incomplete state.

## Failure Semantics

- Internal I/O, index, or metadata failures should move the cache to miss-only
  when safe operation can no longer be guaranteed.
- Reads in miss-only mode fail open as cache misses rather than application data
  errors.
- Resource overload remains explicit, bounded, and observable.
- Invalid, out-of-bounds, wrong-key, or corrupt records are misses and must
  never be returned.
