# C² Engineering Constraints

## Priorities

Use this order when correctness and performance goals compete:

1. Keep the request path simple and fast.
2. Preserve or improve hit rate.
3. Prefer newer values when sequence information makes that cheap.

C² uses best-effort consistency and may return stale hits. Sequence numbers
provide advisory ordering. Each L2 attempt uses one index lookup and one local
validation pass.

## Global bounds

- Bound every cache-owned allocation, queue, buffer, probe, scan, retry, and
  eviction decision.
- Resolve saturation through L1 bypass, a cache miss, mutation throttling, or
  explicit overload.
- Keep critical sections short and shard-local. Release locks before device I/O.
- Keep read, write, and reclaim I/O pools bounded and independent.
- Compute the full-key hash and record size once at the public boundary.
- Give compare-exchange bookkeeping a small retry budget, then bypass or miss.
- Keep statistics optional and implement enabled counters with low-cost atomics.

## Read path

- Probe L1 through shard routing and one short critical section. Bound
  same-hash chains and victim work; pressure bypasses insertion and promotion.
- Consult L2 after every L1 miss. Index misses and index or warm-page contention
  return a miss before read-buffer allocation.
- Admit one Region- and size-class-bounded read. Reserve one read slot, charge
  one managed buffer, and validate the record locally.
- Expand direct-I/O reads to 4 KiB boundaries within the selected Region.
- Use the full read-pool depth for immediate admission.
- Optional waiting begins after candidate selection and is limited by a short
  deadline and one waiter per worker. A queued request retains its plan and
  allocates its buffer after admission.
- Queue, memory, and timeout pressure return overload in wait mode. Memory and
  engine pressure return a miss in immediate mode.
- Validate address, generation, size class, hash, full key, lengths, checksums,
  and sequence structure in one pass.
- Successful promotion returns an L1-backed value, preserves Region as the hit
  source, and releases the transient read buffer. Bypassed promotion may return
  a zero-copy Region value that owns the aligned allocation until drop.
- Heat updates are single, bounded, lossy relaxed-atomic operations independent
  of the read result.

## Mutation path

- Finish foreground mutations after bounded in-memory work. Accepted writes
  publish asynchronously.
- Preflight shard capacity before allocating an append receipt. Reserve the
  Region tail and open span under one manager try-lock; encode under the shard
  mutation gate after releasing the manager guard.
- Batch Region writes. Background shard workers own write-slot waits; explicit
  completion barriers may wait for those workers.
- Publish an L2 index entry after its data write completes.
- Make L1 admission immediately readable and evictable. L1 bypass continues
  through Region.
- Route `put_l2` payloads through Region, apply best-effort L1 cleanup, and make
  them visible after L2 publication.
- Preserve logical sequence numbers across reinsertion and use them to cheaply
  prefer newer L1 values. L2 stores compact location metadata.
- Accept older valid values from contention, eviction, delayed publication, and
  promotion. Validate the complete key after every hash match.
- Treat `drain` as the completion barrier for accepted writes.

## Recovery and failure

- Treat eviction, bypass, rejection, throttling, misses, stale reads, overload,
  and cache loss as valid cache outcomes.
- Publish a clean recovery image during warm close. Fast close and unclean exit
  reopen empty.
- Move unsafe I/O, index, or metadata failures to miss-only. Reads then return
  misses, while mutations report errors.
- Return values that pass bounds, key, and checksum validation. Failed
  validation returns a miss.
