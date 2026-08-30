# C² configuration guide

C² keeps cache-owned memory, queues, probes, and request-path work bounded.
Configuration therefore decides not only throughput, but also how saturation
appears to the caller: an L1 bypass, cache miss, explicit overload, or bounded
eviction. There is no configuration that maximizes hit rate, minimizes tail
latency, absorbs every write burst, and uses the least memory at the same time.

This guide explains the interactions between `StaticConfig` and
`RuntimeConfig`, then gives tuning directions for common operating goals. The
profiles are starting points. Validate them with the production key count,
value-size distribution, concurrency, device, and hit-cost model.

## Measure the workload envelope first

Collect these quantities before choosing values:

| Quantity | Use |
| --- | --- |
| Maximum simultaneously live distinct keys | Sizes the fixed L2 index. Count current mappings, not lifetime writes. |
| Value-size distribution, including the maximum encoded record | Selects Region size, estimates live-key density, and determines read-buffer pressure. |
| Peak concurrent L2 reads after L1 | Sizes read admission. Total request concurrency is usually an overestimate. |
| Concurrent foreground writers and hash skew | Sizes append shards and write execution. |
| Write burst bytes and acceptable retry policy | Determines whether existing Region buffers are sufficient. |
| Logical cache turns per unit time | Indicates reclaim pressure and whether capacity is large enough. |
| Reuse distribution | Chooses L1 capacity and CLOCK versus S3-FIFO. |
| Cost of a cache miss versus overload | Chooses immediate read admission or bounded waiting. |

Tune persistent geometry before runtime topology. Region size and index size
are part of the on-disk identity; changing either makes existing cache contents
ineligible for warm recovery. Most `RuntimeConfig` values may be changed at
each open, although append-shard growth can also cause a cold start when the
recovered topology has too few Free Regions.

## Resource model

The managed-memory floor is approximately:

```text
fixed index and heat metadata
+ L1 retained-byte capacity and fixed L1 metadata
+ runtime controls, queues, and 512 KiB cache-thread stacks
+ 2 * Region size * append shards
+ Region size * reclaim workers
+ one Region-sized foreground-read allowance
```

This is a floor, not a recommended limit. Concurrent L2 reads allocate
alignment-rounded transient buffers after admission, and an unpromoted
Region-backed value retains its buffer until the caller drops it. Evicted L1
values can also remain charged while callers own them. Leave dynamic headroom
for the desired read concurrency and returned-value lifetime.

`open` rejects a configuration whose fixed plan cannot fit the managed-memory
limit. A configuration that only barely opens can still produce read-memory
misses or overload once concurrent transient buffers consume the remaining
budget.

The managed-memory limit is not an RSS limit. Allocator metadata, Tokio, the
application, mapped-file residency, and the kernel page cache are outside it.
Buffered I/O can therefore use substantial kernel memory even when the C²
managed-memory gauges remain below their limit.

### High-impact interaction map

| Change | Direct effect | Important coupled effects |
| --- | --- | --- |
| Increase `expected_entries` | Lowers fixed-index load | Grows index mapping, heat bits, recovery images, and potentially L1 slot metadata; leaves less managed memory for L1 and reads |
| Increase Region size | Holds larger records and more staged bytes per shard | Multiplies append/reclaim/read allowances, reduces Region count and rotation frequency, and makes reclaim coarser |
| Increase append shards | Adds mutation gates and staging paths | Adds two Region buffers, one Active Region, one worker, controls, and minimum geometry per shard; raises the allowed reclaim-worker ceiling |
| Increase read workers | Admits more L2 reads | Adds stacks/queues and increases possible transient-buffer demand; the meaning differs between POSIX and io_uring |
| Enable or lengthen read waiting | Trades immediate misses for bounded wait | Adds only one waiter per configured read worker; can raise p99 and returns overload when queue, memory, or deadline is exhausted |
| Increase write workers | Adds write I/O concurrency | Adds stacks/queue pressure and device contention; does not add foreground staging space |
| Increase reclaim workers | Recycles more Regions concurrently | Adds one Region buffer and lane per worker and can compete for device and append-staging capacity |
| Increase L1 capacity | Retains more reusable values | Reduces L2 demand but consumes retained bytes and fixed metadata from the same managed-memory limit |
| Increase L1 shards | Reduces shard-local contention | Adds fixed metadata and controls; too many small shards reduce useful capacity efficiency |
| Lower write flush threshold | Requests earlier partial publication | Increases write operation count and reduces batching without increasing staging capacity |

## Persistent layout

### Capacity and Region size

`StaticConfig::new(capacity_bytes)` defaults to 32 MiB Regions. Capacity must
be an exact multiple of the Region size. The geometry needs one Active Region
per append shard and at least one additional Region, so runtime topology can
make an otherwise valid static layout fail at open.

Every encoded key/value record must fit in one Region. Region size also scales
several resources and policies at once:

| Smaller Regions | Larger Regions |
| --- | --- |
| Lower append and reclaim buffer memory | More bytes available in the two buffers owned by each append shard |
| Finer-grained FIFO eviction and reclaim | Larger write batches and fewer rotations |
| More rotations and Region metadata activity | Supports larger individual records |
| More opportunities to separate hot and cold records | Coarser reclaim units and more hot/cold mixing |
| Smaller absolute reinsertion allowance per reclaimed Region | Larger absolute reinsertion allowance, still roughly one eighth of reclaimed bytes |

For `A` append shards and `R`-byte Regions, append staging alone is `2 * A * R`.
Changing from 24 MiB to 32 MiB with four append shards, for example, adds
64 MiB of fixed staging. Each reclaim worker adds another `R`-byte scan buffer,
and the memory plan keeps one additional `R`-byte read allowance.

Choose the smallest Region that safely holds the largest cacheable record,
then increase it only when fewer rotations or more burst absorption justifies
the memory and reclaim granularity. The valid range is 4 KiB-aligned through
32 MiB.

### Expected entries and index load

`with_expected_entries(E)` creates approximately `2 * E` physical index
slots. The input is the expected simultaneously live key count, not a byte
capacity and not the number of mutations between restarts.

If `N` is the measured maximum live-key count, deliberately passing a multiple
of `N` supplies placement headroom:

| Configured `E` | Slots per live key | Nominal offered load | Approximate index plus heat bytes per live key | Bias |
| ---: | ---: | ---: | ---: | --- |
| `N` | 2 | 50% | 17 B | Minimum fixed memory |
| `2N` | 4 | 25% | 34 B | Balanced |
| `4N` | 8 | 12.5% | 67 B | Hit-rate oriented |
| `8N` | 16 | 6.25% | 134 B | Aggressive hit-rate headroom |

The byte estimates use about 8.13 bytes per persisted slot plus two volatile
heat bits. Lower load reduces bounded-placement overflow and compact-identity
aliases, but it cannot mathematically guarantee that a cache lookup never
misses. C² validates the complete key after I/O, so an index collision becomes
a safe miss rather than a wrong-key hit.

The default index estimate is `capacity_bytes / 16 KiB`, followed by two slots
per estimated entry. Override it when actual live values are materially
smaller, key cardinality is known directly, or hit rate is more valuable than
fixed memory. An aggressively oversized index consumes managed memory that
could otherwise hold L1 entries or transient read buffers. Very large indexes
must still fit the platform's addressable page and mapping layout, the checked
recovery-image layout, and the configured managed-memory plan.

Use `StaticConfig::peak_disk_bytes()` when planning the data file, state file,
and two possible clean recovery images. Larger indexes increase both the
mapping extent and recovery-image space.

## Runtime topology

### L1 capacity, shards, and eviction policy

These three controls should be tuned together:

| Control | Increase when | Cost or failure mode |
| --- | --- | --- |
| `l1_capacity_bytes` | A reusable working set can avoid expensive L2 reads | Retained bytes and fixed entry metadata consume managed memory |
| `l1_shards` | L1 try-lock contention causes bypass or promotion loss | More directory, policy, and runtime-control accounting; tiny shards waste capacity |
| `L1EvictionPolicy::S3Fifo` | Reuse is skewed or scans create one-hit pollution | More fixed policy metadata than CLOCK |

Use a power-of-two shard count for the cheapest routing path. Do not scale
shards directly from total request concurrency: first observe L1 bypasses and
contention under the intended capacity. A small L1 split into thousands of
shards can have more metadata and less useful capacity without reducing
meaningful contention.

Increasing L1 capacity can be cheaper than increasing L2 read workers because
it reduces the peak concurrent L2 demand. It can also release transient read
buffers sooner when promotion succeeds. Conversely, an oversized L1 or an
oversized fixed index can leave too little managed-memory headroom for L2 reads.
Index sizing also feeds the fixed L1 slot plan: at the same L1 byte budget, a
larger expected-entry count can allocate more L1 slot metadata up to the
planner's density bounds. Recheck `l1.metadata_bytes` after changing the index.

Entries whose complete L1 charge exceeds 256 KiB bypass L1 regardless of the
configured byte capacity. Large-object workloads must therefore plan their
read concurrency from L2 behavior, not from the nominal L1 size.

Start with CLOCK when metadata and minimal hit-path work matter. Try S3-FIFO
when the workload has a stable reused set mixed with scans or one-hit entries.
Compare hit rate and bypasses at the same L1 byte budget.

### Read workers, waiting, and memory

The read path first selects an index candidate, then admits one bounded read.
Three resources can reject that plan independently:

1. read execution capacity;
2. the optional wait queue;
3. managed memory for the aligned read buffer.

`read_io_workers` adds execution capacity. With POSIX I/O it is both the number
of worker threads and the maximum admitted reads; every additional worker adds
a 512 KiB reserved cache-thread stack. With io_uring it selects independent
fixed-depth rings, currently depth 64 each. Worker counts are therefore not
portable between the two engines.

`read_io_wait_timeout` changes pressure behavior, not physical capacity:

| Setting | When execution is full | Best fit |
| --- | --- | --- |
| Zero | Return a cache miss immediately | Lowest bounded tail; authoritative fallback is cheap |
| Positive | Queue at most one waiter per read worker | Preserve more hits within a short latency budget |

A full wait queue, memory pressure, or deadline expiry returns explicit
`ErrorKind::Overloaded`. Making the timeout longer does not enlarge the queue
and cannot compensate for a severely undersized worker pool. If busy misses or
queue-full overloads dominate, add execution capacity or reduce L2 demand. If
memory misses dominate, adding workers can make the problem worse; add managed
headroom, reduce retained buffers, or improve L1 promotion instead.

Size POSIX workers from peak concurrent L2 reads, not total requests. For a
strict-hit bias, provision close to that peak and use zero or a very short
wait. For a memory/thread bias, use fewer workers plus a short deadline and
accept explicit overload. Validate p99 as well as hit rate.

Direct I/O expands reads to 4 KiB boundaries within the selected Region. Small
or unaligned records therefore need more buffer and device bytes than their
payload size suggests. Include that amplification when setting memory
headroom and read concurrency.

### Append shards, write workers, and flush threshold

The write pipeline is:

```text
foreground writer -> hash-routed append shard -> two Region buffers
                  -> ordered shard worker -> bounded write I/O pool
                  -> L2 index publication
```

The controls operate at different stages:

| Control | Primary benefit | Main cost |
| --- | --- | --- |
| `append_shards` | Reduces foreground mutation-gate contention and supplies independent staging paths | Two Region buffers, one Active Region, one worker, and fixed controls per shard |
| `write_io_workers` | Lets more completed batches reach the device concurrently | Worker stacks, queue capacity, and device contention |
| `write_flush_threshold_bytes` | Controls when a partial buffer requests publication | Smaller values issue more, smaller writes; larger values favor batching |
| Region size | Changes per-shard burst reservoir and rotation frequency | Multiplies staging and reclaim memory and changes eviction granularity |

Start with append shards near the number of genuinely concurrent foreground
writers, then measure. More shards are not monotonic: excess shards fragment
data across additional partial buffers and Active Regions, consume memory, and
can reduce useful capacity. Hash skew can still overload one shard even when
aggregate buffers look empty.

Start write workers near the append-shard count. Increase them only when write
I/O in-flight peaks at the configured capacity or slot-wait time grows while
the device still has headroom. More write workers cannot fix a foreground
mutation-gate collision, a full shard-local staging pair, or a saturated SSD.

The flush threshold is per append shard, 4 KiB-aligned, and ranges from 4 KiB
through 4 MiB. Lower it when publication latency matters more than write
amplification. Keep it high for throughput and batching. Pressure, the bounded
flush delay, rotation, and `drain` can flush below the threshold, so it is not
a visibility deadline.

`put` and `put_l2` intentionally do not wait for staging capacity. A larger
Region or more appropriately placed shards can absorb more of a finite burst,
but configuration does not turn immediate admission into an unbounded lossless
queue. Applications that require higher acceptance must retry with their own
deadline, bypass the cache, or split bursts with completion barriers. A
successful admission becomes visible after publication; `drain` is the barrier
for accepted writes.

### Reclaim workers

Each reclaim worker owns one Region-sized scan buffer, one reclaim I/O lane,
and worker resources. The count must be between one and the append-shard count.

Reclaim workers are a capacity-progress knob, not a direct hit-rate knob.
Increase the count only when sealed Regions accumulate, Free Regions remain
scarce, and one worker cannot recycle capacity at the write rate. More workers
consume memory and device bandwidth. Hot-record reinsertion also stages through
existing append shards, so aggressive reclaim can compete with foreground
writes and with other reinserts.

Start with one worker. After increasing it, check all of these together:

- Free, Sealed, and Reclaiming Region counts;
- foreground write rejections;
- reclaim throughput;
- successful and skipped reinserts;
- device latency and read/write bandwidth;
- final hot-set retention across repeated fresh-cache runs.

`reinsert_budget_skipped` identifies the fixed per-Region byte budget. A zero
budget-skip count with nonzero `reinsert_skipped` points instead to validation
or staging pressure; adding reclaim workers is unlikely to repair that cause.

### Managed-memory limit

Raise `managed_memory_limit_bytes` whenever another knob adds fixed resources:

- more index slots;
- more L1 capacity or shards;
- more append shards or a larger Region;
- more read, write, or reclaim workers;
- more reclaim buffers;
- bounded read waiting and the desired concurrent read buffers.

Do not mechanically raise the limit until the process fits the host. Decide
which tier buys the most hit value per byte. A read-heavy service may prefer L1
and read-buffer headroom; a high-cardinality service may prefer a lower-load L2
index; a bursty writer may prefer Region staging. These allocations compete
inside the same hard limit.

During tuning, compare `managed_memory_peak_bytes` with the configured limit.
If the peak approaches the limit, distinguish fixed-plan growth from transient
read or retained-value pressure before choosing the next knob.

### I/O engine and mode

Buffered POSIX I/O is the portable production baseline. It benefits from the
kernel page cache, so cache-owned memory metrics do not describe all memory
used by the workload. Benchmark with a dataset larger than host RAM when the
goal is device behavior rather than page-cache behavior.

Direct mode is Linux-only and requires `O_DIRECT` for aligned record I/O. It
reduces page-cache duplication but can amplify small reads and exposes aligned
I/O failures instead of silently falling back. Control, recovery, and
necessarily unaligned remainder operations remain buffered.

io_uring is feature-gated and experimental in 0.2. One configured worker means
one fixed-depth ring, not one POSIX-equivalent request slot. Retune worker
counts and managed memory rather than copying a POSIX configuration.

### Statistics

Health and managed-resource gauges are always available. Enable
`with_statistics(true)` while tuning to obtain cumulative request, index, L1,
and I/O counters. Enabled counters add relaxed atomic work on active paths, so
measure the overhead before leaving all activity statistics enabled in a
latency-critical deployment.

## Goal-oriented profiles

### Balanced starting point

- Size the index from the measured live-key count with moderate headroom,
  commonly `E = 2N` before workload validation.
- Keep Region size at 24 or 32 MiB unless objects or memory require otherwise.
- Start with four append shards, four read workers, four write workers, and one
  reclaimer.
- Use CLOCK and a nonzero L1 sized for the most valuable reusable working set.
- Keep immediate reads until the application explicitly chooses overload and
  latency semantics for waiting.

### Hit-rate oriented

- Spend index memory on `E = 4N` or, after measurement, `8N`.
- Give L1 enough bytes for the reused working set and compare S3-FIFO against
  CLOCK under scan pollution.
- Size read execution near peak concurrent L2 demand. Add a short wait only if
  its overload semantics and p99 are acceptable.
- Increase the managed-memory limit for both fixed index growth and concurrent
  read buffers.
- Keep reclaim conservative; extra workers do not inherently preserve hot
  records.

### Lowest bounded read latency

- Keep `read_io_wait_timeout` at zero.
- Provision a modest worker pool and treat pressure misses as normal fallback.
- Use L1 to remove repeatedly hot reads from L2 instead of building a long L2
  queue.
- Prefer CLOCK when its hit rate is adequate.
- Monitor successful-hit throughput separately from total request throughput;
  fast misses can otherwise make an overloaded configuration look faster.

### Memory constrained

- Use `E = N` or `2N` and accept the corresponding bounded index-eviction risk.
- Choose the smallest Region that holds the maximum record and keep append
  shards and reclaim workers low.
- Keep read waiting off and cap POSIX worker counts.
- Allocate L1 only when its measured hit savings exceed the index or transient
  read headroom it displaces.
- Leave enough dynamic memory for at least the intended concurrent read set;
  merely satisfying the open-time floor is not sufficient.

### Bursty writes

- Match append shards to independently active writers; do not scale them from
  queued tasks alone.
- Keep write workers high enough to drain those shards when the device has
  spare concurrency.
- Consider a larger Region for more fixed burst reservoir, accounting for
  `2 * append_shards * Region size` staging.
- Keep a larger flush threshold for batch throughput; lower it only when
  publication latency is the measured bottleneck.
- Preserve application retry, bypass, or batching logic. Immediate no-retry
  admission can still return overload under any bounded configuration.

### High turnover or hot-record retention

- Increase capacity first if the cache turns so quickly that useful values
  cannot survive a reasonable reuse interval.
- Use moderate Region sizes when finer eviction units matter.
- Start with one reclaimer and increase only for observed capacity lag.
- Keep foreground staging healthy so reinsertion has somewhere to write.
- Evaluate hot retention across repeated runs; scheduling and Region grouping
  make a single sample misleading.
- Use S3-FIFO for the L1 hot set, but remember that L2 heat and reinsertion are
  separate best-effort mechanisms.

### Large records

- Set the Region size above the maximum encoded key/value record.
- Expect records above the L1 charge limit to stay on L2.
- Limit append shards because each one owns two maximum-size buffers.
- Reserve managed-memory headroom for concurrent aligned L2 reads and for
  Region-backed values retained by callers.
- Validate direct-I/O read amplification if Direct mode is used.

## Diagnostic map

Use `Cache::snapshot()` for regular telemetry and
`Cache::detailed_snapshot()` for periodic diagnosis.

| Observation | Likely constraint | Tuning direction |
| --- | --- | --- |
| Rising `index.overflow_evictions` or high slot occupancy | Fixed index load | Increase `expected_entries`; this is a static-layout change |
| `l2_read_busy_misses` | Immediate read execution pressure | Increase read workers, improve L1, or deliberately enable bounded waiting |
| `l2_read_memory_misses` | No managed buffer available | Add memory headroom, reduce retained values/fixed allocations, or reduce read concurrency |
| `l2_read_overloads` with low wait time | Queue saturation | Add execution capacity or reduce L2 demand; a longer timeout alone may not help |
| `l2_read_overloads` with high wait time | Deadline or slow device | Raise the deadline only if p99 allows it; otherwise add capacity or reduce device work |
| Rising `write_buffer_rejections` | Shard-local staging or mutation pressure | Check shard count, hash skew, Region size, write progress, and application burst policy |
| Write in-flight peak equals configured capacity and slot wait rises | Write pool is limiting | Add write workers if the device has headroom |
| Many partial buffers and low per-shard traffic | Too many append shards | Reduce append shards |
| Persistently low Free Regions with growing Sealed queue | Reclaim cannot keep up | Add capacity or cautiously add reclaim workers |
| `reinsert_skipped` rises while `reinsert_budget_skipped` is zero | Reinsertion staging/validation pressure | Reduce concurrent reclaim or foreground pressure; do not increase the byte budget indirectly |
| `reinsert_budget_skipped` rises | Hot live bytes exceed the fixed reclaim allowance | Treat retention as best effort; change capacity/workload geometry rather than worker count |
| High `l1_bypasses` with useful candidates | L1 contention, slots, or byte pressure | Inspect L1 occupancy, retained bytes, shards, capacity, and oversized values |
| Managed-memory peak approaches the limit | Fixed or transient memory pressure | Rebalance index, L1, staging, workers, and read headroom |
| Unexpected cold start after configuration change | Static identity or append-shard rebind failed | Verify Region/index geometry and available Free Regions; cache loss is safe |

Always correlate cache counters with device latency, physical IOPS, filesystem
behavior, process RSS, and authoritative-backend load. Cache throughput alone
can reward configurations that merely turn work into fast misses.

## Tuning sequence

1. Record the workload envelope and choose capacity.
2. Select Region size from maximum record size, memory, and desired eviction
   granularity.
3. Size the index from maximum live keys and an explicit hit-rate/memory bias.
4. Set a managed-memory limit that fits the fixed plan plus dynamic read and
   retained-value headroom.
5. Tune L1 capacity, shards, and policy at fixed L2 geometry.
6. Tune read workers and wait semantics using hit rate, overload, and p99.
7. Tune append shards, write workers, and flush threshold using acceptance,
   publication, and device counters.
8. Increase reclaim workers only if one worker cannot maintain Free Regions.
9. Re-run after selecting Buffered versus Direct or POSIX versus io_uring; the
   same worker counts are not equivalent.
10. Validate cold and warm opens, `drain`, overload behavior, managed-memory
    peak, and final value correctness before deployment.

Change one resource family at a time and alternate baseline and candidate runs
on the same host. Use multiple fresh-cache samples for turnover and burst
tests. For storage qualification, use a dataset larger than host RAM and follow
[Validation](BENCHMARK.md).

## Hard configuration bounds

| Setting | Bound |
| --- | --- |
| Region size | Nonzero 4 KiB multiple, at most 32 MiB |
| Capacity | Exact Region multiple with more Regions than append shards |
| Index slots | At least 8; the upper bound is derived from the addressable page, mapping, and recovery-image layout; `with_expected_entries` requests two slots per entry |
| Append shards | 1 through 256 |
| Reclaim workers | 1 through append-shard count |
| POSIX read/write workers | 1 through 4,096 each |
| Read wait timeout | Zero through five seconds |
| L1 shards | 1 through 65,536 |
| Write flush threshold | 4 KiB multiple from 4 KiB through 4 MiB |
| Managed-memory limit | Nonzero, at least L1 capacity, and large enough for the validated fixed plan |

Open validates the exact geometry, platform capabilities, and memory plan and
returns a structured configuration error before the cache becomes observable.
