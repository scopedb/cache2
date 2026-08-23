# cache-rs 精简目标架构

状态：2026-08-23 architecture decision。本文是下一版 production 数据路径的权威设计；
DESIGN.md 和 README.md 中的 Memory + Bucket + Region、全局 journal/checkpoint 描述只代表
当前 legacy implementation。

## 1. 结论

cache-rs 面向 file chunk cache，典型对象为 16 KiB 到 256 KiB。目标架构收敛为：

~~~text
ChunkCache
├── MemoryCache                 DRAM L1
├── Coordinator
│   ├── get fast path
│   ├── bounded mutation lanes  唯一的 logical mutation ordering
│   │   ├── reserved foreground inbox
│   │   ├── lossy demotion inbox
│   │   └── lane-local RegionAppender / 4 MiB staging handle
│   ├── L1 meta                seqno / state / cache epoch
│   ├── L1 eviction admission
│   └── lifecycle
└── RegionCache                 唯一的 SSD L2
    ├── CompactIndex            Empty / Value / Masked
    ├── RegionManager           Active / Sealed / Free
    ├── FIFO reclaimer          首版唯一 L2 replacement
    └── IoEngine / Device       positioned I/O 或 io_uring
~~~

不再保留 BucketCache、DiskPair、大小对象双路 lookup、cross-engine version、route journal、
逐 record tombstone、dirty-tail recovery 和周期性全量 checkpoint。对这个对象区间，
BigHash 类整 bucket RMW 路径没有收益，反而增加一次路由、一个持久化域和一套恢复协议。

durability 契约也收敛为一句话：

> 普通 cache mutation 不 durable；只有显式 warm close 产生可恢复镜像。任何非 clean
> 启动、镜像损坏或配置不匹配都立即 cold start，production open 永不扫描 data file。

cache 可以丢，但仍然不能返回别的 key、校验失败的数据，或在已经观察到更新/删除后复活旧值。

## 2. CacheLib 的设计分层

CacheLib 的价值不在于把 BigHash 和 BlockCache 全部复制过来，而在于每层只拥有一个职责。
以下结论基于官方仓库 commit 278ffc74e3015608e385a87caba9f65f9d1113dd
（2026-08-22）及官方架构文档。

~~~text
CacheAllocator API
├── DRAM cache
│   ├── CacheAllocator          item 生命周期和公开 API
│   ├── AccessContainer         key → compressed pointer
│   ├── MMContainer             LRU / 2Q 等 L1 淘汰
│   └── MemoryAllocator         slab / pool / size class
├── NvmCache                    L1 ↔ L2 协调
│   ├── promotion / demotion
│   ├── NvmClean 状态
│   ├── 并发 token / tombstone
│   └── L1 victim admission
└── Navy
    ├── Driver                  device admission、资源上限、同 key scheduler
    ├── EnginePair
    │   ├── BigHash             小于 device block 的对象
    │   └── BlockCache          region-based 大对象
    └── Device                  对齐、I/O backend、RAID/FDP/统计
~~~

| 层 | 负责 | 不负责 |
| --- | --- | --- |
| DRAM allocator / policy | L1 容量和淘汰候选 | SSD 队列和写预算 |
| NvmCache | promotion、demotion、L1/L2 状态协调 | Region reclaim |
| Nvm admission | L1 victim 是否值得进入 L2 | device QD |
| Navy Driver admission | SSD write budget 和 outstanding resource | L1 是否接纳对象 |
| JobScheduler | 同 key 请求排序 | replacement policy |
| BlockCache | index、region append、reclaim | DRAM item 生命周期 |
| Device | 物理 I/O | cache policy |

这解释了当前 cache-rs 的一个结构性问题：L1 admission、L2 admission、Hybrid request gate
和 Region queue 被绑成一条链，L2 饱和会反向阻塞 L1。精简后两层 admission 独立：

- put 能进入 L1，不以预留 L2 容量为前提；
- 通常只有 L1 victim 才尝试 L2 admission；显式 L1-bypass 对象可以直接进入 L2 lane；
- foreground inbox 有保留容量和优先级；demotion inbox 满时丢 victim 并计数；
- RegionCache 再独立执行 device write-budget admission。

官方说明和实现：

- [RAM cache design](https://cachelib.org/docs/Cache_Library_Architecture_Guide/ram_cache_design/)
- [Hybrid cache](https://cachelib.org/docs/Cache_Library_Architecture_Guide/hybrid_cache/)
- [Navy overview](https://cachelib.org/docs/Cache_Library_Architecture_Guide/navy_overview/)
- [BlockCache insert](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/block_cache/BlockCache.cpp#L282-L336)
- [Driver admission](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/driver/Driver.cpp#L134-L216)

## 3. CacheLib 实际如何处理 durability

CacheLib 没有把 Navy 当作 crash-durable storage。它把运行期性能和 warm restart 分成两个
互不混淆的契约。

### 3.1 普通写

BlockCache insert 把 record 复制到 Active Region 的内存 buffer，更新内存 index 后即可
成功。Region 满或 flush 时才形成顺序设备写；普通 insert 不执行 per-entry fsync。

### 3.2 clean shutdown

~~~text
停止新流量
  → drain pending jobs
  → seal/flush Active Regions
  → flush device
  → persist Region metadata 和 index
  → 最后写 safeShutDown = true
~~~

CacheLib 明确区分：

- drain：等待已提交任务完成；
- flush：drain 后把数据刷到 device；
- persist：保存下次启动所需的恢复 metadata。

源码见 [AbstractCache](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/AbstractCache.h#L116-L130)、
[NvmCache shutdown](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/allocator/nvmcache/NvmCache.h#L1705-L1726)
和 [CacheAllocator shutdown](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/allocator/CacheAllocator.h#L6009-L6027)。

这只证明 clean process restart 边界。CacheLib 当前 safe marker/metadata 路径没有完整的
power-loss 原子 fsync 协议，官方也不承诺掉电恢复。本文后续定义的 image fdatasync、
directory fsync 和 state-last 发布是 cache-rs 自己更强、需要独立 failpoint 验证的协议。

### 3.3 dirty restart

CacheLib 启动后立即清掉上一代 clean 状态。上一次不是 clean shutdown 时，
shouldStartFresh 直接成立，Navy reset 后以空 cache 服务，不做 tail replay 或全盘扫描。
恢复失败也 reset；恢复成功后旧 metadata 立即失效，不能在下一次 crash 后重复使用。

源码见 [NvmCacheState](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/allocator/NvmCacheState.cpp#L74-L167)
和 [Driver recover](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/driver/Driver.cpp#L264-L303)。

因此 CacheLib 不需要数据库才需要的 WAL、逐写 sync、crash redo/undo 和 data scan。
这是 cache-rs 应采用的 durability 边界。

### 3.4 DRAM 层也只做 clean attach

CacheLib 的 DRAM persistence 是 safe shutdown 后重新 attach shared memory，并不是从 SSD
重建 L1。若 DRAM shared memory 没有保留，配置可以同时丢弃 NVM cache，避免只恢复 L2 后
暴露与旧 L1 状态不协调的值。cache-rs 不要求保留 L1；它通过“新 put 先 invalidate L2”
保证 clean close 丢弃 resident dirty L1 后只产生 miss。

官方说明见 [Cache persistence](https://cachelib.org/docs/Cache_Library_User_Guides/Cache_persistence/)。

## 4. CacheLib 并未自动解决 10M entry recovery

BlockCache 有两种 index persistence，差别很大。

### SparseMapIndex

shutdown 遍历所有 entry 序列化；startup 再逐 entry deserialize 和 try_emplace。
累计时间、serialized metadata 和最终 rebuilt index 为 O(live entries)；它按 bucket 复用
decode buffer，所以峰值临时 decode memory 不是 O(live entries)。10M entry 的时间 cliff
仍然存在。

源码见 [SparseMapIndex persist/recover](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/block_cache/SparseMapIndex.cpp#L338-L423)。

### FixedSizeIndex

固定容量 index 直接放在 persistent shared memory。persist 对 index 是 no-op；recover 只校验
小配置、attach SHM 并重设数组指针，不遍历 entry。因此 clean recovery 与 live-entry count
无关，总体为 O(region count + fixed index shard count)。

源码见 [FixedSizeIndex recovery](https://github.com/facebook/CacheLib/blob/278ffc74e3015608e385a87caba9f65f9d1113dd/cachelib/navy/block_cache/FixedSizeIndex.cpp#L295-L339)。

我们借 FixedSizeIndex 的“恢复物理布局，不重建逻辑 entry”思路，但不要求部署必须保留
CacheLib shared memory segment。

## 5. cache-rs 的恢复镜像

### 5.1 运行时 index

保留当前字段模型，但不能把现有 Rust Slot/Vec 当作盘上格式。新增独立 Index Image V1：

~~~text
hash64 | packed_location64 | seqno64 | namespace32 | flags32
~~~

- 固定 32-byte POD layout、明确 little-endian 和支持的 host architecture，并做 compile-time
  size/alignment 检查；
- 引入 IndexStorage::Anonymous 和 IndexStorage::MmapPrivate；shard 只是同一 storage 上的
  offset range，不再各自持有不可映射的 Vec；
- 每个 4 KiB index page 只属于一个 shard，shard range 必须 page-aligned，避免 first-touch
  validation 与 COW mutation 跨锁竞争；
- 固定容量、固定 shard、bounded probe，不 rehash；
- flags/location 明确定义 Empty、Value 和 Masked(seqno) 三态；
- 每 shard 的 physical live/deleted/masked count 和 slot range 存在小目录中，locks 不进 image；
- image writer 清除 VOLATILE、PENDING、hit hint 等 process-local flags；
- full key 仍保存在 Region record，读取时必须验证；
- record location、Region incarnation、seqno、header CRC 和 payload CRC 都必须匹配；
- hash collision 或损坏只允许造成 miss，不能返回错误 value。

fresh/dirty start 使用零页懒分配的 anonymous mapping。clean start 将恢复镜像的 slot 区域
以 writable MAP_PRIVATE 直接映射为运行时 index：

- open 不 decode、不 insert、不分配第二份全量 index；
- mutation 触发正常 copy-on-write，只写 DRAM/private page；
- 内核不会把运行时随机 index mutation 回写到恢复镜像；
- 第一次访问的 page 可以按需 fault，后台 prefetch 只是优化，不阻塞开放流量。

当前 foundation 已使用单一 mapping owner 和 canonical page-balanced shard ranges：每个 4 KiB
index page 只属于一个 range，range 分别加锁并维护 physical stats，但共享 page validation 与
sticky image-health。这样既不会在 16 KiB host page 上产生重叠 MAP_PRIVATE COW 分叉，也能在
任一 shard 首次发现损坏时 O(1) 拒绝整张 image。production hash operations 和 RegionManager
仍需在这组 range views 上完成接线。

不采用 Base + Delta，也不采用运行期 WAL。它们只有在要求“异常退出仍增量恢复”或
“在线持续发布 checkpoint”时才有必要，而这两个都不是当前 cache 契约。

### 5.2 recovery image

单个 sidecar image 即可，内容为：

~~~text
4 KiB header：cache/data identity + config fingerprint + exact section directory
4 KiB self-checking index pages
  └── 64 B page header/CRC + 126 × 32 B IndexSlotV1
mandatory Region metadata pages
  ├── cache epoch / clear floor / max seqno
  ├── per-shard physical counts / slot ranges
  ├── Region table + FIFO order + per-region accounting
  └── global admission accounting
~~~

V1 物理顺序固定为 header → index pages → Region metadata pages，不能留 gap 或 trailing
bytes。Region metadata 是 CLEAN image 的必需部分；只有 index 的临时镜像不得编码、发布或
恢复。当前 crate-private recovery vertical slice 已能冻结、发布并 mmap 恢复完整 image；
production Region appender/manager 尚未接入该 seam，因此还不是对外数据路径。

Region table 只与 Region 数量有关。以 100 GiB、32 MiB Region 计算约 3,200 项。
slot array 保留物理位置，恢复时不重新 hash。per-region accounting 至少保存 state、
incarnation、created seqno、durable used offset、record count、logical live bytes 和
value bytes，使 region reuse 与全局容量无需扫描 index 重建。shard directory 保存 physical
occupancy，而不只是逻辑 live count。

首版只支持单 namespace、FIFO reclaim，不恢复 second chance、per-namespace quota 或
reinsertion accounting；这些策略状态可以 cold reset，不能为了保留它们重新引入 O(entries)
recovery。以后加入 namespace 时必须使用有硬上限的独立 usage table。任何会影响
correctness、capacity admission 或 Region reuse 的值都必须来自受校验的
O(region + configured namespaces + shards) metadata；纯统计可以 reset。open 禁止通过
slot walk 校验或重算这些状态。

对于最坏 16 KiB chunk：

- 100 GiB 最多约 6.55M live entries；
- 80% index load 需要约 8.19M slots；
- 32 byte/slot 的 raw slots 约 250 MiB，计入 page header 后约 254 MiB。

这部分成本被移动到显式 warm close 的顺序写，而不是每次启动的随机分配和逐 entry rebuild。
open 只立即验证小 header 和 O(region + shards) metadata；每个 self-checking index page 必须
在首次 lookup/mutation 前、持所属 shard lock 懒校验。验证状态使用 bounded bitmap。任一 page
失败就递增 cache epoch 并 safe-clear 整个 L2，避免 page-local 丢失与已恢复 accounting
不一致；reset 失败才降级 MemoryOnly。MAP_PRIVATE page 一旦验证后可以 COW mutation，不再与
旧 CRC 比较；warm close 从冻结后的虚拟 view 重新生成每页 CRC。不能为了验证完整 image 而在
开放流量前顺序扫描 slots。

### 5.3 clean marker

一个很小的双槽 state file 保存单调 generation、state、cache UUID、data/image identity、
config fingerprint 和 CRC：

~~~text
EMPTY | RUNNING | CLEAN
~~~

open 顺序：

~~~text
获取独占锁
  → 读取最新有效 state
  → CLEAN 且 cache UUID、data/image identity、config 完全匹配：MAP_PRIVATE 映射 index
  → 否则：先使 state 失效，再 truncate/reinitialize data extent，使用 anonymous empty index
  → 将两个 state 槽都改写为递增 generation 的 RUNNING，再一次 fdatasync
  → RUNNING barrier 失败则 abort open，绝不开放或 mutate recovered mapping
  → 开放流量
~~~

双槽总是选择 CRC 有效且 generation 最大的 state。open 不能只在另一槽写 RUNNING 后保留
旧 CLEAN：否则新 RUNNING 页后续损坏时，latest-valid 会回退到已被当前 session 复用过
data Region 的旧 CLEAN。因此两个 RUNNING 页必须在同一个开放流量的 durable barrier 前写完；
warm close 只在其中一槽发布新 CLEAN，另一槽保留 RUNNING，使 CLEAN 损坏时必然 cold start。
CLEAN 还必须绑定 Data Superblock
generation、hash seed 和 data file length；仅配置相同不足以恢复。

warm close 顺序：

~~~text
关闭 public-op gate，并等待已进入 API 的请求退出
  → 停止并 join reclaimer、promotion 和所有 background producer
  → L1 eviction 切换为 Drop，丢弃 resident dirty value
  → drain foreground/demotion lanes
  → seal 所有 staging buffer
  → 等待所有 data completion
  → 将已无旧 demotion 的 Masked 规范化为 Deleted
  → 一次性冻结 index、Region/FIFO、epoch/floor、max seqno 和 accounting
  → fdatasync data
  → 从 frozen virtual view 顺序编码 recovery.next，并清除 process-local flags
  → 不 copy/reflink 旧 backing image
  → fdatasync image，rename，sync directory
  → 最后写入并 fdatasync CLEAN state
  → 释放锁
~~~

在 CLEAN marker 写入前任一步骤失败都保留 RUNNING；下次启动直接 empty，不尝试修复半成品。
最终 CLEAN 全页写已完成但 state fdatasync 报错时，close 返回错误，下一次允许得到 fully-safe
CLEAN 或 empty（sync 错误无法证明此前的完整页一定未落盘），但绝不能选择未完成的 image。

### 5.4 唯一格式 authority

只复用现有 Record Header 和 Region Header 的 encoding，不完整继承旧 Data Format V1
Superblock。旧 Superblock 的 clean/checkpoint 字段绑定 V1–V4 checkpoint 协议，与 sidecar
state 冲突。

新 profile 使用 Data Superblock V2，format 时生成不可复用 cache UUID，并保存 device
geometry、hash seed 和 superblock generation；它不再包含 session clean/dirty bit，也不在
每次运行期 mutation 时重写。同一 identity 同时写入 recovery image 和
state。RUNNING/CLEAN state file 是 startup 的唯一 recovery authority，recovery image 的
Region table 是 clean open 的唯一 Region runtime state。format/reset 必须先使旧 CLEAN
不可用，再生成新 identity。旧 V1 文件首次由 V2 打开时直接 cold reset，不做在线 metadata
升级或旧 recovery fallback。

Data、state、image 使用不含版本号的稳定 family magic；版本只编码在固定 envelope 字段中。
三个文件必须位于同一目录，使 image rename 后的一次 parent-directory fsync 同时固定首次创建的
data/state 目录项；recovery temporary path 也必须与三者互异。
magic 匹配且整页 CRC 有效的未知版本必须拒绝打开，不能静默 downgrade；CRC 无效的未知
version byte 按 torn/corrupt cache 冷启动。只要 IndexSlotV1 继续使用 PackedLocation，
geometry 就必须限制在其 region-id 和 region-offset 位宽内。

## 6. L1/L2 协调语义

Coordinator 为每个 L1 item 保存轻量 metadata：

~~~text
logical_seqno
state = Dirty | L2Resident { location, seqno }
cache_epoch
~~~

用户 value 仍是独立 immutable Arc，metadata 不再编码进 value envelope。新 value 的分配、
复制和尺寸 admission 在进入 mutation lane 前完成；lane 内 commit 必须是不会因内存分配
失败而中止的短操作。logical seqno 只能由同 key lane 在 commit 时分配，不能由 caller
预分配；L1 victim 原样携带创建它的 seqno。

### put

典型 chunk 先进入 L1。put 成功表示当前进程可见，不表示已经写入 SSD，也不表示重启可恢复。

同 key mutation lane 在发布新 L1 value 前先使旧 L2 index entry 不可见。新 L1 entry 标记为
dirty；旧 SSD record 不擦除，只是不可达。这样即使 warm close 选择丢弃 resident dirty L1，
恢复后也只会 miss，不会重新暴露旧值。

lane 内 invalidate 先在 CompactIndex 安装带 seqno 的 transient Masked slot，而不是立刻
丢掉版本信息。它拒绝随后迟到的旧 demotion。Mask 只存在于 DRAM index，不是 data-file
tombstone，也不产生设备 I/O。

Masked 不能只凭 queue watermark 退休，因为可能存在“victim 已摘下、尚未 try_send”的 producer。
每个 lane 因此维护固定成本的 demotion producer gate/count：

- eviction 在摘下 dirty victim 前先 try_acquire 对应 lane producer guard；
- guard 不可得时直接 drop victim，不等待；
- producer 在 try_send 或明确 drop 后释放 guard；
- 退休 Masked 时短暂关闭 demotion producer gate，新 victim 直接 drop，等待旧 guard 归零，
  再 drain 该 lane 已登记的旧 demotion；
- 只有完成这次 producer quiescence 后，Masked 才能降为普通 Deleted 并复用。

普通 hash collision、probe-window replacement 和别的 key insertion 都不能覆盖 Masked。
只有同 hash/key 的更新且 seqno 更新，或完成上述 quiescence 后的退休可以改变它；probe
window 全为 Masked 时 L2 insert 必须 reject/drop。

这是每 lane 一个有界 counter/gate，不是 per-key pending directory。warm close 的
stop producers → drain lanes → normalize masks 顺序天然满足同一条件。

### promotion

L2 read 和 record 校验完全在 lane 外。promotion commit 才投递到同 key foreground inbox，
并携带 expected index location、expected seqno 和 cache epoch；三者仍匹配且 L1 没有更新值时
才插入共享 immutable value，否则只跳过 promotion。未经修改的 clean L1 victim 无需再次写
SSD。Region 后续若已回收该 L2 copy，最多让 victim 变成 miss，不影响正确性。

### demotion

只有 dirty L1 victim 才尝试 best-effort demotion：

- 直接 try_send 到该 key lane 的 lossy demotion inbox；
- queue 满、write budget 用尽或 L2 降级时直接丢弃；
- 不允许回退为前台同步 SSD I/O；
- demotion 只能 try_reserve staging/write budget，不能在 lane 内等待 device completion；
- accepted demotion 在 Active Region buffer 内可读，Region seal 后形成大块顺序写。

mutation 使用单调 seqno。index publish 只接受最新 seqno，所以旧 victim 即使与新 put 交错，
也不能覆盖较新的 invalidation/value。这个 seqno 是 engine 内部 ordering，不再编码进
Hybrid value envelope，也不需要 route journal。

foreground inbox 与 demotion inbox 由同一个 lane executor 消费，前者有保留容量并优先。
这不是两层 executor：只有 lane 是 logical ordering owner，RegionCache 不再拥有独立 mutation
queue 或 key ordering。显式 L1-bypass put 无法立即取得 L2 资源时返回明确 reject。

### remove

remove 删除 L1，并在 L2 index 安装 transient Masked slot，不写 data tombstone。运行期旧
record 不可达；所有较旧 demotion 被拒绝后，mask 可降为普通 deleted slot。clean image
保存删除后的 index；dirty restart 本来就是 empty，因此没有旧值可被 recovery replay。

### clear

clear 在所有 mutation lanes 上建立短 barrier，然后交换为空的 L1 和 anonymous index mapping，
先递增 global cache epoch/clear floor，再逻辑重置 RegionManager。所有 staging job、I/O
submission 和 completion 都携带 epoch；旧 epoch completion 只能释放资源，不能 publish index。
仍有旧 I/O 的 Region generation 必须 quarantine，直到 completion/cancel 后才能复用，避免晚到
write 覆盖新 Region。旧 mapping 等现有 reader 退出后释放；不逐 slot 清零，也不擦 data file。
旧 epoch 的 read 可以按线性化顺序完成，但返回前必须复核 mapping epoch，且不能向新 L1
promotion。epoch/floor 进入 recovery image；当前 session 保持 RUNNING，后续 warm close 才会
发布空 recovery image。

### reclaim

reclaimer 是 logical ordering 的唯一例外，但只能执行物理条件更新：

- replace_if_match(hash, old_location, old_seqno, new_location)；
- remove_if_match(hash, old_location, old_seqno)。

CAS 条件失败就丢弃该副本；reclaimer 永远不能从 Empty 或 Masked 安装 Value，因此不能复活
已更新/删除的 key，也不需要进入 foreground mutation inbox。

### get

~~~text
L1 lookup
  → miss: snapshot L2 index location
  → read Active Region buffer 或 device
  → 校验 Region incarnation、record seqno、full key 和 CRC
  → revalidate index location，并确认 L1 没有更新值
  → return hit 或 miss
  → promotion side effect 条件提交到同 key lane
~~~

读 I/O 不进入 mutation lane，也不在任何 ordering lock 内等待 NVMe I/O。只有不影响本次
返回值的 promotion commit 进入 lane；inbox 满时直接跳过 promotion。

## 7. API 生命周期

公开语义必须区分以下操作：

| 操作 | 承诺 |
| --- | --- |
| put | 当前进程可见；cache value 可丢 |
| drain | 已接受 mutation 完成或明确被丢弃 |
| flush | drain，seal staging，等待 device flush；不发布 recovery image |
| close Fast | 停止并释放资源；下次允许 cold start |
| close Warm | 执行 clean recovery-image 协议；成功后下次可 warm start |

旧 close/flush API 的兼容策略在实现阶段决定，但内部不再让 flush 隐式做 O(index slots)
checkpoint，也不在 close 时强制 demote 全部 L1。

运行状态收敛为：

~~~text
Ready      L1 + L2 正常
MemoryOnly L2 I/O/metadata 不健康，L1 继续服务
Closed     不再接受请求
~~~

普通设备故障默认降级 MemoryOnly，而不是 poison 整个 cache。恢复镜像错误只导致 cold start。

## 8. 保留、合并、退出

| 当前组件 | 决策 |
| --- | --- |
| MemoryEngine / Arc value | 保留，移除 HybridVersion 持久化耦合 |
| V1 Record/Region Header encoding | 保留 full key、incarnation、seqno、CRC |
| V1 Superblock clean/checkpoint | 退出；Data Superblock V2 + sidecar state 成为唯一 authority |
| CompactIndex | 保留 hash/probe 算法；新增稳定 IndexSlotV1 和 mmap-backed storage |
| RegionManager / reader pin / reclaim | 保留 |
| IoBackend / IoEngine / aligned pools | 保留 |
| region_staging / write_batch / append workers | 合并为唯一 mutation/append lanes |
| resource budget / metrics / policy | 保留一份，不能拥有正确性状态 |
| BucketCache / DiskPair / DiskRoute | 退出 production path |
| hybrid journal / global manifest / value envelope | 删除 |
| PendingWriteDirectory | 由 lane producer gate + Masked slot + seqno/epoch revalidation 取代 |
| 独立 write-back executor | 合入 mutation lanes |
| AsyncHybrid + AsyncDisk 双 executor | 合并为 native async ChunkCache facade |
| checkpoint V1–V4 / dirty-tail recovery | 退出 runtime；只留离线兼容工具直到旧格式退役 |
| ManagedPolicyHooks / owner_dirty / delegated_policy | 退出 Region runtime |
| DiskCache RecoveryMode / checkpoint workers / MissOnly | 退出新 profile |
| DiskCache 内建 async handle | 退出，由 ChunkCache native async API 取代 |
| checkpoint namespace callbacks / diagnostics | 删除或改为 warm-image 指标 |
| legacy management/tests/bench enums | 只保留明确的 inspect/reset 兼容面，其余随旧路径删除 |

所有 I/O mode 都必须经过同一 staging/append 路径。O_DIRECT 只改变 buffer alignment 和
submission，不得绕过 4 MiB staging 回到 128 KiB/64-record 小 batch。

## 9. 不变量

1. 同 key logical mutation 只由一个 hash-selected lane 排序。
2. L1 admission 不等待或预留 L2 资源。
3. L1 eviction 永不执行同步设备 I/O。
4. Coordinator lane 是唯一 logical ordering 和 command-inbox owner；RegionCache 只拥有
   physical buffer/device budget 与 bounded I/O submission，不另建 mutation queue。
5. RUNNING 必须在开放流量前持久化；CLEAN 只能在 data 和 image 完成后发布。
6. dirty open、配置不匹配和 recovery error 都 safe-empty，不扫描 data 或 entry；初始化复杂度
   最多为 O(region count + fixed shard count)。
7. clean open 不执行 per-entry decode/insert，复杂度为 mmap +
   O(region count + fixed shard count + configured namespace count)。
8. get 返回前验证 full key、record checksum 和 index location。
9. remove/update 后旧 record 最多占空间，不能重新可见。
10. policy、metrics 和管理工具不能参与 correctness ownership。
11. transient Masked slot 只有在关闭对应 producer gate、旧 producer guard 归零且旧 demotion
    已 drain 后才能复用。
12. reclaimer 只能 conditional replace/remove 已匹配的 Value，不能覆盖 Empty 或 Masked。
13. memory budget 始终按完整 slot image 的逻辑大小预留；不能把尚未 fault 的 MAP_PRIVATE
    page 借给 L1，OS page cache 只可视为 reclaimable overhead。
14. recovered slot page 必须先验证再 lookup/mutate；校验失败 safe-clear 整个 L2，不能带着
    不一致的 Region/accounting 状态继续部分服务。
15. 两个 state 槽的 RUNNING write/fdatasync 失败必须 abort open；开放流量前不得留下可回退的
    旧 CLEAN，旧 image 绝不能被已开始 mutation 的新进程重复使用。
16. CLEAN image 必须来自同一个 frozen view，覆盖 index、Region/FIFO、epoch/floor、seqno
    和 admission accounting。
17. state、image 和 Data Superblock 必须绑定同一不可复用 cache UUID 与 data identity。
18. clear 后的旧 I/O completion 只能释放资源；Region generation 在旧 write 结束前不可复用。
19. Masked 是不可被 collision/replacement 淘汰的 visibility fence；全 Masked probe window
    必须拒绝插入。

## 10. 实施顺序

### A. 冻结边界并抽出 RegionStore V2

- 把当前三层 Hybrid 标记 legacy，不再增加功能；
- 冻结上述 put/flush/Fast close/Warm close 契约；
- 先抽出独立 RegionStore::open_v2 和 IndexStorage，不包装现有 DiskCache::open；
- state 判定必须发生在全量 index allocation 之前；
- 引入 Data Superblock V2，切断 owner_dirty、旧 checkpoint 和 recovery fallback。

### B. 替换 index/recovery

- 定义稳定的 Index Image V1 和 Anonymous/MmapPrivate storage；
- 实现 RUNNING/CLEAN state、per-page lazy CRC 和 recovery image；
- clean open 直接 MAP_PRIVATE，dirty open anonymous empty；
- production 删除 data scan、delta replay 和周期 checkpoint。

### C. 合并 Region steady-state 数据路径

- buffered、O_DIRECT、sync 和 io_uring 共用 4 MiB staging；
- RegionAppender 不再经过独立 mutation worker；
- reclaim 只做 conditional replace/remove；
- 所有 physical buffer、QD 和 write budget 只保留一份。

### D. 接入 ChunkCache coordinator

- 新增 Memory + Region only 的 production profile；
- 只保留一套 bounded mutation lane executors，每 lane 使用 foreground reserved inbox 和
  lossy demotion inbox；
- demotion 直接进入对应 lane-local RegionAppender；
- 实现 L1 metadata、Masked fence、conditional promotion 和 cache epoch；
- 实现 per-lane demotion producer guard/quiescence；
- 删除顶层 L2 reservation、同步 demotion fallback 和双 async executor。

### E. 删除 legacy

- 删除 Bucket、DiskPair、journal、global checkpoint 及其配置/指标/测试面；
- 旧盘上 metadata 只支持 cachectl inspect/reset，不做复杂在线升级；

### 验收

- 10M、100M slot image 的 clean open 不 decode entry；
- dirty open 不随 entry count 增长，也不读取 data extent；只允许
  O(region count + fixed shard count) 初始化；
- 100 GiB fixed-size 16/64/256 KiB 持续轮转正确；
- 再引入 16–256 KiB time-local mixed workload；
- steady state 除显式 flush/Warm close 外没有 fsync/msync；
- L2 饱和或失效不阻塞 L1 hit/put；
- O_DIRECT 路径能形成 MiB 级顺序 submission。

只有在这条精简路径完成并取得 100 GiB 数据后，才讨论周期 recovery snapshot、
Base + Delta、dirty recovery 或更复杂 admission。它们不是当前 production-ready 的前置条件。
