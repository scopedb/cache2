# cache-rs Roadmap

> 当前状态：**v1.1 — large-scale source-complete production candidate**。M0--M8 与
> 单盘大容量/大 entry 数量数据路径代码已完成；M5 目标 NVMe 吞吐/p99/profile、M6 TB 级恢复 SLA、M7 workload hit-rate/DWPD，
> 以及 M8 24--72 小时 soak、canary 和真实掉电仍待部署环境 sign-off。大小对象 + DRAM
> Hybrid 的 H0--H5 源码路径也已完成，包括 session manifest、统一 policy、默认有界
> write-back、统一 async API、Bucket NVMe I/O 与 mixed-size benchmark；目标 NVMe 矩阵、
> 长稳、DWPD、TB 级恢复和真实掉电仍待部署环境 sign-off。

## 推进原则

先正确、再并发、后榨 NVMe。每个 milestone 都必须保持版本可运行、可测试、可独立验收，不以尚未完成的后续阶段作为正确性前提。

`M0 → M1 → M2 → M3 → M4 → M5 → M6 → M7 → M8` 的代码已严格按序完成。v1.1
在 NVMe data path 和双槽增量恢复之上，加入 admission/namespace/second-chance/SSD
治理、OpenMetrics/诊断/管理工具和 miss-storm guard；checkpoint v4 在相同索引布局下
精确恢复亿级 bounded-probe 索引，索引布局改变时安全降级为额外 miss。源码测试不宣称具体硬件性能、
TB 级恢复时间、设备寿命或生产长稳结果；这些验收必须由目标环境实测签字。

发布节点：

| 节点 | Milestone | 含义 |
| --- | --- | --- |
| Internal beta | M3 / v0.4 | 核心并发架构成形，可供内部真实 workload 验证 |
| Staging | M5 / v0.6 | NVMe 主路径代码完成，并在目标硬件通过预声明性能门槛后进入预发布环境 |
| Production candidate | M8 / v1.0 | 可观测、运维和升级源码完成；环境长稳/canary 签字后方可宣告具体部署 production-ready |
| Large-scale candidate | v1.1 | 亿级 entry 数据路径与 1–8 lane 源码完成；目标容量/稳态 churn/checkpoint/profile 实测后签字 |
| Hybrid source candidate | H5 | DRAM + Bucket + Region、write-through/write-back 与运维源码完成；目标 NVMe/soak/掉电签字后进入部署 staging |

## M0 — v0.1.1：API 与正确性基线（completed）

### 目标

收紧当前同步单文件实现的公开语义，修复已知正确性和生命周期问题，为后续重构建立不可回退的行为基线。

### 主要工作

- 明确配置项的 create/reopen 契约：持久布局参数必须匹配，运行时策略参数允许调整，非法配置在触碰文件前失败。
- 统一 `Healthy`、`Closed`、`Poisoned` 下所有公开 API 的返回行为和错误优先级。
- 修复 `Some(0)` TTL 被视为永不过期、缩小 `max_key_size` 后旧 key 无法删除等失效语义。
- 让 `close` 真正结束服务并释放 writer lock，同时保持幂等。
- 为 `index_slots` 增加明确上限和 fallible 校验，避免配置触发 panic 或超界分配。
- 修正首次启用 Free Region 被统计为 reuse，并补充针对行为契约的回归测试。

### 验收

- TTL、覆盖、删除、clear、hash collision、关闭和重开均不会返回已明确失效的旧值或其他 key 的 value。
- `Some(0)` 返回 `Rejected(AlreadyExpired)`；缩限重开后旧长 key 仍可删除，flush/reopen 后保持 miss。
- `close` 后旧对象尚未 drop 时，同一路径可以重新打开；后续操作稳定返回 `Closed`。
- 所有 fallible 公开 API 在 `Closed`/`Poisoned` 状态下使用一致的错误优先级；`stats` 始终提供最后快照。
- `remove` 成功后，调整任意运行时配置并重开都不能读到旧值。
- 非法配置返回 `InvalidConfig`，不 panic、不创建或修改目标文件。
- Rust 1.85、全部行为测试和 `cargo clippy --all-targets -- -D warnings` 均通过。

## M1 — v0.2：可注入 I/O 与崩溃模型（completed）

### 目标

把文件 I/O 和故障行为从 cache 逻辑中隔离，使恢复协议能够被确定性验证，而不是只依赖正常路径测试。

### 主要工作

- 引入最小 `IoBackend` 抽象，覆盖 positioned read/write、文件长度和持久化 barrier；保留同步文件实现。
- 增加 failpoints：short write、torn write、`EIO`、`ENOSPC`、sync failure 和指定操作序号失败。
- 建立进程级 kill harness，在 record、region header、dirty/clean superblock 等关键持久化点终止并重开。
- 固化 Format V1 golden fixtures，验证编码、解码、兼容性和损坏拒绝行为。
- 冻结 Format V1 的升级与明确拒绝策略，不把未知版本当作空文件覆盖。
- 明确并实现 `Healthy`、`Poisoned`、`MissOnly` 状态转换；不可恢复设备错误可以降级为 miss，而不能返回错误 value。

### 验收

- 同一故障脚本可重复得到相同恢复结果，所有注入点只允许恢复已发布 checkpoint 或空 cache。
- 每个持久化步骤中断后，重开只允许得到正确值或 miss；损坏 tombstone、Region Header 或 Superblock 不会复活旧值。
- I/O 错误不 panic、不死锁，也不会发布错误的 clean checkpoint。
- short I/O 循环、torn metadata、sync failure 和进程终止均有行为测试覆盖。
- Format V1 golden fixtures 跨构建稳定；格式变化必须显式升级版本。
- 同步文件后端与故障包装后端通过同一套 exact-I/O、锁和恢复行为路径。

## M2 — v0.3：有界资源与背压（completed）

### 目标

在引入并发和异步 I/O 前先固定资源上限，确保过载时可预测地拒绝，而不是扩大内存、队列或延迟。

### 主要工作

- 引入固定容量 aligned buffer pool，以及有界 read/write submission queue。
- 建立统一 engine-owned logical heap budget，覆盖 index、region/recovery metadata 和全部 aligned scratch buffer；调用者对象、线程栈、allocator metadata、page cache 不在该统计域。
- 定义直接的 backpressure 规则：阻塞、拒绝或超时必须由 API/配置明确选择。
- 加入基础 admission：尺寸、TTL、当前容量和资源预算检查；暂不引入复杂频率策略。
- 暴露队列深度、buffer 使用量、分类拒绝计数和等待时间等基础指标；每次 API 返回携带精确拒绝原因。

### 验收

- 任意合法配置和过载流量下，engine charged/reserved heap 与 engine-owned queue 占用不超过声明上限。
- `Reject`/`Timeout` 下 buffer、queue 或预算满时有界返回；显式选择 `Block` 时只阻塞调用线程，不建立 engine-owned 无界队列。
- 普通写压力不能消耗 read/control 保留 gate 与 buffer；v0.3 的全局 I/O 串行化仍可能影响延迟，M3 负责消除该限制。每次拒绝都有明确结果和分类指标。
- 故障和重复过载后所有 lease/permit 都能归还；确定性压力测试无资源泄漏。异步取消语义从 M4 开始。
- 同步 v0.2 语义与恢复测试全部保持通过。

## M3 — v0.4：并发核心架构（completed）

### 目标

将全局串行状态拆为可推理的并发组件，在保持单 append 顺序的前提下支持并行读和不同 key 的并行操作。

### 主要工作

- 将 compact index 分片，固定 shard 数和每 shard 的资源预算。
- 引入 key ordering，保证同 key 的 `get`、`put`、`remove` 具有明确线性顺序。
- 支持 concurrent reads，并用 location/seqno/incarnation 再验证防止 region reuse 竞态。
- 抽出 region manager，集中管理 active/sealed/free/reclaiming 状态和 incarnation。
- 使用单 append worker 串行发布 record 与 region 转换，先保持直接算法和可恢复顺序。
- 建立并发 race、关闭 drain、region reuse 与碰撞压力测试。

### 验收

- 同 key 操作满足既定顺序，不同 key 可以并行；并发读取不会访问已复用 region 的错误内容。
- 并发 put/remove/reclaim/clear/close 不产生 stale resurrection、错误 value、死锁或资源泄漏。
- `clear`、`flush` 不与正在发布的写入产生状态穿透。
- 多线程读 workload 相比全局 mutex 版本有可重复的扩展收益。
- M0–M2 的 API、故障恢复和资源上限测试全部保持通过。

实现证据：compact index 最多分为 256 个独立 `RwLock` shard；256 个 ordering
stripe 固定同 hash 的同步顺序；`get` 持有 ReadView read guard 跨 positioned I/O，
reuse/clear 使用 write guard；所有 mutation、checkpoint 与 shutdown 经有界单 append
worker 发布。确定性 gate 测试证明两个 public `get` 同时进入 backend、并发 caller 的
record write 只来自同一非 caller worker、reuse 确实被旧 incarnation reader 阻塞，
以及 accepted put 在 clear/close 前完成 drain。M0–M2 的 crash/resource/API 测试保持
通过，Format V1 未变化。

**节点：完成后进入 internal beta。**

## M4 — v0.5：异步 I/O 引擎（completed）

### 目标

在稳定的并发和资源模型之上引入异步 I/O，同时保留可替换的兼容后端和一致语义。

### 主要工作

- 定义支持 read、write、flush、cancel 的 `IoEngine`，提供同步 positioned I/O 后端与 `io_uring` 后端。
- 提供异步 cache API，并明确 buffer 所有权、completion 生命周期和错误传播。
- 实现 cancellation、timeout、关闭 drain 与 in-flight 操作清理。
- 将 queue depth、submit/completion latency 和 I/O error 纳入指标。
- 支持批量 submit/completion，并允许设备 completion 乱序返回。
- 保持 buffered I/O 可用；本阶段不把 `O_DIRECT` 作为正确性依赖。

### 验收

- 同一后端一致性测试在 sync 和 `io_uring` 上得到相同行为。
- cancel/timeout/close 不造成 use-after-free、重复完成、预算泄漏或未完成写入被错误发布。
- `close` 能排空已接收操作并在确定的时间边界内结束。
- completion 乱序不破坏同 key 顺序；queue depth 可配置且有硬上限。
- 不支持 `io_uring` 的环境可退回同步后端运行。

### v0.5 实现结果

运行时 I/O 已统一为持有 `BufferLease` 所有权的内部 `IoEngine`。同步后端使用最多
4 个固定 worker；Linux 后端以 `io_uring 0.7.14` 批量推送 SQE、批量回收 CQE，
两者共享 queue-depth、取消、Future/阻塞 completion、统计和 shutdown 协议。
公开 `AsyncDiskCache` 使用最多 2 个 read worker、单 mutation worker、有界 ordinary
queue 和 2 个 control reserve；输入复制前先预留槽，queued cancel 会立即回收容量，
mutation 开始执行后必须返回真实提交结果。同步/异步并发 close 只选出一个物理 owner。

fatal `io_uring` 路径只在观察到 target CQE 后归还内核可能引用的 buffer；无法 fence 的
read buffer 被有界隔离。若 active write/flush 无法 fence，实例不会执行 `LOCK_UN`，并
保留同一 open-file-description，防止后续实例与旧硬件 I/O 竞争同一 inode。Format V1
未变化；Rust 1.85 的 default/no-default tests 与 clippy、Linux all-features check/clippy
全部通过。

## M5 — v0.6：NVMe 数据路径（code completed；staging hardware sign-off pending）

### 目标

完成面向 NVMe SSD 的主数据路径，减少 syscall、拷贝和非对齐 I/O，并建立稳定性能基线。

### 主要工作

- 提供 `Buffered`、`Auto` 和 `Direct` 文件 I/O 策略；`Auto` 只在系统报告
  direct capability 不可用时降级，`Direct` 要求该 capability，且对齐 direct I/O
  出错后不做 buffered retry。
- direct fd 只接收 buffer address、offset 和 length 全部 4 KiB 对齐的 runtime data
  请求；metadata、recovery、旧 Format V1 非对齐 record，以及 positive short I/O
  后的非对齐 remainder 使用 buffered compatibility descriptor。
- 每个 append worker 合并已经排队的 put prefix，上限为 128 KiB 或 64 records；
  单个更大的 record 仍作为合法 one-record batch。默认 1 个 hash lane，硬上限 2。
- format 在初始化 Region Header 前建立精确文件 extent，并在 64-bit Linux 上使用
  `posix_fallocate` 请求物理预分配。
- read/write data buffer slot 随 submission/I/O depth 扩展，分别硬限制为 32；Linux
  backing 使用 `MAP_SHARED | MAP_ANONYMOUS`，扩容期间新旧 mapping 的重叠也计入
  hard memory budget。
- 增加 batch/coalescing、direct/buffered operations/bytes、direct-active 状态，并以
  benchmark JSON 汇总 throughput、hit rate、latency、CPU、device counter 和
  write-amplification 数据。

### 验收

- [x] direct 与 buffered 路径通过相同 Format V1、恢复和故障行为，且可交叉写入、
  `flush`、重开。
- [x] direct fd 的每次 submission 都满足 4 KiB alignment；short/torn I/O 不破坏
  发布语义，compatibility descriptor 的使用可由统计观测。
- [x] 最多两个 lane 不破坏 key ordering、Region 生命周期或 clean-checkpoint 顺序。
- [x] 可复现 harness、专用 path 检查、JSON 报告，以及 `--min-ops-per-sec`、
  `--max-p99-us`、`--min-hit-percent` 非零退出验收门槛已实现。
- [ ] 在目标 NVMe 上预声明门槛并完成 required matrix；确认 4 KiB 随机读、批量
  顺序写吞吐和 p99 无明显长尾失控。
- [ ] 保存同次运行的 CPU、内存复制与设备利用率 profile，形成 staging sign-off。

### v0.6 实现结果

M5 代码和行为测试已完成，Format V1 保持兼容。`Direct` 的“required”指 direct
descriptor/capability 和对齐请求的错误语义，并不要求旧 record、metadata 或 short
remainder 的每个 byte 都经 `O_DIRECT`。验收路径和报告字段见
[`docs/NVME_BENCHMARK.md`](docs/NVME_BENCHMARK.md)。

**节点：目标 NVMe throughput/p99/profile 结果通过预声明门槛后，才完成 staging
sign-off；这项 M5 硬件验收不因 M6 代码完成而自动关闭。**

## M6 — v0.7：双 checkpoint 与渐进恢复（code completed；target-hardware SLA pending）

### 目标

把启动成本从全盘严格扫描演进为可靠的 checkpoint 加增量恢复，并支持恢复期间逐步接流量。

### 主要工作

- [x] 在原 Format V1 data extent 之后实现 directory + 双 checkpoint slot；payload
  使用流式 CRC32C，先写并 sync payload，再写并 sync 4 KiB commit header，最后发布
  与之严格配对的 clean Superblock。
- [x] checkpoint 记录 compact index、Region incarnation/状态/used/max seqno 和全局
  最大 seqno；clean 启动验证 headers 后直接装载，dirty 启动只扫描 checkpoint 后变化
  的 Region incarnation 或 tail。
- [x] standalone Region 以 admitted record bytes 节流并合并周期 checkpoint；默认阈值
  256 MiB，`0` 仅禁用周期任务，显式 `flush`、`clear`、`close` 仍发布 checkpoint；维护
  任务以排队 writer 获取 operation barrier，持续读流量不会使 checkpoint 饥饿。Hybrid
  managed Region 的 clean publication 后续由全局 driver 独占，避免跨文件 generation 脱节。
- [x] 实现 `RecoveryMode::Blocking`（默认）与 `RecoveryMode::MissOnly`；后者在后台
  恢复完成前稳定返回 miss、拒绝 mutation，并在新 clean checkpoint 成功后一次性
  原子开放全部流量。
- [x] 保持 Format V1 的 Superblock/Region/record 编码不变；checkpoint 是 data extent
  之后的兼容扩展，legacy golden fixture 的 data extent 不重写、只追加 v0.7 tail；缺失
  或不兼容时按已冻结的 full-scan/安全重建策略处理。
- [x] 增加 checkpoint write/load/fallback/error，以及 recovery regions/records/bytes、
  elapsed、completed/total、in-progress 指标；`regions_scanned` 只计实际扫描 record data
  的 Region，bytes 包含全部 Region Header 和扫描的 record bytes，completed/total 是
  单调全 Region 进度。checkpoint 保持 4 KiB 盘上对齐并使用固定 256 KiB I/O window，
  Region/recovery workspace 纳入 hard memory budget。

### 验收

- [x] 任意 checkpoint payload/header 与 durability barrier 前后 `SIGKILL`，重开只选择
  完整、CRC 有效且与 Superblock 世代/epoch/seqno 严格配对的 generation。
- [x] checkpoint + 增量扫描保持 put/remove/clear 逻辑结果；损坏或截断的 dirty
  tombstone、Region Header、slot header/payload 都不会复活旧值。
- [x] 恢复与 checkpoint 内存计入统一预算，没有按文件或 payload 大小无界增长。
- [x] `MissOnly` 恢复期间不返回未经验证的数据；完成 checkpoint 后原子切换
  `Healthy`，shutdown 可取消并排空后台恢复。
- [x] 旧 Format V1 文件可 full scan，checkpoint extension 损坏可回退或安全重建，
  未知格式仍明确拒绝，不做隐式猜测。
- [ ] 在目标大容量 cache 上预声明并验证 first-service/full-recovery SLA、恢复峰值和
  checkpoint metadata 写放大；源码行为测试不替代这项硬件验收。

### v0.7 实现结果

M6 代码已完成。9 个聚焦行为测试覆盖首次 baseline、dirty put/remove 增量 replay、
`clear` epoch barrier、双槽轮换与最新槽损坏、后台 `MissOnly` 原子开放、周期
checkpoint + close、Active tail 边界、损坏/截断 tombstone，以及 checkpoint/clear 持久化点的真实子进程
`SIGKILL/restart`。v1.0 的 checkpoint v3 还持久化 Active Region lane identity，并继续
读取 v1/v2；大容量恢复 SLA 与 M5 staging 仍分别等待目标硬件数据。

## M7 — v0.8：命中率、写放大与 SSD 治理（code completed；workload/device sign-off pending）

### 目标

在正确性和性能路径稳定后优化缓存价值与 SSD 寿命，并为多租户和设备退化提供控制面。

### 主要工作

- [x] 实现固定 64 KiB frequency table 的 `Always` / `SecondHit` admission；普通新对象
  第二次观察、大于 1 MiB 的对象第三次观察后进入，已有 key 更新不受阈值影响。
- [x] 为每个 Region 维护 used/valid bytes、basis-point valid ratio 和 second-chance bytes；
  保留严格 FIFO baseline，并实现一次性、异步、有界 reinsertion。
- [x] reinsertion queue 固定 64，单 record 上限 128 KiB，单 victim reinsertion 上限为
  payload 的 25%；前台同 key 更新、seqno/location/incarnation 再验证优先。
- [x] 增加 namespace-aware record/index/checkpoint、`get_in/put_in/remove_in`、live-byte
  capacity 和 bytes/s write quota；namespace zero 保持旧 API/Format V1 表示。
- [x] 按 foreground/reinsertion/reclaim/forced-tombstone/metadata/checkpoint 分类统计
  submitted host bytes、WA，并提供 UTC-day host-write budget 与外部 baseline。
- [x] 接受部署侧采集的 NVMe SMART/health sample，跟踪 data-units-written、spare、wear、
  media-error 增长和 critical；默认 advisory，可选只拒绝 critical 后的新 put。
- [x] checkpoint v3 保存 namespace/flags 与 Active Region lane identity，同时兼容读取
  v1/v2 并安全推断 legacy lane mapping。

### 验收

- [x] admission/reinsertion 服从 M2 内存、queue、buffer 和前台优先契约；饱和时精确拒绝，
  不建立无界后台写或前台等待。
- [x] namespace quota reservation/rollback、恢复期 over-quota 只读、异步 namespace API、
  checkpoint/reopen 隔离和 namespace-zero 兼容都有行为测试。
- [x] second-chance 的 stale/drop/complete、victim reuse fencing、多 lane reopen/tombstone 和
  Region valid-ratio 统计都有回归测试；reclaimer 不在锁内执行 reinsertion I/O。
- [x] submitted host-write 分类、daily budget rollover/baseline、SMART critical/advisory 状态
  和明确的 put-only health policy 可观测且可测试。
- [ ] 在目标 workload 上完成 `always/fifo`、`second-hit/fifo`、
  `always/second-chance`、`second-hit/second-chance` A/B，证明命中率不低于 FIFO 基线。
- [ ] 用设备容量、厂商 DWPD 和 SMART/NVMe counters 验证每日写入预算与长期 wear；源码
  计数不能替代这项设备 sign-off。

### v0.8 实现结果

M7 行为和数据结构代码完成，所有 policy table、quota reservation 与 reinsertion work
都有固定上限。默认仍是 `Always + Fifo + ObserveOnly`，避免升级时隐式改变命中或写入
行为。A/B 命令见 [`docs/NVME_BENCHMARK.md`](docs/NVME_BENCHMARK.md)；hit-rate/DWPD
结论必须由代表性 workload 和目标 SSD 签字。

## M8 — v1.0：生产化（code completed；production-environment sign-off pending）

### 目标

补齐部署、观测、诊断和升级代码及长稳验收入口，形成可安全灰度与回退的 production
candidate；具体部署通过外部验收后再签署 production。

### 主要工作

- [x] 提供 dependency-free `MetricsSnapshot` / OpenMetrics：六类公开 operation、24 个有限
  latency bucket + overflow、稳定 result/error class 和最新 32 条 lifecycle event；
  OpenTelemetry 通过 Collector Prometheus receiver 接入。
- [x] 提供 `ConfigDiagnostics`（不触碰 path）、`open_with_diagnostics`、startup/health
  snapshot，并把 recovery、I/O mode/engine、memory plan 和 readiness 明确输出。
- [x] 提供 rate + concurrency 双硬上限的 origin-fill RAII permit，冷启动/cold miss 的
  回源必须由调用方显式取得 permit，拒绝原因和 in-flight 状态进入指标。
- [x] 提供 `cachectl inspect/verify/format/reset/diagnose`：离线只读命令固定 buffer 与
  issue 上限，format 只接受 missing/empty；reset 在同一 fd/flock 内识别并重建 Format
  V1；写操作要求 `--yes` 且不作为自动故障恢复动作。
- [x] 固化 Format V1 与 checkpoint v1/v2/v3 读取策略、升级/回退、readiness、canary、
  miss-storm、safe reset 和容量调整 runbook。
- [x] 现有 failpoint、真实 `SIGKILL/restart`、ENOSPC/EIO/sync failure、unknown format、
  资源边界与管理工具损坏输入测试形成源码回归基线。

### 验收

- [x] 源码行为测试验证 API/error/state 分类、metrics label 有界、配置诊断无 path 副作用、
  origin-fill rate/concurrency 硬上限和 offline verifier 的损坏/大数输入边界。
- [x] 运维可以从 OpenMetrics/health/startup diagnostics 定位 lifecycle、队列、buffer、I/O、
  checkpoint/recovery、reclaim、WA、SMART 和 origin-fill 问题。
- [x] Format V1 升级/拒绝、checkpoint v1--v3、safe reset、canary/rollback/miss-storm
  操作契约有代码路径和文档，不把未知非空文件当空 cache 覆盖。
- [ ] 在部署环境完成 24--72 小时 mixed-workload soak、反复重启和真实掉电；确认无错误
  value、panic、死锁、泄漏或不可解释恢复。
- [ ] 目标 host 用实际 origin 限额执行 cold-start/mass-invalidation canary，验证 miss-storm
  不突破上游边界并可渐进放量/回退。
- [ ] 汇总 M5 NVMe、M6 TB recovery、M7 hit-rate/DWPD 与 M8 soak/canary 证据，达成该部署
  的 production SLO 和容量模型。

### v1.0 实现结果

M8 源码已完成，因此版本标记为 production candidate，不等同于任意硬件/业务环境已经
production-ready。完成上述外部项并保留报告后，才对具体 deployment 签署 production。
源码仓库本身不宣称这些环境结果已经通过。

## v1.1：单盘大容量、大 entry 数量与高吞吐（source completed；hardware sign-off pending）

### 已完成

- [x] index 上限提升到 256 Mi slots（32 B/slot），最多 4096 shards；新增
  `with_expected_entries` 按 80% load 直接 sizing，checkpoint 单槽支持到 16 GiB。
- [x] index visibility 加入全局 clear floor、Region generation 与 per-Region counters；
  entry 统计不扫描总 index，`clear` 不清零数 GiB slot array，Region incarnation 失效为
  O(1) counter 操作。
- [x] FIFO/second-chance Region 管理使用 `free_regions`/`sealed_regions` 有界队列；正常
  reuse 只扫描 victim record headers 并 compare-remove 精确 index identity，损坏时才走
  可观测的 full-index fallback。
- [x] `get` 移除 key-ordering HOL，read stats 原子化；ReadView 拆成 per-Region guard，
  rotation 仅等待 victim reader。
- [x] append lane 上限提升到 8，aligned read/write buffer 各支持到 128；async ordinary
  mutation 按每 lane 最多 8、全局最多 64 workers 驱动合并与跨 lane 并发，
  `flush`/`clear`/close 保持 FIFO exclusive barrier。
- [x] standalone Region 的大 index 隐式周期 checkpoint 阈值按最大 snapshot 写量的 16 倍
  扩展并合并请求；显式 interval 与 `0` 语义保持精确。managed Region 禁止自主 clean，
  由 Hybrid 显式 `flush`/`close` 统一发布全局 usage boundary。
- [x] `cache-bench --api sync|async` 与 1/2/4/8 lane、QD 1/8/32/64/128、至少
  2×capacity steady-state churn 验收流程已记录。

### 外部验收与已知边界

- [ ] 在实际最大 production 容量和至少 1 亿 live entries 下完成 prefill、2×capacity
  churn、close/checkpoint/reopen、dirty recovery 与 mixed soak；保存吞吐、p99、CPU、RSS、
  NVMe utilization、reclaim scan/fallback 和 checkpoint pause 证据。
- [ ] 验证 lane 1→4→8 的收益来自目标设备并行能力，而不是只增加 CPU/锁竞争；选取最小
  达标 lane/QD 作为部署值。
- [ ] 显式 `flush`/`clear`/`close` 仍生成完整 index checkpoint，是 O(index slots) 的
  有界暂停；v1.1 已把 payload syscall 从 4 KiB 聚合为 256 KiB，但没有消除 exclusive
  barrier。若实测不满足 checkpoint pause SLA，后续实现分段 copy-on-write/incremental
  checkpoint；本版本不以提高常量掩盖该边界。
- [ ] FIFO Region turnover 仍在全局 Region-manager State 锁内执行 header write 与 victim
  scrub，会短暂停顿其他 append lane；owner-fenced Hybrid 已把 rotation sync 推迟到 clean
  boundary，standalone 仍保留 barrier。后续改成两阶段 reservation。`SecondChance` 已把 scrub
  移到维护线程，但当前只有一个 ready Region，持续满盘写需用低水位 ready queue 消除周期性
  `ReclaimBacklog`。
- [ ] `stats()` 仍持 State 锁按 Region 数量汇总 valid ratio；百万 Region 部署应降低抓取频率，
  后续改为增量 gauge/后台采样。
- [ ] 默认 32 MiB Region 下 Format V1 容量上限略低于 64 TiB；256 Mi slots 在 80%
  load 下约支持 2.14 亿 live entries。超出任一边界需拆分多个单盘 engine 或进入多设备设计。

## H0--H5：大小对象 + Memory Hybrid

### H0：Region 大对象基线（completed）

- [x] 复用 v1.1 RegionLog 的大容量 compact index、append lanes、恢复和 NVMe I/O 路径。
- [x] 保持 Region Format V1 与 checkpoint v1--v4 兼容。

### H1：Memory 与 Bucket engine（completed reference path）

- [x] 固定 byte capacity、最多 4096 shard 的 LRU `MemoryEngine`，absolute TTL 和
  clean/dirty metadata；value 使用共享 `Arc` allocation，`get_handle` 的 L1 hit 不复制
  payload，兼容 owned 返回在 shard lock 外复制；clean victim 原地释放。
- [x] 独立 B1 `BucketCache`：固定 bucket、完整 key、CRC32C、FIFO、Bloom/known bitmap、
  per-bucket ordering、固定页池和统一内存诊断。
- [x] dirty marker 冗余发布；dirty reopen 前进 epoch 并安全清空；`clear` epoch fence
  同时落入两个 Superblock 槽。

### H2：Inclusive Hybrid coordinator（completed baseline）

- [x] `HybridCacheConfig/HybridCache` 统一 `get/get_handle/lookup/put/remove/clear/flush/close/status/stats`。
- [x] 按完整用户 key+value 大小自动路由，默认 memory-first write-back、显式 L2-first
  write-through；L2 hit 保持原始 absolute TTL promotion 到 L1，typed lookup 区分 tier。
- [x] target-first update：目标拒绝不改变旧 route；成功后再失效旧 engine。任何第二阶段
  失败会 poison Hybrid，阻止部分状态继续命中。
- [x] Hybrid 全局 barrier 与同 key ordering；两个 disk path 必须独立，组件预算纳入聚合诊断。
- [x] clean reopen 使用匹配的 lower/global checkpoint；dirty Hybrid session 允许两个 lower
  一并 safe-clear，因此跨阈值 update 不返回旧 route，以 cold start 换取稳态无 journal sync。

### H3：session fence、进程内版本与统一 policy（completed）

- [x] Hybrid manifest 持久化 format、三文件 layout identity、epoch 与全局 seqno。
- [x] Bucket/Region candidate 携带同一 `Version { epoch, seqno }`，读取双候选时按 version 选择。
- [x] open 与 flush-resume 各持久化一次 session dirty fence；稳态 put/remove/clear 使用
  process-local version，不追加 route journal、不执行逐 mutation durability sync。异常退出
  允许 safe-clear 两个 lower，以 miss 换取不复活旧 route。
- [x] 兼容 journal recovery 两遍流式校验，非空旧日志只保留 exact encoded prefix + `u32` offsets；
  `journal_capacity + 4 × floor(capacity / 96)` 为诊断和 aggregate budget 中的硬上界。
- [x] namespace capacity/write quota、admission、device write budget 上移 Hybrid Driver，不能由
  size route 绕过。
- [x] managed Region 不能独立发布 clean checkpoint；显式 `flush` 通常 drain dirty L1，存在
  volatile loss 时改为 safe-clear 全 cache，再发布匹配 lower/global clean 并重新挂 dirty fence；
  `close` 发布最终 clean boundary，`clear` 保持 dirty。
- [x] 组合级 `SIGKILL/reopen` matrix 覆盖双向 Bucket↔Region 顺序、remove、TTL、write-back
  dirty eviction、open/session fence、lower/global clean 与 dirty+empty journal safe-clear；
  恢复只允许 clean boundary 的值或 miss。

### H4：有界 write-back 与统一 Async API（completed）

- [x] 默认 DRAM write-back、可选 write-through；dirty victim 通过有界 exact-key pending
  directory detached，pending 期间 mask 旧 lower value，同 key mutation 等待时不持有 coarse
  ordering lock。lower-absent 任务限用 75% executor、压力下可 drop 为 miss；lower-candidate
  任务在完整 value 加入后 projected slot/byte 不超过 75% 时写 value，否则同步 volatile hide
  Region candidate 与 Bucket page、把最新值 drop 为 miss，不占 queue/设备 I/O；下一 flush/close
  发布 safe-empty boundary。
- [x] dirty expiry fence、flush/close drain-or-safe-empty、clear discard、取消/超时和单一
  `AsyncHybridCache` executor；
  read-side cleanup 以 CAS 动态提交，cancel 先赢无 mutation，commit 先赢返回 `TooLate` 并
  交付真实 completion。
- [x] demotion entries/bytes、输入复制、queue 与 buffer 全部纳入 Hybrid hard memory budget。
- [x] dirty L1 保存 exact pending physical charge；O_DIRECT fallback、demotion、expiry、remove
  和 write-through fallback 在 pending fence 覆盖期间按 durable receipt 结算。pressure
  invalidation 在 L1 退出前同步隐藏 lower；Bucket usage 保守保持到 safe-empty boundary，不会低估。
- [x] Bucket expired entry 在成功整页 compact 前继续占 exact physical charge；仅 durable
  removal receipt 退款，预算拒绝/提交前取消/写失败保持原 quota 与 Bloom。
- [x] `get_handle` 以共享 handle 返回 L1 value；兼容 L1 owned clone 与跨大小 Bucket/Region
  双候选按当前 record 大小动态计入 request-byte gate，最大单请求需求可诊断。

### H5：Bucket NVMe 快路径与 mixed benchmark（source completed；hardware sign-off pending）

- [x] Bucket bounded owned decode/compact；逐 entry key/value allocation 纳入每 workspace
  八页等价的保守内存预算，page-view decode 留作后续优化。
- [x] 与 Region 共用可替换 `IoEngine`、4 KiB aligned pool、`io_uring`、`O_DIRECT` 和 bounded completion。
- [x] `cache-bench hybrid` 使用按需 key 生成与有界 per-key version state（支持 1 亿 key）、
  有界并发 prefill，并覆盖同 key 小↔大跨 Bucket/Region 更新、remove、TTL；stale value
  直接使验收失败，且不会覆盖已有 cache。
- [x] software scale gate 可强制两个 disk tier 实际 I/O、write-back demotion/QD peak、
  至少 2× capacity turnover，并记录 clean close/drain 延迟；所有输入、worker、scratch
  与生成器内存均有硬界。
- [ ] 在真实目标 NVMe 上执行完整矩阵，覆盖 L1 命中率、1 亿 entry/2× turnover、
  跨阈值 update、queue depth、close drain、CPU/RSS、p99、write
  amplification 与 DWPD；保存独立 soak、thermal、power-loss 和 canary 签字。

H0--H5 使源码成为完整 Hybrid production candidate；只有目标 NVMe 矩阵、长稳、TB 级恢复、
DWPD、canary 与真实掉电证据全部通过后，具体部署才可签署 production-ready。

## 后续可选方向

在单设备 v1.1 稳定并有真实需求数据后，再评估多设备支持，包括 striping、独立 per-device ring 与健康状态、单盘故障隔离、容量重平衡、FDP、raw block device 或 SPDK。它不是 v1.1 发布前置条件。
