# HPC Batch Scheduler（本地内存态批作业调度与资源核算）

一个不依赖外部集群的本地批作业调度器：用互斥锁保护的内存节点表与队列表
模拟集群，目标是在**并发提交、资源不足、节点故障与计划内节点维护**四类情形下，
让调度结论**一致、可复现、可解释**，并保证资源账目始终守恒。

## 目录结构

```
scheduler/
  types.go       资源、作业、节点（含维护状态）、队列等核心类型与状态枚举
  scheduler.go   调度器：提交/排队/取消/完成/故障恢复/计划维护/公平调度主循环
  fairness.go    加权轮转（WRR）与作业老化（aging）
  ledger.go      资源流水账目（ALLOCATE/RELEASE/EVICT/MIGRATE/GROUP_REQUEUE/…）与守恒校验
  errors.go      哨兵错误
  *_test.go      并发、作业组、故障回收、计划维护、公平性、可复现性、账目测试
cmd/hpcsim/      可运行演示（main.go）
```

## 资源模型

- 每个作业声明一份 `Resources{CPU, MemoryMB, GPU}`，三维独立计算，
  放置要求在**单一节点**上逐维满足（不做跨节点切分）。
- 每个节点维护：
  - `Total`：物理总量（注册时确定）；
  - `available`：当前未分配量；
  - `jobs`：节点上运行作业的占用量表。
- 不变式（任何状态变更后都必须成立）：

  ```
  对每个节点：sum(节点上作业占用) + available == Total
  全集群：  不存在负数分配；任一运行作业恰好出现在一个节点上
  ```

  `Scheduler.CheckConservation()` 会同时核对账目侧（Ledger 累计的已分配）
  与节点侧（available、节点作业表），任一侧对不上立即返回错误。

## 判定结论与可区分的原因

提交结论是 `SubmitResult{Accepted, State, Reason, Detail}`，拒绝与排队
使用不同编码，从名字即可区分：

| 情形 | Accepted | State | Reason |
|---|---|---|---|
| 直接被公平调度器放置 | true | `RUNNING` | `OK` |
| 资源暂时不足，保持排队 | true | `QUEUED` | `QUEUED_INSUFFICIENT_RESOURCES` |
| 作业组当前无法整体满足，保持排队 | true | `QUEUED` | `QUEUED_GROUP_ATOMIC` |
| 资源声明非法（负数/全零/无 ID） | false | `REJECTED` | `REJECT_INVALID_REQUEST` |
| 超过所有节点容量，永远不可调度 | false | `REJECTED` | `REJECT_INFEASIBLE` |
| 作业 ID 重复 | false | `REJECTED` | `REJECT_DUPLICATE_ID` |
| 队列不存在 | false | `REJECTED` | `REJECT_NO_QUEUE` |
| 队列在途作业达到 `MaxJobs` | false | `REJECTED` | `REJECT_QUEUE_FULL` |
| 组成员组 ID 不一致等 | false | `REJECTED` | `REJECT_GROUP_MISMATCH` |

关键区分：**“排队”是被接受但暂时放不下**（资源归还后会再调度），
**“拒绝”是根本不会排队**（非法或永远不可行）。

## 并发与一致性

- 调度器只有一把 `sync.Mutex`，节点表、队列表、作业表、组表、账目更新
  全部在同一临界区内完成，因此“同一份资源在同一时刻被重复分配”不可能发生。
  计划维护/取消维护与并发提交、完成、取消、故障走的是同一把锁与同一条
  `scheduleLocked` 路径（维护迁移、整组重排队也在其中原子落地）。
- 分配（ALLOCATE）与回收（RELEASE/EVICT/MIGRATE/GROUP_REQUEUE）成对写入
  Ledger，取消、完成、故障驱逐、维护迁移、整组中断都经 `releaseLocked`
  同一路径，被中断作业的占用必然被释放，不会泄漏。
- Ledger 自身也带锁并有防御性快照（`Events()` 返回副本），可在调度进行中
  被巡检线程并发调用 `CheckConservation()`（测试 `TestConcurrentSubmitCancelComplete`
  专门在并发提交通道、完成/取消通道之外再加一条持续核对守恒的 goroutine）。

## 作业组（整体调度）

- `SubmitGroup([]Job)`：所有成员必须声明同一个非空 `GroupID`。
- **整组原子校验**：任一成员非法 → 整组拒绝，不登记任何成员、不留账目占用。
- **整组原子放置**：所有成员先进入待调度视图，调度器在一份“临时可用视图”上
  为每个成员 best-fit 预留；任何一个成员放不下就整组保持排队
  （`QUEUED_GROUP_ATOMIC`），绝不先放一部分。
- 组成员可分散到不同节点，放置在同一个调度决策内同时落地。
- 单条 `Submit` 携带 `GroupID` 时视为“单成员原子组”；已通过 `SubmitGroup`
  提交的组不允许再混入新成员（返回 `REJECT_GROUP_MISMATCH`）。
- **中断一致性**：维护迁移与节点故障只要打断组内任一成员，整组（含健康节点
  上的成员）立即一起重排队，随后仍按原子放置整组裁决（详见“计划维护下线”）。

## 调度与公平策略

1. **统一入口**：所有作业（包括提交时立即可运行的）都先进入队列的待调度
   视图，再由公平调度器裁决，避免“逐条提交即逐条占满”绕过多队列公平。
2. **队列间：加权轮转（Weighted Round-Robin）**
   - 每个队列有 `virtual` 虚位与权重 `Weight`；每服务一个作业，虚位前进
     `stride = floor(总权重 / 队列权重)`，权重越大跨步越小，被选中频率越高。
   - 每轮（一次调度扫描）队列的服务预算等于其权重；平局按队列名升序。
   - 队头放不下时先跳过该队列服务别人；扫描反复进行直到整轮无新进展。
3. **队列内：优先级 + 老化（aging）**
   - 次序为“有效优先级降序，提交序号升序”。
   - 有效优先级 = 静态优先级 + 老化加成；每等待 `boostInterval` 个调度周期
     （`Tick()`）加成 +1，封顶 `maxBoost`。因此低优先级/长作业等待足够久后
     必然超过新到的高优先级作业，**不会被持续饿死**；而新作业仍能在队列
     空闲时立即被服务（默认即时模式下提交即触发调度；批量模式下由 `Tick` 裁决）。
4. **节点选择：best-fit + 固定次序**
   - 选“放置后剩余资源足迹最小”的健康节点，减少碎片；
   - 平局完全按节点名升序裁定，队列内次序按（有效优先级、提交序号）裁定，
     因此**相同初始状态与提交序列给出完全一致的放置结论**（见
     `TestDeterministicReplay`，连跑 6 次逐作业比对）。
5. **资源归还后重调度**：取消、完成、节点恢复都会立即触发一轮调度（批量
   模式除外），排队作业在槽位归还的同一决策内被唤醒。

## 节点故障

- `MarkNodeUnhealthy(name)`：节点被标记为不健康，其上运行作业按提交序
  **逐个驱逐**：每个作业先经 `EVICT` 流水释放占用、再回到原队列
  （保留提交序号与已积累的老化），随后调度器尝试把它们安置到其他健康节点；
  放不下则保持排队，原因记为 `QUEUED_INSUFFICIENT_RESOURCES`。
- 不健康节点不参与放置；`MarkNodeHealthy` 恢复后（节点已空，可用量复位
  为总量）立即重新加入调度。
- 每次驱逐都会在流水里留下成对的 `EVICT`（释放量扣减）与重新排队备注事件，
  判定依据可追溯。

## 计划维护下线（DRAINING）

与“故障”相对，维护是**运维主动发起、节点仍然存活**的受控下线过程。两种状态
语义严格区分：

| 维度 | 计划维护 `DRAINING` | 突发故障 `UNHEALTHY` |
|---|---|---|
| 发起方式 | 运维主动 `MarkNodeMaintenance(node, reason)` | 被动 `MarkNodeUnhealthy(node)` |
| 节点存活 | 存活，可查、可取消 | 视为不可用 |
| 新作业 | 不接收（只在 `HEALTHY` 节点放置） | 不接收 |
| 其上**单作业** | 能迁则**平移**到健康节点继续运行；迁不走则**等待自然结束** | **立即全部驱逐** |
| 其上**作业组** | 整组立即停下重排队（含健康节点上的同伴） | 同左：整组立即停下重排队 |
| 可撤销 | `CancelMaintenance(node)` 立即回到正常调度 | 只能 `MarkNodeHealthy`（节点已清空、可用量复位） |
| 流水标记 | `NODE_DRAIN` / `MIGRATE` / `NODE_READY` | `NODE_DOWN` / `EVICT` / `NODE_UP` |
| 安全重启判定 | `NodeDrained(node)` 返回 true（维护中且 `jobs==0`） | 不适用（故障即视为离线） |

维护期间的规则：

1. **不接收新作业**：公平调度与组原子规划只在 `HEALTHY` 节点上选择容量，
   维护节点剩余的 `available` 不再被视为可放置容量；但维护节点上已在运行、
   迁不走的单作业仍持有其占用（账目照常守恒）。
2. **提交/排队/公平不被阻塞**：维护动作与提交共用同一把锁、同一条调度路径，
   维护期间 `Submit` 照常受理，放不下就排队，多队列 WRR 结论不变。
3. **单作业迁移 = 占用平移**：每轮调度开头先执行维护迁移 pass，按
   （节点名、提交序）确定性遍历，对能放入健康节点的作业在**同一决策内**记
   `MIGRATE`（源节点扣减）+ `ALLOCATE`（目标节点增加），作业保持 `RUNNING`、
   不重置起始时间、不重新参与队列竞争；迁不走的留在原节点继续跑到自然结束，
   之后每轮（提交、完成、取消、`Tick` 触发的调度）都会重试。
4. **取消维护**：`CancelMaintenance` 使节点立即回到 `HEALTHY`，
   仍在其上运行的作业不受影响，已重排队的作业按公平次序竞争（可能被放回来，
   但不强制拉回）。
5. **维护中再故障**：`MarkNodeUnhealthy` 会把 `DRAINING` 升级为 `UNHEALTHY`，
   剩余作业按故障语义驱逐。

### 作业组的“中断一致性”

无论计划维护还是突发故障，**组里只要有一个成员被打断，整组一起停下重排队**：

- 被处理节点上的组成员：维护记 `MIGRATE`、故障记 `EVICT`，原因统一带
  `REQUEUED_GROUP_INTERRUPTED`；
- 跑在**健康节点**上的同组同伴：记 `GROUP_REQUEUE`（占用同步释放），
  原因同为 `REQUEUED_GROUP_INTERRUPTED`；
- 整组回到原队列后仍按原子放置 + WRR 次序裁决：要么整组一起重新运行，
  要么继续整组排队，**绝不允许一部分 RUNNING、一部分 QUEUED**。
- 重排队保留原 `SubmitSeq` 与已积累老化；组内部分成员此前就在排队时，
  它们不会被重复登记入队。

### 可审计、可复现

正常完成归还（`RELEASE`/`COMPLETE`）、计划维护迁移（`MIGRATE`）、
故障中断（`EVICT`）、整组重排队（`GROUP_REQUEUE`）使用**互不重叠的事件类型与
原因码**，审计可直接区分：

```
#0013 MIGRATE job=j2 node=node-b delta=(...) reason=REQUEUED_MAINTENANCE
       (planned maintenance: migrate off node-b toward healthy node node-a)
#0014 ALLOCATE job=j2 node=node-a delta=(...)
       (planned maintenance: migrated from node-b to node-a (continues running))
#0021 GROUP_REQUEUE job=g2 node=node-c delta=(...) reason=REQUEUED_GROUP_INTERRUPTED
       (group "G" interrupted (planned maintenance); healthy-node member stopped ...)
```

迁移/驱逐的遍历次序（节点名 + `SubmitSeq`）、健康节点选择（best-fit +
节点名平局裁定）全部确定，相同初始状态与操作序列下，逐作业落点与逐事件流水
都可复现（`TestMaintenanceDeterministicReplay`）。

## 资源流水（Ledger）

`Ledger` 记录单调编号、带时间戳与原因的不可变事件：

```
#0010 EVICT job=j2 queue=batch node=node-b delta=(cpu=6,mem=16384MB,gpu=2)
       (node unhealthy: preempt and requeue)
```

事件类型：`ALLOCATE / RELEASE / EVICT / MIGRATE / GROUP_REQUEUE / REJECT /
QUEUE / CANCEL / COMPLETE / NODE_DOWN / NODE_UP / NODE_DRAIN / NODE_READY`。
其中 `ALLOCATE` 的 delta 是加到节点的正量，`RELEASE / EVICT / MIGRATE /
GROUP_REQUEUE` 的 delta 是从节点已分配量中扣减的正量（事件类型本身即表明
方向，也表明了中断来源，便于审计）。`Scheduler.Ledger().FormatLog()` 与
`Snapshot()` 可直接打印“分配流水 + 判定依据 + 集群状态”。

## 本地验证

需要 Go 1.25+（见 `go.mod`）。

```bash
# 构建
go build ./...

# 跑全部单测（测试日志中会打印分配流水与判定依据）
go test -v ./...

# 竞态检测 + 重复执行（并发测试在 -race 下验证）
go test -race -count=3 ./...

# 覆盖率
go test -coverprofile=cov.out ./... && go tool cover -func=cov.out

# 运行端到端演示：提交/排队/拒绝/作业组/计划维护/迁移/故障/恢复/老化，末尾打印完整流水
go run ./cmd/hpcsim
```

测试覆盖要点：

- `TestConcurrentSubmitNoDoubleAllocate`：16×25 并发提交，GPU 瓶颈下运行数
  严格不超过 8，账目守恒、无双重放置；
- `TestConcurrentSubmitCancelComplete`：并发提交 + 完成/取消 + 并发守恒巡检；
- `TestSubmitGroupAtomicPlacement` / `...FitsAtomically` /
  `...InvalidMemberRejectsWholeGroup`：作业组资源不足排队、跨节点整体放置、
  整组拒绝不留痕；
- `TestNodeFailureEvictsAndReleases` / `TestEvictedJobKeepsQueuePosition`：
  故障驱逐、占用归零、重新排队、迁移到健康节点、恢复后可再次承载；
- `TestWeightedRoundRobinAcrossQueues` / `TestAgingPreventsStarvation`：
  加权份额方向正确、低权重/低优先级不被饿死、资源归还后积压排空；
- `TestDeterministicReplay`：相同输入序列多次执行的放置逐作业一致；
- `TestConcurrentMaintenanceConservation`：多节点反复进入/取消维护叠加并发
  提交、完成与作业组提交，巡检者持续核对账目守恒、无双重放置、无组状态撕裂、
  维护节点上不残留组成员（`-race` 下验证）；
- `TestMaintenanceNoNewPlacementsAndNaturalDrain` /
  `TestMaintenanceJobRunsToCompletionWhenNoCapacity`：维护节点不接收新作业、
  迁不走的作业自然结束后 `NodeDrained` 才为 true、取消维护后恢复承载；
- `TestMaintenanceMigratesStandaloneJob`：可迁单作业 `MIGRATE`+`ALLOCATE`
  成对流水、源节点账目归零、作业保持运行；
- `TestMaintenanceWholeGroupRequeue` / `TestFailureWholeGroupRequeue`：
  维护与故障都保证整组重排队并原子重回，且 `MIGRATE` 与 `EVICT` 流水可区分；
- `TestMaintenanceDeterministicReplay`：维护/迁移/整组重排队序列逐作业、
  逐事件可复现。

## 接口兼容范围

- 既有接口（`Submit / SubmitGroup / Cancel / Complete / MarkNodeUnhealthy /
  MarkNodeHealthy / Tick / Snapshot / CheckConservation / Ledger`）签名保持
  不变；新增能力全部以**新方法/新枚举/新事件类型**提供：
  - 新方法：`MarkNodeMaintenance(node, reason)`、`CancelMaintenance(node)`、
    `NodeDrained(node)`、`GetNode(name)`；
  - 新节点状态：`NodeDraining`（`Node.State()` 返回 `DRAINING`）；
  - 新原因码：`REQUEUED_MAINTENANCE`、`REQUEUED_GROUP_INTERRUPTED`、
    `REQUEUED_EVICTED`；
  - 新流水事件：`MIGRATE`、`GROUP_REQUEUE`、`NODE_DRAIN`、`NODE_READY`。
- 一处**语义增强**（对既有调用方仍然合理）：故障被驱逐作业的排队原因由
  `QUEUED_INSUFFICIENT_RESOURCES` 细化为 `REQUEUED_EVICTED`，
  但既有的 `EVICT` 事件与 `"requeue after eviction"` 备注文案保留，
  旧审计规则不受影响；故障节点上的组作业现在会连带健康节点上的同伴整组
  重排队（此前不保证这一点，正是本次补齐的一致性语义）。
- 节点状态判定请使用返回值/`GetNode`/`Snapshot` 的字符串，不要依赖状态枚举的
  底层数值。

## 工程取舍说明

- 单节点放置模型：一个作业不可跨节点拆分，因此“超过单节点容量”的请求
  直接判 `REJECT_INFEASIBLE`，而不是无意义地长期排队。
- 公平优先于局部最优：同一份资源若多个队列竞争，由 WRR 决定归属，而不是
  谁先提交谁拿走；提交与调度解耦后，即时模式仍在同一把锁内完成“入队+裁决”，
  对调用方而言提交返回时结论已经确定。
- 老化设上限（`maxBoost`）：让等待者最终能赢，但避免低优先级作业永久霸榜。
