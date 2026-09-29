# HPC Batch Scheduler（本地内存态批作业调度与资源核算）

一个不依赖外部集群的本地批作业调度器：用互斥锁保护的内存节点表与队列表
模拟集群，目标是在**并发提交、资源不足、节点故障、计划维护下线**四类情形下，
让调度结论**一致、可复现、可解释**，并保证资源账目始终守恒。

## 目录结构

```
scheduler/
  types.go       资源、作业、节点、队列等核心类型与状态枚举（含 NodeMaintenance）
  scheduler.go   调度器：提交/排队/取消/完成/故障恢复/公平调度主循环
  fairness.go    加权轮转（WRR）与作业老化（aging）
  maintenance.go 计划维护下线：进入/排空/状态查询/取消、迁移、整组中断
  ledger.go      资源流水账目（ALLOCATE/RELEASE/EVICT/MIGRATE/…）与守恒校验
  errors.go      哨兵错误
  *_test.go      并发、作业组、故障回收、维护迁移、公平性、可复现性、账目测试
cmd/hpcsim/      可运行演示（main.go：基础场景 + 独立的计划维护场景）
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
| 作业组因成员被中断（维护/故障）而整组重排队 | true | `QUEUED` | `QUEUED_GROUP_INTERRUPTED` |
| 维护迁移在健康节点重新落地（ALLOCATE 原因） | true | `RUNNING` | `MIGRATED_MAINTENANCE` |
| 维护节点上的作业暂时迁不走、等待自然结束 | true | `RUNNING` | `MAINTENANCE_DRAIN_PENDING` |

关键区分：**“排队”是被接受但暂时放不下**（资源归还后会再调度），
**“拒绝”是根本不会排队**（非法或永远不可行）。

## 并发与一致性

- 调度器只有一把 `sync.Mutex`，节点表、队列表、作业表、组表、账目更新
  全部在同一临界区内完成，因此“同一份资源在同一时刻被重复分配”不可能发生。
- 分配（ALLOCATE）与回收（RELEASE/EVICT）成对写入 Ledger，取消、完成、
  故障驱逐走同一条 `releaseLocked` 路径，被中断作业的占用必然被释放，不会泄漏。
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

## 计划维护下线（Planned Maintenance）

运维可主动把节点置为维护中（滚动升级、硬件保养等）。**维护与突发故障是两种
不同的节点状态、两套不同流水，语义严格区分：**

| 维度 | 计划维护 `NodeMaintenance` | 突发故障 `NodeUnhealthy` |
|---|---|---|
| 触发方式 | 运维主动 `MarkNodeMaintenance` | `MarkNodeUnhealthy` |
| 节点上作业 | **可迁走继续跑**，或等待自然结束（不强制清空） | **强制全部驱逐** |
| 资源释放流水 | 维护节点成员 `MIGRATE`；健康伙伴 `GROUP_REQUEUE` | 故障节点成员 `EVICT`；健康伙伴 `GROUP_REQUEUE` |
| 节点状态流水 | `NODE_MAINTENANCE` / `NODE_READY`（取消） | `NODE_DOWN` / `NODE_UP` |
| 可否取消恢复 | 可 `CancelNodeMaintenance`，留在节点上的作业继续运行 | 只能 `MarkNodeHealthy`（节点已空、可用量复位） |
| 期间是否收新作业 | 否，剩余可用量不计入可放置容量 | 否 |

### 接口

```go
// 进入维护：不接收新作业，并立即执行一轮尽力而为排空，返回排空结论。
res, err := s.MarkNodeMaintenance("node-x") // (DrainResult, error)

// 维护窗口内再执行一轮排空（滚动升级中可周期性重试）。
res, err = s.DrainNode("node-x")

// 运维据此判断能否安全重启：Drained==true 即节点已无运行作业。
st, err := s.DrainStatus("node-x") // NodeDrainStatus{State,Running,InUse,Drained,Reason,...}

// 取消维护：节点回到正常调度；未迁走的作业继续运行，可用量不被复位。
err = s.CancelNodeMaintenance("node-x")
```

`DrainResult{Drained,Running,InUse,Migrated,Requeued,Waiting}` 同时给出本轮
迁走、整组重排队、仍在等待的作业清单，便于运维脚本判断与审计。

### 排空与迁移规则

1. **维护节点的剩余可用量不再是可放置容量**：best-fit 与作业组规划都只在
   `NodeHealthy` 节点上进行。维护期间新作业仍可正常提交、排队、按公平次序被
   服务到健康节点，公平结论不会因为维护动作而卡死。
2. **非组作业：尽力而为迁移**。若存在放得下的健康节点，立即迁移：
   `MIGRATE` 先从维护节点释放占用，再在目标节点产生一条带
   `MIGRATED_MAINTENANCE` 原因的 `ALLOCATE`；`StartedAt` 保留，视为同一次运行
   的延续。若当前没有健康节点放得下，作业**留在维护节点继续运行**
   （原因 `MAINTENANCE_DRAIN_PENDING`），等它自然结束，或下一轮
   `DrainNode`/资源归还后再试迁。
3. **作业组：整组一致中断**（见下节）。维护节点上的组成员以 `MIGRATE` 释放，
   其它健康节点上的同组成员以 `GROUP_REQUEUE` 释放，整组回到队列重竞争。
4. **安全重启判定**：`DrainStatus().Drained == true` 当且仅当节点上已无运行作业。
5. **维护可取消**：`CancelNodeMaintenance` 把节点置回健康并立即触发一轮调度；
   维护期间已经迁走、已经整组重排队的既有结论不回滚。

### 作业组在“被中断”上的整组一致性

无论触发源是**计划维护**还是**节点故障**，只要组里有成员被打断，**同一个组
的其它成员——哪怕正跑在健康节点上——也一起停下来**，整组回到排队状态重新竞争：

- 不允许出现组里一部分还在跑、另一部分已排队；
- 所有中断成员统一记排队原因 `QUEUED_GROUP_INTERRUPTED`，保留原 `SubmitSeq`
  与已积累的老化；
- 重新调度仍走原来的**原子放置**（一份临时可用视图上逐成员预留，任一放不下
  则整组继续排队）与**公平次序**：要么整组在同一决策里一起再跑起来，要么整组
  继续排队；整组重排只会落在健康节点，绝不回维护节点。

四种“停止/归还”情形在流水里用不同事件类型区分，互不混淆：

```
RELEASE        正常完成 / 取消的正常归还
MIGRATE        计划维护：占用从维护节点释放以便迁移
EVICT          突发故障：强制驱逐
GROUP_REQUEUE  整组一致：健康节点上的同组伙伴随中断成员一起停止
```

### 接口兼容范围

- **既有接口签名与语义保持不变**：`Submit / SubmitGroup / Cancel / Complete /
  MarkNodeUnhealthy / MarkNodeHealthy / Tick / CheckConservation / Snapshot /
  Ledger / GetJob` 均未改签名。
- `MarkNodeUnhealthy` 的行为增强为“故障也满足整组一致”：非组作业仍按原逻辑
  `EVICT + 重排队`（原因仍为 `QUEUED_INSUFFICIENT_RESOURCES`），组作业则整组
  中断（原因 `QUEUED_GROUP_INTERRUPTED`）。
- `MarkNodeHealthy` 现在拒绝在维护节点上调用（返回 `ErrInvalidState`）；维护
  取消必须使用 `CancelNodeMaintenance`，避免把仍有作业的维护节点可用量错误
  复位为总量而破坏守恒。
- 维护能力为**新增 API + 新增节点状态 + 新增流水事件/原因码**，旧调用方无需
  改动即可继续编译运行。

## 资源流水（Ledger）

`Ledger` 记录单调编号、带时间戳与原因的不可变事件：

```
#0010 EVICT job=j2 queue=batch node=node-b delta=(cpu=6,mem=16384MB,gpu=2)
       (node unhealthy: preempt and requeue)
```

事件类型：`ALLOCATE / RELEASE / EVICT / MIGRATE / GROUP_REQUEUE / REJECT /
QUEUE / CANCEL / COMPLETE / NODE_DOWN / NODE_UP / NODE_MAINTENANCE / NODE_READY /
DRAIN_PASS`。其中 `ALLOCATE` 的 delta 是加到节点的正量（维护迁移落地的那条带
原因 `MIGRATED_MAINTENANCE`），`RELEASE / EVICT / MIGRATE / GROUP_REQUEUE` 的
delta 是从节点已分配量中扣减的正量（事件类型本身即表明方向与原因）。
`Scheduler.Ledger().FormatLog()` 与 `Snapshot()` 可直接打印“分配流水 +
判定依据 + 集群状态”。

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

# 运行端到端演示：前半为提交/排队/拒绝/作业组/故障/恢复/老化；
# 后半在独立调度器上演示计划维护（整组中断、迁移失败等待、排空、取消），末尾打印两份流水
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
- `TestFailureInterruptsWholeGroup` / `TestMaintenanceWhileGroupOnNodeFails`：
  故障下整组一致中断（EVICT 与 GROUP_REQUEUE 可区分）、维护中节点再故障；
- `TestMaintenanceDrainsByMigrationAndBlocksNewPlacements`：
  维护节点不收新作业、暂不可迁时原地等待、资源归还后 `MIGRATE +
  ALLOCATE(MIGRATED_MAINTENANCE)` 成对迁移、StartedAt 保留、排空判定；
- `TestMaintenanceNaturalCompletion`：维护节点作业自然结束后节点判定可安全重启；
- `TestMaintenanceInterruptsWholeGroup`：维护触发整组一致中断，MIGRATE 与
  GROUP_REQUEUE 流水区分，容量归还后整组原子重排、绝不回维护节点；
- `TestCancelMaintenanceReturnsNodeToService`：取消维护恢复调度、遗留作业继续
  运行、非法状态转换（对维护节点调 MarkNodeHealthy / 重复取消等）报错；
- `TestConcurrentMaintenanceSubmitCancel`：多节点同时维护/排空/取消与并发提交、
  完成、取消交织，持续核对账目守恒、无重复放置，并通过流水审计确认维护期间
  没有任何新 `ALLOCATE` 落到维护节点；
- `TestMaintenanceDeterministicReplay`：含维护/排空/取消的相同操作序列多次
  执行，逐作业的状态、落点、原因完全一致；
- `TestWeightedRoundRobinAcrossQueues` / `TestAgingPreventsStarvation`：
  加权份额方向正确、低权重/低优先级不被饿死、资源归还后积压排空；
- `TestDeterministicReplay`：相同输入序列多次执行的放置逐作业一致。

## 工程取舍说明

- 单节点放置模型：一个作业不可跨节点拆分，因此“超过单节点容量”的请求
  直接判 `REJECT_INFEASIBLE`，而不是无意义地长期排队。
- 公平优先于局部最优：同一份资源若多个队列竞争，由 WRR 决定归属，而不是
  谁先提交谁拿走；提交与调度解耦后，即时模式仍在同一把锁内完成“入队+裁决”，
  对调用方而言提交返回时结论已经确定。
- 老化设上限（`maxBoost`）：让等待者最终能赢，但避免低优先级作业永久霸榜。
