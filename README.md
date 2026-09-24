# hpc-batch-scheduler-go

本地批作业调度与资源核算服务：用内存中的节点与队列模拟集群状态，
保证并发提交、资源不足与节点故障场景下调度结论**一致、守恒且可解释**。

## 资源模型

- **Resource**：三维资源向量 `{CPUMilli, MemMiB, GPU}`（CPU 以毫核、内存以 MiB 计）。
- **Node**：具有固定 `Capacity` 与健康状态（`NodeReady` / `NodeDown`）的计算节点。
- **Job**：声明 `Request` 资源、所属 `Queue`、`Priority`（越大越高）与可选 `GroupID`。
  `GroupID` 非空的作业属于 **gang 作业组**：整组要么全部运行，要么全部等待，
  绝不部分占用；组内任一成员因节点故障中断时，整组一起回收并重新排队。
- **Ledger（资源账目）**：所有分配/回收的唯一入口，内部一把互斥锁串行化全部账目操作：
  - 同一资源不会被重复分配（分配前逐节点校验 `used+req <= capacity`）；
  - 作业取消、节点故障都会显式回收，杜绝泄漏；
  - `CheckConservation()` 校验守恒不变量：各节点已用非负、不超容量、
    逐条分配记录与汇总账目一致。测试在每个关键步骤后调用它。

## 提交准入：拒绝与排队原因可区分

| 情形 | 结果 | 原因码 |
|---|---|---|
| 队列不存在 | 拒绝 | `REJECT_UNKNOWN_QUEUE` |
| 声明非法（负资源、空/重复 ID） | 拒绝 | `REJECT_INVALID` |
| 需求超过集群全部节点总容量（永久不可行） | 拒绝 | `REJECT_EXCEEDS_CLUSTER` |
| 当前资源不足（暂时，可恢复） | 排队等待 | 判定日志 `WAIT_INSUFFICIENT_RESOURCE` |
| 作业组暂时无法整体放置 | 排队等待 | 判定日志 `WAIT_GROUP_NOT_READY` |

准入判定使用**含不健康节点**的总容量：节点故障是暂时状态，恢复后作业仍可调度，
不应被永久拒绝。

## 调度与公平策略

每轮 `ScheduleOnce()` 重复以下扫描直到无进展：

1. **队列间：加权主导份额公平（weighted DRF）**。
   每个队列的份额 = 其运行作业在 CPU/内存/GPU 三维度上占集群比例的最大值；
   按 `份额 / 权重` 升序排列，最低者优先获得下一个调度机会；
   份额相同时权重高者优先（最大-最小公平），再相同按队列名排序。
   **每次只全局启动一个作业（或一个作业组）就重算份额**，份额随分配即时反映。
2. **队列内：防饿死优先级**。有效优先级 = 声明优先级 + 年龄加成
   （被跳过超过 `starvationRounds=3` 轮后每轮 +1），相同则按提交顺序 FIFO。
   因此低优先级 / 长作业在有限轮次内必然获得调度，不会被持续饿死。
3. **gang 原子放置**：逐成员分配，任一失败即回滚账面分配，
   调度器状态在全部成员分配成功后才统一提交，绝不部分占用。
4. **回填**：队首作业组暂不可放置时，同队列后续小作业仍可被调度，
   提高利用率（其等待判定同样记入日志）。

**故障与回收**：`NodeDown` 回收故障节点上全部分配；受影响作业重新排队
（gang 作业整组回收重排）；`NodeUp` 后资源归还，等待作业可在后续轮次被重新调度。

**可复现性**：节点按 ID 字典序首次适配（first-fit），作业按提交序号排序，
所有排序都有确定性决胜规则——相同初始状态与提交序列下，
调度结论与分配流水完全一致（见 `TestReproducibility`）。

**可解释性**：每次提交、调度、等待、取消、故障重排都写入判定日志
（`Scheduler.Decisions()`），每次分配/回收写入分配流水（`Ledger.Log()`），
单元测试日志中会完整打印两者。

## 本地验证

```bash
# 全部单元测试（含分配流水与判定依据日志）
go test ./scheduler/ -v

# 并发安全（race detector）与稳定性
go test ./scheduler/ -race -count=3

# 交互式演示：提交 / gang / 节点故障 / 恢复重调度
go run ./cmd/scheddemo
```

测试覆盖：

- `TestConcurrentSubmitConservation`：64 路并发提交，账目守恒、不超分、不泄漏；
- `TestRejectReasonsDistinguishable`：拒绝与排队原因逐一可区分；
- `TestGangAllOrNothing`：作业组资源不足时整体等待、零部分占用，资源归还后整组调度；
- `TestNodeFailureReclaim`：节点故障回收、整组重排队、恢复后重调度；
- `TestCancelReleasesResources`：运行中/排队中取消的资源回收；
- `TestFairnessNoStarvation`：低优先级作业在高优先级作业持续到来下有限轮次内获调度；
- `TestWeightedFairShare`：权重 1:3 的两队列在 4 份容量下分得 1:3；
- `TestReproducibility`：相同初始状态与提交序列下调度结论与流水完全一致。

## 代码结构

```
scheduler/
  types.go      资源向量、节点、作业、队列、原因码等类型定义
  ledger.go     资源账目：分配/回收/故障处理/守恒校验/分配流水
  fairness.go   加权 DRF 队列排序、防饿死优先级、队内功序
  scheduler.go  提交准入、调度主循环、gang 原子放置、取消与故障处理
  scheduler_test.go  上述全部场景的单元测试
cmd/scheddemo/  本地演示程序
```
