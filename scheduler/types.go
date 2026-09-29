// Package scheduler 实现一个本地内存态的批作业调度与资源核算服务。
//
// 它用互斥锁保护的节点表与队列表模拟集群状态，提供作业提交/排队、
// 组作业整体调度、分配回收、故障迁移、多队列公平轮转以及资源账目守恒校验。
package scheduler

import (
	"fmt"
	"time"
)

// Resources 描述一份资源量（CPU 核数、内存 MB、加速器卡数）。
type Resources struct {
	CPU      int
	MemoryMB int
	GPU      int
}

// Add 返回 r + other，不修改接收者。
func (r Resources) Add(other Resources) Resources {
	return Resources{
		CPU:      r.CPU + other.CPU,
		MemoryMB: r.MemoryMB + other.MemoryMB,
		GPU:      r.GPU + other.GPU,
	}
}

// Sub 返回 r - other，调用方需保证结果非负。
func (r Resources) Sub(other Resources) Resources {
	return Resources{
		CPU:      r.CPU - other.CPU,
		MemoryMB: r.MemoryMB - other.MemoryMB,
		GPU:      r.GPU - other.GPU,
	}
}

// Fits 判断 other 是否能放入 r（逐维 <=）。
func (r Resources) Fits(other Resources) bool {
	return other.CPU <= r.CPU && other.MemoryMB <= r.MemoryMB && other.GPU <= r.GPU
}

// AnyPositive 判断资源量是否至少有一维为正。
func (r Resources) AnyPositive() bool {
	return r.CPU > 0 || r.MemoryMB > 0 || r.GPU > 0
}

func (r Resources) String() string {
	return fmt.Sprintf("(cpu=%d,mem=%dMB,gpu=%d)", r.CPU, r.MemoryMB, r.GPU)
}

// JobState 表示作业生命周期状态。
type JobState int

const (
	// StateQueued 已入队，等待资源。
	StateQueued JobState = iota
	// StateRunning 已分配资源并运行。
	StateRunning
	// StateCompleted 正常完成并释放资源。
	StateCompleted
	// StateCancelled 被用户取消。
	StateCancelled
	// StateRejected 提交时被拒绝（不进入排队）。
	StateRejected
)

func (s JobState) String() string {
	switch s {
	case StateQueued:
		return "QUEUED"
	case StateRunning:
		return "RUNNING"
	case StateCompleted:
		return "COMPLETED"
	case StateCancelled:
		return "CANCELLED"
	case StateRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

// RejectReason / 排队原因枚举。拒绝与排队使用不同编码，便于区分。
type ReasonCode string

const (
	// ReasonOK 无异常，作业被直接分配。
	ReasonOK ReasonCode = "OK"
	// ReasonQueuedInsufficient 资源不足，作业保持排队。
	ReasonQueuedInsufficient ReasonCode = "QUEUED_INSUFFICIENT_RESOURCES"
	// ReasonQueuedGroupAtomic 组作业当前无法整体满足，保持排队。
	ReasonQueuedGroupAtomic ReasonCode = "QUEUED_GROUP_ATOMIC"
	// ReasonRejectInvalid 资源声明非法（负数/全零）。
	ReasonRejectInvalid ReasonCode = "REJECT_INVALID_REQUEST"
	// ReasonRejectInfeasible 集群总容量永远无法满足该请求。
	ReasonRejectInfeasible ReasonCode = "REJECT_INFEASIBLE"
	// ReasonRejectDuplicate 作业 ID 重复。
	ReasonRejectDuplicate ReasonCode = "REJECT_DUPLICATE_ID"
	// ReasonRejectNoQueue 指定的队列不存在。
	ReasonRejectNoQueue ReasonCode = "REJECT_NO_QUEUE"
	// ReasonRejectGroupMismatch 同组作业的队列/资源约束不一致。
	ReasonRejectGroupMismatch ReasonCode = "REJECT_GROUP_MISMATCH"
	// ReasonRejectQueueFull 队列在途作业数已达上限。
	ReasonRejectQueueFull ReasonCode = "REJECT_QUEUE_FULL"
	// ReasonNotFound 取消/查询时作业不存在。
	ReasonNotFound ReasonCode = "NOT_FOUND"
	// ReasonQueuedGroupInterrupted 作业组因成员被中断（计划维护迁移或
	// 节点故障）而整组停止、回到排队状态重新竞争资源。
	ReasonQueuedGroupInterrupted ReasonCode = "QUEUED_GROUP_INTERRUPTED"
	// ReasonMigratedMaintenance 作业因计划维护从维护节点迁移到健康节点
	// （ALLOCATE 流水携带，用于区分常规放置）。
	ReasonMigratedMaintenance ReasonCode = "MIGRATED_MAINTENANCE"
	// ReasonMaintenanceDrainPending 维护节点上的非组作业当前迁不走，
	// 仍留在原节点继续运行，等待维护窗口内自然结束或下一轮迁移。
	ReasonMaintenanceDrainPending ReasonCode = "MAINTENANCE_DRAIN_PENDING"
)

// QueueConfig 描述一个队列的静态配置。
type QueueConfig struct {
	Name       string
	Weight     int           // 公平权重，>=1；轮转时每轮可获得的调度预算份额
	Priority   int           // 静态优先级，越大越优先；仅作老化前的次序依据之一
	MaxJobs    int           // 队列最大在途作业数（排队+运行），<=0 表示不限
	MaxRuntime time.Duration // 最大运行时长，<=0 表示不限（用于长作业判定演示）
}

// Queue 是运行期队列对象：配置 + 待调度作业 ID 列表（FIFO）。
type Queue struct {
	config  QueueConfig
	pending []string
}

// NodeState 表示节点运行状态。
type NodeState int

const (
	// NodeHealthy 健康：正常参与调度、接收新作业。
	NodeHealthy NodeState = iota
	// NodeUnhealthy 突发故障：节点不可用，其上作业被强制驱逐（EVICT）。
	NodeUnhealthy
	// NodeMaintenance 计划维护下线：节点不再接收新作业，剩余容量不计入
	// 可放置容量；其上作业可在维护窗口内被迁移或等待自然结束。
	NodeMaintenance
)

func (s NodeState) String() string {
	switch s {
	case NodeHealthy:
		return "HEALTHY"
	case NodeUnhealthy:
		return "UNHEALTHY"
	case NodeMaintenance:
		return "MAINTENANCE"
	default:
		return "UNKNOWN"
	}
}

// Node 描述一个计算节点。
type Node struct {
	Name      string
	Total     Resources
	available Resources // 未分配资源
	state     NodeState
	jobs      map[string]Resources // 当前占用该节点的作业 -> 占用量
}

// Job 描述一个批作业。
type Job struct {
	ID          string
	Queue       string
	Request     Resources
	Priority    int    // 作业级静态优先级，越大越优先
	GroupID     string // 非空表示属于需整体调度的作业组
	SubmitSeq   int    // 全局提交序号，保证同状态下次序可复现
	SubmittedAt time.Time
	submitTick  int // 提交时所处的调度周期（老化计时起点）

	State     JobState
	NodeName  string     // 运行时所在节点；排队中为空
	Reason    ReasonCode // 最近一次判定原因（排队/拒绝）
	Detail    string     // 人类可读说明
	StartedAt time.Time
	EndedAt   time.Time
	ageBoost  int // 老化加成（由公平策略维护）
}

// IsGrouped 判断作业是否属于某个作业组。
func (j *Job) IsGrouped() bool { return j.GroupID != "" }

// SubmitResult 是提交判定的结论。
type SubmitResult struct {
	JobID    string
	Accepted bool       // true=已接收（可能直接运行，也可能排队）；false=被拒绝
	State    JobState   // 接收后的状态（StateRunning 或 StateQueued）
	Reason   ReasonCode // OK / QUEUED_* / REJECT_*
	Detail   string
}

// Placement 是单次分配的落地记录。
type Placement struct {
	JobID    string
	NodeName string
	Request  Resources
	Seq      int // 全局分配序号
}
