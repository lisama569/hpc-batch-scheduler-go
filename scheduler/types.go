// Package scheduler 实现本地批作业调度与资源核算。
package scheduler

import "fmt"

// Resource 表示一份可分配资源向量（CPU 以毫核计，内存以 MiB 计）。
type Resource struct {
	CPUMilli int64
	MemMiB   int64
	GPU      int64
}

// Add 返回两份资源的和。
func (r Resource) Add(o Resource) Resource {
	return Resource{r.CPUMilli + o.CPUMilli, r.MemMiB + o.MemMiB, r.GPU + o.GPU}
}

// Sub 返回 r 减去 o 的结果（调用方保证不产生负值）。
func (r Resource) Sub(o Resource) Resource {
	return Resource{r.CPUMilli - o.CPUMilli, r.MemMiB - o.MemMiB, r.GPU - o.GPU}
}

// Fits 判断 r 是否各维度都不超过 cap。
func (r Resource) Fits(cap Resource) bool {
	return r.CPUMilli <= cap.CPUMilli && r.MemMiB <= cap.MemMiB && r.GPU <= cap.GPU
}

// IsZero 判断资源是否为空。
func (r Resource) IsZero() bool {
	return r.CPUMilli == 0 && r.MemMiB == 0 && r.GPU == 0
}

func (r Resource) String() string {
	return fmt.Sprintf("cpu=%dm mem=%dMi gpu=%d", r.CPUMilli, r.MemMiB, r.GPU)
}

// NodeStatus 表示节点健康状态。
type NodeStatus int

const (
	NodeReady NodeStatus = iota
	NodeDown
)

// Node 表示集群中的一个计算节点。
type Node struct {
	ID       string
	Capacity Resource
	Status   NodeStatus
}

// JobState 表示作业生命周期状态。
type JobState int

const (
	JobPending JobState = iota
	JobRunning
	JobCancelled
	JobFailed // 因节点故障等原因被中断
	JobDone
)

func (s JobState) String() string {
	switch s {
	case JobPending:
		return "PENDING"
	case JobRunning:
		return "RUNNING"
	case JobCancelled:
		return "CANCELLED"
	case JobFailed:
		return "FAILED"
	case JobDone:
		return "DONE"
	}
	return "UNKNOWN"
}

// Job 表示一个批作业。GroupID 非空时属于需整体调度（gang）的作业组。
type Job struct {
	ID        string
	Queue     string
	Priority  int // 数值越大优先级越高
	Request   Resource
	GroupID   string
	SubmitSeq int64 // 提交顺序，用于公平与可复现排序
	Age       int64 // 被跳过的轮次，用于防饿死
}

// Queue 表示一个逻辑队列，Weight 用于公平分配。
type Queue struct {
	Name   string
	Weight float64
}

// RejectReason 区分提交被拒绝 / 保持排队的原因。
type RejectReason int

const (
	RejectNone RejectReason = iota
	// RejectExceedsCluster 表示作业（或作业组）需求超过集群总容量，永远不可调度。
	RejectExceedsCluster
	// RejectUnknownQueue 表示提交到不存在的队列。
	RejectUnknownQueue
	// WaitInsufficientResource 表示当前资源不足，保持排队（可恢复）。
	WaitInsufficientResource
	// WaitGroupNotReady 表示作业组暂无法整体放置，保持排队。
	WaitGroupNotReady
)

func (r RejectReason) String() string {
	switch r {
	case RejectNone:
		return "NONE"
	case RejectExceedsCluster:
		return "REJECT_EXCEEDS_CLUSTER"
	case RejectUnknownQueue:
		return "REJECT_UNKNOWN_QUEUE"
	case WaitInsufficientResource:
		return "WAIT_INSUFFICIENT_RESOURCE"
	case WaitGroupNotReady:
		return "WAIT_GROUP_NOT_READY"
	}
	return "UNKNOWN"
}

// RejectInvalid 表示作业声明非法（负资源、空 ID、重复 ID 等）。
const RejectInvalid RejectReason = 100
