package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// EventType 标识资源流水事件类型。
type EventType string

const (
	EvAllocate     EventType = "ALLOCATE"      // 分配资源给作业
	EvRelease      EventType = "RELEASE"       // 作业完成/取消，正常归还
	EvEvict        EventType = "EVICT"         // 节点突发故障，作业被驱逐（占用被强制释放）
	EvMigrate      EventType = "MIGRATE"       // 计划维护，作业被迁离维护节点（占用在源节点释放）
	EvGroupRequeue EventType = "GROUP_REQUEUE" // 整组中断：健康节点上的同组成员一并停下重排队
	EvNodeDown     EventType = "NODE_DOWN"     // 节点突发故障（UNHEALTHY）
	EvNodeUp       EventType = "NODE_UP"       // 节点从故障恢复健康
	EvNodeDrain    EventType = "NODE_DRAIN"    // 节点进入计划维护（DRAINING，不接收新作业）
	EvNodeReady    EventType = "NODE_READY"    // 维护被取消，节点回到正常调度
	EvReject       EventType = "REJECT"        // 提交被拒绝
	EvQueue        EventType = "QUEUE"         // 资源不足进入排队
	EvCancel       EventType = "CANCEL"        // 排队作业被取消（无资源量变动）
	EvComplete     EventType = "COMPLETE"      // 作业完成（无资源量变动的备注事件）
)

// LedgerEvent 是一条不可变的资源分配流水。
type LedgerEvent struct {
	Seq      int
	Time     time.Time
	Type     EventType
	JobID    string
	NodeName string
	Queue    string
	Delta    Resources // 资源量绝对值；ALLOCATE 时加到节点，RELEASE/EVICT 时从节点扣减，方向由 Type 决定
	Reason   ReasonCode
	Detail   string
}

func (e LedgerEvent) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%04d %s %-9s", e.Seq, e.Time.Format("15:04:05.000"), e.Type)
	if e.JobID != "" {
		fmt.Fprintf(&b, " job=%s", e.JobID)
	}
	if e.Queue != "" {
		fmt.Fprintf(&b, " queue=%s", e.Queue)
	}
	if e.NodeName != "" {
		fmt.Fprintf(&b, " node=%s", e.NodeName)
	}
	if e.Delta.AnyPositive() {
		fmt.Fprintf(&b, " delta=%s", e.Delta)
	}
	if e.Reason != "" && e.Reason != ReasonOK {
		fmt.Fprintf(&b, " reason=%s", e.Reason)
	}
	if e.Detail != "" {
		fmt.Fprintf(&b, " (%s)", e.Detail)
	}
	return b.String()
}

// Ledger 是线程安全的资源账目：记录流水并维护各节点已分配量，
// 可随时校验“已分配 + 可用 == 总量”的守恒关系。
type Ledger struct {
	mu        sync.Mutex
	events    []LedgerEvent
	seq       int
	allocated map[string]Resources // node -> 已分配总量
	total     map[string]Resources // node -> 总量（含下线节点，账目始终核对全集群）
}

// NewLedger 创建空账目。
func NewLedger() *Ledger {
	return &Ledger{
		allocated: make(map[string]Resources),
		total:     make(map[string]Resources),
	}
}

// RegisterNode 将一个节点纳入账目核对范围。
func (l *Ledger) RegisterNode(name string, total Resources) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.total[name] = total
	if _, ok := l.allocated[name]; !ok {
		l.allocated[name] = Resources{}
	}
}

// RemoveNode 从账目范围移除节点（仅允许在该节点已分配为零时）。
func (l *Ledger) RemoveNode(name string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if a, ok := l.allocated[name]; ok && a.AnyPositive() {
		return fmt.Errorf("ledger: cannot remove node %s with %s allocated", name, a)
	}
	delete(l.total, name)
	delete(l.allocated, name)
	return nil
}

// record 追加一条流水并更新节点已分配量。
func (l *Ledger) record(evType EventType, jobID, node, queue string, delta Resources, reason ReasonCode, detail string) LedgerEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	ev := LedgerEvent{
		Seq:      l.seq,
		Time:     time.Now(),
		Type:     evType,
		JobID:    jobID,
		NodeName: node,
		Queue:    queue,
		Delta:    delta,
		Reason:   reason,
		Detail:   detail,
	}
	if node != "" {
		cur := l.allocated[node]
		switch evType {
		case EvAllocate:
			cur = cur.Add(delta)
		case EvRelease, EvEvict, EvMigrate, EvGroupRequeue:
			cur = cur.Sub(delta)
		}
		l.allocated[node] = cur
	}
	l.events = append(l.events, ev)
	return ev
}

// Allocate 记录一次分配。
func (l *Ledger) Allocate(jobID, node, queue string, req Resources, detail string) LedgerEvent {
	return l.record(EvAllocate, jobID, node, queue, req, ReasonOK, detail)
}

// Release 记录一次正常归还。
func (l *Ledger) Release(jobID, node, queue string, req Resources, detail string) LedgerEvent {
	return l.record(EvRelease, jobID, node, queue, req, ReasonOK, detail)
}

// Evict 记录一次故障驱逐导致的强制释放。
func (l *Ledger) Evict(jobID, node, queue string, req Resources, detail string) LedgerEvent {
	return l.record(EvEvict, jobID, node, queue, req, ReasonOK, detail)
}

// Migrate 记录计划维护导致的一次迁离（占用在源节点释放，随后重新排队）。
func (l *Ledger) Migrate(jobID, node, queue string, req Resources, reason ReasonCode, detail string) LedgerEvent {
	return l.record(EvMigrate, jobID, node, queue, req, reason, detail)
}

// GroupRequeue 记录整组中断：健康节点上的同组成员被一并停下（占用释放）。
func (l *Ledger) GroupRequeue(jobID, node, queue string, req Resources, reason ReasonCode, detail string) LedgerEvent {
	return l.record(EvGroupRequeue, jobID, node, queue, req, reason, detail)
}

// Note 记录不改变资源量的判定事件（拒绝/排队/节点上下线）。
func (l *Ledger) Note(evType EventType, jobID, node, queue string, reason ReasonCode, detail string) LedgerEvent {
	return l.record(evType, jobID, node, queue, Resources{}, reason, detail)
}

// Events 返回流水的快照副本。
func (l *Ledger) Events() []LedgerEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]LedgerEvent, len(l.events))
	copy(out, l.events)
	return out
}

// Allocated 返回某节点当前已分配量的副本。
func (l *Ledger) Allocated(node string) Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.allocated[node]
}

// TotalAllocated 返回全集群已分配量之和。
func (l *Ledger) TotalAllocated() Resources {
	l.mu.Lock()
	defer l.mu.Unlock()
	var sum Resources
	for _, a := range l.allocated {
		sum = sum.Add(a)
	}
	return sum
}

// CheckConservation 校验每个节点 已分配+可用==总量；
// available 由调用方（调度器）按节点传入，避免账目与节点表互相耦合。
func (l *Ledger) CheckConservation(available map[string]Resources) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	names := make([]string, 0, len(l.total))
	for name := range l.total {
		names = append(names, name)
	}
	// 确定性输出：按名称排序
	sort.Strings(names)
	for _, name := range names {
		total := l.total[name]
		used := l.allocated[name]
		avail, ok := available[name]
		if !ok {
			return fmt.Errorf("ledger: node %s missing in availability table", name)
		}
		got := used.Add(avail)
		if got != total {
			return fmt.Errorf("ledger: conservation violated on %s: used %s + available %s = %s, want total %s",
				name, used, avail, got, total)
		}
		if used.CPU < 0 || used.MemoryMB < 0 || used.GPU < 0 {
			return fmt.Errorf("ledger: negative allocation on %s: %s", name, used)
		}
	}
	return nil
}

// FormatLog 将全部流水渲染为可打印文本（供测试日志输出判定依据）。
func (l *Ledger) FormatLog() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "ledger: %d events, total allocated %s\n", len(l.events), l.totalAllocatedLocked())
	for _, ev := range l.events {
		fmt.Fprintf(&b, "  %s\n", ev)
	}
	return b.String()
}

func (l *Ledger) totalAllocatedLocked() (sum Resources) {
	for _, a := range l.allocated {
		sum = sum.Add(a)
	}
	return sum
}
