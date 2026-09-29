package scheduler

import (
	"fmt"
	"sort"
)

// 计划维护下线相关能力：
//   - 运维可把节点置为 NodeMaintenance（计划维护），与突发故障 NodeUnhealthy 区分；
//   - 维护节点不再接收新作业，剩余可用量不参与任何放置；
//   - 维护窗口内可反复执行排空（Drain）：运行中作业被迁移到健康节点，
//     或等待自然结束；运维可通过 DrainStatus 判断节点是否已清空可安全重启；
//   - 维护可被取消（CancelNodeMaintenance），节点回到正常参与调度；
//   - 作业组在“被中断”这件事上保持整组一致：任一成员因维护迁移或节点故障
//     被打断，同组其它成员（即便在健康节点上）也一起停止、整组重新排队。

// DrainResult 描述一轮维护排空尝试的结果。
type DrainResult struct {
	Node     string    // 被排空的维护节点
	Migrated []string  // 本轮成功迁移到健康节点的非组作业 ID（按提交序）
	Requeued []string  // 本轮因整组一致而回到排队的作业 ID（含同组健康节点成员）
	Waiting  []string  // 仍留在维护节点运行、等待自然结束或下一轮迁移的作业 ID
	Drained  bool      // 节点是否已无运行作业（可安全重启）
	Running  int       // 节点上当前运行作业数
	InUse    Resources // 节点上当前仍被占用的资源量
}

// NodeDrainStatus 是维护节点的排空判定视图，供运维判断能否安全重启。
type NodeDrainStatus struct {
	Node      string
	State     NodeState
	Running   int
	InUse     Resources
	Available Resources // 维护期间不参与放置，仅作账目展示
	JobIDs    []string  // 仍在该节点运行的作业（按提交序）
	Drained   bool      // Running == 0
	Reason    ReasonCode
	Detail    string
}

// MarkNodeMaintenance 将节点置于计划维护状态：不再接收新作业，并立即执行
// 一轮尽力而为的排空。维护状态与突发故障可区分（流水 NODE_MAINTENANCE）。
func (s *Scheduler) MarkNodeMaintenance(node string) (DrainResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return DrainResult{}, fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.state == NodeMaintenance {
		return DrainResult{}, fmt.Errorf("%w: node %q already in maintenance", ErrInvalidState, node)
	}
	if n.state == NodeUnhealthy {
		return DrainResult{}, fmt.Errorf("%w: node %q is unhealthy; recover it before maintenance", ErrInvalidState, node)
	}
	n.state = NodeMaintenance
	s.ledger.Note(EvNodeMaint, "", node, "", ReasonOK, "node entered planned maintenance; no new placements")
	return s.drainLocked(node), nil
}

// DrainNode 在维护窗口内再执行一轮排空尝试（迁移可迁走的作业），
// 返回最新排空结论。仅对处于维护状态的节点有效。
func (s *Scheduler) DrainNode(node string) (DrainResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return DrainResult{}, fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.state != NodeMaintenance {
		return DrainResult{}, fmt.Errorf("%w: node %q is %s, not in maintenance", ErrInvalidState, node, n.state)
	}
	return s.drainLocked(node), nil
}

// drainLocked 执行一轮排空（调用方持锁）。规则：
//   - 维护节点上的非组作业：若存在放得下的健康节点则立即迁移（MIGRATE 释放 +
//     带 MIGRATED_MAINTENANCE 原因的 ALLOCATE 落地）；否则留在原地继续运行，
//     等待自然结束或下一轮排空；
//   - 维护节点上的组作业：整组一起停止——维护节点上的成员以 MIGRATE 释放，
//     其它健康节点上的同组成员以 GROUP_REQUEUE 释放，整组回到队列按原子
//     放置与公平次序重新竞争（要么整组再跑起来，要么整组排队）；
//   - 整个过程不影响新作业提交与其它队列的公平结论；批量模式下不在此处裁决，
//     由显式 Tick() 统一调度。
func (s *Scheduler) drainLocked(node string) DrainResult {
	n := s.nodes[node]

	// 按提交序确定性遍历节点上的作业（map 迭代顺序不可复现）。
	onNode := make([]*Job, 0, len(n.jobs))
	for id := range n.jobs {
		onNode = append(onNode, s.jobs[id])
	}
	sort.Slice(onNode, func(i, j int) bool { return onNode[i].SubmitSeq < onNode[j].SubmitSeq })

	var migrated, requeued []string
	handledGroups := make(map[string]bool)
	for _, j := range onNode {
		// 组内成员可能已随前一个成员的整组中断被移出该节点。
		if _, still := n.jobs[j.ID]; !still {
			continue
		}
		if j.IsGrouped() {
			if handledGroups[j.GroupID] {
				continue
			}
			handledGroups[j.GroupID] = true
			requeued = append(requeued, s.interruptGroupLocked(j.GroupID, node, causeMaintenance)...)
			continue
		}
		target := s.pickNodeLocked(j.Request)
		if target == nil {
			j.Reason = ReasonMaintenanceDrainPending
			j.Detail = fmt.Sprintf("maintenance: no healthy node fits %s; keeps running on %s until it finishes or next drain", j.Request, node)
			s.ledger.Note(EvDrain, j.ID, node, j.Queue, ReasonMaintenanceDrainPending,
				"drain pass: job cannot migrate yet; waiting on maintenance node")
			continue
		}
		s.migrateJobLocked(j, target)
		migrated = append(migrated, j.ID)
	}

	// 整组回到队列后，尝试在同一决策里按公平次序重新安置（批量模式除外）。
	s.maybeScheduleLocked()

	waiting := make([]string, 0, len(n.jobs))
	var inUse Resources
	for id := range n.jobs {
		waiting = append(waiting, id)
		inUse = inUse.Add(n.jobs[id])
	}
	sort.Strings(waiting)
	res := DrainResult{
		Node:     node,
		Migrated: migrated,
		Requeued: requeued,
		Waiting:  waiting,
		Drained:  len(n.jobs) == 0,
		Running:  len(n.jobs),
		InUse:    inUse,
	}
	s.ledger.Note(EvDrain, "", node, "", ReasonOK,
		fmt.Sprintf("drain pass: migrated=%d group-requeued=%d waiting=%d drained=%v",
			len(migrated), len(requeued), len(waiting), res.Drained))
	return res
}

// interruptCause 区分两类中断来源，保证流水与原因可审计。
type interruptCause string

const (
	causeMaintenance interruptCause = "maintenance"
	causeFailure     interruptCause = "failure"
)

// interruptGroupLocked 实现“整组一致中断”：触发节点上的成员与其它健康节点
// 上的同组成员一起释放占用、整组回到原队列排队。返回被重新排队的成员 ID
// （按提交序）。仅处理当前处于运行态的成员；已结束成员不再参与。
func (s *Scheduler) interruptGroupLocked(groupID, triggerNode string, cause interruptCause) []string {
	members := append([]*Job(nil), s.groups[groupID]...)
	sort.Slice(members, func(i, j int) bool { return members[i].SubmitSeq < members[j].SubmitSeq })

	var requeued []string
	for _, j := range members {
		if j.State != StateRunning {
			continue
		}
		oldNode := j.NodeName
		switch {
		case oldNode == triggerNode && cause == causeMaintenance:
			// 维护节点上的成员：占用按“迁移释放”记账，落地与否由随后的
			// 整组重调度决定；这里先整组回到队列。
			s.releaseLocked(j, EvMigrate, "maintenance: group member interrupted on draining node; whole group requeued")
		case oldNode == triggerNode && cause == causeFailure:
			s.releaseLocked(j, EvEvict, "node unhealthy: preempt group member; whole group requeued")
		default:
			// 健康节点上的同组成员：为保持整组一致而一起停止。
			s.releaseLocked(j, EvGroupRequeue, fmt.Sprintf("%s: peer group member stopped for consistent group requeue", cause))
		}
		j.State = StateQueued
		j.Reason = ReasonQueuedGroupInterrupted
		if cause == causeMaintenance {
			j.Detail = fmt.Sprintf("group %q interrupted by planned maintenance on %s; whole group requeued", groupID, triggerNode)
		} else {
			j.Detail = fmt.Sprintf("group %q interrupted by node failure on %s; whole group requeued", groupID, triggerNode)
		}
		// 回到原队列，按原提交序插入，SubmitSeq/年龄保留。
		s.enqueueLocked(j)
		if cause == causeMaintenance {
			s.ledger.Note(EvQueue, j.ID, oldNode, j.Queue, ReasonQueuedGroupInterrupted,
				"requeue after planned-maintenance group interruption")
		} else {
			s.ledger.Note(EvQueue, j.ID, oldNode, j.Queue, ReasonQueuedGroupInterrupted,
				"requeue after node-failure group interruption")
		}
		requeued = append(requeued, j.ID)
	}
	return requeued
}

// migrateJobLocked 把一个非组运行作业从维护节点迁到健康目标节点：
// MIGRATE 先从旧节点释放占用，ALLOCATE(MIGRATED_MAINTENANCE) 再落到新节点，
// 账目逐节点守恒；StartedAt 保留，视为同一次运行的延续而非重启。
func (s *Scheduler) migrateJobLocked(j *Job, target *Node) {
	started := j.StartedAt
	s.releaseLocked(j, EvMigrate, "maintenance: migrate job off draining node")
	s.placeLockedR(j, target, ReasonMigratedMaintenance, "maintenance: migration placement on healthy node")
	j.StartedAt = started
}

// DrainStatus 返回节点当前的排空判定（运维据此判断能否安全重启）。
// 对任意已注册节点均可查询；非维护节点返回其当前运行视图。
func (s *Scheduler) DrainStatus(node string) (NodeDrainStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return NodeDrainStatus{}, fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	ids := make([]string, 0, len(n.jobs))
	var inUse Resources
	for id, req := range n.jobs {
		ids = append(ids, id)
		inUse = inUse.Add(req)
	}
	sort.Slice(ids, func(a, b int) bool {
		return s.jobs[ids[a]].SubmitSeq < s.jobs[ids[b]].SubmitSeq
	})
	st := NodeDrainStatus{
		Node:      node,
		State:     n.state,
		Running:   len(n.jobs),
		InUse:     inUse,
		Available: n.available,
		JobIDs:    ids,
		Drained:   len(n.jobs) == 0,
	}
	switch n.state {
	case NodeMaintenance:
		if st.Drained {
			st.Reason = ReasonOK
			st.Detail = "node drained: no running jobs, safe to restart"
		} else {
			st.Reason = ReasonMaintenanceDrainPending
			st.Detail = fmt.Sprintf("node still running %d job(s); wait for completion or run another drain pass", len(ids))
		}
	case NodeUnhealthy:
		st.Reason = ReasonOK
		st.Detail = "node is unhealthy (failure); maintenance drain view not applicable"
	default:
		st.Reason = ReasonOK
		st.Detail = "node healthy and serving placements"
	}
	return st, nil
}

// CancelNodeMaintenance 取消维护：节点回到健康状态，重新参与调度，
// 仍在该节点上运行的作业继续运行（其维护标记被清除）；随后立即触发一轮
// 公平调度。维护取消不影响维护期间已迁走、已整组重排队的既有结论。
func (s *Scheduler) CancelNodeMaintenance(node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.state != NodeMaintenance {
		return fmt.Errorf("%w: node %q is %s, not in maintenance", ErrInvalidState, node, n.state)
	}
	n.state = NodeHealthy
	// 仍留在节点上的作业恢复为常规运行标记；available 从未被维护动作改写，
	// 账目无需调整（维护只排除放置，不改变物理容量）。
	ids := make([]string, 0, len(n.jobs))
	for id := range n.jobs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(a, b int) bool { return s.jobs[ids[a]].SubmitSeq < s.jobs[ids[b]].SubmitSeq })
	for _, id := range ids {
		j := s.jobs[id]
		j.Reason = ReasonOK
		j.Detail = ""
	}
	s.ledger.Note(EvNodeReady, "", node, "", ReasonOK, "maintenance cancelled; node back in service")
	s.maybeScheduleLocked()
	return nil
}
