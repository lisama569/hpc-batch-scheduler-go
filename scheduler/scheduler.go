package scheduler

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Scheduler 是内存态集群调度器。所有可变状态均由单一 mutex 保护，
// 保证“同一时刻同一份资源不会被重复分配或泄漏”。
type Scheduler struct {
	mu sync.Mutex

	nodes  map[string]*Node
	queues map[string]*QueueRuntime
	jobs   map[string]*Job
	groups map[string][]*Job // groupID -> 成员（提交次序，含已结束作业以便解释）
	ledger *Ledger
	aging  agingPolicy

	submitSeq int // 单调提交序号
	tickCount int // 已完成的调度周期数（老化依据）
	now       func() time.Time
	// batchMode 为 true 时，Submit/SubmitGroup/取消/故障只更新状态入队，
	// 不立即触发调度；由显式 Tick() 统一裁决，便于按调度周期复现公平份额。
	batchMode bool

	nodeOrder []string // 节点名固定排序，保证放置可复现
}

// Option 用于可选配置。
type Option func(*Scheduler)

// WithClock 注入时钟（测试复现用）。
func WithClock(f func() time.Time) Option {
	return func(s *Scheduler) { s.now = f }
}

// WithAging 自定义老化参数（<=0 表示不老化）。
func WithAging(boostInterval, maxBoost int) Option {
	return func(s *Scheduler) { s.aging = agingPolicy{boostInterval: boostInterval, maxBoost: maxBoost} }
}

// New 创建空调度器。
func New(opts ...Option) *Scheduler {
	s := &Scheduler{
		nodes:  make(map[string]*Node),
		queues: make(map[string]*QueueRuntime),
		jobs:   make(map[string]*Job),
		groups: make(map[string][]*Job),
		ledger: NewLedger(),
		aging:  defaultAgingPolicy(),
		now:    time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// SetBatchMode 切换批量模式：开启后提交与状态变更不会立即触发调度，
// 需要由调用方显式 Tick() 统一裁决。必须在提交任何作业之前设置。
func (s *Scheduler) SetBatchMode(on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchMode = on
}

// maybeScheduleLocked 非批量模式下立即执行一轮公平调度。
func (s *Scheduler) maybeScheduleLocked() {
	if !s.batchMode {
		s.scheduleLocked()
	}
}

// AddNode 注册一个健康节点。重复名称返回错误。
func (s *Scheduler) AddNode(name string, total Resources) error {
	if !total.AnyPositive() {
		return fmt.Errorf("%w: node %q total resources must be positive", ErrInvalidState, name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[name]; ok {
		return fmt.Errorf("%w: node %q", ErrAlreadyExists, name)
	}
	n := &Node{
		Name:      name,
		Total:     total,
		available: total,
		state:     NodeHealthy,
		jobs:      make(map[string]Resources),
	}
	s.nodes[name] = n
	s.nodeOrder = append(s.nodeOrder, name)
	sort.Strings(s.nodeOrder)
	s.ledger.RegisterNode(name, total)
	return nil
}

// AddQueue 注册队列。
func (s *Scheduler) AddQueue(cfg QueueConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("%w: queue name required", ErrInvalidState)
	}
	if cfg.Weight < 0 {
		return fmt.Errorf("%w: queue %q weight must be >= 0", ErrInvalidState, cfg.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.queues[cfg.Name]; ok {
		return fmt.Errorf("%w: queue %q", ErrAlreadyExists, cfg.Name)
	}
	if cfg.Weight == 0 {
		cfg.Weight = 1
	}
	s.queues[cfg.Name] = &QueueRuntime{cfg: cfg}
	return nil
}

// validateRequestLocked 校验单个作业声明，返回拒绝原因（OK 表示通过）。
func (s *Scheduler) validateRequestLocked(j Job) (ReasonCode, string) {
	req := j.Request
	if req.CPU < 0 || req.MemoryMB < 0 || req.GPU < 0 {
		return ReasonRejectInvalid, fmt.Sprintf("negative resources in request %s", req)
	}
	if !req.AnyPositive() {
		return ReasonRejectInvalid, "requested resources are all zero"
	}
	if j.ID == "" {
		return ReasonRejectInvalid, "job id required"
	}
	if _, ok := s.jobs[j.ID]; ok {
		return ReasonRejectDuplicate, fmt.Sprintf("job id %q already exists", j.ID)
	}
	if _, ok := s.queues[j.Queue]; !ok {
		return ReasonRejectNoQueue, fmt.Sprintf("queue %q not found", j.Queue)
	}
	if j.GroupID != "" {
		if _, exists := s.groups[j.GroupID]; exists {
			return ReasonRejectGroupMismatch,
				fmt.Sprintf("group %q already submitted as a unit; add members via SubmitGroup", j.GroupID)
		}
	}
	// 可行性：集群中任意单个健康/曾经健康的最大节点都放不下 => 永远不可调度。
	feasible := false
	for _, n := range s.nodes {
		if n.Total.Fits(req) {
			feasible = true
			break
		}
	}
	if !feasible {
		return ReasonRejectInfeasible, fmt.Sprintf("request %s exceeds every node capacity", req)
	}
	// 队列在途上限
	if q := s.queues[j.Queue]; q.cfg.MaxJobs > 0 && q.running+len(q.pending) >= q.cfg.MaxJobs {
		return ReasonRejectQueueFull, fmt.Sprintf("queue %q reached MaxJobs=%d", j.Queue, q.cfg.MaxJobs)
	}
	return ReasonOK, ""
}

// admitLocked 给作业分配提交序号并登记（不做放置）。
func (s *Scheduler) admitLocked(j *Job) {
	s.submitSeq++
	j.SubmitSeq = s.submitSeq
	j.submitTick = s.tickCount
	j.SubmittedAt = s.now()
	j.State = StateQueued
	s.jobs[j.ID] = j
	if j.IsGrouped() {
		s.groups[j.GroupID] = append(s.groups[j.GroupID], j)
	}
}

// enqueueLocked 将作业加入其队列，按提交序保持 FIFO。
func (s *Scheduler) enqueueLocked(j *Job) {
	q := s.queues[j.Queue]
	q.pending = append(q.pending, j.ID)
	sort.SliceStable(q.pending, func(a, b int) bool {
		return s.jobs[q.pending[a]].SubmitSeq < s.jobs[q.pending[b]].SubmitSeq
	})
}

// Submit 提交一个作业。
func (s *Scheduler) Submit(j Job) SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if reason, detail := s.validateRequestLocked(j); reason != ReasonOK {
		s.ledger.Note(EvReject, j.ID, "", j.Queue, reason, detail)
		return SubmitResult{JobID: j.ID, Accepted: false, State: StateRejected, Reason: reason, Detail: detail}
	}
	jp := &j
	s.admitLocked(jp)

	// 组作业：单条提交视为“单成员原子组”；多成员作业组请使用 SubmitGroup
	// 整体提交，以保证“要么同时放置、要么一起排队”的原子语义。
	if jp.IsGrouped() {
		s.enqueueLocked(jp)
		jp.State = StateQueued
		s.maybeScheduleLocked()
		if jp.State == StateRunning {
			return SubmitResult{JobID: jp.ID, Accepted: true, State: StateRunning, Reason: ReasonOK,
				Detail: fmt.Sprintf("group %q member placed atomically on %s", jp.GroupID, jp.NodeName)}
		}
		detail := fmt.Sprintf("group %q cannot be satisfied as a whole; job stays queued", jp.GroupID)
		jp.Reason = ReasonQueuedGroupAtomic
		jp.Detail = detail
		s.ledger.Note(EvQueue, jp.ID, "", jp.Queue, ReasonQueuedGroupAtomic, detail)
		return SubmitResult{JobID: jp.ID, Accepted: true, State: StateQueued, Reason: ReasonQueuedGroupAtomic, Detail: detail}
	}

	// 普通作业：先进待调度视图，再由公平调度器统一决策，
	// 这样即使提交发生在资源归还之外，队列间份额也不会被“先到先得”绕过。
	s.enqueueLocked(jp)
	jp.State = StateQueued
	s.maybeScheduleLocked()
	if jp.State == StateRunning {
		return SubmitResult{JobID: jp.ID, Accepted: true, State: StateRunning, Reason: ReasonOK,
			Detail: fmt.Sprintf("allocated by fair scheduling on node %s", jp.NodeName)}
	}
	detail := fmt.Sprintf("no node currently has %s free; job stays queued", jp.Request)
	jp.Reason = ReasonQueuedInsufficient
	jp.Detail = detail
	s.ledger.Note(EvQueue, jp.ID, "", jp.Queue, ReasonQueuedInsufficient, detail)
	return SubmitResult{JobID: jp.ID, Accepted: true, State: StateQueued, Reason: ReasonQueuedInsufficient, Detail: detail}
}

// SubmitGroup 一次性提交一个作业组：任何成员非法 => 整组拒绝；
// 否则整组先进入待调度视图，再由公平调度器在同一决策里放置——
// 要么整组同时运行，要么整组排队（不允许部分占用）。
func (s *Scheduler) SubmitGroup(jobs []Job) []SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	results := make([]SubmitResult, len(jobs))
	if len(jobs) == 0 {
		return results
	}
	// 全部校验通过后才登记，保证“整组拒绝”不留任何痕迹。
	groupID := jobs[0].GroupID
	for i := range jobs {
		j := &jobs[i]
		if groupID == "" || j.GroupID != groupID {
			reason := ReasonRejectGroupMismatch
			detail := "every member must declare the same non-empty GroupID"
			s.ledger.Note(EvReject, j.ID, "", j.Queue, reason, detail)
			results[i] = SubmitResult{JobID: j.ID, Accepted: false, State: StateRejected, Reason: reason, Detail: detail}
			s.rejectGroupLocked(jobs, results)
			return results
		}
		reason, detail := s.validateRequestLocked(*j)
		if reason != ReasonOK {
			s.ledger.Note(EvReject, j.ID, "", j.Queue, reason, detail)
			results[i] = SubmitResult{JobID: j.ID, Accepted: false, State: StateRejected, Reason: reason, Detail: detail}
			s.rejectGroupLocked(jobs, results)
			return results
		}
	}

	// 登记全部成员并整体入队，随后统一调度（组在 scheduleLocked 中作为
	// 不可分割的候选处理，因此不会出现先放一个成员的情况）。
	members := make([]*Job, len(jobs))
	for i := range jobs {
		jp := &jobs[i]
		s.admitLocked(jp)
		members[i] = jp
		s.enqueueLocked(jp)
		jp.State = StateQueued
	}
	s.maybeScheduleLocked()

	allRun := true
	for _, jp := range members {
		if jp.State != StateRunning {
			allRun = false
			break
		}
	}
	if allRun {
		for i, jp := range members {
			results[i] = SubmitResult{JobID: jp.ID, Accepted: true, State: StateRunning, Reason: ReasonOK,
				Detail: fmt.Sprintf("group %q placed atomically on %s", groupID, jp.NodeName)}
		}
		return results
	}

	// 整组排队
	for i, jp := range members {
		detail := fmt.Sprintf("group %q (%d members) cannot be satisfied as a whole; queued", groupID, len(members))
		jp.Reason = ReasonQueuedGroupAtomic
		jp.Detail = detail
		if jp.State == StateQueued {
			s.ledger.Note(EvQueue, jp.ID, "", jp.Queue, ReasonQueuedGroupAtomic, detail)
		}
		results[i] = SubmitResult{JobID: jp.ID, Accepted: true, State: StateQueued, Reason: ReasonQueuedGroupAtomic, Detail: detail}
	}
	return results
}

// rejectGroupLocked 在发现首个非法成员后，将其余尚未写结论的成员标记为整组拒绝。
func (s *Scheduler) rejectGroupLocked(jobs []Job, results []SubmitResult) {
	for i := range results {
		if results[i].Reason != "" {
			continue
		}
		reason := ReasonRejectGroupMismatch
		detail := "group rejected atomically due to another invalid member"
		s.ledger.Note(EvReject, jobs[i].ID, "", jobs[i].Queue, reason, detail)
		results[i] = SubmitResult{JobID: jobs[i].ID, Accepted: false, State: StateRejected, Reason: reason, Detail: detail}
	}
}

// queuedGroupMembersLocked 返回某组当前排队中的成员（按提交序）；
// 已在运行/已结束的成员不参与原子放置。
func (s *Scheduler) queuedGroupMembersLocked(groupID string) []*Job {
	var out []*Job
	for _, m := range s.groups[groupID] {
		if m.State == StateQueued {
			out = append(out, m)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].SubmitSeq < out[j].SubmitSeq })
	return out
}

// planGroupLocked 尝试为组内所有成员在“同一份当前可用视图”上寻找放置方案。
// 采用逐成员 best-fit + 临时预留的方式：任何一个放不下即整体失败，绝不部分占用。
// 返回 jobID -> nodeName 的方案。
func (s *Scheduler) planGroupLocked(members []*Job) (map[string]string, bool) {
	plan := make(map[string]string, len(members))
	// 临时可用视图（不修改真实节点状态）
	avail := make(map[string]Resources, len(s.nodes))
	for name, n := range s.nodes {
		if n.state == NodeHealthy {
			avail[name] = n.available
		}
	}
	for _, m := range members {
		node := s.bestFitLocked(m.Request, avail)
		if node == "" {
			return nil, false
		}
		plan[m.ID] = node
		avail[node] = avail[node].Sub(m.Request) // 预留
	}
	return plan, true
}

// applyGroupPlanLocked 依据方案一次性落地整组分配。
func (s *Scheduler) applyGroupPlanLocked(members []*Job, plan map[string]string) {
	for _, m := range members {
		node := s.nodes[plan[m.ID]]
		detail := fmt.Sprintf("group %q atomic placement", m.GroupID)
		// 中断后整组重排的成员：在落地流水里保留可审计线索（原因仍为 OK，
		// 排队事件 QUEUED_GROUP_INTERRUPTED 已记录中断原因）。
		if m.Reason == ReasonQueuedGroupInterrupted {
			detail = fmt.Sprintf("group %q atomic re-placement after interruption", m.GroupID)
		}
		s.placeLocked(m, node, detail)
	}
}

// pickNodeLocked 为单个请求选择一个健康节点（best-fit），无可用返回 nil。
func (s *Scheduler) pickNodeLocked(req Resources) *Node {
	name := s.bestFitLocked(req, nil)
	if name == "" {
		return nil
	}
	return s.nodes[name]
}

// bestFitLocked 在健康节点中选“放置后剩余资源最少”的节点（碎片最小化），
// 平局完全由 nodeOrder（节点名升序）裁定，保证可复现。
// tempAvail 非空时表示组规划中的临时可用视图。
func (s *Scheduler) bestFitLocked(req Resources, tempAvail map[string]Resources) string {
	best := ""
	var bestLeft Resources
	for _, name := range s.nodeOrder {
		n := s.nodes[name]
		if n.state != NodeHealthy {
			continue
		}
		free := n.available
		if tempAvail != nil {
			free = tempAvail[name]
		}
		if !free.Fits(req) {
			continue
		}
		left := free.Sub(req)
		if best == "" || smallerFootprint(left, bestLeft) {
			best = name
			bestLeft = left
		}
	}
	return best
}

// smallerFootprint 以 (CPU, 内存, GPU) 的次序比较剩余足迹，越小越紧凑。
func smallerFootprint(a, b Resources) bool {
	if a.CPU != b.CPU {
		return a.CPU < b.CPU
	}
	if a.MemoryMB != b.MemoryMB {
		return a.MemoryMB < b.MemoryMB
	}
	return a.GPU < b.GPU
}

// canPlaceLocked 判断单个作业当前能否放到某健康节点。
func (s *Scheduler) canPlaceLocked(j *Job, n *Node) bool {
	return n.state == NodeHealthy && n.available.Fits(j.Request)
}

// placeLocked 在节点上放置作业、更新节点占用、队列计数并记账。
func (s *Scheduler) placeLocked(j *Job, n *Node, detail string) {
	s.placeLockedR(j, n, ReasonOK, detail)
}

// placeLockedR 是带显式分配原因的放置（ReasonMigratedMaintenance 用于
// 维护迁移落地，其余常规放置传 ReasonOK）。
func (s *Scheduler) placeLockedR(j *Job, n *Node, reason ReasonCode, detail string) {
	n.available = n.available.Sub(j.Request)
	n.jobs[j.ID] = j.Request
	j.State = StateRunning
	j.NodeName = n.Name
	j.StartedAt = s.now()
	j.Reason = reason
	q := s.queues[j.Queue]
	q.running++
	if reason == ReasonOK {
		s.ledger.Allocate(j.ID, n.Name, j.Queue, j.Request, detail)
	} else {
		s.ledger.AllocateR(j.ID, n.Name, j.Queue, j.Request, reason, detail)
	}
}

// removePendingLocked 从队列 pending 中删除作业。
func (s *Scheduler) removePendingLocked(j *Job) {
	q := s.queues[j.Queue]
	out := q.pending[:0]
	for _, id := range q.pending {
		if id != j.ID {
			out = append(out, id)
		}
	}
	q.pending = out
}

// releaseLocked 回收运行中作业占用，返回被释放的资源量。
// 事件类型决定账目流水与可审计原因：
//   - EvRelease：完成/取消，正常归还；
//   - EvEvict：突发故障强制驱逐；
//   - EvMigrate：计划维护，占用从维护节点释放以便迁移；
//   - EvGroupRequeue：整组一致性，健康节点上的同组成员随中断成员一起停止。
func (s *Scheduler) releaseLocked(j *Job, ev EventType, detail string) Resources {
	n := s.nodes[j.NodeName]
	req := n.jobs[j.ID]
	n.available = n.available.Add(req)
	delete(n.jobs, j.ID)
	j.NodeName = ""
	s.queues[j.Queue].running--
	switch ev {
	case EvEvict:
		s.ledger.Evict(j.ID, n.Name, j.Queue, req, detail)
	case EvMigrate:
		s.ledger.Migrate(j.ID, n.Name, j.Queue, req, detail)
	case EvGroupRequeue:
		s.ledger.GroupRequeue(j.ID, n.Name, j.Queue, req, detail)
	default:
		s.ledger.Release(j.ID, n.Name, j.Queue, req, detail)
	}
	return req
}

// Cancel 取消排队或运行中的作业。
func (s *Scheduler) Cancel(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %q", ErrNotFound, jobID)
	}
	switch j.State {
	case StateQueued:
		s.removePendingLocked(j)
		j.State = StateCancelled
		j.EndedAt = s.now()
		s.ledger.Note(EvCancel, j.ID, "", j.Queue, ReasonOK, "cancel: removed from queue")
	case StateRunning:
		s.releaseLocked(j, EvRelease, "cancel: release running allocation")
		j.State = StateCancelled
		j.EndedAt = s.now()
	default:
		return fmt.Errorf("%w: job %q is %s", ErrInvalidState, jobID, j.State)
	}
	s.maybeScheduleLocked() // 资源归还后唤醒排队作业
	return nil
}

// Complete 标记运行中作业完成并释放资源。
func (s *Scheduler) Complete(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return fmt.Errorf("%w: job %q", ErrNotFound, jobID)
	}
	if j.State != StateRunning {
		return fmt.Errorf("%w: job %q is %s, expected RUNNING", ErrInvalidState, jobID, j.State)
	}
	s.releaseLocked(j, EvRelease, "complete: release allocation")
	j.State = StateCompleted
	j.EndedAt = s.now()
	s.ledger.Note(EvComplete, j.ID, "", j.Queue, ReasonOK, "complete")
	s.maybeScheduleLocked()
	return nil
}

// MarkNodeUnhealthy 标记节点不健康（突发故障）：释放全部运行作业占用（原子
// 记账），被中断作业回到原队列，保持原提交序号与年龄，随后触发一轮重调度。
//
// 整组一致性：若故障节点上有作业组成员，则该组所有运行成员——包括跑在健康
// 节点上的成员——都一起停止、整组回到队列（成员间不再出现一部分在跑、
// 一部分排队）；故障成员走 EVICT，健康成员走 GROUP_REQUEUE，便于审计区分。
// 计划维护中的节点若突发故障，同样按故障语义强制处理（维护语义被故障取代）。
func (s *Scheduler) MarkNodeUnhealthy(node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.state == NodeUnhealthy {
		return fmt.Errorf("%w: node %q already unhealthy", ErrInvalidState, node)
	}
	wasMaintenance := n.state == NodeMaintenance
	n.state = NodeUnhealthy
	if wasMaintenance {
		s.ledger.Note(EvNodeDown, "", node, "", ReasonOK, "node marked unhealthy while in maintenance; failure semantics apply")
	} else {
		s.ledger.Note(EvNodeDown, "", node, "", ReasonOK, "node marked unhealthy")
	}

	// 收集节点上的作业并按提交序处理（map 迭代乱序，必须显式排序以复现）。
	var onNode []*Job
	for id := range n.jobs {
		onNode = append(onNode, s.jobs[id])
	}
	sort.Slice(onNode, func(i, j int) bool { return onNode[i].SubmitSeq < onNode[j].SubmitSeq })

	handledGroups := make(map[string]bool)
	for _, j := range onNode {
		// 组内其它成员可能已在处理前一个成员时把该作业一并移出本节点。
		if _, still := n.jobs[j.ID]; !still {
			continue
		}
		if j.IsGrouped() {
			if handledGroups[j.GroupID] {
				continue
			}
			handledGroups[j.GroupID] = true
			// 整组（含健康节点上的同组作业）一起停止并重新排队。
			s.interruptGroupLocked(j.GroupID, node, causeFailure)
			continue
		}
		// 非组作业：故障驱逐，占用释放后回到原队列等待迁移。
		s.releaseLocked(j, EvEvict, "node unhealthy: preempt and requeue")
		j.State = StateQueued
		j.Reason = ReasonQueuedInsufficient
		j.Detail = fmt.Sprintf("evicted from unhealthy node %s; requeued", node)
		s.enqueueLocked(j)
		s.ledger.Note(EvQueue, j.ID, node, j.Queue, ReasonQueuedInsufficient, "requeue after eviction")
	}
	// 故障后该节点 available 理论上为零（全部释放）；保持 available 字段不变，
	// 调度时因 state=Unhealthy 不会选用它。
	s.maybeScheduleLocked()
	return nil
}

// MarkNodeHealthy 恢复故障节点健康，使其重新可被调度，并立即尝试调度。
// 计划维护的取消必须使用 CancelNodeMaintenance（维护节点上可能仍有作业，
// 不能像故障恢复那样把可用量复位为总量）。
func (s *Scheduler) MarkNodeHealthy(node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[node]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.state == NodeHealthy {
		return fmt.Errorf("%w: node %q already healthy", ErrInvalidState, node)
	}
	if n.state == NodeMaintenance {
		return fmt.Errorf("%w: node %q is in maintenance; use CancelNodeMaintenance instead", ErrInvalidState, node)
	}
	n.state = NodeHealthy
	// 恢复时节点上应无作业（故障时已全部驱逐），可用量即总量。
	n.available = n.Total
	s.ledger.Note(EvNodeUp, "", node, "", ReasonOK, "node recovered healthy")
	s.maybeScheduleLocked()
	return nil
}

// Tick 推进一个调度周期：老化 + 公平重调度，返回本轮发生的分配数。
func (s *Scheduler) Tick() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickCount++
	tickAging(s.queues, s.jobs, s.tickCount, s.aging)
	n := s.scheduleLocked()
	return n
}

// scheduleLocked 执行一轮加权轮转公平调度（调用方持锁）。
// 每个队列按 virtual 最小优先被服务，每轮预算等于权重；队头候选放不下时，
// 该队列本轮阻塞（避免大作业/整组阻塞后方作业时拖垮其他队列）。
func (s *Scheduler) scheduleLocked() int {
	totalWeight := 0
	for _, q := range s.queues {
		w := q.cfg.Weight
		if w < 1 {
			w = 1
		}
		totalWeight += w
	}

	placed := 0
	// 每轮在非空队列间按 virtual 选择，服务预算等于权重。
	// 若某个队列队头当前放不下，则本轮先跳过它服务其他队列；
	// 只要整个扫描过程中发生过新的放置，就重新开始一轮扫描——
	// 因为新的放置不会释放资源，但其他队列在同一次 scheduleLocked
	// 调用期间若有外部状态变化（例如组批量入队），容量视图需要重算。
	// 终止条件：连续一轮所有非空队列都无法放置任何作业。
	for {
		progress := false
		// 每轮按 budget 服务；队列在该轮已获得其权重份额后本拍不再重复，
		// 避免单个队列在一个 scheduleLocked 中无限吃满所有释放出来的槽位
		// （份额在多个 Tick 间通过 virtual 追平）。
		servedThisPass := map[string]int{}
		for {
			q := s.pickNextQueueWithinBudgetLocked(servedThisPass)
			if q == nil {
				break
			}
			candidate := s.nextCandidateLocked(q)
			if candidate == nil {
				break
			}
			if candidate.IsGrouped() {
				members := s.queuedGroupMembersLocked(candidate.GroupID)
				plan, ok := s.planGroupLocked(members)
				if !ok {
					// 当前放不下：耗尽该队列本轮预算，下一轮扫描再试。
					servedThisPass[q.cfg.Name] = serveBudget(q)
					continue
				}
				s.applyGroupPlanLocked(members, plan)
				for _, m := range members {
					s.removePendingLocked(m)
				}
				servedThisPass[q.cfg.Name]++
				markServed(q, totalWeight, 1)
				placed += len(members)
				progress = true
				continue
			}
			node := s.pickNodeLocked(candidate.Request)
			if node == nil {
				servedThisPass[q.cfg.Name] = serveBudget(q)
				continue
			}
			s.placeLocked(candidate, node, "scheduler: fair placement")
			s.removePendingLocked(candidate)
			servedThisPass[q.cfg.Name]++
			markServed(q, totalWeight, 1)
			placed++
			progress = true
		}
		if !progress {
			break
		}
	}
	return placed
}

// pickNextQueueWithinBudgetLocked 选择 virtual 最小、且本轮服务次数未达
// 权重预算、仍有待处理作业的队列。
func (s *Scheduler) pickNextQueueWithinBudgetLocked(served map[string]int) *QueueRuntime {
	var best *QueueRuntime
	for _, q := range queueOrder(s.queues) {
		if len(q.pending) == 0 {
			continue
		}
		if served[q.cfg.Name] >= serveBudget(q) {
			continue
		}
		if best == nil || q.virtual < best.virtual ||
			(q.virtual == best.virtual && q.cfg.Name < best.cfg.Name) {
			best = q
		}
	}
	return best
}

// nextCandidateLocked 取队列当前应考虑的候选作业（有效优先级降序、提交序升序），
// 但不将其从 pending 移除。
func (s *Scheduler) nextCandidateLocked(q *QueueRuntime) *Job {
	if len(q.pending) == 0 {
		return nil
	}
	ordered := orderPending(q.pending, s.jobs, s.aging)
	return s.jobs[ordered[0]]
}

// CheckConservation 对外暴露账目守恒校验：对每个节点 已分配+可用==总量。
func (s *Scheduler) CheckConservation() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	avail := make(map[string]Resources, len(s.nodes))
	for name, n := range s.nodes {
		avail[name] = n.available
	}
	if err := s.ledger.CheckConservation(avail); err != nil {
		return err
	}
	// 交叉核对：节点视图与队列计数一致（运行作业数、节点作业表）。
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		n, ok := s.nodes[j.NodeName]
		if !ok {
			return fmt.Errorf("running job %s references missing node %q", j.ID, j.NodeName)
		}
		if _, ok := n.jobs[j.ID]; !ok {
			return fmt.Errorf("running job %s missing from node %s allocation table", j.ID, n.Name)
		}
	}
	return nil
}

// Ledger 暴露流水（只读使用）。
func (s *Scheduler) Ledger() *Ledger { return s.ledger }

// GetJob 查询作业副本。
func (s *Scheduler) GetJob(id string) (Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *j, true
}

// Snapshot 返回可打印的集群状态，用于解释调度判定。
func (s *Scheduler) Snapshot() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "=== cluster snapshot @tick=%d seq=%d ===\n", s.tickCount, s.submitSeq)
	for _, name := range s.nodeOrder {
		n := s.nodes[name]
		fmt.Fprintf(&b, "node %s [%s] total=%s available=%s jobs=%d\n",
			name, n.state, n.Total, n.available, len(n.jobs))
		ids := make([]string, 0, len(n.jobs))
		for id := range n.jobs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			fmt.Fprintf(&b, "    - %s uses %s\n", id, n.jobs[id])
		}
	}
	for _, q := range queueOrder(s.queues) {
		fmt.Fprintf(&b, "queue %s weight=%d priority=%d running=%d pending=%d virtual=%d\n",
			q.cfg.Name, q.cfg.Weight, q.cfg.Priority, q.running, len(q.pending), q.virtual)
		for _, id := range orderPending(q.pending, s.jobs, s.aging) {
			j := s.jobs[id]
			fmt.Fprintf(&b, "    #%d %s req=%s prio=%d eff=%d reason=%s\n",
				j.SubmitSeq, j.ID, j.Request, j.Priority, effectivePriority(j, s.aging), j.Reason)
		}
	}
	return b.String()
}
