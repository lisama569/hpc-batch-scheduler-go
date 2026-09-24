package scheduler

import (
	"fmt"
	"sort"
	"sync"
)

// SubmitResult 是提交结果：Accepted=false 时 Reason 为拒绝原因；
// Accepted=true 时作业已进入排队（可能因资源不足等待）。
type SubmitResult struct {
	Accepted bool
	Reason   RejectReason
}

// Scheduler 管理队列、待调度作业与资源账目。
type Scheduler struct {
	mu        sync.Mutex
	ledger    *Ledger
	queues    map[string]Queue
	pending   []*Job // 等待队列（含作业组成员）
	states    map[string]JobState
	running   map[string]*Job // RUNNING 作业
	seq       int64
	decisions []string // 判定依据日志
}

// NewScheduler 创建调度器。
func NewScheduler(nodes []Node, queues []Queue) *Scheduler {
	s := &Scheduler{
		ledger:  NewLedger(nodes),
		queues:  make(map[string]Queue, len(queues)),
		states:  make(map[string]JobState),
		running: make(map[string]*Job),
	}
	for _, q := range queues {
		s.queues[q.Name] = q
	}
	return s
}

// Submit 提交作业。
// 拒绝（永久不可行）：队列不存在、声明非法、单作业需求超过集群总容量。
// 接受：作业进入排队；资源不足时保持等待，原因在调度判定日志中给出。
func (s *Scheduler) Submit(j Job) SubmitResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if j.ID == "" {
		s.decide("SUBMIT_REJECT job=<empty> reason=%s", RejectInvalid)
		return SubmitResult{false, RejectInvalid}
	}
	if j.Request.CPUMilli < 0 || j.Request.MemMiB < 0 || j.Request.GPU < 0 {
		s.decide("SUBMIT_REJECT job=%s reason=%s (negative resource)", j.ID, RejectInvalid)
		return SubmitResult{false, RejectInvalid}
	}
	if _, ok := s.queues[j.Queue]; !ok {
		s.decide("SUBMIT_REJECT job=%s reason=%s queue=%s", j.ID, RejectUnknownQueue, j.Queue)
		return SubmitResult{false, RejectUnknownQueue}
	}
	if _, dup := s.states[j.ID]; dup {
		s.decide("SUBMIT_REJECT job=%s reason=%s (duplicate id)", j.ID, RejectInvalid)
		return SubmitResult{false, RejectInvalid}
	}
	// 永久不可行判定：需求超过集群全部节点（含暂时不健康节点）的总容量。
	if !j.Request.Fits(s.clusterCapacityLocked()) {
		s.decide("SUBMIT_REJECT job=%s reason=%s request=%s cluster=%s",
			j.ID, RejectExceedsCluster, j.Request, s.clusterCapacityLocked())
		return SubmitResult{false, RejectExceedsCluster}
	}

	j.SubmitSeq = s.seq
	s.seq++
	j.Age = 0
	jp := j
	s.pending = append(s.pending, &jp)
	s.states[j.ID] = JobPending
	s.decide("SUBMIT_ACCEPT job=%s queue=%s prio=%d group=%s request=%s seq=%d",
		j.ID, j.Queue, j.Priority, blankGroup(j.GroupID), j.Request, j.SubmitSeq)
	return SubmitResult{true, RejectNone}
}

func blankGroup(g string) string {
	if g == "" {
		return "-"
	}
	return g
}

// Cancel 取消作业：排队中则移除，运行中则回收资源。
func (s *Scheduler) Cancel(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.states[jobID] {
	case JobPending:
		s.removePendingLocked(jobID)
		s.states[jobID] = JobCancelled
		s.decide("CANCEL job=%s state=PENDING", jobID)
		return true
	case JobRunning:
		n := s.ledger.ReleaseJob(jobID)
		delete(s.running, jobID)
		s.states[jobID] = JobCancelled
		s.decide("CANCEL job=%s state=RUNNING released_records=%d", jobID, n)
		return true
	default:
		return false
	}
}

// NodeDown 处理节点故障：回收资源；受影响的作业（gang 作业则整组）重新排队。
// 返回被中断并重新排队的作业 ID 列表（按字典序）。
func (s *Scheduler) NodeDown(nodeID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	affected := s.ledger.NodeDown(nodeID)
	requeue := map[string]bool{}
	for _, id := range affected {
		j, ok := s.running[id]
		if !ok {
			continue
		}
		requeue[id] = true
		// gang 作业必须整体生存：同组仍在运行的成员一并回收、重排队。
		if j.GroupID != "" {
			for _, other := range s.running {
				if other.GroupID == j.GroupID {
					requeue[other.ID] = true
				}
			}
		}
	}
	ids := make([]string, 0, len(requeue))
	for id := range requeue {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		j := s.running[id]
		// ledger.NodeDown 只回收了故障节点上的记录，其余成员先显式回收。
		s.ledger.ReleaseJob(id)
		delete(s.running, id)
		j.Age = 0
		s.pending = append(s.pending, j)
		s.states[id] = JobPending
		s.decide("FAULT_REQUEUE job=%s group=%s node=%s", id, blankGroup(j.GroupID), nodeID)
	}
	if len(ids) == 0 {
		s.decide("NODE_DOWN node=%s no_running_jobs", nodeID)
	}
	return ids
}

// NodeUp 恢复节点，资源归还后等待中的作业可在下一轮被调度。
func (s *Scheduler) NodeUp(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ledger.NodeUp(nodeID)
	s.decide("NODE_UP node=%s", nodeID)
}

// ScheduleOnce 执行一轮调度，返回本轮成功开始运行的作业 ID。
// 一轮内按“队列加权份额 → 队列内防饿死优先级/提交序”多趟尝试，直到无进展。
func (s *Scheduler) ScheduleOnce() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	scheduled := []string{}

	// 每次扫描：按当前实时加权份额排列队列，从份额最低的队列取出队首
	// 可调度候选，全局只启动一个作业（或作业组），随后立刻重算份额。
	// 若整轮扫描没有任何候选可以启动，则记录所有 WAIT 判定并结束本轮。
	groupsFailed := map[string]bool{} // 本轮已失败的组无需重试（资源只会更紧）
	for {
		qOrder := orderQueues(s.queueViewsLocked())
		progressed := false
		waits := []string{}

		for _, qn := range qOrder {
			jobs := make([]*Job, 0)
			for _, j := range s.pending {
				if j.Queue == qn && s.states[j.ID] == JobPending {
					jobs = append(jobs, j)
				}
			}
			sortPending(jobs)

			done := false
			groupsSeen := map[string]bool{}
			for _, j := range jobs {
				if j.GroupID != "" {
					if groupsSeen[j.GroupID] || groupsFailed[j.GroupID] {
						continue
					}
					groupsSeen[j.GroupID] = true
					if ids, ok := s.tryGangLocked(j.GroupID); ok {
						scheduled = append(scheduled, ids...)
						progressed, done = true, true
					} else {
						groupsFailed[j.GroupID] = true
					}
					// 组暂不可放置时继续看同队列后续候选（小作业回填）。
					if done {
						break
					}
					continue
				}
				if node, ok := s.ledger.Allocate(j.ID, j.Request); ok {
					s.startLocked(j, node)
					scheduled = append(scheduled, j.ID)
					s.decide("SCHEDULE job=%s queue=%s eff_prio=%d node=%s",
						j.ID, j.Queue, j.Priority+int(ageBoost(j.Age)), node)
					progressed, done = true, true
					break // 全局只出一个，立即重算份额
				}
				waits = append(waits, fmt.Sprintf(
					"WAIT job=%s queue=%s reason=%s request=%s free=%s",
					j.ID, j.Queue, WaitInsufficientResource, j.Request, s.ledger.Free()))
			}
			if done {
				break
			}
		}
		if !progressed {
			s.decisions = append(s.decisions, waits...)
			break
		}
	}

	// 本轮结束仍在排队的作业累积“年龄”，跨轮防饿死。
	for _, j := range s.pending {
		j.Age++
	}
	return scheduled
}

// tryGangLocked 尝试原子放置整个作业组：全部成员分配成功才提交，
// 任一失败则回滚已完成的分配，绝不部分占用。
func (s *Scheduler) tryGangLocked(groupID string) ([]string, bool) {
	members := make([]*Job, 0)
	for _, j := range s.pending {
		if j.GroupID == groupID && s.states[j.ID] == JobPending {
			members = append(members, j)
		}
	}
	sort.SliceStable(members, func(i, j int) bool {
		if members[i].SubmitSeq != members[j].SubmitSeq {
			return members[i].SubmitSeq < members[j].SubmitSeq
		}
		return members[i].ID < members[j].ID
	})
	sum := Resource{}
	for _, m := range members {
		sum = sum.Add(m.Request)
	}
	free := s.ledger.Free()
	reason := WaitGroupNotReady
	if !sum.Fits(free) {
		reason = WaitInsufficientResource
	}
	// 先完成全部分配，再统一提交状态；任一失败则回滚账面上的分配，
	// 调度器状态（running/states/pending）尚未变更，无需回滚。
	committed := []string{}
	for _, m := range members {
		_, ok := s.ledger.Allocate(m.ID, m.Request)
		if !ok {
			for _, id := range committed {
				s.ledger.ReleaseJob(id)
			}
			s.decide("WAIT group=%s reason=%s members=%d group_need=%s free=%s rollback=%d",
				groupID, reason, len(members), sum, free, len(committed))
			return nil, false
		}
		committed = append(committed, m.ID)
	}
	for _, m := range members {
		s.startLocked(m, "")
	}
	ids := make([]string, len(members))
	for i, m := range members {
		ids[i] = m.ID
	}
	sort.Strings(ids)
	s.decide("SCHEDULE_GROUP group=%s members=%v group_need=%s", groupID, ids, sum)
	return ids, true
}

// queueViewsLocked 计算各队列当前的加权公平份额视图。
func (s *Scheduler) queueViewsLocked() []queueView {
	total := s.ledger.Total()
	usedByQ := map[string]Resource{}
	for _, j := range s.running {
		usedByQ[j.Queue] = usedByQ[j.Queue].Add(j.Request)
	}
	views := make([]queueView, 0, len(s.queues))
	for name, q := range s.queues {
		views = append(views, queueView{name: name, weight: q.Weight, share: dominantShare(usedByQ[name], total)})
	}
	return views
}

func (s *Scheduler) startLocked(j *Job, node string) {
	s.removePendingLocked(j.ID)
	s.running[j.ID] = j
	s.states[j.ID] = JobRunning
	_ = node
}

func (s *Scheduler) removePendingLocked(id string) {
	out := s.pending[:0]
	for _, j := range s.pending {
		if j.ID != id {
			out = append(out, j)
		}
	}
	s.pending = out
}

func (s *Scheduler) clusterCapacityLocked() Resource {
	// 准入判定使用全部节点（含暂时不健康）的容量：
	// 节点故障是暂时状态，恢复后作业仍可被调度，不应被永久拒绝。
	return s.ledger.CapacityAll()
}

// State 返回作业当前状态。
func (s *Scheduler) State(jobID string) (JobState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[jobID]
	return st, ok
}

// PendingIDs 返回当前排队作业 ID（按提交顺序）。
func (s *Scheduler) PendingIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.pending))
	for _, j := range s.pending {
		out = append(out, j.ID)
	}
	return out
}

// RunningIDs 返回当前运行作业 ID（按字典序）。
func (s *Scheduler) RunningIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.running))
	for id := range s.running {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Ledger 暴露账目供校验与审计。
func (s *Scheduler) Ledger() *Ledger { return s.ledger }

// Decisions 返回判定依据日志副本。
func (s *Scheduler) Decisions() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.decisions))
	copy(out, s.decisions)
	return out
}

func (s *Scheduler) decide(format string, args ...any) {
	s.decisions = append(s.decisions, fmt.Sprintf(format, args...))
}
