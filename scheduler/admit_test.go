package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// heterogeneousCluster 构造异构集群：n1 是唯一装得下“大作业”（>4 CPU）的节点，
// n2/n3 只能承载小作业。用于复现“唯一可行节点处于维护/故障时提交被误拒”。
func heterogeneousCluster(t *testing.T) *Scheduler {
	t.Helper()
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1-big", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddNode("n2-small", Resources{CPU: 4, MemoryMB: 8000, GPU: 1}))
	mustOK(t, s.AddNode("n3-small", Resources{CPU: 4, MemoryMB: 8000, GPU: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "viz", Weight: 1}))
	return s
}

// TestSubmitAcceptedWhenOnlyFitNodeMaintenance：唯一装得下大作业的机器进入维护后，
// 提交必须被正常受理并排队（而不是 REJECT_INFEASIBLE）；维护期间绝不落到维护节点；
// 取消维护后排队作业按原次序被调度运行。
func TestSubmitAcceptedWhenOnlyFitNodeMaintenance(t *testing.T) {
	s := heterogeneousCluster(t)

	// 先把唯一的大节点 n1-big 拉进维护，再提交只有它装得下的作业（7 CPU）。
	mustOK(t, s.MarkNodeMaintenance("n1-big", "rolling upgrade"))

	big := s.Submit(Job{ID: "big", Queue: "batch", Request: Resources{CPU: 7, MemoryMB: 1000}})
	if !big.Accepted {
		t.Fatalf("submit while only-fit node drains must be accepted, got %s: %s", big.Reason, big.Detail)
	}
	if big.State != StateQueued || big.Reason != ReasonQueuedInsufficient {
		t.Fatalf("big should queue with QUEUED_INSUFFICIENT_RESOURCES, got state=%s reason=%s", big.State, big.Reason)
	}
	j, _ := s.GetJob("big")
	if j.NodeName != "" {
		t.Fatalf("queued job must not be placed, got node %s", j.NodeName)
	}

	// Tick 也不能把它塞到维护节点上。
	if n := s.Tick(); n != 0 {
		t.Fatalf("no placement possible while n1-big drains, tick placed %d", n)
	}
	assertState(t, s, "big", StateQueued)
	if j, _ := s.GetJob("big"); j.NodeName == "n1-big" {
		t.Fatalf("job must never be newly placed on draining node")
	}

	// 小节点同时是健康的：小作业照常立即运行，证明“维护只影响落点、不影响受理”。
	small := s.Submit(Job{ID: "small", Queue: "viz", Request: Resources{CPU: 3, MemoryMB: 1000}})
	if !small.Accepted || small.State != StateRunning {
		t.Fatalf("small job should run on a healthy node, got %+v", small)
	}
	if j, _ := s.GetJob("small"); j.NodeName == "n1-big" {
		t.Fatalf("small job must not land on draining n1-big, got %s", j.NodeName)
	}

	// 取消维护：排队作业必须被唤醒并跑在恢复的大节点上。
	mustOK(t, s.CancelMaintenance("n1-big"))
	assertState(t, s, "big", StateRunning)
	if j, _ := s.GetJob("big"); j.NodeName != "n1-big" {
		t.Fatalf("big should run on restored n1-big, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())
}

// TestSubmitAcceptedWhenOnlyFitNodeUnhealthy：唯一装得下大作业的机器被标记故障后，
// 提交同样必须受理并排队（而不是 REJECT_INFEASIBLE）；节点恢复后排队作业跑起来。
func TestSubmitAcceptedWhenOnlyFitNodeUnhealthy(t *testing.T) {
	s := heterogeneousCluster(t)

	// 占满两台小节点，使 holder 只能落在大节点；故障时它被驱逐且无处可迁而排队。
	for _, id := range []string{"fill2", "fill3"} {
		r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 1000}})
		if !r.Accepted || r.State != StateRunning {
			t.Fatalf("%s: %+v", id, r)
		}
	}
	holder := s.Submit(Job{ID: "holder", Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 1000}})
	if !holder.Accepted || holder.State != StateRunning {
		t.Fatalf("holder: %+v", holder)
	}
	if j, _ := s.GetJob("holder"); j.NodeName != "n1-big" {
		t.Fatalf("holder must land on n1-big (small nodes full), got %s", j.NodeName)
	}

	mustOK(t, s.MarkNodeUnhealthy("n1-big"))
	// 大节点故障：holder 被驱逐，小节点已满 => holder 排队；下面的新大作业也必须排队。
	assertState(t, s, "holder", StateQueued)

	big := s.Submit(Job{ID: "big", Queue: "batch", Request: Resources{CPU: 7, MemoryMB: 1000}})
	if !big.Accepted {
		t.Fatalf("submit while only-fit node unhealthy must be accepted, got %s: %s", big.Reason, big.Detail)
	}
	if big.State != StateQueued || big.Reason != ReasonQueuedInsufficient {
		t.Fatalf("big should queue, got state=%s reason=%s", big.State, big.Reason)
	}
	if j, _ := s.GetJob("big"); j.NodeName != "" {
		t.Fatalf("big must not be placed on an unhealthy node, got %s", j.NodeName)
	}
	for i := 0; i < 3; i++ {
		if n := s.Tick(); n != 0 && stateOf(s, "big") == StateRunning {
			t.Fatalf("big must stay queued while n1-big is unhealthy")
		}
		assertState(t, s, "big", StateQueued)
	}

	// 恢复：节点清空复位为总量，排队作业按原提交序被唤醒；
	// holder(1) 先落位，big(7) 随后恰好放入（1+7=8），都跑在恢复的大节点上。
	mustOK(t, s.MarkNodeHealthy("n1-big"))
	assertState(t, s, "holder", StateRunning)
	assertState(t, s, "big", StateRunning)
	if j, _ := s.GetJob("big"); j.NodeName != "n1-big" {
		t.Fatalf("big should run on recovered n1-big, got %s", j.NodeName)
	}
	if j, _ := s.GetJob("holder"); j.NodeName != "n1-big" {
		t.Fatalf("holder should run on recovered n1-big, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// stateOf 小工具：读取作业当前状态。
func stateOf(s *Scheduler, id string) JobState {
	j, _ := s.GetJob(id)
	return j.State
}

// TestRejectInfeasibleIgnoresNodeState：只有单机需求超过所有节点（无论健康、维护
// 还是故障）的单节点总量时才拒绝。边界两侧都要稳定：恰好等于最大节点总量必须受理
// （哪怕该节点在维护/故障，也要排队而非拒绝）；超过最大节点总量才 REJECT_INFEASIBLE。
func TestRejectInfeasibleIgnoresNodeState(t *testing.T) {
	s := heterogeneousCluster(t)

	// 全部节点都不可用于放置：大节点维护，两台小节点故障。
	mustOK(t, s.MarkNodeMaintenance("n1-big", "all down drill"))
	mustOK(t, s.MarkNodeUnhealthy("n2-small"))
	mustOK(t, s.MarkNodeUnhealthy("n3-small"))

	// 恰好等于最大节点（n1-big, 8 CPU）总量：虽然此刻无处可放，仍必须受理排队，
	// 因为维护取消后它是可行的。
	atLimit := s.Submit(Job{ID: "at-limit", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if !atLimit.Accepted || atLimit.State != StateQueued {
		t.Fatalf("request exactly == biggest node total must be accepted+queued, got %+v", atLimit)
	}

	// 超过所有节点单节点总量：永久不可行，必须拒绝（即使“所有节点都不可用”）。
	tooBig := s.Submit(Job{ID: "too-big", Queue: "batch", Request: Resources{CPU: 9, MemoryMB: 1}})
	if tooBig.Accepted || tooBig.State != StateRejected || tooBig.Reason != ReasonRejectInfeasible {
		t.Fatalf("request exceeding every node total must be REJECT_INFEASIBLE, got %+v", tooBig)
	}
	// 单维（GPU）超限同样拒绝：最大 GPU=2。
	tooManyGPU := s.Submit(Job{ID: "too-gpu", Queue: "batch", Request: Resources{CPU: 1, GPU: 3}})
	if tooManyGPU.Accepted || tooManyGPU.Reason != ReasonRejectInfeasible {
		t.Fatalf("GPU 3 exceeds every node (max 2): want REJECT_INFEASIBLE, got %+v", tooManyGPU)
	}

	// 取消维护后，恰好临界的排队作业正常跑起来（没有被错误拒绝、无残留卡死）。
	mustOK(t, s.CancelMaintenance("n1-big"))
	assertState(t, s, "at-limit", StateRunning)
	if j, _ := s.GetJob("at-limit"); j.NodeName != "n1-big" {
		t.Fatalf("at-limit should run on restored n1-big, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestConcurrentMaintenanceSubmitAdmitAndConservation：多台机器同时维护/故障、
// 维护与并发提交/取消交错进行时：
//   - 任何“单机需求不超过最大节点总量”的作业都不允许被 REJECT_INFEASIBLE 误拒
//     （只能立即运行或排队）；
//   - 账目始终守恒、无双重放置；
//   - 维护/故障节点上绝不出现“新分配”（只允许迁不走的既有单作业继续留在 DRAINING）；
//   - 全部恢复后，先前排队的作业按公平次序被调度，不残留卡死。
func TestConcurrentMaintenanceSubmitAdmitAndConservation(t *testing.T) {
	s := New(WithClock(fakeClock()))
	const maxCPU = 8
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		mustOK(t, s.AddNode(n, Resources{CPU: maxCPU, MemoryMB: 16000, GPU: 2}))
	}
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "viz", Weight: 1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var seq int64
	var badRejects int64

	// 提交者：作业大小 1..8 CPU，均不超过单节点总量 => 永远不该被 INFEASIBLE 拒绝。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := atomic.AddInt64(&seq, 1)
				cpu := 1 + int(n%maxCPU)
				queue := "batch"
				if w%2 == 0 {
					queue = "viz"
				}
				r := s.Submit(Job{ID: fmt.Sprintf("w%d-%06d", w, n), Queue: queue,
					Request: Resources{CPU: cpu, MemoryMB: 500}})
				if !r.Accepted && r.Reason == ReasonRejectInfeasible {
					atomic.AddInt64(&badRejects, 1)
				}
				i++
			}
		}(w)
	}

	// 维护者：n1/n2 反复进入/取消维护。
	wg.Add(1)
	go func() {
		defer wg.Done()
		nodes := []string{"n1", "n2"}
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			n := nodes[i%2]
			if i%2 == 0 {
				_ = s.MarkNodeMaintenance(n, "concurrent drain")
			} else {
				_ = s.CancelMaintenance(n)
			}
		}
	}()

	// 故障/恢复者：n3 反复故障/恢复（故障会立即驱逐，更考验受理与重排队）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				_ = s.MarkNodeUnhealthy("n3")
			} else {
				_ = s.MarkNodeHealthy("n3")
			}
		}
	}()

	// 取消者：不断取消部分排队作业，制造容量归还与状态切换交错。
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 1
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.Cancel(fmt.Sprintf("w%d-%06d", i%8, int64(i)))
			i++
		}
	}()

	// 巡检者：守恒 + 无双重放置 + 维护/故障节点无“新分配”。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := s.CheckConservation(); err != nil {
				t.Errorf("concurrent conservation: %v", err)
				return
			}
			if msg := findDoublePlacement(s); msg != "" {
				t.Errorf("%s", msg)
				return
			}
			if msg := findNewPlacementOnUnavailableNode(s); msg != "" {
				t.Errorf("%s", msg)
				return
			}
		}
	}()

	for k := 0; k < 500; k++ {
		s.Tick()
	}
	close(stop)
	wg.Wait()

	if atomic.LoadInt64(&badRejects) != 0 {
		t.Fatalf("feasible jobs were wrongly rejected REJECT_INFEASIBLE: %d", badRejects)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("final conservation: %v", err)
	}
	if msg := findDoublePlacement(s); msg != "" {
		t.Fatalf("%s", msg)
	}

	// 收尾：恢复全部非健康节点；随后逐轮完成运行作业释放容量并 Tick，
	// 先前排队的作业应全部按公平次序被调度出去（无残留卡死）。
	for _, n := range []string{"n1", "n2", "n3"} {
		if nn, ok := s.GetNode(n); ok {
			switch nn.State() {
			case NodeDraining:
				_ = s.CancelMaintenance(n)
			case NodeUnhealthy:
				_ = s.MarkNodeHealthy(n)
			}
		}
	}
	// 收尾排空：所有提交作业均 <= 单节点总量（maxCPU），节点全部健康后不存在
	// “永久不可行”。每轮先 Tick；无进展时完成一个运行作业释放容量；若已无运行
	// 作业（容量全空）而队头仍放不下，说明状态已损坏（理论上不可能），直接失败。
	maxNodeTotal := Resources{CPU: maxCPU, MemoryMB: 16000, GPU: 2}
	guard := 0
	for pendingCount(s) > 0 {
		guard++
		if guard > 200000 {
			t.Fatalf("drain stalled with %d jobs queued", pendingCount(s))
		}
		if s.Tick() > 0 {
			continue
		}
		if id := oneRunningJob(s); id != "" {
			_ = s.Complete(id)
			continue
		}
		// 无运行作业 => 全部健康节点均空闲。任何 <=maxNodeTotal 的请求都必须放得下，
		// 仍报告放不下只能是受理/状态错误。
		if id := blockedHeadJob(s); id != "" {
			j, _ := s.GetJob(id)
			t.Fatalf("accepted feasible job %s req=%s cannot fit any idle healthy node (total=%s)",
				id, j.Request, maxNodeTotal)
		}
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after recovery: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())
}

// pendingCount 统计所有队列中仍排队的作业数。
func pendingCount(s *Scheduler) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, q := range s.queues {
		n += len(q.pending)
	}
	return n
}

// oneRunningJob 返回任意一个运行中作业 ID（用于测试中释放容量），无则返回空串。
func oneRunningJob(s *Scheduler) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.State == StateRunning {
			return j.ID
		}
	}
	return ""
}

// blockedHeadJob 返回某个队列中“按队列内次序排第一、但任何健康节点当前空闲量
// 都放不下”的作业（队头阻塞候选），无则返回空串。
func blockedHeadJob(s *Scheduler) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range queueOrder(s.queues) {
		if len(q.pending) == 0 {
			continue
		}
		head := s.jobs[orderPending(q.pending, s.jobs, s.aging)[0]]
		fitsNow := false
		for _, name := range s.nodeOrder {
			n := s.nodes[name]
			if n.state == NodeHealthy && n.available.Fits(head.Request) {
				fitsNow = true
				break
			}
		}
		if !fitsNow {
			return head.ID
		}
	}
	return ""
}

// findNewPlacementOnUnavailableNode 校验非健康节点上不存在“新分配”：
// UNHEALTHY 节点必须无任何运行作业；DRAINING 节点上只允许有进入维护前就在、
// 且迁不走的单作业（StartedAt 早于进入维护时间），不允许组成员。
func findNewPlacementOnUnavailableNode(s *Scheduler) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.State != StateRunning {
			continue
		}
		n, ok := s.nodes[j.NodeName]
		if !ok {
			return fmt.Sprintf("job %s references missing node %q", j.ID, j.NodeName)
		}
		switch n.state {
		case NodeUnhealthy:
			return fmt.Sprintf("new placement: job %s runs on unhealthy node %s", j.ID, n.Name)
		case NodeDraining:
			if j.IsGrouped() {
				return fmt.Sprintf("new placement: group member %s runs on draining node %s", j.ID, n.Name)
			}
			if !j.StartedAt.Before(n.maintStarted) && !j.StartedAt.Equal(n.maintStarted) {
				return fmt.Sprintf("new placement: job %s started %s after node %s drained at %s",
					j.ID, j.StartedAt, n.Name, n.maintStarted)
			}
		}
	}
	return ""
}
