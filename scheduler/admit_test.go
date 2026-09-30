package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// clusterForAdmission 构造两台异构节点：n-big 是唯一装得下 12-CPU 大作业的
// 机器（16 CPU），n-small 只能装 8-CPU 作业。所有用例围绕“唯一装得下的
// 机器不可用时，提交是否仍被受理”展开。
func clusterForAdmission(t *testing.T) *Scheduler {
	t.Helper()
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n-big", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddNode("n-small", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
	return s
}

func bigJob(id string) Job {
	return Job{ID: id, Queue: "batch", Request: Resources{CPU: 12, MemoryMB: 20000, GPU: 3}}
}

// TestSubmitAcceptedQueuedWhenOnlyFittingNodeInMaintenance：
// 唯一装得下作业的机器处于计划维护时，提交必须被接收并排队，
// 取消维护后作业按原次序跑起来，且维护期间绝不落到维护节点上。
func TestSubmitAcceptedQueuedWhenOnlyFittingNodeInMaintenance(t *testing.T) {
	s := clusterForAdmission(t)

	mustOK(t, s.MarkNodeMaintenance("n-big", "rolling upgrade"))

	// 健康的 n-small 只有 8C/16000MB/2GPU，装不下 12C/20000MB/3GPU 的大作业。
	r := s.Submit(bigJob("big1"))
	if !r.Accepted {
		t.Fatalf("submit while only fitting node is draining must be accepted, got reject reason=%s detail=%s", r.Reason, r.Detail)
	}
	if r.State != StateQueued || r.Reason != ReasonQueuedInsufficient {
		t.Fatalf("big1 should be QUEUED/QUEUED_INSUFFICIENT_RESOURCES, got state=%s reason=%s", r.State, r.Reason)
	}
	assertState(t, s, "big1", StateQueued)
	j, _ := s.GetJob("big1")
	if j.NodeName == "n-big" {
		t.Fatalf("queued job must never be placed on draining node")
	}
	// 再多调度几轮也不应偷偷落到维护节点上。
	if n := s.Tick(); n != 0 {
		t.Fatalf("no placement possible while n-big is draining, placed=%d", n)
	}
	assertState(t, s, "big1", StateQueued)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation while queued: %v", err)
	}

	// 取消维护：排队作业立即在 n-big 上跑起来。
	mustOK(t, s.CancelMaintenance("n-big"))
	assertState(t, s, "big1", StateRunning)
	j, _ = s.GetJob("big1")
	if j.NodeName != "n-big" {
		t.Fatalf("big1 should run on restored n-big, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after maintenance cancel: %v", err)
	}
}

// TestSubmitAcceptedQueuedWhenOnlyFittingNodeUnhealthy：
// 唯一装得下作业的机器突发故障时，提交同样必须被接收并排队，
// 节点恢复后作业跑起来。
func TestSubmitAcceptedQueuedWhenOnlyFittingNodeUnhealthy(t *testing.T) {
	s := clusterForAdmission(t)

	mustOK(t, s.MarkNodeUnhealthy("n-big"))

	r := s.Submit(bigJob("big1"))
	if !r.Accepted {
		t.Fatalf("submit while only fitting node is unhealthy must be accepted, got reject reason=%s detail=%s", r.Reason, r.Detail)
	}
	if r.State != StateQueued || r.Reason != ReasonQueuedInsufficient {
		t.Fatalf("big1 should be QUEUED/QUEUED_INSUFFICIENT_RESOURCES, got state=%s reason=%s", r.State, r.Reason)
	}
	if n := s.Tick(); n != 0 {
		t.Fatalf("no placement possible while n-big is unhealthy, placed=%d", n)
	}
	assertState(t, s, "big1", StateQueued)

	mustOK(t, s.MarkNodeHealthy("n-big"))
	assertState(t, s, "big1", StateRunning)
	j, _ := s.GetJob("big1")
	if j.NodeName != "n-big" {
		t.Fatalf("big1 should run on recovered n-big, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after recovery: %v", err)
	}
}

// TestSubmitRejectedWhenExceedsEveryNodeTotal：无论节点是什么状态，
// 只有单机需求确实超过所有节点（含维护中、故障中）的单节点总量时才拒绝。
func TestSubmitRejectedWhenExceedsEveryNodeTotal(t *testing.T) {
	s := clusterForAdmission(t)

	// 全部节点健康时超过最大单节点容量 => 拒绝。
	r := s.Submit(Job{ID: "too-big", Queue: "batch",
		Request: Resources{CPU: 17, MemoryMB: 1000, GPU: 1}})
	if r.Accepted || r.Reason != ReasonRejectInfeasible {
		t.Fatalf("17 CPU exceeds every node total, want REJECT_INFEASIBLE, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}

	// 各维分别超限也要拒绝（内存维超过 n-big 总量）。
	r = s.Submit(Job{ID: "too-big-mem", Queue: "batch",
		Request: Resources{CPU: 1, MemoryMB: 32001}})
	if r.Accepted || r.Reason != ReasonRejectInfeasible {
		t.Fatalf("32001MB exceeds every node total, want REJECT_INFEASIBLE, got reason=%s", r.Reason)
	}
	// GPU 维超限。
	r = s.Submit(Job{ID: "too-big-gpu", Queue: "batch",
		Request: Resources{CPU: 1, MemoryMB: 1000, GPU: 5}})
	if r.Accepted || r.Reason != ReasonRejectInfeasible {
		t.Fatalf("5 GPU exceeds every node total, want REJECT_INFEASIBLE, got reason=%s", r.Reason)
	}

	// 把唯一的大机器置为维护/故障，超过它总量的请求依旧必须拒绝
	//（节点状态不改变物理可行性边界）。
	mustOK(t, s.MarkNodeMaintenance("n-big", "border check"))
	r = s.Submit(Job{ID: "too-big-2", Queue: "batch",
		Request: Resources{CPU: 17, MemoryMB: 1000}})
	if r.Accepted || r.Reason != ReasonRejectInfeasible {
		t.Fatalf("infeasible request must stay rejected while big node drains, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}

	mustOK(t, s.CancelMaintenance("n-big"))
	mustOK(t, s.MarkNodeUnhealthy("n-big"))
	r = s.Submit(Job{ID: "too-big-3", Queue: "batch",
		Request: Resources{CPU: 17, MemoryMB: 1000}})
	if r.Accepted || r.Reason != ReasonRejectInfeasible {
		t.Fatalf("infeasible request must stay rejected while big node is unhealthy, got accepted=%v reason=%s", r.Accepted, r.Reason)
	}

	// 被拒绝的作业不得留下任何在途痕迹（不进作业排队统计、不占账目）。
	if _, ok := s.GetJob("too-big-3"); ok {
		t.Fatalf("rejected job must not be registered")
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestQueuedDuringMaintenanceKeepsFairOrderAcrossRecovery：维护期间积压的
// 多个作业在取消维护后按原提交公平次序依次跑起来，不被误拒、不残留。
func TestQueuedDuringMaintenanceKeepsFairOrderAcrossRecovery(t *testing.T) {
	s := clusterForAdmission(t)
	mustOK(t, s.MarkNodeMaintenance("n-big", "upgrade"))

	// n-small 上放一个占位作业，随后两个大作业都只能排队等 n-big。
	small := s.Submit(Job{ID: "small1", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if small.State != StateRunning {
		t.Fatalf("small1 should run on n-small, got %s/%s", small.State, small.Reason)
	}
	if j, _ := s.GetJob("small1"); j.NodeName != "n-small" {
		t.Fatalf("small1 should be on n-small, got %s", j.NodeName)
	}
	for _, id := range []string{"big1", "big2"} {
		r := s.Submit(bigJob(id))
		if !r.Accepted || r.State != StateQueued {
			t.Fatalf("%s must be accepted and queued during maintenance, got %+v", id, r)
		}
	}

	// n-big 恢复：big1 先跑（16C 装不下两个 12C 作业），big2 继续排队。
	mustOK(t, s.CancelMaintenance("n-big"))
	assertState(t, s, "big1", StateRunning)
	assertState(t, s, "big2", StateQueued)
	if j, _ := s.GetJob("big1"); j.NodeName != "n-big" {
		t.Fatalf("big1 should take n-big first, got %s", j.NodeName)
	}

	// big1 完成后 big2 按原次序接上，无残留卡住。
	mustOK(t, s.Complete("big1"))
	assertState(t, s, "big2", StateRunning)
	if j, _ := s.GetJob("big2"); j.NodeName != "n-big" {
		t.Fatalf("big2 should run on n-big after big1 completes, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestMultipleNodesDownSubmitStillQueuedAndDrain：多台机器同时维护时，
// 能被任一（哪怕正在维护的）节点容纳的请求都要排队；恢复后全部排空；
// 维护机器上始终不出现新分配。
func TestMultipleNodesDownSubmitStillQueuedAndDrain(t *testing.T) {
	s := New(WithClock(fakeClock()))
	for _, n := range []string{"n1", "n2", "n3"} {
		mustOK(t, s.AddNode(n, Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	}
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	mustOK(t, s.MarkNodeMaintenance("n1", "drain1"))
	mustOK(t, s.MarkNodeMaintenance("n2", "drain2"))

	// 三台里两台维护、健康的 n3 空着：8-CPU 作业应立即落在 n3。
	r := s.Submit(Job{ID: "run1", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if r.State != StateRunning {
		t.Fatalf("run1 should run on healthy n3, got %s/%s", r.State, r.Reason)
	}
	if j, _ := s.GetJob("run1"); j.NodeName != "n3" {
		t.Fatalf("run1 should be on n3, got %s", j.NodeName)
	}
	// 再来的整节点作业无人可放 => 排队而不是被拒。
	for _, id := range []string{"q1", "q2"} {
		r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
		if !r.Accepted || r.State != StateQueued {
			t.Fatalf("%s must queue while n1/n2 drain and n3 is full, got %+v", id, r)
		}
	}

	// 恢复一台：q1 起跑，q2 仍排队；维护中的 n2 绝不接收新作业。
	mustOK(t, s.CancelMaintenance("n1"))
	assertState(t, s, "q1", StateRunning)
	assertState(t, s, "q2", StateQueued)
	if j, _ := s.GetJob("q1"); j.NodeName != "n1" {
		t.Fatalf("q1 should run on n1, got %s", j.NodeName)
	}
	if allocated := s.Ledger().Allocated("n2"); allocated.AnyPositive() {
		t.Fatalf("draining n2 must carry no new allocation, got %s", allocated)
	}

	// 恢复第二台：q2 排空。
	mustOK(t, s.CancelMaintenance("n2"))
	assertState(t, s, "q2", StateRunning)
	if j, _ := s.GetJob("q2"); j.NodeName != "n2" {
		t.Fatalf("q2 should run on n2, got %s", j.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestConcurrentSubmitAcrossMaintenanceNoFalseReject：多台机器反复进入/取消
// 维护并叠加故障/恢复，提交协程持续提交“能被集群中某台机器容纳、但健康容量
// 暂时可能为零”的作业。要求：全程不出现 INFEASIBLE 误拒、账目守恒、
// 维护/故障机器上不出现新分配；全部节点恢复后排队作业可继续被调度。
func TestConcurrentSubmitAcrossMaintenanceNoFalseReject(t *testing.T) {
	s := New(WithClock(fakeClock()))
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		mustOK(t, s.AddNode(n, Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	}
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var seq int64
	var rejects int64

	// 提交过的作业 ID 共享给取消者（取消排队/运行作业都应只改状态、不破坏账目）。
	var submittedMu sync.Mutex
	var submitted []string
	recordSubmitted := func(id string) {
		submittedMu.Lock()
		submitted = append(submitted, id)
		submittedMu.Unlock()
	}

	// 所有提交请求都是 8 CPU 整节点：任一节点（含维护/故障）的 Total 都装得下，
	// 队列无 MaxJobs、ID 唯一 => 除“误判不可行”外不存在任何拒绝理由，
	// 因此任何 !Accepted 都视为缺陷。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := atomic.AddInt64(&seq, 1)
				id := fmt.Sprintf("w%d-%05d", w, n)
				r := s.Submit(Job{ID: id, Queue: "batch",
					Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
				if !r.Accepted {
					atomic.AddInt64(&rejects, 1)
					continue
				}
				recordSubmitted(id)
			}
		}(w)
	}

	// 取消者：边维护边并发取消随机选取的在途作业（排队或运行中都可能）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			submittedMu.Lock()
			if len(submitted) == 0 {
				submittedMu.Unlock()
				continue
			}
			idx := int(atomic.AddInt64(&seq, 1)) % len(submitted)
			id := submitted[idx]
			submitted = append(submitted[:idx], submitted[idx+1:]...)
			submittedMu.Unlock()
			_ = s.Cancel(id) // 已结束/已取消的作业报错属正常，忽略
		}
	}()

	// 状态翻转者：n1/n2 反复维护/取消维护，n3 反复故障/恢复；
	// 某些时刻健康节点可能一台不剩。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			switch i % 4 {
			case 0:
				_ = s.MarkNodeMaintenance("n1", "concurrent drain")
			case 1:
				_ = s.CancelMaintenance("n1")
			case 2:
				_ = s.MarkNodeUnhealthy("n3")
			case 3:
				_ = s.MarkNodeHealthy("n3")
			}
		}
	}()

	// 巡检者：守恒 + 非健康节点绝不出现“新提交作业”的分配。
	// 维护节点上可能存在进入维护前就在跑、迁不走的遗留单作业，因此这里
	// 只核对故障节点（故障时作业必被全部驱逐）账目必须归零，以及全局守恒。
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
			if msg := auditNoAllocationsWhileNodeDown(s); msg != "" {
				t.Errorf("%s", msg)
				return
			}
			for _, name := range []string{"n1", "n2", "n3", "n4"} {
				n, ok := s.GetNode(name)
				if !ok {
					continue
				}
				if n.State() == NodeUnhealthy && s.Ledger().Allocated(name).AnyPositive() {
					t.Errorf("unhealthy node %s carries allocation %s", name, s.Ledger().Allocated(name))
					return
				}
			}
		}
	}()

	for k := 0; k < 400; k++ {
		s.Tick()
	}
	close(stop)
	wg.Wait()

	if got := atomic.LoadInt64(&rejects); got != 0 {
		t.Fatalf("got %d unexpected rejections during maintenance/failure churn", got)
	}
	if msg := auditNoAllocationsWhileNodeDown(s); msg != "" {
		t.Fatalf("%s", msg)
	}

	// 收尾：恢复全部节点，排队作业应能继续被调度排空（至少不会卡死）。
	for _, n := range []string{"n1", "n2"} {
		if nn, ok := s.GetNode(n); ok && nn.State() == NodeDraining {
			_ = s.CancelMaintenance(n)
		}
	}
	if nn, ok := s.GetNode("n3"); ok && nn.State() == NodeUnhealthy {
		_ = s.MarkNodeHealthy("n3")
	}
	for k := 0; k < 50; k++ {
		if s.Tick() == 0 {
			break
		}
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("final conservation: %v", err)
	}
	// 恢复后不存在残留 QUEUED：每个 8-CPU 排队作业都能独占某台节点，
	// 但运行中的作业会占着节点；只校验“没有作业既排队又被重复记账”。
	t.Logf("\n%s", s.Snapshot())
}

// auditNoAllocationsWhileNodeDown 依据流水审计：任何节点在 DRAINING
// （NODE_DRAIN..NODE_READY）或 UNHEALTHY（NODE_DOWN..NODE_UP）区间内
// 都不得出现 ALLOCATE——维护/故障机器上不能被塞进任何新作业。
// 维护前就在节点上运行、迁不走的遗留作业不会产生新的 ALLOCATE 事件，
// 因此该检查能精确识别“新分配”，不会误伤遗留占用。
func auditNoAllocationsWhileNodeDown(s *Scheduler) string {
	down := map[string]bool{}
	for _, ev := range s.Ledger().Events() {
		switch ev.Type {
		case EvNodeDrain, EvNodeDown:
			down[ev.NodeName] = true
		case EvNodeReady, EvNodeUp:
			down[ev.NodeName] = false
		case EvAllocate:
			if down[ev.NodeName] {
				return fmt.Sprintf("new ALLOCATE of job %s on non-healthy node %s (event #%d)",
					ev.JobID, ev.NodeName, ev.Seq)
			}
		}
	}
	return ""
}
