package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// clusterForMaintenance 构造 3 节点测试集群（每节点 8 CPU / 16000MB / 2 GPU）
// 与两个等权队列。
func clusterForMaintenance(t *testing.T) *Scheduler {
	t.Helper()
	s := New(WithClock(fakeClock()))
	for _, n := range []string{"n1", "n2", "n3"} {
		mustOK(t, s.AddNode(n, Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	}
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "viz", Weight: 1}))
	return s
}

// TestMaintenanceNoNewPlacementsAndNaturalDrain：
// 维护节点不再接收新作业；迁不走的单作业继续跑、自然结束后节点才算清空；
// 维护期间提交与排队照常，取消维护后节点重新承载。
func TestMaintenanceNoNewPlacementsAndNaturalDrain(t *testing.T) {
	s := clusterForMaintenance(t)

	fill := func(id, nodeWant string, cpu int) {
		t.Helper()
		r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: cpu, MemoryMB: 2000}})
		if r.State != StateRunning {
			t.Fatalf("%s should run, got %s", id, r.Reason)
		}
		j, _ := s.GetJob(id)
		if j.NodeName != nodeWant {
			t.Fatalf("%s on %s, want %s", id, j.NodeName, nodeWant)
		}
	}
	// 三节点全部占满；d(8) 排队。
	fill("a", "n1", 8)
	fill("b", "n2", 8)
	fill("c", "n3", 8)
	d := s.Submit(Job{ID: "d", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 2000}})
	if d.State != StateQueued {
		t.Fatalf("d should queue, got %s", d.State)
	}

	// n1 进入计划维护：a 需要 8，健康节点 n2/n3 全满 => 迁不走，继续运行。
	mustOK(t, s.MarkNodeMaintenance("n1", "rolling upgrade"))
	n1, _ := s.GetNode("n1")
	if n1.State() != NodeDraining {
		t.Fatalf("n1 state=%s, want DRAINING", n1.State())
	}
	if reason, _, ok := n1.MaintenanceInfo(); !ok || reason != "rolling upgrade" {
		t.Fatalf("maintenance info wrong: %q %v", reason, ok)
	}
	assertState(t, s, "a", StateRunning)
	drained, err := s.NodeDrained("n1")
	mustOK(t, err)
	if drained {
		t.Fatalf("n1 still runs job a; must not be drainable yet")
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after maintenance start: %v", err)
	}

	// 维护期间仍可正常提交小作业；健康节点全满 => 排队（绝不落到维护节点）。
	e := s.Submit(Job{ID: "e", Queue: "viz", Request: Resources{CPU: 1, MemoryMB: 1000}})
	if !e.Accepted {
		t.Fatalf("submit during maintenance must be accepted: %s", e.Reason)
	}

	// b 完成释放 n2 的 8 CPU：维护迁移优先把 a 迁到 n2 继续运行；
	// 排队作业绝不能被放到 DRAINING 的 n1。
	mustOK(t, s.Complete("b"))
	ja, _ := s.GetJob("a")
	if ja.NodeName != "n2" {
		t.Fatalf("a should migrate to healthy n2, got %s", ja.NodeName)
	}
	drained, _ = s.NodeDrained("n1")
	if !drained {
		t.Fatalf("n1 should be drained after a migrated")
	}
	for _, id := range []string{"d", "e"} {
		j, _ := s.GetJob(id)
		if j.NodeName == "n1" {
			t.Fatalf("%s must never be placed on draining n1", id)
		}
	}
	// a 迁出后 n1 虽空，仍不接收新作业：n2 被 a 占满、n3 被 c 占满，
	// 8-CPU 的 d 无处可去，保持排队。
	assertState(t, s, "d", StateQueued)
	assertState(t, s, "e", StateQueued)

	// c 完成释放 n3：WRR 公平轮转让 viz 的小作业 e 拿到 n3 的 1 个槽位
	// （维护没有卡住队列间公平）；8-CPU 的 d 仍无整节点可用，继续排队。
	mustOK(t, s.Complete("c"))
	assertState(t, s, "e", StateRunning)
	je, _ := s.GetJob("e")
	if je.NodeName != "n3" {
		t.Fatalf("e should be served on healthy n3 by WRR, got %s", je.NodeName)
	}
	assertState(t, s, "d", StateQueued)

	// 取消维护：n1 回到调度，d 立即获得安置；a/e 位置不变。
	mustOK(t, s.CancelMaintenance("n1"))
	n1, _ = s.GetNode("n1")
	if n1.State() != NodeHealthy {
		t.Fatalf("n1 state=%s, want HEALTHY", n1.State())
	}
	assertState(t, s, "d", StateRunning)
	jd, _ := s.GetJob("d")
	if jd.NodeName != "n1" {
		t.Fatalf("d should run on restored n1, got %s", jd.NodeName)
	}
	assertState(t, s, "a", StateRunning)
	assertState(t, s, "e", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation at end: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestMaintenanceMigratesStandaloneJob：维护时有健康容量 => 单作业被迁走，
// 源节点清空，流水留下 MIGRATE + 重新 ALLOCATE 的成对记录。
func TestMaintenanceMigratesStandaloneJob(t *testing.T) {
	s := clusterForMaintenance(t)
	fill := func(id, nodeWant string, cpu int) {
		t.Helper()
		r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: cpu, MemoryMB: 2000}})
		if r.State != StateRunning {
			t.Fatalf("%s: %s", id, r.Reason)
		}
		j, _ := s.GetJob(id)
		if j.NodeName != nodeWant {
			t.Fatalf("%s on %s want %s", id, j.NodeName, nodeWant)
		}
	}
	fill("a", "n1", 8)
	fill("b", "n2", 8)
	// n3 全空：维护 n1 时 a 应整体迁到 n3 继续运行。
	mustOK(t, s.MarkNodeMaintenance("n1", "hw repair"))
	assertState(t, s, "a", StateRunning)
	ja, _ := s.GetJob("a")
	if ja.NodeName != "n3" {
		t.Fatalf("a migrated to %s, want n3", ja.NodeName)
	}
	drained, _ := s.NodeDrained("n1")
	if !drained {
		t.Fatalf("n1 should be drained after migration")
	}
	if s.Ledger().Allocated("n1").AnyPositive() {
		t.Fatalf("n1 allocation must be zero after migration")
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	// 审计：必须能找到 a 的 MIGRATE（源 n1）与新的 ALLOCATE（目标 n3）。
	var sawMigrate, sawRealloc bool
	for _, ev := range s.Ledger().Events() {
		if ev.Type == EvMigrate && ev.JobID == "a" && ev.NodeName == "n1" &&
			ev.Reason == ReasonRequeueMaintenance {
			sawMigrate = true
		}
		if ev.Type == EvAllocate && ev.JobID == "a" && ev.NodeName == "n3" {
			sawRealloc = true
		}
	}
	if !sawMigrate || !sawRealloc {
		t.Fatalf("want MIGRATE(n1)+ALLOCATE(n3) for a, got %v/%v", sawMigrate, sawRealloc)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestMaintenanceWholeGroupRequeue：维护只打断一个组成员时，健康节点上的同伴
// 也必须一起停下；整组要么一起重新运行，要么一起排队，不允许半跑半等。
func TestMaintenanceWholeGroupRequeue(t *testing.T) {
	s := clusterForMaintenance(t)
	// g1->n1, g2->n2：两个 6 CPU 作业跨节点原子放置；n3 先被 8-CPU 作业占满。
	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
	})
	for _, r := range res {
		if r.State != StateRunning {
			t.Fatalf("%s should run: %s", r.JobID, r.Reason)
		}
	}
	blk := s.Submit(Job{ID: "blk", Queue: "viz", Request: Resources{CPU: 8, MemoryMB: 2000}})
	if blk.State != StateRunning {
		t.Fatalf("blk should run on n3: %s", blk.Reason)
	}
	if j, _ := s.GetJob("blk"); j.NodeName != "n3" {
		t.Fatalf("blk on %s, want n3", j.NodeName)
	}
	if msg := groupSplitState(s, "G"); msg != "" {
		t.Fatalf("%s", msg)
	}

	// 维护 n1：g1 被迁离，g2 虽在健康的 n2 上也必须一起停下整组排队。
	mustOK(t, s.MarkNodeMaintenance("n1", "kernel upgrade"))
	if msg := groupSplitState(s, "G"); msg != "" {
		t.Fatalf("group split after maintenance: %s", msg)
	}
	assertState(t, s, "g1", StateQueued)
	assertState(t, s, "g2", StateQueued)
	if s.Ledger().Allocated("n2").AnyPositive() {
		t.Fatalf("healthy-node peer g2 must have been released from n2")
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	// 健康容量此刻放不下 2x6：n2 空 8、n3 被 blk 占满；Tick 后仍整组排队。
	if n := s.Tick(); n != 0 {
		t.Fatalf("no placement expected while group cannot fit atomically, got %d", n)
	}
	assertState(t, s, "g1", StateQueued)
	assertState(t, s, "g2", StateQueued)

	// blk 结束释放 n3：n2(8)+n3(8) 可容纳 2x6 => 整组重新原子运行。
	mustOK(t, s.Complete("blk"))
	if msg := groupSplitState(s, "G"); msg != "" {
		t.Fatalf("%s", msg)
	}
	assertState(t, s, "g1", StateRunning)
	assertState(t, s, "g2", StateRunning)
	j1, _ := s.GetJob("g1")
	j2, _ := s.GetJob("g2")
	if j1.NodeName == j2.NodeName || j1.NodeName == "n1" || j2.NodeName == "n1" {
		t.Fatalf("group should respread on healthy n2/n3, got g1=%s g2=%s", j1.NodeName, j2.NodeName)
	}
	if j1.NodeName != "n2" || j2.NodeName != "n3" {
		t.Fatalf("group should respread on n2/n3, got g1=%s g2=%s", j1.NodeName, j2.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after regroup: %v", err)
	}

	// 审计事件齐备且可区分
	counts := map[EventType]int{}
	for _, ev := range s.Ledger().Events() {
		counts[ev.Type]++
	}
	for _, want := range []EventType{EvNodeDrain, EvMigrate, EvGroupRequeue, EvAllocate} {
		if counts[want] == 0 {
			t.Fatalf("missing %s event; counts=%v", want, counts)
		}
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestFailureWholeGroupRequeue：突发故障同样保证整组一致性，
// 但流水类型必须是 EVICT/GROUP_REQUEUE，与计划维护的 MIGRATE 区分。
func TestFailureWholeGroupRequeue(t *testing.T) {
	s := clusterForMaintenance(t)
	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
	})
	for _, r := range res {
		if r.State != StateRunning {
			t.Fatalf("%s: %s", r.JobID, r.Reason)
		}
	}
	// 占满 n3，使组暂时无法整体放置，从而能观察到“整组排队”这一中间态。
	blk := s.Submit(Job{ID: "blk", Queue: "viz", Request: Resources{CPU: 8, MemoryMB: 2000}})
	if blk.State != StateRunning {
		t.Fatalf("blk: %s", blk.Reason)
	}
	mustOK(t, s.MarkNodeUnhealthy("n1"))
	if msg := groupSplitState(s, "G"); msg != "" {
		t.Fatalf("%s", msg)
	}
	assertState(t, s, "g1", StateQueued)
	assertState(t, s, "g2", StateQueued)
	jg1, _ := s.GetJob("g1")
	if jg1.Reason != ReasonRequeueGroupInterrupt {
		t.Fatalf("g1 reason=%s, want REQUEUED_GROUP_INTERRUPTED", jg1.Reason)
	}
	var evict, groupRequeue, migrate int
	for _, ev := range s.Ledger().Events() {
		switch ev.Type {
		case EvEvict:
			evict++
		case EvGroupRequeue:
			groupRequeue++
		case EvMigrate:
			migrate++
		}
	}
	if evict != 1 || groupRequeue != 1 || migrate != 0 {
		t.Fatalf("want 1 EVICT + 1 GROUP_REQUEUE + 0 MIGRATE, got %d/%d/%d",
			evict, groupRequeue, migrate)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestConcurrentMaintenanceConservation：多节点同时维护/取消，叠加并发提交、
// 完成与组提交，账目始终守恒、无双重放置、作业组不出现半跑半等。
func TestConcurrentMaintenanceConservation(t *testing.T) {
	s := New(WithClock(fakeClock()))
	for _, n := range []string{"n1", "n2", "n3", "n4"} {
		mustOK(t, s.AddNode(n, Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	}
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var seq int64

	// 提交者：单作业 + 偶发整组
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
				n := int(atomic.AddInt64(&seq, 1))
				if i%5 == 0 {
					gid := fmt.Sprintf("grp-w%d-%d", w, i)
					s.SubmitGroup([]Job{
						{ID: fmt.Sprintf("%sa", gid), Queue: "batch", GroupID: gid,
							Request: Resources{CPU: 2, MemoryMB: 1000}},
						{ID: fmt.Sprintf("%sb", gid), Queue: "batch", GroupID: gid,
							Request: Resources{CPU: 2, MemoryMB: 1000}},
					})
				} else {
					s.Submit(Job{ID: fmt.Sprintf("w%d-%05d", w, n), Queue: "batch",
						Request: Resources{CPU: 1, MemoryMB: 500, GPU: 1}})
				}
				i++
			}
		}(w)
	}

	// 维护者：在 n1/n2 上反复进入维护/取消维护
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

	// 完成者：不断完成随机选取的运行作业（通过 Snapshot 文本太重，直接尝试命名空间）
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = s.Complete(fmt.Sprintf("w%d-%05d", i%8, i))
			i++
		}
	}()

	// 巡检者：守恒 + 双重放置 + 组一致性 + 非健康节点无运行作业
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
			if msg := findRunningJobOnNonHealthy(s); msg != "" {
				t.Errorf("%s", msg)
				return
			}
			if msg := findSplitGroup(s); msg != "" {
				t.Errorf("%s", msg)
				return
			}
		}
	}()

	for k := 0; k < 400; k++ {
		s.Tick()
	}
	close(stop)
	wg.Wait()

	if err := s.CheckConservation(); err != nil {
		t.Fatalf("final conservation: %v", err)
	}
	if msg := findDoublePlacement(s); msg != "" {
		t.Fatalf("%s", msg)
	}
	if msg := findRunningJobOnNonHealthy(s); msg != "" {
		t.Fatalf("%s", msg)
	}
	if msg := findSplitGroup(s); msg != "" {
		t.Fatalf("%s", msg)
	}
	// 收尾：恢复全部节点，排队作业最终应能排空到只剩容量约束
	for _, n := range []string{"n1", "n2"} {
		if nn, ok := s.GetNode(n); ok && nn.State() == NodeDraining {
			_ = s.CancelMaintenance(n)
		}
	}
	for k := 0; k < 20; k++ {
		if s.Tick() == 0 {
			break
		}
	}
	t.Logf("\n%s", s.Snapshot())
	t.Logf("ledger events=%d", len(s.Ledger().Events()))
}

// TestMaintenanceDeterministicReplay：相同初始状态与相同操作序列，
// 维护/迁移/整组重排队结论逐作业、逐事件可复现。
func TestMaintenanceDeterministicReplay(t *testing.T) {
	ops := func(s *Scheduler) {
		for _, n := range []string{"n1", "n2", "n3"} {
			mustOK(t, s.AddNode(n, Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
		}
		mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
		s.SubmitGroup([]Job{
			{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
			{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 6, MemoryMB: 2000}},
		})
		s.Submit(Job{ID: "solo", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 2000}})
		s.Submit(Job{ID: "waiter", Queue: "batch", Request: Resources{CPU: 3, MemoryMB: 1000}})
		mustOK(t, s.MarkNodeMaintenance("n1", "upgrade"))
		s.Tick()
		mustOK(t, s.MarkNodeMaintenance("n2", "upgrade"))
		s.Tick()
		_ = s.Cancel("waiter")
		mustOK(t, s.CancelMaintenance("n1"))
		s.Tick()
	}

	build := func() *Scheduler {
		s := New(WithClock(fakeClock()))
		ops(s)
		return s
	}
	s1, s2 := build(), build()

	// 逐作业比较状态与落点
	for id := range s1.jobs {
		a, _ := s1.GetJob(id)
		b, ok := s2.GetJob(id)
		if !ok {
			t.Fatalf("job %s missing in replay", id)
		}
		if a.State != b.State || a.NodeName != b.NodeName || a.Reason != b.Reason {
			t.Fatalf("nondeterministic for %s: %s/%s/%s vs %s/%s/%s",
				id, a.State, a.NodeName, a.Reason, b.State, b.NodeName, b.Reason)
		}
	}
	// 逐事件比较类型/对象/原因（时间戳不参与比较）
	e1, e2 := s1.Ledger().Events(), s2.Ledger().Events()
	if len(e1) != len(e2) {
		t.Fatalf("event length %d vs %d", len(e1), len(e2))
	}
	for i := range e1 {
		a, b := e1[i], e2[i]
		if a.Type != b.Type || a.JobID != b.JobID || a.NodeName != b.NodeName ||
			a.Queue != b.Queue || a.Reason != b.Reason || a.Delta != b.Delta {
			t.Fatalf("event #%d diverges:\n %s\n %s", i, a, b)
		}
	}
	if err := s1.CheckConservation(); err != nil {
		t.Fatalf("%s", err.Error())
	}
}

// ---- 辅助断言 ----

// groupSplitState 检查组内活跃成员是否同时处于同一状态（全运行或全排队）。
func groupSplitState(s *Scheduler, groupID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	states := map[JobState]string{}
	for _, m := range s.groups[groupID] {
		if m.State == StateRunning || m.State == StateQueued {
			states[m.State] = m.ID
		}
	}
	if len(states) > 1 {
		return fmt.Sprintf("group %s split across states: %v", groupID, states)
	}
	return ""
}

// findRunningJobOnNonHealthy 校验非健康节点上的运行作业合法性：
//   - UNHEALTHY 节点上不允许存在任何运行作业（故障已全部驱逐）；
//   - DRAINING 节点上只允许存在迁不走、等待自然结束的单作业，
//     不允许组成员（组成员必须整组重排队）。
func findRunningJobOnNonHealthy(s *Scheduler) string {
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
			return fmt.Sprintf("job %s runs on unhealthy node %s", j.ID, n.Name)
		case NodeDraining:
			if j.IsGrouped() {
				return fmt.Sprintf("group member %s still runs on draining node %s", j.ID, n.Name)
			}
		}
	}
	return ""
}

func findSplitGroup(s *Scheduler) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for gid, members := range s.groups {
		hasRun, hasQueue := false, false
		for _, m := range members {
			switch m.State {
			case StateRunning:
				hasRun = true
			case StateQueued:
				hasQueue = true
			}
		}
		if hasRun && hasQueue {
			return fmt.Sprintf("group %s has members both RUNNING and QUEUED", gid)
		}
	}
	return ""
}

// TestMaintenanceJobRunsToCompletionWhenNoCapacity：健康节点没有容纳空间时，
// 维护节点上的单作业继续运行直到自然结束；结束后节点才报告 drained。
func TestMaintenanceJobRunsToCompletionWhenNoCapacity(t *testing.T) {
	s := clusterForMaintenance(t)
	// 三个 8-CPU 作业占满三节点。
	for _, spec := range []struct{ id, node string }{
		{"a", "n1"}, {"b", "n2"}, {"c", "n3"},
	} {
		r := s.Submit(Job{ID: spec.id, Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 2000}})
		if r.State != StateRunning {
			t.Fatalf("%s: %s", spec.id, r.Reason)
		}
		if j, _ := s.GetJob(spec.id); j.NodeName != spec.node {
			t.Fatalf("%s on %s want %s", spec.id, j.NodeName, spec.node)
		}
	}
	mustOK(t, s.MarkNodeMaintenance("n1", "psu replacement"))

	// 反复调度：n2/n3 全满，a 迁不走，必须保持在 n1 上运行。
	for i := 0; i < 3; i++ {
		if s.Tick() != 0 {
			t.Fatalf("tick %d: nothing should be placed while healthy nodes are full", i)
		}
		assertState(t, s, "a", StateRunning)
		ja, _ := s.GetJob("a")
		if ja.NodeName != "n1" {
			t.Fatalf("a must keep running on draining n1, got %s", ja.NodeName)
		}
		drained, _ := s.NodeDrained("n1")
		if drained {
			t.Fatalf("n1 is not drained while a is still running")
		}
	}
	// a 自然结束：n1 清空；b/c 不受影响。
	mustOK(t, s.Complete("a"))
	drained, _ := s.NodeDrained("n1")
	if !drained {
		t.Fatalf("n1 must be drained after a ran to completion")
	}
	assertState(t, s, "b", StateRunning)
	assertState(t, s, "c", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	// a 的结束在流水里应是 RELEASE（正常归还），而非 MIGRATE/EVICT。
	for _, ev := range s.Ledger().Events() {
		if ev.JobID == "a" && (ev.Type == EvMigrate || ev.Type == EvEvict) {
			t.Fatalf("a was not migrated/evicted; unexpected %s event", ev.Type)
		}
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}
