package scheduler

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

// eventSummary 把流水折叠成 (Type,JobID,NodeName,Reason) 的有序切片，便于断言。
func eventSummary(s *Scheduler) []string {
	evs := s.Ledger().Events()
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		if e.Delta.AnyPositive() || e.Reason != "" && e.Reason != ReasonOK {
			out = append(out, string(e.Type)+"|"+e.JobID+"|"+e.NodeName+"|"+string(e.Reason))
		}
	}
	sort.Strings(out)
	return out
}

// TestMaintenanceDrainsByMigrationAndBlocksNewPlacements 计划维护：
// 维护节点不接收新作业；不可迁移的作业原地等待；资源归还后可显式排空迁移；
// 迁移走 MIGRATE+ALLOCATE(MIGRATED_MAINTENANCE) 成对流水，账目守恒。
func TestMaintenanceDrainsByMigrationAndBlocksNewPlacements(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// a 占满 n1；m 落在 n2
	a := s.Submit(Job{ID: "a", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000}})
	mv := s.Submit(Job{ID: "m", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 4000}})
	if a.State != StateRunning || mv.State != StateRunning {
		t.Fatalf("setup: a=%s m=%s", a.State, mv.State)
	}
	jm, _ := s.GetJob("m")
	if jm.NodeName != "n2" {
		t.Fatalf("setup: m should be on n2, got %s", jm.NodeName)
	}

	// n2 进入维护：n1 被 a 占满，m 迁不走 => 原地等待，节点未清空
	res, err := s.MarkNodeMaintenance("n2")
	mustOK(t, err)
	if res.Drained || len(res.Waiting) != 1 || res.Waiting[0] != "m" {
		t.Fatalf("immediate drain should leave m waiting, got %+v", res)
	}
	if res.InUse != (Resources{CPU: 4, MemoryMB: 4000}) {
		t.Fatalf("in-use = %s", res.InUse)
	}

	// 维护节点剩余可用量不得被当作可放置容量：n2 明明还空 4 CPU，
	// 新作业 x 也只能排队，绝不能被放到 n2。
	x := s.Submit(Job{ID: "x", Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 100}})
	if x.State != StateQueued {
		t.Fatalf("new job must not land on maintenance node n2 (which has free capacity), got %s on submit", x.State)
	}
	if jx, _ := s.GetJob("x"); jx.NodeName == "n2" {
		t.Fatalf("x placed on maintenance node n2")
	}

	st, err := s.DrainStatus("n2")
	mustOK(t, err)
	if st.Drained || st.Running != 1 || st.State != NodeMaintenance || st.Reason != ReasonMaintenanceDrainPending {
		t.Fatalf("drain status = %+v", st)
	}

	// a 正常完成：n1 空出，排队的 x 先被公平调度到 n1
	mustOK(t, s.Complete("a"))
	assertState(t, s, "x", StateRunning)
	if jx, _ := s.GetJob("x"); jx.NodeName != "n1" {
		t.Fatalf("x should run on healthy n1, got %s", jx.NodeName)
	}

	// 维护窗口内再排空：m 迁到 n1；StartedAt 保留（同一次运行的延续）
	before, _ := s.GetJob("m")
	res2, err := s.DrainNode("n2")
	mustOK(t, err)
	if !res2.Drained || len(res2.Migrated) != 1 || res2.Migrated[0] != "m" || res2.Running != 0 {
		t.Fatalf("second drain should migrate m and drain node, got %+v", res2)
	}
	after, _ := s.GetJob("m")
	if after.State != StateRunning || after.NodeName != "n1" {
		t.Fatalf("m migrated = %s@%s, want RUNNING@n1", after.State, after.NodeName)
	}
	if after.Reason != ReasonMigratedMaintenance {
		t.Fatalf("m migration reason = %s, want MIGRATED_MAINTENANCE", after.Reason)
	}
	if !after.StartedAt.Equal(before.StartedAt) {
		t.Fatalf("migration must preserve StartedAt: before=%s after=%s", before.StartedAt, after.StartedAt)
	}

	st2, err := s.DrainStatus("n2")
	mustOK(t, err)
	if !st2.Drained || st2.Running != 0 || st2.Reason != ReasonOK {
		t.Fatalf("n2 should be reported safe to restart: %+v", st2)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}

	// 审计：m 的 MIGRATE（n2 扣减）与带原因的 ALLOCATE（n1 落地）必须可区分
	var sawMigrate, sawMigratedAllocate bool
	for _, e := range s.Ledger().Events() {
		if e.JobID == "m" && e.Type == EvMigrate && e.NodeName == "n2" &&
			e.Delta == (Resources{CPU: 4, MemoryMB: 4000}) {
			sawMigrate = true
		}
		if e.JobID == "m" && e.Type == EvAllocate && e.NodeName == "n1" && e.Reason == ReasonMigratedMaintenance {
			sawMigratedAllocate = true
		}
	}
	if !sawMigrate || !sawMigratedAllocate {
		t.Fatalf("migration audit trail incomplete: migrate=%v allocate=%v", sawMigrate, sawMigratedAllocate)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestMaintenanceNaturalCompletion 维护节点上迁不走的作业允许自然结束，
// 结束后节点被判定为已清空。
func TestMaintenanceNaturalCompletion(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	ra := s.Submit(Job{ID: "a", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000}})
	if ra.State != StateRunning {
		t.Fatalf("a: %s", ra.State)
	}
	r := s.Submit(Job{ID: "m", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 4000}})
	if r.State != StateRunning {
		t.Fatalf("m: %s", r.State)
	}

	res, err := s.MarkNodeMaintenance("n2")
	mustOK(t, err)
	if res.Drained {
		t.Fatalf("n2 should not be drained with m running")
	}
	// n1 被 a 占满；再排空一次仍然迁不走
	res, err = s.DrainNode("n2")
	mustOK(t, err)
	if res.Drained || len(res.Waiting) != 1 {
		t.Fatalf("m still cannot migrate: %+v", res)
	}

	// 维护节点上的作业自然结束 => 节点清空
	mustOK(t, s.Complete("m"))
	st, err := s.DrainStatus("n2")
	mustOK(t, err)
	if !st.Drained || st.Running != 0 {
		t.Fatalf("n2 should be drained after natural completion: %+v", st)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestMaintenanceInterruptsWholeGroup 维护排空触发的整组中断：
// 维护节点上的组成员 MIGRATE，健康节点上的同组伙伴 GROUP_REQUEUE；
// 整组回到队列（QUEUED_GROUP_INTERRUPTED），容量不足时整组排队，
// 容量归还后在同一决策里原子重排。
func TestMaintenanceInterruptsWholeGroup(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n3", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 5-CPU 的两成员必然分散到 n1,n2；f3 占满 n3，使整组中断后暂时放不下
	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
	})
	for _, r := range res {
		if r.State != StateRunning {
			t.Fatalf("group setup: %+v", r)
		}
	}
	jg1, _ := s.GetJob("g1")
	jg2, _ := s.GetJob("g2")
	if jg1.NodeName != "n1" || jg2.NodeName != "n2" {
		t.Fatalf("group setup spread: g1=%s g2=%s", jg1.NodeName, jg2.NodeName)
	}
	f3 := s.Submit(Job{ID: "f3", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000}})
	if f3.State != StateRunning {
		t.Fatalf("f3: %s", f3.State)
	}
	jf3, _ := s.GetJob("f3")
	if jf3.NodeName != "n3" {
		t.Fatalf("f3 should be on n3, got %s", jf3.NodeName)
	}

	// n1 维护 => 整组 G 一起停止（g1 在维护节点、g2 在健康节点 n2）
	dres, err := s.MarkNodeMaintenance("n1")
	mustOK(t, err)
	if len(dres.Requeued) != 2 {
		t.Fatalf("both group members must requeue, got %+v", dres)
	}
	assertState(t, s, "g1", StateQueued)
	assertState(t, s, "g2", StateQueued)
	for _, id := range []string{"g1", "g2"} {
		j, _ := s.GetJob(id)
		if j.Reason != ReasonQueuedGroupInterrupted {
			t.Fatalf("%s reason=%s, want QUEUED_GROUP_INTERRUPTED", id, j.Reason)
		}
	}
	if j, _ := s.GetJob("g2"); j.NodeName != "" {
		t.Fatalf("healthy peer g2 must be stopped, still on %s", j.NodeName)
	}
	// 不允许部分在跑部分排队：f3 仍在跑，组 G 全部排队
	if j, _ := s.GetJob("f3"); j.State != StateRunning {
		t.Fatalf("unrelated job f3 must keep running")
	}
	if s.Ledger().Allocated("n1").AnyPositive() || s.Ledger().Allocated("n2").AnyPositive() {
		t.Fatalf("n1/n2 group allocations must be fully released")
	}

	// 审计事件可区分：g1 MIGRATE@n1，g2 GROUP_REQUEUE@n2
	cnt := map[string]int{}
	for _, e := range s.Ledger().Events() {
		if (e.JobID == "g1" || e.JobID == "g2") && (e.Type == EvMigrate || e.Type == EvGroupRequeue) {
			cnt[string(e.Type)+"@"+e.NodeName]++
		}
		if e.Type == EvQueue && (e.JobID == "g1" || e.JobID == "g2") && e.Reason != ReasonQueuedGroupInterrupted {
			t.Fatalf("group requeue note reason = %s", e.Reason)
		}
	}
	if cnt["MIGRATE@n1"] != 1 || cnt["GROUP_REQUEUE@n2"] != 1 {
		t.Fatalf("audit counter = %v", cnt)
	}

	// f3 完成释放 n3：整组必须在同一决策里原子重排（仍然不能只起一个）
	mustOK(t, s.Complete("f3"))
	assertState(t, s, "g1", StateRunning)
	assertState(t, s, "g2", StateRunning)
	j1, _ := s.GetJob("g1")
	j2, _ := s.GetJob("g2")
	if j1.NodeName == j2.NodeName {
		t.Fatalf("group must spread again, both on %s", j1.NodeName)
	}
	for _, name := range []string{j1.NodeName, j2.NodeName} {
		if name == "n1" {
			t.Fatalf("interrupted group must not re-place on maintenance node n1")
		}
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestFailureInterruptsWholeGroup 节点故障触发的整组中断：故障成员 EVICT，
// 健康节点上的同组伙伴 GROUP_REQUEUE，整组按 QUEUED_GROUP_INTERRUPTED 重新排队，
// 资源归还后原子重排。该语义与计划维护使用不同流水，审计可区分。
func TestFailureInterruptsWholeGroup(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n3", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
	})
	for _, r := range res {
		if r.State != StateRunning {
			t.Fatalf("group setup: %+v", r)
		}
	}
	f3 := s.Submit(Job{ID: "f3", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000}})
	if f3.State != StateRunning {
		t.Fatalf("f3: %s", f3.State)
	}

	mustOK(t, s.MarkNodeUnhealthy("n1"))
	assertState(t, s, "g1", StateQueued)
	assertState(t, s, "g2", StateQueued)
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.GetJob(id); j.Reason != ReasonQueuedGroupInterrupted {
			t.Fatalf("%s reason=%s", id, j.Reason)
		}
	}
	assertState(t, s, "f3", StateRunning) // 不相关作业不受影响
	// 不允许部分在跑
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.GetJob(id); j.NodeName != "" {
			t.Fatalf("%s must not stay placed, on %s", id, j.NodeName)
		}
	}

	cnt := map[string]int{}
	for _, e := range s.Ledger().Events() {
		if (e.JobID == "g1" || e.JobID == "g2") && (e.Type == EvEvict || e.Type == EvGroupRequeue || e.Type == EvMigrate) {
			cnt[string(e.Type)+"@"+e.NodeName]++
		}
	}
	if cnt["EVICT@n1"] != 1 || cnt["GROUP_REQUEUE@n2"] != 1 || cnt["MIGRATE@n1"] != 0 {
		t.Fatalf("failure audit counter = %v, want EVICT@n1=1 GROUP_REQUEUE@n2=1 MIGRATE=0", cnt)
	}
	// 四类情形（完成归还 RELEASE / 维护迁移 MIGRATE / 故障驱逐 EVICT /
	// 整组重排队 GROUP_REQUEUE）在同一套流水里必须类型互不混淆。
	summary := eventSummary(s)
	t.Logf("event summary: %v", summary)

	mustOK(t, s.Complete("f3"))
	assertState(t, s, "g1", StateRunning)
	assertState(t, s, "g2", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestMaintenanceWhileGroupOnNodeFails 维护中的节点突发故障：按故障语义
// 强制处理，整组一致性仍然成立。
func TestMaintenanceWhileGroupOnNodeFails(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
	})
	for _, r := range res {
		if r.State != StateRunning {
			t.Fatalf("group setup: %+v", r)
		}
	}
	jg1, _ := s.GetJob("g1")
	_, err := s.MarkNodeMaintenance(jg1.NodeName)
	mustOK(t, err)
	// 整组此时已重排队，节点已空；再标记故障不应出错，节点进入 UNHEALTHY
	mustOK(t, s.MarkNodeUnhealthy(jg1.NodeName))
	st, err := s.DrainStatus(jg1.NodeName)
	mustOK(t, err)
	if st.State != NodeUnhealthy {
		t.Fatalf("state = %s, want UNHEALTHY", st.State)
	}
	if err := s.MarkNodeUnhealthy(jg1.NodeName); err == nil {
		t.Fatalf("double unhealthy should fail")
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
}

// TestCancelMaintenanceReturnsNodeToService 取消维护后节点回到正常调度：
// 未迁走的作业继续运行、不重置可用量；接口状态机非法转换要报错。
func TestCancelMaintenanceReturnsNodeToService(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	ra := s.Submit(Job{ID: "a", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000}})
	if ra.State != StateRunning {
		t.Fatalf("a: %s", ra.State)
	}
	rm := s.Submit(Job{ID: "m", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 4000}})
	if rm.State != StateRunning {
		t.Fatalf("m: %s", rm.State)
	}
	ja, _ := s.GetJob("a")
	jm, _ := s.GetJob("m")
	maintNode := jm.NodeName
	other := ja.NodeName
	if maintNode == other {
		t.Fatalf("setup: a and m on same node")
	}

	_, err := s.MarkNodeMaintenance(maintNode)
	mustOK(t, err)
	assertState(t, s, "m", StateRunning) // 迁不走但继续运行
	if err := s.CancelNodeMaintenance(other); err == nil {
		t.Fatalf("cancelling maintenance on healthy node should fail")
	}
	if err := s.MarkNodeHealthy(maintNode); err == nil {
		t.Fatalf("MarkNodeHealthy on maintenance node should fail")
	}
	if _, err := s.DrainNode(other); err == nil {
		t.Fatalf("DrainNode on healthy node should fail")
	}

	mustOK(t, s.CancelNodeMaintenance(maintNode))
	if err := s.CancelNodeMaintenance(maintNode); err == nil {
		t.Fatalf("double cancel should fail")
	}
	j, _ := s.GetJob("m")
	if j.State != StateRunning || j.NodeName != maintNode || j.Reason != ReasonOK {
		t.Fatalf("m should keep running normally after cancel: %+v", j)
	}
	// 节点回到服务：新作业可落到该节点剩余 4 CPU
	nw := s.Submit(Job{ID: "n", Queue: "batch", Request: Resources{CPU: 2, MemoryMB: 200}})
	if nw.State != StateRunning {
		t.Fatalf("n should run after maintenance cancelled, got %s %s", nw.State, nw.Reason)
	}
	if jn, _ := s.GetJob("n"); jn.NodeName != maintNode {
		t.Fatalf("n should use restored node %s, got %s", maintNode, jn.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// newPlacementOnMaintenance 审计流水：任何节点在一次 NODE_MAINTENANCE 之后、
// 下一次 NODE_READY/NODE_UP/NODE_DOWN 之前，都不允许出现 ALLOCATE。
// 返回非空字符串表示发现违规说明。
func newPlacementOnMaintenance(evs []LedgerEvent) string {
	inMaint := map[string]bool{}
	for _, e := range evs {
		switch e.Type {
		case EvNodeMaint:
			inMaint[e.NodeName] = true
		case EvNodeReady, EvNodeUp, EvNodeDown:
			inMaint[e.NodeName] = false
		case EvAllocate:
			if inMaint[e.NodeName] {
				return fmt.Sprintf("ILLEGAL new ALLOCATE of job %s on maintenance node %s", e.JobID, e.NodeName)
			}
		}
	}
	return ""
}

// TestConcurrentMaintenanceSubmitCancel 多节点同时维护/排空/取消，
// 与并发提交、完成/取消交织：资源账目始终守恒，无重复分配，维护节点不收新作业。
func TestConcurrentMaintenanceSubmitCancel(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddNode("n3", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var idCounter atomic.Int64

	// 提交者：持续提交 1 CPU/1 GPU 小作业（总量 12 GPU 上限）
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				id := fmt.Sprintf("j%06d", idCounter.Add(1))
				s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 500, GPU: 1}})
			}
		}()
	}

	// 维护者：对 n1/n2 反复 进入维护->排空->取消
	for _, node := range []string{"n1", "n2"} {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := s.MarkNodeMaintenance(node); err == nil {
					_, _ = s.DrainNode(node)
					_ = s.CancelNodeMaintenance(node)
				}
			}
		}(node)
	}

	// 回收者：完成/取消运行中与排队作业
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
			_ = s.Complete(fmt.Sprintf("j%06d", i%400))
			if i%2 == 0 {
				_ = s.Cancel(fmt.Sprintf("j%06d", (i+1)%400))
			}
			i++
		}
	}()

	// 巡检者：持续核对守恒，并通过流水确认“进入维护后该节点没有任何新放置”。
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
				t.Errorf("concurrent maintenance conservation: %v", err)
				return
			}
			if msg := newPlacementOnMaintenance(s.Ledger().Events()); msg != "" {
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

	_ = s.CancelNodeMaintenance("n1")
	_ = s.CancelNodeMaintenance("n2")
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("final conservation: %v", err)
	}
	if dup := findDoublePlacement(s); dup != "" {
		t.Fatalf("%s", dup)
	}
	// 维护取消后全集群 12 GPU 容量恢复：排空 backlog，最终排队+运行守恒
	for k := 0; k < 2000 && countByState(s, StateQueued) > 0; k++ {
		var runningID string
		s.mu.Lock()
		for id, jj := range s.jobs {
			if jj.State == StateRunning {
				runningID = id
				break
			}
		}
		s.mu.Unlock()
		if runningID == "" {
			break
		}
		_ = s.Complete(runningID)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("post-drain conservation: %v", err)
	}
	t.Logf("concurrent maintenance: running=%d queued=%d events=%d",
		countByState(s, StateRunning), countByState(s, StateQueued), len(s.Ledger().Events()))
}

// TestMaintenanceDeterministicReplay 含维护/排空/取消的相同操作序列，
// 多次执行必须得到逐作业一致的状态与落点。
func TestMaintenanceDeterministicReplay(t *testing.T) {
	run := func() map[string]string {
		s := New(WithClock(fakeClock()))
		mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
		mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000}))
		mustOK(t, s.AddNode("n3", Resources{CPU: 8, MemoryMB: 16000}))
		mustOK(t, s.AddQueue(QueueConfig{Name: "q1", Weight: 1}))
		mustOK(t, s.AddQueue(QueueConfig{Name: "q2", Weight: 2}))

		seq := []Job{
			{ID: "a", Queue: "q1", Request: Resources{CPU: 4, MemoryMB: 1000}},
			{ID: "b", Queue: "q1", Request: Resources{CPU: 4, MemoryMB: 1000}},
			{ID: "c", Queue: "q2", Request: Resources{CPU: 8, MemoryMB: 16000}},
		}
		s.SubmitGroup([]Job{
			{ID: "g1", Queue: "q2", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
			{ID: "g2", Queue: "q2", GroupID: "G", Request: Resources{CPU: 5, MemoryMB: 1000}},
		})
		for _, j := range seq {
			s.Submit(j)
		}
		_, _ = s.MarkNodeMaintenance("n2")
		s.Tick()
		_, _ = s.DrainNode("n2")
		_ = s.Complete("c")
		s.Tick()
		_, _ = s.DrainNode("n2")
		_ = s.CancelNodeMaintenance("n2")
		s.Tick()

		out := map[string]string{}
		for _, id := range []string{"a", "b", "c", "g1", "g2"} {
			if jj, ok := s.GetJob(id); ok {
				out[id] = jj.State.String() + "@" + jj.NodeName + "#" + string(jj.Reason)
			}
		}
		if err := s.CheckConservation(); err != nil {
			t.Fatalf("conservation: %v", err)
		}
		return out
	}
	first := run()
	for k := 0; k < 5; k++ {
		again := run()
		for id, v := range first {
			if again[id] != v {
				t.Fatalf("non-deterministic: %s was %s then %s", id, v, again[id])
			}
		}
	}
	t.Logf("maintenance replay mapping: %v", first)
}
