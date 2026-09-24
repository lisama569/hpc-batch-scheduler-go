package scheduler

import "testing"

func TestNodeFailureEvictsAndReleases(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// n1 被两个作业占满，n2 放一个占位作业，再排队一个等资源的作业
	a := s.Submit(Job{ID: "a", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 8000, GPU: 1}})
	b := s.Submit(Job{ID: "b", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 8000, GPU: 1}})
	c := s.Submit(Job{ID: "c", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if !a.Accepted || !b.Accepted || !c.Accepted {
		t.Fatalf("a,b,c should be accepted")
	}
	// 确定性放置：best-fit + 节点名升序 => a,b 落 n1（平分），c 落 n2
	assertOnNode := func(id, node string) {
		t.Helper()
		j, _ := s.GetJob(id)
		if j.NodeName != node {
			t.Fatalf("%s on %s, want %s", id, j.NodeName, node)
		}
	}
	assertOnNode("a", "n1")
	assertOnNode("b", "n1")
	assertOnNode("c", "n2")

	// 故障前账目守恒
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation before failure: %v", err)
	}

	// n1 故障：a,b 被驱逐，资源必须释放（账目归零），作业回到队列；
	// n2 被 c 占满，a,b 无法立即运行，保持排队。
	mustOK(t, s.MarkNodeUnhealthy("n1"))
	assertState(t, s, "a", StateQueued)
	assertState(t, s, "b", StateQueued)
	assertState(t, s, "c", StateRunning)
	if got := s.Ledger().Allocated("n1"); got.AnyPositive() {
		t.Fatalf("n1 should have zero allocation after failure, got %s", got)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after failure: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())

	// c 完成后 n2 全空：被驱逐的 a,b 应被重新调度安置（可迁移到健康节点）
	mustOK(t, s.Complete("c"))
	assertState(t, s, "a", StateRunning)
	assertState(t, s, "b", StateRunning)
	j1, _ := s.GetJob("a")
	j2, _ := s.GetJob("b")
	if j1.NodeName != "n2" || j2.NodeName != "n2" {
		t.Fatalf("evicted jobs should migrate to healthy n2: a=%s b=%s", j1.NodeName, j2.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after migration: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())

	// n1 恢复后应可再次承载作业；且恢复调用是幂等校验的（重复恢复报错）
	mustOK(t, s.MarkNodeHealthy("n1"))
	if err := s.MarkNodeHealthy("n1"); err == nil {
		t.Fatalf("double healthy transition should fail")
	}
	d := s.Submit(Job{ID: "d", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if d.State != StateRunning {
		t.Fatalf("d should run on recovered n1, got %s %s", d.State, d.Reason)
	}
	jd, _ := s.GetJob("d")
	if jd.NodeName != "n1" {
		t.Fatalf("d on %s, want n1", jd.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after recovery: %v", err)
	}
}

func TestEvictedJobKeepsQueuePosition(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 4, MemoryMB: 8000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 占满集群
	for _, id := range []string{"x1", "x2", "x3", "x4"} {
		r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: 2, MemoryMB: 4000}})
		if r.State != StateRunning {
			t.Fatalf("%s should run: %s", id, r.Reason)
		}
	}
	// n1 故障：其上两个作业被驱逐并排队（保留原 SubmitSeq）
	mustOK(t, s.MarkNodeUnhealthy("n1"))
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	// n2 仍健康，上面两个作业不受影响
	evicted := []string{}
	for _, id := range []string{"x1", "x2", "x3", "x4"} {
		j, _ := s.GetJob(id)
		if j.State == StateQueued {
			evicted = append(evicted, id)
		}
		if j.State == StateRunning && j.NodeName != "n2" {
			t.Fatalf("healthy-node job %s unexpectedly on %s", id, j.NodeName)
		}
	}
	if len(evicted) != 2 {
		t.Fatalf("expected 2 evicted, got %v", evicted)
	}
	// 流水必须包含每个被驱逐作业的 EVICT（释放）与重新排队事件
	evs := s.Ledger().Events()
	evictCnt, queueCnt := 0, 0
	for _, e := range evs {
		if e.Type == EvEvict {
			evictCnt++
		}
		if e.Type == EvQueue && e.Detail == "requeue after eviction" {
			queueCnt++
		}
	}
	if evictCnt != 2 || queueCnt != 2 {
		t.Fatalf("expected 2 evict + 2 requeue events, got %d/%d", evictCnt, queueCnt)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}
