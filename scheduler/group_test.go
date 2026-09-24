package scheduler

import "testing"

func TestSubmitGroupAtomicPlacement(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 整组 3 个作业合计 6 CPU > 4 => 全部排队，不得有任何部分占用
	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 2, MemoryMB: 2000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 2, MemoryMB: 2000}},
		{ID: "g3", Queue: "batch", GroupID: "G", Request: Resources{CPU: 2, MemoryMB: 2000}},
	})
	for _, r := range res {
		if r.State != StateQueued || r.Reason != ReasonQueuedGroupAtomic {
			t.Fatalf("%s should queue atomically, got state=%s reason=%s", r.JobID, r.State, r.Reason)
		}
	}
	if used := s.Ledger().TotalAllocated(); used.AnyPositive() {
		t.Fatalf("no resource may be partially allocated, total allocated=%s", used)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())
}

func TestSubmitGroupFitsAtomically(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000, GPU: 2}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 4, MemoryMB: 8000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 两个 3-CPU 作业：任一节点都放不下两个，必须分散到两个节点，同一决策内同时放置
	res := s.SubmitGroup([]Job{
		{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 3, MemoryMB: 2000}},
		{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 3, MemoryMB: 2000}},
	})
	for _, r := range res {
		if r.State != StateRunning || r.Reason != ReasonOK {
			t.Fatalf("%s should run atomically, got %+v", r.JobID, r)
		}
	}
	j1, _ := s.GetJob("g1")
	j2, _ := s.GetJob("g2")
	if j1.NodeName == j2.NodeName {
		t.Fatalf("group members must spread across nodes, both on %s", j1.NodeName)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

func TestSubmitGroupInvalidMemberRejectsWholeGroup(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	cases := []struct {
		name string
		jobs []Job
		want ReasonCode
	}{
		{
			"missing group id on a member",
			[]Job{
				{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 1}},
				{ID: "g2", Queue: "batch", Request: Resources{CPU: 1}},
			},
			ReasonRejectGroupMismatch,
		},
		{
			"different group ids",
			[]Job{
				{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 1}},
				{ID: "g2", Queue: "batch", GroupID: "H", Request: Resources{CPU: 1}},
			},
			ReasonRejectGroupMismatch,
		},
		{
			"one member infeasible",
			[]Job{
				{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 1}},
				{ID: "g2", Queue: "batch", GroupID: "G", Request: Resources{CPU: 99}},
			},
			ReasonRejectInfeasible,
		},
		{
			"one member references bad queue",
			[]Job{
				{ID: "g1", Queue: "batch", GroupID: "G", Request: Resources{CPU: 1}},
				{ID: "g2", Queue: "ghost", GroupID: "G", Request: Resources{CPU: 1}},
			},
			ReasonRejectNoQueue,
		},
	}
	for _, c := range cases {
		res := s.SubmitGroup(c.jobs)
		for i, r := range res {
			if r.Accepted {
				t.Fatalf("%s: member %d accepted, whole group must reject", c.name, i)
			}
		}
		// 至少有一个成员的原因为具体校验失败
		found := false
		for _, r := range res {
			if r.Reason == c.want {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: expected at least one reason %s, got %+v", c.name, c.want, res)
		}
		if _, ok := s.GetJob(c.jobs[0].ID); ok {
			t.Fatalf("%s: rejected group must leave no jobs behind", c.name)
		}
		if used := s.Ledger().TotalAllocated(); used.AnyPositive() {
			t.Fatalf("%s: rejected group allocated resources: %s", c.name, used)
		}
		t.Logf("%s => %+v", c.name, res)
	}
}

func TestQueuedGroupReleasedAllowsOtherQueues(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "a", Weight: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "b", Weight: 1}))

	// 队列 a 放一个占满节点的作业
	big := s.Submit(Job{ID: "big", Queue: "a", Request: Resources{CPU: 4, MemoryMB: 8000, GPU: 2}})
	if big.State != StateRunning {
		t.Fatalf("big: %+v", big)
	}
	// 队列 b 提交需要整节点的作业组 => 排队
	res := s.SubmitGroup([]Job{
		{ID: "gp1", Queue: "b", GroupID: "GP", Request: Resources{CPU: 2, MemoryMB: 4000, GPU: 1}},
		{ID: "gp2", Queue: "b", GroupID: "GP", Request: Resources{CPU: 2, MemoryMB: 4000, GPU: 1}},
	})
	for _, r := range res {
		if r.State != StateQueued {
			t.Fatalf("%s should queue: %+v", r.JobID, r)
		}
	}
	// big 完成，整组应在同一决策里一起运行
	mustOK(t, s.Complete("big"))
	assertState(t, s, "gp1", StateRunning)
	assertState(t, s, "gp2", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}
