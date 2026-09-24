package scheduler

import (
	"testing"
)

func newTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "gpu", Weight: 1, MaxJobs: 4}))
	return s
}

func mustOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertState(t *testing.T, s *Scheduler, id string, want JobState) {
	t.Helper()
	j, ok := s.GetJob(id)
	if !ok {
		t.Fatalf("job %s not found", id)
	}
	if j.State != want {
		t.Fatalf("job %s state = %s, want %s (reason=%s detail=%s)", id, j.State, want, j.Reason, j.Detail)
	}
}

func TestSubmitImmediateAndQueued(t *testing.T) {
	s := newTestScheduler(t)

	r := s.Submit(Job{ID: "a", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 4000}})
	if !r.Accepted || r.State != StateRunning || r.Reason != ReasonOK {
		t.Fatalf("a should run immediately, got %+v", r)
	}
	// a2 与 a 平分 n1，保证只完成 a 时 n1 仍有一半被占
	r = s.Submit(Job{ID: "a2", Queue: "batch", Request: Resources{CPU: 4, MemoryMB: 4000}})
	if !r.Accepted || r.State != StateRunning {
		t.Fatalf("a2 should run immediately, got %+v", r)
	}

	// 两个占满单节点的作业：第二个必须排队，绝不部分占用
	r = s.Submit(Job{ID: "b", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if !r.Accepted || r.State != StateRunning {
		t.Fatalf("b should run on n2, got %+v", r)
	}
	r = s.Submit(Job{ID: "c", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if !r.Accepted || r.State != StateQueued || r.Reason != ReasonQueuedInsufficient {
		t.Fatalf("c should be queued with distinguishable reason, got %+v", r)
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())

	// 完成 a 后 n1 仍被 a2 占用一半，放不下 c；完成 b 后 n2 全空，c 必须被唤醒
	mustOK(t, s.Complete("a"))
	assertState(t, s, "c", StateQueued)
	mustOK(t, s.Complete("b"))
	assertState(t, s, "c", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after wake: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

func TestRejectReasonsAreDistinguishable(t *testing.T) {
	s := newTestScheduler(t)

	cases := []struct {
		name string
		job  Job
		want ReasonCode
	}{
		{"zero request", Job{ID: "r0", Queue: "batch", Request: Resources{}}, ReasonRejectInvalid},
		{"negative cpu", Job{ID: "r1", Queue: "batch", Request: Resources{CPU: -1}}, ReasonRejectInvalid},
		{"no queue", Job{ID: "r2", Queue: "ghost", Request: Resources{CPU: 1}}, ReasonRejectNoQueue},
		{"too big", Job{ID: "r3", Queue: "batch", Request: Resources{CPU: 9, MemoryMB: 1}}, ReasonRejectInfeasible},
	}
	for _, c := range cases {
		r := s.Submit(c.job)
		if r.Accepted {
			t.Fatalf("%s: should be rejected", c.name)
		}
		if r.Reason != c.want {
			t.Fatalf("%s: reason=%s want %s", c.name, r.Reason, c.want)
		}
		// 拒绝与排队必须可区分：排队原因是 QUEUED_*
		if r.Reason == ReasonQueuedInsufficient || r.Reason == ReasonQueuedGroupAtomic {
			t.Fatalf("%s: rejection must not carry queued reason", c.name)
		}
		t.Logf("reject case %-14s => %s (%s)", c.name, r.Reason, r.Detail)
	}
	// 重复 ID
	_ = s.Submit(Job{ID: "dup", Queue: "batch", Request: Resources{CPU: 1}})
	r := s.Submit(Job{ID: "dup", Queue: "batch", Request: Resources{CPU: 1}})
	if r.Reason != ReasonRejectDuplicate {
		t.Fatalf("duplicate id: reason=%s want %s", r.Reason, ReasonRejectDuplicate)
	}
	// 队列容量：gpu 队列 MaxJobs=4
	for i := 0; i < 4; i++ {
		if rr := s.Submit(Job{ID: "g" + string(rune('0'+i)), Queue: "gpu", Request: Resources{CPU: 8}}); !rr.Accepted {
			t.Fatalf("gpu job %d: %+v", i, rr)
		}
	}
	if rr := s.Submit(Job{ID: "gx", Queue: "gpu", Request: Resources{CPU: 1}}); rr.Reason != ReasonRejectQueueFull {
		t.Fatalf("queue full: reason=%s want %s", rr.Reason, ReasonRejectQueueFull)
	}
}

func TestCancelReleasesResources(t *testing.T) {
	s := newTestScheduler(t)
	// n1 与 n2 各放一个占满节点的作业
	r1 := s.Submit(Job{ID: "big1", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	r2 := s.Submit(Job{ID: "big2", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if !r1.Accepted || !r2.Accepted {
		t.Fatalf("big jobs should both run: %+v %+v", r1, r2)
	}
	wait := s.Submit(Job{ID: "waiter", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if wait.State != StateQueued {
		t.Fatalf("waiter should queue, got %s", wait.State)
	}

	// 取消排队作业不应改变账目
	mustOK(t, s.Cancel("waiter"))
	assertState(t, s, "waiter", StateCancelled)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after queued cancel: %v", err)
	}

	// 重新排队一个等待者，再取消运行中的 big1：资源必须被释放并唤醒它
	w2 := s.Submit(Job{ID: "waiter2", Queue: "batch", Request: Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if w2.State != StateQueued {
		t.Fatalf("waiter2 should queue, got %s", w2.State)
	}
	mustOK(t, s.Cancel("big1"))
	assertState(t, s, "big1", StateCancelled)
	assertState(t, s, "waiter2", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after running cancel: %v", err)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}
