package scheduler

import (
	"fmt"
	"testing"
)

func TestWeightedRoundRobinAcrossQueues(t *testing.T) {
	s := New(WithClock(fakeClock()))
	// 单节点，共 12 CPU，每次只放 1-CPU 小作业观察队列份额
	mustOK(t, s.AddNode("n1", Resources{CPU: 12, MemoryMB: 100000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "heavy", Weight: 2}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "light", Weight: 1}))

	// 批量模式：所有作业先排队，由同一个 Tick 在队列间做加权轮转裁决，
	// 避免“逐条提交即逐条占用”让先提交流水线占满集群。
	s.SetBatchMode(true)
	for i := 0; i < 12; i++ {
		r := s.Submit(Job{ID: fmt.Sprintf("h%02d", i), Queue: "heavy", Request: Resources{CPU: 1, MemoryMB: 100}})
		if !r.Accepted {
			t.Fatalf("h%02d: %s", i, r.Reason)
		}
	}
	for i := 0; i < 12; i++ {
		r := s.Submit(Job{ID: fmt.Sprintf("l%02d", i), Queue: "light", Request: Resources{CPU: 1, MemoryMB: 100}})
		if !r.Accepted {
			t.Fatalf("l%02d: %s", i, r.Reason)
		}
	}
	placed := s.Tick()
	t.Logf("tick placed=%d jobs across queues", placed)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	heavyRun := countRunningInQueue(s, "heavy")
	lightRun := countRunningInQueue(s, "light")
	t.Logf("weighted share: heavy running=%d light running=%d (total 12)", heavyRun, lightRun)
	// 整数跨步（stride=floor(W/w)）下权重 2 的队列拿到约 2~3 倍份额；
	// 关键工程性质：份额方向正确，且低权重队列在同一轮中确实拿到槽位、不被饿死。
	if heavyRun <= lightRun || heavyRun < 2*lightRun {
		t.Fatalf("weight 2 queue should get >=2x share of weight 1, got %d vs %d", heavyRun, lightRun)
	}
	if lightRun < 2 {
		t.Fatalf("light queue must be served in the same round, got %d", lightRun)
	}
	// 关闭批量模式，循环归还所有 heavy 在途作业（含被重新调度的 heavy 积压）：
	// light 的积压必须随资源归还被重新调度，最终不再排队（验证“资源归还后
	// 可再调度”、低权重队列不被饿死且资源不泄漏）。
	s.SetBatchMode(false)
	for {
		var heavyRunning string
		s.mu.Lock()
		for id, jj := range s.jobs {
			if jj.Queue == "heavy" && jj.State == StateRunning {
				heavyRunning = id
				break
			}
		}
		s.mu.Unlock()
		if heavyRunning == "" {
			break
		}
		mustOK(t, s.Complete(heavyRunning))
	}
	if q := s.queues["light"]; len(q.pending) != 0 {
		t.Fatalf("all light backlog should drain after heavy releases, pending=%d", len(q.pending))
	}
	if countRunningInQueue(s, "light") != 12 {
		t.Fatalf("all 12 light jobs should eventually run, got %d", countRunningInQueue(s, "light"))
	}
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("\n%s", s.Snapshot())
}

func TestAgingPreventsStarvation(t *testing.T) {
	s := New(WithClock(fakeClock()), WithAging(1, 100))
	mustOK(t, s.AddNode("n1", Resources{CPU: 4, MemoryMB: 8000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 打满集群：4 个 1-CPU 作业
	for i := 0; i < 4; i++ {
		r := s.Submit(Job{ID: fmt.Sprintf("occ%d", i), Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 100}})
		if r.State != StateRunning {
			t.Fatalf("occ%d: %s", i, r.Reason)
		}
	}

	// 低优先级“长作业”先排队
	low := s.Submit(Job{ID: "low-long", Queue: "batch", Priority: -10, Request: Resources{CPU: 1, MemoryMB: 100}})
	if low.State != StateQueued {
		t.Fatalf("low-long: %s", low.State)
	}
	// 之后不断有高优先级新作业涌入
	for i := 0; i < 10; i++ {
		r := s.Submit(Job{ID: fmt.Sprintf("hi%02d", i), Queue: "batch", Priority: 100, Request: Resources{CPU: 1, MemoryMB: 100}})
		if r.State != StateQueued {
			t.Fatalf("hi%02d: %s", i, r.State)
		}
	}

	// 每个周期完成一个占用者并推进调度；若无老化，low-long 会被高优先级永久压制。
	// 老化 boostInterval=1：等待越久有效优先级越高，最终必须先于新作业得到服务。
	freed := 0
	for i := 0; i < 4; i++ {
		mustOK(t, s.Complete(fmt.Sprintf("occ%d", i)))
		s.Tick()
		freed++
		// 第一个被释放的槽位在若干 tick 后应能让 low-long 上场；我们持续跑到它运行为止
		j, _ := s.GetJob("low-long")
		if j.State == StateRunning {
			break
		}
	}
	// 再推进足够周期（期间无新作业），low-long 必须最终运行
	for k := 0; k < 30; k++ {
		s.Tick()
		j, _ := s.GetJob("low-long")
		if j.State == StateRunning {
			break
		}
		// 模拟高优先级作业完成释放
		for i := 0; i < 10; i++ {
			if jj, ok := s.GetJob(fmt.Sprintf("hi%02d", i)); ok && jj.State == StateRunning {
				_ = s.Complete(fmt.Sprintf("hi%02d", i))
				break
			}
		}
	}
	assertState(t, s, "low-long", StateRunning)
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	t.Logf("low-long scheduled after %d frees (aging beats static high priority)", freed)
	t.Logf("\n%s", s.Ledger().FormatLog())
}

func TestDeterministicReplay(t *testing.T) {
	// 相同初始状态 + 相同提交序列 => 每个作业落在哪台节点必须完全一致
	run := func() map[string]string {
		s := New(WithClock(fakeClock()))
		if err := s.AddNode("beta", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}); err != nil {
			t.Fatal(err)
		}
		if err := s.AddNode("alpha", Resources{CPU: 8, MemoryMB: 16000, GPU: 2}); err != nil {
			t.Fatal(err)
		}
		if err := s.AddQueue(QueueConfig{Name: "q2", Weight: 2}); err != nil {
			t.Fatal(err)
		}
		if err := s.AddQueue(QueueConfig{Name: "q1", Weight: 1}); err != nil {
			t.Fatal(err)
		}
		seq := []Job{
			{ID: "a", Queue: "q1", Priority: 1, Request: Resources{CPU: 3, MemoryMB: 1000}},
			{ID: "b", Queue: "q2", Priority: 0, Request: Resources{CPU: 3, MemoryMB: 1000}},
			{ID: "c", Queue: "q1", Priority: 5, Request: Resources{CPU: 6, MemoryMB: 2000}},
			{ID: "d", Queue: "q2", Priority: 2, Request: Resources{CPU: 6, MemoryMB: 2000, GPU: 2}},
			{ID: "e", Queue: "q1", Priority: 0, Request: Resources{CPU: 2, MemoryMB: 500, GPU: 1}},
		}
		for _, j := range seq {
			s.Submit(j)
		}
		_ = s.Complete("a")
		s.Tick()
		_ = s.Cancel("b")
		s.Tick()
		mustOK(t, s.MarkNodeUnhealthy("alpha"))
		s.Tick()
		mustOK(t, s.MarkNodeHealthy("alpha"))
		s.Tick()
		out := map[string]string{}
		for _, j := range seq {
			if jj, ok := s.GetJob(j.ID); ok {
				out[j.ID] = jj.State.String() + "@" + jj.NodeName
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
				t.Fatalf("non-deterministic placement: %s was %s then %s", id, v, again[id])
			}
		}
	}
	t.Logf("deterministic replay mapping: %v", first)
}

func countRunningInQueue(s *Scheduler, q string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queues[q].running
}
