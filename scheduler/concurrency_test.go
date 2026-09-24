package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// TestConcurrentSubmitNoDoubleAllocate 大量并发提交超过集群容量的作业：
// 任意时刻每个节点的分配不得超卖，运行作业总数不超物理承载，账目始终守恒。
func TestConcurrentSubmitNoDoubleAllocate(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddNode("n2", Resources{CPU: 16, MemoryMB: 32000, GPU: 4}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	const submitters = 16
	const per = 25
	var wg sync.WaitGroup
	var runningAtEnd, queuedAtEnd int64
	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := fmt.Sprintf("w%d-j%02d", g, i)
				r := s.Submit(Job{ID: id, Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 1000, GPU: 1}})
				if !r.Accepted {
					t.Errorf("submit %s unexpectedly rejected: %s %s", id, r.Reason, r.Detail)
					return
				}
				switch r.State {
				case StateRunning:
					atomic.AddInt64(&runningAtEnd, 1)
				case StateQueued:
					atomic.AddInt64(&queuedAtEnd, 1)
				}
			}
		}(g)
	}
	wg.Wait()

	// 集群总量：32 CPU / 8 GPU。每个作业 1 CPU + 1 GPU => GPU 是瓶颈，最多 8 个运行。
	j, _ := s.GetJob("w0-j00")
	_ = j
	if err := s.CheckConservation(); err != nil {
		t.Fatalf("conservation after concurrent submit: %v", err)
	}
	snap := s.Snapshot()
	t.Logf("\n%s", snap)

	// 统计最终运行数并逐节点核对未超卖
	total := submitters * per
	running := countByState(s, StateRunning)
	queued := countByState(s, StateQueued)
	if running+queued != total {
		t.Fatalf("accounting: running=%d queued=%d, sum=%d want %d", running, queued, running+queued, total)
	}
	if running > 8 {
		t.Fatalf("GPU bottleneck violated: %d running > 8", running)
	}
	if queued != total-8 {
		t.Fatalf("queued=%d want %d", queued, total-8)
	}
	// 每个作业至多出现在一个节点上
	if dup := findDoublePlacement(s); dup != "" {
		t.Fatalf("%s", dup)
	}
	t.Logf("\n%s", s.Ledger().FormatLog())
}

// TestConcurrentSubmitCancelComplete 在并发提交的同时并发取消/完成作业，
// 检验资源回收无泄漏、无重复分配。
func TestConcurrentSubmitCancelComplete(t *testing.T) {
	s := New(WithClock(fakeClock()))
	mustOK(t, s.AddNode("n1", Resources{CPU: 8, MemoryMB: 16000}))
	mustOK(t, s.AddQueue(QueueConfig{Name: "batch", Weight: 1}))

	// 预放 8 个占 1 CPU 的作业把集群打满
	for i := 0; i < 8; i++ {
		r := s.Submit(Job{ID: fmt.Sprintf("seed%02d", i), Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 1000}})
		if !r.Accepted || r.State != StateRunning {
			t.Fatalf("seed %d not running: %+v", i, r)
		}
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 提交者：不断提交小作业，超出的会排队
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
			s.Submit(Job{ID: fmt.Sprintf("flow%05d", i), Queue: "batch", Request: Resources{CPU: 1, MemoryMB: 1000}})
			i++
		}
	}()

	// 回收者：不断完成/取消运行中作业（通过快照随机选取不稳定，改用固定 seed 集 + flow 取消）
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i < 200 {
				_ = s.Complete(fmt.Sprintf("seed%02d", i%8)) // 已完成的会返回错误，忽略即可
			} else {
				_ = s.Cancel(fmt.Sprintf("flow%05d", i-200))
			}
		}
	}()

	// 巡检者：持续核对守恒
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
		}
	}()

	// 让混乱运行一小段
	for k := 0; k < 300; k++ {
		s.Tick()
	}
	close(stop)
	wg.Wait()

	if err := s.CheckConservation(); err != nil {
		t.Fatalf("final conservation: %v", err)
	}
	if dup := findDoublePlacement(s); dup != "" {
		t.Fatalf("%s", dup)
	}
	running := countByState(s, StateRunning)
	if running > 8 {
		t.Fatalf("capacity exceeded: %d running with 8 CPU total", running)
	}
	t.Logf("\n%s", s.Snapshot())
	t.Logf("final running=%d, ledger events=%d", running, len(s.Ledger().Events()))
}

func countByState(s *Scheduler, want JobState) int {
	// 通过快照外的内部视图在测试包内直接统计
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, j := range s.jobs {
		if j.State == want {
			n++
		}
	}
	return n
}

func findDoublePlacement(s *Scheduler) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[string]string)
	for name, n := range s.nodes {
		var used Resources
		for jobID, req := range n.jobs {
			if prev, ok := seen[jobID]; ok {
				return fmt.Sprintf("job %s placed on both %s and %s", jobID, prev, name)
			}
			seen[jobID] = name
			used = used.Add(req)
			if !n.Total.Fits(used) {
				return fmt.Sprintf("node %s overallocated: used %s > total %s", name, used, n.Total)
			}
		}
	}
	return ""
}
