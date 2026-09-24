package scheduler

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

// ---------- 测试辅助 ----------

func testNodes() []Node {
	return []Node{
		{ID: "node-a", Capacity: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}},
		{ID: "node-b", Capacity: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}},
	}
}

func testQueues() []Queue {
	return []Queue{{Name: "lo", Weight: 1}, {Name: "hi", Weight: 2}}
}

func dumpLogs(t *testing.T, s *Scheduler) {
	t.Helper()
	t.Log("---- 判定依据 ----")
	for _, d := range s.Decisions() {
		t.Log(d)
	}
	t.Log("---- 分配流水 ----")
	for _, l := range s.Ledger().Log() {
		t.Log(l)
	}
}

func mustConserve(t *testing.T, s *Scheduler) {
	t.Helper()
	if err := s.Ledger().CheckConservation(); err != nil {
		t.Fatalf("账目不守恒: %v", err)
	}
}

// runningSum 汇总所有 RUNNING 作业的声明资源，应与账目已用量一致。
func runningSum(s *Scheduler) Resource {
	sum := Resource{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.running {
		sum = sum.Add(j.Request)
	}
	return sum
}

// ---------- 1. 并发提交：账目守恒、不重复分配 ----------

func TestConcurrentSubmitConservation(t *testing.T) {
	s := NewScheduler(testNodes(), testQueues())
	const n = 64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := "lo"
			if i%2 == 0 {
				q = "hi"
			}
			r := s.Submit(Job{
				ID:      fmt.Sprintf("job-%02d", i),
				Queue:   q,
				Request: Resource{CPUMilli: 500, MemMiB: 512},
			})
			if !r.Accepted {
				t.Errorf("job-%02d 被拒绝: %s", i, r.Reason)
			}
		}(i)
	}
	wg.Wait()

	// 多轮调度直到无新调度（容量有限，部分作业保持排队）。
	total := 0
	for round := 0; round < 20; round++ {
		got := s.ScheduleOnce()
		total += len(got)
		if len(got) == 0 {
			break
		}
	}
	mustConserve(t, s)

	used := s.Ledger().Used()
	if !used.Fits(s.Ledger().Total()) {
		t.Fatalf("已用 %s 超过健康节点总容量 %s", used, s.Ledger().Total())
	}
	if rs := runningSum(s); rs != used {
		t.Fatalf("运行作业资源合计 %s 与账目已用 %s 不一致", rs, used)
	}
	if total == 0 {
		t.Fatal("并发提交后没有任何作业被调度")
	}
	t.Logf("调度运行 %d 个作业，排队 %d 个，已用 %s / %s",
		total, len(s.PendingIDs()), used, s.Ledger().Total())
	dumpLogs(t, s)
}

// ---------- 2. 拒绝与排队原因可区分 ----------

func TestRejectReasonsDistinguishable(t *testing.T) {
	s := NewScheduler(testNodes(), testQueues())

	// 未知队列。
	if r := s.Submit(Job{ID: "bad-q", Queue: "nope", Request: Resource{CPUMilli: 100}}); r.Accepted || r.Reason != RejectUnknownQueue {
		t.Fatalf("未知队列应拒绝(%s)，得到 %+v", RejectUnknownQueue, r)
	}
	// 超过集群总容量（永久不可行）。
	if r := s.Submit(Job{ID: "too-big", Queue: "lo", Request: Resource{CPUMilli: 99999}}); r.Accepted || r.Reason != RejectExceedsCluster {
		t.Fatalf("超容量应拒绝(%s)，得到 %+v", RejectExceedsCluster, r)
	}
	// 非法声明。
	if r := s.Submit(Job{ID: "neg", Queue: "lo", Request: Resource{CPUMilli: -1}}); r.Accepted || r.Reason != RejectInvalid {
		t.Fatalf("负资源应拒绝(%s)，得到 %+v", RejectInvalid, r)
	}
	// 暂时资源不足：接受但保持排队，判定日志含 WAIT_INSUFFICIENT_RESOURCE。
	s.Submit(Job{ID: "fill-a", Queue: "lo", Request: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}})
	s.Submit(Job{ID: "fill-b", Queue: "lo", Request: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}})
	s.Submit(Job{ID: "waiter", Queue: "lo", Request: Resource{CPUMilli: 1000}})
	s.ScheduleOnce()
	if st, _ := s.State("waiter"); st != JobPending {
		t.Fatalf("资源不足时应保持排队，实际状态 %s", st)
	}
	found := false
	for _, d := range s.Decisions() {
		if strings.Contains(d, "job=waiter") && strings.Contains(d, WaitInsufficientResource.String()) {
			found = true
		}
	}
	if !found {
		t.Fatal("判定日志缺少 waiter 的 WAIT_INSUFFICIENT_RESOURCE 记录")
	}
	mustConserve(t, s)
	dumpLogs(t, s)
}

// ---------- 3. 作业组：资源不足时整体等待、绝不部分占用 ----------

func TestGangAllOrNothing(t *testing.T) {
	s := NewScheduler(testNodes(), testQueues())

	// 占满 node-a，使剩余容量不足以放下整个组。
	s.Submit(Job{ID: "filler", Queue: "lo", Request: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}})
	s.ScheduleOnce()

	// 组 g1 需要 2×(4000m,8192Mi,1gpu)，当前只剩一个节点的容量。
	s.Submit(Job{ID: "g1-0", Queue: "lo", GroupID: "g1", Request: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}})
	s.Submit(Job{ID: "g1-1", Queue: "lo", GroupID: "g1", Request: Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}})
	s.ScheduleOnce()

	for _, id := range []string{"g1-0", "g1-1"} {
		if st, _ := s.State(id); st != JobPending {
			t.Fatalf("组资源不足时成员 %s 应保持排队，实际 %s", id, st)
		}
	}
	// 账目上不得有部分占用：除 filler 外无其他占用。
	used := s.Ledger().Used()
	want := Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}
	if used != want {
		t.Fatalf("组未整体调度却产生部分占用: used=%s want=%s", used, want)
	}
	mustConserve(t, s)

	// 释放 filler 后，整组应能被原子调度。
	s.Cancel("filler")
	got := s.ScheduleOnce()
	if len(got) != 2 {
		t.Fatalf("资源归还后整组应被调度，实际调度 %v", got)
	}
	for _, id := range []string{"g1-0", "g1-1"} {
		if st, _ := s.State(id); st != JobRunning {
			t.Fatalf("成员 %s 应运行，实际 %s", id, st)
		}
	}
	mustConserve(t, s)
	dumpLogs(t, s)
}

// ---------- 4. 节点故障：回收资源、整组重排队、恢复后可再调度 ----------

func TestNodeFailureReclaim(t *testing.T) {
	s := NewScheduler(testNodes(), testQueues())

	s.Submit(Job{ID: "solo", Queue: "lo", Request: Resource{CPUMilli: 2000, MemMiB: 1024}})
	s.Submit(Job{ID: "g1-0", Queue: "lo", GroupID: "g1", Request: Resource{CPUMilli: 2000, MemMiB: 1024}})
	s.Submit(Job{ID: "g1-1", Queue: "lo", GroupID: "g1", Request: Resource{CPUMilli: 2000, MemMiB: 1024}})
	s.ScheduleOnce()
	if got := len(s.RunningIDs()); got != 3 {
		t.Fatalf("预期 3 个作业运行，实际 %d", got)
	}
	mustConserve(t, s)

	// 使 node-a 故障：其上作业被回收；gang 组任一成员受影响则整组重排队。
	down := "node-a"
	affected := s.NodeDown(down)
	t.Logf("节点 %s 故障，受影响作业: %v", down, affected)
	mustConserve(t, s)

	// 故障节点上的资源必须被回收：已用量不超过幸存节点容量。
	if used := s.Ledger().Used(); !used.Fits(Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}) {
		t.Fatalf("故障回收后已用 %s 超过幸存节点容量", used)
	}
	// 受影响作业重新排队，未受影响的保持运行。
	for _, id := range affected {
		if st, _ := s.State(id); st != JobPending {
			t.Fatalf("受影响作业 %s 应重新排队，实际 %s", id, st)
		}
	}
	// gang 语义：若组内任一成员受影响，则全组都不得在运行。
	g1Running := 0
	for _, id := range []string{"g1-0", "g1-1"} {
		if st, _ := s.State(id); st == JobRunning {
			g1Running++
		}
	}
	if g1Running == 1 {
		t.Fatal("gang 组部分成员仍在运行，违反整体生存语义")
	}

	// 节点恢复后资源归还，排队作业可被重新调度。
	s.NodeUp(down)
	s.ScheduleOnce()
	mustConserve(t, s)
	for _, id := range affected {
		if st, _ := s.State(id); st != JobRunning {
			t.Fatalf("节点恢复后 %s 应重新运行，实际 %s", id, st)
		}
	}
	dumpLogs(t, s)
}

// ---------- 5. 取消：运行中与排队中的资源回收 ----------

func TestCancelReleasesResources(t *testing.T) {
	nodes := []Node{{ID: "node-a", Capacity: Resource{CPUMilli: 4000, MemMiB: 8192}}}
	s := NewScheduler(nodes, testQueues())
	s.Submit(Job{ID: "run-me", Queue: "lo", Request: Resource{CPUMilli: 2000, MemMiB: 1024}})
	s.Submit(Job{ID: "wait-me", Queue: "lo", Request: Resource{CPUMilli: 3000, MemMiB: 1024}})
	s.ScheduleOnce()
	if st, _ := s.State("wait-me"); st != JobPending {
		t.Fatalf("wait-me 应排队，实际 %s", st)
	}

	if !s.Cancel("run-me") {
		t.Fatal("取消运行中作业失败")
	}
	if !s.Ledger().Used().IsZero() {
		t.Fatalf("取消后资源未完全回收: %s", s.Ledger().Used())
	}
	if !s.Cancel("wait-me") {
		t.Fatal("取消排队作业失败")
	}
	if len(s.PendingIDs()) != 0 {
		t.Fatalf("排队作业未被移除: %v", s.PendingIDs())
	}
	// 已取消的作业再次取消应失败。
	if s.Cancel("run-me") {
		t.Fatal("重复取消应返回 false")
	}
	mustConserve(t, s)
	dumpLogs(t, s)
}

// ---------- 6. 公平与防饿死：低优先级作业在有限轮次内获得调度 ----------

func TestFairnessNoStarvation(t *testing.T) {
	nodes := []Node{{ID: "node-a", Capacity: Resource{CPUMilli: 1000, MemMiB: 1024}}}
	s := NewScheduler(nodes, []Queue{{Name: "lo", Weight: 1}})

	// 低优先级长作业先到。
	s.Submit(Job{ID: "low", Queue: "lo", Priority: 0, Request: Resource{CPUMilli: 1000, MemMiB: 1024}})

	// 每轮都到来一个高优先级短作业（运行一轮后“完成”即取消），
	// 若无防饿死加成，low 将被持续饿死。
	ran := -1
	for round := 0; round < 30; round++ {
		hi := fmt.Sprintf("hi-%02d", round)
		s.Submit(Job{ID: hi, Queue: "lo", Priority: 5, Request: Resource{CPUMilli: 1000, MemMiB: 1024}})
		s.ScheduleOnce()
		if st, _ := s.State("low"); st == JobRunning {
			ran = round
			break
		}
		// 高优先级作业完成，释放资源进入下一轮。
		s.Cancel(hi)
		mustConserve(t, s)
	}
	if ran < 0 {
		t.Fatal("低优先级作业在 30 轮内未获调度，发生饿死")
	}
	t.Logf("低优先级作业在第 %d 轮获得调度（防饿死加成生效）", ran)
	mustConserve(t, s)
	dumpLogs(t, s)
}

// ---------- 7. 多队列公平：权重份额低者优先 ----------

func TestWeightedFairShare(t *testing.T) {
	nodes := []Node{{ID: "node-a", Capacity: Resource{CPUMilli: 4000, MemMiB: 4096}}}
	s := NewScheduler(nodes, []Queue{{Name: "a", Weight: 1}, {Name: "b", Weight: 3}})

	// 两队各提交 4 个等资源作业，单节点容量仅够 4 个。
	for i := 0; i < 4; i++ {
		s.Submit(Job{ID: fmt.Sprintf("a-%d", i), Queue: "a", Request: Resource{CPUMilli: 1000, MemMiB: 1024}})
		s.Submit(Job{ID: fmt.Sprintf("b-%d", i), Queue: "b", Request: Resource{CPUMilli: 1000, MemMiB: 1024}})
	}
	s.ScheduleOnce()
	mustConserve(t, s)

	running := s.RunningIDs()
	countA, countB := 0, 0
	for _, id := range running {
		if strings.HasPrefix(id, "a-") {
			countA++
		} else {
			countB++
		}
	}
	t.Logf("运行分布: a=%d b=%d (权重 1:3)", countA, countB)
	// 加权公平：b 的份额目标 3 倍于 a，容量 4 份时应为 a=1,b=3。
	if countA != 1 || countB != 3 {
		t.Fatalf("加权公平分配不符预期: a=%d b=%d，期望 a=1 b=3", countA, countB)
	}
	dumpLogs(t, s)
}

// ---------- 8. 可复现性：相同初始状态与提交序列 → 相同调度结论 ----------

func TestReproducibility(t *testing.T) {
	run := func() ([]string, []string) {
		s := NewScheduler(testNodes(), testQueues())
		jobs := []Job{
			{ID: "j1", Queue: "lo", Priority: 1, Request: Resource{CPUMilli: 3000, MemMiB: 2048}},
			{ID: "j2", Queue: "hi", Priority: 0, Request: Resource{CPUMilli: 2000, MemMiB: 2048}},
			{ID: "j3", Queue: "lo", GroupID: "g", Request: Resource{CPUMilli: 2000, MemMiB: 1024}},
			{ID: "j4", Queue: "hi", GroupID: "g", Request: Resource{CPUMilli: 2000, MemMiB: 1024}},
			{ID: "j5", Queue: "hi", Request: Resource{CPUMilli: 1000, MemMiB: 512}},
		}
		for _, j := range jobs {
			s.Submit(j)
		}
		all := []string{}
		for i := 0; i < 5; i++ {
			all = append(all, s.ScheduleOnce()...)
			s.Cancel("j1") // 制造资源归还，观察重调度
		}
		return all, s.Ledger().Log()
	}
	sched1, log1 := run()
	sched2, log2 := run()
	if fmt.Sprint(sched1) != fmt.Sprint(sched2) {
		t.Fatalf("调度结论不可复现:\n run1=%v\n run2=%v", sched1, sched2)
	}
	if strings.Join(log1, "\n") != strings.Join(log2, "\n") {
		t.Fatal("分配流水不可复现")
	}
	t.Logf("两次运行调度序列一致: %v", sched1)
}
