// hpcsim 是本地批调度器的可运行演示：
// 构造节点与多队列，提交普通作业、作业组，制造资源不足与节点故障，
// 打印分配流水、排队/拒绝原因与资源守恒校验结论。
package main

import (
	"fmt"

	"github.com/lisama569/hpc-batch-scheduler-go/scheduler"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func show(s *scheduler.Scheduler, tag string) {
	fmt.Printf("\n---------- %s ----------\n", tag)
	fmt.Print(s.Snapshot())
	if err := s.CheckConservation(); err != nil {
		fmt.Printf("!! CONSERVATION BROKEN: %v\n", err)
	} else {
		fmt.Println("conservation: OK (allocated + available == total on every node)")
	}
}

func main() {
	s := scheduler.New()
	must(s.AddNode("node-a", scheduler.Resources{CPU: 8, MemoryMB: 32768, GPU: 2}))
	must(s.AddNode("node-b", scheduler.Resources{CPU: 8, MemoryMB: 32768, GPU: 2}))
	must(s.AddQueue(scheduler.QueueConfig{Name: "batch", Weight: 2, Priority: 5}))
	must(s.AddQueue(scheduler.QueueConfig{Name: "accel", Weight: 1, Priority: 3, MaxJobs: 6}))

	// 1) 普通作业：直接放置
	r1 := s.Submit(scheduler.Job{ID: "j1", Queue: "batch", Request: scheduler.Resources{CPU: 4, MemoryMB: 8192}})
	fmt.Printf("submit j1 => accepted=%v state=%s reason=%s | %s\n", r1.Accepted, r1.State, r1.Reason, r1.Detail)

	// 2) 大作业：单节点放不下 => 拒绝（不可行），与排队可区分
	r2 := s.Submit(scheduler.Job{ID: "j-big", Queue: "batch", Request: scheduler.Resources{CPU: 64, MemoryMB: 1024}})
	fmt.Printf("submit j-big => accepted=%v reason=%s | %s\n", r2.Accepted, r2.Reason, r2.Detail)

	// 3) 暂时资源不足 => 排队
	r3 := s.Submit(scheduler.Job{ID: "j2", Queue: "batch", Request: scheduler.Resources{CPU: 6, MemoryMB: 16384, GPU: 2}})
	fmt.Printf("submit j2 => accepted=%v state=%s reason=%s | %s\n", r3.Accepted, r3.State, r3.Reason, r3.Detail)
	r4 := s.Submit(scheduler.Job{ID: "j3", Queue: "accel", Request: scheduler.Resources{CPU: 2, MemoryMB: 4096, GPU: 2}})
	fmt.Printf("submit j3 => accepted=%v state=%s reason=%s | %s\n", r4.Accepted, r4.State, r4.Reason, r4.Detail)
	show(s, "after submissions: j2 should be queued")

	// 4) 作业组：两成员各需 1 GPU，而 4 张 GPU 已被 j2/j3 占满 => 整组一起排队
	grp := s.SubmitGroup([]scheduler.Job{
		{ID: "g1", Queue: "accel", GroupID: "grp-1", Request: scheduler.Resources{CPU: 2, MemoryMB: 4096, GPU: 1}},
		{ID: "g2", Queue: "accel", GroupID: "grp-1", Request: scheduler.Resources{CPU: 2, MemoryMB: 4096, GPU: 1}},
	})
	for _, r := range grp {
		fmt.Printf("submit-group => %s accepted=%v state=%s reason=%s\n", r.JobID, r.Accepted, r.State, r.Reason)
	}
	show(s, "group grp-1 queued atomically")

	// 5) 完成 j1 归还资源，应唤醒排队作业（公平轮转决定先服务谁）
	must(s.Complete("j1"))
	show(s, "after completing j1: queued jobs rescheduled")

	// 6) 节点故障：驱逐运行作业并把资源释放，被驱逐作业重新排队、可迁移
	must(s.MarkNodeUnhealthy("node-b"))
	show(s, "node-b unhealthy: evicted jobs requeued, resources released")

	// 7) 推进若干周期，观察老化（低优先级不被饿死）
	for i := 0; i < 6; i++ {
		s.Tick()
	}
	show(s, "after aging ticks")

	// 8) 节点恢复，排队作业应再次被安置
	must(s.MarkNodeHealthy("node-b"))
	show(s, "node-b healthy again")

	// 9) 非法请求 => 拒绝原因与排队明确区分
	bad := s.Submit(scheduler.Job{ID: "j-zero", Queue: "batch", Request: scheduler.Resources{}})
	fmt.Printf("\nsubmit j-zero => accepted=%v reason=%s | %s\n", bad.Accepted, bad.Reason, bad.Detail)
	missing := s.Submit(scheduler.Job{ID: "j-x", Queue: "nope", Request: scheduler.Resources{CPU: 1}})
	fmt.Printf("submit j-x => accepted=%v reason=%s | %s\n", missing.Accepted, missing.Reason, missing.Detail)

	fmt.Println("\n================= allocation ledger =================")
	fmt.Print(s.Ledger().FormatLog())
}
