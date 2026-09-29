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

	// 10) 计划维护下线能力在一个独立调度器上演示，避免与前面的存量容量相互影响。
	runMaintenanceDemo()

	fmt.Println("\n================= allocation ledger =================")
	fmt.Print(s.Ledger().FormatLog())
}

// runMaintenanceDemo 在一个全新、容量自洽的调度器上演示计划维护全链路：
// 整组中断重排、维护节点不收新作业、迁移失败时原地等待、排空判定、维护取消。
func runMaintenanceDemo() {
	// 独立调度器：4 个 8CPU/2GPU 节点 z0..z3，两条队列，容量布局逐拍可控。
	m := scheduler.New()
	for _, n := range []string{"z0", "z1", "z2", "z3"} {
		must(m.AddNode(n, scheduler.Resources{CPU: 8, MemoryMB: 16000, GPU: 2}))
	}
	must(m.AddQueue(scheduler.QueueConfig{Name: "ops", Weight: 1}))
	must(m.AddQueue(scheduler.QueueConfig{Name: "holdq", Weight: 1}))

	// 准备：用 4 个占座作业各占一个节点的 GPU，再撤掉 z2 的占座、提交 fillz2，
	// 使 fillz2 唯一可落 z2（其它节点 GPU 仍被占座占住），随后撤掉其余占座。
	seats := []struct{ id, node string }{
		{"seat0", "z0"}, {"seat1", "z1"}, {"seat2", "z2"}, {"seat3", "z3"},
	}
	for _, seat := range seats {
		m.Submit(scheduler.Job{ID: seat.id, Queue: "holdq", Request: scheduler.Resources{CPU: 1, MemoryMB: 100, GPU: 2}})
		if j, _ := m.GetJob(seat.id); j.NodeName != seat.node {
			panic(seat.id + " want " + seat.node + " got " + j.NodeName)
		}
	}
	must(m.Complete("seat2")) // 只腾出 z2
	fill := m.Submit(scheduler.Job{ID: "fillz2", Queue: "holdq", Request: scheduler.Resources{CPU: 3, MemoryMB: 8000, GPU: 2}})
	if j, _ := m.GetJob("fillz2"); j.NodeName != "z2" {
		panic("fillz2 want z2 got " + j.NodeName)
	}
	// seat3 保留到整组重排前，确保 z3 的 GPU 不可用；只撤 z0/z1 占座。
	must(m.Complete("seat0"))
	must(m.Complete("seat1"))
	_ = fill

	// 子场景 A：作业组 (5CPU,2GPU)x2 分散在 z0、z1（z2 被 fillz2 占住 GPU）。
	for _, r := range m.SubmitGroup([]scheduler.Job{
		{ID: "mg1", Queue: "ops", GroupID: "maint-grp", Request: scheduler.Resources{CPU: 5, MemoryMB: 1000, GPU: 2}},
		{ID: "mg2", Queue: "ops", GroupID: "maint-grp", Request: scheduler.Resources{CPU: 5, MemoryMB: 1000, GPU: 2}},
	}) {
		if r.State != scheduler.StateRunning {
			panic("maint-grp " + r.State.String())
		}
		fmt.Printf("submit-group maint => %s state=%s\n", r.JobID, r.State)
	}

	// z0 进入计划维护：mg1=MIGRATE、健康节点 z1 上的 mg2=GROUP_REQUEUE，整组以
	// QUEUED_GROUP_INTERRUPTED 排队。此刻 z0 维护、z2 GPU 被占、z3 空闲但 z1
	// 无 2GPU => 缺第二个 2-GPU 节点，整组保持排队（不允许只起一个）。
	d, err := m.MarkNodeMaintenance("z0")
	must(err)
	fmt.Printf("\nmaintenance z0 => migrated=%v requeued=%v waiting=%v drained=%v\n",
		d.Migrated, d.Requeued, d.Waiting, d.Drained)
	show(m, "z0 planned maintenance: maint-grp wholly requeued (mg1+mg2 both stopped)")
	for _, id := range []string{"mg1", "mg2"} {
		if jj, _ := m.GetJob(id); jj.State != scheduler.StateQueued {
			panic(id + " must be wholly queued right after interruption, got " + jj.State.String())
		}
	}

	// 维护期间正常提交：7CPU 无 GPU 小作业放独立 holdq，避免被 ops 队头阻塞。
	// z3 被 seat3 占 1CPU 后恰余 7CPU（best-fit 最紧凑），z2 余 5CPU 放不下
	// => 落健康节点 z3，绝不落到维护节点 z0。
	nw := m.Submit(scheduler.Job{ID: "maint-new", Queue: "holdq", Request: scheduler.Resources{CPU: 7, MemoryMB: 100}})
	if nw.State != scheduler.StateRunning {
		panic("maint-new should run during maintenance: " + nw.State.String())
	}
	if jn, _ := m.GetJob("maint-new"); jn.NodeName == "z0" {
		panic("maint-new must never land on maintenance z0")
	}
	fmt.Printf("submit maint-new during maintenance => state=%s@%s (healthy node; z0 rejected it)\n",
		nw.State, func() string { j, _ := m.GetJob("maint-new"); return j.NodeName }())
	show(m, "during maintenance: new submit served on a healthy node; maint-grp still wholly queued")

	// fillz2 自然结束腾出 z2，再撤掉 maint-new 与 seat3 腾出 z3：但整组只需
	// z1、z2 两个 2-GPU 节点，fillz2 一结束即可在同一决策里原子重排
	// （绝不回维护中的 z0）。先只完成 fillz2 验证整组重排。
	must(m.Complete("fillz2"))
	show(m, "fillz2 completed: maint-grp re-placed atomically on two healthy nodes")
	for _, id := range []string{"mg1", "mg2"} {
		jj, _ := m.GetJob(id)
		if jj.State != scheduler.StateRunning {
			panic(id + " must re-run after capacity returns, got " + jj.State.String())
		}
		if jj.NodeName == "z0" {
			panic(id + " must never re-place on maintenance z0")
		}
	}

	// 子场景 B：迁移失败 -> 原地等待。先撤掉 maint-new 与 z3 占座，使 z3 成为
	// 唯一空闲整节点；整节点作业 maint-hold 落 z3 后立即维护 z3。此时其它健康
	// 节点都有在跑作业、放不下整节点（z0 维护）=> maint-hold 迁不走，原地等待。
	must(m.Complete("maint-new"))
	must(m.Complete("seat3"))
	hold := m.Submit(scheduler.Job{ID: "maint-hold", Queue: "holdq", Request: scheduler.Resources{CPU: 8, MemoryMB: 16000, GPU: 2}})
	if hold.State != scheduler.StateRunning {
		panic("maint-hold " + hold.State.String())
	}
	jh, _ := m.GetJob("maint-hold")
	if jh.NodeName != "z3" {
		panic("maint-hold want z3 got " + jh.NodeName)
	}
	fmt.Printf("submit maint-hold => state=RUNNING@%s\n", jh.NodeName)
	_, err = m.MarkNodeMaintenance("z3")
	must(err)
	st, err := m.DrainStatus("z3")
	must(err)
	fmt.Printf("drain status z3 => state=%s running=%d drained=%v reason=%s | %s\n",
		st.State, st.Running, st.Drained, st.Reason, st.Detail)
	d2, err := m.DrainNode("z3")
	must(err)
	fmt.Printf("drain again z3 => migrated=%v waiting=%v drained=%v\n", d2.Migrated, d2.Waiting, d2.Drained)
	if d2.Drained || len(d2.Waiting) != 1 {
		panic("maint-hold must wait on z3 (no second whole-node target)")
	}

	// maint-hold 自然结束 => z3 清空，可安全重启。
	must(m.Complete("maint-hold"))
	st, err = m.DrainStatus("z3")
	must(err)
	if !st.Drained {
		panic("z3 must be drained after maint-hold completes")
	}
	fmt.Printf("drain status z3 after completion => drained=%v running=%d | %s\n",
		st.Drained, st.Running, st.Detail)

	// 取消维护：z0、z3 回到正常调度。
	must(m.CancelNodeMaintenance("z0"))
	must(m.CancelNodeMaintenance("z3"))
	show(m, "maintenance cancelled on z0 and z3: both back in service")
	if err := m.CheckConservation(); err != nil {
		panic(err)
	}
	fmt.Println("conservation: OK throughout maintenance lifecycle")
	fmt.Println("\n------------- maintenance demo ledger -------------")
	fmt.Print(m.Ledger().FormatLog())
}
