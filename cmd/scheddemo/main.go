// scheddemo 演示本地批作业调度器：提交、排队、gang 作业、节点故障与回收。
package main

import (
	"fmt"

	"github.com/lisama569/hpc-batch-scheduler-go/scheduler"
)

func main() {
	nodes := []scheduler.Node{
		{ID: "node-a", Capacity: scheduler.Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}},
		{ID: "node-b", Capacity: scheduler.Resource{CPUMilli: 4000, MemMiB: 8192, GPU: 1}},
	}
	queues := []scheduler.Queue{
		{Name: "lo", Weight: 1},
		{Name: "hi", Weight: 2},
	}
	s := scheduler.NewScheduler(nodes, queues)

	submit := func(j scheduler.Job) {
		r := s.Submit(j)
		fmt.Printf("submit %-6s accepted=%-5v reason=%s\n", j.ID, r.Accepted, r.Reason)
	}

	// 1) 普通作业与 gang 作业提交。
	submit(scheduler.Job{ID: "w1", Queue: "lo", Request: scheduler.Resource{CPUMilli: 3000, MemMiB: 2048}})
	submit(scheduler.Job{ID: "g1-0", Queue: "hi", GroupID: "g1", Request: scheduler.Resource{CPUMilli: 2000, MemMiB: 2048}})
	submit(scheduler.Job{ID: "g1-1", Queue: "hi", GroupID: "g1", Request: scheduler.Resource{CPUMilli: 2000, MemMiB: 2048}})
	submit(scheduler.Job{ID: "too-big", Queue: "lo", Request: scheduler.Resource{CPUMilli: 999999}})

	fmt.Println("== 调度一轮 ==")
	fmt.Println("scheduled:", s.ScheduleOnce())
	fmt.Println("running:", s.RunningIDs(), "pending:", s.PendingIDs())

	// 2) 节点故障：受影响作业（gang 则整组）回收并重新排队。
	fmt.Println("== node-a 故障 ==")
	fmt.Println("requeue:", s.NodeDown("node-a"))
	if err := s.Ledger().CheckConservation(); err != nil {
		panic(err)
	}
	fmt.Println("used after fault:", s.Ledger().Used(), "pending:", s.PendingIDs())

	// 3) 节点恢复，资源归还后重调度。
	s.NodeUp("node-a")
	fmt.Println("== 恢复后再调度一轮 ==")
	fmt.Println("scheduled:", s.ScheduleOnce())
	fmt.Println("running:", s.RunningIDs())
	if err := s.Ledger().CheckConservation(); err != nil {
		panic(err)
	}

	fmt.Println("== 判定依据（节选） ==")
	for _, d := range s.Decisions() {
		fmt.Println(" ", d)
	}
	fmt.Println("== 分配流水 ==")
	for _, l := range s.Ledger().Log() {
		fmt.Println(" ", l)
	}
}
