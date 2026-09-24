package scheduler

import "testing"

func TestLedgerConservationAndEvents(t *testing.T) {
	l := NewLedger()
	l.RegisterNode("n1", Resources{CPU: 8, MemoryMB: 1000})
	l.RegisterNode("n2", Resources{CPU: 4, MemoryMB: 500})

	l.Allocate("j1", "n1", "q", Resources{CPU: 3, MemoryMB: 300}, "test alloc")
	l.Allocate("j2", "n1", "q", Resources{CPU: 2, MemoryMB: 200}, "test alloc")
	l.Release("j1", "n1", "q", Resources{CPU: 3, MemoryMB: 300}, "test release")

	// n1 当前已分配 = j2 的 (2,200)，可用应为 (6,800)
	avail := map[string]Resources{
		"n1": {CPU: 6, MemoryMB: 800},
		"n2": {CPU: 4, MemoryMB: 500},
	}
	if err := l.CheckConservation(avail); err != nil {
		t.Fatalf("conservation: %v", err)
	}
	if got := l.Allocated("n1"); got != (Resources{CPU: 2, MemoryMB: 200}) {
		t.Fatalf("n1 allocated=%s", got)
	}
	if got := l.TotalAllocated(); got != (Resources{CPU: 2, MemoryMB: 200}) {
		t.Fatalf("total allocated=%s", got)
	}

	// 账目失配必须被检出
	bad := map[string]Resources{
		"n1": {CPU: 7, MemoryMB: 800}, // 与已分配 2 相加 = 9 != 8
		"n2": {CPU: 4, MemoryMB: 500},
	}
	if err := l.CheckConservation(bad); err == nil {
		t.Fatalf("expected conservation violation")
	}

	// 流水单调编号且事件类型正确
	evs := l.Events()
	if len(evs) != 3 {
		t.Fatalf("events=%d want 3", len(evs))
	}
	wantTypes := []EventType{EvAllocate, EvAllocate, EvRelease}
	for i, e := range evs {
		if e.Seq != i+1 {
			t.Fatalf("event %d seq=%d", i, e.Seq)
		}
		if e.Type != wantTypes[i] {
			t.Fatalf("event %d type=%s want %s", i, e.Type, wantTypes[i])
		}
	}
	// Events() 返回快照，修改切片不影响内部账目
	evs[0].JobID = "tampered"
	if l.Events()[0].JobID != "j1" {
		t.Fatalf("Events() must return a defensive copy")
	}
	t.Logf("\n%s", l.FormatLog())
}

func TestLedgerEvictReleasesAndRejectsRemoveNode(t *testing.T) {
	l := NewLedger()
	l.RegisterNode("n1", Resources{CPU: 4, MemoryMB: 400})
	l.Allocate("j1", "n1", "q", Resources{CPU: 4, MemoryMB: 400}, "")
	l.Evict("j1", "n1", "q", Resources{CPU: 4, MemoryMB: 400}, "node dead")
	if got := l.Allocated("n1"); got.AnyPositive() {
		t.Fatalf("after evict allocated=%s, want zero", got)
	}
	if err := l.RemoveNode("n1"); err != nil {
		t.Fatalf("remove empty node: %v", err)
	}

	l.RegisterNode("n2", Resources{CPU: 4, MemoryMB: 400})
	l.Allocate("j2", "n2", "q", Resources{CPU: 1, MemoryMB: 100}, "")
	if err := l.RemoveNode("n2"); err == nil {
		t.Fatalf("removing node with allocations must fail")
	}
}
