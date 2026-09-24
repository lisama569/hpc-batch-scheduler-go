package scheduler

import (
	"fmt"
	"sort"
	"sync"
)

// allocRecord 记录一次资源分配，用于审计与回收。
type allocRecord struct {
	JobID string
	Node  string
	Res   Resource
}

// Ledger 是集群资源账目：所有分配与回收都经过它，保证同一资源不被重复分配或泄漏。
// 所有方法都持有同一把互斥锁，因此并发提交下的分配是串行化、可复现的。
type Ledger struct {
	mu       sync.Mutex
	nodes    map[string]*Node
	used     map[string]Resource // nodeID -> 已用资源
	byJob    map[string][]allocRecord
	allocLog []string // 分配流水（供测试与审计打印）
}

// NewLedger 以给定节点集合创建账目。
func NewLedger(nodes []Node) *Ledger {
	l := &Ledger{
		nodes: make(map[string]*Node, len(nodes)),
		used:  make(map[string]Resource, len(nodes)),
		byJob: make(map[string][]allocRecord),
	}
	for i := range nodes {
		n := nodes[i]
		l.nodes[n.ID] = &n
		l.used[n.ID] = Resource{}
	}
	return l
}

// Total 返回当前健康节点的总容量。
func (l *Ledger) Total() Resource {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.totalLocked()
}

func (l *Ledger) totalLocked() Resource {
	t := Resource{}
	for _, n := range l.nodes {
		if n.Status == NodeReady {
			t = t.Add(n.Capacity)
		}
	}
	return t
}

// CapacityAll 返回全部节点（含不健康）的总容量，用于提交准入判定。
func (l *Ledger) CapacityAll() Resource {
	l.mu.Lock()
	defer l.mu.Unlock()
	t := Resource{}
	for _, n := range l.nodes {
		t = t.Add(n.Capacity)
	}
	return t
}

// Used 返回当前已分配总量。
func (l *Ledger) Used() Resource {
	l.mu.Lock()
	defer l.mu.Unlock()
	u := Resource{}
	for _, r := range l.used {
		u = u.Add(r)
	}
	return u
}

// Free 返回当前可分配总量。
func (l *Ledger) Free() Resource {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.totalLocked().Sub(l.usedLocked())
}

func (l *Ledger) usedLocked() Resource {
	u := Resource{}
	for _, r := range l.used {
		u = u.Add(r)
	}
	return u
}

// Allocate 尝试把 res 分配到某个健康节点；成功返回节点名，失败返回空串。
// 节点选择按节点 ID 字典序做首次适配（first-fit），保证相同状态下结论可复现。
func (l *Ledger) Allocate(jobID string, res Resource) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ids := make([]string, 0, len(l.nodes))
	for id := range l.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		n := l.nodes[id]
		if n.Status != NodeReady {
			continue
		}
		if l.used[id].Add(res).Fits(n.Capacity) {
			l.used[id] = l.used[id].Add(res)
			l.byJob[jobID] = append(l.byJob[jobID], allocRecord{JobID: jobID, Node: id, Res: res})
			l.allocLog = append(l.allocLog,
				fmt.Sprintf("ALLOC job=%s node=%s res=%s node_used=%s", jobID, id, res, l.used[id]))
			return id, true
		}
	}
	l.allocLog = append(l.allocLog, fmt.Sprintf("ALLOC_FAIL job=%s res=%s", jobID, res))
	return "", false
}

// ReleaseJob 回收某作业占用的全部资源，返回回收条数。
func (l *Ledger) ReleaseJob(jobID string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	recs := l.byJob[jobID]
	for _, r := range recs {
		l.used[r.Node] = l.used[r.Node].Sub(r.Res)
		l.allocLog = append(l.allocLog,
			fmt.Sprintf("RELEASE job=%s node=%s res=%s node_used=%s", jobID, r.Node, r.Res, l.used[r.Node]))
	}
	delete(l.byJob, jobID)
	return len(recs)
}

// NodeDown 将节点标记为不健康并回收其上全部资源，返回受影响的作业 ID。
func (l *Ledger) NodeDown(nodeID string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	n, ok := l.nodes[nodeID]
	if !ok || n.Status == NodeDown {
		return nil
	}
	n.Status = NodeDown
	affected := map[string]bool{}
	for jobID, recs := range l.byJob {
		kept := recs[:0]
		for _, r := range recs {
			if r.Node == nodeID {
				l.used[nodeID] = l.used[nodeID].Sub(r.Res)
				l.allocLog = append(l.allocLog,
					fmt.Sprintf("RECLAIM_NODE_DOWN job=%s node=%s res=%s", jobID, nodeID, r.Res))
				affected[jobID] = true
			} else {
				kept = append(kept, r)
			}
		}
		if len(kept) == 0 {
			delete(l.byJob, jobID)
		} else {
			l.byJob[jobID] = kept
		}
	}
	out := make([]string, 0, len(affected))
	for id := range affected {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// NodeUp 恢复节点为健康状态。
func (l *Ledger) NodeUp(nodeID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n, ok := l.nodes[nodeID]; ok {
		n.Status = NodeReady
		l.allocLog = append(l.allocLog, fmt.Sprintf("NODE_UP node=%s", nodeID))
	}
}

// CheckConservation 校验账目守恒：各节点已用不超过容量、无负值，且流水记录与汇总一致。
func (l *Ledger) CheckConservation() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	zero := Resource{}
	sum := map[string]Resource{}
	for _, recs := range l.byJob {
		for _, r := range recs {
			sum[r.Node] = sum[r.Node].Add(r.Res)
		}
	}
	for id, n := range l.nodes {
		u := l.used[id]
		if !zero.Fits(u) {
			return fmt.Errorf("node %s: negative usage %s", id, u)
		}
		if !u.Fits(n.Capacity) {
			return fmt.Errorf("node %s: usage %s exceeds capacity %s", id, u, n.Capacity)
		}
		if sum[id] != u {
			return fmt.Errorf("node %s: record sum %s != usage %s", id, sum[id], u)
		}
	}
	return nil
}

// Log 返回分配流水副本。
func (l *Ledger) Log() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.allocLog))
	copy(out, l.allocLog)
	return out
}
