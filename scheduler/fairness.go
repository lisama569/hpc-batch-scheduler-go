package scheduler

import "sort"

// fairness 维护多队列轮转与作业老化，保证：
//   - 每个调度周期按固定次序在队列间轮转，高权重队列每轮获得更多预算；
//   - 排队等待的作业随时间获得老化加成，低优先级/长作业不会被持续饿死；
//   - 相同初始状态与提交序列下，遍历次序完全确定（不依赖 map 迭代）。

// QueueRuntime 是队列的运行期公平状态。
type QueueRuntime struct {
	cfg     QueueConfig
	pending []string // 排队中的作业 ID（插入即按提交序，排序时再生成有序视图）
	running int      // 运行中作业数
	// virtual 是加权轮转的虚位记账值：每次被服务后前进
	// stride = totalWeight/weight，值最小的队列下一个被服务。
	virtual int
	// served 记录历史被服务次数，仅用于可解释性展示。
	served int
}

// agingPolicy 控制老化速度。
type agingPolicy struct {
	// boostInterval 每等待多少个调度周期，作业优先级 +1。
	boostInterval int
	// maxBoost 老化加成上限，避免低优先级永久霸榜。
	maxBoost int
}

func defaultAgingPolicy() agingPolicy {
	return agingPolicy{boostInterval: 2, maxBoost: 100}
}

// effectivePriority 返回作业静态优先级叠加老化后的有效优先级。
// 老化加成直接缓存在 job.ageBoost（由 tickAging 统一推进），保证同一次
// 调度周期内所有队列看到一致的判定。
func effectivePriority(j *Job, p agingPolicy) int {
	return j.Priority + j.ageBoost
}

// tickAging 在每个调度周期开始时推进所有排队作业的老化加成。
// 返回加成发生变化的作业数（仅用于测试可观测）。
func tickAging(queues map[string]*QueueRuntime, jobs map[string]*Job, tickCount int, p agingPolicy) int {
	changed := 0
	for _, q := range queues {
		for _, id := range q.pending {
			j := jobs[id]
			if j == nil || j.State != StateQueued {
				continue
			}
			waited := tickCount - j.submitTick
			boost := 0
			if p.boostInterval > 0 {
				boost = waited / p.boostInterval
				if boost > p.maxBoost {
					boost = p.maxBoost
				}
			}
			if boost != j.ageBoost {
				j.ageBoost = boost
				changed++
			}
		}
	}
	return changed
}

// orderPending 返回队列内待调度作业的确定性次序：
// 有效优先级降序，其次提交序号升序（先到先得，保证可复现）。
func orderPending(ids []string, jobs map[string]*Job, p agingPolicy) []string {
	out := append([]string(nil), ids...)
	sort.SliceStable(out, func(a, b int) bool {
		ja, jb := jobs[out[a]], jobs[out[b]]
		pa, pb := effectivePriority(ja, p), effectivePriority(jb, p)
		if pa != pb {
			return pa > pb
		}
		return ja.SubmitSeq < jb.SubmitSeq
	})
	return out
}

// queueOrder 返回全部队列的确定性次序：按队列名升序。
// 加权公平由 QueueRuntime.virtual 在 pickNextQueue 中体现。
func queueOrder(queues map[string]*QueueRuntime) []*QueueRuntime {
	out := make([]*QueueRuntime, 0, len(queues))
	for _, q := range queues {
		out = append(out, q)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].cfg.Name < out[j].cfg.Name })
	return out
}

// pickNextQueue 选择下一个应服务的队列：virtual 最小者优先，
// 平局按队列名升序，保证相同状态下选择可复现。
// 仅在有待处理作业的队列之间选择；全部为空时返回 nil。
func pickNextQueue(queues map[string]*QueueRuntime) *QueueRuntime {
	var best *QueueRuntime
	for _, q := range queueOrder(queues) {
		if len(q.pending) == 0 {
			continue
		}
		if best == nil || q.virtual < best.virtual ||
			(q.virtual == best.virtual && q.cfg.Name < best.cfg.Name) {
			best = q
		}
	}
	return best
}

// serveBudget 返回某队列在一次服务中可尝试调度的作业数：
// 至少 1，且与权重成线性关系（权重翻倍则每轮机会翻倍）。
func serveBudget(q *QueueRuntime) int {
	w := q.cfg.Weight
	if w < 1 {
		w = 1
	}
	return w
}

// markServed 在队列被服务后推进其虚位。
// stride = totalWeight / weight（向下取整，至少 1）：权重越大跨步越小，
// 因此被再次选中的频率越高。
func markServed(q *QueueRuntime, totalWeight, served int) {
	w := q.cfg.Weight
	if w < 1 {
		w = 1
	}
	stride := totalWeight / w
	if stride < 1 {
		stride = 1
	}
	q.virtual += stride * served
	q.served += served
}
