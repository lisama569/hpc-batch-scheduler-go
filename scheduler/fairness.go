package scheduler

import "sort"

// starvationRounds 表示作业被连续跳过多轮后，优先级开始按轮次加成，
// 使长作业 / 低优先级作业在有限轮次内获得调度，避免持续饿死。
const starvationRounds = 3

// queueView 是公平排序所需的队列视图：share 为该队列当前占有的
// 归一化主导资源份额（见 dominantShare），weight 为权重。
type queueView struct {
	name   string
	weight float64
	share  float64
}

// dominantShare 计算队列在三个维度上的占用比例，取最大值（DRF 思想）。
func dominantShare(used, total Resource) float64 {
	s := 0.0
	if total.CPUMilli > 0 {
		if v := float64(used.CPUMilli) / float64(total.CPUMilli); v > s {
			s = v
		}
	}
	if total.MemMiB > 0 {
		if v := float64(used.MemMiB) / float64(total.MemMiB); v > s {
			s = v
		}
	}
	if total.GPU > 0 {
		if v := float64(used.GPU) / float64(total.GPU); v > s {
			s = v
		}
	}
	return s
}

// orderQueues 按加权公平份额排序：份额/权重最小的队列优先；
// 份额相同时权重高者优先（最大-最小公平：把下一份资源给权重高者，
// 可使最大加权份额更小）；再相同按队列名排序，保证结论可复现。
func orderQueues(views []queueView) []string {
	sort.SliceStable(views, func(i, j int) bool {
		fi := views[i].share / weightOf(views[i].weight)
		fj := views[j].share / weightOf(views[j].weight)
		if fi != fj {
			return fi < fj
		}
		if views[i].weight != views[j].weight {
			return views[i].weight > views[j].weight
		}
		return views[i].name < views[j].name
	})
	out := make([]string, len(views))
	for i, v := range views {
		out[i] = v.name
	}
	return out
}

func weightOf(w float64) float64 {
	if w <= 0 {
		return 1
	}
	return w
}

// ageBoost 返回等待轮次带来的优先级加成；超过 starvationRounds 后每轮 +1。
func ageBoost(age int64) int64 {
	if age <= starvationRounds {
		return 0
	}
	return age - starvationRounds
}

// sortPending 对同一队列内的待调度作业排序：
// 防饿死加成后的优先级降序，其次提交顺序升序（FIFO，可复现）。
func sortPending(jobs []*Job) {
	sort.SliceStable(jobs, func(i, j int) bool {
		pi := jobs[i].Priority + int(ageBoost(jobs[i].Age))
		pj := jobs[j].Priority + int(ageBoost(jobs[j].Age))
		if pi != pj {
			return pi > pj
		}
		return jobs[i].SubmitSeq < jobs[j].SubmitSeq
	})
}
