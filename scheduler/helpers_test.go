package scheduler

import (
	"sync/atomic"
	"time"
)

// fakeClock 返回单调推进的固定时间戳，消除 wall-clock 抖动，保证流水可复现。
func fakeClock() func() time.Time {
	var n atomic.Int64
	base := time.Date(2026, 9, 24, 9, 0, 0, 0, time.UTC)
	return func() time.Time {
		i := n.Add(1)
		return base.Add(time.Duration(i) * time.Millisecond)
	}
}
