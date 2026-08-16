// Package intake bounds concurrent event processing so a burst delays
// producers rather than dropping events, and keeps a fall_warn flood-out
// from starving on heartbeat/motion traffic.
package intake

import "log/slog"

const (
	DefaultHighCapacity   = 200  // fall_warn: rare, generous headroom
	DefaultNormalCapacity = 2000 // everything else
)

// Limiter is a two-lane, bounded concurrency gate. Acquire blocks once
// its lane is full — that block is the backpressure: the caller (an HTTP
// handler) gets delayed, not dropped.
type Limiter struct {
	high   chan struct{}
	normal chan struct{}
}

func NewLimiter(highCap, normalCap int) *Limiter {
	return &Limiter{
		high:   make(chan struct{}, highCap),
		normal: make(chan struct{}, normalCap),
	}
}

// Acquire reserves a slot in the high-priority lane if isHighPriority,
// otherwise the normal lane, and returns a function to release it.
func (l *Limiter) Acquire(isHighPriority bool) (release func()) {
	ch := l.normal
	lane := "normal"
	if isHighPriority {
		ch = l.high
		lane = "high"
	}

	select {
	case ch <- struct{}{}:
	default:
		slog.Warn("intake lane saturated, blocking", "lane", lane, "occupied", len(ch), "capacity", cap(ch))
		ch <- struct{}{}
	}
	return func() { <-ch }
}

// Depth reports current lane occupancy, for observability.
func (l *Limiter) Depth() (high, normal int) {
	return len(l.high), len(l.normal)
}
