package api

import (
	"sync"
	"time"

	"teton-streaming-backend/internal/model"
)

const subscriberBufferSize = 64

// alarmFlushInterval bounds how stale the live feed can be.
const alarmFlushInterval = 200 * time.Millisecond

// alarmBroker fans newly-detected alarms out to subscribed SSE streams,
// deduplicating by event_id so a jittered event that merges into an
// already-broadcast cluster isn't re-sent.
type alarmBroker struct {
	mu          sync.Mutex
	seen        map[string]bool
	subscribers map[chan model.Alarm]struct{}
}

func newAlarmBroker() *alarmBroker {
	return &alarmBroker{
		seen:        make(map[string]bool),
		subscribers: make(map[chan model.Alarm]struct{}),
	}
}

// publishNew broadcasts any alarm in alarms not already seen. Call with
// the full, freshly deduplicated alarm list (e.g. from deduplicateFalls).
func (b *alarmBroker) publishNew(alarms []model.Alarm) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, a := range alarms {
		if b.seen[a.EventID] {
			continue
		}
		b.seen[a.EventID] = true
		for ch := range b.subscribers {
			select {
			case ch <- a:
			default:
				// Slow subscriber; drop rather than block ingestion.
			}
		}
	}
}

// subscribe registers a new SSE subscriber and returns its channel plus
// an unsubscribe function that must be called when the stream ends.
func (b *alarmBroker) subscribe() (<-chan model.Alarm, func()) {
	ch := make(chan model.Alarm, subscriberBufferSize)
	b.mu.Lock()
	b.subscribers[ch] = struct{}{}
	b.mu.Unlock()

	unsubscribe := func() {
		b.mu.Lock()
		delete(b.subscribers, ch)
		b.mu.Unlock()
	}
	return ch, unsubscribe
}

// flushAlarmsLoop republishes on a fixed tick. Publishing only from this
// one goroutine (not per-request) is what keeps delivery in ts order per
// room despite concurrent requests finishing out of order.
func (s *Server) flushAlarmsLoop() {
	ticker := time.NewTicker(alarmFlushInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.flushAlarmsOnce()
	}
}

func (s *Server) flushAlarmsOnce() {
	s.broker.publishNew(deduplicateFalls(s.store.FallWarnEvents()))
}
