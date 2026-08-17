package api

import (
	"sort"
	"sync"
	"time"

	"teton-streaming-backend/internal/model"
)

const subscriberBufferSize = 64

// alarmFlushInterval bounds how stale the live feed can be.
const alarmFlushInterval = 200 * time.Millisecond

// deliveredAlarm is what actually goes out over the broker: the alarm
// plus the monotonic publish-order sequence number it was assigned.
type deliveredAlarm struct {
	model.Alarm
	Seq int64
}

// alarmBroker fans newly-detected alarms out to subscribed SSE streams,
// deduplicating by event_id so a jittered event that merges into an
// already-broadcast cluster isn't re-sent.
type alarmBroker struct {
	mu          sync.Mutex
	nextSeq     int64
	seen        map[string]bool
	history     []deliveredAlarm // ordered by Seq, oldest first
	subscribers map[chan deliveredAlarm]struct{}
	receipts    *fallReceiptTracker // ingest-time lookups for alarmPublishLatencySeconds; nil-safe
}

func newAlarmBroker(receipts *fallReceiptTracker) *alarmBroker {
	return &alarmBroker{
		seen:        make(map[string]bool),
		subscribers: make(map[chan deliveredAlarm]struct{}),
		receipts:    receipts,
	}
}

// publishNew broadcasts any alarm in alarms not already seen. Call with
// the full, freshly deduplicated alarm list (e.g. from deduplicateFalls).
func (b *alarmBroker) publishNew(alarms []model.Alarm) {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	for _, a := range alarms {
		if b.seen[a.EventID] {
			continue
		}
		b.seen[a.EventID] = true
		b.nextSeq++
		da := deliveredAlarm{Alarm: a, Seq: b.nextSeq}
		b.history = append(b.history, da)
		alarmsEmittedTotal.Inc()
		alarmsEmittedCount.Add(1)
		if b.receipts != nil {
			if latency, ok := b.receipts.takeLatency(a.EventID, now); ok {
				alarmPublishLatencySeconds.Observe(latency.Seconds())
			}
		}
		for ch := range b.subscribers {
			select {
			case ch <- da:
			default:
				// Slow subscriber; drop rather than block ingestion.
			}
		}
	}
}

// historySince returns every published alarm with Seq > cursor, in
// publish order. Used to replay backlog to a reconnecting SSE client.
func (b *alarmBroker) historySince(cursor int64) []deliveredAlarm {
	b.mu.Lock()
	defer b.mu.Unlock()

	// history is append-ordered by increasing Seq, so the first entry
	// past the cursor starts the slice to return.
	i := sort.Search(len(b.history), func(i int) bool { return b.history[i].Seq > cursor })
	out := make([]deliveredAlarm, len(b.history)-i)
	copy(out, b.history[i:])
	return out
}

// subscribe registers a new SSE subscriber and returns its channel plus
// an unsubscribe function that must be called when the stream ends.
func (b *alarmBroker) subscribe() (<-chan deliveredAlarm, func()) {
	ch := make(chan deliveredAlarm, subscriberBufferSize)
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
		s.fallReceipts.sweep(fallReceiptMaxAge, time.Now())
	}
}

func (s *Server) flushAlarmsOnce() {
	s.broker.publishNew(s.deduplication.snapshot(s.store))
}
