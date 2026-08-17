package api

import (
	"sync"
	"time"
)

// fallReceiptMaxAge bounds how long an unclaimed receipt sticks around.
const fallReceiptMaxAge = 5 * time.Second

// fallReceiptTracker records wall-clock ingest time for fall_warn
// events, keyed identically to the alarm event_id a cluster-starting
// event would produce (alarmEventID).
type fallReceiptTracker struct {
	mu       sync.Mutex
	receipts map[string]time.Time
}

func newFallReceiptTracker() *fallReceiptTracker {
	return &fallReceiptTracker{receipts: make(map[string]time.Time)}
}

// record notes when eventID was ingested, unless it's already been recorded.
func (t *fallReceiptTracker) record(eventID string, at time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.receipts[eventID]; !exists {
		t.receipts[eventID] = at
	}
}

// takeLatency returns how long ago eventID was recorded.
func (t *fallReceiptTracker) takeLatency(eventID string, now time.Time) (time.Duration, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.receipts[eventID]
	if !ok {
		return 0, false
	}
	delete(t.receipts, eventID)
	return now.Sub(at), true
}

// sweep drops any receipt older than maxAge.
func (t *fallReceiptTracker) sweep(maxAge time.Duration, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, at := range t.receipts {
		if now.Sub(at) > maxAge {
			delete(t.receipts, id)
		}
	}
}
