package api

import (
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"teton-streaming-backend/internal/model"
)

func TestFallReceiptTrackerRecordAndTake(t *testing.T) {
	tr := newFallReceiptTracker()
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	tr.record("dev_1-123", now)

	got, ok := tr.takeLatency("dev_1-123", now.Add(150*time.Millisecond))
	if !ok {
		t.Fatal("expected a recorded receipt to be found")
	}
	if got != 150*time.Millisecond {
		t.Errorf("expected latency 150ms, got %v", got)
	}

	// Claimed once; a second take must miss.
	if _, ok := tr.takeLatency("dev_1-123", now); ok {
		t.Error("expected takeLatency to remove the entry after the first claim")
	}
}

func TestFallReceiptTrackerTakeMissingIsFalse(t *testing.T) {
	tr := newFallReceiptTracker()
	if _, ok := tr.takeLatency("never-recorded", time.Now()); ok {
		t.Error("expected takeLatency on an unrecorded id to report not-found")
	}
}

func TestFallReceiptTrackerRecordIsFirstWriteWins(t *testing.T) {
	tr := newFallReceiptTracker()
	base := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	tr.record("dev_1-1", base)
	tr.record("dev_1-1", base.Add(time.Second)) // a later duplicate resend must not reset the clock

	got, ok := tr.takeLatency("dev_1-1", base.Add(2*time.Second))
	if !ok {
		t.Fatal("expected the id to be found")
	}
	if got != 2*time.Second {
		t.Errorf("expected latency measured from the first record (2s), got %v", got)
	}
}

func TestFallReceiptTrackerSweepDropsOnlyStaleEntries(t *testing.T) {
	tr := newFallReceiptTracker()
	base := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	tr.record("old", base)
	tr.record("fresh", base.Add(4*time.Second))

	tr.sweep(5*time.Second, base.Add(6*time.Second))

	if _, ok := tr.takeLatency("old", base); ok {
		t.Error("expected the stale entry to have been swept")
	}
	if _, ok := tr.takeLatency("fresh", base); !ok {
		t.Error("expected the fresh entry to survive the sweep")
	}
}

// histogramSampleCount reads a Histogram's current observation count
// directly off its protobuf representation — testutil.ToFloat64 only
// supports single-value metrics (Counter/Gauge), not Histogram.
func histogramSampleCount(t *testing.T, h prometheusHistogram) uint64 {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("write histogram metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount()
}

// TestAlarmBrokerObservesPublishLatency confirms publishNew looks up and
// observes a fall_warn event's real ingest-to-publish latency, keyed the
// same way handleEventIntake records it (alarmEventID) — and that it's
// only observed once, on the first publish of a given alarm.
func TestAlarmBrokerObservesPublishLatency(t *testing.T) {
	receipts := newFallReceiptTracker()
	b := newAlarmBroker(receipts)

	deviceID := "dev_stream_latency"
	ts := time.Now().UTC()
	eventID := alarmEventID(deviceID, ts)
	receipts.record(eventID, time.Now().Add(-42*time.Millisecond))

	before := histogramSampleCount(t, alarmPublishLatencySeconds)
	b.publishNew([]model.Alarm{{EventID: eventID, DeviceID: deviceID, TS: ts}})
	after := histogramSampleCount(t, alarmPublishLatencySeconds)

	if after != before+1 {
		t.Errorf("expected exactly one new latency observation, got %d (before %d)", after, before)
	}

	// Re-publishing the same already-seen alarm must not observe again
	// (no receipt left to find, and publishNew's own seen-map skips it).
	b.publishNew([]model.Alarm{{EventID: eventID, DeviceID: deviceID, TS: ts}})
	if got := histogramSampleCount(t, alarmPublishLatencySeconds); got != after {
		t.Errorf("expected no additional observation on redelivery, got %d (want %d)", got, after)
	}
}

// prometheusHistogram is the minimal interface histogramSampleCount needs
// — satisfied by prometheus.Histogram, without importing the whole
// package just for the type name here.
type prometheusHistogram interface {
	Write(*dto.Metric) error
}
