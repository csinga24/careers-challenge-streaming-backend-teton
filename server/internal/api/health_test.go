package api

import (
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

func heartbeatAt(ts time.Time) model.Event {
	return model.Event{Type: model.Heartbeat, TS: ts}
}

func TestComputeDeviceHealthNoEvents(t *testing.T) {
	got := computeDeviceHealth(nil, time.Now())
	if got.LastHeartbeatTS != nil {
		t.Errorf("expected nil LastHeartbeatTS, got %v", got.LastHeartbeatTS)
	}
	if got.Availability5m != 0 {
		t.Errorf("expected 0 availability, got %v", got.Availability5m)
	}
}

func TestComputeDeviceHealthIgnoresNonHeartbeatEvents(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		{Type: model.Presence, TS: now},
		{Type: model.Motion, TS: now},
	}
	got := computeDeviceHealth(events, now)
	if got.LastHeartbeatTS != nil {
		t.Errorf("expected nil LastHeartbeatTS, got %v", got.LastHeartbeatTS)
	}
}

func TestComputeDeviceHealthLatestByTSNotOrder(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	// Deliberately out of ts order, matching the append-at-write store
	// contract: events arrive in whatever order they were inserted, not
	// sorted by ts.
	events := []model.Event{
		heartbeatAt(now.Add(-1 * time.Minute)),
		heartbeatAt(now.Add(-3 * time.Minute)),
		heartbeatAt(now.Add(-2 * time.Minute)), // latest by ts, but not last in slice
	}
	got := computeDeviceHealth(events, now)
	if got.LastHeartbeatTS == nil || !got.LastHeartbeatTS.Equal(now.Add(-1*time.Minute)) {
		t.Errorf("expected latest heartbeat ts %v, got %v", now.Add(-1*time.Minute), got.LastHeartbeatTS)
	}
}

func TestComputeDeviceHealthAvailabilityWindow(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	var events []model.Event
	for i := range 60 { // 60 heartbeats within the last 5m, at 1Hz
		events = append(events, heartbeatAt(now.Add(-time.Duration(i)*time.Second)))
	}
	for i := range 10 { // 10 more, outside the 5m window
		events = append(events, heartbeatAt(now.Add(-10*time.Minute-time.Duration(i)*time.Second)))
	}

	got := computeDeviceHealth(events, now)
	wantAvailability := 60.0 / 300.0
	if got.Availability5m != wantAvailability {
		t.Errorf("expected availability %v, got %v", wantAvailability, got.Availability5m)
	}
}

func TestComputeDeviceHealthAvailabilityCapsAtOne(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	var events []model.Event
	for i := range 400 { // more than the 300 expected in 5m
		events = append(events, heartbeatAt(now.Add(-time.Duration(i)*time.Second)))
	}

	got := computeDeviceHealth(events, now)
	if got.Availability5m != 1 {
		t.Errorf("expected availability capped at 1, got %v", got.Availability5m)
	}
}

func TestComputeDeviceHealthExcludesFutureEvents(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		heartbeatAt(now.Add(30 * time.Minute)), // within the 1h clock-skew tolerance, but after "now"
	}
	got := computeDeviceHealth(events, now)
	if got.Availability5m != 0 {
		t.Errorf("expected future event excluded from availability window, got %v", got.Availability5m)
	}
}
