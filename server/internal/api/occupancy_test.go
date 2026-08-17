package api

import (
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

func inRoom(v bool) *bool { return &v }

func presenceAt(ts time.Time, in bool) model.Event {
	return model.Event{Type: model.Presence, TS: ts, InRoom: inRoom(in)}
}

func TestComputeRoomOccupancyNoEvents(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	got := computeRoomOccupancy(nil, 5*time.Minute, now)
	if got.InRoom || got.OccupiedPct != 0 {
		t.Errorf("expected empty state for no events, got %+v", got)
	}
}

func TestComputeRoomOccupancyCurrentInRoomIsLatestByTS(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	// Deliberately out of arrival order: the true event has the latest ts
	// even though it's not last in the slice.
	events := []model.Event{
		presenceAt(now.Add(-1*time.Minute), true),
		presenceAt(now, false),
		presenceAt(now.Add(-2*time.Minute), true),
	}
	got := computeRoomOccupancy(events, 5*time.Minute, now)
	if got.InRoom {
		t.Errorf("expected current in_room false (latest by ts), got true")
	}
}

func TestComputeRoomOccupancyIgnoresNonPresenceEvents(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		{Type: model.Heartbeat, TS: now},
		{Type: model.Motion, TS: now},
	}
	got := computeRoomOccupancy(events, 5*time.Minute, now)
	if got.InRoom || got.OccupiedPct != 0 {
		t.Errorf("expected empty state ignoring non-presence events, got %+v", got)
	}
}

func TestComputeRoomOccupancyFullyOccupiedWindow(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	window := 5 * time.Minute
	// Became occupied well before the window even started.
	events := []model.Event{presenceAt(now.Add(-time.Hour), true)}

	got := computeRoomOccupancy(events, window, now)
	if !got.InRoom {
		t.Errorf("expected in_room true")
	}
	if got.OccupiedPct != 1 {
		t.Errorf("expected occupied_pct 1, got %v", got.OccupiedPct)
	}
}

func TestComputeRoomOccupancyNeverOccupied(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{presenceAt(now.Add(-time.Hour), false)}

	got := computeRoomOccupancy(events, 5*time.Minute, now)
	if got.InRoom {
		t.Errorf("expected in_room false")
	}
	if got.OccupiedPct != 0 {
		t.Errorf("expected occupied_pct 0, got %v", got.OccupiedPct)
	}
}

func TestComputeRoomOccupancyHalfOccupied(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 5, 0, 0, time.UTC)
	window := 5 * time.Minute
	// Occupied for the first half of the window, then left.
	events := []model.Event{
		presenceAt(now.Add(-5*time.Minute), true),
		presenceAt(now.Add(-2*time.Minute-30*time.Second), false),
	}

	got := computeRoomOccupancy(events, window, now)
	if got.InRoom {
		t.Errorf("expected current in_room false")
	}
	wantPct := 0.5
	if diff := got.OccupiedPct - wantPct; diff > 0.001 || diff < -0.001 {
		t.Errorf("expected occupied_pct ~%v, got %v", wantPct, got.OccupiedPct)
	}
}

func TestComputeRoomOccupancyLateEventFixesUpWindow(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 5, 0, 0, time.UTC)
	window := 5 * time.Minute

	// Without the late event, the room looks unoccupied the whole window.
	before := computeRoomOccupancy([]model.Event{
		presenceAt(now.Add(-5*time.Minute), false),
	}, window, now)
	if before.OccupiedPct != 0 {
		t.Fatalf("sanity check failed: expected 0 before late event, got %v", before.OccupiedPct)
	}

	// A late-arriving event reveals the room was actually occupied for the
	// middle third of the window; replaying it must correct the result.
	afterLateEvent := computeRoomOccupancy([]model.Event{
		presenceAt(now.Add(-5*time.Minute), false),
		presenceAt(now.Add(-4*time.Minute), true),  // arrives late, ts in the middle
		presenceAt(now.Add(-3*time.Minute), false), // arrives late, ts in the middle
	}, window, now)

	wantPct := 1.0 / 5.0
	if diff := afterLateEvent.OccupiedPct - wantPct; diff > 0.001 || diff < -0.001 {
		t.Errorf("expected occupied_pct ~%v after late replay, got %v", wantPct, afterLateEvent.OccupiedPct)
	}
}

func TestParseWindow(t *testing.T) {
	cases := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"1m", time.Minute, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"", 5 * time.Minute, false},
		{"bogus", 0, true},
		{"0m", 0, true},
		{"5x", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseWindow(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseWindow(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("parseWindow(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
