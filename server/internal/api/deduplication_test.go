package api

import (
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

func fallWarnAt(deviceID string, ts time.Time, confidence float64) model.Event {
	c := confidence
	return model.Event{DeviceID: deviceID, RoomID: "room_14", Type: model.FallWarn, TS: ts, Confidence: &c}
}

func TestDeduplicateFallsNoEvents(t *testing.T) {
	if got := deduplicateFalls(nil); len(got) != 0 {
		t.Errorf("expected 0 alarms, got %d", len(got))
	}
}

func TestDeduplicateFallsIgnoresNonFallEvents(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		{Type: model.Heartbeat, TS: now},
		{Type: model.Motion, TS: now},
	}
	if got := deduplicateFalls(events); len(got) != 0 {
		t.Errorf("expected 0 alarms, got %d", len(got))
	}
}

func TestDeduplicateFallsJitterCollapsesToOne(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	// Jittered resends repeat the same confidence, matching how the
	// generator produces them (byte-identical copies, only seq differs).
	events := []model.Event{
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_1", now.Add(1*time.Second), 0.7),
		fallWarnAt("dev_1", now.Add(3*time.Second), 0.7),
	}
	got := deduplicateFalls(events)
	if len(got) != 1 {
		t.Fatalf("expected 1 alarm from jittered duplicates, got %d", len(got))
	}
	if !got[0].TS.Equal(now) {
		t.Errorf("expected alarm ts to be the earliest (original) ts %v, got %v", now, got[0].TS)
	}
	if got[0].Confidence != 0.7 {
		t.Errorf("expected alarm confidence from the earliest event (0.7), got %v", got[0].Confidence)
	}
}

func TestDeduplicateFallsCloseInTimeButDifferentConfidenceStaysSeparate(t *testing.T) {
	// Two genuinely distinct falls from the same device, coincidentally
	// close in time — time proximity alone must not merge them.
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_1", now.Add(2*time.Second), 0.4),
	}
	got := deduplicateFalls(events)
	if len(got) != 2 {
		t.Fatalf("expected 2 distinct alarms despite time proximity, got %d", len(got))
	}
}

func TestDeduplicateFallsOutsideJitterWindowStaysSeparate(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_1", now.Add(10*time.Second), 0.8), // well outside fallJitterWindow
	}
	got := deduplicateFalls(events)
	if len(got) != 2 {
		t.Fatalf("expected 2 distinct alarms, got %d", len(got))
	}
}

func TestDeduplicateFallsChainedClusterBeyondWindow(t *testing.T) {
	// A slow drift of jittered events, each within fallJitterWindow of its
	// neighbor, but the first and last are more than fallJitterWindow
	// apart overall. Should still collapse to one alarm.
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_1", now.Add(4*time.Second), 0.7),
		fallWarnAt("dev_1", now.Add(8*time.Second), 0.7),
	}
	got := deduplicateFalls(events)
	if len(got) != 1 {
		t.Fatalf("expected 1 chained alarm, got %d", len(got))
	}
	if !got[0].TS.Equal(now) {
		t.Errorf("expected alarm ts %v, got %v", now, got[0].TS)
	}
}

func TestDeduplicateFallsDifferentDevicesNeverMerge(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_2", now, 0.9), // same ts, different device
	}
	got := deduplicateFalls(events)
	if len(got) != 2 {
		t.Fatalf("expected 2 alarms (different devices never cluster), got %d", len(got))
	}
}

func TestDeduplicateFallsOutOfOrderInput(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	// Deliberately fed out of ts order, matching the store's
	// append-at-write (arrival order, not ts order) contract.
	events := []model.Event{
		fallWarnAt("dev_1", now.Add(2*time.Second), 0.7),
		fallWarnAt("dev_1", now, 0.7),
		fallWarnAt("dev_1", now.Add(1*time.Second), 0.7),
	}
	got := deduplicateFalls(events)
	if len(got) != 1 {
		t.Fatalf("expected 1 alarm, got %d", len(got))
	}
	if !got[0].TS.Equal(now) {
		t.Errorf("expected earliest ts %v regardless of input order, got %v", now, got[0].TS)
	}
}

func TestDeduplicateFallsStableEventID(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{fallWarnAt("dev_1", now, 0.7), fallWarnAt("dev_1", now.Add(time.Second), 0.7)}

	first := deduplicateFalls(events)
	second := deduplicateFalls(events)
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected 1 alarm each call")
	}
	if first[0].EventID != second[0].EventID {
		t.Errorf("expected stable event_id across calls, got %q then %q", first[0].EventID, second[0].EventID)
	}
	if first[0].EventID == "" {
		t.Errorf("expected non-empty event_id")
	}
}

func TestDeduplicateFallsSortedByTS(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	events := []model.Event{
		fallWarnAt("dev_2", now.Add(20*time.Second), 0.9),
		fallWarnAt("dev_1", now, 0.7),
	}
	got := deduplicateFalls(events)
	if len(got) != 2 {
		t.Fatalf("expected 2 alarms, got %d", len(got))
	}
	if got[0].DeviceID != "dev_1" || got[1].DeviceID != "dev_2" {
		t.Errorf("expected alarms sorted by ts, got %+v", got)
	}
}

func TestParseSince(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    time.Time
		wantErr bool
	}{
		{"empty means beginning of time", "", time.Time{}, false},
		{"epoch zero", "0", time.Unix(0, 0).UTC(), false},
		{"epoch with fraction", "1755289200.5", time.Unix(1755289200, 500000000).UTC(), false},
		{"iso8601 not supported", "2026-08-15T18:53:49.123Z", time.Time{}, true},
		{"garbage", "not-a-time", time.Time{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseSince(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("parseSince(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			}
			if !tc.wantErr && !got.Equal(tc.want) {
				t.Errorf("parseSince(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
