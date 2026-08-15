package durablelog

import (
	"context"
	"os"
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

// requires a running Postgres; skips without DATABASE_URL, same as the
// bake-off did. Run against the same local Postgres used for M4a:
//
//	DATABASE_URL=postgres://postgres:bench@localhost:15432/bench?sslmode=disable go test ./internal/durablelog/
func openTestPostgres(t *testing.T) *PostgresLog {
	t.Helper()
	connString := os.Getenv("DATABASE_URL")
	if connString == "" {
		t.Skip("DATABASE_URL not set, skipping Postgres durable log tests")
	}
	log, err := OpenPostgres(context.Background(), connString)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	if _, err := log.pool.Exec(context.Background(), "TRUNCATE events"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return log
}

func confidence(v float64) *float64 { return &v }

func TestPostgresLogAppendAndReplayRoundTrip(t *testing.T) {
	log := openTestPostgres(t)

	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	want := []model.Event{
		{DeviceID: "dev_1", RoomID: "room_14", Type: model.Heartbeat, TS: now, Seq: 1},
		{DeviceID: "dev_1", RoomID: "room_14", Type: model.FallWarn, TS: now.Add(time.Second), Seq: 2, Confidence: confidence(0.9)},
		{DeviceID: "dev_2", RoomID: "room_15", Type: model.Presence, TS: now.Add(2 * time.Second), Seq: 1, InRoom: ptrBool(true)},
	}
	for _, e := range want {
		if err := log.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.Close(); err != nil { // forces the buffered writer to flush
		t.Fatalf("close: %v", err)
	}

	log2, err := OpenPostgres(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()

	var got []model.Event
	if err := log2.Replay(func(e model.Event) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d replayed events, got %d", len(want), len(got))
	}
	for i := range want {
		if got[i].DeviceID != want[i].DeviceID || got[i].Type != want[i].Type || !got[i].TS.Equal(want[i].TS) {
			t.Errorf("event %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
	if got[1].Confidence == nil || *got[1].Confidence != 0.9 {
		t.Errorf("expected confidence 0.9 preserved on fall_warn, got %v", got[1].Confidence)
	}
	if got[2].InRoom == nil || !*got[2].InRoom {
		t.Errorf("expected in_room true preserved on presence, got %v", got[2].InRoom)
	}
}

func ptrBool(v bool) *bool { return &v }

func TestPostgresLogReplayOrderMatchesAppendOrder(t *testing.T) {
	log := openTestPostgres(t)

	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)
	// Deliberately out of ts order — the log preserves append order, not
	// ts order; ordering is the API layer's job (see M2a).
	events := []model.Event{
		{DeviceID: "dev_1", Type: model.Heartbeat, TS: now.Add(5 * time.Second)},
		{DeviceID: "dev_1", Type: model.Heartbeat, TS: now},
		{DeviceID: "dev_1", Type: model.Heartbeat, TS: now.Add(2 * time.Second)},
	}
	for _, e := range events {
		if err := log.Append(e); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := log.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	log2, err := OpenPostgres(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer log2.Close()

	var got []model.Event
	if err := log2.Replay(func(e model.Event) error {
		got = append(got, e)
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 events, got %d", len(got))
	}
	for i := range events {
		if !got[i].TS.Equal(events[i].TS) {
			t.Errorf("event %d: expected append order preserved, got ts %v want %v", i, got[i].TS, events[i].TS)
		}
	}
}
