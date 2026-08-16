package store

import (
	"fmt"
	"sync"
	"testing"

	"teton-streaming-backend/internal/model"
)

func TestMemoryStoreAppendAndEvents(t *testing.T) {
	s := NewMemoryStore()
	s.Append(model.Event{DeviceID: "dev_1", Seq: 1})
	s.Append(model.Event{DeviceID: "dev_1", Seq: 2})
	s.Append(model.Event{DeviceID: "dev_2", Seq: 1})

	got := s.Events("dev_1")
	if len(got) != 2 {
		t.Fatalf("expected 2 events for dev_1, got %d", len(got))
	}
	if len(s.Events("dev_2")) != 1 {
		t.Fatalf("expected 1 event for dev_2")
	}
	if len(s.Events("dev_unknown")) != 0 {
		t.Fatalf("expected 0 events for unknown device")
	}
}

func TestMemoryStoreCapsPerDevice(t *testing.T) {
	s := NewMemoryStore()
	for i := range maxEventsPerDevice + 100 {
		s.Append(model.Event{DeviceID: "dev_1", Seq: int64(i)})
	}
	got := s.Events("dev_1")
	if len(got) != maxEventsPerDevice {
		t.Fatalf("expected capped at %d events, got %d", maxEventsPerDevice, len(got))
	}
	if got[0].Seq != 100 {
		t.Fatalf("expected oldest retained seq 100, got %d", got[0].Seq)
	}
}

func TestMemoryStoreConcurrentAppend(t *testing.T) {
	s := NewMemoryStore()
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Append(model.Event{DeviceID: "dev_concurrent", Seq: int64(i)})
		}(i)
	}
	wg.Wait()

	if len(s.Events("dev_concurrent")) != 100 {
		t.Fatalf("expected 100 events, got %d", len(s.Events("dev_concurrent")))
	}
}

func inRoom(v bool) *bool { return &v }

func TestMemoryStoreRoomPresenceEvents(t *testing.T) {
	s := NewMemoryStore()
	// Two different devices reporting into the same room.
	s.Append(model.Event{DeviceID: "dev_1", RoomID: "room_14", Type: model.Presence, InRoom: inRoom(true)})
	s.Append(model.Event{DeviceID: "dev_2", RoomID: "room_14", Type: model.Presence, InRoom: inRoom(false)})
	// A different room, and a non-presence event, should not show up.
	s.Append(model.Event{DeviceID: "dev_3", RoomID: "room_15", Type: model.Presence, InRoom: inRoom(true)})
	s.Append(model.Event{DeviceID: "dev_1", RoomID: "room_14", Type: model.Heartbeat})

	got := s.RoomPresenceEvents("room_14")
	if len(got) != 2 {
		t.Fatalf("expected 2 presence events for room_14, got %d", len(got))
	}
	if len(s.RoomPresenceEvents("room_15")) != 1 {
		t.Fatalf("expected 1 presence event for room_15")
	}
	if len(s.RoomPresenceEvents("room_unknown")) != 0 {
		t.Fatalf("expected 0 presence events for unknown room")
	}
}

func confidence(v float64) *float64 { return &v }

func TestMemoryStoreFallWarnEvents(t *testing.T) {
	s := NewMemoryStore()
	s.Append(model.Event{DeviceID: "dev_1", Type: model.FallWarn, Confidence: confidence(0.9)})
	s.Append(model.Event{DeviceID: "dev_2", Type: model.FallWarn, Confidence: confidence(0.8)})
	// Non-fall events should not show up in the global fall index.
	s.Append(model.Event{DeviceID: "dev_1", Type: model.Heartbeat})

	got := s.FallWarnEvents()
	if len(got) != 2 {
		t.Fatalf("expected 2 fall_warn events, got %d", len(got))
	}
}

// TestMemoryStoreFallWarnEventsNotCappedAtPerDeviceLimit is a regression
// test: s.falls is global across every device, not per-device.
func TestMemoryStoreFallWarnEventsNotCappedAtPerDeviceLimit(t *testing.T) {
	s := NewMemoryStore()
	n := maxEventsPerDevice + 1000
	for i := range n {
		s.Append(model.Event{
			DeviceID:   fmt.Sprintf("dev_%d", i%50), // spread across 50 devices
			Type:       model.FallWarn,
			Confidence: confidence(0.9),
		})
	}

	got := s.FallWarnEvents()
	if len(got) != n {
		t.Fatalf("expected all %d fall_warn events retained (global cap is maxFallEvents, not maxEventsPerDevice), got %d", n, len(got))
	}
}
