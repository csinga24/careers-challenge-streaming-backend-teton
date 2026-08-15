package store

import (
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
