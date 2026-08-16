// Package store holds accepted events.
package store

import (
	"sync"

	"teton-streaming-backend/internal/model"
)

// maxEventsPerDevice bounds the per-device and per-room event buffers.
const maxEventsPerDevice = 10000

// maxFallEvents is global across every device
const maxFallEvents = 1000000

// MemoryStore keeps recent events per device, plus room/global indexes
// for presence and fall_warn events that occupancy and alarms need
// merged across devices.
type MemoryStore struct {
	mu      sync.Mutex
	devices map[string][]model.Event
	rooms   map[string][]model.Event
	falls   []model.Event
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		devices: make(map[string][]model.Event),
		rooms:   make(map[string][]model.Event),
	}
}

// Append stores an accepted event.
func (s *MemoryStore) Append(e model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := append(s.devices[e.DeviceID], e)
	if len(events) > maxEventsPerDevice {
		events = events[len(events)-maxEventsPerDevice:]
	}
	s.devices[e.DeviceID] = events

	switch e.Type {
	case model.Presence:
		roomEvents := append(s.rooms[e.RoomID], e)
		if len(roomEvents) > maxEventsPerDevice {
			roomEvents = roomEvents[len(roomEvents)-maxEventsPerDevice:]
		}
		s.rooms[e.RoomID] = roomEvents
	case model.FallWarn:
		s.falls = append(s.falls, e)
		if len(s.falls) > maxFallEvents {
			s.falls = s.falls[len(s.falls)-maxFallEvents:]
		}
	}
}

// Events returns a copy of the stored events for a device.
func (s *MemoryStore) Events(deviceID string) []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.devices[deviceID]
	out := make([]model.Event, len(events))
	copy(out, events)
	return out
}

// RoomPresenceEvents returns a copy of the presence events reported for a
// room, across whichever devices reported them.
func (s *MemoryStore) RoomPresenceEvents(roomID string) []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	events := s.rooms[roomID]
	out := make([]model.Event, len(events))
	copy(out, events)
	return out
}

// FallWarnEvents returns a copy of every fall_warn event accepted, across
// all devices.
func (s *MemoryStore) FallWarnEvents() []model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]model.Event, len(s.falls))
	copy(out, s.falls)
	return out
}
