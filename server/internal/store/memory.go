// Package store holds accepted events.
package store

import (
	"sync"

	"teton-streaming-backend/internal/model"
)

const maxEventsPerDevice = 10000

// MemoryStore keeps recent events per device, plus a room index for
// presence events that room occupancy needs merged across devices.
type MemoryStore struct {
	mu      sync.Mutex
	devices map[string][]model.Event
	rooms   map[string][]model.Event
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

	if e.Type == model.Presence {
		roomEvents := append(s.rooms[e.RoomID], e)
		if len(roomEvents) > maxEventsPerDevice {
			roomEvents = roomEvents[len(roomEvents)-maxEventsPerDevice:]
		}
		s.rooms[e.RoomID] = roomEvents
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
