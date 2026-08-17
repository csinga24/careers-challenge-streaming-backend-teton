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
	mu           sync.Mutex
	devices      map[string][]model.Event
	rooms        map[string][]model.Event
	fallsMu      sync.Mutex
	falls        []model.Event
	fallsEvicted int // count of fall_warn events ever dropped from falls' front by the maxFallEvents cap; lets FallWarnEventsFrom detect an offset it can no longer honor.
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
	s.mu.Unlock()

	if e.Type == model.FallWarn {
		s.fallsMu.Lock()
		s.falls = append(s.falls, e)
		if len(s.falls) > maxFallEvents {
			dropped := len(s.falls) - maxFallEvents
			s.fallsEvicted += dropped
			s.falls = s.falls[dropped:]
		}
		s.fallsMu.Unlock()
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
	s.fallsMu.Lock()
	defer s.fallsMu.Unlock()

	out := make([]model.Event, len(s.falls))
	copy(out, s.falls)
	return out
}

// FallWarnEventsFrom returns only the fall_warn events accepted since
// offset, plus the current total. offset is interpreted against the
// store's all-time fall_warn count (fallsEvicted + len(falls)), not the
// live slice index, so it stays correct across an eviction. An offset
// that's already been evicted past (only possible once more than
// maxFallEvents fall_warn events have ever been accepted) falls back to
// returning everything currently buffered.
func (s *MemoryStore) FallWarnEventsFrom(offset int) (events []model.Event, total int) {
	s.fallsMu.Lock()
	defer s.fallsMu.Unlock()

	idx := offset - s.fallsEvicted
	if idx < 0 || idx > len(s.falls) {
		idx = 0
	}
	out := make([]model.Event, len(s.falls)-idx)
	copy(out, s.falls[idx:])
	return out, s.fallsEvicted + len(s.falls)
}
