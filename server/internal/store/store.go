package store

import "teton-streaming-backend/internal/model"

// Store is what the API layer needs from an event store.
type Store interface {
	// Append records an accepted event.
	Append(e model.Event)
	// Events returns a device's events, in the order they were appended.
	Events(deviceID string) []model.Event
	// RoomPresenceEvents returns the presence events reported for a room,
	// across whichever devices reported them.
	RoomPresenceEvents(roomID string) []model.Event
	// FallWarnEvents returns every fall_warn event accepted, across all
	// devices.
	FallWarnEvents() []model.Event
}
