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
	// FallWarnEventsFrom returns only the fall_warn events accepted since
	// offset, plus the current total (pass that back as offset on the
	// next call), so a repeat caller doesn't have to re-copy the whole,
	// ever-growing fall_warn history just to see what's new since last
	// time. offset 0 (or any offset the implementation can no longer
	// honor, e.g. after evicting old events) returns everything
	// currently available.
	FallWarnEventsFrom(offset int) (events []model.Event, total int)
}
