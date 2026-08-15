// Package durablelog defines the write-ahead log.
// It sits behind MemoryStore: writes go to MemoryStore synchronously and to the
// log in the background. Replay rebuilds MemoryStore from it.
package durablelog

import "teton-streaming-backend/internal/model"

// Log is a durable, append-only event log.
type Log interface {
	// Append durably records event.
	Append(e model.Event) error
	// Replay calls for rebuilding in-memory state at boot.
	Replay(fn func(model.Event) error) error
	// Close releases any underlying connection.
	Close() error
}
