package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

// handleAlarmsStream is the real-time SSE alarm feed. A Last-Event-ID
// header resumes from that cursor: backlog since then replays before the
// stream continues live.
//
// The cursor is the broker's own monotonic publish-order sequence (see
// broker.go's package comment for why it can't be the alarm's ts): the
// SSE `id:` line carries that sequence, not event_id, so a client's
// Last-Event-ID round-trips it back to us as the resume point.
func (s *Server) handleAlarmsStream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	// Subscribe before replaying backlog so nothing published concurrently
	// with the replay can slip through the gap; sent (keyed by event_id,
	// unaffected by the seq change) drops the resulting duplicate if the
	// same alarm shows up on both paths.
	ch, unsubscribe := s.broker.subscribe()
	defer unsubscribe()

	sent := make(map[string]bool)
	if cursor, ok := parseCursor(r.Header.Get("Last-Event-ID")); ok {
		for _, a := range s.broker.historySince(cursor) {
			if err := writeSSEAlarm(w, flusher, a); err != nil {
				return
			}
			sent[a.EventID] = true
		}
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case a := <-ch:
			if sent[a.EventID] {
				continue
			}
			if err := writeSSEAlarm(w, flusher, a); err != nil {
				return
			}
			sent[a.EventID] = true
		}
	}
}

func writeSSEAlarm(w http.ResponseWriter, flusher http.Flusher, a deliveredAlarm) error {
	payload, err := json.Marshal(a.Alarm)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %d\nevent: alarm\ndata: %s\n\n", a.Seq, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// parseCursor reads a Last-Event-ID header back as the broker sequence
// it was minted from (see writeSSEAlarm). ok is false for an empty or
// malformed id, in which case the caller skips backlog replay entirely
// and starts the subscriber live-only — the same fail-open behavior as
// before, just on a parse failure instead of a missing header.
//
// Residual, honestly stated: the sequence is in-memory and resets to 0
// on every process restart. A cursor held by a client across a hard
// restart is being replayed against a different counter than the one
// that minted it — it may resolve to the wrong point, or to nothing at
// all if the new process hasn't published that many alarms yet. Closing
// that gap needs the sequence (or the alarm history itself) to survive
// a restart the way the event log already does; not done here — see
// SUBMISSION.md.
func parseCursor(id string) (seq int64, ok bool) {
	if id == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
