package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"teton-streaming-backend/internal/model"
)

// handleAlarmsStream is the real-time SSE alarm feed. A Last-Event-ID
// header resumes from that cursor: backlog since then replays before the
// stream continues live.
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

	ch, unsubscribe := s.broker.subscribe()
	defer unsubscribe()

	sent := make(map[string]bool)
	if cursor, ok := eventIDTS(r.Header.Get("Last-Event-ID")); ok {
		for _, a := range deduplicateFalls(s.store.FallWarnEvents()) {
			if !a.TS.After(cursor) {
				continue
			}
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

func writeSSEAlarm(w http.ResponseWriter, flusher http.Flusher, a model.Alarm) error {
	payload, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "id: %s\nevent: alarm\ndata: %s\n\n", a.EventID, payload); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}

// eventIDTS extracts the ts embedded in an alarm event_id
// ("<device_id>-<unix_nanos>", see newAlarm), used to resume a stream
// from Last-Event-ID. ok is false for an empty or malformed id.
func eventIDTS(id string) (ts time.Time, ok bool) {
	if id == "" {
		return time.Time{}, false
	}
	idx := strings.LastIndex(id, "-")
	if idx < 0 {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(id[idx+1:], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, n).UTC(), true
}
