package api

import (
	"encoding/json"
	"net/http"
	"time"

	"teton-streaming-backend/internal/model"
)

func (s *Server) handleEventIntake(w http.ResponseWriter, r *http.Request) {
	var e model.Event
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json: " + err.Error()})
		return
	}
	if err := e.Validate(); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if err := e.Acceptable(time.Now().UTC()); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	s.store.Append(e)
	writeJSON(w, http.StatusAccepted, map[string]bool{"ok": true})
}

func (s *Server) handleDeviceHealth(w http.ResponseWriter, r *http.Request) {
	deviceID := r.PathValue("device_id")
	events := s.store.Events(deviceID)
	writeJSON(w, http.StatusOK, computeDeviceHealth(events, time.Now().UTC()))
}

func (s *Server) handleRoomOccupancy(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("room_id")
	window, err := parseWindow(r.URL.Query().Get("window"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	events := s.store.RoomPresenceEvents(roomID)
	writeJSON(w, http.StatusOK, computeRoomOccupancy(events, window, time.Now().UTC()))
}

func (s *Server) handleAlarms(w http.ResponseWriter, r *http.Request) {
	_ = r.URL.Query().Get("since")
	writeJSON(w, http.StatusOK, map[string]any{
		"alarms": []any{},
	})
}
