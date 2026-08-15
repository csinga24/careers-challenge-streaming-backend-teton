package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func postEvent(t *testing.T, s *Server, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func TestHandleEventIntake(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	cases := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "valid heartbeat",
			body:       `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"` + now + `","seq":1}`,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "invalid json",
			body:       `not json`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing device_id",
			body:       `{"room_id":"room_14","type":"heartbeat","ts":"` + now + `"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "ts too far in the future",
			body:       `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"2099-01-01T00:00:00Z"}`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := New()
			rec := postEvent(t, s, tc.body)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

func TestHandleEventIntakeStoresAcceptedEvent(t *testing.T) {
	s := New()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	rec := postEvent(t, s, `{"device_id":"dev_0001","room_id":"room_14","type":"heartbeat","ts":"`+now+`","seq":1}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 (body %s)", rec.Code, rec.Body.String())
	}
	if got := s.store.Events("dev_0001"); len(got) != 1 {
		t.Fatalf("expected 1 stored event, got %d", len(got))
	}
}

func TestHandleDeviceHealth(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/devices/dev_0001/health", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if _, ok := body["last_heartbeat_ts"]; !ok {
		t.Errorf("missing last_heartbeat_ts field")
	}
	if _, ok := body["availability_5m"]; !ok {
		t.Errorf("missing availability_5m field")
	}
}

func TestHandleRoomOccupancy(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/rooms/room_14/occupancy?window=1m", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if _, ok := body["in_room"]; !ok {
		t.Errorf("missing in_room field")
	}
	if _, ok := body["occupied_pct"]; !ok {
		t.Errorf("missing occupied_pct field")
	}
}

func TestHandleAlarms(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/alarms?since=0", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Alarms []any `json:"alarms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid json body: %v", err)
	}
	if body.Alarms == nil {
		t.Errorf("expected alarms to be an empty array, got null")
	}
}

func TestUnknownRoute(t *testing.T) {
	s := New()
	req := httptest.NewRequest(http.MethodGet, "/nonexistent", nil)
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
