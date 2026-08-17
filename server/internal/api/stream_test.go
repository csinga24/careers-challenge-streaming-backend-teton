package api

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// readSSELine reads one line with a deadline, so a test never hangs
// forever if the expected data doesn't show up.
func readSSELine(t *testing.T, r *bufio.Reader, timeout time.Duration) (string, bool) {
	t.Helper()
	type result struct {
		line string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		line, err := r.ReadString('\n')
		ch <- result{line, err}
	}()
	select {
	case res := <-ch:
		return res.line, res.err == nil
	case <-time.After(timeout):
		return "", false
	}
}

func readSSEEvent(t *testing.T, r *bufio.Reader) (id, data string) {
	t.Helper()
	for {
		line, ok := readSSELine(t, r, 3*time.Second)
		if !ok {
			t.Fatal("timed out waiting for SSE event")
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case strings.HasPrefix(line, "id: "):
			id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case line == "":
			if id != "" || data != "" {
				return id, data
			}
		}
	}
}

func TestAlarmsStreamDeliversNewAlarm(t *testing.T) {
	s := New()
	ts := httptest.NewServer(s)
	defer ts.Close()

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/alarms/stream", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	// Give the handler a moment to register its subscription before we
	// post, so the event isn't published before anyone is listening.
	time.Sleep(100 * time.Millisecond)

	now := time.Now().UTC().Format(time.RFC3339Nano)
	postResp, err := http.Post(ts.URL+"/events", "application/json", strings.NewReader(
		`{"device_id":"dev_0001","room_id":"room_14","type":"fall_warn","ts":"`+now+`","seq":1,"confidence":0.9}`))
	if err != nil {
		t.Fatalf("post event: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusAccepted {
		t.Fatalf("post status = %d, want 202", postResp.StatusCode)
	}

	id, data := readSSEEvent(t, reader)
	if !strings.HasPrefix(id, "dev_0001-") {
		t.Errorf("expected id to start with dev_0001-, got %q", id)
	}
	if !strings.Contains(data, `"device_id":"dev_0001"`) {
		t.Errorf("expected data to contain device_id, got %q", data)
	}
	if !strings.Contains(data, `"room_id":"room_14"`) {
		t.Errorf("expected data to contain room_id, got %q", data)
	}
}

func TestAlarmsStreamResumeFromLastEventID(t *testing.T) {
	s := New()
	ts := httptest.NewServer(s)
	defer ts.Close()

	base := time.Now().UTC().Add(-50 * time.Minute)

	// Two historical falls, posted before anyone subscribes — simulating
	// alarms that happened during a consumer's disconnected gap.
	for i, dev := range []string{"dev_0001", "dev_0002"} {
		eventTS := base.Add(time.Duration(i) * time.Minute)
		resp, err := http.Post(ts.URL+"/events", "application/json", strings.NewReader(
			`{"device_id":"`+dev+`","room_id":"room_14","type":"fall_warn","ts":"`+eventTS.Format(time.RFC3339Nano)+`","seq":1,"confidence":0.9}`))
		if err != nil {
			t.Fatalf("post event %d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("post %d status = %d, want 202", i, resp.StatusCode)
		}
	}

	// Reconnect with a cursor from before both events, using the same
	// "<anything>-<unix_nanos>" id format the server generates.
	cursor := base.Add(-time.Minute)
	lastEventID := "cursor-" + strconv.FormatInt(cursor.UnixNano(), 10)

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/alarms/stream", nil)
	req.Header.Set("Last-Event-ID", lastEventID)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	id1, _ := readSSEEvent(t, reader)
	id2, _ := readSSEEvent(t, reader)
	if !strings.HasPrefix(id1, "dev_0001-") {
		t.Errorf("expected first backlog event from dev_0001, got %q", id1)
	}
	if !strings.HasPrefix(id2, "dev_0002-") {
		t.Errorf("expected second backlog event from dev_0002, got %q", id2)
	}
}
