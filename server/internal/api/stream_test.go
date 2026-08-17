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
	if id != "1" {
		t.Errorf("expected id to be the first published sequence (1), got %q", id)
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

	// Give a flush tick time to discover and publish both falls (assigning
	// them broker sequences 1 and 2) before anyone subscribes — this is
	// what "generated during a gap" means: discovered while disconnected.
	time.Sleep(2 * alarmFlushInterval)

	// Reconnect with a cursor from before either sequence was assigned.
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/alarms/stream", nil)
	req.Header.Set("Last-Event-ID", "0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	id1, data1 := readSSEEvent(t, reader)
	id2, data2 := readSSEEvent(t, reader)
	if id1 != "1" || !strings.Contains(data1, `"device_id":"dev_0001"`) {
		t.Errorf("expected seq 1 from dev_0001 first, got id=%q data=%q", id1, data1)
	}
	if id2 != "2" || !strings.Contains(data2, `"device_id":"dev_0002"`) {
		t.Errorf("expected seq 2 from dev_0002 second, got id=%q data=%q", id2, data2)
	}
}

// TestAlarmsStreamResumeSurvivesLateReplayAfterDiscovery reproduces the
// exact gap the ts-based cursor used to have: an alarm whose event ts is
// old (a late-replayed fall from an offline device) but that is only
// *discovered* — and therefore only published — after a client's cursor
// was minted. A ts-based cursor would filter it out of backlog as "too
// old" while the live path also skips it (already in broker.seen),
// losing it permanently. The seq-based cursor must still deliver it,
// because "discovered after your cursor" is exactly what it tracks.
func TestAlarmsStreamResumeSurvivesLateReplayAfterDiscovery(t *testing.T) {
	s := New()
	ts := httptest.NewServer(s)
	defer ts.Close()

	// A normal, recent fall — discovered and published first, seq 1.
	recent := time.Now().UTC().Add(-time.Second)
	postFall(t, ts.URL, "dev_0001", "room_14", recent, 0.9)
	time.Sleep(2 * alarmFlushInterval)

	// Client connects, immediately gets seq 1 live, and remembers seq 1
	// as its cursor for a future reconnect.
	cursor := int64(1)

	// Now an offline device reconnects and replays a fall from long
	// before the client's cursor was set — old ts, but not yet seen by
	// the broker, so it's a brand-new discovery once it lands.
	late := time.Now().UTC().Add(-30 * time.Minute)
	postFall(t, ts.URL, "dev_0002", "room_14", late, 0.9)
	time.Sleep(2 * alarmFlushInterval)

	req, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+"/alarms/stream", nil)
	req.Header.Set("Last-Event-ID", strconv.FormatInt(cursor, 10))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)

	id, data := readSSEEvent(t, reader)
	if id != "2" || !strings.Contains(data, `"device_id":"dev_0002"`) {
		t.Fatalf("expected the late-replayed dev_0002 alarm (seq 2) to survive the reconnect, got id=%q data=%q", id, data)
	}
}

func postFall(t *testing.T, baseURL, deviceID, roomID string, ts time.Time, confidence float64) {
	t.Helper()
	resp, err := http.Post(baseURL+"/events", "application/json", strings.NewReader(
		`{"device_id":"`+deviceID+`","room_id":"`+roomID+`","type":"fall_warn","ts":"`+ts.Format(time.RFC3339Nano)+`","seq":1,"confidence":`+strconv.FormatFloat(confidence, 'f', 2, 64)+`}`))
	if err != nil {
		t.Fatalf("post fall_warn: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post fall_warn status = %d, want 202", resp.StatusCode)
	}
}
