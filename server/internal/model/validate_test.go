package model

import (
	"testing"
	"time"
)

func validHeartbeat() Event {
	return Event{
		DeviceID: "dev_0001",
		RoomID:   "room_14",
		Type:     Heartbeat,
		TS:       time.Now(),
		Seq:      1,
	}
}

func ptr[T any](v T) *T { return &v }

func TestValidate(t *testing.T) {
	cases := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{"valid heartbeat", validHeartbeat(), false},
		{"missing device_id", func() Event { e := validHeartbeat(); e.DeviceID = ""; return e }(), true},
		{"missing room_id", func() Event { e := validHeartbeat(); e.RoomID = ""; return e }(), true},
		{"missing ts", func() Event { e := validHeartbeat(); e.TS = time.Time{}; return e }(), true},
		{"unknown type", func() Event { e := validHeartbeat(); e.Type = "bogus"; return e }(), true},

		{"valid presence", func() Event {
			e := validHeartbeat()
			e.Type = Presence
			e.InRoom = ptr(true)
			return e
		}(), false},
		{"presence missing in_room", func() Event { e := validHeartbeat(); e.Type = Presence; return e }(), true},

		{"valid motion", func() Event {
			e := validHeartbeat()
			e.Type = Motion
			e.Magnitude = ptr(0.5)
			return e
		}(), false},
		{"motion missing magnitude", func() Event { e := validHeartbeat(); e.Type = Motion; return e }(), true},
		{"motion magnitude out of range", func() Event {
			e := validHeartbeat()
			e.Type = Motion
			e.Magnitude = ptr(1.5)
			return e
		}(), true},

		{"valid sleep_state", func() Event {
			e := validHeartbeat()
			e.Type = SleepState
			e.State = ptr("asleep")
			return e
		}(), false},
		{"sleep_state invalid value", func() Event {
			e := validHeartbeat()
			e.Type = SleepState
			e.State = ptr("napping")
			return e
		}(), true},

		{"valid fall_warn", func() Event {
			e := validHeartbeat()
			e.Type = FallWarn
			e.Confidence = ptr(0.9)
			return e
		}(), false},
		{"fall_warn confidence out of range", func() Event {
			e := validHeartbeat()
			e.Type = FallWarn
			e.Confidence = ptr(-0.1)
			return e
		}(), true},

		{"valid net_status", func() Event {
			e := validHeartbeat()
			e.Type = NetStatus
			e.RSSI = ptr(-68)
			return e
		}(), false},
		{"net_status missing rssi", func() Event { e := validHeartbeat(); e.Type = NetStatus; return e }(), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.event.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestAcceptable(t *testing.T) {
	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		name    string
		ts      time.Time
		wantErr bool
	}{
		{"now", now, false},
		{"59m in future", now.Add(59 * time.Minute), false},
		{"exactly 1h in future (boundary, accepted)", now.Add(time.Hour), false},
		{"61m in future rejected", now.Add(61 * time.Minute), true},
		{"59m in past", now.Add(-59 * time.Minute), false},
		{"exactly 1h in past (boundary, accepted)", now.Add(-time.Hour), false},
		{"61m in past rejected", now.Add(-61 * time.Minute), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := Event{TS: tc.ts}
			err := e.Acceptable(now)
			if (err != nil) != tc.wantErr {
				t.Errorf("Acceptable() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
