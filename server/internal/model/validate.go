package model

import (
	"fmt"
	"time"
)

const clockSkewTolerance = time.Hour

// Validate checks required envelope fields.
func (e Event) Validate() error {
	if e.DeviceID == "" {
		return fmt.Errorf("missing device_id")
	}
	if e.RoomID == "" {
		return fmt.Errorf("missing room_id")
	}
	if e.TS.IsZero() {
		return fmt.Errorf("missing or invalid ts")
	}

	switch e.Type {
	case Heartbeat:
		// no additional fields
	case Presence:
		if e.InRoom == nil {
			return fmt.Errorf("presence event missing in_room")
		}
	case Motion:
		if e.Magnitude == nil {
			return fmt.Errorf("motion event missing magnitude")
		}
		if *e.Magnitude < 0 || *e.Magnitude > 1 {
			return fmt.Errorf("motion magnitude %v out of range [0,1]", *e.Magnitude)
		}
	case SleepState:
		if e.State == nil {
			return fmt.Errorf("sleep_state event missing state")
		}
		switch *e.State {
		case "asleep", "awake", "unknown":
		default:
			return fmt.Errorf("sleep_state has invalid state %q", *e.State)
		}
	case FallWarn:
		if e.Confidence == nil {
			return fmt.Errorf("fall_warn event missing confidence")
		}
		if *e.Confidence < 0 || *e.Confidence > 1 {
			return fmt.Errorf("fall_warn confidence %v out of range [0,1]", *e.Confidence)
		}
	case NetStatus:
		if e.RSSI == nil {
			return fmt.Errorf("net_status event missing rssi")
		}
	default:
		return fmt.Errorf("unknown event type %q", e.Type)
	}
	return nil
}

// Acceptable reports whether ts falls within the accepted clock-skew window
func (e Event) Acceptable(now time.Time) error {
	if e.TS.After(now.Add(clockSkewTolerance)) {
		return fmt.Errorf("ts %s is more than 1h in the future", e.TS)
	}
	if e.TS.Before(now.Add(-clockSkewTolerance)) {
		return fmt.Errorf("ts %s is more than 1h in the past", e.TS)
	}
	return nil
}
