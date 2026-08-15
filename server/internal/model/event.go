// Package model defines the event envelope
package model

import "time"

type EventType string

const (
	Heartbeat  EventType = "heartbeat"
	Presence   EventType = "presence"
	Motion     EventType = "motion"
	SleepState EventType = "sleep_state"
	FallWarn   EventType = "fall_warn"
	NetStatus  EventType = "net_status"
)

type Event struct {
	DeviceID string    `json:"device_id"`
	RoomID   string    `json:"room_id"`
	Type     EventType `json:"type"`
	TS       time.Time `json:"ts"`
	Seq      int64     `json:"seq"`

	InRoom     *bool    `json:"in_room,omitempty"`
	Magnitude  *float64 `json:"magnitude,omitempty"`
	State      *string  `json:"state,omitempty"`
	Confidence *float64 `json:"confidence,omitempty"`
	RSSI       *int     `json:"rssi,omitempty"`
}
