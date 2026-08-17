package model

import "time"

type Alarm struct {
	EventID    string    `json:"event_id"`
	DeviceID   string    `json:"device_id"`
	RoomID     string    `json:"room_id"`
	TS         time.Time `json:"ts"`
	Confidence float64   `json:"confidence"`
}
