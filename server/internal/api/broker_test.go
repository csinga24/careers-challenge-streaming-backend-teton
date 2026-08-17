package api

import (
	"testing"
	"time"

	"teton-streaming-backend/internal/model"
)

func recvAlarm(t *testing.T, ch <-chan deliveredAlarm, timeout time.Duration) (deliveredAlarm, bool) {
	t.Helper()
	select {
	case a := <-ch:
		return a, true
	case <-time.After(timeout):
		return deliveredAlarm{}, false
	}
}

func TestAlarmBrokerDeliversNewAlarm(t *testing.T) {
	b := newAlarmBroker(newFallReceiptTracker())
	ch, unsubscribe := b.subscribe()
	defer unsubscribe()

	b.publishNew([]model.Alarm{{EventID: "a1", DeviceID: "dev_1"}})

	got, ok := recvAlarm(t, ch, time.Second)
	if !ok {
		t.Fatal("expected to receive an alarm")
	}
	if got.EventID != "a1" {
		t.Errorf("expected event_id a1, got %q", got.EventID)
	}
}

func TestAlarmBrokerDoesNotRedeliverSeenAlarm(t *testing.T) {
	b := newAlarmBroker(newFallReceiptTracker())
	ch, unsubscribe := b.subscribe()
	defer unsubscribe()

	b.publishNew([]model.Alarm{{EventID: "a1"}})
	if _, ok := recvAlarm(t, ch, time.Second); !ok {
		t.Fatal("expected first publish to deliver")
	}

	// Same event_id again (as would happen if deduplicateFalls is recomputed
	// and the cluster is unchanged) must not be redelivered.
	b.publishNew([]model.Alarm{{EventID: "a1"}})
	if _, ok := recvAlarm(t, ch, 100*time.Millisecond); ok {
		t.Fatal("did not expect a redelivery of an already-seen alarm")
	}
}

func TestAlarmBrokerMultipleSubscribers(t *testing.T) {
	b := newAlarmBroker(newFallReceiptTracker())
	ch1, unsub1 := b.subscribe()
	defer unsub1()
	ch2, unsub2 := b.subscribe()
	defer unsub2()

	b.publishNew([]model.Alarm{{EventID: "a1"}})

	if _, ok := recvAlarm(t, ch1, time.Second); !ok {
		t.Error("subscriber 1 did not receive the alarm")
	}
	if _, ok := recvAlarm(t, ch2, time.Second); !ok {
		t.Error("subscriber 2 did not receive the alarm")
	}
}

func TestAlarmBrokerUnsubscribeStopsDelivery(t *testing.T) {
	b := newAlarmBroker(newFallReceiptTracker())
	ch, unsubscribe := b.subscribe()
	unsubscribe()

	b.publishNew([]model.Alarm{{EventID: "a1"}})

	if _, ok := recvAlarm(t, ch, 100*time.Millisecond); ok {
		t.Fatal("did not expect delivery after unsubscribe")
	}
}

func TestFlushAlarmsOnceOrdersDespiteRacingAppendOrder(t *testing.T) {
	s := New()
	ch, unsubscribe := s.broker.subscribe()
	defer unsubscribe()

	now := time.Date(2026, 8, 15, 12, 0, 0, 0, time.UTC)

	// Later-ts fall appended first, simulating its request finishing
	// before the earlier-ts fall's. Delivery should still come out sorted.
	s.store.Append(fallWarnAt("dev_2", now.Add(5*time.Second), 0.9))
	s.store.Append(fallWarnAt("dev_1", now, 0.8))

	s.flushAlarmsOnce()

	first, ok := recvAlarm(t, ch, time.Second)
	if !ok {
		t.Fatal("expected first alarm")
	}
	second, ok := recvAlarm(t, ch, time.Second)
	if !ok {
		t.Fatal("expected second alarm")
	}

	if first.DeviceID != "dev_1" || !first.TS.Equal(now) {
		t.Errorf("expected dev_1 (earlier ts) first, got %+v", first)
	}
	if second.DeviceID != "dev_2" || !second.TS.Equal(now.Add(5*time.Second)) {
		t.Errorf("expected dev_2 (later ts) second, got %+v", second)
	}
}

func TestFlushAlarmsOnceNoOpWithNoFallEvents(t *testing.T) {
	s := New()
	ch, unsubscribe := s.broker.subscribe()
	defer unsubscribe()

	s.flushAlarmsOnce()

	if _, ok := recvAlarm(t, ch, 100*time.Millisecond); ok {
		t.Fatal("did not expect a delivery when no fall_warn events exist")
	}
}
