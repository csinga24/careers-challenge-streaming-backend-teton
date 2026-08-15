package intake

import (
	"testing"
	"time"
)

func TestLimiterAcquireReleaseAllowsReuse(t *testing.T) {
	l := NewLimiter(1, 1)
	release := l.Acquire(false)
	release()

	done := make(chan struct{})
	go func() {
		l.Acquire(false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected acquire to succeed immediately after release")
	}
}

func TestLimiterBlocksWhenLaneFull(t *testing.T) {
	l := NewLimiter(1, 1)
	release1 := l.Acquire(false)

	acquired := make(chan func())
	go func() {
		acquired <- l.Acquire(false)
	}()

	select {
	case <-acquired:
		t.Fatal("expected second acquire to block while the lane is full")
	case <-time.After(100 * time.Millisecond):
	}

	release1()

	select {
	case release2 := <-acquired:
		release2()
	case <-time.After(time.Second):
		t.Fatal("expected second acquire to succeed after release")
	}
}

func TestLimiterLanesAreIndependent(t *testing.T) {
	l := NewLimiter(1, 1)
	releaseNormal := l.Acquire(false)
	defer releaseNormal()

	done := make(chan struct{})
	go func() {
		release := l.Acquire(true) // high-priority lane, untouched
		close(done)
		release()
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected high-priority acquire to succeed despite normal lane being full")
	}
}

func TestLimiterDepth(t *testing.T) {
	l := NewLimiter(2, 2)
	if high, normal := l.Depth(); high != 0 || normal != 0 {
		t.Fatalf("expected 0,0 depth initially, got %d,%d", high, normal)
	}

	releaseHigh := l.Acquire(true)
	releaseNormal := l.Acquire(false)
	if high, normal := l.Depth(); high != 1 || normal != 1 {
		t.Fatalf("expected 1,1 depth after one acquire each, got %d,%d", high, normal)
	}

	releaseHigh()
	releaseNormal()
	if high, normal := l.Depth(); high != 0 || normal != 0 {
		t.Fatalf("expected 0,0 depth after release, got %d,%d", high, normal)
	}
}
