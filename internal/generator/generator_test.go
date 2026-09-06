package generator

import (
	"context"
	"sync"
	"testing"
	"time"

	"streamforge/internal/event"
)

func TestRateController_ClampsNonPositiveRate(t *testing.T) {
	rc := NewRateController(0)
	if got := rc.Rate(); got != 1 {
		t.Errorf("Rate() after NewRateController(0) = %d, want 1", got)
	}

	rc.SetRate(-5)
	if got := rc.Rate(); got != 1 {
		t.Errorf("Rate() after SetRate(-5) = %d, want 1", got)
	}
}

func TestRateController_SetAndGet(t *testing.T) {
	rc := NewRateController(10)
	rc.SetRate(500)
	if got := rc.Rate(); got != 500 {
		t.Errorf("Rate() = %d, want 500", got)
	}
}

type fakeSender struct {
	mu     sync.Mutex
	events []event.Event
}

func (f *fakeSender) Send(_ context.Context, evt event.Event) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, evt)
	return true
}

func (f *fakeSender) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

type rejectingSender struct{}

func (rejectingSender) Send(context.Context, event.Event) bool { return false }

func TestRun_ProducesEventsAtApproximateRate(t *testing.T) {
	const rate = 100 // events/sec
	rc := NewRateController(rate)
	sender := &fakeSender{}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	Run(ctx, sender, rc)

	got := sender.count()
	want := rate * 0.3 // ~30 events expected over 300ms
	if float64(got) < want*0.5 || float64(got) > want*2 {
		t.Errorf("produced %d events in 300ms at rate=%d/s, want roughly %.0f", got, rate, want)
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	rc := NewRateController(1000)
	sender := &fakeSender{}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		Run(ctx, sender, rc)
		close(done)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context cancellation")
	}
}

func TestRun_StopsWhenSendIsRejected(t *testing.T) {
	rc := NewRateController(1000)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		Run(ctx, rejectingSender{}, rc)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after Send returned false")
	}
}
