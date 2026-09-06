package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"streamforge/internal/event"
)

func TestCounts_SnapshotIsIndependentCopy(t *testing.T) {
	c := NewCounts()
	c.inc(event.TypeError)
	c.inc(event.TypeError)
	c.inc(event.TypePayment)

	snap := c.Snapshot()
	if got := snap[event.TypeError]; got != 2 {
		t.Errorf("snapshot[error] = %d, want 2", got)
	}
	if got := snap[event.TypePayment]; got != 1 {
		t.Errorf("snapshot[payment] = %d, want 1", got)
	}

	c.inc(event.TypeError)
	if got := snap[event.TypeError]; got != 2 {
		t.Errorf("earlier snapshot changed after a later increment: got %d, want 2", got)
	}
	if got := c.Snapshot()[event.TypeError]; got != 3 {
		t.Errorf("current snapshot[error] = %d, want 3", got)
	}
}

type stubRecorder struct {
	mu        sync.Mutex
	latencies []time.Duration
}

func (s *stubRecorder) RecordProcessed(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latencies = append(s.latencies, d)
}

func (s *stubRecorder) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.latencies)
}

func TestStartPool_ProcessesEventsAndRecordsLatency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	in := make(chan event.Event, 10)
	counts := NewCounts()
	rec := &stubRecorder{}

	var wg sync.WaitGroup
	StartPool(ctx, &wg, in, 2, counts, rec)

	in <- event.Event{ID: "1", Type: event.TypeError, Timestamp: time.Now()}
	in <- event.Event{ID: "2", Type: event.TypePayment, Timestamp: time.Now()}
	in <- event.Event{ID: "3", Type: event.TypeError, Timestamp: time.Now()}
	in <- event.Event{ID: "4", Type: event.TypeUserAction, Timestamp: time.Now()}

	deadline := time.Now().Add(time.Second)
	for rec.count() < 4 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	if got := rec.count(); got != 4 {
		t.Fatalf("recorded %d latencies, want 4", got)
	}

	snap := counts.Snapshot()
	if got := snap[event.TypeError]; got != 2 {
		t.Errorf("counts[error] = %d, want 2", got)
	}
	if got := snap[event.TypePayment]; got != 1 {
		t.Errorf("counts[payment] = %d, want 1", got)
	}
	if got := snap[event.TypeUserAction]; got != 1 {
		t.Errorf("counts[user-action] = %d, want 1", got)
	}
}

func TestStartPool_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	in := make(chan event.Event)
	counts := NewCounts()
	rec := &stubRecorder{}

	var wg sync.WaitGroup
	StartPool(ctx, &wg, in, 3, counts, rec)

	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after context cancellation")
	}
}

func TestStartPool_StopsWhenChannelClosed(t *testing.T) {
	ctx := context.Background()
	in := make(chan event.Event)
	counts := NewCounts()
	rec := &stubRecorder{}

	var wg sync.WaitGroup
	StartPool(ctx, &wg, in, 2, counts, rec)

	close(in)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("workers did not stop after the input channel was closed")
	}
}
