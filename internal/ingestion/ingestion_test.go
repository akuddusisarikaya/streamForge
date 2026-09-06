package ingestion

import (
	"context"
	"testing"
	"time"

	"streamforge/internal/event"
)

func TestBuffer_SendAndChan(t *testing.T) {
	buf := NewBuffer(2)
	ctx := context.Background()

	if !buf.Send(ctx, event.Event{ID: "1", Type: event.TypeError}) {
		t.Fatal("Send returned false, want true")
	}
	if got := buf.Len(); got != 1 {
		t.Errorf("Len() = %d, want 1", got)
	}
	if got := buf.Cap(); got != 2 {
		t.Errorf("Cap() = %d, want 2", got)
	}

	got := <-buf.Chan()
	if got.ID != "1" {
		t.Errorf("received ID %q, want %q", got.ID, "1")
	}
}

func TestBuffer_SendBlocksWhenFullAndRecordsBackpressure(t *testing.T) {
	buf := NewBuffer(1)
	ctx := context.Background()

	if !buf.Send(ctx, event.Event{ID: "1"}) {
		t.Fatal("first Send returned false, want true (buffer has room)")
	}

	sendDone := make(chan bool, 1)
	go func() {
		sendDone <- buf.Send(ctx, event.Event{ID: "2"})
	}()

	select {
	case <-sendDone:
		t.Fatal("second Send returned before the buffer had room; expected it to block")
	case <-time.After(50 * time.Millisecond):
		// still blocked, as expected
	}

	<-buf.Chan() // frees one slot

	select {
	case ok := <-sendDone:
		if !ok {
			t.Fatal("Send returned false, want true")
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not unblock after the buffer had room")
	}

	stats := buf.Stats()
	if stats.BlockedSends != 1 {
		t.Errorf("BlockedSends = %d, want 1", stats.BlockedSends)
	}
	if stats.BlockedTotal <= 0 {
		t.Errorf("BlockedTotal = %v, want > 0", stats.BlockedTotal)
	}
}

func TestBuffer_SendDoesNotCountAFreeSlotAsBlocked(t *testing.T) {
	buf := NewBuffer(3)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if !buf.Send(ctx, event.Event{ID: "x"}) {
			t.Fatalf("Send %d returned false, want true", i)
		}
	}

	stats := buf.Stats()
	if stats.BlockedSends != 0 {
		t.Errorf("BlockedSends = %d, want 0 (buffer never filled)", stats.BlockedSends)
	}
}

func TestBuffer_SendReturnsFalseOnContextCancel(t *testing.T) {
	buf := NewBuffer(1)
	ctx := context.Background()

	if !buf.Send(ctx, event.Event{ID: "1"}) {
		t.Fatal("Send returned false, want true")
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	if buf.Send(canceledCtx, event.Event{ID: "2"}) {
		t.Fatal("Send returned true with an already-canceled context, want false")
	}
}
