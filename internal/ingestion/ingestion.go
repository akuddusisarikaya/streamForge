// Package ingestion holds the buffer that receives events before workers
// consume them, and applies backpressure when that buffer is full.
package ingestion

import (
	"context"
	"sync"
	"time"

	"streamforge/internal/event"
)

// blockedThreshold is the minimum wait before a send counts as
// "backpressure applied" rather than noise from normal scheduling.
const blockedThreshold = time.Millisecond

// Buffer is a channel-based, bounded queue between the generator and the
// worker pool. When full, Send blocks the caller instead of dropping
// events: StreamForge's core requirement is not losing data, so a slow
// consumer must slow down the producer rather than discard input.
type Buffer struct {
	ch    chan event.Event
	stats *stats
}

// NewBuffer creates a buffer with the given capacity. Capacity is
// deliberately bounded (not unbounded) so backpressure is actually
// reachable under load, instead of just growing memory forever.
func NewBuffer(capacity int) *Buffer {
	return &Buffer{
		ch:    make(chan event.Event, capacity),
		stats: &stats{},
	}
}

// Send enqueues evt, blocking if the buffer is full. It returns false only
// if ctx is canceled while waiting, so callers can stop cleanly on
// shutdown instead of blocking forever.
func (b *Buffer) Send(ctx context.Context, evt event.Event) bool {
	start := time.Now()
	select {
	case b.ch <- evt:
		if waited := time.Since(start); waited >= blockedThreshold {
			b.stats.recordBlocked(waited)
		}
		return true
	case <-ctx.Done():
		return false
	}
}

// Chan exposes the receive side for workers to read from.
func (b *Buffer) Chan() <-chan event.Event {
	return b.ch
}

// Len returns the current number of buffered, unprocessed events.
func (b *Buffer) Len() int {
	return len(b.ch)
}

// Cap returns the buffer's fixed capacity.
func (b *Buffer) Cap() int {
	return cap(b.ch)
}

// Stats returns a snapshot of backpressure statistics observed so far.
func (b *Buffer) Stats() StatsSnapshot {
	return b.stats.snapshot()
}

// StatsSnapshot is a point-in-time read of backpressure statistics.
type StatsSnapshot struct {
	BlockedSends int64         // number of Send calls that had to wait
	BlockedTotal time.Duration // cumulative time spent waiting
}

type stats struct {
	mu           sync.Mutex
	blockedSends int64
	blockedTotal time.Duration
}

func (s *stats) recordBlocked(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockedSends++
	s.blockedTotal += d
}

func (s *stats) snapshot() StatsSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatsSnapshot{BlockedSends: s.blockedSends, BlockedTotal: s.blockedTotal}
}
