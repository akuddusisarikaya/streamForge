// Package worker consumes events from the ingestion buffer, classifies them,
// and keeps per-type counts.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"streamforge/internal/event"
)

// Counts is a thread-safe per-event-type counter, shared by all workers in
// the pool.
type Counts struct {
	mu     sync.Mutex
	counts map[event.Type]int64
}

// NewCounts creates an empty counter set.
func NewCounts() *Counts {
	return &Counts{counts: make(map[event.Type]int64)}
}

func (c *Counts) inc(t event.Type) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[t]++
}

// Snapshot returns a copy of the current counts, safe to read without
// racing further increments.
func (c *Counts) Snapshot() map[event.Type]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	snap := make(map[event.Type]int64, len(c.counts))
	for t, n := range c.counts {
		snap[t] = n
	}
	return snap
}

// Recorder receives the end-to-end latency of a processed event. Defined
// here (rather than depending on the metrics package) so worker has no
// import on metrics — metrics depends on worker for Counts, and a
// reverse dependency would be a cycle.
type Recorder interface {
	RecordProcessed(latency time.Duration)
}

// StartPool launches n worker goroutines that read from in, classify each
// event, update counts, and report latency to rec, until ctx is canceled
// or in is closed. It registers each goroutine on wg so callers can wait
// for a clean shutdown.
func StartPool(ctx context.Context, wg *sync.WaitGroup, in <-chan event.Event, n int, counts *Counts, rec Recorder) {
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case evt, ok := <-in:
					if !ok {
						return
					}
					process(workerID, evt, counts, rec)
				}
			}
		}(i)
	}
}

func process(workerID int, evt event.Event, counts *Counts, rec Recorder) {
	counts.inc(evt.Type)
	rec.RecordProcessed(time.Since(evt.Timestamp))
	fmt.Printf("[worker %d] processed event: id=%s type=%s\n", workerID, evt.ID, evt.Type)
}
