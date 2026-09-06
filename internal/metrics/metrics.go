// Package metrics samples throughput, latency, queue depth, and per-type
// counts on a fixed interval, prints them to the console, and serves the
// latest snapshot as JSON over HTTP.
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"streamforge/internal/ingestion"
	"streamforge/internal/worker"
)

// Recorder accumulates raw processed-event data. Workers call
// RecordProcessed once per event; Reporter turns the accumulated totals
// into rates on each sampling tick. Using atomics here means workers never
// contend on a mutex on the hot path.
type Recorder struct {
	processed    atomic.Int64
	latencySumNs atomic.Int64
}

// NewRecorder creates an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{}
}

// RecordProcessed records that one event was processed with the given
// end-to-end latency (time from generation to processing).
func (r *Recorder) RecordProcessed(latency time.Duration) {
	r.processed.Add(1)
	r.latencySumNs.Add(int64(latency))
}

// Snapshot is a point-in-time read of all metrics, serialized as the
// /stats JSON response.
type Snapshot struct {
	Timestamp                time.Time        `json:"timestamp"`
	TargetRatePerSec         int64            `json:"target_rate_per_sec"`
	ThroughputPerSec         float64          `json:"throughput_per_sec"`
	AvgLatencyMs             float64          `json:"avg_latency_ms"`
	QueueLength              int              `json:"queue_length"`
	QueueCapacity            int              `json:"queue_capacity"`
	CountsByType             map[string]int64 `json:"counts_by_type"`
	BackpressureBlockedSends int64            `json:"backpressure_blocked_sends"`
	BackpressureBlockedMs    float64          `json:"backpressure_blocked_ms"`
}

// rateProvider is satisfied by *generator.RateController. Defined locally
// (rather than importing generator) so metrics doesn't take on a
// dependency it only needs one method from.
type rateProvider interface {
	Rate() int64
}

// Reporter periodically samples a Recorder, an ingestion.Buffer, and
// worker.Counts into a Snapshot.
//
// Throughput and average latency are both computed as deltas since the
// previous tick (not cumulative since startup), so a slowdown shows up
// within one interval instead of being smoothed away by history - this
// matters for the load test step, where we want to see degradation as
// soon as it happens.
type Reporter struct {
	recorder *Recorder
	buffer   *ingestion.Buffer
	counts   *worker.Counts
	rate     rateProvider
	interval time.Duration

	mu     sync.RWMutex
	latest Snapshot

	lastProcessed    int64
	lastLatencySumNs int64
	lastSample       time.Time
}

// NewReporter creates a Reporter that samples every interval.
func NewReporter(r *Recorder, buf *ingestion.Buffer, counts *worker.Counts, rate rateProvider, interval time.Duration) *Reporter {
	return &Reporter{
		recorder:   r,
		buffer:     buf,
		counts:     counts,
		rate:       rate,
		interval:   interval,
		lastSample: time.Now(),
	}
}

// Run samples on a ticker, printing each snapshot to the console, until
// ctx is canceled.
func (rep *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(rep.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rep.sample()
		}
	}
}

func (rep *Reporter) sample() {
	now := time.Now()
	elapsed := now.Sub(rep.lastSample).Seconds()

	processed := rep.recorder.processed.Load()
	latencySumNs := rep.recorder.latencySumNs.Load()

	deltaProcessed := processed - rep.lastProcessed
	deltaLatencyNs := latencySumNs - rep.lastLatencySumNs

	var throughput float64
	if elapsed > 0 {
		throughput = float64(deltaProcessed) / elapsed
	}

	var avgLatencyMs float64
	if deltaProcessed > 0 {
		avgLatencyMs = float64(deltaLatencyNs) / float64(deltaProcessed) / float64(time.Millisecond)
	}

	rep.lastProcessed = processed
	rep.lastLatencySumNs = latencySumNs
	rep.lastSample = now

	countsByType := make(map[string]int64)
	for t, n := range rep.counts.Snapshot() {
		countsByType[string(t)] = n
	}

	bs := rep.buffer.Stats()

	snap := Snapshot{
		Timestamp:                now,
		TargetRatePerSec:         rep.rate.Rate(),
		ThroughputPerSec:         throughput,
		AvgLatencyMs:             avgLatencyMs,
		QueueLength:              rep.buffer.Len(),
		QueueCapacity:            rep.buffer.Cap(),
		CountsByType:             countsByType,
		BackpressureBlockedSends: bs.BlockedSends,
		BackpressureBlockedMs:    float64(bs.BlockedTotal) / float64(time.Millisecond),
	}

	rep.mu.Lock()
	rep.latest = snap
	rep.mu.Unlock()

	fmt.Printf("[metrics] target=%d/s throughput=%.1f/s avg_latency=%.2fms queue=%d/%d counts=%v blocked_sends=%d blocked_ms=%.1f\n",
		snap.TargetRatePerSec, snap.ThroughputPerSec, snap.AvgLatencyMs, snap.QueueLength, snap.QueueCapacity,
		snap.CountsByType, snap.BackpressureBlockedSends, snap.BackpressureBlockedMs)
}

// Latest returns the most recently computed snapshot.
func (rep *Reporter) Latest() Snapshot {
	rep.mu.RLock()
	defer rep.mu.RUnlock()
	return rep.latest
}

// Handler serves the latest snapshot as JSON, for mounting at /stats.
func (rep *Reporter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rep.Latest())
	}
}
