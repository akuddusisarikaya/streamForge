package metrics

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"streamforge/internal/ingestion"
	"streamforge/internal/worker"
)

type fakeRate struct{ rate int64 }

func (f fakeRate) Rate() int64 { return f.rate }

func TestRecorder_RecordProcessedAccumulates(t *testing.T) {
	r := NewRecorder()
	r.RecordProcessed(10 * time.Millisecond)
	r.RecordProcessed(20 * time.Millisecond)

	if got := r.processed.Load(); got != 2 {
		t.Errorf("processed = %d, want 2", got)
	}
	want := int64(30 * time.Millisecond)
	if got := r.latencySumNs.Load(); got != want {
		t.Errorf("latencySumNs = %d, want %d", got, want)
	}
}

func TestReporter_SampleComputesSnapshot(t *testing.T) {
	rec := NewRecorder()
	buf := ingestion.NewBuffer(10)
	counts := worker.NewCounts()
	rep := NewReporter(rec, buf, counts, fakeRate{rate: 42}, time.Second)

	rec.RecordProcessed(10 * time.Millisecond)
	rec.RecordProcessed(30 * time.Millisecond)

	rep.sample()
	snap := rep.Latest()

	if snap.TargetRatePerSec != 42 {
		t.Errorf("TargetRatePerSec = %d, want 42", snap.TargetRatePerSec)
	}
	if snap.QueueCapacity != 10 {
		t.Errorf("QueueCapacity = %d, want 10", snap.QueueCapacity)
	}
	if snap.QueueLength != 0 {
		t.Errorf("QueueLength = %d, want 0", snap.QueueLength)
	}

	const wantAvg = 20.0 // (10ms + 30ms) / 2 events
	if diff := snap.AvgLatencyMs - wantAvg; diff < -0.01 || diff > 0.01 {
		t.Errorf("AvgLatencyMs = %v, want ~%v", snap.AvgLatencyMs, wantAvg)
	}
	if snap.ThroughputPerSec <= 0 {
		t.Errorf("ThroughputPerSec = %v, want > 0", snap.ThroughputPerSec)
	}
}

func TestReporter_SampleIsDeltaNotCumulative(t *testing.T) {
	rec := NewRecorder()
	buf := ingestion.NewBuffer(10)
	counts := worker.NewCounts()
	rep := NewReporter(rec, buf, counts, fakeRate{rate: 1}, time.Second)

	rec.RecordProcessed(10 * time.Millisecond)
	rep.sample()

	rec.RecordProcessed(100 * time.Millisecond)
	rep.sample()
	second := rep.Latest()

	// If latency were averaged cumulatively since startup, this would land
	// around 55ms ((10+100)/2). Delta-since-last-tick means the second
	// sample reflects only the 100ms event.
	if second.AvgLatencyMs < 99 || second.AvgLatencyMs > 101 {
		t.Errorf("AvgLatencyMs = %v, want ~100 (delta since previous sample, not cumulative)", second.AvgLatencyMs)
	}
}

func TestReporter_SnapshotIncludesBackpressureAndCounts(t *testing.T) {
	rec := NewRecorder()
	buf := ingestion.NewBuffer(1)
	counts := worker.NewCounts()
	rep := NewReporter(rec, buf, counts, fakeRate{rate: 1}, time.Second)

	rep.sample()
	snap := rep.Latest()

	if snap.CountsByType == nil {
		t.Error("CountsByType is nil, want an empty (non-nil) map")
	}
	if snap.BackpressureBlockedSends != 0 {
		t.Errorf("BackpressureBlockedSends = %d, want 0", snap.BackpressureBlockedSends)
	}
}

func TestHandler_ServesLatestSnapshotAsJSON(t *testing.T) {
	rec := NewRecorder()
	buf := ingestion.NewBuffer(5)
	counts := worker.NewCounts()
	rep := NewReporter(rec, buf, counts, fakeRate{rate: 7}, time.Second)
	rep.sample()

	req := httptest.NewRequest("GET", "/stats", nil)
	w := httptest.NewRecorder()
	rep.Handler()(w, req)

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var snap Snapshot
	if err := json.Unmarshal(w.Body.Bytes(), &snap); err != nil {
		t.Fatalf("failed to decode response body: %v", err)
	}
	if snap.TargetRatePerSec != 7 {
		t.Errorf("TargetRatePerSec = %d, want 7", snap.TargetRatePerSec)
	}
	if snap.QueueCapacity != 5 {
		t.Errorf("QueueCapacity = %d, want 5", snap.QueueCapacity)
	}
}
