// Package generator produces synthetic events for StreamForge.
package generator

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"streamforge/internal/event"
)

var types = []event.Type{event.TypeError, event.TypePayment, event.TypeUserAction}

// sender is the subset of ingestion.Buffer that Run depends on, so the
// blocking-send (backpressure) behavior lives in one place: ingestion.
type sender interface {
	Send(ctx context.Context, evt event.Event) bool
}

// RateController holds the generator's target rate (events per second) as
// an atomic value so it can be changed at runtime - e.g. from an HTTP
// handler while a load test is running - without restarting the process.
type RateController struct {
	rate atomic.Int64
}

// NewRateController creates a controller starting at the given rate. A
// non-positive rate is clamped to 1 to keep the derived interval finite.
func NewRateController(initial int64) *RateController {
	rc := &RateController{}
	rc.SetRate(initial)
	return rc
}

// SetRate updates the target rate. It takes effect on the generator's next
// tick, no restart needed.
func (rc *RateController) SetRate(eventsPerSec int64) {
	if eventsPerSec <= 0 {
		eventsPerSec = 1
	}
	rc.rate.Store(eventsPerSec)
}

// Rate returns the current target rate in events per second.
func (rc *RateController) Rate() int64 {
	return rc.rate.Load()
}

// tickInterval is the generator's scheduling granularity. One event per
// timer fire would seem simpler, but at high rates the required interval
// (e.g. 200µs for 5000/s) runs into the runtime/OS timer resolution, which
// in practice caps out around 1ms in constrained environments - well
// before the buffer or workers become the bottleneck. Ticking at a fixed,
// coarser interval and emitting a batch of events per tick decouples the
// achievable rate from timer resolution: the only thing left to limit
// throughput is buffer capacity and worker speed, which is what the load
// test is meant to observe.
const tickInterval = 20 * time.Millisecond

// Run continuously produces synthetic events and sends them to buf at the
// rate rc currently specifies, until ctx is canceled. Each tick emits
// however many events the current rate implies for that slice of time;
// fractional events carry over to the next tick so low rates (less than
// one event per tick) still average out correctly instead of rounding
// down to zero.
func Run(ctx context.Context, buf sender, rc *RateController) {
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	var seq int
	var carry float64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			due := float64(rc.Rate())*tickInterval.Seconds() + carry
			n := int(due)
			carry = due - float64(n)

			for i := 0; i < n; i++ {
				seq++
				evt := event.Event{
					ID:        fmt.Sprintf("evt-%d", seq),
					Type:      types[seq%len(types)],
					Payload:   map[string]any{"seq": seq},
					Timestamp: time.Now(),
				}
				if !buf.Send(ctx, evt) {
					return
				}
			}
		}
	}
}
