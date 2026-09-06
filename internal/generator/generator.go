// Package generator produces synthetic events for StreamForge.
package generator

import (
	"context"
	"fmt"
	"time"

	"streamforge/internal/event"
)

var types = []event.Type{event.TypeError, event.TypePayment, event.TypeUserAction}

// sender is the subset of ingestion.Buffer that Run depends on, so the
// blocking-send (backpressure) behavior lives in one place: ingestion.
type sender interface {
	Send(ctx context.Context, evt event.Event) bool
}

// Run continuously produces synthetic events and sends them to buf, one
// every interval, until ctx is canceled. If buf is full, Send blocks —
// that's the backpressure signal propagating upstream to the generator.
func Run(ctx context.Context, buf sender, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var seq int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
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
