// Package generator produces synthetic events for StreamForge.
package generator

import (
	"context"
	"fmt"
	"time"

	"streamforge/internal/event"
)

var types = []event.Type{event.TypeError, event.TypePayment, event.TypeUserAction}

// Run continuously produces synthetic events and sends them to out, one
// every interval, until ctx is canceled. It cycles through event types so
// downstream classification has something to distinguish.
func Run(ctx context.Context, out chan<- event.Event, interval time.Duration) {
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
			select {
			case out <- evt:
			case <-ctx.Done():
				return
			}
		}
	}
}
