// Package generator produces synthetic events for StreamForge.
package generator

import (
	"time"

	"streamforge/internal/event"
)

// Generate produces a single synthetic event and sends it to out.
// This is the walking-skeleton version: one call, one event.
func Generate(out chan<- event.Event) {
	out <- event.Event{
		ID:        "evt-1",
		Type:      event.TypeUserAction,
		Payload:   map[string]any{"action": "click"},
		Timestamp: time.Now(),
	}
}
