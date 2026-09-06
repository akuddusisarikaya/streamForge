// Package ingestion holds the buffer that receives events before workers
// consume them. In later steps this is where backpressure/drop behavior lives.
package ingestion

import "streamforge/internal/event"

// NewBuffer creates the channel-based buffer events flow through.
// capacity is deliberately small and configurable so backpressure can be
// observed once the load-testing step is added.
func NewBuffer(capacity int) chan event.Event {
	return make(chan event.Event, capacity)
}
