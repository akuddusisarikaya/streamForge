// Package worker consumes events from the ingestion buffer and processes them.
package worker

import (
	"fmt"

	"streamforge/internal/event"
)

// Process reads a single event from in and prints it.
// Walking-skeleton version: one worker, one event, no classification yet.
func Process(in <-chan event.Event) {
	evt := <-in
	fmt.Printf("processed event: id=%s type=%s payload=%v\n", evt.ID, evt.Type, evt.Payload)
}
