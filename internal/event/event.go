// Package event defines the data model that flows through StreamForge.
package event

import "time"

// Type identifies the category of an event.
type Type string

const (
	TypeError      Type = "error"
	TypePayment    Type = "payment"
	TypeUserAction Type = "user-action"
)

// Event is a single unit of data flowing through the pipeline.
type Event struct {
	ID        string
	Type      Type
	Payload   map[string]any
	Timestamp time.Time // set at generation time; used to measure end-to-end latency
}
