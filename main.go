// Command streamforge wires together the generator, ingestion buffer, and
// worker for the walking-skeleton flow: one event, end to end.
package main

import (
	"streamforge/internal/generator"
	"streamforge/internal/ingestion"
	"streamforge/internal/worker"
)

func main() {
	buffer := ingestion.NewBuffer(10)

	generator.Generate(buffer)
	worker.Process(buffer)
}
