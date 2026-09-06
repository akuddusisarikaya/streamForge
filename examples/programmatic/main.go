// Command programmatic demonstrates composing StreamForge's pipeline
// (generator, ingestion buffer, worker pool, metrics recorder) directly in
// Go code, without the CLI flags or the /stats HTTP server from main.go.
// This is the shape you'd use to embed StreamForge in your own program.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"streamforge/internal/generator"
	"streamforge/internal/ingestion"
	"streamforge/internal/metrics"
	"streamforge/internal/worker"
)

func main() {
	// Run the pipeline for a fixed window instead of until SIGINT/SIGTERM -
	// convenient for a one-shot embedded run.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	buffer := ingestion.NewBuffer(20)
	counts := worker.NewCounts()
	rate := generator.NewRateController(50) // events per second
	recorder := metrics.NewRecorder()

	var wg sync.WaitGroup
	worker.StartPool(ctx, &wg, buffer.Chan(), 2, counts, recorder)

	wg.Add(1)
	go func() {
		defer wg.Done()
		generator.Run(ctx, buffer, rate)
	}()

	<-ctx.Done()
	wg.Wait()

	fmt.Println()
	fmt.Println("counts by type:")
	for t, n := range counts.Snapshot() {
		fmt.Printf("  %s: %d\n", t, n)
	}

	bs := buffer.Stats()
	fmt.Printf("backpressure: %d blocked sends, %s total blocked time\n", bs.BlockedSends, bs.BlockedTotal)
}
