// Command streamforge wires together the generator, ingestion buffer, and
// worker pool, and prints a per-type count summary on shutdown.
package main

import (
	"context"
	"flag"
	"fmt"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"streamforge/internal/generator"
	"streamforge/internal/ingestion"
	"streamforge/internal/worker"
)

func main() {
	numWorkers := flag.Int("workers", 4, "number of parallel workers")
	bufferSize := flag.Int("buffer", 10, "ingestion buffer capacity")
	interval := flag.Duration("interval", 200*time.Millisecond, "delay between generated events")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	buffer := ingestion.NewBuffer(*bufferSize)
	counts := worker.NewCounts()

	var wg sync.WaitGroup
	worker.StartPool(ctx, &wg, buffer.Chan(), *numWorkers, counts)

	wg.Add(1)
	go func() {
		defer wg.Done()
		generator.Run(ctx, buffer, *interval)
	}()

	<-ctx.Done()
	fmt.Println("shutting down...")
	wg.Wait()

	fmt.Println("final counts by type:")
	for t, n := range counts.Snapshot() {
		fmt.Printf("  %s: %d\n", t, n)
	}

	bs := buffer.Stats()
	fmt.Printf("backpressure: %d blocked sends, %s total blocked time\n", bs.BlockedSends, bs.BlockedTotal)
}
