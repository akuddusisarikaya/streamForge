// Command streamforge wires together the generator, ingestion buffer,
// worker pool, and metrics reporting (console + /stats HTTP endpoint).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"streamforge/internal/generator"
	"streamforge/internal/ingestion"
	"streamforge/internal/metrics"
	"streamforge/internal/worker"
)

func main() {
	numWorkers := flag.Int("workers", 4, "number of parallel workers")
	bufferSize := flag.Int("buffer", 10, "ingestion buffer capacity")
	interval := flag.Duration("interval", 200*time.Millisecond, "delay between generated events")
	metricsInterval := flag.Duration("metrics-interval", 1*time.Second, "how often to sample and print metrics")
	addr := flag.String("addr", ":8080", "address to serve /stats on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	buffer := ingestion.NewBuffer(*bufferSize)
	counts := worker.NewCounts()
	recorder := metrics.NewRecorder()
	reporter := metrics.NewReporter(recorder, buffer, counts, *metricsInterval)

	var wg sync.WaitGroup
	worker.StartPool(ctx, &wg, buffer.Chan(), *numWorkers, counts, recorder)

	wg.Add(1)
	go func() {
		defer wg.Done()
		generator.Run(ctx, buffer, *interval)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		reporter.Run(ctx)
	}()

	mux := http.NewServeMux()
	mux.Handle("/stats", reporter.Handler())
	server := &http.Server{Addr: *addr, Handler: mux}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("serving /stats on %s\n", *addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("stats server error: %v", err)
		}
	}()

	<-ctx.Done()
	fmt.Println("shutting down...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)

	wg.Wait()

	fmt.Println("final counts by type:")
	for t, n := range counts.Snapshot() {
		fmt.Printf("  %s: %d\n", t, n)
	}

	bs := buffer.Stats()
	fmt.Printf("backpressure: %d blocked sends, %s total blocked time\n", bs.BlockedSends, bs.BlockedTotal)
}
