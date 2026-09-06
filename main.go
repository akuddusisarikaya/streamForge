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
	"strconv"
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
	rate := flag.Int64("rate", 20, "initial generator rate, in events per second")
	metricsInterval := flag.Duration("metrics-interval", 1*time.Second, "how often to sample and print metrics")
	addr := flag.String("addr", ":8080", "address to serve /stats and /rate on")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	buffer := ingestion.NewBuffer(*bufferSize)
	counts := worker.NewCounts()
	rateController := generator.NewRateController(*rate)
	recorder := metrics.NewRecorder()
	reporter := metrics.NewReporter(recorder, buffer, counts, rateController, *metricsInterval)

	var wg sync.WaitGroup
	worker.StartPool(ctx, &wg, buffer.Chan(), *numWorkers, counts, recorder)

	wg.Add(1)
	go func() {
		defer wg.Done()
		generator.Run(ctx, buffer, rateController)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		reporter.Run(ctx)
	}()

	mux := http.NewServeMux()
	mux.Handle("/stats", reporter.Handler())
	mux.HandleFunc("/rate", rateHandler(rateController))
	server := &http.Server{Addr: *addr, Handler: mux}

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Printf("serving /stats and /rate on %s\n", *addr)
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

// rateHandler serves the generator's current target rate on GET, and
// updates it on POST (?value=N), so load can be ramped up live without
// restarting the process.
func rateHandler(rc *generator.RateController) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprintf(w, `{"rate_per_sec":%d}`, rc.Rate())
		case http.MethodPost:
			n, err := strconv.ParseInt(r.URL.Query().Get("value"), 10, 64)
			if err != nil || n <= 0 {
				http.Error(w, `{"error":"value must be a positive integer"}`, http.StatusBadRequest)
				return
			}
			rc.SetRate(n)
			fmt.Fprintf(w, `{"rate_per_sec":%d}`, n)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	}
}
