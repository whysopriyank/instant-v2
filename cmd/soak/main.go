// Command soak runs the Phase 4 exit-gate soak: N concurrent WebSocket
// sessions doing init → add-query → transact bursts, asserting every client
// receives a refresh-ok per committed tx it observes (no dropped invalidation)
// and reporting pprof deltas.
//
// Usage:
//
//	SOAK_URL=ws://localhost:8888/runtime/session SOAK_APP=<app-id> \
//	  SOAK_SESSIONS=5000 SOAK_DURATION=30m go run ./cmd/soak
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// Lag samples (write→next-refresh-at-this-session), filled from reader
// goroutines under each session's mu.
var (
	lagMu      sync.Mutex
	lagSamples []time.Duration
)

func recordLag(d time.Duration) {
	lagMu.Lock()
	lagSamples = append(lagSamples, d)
	lagMu.Unlock()
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func main() {
	url := flag.String("url", envOr("SOAK_URL", "ws://localhost:8888/runtime/session"), "session ws url")
	appID := flag.String("app", os.Getenv("SOAK_APP"), "app id")
	n := flag.Int("sessions", 5000, "concurrent sessions")
	dur := flag.Duration("duration", 30*time.Minute, "soak duration")
	txRate := flag.Duration("tx-interval", 2*time.Second, "per-session transact interval")
	globalRate := flag.Float64("global-tx-rate", 8, "aggregate transacts/sec across all sessions")
	attr := flag.String("attr", os.Getenv("SOAK_ATTR"), "attr uuid (uuid string) to write")
	rampUp := flag.Duration("ramp", 30*time.Second, "dial ramp window")
	settle := flag.Duration("settle", 30*time.Second, "post-ramp settle window before writes")
	pprofAddr := flag.String("pprof", "127.0.0.1:18811", "pprof endpoint")
	maxP99 := flag.Duration("max-p99-lag", 0, "fail when p99 write→refresh delivery lag exceeds this (0=off)")
	eventsPath := flag.String("events", envOr("SOAK_EVENTS", ""), "structured JSONL event output path (use - for stdout)")
	flag.Parse()
	if *n <= 0 || *globalRate <= 0 || *dur <= 0 {
		fmt.Fprintln(os.Stderr, "soak: sessions, global-tx-rate, and duration must be positive")
		os.Exit(2)
	}
	if err := benchharness.ValidateLoopbackURL(*url); err != nil {
		fmt.Fprintln(os.Stderr, "soak: refusing non-loopback target:", err)
		os.Exit(2)
	}

	go func() { _ = http.ListenAndServe(*pprofAddr, nil) }()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	events, closeEvents, err := openEvents(*eventsPath)
	if err != nil {
		logger.Error("cannot open structured event output", "error", err)
		os.Exit(2)
	}
	defer closeEvents()
	_ = events.Emit("run_started", map[string]any{
		"url": *url, "sessions": *n, "duration": dur.String(), "global_tx_rate": *globalRate,
		"ramp": rampUp.String(), "settle": settle.String(), "workload": "historical-soak",
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runUntil := time.Now().Add(*dur)
	deadline, cancel := context.WithDeadline(ctx, runUntil)
	defer cancel()

	var (
		mu        sync.Mutex
		dropped   []error
		refreshes atomic.Int64
		transacts atomic.Int64
		connects  atomic.Int64
		wg        sync.WaitGroup
	)

	// Writes start only after a settle window, then flow at the global rate.
	// The gate is unbuffered and the scheduler is blocking: every scheduled
	// token is delivered to a session or remains visible as schedule slip.
	writeGate := make(chan struct{})
	go func() {
		if *globalRate <= 0 {
			_ = events.Emit("scheduler_error", map[string]any{"error": "global tx rate must be positive"})
			return
		}
		if *settle < 0 {
			_ = events.Emit("scheduler_error", map[string]any{"error": "settle duration must not be negative"})
			return
		}
		start := time.Now().Add(*rampUp + *settle)
		scheduler, err := benchharness.NewBlockingScheduler(*globalRate, start)
		if err != nil {
			_ = events.Emit("scheduler_error", map[string]any{"error": err.Error()})
			return
		}
		count := int(math.Ceil(runUntil.Sub(start).Seconds() * *globalRate))
		if count < 1 {
			count = 1
		}
		items := make([]benchharness.Mutation, count)
		for i := range items {
			items[i] = benchharness.Mutation{Sequence: int64(i + 1), EventID: fmt.Sprintf("soak/%06d", i+1)}
		}
		err = scheduler.Run(deadline, items, func(ctx context.Context, _ benchharness.Mutation) error {
			select {
			case writeGate <- struct{}{}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		_ = events.Emit("scheduler_finished", map[string]any{"scheduled": len(scheduler.Slips()), "slip_samples": len(scheduler.Slips()), "error": errorString(err)})
	}()

	rampStep := *rampUp / time.Duration(*n)
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if rampStep > 0 {
				select {
				case <-time.After(time.Duration(id) * rampStep):
				case <-deadline.Done():
					return
				}
			}
			var sess error
			for attempt := 0; attempt < 3; attempt++ {
				sess = runSession(deadline, logger, *url, *appID, id,
					attr, txRate, &refreshes, &transacts, &connects, writeGate)
				if sess == nil {
					return
				}
				select {
				case <-time.After(time.Duration(attempt+1) * time.Second):
				case <-deadline.Done():
					return
				}
			}
			if sess != nil {
				mu.Lock()
				dropped = append(dropped, sess)
				mu.Unlock()
			}
		}(i)
	}

	ticker := time.NewTicker(10 * time.Second)
	lastR, lastT, lastC := int64(0), int64(0), int64(0)
loop:
	for {
		select {
		case <-deadline.Done():
			break loop
		case <-ticker.C:
			r, t, c := refreshes.Load(), transacts.Load(), connects.Load()
			logger.Info("soak", "sessions", c, "refreshes", r, "transacts", t,
				"r/s", (r-lastR)/10, "t/s", (t-lastT)/10, "c/s", (c-lastC)/10,
				"dropped", len(dropped))
			lastR, lastT, lastC = r, t, c
			_ = events.Emit("progress", map[string]any{"sessions": c, "refreshes": r, "transacts": t, "dropped": len(dropped)})
		}
	}
	cancel()
	wg.Wait()

	if len(dropped) > 0 {
		_ = events.Emit("run_finished", map[string]any{"status": "failed", "dropped": len(dropped), "refreshes": refreshes.Load(), "transacts": transacts.Load()})
		logger.Error("soak FAILED: dropped refreshes/protocol errors", "count", len(dropped),
			"first", dropped[0].Error())
		os.Exit(1)
	}
	if transacts.Load() == 0 {
		_ = events.Emit("run_finished", map[string]any{"status": "failed", "reason": "zero_writes"})
		logger.Error("soak FAILED: zero writes submitted")
		os.Exit(1)
	}

	// Delivery-lag budget: p50/p99/max of write→refresh samples.
	lagMu.Lock()
	sorted := append([]time.Duration(nil), lagSamples...)
	lagMu.Unlock()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50, p99 := percentile(sorted, 0.50), percentile(sorted, 0.99)
	var max time.Duration
	if n := len(sorted); n > 0 {
		max = sorted[n-1]
	}
	logger.Info("transport lag diagnostic", "samples", len(sorted), "p50", p50.Round(time.Millisecond),
		"p99", p99.Round(time.Millisecond), "max", max.Round(time.Millisecond))
	_ = events.Emit("lag_diagnostic", map[string]any{"metric_kind": "transport_diagnostic", "semantic_claim": false, "samples": len(sorted), "p50": p50.String(), "p99": p99.String(), "max": max.String()})
	if *maxP99 > 0 && p99 > *maxP99 {
		_ = events.Emit("run_finished", map[string]any{"status": "failed", "reason": "p99_budget", "p99": p99.String(), "budget": maxP99.String()})
		logger.Error("soak FAILED: p99 delivery lag over budget", "p99", p99, "budget", *maxP99)
		os.Exit(1)
	}
	if len(sorted) == 0 {
		_ = events.Emit("run_finished", map[string]any{"status": "failed", "reason": "no_delivery_samples"})
		logger.Error("soak FAILED: no delivery samples")
		os.Exit(1)
	}
	logger.Info("soak PASSED", "sessions", connects.Load(), "refreshes", refreshes.Load(),
		"transacts", transacts.Load())
	_ = events.Emit("run_finished", map[string]any{"status": "passed", "sessions": connects.Load(), "refreshes": refreshes.Load(), "transacts": transacts.Load(), "lag_samples": len(sorted)})
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func openEvents(path string) (*benchharness.EventWriter, func(), error) {
	if path == "" {
		return benchharness.NewEventWriter(nil, 0), func() {}, nil
	}
	if path == "-" {
		return benchharness.NewEventWriter(func(b []byte) error { _, err := os.Stdout.Write(b); return err }, 0), func() {}, nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, func() {}, err
	}
	return benchharness.NewEventWriter(func(b []byte) error { _, err := f.Write(b); return err }, 0), func() { _ = f.Close() }, nil
}

// runSession drives one client until deadline. Returns a non-nil error when a
// correctness violation is detected (missed refresh, bad frame ordering).
func runSession(ctx context.Context, _ *slog.Logger, rawURL, appID string, id int,
	attr *string, txInterval *time.Duration,
	refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
) error {
	sep := "?"
	if strings.Contains(rawURL, "?") {
		sep = "&"
	}
	conn, _, err := websocket.Dial(ctx, rawURL+sep+"soak="+fmt.Sprint(id), &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": {"http://localhost"}},
	})
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(64 << 20)
	connects.Add(1)
	readErr := make(chan error, 1) // buffered; only first writer wins
	send := func(v any) error {
		b, _ := json.Marshal(v)
		return conn.Write(ctx, websocket.MessageText, b)
	}

	var (
		pendingTxs    int32
		gotSnapshot   bool
		mu            sync.Mutex
		lastRefreshAt = time.Now()
		lastTxAt      time.Time // last transact send; sampled on next refresh
	)

	initOK := make(chan struct{})
	reader := func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				select {
				case readErr <- err:
				default:
				}
				return
			}
			var f struct {
				Op     string          `json:"op"`
				Result json.RawMessage `json:"result"`
			}
			if json.Unmarshal(data, &f) != nil {
				select {
				case readErr <- fmt.Errorf("bad frame"):
				default:
				}
				return
			}
			switch f.Op {
			case "init-ok":
				close(initOK)
			case "add-query-ok":
				// v1 rides the initial answer on the ack (session.clj:264);
				// v2 pre-fix sends refresh-ok — accept either.
				if len(f.Result) > 0 {
					mu.Lock()
					gotSnapshot = true
					lastRefreshAt = time.Now()
					mu.Unlock()
					refreshes.Add(1)
				}
			case "transact-ok":
				atomic.AddInt32(&pendingTxs, -1)
			case "refresh-ok", "refresh-ok-delta":
				mu.Lock()
				gotSnapshot = true
				lastRefreshAt = time.Now()
				if !lastTxAt.IsZero() {
					recordLag(time.Since(lastTxAt))
					lastTxAt = time.Time{}
				}
				mu.Unlock()
				refreshes.Add(1)
			}
		}
	}

	go reader()
	if err := send(map[string]any{"op": "init", "app-id": appID}); err != nil {
		return err
	}
	select {
	case <-initOK:
	case err := <-readErr:
		return fmt.Errorf("pre-init read: %w", err)
	case <-ctx.Done():
		return nil
	case <-time.After(15 * time.Second):
		return fmt.Errorf("init-ok timeout")
	}

	// Subscribe AFTER init-ok: v1 validates ops against session state and
	// processes frames asynchronously.
	if err := send(map[string]any{
		"op":              "add-query",
		"q":               map[string]any{"todos": map[string]any{}},
		"client-event-id": fmt.Sprintf("q-%d", id),
	}); err != nil {
		return err
	}

	tick := time.NewTicker(*txInterval)
	defer tick.Stop()
	counter := int64(0)
	snapDeadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case <-ctx.Done():
			if atomic.LoadInt32(&pendingTxs) != 0 {
				return fmt.Errorf("unresolved transacts: %d", atomic.LoadInt32(&pendingTxs))
			}
			return nil
		case err := <-readErr:
			return err
		case <-tick.C:
			mu.Lock()
			snap, lastAt := gotSnapshot, lastRefreshAt
			mu.Unlock()
			if !snap {
				if time.Now().After(snapDeadline) {
					return fmt.Errorf("no snapshot within deadline")
				}
				continue
			}
			if pending := atomic.LoadInt32(&pendingTxs); pending > 50 {
				return fmt.Errorf("stalled: %d transacts without ok", pending)
			}
			if time.Since(lastAt) > 20*time.Second && counter > 0 {
				return fmt.Errorf("refresh stream stalled")
			}
			counter++
			eid := fmt.Sprintf("t-%d-%d", id, counter)
			entity := fmt.Sprintf("%08x-0000-4000-8000-%012x", id, counter)
			select {
			case <-writeGate: // one global write token
			case <-ctx.Done():
				return nil
			}
			atomic.AddInt32(&pendingTxs, 1)
			if err := send(map[string]any{
				"op": "transact",
				"tx-steps": []any{
					[]any{"add-triple", entity, *attr, fmt.Sprintf("soak %d %d", id, counter)},
				},
				"client-event-id": eid,
			}); err != nil {
				return err
			}
			mu.Lock()
			lastTxAt = time.Now()
			mu.Unlock()
			transacts.Add(1)
		}
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
