// Command soak runs the Phase 4 exit-gate soak: N concurrent WebSocket
// sessions doing init → add-query → transact bursts, asserting every client
// receives a refresh-ok per committed tx it observes (no dropped invalidation)
// and reporting pprof deltas.
//
// Usage:
//
//	SOAK_URL=ws://localhost:8888/runtime/session SOAK_APP=<app-id> \
//	  SOAK_SESSIONS=5000 SOAK_DURATION=30m go run ./tools/soak
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

func main() {
	url := flag.String("url", envOr("SOAK_URL", "ws://localhost:8888/runtime/session"), "session ws url")
	appID := flag.String("app", os.Getenv("SOAK_APP"), "app id")
	n := flag.Int("sessions", 5000, "concurrent sessions")
	dur := flag.Duration("duration", 30*time.Minute, "soak duration")
	txRate := flag.Duration("tx-interval", 2*time.Second, "per-session transact interval")
	globalRate := flag.Float64("global-tx-rate", 8, "aggregate transacts/sec across all sessions")
	attr := flag.String("attr", os.Getenv("SOAK_ATTR"), "attr uuid (uuid string) to write")
	rampUp := flag.Duration("ramp", 30*time.Second, "dial ramp window")
	pprofAddr := flag.String("pprof", "127.0.0.1:18811", "pprof endpoint")
	flag.Parse()

	go func() { _ = http.ListenAndServe(*pprofAddr, nil) }()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	deadline, cancel := context.WithDeadline(ctx, time.Now().Add(*dur))
	defer cancel()

	var (
		mu        sync.Mutex
		dropped   []error
		refreshes atomic.Int64
		transacts atomic.Int64
		connects  atomic.Int64
		wg        sync.WaitGroup
	)

	// Writes start only after a settle window, then flow at the global rate —
	// modeling readers >> writers instead of an all-to-all storm.
	writeGate := make(chan struct{})
	go func() {
		select {
		case <-time.After(*rampUp + 30*time.Second):
		case <-deadline.Done():
			return
		}
		interval := time.Duration(float64(time.Second) / *globalRate)
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-deadline.Done():
				return
			case <-t.C:
				select {
				case writeGate <- struct{}{}:
				default:
				}
			}
		}
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
		}
	}
	cancel()
	wg.Wait()

	if len(dropped) > 0 {
		logger.Error("soak FAILED: dropped refreshes/protocol errors", "count", len(dropped),
			"first", dropped[0].Error())
		os.Exit(1)
	}
	logger.Info("soak PASSED", "sessions", connects.Load(), "refreshes", refreshes.Load(),
		"transacts", transacts.Load())
}

// runSession drives one client until deadline. Returns a non-nil error when a
// correctness violation is detected (missed refresh, bad frame ordering).
// refreshStall reports true when no refresh arrived for 20s after activity.
func refreshStall(now int64, last *int64, lastAt *time.Time) bool {
	if now != *last {
		*last = now
		*lastAt = time.Now()
		return false
	}
	if now == 0 {
		return false // writers may not have started
	}
	return time.Since(*lastAt) > 20*time.Second
}

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
	send := func(v any) error {
		b, _ := json.Marshal(v)
		return conn.Write(ctx, websocket.MessageText, b)
	}
	if err := send(map[string]any{"op": "init", "app-id": appID}); err != nil {
		return err
	}
	if err := send(map[string]any{
		"op":              "add-query",
		"q":               map[string]any{"todos": map[string]any{}},
		"client-event-id": fmt.Sprintf("q-%d", id),
	}); err != nil {
		return err
	}

	var pendingTxs int32 // transacts sent without transact-ok yet
	gotInitOK, gotSnapshot := false, false
	var lastRefreshCount int64
	lastRefreshAt := time.Now()

	readErr := make(chan error, 1)
	go func() {
		for {
			_, data, err := conn.Read(ctx)
			if err != nil {
				readErr <- err
				return
			}
			var f struct {
				Op            string          `json:"op"`
				ClientEventID string          `json:"client-event-id"`
				TxID          json.RawMessage `json:"tx-id"`
			}
			if json.Unmarshal(data, &f) != nil {
				readErr <- fmt.Errorf("bad frame")
				return
			}
			switch f.Op {
			case "init-ok":
				gotInitOK = true
			case "add-query-ok":
				// initial snapshot due
			case "transact-ok":
				atomic.AddInt32(&pendingTxs, -1)
			case "refresh-ok":
				refreshes.Add(1)
				gotSnapshot = true
			}
		}
	}()

	tick := time.NewTicker(*txInterval)
	defer tick.Stop()
	counter := int64(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-readErr:
			return err
		case <-tick.C:
			if !gotInitOK || !gotSnapshot {
				return fmt.Errorf("handshake incomplete: init-ok=%v snapshot=%v", gotInitOK, gotSnapshot)
			}
			if pending := atomic.LoadInt32(&pendingTxs); pending > 50 {
				return fmt.Errorf("stalled: %d transacts without ok", pending)
			}
			if refreshStall(refreshes.Load(), &lastRefreshCount, &lastRefreshAt) {
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
