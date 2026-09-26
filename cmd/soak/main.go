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
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
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

type soakConfig struct {
	URL          string
	AppID        string
	Sessions     int
	Duration     time.Duration
	TxInterval   time.Duration
	GlobalTxRate float64
	Attr         string
	RampUp       time.Duration
	Settle       time.Duration
	PprofAddr    string
	MaxP99Lag    time.Duration
	EventsPath   string
	OutDir       string
	Quiescence   time.Duration
	// SDKVersion, when set, is sent as the init versions map's
	// @instantdb/core so sessions negotiate that SDK's feature gates (e.g.
	// delta-refresh at >= 0.23.0). Empty keeps the historical init frame.
	SDKVersion string
}

// initSDKVersion is the negotiated SDK version runSoak publishes to the
// session dialer before any session starts (see soakConfig.SDKVersion).
var initSDKVersion string

type soakDeps struct {
	Clock       Clock
	DialSession func(ctx context.Context, logger *slog.Logger, rawURL, appID string, id int,
		attr *string, txInterval *time.Duration,
		refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
		ledger *TransactionLedger, quiescence time.Duration) error
	StartPprof func(addr string) (io.Closer, error)
	Evidence   EvidenceDeps
	Stdout     io.Writer
	Stderr     io.Writer
}

func defaultSoakDeps() soakDeps {
	return soakDeps{
		Clock:       realClock{},
		DialSession: runSession,
		StartPprof:  defaultStartPprof,
		Evidence:    defaultEvidenceDeps(),
		Stdout:      os.Stdout,
		Stderr:      os.Stderr,
	}
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func defaultStartPprof(addr string) (io.Closer, error) {
	switch strings.ToLower(strings.TrimSpace(addr)) {
	case "", "none", "disabled":
		return nil, errors.New("pprof endpoint is required; disabling pprof is not permitted")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("pprof listen on %s failed: %w", addr, err)
	}
	server := &http.Server{Handler: nil}
	go func() {
		_ = server.Serve(ln)
	}()
	return server, nil
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func parseFlags(args []string) (soakConfig, error) {
	fs := flag.NewFlagSet("soak", flag.ContinueOnError)
	url := fs.String("url", envOr("SOAK_URL", "ws://localhost:8888/runtime/session"), "session ws url")
	appID := fs.String("app", os.Getenv("SOAK_APP"), "app id")
	n := fs.Int("sessions", 5000, "concurrent sessions")
	dur := fs.Duration("duration", 30*time.Minute, "soak duration")
	txRate := fs.Duration("tx-interval", 2*time.Second, "per-session transact interval")
	globalRate := fs.Float64("global-tx-rate", 8, "aggregate transacts/sec across all sessions")
	attr := fs.String("attr", os.Getenv("SOAK_ATTR"), "attr uuid (uuid string) to write")
	rampUp := fs.Duration("ramp", 30*time.Second, "dial ramp window")
	settle := fs.Duration("settle", 30*time.Second, "post-ramp settle window before writes")
	pprofAddr := fs.String("pprof", "127.0.0.1:18811", "pprof endpoint")
	maxP99 := fs.Duration("max-p99-lag", 0, "fail when p99 write→refresh delivery lag exceeds this (0=off)")
	eventsPath := fs.String("events", envOr("SOAK_EVENTS", ""), "structured JSONL event output path (use - for stdout)")
	outDir := fs.String("out", envOr("SOAK_OUT", ""), "fresh output directory for soak evidence bundle")
	sdkVersion := fs.String("sdk-version", "", "SDK version sent in init (e.g. 0.23.0 negotiates delta-refresh); empty = historical init")
	quiescence := fs.Duration("quiescence", 10*time.Second, "bounded quiescence window after soak duration")

	if err := fs.Parse(args); err != nil {
		return soakConfig{}, err
	}

	if *n <= 0 || *globalRate <= 0 || *dur <= 0 {
		return soakConfig{}, fmt.Errorf("sessions, global-tx-rate, and duration must be positive")
	}
	if err := benchharness.ValidateLoopbackURL(*url); err != nil {
		return soakConfig{}, fmt.Errorf("refusing non-loopback target: %w", err)
	}
	if strings.TrimSpace(*pprofAddr) == "" || strings.EqualFold(strings.TrimSpace(*pprofAddr), "none") || strings.EqualFold(strings.TrimSpace(*pprofAddr), "disabled") {
		return soakConfig{}, errors.New("pprof endpoint is required; disabling pprof is not permitted")
	}

	return soakConfig{
		URL:          *url,
		AppID:        *appID,
		Sessions:     *n,
		Duration:     *dur,
		TxInterval:   *txRate,
		GlobalTxRate: *globalRate,
		Attr:         *attr,
		RampUp:       *rampUp,
		Settle:       *settle,
		PprofAddr:    *pprofAddr,
		MaxP99Lag:    *maxP99,
		EventsPath:   *eventsPath,
		OutDir:       *outDir,
		Quiescence:   *quiescence,
		SDKVersion:   *sdkVersion,
	}, nil
}

func runSoak(ctx context.Context, cfg soakConfig, deps soakDeps, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}
	initSDKVersion = cfg.SDKVersion
	if deps.DialSession == nil {
		deps.DialSession = runSession
	}
	if deps.StartPprof == nil {
		deps.StartPprof = defaultStartPprof
	}

	// EV-002: Required pprof startup failure must affect exit/eligibility.
	pprofCloser, err := deps.StartPprof(cfg.PprofAddr)
	if err != nil {
		logger.Error("soak FAILED: pprof startup failed", "error", err)
		return fmt.Errorf("pprof startup failed: %w", err)
	}
	defer func() {
		if pprofCloser != nil {
			_ = pprofCloser.Close()
		}
	}()

	// EV-002: Mandatory soak evidence and atomic output.
	evCfg := EvidenceConfig{
		EventsPath: cfg.EventsPath,
		OutDir:     cfg.OutDir,
		AppID:      cfg.AppID,
		TargetURL:  cfg.URL,
	}
	evMgr, err := NewEvidenceManager(evCfg, deps.Evidence)
	if err != nil {
		logger.Error("soak FAILED: evidence initialization failed", "error", err)
		return fmt.Errorf("evidence initialization failed: %w", err)
	}
	defer evMgr.Abort()

	if emitErr := evMgr.Emit("run_started", map[string]any{
		"url": cfg.URL, "sessions": cfg.Sessions, "duration": cfg.Duration.String(),
		"global_tx_rate": cfg.GlobalTxRate, "ramp": cfg.RampUp.String(), "settle": cfg.Settle.String(),
		"workload": "historical-soak", "quiescence": cfg.Quiescence.String(),
	}); emitErr != nil {
		logger.Error("soak FAILED: cannot emit run_started event", "error", emitErr)
		return fmt.Errorf("emit run_started: %w", emitErr)
	}

	runUntil := time.Now().Add(cfg.Duration)
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

	ledger := NewTransactionLedger(deps.Clock)

	// Writes start only after a settle window, then flow at the global rate.
	writeGate := make(chan struct{})
	go func() {
		if cfg.GlobalTxRate <= 0 {
			_ = evMgr.Emit("scheduler_error", map[string]any{"error": "global tx rate must be positive"})
			return
		}
		if cfg.Settle < 0 {
			_ = evMgr.Emit("scheduler_error", map[string]any{"error": "settle duration must not be negative"})
			return
		}
		start := time.Now().Add(cfg.RampUp + cfg.Settle)
		scheduler, err := benchharness.NewBlockingScheduler(cfg.GlobalTxRate, start)
		if err != nil {
			_ = evMgr.Emit("scheduler_error", map[string]any{"error": err.Error()})
			return
		}
		count := int(math.Ceil(runUntil.Sub(start).Seconds() * cfg.GlobalTxRate))
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
		_ = evMgr.Emit("scheduler_finished", map[string]any{"scheduled": len(scheduler.Slips()), "slip_samples": len(scheduler.Slips()), "error": errorString(err)})
	}()

	rampStep := cfg.RampUp / time.Duration(cfg.Sessions)
	for i := 0; i < cfg.Sessions; i++ {
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
				sess = deps.DialSession(deadline, logger, cfg.URL, cfg.AppID, id,
					&cfg.Attr, &cfg.TxInterval, &refreshes, &transacts, &connects, writeGate, ledger, cfg.Quiescence)
				if sess == nil {
					return
				}
				logger.Warn("soak session ended with error; redialing", "session", id, "attempt", attempt+1, "err", sess)
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
	defer ticker.Stop()
	lastR, lastT, lastC := int64(0), int64(0), int64(0)
loop:
	for {
		select {
		case <-deadline.Done():
			break loop
		case <-ticker.C:
			r, t, c := refreshes.Load(), transacts.Load(), connects.Load()
			mu.Lock()
			dCount := len(dropped)
			mu.Unlock()
			logger.Info("soak", "sessions", c, "refreshes", r, "transacts", t,
				"r/s", (r-lastR)/10, "t/s", (t-lastT)/10, "c/s", (c-lastC)/10,
				"dropped", dCount, "unresolved", ledger.UnresolvedCount())
			lastR, lastT, lastC = r, t, c
			_ = evMgr.Emit("progress", map[string]any{
				"sessions": c, "refreshes": r, "transacts": t, "dropped": dCount,
				"unresolved": ledger.UnresolvedCount(),
			})
		}
	}
	cancel()
	wg.Wait()

	// EV-001d: Bounded quiescence must resolve all submitted transactions.
	if qErr := ledger.QuiesceAll(context.Background(), cfg.Quiescence); qErr != nil {
		mu.Lock()
		dropped = append(dropped, qErr)
		mu.Unlock()
	}

	workloadManifest := WorkloadManifest{
		Sessions:     cfg.Sessions,
		Duration:     cfg.Duration.String(),
		GlobalTxRate: cfg.GlobalTxRate,
		TxInterval:   cfg.TxInterval.String(),
		RampUp:       cfg.RampUp.String(),
		Settle:       cfg.Settle.String(),
		Quiescence:   cfg.Quiescence.String(),
		MaxP99Lag:    cfg.MaxP99Lag.String(),
		SDKVersion:   cfg.SDKVersion,
	}

	summaryManifest := SummaryManifest{
		Connects:   connects.Load(),
		Transacts:  transacts.Load(),
		Refreshes:  refreshes.Load(),
		Dropped:    len(dropped),
		Unresolved: ledger.UnresolvedCount(),
	}

	// Check failure conditions:
	var failureReason string
	if len(dropped) > 0 {
		failureReason = fmt.Sprintf("dropped refreshes/protocol errors (count: %d, first: %v)", len(dropped), dropped[0])
	} else if transacts.Load() == 0 {
		failureReason = "zero writes submitted"
	}

	if failureReason != "" {
		summaryManifest.Success = false
		summaryManifest.FailureError = failureReason
		_ = evMgr.Emit("run_finished", map[string]any{
			"status": "failed", "dropped": len(dropped), "refreshes": refreshes.Load(),
			"transacts": transacts.Load(), "unresolved": ledger.UnresolvedCount(),
			"reason": failureReason,
		})
		_ = evMgr.FlushAndClose()
		logger.Error("soak FAILED", "reason", failureReason)
		return fmt.Errorf("soak failed: %s", failureReason)
	}

	// Delivery-lag budget: p50/p99/max of write→refresh samples from the deterministic ledger.
	sorted := ledger.LagSamples()
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	p50, p99 := percentile(sorted, 0.50), percentile(sorted, 0.99)
	var max time.Duration
	if n := len(sorted); n > 0 {
		max = sorted[n-1]
	}
	summaryManifest.LagSamples = len(sorted)
	summaryManifest.LagP50 = p50.String()
	summaryManifest.LagP99 = p99.String()
	summaryManifest.LagMax = max.String()

	logger.Info("transport lag diagnostic", "samples", len(sorted), "p50", p50.Round(time.Millisecond),
		"p99", p99.Round(time.Millisecond), "max", max.Round(time.Millisecond))
	_ = evMgr.Emit("lag_diagnostic", map[string]any{
		"metric_kind": "transport_diagnostic", "semantic_claim": false, "samples": len(sorted),
		"p50": p50.String(), "p99": p99.String(), "max": max.String(),
	})

	if cfg.MaxP99Lag > 0 && p99 > cfg.MaxP99Lag {
		failureReason = fmt.Sprintf("p99 delivery lag over budget (%v > %v)", p99, cfg.MaxP99Lag)
		summaryManifest.Success = false
		summaryManifest.FailureError = failureReason
		_ = evMgr.Emit("run_finished", map[string]any{
			"status": "failed", "reason": "p99_budget", "p99": p99.String(), "budget": cfg.MaxP99Lag.String(),
		})
		_ = evMgr.FlushAndClose()
		logger.Error("soak FAILED: p99 delivery lag over budget", "p99", p99, "budget", cfg.MaxP99Lag)
		return fmt.Errorf("soak failed: %s", failureReason)
	}

	if len(sorted) == 0 {
		failureReason = "no delivery samples"
		summaryManifest.Success = false
		summaryManifest.FailureError = failureReason
		_ = evMgr.Emit("run_finished", map[string]any{"status": "failed", "reason": "no_delivery_samples"})
		_ = evMgr.FlushAndClose()
		logger.Error("soak FAILED: no delivery samples")
		return fmt.Errorf("soak failed: %s", failureReason)
	}

	// EV-002: Check if any event writing errors occurred during the run.
	if evErr := evMgr.FirstError(); evErr != nil {
		failureReason = fmt.Sprintf("mandatory event write error: %v", evErr)
		summaryManifest.Success = false
		summaryManifest.FailureError = failureReason
		_ = evMgr.FlushAndClose()
		logger.Error("soak FAILED: mandatory event write error", "error", evErr)
		return fmt.Errorf("soak failed: %s", failureReason)
	}

	summaryManifest.Success = true
	logger.Info("soak PASSED", "sessions", connects.Load(), "refreshes", refreshes.Load(),
		"transacts", transacts.Load())
	_ = evMgr.Emit("run_finished", map[string]any{
		"status": "passed", "sessions": connects.Load(), "refreshes": refreshes.Load(),
		"transacts": transacts.Load(), "lag_samples": len(sorted),
	})

	// EV-002: Flush and close staged events file.
	if flushErr := evMgr.FlushAndClose(); flushErr != nil {
		logger.Error("soak FAILED: flush/close event file error", "error", flushErr)
		return fmt.Errorf("flush/close event file: %w", flushErr)
	}

	// EV-002: Atomic publication with completeness manifest.
	if _, pubErr := evMgr.Publish(summaryManifest, workloadManifest, cfg.PprofAddr); pubErr != nil {
		logger.Error("soak FAILED: publish evidence bundle error", "error", pubErr)
		return fmt.Errorf("publish evidence bundle: %w", pubErr)
	}

	return nil
}

func runMain(args []string) int {
	cfg, err := parseFlags(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "soak flag error:", err)
		return 2
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	err = runSoak(ctx, cfg, defaultSoakDeps(), logger)
	if err != nil {
		logger.Error("soak run failed", "error", err)
		return 1
	}
	return 0
}

func main() {
	code := runMain(os.Args[1:])
	if code != 0 {
		os.Exit(code)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sessionConn abstracts websocket communication for production and injected testing.
type sessionConn interface {
	Read(ctx context.Context) (websocket.MessageType, []byte, error)
	Write(ctx context.Context, typ websocket.MessageType, p []byte) error
	Close(status websocket.StatusCode, reason string) error
	SetReadLimit(limit int64)
}

// runSession drives one client until deadline using a real WebSocket connection.
func runSession(ctx context.Context, _ *slog.Logger, rawURL, appID string, id int,
	attr *string, txInterval *time.Duration,
	refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
	ledger *TransactionLedger, quiescence time.Duration,
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
	if connects != nil {
		connects.Add(1)
	}

	return driveSessionWithConn(ctx, conn, ledger, realClock{}, id, appID, attr, txInterval,
		refreshes, transacts, connects, writeGate, quiescence)
}

// driveSessionWithConn runs the protocol lifecycle for one session using an abstract connection.
// This allows full deterministic testing with injected frames and clocks without real sockets.
func driveSessionWithConn(ctx context.Context, conn sessionConn,
	ledger *TransactionLedger, clock Clock, id int, appID string,
	attr *string, txInterval *time.Duration,
	refreshes, transacts, _ *atomic.Int64, writeGate chan struct{},
	quiescence time.Duration,
) error {
	if ledger == nil {
		ledger = NewTransactionLedger(clock)
	}
	if clock == nil {
		clock = realClock{}
	}
	if quiescence <= 0 {
		quiescence = 10 * time.Second
	}
	var (
		attrVal       = "default"
		txIntervalVal = 2 * time.Second
	)
	if attr != nil && *attr != "" {
		attrVal = *attr
	}
	if txInterval != nil && *txInterval > 0 {
		txIntervalVal = *txInterval
	}
	if appID == "" {
		appID = "app"
	}

	readerCtx, cancelReader := context.WithCancel(context.Background())
	defer cancelReader()

	readErr := make(chan error, 1) // buffered; only first writer wins
	send := func(v any) error {
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return conn.Write(ctx, websocket.MessageText, b)
	}

	var (
		mu            sync.Mutex
		gotSnapshot   bool
		lastRefreshAt = clock.Now()
	)

	initOK := make(chan struct{})
	readerDone := make(chan struct{})

	go func() {
		defer close(readerDone)
		for {
			_, data, err := conn.Read(readerCtx)
			if err != nil {
				if readerCtx.Err() != nil {
					return
				}
				select {
				case readErr <- err:
				default:
				}
				return
			}
			var f struct {
				Op            string          `json:"op"`
				ClientEventID string          `json:"client-event-id"`
				TxID          json.RawMessage `json:"tx-id"`
				ProcessedTxID json.RawMessage `json:"processed-tx-id"`
				Result        json.RawMessage `json:"result"`
				Status        json.RawMessage `json:"status"`
				Type          string          `json:"type"`
				Message       string          `json:"message"`
			}
			if json.Unmarshal(data, &f) != nil {
				protocolErr := ledger.RecordError("", "bad frame JSON")
				select {
				case readErr <- protocolErr:
				default:
				}
				return
			}
			switch f.Op {
			case "init-ok":
				select {
				case <-initOK:
				default:
					close(initOK)
				}
			case "add-query-ok":
				if len(f.Result) > 0 {
					mu.Lock()
					gotSnapshot = true
					lastRefreshAt = clock.Now()
					mu.Unlock()
					if refreshes != nil {
						refreshes.Add(1)
					}
				}
			case "transact-ok":
				txID := rawToString(f.TxID)
				if _, err := ledger.RecordAck(f.ClientEventID, txID); err != nil {
					select {
					case readErr <- err:
					default:
					}
					return
				}
			case "refresh-ok", "refresh-ok-delta":
				mu.Lock()
				gotSnapshot = true
				lastRefreshAt = clock.Now()
				mu.Unlock()
				if refreshes != nil {
					refreshes.Add(1)
				}
				pid := rawToString(f.ProcessedTxID)
				if _, err := ledger.RecordRefresh(pid, id); err != nil {
					select {
					case readErr <- err:
					default:
					}
					return
				}
			case "error", "protocol-error":
				reason := f.Message
				if reason == "" {
					reason = f.Type
				}
				if reason == "" {
					reason = "protocol error frame"
				}
				terminalErr := ledger.RecordError(f.ClientEventID, reason)
				select {
				case readErr <- terminalErr:
				default:
				}
				return
			default:
				unknownOpErr := ledger.RecordError(f.ClientEventID, fmt.Sprintf("unrecognized op: %s", f.Op))
				select {
				case readErr <- unknownOpErr:
				default:
				}
				return
			}
		}
	}()

	initFrame := map[string]any{"op": "init", "app-id": appID}
	if initSDKVersion != "" {
		initFrame["versions"] = map[string]string{"@instantdb/core": initSDKVersion}
	}
	if err := send(initFrame); err != nil {
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

	// Subscribe AFTER init-ok.
	if err := send(map[string]any{
		"op":              "add-query",
		"q":               map[string]any{"todos": map[string]any{}},
		"client-event-id": fmt.Sprintf("q-%d", id),
	}); err != nil {
		return err
	}

	tick := time.NewTicker(txIntervalVal)
	defer tick.Stop()
	counter := int64(0)
	var firstWriteAt time.Time
	snapDeadline := clock.Now().Add(20 * time.Second)

writeLoop:
	for {
		select {
		case <-ctx.Done():
			break writeLoop
		case err := <-readErr:
			return err
		case <-tick.C:
			mu.Lock()
			snap, lastAt := gotSnapshot, lastRefreshAt
			mu.Unlock()
			if !snap {
				if clock.Now().After(snapDeadline) {
					return fmt.Errorf("no snapshot within deadline")
				}
				continue
			}
			if pending := ledger.UnresolvedCountForSession(id); pending > 50 {
				return fmt.Errorf("stalled: %d transacts without ok", pending)
			}
			// The stall window starts at the later of the last refresh and
			// this session's first write: ramp/settle are write-free, so the
			// snapshot can be minutes old when writing begins.
			if counter > 0 {
				since := lastAt
				if firstWriteAt.After(since) {
					since = firstWriteAt
				}
				if clock.Now().Sub(since) > 20*time.Second {
					return fmt.Errorf("refresh stream stalled")
				}
			} else {
				firstWriteAt = clock.Now()
			}
			counter++
			sequence := ledger.NextSequence(id)
			eid := fmt.Sprintf("t-%d-%d", id, sequence)
			entity := fmt.Sprintf("%08x-0000-4000-8000-%012x", id, sequence)

			if writeGate != nil {
				select {
				case <-writeGate: // one global write token
				case <-ctx.Done():
					break writeLoop
				case err := <-readErr:
					return err
				}
			}

			// EV-001a: Record submit in deterministic ledger
			if _, err := ledger.RecordSubmit(eid, id); err != nil {
				return err
			}

			if err := send(map[string]any{
				"op": "transact",
				"tx-steps": []any{
					[]any{"add-triple", entity, attrVal, fmt.Sprintf("soak %d %d", id, counter)},
				},
				"client-event-id": eid,
			}); err != nil {
				_ = ledger.RecordError(eid, err.Error())
				return err
			}
			if transacts != nil {
				transacts.Add(1)
			}
		}
	}

	// EV-001d: Bounded quiescence window: wait for all in-flight transacts for this session to resolve.
	quiesceCtx, qcancel := context.WithTimeout(context.Background(), quiescence)
	defer qcancel()

	quiesceCh := make(chan error, 1)
	go func() {
		quiesceCh <- ledger.QuiesceSession(quiesceCtx, id, quiescence)
	}()

	select {
	case err := <-readErr:
		return err
	case err := <-quiesceCh:
		if err != nil {
			return err
		}
		return nil
	case <-quiesceCtx.Done():
		return fmt.Errorf("quiescence timeout: %d unresolved transactions remaining", ledger.UnresolvedCountForSession(id))
	}
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
