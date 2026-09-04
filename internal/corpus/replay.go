package corpus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/coder/websocket"
)

// ReplayResult retains raw evidence separately from policy-normalized outputs.
type ReplayResult struct {
	Scenario     *Scenario
	Expected     [][]byte
	Collected    [][]byte
	RawCollected []json.RawMessage
	DurationMS   int64
	Err          error
	Delta        string
	Passed       bool
}

const defaultReplayQuiescence = 25 * time.Millisecond

type replayConn interface {
	Write(context.Context, websocket.MessageType, []byte) error
	Read(context.Context) (websocket.MessageType, []byte, error)
	CloseNow() error
}

type replayDialer func(context.Context, string) (replayConn, error)

type replayOptions struct {
	canonical  CanonicalOptions
	quiescence time.Duration
	dial       replayDialer
}

// Replay executes NDJSON in order. An s2c step is a receive barrier before the
// next c2s step. Existing grouped-send scenarios retain their grouped semantics.
// The scenario declares the complete expected frame sequence. After the final
// expected frame, Replay observes a bounded quiescence window and fails on any
// additional text/binary frame; the finite window is not a claim about frames
// that could arrive later.
func Replay(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration) ReplayResult {
	return replayWithOptions(ctx, wsURL, sc, timeout, replayOptions{
		canonical:  CanonicalOptions{},
		quiescence: defaultReplayQuiescence,
		dial:       defaultReplayDial,
	})
}

func replay(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration, opts CanonicalOptions) (res ReplayResult) {
	return replayWithOptions(ctx, wsURL, sc, timeout, replayOptions{
		canonical:  opts,
		quiescence: defaultReplayQuiescence,
		dial:       defaultReplayDial,
	})
}

func defaultReplayDial(ctx context.Context, wsURL string) (replayConn, error) {
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	return conn, err
}

func replayWithOptions(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration, opts replayOptions) (res ReplayResult) {
	start := time.Now()
	res.Scenario = sc
	defer func() {
		res.DurationMS = time.Since(start).Milliseconds()
		res.Delta = Diff(res.Expected, res.Collected)
		res.Passed = res.Err == nil && res.Delta == ""
	}()
	if len(sc.C2S()) == 0 || len(sc.ExpectedS2C()) == 0 {
		res.Err = fmt.Errorf("replay requires client frames and expected server frames")
		return
	}
	for i, raw := range sc.ExpectedS2C() {
		canonical, err := CanonicalBytesOpts(raw, opts.canonical)
		if err != nil {
			res.Err = fmt.Errorf("expected frame %d: %w", i, err)
			return
		}
		res.Expected = append(res.Expected, canonical)
	}
	if timeout <= 0 {
		timeout = 12 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	dial := opts.dial
	if dial == nil {
		dial = defaultReplayDial
	}
	conn, err := dial(dctx, wsURL)
	if err != nil {
		res.Err = fmt.Errorf("dial: %w", err)
		return
	}
	defer func() { _ = conn.CloseNow() }() // best-effort transport cleanup
	for i, step := range sc.Steps {
		switch step.Dir {
		case "meta":
			continue
		case "c2s":
			if err := conn.Write(dctx, websocket.MessageText, step.Raw); err != nil {
				res.Err = fmt.Errorf("step %d write: %w", i+1, err)
				return
			}
		case "s2c":
			typ, data, err := conn.Read(dctx)
			if err != nil {
				res.Err = fmt.Errorf("step %d read (%d/%d): %w", i+1, len(res.Collected), len(res.Expected), err)
				return
			}
			res.RawCollected = append(res.RawCollected, append(json.RawMessage(nil), data...))
			if typ != websocket.MessageText {
				res.Err = fmt.Errorf("step %d: expected text frame, got %v", i+1, typ)
				return
			}
			canonical, err := CanonicalBytesOpts(data, opts.canonical)
			if err != nil {
				res.Err = fmt.Errorf("step %d server JSON: %w", i+1, err)
				return
			}
			res.Collected = append(res.Collected, canonical)
		default:
			res.Err = fmt.Errorf("step %d: invalid direction %q", i+1, step.Dir)
			return
		}
	}
	if res.Err == nil {
		res.Err = observeQuiescence(dctx, conn, opts.quiescence, &res, opts.canonical)
	}
	return
}

func observeQuiescence(ctx context.Context, conn replayConn, window time.Duration, res *ReplayResult, opts CanonicalOptions) error {
	if window <= 0 {
		return nil
	}
	qctx, cancel := context.WithTimeout(ctx, window)
	defer cancel()
	typ, data, err := conn.Read(qctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// Only the quiescence child deadline is an expected silent-peer
			// outcome. If the scenario context expired or was cancelled first,
			// preserve that failure instead of certifying an incomplete replay.
			if qctx.Err() != nil && ctx.Err() == nil && errors.Is(qctx.Err(), context.DeadlineExceeded) {
				return nil
			}
			return fmt.Errorf("quiescence read: %w", err)
		}
		if errors.Is(err, qctx.Err()) && ctx.Err() == nil {
			return nil
		}
		status := websocket.CloseStatus(err)
		if status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway {
			return nil
		}
		return fmt.Errorf("quiescence read: %w", err)
	}
	res.RawCollected = append(res.RawCollected, append(json.RawMessage(nil), data...))
	if typ != websocket.MessageText {
		return fmt.Errorf("unexpected frame after final expected: got %v", typ)
	}
	canonical, err := CanonicalBytesOpts(data, opts)
	if err != nil {
		return fmt.Errorf("unexpected frame after final expected: %w", err)
	}
	res.Collected = append(res.Collected, canonical)
	return fmt.Errorf("unexpected frame after final expected")
}

// Differential compares actual outputs, not authored goldens. Failed transport
// or decoding is inconclusive evidence and always produces a nonempty delta.
func Differential(ctx context.Context, sc *Scenario, aURL, bURL string, timeout time.Duration) (aRes, bRes ReplayResult, delta string) {
	opts := CanonicalOptions{Differential: true}
	aRes = replay(ctx, aURL, sc, timeout, opts)
	bRes = replay(ctx, bURL, sc, timeout, opts)
	if aRes.Err != nil || bRes.Err != nil {
		return aRes, bRes, fmt.Sprintf("replay incomplete: target=%v; other=%v", aRes.Err, bRes.Err)
	}
	return aRes, bRes, Diff(aRes.Collected, bRes.Collected)
}
