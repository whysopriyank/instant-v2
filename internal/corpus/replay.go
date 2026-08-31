package corpus

import (
	"context"
	"encoding/json"
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

// Replay executes NDJSON in order. An s2c step is a receive barrier before the
// next c2s step. Existing grouped-send scenarios retain their grouped semantics.
// The scenario declares the complete expected frame sequence; this bounded
// replay does not claim the absence of additional frames after its final step.
func Replay(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration) ReplayResult {
	return replay(ctx, wsURL, sc, timeout, CanonicalOptions{})
}

func replay(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration, opts CanonicalOptions) (res ReplayResult) {
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
		canonical, err := CanonicalBytesOpts(raw, opts)
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
	conn, _, err := websocket.Dial(dctx, wsURL, nil)
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
			if typ != websocket.MessageText {
				res.Err = fmt.Errorf("step %d: expected text frame, got %v", i+1, typ)
				return
			}
			res.RawCollected = append(res.RawCollected, append(json.RawMessage(nil), data...))
			canonical, err := CanonicalBytesOpts(data, opts)
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
	return
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
