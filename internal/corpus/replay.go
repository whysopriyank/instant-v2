package corpus

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/coder/websocket"
)

// ReplayResult is the outcome of one scenario replay against a live server.
type ReplayResult struct {
	Scenario   *Scenario
	Expected   [][]byte // canonical golden s2c frames
	Collected  [][]byte // canonical frames actually received
	DurationMS int64
	Err        error
	Delta      string // Diff(Expected,Collected); empty on pass
	Passed     bool
}

// Replay drives the c2s frames of sc against the WebSocket at wsURL (ws:// or wss://),
// collects s2c frames until the golden count is reached or the deadline expires,
// canonicalizes both sides, and returns the delta.
// A live server is required; use FakeServer in unit tests (replay_test.go).
func Replay(ctx context.Context, wsURL string, sc *Scenario, timeout time.Duration) ReplayResult {
	start := time.Now()
	res := ReplayResult{Scenario: sc}

	c2s := sc.C2S()
	expectedRaw := sc.ExpectedS2C()
	res.Expected = canonicalizeAll(expectedRaw)

	if timeout == 0 {
		timeout = 12 * time.Second
	}
	dctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, _, err := websocket.Dial(dctx, wsURL, nil)
	if err != nil {
		res.Err = fmt.Errorf("dial %s: %w", wsURL, err)
		return res
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	// Send all c2s frames one after another so that the server sees client order.
	for _, raw := range c2s {
		if err := conn.Write(dctx, websocket.MessageText, raw); err != nil {
			res.Err = fmt.Errorf("write: %w", err)
			return res
		}
	}

	// Collect until golden count or timeout. If golden is empty (record-only
	// scenario), collect for 500ms and report what arrived.
	goal := len(expectedRaw)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	var got [][]byte
	for goal == 0 || len(got) < goal {
		// Per-message read with remaining time; treat deadline expiry as completion.
		msgType, data, err := conn.Read(dctx)
		if err != nil {
			if goal == 0 {
				// record-only shape — read ending is expected
				break
			}
			// Closed before goal — record what we have and let the delta surface it.
			if len(got) == goal {
				break
			}
			res.Err = fmt.Errorf("read (%d/%d): %w", len(got), goal, err)
			break
		}
		if msgType != websocket.MessageText {
			continue
		}
		c, err := CanonicalBytes(data)
		if err != nil {
			c = data
		}
		got = append(got, c)
		if goal == 0 {
			// No golden: if we stalled for the full timeout window, stop rather than hang.
			select {
			case <-dctx.Done():
				goto finish
			default:
			}
		}
	}
finish:
	res.Collected = got
	res.DurationMS = time.Since(start).Milliseconds()
	// Canonicalize expected as well when constructing the golden from file bytes.
	// Already done into res.Expected above.
	res.Delta = Diff(res.Expected, res.Collected)
	res.Passed = res.Err == nil && res.Delta == ""
	return res
}

func canonicalizeAll(in []json.RawMessage) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, b := range in {
		c, err := CanonicalBytes(b)
		if err != nil {
			c = b
		}
		out = append(out, c)
	}
	return out
}

// Differential replays the same scenario against two WebSocket endpoints and diffs
// their collected outputs against each other (v1 vs v2 side-by-side). The golden
// is not consulted — the two servers are the oracles for each other.
func Differential(ctx context.Context, sc *Scenario, aURL, bURL string, timeout time.Duration) (aRes, bRes ReplayResult, delta string) {
	if timeout == 0 {
		timeout = 12 * time.Second
	}
	aRes = Replay(ctx, aURL, sc, timeout)
	bRes = Replay(ctx, bURL, sc, timeout)
	// Compare collected sides even if golden mismatches — the point is side-by-side fidelity.
	d := Diff(aRes.Collected, bRes.Collected)
	return aRes, bRes, d
}
