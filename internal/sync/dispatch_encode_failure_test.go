package sync

// RT-002b/RT-002e deterministic encode-failure coverage.
//
// Frame.Encode now validates every value with json.Valid (matching stdlib
// Marshal's rejection of bad RawMessage) so an invalid or unencodable
// payload fails before any wire bytes are produced. These regressions force
// that failure through the real dispatch path — not a valid-frame test —
// using an invalid QueryJSON that still passes rendering (valid ResultJSON)
// but makes the built refresh envelope unencodable.
//
// Proven for both wire classes and both transports:
//   - dispatchGroup returns an error;
//   - no WS (SendRaw) or SSE (SendRawGen) writer receives bytes;
//   - no empty frame is emitted;
//   - snapshot and watermark remain unchanged;
//   - the same transaction is re-armed for retry (via the real notifier).

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

func invalidQueryFrame(subID string) reactive.Frame {
	return reactive.Frame{
		SubID:         subID,
		QueryJSON:     json.RawMessage(`not json{{{`),
		ResultJSON:    json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`),
		ProcessedTxID: 7,
	}
}

// TestDispatchEncodeFailureServesNothing proves an unencodable generation
// serves nothing and commits nothing on either wire class.
func TestDispatchEncodeFailureServesNothing(t *testing.T) {
	for _, class := range []wireClass{wireTree, wireNodelist} {
		var wsSent, sseSent [][]byte
		ws := &Session{Subs: map[string]bool{}, SendRaw: func(b []byte) error {
			wsSent = append(wsSent, b)
			return nil
		}}
		sse := &Session{Subs: map[string]bool{}, SendRawGen: func(b []byte, _ uint64, _ *reactive.Subscription) error {
			sseSent = append(sseSent, b)
			return nil
		}}
		mgr := NewManager(Deps{})
		sub := &reactive.Subscription{ID: "grp-enc", AppID: "app", Query: json.RawMessage(`{"todos":{}}`)}
		g := &queryGroup{key: "grp-enc", class: class, appID: "app", sub: sub,
			cat: &platform.AttrCatalog{}, members: map[*Session]member{}}
		mgr.groups["grp-enc"] = g
		g.members[ws] = member{sess: ws}
		g.members[sse] = member{sess: sse, delta: false}
		ws.Subs["grp-enc"] = true
		sse.Subs["grp-enc"] = true

		bad := invalidQueryFrame("grp-enc")
		if err := mgr.dispatchGroup(g, bad); err == nil {
			t.Fatalf("class %v: encode failure must return an error", class)
		} else if !strings.Contains(err.Error(), "invalid JSON") && !strings.Contains(err.Error(), "encode") {
			t.Fatalf("class %v: unexpected encode error %v", class, err)
		}
		if len(wsSent) != 0 {
			t.Fatalf("class %v: WS writer received %d frames on encode failure", class, len(wsSent))
		}
		if len(sseSent) != 0 {
			t.Fatalf("class %v: SSE writer received %d frames on encode failure", class, len(sseSent))
		}
		for _, b := range append(wsSent, sseSent...) {
			if len(b) == 0 {
				t.Fatalf("class %v: empty frame emitted on encode failure", class)
			}
		}
		if snap := sub.Snapshot(); snap != nil {
			t.Fatalf("class %v: snapshot committed on encode failure: %s", class, snap)
		}
		if tx := sub.TxID.Load(); tx != 0 {
			t.Fatalf("class %v: watermark advanced to %d on encode failure", class, tx)
		}
	}
}

// TestDispatchEncodeFailureSchedulesSameTxRetry proves the failed
// transaction is re-armed with its identical ID through the real notifier
// drain → refresh → dispatch → PublishGeneration chain (hermetic refresh,
// real dispatch, real retry timers).
func TestDispatchEncodeFailureSchedulesSameTxRetry(t *testing.T) {
	ctx := context.Background()
	store := reactive.NewStore()
	sub := &reactive.Subscription{
		ID:     "grp-enc-retry",
		AppID:  "app",
		Query:  json.RawMessage(`{"todos":{}}`),
		Topics: map[string]bool{"attr1": true},
	}
	// Invalid query forces Encode failure inside the real dispatch while
	// refresh itself succeeds (valid ResultJSON renders fine).
	badQuery := json.RawMessage(`not json{{{`)
	sub.Query = badQuery
	if _, err := store.Add(sub); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(Deps{Store: store})
	g := &queryGroup{key: sub.ID, class: wireTree, appID: "app", sub: sub,
		members: map[*Session]member{}}
	mgr.groups[sub.ID] = g
	var sent atomic.Int64
	sess := &Session{ID: "s", AppID: "app", Subs: map[string]bool{},
		SendRaw: func(b []byte) error { sent.Add(1); return nil },
		Close:   func() {}}
	g.members[sess] = member{sess: sess}
	sess.Subs[sub.ID] = true

	var attempts atomic.Int64
	var txMu sync.Mutex
	var attemptTx []int64
	goodResult := json.RawMessage(`{"data":{"todos":[{"id":"e1"}]}}`)
	notifier := &reactive.Notifier{
		Store: store,
		Refresh: func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
			return goodResult, nil
		},
		Publish: mgr.PublishGeneration,
	}
	// Wrap Emit with attempt recording, then delegate to the real dispatch.
	origSub := sub
	origSub.Emit = func(fr reactive.Frame) error {
		attempts.Add(1)
		txMu.Lock()
		attemptTx = append(attemptTx, fr.ProcessedTxID)
		txMu.Unlock()
		return mgr.dispatchGroup(g, fr)
	}
	nctx, stop := context.WithCancel(ctx)
	defer stop()
	go notifier.Run(nctx)

	notifier.Notify(ctx, "app", []string{"attr1"}, 7)
	// First attempt fails fast (drain is immediate); retry is bounded
	// (100ms base + stable jitter, capped at 5s). Await two attempts.
	deadline := time.Now().Add(10 * time.Second)
	for attempts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("encode-failed generation attempts = %d; want >=2 (initial + same-tx retry)", got)
	}
	txMu.Lock()
	defer txMu.Unlock()
	for _, tx := range attemptTx {
		if tx != 7 {
			t.Fatalf("retry used txID %d; want same transaction 7", tx)
		}
	}
	if sent.Load() != 0 {
		t.Fatalf("encode failure served %d frames; want none", sent.Load())
	}
	if snap := sub.Snapshot(); snap != nil {
		t.Fatalf("snapshot committed on encode failure: %s", snap)
	}
	if tx := sub.TxID.Load(); tx != 0 {
		t.Fatalf("watermark advanced to %d on encode failure", tx)
	}
}
