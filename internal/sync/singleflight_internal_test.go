package sync

// Internal tests for the add-query single-flight primitive and the
// single-encode tree dispatch (docs/08-tier1-hotpath.md §T1.1).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// TestSnapshotOrRefreshSingleFlight: N concurrent callers on one group see
// exactly ONE refresh; every caller receives the same envelope; the snapshot
// is seeded exactly once and later callers reuse it without any refresh.
func TestSnapshotOrRefreshSingleFlight(t *testing.T) {
	mgr := NewManager(Deps{Store: reactive.NewStore()})
	sub := &reactive.Subscription{ID: "grp-single", AppID: "app"}

	var calls atomic.Int64
	release := make(chan struct{})
	refresh := func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
		calls.Add(1)
		<-release // hold the leader inside the critical section
		return json.RawMessage(`{"data":{"todos":[]}}`), nil
	}

	const n = 8
	results := make([]json.RawMessage, n)
	errs := make([]error, n)
	var start, wg sync.WaitGroup
	start.Add(n)
	ready := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			start.Done()
			<-ready
			results[i], errs[i] = mgr.snapshotOrRefresh(context.Background(), refresh, sub)
		}(i)
	}
	start.Wait()
	close(ready)
	waitForSnapshotFlightParticipants(t, mgr, sub.ID, n)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("refresh ran %d times, want 1", got)
	}
	for i := range results {
		if errs[i] != nil {
			t.Fatalf("caller %d: %v", i, errs[i])
		}
		if !bytes.Equal(results[i], results[0]) {
			t.Fatalf("caller %d received a divergent result", i)
		}
	}
	if snap := sub.Snapshot(); snap == nil || !bytes.Equal(snap, results[0]) {
		t.Fatal("snapshot not seeded by the leader")
	}
	// A later caller rides the snapshot: no additional refresh.
	got, err := mgr.snapshotOrRefresh(context.Background(), refresh, sub)
	if err != nil || !bytes.Equal(got, results[0]) || calls.Load() != 1 {
		t.Fatalf("reuse path: result=%s err=%v calls=%d", got, err, calls.Load())
	}
}

// TestSnapshotOrRefreshErrorPropagatesAndRetries: a failed leader leaves no
// snapshot behind, and the next caller refreshes again.
func TestSnapshotOrRefreshErrorPropagatesAndRetries(t *testing.T) {
	mgr := NewManager(Deps{Store: reactive.NewStore()})
	sub := &reactive.Subscription{ID: "grp-err", AppID: "app"}
	boom := errors.New("boom")
	var calls atomic.Int64
	leaderStarted := make(chan struct{})
	releaseLeader := make(chan struct{})
	refresh := func(context.Context, *reactive.Subscription) (json.RawMessage, error) {
		if calls.Add(1) == 1 {
			close(leaderStarted)
			<-releaseLeader
			return nil, boom
		}
		return json.RawMessage(`{"data":{}}`), nil
	}

	results := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, results[0] = mgr.snapshotOrRefresh(context.Background(), refresh, sub)
	}()
	select {
	case <-leaderStarted:
	case <-time.After(time.Second):
		t.Fatal("leader did not enter refresh")
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, results[1] = mgr.snapshotOrRefresh(context.Background(), refresh, sub)
	}()
	waitForSnapshotFlightParticipants(t, mgr, sub.ID, 2)
	close(releaseLeader)
	wg.Wait()
	for i, err := range results {
		if !errors.Is(err, boom) {
			t.Fatalf("caller %d: want boom, got %v", i, err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("failed flight ran %d refreshes, want 1", got)
	}
	if sub.Snapshot() != nil {
		t.Fatal("a failed refresh must not seed a snapshot")
	}
	got, err := mgr.snapshotOrRefresh(context.Background(), refresh, sub)
	if err != nil || string(got) != `{"data":{}}` || calls.Load() != 2 {
		t.Fatalf("retry after failure: result=%s err=%v calls=%d", got, err, calls.Load())
	}
}

// TestSnapshotOrRefreshCanceledWaiter does not let a canceled follower hold
// up the leader or turn a completed failure into an extra refresh attempt.
func TestSnapshotOrRefreshCanceledWaiter(t *testing.T) {
	mgr := NewManager(Deps{Store: reactive.NewStore()})
	sub := &reactive.Subscription{ID: "grp-cancel", AppID: "app"}
	leaderStarted := make(chan struct{})
	var calls atomic.Int64
	refresh := func(ctx context.Context, _ *reactive.Subscription) (json.RawMessage, error) {
		calls.Add(1)
		close(leaderStarted)
		<-ctx.Done()
		return nil, ctx.Err()
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	var leaderErr error
	leaderDone := make(chan struct{})
	go func() {
		_, leaderErr = mgr.snapshotOrRefresh(leaderCtx, refresh, sub)
		close(leaderDone)
	}()
	select {
	case <-leaderStarted:
	case <-time.After(time.Second):
		t.Fatal("leader did not enter refresh")
	}

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	var waiterErr error
	waiterDone := make(chan struct{})
	go func() {
		_, waiterErr = mgr.snapshotOrRefresh(waiterCtx, refresh, sub)
		close(waiterDone)
	}()
	waitForSnapshotFlightParticipants(t, mgr, sub.ID, 2)
	cancelWaiter()
	select {
	case <-waiterDone:
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}
	if !errors.Is(waiterErr, context.Canceled) {
		t.Fatalf("canceled waiter: want context canceled, got %v", waiterErr)
	}
	cancelLeader()
	select {
	case <-leaderDone:
	case <-time.After(time.Second):
		t.Fatal("canceled leader did not return")
	}
	if !errors.Is(leaderErr, context.Canceled) {
		t.Fatalf("canceled leader: want context canceled, got %v", leaderErr)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("canceled flight ran %d refreshes, want 1", got)
	}
}

func waitForSnapshotFlightParticipants(t *testing.T, mgr *Manager, id string, want int) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		snapshotFlightsMu.Lock()
		flightAny, ok := mgr.flight.Load(id)
		participants := 0
		if ok {
			participants = flightAny.(*snapshotFlight).participants
		}
		snapshotFlightsMu.Unlock()
		if participants >= want {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("flight %q reached %d participants, want %d", id, participants, want)
		case <-ticker.C:
		}
	}
}

// TestDispatchTreeSingleEncodeParity pins the tree-class frame bytes to the
// exact shape the pre-fix meta branch produced — the single-encode reorder
// must be byte-invisible on the wire.
func TestDispatchTreeSingleEncodeParity(t *testing.T) {
	mgr := NewManager(Deps{Store: reactive.NewStore()})
	cat := &platform.AttrCatalog{} // unknown attrs take the lossless fallback branch
	sub := &reactive.Subscription{
		ID:     "grp-tree",
		AppID:  "app",
		Query:  json.RawMessage(`{"todos":{}}`),
		Topics: map[string]bool{"attr-a": true},
	}
	g := &queryGroup{
		key: sub.ID, class: wireTree, appID: sub.AppID, sub: sub, cat: cat,
		members: map[*Session]member{},
	}

	var got []byte
	sess := &Session{ID: "s1", AppID: "app", SendRaw: func(b []byte) error {
		got = append([]byte(nil), b...)
		return nil
	}}
	g.members[sess] = member{sess: sess}

	fr := reactive.Frame{
		SubID:         sub.ID,
		QueryJSON:     json.RawMessage(`{"todos":{}}`),
		ResultJSON:    json.RawMessage(`{"data":{"todos":[]},"page-info":{"hasNextPage":false}}`),
		ProcessedTxID: 7,
	}
	mgr.dispatchGroup(g, fr)
	if got == nil {
		t.Fatal("no frame delivered")
	}

	// Hand-assembled expectation: exactly one encode, meta branch shape
	// (resultMetaOf surfaces the page-info meta for this envelope).
	tree, err := UnwrapTree(fr.ResultJSON)
	if err != nil {
		t.Fatal(err)
	}
	want := encodeFrame(Frame{
		"op": json.RawMessage(`"refresh-ok"`),
		"computations": computationEntry(
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(fr.QueryJSON)},
			[2]json.RawMessage{keyInstaqlResult, tree},
			[2]json.RawMessage{keyResultMeta, json.RawMessage(`{"page-info":{"hasNextPage":false}}`)},
		),
		"result-meta":     json.RawMessage(`{"page-info":{"hasNextPage":false}}`),
		"processed-tx-id": json.RawMessage(`7`),
	})
	if !bytes.Equal(got, want) {
		t.Fatalf("tree frame diverged from single-encode expectation:\n got=%s\nwant=%s", got, want)
	}
}
