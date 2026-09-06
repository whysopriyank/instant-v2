package reactive

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

func ents(pairs ...string) string {
	// pairs: id,value alternating; builds [{"id":"<id>","v":<value>}...]
	var out []byte
	out = append(out, '[')
	for i := 0; i < len(pairs); i++ {
		if i > 0 {
			out = append(out, ',')
		}
		id, val := pairs[i], pairs[i+1]
		i++
		out = append(out, fmt.Sprintf(`{"id":%q,"v":%s}`, id, val)...)
	}
	return string(append(out, ']'))
}

func envJSON(data string, extra ...string) json.RawMessage {
	obj := `{"data":{"todos":` + data + `}`
	for _, e := range extra {
		obj += "," + e
	}
	return json.RawMessage(obj + "}")
}

func TestDiffResultsSingleUpdate(t *testing.T) {
	oldJ := envJSON(ents("a", `"x"`, "b", `"y"`))
	newJ := envJSON(ents("a", `"x2"`, "b", `"y"`))
	p, ok := DiffResults(oldJ, newJ)
	if !ok {
		t.Fatal("single-row change must be expressible")
	}
	if len(p.Ops) != 1 || p.Ops[0].Op != OpUpdate || p.Ops[0].ID != "a" || p.Ops[0].Etype != "todos" {
		t.Fatalf("unexpected patch: %+v", p.Ops)
	}
	if p.PageInfo != nil {
		t.Fatal("page-info unchanged; no delta expected")
	}
}

func TestDiffResultsAddRemovePageInfo(t *testing.T) {
	// 8 untouched survivors keep churn at 2/10 — well under MaxTouchedRatio.
	oldJ := envJSON(ents("a", `"x"`, "b", `"b"`, "c", `"c"`, "d", `"d"`, "e", `"e"`,
		"f", `"f"`, "g", `"g"`, "h", `"h"`, "i", `"i"`, "j", `"j"`))
	newJ := envJSON(ents("z", `"new"`, "b", `"b"`, "c", `"c"`, "d", `"d"`, "e", `"e"`,
		"f", `"f"`, "g", `"g"`, "h", `"h"`, "i", `"i"`, "j", `"j"`),
		`"page-info":{"startCursor":"c1","endCursor":"c2","hasNextPage":true}`)
	p, ok := DiffResults(oldJ, newJ)
	if !ok {
		t.Fatal("add/remove must be expressible")
	}
	kinds := map[string]int{}
	for _, op := range p.Ops {
		kinds[op.Op]++
	}
	if kinds[OpAdd] != 1 || kinds[OpRemove] != 1 {
		t.Fatalf("want 1 add + 1 remove, got %v", kinds)
	}
	if p.PageInfo == nil {
		t.Fatal("pagination shifted; page-info delta expected")
	}
}

// JSON spelling is not entity identity: unchanged nested values and page
// metadata must remain a no-op even when the producer changes key order.
func TestDiffResultsEquivalentJSON(t *testing.T) {
	oldJ := envJSON(`[{"id":"a","v":{"n":1,"items":[true,null]}},{"id":"b","v":"keep"}]`,
		`"page-info":{"endCursor":"b","hasNextPage":false}`)
	newJ := envJSON(`[{"v":{"items":[true,null],"n":1.0},"id":"a"},{"v":"keep","id":"b"}]`,
		`"page-info":{"hasNextPage":false,"endCursor":"b"}`)
	p, ok := DiffResults(oldJ, newJ)
	if !ok || len(p.Ops) != 0 || p.PageInfo != nil {
		t.Fatalf("equivalent JSON changed the result: patch=%+v ok=%v", p, ok)
	}
}

func TestDiffResultsEntityOrderAndRawPayload(t *testing.T) {
	oldJ := envJSON(`[{"id":"z"},{"id":"a"},{"id":"b"},{"id":"c"},{"id":"d"},{"id":"e"},{"id":"f"},{"id":"g"}]`)
	newJ := envJSON(`[{"id":"b"},{"id":"c"},{"id":"d"},{"id":"e"},{"id":"f"},{"id":"g","v": {"n": 2}},{"id":"h"}]`)
	p, ok := DiffResults(oldJ, newJ)
	if !ok || len(p.Ops) != 4 {
		t.Fatalf("expected four operations at the churn boundary: patch=%+v ok=%v", p, ok)
	}
	for i, want := range []struct{ kind, id string }{{OpRemove, "a"}, {OpRemove, "z"}, {OpUpdate, "g"}, {OpAdd, "h"}} {
		if got := p.Ops[i]; got.Op != want.kind || got.ID != want.id || got.Etype != "todos" {
			t.Fatalf("op %d = %+v, want %s %s", i, got, want.kind, want.id)
		}
	}
	if got := string(p.Ops[2].Entity); got != `{"id":"g","v": {"n": 2}}` {
		t.Fatalf("update must preserve new raw payload, got %s", got)
	}
}

func TestDiffResultsEmptyEntitiesAndPageOnly(t *testing.T) {
	for _, empty := range []string{`null`, `[]`} {
		t.Run(empty, func(t *testing.T) {
			p, ok := DiffResults(envJSON(empty), envJSON(`[]`, `"page-info":{"hasNextPage":false}`))
			if !ok || len(p.Ops) != 0 || string(p.PageInfo) != `{"hasNextPage":false}` {
				t.Fatalf("empty entity/page-only diff: patch=%+v ok=%v", p, ok)
			}
		})
	}
}

// TestPatchFallbacks pins every documented fallback trigger.
func TestPatchFallbacks(t *testing.T) {
	cases := []struct {
		name string
		oldJ json.RawMessage
		newJ json.RawMessage
	}{
		{
			name: "aggregate-present",
			oldJ: envJSON(ents("a", `"x"`), `"aggregate":{"count":1}`),
			newJ: envJSON(ents("a", `"x"`, "b", `"y"`), `"aggregate":{"count":2}`),
		},
		{
			name: "aggregate-appears-later",
			oldJ: envJSON(ents("a", `"x"`)),
			newJ: envJSON(ents("a", `"x"`), `"aggregate":{"count":1}`),
		},
		{
			// 6 of 10 entities touched ⇒ 60% > MaxTouchedRatio (0.5).
			name: "over-half-touched",
			oldJ: envJSON(ents("a", `"0"`, "b", `"1"`, "c", `"2"`, "d", `"3"`, "e", `"4"`,
				"f", `"5"`, "g", `"6"`, "h", `"7"`, "i", `"8"`, "j", `"9"`)),
			newJ: envJSON(ents("a", `"x0"`, "b", `"x1"`, "c", `"x2"`, "d", `"x3"`, "e", `"x4"`,
				"f", `"x5"`, "g", `"6"`, "h", `"7"`, "i", `"8"`, "j", `"9"`)),
		},
		{
			// Reorder: surviving ids change relative position — no move op
			// exists, positional shifts are ambiguous → full envelope.
			name: "reorder",
			oldJ: envJSON(ents("a", `"1"`, "b", `"2"`, "c", `"3"`)),
			newJ: envJSON(ents("c", `"3"`, "a", `"1"`, "b", `"2"`)),
		},
		{
			name: "malformed-json",
			oldJ: json.RawMessage(`{"data":{`),
			newJ: envJSON(ents("a", `"x"`)),
		},
		{
			name: "duplicate-ids",
			oldJ: envJSON(`[{"id":"a","v":"1"},{"id":"a","v":"2"}]`),
			newJ: envJSON(`[{"id":"a","v":"1"},{"id":"a","v":"2"}]`),
		},
		{
			name: "non-array-etype-payload",
			oldJ: envJSON(`{"not":"array"}`),
			newJ: envJSON(`{"not":"array"}`),
		},
		{name: "missing-id", oldJ: envJSON(`[{"v":1}]`), newJ: envJSON(`[]`)},
		{name: "non-string-id", oldJ: envJSON(`[]`), newJ: envJSON(`[{"id":1}]`)},
		{name: "non-object-entity", oldJ: envJSON(`[]`), newJ: envJSON(`[null]`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if p, ok := DiffResults(tc.oldJ, tc.newJ); ok {
				b, _ := json.Marshal(p)
				t.Fatalf("expected fallback to full envelope, got patch %s", b)
			}
		})
	}
}

// TestApplyPatchRoundTrip mirrors client-side reconciliation: applying the
// patch ops to the old document reproduces the new one exactly.
func TestApplyPatchRoundTrip(t *testing.T) {
	type doc map[string][]map[string]any
	parse := func(raw json.RawMessage) doc {
		var env struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			t.Fatal(err)
		}
		d := doc{}
		for k, v := range env.Data {
			var arr []map[string]any
			_ = json.Unmarshal(v, &arr)
			d[k] = arr
		}
		return d
	}
	// 4 touched of 10 entities — under MaxTouchedRatio.
	oldJ := envJSON(ents("a", `"keep"`, "b", `"drop-me"`, "c", `"also-keep"`,
		"e", `"e"`, "f", `"f"`, "g", `"g"`, "h", `"h"`, "i", `"i"`, "j", `"j"`, "k", `"k"`),
		`"page-info":{"hasNextPage":false}`)
	newJ := envJSON(ents("a", `"keep"`, "c", `"edited"`, "d", `"fresh"`,
		"e", `"e"`, "f", `"f"`, "g", `"g"`, "h", `"h"`, "i", `"i"`, "j", `"j"`, "k", `"k"`),
		`"page-info":{"hasNextPage":true}`)
	p, ok := DiffResults(oldJ, newJ)
	if !ok {
		t.Fatal("expected expressible diff")
	}

	d := parse(oldJ)
	for _, op := range p.Ops {
		switch op.Op {
		case OpRemove:
			live := d[op.Etype][:0]
			for _, e := range d[op.Etype] {
				if e["id"] != op.ID {
					live = append(live, e)
				}
			}
			d[op.Etype] = live
		case OpAdd, OpUpdate:
			var ent map[string]any
			if err := json.Unmarshal(op.Entity, &ent); err != nil {
				t.Fatal(err)
			}
			replaced := false
			for i, e := range d[op.Etype] {
				if e["id"] == op.ID {
					d[op.Etype][i] = ent // update in place
					replaced = true
				}
			}
			if !replaced {
				d[op.Etype] = append(d[op.Etype], ent) // add at tail; order not asserted
			}
		default:
			t.Fatalf("unknown op %q", op.Op)
		}
	}
	sortByID := func(arr []map[string]any) {
		sort.Slice(arr, func(i, j int) bool {
			return arr[i]["id"].(string) < arr[j]["id"].(string)
		})
	}
	sortByID(d["todos"])
	want := parse(newJ)["todos"]
	sortByID(want)
	gotB, _ := json.Marshal(d["todos"])
	wantB, _ := json.Marshal(want) // membership equality; adds land at tail
	if string(gotB) != string(wantB) {
		t.Fatalf("reconciliation mismatch:\n got %s\nwant %s", gotB, wantB)
	}
}

func TestSubscriptionCap(t *testing.T) {
	s := NewStore()
	s.MaxSubsPerApp = 2
	mk := func(id, app string) *Subscription {
		return &Subscription{ID: id, AppID: app, Topics: map[string]bool{"x": true}}
	}
	if _, err := s.Add(mk("s1", "app1")); err != nil {
		t.Fatalf("add 1: %v", err)
	}
	if _, err := s.Add(mk("s2", "app1")); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	_, err := s.Add(mk("s3", "app1"))
	var capErr *SubLimitError
	if err == nil || !errorsAsSubLimit(err, &capErr) {
		t.Fatalf("third add must fail with *SubLimitError, got %v", err)
	}
	if capErr.Max != 2 || capErr.AppID != "app1" {
		t.Fatalf("bad SubLimitError: %+v", capErr)
	}
	if s.Len() != 2 {
		t.Fatalf("rejected sub leaked into store: %d", s.Len())
	}
	// Other apps unaffected; removal frees capacity.
	if _, err := s.Add(mk("s4", "app2")); err != nil {
		t.Fatalf("other app capped wrongly: %v", err)
	}
	s.Remove("s1")
	if _, err := s.Add(mk("s5", "app1")); err != nil {
		t.Fatalf("cap should free after remove: %v", err)
	}
	// Unlimited default (MaxSubsPerApp == 0).
	u := NewStore()
	for i := range 100 {
		if _, err := u.Add(mk(fmt.Sprintf("u%d", i), "app")); err != nil {
			t.Fatalf("unlimited store rejected at %d: %v", i, err)
		}
	}
}

func errorsAsSubLimit(err error, target **SubLimitError) bool {
	e, ok := err.(*SubLimitError)
	if ok {
		*target = e
	}
	return ok
}

// TestNotifierEmitsDeltaPatches proves the notifier path end-to-end: a
// Delta-enabled sub with a prior snapshot gets a patch frame on single-entity
// change; a non-negotiating sub gets only the full envelope.
func TestNotifierEmitsDeltaPatches(t *testing.T) {
	s := NewStore()
	frames := make(chan Frame, 2)
	emit := func(fr Frame) error { frames <- fr; return nil }
	sub := &Subscription{
		ID: "sub-1", AppID: "app1", Topics: map[string]bool{"attr-a": true},
		Emit: emit,
	}
	sub.Delta.Store(true)
	if _, err := s.Add(sub); err != nil {
		t.Fatal(err)
	}
	plain := &Subscription{
		ID: "sub-2", AppID: "app1", Topics: map[string]bool{"attr-a": true},
		Emit: emit,
	}
	if _, err := s.Add(plain); err != nil {
		t.Fatal(err)
	}

	n := &Notifier{Store: s}
	// Both subs' baselines are seeded below; every post-baseline refresh
	// returns the edited result regardless of worker scheduling order.
	edited := envJSON(ents("a", `"1-edited"`, "b", `"2"`))
	n.Refresh = func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
		return edited, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		n.Run(ctx)
		close(runDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(3 * time.Second):
			t.Error("notifier did not stop")
		}
	})

	// Initial snapshots are seeded outside the notifier (ws/sse direct path).
	baseline := envJSON(ents("a", `"1"`, "b", `"2"`))
	sub.SetSnapshot(baseline)
	plain.SetSnapshot(baseline)

	n.Notify(ctx, "app1", []string{"attr-a"}, 7)
	deadline := time.After(3 * time.Second)
	got := map[string]Frame{}
	for len(got) < 2 {
		select {
		case fr := <-frames:
			got[fr.SubID] = fr
		case <-deadline:
			t.Fatalf("timed out; got %d frames", len(got))
		}
	}

	fr := got["sub-1"]
	if len(fr.PatchJSON) == 0 {
		t.Fatal("delta sub must receive a patch frame")
	}
	var p Patch
	if err := json.Unmarshal(fr.PatchJSON, &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Ops) != 1 || p.Ops[0].Op != OpUpdate || p.Ops[0].ID != "a" {
		t.Fatalf("unexpected ops: %+v", p.Ops)
	}
	if fr := got["sub-2"]; len(fr.PatchJSON) != 0 || len(fr.ResultJSON) == 0 {
		t.Fatal("non-delta sub must receive the full envelope only")
	}
}

// TestQueueDepthGauge checks the backpressure gauge the invalidator polls.
func TestQueueDepthGauge(t *testing.T) {
	s := NewStore()
	started := make(chan struct{}, 5)
	release := make(chan struct{})
	n := &Notifier{
		Store: s,
		Refresh: func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
			started <- struct{}{}
			<-release
			return json.RawMessage(`{"data":{}}`), nil
		},
	}
	for i := range 5 {
		if _, err := s.Add(&Subscription{
			ID: fmt.Sprintf("q%d", i), AppID: "app", Topics: map[string]bool{"t": true},
			Emit: func(Frame) error { return nil },
		}); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()

	n.Notify(ctx, "app", []string{"t"}, 1)
	if d := n.QueueDepth(); d != 5 {
		t.Fatalf("queue depth during backlog = %d, want 5", d)
	}
	drained := make(chan struct{})
	go func() {
		n.drainPass(ctx, nil)
		close(drained)
	}()
	t.Cleanup(func() {
		close(release)
		select {
		case <-drained:
		case <-time.After(3 * time.Second):
			t.Error("drain did not finish after releasing refreshes")
		}
	})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("refresh did not start")
	}
	// The gauge counts queued work, not active workers. The first refresh
	// entering proves the pending batch was detached, while release keeps
	// every refresh in flight until after this assertion.
	if d := n.QueueDepth(); d != 0 {
		t.Fatalf("queue depth with batch in flight = %d, want 0", d)
	}
}

// --- Benchmark + acceptance-reported numbers (skipped in -short) ---

const benchEntities = 10_000

func buildLargePair() (oldJSON, newJSON []byte, mutatedIdx int) {
	type e struct {
		ID    string `json:"id"`
		Title string `json:"title"`
		Done  bool   `json:"done"`
		Rank  int    `json:"rank"`
	}
	rng := rand.New(rand.NewPCG(42, 7)) // deterministic fixture
	oldEnts := make([]e, benchEntities)
	for i := range oldEnts {
		oldEnts[i] = e{
			ID:    fmt.Sprintf("00000000-0000-4000-8000-%012d", i),
			Title: fmt.Sprintf("todo #%d", i),
			Done:  rng.IntN(2) == 0,
			Rank:  i,
		}
	}
	newEnts := append([]e(nil), oldEnts...)
	mutatedIdx = rng.IntN(benchEntities) // single-row mutation
	newEnts[mutatedIdx].Title = "mutated!"
	oldData, _ := json.Marshal(map[string]any{"todos": oldEnts})
	newData, _ := json.Marshal(map[string]any{"todos": newEnts})
	oldJSON, _ = json.Marshal(map[string]any{"data": json.RawMessage(oldData)})
	newJSON, _ = json.Marshal(map[string]any{"data": json.RawMessage(newData)})
	return oldJSON, newJSON, mutatedIdx
}

// TestDeltaRefreshBytesReport logs the phase-6 acceptance numbers over a
// 10k-entity result with a single-row mutation: bytes-on-wire full vs delta,
// server-side diff cost, and reconciliation (apply-patch) time.
func TestDeltaRefreshBytesReport(t *testing.T) {
	if testing.Short() {
		t.Skip("reporting test skipped in short mode")
	}
	oldJSON, newJSON, mutated := buildLargePair()

	diffStart := time.Now()
	patch, ok := DiffResults(oldJSON, newJSON)
	diffDur := time.Since(diffStart)
	if !ok {
		t.Fatal("single-row mutation of 10k entities must be expressible")
	}
	patchJSON, _ := json.Marshal(patch)

	fullWire := len(fmt.Sprintf(`{"instaql-query":{},"instaql-result":%s}`, newJSON))
	deltaWire := len(fmt.Sprintf(`{"instaql-query":{},"delta":%s}`, patchJSON))

	reconStart := time.Now()
	var baseline struct {
		Data map[string][]map[string]any `json:"data"`
	}
	if err := json.Unmarshal(oldJSON, &baseline); err != nil {
		t.Fatal(err)
	}
	for _, op := range patch.Ops {
		switch op.Op {
		case OpRemove:
			live := baseline.Data[op.Etype][:0]
			for _, e := range baseline.Data[op.Etype] {
				if e["id"] != op.ID {
					live = append(live, e)
				}
			}
			baseline.Data[op.Etype] = live
		case OpAdd, OpUpdate:
			var ent map[string]any
			if err := json.Unmarshal(op.Entity, &ent); err != nil {
				t.Fatal(err)
			}
			baseline.Data[op.Etype] = append(baseline.Data[op.Etype], ent)
		}
	}
	reconDur := time.Since(reconStart)

	// The reconciled doc must equal the fresh result on the mutated entity.
	var fresh struct {
		Data map[string][]map[string]any `json:"data"`
	}
	if err := json.Unmarshal(newJSON, &fresh); err != nil {
		t.Fatal(err)
	}
	if got := baseline.Data["todos"][len(baseline.Data["todos"])-1]["title"]; got != "mutated!" {
		t.Fatalf("reconciliation lost the mutation: %v", got)
	}
	_ = mutated

	t.Logf("entities=%d ops=%d full_wire=%dB delta_wire=%dB savings=%.2fx diff=%s reconcile=%s",
		benchEntities, len(patch.Ops), fullWire, deltaWire,
		float64(fullWire)/float64(deltaWire), diffDur, reconDur)
}

func BenchmarkDeltaRefresh10k(b *testing.B) {
	oldJSON, newJSON, _ := buildLargePair()
	b.ResetTimer()
	for range b.N {
		if _, ok := DiffResults(oldJSON, newJSON); !ok {
			b.Fatal("expected expressible diff")
		}
	}
}
