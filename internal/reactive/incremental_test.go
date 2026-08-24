package reactive

// Incremental result maintenance tests (docs/09-tier2-architecture.md §T2.5).
//
// The centerpiece is TestIncrementalMatchesFullRefresh: a randomized
// workload of creates/updates/deletes runs against an in-memory world; every
// step renders the full-refresh ORACLE for each eligible subscription and
// demands byte equality with whatever the incremental path emitted. Any
// bail-out divergence is a bug. A live-DB variant of the same guarantee
// holds because InstaqlSource mirrors instaql's WHERE compiler and loader —
// the mirror is exercised by cmd-level wiring, this file pins the logic.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/instant-v2/instant-v2/internal/instaql"
)

// diffWorld is the in-memory entity store backing both the fake
// ChangeSource and the oracle renderer.
type diffWorld struct {
	ents map[string]map[string]map[string]any // etype → id → label → value
	next int
}

func newDiffWorld() *diffWorld {
	return &diffWorld{ents: map[string]map[string]map[string]any{}}
}

func (w *diffWorld) etypes() []string { return []string{"posts", "comments"} }

func (w *diffWorld) create(etype string, fields map[string]any) Change {
	w.next++
	id := fmt.Sprintf("e-%04d", w.next)
	if w.ents[etype] == nil {
		w.ents[etype] = map[string]map[string]any{}
	}
	w.ents[etype][id] = fields
	return Change{Etype: etype, EntityID: id, AttrIDs: attrLabels(fields)}
}

func (w *diffWorld) update(etype, id string, mutate func(map[string]any)) (Change, bool) {
	e, ok := w.ents[etype][id]
	if !ok {
		return Change{}, false
	}
	before := map[string]any{}
	for k, v := range e {
		before[k] = v
	}
	mutate(e)
	touched := map[string]bool{}
	for k := range e {
		touched[k] = true
	}
	for k := range before {
		touched[k] = true
	}
	return Change{Etype: etype, EntityID: id, AttrIDs: keysOf(touched)}, true
}

func (w *diffWorld) delete(etype, id string) (Change, bool) {
	e, ok := w.ents[etype][id]
	if !ok {
		return Change{}, false
	}
	delete(w.ents[etype], id)
	return Change{Etype: etype, EntityID: id, AttrIDs: attrLabels(e)}, true
}

// matchWhere evaluates a flat form's scalar-equality where clause — the only
// op the differential queries use. Deleted entities match nothing.
func matchWhere(o *instaql.Options, e map[string]any) bool {
	if o == nil {
		return true
	}
	for _, cond := range o.Where {
		want, ok := cond.Value.(string)
		if !ok {
			continue // non-scalar conditions aren't used by these queries
		}
		if got, _ := e[cond.Path[0]].(string); got != want {
			return false
		}
	}
	return true
}

// modelSource is the fake ChangeSource over the world.
type modelSource struct{ w *diffWorld }

func (m modelSource) Members(ctx context.Context, appID string, f *instaql.Form, candidates []string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, id := range candidates {
		if e, ok := m.w.ents[f.Etype][id]; ok && matchWhere(f.Options, e) {
			out[id] = true
		}
	}
	return out, nil
}

func (m modelSource) Entities(ctx context.Context, appID string, f *instaql.Form, ids []string) (map[string]json.RawMessage, error) {
	out := map[string]json.RawMessage{}
	for _, id := range ids {
		e, ok := m.w.ents[f.Etype][id]
		if !ok {
			continue
		}
		payload := map[string]any{"id": id}
		for k, v := range e {
			payload[k] = v
		}
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		out[id] = b
	}
	return out, nil
}

// renderOracle mirrors instaql.Run for flat root forms: members filtered by
// WHERE, ids ascending (applyOrder's default), payloads projected like
// projectEntity, envelope {"data":{...}}.
func renderOracle(w *diffWorld, rawQuery json.RawMessage) json.RawMessage {
	var raw map[string]any
	if err := json.Unmarshal(rawQuery, &raw); err != nil {
		panic(err)
	}
	q, err := instaql.Coerce(raw)
	if err != nil {
		panic(err)
	}
	data := map[string]json.RawMessage{}
	for _, f := range q.Forms {
		var ids []string
		for id, e := range w.ents[f.Etype] {
			if matchWhere(f.Options, e) {
				ids = append(ids, id)
			}
		}
		sortStrings(ids)
		arr := make([]json.RawMessage, 0, len(ids))
		for _, id := range ids {
			payload := map[string]any{"id": id}
			for k, v := range w.ents[f.Etype][id] {
				payload[k] = v
			}
			b, err := json.Marshal(payload)
			if err != nil {
				panic(err)
			}
			arr = append(arr, b)
		}
		b, err := json.Marshal(arr)
		if err != nil {
			panic(err)
		}
		data[f.Etype] = b
	}
	envB, err := json.Marshal(struct {
		Data map[string]json.RawMessage `json:"data"`
	}{data})
	if err != nil {
		panic(err)
	}
	return envB
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func attrLabels(fields map[string]any) []string {
	out := make([]string, 0, len(fields))
	for k := range fields {
		out = append(out, k)
	}
	return out
}

const (
	diffApp   = "00000000-0000-4000-8000-000000000001"
	diffTitle = "title"
	diffPub   = "published"
	diffText  = "text"
)

var diffQueries = []struct {
	name     string
	raw      string
	eligible bool
}{
	{"match-all", `{"posts":{}}`, true},
	{"where", `{"posts":{"$":{"where":{"published":"yes"}}}}`, true},
	{"multi-root", `{"posts":{},"comments":{}}`, true},
	{"nested", `{"posts":{"comments":{}}}`, false},       // must always bail
	{"pagination", `{"posts":{"$":{"limit":5}}}`, false}, // must always bail
	{"order", `{"posts":{"$":{"order":{"k":"title"}}}}`, false},
	{"aggregate", `{"posts":{"$":{"aggregate":"count"}}}`, false},
}

type diffHarness struct {
	n      *Notifier
	store  *Store
	world  *diffWorld
	mu     sync.Mutex
	frames map[string]json.RawMessage
	calls  map[string]*atomic.Int64 // per sub Refresh invocations
	subs   map[string]*Subscription
}

func newDiffHarness(t testing.TB, inc bool) *diffHarness {
	t.Helper()
	h := &diffHarness{
		store:  NewStore(),
		world:  newDiffWorld(),
		frames: map[string]json.RawMessage{},
		calls:  map[string]*atomic.Int64{},
		subs:   map[string]*Subscription{},
	}
	h.n = &Notifier{Store: h.store}
	if inc {
		h.n.Inc = &Incremental{Source: modelSource{w: h.world}}
	}
	allTopics := map[string]bool{diffTitle: true, diffPub: true, diffText: true}
	for _, dq := range diffQueries {
		dq := dq
		sub := &Subscription{
			ID: "sub-" + dq.name, AppID: diffApp,
			Query:  json.RawMessage(dq.raw),
			Topics: allTopics,
		}
		counter := &atomic.Int64{}
		h.calls[sub.ID] = counter
		mu := &h.mu
		sub.Emit = func(f Frame) {
			mu.Lock()
			h.frames[f.SubID] = f.ResultJSON
			mu.Unlock()
		}
		if _, err := h.store.Add(sub); err != nil {
			t.Fatalf("add %s: %v", dq.name, err)
		}
		h.subs[dq.name] = sub
	}
	h.n.Refresh = func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
		if c, ok := h.calls[sub.ID]; ok {
			c.Add(1)
		}
		return renderOracle(h.world, sub.Query), nil
	}
	return h
}

// seed does the initial full refresh so materialized state exists.
func (h *diffHarness) seed(ctx context.Context) {
	h.n.Notify(ctx, diffApp, []string{diffTitle}, 1)
	for h.n.drainPass(ctx, slog.Default()) {
	}
}

// TestIncrementalMatchesFullRefresh is the property/fuzz differential:
// randomized workloads through the incremental-only path must produce
// byte-identical envelopes to the full-refresh oracle at every step,
// including steps that force bail-outs (unknown changes, ineligible shapes).
func TestIncrementalMatchesFullRefresh(t *testing.T) {
	ctx := context.Background()
	for seed := int64(1); seed <= 8; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(seed), uint64(seed*7919)))
			h := newDiffHarness(t, true)
			h.seed(ctx)

			tx := int64(1)
			steps := 300
			for step := 0; step < steps; step++ {
				var changes []Change
				switch rng.IntN(10) {
				case 0, 1, 2, 3: // create
					etype := h.world.etypes()[rng.IntN(2)]
					fields := map[string]any{diffTitle: fmt.Sprintf("t-%d", rng.IntN(20))}
					if rng.IntN(2) == 0 {
						fields[diffPub] = "yes"
					} else if rng.IntN(2) == 0 {
						fields[diffPub] = "no"
					}
					changes = append(changes, h.world.create(etype, fields))
				case 4, 5, 6: // delete random existing entity
					etype := h.world.etypes()[rng.IntN(2)]
					if ids := existingIDs(h.world, etype); len(ids) > 0 {
						id := ids[rng.IntN(len(ids))]
						if c, ok := h.world.delete(etype, id); ok {
							changes = append(changes, c)
						}
					}
				default: // update (may flip membership via published)
					etype := h.world.etypes()[rng.IntN(2)]
					if ids := existingIDs(h.world, etype); len(ids) > 0 {
						id := ids[rng.IntN(len(ids))]
						c, ok := h.world.update(etype, id, func(e map[string]any) {
							switch rng.IntN(3) {
							case 0:
								e[diffTitle] = fmt.Sprintf("t-%d", rng.IntN(20))
							case 1:
								if rng.IntN(2) == 0 {
									e[diffPub] = "yes"
								} else {
									delete(e, diffPub)
								}
							default:
								e[diffText] = fmt.Sprintf("x-%d", rng.IntN(50))
							}
						})
						if ok {
							changes = append(changes, c)
						}
					}
				}
				if len(changes) == 0 {
					continue
				}
				tx++

				// Oracles are computed AFTER the mutation but BEFORE the
				// drain — the drain must observe exactly this world.
				oracles := map[string]json.RawMessage{}
				for _, dq := range diffQueries {
					if !dq.eligible {
						continue
					}
					oracles[dq.name] = renderOracle(h.world, json.RawMessage(dq.raw))
				}

				if step%11 == 5 {
					// Unknown-changes step: legacy Notify must still land the
					// identical envelope via full refresh.
					h.n.Notify(ctx, diffApp, []string{diffTitle}, tx)
				} else {
					h.n.NotifyChanges(ctx, diffApp, changes, tx)
				}
				for h.n.drainPass(ctx, slog.Default()) {
				}

				for name, want := range oracles {
					got := h.frames["sub-"+name]
					if string(got) == "" {
						t.Fatalf("step %d: sub %s emitted nothing", step, name)
					}
					if string(got) != string(want) {
						t.Fatalf("step %d: sub %s diverged from oracle\n incremental: %s\n oracle:      %s",
							step, name, got, want)
					}
				}
			}

			// The economics claim, asserted structurally: eligible subs must
			// have needed dramatically fewer full recomputes than drains.
			full := h.calls["sub-match-all"].Load()
			if full > 30 { // 300 steps, ~27 unknown-change drains tolerated
				t.Fatalf("match-all fell back to full refresh %d times", full)
			}
			// Ineligible subs must have bailed on EVERY known-change drain.
			if got := h.calls["sub-nested"].Load(); got < 200 {
				t.Fatalf("nested bailed only %d times; bail-out coverage regressed", got)
			}
		})
	}
}

func existingIDs(w *diffWorld, etype string) []string {
	var ids []string
	for id := range w.ents[etype] {
		ids = append(ids, id)
	}
	sortStrings(ids)
	return ids
}

// TestNotifyChangesUnknownBroadcastDirtiesWholeApp pins the routing rule:
// nil/empty or partially-identified change sets dirty EVERY subscription of
// the app, including ones no topic would match.
func TestNotifyChangesUnknownBroadcastDirtiesWholeApp(t *testing.T) {
	ctx := context.Background()
	h := newDiffHarness(t, false) // engine off: pure routing check
	h.seed(ctx)

	// A sub whose topics match nothing the changes reference; its frames
	// land in their own capture slot.
	oddSeen := false
	odd := &Subscription{
		ID: "sub-odd", AppID: diffApp,
		Query:  json.RawMessage(`{"posts":{}}`),
		Topics: map[string]bool{"unrelated": true},
		Emit:   func(Frame) { oddSeen = true },
	}
	if _, err := h.store.Add(odd); err != nil {
		t.Fatalf("add odd: %v", err)
	}

	tx := int64(100)
	for _, tc := range []struct {
		name    string
		changes []Change
	}{
		{"nil slice", nil},
		{"empty slice", []Change{}},
		{"missing attrs", []Change{{Etype: "posts", EntityID: "e-1"}}},
		{"missing etype", []Change{{EntityID: "e-1", AttrIDs: []string{diffTitle}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx++
			h.n.NotifyChanges(ctx, diffApp, tc.changes, tx)
			if got := h.n.QueueDepth(); got == 0 {
				t.Fatalf("%s: nothing enqueued; broadcast expected", tc.name)
			}
			for h.n.drainPass(ctx, slog.Default()) {
			}
			if !oddSeen {
				t.Fatalf("%s: topic-unmatched app sub was not dirtied", tc.name)
			}
		})
	}
}

// TestLegacyPathByteIdenticalWhenEngineOff pins the default-behavior
// invariant: with Inc nil, Notify AND NotifyChanges take full refresh and
// emit exactly what Refresh returned.
func TestLegacyPathByteIdenticalWhenEngineOff(t *testing.T) {
	ctx := context.Background()
	h := newDiffHarness(t, false)
	h.seed(ctx)

	ch := h.world.create("posts", map[string]any{diffTitle: "hello"})
	want := renderOracle(h.world, json.RawMessage(`{"posts":{}}`))
	h.n.NotifyChanges(ctx, diffApp, []Change{ch}, 99)
	for h.n.drainPass(ctx, slog.Default()) {
	}
	if got := h.frames["sub-match-all"]; string(got) != string(want) {
		t.Fatalf("legacy path diverged:\n got:  %s\n want: %s", got, want)
	}
	if got := h.calls["sub-match-all"].Load(); got == 0 {
		t.Fatal("engine-off NotifyChanges must take full refresh")
	}
}

// BenchmarkMatchAllAppend measures docs/09 §T2.5's target scenario: a
// match-all group under append writes. The counter hooks the Refresh seam —
// in production wiring each call is one instaql CTE execution.
func BenchmarkMatchAllAppend(b *testing.B) {
	ctx := context.Background()
	for _, mode := range []struct {
		name string
		inc  bool
	}{
		{"full-refresh", false},
		{"incremental", true},
	} {
		b.Run(mode.name, func(b *testing.B) {
			h := newDiffHarness(b, mode.inc)
			// Pre-populate 1000 entities so the group has real mass.
			for i := 0; i < 1000; i++ {
				h.world.create("posts", map[string]any{
					diffTitle: fmt.Sprintf("seed-%d", i),
					diffPub:   "yes",
				})
			}
			h.seed(ctx)
			executions := h.calls["sub-match-all"]
			start := executions.Load()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				ch := h.world.create("posts", map[string]any{
					diffTitle: fmt.Sprintf("append-%d", i),
					diffPub:   "yes",
				})
				tx := int64(i + 2)
				if mode.inc {
					h.n.NotifyChanges(ctx, diffApp, []Change{ch}, tx)
				} else {
					h.n.Notify(ctx, diffApp, ch.AttrIDs, tx)
				}
				for h.n.drainPass(ctx, slog.Default()) {
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(executions.Load()-start)/float64(b.N), "instaql-exec/op")
		})
	}
}
