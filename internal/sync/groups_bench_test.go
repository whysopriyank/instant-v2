package sync

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// BenchmarkGroupDispatchFanout measures one generation fanned to N members
// through the shared-render path (docs/08-tier1-hotpath.md §T1.1): render +
// marshal happen ONCE; each member send is a pre-encoded byte copy.
func BenchmarkGroupDispatchFanout(b *testing.B) {
	const members = 100

	mgr := NewManager(Deps{Store: reactive.NewStore()})
	cat := &platform.AttrCatalog{} // unknown attrs take the lossless fallback branch

	rawQ := json.RawMessage(`{"todos":{}}`)
	sub := &reactive.Subscription{
		ID:     "grp-bench",
		AppID:  "bench-app",
		Query:  rawQ,
		Topics: map[string]bool{"attr-a": true},
	}
	g := &queryGroup{
		key: sub.ID, class: wireNodelist, appID: sub.AppID,
		sub: sub, cat: cat,
		members: map[*Session]member{},
	}

	result := buildBenchResult(200)
	fr := reactive.Frame{
		SubID:         sub.ID,
		QueryJSON:     rawQ,
		ResultJSON:    result,
		ProcessedTxID: 42,
	}

	var sent int
	for i := range members {
		// distinct sessions: group membership is keyed by *Session
		s := &Session{
			ID:    fmt.Sprintf("sess-%d", i),
			AppID: "bench-app",
			Subs:  map[string]bool{},
			SendRaw: func(b []byte) error {
				sent++
				return nil
			},
		}
		g.mu.Lock()
		g.members[s] = member{sess: s, delta: false}
		g.mu.Unlock()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sent = 0
		mgr.dispatchGroup(g, fr)
		if sent != members {
			b.Fatalf("delivered %d/%d", sent, members)
		}
	}
}

func buildBenchResult(n int) json.RawMessage {
	ents := make([]map[string]any, 0, n)
	for i := range n {
		ents = append(ents, map[string]any{
			"id":    fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i),
			"title": fmt.Sprintf("todo item number %d with some padding text", i),
			"done":  i%2 == 0,
		})
	}
	b, _ := json.Marshal(map[string]any{"data": map[string]any{"todos": ents}, "page-info": nil, "aggregate": nil})
	return b
}
