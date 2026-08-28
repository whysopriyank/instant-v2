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

// benchAttrID derives a unique [16]byte from i — deterministic, no crypto.
func benchAttrID(i int) [16]byte {
	var u [16]byte
	u[0] = byte(i >> 24)
	u[1] = byte(i >> 16)
	u[2] = byte(i >> 8)
	u[3] = byte(i)
	u[6] = 0x40 // version 4 bits, so ids look like real uuids
	u[8] = 0x80
	return u
}

func benchStr(s string) *string { return &s }

// BenchmarkGroupDispatchFanoutCatalog is the existing fanout benchmark with a
// POPULATED catalog (300 attrs across 3 etypes). The empty-catalog variant
// above systematically under-measures the render path: real apps carry
// hundreds of attrs, and every entity label resolves through the catalog.
// This is the metric the platform index work must be judged by.
func BenchmarkGroupDispatchFanoutCatalog(b *testing.B) {
	const members = 100

	mgr := NewManager(Deps{Store: reactive.NewStore()})
	cat := &platform.AttrCatalog{}
	etypes := []string{"todos", "users", "comments"}
	baseLabels := []string{"title", "done", "priority", "owner", "tag", "size"}
	for i := 0; i < 300; i++ {
		label := baseLabels[i%len(baseLabels)]
		if i >= len(baseLabels) {
			// First rotation keeps bare labels (title/done/...) so entity
			// fields hit the index; the rest get suffixes for volume.
			label += string(rune('a' + (i/len(baseLabels))%26))
		}
		a := platform.Attr{
			ID:          benchAttrID(i),
			Etype:       benchStr(etypes[i%len(etypes)]),
			Label:       benchStr(label),
			ValueType:   "blob",
			Cardinality: "one",
		}
		if i%2 == 0 {
			a.ValueType = "ref"
		}
		cat.Add(a)
	}

	rawQ := json.RawMessage(`{"todos":{}}`)
	sub := &reactive.Subscription{
		ID:     "grp-bench-cat",
		AppID:  "bench-app",
		Query:  rawQ,
		Topics: map[string]bool{"attr-a": true},
	}
	g := &queryGroup{
		key: sub.ID, class: wireNodelist, appID: sub.AppID, sub: sub, cat: cat,
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
		s := &Session{
			ID:      fmt.Sprintf("sess-%d", i),
			AppID:   "bench-app",
			Subs:    map[string]bool{},
			SendRaw: func(b []byte) error { sent++; return nil },
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
