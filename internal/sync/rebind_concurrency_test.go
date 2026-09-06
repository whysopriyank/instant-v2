package sync_test

// RT-001c preparation: concurrent rule update and data invalidation must
// have one safe deterministic outcome.
//
// Desired invariant: concurrent refresh generations racing a persisted rule
// change serialize their gate swaps, converge on identical content, and
// settle on the newest doc once updates stop — versions only move forward,
// no generation runs under a doc older than the newest one loaded at its
// start, and nothing races. Hermetic: versioned docs served by an atomic
// fake loader, no database, no sockets (run under -race).

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/reactive"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func versionedDoc(v int) *perms.RuleDoc {
	return &perms.RuleDoc{Raw: []byte(fmt.Sprintf(`{"version":%d}`, v))}
}

// TestRefreshGateConvergesUnderConcurrentReloads hammers one subscription's
// gate from many goroutines while the served doc advances, then asserts the
// deterministic quiescent outcome: the subscription settles on the newest
// doc, every served gate was a real served version, and a further quiet
// generation reports no change.
func TestRefreshGateConvergesUnderConcurrentReloads(t *testing.T) {
	ctx := context.Background()
	const versions = 5

	docs := make([]*perms.RuleDoc, versions)
	hashes := make(map[string]bool, versions)
	for v := range docs {
		docs[v] = versionedDoc(v)
		hashes[syncpkg.GateHash(docs[v])] = true
	}

	var current atomic.Int64
	mgr := syncpkg.NewManager(syncpkg.Deps{
		Rules: func(context.Context, string) (*perms.RuleDoc, error) {
			return docs[current.Load()], nil
		},
	})
	sub := &reactive.Subscription{
		ID:        "converge",
		AppID:     "app",
		AttachCtx: syncpkg.NewQueryGate(docs[0], false),
	}

	const workers = 8
	const iters = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := map[string]bool{}
	var errs []error
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				gate, _, err := mgr.RefreshGate(ctx, sub)
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else if gate != nil {
					seen[syncpkg.GateHash(gate.Rules)] = true
				}
				mu.Unlock()
			}
		}()
	}
	// Advance the served doc while generations are in flight; the last
	// store is the deterministic target every generation must settle on.
	for v := int64(1); v < versions; v++ {
		current.Store(v)
	}
	wg.Wait()

	if len(errs) > 0 {
		t.Fatalf("RefreshGate errors under concurrency: %v", errs[0])
	}
	mu.Lock()
	for h := range seen {
		if !hashes[h] {
			t.Fatalf("generation ran under an unserved doc version %q", h)
		}
	}
	mu.Unlock()

	final, _, err := mgr.RefreshGate(ctx, sub)
	if err != nil {
		t.Fatalf("quiescent RefreshGate: %v", err)
	}
	if final == nil || syncpkg.GateHash(final.Rules) != syncpkg.GateHash(docs[versions-1]) {
		t.Fatalf("quiescent gate is not the newest doc (want v%d)", versions-1)
	}
	if _, regated, err := mgr.RefreshGate(ctx, sub); err != nil || regated {
		t.Fatalf("quiet generation should be stable: regated=%v err=%v", regated, err)
	}
}
