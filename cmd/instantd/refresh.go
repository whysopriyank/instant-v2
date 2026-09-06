package main

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

// errRefreshSuperseded drops a generation that a concurrent rule change
// overtook mid-run (RT-001 publication-time validation). Callers already
// treat refresh errors as retryable, so this fails closed without a new
// handling path.
var errRefreshSuperseded = errors.New("instantd: refresh generation superseded by a concurrent rule change")

// rebindFunc reloads the current gate for a subscription (see
// sync.Manager.RefreshGate). A nil rebind preserves the legacy attach-time
// snapshot read and must only be used where no Manager exists yet; every
// production wiring replaces it with Manager.RefreshGate once built.
func refreshFor(ex *instaql.Executor, cats *platform.CatalogCache, rebind func(context.Context, *reactive.Subscription) (*syncpkg.QueryGate, bool, error)) func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
	return func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		q, err := instaql.Coerce(rawToMap(sub.Query))
		if err != nil {
			return nil, err
		}
		cat, err := cats.For(ctx, sub.AppID)
		if err != nil {
			return nil, err
		}
		appID, err := platform.ScanUUIDErr(sub.AppID)
		if err != nil {
			return nil, err
		}
		runner := ex
		gated := false
		var ranGate *syncpkg.QueryGate
		if rebind != nil {
			// RT-001: run under the newest LOADED doc. A reload failure
			// drops this generation (fail closed) instead of serving
			// under a possibly stale permissive snapshot.
			gate, _, rerr := rebind(ctx, sub)
			if rerr != nil {
				return nil, rerr
			}
			if gate != nil {
				runner = &instaql.Executor{DB: ex.DB, Rules: gate.Rules, Admin: gate.Admin}
				gated, ranGate = true, gate
			}
		} else if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
			// Reproduce the visibility the group was admitted under: closed
			// view rules render empty; dynamic ones were rejected pre-attach.
			runner = &instaql.Executor{DB: ex.DB, Rules: gate.Rules, Admin: gate.Admin}
		}
		res, err := runner.Run(ctx, q, cat, appID)
		if err != nil {
			return nil, err
		}
		if gated {
			// RT-001 publication-time validation: a concurrent re-gate
			// may have landed while the executor ran. Re-read the gate;
			// proceeding under a superseded one would publish
			// stale-authorized state. The re-read is side-effect-free
			// when nothing changed (same content returns the same
			// gate), and dropping retries through the existing paths.
			now, _, rerr := rebind(ctx, sub)
			if rerr != nil {
				return nil, rerr
			}
			if now == nil || syncpkg.GateHash(now.Rules) != syncpkg.GateHash(ranGate.Rules) {
				return nil, errRefreshSuperseded
			}
		}
		return json.Marshal(res)
	}
}

// rebindChanged adapts Manager.RefreshGate to the notifier's Revalidate
// hook: a re-gated subscription skips incremental splicing and takes the
// full recompute path (which runs under the new gate), so spliced state
// admitted under stale rules can never emit. A reload failure drops the
// generation fail-closed.
func rebindChanged(mgr *syncpkg.Manager) func(context.Context, *reactive.Subscription) (bool, error) {
	return func(ctx context.Context, sub *reactive.Subscription) (bool, error) {
		_, changed, err := mgr.RefreshGate(ctx, sub)
		return changed, err
	}
}

func rawToMap(raw json.RawMessage) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

// gateFor returns the transact shed gate, or nil when shedding is disabled
// (MaxQueueDepth <= 0 preserves pre-T2 unbounded-queue behavior).
func gateFor(n *reactive.Notifier, maxDepth int64) func(appID string) error {
	if n == nil || maxDepth <= 0 {
		return nil
	}
	return n.Gate(maxDepth)
}
