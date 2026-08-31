package main

import (
	"context"
	"encoding/json"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
)

func refreshFor(ex *instaql.Executor, cats *platform.CatalogCache) func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
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
		if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
			// Reproduce the visibility the group was admitted under: closed
			// view rules render empty; dynamic ones were rejected pre-attach.
			runner = &instaql.Executor{DB: ex.DB, Rules: gate.Rules, Admin: gate.Admin}
		}
		res, err := runner.Run(ctx, q, cat, appID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(res)
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
