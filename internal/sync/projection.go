package sync

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// UnwrapTree extracts the bare object-tree ({etype:[entities…]}) used by the
// SSE init-query path — v1 session.clj:1395 requests :tree semantics there,
// and instaql-nodes->object-tree returns the tree WITHOUT a "data" wrapper.
func UnwrapTree(result json.RawMessage) (json.RawMessage, error) {
	var env struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(result, &env); err != nil {
		return nil, fmt.Errorf("nodelist: bad instaql-result envelope: %w", err)
	}
	out := make(map[string]json.RawMessage, len(env.Data))
	for k, v := range env.Data {
		if len(v) == 0 || string(v) == "null" {
			out[k] = json.RawMessage(`[]`)
			continue
		}
		out[k] = v
	}
	return json.Marshal(out)
}

// nodelistFor resolves the session's catalog and projects a flat instaql
// result into the v1 node-list wire shape.
func nodelistFor(ctx context.Context, catalogs *platform.CatalogCache, appID string, result json.RawMessage) (json.RawMessage, error) {
	cat, err := catalogs.For(ctx, appID)
	if err != nil {
		return nil, err
	}
	return BuildNodeList(cat, result)
}
