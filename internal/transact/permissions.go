package transact

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func etypeFor(cat *platform.AttrCatalog, attrID [16]byte) string {
	if a, ok := cat.ByID(attrID); ok {
		if a.Etype != nil {
			return *a.Etype
		}
	}
	return "$default"
}

// entityProjection projects one entity's committed triples into a
// label→value map (the `data` binding v1 rules see). Cardinality-many attrs
// accumulate as arrays; unknown attrs are skipped.
type permissionFetcher func(context.Context, pgx.Tx, [16]byte, storage.FetchFilter) ([]storage.Enhanced, error)

func entityProjectionWithFetcher(ctx context.Context, tx pgx.Tx, appID [16]byte, eid [16]byte, cat *platform.AttrCatalog, fetch permissionFetcher) (map[string]any, error) {
	out := map[string]any{}
	rows, err := fetch(ctx, tx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{eid},
	})
	if err != nil {
		return nil, fmt.Errorf("fetch entity projection: %w", err)
	}
	for _, r := range rows {
		a, ok := cat.ByID(r.Triple.A)
		if !ok || a.Label == nil {
			continue
		}
		label := *a.Label
		if a.Cardinality == "many" {
			arr, _ := out[label].([]any)
			out[label] = append(arr, r.Triple.V)
		} else {
			out[label] = r.Triple.V
		}
	}
	return out, nil
}

func permBindings(opts Options, data, newData map[string]any) perms.Bindings {
	return perms.Bindings{
		Data:       data,
		NewData:    newData,
		Auth:       opts.AuthUser,
		RuleParams: opts.RuleParams,
		Request:    opts.Request,
	}
}

// enforcePerms checks every mutating step in the batch against doc. Runs
// inside the write transaction after lookup resolution so existence probes
// and data projections see same-batch state. Any check error or denial
// aborts the whole transaction (fail-closed).
func enforcePerms(ctx context.Context, tx pgx.Tx, appID [16]byte,
	cat *platform.AttrCatalog, steps []Step, opts Options, doc *perms.RuleDoc, fetch permissionFetcher,
) error {
	for _, op := range orderSteps(steps) {
		switch op {
		case "add-triple", "deep-merge-triple":
			for _, st := range filterSteps(steps, op) {
				ta, err := parseTripleArgs(st, cat)
				if err != nil {
					return err
				}
				var eid [16]byte
				if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
					return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
				}
				action := "create"
				existing, err := fetch(ctx, tx, appID, storage.FetchFilter{
					EntityIDs: [][16]byte{eid}, AttrIDs: [][16]byte{ta.AttrID},
				})
				if err != nil {
					return err
				}
				if len(existing) > 0 {
					action = "update"
				}
				etype := etypeFor(cat, ta.AttrID)
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				newData := cloneMap(data)
				incoming, err := parseValueJSON(ta.Value)
				if err != nil {
					return err
				}
				if label := attrLabel(cat, ta.AttrID); label != "" {
					newData[label] = incoming
				}
				allow, err := perms.Check(etype, action, doc, permBindings(opts, data, newData))
				if err != nil {
					return fmt.Errorf("perms %s %s: %w", etype, action, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (%s %s)", action, etype)
				}
			}

		case "retract-triple":
			for _, st := range filterSteps(steps, op) {
				ta, err := parseTripleArgs(st, cat)
				if err != nil {
					return err
				}
				var eid [16]byte
				if err := parseUUID(strings.Trim(string(ta.EID), `"`), &eid); err != nil {
					return fmt.Errorf("perms: eid %s: %w", ta.EID, err)
				}
				etype := etypeFor(cat, ta.AttrID)
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				newData := cloneMap(data)
				if label := attrLabel(cat, ta.AttrID); label != "" {
					delete(newData, label) // post-retract state approximation
				}
				allow, err := perms.Check(etype, "delete", doc, permBindings(opts, data, newData))
				if err != nil {
					return fmt.Errorf("perms %s delete: %w", etype, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (delete %s)", etype)
				}
			}

		case "delete-entity":
			for _, st := range filterSteps(steps, op) {
				var eidStr string
				if err := json.Unmarshal(st.Args[0], &eidStr); err != nil {
					continue // non-uuid eids never reached applyDeleteEntity either
				}
				var eid [16]byte
				if err := parseUUID(eidStr, &eid); err != nil {
					continue
				}
				etype := "$default"
				if len(st.Args) == 2 {
					var e string
					if json.Unmarshal(st.Args[1], &e) == nil && e != "" {
						etype = e
					}
				}
				data, err := entityProjectionWithFetcher(ctx, tx, appID, eid, cat, fetch)
				if err != nil {
					return fmt.Errorf("perms %s projection: %w", etype, err)
				}
				allow, err := perms.Check(etype, "delete", doc, permBindings(opts, data, map[string]any{}))
				if err != nil {
					return fmt.Errorf("perms %s delete: %w", etype, err)
				}
				if !allow {
					return fmt.Errorf("transact: permission denied (delete %s)", etype)
				}
			}
		}
	}
	return nil
}

func cloneMap(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func attrLabel(cat *platform.AttrCatalog, id [16]byte) string {
	if a, ok := cat.ByID(id); ok && a.Label != nil {
		return *a.Label
	}
	return ""
}
