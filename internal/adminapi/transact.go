package adminapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/tracing"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// ---- Transact --------------------------------------------------------------

// handleTransact ports transact-post: {"steps": [...]} → {"tx-id": N} with
// transact.Options{Admin: true}. DEVIATION: v1 loads persisted rules
// (rule-model/get-by-app-id) into the permissioned transaction; rules
// persistence does not exist yet in v2, so the admin path runs without a
func (h *Handler) handleTransact(w http.ResponseWriter, r *http.Request, a *authedReq) {
	ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := tracing.Tracer.Start(ctx, "transact.admin")
	defer span.End()
	stepsRaw, err := stepsFromBody(a.body)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	throwOnMissing := false
	if v, ok := a.body["throw-on-missing-attrs?"].(bool); ok {
		throwOnMissing = v
	}
	cat := a.cat
	if transact.HasHighLevelOps(stepsRaw) {
		var lerr error
		stepsRaw, cat, lerr = h.lowerAdminSteps(ctx, a, stepsRaw, throwOnMissing)
		if lerr != nil {
			writeErr(w, http.StatusBadRequest, lerr.Error())
			return
		}
	}
	steps, err := transact.ParseSteps(stepsRaw)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	started := time.Now()
	res, err := transact.Transact(ctx, h.DB, cat, a.appID, steps,
		transact.Options{Admin: true}, nil)
	metrics.TransactDuration.WithLabelValues("admin").Observe(time.Since(started).Seconds())
	if err != nil {
		h.logger().Error("adminapi: transact", "err", err)
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if res.AttrsChanged {
		h.Catalogs.Invalidate(a.appStr)
	}
	var changes []reactive.Change
	if h.OnCommitChanges != nil {
		if triples, ok := transact.ResolveTriples(steps, cat); ok && len(triples) > 0 {
			changes = make([]reactive.Change, 0, len(triples))
			for _, tt := range triples {
				changes = append(changes, reactive.Change{
					Etype: tt.Etype, EntityID: tt.EntityID, AttrIDs: []string{tt.AttrID},
				})
			}
		}
	}
	if len(changes) > 0 {
		h.OnCommitChanges(ctx, a.appID, changes, res.TxID, res.AttrsChanged)
	} else if h.OnCommit != nil {
		h.OnCommit(r.Context(), a.appID, touchedAttrs(steps), res.TxID, res.AttrsChanged)
	}
	writeJSON(w, http.StatusOK, map[string]any{"tx-id": res.TxID})
}

// lowerAdminSteps runs the instant.admin.model lowering (see
// internal/transact/highlevel.go): it resolves lookup eids against committed
// state and provisions missing attrs via platform.GetOrCreateAttr(+Rev),
// mirroring how authn creates system attrs. Provisioning commits in its own
// transaction and the catalog cache is invalidated + reloaded so Transact's
// parseTripleArgs sees the fresh attr ids. Pure-low-level batches never reach
// this path.
func (h *Handler) lowerAdminSteps(
	ctx context.Context, a *authedReq, raw []json.RawMessage, throwOnMissing bool,
) ([]json.RawMessage, *platform.AttrCatalog, error) {
	var (
		tx      pgx.Tx
		created int
	)
	hooks := transact.LowerHooks{
		ResolveUnique: func(ctx context.Context, appID [16]byte, attrID [16]byte, value json.RawMessage) ([16]byte, bool, error) {
			var eid [16]byte
			err := h.Pool.QueryRow(ctx,
				`SELECT entity_id FROM triples
				  WHERE app_id=$1 AND attr_id=$2 AND av AND value=$3::jsonb
				  LIMIT 1`, appID, attrID, string(value)).Scan(&eid)
			if errors.Is(err, pgx.ErrNoRows) {
				return [16]byte{}, false, nil
			}
			if err != nil {
				return [16]byte{}, false, err
			}
			return eid, true, nil
		},
		EntityExists: func(ctx context.Context, appID [16]byte, eid [16]byte) (bool, error) {
			var one int
			err := h.Pool.QueryRow(ctx,
				`SELECT 1 FROM triples WHERE app_id=$1 AND entity_id=$2 LIMIT 1`,
				appID, eid).Scan(&one)
			if errors.Is(err, pgx.ErrNoRows) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		},
		CreateAttr: func(ctx context.Context, appID [16]byte, spec transact.AttrSpec) (platform.Attr, error) {
			if tx == nil {
				var err error
				tx, err = h.Pool.Begin(ctx)
				if err != nil {
					return platform.Attr{}, err
				}
			}
			created++
			if spec.Ref {
				return platform.GetOrCreateAttrRev(ctx, tx, appID,
					spec.Etype, spec.Label, spec.ReverseEtype, spec.ReverseLabel,
					spec.ValueType, spec.Cardinality, spec.Unique, spec.Indexed)
			}
			return platform.GetOrCreateAttr(ctx, tx, appID,
				spec.Etype, spec.Label, spec.ValueType, spec.Cardinality,
				spec.Unique, spec.Indexed)
		},
	}

	lowered, err := transact.LowerAdminSteps(ctx, a.appID, a.cat, raw, hooks, throwOnMissing)
	if tx != nil {
		if err != nil {
			_ = tx.Rollback(ctx)
			return nil, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, nil, err
		}
	}
	if err != nil {
		return nil, nil, err
	}
	if created == 0 {
		return lowered, a.cat, nil
	}
	// Attrs were provisioned: drop the stale cached catalog and reload so the
	// freshly minted attr ids resolve inside Transact.
	h.Catalogs.Invalidate(a.appStr)
	fresh, err := h.Catalogs.For(ctx, a.appStr)
	if err != nil {
		return nil, nil, err
	}
	return lowered, fresh, nil
}

func stepsFromBody(body map[string]any) ([]json.RawMessage, error) {
	arr, ok := body["steps"].([]any)
	if !ok {
		return nil, errors.New("missing or invalid `steps` array")
	}
	out := make([]json.RawMessage, 0, len(arr))
	for _, el := range arr {
		b, err := json.Marshal(el)
		if err != nil {
			return nil, fmt.Errorf("invalid step: %w", err)
		}
		out = append(out, b)
	}
	return out, nil
}

// touchedAttrs extracts attr-id strings from triple-shaped steps so callers
// can invalidate reactive subscriptions (mirrors cmd's touchedAttrs).
func touchedAttrs(steps []transact.Step) []string {
	var out []string
	for _, s := range steps {
		switch s.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if len(s.Args) >= 2 {
				var attrStr string
				if json.Unmarshal(s.Args[1], &attrStr) == nil {
					out = append(out, attrStr)
				}
			}
		}
	}
	return out
}
