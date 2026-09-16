package adminapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	highLevel := transact.HasHighLevelOps(stepsRaw)
	attempt := func(base *platform.AttrCatalog) (transact.Result, []transact.Step, *platform.AttrCatalog, error) {
		lowered := stepsRaw
		cat := base
		if highLevel {
			request := *a
			request.cat = base
			var err error
			lowered, cat, err = h.lowerAdminSteps(ctx, &request, stepsRaw, throwOnMissing)
			if err != nil {
				return transact.Result{}, nil, nil, err
			}
		}
		steps, err := transact.ParseSteps(lowered)
		if err != nil {
			return transact.Result{}, nil, nil, err
		}
		res, err := transact.Transact(ctx, h.DB, cat, a.appID, steps, transact.Options{Admin: true}, nil)
		return res, steps, cat, err
	}
	started := time.Now()
	res, steps, cat, err := attempt(a.cat)
	if err != nil && highLevel && isAttrProvisioningConflict(err) {
		// A concurrent request may have committed the same missing identity
		// after this request lowered its steps. The failed transaction is fully
		// rolled back; reload once and lower against the winning attr.
		h.Catalogs.Invalidate(a.appStr)
		if fresh, loadErr := h.Catalogs.For(ctx, a.appStr); loadErr == nil {
			res, steps, cat, err = attempt(fresh)
		} else {
			err = loadErr
		}
	}
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

func isAttrProvisioningConflict(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	if pgErr.Code == "P0001" && pgErr.Message == "trigger violation trg_attrs_unique_names" {
		return true
	}
	if pgErr.Code != "23505" {
		return false
	}
	switch pgErr.ConstraintName {
	case "attrs_etype_label_unique", "attrs_reverse_etype_label_unique", "app_ident_uq":
		return true
	default:
		return false
	}
}

// lowerAdminSteps runs the instant.admin.model lowering (see
// internal/transact/highlevel.go). Missing attrs become ordinary add-attr
// steps in the same Transact call as the requested writes, so schema and data
// either commit together or both roll back. Pure-low-level batches never reach
// this path.
func (h *Handler) lowerAdminSteps(
	ctx context.Context, a *authedReq, raw []json.RawMessage, throwOnMissing bool,
) ([]json.RawMessage, *platform.AttrCatalog, error) {
	var provision []json.RawMessage
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
			attrID, err := newAdminUUID()
			if err != nil {
				return platform.Attr{}, err
			}
			forwardID, err := newAdminUUID()
			if err != nil {
				return platform.Attr{}, err
			}
			payload := map[string]any{
				"id":               platform.UUIDToStr(attrID),
				"forward-identity": []string{platform.UUIDToStr(forwardID), spec.Etype, spec.Label},
				"value-type":       spec.ValueType,
				"cardinality":      spec.Cardinality,
				"unique?":          spec.Unique,
				"index?":           spec.Indexed,
			}
			attr := platform.Attr{
				ID: attrID, AppID: appID, ValueType: spec.ValueType,
				Cardinality: spec.Cardinality, IsUnique: spec.Unique,
				IsIndexed: spec.Indexed || spec.Unique, ForwardIdent: forwardID,
			}
			etype, label := spec.Etype, spec.Label
			attr.Etype, attr.Label = &etype, &label
			if spec.ReverseEtype != nil && spec.ReverseLabel != nil {
				reverseID, err := newAdminUUID()
				if err != nil {
					return platform.Attr{}, err
				}
				reverseEtype, reverseLabel := *spec.ReverseEtype, *spec.ReverseLabel
				payload["reverse-identity"] = []string{platform.UUIDToStr(reverseID), reverseEtype, reverseLabel}
				attr.ReverseIdent = &reverseID
				attr.ReverseEtype, attr.ReverseLabel = &reverseEtype, &reverseLabel
			}
			step, err := json.Marshal([]any{"add-attr", payload})
			if err != nil {
				return platform.Attr{}, err
			}
			provision = append(provision, step)
			return attr, nil
		},
	}

	lowered, err := transact.LowerAdminSteps(ctx, a.appID, a.cat, raw, hooks, throwOnMissing)
	if err != nil {
		return nil, nil, err
	}
	return append(provision, lowered...), a.cat, nil
}

func newAdminUUID() ([16]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return id, err
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return id, nil
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
