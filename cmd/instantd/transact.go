package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/tracing"
	"github.com/instant-v2/instant-v2/internal/transact"
)

// writeJSONError emits a safely-encoded {"error": ...} body. Never build
// error JSON by string concatenation — messages contain client input and
// provider strings that must not break the envelope.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// transactHandler runs one runtime transaction batch.
func transactHandler(st *storage.DB, cats *platform.CatalogCache,
	invalidate func(ctx context.Context, appID string, attrIDs []string, txID int64, attrsChanged bool),
	invalidateChanges func(ctx context.Context, appID string, changes []reactive.Change, txID int64, attrsChanged bool),
	gate func(appID string) error, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracing.Tracer.Start(ctx, "transact.runtime")
		defer span.End()
		var req struct {
			AppID string            `json:"app-id"`
			Steps []json.RawMessage `json:"tx-steps"`
			Rules bool              `json:"-"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad json"}`, 400)
			return
		}
		// Overload gate (docs/09 §T2.1): 429 + Retry-After before any
		// Postgres work, mirroring the WS shed frame.
		if gate != nil {
			if gerr := gate(req.AppID); gerr != nil {
				var shed *reactive.ShedError
				retry := time.Second
				if errors.As(gerr, &shed) && shed.RetryAfter > 0 {
					retry = shed.RetryAfter
				}
				w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
				http.Error(w, `{"error":"server busy"}`, http.StatusTooManyRequests)
				return
			}
		}
		parsed, err := transact.ParseSteps(req.Steps)
		if err != nil {
			writeJSONError(w, 400, err.Error())
			return
		}
		cat, err := cats.For(r.Context(), req.AppID)
		if err != nil {
			http.Error(w, `{"error":"unknown app"}`, 404)
			return
		}
		appID, err := platform.ScanUUIDErr(req.AppID)
		if err != nil {
			http.Error(w, `{"error":"bad app id"}`, 400)
			return
		}
		doc, derr := cats.RuleDocFor(r.Context(), req.AppID)
		if derr != nil {
			// Fail closed: an unloadable rule doc must never widen access.
			logger.Error("transact: rules load failed", "app", req.AppID, "err", derr)
			writeJSONError(w, http.StatusInternalServerError, "internal error")
			return
		}
		started := time.Now()
		res, err := transact.Transact(ctx, st, cat, appID, parsed, transact.Options{}, doc)
		metrics.TransactDuration.WithLabelValues("runtime").Observe(time.Since(started).Seconds())
		if err != nil {
			writeJSONError(w, http.StatusForbidden, platform.ClientMessage(err))
			return
		}
		if res.AttrsChanged {
			cats.Invalidate(req.AppID)
		}
		// Post-commit invalidation on every node (docs/09 §T2.4); the
		// change-routed variant feeds the incremental engine (§T2.5).
		if invalidateChanges != nil {
			if triples, ok := transact.ResolveTriples(parsed, cat); ok && len(triples) > 0 {
				changes := make([]reactive.Change, 0, len(triples))
				for _, tt := range triples {
					changes = append(changes, reactive.Change{
						Etype: tt.Etype, EntityID: tt.EntityID, AttrIDs: []string{tt.AttrID},
					})
				}
				go invalidateChanges(context.WithoutCancel(r.Context()), req.AppID, changes, res.TxID, res.AttrsChanged)
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{"tx-id": res.TxID})
				return
			}
		}
		attrsTouched := touchedAttrs(parsed, cat)
		go invalidate(context.WithoutCancel(r.Context()), req.AppID, attrsTouched, res.TxID, res.AttrsChanged)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"tx-id": res.TxID})
	}
}

func touchedAttrs(steps []transact.Step, _ *platform.AttrCatalog) []string {
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
