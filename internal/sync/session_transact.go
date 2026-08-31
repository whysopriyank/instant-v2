package sync

// WebSocket transaction validation, execution, and commit notification.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/instant-v2/instant-v2/internal/metrics"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/tracing"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func (m *Manager) handleTransact(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	// WS frames carry no trace context; each op is a fresh root span. The
	// span covers parse→commit→notify so a waterfall shows the full chain.
	ctx, span := tracing.Tracer.Start(ctx, "transact.ws")
	defer span.End()
	rawSteps, ok := f["tx-steps"]
	if !ok {
		return []Frame{ErrFrame(400, "bad-request", "transact requires tx-steps")}, nil
	}
	// Per-app write budget: shed before parsing/DB work.
	if m.Deps.Limiter != nil {
		if ok2, retry := m.Deps.Limiter.Allow(sess.AppID, "transact"); !ok2 {
			return []Frame{ErrFrame(429, "rate-limited",
				fmt.Sprintf("rate limited; retry after %s", retry.Round(time.Millisecond)))}, nil
		}
	}
	// Shed before doing any work: an overloaded drain loop must shed
	// publishers at the gate, not after they burned a Postgres txn
	// (docs/reference/09-tier2-architecture.md §T2.1).
	if m.Deps.TransactGate != nil {
		if gerr := m.Deps.TransactGate(sess.AppID); gerr != nil {
			var shed *reactive.ShedError
			hint := "server busy"
			if errors.As(gerr, &shed) && shed.RetryAfter > 0 {
				hint = fmt.Sprintf("server busy; retry after %s", shed.RetryAfter)
			}
			return []Frame{ErrFrame(429, "shed", hint)}, nil
		}
	}
	var steps []json.RawMessage
	if err := json.Unmarshal(rawSteps, &steps); err != nil {
		return []Frame{ErrFrame(400, "bad-request", "tx-steps must be an array")}, nil
	}
	parsed, err := transact.ParseSteps(steps)
	if err != nil {
		return []Frame{ErrFrame(400, "tx-step-validation", err.Error())}, nil
	}
	cat, err := m.Deps.Catalogs.For(ctx, sess.AppID)
	if err != nil {
		return []Frame{ErrFrame(404, "unknown-app", "no such app")}, nil
	}
	appID := parseUUIDOrZero(sess.AppID)
	doc, rerr := m.rulesFor(ctx, sess.AppID)
	if rerr != nil {
		return []Frame{ErrFrame(503, "rules-unavailable",
			"permission rules temporarily unavailable; retry shortly")}, nil
	}
	opts := transact.Options{
		Admin:    sess.Admin,
		AuthUser: sess.AuthUser,
	}
	started := time.Now()
	res, err := transact.Transact(ctx, m.Deps.DB, cat, appID, parsed, opts, doc)
	metrics.TransactDuration.WithLabelValues("ws").Observe(time.Since(started).Seconds())
	if err != nil {
		return []Frame{ErrFrame(403, "transact-error", platform.ClientMessage(err))}, nil
	}
	if res.AttrsChanged && m.Deps.Catalogs != nil {
		m.Deps.Catalogs.Invalidate(sess.AppID)
	}
	if m.Deps.OnCommit != nil {
		var attrIDs []string
		for _, st := range parsed {
			if st.Op == "add-triple" || st.Op == "deep-merge-triple" || st.Op == "retract-triple" {
				if len(st.Args) >= 2 {
					var a string
					if json.Unmarshal(st.Args[1], &a) == nil && a != "" {
						attrIDs = append(attrIDs, a)
					}
				}
			}
		}
		// Change-routed path (docs/09 §T2.5): fully-resolved plain triple
		// writes carry entity identity so the incremental engine can splice;
		// anything else keeps the topic-wide Notify semantics.
		if m.Deps.OnCommitChanges != nil {
			if triples, ok := transact.ResolveTriples(parsed, cat); ok && len(triples) > 0 {
				changes := make([]reactive.Change, 0, len(triples))
				for _, tt := range triples {
					changes = append(changes, reactive.Change{
						Etype:    tt.Etype,
						EntityID: tt.EntityID,
						AttrIDs:  []string{tt.AttrID},
					})
				}
				m.Deps.OnCommitChanges(ctx, sess.AppID, changes, res.TxID, res.AttrsChanged)
			} else {
				m.Deps.OnCommit(ctx, sess.AppID, attrIDs, res.TxID, res.AttrsChanged)
			}
		} else {
			m.Deps.OnCommit(ctx, sess.AppID, attrIDs, res.TxID, res.AttrsChanged)
		}
	}
	txID, _ := f.String("client-event-id")
	fr := Frame{
		"op":    json.RawMessage(`"transact-ok"`),
		"tx-id": json.RawMessage(mustJSON(res.TxID)),
	}
	if txID != "" {
		fr["client-event-id"] = json.RawMessage(mustJSON(txID))
	}
	return []Frame{fr}, nil
}
