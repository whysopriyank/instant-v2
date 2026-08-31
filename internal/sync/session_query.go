package sync

// Query admission, permission gates, and invalidation-topic projection.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

// QueryGate is the permission snapshot stored on every subscription at
// attach time (Subscription.AttachCtx). Refresh executors rebuild the same
// visibility the group was admitted under: Rules drives instaql's view gate
// (closed etypes render empty; dynamic ones were rejected pre-attach for
// non-admin callers), Admin bypasses.
type QueryGate struct {
	Rules *perms.RuleDoc
	Admin bool
}

// collectEtypes walks a raw InstaQL body and returns every etype name at any
// nesting level (the "$" options key excluded).
func collectEtypes(rawQ json.RawMessage) []string {
	var q map[string]any
	if err := json.Unmarshal(rawQ, &q); err != nil {
		return nil
	}
	var out []string
	var walk func(map[string]any)
	seen := map[string]bool{}
	walk = func(m map[string]any) {
		for k, v := range m {
			if k == "$" || seen[k] {
				continue
			}
			seen[k] = true
			out = append(out, k)
			if child, ok := v.(map[string]any); ok {
				walk(child)
			}
		}
	}
	walk(q)
	return out
}

// gateQuery enforces view rules for a would-be subscription. Returns:
//   - err non-nil → dynamic view rule and caller is not admin (reject).
//
// Closed etypes are allowed through with an empty-result gate: refreshes
// render [] via instaql's Executor, so live updates never leak denied data.
func gateQuery(doc *perms.RuleDoc, rawQ json.RawMessage) error {
	for _, etype := range collectEtypes(rawQ) {
		if perms.ViewGate(doc, etype) == perms.ViewDynamic {
			return &instaql.ErrRuleFilterUnsupported{Etype: etype}
		}
	}
	return nil
}

func (m *Manager) handleAddQuery(ctx context.Context, sess *Session, f Frame) ([]Frame, error) {
	rawQ, ok := f["q"]
	if !ok {
		return []Frame{ErrFrame(400, "bad-request", "add-query requires q")}, nil
	}
	eventID, _ := f.String("client-event-id")

	class := wireNodelist
	if sess.TreeResults {
		class = wireTree
	}
	key := groupKey(sess.AppID, class, rawQ, sess.Admin)

	// Same session re-adding its own query → v1 add-query-exists.
	sess.mu.Lock()
	dup := sess.Subs[key]
	sess.mu.Unlock()
	if dup {
		fr := Frame{"op": json.RawMessage(`"add-query-exists"`)}
		if eventID != "" {
			fr["client-event-id"] = json.RawMessage(mustJSON(eventID))
		}
		return []Frame{fr}, nil
	}

	// Compile topics + catalog BEFORE touching the registry (same order as
	// the pre-group code; failures never create groups).
	topics, err := m.topicsFor(ctx, sess.AppID, rawQ)
	if err != nil {
		return []Frame{ErrFrame(400, "invalid-query", err.Error())}, nil
	}
	cat, err := m.Deps.Catalogs.For(ctx, sess.AppID)
	if err != nil {
		return []Frame{ErrFrame(404, "unknown-app", "no such app")}, nil
	}

	// View-rule gate: dynamic rules cannot be honored on shared subscriptions
	// (no rule-where pushdown yet) — refuse them for non-admin callers rather
	// than leak. Closed rules proceed with an empty-result gate.
	doc, rerr := m.rulesFor(ctx, sess.AppID)
	if rerr != nil {
		return []Frame{ErrFrame(503, "rules-unavailable",
			"permission rules temporarily unavailable; retry shortly")}, nil
	}
	if !sess.Admin {
		if gerr := gateQuery(doc, rawQ); gerr != nil {
			return []Frame{ErrFrame(400, "invalid-query", gerr.Error())}, nil
		}
	}

	// Attach (creates the shared subscription on first use). Cap breaches
	// reject with the exact 429 protocol frames and close the connection.
	if _, aerr := m.attachGroup(sess, rawQ, topics, cat, class, doc); aerr != nil {
		var capErr *reactive.SubLimitError
		if errors.As(aerr, &capErr) {
			return []Frame{ErrFrame(429, "subscription-limit",
					fmt.Sprintf("per-app subscription cap (%d) exceeded", capErr.Max))},
				fmt.Errorf("%w: %v", ErrCloseSession, aerr)
		}
		return []Frame{ErrFrame(500, "internal", platform.ClientMessage(aerr))}, aerr
	}

	reply := Frame{"op": json.RawMessage(`"add-query-ok"`)}
	if eventID != "" {
		reply["client-event-id"] = json.RawMessage(mustJSON(eventID))
	}
	return []Frame{reply}, nil
}

// topicsFor compiles the query to its attr-id topic set.
func (m *Manager) topicsFor(ctx context.Context, appID string, rawQ json.RawMessage) (map[string]bool, error) {
	cat, err := m.Deps.Catalogs.For(ctx, appID)
	if err != nil {
		return nil, err
	}
	var q map[string]any
	if err := json.Unmarshal(rawQ, &q); err != nil {
		return nil, err
	}
	topics := map[string]bool{}
	var walk func(prefixEtype string, node map[string]any) error
	walk = func(etype string, node map[string]any) error {
		for k, v := range node {
			if k == "$" {
				if wm, ok := v.(map[string]any); ok {
					if w, ok := wm["where"].(map[string]any); ok {
						for label := range w {
							if a := cat.FindByEtypeLabel(etype, label); a != nil {
								topics[platform.UUIDToStr(a.ID)] = true
							}
						}
					}
				}
				continue
			}
			child, ok := v.(map[string]any)
			if !ok {
				child = map[string]any{}
			}
			// child level invalidates on the link attr of this etype too
			if a := cat.FindByEtypeLabel(etype, k); a != nil {
				topics[platform.UUIDToStr(a.ID)] = true
			}
			if err := walk(k, child); err != nil {
				return err
			}
		}
		// The level itself depends on every attr of its etype (new entities).
		for _, a := range cat.ByEtype(etype) {
			topics[a] = true
		}
		return nil
	}
	if err := walk("", q); err != nil {
		return nil, err
	}
	if len(topics) == 0 {
		// conservative fallback: invalidate on everything of known etypes
		for etype := range q {
			for _, a := range cat.ByEtype(etype) {
				topics[a] = true
			}
		}
	}
	return topics, nil
}
