package sync

import (
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
)

// TestGateQueryAndCollectEtypes pins subscription view-gating: dynamic view
// rules are rejected pre-attach (shared groups cannot honor them without
// rule-where pushdown), closed rules pass through with the empty-result
// executor gate, and etype collection reaches every nesting level.
func TestGateQueryAndCollectEtypes(t *testing.T) {
	raw := json.RawMessage(`{"posts":{"comments":{}},"users":{"$":{"where":{"name":"x"}}}}`)
	got := collectEtypes(raw)
	want := map[string]bool{"posts": true, "comments": true, "users": true}
	if len(got) != len(want) {
		t.Fatalf("collectEtypes = %v", got)
	}
	for _, e := range got {
		if !want[e] {
			t.Fatalf("unexpected etype %q in %v", e, got)
		}
	}

	dyn, _ := perms.ParseRuleDoc([]byte(`{"comments":{"allow":{"view":"auth.id != null"}}}`))
	if err := gateQuery(dyn, raw); err == nil {
		t.Fatal("dynamic nested view rule must be rejected")
	}
	closed, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"false"}}}`))
	if err := gateQuery(closed, raw); err != nil {
		t.Fatalf("closed rules pass the attach gate (executor renders []): %v", err)
	}
	open, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"true"}}}`))
	if err := gateQuery(open, raw); err != nil {
		t.Fatalf("open rules must pass: %v", err)
	}
}

// TestGroupKeySplitsAdminFromPublic pins the fan-out isolation: the same
// query must land in different groups for admin and public sessions so
// admin-seeded snapshots can never leak gated etypes to public members.
func TestGroupKeySplitsAdminFromPublic(t *testing.T) {
	q := json.RawMessage(`{"posts":{}}`)
	a1 := groupKey("app-1", wireNodelist, q, true)
	p1 := groupKey("app-1", wireNodelist, q, false)
	a2 := groupKey("app-1", wireNodelist, q, true)
	if a1 == p1 {
		t.Fatal("admin and public sessions must not share a group")
	}
	if a1 != a2 {
		t.Fatal("same class+query must share a group")
	}
}
