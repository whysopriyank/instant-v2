package perms_test

import (
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
)

func TestResolveExprFallbackChain(t *testing.T) {
	docJSON := []byte(`{
		"posts":  {"allow":{"view":"auth.id != null","$default":"false"},"fallback":{"delete":"auth.id == data.ownerId"}},
		"$default":{"allow":{"view":"true","$default":"false"}}
	}`)
	doc, err := perms.ParseRuleDoc(docJSON)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		etype, action, want string
	}{
		{"posts", "view", "auth.id != null"},
		{"posts", "create", "false"},
		{"posts", "delete", "false"},
		{"unknown", "view", "true"},
		{"unknown", "create", "false"},
	} {
		if got := doc.ResolveExpr(tc.etype, tc.action); got != tc.want {
			t.Fatalf("ResolveExpr(%q,%q)=%q want %q", tc.etype, tc.action, got, tc.want)
		}
	}
	// Fallback is only reached when the allow chain has no match.
	doc2, _ := perms.ParseRuleDoc([]byte(`{"posts":{"fallback":{"delete":"false"}}}`))
	if got := doc2.ResolveExpr("posts", "delete"); got != "false" {
		t.Fatalf("fallback only: %q", got)
	}
}

func TestParseRuleDocRateLimits(t *testing.T) {
	raw := []byte(`{"$rateLimits":{"chat":{"capacity":10,"refill":{"amount":5,"period":"1m","type":"greedy"}}}}`)
	doc, err := perms.ParseRuleDoc(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(doc.RateLimits) != 1 || doc.RateLimits["chat"].Capacity != 10 {
		t.Fatalf("rateLimits: %+v", doc.RateLimits)
	}
}

func TestFieldExprRejectsID(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"fields":{"secret":"false"}}}`))
	if _, err := doc.FieldExpr("posts", "id"); err == nil {
		t.Fatal("expected error for field id")
	}
	if expr, err := doc.FieldExpr("posts", "secret"); err != nil || expr != "false" {
		t.Fatalf("fields secret: %q err %v", expr, err)
	}
}

func TestBindsFor(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"$default":{"bind":["isOwner","auth.id == data.ownerId"]},"posts":{"bind":["isPublic","data.privacy == \"public\""]}}`))
	names, exprs, err := doc.BindsFor("posts")
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 || names[0] != "isOwner" || names[1] != "isPublic" {
		t.Fatalf("names: %v", names)
	}
	_ = exprs
	badDoc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"bind":["one","a","odd"]}}`))
	if _, _, err := badDoc.BindsFor("posts"); err == nil {
		t.Fatal("odd bind should error")
	}
}

func TestParseRuleDocEdgeCases(t *testing.T) {
	if d, err := perms.ParseRuleDoc([]byte(``)); err != nil || len(d.Etypes) != 0 {
		t.Fatalf("empty: %v err %v", d, err)
	}
	if d, err := perms.ParseRuleDoc([]byte(`null`)); err != nil || len(d.Etypes) != 0 {
		t.Fatalf("null: %v err %v", d, err)
	}
}

func TestResolveExprNoFallbackLeaksIntoAllow(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{
		"posts":{"allow":{"$default":"true"},"fallback":{"view":"false"}}
	}`))
	if got := doc.ResolveExpr("posts", "view"); got != "true" {
		t.Fatalf("allow should win over fallback: %q", got)
	}
	doc2, _ := perms.ParseRuleDoc([]byte(`{"posts":{"fallback":{"delete":"false"}}}`))
	if got := doc2.ResolveExpr("posts", "delete"); got != "false" {
		t.Fatalf("fallback: %q", got)
	}
}
