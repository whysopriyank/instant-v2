package perms_test

import (
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
)

// TestViewGate_NilDoc ensures nil rule doc evaluates to ViewOpen (empty doc => permissive).
func TestViewGate_NilDoc(t *testing.T) {
	for _, etype := range []string{"posts", "comments", "", "$users"} {
		if got := perms.ViewGate(nil, etype); got != perms.ViewOpen {
			t.Fatalf("ViewGate(nil, %q) = %v; want ViewOpen (%v)", etype, got, perms.ViewOpen)
		}
	}
}

// TestViewGate_EmptyDoc ensures rule docs with no rules or empty JSON evaluate to ViewOpen.
func TestViewGate_EmptyDoc(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"empty-object", `{}`},
		{"null", `null`},
		{"empty-string", ``},
		{"unrelated-etype", `{"comments":{"allow":{"view":"false"}}}`},
		{"empty-allow", `{"posts":{"allow":{}}}`},
		{"empty-view-expr", `{"posts":{"allow":{"view":""}}}`},
		{"other-action-only", `{"posts":{"allow":{"create":"false","delete":"false"}}}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := perms.ParseRuleDoc([]byte(tc.raw))
			if err != nil {
				t.Fatalf("ParseRuleDoc failed: %v", err)
			}
			if got := perms.ViewGate(doc, "posts"); got != perms.ViewOpen {
				t.Fatalf("ViewGate(%s, posts) = %v; want ViewOpen (%v)", tc.name, got, perms.ViewOpen)
			}
		})
	}
}

// TestViewGate_LiteralTrue verifies that literal "true" view rules resolve to ViewOpen
// across all positions in the resolution chain.
func TestViewGate_LiteralTrue(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{
			name: "etype-allow-view",
			json: `{"posts":{"allow":{"view":"true"}}}`,
		},
		{
			name: "etype-allow-default",
			json: `{"posts":{"allow":{"$default":"true"}}}`,
		},
		{
			name: "default-allow-view",
			json: `{"$default":{"allow":{"view":"true"}}}`,
		},
		{
			name: "default-allow-default",
			json: `{"$default":{"allow":{"$default":"true"}}}`,
		},
		{
			name: "etype-fallback-view",
			json: `{"posts":{"fallback":{"view":"true"}}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := perms.ParseRuleDoc([]byte(tc.json))
			if err != nil {
				t.Fatalf("ParseRuleDoc failed: %v", err)
			}
			if got := perms.ViewGate(doc, "posts"); got != perms.ViewOpen {
				t.Fatalf("ViewGate(%s) = %v; want ViewOpen (%v)", tc.name, got, perms.ViewOpen)
			}
		})
	}
}

// TestViewGate_LiteralFalse verifies that literal "false" view rules resolve to ViewClosed
// across all positions in the resolution chain.
func TestViewGate_LiteralFalse(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{
			name: "etype-allow-view",
			json: `{"posts":{"allow":{"view":"false"}}}`,
		},
		{
			name: "etype-allow-default",
			json: `{"posts":{"allow":{"$default":"false"}}}`,
		},
		{
			name: "default-allow-view",
			json: `{"$default":{"allow":{"view":"false"}}}`,
		},
		{
			name: "default-allow-default",
			json: `{"$default":{"allow":{"$default":"false"}}}`,
		},
		{
			name: "etype-fallback-view",
			json: `{"posts":{"fallback":{"view":"false"}}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := perms.ParseRuleDoc([]byte(tc.json))
			if err != nil {
				t.Fatalf("ParseRuleDoc failed: %v", err)
			}
			if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
				t.Fatalf("ViewGate(%s) = %v; want ViewClosed (%v)", tc.name, got, perms.ViewClosed)
			}
		})
	}
}

// TestViewGate_DynamicAndFailClosed verifies that data-dependent, auth-dependent,
// compound, or any non-literal-boolean expressions resolve to ViewDynamic (fail-closed).
func TestViewGate_DynamicAndFailClosed(t *testing.T) {
	expressions := []struct {
		name string
		expr string
	}{
		{"data-dependent", "auth.id == data.ownerId"},
		{"data-field-check", "data.status == 'published'"},
		{"auth-only", "auth.id != null"},
		{"request-ip", "request.ip == '127.0.0.1'"},
		{"compound-true", "true && true"},
		{"compound-false", "false || false"},
		{"whitespace-true", " true "},
		{"whitespace-false", " false "},
		{"uppercase-true", "TRUE"},
		{"uppercase-false", "FALSE"},
		{"integer-literal", "1"},
		{"null-literal", "null"},
		{"bind-reference", "isOwner"},
		{"invalid-cel-syntax", "invalid cel syntax !!!"},
	}

	for _, tc := range expressions {
		t.Run("direct-"+tc.name, func(t *testing.T) {
			doc, err := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"` + tc.expr + `"}}}`))
			if err != nil {
				t.Fatalf("ParseRuleDoc failed: %v", err)
			}
			if got := perms.ViewGate(doc, "posts"); got != perms.ViewDynamic {
				t.Fatalf("ViewGate(%q) = %v; want ViewDynamic (%v)", tc.expr, got, perms.ViewDynamic)
			}
		})
	}

	// Also verify dynamic expressions resolve to ViewDynamic across fallback levels.
	fallbackLevels := []struct {
		name string
		json string
	}{
		{
			name: "etype-allow-default-dynamic",
			json: `{"posts":{"allow":{"$default":"auth.id != null"}}}`,
		},
		{
			name: "default-allow-view-dynamic",
			json: `{"$default":{"allow":{"view":"auth.id == data.creator"}}}`,
		},
		{
			name: "default-allow-default-dynamic",
			json: `{"$default":{"allow":{"$default":"data.isPublic == true"}}}`,
		},
		{
			name: "etype-fallback-view-dynamic",
			json: `{"posts":{"fallback":{"view":"auth.id != null"}}}`,
		},
	}

	for _, tc := range fallbackLevels {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := perms.ParseRuleDoc([]byte(tc.json))
			if err != nil {
				t.Fatalf("ParseRuleDoc failed: %v", err)
			}
			if got := perms.ViewGate(doc, "posts"); got != perms.ViewDynamic {
				t.Fatalf("ViewGate(%s) = %v; want ViewDynamic (%v)", tc.name, got, perms.ViewDynamic)
			}
		})
	}
}

// TestViewGate_FallbackPrecedence verifies the exact precedence order of the resolution chain:
// [etype, view] > [etype, $default] > [$default, view] > [$default, $default] > [etype, fallback, view]
func TestViewGate_FallbackPrecedence(t *testing.T) {
	// 1. etype.allow.view overrides etype.allow.$default
	t.Run("allow.view over allow.$default", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"allow":{"view":"false","$default":"true"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
			t.Fatalf("got %v, want ViewClosed", got)
		}
	})

	// 2. etype.allow.$default overrides $default.allow.view
	t.Run("allow.$default over $default.allow.view", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"allow":{"$default":"false"}},
			"$default":{"allow":{"view":"true"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
			t.Fatalf("got %v, want ViewClosed", got)
		}
	})

	// 3. $default.allow.view overrides $default.allow.$default
	t.Run("$default.allow.view over $default.allow.$default", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"$default":{"allow":{"view":"false","$default":"true"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
			t.Fatalf("got %v, want ViewClosed", got)
		}
	})

	// 4. $default.allow.$default overrides etype.fallback.view
	t.Run("$default.allow.$default over etype.fallback.view", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"fallback":{"view":"true"}},
			"$default":{"allow":{"$default":"false"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
			t.Fatalf("got %v, want ViewClosed", got)
		}
	})

	// 5. etype.fallback.view applies when allow chain is exhausted
	t.Run("etype.fallback.view applies when allow chain exhausted", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"allow":{"create":"true"},"fallback":{"view":"false"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewClosed {
			t.Fatalf("got %v, want ViewClosed", got)
		}
	})

	// 6. Dynamic expression at higher priority wins over literal true/false at lower priority
	t.Run("dynamic at higher priority wins over literal at lower priority", func(t *testing.T) {
		doc, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"allow":{"view":"auth.id != null"}},
			"$default":{"allow":{"view":"true"}}
		}`))
		if got := perms.ViewGate(doc, "posts"); got != perms.ViewDynamic {
			t.Fatalf("got %v, want ViewDynamic", got)
		}

		doc2, _ := perms.ParseRuleDoc([]byte(`{
			"posts":{"allow":{"$default":"auth.id == data.ownerId"}},
			"$default":{"allow":{"view":"false"}}
		}`))
		if got := perms.ViewGate(doc2, "posts"); got != perms.ViewDynamic {
			t.Fatalf("got %v, want ViewDynamic", got)
		}
	})
}
