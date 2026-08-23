package perms_test

import (
	"testing"

	"github.com/instant-v2/instant-v2/internal/perms"
)

func TestCheckLiterals(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"true","delete":"false"}}}`))
	if ok, err := perms.Check("posts", "view", doc, perms.Bindings{}); err != nil || !ok {
		t.Fatalf("true literal: %v %v", ok, err)
	}
	if ok, err := perms.Check("posts", "delete", doc, perms.Bindings{}); err != nil || ok {
		t.Fatalf("false literal: %v %v", ok, err)
	}
}

func TestCheckAuthBinding(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"auth.id == data.ownerId"}}}`))
	ok, err := perms.Check("posts", "view", doc, perms.Bindings{
		Auth: map[string]any{"id": "u1"},
		Data: map[string]any{"ownerId": "u1"},
	})
	if err != nil || !ok {
		t.Fatalf("owner check should pass: %v %v", ok, err)
	}
	ok2, _ := perms.Check("posts", "view", doc, perms.Bindings{
		Auth: map[string]any{"id": "u1"},
		Data: map[string]any{"ownerId": "u2"},
	})
	if ok2 {
		t.Fatal("non-owner should fail")
	}
}

func TestCheckRequestBinding(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"request.ip == \"1.2.3.4\""}}}`))
	ok, err := perms.Check("posts", "view", doc, perms.Bindings{
		Request: perms.RequestInfo{IP: "1.2.3.4"},
	})
	if err != nil || !ok {
		t.Fatalf("request.ip: %v %v", ok, err)
	}
	ok2, _ := perms.Check("posts", "view", doc, perms.Bindings{
		Request: perms.RequestInfo{IP: "9.9.9.9"},
	})
	if ok2 {
		t.Fatal("wrong ip should fail")
	}
}

func TestCheckNoRuleIsPermissive(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{}`))
	if ok, err := perms.Check("posts", "view", doc, perms.Bindings{}); err != nil || !ok {
		t.Fatalf("missing rule should be permissive: %v %v", ok, err)
	}
}

func TestCheckFallbackChain(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"fallback":{"view":"true"}}}`))
	ok, err := perms.Check("posts", "view", doc, perms.Bindings{})
	if err != nil || !ok {
		t.Fatalf("fallback view true: %v %v", ok, err)
	}
}

func TestCheckWithBinds(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{
		"posts":{"bind":["isOwner","auth.id == data.ownerId"],"allow":{"view":"isOwner"}}
	}`))
	ok, _ := perms.Check("posts", "view", doc, perms.Bindings{
		Auth: map[string]any{"id": "u1"},
		Data: map[string]any{"ownerId": "u1"},
	})
	if !ok {
		t.Fatal("bind isOwner should be true")
	}
	ok2, _ := perms.Check("posts", "view", doc, perms.Bindings{
		Auth: map[string]any{"id": "u1"},
		Data: map[string]any{"ownerId": "u2"},
	})
	if ok2 {
		t.Fatal("isOwner should be false for non-owner")
	}
}

func TestCheckInvalidCEL(t *testing.T) {
	doc, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"this is not valid cel !!"}}}`))
	if _, err := perms.Check("posts", "view", doc, perms.Bindings{}); err == nil {
		t.Fatal("invalid CEL should error")
	}
}
