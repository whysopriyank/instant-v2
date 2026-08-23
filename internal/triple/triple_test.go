package triple

import (
	"encoding/json"
	"testing"
)

func TestEncodeValuePrimitives(t *testing.T) {
	for _, tc := range []struct {
		in   any
		want string
	}{
		{nil, "null"},
		{true, "true"},
		{"red", `"red"`},
		{int64(25), "25"},
		{3.5, "3.5"},
	} {
		b, err := EncodeValue(tc.in)
		if err != nil {
			t.Fatalf("EncodeValue(%v): %v", tc.in, err)
		}
		if string(b) != tc.want {
			t.Fatalf("EncodeValue(%v) = %s want %s", tc.in, b, tc.want)
		}
	}
}

func TestEncodeValueContainers(t *testing.T) {
	b, err := EncodeValue(map[string]any{"b": 1, "a": []any{"x", nil}})
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back["a"].([]any)[0] != "x" {
		t.Fatalf("round trip: %s", b)
	}
}

func TestEncodeValueNoHTMLEscape(t *testing.T) {
	// cheshire does not HTML-escape; Go must not either or md5 inputs drift.
	b, err := EncodeValue("<a>&</a>")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"<a>&</a>"` {
		t.Fatalf("HTML escaping leaked: %s", b)
	}
}

func TestIsValueRejectsUnsupported(t *testing.T) {
	if IsValue(struct{}{}) {
		t.Fatal("struct accepted")
	}
	if IsValue(func() {}) {
		t.Fatal("func accepted")
	}
	if !IsValue([]any{1, "x", nil, true}) {
		t.Fatal("valid nested slice rejected")
	}
	if IsValue([]any{1, struct{}{}}) {
		t.Fatal("invalid nested element accepted")
	}
}

func TestIsRefValue(t *testing.T) {
	ok := "550e8400-e29b-41d4-a716-446655440000"
	if !IsRefValue(ok) {
		t.Fatalf("valid ref rejected: %q", ok)
	}
	if !IsRefValue("550E8400-E29B-41D4-A716-446655440000") {
		t.Fatal("uppercase uuid rejected")
	}
	for _, bad := range []string{
		"", "not-a-uuid",
		"550e8400-e29b-41d4-a716-44665544000",   // short
		"550e8400-e29b-41d4-a716-4466554400000", // long
		"550e8400ge29b-41d4-a716-446655440000",  // bad hex
	} {
		if IsRefValue(bad) {
			t.Fatalf("bad ref accepted: %q", bad)
		}
	}
}

func TestJSONNullMD5Constant(t *testing.T) {
	// Pin the constant: it is md5("null") and part of the physical contract.
	if JSONNullMD5 != "37a6259cc0c1dae299a7866489dff0bd" {
		t.Fatalf("JSONNullMD5 drifted: %q", JSONNullMD5)
	}
}
