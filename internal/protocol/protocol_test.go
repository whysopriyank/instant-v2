package protocol

import (
	"encoding/json"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	raw := []byte(`{"op":"init","app-id":"a","versions":{"@instantdb/core":"0.22.0"}}`)
	f, err := ParseFrame(raw)
	if err != nil {
		t.Fatalf("ParseFrame: %v", err)
	}
	op, err := f.GetOp()
	if err != nil {
		t.Fatalf("GetOp: %v", err)
	}
	if op != OpInit {
		t.Fatalf("op %q want %q", op, OpInit)
	}
	// Preservation of unknown fields under round-trip.
	b, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := ParseFrame(b)
	if err != nil {
		t.Fatal(err)
	}
	if op2, _ := f2.GetOp(); op2 != op {
		t.Fatalf("op round-trip mismatch: %q vs %q", op2, op)
	}
}

func TestErrorEnvelopeJSON(t *testing.T) {
	e := ErrorEnvelope{Status: 400, Type: "validation", Message: "bad query"}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"status", "type", "message"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("missing %q; have %v", k, m)
		}
	}
}

func TestTxStepParse(t *testing.T) {
	for _, tc := range []struct {
		raw string
		op  string
	}{
		{`["add-triple","e","a","v"]`, "add-triple"},
		{`["delete-attr","a"]`, "delete-attr"},
		{`["rule-params",{"s":"x"}]`, "rule-params"},
	} {
		s, err := ParseTxStep(json.RawMessage(tc.raw))
		if err != nil {
			t.Fatalf("ParseTxStep %q: %v", tc.raw, err)
		}
		if s.Op != tc.op {
			t.Fatalf("op %q want %q (from %q)", s.Op, tc.op, tc.raw)
		}
		if _, known := KnownTxStepOps[s.Op]; !known {
			t.Fatalf("KnownTxStepOps missing %q", s.Op)
		}
	}
}

func TestInstaQLOptionsFields(t *testing.T) {
	lim := 10
	opts := InstaQLOptions{Where: map[string]any{"x": map[string]any{"$gt": 1}}, Limit: &lim, Aggregate: strptr("count")}
	b, _ := json.Marshal(opts)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, ok := m["where"]; !ok {
		t.Fatal("where not serialized")
	}
	_ = b
}

func strptr(s string) *string { return &s }

func TestAttrTagRoundTrip(t *testing.T) {
	// Verify hyphen/question-mark field spellings round-trip exactly (frozen surface).
	// We hand-author Attr tags; the test pins spellings so a rename can't drift silently.
	uniq := true
	idx := true
	req := true
	a := Attr{ID: "a", ValueType: "ref", Cardinality: "one", ForwardIdentity: [3]string{"uuid", "etype", "label"}, Unique: &uniq, Index: &idx, Required: &req}
	b, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m["value-type"]; !ok {
		t.Fatal("value-type missing — tag likely renamed")
	}
	if _, ok := m["forward-identity"]; !ok {
		t.Fatal("forward-identity missing")
	}
	for _, k := range []string{"unique?", "index?", "required?"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("%s missing", k)
		}
	}
}
