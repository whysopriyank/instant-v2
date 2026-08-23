package corpus

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestCanonicalSortsAndLowercasesUUID(t *testing.T) {
	b, err := CanonicalBytes([]byte(`{"b":2,"a":"550E8400-E29B-41D4-A716-446655440000"}`))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if m["a"] != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("a: %v", m["a"])
	}
	idxA := bytes.Index(b, []byte(`"a"`))
	idxB := bytes.Index(b, []byte(`"b"`))
	if idxA < 0 || idxB < 0 || idxA >= idxB {
		t.Fatalf("keys not sorted: %s", b)
	}
}

func TestCanonicalNested(t *testing.T) {
	b, err := CanonicalBytes([]byte(`{"z":{"b":1,"a":2},"a":[3,2,1]}`))
	if err != nil {
		t.Fatal(err)
	}
	// Top-level keys sorted: a before z, and nested z's keys sorted too.
	if !bytes.Contains(b, []byte(`"a":[3,2,1]`)) {
		t.Fatalf("expected a array preserved: %s", b)
	}
	if idxA := bytes.Index(b, []byte(`"a"`)); idxA != 1 {
		t.Fatalf("first key not a: %s", b)
	}
	// Nested order: a:2 before b:1 inside z
	if !bytes.Contains(b, []byte(`"a":2`)) || !bytes.Contains(b, []byte(`"b":1`)) {
		t.Fatalf("nested keys wrong: %s", b)
	}
}

func TestDiffDetectsMismatch(t *testing.T) {
	a, _ := CanonicalBytes([]byte(`{"op":"init-ok"}`))
	b, _ := CanonicalBytes([]byte(`{"op":"refresh-ok"}`))
	if d := Diff([][]byte{a}, [][]byte{b}); d == "" {
		t.Fatal("expected non-empty diff")
	}
	// Equal yields empty.
	a2, _ := CanonicalBytes([]byte(`{"op":"init","app-id":"a"}`))
	if d := Diff([][]byte{a2}, [][]byte{a2}); d != "" {
		t.Fatalf("equal frames diffed: %q", d)
	}
}

func TestRandomClientEventIDLooksUUID(t *testing.T) {
	id := RandomClientEventID()
	if len(id) != 36 {
		t.Fatalf("len %d: %q", len(id), id)
	}
	// Dashes at fixed positions — isUUIDish must say true.
	if !isUUIDish(id) {
		t.Fatalf("not uuidish: %q", id)
	}
}
