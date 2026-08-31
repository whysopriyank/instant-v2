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

func TestCanonicalExactNumbers(t *testing.T) {
	for _, input := range []string{`9007199254740993`, `9223372036854775807`, `{"nested":[9007199254740993,0.123456789012345678901]}`} {
		got, err := CanonicalBytes([]byte(input))
		if err != nil || string(got) != input {
			t.Errorf("exact number %s: got %s, err %v", input, got, err)
		}
	}
	a, _ := CanonicalBytes([]byte(`9007199254740992`))
	b, _ := CanonicalBytes([]byte(`9007199254740993`))
	if bytes.Equal(a, b) {
		t.Fatal("adjacent integers above 2^53 must remain distinct")
	}
	for _, input := range []string{`1`, `1.0`, `1e0`, `10e-1`} {
		got, err := CanonicalBytes([]byte(input))
		if err != nil || string(got) != "1" {
			t.Errorf("semantic equality %s: got %s, err %v", input, got, err)
		}
	}
}

func TestCanonicalPolicyIdempotenceAndSensitivity(t *testing.T) {
	input := []byte(`{"auth":{"app":{"id":"AABBCCDD-AAAA-4AAA-8AAA-AAAAAAAAAAAA","title":"secret"},"admin?":null},"attrs":[],"session-id":"random","tx-id":12,"processed-isn":8,"result-meta":null,"data":[{"value":1.0},2]}`)
	for _, opts := range []CanonicalOptions{{}, {Differential: true}} {
		first, err := CanonicalBytesOpts(input, opts)
		if err != nil {
			t.Fatal(err)
		}
		second, err := CanonicalBytesOpts(first, opts)
		if err != nil || !bytes.Equal(first, second) {
			t.Fatalf("not idempotent: %s / %s (%v)", first, second, err)
		}
		for _, pair := range [][2]string{
			{`{"op":"init-ok"}`, `{"op":"error"}`},
			{`{"data":[1,2]}`, `{"data":[2,1]}`},
			{`{"auth":{"admin?":true}}`, `{"auth":{"admin?":false}}`},
			{`{"auth":{"app":{"id":"a"}}}`, `{"auth":{"app":{"id":"b"}}}`},
			{`{"status":400,"message":"a"}`, `{"status":400,"message":"b"}`},
			{`{"value_md5":"a"}`, `{"value_md5":"b"}`},
		} {
			a, _ := CanonicalBytesOpts([]byte(pair[0]), opts)
			b, _ := CanonicalBytesOpts([]byte(pair[1]), opts)
			if bytes.Equal(a, b) {
				t.Errorf("meaningful difference lost: %v", pair)
			}
		}
	}
}

func TestCanonicalNumericFormsAndJSONBoundary(t *testing.T) {
	for input, want := range map[string]string{
		`-0.0`: `0`, `0e999999999999999999999`: `0`,
		`1e999999999999999999999`:   `1e999999999999999999999`,
		`10e-999999999999999999999`: `1e-999999999999999999998`,
		`0.0000001`:                 `1e-7`, `1e+21`: `1e21`, `123e-2`: `1.23`,
		`100.00`: `100`, `-0.00100`: `-0.001`,
	} {
		got, err := CanonicalBytes([]byte(input))
		if err != nil || string(got) != want {
			t.Errorf("%s: got %s want %s (%v)", input, got, want, err)
		}
	}
	for _, input := range []string{`{} {}`, `{} garbage`, `1e`, ``} {
		if _, err := CanonicalBytes([]byte(input)); err == nil {
			t.Errorf("invalid JSON accepted: %s", input)
		}
	}
}

func TestCanonicalExistingExclusionsOnly(t *testing.T) {
	input := []byte(`{"attrs":[],"processed-isn":1,"isn":2,"trace-id":"trace","server-hostname":"host","server-port":80,"auth":{"app":{"id":"a","other":1},"admin?":null},"result-meta":null,"session-id":7,"nested":{"admin?":null,"app":{"id":"b","other":2}},"tx-id":123}`)
	got, err := CanonicalBytesOpts(input, CanonicalOptions{Differential: true})
	want := `{"attrs":"<attrs>","auth":{"admin?":false,"app":{"id":"a"}},"nested":{"admin?":null,"app":{"id":"b","other":2}},"result-meta":{},"session-id":7,"tx-id":"<tx-id>"}`
	if err != nil || string(got) != want {
		t.Fatalf("policy: %s want %s (%v)", got, want, err)
	}
	plain, err := CanonicalBytes(input)
	if err != nil {
		t.Fatal(err)
	}
	for _, retained := range []string{`"attrs":[]`, `"trace-id":"trace"`, `"processed-isn":1`, `"result-meta":null`, `"other":1`} {
		if !bytes.Contains(plain, []byte(retained)) {
			t.Fatalf("plain policy broadened: missing %s from %s", retained, plain)
		}
	}
}

func FuzzCanonicalIdempotent(f *testing.F) {
	for _, seed := range []string{`{"value":9007199254740993}`, `{"auth":{"app":{"id":"a"},"admin?":null}}`, `[1.0,1e0,-0,0.123456789012345678901]`, `{"attrs":null,"result-meta":null,"tx-id":999}`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		for _, opts := range []CanonicalOptions{{}, {Differential: true}} {
			first, err := CanonicalBytesOpts(raw, opts)
			if err != nil {
				return
			}
			second, err := CanonicalBytesOpts(first, opts)
			if err != nil || !bytes.Equal(first, second) {
				t.Fatalf("not idempotent: %s / %s (%v)", first, second, err)
			}
		}
	})
}
