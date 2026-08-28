package sync

// Number-fidelity pins for the B5 chain: instaql now decodes with UseNumber
// and BuildNodeList projects values verbatim, so numeric literals flow
// byte-exactly to the wire instead of being re-rendered through float64
// (which reformatted 1.0 → 1 and corrupted integers beyond 2^53).

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func TestBuildNodeListNumberFidelity(t *testing.T) {
	idAttr := &platform.Attr{ID: mustUUID(t, "11111111-1111-4111-8111-111111111111"), ValueType: "blob", Cardinality: "one"}
	idLabel, idEtype := "id", "todos"
	idAttr.Label, idAttr.Etype = &idLabel, &idEtype
	priceAttr := &platform.Attr{ID: mustUUID(t, "33333333-3333-4333-8333-333333333333"), ValueType: "blob", Cardinality: "one"}
	priceLabel, priceEtype := "price", "todos"
	priceAttr.Label, priceAttr.Etype = &priceLabel, &priceEtype
	cat := &platform.AttrCatalog{}
	cat.Add(*idAttr)
	cat.Add(*priceAttr)

	// Literals a float64 round-trip would mangle: trailing-zero scale,
	// large integers beyond 2^53, exponent forms.
	result := json.RawMessage(`{"data":{"todos":[
		{"id":"aaa","price":1.0},
		{"id":"bbb","price":9007199254740993},
		{"id":"ccc","price":0.30000000000000004}
	]}}`)
	out, err := BuildNodeList(cat, result)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{"1.0", "9007199254740993", "0.30000000000000004"} {
		if !strings.Contains(s, want) {
			t.Fatalf("literal %q not preserved verbatim in %s", want, s)
		}
	}
	if strings.Contains(s, "9007199254740992") {
		t.Fatal("float64 corruption leaked into join-rows")
	}

	// The output must remain valid JSON the frozen SDK can parse.
	var nodes []struct {
		Data struct {
			DatalogResult struct {
				JoinRows [][][]any `json:"join-rows"`
			} `json:"datalog-result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatalf("output not valid JSON: %v (%s)", err, s)
	}
	if len(nodes) != 1 || len(nodes[0].Data.DatalogResult.JoinRows) == 0 {
		t.Fatalf("shape lost: %s", s)
	}
}

func TestBuildNodeListVerbatimValuesAndEscaping(t *testing.T) {
	idAttr := &platform.Attr{ID: mustUUID(t, "11111111-1111-4111-8111-111111111111"), ValueType: "blob", Cardinality: "one"}
	idLabel, idEtype := "id", "todos"
	idAttr.Label, idAttr.Etype = &idLabel, &idEtype
	textAttr := &platform.Attr{ID: mustUUID(t, "22222222-2222-4222-8222-222222222222"), ValueType: "blob", Cardinality: "one"}
	textLabel, textEtype := "text", "todos"
	textAttr.Label, textAttr.Etype = &textLabel, &textEtype
	// tags: many-cardinality ref → one triple per linked id.
	tagsAttr := &platform.Attr{ID: mustUUID(t, "44444444-4444-4444-8444-444444444444"), ValueType: "ref", Cardinality: "many"}
	tagsLabel := "tags"
	tagsAttr.Label, tagsAttr.Etype = &tagsLabel, &idEtype
	cat := &platform.AttrCatalog{}
	cat.Add(*idAttr)
	cat.Add(*textAttr)
	cat.Add(*tagsAttr)

	result := json.RawMessage(`{"data":{"todos":[
		{"id":"a-b-c","text":"he said \"hi\" \u003cok\u003e","tags":["t-1",{"id":"t-2"}]},
		{"id":"ddd","text":""}
	]},"page-info":{"hasNextPage":false}}`)
	out, err := BuildNodeList(cat, result)
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	// Values round-trip byte-stably: instaql emits HTML-escaped strings,
	// decode+re-marshal reproduces the same escapes (stdlib parity).
	for _, want := range []string{`"he said \"hi\" \u003cok\u003e"`, `"t-1"`, `"t-2"`, `{"hasNextPage":false}`} {
		if !strings.Contains(s, want) {
			t.Fatalf("verbatim value %q missing from %s", want, s)
		}
	}
	// Keys stay in stdlib sorted order, byte-identical to json.Marshal form.
	if !strings.Contains(s, `{"child-nodes":[],"data":{"datalog-result"`) {
		t.Fatalf("key order diverged from stdlib marshal: %s", s)
	}
	// Round-trips as the frozen SDK would read it.
	var nodes []map[string]json.RawMessage
	if err := json.Unmarshal(out, &nodes); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("node count: %d", len(nodes))
	}
}
