package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestExtractEntitiesSupportedWireShapes(t *testing.T) {
	for _, result := range []string{
		`[{"data":{"datalog-result":{"join-rows":[[["first","attr",1],["first","other",2],["second","attr",3]]]}}}]`,
		`{"data":{"chaos-items":[{"id":"first"},{"id":"second"}]}}`,
	} {
		got, err := extractEntities(frame{"computations": json.RawMessage(`[{"instaql-result":` + result + `}]`)})
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string]bool{"first": true, "second": true}; !reflect.DeepEqual(got, want) {
			t.Fatalf("entity IDs: got %v, want %v", got, want)
		}
	}
}

func TestExtractEntitiesRejectsMissingOrMalformedComputations(t *testing.T) {
	for _, input := range []frame{nil, {"computations": json.RawMessage(`[]`)}, {"computations": json.RawMessage(`{`)}} {
		if _, err := extractEntities(input); err == nil {
			t.Fatalf("accepted malformed refresh: %v", input)
		}
	}
}

func TestWriteEntityIDAndSetDifference(t *testing.T) {
	if got, want := writeEntityID(42), "00000000-0000-4000-8000-00000000002a"; got != want {
		t.Fatalf("write entity ID = %s, want %s", got, want)
	}
	extra, missing := diffSets(map[string]bool{"same": true, "missing": true}, map[string]bool{"same": true, "extra": true})
	if !reflect.DeepEqual(extra, []string{"extra"}) || !reflect.DeepEqual(missing, []string{"missing"}) {
		t.Fatalf("set difference: extra=%v missing=%v", extra, missing)
	}
}
