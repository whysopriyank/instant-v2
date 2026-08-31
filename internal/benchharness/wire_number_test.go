package benchharness_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func TestDecodeWireRefreshPreservesExactIntegers(t *testing.T) {
	for _, number := range []string{
		"9007199254740991", "9007199254740992", "9007199254740993",
		"-9007199254740993", "9223372036854775807", "-9223372036854775808",
		"9223372036854775808", "18446744073709551615",
	} {
		for _, shape := range []string{"object", "node-list", "delta-add", "delta-update"} {
			t.Run(shape+"/"+number, func(t *testing.T) {
				state := decodeNumericState(t, shape, number)
				got, err := json.Marshal(state.Entities["item"].Attributes["value"])
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != number {
					t.Errorf("public decoder changed exact integer: got %s, want %s", got, number)
				}
				got, err = json.Marshal(state.Entities["item"].Attributes["nested"])
				if err != nil {
					t.Fatal(err)
				}
				if want := `{"values":[` + number + `]}`; string(got) != want {
					t.Fatalf("public decoder changed nested integer: got %s, want %s", got, want)
				}
			})
		}
	}
}

func TestDecodeWireRefreshIntegerSemanticHashes(t *testing.T) {
	for _, equivalents := range [][]string{
		{"1", "1.0", "1e0"},
		{"0.125", "0.1250", "1.25e-1"},
		{"9007199254740993", "9007199254740993.0", "9.007199254740993e15"},
		{"9223372036854775807", "9223372036854775807.0", "9.223372036854775807e18"},
		{"-9223372036854775808", "-9223372036854775808.0", "-9.223372036854775808e18"},
	} {
		t.Run(equivalents[0], func(t *testing.T) {
			var expected string
			for _, number := range equivalents {
				state := decodeNumericState(t, "object", number)
				digest, err := state.Digest()
				if err != nil {
					t.Fatal(err)
				}
				if expected == "" {
					expected = digest
				} else if digest != expected {
					t.Fatalf("equivalent integer spelling %s changed semantic hash", number)
				}
			}
		})
	}
	left := decodeNumericState(t, "object", "9007199254740992")
	right := decodeNumericState(t, "object", "9007199254740993")
	leftDigest, err := left.Digest()
	if err != nil {
		t.Fatal(err)
	}
	rightDigest, err := right.Digest()
	if err != nil {
		t.Fatal(err)
	}
	if leftDigest == rightDigest {
		t.Fatal("neighboring integers above 2^53 share a semantic hash")
	}
}

func TestDecodeWireRefreshRetainsSafeNumberHashes(t *testing.T) {
	for number, want := range map[string]string{
		"1":     "f1cda3fcf0d3208ff1a8c6be77a07848424ebf00f486c5f1bc77f246c7feee81",
		"0.125": "673e3dba630b68342cc1056bf1b0580a324b90db480c1389837a5ca1546afce5",
	} {
		t.Run(number, func(t *testing.T) {
			state := decodeNumericState(t, "object", number)
			got, err := state.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("safe-number hash changed: got %s, want %s", got, want)
			}
		})
	}
}

func TestDecodeWireRefreshRetainsNumericOverflowFallback(t *testing.T) {
	for _, number := range []string{"1e400", "-1e400"} {
		state := decodeNumericState(t, "object", number)
		if got := state.Entities["item"].Attributes["value"]; got != number {
			t.Fatalf("out-of-range exponent fallback changed: got %#v, want %q", got, number)
		}
	}
}

func TestDecodeWireRefreshForQueryDistinguishesExactIntegers(t *testing.T) {
	event := benchharness.SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"computations":[
			{"instaql-query":{"items":{"where":{"value":9007199254740992}}},"instaql-result":{"items":[{"id":"wrong"}]}},
			{"instaql-query":{"items":{"where":{"value":9007199254740993.0}}},"instaql-result":{"items":[{"id":"right"}]}}
		]}`)}
	refresh, err := benchharness.DecodeWireRefreshForQuery(event, "q", json.RawMessage(`{"items":{"where":{"value":9.007199254740993e15}}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Full == nil || len(refresh.Full.Entities) != 1 || refresh.Full.Entities["right"].ID != "right" {
		t.Fatalf("public query decoder matched neighboring integer: %#v", refresh.Full)
	}
}

func decodeNumericState(t *testing.T, shape, number string) benchharness.Materialized {
	t.Helper()
	entity := fmt.Sprintf(`{"id":"item","value":%s,"nested":{"values":[%s]}}`, number, number)
	op, computation := "refresh-ok", `"instaql-result":{"items":[`+entity+`]}`
	switch shape {
	case "node-list":
		computation = fmt.Sprintf(`"instaql-result":[{"data":{"datalog-result":{"join-rows":[[["item","value",%s],["item","nested",{"values":[%s]}]]]}}}]`, number, number)
	case "delta-add", "delta-update":
		op = "refresh-ok-delta"
		deltaOp := "add"
		if shape == "delta-update" {
			deltaOp = "update"
		}
		computation = fmt.Sprintf(`"delta":{"ops":[{"op":%q,"id":"item","entity":%s}]}`, deltaOp, entity)
	}
	event := benchharness.SessionEvent{Op: op, Payload: json.RawMessage(`{"computations":[{"instaql-query":{"items":{}},` + computation + `}]}`)}
	refresh, err := benchharness.DecodeWireRefresh(event, "q")
	if err != nil {
		t.Fatal(err)
	}
	state, err := benchharness.ApplyRefresh(benchharness.Materialized{QueryID: "q"}, refresh)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
