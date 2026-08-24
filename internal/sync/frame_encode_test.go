package sync

import (
	"encoding/json"
	"testing"
)

// Frame.Encode skips stdlib's compact() scan; it must stay byte-identical to
// json.Marshal(map[string]json.RawMessage) for every frame this package
// builds — sorted keys, compact values, HTML escaping preserved as produced.
func TestFrameEncodeParity(t *testing.T) {
	cases := []Frame{
		ErrFrame(429, "shed", "server busy; retry after 250ms"),
		{"op": json.RawMessage(`"refresh-ok"`), "processed-tx-id": json.RawMessage(`42`)},
		{
			"op":      json.RawMessage(`"error"`),
			"message": mustJSON(`html&<chars>"quoted" and ünïcode ✓`),
			"empty":   json.RawMessage(`{}`),
			"nested":  json.RawMessage(`[1,{"a":null},[true,false]]`),
			"z-last":  json.RawMessage(`"sorts last"`),
			"a-first": json.RawMessage(`"sorts first"`),
		},
	}
	for i, f := range cases {
		want, err := json.Marshal(map[string]json.RawMessage(f))
		if err != nil {
			t.Fatal(err)
		}
		got, err := f.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(want) {
			t.Fatalf("case %d:\n got %s\nwant %s", i, got, want)
		}
		if !json.Valid(got) {
			t.Fatalf("case %d: invalid JSON %s", i, got)
		}
	}
}

func TestFrameEncodeNil(t *testing.T) {
	var f Frame
	b, err := f.Encode()
	if err != nil || string(b) != "null" {
		t.Fatalf("nil frame: %q %v", b, err)
	}
}
