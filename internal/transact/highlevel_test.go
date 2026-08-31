package transact_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/transact"
)

func TestLowerAdminStepsProvisionAndEmissionOrder(t *testing.T) {
	ctx := context.Background()
	appID, eid := [16]byte{1}, uuidStr([16]byte{2})
	cat := &platform.AttrCatalog{}
	var provisioned []string
	ids := map[string][16]byte{"id": {3}, "a": {4}, "z": {5}}
	pass := json.RawMessage(`[ "future-op", { "untouched": true } ]`)
	hooks := transact.LowerHooks{
		CreateAttr: func(_ context.Context, gotApp [16]byte, spec transact.AttrSpec) (platform.Attr, error) {
			if gotApp != appID || spec.Etype != "todos" || spec.ValueType != "blob" || spec.Cardinality != "one" || spec.Unique != (spec.Label == "id") {
				t.Fatalf("unexpected provision request: app=%x spec=%+v", gotApp, spec)
			}
			provisioned = append(provisioned, spec.Label)
			return platform.Attr{ID: ids[spec.Label], Etype: &spec.Etype, Label: &spec.Label, ValueType: spec.ValueType, Cardinality: spec.Cardinality, IsUnique: spec.Unique}, nil
		},
	}
	raw := []json.RawMessage{pass, mustJSON(t, []any{"update", "todos", eid, map[string]any{"z": 2, "id": "ignored", "a": 1}, map[string]any{"upsert": true}})}
	got, err := transact.LowerAdminSteps(ctx, appID, cat, raw, hooks, false)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(provisioned, []string{"id", "a", "z"}) {
		t.Fatalf("provision order: %v", provisioned)
	}
	want := []json.RawMessage{pass}
	for _, label := range []string{"id", "a", "z"} {
		value := any(eid)
		if label == "a" {
			value = 1
		}
		if label == "z" {
			value = 2
		}
		want = append(want, mustJSON(t, []any{"add-triple", eid, uuidStr(ids[label]), value, map[string]string{"mode": "upsert"}}))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("lowered steps:\ngot  %s\nwant %s", got, want)
	}
	if len(cat.Attrs()) != 0 {
		t.Fatal("lowering mutated the shared catalog")
	}
}

func TestLowerAdminStepsReusesMintedLookupForDelete(t *testing.T) {
	ctx := context.Background()
	appID := [16]byte{1}
	cat := &platform.AttrCatalog{}
	etype := "todos"
	ids := map[string][16]byte{"id": {2}, "key": {3}, "name": {4}}
	for _, label := range []string{"id", "key", "name"} {
		cat.Add(platform.Attr{ID: ids[label], Etype: &etype, Label: &label, ValueType: "blob", Cardinality: "one", IsUnique: label != "name"})
	}
	lookups := 0
	hooks := transact.LowerHooks{ResolveUnique: func(_ context.Context, gotApp, attr [16]byte, value json.RawMessage) ([16]byte, bool, error) {
		lookups++
		if gotApp != appID || attr != ids["key"] || string(value) != `"same"` {
			t.Fatalf("lookup: app=%x attr=%x value=%s", gotApp, attr, value)
		}
		return [16]byte{}, false, nil
	}}
	raw := []json.RawMessage{
		mustJSON(t, []any{"update", "todos", map[string]any{"key": "same"}, map[string]any{"name": "created"}}),
		mustJSON(t, []any{"delete", "todos", []any{"key", "same"}}),
	}
	got, err := transact.LowerAdminSteps(ctx, appID, cat, raw, hooks, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 5 || lookups != 1 {
		t.Fatalf("steps=%s lookups=%d", got, lookups)
	}
	var first []json.RawMessage
	if err := json.Unmarshal(got[0], &first); err != nil {
		t.Fatal(err)
	}
	var minted string
	if err := json.Unmarshal(first[1], &minted); err != nil {
		t.Fatal(err)
	}
	want := []json.RawMessage{
		mustJSON(t, []any{"add-triple", minted, uuidStr(ids["key"]), "same"}),
		mustJSON(t, []any{"add-triple", minted, uuidStr(ids["id"]), minted}),
		mustJSON(t, []any{"add-triple", minted, uuidStr(ids["id"]), minted}),
		mustJSON(t, []any{"add-triple", minted, uuidStr(ids["name"]), "created"}),
		mustJSON(t, []any{"delete-entity", minted, "todos"}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %s; want %s", got, want)
	}
	if _, err := transact.ParseSteps(got); err != nil {
		t.Fatalf("lowered wire is invalid: %v", err)
	}
}
