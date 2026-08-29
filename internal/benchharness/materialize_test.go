package benchharness

import "testing"

func TestApplyRefreshNoopDeepClonesNestedAttributes(t *testing.T) {
	previous := Materialized{
		QueryID: "q-1",
		Entities: map[string]Entity{
			"e-1": {ID: "e-1", Attributes: map[string]any{
				"nested": map[string]any{
					"label": "before",
					"items": []any{map[string]any{"value": "before"}, "scalar"},
				},
			}},
		},
	}
	got, err := ApplyRefresh(previous, Refresh{Kind: RefreshNoop, ProcessedTransactionID: "17", StateVersion: 17})
	if err != nil {
		t.Fatal(err)
	}
	nested := got.Entities["e-1"].Attributes["nested"].(map[string]any)
	nested["label"] = "after"
	items := nested["items"].([]any)
	items[0].(map[string]any)["value"] = "after"
	items = append(items, "appended")
	nested["items"] = items
	originalNested := previous.Entities["e-1"].Attributes["nested"].(map[string]any)
	if originalNested["label"] != "before" {
		t.Fatalf("no-op clone shared nested map: %#v", previous)
	}
	originalItems := originalNested["items"].([]any)
	if originalItems[0].(map[string]any)["value"] != "before" || len(originalItems) != 2 {
		t.Fatalf("no-op clone shared nested slice: %#v", previous)
	}
}
