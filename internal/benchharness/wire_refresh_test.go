package benchharness

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestDecodeWireRefreshUsesComputationsAndNumericWatermark(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", At: time.Now(), Payload: json.RawMessage(`{
		"processed-tx-id": 17,
		"query-id": "wrong-top-level-value",
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": {"data": {"todos": [{"id":"todo-1","value":"marker"}]}}
		}]
	}`)}
	refresh, err := DecodeWireRefresh(event, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	if refresh.ProcessedTransactionID != "17" || refresh.StateVersion != 17 {
		t.Fatalf("watermark %#v", refresh)
	}
	if refresh.Full == nil || refresh.Full.QueryID != "q-1" {
		t.Fatalf("missing semantic full result %#v", refresh)
	}
	entity, ok := refresh.Full.Entities["todo-1"]
	if !ok || entity.Attributes["value"] != "marker" {
		t.Fatalf("computation result was not materialized: %#v", refresh.Full.Entities)
	}
}

func TestDecodeAddQuerySnapshotUsesInlineInitialResult(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	event := SessionEvent{Op: "add-query-ok", At: time.Now(), Payload: json.RawMessage(`{
		"q":{"todos":{}},
		"processed-tx-id":17,
		"result":{"data":{"todos":[{"id":"todo-1","value":"marker"}]}}
	}`)}
	refresh, available, err := decodeAddQuerySnapshot(event, "q-1", nil, query)
	if err != nil {
		t.Fatal(err)
	}
	if !available || refresh.Full == nil || refresh.StateVersion != 17 {
		t.Fatalf("inline initial snapshot not decoded: available=%t refresh=%#v", available, refresh)
	}
	if got := refresh.Full.Entities["todo-1"].Attributes["value"]; got != "marker" {
		t.Fatalf("initial entity value = %#v", got)
	}
}

func TestDecodeAddQuerySnapshotFallsBackWhenResultAbsent(t *testing.T) {
	_, available, err := decodeAddQuerySnapshot(SessionEvent{Op: "add-query-ok", Payload: json.RawMessage(`{"op":"add-query-ok"}`)}, "q-1", nil, json.RawMessage(`{"todos":{}}`))
	if err != nil || available {
		t.Fatalf("result-less add-query should use refresh fallback: available=%t err=%v", available, err)
	}
}

func TestDecodeWireRefreshParsesV1NodeList(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"processed-tx-id": 4,
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				["todo-1","attr-id","marker"]
			]]}}}]
		}]
	}`)}
	refresh, err := DecodeWireRefresh(event, "q-1")
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Full == nil || refresh.Full.Entities["todo-1"].Attributes["attr-id"] != "marker" {
		t.Fatalf("node-list not materialized: %#v", refresh.Full)
	}
}

func TestDecodeWireRefreshNormalizesUUIDNodeListAttributes(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"processed-tx-id": 4,
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000100","00000000-0000-4000-8000-000000000010"],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000001","marker"],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000002",7],
				["00000000-0000-4000-8000-000000000010","00000000-0000-4000-8000-000000000003",2]
			]]}}}]
		}]
	}`)}
	aliases := map[string]string{
		"id":     "00000000-0000-4000-8000-000000000100",
		"value":  "00000000-0000-4000-8000-000000000001",
		"bucket": "00000000-0000-4000-8000-000000000002",
		"rank":   "00000000-0000-4000-8000-000000000003",
	}
	refresh, err := DecodeWireRefreshWithAliases(event, "q-1", aliases)
	if err != nil {
		t.Fatal(err)
	}
	entity := refresh.Full.Entities["00000000-0000-4000-8000-000000000010"]
	if entity.Attributes["value"] != "marker" || entity.Bucket != 7 || entity.Rank != 2 {
		t.Fatalf("UUID attributes were not normalized: %#v", entity)
	}
	if _, present := entity.Attributes["id"]; present {
		t.Fatalf("identity UUID leaked into semantic attributes: %#v", entity.Attributes)
	}
	for _, wireKey := range []string{aliases["value"], aliases["bucket"], aliases["rank"]} {
		if _, present := entity.Attributes[wireKey]; present {
			t.Fatalf("wire UUID leaked into semantic attributes: %#v", entity.Attributes)
		}
	}
}

func TestNormalizeWireAttributeCanonicalizesUUIDAliases(t *testing.T) {
	alias := "00000000-0000-4000-8000-000000000100"
	if got := normalizeWireAttribute(strings.ToUpper(alias), map[string]string{"id": alias}); got != "id" {
		t.Fatalf("uppercase UUID alias normalized as %q, want id", got)
	}
}

func TestDecodeWireRefreshRejectsNonCanonicalEntityUUIDCasing(t *testing.T) {
	entityID := "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB"
	idAttr := "00000000-0000-4000-8000-000000000100"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":[{"data":{"datalog-result":{"join-rows":[[
			[%q,%q,%q]
		]]}}}]}]
	}`, entityID, idAttr, entityID))}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": strings.ToLower(idAttr)})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lowercase") {
		t.Fatalf("noncanonical entity UUID casing was accepted: %v", err)
	}
}

func TestDecodeWireRefreshRejectsMissingIdentityForResultEntity(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{
			"instaql-query": {"todos": {}},
			"instaql-result": [{"data":{"datalog-result":{"join-rows":[[
				[%q,%q,"marker"]
			]]}}}]
		}]
	}`, entityID, valueAttr))}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": idAttr, "value": valueAttr})
	if err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing identity row was accepted: %v", err)
	}
}

func TestDecodeWireRefreshRejectsInvalidDuplicateAndConflictingIdentity(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	aliases := map[string]string{"id": idAttr, "value": valueAttr}
	for _, tc := range []struct {
		name string
		rows string
		want string
	}{
		{name: "invalid", rows: fmt.Sprintf(`[%q,%q,"not-a-uuid"]`, entityID, idAttr), want: "UUID"},
		{name: "duplicate", rows: fmt.Sprintf(`[%q,%q,%q],[%q,%q,%q]`, entityID, idAttr, entityID, entityID, idAttr, entityID), want: "duplicate"},
		{name: "conflicting", rows: fmt.Sprintf(`[%q,%q,%q],[%q,%q,%q]`, entityID, idAttr, entityID, entityID, idAttr, "00000000-0000-4000-8000-000000000011"), want: "identity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
				"computations": [{"instaql-query":{"todos":{}},"instaql-result":[{"data":{"datalog-result":{"join-rows":[[%s]]}}}]}]
			}`, tc.rows))}
			_, err := DecodeWireRefreshWithAliases(event, "q-1", aliases)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("%s identity was accepted: %v", tc.name, err)
			}
		})
	}
}

func TestDecodeWireRefreshNormalizesObjectIdentityAliasBeforeOmitting(t *testing.T) {
	entityID := "00000000-0000-4000-8000-000000000010"
	idAttr := "00000000-0000-4000-8000-000000000100"
	valueAttr := "00000000-0000-4000-8000-000000000001"
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(fmt.Sprintf(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":%q,%q:%q,%q:"marker"}]}}}]
	}`, entityID, idAttr, entityID, valueAttr))}
	refresh, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": idAttr, "value": valueAttr})
	if err != nil {
		t.Fatal(err)
	}
	entity := refresh.Full.Entities[entityID]
	if entity.Attributes["value"] != "marker" {
		t.Fatalf("object value was not normalized: %#v", entity)
	}
	if _, ok := entity.Attributes["id"]; ok {
		t.Fatalf("object identity leaked into attributes: %#v", entity.Attributes)
	}
	if _, ok := entity.Attributes[idAttr]; ok {
		t.Fatalf("object identity alias leaked into attributes: %#v", entity.Attributes)
	}
}

func TestDecodeWireRefreshRejectsInvalidObjectIdentity(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"computations": [{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"not-a-uuid","value":"marker"}]}}}]
	}`)}
	_, err := DecodeWireRefreshWithAliases(event, "q-1", map[string]string{"id": "00000000-0000-4000-8000-000000000100"})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "identity") {
		t.Fatalf("invalid object identity was accepted: %v", err)
	}
}

func TestDecodeWireRefreshCombinesMatchingComputations(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"computations": [
			{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[{"id":"post-1","value":"unrelated-before"}]} }},
			{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"todo-1","value":"first"}]} }},
			{"instaql-query":{"todos":{}},"instaql-result":{"data":{"todos":[{"id":"todo-2","value":"second"}]} }},
			{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[{"id":"post-2","value":"unrelated-after"}]}}}
		]
	}`)}
	refresh, err := DecodeWireRefreshForQuery(event, "q-1", json.RawMessage(`{"todos":{}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Full == nil || len(refresh.Full.Entities) != 2 {
		t.Fatalf("matching computations were not combined: %#v", refresh.Full)
	}
	if refresh.Full.Entities["todo-1"].Attributes["value"] != "first" || refresh.Full.Entities["todo-2"].Attributes["value"] != "second" {
		t.Fatalf("matching computation results were lost: %#v", refresh.Full.Entities)
	}
	if _, present := refresh.Full.Entities["post-1"]; present {
		t.Fatal("unrelated computation before matching query was decoded")
	}
	if _, present := refresh.Full.Entities["post-2"]; present {
		t.Fatal("unrelated computation after matching query was decoded")
	}
}

func TestDecodeWireRefreshCombinesMatchingDeltas(t *testing.T) {
	event := SessionEvent{Op: "refresh-ok-delta", Payload: json.RawMessage(`{
		"processed-tx-id": 9,
		"computations": [
			{"instaql-query":{"posts":{}},"delta":{"ops":[{"op":"add","id":"post-1","entity":{"id":"post-1","value":"ignore"}}]}},
			{"instaql-query":{"todos":{}},"delta":{"ops":[{"op":"add","id":"todo-1","entity":{"id":"todo-1","value":"first"}}]}},
			{"instaql-query":{"todos":{}},"delta":{"ops":[{"op":"update","id":"todo-2","entity":{"id":"todo-2","value":"second","bucket":3}}]}},
			{"instaql-query":{"posts":{}},"delta":{"ops":[{"op":"remove","id":"post-2"}]}}
		]
	}`)}
	refresh, err := DecodeWireRefreshForQuery(event, "q-1", json.RawMessage(`{"todos":{}}`), nil)
	if err != nil {
		t.Fatal(err)
	}
	if refresh.Kind != RefreshDelta || refresh.Delta == nil || len(refresh.Delta.Adds) != 1 || len(refresh.Delta.Updates) != 1 || len(refresh.Delta.Removes) != 0 {
		t.Fatalf("matching deltas were not combined: %#v", refresh)
	}
	if refresh.Delta.Adds[0].ID != "todo-1" || refresh.Delta.Adds[0].Attributes["value"] != "first" || refresh.Delta.Updates[0].Bucket != 3 {
		t.Fatalf("matching delta payloads were not materialized: %#v", refresh.Delta)
	}
}
