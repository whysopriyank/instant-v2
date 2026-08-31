package benchharness

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestV1TargetAcceptsLegacyEmptyRefreshAsNoop(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	session := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": query},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	refresh, err := session.decodeRefresh(event, "q-1")
	if err != nil {
		t.Fatalf("V1 empty legacy refresh is a valid no-op: %v", err)
	}
	if refresh.ProcessedTransactionID != "17" || refresh.Full != nil || refresh.Delta != nil {
		t.Fatalf("V1 empty legacy refresh was not preserved as a no-op: %#v", refresh)
	}
	if refresh.StateVersion != 0 {
		t.Fatalf("metadata-only refresh must not become semantic state version: %#v", refresh)
	}
}

func TestV1TargetAcceptsThreeComponentISN(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err != nil {
		t.Fatalf("V1 three-component ISN was rejected: %v", err)
	}
}

func TestV1TargetAcceptsLegacyComputationEnvelope(t *testing.T) {
	query := json.RawMessage(`{"todos":{}}`)
	session := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": query},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[{
			"instaql-query":{"todos":{}},
			"instaql-query-hash":123,
			"instaql-result":[{"data":{"datalog-result":{"join-rows":[[["todo-1","value","marker"]]]}},"child-nodes":[]}],
			"result-meta":null,
			"result-changed?":true,
			"duration-ms":1,
			"instaql-topic?":false
		}],
		"attrs":[]
	}`)}
	refresh, err := session.decodeRefresh(event, "q-1")
	if err != nil {
		t.Fatalf("source-shaped V1 computation was rejected: %v", err)
	}
	if refresh.Kind != RefreshFull || refresh.Full == nil || refresh.Full.Entities["todo-1"].Attributes["value"] != "marker" {
		t.Fatalf("source-shaped V1 computation was not materialized: %#v", refresh)
	}
}

func TestV1TargetRejectsMalformedLegacyEnvelope(t *testing.T) {
	base := `"processed-tx-id":17,"processed-isn":"0/0/17","computations":[],"attrs":[]`
	for name, payload := range map[string]string{
		"missing op":          `{` + base + `}`,
		"wrong isn type":      `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":23,"computations":[],"attrs":[]}`,
		"two isn components":  `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/17","computations":[],"attrs":[]}`,
		"four isn components": `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17/99","computations":[],"attrs":[]}`,
		"malformed isn":       `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/nope","computations":[],"attrs":[]}`,
		"missing attrs noop":  `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17","computations":[]}`,
		"attrs wrong type":    `{"op":"refresh-ok","processed-tx-id":17,"processed-isn":"0/0/17","computations":[],"attrs":{}}`,
	} {
		t.Run(name, func(t *testing.T) {
			session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
			event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(payload)}
			if _, err := session.decodeRefresh(event, "q-1"); err == nil {
				t.Fatalf("malformed V1 envelope was accepted: %s", payload)
			}
		})
	}
}

func TestV1TargetRejectsDeltaRefreshEnvelope(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV1}}}
	event := SessionEvent{Op: "refresh-ok-delta", Payload: json.RawMessage(`{
		"op":"refresh-ok-delta",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[{"instaql-query":{"todos":{}},"delta":{"ops":[]}}]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err == nil {
		t.Fatal("V1 legacy decoder accepted a V2 delta envelope")
	}
}

func TestV2TargetStillRejectsEmptyComputations(t *testing.T) {
	session := &TargetSession{driver: &TargetDriver{cfg: TargetConfig{Kind: TargetV2}}}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[]
	}`)}
	if _, err := session.decodeRefresh(event, "q-1"); err == nil || !strings.Contains(err.Error(), "refresh missing computations") {
		t.Fatalf("V2 empty refresh was not rejected fail-closed: %v", err)
	}
}

func TestV1NoopRefreshDoesNotBecomeProtocolReceipt(t *testing.T) {
	client := &TargetSession{
		driver:      &TargetDriver{cfg: TargetConfig{Kind: TargetV1}},
		wireQueries: map[string]json.RawMessage{"q-1": json.RawMessage(`{"todos":{}}`)},
	}
	event := SessionEvent{Op: "refresh-ok", Payload: json.RawMessage(`{
		"op":"refresh-ok",
		"processed-tx-id":17,
		"processed-isn":"0/0/17",
		"computations":[],
		"attrs":[]
	}`)}
	items, err := (&TargetDriver{}).decodeReceipts(client, event, "q-1", "recipient", newCommitPrefixes(nil))
	if err != nil || items != nil {
		t.Fatalf("V1 metadata-only refresh should be ignored, items=%#v err=%v", items, err)
	}
}
