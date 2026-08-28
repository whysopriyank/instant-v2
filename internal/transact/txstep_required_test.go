package transact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseRequiredAttrUpdate(t *testing.T) {
	req, err := parseRequiredAttrUpdate(json.RawMessage(`{"id":"11111111-1111-4111-8111-111111111111","required?":true}`))
	if err != nil {
		t.Fatalf("parse required update: %v", err)
	}
	if req.ID == "" || req.Required == nil || !*req.Required {
		t.Fatalf("decoded patch: %+v", req)
	}
	for _, raw := range []string{
		`{"id":"11111111-1111-4111-8111-111111111111"}`,
		`{"id":"11111111-1111-4111-8111-111111111111","index?":true}`,
		`{"id":"11111111-1111-4111-8111-111111111111","required?":"yes"}`,
		`{"id":"11111111-1111-4111-8111-111111111111","required?":null}`,
	} {
		if _, err := parseRequiredAttrUpdate(json.RawMessage(raw)); err == nil {
			t.Fatalf("invalid update %s was accepted", raw)
		}
	}
	if _, err := parseRequiredAttrUpdate(json.RawMessage(`{"id":"11111111-1111-4111-8111-111111111111","index?":true}`)); !strings.Contains(err.Error(), "unsupported update-attr field") {
		t.Fatalf("unsupported field error: %v", err)
	}
}
