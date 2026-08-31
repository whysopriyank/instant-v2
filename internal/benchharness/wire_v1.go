package benchharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// decodeV1WireRefresh accepts the legacy V1 refresh envelope. V1 legitimately
// emits refresh-ok with an empty computations array when only attrs changed;
// that frame is a metadata-only no-op, not a malformed snapshot. V2 keeps the
// stricter generic decoder below, which requires at least one computation.
func decodeV1WireRefresh(ev SessionEvent, queryID string, aliases map[string]string, expectedQuery json.RawMessage) (Refresh, error) {
	if ev.Op != "refresh-ok" {
		return Refresh{}, fmt.Errorf("V1 legacy decoder rejects %s", ev.Op)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(ev.Payload, &fields); err != nil {
		return Refresh{}, err
	}
	for name := range fields {
		switch name {
		case "op", "processed-tx-id", "processed-isn", "computations", "attrs", "trace-id":
		default:
			return Refresh{}, fmt.Errorf("V1 refresh contains unsupported field %q", name)
		}
	}
	if err := validateV1RefreshField(fields, "op", func(raw json.RawMessage) error {
		var op string
		if err := json.Unmarshal(raw, &op); err != nil || op != "refresh-ok" {
			return fmt.Errorf("must be %q", "refresh-ok")
		}
		if op != ev.Op {
			return fmt.Errorf("does not match event op %q", ev.Op)
		}
		return nil
	}); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh op %w", err)
	}
	if err := validateV1RefreshField(fields, "processed-tx-id", validateV1Integer); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh processed-tx-id %w", err)
	}
	if err := validateV1RefreshField(fields, "processed-isn", validateV1ISN); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh processed-isn %w", err)
	}
	if attrs, ok := fields["attrs"]; ok {
		if err := validateV1Attrs(attrs); err != nil {
			return Refresh{}, fmt.Errorf("V1 refresh attrs %w", err)
		}
	}
	computationsRaw, ok := fields["computations"]
	if !ok || rawIsNull(computationsRaw) {
		return Refresh{}, fmt.Errorf("V1 refresh missing computations")
	}
	var computations []json.RawMessage
	if err := json.Unmarshal(computationsRaw, &computations); err != nil {
		return Refresh{}, fmt.Errorf("V1 refresh computations must be an array: %w", err)
	}
	for i, raw := range computations {
		var computation map[string]json.RawMessage
		if err := json.Unmarshal(raw, &computation); err != nil {
			return Refresh{}, fmt.Errorf("V1 computation %d must be an object: %w", i, err)
		}
		if _, ok := computation["delta"]; ok {
			return Refresh{}, fmt.Errorf("V1 computation %d contains unsupported delta", i)
		}
		if query, ok := computation["instaql-query"]; !ok || rawIsNull(query) {
			return Refresh{}, fmt.Errorf("V1 computation %d missing instaql-query", i)
		}
		if result, ok := computation["instaql-result"]; !ok || rawIsNull(result) {
			return Refresh{}, fmt.Errorf("V1 computation %d missing instaql-result", i)
		}
		var queryObject map[string]json.RawMessage
		if err := json.Unmarshal(computation["instaql-query"], &queryObject); err != nil || queryObject == nil {
			return Refresh{}, fmt.Errorf("V1 computation %d instaql-query must be an object", i)
		}
	}
	if len(computations) == 0 {
		attrs, ok := fields["attrs"]
		if !ok || rawIsNull(attrs) {
			return Refresh{}, fmt.Errorf("V1 metadata-only refresh missing attrs")
		}
		processed := scalarString(fields["processed-tx-id"])
		// The transaction id is retained as wire evidence only. An attrs-only
		// refresh carries no semantic query state transition, so it must never
		// advance the materialized state's version.
		return Refresh{QueryID: queryID, Kind: RefreshNoop, ProcessedTransactionID: processed, At: ev.At}, nil
	}
	return decodeWireRefresh(ev, queryID, aliases, expectedQuery)
}

func validateV1RefreshField(fields map[string]json.RawMessage, name string, validate func(json.RawMessage) error) error {
	raw, ok := fields[name]
	if !ok || rawIsNull(raw) {
		return fmt.Errorf("is missing")
	}
	if err := validate(raw); err != nil {
		return err
	}
	return nil
}

func validateV1Integer(raw json.RawMessage) error {
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] == '"' {
		return fmt.Errorf("must be an integer")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var number json.Number
	if err := decoder.Decode(&number); err != nil {
		return fmt.Errorf("must be an integer")
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil || value < 0 {
		return fmt.Errorf("must be a non-negative integer")
	}
	return nil
}

func validateV1Attrs(raw json.RawMessage) error {
	if rawIsNull(raw) {
		return fmt.Errorf("must be an array")
	}
	var attrs []json.RawMessage
	if err := json.Unmarshal(raw, &attrs); err != nil {
		return fmt.Errorf("must be an array: %w", err)
	}
	for i, attrRaw := range attrs {
		var attr map[string]json.RawMessage
		if err := json.Unmarshal(attrRaw, &attr); err != nil || attr == nil {
			return fmt.Errorf("item %d must be an object", i)
		}
	}
	return nil
}

func validateV1ISN(raw json.RawMessage) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil || !validV1ISN(value) {
		return fmt.Errorf("must be an ISN string in slot/LSN form")
	}
	return nil
}

func validV1ISN(value string) bool {
	parts := strings.Split(value, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return false
	}
	for _, part := range parts {
		for _, r := range part {
			if (r < '0' || r > '9') && (r < 'a' || r > 'f') && (r < 'A' || r > 'F') {
				return false
			}
		}
	}
	return true
}
