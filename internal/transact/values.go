package transact

import (
	"encoding/json"
	"fmt"
	"strings"
)

// tripleValueMatches mirrors triples_valid_value (migration 001) so clients
// get a clean 4xx-class error instead of an opaque CHECK-constraint failure.
// JSON null is always allowed (v1 semantics); dates accept numbers (epoch ms)
// or strings — the DB CHECK stays authoritative for string date formats.
func tripleValueMatches(cdt string, v any) error {
	if v == nil {
		return nil
	}
	bad := func() error {
		return fmt.Errorf("value does not match attr checked-data-type %q", cdt)
	}
	switch cdt {
	case "string":
		if _, ok := v.(string); !ok {
			return bad()
		}
	case "number":
		switch v.(type) {
		case float64, int64, json.Number:
		default:
			return bad()
		}
	case "boolean":
		if _, ok := v.(bool); !ok {
			return bad()
		}
	case "date":
		switch v.(type) {
		case float64, int64, json.Number, string:
		default:
			return bad()
		}
	}
	return nil
}

func parseValueJSON(raw json.RawMessage) (any, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	// Nested numbers arrive as json.Number, which triple.IsValue rejects;
	// widen them to int64/float64 everywhere so deep-merged objects encode.
	return convertNumbers(v), nil
}

func convertNumbers(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = convertNumbers(e)
		}
		return x
	case []any:
		for i, e := range x {
			x[i] = convertNumbers(e)
		}
		return x
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		f, _ := x.Float64()
		return f
	default:
		return v
	}
}
