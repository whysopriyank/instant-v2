package transact

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// isLookupRef reports whether raw is a lookup ref tuple [attrId, value].
func isLookupRef(raw json.RawMessage) bool {
	var arr []json.RawMessage
	if err := json.Unmarshal(raw, &arr); err != nil {
		return false
	}
	if len(arr) != 2 {
		return false
	}
	var attrID string
	if err := json.Unmarshal(arr[0], &attrID); err != nil || len(attrID) != 36 {
		return false
	}
	return true
}

// resolveEntityIDs rewrites any lookup-ref eids in triple steps to bare uuids.
func resolveEntityIDs(ctx context.Context, tx pgx.Tx, appID [16]byte, steps []Step, cat *platform.AttrCatalog) error {
	for i, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple", "retract-triple":
			if !isLookupRef(st.Args[0]) {
				continue
			}
			var arr []json.RawMessage
			_ = json.Unmarshal(st.Args[0], &arr)
			var attrIDStr string
			_ = json.Unmarshal(arr[0], &attrIDStr)
			var attrID [16]byte
			if err := parseUUID(attrIDStr, &attrID); err != nil {
				return err
			}
			attr, ok := cat.ByID(attrID)
			if !ok {
				return fmt.Errorf("lookup: unknown attr %s", attrIDStr)
			}
			if !attr.IsUnique {
				return fmt.Errorf("lookup: attr %s/%s is not unique", deref(attr.Etype), deref(attr.Label))
			}
			enc, err := normalizeValue(arr[1])
			if err != nil {
				return err
			}
			var eid [16]byte
			err = tx.QueryRow(ctx, `
				SELECT entity_id FROM triples
				 WHERE app_id=$1 AND attr_id=$2 AND value=$3::jsonb
				 LIMIT 1`, appID, attrID, string(enc)).Scan(&eid)
			if err == pgx.ErrNoRows {
				return fmt.Errorf("lookup: no entity with %s.%s=%s", deref(attr.Etype), deref(attr.Label), string(arr[1]))
			}
			if err != nil {
				return err
			}
			steps[i].Args[0] = mustMarshalJSON(uuidToStr(eid))
		}
	}
	return nil
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func normalizeValue(raw json.RawMessage) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("invalid JSON value: multiple values")
		}
		return nil, err
	}
	v = canonicalizeLookupValue(v)
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func canonicalizeLookupValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for key, value := range x {
			x[key] = canonicalizeLookupValue(value)
		}
		return x
	case []any:
		for i, value := range x {
			x[i] = canonicalizeLookupValue(value)
		}
		return x
	case json.Number:
		return canonicalNumber(x)
	default:
		return v
	}
}

// canonicalNumber preserves exact decimal semantics for lookup cache keys:
// equivalent spellings such as 1, 1.0, and 1e0 match, while adjacent large
// integers remain distinct. Exponents are manipulated symbolically so a huge
// exponent never expands into a large decimal string.
func canonicalNumber(n json.Number) json.Number {
	s := string(n)
	sign := ""
	if strings.HasPrefix(s, "-") {
		sign, s = "-", s[1:]
	}
	exponent := new(big.Int)
	if i := strings.IndexAny(s, "eE"); i >= 0 {
		exponent.SetString(s[i+1:], 10)
		s = s[:i]
	}
	if i := strings.IndexByte(s, '.'); i >= 0 {
		exponent.Sub(exponent, big.NewInt(int64(len(s)-i-1)))
		s = s[:i] + s[i+1:]
	}
	s = strings.TrimLeft(s, "0")
	if s == "" {
		return "0"
	}
	digits := strings.TrimRight(s, "0")
	exponent.Add(exponent, big.NewInt(int64(len(s)-len(digits))))
	point := new(big.Int).Add(exponent, big.NewInt(int64(len(digits))))
	if point.IsInt64() && point.Int64() > -6 && point.Int64() <= 21 {
		p := int(point.Int64())
		switch {
		case p <= 0:
			s = "0." + strings.Repeat("0", -p) + digits
		case p >= len(digits):
			s = digits + strings.Repeat("0", p-len(digits))
		default:
			s = digits[:p] + "." + digits[p:]
		}
	} else {
		s = digits[:1]
		if len(digits) > 1 {
			s += "." + digits[1:]
		}
		s += "e" + point.Sub(point, big.NewInt(1)).String()
	}
	return json.Number(sign + s)
}

func mustMarshalJSON(s string) json.RawMessage {
	b, _ := json.Marshal(s)
	return b
}

// resolveValues handles Phase 2 value lookups: when a ref value is given as
// [attrId, scalar] (linking by unique attribute), resolve it to a uuid string.
func resolveValues(ctx context.Context, tx pgx.Tx, appID [16]byte, steps []Step, cat *platform.AttrCatalog) error {
	for i, st := range steps {
		switch st.Op {
		case "add-triple", "deep-merge-triple":
			if !isLookupRef(st.Args[2]) {
				continue
			}
			var arr []json.RawMessage
			_ = json.Unmarshal(st.Args[2], &arr)
			var attrIDStr string
			_ = json.Unmarshal(arr[0], &attrIDStr)
			var attrID [16]byte
			if err := parseUUID(attrIDStr, &attrID); err != nil {
				return err
			}
			enc, _ := normalizeValue(arr[1])
			la, ok := cat.ByID(attrID)
			if !ok {
				return fmt.Errorf("value lookup: unknown attr %s", attrIDStr)
			}
			if !la.IsUnique {
				// Audit M3: value-position lookups on non-unique attrs
				// previously scanned an arbitrary matching row, silently
				// linking to a nondeterministic entity.
				return fmt.Errorf(
					"value lookup: attr %s is not unique", attrIDStr)
			}
			var eid [16]byte
			err := tx.QueryRow(ctx, `
				SELECT entity_id FROM triples
				 WHERE app_id=$1 AND attr_id=$2 AND value=$3::jsonb`,
				appID, attrID, string(enc)).Scan(&eid)
			if err == pgx.ErrNoRows {
				return fmt.Errorf("value lookup: no entity for attr %s", attrIDStr)
			}
			if err != nil {
				return err
			}
			steps[i].Args[2] = mustMarshalJSON(uuidToStr(eid))
		}
	}
	return nil
}

func uuidToStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }
