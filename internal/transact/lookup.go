package transact

import (
	"context"
	"encoding/json"
	"fmt"

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
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, err
	}
	b, _ := json.Marshal(v)
	return b, nil
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
