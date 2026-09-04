package storage

import (
	"context"
	"testing"

	"github.com/instant-v2/instant-v2/internal/triple"
)

// CopyTriples must share InsertTriples' JSON encoding and cardinality-one
// semantics: nil is JSON null, and the last input wins within one batch.
func TestCopyTriplesNullAndLastCardinalityOneInputWins(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	appID, cat, ids := seedCatalog(t, db)
	nilEntity, overwriteEntity := rand16(), rand16()

	n, err := db.CopyTriples(ctx, appID, cat, []triple.Triple{
		{E: nilEntity, A: ids.name, V: nil},
		{E: overwriteEntity, A: ids.name, V: "first"},
		{E: overwriteEntity, A: ids.name, V: "last"},
	})
	if err != nil {
		t.Fatalf("CopyTriples: %v", err)
	}
	if n != 2 {
		t.Fatalf("n=%d want 2 (one null row and one cardinality-one winner)", n)
	}

	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{nilEntity, overwriteEntity}})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows=%d want 2", len(rows))
	}
	for _, row := range rows {
		switch row.Triple.E {
		case nilEntity:
			if row.Triple.V != nil {
				t.Fatalf("nil copy value=%v want nil", row.Triple.V)
			}
			if row.MD5 != triple.JSONNullMD5 {
				t.Fatalf("nil copy md5=%q want %q", row.MD5, triple.JSONNullMD5)
			}
		case overwriteEntity:
			if row.Triple.V != "last" {
				t.Fatalf("cardinality-one value=%v want last", row.Triple.V)
			}
		default:
			t.Fatalf("unexpected entity %x", row.Triple.E)
		}
	}
}
