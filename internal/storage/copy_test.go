package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// CopyTriples must share InsertTriples' JSON encoding and cardinality-one
// semantics: nil is JSON null, and the last input wins within one batch.
func TestCopyTriplesNullAndLastCardinalityOneInputWins(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	appID, cat, ids := seedCatalog(t, db)
	nilEntity, stringNullEntity, overwriteEntity := rand16(), rand16(), rand16()

	n, err := db.CopyTriples(ctx, appID, cat, []triple.Triple{
		{E: nilEntity, A: ids.name, V: nil},
		{E: stringNullEntity, A: ids.name, V: "null"},
		{E: overwriteEntity, A: ids.name, V: "first"},
		{E: overwriteEntity, A: ids.name, V: "last"},
	})
	if err != nil {
		t.Fatalf("CopyTriples: %v", err)
	}
	if n != 3 {
		t.Fatalf("n=%d want 3 (JSON null, string null, and one cardinality-one winner)", n)
	}

	rows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{nilEntity, stringNullEntity, overwriteEntity}})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("rows=%d want 3", len(rows))
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
		case stringNullEntity:
			if row.Triple.V != "null" || row.MD5 == triple.JSONNullMD5 {
				t.Fatalf("string null copy value/md5=%#v/%q", row.Triple.V, row.MD5)
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

func TestCopyTriplesMixedCardinalityMatchesInsertTriples(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	appID, cat, ids := seedCatalog(t, db)
	copyEntity, insertEntity := rand16(), rand16()
	targetA, targetB := rand16(), rand16()

	values := func(entity [16]byte) []triple.Triple {
		return []triple.Triple{
			{E: entity, A: ids.name, V: nil},
			{E: entity, A: ids.name, V: "last"},
			{E: entity, A: ids.link, V: uuidStr(targetA)},
			{E: entity, A: ids.link, V: uuidStr(targetB)},
		}
	}
	if n, err := db.CopyTriples(ctx, appID, cat, values(copyEntity)); err != nil || n != 3 {
		t.Fatalf("CopyTriples mixed = n %d err %v", n, err)
	}
	if result, err := db.InsertTriples(ctx, appID, cat, values(insertEntity), false); err != nil || result.Upserted != 1 || result.Inserted != 2 {
		t.Fatalf("InsertTriples mixed = %+v err %v", result, err)
	}

	copyRows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{copyEntity}})
	if err != nil {
		t.Fatal(err)
	}
	insertRows, err := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{insertEntity}})
	if err != nil {
		t.Fatal(err)
	}
	canonical := func(rows []Enhanced) []string {
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			encoded, err := triple.EncodeValue(row.Triple.V)
			if err != nil {
				t.Fatal(err)
			}
			checked := ""
			if row.CheckedDataType != nil {
				checked = *row.CheckedDataType
			}
			out = append(out, fmt.Sprintf("%x|%s|%s|%t|%t|%t|%t|%t|%s",
				row.Triple.A, encoded, row.MD5,
				row.Flags.EA, row.Flags.EAV, row.Flags.AV, row.Flags.AVE, row.Flags.VAE, checked))
		}
		sort.Strings(out)
		return out
	}
	if got, want := canonical(copyRows), canonical(insertRows); !reflect.DeepEqual(got, want) {
		t.Fatalf("COPY/insert parity = %#v / %#v", got, want)
	}
}

func TestCopyTriplesUniqueConflictRollsBack(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	appID, _, ids := seedCatalog(t, db)
	baseline, contenderA, contenderB := rand16(), rand16(), rand16()
	var uniqueMany platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var err error
		uniqueMany, err = platform.GetOrCreateAttr(ctx, tx, appID, "users", "alias", "blob", "many", true, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{{E: baseline, A: ids.name, V: "before"}}, false); err != nil {
		t.Fatal(err)
	}

	_, err = db.CopyTriples(ctx, appID, cat, []triple.Triple{
		{E: baseline, A: ids.name, V: "must-roll-back"},
		{E: contenderA, A: uniqueMany.ID, V: "same-alias"},
		{E: contenderB, A: uniqueMany.ID, V: "same-alias"},
	})
	if !errors.Is(err, ErrUniqueViolation) {
		t.Fatalf("CopyTriples conflict = %v; want ErrUniqueViolation", err)
	}
	rows, fetchErr := db.FetchTriples(ctx, appID, FetchFilter{EntityIDs: [][16]byte{baseline, contenderA, contenderB}})
	if fetchErr != nil {
		t.Fatal(fetchErr)
	}
	if len(rows) != 1 || rows[0].Triple.E != baseline || rows[0].Triple.V != "before" {
		t.Fatalf("COPY conflict left partial state: %#v", rows)
	}
}
