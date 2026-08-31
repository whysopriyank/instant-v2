package backup_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestExportImportRoundTrip seeds an app via the authn-style fixture, exports
// it, imports into a fresh database, and asserts instaql
// results are identical pre/post. Re-importing the same dump is idempotent.
func TestExportImportRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appID, 4)
	before := compactJSON(t, runInstaql(t, ctx, pool, appID))

	dump, expCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if expCounts.Attrs != 4 || expCounts.Triples != 20 || expCounts.Rules != 0 || expCounts.Transactions == 0 {
		t.Fatalf("unexpected export counts: %+v", expCounts)
	}

	pool = newDatabase(t)

	imported, err := backup.Import(ctx, pool, bytes.NewReader(dump), appID)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if imported.Triples != 20 || imported.Attrs != 4 {
		t.Fatalf("unexpected import counts: %+v", imported)
	}

	after := compactJSON(t, runInstaql(t, ctx, pool, appID))
	if before != after {
		t.Fatalf("instaql results diverged:\nbefore=%s\nafter=%s", before, after)
	}

	// Idempotency: re-importing the same dump succeeds as a no-op.
	reimported, err := backup.Import(ctx, pool, bytes.NewReader(dump), appID)
	if err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if reimported.Triples != imported.Triples || reimported.Attrs != imported.Attrs {
		t.Fatalf("re-import changed counts: %+v vs %+v", reimported, imported)
	}
	again := compactJSON(t, runInstaql(t, ctx, pool, appID))
	if again != after {
		t.Fatal("re-import changed query results")
	}
}

// TestChecksumCorruption flips a digit inside a triple's numeric value (still
// valid JSON) and asserts import rejects the dump on checksum mismatch.
func TestChecksumCorruption(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appID, 2)
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	// Priorities are 40+i; rewrite the first numeric value to a different
	// number (stays valid JSON, so the checksum — not the parser — must
	// catch it).
	marker := []byte(`"value":4`)
	idx := bytes.Index(dump, marker)
	if idx < 0 {
		t.Fatalf("corruption target not found in dump:\n%s", dump)
	}
	corrupt := make([]byte, len(dump))
	copy(corrupt, dump)
	copy(corrupt[idx:], []byte(`"value":9`))
	if bytes.Equal(corrupt, dump) {
		t.Fatal("corruption was a no-op")
	}

	_, err := backup.Import(ctx, pool, bytes.NewReader(corrupt), appID)
	if err == nil {
		t.Fatal("expected import to fail on corrupted dump")
	}
	if !strings.Contains(err.Error(), "checksum") && !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("expected checksum error, got: %v", err)
	}
}

// TestImportRejectsCrossAppDump pins the tenant boundary on restore: a dump
// whose header carries app B's id must be refused when imported through an
// authenticated session for app A, with zero rows written for B.
func TestImportRejectsCrossAppDump(t *testing.T) {
	ctx := context.Background()
	pool, appA, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appA, 2)
	dump, _ := exportApp(t, ctx, pool, appA, backup.ExportOptions{})

	appB := newUUID()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.CreateApp(ctx, tx, creatorID(t, ctx, pool), appB, "victim"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	_, err = backup.Import(ctx, pool, bytes.NewReader(dump), appB)
	if !errors.Is(err, backup.ErrAppMismatch) {
		t.Fatalf("expected ErrAppMismatch, got: %v", err)
	}

	var triples, attrs int
	if err := pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM triples WHERE app_id=$1),
		        (SELECT count(*) FROM attrs   WHERE app_id=$1)`, appB).Scan(&triples, &attrs); err != nil {
		t.Fatal(err)
	}
	if triples != 0 || attrs != 0 {
		t.Fatalf("cross-app restore wrote rows into the target app: triples=%d attrs=%d", triples, attrs)
	}
}

// TestImportRejectsForeignAttr pins the attr-id tenant guard: attr ids are
// public (clients receive them in init-ok), so a dump must never mutate an
// attr row owned by a different app even when the header matches the route.
func TestImportRejectsForeignAttr(t *testing.T) {
	ctx := context.Background()
	pool, appA, cleanup := env(t)
	defer cleanup()

	// App B owns an indexed attr whose id leaks publicly in normal operation.
	appB := newUUID()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.CreateApp(ctx, tx, creatorID(t, ctx, pool), appB, "other"); err != nil {
		t.Fatal(err)
	}
	foreign, err := platform.GetOrCreateAttr(ctx, tx, appB, "secret", "body", "blob", "one", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Hand-build a minimal valid dump FOR APP A that redefines B's attr with
	// flipped flags (the mutation an attacker would attempt).
	header := fmt.Sprintf(`{"kind":"header","format":%q,"version":1,"app_id":%q,"title":"x","creator_id":%q}`,
		"instant-v2-backup", uuidStr(appA), uuidStr(appA))
	attrLine := fmt.Sprintf(
		`{"kind":"attr","id":%q,"etype":"secret","label":"body","reverse_etype":null,"reverse_label":null,`+
			`"value_type":"blob","cardinality":"many","is_unique":true,"is_indexed":false,`+
			`"forward_ident":%q,"reverse_ident":null,"checked_data_type":null,`+
			`"checking_data_type":null,"deletion_marked_at":null}`,
		uuidStr(foreign.ID), uuidStr(foreign.ForwardIdent))
	hash := sha256.New()
	hash.Write([]byte(header + "\n"))
	hash.Write([]byte(attrLine + "\n"))
	dump := []byte(header + "\n" + attrLine + "\n" +
		fmt.Sprintf(`{"kind":"checksum","sha256":"%x","records":1}`, hash.Sum(nil)) + "\n")

	_, err = backup.Import(ctx, pool, bytes.NewReader(dump), appA)
	if err == nil || !strings.Contains(err.Error(), "different app") {
		t.Fatalf("expected foreign-attr rejection, got: %v", err)
	}

	// App B's row must be untouched.
	var indexed, unique bool
	if err := pool.QueryRow(ctx,
		`SELECT is_indexed, is_unique FROM attrs WHERE id=$1`, foreign.ID).Scan(&indexed, &unique); err != nil {
		t.Fatal(err)
	}
	if indexed != true || unique != false {
		t.Fatalf("foreign attr was mutated: indexed=%v unique=%v", indexed, unique)
	}
}

// TestImportRejectsForeignTripleAttr ensures a native dump cannot use a
// publicly visible attr id belonging to another app when the attr record is
// omitted from the dump.
func TestImportRejectsForeignTripleAttr(t *testing.T) {
	ctx := context.Background()
	pool, appA, cleanup := env(t)
	defer cleanup()

	appB := newUUID()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.CreateApp(ctx, tx, creatorID(t, ctx, pool), appB, "other"); err != nil {
		t.Fatal(err)
	}
	foreign, err := platform.GetOrCreateAttr(ctx, tx, appB, "secret", "body", "blob", "one", false, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	header := fmt.Sprintf(`{"kind":"header","format":%q,"version":2,"app_id":%q,"title":"x","creator_id":%q}`,
		"instant-v2-backup", uuidStr(appA), uuidStr(appA))
	triple := fmt.Sprintf(`{"kind":"triple","entity_id":%q,"attr_id":%q,"value":"cross-tenant"}`,
		uuidStr(newUUID()), uuidStr(foreign.ID))
	h := sha256.New()
	h.Write([]byte(header + "\n"))
	h.Write([]byte(triple + "\n"))
	dump := header + "\n" + triple + "\n" + fmt.Sprintf(`{"kind":"checksum","sha256":"%x","records":1}`, h.Sum(nil)) + "\n"

	_, err = backup.Import(ctx, pool, strings.NewReader(dump), appA)
	if err == nil || !strings.Contains(err.Error(), "unknown attrs") {
		t.Fatalf("expected foreign triple attr rejection, got: %v", err)
	}
	var triples int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM triples WHERE app_id=$1`, appA).Scan(&triples); err != nil {
		t.Fatal(err)
	}
	if triples != 0 {
		t.Fatalf("foreign triple import wrote %d rows", triples)
	}
}

// TestV2AttrRequiredFlagIsStrict prevents a v2 dump from silently dropping
// requiredness. Version-1 dumps remain accepted for backward compatibility;
// only the v2 wire version requires a present boolean field.
func TestV2AttrRequiredFlagIsStrict(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	header := fmt.Sprintf(`{"kind":"header","format":%q,"version":2,"app_id":%q,"title":"x","creator_id":%q}`,
		"instant-v2-backup", uuidStr(appID), uuidStr(appID))
	attr := fmt.Sprintf(`{"kind":"attr","id":%q,"etype":"todo","label":"title","reverse_etype":null,"reverse_label":null,"value_type":"blob","cardinality":"one","is_unique":false,"is_indexed":false,"forward_ident":%q,"reverse_ident":null,"checked_data_type":null,"checking_data_type":null,"deletion_marked_at":null}`,
		uuidStr(newUUID()), uuidStr(newUUID()))
	for _, record := range []string{attr, strings.Replace(attr, `"deletion_marked_at":null}`, `"deletion_marked_at":null,"is_required":"yes"}`, 1)} {
		h := sha256.New()
		h.Write([]byte(header + "\n"))
		h.Write([]byte(record + "\n"))
		dump := header + "\n" + record + "\n" + fmt.Sprintf(`{"kind":"checksum","sha256":"%x","records":1}`, h.Sum(nil)) + "\n"
		if _, err := backup.Import(ctx, pool, strings.NewReader(dump), appID); err == nil || !strings.Contains(err.Error(), "is_required") {
			t.Fatalf("expected strict is_required rejection for %s, got %v", record, err)
		}
	}
}
