package backup_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

var (
	dsnOnce  sync.Once
	dsnValue string
)

// testDSN returns the DATABASE_URL, redirected to a private database clone
// (instant_v2_backup_test) so sibling workstreams wiping the shared
// instant_v2_test schema cannot corrupt fixtures mid-run. Falls back to the
// raw DSN if the clone cannot be provisioned.
func testDSN(t *testing.T) string {
	t.Helper()
	dsnOnce.Do(func() { dsnValue = isolateDSN(t, os.Getenv("DATABASE_URL")) })
	if dsnValue == "" {
		t.Skip("DATABASE_URL not set")
	}
	return dsnValue
}

func isolateDSN(t *testing.T, raw string) string {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") || u.Path == "" {
		return raw
	}
	const target = "instant_v2_backup_test"
	if u.Path == "/"+target {
		return raw
	}
	admin := *u
	admin.Path = "/postgres"
	pool, err := pgxpool.New(context.Background(), admin.String())
	if err != nil {
		return raw
	}
	defer pool.Close()
	var exists bool
	if err := pool.QueryRow(context.Background(),
		`SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname=$1)`, target).Scan(&exists); err != nil {
		return raw
	}
	if !exists {
		if _, err := pool.Exec(context.Background(), `CREATE DATABASE `+target); err != nil {
			return raw
		}
	}
	out := *u
	out.Path = "/" + target
	return out.String()
}

// env follows internal/authn/authn_test.go's fixture pattern: drop schema →
// Migrate → seed one user + app.
func env(t *testing.T) (*pgxpool.Pool, [16]byte, func()) {
	t.Helper()
	dsn := testDSN(t)
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}
	appID := newUUID()
	creator := newUUID()
	if _, err := pool.Exec(ctx,
		`INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "backup@test"); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.CreateApp(ctx, tx, creator, appID, "backup-test"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	cleanup := func() { pool.Close(); _ = sqldb.Close() }
	return pool, appID, cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func uuidStr(u [16]byte) string { return platform.UUIDToStr(u) }

// seedTodoApp creates attrs and inserts 5 triples per entity:
// text (blob one), done (blob one), priority number 40+i (blob one), tags
// "urgent"+"home" (blob many).
func seedTodoApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte, n int) {
	t.Helper()
	var attrs []platform.Attr
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, def := range []struct {
		label, card string
		uniq, idx   bool
	}{
		{"text", "one", false, true},
		{"done", "one", false, false},
		{"priority", "one", false, true},
		{"tags", "many", false, false},
	} {
		a, aerr := platform.GetOrCreateAttr(ctx, tx, appID, "todo", def.label, "blob", def.card, def.uniq, def.idx)
		if aerr != nil {
			t.Fatal(aerr)
		}
		attrs = append(attrs, a)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	st := storage.New(pool)
	for i := 0; i < n; i++ {
		e := newUUID()
		ts := []triple.Triple{
			{E: e, A: attrs[0].ID, V: fmt.Sprintf("todo number %d", i)},
			{E: e, A: attrs[1].ID, V: i%2 == 0},
			{E: e, A: attrs[2].ID, V: float64(40 + i)}, // checksum-corruption target
			{E: e, A: attrs[3].ID, V: "urgent"},
			{E: e, A: attrs[3].ID, V: "home"},
		}
		if _, err := st.InsertTriples(ctx, appID, cat, ts, false); err != nil {
			t.Fatal(err)
		}
	}
	// One journal row so the export covers the transactions section.
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := storage.RecordTransaction(ctx, tx, appID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func runInstaql(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte) map[string]json.RawMessage {
	t.Helper()
	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	q, err := instaql.Coerce(map[string]any{"todo": map[string]any{}})
	if err != nil {
		t.Fatal(err)
	}
	x := &instaql.Executor{DB: pool}
	res, err := x.Run(ctx, q, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	return res.Data
}

func compactJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func exportApp(t *testing.T, ctx context.Context, pool *pgxpool.Pool, appID [16]byte, opts backup.ExportOptions) ([]byte, backup.Counts) {
	t.Helper()
	var buf bytes.Buffer
	counts, err := backup.Export(ctx, &buf, pool, appID, opts)
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), counts
}

// remigrateSchema drops + re-migrates and returns a fresh pool (the old
// pool's cached statements go stale across DDL).
func remigrateSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) *pgxpool.Pool {
	t.Helper()
	dsn := testDSN(t)
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	fresh, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}
	return fresh
}

// TestExportImportRoundTrip seeds an app via the authn-style fixture, exports
// it, wipes + re-migrates the schema, imports the dump, and asserts instaql
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

	pool = remigrateSchema(t, ctx, pool)

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

func creatorID(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [16]byte {
	t.Helper()
	var id [16]byte
	if err := pool.QueryRow(ctx, `SELECT id FROM instant_users LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// TestV1DumpImport hand-crafts a v1-shaped export zip (config.json first,
// entities/<etype>.jsonl next — the layout restore.clj consumes) and asserts
// the importer materializes triples from it.
func TestV1DumpImport(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	e1, e2 := newUUID(), newUUID()
	configJSON := `{
	  "title": "legacy app",
	  "rules": {"fn": "true"},
	  "schema": {
	    "blobs": {
	      "todo": {
	        "text":     {"valueType": "string",  "config": {"unique": false, "indexed": true, "required": true}},
	        "priority": {"valueType": "number",  "config": {"unique": false, "indexed": false}},
	        "done":     {"valueType": "boolean", "config": {"unique": false, "indexed": false}}
	      }
	    },
	    "refs": {}
	  }
	}`
	line := func(id, text string, priority int, done bool) string {
		return fmt.Sprintf(`{"entity":{"id":%q,"text":%q,"priority":%d,"done":%t},"createdAt":1700000000000}`, id, text, priority, done)
	}
	entityData := strings.Join([]string{
		line(uuidStr(e1), "from v1 one", 1, true),
		line(uuidStr(e2), "from v1 two", 2, false),
	}, "\n")

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	wCfg, err := zw.Create("config.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wCfg.Write([]byte(configJSON)); err != nil {
		t.Fatal(err)
	}
	wEnt, err := zw.Create("entities/todo.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wEnt.Write([]byte(entityData)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(zipBuf.Bytes())
	counts, err := backup.RestoreV1Zip(ctx, pool, reader, int64(reader.Len()), appID)
	if err != nil {
		t.Fatalf("RestoreV1Zip: %v", err)
	}
	// attrs: text/priority/done + implicit id; each entity also carries its id
	// triple, plus the rules row.
	if counts.Triples != 8 || counts.Attrs != 4 || counts.Rules != 1 {
		t.Fatalf("unexpected v1 import counts: %+v", counts)
	}
	var textRequired, idRequired bool
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='todo' AND label='text'`, appID).Scan(&textRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='todo' AND label='id'`, appID).Scan(&idRequired); err != nil {
		t.Fatal(err)
	}
	if !textRequired || !idRequired {
		t.Fatalf("v1 requiredness not preserved: text=%v id=%v", textRequired, idRequired)
	}

	st := storage.New(pool)
	got, err := st.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e1, e2}})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]bool{}
	for _, g := range got {
		switch v := g.Triple.V.(type) {
		case string:
			values[v] = true
		case json.Number:
			values["num:"+v.String()] = true
		case bool:
			values[fmt.Sprintf("bool:%v", v)] = true
		}
	}
	for _, want := range []string{"from v1 one", "from v1 two", "num:1", "num:2", "bool:true", "bool:false"} {
		if !values[want] {
			t.Fatalf("missing triple value %q; got %v", want, values)
		}
	}
}

// TestV1EntitiesLinksImport covers the current v1 config shape. V1 has also
// emitted the older schema.blobs/schema.refs shape, which TestV1DumpImport
// above keeps covered for compatibility.
func TestV1EntitiesLinksImport(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	postID, userID := newUUID(), newUUID()
	configJSON := fmt.Sprintf(`{
      "title":"current v1",
      "schema": {
		  "entities": {
		    "posts": {"attrs": {"title": {"valueType":"any", "config":{"unique":false,"indexed":true,"required":true}}}},
          "users": {"attrs": {}}
        },
        "links": {
          "postAuthor": {"forward":{"on":"posts","label":"author","has":"one","required":true},"reverse":{"on":"users","label":"posts","has":"many"}}
        }
      }
    }`)
	postLine := fmt.Sprintf(`{"entity":{"id":%q,"title":["hello","world"],"author":%q},"createdAt":1700000000000}`, uuidStr(postID), uuidStr(userID))
	userLine := fmt.Sprintf(`{"entity":{"id":%q},"createdAt":1700000000000}`, uuidStr(userID))

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	wCfg, err := zw.Create("config.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wCfg.Write([]byte(configJSON)); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"entities/posts.jsonl": postLine, "entities/users.jsonl": userLine} {
		w, werr := zw.Create(name)
		if werr != nil {
			t.Fatal(werr)
		}
		if _, werr := w.Write([]byte(body)); werr != nil {
			t.Fatal(werr)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(zipBuf.Bytes())
	counts, err := backup.RestoreV1Zip(ctx, pool, reader, int64(reader.Len()), appID)
	if err != nil {
		t.Fatalf("RestoreV1Zip: %v", err)
	}
	if counts.Attrs != 4 || counts.Triples != 4 {
		t.Fatalf("unexpected current-v1 import counts: %+v", counts)
	}
	var titleRequired, idRequired, authorRequired bool
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='title'`, appID).Scan(&titleRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='id'`, appID).Scan(&idRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='author'`, appID).Scan(&authorRequired); err != nil {
		t.Fatal(err)
	}
	if !titleRequired || !idRequired || !authorRequired {
		t.Fatalf("current-v1 requiredness not preserved: title=%v id=%v author=%v", titleRequired, idRequired, authorRequired)
	}
	var titleTriples int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM triples t JOIN attrs a ON a.id=t.attr_id WHERE t.app_id=$1 AND a.etype='posts' AND a.label='title'`, appID).Scan(&titleTriples); err != nil {
		t.Fatal(err)
	}
	if titleTriples != 1 {
		t.Fatalf("one-cardinality array was expanded into %d triples", titleTriples)
	}
}

// TestExportResumeOffset verifies ?from-offset=N semantics: the first N
// logical records are skipped, the emitted dump still self-verifies on
// import, and counts reflect only what was emitted.
func TestExportResumeOffset(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appID, 2)
	full, fullCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if fullCounts.Transactions != 1 {
		t.Fatalf("expected 1 journal record in full dump: %+v", fullCounts)
	}
	if got := strings.Count(string(full), `"kind":"triple"`); got != 10 {
		t.Fatalf("full dump triple records: %d", got)
	}

	// Skip the first 2 logical records (both attrs).
	partial, partCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{FromOffset: 2})
	if partCounts.Attrs != fullCounts.Attrs-2 || partCounts.Triples != fullCounts.Triples {
		t.Fatalf("offset counts wrong: partial=%+v full=%+v", partCounts, fullCounts)
	}
	if !bytes.Contains(partial, []byte(`"kind":"header"`)) || !bytes.Contains(partial, []byte(`"kind":"checksum"`)) {
		t.Fatal("partial dump must keep header and checksum lines")
	}
	// First two attr lines must be gone (attrs are ordered by id).
	nAttrs := strings.Count(string(partial), `{"kind":"attr"`)
	if nAttrs != 2 {
		t.Fatalf("expected 2 remaining attr lines, got %d", nAttrs)
	}

	pool = remigrateSchema(t, ctx, pool)
	// Import of the partial dump fails cleanly (triples reference missing
	// attrs) but its checksum still validates ordering-wise; assert the
	// failure is about unknown attrs, not checksum.
	_, err := backup.Import(ctx, pool, bytes.NewReader(partial), appID)
	if err == nil || !strings.Contains(err.Error(), "unknown attrs") {
		t.Fatalf("expected unknown-attr error for partial dump, got: %v", err)
	}

	pool2, appID2, cleanup2 := env(t)
	defer cleanup2()
	seedTodoApp(t, ctx, pool2, appID2, 1)
	// Export skipping all triples+attrs+rule → only transactions remain.
	all, _ := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{})
	totalRecords := countLogical(all)
	tail, tailCounts := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{FromOffset: totalRecords - tailCount(pool2, appID2, t)})
	if tailCounts.Transactions <= 0 || tailCounts.Triples != 0 || tailCounts.Attrs != 0 {
		t.Fatalf("tail counts unexpected: %+v", tailCounts)
	}
	pool2 = remigrateSchema(t, ctx, pool2)
	if _, err := backup.Import(ctx, pool2, bytes.NewReader(tail), appID2); err != nil {
		t.Fatalf("tail-only import failed: %v", err)
	}
}

// countLogical counts logical record lines (header/checksum excluded).
func countLogical(dump []byte) int {
	n := 0
	for _, kind := range []string{"attr", "triple", "rule", "transaction"} {
		n += bytes.Count(dump, []byte(`{"kind":"`+kind+`"`))
	}
	return n
}

func tailCount(pool *pgxpool.Pool, appID [16]byte, t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(),
		`SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestExportMemoryCap proves streaming: exporting 100k triples keeps heap
// growth under a fixed cap (rows stream out of PG one at a time; nothing
// accumulates server- or client-side).
func TestExportMemoryCap(t *testing.T) {
	if testing.Short() {
		t.Skip("memory cap test in short mode")
	}
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	const n = 100_000
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a, err := platform.GetOrCreateAttr(ctx, tx, appID, "big", "payload", "blob", "one", false, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Bulk seed: one INSERT per 10k chunk with PG-computed md5 (cardinality-
	// one blob attr → ea=true, all other flags false). Much faster than the
	// storage helper for a pure fixture.
	const chunk = 10_000
	vals := make([]string, 0, chunk)
	idStrs := make([]string, 0, chunk)
	for base := 0; base < n; base += chunk {
		vals = vals[:0]
		idStrs = idStrs[:0]
		for i := base; i < base+chunk && i < n; i++ {
			idStrs = append(idStrs, uuidStr(newUUID()))
			vals = append(vals, fmt.Sprintf("payload-%d", i))
		}
		if _, err := pool.Exec(ctx, `
			WITH pairs AS (
				SELECT e.e AS entity_id, u.v AS value_text, u.ord
				FROM unnest($3::text[]) WITH ORDINALITY AS u(v, ord)
				JOIN unnest($4::text[]) WITH ORDINALITY AS e(e, ord) ON e.ord = u.ord
			)
			INSERT INTO triples (app_id, entity_id, attr_id, value, value_md5,
			                     ea, eav, av, ave, vae)
			SELECT $1, p.entity_id::uuid, $2, to_json(p.value_text),
			       md5(to_json(p.value_text)::text), true, false, false, false, false
			FROM pairs p`,
			appID, a.ID, vals, idStrs); err != nil {
			t.Fatal(err)
		}
	}

	runtime.GC()
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	type countingWriter struct {
		n    int64
		last strings.Builder
	}
	cw := &countingWriter{}
	w := ioWriterFunc(func(p []byte) (int, error) {
		cw.n += int64(len(p))
		if cw.n > cwKeptBytes {
			// keep only the trailing window
			s := cw.last.String()
			cw.last.Reset()
			s += string(p)
			if len(s) > cwKeptBytes {
				s = s[len(s)-cwKeptBytes:]
			}
			cw.last.WriteString(s)
		} else {
			cw.last.Write(p)
		}
		return len(p), nil
	})

	counts, err := backup.Export(ctx, w, pool, appID, backup.ExportOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if counts.Triples != n {
		t.Fatalf("exported %d triples, want %d", counts.Triples, n)
	}
	tail := cw.last.String()
	if !strings.Contains(tail, `"kind":"checksum"`) || !strings.Contains(tail, `"records":`) {
		t.Fatalf("missing checksum trailer in tail: %q", tail)
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if growth < 0 {
		growth = 0
	}
	const capBytes = 32 << 20
	if growth > capBytes {
		t.Fatalf("export accumulated %d bytes of live heap (> %d) — not streaming?", growth, capBytes)
	}
}

const cwKeptBytes = 512

func ioWriterFunc(fn func([]byte) (int, error)) io.Writer { return fnWriter{fn} }

type fnWriter struct{ fn func([]byte) (int, error) }

func (f fnWriter) Write(p []byte) (int, error) { return f.fn(p) }

// ---- HTTP surface ----

type fakeAuth struct{ valid string }

func (f fakeAuth) check(_ context.Context, appID, token string) (bool, error) {
	return token == f.valid, nil
}

func TestHandlerRoutesAndAuth(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check}
	appStr := uuidStr(appID)

	do := func(method, path string, body []byte, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// No token → 401.
	if rec := do(http.MethodGet, "/backup/"+appStr, nil, nil); rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated export: got %d", rec.Code)
	}
	// Bad token → 401.
	if rec := do(http.MethodGet, "/backup/"+appStr, nil, map[string]string{"Authorization": "Bearer nope"}); rec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-token export: got %d", rec.Code)
	}

	seedTodoApp(t, ctx, pool, appID, 2)

	// Valid token → 200 NDJSON stream.
	rec := do(http.MethodGet, "/backup/"+appStr, nil, map[string]string{"X-admin-token": "tok-123"})
	if rec.Code != http.StatusOK {
		t.Fatalf("export: got %d body=%s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/x-ndjson" {
		t.Fatalf("content-type: %q", ct)
	}
	if ar := rec.Header().Get("Accept-Ranges"); ar != "records" {
		t.Fatalf("accept-ranges: %q", ar)
	}
	dump := rec.Body.Bytes()
	if !strings.HasPrefix(string(dump), `{"kind":"header"`) {
		t.Fatalf("dump does not start with header line: %q", firstLine(dump))
	}

	// POST restore round-trips through HTTP.
	rec = do(http.MethodPost, "/backup/"+appStr+"/restore", dump, map[string]string{"Authorization": "Bearer tok-123"})
	if rec.Code != http.StatusOK {
		t.Fatalf("restore: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Counts backup.Counts `json:"counts"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Counts.Triples != 10 || resp.Counts.Attrs != 4 {
		t.Fatalf("restore counts: %+v", resp.Counts)
	}

	// Corrupted restore body → 400 with message. Rewrite the first numeric
	// value (priorities are 40+i) so the JSON stays valid but the checksum
	// must catch it.
	bad := append([]byte{}, dump...)
	if i := bytes.Index(bad, []byte(`"value":4`)); i >= 0 {
		copy(bad[i:], []byte(`"value":9`))
	}
	rec = do(http.MethodPost, "/backup/"+appStr+"/restore", bad, map[string]string{"Authorization": "Bearer tok-123"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("corrupted restore: got %d", rec.Code)
	}

	// Unknown app → 404 (auth passes).
	foreign := newUUID()
	rec = do(http.MethodGet, "/backup/"+uuidStr(foreign), nil, map[string]string{"Authorization": "Bearer tok-123"})
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown app export: got %d", rec.Code)
	}

	// Bad from-offset → 400.
	rec = do(http.MethodGet, "/backup/"+appStr+"?from-offset=-1", nil, map[string]string{"Authorization": "Bearer tok-123"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("negative offset: got %d", rec.Code)
	}
}

func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}
