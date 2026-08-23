package backup_test

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// env follows internal/authn/authn_test.go's fixture pattern: drop schema →
// Migrate → seed one user + app.
func env(t *testing.T) (*pgxpool.Pool, [16]byte, func()) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set")
	}
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

func remigrateSchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	sqldb, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatal(err)
	}
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}
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

	remigrateSchema(t, ctx, pool)

	imported, err := backup.Import(ctx, pool, bytes.NewReader(dump))
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
	reimported, err := backup.Import(ctx, pool, bytes.NewReader(dump))
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

	marker := []byte(`"value":42`)
	idx := bytes.Index(dump, marker)
	if idx < 0 {
		t.Fatalf("corruption target not found in dump:\n%s", dump)
	}
	corrupt := make([]byte, len(dump))
	copy(corrupt, dump)
	copy(corrupt[idx:], []byte(`"value":43`))
	if bytes.Equal(corrupt, dump) {
		t.Fatal("corruption was a no-op")
	}

	_, err := backup.Import(ctx, pool, bytes.NewReader(corrupt))
	if err == nil {
		t.Fatal("expected import to fail on corrupted dump")
	}
	if !strings.Contains(err.Error(), "checksum") && !strings.Contains(err.Error(), "malformed") {
		t.Fatalf("expected checksum error, got: %v", err)
	}
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
	        "text":     {"valueType": "string",  "config": {"unique": false, "indexed": true}},
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
	// attrs: text/priority/done + implicit id + rules row.
	if counts.Triples != 6 || counts.Attrs != 4 || counts.Rules != 1 {
		t.Fatalf("unexpected v1 import counts: %+v", counts)
	}

	st := storage.New(pool)
	got, err := st.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e1, e2}})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]bool{}
	for _, g := range got {
		switch v := g.Value.(type) {
		case string:
			values[v] = true
		case float64:
			values[fmt.Sprintf("num:%v", v)] = true
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

// TestExportResumeOffset verifies ?from-offset=N semantics: the first N
// logical records are skipped, the emitted dump still self-verifies on
// import, and counts reflect only what was emitted.
func TestExportResumeOffset(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	seedTodoApp(t, ctx, pool, appID, 2)
	full, fullCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	// Skip the first 2 logical records (both attrs).
	partial, partCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{FromOffset: 2})
	if partCounts.Attrs != fullCounts.Attrs-2 || partCounts.Triples != fullCounts.Triples {
		t.Fatalf("offset counts wrong: partial=%+v full=%+v", partCounts, fullCounts)
	}
	if !bytes.Contains(partial, []byte(`"kind":"header"`)) || !bytes.Contains(partial, []byte(`"kind":"checksum"`)) {
		t.Fatal("partial dump must keep header and checksum lines")
	}
	if bytes.Contains(partial, []byte(`"kind":"attr","id":"`)) {
		// First two attr lines must be gone (attrs are ordered by id).
		lines := strings.Split(string(partial), "\n")
		nAttrs := 0
		for _, l := range lines {
			if strings.HasPrefix(l, `{"kind":"attr"`) {
				nAttrs++
			}
		}
		if nAttrs != 2 {
			t.Fatalf("expected 2 remaining attr lines, got %d", nAttrs)
		}
	}

	remigrateSchema(t, ctx, pool)
	// Import of the partial dump fails cleanly (triples reference missing
	// attrs) but its checksum still validates ordering-wise; assert the
	// failure is about unknown attrs, not checksum.
	_, err := backup.Import(ctx, pool, bytes.NewReader(partial))
	if err == nil || !strings.Contains(err.Error(), "unknown attrs") {
		t.Fatalf("expected unknown-attr error for partial dump, got: %v", err)
	}

	// A partial dump whose skipped records are all transactions imports fine.
	fullLines := strings.SplitN(strings.TrimRight(string(full), "\n"), "\n", -1)
	_ = fullLines

	pool2, appID2, cleanup2 := env(t)
	defer cleanup2()
	seedTodoApp(t, ctx, pool2, appID2, 1)
	// Export skipping all triples+attrs+rule → only transactions remain.
	all, _ := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{})
	totalRecords := countKind(all, `"kind":"`)
	tail, tailCounts := exportApp(t, ctx, pool2, appID2, backup.ExportOptions{FromOffset: totalRecords - tailCount(pool2, appID2, t)})
	if tailCounts.Transactions <= 0 || tailCounts.Triples != 0 || tailCounts.Attrs != 0 {
		t.Fatalf("tail counts unexpected: %+v", tailCounts)
	}
	remigrateSchema(t, ctx, pool2)
	if _, err := backup.Import(ctx, pool2, bytes.NewReader(tail)); err != nil {
		t.Fatalf("tail-only import failed: %v", err)
	}
}

func countKind(dump []byte, needle string) int { return bytes.Count(dump, []byte(needle)) }

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
	cat, err := platform.LoadAttrCatalog(ctx, pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	st := storage.New(pool)
	chunk := make([]triple.Triple, 0, 10_000)
	for i := 0; i < n; i++ {
		chunk = chunk[:0]
		base := i
		for j := 0; j < 10_000 && base < n; j, base = j+1, base+1 {
			chunk = append(chunk, triple.Triple{E: newUUID(), A: a.ID, V: fmt.Sprintf("payload-%d", base)})
		}
		if _, err := st.InsertTriples(ctx, appID, cat, chunk, false); err != nil {
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

	// Corrupted restore body → 400 with message.
	bad := append([]byte{}, dump...)
	if i := bytes.Index(bad, []byte(`"value":42`)); i >= 0 {
		copy(bad[i:], []byte(`"value":43`))
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

var _ = time.Now // keep time import if unused after edits
