package backup_test

// DA-003 contract gaps: extra-after-checksum, record-count mismatch,
// key traversal/namespace escape, and exact-state preservation on failed
// restore. The production code already rejects these; these tests pin the
// behavior so the rows are proven, not assumed.

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

// TestImportRejectsExtraAfterChecksum pins DA-003c: a dump with a valid
// record after the terminal checksum is refused and commits nothing.
func TestImportRejectsExtraAfterChecksum(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	extra := []byte(`{"kind":"transaction","id":999999,"created_at":"2026-01-01T00:00:00Z"}` + "\n")
	tainted := append(append([]byte{}, dump...), extra...)

	target := newDatabase(t)
	if _, err := backup.Import(ctx, target, bytes.NewReader(tainted), appID); err == nil {
		t.Fatal("expected extra-after-checksum dump to fail import")
	} else if !strings.Contains(err.Error(), "unexpected content after checksum") {
		t.Fatalf("expected unexpected-content error, got: %v", err)
	}

	var apps, triples int
	if err := target.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM apps), (SELECT count(*) FROM triples)`).Scan(&apps, &triples); err != nil {
		t.Fatal(err)
	}
	if apps != 0 || triples != 0 {
		t.Fatalf("extra-after-checksum import committed rows: apps=%d triples=%d", apps, triples)
	}
}

// TestImportRejectsRecordsMismatch pins DA-003c: a dump whose sha256 is
// correct but whose records count lies is refused and commits nothing.
func TestImportRejectsRecordsMismatch(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)
	dump, expCounts := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	lines := bytes.Split(bytes.TrimSuffix(dump, []byte("\n")), []byte("\n"))
	var trailer struct {
		Kind    string `json:"kind"`
		SHA256  string `json:"sha256"`
		Records int64  `json:"records"`
	}
	if err := json.Unmarshal(lines[len(lines)-1], &trailer); err != nil {
		t.Fatalf("decode trailer: %v", err)
	}
	if trailer.Kind != "checksum" || trailer.SHA256 == "" {
		t.Fatalf("bad trailer: %q", lines[len(lines)-1])
	}
	wantSum := expCounts.Attrs + expCounts.Triples + expCounts.Rules + expCounts.Transactions
	if trailer.Records != wantSum {
		t.Fatalf("fixture trailer records %d != counts sum %d", trailer.Records, wantSum)
	}
	// Lie about the count while keeping the hash valid for the prefix.
	trailer.Records++
	lied, err := json.Marshal(map[string]any{"kind": "checksum", "sha256": trailer.SHA256, "records": trailer.Records})
	if err != nil {
		t.Fatal(err)
	}
	tainted := append(bytes.Join(lines[:len(lines)-1], []byte("\n")), '\n')
	tainted = append(tainted, append(lied, '\n')...)

	target := newDatabase(t)
	if _, err := backup.Import(ctx, target, bytes.NewReader(tainted), appID); err == nil {
		t.Fatal("expected records-mismatch dump to fail import")
	} else if !strings.Contains(err.Error(), "records") {
		t.Fatalf("expected records-count error, got: %v", err)
	}

	var apps, triples int
	if err := target.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM apps), (SELECT count(*) FROM triples)`).Scan(&apps, &triples); err != nil {
		t.Fatal(err)
	}
	if apps != 0 || triples != 0 {
		t.Fatalf("records-mismatch import committed rows: apps=%d triples=%d", apps, triples)
	}
}

// TestObjectKeysRejectTraversalAndEscape pins the security namespace: keys
// containing ".." or leading "/" are rejected before any store call, and a
// key containing a foreign app id never escapes the caller's prefix.
func TestObjectKeysRejectTraversalAndEscape(t *testing.T) {
	store := newDA003MemStore()
	h := &backup.Handler{Pool: nil, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := "00000000-0000-4000-8000-000000000001"

	malicious := []string{
		"..",
		"../evil",
		"a/../b",
		"dumps/../../escape",
		"/etc/passwd",
		"/absolute",
	}
	for _, key := range malicious {
		enc := url.QueryEscape(key)
		for _, rt := range []struct{ method, path string }{
			{http.MethodPut, "/backup/" + appStr + "/object?key=" + enc},
			{http.MethodGet, "/backup/" + appStr + "/object?key=" + enc},
			{http.MethodPost, "/backup/" + appStr + "/restore-object?key=" + enc},
		} {
			req := httptest.NewRequest(rt.method, rt.path, nil)
			req.Header.Set("Authorization", "Bearer tok-123")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("traversal key %q %s %s = %d, want 400", key, rt.method, rt.path, rec.Code)
			}
			if strings.Contains(rec.Body.String(), `"kind"`) {
				t.Fatalf("traversal key %q emitted dump bytes: %s", key, rec.Body.String())
			}
		}
	}
	store.mu.Lock()
	puts, gets := store.puts, store.gets
	promotions := len(store.promotions)
	deletes := len(store.deletes)
	store.mu.Unlock()
	if puts != 0 || gets != 0 || promotions != 0 || deletes != 0 {
		t.Fatalf("traversal keys reached store: puts=%d gets=%d promotions=%d deletes=%d", puts, gets, promotions, deletes)
	}

	// Namespace isolation: a key naming a foreign app stays under the caller.
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)
	store2 := newDA003MemStore()
	h2 := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store2}
	caller := uuidStr(appID)
	foreign := "00000000-0000-4000-8000-00000000ffff/dump.ndjson"
	req := httptest.NewRequest(http.MethodPut, "/backup/"+caller+"/object?key="+url.QueryEscape(foreign), nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h2.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("namespaced put: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode put response: %v", err)
	}
	if resp.Key != caller+"/"+foreign {
		t.Fatalf("key escaped namespace: got %q want %q", resp.Key, caller+"/"+foreign)
	}
	if _, ok := store2.snapshot("00000000-0000-4000-8000-00000000ffff/dump.ndjson"); ok {
		t.Fatal("object written outside caller prefix")
	}
}

// TestFailedImportPreservesExactDump pins DA-003e at byte equality: a
// checksum-corrupt re-import into the same database leaves the subsequent
// export byte-identical (single-transaction rollback, not just row counts).
func TestFailedImportPreservesExactDump(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 2)
	before, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	corrupt := append([]byte{}, before...)
	marker := []byte(`"value":4`)
	idx := bytes.Index(corrupt, marker)
	if idx < 0 {
		t.Fatal("corruption target not found in dump")
	}
	copy(corrupt[idx:], []byte(`"value":9`))

	if _, err := backup.Import(ctx, pool, bytes.NewReader(corrupt), appID); err == nil {
		t.Fatal("expected corrupt re-import to fail")
	} else if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("expected checksum error, got: %v", err)
	}

	after, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	if !bytes.Equal(before, after) {
		t.Fatalf("failed import mutated target: before %d bytes, after %d bytes", len(before), len(after))
	}
}
