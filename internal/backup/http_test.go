package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

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

// TestHandlerRejectsMissingAuthConfig pins DA-003 row 1: a handler
// without an authorization callback refuses the request instead of
// panicking on the nil func value.
func TestHandlerRejectsMissingAuthConfig(t *testing.T) {
	h := &backup.Handler{}
	req := httptest.NewRequest(http.MethodGet, "/backup/00000000-0000-4000-8000-000000000001", nil)
	rec := httptest.NewRecorder()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("nil AdminTokenCheck panicked: %v", recovered)
		}
	}()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}

// TestUnauthorizedRestoreWritesNothing pins DA-003 row 2 on the write
// path: a restore with a bad admin token is rejected before Import runs,
// leaving existing rows untouched.
func TestUnauthorizedRestoreWritesNothing(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 2)
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})

	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check}
	req := httptest.NewRequest(http.MethodPost, "/backup/"+uuidStr(appID)+"/restore", bytes.NewReader(dump))
	req.Header.Set("Authorization", "Bearer wrong")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}

	var triples, attrs int
	if err := pool.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM triples WHERE app_id=$1),
		        (SELECT count(*) FROM attrs   WHERE app_id=$1)`, appID).Scan(&triples, &attrs); err != nil {
		t.Fatal(err)
	}
	if triples != 10 || attrs != 4 {
		t.Fatalf("unauthorized restore mutated rows: triples=%d attrs=%d", triples, attrs)
	}
}

func firstLine(b []byte) string {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}
