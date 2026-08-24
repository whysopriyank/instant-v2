package backup_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

// TestHandlerS3ObjectRoundTrip drives export→object-store→restore through
// the HTTP surface: PUT /backup/{app}/object?key=K streams an export into
// the store; POST /backup/{app}/restore-object?key=K imports it back.
func TestHandlerS3ObjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	f := newFakeS3(t, "bkt")
	store, err := backup.NewS3Store(ctx, backup.S3Config{
		Endpoint:  strings.TrimPrefix(f.srv.URL, "http://"),
		Bucket:    "bkt",
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("s3 store: %v", err)
	}
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)
	auth := map[string]string{"Authorization": "Bearer tok-123"}

	do := func(method, path string, hdr map[string]string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, nil)
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// S3 unwired → 503 on object routes (after auth passes).
	bare := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check}
	rec := httptest.NewRecorder()
	bareReq := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=x", nil)
	bareReq.Header.Set("Authorization", "Bearer tok-123")
	bare.ServeHTTP(rec, bareReq)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 without S3 wired, got %d body=%s", rec.Code, rec.Body.String())
	}

	seedTodoApp(t, ctx, pool, appID, 3)

	// PUT export into the store.
	rec = do(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", auth)
	if rec.Code != http.StatusOK {
		t.Fatalf("put object: got %d body=%s", rec.Code, rec.Body.String())
	}
	f.mu.Lock()
	dump, ok := f.objects[appStr+"/dumps/app.ndjson"] // keys are app-scoped (security fix)
	f.mu.Unlock()
	if !ok || len(dump) == 0 {
		t.Fatal("object missing after put")
	}

	// POST restore-object re-imports from the store (idempotent).
	rec = do(http.MethodPost, "/backup/"+appStr+"/restore-object?key=dumps/app.ndjson", auth)
	if rec.Code != http.StatusOK {
		t.Fatalf("restore-object: got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"counts"`) {
		t.Fatalf("unexpected restore body: %s", rec.Body.String())
	}

	// GET /object downloads the raw dump.
	rec = do(http.MethodGet, "/backup/"+appStr+"/object?key=dumps/app.ndjson", auth)
	if rec.Code != http.StatusOK || len(rec.Body.Bytes()) != len(dump) {
		t.Fatalf("get object: got %d len=%d want %d", rec.Code, rec.Body.Len(), len(dump))
	}

	// Missing key → 404.
	rec = do(http.MethodPost, "/backup/"+appStr+"/restore-object?key=nope", auth)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing key: got %d", rec.Code)
	}
}
