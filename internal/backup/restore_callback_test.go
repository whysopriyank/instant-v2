package backup_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storageapi"
)

func TestHandlerRestoreRefreshesCachedCatalogAfterCommit(t *testing.T) {
	for _, format := range []string{"restore", "restore-v1zip"} {
		t.Run(format, func(t *testing.T) {
			ctx := context.Background()
			source, appID, cleanup := env(t)
			defer cleanup()
			seedTodoApp(t, ctx, source, appID, 1)
			body, _ := exportApp(t, ctx, source, appID, backup.ExportOptions{})
			if format == "restore-v1zip" {
				body = restoreZip(t, "config.json", `{"schema":{"entities":{"todo":{"attrs":{"text":{"valueType":"string"}}}}}}`,
					"entities/todo.jsonl", `{"entity":{"id":"00000000-0000-4000-8000-000000000004","text":"restored"},"createdAt":1700000000000}`+"\n")
			}
			target := newDatabase(t)
			cache := platform.NewCatalogCache(target, target)
			app := uuidStr(appID)
			before, err := cache.For(ctx, app)
			if err != nil || len(before.WireAttrs()) != 0 {
				t.Fatalf("warm empty catalog = %v, %v", before, err)
			}
			calls, committedAttrs := 0, 0
			h := &backup.Handler{Pool: target, AdminTokenCheck: fakeAuth{valid: "restore-token"}.check,
				OnRestore: func(restored string) {
					calls++
					if restored != app {
						t.Errorf("restored app = %q, want %q", restored, app)
					}
					cache.Invalidate(restored)
					catalog, loadErr := cache.For(ctx, restored)
					if loadErr != nil {
						t.Error(loadErr)
						return
					}
					committedAttrs = len(catalog.WireAttrs())
				}}
			post := func() *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodPost, "/backup/"+app+"/"+format, bytes.NewReader(body))
				req.Header.Set("X-admin-token", "restore-token")
				response := httptest.NewRecorder()
				h.ServeHTTP(response, req)
				return response
			}
			response := post()
			if response.Code != http.StatusOK || calls != 1 || committedAttrs == 0 {
				t.Fatalf("committed restore response=%d %s, callbacks=%d visible attrs=%d", response.Code, response.Body.String(), calls, committedAttrs)
			}
			after, err := cache.For(ctx, app)
			if err != nil || len(after.WireAttrs()) != committedAttrs {
				t.Fatalf("restored catalog = %v, %v", after, err)
			}
			response = post()
			if response.Code != http.StatusConflict || calls != 1 {
				t.Fatalf("rejected restore response=%d, callbacks=%d", response.Code, calls)
			}
		})
	}
}

func TestHandlerRestoreUnknownCommitDoesNotNotify(t *testing.T) {
	pool, appID, cleanup := env(t)
	defer cleanup()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store, err := storageapi.NewDiskBackend(t.TempDir(), []byte("restore-test"))
	if err != nil {
		t.Fatal(err)
	}
	files := &restoreFaultStore{ObjectStore: store, cancel: cancel}
	body := fileArchive(t, []string{uuidStr(newUUID())}, []string{uuidStr(newUUID())}, []string{"retained uncertain object"})
	calls := 0
	h := &backup.Handler{Pool: pool, Files: files, AdminTokenCheck: fakeAuth{valid: "restore-token"}.check,
		OnRestore: func(string) { calls++ }}
	req := httptest.NewRequest(http.MethodPost, "/backup/"+uuidStr(appID)+"/restore-v1zip", bytes.NewReader(body)).WithContext(ctx)
	req.Header.Set("X-admin-token", "restore-token")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), backup.ErrCommitOutcomeUnknown.Error()) || calls != 0 {
		t.Fatalf("unknown commit response=%d %s, callbacks=%d", response.Code, response.Body.String(), calls)
	}
}
