package storageapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlRejectsTrailingJSON(t *testing.T) {
	secret := []byte("storage-boundary-test")
	h := &Handler{
		Store: NewDiskBackend(t.TempDir(), secret), Secret: secret,
		AdminTokenCheck: func(context.Context, string, string) (bool, error) { return true, nil },
	}
	const appID = "00000000-0000-4000-8000-000000000001"
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/storage/signed-upload-url", `{"app-id":"` + appID + `","filename":"file.txt"} {}`},
		{http.MethodDelete, "/storage/files", `{"app-id":"` + appID + `","ids":["00000000-0000-4000-8000-000000000002"]} garbage`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("app-id", appID)
			req.Header.Set("X-admin-token", "test-token")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("trailing JSON status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
			if rec.Body.String() != "{\"message\":\"invalid JSON body\"}\n" {
				t.Fatalf("unexpected JSON error envelope: %s", rec.Body.String())
			}
		})
	}
}

func TestRequestProjectionContracts(t *testing.T) {
	const app = "00000000-0000-4000-8000-000000000001"
	const id = "00000000-0000-4000-8000-000000000002"
	upload, err := readUploadRequest(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"app_id":"`+app+`","path":"primary","filename":"fallback","extra":true}`)))
	if err != nil || upload.AppID[15] != 1 || upload.Filename != "primary" {
		t.Fatalf("upload request = %+v, %v", upload, err)
	}
	deleted, err := readDeleteRequest(httptest.NewRequest(http.MethodDelete, "/", strings.NewReader(`{"app_id":"`+app+`","ids":[42,null,"invalid","`+id+`"]}`)))
	if err != nil || len(deleted.IDs) != 1 || deleted.IDs[0] != id {
		t.Fatalf("delete request = %+v, %v", deleted, err)
	}
}

func TestResponseEscapingContract(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, http.StatusBadRequest, map[string]any{"message": "<&>"})
	if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != "{\"message\":\"\\u003c\\u0026\\u003e\"}\n" {
		t.Fatalf("storage JSON contract changed: %d %v %q", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestControlJSONSizeBoundary(t *testing.T) {
	const app = "00000000-0000-4000-8000-000000000001"
	base := `{"app-id":"` + app + `","filename":"file.txt"}`
	padded := base + strings.Repeat(" ", (1<<20)-len(base))
	secret := []byte("body-limit-test")
	h := &Handler{Store: NewDiskBackend(t.TempDir(), secret), Secret: secret,
		AdminTokenCheck: func(context.Context, string, string) (bool, error) { return true, nil }}
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"at-limit", padded, http.StatusOK},
		{"overflow-space", padded + " ", http.StatusBadRequest},
		{"second-object-beyond-limit", padded + "{}", http.StatusBadRequest},
		{"junk-beyond-limit", padded + "garbage", http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/storage/signed-upload-url", strings.NewReader(tc.body))
			req.Header.Set("app-id", app)
			req.Header.Set("X-admin-token", "token")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("body boundary status = %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}
