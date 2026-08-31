package storageapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestControlAppBinding(t *testing.T) {
	const authorized = "00000000-0000-4000-8000-000000000001"
	const foreign = "00000000-0000-4000-8000-000000000002"
	const file = "00000000-0000-4000-8000-000000000003"
	for _, target := range []string{authorized, foreign} {
		for _, tc := range []struct{ method, path, body, headerApp string }{
			{http.MethodPost, "/storage/signed-upload-url", `{"app-id":"` + target + `","path":"file"}`, authorized},
			{http.MethodGet, "/storage/signed-download-url?app-id=" + target + "&id=" + file, "", authorized},
			{http.MethodDelete, "/storage/files?app-id=" + target + "&id=" + file, "", authorized},
			{http.MethodDelete, "/storage/files", `{"app_id":"` + target + `","ids":["` + file + `"]}`, authorized},
			{http.MethodPost, "/storage/signed-upload-url?app_id=" + authorized, `{"app_id":"` + target + `","filename":"file"}`, ""},
			{http.MethodDelete, "/storage/files?app-id=" + authorized, `{"app-id":"` + target + `","ids":["` + file + `"]}`, ""},
		} {
			t.Run(target+tc.method+tc.path, func(t *testing.T) {
				secret := []byte("app-binding-test")
				store := NewDiskBackend(t.TempDir(), secret)
				for _, app := range []string{authorized, foreign} {
					if err := store.Put(app+"/"+file, strings.NewReader("original "+app)); err != nil {
						t.Fatal(err)
					}
				}
				h := &Handler{Store: store, Secret: secret, AdminTokenCheck: func(_ context.Context, appID, token string) (bool, error) {
					return appID == authorized && token == "admin-A", nil
				}}
				req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
				if tc.headerApp != "" {
					req.Header.Set("app-id", tc.headerApp)
					req.Header.Set("X-admin-token", "admin-A")
				} else {
					req.Header.Set("Authorization", "Bearer admin-A")
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				want := http.StatusOK
				if target == foreign {
					want = http.StatusUnauthorized
					if rec.Body.String() != "{\"message\":\"invalid admin credentials\"}\n" {
						t.Errorf("cross-app operation exposed response: %s", rec.Body.String())
					}
				}
				if rec.Code != want {
					t.Errorf("status = %d, want %d", rec.Code, want)
				}
				// Regardless of the operation or response, app B's bytes must survive
				// a request authenticated only for app A.
				obj, err := store.Open(foreign + "/" + file)
				if err != nil {
					t.Fatalf("foreign object was deleted: %v", err)
				}
				defer func() { _ = obj.Body.Close() }()
				data, err := io.ReadAll(obj.Body)
				if err != nil || string(data) != "original "+foreign {
					t.Fatalf("foreign bytes changed: %q, %v", data, err)
				}
			})
		}
	}
}
