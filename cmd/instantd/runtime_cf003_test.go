package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

const (
	cf003AppID       = "00000000-0000-4000-8000-000000000003"
	cf003CreatorID   = "00000000-0000-4000-8000-000000000001"
	cf003AdminToken  = "00000000-0000-4000-8000-000000000002"
	cf003EntityID    = "00000000-0000-4000-8000-000000000004"
	cf003FileID      = "00000000-0000-4000-8000-000000000005"
	cf003HermeticDSN = "postgres://127.0.0.1:1/instant?pool_min_conns=0&connect_timeout=1"
)

func TestCF003HTTPMatrixHermetic(t *testing.T) {
	mux, root := cf003HermeticMux(t)
	appID := cf003AppID

	cases := []struct {
		name, method, path, body, wantBody string
		wantStatus                         int
		headers                            map[string]string
	}{
		{
			name:       "auth malformed body",
			method:     http.MethodPost,
			path:       "/runtime/auth/send_magic_code",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantBody:   "{\"message\":\"email and app-id are required\"}\n",
		},
		{
			name:       "auth delivery unavailable",
			method:     http.MethodPost,
			path:       "/runtime/auth/send_magic_code",
			body:       `{"email":"cf003@example.test","app-id":"` + appID + `"}`,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "{\"message\":\"magic code delivery unavailable\"}\n",
		},
		{
			name:       "runtime refresh malformed body",
			method:     http.MethodPost,
			path:       "/runtime/auth/refresh_tokens",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
			wantBody:   "{\"message\":\"refresh_tokens and app-id are required\"}\n",
		},
		{
			name:       "runtime query missing app",
			method:     http.MethodPost,
			path:       "/runtime/framework/query",
			body:       `{"query":{"todos":{}}}`,
			wantStatus: http.StatusBadRequest,
			wantBody:   "{\"message\":\"missing or invalid app-id\"}\n",
		},
		{
			name:       "runtime openid positive",
			method:     http.MethodGet,
			path:       "/runtime/openid-configuration?app_id=" + url.QueryEscape(appID),
			wantStatus: http.StatusOK,
			wantBody:   `{"authorization_endpoint":"http://cf003.test/runtime/` + appID + `/oauth/start","token_endpoint":"http://cf003.test/runtime/` + appID + `/oauth/token"}` + "\n",
		},
		{
			name:       "admin missing token",
			method:     http.MethodGet,
			path:       "/admin/schema?app-id=" + url.QueryEscape(appID),
			wantStatus: http.StatusUnauthorized,
			wantBody:   "{\"message\":\"missing admin token\"}\n",
		},
		{
			name:       "storage missing admin",
			method:     http.MethodPost,
			path:       "/storage/signed-upload-url",
			body:       `{"app-id":"` + appID + `","path":"cf003.txt"}`,
			wantStatus: http.StatusUnauthorized,
			wantBody:   "{\"message\":\"invalid admin credentials\"}\n",
		},
		{
			name:       "storage unsigned upload",
			method:     http.MethodPut,
			path:       "/storage/upload/" + cf003FileID + "?app-id=" + url.QueryEscape(appID),
			body:       "must not land",
			wantStatus: http.StatusForbidden,
			wantBody:   "{\"message\":\"storageapi: missing or malformed expiry\"}\n",
		},
		{
			name:       "backup missing admin",
			method:     http.MethodGet,
			path:       "/backup/" + appID,
			wantStatus: http.StatusUnauthorized,
			wantBody:   `{"message":"Invalid admin token"}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			status, body, _ := cf003Serve(mux, tc.method, tc.path, tc.body, tc.headers)
			if status != tc.wantStatus || body != tc.wantBody {
				t.Fatalf("status/body = %d %q; want %d %q", status, body, tc.wantStatus, tc.wantBody)
			}
		})
	}

	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("unsigned storage request created files: %v", entries)
	}
}

func TestCF003HTTPMatrixOwnedPostgres(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	appStr := platform.UUIDToStr(appID)

	status, body, _ := cf003Serve(mux, http.MethodGet,
		"/admin/schema?app-id="+url.QueryEscape(appStr), "", map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK || body != `{"schema":{"attrs":[]}}`+"\n" {
		t.Fatalf("admin schema status/body = %d %q", status, body)
	}

	status, body, _ = cf003Serve(mux, http.MethodGet,
		"/admin/schema?app-id="+url.QueryEscape(appStr), "", map[string]string{"X-admin-token": "00000000-0000-4000-8000-000000000099"})
	if status != http.StatusUnauthorized || body != "{\"message\":\"Invalid admin token\"}\n" {
		t.Fatalf("admin denial status/body = %d %q", status, body)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "cf003"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("admin transact status/body = %d %q", status, body)
	}
	var txResponse struct {
		TxID int64 `json:"tx-id"`
	}
	if err := json.Unmarshal([]byte(body), &txResponse); err != nil || txResponse.TxID <= 0 {
		t.Fatalf("admin transact body = %q; want positive tx-id: %v", body, err)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
		`{"query":{"todos":{}}}`, map[string]string{"app-id": appStr})
	wantQuery := `{"data":{"todos":[{"id":"` + cf003EntityID + `","title":"cf003"}]}}` + "\n"
	if status != http.StatusOK || body != wantQuery {
		t.Fatalf("runtime query status/body = %d %q; want %d %q", status, body, http.StatusOK, wantQuery)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_in_guest",
		`{"app-id":"`+appStr+`"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("guest sign-in status/body = %d %q", status, body)
	}
	var guestResponse struct {
		User map[string]any `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &guestResponse); err != nil {
		t.Fatalf("guest sign-in body = %q: %v", body, err)
	}
	if len(guestResponse.User) != 3 || guestResponse.User["type"] != "guest" {
		t.Fatalf("guest user shape = %#v; want exactly id/type/refresh_token", guestResponse.User)
	}
	guestID, _ := guestResponse.User["id"].(string)
	refreshToken, _ := guestResponse.User["refresh_token"].(string)
	if _, err := platform.ScanUUIDErr(guestID); err != nil || len(refreshToken) != 36 {
		t.Fatalf("guest identity/token = %q/%q", guestID, refreshToken)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": refreshToken, "app-id": appStr}), nil)
	wantVerify := `{"user":{"id":"` + guestID + `","refresh_token":"` + refreshToken + `","type":"guest"}}` + "\n"
	if status != http.StatusOK || body != wantVerify {
		t.Fatalf("refresh verification status/body = %d %q; want %d %q", status, body, http.StatusOK, wantVerify)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_out",
		cf003JSON(t, map[string]any{"refresh_token": refreshToken, "app_id": appStr}), nil)
	if status != http.StatusOK || body != "{}\n" {
		t.Fatalf("sign-out status/body = %d %q", status, body)
	}
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": refreshToken, "app-id": appStr}), nil)
	if status != http.StatusUnauthorized || body != "{\"message\":\"authn: unknown refresh token\"}\n" {
		t.Fatalf("refresh replay status/body = %d %q", status, body)
	}

	status, dump, headers := cf003Serve(mux, http.MethodGet, "/backup/"+appStr, "", map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK || headers.Get("Content-Type") != "application/x-ndjson" || headers.Get("Accept-Ranges") != "records" {
		t.Fatalf("backup export = %d headers=%v body=%q", status, headers, dump)
	}
	counts := cf003CheckDump(t, dump, appStr)

	status, body, _ = cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": appStr,
		"steps":  []any{[]any{"delete", "todos", cf003EntityID}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("destructive backup fixture delete status/body = %d %q", status, body)
	}
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
		`{"query":{"todos":{}}}`, map[string]string{"app-id": appStr})
	if status != http.StatusOK || body != `{"data":{"todos":[]}}`+"\n" {
		t.Fatalf("query after destructive backup fixture delete = %d %q; want absent entity", status, body)
	}

	status, restoreBody, _ := cf003Serve(mux, http.MethodPost, "/backup/"+appStr+"/restore", dump, map[string]string{"Authorization": "Bearer " + adminToken})
	wantRestore := fmt.Sprintf(`{"counts":{"attrs":%d,"triples":%d,"rules":%d,"transactions":%d}}`, counts.attrs, counts.triples, counts.rules, counts.transactions)
	if status != http.StatusOK || restoreBody != wantRestore {
		t.Fatalf("backup restore status/body = %d %q; want %d %q", status, restoreBody, http.StatusOK, wantRestore)
	}
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
		`{"query":{"todos":{}}}`, map[string]string{"app-id": appStr})
	if status != http.StatusOK || body != wantQuery {
		t.Fatalf("query after backup restore = %d %q; want %d %q", status, body, http.StatusOK, wantQuery)
	}

	status, body, _ = cf003Serve(mux, http.MethodPut,
		"/backup/"+appStr+"/object?key=dumps/cf003.ndjson", "", map[string]string{"X-admin-token": adminToken})
	if status != http.StatusServiceUnavailable || body != `{"message":"no object store wired"}` {
		t.Fatalf("excluded object backup status/body = %d %q", status, body)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/storage/signed-upload-url",
		cf003JSON(t, map[string]any{"app-id": appStr, "path": "docs/cf003.txt"}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("storage presign status/body = %d %q", status, body)
	}
	var uploadResponse struct {
		Data struct {
			URL       string `json:"url"`
			ID        string `json:"id"`
			ExpiresAt string `json:"expires-at"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &uploadResponse); err != nil {
		t.Fatalf("storage presign body = %q: %v", body, err)
	}
	if _, err := platform.ScanUUIDErr(uploadResponse.Data.ID); err != nil || uploadResponse.Data.URL == "" || uploadResponse.Data.ExpiresAt == "" {
		t.Fatalf("storage presign data = %#v", uploadResponse.Data)
	}
	if _, err := time.Parse(time.RFC3339, uploadResponse.Data.ExpiresAt); err != nil {
		t.Fatalf("storage expires-at = %q: %v", uploadResponse.Data.ExpiresAt, err)
	}
	uploadURL, err := url.Parse(uploadResponse.Data.URL)
	if err != nil || uploadURL.Path != "/storage/upload/"+uploadResponse.Data.ID {
		t.Fatalf("storage upload URL = %q: %v", uploadResponse.Data.URL, err)
	}
	for _, key := range []string{"app-id", "filename", "expires", "signature"} {
		if uploadURL.Query().Get(key) == "" {
			t.Fatalf("storage upload URL missing %s: %q", key, uploadURL.RawQuery)
		}
	}

	const payload = "cf003 assembled storage payload"
	status, body, _ = cf003Serve(mux, http.MethodPut, uploadURL.RequestURI(), payload, map[string]string{"Content-Type": "text/plain"})
	wantUpload := `{"data":{"id":"` + uploadResponse.Data.ID + `"}}` + "\n"
	if status != http.StatusOK || body != wantUpload {
		t.Fatalf("storage upload status/body = %d %q; want %d %q", status, body, http.StatusOK, wantUpload)
	}

	downloadPath := "/storage/signed-download-url?" + url.Values{
		"app-id": {appStr},
		"id":     {uploadResponse.Data.ID},
	}.Encode()
	status, body, _ = cf003Serve(mux, http.MethodGet, downloadPath, "", map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("storage download presign status/body = %d %q", status, body)
	}
	var downloadResponse struct {
		Data struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &downloadResponse); err != nil {
		t.Fatalf("storage download presign body = %q: %v", body, err)
	}
	downloadURL, err := url.Parse(downloadResponse.Data.URL)
	if err != nil || downloadURL.Path != "/storage/files/"+uploadResponse.Data.ID {
		t.Fatalf("storage download URL = %q: %v", downloadResponse.Data.URL, err)
	}
	status, body, _ = cf003Serve(mux, http.MethodGet, downloadURL.RequestURI(), "", nil)
	if status != http.StatusOK || body != payload {
		t.Fatalf("storage download status/body = %d %q", status, body)
	}

	deletePath := "/storage/files?" + url.Values{
		"app-id": {appStr},
		"id":     {uploadResponse.Data.ID},
	}.Encode()
	status, body, _ = cf003Serve(mux, http.MethodDelete, deletePath, "", map[string]string{"X-admin-token": "00000000-0000-4000-8000-000000000099"})
	if status != http.StatusUnauthorized || body != "{\"message\":\"invalid admin credentials\"}\n" {
		t.Fatalf("storage denied delete status/body = %d %q", status, body)
	}
	status, body, _ = cf003Serve(mux, http.MethodGet, downloadURL.RequestURI(), "", nil)
	if status != http.StatusOK || body != payload {
		t.Fatalf("denied storage delete mutated object: %d %q", status, body)
	}
}

type cf003DumpCounts struct {
	attrs, triples, rules, transactions int64
}

func cf003CheckDump(t *testing.T, dump, appID string) cf003DumpCounts {
	t.Helper()
	lines := strings.Split(strings.TrimSuffix(dump, "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("backup dump has too few lines: %q", dump)
	}
	var header struct {
		Kind      string `json:"kind"`
		Format    string `json:"format"`
		Version   int    `json:"version"`
		AppID     string `json:"app_id"`
		CreatorID string `json:"creator_id"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &header); err != nil {
		t.Fatalf("backup header: %v", err)
	}
	if header.Kind != "header" || header.Format != "instant-v2-backup" || header.Version != 2 || header.AppID != appID || header.CreatorID == "" {
		t.Fatalf("backup header = %#v", header)
	}

	var counts cf003DumpCounts
	for _, line := range lines[1 : len(lines)-1] {
		var record struct {
			Kind string `json:"kind"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("backup record %q: %v", line, err)
		}
		switch record.Kind {
		case "attr":
			counts.attrs++
		case "triple":
			counts.triples++
		case "rule":
			counts.rules++
		case "transaction":
			counts.transactions++
		default:
			t.Fatalf("unexpected backup record kind %q", record.Kind)
		}
	}
	var trailer struct {
		Kind    string `json:"kind"`
		SHA256  string `json:"sha256"`
		Records int64  `json:"records"`
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &trailer); err != nil {
		t.Fatalf("backup trailer: %v", err)
	}
	wantRecords := counts.attrs + counts.triples + counts.rules + counts.transactions
	if trailer.Kind != "checksum" || len(trailer.SHA256) != 64 || trailer.Records != wantRecords {
		t.Fatalf("backup trailer = %#v; want records=%d", trailer, wantRecords)
	}
	return counts
}

func cf003HermeticMux(t *testing.T) (*http.ServeMux, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	pool, err := pgxpool.New(ctx, cf003HermeticDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	db, err := sql.Open("pgx", cf003HermeticDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	root := t.TempDir()
	cfg := cf003Config(root)
	a := newAppRuntime(pool, pool, cfg, slog.Default())
	mux := http.NewServeMux()
	if _, _, err := a.mountRoutes(ctx, db, mux, cfg, ratelimit.New(ratelimit.Config{})); err != nil {
		t.Fatal(err)
	}
	return mux, root
}

func cf003PostgresMux(t *testing.T) (*http.ServeMux, [16]byte, string) {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	db, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	setupCtx, setupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer setupCancel()
	if err := platform.Migrate(setupCtx, db); err != nil {
		t.Fatal(err)
	}
	appID, err := platform.ScanUUIDErr(cf003AppID)
	if err != nil {
		t.Fatal(err)
	}
	creatorID, err := platform.ScanUUIDErr(cf003CreatorID)
	if err != nil {
		t.Fatal(err)
	}
	adminID, err := platform.ScanUUIDErr(cf003AdminToken)
	if err != nil {
		t.Fatal(err)
	}
	st := storage.New(fixture.Pool)
	if err := st.WithTx(setupCtx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(setupCtx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "cf003@example.test"); err != nil {
			return err
		}
		if err := platform.CreateApp(setupCtx, tx, creatorID, appID, "cf003-http-matrix"); err != nil {
			return err
		}
		return platform.SetAdminToken(setupCtx, tx, appID, adminID)
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	root := t.TempDir()
	cfg := cf003Config(root)
	a := newAppRuntime(fixture.Pool, fixture.Pool, cfg, slog.Default())
	notifierDone := make(chan struct{})
	go func() {
		defer close(notifierDone)
		a.notifier.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-notifierDone:
		case <-time.After(5 * time.Second):
			t.Error("CF-003 notifier did not stop")
		}
	})
	mux := http.NewServeMux()
	if _, _, err := a.mountRoutes(ctx, db, mux, cfg, ratelimit.New(ratelimit.Config{})); err != nil {
		t.Fatal(err)
	}
	return mux, appID, cf003AdminToken
}

func cf003Config(root string) config.Config {
	return config.Config{
		StorageSecret:    "cf003-test-storage-secret",
		StorageRoot:      root,
		HTTPAddr:         "127.0.0.1:0",
		MaxUploadBytes:   1 << 20,
		WSAllowedOrigins: "cf003.test",
	}
}

func cf003Serve(mux *http.ServeMux, method, path, body string, headers map[string]string) (int, string, http.Header) {
	var reader io.Reader = strings.NewReader(body)
	req := httptest.NewRequest(method, path, reader)
	req.Host = "cf003.test"
	for key, value := range headers {
		req.Header.Set(key, value)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String(), rec.Header()
}

func cf003JSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
