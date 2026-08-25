package storageapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func newUUIDStr() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

// newServer wires a Handler over a DiskBackend in a temp dir and serves it
// over a real httptest server so presigned absolute URLs round-trip.
func newServer(t *testing.T) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(newHandler(t))
	t.Cleanup(srv.Close)
	return srv, platform.UUIDToStr(newUUIDStr())
}

// testAdminToken is accepted by the stub AdminTokenCheck wired into every
// test handler.
const testAdminToken = "0f0e0d0c-0b0a-4938-8271-665544332211"

func newHandler(t *testing.T) *Handler {
	t.Helper()
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	return &Handler{
		Store:           NewDiskBackend(t.TempDir(), secret),
		Secret:          secret,
		AdminTokenCheck: func(_ context.Context, _, tok string) (bool, error) { return tok == testAdminToken, nil },
	}
}

func postJSON(t *testing.T, srv *httptest.Server, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	// Control routes (presigning included) require admin credentials since
	// TestStorageAdminGate pinned the gate; tests exercise round-trip
	// mechanics with the valid test token.
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-admin-token", testAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &out); err != nil && len(raw) > 0 {
		t.Fatalf("non-JSON response %d: %s", resp.StatusCode, raw)
	}
	return resp.StatusCode, out
}

// presignUpload calls the management endpoint and returns the presigned url,
// the file id, and the full data payload.
func presignUpload(t *testing.T, srv *httptest.Server, appID, filename string) (string, string, map[string]any) {
	t.Helper()
	status, resp := postJSON(t, srv, "/storage/signed-upload-url", map[string]any{"app-id": appID, "filename": filename})
	if status != 200 {
		t.Fatalf("signed-upload-url: %d %v", status, resp)
	}
	data, ok := resp["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data envelope: %v", resp)
	}
	u, _ := data["url"].(string)
	id, _ := data["id"].(string)
	if u == "" || id == "" {
		t.Fatalf("bad presign payload: %v", data)
	}
	if _, ok := data["expires-at"]; !ok {
		t.Fatalf("missing expires-at: %v", data)
	}
	return u, id, data
}

func downloadURLFor(t *testing.T, srv *httptest.Server, appID, id string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		srv.URL+"/storage/signed-download-url?app-id="+url.QueryEscape(appID)+"&id="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-admin-token", testAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("non-JSON response %d: %s", resp.StatusCode, raw)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("signed-download-url: %d %v", resp.StatusCode, out)
	}
	data, ok := out["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data envelope: %v", out)
	}
	return data["url"].(string)
}
func doReq(t *testing.T, method, urlStr string, body []byte) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, urlStr, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func readAll(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// reSign rewrites a presigned URL's expiry/signature for op/app/id while
// keeping the route path intact.
func reSign(h *Handler, rawURL, op, appID, id string, exp int64) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	q := u.Query()
	q.Set("app-id", appID)
	q.Set("expires", strconv.FormatInt(exp, 10))
	q.Set("signature", signPayload(h.Secret, op, appID, id, exp))
	u.RawQuery = q.Encode()
	return u.String()
}

func mustStatus(t *testing.T, resp *http.Response, want int) {
	t.Helper()
	if resp.StatusCode != want {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("got %d want %d: %s", resp.StatusCode, want, body)
	}
	_ = resp.Body.Close()
}

// TestPresignRoundTrip proves the acceptance flow: presign upload → PUT
// bytes → presign download → GET returns identical bytes; expired and
// tampered signatures are both 403.
func TestPresignRoundTrip(t *testing.T) {
	h, srv, appID := newHandlerSrvApp(t)

	uploadURL, id, _ := presignUpload(t, srv, appID, "hello.txt")
	want := []byte("hello instant storage")

	// Tampered signature must not upload.
	tampered := reSign(h, uploadURL, "upload", appID, id, time.Now().Add(time.Minute).Unix())
	u, _ := url.Parse(tampered)
	q := u.Query()
	sig := q.Get("signature")
	q.Set("signature", sig[:len(sig)-2]+"ff") // flip tail hex chars
	u.RawQuery = q.Encode()
	mustStatus(t, doReq(t, http.MethodPut, u.String(), want), http.StatusForbidden)

	// Expired signature must not upload either.
	expiredUp := reSign(h, uploadURL, "upload", appID, id, time.Now().Add(-time.Minute).Unix())
	mustStatus(t, doReq(t, http.MethodPut, expiredUp, want), http.StatusForbidden)

	// Valid upload succeeds and returns {"data":{"id":...}}.
	status, up := putBody(t, uploadURL, want)
	if status != 200 || up["data"].(map[string]any)["id"] != id {
		t.Fatalf("upload: %d %v", status, up)
	}

	// Mint a download URL and fetch the exact bytes back.
	dlURL := downloadURLFor(t, srv, appID, id)
	resp := doReq(t, http.MethodGet, dlURL, nil)
	got := readAll(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("download: got %d want 200", resp.StatusCode)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: got %q want %q", got, want)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("content type: got %q want text/plain/*", ct)
	}

	// Expired download signature is a 403.
	expiredGet := reSign(h, dlURL, "download", appID, id, time.Now().Add(-time.Hour).Unix())
	mustStatus(t, doReq(t, http.MethodGet, expiredGet, nil), http.StatusForbidden)

	// Unsigned (missing signature) download is a 403 too.
	bare, _ := url.Parse(dlURL)
	bare.RawQuery = "app-id=" + url.QueryEscape(appID) + "&expires=9999999999"
	mustStatus(t, doReq(t, http.MethodGet, bare.String(), nil), http.StatusForbidden)
}

func newHandlerSrvApp(t *testing.T) (*Handler, *httptest.Server, string) {
	t.Helper()
	h := newHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return h, srv, platform.UUIDToStr(newUUIDStr())
}

func putBody(t *testing.T, urlStr string, body []byte) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, urlStr, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func deleteJSONAuthed(t *testing.T, srv *httptest.Server, path string, body map[string]any, token string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodDelete, srv.URL+path, bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("X-admin-token", token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	raw, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

// TestStorageAdminGate pins the destructive-route authorization: without a
// configured checker the admin routes answer 503 (fail-closed); with one,
// bad tokens are 401 and the valid token passes. Uploads stay runtime-open.
func TestStorageAdminGate(t *testing.T) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	appID := platform.UUIDToStr(newUUIDStr())

	// No checker wired → 503 on delete + download-url.
	bare := &Handler{Store: NewDiskBackend(t.TempDir(), secret), Secret: secret}
	srvBare := httptest.NewServer(bare)
	t.Cleanup(srvBare.Close)
	status, _ := deleteJSONAuthed(t, srvBare, "/storage/files", map[string]any{"app-id": appID, "ids": []string{uuidStrOf(newUUIDStr())}}, testAdminToken)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("delete without checker must 503, got %d", status)
	}
	dlReq, _ := http.NewRequest(http.MethodGet,
		srvBare.URL+"/storage/signed-download-url?app-id="+appID+"&id="+uuidStrOf(newUUIDStr()), nil)
	dlResp, err := http.DefaultClient.Do(dlReq)
	if err != nil {
		t.Fatal(err)
	}
	_ = dlResp.Body.Close()
	if dlResp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("download-url without checker must 503, got %d", dlResp.StatusCode)
	}

	// With a checker: bad token 401, good token 200.
	h := newHandler(t)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	status, _ = deleteJSONAuthed(t, srv, "/storage/files",
		map[string]any{"app-id": appID, "ids": []string{uuidStrOf(newUUIDStr())}}, "wrong-token")
	if status != http.StatusUnauthorized {
		t.Fatalf("bad admin token must 401, got %d", status)
	}
	status, _ = deleteJSONAuthed(t, srv, "/storage/files",
		map[string]any{"app-id": appID, "ids": []string{uuidStrOf(newUUIDStr())}}, testAdminToken)
	if status != 200 {
		t.Fatalf("valid admin token must pass, got %d", status)
	}

	// signed-upload-url mints write access into the app's $files namespace —
	// it is a control route and must be gated identically. Bare handler
	// (no checker) → 503; bad token → 401; valid token → 200.
	upBody := map[string]any{"app-id": appID, "path": "docs/readme.txt"}
	postStatus := func(h *Handler, token string) int {
		s := httptest.NewServer(h)
		t.Cleanup(s.Close)
		b, _ := json.Marshal(upBody)
		req, err := http.NewRequest(http.MethodPost, s.URL+"/storage/signed-upload-url", bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("app-id", appID)
		if token != "" {
			req.Header.Set("X-admin-token", token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		return resp.StatusCode
	}
	if got := postStatus(bare, ""); got != http.StatusServiceUnavailable {
		t.Fatalf("upload-url without checker must 503, got %d", got)
	}
	if got := postStatus(newHandler(t), "wrong-token"); got != http.StatusUnauthorized {
		t.Fatalf("upload-url with bad admin token must 401, got %d", got)
	}
	if got := postStatus(newHandler(t), testAdminToken); got != 200 {
		t.Fatalf("upload-url with valid admin token must pass, got %d", got)
	}
}

func uuidStrOf(u [16]byte) string { return platform.UUIDToStr(u) }

// TestUploadSizeCap proves MaxUploadBytes truncates oversized uploads with
// 413 instead of streaming unbounded bytes to disk.
func TestUploadSizeCap(t *testing.T) {
	h := newHandler(t)
	h.MaxUploadBytes = 8
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	appID := platform.UUIDToStr(newUUIDStr())

	uploadURL, _, _ := presignUpload(t, srv, appID, "big.bin")
	mustStatus(t, doReq(t, http.MethodPut, uploadURL, []byte("0123456789ABCDEF")), http.StatusRequestEntityTooLarge)
	// Under the cap still succeeds.
	mustStatus(t, doReq(t, http.MethodPut, uploadURL, []byte("tiny")), 200)
}

// TestDeleteFiles proves the bulk delete route removes objects such that a
// subsequent signed download is a 404, plus the single-file variant.
func TestDeleteFiles(t *testing.T) {
	srv, appID := newServer(t)

	url1, id1, _ := presignUpload(t, srv, appID, "a.txt")
	url2, id2, _ := presignUpload(t, srv, appID, "b.txt")
	mustStatus(t, doReq(t, http.MethodPut, url1, []byte("aaa")), 200)
	mustStatus(t, doReq(t, http.MethodPut, url2, []byte("bbb")), 200)

	// Bulk delete by ids (admin-gated).
	status, del := deleteJSONAuthed(t, srv, "/storage/files", map[string]any{"app-id": appID, "ids": []string{id1, id2}}, testAdminToken)
	if status != 200 {
		t.Fatalf("bulk delete: %d %v", status, del)
	}
	data, ok := del["data"].(map[string]any)
	if !ok {
		t.Fatalf("missing data envelope: %v", del)
	}
	deleted, _ := data["ids"].([]any)
	if len(deleted) != 2 {
		t.Fatalf("bulk delete ids: %v", data)
	}

	// Subsequent downloads must 404.
	for _, id := range []string{id1, id2} {
		dlURL := downloadURLFor(t, srv, appID, id)
		mustStatus(t, doReq(t, http.MethodGet, dlURL, nil), http.StatusNotFound)
	}

	// Single delete variant still works (admin-gated).
	url3, id3, _ := presignUpload(t, srv, appID, "c.txt")
	mustStatus(t, doReq(t, http.MethodPut, url3, []byte("ccc")), 200)
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/storage/files?id="+id3+"&app-id="+url.QueryEscape(appID), nil)
	req.Header.Set("X-admin-token", testAdminToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body := readAll(t, resp)
	if resp.StatusCode != 200 || !strings.Contains(string(body), id3) {
		t.Fatalf("single delete: %d %s", resp.StatusCode, body)
	}
}

// --- DATABASE_URL-gated $files triple linkage -------------------------------

// dbEnv follows the authn_test.go fixture pattern: fresh pool, drop schema,
// migrate, seed instant_users + platform.CreateApp.
func dbEnv(t *testing.T) (*httptest.Server, *Handler, *pgxpool.Pool, [16]byte) {
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
	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appUUID := newUUIDStr()
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUIDStr()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "s@test"); e != nil {
			return e
		}
		return platform.CreateApp(ctx, tx, creator, appUUID, "storage-test")
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Store: NewDiskBackend(t.TempDir(), secret), Secret: secret, Triples: st, Catalogs: cats,
		AdminTokenCheck: func(_ context.Context, _, tok string) (bool, error) { return tok == testAdminToken, nil }}
	srv := httptest.NewServer(h)
	t.Cleanup(func() { srv.Close(); pool.Close(); _ = sqldb.Close() })
	return srv, h, pool, appUUID
}

// TestFilesTriples proves a completed upload links $files entity triples
// exactly as v1's app-file-model/create! does (path, id, size, content-type,
// location-id, key-version).
// TestDuplicateFilename409 pins the M11 fix: a second upload reusing a
// $files.path that already exists must answer 409 Conflict, not leak a raw
// Postgres unique-violation as 500.
func TestDuplicateFilename409(t *testing.T) {
	srv, _, _, appUUID := dbEnv(t)
	appID := platform.UUIDToStr(appUUID)

	uploadURL1, _, _ := presignUpload(t, srv, appID, "dup.txt")
	req1, _ := http.NewRequest(http.MethodPut, uploadURL1+"&filename=dup.txt", bytes.NewReader([]byte("first")))
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp1, 200)

	uploadURL2, _, _ := presignUpload(t, srv, appID, "other.bin")
	req2, _ := http.NewRequest(http.MethodPut, uploadURL2+"&filename=dup.txt", bytes.NewReader([]byte("second")))
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp2.Body.Close() }()
	b, _ := io.ReadAll(resp2.Body)
	if resp2.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate filename must be 409, got %d (%s)", resp2.StatusCode, b)
	}
}

func TestFilesTriples(t *testing.T) {
	srv, h, pool, appUUID := dbEnv(t)
	appID := platform.UUIDToStr(appUUID)
	ctx := context.Background()

	uploadURL, id, _ := presignUpload(t, srv, appID, "report.txt")
	body := []byte("file contents here")
	// Attach advisory metadata like a real client would.
	req, _ := http.NewRequest(http.MethodPut, uploadURL+"&filename=report.txt", bytes.NewReader(body))
	req.Header.Set("Content-Type", "text/plain; charset=utf-8") // Go's client does not sniff PUT bodies
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	mustStatus(t, resp, 200)

	cat, err := platform.LoadAttrCatalog(ctx, pool, appUUID)
	if err != nil {
		t.Fatal(err)
	}
	entity, err := platform.ScanUUIDErr(id)
	if err != nil {
		t.Fatal(err)
	}
	fetched, err := h.Triples.FetchTriples(ctx, appUUID, storage.FetchFilter{EntityIDs: [][16]byte{entity}})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]any{}
	for _, row := range fetched {
		if a, ok := cat.ByID(row.Triple.A); ok && a.Label != nil {
			got[*a.Label] = row.Triple.V
		}
	}
	// storage.decodeValue decodes numerics with UseNumber → json.Number.
	normalize := func(v any) any {
		if n, ok := v.(json.Number); ok {
			f, _ := n.Float64()
			return f
		}
		return v
	}
	want := map[string]any{
		"path":         "report.txt",
		"id":           id,
		"size":         float64(len(body)),
		"location-id":  id,
		"key-version":  float64(1),
		"content-type": "text/plain; charset=utf-8",
	}
	for label, w := range want {
		if g := normalize(got[label]); g != w {
			t.Errorf("$files.%s: got %#v want %#v", label, got[label], w)
		}
	}
}
