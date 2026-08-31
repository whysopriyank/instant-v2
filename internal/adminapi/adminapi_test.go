package adminapi_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func env(t *testing.T) (*adminapi.Handler, *storage.DB, [16]byte, string, func()) {
	t.Helper()
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx := context.Background()
	pool := fixture.Pool
	sqldb, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqldb.Close() })
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}
	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)
	appID := newUUID()
	token := newUUID()
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "a@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "admin-test"); e != nil {
			return e
		}
		return platform.SetAdminToken(ctx, tx, appID, token)
	})
	if err != nil {
		t.Fatal(err)
	}
	h := &adminapi.Handler{Pool: pool, DB: st, Catalogs: cats}
	cleanup := func() { pool.Close(); _ = sqldb.Close() }
	return h, st, appID, uuidStr(token), cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

// do issues one request against h with headers and an optional JSON body.
func do(t *testing.T, h *adminapi.Handler, method, path string, headers map[string]string, body any) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func authHeaders(token string) map[string]string {
	return map[string]string{"X-admin-token": token}
}

func bodyApp(appID [16]byte, extra map[string]any) map[string]any {
	m := map[string]any{"app-id": uuidStr(appID)}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// seedAttr creates a todos.name blob attr and returns its id plus a fresh cat.
func seedAttr(t *testing.T, h *adminapi.Handler, db *storage.DB, appID [16]byte) ([16]byte, *platform.AttrCatalog) {
	t.Helper()
	ctx := context.Background()
	var attr platform.Attr
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		var e error
		attr, e = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "name", "blob", "one", false, true)
		return e
	}); err != nil {
		t.Fatal(err)
	}
	h.Catalogs.Invalidate(uuidStr(appID))
	cat, err := h.Catalogs.For(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	return attr.ID, cat
}
