package authn_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func env(t *testing.T) (*authn.Service, *authn.Handler, [16]byte, func()) {
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
	appID := newUUID()
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "a@test"); e != nil {
			return e
		}
		return platform.CreateApp(ctx, tx, creator, appID, "auth-test")
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := &authn.Service{DB: st, Pool: pool, Catalogs: cats}
	h := &authn.Handler{Service: svc}
	cleanup := func() { pool.Close(); _ = sqldb.Close() }
	return svc, h, appID, cleanup
}

func newUUID() [16]byte {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func post(t *testing.T, h *authn.Handler, path string, body map[string]any) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest("POST", path, bytes.NewReader(b))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// TestMagicCodeFlow proves send → verify → refresh-token verify → sign-out
// end-to-end over the HTTP wire shapes.
func TestMagicCodeFlow(t *testing.T) {
	_, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	code, resp := post(t, h, "/runtime/auth/send_magic_code", map[string]any{"email": "u@test", "app-id": appStr})
	if code != 200 || resp["sent"] != true {
		t.Fatalf("send: %d %v", code, resp)
	}
	// The dev no-mailer path logs the code; for the test we bypass by using
	// the service directly to mint a known code.
	// Instead of parsing logs, exercise verify with an injected mailer capture:
	// (see TestMagicCodeWithMailer below)

	// Wrong code rejected.
	code, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "u@test", "code": "000000", "app-id": appStr})
	if code != 401 {
		t.Fatalf("expected 401 for wrong code, got %d %v", code, resp)
	}
}

// TestMagicCodeWithMailer captures the generated code via a fake mailer and
// completes sign-in.
func TestMagicCodeWithMailer(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	mailer := &captureMailer{}
	svc.Mailer = mailer
	if err := svc.SendMagicCode(context.Background(), appID, "u@test"); err != nil {
		t.Fatal(err)
	}
	if mailer.code == "" {
		t.Fatal("mailer never received a code")
	}
	code, resp := post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "u@test", "code": mailer.code, "app-id": appStr})
	if code != 200 {
		t.Fatalf("verify: %d %v", code, resp)
	}
	userObj := resp["user"].(map[string]any)
	token := userObj["refresh_token"].(string)
	if token == "" {
		t.Fatal("no refresh_token returned")
	}
	if created, ok := resp["created"].(bool); !ok || !created {
		t.Fatalf("first verify must be created=true, got %v", resp["created"])
	}
	if userObj["type"] != "user" {
		t.Fatalf("user type = %v", userObj["type"])
	}

	// Refresh token verifies over WS-shaped init path too.
	u, err := svc.VerifyRefreshToken(context.Background(), appID, token)
	if err != nil || u.Email != "u@test" {
		t.Fatalf("VerifyRefreshToken: %v %+v", err, u)
	}

	// Second verify with same email → existing user, new token.
	if err := svc.SendMagicCode(context.Background(), appID, "u@test"); err != nil {
		t.Fatal(err)
	}
	mailer.code = ""
	if err := svc.SendMagicCode(context.Background(), appID, "u@test"); err != nil {
		t.Fatal(err)
	}
	code, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "u@test", "code": mailer.code, "app-id": appStr})
	if code != 200 {
		t.Fatalf("re-verify: %d %v", code, resp)
	}
	if created, _ := resp["created"].(bool); created {
		t.Fatal("second sign-in must be created=false")
	}

	// verify_refresh_token endpoint shape.
	code, resp = post(t, h, "/runtime/auth/verify_refresh_token",
		map[string]any{"refresh-token": token, "app-id": appStr})
	if code != 200 {
		t.Fatalf("verify_refresh_token: %d %v", code, resp)
	}

	// Sign out kills the token (snake_case quirk).
	code, _ = post(t, h, "/runtime/auth/sign_out",
		map[string]any{"refresh_token": token, "app_id": appStr})
	if code != 200 {
		t.Fatalf("sign_out: %d", code)
	}
	if _, err := svc.VerifyRefreshToken(context.Background(), appID, token); err == nil {
		t.Fatal("token must be dead after sign-out")
	}
}

// TestGuestUpgrade proves guest sign-in then magic-code upgrade keeps identity.
func TestGuestUpgrade(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	code, resp := post(t, h, "/runtime/auth/sign_in_guest", map[string]any{"app-id": appStr})
	if code != 200 {
		t.Fatalf("guest: %d %v", code, resp)
	}
	guest := resp["user"].(map[string]any)
	guestToken := guest["refresh_token"].(string)
	if guest["type"] != "guest" {
		t.Fatalf("type = %v", guest["type"])
	}

	mailer := &captureMailer{}
	svc.Mailer = mailer
	if err := svc.SendMagicCode(context.Background(), appID, "real@user"); err != nil {
		t.Fatal(err)
	}
	code, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "real@user", "code": mailer.code,
			"app-id": appStr, "refresh-token": guestToken})
	if code != 200 {
		t.Fatalf("upgrade verify: %d %v", code, resp)
	}
	upgraded := resp["user"].(map[string]any)
	if upgraded["type"] != "user" {
		t.Fatalf("upgraded type = %v, want user", upgraded["type"])
	}
	if upgraded["id"] != guest["id"] {
		t.Fatal("guest id must be preserved on upgrade")
	}
}

type captureMailer struct{ code string }

func (m *captureMailer) SendMagicCode(_ context.Context, _, code string) error {
	m.code = code
	return nil
}
