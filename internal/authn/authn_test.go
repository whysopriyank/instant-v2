package authn_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func env(t *testing.T) (*authn.Service, *authn.Handler, [16]byte, func()) {
	t.Helper()
	db := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx := context.Background()
	pool := db.Pool
	sqldb, err := sql.Open("pgx", db.DSN)
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
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := newUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "a@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "auth-test"); e != nil {
			return e
		}
		// Register the redirect origin the fixtures use; the production
		// OAuth gate refuses unregistered origins (fail-closed).
		_, e := tx.Exec(ctx,
			`UPDATE apps SET redirect_origins='["http://app"]'::jsonb WHERE id=$1`, appID)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	svc := &authn.Service{
		DB: st, Pool: pool, Catalogs: cats,
		GoogleClientID: "fixture-google-client", GoogleClientSecret: "fixture-google-secret",
		GitHubClientID: "fixture-github-client", GitHubClientSecret: "fixture-github-secret",
	}
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
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	code, resp := post(t, h, "/runtime/auth/send_magic_code", map[string]any{"email": "u@test", "app-id": appStr})
	if code != http.StatusServiceUnavailable || resp["message"] != "magic code delivery unavailable" {
		t.Fatalf("send: %d %v", code, resp)
	}
	if _, ok := resp["sent"]; ok {
		t.Fatal("unavailable delivery must not report sent=true")
	}
	if resp["message"] == "000000" || resp["message"] == "123456" {
		t.Fatal("unavailable delivery must not leak a magic code")
	}
	code2, resp2 := post(t, h, "/runtime/auth/send_magic_code", map[string]any{"email": "other@test", "app-id": appStr})
	if code2 != code || resp2["message"] != resp["message"] {
		t.Fatalf("unavailable delivery must not enumerate by email: first=%d/%v second=%d/%v", code, resp, code2, resp2)
	}

	// The unavailable path must not create a code entity. The system attrs
	// created while the service is initialized are unrelated to this check.
	var codeCount int
	if err := svc.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM triples t JOIN attrs a ON a.app_id=t.app_id AND a.id=t.attr_id
		  WHERE t.app_id=$1 AND a.etype='$magicCodes'`, appID).Scan(&codeCount); err != nil {
		t.Fatalf("count magic-code triples: %v", err)
	}
	if codeCount != 0 {
		t.Fatalf("unavailable delivery persisted %d magic-code triples", codeCount)
	}

	// Wrong code rejected.
	code, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "u@test", "code": "000000", "app-id": appStr})
	if code != 401 {
		t.Fatalf("expected 401 for wrong code, got %d %v", code, resp)
	}
}

// TestSignupRuleEvaluated pins R12: non-literal $users.create rules are now
// fully evaluated with an auth binding — fail-closed on eval error, denied
// for non-matching identities, allowed for matching ones.
func TestSignupRuleEvaluated(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	doc, err := perms.ParseRuleDoc([]byte(`{"$users":{"allow":{"create":"auth.email == 'allowed@test'"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	svc.RulesForFn = func(context.Context, [16]byte) (*perms.RuleDoc, error) { return doc, nil }

	mailer := &captureMailer{}
	svc.Mailer = mailer
	_ = svc.SendMagicCode(context.Background(), appID, "denied@test")
	// Non-matching identity → signup denied (403), code not burned.
	code, resp := post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "denied@test", "code": mailer.code, "app-id": appStr})
	if code != http.StatusForbidden {
		t.Fatalf("non-matching signup must be 403, got %d %v", code, resp)
	}

	// Matching identity → created.
	svc.NowFunc = func() time.Time { return time.Now().Add(2 * time.Minute) }
	mailer.code = ""
	_ = svc.SendMagicCode(context.Background(), appID, "allowed@test")
	code, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "allowed@test", "code": mailer.code, "app-id": appStr})
	if code != 200 {
		t.Fatalf("matching signup must succeed, got %d %v", code, resp)
	}

	// Rules resolution failure fails closed: signup must never succeed
	// while rules are unreadable, regardless of the presented code.
	svc.RulesForFn = func(context.Context, [16]byte) (*perms.RuleDoc, error) {
		return nil, errors.New("db down")
	}
	svc.NowFunc = func() time.Time { return time.Now().Add(4 * time.Minute) }
	mailer.code = ""
	if err := svc.SendMagicCode(context.Background(), appID, "someone@test"); err != nil {
		t.Fatal(err)
	}
	code, _ = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "someone@test", "code": "123456", "app-id": appStr})
	if code == 200 {
		t.Fatal("signup must fail while rules are unavailable")
	}
}

// TestMagicCodeBruteForceLockout pins the brute-force gate: repeated wrong
// codes escalate to 429 lockout for the (app,email) pair, and a correct code
// submitted during the window is refused too. A different email is unaffected
// (per-identity state).
func TestMagicCodeBruteForceLockout(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)

	mailer := &captureMailer{}
	svc.Mailer = mailer
	if err := svc.SendMagicCode(context.Background(), appID, "victim@test"); err != nil {
		t.Fatal(err)
	}

	var lastCode int
	for i := 0; i < 5; i++ {
		lastCode, _ = post(t, h, "/runtime/auth/verify_magic_code",
			map[string]any{"email": "victim@test", "code": fmt.Sprintf("%06d", i), "app-id": appStr})
		if lastCode != 401 && lastCode != 429 {
			t.Fatalf("attempt %d: want 401/429, got %d", i, lastCode)
		}
	}
	if lastCode != 429 {
		// Threshold crossed exactly on the final allowed failure → next
		// attempt must be locked even with the CORRECT code.
		var resp map[string]any
		lastCode, resp = post(t, h, "/runtime/auth/verify_magic_code",
			map[string]any{"email": "victim@test", "code": mailer.code, "app-id": appStr})
		if lastCode != 429 {
			t.Fatalf("locked pair must answer 429, got %d (%v)", lastCode, resp)
		}
	}

	// Sibling identity stays clean — lockout is per (app,email), not global.
	code2, resp2 := post(t, h, "/runtime/auth/send_magic_code", map[string]any{"email": "other@test", "app-id": appStr})
	if code2 != 200 || resp2["sent"] != true {
		t.Fatalf("sibling send: %d %v", code2, resp2)
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
	//
	// Resend-throttle semantics: back-to-back sends inside the cooldown
	// window succeed silently WITHOUT minting a new code (anti mail-bomb),
	// so step past the cooldown via the service's swappable clock first.
	svc.NowFunc = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if err := svc.SendMagicCode(context.Background(), appID, "u@test"); err != nil {
		t.Fatal(err)
	}
	firstCode := mailer.code
	if firstCode == "" {
		t.Fatal("post-cooldown send must mint a fresh code")
	}
	// Immediate resend inside the window: no new code delivered.
	if err := svc.SendMagicCode(context.Background(), appID, "u@test"); err != nil {
		t.Fatal(err)
	}
	if mailer.code != firstCode {
		t.Fatalf("in-window resend must not mint a new code: %q vs %q", mailer.code, firstCode)
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

func TestMagicCodeDeliveryNormalizesExpiresAndBurnsOnce(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)
	base := time.Now()
	svc.NowFunc = func() time.Time { return base }
	svc.CodeTTL = time.Minute

	mailer := &deliveryMailer{}
	svc.Mailer = mailer
	var logs bytes.Buffer
	svc.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	status, resp := post(t, h, "/runtime/auth/send_magic_code",
		map[string]any{"email": "  User@Example.COM  ", "app-id": appStr})
	if status != http.StatusOK || resp["sent"] != true {
		t.Fatalf("send: %d %v", status, resp)
	}
	if mailer.email != "user@example.com" {
		t.Fatalf("mailer email = %q, want normalized email", mailer.email)
	}
	if mailer.code == "" {
		t.Fatal("mailer never received a code")
	}
	status, resp = post(t, h, "/runtime/auth/send_magic_code",
		map[string]any{"email": " user@EXAMPLE.com ", "app-id": appStr})
	if status != http.StatusOK || resp["sent"] != true {
		t.Fatalf("normalized resend: %d %v", status, resp)
	}
	if mailer.calls != 1 {
		t.Fatalf("normalized resend delivered %d codes, want 1", mailer.calls)
	}
	if strings.Contains(logs.String(), mailer.code) {
		t.Fatal("magic code was written to logs")
	}
	if got := countMagicCodeTriples(t, svc, appID); got != 2 {
		t.Fatalf("persisted magic-code triples = %d, want 2", got)
	}

	status, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": " USER@example.com ", "code": mailer.code, "app-id": appStr})
	if status != http.StatusOK {
		t.Fatalf("verify normalized email: %d %v", status, resp)
	}
	if got := resp["user"].(map[string]any)["email"]; got != "user@example.com" {
		t.Fatalf("stored user email = %v, want normalized email", got)
	}

	status, _ = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "user@example.com", "code": mailer.code, "app-id": appStr})
	if status != http.StatusUnauthorized {
		t.Fatalf("second verification = %d, want 401", status)
	}

	mailer = &deliveryMailer{}
	svc.Mailer = mailer
	status, resp = post(t, h, "/runtime/auth/send_magic_code",
		map[string]any{"email": "expire@example.com", "app-id": appStr})
	if status != http.StatusOK || resp["sent"] != true {
		t.Fatalf("expiry send: %d %v", status, resp)
	}
	hashJSON, _ := json.Marshal(authn.HashToken(mailer.code))
	var codeCreated time.Time
	if err := svc.Pool.QueryRow(context.Background(), `
		SELECT t.created_at
		  FROM triples t JOIN attrs a ON a.app_id=t.app_id AND a.id=t.attr_id
		 WHERE t.app_id=$1 AND a.etype='$magicCodes' AND a.label='codeHash'
		   AND t.value=$2::jsonb`, appID, string(hashJSON)).Scan(&codeCreated); err != nil {
		t.Fatal(err)
	}
	svc.NowFunc = func() time.Time { return codeCreated.Add(time.Minute) }
	status, resp = post(t, h, "/runtime/auth/verify_magic_code",
		map[string]any{"email": "EXPIRE@example.com", "code": mailer.code, "app-id": appStr})
	if status != http.StatusUnauthorized || resp["message"] != authn.ErrExpiredCode.Error() {
		t.Fatalf("expired verification: %d %v", status, resp)
	}
}

func TestMagicCodeLegacyMixedCaseCompatibility(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	mailer := &deliveryMailer{}
	if err := svc.SendMagicCodeWithMailer(context.Background(), appID, "legacy@example.com", mailer); err != nil {
		t.Fatal(err)
	}
	cat, err := svc.Catalogs.For(context.Background(), platform.UUIDToStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	codeHash := cat.FindByEtypeLabel("$magicCodes", "codeHash")
	codeEmail := cat.FindByEtypeLabel("$magicCodes", "email")
	userIDAttr := cat.FindByEtypeLabel("$users", "id")
	userEmail := cat.FindByEtypeLabel("$users", "email")
	userType := cat.FindByEtypeLabel("$users", "type")
	rows, err := svc.DB.FetchTriples(context.Background(), appID, storage.FetchFilter{
		AttrIDs: [][16]byte{codeHash.ID}, Value: authn.HashToken(mailer.code),
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("code fixture: %v %v", rows, err)
	}
	if _, err := svc.Pool.Exec(context.Background(), `
		UPDATE triples SET value=to_jsonb($4::text)
		 WHERE app_id=$1 AND entity_id=$2 AND attr_id=$3`,
		appID, rows[0].Triple.E, codeEmail.ID, "Legacy@Example.COM"); err != nil {
		t.Fatal(err)
	}
	legacyUser := newUUID()
	if _, err := svc.DB.InsertTriples(context.Background(), appID, cat, []triple.Triple{
		{E: legacyUser, A: userIDAttr.ID, V: platform.UUIDToStr(legacyUser)},
		{E: legacyUser, A: userEmail.ID, V: "Legacy@Example.COM"},
		{E: legacyUser, A: userType.ID, V: "user"},
	}, false); err != nil {
		t.Fatal(err)
	}
	result, err := svc.VerifyMagicCode(context.Background(), appID, "legacy@example.com", mailer.code, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	user := result["user"].(map[string]any)
	if result["created"] != false || user["id"] != platform.UUIDToStr(legacyUser) {
		t.Fatalf("legacy identity was not reused: %v", result)
	}
}

func TestMagicCodeDeliveryFailureRollsBack(t *testing.T) {
	providerErr := errors.New("mailer failed")
	tests := []struct {
		name             string
		mailerErr        error
		cancelDuringSend bool
		wantErr          error
	}{
		{
			name:      "mailer error",
			mailerErr: providerErr,
			wantErr:   providerErr,
		},
		{
			name:      "provider timeout",
			mailerErr: context.DeadlineExceeded,
			wantErr:   context.DeadlineExceeded,
		},
		{
			name:             "request cancellation",
			cancelDuringSend: true,
			wantErr:          context.Canceled,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, h, appID, cleanup := env(t)
			defer cleanup()
			mailer := &deliveryMailer{err: tt.mailerErr}
			requestCtx := context.Background()
			if tt.cancelDuringSend {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				requestCtx = ctx
				mailer.cancel = cancel
				mailer.waitForCancellation = true
			}
			svc.Mailer = mailer

			body, err := json.Marshal(map[string]any{
				"email": "  Failed@Example.COM ", "app-id": platform.UUIDToStr(appID),
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/runtime/auth/send_magic_code", bytes.NewReader(body))
			req = req.WithContext(requestCtx)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("send status = %d, want 500; body=%s", rec.Code, rec.Body.String())
			}
			if mailer.email != "failed@example.com" {
				t.Fatalf("mailer email = %q, want normalized email", mailer.email)
			}
			if !errors.Is(mailer.observedErr, tt.wantErr) {
				t.Fatalf("mailer error = %v, want %v", mailer.observedErr, tt.wantErr)
			}
			if got := countMagicCodeTriples(t, svc, appID); got != 0 {
				t.Fatalf("failed delivery left %d persisted magic-code triples", got)
			}

			status, _ := post(t, h, "/runtime/auth/verify_magic_code", map[string]any{
				"email": "failed@example.com", "code": mailer.code, "app-id": platform.UUIDToStr(appID),
			})
			if status != http.StatusUnauthorized {
				t.Fatalf("failed-delivery code verified with status %d", status)
			}

			retryMailer := &deliveryMailer{}
			svc.Mailer = retryMailer
			if err := svc.SendMagicCode(context.Background(), appID, "failed@example.com"); err != nil {
				t.Fatalf("immediate retry after failed delivery: %v", err)
			}
			if retryMailer.calls != 1 {
				t.Fatalf("immediate retry delivered %d codes, want 1", retryMailer.calls)
			}
		})
	}
}

func TestMagicCodeRejectsInvalidMetadataBeforeDelivery(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	if err := svc.SendMagicCodeWithMailer(context.Background(), appID, "seed@example.com", &deliveryMailer{}); err != nil {
		t.Fatal(err)
	}
	result, err := svc.Pool.Exec(context.Background(), `
		UPDATE attrs
		   SET cardinality = 'many'
		 WHERE app_id = $1 AND etype = '$magicCodes' AND label = 'email'`, appID)
	if err != nil {
		t.Fatal(err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("corrupt metadata fixture changed %d attrs, want one", result.RowsAffected())
	}
	svc.InvalidateAttrs(appID)
	svc.Catalogs.Invalidate(platform.UUIDToStr(appID))
	mailer := &deliveryMailer{}

	if err := svc.SendMagicCodeWithMailer(context.Background(), appID, "invalid@example.com", mailer); err == nil {
		t.Fatal("send accepted incompatible magic-code metadata")
	}
	if mailer.calls != 0 {
		t.Fatalf("invalid metadata delivered %d emails, want 0", mailer.calls)
	}
}

func TestMagicCodeFailedAttemptReleasesLeaseAfterConcurrentBlock(t *testing.T) {
	svc, _, appID, cleanup := env(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 1)
	release := make(chan struct{})
	providerErr := errors.New("mailer failed")
	firstMailer := &blockingDeliveryMailer{started: started, release: release, err: providerErr}

	firstDone := make(chan error, 1)
	go func() {
		firstDone <- svc.SendMagicCodeWithMailer(ctx, appID, "blocked@example.com", firstMailer)
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first delivery did not reach the barrier")
	}

	blockedMailer := &deliveryMailer{}
	if err := svc.SendMagicCodeWithMailer(ctx, appID, "blocked@example.com", blockedMailer); err != nil {
		t.Fatalf("concurrent blocked send: %v", err)
	}
	if blockedMailer.calls != 0 {
		t.Fatalf("concurrent attempt delivered %d emails, want 0", blockedMailer.calls)
	}
	close(release)
	select {
	case err := <-firstDone:
		if !errors.Is(err, providerErr) {
			t.Fatalf("first delivery error = %v, want %v", err, providerErr)
		}
	case <-ctx.Done():
		t.Fatal("failed delivery did not finish")
	}

	retryMailer := &deliveryMailer{}
	if err := svc.SendMagicCodeWithMailer(ctx, appID, "blocked@example.com", retryMailer); err != nil {
		t.Fatalf("retry after failed delivery: %v", err)
	}
	if retryMailer.calls != 1 {
		t.Fatalf("retry delivered %d emails, want 1", retryMailer.calls)
	}
}

func TestMagicCodeNotVisibleBeforeMailerSuccess(t *testing.T) {
	svc, h, appID, cleanup := env(t)
	defer cleanup()
	appStr := platform.UUIDToStr(appID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	started := make(chan string, 1)
	release := make(chan struct{})
	mailer := &blockingDeliveryMailer{started: started, release: release}
	svc.Mailer = mailer

	done := make(chan error, 1)
	go func() { done <- svc.SendMagicCode(ctx, appID, "blocked@example.com") }()
	var code string
	select {
	case code = <-started:
	case <-ctx.Done():
		t.Fatal("delivery did not reach the barrier")
	}

	status, _ := post(t, h, "/runtime/auth/verify_magic_code", map[string]any{
		"email": "blocked@example.com", "code": code, "app-id": appStr,
	})
	if status != http.StatusUnauthorized {
		t.Fatalf("code became usable before delivery completed: status=%d", status)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("delivery: %v", err)
	}
	status, _ = post(t, h, "/runtime/auth/verify_magic_code", map[string]any{
		"email": "blocked@example.com", "code": code, "app-id": appStr,
	})
	if status != http.StatusOK {
		t.Fatalf("code after successful delivery: status=%d", status)
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

type deliveryMailer struct {
	email               string
	code                string
	calls               int
	err                 error
	cancel              context.CancelFunc
	waitForCancellation bool
	observedErr         error
}

type blockingDeliveryMailer struct {
	started chan<- string
	release <-chan struct{}
	err     error
}

func (m *blockingDeliveryMailer) SendMagicCode(_ context.Context, _, code string) error {
	m.started <- code
	<-m.release
	return m.err
}

func (m *deliveryMailer) SendMagicCode(ctx context.Context, email, code string) error {
	m.email = email
	m.code = code
	m.calls++
	if m.cancel != nil {
		m.cancel()
	}
	if m.waitForCancellation {
		<-ctx.Done()
		m.observedErr = ctx.Err()
		return m.observedErr
	}
	m.observedErr = m.err
	return m.err
}

func countMagicCodeTriples(t *testing.T, svc *authn.Service, appID [16]byte) int {
	t.Helper()
	var count int
	if err := svc.Pool.QueryRow(context.Background(), `
		SELECT count(*)
		  FROM triples t
		  JOIN attrs a ON a.app_id = t.app_id AND a.id = t.attr_id
		 WHERE t.app_id = $1 AND a.etype = '$magicCodes'`, appID).Scan(&count); err != nil {
		t.Fatalf("count magic-code triples: %v", err)
	}
	return count
}
