package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/ratelimit"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// deniedCapture is the checked-in raw HTTP capture
// (corpus/auth-http-denied-error.json) bound to the
// auth-http-denied-error coverage row by its self-bound metadata envelope
// (scenario == id == fixture == auth-http-denied-error).
type deniedCapture struct {
	Scenario  string `json:"scenario"`
	ID        string `json:"id"`
	Fixture   string `json:"fixture"`
	Transport string `json:"transport"`
	Status    string `json:"status"`
	Exchanges []struct {
		Request struct {
			Method string `json:"method"`
			Target string `json:"target"`
			Body   string `json:"body"`
		} `json:"request"`
		Response struct {
			Status  int                 `json:"status"`
			Headers map[string][]string `json:"headers"`
			Body    string              `json:"body"`
		} `json:"response"`
	} `json:"exchanges"`
}

const (
	deniedInvalidEmail = "cf003-denied@example.test"
	deniedExpiredEmail = "cf003-expired@example.test"
	deniedExpiredCode  = "654321"
	deniedSeedEntity   = "00000000-0000-4000-8000-000000000010"
)

// cf003DeniedMux provisions the owned isolated auth-http-denied-error fixture:
// the same deterministic seed as cf003PostgresMux (app
// 00000000-0000-4000-8000-000000000003, declared by
// corpus/fixtures/auth-http-denied-error.json) with the pool and runtime
// retained so the test can seed the expired leg and read throttle state.
func cf003DeniedMux(t *testing.T) (*http.ServeMux, [16]byte, *appRuntime) {
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
	return mux, appID, a
}

// cf003DeniedSeedExpired deterministically seeds the expired-magic-code leg:
// it bootstraps the $magicCodes system attrs through a guest sign-in (no
// throttle residue, no sleep), inserts a magic-code triple pair for the fixed
// code/email, and backdates created_at 48h (past the 24h TTL) so the verify
// path returns the exact expired denial.
func cf003DeniedSeedExpired(t *testing.T, mux *http.ServeMux, rt *appRuntime, appID [16]byte, email, code string) {
	t.Helper()
	ctx := context.Background()
	appStr := platform.UUIDToStr(appID)
	status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_in_guest",
		`{"app-id":"`+appStr+`"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("seed bootstrap guest sign-in = %d %q; want 200", status, body)
	}
	cat, err := rt.cats.For(ctx, appStr)
	if err != nil {
		t.Fatal(err)
	}
	codeHash := cat.FindByEtypeLabel("$magicCodes", "codeHash")
	codeEmail := cat.FindByEtypeLabel("$magicCodes", "email")
	if codeHash == nil || codeEmail == nil {
		t.Fatal("seed bootstrap did not create $magicCodes attrs")
	}
	entity, err := platform.ScanUUIDErr(deniedSeedEntity)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.st.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: entity, A: codeHash.ID, V: authn.HashToken(code)},
		{E: entity, A: codeEmail.ID, V: email},
	}, false); err != nil {
		t.Fatalf("seed expired code triples: %v", err)
	}
	if _, err := rt.pool.Exec(ctx, `UPDATE triples SET created_at = now() - make_interval(secs => 172800) WHERE app_id=$1 AND entity_id=$2`,
		appID, entity); err != nil {
		t.Fatalf("seed backdate created_at: %v", err)
	}
}

// TestCF003DeniedErrorCaptureReplay replays the checked-in
// auth-http-denied-error raw capture against the production-mounted routes on
// the owned isolated auth-http-denied-error fixture. Every exchange asserts
// exact status and exact whole-body bytes in file order, so a narrowed denial
// message or a replay-flipping second denial cannot hide behind a subset
// check. The expired leg is seeded deterministically (backdated created_at,
// never sleep-based) and replays the exact expired denial. The invalid and
// expired 401s persist auth_throttle failure rows via noteFailure, so the
// test asserts that exact expected throttle state (2 + 1 fails, below the
// 5-attempt lockout) instead of claiming no residue. The post-denial guest
// sign-in is a live shape check only (minted UUIDs are never checked in),
// not a no-residue proof. FU-02 Option A report-only; no corpusctl behavior
// change, no WS leg, no v1 claim.
func TestCF003DeniedErrorCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "auth-http-denied-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture deniedCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "auth-http-denied-error" || capture.ID != "auth-http-denied-error" ||
		capture.Fixture != "auth-http-denied-error" || capture.Transport != "http" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want auth-http-denied-error/auth-http-denied-error/auth-http-denied-error/http/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Exchanges) != 4 {
		t.Fatalf("capture exchanges = %d; want exactly 4 (400, 401, identical 401 replay, 401 expired)", len(capture.Exchanges))
	}

	// The capture's id and fixture must match the manifest coverage row that
	// claims it: a borrowed scenario or a mismatched fixture is rejected here
	// before any byte is replayed.
	manifestRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Coverage []struct {
			ID        string `json:"id"`
			Scenario  string `json:"scenario"`
			Fixture   string `json:"fixture"`
			Transport string `json:"transport"`
			Status    string `json:"status"`
		} `json:"coverage"`
	}
	if err := json.Unmarshal(manifestRaw, &manifest); err != nil {
		t.Fatalf("manifest decode: %v", err)
	}
	var row *struct {
		ID        string `json:"id"`
		Scenario  string `json:"scenario"`
		Fixture   string `json:"fixture"`
		Transport string `json:"transport"`
		Status    string `json:"status"`
	}
	for i := range manifest.Coverage {
		if manifest.Coverage[i].ID == capture.ID {
			row = &manifest.Coverage[i]
			break
		}
	}
	if row == nil {
		t.Fatalf("manifest has no coverage row for capture id %q", capture.ID)
	}
	if capture.Scenario != row.Scenario || capture.Fixture != row.Fixture ||
		capture.Transport != row.Transport || capture.Status != row.Status {
		t.Fatalf("capture envelope %q/%q/%q/%q does not match manifest row %q/%q/%q/%q",
			capture.Scenario, capture.Fixture, capture.Transport, capture.Status,
			row.Scenario, row.Fixture, row.Transport, row.Status)
	}

	mux, appID, rt := cf003DeniedMux(t)
	appStr := platform.UUIDToStr(appID)

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId and every app-bearing request body must equal the
	// live replay app. A capture edited to a foreign app-id fails here and in
	// the exchange loop below.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "auth-http-denied-error.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declaredFixture struct {
		AppID string `json:"appId"`
	}
	if err := json.Unmarshal(fixtureRaw, &declaredFixture); err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	if declaredFixture.AppID != appStr {
		t.Fatalf("declared fixture appId = %q; want replay app %q", declaredFixture.AppID, appStr)
	}
	for i, exchange := range capture.Exchanges {
		var reqBody map[string]any
		if err := json.Unmarshal([]byte(exchange.Request.Body), &reqBody); err != nil {
			t.Fatalf("exchange %d request body is not JSON: %v", i, err)
		}
		if app, ok := reqBody["app-id"].(string); ok && app != appStr {
			t.Fatalf("exchange %d app-id = %q; want replay app %q", i, app, appStr)
		}
	}

	// Replay the malformed leg and both invalid-code legs exactly. The system
	// attrs now exist, so seed the deterministically expired code before the
	// final leg.
	for i, exchange := range capture.Exchanges[:3] {
		status, body, headers := cf003Serve(mux, exchange.Request.Method, exchange.Request.Target, exchange.Request.Body, nil)
		if status != exchange.Response.Status || body != exchange.Response.Body {
			t.Fatalf("exchange %d status/body = %d %q; want %d %q",
				i, status, body, exchange.Response.Status, exchange.Response.Body)
		}
		if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, exchange.Response.Headers["Content-Type"]) {
			t.Fatalf("exchange %d Content-Type = %q; want %q",
				i, got, exchange.Response.Headers["Content-Type"])
		}
	}
	cf003DeniedSeedExpired(t, mux, rt, appID, deniedExpiredEmail, deniedExpiredCode)
	for i, exchange := range capture.Exchanges[3:] {
		status, body, headers := cf003Serve(mux, exchange.Request.Method, exchange.Request.Target, exchange.Request.Body, nil)
		if status != exchange.Response.Status || body != exchange.Response.Body {
			t.Fatalf("exchange %d status/body = %d %q; want %d %q",
				i+3, status, body, exchange.Response.Status, exchange.Response.Body)
		}
		if got := headers.Values("Content-Type"); !reflect.DeepEqual(got, exchange.Response.Headers["Content-Type"]) {
			t.Fatalf("exchange %d Content-Type = %q; want %q",
				i+3, got, exchange.Response.Headers["Content-Type"])
		}
	}

	// The failed verifications do not burn a code: the two checked-in 401
	// invalid legs are byte-identical on both sides of the exchange, not just
	// status-equal.
	first, second := capture.Exchanges[1], capture.Exchanges[2]
	if first.Request.Body != second.Request.Body || first.Response.Body != second.Response.Body {
		t.Fatalf("401 replay legs differ: %q/%q vs %q/%q",
			first.Request.Body, first.Response.Body, second.Request.Body, second.Response.Body)
	}

	// The 401s persist auth_throttle failure rows via noteFailure (there is
	// no no-residue property for invalid codes): assert the exact expected
	// state — 2 fails for the twice-denied email, 1 for the expired email —
	// with neither pair locked, all below the 5-attempt lockout threshold.
	ctx := context.Background()
	rows, err := rt.pool.Query(ctx, `SELECT key, fails, locked_until > now() AS locked FROM auth_throttle WHERE app_id=$1`, appID)
	if err != nil {
		t.Fatalf("read auth_throttle: %v", err)
	}
	throttle := map[string]int{}
	locked := map[string]bool{}
	for rows.Next() {
		var key string
		var fails int
		var isLocked bool
		if err := rows.Scan(&key, &fails, &isLocked); err != nil {
			rows.Close()
			t.Fatalf("scan auth_throttle: %v", err)
		}
		throttle[key] = fails
		locked[key] = isLocked
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate auth_throttle: %v", err)
	}
	wantThrottle := map[string]int{deniedInvalidEmail: 2, deniedExpiredEmail: 1}
	if !reflect.DeepEqual(throttle, wantThrottle) {
		t.Fatalf("auth_throttle = %#v; want exactly %#v", throttle, wantThrottle)
	}
	for key, isLocked := range locked {
		if isLocked {
			t.Fatalf("auth_throttle %q is locked; want unlocked (fails below threshold)", key)
		}
	}

	// Post-denial guest sign-in is a live shape check only (minted values
	// asserted live, never checked in): it proves the denial did not break
	// guest issuance, not that the denial left no residue.
	status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_in_guest",
		`{"app-id":"`+appStr+`"}`, nil)
	if status != http.StatusOK {
		t.Fatalf("guest sign-in after denial = %d %q; want 200", status, body)
	}
	var envelope struct {
		User map[string]any `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("guest sign-in body = %q: %v", body, err)
	}
	if len(envelope.User) != 3 || envelope.User["type"] != "guest" {
		t.Fatalf("guest user shape = %#v; want exactly id/type/refresh_token", envelope.User)
	}
	id, _ := envelope.User["id"].(string)
	tok, _ := envelope.User["refresh_token"].(string)
	if _, err := platform.ScanUUIDErr(id); err != nil || len(tok) != 36 {
		t.Fatalf("guest identity/token = %q/%q", id, tok)
	}
	if _, err := platform.ScanUUIDErr(tok); err != nil {
		t.Fatalf("guest refresh token not uuid = %q: %v", tok, err)
	}
}
