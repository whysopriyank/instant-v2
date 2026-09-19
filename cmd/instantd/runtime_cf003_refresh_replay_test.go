package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// refreshCapture is the checked-in raw HTTP capture
// (corpus/auth-http-refresh-lifecycle.json) bound to the
// auth-http-refresh-lifecycle coverage row by its self-bound metadata envelope
// (scenario == id == auth-http-refresh-lifecycle, dedicated fixture
// auth-http-refresh-lifecycle).
type refreshCapture struct {
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

// refreshUnknownToken is the fixed unknown refresh token used by every
// deterministic checked-in exchange. It never exists in the fixture store, so
// the batch, singular-verify, and signout legs are exactly replayable without
// minted values.
const refreshUnknownToken = "00000000-0000-4000-8000-000000000099"

// TestCF003RefreshLifecycleCaptureReplay replays the checked-in
// auth-http-refresh-lifecycle raw capture against the production-mounted
// routes on the owned isolated auth-http-refresh-lifecycle fixture (the same
// deterministic cf003 seed triple as the denied leg: app
// 00000000-0000-4000-8000-000000000003). Every exchange asserts exact status
// and exact whole-body bytes in file order, so a narrowed denial message or a
// replay-flipping second denial cannot hide behind a subset check. The
// unknown-token batch 401 replayed byte-identically proves a failed batch
// mutates nothing; the signout 200 pair proves the alias empty envelope and
// idempotence. Minted-token rotation (issuance, positive whole-user batch,
// mixed minted-plus-unknown denial, minted signout invalidation, survivor
// convergence) cannot be checked in as exact bytes, so the test replays that
// flow live with the same exact assertions as the accepted assembled leg —
// values asserted live, never checked in. Refresh 401s never touch
// auth_throttle (only the magic-code path calls noteFailure), so the test
// asserts the throttle table stays exactly empty instead of claiming
// per-email failure rows. FU-02 Option A report-only; no corpusctl behavior
// change, no WS leg, no v1 claim.
func TestCF003RefreshLifecycleCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "auth-http-refresh-lifecycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture refreshCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "auth-http-refresh-lifecycle" || capture.ID != "auth-http-refresh-lifecycle" ||
		capture.Fixture != "auth-http-refresh-lifecycle" || capture.Transport != "http" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want auth-http-refresh-lifecycle/auth-http-refresh-lifecycle/auth-http-refresh-lifecycle/http/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if len(capture.Exchanges) != 7 {
		t.Fatalf("capture exchanges = %d; want exactly 7 (400 batch, 401 batch, identical 401 replay, 401 singular, 200 signout, identical 200 replay, 400 signout)", len(capture.Exchanges))
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

	// The denied-leg mux provisions the identical deterministic cf003 seed
	// triple declared by corpus/fixtures/auth-http-refresh-lifecycle.json, so
	// it is reused here rather than duplicated.
	mux, appID, rt := cf003DeniedMux(t)
	appStr := platform.UUIDToStr(appID)

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId, creatorId, adminToken, and txSteps must equal the
	// live replay seed (deterministic triple app
	// 00000000-0000-4000-8000-000000000003, creator
	// 00000000-0000-4000-8000-000000000001, admin
	// 00000000-0000-4000-8000-000000000002, plus exactly-empty txSteps) declared by the seed
	// helper, and every app-bearing request body (both the "app-id" and the
	// alias "app_id" spellings) must equal the live replay app. A capture
	// edited to a foreign app-id fails here and in the exchange loop below;
	// a fixture edited to a foreign creator, admin, or non-empty txSteps
	// fails here before any byte is replayed.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "auth-http-refresh-lifecycle.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declaredFixture struct {
		AppID      string `json:"appId"`
		CreatorID  string `json:"creatorId"`
		AdminToken string `json:"adminToken"`
		TxSteps    []any  `json:"txSteps"`
	}
	if err := json.Unmarshal(fixtureRaw, &declaredFixture); err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	if declaredFixture.AppID != appStr {
		t.Fatalf("declared fixture appId = %q; want replay app %q", declaredFixture.AppID, appStr)
	}
	if declaredFixture.CreatorID != cf003CreatorID {
		t.Fatalf("declared fixture creatorId = %q; want seed creator %q", declaredFixture.CreatorID, cf003CreatorID)
	}
	if declaredFixture.AdminToken != cf003AdminToken {
		t.Fatalf("declared fixture adminToken = %q; want seed admin %q", declaredFixture.AdminToken, cf003AdminToken)
	}
	if declaredFixture.TxSteps == nil || len(declaredFixture.TxSteps) != 0 {
		t.Fatalf("declared fixture txSteps = %#v; want exactly empty", declaredFixture.TxSteps)
	}
	for i, exchange := range capture.Exchanges {
		var reqBody map[string]any
		if err := json.Unmarshal([]byte(exchange.Request.Body), &reqBody); err != nil {
			t.Fatalf("exchange %d request body is not JSON: %v", i, err)
		}
		for _, key := range []string{"app-id", "app_id"} {
			if app, ok := reqBody[key].(string); ok && app != appStr {
				t.Fatalf("exchange %d %s = %q; want replay app %q", i, key, app, appStr)
			}
		}
		if tok, ok := reqBody["refresh_token"].(string); ok && tok != refreshUnknownToken {
			t.Fatalf("exchange %d refresh_token = %q; want fixed unknown %q", i, tok, refreshUnknownToken)
		}
	}

	// Replay every checked-in exchange exactly, in file order.
	for i, exchange := range capture.Exchanges {
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

	// The failed unknown-token batch burns nothing: the two checked-in 401
	// batch legs are byte-identical on both sides of the exchange, not just
	// status-equal. The same holds for the idempotent signout 200 pair.
	first, second := capture.Exchanges[1], capture.Exchanges[2]
	if first.Request.Body != second.Request.Body || first.Response.Body != second.Response.Body {
		t.Fatalf("401 batch replay legs differ: %q/%q vs %q/%q",
			first.Request.Body, first.Response.Body, second.Request.Body, second.Response.Body)
	}
	fourth, fifth := capture.Exchanges[4], capture.Exchanges[5]
	if fourth.Request.Body != fifth.Request.Body || fourth.Response.Body != fifth.Response.Body {
		t.Fatalf("signout replay legs differ: %q/%q vs %q/%q",
			fourth.Request.Body, fourth.Response.Body, fifth.Request.Body, fifth.Response.Body)
	}

	// Minted-token rotation cannot be checked in, so it is replayed live with
	// the same exact assertions as the accepted assembled leg (values asserted
	// live, never checked in): two distinct guest issuances, exact whole-user
	// batch application in request order, mixed minted-plus-unknown denial
	// that mutates nothing, alias signout invalidation, survivor convergence,
	// singular replay denial, idempotent signout, and survivor singular
	// verification.
	signInGuest := func() (string, string, map[string]any) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_in_guest",
			`{"app-id":"`+appStr+`"}`, nil)
		if status != http.StatusOK {
			t.Fatalf("guest sign-in = %d %q", status, body)
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
		return id, tok, envelope.User
	}
	batch := func(tokens ...string) (int, string) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/auth/refresh_tokens",
			cf003JSON(t, map[string]any{"refresh_tokens": tokens, "app-id": appStr}), nil)
		return status, body
	}
	usersOf := func(body string) []any {
		t.Helper()
		var envelope struct {
			Users []any `json:"users"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatalf("refresh batch body = %q: %v", body, err)
		}
		if envelope.Users == nil {
			envelope.Users = []any{}
		}
		return envelope.Users
	}

	idA, tokA, userA := signInGuest()
	idB, tokB, userB := signInGuest()
	if idA == idB || tokA == tokB {
		t.Fatalf("guest identities not distinct: %q/%q vs %q/%q", idA, tokA, idB, tokB)
	}
	wantA := map[string]any{"id": idA, "type": "guest", "refresh_token": tokA}
	wantB := map[string]any{"id": idB, "type": "guest", "refresh_token": tokB}
	if !reflect.DeepEqual(userA, wantA) {
		t.Fatalf("sign-in user A = %#v; want exactly %#v", userA, wantA)
	}
	if !reflect.DeepEqual(userB, wantB) {
		t.Fatalf("sign-in user B = %#v; want exactly %#v", userB, wantB)
	}

	status, body := batch(tokA, tokB)
	if status != http.StatusOK {
		t.Fatalf("refresh batch = %d %q; want 200", status, body)
	}
	if got := usersOf(body); !reflect.DeepEqual(got, []any{wantA, wantB}) {
		t.Fatalf("refresh batch users = %#v; want exactly [%#v %#v]", got, wantA, wantB)
	}

	status, body = batch(tokA, refreshUnknownToken)
	if status != http.StatusUnauthorized || body != "{\"message\":\"authn: unknown refresh token\"}\n" {
		t.Fatalf("unknown batch status/body = %d %q; want 401 exact denial", status, body)
	}
	status, body = batch(tokA, tokB)
	if status != http.StatusOK {
		t.Fatalf("batch after denial = %d %q; want 200", status, body)
	}
	if got := usersOf(body); !reflect.DeepEqual(got, []any{wantA, wantB}) {
		t.Fatalf("batch after denial users = %#v; want exactly [%#v %#v]", got, wantA, wantB)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/signout",
		cf003JSON(t, map[string]any{"app_id": appStr, "refresh_token": tokA}), nil)
	if status != http.StatusOK || body != "{}\n" {
		t.Fatalf("alias signout status/body = %d %q; want 200 {}\\n", status, body)
	}

	status, body = batch(tokA, tokB)
	if status != http.StatusUnauthorized || body != "{\"message\":\"authn: unknown refresh token\"}\n" {
		t.Fatalf("post-signout batch status/body = %d %q; want 401 exact denial", status, body)
	}
	status, body = batch(tokB)
	if status != http.StatusOK {
		t.Fatalf("survivor batch = %d %q; want 200", status, body)
	}
	if got := usersOf(body); !reflect.DeepEqual(got, []any{wantB}) {
		t.Fatalf("survivor batch users = %#v; want exactly [%#v]", got, wantB)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": tokA, "app-id": appStr}), nil)
	if status != http.StatusUnauthorized || body != "{\"message\":\"authn: unknown refresh token\"}\n" {
		t.Fatalf("signed-out replay status/body = %d %q; want 401 exact denial", status, body)
	}
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/signout",
		cf003JSON(t, map[string]any{"app_id": appStr, "refresh_token": tokA}), nil)
	if status != http.StatusOK || body != "{}\n" {
		t.Fatalf("idempotent signout status/body = %d %q; want 200 {}\\n", status, body)
	}

	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": tokB, "app-id": appStr}), nil)
	wantVerify := `{"user":{"id":"` + idB + `","refresh_token":"` + tokB + `","type":"guest"}}` + "\n"
	if status != http.StatusOK || body != wantVerify {
		t.Fatalf("survivor verify status/body = %d %q; want %d %q", status, body, http.StatusOK, wantVerify)
	}

	// The refresh lifecycle leaves no throttle residue: unlike the
	// magic-code path, refresh verification and signout never call
	// noteFailure, so the throttle table must be exactly empty after the
	// checked-in 401s and the full live rotation/invalidation flow.
	ctx := context.Background()
	rows, err := rt.pool.Query(ctx, `SELECT count(*) FROM auth_throttle WHERE app_id=$1`, appID)
	if err != nil {
		t.Fatalf("read auth_throttle: %v", err)
	}
	var throttleCount int64
	if rows.Next() {
		if err := rows.Scan(&throttleCount); err != nil {
			rows.Close()
			t.Fatalf("scan auth_throttle: %v", err)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate auth_throttle: %v", err)
	}
	if throttleCount != 0 {
		t.Fatalf("auth_throttle rows = %d; want exactly 0 (refresh flow writes no throttle state)", throttleCount)
	}
}
