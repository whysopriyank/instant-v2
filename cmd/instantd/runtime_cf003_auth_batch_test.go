package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestCF003AssembledAuthRefreshBatchAndSignout closes the CF-003 auth-http
// lifecycle/denied remainder over the production-mounted routes: the plural
// POST /runtime/auth/refresh_tokens batch and the POST /runtime/signout alias
// (both served by runtimeapi through mountRoutes, distinct from the singular
// /runtime/auth/verify_refresh_token + /runtime/auth/sign_out paths pinned by
// TestCF003HTTPMatrixOwnedPostgres). Every leg asserts the exact whole result
// so a narrowed user projection or a partially applied batch cannot hide
// behind an ID-only check.
func TestCF003AssembledAuthRefreshBatchAndSignout(t *testing.T) {
	mux, appID, _ := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)

	const unknownToken = "00000000-0000-4000-8000-000000000099"

	signInGuest := func() (string, string, map[string]any) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/auth/sign_in_guest",
			`{"app-id":"`+app+`"}`, nil)
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
			cf003JSON(t, map[string]any{"refresh_tokens": tokens, "app-id": app}), nil)
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

	// Positive batch resolves both tokens in request order with whole users.
	status, body := batch(tokA, tokB)
	if status != http.StatusOK {
		t.Fatalf("refresh batch = %d %q; want 200", status, body)
	}
	if got := usersOf(body); !reflect.DeepEqual(got, []any{wantA, wantB}) {
		t.Fatalf("refresh batch users = %#v; want exactly [%#v %#v]", got, wantA, wantB)
	}

	// One unknown token fails the whole batch with the stable denial, and the
	// failed batch mutates nothing: the same positive batch still resolves.
	status, body = batch(tokA, unknownToken)
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

	// The runtime alias signout uses snake_case keys and an empty envelope.
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/signout",
		cf003JSON(t, map[string]any{"app_id": app, "refresh_token": tokA}), nil)
	if status != http.StatusOK || body != "{}\n" {
		t.Fatalf("alias signout status/body = %d %q; want 200 {}\\n", status, body)
	}

	// The signed-out token now fails the whole batch; the survivor alone
	// still resolves to its exact whole user.
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

	// Singular replay of the signed-out token denies identically, and a
	// second alias signout is idempotent with the same empty envelope.
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": tokA, "app-id": app}), nil)
	if status != http.StatusUnauthorized || body != "{\"message\":\"authn: unknown refresh token\"}\n" {
		t.Fatalf("signed-out replay status/body = %d %q; want 401 exact denial", status, body)
	}
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/signout",
		cf003JSON(t, map[string]any{"app_id": app, "refresh_token": tokA}), nil)
	if status != http.StatusOK || body != "{}\n" {
		t.Fatalf("idempotent signout status/body = %d %q; want 200 {}\\n", status, body)
	}

	// Survivor stays valid through the singular path with its exact user.
	status, body, _ = cf003Serve(mux, http.MethodPost, "/runtime/auth/verify_refresh_token",
		cf003JSON(t, map[string]any{"refresh-token": tokB, "app-id": app}), nil)
	wantVerify := `{"user":{"id":"` + idB + `","refresh_token":"` + tokB + `","type":"guest"}}` + "\n"
	if status != http.StatusOK || body != wantVerify {
		t.Fatalf("survivor verify status/body = %d %q; want %d %q", status, body, http.StatusOK, wantVerify)
	}
}
