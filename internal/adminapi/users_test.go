package adminapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func TestAdminUsersPaginationContract(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	path := "/admin/users?app-id=" + uuidStr(appID)
	code, got := do(t, h, "GET", path, authHeaders(token), nil)
	if code != 200 || !reflect.DeepEqual(got, map[string]any{"users": []any{}}) {
		t.Fatalf("unprovisioned users: %d %#v", code, got)
	}

	ctx := context.Background()
	attrs := map[string]platform.Attr{}
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		for _, label := range []string{"email", "type", "extra"} {
			at, err := platform.GetOrCreateAttr(ctx, tx, appID, "$users", label, "blob", "one", false, false)
			if err != nil {
				return err
			}
			attrs[label] = at
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	h.Catalogs.Invalidate(uuidStr(appID))
	cat, err := h.Catalogs.For(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}

	// Insert out of UUID order; include email-only, type-only, JSON-null, and
	// extra-only entities. Pagination is over users, not individual triples.
	want := []any{}
	var triples []triple.Triple
	for i := 105; i > 0; i-- {
		id := [16]byte{15: byte(i)}
		u := map[string]any{"id": uuidStr(id)}
		if i != 2 {
			u["type"] = "user"
			triples = append(triples, triple.Triple{E: id, A: attrs["type"].ID, V: "user"})
		}
		if i == 1 || i == 2 {
			u["email"] = fmt.Sprintf("user%d@example.test", i)
			triples = append(triples, triple.Triple{E: id, A: attrs["email"].ID, V: u["email"]})
		}
		if i == 3 {
			u["email"] = nil
			triples = append(triples, triple.Triple{E: id, A: attrs["email"].ID, V: nil})
		}
		triples = append(triples, triple.Triple{E: id, A: attrs["extra"].ID, V: "not projected"})
		want = append([]any{u}, want...)
	}
	triples = append(triples, triple.Triple{E: [16]byte{15: 106}, A: attrs["extra"].ID, V: "not a listed user"})
	if _, err := db.InsertTriples(ctx, appID, cat, triples, false); err != nil {
		t.Fatal(err)
	}

	maxInt := strconv.Itoa(int(^uint(0) >> 1))
	for _, tc := range []struct {
		name, params string
		want         []any
	}{
		{"default limit", "", want[:100]},
		{"all", "&limit=200", want},
		{"entity page", "&limit=2&offset=1", want[1:3]},
		{"last page", "&limit=10&offset=103", want[103:]},
		{"zero limit", "&limit=0", []any{}},
		{"at end", "&offset=105", []any{}},
		{"past end", "&offset=200", []any{}},
		{"maximum limit", "&limit=" + maxInt, want},
		// Preserve the existing integer-overflow empty-page behavior; changing
		// validation semantics belongs to a separate API contract decision.
		{"overflow page end", "&limit=" + maxInt + "&offset=1", []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code, got := do(t, h, "GET", path+tc.params, authHeaders(token), nil)
			if code != 200 || !reflect.DeepEqual(got, map[string]any{"users": tc.want}) {
				t.Fatalf("got %d %#v; want %#v", code, got, tc.want)
			}
		})
	}
	for _, param := range []string{"limit", "offset"} {
		for _, value := range []string{"-1", "bad", maxInt + "0"} {
			t.Run(param+"="+value, func(t *testing.T) {
				code, got := do(t, h, "GET", path+"&"+param+"="+value, authHeaders(token), nil)
				want := map[string]any{"message": "invalid `" + param + "`"}
				if code != 400 || !reflect.DeepEqual(got, want) {
					t.Fatalf("got %d %#v; want 400 %#v", code, got, want)
				}
			})
		}
	}
}

// TestAdminUserRoutes walks the delegated authn envelopes end-to-end:
// sign_in_guest → users list → magic_code/verify_magic_code → refresh_tokens →
// sign_out → bulk delete.
func TestAdminUserRoutes(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	// sign_in_guest
	code, guest := do(t, h, "POST", "/admin/sign_in_guest", authHeaders(token), bodyApp(appID, nil))
	if code != 200 {
		t.Fatalf("sign_in_guest: %d %v", code, guest)
	}
	userMap, _ := guest["user"].(map[string]any)
	if userMap["refresh_token"] == "" || userMap["type"] != "guest" {
		t.Fatalf("guest envelope: %v", guest)
	}

	// users list (limit param)
	code, resp := do(t, h, "GET", "/admin/users?limit=10&offset=0&app-id="+appStr, authHeaders(token), nil)
	if code != 200 {
		t.Fatalf("users list: %d %v", code, resp)
	}
	usersJSON, _ := json.Marshal(resp["users"])
	var users []map[string]any
	_ = json.Unmarshal(usersJSON, &users)
	if len(users) != 1 || users[0]["type"] != "guest" {
		t.Fatalf("users list: %v", resp)
	}
	var guestID string
	_ = json.Unmarshal([]byte(users[0]["id"].(string)), &guestID)
	guestID = users[0]["id"].(string)

	// sign_out via the minted refresh token
	code, resp = do(t, h, "POST", "/admin/sign_out", authHeaders(token),
		map[string]any{"app-id": appStr, "refresh_token": userMap["refresh_token"]})
	if code != 200 || resp["ok"] != true {
		t.Fatalf("sign_out: %d %v", code, resp)
	}
	// sign_out with no selector → v1 message
	code, resp = do(t, h, "POST", "/admin/sign_out", authHeaders(token), map[string]any{"app-id": appStr})
	if code != 400 || !strings.Contains(resp["message"].(string), "`id`, `email`, or `refresh_token`") {
		t.Fatalf("sign_out validation: %d %v", code, resp)
	}

	// magic_code returns the generated 6-digit code; verify_magic_code burns it.
	email := "ada@test.example"
	code, resp = do(t, h, "POST", "/admin/magic_code", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email})
	if code != 200 {
		t.Fatalf("magic_code: %d %v", code, resp)
	}
	magicCode, _ := resp["code"].(string)
	if len(magicCode) != 6 {
		t.Fatalf("expected 6-digit code, got %v", resp["code"])
	}
	code, verified := do(t, h, "POST", "/admin/verify_magic_code", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email, "code": magicCode})
	if code != 200 {
		t.Fatalf("verify_magic_code: %d %v", code, verified)
	}
	vUser, _ := verified["user"].(map[string]any)
	if vUser["email"] != email || vUser["refresh_token"] == "" || verified["created"] != true {
		t.Fatalf("verify envelope: %v", verified)
	}

	// refresh_tokens creates then finds by email.
	code, created := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": "grace@test.example"})
	if code != 200 || created["created"] != true {
		t.Fatalf("refresh_tokens create: %d %v", code, created)
	}
	if cu, _ := created["user"].(map[string]any); cu["refresh_token"] == "" {
		t.Fatalf("refresh_tokens envelope: %v", created)
	}
	code, existing := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": "grace@test.example"})
	if code != 200 || existing["created"] != false {
		t.Fatalf("refresh_tokens find: %d %v", code, existing)
	}

	// Bulk delete the guest user; list must shrink back to the email users.

	code, del := do(t, h, "DELETE", "/admin/users", authHeaders(token),
		map[string]any{"app-id": appStr, "ids": []string{guestID}})
	if code != 200 {
		t.Fatalf("users delete: %d %v", code, del)
	}
	if n, _ := del["deleted"].(float64); n <= 0 {
		t.Fatalf("expected deleted > 0: %v", del)
	}
	code, resp = do(t, h, "GET", "/admin/users?app-id="+appStr, authHeaders(token), nil)
	if code != 200 {
		t.Fatalf("users relist: %d", code)
	}
	usersJSON, _ = json.Marshal(resp["users"])
	users = nil
	_ = json.Unmarshal(usersJSON, &users)
	for _, u := range users {
		if u["id"] == guestID {
			t.Fatalf("guest still listed after delete: %v", users)
		}
	}
}
