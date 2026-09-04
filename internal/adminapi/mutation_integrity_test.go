package adminapi_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/authn"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
)

func TestAdminUserMutationJournalingAndInvalidation(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)

	var lastTxID int64
	var lastAttrIDs []string
	var commitCount int
	h.OnCommit = func(ctx context.Context, committedAppID [16]byte, attrIDs []string, txID int64, attrsChanged bool) {
		if committedAppID == appID {
			lastTxID = txID
			lastAttrIDs = attrIDs
			commitCount++
		}
	}

	// 1. Create user and mint refresh token via /admin/refresh_tokens with extra-fields
	email := "integrity-user@example.test"
	code, resp := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{
			"app-id":       appStr,
			"email":        email,
			"extra-fields": map[string]any{"role": "admin"},
		})
	if code != 200 || resp["created"] != true {
		t.Fatalf("refresh_tokens create: %d %v", code, resp)
	}
	if lastTxID <= 0 {
		t.Fatalf("expected transaction journaled with txID > 0, got %d", lastTxID)
	}
	if len(lastAttrIDs) == 0 {
		t.Fatalf("expected invalidation attrIDs, got empty")
	}
	cat, err := h.Catalogs.For(context.Background(), appStr)
	if err != nil {
		t.Fatalf("reload catalog after user creation: %v", err)
	}
	wantMutationAttrs := make(map[string]bool)
	for _, spec := range [][2]string{
		{"$users", "type"},
		{"$users", "email"},
		{"$users", "role"},
		{"$userRefreshTokens", "hashedToken"},
		{"$userRefreshTokens", "$user"},
	} {
		at := cat.FindByEtypeLabel(spec[0], spec[1])
		if at == nil {
			t.Fatalf("missing expected mutation attr %s.%s", spec[0], spec[1])
		}
		wantMutationAttrs[at.UUID()] = true
	}
	if !sameStringSet(lastAttrIDs, wantMutationAttrs) {
		t.Fatalf("create invalidation attrs = %v; want exact %v", lastAttrIDs, wantMutationAttrs)
	}

	// Verify journal entry in the database
	ctx := context.Background()
	var one int
	err = db.Pool.QueryRow(ctx, "SELECT 1 FROM transactions WHERE id = $1 AND app_id = $2", lastTxID, appID).Scan(&one)
	if err != nil {
		t.Fatalf("expected transactions table row for txID %d: %v", lastTxID, err)
	}

	uMap, _ := resp["user"].(map[string]any)
	userIDStr, _ := uMap["id"].(string)

	// 2. Delete user via DELETE /admin/users and verify RETURNING attr_id derivation
	prevTxID := lastTxID
	code, delResp := do(t, h, "DELETE", "/admin/users", authHeaders(token),
		map[string]any{"app-id": appStr, "ids": []string{userIDStr}})
	if code != 200 {
		t.Fatalf("users delete: %d %v", code, delResp)
	}
	if lastTxID <= prevTxID {
		t.Fatalf("expected new transaction journaled on delete (%d > %d)", lastTxID, prevTxID)
	}
	if !sameStringSet(lastAttrIDs, wantMutationAttrs) {
		t.Fatalf("delete invalidation attrs = %v; want exact %v", lastAttrIDs, wantMutationAttrs)
	}

	err = db.Pool.QueryRow(ctx, "SELECT 1 FROM transactions WHERE id = $1 AND app_id = $2", lastTxID, appID).Scan(&one)
	if err != nil {
		t.Fatalf("expected transactions table row for delete txID %d: %v", lastTxID, err)
	}

	// 3. Test OnCommitChanges-only handler receives unknown-change notification
	h.OnCommit = nil
	changesReceived := false
	h.OnCommitChanges = func(ctx context.Context, committedApp [16]byte, changes []reactive.Change, txID int64, attrsChanged bool) {
		if committedApp == appID && changes == nil {
			changesReceived = true
		}
	}
	do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": "oncommit-changes-only@example.test"})
	if !changesReceived {
		t.Fatalf("expected handler with only OnCommitChanges to receive unknown-change invalidation")
	}
}

func sameStringSet(got []string, want map[string]bool) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, value := range got {
		if seen[value] || !want[value] {
			return false
		}
		seen[value] = true
	}
	return true
}

func TestAdminAuthServiceReuseAndCustomFieldInvalidation(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)
	ctx := context.Background()

	// Share a single auth service instance
	authSvc := &authn.Service{DB: h.DB, Pool: h.Pool, Catalogs: h.Catalogs, Logger: h.Logger}
	h.Auth = authSvc

	// 1. Warm the shared auth service cache for this appID before the user/attribute exists
	_, _ = authSvc.VerifyRefreshToken(ctx, appID, "warmup-nonexistent-token")

	// 2. Create an admin user with extra-fields.nickname
	email := "lovelace@example.test"
	nickname := "Ada Lovelace"
	code, resp := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{
			"app-id":       appStr,
			"email":        email,
			"extra-fields": map[string]any{"nickname": nickname},
		})
	if code != 200 || resp["created"] != true {
		t.Fatalf("create user: %d %v", code, resp)
	}
	uMap, _ := resp["user"].(map[string]any)
	rt, _ := uMap["refresh_token"].(string)
	if rt == "" {
		t.Fatalf("missing refresh token in response: %v", resp)
	}

	// 3. Verify the returned refresh token through that same shared service
	verifiedUser, err := authSvc.VerifyRefreshToken(ctx, appID, rt)
	if err != nil {
		t.Fatalf("verify refresh token failed: %v", err)
	}
	if verifiedUser.Extra["nickname"] != nickname {
		t.Fatalf("expected nickname %q in Extra, got %#v", nickname, verifiedUser.Extra)
	}
}

func TestAdminExistingUserTokenMintDoesNotReportAttrChanges(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)
	email := "existing-token@example.test"

	var attrsChanged []bool
	h.OnCommit = func(_ context.Context, committedAppID [16]byte, _ []string, _ int64, changed bool) {
		if committedAppID == appID {
			attrsChanged = append(attrsChanged, changed)
		}
	}

	code, resp := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email})
	if code != 200 || resp["created"] != true {
		t.Fatalf("create user: %d %v", code, resp)
	}
	code, resp = do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{"app-id": appStr, "email": email})
	if code != 200 || resp["created"] != false {
		t.Fatalf("mint existing-user token: %d %v", code, resp)
	}
	if len(attrsChanged) != 2 || !attrsChanged[0] || attrsChanged[1] {
		t.Fatalf("attrsChanged callbacks = %v, want [true false]", attrsChanged)
	}
}

func TestAdminUserCreationAtomicFailure(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	appStr := uuidStr(appID)

	var txCountBefore int
	err := db.Pool.QueryRow(ctx, "SELECT count(*) FROM transactions WHERE app_id = $1", appID).Scan(&txCountBefore)
	if err != nil {
		t.Fatal(err)
	}

	var postCommitFired bool
	h.OnCommit = func(ctx context.Context, committedAppID [16]byte, attrIDs []string, txID int64, attrsChanged bool) {
		postCommitFired = true
	}

	// Inject deterministic failure before token insertion commit
	faultCalls := 0
	adminapi.SetFaultBeforeTokenCommitForTest(h, func() error {
		faultCalls++
		return errors.New("simulated token insertion fault")
	})

	email := "atomic-fault@example.test"
	code, resp := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
		map[string]any{
			"app-id":       appStr,
			"email":        email,
			"extra-fields": map[string]any{"role": "admin"},
		})
	if code != 500 {
		t.Fatalf("expected 500 on token fault, got %d %v", code, resp)
	}
	if resp["message"] != "could not create refresh token" {
		t.Fatalf("expected 'could not create refresh token' error message, got %v", resp["message"])
	}

	// 1. No user triples remain
	var userTriplesCount int
	err = db.Pool.QueryRow(ctx, "SELECT count(*) FROM triples WHERE app_id = $1 AND value = to_jsonb($2::text)", appID, email).Scan(&userTriplesCount)
	if err != nil {
		t.Fatal(err)
	}
	if userTriplesCount != 0 {
		t.Fatalf("expected 0 user triples, got %d", userTriplesCount)
	}
	var userAttrCount int
	err = db.Pool.QueryRow(ctx, "SELECT count(*) FROM attrs WHERE app_id = $1 AND etype = '$users'", appID).Scan(&userAttrCount)
	if err != nil {
		t.Fatal(err)
	}
	if userAttrCount != 0 {
		t.Fatalf("expected user attrs to roll back, got %d", userAttrCount)
	}

	// 2. No transaction journal row remains
	var txCountAfter int
	err = db.Pool.QueryRow(ctx, "SELECT count(*) FROM transactions WHERE app_id = $1", appID).Scan(&txCountAfter)
	if err != nil {
		t.Fatal(err)
	}
	if txCountAfter != txCountBefore {
		t.Fatalf("expected transaction count unchanged (%d == %d)", txCountAfter, txCountBefore)
	}

	// 3. No post-commit callback fired
	if postCommitFired {
		t.Fatalf("expected no post-commit callback to fire on failure")
	}
	if faultCalls != 1 {
		t.Fatalf("non-unique failure retried %d times, want exactly once", faultCalls)
	}
}

func TestAdminConcurrentSameEmailFindOrCreate(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	appStr := uuidStr(appID)
	ctx := context.Background()

	email := "concurrent-test@example.test"
	concurrency := 6
	var wg sync.WaitGroup
	wg.Add(concurrency)

	type outcome struct {
		code int
		resp map[string]any
	}
	results := make([]outcome, concurrency)

	for i := 0; i < concurrency; i++ {
		idx := i
		go func() {
			defer wg.Done()
			code, resp := do(t, h, "POST", "/admin/refresh_tokens", authHeaders(token),
				map[string]any{
					"app-id": appStr,
					"email":  email,
				})
			results[idx] = outcome{code: code, resp: resp}
		}()
	}
	wg.Wait()

	var sharedUserID string
	for i, o := range results {
		if o.code != 200 {
			t.Fatalf("concurrent worker %d failed with code %d: %v", i, o.code, o.resp)
		}
		uMap, ok := o.resp["user"].(map[string]any)
		if !ok {
			t.Fatalf("worker %d missing user object: %v", i, o.resp)
		}
		uid, _ := uMap["id"].(string)
		if uid == "" {
			t.Fatalf("worker %d empty user id: %v", i, o.resp)
		}
		if sharedUserID == "" {
			sharedUserID = uid
		} else if uid != sharedUserID {
			t.Fatalf("expected all workers to resolve same user ID %q, got %q", sharedUserID, uid)
		}

		rt, _ := uMap["refresh_token"].(string)
		if rt == "" {
			t.Fatalf("worker %d empty refresh token", i)
		}
		// Verify each token successfully resolves the shared user
		var parsedAppID [16]byte
		_ = platform.ScanUUID(appStr, &parsedAppID)
		svc := &authn.Service{DB: h.DB, Pool: h.Pool, Catalogs: h.Catalogs}
		user, err := svc.VerifyRefreshToken(ctx, parsedAppID, rt)
		if err != nil {
			t.Fatalf("worker %d token verification failed: %v", i, err)
		}
		if user.Email != email {
			t.Fatalf("worker %d token resolved wrong email %s", i, user.Email)
		}
	}

	// Verify only 1 user exists with this email in the database
	var userCount int
	err := db.Pool.QueryRow(ctx, "SELECT count(DISTINCT entity_id) FROM triples WHERE app_id = $1 AND value = to_jsonb($2::text)", appID, email).Scan(&userCount)
	if err != nil {
		t.Fatal(err)
	}
	if userCount != 1 {
		t.Fatalf("expected exactly 1 user entity created, got %d", userCount)
	}
}
