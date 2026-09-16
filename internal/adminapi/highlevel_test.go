package adminapi_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/instant-v2/instant-v2/internal/adminapi"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// The four Phase-6 hardening acceptance scenarios for the high-level
// admin tx-step lowering (instant.admin.model port). Each drives real HTTP
// requests through Handler.ServeHTTP against Postgres.

// transact issues one /admin/transact call with extra body keys merged in.
func transactHTTP(t *testing.T, h *adminapi.Handler, appID [16]byte, token string, extra map[string]any, steps []any) (int, map[string]any) {
	t.Helper()
	m := map[string]any{"steps": steps}
	for k, v := range extra {
		m[k] = v
	}
	return do(t, h, "POST", "/admin/transact", authHeaders(token), bodyApp(appID, m))
}

// queryTodos runs {"todos": {}} through /admin/query and returns the list.
func queryTodos(t *testing.T, h *adminapi.Handler, appID [16]byte, token string) []any {
	t.Helper()
	code, resp := do(t, h, "POST", "/admin/query", authHeaders(token),
		bodyApp(appID, map[string]any{"query": map[string]any{"todos": map[string]any{}}}))
	if code != 200 {
		t.Fatalf("query: %d %v", code, resp)
	}
	list, _ := resp["todos"].([]any)
	return list
}

// TestHighLevelUpdateCreate proves an update op against an empty schema:
// it creates every missing attr, creates the entity, and follow-up updates
// merge without clobbering untouched fields.
func TestHighLevelUpdateCreate(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()

	eid := newUUID()
	code, resp := transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "todos", uuidStr(eid),
			map[string]any{"text": "hi", "done": false, "createdAt": 1720000000000}},
	})
	if code != 200 {
		t.Fatalf("transact: %d %v", code, resp)
	}
	if _, ok := resp["tx-id"]; !ok {
		t.Fatalf("missing tx-id: %v", resp)
	}

	list := queryTodos(t, h, appID, token)
	if len(list) != 1 {
		t.Fatalf("want 1 todo, got %v", list)
	}
	e := list[0].(map[string]any)
	if e["text"] != "hi" || e["done"] != false || e["createdAt"] != float64(1720000000000) {
		t.Fatalf("unexpected fields: %v", e)
	}
	if e["id"] != uuidStr(eid) {
		t.Fatalf("id mismatch: %v vs %s", e["id"], uuidStr(eid))
	}

	// Follow-up update touches only `done`; text must survive.
	code, resp = transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "todos", uuidStr(eid), map[string]any{"done": true}},
	})
	if code != 200 {
		t.Fatalf("second transact: %d %v", code, resp)
	}
	list = queryTodos(t, h, appID, token)
	if len(list) != 1 {
		t.Fatalf("want still 1 todo, got %v", list)
	}
	e = list[0].(map[string]any)
	if e["text"] != "hi" || e["done"] != true {
		t.Fatalf("merge clobbered fields: %v", e)
	}
}

// TestLookupRefEid proves encoded lookups ("lookup__<attr>__<json>") resolve
// to ONE entity across calls and that delete-by-lookup removes it.
func TestLookupRefEid(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()

	for i := 0; i < 2; i++ {
		text := "x"
		if i == 1 {
			text = "y"
		}
		code, resp := transactHTTP(t, h, appID, token, nil, []any{
			[]any{"update", "todos", `lookup__slug__"abc"`, map[string]any{"text": text}},
		})
		if code != 200 {
			t.Fatalf("transact %d: %d %v", i, code, resp)
		}
	}

	list := queryTodos(t, h, appID, token)
	if len(list) != 1 {
		t.Fatalf("lookups must collapse to one entity, got %v", list)
	}
	e := list[0].(map[string]any)
	if e["slug"] != "abc" || e["text"] != "y" {
		t.Fatalf("entity wrong after two updates: %v", e)
	}

	code, resp := transactHTTP(t, h, appID, token, nil, []any{
		[]any{"delete", "todos", `lookup__slug__"abc"`},
	})
	if code != 200 {
		t.Fatalf("delete: %d %v", code, resp)
	}
	if list = queryTodos(t, h, appID, token); len(list) != 0 {
		t.Fatalf("delete-by-lookup left entities: %v", list)
	}
}

// TestLinkUnlinkDeleteMerge links two entities through a ref attr created on
// demand, unlinks them, deep-merges an object attr, and deletes via the
// high-level delete op.
func TestLinkUnlinkDeleteMerge(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	todoEID, commentEID := newUUID(), newUUID()
	code, resp := transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "todos", uuidStr(todoEID), map[string]any{"text": "a"}},
		[]any{"update", "comments", uuidStr(commentEID), map[string]any{"text": "b"}},
		[]any{"link", "comments", uuidStr(commentEID), map[string]any{"todo": uuidStr(todoEID)}},
	})
	if code != 200 {
		t.Fatalf("create+link: %d %v", code, resp)
	}

	// Ref attr created on demand; forward triple stores the todo uuid.
	cat, err := h.Catalogs.For(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	refAttr := cat.FindByEtypeLabel("comments", "todo")
	if refAttr == nil {
		t.Fatal("comments.todo ref attr was not created")
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{commentEID}, AttrIDs: [][16]byte{refAttr.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0].Triple.V) != uuidStr(todoEID) {
		t.Fatalf("link triple missing/wrong: %+v", rows)
	}

	// Unlink removes it.
	code, resp = transactHTTP(t, h, appID, token, nil, []any{
		[]any{"unlink", "comments", uuidStr(commentEID), map[string]any{"todo": uuidStr(todoEID)}},
	})
	if code != 200 {
		t.Fatalf("unlink: %d %v", code, resp)
	}
	rows, _ = db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{commentEID}, AttrIDs: [][16]byte{refAttr.ID}})
	if len(rows) != 0 {
		t.Fatalf("unlink left the link triple: %+v", rows)
	}

	// Deep-merge: partial object merge preserves sibling keys.
	code, resp = transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "comments", uuidStr(commentEID),
			map[string]any{"meta": map[string]any{"a": 1}}},
		[]any{"merge", "comments", uuidStr(commentEID),
			map[string]any{"meta": map[string]any{"b": 2}}},
	})
	if code != 200 {
		t.Fatalf("merge: %d %v", code, resp)
	}
	cat, err = h.Catalogs.For(ctx, uuidStr(appID)) // reloaded: meta was just provisioned
	if err != nil {
		t.Fatal(err)
	}
	metaAttr := cat.FindByEtypeLabel("comments", "meta")
	if metaAttr == nil {
		t.Fatal("comments.meta attr was not created")
	}
	rows, _ = db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{commentEID}, AttrIDs: [][16]byte{metaAttr.ID}})
	if len(rows) != 1 {
		t.Fatalf("meta triple missing: %+v", rows)
	}
	mm, ok := rows[0].Triple.V.(map[string]any)
	// FetchTriples decodes nested numbers as json.Number (storage contract);
	// compare textually.
	if !ok || fmt.Sprint(mm["a"]) != "1" || fmt.Sprint(mm["b"]) != "2" {
		t.Fatalf("deep merge wrong: %#v", rows[0].Triple.V)
	}

	// High-level delete removes the whole entity.
	code, resp = transactHTTP(t, h, appID, token, nil, []any{
		[]any{"delete", "todos", uuidStr(todoEID)},
	})
	if code != 200 {
		t.Fatalf("delete: %d %v", code, resp)
	}
	rows, _ = db.FetchTriples(ctx, appID, storage.FetchFilter{
		EntityIDs: [][16]byte{todoEID}})
	if len(rows) != 0 {
		t.Fatalf("delete left triples: %+v", rows)
	}
}

// TestThrowOnMissingAttrs proves throw-on-missing-attrs?=true rejects unknown
// attributes with v1's message shape and creates nothing, while the default
// (falsy) path still creates attrs on demand.
func TestThrowOnMissingAttrs(t *testing.T) {
	h, _, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()

	code, resp := transactHTTP(t, h, appID, token,
		map[string]any{"throw-on-missing-attrs?": true},
		[]any{[]any{"update", "todos", uuidStr(newUUID()), map[string]any{"ghost": "v"}}})
	if code != 400 {
		t.Fatalf("want 400, got %d %v", code, resp)
	}
	if msg, _ := resp["message"].(string); msg != "Validation failed for steps: Attributes are missing in your schema" {
		t.Fatalf("message mismatch: %q", msg)
	}
	cat, err := h.Catalogs.For(ctx, uuidStr(appID))
	if err != nil {
		t.Fatal(err)
	}
	if cat.Len() != 0 {
		t.Fatalf("throw-on-missing must not create attrs, catalog has %d", cat.Len())
	}

	// Default (falsy) still creates on demand.
	code, resp = transactHTTP(t, h, appID, token, nil,
		[]any{[]any{"update", "todos", uuidStr(newUUID()), map[string]any{"ghost": "v"}}})
	if code != 200 {
		t.Fatalf("default path should create attrs: %d %v", code, resp)
	}
}

func TestHighLevelProvisioningMutationFailureIsAtomic(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()
	ctx := context.Background()
	appStr := uuidStr(appID)

	snapshot := func(query string) []string {
		t.Helper()
		rows, err := db.Pool.Query(ctx, query, appID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				t.Fatal(err)
			}
			out = append(out, value)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}

	attrsBefore := snapshot(`
		SELECT id::text || '|' || coalesce(etype, '') || '|' || coalesce(label, '') || '|' ||
		       value_type || '|' || cardinality || '|' || is_unique::text || '|' || is_indexed::text
		  FROM attrs WHERE app_id=$1 ORDER BY id`)
	identsBefore := snapshot(`
		SELECT id::text || '|' || attr_id::text || '|' || etype || '|' || label
		  FROM idents WHERE app_id=$1 ORDER BY id`)
	triplesBefore := snapshot(`
		SELECT entity_id::text || '|' || attr_id::text || '|' || coalesce(value::text, 'null')
		  FROM triples WHERE app_id=$1 ORDER BY entity_id, attr_id, value_md5`)
	journalBefore := snapshot(`
		SELECT id::text FROM transactions WHERE app_id=$1 ORDER BY id`)
	catBefore, err := h.Catalogs.For(ctx, appStr)
	if err != nil {
		t.Fatal(err)
	}

	var commitCalls int
	h.OnCommit = func(context.Context, [16]byte, []string, int64, bool) {
		commitCalls++
	}

	missingAttr := "ghost"
	unknownAttr := uuidStr(newUUID())
	code, resp := transactHTTP(t, h, appID, token, nil, []any{
		[]any{"update", "todos", uuidStr(newUUID()), map[string]any{missingAttr: "value"}},
		[]any{"add-triple", uuidStr(newUUID()), unknownAttr, "must fail"},
	})
	if code != 400 {
		t.Fatalf("expected failed mutation, got %d %v", code, resp)
	}
	if _, ok := resp["tx-id"]; ok {
		t.Fatalf("failed mutation returned tx-id: %v", resp)
	}

	attrsAfter := snapshot(`
		SELECT id::text || '|' || coalesce(etype, '') || '|' || coalesce(label, '') || '|' ||
		       value_type || '|' || cardinality || '|' || is_unique::text || '|' || is_indexed::text
		  FROM attrs WHERE app_id=$1 ORDER BY id`)
	identsAfter := snapshot(`
		SELECT id::text || '|' || attr_id::text || '|' || etype || '|' || label
		  FROM idents WHERE app_id=$1 ORDER BY id`)
	triplesAfter := snapshot(`
		SELECT entity_id::text || '|' || attr_id::text || '|' || coalesce(value::text, 'null')
		  FROM triples WHERE app_id=$1 ORDER BY entity_id, attr_id, value_md5`)
	journalAfter := snapshot(`
		SELECT id::text FROM transactions WHERE app_id=$1 ORDER BY id`)
	if !reflect.DeepEqual(attrsAfter, attrsBefore) ||
		!reflect.DeepEqual(identsAfter, identsBefore) ||
		!reflect.DeepEqual(triplesAfter, triplesBefore) ||
		!reflect.DeepEqual(journalAfter, journalBefore) {
		t.Fatalf("failed mutation left DB state:\nattrs: %v -> %v\nidents: %v -> %v\ntriples: %v -> %v\njournal: %v -> %v",
			attrsBefore, attrsAfter, identsBefore, identsAfter, triplesBefore, triplesAfter, journalBefore, journalAfter)
	}
	catAfter, err := h.Catalogs.For(ctx, appStr)
	if err != nil {
		t.Fatal(err)
	}
	if catAfter != catBefore || catAfter.Len() != catBefore.Len() {
		t.Fatalf("failed mutation changed cached schema: before=%p/%d after=%p/%d", catBefore, catBefore.Len(), catAfter, catAfter.Len())
	}
	if commitCalls != 0 {
		t.Fatalf("failed mutation fired %d post-commit callbacks", commitCalls)
	}
}

func TestHighLevelConcurrentMissingAttrProvisioningConverges(t *testing.T) {
	h, db, appID, token, cleanup := env(t)
	defer cleanup()

	type outcome struct {
		code int
		resp map[string]any
	}
	start := make(chan struct{})
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			code, resp := transactHTTP(t, h, appID, token, nil, []any{
				[]any{"update", "todos", uuidStr(newUUID()), map[string]any{"shared": i}},
			})
			results <- outcome{code: code, resp: resp}
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	for result := range results {
		if result.code != 200 {
			t.Fatalf("concurrent provisioning failed: %d %v", result.code, result.resp)
		}
	}

	var attrs, triples int
	if err := db.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM attrs WHERE app_id=$1 AND etype='todos' AND label='shared'`, appID,
	).Scan(&attrs); err != nil {
		t.Fatal(err)
	}
	if err := db.Pool.QueryRow(context.Background(), `
		SELECT count(*)
		  FROM triples t JOIN attrs a ON a.id=t.attr_id AND a.app_id=t.app_id
		 WHERE t.app_id=$1 AND a.etype='todos' AND a.label='shared'`, appID,
	).Scan(&triples); err != nil {
		t.Fatal(err)
	}
	if attrs != 1 || triples != 2 {
		t.Fatalf("concurrent provisioning did not converge: attrs=%d triples=%d", attrs, triples)
	}
}
