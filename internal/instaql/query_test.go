package instaql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/triple"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func qdb(t *testing.T) *storage.DB {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping live query tests")
	}
	ctx := context.Background()
	pool, err := newPool(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `DROP SCHEMA public CASCADE; CREATE SCHEMA public;`); err != nil {
		t.Fatalf("reset: %v", err)
	}
	sdb, _ := sql.Open("pgx", dsn)
	defer sdb.Close()
	if err := platform.Migrate(ctx, sdb); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return storage.New(pool)
}

func qseed(t *testing.T, db *storage.DB) ([16]byte, *platform.AttrCatalog, qids) {
	t.Helper()
	ctx := context.Background()
	appID := rand16()
	err := db.WithTx(ctx, func(tx pgx.Tx) error {
		creator := rand16()
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creator, "q@test"); err != nil {
			return err
		}
		return platform.CreateApp(ctx, tx, creator, appID, "q")
	})
	if err != nil {
		t.Fatal(err)
	}
	var (
		postTitle   platform.Attr
		postViews   platform.Attr
		postTag     platform.Attr // blob many
		postAuthor  platform.Attr // ref one → users
		userName    platform.Attr
		commentTxt  platform.Attr
		commentPost platform.Attr // ref many (comments on post)
	)
	err = db.WithTx(ctx, func(tx pgx.Tx) error {
		var e1, e2, e3, e4, e5, e6, e7 error
		postTitle, e1 = mustAttr(t, tx, appID, "posts", "title", "blob", "one", false, true)
		postViews, e2 = mustAttr(t, tx, appID, "posts", "views", "number", "one", false, true)
		postTag, e3 = mustAttr(t, tx, appID, "posts", "tags", "blob", "many", false, false)
		postAuthor, e4 = mustAttr(t, tx, appID, "posts", "author", "ref", "one", false, false)
		userName, e5 = mustAttr(t, tx, appID, "users", "name", "blob", "one", false, true)
		commentTxt, e6 = mustAttr(t, tx, appID, "comments", "text", "blob", "one", false, true)
		commentPost, e7 = platform.GetOrCreateAttrRev(context.Background(), tx, appID,
			"comments", "post", strptr("posts"), strptr("comments"),
			"ref", "many", false, false)
		for _, e := range []error{e1, e2, e3, e4, e5, e6, e7} {
			if e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	return appID, cat, qids{
		title: postTitle.ID, views: postViews.ID, tag: postTag.ID,
		author: postAuthor.ID, uname: userName.ID,
		ctext: commentTxt.ID, cpost: commentPost.ID,
	}
}

type qids struct {
	title, views, tag, author, uname, ctext, cpost [16]byte
}

func TestQuerySimpleAndWhereOps(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)

	p1, p2, p3 := rand16(), rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: p1, A: ids.title, V: "alpha"}, {E: p1, A: ids.views, V: int64(10)},
		{E: p2, A: ids.title, V: "beta"}, {E: p2, A: ids.views, V: int64(20)},
		{E: p3, A: ids.title, V: "alphabet"}, {E: p3, A: ids.views, V: int64(30)},
	}, false); err != nil {
		t.Fatal(err)
	}

	ex := &instaql.Executor{DB: db.Pool}
	countTitles := func(where map[string]any) int {
		raw := map[string]any{"posts": map[string]any{"$": map[string]any{"where": where}}}
		q, _ := instaql.Coerce(raw)
		res, err := ex.Run(ctx, q, cat, appID)
		if err != nil {
			t.Fatalf("run %+v: %v", where, err)
		}
		var arr []map[string]any
		if string(res.Data["posts"]) == "null" || len(res.Data["posts"]) == 0 {
			return 0
		}
		if err := json.Unmarshal(res.Data["posts"], &arr); err != nil {
			t.Fatalf("unmarshal %s: %v", res.Data["posts"], err)
		}
		return len(arr)
	}

	if got := countTitles(map[string]any{"title": "beta"}); got != 1 {
		t.Fatalf("$eq: %d want 1", got)
	}
	if got := countTitles(map[string]any{"views": map[string]any{"$gt": 15}}); got != 2 {
		t.Fatalf("$gt: %d want 2", got)
	}
	if got := countTitles(map[string]any{"views": map[string]any{"$gte": 20}}); got != 2 {
		t.Fatalf("$gte: %d want 2", got)
	}
	if got := countTitles(map[string]any{"title": map[string]any{"$in": []any{"alpha", "beta"}}}); got != 2 {
		t.Fatalf("$in: %d want 2", got)
	}
	if got := countTitles(map[string]any{"title": map[string]any{"$not": "alpha"}}); got != 2 {
		t.Fatalf("$not: %d want 2", got)
	}
	if got := countTitles(map[string]any{"title": map[string]any{"$like": "alpha%"}}); got != 2 {
		t.Fatalf("$like: %d want 2", got)
	}
	if got := countTitles(map[string]any{"title": map[string]any{"$ilike": "ALPHA%"}}); got != 2 {
		t.Fatalf("$ilike: %d want 2", got)
	}
	if got := countTitles(map[string]any{"missing": map[string]any{"$isNull": true}}); got != 3 {
		t.Fatalf("$isNull true on absent attr: %d want 3 (all entities lack it)", got)
	}
}

func TestQueryNestedJoin(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)

	post, user, c1, c2 := rand16(), rand16(), rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: post, A: ids.title, V: "hello"},
		{E: post, A: ids.author, V: uuidStr(user)},
		{E: user, A: ids.uname, V: "ada"},
		{E: c1, A: ids.ctext, V: "first!"},
		{E: c1, A: ids.cpost, V: uuidStr(post)},
		{E: c2, A: ids.ctext, V: "second"},
		{E: c2, A: ids.cpost, V: uuidStr(post)},
	}, false); err != nil {
		t.Fatal(err)
	}

	ex := &instaql.Executor{DB: db.Pool}
	q, err := instaql.Coerce(map[string]any{
		"posts": map[string]any{"comments": map[string]any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := ex.Run(ctx, q, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	var posts []map[string]any
	if err := json.Unmarshal(res.Data["posts"], &posts); err != nil {
		t.Fatal(err)
	}
	if len(posts) != 1 {
		t.Fatalf("posts %d want 1", len(posts))
	}
	kids, ok := posts[0]["comments"].([]any)
	if !ok || len(kids) != 2 {
		t.Fatalf("nested comments missing: %v", posts[0])
	}
	var comments []map[string]any
	if err := json.Unmarshal(res.Data["comments"], &comments); err != nil {
		t.Fatal(err)
	}
	if len(comments) != 2 {
		t.Fatalf("top-level comments %d want 2", len(comments))
	}
}

func TestQueryPaginationAndAggregate(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)

	var triples []triple.Triple
	for i := range 5 {
		e := rand16()
		triples = append(triples, triple.Triple{E: e, A: ids.title, V: fmt.Sprintf("p%d", i)})
	}
	if _, err := db.InsertTriples(ctx, appID, cat, triples, false); err != nil {
		t.Fatal(err)
	}

	ex := &instaql.Executor{DB: db.Pool}
	limit := 2
	offset := 1
	q, err := instaql.Coerce(map[string]any{
		"posts": map[string]any{"$": map[string]any{"limit": float64(limit), "offset": float64(offset), "aggregate": "count"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := ex.Run(ctx, q, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	var posts []map[string]any
	_ = json.Unmarshal(res.Data["posts"], &posts)
	if len(posts) != 2 {
		t.Fatalf("page size %d want 2", len(posts))
	}
	if res.Aggregate == nil || res.Aggregate.Count != 5 {
		t.Fatalf("aggregate count: %+v want 5", res.Aggregate)
	}
	if res.PageInfo == nil || !res.PageInfo.HasNextPage {
		t.Fatalf("page-info: %+v", res.PageInfo)
	}
}

func TestCoerceRejectsUnknowns(t *testing.T) {
	for _, raw := range []map[string]any{
		{"posts": map[string]any{"$": map[string]any{"bogus": 1}}},
		{"posts": map[string]any{"$": map[string]any{"where": map[string]any{"x": map[string]any{"$bogus": 1}}}}},
		{"posts": map[string]any{"$": map[string]any{"limit": -1}}},
		{"posts": map[string]any{"$": map[string]any{"aggregate": "sum"}}},
	} {
		if _, err := instaql.Coerce(raw); err == nil {
			t.Fatalf("expected rejection for %v", raw)
		}
	}
}

func strptr(s string) *string { return &s }

// TestViewGateRules pins the read-side permission gate: closed view rules
// render empty results, dynamic rules are refused for non-admin callers
// (fail-closed until rule-where pushdown exists), admins bypass, and a nil
// doc keeps v1 default-open behavior.
func TestViewGateRules(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)
	st := storage.New(db.Pool)

	e := rand16()
	if _, err := st.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: e, A: ids.title, V: "visible-ish"},
	}, false); err != nil {
		t.Fatal(err)
	}
	run := func(doc *perms.RuleDoc, admin bool) (map[string]json.RawMessage, error) {
		q, err := instaql.Coerce(map[string]any{"posts": map[string]any{}})
		if err != nil {
			t.Fatal(err)
		}
		ex := &instaql.Executor{DB: db.Pool, Rules: doc, Admin: admin}
		res, rerr := ex.Run(ctx, q, cat, appID)
		if rerr != nil {
			return nil, rerr
		}
		return res.Data, nil
	}

	closed, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"false"}}}`))
	data, err := run(closed, false)
	if err != nil {
		t.Fatalf("closed view should not error: %v", err)
	}
	if string(data["posts"]) != "[]" {
		t.Fatalf("closed view must return [], got: %s", data["posts"])
	}

	open, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"true"}}}`))
	data, err = run(open, false)
	if err != nil {
		t.Fatalf("open view errored: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(data["posts"], &rows); err != nil || len(rows) != 1 {
		t.Fatalf("open view lost data: %s (%v)", data["posts"], err)
	}

	dyn, _ := perms.ParseRuleDoc([]byte(`{"posts":{"allow":{"view":"auth.id == data.ownerId"}}}`))
	_, err = run(dyn, false)
	var unsupported *instaql.ErrRuleFilterUnsupported
	if !errors.As(err, &unsupported) || unsupported.Etype != "posts" {
		t.Fatalf("expected ErrRuleFilterUnsupported for dynamic rule, got: %v", err)
	}
	if _, err = run(dyn, true); err != nil {
		t.Fatalf("admin bypass failed: %v", err)
	}

	data, err = run(nil, false)
	if err != nil {
		t.Fatalf("nil rules must stay open: %v", err)
	}
	if !strings.Contains(string(data["posts"]), "visible-ish") {
		t.Fatalf("nil-rule query lost data: %s", data["posts"])
	}
}

// TestRangePredicatesAreTyped pins type-aware range comparison: numbers
// compare numerically ('10' > '9' must hold — pure text extraction ordered
// it below), strings compare lexically, and cross-type bounds match nothing
// instead of falling back to text ordering.
func TestRangePredicatesAreTyped(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)

	p1, p2, p3 := rand16(), rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: p1, A: ids.title, V: "alpha"}, {E: p1, A: ids.views, V: int64(5)},
		{E: p2, A: ids.title, V: "beta"}, {E: p2, A: ids.views, V: int64(10)},
		{E: p3, A: ids.title, V: "alphabet"}, {E: p3, A: ids.views, V: int64(100)},
	}, false); err != nil {
		t.Fatal(err)
	}

	ex := &instaql.Executor{DB: db.Pool}
	countWhere := func(attr string, where map[string]any) int {
		raw := map[string]any{"posts": map[string]any{"$": map[string]any{"where": map[string]any{attr: where}}}}
		q, _ := instaql.Coerce(raw)
		res, err := ex.Run(ctx, q, cat, appID)
		if err != nil {
			t.Fatalf("run %+v: %v", where, err)
		}
		var arr []map[string]any
		if string(res.Data["posts"]) == "null" || len(res.Data["posts"]) == 0 {
			return 0
		}
		if err := json.Unmarshal(res.Data["posts"], &arr); err != nil {
			t.Fatalf("unmarshal %s: %v", res.Data["posts"], err)
		}
		return len(arr)
	}

	// Numeric: lexical ordering would return 0 ('100','10','5' all < '9').
	if got := countWhere("views", map[string]any{"$gt": 9}); got != 2 {
		t.Fatalf("$gt 9 numeric: %d want 2 (lexical regression returns 0)", got)
	}
	if got := countWhere("views", map[string]any{"$lte": 10}); got != 2 {
		t.Fatalf("$lte 10 numeric: %d want 2", got)
	}
	// String ranges still work lexically.
	if got := countWhere("title", map[string]any{"$gte": "beta"}); got != 1 {
		t.Fatalf("$gte beta string: %d want 1", got)
	}
	// Cross-type bound excludes instead of comparing as text.
	if got := countWhere("views", map[string]any{"$gt": "x"}); got != 0 {
		t.Fatalf("$gt cross-type: %d want 0", got)
	}
}

// TestLimitOffsetPagesAreDisjointAndCovering pins end-to-end paging: with a
// deterministic order, fixed-size offset pages partition the match set.
func TestLimitOffsetPagesAreDisjointAndCovering(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)

	ents := make([]triple.Triple, 0, 12)
	var wantIDs []string
	for i := 0; i < 6; i++ {
		e := rand16()
		wantIDs = append(wantIDs, uuidStr(e))
		ents = append(ents, triple.Triple{E: e, A: ids.title, V: fmt.Sprintf("t%d", i)})
	}
	if _, err := db.InsertTriples(ctx, appID, cat, ents, false); err != nil {
		t.Fatal(err)
	}

	ex := &instaql.Executor{DB: db.Pool}
	pageIDs := func(offset int) []string {
		raw := map[string]any{"posts": map[string]any{"$": map[string]any{
			"limit": float64(2), "offset": float64(offset),
		}}}
		q, _ := instaql.Coerce(raw)
		res, err := ex.Run(ctx, q, cat, appID)
		if err != nil {
			t.Fatalf("run page %d: %v", offset, err)
		}
		var arr []map[string]any
		if err := json.Unmarshal(res.Data["posts"], &arr); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		out := make([]string, 0, len(arr))
		for _, item := range arr {
			id, _ := item["id"].(string)
			out = append(out, id)
		}
		return out
	}

	seen := map[string]bool{}
	for off := 0; off < 6; off += 2 {
		page := pageIDs(off)
		if len(page) != 2 {
			t.Fatalf("page@%d returned %d rows, want 2", off, len(page))
		}
		for _, id := range page {
			if seen[id] {
				t.Fatalf("page@%d repeated entity %s — pages overlap", off, id)
			}
			seen[id] = true
		}
	}
	if len(seen) != 6 {
		t.Fatalf("pages covered %d entities, want 6 (skipped rows?)", len(seen))
	}
	_ = wantIDs
}

// Audit L1: InstaQL nesting is depth-capped so one query can't drive
// hundreds of sequential round-trips.
func TestCoerceRejectsExcessiveDepth(t *testing.T) {
	deep := map[string]any{"todos": map[string]any{}}
	cur := deep["todos"].(map[string]any)
	for i := 0; i < 25; i++ {
		next := map[string]any{}
		cur["$nest"] = next // any child key deepens the form
		cur = next
	}
	if _, err := instaql.Coerce(deep); err == nil {
		t.Fatal("over-deep nesting must be rejected")
	}
}
