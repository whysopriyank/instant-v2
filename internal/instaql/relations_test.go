package instaql_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/triple"
)

func TestForwardAliasProjectionAndRootIsolation(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, cat, ids := qseed(t, db)
	post, empty, user, unrelated := rand16(), rand16(), rand16(), rand16()
	if _, err := db.InsertTriples(ctx, appID, cat, []triple.Triple{
		{E: post, A: ids.title, V: "linked"}, {E: post, A: ids.author, V: uuidStr(user)},
		{E: empty, A: ids.title, V: "empty"}, {E: user, A: ids.uname, V: "author"},
		{E: unrelated, A: ids.uname, V: "unrelated"},
	}, false); err != nil {
		t.Fatal(err)
	}
	query, err := instaql.Coerce(map[string]any{
		"posts": map[string]any{"$": map[string]any{"fields": []any{"title"}}, "author": map[string]any{}},
		"users": map[string]any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, reverse := range []bool{false, true} {
		if reverse {
			query.Forms[0], query.Forms[1] = query.Forms[1], query.Forms[0]
		}
		result, err := (&instaql.Executor{DB: db.Pool}).Run(ctx, query, cat, appID)
		if err != nil {
			t.Fatal(err)
		}
		var users, posts []map[string]any
		if err := json.Unmarshal(result.Data["users"], &users); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(result.Data["posts"], &posts); err != nil {
			t.Fatal(err)
		}
		if len(users) != 2 || len(posts) != 2 {
			t.Fatalf("root results overwritten: %s / %s", result.Data["users"], result.Data["posts"])
		}
		for _, row := range posts {
			children, ok := row["author"].([]any)
			if !ok {
				t.Fatalf("author must be array: %v", row)
			}
			if row["id"] == uuidStr(post) {
				if len(children) != 1 || children[0].(map[string]any)["id"] != uuidStr(user) {
					t.Fatalf("linked projection: %v", row)
				}
			} else if len(children) != 0 {
				t.Fatalf("unlinked post exposed children: %v", row)
			}
		}
	}
	closed, _ := perms.ParseRuleDoc([]byte(`{"users":{"allow":{"view":"false"}}}`))
	result, err := (&instaql.Executor{DB: db.Pool, Rules: closed}).Run(ctx, query, cat, appID)
	if err != nil {
		t.Fatal(err)
	}
	var posts []map[string]any
	if err := json.Unmarshal(result.Data["posts"], &posts); err != nil {
		t.Fatal(err)
	}
	for _, row := range posts {
		if children, ok := row["author"].([]any); !ok || len(children) != 0 {
			t.Fatalf("denied target leaked: %v", row)
		}
	}
	// Query the alias without the root: the gate must use users, not author.
	nested, err := instaql.Coerce(map[string]any{"posts": map[string]any{"author": map[string]any{}}})
	if err != nil {
		t.Fatal(err)
	}
	dynamic, _ := perms.ParseRuleDoc([]byte(`{"users":{"allow":{"view":"auth.id == data.id"}}}`))
	_, err = (&instaql.Executor{DB: db.Pool, Rules: dynamic}).Run(ctx, nested, cat, appID)
	var unsupported *instaql.ErrRuleFilterUnsupported
	if !errors.As(err, &unsupported) || unsupported.Etype != "users" {
		t.Fatalf("target gate: %v", err)
	}
}
