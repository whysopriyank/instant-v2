package instaql_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
)

// TestQueryServerCreatedAtOrdersByIDTripleTimestamp pins the v1 meaning of
// serverCreatedAt: the creation timestamp of the implicit etype/id triple,
// with entity-id as the deterministic tie-breaker in the requested direction.
// It also proves that a cursor can advance after the cursor entity is removed
// by carrying the timestamp in the existing v1 cursor tuple.
func TestQueryServerCreatedAtOrdersByIDTripleTimestamp(t *testing.T) {
	db := qdb(t)
	ctx := context.Background()
	appID, _, ids := qseed(t, db)
	// qseed provisions only the fields needed by the general query tests. The
	// production high-level transactor always creates the implicit id attr, so
	// add that contract fixture explicitly here before testing its timestamp.
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		_, err := platform.GetOrCreateAttr(ctx, tx, appID, "posts", "id", "blob", "one", true, true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	idAttr := cat.FindByEtypeLabel("posts", "id")
	if idAttr == nil {
		t.Fatal("qseed missing posts/id")
	}

	e1 := [16]byte{3} // created first, but sorts after e2 by id
	e2 := [16]byte{1}
	e3 := [16]byte{2}
	e4 := [16]byte{4} // ties with e3; desc must reverse the id tie-break
	triples := []struct {
		eid [16]byte
		at  time.Time
	}{{e1, time.UnixMilli(1000)}, {e2, time.UnixMilli(2000)}, {e3, time.UnixMilli(3000)}, {e4, time.UnixMilli(3000)}}
	var input []triple.Triple
	for _, row := range triples {
		input = append(input,
			triple.Triple{E: row.eid, A: idAttr.ID, V: uuidStr(row.eid)},
			triple.Triple{E: row.eid, A: ids.title, V: uuidStr(row.eid)},
		)
	}
	if _, err := db.InsertTriples(ctx, appID, cat, input, false); err != nil {
		t.Fatal(err)
	}
	for _, row := range triples {
		if _, err := db.Pool.Exec(ctx,
			`UPDATE triples SET created_at=$1 WHERE app_id=$2 AND entity_id=$3 AND attr_id=$4`,
			row.at, appID, row.eid, idAttr.ID); err != nil {
			t.Fatal(err)
		}
	}

	ex := &instaql.Executor{DB: db.Pool}
	run := func(opts map[string]any) ([]map[string]any, *instaql.PageInfo, error) {
		q, err := instaql.Coerce(map[string]any{"posts": map[string]any{"$": opts}})
		if err != nil {
			return nil, nil, err
		}
		res, err := ex.Run(ctx, q, cat, appID)
		if err != nil {
			return nil, nil, err
		}
		var rows []map[string]any
		if err := json.Unmarshal(res.Data["posts"], &rows); err != nil {
			return nil, nil, err
		}
		return rows, res.PageInfo, nil
	}
	idsOf := func(rows []map[string]any) []string {
		out := make([]string, 0, len(rows))
		for _, row := range rows {
			out = append(out, row["id"].(string))
		}
		return out
	}

	asc := map[string]any{"order": map[string]any{"k": "serverCreatedAt", "direction": "asc"}}
	rows, info, err := run(asc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idsOf(rows), []string{uuidStr(e1), uuidStr(e2), uuidStr(e3), uuidStr(e4)}; !equalStrings(got, want) {
		t.Fatalf("serverCreatedAt asc = %v, want %v", got, want)
	}

	page := map[string]any{"order": map[string]any{"k": "serverCreatedAt", "direction": "asc"}, "limit": float64(2)}
	first, firstInfo, err := run(page)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idsOf(first), []string{uuidStr(e1), uuidStr(e2)}; !equalStrings(got, want) {
		t.Fatalf("serverCreatedAt first page = %v, want %v", got, want)
	}
	if firstInfo == nil || firstInfo.EndCursor == nil || info == nil {
		t.Fatalf("missing page info: %+v", firstInfo)
	}
	var cursor []any
	if err := json.Unmarshal([]byte(*firstInfo.EndCursor), &cursor); err != nil {
		t.Fatal(err)
	}
	if len(cursor) != 4 || cursor[0] != uuidStr(e2) || cursor[1] != idAttr.UUID() || cursor[2] != uuidStr(e2) {
		t.Fatalf("serverCreatedAt cursor = %v, want [eid,id-attr,eid,millis]", cursor)
	}
	if millis, ok := cursor[3].(float64); !ok || int64(millis) != triples[1].at.UnixMilli() {
		t.Fatalf("serverCreatedAt cursor timestamp = %v, want %d", cursor[3], triples[1].at.UnixMilli())
	}

	page["after"] = *firstInfo.EndCursor
	second, _, err := run(page)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idsOf(second), []string{uuidStr(e3), uuidStr(e4)}; !equalStrings(got, want) {
		t.Fatalf("serverCreatedAt cursor page = %v, want %v", got, want)
	}

	if _, err := db.Pool.Exec(ctx, `DELETE FROM triples WHERE app_id=$1 AND entity_id=$2`, appID, e2); err != nil {
		t.Fatal(err)
	}
	removedCursorPage, _, err := run(page)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idsOf(removedCursorPage), []string{uuidStr(e3), uuidStr(e4)}; !equalStrings(got, want) {
		t.Fatalf("serverCreatedAt removed-cursor page = %v, want %v", got, want)
	}

	desc := map[string]any{"order": map[string]any{"k": "serverCreatedAt", "direction": "desc"}}
	rows, _, err = run(desc)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := idsOf(rows), []string{uuidStr(e4), uuidStr(e3), uuidStr(e1)}; !equalStrings(got, want) {
		t.Fatalf("serverCreatedAt desc = %v, want %v", got, want)
	}
}

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
