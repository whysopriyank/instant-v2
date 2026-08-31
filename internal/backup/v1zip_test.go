package backup_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
	"github.com/instant-v2/instant-v2/internal/storage"
)

// TestV1DumpImport hand-crafts a v1-shaped export zip (config.json first,
// entities/<etype>.jsonl next — the layout restore.clj consumes) and asserts
// the importer materializes triples from it.
func TestV1DumpImport(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	e1, e2 := newUUID(), newUUID()
	configJSON := `{
	  "title": "legacy app",
	  "rules": {"fn": "true"},
	  "schema": {
	    "blobs": {
	      "todo": {
	        "text":     {"valueType": "string",  "config": {"unique": false, "indexed": true, "required": true}},
	        "priority": {"valueType": "number",  "config": {"unique": false, "indexed": false}},
	        "done":     {"valueType": "boolean", "config": {"unique": false, "indexed": false}}
	      }
	    },
	    "refs": {}
	  }
	}`
	line := func(id, text string, priority int, done bool) string {
		return fmt.Sprintf(`{"entity":{"id":%q,"text":%q,"priority":%d,"done":%t},"createdAt":1700000000000}`, id, text, priority, done)
	}
	entityData := strings.Join([]string{
		line(uuidStr(e1), "from v1 one", 1, true),
		line(uuidStr(e2), "from v1 two", 2, false),
	}, "\n")

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	wCfg, err := zw.Create("config.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wCfg.Write([]byte(configJSON)); err != nil {
		t.Fatal(err)
	}
	wEnt, err := zw.Create("entities/todo.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wEnt.Write([]byte(entityData)); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(zipBuf.Bytes())
	counts, err := backup.RestoreV1Zip(ctx, pool, reader, int64(reader.Len()), appID)
	if err != nil {
		t.Fatalf("RestoreV1Zip: %v", err)
	}
	// attrs: text/priority/done + implicit id; each entity also carries its id
	// triple, plus the rules row.
	if counts.Triples != 8 || counts.Attrs != 4 || counts.Rules != 1 {
		t.Fatalf("unexpected v1 import counts: %+v", counts)
	}
	var textRequired, idRequired bool
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='todo' AND label='text'`, appID).Scan(&textRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='todo' AND label='id'`, appID).Scan(&idRequired); err != nil {
		t.Fatal(err)
	}
	if !textRequired || !idRequired {
		t.Fatalf("v1 requiredness not preserved: text=%v id=%v", textRequired, idRequired)
	}

	st := storage.New(pool)
	got, err := st.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{e1, e2}})
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]bool{}
	for _, g := range got {
		switch v := g.Triple.V.(type) {
		case string:
			values[v] = true
		case json.Number:
			values["num:"+v.String()] = true
		case bool:
			values[fmt.Sprintf("bool:%v", v)] = true
		}
	}
	for _, want := range []string{"from v1 one", "from v1 two", "num:1", "num:2", "bool:true", "bool:false"} {
		if !values[want] {
			t.Fatalf("missing triple value %q; got %v", want, values)
		}
	}
}

// TestV1EntitiesLinksImport covers the current v1 config shape. V1 has also
// emitted the older schema.blobs/schema.refs shape, which TestV1DumpImport
// above keeps covered for compatibility.
func TestV1EntitiesLinksImport(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()

	postID, userID := newUUID(), newUUID()
	configJSON := `{
      "title":"current v1",
      "schema": {
		  "entities": {
		    "posts": {"attrs": {"title": {"valueType":"any", "config":{"unique":false,"indexed":true,"required":true}}}},
          "users": {"attrs": {}}
        },
        "links": {
          "postAuthor": {"forward":{"on":"posts","label":"author","has":"one","required":true},"reverse":{"on":"users","label":"posts","has":"many"}}
        }
      }
    }`
	postLine := fmt.Sprintf(`{"entity":{"id":%q,"title":["hello","world",9007199254740993,{"nested":9223372036854775807}],"author":%q},"createdAt":1700000000000}`, uuidStr(postID), uuidStr(userID))
	userLine := fmt.Sprintf(`{"entity":{"id":%q},"createdAt":1700000000000}`, uuidStr(userID))

	var zipBuf bytes.Buffer
	zw := zip.NewWriter(&zipBuf)
	wCfg, err := zw.Create("config.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wCfg.Write([]byte(configJSON)); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{"entities/posts.jsonl": postLine, "entities/users.jsonl": userLine} {
		w, werr := zw.Create(name)
		if werr != nil {
			t.Fatal(werr)
		}
		if _, werr := w.Write([]byte(body)); werr != nil {
			t.Fatal(werr)
		}
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(zipBuf.Bytes())
	counts, err := backup.RestoreV1Zip(ctx, pool, reader, int64(reader.Len()), appID)
	if err != nil {
		t.Fatalf("RestoreV1Zip: %v", err)
	}
	if counts.Attrs != 4 || counts.Triples != 4 {
		t.Fatalf("unexpected current-v1 import counts: %+v", counts)
	}
	var titleRequired, idRequired, authorRequired bool
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='title'`, appID).Scan(&titleRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='id'`, appID).Scan(&idRequired); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT is_required FROM attrs WHERE app_id=$1 AND etype='posts' AND label='author'`, appID).Scan(&authorRequired); err != nil {
		t.Fatal(err)
	}
	if !titleRequired || !idRequired || !authorRequired {
		t.Fatalf("current-v1 requiredness not preserved: title=%v id=%v author=%v", titleRequired, idRequired, authorRequired)
	}
	var titleTriples int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM triples t JOIN attrs a ON a.id=t.attr_id WHERE t.app_id=$1 AND a.etype='posts' AND a.label='title'`, appID).Scan(&titleTriples); err != nil {
		t.Fatal(err)
	}
	if titleTriples != 1 {
		t.Fatalf("one-cardinality array was expanded into %d triples", titleTriples)
	}
	var integer, nested string
	if err := pool.QueryRow(ctx, `SELECT t.value->>2, t.value->3->>'nested'
		FROM triples t JOIN attrs a ON a.id=t.attr_id
		WHERE t.app_id=$1 AND a.etype='posts' AND a.label='title'`, appID).Scan(&integer, &nested); err != nil {
		t.Fatal(err)
	}
	if integer != "9007199254740993" || nested != "9223372036854775807" {
		t.Fatalf("restore changed exact integers: %s, %s", integer, nested)
	}
}
