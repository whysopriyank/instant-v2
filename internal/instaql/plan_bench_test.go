package instaql

import (
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/datalog"
	"github.com/instant-v2/instant-v2/internal/platform"
)

var (
	benchmarkPreparedQuery *Query
	benchmarkPreparedSQL   string
	benchmarkPreparedArgs  []any
)

// BenchmarkQueryPreparationHeadroom measures the exact catalog-independent
// work a compiled-plan cache could remove from an eligible refresh. Database
// execution, entity loading, projection, and rendering are intentionally out
// of scope so this is an upper-bound component measurement, not a latency
// claim.
func BenchmarkQueryPreparationHeadroom(b *testing.B) {
	cat, raw := benchmarkPreparationFixture(b)
	appID := "00000000-0000-4000-8000-000000000001"

	b.Run("coerce", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var input map[string]any
			if err := json.Unmarshal(raw, &input); err != nil {
				b.Fatal(err)
			}
			q, err := Coerce(input)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPreparedQuery = q
		}
	})

	var input map[string]any
	if err := json.Unmarshal(raw, &input); err != nil {
		b.Fatal(err)
	}
	q, err := Coerce(input)
	if err != nil {
		b.Fatal(err)
	}
	form := q.Forms[0]
	b.Run("conditions-and-sql", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			conds, err := buildConditions(form, cat)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPreparedSQL, benchmarkPreparedArgs = datalog.BuildPlan(appID, form.Etype, conds).EntitySetSQL(cat.ByEtype(form.Etype))
		}
	})

	b.Run("complete", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			var input map[string]any
			if err := json.Unmarshal(raw, &input); err != nil {
				b.Fatal(err)
			}
			q, err := Coerce(input)
			if err != nil {
				b.Fatal(err)
			}
			conds, err := buildConditions(q.Forms[0], cat)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkPreparedQuery = q
			benchmarkPreparedSQL, benchmarkPreparedArgs = datalog.BuildPlan(appID, q.Forms[0].Etype, conds).EntitySetSQL(cat.ByEtype(q.Forms[0].Etype))
		}
	})
}

func benchmarkPreparationFixture(tb testing.TB) (*platform.AttrCatalog, []byte) {
	tb.Helper()
	cat := &platform.AttrCatalog{}
	etype := "todos"
	labels := []string{"id", "title", "rank", "done", "owner", "priority", "status", "score", "category"}
	for i, label := range labels {
		id := [16]byte{0: 0x40, 14: byte(i >> 8), 15: byte(i + 1)}
		valueType := "blob"
		if label == "owner" {
			valueType = "ref"
		}
		labelCopy := label
		etypeCopy := etype
		cat.Add(platform.Attr{ID: id, Etype: &etypeCopy, Label: &labelCopy, ValueType: valueType, Cardinality: "one", IsIndexed: true})
	}
	raw := []byte(`{"todos":{"$":{"where":{"title":{"$like":"task%"},"rank":{"$gte":10},"done":false,"owner":"00000000-0000-4000-8000-000000000002","priority":{"$in":[1,2,3]},"status":{"$not":"archived"},"score":{"$lt":100},"category":"work"}}}}`)
	return cat, raw
}
