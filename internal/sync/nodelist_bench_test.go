package sync

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

var benchmarkNodeListSink json.RawMessage

func BenchmarkBuildNodeList(b *testing.B) {
	cat, result := benchmarkNodeListFixture(b, 300)
	b.ReportAllocs()
	b.SetBytes(int64(len(result)))
	b.ResetTimer()
	for range b.N {
		var err error
		benchmarkNodeListSink, err = BuildNodeList(cat, result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkNodeListFixture(tb testing.TB, count int) (*platform.AttrCatalog, json.RawMessage) {
	tb.Helper()
	cat := nodeListTestCatalog(tb)
	entities := make([]map[string]any, count)
	for i := range entities {
		entities[i] = map[string]any{
			"id":    fmt.Sprintf("todo-%04d", i),
			"title": "some reasonably long todo title",
			"rank":  json.Number(fmt.Sprintf("%d.0", i)),
			"done":  i%2 == 0,
			"owner": []any{map[string]any{"id": fmt.Sprintf("user-%04d", i%25)}},
		}
	}
	result, err := json.Marshal(map[string]any{"data": map[string]any{"todos": entities}})
	if err != nil {
		tb.Fatal(err)
	}
	return cat, result
}
