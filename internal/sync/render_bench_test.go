package sync

import (
	"encoding/json"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// Baseline for the T1.3 follow-up: cost of rendering one refresh frame for a
// match-all envelope (~460 KB class). Run with -bench BenchmarkRenderBigFrame.
func benchEnvelope(n int) json.RawMessage {
	ents := make([]map[string]any, n)
	for i := range ents {
		ents[i] = map[string]any{
			"id":    "9f296e7e-1a41-4f21-9b8e-000000000001",
			"title": "some reasonably long todo title to inflate the envelope size",
			"done":  false,
		}
	}
	b, _ := json.Marshal(map[string]any{"data": map[string]any{"todos": ents}})
	return b
}

func BenchmarkRenderBigFrame(b *testing.B) {
	result := benchEnvelope(4000)
	cat := &platform.AttrCatalog{} // unknown attrs take the lossless fallback branch
	nl, err := BuildNodeList(cat, result)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload := computationEntry(
			[2]json.RawMessage{keyInstaqlQuery, json.RawMessage(`{"todos":{}}`)},
			[2]json.RawMessage{keyInstaqlResult, nl},
		)
		fr := Frame{
			"op":              json.RawMessage(`"refresh-ok"`),
			"computations":    payload,
			"processed-tx-id": json.RawMessage(`7`),
		}
		if _, err := fr.Encode(); err != nil {
			b.Fatal(err)
		}
	}
}
