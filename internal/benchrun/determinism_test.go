package benchrun

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestRenderMarkdownDeterministicAcrossMapOrder(t *testing.T) {
	makeSummary := func() Summary {
		return Summary{SchemaVersion: SchemaVersion, AggregatorVersion: AggregatorVersion, ClaimGate: ClaimGate{Eligible: false, Reasons: []string{"z-reason", "a-reason", "z-reason"}}, Cells: []CellSummary{{Family: "H", Scale: 300, ClaimGate: ClaimGate{Eligible: false, Reasons: []string{"z-cell", "a-cell"}}, EndpointClaims: map[string]ClaimGate{"cpu": {Eligible: false, Reasons: []string{"z", "a"}}, "wire_bytes": {Eligible: true}}}}}
	}
	a := RenderMarkdown(makeSummary())
	b := RenderMarkdown(makeSummary())
	ha, hb := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	if hex.EncodeToString(ha[:]) != hex.EncodeToString(hb[:]) || a != b {
		t.Fatal("markdown rendering is not deterministic")
	}
}
