package benchrun

import (
	"strings"
	"testing"
)

func TestRenderMarkdownNamesThreeTargetComparisonsAndRevisions(t *testing.T) {
	summary := Summary{
		SchemaVersion:     SchemaVersion,
		AggregatorVersion: AggregatorVersion,
		Comparisons: []ComparisonSummary{
			{ID: "v1-v2_current", BaselineID: "v1", CandidateID: "v2_current", BaselineRevision: "v1-sha", CandidateRevision: "current-sha", ClaimGate: ClaimGate{Eligible: false}},
			{ID: "v2_reference-v2_current", BaselineID: "v2_reference", CandidateID: "v2_current", BaselineRevision: "reference-sha", CandidateRevision: "current-sha", ClaimGate: ClaimGate{Eligible: false}},
		},
	}
	markdown := RenderMarkdown(summary)
	for _, want := range []string{"v1-v2_current", "v2_reference-v2_current", "v1-sha", "reference-sha", "current-sha"} {
		if !strings.Contains(markdown, want) {
			t.Fatalf("markdown omitted %q:\n%s", want, markdown)
		}
	}
}
