package benchrun

import (
	"fmt"
	"math"
	"sort"
	"strings"
)

// RenderMarkdown formats only into strings.Builder, whose writes cannot fail.
func RenderMarkdown(s Summary) string {
	normalizeSummaryReasons(&s)
	var b strings.Builder
	b.WriteString("# Benchmark report\n\n")
	_, _ = fmt.Fprintf(&b, "Schema: `%s`; aggregator: `%s`\n\n", s.SchemaVersion, s.AggregatorVersion)
	if len(s.Comparisons) > 0 {
		for _, comparison := range s.Comparisons {
			_, _ = fmt.Fprintf(&b, "# Comparison `%s`: `%s` (%s) → `%s` (%s)\n\n", comparison.ID, comparison.BaselineID, comparison.BaselineRevision, comparison.CandidateID, comparison.CandidateRevision)
			_, _ = fmt.Fprintf(&b, "Comparison claim eligible: %t\n\n", comparison.ClaimGate.Eligible)
			for _, cell := range comparison.Cells {
				renderCell(&b, cell)
			}
		}
		return b.String()
	}
	for _, c := range s.Cells {
		renderCell(&b, c)
	}
	return b.String()
}

func renderCell(b *strings.Builder, c CellSummary) {
	_, _ = fmt.Fprintf(b, "## %s @ %d subscribers\n\nAttempts: %d\n\n", c.Family, c.Scale, c.Attempts)
	if len(c.Ratios) > 0 {
		_, _ = fmt.Fprintf(b, "Median paired ratio: %.4f; 95%% CI [%.4f, %.4f]\n\n", c.CIpoint(), c.CI.Lower, c.CI.Upper)
	}
	_, _ = fmt.Fprintf(b, "Claim eligible: %t\n", c.ClaimGate.Eligible)
	if len(c.ClaimGate.Reasons) > 0 {
		b.WriteString("Reasons: " + strings.Join(c.ClaimGate.Reasons, "; ") + "\n")
	}
	endpoints := make([]string, 0, len(c.EndpointClaims))
	for endpoint := range c.EndpointClaims {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	for _, endpoint := range endpoints {
		gate := c.EndpointClaims[endpoint]
		_, _ = fmt.Fprintf(b, "Endpoint %s claim eligible: %t", endpoint, gate.Eligible)
		if len(gate.Reasons) > 0 {
			b.WriteString(" (" + strings.Join(gate.Reasons, "; ") + ")")
		}
		b.WriteByte('\n')
	}
	b.WriteByte('\n')
}

func (c CellSummary) CIpoint() float64 {
	if len(c.Ratios) == 0 {
		return 0
	}
	xs := append([]float64(nil), c.Ratios...)
	sort.Float64s(xs)
	return math.Exp(xs[len(xs)/2])
}
