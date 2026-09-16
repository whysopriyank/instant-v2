package corpus

import (
	"fmt"
	"os"
	"strings"
)

// ValidateDA004VReleaseEnvelope verifies the release-facing policy evidence
// paired with the canonical corpus manifest. Package/runtime tests remain
// implementation evidence; this assertion is the release composition check.
func ValidateDA004VReleaseEnvelope(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("DA-004V release-envelope evidence: %w", err)
	}
	text := strings.ToLower(string(b))
	for _, marker := range []string{
		"## da-004v exclusion",
		"decision: `excluded`",
		"dynamic data-dependent view rules",
		"rejection is enforced before protected rows are fetched or returned",
		"`permissions-ws-dynamic-view-exclusion`",
		"package/runtime and db-backed tests are implementation evidence",
	} {
		if !strings.Contains(text, marker) {
			return fmt.Errorf("DA-004V release-envelope evidence missing %q", marker)
		}
	}
	for _, claim := range []string{
		"dynamic view rules supported",
		"dynamic view rules are supported",
		"dynamic view support: supported",
		"dynamic-view support: supported",
	} {
		if strings.Contains(text, claim) {
			return fmt.Errorf("DA-004V release envelope claims unsupported dynamic-view behavior: %q", claim)
		}
	}
	return nil
}
