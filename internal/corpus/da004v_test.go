package corpus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDA004VExplicitCompositionRequiresExclusionEvidenceForAnyDirectoryName(t *testing.T) {
	dir, manifest := manifestFixture(t)
	renamedDir := filepath.Join(t.TempDir(), "renamed-fixture")
	if err := os.Rename(dir, renamedDir); err != nil {
		t.Fatal(err)
	}
	saveManifest(t, renamedDir, manifest)

	if err := ValidateDA004VExclusion(renamedDir, "../../docs/reference/release-envelope.md"); err == nil || !strings.Contains(err.Error(), "DA-004V") {
		t.Fatalf("renamed corpus accepted without DA-004V exclusion evidence: %v", err)
	}

	symlink := filepath.Join(t.TempDir(), "corpus-link")
	if err := os.Symlink(renamedDir, symlink); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDA004VExclusion(symlink, "../../docs/reference/release-envelope.md"); err == nil || !strings.Contains(err.Error(), "DA-004V") {
		t.Fatalf("symlinked corpus accepted without DA-004V exclusion evidence: %v", err)
	}
}

func TestDA004VCorpusRejectsSupportClaim(t *testing.T) {
	surfaces := map[string]Surface{
		DA004VExclusionSurfaceID: {ID: DA004VExclusionSurfaceID, Status: "covered", Note: "dynamic view rules are supported"},
	}
	coverage := []CoverageEntry{{
		ID:            DA004VExclusionCoverageID,
		Surface:       DA004VExclusionSurfaceID,
		Status:        "unsupported",
		ExpectedState: "dynamic view rules are rejected before protected rows are fetched or returned",
	}}
	if err := validateDA004VCorpusExclusion(surfaces, coverage); err == nil || !strings.Contains(err.Error(), "DA-004V") {
		t.Fatalf("dynamic-view support claim was accepted: %v", err)
	}
}

func TestDA004VReleaseEnvelopeRejectsSupportClaimOrMissingEvidence(t *testing.T) {
	original, err := os.ReadFile("../../docs/reference/release-envelope.md")
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"support-claim": strings.Replace(string(original), "Decision: `EXCLUDED`", "Decision: `SUPPORTED`", 1),
		"missing-row":   strings.Replace(string(original), "`permissions-ws-dynamic-view-exclusion`", "removed-exclusion-row", 1),
	}
	for name, contents := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "release-envelope.md")
			if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
				t.Fatal(err)
			}
			if err := ValidateDA004VReleaseEnvelope(path); err == nil || !strings.Contains(err.Error(), "DA-004V") {
				t.Fatalf("invalid release evidence accepted: %v", err)
			}
		})
	}
}

func TestDA004VReleaseEnvelopeEvidence(t *testing.T) {
	if err := ValidateDA004VReleaseEnvelope("../../docs/reference/release-envelope.md"); err != nil {
		t.Fatal(err)
	}
}
