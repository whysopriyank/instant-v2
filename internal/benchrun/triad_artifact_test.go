package benchrun

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func signedTriadManifest(t *testing.T, root string) Manifest {
	t.Helper()
	order, err := ThreeTargetSchedule(17, "triad")
	if err != nil {
		t.Fatal(err)
	}
	plan := Plan{SchemaVersion: SchemaVersion, Seed: 17, Families: []string{"H-append"}, Scales: []int{300}, Pairs: 7, MeasureSeconds: 180}
	total, file, err := ContractArtifactBudgetForTargets("H-append", 300, plan, 3)
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	m := Manifest{
		SchemaVersion: SchemaVersion, BundleID: "triad", PairID: "triad", Family: "H-append", SubscriberScale: 300,
		Seed: 17, RunOrder: order.Order, TargetOrder: order.Blocks,
		TargetRevisions: map[string]string{"v1": "v1-sha", "v2_reference": "reference-sha", "v2_current": "current-sha"},
		Comparisons: []ComparisonSpec{
			{ID: "v1-v2_current", BaselineID: "v1", CandidateID: "v2_current", BaselineRevision: "v1-sha", CandidateRevision: "current-sha"},
			{ID: "v2_reference-v2_current", BaselineID: "v2_reference", CandidateID: "v2_current", BaselineRevision: "reference-sha", CandidateRevision: "current-sha"},
		},
		ArtifactMaxTotalBytes: total, ArtifactMaxFileBytes: file, ContentRoot: strings.Repeat("a", 64), StartedAt: time.Unix(1, 0).UTC(),
	}
	tuple, err := ApprovalTuple(m)
	if err != nil {
		t.Fatal(err)
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), mustJSON(m), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "plan.json"), mustJSON(plan), 0600); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestBundleArtifactLimitsUsesSignedThreeTargetBudget(t *testing.T) {
	root := t.TempDir()
	m := signedTriadManifest(t, root)
	total, file, err := bundleArtifactLimits(root)
	if err != nil {
		t.Fatal(err)
	}
	if total != m.ArtifactMaxTotalBytes || file != m.ArtifactMaxFileBytes {
		t.Fatalf("limits=%d/%d want %d/%d", total, file, m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes)
	}
}

func TestBundleArtifactLimitsRejectsForgedThreeTargetCount(t *testing.T) {
	root := t.TempDir()
	m := signedTriadManifest(t, root)
	delete(m.TargetRevisions, "v2_reference")
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), mustJSON(m), 0600); err != nil {
		t.Fatal(err)
	}
	total, file, err := bundleArtifactLimits(root)
	if err == nil {
		t.Fatal("forged three-target role count was accepted")
	}
	if total != DefaultMaxArtifactBytes || file != DefaultMaxArtifactBytes {
		t.Fatalf("forged limits=%d/%d want conservative defaults %d", total, file, DefaultMaxArtifactBytes)
	}
}
