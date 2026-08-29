package benchrun

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestApprovalTupleDetectsForgery(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(), V1SHA: "v1", V2SHA: "v2", SourceTree: "source", SchemaHash: "schema", FixtureHash: "fixture", ConfigHash: "config", DatabaseIDs: map[string]string{"v1": "instant_bench_v1", "v2": "instant_bench_v2"}, TargetProvenance: map[string]string{"v1": "rev|clean|wal2json|post-commit", "v2": "rev|clean||post-commit"}}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	m.ContentRoot = "root"
	m.ApprovalPublicKey = hex.EncodeToString(pub)
	tuple, err := ApprovalTuple(m)
	if err != nil {
		t.Fatal(err)
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	if err := VerifyApproval(m); err != nil {
		t.Fatalf("valid approval rejected: %v", err)
	}
	m.ContentRoot = "forged"
	if err := VerifyApproval(m); err == nil {
		t.Fatal("forged provenance accepted")
	}
}

func TestApprovalTupleBindsArtifactBudget(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(), ContentRoot: strings.Repeat("a", 64), ArtifactMaxTotalBytes: 1000, ArtifactMaxFileBytes: 500}
	tuple, err := ApprovalTuple(m)
	if err != nil {
		t.Fatal(err)
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	if err := VerifyApproval(m); err != nil {
		t.Fatal(err)
	}
	m.ArtifactMaxTotalBytes++
	if err := VerifyApproval(m); err == nil {
		t.Fatal("budget-only manifest mutation passed approval")
	}
}

func TestApprovalTupleBindsTargetKindProvenance(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	m := Manifest{
		SchemaVersion: SchemaVersion, BundleID: "kind-bound", PairID: "p", Family: "H-append", SubscriberScale: 300, Seed: 7,
		RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(),
		ContentRoot: strings.Repeat("a", 64), TargetProvenance: map[string]string{"v1": "v1|rev|clean"},
	}
	tuple, err := ApprovalTuple(m)
	if err != nil {
		t.Fatal(err)
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	if err := VerifyApproval(m); err != nil {
		t.Fatal(err)
	}
	m.TargetProvenance["v1"] = "v2|rev|clean"
	if err := VerifyApproval(m); err == nil {
		t.Fatal("target kind provenance forgery passed detached approval")
	}
}

func TestUnsignedReplayOfLargerContractBudgetStaysConservative(t *testing.T) {
	root := t.TempDir()
	plan300 := Plan{SchemaVersion: SchemaVersion, Seed: 7, Families: []string{"H-append"}, Scales: []int{300}, Pairs: 7, MeasureSeconds: 180}
	total300, file300, err := ContractArtifactBudget("H-append", 300, plan300)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(), ArtifactMaxTotalBytes: total300, ArtifactMaxFileBytes: file300}
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("manifest.json", m); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", plan300); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	t.Setenv(ApprovalPrivateKeyEnv, hex.EncodeToString(priv))
	if err = ApproveBundle(root); err != nil {
		t.Fatal(err)
	}
	approved, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	largePath := filepath.Join(root, "huge.jsonl")
	f, err := os.Create(largePath)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.Truncate(DefaultMaxArtifactBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	plan2000 := plan300
	plan2000.Scales = []int{2000}
	total2000, file2000, err := ContractArtifactBudget("H-append", 2000, plan2000)
	if err != nil {
		t.Fatal(err)
	}
	approved.ArtifactMaxTotalBytes, approved.ArtifactMaxFileBytes = total2000, file2000
	manifestBytes, err := json.Marshal(approved)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "manifest.json"), append(manifestBytes, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "plan.json"), mustJSON(plan2000), 0600); err != nil {
		t.Fatal(err)
	}
	files, err := bundleFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	var checks strings.Builder
	for _, path := range paths {
		sum, err := hashFileStringBounded(filepath.Join(root, path), total2000)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&checks, "%s  %s\n", sum, path)
	}
	if err = os.WriteFile(filepath.Join(root, "checksums.sha256"), []byte(checks.String()), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("replayed larger unsigned budget was not conservatively rejected: %v", err)
	}
}

func TestLiveConfigAuthorizationBindsConfig(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	cfg := LiveConfig{PairID: "p", Seed: 7, Family: "H", Scale: 300, Output: "bundle", Fixture: FixtureIDs{EntityType: "q", ValueAttr: "value", ValueAttrID: "00000000-0000-0000-0000-000000000001"}, Targets: []LiveTargetConfig{{ID: "v1", ProvisionCommand: []string{"/bin/true"}, ProvisionCommandSHA256: "hash"}}}
	tuple, err := LiveConfigAuthorizationTuple(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.AuthorizationSignature = hex.EncodeToString(ed25519.Sign(priv, tuple))
	if err := VerifyLiveConfigAuthorization(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Targets[0].ProvisionCommand[0] = "/bin/false"
	if err := VerifyLiveConfigAuthorization(cfg); err == nil {
		t.Fatal("tampered authorized executable accepted")
	}
}

func TestLiveConfigAuthorizationPreservesAdjacentLargeSeeds(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	cfg := LiveConfig{PairID: "p", Seed: 9007199254740993, Family: "H", Scale: 300, Fixture: FixtureIDs{EntityType: "q", ValueAttrID: "00000000-0000-0000-0000-000000000001"}}
	one, err := LiveConfigAuthorizationTuple(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Seed++
	two, err := LiveConfigAuthorizationTuple(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(one) == string(two) {
		t.Fatal("adjacent large authorization seeds were aliased")
	}
	cfg.Seed--
	cfg.AuthorizationSignature = hex.EncodeToString(ed25519.Sign(priv, one))
	if err := VerifyLiveConfigAuthorization(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Seed++
	if err := VerifyLiveConfigAuthorization(cfg); err == nil {
		t.Fatal("modified large seed passed authorization")
	}
}

func TestApproveBundleSignsImmutableContentRoot(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()}
	if err = w.WriteJSON("manifest.json", m); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	t.Setenv(ApprovalPrivateKeyEnv, hex.EncodeToString(priv))
	if err = ApproveBundle(root); err != nil {
		t.Fatal(err)
	}
	approved, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = VerifyApproval(approved); err != nil {
		t.Fatal(err)
	}
	if err = VerifyContentRoot(root, approved); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "plan.json"), []byte("tampered"), 0640); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("tampered raw evidence passed checksums")
	}
}
