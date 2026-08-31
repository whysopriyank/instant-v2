package benchrun

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func TestVerifyChecksumsIgnoresUnsignedLargeManifestBudget(t *testing.T) {
	root := t.TempDir()
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "unsigned", PairID: "pair", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC(), ArtifactMaxTotalBytes: MaxAbsoluteArtifactBudget, ArtifactMaxFileBytes: MaxAbsoluteArtifactBudget}
	manifest, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), append(manifest, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	largePath := filepath.Join(root, "huge.jsonl")
	f, err := os.Create(largePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(DefaultMaxArtifactBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	manifestHash, err := hashFileStringBounded(filepath.Join(root, "manifest.json"), SmallArtifactBytes)
	if err != nil {
		t.Fatal(err)
	}
	checksums := fmt.Sprintf("%s  manifest.json\n%s  huge.jsonl\n", manifestHash, strings.Repeat("0", 64))
	if err := os.WriteFile(filepath.Join(root, "checksums.sha256"), []byte(checksums), 0600); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unsigned large artifact was not rejected under conservative limits: %v", err)
	}
}

func TestContractArtifactBudgetScalesBeyondDefault(t *testing.T) {
	w, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureContractArtifactBudget(w, "H-append", 300, Plan{Seed: 7, MeasureSeconds: 180}); err != nil {
		t.Fatal(err)
	}
	if w.MaxBytes <= DefaultMaxArtifactBytes || w.MaxFileBytes <= DefaultMaxArtifactBytes {
		t.Fatalf("contract budget did not exceed default: total=%d file=%d", w.MaxBytes, w.MaxFileBytes)
	}
	w2000, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := configureContractArtifactBudget(w2000, "H-append", 2000, Plan{Seed: 7, MeasureSeconds: 180}); err != nil {
		t.Fatal(err)
	}
	if w2000.MaxBytes <= w.MaxBytes {
		t.Fatalf("2000 scale budget did not grow: 300=%d 2000=%d", w.MaxBytes, w2000.MaxBytes)
	}
}

func TestContractEvidenceBudgetBindsWorkloadShapeAndManifest(t *testing.T) {
	plan := Plan{SchemaVersion: SchemaVersion, Seed: 7, MeasureSeconds: 180}
	hBudget, err := ContractEvidenceBudget("H-append", 300, plan)
	if err != nil {
		t.Fatal(err)
	}
	if hBudget.MaxBytes <= 85_902_606_202 || hBudget.MaxBytes > benchharness.MaxAbsoluteEvidenceBytes {
		t.Fatalf("H300 live budget is not bounded/admitting: %#v", hBudget)
	}
	xBudget, err := ContractEvidenceBudget("X-heterogeneous", 300, plan)
	if err != nil {
		t.Fatal(err)
	}
	if xBudget.MaxBytes >= hBudget.MaxBytes {
		t.Fatalf("X300 budget ignored bucket cardinality: H=%d X=%d", hBudget.MaxBytes, xBudget.MaxBytes)
	}
	m := Manifest{Family: "H-append", SubscriberScale: 300}
	if err := bindManifestEvidenceBudget(&m, plan); err != nil {
		t.Fatal(err)
	}
	if err := validateManifestEvidenceBudget(m, plan); err != nil {
		t.Fatalf("bound evidence budget did not verify: %v", err)
	}
	m.EvidenceMaxBytes++
	if err := validateManifestEvidenceBudget(m, plan); err == nil {
		t.Fatal("tampered manifest evidence budget passed offline recomputation")
	}
}

func TestContractEvidenceBudgetRejectsMutationCountOverflow(t *testing.T) {
	plan := Plan{SchemaVersion: SchemaVersion, Seed: 7, MeasureSeconds: int(^uint(0) >> 1)}
	if _, err := ContractEvidenceBudget("H-append", 300, plan); err == nil {
		t.Fatal("overflowing live evidence mutation count was accepted")
	}
}

func TestContractEvidenceBudgetMaximumFamilyScaleMatrixStaysUnderCeiling(t *testing.T) {
	plan := Plan{SchemaVersion: SchemaVersion, Seed: 17, MeasureSeconds: 180}
	for _, family := range []benchharness.Family{benchharness.FamilyH, benchharness.FamilyX, benchharness.FamilyM, benchharness.FamilyO, benchharness.FamilyS, benchharness.FamilyR, benchharness.FamilyC, benchharness.FamilyT} {
		t.Run(string(family), func(t *testing.T) {
			budget, err := ContractEvidenceBudget(string(family), 2000, plan)
			if err != nil {
				t.Fatal(err)
			}
			if budget.MaxBytes > benchharness.MaxAbsoluteEvidenceBytes {
				t.Fatalf("maximum contract workload crossed evidence hard ceiling: %d", budget.MaxBytes)
			}
		})
	}
}

func TestContractEvidenceBudgetTracksSignedBehaviorWindow(t *testing.T) {
	canonical := Plan{SchemaVersion: SchemaVersion, Seed: 17, MeasureSeconds: 180}
	short := Plan{SchemaVersion: SchemaVersion, Seed: 17, MeasureSeconds: 120}
	canonicalBudget, err := ContractEvidenceBudget("R-reconnect", 300, canonical)
	if err != nil {
		t.Fatal(err)
	}
	w, err := benchharness.NewWorkload(benchharness.FamilyR, 300, canonical.Seed)
	if err != nil {
		t.Fatal(err)
	}
	directBudget, err := benchharness.EvidenceBudgetForWorkload(w, canonical.MeasureSeconds*8)
	if err != nil {
		t.Fatal(err)
	}
	if canonicalBudget != directBudget {
		t.Fatalf("offline contract budget diverged from adapter workload budget: offline=%#v direct=%#v", canonicalBudget, directBudget)
	}
	shortBudget, err := ContractEvidenceBudget("R-reconnect", 300, short)
	if err != nil {
		t.Fatal(err)
	}
	if shortBudget.MaxBytes >= canonicalBudget.MaxBytes {
		t.Fatalf("signed shorter R behavior window did not reduce lifecycle budget: canonical=%d short=%d", canonicalBudget.MaxBytes, shortBudget.MaxBytes)
	}
	tooLong := Plan{SchemaVersion: SchemaVersion, Seed: 17, MeasureSeconds: 360}
	if _, err := ContractEvidenceBudget("R-reconnect", 300, tooLong); err == nil {
		t.Fatal("signed R behavior window above canonical duration was accepted")
	}
}

func TestContractLedgerBoundCoversWorstShapeAndScalesWithCardinality(t *testing.T) {
	makeBound := func(scale int) (int64, int64, error) {
		w, err := benchharness.NewWorkload(benchharness.FamilyH, scale, 7)
		if err != nil {
			return 0, 0, err
		}
		bound, err := contractLedgerRowBound(scale, len(w.Fixture.Queries))
		if err != nil {
			return 0, 0, err
		}
		row := LedgerRow{SchemaVersion: SchemaVersion, PairID: "p", RunID: "r", WriterID: "writer", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: make([]string, len(w.Fixture.Queries)), ExpectedRecipientSet: make([]string, scale), ExpectedMaterializedDigest: strings.Repeat("a", 64), ObservedMaterializedDigest: strings.Repeat("b", 64), Coverage: "coalesced", CoveredExpectedMaterializedDigest: strings.Repeat("b", 64)}
		for i := range row.ExpectedQuerySet {
			row.ExpectedQuerySet[i] = fmt.Sprintf("query-%06d", i)
		}
		for i := range row.ExpectedRecipientSet {
			row.ExpectedRecipientSet[i] = fmt.Sprintf("recipient-%06d", i)
		}
		encoded, err := json.Marshal(row)
		if err != nil {
			return 0, 0, err
		}
		return bound, int64(len(encoded)), nil
	}
	bound300, size300, err := makeBound(300)
	if err != nil {
		t.Fatal(err)
	}
	if size300 >= bound300 {
		t.Fatalf("worst-shape row exceeds bound: size=%d bound=%d", size300, bound300)
	}
	bound2000, _, err := makeBound(2000)
	if err != nil {
		t.Fatal(err)
	}
	if bound2000 <= bound300*5 {
		t.Fatalf("row bound did not scale with repeated recipient sets: 300=%d 2000=%d", bound300, bound2000)
	}
	if _, err := contractLedgerRowBound(int(^uint(0)>>1), int(^uint(0)>>1)); err == nil {
		t.Fatal("overflowing ledger bound was accepted")
	}
}
