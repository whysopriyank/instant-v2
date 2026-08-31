package benchrun

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestEndpointObservationsRejectUnitMismatch(t *testing.T) {
	pairs := make(map[string]map[string]Run, 7)
	for i := 0; i < 7; i++ {
		id := fmt.Sprintf("pair-%d", i)
		v1 := Run{PrimaryClass: Pass, Measurements: map[string]Measurement{"wire_bytes": {Status: StatusValue, Value: 10, Unit: "bytes_per_affected_recipient"}}}
		v2 := Run{PrimaryClass: Pass, Measurements: map[string]Measurement{"wire_bytes": {Status: StatusValue, Value: 5, Unit: "bytes"}}}
		pairs[id] = map[string]Run{"v1": v1, "v2": v2}
	}
	obs := endpointObservations(pairs, "wire_bytes")
	if len(obs) != 7 {
		t.Fatalf("observations=%d", len(obs))
	}
	for _, o := range obs {
		if !o.Failed || o.Failure != HarnessDefect {
			t.Fatalf("unit mismatch was accepted: %+v", o)
		}
	}
	if gate := AggregateDirection(obs, "wire_bytes", "bytes", "affected-recipient coverages", 1, LowerIsBetter).ClaimGate; gate.Eligible {
		t.Fatal("unit mismatch produced an eligible claim")
	}
}

func TestOfflineReportReplay(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	m := Manifest{SchemaVersion: SchemaVersion, BundleID: "b1", PairID: "p1", Family: "H", SubscriberScale: 300, Seed: 1, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Now().UTC()}
	if err := w.WriteJSON("manifest.json", m); err != nil {
		t.Fatal(err)
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := ReportFromArtifacts(root); err == nil {
		t.Fatal("incomplete bundle was accepted")
	}
}

func TestReportRejectsOneRowExpectedCardinalityBypass(t *testing.T) {
	dir := t.TempDir()
	now := time.Now().UTC()
	run := Run{SchemaVersion: SchemaVersion, ID: "r", PairID: "p", TargetID: "v1", Family: "H", Scale: 300, Seed: 1, PrimaryClass: Pass, ExpectedLedgerRows: 2, ExpectedMutationRecipients: 2}
	row := LedgerRow{SchemaVersion: SchemaVersion, RunID: "r", PairID: "p", WriterID: "w", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"}, ExpectedMaterializedDigest: "digest", ObservedMaterializedDigest: "digest", Coverage: "exact", SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now}
	frame := Frame{SchemaVersion: SchemaVersion, RunID: "r", RecipientID: "recipient", ReceivedAt: now, Kind: "refresh", PayloadBytes: Zero("bytes")}
	if err := os.WriteFile(filepath.Join(dir, "ledger.jsonl"), mustJSONLForTest(row), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "frames.jsonl"), mustJSONLForTest(frame), 0600); err != nil {
		t.Fatal(err)
	}
	if err := validateRunEvidence(dir, run); err == nil {
		t.Fatal("one-row ledger bypassed expected cardinality")
	}
}

func mustJSONLForTest(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

func TestCoalescedLedgerEvidenceRequiresCoveredPrefixAndDigest(t *testing.T) {
	root := t.TempDir()
	run := Run{ID: "run-coalesced", PrimaryClass: Pass, ExpectedLedgerRows: 1, ExpectedMutationRecipients: 1}
	now := time.Now().UTC()
	row := LedgerRow{SchemaVersion: SchemaVersion, RunID: run.ID, PairID: "p", WriterID: "w", RecipientID: "r", ClientEventID: "e", ExpectedQuerySet: []string{"q"}, ExpectedRecipientSet: []string{"r"}, SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now, ExpectedMaterializedDigest: "original", CoveredExpectedMaterializedDigest: "covered", ObservedMaterializedDigest: "covered", ExpectedStateVersion: 1, CoveredExpectedStateVersion: 2, ObservedStateVersion: 2, Coverage: "coalesced"}
	frame := Frame{SchemaVersion: SchemaVersion, RunID: run.ID, RecipientID: "r", ReceivedAt: now, Kind: "refresh", PayloadBytes: Zero("bytes")}
	write := func(name string, value any) {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, name), append(b, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	write("ledger.jsonl", row)
	write("frames.jsonl", frame)
	if err := validateRunEvidence(root, run); err != nil {
		t.Fatalf("valid coalesced evidence rejected: %v", err)
	}
	row.CoveredExpectedMaterializedDigest = "original"
	write("ledger.jsonl", row)
	if err := validateRunEvidence(root, run); err == nil {
		t.Fatal("coalesced digest bypass was accepted")
	}
}

func TestPairOfflineReportRejectsOmittedOrForgedRunEvidenceBudget(t *testing.T) {
	for name, mutate := range map[string]func(*Run){
		"omitted": func(run *Run) {
			run.EvidenceMaxFrames = 0
			run.EvidenceMaxBytes = 0
			run.EvidenceMaxRetained = 0
		},
		"forged": func(run *Run) { run.EvidenceMaxBytes++ },
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			writer, err := NewArtifactWriter(root, 1<<26)
			if err != nil {
				t.Fatal(err)
			}
			runner := PairRunner{
				Writer: writer, Executor: SyntheticExecutor{},
				Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "budget-pair", PairID: "budget-pair", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()},
				Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7},
				Targets:  []Target{{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"}},
			}
			if _, err := runner.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(root, "runs", "budget-pair-01-v1", "run.json")
			var run Run
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(b, &run); err != nil {
				t.Fatal(err)
			}
			manifest, err := LoadManifest(root)
			if err != nil {
				t.Fatal(err)
			}
			if run.EvidenceMaxFrames != manifest.EvidenceMaxFrames || run.EvidenceMaxBytes != manifest.EvidenceMaxBytes || run.EvidenceMaxRetained != manifest.EvidenceMaxRetained {
				t.Fatalf("runner did not initialize run evidence budget: run=%+v manifest=%+v", run, manifest)
			}
			mutate(&run)
			if err := os.WriteFile(path, mustJSON(run), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := reportFromArtifacts(root, false); err == nil {
				t.Fatalf("offline pair report accepted %s run evidence budget", name)
			}
		})
	}
}

func TestPairOfflineReportFlagsForgedTargetKind(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<26)
	if err != nil {
		t.Fatal(err)
	}
	runner := PairRunner{
		Writer: w, Executor: SyntheticExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "kind-pair", PairID: "kind-pair", Family: "H-append", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"},
		},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "targets", "v2.json")
	var target Target
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &target); err != nil {
		t.Fatal(err)
	}
	target.Kind = "v1"
	if err := os.WriteFile(path, mustJSON(target), 0600); err != nil {
		t.Fatal(err)
	}
	summary, err := reportFromArtifacts(root, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.ClaimGate.Eligible || !strings.Contains(strings.Join(summary.ClaimGate.Reasons, "\n"), "kind binding") {
		t.Fatalf("forged pair target kind was not gated: %+v", summary.ClaimGate)
	}
}
