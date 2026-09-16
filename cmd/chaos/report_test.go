package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validPASSReport() chaosRunReport {
	return chaosRunReport{
		Status: "PASS",
		Candidate: candidateProvenance{
			Revision:        "abc",
			TreeFingerprint: "tree",
			BinarySHA256:    "bin",
			SnapshotSHA256:  "snapshot",
		},
		Fixture: chaosFixture{ChaosApp: "chaos", AttrID: "attr", CorpusApp: "corpus", PGDev: 1, PGIno: 2, PGIdentity: "dev:1 ino:2"},
		Process: chaosProcessIdentity{
			Postmaster:        postmasterProcessIdentity{PID: 1, StartTime: 1},
			PostmasterRestart: postmasterProcessIdentity{PID: 4, StartTime: 2},
			InstantdPreCrash:  instantdProcessIdentity{PID: 2, StartTime: 1, BinarySHA256: "bin"},
			InstantdPostCrash: instantdProcessIdentity{PID: 3, StartTime: 2, BinarySHA256: "bin"},
		},
		Artifact: chaosArtifact{
			InstantdSHA256:     "bin",
			CandidateAgreement: true,
			ReplayBaselineOK:   true,
			ReplayAfterOK:      true,
			ReplayOutcome:      "baseline: 1/1; post-chaos: 1/1",
			CleanupComplete:    true,
		},
	}
}

func TestWriteChaosReportIsDurableAndWriteOnce(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "chaos-report.json")
	reportPath, err := writeChaosReport(path, validPASSReport())
	if err != nil || reportPath != path {
		t.Fatalf("report write failed: path=%q err=%v", reportPath, err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got chaosRunReport
	if err := json.Unmarshal(b, &got); err != nil || got.Status != "PASS" || !got.Artifact.CandidateAgreement {
		t.Fatalf("invalid durable report: err=%v report=%s", err, b)
	}
	if _, err := writeChaosReport(path, chaosRunReport{Status: "OVERWRITE"}); err == nil {
		t.Fatal("existing report was overwritten")
	}
}

func TestWriteChaosReportPropagatesWriteFailure(t *testing.T) {
	_, err := writeChaosReport(filepath.Join(t.TempDir(), "missing", "report.json"), validPASSReport())
	if err == nil || !strings.Contains(err.Error(), "create chaos report") {
		t.Fatalf("report write failure swallowed: %v", err)
	}
}

func TestValidateReportPathRejectsDisposableDataDir(t *testing.T) {
	root := t.TempDir()
	pgData := filepath.Join(root, "pg-data")
	if err := validateReportPath(filepath.Join(pgData, "report.json"), pgData); err == nil {
		t.Fatal("report inside disposable data directory accepted")
	}
	if err := validateReportPath(filepath.Join(root, "report.json"), pgData); err != nil {
		t.Fatalf("report beside disposable data directory rejected: %v", err)
	}
}

func TestVerifyBinaryHashDetectsReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "instantd")
	if err := os.WriteFile(path, []byte("candidate"), 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := sha256File(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyBinaryHash(path, hash); err != nil {
		t.Fatalf("matching binary rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyBinaryHash(path, hash); err == nil || !strings.Contains(err.Error(), "binary hash mismatch") {
		t.Fatalf("replacement binary accepted: %v", err)
	}
}
