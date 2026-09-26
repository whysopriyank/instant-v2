package benchrun

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func failedMeasurement(unit string) Measurement {
	return Measurement{Status: StatusFailed, Unit: unit, Error: "injected measurement failure"}
}

type dbFailingExecutor struct {
	failBefore  bool
	failAfter   bool
	unsupported bool
	err         error
}

func (e *dbFailingExecutor) Qualify(context.Context, Target) (Qualification, error) {
	return Qualification{Passed: true, Checks: map[string]bool{"db": true}}, nil
}

func (e *dbFailingExecutor) Execute(ctx context.Context, spec RunSpec) (ExecutionResult, error) {
	result := ExecutionResult{Run: spec.Run}
	now := time.Now().UTC()
	result.Run.ExpectedLedgerRows = 1
	result.Run.ExpectedMutationRecipients = 1
	result.Run.StartedAt = now
	result.Run.EndedAt = now.Add(time.Second)
	result.Run.MeasuredStartedAt = now
	result.Run.MeasuredFinishedAt = now.Add(time.Second)
	result.Run.PrimaryClass = Pass
	result.Run.Measurements = map[string]Measurement{
		"primary": {Status: StatusValue, Value: 10, Unit: "ms"},
	}
	result.Ledger = []LedgerRow{{
		SchemaVersion:              SchemaVersion,
		RunID:                      spec.Run.ID,
		PairID:                     spec.Run.PairID,
		WriterID:                   "w",
		RecipientID:                "r",
		ClientEventID:              spec.Run.ID + "/event-1",
		ExpectedQuerySet:           []string{"q"},
		ExpectedRecipientSet:       []string{"r"},
		ExpectedMaterializedDigest: "d",
		ObservedMaterializedDigest: "d",
		Coverage:                   "exact",
		SubmittedAt:                now,
		AcknowledgementAt:          now,
		CoverAt:                    now,
		ConvergedAt:                now,
	}}
	result.Frames = []Frame{{
		SchemaVersion:      SchemaVersion,
		RunID:              spec.Run.ID,
		RecipientID:        "r",
		ReceivedAt:         now,
		Kind:               "refresh",
		MaterializedDigest: "d",
		PayloadBytes:       Zero("bytes"),
	}}
	result.Run.CollectorProvenance = map[string]string{
		"process":  "supported: pid=42",
		"runtime":  "supported: prometheus",
		"database": "supported: pg_stat_database",
		"network":  "unsupported: none",
	}

	if e.unsupported {
		result.Run.CollectorProvenance["database"] = "unsupported: db collector not available"
		result.DBBefore = unsupportedDB("db collector not available")
		result.DBAfter = unsupportedDB("db collector not available")
		return result, nil
	}

	if e.failBefore {
		err := e.err
		if err == nil {
			err = errors.New("injected db before failure")
		}
		result.DBBefore = failedDBSnapshot(err.Error())
		result.Run.CollectorProvenance["database"] = "failed: " + err.Error()
		result.DBAfter = syntheticDBSnapshot(now.Add(time.Second))
		return result, nil
	}

	if e.failAfter {
		err := e.err
		if err == nil {
			err = errors.New("injected db after failure")
		}
		result.DBBefore = syntheticDBSnapshot(now)
		result.DBAfter = failedDBSnapshot(err.Error())
		result.Run.CollectorProvenance["database"] = "failed: " + err.Error()
		return result, nil
	}

	result.DBBefore = syntheticDBSnapshot(now)
	result.DBAfter = syntheticDBSnapshot(now.Add(time.Second))
	return result, nil
}

func TestEV005InjectedDBBeforeFailure(t *testing.T) {
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	exec := &dbFailingExecutor{failBefore: true}
	runner := PairRunner{
		Writer:   writer,
		Executor: exec,
		Manifest: Manifest{
			SchemaVersion: SchemaVersion, BundleID: "ev005-before", PairID: "ev005-before",
			Family: "H-append", SubscriberScale: 300, Seed: 17, V1SHA: "v1-sha", V2SHA: "current-sha",
			RunOrder:        []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"},
			StartedAt:       time.Unix(1, 0).UTC(),
			DatabaseIDs:     map[string]string{"v1": "instant_bench_v1", "v2": "instant_bench_v2"},
			PostgresVersion: "17.0",
		},
		Plan: Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha", DatabaseName: "instant_bench_v1", PostgresVersion: "17.0"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Revision: "current-sha", DatabaseName: "instant_bench_v2", PostgresVersion: "17.0"},
		},
	}

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("runner.Run should aggregate failed attempt into report, but got error: %v", err)
	}

	if summary.ClaimGate.Eligible {
		t.Fatalf("claim gate should be ineligible on injected db before failure: %+v", summary.ClaimGate)
	}

	reasons := strings.Join(summary.ClaimGate.Reasons, "; ")
	if !strings.Contains(reasons, "failure") && !strings.Contains(reasons, "database") {
		t.Fatalf("claim gate reasons should indicate failure: %q", reasons)
	}

	runData, err := os.ReadFile(filepath.Join(root, "runs", "ev005-before-01-v1", "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run Run
	if err := json.Unmarshal(runData, &run); err != nil {
		t.Fatal(err)
	}
	if run.PrimaryClass != HarnessDefect {
		t.Fatalf("run.PrimaryClass = %q, want %q", run.PrimaryClass, HarnessDefect)
	}
}

func TestEV005InjectedDBAfterFailure(t *testing.T) {
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	exec := &dbFailingExecutor{failAfter: true}
	runner := PairRunner{
		Writer:   writer,
		Executor: exec,
		Manifest: Manifest{
			SchemaVersion: SchemaVersion, BundleID: "ev005-after", PairID: "ev005-after",
			Family: "H-append", SubscriberScale: 300, Seed: 17, V1SHA: "v1-sha", V2SHA: "current-sha",
			RunOrder:        []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"},
			StartedAt:       time.Unix(1, 0).UTC(),
			DatabaseIDs:     map[string]string{"v1": "instant_bench_v1", "v2": "instant_bench_v2"},
			PostgresVersion: "17.0",
		},
		Plan: Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha", DatabaseName: "instant_bench_v1", PostgresVersion: "17.0"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Revision: "current-sha", DatabaseName: "instant_bench_v2", PostgresVersion: "17.0"},
		},
	}

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("runner.Run should aggregate failed attempt into report, but got error: %v", err)
	}

	if summary.ClaimGate.Eligible {
		t.Fatalf("claim gate should be ineligible on injected db after failure: %+v", summary.ClaimGate)
	}

	runData, err := os.ReadFile(filepath.Join(root, "runs", "ev005-after-01-v1", "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run Run
	if err := json.Unmarshal(runData, &run); err != nil {
		t.Fatal(err)
	}
	if run.PrimaryClass != HarnessDefect {
		t.Fatalf("run.PrimaryClass = %q, want %q", run.PrimaryClass, HarnessDefect)
	}
}

func TestEV005UnsupportedDatabaseCollectorWhenRequired(t *testing.T) {
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	exec := &dbFailingExecutor{unsupported: true}
	runner := PairRunner{
		Writer:   writer,
		Executor: exec,
		Manifest: Manifest{
			SchemaVersion: SchemaVersion, BundleID: "ev005-unsupported", PairID: "ev005-unsupported",
			Family: "H-append", SubscriberScale: 300, Seed: 17, V1SHA: "v1-sha", V2SHA: "current-sha",
			RunOrder:        []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"},
			StartedAt:       time.Unix(1, 0).UTC(),
			DatabaseIDs:     map[string]string{"v1": "instant_bench_v1", "v2": "instant_bench_v2"},
			PostgresVersion: "17.0",
		},
		Plan: Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1-sha", DatabaseName: "instant_bench_v1", PostgresVersion: "17.0"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Revision: "current-sha", DatabaseName: "instant_bench_v2", PostgresVersion: "17.0"},
		},
	}

	summary, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("runner.Run should aggregate failed attempt into report, but got error: %v", err)
	}

	if summary.ClaimGate.Eligible {
		t.Fatalf("claim gate should be ineligible when required db collector is unsupported: %+v", summary.ClaimGate)
	}

	runData, err := os.ReadFile(filepath.Join(root, "runs", "ev005-unsupported-01-v1", "run.json"))
	if err != nil {
		t.Fatal(err)
	}
	var run Run
	if err := json.Unmarshal(runData, &run); err != nil {
		t.Fatal(err)
	}
	if run.PrimaryClass != HarnessDefect {
		t.Fatalf("run.PrimaryClass = %q, want %q", run.PrimaryClass, HarnessDefect)
	}

	// Also test offline replay: a passing live run with unsupported db collector must fail offline validation
	liveRoot := makeLiveBundle(t, false)
	mutateLiveRun(t, liveRoot, false, func(r *Run, runPath string) {
		r.CollectorProvenance["database"] = "unsupported: disabled in test"
		db := unsupportedDB("disabled in test")
		_ = os.WriteFile(filepath.Join(runPath, "db-before.json"), mustJSON(db), 0600)
		_ = os.WriteFile(filepath.Join(runPath, "db-after.json"), mustJSON(db), 0600)
	})
	if _, err := reportFromArtifacts(liveRoot, false); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatalf("offline report accepted live target with unsupported db collector: %v", err)
	}
}

func TestEV005AnyRequiredDatabaseMeasurementFailureIsIneligible(t *testing.T) {
	base := syntheticDBSnapshot(time.Now().UTC())
	setters := []struct {
		name string
		set  func(*DBSnapshot)
	}{
		{"connections", func(s *DBSnapshot) { s.Connections = failedMeasurement("count") }},
		{"block_hits", func(s *DBSnapshot) { s.BlockHits = failedMeasurement("count") }},
		{"block_reads", func(s *DBSnapshot) { s.BlockReads = failedMeasurement("count") }},
		{"temp_bytes", func(s *DBSnapshot) { s.TempBytes = failedMeasurement("bytes") }},
		{"temp_files", func(s *DBSnapshot) { s.TempFiles = failedMeasurement("count") }},
		{"commits", func(s *DBSnapshot) { s.Commits = failedMeasurement("count") }},
		{"rollbacks", func(s *DBSnapshot) { s.Rollbacks = failedMeasurement("count") }},
		{"tuple_reads", func(s *DBSnapshot) { s.TupleReads = failedMeasurement("count") }},
		{"tuple_writes", func(s *DBSnapshot) { s.TupleWrites = failedMeasurement("count") }},
		{"wal_bytes", func(s *DBSnapshot) { s.WALBytes = failedMeasurement("bytes") }},
		{"slot_lag", func(s *DBSnapshot) { s.SlotLag = failedMeasurement("bytes") }},
		{"pool_active", func(s *DBSnapshot) { s.PoolActive = failedMeasurement("count") }},
		{"pool_idle", func(s *DBSnapshot) { s.PoolIdle = failedMeasurement("count") }},
	}
	for _, tc := range setters {
		t.Run(tc.name, func(t *testing.T) {
			before := base
			tc.set(&before)
			if !isFailedDBSnapshot(before) {
				t.Fatal("required DB measurement failure was not detected")
			}
		})
	}
}

func TestEV005MixedUnsupportedDatabaseMeasurementIsIneligible(t *testing.T) {
	snapshot := syntheticDBSnapshot(time.Now().UTC())
	snapshot.PoolIdle = Unsupported("count", "pool collector unavailable")
	if !hasUnsupportedDBSnapshotMeasurement(snapshot) {
		t.Fatal("mixed unsupported database measurement was not detected")
	}
}

func TestEV005FinalizeWriteOnceAndExistingBundle(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("manifest.json", Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()}); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}

	// In-memory repeat finalize must fail
	if err = w.Finalize(); err == nil {
		t.Fatal("second Finalize on same writer succeeded")
	}

	// New writer on same finalized root must fail to finalize (write-once)
	w2, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w2.Finalize(); err == nil || !strings.Contains(err.Error(), "already finalized") {
		t.Fatalf("second Finalize on existing bundle succeeded: %v", err)
	}

	// Runner on existing finalized bundle must fail
	runner := PairRunner{
		Writer:   w2,
		Executor: SyntheticExecutor{},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7},
		Targets: []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"},
			{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"},
		},
	}
	if _, err := runner.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "already finalized") {
		t.Fatalf("runner.Run succeeded on already finalized bundle: %v", err)
	}
}

func TestEV005ApprovedBundleCannotBeRegeneratedOrTruncated(t *testing.T) {
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

	// Cannot re-approve
	if err = ApproveBundle(root); err == nil || (!strings.Contains(err.Error(), "already approved") && !strings.Contains(err.Error(), "regenerate")) {
		t.Fatalf("re-approval of approved bundle succeeded: %v", err)
	}

	// Cannot RebuildBundleIndexes
	if err = RebuildBundleIndexes(root); err == nil || (!strings.Contains(err.Error(), "approved bundle") && !strings.Contains(err.Error(), "regenerated")) {
		t.Fatalf("RebuildBundleIndexes succeeded on approved bundle: %v", err)
	}

	// Cannot BuildChecksums
	if err = BuildChecksums(root); err == nil || (!strings.Contains(err.Error(), "approved bundle") && !strings.Contains(err.Error(), "regenerated")) {
		t.Fatalf("BuildChecksums succeeded on approved bundle: %v", err)
	}

	// Truncating a file in place must fail verification
	if err = os.Truncate(filepath.Join(root, "plan.json"), 2); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("truncated file passed checksum verification")
	}
	approvedManifest, _ := LoadManifest(root)
	if err = VerifyContentRoot(root, approvedManifest); err == nil {
		t.Fatal("truncated file passed content root verification")
	}
}

func TestEV005PendingApprovalRecoveryRequiresValidSignature(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{SchemaVersion: SchemaVersion, BundleID: "pending", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()}
	if err = w.WriteJSON("manifest.json", manifest); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	t.Setenv(ApprovalPrivateKeyEnv, hex.EncodeToString(priv))
	if err = ApproveBundle(root); err != nil {
		t.Fatal(err)
	}

	approved, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	approved.ApprovalSignature = strings.Repeat("0", ed25519.SignatureSize*2)
	corrupt, err := marshalRedactedJSON(approved)
	if err != nil {
		t.Fatal(err)
	}
	corrupt = append(corrupt, '\n')
	if err = os.WriteFile(filepath.Join(root, "manifest.json"), corrupt, 0o640); err != nil {
		t.Fatal(err)
	}
	if err = buildChecksumsWithLimits(root, DefaultMaxArtifactBytes, DefaultMaxArtifactBytes); err != nil {
		t.Fatal(err)
	}
	marker, err := json.Marshal(approvalPendingMarker{OriginalManifest: original})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(approvalPendingPath(root), marker, 0o600); err != nil {
		t.Fatal(err)
	}

	if err = ApproveBundle(root); err != nil {
		t.Fatalf("ApproveBundle failed after restoring invalid pending approval: %v", err)
	}
	recovered, err := LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = VerifyApproval(recovered); err != nil {
		t.Fatalf("recovered bundle has invalid approval: %v", err)
	}
	if _, err = os.Stat(approvalPendingPath(root)); !os.IsNotExist(err) {
		t.Fatalf("pending approval marker remains after recovery: %v", err)
	}
}

func TestEV005PendingApprovalRecoveryAcceptsVerifiedApproval(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{SchemaVersion: SchemaVersion, BundleID: "pending-valid", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()}
	if err = w.WriteJSON("manifest.json", manifest); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv(TrustedApprovalPublicKeyEnv, hex.EncodeToString(pub))
	t.Setenv(ApprovalPrivateKeyEnv, hex.EncodeToString(priv))
	if err = ApproveBundle(root); err != nil {
		t.Fatal(err)
	}
	marker, err := json.Marshal(approvalPendingMarker{OriginalManifest: original})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(approvalPendingPath(root), marker, 0o600); err != nil {
		t.Fatal(err)
	}

	err = ApproveBundle(root)
	if err == nil || !strings.Contains(err.Error(), "already approved") {
		t.Fatalf("verified pending approval was not recognized as already approved: %v", err)
	}
	if _, err = os.Stat(approvalPendingPath(root)); !os.IsNotExist(err) {
		t.Fatalf("pending approval marker remains after verified recovery: %v", err)
	}
}

func TestEV005AtomicChecksumsInterruptionAndPermission(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("data.json", map[string]string{"key": "value"}); err != nil {
		t.Fatal(err)
	}

	// Verify that BuildChecksums writes atomically: write failure must leave no truncated checksums.sha256
	// and clean up any temp file.
	checksumsPath := filepath.Join(root, "checksums.sha256")
	_ = os.Remove(checksumsPath)

	// Make root read-only to simulate permission failure when creating checksums
	if err := os.Chmod(root, 0o555); err == nil {
		defer func() { _ = os.Chmod(root, 0o755) }()
		if err := BuildChecksums(root); err == nil {
			t.Fatal("BuildChecksums succeeded in read-only directory")
		}
		_ = os.Chmod(root, 0o755)
		if _, statErr := os.Stat(checksumsPath); !os.IsNotExist(statErr) {
			t.Fatalf("checksums.sha256 created despite permission error: %v", statErr)
		}
		entries, _ := filepath.Glob(filepath.Join(root, ".checksums-*.tmp"))
		if len(entries) > 0 {
			t.Fatalf("leftover temp checksum files: %v", entries)
		}
	}
}

func TestEV005SymlinkRejection(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("manifest.json", Manifest{SchemaVersion: SchemaVersion, BundleID: "b", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("plan.json", Plan{SchemaVersion: SchemaVersion, Seed: 7, Pairs: 7}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}

	// Symlink checksums.sha256 to another file
	targetChecksums := filepath.Join(t.TempDir(), "target-checksums.sha256")
	original, _ := os.ReadFile(filepath.Join(root, "checksums.sha256"))
	_ = os.WriteFile(targetChecksums, original, 0640)
	_ = os.Remove(filepath.Join(root, "checksums.sha256"))
	if err := os.Symlink(targetChecksums, filepath.Join(root, "checksums.sha256")); err != nil {
		t.Fatal(err)
	}

	if err := VerifyChecksums(root); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("VerifyChecksums accepted symlinked checksums.sha256: %v", err)
	}
}

func TestEV005ChecksumMismatchAndSerialization(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.WriteJSON("data.json", map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}

	// Mismatch test
	if err = os.WriteFile(filepath.Join(root, "data.json"), []byte("tampered\n"), 0640); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("VerifyChecksums accepted tampered file: %v", err)
	}
}
