package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// helper to create a mock session dialer that completes 1 transaction cleanly.
func mockSuccessfulDialer(t *testing.T, clock Clock) func(ctx context.Context, logger *slog.Logger, rawURL, appID string, id int,
	attr *string, txInterval *time.Duration,
	refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
	ledger *TransactionLedger, quiescence time.Duration,
) error {
	return func(ctx context.Context, _ *slog.Logger, rawURL, appID string, id int,
		attr *string, txInterval *time.Duration,
		refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
		ledger *TransactionLedger, quiescence time.Duration,
	) error {
		conn := newFakeSessionConn()
		conn.PushFrame(map[string]any{"op": "init-ok", "app-id": appID})
		conn.PushFrame(map[string]any{
			"op":              "add-query-ok",
			"client-event-id": fmt.Sprintf("q-%d", id),
			"result":          []any{map[string]any{"id": "init-item"}},
			"processed-tx-id": 100,
		})

		var txSeq atomic.Int64
		conn.onWrite = func(data []byte) {
			var f struct {
				Op            string `json:"op"`
				ClientEventID string `json:"client-event-id"`
			}
			if err := json.Unmarshal(data, &f); err == nil && f.Op == "transact" {
				num := txSeq.Add(1)
				txID := fmt.Sprintf("%d", 100+num)
				conn.PushFrame(map[string]any{
					"op":              "transact-ok",
					"client-event-id": f.ClientEventID,
					"tx-id":           txID,
				})
				conn.PushFrame(map[string]any{
					"op":              "refresh-ok",
					"processed-tx-id": txID,
				})
			}
		}

		return driveSessionWithConn(ctx, conn, ledger, clock, id, appID, attr, txInterval,
			refreshes, transacts, connects, writeGate, quiescence)
	}
}

// EV-002a: Existing output file or directory must be rejected without overwriting prior evidence.
func TestEvidence_EV002_RejectExistingOutputPath(t *testing.T) {
	tempDir := t.TempDir()

	// 1. File target already exists
	existingFile := filepath.Join(tempDir, "existing-events.jsonl")
	originalContent := "prior immutable evidence"
	if err := os.WriteFile(existingFile, []byte(originalContent), 0o600); err != nil {
		t.Fatal(err)
	}

	err := ValidateDestination(existingFile, false, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error on existing file target, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}

	// Verify prior content was NOT modified
	data, _ := os.ReadFile(existingFile)
	if string(data) != originalContent {
		t.Fatalf("prior evidence was modified! got %q", string(data))
	}

	// 2. Directory target already exists
	existingDir := filepath.Join(tempDir, "existing-dir")
	if err := os.Mkdir(existingDir, 0o750); err != nil {
		t.Fatal(err)
	}
	err = ValidateDestination(existingDir, true, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error on existing directory target, got nil")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected 'already exists' error, got: %v", err)
	}
}

func TestDefaultStartPprofRejectsDisabledEndpoint(t *testing.T) {
	for _, addr := range []string{"", "none", "disabled"} {
		if _, err := defaultStartPprof(addr); err == nil {
			t.Fatalf("defaultStartPprof(%q) unexpectedly disabled mandatory pprof", addr)
		}
	}
}

// EV-002a: Existing manifest file must be rejected.
func TestEvidence_EV002_RejectExistingManifestPath(t *testing.T) {
	tempDir := t.TempDir()
	eventsFile := filepath.Join(tempDir, "new-events.jsonl")
	manifestFile := filepath.Join(tempDir, "new-events.manifest.json")

	// Events file does not exist, but manifest already exists
	if err := os.WriteFile(manifestFile, []byte(`{"status":"prior"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	err := ValidateDestination(eventsFile, false, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error when manifest already exists, got nil")
	}
	if !strings.Contains(err.Error(), "manifest target") || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected manifest already exists error, got: %v", err)
	}
}

// EV-002a: Symlink target must be rejected.
func TestEvidence_EV002_RejectSymlinkTarget(t *testing.T) {
	tempDir := t.TempDir()
	realTarget := filepath.Join(tempDir, "real-target.jsonl")
	symlinkTarget := filepath.Join(tempDir, "symlink-target.jsonl")

	if err := os.Symlink(realTarget, symlinkTarget); err != nil {
		t.Fatal(err)
	}

	err := ValidateDestination(symlinkTarget, false, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error for symlink target, got nil")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected symlink rejection error, got: %v", err)
	}
}

// EV-002a: Symlink ancestor must be rejected.
func TestEvidence_EV002_RejectSymlinkAncestor(t *testing.T) {
	tempDir := t.TempDir()
	realDir := filepath.Join(tempDir, "real-dir")
	symlinkDir := filepath.Join(tempDir, "symlink-dir")

	if err := os.Mkdir(realDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realDir, symlinkDir); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(symlinkDir, "events.jsonl")
	err := ValidateDestination(target, false, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error for symlink ancestor, got nil")
	}
	if !strings.Contains(err.Error(), "crosses symlink ancestor") {
		t.Fatalf("expected crosses symlink ancestor error, got: %v", err)
	}
}

// EV-002b: Required pprof startup failure must affect exit/eligibility.
func TestEvidence_EV002_InjectedPprofStartupFailure(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "soak-out")

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     100 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		PprofAddr:    "127.0.0.1:18811",
		OutDir:       outDir,
	}

	deps := defaultSoakDeps()
	// Inject failing pprof starter
	deps.StartPprof = func(addr string) (io.Closer, error) {
		return nil, errors.New("injected pprof bind failure: port in use")
	}

	err := runSoak(context.Background(), cfg, deps, nil)
	if err == nil {
		t.Fatal("expected runSoak to fail when pprof startup fails, got nil")
	}
	if !strings.Contains(err.Error(), "pprof startup failed") {
		t.Fatalf("expected pprof startup failed error, got: %v", err)
	}

	// Verify no evidence was published
	if _, statErr := os.Stat(outDir); !os.IsNotExist(statErr) {
		t.Fatalf("outDir %q should not exist after pprof failure", outDir)
	}
}

// EV-002c: Event write failure must fail the run and prevent complete publication.
func TestEvidence_EV002_InjectedEventWriterFailure(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "soak-out")
	clock := newFakeClock(time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC))

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     200 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		OutDir:       outDir,
		Quiescence:   100 * time.Millisecond,
	}

	deps := defaultSoakDeps()
	deps.Clock = clock
	deps.StartPprof = func(addr string) (io.Closer, error) { return nopCloser{}, nil }
	deps.DialSession = mockSuccessfulDialer(t, clock)

	// Custom EvidenceDeps with injected writer failure
	evCfg := EvidenceConfig{OutDir: outDir, AppID: "test-app", TargetURL: cfg.URL}
	evMgr, err := NewEvidenceManager(evCfg, defaultEvidenceDeps())
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()

	// Inject write error into the manager
	evMgr.customWriteFn = func(b []byte) error {
		if strings.Contains(string(b), "lag_diagnostic") {
			return errors.New("injected disk full during lag diagnostic")
		}
		return nil
	}

	// Calling emit with lag_diagnostic must fail
	emitErr := evMgr.Emit("lag_diagnostic", map[string]any{"p50": "10ms"})
	if emitErr == nil {
		t.Fatal("expected emit error, got nil")
	}
	if evMgr.FirstError() == nil {
		t.Fatal("expected FirstError to be non-nil")
	}

	// Attempting to publish after write error must be rejected
	_, pubErr := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "")
	if pubErr == nil {
		t.Fatal("expected publish to fail after write error, got nil")
	}
	if !strings.Contains(pubErr.Error(), "prior failure") {
		t.Fatalf("expected prior failure error, got: %v", pubErr)
	}
}

// EV-002c: Flush/Close failure must fail the run and prevent complete publication.
func TestEvidence_EV002_InjectedFlushCloseFailure(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "soak-out")

	evCfg := EvidenceConfig{OutDir: outDir, AppID: "test-app", TargetURL: "ws://localhost:8888"}
	evMgr, err := NewEvidenceManager(evCfg, defaultEvidenceDeps())
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()

	_ = evMgr.Emit("run_started", map[string]any{"url": "ws://localhost:8888"})

	// Force close failure by manually closing the file handle first
	_ = evMgr.stagedFile.Close()

	// FlushAndClose should record the error
	err = evMgr.FlushAndClose()
	if err == nil {
		t.Fatal("expected error on flush/close of already closed file, got nil")
	}

	// Publish must refuse to publish complete evidence
	_, pubErr := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "")
	if pubErr == nil {
		t.Fatal("expected publish to fail after flush/close error, got nil")
	}
}

// EV-002d: Rename/publication failure must abort and roll back.
func TestEvidence_EV002_InjectedRenamePublicationFailure(t *testing.T) {
	tempDir := t.TempDir()
	eventsFile := filepath.Join(tempDir, "soak-events.jsonl")

	deps := defaultEvidenceDeps()
	// Inject manifest-link failure after the events link succeeds.
	linkCalls := 0
	deps.LinkFile = func(oldname, newname string) error {
		linkCalls++
		if linkCalls == 2 {
			return errors.New("injected publication link failure")
		}
		return os.Link(oldname, newname)
	}

	evCfg := EvidenceConfig{EventsPath: eventsFile, AppID: "test-app", TargetURL: "ws://localhost:8888"}
	evMgr, err := NewEvidenceManager(evCfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()

	_ = evMgr.Emit("run_started", map[string]any{"url": "ws://localhost:8888"})
	_ = evMgr.FlushAndClose()

	_, pubErr := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "")
	if pubErr == nil {
		t.Fatal("expected publish to fail on rename error, got nil")
	}
	if !strings.Contains(pubErr.Error(), "publish manifest") {
		t.Fatalf("expected manifest publication error, got: %v", pubErr)
	}

	// The events link is retained as explicitly incomplete evidence; without a
	// manifest it cannot be mistaken for a complete bundle.
	if data, readErr := os.ReadFile(eventsFile); readErr != nil || len(data) == 0 {
		t.Fatalf("incomplete events evidence was not retained: err=%v bytes=%d", readErr, len(data))
	}
	if _, statErr := os.Stat(DeriveManifestPath(eventsFile)); !os.IsNotExist(statErr) {
		t.Fatalf("manifest target should not exist after publication failure: %v", statErr)
	}
}

func TestEvidence_EV002_ManifestCollisionLeavesNoCompletionMarker(t *testing.T) {
	tempDir := t.TempDir()
	eventsFile := filepath.Join(tempDir, "collision-events.jsonl")
	manifestPath := DeriveManifestPath(eventsFile)
	deps := defaultEvidenceDeps()
	linkCalls := 0
	deps.LinkFile = func(oldname, newname string) error {
		linkCalls++
		if linkCalls == 2 {
			if err := os.WriteFile(newname, []byte("racing manifest"), 0o600); err != nil {
				return err
			}
		}
		return os.Link(oldname, newname)
	}
	evMgr, err := NewEvidenceManager(EvidenceConfig{EventsPath: eventsFile, AppID: "test-app", TargetURL: "ws://localhost:8888"}, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()
	if err := evMgr.Emit("run_started", map[string]any{"url": "ws://localhost:8888"}); err != nil {
		t.Fatal(err)
	}
	if err := evMgr.FlushAndClose(); err != nil {
		t.Fatal(err)
	}
	if _, err := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "127.0.0.1:18811"); err == nil {
		t.Fatal("expected manifest collision to fail publication")
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("expected racing manifest to remain for reconciliation: %v", err)
	}
	if _, err := os.Stat(DeriveCompletionPath(eventsFile)); !os.IsNotExist(err) {
		t.Fatalf("completion marker must be absent after manifest collision: %v", err)
	}
}

func TestEvidence_EV002_DirectoryPublicationDoesNotOverwriteRacingTarget(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "racing-dir")
	deps := defaultEvidenceDeps()
	deps.Mkdir = func(path string, perm os.FileMode) error {
		if err := os.Mkdir(path, perm); err != nil {
			return err
		}
		return os.Mkdir(path, perm)
	}
	evMgr, err := NewEvidenceManager(EvidenceConfig{OutDir: outDir, AppID: "test-app", TargetURL: "ws://localhost:8888"}, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()
	if err := evMgr.Emit("run_started", map[string]any{"url": "ws://localhost:8888"}); err != nil {
		t.Fatal(err)
	}
	if err := evMgr.FlushAndClose(); err != nil {
		t.Fatal(err)
	}
	if _, err := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "127.0.0.1:18811"); err == nil {
		t.Fatal("expected no-replace directory claim to fail on racing target")
	}
	if _, err := os.Stat(filepath.Join(outDir, "manifest.json")); !os.IsNotExist(err) {
		t.Fatalf("racing directory contains a complete manifest: %v", err)
	}
}

func TestEvidence_EV002_FilePublicationDoesNotOverwriteRacingTarget(t *testing.T) {
	tempDir := t.TempDir()
	target := filepath.Join(tempDir, "events.jsonl")
	deps := defaultEvidenceDeps()
	deps.LinkFile = func(oldname, newname string) error {
		if filepath.Base(oldname) == "events.jsonl" {
			if err := os.WriteFile(newname, []byte("prior evidence"), 0o600); err != nil {
				return err
			}
		}
		return os.Link(oldname, newname)
	}
	evMgr, err := NewEvidenceManager(EvidenceConfig{EventsPath: target}, deps)
	if err != nil {
		t.Fatal(err)
	}
	defer evMgr.Abort()
	if err := evMgr.Emit("run_started", map[string]any{"url": "ws://localhost:8888"}); err != nil {
		t.Fatal(err)
	}
	if err := evMgr.FlushAndClose(); err != nil {
		t.Fatal(err)
	}
	if _, err := evMgr.Publish(SummaryManifest{Success: true}, WorkloadManifest{}, "127.0.0.1:18811"); err == nil {
		t.Fatal("expected no-replace publication to reject racing target")
	}
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "prior evidence" {
		t.Fatalf("racing target was overwritten: %q", got)
	}
}

// EV-002d: Injected permission failure when creating staging must fail immediately.
func TestEvidence_EV002_InjectedPermissionFailure(t *testing.T) {
	tempDir := t.TempDir()
	readOnlyDir := filepath.Join(tempDir, "readonly")
	if err := os.Mkdir(readOnlyDir, 0o500); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(readOnlyDir, 0o700) }() // restore for cleanup

	target := filepath.Join(readOnlyDir, "events.jsonl")
	evCfg := EvidenceConfig{EventsPath: target}
	_, err := NewEvidenceManager(evCfg, defaultEvidenceDeps())
	if err == nil {
		t.Fatal("expected error in read-only directory, got nil")
	}
	if !strings.Contains(err.Error(), "permission") && !strings.Contains(err.Error(), "staging directory") {
		t.Fatalf("expected permission / staging creation error, got: %v", err)
	}
}

// EV-002e: Successful soak in Directory Mode emits completeness manifest with correct hashes.
func TestEvidence_EV002_AtomicPublicationAndCompletenessManifest_DirectoryMode(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "soak-complete-dir")
	clock := realClock{}

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     100 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		OutDir:       outDir,
		Quiescence:   100 * time.Millisecond,
	}

	deps := defaultSoakDeps()
	deps.Clock = clock
	deps.StartPprof = func(addr string) (io.Closer, error) { return nopCloser{}, nil }
	deps.DialSession = mockSuccessfulDialer(t, clock)

	err := runSoak(context.Background(), cfg, deps, nil)
	if err != nil {
		t.Fatalf("expected runSoak success, got: %v", err)
	}

	// 1. Output directory must exist
	fi, err := os.Stat(outDir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("expected output directory at %q, err: %v", outDir, err)
	}

	// 2. events.jsonl must exist
	eventsPath := filepath.Join(outDir, "events.jsonl")
	eventsInfo, err := os.Stat(eventsPath)
	if err != nil || eventsInfo.Size() == 0 {
		t.Fatalf("expected non-empty events.jsonl, err: %v", err)
	}

	// 3. manifest.json must exist
	manifestPath := filepath.Join(outDir, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("expected manifest.json, err: %v", err)
	}

	var manifest CompletenessManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("cannot decode manifest: %v", err)
	}

	if manifest.Status != "complete" || !manifest.Completed {
		t.Fatalf("expected complete status and completed=true, got status=%q completed=%v", manifest.Status, manifest.Completed)
	}
	if manifest.SchemaVersion != 1 {
		t.Fatalf("expected SchemaVersion 1, got %d", manifest.SchemaVersion)
	}
	if !manifest.Summary.Success {
		t.Fatal("expected Summary.Success=true in completeness manifest")
	}
	if manifest.Summary.Transacts == 0 {
		t.Fatalf("expected Transacts > 0, got %d", manifest.Summary.Transacts)
	}

	// Verify artifact entry in manifest
	if len(manifest.Artifacts) == 0 {
		t.Fatal("expected at least 1 artifact in manifest")
	}
	eventsArtifact := manifest.Artifacts[0]
	if eventsArtifact.Name != "events.jsonl" || !eventsArtifact.Required {
		t.Fatalf("unexpected events artifact: %#v", eventsArtifact)
	}
	if eventsArtifact.SizeBytes != eventsInfo.Size() {
		t.Fatalf("manifest size %d != actual size %d", eventsArtifact.SizeBytes, eventsInfo.Size())
	}
	expectedHash, _ := hashFile(eventsPath)
	if eventsArtifact.SHA256 != expectedHash {
		t.Fatalf("manifest sha256 %q != actual %q", eventsArtifact.SHA256, expectedHash)
	}
	if manifest.CompletionMarker != "" {
		t.Fatalf("directory mode should use manifest as completion marker, got %q", manifest.CompletionMarker)
	}

	// Verify no temporary staging directory remained in parent
	parentEntries, _ := os.ReadDir(tempDir)
	for _, entry := range parentEntries {
		if strings.HasPrefix(entry.Name(), ".soak-staging-") {
			t.Fatalf("found lingering staging directory %q", entry.Name())
		}
	}
}

// EV-002e: Successful soak in File Mode emits events file and paired .manifest.json.
func TestEvidence_EV002_AtomicPublicationAndCompletenessManifest_FileMode(t *testing.T) {
	tempDir := t.TempDir()
	eventsFile := filepath.Join(tempDir, "quality-soak-events.jsonl")
	expectedManifest := filepath.Join(tempDir, "quality-soak-events.manifest.json")
	clock := realClock{}

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     100 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		EventsPath:   eventsFile,
		Quiescence:   100 * time.Millisecond,
	}

	deps := defaultSoakDeps()
	deps.Clock = clock
	deps.StartPprof = func(addr string) (io.Closer, error) { return nopCloser{}, nil }
	deps.DialSession = mockSuccessfulDialer(t, clock)

	err := runSoak(context.Background(), cfg, deps, nil)
	if err != nil {
		t.Fatalf("expected runSoak success, got: %v", err)
	}

	// Events file must exist
	if fi, err := os.Stat(eventsFile); err != nil || fi.Size() == 0 {
		t.Fatalf("expected non-empty events file %q, err: %v", eventsFile, err)
	}

	// Manifest file must exist alongside
	manifestData, err := os.ReadFile(expectedManifest)
	if err != nil {
		t.Fatalf("expected paired manifest file at %q: %v", expectedManifest, err)
	}

	var manifest CompletenessManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("cannot decode manifest: %v", err)
	}
	if manifest.Status != "complete" || !manifest.Completed {
		t.Fatalf("expected complete manifest, got status=%q completed=%v", manifest.Status, manifest.Completed)
	}
	if _, err := os.Stat(DeriveCompletionPath(eventsFile)); err != nil {
		t.Fatalf("expected file-mode completion marker: %v", err)
	}
	if manifest.CompletionMarker != filepath.Base(DeriveCompletionPath(eventsFile)) {
		t.Fatalf("manifest completion marker = %q, want %q", manifest.CompletionMarker, filepath.Base(DeriveCompletionPath(eventsFile)))
	}
}

type safeBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func (s *safeBuffer) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Len()
}

// EV-002f: Stdout mode (-events -) is explicitly non-publishing.
func TestEvidence_EV002_StdoutModeNonPublishing(t *testing.T) {
	tempDir := t.TempDir()
	clock := realClock{}

	stdoutBuf := &safeBuffer{}

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     100 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		EventsPath:   "-",
		Quiescence:   100 * time.Millisecond,
	}

	deps := defaultSoakDeps()
	deps.Clock = clock
	deps.StartPprof = func(addr string) (io.Closer, error) { return nopCloser{}, nil }
	deps.DialSession = mockSuccessfulDialer(t, clock)
	deps.Evidence.Stdout = stdoutBuf

	err := runSoak(context.Background(), cfg, deps, nil)
	if err != nil {
		t.Fatalf("expected runSoak success in stdout mode, got: %v", err)
	}

	// Events must have been streamed to stdoutBuf
	if stdoutBuf.Len() == 0 {
		t.Fatal("expected stdout events output, got empty buffer")
	}
	if !strings.Contains(stdoutBuf.String(), `"event":"run_started"`) {
		t.Fatalf("expected run_started in stdout, got: %s", stdoutBuf.String())
	}
	if !strings.Contains(stdoutBuf.String(), `"event":"run_finished"`) {
		t.Fatalf("expected run_finished in stdout, got: %s", stdoutBuf.String())
	}

	// No files must have been created in tempDir
	entries, _ := os.ReadDir(tempDir)
	if len(entries) != 0 {
		t.Fatalf("stdout mode created files on disk: %v", entries)
	}
}

// EV-002: Failed soak run must NOT publish complete manifest to target location.
func TestEvidence_EV002_PartialRunDoesNotPublishCompleteManifest(t *testing.T) {
	tempDir := t.TempDir()
	outDir := filepath.Join(tempDir, "soak-failed-run")
	clock := realClock{}

	cfg := soakConfig{
		URL:          "ws://localhost:8888/runtime/session",
		AppID:        "test-app",
		Sessions:     1,
		Duration:     100 * time.Millisecond,
		TxInterval:   10 * time.Millisecond,
		GlobalTxRate: 10,
		OutDir:       outDir,
		Quiescence:   100 * time.Millisecond,
	}

	deps := defaultSoakDeps()
	deps.Clock = clock
	deps.StartPprof = func(addr string) (io.Closer, error) { return nopCloser{}, nil }
	// DialSession injects a fatal protocol error
	deps.DialSession = func(ctx context.Context, logger *slog.Logger, rawURL, appID string, id int,
		attr *string, txInterval *time.Duration,
		refreshes, transacts, connects *atomic.Int64, writeGate chan struct{},
		ledger *TransactionLedger, quiescence time.Duration,
	) error {
		conn := newFakeSessionConn()
		conn.PushFrame(map[string]any{"op": "protocol-error", "message": "injected fatal server error"})
		return driveSessionWithConn(ctx, conn, ledger, clock, id, appID, attr, txInterval,
			refreshes, transacts, connects, writeGate, quiescence)
	}

	err := runSoak(context.Background(), cfg, deps, nil)
	if err == nil {
		t.Fatal("expected runSoak to fail when protocol error occurs, got nil")
	}

	// Target directory must NOT exist (atomic publication was never performed)
	if _, statErr := os.Stat(outDir); !os.IsNotExist(statErr) {
		t.Fatalf("target %q must not exist after failed soak run", outDir)
	}
}
