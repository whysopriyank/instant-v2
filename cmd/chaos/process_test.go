package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCorpusReplayUsesSourceLayout(t *testing.T) {
	for _, layout := range []string{"cmd", "tools"} {
		t.Run(layout, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, layout, "corpusctl")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte("package main\n"), 0600); err != nil {
				t.Fatal(err)
			}
			command := corpusReplayCommand(root, "ws://127.0.0.1:1")
			if command.Dir != root || command.Args[2] != "./"+layout+"/corpusctl" {
				t.Fatalf("source layout %s: dir=%s args=%v", layout, command.Dir, command.Args)
			}
		})
	}
}

func TestBuildWorkingTreeDoesNotFallbackToHEAD(t *testing.T) {
	called := ""
	out, err := buildWorkingTree("working-tree", filepath.Join(t.TempDir(), "instantd"), func(root, _ string) ([]byte, error) {
		called = root
		return []byte("working tree compiler output"), errors.New("compile failed")
	})
	if err == nil || !strings.Contains(err.Error(), "no HEAD fallback") {
		t.Fatalf("working-tree failure was not propagated: %v", err)
	}
	if called != "working-tree" {
		t.Fatalf("builder used unexpected source: %q", called)
	}
	if string(out) != "working tree compiler output" {
		t.Fatalf("compiler output lost: %q", out)
	}
}

func TestRunCorpusReplayPropagatesSubprocessFailure(t *testing.T) {
	report, err := runCorpusReplayWith(t.TempDir(), "ws://127.0.0.1:1", func(*exec.Cmd) ([]byte, error) {
		return []byte("0/1 scenarios passed\n"), errors.New("exit status 9")
	})
	if err == nil || !strings.Contains(err.Error(), "subprocess failed") {
		t.Fatalf("replay failure was swallowed: %v", err)
	}
	if !strings.Contains(report, "exit status 9") {
		t.Fatalf("replay diagnostics missing: %q", report)
	}
}

func TestRunCorpusReplayRejectsZeroSelectionAndIncompleteSummary(t *testing.T) {
	for name, output := range map[string]string{
		"zero":       "0/0 scenarios passed\n",
		"no-summary": "PASS smoke\n",
		"partial":    "1/2 scenarios passed\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := runCorpusReplayWith(t.TempDir(), "ws://127.0.0.1:1", func(*exec.Cmd) ([]byte, error) {
				return []byte(output), nil
			})
			if err == nil {
				t.Fatalf("invalid replay output accepted: %q", output)
			}
		})
	}
}

func TestRunCorpusReplayAcceptsCompleteSuccess(t *testing.T) {
	report, err := runCorpusReplayWith(t.TempDir(), "ws://127.0.0.1:1", func(*exec.Cmd) ([]byte, error) {
		return []byte("PASS smoke (replay)\n2/2 scenarios passed\n"), nil
	})
	if err != nil {
		t.Fatalf("complete replay rejected: %v\n%s", err, report)
	}
}

func TestFreezeAndThawPropagatesSignalFailures(t *testing.T) {
	old := sendPostmasterSignal
	t.Cleanup(func() { sendPostmasterSignal = old })
	var signals []string
	sendPostmasterSignal = func(signal, pid string) error {
		signals = append(signals, signal+":"+pid)
		return nil
	}
	if err := freezeAndThawPostmaster("123", 0); err != nil {
		t.Fatalf("successful signal sequence rejected: %v", err)
	}
	if got := strings.Join(signals, ","); got != "-STOP:123,-CONT:123" {
		t.Fatalf("unexpected signal sequence: %s", got)
	}

	sendPostmasterSignal = func(signal, _ string) error {
		if signal == "-STOP" {
			return errors.New("stop failed")
		}
		return nil
	}
	if err := freezeAndThawPostmaster("123", 0); err == nil || !strings.Contains(err.Error(), "SIGSTOP") {
		t.Fatalf("SIGSTOP failure swallowed: %v", err)
	}

	sendPostmasterSignal = func(signal, _ string) error {
		if signal == "-CONT" {
			return errors.New("continue failed")
		}
		return nil
	}
	if err := freezeAndThawPostmaster("123", 0); err == nil || !strings.Contains(err.Error(), "SIGCONT") {
		t.Fatalf("SIGCONT failure swallowed: %v", err)
	}
}

func TestWaitTailerPropagatesNonCancellationErrors(t *testing.T) {
	done := make(chan struct{})
	errs := make(chan error, 1)
	errs <- errors.New("tail stream failed")
	close(done)
	if err := waitTailer(done, errs, time.Second); err == nil || !strings.Contains(err.Error(), "tail stream failed") {
		t.Fatalf("tailer error swallowed: %v", err)
	}

	done = make(chan struct{})
	errs = make(chan error, 1)
	errs <- context.Canceled
	close(done)
	if err := waitTailer(done, errs, time.Second); err != nil {
		t.Fatalf("context cancellation reported as failure: %v", err)
	}
}

func TestFingerprintIncludesIgnoredCorpusInputs(t *testing.T) {
	root := t.TempDir()
	inputDir := filepath.Join(root, "corpus", ".tmp")
	if err := os.MkdirAll(inputDir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(inputDir, "replay.ndjson")
	if err := os.WriteFile(path, []byte("first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	fingerprint := func() string {
		h := sha256.New()
		if err := fingerprintReplayInputs(root, h); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%x", h.Sum(nil))
	}
	first := fingerprint()
	if err := os.WriteFile(path, []byte("second\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if second := fingerprint(); first == second {
		t.Fatal("ignored corpus input was omitted from fingerprint")
	}
}

func TestCandidateSnapshotIsolatedAndHashesIgnoredReplayInputs(t *testing.T) {
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "corpus", ".tmp"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "go.mod"), []byte("module candidate\n"), 0600); err != nil {
		t.Fatal(err)
	}
	input := filepath.Join(source, "corpus", ".tmp", "replay.ndjson")
	if err := os.WriteFile(input, []byte("before\n"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot := t.TempDir()
	if err := copyCandidateTree(source, snapshot); err != nil {
		t.Fatalf("copy candidate: %v", err)
	}
	first, err := sha256Directory(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(input, []byte("after\n"), 0600); err != nil {
		t.Fatal(err)
	}
	second, err := sha256Directory(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("mutable source changed immutable snapshot hash")
	}
	b, err := os.ReadFile(filepath.Join(snapshot, "corpus", ".tmp", "replay.ndjson"))
	if err != nil || string(b) != "before\n" {
		t.Fatalf("snapshot did not preserve original replay input: %q err=%v", b, err)
	}
}

func TestPGHelperRejectsReplacedWorkingDirectory(t *testing.T) {
	root := t.TempDir()
	oldRoot := chaosTempRoot
	chaosTempRoot = func() string { return root }
	t.Cleanup(func() { chaosTempRoot = oldRoot })
	data := filepath.Join(root, chaosPGDataPrefix+"helper")
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	identity, err := os.Lstat(data)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(data, filepath.Join(root, "original")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(data, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := pgBinInDir(data, identity, "/does/not/exist", "-D", "."); err == nil || !strings.Contains(err.Error(), "changed before identity-bound") {
		t.Fatalf("replaced working directory was accepted: %v", err)
	}
}

func TestCleanupBeforePASSPropagatesFailure(t *testing.T) {
	called := false
	err := cleanupBeforePASS(false, func() error {
		called = true
		return errors.New("cleanup failed")
	})
	if !called || err == nil || !strings.Contains(err.Error(), "cleanup before PASS") {
		t.Fatalf("cleanup failure was not propagated: called=%v err=%v", called, err)
	}
	if err := cleanupBeforePASS(true, func() error { return errors.New("must be skipped") }); err != nil {
		t.Fatalf("keep mode attempted cleanup: %v", err)
	}
}
