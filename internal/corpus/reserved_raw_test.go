//go:build darwin || linux

package corpus

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestWriteRawEvidenceRoundTripWriteOnce proves the raw-byte publisher lands
// exact bytes with mode 0600 and enforces write-once through the pinned
// reservation FD.
func TestWriteRawEvidenceRoundTripWriteOnce(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "raw")
	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()

	payload := []byte("{\"subscriber\":\"query-A\",\"seq\":0,\"frame\":{\"op\":\"init-ok\"}}\n")
	if err := reserved.WriteRawEvidence("fu01-query-a.ndjson", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	back, err := os.ReadFile(filepath.Join(outDir, "fu01-query-a.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(back, payload) {
		t.Fatalf("raw payload mutated: got %q want %q", back, payload)
	}
	fi, err := os.Stat(filepath.Join(outDir, "fu01-query-a.ndjson"))
	if err != nil || !fi.Mode().IsRegular() || fi.Mode().Perm() != 0600 {
		t.Fatalf("evidence mode = %v err=%v; want regular 0600", fi, err)
	}
	if err := reserved.WriteRawEvidence("fu01-query-a.ndjson", payload); err == nil ||
		!strings.Contains(err.Error(), "already exists") {
		t.Fatalf("write-once republish accepted: %v", err)
	}
	if err := reserved.WriteRawEvidence("../escape.ndjson", payload); err == nil {
		t.Fatal("escaping filename accepted")
	}
	if err := reserved.WriteRawEvidence("sub/dir.ndjson", payload); err == nil {
		t.Fatal("nested filename accepted")
	}
}

// TestWriteRawEvidencePostVerificationSwapFailsClosed is the R6
// counterexample: swap the visible directory after identity verification but
// before temporary creation. Publication must fail, the victim must stay
// byte-identical, and no temp or final payload may reach the replacement.
// Descriptor-relative temp creation plus the post-hook parent-FD re-verify
// make this fail closed before openat is ever reached.
func TestWriteRawEvidencePostVerificationSwapFailsClosed(t *testing.T) {
	temp := t.TempDir()
	outDir := filepath.Join(temp, "reserved")
	reserved, err := ReserveOutputDir(outDir)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	defer reserved.Close()

	canary := []byte("{\"v\":1}\n")
	if err := reserved.WriteRawEvidence("canary.json", canary); err != nil {
		t.Fatalf("canary publish: %v", err)
	}
	canaryBefore, err := os.ReadFile(filepath.Join(outDir, "canary.json"))
	if err != nil {
		t.Fatal(err)
	}

	movedVictim := filepath.Join(temp, "victim-moved")
	swapExecuted := false
	hooks := writeHooks{
		beforeTempCreate: func() error {
			if err := os.Rename(outDir, movedVictim); err != nil {
				return err
			}
			if err := os.Mkdir(outDir, 0700); err != nil {
				return err
			}
			swapExecuted = true
			return nil
		},
	}

	payload := []byte("{\"subscriber\":\"query-A\",\"seq\":0,\"frame\":{\"op\":\"init-ok\"}}\n")
	err = writeRawEvidenceInDir(reserved, "payload.ndjson", payload, hooks)
	if !swapExecuted {
		t.Fatal("beforeTempCreate swap hook was not executed")
	}
	if err == nil || (!strings.Contains(err.Error(), "swapped") && !strings.Contains(err.Error(), "replaced")) {
		t.Fatalf("post-verification swap was not rejected: %v", err)
	}

	// Victim stays byte-identical: same canary bytes, no new entries.
	canaryAfter, err := os.ReadFile(filepath.Join(movedVictim, "canary.json"))
	if err != nil || !bytes.Equal(canaryBefore, canaryAfter) {
		t.Fatalf("victim changed across rejected publication: %v", err)
	}
	victimEntries, err := os.ReadDir(movedVictim)
	if err != nil || len(victimEntries) != 1 || victimEntries[0].Name() != "canary.json" {
		t.Fatalf("victim entries changed: %v %v", victimEntries, err)
	}

	// Replacement is clean: no temp residue and no final payload.
	replacementEntries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(replacementEntries) != 0 {
		names := make([]string, 0, len(replacementEntries))
		for _, e := range replacementEntries {
			names = append(names, e.Name())
		}
		t.Fatalf("replacement holds %d entries after rejected publication: %v", len(names), names)
	}
}

// TestWriteRawEvidenceInjectedFailuresNoFinalArtifact mirrors the JSON
// failure discipline for the raw-byte path: write/sync failures publish no
// final artifact, and a directory-sync failure reports the artifact as
// untrusted and incomplete.
func TestWriteRawEvidenceInjectedFailuresNoFinalArtifact(t *testing.T) {
	temp := t.TempDir()
	injectedErr := errors.New("simulated io error")
	payload := []byte("{\"k\":\"v\"}\n")

	for _, tc := range []struct {
		name  string
		hooks writeHooks
	}{
		{"write-failure", writeHooks{beforeWrite: func() error { return injectedErr }}},
		{"sync-failure", writeHooks{beforeSync: func() error { return injectedErr }}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(temp, tc.name)
			targetFile := filepath.Join(dir, "evidence.ndjson")
			err := writeEvidenceWithHooksRaw(t, dir, targetFile, payload, tc.hooks)
			if err == nil || !errors.Is(err, injectedErr) {
				t.Fatalf("expected injected failure, got: %v", err)
			}
			if _, err := os.Stat(targetFile); !os.IsNotExist(err) {
				t.Fatalf("final artifact exists despite %s failure", tc.name)
			}
		})
	}

	t.Run("dir-sync-failure", func(t *testing.T) {
		dir := filepath.Join(temp, "dirsync")
		reserved, err := ReserveOutputDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer reserved.Close()
		err = writeRawEvidenceInDir(reserved, "evidence.ndjson", payload, writeHooks{
			beforeDirSync: func() error { return injectedErr },
		})
		if err == nil || !errors.Is(err, injectedErr) {
			t.Fatalf("expected injected dir sync failure, got: %v", err)
		}
		if !strings.Contains(err.Error(), "untrusted and incomplete") {
			t.Fatalf("dir sync failure does not mark the artifact untrusted: %v", err)
		}
	})
}

// writeEvidenceWithHooksRaw reserves dir and publishes payload through the
// raw-byte path with hooks. It exists because the package-level
// WriteEvidence helper only covers the JSON path.
func writeEvidenceWithHooksRaw(t *testing.T, dir, targetFile string, payload []byte, hooks writeHooks) error {
	t.Helper()
	reserved, err := ReserveOutputDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reserved.Close()
	return writeRawEvidenceInDir(reserved, filepath.Base(targetFile), payload, hooks)
}
