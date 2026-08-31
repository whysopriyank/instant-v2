package benchrun

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestChecksumIndexPropagatesWriteFailure(t *testing.T) {
	writer, err := NewArtifactWriter(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteText("data.json", []byte("{}\n")); err != nil {
		t.Fatal(err)
	}
	want := errors.New("checksum destination unavailable")
	reader, destination := io.Pipe()
	if err := reader.CloseWithError(want); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = destination.Close() }()
	err = writeChecksumIndex(destination, writer.Root, []string{"data.json"}, 1<<20, 1<<20)
	if !errors.Is(err, want) {
		t.Fatalf("checksum write error=%v, want %v", err, want)
	}
}

func TestArtifactWriterRedactsAndVerifies(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.WriteJSON("runs/r1/stdout.log", map[string]string{"authorization": "Bearer secret", "token": "abc", "safe": "ok"}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "runs/r1/stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "abc") {
		t.Fatalf("secret was retained: %s", b)
	}
	uri := Redact("postgres://bench:secret@localhost/instant_bench_x")
	if strings.Contains(uri, "secret") || !strings.Contains(uri, "[REDACTED]@") {
		t.Fatalf("URI credentials not redacted: %s", uri)
	}
	for _, header := range []string{"Authorization: Bearer very secret value", "Authorization: Basic dXNlcjpwYXNz"} {
		redacted := Redact(header)
		if strings.Contains(redacted, "secret") || strings.Contains(redacted, "dXNlcjpwYXNz") {
			t.Fatalf("authorization header leaked: %s", redacted)
		}
	}
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
}

func TestRedactConsumesQuotedSecretValues(t *testing.T) {
	input := `dsn='postgres://bench:top secret@localhost/instant_bench_x?note=escaped\'suffix' cookie="session value with spaces\"and suffix" auth: "Bearer value with spaces"`
	redacted := Redact(input)
	for _, leaked := range []string{"top secret", "escaped", "suffix", "session value", "Bearer value"} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("quoted secret leaked %q: %s", leaked, redacted)
		}
	}
}

func TestArtifactWriterRedactionPreservesJSONWithQuotedDSNError(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	record := map[string]any{
		"failure": `unsafe benchmark database DSN: "postgresql://bench:top-secret@127.0.0.1/instant_bench_v1" host must be explicit`,
		"token":   "also-secret",
	}
	if err := w.WriteJSON("target.json", record); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "target.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !json.Valid(b) {
		t.Fatalf("redaction produced invalid JSON: %s", b)
	}
	if strings.Contains(string(b), "top-secret") || strings.Contains(string(b), "also-secret") {
		t.Fatalf("secret was retained: %s", b)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactWriterRejectsUnsafePathsAndCaps(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Write("../escape", []byte("x")); err == nil {
		t.Fatal("path traversal accepted")
	}
	if err := w.Write("big", []byte("1234")); err == nil {
		t.Fatal("size cap accepted oversized file")
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if err := w.Write("link", []byte("x")); err == nil {
		t.Fatal("symlink escape accepted")
	}
}

func TestChecksumRejectsExtraAndTamperAfterFinalize(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if err = w.Write("one.json", []byte("{}")); err != nil {
		t.Fatal(err)
	}
	if err = w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "extra.json"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("extra file was accepted")
	}
	if err = os.Remove(filepath.Join(root, "extra.json")); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "one.json"), []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyChecksums(root); err == nil {
		t.Fatal("tamper was accepted")
	}
}

func TestArtifactWriterConcurrentWritersRemainBounded(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); _ = w.Write(fmt.Sprintf("%02d.json", i), []byte("{}")) }(i)
	}
	wg.Wait()
	if err := w.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
}

func TestArtifactWriterJSONLStreamingCleansPartialAndTracksTotal(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 128)
	if err != nil {
		t.Fatal(err)
	}
	records := make([]any, 0, 100)
	for i := 0; i < cap(records); i++ {
		records = append(records, map[string]int{"index": i, "value": i})
	}
	if err := w.WriteJSONL("runs/oversized.jsonl", records); err == nil {
		t.Fatal("oversized JSONL was accepted")
	}
	if _, err := os.Stat(filepath.Join(root, "runs", "oversized.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("partial JSONL remained: %v", err)
	}
	entries, err := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL files remained: %v %v", entries, err)
	}
	if w.total != 0 {
		t.Fatalf("failed JSONL changed total accounting: %d", w.total)
	}
}

func TestArtifactWriterJSONLStreamsTypedSlice(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	rows := []LedgerRow{{SchemaVersion: SchemaVersion, RunID: "run-typed", PairID: "pair", WriterID: "writer", RecipientID: "recipient", ClientEventID: "event", ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"}, ExpectedMaterializedDigest: "digest", Coverage: "none"}}
	if err := w.WriteJSONL("runs/typed.jsonl", rows); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "runs", "typed.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var got LedgerRow
	if err := json.Unmarshal(bytes.TrimSpace(data), &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != rows[0].RunID || got.RecipientID != rows[0].RecipientID {
		t.Fatalf("typed JSONL row changed: %+v", got)
	}
}

func TestArtifactWriterContractBudgetIsIdempotent(t *testing.T) {
	w, err := NewArtifactWriter(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.ConfigureContractBudget(1024, 512); err != nil {
		t.Fatal(err)
	}
	if err := w.ConfigureContractBudget(1024, 512); err != nil {
		t.Fatalf("identical budget was not idempotent: %v", err)
	}
	if err := w.ConfigureContractBudget(2048, 512); err == nil {
		t.Fatal("different repeated budget was accepted")
	}
}

func TestArtifactWriterJSONLPublicationRollbackOnTempUnlinkFailure(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	removeCalls := 0
	w.removeFile = func(path string) error {
		removeCalls++
		if removeCalls == 1 {
			return errors.New("injected temp unlink failure")
		}
		return os.Remove(path)
	}
	if err := w.WriteJSONL("runs/retry.jsonl", []any{map[string]string{"ok": "value"}}); err == nil {
		t.Fatal("post-link temp unlink failure was hidden")
	}
	if w.total != 0 {
		t.Fatalf("failed publication changed total accounting: %d", w.total)
	}
	if _, err := os.Stat(filepath.Join(root, "runs", "retry.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("published destination survived rollback: %v", err)
	}
	entries, err := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL survived deferred cleanup: %v %v", entries, err)
	}
	w.removeFile = os.Remove
	if err := w.WriteJSONL("runs/retry.jsonl", []any{map[string]string{"ok": "value"}}); err != nil {
		t.Fatalf("retry after rollback failed: %v", err)
	}
	if w.total == 0 {
		t.Fatal("successful retry did not update total accounting")
	}
}

func TestArtifactWriterJSONLRollbackFailureIsReported(t *testing.T) {
	root := t.TempDir()
	w, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	removeCalls := 0
	w.removeFile = func(path string) error {
		removeCalls++
		if removeCalls <= 2 {
			return fmt.Errorf("injected remove failure %d", removeCalls)
		}
		return os.Remove(path)
	}
	err = w.WriteJSONL("runs/rollback-failure.jsonl", []any{map[string]string{"ok": "value"}})
	if err == nil || !strings.Contains(err.Error(), "injected remove failure 1") || !strings.Contains(err.Error(), "injected remove failure 2") {
		t.Fatalf("rollback failures were not both reported: %v", err)
	}
	if w.total != 0 {
		t.Fatalf("failed publication changed total accounting: %d", w.total)
	}
	if _, statErr := os.Stat(filepath.Join(root, "runs", "rollback-failure.jsonl")); statErr != nil {
		t.Fatalf("published destination was unexpectedly removed after rollback failure: %v", statErr)
	}
	entries, globErr := filepath.Glob(filepath.Join(root, "runs", ".benchrun-jsonl-*"))
	if globErr != nil || len(entries) != 0 {
		t.Fatalf("temporary JSONL survived cleanup: %v %v", entries, globErr)
	}
}
