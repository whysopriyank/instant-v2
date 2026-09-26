package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

func TestCF003ExceptionsCurrentCorpusAndEnvelopePass(t *testing.T) {
	if err := ValidateCF003Exceptions("../../corpus", "../../docs/reference/release-envelope.md"); err != nil {
		t.Fatalf("current corpus + envelope rejected: %v", err)
	}
}

func writeCF003Manifest(t *testing.T, m *corpus.Manifest) string {
	t.Helper()
	dir := t.TempDir()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

func loadRealCF003Manifest(t *testing.T) *corpus.Manifest {
	t.Helper()
	b, err := os.ReadFile("../../corpus/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var m corpus.Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return &m
}

func TestCF003ExceptionsExtraUnlistedGapFails(t *testing.T) {
	m := loadRealCF003Manifest(t)
	m.Coverage = append(m.Coverage, corpus.CoverageEntry{ID: "extra-unlisted-gap", Status: "gap"})
	dir := writeCF003Manifest(t, m)
	err := ValidateCF003Exceptions(dir, "../../docs/reference/release-envelope.md")
	if err == nil || !strings.Contains(err.Error(), "extra-unlisted-gap") || !strings.Contains(err.Error(), "not listed") {
		t.Fatalf("extra unlisted gap accepted or wrong reason: %v", err)
	}
}

func TestCF003ExceptionsListedCoveredRowFails(t *testing.T) {
	m := loadRealCF003Manifest(t)
	for i, entry := range m.Coverage {
		if entry.ID == "refresh-delta-boundary" {
			m.Coverage[i].Status = "covered"
		}
	}
	dir := writeCF003Manifest(t, m)
	err := ValidateCF003Exceptions(dir, "../../docs/reference/release-envelope.md")
	if err == nil || !strings.Contains(err.Error(), "refresh-delta-boundary") || !strings.Contains(err.Error(), "is not gap") {
		t.Fatalf("covered listed row accepted or wrong reason: %v", err)
	}
}

func writeCF003EnvelopeVariant(t *testing.T, transform func(string) string) string {
	t.Helper()
	b, err := os.ReadFile("../../docs/reference/release-envelope.md")
	if err != nil {
		t.Fatal(err)
	}
	out := transform(string(b))
	path := filepath.Join(t.TempDir(), "release-envelope.md")
	if err := os.WriteFile(path, []byte(out), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCF003ExceptionsUnknownRowFails(t *testing.T) {
	envelope := writeCF003EnvelopeVariant(t, func(s string) string {
		return strings.Replace(s, "| `transactions-concurrency-gap` |", "| `transactions-concurrency-gap` |\n| `no-such-row` | test |", 1)
	})
	err := ValidateCF003Exceptions("../../corpus", envelope)
	if err == nil || !strings.Contains(err.Error(), "no-such-row") || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("unknown exception row accepted or wrong reason: %v", err)
	}
}

func TestCF003ExceptionsRemovedEntryFails(t *testing.T) {
	envelope := writeCF003EnvelopeVariant(t, func(s string) string {
		var kept []string
		for _, line := range strings.Split(s, "\n") {
			if strings.Contains(line, "`rooms-presence-lifecycle`") {
				continue
			}
			kept = append(kept, line)
		}
		return strings.Join(kept, "\n")
	})
	err := ValidateCF003Exceptions("../../corpus", envelope)
	if err == nil || !strings.Contains(err.Error(), "rooms-presence-lifecycle") || !strings.Contains(err.Error(), "not listed") {
		t.Fatalf("removed exception entry accepted or wrong reason: %v", err)
	}
}

func TestCF003SurfaceExceptionsUnlistedGapFails(t *testing.T) {
	m := loadRealCF003Manifest(t)
	m.Surfaces = append(m.Surfaces, corpus.Surface{ID: "extra.unlisted.surface", Status: "gap", Note: "test"})
	dir := writeCF003Manifest(t, m)
	err := ValidateCF003Exceptions(dir, "../../docs/reference/release-envelope.md")
	if err == nil || !strings.Contains(err.Error(), "extra.unlisted.surface") || !strings.Contains(err.Error(), "surface gap") {
		t.Fatalf("unlisted gap surface accepted or wrong reason: %v", err)
	}
}

func TestCF003SurfaceExceptionsStaleEntryFails(t *testing.T) {
	m := loadRealCF003Manifest(t)
	for i, surface := range m.Surfaces {
		if surface.ID == "ws.rooms.fanout" {
			m.Surfaces[i].Status = "covered"
		}
	}
	dir := writeCF003Manifest(t, m)
	err := ValidateCF003Exceptions(dir, "../../docs/reference/release-envelope.md")
	if err == nil || !strings.Contains(err.Error(), "ws.rooms.fanout") || !strings.Contains(err.Error(), "is not gap") {
		t.Fatalf("covered listed surface accepted or wrong reason: %v", err)
	}
}

func TestCF003SurfaceExceptionsMissingSectionFails(t *testing.T) {
	envelope := writeCF003EnvelopeVariant(t, func(s string) string {
		return strings.Replace(s, "## CF-003 surface gap exceptions", "## Something else", 1)
	})
	err := ValidateCF003Exceptions("../../corpus", envelope)
	if err == nil || !strings.Contains(err.Error(), "surface gap exceptions") || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("missing surface section accepted or wrong reason: %v", err)
	}
}
