//go:build !darwin && !linux

package corpus

import (
	"strings"
	"testing"
)

func TestReserveOutputDirUnsupportedOnOtherPlatform(t *testing.T) {
	_, err := ReserveOutputDir("output/test")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported error on non-Unix platform, got: %v", err)
	}
}

func TestCheckFreshOutputDirUnsupportedOnOtherPlatform(t *testing.T) {
	err := CheckFreshOutputDir("output/test")
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported error on non-Unix platform, got: %v", err)
	}
}

func TestWriteEvidenceUnsupportedOnOtherPlatform(t *testing.T) {
	err := WriteEvidence("output/test/evidence.json", map[string]string{"foo": "bar"})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("expected unsupported error on non-Unix platform, got: %v", err)
	}
}
