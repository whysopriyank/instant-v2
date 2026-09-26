package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func manifestFixture(t *testing.T) (root string, binary, config ArtifactRef, handoffsPath string) {
	t.Helper()
	root = t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "instantd"), []byte("candidate-binary-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "instantd.env"), []byte("INSTANT_V2_HTTP_ADDR=:8080\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var err error
	binary, err = HashFileArtifact(root, "instantd")
	if err != nil {
		t.Fatal(err)
	}
	config, err = HashFileArtifact(root, "instantd.env")
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"records/native.json", "records/recovery.json", "records/soak.json"} {
		if err := os.MkdirAll(filepath.Join(root, filepath.Dir(rel)), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, rel), []byte(`{"packet":"x"}`), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	handoffsPath = filepath.Join(root, "handoffs-input.json")
	writeHandoffs(t, handoffsPath, nil)
	return root, binary, config, handoffsPath
}

func writeHandoffs(t *testing.T, path string, mutate func([]handoffInput) []handoffInput) {
	t.Helper()
	inputs := make([]handoffInput, 0, len(ExpectedPackets))
	for _, p := range ExpectedPackets {
		state := "GREEN"
		if p == "DA-004V" {
			state = "ACCEPTED_EXCEPTION"
		}
		inputs = append(inputs, handoffInput{Packet: p, State: state, LedgerRef: "ledger-" + p, FinishedAt: "2026-09-20T00:00:00Z"})
	}
	if mutate != nil {
		inputs = mutate(inputs)
	}
	raw, err := json.Marshal(inputs)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func manifestArgs(root string, binary, config ArtifactRef, handoffsPath, out string) []string {
	return []string{
		"--evidence-root", root, "--campaign", "qh001-test", "--candidate", strings.Repeat("a", 40),
		"--binary", binary.Path + ":" + itoa(binary.SizeBytes) + ":" + binary.SHA256,
		"--configuration", config.Path + ":" + itoa(config.SizeBytes) + ":" + config.SHA256,
		"--campaign-started-at", "2026-09-20T00:00:00Z",
		"--native-record", "records/native.json", "--recovery-record", "records/recovery.json",
		"--soak-record", "records/soak.json", "--handoffs", handoffsPath, "--out", out,
	}
}

func TestManifestHappyPath(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code != 0 {
		t.Fatalf("manifest exit=%d", code)
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m gateManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(root, m, strings.Repeat("a", 40), "qh001-test"); err != nil {
		t.Fatal(err)
	}
	if len(m.Handoffs) != len(ExpectedPackets) {
		t.Fatalf("want %d handoffs", len(ExpectedPackets))
	}
	// One handoff file per packet, each binding packet/state/candidate/campaign.
	for _, h := range m.Handoffs {
		got, err := HashFileArtifact(root, h.Path)
		if err != nil {
			t.Fatalf("handoff file %q: %v", h.Path, err)
		}
		if got.SHA256 != h.SHA256 {
			t.Fatalf("handoff %s hash mismatch", h.Packet)
		}
		raw, _ := os.ReadFile(filepath.Join(root, h.Path))
		var hf map[string]any
		if err := json.Unmarshal(raw, &hf); err != nil {
			t.Fatal(err)
		}
		if hf["packet"] != h.Packet || hf["candidate_sha"] != strings.Repeat("a", 40) || hf["campaign_id"] != "qh001-test" {
			t.Fatalf("handoff file content wrong: %v", hf)
		}
		if h.Packet == "OP-003" && hf["record"] != "records/native.json" {
			t.Fatalf("OP-003 handoff must bind the native record: %v", hf)
		}
	}
}

func TestManifestRefusesMissingPacket(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	writeHandoffs(t, handoffsPath, func(in []handoffInput) []handoffInput { return in[:len(in)-1] })
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code == 0 {
		t.Fatal("must refuse a missing packet")
	}
}

func TestManifestRefusesExtraPacket(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	writeHandoffs(t, handoffsPath, func(in []handoffInput) []handoffInput {
		in[0].Packet = "BOGUS-001"
		return in
	})
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code == 0 {
		t.Fatal("must refuse an extra packet")
	}
}

func TestManifestRefusesNonGreenState(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	writeHandoffs(t, handoffsPath, func(in []handoffInput) []handoffInput {
		for i := range in {
			if in[i].Packet == "RT-003" {
				in[i].State = "PARTIAL"
			}
		}
		return in
	})
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code == 0 {
		t.Fatal("must refuse a non-GREEN state")
	}
}

func TestManifestRefusesBinaryMismatch(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	binary.SHA256 = strings.Repeat("0", 64)
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code == 0 {
		t.Fatal("must refuse a binary sha mismatch")
	}
}

func TestManifestRefusesDuplicatePacket(t *testing.T) {
	root, binary, config, handoffsPath := manifestFixture(t)
	writeHandoffs(t, handoffsPath, func(in []handoffInput) []handoffInput {
		in[len(in)-1].Packet = in[0].Packet
		return in
	})
	if code := runManifest(manifestArgs(root, binary, config, handoffsPath, "manifest.json")); code == 0 {
		t.Fatal("must refuse a duplicated packet")
	}
}
