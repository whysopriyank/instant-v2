package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// gateManifest is the exact manifest schema
// scripts/quality-release-gate.sh validates (schema, selection, identity,
// handoff inventory). Keys here must stay identical to that jq predicate.
type gateManifest struct {
	CampaignID            string            `json:"campaign_id"`
	CampaignMaxAgeSeconds int64             `json:"campaign_max_age_seconds"`
	CampaignStartedAt     string            `json:"campaign_started_at"`
	Candidate             manifestCandidate `json:"candidate"`
	DecisionID            string            `json:"decision_id"`
	ExternalRecords       map[string]string `json:"external_records"`
	Handoffs              []manifestHandoff `json:"handoffs"`
	Lanes                 map[string]string `json:"lanes"`
	Profile               string            `json:"profile"`
	ProviderEvidence      string            `json:"provider_evidence"`
	SchemaVersion         int               `json:"schema_version"`
}

type manifestCandidate struct {
	SHA            string      `json:"sha"`
	EndpointSHA256 string      `json:"endpoint_sha256"`
	Binary         ArtifactRef `json:"binary"`
	Configuration  ArtifactRef `json:"configuration"`
}

type manifestHandoff struct {
	CampaignID   string `json:"campaign_id"`
	CandidateSHA string `json:"candidate_sha"`
	FinishedAt   string `json:"finished_at"`
	Packet       string `json:"packet"`
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	SizeBytes    int64  `json:"size_bytes"`
	State        string `json:"state"`
}

// handoffInput is one coordinator-authored handoff row: packet → state plus
// the ledger reference the coordinator binds to finished packet work.
type handoffInput struct {
	Packet     string `json:"packet"`
	State      string `json:"state"`
	LedgerRef  string `json:"ledger_ref"`
	FinishedAt string `json:"finished_at"`
}

func runManifest(args []string) int {
	fs := flag.NewFlagSet("manifest", flag.ContinueOnError)
	evidenceRoot := fs.String("evidence-root", "", "evidence directory all relative paths resolve under (required)")
	campaign := fs.String("campaign", "", "campaign id (required)")
	candidate := fs.String("candidate", "", "40-char candidate SHA (required)")
	binarySpec := fs.String("binary", "", "candidate binary artifact path:size:sha256 (required)")
	configSpec := fs.String("configuration", "", "configuration artifact path:size:sha256 (required)")
	endpointSHA := fs.String("endpoint-sha", "", "endpoint sha256 (default: canonical endpoint hash)")
	campaignStarted := fs.String("campaign-started-at", "", "RFC3339 UTC campaign start (required)")
	maxAge := fs.Int64("max-age-seconds", 86400, "campaign max age seconds (1..604800)")
	nativeRecord := fs.String("native-record", "", "native_linux record path relative to evidence root (required)")
	recoveryRecord := fs.String("recovery-record", "", "recovery record path relative to evidence root (required)")
	soakRecord := fs.String("soak-record", "", "soak record path relative to evidence root (required)")
	handoffsIn := fs.String("handoffs", "", "coordinator-authored handoff input JSON file (required)")
	handoffDir := fs.String("handoff-dir", "handoffs", "handoff file dir relative to evidence root")
	out := fs.String("out", "", "manifest output path relative to evidence root (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *evidenceRoot == "" || *campaign == "" || *candidate == "" || *binarySpec == "" ||
		*configSpec == "" || *campaignStarted == "" || *nativeRecord == "" ||
		*recoveryRecord == "" || *soakRecord == "" || *handoffsIn == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "manifest: --evidence-root, --campaign, --candidate, --binary, --configuration, --campaign-started-at, --native-record, --recovery-record, --soak-record, --handoffs, and --out are required")
		return 2
	}
	if !isHex40(strings.ToLower(*candidate)) {
		fmt.Fprintln(os.Stderr, "manifest: --candidate must be 40 lowercase hex characters")
		return 2
	}
	if *maxAge <= 0 || *maxAge > 604800 {
		fmt.Fprintln(os.Stderr, "manifest: --max-age-seconds must be in 1..604800")
		return 2
	}
	endpoint := *endpointSHA
	if endpoint == "" {
		endpoint = CanonicalEndpointSHA256()
	}
	if !isHex64(strings.ToLower(endpoint)) {
		fmt.Fprintln(os.Stderr, "manifest: --endpoint-sha must be 64 lowercase hex")
		return 2
	}
	binary, err := parseArtifactSpec(*binarySpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manifest: --binary:", err)
		return 2
	}
	config, err := parseArtifactSpec(*configSpec)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manifest: --configuration:", err)
		return 2
	}
	// Verify artifact hashes against the evidence root now (fail closed
	// before writing anything).
	for _, a := range []ArtifactRef{binary, config} {
		got, err := HashFileArtifact(*evidenceRoot, a.Path)
		if err != nil {
			fmt.Fprintln(os.Stderr, "manifest: candidate artifact:", err)
			return 1
		}
		if got.SHA256 != strings.ToLower(a.SHA256) || got.SizeBytes != a.SizeBytes {
			fmt.Fprintf(os.Stderr, "manifest: candidate artifact %q hash/size mismatch\n", a.Path)
			return 1
		}
	}
	raw, err := os.ReadFile(*handoffsIn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manifest:", err)
		return 2
	}
	var inputs []handoffInput
	if err := json.Unmarshal(raw, &inputs); err != nil {
		fmt.Fprintln(os.Stderr, "manifest: handoff input is not JSON:", err)
		return 1
	}
	// Refuse unless the packet set is exactly the gate's expected set.
	if err := checkPacketSet(inputs); err != nil {
		fmt.Fprintln(os.Stderr, "manifest: refusing:", err)
		return 1
	}
	handoffs := make([]manifestHandoff, 0, len(inputs))
	for _, in := range inputs {
		if in.State != "GREEN" && !(in.Packet == "DA-004V" && in.State == "ACCEPTED_EXCEPTION") {
			fmt.Fprintf(os.Stderr, "manifest: refusing: packet %s state %q is not accepted\n", in.Packet, in.State)
			return 1
		}
		if _, err := time.Parse(time.RFC3339, in.FinishedAt); err != nil {
			fmt.Fprintf(os.Stderr, "manifest: refusing: packet %s finished_at not RFC3339\n", in.Packet)
			return 1
		}
		content := formatHandoffFile(*campaign, strings.ToLower(*candidate), in)
		rel := filepath.ToSlash(filepath.Join(*handoffDir, in.Packet))
		full := filepath.Join(*evidenceRoot, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			fmt.Fprintln(os.Stderr, "manifest:", err)
			return 1
		}
		if err := writeJSONFile(full, map[string]any{
			"packet": in.Packet, "state": in.State, "candidate_sha": strings.ToLower(*candidate),
			"campaign_id": *campaign, "ledger_ref": in.LedgerRef, "finished_at": in.FinishedAt,
			"record": recordPathForPacket(in.Packet, *nativeRecord, *recoveryRecord, *soakRecord),
		}); err != nil {
			fmt.Fprintln(os.Stderr, "manifest:", err)
			return 1
		}
		_ = content
		got, err := HashFileArtifact(*evidenceRoot, rel)
		if err != nil {
			fmt.Fprintln(os.Stderr, "manifest:", err)
			return 1
		}
		handoffs = append(handoffs, manifestHandoff{
			CampaignID: *campaign, CandidateSHA: strings.ToLower(*candidate),
			FinishedAt: in.FinishedAt, Packet: in.Packet,
			Path: rel, SHA256: got.SHA256, SizeBytes: got.SizeBytes, State: in.State,
		})
	}
	manifest := gateManifest{
		CampaignID: *campaign, CampaignMaxAgeSeconds: *maxAge, CampaignStartedAt: *campaignStarted,
		Candidate: manifestCandidate{
			SHA: strings.ToLower(*candidate), EndpointSHA256: strings.ToLower(endpoint),
			Binary: binary, Configuration: config,
		},
		DecisionID: ExpectedDecision,
		ExternalRecords: map[string]string{
			"native_linux": *nativeRecord, "recovery": *recoveryRecord, "soak": *soakRecord,
		},
		Handoffs: handoffs, Lanes: ExpectedLanes, Profile: ExpectedProfile,
		ProviderEvidence: "not_selected", SchemaVersion: 1,
	}
	// Self-validation with the same checks as the gate (Go mirror of the jq
	// predicates); the coordinator additionally runs the real gate.
	if err := validateManifest(*evidenceRoot, manifest, strings.ToLower(*candidate), *campaign); err != nil {
		fmt.Fprintln(os.Stderr, "manifest: self-validation failed:", err)
		return 1
	}
	outFull := filepath.Join(*evidenceRoot, filepath.FromSlash(*out))
	if err := checkRelativePath(*out); err != nil {
		fmt.Fprintln(os.Stderr, "manifest: --out:", err)
		return 2
	}
	if err := writeJSONFile(outFull, manifest); err != nil {
		fmt.Fprintln(os.Stderr, "manifest:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "manifest: wrote %s (%d handoffs, self-validated)\n", *out, len(handoffs))
	return 0
}

// formatHandoffFile documents the handoff file content contract (the actual
// bytes are written in runManifest via writeJSONFile).
func formatHandoffFile(campaign, candidate string, in handoffInput) string {
	return fmt.Sprintf("packet=%s state=%s candidate=%s campaign=%s ledger=%s finished=%s",
		in.Packet, in.State, candidate, campaign, in.LedgerRef, in.FinishedAt)
}

// recordPathForPacket binds the handoff file to its external record for the
// three evidence packets; other packets carry no record binding.
func recordPathForPacket(packet, native, recovery, soak string) string {
	switch packet {
	case "OP-003":
		return native
	case "OP-005":
		return recovery
	case "QR-001":
		return soak
	}
	return ""
}

// checkPacketSet refuses when the input packet set differs from the gate's
// expected_packets (missing, extra, or duplicated).
func checkPacketSet(inputs []handoffInput) error {
	if len(inputs) != len(ExpectedPackets) {
		return fmt.Errorf("handoff packet count %d != expected %d", len(inputs), len(ExpectedPackets))
	}
	want := map[string]bool{}
	for _, p := range ExpectedPackets {
		want[p] = true
	}
	seen := map[string]bool{}
	for _, in := range inputs {
		if !want[in.Packet] {
			return fmt.Errorf("unexpected packet %q (not in gate expected_packets)", in.Packet)
		}
		if seen[in.Packet] {
			return fmt.Errorf("duplicate packet %q", in.Packet)
		}
		seen[in.Packet] = true
	}
	return nil
}

// validateManifest mirrors the gate's manifest jq predicate plus the
// per-packet handoff acceptance rules (state, candidate, campaign,
// timestamps) and artifact hash verification.
func validateManifest(evidenceRoot string, m gateManifest, candidate, campaign string) error {
	if m.SchemaVersion != 1 || m.DecisionID != ExpectedDecision || m.Profile != ExpectedProfile {
		return fmt.Errorf("schema/decision/profile mismatch")
	}
	if m.CampaignID != campaign || m.ProviderEvidence != "not_selected" {
		return fmt.Errorf("campaign/provider mismatch")
	}
	if m.CampaignMaxAgeSeconds <= 0 || m.CampaignMaxAgeSeconds > 604800 {
		return fmt.Errorf("campaign_max_age_seconds out of range")
	}
	if _, err := time.Parse(time.RFC3339, m.CampaignStartedAt); err != nil {
		return fmt.Errorf("campaign_started_at not RFC3339")
	}
	if m.Candidate.SHA != candidate || !isHex64(m.Candidate.EndpointSHA256) {
		return fmt.Errorf("candidate sha/endpoint mismatch")
	}
	for name, lanes := range map[string]map[string]string{"lanes": m.Lanes} {
		_ = name
		for k, want := range ExpectedLanes {
			if lanes[k] != want {
				return fmt.Errorf("lane %q = %q, want %q", k, lanes[k], want)
			}
		}
		if len(lanes) != len(ExpectedLanes) {
			return fmt.Errorf("lane set mismatch")
		}
	}
	if len(m.ExternalRecords) != 3 {
		return fmt.Errorf("external_records must have 3 lanes")
	}
	for _, name := range []string{"native_linux", "recovery", "soak"} {
		rel, ok := m.ExternalRecords[name]
		if !ok {
			return fmt.Errorf("external_records missing %q", name)
		}
		if err := checkRelativePath(rel); err != nil {
			return fmt.Errorf("external record %q: %w", name, err)
		}
		if _, err := os.Lstat(filepath.Join(evidenceRoot, filepath.FromSlash(rel))); err != nil {
			return fmt.Errorf("external record %q missing: %w", name, err)
		}
	}
	if len(m.Handoffs) != len(ExpectedPackets) {
		return fmt.Errorf("handoff count mismatch")
	}
	seen := map[string]bool{}
	for _, h := range m.Handoffs {
		if seen[h.Packet] {
			return fmt.Errorf("duplicate handoff %q", h.Packet)
		}
		seen[h.Packet] = true
		if h.State != "GREEN" && !(h.Packet == "DA-004V" && h.State == "ACCEPTED_EXCEPTION") {
			return fmt.Errorf("handoff %s state %q not accepted", h.Packet, h.State)
		}
		if h.CandidateSHA != candidate || h.CampaignID != campaign {
			return fmt.Errorf("handoff %s identity mismatch", h.Packet)
		}
		if _, err := time.Parse(time.RFC3339, h.FinishedAt); err != nil {
			return fmt.Errorf("handoff %s finished_at invalid", h.Packet)
		}
		got, err := HashFileArtifact(evidenceRoot, h.Path)
		if err != nil {
			return fmt.Errorf("handoff %s: %w", h.Packet, err)
		}
		if got.SHA256 != h.SHA256 || got.SizeBytes != h.SizeBytes {
			return fmt.Errorf("handoff %s hash/size mismatch", h.Packet)
		}
	}
	for _, p := range ExpectedPackets {
		if !seen[p] {
			return fmt.Errorf("missing handoff %q", p)
		}
	}
	return nil
}
