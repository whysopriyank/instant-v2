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
	Fixtures              map[string]fixtureIdentity `json:"fixtures,omitempty"`
	CampaignID            string                     `json:"campaign_id"`
	CampaignMaxAgeSeconds int64                      `json:"campaign_max_age_seconds"`
	CampaignStartedAt     string                     `json:"campaign_started_at"`
	Candidate             manifestCandidate          `json:"candidate"`
	DecisionID            string                     `json:"decision_id"`
	ExternalRecords       map[string]string          `json:"external_records"`
	Handoffs              []manifestHandoff          `json:"handoffs"`
	Lanes                 map[string]string          `json:"lanes"`
	Profile               string                     `json:"profile"`
	ProviderEvidence      string                     `json:"provider_evidence"`
	SchemaVersion         int                        `json:"schema_version"`
}

type manifestCandidate struct {
	ImageDigest    string      `json:"image_digest,omitempty"`
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
	profile := fs.String("profile", ExpectedProfile, "qualification profile")
	policyPath := fs.String("policy", "", "candidate-owned public qualification policy JSON")
	image := fs.String("image-digest", "", "distribution OCI digest (public alpha required)")
	fixturesPath := fs.String("fixtures", "", "public fixture identity map JSON")
	containerRecord := fs.String("container-record", "", "public container record path")
	restoreRecord := fs.String("restore-record", "", "public restore record path")
	differentialRecord := fs.String("v1-differential-record", "", "public v1 differential record path")
	performanceRecord := fs.String("performance-record", "", "public performance record path")
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
	decision, expectedPackets, expectedLanes, err := profileSelection(*profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "manifest:", err)
		return 2
	}
	records := map[string]string{"native_linux": *nativeRecord, "recovery": *recoveryRecord, "soak": *soakRecord}
	var fixtures map[string]fixtureIdentity
	if *profile == PublicProfile {
		if !imageDigest(*image) || *fixturesPath == "" {
			fmt.Fprintln(os.Stderr, "manifest: public alpha requires --image-digest and --fixtures")
			return 2
		}
		records["container"] = *containerRecord
		records["restore"] = *restoreRecord
		policy, err := readPublicPolicy(*policyPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "manifest: public policy:", err)
			return 2
		}
		expectedPackets, expectedLanes, _, err = selectPublicPerformance(policy.Performance, policy.ExternalV1)
		if err != nil {
			return 2
		}
		if policy.ExternalV1 == "run" {
			records["v1_differential"] = *differentialRecord
		} else if *differentialRecord != "" {
			fmt.Fprintln(os.Stderr, "manifest: v1 differential record is not selected")
			return 2
		}
		if policy.Performance == "artifact" {
			records["performance"] = *performanceRecord
		} else if *performanceRecord != "" {
			fmt.Fprintln(os.Stderr, "manifest: performance record is not selected")
			return 2
		}
		b, err := os.ReadFile(*fixturesPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "manifest:", err)
			return 2
		}
		if err := decodeStrict(b, &fixtures); err != nil {
			fmt.Fprintln(os.Stderr, "manifest:", err)
			return 2
		}
		for name := range records {
			if records[name] == "" {
				fmt.Fprintln(os.Stderr, "manifest: missing public record", name)
				return 2
			}
		}
	} else if *image != "" || *fixturesPath != "" || *containerRecord != "" || *restoreRecord != "" || *differentialRecord != "" || *performanceRecord != "" || *policyPath != "" {
		fmt.Fprintln(os.Stderr, "manifest: public arguments require the public profile")
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
	if err := checkPacketSetFor(inputs, expectedPackets); err != nil {
		fmt.Fprintln(os.Stderr, "manifest: refusing:", err)
		return 1
	}
	handoffs := make([]manifestHandoff, 0, len(inputs))
	for _, in := range inputs {
		if in.State != "GREEN" && (in.Packet != "DA-004V" || in.State != "ACCEPTED_EXCEPTION") {
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
			"record": recordPathForPublicPacket(in.Packet, records),
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
		DecisionID: decision, ExternalRecords: records,
		Handoffs: handoffs, Lanes: expectedLanes, Profile: *profile,
		ProviderEvidence: "not_selected", SchemaVersion: 1,
	}
	if *profile == PublicProfile {
		manifest.SchemaVersion = 2
		manifest.Candidate.ImageDigest = *image
		manifest.Fixtures = fixtures
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

// validateManifest mirrors the gate's manifest jq predicate plus the
// per-packet handoff acceptance rules (state, candidate, campaign,
// timestamps) and artifact hash verification.
func validateManifest(evidenceRoot string, m gateManifest, candidate, campaign string) error {
	decision, packets, lanes, err := profileSelection(m.Profile)
	if err != nil {
		return err
	}
	schema := 1
	recordNames := []string{"native_linux", "recovery", "soak"}
	if m.Profile == PublicProfile {
		schema = 2
		var records map[string]string
		packets, lanes, records, err = selectPublicPerformance(m.Lanes["performance"], m.Lanes["external_v1"])
		if err != nil {
			return err
		}
		recordNames = sortedRecordNames(records)
		if !imageDigest(m.Candidate.ImageDigest) || len(m.Fixtures) != len(records) {
			return fmt.Errorf("public image/fixture inventory invalid")
		}
	} else if m.Candidate.ImageDigest != "" || len(m.Fixtures) != 0 {
		return fmt.Errorf("schema-1 manifest has public identities")
	}
	if m.SchemaVersion != schema || m.DecisionID != decision {
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
	if !sameKeys(m.Lanes, lanes) {
		return fmt.Errorf("lane set mismatch")
	}
	if len(m.ExternalRecords) != len(recordNames) {
		return fmt.Errorf("external record inventory mismatch")
	}
	for _, name := range recordNames {
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
		if m.Profile == PublicProfile {
			if _, err := publicIdentityMatches(evidenceRoot, m, name); err != nil {
				return err
			}
		}
	}
	if len(m.Handoffs) != len(packets) {
		return fmt.Errorf("handoff count mismatch")
	}
	seen := map[string]bool{}
	for _, h := range m.Handoffs {
		if seen[h.Packet] {
			return fmt.Errorf("duplicate handoff %q", h.Packet)
		}
		seen[h.Packet] = true
		if h.State != "GREEN" && (h.Packet != "DA-004V" || h.State != "ACCEPTED_EXCEPTION") {
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
	for _, p := range packets {
		if !seen[p] {
			return fmt.Errorf("missing handoff %q", p)
		}
	}
	return nil
}

func recordPathForPublicPacket(packet string, records map[string]string) string {
	for name, p := range publicRecordPackets {
		if p == packet {
			return records[name]
		}
	}
	return ""
}
