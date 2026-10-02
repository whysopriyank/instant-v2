package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
)

const PublicProfile = "single-node-public-alpha"
const PublicDecision = "DEC-002-single-node-public-alpha-20261002"

var PublicPackets = append(append([]string{}, ExpectedPackets...),
	"CF-004", "CF-005", "DA-009", "DA-010", "OP-004", "OP-004D", "OP-006", "QR-003R", "QR-004")

var PublicLanes = map[string]string{
	"artifact": "validate", "container": "run", "corpus": "run", "external_v1": "run",
	"hermetic": "run", "owned_db": "run", "performance": "not_selected", "recovery": "artifact", "soak": "artifact",
}

var publicRecordPackets = map[string]string{
	"native_linux": "OP-003", "recovery": "OP-005", "soak": "QR-001",
	"container": "OP-004", "restore": "OP-006", "v1_differential": "CF-005", "performance": "QR-002",
}

type fixtureIdentity struct {
	ID     string `json:"id"`
	SHA256 string `json:"sha256"`
}

func profileSelection(profile string) (decision string, packets []string, lanes map[string]string, err error) {
	switch profile {
	case ExpectedProfile:
		return ExpectedDecision, ExpectedPackets, ExpectedLanes, nil
	case PublicProfile:
		return PublicDecision, PublicPackets, PublicLanes, nil
	default:
		return "", nil, nil, fmt.Errorf("unknown qualification profile %q", profile)
	}
}

type publicPolicy struct {
	DecisionID         string `json:"decision_id"`
	Profile            string `json:"profile"`
	Performance        string `json:"performance"`
	ReleaseVersion     string `json:"release_version"`
	ExternalV1         string `json:"external_v1"`
	ExternalV1Approval string `json:"external_v1_approval,omitempty"`
	ExternalV1Scope    string `json:"external_v1_scope,omitempty"`
}

func selectPublicPerformance(performance, externalV1 string) ([]string, map[string]string, map[string]string, error) {
	packets := append([]string{}, PublicPackets...)
	lanes := map[string]string{}
	records := map[string]string{}
	for k, v := range PublicLanes {
		lanes[k] = v
	}
	for k, v := range publicRecordPackets {
		if k != "performance" {
			records[k] = v
		}
	}
	if externalV1 == "not_selected" {
		lanes["external_v1"] = "not_selected"
		delete(records, "v1_differential")
		selected := packets[:0]
		for _, packet := range packets {
			if packet != "CF-004" && packet != "CF-005" {
				selected = append(selected, packet)
			}
		}
		packets = selected
	} else if externalV1 != "run" {
		return nil, nil, nil, fmt.Errorf("invalid public external_v1 selection")
	}
	if performance == "artifact" {
		packets = append(packets, "EV-007", "QR-002")
		lanes["performance"] = "artifact"
		records["performance"] = "QR-002"
	} else if performance != "not_selected" {
		return nil, nil, nil, fmt.Errorf("invalid public performance selection")
	}
	return packets, lanes, records, nil
}

func readPublicPolicy(path string) (publicPolicy, error) {
	var p publicPolicy
	b, err := os.ReadFile(path)
	if err != nil {
		return p, err
	}
	if err = strictPublicJSON(b, &p); err != nil {
		return p, err
	}
	if p.DecisionID != PublicDecision || p.Profile != PublicProfile || !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+-alpha\.[0-9]+$`).MatchString(p.ReleaseVersion) {
		return p, fmt.Errorf("public policy decision/profile mismatch")
	}
	if p.ExternalV1 == "not_selected" {
		if p.ExternalV1Approval != "EXCLUDED_APPROVED" || p.ExternalV1Scope != "Document explicit compatibility limits; no v1 parity claim" {
			return p, fmt.Errorf("public external_v1 exclusion lacks approved owner scope")
		}
	} else if p.ExternalV1Approval != "" || p.ExternalV1Scope != "" {
		return p, fmt.Errorf("selected external_v1 may not carry an exclusion")
	}
	_, _, _, err = selectPublicPerformance(p.Performance, p.ExternalV1)
	return p, err
}

func publicArtifact(root, path string) (ArtifactRef, error) {
	if err := checkRelativePath(path); err != nil {
		return ArtifactRef{}, err
	}
	current := root
	for _, part := range strings.Split(path, "/") {
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return ArtifactRef{}, err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return ArtifactRef{}, fmt.Errorf("public artifact may not traverse symlinks")
		}
	}
	return HashFileArtifact(root, path)
}

func imageDigest(s string) bool {
	return strings.HasPrefix(s, "sha256:") && isHex64(strings.TrimPrefix(s, "sha256:"))
}

func publicIdentityMatches(root string, m gateManifest, name string) (gateRecord, error) {
	var r gateRecord
	path, ok := m.ExternalRecords[name]
	if !ok {
		return r, fmt.Errorf("missing public record %q", name)
	}
	if err := checkRelativePath(path); err != nil {
		return r, err
	}
	if _, err := publicArtifact(root, path); err != nil {
		return r, err
	}
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		return r, err
	}
	if err := strictPublicJSON(b, &r); err != nil {
		return r, err
	}
	f, ok := m.Fixtures[name]
	if !ok || f.ID == "" || !isHex64(f.SHA256) {
		return r, fmt.Errorf("public fixture %q is missing or malformed", name)
	}
	if r.SchemaVersion != 2 || r.Packet != publicRecordPackets[name] ||
		r.CampaignID != m.CampaignID || r.CandidateSHA != m.Candidate.SHA ||
		r.BinarySHA256 != m.Candidate.Binary.SHA256 || r.ConfigurationSHA256 != m.Candidate.Configuration.SHA256 ||
		r.EndpointSHA256 != m.Candidate.EndpointSHA256 || r.ImageDigest != m.Candidate.ImageDigest ||
		r.FixtureID != f.ID || r.FixtureSHA256 != f.SHA256 {
		return r, fmt.Errorf("public record %q identity mismatch", name)
	}
	if err := validateGateRecord(r); err != nil {
		return r, fmt.Errorf("public record %q: %w", name, err)
	}
	fixtureBound := false
	for _, a := range r.Artifacts {
		if a.SHA256 == f.SHA256 {
			fixtureBound = true
		}
		got, err := publicArtifact(root, a.Path)
		if err != nil || got != a {
			return r, fmt.Errorf("public record %q artifact %q hash/size mismatch", name, a.Path)
		}
	}
	if !fixtureBound {
		return r, fmt.Errorf("public fixture descriptor is absent from hashed artifacts")
	}
	if r.Packet == "OP-004" || r.Packet == "OP-006" || r.Packet == "CF-005" || r.Packet == "QR-002" {
		if err := validatePublicSource(root, r); err != nil {
			return r, fmt.Errorf("public record %q source: %w", name, err)
		}
	}
	return r, nil
}

func checkPacketSetFor(inputs []handoffInput, expected []string) error {
	if len(inputs) != len(expected) {
		return fmt.Errorf("handoff packet count %d != expected %d", len(inputs), len(expected))
	}
	want := map[string]bool{}
	for _, p := range expected {
		want[p] = true
	}
	for _, in := range inputs {
		if !want[in.Packet] {
			return fmt.Errorf("unexpected or duplicate packet %q", in.Packet)
		}
		delete(want, in.Packet)
	}
	return nil
}

func sameKeys(m map[string]string, expected map[string]string) bool {
	return reflect.DeepEqual(m, expected)
}

func sortedRecordNames(m map[string]string) []string {
	k := make([]string, 0, len(m))
	for s := range m {
		k = append(k, s)
	}
	sort.Strings(k)
	return k
}
