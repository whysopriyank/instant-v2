package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// gateRecord is the exact external-record schema
// scripts/quality-release-gate.sh validates in verify_record (common
// contract). Keys here must stay identical to that jq predicate.
type gateRecord struct {
	Artifacts           []ArtifactRef  `json:"artifacts"`
	BinarySHA256        string         `json:"binary_sha256"`
	CampaignID          string         `json:"campaign_id"`
	CandidateSHA        string         `json:"candidate_sha"`
	Cleanup             string         `json:"cleanup"`
	ConfigurationSHA256 string         `json:"configuration_sha256"`
	Details             map[string]any `json:"details"`
	EndpointSHA256      string         `json:"endpoint_sha256"`
	FinishedAt          string         `json:"finished_at"`
	FixtureID           string         `json:"fixture_id"`
	Host                HostIdentity   `json:"host"`
	Packet              string         `json:"packet"`
	Result              string         `json:"result"`
	SchemaVersion       int            `json:"schema_version"`
	SelectedCount       int            `json:"selected_count"`
	SkippedCount        int            `json:"skipped_count"`
	StartedAt           string         `json:"started_at"`
}

// lanePacket maps a lane result packet to its gate packet name (identical,
// kept as a function so the mapping is explicit and tested).
func lanePacket(packet string) (string, error) {
	switch packet {
	case "OP-003", "OP-005", "QR-001":
		return packet, nil
	}
	return "", fmt.Errorf("record: lane packet %q is not a gate external record (want OP-003, OP-005, or QR-001)", packet)
}

type multiArtifact []ArtifactRef

func (m *multiArtifact) String() string { return fmt.Sprint(*m) }

func (m *multiArtifact) Set(spec string) error {
	a, err := parseArtifactSpec(spec)
	if err != nil {
		return err
	}
	*m = append(*m, a)
	return nil
}

func runRecord(args []string) int {
	fs := flag.NewFlagSet("record", flag.ContinueOnError)
	lanePath := fs.String("lane-result", "", "lane result JSON from native|recovery|soak (required)")
	campaign := fs.String("campaign", "", "campaign id (required)")
	candidate := fs.String("candidate", "", "40-char candidate SHA (required)")
	binarySHA := fs.String("binary-sha", "", "candidate binary sha256 (required)")
	configSHA := fs.String("config-sha", "", "configuration sha256 (required)")
	endpointSHA := fs.String("endpoint-sha", "", "endpoint sha256 (default: canonical endpoint hash)")
	fixture := fs.String("fixture", "", "fixture identity (required)")
	hostID := fs.String("host-id", "", "host id (required)")
	hostOS := fs.String("host-os", "", "host os (default: runtime GOOS)")
	hostKernel := fs.String("host-kernel", "", "host kernel (default: detected)")
	hostArch := fs.String("host-arch", "", "host arch (default: runtime GOARCH)")
	hostRuntime := fs.String("host-runtime", "", "host runtime (default: runtime version)")
	cleanup := fs.String("cleanup", "", "cleanup state; must be 'complete' (orchestrator-verified removal)")
	var artifacts multiArtifact
	fs.Var(&artifacts, "artifact", "repeatable path:size:sha256 artifact spec (at least one required)")
	out := fs.String("out", "", "gate record JSON output path (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *lanePath == "" || *campaign == "" || *candidate == "" || *binarySHA == "" ||
		*configSHA == "" || *fixture == "" || *hostID == "" || *out == "" {
		fmt.Fprintln(os.Stderr, "record: --lane-result, --campaign, --candidate, --binary-sha, --config-sha, --fixture, --host-id, and --out are required")
		return 2
	}
	if !isHex40(strings.ToLower(*candidate)) {
		fmt.Fprintln(os.Stderr, "record: --candidate must be 40 lowercase hex characters")
		return 2
	}
	if !isHex64(strings.ToLower(*binarySHA)) || !isHex64(strings.ToLower(*configSHA)) {
		fmt.Fprintln(os.Stderr, "record: --binary-sha and --config-sha must be 64 lowercase hex")
		return 2
	}
	endpoint := *endpointSHA
	if endpoint == "" {
		endpoint = CanonicalEndpointSHA256()
	}
	if !isHex64(strings.ToLower(endpoint)) {
		fmt.Fprintln(os.Stderr, "record: --endpoint-sha must be 64 lowercase hex")
		return 2
	}
	if *cleanup != "complete" {
		fmt.Fprintln(os.Stderr, "record: refusing: cleanup is not verified complete (orchestrator must verify removal first)")
		return 1
	}
	if len(artifacts) == 0 {
		fmt.Fprintln(os.Stderr, "record: at least one --artifact is required")
		return 2
	}
	raw, err := os.ReadFile(*lanePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		return 2
	}
	var lr laneResult
	if err := json.Unmarshal(raw, &lr); err != nil {
		fmt.Fprintln(os.Stderr, "record: lane result is not JSON:", err)
		return 1
	}
	packet, err := lanePacket(lr.Packet)
	if err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		return 1
	}
	if lr.Result != "PASS" {
		fmt.Fprintln(os.Stderr, "record: refusing: lane result is not PASS")
		return 1
	}
	if lr.SkippedCount != 0 {
		fmt.Fprintln(os.Stderr, "record: refusing: lane skipped_count != 0")
		return 1
	}
	host := LocalHostIdentity(*hostID)
	if *hostOS != "" {
		host.OS = *hostOS
	}
	if *hostKernel != "" {
		host.Kernel = *hostKernel
	}
	if *hostArch != "" {
		host.Arch = *hostArch
	}
	if *hostRuntime != "" {
		host.Runtime = *hostRuntime
	}
	// Project lane details onto exactly the key set the gate requires per
	// packet. Lane-local provenance (e.g. soak derived_from_ledger notes)
	// stays in the lane result file, which the record references as an
	// artifact; it must not leak into gate details (keys == [...] is exact).
	details, err := projectDetails(packet, lr.Details)
	if err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		return 1
	}
	rec := gateRecord{
		Artifacts: artifacts, BinarySHA256: strings.ToLower(*binarySHA),
		CampaignID: *campaign, CandidateSHA: strings.ToLower(*candidate),
		Cleanup: "complete", ConfigurationSHA256: strings.ToLower(*configSHA),
		Details: details, EndpointSHA256: strings.ToLower(endpoint),
		FinishedAt: lr.FinishedAt, FixtureID: *fixture, Host: host,
		Packet: packet, Result: "PASS", SchemaVersion: 1,
		SelectedCount: lr.SelectedCount, SkippedCount: 0, StartedAt: lr.StartedAt,
	}
	if err := validateGateRecord(rec); err != nil {
		fmt.Fprintln(os.Stderr, "record: assembled record fails gate contract:", err)
		return 1
	}
	if err := writeJSONFile(*out, rec); err != nil {
		fmt.Fprintln(os.Stderr, "record:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "record: wrote %s packet=%s selected=%d\n", *out, packet, lr.SelectedCount)
	return 0
}

// validateGateRecord mirrors the gate's common-contract jq predicate in Go
// (the coordinator additionally runs the real gate). It deliberately
// duplicates the predicate text-adjacent logic; gate_predicate_test.go fails
// if scripts/quality-release-gate.sh changes its record predicate.
func validateGateRecord(r gateRecord) error {
	if r.SchemaVersion != 1 {
		return fmt.Errorf("schema_version != 1")
	}
	if r.Packet != "OP-003" && r.Packet != "OP-005" && r.Packet != "QR-001" {
		return fmt.Errorf("packet %q not external", r.Packet)
	}
	if r.Result != "PASS" || r.Cleanup != "complete" || r.SkippedCount != 0 || r.SelectedCount <= 0 {
		return fmt.Errorf("result/cleanup/skipped/selected contract violated")
	}
	for _, s := range []string{r.CampaignID, r.CandidateSHA, r.FixtureID, r.StartedAt, r.FinishedAt} {
		if s == "" {
			return fmt.Errorf("empty required string field")
		}
	}
	if !isHex40(r.CandidateSHA) || !isHex64(r.BinarySHA256) || !isHex64(r.ConfigurationSHA256) || !isHex64(r.EndpointSHA256) {
		return fmt.Errorf("identity hash contract violated")
	}
	for _, s := range []string{r.Host.ID, r.Host.OS, r.Host.Kernel, r.Host.Arch, r.Host.Runtime} {
		if s == "" {
			return fmt.Errorf("empty host field")
		}
	}
	if len(r.Artifacts) == 0 {
		return fmt.Errorf("at least one artifact required")
	}
	for _, a := range r.Artifacts {
		if err := checkRelativePath(a.Path); err != nil {
			return err
		}
		if !isHex64(a.SHA256) || a.SizeBytes <= 0 {
			return fmt.Errorf("malformed artifact entry")
		}
	}
	switch r.Packet {
	case "OP-003":
		native, _ := r.Details["native"].(bool)
		pc, _ := r.Details["platform_checks"].(float64)
		if r.Host.OS != "linux" || !native || pc <= 0 || float64(int(pc)) != pc {
			return fmt.Errorf("native Linux evidence insufficient")
		}
	case "OP-005":
		if err := validateRecoveryDetails(r.Details); err != nil {
			return err
		}
	case "QR-001":
		if err := validateSoakDetails(r.Details); err != nil {
			return err
		}
	}
	if _, err := time.Parse(time.RFC3339, r.StartedAt); err != nil {
		return fmt.Errorf("started_at not RFC3339")
	}
	if _, err := time.Parse(time.RFC3339, r.FinishedAt); err != nil {
		return fmt.Errorf("finished_at not RFC3339")
	}
	return nil
}

func validateRecoveryDetails(d map[string]any) error {
	if len(d) != 3 {
		return fmt.Errorf("recovery details must have exactly max_drain_seconds, max_rto_seconds, outcomes")
	}
	rto, _ := d["max_rto_seconds"].(float64)
	drain, _ := d["max_drain_seconds"].(float64)
	if float64(int(rto)) != rto || rto < 0 || rto > 3600 {
		return fmt.Errorf("max_rto_seconds out of [0,3600]")
	}
	if float64(int(drain)) != drain || drain < 0 || drain > 30 {
		return fmt.Errorf("max_drain_seconds out of [0,30]")
	}
	outcomes, ok := d["outcomes"].([]any)
	if !ok || len(outcomes) != 7 {
		return fmt.Errorf("outcomes must be an array of 7")
	}
	var ids []string
	for _, o := range outcomes {
		m, ok := o.(map[string]any)
		if !ok || len(m) != 2 {
			return fmt.Errorf("outcome must be exactly {id,result}")
		}
		id, _ := m["id"].(string)
		res, _ := m["result"].(string)
		if id == "" || res != "PASS" {
			return fmt.Errorf("outcome %q is not PASS", id)
		}
		ids = append(ids, id)
	}
	got := sortedCopy(ids)
	want := sortedCopy(RecoveryOutcomeIDs)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		return fmt.Errorf("outcome id set mismatch: got %v", got)
	}
	return nil
}

func validateSoakDetails(d map[string]any) error {
	want := []string{"acknowledged_transactions", "active_seconds", "committed_transactions", "dropped_transactions", "refreshed_transactions", "sessions", "unresolved_transactions"}
	if len(d) < len(want) {
		return fmt.Errorf("soak details missing keys")
	}
	nums := map[string]int64{}
	for _, k := range want {
		v, ok := d[k]
		if !ok {
			return fmt.Errorf("soak details missing %q", k)
		}
		var n int64
		switch t := v.(type) {
		case float64:
			if float64(int64(t)) != t || t < 0 {
				return fmt.Errorf("soak detail %q not a uint", k)
			}
			n = int64(t)
		case int:
			if t < 0 {
				return fmt.Errorf("soak detail %q not a uint", k)
			}
			n = int64(t)
		case int64:
			if t < 0 {
				return fmt.Errorf("soak detail %q not a uint", k)
			}
			n = t
		default:
			return fmt.Errorf("soak detail %q not a number", k)
		}
		nums[k] = n
	}
	if nums["sessions"] < 500 || nums["active_seconds"] < 900 || nums["committed_transactions"] <= 0 ||
		nums["acknowledged_transactions"] != nums["committed_transactions"] ||
		nums["refreshed_transactions"] != nums["committed_transactions"] ||
		nums["dropped_transactions"] != 0 || nums["unresolved_transactions"] != 0 {
		return fmt.Errorf("qualified soak budgets violated: %+v", nums)
	}
	return nil
}

// projectDetails copies exactly the gate-required keys for packet from the
// lane details, refusing when a required key is absent.
func projectDetails(packet string, lane map[string]any) (map[string]any, error) {
	var keys []string
	switch packet {
	case "OP-003":
		keys = []string{"native", "platform_checks"}
	case "OP-005":
		keys = []string{"max_drain_seconds", "max_rto_seconds", "outcomes"}
	case "QR-001":
		keys = []string{"acknowledged_transactions", "active_seconds", "committed_transactions", "dropped_transactions", "refreshed_transactions", "sessions", "unresolved_transactions"}
	default:
		return nil, fmt.Errorf("unknown packet %q", packet)
	}
	out := map[string]any{}
	for _, k := range keys {
		v, ok := lane[k]
		if !ok {
			return nil, fmt.Errorf("lane details lack required key %q", k)
		}
		out[k] = v
	}
	if packet == "OP-005" {
		// The gate requires outcomes[] entries with exactly {id,result}:
		// normalize (strip any lane-local diagnostic keys such as reason)
		// so the record can never carry a key the predicate rejects.
		rawOutcomes, ok := out["outcomes"].([]map[string]any)
		if !ok {
			if anyOutcomes, ok := out["outcomes"].([]any); ok {
				rawOutcomes = nil
				for _, item := range anyOutcomes {
					m, ok := item.(map[string]any)
					if !ok {
						return nil, fmt.Errorf("outcome must be {id,result}")
					}
					rawOutcomes = append(rawOutcomes, m)
				}
			} else {
				return nil, fmt.Errorf("outcomes must be an array")
			}
		}
		norm := make([]any, 0, len(rawOutcomes))
		for _, m := range rawOutcomes {
			id, _ := m["id"].(string)
			res, _ := m["result"].(string)
			if id == "" || res == "" {
				return nil, fmt.Errorf("outcome must be {id,result}")
			}
			norm = append(norm, map[string]any{"id": id, "result": res})
		}
		out["outcomes"] = norm
	}
	return out, nil
}

func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
