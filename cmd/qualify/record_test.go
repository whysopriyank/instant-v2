package main

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// gatePredicatePins are verbatim substrings of scripts/quality-release-gate.sh.
// This test FAILS if the gate's predicate text changes, forcing the Go mirror
// (validateGateRecord / validateRecoveryDetails / validateSoakDetails /
// validateManifest) to be re-reviewed against the new authority.
var gatePredicatePins = []string{
	// record common contract (verify_record)
	`keys == ["artifacts","binary_sha256","campaign_id","candidate_sha","cleanup","configuration_sha256","details","endpoint_sha256","finished_at","fixture_id","host","packet","result","schema_version","selected_count","skipped_count","started_at"]`,
	// native per-packet check
	`.host.os == "linux" and (.details | type == "object" and keys == ["native","platform_checks"] and .native == true`,
	// recovery per-packet check
	`keys == ["max_drain_seconds","max_rto_seconds","outcomes"]`,
	`["crash-after-commit","crash-before-commit","crash-during-publication","drain-idle","drain-moderate","drain-saturated","postgres-restart"]`,
	// soak per-packet check
	`keys == ["acknowledged_transactions","active_seconds","committed_transactions","dropped_transactions","refreshed_transactions","sessions","unresolved_transactions"]`,
	// manifest schema predicate
	`keys == ["campaign_id","campaign_max_age_seconds","campaign_started_at","candidate","decision_id","external_records","handoffs","lanes","profile","provider_evidence","schema_version"]`,
	`(.external_records | keys == ["native_linux","recovery","soak"])`,
	// manifest selection pins
	`DEC-001-single-node-alpha-20260905`,
	`"OP-003","OP-005","QR-001","QR-003","QR-005"`,
}

func gateScriptPath(t *testing.T) string {
	t.Helper()
	// cmd/qualify → repo root → scripts/quality-release-gate.sh
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(root, "scripts", "quality-release-gate.sh")
	if _, err := os.Stat(p); err != nil {
		t.Skipf("gate script unavailable: %v", err)
	}
	return p
}

func TestGatePredicatePins(t *testing.T) {
	raw, err := os.ReadFile(gateScriptPath(t))
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, pin := range gatePredicatePins {
		if !strings.Contains(text, pin) {
			t.Errorf("gate predicate pin missing (gate changed? re-review Go mirror): %q", pin)
		}
	}
}

// jqPolicyCheck shells produced records through jq using the same per-packet
// predicates the gate enforces (duplicated filter text kept in sync by
// TestGatePredicatePins).
func jqPolicyCheck(t *testing.T, recordPath, filter string) {
	t.Helper()
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("jq is required to check gate predicates")
	}
	cmd := exec.Command("jq", "-e", filter, recordPath)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("record rejected by gate-equivalent jq predicate: %v\n%s", err, out)
	}
}

const jqNativeFilter = `.host.os == "linux" and (.details | type == "object" and keys == ["native","platform_checks"] and .native == true and (.platform_checks|type=="number" and floor==. and .>0))`

const jqRecoveryFilter = `.details | type == "object" and keys == ["max_drain_seconds","max_rto_seconds","outcomes"] and (.max_rto_seconds|type=="number" and floor==. and .>=0 and .<=3600) and (.max_drain_seconds|type=="number" and floor==. and .>=0 and .<=30) and (.outcomes|type=="array" and length==7) and all(.outcomes[]; type=="object" and keys==["id","result"] and (.id|type=="string" and length>0) and .result=="PASS") and ([.outcomes[].id] | sort) == ["crash-after-commit","crash-before-commit","crash-during-publication","drain-idle","drain-moderate","drain-saturated","postgres-restart"]`

const jqSoakFilter = `def uint: type=="number" and floor==. and .>=0; .details | type == "object" and keys == ["acknowledged_transactions","active_seconds","committed_transactions","dropped_transactions","refreshed_transactions","sessions","unresolved_transactions"] and all(.[]; uint) and .sessions >= 500 and .active_seconds >= 900 and .committed_transactions > 0 and .acknowledged_transactions == .committed_transactions and .refreshed_transactions == .committed_transactions and .dropped_transactions == 0 and .unresolved_transactions == 0`

func writeLaneFile(t *testing.T, dir, name string, lr laneResult) string {
	t.Helper()
	raw, err := json.Marshal(lr)
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func testArtifact(t *testing.T, dir, name, content string) ArtifactRef {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, filepath.Dir(name)), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	a, err := HashFileArtifact(dir, name)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestRecordNativeThroughJQ(t *testing.T) {
	dir := t.TempDir()
	plat, ok := checkPlatformCoverage(fullPlatformPass())
	if !ok {
		t.Fatal("fixture must be platform GREEN")
	}
	lane := laneResult{Packet: "OP-003", SelectedCount: 7, SkippedCount: 0, Result: "PASS",
		Details:   map[string]any{"native": true, "platform_checks": float64(plat.Passed)},
		StartedAt: "2026-09-20T00:00:00Z", FinishedAt: "2026-09-20T01:00:00Z"}
	lanePath := writeLaneFile(t, dir, "native-lane.json", lane)
	art := testArtifact(t, dir, "raw/native.log", "native-evidence")
	out := filepath.Join(dir, "records", "native.json")
	code := runRecord([]string{
		"--lane-result", lanePath, "--campaign", "qh001-test", "--candidate", strings.Repeat("a", 40),
		"--binary-sha", strings.Repeat("b", 64), "--config-sha", strings.Repeat("c", 64),
		"--endpoint-sha", CanonicalEndpointSHA256(), "--fixture", "fixture-1",
		"--host-id", "host-1", "--host-os", "linux", "--host-kernel", "6.8.0", "--host-arch", "amd64",
		"--host-runtime", "go1.25.14", "--cleanup", "complete",
		"--artifact", art.Path + ":" + itoa(art.SizeBytes) + ":" + art.SHA256,
		"--out", out,
	})
	if code != 0 {
		t.Fatalf("record native exit=%d", code)
	}
	if err := validateGateRecordFile(out); err != nil {
		t.Fatal(err)
	}
	jqPolicyCheck(t, out, jqNativeFilter)
}

func TestRecordRecoveryThroughJQ(t *testing.T) {
	dir := t.TempDir()
	details, ok := verdictRecovery(passAll())
	if !ok {
		t.Fatal("fixture must be recovery GREEN")
	}
	// verdictRecovery returns map[string]any with int64 budgets; normalize to
	// JSON numbers the way a real lane file carries them.
	raw, _ := json.Marshal(details)
	var norm map[string]any
	if err := json.Unmarshal(raw, &norm); err != nil {
		t.Fatal(err)
	}
	lane := laneResult{Packet: "OP-005", SelectedCount: 7, SkippedCount: 0, Result: "PASS",
		Details: norm, StartedAt: "2026-09-20T00:00:00Z", FinishedAt: "2026-09-20T01:00:00Z"}
	lanePath := writeLaneFile(t, dir, "recovery-lane.json", lane)
	art := testArtifact(t, dir, "raw/recovery.log", "recovery-evidence")
	out := filepath.Join(dir, "records", "recovery.json")
	code := runRecord([]string{
		"--lane-result", lanePath, "--campaign", "qh001-test", "--candidate", strings.Repeat("a", 40),
		"--binary-sha", strings.Repeat("b", 64), "--config-sha", strings.Repeat("c", 64),
		"--endpoint-sha", CanonicalEndpointSHA256(), "--fixture", "fixture-1",
		"--host-id", "host-1", "--host-os", "linux", "--host-kernel", "6.8.0", "--host-arch", "amd64",
		"--host-runtime", "go1.25.14", "--cleanup", "complete",
		"--artifact", art.Path + ":" + itoa(art.SizeBytes) + ":" + art.SHA256,
		"--out", out,
	})
	if code != 0 {
		t.Fatalf("record recovery exit=%d", code)
	}
	if err := validateGateRecordFile(out); err != nil {
		t.Fatal(err)
	}
	jqPolicyCheck(t, out, jqRecoveryFilter)
}

func TestRecordSoakThroughJQ(t *testing.T) {
	dir := t.TempDir()
	lane := laneResult{Packet: "QR-001", SelectedCount: 100, SkippedCount: 0, Result: "PASS",
		Details: map[string]any{
			"sessions": float64(500), "active_seconds": float64(900),
			"committed_transactions": float64(100), "acknowledged_transactions": float64(100),
			"refreshed_transactions": float64(100), "dropped_transactions": float64(0),
			"unresolved_transactions": float64(0),
		},
		StartedAt: "2026-09-20T00:00:00Z", FinishedAt: "2026-09-20T01:00:00Z"}
	lanePath := writeLaneFile(t, dir, "soak-lane.json", lane)
	art := testArtifact(t, dir, "raw/soak.log", "soak-evidence")
	out := filepath.Join(dir, "records", "soak.json")
	code := runRecord([]string{
		"--lane-result", lanePath, "--campaign", "qh001-test", "--candidate", strings.Repeat("a", 40),
		"--binary-sha", strings.Repeat("b", 64), "--config-sha", strings.Repeat("c", 64),
		"--endpoint-sha", CanonicalEndpointSHA256(), "--fixture", "fixture-1",
		"--host-id", "host-1", "--host-os", "linux", "--host-kernel", "6.8.0", "--host-arch", "amd64",
		"--host-runtime", "go1.25.14", "--cleanup", "complete",
		"--artifact", art.Path + ":" + itoa(art.SizeBytes) + ":" + art.SHA256,
		"--out", out,
	})
	if code != 0 {
		t.Fatalf("record soak exit=%d", code)
	}
	if err := validateGateRecordFile(out); err != nil {
		t.Fatal(err)
	}
	jqPolicyCheck(t, out, jqSoakFilter)
}

func TestRecordRefusals(t *testing.T) {
	dir := t.TempDir()
	art := testArtifact(t, dir, "raw/x.log", "x")
	base := []string{
		"--campaign", "qh001-test", "--candidate", strings.Repeat("a", 40),
		"--binary-sha", strings.Repeat("b", 64), "--config-sha", strings.Repeat("c", 64),
		"--fixture", "f", "--host-id", "h", "--host-os", "linux",
		"--artifact", art.Path + ":" + itoa(art.SizeBytes) + ":" + art.SHA256,
	}
	good := laneResult{Packet: "OP-003", SelectedCount: 7, Result: "PASS",
		Details:   map[string]any{"native": true, "platform_checks": float64(7)},
		StartedAt: "2026-09-20T00:00:00Z", FinishedAt: "2026-09-20T01:00:00Z"}
	goodPath := writeLaneFile(t, dir, "good.json", good)

	// cleanup not complete → refuse
	if code := runRecord(append(append([]string{}, base...), "--lane-result", goodPath, "--cleanup", "partial", "--out", filepath.Join(dir, "o1.json"))); code == 0 {
		t.Fatal("must refuse incomplete cleanup")
	}
	// skipped lane → refuse
	skipped := good
	skipped.SkippedCount = 1
	skippedPath := writeLaneFile(t, dir, "skipped.json", skipped)
	if code := runRecord(append(append([]string{}, base...), "--lane-result", skippedPath, "--cleanup", "complete", "--out", filepath.Join(dir, "o2.json"))); code == 0 {
		t.Fatal("must refuse skipped lane")
	}
	// FAIL lane → refuse
	failed := good
	failed.Result = "FAIL"
	failedPath := writeLaneFile(t, dir, "failed.json", failed)
	if code := runRecord(append(append([]string{}, base...), "--lane-result", failedPath, "--cleanup", "complete", "--out", filepath.Join(dir, "o3.json"))); code == 0 {
		t.Fatal("must refuse FAIL lane")
	}
}

func validateGateRecordFile(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var rec gateRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return err
	}
	return validateGateRecord(rec)
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func TestCommandErrorIncludesStderr(t *testing.T) {
	_, err := exec.Command("sh", "-c", "echo 'soaksetup: too many clients' >&2; exit 2").Output()
	got := commandError("soaksetup", err).Error()
	if !strings.Contains(got, "exit status 2") || !strings.Contains(got, "too many clients") {
		t.Fatalf("stderr not carried: %q", got)
	}
}
