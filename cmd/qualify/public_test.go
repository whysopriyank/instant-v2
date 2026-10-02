package main

import (
	"encoding/json"
	"github.com/instant-v2/instant-v2/internal/corpus"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func publicContainerFacts() map[string]any {
	return map[string]any{
		"schema_version": 1, "measurement_class": "live",
		"identity": map[string]any{"candidate_sha": strings.Repeat("a", 40), "binary_sha256": strings.Repeat("b", 64),
			"configuration_sha256": strings.Repeat("c", 64), "endpoint_sha256": CanonicalEndpointSHA256(),
			"image_digest": "sha256:" + strings.Repeat("d", 64), "fixture_id": "fixture-1", "fixture_sha256": strings.Repeat("e", 64)},
		"started_at": "2026-10-02T00:00:00Z", "finished_at": "2026-10-02T00:01:00Z",
		"observations": map[string]any{"uid": 65532, "health_status": 200, "ready_status": 200,
			"tls_verified": true, "transact_acked": true, "subscription_converged": true,
			"object_sha256_before": strings.Repeat("f", 64), "object_sha256_after": strings.Repeat("f", 64),
			"drain_seconds": 1, "port_released": true},
	}
}

func TestPublicContainerProducer(t *testing.T) {
	dir := t.TempDir()
	b := marshalPublicTest(t, publicContainerFacts())
	testArtifact(t, dir, "container/facts.json", string(b))
	if code := run([]string{"container", "--evidence-root", dir, "--facts", "container/facts.json", "--out", filepath.Join(dir, "lane.json")}); code != 0 {
		t.Fatalf("concrete live container observations rejected: exit=%d", code)
	}
	var lr laneResult
	b, err := os.ReadFile(filepath.Join(dir, "lane.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &lr); err != nil || lr.Result != "PASS" || lr.SelectedCount != 9 || lr.SkippedCount != 0 {
		t.Fatalf("container verdict not derived from complete observations: %s", b)
	}
}

func TestPublicContainerProducerRejectsInvalidObservations(t *testing.T) {
	cases := map[string]func(map[string]any){
		"synthetic":      func(f map[string]any) { f["measurement_class"] = "synthetic" },
		"arbitrary PASS": func(f map[string]any) { f["observations"] = map[string]any{"result": "PASS"} },
		"root":           func(f map[string]any) { f["observations"].(map[string]any)["uid"] = 0 },
		"storage changed": func(f map[string]any) {
			f["observations"].(map[string]any)["object_sha256_after"] = strings.Repeat("0", 64)
		},
		"TLS failure":     func(f map[string]any) { f["observations"].(map[string]any)["tls_verified"] = false },
		"missing drain":   func(f map[string]any) { delete(f["observations"].(map[string]any), "drain_seconds") },
		"slow drain":      func(f map[string]any) { f["observations"].(map[string]any)["drain_seconds"] = 31 },
		"missing fixture": func(f map[string]any) { delete(f["identity"].(map[string]any), "fixture_sha256") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			f := publicContainerFacts()
			mutate(f)
			b := marshalPublicTest(t, f)
			testArtifact(t, dir, "facts.json", string(b))
			out := filepath.Join(dir, "lane.json")
			if code := run([]string{"container", "--evidence-root", dir, "--facts", "facts.json", "--out", out}); code == 0 {
				t.Fatal("invalid observations accepted")
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("rejected producer emitted acceptance file")
			}
		})
	}
}

// These are CLI contract fixtures, never qualification evidence.
func TestPublicNativeRecordIdentity(t *testing.T) {
	dir := t.TempDir()
	lane := laneResult{Packet: "OP-003", SelectedCount: 7, Result: "PASS",
		Details:   map[string]any{"native": true, "platform_checks": 7},
		StartedAt: "2026-10-02T00:00:00Z", FinishedAt: "2026-10-02T01:00:00Z"}
	lanePath := writeLaneFile(t, dir, "native-lane.json", lane)
	art := testArtifact(t, dir, "raw/native.log", "contract-test-only")
	out := filepath.Join(dir, "records", "native.json")
	image := "sha256:" + strings.Repeat("d", 64)
	fixture := strings.Repeat("e", 64)
	args := []string{"--profile", "single-node-public-alpha", "--lane-result", lanePath,
		"--campaign", "public-test", "--candidate", strings.Repeat("a", 40),
		"--binary-sha", strings.Repeat("b", 64), "--config-sha", strings.Repeat("c", 64),
		"--image-digest", image, "--fixture-sha", fixture,
		"--fixture", "fixture-1", "--host-id", "host-1", "--host-os", "linux",
		"--host-kernel", "6.8.0", "--host-arch", "amd64", "--host-runtime", "go1.25.14",
		"--cleanup", "complete", "--artifact", art.Path + ":" + itoa(art.SizeBytes) + ":" + art.SHA256,
		"--out", out}
	if code := runRecord(args); code != 0 {
		t.Fatalf("public native record rejected: exit=%d", code)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	var r map[string]any
	if err := json.Unmarshal(b, &r); err != nil {
		t.Fatal(err)
	}
	if r["schema_version"] != float64(2) || r["image_digest"] != image || r["fixture_sha256"] != fixture {
		t.Fatalf("public record did not retain schema/image/fixture identities: %v", r)
	}
}

func publicRestoreFacts() map[string]any {
	f := publicContainerFacts()
	formats := []any{}
	for _, id := range []string{"v1zip", "v2ndjson"} {
		formats = append(formats, map[string]any{"format": id, "triple_count": 1000000, "duration_seconds": 1, "logical_sha256_expected": strings.Repeat("f", 64), "logical_sha256_actual": strings.Repeat("f", 64), "objects_sha256_expected": strings.Repeat("f", 64), "objects_sha256_actual": strings.Repeat("f", 64)})
	}
	rejections := []any{}
	for _, id := range []string{"truncated", "corrupt", "wrong-app", "oversized", "non-empty"} {
		rejections = append(rejections, map[string]any{"id": id, "error": "rejected", "target_sha256_before": strings.Repeat("f", 64), "target_sha256_after": strings.Repeat("f", 64), "source_sha256_before": strings.Repeat("f", 64), "source_sha256_after": strings.Repeat("f", 64)})
	}
	f["observations"] = map[string]any{"formats": formats, "rejections": rejections}
	return f
}

func TestPublicRestoreFacts(t *testing.T) {
	for _, invalid := range []string{"", "missing-format", "state-changed", "source-changed", "slow", "subset-rejections"} {
		t.Run(invalid, func(t *testing.T) {
			f := publicRestoreFacts()
			o := f["observations"].(map[string]any)
			switch invalid {
			case "missing-format":
				o["formats"] = o["formats"].([]any)[:1]
			case "state-changed":
				o["formats"].([]any)[0].(map[string]any)["logical_sha256_actual"] = strings.Repeat("0", 64)
			case "source-changed":
				o["rejections"].([]any)[0].(map[string]any)["source_sha256_after"] = strings.Repeat("0", 64)
			case "slow":
				o["formats"].([]any)[0].(map[string]any)["duration_seconds"] = 601
			case "subset-rejections":
				o["rejections"] = o["rejections"].([]any)[:4]
			}
			b := marshalPublicTest(t, f)
			var facts liveFacts
			if err := strictPublicJSON(b, &facts); err != nil {
				t.Fatal(err)
			}
			n, err := validateLiveFacts("OP-006", facts)
			if (invalid == "") != (err == nil) || (invalid == "" && n != 7) {
				t.Fatalf("verdict n=%d err=%v", n, err)
			}
		})
	}
}

// Exact candidate source scenarios and raw corpusctl output format are used.
func writeDifferentialFixture(t *testing.T, root string, identity liveIdentity) (map[string]any, []ArtifactRef) {
	t.Helper()
	source, err := os.ReadFile("../../corpus/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	manifest := testArtifact(t, root, "v1_differential/manifest.json", string(source))
	var m corpus.Manifest
	if err := json.Unmarshal(source, &m); err != nil {
		t.Fatal(err)
	}
	paths := []string{}
	arts := []ArtifactRef{manifest}
	clean := false
	delta := ""
	for _, s := range m.Scenarios {
		path := "v1_differential/" + filepath.Base(s.Path) + ".evidence.json"
		sc, err := corpus.LoadScenario(filepath.Join("../../corpus", s.Path))
		if err != nil {
			t.Fatal(err)
		}
		frame := differentialFrames{}
		for _, raw := range sc.ExpectedS2C() {
			canonical, err := corpus.CanonicalBytesOpts(raw, corpus.CanonicalOptions{Differential: true})
			if err != nil {
				t.Fatal(err)
			}
			frame.Raw = append(frame.Raw, string(raw))
			frame.Normalized = append(frame.Normalized, json.RawMessage(canonical))
		}
		e := differentialEvidence{Scenario: s.ID, Fixture: s.Fixture, Mode: "differential", V1Ref: m.V1Ref, V2Ref: identity.CandidateSHA, V2Dirty: &clean, Normalization: "canonical-v1", Target: frame, Other: frame, Delta: &delta}
		b := marshalPublicTest(t, e)
		arts = append(arts, testArtifact(t, root, path, string(b)))
		paths = append(paths, path)
	}
	return map[string]any{"corpus_manifest_artifact": manifest.Path, "evidence": paths}, arts
}

func publicManifestFixture(t *testing.T) (string, gateManifest, string) {
	t.Helper()
	root := t.TempDir()
	binary := testArtifact(t, root, "candidate/instantd", "contract-only-binary")
	config := testArtifact(t, root, "candidate/config", "contract-only-config")
	fixture := testArtifact(t, root, "candidate/fixture.json", "contract-only-fixture")
	identity := liveIdentity{strings.Repeat("a", 40), binary.SHA256, config.SHA256, CanonicalEndpointSHA256(), "sha256:" + strings.Repeat("d", 64), "fixture-1", fixture.SHA256}
	packets, lanes, records, err := selectPublicPerformance("not_selected")
	requirePublicTestSuccess(t, err)
	start := time.Now().Add(-time.Minute).UTC().Truncate(time.Second).Format(time.RFC3339)
	finish := time.Now().Add(-time.Second).UTC().Truncate(time.Second).Format(time.RFC3339)
	m := gateManifest{SchemaVersion: 2, Profile: PublicProfile, DecisionID: PublicDecision, CampaignID: "public-test", CampaignStartedAt: start, CampaignMaxAgeSeconds: 86400, ProviderEvidence: "not_selected", Candidate: manifestCandidate{SHA: identity.CandidateSHA, Binary: binary, Configuration: config, EndpointSHA256: identity.EndpointSHA256, ImageDigest: identity.ImageDigest}, Lanes: lanes, ExternalRecords: map[string]string{}, Fixtures: map[string]fixtureIdentity{}}
	recovery, ok := verdictRecovery(passAll())
	if !ok {
		t.Fatal("recovery fixture verdict failed")
	}
	rb := marshalPublicTest(t, recovery)
	requirePublicTestSuccess(t, json.Unmarshal(rb, &recovery))
	for name, packet := range records {
		r := gateRecord{SchemaVersion: 2, Packet: packet, CampaignID: m.CampaignID, CandidateSHA: identity.CandidateSHA, BinarySHA256: identity.BinarySHA256, ConfigurationSHA256: identity.ConfigurationSHA256, EndpointSHA256: identity.EndpointSHA256, ImageDigest: identity.ImageDigest, FixtureID: identity.FixtureID, FixtureSHA256: identity.FixtureSHA256, Result: "PASS", Cleanup: "complete", SelectedCount: 7, StartedAt: start, FinishedAt: finish, Host: HostIdentity{"host", "linux", "kernel", "amd64", "go"}, Artifacts: []ArtifactRef{fixture}}
		switch name {
		case "native_linux":
			r.Details = map[string]any{"native": true, "platform_checks": float64(7)}
		case "recovery":
			r.Details = recovery
		case "soak":
			r.Details = map[string]any{"sessions": float64(500), "active_seconds": float64(900), "acknowledged_transactions": float64(1), "committed_transactions": float64(1), "refreshed_transactions": float64(1), "dropped_transactions": float64(0), "unresolved_transactions": float64(0)}
		default:
			f := publicContainerFacts()
			if name == "restore" {
				f = publicRestoreFacts()
			}
			f["identity"] = identity
			f["started_at"] = start
			f["finished_at"] = finish
			switch name {
			case "v1_differential":
				obs, arts := writeDifferentialFixture(t, root, identity)
				f["observations"] = obs
				r.Artifacts = append(r.Artifacts, arts...)
				r.SelectedCount = len(arts) - 1
			case "container":
				r.SelectedCount = 9
			}
			factsPath := name + "/facts.json"
			b := marshalPublicTest(t, f)
			r.Artifacts = append(r.Artifacts, testArtifact(t, root, factsPath, string(b)))
			var observations map[string]any
			ob := marshalPublicTest(t, f["observations"])
			requirePublicTestSuccess(t, json.Unmarshal(ob, &observations))
			r.Details = map[string]any{"measurement_class": "live", "facts_artifact": factsPath, "observations": observations}
		}
		path := "records/" + name + ".json"
		b := marshalPublicTest(t, r)
		testArtifact(t, root, path, string(b))
		m.ExternalRecords[name] = path
		m.Fixtures[name] = fixtureIdentity{identity.FixtureID, identity.FixtureSHA256}
	}
	for _, packet := range packets {
		state := "GREEN"
		if packet == "DA-004V" {
			state = "ACCEPTED_EXCEPTION"
		}
		path := "handoffs/" + packet
		b := marshalPublicTest(t, map[string]any{"packet": packet, "state": state, "candidate_sha": identity.CandidateSHA, "campaign_id": m.CampaignID, "finished_at": finish, "ledger_ref": "contract-test-only", "record": recordPathForPublicPacket(packet, m.ExternalRecords)})
		a := testArtifact(t, root, path, string(b))
		m.Handoffs = append(m.Handoffs, manifestHandoff{m.CampaignID, identity.CandidateSHA, finish, packet, path, a.SHA256, a.SizeBytes, state})
	}
	policy := testArtifact(t, root, "policy.json", `{"decision_id":"`+PublicDecision+`","profile":"`+PublicProfile+`","performance":"not_selected","release_version":"v0.1.0-alpha.1"}`)
	return root, m, filepath.Join(root, policy.Path)
}

func TestPublicProfileSelectionAndEvidence(t *testing.T) {
	// The producer resolves only committed candidate-owned corpus sources.
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chdir("../.."); err != nil {
		t.Fatal(err)
	}
	defer func() { requirePublicTestSuccess(t, os.Chdir(original)) }()
	// Fixture helper reads source relative to package, so restore briefly per fixture.
	for _, mutation := range []string{"", "missing-record", "old-profile", "wrong-image", "wrong-fixture", "synthetic", "missing-artifact", "hash-tamper", "missing-packet", "extra-performance", "old-decision", "differential-raw-mismatch", "differential-delta", "differential-dirty", "differential-equal-truncated"} {
		t.Run(mutation, func(t *testing.T) {
			requirePublicTestSuccess(t, os.Chdir(original))
			root, m, policy := publicManifestFixture(t)
			requirePublicTestSuccess(t, os.Chdir(filepath.Join(original, "../..")))

			if strings.HasPrefix(mutation, "differential-") {
				recordPath := m.ExternalRecords["v1_differential"]
				rb := readPublicTestFile(t, filepath.Join(root, recordPath))
				var r gateRecord
				requirePublicTestSuccess(t, json.Unmarshal(rb, &r))
				sourcePath := "v1_differential/00-smoke.ndjson.evidence.json"
				if mutation == "differential-equal-truncated" {
					sourcePath = "v1_differential/01-transact-refresh.ndjson.evidence.json"
				}
				eb := readPublicTestFile(t, filepath.Join(root, sourcePath))
				var e differentialEvidence
				requirePublicTestSuccess(t, json.Unmarshal(eb, &e))
				switch mutation {
				case "differential-raw-mismatch":
					e.Target.Raw[0] = `{"op":"wrong"}`
				case "differential-delta":
					delta := "mismatch"
					e.Delta = &delta
				case "differential-equal-truncated":
					e.Target.Raw = e.Target.Raw[:1]
					e.Target.Normalized = e.Target.Normalized[:1]
					e.Other = e.Target
				case "differential-dirty":
					dirty := true
					e.V2Dirty = &dirty
				}
				eb = marshalPublicTest(t, e)
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, sourcePath), eb, 0600))
				for i, a := range r.Artifacts {
					if a.Path == sourcePath {
						r.Artifacts[i] = hashPublicTestArtifact(t, root, sourcePath)
					}
				}
				rb = marshalPublicTest(t, r)
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, recordPath), rb, 0600))
			}
			switch mutation {
			case "missing-record":
				delete(m.ExternalRecords, "restore")
			case "old-profile":
				m.Profile = ExpectedProfile
			case "wrong-image":
				m.Candidate.ImageDigest = "sha256:" + strings.Repeat("0", 64)
			case "wrong-fixture":
				m.Fixtures["container"] = fixtureIdentity{"other", strings.Repeat("0", 64)}
			case "missing-artifact":
				requirePublicTestSuccess(t, os.Remove(filepath.Join(root, "container/facts.json")))
			case "hash-tamper":
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, "candidate/fixture.json"), []byte("modified"), 0600))
			case "missing-packet":
				m.Handoffs = m.Handoffs[:len(m.Handoffs)-1]
			case "extra-performance":
				m.Lanes["performance"] = "artifact"
			case "old-decision":
				m.DecisionID = ExpectedDecision
			case "synthetic":
				p := filepath.Join(root, m.ExternalRecords["container"])
				b := readPublicTestFile(t, p)
				var r gateRecord
				requirePublicTestSuccess(t, json.Unmarshal(b, &r))
				r.Details["measurement_class"] = "synthetic"
				b = marshalPublicTest(t, r)
				requirePublicTestSuccess(t, os.WriteFile(p, b, 0600))
			}
			b := marshalPublicTest(t, m)
			testArtifact(t, root, "manifest.json", string(b))
			code := runVerifyPublic([]string{"--evidence-root", root, "--manifest", "manifest.json", "--policy", policy, "--candidate", m.Candidate.SHA, "--campaign", m.CampaignID})
			if (mutation == "") != (code == 0) {
				t.Fatalf("mutation %q exit=%d", mutation, code)
			}
		})
	}
}

func TestPublicArtifactRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	testArtifact(t, outside, "facts.json", "outside root")
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}
	if _, err := publicArtifact(root, "linked/facts.json"); err == nil {
		t.Fatal("symlink parent escaped evidence root")
	}
}

func requirePublicTestSuccess(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func marshalPublicTest(t *testing.T, value any) []byte {
	t.Helper()
	b, err := json.Marshal(value)
	requirePublicTestSuccess(t, err)
	return b
}

func readPublicTestFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	requirePublicTestSuccess(t, err)
	return b
}

func hashPublicTestArtifact(t *testing.T, root, path string) ArtifactRef {
	t.Helper()
	a, err := HashFileArtifact(root, path)
	requirePublicTestSuccess(t, err)
	return a
}
