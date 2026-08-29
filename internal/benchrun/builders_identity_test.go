package benchrun

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

func TestDefaultTransactionStepsAppendIncludesEntityIdentityTriple(t *testing.T) {
	ids := FixtureIDs{
		EntityType:          "bench_items",
		IDAttr:              "id",
		ValueAttr:           "value",
		BucketAttr:          "bucket",
		RankAttr:            "rank",
		IDAttrID:            "00000000-0000-0000-0000-000000000100",
		ValueAttrID:         "00000000-0000-0000-0000-000000000101",
		BucketAttrID:        "00000000-0000-0000-0000-000000000102",
		RankAttrID:          "00000000-0000-0000-0000-000000000103",
		IDAttrValueType:     "blob",
		IDAttrValueEncoding: "string",
		IDAttrCardinality:   "one",
		IDAttrUnique:        true,
		IDAttrIndexed:       true,
		IDAttrPrimary:       true,
		IDAttrIdentity:      true,
		IDAttrRequired:      true,
	}
	const entityID = "00000000-0000-0000-0000-000000000201"
	steps, err := DefaultTransactionSteps(ids, benchharness.Mutation{
		Kind:     benchharness.MutationAppend,
		EntityID: entityID,
		Bucket:   4,
		Rank:     9,
		Marker:   "bench/append",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(steps) != 4 {
		t.Fatalf("append steps = %#v, want identity plus value/bucket/rank", steps)
	}
	found := false
	for _, raw := range steps {
		step, ok := raw.([]any)
		if !ok || len(step) != 4 {
			continue
		}
		if step[0] == "add-triple" && step[1] == entityID && step[2] == "00000000-0000-0000-0000-000000000100" && step[3] == entityID {
			found = true
		}
	}
	if !found {
		t.Fatalf("append steps have no entity identity triple: %#v", steps)
	}
}

func TestDefaultTransactionStepsExistingMutationsDoNotFabricateIdentity(t *testing.T) {
	ids := DefaultFixtureIDs
	for _, kind := range []benchharness.MutationKind{benchharness.MutationUpdate, benchharness.MutationReorder, benchharness.MutationRetract} {
		steps, err := DefaultTransactionSteps(ids, benchharness.Mutation{Kind: kind, EntityID: "00000000-0000-0000-0000-000000000201", Marker: "bench/existing", Rank: 2})
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		for _, raw := range steps {
			step, ok := raw.([]any)
			if ok && len(step) == 4 && step[0] == "add-triple" && step[2] == ids.IDAttrID {
				t.Fatalf("%s fabricated identity triple: %#v", kind, steps)
			}
		}
	}
}

func TestLiveConfigRequiresFixtureIdentityAttribute(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Fixture.IDAttr = ""
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "id attribute") {
		t.Fatalf("missing fixture identity label was accepted: %v", err)
	}
	c = readinessLiveConfig(true)
	c.Fixture.IDAttrID = ""
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "id attribute") {
		t.Fatalf("missing fixture identity UUID was accepted: %v", err)
	}
}

func TestLiveConfigAuthorizationTupleBindsFixtureIdentity(t *testing.T) {
	c := LiveConfig{PairID: "p", Seed: 17, Family: "H-append", Scale: 300, Fixture: DefaultFixtureIDs}
	before, err := LiveConfigAuthorizationTuple(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Fixture.IDAttrID = "00000000-0000-0000-0000-000000000999"
	after, err := LiveConfigAuthorizationTuple(c)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("authorization tuple did not bind fixture identity UUID")
	}
}

func TestProvisionEnvironmentCarriesBoundedFixtureIdentity(t *testing.T) {
	ids := DefaultFixtureIDs
	env := provisionEnvironment(LiveTargetConfig{ID: "v1", DatabaseURLEnv: "BENCH_V1_DATABASE_URL"}, ids, "/tmp/fixture.json", strings.Repeat("a", 64), "H-append", 300, 17)
	values := make(map[string]string, len(env))
	for _, entry := range env {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) == 2 {
			values[parts[0]] = parts[1]
		}
	}
	if values["BENCH_FIXTURE_ID_ATTR"] != ids.IDAttr || values["BENCH_FIXTURE_ID_ATTR_ID"] != ids.IDAttrID {
		t.Fatalf("provision environment identity fields = %#v", values)
	}
	for key, want := range map[string]string{
		"BENCH_FIXTURE_ID_VALUE_TYPE":     ids.IDAttrValueType,
		"BENCH_FIXTURE_ID_VALUE_ENCODING": ids.IDAttrValueEncoding,
		"BENCH_FIXTURE_ID_CARDINALITY":    ids.IDAttrCardinality,
		"BENCH_FIXTURE_ID_UNIQUE":         "true",
		"BENCH_FIXTURE_ID_INDEXED":        "true",
		"BENCH_FIXTURE_ID_REQUIRED":       "true",
		"BENCH_FIXTURE_ID_PRIMARY":        "true",
		"BENCH_FIXTURE_IDENTITY":          "true",
	} {
		if values[key] != want {
			t.Fatalf("provision environment %s=%q, want %q", key, values[key], want)
		}
	}
}

func TestFixtureEvidenceSerializesIdentityAttributeContract(t *testing.T) {
	ids := FixtureIDs{
		EntityType:          "bench_items",
		IDAttr:              "id",
		ValueAttr:           "value",
		BucketAttr:          "bucket",
		RankAttr:            "rank",
		IDAttrID:            "00000000-0000-0000-0000-000000000100",
		ValueAttrID:         "00000000-0000-0000-0000-000000000101",
		BucketAttrID:        "00000000-0000-0000-0000-000000000102",
		RankAttrID:          "00000000-0000-0000-0000-000000000103",
		IDAttrValueType:     "blob",
		IDAttrValueEncoding: "string",
		IDAttrCardinality:   "one",
		IDAttrUnique:        true,
		IDAttrIndexed:       true,
		IDAttrPrimary:       true,
		IDAttrIdentity:      true,
		IDAttrRequired:      true,
	}
	evidence, err := BuildFixtureEvidence(ids, "H-append", 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if got, ok := raw["id_attr"].(string); !ok || got != "id" {
		t.Fatalf("fixture id_attr = %#v, want id", raw["id_attr"])
	}
	if got, ok := raw["id_attr_id"].(string); !ok || got != "00000000-0000-0000-0000-000000000100" {
		t.Fatalf("fixture id_attr_id = %#v, want fixed identity UUID", raw["id_attr_id"])
	}
	for key, want := range map[string]any{
		"id_attr_value_type":     "blob",
		"id_attr_value_encoding": "string",
		"id_attr_cardinality":    "one",
		"id_attr_unique":         true,
		"id_attr_indexed":        true,
		"id_attr_primary":        true,
		"id_attr_identity":       true,
		"id_attr_required":       true,
	} {
		if raw[key] != want {
			t.Fatalf("fixture %s = %#v, want %#v", key, raw[key], want)
		}
	}
}

func TestFixtureIdentityContractRequiresExactLabelMetadata(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*FixtureIDs)
		want   string
	}{
		{name: "label", mutate: func(ids *FixtureIDs) { ids.IDAttr = "ID" }, want: "exactly id"},
		{name: "value type", mutate: func(ids *FixtureIDs) { ids.IDAttrValueType = "json" }, want: "blob"},
		{name: "encoding", mutate: func(ids *FixtureIDs) { ids.IDAttrValueEncoding = "bytes" }, want: "string"},
		{name: "cardinality", mutate: func(ids *FixtureIDs) { ids.IDAttrCardinality = "many" }, want: "cardinality"},
		{name: "unique", mutate: func(ids *FixtureIDs) { ids.IDAttrUnique = false }, want: "unique"},
		{name: "indexed", mutate: func(ids *FixtureIDs) { ids.IDAttrIndexed = false }, want: "indexed"},
		{name: "primary", mutate: func(ids *FixtureIDs) { ids.IDAttrPrimary = false }, want: "primary"},
		{name: "identity", mutate: func(ids *FixtureIDs) { ids.IDAttrIdentity = false }, want: "identity"},
		{name: "required", mutate: func(ids *FixtureIDs) { ids.IDAttrRequired = false }, want: "required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ids := DefaultFixtureIDs
			tc.mutate(&ids)
			if _, err := BuildFixtureEvidence(ids, "H-append", 300, 17); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("invalid identity metadata accepted: %v", err)
			}
		})
	}
}

func TestFixtureIdentityUUIDsRejectCaseInsensitiveCollisions(t *testing.T) {
	ids := DefaultFixtureIDs
	ids.ValueAttrID = strings.ToUpper(ids.IDAttrID)
	if _, err := BuildFixtureEvidence(ids, "H-append", 300, 17); err == nil || !strings.Contains(err.Error(), "collide") {
		t.Fatalf("case-insensitive identity/value UUID collision accepted: %v", err)
	}
}

func TestFixtureIdentityUUIDsRejectNonCanonicalValues(t *testing.T) {
	ids := DefaultFixtureIDs
	ids.ValueAttrID = strings.Replace(CanonicalFixtureValueAttrID, "1", "a", 1)
	if _, err := BuildFixtureEvidence(ids, "H-append", 300, 17); err == nil || !strings.Contains(err.Error(), "canonical") {
		t.Fatalf("uppercase non-canonical fixture UUID was accepted: %v", err)
	}
}

func TestBenchmarkSchemasPinUUIDRefsAndLiveTargetContracts(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	readSchema := func(name string) map[string]any {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "..", "..", "benchmarks", "schema", name))
		if err != nil {
			t.Fatal(err)
		}
		var schema map[string]any
		if err := json.Unmarshal(b, &schema); err != nil {
			t.Fatal(err)
		}
		return schema
	}
	canonical := map[string]string{
		"id_attr_id":     CanonicalFixtureIDAttrID,
		"value_attr_id":  CanonicalFixtureValueAttrID,
		"bucket_attr_id": CanonicalFixtureBucketAttrID,
		"rank_attr_id":   CanonicalFixtureRankAttrID,
	}
	fixture := readSchema("fixture.schema.json")
	fixtureDefs, ok := fixture["$defs"].(map[string]any)
	if !ok || fixtureDefs["uuid"] == nil {
		t.Fatal("fixture schema must define uuid")
	}
	fixtureProperties := fixture["properties"].(map[string]any)
	for name, want := range canonical {
		property := fixtureProperties[name].(map[string]any)
		allOf, ok := property["allOf"].([]any)
		if !ok || len(allOf) != 2 {
			t.Fatalf("fixture %s does not combine uuid ref and const: %#v", name, property)
		}
		ref, _ := allOf[0].(map[string]any)
		if ref["$ref"] != "#/$defs/uuid" {
			t.Fatalf("fixture %s uuid ref = %#v", name, ref)
		}
		constant, _ := allOf[1].(map[string]any)
		if constant["const"] != want {
			t.Fatalf("fixture %s const = %#v, want %s", name, constant["const"], want)
		}
	}
	entityIDs := fixtureProperties["entity_ids"].(map[string]any)["items"].(map[string]any)
	if entityIDs["$ref"] != "#/$defs/uuid" {
		t.Fatalf("fixture entity_ids item ref = %#v", entityIDs)
	}

	live := readSchema("live-config.schema.json")
	liveDefs, ok := live["$defs"].(map[string]any)
	if !ok || liveDefs["uuid"] == nil || liveDefs["target"] == nil {
		t.Fatal("live-config schema must define uuid and target")
	}
	targets := live["properties"].(map[string]any)["targets"].(map[string]any)
	if targets["items"].(map[string]any)["$ref"] != "#/$defs/target" {
		t.Fatalf("live target item ref = %#v", targets["items"])
	}
	target := liveDefs["target"].(map[string]any)
	targetRequired, _ := target["required"].([]any)
	required := make(map[string]bool, len(targetRequired))
	for _, raw := range targetRequired {
		required[raw.(string)] = true
	}
	for _, name := range []string{"id", "kind", "session_url", "health_url", "app_id", "revision", "database_name", "metadata_file", "provision_command", "database_url_env", "probe_entity_id", "process_executable_path", "process_executable_sha256"} {
		if !required[name] {
			t.Fatalf("live target does not require %s", name)
		}
	}
	targetProperties := target["properties"].(map[string]any)
	for _, name := range []string{"app_id", "probe_entity_id"} {
		property := targetProperties[name].(map[string]any)
		if property["$ref"] != "#/$defs/uuid" {
			t.Fatalf("target %s uuid ref = %#v", name, property)
		}
	}
}

func TestCatalogIdentityProofUsesLabelForPrimarySemantics(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "live_config.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(b)
	if strings.Contains(source, "evidence.Primary = evidence.Required") {
		t.Fatal("catalog proof derives primary semantics from required metadata")
	}
	if !strings.Contains(source, "a.label = 'id'") {
		t.Fatal("catalog proof does not independently derive primary semantics from the id label")
	}
}

func TestDefaultTransactionStepsRejectNonUUIDOutboundEntityID(t *testing.T) {
	_, err := DefaultTransactionSteps(DefaultFixtureIDs, benchharness.Mutation{
		Kind: benchharness.MutationAppend, EntityID: "entity-not-a-uuid", Marker: "bench/append", Bucket: 1, Rank: 1,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "uuid") {
		t.Fatalf("non-UUID outbound entity ID was accepted: %v", err)
	}
}

func TestDefaultTransactionStepsRejectNoncanonicalOutboundEntityIDCasing(t *testing.T) {
	_, err := DefaultTransactionSteps(DefaultFixtureIDs, benchharness.Mutation{
		Kind: benchharness.MutationAppend, EntityID: "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB", Marker: "bench/append", Bucket: 1, Rank: 1,
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "lowercase") {
		t.Fatalf("uppercase outbound entity UUID was accepted: %v", err)
	}
}

func TestLiveConfigRejectsNonUUIDProbeEntityID(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[0].ProbeEntityID = "probe-not-a-uuid"
	if err := c.Validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "probe") || !strings.Contains(strings.ToLower(err.Error()), "uuid") {
		t.Fatalf("non-UUID probe entity ID was accepted: %v", err)
	}
}

func TestLiveConfigRejectsNoncanonicalProbeEntityIDCasing(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[0].ProbeEntityID = "ABCDEFAB-CDEF-ABCD-EFAB-CDEFABCDEFAB"
	if err := c.Validate(); err == nil || !strings.Contains(strings.ToLower(err.Error()), "lowercase") {
		t.Fatalf("uppercase probe entity UUID was accepted: %v", err)
	}
}

func TestFixtureHandoffRejectsForgedIdentityAttributeEvidence(t *testing.T) {
	evidence, err := BuildFixtureEvidence(DefaultFixtureIDs, "H-append", 300, 17)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	var forged FixtureEvidence
	if err := json.Unmarshal(b, &forged); err != nil {
		t.Fatal(err)
	}
	forged.IDAttrID = "00000000-0000-0000-0000-000000000999"
	forgedBytes, err := json.Marshal(forged)
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/fixture.json"
	if err := os.WriteFile(path, forgedBytes, 0600); err != nil {
		t.Fatal(err)
	}
	hash, err := hashFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyFixtureHandoff(path, hash, DefaultFixtureIDs, "H-append", 300, 17); err == nil {
		t.Fatal("fixture handoff accepted forged identity attribute UUID")
	}
}

func TestOfflineReportsRejectConfigFixtureIdentityForgery(t *testing.T) {
	tests := []struct {
		name    string
		targets []Target
		budget  int64
	}{
		{name: "pair", targets: []Target{{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current"}}, budget: 1 << 26},
		{name: "triad", targets: []Target{{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "v1"}, {SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "v2-reference"}, {SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "v2-current"}}, budget: 1 << 28},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			writer, err := NewArtifactWriter(root, tc.budget)
			if err != nil {
				t.Fatal(err)
			}
			runner := PairRunner{
				Writer: writer, Executor: SyntheticExecutor{}, Fixture: DefaultFixtureIDs,
				Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "identity-forgery-" + tc.name, PairID: "identity-forgery-" + tc.name, Family: "H-append", SubscriberScale: 300, Seed: 17, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()},
				Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7}, Targets: tc.targets,
			}
			if _, err := runner.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			configPath := filepath.Join(root, "config.json")
			configBytes, err := os.ReadFile(configPath)
			if err != nil {
				t.Fatal(err)
			}
			var cfg LiveConfig
			if err := json.Unmarshal(configBytes, &cfg); err != nil {
				t.Fatal(err)
			}
			cfg.Fixture.IDAttrID = "00000000-0000-0000-0000-000000000999"
			if err := os.WriteFile(configPath, mustJSON(cfg), 0600); err != nil {
				t.Fatal(err)
			}
			manifest, err := LoadManifest(root)
			if err != nil {
				t.Fatal(err)
			}
			manifest.ConfigHash, err = DigestJSON(CanonicalConfigEvidence(cfg))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "manifest.json"), mustJSON(manifest), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := reportFromArtifacts(root, false); err == nil || !strings.Contains(err.Error(), "fixture does not match") {
				t.Fatalf("offline %s report accepted config/fixture identity forgery: %v", tc.name, err)
			}
		})
	}
}
