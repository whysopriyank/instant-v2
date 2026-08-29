package benchrun

import (
	"strings"
	"testing"
)

func readinessFixtureTarget(id, role, revision, database string, processPIDFile bool) LiveTargetConfig {
	sha := strings.Repeat("a", 64)
	if role == "" {
		if id == "v1" {
			role = "v1"
		} else {
			role = "v2_current"
		}
	}
	target := LiveTargetConfig{
		ID: id, Role: role, Kind: "v2", Transport: "websocket",
		SessionURL: "ws://127.0.0.1:8080/runtime/session", HealthURL: "http://127.0.0.1:8080/health",
		AppID: "00000000-0000-0000-0000-000000000001", Revision: revision,
		DatabaseName: database, PostgresVersion: "17.11", InvalidationMode: "post-commit",
		MetadataFile: "/tmp/metadata.json", ProvisionedMarker: database,
		ProvisionCommand: []string{"/bin/true"}, ProvisionCommandSHA256: sha,
		DatabaseURLEnv: targetEnvPrefix(role) + "DATABASE_URL", AdminTokenEnv: targetEnvPrefix(role) + "ADMIN_TOKEN",
		RefreshTokenEnv: targetEnvPrefix(role) + "REFRESH_TOKEN", RuntimeTokenEnv: targetEnvPrefix(role) + "RUNTIME_TOKEN",
		ProcessExecutablePath: "/usr/bin/true", ProcessExecutableSHA256: sha,
		ProbeEntityID: "00000000-0000-0000-0000-000000000003",
	}
	if processPIDFile {
		target.ProcessPIDFile = "/tmp/" + strings.ReplaceAll(id, "_", "-") + ".pid"
	} else {
		target.ProcessPIDEnv = targetEnvPrefix(role) + "PID"
	}
	if role == "v1" {
		target.Kind = "v1"
		target.OutputPlugin = "wal2json"
	}
	return target
}

func readinessLiveConfig(processPIDFile bool) LiveConfig {
	return LiveConfig{
		PairID: "readiness", Seed: 7, Family: "H-append", Scale: 300,
		V1SHA: "v1-rev", V2SHA: "v2-current-rev", V2ReferenceSHA: "v2-reference-rev",
		FixturePath: "/tmp/fixture.json", FixtureHash: strings.Repeat("b", 64),
		Fixture: FixtureIDs{
			EntityType: "todos", IDAttr: "id", ValueAttr: "value", BucketAttr: "bucket", RankAttr: "rank",
			IDAttrID:        CanonicalFixtureIDAttrID,
			ValueAttrID:     CanonicalFixtureValueAttrID,
			BucketAttrID:    CanonicalFixtureBucketAttrID,
			RankAttrID:      CanonicalFixtureRankAttrID,
			IDAttrValueType: "blob", IDAttrValueEncoding: "string", IDAttrCardinality: "one",
			IDAttrUnique: true, IDAttrIndexed: true, IDAttrPrimary: true, IDAttrIdentity: true,
			IDAttrRequired: true,
		},
		Targets: []LiveTargetConfig{
			readinessFixtureTarget("v1", "v1", "v1-rev", "instant_bench_v1", processPIDFile),
			readinessFixtureTarget("v2_reference", "v2_reference", "v2-reference-rev", "instant_bench_v2_reference", processPIDFile),
			readinessFixtureTarget("v2_current", "v2_current", "v2-current-rev", "instant_bench_v2_current", processPIDFile),
		},
		Executables: map[string]string{
			"v1":           strings.Repeat("a", 64),
			"v2":           strings.Repeat("a", 64),
			"v2_reference": strings.Repeat("a", 64),
			"v2_current":   strings.Repeat("a", 64),
		},
	}
}

func TestLiveConfigAcceptsThreeTargetRolesAndDistinctEnvironmentPrefixes(t *testing.T) {
	c := readinessLiveConfig(true)
	if err := c.Validate(); err != nil {
		t.Fatalf("three-target live config rejected: %v", err)
	}
	for _, role := range []string{"v1", "v2_reference", "v2_current"} {
		if got, want := targetEnvPrefix(role), "BENCH_"+strings.ToUpper(role)+"_"; got != want {
			t.Fatalf("environment prefix for %s = %q, want %q", role, got, want)
		}
	}
}

func TestLiveConfigRejectsTargetKindRoleMismatch(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[0].Kind = "v2"
	if err := c.Validate(); err == nil {
		t.Fatal("V1 target with V2 kind was accepted")
	}
}

func TestLiveConfigRequiresExplicitTargetKind(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[2].Kind = ""
	if err := c.Validate(); err == nil {
		t.Fatal("live config without target kind was accepted")
	}
}

func TestLiveConfigPreservesTwoTargetV1V2Compatibility(t *testing.T) {
	c := readinessLiveConfig(false)
	c.Targets = []LiveTargetConfig{
		readinessFixtureTarget("v1", "", "v1-rev", "instant_bench_v1", false),
		readinessFixtureTarget("v2", "", "v2-current-rev", "instant_bench_v2", false),
	}
	c.V2ReferenceSHA = ""
	if err := c.Validate(); err != nil {
		t.Fatalf("two-target v1/v2 config rejected: %v", err)
	}
}

func TestLiveConfigAcceptsAbsolutePIDFileForNonColdFamily(t *testing.T) {
	for _, family := range []string{"H-append", "X-heterogeneous", "M-mixed", "O-reorder", "S-slow-reader", "R-reconnect", "C-process-cold", "T-saturation"} {
		t.Run(family, func(t *testing.T) {
			c := readinessLiveConfig(true)
			c.Family = family
			for i := range c.Targets {
				if c.Targets[i].ProcessPIDEnv != "" {
					t.Fatalf("target %s retained process pid env with pid-file mode", c.Targets[i].ID)
				}
			}
			if err := c.Validate(); err != nil {
				t.Fatalf("absolute pid-file config rejected for %s: %v", family, err)
			}
		})
	}
}

func TestLiveConfigBindsSignedNetworkNamespaceToProcessCollector(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[1].NetworkNamespaceID = "net:[4026533000]"
	c.Targets[1].InitialNamespaceID = "net:[4026531996]"
	if err := c.Validate(); err != nil {
		t.Fatalf("namespace-bound live config rejected: %v", err)
	}
	collectors := buildLiveCollectors(c.Targets[1])
	process, ok := collectors.Process.(ProcProcessCollector)
	if !ok {
		t.Fatalf("process collector type = %T, want ProcProcessCollector", collectors.Process)
	}
	if process.NetworkNamespace.NamespaceID != c.Targets[1].NetworkNamespaceID || process.NetworkNamespace.InitialNamespaceID != c.Targets[1].InitialNamespaceID {
		t.Fatalf("namespace provenance was not bound to collector: %#v", process.NetworkNamespace)
	}
	collectors.Close()
}

func TestLiveConfigRejectsPartialOrMalformedNetworkNamespace(t *testing.T) {
	for name, mutate := range map[string]func(*LiveConfig){
		"partial": func(c *LiveConfig) { c.Targets[0].NetworkNamespaceID = "net:[4026533000]" },
		"malformed target": func(c *LiveConfig) {
			c.Targets[0].NetworkNamespaceID = "net:bad"
			c.Targets[0].InitialNamespaceID = "net:[4026531996]"
		},
		"malformed initial": func(c *LiveConfig) {
			c.Targets[0].NetworkNamespaceID = "net:[4026533000]"
			c.Targets[0].InitialNamespaceID = "host"
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := readinessLiveConfig(true)
			mutate(&c)
			if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "namespace") {
				t.Fatalf("invalid namespace provenance was accepted: %v", err)
			}
		})
	}
}

func TestLiveConfigRejectsRelativePIDFile(t *testing.T) {
	c := readinessLiveConfig(true)
	c.Targets[1].ProcessPIDFile = "relative.pid"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "process pid file must be absolute") {
		t.Fatalf("relative pid file was accepted: %v", err)
	}
}

func TestLiveConfigRejectsReferenceRevisionWithoutTopLevelAnchor(t *testing.T) {
	c := readinessLiveConfig(true)
	c.V2ReferenceSHA = ""
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "v2_reference_sha") {
		t.Fatalf("unanchored reference revision was accepted: %v", err)
	}
}

func TestThreeTargetLiveConfigRequiresExactIDsAndRoles(t *testing.T) {
	for name, mutate := range map[string]func(*LiveConfig){
		"reference alias ID": func(c *LiveConfig) {
			c.Targets[1].ID = "v2-reference"
			c.Executables["v2-reference"] = c.Executables["v2_reference"]
		},
		"current alias ID": func(c *LiveConfig) {
			c.Targets[2].ID = "v2-current"
			c.Executables["v2-current"] = c.Executables["v2_current"]
		},
		"missing reference role": func(c *LiveConfig) {
			c.Targets[1].Role = ""
		},
		"mismatched v2 role": func(c *LiveConfig) {
			c.Targets[1].ID = "v2"
			c.Targets[1].Role = "v2_reference"
			c.Targets[1].DatabaseURLEnv = "BENCH_V2_REFERENCE_DATABASE_URL"
			c.Targets[1].AdminTokenEnv = "BENCH_V2_REFERENCE_ADMIN_TOKEN"
			c.Targets[1].RefreshTokenEnv = "BENCH_V2_REFERENCE_REFRESH_TOKEN"
			c.Targets[1].RuntimeTokenEnv = "BENCH_V2_REFERENCE_RUNTIME_TOKEN"
			c.Executables["v2"] = c.Executables["v2_reference"]
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := readinessLiveConfig(true)
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("ambiguous three-target identity was accepted: %v", err)
			}
		})
	}
}

func TestTwoTargetLiveConfigRequiresCanonicalV1AndV2IDs(t *testing.T) {
	for name, mutate := range map[string]func(*LiveConfig){
		"current alias ID": func(c *LiveConfig) {
			c.Targets[1].ID = "v2_current"
			c.Targets[1].Role = "v2_current"
			c.Executables["v2_current"] = c.Executables["v2"]
		},
		"reference role on v2": func(c *LiveConfig) {
			c.Targets[1].Role = "v2_reference"
			c.Targets[1].DatabaseURLEnv = "BENCH_V2_REFERENCE_DATABASE_URL"
			c.Targets[1].AdminTokenEnv = "BENCH_V2_REFERENCE_ADMIN_TOKEN"
			c.Targets[1].RefreshTokenEnv = "BENCH_V2_REFERENCE_REFRESH_TOKEN"
			c.Targets[1].RuntimeTokenEnv = "BENCH_V2_REFERENCE_RUNTIME_TOKEN"
			c.Targets[1].ProcessPIDEnv = "BENCH_V2_REFERENCE_PID"
			c.Executables["v2"] = c.Executables["v2_reference"]
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := readinessLiveConfig(false)
			c.Targets = []LiveTargetConfig{
				readinessFixtureTarget("v1", "", "v1-rev", "instant_bench_v1", false),
				readinessFixtureTarget("v2", "", "v2-current-rev", "instant_bench_v2", false),
			}
			c.V2ReferenceSHA = ""
			c.Executables = map[string]string{"v1": strings.Repeat("a", 64), "v2": strings.Repeat("a", 64), "v2_current": strings.Repeat("a", 64)}
			mutate(&c)
			if err := c.Validate(); err == nil {
				t.Fatalf("non-canonical two-target identity was accepted: %v", err)
			}
		})
	}
}
