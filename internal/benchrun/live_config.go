package benchrun

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
	_ "github.com/jackc/pgx/v5/stdlib"
)

type LiveTargetConfig struct {
	ID                     string   `json:"id"`
	Role                   string   `json:"role,omitempty"`
	Kind                   string   `json:"kind"`
	Transport              string   `json:"transport"`
	SessionURL             string   `json:"session_url"`
	HealthURL              string   `json:"health_url"`
	AdminBaseURL           string   `json:"admin_base_url,omitempty"`
	AppID                  string   `json:"app_id"`
	Revision               string   `json:"revision"`
	DirtyTreeHash          string   `json:"dirty_tree_hash,omitempty"`
	DatabaseName           string   `json:"database_name"`
	PostgresVersion        string   `json:"postgres_version"`
	InvalidationMode       string   `json:"invalidation_mode"`
	OutputPlugin           string   `json:"output_plugin,omitempty"`
	AdminTokenEnv          string   `json:"admin_token_env,omitempty"`
	RefreshTokenEnv        string   `json:"refresh_token_env,omitempty"`
	MetadataFile           string   `json:"metadata_file"`
	ProvisionedMarker      string   `json:"provisioned_marker"`
	ProvisionCommand       []string `json:"provision_command"`
	ProvisionEnv           []string `json:"provision_env,omitempty"`
	ProvisionCommandSHA256 string   `json:"provision_command_sha256,omitempty"`
	DatabaseURLEnv         string   `json:"database_url_env"`
	// MarkerQuery is retained for source compatibility but is deliberately
	// ignored; benchmark metadata always uses the fixed query below.
	MarkerQuery             string            `json:"-"`
	ProbeEntityID           string            `json:"probe_entity_id"`
	Versions                map[string]string `json:"versions,omitempty"`
	ProcessPIDEnv           string            `json:"process_pid_env,omitempty"`
	ProcessPIDFile          string            `json:"process_pid_file,omitempty"`
	NetworkNamespaceID      string            `json:"network_namespace_id,omitempty"`
	InitialNamespaceID      string            `json:"initial_network_namespace_id,omitempty"`
	ProcessExecutablePath   string            `json:"process_executable_path,omitempty"`
	ProcessExecutableSHA256 string            `json:"process_executable_sha256,omitempty"`
	RuntimeEndpoint         string            `json:"runtime_endpoint,omitempty"`
	RuntimeTokenEnv         string            `json:"runtime_token_env,omitempty"`
	CollectorIntervalMillis int               `json:"collector_interval_millis,omitempty"`
}
type LiveConfig struct {
	PairID                 string             `json:"pair_id"`
	Seed                   int64              `json:"seed"`
	Family                 string             `json:"family"`
	Scale                  int                `json:"scale"`
	Output                 string             `json:"output"`
	RampSeconds            int                `json:"ramp_seconds,omitempty"`
	SettleSeconds          int                `json:"settle_seconds,omitempty"`
	WarmupSeconds          int                `json:"warmup_seconds,omitempty"`
	WarmupMutations        int                `json:"warmup_mutations,omitempty"`
	MeasureSeconds         int                `json:"measure_seconds,omitempty"`
	GraceSeconds           int                `json:"grace_seconds,omitempty"`
	TimeoutSeconds         int                `json:"timeout_seconds,omitempty"`
	V1SHA                  string             `json:"v1_sha,omitempty"`
	V2SHA                  string             `json:"v2_sha,omitempty"`
	V2ReferenceSHA         string             `json:"v2_reference_sha,omitempty"`
	SourceTree             string             `json:"source_tree,omitempty"`
	SchemaHash             string             `json:"schema_hash,omitempty"`
	FixtureHash            string             `json:"fixture_hash,omitempty"`
	FixturePath            string             `json:"fixture_path,omitempty"`
	ConfigHash             string             `json:"config_hash,omitempty"`
	HostID                 string             `json:"host_id,omitempty"`
	Executables            map[string]string  `json:"executables,omitempty"`
	ApprovalPublicKey      string             `json:"approval_public_key,omitempty"`
	ApprovalSignature      string             `json:"approval_signature,omitempty"`
	AuthorizationPublicKey string             `json:"authorization_public_key,omitempty"`
	AuthorizationSignature string             `json:"authorization_signature,omitempty"`
	Fixture                FixtureIDs         `json:"fixture"`
	Targets                []LiveTargetConfig `json:"targets"`
}

func LoadLiveConfig(path string) (LiveConfig, error) {
	var cfg LiveConfig
	b, e := readBounded(path, SmallArtifactBytes)
	if e != nil {
		return cfg, e
	}
	if e = json.Unmarshal(b, &cfg); e != nil {
		return cfg, e
	}
	if e = cfg.Validate(); e != nil {
		return cfg, e
	}
	return cfg, nil
}
func (c LiveConfig) Validate() error {
	if c.PairID == "" || c.Seed == 0 || c.Family == "" || c.Scale <= 0 {
		return errors.New("live config missing pair, seed, family, or scale")
	}
	if e := c.Fixture.validate(); e != nil {
		return e
	}
	if c.FixturePath == "" || !filepath.IsAbs(c.FixturePath) || c.FixtureHash == "" || !isSHA256(c.FixtureHash) {
		return errors.New("live config requires an absolute fixture path and sha256")
	}
	if c.Family != string(benchharness.FamilyT) && c.Fixture.BucketAttrID == "" {
		return errors.New("fixture bucket attribute UUID is required for this workload")
	}
	if c.Family != string(benchharness.FamilyT) && c.Family != string(benchharness.FamilyS) && c.Family != string(benchharness.FamilyR) && c.Fixture.RankAttrID == "" {
		return errors.New("fixture rank attribute UUID is required for this workload")
	}
	if len(c.Targets) != 2 && len(c.Targets) != 3 {
		return errors.New("live config requires v1/v2 or v1/v2_reference/v2_current targets")
	}
	seen := map[string]bool{}
	for _, t := range c.Targets {
		role, err := canonicalTargetRole(t)
		if err != nil {
			return err
		}
		if err := validateLiveTargetKind(t, role); err != nil {
			return err
		}
		if err := validateTargetIdentity(t, role, len(c.Targets)); err != nil {
			return err
		}
		if seen[role] {
			return fmt.Errorf("duplicate target role: %s", role)
		}
		seen[role] = true
		if t.SessionURL == "" || t.HealthURL == "" || t.AppID == "" || t.Revision == "" || t.DatabaseName == "" || t.PostgresVersion == "" || t.InvalidationMode == "" || t.MetadataFile == "" || t.ProvisionedMarker == "" || t.DatabaseURLEnv == "" || t.ProbeEntityID == "" || len(t.ProvisionCommand) == 0 {
			return fmt.Errorf("target %s missing required provenance/config", t.ID)
		}
		if !strings.HasPrefix(t.DatabaseName, "instant_bench_") {
			return fmt.Errorf("target %s database must be disposable", t.ID)
		}
		if t.ProvisionedMarker != t.DatabaseName {
			return fmt.Errorf("target %s requires matching pre-provisioned marker evidence", t.ID)
		}
		prefix := targetEnvPrefixForConfig(t, role)
		if t.DatabaseURLEnv != prefix+"DATABASE_URL" || t.AdminTokenEnv != prefix+"ADMIN_TOKEN" || t.RefreshTokenEnv != prefix+"REFRESH_TOKEN" || t.RuntimeTokenEnv != prefix+"RUNTIME_TOKEN" {
			return fmt.Errorf("target %s must use fixed benchmark environment names", t.ID)
		}
		if t.ProcessPIDFile != "" && !filepath.IsAbs(t.ProcessPIDFile) {
			return fmt.Errorf("target %s process pid file must be absolute", t.ID)
		}
		if c.Family == "C-process-cold" && t.ProcessPIDFile == "" {
			return fmt.Errorf("target %s requires an absolute process pid file for C-process-cold", t.ID)
		}
		if t.ProcessPIDFile == "" && c.Family != "C-process-cold" && t.ProcessPIDEnv != prefix+"PID" {
			return fmt.Errorf("target %s must use fixed process pid environment name", t.ID)
		}
		if t.ProcessPIDFile != "" && t.ProcessPIDEnv != "" && t.ProcessPIDEnv != prefix+"PID" {
			return fmt.Errorf("target %s must use fixed process pid environment name", t.ID)
		}
		if err := validateNetworkNamespaceFields(t); err != nil {
			return err
		}
		base := strings.ToLower(filepath.Base(t.ProvisionCommand[0]))
		if !filepath.IsAbs(t.ProvisionCommand[0]) {
			return fmt.Errorf("target %s provision executable must be absolute", t.ID)
		}
		if t.ProvisionCommandSHA256 == "" || !isSHA256(t.ProvisionCommandSHA256) {
			return fmt.Errorf("target %s provision executable sha256 is required", t.ID)
		}
		if t.ProcessExecutablePath == "" || !filepath.IsAbs(t.ProcessExecutablePath) || t.ProcessExecutableSHA256 == "" || !isSHA256(t.ProcessExecutableSHA256) {
			return fmt.Errorf("target %s process executable path and sha256 are required", t.ID)
		}
		if c.Executables[t.ID] == "" || !strings.EqualFold(c.Executables[t.ID], t.ProcessExecutableSHA256) {
			return fmt.Errorf("target %s executable hash is not anchored in signed provenance", t.ID)
		}
		if len(t.ProvisionEnv) > 0 {
			return fmt.Errorf("target %s provision_env is not allowed; use fixed named settings", t.ID)
		}
		if base == "sh" || base == "bash" || base == "zsh" || base == "fish" || base == "cmd" || base == "powershell" || base == "pwsh" {
			return fmt.Errorf("target %s provision command must not invoke a shell", t.ID)
		}
		if role == "v1" && !strings.EqualFold(t.OutputPlugin, "wal2json") {
			return errors.New("v1 requires wal2json")
		}
		if t.AdminTokenEnv != "" && strings.Contains(t.AdminTokenEnv, "=") {
			return errors.New("secret values must be environment indirection")
		}
		for _, envName := range []string{t.ProcessPIDEnv, t.RuntimeTokenEnv} {
			if envName != "" && (strings.Contains(envName, "=") || strings.ContainsAny(envName, " \t\r\n")) {
				return errors.New("collector secrets and process identifiers must be environment indirection")
			}
		}
		for _, envName := range []string{t.DatabaseURLEnv, t.AdminTokenEnv, t.RefreshTokenEnv} {
			if envName != "" && (strings.Contains(envName, "=") || strings.ContainsAny(envName, " \t\r\n")) {
				return errors.New("provision environment must contain variable names only")
			}
		}
		if t.RuntimeEndpoint != "" && strings.Contains(t.RuntimeEndpoint, "@") {
			return fmt.Errorf("target %s runtime endpoint must not contain URI credentials", t.ID)
		}
		for _, endpoint := range []string{t.SessionURL, t.HealthURL, t.AdminBaseURL, t.RuntimeEndpoint} {
			if endpoint != "" {
				if err := benchharness.ValidateLoopbackURL(endpoint); err != nil {
					return fmt.Errorf("target %s endpoint: %w", t.ID, err)
				}
			}
		}
		if t.CollectorIntervalMillis < 0 {
			return fmt.Errorf("target %s collector interval cannot be negative", t.ID)
		}
	}
	if !seen["v1"] || !seen["v2_current"] {
		return errors.New("v1 and current v2 targets are required")
	}
	if len(c.Targets) == 3 && !seen["v2_reference"] {
		return errors.New("three-target live config requires v2_reference")
	}
	if len(c.Targets) == 2 && seen["v2_reference"] {
		return errors.New("two-target live config cannot contain v2_reference")
	}
	for _, target := range c.Targets {
		role, _ := canonicalTargetRole(target)
		switch role {
		case "v1":
			if c.V1SHA == "" || c.V1SHA != target.Revision {
				return errors.New("top-level v1_sha must equal the qualified v1 revision")
			}
		case "v2_current":
			if c.V2SHA == "" || c.V2SHA != target.Revision {
				return errors.New("top-level v2_sha must equal the qualified v2 revision")
			}
		case "v2_reference":
			if c.V2ReferenceSHA == "" || c.V2ReferenceSHA != target.Revision {
				return errors.New("top-level v2_reference_sha must equal the qualified v2_reference revision")
			}
		}
	}
	return nil
}

func validateLiveTargetKind(target LiveTargetConfig, role string) error {
	want := "v2"
	if role == "v1" {
		want = "v1"
	}
	if target.Kind == "" {
		return fmt.Errorf("target %s kind is required", target.ID)
	}
	if target.Kind != want {
		return fmt.Errorf("target %s role %s requires kind %s (got %s)", target.ID, role, want, target.Kind)
	}
	return nil
}

func canonicalTargetRole(target LiveTargetConfig) (string, error) {
	role := strings.TrimSpace(target.Role)
	inferred := ""
	switch target.ID {
	case "v1":
		inferred = "v1"
	case "v2", "v2_current", "v2-current":
		inferred = "v2_current"
	case "v2_reference", "v2-reference":
		inferred = "v2_reference"
	}
	if role == "" {
		if inferred == "" {
			return "", fmt.Errorf("target ID must be v1, v2, v2_reference, or v2_current: %s", target.ID)
		}
		role = inferred
	}
	if role == "v2" {
		role = "v2_current"
	}
	if role != "v1" && role != "v2_reference" && role != "v2_current" {
		return "", fmt.Errorf("target %s has unsupported role %q", target.ID, target.Role)
	}
	if inferred != "" && inferred != role && target.ID != "v2" {
		return "", fmt.Errorf("target %s role %q does not match its ID", target.ID, target.Role)
	}
	return role, nil
}

func validateTargetIdentity(target LiveTargetConfig, role string, targetCount int) error {
	if targetCount == 3 {
		if target.ID != role || target.Role != role {
			return fmt.Errorf("three-target config requires exact id and role %q (got id=%q role=%q)", role, target.ID, target.Role)
		}
		return nil
	}
	if target.ID != "v1" && target.ID != "v2" {
		return fmt.Errorf("two-target config requires exact v1/v2 IDs (got %q)", target.ID)
	}
	if target.ID == "v1" && target.Role != "" && target.Role != "v1" {
		return fmt.Errorf("two-target config requires exact v1 role when specified (got %q)", target.Role)
	}
	if target.ID == "v2" && target.Role != "" && role != "v2_current" {
		return fmt.Errorf("two-target v2 must map only to current role (got %q)", target.Role)
	}
	return nil
}

func targetEnvPrefix(role string) string {
	return "BENCH_" + strings.ToUpper(role) + "_"
}

func targetEnvPrefixForConfig(target LiveTargetConfig, role string) string {
	// Preserve the original two-target v1/v2 environment contract. Explicit
	// v2_current IDs/roles use their distinct three-target namespace.
	if target.ID == "v2" && strings.TrimSpace(target.Role) == "" {
		return "BENCH_V2_"
	}
	return targetEnvPrefix(role)
}

func validateNetworkNamespaceFields(target LiveTargetConfig) error {
	if (target.NetworkNamespaceID == "") != (target.InitialNamespaceID == "") {
		return fmt.Errorf("target %s requires both network namespace identities", target.ID)
	}
	for name, value := range map[string]string{
		"network_namespace_id":         target.NetworkNamespaceID,
		"initial_network_namespace_id": target.InitialNamespaceID,
	} {
		if value == "" {
			continue
		}
		if !strings.HasPrefix(value, "net:[") || !strings.HasSuffix(value, "]") {
			return fmt.Errorf("target %s %s must use net:[inode] format", target.ID, name)
		}
		digits := strings.TrimSuffix(strings.TrimPrefix(value, "net:["), "]")
		if digits == "" {
			return fmt.Errorf("target %s %s inode is empty", target.ID, name)
		}
		if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
			return fmt.Errorf("target %s %s inode is invalid", target.ID, name)
		}
	}
	return nil
}

func (c LiveConfig) Executor() (TargetExecutor, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	if e := VerifyLiveConfigAuthorization(c); e != nil {
		return nil, e
	}
	drivers := map[string]*benchharness.TargetDriver{}
	for _, spec := range c.Targets {
		tc, err := spec.driverConfig(c.Fixture, c.FixturePath, c.FixtureHash, c.Family, c.Scale, c.Seed)
		if err != nil {
			return nil, err
		}
		d, err := benchharness.NewTargetDriver(tc)
		if err != nil {
			return nil, err
		}
		drivers[spec.ID] = d
	}
	specs := make(map[string]LiveTargetConfig, len(c.Targets))
	for _, spec := range c.Targets {
		specs[spec.ID] = spec
	}
	return &BenchharnessPairExecutor{Drivers: drivers, Specs: specs}, nil
}
func (s LiveTargetConfig) driverConfig(ids FixtureIDs, fixturePath, fixtureHash, family string, scale int, seed int64) (benchharness.TargetConfig, error) {
	metaBytes, e := readBounded(s.MetadataFile, SmallArtifactBytes)
	if e != nil {
		return benchharness.TargetConfig{}, fmt.Errorf("metadata evidence %s: %w", s.ID, e)
	}
	var meta struct {
		Revision         string `json:"revision"`
		DatabaseName     string `json:"database_name"`
		PostgresVersion  string `json:"postgres_version"`
		InvalidationMode string `json:"invalidation_mode"`
		OutputPlugin     string `json:"output_plugin"`
		DirtyTreeHash    string `json:"dirty_tree_hash"`
	}
	if e = json.Unmarshal(metaBytes, &meta); e != nil {
		return benchharness.TargetConfig{}, fmt.Errorf("metadata evidence %s: %w", s.ID, e)
	}
	role, _ := canonicalTargetRole(s)
	if meta.Revision != s.Revision || meta.DatabaseName != s.DatabaseName || meta.PostgresVersion != s.PostgresVersion || meta.InvalidationMode != s.InvalidationMode || meta.DirtyTreeHash != s.DirtyTreeHash || role == "v1" && !strings.EqualFold(meta.OutputPlugin, "wal2json") {
		return benchharness.TargetConfig{}, fmt.Errorf("metadata evidence mismatch for %s", s.ID)
	}
	kind := benchharness.TargetKind(s.Kind)
	transport := benchharness.TransportWebSocket
	if s.Transport == "sse" {
		transport = benchharness.TransportSSE
	}
	probeMutation := benchharness.Mutation{Sequence: 1, EventID: "bench/probe/1", Kind: benchharness.MutationAppend, EntityID: s.ProbeEntityID, Bucket: 0, Rank: 1, Marker: "bench/probe"}
	tc := benchharness.TargetConfig{ID: s.ID, Kind: kind, Transport: transport, SessionURL: s.SessionURL, HealthURL: s.HealthURL, AdminBaseURL: s.AdminBaseURL, AppID: s.AppID, Revision: s.Revision, DirtyTreeHash: s.DirtyTreeHash, DatabaseName: s.DatabaseName, PostgresVersion: s.PostgresVersion, InvalidationMode: s.InvalidationMode, OutputPlugin: s.OutputPlugin, Versions: s.Versions, AdminToken: os.Getenv(s.AdminTokenEnv), RefreshToken: os.Getenv(s.RefreshTokenEnv), AttributeAliases: map[string]string{
		"value":  ids.ValueAttrID,
		"bucket": ids.BucketAttrID,
		"rank":   ids.RankAttrID,
	}, MetadataProbe: func(ctx context.Context) (benchharness.TargetMetadata, error) {
		actual, e := queryDatabaseEvidence(ctx, s)
		if e != nil {
			return benchharness.TargetMetadata{}, e
		}
		actual.Revision = meta.Revision
		actual.DirtyTreeHash = meta.DirtyTreeHash
		actual.OutputPlugin = meta.OutputPlugin
		return actual, nil
	}, Probe: benchharness.LiveRefreshProbe{Query: benchharness.Query{ID: "probe-query", MatchAll: true}, Mutation: probeMutation, Validate: func(initial, refreshed benchharness.Refresh) error {
		if initial.Full == nil || refreshed.Full == nil {
			return errors.New("probe requires full semantic snapshots")
		}
		before, err := initial.Full.Digest()
		if err != nil {
			return fmt.Errorf("probe initial digest: %w", err)
		}
		after, err := refreshed.Full.Digest()
		if err != nil {
			return fmt.Errorf("probe refreshed digest: %w", err)
		}
		if before == after {
			return errors.New("probe mutation produced no semantic change")
		}
		entity, ok := refreshed.Full.Entities[probeMutation.EntityID]
		if !ok {
			return errors.New("probe entity is absent after mutation")
		}
		// Transactions use the UUID-backed attribute above, while the wire
		// decoder intentionally normalizes that UUID to its semantic key.
		value, ok := entity.Attributes["value"]
		if !ok || fmt.Sprint(value) != probeMutation.Marker {
			return errors.New("probe entity marker was not observed after mutation")
		}
		return nil
	}}, Provisioner: func(ctx context.Context) error {
		if err := verifyFixtureHandoff(fixturePath, fixtureHash, ids, family, scale, seed); err != nil {
			return err
		}
		executable, actualHash, e := openProvisionExecutable(s.ProvisionCommand[0])
		if e != nil {
			return fmt.Errorf("provision executable open failed: %w", e)
		}
		defer executable.Close()
		if !strings.EqualFold(actualHash, s.ProvisionCommandSHA256) {
			return fmt.Errorf("provision executable hash verification failed")
		}
		// Execute the same already-open object that was hashed. The fd is
		// inherited as descriptor 3; no path lookup occurs after hashing.
		cmd := exec.CommandContext(ctx, "/proc/self/fd/3", s.ProvisionCommand[1:]...)
		cmd.ExtraFiles = []*os.File{executable}
		cmd.Env = provisionEnvironment(s, fixturePath, fixtureHash, family, scale, seed)
		stdout, stderr := &cappedBuffer{max: 1 << 20}, &cappedBuffer{max: 1 << 20}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		if e = cmd.Run(); e != nil || stdout.truncated || stderr.truncated {
			return fmt.Errorf("provision command failed: %s", Redact(provisionFailure(e, stdout.String(), stderr.String())))
		}
		_, e = queryDatabaseEvidence(ctx, s)
		return e
	}}
	if e := ApplyDefaultBuilders(&tc, ids); e != nil {
		return benchharness.TargetConfig{}, e
	}
	return tc, nil
}

func verifyFixtureHandoff(path, expectedHash string, ids FixtureIDs, family string, scale int, seed int64) error {
	if path == "" || !filepath.IsAbs(path) || !isSHA256(expectedHash) {
		return errors.New("fixture handoff requires an absolute path and sha256")
	}
	actualHash, err := hashFile(path)
	if err != nil || !strings.EqualFold(actualHash, expectedHash) {
		return errors.New("fixture handoff hash verification failed")
	}
	b, err := readBounded(path, SmallArtifactBytes)
	if err != nil {
		return fmt.Errorf("fixture handoff read failed: %w", err)
	}
	var actual FixtureEvidence
	if err := json.Unmarshal(b, &actual); err != nil {
		return errors.New("fixture handoff is not canonical fixture evidence")
	}
	expected, err := BuildFixtureEvidence(ids, family, scale, seed)
	if err != nil {
		return fmt.Errorf("fixture handoff reconstruction failed: %w", err)
	}
	actualBytes, err := CanonicalJSON(actual)
	if err != nil {
		return err
	}
	expectedBytes, err := CanonicalJSON(expected)
	if err != nil || !bytes.Equal(actualBytes, expectedBytes) {
		return errors.New("fixture handoff evidence does not match frozen workload")
	}
	return nil
}

type cappedBuffer struct {
	b         strings.Builder
	max       int
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.b.Len()+len(p) > b.max {
		remaining := b.max - b.b.Len()
		if remaining > 0 {
			_, _ = b.b.Write(p[:remaining])
		}
		b.truncated = true
		return len(p), nil
	}
	return b.b.Write(p)
}
func (b *cappedBuffer) String() string { return b.b.String() }
func provisionFailure(err error, stdout, stderr string) string {
	if err != nil {
		return err.Error() + " stdout=" + stdout + " stderr=" + stderr
	}
	return "output limit exceeded stdout=" + stdout + " stderr=" + stderr
}

func queryDatabaseEvidence(ctx context.Context, s LiveTargetConfig) (benchharness.TargetMetadata, error) {
	dsn := os.Getenv(s.DatabaseURLEnv)
	if e := ValidateBenchmarkDSN(dsn); e != nil {
		return benchharness.TargetMetadata{}, e
	}
	db, e := sql.Open("pgx", dsn)
	if e != nil {
		return benchharness.TargetMetadata{}, e
	}
	defer db.Close()
	var name, marker, storedDB, version string
	if e = db.QueryRowContext(ctx, `SELECT current_database(), marker, database_name, current_setting('server_version') FROM instant_bench_metadata WHERE key=$1`, "benchmark").Scan(&name, &marker, &storedDB, &version); e != nil {
		return benchharness.TargetMetadata{}, fmt.Errorf("database evidence query: %s", Redact(e.Error()))
	}
	if name != s.DatabaseName || !strings.HasPrefix(name, "instant_bench_") {
		return benchharness.TargetMetadata{}, fmt.Errorf("database identity mismatch: got %q", name)
	}
	if !strings.Contains(version, s.PostgresVersion) {
		return benchharness.TargetMetadata{}, fmt.Errorf("PostgreSQL version mismatch: got %q", version)
	}
	if storedDB != name {
		return benchharness.TargetMetadata{}, fmt.Errorf("metadata database identity mismatch")
	}
	if marker != s.ProvisionedMarker {
		return benchharness.TargetMetadata{}, fmt.Errorf("benchmark marker mismatch")
	}
	return benchharness.TargetMetadata{DatabaseName: name, PostgresVersion: version, InvalidationMode: s.InvalidationMode}, nil
}

func provisionEnvironment(s LiveTargetConfig, fixturePath, fixtureHash, family string, scale int, seed int64) []string {
	names := []string{"PATH"}
	names = append(names, s.DatabaseURLEnv)
	seen := make(map[string]bool, len(names))
	env := make([]string, 0, len(names))
	for _, name := range names {
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	env = append(env,
		"BENCH_TARGET_ID="+s.ID,
		"BENCH_DATABASE_NAME="+s.DatabaseName,
		"BENCH_PROVISIONED_MARKER="+s.ProvisionedMarker,
		"BENCH_REVISION="+s.Revision,
		"BENCH_FIXTURE_PATH="+fixturePath,
		"BENCH_FIXTURE_SHA256="+fixtureHash,
		"BENCH_FAMILY="+family,
		"BENCH_SCALE="+strconv.Itoa(scale),
		"BENCH_SEED="+strconv.FormatInt(seed, 10),
	)
	return env
}

type BenchharnessPairExecutor struct {
	Drivers map[string]*benchharness.TargetDriver
	Specs   map[string]LiveTargetConfig
}

func (e *BenchharnessPairExecutor) driver(id string) *benchharness.TargetDriver {
	if e == nil {
		return nil
	}
	return e.Drivers[id]
}
func (e *BenchharnessPairExecutor) Qualify(ctx context.Context, t Target) (Qualification, error) {
	d := e.driver(t.ID)
	if d == nil {
		return Qualification{}, fmt.Errorf("live target %s is not configured", t.ID)
	}
	return (BenchharnessDriver{Driver: d}).QualifyTarget(ctx, t)
}
func (e *BenchharnessPairExecutor) Execute(ctx context.Context, s RunSpec) (ExecutionResult, error) {
	d := e.driver(s.Target.ID)
	if d == nil {
		return ExecutionResult{}, fmt.Errorf("live target %s is not configured", s.Target.ID)
	}
	spec := e.Specs[s.Target.ID]
	return (BenchharnessDriver{Driver: d, Collectors: buildLiveCollectors(spec)}).RunTarget(ctx, s)
}

func buildLiveCollectors(spec LiveTargetConfig) *LiveCollectors {
	set := &LiveCollectors{Interval: time.Duration(spec.CollectorIntervalMillis) * time.Millisecond, Provenance: map[string]string{}}
	if set.Interval <= 0 {
		set.Interval = time.Second
	}
	namespace := NetworkNamespaceProvenance{NamespaceID: spec.NetworkNamespaceID, InitialNamespaceID: spec.InitialNamespaceID}
	if spec.ProcessPIDFile != "" {
		set.Process = ProcProcessCollector{
			PIDFile:          spec.ProcessPIDFile,
			ExecutablePath:   spec.ProcessExecutablePath,
			ExpectedHash:     spec.ProcessExecutableSHA256,
			EndpointURLs:     []string{spec.SessionURL, spec.HealthURL, spec.RuntimeEndpoint},
			NetworkNamespace: namespace,
		}
		set.Provenance["process"] = "supported: pid file=" + spec.ProcessPIDFile
	} else if spec.ProcessPIDEnv == "" {
		set.Process = UnsupportedProcessCollector{Reason: "process_pid_env is not configured"}
		set.Provenance["process"] = "unsupported: process_pid_env is not configured"
	} else {
		pid, err := strconv.Atoi(os.Getenv(spec.ProcessPIDEnv))
		if err != nil || pid <= 0 {
			reason := "process pid environment value is missing or invalid"
			set.Process = FailedProcessCollector{Reason: reason}
			set.Provenance["process"] = "failed: " + reason
		} else {
			set.Process = ProcProcessCollector{
				PID:              pid,
				ExecutablePath:   spec.ProcessExecutablePath,
				ExpectedHash:     spec.ProcessExecutableSHA256,
				EndpointURLs:     []string{spec.SessionURL, spec.HealthURL, spec.RuntimeEndpoint},
				NetworkNamespace: namespace,
			}
			set.Provenance["process"] = "supported: /proc pid=" + strconv.Itoa(pid)
		}
	}
	if spec.RuntimeEndpoint == "" {
		set.Runtime = UnsupportedRuntimeCollector{Reason: "runtime_endpoint is not configured"}
		set.Provenance["runtime"] = "unsupported: runtime_endpoint is not configured"
	} else {
		set.Runtime = PrometheusRuntimeCollector{URL: spec.RuntimeEndpoint, Token: os.Getenv(spec.RuntimeTokenEnv)}
		set.Provenance["runtime"] = "supported: prometheus endpoint " + Redact(spec.RuntimeEndpoint)
	}
	dsn := os.Getenv(spec.DatabaseURLEnv)
	db, err := NewPGDatabaseCollector(dsn)
	if err != nil {
		reason := Redact(err.Error())
		set.Database = FailedDatabaseCollector{Reason: reason}
		set.Provenance["database"] = "failed: " + reason
	} else {
		set.Database = db
		set.Provenance["database"] = "supported: pg_stat_database/pg_stat_wal via " + spec.DatabaseURLEnv
	}
	return set
}
