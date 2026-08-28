package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchrun"
)

func main() {
	root := flag.String("output", "", "bundle root (required)")
	seed := flag.Int64("seed", 1, "deterministic seed")
	pair := flag.String("pair", "pair-1", "pair identifier")
	family := flag.String("family", "H-append", "workload family")
	scale := flag.Int("scale", 300, "subscriber scale")
	v1Endpoint := flag.String("v1-endpoint", "", "V1 target endpoint")
	v2Endpoint := flag.String("v2-endpoint", "", "V2 target endpoint")
	configPath := flag.String("config", "", "live target configuration JSON")
	synthetic := flag.Bool("synthetic", false, "run the deterministic synthetic executor (testing only)")
	approve := flag.Bool("approve", false, "approve an existing finalized bundle using out-of-band keys")
	flag.Parse()
	if *approve {
		if *root == "" {
			fail(2, "-output is required for -approve")
		}
		if err := benchrun.ApproveBundle(*root); err != nil {
			fail(1, err.Error())
		}
		fmt.Fprintln(os.Stdout, *root)
		return
	}
	if *configPath != "" && *synthetic {
		fail(2, "-config cannot be combined with -synthetic")
	}
	if *configPath == "" && *root == "" {
		fail(2, "-output is required")
	}
	if *configPath == "" && !*synthetic {
		fail(2, "no target executor configured: provide an integrated WP5-A executor; refusing to emit an empty success bundle")
	}
	if *v1Endpoint != "" || *v2Endpoint != "" {
		fail(2, "endpoint flags require -config with a live target executor; refusing an unconfigured run")
	}
	var executor benchrun.TargetExecutor
	var targets []benchrun.Target
	var liveConfig *benchrun.LiveConfig
	if *configPath != "" {
		cfg, err := benchrun.LoadLiveConfig(*configPath)
		if err != nil {
			fail(2, err.Error())
		}
		liveConfig = &cfg
		if *root == "" {
			*root = cfg.Output
		}
		if *root == "" {
			fail(2, "live config output is required")
		}
		*seed = cfg.Seed
		*pair = cfg.PairID
		*family = cfg.Family
		*scale = cfg.Scale
		executor, err = cfg.Executor()
		if err != nil {
			fail(2, err.Error())
		}
		for _, t := range cfg.Targets {
			metadataHash, _ := hashFile(t.MetadataFile)
			targets = append(targets, benchrun.Target{SchemaVersion: benchrun.SchemaVersion, ID: t.ID, Role: t.ID, Endpoint: t.SessionURL, Protocol: t.Transport, Revision: t.Revision, DirtyHash: t.DirtyTreeHash, InvalidationMode: t.InvalidationMode, OutputPlugin: t.OutputPlugin, DatabaseName: t.DatabaseName, PostgresVersion: t.PostgresVersion, MetadataHash: metadataHash, ProcessPIDEnv: t.ProcessPIDEnv, ProcessPIDFile: t.ProcessPIDFile, FixturePath: cfg.FixturePath, FixtureHash: cfg.FixtureHash, ExecutablePath: t.ProcessExecutablePath, ExecutableHash: t.ProcessExecutableSHA256, Qualification: benchrun.Qualification{Checks: map[string]bool{"configured": true}}})
		}
	} else {
		executor = benchrun.SyntheticExecutor{}
		targets = []benchrun.Target{{SchemaVersion: benchrun.SchemaVersion, ID: "v1", Role: "v1"}, {SchemaVersion: benchrun.SchemaVersion, ID: "v2", Role: "v2_current"}}
	}
	order, err := benchrun.Schedule(*seed, *pair)
	if err != nil {
		fail(1, err.Error())
	}
	w, err := benchrun.NewArtifactWriter(*root, 0)
	if err != nil {
		fail(1, err.Error())
	}
	now := time.Now().UTC()
	plan := benchrun.Plan{SchemaVersion: benchrun.SchemaVersion, Seed: *seed, Families: []string{*family}, Scales: []int{*scale}, Pairs: 7, RampSeconds: 30, SettleSeconds: 0, WarmupSeconds: 60, MeasureSeconds: 180, GraceSeconds: 30}
	if liveConfig != nil {
		if liveConfig.RampSeconds > 0 {
			plan.RampSeconds = liveConfig.RampSeconds
		}
		if liveConfig.SettleSeconds > 0 {
			plan.SettleSeconds = liveConfig.SettleSeconds
		}
		if liveConfig.WarmupSeconds > 0 {
			plan.WarmupSeconds = liveConfig.WarmupSeconds
		}
		if liveConfig.WarmupMutations > 0 {
			plan.WarmupMutations = liveConfig.WarmupMutations
		}
		if liveConfig.MeasureSeconds > 0 {
			plan.MeasureSeconds = liveConfig.MeasureSeconds
		}
		if liveConfig.GraceSeconds > 0 {
			plan.GraceSeconds = liveConfig.GraceSeconds
		}
	}
	if *family == "C-process-cold" {
		plan.RampSeconds, plan.WarmupSeconds, plan.MeasureSeconds = 0, 0, 60
	}
	if *family == "T-saturation" && plan.MeasureSeconds > 600 {
		plan.MeasureSeconds = 600
	}
	fixtureIDs := benchrun.FixtureIDs{}
	if liveConfig != nil {
		fixtureIDs = liveConfig.Fixture
	}
	fixtureEvidence, err := benchrun.BuildFixtureEvidence(fixtureIDs, *family, *scale, *seed)
	if err != nil {
		fail(2, fmt.Sprintf("fixture evidence: %v", err))
	}
	fixtureEvidenceHash, err := benchrun.DigestJSON(fixtureEvidence)
	if err != nil {
		fail(2, fmt.Sprintf("fixture evidence hash: %v", err))
	}
	manifest := benchrun.Manifest{SchemaVersion: benchrun.SchemaVersion, BundleID: "bundle-" + *pair, PairID: *pair, Family: *family, SubscriberScale: *scale, Seed: *seed, RunOrder: order.Order, StartedAt: now, ToolchainVersion: runtime.Version()}
	manifest.FixtureHash = fixtureEvidenceHash
	configEvidence := benchrun.LiveConfig{PairID: *pair, Seed: *seed, Family: *family, Scale: *scale, Fixture: fixtureIDs}
	environment := benchrun.Environment{SchemaVersion: benchrun.SchemaVersion, OS: runtime.GOOS, CPUCount: runtime.NumCPU(), MemoryBytes: benchrun.Missing("bytes")}
	if liveConfig != nil {
		manifest.V1SHA, manifest.V2SHA, manifest.SourceTree, manifest.SchemaHash = liveConfig.V1SHA, liveConfig.V2SHA, liveConfig.SourceTree, liveConfig.SchemaHash
		manifest.HostID, manifest.Executables = liveConfig.HostID, liveConfig.Executables
		configEvidence = benchrun.CanonicalConfigEvidence(*liveConfig)
		// Final bundle approval is produced post-run with the out-of-band trust
		// root; live-config authorization fields are never promoted as approval.
		if liveConfig.FixtureHash != "" && !strings.EqualFold(liveConfig.FixtureHash, fixtureEvidenceHash) {
			fail(2, "configured fixture hash does not match deterministic fixture evidence")
		}
		if manifest.SchemaHash == "" {
			manifest.SchemaHash = benchrun.SchemaVersion
		}
		manifest.DatabaseIDs = map[string]string{}
		manifest.TargetProvenance = map[string]string{}
		for _, target := range liveConfig.Targets {
			manifest.DatabaseIDs[target.ID] = target.DatabaseName
			metadataHash, _ := hashFile(target.MetadataFile)
			manifest.TargetProvenance[target.ID] = strings.Join([]string{target.Revision, target.DirtyTreeHash, target.OutputPlugin, target.InvalidationMode, target.DatabaseName, target.PostgresVersion, target.ProcessPIDEnv, target.ProcessPIDFile, liveConfig.FixturePath, liveConfig.FixtureHash, target.ProcessExecutablePath, target.ProcessExecutableSHA256, metadataHash}, "|")
			if target.ID == "v1" && manifest.V1SHA == "" {
				manifest.V1SHA = target.Revision
			}
			if target.ID == "v2" && manifest.V2SHA == "" {
				manifest.V2SHA = target.Revision
			}
		}
		if manifest.SourceTree == "" {
			if cwd, err := os.Getwd(); err == nil {
				manifest.SourceTree, _ = filepath.Abs(cwd)
			}
		}
		if manifest.HostID == "" {
			manifest.HostID, _ = os.Hostname()
		}
		if len(manifest.Executables) == 0 {
			if executable, err := os.Executable(); err == nil {
				if digest, err := hashFile(executable); err == nil {
					manifest.Executables = map[string]string{"benchrun": digest}
				}
			}
		}
	}
	manifest.ConfigHash, err = benchrun.DigestJSON(configEvidence)
	if err != nil {
		fail(2, fmt.Sprintf("config evidence hash: %v", err))
	}
	if liveConfig != nil && liveConfig.ConfigHash != "" && !strings.EqualFold(liveConfig.ConfigHash, manifest.ConfigHash) {
		fail(2, "configured config hash does not match canonical config evidence")
	}
	maxTotal, maxFile, err := benchrun.ContractArtifactBudget(*family, *scale, plan)
	if err != nil {
		fail(2, fmt.Sprintf("artifact budget: %v", err))
	}
	manifest.ArtifactMaxTotalBytes, manifest.ArtifactMaxFileBytes = maxTotal, maxFile
	if err := w.ConfigureContractBudget(maxTotal, maxFile); err != nil {
		fail(1, err.Error())
	}
	// Fixture evidence is mandatory for both live and synthetic bundles. It is
	// reconstructed from the same frozen workload seed during offline replay.
	if err := w.WriteJSON("fixture.json", fixtureEvidence); err != nil {
		fail(1, err.Error())
	}
	if err := w.WriteJSON("config.json", configEvidence); err != nil {
		fail(1, err.Error())
	}
	runner := benchrun.PairRunner{Writer: w, Executor: executor, Manifest: manifest, Plan: plan, Targets: targets, Environment: environment}
	timeout := benchmarkTimeout(*family, plan, liveConfig)
	runCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if _, err := runner.Run(runCtx); err != nil {
		fail(1, err.Error())
	}
	fmt.Fprintln(os.Stdout, *root)
}
func fail(code int, msg string) { fmt.Fprintln(os.Stderr, msg); os.Exit(code) }

func benchmarkTimeout(family string, plan benchrun.Plan, cfg *benchrun.LiveConfig) time.Duration {
	// PairRunner executes seven AB/BA pairs (14 target attempts). The timeout
	// must cover the complete schedule, including a bounded setup/qualification
	// allowance for every attempt, rather than expiring halfway through it.
	perRun := plan.RampSeconds + plan.SettleSeconds + plan.WarmupSeconds + plan.MeasureSeconds + plan.GraceSeconds
	if family == "C-process-cold" {
		perRun = 60 + plan.GraceSeconds
	}
	if perRun <= 0 {
		perRun = 300
	}
	setupAllowance := 60
	if cfg != nil && cfg.TimeoutSeconds > 0 {
		// A configured value is a per-attempt budget, bounded to the T hard max;
		// it is still multiplied across all 14 attempts.
		perRun = cfg.TimeoutSeconds
	}
	if family == "T-saturation" && perRun > 600 {
		perRun = 600
	}
	return 14 * time.Duration(perRun+setupAllowance) * time.Second
}

func hashFile(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > benchrun.DefaultMaxArtifactBytes {
		return "", fmt.Errorf("executable is missing, non-regular, or exceeds artifact budget")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
