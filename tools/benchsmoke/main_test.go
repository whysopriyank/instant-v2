package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchrun"
)

type fakeSmokeExecutor struct {
	failTarget      string
	missingMeasured bool
	badTimestamp    bool
	badIdentity     bool
	duplicateLedger bool
}

func (f fakeSmokeExecutor) Qualify(_ context.Context, target benchrun.Target) (benchrun.Qualification, error) {
	if target.ID == f.failTarget {
		return benchrun.Qualification{Passed: false, Failure: "qualification failed"}, errors.New("qualification failed")
	}
	return benchrun.Qualification{Passed: true, Checks: map[string]bool{"live_refresh": true}}, nil
}

func (f fakeSmokeExecutor) Execute(_ context.Context, spec benchrun.RunSpec) (benchrun.ExecutionResult, error) {
	if spec.Target.ID == f.failTarget {
		return benchrun.ExecutionResult{}, errors.New("execution failed")
	}
	now := time.Now().UTC()
	measuredStartedAt := now
	measuredFinishedAt := now.Add(time.Second)
	if f.missingMeasured {
		measuredStartedAt = time.Time{}
		measuredFinishedAt = time.Time{}
	}
	row := benchrun.LedgerRow{SchemaVersion: benchrun.SchemaVersion, PairID: spec.Run.PairID, RunID: spec.Run.ID,
		WriterID: "writer", RecipientID: "recipient", ClientEventID: spec.Run.ID + "/event",
		ExpectedQuerySet: []string{"query"}, ExpectedRecipientSet: []string{"recipient"},
		SubmittedAt: now, AcknowledgementAt: now, CoverAt: now, ConvergedAt: now,
		ExpectedMaterializedDigest: "digest", ObservedMaterializedDigest: "digest", Coverage: "exact",
	}
	if f.badTimestamp {
		row.AcknowledgementAt = time.Time{}
	}
	if f.badIdentity {
		row.RunID = "other-run"
	}
	rows := []benchrun.LedgerRow{row}
	if f.duplicateLedger {
		rows = append(rows, row)
	}
	frames := []benchrun.Frame{{SchemaVersion: benchrun.SchemaVersion, RunID: spec.Run.ID, RecipientID: "recipient", ReceivedAt: now, Kind: "refresh", MaterializedDigest: "digest", PayloadBytes: benchrun.Zero("bytes")}}
	return benchrun.ExecutionResult{Run: benchrun.Run{
		SchemaVersion:              benchrun.SchemaVersion,
		ID:                         spec.Run.ID,
		PairID:                     spec.Run.PairID,
		TargetID:                   spec.Target.ID,
		TargetRevision:             spec.Target.Revision,
		Family:                     spec.Run.Family,
		Scale:                      spec.Run.Scale,
		Seed:                       spec.Run.Seed,
		PrimaryClass:               benchrun.Pass,
		StartedAt:                  now.Add(-time.Second),
		EndedAt:                    now.Add(2 * time.Second),
		ExpectedLedgerRows:         1,
		MeasuredStartedAt:          measuredStartedAt,
		MeasuredFinishedAt:         measuredFinishedAt,
		ExpectedMutationRecipients: 1,
	}, Ledger: rows, Frames: frames}, nil
}

func smokeTestConfig() benchrun.LiveConfig {
	return benchrun.LiveConfig{
		PairID: "smoke", Seed: 17, Family: "H-append", Scale: 300,
		Targets: []benchrun.LiveTargetConfig{
			{ID: "v1", Role: "v1", Kind: "v1", Revision: "v1-sha"},
			{ID: "v2_reference", Role: "v2_reference", Kind: "v2", Revision: "ref-sha"},
			{ID: "v2_current", Role: "v2_current", Kind: "v2", Revision: "current-sha"},
		},
	}
}

func TestSelectSmokeTargets(t *testing.T) {
	cfg := smokeTestConfig()
	one, err := selectSmokeTargets(cfg, "v1")
	if err != nil || len(one) != 1 || one[0].ID != "v1" {
		t.Fatalf("v1 selection = %#v, %v", one, err)
	}
	all, err := selectSmokeTargets(cfg, "all")
	if err != nil || len(all) != 3 {
		t.Fatalf("all selection = %#v, %v", all, err)
	}
	if _, err := selectSmokeTargets(cfg, "v2"); err == nil {
		t.Fatal("noncanonical target selection was accepted")
	}
	invalid := cfg
	invalid.Targets = append([]benchrun.LiveTargetConfig(nil), cfg.Targets...)
	invalid.Targets[2].ID = "v2_other"
	if _, err := selectSmokeTargets(invalid, "all"); err == nil {
		t.Fatal("noncanonical triad was accepted")
	}
}

func TestRunSmokeExecutesEachSelectedTargetOnce(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{}, cfg.Targets, smokePlan(cfg, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("results=%d, want 3", len(results))
	}
	for _, result := range results {
		if !result.Passed || result.LedgerRows != 1 || result.ExpectedLedgerRows != 1 || result.ProtocolErrors != 0 || !result.MeasuredWindowOK {
			t.Fatalf("incomplete smoke result: %+v", result)
		}
	}
}

func TestRunSmokeFailsClosedOnTargetFailure(t *testing.T) {
	cfg := smokeTestConfig()
	selected, err := selectSmokeTargets(cfg, "all")
	if err != nil {
		t.Fatal(err)
	}
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{failTarget: "v1"}, selected, smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed {
		t.Fatalf("failure was not retained: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsMissingMeasuredWindow(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{missingMeasured: true}, cfg.Targets[:1], smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed || results[0].MeasuredWindowOK {
		t.Fatalf("missing measured window was accepted: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsNonPositiveEvidenceTimestamps(t *testing.T) {
	cfg := smokeTestConfig()
	results, err := runSmoke(context.Background(), cfg, fakeSmokeExecutor{badTimestamp: true}, cfg.Targets[:1], smokePlan(cfg, 1))
	if err == nil || len(results) != 1 || results[0].Passed {
		t.Fatalf("non-positive evidence timestamp was accepted: results=%+v err=%v", results, err)
	}
}

func TestRunSmokeRejectsLedgerIdentityAndCardinalityMismatch(t *testing.T) {
	cfg := smokeTestConfig()
	for name, fake := range map[string]fakeSmokeExecutor{
		"identity":  {badIdentity: true},
		"duplicate": {duplicateLedger: true},
	} {
		t.Run(name, func(t *testing.T) {
			results, err := runSmoke(context.Background(), cfg, fake, cfg.Targets[:1], smokePlan(cfg, 1))
			if err == nil || len(results) != 1 || results[0].Passed {
				t.Fatalf("invalid %s evidence was accepted: results=%+v err=%v", name, results, err)
			}
		})
	}
}

func TestSnapshotStableDoesNotMixSourceReplacement(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "source.bin")
	dataA := bytes.Repeat([]byte{'A'}, 4<<20)
	dataB := bytes.Repeat([]byte{'B'}, len(dataA))
	for attempt := 0; attempt < 8; attempt++ {
		if err := os.WriteFile(source, dataA, 0o600); err != nil {
			t.Fatal(err)
		}
		destination := filepath.Join(root, "snapshot.bin")
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 8; i++ {
				replacement := filepath.Join(root, "replacement.bin")
				if os.WriteFile(replacement, dataB, 0o600) == nil {
					_ = os.Rename(replacement, source)
				}
			}
		}()
		_, err := snapshotStable(source, destination, 0o500)
		<-done
		if err != nil {
			t.Fatal(err)
		}
		copied, err := os.ReadFile(destination)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(copied, dataA) && !bytes.Equal(copied, dataB) {
			t.Fatalf("snapshot mixed source replacements on attempt %d", attempt)
		}
		if err := os.Remove(destination); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStableArtifactReadsUseDescriptorValidation(t *testing.T) {
	mainSource, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mainSource), "readStablePath(path, max)") {
		t.Fatal("bounded artifact reads do not use the stable descriptor reader")
	}
	linuxSource, err := os.ReadFile("stable_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "O_NOFOLLOW", "Fstat", "sameStableIdentity"} {
		if !strings.Contains(string(linuxSource), required) {
			t.Fatalf("stable Linux artifact reader lacks %q", required)
		}
	}
}

func TestLinuxSnapshotPublishesThroughHeldDestinationDirFD(t *testing.T) {
	source, err := os.ReadFile("snapshot_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "Linkat", "Unlinkat", "Fstat"} {
		if !strings.Contains(string(source), required) {
			t.Fatalf("Linux snapshot does not use dirfd operation %q", required)
		}
	}
}

func TestLinuxOutputLockAcquisitionUsesDirFDNoFollow(t *testing.T) {
	source, err := os.ReadFile("lock_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"Openat", "O_CREAT", "O_EXCL", "O_NOFOLLOW", "Flock"} {
		if !strings.Contains(string(source), required) {
			t.Fatalf("Linux output lock helper lacks %q", required)
		}
	}
}

func TestLinuxOutputLockRejectsSymlink(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd/O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "decoy-lock")
	if err := os.WriteFile(decoy, []byte("decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(root, "output.lock")
	if err := os.Symlink(decoy, lock); err != nil {
		t.Fatal(err)
	}
	if file, err := openOutputLock(lock); err == nil {
		_ = file.Close()
		t.Fatal("output lock helper followed a symlink")
	}
}

func TestLinuxOutputLockNeverFollowsConcurrentSymlinkReplacement(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd/O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	decoy := filepath.Join(root, "decoy-lock")
	if err := os.WriteFile(decoy, []byte("decoy"), 0o600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(root, "output.lock")
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = os.Remove(lock)
			_ = os.Symlink(decoy, lock)
			_ = os.Remove(lock)
		}
	}()
	defer func() {
		close(stop)
		<-done
		_ = os.Remove(lock)
	}()
	for attempt := 0; attempt < 256; attempt++ {
		file, openErr := openOutputLock(lock)
		if openErr != nil {
			continue
		}
		info, statErr := file.Stat()
		_ = file.Close()
		if statErr != nil {
			t.Fatal(statErr)
		}
		decoyInfo, statErr := os.Stat(decoy)
		if statErr != nil {
			t.Fatal(statErr)
		}
		if os.SameFile(info, decoyInfo) {
			t.Fatal("output lock helper acquired the decoy through a replacement race")
		}
	}
}

func TestLinuxSnapshotRejectsSymlinkedParentDuringSwap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux O_NOFOLLOW semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(root, "source-parent")
	parked := filepath.Join(root, "source-parent-parked")
	decoy := filepath.Join(root, "decoy")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(parent, "source"), bytes.Repeat([]byte{'A'}, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "source"), bytes.Repeat([]byte{'B'}, 1<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if os.Rename(parent, parked) != nil {
				continue
			}
			_ = os.Symlink(decoy, parent)
			_ = os.Remove(parent)
			_ = os.Rename(parked, parent)
		}
	}()
	defer func() {
		close(stop)
		<-done
	}()
	for attempt := 0; attempt < 64; attempt++ {
		destination := filepath.Join(root, fmt.Sprintf("snapshot-%d.bin", attempt))
		_, snapshotErr := snapshotStable(filepath.Join(parent, "source"), destination, 0o500)
		if snapshotErr != nil {
			continue
		}
		copied, readErr := os.ReadFile(destination)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if len(copied) > 0 && copied[0] == 'B' {
			t.Fatal("snapshot followed a swapped symlink parent into the decoy")
		}
		if err := os.Remove(destination); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinuxArtifactRootReadRemainsPinnedAcrossRootSwap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("requires Linux dirfd semantics")
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(root, "bundle")
	decoy := filepath.Join(root, "decoy")
	if err := os.Mkdir(original, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(decoy, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(original, "manifest.json"), []byte(`{"source":"original"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(decoy, "manifest.json"), []byte(`{"source":"decoy"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	artifacts, err := openArtifactRoot(original)
	if err != nil {
		t.Fatal(err)
	}
	defer artifacts.Close()
	parked := filepath.Join(root, "bundle-parked")
	if err := os.Rename(original, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(decoy, original); err != nil {
		t.Fatal(err)
	}
	data, err := artifacts.read("manifest.json", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("original")) {
		t.Fatalf("descriptor-pinned artifact reader followed swapped root: %s", data)
	}
}

func TestSmokePlanIsDiagnosticAndBounded(t *testing.T) {
	cfg := smokeTestConfig()
	plan := smokePlan(cfg, 2)
	if plan.Pairs != 1 || plan.MeasureSeconds != 2 || plan.RampSeconds != 1 || plan.SettleSeconds != 1 || plan.WarmupSeconds != 1 || plan.GraceSeconds != 5 {
		t.Fatalf("unexpected smoke plan: %+v", plan)
	}
}

func TestNamespaceEvidenceRejectsExternalInterfacesAndRoutes(t *testing.T) {
	dev := "Inter-| Receive                        | Transmit\n face |bytes packets errs drop fifo frame compressed multicast|bytes packets errs drop fifo colls carrier compressed\n    lo: 1 2 0 0 0 0 0 0 3 4 0 0 0 0 0 0 0 0\n"
	if err := validateProcDev(dev); err != nil {
		t.Fatal(err)
	}
	if err := validateProcDev(strings.Replace(dev, "    lo:", "  eth0:", 1)); err == nil {
		t.Fatal("external interface evidence was accepted")
	}
	route := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 00000000 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := validateProcRoute(route); err != nil {
		t.Fatal(err)
	}
	localV4 := strings.Replace(route, "lo 00000000", "lo 0000007F", 1)
	if err := validateProcRoute(localV4); err != nil {
		t.Fatalf("canonical IPv4 loopback route was rejected: %v", err)
	}
	if err := validateProcRoute(strings.Replace(route, "lo 00000000", "eth0 00000000", 1)); err == nil {
		t.Fatal("external IPv4 route evidence was accepted")
	}
	ipv6 := "00000000000000000000000000000000 00 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000000 lo\n"
	if err := validateProcIPv6Route(ipv6); err != nil {
		t.Fatal(err)
	}
	localV6 := strings.Replace(ipv6, strings.Repeat("0", 32)+" 00", strings.Repeat("0", 31)+"1 80", 1)
	if err := validateProcIPv6Route(localV6); err != nil {
		t.Fatalf("canonical IPv6 loopback route was rejected: %v", err)
	}
	if err := validateProcIPv6Route(strings.Replace(ipv6, " lo", " eth0", 1)); err == nil {
		t.Fatal("external IPv6 route evidence was accepted")
	}
}

func TestNamespaceEvidenceRejectsExternalLoopbackRoutes(t *testing.T) {
	v4 := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 0100007F 00000000 0001 0 0 0 000000FF 0 0 0\n"
	if err := validateProcRoute(v4); err == nil {
		t.Fatal("external IPv4 destination on lo was accepted")
	}
	v4 = "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\nlo 00000000 00000000 0001 0 0 0 FFFFFFFF 0 0 0\n"
	if err := validateProcRoute(v4); err == nil {
		t.Fatal("arbitrary IPv4 loopback mask was accepted")
	}
	v6 := "00000000000000000000000000000001 00 00000000000000000000000000000000 00 00000000000000000000000000000000 00000000 00000000 00000000 00000000 lo\n"
	if err := validateProcIPv6Route(v6); err == nil {
		t.Fatal("external IPv6 destination on lo was accepted")
	}
}

func TestValidateProgressRejectsNoncanonicalManifest(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config.json")
	configValue := map[string]any{
		"pair_id": "wave6", "seed": 17, "family": "H-append", "scale": 300,
		"ramp_seconds": 30, "settle_seconds": 30, "warmup_seconds": 60,
		"warmup_mutations": 0, "measure_seconds": 180, "grace_seconds": 30,
		"targets": []map[string]any{
			{"id": "v1", "role": "v1", "kind": "v1", "revision": "v1"},
			{"id": "v2_reference", "role": "v2_reference", "kind": "v2", "revision": "ref"},
			{"id": "v2_current", "role": "v2_current", "kind": "v2", "revision": "cur"},
		},
	}
	writeTestJSON(t, config, configValue)
	output := filepath.Join(root, "bundle")
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal(err)
	}
	writeTestJSON(t, filepath.Join(output, "manifest.json"), map[string]any{"schema_version": "wrong"})
	if _, err := validateProgress(output, config); err == nil {
		t.Fatal("noncanonical manifest was accepted")
	}
}

func TestValidateProgressAcceptsCompleteCanonicalBundle(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bundle := filepath.Join(root, "bundle")
	writer, err := benchrun.NewArtifactWriter(bundle, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	targets := []benchrun.Target{
		{SchemaVersion: benchrun.SchemaVersion, ID: "v1", Role: "v1", Kind: "v1", Revision: "v1"},
		{SchemaVersion: benchrun.SchemaVersion, ID: "v2_reference", Role: "v2_reference", Kind: "v2", Revision: "ref"},
		{SchemaVersion: benchrun.SchemaVersion, ID: "v2_current", Role: "v2_current", Kind: "v2", Revision: "cur"},
	}
	pairID, seed := "wave6", int64(17)
	runner := benchrun.PairRunner{
		Writer: writer, Executor: benchrun.SyntheticExecutor{}, Targets: targets,
		Manifest: benchrun.Manifest{SchemaVersion: benchrun.SchemaVersion, BundleID: "bundle-wave6", PairID: pairID, Family: "H-append", SubscriberScale: 300, Seed: seed, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     benchrun.Plan{SchemaVersion: benchrun.SchemaVersion, Seed: seed, Families: []string{"H-append"}, Scales: []int{300}, Pairs: 7, RampSeconds: 30, SettleSeconds: 30, WarmupSeconds: 60, MeasureSeconds: 180, GraceSeconds: 30},
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	err = filepath.Walk(bundle, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Base(path) != "run.json" {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var run benchrun.Run
		if err := json.Unmarshal(b, &run); err != nil {
			return err
		}
		run.StartedAt = now.Add(-time.Hour)
		run.MeasuredStartedAt = now.Add(-time.Minute)
		run.MeasuredFinishedAt = now
		run.EndedAt = now.Add(time.Second)
		updated, err := json.Marshal(run)
		if err != nil {
			return err
		}
		return os.WriteFile(path, updated, 0o600)
	})
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(root, "config.json")
	writeTestJSON(t, config, map[string]any{
		"pair_id": pairID, "seed": seed, "family": "H-append", "scale": 300,
		"ramp_seconds": 30, "settle_seconds": 30, "warmup_seconds": 60, "warmup_mutations": 0,
		"measure_seconds": 180, "grace_seconds": 30,
		"targets": []map[string]any{
			{"id": "v1", "role": "v1", "kind": "v1", "revision": "v1"},
			{"id": "v2_reference", "role": "v2_reference", "kind": "v2", "revision": "ref"},
			{"id": "v2_current", "role": "v2_current", "kind": "v2", "revision": "cur"},
		},
	})
	progress, err := validateProgress(bundle, config)
	if err != nil || !progress.Valid || progress.CompletedAttempts != 21 || progress.FailedAttempts != 0 {
		t.Fatalf("complete canonical bundle was not accepted: progress=%+v err=%v", progress, err)
	}

	t.Run("failed_qualification_is_not_complete", func(t *testing.T) {
		path := filepath.Join(bundle, "targets", "v1.json")
		var target benchrun.Target
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.WriteFile(path, b, 0o600) }()
		target.Qualification.Passed = false
		writeTestJSON(t, path, target)
		progress, err := validateProgress(bundle, config)
		if err == nil && progress.Valid {
			t.Fatalf("failed qualification was accepted as complete: progress=%+v", progress)
		}
	})

	t.Run("failed_qualification_check_is_not_complete", func(t *testing.T) {
		path := filepath.Join(bundle, "targets", "v1.json")
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var target benchrun.Target
		if err := json.Unmarshal(b, &target); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.WriteFile(path, b, 0o600) }()
		for check := range target.Qualification.Checks {
			target.Qualification.Checks[check] = false
			break
		}
		writeTestJSON(t, path, target)
		progress, err := validateProgress(bundle, config)
		if err == nil && progress.Valid {
			t.Fatalf("failed qualification check was accepted as complete: progress=%+v", progress)
		}
	})

	t.Run("partial_run_is_pending_when_live", func(t *testing.T) {
		path := filepath.Join(bundle, "runs")
		var runJSON string
		err := filepath.Walk(path, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if !info.IsDir() && filepath.Base(path) == "run.json" && runJSON == "" {
				runJSON = path
			}
			return nil
		})
		if err != nil || runJSON == "" {
			t.Fatalf("could not find a run artifact: %v", err)
		}
		original, err := os.ReadFile(runJSON)
		if err != nil {
			t.Fatal(err)
		}
		partialReady := make(chan struct{})
		restore := make(chan struct{})
		go func() {
			if err := os.WriteFile(runJSON, []byte(`{"schema_version":"bench-v1"`), 0o600); err != nil {
				return
			}
			close(partialReady)
			<-restore
			_ = os.WriteFile(runJSON, original, 0o600)
		}()
		select {
		case <-partialReady:
		case <-time.After(2 * time.Second):
			t.Fatal("partial artifact writer did not publish")
		}
		defer close(restore)
		progress, err := validateProgress(bundle, config, true)
		if err != nil || !progress.PendingEvidence || progress.Valid {
			t.Fatalf("partial live artifact was not pending: progress=%+v err=%v", progress, err)
		}
	})
}

func writeTestJSON(t *testing.T, path string, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
}
