// Command benchsmoke runs diagnostic, non-claimable live attempts through the
// production target executor. It exists to prove protocol/provisioning seams
// before committing hours to the balanced seven-block benchmark.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"os"
	"runtime"
	"strconv"
	"time"
)

func main() {
	configPath := flag.String("config", "", "authorized live configuration JSON")
	targetSet := flag.String("targets", "v1", "target selection: v1 or all")
	output := flag.String("output", "-", "diagnostic JSON path, or - for stdout")
	measureSeconds := flag.Int("measure-seconds", 1, "diagnostic measured window, 1-10 seconds")
	perTargetTimeout := flag.Duration("per-target-timeout", 45*time.Minute, "hard timeout per selected target")
	validateNamespace := flag.Bool("validate-namespace", false, "prove the current Linux namespace is the signed loopback-only benchmark namespace")
	validateProgressPath := flag.String("validate-progress", "", "validate an in-progress H300 triad bundle and emit progress JSON")
	allowPartialProgress := flag.Bool("allow-partial-progress", false, "treat malformed live artifacts as pending evidence while the bundle is running")
	redactStream := flag.Bool("redact-stream", false, "redact and cap stdin into -output")
	snapshotSource := flag.String("snapshot-source", "", "copy one source file through a stable descriptor into -output")
	snapshotMode := flag.String("snapshot-mode", "0600", "octal mode for -snapshot-source output")
	lockOutput := flag.String("lock-output", "", "acquire a Linux output lock safely, then exec arguments after --")
	flag.Parse()
	if *redactStream {
		if *output == "" || *output == "-" {
			fail("-redact-stream requires a regular -output path")
		}
		if err := redactStreamTo(*output); err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		return
	}
	if *snapshotSource != "" {
		if *output == "" || *output == "-" {
			fail("-snapshot-source requires a regular -output path")
		}
		modeValue, err := strconv.ParseUint(*snapshotMode, 8, 32)
		if err != nil {
			fail("-snapshot-mode must be an octal file mode")
		}
		digest, err := snapshotStable(*snapshotSource, *output, os.FileMode(modeValue))
		if err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		_, _ = fmt.Fprintln(os.Stdout, digest) // Snapshot publication is already complete; retain CLI exit behavior.
		return
	}
	if *lockOutput != "" {
		if runtime.GOOS != "linux" {
			fail("-lock-output requires Linux dirfd/O_NOFOLLOW support")
		}
		if err := runLockedCommand(*lockOutput, flag.Args()); err != nil { //nolint:staticcheck // SA4023: only the non-Linux stub always fails; the Linux command can succeed.
			fail(benchrun.Redact(err.Error()))
		}
		return
	}

	if *configPath == "" {
		if *validateProgressPath != "" {
			fail("-config is required with -validate-progress")
		}
		if *validateNamespace {
			fail("-config is required with -validate-namespace")
		}
		fail("-config is required")
	}
	if *validateNamespace {
		if err := validateDedicatedNamespace(*configPath); err != nil {
			fail(benchrun.Redact(err.Error()))
		}
		return
	}
	if *validateProgressPath != "" {
		if runtime.GOOS != "linux" {
			fail("-validate-progress requires Linux descriptor-safe artifact reads")
		}
		progress, err := validateProgress(*validateProgressPath, *configPath, *allowPartialProgress)
		if err != nil {
			progress.Error = benchrun.Redact(err.Error())
		}
		progress.Warning = benchrun.Redact(progress.Warning)
		if encodeErr := json.NewEncoder(os.Stdout).Encode(progress); encodeErr != nil {
			fail(benchrun.Redact(encodeErr.Error()))
		}
		if err != nil {
			os.Exit(1)
		}
		return
	}
	if *measureSeconds < 1 || *measureSeconds > 10 {
		fail("-measure-seconds must be between 1 and 10")
	}
	if *perTargetTimeout <= 0 || *perTargetTimeout > 2*time.Hour {
		fail("-per-target-timeout must be positive and at most 2h")
	}
	cfg, err := benchrun.LoadLiveConfig(*configPath)
	if err != nil {
		fail(benchrun.Redact(err.Error()))
	}
	if cfg.Family != "H-append" || cfg.Scale != 300 {
		fail("diagnostic smoke requires the canonical H-append/300 contract")
	}
	selected, err := selectSmokeTargets(cfg, *targetSet)
	if err != nil {
		fail(err.Error())
	}
	executor, err := cfg.Executor()
	if err != nil {
		fail(benchrun.Redact(err.Error()))
	}

	started := time.Now().UTC()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(len(selected))*(*perTargetTimeout))
	defer cancel()
	results, runErr := runSmoke(ctx, cfg, executor, selected, smokePlan(cfg, *measureSeconds))
	report := smokeReport{
		SchemaVersion: benchrun.SchemaVersion, DiagnosticOnly: true,
		Family: cfg.Family, Scale: cfg.Scale, Seed: cfg.Seed,
		StartedAt: started, FinishedAt: time.Now().UTC(),
		Passed: runErr == nil && len(results) == len(selected), Results: results,
	}
	if err := writeSmokeReport(*output, report); err != nil {
		fail(benchrun.Redact(err.Error()))
	}
	if runErr != nil {
		fail(benchrun.Redact(runErr.Error()))
	}
}

func fail(message string) {
	fmt.Fprintln(os.Stderr, message)
	os.Exit(1)
}
