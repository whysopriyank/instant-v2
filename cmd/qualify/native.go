package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"time"
)

// laneResult is the intermediate machine-readable output of the native,
// recovery, and soak subcommands. `record` assembles a gate-schema external
// record from exactly this shape plus campaign identity flags.
type laneResult struct {
	Packet        string         `json:"packet"`
	SelectedCount int            `json:"selected_count"`
	SkippedCount  int            `json:"skipped_count"`
	Result        string         `json:"result"` // PASS or FAIL
	Details       map[string]any `json:"details"`
	Artifacts     []ArtifactRef  `json:"artifacts"`
	StartedAt     string         `json:"started_at"`
	FinishedAt    string         `json:"finished_at"`
}

func runNative(args []string) int {
	fs := flag.NewFlagSet("native", flag.ContinueOnError)
	logPath := fs.String("log", "", "path to `go test -json` output to parse (alternative to --run)")
	doRun := fs.Bool("run", false, "execute the owned-DB + hermetic test selection and capture its -json log")
	out := fs.String("out", "", "lane result JSON output path (required)")
	rawLog := fs.String("raw-log", "", "when --run, also retain the raw go test -json stream at this path")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *out == "" {
		fmt.Fprintln(os.Stderr, "native: --out is required")
		return 2
	}
	started := time.Now().UTC()
	var (
		stream io.Reader
		raw    []byte
	)
	switch {
	case *doRun:
		if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" {
			fmt.Fprintln(os.Stderr, "native: refusing owned-DB run without INSTANT_TEST_INTEGRATION=1")
			return 2
		}
		full, hermetic, err := runNativeSelection()
		if err != nil {
			return failLane(*out, "OP-003", started, map[string]any{"error": err.Error()}, nil)
		}
		raw = append(full, hermetic...)
		stream = bytes.NewReader(raw)
		if *rawLog != "" {
			if err := os.WriteFile(*rawLog, raw, 0o600); err != nil {
				fmt.Fprintln(os.Stderr, "native:", err)
				return 1
			}
		}
	case *logPath != "":
		f, err := os.Open(*logPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "native:", err)
			return 2
		}
		defer func() { _ = f.Close() }()
		stream = f
	default:
		fmt.Fprintln(os.Stderr, "native: one of --run or --log is required")
		return 2
	}

	outcomes, _, err := parseGoTestJSON(stream)
	if err != nil {
		fmt.Fprintln(os.Stderr, "native:", err)
		return 1
	}
	return writeNativeResult(*out, outcomes, started, time.Now().UTC(), raw)
}

// writeNativeResult folds parsed outcomes into the OP-003 lane verdict.
// Exported from runNative for hermetic unit tests.
func writeNativeResult(out string, outcomes []testOutcome, started, finished time.Time, _ []byte) int {
	passed, failed, skipped := countOutcomes(outcomes)
	plat, platOK := checkPlatformCoverage(outcomes)
	native := isNativePlatform()
	result := "PASS"
	if failed > 0 || skipped > 0 || !platOK || !native {
		result = "FAIL"
	}
	details := map[string]any{
		"native":          native,
		"platform_checks": plat.Passed,
	}
	lr := laneResult{
		Packet:        "OP-003",
		SelectedCount: passed,
		SkippedCount:  skipped,
		Result:        result,
		Details:       details,
		Artifacts:     []ArtifactRef{},
		StartedAt:     started.Format(time.RFC3339),
		FinishedAt:    finished.Format(time.RFC3339),
	}
	if result == "FAIL" {
		diag := map[string]any{
			"failed":               failed,
			"platform_missing":     plat.Missing,
			"platform_nopass":      plat.Failed,
			"native_runtime_check": native,
		}
		raw, _ := json.Marshal(diag)
		details["failure_diagnostic"] = string(raw)
	}
	if err := writeJSONFile(out, lr); err != nil {
		fmt.Fprintln(os.Stderr, "native:", err)
		return 1
	}
	if result == "FAIL" {
		fmt.Fprintf(os.Stderr, "native: FAIL selected=%d skipped=%d failed=%d platform_passed=%d missing=%v nopass=%v native=%v\n",
			passed, skipped, failed, plat.Passed, plat.Missing, plat.Failed, native)
		return 1
	}
	fmt.Fprintf(os.Stderr, "native: PASS selected=%d platform_checks=%d\n", passed, plat.Passed)
	return 0
}

// runNativeSelection executes the same selection as `make test-integration`
// (owned DB, no -short) plus the hermetic short lane, returning both raw
// -json streams.
func runNativeSelection() (ownedDB, hermetic []byte, err error) {
	ownedDB, err = goTestJSON("-run", ".", "-count=1")
	if err != nil {
		return nil, nil, fmt.Errorf("owned-DB selection: %w", err)
	}
	hermetic, err = goTestJSON("-run", ".", "-count=1", "-short")
	if err != nil {
		return ownedDB, nil, fmt.Errorf("hermetic selection: %w", err)
	}
	return ownedDB, hermetic, nil
}

func goTestJSON(extra ...string) ([]byte, error) {
	args := append([]string{"test", "./...", "-race", "-count=1", "-json"}, extra...)
	cmd := exec.Command("go", args...)
	cmd.Env = os.Environ()
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return out, fmt.Errorf("go test failed: %v\n%s", err, ee.Stderr)
		}
		return out, fmt.Errorf("go test failed: %w", err)
	}
	return out, nil
}

// osArch reports the build GOARCH; factored for tests.
func osArch() string { return runtime.GOARCH }

// isNativePlatform reports whether the qualify binary runs natively on the
// Linux qualification target (not cross-compiled). It is a variable so
// hermetic unit tests can pin both branches without a Linux host.
var isNativePlatform = func() bool {
	return runtime.GOOS == "linux" && runtime.GOARCH == osArch() && !crossCompiled()
}

// crossCompiled reports whether the qualify binary was cross-compiled
// relative to its host (GOOS/GOARCH env overrides differ from runtime).
// In a normal native build both are empty and this is false.
func crossCompiled() bool {
	if goos := os.Getenv("GOOS"); goos != "" && goos != runtime.GOOS {
		return true
	}
	if goarch := os.Getenv("GOARCH"); goarch != "" && goarch != runtime.GOARCH {
		return true
	}
	return false
}
