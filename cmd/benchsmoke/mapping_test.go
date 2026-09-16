package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("failed to determine repository root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "Makefile")); err != nil {
		t.Fatalf("Makefile not found at repo root %s: %v", root, err)
	}
	return root
}

func readRepoFile(t *testing.T, relPath string) string {
	t.Helper()
	root := repoRoot(t)
	fullPath := filepath.Join(root, relPath)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", relPath, err)
	}
	return string(data)
}

func TestPackageSelection(t *testing.T) {
	makefile := readRepoFile(t, "Makefile")

	reBenchPackages := regexp.MustCompile(`(?m)^BENCH_PACKAGES\s*:?=\s*(.+)$`)
	matches := reBenchPackages.FindStringSubmatch(makefile)
	if len(matches) < 2 {
		t.Fatal("BENCH_PACKAGES definition not found in Makefile")
	}
	benchPackagesLine := matches[1]
	packages := strings.Fields(benchPackagesLine)

	expectedRequired := []string{
		"./internal/benchharness",
		"./internal/benchrun",
		"./cmd/soak",
		"./cmd/soaksetup",
		"./cmd/benchrun",
		"./cmd/benchreport",
	}
	for _, req := range expectedRequired {
		found := false
		for _, pkg := range packages {
			if pkg == req {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("BENCH_PACKAGES missing required supported package: %s", req)
		}
	}

	for _, pkg := range packages {
		if strings.Contains(pkg, "benchsmoke") {
			t.Fatalf("BENCH_PACKAGES must not contain benchsmoke; got %s", pkg)
		}
	}

	reBenchAcceptance := regexp.MustCompile(`(?m)^bench-acceptance:`)
	if !reBenchAcceptance.MatchString(makefile) {
		t.Fatal("bench-acceptance target not found in Makefile")
	}
}

func TestBuildTargetMapping(t *testing.T) {
	makefile := readRepoFile(t, "Makefile")

	// build-bench must not build benchsmoke.
	reBuildBench := regexp.MustCompile(`(?ms)^build-bench:.*?\n\t(.*?)(?:\n\n|\n[a-zA-Z_-]+:)`)
	match := reBuildBench.FindStringSubmatch(makefile)
	if len(match) < 2 {
		t.Fatal("build-bench target recipe not found in Makefile")
	}
	buildBenchRecipe := match[1]

	if strings.Contains(buildBenchRecipe, "benchsmoke") {
		t.Fatal("build-bench recipe must not build or mention benchsmoke")
	}

	for _, expectedBin := range []string{"instantd", "benchrun", "benchreport", "soak", "soaksetup"} {
		if !strings.Contains(buildBenchRecipe, expectedBin) {
			t.Errorf("build-bench recipe missing expected supported binary %s", expectedBin)
		}
	}

	// build-historical-benchsmoke must exist and compile ./cmd/benchsmoke
	reHistorical := regexp.MustCompile(`(?ms)^build-historical-benchsmoke:.*?\n\t(.*?)(?:\n\n|\n[a-zA-Z_-]+:)`)
	histMatch := reHistorical.FindStringSubmatch(makefile)
	if len(histMatch) < 2 {
		t.Fatal("build-historical-benchsmoke target recipe not found in Makefile")
	}
	histRecipe := histMatch[1]
	if !strings.Contains(histRecipe, "./cmd/benchsmoke") {
		t.Fatalf("build-historical-benchsmoke must compile ./cmd/benchsmoke; got %s", histRecipe)
	}
}

func TestBenchSmokeTargetMapping(t *testing.T) {
	makefile := readRepoFile(t, "Makefile")

	reBenchSmoke := regexp.MustCompile(`(?ms)^bench-smoke:.*?\n(\t.*?)(?:\n\n|\n[a-zA-Z_-]+:)`)
	match := reBenchSmoke.FindStringSubmatch(makefile)
	if len(match) < 2 {
		t.Fatal("bench-smoke target recipe not found in Makefile")
	}
	recipe := match[1]

	if !strings.Contains(recipe, `"$(BIN)/soaksetup"`) {
		t.Fatalf("bench-smoke must execute soaksetup; recipe: %s", recipe)
	}
	if strings.Contains(recipe, "benchsmoke") {
		t.Fatalf("bench-smoke recipe must not execute benchsmoke; recipe: %s", recipe)
	}

	reHeader := regexp.MustCompile(`(?m)^bench-smoke:[^\n]*##[^\n]*$`)
	headerLine := reHeader.FindString(makefile)
	if headerLine == "" {
		t.Fatal("bench-smoke target header with help text not found in Makefile")
	}
	if !strings.Contains(headerLine, "soaksetup") {
		t.Errorf("bench-smoke help text must explicitly mention soaksetup mapping; got %q", headerLine)
	}
}

func TestCompileHistoricalBenchsmoke(t *testing.T) {
	root := repoRoot(t)
	tempDir := t.TempDir()
	outBin := filepath.Join(tempDir, "benchsmoke")

	cmd := exec.Command("go", "build", "-o", outBin, "./cmd/benchsmoke")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build historical cmd/benchsmoke: %v\nOutput:\n%s", err, string(output))
	}
	info, err := os.Stat(outBin)
	if err != nil {
		t.Fatalf("compiled binary not found: %v", err)
	}
	if info.Size() == 0 {
		t.Fatal("compiled binary is empty")
	}
}

func TestHistoricalBoundaryDocumentation(t *testing.T) {
	mainSrc := readRepoFile(t, "cmd/benchsmoke/main.go")

	if !strings.Contains(mainSrc, "DEC-001") {
		t.Error("cmd/benchsmoke/main.go doc comment must cite DEC-001 ruling")
	}
	if !strings.Contains(strings.ToLower(mainSrc), "historical") {
		t.Error("cmd/benchsmoke/main.go doc comment must designate command as historical")
	}
	reNotSupported := regexp.MustCompile(`(?i)not\s+a\s+supported\s+acceptance\s+entrypoint`)
	if !reNotSupported.MatchString(mainSrc) {
		t.Error("cmd/benchsmoke/main.go doc comment must state it is not a supported acceptance entrypoint")
	}
}

func TestReferenceDocsAlignment(t *testing.T) {
	archDoc := readRepoFile(t, "docs/reference/02-architecture.md")
	if strings.Contains(archDoc, "benchsmoke/            bounded live smoke and evidence CLI") {
		t.Error("docs/reference/02-architecture.md still contains false claim 'bounded live smoke and evidence CLI'")
	}
	if !strings.Contains(archDoc, "benchsmoke/") || !strings.Contains(archDoc, "historical") {
		t.Error("docs/reference/02-architecture.md must record benchsmoke as historical")
	}

	scorecardDoc := readRepoFile(t, "docs/reference/quality-scorecard.md")
	reScorecard := regexp.MustCompile(`(?m)^\|\s*` + "`cmd/benchsmoke`" + `\s*\|.*$`)
	scorecardLine := reScorecard.FindString(scorecardDoc)
	if scorecardLine == "" {
		t.Fatal("cmd/benchsmoke row not found in docs/reference/quality-scorecard.md")
	}
	if !strings.Contains(scorecardLine, "Historical") && !strings.Contains(scorecardLine, "historical") {
		t.Errorf("quality-scorecard.md cmd/benchsmoke responsibility must note historical status; got: %s", scorecardLine)
	}
	if !strings.Contains(scorecardLine, "DEC-001") {
		t.Errorf("quality-scorecard.md cmd/benchsmoke responsibility must reference DEC-001; got: %s", scorecardLine)
	}
}
