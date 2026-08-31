package testkit

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate quality target tests")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "..", ".."))
}

func TestQualityTargetsFailMissingPrerequisites(t *testing.T) {
	if _, err := exec.LookPath("make"); err != nil {
		t.Fatal("quality target tests require make")
	}
	for _, tc := range []struct {
		name string
		args []string
		want string
	}{
		{"lint", []string{"lint", "LINT=false"}, "Error"},
		{"integration", []string{"test-integration", "DATABASE_URL="}, "DATABASE_URL is required"},
		{"contract", []string{"test-contract", "DATABASE_URL="}, "DATABASE_URL is required"},
		{"differential", []string{"differential", "V1_URL=", "V2_URL="}, "V1_URL and V2_URL"},
		{"replay", []string{"replay", "TARGET="}, "TARGET must be a seeded WebSocket endpoint"},
		{"corpus-check", []string{"corpus-check", "V1_URL="}, "V1_URL must be a seeded WebSocket endpoint"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("make", tc.args...)
			cmd.Dir = repositoryRoot(t)
			cmd.Env = append(withoutFixtureEnv(os.Environ()), "DATABASE_URL=")
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("make %v: got err=%v output=%s", tc.args, err, output)
			}
		})
	}
}

func TestReplayTargetPreservesEmptySuiteArgument(t *testing.T) {
	// An invalid URL cannot open a socket; argument parsing must still reach
	// replay instead of treating --corpus as the value of an empty --suite.
	cmd := exec.Command("make", "--silent", "replay", "TARGET=not-a-websocket-url", "SUITE=")
	cmd.Dir = repositoryRoot(t)
	output, err := cmd.CombinedOutput()
	if err == nil || strings.Contains(string(output), "unexpected positional arguments") || !strings.Contains(string(output), "FAIL 00-smoke") {
		t.Fatalf("empty suite did not reach replay: err=%v output=%s", err, output)
	}
}

func TestIntegrationAuditRejectsLegacyFixtures(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		fail bool
	}{
		{"shared reset", "DROP SCHEMA public CASCADE", true},
		{"direct lookup", `os.Getenv("DATABASE_URL")`, true},
		{"isolated", "testkit.NewPostgres(t, testkit.PostgresOptions{})", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Mkdir(filepath.Join(dir, "internal"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "internal", "fixture_test.go"), []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("bash", filepath.Join(repositoryRoot(t), "scripts", "quality-integration.sh"))
			cmd.Dir = dir
			output, err := cmd.CombinedOutput()
			if (err != nil) != tc.fail {
				t.Fatalf("fixture audit fail=%t: err=%v output=%s", tc.fail, err, output)
			}
		})
	}
}

func TestUnitTargetClearsLiveDatabaseEnvironment(t *testing.T) {
	cmd := exec.Command("make", "-n", "test-unit")
	cmd.Dir = repositoryRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "INSTANT_TEST_INTEGRATION=0 DATABASE_URL= TEST_DATABASE_URL=") {
		t.Fatalf("unit command must disable live fixtures: %s, %v", output, err)
	}
}

func TestDifferentialTargetChecksRevisionInOneShell(t *testing.T) {
	root := repositoryRoot(t)
	checkout := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		command := exec.Command("git", append([]string{"-c", "user.name=Quality fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null"}, args...)...)
		command.Dir = checkout
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("fixture git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	git("init", "--quiet")
	git("commit", "--quiet", "--allow-empty", "-m", "baseline")
	previous := git("rev-parse", "HEAD")
	git("commit", "--quiet", "--allow-empty", "-m", "current")
	head := git("rev-parse", "HEAD")
	base := []string{"--silent", "differential", "V1_URL=ws://127.0.0.1:1", "V2_URL=ws://127.0.0.1:2",
		"DIFFERENTIAL_OUTPUT=" + filepath.Join(t.TempDir(), "unused"), "V1_PATH=" + checkout, "GO=echo"}
	for _, tc := range []struct {
		name string
		ref  string
		fail bool
	}{
		{"mismatched", previous, true},
		{"matching", "HEAD", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd := exec.Command("make", append(base, "V1_REF="+tc.ref)...)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			if tc.fail {
				if err == nil || !strings.Contains(string(output), "checkout must match V1_REF") {
					t.Fatalf("mismatched checkout accepted: err=%v output=%s", err, output)
				}
				return
			}
			if err != nil || !strings.Contains(string(output), "--v1-ref "+head) {
				t.Fatalf("full pin not forwarded: err=%v output=%s", err, output)
			}
		})
	}
}
