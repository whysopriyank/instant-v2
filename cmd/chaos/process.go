package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type instantdProcessIdentity struct {
	PID          int    `json:"pid"`
	StartTime    int64  `json:"start_time"`
	BinarySHA256 string `json:"binary_sha256"`
	Addr         string `json:"addr"`
}

type instantdInstance struct {
	Cmd      *exec.Cmd
	Identity instantdProcessIdentity
}

func verifyInstantdIdentity(inst *instantdInstance) error {
	if inst == nil || inst.Cmd == nil || inst.Cmd.Process == nil {
		return errors.New("instantd process identity is unavailable")
	}
	token, err := processStartToken(inst.Cmd.Process.Pid)
	if err != nil {
		return err
	}
	if token != inst.Identity.StartTime {
		return fmt.Errorf("instantd kernel start identity changed for pid %d", inst.Identity.PID)
	}
	got, err := sha256File(inst.Cmd.Path)
	if err != nil {
		return err
	}
	if got != inst.Identity.BinarySHA256 {
		return fmt.Errorf("instantd binary identity changed for pid %d", inst.Identity.PID)
	}
	return nil
}

func (inst *instantdInstance) Stop() error {
	if inst == nil || inst.Cmd == nil || inst.Cmd.Process == nil {
		return nil
	}
	if err := verifyInstantdIdentity(inst); err != nil {
		return fmt.Errorf("refuse to kill instantd: %w", err)
	}
	if err := inst.Cmd.Process.Kill(); err != nil {
		return fmt.Errorf("kill instantd pid %d: %w", inst.Identity.PID, err)
	}
	_, _ = inst.Cmd.Process.Wait()
	return nil
}

func startInstantd(bin, dsn, addr string) (*instantdInstance, error) {
	hash, err := sha256File(bin)
	if err != nil {
		return nil, fmt.Errorf("hash instantd binary before start: %w", err)
	}
	cmd := exec.Command(bin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(),
		"DATABASE_URL="+dsn,
		"INSTANT_V2_HTTP_ADDR="+addr,
	)
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	startTime, err := processStartToken(cmd.Process.Pid)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	if currentHash, hashErr := sha256File(bin); hashErr != nil || currentHash != hash {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		if hashErr != nil {
			return nil, fmt.Errorf("verify instantd binary after start: %w", hashErr)
		}
		return nil, fmt.Errorf("instantd binary changed between hash and exec")
	}
	return &instantdInstance{
		Cmd: cmd,
		Identity: instantdProcessIdentity{
			PID:          cmd.Process.Pid,
			StartTime:    startTime,
			BinarySHA256: hash,
			Addr:         addr,
		},
	}, nil
}

func buildInstantd(repoRoot, output string) ([]byte, error) {
	cmd := exec.Command("go", "build", "-o", output, "./cmd/instantd")
	cmd.Dir = repoRoot
	return cmd.CombinedOutput()
}

func buildWorkingTree(repoRoot, output string, build func(string, string) ([]byte, error)) ([]byte, error) {
	out, err := build(repoRoot, output)
	if err != nil {
		return out, fmt.Errorf("working-tree build failed; no HEAD fallback is permitted: %w", err)
	}
	return out, nil
}

var pgHelperExecutable = os.Executable

// pgBinInDir runs a Postgres helper with an already-open data-directory file
// descriptor as its working directory. The child receives -D . and therefore
// cannot be redirected by replacing the validated pathname between checks.
func pgBinInDir(path string, expected os.FileInfo, name string, args ...string) (string, error) {
	if !pgHelperAvailable() {
		return "", errors.New("identity-bound Postgres helper is unsupported on this platform")
	}
	dir, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open pg-data for %s: %w", name, err)
	}
	defer func() { _ = dir.Close() }()
	info, err := dir.Stat()
	if err != nil {
		return "", fmt.Errorf("stat pg-data for %s: %w", name, err)
	}
	if expected == nil || !os.SameFile(expected, info) || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("--pg-data changed before identity-bound Postgres start")
	}
	full := append([]string{name}, args...)
	pgPath := name
	if !filepath.IsAbs(name) {
		pgPath = filepath.Join("/opt/homebrew/opt/postgresql@17/bin", name)
		if _, statErr := os.Stat(pgPath); statErr != nil {
			pgPath, err = exec.LookPath(name)
			if err != nil {
				return "", fmt.Errorf("find %s: %w", name, err)
			}
		}
	}
	helper, err := pgHelperExecutable()
	if err != nil {
		return "", fmt.Errorf("find chaos helper executable: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	// os/exec applies Cmd.Dir before ExtraFiles are installed. The chaos
	// executable's helper mode therefore performs fchdir(3) after fd 3 exists,
	// then execs the absolute Postgres binary from that identity-bound cwd.
	cmdArgs := append([]string{pgPath}, full[1:]...)
	cmd := exec.CommandContext(ctx, helper, cmdArgs...)
	cmd.Dir = "/"
	cmd.ExtraFiles = []*os.File{dir}
	cmd.Env = append(os.Environ(),
		"PATH=/opt/homebrew/opt/postgresql@17/bin:"+os.Getenv("PATH"),
		"INSTANT_V2_CHAOS_PG_HELPER=1")
	out, runErr := cmd.CombinedOutput()
	if runErr == nil {
		return string(out), nil
	}
	var ee *exec.ExitError
	if errors.As(runErr, &ee) {
		return string(out), fmt.Errorf("%v: %s", full, strings.TrimSpace(string(out)))
	}
	return string(out), fmt.Errorf("%s: %w", full[0], runErr)
}

func stopInstantd(inst *instantdInstance) error {
	if inst == nil {
		return nil
	}
	return inst.Stop()
}

var sendPostmasterSignal = func(signal, pid string) error {
	return exec.Command("kill", signal, pid).Run()
}

func freezeAndThawPostmaster(pid string, hold time.Duration) error {
	if err := sendPostmasterSignal("-STOP", pid); err != nil {
		return fmt.Errorf("SIGSTOP postmaster %s: %w", pid, err)
	}
	time.Sleep(hold)
	if err := sendPostmasterSignal("-CONT", pid); err != nil {
		return fmt.Errorf("SIGCONT postmaster %s: %w", pid, err)
	}
	return nil
}

func freezeAndThawPostmasterIdentity(ident postmasterProcessIdentity, pgData string, expected os.FileInfo, hold time.Duration) error {
	if token, err := processStartToken(ident.PID); err != nil || token != ident.StartTime {
		if err != nil {
			return fmt.Errorf("verify postmaster kernel identity before SIGSTOP: %w", err)
		}
		return fmt.Errorf("postmaster kernel start identity changed before SIGSTOP")
	}
	current, err := readPostmasterIdentity(pgData, expected)
	if err != nil {
		return fmt.Errorf("verify postmaster before SIGSTOP: %w", err)
	}
	if current != ident {
		return fmt.Errorf("postmaster identity changed before SIGSTOP (expected %+v, got %+v)", ident, current)
	}
	pidStr := strconv.Itoa(ident.PID)
	if err := sendPostmasterSignal("-STOP", pidStr); err != nil {
		return fmt.Errorf("SIGSTOP postmaster %s: %w", pidStr, err)
	}
	time.Sleep(hold)
	current, err = readPostmasterIdentity(pgData, expected)
	if err != nil {
		return fmt.Errorf("verify postmaster before SIGCONT: %w", err)
	}
	if current != ident {
		return fmt.Errorf("postmaster identity changed during SIGSTOP (expected %+v, got %+v)", ident, current)
	}
	if err := sendPostmasterSignal("-CONT", pidStr); err != nil {
		return fmt.Errorf("SIGCONT postmaster %s: %w", pidStr, err)
	}
	current, err = readPostmasterIdentity(pgData, expected)
	if err != nil {
		return fmt.Errorf("verify postmaster after SIGCONT: %w", err)
	}
	if current != ident {
		return fmt.Errorf("postmaster identity changed after SIGCONT (expected %+v, got %+v)", ident, current)
	}
	return nil
}

func pollTailerError(errCh <-chan error) error {
	select {
	case err := <-errCh:
		if err == nil {
			return errors.New("waltail stopped unexpectedly")
		}
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	default:
		return nil
	}
}

func waitTailer(done <-chan struct{}, errCh <-chan error, timeout time.Duration) error {
	select {
	case <-done:
	case <-time.After(timeout):
		return fmt.Errorf("waltail did not stop within %s", timeout)
	}
	return pollTailerErrorBlocking(errCh)
}

func pollTailerErrorBlocking(errCh <-chan error) error {
	err := <-errCh
	if err == nil {
		return errors.New("waltail stopped unexpectedly")
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

// chaosHTTP bounds every probe so a wedged handler cannot hang the harness.
// waitHealth polls /health until it matches wantUp (200 = up, anything else
// including connection errors counts as down).
var chaosHTTP = &http.Client{Timeout: 5 * time.Second}

func waitHealth(baseURL string, timeout time.Duration, wantUp bool) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := chaosHTTP.Get(baseURL + "/health")
		if err != nil {
			last = err.Error()
		} else {
			_ = resp.Body.Close()
			last = fmt.Sprintf("status %d", resp.StatusCode)
			if (resp.StatusCode == http.StatusOK) == wantUp {
				return nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	state := "healthy"
	if !wantUp {
		state = "degraded"
	}
	return fmt.Errorf("/health never became %s within %s (last: %s)", state, timeout, last)
}

var corpusSummaryRE = regexp.MustCompile(`(?m)^(\d+)/(\d+) scenarios passed$`)

func runCorpusReplay(repoRoot, wsURL string) (string, error) {
	return runCorpusReplayWith(repoRoot, wsURL, func(cmd *exec.Cmd) ([]byte, error) {
		return cmd.CombinedOutput()
	})
}

func runCorpusReplayWith(repoRoot, wsURL string, runCommand func(*exec.Cmd) ([]byte, error)) (string, error) {
	cmd := corpusReplayCommand(repoRoot, wsURL)
	out, err := runCommand(cmd)
	verb := "exit 0"
	if err != nil {
		verb = err.Error()
	}
	report := fmt.Sprintf("$ corpusctl --mode replay --target %s → %s\n%s",
		wsURL, verb, indent(strings.TrimSpace(string(out))))
	if err != nil {
		return report, fmt.Errorf("subprocess failed: %w", err)
	}
	match := corpusSummaryRE.FindStringSubmatch(string(out))
	if len(match) != 3 {
		return report, fmt.Errorf("replay produced no complete scenario summary")
	}
	passed, parseErr := strconv.Atoi(match[1])
	if parseErr != nil {
		return report, fmt.Errorf("parse replay passed count: %w", parseErr)
	}
	total, parseErr := strconv.Atoi(match[2])
	if parseErr != nil {
		return report, fmt.Errorf("parse replay total count: %w", parseErr)
	}
	if total == 0 {
		return report, errors.New("replay selected zero scenarios")
	}
	if passed != total {
		return report, fmt.Errorf("replay completed with %d/%d scenarios passed", passed, total)
	}
	return report, nil
}

func corpusReplayCommand(repoRoot, wsURL string) *exec.Cmd {
	commandPath := "./cmd/corpusctl"
	// Keep the command rooted in the caller-selected candidate; callers verify
	// its fingerprint immediately before invoking replay.
	if _, err := os.Stat(filepath.Join(repoRoot, "cmd", "corpusctl", "main.go")); os.IsNotExist(err) {
		if _, err := os.Stat(filepath.Join(repoRoot, "tools", "corpusctl", "main.go")); err == nil {
			commandPath = "./tools/corpusctl"
		}
	}
	cmd := exec.Command("go", "run", commandPath,
		"--mode", "replay",
		"--target", wsURL,
		"--corpus", "corpus",
		"--timeout", "12s")
	cmd.Dir = repoRoot
	return cmd
}

func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			panic("chaos: cannot locate repo root (go.mod) from cwd")
		}
		dir = parent
	}

}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
