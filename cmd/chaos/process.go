package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func startInstantd(bin, dsn, addr string) (*exec.Cmd, error) {
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
	return cmd, nil
}

func stopInstantd(p *exec.Cmd) {
	if p == nil || p.Process == nil {
		return
	}
	_ = p.Process.Kill()
	_, _ = p.Process.Wait()
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

func runCorpusReplay(repoRoot, wsURL string) string {
	cmd := corpusReplayCommand(repoRoot, wsURL)
	out, err := cmd.CombinedOutput()
	verb := "exit 0"
	if err != nil {
		verb = err.Error()
	}
	return fmt.Sprintf("$ corpusctl --mode replay --target %s → %s\n%s",
		wsURL, verb, indent(strings.TrimSpace(string(out))))
}

func corpusReplayCommand(repoRoot, wsURL string) *exec.Cmd {
	commandPath := "./cmd/corpusctl"
	// The existing build fallback can export a pre-migration HEAD checkout.
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

// exportHead exports the repo's HEAD commit into a temp dir (read-only with
// respect to the live working tree) and returns its path.
func exportHead(repoRoot string) (string, error) {
	dir, err := os.MkdirTemp("", "chaos-src-*")
	if err != nil {
		return "", err
	}
	tarball := filepath.Join(dir, "..", filepath.Base(dir)+"-head.tar")
	defer func() { _ = os.Remove(tarball) }()
	arch := exec.Command("git", "archive", "--format=tar", "-o", tarball, "HEAD")
	arch.Dir = repoRoot
	if out, err := arch.CombinedOutput(); err != nil {
		return "", fmt.Errorf("git archive: %v\n%s", err, out)
	}
	untar := exec.Command("tar", "-xf", tarball, "-C", dir)
	if out, err := untar.CombinedOutput(); err != nil {
		return "", fmt.Errorf("tar extract: %v\n%s", err, out)
	}
	return dir, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
