package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// pgBin runs a Postgres binary with Homebrew's postgres@17 bin dir prepended
// to PATH so the harness works regardless of the caller's environment.
func pgBin(name string, args ...string) (string, error) {
	full := append([]string{name}, args...)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, full[0], full[1:]...)
	cmd.Env = append(os.Environ(),
		"PATH=/opt/homebrew/opt/postgresql@17/bin:"+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err == nil {
		return string(out), nil
	}
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return string(out), fmt.Errorf("%v: %s", full, strings.TrimSpace(string(out)))
	}
	return "", fmt.Errorf("%s: %w", full[0], err)
}

func pgCtl(pgData string, args ...string) (string, error) {
	return pgBin("pg_ctl", append([]string{"-D", pgData}, args...)...)
}

func cleanupCluster(pgData string) {
	fmt.Println("== [9] cleanup: stopping throwaway cluster + removing datadir ==")
	if out, err := pgCtl(pgData, "-m", "fast", "-w", "stop"); err != nil {
		fmt.Printf("   warning: pg_ctl stop: %v\n%s", err, out)
	}
	mustRm(pgData)
}

func mustRm(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		panic(fmt.Sprintf("rm %s: %v", dir, err))
	}
}

func pidOfPostmaster(pgData string) string {
	b, err := os.ReadFile(filepath.Join(pgData, "postmaster.pid"))
	if err != nil {
		return ""
	}
	line, _, _ := strings.Cut(string(b), "\n")
	return strings.TrimSpace(line)
}
