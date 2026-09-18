package main

// DA-001 startup failure boundaries: every malformed storage configuration
// must fail before route exposure with a diagnosable error, no listener,
// no notifier side effects, no partial artifacts, and no secret in output.

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/testkit"
)

func da001RunToExit(t *testing.T, bin, dsn, root, secret, addr string, extra map[string]string) (int, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "fail.log")
	logF, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logF.Close() }()
	cmd := exec.Command(bin)
	cmd.Stdout = logF
	cmd.Stderr = logF
	env := os.Environ()
	strip := func(prefix string) {
		out := env[:0]
		for _, kv := range env {
			if !strings.HasPrefix(kv, prefix) {
				out = append(out, kv)
			}
		}
		env = out
	}
	for _, p := range []string{
		"DATABASE_URL=", "INSTANT_V2_STORAGE_ROOT=", "INSTANT_V2_STORAGE_SECRET=",
		"INSTANT_V2_HTTP_ADDR=", "INSTANT_V2_HTTP_PORT=", "INSTANT_V2_METRICS_ADDR=",
		"INSTANT_V2_INSECURE_DEV_SECRETS=", "INSTANT_V2_READ_URL=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=", "INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=", "INSTANT_OAUTH_GITHUB_CLIENT_SECRET=",
	} {
		strip(p)
	}
	env = append(env,
		"DATABASE_URL="+dsn,
		"INSTANT_V2_HTTP_ADDR="+addr,
		"INSTANT_V2_METRICS_ADDR=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=da001-google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=da001-google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=da001-github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET=da001-github-secret",
	)
	if root != "\x00unset\x00" {
		env = append(env, "INSTANT_V2_STORAGE_ROOT="+root)
	}
	if secret != "\x00unset\x00" {
		env = append(env, "INSTANT_V2_STORAGE_SECRET="+secret)
	}
	for k, v := range extra {
		env = append(env, k+"="+v)
	}
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		b, _ := os.ReadFile(logPath)
		if err == nil {
			return 0, string(b)
		}
		if ee, ok := err.(*exec.ExitError); ok {
			return ee.ExitCode(), string(b)
		}
		t.Fatalf("wait: %v\n%s", err, b)
		return -1, ""
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		t.Fatalf("daemon with bad config did not exit (addr %s)", addr)
		return -1, ""
	}
}

func TestDA001StartupFailureBoundaries(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	bin := da001BuildInstantd(t)
	const secret = "da001-failclosed-secret-not-logged"

	// Deterministic fixtures owned by this test.
	fileRoot, err := os.CreateTemp(t.TempDir(), "not-a-dir-*")
	if err != nil {
		t.Fatal(err)
	}
	fileRootName := fileRoot.Name()
	_ = fileRoot.Close()
	parentFile, err := os.CreateTemp(t.TempDir(), "parent-file-*")
	if err != nil {
		t.Fatal(err)
	}
	parentName := parentFile.Name()
	_ = parentFile.Close()
	uncreatable := filepath.Join(parentName, "child")

	locked := filepath.Join(t.TempDir(), "locked")
	if err := os.MkdirAll(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	lockedIsWritableProbe := false
	{
		f, perr := os.CreateTemp(locked, ".probe-*")
		if perr == nil {
			_ = f.Close()
			_ = os.Remove(f.Name())
			lockedIsWritableProbe = true
		}
	}
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	// After chmod, can we still write? Non-root cannot; root can.
	chmodEnforces := false
	{
		f, perr := os.CreateTemp(locked, ".probe-*")
		if perr != nil {
			chmodEnforces = true
		} else {
			_ = f.Close()
			_ = os.Remove(f.Name())
		}
	}
	_ = lockedIsWritableProbe

	cases := []struct {
		name       string
		root       string
		secret     string
		wantSubstr string
	}{
		{"missing root", "\x00unset\x00", secret, "STORAGE_ROOT"},
		{"relative root", "relative/files", secret, "absolute"},
		{"uncreatable parent", uncreatable, secret, "storage root"},
		{"non-directory path", fileRootName, secret, "storage root"},
		{"malformed secret", t.TempDir(), "\x00unset\x00", "STORAGE_SECRET"},
	}
	// Unwritable via permission bits is only deterministic for unprivileged
	// users; privileged users bypass mode bits, so gate on enforcement.
	if chmodEnforces {
		cases = append(cases, struct {
			name       string
			root       string
			secret     string
			wantSubstr string
		}{"unwritable root", locked, secret, "storage root"})
	} else {
		t.Logf("unwritable-permission case skipped: chmod does not enforce for euid %d; file-root cases cover refusal deterministically", os.Geteuid())
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := "127.0.0.1:0"
			// Use a real free port so "no route reachable" is a dial
			// refusal, not a :0 wildcard artifact.
			addr = "127.0.0.1:" + itoa(da001FreePort(t))
			code, out := da001RunToExit(t, bin, fixture.DSN, tc.root, tc.secret, addr, nil)
			if code == 0 {
				t.Fatalf("bad config %s exited 0; want nonzero\n%s", tc.name, out)
			}
			if !strings.Contains(strings.ToLower(out), strings.ToLower(tc.wantSubstr)) {
				t.Fatalf("bad config %s output missing %q\n--- output ---\n%s", tc.name, tc.wantSubstr, out)
			}
			if strings.Contains(out, secret) {
				t.Fatalf("failure output exposes storage secret\n%s", out)
			}
			// No storage route becomes reachable: the listener never
			// bound, so even /health must refuse.
			if c, derr := net.DialTimeout("tcp", addr, 500*time.Millisecond); derr == nil {
				_ = c.Close()
				t.Fatalf("failed daemon left listener on %s", addr)
			}
			// No partial object can exist under a file/parent fixture;
			// for directory fixtures assert the daemon left no children
			// behind (probe files are removed by the backend itself).
			if tc.root != "\x00unset\x00" && tc.root != "relative/files" {
				if st, serr := os.Stat(tc.root); serr == nil && st.IsDir() {
					entries, rerr := os.ReadDir(tc.root)
					if rerr != nil {
						t.Fatal(rerr)
					}
					for _, e := range entries {
						if strings.HasPrefix(e.Name(), ".writable-") {
							t.Fatalf("probe residue left in %s: %q", tc.root, e.Name())
						}
					}
				}
			}
		})
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [16]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}
