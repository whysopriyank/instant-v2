package main

// DA-001b: real daemon restart preserves storage bytes and metadata.
// Two (then three) distinct instantd processes share one PostgreSQL fixture
// and one durable storage root. Distinct PIDs are required — two backend
// objects in one process would not prove the packet.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/config"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

const da001StorageSecret = "da001-restart-test-secret-not-logged"

var da001HTTP = &http.Client{Timeout: 5 * time.Second}

func da001RepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot locate repo root (go.mod)")
		}
		dir = parent
	}
}

func da001BuildInstantd(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "instantd-da001")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/instantd")
	cmd.Dir = da001RepoRoot(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build instantd: %v\n%s", err, out)
	}
	return out
}

func da001FreePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

type da001Daemon struct {
	cmd  *exec.Cmd
	log  string
	addr string
	pid  int
}

func da001Start(t *testing.T, bin, dsn, root, addr string, logPath string) *da001Daemon {
	t.Helper()
	logF, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin)
	cmd.Stdout = logF
	cmd.Stderr = logF
	// Explicit environment: no insecure fallback, no temp default. Inherit
	// PATH/HOME/etc. but override every DA-001-relevant knob.
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
		"INSTANT_V2_STORAGE_ROOT="+root,
		"INSTANT_V2_STORAGE_SECRET="+da001StorageSecret,
		"INSTANT_V2_HTTP_ADDR="+addr,
		"INSTANT_V2_METRICS_ADDR=",
		"INSTANT_OAUTH_GOOGLE_CLIENT_ID=da001-google-id",
		"INSTANT_OAUTH_GOOGLE_CLIENT_SECRET=da001-google-secret",
		"INSTANT_OAUTH_GITHUB_CLIENT_ID=da001-github-id",
		"INSTANT_OAUTH_GITHUB_CLIENT_SECRET=da001-github-secret",
	)
	cmd.Env = env
	if err := cmd.Start(); err != nil {
		_ = logF.Close()
		t.Fatalf("start instantd %s: %v", addr, err)
	}
	_ = logF.Close() // child holds its own fd; close parent copy
	d := &da001Daemon{cmd: cmd, log: logPath, addr: addr, pid: cmd.Process.Pid}
	t.Cleanup(func() {
		if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
			return
		}
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _, _ = cmd.Process.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			_, _ = cmd.Process.Wait()
		}
	})
	return d
}

func da001WaitHealth(t *testing.T, addr string, wantUp bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	url := "http://" + addr + "/health"
	for time.Now().Before(deadline) {
		resp, err := da001HTTP.Get(url)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if wantUp && resp.StatusCode == http.StatusOK {
				return
			}
			if !wantUp && resp.StatusCode != http.StatusOK {
				return
			}
		} else if !wantUp {
			// Connection refused counts as down only after the process
			// has exited; callers confirm exit separately. Keep polling
			// briefly so a slow shutdown is not mistaken for lingering.
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		select {
		case <-time.After(100 * time.Millisecond):
		}
		// For wantUp=false the loop exits via deadline with failure; the
		// caller asserts port-down separately after confirming exit.
		if !wantUp {
			// Re-probe: a refused dial proves the listener is gone.
			if _, derr := net.DialTimeout("tcp", addr, 500*time.Millisecond); derr != nil {
				return
			}
		}
	}
	if wantUp {
		t.Fatalf("daemon %s never became healthy", addr)
	}
}

func da001Stop(t *testing.T, d *da001Daemon) {
	t.Helper()
	if d.cmd.ProcessState != nil && d.cmd.ProcessState.Exited() {
		t.Fatalf("daemon %s already exited before bounded stop", d.addr)
	}
	if err := d.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("SIGTERM %s: %v", d.addr, err)
	}
	done := make(chan error, 1)
	go func() { _, err := d.cmd.Process.Wait(); done <- err }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		_ = d.cmd.Process.Kill()
		_, _ = d.cmd.Process.Wait()
		t.Fatalf("daemon %s did not exit within bound", d.addr)
	}
	// Confirm no server remains on the port.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", d.addr, 300*time.Millisecond)
		if err != nil {
			return
		}
		_ = c.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("server still listening on %s after process exit", d.addr)
}

func da001NewUUID(t *testing.T) [16]byte {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return b
}

func da001LogContains(t *testing.T, logPath, needle string) string {
	t.Helper()
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), needle) {
		t.Fatalf("log %s missing %q\n--- log ---\n%s", logPath, needle, b)
	}
	return string(b)
}

func da001Upload(t *testing.T, baseURL, appStr, adminToken, filename string, payload []byte, contentType string) (id, uploadURL string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"app-id": appStr, "path": filename})
	req, err := http.NewRequest(http.MethodPost, baseURL+"/storage/signed-upload-url", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-admin-token", adminToken)
	resp, err := da001HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed-upload-url: %d %s", resp.StatusCode, raw)
	}
	var out struct {
		Data struct {
			URL       string `json:"url"`
			ID        string `json:"id"`
			ExpiresAt string `json:"expires-at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode presign: %v (%s)", err, raw)
	}
	if out.Data.URL == "" || out.Data.ID == "" || out.Data.ExpiresAt == "" {
		t.Fatalf("presign envelope incomplete: %s", raw)
	}
	if _, err := platform.ScanUUIDErr(out.Data.ID); err != nil {
		t.Fatalf("presign id not uuid: %q", out.Data.ID)
	}
	put, err := http.NewRequest(http.MethodPut, out.Data.URL, bytes.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		put.Header.Set("Content-Type", contentType)
	}
	presp, err := da001HTTP.Do(put)
	if err != nil {
		t.Fatal(err)
	}
	praw, _ := io.ReadAll(presp.Body)
	_ = presp.Body.Close()
	if presp.StatusCode != http.StatusOK {
		t.Fatalf("upload PUT: %d %s", presp.StatusCode, praw)
	}
	return out.Data.ID, out.Data.URL
}

func da001Download(t *testing.T, baseURL, appStr, adminToken, id string) (payload []byte, contentType string, contentLength string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		baseURL+"/storage/signed-download-url?app-id="+appStr+"&id="+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-admin-token", adminToken)
	resp, err := da001HTTP.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signed-download-url: %d %s", resp.StatusCode, raw)
	}
	var out struct {
		Data struct {
			URL string `json:"url"`
		} `json:"data"`
	}
	if err := json.Unmarshal(raw, &out); err != nil || out.Data.URL == "" {
		t.Fatalf("decode download presign: %v (%s)", err, raw)
	}
	get, err := http.NewRequest(http.MethodGet, out.Data.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	gresp, err := da001HTTP.Do(get)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gresp.Body.Close() }()
	got, err := io.ReadAll(gresp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if gresp.StatusCode != http.StatusOK {
		t.Fatalf("download GET: %d %s", gresp.StatusCode, got)
	}
	return got, gresp.Header.Get("Content-Type"), gresp.Header.Get("Content-Length")
}

func TestDA001DaemonRestartPreservesObjects(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	bin := da001BuildInstantd(t)

	// One durable root created outside either daemon process and owned by
	// this test; only this directory and the isolated test database may be
	// removed at cleanup.
	root, err := os.MkdirTemp("", "da001-durable-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if !filepath.IsAbs(root) {
		t.Fatalf("storage root must be absolute: %q", root)
	}

	// Expected fingerprint uses the daemon's effective quota default
	// (512MiB when INSTANT_V2_MAX_UPLOAD_BYTES is unset) and secret-present
	// posture. The secret VALUE must never appear in logs or fingerprints.
	wantFP := config.Config{
		StorageRoot:    root,
		MaxUploadBytes: 512 << 20,
		StorageSecret:  da001StorageSecret,
	}.StorageFingerprint()
	if len(wantFP) != 64 {
		t.Fatalf("fingerprint = %q", wantFP)
	}

	// --- daemon A ---
	addrA := fmt.Sprintf("127.0.0.1:%d", da001FreePort(t))
	logA := filepath.Join(t.TempDir(), "daemon-a.log")
	da := da001Start(t, bin, fixture.DSN, root, addrA, logA)
	da001WaitHealth(t, addrA, true)
	t.Logf("daemon A pid=%d addr=%s", da.pid, addrA)

	// Fail closed if the daemon silently selected a temporary root: the log
	// must name the configured root and fingerprint, never the secret.
	da001LogContains(t, logA, root)
	da001LogContains(t, logA, wantFP)
	if b, _ := os.ReadFile(logA); strings.Contains(string(b), da001StorageSecret) {
		t.Fatal("daemon log exposes storage secret")
	}

	// Supported setup: creator + app + admin token through the migrated
	// schema. The daemon migrated on boot; insert after readiness.
	appID := da001NewUUID(t)
	creatorID := da001NewUUID(t)
	adminID := da001NewUUID(t)
	appStr := platform.UUIDToStr(appID)
	adminStr := platform.UUIDToStr(adminID)
	st := storage.New(fixture.Pool)
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "da001@example.test"); err != nil {
			return err
		}
		if err := platform.CreateApp(ctx, tx, creatorID, appID, "da001-restart"); err != nil {
			return err
		}
		return platform.SetAdminToken(ctx, tx, appID, adminID)
	}); err != nil {
		t.Fatalf("seed app: %v", err)
	}

	baseA := "http://" + addrA
	payload1 := []byte("da001-durable-object-one-" + strings.Repeat("0123456789", 200))
	filename1 := "docs/restart-one.txt"
	ct1 := "text/plain; charset=utf-8"
	id1, _ := da001Upload(t, baseA, appStr, adminStr, filename1, payload1, ct1)

	gotA, gotCT1, _ := da001Download(t, baseA, appStr, adminStr, id1)
	if !bytes.Equal(gotA, payload1) {
		t.Fatal("download via A differs from upload")
	}

	// Bytes must be on the configured root (not a temp fallback).
	appDir := filepath.Join(root, appStr)
	if st, err := os.Stat(filepath.Join(appDir, id1)); err != nil || st.Size() != int64(len(payload1)) {
		t.Fatalf("object file missing under configured root: %v size=%v", err, st)
	}

	// --- terminate A, confirm port down ---
	da001Stop(t, da)
	t.Logf("daemon A pid=%d exited; port %s down", da.pid, addrA)

	// --- daemon B: same binary, same DB, same root+secret, new port ---
	addrB := fmt.Sprintf("127.0.0.1:%d", da001FreePort(t))
	if addrB == addrA {
		t.Fatal("daemon B must use a new loopback port")
	}
	logB := filepath.Join(t.TempDir(), "daemon-b.log")
	db := da001Start(t, bin, fixture.DSN, root, addrB, logB)
	da001WaitHealth(t, addrB, true)
	t.Logf("daemon B pid=%d addr=%s", db.pid, addrB)
	if db.pid == da.pid {
		t.Fatalf("daemon B pid %d equals daemon A pid; distinct processes required", db.pid)
	}
	da001LogContains(t, logB, root)
	da001LogContains(t, logB, wantFP)
	if b, _ := os.ReadFile(logB); strings.Contains(string(b), da001StorageSecret) {
		t.Fatal("daemon B log exposes storage secret")
	}

	baseB := "http://" + addrB
	gotB, gotCTB, _ := da001Download(t, baseB, appStr, adminStr, id1)
	if !bytes.Equal(gotB, payload1) {
		t.Fatal("daemon B returned different bytes for object one")
	}
	if gotCTB != gotCT1 {
		t.Fatalf("content-type changed across restart: A=%q B=%q", gotCT1, gotCTB)
	}

	// Second object via B proves the reopened root still accepts writes.
	payload2 := []byte("da001-durable-object-two-" + strings.Repeat("abcdef", 200))
	id2, _ := da001Upload(t, baseB, appStr, adminStr, "docs/restart-two.txt", payload2, ct1)
	got2B, _, _ := da001Download(t, baseB, appStr, adminStr, id2)
	if !bytes.Equal(got2B, payload2) {
		t.Fatal("daemon B second-object round trip differs")
	}

	// --- daemon C: prove both objects survive a second restart ---
	da001Stop(t, db)
	t.Logf("daemon B pid=%d exited", db.pid)
	addrC := fmt.Sprintf("127.0.0.1:%d", da001FreePort(t))
	logC := filepath.Join(t.TempDir(), "daemon-c.log")
	dc := da001Start(t, bin, fixture.DSN, root, addrC, logC)
	da001WaitHealth(t, addrC, true)
	t.Logf("daemon C pid=%d addr=%s", dc.pid, addrC)
	if dc.pid == da.pid || dc.pid == db.pid {
		t.Fatalf("daemon C pid %d reuses an earlier pid", dc.pid)
	}
	da001LogContains(t, logC, wantFP)
	baseC := "http://" + addrC
	got1C, _, _ := da001Download(t, baseC, appStr, adminStr, id1)
	got2C, _, _ := da001Download(t, baseC, appStr, adminStr, id2)
	if !bytes.Equal(got1C, payload1) || !bytes.Equal(got2C, payload2) {
		t.Fatal("daemon C lost an object across the second restart")
	}
	da001Stop(t, dc)

	// Only the isolated fixture database and the test-owned root are
	// removed (testkit drops instant_test_*; t.Cleanup removes root).
	// No other filesystem or database state is touched.
	_ = cancel
}
