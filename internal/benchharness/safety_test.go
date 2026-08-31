package benchharness

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestSafetyGuards(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1:1234/runtime/session", "http://localhost:1/runtime/session"} {
		if err := ValidateLoopbackURL(raw); err != nil {
			t.Fatalf("loopback rejected: %v", err)
		}
	}
	if err := ValidateLoopbackURL("http://example.com"); err == nil {
		t.Fatal("public URL accepted")
	}
	if err := ValidateLoopbackURL("http://localhost.localdomain:1"); err == nil {
		t.Fatal("localhost.localdomain shortcut accepted")
	}
	if err := ValidateDatabaseURL("postgres://u:p@127.0.0.1:5432/instant_bench_x"); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDatabaseURL("postgres://u:p@127.0.0.1:5432/postgres"); err == nil {
		t.Fatal("non-benchmark DB accepted")
	}
	for _, raw := range []string{
		"postgres://u:p@127.0.0.1:5432/instant_bench_x?host=192.0.2.1",
		"postgres://u:p@127.0.0.1:5432/instant_bench_x?hostaddr=192.0.2.1",
		"host=127.0.0.1,192.0.2.1 port=5432 dbname=instant_bench_x",
		"host=127.0.0.1 hostaddr=192.0.2.1 dbname=instant_bench_x",
		"host=127.0.0.1 service=unsafe dbname=instant_bench_x",
		"host=127.0.0.1 fallback=192.0.2.1 dbname=instant_bench_x",
	} {
		if err := ValidateDatabaseURL(raw); err == nil {
			t.Fatalf("unsafe database DSN accepted: %q", raw)
		}
	}
	if name, err := DatabaseName("host=127.0.0.1 port=5432 dbname=instant_bench_keyword user=bench"); err != nil || name != "instant_bench_keyword" {
		t.Fatalf("keyword DSN name %q/%v", name, err)
	}
	root := t.TempDir()
	if _, err := SafeBundlePath(root, "../outside"); err == nil {
		t.Fatal("path traversal accepted")
	}
	if got, err := SafeBundlePath(root, "runs/a.json"); err != nil || filepath.Dir(got) != filepath.Join(root, "runs") {
		t.Fatalf("safe path %q/%v", got, err)
	}
	if err := (ResetGuard{}).Validate("instant_bench_x"); err == nil {
		t.Fatal("empty marker accepted")
	}
	_ = os.ErrNotExist
}

func TestQualificationRejectsNonLoopbackBeforeChecker(t *testing.T) {
	called := false
	q := Qualify(context.Background(), "v2", SessionOptions{URL: "https://example.com"}, fakeHealth{called: &called})
	if q.Passed || called {
		t.Fatalf("qualification did not fail closed: %#v called=%v", q, called)
	}
}

type fakeHealth struct{ called *bool }

func (f fakeHealth) Health(context.Context) error { *f.called = true; return nil }

func (f fakeHealth) LiveProbe(context.Context, int) error { return nil }

func TestSafeBundlePathRootSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	target := t.TempDir()
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Skip("symlink unavailable")
	}
	if _, err := SafeBundlePath(link, "x"); err == nil {
		t.Fatal("symlink root accepted")
	}
}
