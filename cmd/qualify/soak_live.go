package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func contextTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}

func contextCancel() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// sampleProcessIdentity returns an opaque start-instance+binary token for the
// candidate process (Linux /proc starttime + exe link; ps fallback). The soak
// lane compares pre/post samples like scripts/quality-soak.sh: any change
// means replacement and fails the lane.
func sampleProcessIdentity(proc *exec.Cmd) string {
	if proc == nil || proc.Process == nil {
		return ""
	}
	pid := proc.Process.Pid
	token := ""
	if raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)); err == nil {
		s := string(raw)
		if i := indexByteReverse(s, ')'); i >= 0 {
			fields := splitFields(s[i+2:])
			if len(fields) >= 20 {
				token = "starttime=" + fields[19]
			}
		}
	}
	exe, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		exe = proc.Path
	}
	return fmt.Sprintf("pid=%d %s exe=%s", pid, token, exe)
}

func indexByteReverse(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func splitFields(s string) []string {
	var out []string
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == ' ' || s[i] == '\t' || s[i] == '\n' {
			if start >= 0 {
				out = append(out, s[start:i])
				start = -1
			}
		} else if start < 0 {
			start = i
		}
	}
	if start >= 0 {
		out = append(out, s[start:])
	}
	return out
}

// sampleTimeSeries appends {t, rss_kb, fds, pg_connections} rows every 10s
// until done is closed. Failures are recorded as rows with an error field;
// sampling never fails the lane by itself.
func sampleTimeSeries(done <-chan struct{}, path string, pid int, databaseURL string) {
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	sample := func() {
		row := map[string]any{"t": time.Now().UTC().Format(time.RFC3339)}
		if rss, fds, err := procStats(pid); err != nil {
			row["error"] = err.Error()
		} else {
			row["rss_kb"] = rss
			row["open_fds"] = fds
		}
		if n, err := pgConnectionCount(databaseURL); err != nil {
			row["pg_error"] = "unavailable"
		} else {
			row["pg_connections"] = n
		}
		_ = enc.Encode(row)
	}
	sample()
	for {
		select {
		case <-done:
			return
		case <-tick.C:
			sample()
		}
	}
}

func procStats(pid int) (rssKB, fds int64, err error) {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/statm", pid))
	if err != nil {
		return 0, 0, err
	}
	var residentPages int64
	fields := splitFields(string(raw))
	if len(fields) < 2 {
		return 0, 0, fmt.Errorf("parse statm")
	}
	if _, err := fmt.Sscanf(fields[1], "%d", &residentPages); err != nil {
		return 0, 0, fmt.Errorf("parse statm: %w", err)
	}
	pageKB := int64(os.Getpagesize() / 1024)
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", pid))
	if err != nil {
		return residentPages * pageKB, -1, nil
	}
	return residentPages * pageKB, int64(len(entries)), nil
}

func pgConnectionCount(databaseURL string) (int, error) {
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
