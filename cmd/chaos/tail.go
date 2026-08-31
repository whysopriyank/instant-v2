package main

import (
	"context"
	"github.com/instant-v2/instant-v2/internal/waltail"
	"sync"
)

type tailStats struct {
	mu        sync.Mutex
	seen      map[string]bool // entity -> true (dedup across crash)
	total     int             // raw record count incl. replays
	lastLSN   uint64
	appFilter string
}

func (ts *tailStats) handle(_ context.Context, rec waltail.Record) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if ts.appFilter != "" && rec.AppID != ts.appFilter {
		return nil
	}
	ts.total++
	ts.lastLSN = rec.LSN
	ts.seen[rec.EntityID] = true
	return nil
}

func (ts *tailStats) snapshot() (seen, total int, lsn uint64) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return len(ts.seen), ts.total, ts.lastLSN
}
