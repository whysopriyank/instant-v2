package benchrun

import (
	"context"
	"testing"
)

func TestUnsupportedCollectorsDoNotBecomeZero(t *testing.T) {
	p, _ := UnsupportedProcessCollector{Reason: "test"}.Sample(context.Background())
	if p.RSS.Status != StatusUnsupported || p.RSS.Value != 0 {
		t.Fatalf("unsupported process became zero: %+v", p.RSS)
	}
	d, _ := UnsupportedDatabaseCollector{Reason: "test"}.Before(context.Background())
	if d.Commits.Status != StatusUnsupported {
		t.Fatalf("unsupported database status lost: %+v", d.Commits)
	}
	n, _ := (UnsupportedCollector{Reason: "network namespace unavailable"}).Sample(context.Background())
	if n.Status != StatusUnsupported {
		t.Fatalf("unsupported network status lost: %+v", n)
	}
}
