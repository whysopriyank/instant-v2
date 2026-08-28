package main

import (
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchrun"
)

func TestBenchmarkTimeoutCoversAllAttempts(t *testing.T) {
	plan := benchrun.Plan{RampSeconds: 2, SettleSeconds: 3, WarmupSeconds: 4, MeasureSeconds: 5, GraceSeconds: 6}
	want := 14 * time.Duration(2+3+4+5+6+60) * time.Second
	if got := benchmarkTimeout("H-append", plan, nil); got != want {
		t.Fatalf("timeout=%s want=%s", got, want)
	}
}

func TestBenchmarkTimeoutTIsHardBoundPerAttempt(t *testing.T) {
	plan := benchrun.Plan{RampSeconds: 30, WarmupSeconds: 60, MeasureSeconds: 600, GraceSeconds: 30}
	if got, max := benchmarkTimeout("T-saturation", plan, nil), 14*time.Duration(600+60)*time.Second; got != max {
		t.Fatalf("T timeout=%s want=%s", got, max)
	}
}
