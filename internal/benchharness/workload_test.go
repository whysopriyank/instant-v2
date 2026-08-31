package benchharness

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestContractWorkloadsDeterministicMatrix(t *testing.T) {
	a, b := ContractWorkloads(7), ContractWorkloads(7)
	if len(a) != 24 || len(b) != len(a) {
		t.Fatalf("matrix lengths %d/%d", len(a), len(b))
	}
	for i := range a {
		if a[i].Family != b[i].Family || a[i].Subscribers != b[i].Subscribers || a[i].Mutation(3) != b[i].Mutation(3) {
			t.Fatalf("cell %d is not deterministic", i)
		}
	}
	if got := a[0].Mutation(1).EventID; got != "bench/7/000001" {
		t.Fatalf("event id %q", got)
	}
	for _, cell := range a {
		for id := range cell.Fixture.Entities {
			if _, err := uuid.Parse(id); err != nil {
				t.Fatalf("fixture entity id %q is not product-compatible UUID: %v", id, err)
			}
		}
		got := cell.Mutation(1).EntityID
		if _, err := uuid.Parse(got); err != nil {
			t.Fatalf("mutation entity id %q is not product-compatible UUID: %v", got, err)
		}
	}
	first, _ := NewWorkload(FamilyX, 300, 7)
	second, _ := NewWorkload(FamilyX, 300, 7)
	if first.Mutation(1).EntityID != second.Mutation(1).EntityID {
		t.Fatal("deterministic UUID entity id changed between identical manifests")
	}
	if _, err := NewWorkload(Family("bad"), 300, 1); err == nil {
		t.Fatal("unknown family accepted")
	}
	w, _ := NewWorkload(FamilyS, 300, 1)
	if got := w.Behavior(0, 10); !got.PauseReads || got.PauseFor != 2*time.Second {
		t.Fatalf("slow-reader behavior %#v", got)
	}
	if got := w.Behavior(1, 10); got.PauseReads {
		t.Fatal("non-cohort client paused")
	}
	r, _ := NewWorkload(FamilyR, 300, 1)
	if got := r.Behavior(0, 30); !got.Reconnect || got.Backoff < 500*time.Millisecond || got.Backoff > 1500*time.Millisecond {
		t.Fatalf("reconnect behavior %#v", got)
	}
}
