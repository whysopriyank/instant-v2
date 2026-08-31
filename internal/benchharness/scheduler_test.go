package benchharness

import (
	"context"
	"testing"
	"time"
)

func TestBlockingSchedulerDoesNotDropAndRecordsSlip(t *testing.T) {
	s, err := NewBlockingScheduler(100, time.Now().Add(10*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	items := []Mutation{{Sequence: 1}, {Sequence: 2}, {Sequence: 3}}
	var got []int64
	if err := s.Run(context.Background(), items, func(_ context.Context, m Mutation) error {
		got = append(got, m.Sequence)
		if m.Sequence == 1 {
			time.Sleep(25 * time.Millisecond)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("submitted sequence %v", got)
	}
	slips := s.Slips()
	if len(slips) != 3 {
		t.Fatalf("slip count %d", len(slips))
	}
	if slips[1].Slip < slips[0].Slip {
		t.Fatalf("slip did not reflect blocking: %#v", slips)
	}
}

func TestStallDetector(t *testing.T) {
	d := NewStallDetector(time.Second)
	now := time.Now()
	if d.Stalled(now.Add(2 * time.Second)) {
		t.Fatal("stalled before write")
	}
	d.ObserveWrite(now)
	if d.Stalled(now.Add(500 * time.Millisecond)) {
		t.Fatal("wrong early stall state")
	}
	if !d.Stalled(now.Add(2 * time.Second)) {
		t.Fatal("stall not detected")
	}
	d.ObserveReceipt(now.Add(2 * time.Second))
	if d.Stalled(now.Add(3 * time.Second)) {
		t.Fatal("receipt did not clear stall")
	}
}
