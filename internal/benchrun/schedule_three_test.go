package benchrun

import (
	"reflect"
	"testing"
)

func TestThreeTargetOrderIsDeterministicAndBalanced(t *testing.T) {
	a, err := ThreeTargetOrder(17, "three-target")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ThreeTargetOrder(17, "three-target")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("same seed produced different schedule:\n%v\n%v", a, b)
	}
	if err := ValidateThreeTargetOrder(a); err != nil {
		t.Fatal(err)
	}
	permutations := make(map[string]int)
	for _, block := range a {
		key := block.Order[0] + ">" + block.Order[1] + ">" + block.Order[2]
		permutations[key]++
	}
	if len(permutations) != 6 {
		t.Fatalf("want six distinct permutations plus one repeat, got %v", permutations)
	}
}

func TestThreeTargetOrderRejectsMalformedSchedule(t *testing.T) {
	blocks, err := ThreeTargetOrder(17, "three-target")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit func([]ScheduleBlock) []ScheduleBlock
	}{
		{"missing block", func(in []ScheduleBlock) []ScheduleBlock { return in[:6] }},
		{"duplicate target", func(in []ScheduleBlock) []ScheduleBlock {
			in[0].Order[1] = in[0].Order[0]
			return in
		}},
		{"wrong index", func(in []ScheduleBlock) []ScheduleBlock {
			in[0].Index = 2
			return in
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			in := make([]ScheduleBlock, len(blocks))
			for i, block := range blocks {
				in[i] = ScheduleBlock{Index: block.Index, Order: append([]string(nil), block.Order...)}
			}
			if err := ValidateThreeTargetOrder(test.edit(in)); err == nil {
				t.Fatal("malformed schedule was accepted")
			}
		})
	}
}

func TestThreeTargetScheduleSerializesBlocksWithoutPairOrder(t *testing.T) {
	schedule, err := ThreeTargetSchedule(17, "three-target")
	if err != nil {
		t.Fatal(err)
	}
	if len(schedule.Order) != ThreeTargetBlocks || len(schedule.Blocks) != ThreeTargetBlocks {
		t.Fatalf("unexpected serialized schedule: %+v", schedule)
	}
}
