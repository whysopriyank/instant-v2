package transact

import (
	"testing"
)

func TestOperationGroupsByFirstAppearance(t *testing.T) {
	if order := orderSteps(nil); len(order) != 0 {
		t.Fatalf("expected empty order, got %d", len(order))
	}

	// Each operation appears once, in first-appearance order. Later occurrences
	// remain in their operation group, matching the v1 dispatcher.
	steps := []Step{
		{Op: "add-triple"},
		{Op: "add-triple"},
		{Op: "retract-triple"},
		{Op: "add-triple"},
		{Op: "add-triple"},
		{Op: "deep-merge-triple"},
		{Op: "deep-merge-triple"},
		{Op: "deep-merge-triple"},
		{Op: "delete-entity"},
	}
	order := orderSteps(steps)
	expected := []string{"add-triple", "retract-triple", "deep-merge-triple", "delete-entity"}
	if len(order) != len(expected) {
		t.Fatalf("expected %d operation groups, got %d", len(expected), len(order))
	}
	for i, exp := range expected {
		if order[i] != exp {
			t.Errorf("group %d: expected op %q, got %q", i, exp, order[i])
		}
	}
}
