package benchrun

import (
	"strings"
	"testing"
)

func TestPairScheduleIsSeededAndBalanced(t *testing.T) {
	a, err := PairOrder(42, "p1")
	if err != nil {
		t.Fatal(err)
	}
	b, err := PairOrder(42, "p1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(a, ",") != strings.Join(b, ",") {
		t.Fatal("schedule not deterministic")
	}
	var ab, ba int
	for _, x := range a {
		if x == "AB" {
			ab++
		}
		if x == "BA" {
			ba++
		}
	}
	if ab != 4 || ba != 3 {
		t.Fatalf("unbalanced schedule: %v", a)
	}
}
