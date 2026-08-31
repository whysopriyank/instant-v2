package benchrun

import (
	"testing"
)

func TestCanonicalJSONPreservesLargeIntegerSeeds(t *testing.T) {
	first, err := CanonicalJSON(struct {
		Seed int64 `json:"seed"`
	}{Seed: 9007199254740993})
	if err != nil {
		t.Fatal(err)
	}
	second, err := CanonicalJSON(struct {
		Seed int64 `json:"seed"`
	}{Seed: 9007199254740994})
	if err != nil {
		t.Fatal(err)
	}
	if string(first) == string(second) || string(first) != `{"seed":9007199254740993}` {
		t.Fatalf("large integer canonicalization aliased or changed: %s / %s", first, second)
	}
}
