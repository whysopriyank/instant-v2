package benchrun

import (
	"fmt"
	"math/rand"
)

// PairOrder returns exactly seven deterministic AB/BA attempts. A Fisher-Yates
// shuffle is used so the seed is the sole source of treatment-order variation.
func PairOrder(seed int64, pairID string) ([]string, error) {
	if pairID == "" {
		return nil, fmt.Errorf("pair id is required")
	}
	r := rand.New(rand.NewSource(seed))
	out := make([]string, 7)
	for i := range out {
		if i%2 == 0 {
			out[i] = "AB"
		} else {
			out[i] = "BA"
		}
	}
	r.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out, nil
}
func Schedule(seed int64, pairID string) (RunOrder, error) {
	order, e := PairOrder(seed, pairID)
	return RunOrder{SchemaVersion: SchemaVersion, PairID: pairID, Seed: seed, Order: order}, e
}
