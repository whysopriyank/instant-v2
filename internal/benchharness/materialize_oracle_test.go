package benchharness

import (
	"testing"
)

func TestPrefixOracleFullDeltaAndCanonicalDigest(t *testing.T) {
	w, _ := NewWorkload(FamilyH, 300, 3)
	o := NewPrefixOracle(w.Fixture)
	q := w.Fixture.Queries[0].ID
	before, _ := o.ExpectedDigest(q, 0)
	m := w.Mutation(1)
	o.Append(m)
	after, _ := o.Materialize(q, 1)
	afterDigest, _ := after.Digest()
	if before == afterDigest {
		t.Fatal("mutation did not change digest")
	}
	delta := Delta{QueryID: q, Adds: []Entity{{ID: m.EntityID, Bucket: 0, Rank: 1, Attributes: map[string]any{"value": m.Marker}}}}
	replayed, err := ApplyDelta(Materialized{QueryID: q, Entities: map[string]Entity{}}, delta)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := replayed.Entities[m.EntityID]; !ok {
		t.Fatal("delta add missing")
	}
	if _, err := o.ExpectedDigest("missing", 1); err == nil {
		t.Fatal("missing query accepted")
	}
}
