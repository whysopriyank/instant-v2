package benchrun

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSchemaRoundTripAndMeasurementStates(t *testing.T) {
	in := Run{SchemaVersion: SchemaVersion, ID: "r1", PairID: "p1", TargetID: "v2", Family: "H", Scale: 300, Seed: 7, MeasuredStartedAt: time.Unix(10, 0).UTC(), MeasuredFinishedAt: time.Unix(11, 0).UTC(), PrimaryClass: Pass, Measurements: map[string]Measurement{"zero": Zero("bytes"), "missing": Missing("bytes"), "unsupported": Unsupported("bytes", "network namespace unavailable")}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Run
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Measurements["zero"].Status != StatusZero || out.Measurements["missing"].Status != StatusMissing || out.Measurements["unsupported"].Status != StatusUnsupported {
		t.Fatalf("states lost in round trip: %+v", out.Measurements)
	}
	if !out.MeasuredStartedAt.Equal(in.MeasuredStartedAt) || !out.MeasuredFinishedAt.Equal(in.MeasuredFinishedAt) {
		t.Fatalf("measured boundaries lost in round trip: %v..%v", out.MeasuredStartedAt, out.MeasuredFinishedAt)
	}
}

func TestRunSchemaRetainsIntegerSeedProperty(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("could not locate test source")
	}
	b, err := os.ReadFile(filepath.Join(filepath.Dir(sourceFile), "..", "..", "benchmarks", "schema", "run.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties["seed"].Type != "integer" {
		t.Fatalf("run schema seed property type=%q, want integer", schema.Properties["seed"].Type)
	}
}
