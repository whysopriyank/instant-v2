package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

type differentialObservations struct {
	ManifestArtifact string   `json:"corpus_manifest_artifact"`
	Evidence         []string `json:"evidence"`
}
type differentialFrames struct {
	Raw        []string          `json:"raw"`
	Normalized []json.RawMessage `json:"normalized"`
	Error      string            `json:"error,omitempty"`
}
type differentialEvidence struct {
	Scenario      string             `json:"scenario"`
	Fixture       string             `json:"fixture"`
	Mode          string             `json:"mode"`
	V1Ref         string             `json:"v1Ref,omitempty"`
	V2Ref         string             `json:"v2Ref"`
	V2Dirty       *bool              `json:"v2Dirty"`
	Normalization string             `json:"normalization"`
	Target        differentialFrames `json:"target"`
	Other         differentialFrames `json:"other"`
	Delta         *string            `json:"delta"`
}

func validateDifferential(root string, f liveFacts) (int, error) {
	var o differentialObservations
	if err := strictPublicJSON(f.Observations, &o); err != nil {
		return 0, err
	}
	if _, err := publicArtifact(root, o.ManifestArtifact); err != nil {
		return 0, err
	}
	retained, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(o.ManifestArtifact)))
	if err != nil {
		return 0, err
	}
	source, err := os.ReadFile("corpus/manifest.json")
	if err != nil {
		return 0, err
	}
	if !bytes.Equal(retained, source) {
		return 0, fmt.Errorf("differential manifest differs from candidate corpus")
	}
	var m corpus.Manifest
	if err := strictPublicJSON(source, &m); err != nil {
		return 0, err
	}
	if len(o.Evidence) != len(m.Scenarios) || len(m.Scenarios) == 0 {
		return 0, fmt.Errorf("differential scenario inventory incomplete")
	}
	wanted := map[string]corpus.ScenarioEntry{}
	for _, s := range m.Scenarios {
		wanted[s.ID] = s
	}
	for _, path := range o.Evidence {
		if _, err := publicArtifact(root, path); err != nil {
			return 0, err
		}
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
		if err != nil {
			return 0, err
		}
		var e differentialEvidence
		if err := strictPublicJSON(b, &e); err != nil {
			return 0, err
		}
		s, ok := wanted[e.Scenario]
		if !ok || e.Fixture != s.Fixture || e.Mode != "differential" || e.V1Ref != m.V1Ref || e.V2Ref != f.Identity.CandidateSHA || e.V2Dirty == nil || *e.V2Dirty || e.Normalization != "canonical-v1" || e.Delta == nil || *e.Delta != "" {
			return 0, fmt.Errorf("differential scenario %q identity or verdict invalid", e.Scenario)
		}
		// Equal truncated streams are not a completed replay. Use candidate
		// scenarios only for required frame count, not as the v1 oracle.
		scenario, err := corpus.LoadScenario(filepath.Join("corpus", s.Path))
		if err != nil {
			return 0, err
		}
		required := len(scenario.ExpectedS2C())
		if required == 0 || len(e.Target.Raw) != required || len(e.Target.Normalized) != required || len(e.Other.Raw) != required || len(e.Other.Normalized) != required {
			return 0, fmt.Errorf("differential scenario %q requires %d complete frames from each engine", e.Scenario, required)
		}
		a, err := canonicalFrames(e.Target)
		if err != nil {
			return 0, err
		}
		bframes, err := canonicalFrames(e.Other)
		if err != nil {
			return 0, err
		}
		if corpus.Diff(a, bframes) != "" {
			return 0, fmt.Errorf("differential scenario %q raw frames differ", e.Scenario)
		}
		delete(wanted, e.Scenario)
	}
	return len(o.Evidence), nil
}

func canonicalFrames(f differentialFrames) ([][]byte, error) {
	if f.Error != "" || len(f.Raw) == 0 || len(f.Raw) != len(f.Normalized) {
		return nil, fmt.Errorf("differential raw frame inventory/error invalid")
	}
	out := make([][]byte, 0, len(f.Raw))
	for i, raw := range f.Raw {
		canonical, err := corpus.CanonicalBytesOpts([]byte(raw), corpus.CanonicalOptions{Differential: true})
		if err != nil {
			return nil, err
		}
		normalized, err := corpus.CanonicalBytes(f.Normalized[i])
		if err != nil || !bytes.Equal(canonical, normalized) {
			return nil, fmt.Errorf("differential normalization does not match raw frame")
		}
		out = append(out, canonical)
	}
	return out, nil
}
