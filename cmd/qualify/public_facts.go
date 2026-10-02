package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

// Runtime producers retain these observations. This adapter validates them;
// it never launches a server and never converts a caller's PASS into evidence.
type liveIdentity struct {
	CandidateSHA        string `json:"candidate_sha"`
	BinarySHA256        string `json:"binary_sha256"`
	ConfigurationSHA256 string `json:"configuration_sha256"`
	EndpointSHA256      string `json:"endpoint_sha256"`
	ImageDigest         string `json:"image_digest"`
	FixtureID           string `json:"fixture_id"`
	FixtureSHA256       string `json:"fixture_sha256"`
}

type liveFacts struct {
	SchemaVersion    int             `json:"schema_version"`
	MeasurementClass string          `json:"measurement_class"`
	Identity         liveIdentity    `json:"identity"`
	StartedAt        string          `json:"started_at"`
	FinishedAt       string          `json:"finished_at"`
	Observations     json.RawMessage `json:"observations"`
}

type containerObservations struct {
	UID                   int      `json:"uid"`
	HealthStatus          int      `json:"health_status"`
	ReadyStatus           int      `json:"ready_status"`
	TLSVerified           bool     `json:"tls_verified"`
	TransactAcked         bool     `json:"transact_acked"`
	SubscriptionConverged bool     `json:"subscription_converged"`
	ObjectBefore          string   `json:"object_sha256_before"`
	ObjectAfter           string   `json:"object_sha256_after"`
	DrainSeconds          *float64 `json:"drain_seconds"`
	PortReleased          bool     `json:"port_released"`
}

type restoreFormat struct {
	Format          string  `json:"format"`
	TripleCount     int64   `json:"triple_count"`
	DurationSeconds float64 `json:"duration_seconds"`
	LogicalExpected string  `json:"logical_sha256_expected"`
	LogicalActual   string  `json:"logical_sha256_actual"`
	ObjectsExpected string  `json:"objects_sha256_expected"`
	ObjectsActual   string  `json:"objects_sha256_actual"`
}

type restoreRejection struct {
	ID           string `json:"id"`
	Error        string `json:"error"`
	TargetBefore string `json:"target_sha256_before"`
	TargetAfter  string `json:"target_sha256_after"`
	SourceBefore string `json:"source_sha256_before"`
	SourceAfter  string `json:"source_sha256_after"`
}

type restoreObservations struct {
	Formats    []restoreFormat    `json:"formats"`
	Rejections []restoreRejection `json:"rejections"`
}

func strictPublicJSON(b []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected exactly one JSON value")
	}
	return nil
}

func validateLiveIdentity(i liveIdentity) error {
	if !isHex40(i.CandidateSHA) || !isHex64(i.BinarySHA256) || !isHex64(i.ConfigurationSHA256) ||
		!isHex64(i.EndpointSHA256) || !imageDigest(i.ImageDigest) || i.FixtureID == "" || !isHex64(i.FixtureSHA256) {
		return fmt.Errorf("runtime facts identity is incomplete")
	}
	return nil
}

func validateLiveFacts(packet string, f liveFacts) (int, error) {
	if f.SchemaVersion != 1 || f.MeasurementClass != "live" {
		return 0, fmt.Errorf("runtime facts must be schema 1 and measurement_class live")
	}
	if err := validateLiveIdentity(f.Identity); err != nil {
		return 0, err
	}
	started, err := time.Parse(time.RFC3339, f.StartedAt)
	if err != nil {
		return 0, fmt.Errorf("runtime facts started_at invalid")
	}
	finished, err := time.Parse(time.RFC3339, f.FinishedAt)
	if err != nil || finished.Before(started) {
		return 0, fmt.Errorf("runtime facts finished_at invalid or reversed")
	}
	switch packet {
	case "CF-005":
		return 1, nil // Full raw-frame inventory is checked with its evidence root.
	case "OP-004":
		var o containerObservations
		if err := strictPublicJSON(f.Observations, &o); err != nil {
			return 0, err
		}
		if o.UID != 65532 || o.HealthStatus != 200 || o.ReadyStatus != 200 || !o.TLSVerified ||
			!o.TransactAcked || !o.SubscriptionConverged || !isHex64(o.ObjectBefore) || o.ObjectBefore != o.ObjectAfter ||
			o.DrainSeconds == nil || *o.DrainSeconds < 0 || *o.DrainSeconds > 30 || !o.PortReleased {
			return 0, fmt.Errorf("container startup, identity, TLS, storage, delivery or drain assertion failed")
		}
		return 9, nil
	case "OP-006":
		var o restoreObservations
		if err := strictPublicJSON(f.Observations, &o); err != nil {
			return 0, err
		}
		formats := map[string]bool{"v1zip": true, "v2ndjson": true}
		if len(o.Formats) != len(formats) {
			return 0, fmt.Errorf("restore requires both v1zip and v2ndjson")
		}
		for _, r := range o.Formats {
			if !formats[r.Format] || r.TripleCount < 1000000 || r.DurationSeconds <= 0 || r.DurationSeconds > 600 ||
				!isHex64(r.LogicalExpected) || r.LogicalExpected != r.LogicalActual ||
				!isHex64(r.ObjectsExpected) || r.ObjectsExpected != r.ObjectsActual {
				return 0, fmt.Errorf("restore %q exact state or capacity assertion failed", r.Format)
			}
			delete(formats, r.Format)
		}
		rejections := map[string]bool{"truncated": true, "corrupt": true, "wrong-app": true, "oversized": true, "non-empty": true}
		if len(o.Rejections) != len(rejections) {
			return 0, fmt.Errorf("restore rejection inventory incomplete")
		}
		for _, r := range o.Rejections {
			if !rejections[r.ID] || r.Error == "" || !isHex64(r.TargetBefore) || r.TargetBefore != r.TargetAfter ||
				!isHex64(r.SourceBefore) || r.SourceBefore != r.SourceAfter {
				return 0, fmt.Errorf("restore rejection %q did not preserve source and target", r.ID)
			}
			delete(rejections, r.ID)
		}
		return 7, nil
	default:
		return 0, fmt.Errorf("runtime facts adapter unavailable for packet %q", packet)
	}
}

func runPublicFacts(packet string, args []string) int {
	fs := flag.NewFlagSet("public-facts", flag.ContinueOnError)
	root := fs.String("evidence-root", "", "evidence root")
	facts := fs.String("facts", "", "relative immutable runtime observations JSON")
	out := fs.String("out", "", "lane result output path")
	if err := fs.Parse(args); err != nil || *root == "" || *facts == "" || *out == "" {
		return 2
	}
	if _, err := publicArtifact(*root, *facts); err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	b, err := os.ReadFile(filepath.Join(*root, filepath.FromSlash(*facts)))
	if err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	var f liveFacts
	if err := strictPublicJSON(b, &f); err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	n, err := validateLiveFacts(packet, f)
	if packet == "CF-005" && err == nil {
		n, err = validateDifferential(*root, f)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	var observations map[string]any
	if err := json.Unmarshal(f.Observations, &observations); err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	lr := laneResult{Packet: packet, SelectedCount: n, Result: "PASS", SkippedCount: 0,
		Details:   map[string]any{"measurement_class": "live", "facts_artifact": *facts, "observations": observations},
		StartedAt: f.StartedAt, FinishedAt: f.FinishedAt, Identity: &f.Identity}
	if err := writeJSONFile(*out, lr); err != nil {
		fmt.Fprintln(os.Stderr, "public-facts:", err)
		return 1
	}
	return 0
}

func validatePublicSource(root string, r gateRecord) error {
	path, ok := r.Details["facts_artifact"].(string)
	if !ok || path == "" || r.Details["measurement_class"] != "live" || len(r.Details) != 3 {
		return fmt.Errorf("live facts reference required")
	}
	bound := false
	for _, a := range r.Artifacts {
		if a.Path == path {
			bound = true
		}
	}
	if !bound {
		return fmt.Errorf("runtime facts absent from artifact inventory")
	}
	if _, err := publicArtifact(root, path); err != nil {
		return err
	}
	b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		return err
	}
	var f liveFacts
	if err := strictPublicJSON(b, &f); err != nil {
		return err
	}
	if r.Packet == "CF-005" {
		var o differentialObservations
		if err := strictPublicJSON(f.Observations, &o); err != nil {
			return err
		}
		refs := append(append([]string{}, o.Evidence...), o.ManifestArtifact)
		for _, path := range refs {
			found := false
			for _, a := range r.Artifacts {
				if a.Path == path {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("differential raw source %q absent from artifact inventory", path)
			}
		}
	}
	i := liveIdentity{r.CandidateSHA, r.BinarySHA256, r.ConfigurationSHA256, r.EndpointSHA256, r.ImageDigest, r.FixtureID, r.FixtureSHA256}
	if f.Identity != i || f.StartedAt != r.StartedAt || f.FinishedAt != r.FinishedAt {
		return fmt.Errorf("runtime source facts identity/time mismatch")
	}
	n, err := validateLiveFacts(r.Packet, f)
	if r.Packet == "CF-005" && err == nil {
		n, err = validateDifferential(root, f)
	}
	if err != nil || n != r.SelectedCount {
		return fmt.Errorf("runtime source facts assertions/count failed: %v", err)
	}
	var observations map[string]any
	if err := json.Unmarshal(f.Observations, &observations); err != nil {
		return err
	}
	if !reflect.DeepEqual(observations, r.Details["observations"]) {
		return fmt.Errorf("runtime source observations differ from record")
	}
	return nil
}
