// Command corpusctl validates and replays the WS corpus. See corpus/README.md.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

type options struct {
	mode, corpusDir, suite, target, other, v1Path, v1Ref, outputDir string
	timeout                                                         time.Duration
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, diagnostic io.Writer) int {
	var o options
	flags := flag.NewFlagSet("corpusctl", flag.ContinueOnError)
	flags.SetOutput(diagnostic)
	flags.StringVar(&o.mode, "mode", "replay", "validate, replay, differential, or record (unavailable)")
	flags.StringVar(&o.corpusDir, "corpus", "corpus", "corpus directory or one .ndjson file")
	flags.StringVar(&o.suite, "suite", "", "suite/filename substring")
	flags.StringVar(&o.target, "target", "", "target WebSocket URL")
	flags.StringVar(&o.other, "other", "", "pinned v1 WebSocket URL for differential")
	flags.StringVar(&o.v1Path, "v1-path", "../instant", "local pinned v1 checkout")
	flags.StringVar(&o.v1Ref, "v1-ref", "", "expected v1 commit (defaults to corpus manifest)")
	flags.StringVar(&o.outputDir, "output-dir", "", "new private evidence files (required for differential)")
	flags.DurationVar(&o.timeout, "timeout", 12*time.Second, "per-scenario timeout")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	fail := func(err error) int { _, _ = fmt.Fprintf(diagnostic, "corpusctl: %v\n", err); return 1 }
	if flags.NArg() != 0 {
		return fail(fmt.Errorf("unexpected positional arguments"))
	}
	switch o.mode {
	case "validate":
		if o.suite != "" {
			return fail(fmt.Errorf("validate checks the entire corpus; --suite is not supported"))
		}
		report, err := corpus.ValidateCorpus(o.corpusDir)
		if err != nil {
			return fail(err)
		}
		if _, err := fmt.Fprint(out, report); err != nil {
			return fail(err)
		}
		return 0
	case "record":
		return fail(fmt.Errorf("record unavailable: SDK/HTTP capture is not implemented; authored scenarios must be labeled spec or regression"))
	case "replay", "differential":
	default:
		return fail(fmt.Errorf("unknown mode %q", o.mode))
	}
	if o.target == "" {
		return fail(fmt.Errorf("--target is required"))
	}
	if o.mode == "differential" {
		if o.other == "" || o.outputDir == "" {
			return fail(fmt.Errorf("differential requires --other and --output-dir; independently provision equivalent isolated fixtures first"))
		}
		manifestDir := o.corpusDir
		if filepath.Ext(manifestDir) == ".ndjson" {
			manifestDir = filepath.Dir(manifestDir)
		}
		manifest, err := corpus.LoadManifest(manifestDir)
		if err != nil {
			return fail(err)
		}
		if o.v1Ref == "" {
			o.v1Ref = manifest.V1Ref
		}
		if o.v1Ref != manifest.V1Ref {
			return fail(fmt.Errorf("--v1-ref must equal the full manifest pin"))
		}
		ref, err := revision(o.v1Path)
		if err != nil {
			return fail(fmt.Errorf("read v1 revision: %w", err))
		}
		if ref != o.v1Ref {
			return fail(fmt.Errorf("v1 checkout revision does not match manifest pin"))
		}
	}
	scenarios, err := resolveCorpus(o.corpusDir, o.suite)
	if err != nil {
		return fail(err)
	}
	if len(scenarios) == 0 {
		return fail(fmt.Errorf("no scenarios matched"))
	}
	failures := 0
	for _, sc := range scenarios {
		var a, b corpus.ReplayResult
		var delta string
		if o.mode == "replay" {
			a = corpus.Replay(context.Background(), o.target, sc, o.timeout)
			delta = a.Delta
		} else {
			a, b, delta = corpus.Differential(context.Background(), sc, o.target, o.other, o.timeout)
		}
		if o.outputDir != "" {
			if err := writeEvidence(o, sc, a, b, delta); err != nil {
				return fail(err)
			}
		}
		if a.Err != nil || b.Err != nil || delta != "" {
			if _, err := fmt.Fprintf(out, "FAIL %s: target=%v other=%v\n%s\n", sc.Meta.Suite, a.Err, b.Err, delta); err != nil {
				return fail(err)
			}
			failures++
		} else {
			if _, err := fmt.Fprintf(out, "PASS %s (%s)\n", sc.Meta.Suite, o.mode); err != nil {
				return fail(err)
			}
		}
	}
	if _, err := fmt.Fprintf(out, "%d/%d scenarios passed\n", len(scenarios)-failures, len(scenarios)); err != nil {
		return fail(err)
	}
	if failures > 0 {
		return 1
	}
	return 0
}

func resolveCorpus(path, suite string) ([]*corpus.Scenario, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if fi.IsDir() {
		return corpus.LoadCorpus(path, suite)
	}
	sc, err := corpus.LoadScenario(path)
	if err != nil {
		return nil, err
	}
	if suite != "" && !strings.Contains(sc.Meta.Suite, suite) && !strings.Contains(filepath.Base(path), suite) {
		return nil, nil
	}
	return []*corpus.Scenario{sc}, nil
}

func revision(path string) (string, error) {
	b, err := exec.Command("git", "-C", path, "rev-parse", "HEAD").Output()
	return strings.TrimSpace(string(b)), err
}

type frameEvidence struct {
	Raw        []string          `json:"raw"`
	Normalized []json.RawMessage `json:"normalized"`
	Error      string            `json:"error,omitempty"`
}

func evidenceOf(result corpus.ReplayResult) frameEvidence {
	out := frameEvidence{Raw: []string{}, Normalized: []json.RawMessage{}}
	for _, raw := range result.RawCollected {
		out.Raw = append(out.Raw, string(raw))
	}
	for _, raw := range result.Collected {
		out.Normalized = append(out.Normalized, json.RawMessage(raw))
	}
	if result.Err != nil {
		out.Error = result.Err.Error()
	}
	return out
}

// Evidence contains full raw frames, which can carry auth/application data.
// Files are private, write-once, and never automatically promoted to v1 oracles.
func writeEvidence(o options, sc *corpus.Scenario, a, b corpus.ReplayResult, delta string) error {
	v2Ref, err := revision(".")
	if err != nil {
		return err
	}
	dirty, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return err
	}
	record := struct {
		Scenario      string        `json:"scenario"`
		Fixture       string        `json:"fixture"`
		Mode          string        `json:"mode"`
		V1Ref         string        `json:"v1Ref,omitempty"`
		V2Ref         string        `json:"v2Ref"`
		V2Dirty       bool          `json:"v2Dirty"`
		Normalization string        `json:"normalization"`
		Target        frameEvidence `json:"target"`
		Other         frameEvidence `json:"other"`
		Delta         string        `json:"delta"`
	}{sc.Meta.Suite, sc.Meta.SeedFixture, o.mode, o.v1Ref, v2Ref, len(dirty) > 0, "canonical-v1", evidenceOf(a), evidenceOf(b), delta}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(o.outputDir, 0700); err != nil {
		return err
	}
	name := filepath.Base(sc.File) + ".evidence.json"
	f, err := os.OpenFile(filepath.Join(o.outputDir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(append(data, '\n'))
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}
