// Command corpusctl drives Instant's golden-corpus lifecycle.
// See docs/05-conformance.md for the design.
//
// Modes:
//   --mode replay --target ws://host/runtime/session [--corpus dir] [--suite filter]
//   --mode differential --target ws://v2 --other ws://v1
//   --mode record (future — requires live v1 + browser proxy)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

var (
	flagMode    = flag.String("mode", "replay", "one of: replay, differential, record")
	flagCorpus  = flag.String("corpus", "corpus", "path to corpus dir (or a single .ndjson file)")
	flagSuite   = flag.String("suite", "", "optional substring filtering scenario suites / filenames")
	flagTarget  = flag.String("target", "", "WebSocket URL of the server under test (replay/differential)")
	flagOther   = flag.String("other", "", "second WebSocket URL for differential mode")
	flagTimeout = flag.Duration("timeout", 12*time.Second, "per-scenario timeout")
	flagV1Path  = flag.String("v1-path", filepath.Join("..", "instant"), "local checkout path for v1 (advisory, record mode)")
	flagV1Ref   = flag.String("v1-ref", "", "pinned V1_REF commit (overrides file)")
)

func main() {
	flag.Parse()
	switch *flagMode {
	case "replay":
		runReplay()
	case "differential":
		runDifferential()
	case "record":
		fmt.Fprintln(os.Stderr, "record mode requires a live v1 at --v1-path and the SDK proxy (tools/corpusctl/README.md); not yet — author scenarios directly under corpus/")
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "unknown --mode %q; want replay|differential|record\n", *flagMode)
		os.Exit(2)
	}
}

func resolveCorpus() ([]*corpus.Scenario, error) {
	corpusPath := *flagCorpus
	fi, err := os.Stat(corpusPath)
	if err != nil {
		return nil, err
	}
	if !fi.IsDir() {
		sc, err := corpus.LoadScenario(corpusPath)
		if err != nil {
			return nil, err
		}
		if *flagSuite != "" && !strings.Contains(sc.Meta.Suite, *flagSuite) && !strings.Contains(filepath.Base(corpusPath), *flagSuite) {
			return nil, nil
		}
		return []*corpus.Scenario{sc}, nil
	}
	return corpus.LoadCorpus(corpusPath, *flagSuite)
}

func runReplay() {
	if *flagTarget == "" {
		fmt.Fprintln(os.Stderr, "--target ws://... is required for replay mode")
		os.Exit(2)
	}
	_ = *flagV1Path
	_ = *flagV1Ref
	scenarios, err := resolveCorpus()
	if err != nil {
		fatal("load corpus: %v", err)
	}
	if len(scenarios) == 0 {
		fmt.Printf("corpusctl replay: no scenarios matched (corpus=%s, suite=%q)\n", *flagCorpus, *flagSuite)
		return
	}
	ctx := context.Background()
	failures := 0
	for _, sc := range scenarios {
		label := filepath.Base(sc.File)
		if sc.Meta.Suite != "" {
			label = sc.Meta.Suite + " (" + label + ")"
		}
		res := corpus.Replay(ctx, *flagTarget, sc, *flagTimeout)
		if res.Err != nil {
			fmt.Printf("FAIL %s: %v\n", label, res.Err)
			failures++
			continue
		}
		if !res.Passed {
			fmt.Printf("FAIL %s (%dms):\n%s\n", label, res.DurationMS, res.Delta)
			failures++
			continue
		}
		fmt.Printf("PASS %s (%dms)\n", label, res.DurationMS)
	}
	if failures > 0 {
		fmt.Printf("\n%d/%d scenarios failed\n", failures, len(scenarios))
		os.Exit(1)
	}
	fmt.Printf("\n%d/%d scenarios passed\n", len(scenarios), len(scenarios))
}

func runDifferential() {
	if *flagTarget == "" || *flagOther == "" {
		fmt.Fprintln(os.Stderr, "differential needs --target ws://v2 and --other ws://v1")
		os.Exit(2)
	}
	scenarios, err := resolveCorpus()
	if err != nil {
		fatal("load corpus: %v", err)
	}
	if len(scenarios) == 0 {
		fmt.Printf("corpusctl differential: no scenarios matched\n")
		return
	}
	ctx := context.Background()
	failures := 0
	for _, sc := range scenarios {
		label := filepath.Base(sc.File)
		if sc.Meta.Suite != "" {
			label = sc.Meta.Suite + " (" + label + ")"
		}
		_, _, delta := corpus.Differential(ctx, sc, *flagTarget, *flagOther, *flagTimeout)
		if delta != "" {
			fmt.Printf("DIFF %s:\n%s\n", label, delta)
			failures++
			continue
		}
		fmt.Printf("EQUAL %s\n", label)
	}
	if failures > 0 {
		fmt.Printf("\n%d/%d scenarios differ\n", failures, len(scenarios))
		os.Exit(1)
	}
	fmt.Printf("\n%d/%d scenarios equal\n", len(scenarios), len(scenarios))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "corpusctl: "+format+"\n", args...)
	os.Exit(1)
}
