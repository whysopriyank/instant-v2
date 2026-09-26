package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
)

// testEvent is the subset of `go test -json` fields the harness consumes.
type testEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
	Output  string `json:"Output"`
}

// testOutcome is the final action observed for one (package, test) identity.
type testOutcome struct {
	Package string
	Test    string
	Action  string // "pass", "fail", or "skip"
	Output  string // concatenated Output frames for the identity
}

// parseGoTestJSON folds a `go test -json` stream into final per-(package,test)
// outcomes. Entries without a Test name (package-level lines) are ignored for
// counting; the final action per identity wins, matching the gate's
// group_by(.Package,.Test) | map(.[-1]) reduction.
func parseGoTestJSON(r io.Reader) (outcomes []testOutcome, packageFailed map[string]bool, err error) {
	type key struct{ pkg, test string }
	final := map[key]string{}
	outputs := map[key]string{}
	packageFailed = map[string]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev testEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue // non-JSON compiler/vendor noise is not a test identity
		}
		if ev.Test == "" {
			if ev.Action == "fail" && ev.Package != "" {
				packageFailed[ev.Package] = true
			}
			continue
		}
		k := key{ev.Package, ev.Test}
		switch ev.Action {
		case "pass", "fail", "skip":
			final[k] = ev.Action
		}
		if ev.Output != "" {
			outputs[k] += ev.Output
		}
	}
	if err := sc.Err(); err != nil {
		return nil, nil, fmt.Errorf("read go test json: %w", err)
	}
	for k, action := range final {
		if action == "fail" {
			packageFailed[k.pkg] = true
		}
		outcomes = append(outcomes, testOutcome{
			Package: k.pkg,
			Test:    k.test,
			Action:  action,
			Output:  outputs[k],
		})
	}
	sort.Slice(outcomes, func(i, j int) bool {
		if outcomes[i].Package != outcomes[j].Package {
			return outcomes[i].Package < outcomes[j].Package
		}
		return outcomes[i].Test < outcomes[j].Test
	})
	return outcomes, packageFailed, nil
}

// countOutcomes splits outcomes into passed / failed / skipped tallies.
func countOutcomes(outcomes []testOutcome) (passed, failed, skipped int) {
	for _, o := range outcomes {
		switch o.Action {
		case "pass":
			passed++
		case "fail":
			failed++
		case "skip":
			skipped++
		}
	}
	return passed, failed, skipped
}
