package main

import (
	"strings"
	"testing"
)

func TestParseGoTestJSONFinalActionWins(t *testing.T) {
	log := strings.Join([]string{
		`{"Action":"run","Package":"example/a","Test":"TestFoo"}`,
		`{"Action":"output","Package":"example/a","Test":"TestFoo","Output":"    a_test.go:10: boom\n"}`,
		`{"Action":"fail","Package":"example/a","Test":"TestFoo","Time":"2026-01-01T00:00:00Z"}`,
		`{"Action":"run","Package":"example/a","Test":"TestFoo"}`,
		`{"Action":"pass","Package":"example/a","Test":"TestFoo"}`,
		`{"Action":"pass","Package":"example/a","Test":"TestBar"}`,
		`{"Action":"skip","Package":"example/a","Test":"TestBaz"}`,
		`{"Action":"output","Package":"example/a","Test":"TestSub/child","Output":"    x_test.go:1: in short mode\n"}`,
		`{"Action":"skip","Package":"example/a","Test":"TestSub/child"}`,
		`{"Action":"fail","Package":"example/a"}`,
		`not json at all`,
		``,
	}, "\n")
	outcomes, pkgFailed, err := parseGoTestJSON(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	byTest := map[string]string{}
	for _, o := range outcomes {
		byTest[o.Test] = o.Action
	}
	if byTest["TestFoo"] != "pass" {
		t.Fatalf("final action must win: TestFoo=%q", byTest["TestFoo"])
	}
	if byTest["TestBar"] != "pass" || byTest["TestBaz"] != "skip" || byTest["TestSub/child"] != "skip" {
		t.Fatalf("unexpected outcomes: %v", byTest)
	}
	if len(outcomes) != 4 {
		t.Fatalf("want 4 identities, got %d", len(outcomes))
	}
	passed, failed, skipped := countOutcomes(outcomes)
	if passed != 2 || failed != 0 || skipped != 2 {
		t.Fatalf("counts = %d/%d/%d", passed, failed, skipped)
	}
	if !pkgFailed["example/a"] {
		t.Fatal("package-level fail must mark package failed")
	}
	for _, o := range outcomes {
		if o.Test == "TestSub/child" && !strings.Contains(o.Output, "in short mode") {
			t.Fatal("Output frames must accumulate per identity")
		}
	}
}

func TestParseGoTestJSONFailCounts(t *testing.T) {
	log := `{"Action":"fail","Package":"example/a","Test":"TestFoo"}` + "\n"
	outcomes, _, err := parseGoTestJSON(strings.NewReader(log))
	if err != nil {
		t.Fatal(err)
	}
	passed, failed, skipped := countOutcomes(outcomes)
	if passed != 0 || failed != 1 || skipped != 0 {
		t.Fatalf("counts = %d/%d/%d", passed, failed, skipped)
	}
}
