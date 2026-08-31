package corpus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// echoServer is a minimal WS server used by replay tests.
// For every text frame received, it echoes the configured reply (if any).
func echoServer(t *testing.T, replyFor func(op string) []byte) *httptest.Server {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		ctx := r.Context()
		for {
			typ, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			var env map[string]json.RawMessage
			_ = json.Unmarshal(data, &env)
			var op string
			_ = json.Unmarshal(env["op"], &op)
			reply := replyFor(op)
			if reply == nil {
				continue
			}
			_ = c.Write(ctx, websocket.MessageText, reply)
		}
	})
	srv := httptest.NewServer(handler)
	return srv
}

func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func loadScenarioFromLines(t *testing.T, lines []string) *Scenario {
	t.Helper()
	// Build an in-memory scenario without touching disk — convenient for replay tests.
	sc := &Scenario{Meta: Meta{Suite: "test"}}
	for _, line := range lines {
		var outer struct {
			Dir string          `json:"dir"`
			Raw json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal([]byte(line), &outer); err != nil {
			t.Fatalf("line %q: %v", line, err)
		}
		sc.Steps = append(sc.Steps, Step{Dir: outer.Dir, Raw: outer.Raw})
	}
	return sc
}

func TestReplayViaEchoServer(t *testing.T) {
	// Scenario: one add-query that expects one refresh-ok; server echoes it back.
	sc := loadScenarioFromLines(t, []string{
		`{"dir":"c2s","raw":{"op":"add-query","q":{"posts":{}},"client-event-id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}`,
		`{"dir":"s2c","raw":{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[]}}}],"processed-tx-id":0,"processed-isn":0}}`,
	})

	srv := echoServer(t, func(op string) []byte {
		if op == "add-query" {
			return []byte(`{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[]}}}],"processed-tx-id":0,"processed-isn":0}`)
		}
		return nil
	})
	defer srv.Close()

	ctx := context.Background()
	res := Replay(ctx, wsURL(srv), sc, 4*time.Second)
	if res.Err != nil {
		t.Fatalf("Replay err: %v", res.Err)
	}
	if !res.Passed {
		t.Fatalf("replay should have passed; delta:\n%s", res.Delta)
	}
}

func TestReplayDetectsMismatch(t *testing.T) {
	sc := loadScenarioFromLines(t, []string{
		`{"dir":"c2s","raw":{"op":"add-query","q":{"posts":{}},"client-event-id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}`,
		`{"dir":"s2c","raw":{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[{"id":"p1"}]}}}],"processed-tx-id":0,"processed-isn":0}}`,
	})
	srv := echoServer(t, func(op string) []byte {
		return []byte(`{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[]}}}],"processed-tx-id":0,"processed-isn":0}`)
	})
	defer srv.Close()
	res := Replay(context.Background(), wsURL(srv), sc, 3*time.Second)
	if res.Passed {
		t.Fatal("expected mismatch, got pass")
	}
	if res.Delta == "" {
		t.Fatal("delta empty on mismatch")
	}
}

func TestDifferentialMatches(t *testing.T) {
	sc := loadScenarioFromLines(t, []string{
		`{"dir":"c2s","raw":{"op":"add-query","q":{"posts":{}},"client-event-id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}`,
		`{"dir":"s2c","raw":{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[]}}}]}}`,
	})
	reply := []byte(`{"op":"refresh-ok","computations":[{"instaql-query":{"posts":{}},"instaql-result":{"data":{"posts":[]}}}],"processed-tx-id":0,"processed-isn":0}`)
	srvA := echoServer(t, func(string) []byte { return reply })
	defer srvA.Close()
	srvB := echoServer(t, func(string) []byte { return reply })
	defer srvB.Close()
	_, _, delta := Differential(context.Background(), sc, wsURL(srvA), wsURL(srvB), 4*time.Second)
	if delta != "" {
		t.Fatalf("differential should be empty: %q", delta)
	}
}

func TestScenarioLoadFromDisk(t *testing.T) {
	// Verify the seed corpus loads; if fixtures have not landed yet, skip with signal.
	// This makes the test honest: failure here is the exit gate for corpus authorship.
	dir := "../../corpus"
	scs, err := LoadCorpus(dir, "")
	if err != nil {
		t.Fatalf("LoadCorpus: %v", err)
	}
	if len(scs) == 0 {
		t.Fatal("corpus must contain checked-in scenarios")
	}
	for _, sc := range scs {
		if len(sc.C2S()) == 0 && len(sc.ExpectedS2C()) == 0 {
			t.Fatalf("%s has no c2s nor s2c steps", sc.File)
		}
		for _, raw := range sc.ExpectedS2C() {
			if _, err := CanonicalBytes(raw); err != nil {
				t.Fatalf("%s: %v ( %s )", sc.File, err, raw)
			}
		}
	}
}

func TestDifferentialFailedEndpointsAreNotEqual(t *testing.T) {
	sc := loadScenarioFromLines(t, []string{
		`{"dir":"c2s","raw":{"op":"init"}}`,
		`{"dir":"s2c","raw":{"op":"init-ok"}}`,
	})
	a, b, delta := Differential(context.Background(), sc, ":invalid", ":invalid", time.Second)
	if a.Err == nil || b.Err == nil {
		t.Fatal("expected dial errors")
	}
	if delta == "" {
		t.Fatal("two failed replays must not be reported equal")
	}
}

func TestReplayRejectsInvalidServerJSON(t *testing.T) {
	sc := loadScenarioFromLines(t, []string{
		`{"dir":"c2s","raw":{"op":"init"}}`,
		`{"dir":"s2c","raw":{"op":"init-ok"}}`,
	})
	srv := echoServer(t, func(string) []byte { return []byte(`{broken`) })
	defer srv.Close()
	res := Replay(context.Background(), wsURL(srv), sc, time.Second)
	if res.Err == nil {
		t.Fatal("invalid server JSON must be a replay error")
	}
}
