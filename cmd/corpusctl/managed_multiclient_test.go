// FU-01 managed multi-client recorder tests (packet CF-003-2B1).
//
// Hermetic contract battery: exact oracle shapes (accept + tamper rejection),
// retention keying, room op-set ordering, redaction/hygiene, bounded
// quiescence behavior, and NDJSON publish round-trip — all without a database.
// Owned-DB end-to-end: the production entry point
// (`--mode managed-record-multiclient`) against a managed daemon, replayed
// from retained disk artifacts alone, twice on separate fixtures.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/instant-v2/instant-v2/internal/corpus"
)

const mcTestApp = "00000000-0000-4000-8000-000000000003"

func mcTestAttrs(app string) []any {
	return []any{
		map[string]any{
			"cardinality": "one", "checked-data-type": nil,
			"forward-identity": []any{app, "todos", "id"},
			"id":               "11111111-1111-4111-8111-111111111111",
			"index?":           true, "primary?": true, "required?": true,
			"unique?": true, "value-type": "blob",
		},
		map[string]any{
			"cardinality": "one", "checked-data-type": nil,
			"forward-identity": []any{app, "todos", "title"},
			"id":               "22222222-2222-4222-8222-222222222222",
			"index?":           false, "required?": false, "unique?": false, "value-type": "blob",
		},
	}
}

func mcTestInit(app string) map[string]any {
	return map[string]any{
		"op": "init-ok", "session-id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"app-status": map[string]any{"status": "active"},
		"auth":       map[string]any{"admin?": false, "app": map[string]any{"id": app}, "user": nil},
		"attrs":      mcTestAttrs(app),
	}
}

func mcTestSnapshot(want []any) map[string]any {
	return map[string]any{
		"op": "refresh-ok", "processed-tx-id": float64(0),
		"computations": []any{map[string]any{
			"instaql-query":  map[string]any{"todos": map[string]any{}},
			"instaql-result": map[string]any{"todos": want},
		}},
	}
}

func mcTestRefresh(tx int64, idAttr, titleAttr string, titles map[string]string) map[string]any {
	// Deterministic triple order: sorted entity IDs.
	eids := make([]string, 0, len(titles))
	for eid := range titles {
		eids = append(eids, eid)
	}
	for i := 1; i < len(eids); i++ {
		for j := i; j > 0 && eids[j] < eids[j-1]; j-- {
			eids[j], eids[j-1] = eids[j-1], eids[j]
		}
	}
	var triples []any
	for _, eid := range eids {
		triples = append(triples,
			[]any{eid, idAttr, eid},
			[]any{eid, titleAttr, titles[eid]},
		)
	}
	return map[string]any{
		"op": "refresh-ok", "processed-tx-id": float64(tx),
		"computations": []any{map[string]any{
			"instaql-query": map[string]any{"todos": map[string]any{}},
			"instaql-result": []any{map[string]any{
				"child-nodes": []any{},
				"data": map[string]any{"datalog-result": map[string]any{
					"join-rows": []any{triples},
				}},
			}},
		}},
	}
}

// Note: join-rows above nests one row holding all triples as a single row
// element; the oracle accepts any row grouping with exact triple shapes.

func TestMcQueryOracleAcceptsExactShapes(t *testing.T) {
	idAttr, titleAttr := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	gotID, gotTitle, err := mcAssertInit(mcTestInit(mcTestApp), mcTestApp, "T")
	if err != nil {
		t.Fatalf("init oracle: %v", err)
	}
	if gotID != idAttr || gotTitle != titleAttr {
		t.Fatalf("attr ids = %q/%q; want %q/%q", gotID, gotTitle, idAttr, titleAttr)
	}
	if err := mcAssertAddQuery(map[string]any{"client-event-id": "e1", "op": "add-query-ok"}, "e1", "T"); err != nil {
		t.Fatalf("add-query oracle: %v", err)
	}
	want := []any{
		map[string]any{"id": "a", "title": "alpha"},
		map[string]any{"id": "b", "title": "beta"},
	}
	if err := mcAssertOrderedSnapshot(mcTestSnapshot(want), want, "T"); err != nil {
		t.Fatalf("snapshot oracle: %v", err)
	}
	titles := map[string]string{"a": "alpha", "b": "beta-2"}
	if err := mcAssertOrderedRefresh(mcTestRefresh(7, idAttr, titleAttr, titles), idAttr, titleAttr, 7, titles, "T"); err != nil {
		t.Fatalf("refresh oracle: %v", err)
	}
	late := mcTestSnapshot(want)
	late["processed-tx-id"] = float64(7)
	if err := mcAssertLateSnapshot(late, 7, want, "T"); err != nil {
		t.Fatalf("late snapshot oracle: %v", err)
	}
	if got, err := mcLateTreeTitles(late, "T"); err != nil || !reflect.DeepEqual(got, map[string]string{"a": "alpha", "b": "beta"}) {
		t.Fatalf("late titles = %#v (%v)", got, err)
	}
}

func TestMcQueryOracleRejectsTampered(t *testing.T) {
	idAttr, titleAttr := "id-attr", "title-attr"
	want := []any{map[string]any{"id": "a", "title": "alpha"}}
	titles := map[string]string{"a": "alpha"}

	mutate := func(frame map[string]any, fn func(map[string]any)) map[string]any {
		raw, _ := json.Marshal(frame)
		var cp map[string]any
		_ = json.Unmarshal(raw, &cp)
		fn(cp)
		return cp
	}
	refresh := func() map[string]any { return mcTestRefresh(7, idAttr, titleAttr, titles) }

	cases := map[string]func() error{
		"snapshot nonzero tx": func() error {
			return mcAssertOrderedSnapshot(mutate(mcTestSnapshot(want), func(m map[string]any) {
				m["processed-tx-id"] = float64(7)
			}), want, "T")
		},
		"snapshot reordered rows": func() error {
			w2 := []any{
				map[string]any{"id": "b", "title": "beta"},
				map[string]any{"id": "a", "title": "alpha"},
			}
			return mcAssertOrderedSnapshot(mcTestSnapshot(want), w2, "T")
		},
		"snapshot subset rows": func() error {
			return mcAssertOrderedSnapshot(mcTestSnapshot(want), []any{map[string]any{"id": "a", "title": "alpha"}, map[string]any{"id": "x", "title": "y"}}, "T")
		},
		"refresh tx off by one": func() error {
			return mcAssertOrderedRefresh(mcTestRefresh(8, idAttr, titleAttr, titles), idAttr, titleAttr, 7, titles, "T")
		},
		"refresh delta key": func() error {
			return mcAssertOrderedRefresh(mutate(refresh(), func(m map[string]any) {
				m["computations"].([]any)[0].(map[string]any)["delta"] = []any{}
			}), idAttr, titleAttr, 7, titles, "T")
		},
		"refresh wrong title": func() error {
			return mcAssertOrderedRefresh(refresh(), idAttr, titleAttr, 7, map[string]string{"a": "wrong"}, "T")
		},
		"refresh subset titles": func() error {
			full := map[string]string{"a": "alpha", "b": "beta"}
			return mcAssertOrderedRefresh(mcTestRefresh(7, idAttr, titleAttr, full), idAttr, titleAttr, 7, titles, "T")
		},
		"late snapshot wrong tx": func() error {
			late := mcTestSnapshot(want)
			late["processed-tx-id"] = float64(9)
			return mcAssertLateSnapshot(late, 7, want, "T")
		},
		"late snapshot delta": func() error {
			late := mutate(mcTestSnapshot(want), func(m map[string]any) {
				m["processed-tx-id"] = float64(7)
				m["computations"].([]any)[0].(map[string]any)["delta"] = []any{}
			})
			return mcAssertLateSnapshot(late, 7, want, "T")
		},
		"add-query wrong event": func() error {
			return mcAssertAddQuery(map[string]any{"client-event-id": "e2", "op": "add-query-ok"}, "e1", "T")
		},
		"init wrong app": func() error {
			return func() error {
				_, _, err := mcAssertInit(mcTestInit("other-app"), mcTestApp, "T")
				return err
			}()
		},
		"empty snapshot oracle": func() error {
			return mcAssertOrderedSnapshot(mcTestSnapshot(want), nil, "T")
		},
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			if err := fn(); err == nil {
				t.Fatalf("tampered frame accepted: %s", name)
			}
		})
	}
}

func mcTestRoomInit(sess string) map[string]any {
	return map[string]any{
		"op": "init-ok", "session-id": sess, "attrs": []any{},
		"app-status": map[string]any{"status": "active"},
		"auth":       map[string]any{"admin?": false, "app": map[string]any{"id": mcTestApp}, "user": nil},
	}
}

// mcTestSub builds a subscriber reading scripted data frames from an
// httptest SSE server. No POSTs are issued; only read-path oracles run.
func mcTestSub(t *testing.T, name string, frames []map[string]any) *mcSub {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		for _, f := range frames {
			raw, _ := json.Marshal(f)
			_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
			if flusher != nil {
				flusher.Flush()
			}
		}
		// Hold the stream open so later quiet checks observe silence, not EOF.
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	resp, err := mcStreamClient.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	sub := &mcSub{name: name, resp: resp, app: mcTestApp, baseURL: srv.URL}
	sub.scanner = mcTestScanner(resp.Body)
	return sub
}

// mcTestScanner wraps a stream body in an SSE scanner with a 1MB line bound.
func mcTestScanner(body io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64*1024), 1<<20)
	return scanner
}

func TestMcRoomOracleAcceptsExactShapes(t *testing.T) {
	sessA := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	sessB := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	sess, err := mcAssertRoomInit(mcTestRoomInit(sessA), mcTestApp, "A")
	if err != nil || sess != sessA {
		t.Fatalf("room init = %q (%v)", sess, err)
	}
	wantBoth := map[string]any{
		sessA: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
		sessB: map[string]any{"peer": "peer-b", "data": map[string]any{}},
	}
	presence := map[string]any{"op": "refresh-presence", "room-id": mcRoomID, "data": wantBoth}
	if err := mcAssertRoomPresence(presence, mcRoomID, wantBoth, "B"); err != nil {
		t.Fatalf("presence oracle: %v", err)
	}
	ack := map[string]any{"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-b"}
	// Either order inside the ack/presence pair is accepted.
	for _, order := range [][][]map[string]any{
		{{ack}, {presence}},
		{{presence}, {ack}},
	} {
		var script []map[string]any
		for _, step := range order {
			script = append(script, step[0])
		}
		sub := mcTestSub(t, "B", script)
		var log []mcFrame
		if err := mcAwaitAckAndPresence(sub, &log, "join-room-ok", ack, wantBoth, mcRoomID); err != nil {
			t.Fatalf("ack/presence order %#v: %v", order, err)
		}
		if err := mcAssertRetentionSeq(log, "B", 2); err != nil {
			t.Fatalf("retention: %v", err)
		}
	}
	broadcast := map[string]any{
		"op": "server-broadcast", "room-id": mcRoomID, "topic": "chat",
		"session-id": sessA,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	}
	if err := mcExactFrame(broadcast, broadcast, "B", "broadcast"); err != nil {
		t.Fatalf("broadcast exact: %v", err)
	}
}

func TestMcRoomOracleRejectsTampered(t *testing.T) {
	sessA := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	wantSolo := map[string]any{
		sessA: map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
	}
	ack := map[string]any{"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-a"}
	presence := map[string]any{"op": "refresh-presence", "room-id": mcRoomID, "data": wantSolo}
	t.Run("unknown interleave", func(t *testing.T) {
		sub := mcTestSub(t, "A", []map[string]any{
			ack,
			{"op": "server-broadcast", "room-id": mcRoomID},
		})
		var log []mcFrame
		if err := mcAwaitAckAndPresence(sub, &log, "join-room-ok", ack, wantSolo, mcRoomID); err == nil {
			t.Fatal("unknown interleave accepted")
		}
	})
	t.Run("duplicate ack", func(t *testing.T) {
		sub := mcTestSub(t, "A", []map[string]any{ack, ack})
		var log []mcFrame
		if err := mcAwaitAckAndPresence(sub, &log, "join-room-ok", ack, wantSolo, mcRoomID); err == nil {
			t.Fatal("duplicate ack accepted")
		}
	})
	t.Run("wrong member count", func(t *testing.T) {
		if err := mcAssertRoomPresence(presence, mcRoomID, map[string]any{}, "A"); err == nil {
			t.Fatal("empty oracle accepted")
		}
		extra := map[string]any{
			sessA:   map[string]any{"peer": "peer-a", "data": map[string]any{"mood": "ok"}},
			"extra": map[string]any{"peer": "x", "data": map[string]any{}},
		}
		if err := mcAssertRoomPresence(
			map[string]any{"op": "refresh-presence", "room-id": mcRoomID, "data": extra},
			mcRoomID, wantSolo, "A"); err == nil {
			t.Fatal("extra member accepted")
		}
	})
	t.Run("sender echo breaks op steps", func(t *testing.T) {
		// Sender isolation by ordering: after broadcast-ack the sender's
		// next frame must be leave-driven presence, never an echo.
		log := []mcFrame{
			{Subscriber: "room-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}},
			{Subscriber: "room-A", Seq: 1, Frame: map[string]any{"op": "join-room-ok"}},
			{Subscriber: "room-A", Seq: 2, Frame: map[string]any{"op": "refresh-presence"}},
			{Subscriber: "room-A", Seq: 3, Frame: map[string]any{"op": "refresh-presence"}},
			{Subscriber: "room-A", Seq: 4, Frame: map[string]any{"op": "set-presence-ok"}},
			{Subscriber: "room-A", Seq: 5, Frame: map[string]any{"op": "refresh-presence"}},
			{Subscriber: "room-A", Seq: 6, Frame: map[string]any{"op": "client-broadcast-ok"}},
			{Subscriber: "room-A", Seq: 7, Frame: map[string]any{"op": "server-broadcast"}},
		}
		if err := mcAssertRoomOps(log, "room-A", [][]string{
			{"init-ok"},
			{"join-room-ok", "refresh-presence"},
			{"refresh-presence"},
			{"set-presence-ok", "refresh-presence"},
			{"client-broadcast-ok"},
			{"refresh-presence"},
		}); err == nil {
			t.Fatal("sender echo accepted in op steps")
		}
	})
}

func TestMcRetentionSeqDense(t *testing.T) {
	good := []mcFrame{
		{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}},
		{Subscriber: "query-A", Seq: 1, Frame: map[string]any{"op": "add-query-ok"}},
	}
	if err := mcAssertRetentionSeq(good, "query-A", 2); err != nil {
		t.Fatalf("dense retention: %v", err)
	}
	badGap := []mcFrame{
		{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}},
		{Subscriber: "query-A", Seq: 2, Frame: map[string]any{"op": "add-query-ok"}},
	}
	if err := mcAssertRetentionSeq(badGap, "query-A", 2); err == nil {
		t.Fatal("seq gap accepted")
	}
	badSub := []mcFrame{
		{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}},
		{Subscriber: "query-B", Seq: 1, Frame: map[string]any{"op": "add-query-ok"}},
	}
	if err := mcAssertRetentionSeq(badSub, "query-A", 2); err == nil {
		t.Fatal("misattributed frame accepted")
	}
}

func TestMcRedactionAndFactsHygiene(t *testing.T) {
	policy := redactionPolicy(options{redactHeaders: headerList{"X-Trace"}})
	exchange := corpus.HTTPExchange{
		Request:  corpus.HTTPRequest{Method: "GET", Target: "/x", Headers: map[string][]string{"Authorization": {"Bearer secret"}, "X-Trace": {"t"}}},
		Response: corpus.HTTPResponse{Status: 200, Headers: map[string][]string{"Set-Cookie": {"s=secret"}, "Cookie": {"c=secret"}}},
	}
	redacted := corpus.RedactExchange(exchange, policy)
	for _, h := range []struct {
		got  string
		name string
	}{
		{redacted.Request.Headers.Get("Authorization"), "Authorization"},
		{redacted.Request.Headers.Get("X-Trace"), "X-Trace"},
		{redacted.Response.Headers.Get("Set-Cookie"), "Set-Cookie"},
		{redacted.Response.Headers.Get("Cookie"), "Cookie"},
	} {
		if h.got != "<redacted>" {
			t.Fatalf("header %s not redacted: %q", h.name, h.got)
		}
	}
	var facts mcCaptureFacts
	facts.Mode = "managed-record-multiclient"
	facts.RoomID = mcRoomID
	raw, _ := json.Marshal(facts)
	for _, leak := range []string{"sse-token", "postgres://", "session-id", "DATABASE_URL"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("facts envelope leaks %q", leak)
		}
	}
	lines, err := mcRenderNDJSON([]mcFrame{{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}}})
	if err != nil {
		t.Fatal(err)
	}
	var line map[string]any
	if err := json.Unmarshal(bytes.TrimSuffix(lines, []byte("\n")), &line); err != nil {
		t.Fatal(err)
	}
	if line["subscriber"] != "query-A" || line["seq"] != float64(0) || len(line) != 3 {
		t.Fatalf("NDJSON line keys invalid: %s", lines)
	}
	desc := mcDescribeFrame(map[string]any{"op": "refresh-presence", "room-id": mcRoomID, "data": map[string]any{"SECRET-SESSION": 1}})
	if strings.Contains(desc, "SECRET-SESSION") {
		t.Fatalf("diagnostic frame description leaks session material: %q", desc)
	}
}

func TestMcQuietWindowBoundAndBehavior(t *testing.T) {
	if mcQuietWindow != 250*time.Millisecond {
		t.Fatalf("quiet window = %v; want exactly 250ms", mcQuietWindow)
	}
	t.Run("silence passes", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}))
		defer srv.Close()
		resp, err := mcStreamClient.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		sub := &mcSub{name: "Q", resp: resp, app: mcTestApp, baseURL: srv.URL}
		sub.scanner = mcTestScanner(resp.Body)
		if err := mcAssertQuiet(sub); err != nil {
			t.Fatalf("bounded silence failed: %v", err)
		}
	})
	t.Run("extra frame fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(50 * time.Millisecond)
			_, _ = fmt.Fprint(w, "data: {\"op\":\"refresh-ok\"}\n\n")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
			case <-time.After(5 * time.Second):
			}
		}))
		defer srv.Close()
		resp, err := mcStreamClient.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		sub := &mcSub{name: "Q", resp: resp, app: mcTestApp, baseURL: srv.URL}
		sub.scanner = mcTestScanner(resp.Body)
		if err := mcAssertQuiet(sub); err == nil {
			t.Fatal("extra frame inside quiet window accepted")
		}
	})
	t.Run("stream end fails", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
		}))
		defer srv.Close()
		resp, err := mcStreamClient.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		sub := &mcSub{name: "Q", resp: resp, app: mcTestApp, baseURL: srv.URL}
		sub.scanner = mcTestScanner(resp.Body)
		if err := mcAssertQuiet(sub); err == nil {
			t.Fatal("stream end inside quiet window accepted")
		}
	})
}

func TestMcNDJSONPublishRoundTripHermetic(t *testing.T) {
	base := t.TempDir()
	outDir := filepath.Join(base, "run-1")
	reserved, err := corpus.ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reserved.Close() }()
	pin, err := mcPinOutputDir(reserved.Path())
	if err != nil {
		t.Fatal(err)
	}

	idAttr, titleAttr := "id-attr", "title-attr"
	want := []any{map[string]any{"id": "a", "title": "alpha"}}
	titles := map[string]string{"a": "alpha"}
	log := []mcFrame{
		{Subscriber: "query-A", Seq: 0, Frame: mcTestInit(mcTestApp)},
		{Subscriber: "query-A", Seq: 1, Frame: map[string]any{"client-event-id": "e1", "op": "add-query-ok"}},
		{Subscriber: "query-A", Seq: 2, Frame: mcTestSnapshot(want)},
		{Subscriber: "query-A", Seq: 3, Frame: mcTestRefresh(7, idAttr, titleAttr, titles)},
	}
	payload, err := mcRenderNDJSON(log)
	if err != nil {
		t.Fatal(err)
	}
	if got := bytes.Count(payload, []byte("\n")); got != len(log) {
		t.Fatalf("NDJSON lines = %d; want %d", got, len(log))
	}
	if err := mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload); err == nil {
		t.Fatal("write-once republish accepted")
	}
	if err := reserved.WriteEvidence("fu01-capture-facts.json", map[string]any{"mode": "test"}); err != nil {
		t.Fatal(err)
	}
	names := []string{"fu01-query-a.ndjson", "fu01-capture-facts.json"}
	entries, err := corpus.ManifestArtifacts(outDir, names)
	if err != nil {
		t.Fatal(err)
	}
	manifest := corpus.CaptureManifest{
		Candidate:    mcTestCandidate(),
		Precondition: "pre",
		FinalState:   "post",
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		t.Fatal(err)
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		t.Fatalf("manifest verify: %v", err)
	}
	for _, f := range append(names, "manifest.json") {
		fi, err := os.Stat(filepath.Join(outDir, f))
		if err != nil || fi.Mode().Perm() != 0600 {
			t.Fatalf("evidence %s not 0600", f)
		}
	}
	if fi, err := os.Stat(outDir); err != nil || fi.Mode().Perm() != 0700 {
		t.Fatalf("output dir not 0700")
	}
	// Replay from disk alone: parse NDJSON, re-assert the exact oracle.
	raw, err := os.ReadFile(filepath.Join(outDir, "fu01-query-a.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	var back []mcFrame
	for _, line := range bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) {
		var entry mcFrame
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("NDJSON line invalid: %v", err)
		}
		back = append(back, entry)
	}
	if err := mcAssertRetentionSeq(back, "query-A", 4); err != nil {
		t.Fatalf("disk retention: %v", err)
	}
	if err := mcAssertOrderedSnapshot(back[2].Frame, want, "disk"); err != nil {
		t.Fatalf("disk snapshot: %v", err)
	}
	if err := mcAssertOrderedRefresh(back[3].Frame, idAttr, titleAttr, 7, titles, "disk"); err != nil {
		t.Fatalf("disk refresh: %v", err)
	}
	// Tamper probe: one flipped byte breaks manifest verification.
	tampered := append([]byte(nil), raw...)
	tampered[len(tampered)-2] ^= 0x01
	if err := os.WriteFile(filepath.Join(outDir, "fu01-query-a.ndjson"), tampered, 0600); err != nil {
		t.Fatal(err)
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err == nil {
		t.Fatal("tampered NDJSON passed manifest verification")
	}
}

func mcTestCandidate() corpus.CandidateIdentity {
	return corpus.CandidateIdentity{
		GitSHA:       "0123456789abcdef0123456789abcdef01234567",
		GitDirty:     false,
		GoVersion:    "go1.27.1",
		GOOS:         "darwin",
		GOARCH:       "arm64",
		BinaryPath:   "/tmp/test-instantd",
		BinarySHA256: strings.Repeat("ab", 32),
		PID:          1234,
		Executable:   "/tmp/test-instantd",
		Endpoint:     "http://127.0.0.1:9",
		ConfigDigest: strings.Repeat("cd", 32),
		FixtureDB:    "instant_test_abcdef",
	}
}

func mcE2ERepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot locate repo root (go.mod)")
		}
		dir = parent
	}
}

func mcE2EHead(t *testing.T, root string) string {
	t.Helper()
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

// mcParseNDJSON reads one per-subscriber NDJSON artifact back into retention.
func mcParseNDJSON(t *testing.T, dir, name string) []mcFrame {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		t.Fatalf("%s is not newline-delimited", name)
	}
	var log []mcFrame
	for i, line := range bytes.Split(bytes.TrimSuffix(raw, []byte("\n")), []byte("\n")) {
		var entry mcFrame
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("%s line %d invalid: %v", name, i, err)
		}
		log = append(log, entry)
	}
	return log
}

// mcRunMulticlientE2E drives the production entry point once into a fresh
// directory outside the repo and replays the full oracle from disk alone.
func mcRunMulticlientE2E(t *testing.T, root, head, outDir string) (corpus.CaptureManifest, mcCaptureFacts) {
	t.Helper()
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record-multiclient", "--output-dir", outDir, "--repo", root}, &out, &stderr); code != 0 {
		t.Fatalf("managed-record-multiclient failed code=%d stderr=%s", code, &stderr)
	}
	stdout := out.String()
	if !strings.Contains(stdout, "PASS managed-record-multiclient "+head) {
		t.Fatalf("missing pass line for %s: %s (stderr=%s)", head, stdout, &stderr)
	}
	for _, leak := range []string{"session-id", "sse-token"} {
		if strings.Contains(stdout, leak) || strings.Contains(stderr.String(), leak) {
			t.Fatalf("console output leaks %q", leak)
		}
	}

	dirFI, err := os.Stat(outDir)
	if err != nil || dirFI.Mode().Perm() != 0700 {
		t.Fatalf("output dir not 0700: %v %v", dirFI, err)
	}
	manifestBytes, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
	if err != nil {
		t.Fatalf("manifest missing: %v", err)
	}
	var manifest corpus.CaptureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatalf("manifest invalid: %v", err)
	}
	if manifest.Candidate.GitSHA != head || manifest.Candidate.GitDirty {
		t.Fatalf("manifest candidate = %q dirty=%v; want %q clean", manifest.Candidate.GitSHA, manifest.Candidate.GitDirty, head)
	}
	if len(manifest.Candidate.BinarySHA256) != 64 || manifest.Candidate.PID <= 0 {
		t.Fatalf("manifest binary/pid incomplete")
	}
	if err := corpus.RequireLoopbackEndpoint(manifest.Candidate.Endpoint); err != nil {
		t.Fatalf("endpoint not loopback: %v", err)
	}
	if len(manifest.Candidate.ConfigDigest) != 64 {
		t.Fatalf("config digest missing")
	}
	if !strings.HasPrefix(manifest.Candidate.FixtureDB, "instant_test_") {
		t.Fatalf("fixture DB not owned: %q", manifest.Candidate.FixtureDB)
	}
	wantPre := fmt.Sprintf("apps=1 title=%q attrs=0 triples=0 idents=0 rows=[]", mcAppTitle)
	if manifest.Precondition != wantPre {
		t.Fatalf("precondition = %s; want %s", manifest.Precondition, wantPre)
	}
	if len(manifest.Artifacts) != 6 {
		t.Fatalf("artifacts = %d; want 6 (5 NDJSON + facts)", len(manifest.Artifacts))
	}
	for _, f := range append([]string{
		"fu01-query-a.ndjson", "fu01-query-b.ndjson", "fu01-query-c.ndjson",
		"fu01-room-a.ndjson", "fu01-room-b.ndjson", "fu01-capture-facts.json",
	}, "manifest.json") {
		fi, err := os.Stat(filepath.Join(outDir, f))
		if err != nil || fi.Mode().Perm() != 0600 {
			t.Fatalf("evidence %s not 0600: %v %v", f, fi, err)
		}
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		t.Fatalf("manifest verification: %v", err)
	}

	factsBytes, err := os.ReadFile(filepath.Join(outDir, "fu01-capture-facts.json"))
	if err != nil {
		t.Fatalf("facts missing: %v", err)
	}
	var facts mcCaptureFacts
	if err := json.Unmarshal(factsBytes, &facts); err != nil {
		t.Fatalf("facts invalid: %v", err)
	}
	if facts.QuietWindowMS != 250 || facts.RoomID != mcRoomID || facts.Query.TriggerTxID <= 0 {
		t.Fatalf("facts incomplete: %+v", facts)
	}
	if facts.Precondition != wantPre || facts.FinalState != manifest.FinalState {
		t.Fatalf("facts fixture mismatch")
	}
	if len(facts.Query.Entities) != 3 || len(facts.Query.WantInitial) != 3 {
		t.Fatalf("facts query entities incomplete")
	}

	dbURL := os.Getenv("DATABASE_URL")
	for _, f := range []string{"manifest.json", "fu01-capture-facts.json"} {
		b, _ := os.ReadFile(filepath.Join(outDir, f))
		for _, leak := range []string{"sse-token", "session-id", "postgres://", "postgresql://", dbURL} {
			if leak != "" && bytes.Contains(b, []byte(leak)) {
				t.Fatalf("%s leaks %q", f, leak)
			}
		}
	}
	for _, f := range []string{
		"fu01-query-a.ndjson", "fu01-query-b.ndjson", "fu01-query-c.ndjson",
		"fu01-room-a.ndjson", "fu01-room-b.ndjson",
	} {
		b, _ := os.ReadFile(filepath.Join(outDir, f))
		for _, leak := range []string{"sse-token", "postgres://", "postgresql://", dbURL} {
			if leak != "" && bytes.Contains(b, []byte(leak)) {
				t.Fatalf("%s leaks %q", f, leak)
			}
		}
	}

	mcAssertDiskReplay(t, outDir, facts)

	// The daemon recorded in the manifest must be stopped.
	endpoint := strings.TrimPrefix(manifest.Candidate.Endpoint, "http://")
	if conn, err := net.DialTimeout("tcp", endpoint, 500*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatalf("multiclient daemon still listening on %s after exit", endpoint)
	}
	return manifest, facts
}

func TestManagedMulticlientRecordEndToEnd(t *testing.T) {
	if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" || os.Getenv("DATABASE_URL") == "" {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	root := mcE2ERepoRoot(t)
	if _, dirty, err := corpus.GitIdentity(root); err != nil {
		t.Fatal(err)
	} else if dirty {
		t.Skip("managed-record-multiclient acceptance requires a clean worktree")
	}
	head := mcE2EHead(t, root)

	// Two full runs on separate fixtures (each run mints and drops its own
	// owned instant_test_* database against the same role).
	var firstManifest []byte
	for i := 1; i <= 2; i++ {
		outDir := filepath.Join(t.TempDir(), fmt.Sprintf("run-%d", i))
		manifest, _ := mcRunMulticlientE2E(t, root, head, outDir)
		_ = manifest
		if i == 1 {
			b, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			firstManifest = b

			// Tamper probe on a copy: one flipped byte breaks verification.
			tamperDir := filepath.Join(t.TempDir(), "tampered")
			if err := copyDir(t, outDir, tamperDir); err != nil {
				t.Fatal(err)
			}
			ndjson := filepath.Join(tamperDir, "fu01-query-a.ndjson")
			raw, err := os.ReadFile(ndjson)
			if err != nil {
				t.Fatal(err)
			}
			raw[len(raw)-2] ^= 0x01
			if err := os.WriteFile(ndjson, raw, 0600); err != nil {
				t.Fatal(err)
			}
			var tm corpus.CaptureManifest
			tb, _ := os.ReadFile(filepath.Join(tamperDir, "manifest.json"))
			if err := json.Unmarshal(tb, &tm); err != nil {
				t.Fatal(err)
			}
			if err := corpus.VerifyCaptureManifest(tamperDir, tm); err == nil {
				t.Fatal("tampered NDJSON passed manifest verification")
			}

			// Output reuse fails closed with the first manifest byte-identical.
			var out2, stderr2 bytes.Buffer
			if code := run([]string{"--mode", "managed-record-multiclient", "--output-dir", outDir, "--repo", root}, &out2, &stderr2); code == 0 {
				t.Fatal("output reuse reported success")
			} else if !strings.Contains(stderr2.String(), "already exists") {
				t.Fatalf("reuse error unstable: %s", stderr2.String())
			}
			after, err := os.ReadFile(filepath.Join(outDir, "manifest.json"))
			if err != nil || !bytes.Equal(firstManifest, after) {
				t.Fatal("output reuse changed the first evidence")
			}
		}
	}
}

// mcAssertDiskReplay re-asserts the full oracle (whole-result DeepEqual +
// exact float watermarks + exact tx IDs) from retained disk artifacts alone.
// It delegates to the production disk verifier (mcVerifyDiskCapture), so the
// test helper and the PASS gate check identical coverage: manifest checksums,
// facts, all five NDJSON logs, every init/ack envelope, exact op-step
// ordering on both room streams, session relationships, snapshots,
// watermarks, no-delta, and no-quorum DeepEqual.
func mcAssertDiskReplay(t *testing.T, outDir string, facts mcCaptureFacts) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(outDir, "fu01-capture-facts.json"))
	if err != nil {
		t.Fatalf("disk facts missing: %v", err)
	}
	var diskFacts mcCaptureFacts
	if err := json.Unmarshal(raw, &diskFacts); err != nil {
		t.Fatalf("disk facts invalid: %v", err)
	}
	if diskFacts.Query.TriggerTxID != facts.Query.TriggerTxID ||
		!reflect.DeepEqual(diskFacts.Query.FinalTitles, facts.Query.FinalTitles) {
		t.Fatalf("disk facts diverged from capture facts")
	}
	if err := mcVerifyDiskCapture(outDir); err != nil {
		t.Fatalf("disk replay: %v", err)
	}
}

func copyDir(t *testing.T, src, dst string) error {
	t.Helper()
	if err := os.MkdirAll(dst, 0700); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), b, 0600); err != nil {
			return err
		}
	}
	return nil
}

// TestManagedMulticlientRejectsSymlinkedOutput proves symlink fail-closed
// before any database or daemon work.
func TestManagedMulticlientRejectsSymlinkedOutput(t *testing.T) {
	base := t.TempDir()
	target := filepath.Join(base, "real")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(base, "link-out")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record-multiclient", "--output-dir", link, "--repo", mcE2ERepoRoot(t)}, &out, &stderr); code == 0 {
		t.Fatal("symlinked output reported success")
	} else if !strings.Contains(stderr.String(), "symlink") {
		t.Fatalf("symlink error unstable: %s", stderr.String())
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("symlinked output run wrote %d entries under the target", len(entries))
	}
}

// TestManagedMulticlientRequiresCleanTree proves dirty-worktree fail-closed
// without touching a database.
func TestManagedMulticlientRequiresCleanTree(t *testing.T) {
	repo := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q", repo},
		{"-C", repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-q", "--allow-empty", "-m", "init"},
	} {
		if out, err := exec.Command("git", args...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("untracked"), 0600); err != nil {
		t.Fatal(err)
	}
	outDir := filepath.Join(t.TempDir(), "dirty-out")
	var out, stderr bytes.Buffer
	if code := run([]string{"--mode", "managed-record-multiclient", "--output-dir", outDir, "--repo", repo}, &out, &stderr); code == 0 {
		t.Fatal("dirty worktree reported success")
	} else if !strings.Contains(stderr.String(), "clean Git worktree") {
		t.Fatalf("dirty-tree error unstable: %s", stderr.String())
	}
	if _, err := os.Stat(outDir); !os.IsNotExist(err) {
		t.Fatalf("dirty worktree run produced output at %s", outDir)
	}
}

// TestManagedMulticlientLegsOwnedDB runs the recorder legs end to end against
// a managed daemon on owned isolated databases, twice on separate fixtures,
// publishing through the production publisher and replaying from disk alone.
//
// This test intentionally does not pass through the clean-tree acceptance
// gate: it builds the daemon from the working tree as test scaffolding and
// records no acceptance manifest (artifacts stay under t.TempDir with the
// actual dirty bit). The gate itself is shared CF-002 machinery proven
// fail-closed by TestManagedMulticlientRequiresCleanTree and the full-gate
// TestManagedMulticlientRecordEndToEnd (which runs unskipped on a clean
// tree, e.g. post-commit/CI).
func TestManagedMulticlientLegsOwnedDB(t *testing.T) {
	if os.Getenv("INSTANT_TEST_INTEGRATION") != "1" || os.Getenv("DATABASE_URL") == "" {
		t.Skip("integration test: set INSTANT_TEST_INTEGRATION=1 and DATABASE_URL")
	}
	root := mcE2ERepoRoot(t)
	for i := 1; i <= 2; i++ {
		mcLegsRunOnce(t, root, i)
	}
}

func mcLegsRunOnce(t *testing.T, root string, iter int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	adminDSN := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if _, err := pgxpool.ParseConfig(adminDSN); err != nil {
		t.Fatalf("iter %d: database URL invalid", iter)
	}
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("iter %d: PostgreSQL connection failed", iter)
	}
	defer func() { _ = admin.Close(context.Background()) }()
	ownedDB := "instant_test_" + managedRandHex(12)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{ownedDB}.Sanitize()+" TEMPLATE template0"); err != nil {
		t.Fatalf("iter %d: create owned database", iter)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(dropCtx, "DROP DATABASE "+pgx.Identifier{ownedDB}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Errorf("iter %d: drop owned database %s failed", iter, ownedDB)
		}
	}()
	ownedDSN, err := managedDatabaseDSN(adminDSN, ownedDB)
	if err != nil {
		t.Fatal(err)
	}

	buildTmp, err := os.MkdirTemp("", "fu01-legs-bin-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(buildTmp) }()
	bin := filepath.Join(buildTmp, "instantd-legs")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "./cmd/instantd")
	buildCmd.Dir = root
	if combined, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("iter %d: build instantd: %v\n%s", iter, err, combined)
	}

	var r managedResources
	r.bin = bin
	r.buildTmp = ""
	storageRoot, err := os.MkdirTemp("", "fu01-legs-root-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(storageRoot) }()
	r.storageRoot = storageRoot
	r.addr = "127.0.0.1:" + fmt.Sprint(managedFreePort())
	r.baseURL = "http://" + r.addr
	if err := corpus.RequireLoopbackEndpoint(r.baseURL); err != nil {
		t.Fatal(err)
	}
	logDir, err := os.MkdirTemp("", "fu01-legs-log-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(logDir) }()
	r.logPath = filepath.Join(logDir, "managed.log")
	if err := managedStartDaemon(ctx, &r, ownedDSN); err != nil {
		t.Fatalf("iter %d: start daemon: %v", iter, err)
	}
	defer managedStopDaemon(&r)
	r.pid = r.daemon.Process.Pid
	if err := managedWaitHealth(ctx, r.baseURL); err != nil {
		t.Fatalf("iter %d: daemon health: %v", iter, err)
	}

	poolCfg, err := pgxpool.ParseConfig(ownedDSN)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("iter %d: ping owned database", iter)
	}
	r.pool = pool
	managedNewUUID(r.appID[:])
	managedNewUUID(r.creatorID[:])
	managedNewUUID(r.adminID[:])
	r.appStr = managedUUIDStr(r.appID)
	r.adminStr = managedUUIDStr(r.adminID)
	if _, err := pool.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, r.creatorID, "fu01@example.test"); err != nil {
		t.Fatalf("iter %d: seed creator", iter)
	}
	if err := managedResetFixture(ctx, &r, mcAppTitle); err != nil {
		t.Fatalf("iter %d: reset fixture: %v", iter, err)
	}
	pre, err := managedFixtureState(ctx, &r)
	if err != nil {
		t.Fatal(err)
	}
	if want := fmt.Sprintf("apps=1 title=%q attrs=0 triples=0 idents=0 rows=[]", mcAppTitle); pre != want {
		t.Fatalf("iter %d: precondition = %s; want %s", iter, pre, want)
	}

	// Room leg first on the freshly reset fixture (its init oracle pins
	// empty attrs); the query leg seeds afterwards.
	rlogs, rm, err := mcCaptureRoomLeg(ctx, &r)
	if err != nil {
		mcCloseAll(rm.subs)
		t.Fatalf("iter %d: room leg: %v", iter, err)
	}
	mcCloseAll(rm.subs)
	qlogs, q, err := mcCaptureQueryLeg(ctx, &r)
	if err != nil {
		mcCloseAll(q.subs)
		t.Fatalf("iter %d: query leg: %v", iter, err)
	}
	mcCloseAll(q.subs)
	final, err := managedFixtureState(ctx, &r)
	if err != nil {
		t.Fatal(err)
	}
	if err := mcAssertQueryFinalState(final, q); err != nil {
		t.Fatalf("iter %d: %v", iter, err)
	}

	// Publish through the production publisher into a temp dir outside the repo.
	outDir := filepath.Join(t.TempDir(), "legs")
	reserved, err := corpus.ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reserved.Close() }()
	pin, err := mcPinOutputDir(reserved.Path())
	if err != nil {
		t.Fatal(err)
	}
	ndjsonLogs := []struct {
		base string
		log  []mcFrame
	}{
		{"fu01-query-a", qlogs["query-A"]},
		{"fu01-query-b", qlogs["query-B"]},
		{"fu01-query-c", qlogs["query-C"]},
		{"fu01-room-a", rlogs["room-A"]},
		{"fu01-room-b", rlogs["room-B"]},
	}
	var names []string
	for _, f := range ndjsonLogs {
		payload, err := mcRenderNDJSON(f.log)
		if err != nil {
			t.Fatal(err)
		}
		if err := mcPublishNDJSON(reserved, pin, f.base, ".ndjson", payload); err != nil {
			t.Fatalf("iter %d: publish %s: %v", iter, f.base, err)
		}
		names = append(names, f.base+".ndjson")
	}
	facts := mcBuildFacts(pre, final, q)
	if err := reserved.WriteEvidence("fu01-capture-facts.json", facts); err != nil {
		t.Fatal(err)
	}
	names = append(names, "fu01-capture-facts.json")
	entries, err := corpus.ManifestArtifacts(outDir, names)
	if err != nil {
		t.Fatal(err)
	}
	binSHA, err := corpus.HashFile(bin)
	if err != nil {
		t.Fatal(err)
	}
	_, gitDirty, err := corpus.GitIdentity(root)
	if err != nil {
		t.Fatal(err)
	}
	manifest := corpus.CaptureManifest{
		Candidate: corpus.CandidateIdentity{
			GitSHA:       mcE2EHead(t, root),
			GitDirty:     gitDirty,
			GoVersion:    runtime.Version(),
			GOOS:         runtime.GOOS,
			GOARCH:       runtime.GOARCH,
			BinaryPath:   bin,
			BinarySHA256: binSHA,
			PID:          r.pid,
			Executable:   bin,
			Endpoint:     r.baseURL,
			ConfigDigest: corpus.ConfigDigest(map[string]string{"legs": "test"}),
			FixtureDB:    ownedDB,
			FixtureApp:   r.appStr,
		},
		Precondition: pre,
		FinalState:   final,
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		t.Fatal(err)
	}
	if err := corpus.VerifyCaptureManifest(outDir, manifest); err != nil {
		t.Fatalf("iter %d: manifest verify: %v", iter, err)
	}
	// Test scaffolding honesty: the manifest dirty bit must match the actual
	// tree state (clean on a checked-out candidate, dirty during development);
	// this manifest is never acceptance evidence.
	status, err := exec.Command("git", "-C", root, "status", "--porcelain").Output()
	if err != nil {
		t.Fatalf("iter %d: git status: %v", iter, err)
	}
	if actualDirty := len(bytes.TrimSpace(status)) > 0; manifest.Candidate.GitDirty != actualDirty {
		t.Fatalf("iter %d: legs manifest dirty=%v but tree dirty=%v", iter, manifest.Candidate.GitDirty, actualDirty)
	}

	mcAssertDiskReplay(t, outDir, facts)

	dbURL := os.Getenv("DATABASE_URL")
	for _, f := range append(names, "manifest.json") {
		b, _ := os.ReadFile(filepath.Join(outDir, f))
		for _, leak := range []string{"sse-token", "postgres://", "postgresql://", dbURL} {
			if leak != "" && bytes.Contains(b, []byte(leak)) {
				t.Fatalf("iter %d: %s leaks %q", iter, f, leak)
			}
		}
	}

	managedStopDaemon(&r)
	if conn, err := net.DialTimeout("tcp", r.addr, 500*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatalf("iter %d: legs daemon still listening after stop", iter)
	}
	t.Logf("iter %d: legs e2e green on %s trigger-tx=%d", iter, ownedDB, q.triggerTx)
}

// TestMcPublishRejectsReplacementRace proves F1 fail-closed: the pin is
// captured before the post-reservation swap (rename the reserved directory
// away, plant a fresh 0700 replacement), so publication fails, the victim
// stays byte-identical, and no NDJSON lands in the replacement.
func TestMcPublishRejectsReplacementRace(t *testing.T) {
	base := t.TempDir()
	outDir := filepath.Join(base, "run-1")
	reserved, err := corpus.ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reserved.Close() }()
	pin, err := mcPinOutputDir(reserved.Path())
	if err != nil {
		t.Fatal(err)
	}
	if err := reserved.WriteEvidence("canary.json", map[string]any{"v": 1}); err != nil {
		t.Fatal(err)
	}
	canaryBefore, err := os.ReadFile(filepath.Join(reserved.Path(), "canary.json"))
	if err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(base, "run-1.victim")
	if err := os.Rename(reserved.Path(), victim); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(reserved.Path(), 0700); err != nil {
		t.Fatal(err)
	}
	payload, err := mcRenderNDJSON([]mcFrame{{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload); err == nil {
		t.Fatal("directory swap accepted by NDJSON publication")
	}
	canaryAfter, err := os.ReadFile(filepath.Join(victim, "canary.json"))
	if err != nil || !bytes.Equal(canaryBefore, canaryAfter) {
		t.Fatalf("victim changed across rejected publication: %v", err)
	}
	if entries, err := os.ReadDir(victim); err != nil || len(entries) != 1 || entries[0].Name() != "canary.json" {
		t.Fatalf("victim entries changed: %v %v", entries, err)
	}
	if entries, err := os.ReadDir(reserved.Path()); err != nil {
		t.Fatal(err)
	} else {
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".ndjson") {
				t.Fatalf("replacement holds published NDJSON %q", e.Name())
			}
		}
	}
}

// TestMcPublishFailuresLeaveNoFinal proves write/sync failure injection
// publishes no final artifact, and directory-sync failure reports the final
// artifact as untrusted and incomplete (mirroring the ReservedDir discipline:
// the link already landed, so only the error marks it ineligible).
func TestMcPublishFailuresLeaveNoFinal(t *testing.T) {
	injected := fmt.Errorf("injected failure")
	t.Run("write", func(t *testing.T) {
		mcTestHookPublishBeforeWrite = func() error { return injected }
		t.Cleanup(func() { mcTestHookPublishBeforeWrite = nil })
		outDir := filepath.Join(t.TempDir(), "run")
		reserved, err := corpus.ReserveOutputDir(outDir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reserved.Close() }()
		pin, err := mcPinOutputDir(reserved.Path())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := mcRenderNDJSON([]mcFrame{{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload); err == nil {
			t.Fatal("injected write failure published")
		}
		if _, err := os.Stat(filepath.Join(reserved.Path(), "fu01-query-a.ndjson")); !os.IsNotExist(err) {
			t.Fatalf("injected write failure left a final artifact: %v", err)
		}
	})
	t.Run("sync", func(t *testing.T) {
		mcTestHookPublishBeforeSync = func() error { return injected }
		t.Cleanup(func() { mcTestHookPublishBeforeSync = nil })
		outDir := filepath.Join(t.TempDir(), "run")
		reserved, err := corpus.ReserveOutputDir(outDir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reserved.Close() }()
		pin, err := mcPinOutputDir(reserved.Path())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := mcRenderNDJSON([]mcFrame{{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload); err == nil {
			t.Fatal("injected sync failure published")
		}
		if _, err := os.Stat(filepath.Join(reserved.Path(), "fu01-query-a.ndjson")); !os.IsNotExist(err) {
			t.Fatalf("injected sync failure left a final artifact: %v", err)
		}
	})
	t.Run("dirsync", func(t *testing.T) {
		mcTestHookPublishBeforeDirSync = func() error { return injected }
		t.Cleanup(func() { mcTestHookPublishBeforeDirSync = nil })
		outDir := filepath.Join(t.TempDir(), "run")
		reserved, err := corpus.ReserveOutputDir(outDir)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reserved.Close() }()
		pin, err := mcPinOutputDir(reserved.Path())
		if err != nil {
			t.Fatal(err)
		}
		payload, err := mcRenderNDJSON([]mcFrame{{Subscriber: "query-A", Seq: 0, Frame: map[string]any{"op": "init-ok"}}})
		if err != nil {
			t.Fatal(err)
		}
		err = mcPublishNDJSON(reserved, pin, "fu01-query-a", ".ndjson", payload)
		if err == nil {
			t.Fatal("injected directory-sync failure reported success")
		}
		if !strings.Contains(err.Error(), "untrusted and incomplete") {
			t.Fatalf("directory-sync failure does not mark the artifact untrusted: %v", err)
		}
	})
}

// mcDiskArtifactNames lists every checksummed artifact the disk verifier
// requires.
var mcDiskArtifactNames = []string{
	"fu01-query-a.ndjson", "fu01-query-b.ndjson", "fu01-query-c.ndjson",
	"fu01-room-a.ndjson", "fu01-room-b.ndjson", "fu01-capture-facts.json",
}

// mcValidDiskFixture builds a fully valid hermetic disk fixture (no
// database): all five NDJSON logs, facts, and a checksummed manifest that
// the production verifier accepts.
func mcValidDiskFixture(t *testing.T) string {
	t.Helper()
	idAttr, titleAttr := "11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"
	sessQA, sessQB, sessQC := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0002", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0003"
	sessA, sessB := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	initFor := func(sess string) map[string]any {
		m := mcTestInit(mcTestApp)
		m["session-id"] = sess
		return m
	}
	wantInitial := []any{
		map[string]any{"id": "a", "title": "alpha"},
		map[string]any{"id": "b", "title": "beta"},
		map[string]any{"id": "c", "title": "gamma"},
	}
	finalTitles := map[string]string{"a": "alpha", "b": "beta-2", "c": "gamma"}
	wantLate := []any{
		map[string]any{"id": "a", "title": "alpha"},
		map[string]any{"id": "b", "title": "beta-2"},
		map[string]any{"id": "c", "title": "gamma"},
	}
	ack := func(e string) map[string]any { return map[string]any{"client-event-id": e, "op": "add-query-ok"} }
	snap := mcTestSnapshot(wantInitial)
	refresh := mcTestRefresh(7, idAttr, titleAttr, finalTitles)
	late := mcTestSnapshot(wantLate)
	late["processed-tx-id"] = float64(7)
	keyed := func(sub string, frames []map[string]any) []mcFrame {
		log := make([]mcFrame, 0, len(frames))
		for i, f := range frames {
			log = append(log, mcFrame{Subscriber: sub, Seq: i, Frame: f})
		}
		return log
	}
	memberA := func(mood string) map[string]any {
		return map[string]any{"peer": "peer-a", "data": map[string]any{"mood": mood}}
	}
	solo := map[string]any{sessA: memberA("ok")}
	both := map[string]any{sessA: memberA("ok"), sessB: map[string]any{"peer": "peer-b", "data": map[string]any{}}}
	updated := map[string]any{sessA: memberA("great"), sessB: map[string]any{"peer": "peer-b", "data": map[string]any{}}}
	one := map[string]any{sessA: memberA("great")}
	presence := func(m map[string]any) map[string]any {
		return map[string]any{"op": "refresh-presence", "room-id": mcRoomID, "data": m}
	}
	broadcast := map[string]any{
		"op": "server-broadcast", "room-id": mcRoomID, "topic": "chat",
		"session-id": sessA,
		"data":       map[string]any{"peer": "peer-a", "data": map[string]any{"message": "hello"}},
	}
	logs := map[string][]mcFrame{
		"fu01-query-a": keyed("query-A", []map[string]any{initFor(sessQA), ack("fu01-query-a"), snap, refresh}),
		"fu01-query-b": keyed("query-B", []map[string]any{initFor(sessQB), ack("fu01-query-b"), snap, refresh}),
		"fu01-query-c": keyed("query-C", []map[string]any{initFor(sessQC), ack("fu01-query-c"), late}),
		"fu01-room-a": keyed("room-A", []map[string]any{
			mcTestRoomInit(sessA),
			map[string]any{"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-a"},
			presence(solo), presence(both),
			map[string]any{"op": "set-presence-ok", "room-id": mcRoomID, "client-event-id": "presence-a"},
			presence(updated),
			map[string]any{"op": "client-broadcast-ok", "client-event-id": "broadcast-a"},
			presence(one),
		}),
		"fu01-room-b": keyed("room-B", []map[string]any{
			mcTestRoomInit(sessB),
			map[string]any{"op": "join-room-ok", "room-id": mcRoomID, "client-event-id": "join-b"},
			presence(both), presence(both), presence(updated),
			broadcast,
			map[string]any{"op": "leave-room-ok", "room-id": mcRoomID, "client-event-id": "leave-b"},
		}),
	}
	var facts mcCaptureFacts
	facts.Mode = "managed-record-multiclient"
	facts.Contract = "docs/plans/finish-up/fu01-multiclient-capture-contract.md"
	facts.RoomID = mcRoomID
	facts.QuietWindowMS = int64(mcQuietWindow / time.Millisecond)
	facts.RedactedHeaders = append([]string(nil), defaultRedactionHeaders...)
	facts.Precondition = "pre"
	facts.FinalState = "post"
	facts.Query.Entities = []string{"a", "b", "c"}
	facts.Query.SeedTitles = []string{"alpha", "beta", "gamma"}
	facts.Query.FinalTitles = finalTitles
	facts.Query.TriggerTxID = 7
	facts.Query.IDAttr = idAttr
	facts.Query.TitleAttr = titleAttr
	facts.Query.WantInitial = wantInitial
	facts.Query.WantLate = wantLate
	facts.Room.Peers = []string{"peer-a", "peer-b"}
	facts.Room.EventIDs = []string{"join-a", "join-b", "presence-a", "broadcast-a", "leave-b"}

	outDir := filepath.Join(t.TempDir(), "fixture")
	reserved, err := corpus.ReserveOutputDir(outDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reserved.Close() }()
	pin, err := mcPinOutputDir(reserved.Path())
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, n := range []string{"fu01-query-a", "fu01-query-b", "fu01-query-c", "fu01-room-a", "fu01-room-b"} {
		payload, err := mcRenderNDJSON(logs[n])
		if err != nil {
			t.Fatal(err)
		}
		if err := mcPublishNDJSON(reserved, pin, n, ".ndjson", payload); err != nil {
			t.Fatalf("publish %s: %v", n, err)
		}
		names = append(names, n+".ndjson")
	}
	if err := reserved.WriteEvidence("fu01-capture-facts.json", facts); err != nil {
		t.Fatal(err)
	}
	names = append(names, "fu01-capture-facts.json")
	entries, err := corpus.ManifestArtifacts(outDir, names)
	if err != nil {
		t.Fatal(err)
	}
	candidate := mcTestCandidate()
	candidate.FixtureApp = mcTestApp
	manifest := corpus.CaptureManifest{
		Candidate:    candidate,
		Precondition: "pre",
		FinalState:   "post",
		Artifacts:    entries,
	}
	if err := reserved.WriteEvidence("manifest.json", manifest); err != nil {
		t.Fatal(err)
	}
	if err := mcVerifyDiskCapture(outDir); err != nil {
		t.Fatalf("valid hermetic fixture rejected: %v", err)
	}
	return outDir
}

// mcLoadDiskLogs parses every NDJSON artifact of a disk fixture.
func mcLoadDiskLogs(t *testing.T, dir string) map[string][]mcFrame {
	t.Helper()
	out := map[string][]mcFrame{}
	for _, n := range []string{
		"fu01-query-a.ndjson", "fu01-query-b.ndjson", "fu01-query-c.ndjson",
		"fu01-room-a.ndjson", "fu01-room-b.ndjson",
	} {
		out[n] = mcParseNDJSON(t, dir, n)
	}
	return out
}

// mcStoreDiskLogsAndRecompute rewrites mutated logs and recomputes a valid
// manifest over them, modelling an attacker who fixes checksums after
// mutating a previously-unchecked field. The oracle must still reject.
func mcStoreDiskLogsAndRecompute(t *testing.T, dir string, logs map[string][]mcFrame) {
	t.Helper()
	for n, l := range logs {
		payload, err := mcRenderNDJSON(l)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, n), payload, 0600); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := corpus.ManifestArtifacts(dir, mcDiskArtifactNames)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest corpus.CaptureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Artifacts = entries
	out, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(out, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// TestMcDiskReplayRejectsMutations proves F2: independently mutating each
// previously-unchecked field (query-B/C acks, room init, room-B leave event
// ID, room-B ordering) with checksums recomputed is rejected by disk replay
// in every case.
func TestMcDiskReplayRejectsMutations(t *testing.T) {
	valid := mcValidDiskFixture(t)
	findOp := func(log []mcFrame, op string) int {
		idx := -1
		for i, e := range log {
			if e.Frame["op"] == op {
				if idx != -1 {
					t.Fatalf("fixture has duplicate %s", op)
				}
				idx = i
			}
		}
		if idx == -1 {
			t.Fatalf("fixture missing %s", op)
		}
		return idx
	}
	cases := map[string]func(map[string][]mcFrame){
		"query-B ack event": func(logs map[string][]mcFrame) {
			logs["fu01-query-b.ndjson"][1].Frame["client-event-id"] = "fu01-query-b-X"
		},
		"query-C ack event": func(logs map[string][]mcFrame) {
			logs["fu01-query-c.ndjson"][1].Frame["client-event-id"] = "wrong"
		},
		"room init attrs": func(logs map[string][]mcFrame) {
			logs["fu01-room-a.ndjson"][0].Frame["attrs"] = []any{map[string]any{"injected": true}}
		},
		"room-B leave event": func(logs map[string][]mcFrame) {
			log := logs["fu01-room-b.ndjson"]
			log[findOp(log, "leave-room-ok")].Frame["client-event-id"] = "leave-b-X"
		},
		"room-B ordering": func(logs map[string][]mcFrame) {
			log := logs["fu01-room-b.ndjson"]
			i, j := findOp(log, "server-broadcast"), findOp(log, "leave-room-ok")
			log[i].Frame, log[j].Frame = log[j].Frame, log[i].Frame
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dst := filepath.Join(t.TempDir(), "mutated")
			if err := copyDir(t, valid, dst); err != nil {
				t.Fatal(err)
			}
			logs := mcLoadDiskLogs(t, dst)
			mutate(logs)
			mcStoreDiskLogsAndRecompute(t, dst, logs)
			if err := mcVerifyDiskCapture(dst); err == nil {
				t.Fatalf("mutated %s accepted by disk replay", name)
			}
		})
	}
}

// TestMcCredentialDistinctness proves F3: late joiner C reusing A's or B's
// token with a fresh session fails, and pairwise session/token uniqueness
// holds for all query subscribers.
func TestMcCredentialDistinctness(t *testing.T) {
	mk := func(sess, tok string) *mcSub { return &mcSub{name: "x", sessionID: sess, token: tok} }
	t.Run("C reuses A token", func(t *testing.T) {
		subs := []*mcSub{mk("sess-a", "tok-a"), mk("sess-b", "tok-b"), mk("sess-c", "tok-a")}
		if err := mcAssertDistinctCredentials(subs); err == nil {
			t.Fatal("C reusing A's token accepted")
		}
	})
	t.Run("C reuses B token", func(t *testing.T) {
		subs := []*mcSub{mk("sess-a", "tok-a"), mk("sess-b", "tok-b"), mk("sess-c", "tok-b")}
		if err := mcAssertDistinctCredentials(subs); err == nil {
			t.Fatal("C reusing B's token accepted")
		}
	})
	t.Run("A B share session", func(t *testing.T) {
		if err := mcAssertDistinctCredentials([]*mcSub{mk("sess-a", "tok-a"), mk("sess-a", "tok-b")}); err == nil {
			t.Fatal("shared session accepted")
		}
	})
	t.Run("A B share token", func(t *testing.T) {
		if err := mcAssertDistinctCredentials([]*mcSub{mk("sess-a", "tok-a"), mk("sess-b", "tok-a")}); err == nil {
			t.Fatal("shared token accepted")
		}
	})
	t.Run("distinct passes", func(t *testing.T) {
		subs := []*mcSub{mk("sess-a", "tok-a"), mk("sess-b", "tok-b"), mk("sess-c", "tok-c")}
		if err := mcAssertDistinctCredentials(subs); err != nil {
			t.Fatalf("distinct credentials rejected: %v", err)
		}
	})
}

// mcTestSilentSub builds a subscriber whose server holds the stream open
// with headers flushed and no frames, modelling bounded silence.
func mcTestSilentSub(t *testing.T, name string) *mcSub {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	resp, err := mcStreamClient.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	sub := &mcSub{name: name, resp: resp, app: mcTestApp, baseURL: srv.URL}
	sub.scanner = mcTestScanner(resp.Body)
	return sub
}

// mcTestDelayedSub builds a subscriber whose server emits one frame after a
// delay, modelling a post-ack extra frame inside the quiet window.
func mcTestDelayedSub(t *testing.T, name string, delay time.Duration, frame map[string]any) *mcSub {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(delay)
		raw, _ := json.Marshal(frame)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", raw)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(srv.Close)
	resp, err := mcStreamClient.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	sub := &mcSub{name: name, resp: resp, app: mcTestApp, baseURL: srv.URL}
	sub.scanner = mcTestScanner(resp.Body)
	return sub
}

// TestMcRoomQuiescenceCoversBothStreams proves F4: B gets a quiet check after
// its final leave ack. A scripted extra B frame shortly after the leave ack
// fails the room leg tail.
func TestMcRoomQuiescenceCoversBothStreams(t *testing.T) {
	t.Run("both silent passes", func(t *testing.T) {
		a := mcTestSilentSub(t, "room-A")
		b := mcTestSilentSub(t, "room-B")
		if err := mcQuiesceRoomLeg(a, b); err != nil {
			t.Fatalf("silent room tail failed: %v", err)
		}
	})
	t.Run("extra B frame fails", func(t *testing.T) {
		a := mcTestSilentSub(t, "room-A")
		b := mcTestDelayedSub(t, "room-B", 50*time.Millisecond, map[string]any{"op": "refresh-presence"})
		if err := mcQuiesceRoomLeg(a, b); err == nil {
			t.Fatal("extra B frame after leave ack accepted")
		}
	})
}

// TestMcHelpMentionsMulticlient proves F5: --transport, --output-dir, --repo,
// and --database-url help strings cover both managed modes.
func TestMcHelpMentionsMulticlient(t *testing.T) {
	var out, diagnostic bytes.Buffer
	if code := run([]string{"-h"}, &out, &diagnostic); code != 2 {
		t.Fatalf("help exit = %d; want 2", code)
	}
	text := diagnostic.String()
	for _, want := range []string{
		"ignored by managed-record and managed-record-multiclient",
		"required for differential, record, managed-record, and managed-record-multiclient",
		"repository path for managed-record and managed-record-multiclient",
		"admin PostgreSQL URL for managed-record and managed-record-multiclient",
		"managed-record-multiclient (candidate-bound FU-01 multi-client SSE capture)",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("help output omits %q", want)
		}
	}
}
