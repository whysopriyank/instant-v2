package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// TestFU01RoomCaptureReplay closes the FU-01 2B-3 room leg: it replays the
// checked-in raw per-subscriber SSE capture (corpus/rooms-fanout-positive.json)
// bound to the rooms-fanout-positive coverage row by its self-bound metadata
// envelope (scenario == id == fixture == rooms-fanout-positive, transport
// sse, status covered). The capture was produced by the accepted recorder
// `corpusctl --mode managed-record-multiclient` (candidate
// 67718bc0342cc631f5fb963678b58f8338e0ca28, clean tree) against room
// "fu01-room": A joins solo, B late-joins to the exact two-member snapshot,
// B resyncs explicitly, A set-presence fans the exact update to both peers,
// A client-broadcasts peer-only with the exact sender session and no echo
// to the sender, and B leaves converging survivor A to the exact
// one-member snapshot. This test reruns that exact flow live against the
// owned isolated Postgres fixture declared by
// corpus/fixtures/rooms-fanout-positive.json (the deterministic cf003 seed
// identity reused via cf003PostgresMux) over the production-mounted
// GET/POST /runtime/sse path, and asserts every one of the 15 checked-in
// frames exactly (whole-frame reflect.DeepEqual) both as each frame arrives
// live and again from retention alone. Session-bearing values (A's and B's
// mounted session ids, and the fixture app id) are random per run and are
// never checked in literal: the checked-in templates carry sentinel
// placeholders that this test substitutes with the live values via a full
// JSON round trip before comparing. The ack/presence either-order rule
// (contract section 2b) is honored by fu01RoomAwaitPair, which accepts
// exactly the set {ack, presence} per actor step with nothing interleaved,
// mirroring fu01AwaitAckAndPresence / cf003AwaitSSERoomAckAndPresence but
// adapted to the capture's own room id ("fu01-room", not the cf003
// harness's "cf003-room") and to comparison against the rendered checked-in
// envelope. Report-only exact corpus replay of the already-accepted
// assembled leg TestCF003AssembledRoomFanoutSSE plus the FU-01 contract
// shape proof TestFU01CaptureContractRoomFanout; not a captured v1 oracle;
// no corpusctl recorder behavior change.
func TestFU01RoomCaptureReplay(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "rooms-fanout-positive.json"))
	if err != nil {
		t.Fatal(err)
	}
	var capture roomCapture
	if err := json.Unmarshal(raw, &capture); err != nil {
		t.Fatalf("capture decode: %v", err)
	}
	if capture.Scenario != "rooms-fanout-positive" || capture.ID != "rooms-fanout-positive" ||
		capture.Fixture != "rooms-fanout-positive" || capture.Transport != "sse" || capture.Status != "covered" {
		t.Fatalf("capture envelope = %q/%q/%q/%q/%q; want rooms-fanout-positive/rooms-fanout-positive/rooms-fanout-positive/sse/covered",
			capture.Scenario, capture.ID, capture.Fixture, capture.Transport, capture.Status)
	}
	if capture.Source == "" || capture.Redaction == "" {
		t.Fatalf("capture envelope missing source/redaction narrative")
	}
	if len(capture.Stream) != 15 {
		t.Fatalf("capture stream frames = %d; want exactly 15 (8 room-A + 7 room-B)", len(capture.Stream))
	}

	// Split the checked-in stream into its two per-subscriber sequences and
	// prove the (subscriber, seq) keying is dense and correctly tagged
	// before any byte is replayed.
	var streamA, streamB []map[string]any
	for i, entry := range capture.Stream {
		switch entry.Subscriber {
		case "room-A":
			if entry.Seq != len(streamA) {
				t.Fatalf("stream entry %d room-A seq = %d; want %d", i, entry.Seq, len(streamA))
			}
			streamA = append(streamA, entry.Data)
		case "room-B":
			if entry.Seq != len(streamB) {
				t.Fatalf("stream entry %d room-B seq = %d; want %d", i, entry.Seq, len(streamB))
			}
			streamB = append(streamB, entry.Data)
		default:
			t.Fatalf("stream entry %d subscriber = %q; want room-A or room-B", i, entry.Subscriber)
		}
	}
	if len(streamA) != 8 || len(streamB) != 7 {
		t.Fatalf("per-subscriber frame counts = A:%d B:%d; want exactly A:8 B:7", len(streamA), len(streamB))
	}

	// Mechanical masking-rule pre-checks: no checked-in frame may carry a
	// real session value, and the sentinel occurrence counts pin the exact
	// masked shape documented in the capture's redaction rule.
	whole := roomEncode(t, capture.Stream)
	if strings.Contains(whole, "sess-") {
		t.Fatalf("checked-in stream carries a real session value")
	}
	for sentinel, want := range map[string]int{
		roomSentinelA:   9,
		roomSentinelB:   6,
		roomSentinelApp: 2,
	} {
		if got := strings.Count(whole, sentinel); got != want {
			t.Fatalf("sentinel %q occurs %d times; want exactly %d", sentinel, got, want)
		}
	}

	// The declared fixture must name the actual capture/replay identity: the
	// fixture file's appId/creatorId/adminToken/txSteps must equal the
	// deterministic cf003 seed identity (fixed UUID triple, empty txSteps)
	// that cf003PostgresMux always provisions. A fixture edited to a foreign
	// identity fails here before any byte is replayed.
	fixtureRaw, err := os.ReadFile(filepath.Join("..", "..", "corpus", "fixtures", "rooms-fanout-positive.json"))
	if err != nil {
		t.Fatal(err)
	}
	var declaredFixture struct {
		AppID      string `json:"appId"`
		CreatorID  string `json:"creatorId"`
		AdminToken string `json:"adminToken"`
		TxSteps    []any  `json:"txSteps"`
	}
	if err := json.Unmarshal(fixtureRaw, &declaredFixture); err != nil {
		t.Fatalf("fixture decode: %v", err)
	}
	if declaredFixture.AppID != cf003AppID || declaredFixture.CreatorID != cf003CreatorID ||
		declaredFixture.AdminToken != cf003AdminToken {
		t.Fatalf("declared fixture identity = %q/%q/%q; want seed %q/%q/%q",
			declaredFixture.AppID, declaredFixture.CreatorID, declaredFixture.AdminToken,
			cf003AppID, cf003CreatorID, cf003AdminToken)
	}
	if declaredFixture.TxSteps == nil || len(declaredFixture.TxSteps) != 0 {
		t.Fatalf("declared fixture txSteps = %#v; want exactly empty", declaredFixture.TxSteps)
	}

	mux, appID, _ := cf003PostgresMux(t)
	appStr := platform.UUIDToStr(appID)
	if appStr != declaredFixture.AppID {
		t.Fatalf("live fixture app = %q; want declared %q", appStr, declaredFixture.AppID)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	// Live substitution map, filled as each subscriber's credentials are
	// observed. Every want-frame below is rendered from the checked-in
	// envelope through this map, never hardcoded separately, so drift in
	// the corpus file changes the live assertions.
	live := map[string]string{roomSentinelApp: appStr}
	if strings.Contains(appStr, "<room-") {
		t.Fatalf("live app id collides with sentinel space: %q", appStr)
	}

	a := cf003OpenSSE(t, ctx, server, appStr)
	b := cf003OpenSSE(t, ctx, server, appStr)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE room clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}
	var logA, logB []fu01Frame

	a.post(t, ctx, map[string]any{"op": "init", "app-id": appStr})
	initA := fu01Read(t, a, "A", &logA)
	aSess := cf003AssertSSERoomInit(t, initA, appStr)
	if aSess == "" || strings.Contains(aSess, "<room-") {
		t.Fatalf("A live session invalid: %q", aSess)
	}
	live[roomSentinelA] = aSess
	fu01RoomCompare(t, "A init", streamA[0], live, initA)

	b.post(t, ctx, map[string]any{"op": "init", "app-id": appStr})
	initB := fu01Read(t, b, "B", &logB)
	bSess := cf003AssertSSERoomInit(t, initB, appStr)
	if bSess == "" || bSess == aSess || strings.Contains(bSess, "<room-") {
		t.Fatalf("B live session invalid: %q", bSess)
	}
	live[roomSentinelB] = bSess
	fu01RoomCompare(t, "B init", streamB[0], live, initB)

	// Freshness: no checked-in byte may leak the live session/app identity
	// values this run actually minted.
	for _, value := range []string{aSess, bSess, appStr} {
		if strings.Contains(string(raw), value) {
			t.Fatalf("checked-in capture leaks live value %q", value)
		}
	}

	// A joins alone: ack plus the exact solo presence, in either order.
	a.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "fu01-room", "peer-id": "peer-a",
		"data": map[string]any{"mood": "ok"}, "client-event-id": "join-a",
	})
	fu01RoomAwaitPair(t, a, "A", &logA, live, streamA[1], streamA[2])

	// B late-joins: both peers converge to the exact two-member snapshot;
	// B's ack and its caused presence may arrive in either order.
	b.post(t, ctx, map[string]any{
		"op": "join-room", "room-id": "fu01-room", "peer-id": "peer-b",
		"data": map[string]any{}, "client-event-id": "join-b",
	})
	fu01RoomAwaitPair(t, b, "B", &logB, live, streamB[1], streamB[2])
	fu01RoomCompare(t, "A both presence", streamA[3], live, fu01Read(t, a, "A", &logA))

	// B resyncs explicitly and sees the same snapshot.
	b.post(t, ctx, map[string]any{"op": "refresh-presence", "room-id": "fu01-room"})
	fu01RoomCompare(t, "B resync presence", streamB[3], live, fu01Read(t, b, "B", &logB))

	// Presence update fans the exact updated snapshot to both peers.
	a.post(t, ctx, map[string]any{
		"op": "set-presence", "room-id": "fu01-room",
		"data": map[string]any{"mood": "great"}, "client-event-id": "presence-a",
	})
	fu01RoomAwaitPair(t, a, "A", &logA, live, streamA[4], streamA[5])
	fu01RoomCompare(t, "B updated presence", streamB[4], live, fu01Read(t, b, "B", &logB))

	// Peer-only broadcast: sender sees only its ack; peer sees the exact
	// server-broadcast carrying the sender session. Linkage is proven
	// because the checked-in template's session-id sentinel renders to A's
	// own live session: any other session (e.g. a stray echo to the
	// sender, or B's own session) fails the DeepEqual below. Sender
	// isolation (no echo) is proven by retention completeness further
	// down: A's retained log carries no server-broadcast frame at all.
	a.post(t, ctx, map[string]any{
		"op": "client-broadcast", "room-id": "fu01-room", "topic": "chat",
		"data": map[string]any{"message": "hello"}, "client-event-id": "broadcast-a",
	})
	fu01RoomCompare(t, "A broadcast ack", streamA[6], live, fu01Read(t, a, "A", &logA))
	fu01RoomCompare(t, "B server-broadcast", streamB[5], live, fu01Read(t, b, "B", &logB))

	// Leave converges the survivor to the exact one-member snapshot.
	b.post(t, ctx, map[string]any{
		"op": "leave-room", "room-id": "fu01-room", "client-event-id": "leave-b",
	})
	fu01RoomCompare(t, "B leave ack", streamB[6], live, fu01Read(t, b, "B", &logB))
	fu01RoomCompare(t, "A final presence", streamA[7], live, fu01Read(t, a, "A", &logA))

	// Retention completeness: exact per-subscriber op sequences, reusing
	// the FU-01 contract-shape helpers verbatim (they already drive this
	// exact flow's op-set bookkeeping and are room-id agnostic).
	fu01AssertRoomOps(t, logA, "A", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"set-presence-ok", "refresh-presence"},
		{"client-broadcast-ok"},
		{"refresh-presence"},
	})
	fu01AssertRoomOps(t, logB, "B", [][]string{
		{"init-ok"},
		{"join-room-ok", "refresh-presence"},
		{"refresh-presence"},
		{"refresh-presence"},
		{"server-broadcast"},
		{"leave-room-ok"},
	})
	fu01AssertRetentionSeq(t, logA, "A", 8)
	fu01AssertRetentionSeq(t, logB, "B", 7)

	// Replay from retention alone: every retained frame re-asserts against
	// its exact checked-in counterpart by (subscriber, seq); no live
	// stream is touched for this pass.
	for i, entry := range logA {
		fu01RoomCompare(t, fmt.Sprintf("replay A[%d]", i), streamA[i], live, entry.frame)
	}
	for i, entry := range logB {
		fu01RoomCompare(t, fmt.Sprintf("replay B[%d]", i), streamB[i], live, entry.frame)
	}

	// Bounded quiescence on both converged streams.
	fu01AssertQuiet(t, a, "A", 250*time.Millisecond)
	fu01AssertQuiet(t, b, "B", 250*time.Millisecond)

	a.closeAndAwaitUnauthorized(t, ctx)
	b.closeAndAwaitUnauthorized(t, ctx)
}

// roomCapture is the checked-in raw multi-client capture
// (corpus/rooms-fanout-positive.json) bound to the rooms-fanout-positive
// coverage row by its self-bound metadata envelope. Unlike the single-flow
// transactions-lookup-lifecycle capture, this recorder
// (corpusctl --mode managed-record-multiclient) persists per-subscriber
// decoded data frames only, not raw HTTP request/response bytes: there is
// no connects/posts section, only the retained stream (see the envelope's
// "excluded" field).
type roomCapture struct {
	Scenario  string `json:"scenario"`
	ID        string `json:"id"`
	Fixture   string `json:"fixture"`
	Transport string `json:"transport"`
	Status    string `json:"status"`
	Source    string `json:"source"`
	Redaction string `json:"redaction"`
	Excluded  string `json:"excluded"`
	Stream    []struct {
		Subscriber string         `json:"subscriber"`
		Seq        int            `json:"seq"`
		Data       map[string]any `json:"data"`
	} `json:"stream"`
}

// Sentinel placeholders for session-bearing and per-run values. None is
// "sess-"-prefixed and the sentinel space ("<room-") is disjoint from any
// live value this route can emit, so a checked-in real value or an
// unsubstituted sentinel is caught mechanically before any byte is
// replayed.
const (
	roomSentinelA   = "<room-a-session>"
	roomSentinelB   = "<room-b-session>"
	roomSentinelApp = "<room-app-id>"
)

// roomEncode marshals without HTML escaping so sentinel angle brackets
// survive string substitution exactly as checked in.
func roomEncode(t *testing.T, value any) string {
	t.Helper()
	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		t.Fatalf("JSON encode: %v", err)
	}
	return sb.String()
}

// roomRender substitutes live values for their sentinels in a checked-in
// template via a full JSON round trip (never raw string surgery on the
// decoded map), returning the frame with every sentinel resolved. Any
// sentinel left unresolved fails immediately.
func roomRender(t *testing.T, tmpl map[string]any, live map[string]string) map[string]any {
	t.Helper()
	encoded := roomEncode(t, tmpl)
	for sentinel, value := range live {
		encoded = strings.ReplaceAll(encoded, sentinel, value)
	}
	if strings.Contains(encoded, "<room-") {
		t.Fatalf("unsubstituted sentinel remains in rendered frame: %s", encoded)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(encoded), &out); err != nil {
		t.Fatalf("render decode: %v", err)
	}
	return out
}

// fu01RoomCompare renders a checked-in template against the live
// substitution map and asserts the live frame equals it exactly
// (whole-frame reflect.DeepEqual).
func fu01RoomCompare(t *testing.T, tag string, tmpl map[string]any, live map[string]string, got map[string]any) {
	t.Helper()
	want := roomRender(t, tmpl, live)
	if !reflect.DeepEqual(got, want) {
		gotJSON, _ := json.Marshal(got)
		wantJSON, _ := json.Marshal(want)
		t.Fatalf("%s frame = %s; want exactly %s", tag, gotJSON, wantJSON)
	}
}

// fu01RoomAwaitPair reads two frames for client, matching them to the two
// checked-in templates (an ack and its caused refresh-presence) by op
// identity regardless of arrival order: the contract's ack/presence
// either-order rule (fu01-multiclient-capture-contract.md section 2b) --
// the capture accepts exactly the set {ack, presence} with nothing else
// interleaved. This mirrors fu01AwaitAckAndPresence /
// cf003AwaitSSERoomAckAndPresence, adapted to compare each live frame
// against its rendered checked-in counterpart (rather than a hardcoded
// want) and to the capture's own room id ("fu01-room"), since those
// existing helpers hardcode the cf003 harness's "cf003-room".
func fu01RoomAwaitPair(t *testing.T, client *cf003SSEClient, name string, log *[]fu01Frame, live map[string]string, ackTmpl, presenceTmpl map[string]any) {
	t.Helper()
	wantAck := roomRender(t, ackTmpl, live)
	wantPresence := roomRender(t, presenceTmpl, live)
	ackOp, _ := wantAck["op"].(string)
	ackSeen, presenceSeen := false, false
	for i := 0; i < 2; i++ {
		frame := fu01Read(t, client, name, log)
		switch frame["op"] {
		case ackOp:
			if ackSeen {
				t.Fatalf("%s duplicate ack: %#v", name, frame)
			}
			if !reflect.DeepEqual(frame, wantAck) {
				t.Fatalf("%s ack frame = %#v; want exactly %#v", name, frame, wantAck)
			}
			ackSeen = true
		case "refresh-presence":
			if presenceSeen {
				t.Fatalf("%s duplicate presence: %#v", name, frame)
			}
			if !reflect.DeepEqual(frame, wantPresence) {
				t.Fatalf("%s presence frame = %#v; want exactly %#v", name, frame, wantPresence)
			}
			presenceSeen = true
		default:
			t.Fatalf("%s unexpected frame: %#v", name, frame)
		}
	}
	if !ackSeen || !presenceSeen {
		t.Fatalf("%s ack/presence incomplete: ack=%v presence=%v", name, ackSeen, presenceSeen)
	}
}
