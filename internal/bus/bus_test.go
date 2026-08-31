package bus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ---------------------------------------------------------------------------
// fakeConn
// ---------------------------------------------------------------------------

type fakeConn struct {
	mu       sync.Mutex
	execs    []execCall
	execErr  error
	notifs   []*pgconn.Notification
	idx      int
	waitErr  error
	waitHook func(ctx context.Context) error
}

type execCall struct {
	SQL  string
	Args []any
}

func (f *fakeConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]any, len(args))
	copy(cp, args)
	f.execs = append(f.execs, execCall{SQL: sql, Args: cp})
	if f.execErr != nil {
		return pgconn.NewCommandTag(""), f.execErr
	}
	return pgconn.NewCommandTag("LISTEN"), nil
}

func (f *fakeConn) WaitForNotification(ctx context.Context) (*pgconn.Notification, error) {
	f.mu.Lock()
	if f.waitHook != nil {
		hook := f.waitHook
		f.mu.Unlock()
		if err := hook(ctx); err != nil {
			return nil, err
		}
		f.mu.Lock()
	}
	if f.idx < len(f.notifs) {
		n := f.notifs[f.idx]
		f.idx++
		f.mu.Unlock()
		return n, nil
	}
	if f.waitErr != nil {
		f.mu.Unlock()
		return nil, f.waitErr
	}
	f.mu.Unlock()
	// block until ctx done — mirrors pgconn's blocking behavior
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeConn) execCalls() []execCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make([]execCall, len(f.execs))
	copy(cp, f.execs)
	return cp
}

// ---------------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------------

func TestEncodeSmall(t *testing.T) {
	inv := Invalidation{AppID: "app-1", AttrIDs: []string{"a1", "a2"}, TxID: 42}
	b, err := Encode(inv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(b) > 7400 {
		t.Fatalf("small payload unexpectedly >7400: %d", len(b))
	}
	var got Invalidation
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.AppID != inv.AppID || got.TxID != inv.TxID || len(got.AttrIDs) != 2 {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, inv)
	}
}

func TestEncodeTruncationFallback(t *testing.T) {
	// Build payload >7400 bytes by stuffing many attr IDs.
	attr := strings.Repeat("x", 36) // UUID-ish length
	attrs := make([]string, 0, 600)
	for i := range 600 {
		attrs = append(attrs, attr+strings.Repeat("y", i%4))
	}
	inv := Invalidation{AppID: "app-big", AttrIDs: attrs, TxID: 99}
	b, err := Encode(inv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Encode must have truncated to nil AttrIDs, so payload contains null
	if !bytes.Contains(b, []byte(`"attr_ids":null`)) {
		t.Fatalf("expected truncated payload with null attr_ids, got %s (len %d)", b[:min(500, len(b))], len(b))
	}
	var got Invalidation
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal truncated: %v", err)
	}
	if got.AttrIDs != nil {
		t.Fatalf("expected AttrIDs nil after truncation, got %v (len %d)", got.AttrIDs, len(got.AttrIDs))
	}
	if got.AppID != inv.AppID || got.TxID != inv.TxID {
		t.Fatalf("truncated round-trip lost AppID/TxID: got %+v", got)
	}
	// Original must not be mutated
	if len(inv.AttrIDs) != 600 {
		t.Fatalf("Encode mutated input AttrIDs")
	}
}

// Staged degradation (docs/09 §T2.5): an oversized payload drops the
// entity-level Changes first and keeps the attr-id projection; only when
// even that exceeds the cap do AttrIDs go null. Peers lose splice
// granularity before they lose topic granularity.
func TestEncodeStagedDegradation(t *testing.T) {
	attr := strings.Repeat("x", 36)
	// Changes alone push past 7400, attrs stay small.
	changes := make([]EntityChange, 0, 200)
	for i := range 200 {
		changes = append(changes, EntityChange{
			Etype:    "todos",
			EntityID: fmt.Sprintf("%s-%04d", attr, i),
			AttrIDs:  []string{attr},
		})
	}
	smallAttrs := []string{"a1", "a2"}
	inv := Invalidation{AppID: "app-stage", AttrIDs: smallAttrs, TxID: 5, Changes: changes}
	b, err := Encode(inv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got Invalidation
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Changes != nil {
		t.Fatalf("expected Changes dropped first, got %d", len(got.Changes))
	}
	if got.AttrIDs == nil || len(got.AttrIDs) != 2 {
		t.Fatalf("expected AttrIDs projection kept, got %v", got.AttrIDs)
	}
	if got.AppID != inv.AppID || got.TxID != inv.TxID {
		t.Fatalf("lost AppID/TxID: %+v", got)
	}
	if len(inv.Changes) != 200 {
		t.Fatalf("Encode mutated input Changes")
	}
}

func TestEncodeAlreadyNilSmallPath(t *testing.T) {
	inv := Invalidation{AppID: "app-nil", AttrIDs: nil, TxID: 1}
	b, err := Encode(inv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Contains(b, []byte(`null`)) {
		t.Fatalf("expected null attr_ids, got %s", b)
	}
	var got Invalidation
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.AttrIDs != nil {
		t.Fatalf("expected nil AttrIDs, got %v", got.AttrIDs)
	}
}

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

func TestPublishCallsExec(t *testing.T) {
	fc := &fakeConn{}
	inv := Invalidation{AppID: "app-pub", AttrIDs: []string{"attr-1"}, TxID: 123}
	if err := Publish(context.Background(), fc, inv); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	calls := fc.execCalls()
	if len(calls) != 1 {
		t.Fatalf("expected 1 Exec, got %d", len(calls))
	}
	if calls[0].SQL != "SELECT pg_notify($1,$2)" {
		t.Fatalf("unexpected SQL: %q", calls[0].SQL)
	}
	if len(calls[0].Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(calls[0].Args))
	}
	if calls[0].Args[0] != Channel {
		t.Fatalf("expected channel %q, got %q", Channel, calls[0].Args[0])
	}
	payload, ok := calls[0].Args[1].(string)
	if !ok {
		t.Fatalf("expected string payload, got %T", calls[0].Args[1])
	}
	var got Invalidation
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		t.Fatalf("payload not valid JSON: %v payload=%s", err, payload)
	}
	if got.AppID != inv.AppID || got.TxID != inv.TxID {
		t.Fatalf("payload mismatch: got %+v want %+v", got, inv)
	}
}

func TestPublishTruncatesBigPayload(t *testing.T) {
	fc := &fakeConn{}
	attrs := make([]string, 500)
	for i := range attrs {
		attrs[i] = strings.Repeat("z", 36)
	}
	inv := Invalidation{AppID: "app-big-pub", AttrIDs: attrs, TxID: 777}
	if err := Publish(context.Background(), fc, inv); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	calls := fc.execCalls()
	payload := calls[0].Args[1].(string)
	if !strings.Contains(payload, `"attr_ids":null`) {
		t.Fatalf("expected truncated null payload, got len %d", len(payload))
	}
}

// ---------------------------------------------------------------------------
// Run — dispatch, malformed-skip, LISTEN
// ---------------------------------------------------------------------------

func TestRunDispatchesValidNotifications(t *testing.T) {
	inv := Invalidation{AppID: "app-run", AttrIDs: []string{"a1"}, TxID: 10}
	payload, _ := Encode(inv)
	fc := &fakeConn{
		notifs: []*pgconn.Notification{
			{Channel: Channel, Payload: string(payload)},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []Invalidation
	var mu sync.Mutex
	done := make(chan struct{})
	onEvent := func(iv Invalidation) {
		mu.Lock()
		got = append(got, iv)
		mu.Unlock()
		close(done)
		cancel() // stop Run after first delivery
	}
	errCh := make(chan error, 1)
	go func() { errCh <- Run(ctx, fc, onEvent) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for dispatch")
	}
	// Run should exit with context.Canceled after cancel()
	select {
	case err := <-errCh:
		if err != context.Canceled && err != nil {
			// some pgconn impls return context.Canceled wrapped; allow both
			if !strings.Contains(err.Error(), "canceled") {
				t.Fatalf("Run returned unexpected error: %v", err)
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0].AppID != inv.AppID {
		t.Fatalf("got %v want 1 with app %q", got, inv.AppID)
	}
	// Verify LISTEN was issued
	calls := fc.execCalls()
	if len(calls) == 0 || !strings.HasPrefix(calls[0].SQL, "LISTEN") {
		t.Fatalf("expected LISTEN exec, got %v", calls)
	}
}

func TestRunSkipsMalformedAndContinues(t *testing.T) {
	valid := Invalidation{AppID: "app-ok", AttrIDs: []string{"a1"}, TxID: 5}
	validPayload, _ := Encode(valid)

	fc := &fakeConn{
		notifs: []*pgconn.Notification{
			{Channel: Channel, Payload: "not-json{{{ "},       // malformed
			{Channel: Channel, Payload: string(validPayload)}, // valid after malformed
		},
	}

	// Capture log output to verify warning, without polluting test log
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	old := slog.Default()
	slog.SetDefault(slog.New(h))
	defer slog.SetDefault(old)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []Invalidation
	var mu sync.Mutex
	delivered := make(chan struct{})
	onEvent := func(iv Invalidation) {
		mu.Lock()
		got = append(got, iv)
		mu.Unlock()
		close(delivered)
		cancel()
	}
	errCh := make(chan error, 1)
	go func() { errCh <- Run(ctx, fc, onEvent) }()

	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for valid dispatch after malformed")
	}
	select {
	case err := <-errCh:
		if err != context.Canceled && err != nil && !strings.Contains(err.Error(), "canceled") {
			t.Fatalf("Run err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return")
	}
	mu.Lock()
	if len(got) != 1 || got[0].AppID != valid.AppID {
		mu.Unlock()
		t.Fatalf("expected 1 valid dispatch, got %v", got)
	}
	mu.Unlock()

	logged := buf.String()
	if !strings.Contains(logged, "malformed") && !strings.Contains(logged, "skipping") {
		t.Fatalf("expected malformed warning log, got %q", logged)
	}
}

func TestRunIgnoresOtherChannels(t *testing.T) {
	valid := Invalidation{AppID: "app-other", TxID: 1}
	payload, _ := Encode(valid)
	fc := &fakeConn{
		notifs: []*pgconn.Notification{
			{Channel: "other_channel", Payload: string(payload)}, // should be ignored
			{Channel: Channel, Payload: string(payload)},         // should dispatch
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	count := 0
	var mu sync.Mutex
	done := make(chan struct{})
	onEvent := func(Invalidation) {
		mu.Lock()
		count++
		mu.Unlock()
		close(done)
		cancel()
	}
	go func() { _ = Run(ctx, fc, onEvent) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	mu.Lock()
	if count != 1 {
		t.Fatalf("expected 1 dispatch (other channel ignored), got %d", count)
	}
	mu.Unlock()
}

func TestRunWithLoggerUsesProvidedLogger(t *testing.T) {
	valid := Invalidation{AppID: "app-logger", TxID: 2}
	payload, _ := Encode(valid)
	fc := &fakeConn{
		notifs: []*pgconn.Notification{
			{Channel: Channel, Payload: "bad json"},
			{Channel: Channel, Payload: string(payload)},
		},
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	onEvent := func(Invalidation) { close(done); cancel() }
	stopped := make(chan struct{})
	go func() {
		_ = RunWithLogger(ctx, fc, onEvent, logger)
		close(stopped)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("RunWithLogger did not stop")
	}
	if !strings.Contains(buf.String(), "malformed") {
		t.Fatalf("expected logger to capture malformed, got %q", buf.String())
	}
}

func TestRunReturnsListenError(t *testing.T) {
	fc := &fakeConn{execErr: assertErr("listen failed")}
	err := Run(context.Background(), fc, func(Invalidation) {})
	if err == nil || !strings.Contains(err.Error(), "listen failed") {
		t.Fatalf("expected listen error, got %v", err)
	}
}

func assertErr(s string) error { return &fakeErr{s} }

type fakeErr struct{ s string }

func (e *fakeErr) Error() string { return e.s }

// compile-time interface assertion: *pgx.Conn satisfies Conn (dedicated conn, not pool).
var _ Conn = (*pgx.Conn)(nil)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---------------------------------------------------------------------------
// Integration — real Postgres LISTEN/NOTIFY round-trip
// Uses the shared integration opt-in and a private database per test.
// ---------------------------------------------------------------------------

// listenReadyConn observes the real LISTEN completion without issuing a
// second LISTEN outside Run or depending on scheduler timing.
type listenReadyConn struct {
	Conn
	ready chan struct{}
}

func (c *listenReadyConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	tag, err := c.Conn.Exec(ctx, sql, args...)
	if err == nil && sql == "LISTEN "+Channel {
		close(c.ready)
	}
	return tag, err
}

func TestIntegrationBusRoundTrip(t *testing.T) {
	db := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Dedicated listener connection (LISTEN dies with the connection — must not be pooled).
	listenConn, err := pgx.Connect(ctx, db.DSN)
	if err != nil {
		t.Fatalf("listen Connect: %v", err)
	}
	t.Cleanup(func() { _ = listenConn.Close(context.Background()) })

	// Publisher connection (separate, as in production — two nodes).
	pubConn, err := pgx.Connect(ctx, db.DSN)
	if err != nil {
		t.Fatalf("pub Connect: %v", err)
	}
	t.Cleanup(func() { _ = pubConn.Close(context.Background()) })

	received := make(chan Invalidation, 4)
	var runErr error
	runDone := make(chan struct{})
	runCtx, runCancel := context.WithCancel(ctx)
	t.Cleanup(func() {
		runCancel()
		select {
		case <-runDone:
		case <-time.After(2 * time.Second):
			t.Error("Run did not exit after cancel")
		}
	})

	listener := &listenReadyConn{Conn: listenConn, ready: make(chan struct{})}
	go func() {
		runErr = Run(runCtx, listener, func(iv Invalidation) {
			select {
			case received <- iv:
			default:
			}
		})
		close(runDone)
	}()

	select {
	case <-listener.ready:
	case <-runDone:
		t.Fatalf("Run stopped before LISTEN completed: %v", runErr)
	case <-ctx.Done():
		t.Fatal("LISTEN did not complete")
	}

	want := Invalidation{AppID: "int-test-app", AttrIDs: []string{"attr-1", "attr-2"}, TxID: 12345}
	if err := Publish(ctx, pubConn, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("mismatch: got %+v want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for NOTIFY delivery")
	}

	// Truncation still delivers (as nil AttrIDs)
	huge := Invalidation{AppID: "int-test-app", TxID: 999}
	huge.AttrIDs = make([]string, 500)
	for i := range huge.AttrIDs {
		huge.AttrIDs[i] = strings.Repeat("x", 36)
	}
	if err := Publish(ctx, pubConn, huge); err != nil {
		t.Fatalf("Publish huge: %v", err)
	}
	select {
	case got := <-received:
		if got.AppID != huge.AppID || got.TxID != huge.TxID {
			t.Fatalf("huge mismatch: got %+v", got)
		}
		if got.AttrIDs != nil {
			t.Fatalf("expected truncated AttrIDs nil, got len %d", len(got.AttrIDs))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for huge NOTIFY")
	}

	runCancel()
	select {
	case <-runDone:
		if runErr != nil && runErr != context.Canceled && !strings.Contains(strings.ToLower(runErr.Error()), "canceled") && !strings.Contains(strings.ToLower(runErr.Error()), "cancelled") {
			// Run returns context.Canceled on cancel — accept either
			t.Logf("Run returned: %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after cancel")
	}
}

// TestAttrsChangedSurvivesRoundTripAndDegradation pins cross-node catalog
// invalidation: the attrs.create flag must survive json round-trip AND the
// staged size degradation (which strips Changes, then AttrIDs) so every peer
// drops its cached AttrCatalog when any node creates attributes.
func TestAttrsChangedSurvivesRoundTripAndDegradation(t *testing.T) {
	inv := Invalidation{
		AppID:        "app-1",
		AttrIDs:      []string{"a1"},
		TxID:         7,
		Changes:      []EntityChange{{Etype: "todos", EntityID: "e1", AttrIDs: []string{"a1"}}},
		AttrsChanged: true,
	}
	b, err := Encode(inv)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got Invalidation
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !got.AttrsChanged {
		t.Fatal("small payload lost AttrsChanged on round-trip")
	}

	// Degrade past 7400 bytes: Changes and AttrIDs go, the flag must stay.
	big := Invalidation{AppID: "app-1", TxID: 7, AttrsChanged: true}
	for i := 0; i < 400; i++ {
		big.Changes = append(big.Changes, EntityChange{
			Etype: "todos", EntityID: fmt.Sprintf("e%03d", i),
			AttrIDs: []string{strings.Repeat("x", 36)},
		})
	}
	db, err := Encode(big)
	if err != nil {
		t.Fatalf("Encode big: %v", err)
	}
	if len(db) > 8000 {
		t.Fatalf("degraded payload still too large for NOTIFY: %d", len(db))
	}
	got = Invalidation{}
	if err := json.Unmarshal(db, &got); err != nil {
		t.Fatalf("unmarshal degraded: %v", err)
	}
	if !got.AttrsChanged {
		t.Fatal("degraded payload lost AttrsChanged — peers would keep stale catalogs")
	}
	if got.Changes != nil || got.AttrIDs != nil {
		t.Fatalf("degradation did not strip payload fields: %+v", got)
	}

	// Absent flag decodes false (old senders / flag-less commits).
	var old Invalidation
	if err := json.Unmarshal([]byte(`{"app_id":"a","tx_id":1}`), &old); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if old.AttrsChanged {
		t.Fatal("legacy payload decoded AttrsChanged=true")
	}
}
