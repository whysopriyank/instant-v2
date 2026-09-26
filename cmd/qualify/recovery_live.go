package main

// Live recovery outcome drivers (QH-001 R1, F3). Every property below is
// measured from process/DB/client observation:
//
//   - writers submit transact frames WITHOUT waiting (non-blocking submit
//     path) and track each tx by client-event-id in a per-outcome ledger
//     (submitted → acked with server tx id | error | unknown-at-crash);
//   - every crash/restart outcome runs ≥2 subscriber sessions with an
//     add-query over the fixture attribute, recording every add-query-ok /
//     refresh-ok result;
//   - convergence means each subscriber's final observed value set equals
//     exactly the DB oracle's value set for the fixture attr (comparing the
//     oracle with itself is not convergence: compareValueSets takes the
//     subscriber-observed set as its first argument);
//   - kill preconditions are asserted from the ledger / subscriber
//     observations under a bound; a missed precondition FAILs the outcome
//     ("precondition not reached"), never passes it;
//   - every tx carries either 1 triple or a multi-triple shape (≥3 triples
//     on distinct entities), so atomicity is checkable: all of a tx's
//     triples present or none; partial txs are counted from the DB;
//   - drain outcomes use SIGTERM (syscall.SIGTERM), record the real exit
//     status and the real observed websocket close codes, and resolve every
//     tx submitted before exit against the client ledger (not a snapshot).

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
)

// liveConn is one candidate session WebSocket with mutex-serialized writes.
type liveConn struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (c *liveConn) send(ctx context.Context, v any) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.conn.Write(wctx, websocket.MessageText, raw)
}

// frameHooks routes inbound frames to ledger/subscriber recorders. All hooks
// run on the pump goroutine and must be goroutine-safe.
type frameHooks struct {
	onInitOK     func()
	onAddQueryOK func(clientEventID string, frame map[string]json.RawMessage)
	onAck        func(clientEventID, serverTxID string)
	onRefresh    func(processedTx string, frame map[string]json.RawMessage)
	onErrorFrame func(clientEventID, msg string)
	onClose      func(code string)
}

// pumpConn reads frames until ctx ends or the socket breaks, then reports
// the real observed close status exactly once.
func pumpConn(ctx context.Context, conn *websocket.Conn, hk frameHooks) {
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			if hk.onClose != nil {
				hk.onClose(closeCodeOf(err))
			}
			return
		}
		var f struct {
			Op            string          `json:"op"`
			ClientEventID string          `json:"client-event-id"`
			TxID          json.RawMessage `json:"tx-id"`
			ProcessedTxID json.RawMessage `json:"processed-tx-id"`
			Result        json.RawMessage `json:"result"`
			Computations  json.RawMessage `json:"computations"`
			Type          string          `json:"type"`
			Message       string          `json:"message"`
			Status        json.RawMessage `json:"status"`
		}
		if json.Unmarshal(data, &f) != nil {
			if hk.onErrorFrame != nil {
				hk.onErrorFrame("", "bad frame JSON")
			}
			continue
		}
		raw := map[string]json.RawMessage{}
		_ = json.Unmarshal(data, &raw)
		switch f.Op {
		case "init-ok":
			if hk.onInitOK != nil {
				hk.onInitOK()
			}
		case "add-query-ok":
			if hk.onAddQueryOK != nil {
				hk.onAddQueryOK(f.ClientEventID, raw)
			}
		case "transact-ok":
			if hk.onAck != nil {
				hk.onAck(f.ClientEventID, rawToString(f.TxID))
			}
		case "refresh-ok", "refresh-ok-delta":
			if hk.onRefresh != nil {
				hk.onRefresh(rawToString(f.ProcessedTxID), raw)
			}
		case "error", "protocol-error":
			msg := f.Message
			if msg == "" {
				msg = f.Type
			}
			if msg == "" {
				msg = "protocol error frame"
			}
			if hk.onErrorFrame != nil {
				hk.onErrorFrame(f.ClientEventID, msg)
			}
		default:
			if hk.onErrorFrame != nil {
				hk.onErrorFrame(f.ClientEventID, "unrecognized op: "+f.Op)
			}
		}
	}
}

// closeCodeOf renders the real observed websocket close status: the numeric
// status when the peer sent a close frame, else the transport error class.
func closeCodeOf(err error) string {
	if err == nil {
		return ""
	}
	if code := websocket.CloseStatus(err); code != -1 {
		return strconv.Itoa(int(code))
	}
	return "no-status:" + errClass(err)
}

func errClass(err error) string {
	s := err.Error()
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 96 {
		return s[:96]
	}
	return s
}

// rawToString normalizes a raw JSON message into a clean string (handles
// both JSON strings and numbers, e.g. numeric tx ids).
func rawToString(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var num json.Number
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&num); err == nil {
		return num.String()
	}
	return strings.Trim(string(raw), "\"")
}

// parseTxInt parses a numeric server/processed tx id; non-numeric ids
// report ok=false and are excluded from watermark comparisons.
func parseTxInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// collectStringLeaves walks arbitrary JSON and records every string leaf.
// Subscriber result envelopes (add-query-ok result trees, refresh-ok
// node-list computations) carry our written values as string leaves; the
// caller intersects with the submitted-value universe, so envelope shape
// drift can only fail closed (missing values), never pass silently.
func collectStringLeaves(raw json.RawMessage, out map[string]struct{}, cap int) {
	if len(out) >= cap || len(raw) == 0 {
		return
	}
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return
	}
	var walk func(x any)
	walk = func(x any) {
		if len(out) >= cap {
			return
		}
		switch t := x.(type) {
		case string:
			out[t] = struct{}{}
		case []any:
			for _, e := range t {
				walk(e)
			}
		case map[string]any:
			for _, e := range t {
				walk(e)
			}
		}
	}
	walk(v)
}

// dialLiveConn opens a candidate session socket and completes init,
// returning the connection ready for add-query/transact traffic. init-ok is
// awaited synchronously: no pump runs yet, so no frame can be lost or
// double-consumed (a temporary pump would own the socket lifetime and
// close it on stop).
func dialLiveConn(ctx context.Context, wsURL, appID string) (*liveConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": {"http://localhost"}},
	})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(64 << 20)
	lc := &liveConn{conn: conn}
	if err := lc.send(ctx, map[string]any{"op": "init", "app-id": appID}); err != nil {
		_ = conn.CloseNow()
		return nil, err
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		rctx, rcancel := context.WithTimeout(ctx, 5*time.Second)
		_, data, rerr := conn.Read(rctx)
		rcancel()
		if rerr != nil {
			_ = conn.CloseNow()
			return nil, fmt.Errorf("init read: %w", rerr)
		}
		var f struct {
			Op      string `json:"op"`
			Type    string `json:"type"`
			Message string `json:"message"`
		}
		if json.Unmarshal(data, &f) != nil {
			_ = conn.CloseNow()
			return nil, fmt.Errorf("init: bad frame JSON")
		}
		switch f.Op {
		case "init-ok":
			return lc, nil
		case "error", "protocol-error":
			_ = conn.CloseNow()
			msg := f.Message
			if msg == "" {
				msg = f.Type
			}
			return nil, fmt.Errorf("init refused: %s", msg)
		default:
			continue // no other frame can precede init-ok; keep waiting
		}
	}
	_ = conn.CloseNow()
	return nil, fmt.Errorf("init-ok timeout")
}

// ---- pipelined writers ----

// pipelinedWriter submits transact frames without waiting and routes every
// transact-ok / error frame into the shared outcome ledger by
// client-event-id. Use ≥2 sessions per outcome.
type pipelinedWriter struct {
	lc      *liveConn
	ledger  *txLedger
	session int
	outcome string
	attr    string
	seq     atomic.Int64
	stop    context.CancelFunc
	done    chan struct{}
	close   atomic.Value // string
}

func newPipelinedWriter(ctx context.Context, wsURL, appID, outcome, attr string, session int, ledger *txLedger) (*pipelinedWriter, error) {
	lc, err := dialLiveConn(ctx, wsURL, appID)
	if err != nil {
		return nil, err
	}
	pctx, stop := context.WithCancel(context.Background())
	w := &pipelinedWriter{lc: lc, ledger: ledger, session: session, outcome: outcome, attr: attr, stop: stop, done: make(chan struct{})}
	w.close.Store("")
	go pumpConn(pctx, lc.conn, frameHooks{
		onAck: func(eid, serverTx string) {
			ledger.settle(eid, serverTx, "", time.Now())
		},
		onRefresh: func(string, map[string]json.RawMessage) {},
		onErrorFrame: func(eid, msg string) {
			if eid != "" {
				ledger.settle(eid, "", "error-frame: "+msg, time.Now())
			}
		},
		onClose: func(code string) {
			// First observed code wins: the server-death status must not
			// be overwritten by our own post-exit shutdown.
			if w.closeCode() == "" {
				w.close.Store(code)
			}
			// Every still-outstanding tx on this socket ends with the
			// observed close status, not a guess.
			for _, r := range ledger.snapshot() {
				if r.Disposition == txUnknown {
					ledger.settle(r.ClientEventID, "", "socket-close: "+w.closeCode(), time.Now())
				}
			}
		},
	})
	go func() {
		<-pctx.Done()
		close(w.done)
	}()
	return w, nil
}

// submit sends one tx (1 or ≥3 triples) without waiting for its ack. The
// ledger entry exists before the bytes hit the wire, so every submitted tx
// is tracked even if the process dies mid-send.
func (w *pipelinedWriter) submit(ctx context.Context, nTriples int, valuePrefix string) string {
	seq := w.seq.Add(1)
	eid := fmt.Sprintf("%s-w%d-%d", w.outcome, w.session, seq)
	triples := make([]txTriple, 0, nTriples)
	steps := make([]any, 0, nTriples)
	for k := 0; k < nTriples; k++ {
		entity := fmt.Sprintf("%08x-%04x-4000-8000-%012x", uint32(os.Getpid())&0xffffff, w.session&0xffff, (seq<<8)|int64(k))
		val := fmt.Sprintf("%s-%d", valuePrefix, k)
		if nTriples == 1 {
			val = valuePrefix
		}
		triples = append(triples, txTriple{Entity: entity, Attr: w.attr, Value: val})
		steps = append(steps, []any{"add-triple", entity, w.attr, val})
	}
	w.ledger.submit(&txRecord{ClientEventID: eid, Triples: triples, Disposition: txUnknown, SubmittedAt: time.Now()})
	if err := w.lc.send(ctx, map[string]any{"op": "transact", "tx-steps": steps, "client-event-id": eid}); err != nil {
		w.ledger.settle(eid, "", "send: "+err.Error(), time.Now())
	}
	return eid
}

func (w *pipelinedWriter) closeCode() string {
	if v := w.close.Load(); v != nil {
		return v.(string)
	}
	return ""
}

func (w *pipelinedWriter) shutdown() {
	w.stop()
	_ = w.lc.conn.Close(websocket.StatusNormalClosure, "")
	<-w.done
	// Deterministic end state: any tx still un-settled never got a server
	// disposition (no ack, no error frame, socket survived to harness
	// shutdown). Settle it here with an explicit reason instead of leaving
	// pump-scheduling races to decide between unknown and socket-close.
	for _, r := range w.ledger.snapshot() {
		if r.Disposition == txUnknown {
			w.ledger.settle(r.ClientEventID, "", "harness-shutdown: never acked before exit", time.Now())
		}
	}
}

// awaitAck polls the ledger for one tx's ack under a bound.
func awaitLedgerAck(ledger *txLedger, eid string, timeout time.Duration) (serverTx string, ok bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, r := range ledger.snapshot() {
			if r.ClientEventID == eid && r.Disposition == txAcked {
				return r.ServerTxID, true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", false
}

// spinAckUncovered busy-spins until eid is acked (or timeout), then reads
// the subscriber watermarks in the same observation beat. It reports the
// server tx id ("" when never acked) and whether at least one subscriber's
// observed watermark still excludes it — the crash-after-commit kill
// precondition, measured with minimal skew.
func spinAckUncovered(ledger *txLedger, eid string, subs []*liveSubscriber, timeout time.Duration) (serverTx string, uncovered bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var acked *txRecord
		for _, r := range ledger.snapshot() {
			if r.ClientEventID == eid && r.Disposition == txAcked {
				acked = r
				break
			}
		}
		if acked == nil {
			continue // busy-spin: no sleep; the window is milliseconds
		}
		srvN, isNum := parseTxInt(acked.ServerTxID)
		if !isNum {
			return acked.ServerTxID, false
		}
		for _, s := range subs {
			s.mu.Lock()
			has, mx := s.obs.HasMaxTx, s.obs.MaxTx
			s.mu.Unlock()
			if !has || mx < srvN {
				return acked.ServerTxID, true
			}
		}
		return acked.ServerTxID, false
	}
	return "", false
}

// ---- subscribers ----

// liveSubscriber holds an add-query over the fixture attribute and records
// every add-query-ok / refresh-ok result plus the real close status.
type liveSubscriber struct {
	lc     *liveConn
	qid    string
	stop   context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
	obs    subObserved
	leaves map[string]struct{}
}

func newLiveSubscriber(ctx context.Context, wsURL, appID, qid string, session int) (*liveSubscriber, error) {
	lc, err := dialLiveConn(ctx, wsURL, appID)
	if err != nil {
		return nil, err
	}
	pctx, stop := context.WithCancel(context.Background())
	s := &liveSubscriber{lc: lc, qid: qid, stop: stop, done: make(chan struct{}), leaves: map[string]struct{}{}}
	s.obs.Session = session
	ackCh := make(chan error, 1)
	go pumpConn(pctx, lc.conn, frameHooks{
		onAddQueryOK: func(eid string, frame map[string]json.RawMessage) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if eid != s.qid {
				return
			}
			s.obs.AddQueryOK = eid == s.qid
			if raw, ok := frame["result"]; ok {
				collectStringLeaves(raw, s.leaves, 20000)
				s.obs.LeavesCapped = len(s.leaves) >= 20000
			}
			if raw, ok := frame["processed-tx-id"]; ok {
				if n, isNum := parseTxInt(rawToString(raw)); isNum {
					if !s.obs.HasMaxTx || n > s.obs.MaxTx {
						s.obs.MaxTx, s.obs.HasMaxTx = n, true
					}
				}
			}
			select {
			case ackCh <- nil:
			default:
			}
		},
		onRefresh: func(processed string, frame map[string]json.RawMessage) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.obs.RefreshOKs++
			if n, isNum := parseTxInt(processed); isNum {
				if !s.obs.HasMaxTx || n > s.obs.MaxTx {
					s.obs.MaxTx, s.obs.HasMaxTx = n, true
				}
			}
			if raw, ok := frame["computations"]; ok {
				collectStringLeaves(raw, s.leaves, 20000)
				s.obs.LeavesCapped = len(s.leaves) >= 20000
			}
		},
		onErrorFrame: func(eid, msg string) {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.obs.ErrorFrames = append(s.obs.ErrorFrames, eid+":"+msg)
		},
		onClose: func(code string) {
			s.mu.Lock()
			defer s.mu.Unlock()
			// First observed code wins (see writer onClose).
			if s.obs.CloseCode == "" {
				s.obs.CloseCode = code
			}
		},
	})
	if err := lc.send(ctx, map[string]any{"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": qid}); err != nil {
		stop()
		_ = lc.conn.CloseNow()
		return nil, err
	}
	select {
	case err := <-ackCh:
		if err != nil {
			stop()
			return nil, err
		}
	case <-ctx.Done():
		stop()
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		stop()
		return nil, fmt.Errorf("add-query-ok timeout for %s", qid)
	}
	go func() {
		<-pctx.Done()
		close(s.done)
	}()
	return s, nil
}

// observedValues returns the subscriber's reported value set intersected
// with the outcome's submitted-value universe, sorted.
func (s *liveSubscriber) observedValues(universe map[string]struct{}) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	set := map[string]struct{}{}
	for leaf := range s.leaves {
		if _, ok := universe[leaf]; ok {
			set[leaf] = struct{}{}
		}
	}
	return sortedKeys(set)
}

func (s *liveSubscriber) snapshot() subObserved {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := s.obs
	cp.ValueLeaves = sortedKeys(s.leaves)
	truncated := len(cp.ValueLeaves) > 2000
	if truncated {
		cp.ValueLeaves = cp.ValueLeaves[:2000]
	}
	cp.LeavesCapped = s.obs.LeavesCapped || truncated
	return cp
}

func (s *liveSubscriber) shutdown() {
	s.stop()
	_ = s.lc.conn.Close(websocket.StatusNormalClosure, "")
	<-s.done
}

type killPoint int

const (
	killBeforeAck killPoint = iota
	killAfterAck
	killDuringPublication
)

// outcomeCrash drives pipelined writers + ≥2 subscribers, SIGKILLs instantd
// at the selected point, restarts, and verifies exact state against the DB
// oracle with subscriber-vs-oracle convergence.
func (d *recoveryDriver) outcomeCrash(ctx context.Context, id string, when killPoint) outcomeEvidence {
	o := outcomeEvidence{ID: id, Ran: true, Timings: map[string]string{}, SubValues: map[string][]string{}}
	stamp := func(k string) { o.Timings[k] = time.Now().UTC().Format(time.RFC3339Nano) }
	stamp("start")
	nSubs := 2
	if when == killDuringPublication {
		nSubs = 4
	}
	o.ExpectedSubs = nSubs
	o.Precondition = map[killPoint]string{
		killBeforeAck:         "SIGKILL with >=1 submitted tx still un-acked (ledger)",
		killAfterAck:          "SIGKILL after an ack no subscriber refresh has yet covered",
		killDuringPublication: "SIGKILL mid fan-out: >=1 refresh-ok received while >=1 subscriber lags",
	}[when]

	dbURL, err := d.outcomeDatabase(ctx, id)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	appID, attrID, err := d.seedFixture(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	proc, err := d.startInstantd(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	o.PIDBefore = proc.Process.Pid
	wsURL := "ws://" + d.addr + "/runtime/session"
	ledger := newTxLedger()
	universe := map[string]struct{}{}

	subs := make([]*liveSubscriber, 0, nSubs)
	for i := 0; i < nSubs; i++ {
		s, err := newLiveSubscriber(ctx, wsURL, appID, fmt.Sprintf("%s-q%d", id, i), i)
		if err != nil {
			o.HarnessError = fmt.Sprintf("subscribe %d: %v", i, err)
			shutdownAll(subs, nil)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		subs = append(subs, s)
	}
	o.ObservedSubs = nSubs

	writers := make([]*pipelinedWriter, 0, 2)
	for i := 0; i < 2; i++ {
		w, err := newPipelinedWriter(ctx, wsURL, appID, id, attrID, i, ledger)
		if err != nil {
			o.HarnessError = fmt.Sprintf("writer %d: %v", i, err)
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		writers = append(writers, w)
	}

	kill := func() {
		stamp("sigkill")
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
	}

	switch when {
	case killBeforeAck:
		// Two phases. First establish a committed base: a few txs (single
		// and multi-triple) awaited to ack, so the oracle must hold them
		// after the kill. Then the pipelined burst, SIGKILLing the moment
		// the ledger shows ≥1 submitted tx still un-acked — so the verdict
		// checks acked-present AND unacked-absent-or-atomic together.
		for i, w := range writers {
			eid := w.submit(ctx, 1, fmt.Sprintf("%s-base-single-%d", id, i))
			if _, ok := awaitLedgerAck(ledger, eid, 15*time.Second); !ok {
				o.HarnessError = "committed base never acked"
				shutdownAll(subs, writers)
				_ = proc.Process.Kill()
				_, _ = proc.Process.Wait()
				d.finalizeOutcome(&o, ledger, universe, subs)
				return o
			}
			eid = w.submit(ctx, 3, fmt.Sprintf("%s-base-multi-%d", id, i))
			if _, ok := awaitLedgerAck(ledger, eid, 15*time.Second); !ok {
				o.HarnessError = "committed multi-triple base never acked"
				shutdownAll(subs, writers)
				_ = proc.Process.Kill()
				_, _ = proc.Process.Wait()
				d.finalizeOutcome(&o, ledger, universe, subs)
				return o
			}
		}
		bursts := 0
		for {
			for i, w := range writers {
				for k := 0; k < 6; k++ {
					eid := w.submit(ctx, 1, fmt.Sprintf("%s-value-b%d-w%d-%d", id, bursts, i, k))
					_ = eid
				}
				for k := 0; k < 2; k++ {
					w.submit(ctx, 3, fmt.Sprintf("%s-multi-b%d-w%d-%d", id, bursts, i, k))
				}
			}
			sweepUniverse(universe, ledger)
			pending := ledger.pendingCount()
			o.PreconditionMet = pending >= 1
			if o.PreconditionMet {
				o.Precondition = fmt.Sprintf("SIGKILL with %d submitted tx still un-acked (ledger, after %d bursts)", pending, bursts+1)
				break
			}
			bursts++
			if bursts >= 5 || time.Since(mustTime(o.Timings["start"])) > crashPreconditionWait {
				o.Precondition = "un-acked tx never outstanding within bound"
				shutdownAll(subs, writers)
				_ = proc.Process.Kill()
				_, _ = proc.Process.Wait()
				d.finalizeOutcome(&o, ledger, universe, subs)
				return o
			}
			time.Sleep(50 * time.Millisecond)
		}
		kill()
	case killAfterAck:
		// Submit one tx at a time; SIGKILL the moment an ack exists that no
		// subscriber refresh has yet covered (watermark comparison). The
		// ack→refresh gap on loopback is milliseconds, so each attempt
		// busy-spins: the same iteration that first observes the ack reads
		// the subscriber watermarks, minimizing observation skew.
		var trigger string
		attempts := 0
		deadline := time.Now().Add(crashPreconditionWait)
		for time.Now().Before(deadline) && attempts < 100 {
			attempts++
			w := writers[attempts%len(writers)]
			eid := w.submit(ctx, 1, fmt.Sprintf("%s-trigger-%d", id, attempts))
			sweepUniverse(universe, ledger)
			srvTx, uncovered := spinAckUncovered(ledger, eid, subs, 5*time.Second)
			if srvTx == "" {
				continue // tx lost to timing; try the next one
			}
			if uncovered {
				trigger = eid
			}
			o.PreconditionMet = trigger != ""
			if o.PreconditionMet {
				o.Precondition = fmt.Sprintf("SIGKILL after ack %s (server tx %s) with no subscriber refresh covering it (%d attempts)", eid, srvTx, attempts)
				break
			}
		}
		if !o.PreconditionMet {
			o.Precondition = fmt.Sprintf("no ack-before-refresh window in %d attempts", attempts)
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		_ = trigger
		kill()
	case killDuringPublication:
		// Sustained burst across both writers; SIGKILL the moment fan-out
		// skew is observed: ≥1 refresh-ok received for the wave while ≥1
		// subscriber is still missing it.
		stopBurst := make(chan struct{})
		var bg sync.WaitGroup
		for i, w := range writers {
			bg.Add(1)
			go func(w *pipelinedWriter, i int) {
				defer bg.Done()
				n := 0
				for {
					select {
					case <-stopBurst:
						return
					default:
					}
					w.submit(ctx, 1, fmt.Sprintf("%s-wave-%d-%d", id, i, n))
					n++
					if n%25 == 24 {
						w.submit(ctx, 3, fmt.Sprintf("%s-wave-multi-%d-%d", id, i, n))
					}
					if n%20 == 0 {
						time.Sleep(5 * time.Millisecond)
					}
				}
			}(w, i)
		}
		deadline := time.Now().Add(crashPreconditionWait)
		for time.Now().Before(deadline) {
			counts := make([]int, len(subs))
			for i, s := range subs {
				s.mu.Lock()
				counts[i] = s.obs.RefreshOKs
				s.mu.Unlock()
			}
			mx, mn := counts[0], counts[0]
			for _, c := range counts[1:] {
				if c > mx {
					mx = c
				}
				if c < mn {
					mn = c
				}
			}
			skewed := mx >= 1 && mn < mx
			o.PreconditionMet = skewed
			if skewed {
				o.Precondition = fmt.Sprintf("SIGKILL mid fan-out: refresh-ok counts %v (max %d, min %d)", counts, mx, mn)
				break
			}
			time.Sleep(time.Millisecond)
		}
		close(stopBurst)
		bg.Wait()
		sweepUniverse(universe, ledger)
		if !o.PreconditionMet {
			o.Precondition = "no fan-out skew observed within bound"
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		kill()
	}

	for _, s := range subs {
		s.shutdown()
	}
	for _, w := range writers {
		w.shutdown()
	}
	o.MaxOutstanding = ledger.maxOutstandingCount()
	for _, s := range subs {
		if c := s.snapshot().CloseCode; c != "" {
			o.CloseCodes = append(o.CloseCodes, fmt.Sprintf("sub%d:%s", s.obs.Session, c))
		}
	}
	for i, w := range writers {
		if c := w.closeCode(); c != "" {
			o.CloseCodes = append(o.CloseCodes, fmt.Sprintf("writer%d:%s", i, c))
		}
	}
	sort.Strings(o.CloseCodes)
	time.Sleep(500 * time.Millisecond)

	// Restart and verify exact state.
	proc2, err := d.startInstantd(ctx, dbURL)
	if err != nil {
		o.HarnessError = "restart after SIGKILL: " + err.Error()
		o.Ledger = ledger.snapshot()
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	defer func() {
		_ = proc2.Process.Signal(syscall.SIGTERM)
		_, _ = proc2.Process.Wait()
	}()
	stamp("restarted")
	o.PIDAfter = proc2.Process.Pid
	attrs := map[string]struct{}{attrID: struct{}{}}
	oracle, err := d.dbOracle(dbURL, appID)
	if err != nil {
		o.HarnessError = "DB oracle: " + err.Error()
		o.Ledger = ledger.snapshot()
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	o.OracleValues = oracleValueSet(oracle, attrs)

	// Resubscribe and require every subscriber's final reported value set
	// to equal exactly the DB oracle's value set (convergence, not
	// self-comparison).
	fresh := make([]*liveSubscriber, 0, nSubs)
	for i := 0; i < nSubs; i++ {
		s, err := newLiveSubscriber(ctx, wsURL, appID, fmt.Sprintf("%s-rq%d", id, i), 100+i)
		if err != nil {
			o.HarnessError = fmt.Sprintf("reconnect after restart: %v", err)
			d.finalizeOutcome(&o, ledger, universe, fresh)
			return o
		}
		fresh = append(fresh, s)
	}
	defer shutdownSubs(fresh)
	stamp("resubscribed")
	deadline := time.Now().Add(convergeWait)
	for {
		diverged := []string{}
		for _, s := range fresh {
			obs := s.observedValues(universe)
			missing, extra := compareValueSets(obs, o.OracleValues)
			if len(missing) > 0 || len(extra) > 0 {
				diverged = append(diverged, fmt.Sprintf("sub%d missing=%v extra=%v", s.obs.Session, missing, extra))
			}
			o.SubValues[fmt.Sprintf("sub%d", s.obs.Session)] = obs
		}
		if len(diverged) == 0 {
			o.DivergedSubs = nil
			o.Converged = len(diverged) == 0 && len(fresh) == nSubs
			break
		}
		if time.Now().After(deadline) {
			o.DivergedSubs = diverged
			o.Converged = len(diverged) == 0
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	stamp("converged")
	checkLedgerDurability(&o, ledger, oracle)
	d.finalizeOutcome(&o, ledger, universe, fresh)
	return o
}

// snapSubscribers records one snapshot per subscriber session for the
// per-outcome artifact (add-query admission, refresh counts, watermarks,
// close codes).
func snapSubscribers(subs []*liveSubscriber) []subObserved {
	out := make([]subObserved, 0, len(subs))
	for _, sb := range subs {
		out = append(out, sb.snapshot())
	}
	return out
}

// checkLedgerDurability verifies every ledger-acked tx fully present in the
// oracle and counts partial (non-atomic) txs, using the FINAL ledger state.
// Call it after all client sessions have shut down so late acks and
// shutdown settling are included; an acked-but-absent tx observed only in a
// pre-shutdown snapshot would otherwise escape as "cleanly lost".
func checkLedgerDurability(o *outcomeEvidence, ledger *txLedger, oracle map[string]struct{}) {
	snap := ledger.snapshot()
	o.Ledger = snap
	for _, r := range snap {
		if r.Disposition != txAcked {
			continue
		}
		if txPresence(oracle, r.Triples) != len(r.Triples) {
			o.AckedMissing = append(o.AckedMissing, r.ClientEventID)
		}
	}
	partialSet := map[string]struct{}{}
	for _, r := range snap {
		if n := txPresence(oracle, r.Triples); n > 0 && n < len(r.Triples) {
			partialSet[r.ClientEventID] = struct{}{}
		}
	}
	o.PartialTxIDs = sortedKeys(partialSet)
}

// sweepUniverse adds every ledger triple value to the submitted-value
// universe used to interpret subscriber frames.
func sweepUniverse(universe map[string]struct{}, ledger *txLedger) {
	for _, r := range ledger.snapshot() {
		for _, t := range r.Triples {
			universe[t.Value] = struct{}{}
		}
	}
}

// firstRefreshObserved reports whether every subscriber has observed at
// least one refresh-ok (live delivery, not assumed).
func firstRefreshObserved(subs []*liveSubscriber) bool {
	for _, sb := range subs {
		sb.mu.Lock()
		n := sb.obs.RefreshOKs
		sb.mu.Unlock()
		if n < 1 {
			return false
		}
	}
	return true
}

// awaitFirstRefresh waits (bounded) until every subscriber has observed at
// least one refresh-ok, proving live delivery before SIGTERM. Timeout is a
// harness failure: sustained load with zero subscriber refreshes is not a
// drain worth measuring.
func awaitFirstRefresh(subs []*liveSubscriber, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if firstRefreshObserved(subs) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("subscribers never observed refresh-ok during load")
}

func shutdownSubs(subs []*liveSubscriber) {
	for _, s := range subs {
		s.shutdown()
	}
}

func shutdownAll(subs []*liveSubscriber, writers []*pipelinedWriter) {
	shutdownSubs(subs)
	for _, w := range writers {
		if w != nil {
			w.shutdown()
		}
	}
}

func mustTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Now()
	}
	return t
}

// outcomePostgresRestart restarts the DB container via --pg-restart-cmd while
// subscribers and pipelined writers are active, WITHOUT restarting instantd
// (same PID asserted before/after). RTO = DB-ready → first new acked write
// AND all subscribers converged.
func (d *recoveryDriver) outcomePostgresRestart(ctx context.Context) outcomeEvidence {
	o := outcomeEvidence{ID: "postgres-restart", Ran: true, Timings: map[string]string{}, SubValues: map[string][]string{}}
	stamp := func(k string) { o.Timings[k] = time.Now().UTC().Format(time.RFC3339Nano) }
	stamp("start")
	o.ExpectedSubs = 2
	o.Precondition = "writers+subscribers active across DB restart; instantd PID unchanged"

	dbURL, err := d.outcomeDatabase(ctx, "postgres-restart")
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	appID, attrID, err := d.seedFixture(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	proc, err := d.startInstantd(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	defer func() {
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
	}()
	o.PIDBefore, o.PIDAfter = proc.Process.Pid, proc.Process.Pid
	wsURL := "ws://" + d.addr + "/runtime/session"
	ledger := newTxLedger()
	universe := map[string]struct{}{}

	subs := make([]*liveSubscriber, 0, 2)
	for i := 0; i < 2; i++ {
		s, err := newLiveSubscriber(ctx, wsURL, appID, fmt.Sprintf("pg-q%d", i), i)
		if err != nil {
			o.HarnessError = fmt.Sprintf("subscribe %d: %v", i, err)
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		subs = append(subs, s)
	}
	o.ObservedSubs = 2
	writers := make([]*pipelinedWriter, 0, 2)
	for i := 0; i < 2; i++ {
		w, err := newPipelinedWriter(ctx, wsURL, appID, "pg", attrID, i, ledger)
		if err != nil {
			o.HarnessError = fmt.Sprintf("writer %d: %v", i, err)
			shutdownAll(subs, writers)
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		writers = append(writers, w)
	}
	// Pre-restart acked writes (blocking on the ledger, bounded).
	for i := 0; i < 5; i++ {
		eid := writers[i%2].submit(ctx, 1, fmt.Sprintf("pre-restart-%d", i))
		if _, ok := awaitLedgerAck(ledger, eid, 15*time.Second); !ok {
			o.HarnessError = fmt.Sprintf("pre-restart write %d never acked", i)
			shutdownAll(subs, writers)
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
	}
	sweepUniverse(universe, ledger)
	// Background pipelined writes continue across the restart.
	stopBG := make(chan struct{})
	var bg sync.WaitGroup
	bg.Add(1)
	go func() {
		defer bg.Done()
		n := 0
		for {
			select {
			case <-stopBG:
				return
			default:
			}
			writers[n%2].submit(ctx, 1, fmt.Sprintf("pg-during-w%d-%d", (n%2), n))
			n++
			time.Sleep(100 * time.Millisecond)
		}
	}()

	stamp("pg-restart-begin")
	restart := exec.CommandContext(ctx, "sh", "-c", d.pgRestart)
	if out, err := restart.CombinedOutput(); err != nil {
		close(stopBG)
		bg.Wait()
		o.HarnessError = fmt.Sprintf("pg-restart-cmd: %v\n%s", err, out)
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	// DB-ready: first successful ping after the restart command returned.
	dbBack := time.Time{}
	pollDeadline := time.Now().Add(restartWait)
	for time.Now().Before(pollDeadline) {
		if d.dbAlive(dbURL) {
			dbBack = time.Now()
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if dbBack.IsZero() {
		close(stopBG)
		bg.Wait()
		o.HarnessError = "owned DB never became ready after restart"
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	stamp("db-ready")
	// instantd must NOT have restarted: same PID and responsive.
	if err := proc.Process.Signal(syscall.Signal(0)); err != nil {
		close(stopBG)
		bg.Wait()
		o.HarnessError = "instantd died during postgres-restart: " + err.Error()
		o.PIDAfter = -1
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	if err := waitHealth("http://"+d.addr, 60*time.Second, true); err != nil {
		close(stopBG)
		bg.Wait()
		o.HarnessError = "instantd unhealthy after postgres-restart: " + err.Error()
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	// First new acked write after DB-ready (re-dial sessions broken by the
	// outage; every re-dial is observed, never assumed).
	firstAck := time.Time{}
	resumeDeadline := time.Now().Add(restartWait)
	for time.Now().Before(resumeDeadline) {
		eid := writers[0].submit(ctx, 1, fmt.Sprintf("pg-resume-%d", time.Now().UnixNano()))
		sweepUniverse(universe, ledger)
		if srvTx, ok := awaitLedgerAck(ledger, eid, 10*time.Second); ok && srvTx != "" {
			firstAck = time.Now()
			break
		}
		// Re-dial broken sessions: outage may have killed the sockets.
		for i, w := range writers {
			nw, err := newPipelinedWriter(ctx, wsURL, appID, "pg", attrID, 10+i, ledger)
			if err == nil {
				w.shutdown()
				writers[i] = nw
			}
		}
		time.Sleep(time.Second)
	}
	close(stopBG)
	bg.Wait()
	// Final sweep: background submits between the last resume-iteration
	// sweep and the stop land here; the universe must cover every submitted
	// value or subscriber observations would be misread as divergent.
	sweepUniverse(universe, ledger)
	if firstAck.IsZero() {
		o.HarnessError = "writes did not resume after DB restart"
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	stamp("first-ack")
	attrs := map[string]struct{}{attrID: struct{}{}}
	oracle, err := d.dbOracle(dbURL, appID)
	if err != nil {
		o.HarnessError = "DB oracle: " + err.Error()
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	o.OracleValues = oracleValueSet(oracle, attrs)
	o.MaxOutstanding = ledger.maxOutstandingCount()

	// Reconnect/resubscribe and converge to the oracle (old sockets may be
	// dead after the outage; fresh sessions prove serving state).
	shutdownAll(subs, writers)
	fresh := make([]*liveSubscriber, 0, 2)
	for i := 0; i < 2; i++ {
		s, err := newLiveSubscriber(ctx, wsURL, appID, fmt.Sprintf("pg-rq%d", i), 100+i)
		if err != nil {
			o.HarnessError = fmt.Sprintf("resubscribe after DB restart: %v", err)
			d.finalizeOutcome(&o, ledger, universe, fresh)
			return o
		}
		fresh = append(fresh, s)
	}
	defer shutdownSubs(fresh)
	convergedAt := time.Time{}
	cDeadline := time.Now().Add(convergeWait)
	for {
		diverged := []string{}
		for _, s := range fresh {
			obs := s.observedValues(universe)
			missing, extra := compareValueSets(obs, o.OracleValues)
			if len(missing) > 0 || len(extra) > 0 {
				diverged = append(diverged, fmt.Sprintf("sub%d missing=%v extra=%v", s.obs.Session, missing, extra))
			}
			o.SubValues[fmt.Sprintf("sub%d", s.obs.Session)] = obs
		}
		if len(diverged) == 0 {
			convergedAt = time.Now()
			o.DivergedSubs = nil
			o.Converged = len(fresh) == 2
			break
		}
		if time.Now().After(cDeadline) {
			o.DivergedSubs = diverged
			o.Converged = false
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	stamp("converged")
	if !convergedAt.IsZero() {
		latter := firstAck
		if convergedAt.After(latter) {
			latter = convergedAt
		}
		o.RTO = latter.Sub(dbBack)
	}
	o.PreconditionMet = !dbBack.IsZero() && !firstAck.IsZero() && o.PIDBefore == o.PIDAfter
	if o.PreconditionMet {
		o.Precondition = fmt.Sprintf("restart observed: db-ready %s, first-ack %s, pid %d unchanged",
			dbBack.Format(time.RFC3339Nano), firstAck.Format(time.RFC3339Nano), o.PIDBefore)
	} else {
		o.Precondition = "precondition not reached: resume or pid assertion failed"
	}
	checkLedgerDurability(&o, ledger, oracle)
	d.finalizeOutcome(&o, ledger, universe, fresh)
	return o
}

// outcomeDrain delivers SIGTERM at the declared load and checks the 30s,
// port-release, durability, and in-flight accounting contract. Writers are
// pipelined across multiple sessions; saturated submits faster than acks
// return (ledger max-outstanding ≥ 8 at SIGTERM, else precondition FAIL).
func (d *recoveryDriver) outcomeDrain(ctx context.Context, id string, rate float64) outcomeEvidence {
	o := outcomeEvidence{ID: id, Ran: true, Timings: map[string]string{}, SubValues: map[string][]string{}}
	stamp := func(k string) { o.Timings[k] = time.Now().UTC().Format(time.RFC3339Nano) }
	stamp("start")
	o.ExpectedSubs = 2
	o.Precondition = map[string]string{
		"drain-idle":      "SIGTERM at 0 load with a serving probe acked",
		"drain-moderate":  "SIGTERM at pipelined moderate load after settle",
		"drain-saturated": "SIGTERM with ledger max-outstanding >= 8 (submitting faster than acks)",
	}[id]

	dbURL, err := d.outcomeDatabase(ctx, id)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	appID, attrID, err := d.seedFixture(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	proc, err := d.startInstantd(ctx, dbURL)
	if err != nil {
		o.HarnessError = err.Error()
		d.finalizeOutcome(&o, nil, nil, nil)
		return o
	}
	o.PIDBefore, o.PIDAfter = proc.Process.Pid, proc.Process.Pid
	wsURL := "ws://" + d.addr + "/runtime/session"
	ledger := newTxLedger()
	universe := map[string]struct{}{}

	nWriters := 1
	if rate > 0 {
		nWriters = 2
	}
	if id == "drain-saturated" {
		nWriters = 4
	}
	subs := make([]*liveSubscriber, 0, 2)
	for i := 0; i < 2; i++ {
		s, err := newLiveSubscriber(ctx, wsURL, appID, fmt.Sprintf("%s-q%d", id, i), i)
		if err != nil {
			o.HarnessError = fmt.Sprintf("subscribe %d: %v", i, err)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		subs = append(subs, s)
	}
	o.ObservedSubs = 2
	writers := make([]*pipelinedWriter, 0, nWriters)
	for i := 0; i < nWriters; i++ {
		w, err := newPipelinedWriter(ctx, wsURL, appID, id, attrID, i, ledger)
		if err != nil {
			o.HarnessError = fmt.Sprintf("writer %d: %v", i, err)
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
		writers = append(writers, w)
	}

	// Generate the declared load level with the non-blocking submit path.
	stopLoad := make(chan struct{})
	var loadWG sync.WaitGroup
	if rate == 0 {
		// Idle: one serving probe proves the drain target was live.
		eid := writers[0].submit(ctx, 1, id+"-probe")
		_, acked := awaitLedgerAck(ledger, eid, 15*time.Second)
		o.PreconditionMet = acked
		if !acked {
			o.HarnessError = "idle probe transact never acked"
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
	} else if id == "drain-moderate" {
		interval := time.Duration(float64(time.Second) / (rate / float64(nWriters)))
		for _, w := range writers {
			loadWG.Add(1)
			go func(w *pipelinedWriter) {
				defer loadWG.Done()
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				n := 0
				for {
					select {
					case <-stopLoad:
						return
					case <-ticker.C:
						w.submit(ctx, 1, fmt.Sprintf("%s-load-w%d-%d", id, w.session, n))
						n++
					}
				}
			}(w)
		}
		time.Sleep(drainSettleWindow)
		o.PreconditionMet = len(ledger.ackedRecords()) > 0
		if !o.PreconditionMet {
			o.Precondition = "moderate load produced no acked tx during settle"
		}
	} else {
		// Saturated: unthrottled submit loops on 4 sessions until the ledger
		// demonstrates submitting faster than acks return, sustained for a
		// minimum flood window so subscribers observe live delivery before
		// SIGTERM (an instant kill would leave convergence vacuous).
		for _, w := range writers {
			loadWG.Add(1)
			go func(w *pipelinedWriter) {
				defer loadWG.Done()
				n := 0
				for {
					select {
					case <-stopLoad:
						return
					default:
					}
					mix := 1
					if n%7 == 6 {
						mix = 3 // multi-triple shape inside the flood
					}
					w.submit(ctx, mix, fmt.Sprintf("%s-sat-w%d-%d", id, w.session, n))
					n++
				}
			}(w)
		}
		floodStart := time.Now()
		satDeadline := floodStart.Add(60 * time.Second)
		for time.Now().Before(satDeadline) {
			// Bounded flood: stop as soon as the ledger demonstrates
			// submit-faster-than-ack (max-outstanding) with live delivery
			// to both subscribers. Unthrottled loopback submits tens of
			// thousands per second; without this bound the evidence file
			// would carry a hundred thousand ledger entries.
			if ledger.count() >= 200 && ledger.maxOutstandingCount() >= saturatedMinInFlight && firstRefreshObserved(subs) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		o.MaxOutstanding = ledger.maxOutstandingCount()
		o.PreconditionMet = o.MaxOutstanding >= saturatedMinInFlight
		if !o.PreconditionMet {
			o.Precondition = fmt.Sprintf("saturated precondition not reached: max-outstanding %d < %d", o.MaxOutstanding, saturatedMinInFlight)
		} else {
			o.Precondition = fmt.Sprintf("SIGTERM with ledger max-outstanding %d (>= %d) over %d submitted with live delivery", o.MaxOutstanding, saturatedMinInFlight, ledger.count())
		}
	}
	sweepUniverse(universe, ledger)
	if o.HarnessError == "" && !o.PreconditionMet && id != "drain-saturated" {
		// Moderate with no acks is already marked above; idle always met.
		if id == "drain-moderate" {
			close(stopLoad)
			loadWG.Wait()
			shutdownAll(subs, writers)
			_ = proc.Process.Kill()
			_, _ = proc.Process.Wait()
			d.finalizeOutcome(&o, ledger, universe, subs)
			return o
		}
	}
	if id == "drain-saturated" && !o.PreconditionMet {
		close(stopLoad)
		loadWG.Wait()
		shutdownAll(subs, writers)
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	// Live-delivery gate: every subscriber must have observed at least one
	// refresh-ok before SIGTERM, else the drain convergence check below
	// would be vacuous (idle: the probe's refresh; loaded: flood refreshes).
	// Bounded; timeout fails the outcome instead of passing a silent drain.
	if err := awaitFirstRefresh(subs, 15*time.Second); err != nil {
		o.HarnessError = err.Error()
		close(stopLoad)
		loadWG.Wait()
		shutdownAll(subs, writers)
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}

	// SIGTERM (graceful-stop signal), never SIGINT.
	stamp("sigterm")
	t0 := time.Now()
	if err := proc.Process.Signal(syscall.SIGTERM); err != nil {
		o.HarnessError = "signal drain: " + err.Error()
		close(stopLoad)
		loadWG.Wait()
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	close(stopLoad)
	loadWG.Wait()
	waitCh := make(chan error, 1)
	go func() {
		_, err := proc.Process.Wait()
		waitCh <- err
	}()
	select {
	case werr := <-waitCh:
		if werr != nil {
			o.ExitStatus = "wait: " + werr.Error()
		} else {
			o.ExitStatus = "exit 0"
		}
		if st := proc.ProcessState; st != nil {
			o.ExitStatus = fmt.Sprintf("exit=%d %s", st.ExitCode(), st.String())
		}
	case <-time.After(drainExitBudget + 5*time.Second):
		_ = proc.Process.Kill()
		_, _ = proc.Process.Wait()
		o.HarnessError = "process did not exit"
	}
	o.ExitRecorded = o.HarnessError == ""
	if o.HarnessError != "" {
		shutdownAll(subs, writers)
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	o.ExitSeconds = ceilSeconds(time.Since(t0))
	stamp("exited")

	// Drain the client pumps so close codes and final refresh sets settle.
	time.Sleep(time.Second)
	sweepUniverse(universe, ledger)
	o.Ledger = ledger.snapshot()
	o.MaxOutstanding = ledger.maxOutstandingCount()
	for _, s := range subs {
		snap := s.snapshot()
		if snap.CloseCode != "" {
			o.CloseCodes = append(o.CloseCodes, fmt.Sprintf("sub%d:%s", snap.Session, snap.CloseCode))
		}
	}
	for i, w := range writers {
		if c := w.closeCode(); c != "" {
			o.CloseCodes = append(o.CloseCodes, fmt.Sprintf("writer%d:%s", i, c))
		}
	}
	// Pumps that never observed a close (clean local shutdown after the
	// server went away) contribute their socket outcome, not a guess: only
	// observed codes are recorded.
	sort.Strings(o.CloseCodes)
	shutdownAll(subs, writers)

	// Port must be released.
	conn, err := net.DialTimeout("tcp", d.addr, 2*time.Second)
	o.PortReleased = err != nil
	if err == nil {
		_ = conn.Close()
		o.HarnessError = "listen port still bound after exit"
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}

	attrs := map[string]struct{}{attrID: struct{}{}}
	oracle, err := d.dbOracle(dbURL, appID)
	if err != nil {
		o.HarnessError = "post-drain oracle: " + err.Error()
		d.finalizeOutcome(&o, ledger, universe, subs)
		return o
	}
	o.OracleValues = oracleValueSet(oracle, attrs)
	// Durability (acked-present, partial) runs on the final snapshot below;
	// in-flight accounting runs on that same final state.
	finalSnap := ledger.snapshot()
	o.Ledger = finalSnap
	checkLedgerDurability(&o, ledger, oracle)
	// In-flight accounting: every tx submitted before exit resolves to
	// acked+present, explicit error/socket-close, or never-acked which must
	// then be absent-or-atomic. Anything else is unresolved.
	unresolvedSet := map[string]struct{}{}
	for _, r := range finalSnap {
		switch r.Disposition {
		case txAcked:
			continue // durability checked above
		case txError:
			continue // atomicity covered by checkLedgerDurability above
		default:
			n := txPresence(oracle, r.Triples)
			if n > 0 && n < len(r.Triples) {
				// Partial: also in o.PartialTxIDs via checkLedgerDurability.
				unresolvedSet[r.ClientEventID] = struct{}{}
			} else if n == len(r.Triples) && len(r.Triples) > 0 {
				// Never-acked but fully present: crash-consistent (the
				// commit landed, the ack was lost to SIGTERM), resolved.
				continue
			} else if n == 0 {
				// Never-acked and absent: cleanly lost, resolved.
				continue
			} else {
				unresolvedSet[r.ClientEventID] = struct{}{}
			}
		}
	}
	o.UnresolvedTxIDs = sortedKeys(unresolvedSet)
	o.InFlightResolved = len(o.UnresolvedTxIDs) == 0 && len(o.AckedMissing) == 0
	// Drain convergence: every admitted subscriber observed ≥1 refresh-ok
	// before exit with no error frames, and no subscriber reported a value
	// outside the post-exit oracle (lag is expected; phantoms are not).
	diverged := []string{}
	for _, s := range subs {
		snap := s.snapshot()
		obs := s.observedValues(universe)
		o.SubValues[fmt.Sprintf("sub%d", snap.Session)] = obs
		_, extra := compareValueSets(obs, o.OracleValues)
		if len(snap.ErrorFrames) > 0 {
			diverged = append(diverged, fmt.Sprintf("sub%d error-frames=%v", snap.Session, snap.ErrorFrames))
		} else if snap.RefreshOKs == 0 {
			diverged = append(diverged, fmt.Sprintf("sub%d never observed refresh-ok", snap.Session))
		} else if len(extra) > 0 {
			diverged = append(diverged, fmt.Sprintf("sub%d phantom values=%v", snap.Session, extra))
		}
	}
	o.DivergedSubs = diverged
	o.Converged = len(diverged) == 0 && o.ObservedSubs == 2
	stamp("verified")
	d.finalizeOutcome(&o, ledger, universe, subs)
	return o
}
