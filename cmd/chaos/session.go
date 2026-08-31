package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	"sync"
	"time"
)

// sessionStats is one WS client's observed lifecycle.
type sessionStats struct {
	mu          sync.Mutex
	ID          int
	Frames      int
	Refreshes   int
	Dropped     bool
	DropErr     string
	Reconnected bool
	FinalCount  int
	Verdict     string
	finalIDs    map[string]bool
	rawResult   string
}

func (s *sessionStats) dropped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Dropped
}

func sendFrame(conn *websocket.Conn, f frame) error {
	b, err := json.Marshal(map[string]json.RawMessage(f))
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, b)
}

// connectSession dials, inits, add-queries, consumes the initial refresh-ok
// (recording entity ids into the returned stats), then leaves a reader
// goroutine counting frames until the connection drops.
func connectSession(n int, wsURL, appID string) (*sessionStats, error) {
	dctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	conn, _, err := websocket.Dial(dctx, wsURL, nil)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	defer func() {
		if err != nil {
			_ = conn.CloseNow()
		}
	}()
	conn.SetReadLimit(64 << 20)

	err = sendFrame(conn, frame{
		"op":              json.RawMessage(`"init"`),
		"app-id":          json.RawMessage(mustJSON(appID)),
		"versions":        json.RawMessage(`{"@instantdb/core":"0.22.75"}`),
		"client-event-id": json.RawMessage(`"chaos-init"`),
	})
	if err != nil {
		return nil, fmt.Errorf("send init: %w", err)
	}
	var f frame
	f, err = readFrame(conn, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("read init reply: %w", err)
	}
	if f.op() != "init-ok" {
		return nil, fmt.Errorf("want init-ok, got %q (%v)", f.op(), f)
	}
	err = sendFrame(conn, frame{
		"op":              json.RawMessage(`"add-query"`),
		"q":               json.RawMessage(`{"chaos-items":{}}`),
		"client-event-id": json.RawMessage(`"chaos-q1"`),
	})
	if err != nil {
		return nil, fmt.Errorf("send add-query: %w", err)
	}

	stats := &sessionStats{ID: n, finalIDs: map[string]bool{}}
	gotInitial := false
	deadline := time.Now().Add(15 * time.Second)
	for !gotInitial {
		if time.Now().After(deadline) {
			return stats, errors.New("no initial refresh-ok within deadline")
		}
		f, err = readFrame(conn, 15*time.Second)
		if err != nil {
			return stats, fmt.Errorf("await initial refresh-ok: %w", err)
		}
		stats.Frames++
		switch f.op() {
		case "add-query-ok":
			// v1 rides the initial answer on the ack itself
			// (session.clj:264-270); accept either shape.
			if _, has := f["result"]; !has {
				continue
			}
			ids, perr := extractEntitiesFromResult(f["result"])
			if perr != nil {
				return stats, fmt.Errorf("parse add-query-ok result: %w", perr)
			}
			for id := range ids {
				stats.finalIDs[id] = true
			}
			stats.FinalCount = len(ids)
			gotInitial = true
		case "refresh-ok":
			stats.mu.Lock()
			stats.rawResult = string(f["computations"])
			stats.mu.Unlock()
			ids, perr := extractEntities(f)
			if perr != nil {
				return stats, fmt.Errorf("parse refresh-ok: %w", perr)
			}
			for id := range ids {
				stats.finalIDs[id] = true
			}
			stats.Refreshes++
			stats.FinalCount = len(ids)
			gotInitial = true
		default:
			// other frames counted, ignored
		}
	}

	go watchConn(conn, stats)
	return stats, nil
}

// watchConn counts frames and records the drop when the socket dies.
func watchConn(conn *websocket.Conn, stats *sessionStats) {
	for {
		f, err := readFrame(conn, 60*time.Second)
		stats.mu.Lock()
		if err != nil {
			stats.Dropped = true
			stats.DropErr = err.Error()
			stats.mu.Unlock()
			return
		}
		stats.Frames++
		if f.op() == "refresh-ok" {
			stats.Refreshes++
		}
		stats.mu.Unlock()
	}
}

func (s *sessionStats) frameCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Frames
}

func (s *sessionStats) dropErr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.DropErr
}

func readFrame(conn *websocket.Conn, timeout time.Duration) (frame, error) {
	rctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, data, err := conn.Read(rctx)
	if err != nil {
		return nil, err
	}
	var f frame
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, err
	}
	return f, nil
}
