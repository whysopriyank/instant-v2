package main

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeTransactResponsePropagatesBodyReadFailure(t *testing.T) {
	_, status, body, err := decodeTransactResponse(200, []byte(`{"tx-id":7}`), errors.New("body read failed"))
	if err == nil || !strings.Contains(err.Error(), "read transact response") {
		t.Fatalf("body read failure swallowed: %v", err)
	}
	if status != 200 || body != `{"tx-id":7}` {
		t.Fatalf("response diagnostics changed: status=%d body=%q", status, body)
	}
}

func TestDecodeTransactResponsePropagatesMalformedJSON(t *testing.T) {
	_, _, _, err := decodeTransactResponse(200, []byte(`{"tx-id":`), nil)
	if err == nil || !strings.Contains(err.Error(), "decode transact response") {
		t.Fatalf("malformed JSON accepted: %v", err)
	}
}

func TestDecodeTransactResponseRejectsZeroTxID(t *testing.T) {
	for _, raw := range []string{`{"tx-id":0}`, `{"tx-id":-1}`, `{}`} {
		if _, _, _, err := decodeTransactResponse(200, []byte(raw), nil); err == nil || !strings.Contains(err.Error(), "positive tx-id") {
			t.Fatalf("invalid tx-id accepted: %s (%v)", raw, err)
		}
	}
}

func TestDecodeTransactResponseAcceptsPositiveTxID(t *testing.T) {
	txID, status, body, err := decodeTransactResponse(200, []byte(`{"tx-id":42}`), nil)
	if err != nil || txID != 42 || status != 200 || body != `{"tx-id":42}` {
		t.Fatalf("valid response rejected: tx=%d status=%d body=%q err=%v", txID, status, body, err)
	}
}

func TestWriterLoopPropagatesTransportFailure(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tx-id":1}`))
			return
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("server does not support hijacking")
		}
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	}))
	defer srv.Close()

	_, _, err := writerLoop(srv.URL, "app", "attr", 3)
	if err == nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("transport failure after initial write was not propagated: %v", err)
	}
}

func TestWriterLoopPropagatesDecodeFailure(t *testing.T) {
	attempt := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt++
		if attempt == 1 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"tx-id":1}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tx-id": invalid`))
	}))
	defer srv.Close()

	_, _, err := writerLoop(srv.URL, "app", "attr", 3)
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("decode failure after initial write was not propagated: %v", err)
	}
}
