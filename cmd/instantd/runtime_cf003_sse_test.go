package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func TestCF003AssembledSSERefreshAndReconnect(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	connect := func() (*http.Response, *bufio.Scanner, string, string) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/runtime/sse?app_id="+app, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
			_ = resp.Body.Close()
			t.Fatalf("SSE connect = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
		}
		scanner := bufio.NewScanner(resp.Body)
		hello := cf003ReadSSE(t, scanner)
		sessionID, _ := hello["session-id"].(string)
		token, _ := hello["sse-token"].(string)
		machineID, _ := hello["machine-id"].(string)
		if len(hello) != 4 || hello["op"] != "init-ok" || machineID == "" || sessionID == "" || token == "" {
			_ = resp.Body.Close()
			t.Fatalf("SSE handshake = %#v", hello)
		}
		return resp, scanner, sessionID, token
	}

	postRaw := func(sessionID, token string, messages ...map[string]any) (int, string, string) {
		t.Helper()
		body := cf003JSON(t, map[string]any{
			"machine_id": "cf003-sse", "app_id": app,
			"session_id": sessionID, "sse_token": token,
			"messages": messages,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/runtime/sse", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		payload, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		return resp.StatusCode, resp.Header.Get("Content-Type"), string(payload)
	}
	post := func(sessionID, token string, messages ...map[string]any) {
		t.Helper()
		status, contentType, body := postRaw(sessionID, token, messages...)
		if status != http.StatusOK || contentType != "application/json" || body != "{}\n" {
			t.Fatalf("SSE POST = %d %q %q", status, contentType, body)
		}
	}
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "sse-initial"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("SSE seed transact = %d %q", status, body)
	}

	resp, scanner, sessionID, token := connect()
	post(sessionID, token, map[string]any{"op": "init", "app-id": app})
	idAttr, titleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, scanner), app)
	post(sessionID, token, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "cf003-add-query",
	})
	cf003AssertSSEAddQuery(t, cf003ReadSSE(t, scanner), "cf003-add-query")
	cf003AssertSSETodos(t, cf003ReadSSE(t, scanner), "sse-initial", true)

	status, body, _ = cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": "sse-refresh"}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("SSE trigger transact = %d %q", status, body)
	}
	cf003AssertSSERefreshTodo(t, cf003ReadSSE(t, scanner), idAttr, titleAttr, "sse-refresh")
	_ = resp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		oldStatus, oldType, oldBody := postRaw(sessionID, token)
		if oldStatus == http.StatusUnauthorized {
			if !strings.HasPrefix(oldType, "text/plain;") || oldBody != "unknown sse session\n" {
				t.Fatalf("old SSE rejection = %d %q %q", oldStatus, oldType, oldBody)
			}
			break
		}
		if oldStatus != http.StatusOK || time.Now().After(deadline) {
			t.Fatalf("old SSE session remained live: %d %q %q", oldStatus, oldType, oldBody)
		}
		time.Sleep(10 * time.Millisecond)
	}

	resp2, scanner2, sessionID2, token2 := connect()
	defer func() { _ = resp2.Body.Close() }()
	if sessionID2 == sessionID || token2 == token {
		t.Fatalf("reconnect reused credentials: session=%q token=%q", sessionID2, token2)
	}
	post(sessionID2, token2, map[string]any{"op": "init", "app-id": app})
	reconnectedIDAttr, reconnectedTitleAttr := cf003AssertSSEInit(t, cf003ReadSSE(t, scanner2), app)
	if reconnectedIDAttr != idAttr || reconnectedTitleAttr != titleAttr {
		t.Fatalf("reconnect attr identity drift: id=%q/%q title=%q/%q", idAttr, reconnectedIDAttr, titleAttr, reconnectedTitleAttr)
	}
	post(sessionID2, token2, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
		"client-event-id": "cf003-reconnect-query",
	})
	cf003AssertSSEAddQuery(t, cf003ReadSSE(t, scanner2), "cf003-reconnect-query")
	cf003AssertSSETodos(t, cf003ReadSSE(t, scanner2), "sse-refresh", false)
}

func TestCF003AssembledSSEMultiClientTeardown(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	cf003TransactTitle(t, mux, app, adminToken, "multi-initial")
	a := cf003OpenSSE(t, ctx, server, app)
	b := cf003OpenSSE(t, ctx, server, app)
	if a.sessionID == b.sessionID || a.token == b.token {
		t.Fatalf("SSE clients reused credentials: sessions=%q/%q tokens=%q/%q", a.sessionID, b.sessionID, a.token, b.token)
	}

	clients := []*cf003SSEClient{a, b}
	attrs := make([][2]string, len(clients))
	for i, client := range clients {
		client.post(t, ctx, map[string]any{"op": "init", "app-id": app})
		attrs[i][0], attrs[i][1] = cf003AssertSSEInit(t, cf003ReadSSE(t, client.scanner), app)
		eventID := "cf003-multi-query-" + string(rune('a'+i))
		client.post(t, ctx, map[string]any{
			"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
			"client-event-id": eventID,
		})
		cf003AssertSSEAddQuery(t, cf003ReadSSE(t, client.scanner), eventID)
		cf003AssertSSETodos(t, cf003ReadSSE(t, client.scanner), "multi-initial", true)
	}

	firstTx := cf003TransactTitle(t, mux, app, adminToken, "multi-first")
	firstA := cf003ReadSSE(t, a.scanner)
	firstB := cf003ReadSSE(t, b.scanner)
	cf003AssertSSERefreshTodo(t, firstA, attrs[0][0], attrs[0][1], "multi-first", firstTx)
	cf003AssertSSERefreshTodo(t, firstB, attrs[1][0], attrs[1][1], "multi-first", firstTx)
	if firstA["processed-tx-id"] != firstB["processed-tx-id"] {
		t.Fatalf("SSE clients diverged at first refresh: %#v / %#v", firstA["processed-tx-id"], firstB["processed-tx-id"])
	}

	a.closeAndAwaitUnauthorized(t, ctx)
	secondTx := cf003TransactTitle(t, mux, app, adminToken, "multi-second")
	secondB := cf003ReadSSE(t, b.scanner)
	cf003AssertSSERefreshTodo(t, secondB, attrs[1][0], attrs[1][1], "multi-second", secondTx)
	b.closeAndAwaitUnauthorized(t, ctx)
}

type cf003SSEClient struct {
	resp      *http.Response
	scanner   *bufio.Scanner
	server    *httptest.Server
	app       string
	sessionID string
	token     string
}

func cf003OpenSSE(t *testing.T, ctx context.Context, server *httptest.Server, app string) *cf003SSEClient {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/runtime/sse?app_id="+app, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		_ = resp.Body.Close()
		t.Fatalf("SSE connect = %d %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	client := &cf003SSEClient{resp: resp, scanner: bufio.NewScanner(resp.Body), server: server, app: app}
	hello := cf003ReadSSE(t, client.scanner)
	client.sessionID, _ = hello["session-id"].(string)
	client.token, _ = hello["sse-token"].(string)
	machineID, _ := hello["machine-id"].(string)
	if len(hello) != 4 || hello["op"] != "init-ok" || machineID == "" || client.sessionID == "" || client.token == "" {
		_ = resp.Body.Close()
		t.Fatalf("SSE handshake = %#v", hello)
	}
	return client
}

func (c *cf003SSEClient) postRaw(t *testing.T, ctx context.Context, messages ...map[string]any) (int, string, string) {
	t.Helper()
	body := cf003JSON(t, map[string]any{
		"machine_id": "cf003-sse-multi", "app_id": c.app,
		"session_id": c.sessionID, "sse_token": c.token,
		"messages": messages,
	})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.server.URL+"/runtime/sse", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	return resp.StatusCode, resp.Header.Get("Content-Type"), string(payload)
}

func (c *cf003SSEClient) post(t *testing.T, ctx context.Context, messages ...map[string]any) {
	t.Helper()
	status, contentType, body := c.postRaw(t, ctx, messages...)
	if status != http.StatusOK || contentType != "application/json" || body != "{}\n" {
		t.Fatalf("SSE POST = %d %q %q", status, contentType, body)
	}
}

func (c *cf003SSEClient) closeAndAwaitUnauthorized(t *testing.T, ctx context.Context) {
	t.Helper()
	_ = c.resp.Body.Close()
	deadline := time.Now().Add(2 * time.Second)
	for {
		status, contentType, body := c.postRaw(t, ctx)
		if status == http.StatusUnauthorized {
			if !strings.HasPrefix(contentType, "text/plain;") || body != "unknown sse session\n" {
				t.Fatalf("closed SSE rejection = %d %q %q", status, contentType, body)
			}
			return
		}
		if status != http.StatusOK || time.Now().After(deadline) {
			t.Fatalf("closed SSE session remained live: %d %q %q", status, contentType, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func cf003TransactTitle(t *testing.T, mux *http.ServeMux, app, adminToken, title string) int64 {
	t.Helper()
	status, body, contentHeaders := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app,
		"steps":  []any{[]any{"update", "todos", cf003EntityID, map[string]any{"title": title}}},
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK || contentHeaders.Get("Content-Type") != "application/json" {
		t.Fatalf("SSE transact %q = %d %q %q", title, status, contentHeaders.Get("Content-Type"), body)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &envelope); err != nil || len(envelope) != 1 {
		t.Fatalf("SSE transact %q envelope = %q: %v", title, body, err)
	}
	var txID int64
	if raw, ok := envelope["tx-id"]; !ok || json.Unmarshal(raw, &txID) != nil || txID <= 0 {
		t.Fatalf("SSE transact %q tx-id = %#v", title, envelope["tx-id"])
	}
	return txID
}

func cf003ReadSSE(t *testing.T, scanner *bufio.Scanner) map[string]any {
	t.Helper()
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var frame map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &frame); err != nil {
			t.Fatalf("decode SSE frame: %v", err)
		}
		return frame
	}
	t.Fatalf("SSE stream ended: %v", scanner.Err())
	return nil
}

func cf003AssertSSEInit(t *testing.T, frame map[string]any, appID string) (string, string) {
	t.Helper()
	if len(frame) != 5 || frame["op"] != "init-ok" {
		t.Fatalf("SSE protocol init shape = %#v", frame)
	}
	if sessionID, _ := frame["session-id"].(string); sessionID == "" {
		t.Fatalf("SSE protocol session id = %#v", frame["session-id"])
	}
	wantStatus := map[string]any{"status": "active"}
	wantAuth := map[string]any{"admin?": false, "app": map[string]any{"id": appID}, "user": nil}
	if !reflect.DeepEqual(frame["app-status"], wantStatus) || !reflect.DeepEqual(frame["auth"], wantAuth) {
		t.Fatalf("SSE protocol init metadata = %#v", frame)
	}
	attrs, ok := frame["attrs"].([]any)
	if !ok || len(attrs) != 2 {
		t.Fatalf("SSE attrs = %#v", frame["attrs"])
	}
	var idAttr, titleAttr string
	for _, raw := range attrs {
		attr, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("SSE attr = %#v", raw)
		}
		identity, ok := attr["forward-identity"].([]any)
		if !ok || len(identity) != 3 || identity[0] != appID || identity[1] != "todos" {
			t.Fatalf("SSE attr identity = %#v", attr)
		}
		attrID, _ := attr["id"].(string)
		if attrID == "" {
			t.Fatalf("SSE attr id = %#v", attr)
		}
		base := map[string]any{
			"cardinality": "one", "checked-data-type": nil, "forward-identity": identity,
			"id": attrID, "index?": false, "required?": false, "unique?": false, "value-type": "blob",
		}
		switch identity[2] {
		case "id":
			base["index?"] = true
			base["primary?"] = true
			base["required?"] = true
			base["unique?"] = true
			idAttr = attrID
		case "title":
			titleAttr = attrID
		default:
			t.Fatalf("unexpected SSE attr = %#v", attr)
		}
		if !reflect.DeepEqual(attr, base) {
			t.Fatalf("SSE attr shape = %#v; want %#v", attr, base)
		}
	}
	if idAttr == "" || titleAttr == "" {
		t.Fatalf("SSE attrs missing id/title: %#v", attrs)
	}
	return idAttr, titleAttr
}

func cf003AssertSSEAddQuery(t *testing.T, frame map[string]any, eventID string) {
	t.Helper()
	want := map[string]any{"client-event-id": eventID, "op": "add-query-ok"}
	if !reflect.DeepEqual(frame, want) {
		t.Fatalf("SSE add-query = %#v; want %#v", frame, want)
	}
}

func cf003AssertSSETodos(t *testing.T, frame map[string]any, title string, requireZeroTx bool) {
	t.Helper()
	txID, ok := frame["processed-tx-id"].(float64)
	if !ok || txID < 0 || (requireZeroTx && txID != 0) {
		t.Fatalf("SSE processed tx id = %#v", frame["processed-tx-id"])
	}
	want := map[string]any{
		"op": "refresh-ok", "processed-tx-id": txID,
		"computations": []any{map[string]any{
			"instaql-query":  map[string]any{"todos": map[string]any{}},
			"instaql-result": map[string]any{"todos": []any{map[string]any{"id": cf003EntityID, "title": title}}},
		}},
	}
	if !reflect.DeepEqual(frame, want) {
		t.Fatalf("SSE tree refresh = %#v; want %#v", frame, want)
	}
}

func cf003AssertSSERefreshTodo(t *testing.T, frame map[string]any, idAttr, titleAttr, title string, wantTxID ...int64) {
	t.Helper()
	txID, ok := frame["processed-tx-id"].(float64)
	if !ok || txID <= 0 {
		t.Fatalf("SSE refresh tx id = %#v", frame["processed-tx-id"])
	}
	if len(wantTxID) > 0 && (len(wantTxID) != 1 || int64(txID) != wantTxID[0]) {
		t.Fatalf("SSE refresh tx id = %v; want %v", txID, wantTxID)
	}
	want := map[string]any{
		"op": "refresh-ok", "processed-tx-id": txID,
		"computations": []any{map[string]any{
			"instaql-query": map[string]any{"todos": map[string]any{}},
			"instaql-result": []any{map[string]any{
				"child-nodes": []any{},
				"data": map[string]any{"datalog-result": map[string]any{"join-rows": []any{[]any{
					[]any{cf003EntityID, idAttr, cf003EntityID},
					[]any{cf003EntityID, titleAttr, title},
				}}}},
			}},
		}},
	}
	if !reflect.DeepEqual(frame, want) {
		t.Fatalf("SSE raw refresh = %#v; want %#v", frame, want)
	}
}
