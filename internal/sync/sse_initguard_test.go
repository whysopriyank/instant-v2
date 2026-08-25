package sync_test

import (
	"bufio"
	"net/http"
	"testing"
)

// Audit M5: ops before protocol init are refused on the SSE POST path,
// mirroring the WS loop's 401 not-initialized guard. Uses only pre-existing
// handler API so it also compiles against the pre-fix implementation.
func TestSSERejectsOpsBeforeInit(t *testing.T) {
	srv, appID := sseSrv(t, nil)
	resp, err := http.Get(srv.URL + "/runtime/sse?app_id=" + appID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)

	f := nextEvent(t, sc)
	token, _ := f["sse-token"].(string)
	if token == "" {
		t.Fatalf("handshake missing sse-token: %v", f)
	}
	postMsgs(t, srv.URL, token, map[string]any{
		"op": "add-query", "q": map[string]any{"todos": map[string]any{}}, "client-event-id": "q1",
	})
	got := nextEvent(t, sc)
	if got["type"] != "not-initialized" {
		t.Fatalf("pre-init op must be refused with type=not-initialized, got %v", got)
	}
}
