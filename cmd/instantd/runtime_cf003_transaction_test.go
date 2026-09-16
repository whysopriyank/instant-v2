package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func TestCF003AssembledTransactionMatrix(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	otherEID := "00000000-0000-4000-8000-000000000043"

	transact := func(steps ...any) (int, string) {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
			"app-id": app, "steps": steps,
		}), map[string]string{"X-admin-token": adminToken})
		return status, body
	}
	query := func() []any {
		t.Helper()
		status, body, _ := cf003Serve(mux, http.MethodPost, "/runtime/framework/query",
			`{"query":{"todos":{}}}`, map[string]string{"app-id": app})
		if status != http.StatusOK {
			t.Fatalf("transaction matrix query = %d %q", status, body)
		}
		var envelope struct {
			Data struct {
				Todos []any `json:"todos"`
			} `json:"data"`
		}
		if err := json.Unmarshal([]byte(body), &envelope); err != nil {
			t.Fatalf("transaction matrix query body = %q: %v", body, err)
		}
		return envelope.Data.Todos
	}

	status, body := transact([]any{"update", "todos", cf003EntityID, map[string]any{"title": "tx-baseline"}})
	if status != http.StatusOK {
		t.Fatalf("transaction matrix seed = %d %q", status, body)
	}
	idAttr, titleAttr := cf003TransactionAttrs(t, mux, app, adminToken)

	// A unique-ID failure in the second low-level step must roll back the
	// first step's title change and the losing entity's partial state.
	status, body = transact(
		[]any{"add-triple", cf003EntityID, titleAttr, "must-roll-back"},
		[]any{"add-triple", otherEID, idAttr, cf003EntityID},
	)
	if status != http.StatusBadRequest {
		t.Fatalf("transaction matrix rollback status/body = %d %q", status, body)
	}
	todos := query()
	if len(todos) != 1 {
		t.Fatalf("rollback left visible entities: %#v", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "tx-baseline",
	})

	// Within one cardinality-one batch, the final value wins exactly once.
	status, body = transact(
		[]any{"add-triple", cf003EntityID, titleAttr, "tx-first"},
		[]any{"add-triple", cf003EntityID, titleAttr, "tx-last"},
	)
	if status != http.StatusOK {
		t.Fatalf("transaction matrix cardinality = %d %q", status, body)
	}
	cf003ExactTodo(t, cf003TodoByID(t, query(), cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "tx-last",
	})

	// High-level merge preserves the sibling object member and existing title.
	status, body = transact(
		[]any{"update", "todos", cf003EntityID, map[string]any{"meta": map[string]any{"a": 1}}},
		[]any{"merge", "todos", cf003EntityID, map[string]any{"meta": map[string]any{"b": 2}}},
	)
	if status != http.StatusOK {
		t.Fatalf("transaction matrix merge = %d %q", status, body)
	}
	cf003ExactTodo(t, cf003TodoByID(t, query(), cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "tx-last", "meta": map[string]any{"a": float64(1), "b": float64(2)},
	})

	// Concurrent creation through the same unique lookup must converge to one
	// visible entity; a losing request may fail, but may not publish partial data.
	outcomes := cf003ConcurrentTransactions(2, func(i int) cf003TransactionOutcome {
		status, body := transact([]any{
			"update", "todos", `lookup__slug__"cf003-race"`, map[string]any{"title": "race-" + string(rune('a'+i))},
		})
		return cf003TransactionOutcome{status: status, body: body}
	})
	successes := 0
	for i, outcome := range outcomes {
		switch outcome.status {
		case http.StatusOK:
			successes++
		case http.StatusBadRequest:
			var failure struct {
				Message string `json:"message"`
			}
			if err := json.Unmarshal([]byte(outcome.body), &failure); err != nil || !strings.Contains(failure.Message, "unique constraint violated") {
				t.Fatalf("concurrent lookup unrelated 400 = %q: %v", outcome.body, err)
			}
		default:
			t.Fatalf("concurrent lookup %d = %d %q", i, outcome.status, outcome.body)
		}
	}
	if successes == 0 {
		t.Fatalf("concurrent lookup had no committed winner: %#v", outcomes)
	}
	todos = query()
	if len(todos) != 2 {
		t.Fatalf("concurrent lookup left partial or duplicate entities: %#v", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "tx-last", "meta": map[string]any{"a": float64(1), "b": float64(2)},
	})
	var raced []map[string]any
	for _, raw := range todos {
		todo, _ := raw.(map[string]any)
		if todo["slug"] == "cf003-race" {
			raced = append(raced, todo)
		}
	}
	if len(raced) != 1 {
		t.Fatalf("concurrent lookup did not converge exactly once: todos=%#v outcomes=%#v", todos, outcomes)
	}
	winnerID, _ := raced[0]["id"].(string)
	winnerTitle := raced[0]["title"]
	if winnerID == "" || len(raced[0]) != 3 || (winnerTitle != "race-a" && winnerTitle != "race-b") {
		t.Fatalf("concurrent lookup winner = %#v", raced[0])
	}

	status, body = transact([]any{"delete", "todos", `lookup__slug__"cf003-race"`})
	if status != http.StatusOK {
		t.Fatalf("transaction matrix delete-by-lookup = %d %q", status, body)
	}
	todos = query()
	if len(todos) != 1 {
		t.Fatalf("delete-by-lookup left partial or unrelated entities: %#v", todos)
	}
	cf003ExactTodo(t, cf003TodoByID(t, todos, cf003EntityID), map[string]any{
		"id": cf003EntityID, "title": "tx-last", "meta": map[string]any{"a": float64(1), "b": float64(2)},
	})
	for _, raw := range todos {
		todo, _ := raw.(map[string]any)
		if todo["id"] == winnerID {
			t.Fatalf("delete-by-lookup retained winner %q: %#v", winnerID, todos)
		}
	}
}

func cf003TransactionAttrs(t *testing.T, mux *http.ServeMux, app, adminToken string) (string, string) {
	t.Helper()
	status, body, _ := cf003Serve(mux, http.MethodGet, "/admin/schema?app-id="+app, "",
		map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("transaction matrix schema = %d %q", status, body)
	}
	var envelope struct {
		Schema struct {
			Attrs []map[string]any `json:"attrs"`
		} `json:"schema"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		t.Fatalf("transaction matrix schema body = %q: %v", body, err)
	}
	var idAttr, titleAttr string
	for _, attr := range envelope.Schema.Attrs {
		identity, _ := attr["forward-identity"].([]any)
		if len(identity) != 3 || identity[1] != "todos" {
			continue
		}
		id, _ := attr["id"].(string)
		switch identity[2] {
		case "id":
			idAttr = id
		case "title":
			titleAttr = id
		}
	}
	if idAttr == "" || titleAttr == "" {
		t.Fatalf("transaction matrix attrs missing: %#v", envelope.Schema.Attrs)
	}
	return idAttr, titleAttr
}

func cf003TodoByID(t *testing.T, todos []any, id string) map[string]any {
	t.Helper()
	for _, raw := range todos {
		todo, _ := raw.(map[string]any)
		if todo["id"] == id {
			return todo
		}
	}
	t.Fatalf("todo %q absent from %#v", id, todos)
	return nil
}

func cf003ExactTodo(t *testing.T, got map[string]any, want map[string]any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("todo = %#v; want %#v", got, want)
	}
}

type cf003TransactionOutcome struct {
	status int
	body   string
}

func cf003ConcurrentTransactions(n int, call func(int) cf003TransactionOutcome) []cf003TransactionOutcome {
	start := make(chan struct{})
	results := make([]cf003TransactionOutcome, n)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = call(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return results
}
