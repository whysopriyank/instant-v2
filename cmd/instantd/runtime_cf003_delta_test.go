package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func TestCF003AssembledDeltaConvergence(t *testing.T) {
	mux, appID, adminToken := cf003PostgresMux(t)
	app := platform.UUIDToStr(appID)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	eids := []string{
		cf003EntityID,
		"00000000-0000-4000-8000-000000000043",
		"00000000-0000-4000-8000-000000000044",
		"00000000-0000-4000-8000-000000000045",
	}
	steps := make([]any, len(eids))
	wantInitial := make(map[string]string, len(eids))
	for i, eid := range eids {
		title := "delta-" + string(rune('a'+i))
		steps[i] = []any{"update", "todos", eid, map[string]any{"title": title}}
		wantInitial[eid] = title
	}
	status, body, _ := cf003Serve(mux, http.MethodPost, "/admin/transact", cf003JSON(t, map[string]any{
		"app-id": app, "steps": steps,
	}), map[string]string{"X-admin-token": adminToken})
	if status != http.StatusOK {
		t.Fatalf("delta seed = %d %q", status, body)
	}

	open := func(version, eventID string) (*cf003WSClient, string, string, map[string]string, float64, float64, any) {
		t.Helper()
		client := cf003DialWS(t, ctx, server)
		client.send(t, ctx, map[string]any{
			"op": "init", "app-id": app,
			"versions": map[string]string{"@instantdb/core": version},
		})
		init := client.nextOp(t, "init-ok")
		client.sessionID, _ = init["session-id"].(string)
		idAttr, titleAttr := cf003AssertSSEInit(t, init, app)
		client.send(t, ctx, map[string]any{
			"op": "add-query", "q": map[string]any{"todos": map[string]any{}},
			"client-event-id": eventID,
		})
		ack := client.nextOp(t, "add-query-ok")
		processedTx, processedISN, resultMeta := cf003AssertWSQueryAck(t, ack, eventID)
		return client, idAttr, titleAttr, cf003NodeTitles(t, ack["result"], idAttr, titleAttr), processedTx, processedISN, resultMeta
	}

	oldClient, oldIDAttr, oldTitleAttr, oldInitial, oldTx, oldISN, oldMeta := open("0.22.9", "delta-old")
	newClient, newIDAttr, newTitleAttr, newInitial, newTx, newISN, newMeta := open("0.23.0", "delta-new")
	if oldIDAttr != newIDAttr || oldTitleAttr != newTitleAttr ||
		!reflect.DeepEqual(oldInitial, wantInitial) || !reflect.DeepEqual(newInitial, wantInitial) ||
		oldTx != newTx || oldISN != newISN || !reflect.DeepEqual(oldMeta, newMeta) {
		t.Fatalf("delta baselines drifted: attrs=%q/%q %q/%q old=%#v new=%#v want=%#v",
			oldIDAttr, newIDAttr, oldTitleAttr, newTitleAttr, oldInitial, newInitial, wantInitial)
	}

	txID := cf003TransactTitle(t, mux, app, adminToken, "delta-mutated")
	if float64(txID) <= oldTx {
		t.Fatalf("delta trigger tx %d did not advance baseline %v", txID, oldTx)
	}
	full := oldClient.nextFrame(t)
	fullTitles := cf003AssertWSFullRefresh(t, full, txID, oldIDAttr, oldTitleAttr)
	delta := newClient.nextFrame(t)
	patchEntity := cf003AssertWSDeltaRefresh(t, delta, txID, cf003EntityID)
	oldClient.assertQuiet(t, 100*time.Millisecond)
	newClient.assertQuiet(t, 100*time.Millisecond)

	newInitial[cf003EntityID], _ = patchEntity["title"].(string)
	wantFinal := map[string]string{
		eids[0]: "delta-mutated", eids[1]: "delta-b", eids[2]: "delta-c", eids[3]: "delta-d",
	}
	if !reflect.DeepEqual(fullTitles, wantFinal) || !reflect.DeepEqual(newInitial, fullTitles) {
		t.Fatalf("full/delta convergence = full %#v delta %#v want %#v", fullTitles, newInitial, wantFinal)
	}
}

func cf003AssertWSQueryAck(t *testing.T, frame map[string]any, eventID string) (float64, float64, any) {
	t.Helper()
	wantKeys := []string{"op", "q", "result", "result-meta", "processed-tx-id", "processed-isn", "client-event-id"}
	if len(frame) != len(wantKeys) {
		t.Fatalf("WS add-query keys = %#v", frame)
	}
	for _, key := range wantKeys {
		if _, ok := frame[key]; !ok {
			t.Fatalf("WS add-query missing %q: %#v", key, frame)
		}
	}
	if frame["op"] != "add-query-ok" || frame["client-event-id"] != eventID ||
		!reflect.DeepEqual(frame["q"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("WS add-query metadata = %#v", frame)
	}
	processedTx, txOK := frame["processed-tx-id"].(float64)
	processedISN, isnOK := frame["processed-isn"].(float64)
	if !txOK || !isnOK || processedTx < 0 || processedISN < 0 {
		t.Fatalf("WS add-query watermarks = tx %#v isn %#v", frame["processed-tx-id"], frame["processed-isn"])
	}
	return processedTx, processedISN, frame["result-meta"]
}

func cf003AssertWSFullRefresh(t *testing.T, frame map[string]any, txID int64, idAttr, titleAttr string) map[string]string {
	t.Helper()
	if len(frame) != 3 || frame["op"] != "refresh-ok" || frame["processed-tx-id"] != float64(txID) {
		t.Fatalf("WS full refresh envelope = %#v; want tx %d", frame, txID)
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("WS full computations = %#v", frame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("WS full computation = %#v", entry)
	}
	if _, hasDelta := entry["delta"]; hasDelta {
		t.Fatalf("old WS client received delta: %#v", entry)
	}
	return cf003NodeTitles(t, entry["instaql-result"], idAttr, titleAttr)
}

func cf003AssertWSDeltaRefresh(t *testing.T, frame map[string]any, txID int64, eid string) map[string]any {
	t.Helper()
	if len(frame) != 3 || frame["op"] != "refresh-ok-delta" || frame["processed-tx-id"] != float64(txID) {
		t.Fatalf("WS delta refresh envelope = %#v; want tx %d", frame, txID)
	}
	computations, _ := frame["computations"].([]any)
	if len(computations) != 1 {
		t.Fatalf("WS delta computations = %#v", frame["computations"])
	}
	entry, _ := computations[0].(map[string]any)
	if len(entry) != 2 || !reflect.DeepEqual(entry["instaql-query"], map[string]any{"todos": map[string]any{}}) {
		t.Fatalf("WS delta computation = %#v", entry)
	}
	if _, hasFull := entry["instaql-result"]; hasFull {
		t.Fatalf("new WS client received full result: %#v", entry)
	}
	patch, _ := entry["delta"].(map[string]any)
	ops, _ := patch["ops"].([]any)
	if len(patch) != 1 || len(ops) != 1 {
		t.Fatalf("WS delta patch = %#v", patch)
	}
	op, _ := ops[0].(map[string]any)
	wantEntity := map[string]any{"id": eid, "title": "delta-mutated"}
	wantOp := map[string]any{"op": "update", "etype": "todos", "id": eid, "entity": wantEntity}
	if !reflect.DeepEqual(op, wantOp) {
		t.Fatalf("WS delta op = %#v; want %#v", op, wantOp)
	}
	entity, ok := op["entity"].(map[string]any)
	if !ok {
		t.Fatalf("WS delta entity = %#v", op["entity"])
	}
	return entity
}

func cf003NodeTitles(t *testing.T, raw any, idAttr, titleAttr string) map[string]string {
	t.Helper()
	nodes, _ := raw.([]any)
	if len(nodes) != 1 {
		t.Fatalf("WS node result = %#v", raw)
	}
	node, _ := nodes[0].(map[string]any)
	data, _ := node["data"].(map[string]any)
	datalog, _ := data["datalog-result"].(map[string]any)
	rows, _ := datalog["join-rows"].([]any)
	if len(rows) != 1 {
		t.Fatalf("WS node rows = %#v", datalog["join-rows"])
	}
	triples, _ := rows[0].([]any)
	if len(triples) != 8 {
		t.Fatalf("WS node triples = %#v", rows[0])
	}
	ids := make(map[string]string, 4)
	titles := make(map[string]string, 4)
	for _, rawTriple := range triples {
		triple, _ := rawTriple.([]any)
		if len(triple) != 3 {
			t.Fatalf("WS triple = %#v", rawTriple)
		}
		eid, _ := triple[0].(string)
		attr, _ := triple[1].(string)
		switch attr {
		case idAttr:
			ids[eid], _ = triple[2].(string)
		case titleAttr:
			titles[eid], _ = triple[2].(string)
		default:
			t.Fatalf("WS triple has unexpected attr: %#v", triple)
		}
	}
	if len(ids) != 4 || len(titles) != 4 {
		t.Fatalf("WS node identity/title coverage = ids %#v titles %#v", ids, titles)
	}
	for eid, idValue := range ids {
		if eid != idValue || titles[eid] == "" {
			t.Fatalf("WS node entity mismatch: eid=%q id=%q title=%q", eid, idValue, titles[eid])
		}
	}
	return titles
}
