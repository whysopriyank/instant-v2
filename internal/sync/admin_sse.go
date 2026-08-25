package sync

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

// AdminSubscribe serves POST /admin/subscribe-query — v1 admin/routes.clj
// query-sse. The frozen Python SDK's AsyncInstant.subscribe_query drives this
// endpoint: one self-contained POST carrying the InstaQL body; the response
// is an SSE stream of
//
//	{"op":"sse-init","machine-id":…,"session-id":…}
//	{"op":"add-query-ok","result":<object-tree>,"result-meta":{...}}
//	{"op":"refresh-ok","computations":[{instaql-query, instaql-result:tree}]}
//
// with return-type :tree semantics (session.clj:1395) — bare object trees, no
// "data" wrapper. Auth is the admin plane (app-id header + Bearer token);
// AdminAuth must be injected by cmd wiring (CatalogCache.CheckAdminToken).
// Each reconnect re-POSTs, so unlike /runtime/sse there is no token registry:
// subscriptions die with the request context.

type sseEnvelope struct {
	Data     json.RawMessage `json:"data"`
	PageInfo json.RawMessage `json:"page-info"`
}

// resultMetaOf extracts {"page-info": …} from a full instaql envelope.
func resultMetaOf(result json.RawMessage) json.RawMessage {
	var env sseEnvelope
	if err := json.Unmarshal(result, &env); err != nil || len(env.PageInfo) == 0 {
		return json.RawMessage("{}")
	}
	out, _ := json.Marshal(map[string]json.RawMessage{"page-info": env.PageInfo})
	return out
}

func treeOf(result json.RawMessage) json.RawMessage {
	tree, err := UnwrapTree(result)
	if err != nil {
		return json.RawMessage("null")
	}
	return tree
}

func writeJSONErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
}

func (h *SSEHandler) teardownSubs(sess *Session) {
	h.Manager.DetachAll(sess)
	h.Manager.Deps.Rooms.LeaveAll(sess)
}

func (h *SSEHandler) AdminSubscribe(w http.ResponseWriter, r *http.Request) {
	if h.AdminAuth == nil {
		http.Error(w, "admin subscribe not configured", http.StatusInternalServerError)
		return
	}
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	appID := r.Header.Get("app-id")
	if appID == "" {
		appID = r.URL.Query().Get("app_id")
	}
	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if appID == "" || token == "" || !h.AdminAuth(r.Context(), appID, token) {
		writeJSONErr(w, http.StatusUnauthorized, "invalid admin credentials")
		return
	}

	var body struct {
		Query      map[string]any `json:"query"`
		Inference  bool           `json:"inference?"`
		Versions   map[string]any `json:"versions"`
		ClientEvID string         `json:"client-event-id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Query == nil {
		writeJSONErr(w, http.StatusBadRequest, "missing or invalid `query` object")
		return
	}
	qraw, err := json.Marshal(body.Query)
	if err != nil {
		writeJSONErr(w, http.StatusBadRequest, "query not serializable")
		return
	}

	sessionID := newSSESessID()
	sess := &Session{
		ID:          sessionID,
		AppID:       appID,
		Features:    Features{}, // admin subscribe never negotiates client gates
		Admin:       true,       // authenticated above via AdminAuth
		TreeResults: true,
		Subs:        map[string]bool{},
		Rooms:       map[string]bool{},
	}
	events := make(chan Frame, 128)
	sess.Send = func(f Frame) error {
		select {
		case events <- f:
			return nil
		default:
			return errSSEBackpressure
		}
	}
	sess.SendRaw = func(b []byte) error {
		select {
		case events <- Frame{"__raw": json.RawMessage(b)}:
			return nil
		default:
			return errSSEBackpressure
		}
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)

	// writeRaw emits pre-encoded group fan-out bytes verbatim.
	writeRaw := func(b []byte) bool {
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			return false
		}
		fl.Flush()
		return true
	}
	writeEvent := func(f Frame) bool {
		b, err := f.Encode()
		if err != nil {
			return true // skip malformed; keep stream alive
		}
		return writeRaw(b)
	}

	_ = writeEvent(Frame{
		"op":         json.RawMessage(`"sse-init"`),
		"machine-id": json.RawMessage(mustJSON(h.machineID())),
		"session-id": json.RawMessage(mustJSON(sessionID)),
		"app-id":     json.RawMessage(mustJSON(appID)),
	})

	// Register through the same Manager path as WS add-query (topic compile,
	// per-app cap). The initial answer rides add-query-ok below, not the
	// reactive snapshot — v1 answers query-sse synchronously on open.
	addQuery := Frame{
		"op":              json.RawMessage(`"add-query"`),
		"q":               qraw,
		"client-event-id": json.RawMessage(mustJSON(body.ClientEvID)),
	}
	replies, herr := h.Manager.Handle(r.Context(), sess, addQuery)
	badReply := herr != nil && len(replies) > 0 && string(replies[0]["op"]) != `"add-query-ok"`
	if badReply || (herr != nil && len(replies) == 0) {
		if len(replies) > 0 {
			writeEvent(replies[0])
			return
		}
		writeEvent(ErrFrame(400, "subscribe-failed", herr.Error()))
		return
	}

	class := wireNodelist
	if sess.TreeResults {
		class = wireTree
	}
	key := groupKey(sess.AppID, class, qraw, sess.Admin)
	sub, _ := h.Store.Get(key)
	result, meta := json.RawMessage("null"), json.RawMessage("{}")
	if sub != (*reactive.Subscription)(nil) && h.Refresh != nil {
		if res, rerr := h.Refresh(r.Context(), sub); rerr == nil {
			result = res
			meta = resultMetaOf(res)
		}
	}

	if !writeEvent(Frame{
		"op":              json.RawMessage(`"add-query-ok"`),
		"q":               qraw,
		"result":          treeOf(result),
		"result-meta":     meta,
		"processed-tx-id": json.RawMessage(`0`),
	}) {
		return
	}
	// Forward any extra protocol replies, but NOT the Manager's bare
	// add-query-ok — our enriched one above already carried the tree result;
	// re-forwarding would surface a data-less 'ok' to SDK consumers.
	for _, rf := range replies {
		if op, _ := rf.GetOp(); op == "add-query-ok" || op == "add-query-exists" {
			continue
		}
		if !writeEvent(rf) {
			return
		}
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			h.teardownSubs(sess)
			return
		case f := <-events:
			var ok bool
			if raw, isRaw := f["__raw"]; isRaw {
				ok = writeRaw(raw)
			} else {
				ok = writeEvent(f)
			}
			if !ok {
				h.teardownSubs(sess)
				return
			}
		}
	}
}
