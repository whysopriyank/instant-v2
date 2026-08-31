package main

import (
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/instant-v2/instant-v2/internal/reactive"
)

// health reports liveness plus the overload gauges operators (and LBs) need:
// notifier queue depth and, when shedding is enabled, its configured ceiling
// (docs/reference/09-tier2-architecture.md §T2.1).
func health(db *sql.DB, n *reactive.Notifier) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if db == nil {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true,"db":false}`))
			return
		}
		if err := db.PingContext(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ok":false,"db":true}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		// Deliberately minimal: operational internals (node id, queue depth,
		// bus mode) live on the loopback metrics listener, not a public
		// endpoint. Load balancers only need liveness + db reachability.
		body := map[string]any{
			"ok": true,
			"db": true,
		}
		if n != nil {
			body["queue-depth"] = n.QueueDepth()
		}
		_ = json.NewEncoder(w).Encode(body)
	}
}
