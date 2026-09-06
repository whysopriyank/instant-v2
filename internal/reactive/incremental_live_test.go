package reactive

// Live-database variant of the incremental differential (docs/reference/09-tier2-
// architecture.md §T2.5): the in-memory fuzz in incremental_test.go pins the
// engine's logic against a model oracle; THIS file pins InstaqlSource's SQL
// mirrors against the real instaql.Executor on a real Postgres — the drift
// risk called out at InstaqlSource (buildConditions/loadEntities mirrors).
//
// Production wiring end to end: writes go through transact.Transact, change
// records come from transact.ResolveTriples exactly as the WS/HTTP/admin
// bridges build them, Refresh is the real executor, and every emitted
// envelope must byte-match a fresh Executor.Run of the same query.

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/transact"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

func TestLiveIncrementalMatchesExecutor(t *testing.T) {
	db := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx := context.Background()
	pool := db.Pool
	sqldb, err := sql.Open("pgx", db.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer sqldb.Close()
	if err := platform.Migrate(ctx, sqldb); err != nil {
		t.Fatal(err)
	}

	st := storage.New(pool)
	cats := platform.NewCatalogCache(pool, pool)

	appID := liveUUID()
	var title, tag platform.Attr
	err = st.WithTx(ctx, func(tx pgx.Tx) error {
		creator := liveUUID()
		if _, e := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`,
			creator, "live-diff@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, appID, "live-diff"); e != nil {
			return e
		}
		var e1, e2 error
		title, e1 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "title", "blob", "one", false, true)
		tag, e2 = platform.GetOrCreateAttr(ctx, tx, appID, "todos", "tag", "blob", "many", false, false)
		if e1 != nil {
			return e1
		}
		return e2
	})
	if err != nil {
		t.Fatal(err)
	}
	appStr := platform.UUIDToStr(appID)
	cat, err := cats.For(ctx, appStr)
	if err != nil {
		t.Fatal(err)
	}
	titleStr := platform.UUIDToStr(title.ID)
	tagStr := platform.UUIDToStr(tag.ID)

	ex := &instaql.Executor{DB: pool}
	oracle := func(rawQ string) json.RawMessage {
		q, err := instaql.Coerce(rawToMapLive(json.RawMessage(rawQ)))
		if err != nil {
			t.Fatal(err)
		}
		res, err := ex.Run(ctx, q, cat, appID)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	store := NewStore()
	mu := sync.Mutex{}
	frames := map[string]json.RawMessage{}
	var queries = []struct {
		name string
		raw  string
	}{
		{"match-all", `{"todos":{}}`},
		{"where-tag", `{"todos":{"$":{"where":{"tag":"hot"}}}}`},
	}
	topics := map[string]bool{titleStr: true, tagStr: true}
	for _, dq := range queries {
		sub := &Subscription{
			ID: "sub-" + dq.name, AppID: appStr,
			Query:  json.RawMessage(dq.raw),
			Topics: topics,
		}
		sub.Emit = func(f Frame) error {
			mu.Lock()
			frames[f.SubID] = f.ResultJSON
			mu.Unlock()
			return nil
		}
		if _, err := store.Add(sub); err != nil {
			t.Fatal(err)
		}
	}

	n := &Notifier{
		Store: store,
		Refresh: func(ctx context.Context, sub *Subscription) (json.RawMessage, error) {
			q, err := instaql.Coerce(rawToMapLive(sub.Query))
			if err != nil {
				return nil, err
			}
			res, err := ex.Run(ctx, q, cat, appID)
			if err != nil {
				return nil, err
			}
			return json.Marshal(res)
		},
		Inc: &Incremental{Source: &InstaqlSource{DB: pool, Catalog: cats.For}},
	}

	drain := func() {
		for n.drainPass(ctx, slog.Default()) {
		}
	}
	transactStep := func(steps []any, wantChanges bool) []Change {
		raws := make([]json.RawMessage, 0, len(steps))
		for _, s := range steps {
			b, _ := json.Marshal(s)
			raws = append(raws, b)
		}
		parsed, err := transact.ParseSteps(raws)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := transact.Transact(ctx, st, cat, appID, parsed,
			transact.Options{Admin: true}, nil); err != nil {
			t.Fatal(err)
		}
		changes, ok := transact.ResolveTriples(parsed, cat)
		if !ok || !wantChanges {
			return nil // production bridges fall back to topic-wide Notify
		}
		out := make([]Change, 0, len(changes))
		for _, tt := range changes {
			out = append(out, Change{Etype: tt.Etype, EntityID: tt.EntityID, AttrIDs: []string{tt.AttrID}})
		}
		return out
	}

	tx := int64(0)
	step := func(steps []any) {
		tx++
		changes := transactStep(steps, true)
		want := map[string]json.RawMessage{}
		for _, dq := range queries {
			want["sub-"+dq.name] = oracle(dq.raw)
		}
		if changes == nil {
			n.Notify(ctx, appStr, []string{titleStr}, tx)
		} else {
			n.NotifyChanges(ctx, appStr, changes, tx)
		}
		drain()
		for name, w := range want {
			got := frames[name]
			if len(got) == 0 {
				t.Fatalf("tx %d: %s emitted nothing", tx, name)
			}
			if string(got) != string(w) {
				t.Fatalf("tx %d: sub %s diverged from Executor.Run\n incremental: %s\n oracle:      %s",
					tx, name, got, w)
			}
		}
	}

	// Seed baseline so the materialized state exists.
	n.Notify(ctx, appStr, []string{titleStr}, 0)
	drain()

	rng := rand.New(rand.NewPCG(42, 7))
	live := map[string][2]string{} // id → [title, tag]
	next := 0
	for i := 0; i < 120; i++ {
		switch rng.IntN(10) {
		case 0, 1, 2, 3: // create
			next++
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", next)
			titleV := fmt.Sprintf("t-%d", rng.IntN(15))
			tagV := "cold"
			if rng.IntN(2) == 0 {
				tagV = "hot"
			}
			live[id] = [2]string{titleV, tagV}
			step([]any{
				[]any{"add-triple", id, titleStr, titleV},
				[]any{"add-triple", id, tagStr, tagV},
			})
		case 4, 5, 6: // update title (retract+add, splice-in-place path)
			for id := range live {
				newTitle := fmt.Sprintf("t-%d", rng.IntN(15))
				old := live[id]
				step([]any{
					[]any{"retract-triple", id, titleStr, old[0]},
					[]any{"add-triple", id, titleStr, newTitle},
					[]any{"retract-triple", id, tagStr, old[1]},
					[]any{"add-triple", id, tagStr, old[1]},
				})
				live[id] = [2]string{newTitle, old[1]}
				break
			}
		case 7: // membership flip via tag
			for id := range live {
				old := live[id]
				newTag := "hot"
				if old[1] == "hot" {
					newTag = "cold"
				}
				step([]any{
					[]any{"retract-triple", id, tagStr, old[1]},
					[]any{"add-triple", id, tagStr, newTag},
				})
				live[id] = [2]string{old[0], newTag}
				break
			}
		default: // delete
			for id := range live {
				old := live[id]
				step([]any{
					[]any{"retract-triple", id, titleStr, old[0]},
					[]any{"retract-triple", id, tagStr, old[1]},
				})
				delete(live, id)
				break
			}
		}
	}
	if len(live) == 0 {
		t.Fatal("workload deleted everything; coverage degenerated")
	}
}

func rawToMapLive(raw json.RawMessage) map[string]any {
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	return m
}

func liveUUID() [16]byte { return liveUUIDArr() }

func liveUUIDArr() [16]byte {
	var b [16]byte
	_, _ = crand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0xbf
	return b
}
