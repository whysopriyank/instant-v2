package corpus_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/corpus"
	"github.com/instant-v2/instant-v2/internal/instaql"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/reactive"
	"github.com/instant-v2/instant-v2/internal/storage"
	syncpkg "github.com/instant-v2/instant-v2/internal/sync"
	"github.com/instant-v2/instant-v2/internal/testkit"
	"github.com/instant-v2/instant-v2/internal/transact"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestCorpusReplayIntegration is the actual v2 contract gate: real PostgreSQL
// migrations and fixture transactions, production WS dispatch/query/transact/
// rules/reactive paths, then exact authored-output comparison. No fake replies.
// It is not evidence of v1 equality, and does not bootstrap external endpoints.
func TestCorpusReplayIntegration(t *testing.T) {
	const root = "../../corpus"
	if _, err := corpus.ValidateCorpus(root); err != nil {
		t.Fatal(err)
	}
	manifest, err := corpus.LoadManifest(root)
	if err != nil {
		t.Fatal(err)
	}
	fixtures := map[string]string{}
	for _, entry := range manifest.Fixtures {
		fixtures[entry.ID] = entry.Path
	}
	for _, entry := range manifest.Scenarios {
		t.Run(entry.ID, func(t *testing.T) {
			replayFixtureScenario(t, root, fixtures[entry.Fixture], root+"/"+entry.Path)
		})
	}
}

// Opt-in desired-behavior reproductions. These must FAIL until their owning
// product package repairs the documented gaps; skipping them is not green evidence.
func TestCorpusKnownGapIntegration(t *testing.T) {
	if os.Getenv("INSTANT_CORPUS_KNOWN_GAPS") != "1" {
		t.Skip("known gaps: opt in with INSTANT_CORPUS_KNOWN_GAPS=1; see corpus/README.md")
	}
	for _, tc := range []struct{ name, fixture string }{
		{"after-cursor", "posts"}, {"forward-relation", "relations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			replayFixtureScenario(t, "../../corpus", "fixtures/"+tc.fixture+".json", "testdata/known-gaps/"+tc.name+".ndjson")
		})
	}
}

func replayFixtureScenario(t *testing.T, root, fixturePath, scenarioPath string) {
	t.Helper()
	pg := testkit.NewPostgres(t, testkit.PostgresOptions{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", pg.DSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := platform.Migrate(ctx, db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	fixture, err := corpus.LoadFixture(root, fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	st := storage.New(pg.Pool)
	appID, err := platform.ScanUUIDErr(fixture.AppID)
	if err != nil {
		t.Fatal(err)
	}
	creatorID, err := platform.ScanUUIDErr(fixture.CreatorID)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "corpus@example.test"); err != nil {
			return err
		}
		if err := platform.CreateApp(ctx, tx, creatorID, appID, "corpus"); err != nil {
			return err
		}
		if fixture.AdminToken != "" {
			token, err := platform.ScanUUIDErr(fixture.AdminToken)
			if err != nil {
				return err
			}
			return platform.SetAdminToken(ctx, tx, appID, token)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cat, err := platform.LoadAttrCatalog(ctx, pg.Pool, appID)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := transact.ParseSteps(fixture.Steps)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transact.Transact(ctx, st, cat, appID, steps, transact.Options{Admin: true}, nil); err != nil {
		t.Fatalf("fixture bootstrap: %v", err)
	}
	if len(fixture.Rules) > 0 {
		if _, err := pg.Pool.Exec(ctx, `INSERT INTO rules (app_id,code,version) VALUES ($1,$2,0)`, appID, fixture.Rules); err != nil {
			t.Fatal(err)
		}
	}
	cats := platform.NewCatalogCache(pg.Pool, pg.Pool)
	store := reactive.NewStore()
	executor := &instaql.Executor{DB: pg.Pool}
	refresh := func(ctx context.Context, sub *reactive.Subscription) (json.RawMessage, error) {
		var input map[string]any
		if err := json.Unmarshal(sub.Query, &input); err != nil {
			return nil, err
		}
		query, err := instaql.Coerce(input)
		if err != nil {
			return nil, err
		}
		cat, err := cats.For(ctx, sub.AppID)
		if err != nil {
			return nil, err
		}
		runner := executor
		if gate, ok := sub.AttachCtx.(*syncpkg.QueryGate); ok && gate != nil {
			runner = &instaql.Executor{DB: pg.Pool, Rules: gate.Rules, Admin: gate.Admin}
		}
		result, err := runner.Run(ctx, query, cat, appID)
		if err != nil {
			return nil, err
		}
		return json.Marshal(result)
	}
	notifier := &reactive.Notifier{Store: store, Refresh: refresh}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); notifier.Run(runCtx) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("notifier failed to stop")
		}
	})
	mgr := syncpkg.NewManager(syncpkg.Deps{
		DB: st, Catalogs: cats, Store: store, Rooms: syncpkg.NewRoomHub(),
		Rules: cats.RuleDocFor,
		OnCommit: func(ctx context.Context, appID string, attrs []string, txID int64, _ bool) {
			notifier.Notify(ctx, appID, attrs, txID)
		},
	})
	server := httptest.NewServer(&syncpkg.WSHandler{Manager: mgr, Store: store, Refresh: refresh})
	t.Cleanup(server.Close)
	scenario, err := corpus.LoadScenario(scenarioPath)
	if err != nil {
		t.Fatal(err)
	}
	result := corpus.Replay(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), scenario, 10*time.Second)
	if !result.Passed {
		t.Fatalf("replay err=%v\n%s", result.Err, result.Delta)
	}
}
