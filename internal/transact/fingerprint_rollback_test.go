package transact

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/instant-v2/instant-v2/internal/testkit"
)

func TestFingerprintFailureRollsBackJournalAndTriples(t *testing.T) {
	fixture := testkit.NewPostgres(t, testkit.PostgresOptions{})
	db := storage.New(fixture.Pool)
	sqlDB, err := sql.Open("pgx", fixture.DSN)
	if err != nil {
		t.Fatalf("open migrations database: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := platform.Migrate(context.Background(), sqlDB); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	var appID, entityID, attrID [16]byte
	if _, err := rand.Read(appID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(entityID[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(attrID[:]); err != nil {
		t.Fatal(err)
	}
	var creatorID [16]byte
	if _, err := rand.Read(creatorID[:]); err != nil {
		t.Fatal(err)
	}
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO instant_users (id,email) VALUES ($1,$2)`, creatorID, "fingerprint-rollback@test.local"); err != nil {
			return err
		}
		return platform.CreateApp(ctx, tx, creatorID, appID, "fingerprint rollback")
	}); err != nil {
		t.Fatalf("seed app: %v", err)
	}
	if err := db.WithTx(ctx, func(tx pgx.Tx) error {
		attr, err := platform.GetOrCreateAttr(ctx, tx, appID, "todos", "name", "blob", "one", false, true)
		if err == nil {
			attrID = attr.ID
		}
		return err
	}); err != nil {
		t.Fatalf("seed attr: %v", err)
	}
	catalog, err := platform.LoadAttrCatalog(ctx, db.Pool, appID)
	if err != nil {
		t.Fatalf("load catalog: %v", err)
	}

	seedRaw, _ := json.Marshal([]any{"add-triple", platform.UUIDToStr(entityID), platform.UUIDToStr(attrID), "kept"})
	seedSteps, err := ParseSteps([]json.RawMessage{seedRaw})
	if err != nil {
		t.Fatalf("parse seed: %v", err)
	}
	if _, err := Transact(ctx, db, catalog, appID, seedSteps, Options{Admin: true}, nil); err != nil {
		t.Fatalf("seed triple: %v", err)
	}

	var before int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&before); err != nil {
		t.Fatalf("count journal before: %v", err)
	}

	oldFingerprint := fingerprintValuesTx
	fingerprintValuesTx = func(ctx context.Context, tx pgx.Tx, values []any) ([]string, error) {
		var journalRows int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&journalRows); err != nil {
			return nil, err
		}
		if journalRows != before+1 {
			t.Errorf("fingerprint ran before journal insert: rows=%d want=%d", journalRows, before+1)
		}
		return nil, errors.New("injected fingerprint failure")
	}
	t.Cleanup(func() { fingerprintValuesTx = oldFingerprint })

	raw, _ := json.Marshal([]any{"add-triple", platform.UUIDToStr(entityID), platform.UUIDToStr(attrID), "must-not-commit"})
	steps, err := ParseSteps([]json.RawMessage{raw})
	if err != nil {
		t.Fatalf("parse failing step: %v", err)
	}
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"true","update":"true"}}}`))
	if err != nil {
		t.Fatalf("parse rules: %v", err)
	}
	res, err := Transact(ctx, db, catalog, appID, steps, Options{}, doc)
	if err == nil {
		t.Fatalf("fingerprint failure returned success: %+v", res)
	}

	var after int64
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE app_id=$1`, appID).Scan(&after); err != nil {
		t.Fatalf("count journal after: %v", err)
	}
	if after != before {
		t.Fatalf("failed fingerprint transaction left journal row: before=%d after=%d", before, after)
	}
	rows, err := db.FetchTriples(ctx, appID, storage.FetchFilter{EntityIDs: [][16]byte{entityID}})
	if err != nil {
		t.Fatalf("fetch triples after rollback: %v", err)
	}
	if len(rows) != 1 || rows[0].Triple.V != "kept" {
		t.Fatalf("failed fingerprint transaction changed triples: %+v", rows)
	}
}
