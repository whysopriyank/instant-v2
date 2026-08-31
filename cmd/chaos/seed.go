package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// corpusAppID is the app-id baked into corpus/*.ndjson scenarios.
const corpusAppID = "00000000-0000-4000-8000-000000000001"

func newV4() ([16]byte, error) {
	var u [16]byte
	if _, err := rand.Read(u[:]); err != nil {
		return u, err
	}
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u, nil
}

func formatUUID(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// seedChaosApp creates the chaos app plus the one attribute the writer uses.
func seedChaosApp(ctx context.Context, pool *pgxpool.Pool, app [16]byte) ([16]byte, error) {
	st := storage.New(pool)
	creator := [16]byte{0x63, 0x68, 0xa0, 0x53} // arbitrary fixed creator uuid prefix
	var attr [16]byte
	err := st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx,
			`INSERT INTO instant_users (id,email) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			creator, "chaos@test"); e != nil {
			return e
		}
		if e := platform.CreateApp(ctx, tx, creator, app, "chaos"); e != nil {
			return e
		}
		at, e := platform.GetOrCreateAttr(ctx, tx, app, "chaos-items", "n", "blob", "one", false, true)
		attr = at.ID
		return e
	})
	return attr, err
}

// seedCorpusApp creates the bare app the golden scenarios init against.
func seedCorpusApp(ctx context.Context, pool *pgxpool.Pool, app [16]byte) error {
	st := storage.New(pool)
	creator := [16]byte{0x63, 0x6f, 0xa0, 0x72}
	return st.WithTx(ctx, func(tx pgx.Tx) error {
		if _, e := tx.Exec(ctx,
			`INSERT INTO instant_users (id,email) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
			creator, "corpus@test"); e != nil {
			return e
		}
		return platform.CreateApp(ctx, tx, creator, app, "corpus")
	})
}
