package instaql_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func rand16() [16]byte {
	var u [16]byte
	rand.Read(u[:])
	u[6] = (u[6] & 0x0f) | 0x40
	u[8] = (u[8] & 0x3f) | 0x80
	return u
}

func uuidStr(u [16]byte) string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", be32(u[0:4]), be16(u[4:6]), be16(u[6:8]), be16(u[8:10]), u[10:16])
}

func be32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}
func be16(b []byte) uint16 { return uint16(b[0])<<8 | uint16(b[1]) }

var _ = triple.Triple{}
var _ pgx.Tx = nil

func mustAttr(t *testing.T, tx pgx.Tx, appID [16]byte, etype, label, vt, card string, uniq, idx bool) (platform.Attr, error) {
	t.Helper()
	return platform.GetOrCreateAttr(context.Background(), tx, appID, etype, label, vt, card, uniq, idx)
}

func newPool(dsn string) (*pgxpool.Pool, error) {
	return pgxpool.New(context.Background(), dsn)
}
