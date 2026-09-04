package transact

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/perms"
	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storage"
)

func TestPermissionProjectionStorageErrorsFailClosed(t *testing.T) {
	ctx := context.Background()
	appID := [16]byte{1}
	eid := [16]byte{2}
	attrID := [16]byte{3}
	cat := permissionTestCatalog(attrID)
	doc, err := perms.ParseRuleDoc([]byte(`{"todos":{"allow":{"create":"not valid cel !!","update":"not valid cel !!","delete":"not valid cel !!"}}}`))
	if err != nil {
		t.Fatalf("parse rules: %v", err)
	}

	cases := []struct {
		name string
		step []any
	}{
		{
			name: "create",
			step: []any{"add-triple", platform.UUIDToStr(eid), platform.UUIDToStr(attrID), "new"},
		},
		{
			name: "update",
			step: []any{"add-triple", platform.UUIDToStr(eid), platform.UUIDToStr(attrID), "new"},
		},
		{
			name: "deep-merge",
			step: []any{"deep-merge-triple", platform.UUIDToStr(eid), platform.UUIDToStr(attrID), map[string]any{"new": true}},
		},
		{
			name: "retract",
			step: []any{"retract-triple", platform.UUIDToStr(eid), platform.UUIDToStr(attrID), "old"},
		},
		{
			name: "delete-entity",
			step: []any{"delete-entity", platform.UUIDToStr(eid), "todos"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := json.Marshal(tc.step)
			if err != nil {
				t.Fatalf("marshal step: %v", err)
			}
			steps, err := ParseSteps([]json.RawMessage{raw})
			if err != nil {
				t.Fatalf("parse step: %v", err)
			}
			storageErr := errors.New("injected projection storage failure")
			calls := 0
			fetch := func(_ context.Context, _ pgx.Tx, _ [16]byte, _ storage.FetchFilter) ([]storage.Enhanced, error) {
				calls++
				return nil, storageErr
			}
			err = enforcePerms(ctx, nil, appID, cat, steps, Options{}, doc, fetch)
			if !errors.Is(err, storageErr) {
				t.Fatalf("expected storage error, got %v", err)
			}
			if calls != 1 {
				t.Fatalf("fetch calls: got %d want 1 (batch fetch)", calls)
			}
		})
	}
}

func TestEntityProjectionReturnsStorageError(t *testing.T) {
	storageErr := errors.New("projection query failed")
	cat := permissionTestCatalog([16]byte{3})
	got, err := entityProjectionWithFetcher(context.Background(), nil, [16]byte{1}, [16]byte{2}, cat,
		func(context.Context, pgx.Tx, [16]byte, storage.FetchFilter) ([]storage.Enhanced, error) {
			return nil, storageErr
		})
	if !errors.Is(err, storageErr) {
		t.Fatalf("expected storage error, got %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil projection on error, got %#v", got)
	}
}

func permissionTestCatalog(attrID [16]byte) *platform.AttrCatalog {
	et, label := "todos", "name"
	cat := &platform.AttrCatalog{}
	cat.Add(platform.Attr{ID: attrID, Etype: &et, Label: &label, ValueType: "blob", Cardinality: "one"})
	return cat
}
