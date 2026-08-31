package storageapi

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/triple"
)

// fileAttrs lists the $files attrs v1 writes in model/app_file.clj create!;
// unique/indexed flags follow lookup-ref semantics (path is the lookup ref).
var fileAttrs = []struct {
	label   string
	value   func(m fileMeta) any
	unique  bool
	indexed bool
}{
	{"path", func(m fileMeta) any { return m.path }, true, true},
	{"id", func(m fileMeta) any { return m.id }, false, true},
	{"size", func(m fileMeta) any { return m.size }, false, false},
	{"content-type", func(m fileMeta) any { return m.contentType }, false, false},
	{"location-id", func(m fileMeta) any { return m.id }, false, false},
	{"key-version", func(fileMeta) any { return int64(1) }, false, false},
}

type fileMeta struct {
	id, path, contentType string
	size                  int64
}

// linkFileTriple mirrors v1 app-file-model/create!: one transaction creating
// the $files attrs (idempotently) and the entity's triples, cardinality-one
// so re-upload overwrites. The catalog cache is invalidated because fresh
// attrs may have been created.
func (h *Handler) linkFileTriple(ctx context.Context, appID [16]byte, id, filename, contentType string, size int64) error {
	if filename == "" {
		filename = id
	}
	meta := fileMeta{id: id, path: filename, contentType: contentType, size: size}
	entity, err := platform.ScanUUIDErr(id)
	if err != nil {
		return fmt.Errorf("storageapi: bad file id: %w", err)
	}
	err = h.Triples.WithTx(ctx, func(tx pgx.Tx) error {
		attrIDs := make(map[string][16]byte, len(fileAttrs))
		for _, fa := range fileAttrs {
			a, err := platform.GetOrCreateAttr(ctx, tx, appID, "$files", fa.label, "blob", "one", fa.unique, fa.indexed)
			if err != nil {
				return err
			}
			attrIDs[fa.label] = a.ID
		}
		cat, err := platform.LoadAttrCatalog(ctx, tx, appID)
		if err != nil {
			return err
		}
		ts := make([]triple.Triple, 0, len(fileAttrs))
		for _, fa := range fileAttrs {
			ts = append(ts, triple.Triple{E: entity, A: attrIDs[fa.label], V: fa.value(meta)})
		}
		return h.Triples.SetTx(ctx, tx, appID, cat, ts, true)
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Unique $files.path collision (same filename uploaded twice).
			return ErrFilenameTaken
		}
		return err
	}
	h.Catalogs.Invalidate(platform.UUIDToStr(appID))
	return nil
}
