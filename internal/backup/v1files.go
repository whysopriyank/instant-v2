package backup

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/instant-v2/instant-v2/internal/platform"
	"github.com/instant-v2/instant-v2/internal/storageapi"
)

var ErrFileStoreUnavailable = errors.New("backup: no file store wired for ZIP restore")
var ErrRestoreCleanup = errors.New("backup: restore object cleanup failed; reconcile target before retry")

type restoredObject struct {
	key   string
	entry *zip.File
}

// Validate every entry, including its CRC, before any object write. The same
// 1 GiB ceiling bounds compressed input, each entry and total expansion.
func validateV1Archive(reader *zip.Reader, store storageapi.ObjectStore) (map[string]*zip.File, error) {
	blobs := make(map[string]*zip.File)
	seen := make(map[string]bool)
	var expanded uint64
	for _, entry := range reader.File {
		if seen[entry.Name] {
			return nil, fmt.Errorf("backup: duplicate ZIP entry %q", entry.Name)
		}
		seen[entry.Name] = true
		if entry.UncompressedSize64 > maxEntryBytes || expanded > maxEntryBytes-entry.UncompressedSize64 {
			return nil, errors.New("backup: v1 archive expansion exceeds maximum size")
		}
		expanded += entry.UncompressedSize64
		switch {
		case entry.Name == v1ConfigEntry:
		case strings.HasPrefix(entry.Name, v1EntitiesDir) && strings.HasSuffix(entry.Name, v1EntitiesSufx):
			etype := strings.TrimSuffix(strings.TrimPrefix(entry.Name, v1EntitiesDir), v1EntitiesSufx)
			if etype == "" || strings.Contains(etype, "/") {
				return nil, errors.New("backup: invalid entity entry name")
			}
		case strings.HasPrefix(entry.Name, v1FilesDir):
			location, err := platform.ScanUUIDErr(strings.TrimPrefix(entry.Name, v1FilesDir))
			if err != nil {
				return nil, errors.New("backup: invalid file location UUID")
			}
			id := platform.UUIDToStr(location)
			if blobs[id] != nil {
				return nil, errors.New("backup: duplicate file location")
			}
			blobs[id] = entry
		default:
			return nil, fmt.Errorf("backup: unexpected v1 ZIP entry %q", entry.Name)
		}
		body, err := entry.Open()
		if err != nil {
			return nil, err
		}
		n, readErr := io.Copy(io.Discard, io.LimitReader(body, maxEntryBytes+1))
		closeErr := body.Close()
		if readErr != nil || closeErr != nil {
			return nil, errors.Join(readErr, closeErr)
		}
		if uint64(n) != entry.UncompressedSize64 {
			return nil, errors.New("backup: ZIP entry size mismatch")
		}
	}
	if len(blobs) > 0 && store == nil {
		return nil, ErrFileStoreUnavailable
	}
	return blobs, nil
}

func restoredFileObjects(ctx context.Context, tx pgx.Tx, appID [16]byte, blobs map[string]*zip.File) ([]restoredObject, error) {
	rows, err := tx.Query(ctx, `SELECT t.entity_id::text, jsonb_object_agg(a.label,t.value)
		FROM triples t JOIN attrs a ON a.id=t.attr_id
		WHERE t.app_id=$1 AND a.etype='$files'
		GROUP BY t.entity_id ORDER BY t.entity_id`, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	objects := []restoredObject{}
	used := make(map[string]bool)
	for rows.Next() {
		var id string
		var raw []byte
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		var metadata struct {
			ID       string `json:"id"`
			Location string `json:"location-id"`
			Size     *int64 `json:"size"`
		}
		if err := json.Unmarshal(raw, &metadata); err != nil {
			return nil, fmt.Errorf("backup: invalid file metadata: %w", err)
		}
		entityID, idErr := platform.ScanUUIDErr(metadata.ID)
		location, locErr := platform.ScanUUIDErr(metadata.Location)
		if idErr != nil || locErr != nil || platform.UUIDToStr(entityID) != id || metadata.Size == nil || *metadata.Size < 0 {
			return nil, errors.New("backup: file metadata requires matching id, location-id and size")
		}
		locationID := platform.UUIDToStr(location)
		entry := blobs[locationID]
		if entry == nil {
			return nil, fmt.Errorf("backup: missing blob for file location %s", locationID)
		}
		if used[locationID] {
			return nil, errors.New("backup: duplicate file metadata location")
		}
		if uint64(*metadata.Size) != entry.UncompressedSize64 {
			return nil, errors.New("backup: file metadata size differs from blob")
		}
		used[locationID] = true
		objects = append(objects, restoredObject{key: platform.UUIDToStr(appID) + "/" + id, entry: entry})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(used) != len(blobs) {
		return nil, errors.New("backup: orphan ZIP blob has no file metadata")
	}
	return objects, nil
}
