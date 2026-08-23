package storageapi

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// DiskBackend is an ObjectStore rooted at a directory under os.TempDir() (or
// any explicit root). Presigned URLs are plain http URLs served by Handler
// itself: the signature is HMAC-SHA256 over op|app-id|id|expiry using the
// shared server secret, so presigning needs no external service and the whole
// flow is testable without S3.
type DiskBackend struct {
	root   string
	secret []byte
}

// NewDiskBackend builds a DiskBackend. An empty root defaults to
// $TMPDIR/instant-storage.
func NewDiskBackend(root string, secret []byte) *DiskBackend {
	if root == "" {
		root = filepath.Join(os.TempDir(), "instant-storage")
	}
	return &DiskBackend{root: root, secret: secret}
}

// splitKey validates "<uuid>/<uuid>" keys and returns their parts.
func splitKey(key string) (appID, id string, err error) {
	appID, id, ok := strings.Cut(key, "/")
	if !ok {
		return "", "", fmt.Errorf("storageapi: invalid object key %q", key)
	}
	if _, err := platform.ScanUUIDErr(appID); err != nil {
		return "", "", fmt.Errorf("storageapi: invalid object key %q: %w", key, err)
	}
	if _, err := platform.ScanUUIDErr(id); err != nil {
		return "", "", fmt.Errorf("storageapi: invalid object key %q: %w", key, err)
	}
	return appID, id, nil
}

func (d *DiskBackend) file(key string) (string, string, string, error) {
	appID, id, err := splitKey(key)
	if err != nil {
		return "", "", "", err
	}
	clean := path.Clean("/" + appID + "/" + id)
	return appID, id, filepath.Join(d.root, filepath.FromSlash(clean)), nil
}

// presign builds the signed URL path for op ("upload" or "download") routed
// through Handler's passthrough endpoints.
func (d *DiskBackend) presign(op, route, key string, ttl time.Duration) (string, error) {
	appID, id, _, err := d.file(key)
	if err != nil {
		return "", err
	}
	exp := time.Now().Add(ttl).Unix()
	return fmt.Sprintf("/storage/%s/%s?app-id=%s&expires=%d&signature=%s",
		route, id, appID, exp, signPayload(d.secret, op, appID, id, exp)), nil
}

func (d *DiskBackend) PresignUpload(key string, ttl time.Duration) (string, error) {
	return d.presign("upload", "upload", key, ttl)
}

func (d *DiskBackend) PresignDownload(key string, ttl time.Duration) (string, error) {
	return d.presign("download", "files", key, ttl)
}

func (d *DiskBackend) Put(key string, r io.Reader) error {
	_, _, p, err := d.file(key)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(p), ".upload-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(f.Name()) }() // no-op after successful rename
	if _, err = io.Copy(f, r); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), p)
}

func (d *DiskBackend) Open(key string) (*Object, error) {
	_, _, p, err := d.file(key)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Object{Body: f, Size: st.Size()}, nil
}

func (d *DiskBackend) Delete(keys []string) error {
	var errs []error
	for _, key := range keys {
		_, _, p, err := d.file(key)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
