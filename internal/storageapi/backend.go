package storageapi

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

// DiskBackend is an ObjectStore rooted at an explicitly configured,
// validated durable directory (DA-001). There is deliberately no default:
// an empty root is a construction error, never a silent temp fallback.
// Presigned URLs are plain http URLs served by Handler itself: the
// signature is HMAC-SHA256 over op|app-id|id|expiry using the shared server
// secret, so presigning needs no external service and the whole flow is
// testable without S3.
type DiskBackend struct {
	root   string
	secret []byte
	// createMu serializes first-use initialization of app directories:
	// a concurrent Put must not trust (EEXIST) or proceed past a
	// directory entry whose root sync has not been confirmed. The hot
	// path also takes the lock so it cannot observe incomplete creation.
	createMu sync.Mutex
	// unconfirmed tracks entries left behind when both root sync and
	// rollback failed. Existence must not turn those entries into success.
	// Guarded by createMu.
	unconfirmed map[string]bool
	// syncRoot persists directory entries; syncDir in production,
	// overridden by tests to inject a root-sync failure.
	syncRoot func(string) error
}

// NewDiskBackend validates root and builds a DiskBackend. Validation
// refuses empty, relative, malformed, and unwritable roots (DA-001d) so a
// broken mount fails at startup, not at first upload. The daemon passes the
// configured INSTANT_V2_STORAGE_ROOT; tests pass t.TempDir().
func NewDiskBackend(root string, secret []byte) (*DiskBackend, error) {
	if root == "" {
		return nil, fmt.Errorf("storageapi: storage root is required (set INSTANT_V2_STORAGE_ROOT)")
	}
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("storageapi: storage root %q must be absolute", root)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("storageapi: storage root %q: %w", root, err)
	}
	if err := probeWritable(root); err != nil {
		return nil, fmt.Errorf("storageapi: storage root %q: %w", root, err)
	}
	return &DiskBackend{root: root, secret: secret, syncRoot: syncDir}, nil
}

// probeWritable creates, syncs, closes, and removes a probe file, then
// syncs the directory, proving the mount accepts durable writes.
func probeWritable(root string) error {
	f, err := os.CreateTemp(root, ".writable-*")
	if err != nil {
		return err
	}
	name := f.Name()
	if _, err := f.Write([]byte("ok")); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(name)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return err
	}
	if err := os.Remove(name); err != nil {
		return err
	}
	return syncDir(root)
}

// syncDir fsyncs a directory so preceding renames survive a crash.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

// confirmAppDir ensures appDir exists with its root entry persisted.
// Concurrent first uploads serialize here: no Put proceeds past a
// directory whose root sync is unconfirmed, and a failed confirmation
// removes the entry so the next contender re-initializes from scratch.
func (d *DiskBackend) confirmAppDir(appDir string) error {
	d.createMu.Lock()
	defer d.createMu.Unlock()
	if d.unconfirmed[appDir] {
		if err := d.syncRoot(d.root); err != nil {
			return err
		}
		delete(d.unconfirmed, appDir)
	}
	if st, serr := os.Stat(appDir); serr == nil {
		if st.IsDir() {
			return nil
		}
		return fmt.Errorf("storageapi: %q is not a directory", appDir)
	} else if !errors.Is(serr, fs.ErrNotExist) {
		return serr
	}
	if err := os.Mkdir(appDir, 0o755); err != nil {
		if os.IsExist(err) {
			// An external actor created it concurrently; without a
			// confirmed sync of our own, re-stat to at least confirm
			// it is a directory, then trust the volume (as before).
			if st, serr := os.Stat(appDir); serr != nil {
				return serr
			} else if !st.IsDir() {
				return fmt.Errorf("storageapi: %q is not a directory", appDir)
			}
			return nil
		}
		return err
	}
	if err := d.syncRoot(d.root); err != nil {
		// Roll back: a later Put must re-initialize (and re-confirm),
		// never trust this entry.
		if removeErr := os.Remove(appDir); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
			if d.unconfirmed == nil {
				d.unconfirmed = make(map[string]bool)
			}
			d.unconfirmed[appDir] = true
		}
		return err
	}
	return nil
}

func (d *DiskBackend) Put(key string, r io.Reader) error {
	_, _, p, err := d.file(key)
	if err != nil {
		return err
	}
	// Only one level is ever created (root/<app-id>), but an entry's
	// existence alone proves nothing about its durability: a concurrent
	// first upload may have created it without confirming the root sync
	// yet. So every Put serializes directory confirmation on createMu —
	// uncontended cost is noise beside the fsyncs below — and a failed
	// confirmation rolls the entry back instead of leaving it for later
	// uploads to trust. Entries predating the backend (operator-created)
	// are the volume's responsibility, as before.
	if err := d.confirmAppDir(filepath.Dir(p)); err != nil {
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
	// DA-001a: fsync the content before close and the directory after
	// rename, so a stored object survives a crash, not just a clean exit.
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), p); err != nil {
		return err
	}
	return syncDir(filepath.Dir(p))
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
