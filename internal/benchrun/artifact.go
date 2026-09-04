package benchrun

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
)

type ArtifactWriter struct {
	Root                     string
	MaxBytes                 int64
	MaxFileBytes             int64
	maxLogBytes              int64
	total                    int64
	mu                       sync.Mutex
	finalized                bool
	operatorBudget           bool
	contractBudgetConfigured bool
	linkFile                 func(string, string) error
	removeFile               func(string) error
}

func NewArtifactWriter(root string, maxBytes int64) (*ArtifactWriter, error) {
	if root == "" {
		return nil, errors.New("artifact root is required")
	}
	operatorBudget := maxBytes > 0
	if maxBytes <= 0 {
		maxBytes = DefaultMaxArtifactBytes
	}
	r, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(r, 0o750); err != nil {
		return nil, err
	}
	if resolved, e := filepath.EvalSymlinks(r); e == nil {
		r = resolved
	}
	return &ArtifactWriter{Root: r, MaxBytes: maxBytes, MaxFileBytes: maxBytes, maxLogBytes: 8 << 20, operatorBudget: operatorBudget, linkFile: os.Link, removeFile: os.Remove}, nil
}

func (w *ArtifactWriter) resolve(name string) (string, error) {
	if name == "" || filepath.IsAbs(name) {
		return "", errors.New("unsafe artifact path")
	}
	p, err := filepath.Abs(filepath.Join(w.Root, name))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(w.Root, p)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact path escapes root")
	}
	for d := filepath.Dir(p); d != w.Root; d = filepath.Dir(d) {
		if info, e := os.Lstat(d); e == nil && info.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("artifact path crosses symlink")
		}
		if d == filepath.Dir(d) {
			break
		}
	}
	if info, e := os.Lstat(p); e == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("artifact path is symlink")
	}
	return p, nil
}

func (w *ArtifactWriter) WriteJSON(name string, v any) error {
	b, err := marshalRedactedJSON(v)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return w.Write(name, b)
}

func (w *ArtifactWriter) WriteJSONL(name string, records any) (retErr error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return errors.New("artifact writer already finalized")
	}
	p, err := w.resolve(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	values := reflect.ValueOf(records)
	if !values.IsValid() || (values.Kind() != reflect.Slice && values.Kind() != reflect.Array) {
		return errors.New("JSONL records must be a slice or array")
	}
	linkFile := w.linkFile
	if linkFile == nil {
		linkFile = os.Link
	}
	removeFile := w.removeFile
	if removeFile == nil {
		removeFile = os.Remove
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".benchrun-jsonl-")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	tmpClosed := false
	removeTemp := true
	defer func() {
		if !tmpClosed {
			if err := tmp.Close(); err != nil {
				retErr = errors.Join(retErr, err)
			}
		}
		if removeTemp {
			if err := removeFile(tmpName); err != nil {
				retErr = errors.Join(retErr, fmt.Errorf("remove temporary JSONL artifact: %w", err))
			}
		}
	}()
	var fileBytes int64
	for i := 0; i < values.Len(); i++ {
		record := values.Index(i).Interface()
		x, err := json.Marshal(record)
		if err != nil {
			return err
		}
		x, err = redactJSONBytes(x, false)
		if err != nil {
			return err
		}
		x = append(x, '\n')
		if len(x) > MaxJSONLineBytes {
			return fmt.Errorf("JSONL line exceeds limit")
		}
		if fileBytes > w.MaxFileBytes-int64(len(x)) || w.total > w.MaxBytes-fileBytes-int64(len(x)) {
			return fmt.Errorf("artifact size limit exceeded")
		}
		if _, err := tmp.Write(x); err != nil {
			return err
		}
		fileBytes += int64(len(x))
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		tmpClosed = true
		return err
	}
	tmpClosed = true
	if _, err := os.Lstat(p); err == nil {
		return errors.New("artifact already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := linkFile(tmpName, p); err != nil {
		return err
	}
	if err := removeFile(tmpName); err != nil {
		// Keep deferred temp cleanup armed until the unlink actually succeeds.
		// The destination must not survive a partially completed publication.
		rollbackErr := removeFile(p)
		if rollbackErr != nil {
			return errors.Join(err, fmt.Errorf("rollback published artifact: %w", rollbackErr))
		}
		return err
	}
	removeTemp = false
	w.total += fileBytes
	return nil
}

func (w *ArtifactWriter) WriteLog(name string, data []byte) error {
	if !strings.HasSuffix(name, ".log") {
		return errors.New("log artifact must use .log suffix")
	}
	return w.Write(name, []byte(Redact(string(data))))
}

func (w *ArtifactWriter) WriteText(name string, data []byte) error {
	return w.Write(name, []byte(Redact(string(data))))
}

func (w *ArtifactWriter) Write(name string, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeUnlocked(name, data)
}

func mustJSON(v any) []byte {
	b, err := marshalRedactedJSON(v)
	if err != nil {
		panic(err)
	}
	return append(b, '\n')
}

// marshalRedactedJSON is the single fallible JSON serialization boundary for
// artifact JSON. Callers that publish or hash an artifact must propagate its
// error; a failed marshal/redaction must never be represented by an empty or
// malformed successful artifact.
func marshalRedactedJSON(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return redactJSONBytes(b, true)
}

func (w *ArtifactWriter) writeUnlocked(name string, data []byte) error {
	if w.finalized {
		return errors.New("artifact writer already finalized")
	}
	p, err := w.resolve(name)
	if err != nil {
		return err
	}
	if int64(len(data)) > w.MaxFileBytes || w.total > w.MaxBytes-int64(len(data)) {
		return fmt.Errorf("artifact size limit exceeded")
	}
	if strings.HasSuffix(name, ".log") && int64(len(data)) > w.maxLogBytes {
		return fmt.Errorf("log size limit exceeded")
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return err
	}
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = f.Write(data); err != nil {
		return err
	}
	w.total += int64(len(data))
	return nil
}

// RewriteJSON is limited to replacing an existing JSON artifact before final
// sealing. It uses the same root, size, redaction, and accounting controls as
// Write; there is intentionally no raw file handle escape hatch.
func (w *ArtifactWriter) RewriteJSON(name string, v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return errors.New("artifact writer already finalized")
	}
	p, err := w.resolve(name)
	if err != nil {
		return err
	}
	old, err := readBounded(p, w.MaxFileBytes)
	if err != nil {
		return err
	}
	data, err := marshalRedactedJSON(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if int64(len(data)) > w.MaxFileBytes || w.total-int64(len(old))+int64(len(data)) > w.MaxBytes {
		return fmt.Errorf("artifact size limit exceeded")
	}
	if err := os.WriteFile(p, data, 0o640); err != nil {
		return err
	}
	w.total += int64(len(data)) - int64(len(old))
	return nil
}
