package benchrun

import (
	"bufio"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Finalize writes the content index and checksum file after all raw records
// have been flushed. The index explicitly labels JSONL as plain-jsonl because
// this package does not silently claim zstd support.
func (w *ArtifactWriter) Finalize() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finalized {
		return errors.New("artifact writer already finalized")
	}
	var files []string
	err := filepath.Walk(w.Root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in artifact bundle: %s", path)
		}
		if info.IsDir() || filepath.Base(path) == "checksums.sha256" || filepath.Base(path) == "raw-index.json" {
			return nil
		}
		rel, e := filepath.Rel(w.Root, path)
		if e != nil {
			return e
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	idx := RawIndex{SchemaVersion: SchemaVersion}
	for _, rel := range files {
		path := filepath.Join(w.Root, rel)
		info, e := os.Stat(path)
		if e != nil {
			return e
		}
		sum, e := hashFileBounded(path, w.MaxFileBytes)
		if e != nil {
			return e
		}
		enc := "json"
		if strings.HasSuffix(rel, ".jsonl") || strings.HasSuffix(rel, ".jsonl.zst") {
			enc = PlainJSONL
		}
		idx.Files = append(idx.Files, IndexedFile{Path: rel, Bytes: info.Size(), SHA256: hex.EncodeToString(sum[:]), Encoding: enc})
	}
	if err := w.writeUnlocked("raw-index.json", mustJSON(idx)); err != nil {
		return err
	}
	if err := buildChecksumsWithLimits(w.Root, w.MaxBytes, w.MaxFileBytes); err != nil {
		return err
	}
	w.finalized = true
	return nil
}

func BuildChecksums(root string) error {
	total, perFile, err := bundleArtifactLimits(root)
	if err != nil {
		return err
	}
	return buildChecksumsWithLimits(root, total, perFile)
}

func buildChecksumsWithLimits(root string, totalLimit, fileLimit int64) error {
	if err := validateArtifactLimits(totalLimit, fileLimit); err != nil {
		return err
	}
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in artifact bundle: %s", path)
		}
		if info.IsDir() || filepath.Base(path) == "checksums.sha256" {
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		files = append(files, rel)
		return nil
	})
	if err != nil {
		return err
	}
	sort.Strings(files)
	f, err := os.OpenFile(filepath.Join(root, "checksums.sha256"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	return writeChecksumIndex(f, root, files, totalLimit, fileLimit)
}

func writeChecksumIndex(dst io.Writer, root string, files []string, totalLimit, fileLimit int64) error {
	var total int64
	for _, rel := range files {
		path := filepath.Join(root, rel)
		sum, size, e := hashFileBoundedSize(path, fileLimit)
		if e != nil {
			return e
		}
		if size > fileLimit || total > totalLimit-size {
			return errors.New("bundle exceeds artifact budget")
		}
		total += size
		if _, err := fmt.Fprintf(dst, "%s  %s\n", hex.EncodeToString(sum[:]), rel); err != nil {
			return err
		}
	}
	return nil
}

func VerifyChecksums(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	totalLimit, fileLimit, _ := bundleArtifactLimits(root)
	return verifyChecksumsWithLimits(root, totalLimit, fileLimit)
}

func verifyChecksumsWithLimits(root string, totalLimit, fileLimit int64) error {
	if err := validateArtifactLimits(totalLimit, fileLimit); err != nil {
		return err
	}
	f, err := os.Open(filepath.Join(root, "checksums.sha256"))
	if err != nil {
		return err
	}
	defer f.Close()
	if info, e := f.Stat(); e != nil || info.Size() > SmallArtifactBytes {
		return fmt.Errorf("checksum index exceeds artifact budget")
	}
	scan := bufio.NewScanner(f)
	scan.Buffer(make([]byte, 64<<10), MaxJSONLineBytes)
	seen := map[string]bool{}
	actual, err := bundleFiles(root)
	if err != nil {
		return err
	}
	var total int64
	for scan.Scan() {
		fields := strings.Fields(scan.Text())
		if len(fields) != 2 {
			return errors.New("malformed checksum record")
		}
		if seen[fields[1]] {
			return errors.New("duplicate checksum record")
		}
		if fields[1] == "checksums.sha256" {
			return errors.New("checksum file cannot list itself")
		}
		// resolve rejects absolute paths, traversal, and symlink escapes.
		seen[fields[1]] = true
		p, e := (&ArtifactWriter{Root: root}).resolve(fields[1])
		if e != nil {
			return e
		}
		sum, size, e := hashFileBoundedSize(p, fileLimit)
		if e != nil {
			return e
		}
		if size > fileLimit || total > totalLimit-size {
			return errors.New("bundle exceeds artifact budget")
		}
		total += size
		if !strings.EqualFold(fields[0], hex.EncodeToString(sum[:])) {
			return fmt.Errorf("checksum mismatch: %s", fields[1])
		}
		delete(actual, fields[1])
	}
	if err := scan.Err(); err != nil {
		return err
	}
	if len(actual) != 0 {
		missing := make([]string, 0, len(actual))
		for p := range actual {
			missing = append(missing, p)
		}
		sort.Strings(missing)
		return fmt.Errorf("unlisted artifact files: %s", strings.Join(missing, ", "))
	}
	return nil
}

// RebuildBundleIndexes safely refreshes derived indexes after a local
// approval update. It never changes raw evidence.
func RebuildBundleIndexes(root string) error {
	root, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	totalLimit, fileLimit, err := bundleArtifactLimits(root)
	if err != nil {
		return err
	}
	files, err := bundleFiles(root)
	if err != nil {
		return err
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		if path == "raw-index.json" || path == "checksums.sha256" {
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	idx := RawIndex{SchemaVersion: SchemaVersion}
	var total int64
	for _, path := range paths {
		sum, size, err := hashFileBoundedSize(filepath.Join(root, path), fileLimit)
		if err != nil {
			return err
		}
		if size > fileLimit || total > totalLimit-size {
			return errors.New("bundle exceeds artifact budget")
		}
		total += size
		encoding := "json"
		if strings.HasSuffix(path, ".jsonl") || strings.HasSuffix(path, ".jsonl.zst") {
			encoding = PlainJSONL
		}
		idx.Files = append(idx.Files, IndexedFile{Path: path, Bytes: size, SHA256: hex.EncodeToString(sum[:]), Encoding: encoding})
	}
	if err := os.WriteFile(filepath.Join(root, "raw-index.json"), mustJSON(idx), 0o640); err != nil {
		return err
	}
	return BuildChecksums(root)
}

func bundleFiles(root string) (map[string]bool, error) {
	files := map[string]bool{}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink in artifact bundle: %s", path)
		}
		if info.IsDir() || filepath.Base(path) == "checksums.sha256" {
			return nil
		}
		rel, e := filepath.Rel(root, path)
		if e != nil {
			return e
		}
		files[rel] = true
		return nil
	})
	return files, err
}

// PlainJSONL is intentionally the deterministic fallback when zstd support is
// unavailable. It makes the encoding decision explicit in raw-index.json.
const PlainJSONL = "plain-jsonl"
