package benchrun

import (
	"bufio"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/instant-v2/instant-v2/internal/benchharness"
	"github.com/jackc/pgx/v5"
)

const DefaultMaxArtifactBytes int64 = 128 << 20

// MaxAbsoluteArtifactBudget is a hard safety ceiling for contract-derived
// bundles. It is deliberately independent of the historical default so large
// valid JSONL workloads can be measured without unbounded reads or writes.
const MaxAbsoluteArtifactBudget int64 = 64 << 40
const SmallArtifactBytes int64 = 8 << 20
const MaxJSONLineBytes int = 1 << 20

// secretRE consumes a complete quoted value, including escaped quotes and
// whitespace. Keeping the value as one match is important: replacing only
// the first token would leak a quoted DSN/cookie suffix into failure logs.
var secretRE = regexp.MustCompile(`(?is)((?:"?(?:authorization|cookie|password|passwd|token|secret|dsn|auth)"?)\s*[:=]\s*)("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^,\s};]+)`)
var uriCredentialRE = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)([^/:\s@]+):([^@\s/]+)@`)
var authHeaderRE = regexp.MustCompile(`(?i)(authorization\s*[:=]\s*)(?:bearer|basic)\s+[^\r\n,}]+`)
var secretJSONKeyRE = regexp.MustCompile(`(?i)^(?:authorization|cookie|password|passwd|token|secret|dsn|auth)$`)

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
func Redact(s string) string {
	s = authHeaderRE.ReplaceAllString(s, `${1}[REDACTED]`)
	s = uriCredentialRE.ReplaceAllString(s, `${1}[REDACTED]@`)
	return secretRE.ReplaceAllString(s, `${1}"[REDACTED]"`)
}

func redactJSONBytes(b []byte, indent bool) ([]byte, error) {
	redacted := []byte(Redact(string(b)))
	if json.Valid(redacted) {
		return redacted, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	value = redactJSONValue(value)
	if indent {
		return json.MarshalIndent(value, "", "  ")
	}
	return json.Marshal(value)
}

func redactJSONValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if secretJSONKeyRE.MatchString(key) {
				value[key] = "[REDACTED]"
				continue
			}
			value[key] = redactJSONValue(item)
		}
		return value
	case []any:
		for i := range value {
			value[i] = redactJSONValue(value[i])
		}
		return value
	case string:
		return Redact(value)
	default:
		return value
	}
}

func (w *ArtifactWriter) WriteJSON(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	b, err = redactJSONBytes(b, true)
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
func (w *ArtifactWriter) Write(name string, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writeUnlocked(name, data)
}

// ConfigureContractBudget derives bounded artifact capacity from the frozen
// workload cardinality. An explicit operator cap is never raised; the
// default budget may grow to accommodate the contract's real JSONL volume.
func (w *ArtifactWriter) ConfigureContractBudget(total, perFile int64) error {
	if total <= 0 || perFile <= 0 || perFile > total || total > MaxAbsoluteArtifactBudget || perFile > MaxAbsoluteArtifactBudget {
		return errors.New("invalid contract artifact budget")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	effectiveTotal, effectiveFile := total, perFile
	if w.operatorBudget {
		effectiveTotal = w.MaxBytes
		if effectiveTotal <= 0 || effectiveTotal > MaxAbsoluteArtifactBudget {
			return errors.New("invalid operator artifact budget")
		}
		if effectiveFile > w.MaxFileBytes {
			effectiveFile = w.MaxFileBytes
		}
	}
	if w.contractBudgetConfigured {
		if w.MaxBytes != effectiveTotal || w.MaxFileBytes != effectiveFile {
			return errors.New("artifact budget was already configured differently")
		}
		return nil
	}
	if w.finalized || w.total != 0 {
		return errors.New("artifact budget must be configured before writes")
	}
	if !w.operatorBudget {
		w.MaxBytes = total
		w.MaxFileBytes = perFile
	} else {
		if perFile < w.MaxFileBytes {
			w.MaxFileBytes = perFile
		}
	}
	w.contractBudgetConfigured = true
	return nil
}
func mustJSON(v any) []byte {
	b, _ := json.MarshalIndent(v, "", "  ")
	b, _ = redactJSONBytes(b, true)
	return append(b, '\n')
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
	data := mustJSON(v)
	if int64(len(data)) > w.MaxFileBytes || w.total-int64(len(old))+int64(len(data)) > w.MaxBytes {
		return fmt.Errorf("artifact size limit exceeded")
	}
	if err := os.WriteFile(p, data, 0o640); err != nil {
		return err
	}
	w.total += int64(len(data)) - int64(len(old))
	return nil
}

func BuildChecksums(root string) error {
	total, perFile, err := bundleArtifactLimits(root)
	if err != nil {
		return err
	}
	return buildChecksumsWithLimits(root, total, perFile)
}
func buildChecksumsUnlocked(root string) error {
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
	h := sha256.New()
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
		fmt.Fprintf(f, "%s  %s\n", hex.EncodeToString(sum[:]), rel)
		h.Write(sum[:])
	}
	return nil
}

func validateBundleSize(root string, totalLimit, fileLimit int64) error {
	if err := validateArtifactLimits(totalLimit, fileLimit); err != nil {
		return err
	}
	files, err := bundleFiles(root)
	if err != nil {
		return err
	}
	var total int64
	for rel := range files {
		f, size, err := openRegularArtifact(filepath.Join(root, rel))
		if err != nil {
			return err
		}
		_ = f.Close()
		if size > fileLimit || total > totalLimit-size {
			return errors.New("bundle exceeds artifact budget")
		}
		total += size
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

var derivedBundleFiles = map[string]bool{"report.md": true, "checksums.sha256": true, "raw-index.json": true, "approval.json": true}

const ApprovalPrivateKeyEnv = "INSTANT_BENCH_APPROVAL_PRIVATE_KEY"

// ComputeContentRoot hashes only immutable raw evidence. Derived reports,
// indexes, checksums, and approval metadata are excluded to avoid circular
// signing and to make replay independent of renderer output.
func ComputeContentRoot(root string) (string, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	m, err := LoadManifest(root)
	if err != nil {
		return "", err
	}
	return computeContentRoot(root, m)
}

func computeContentRoot(root string, m Manifest) (string, error) {
	totalLimit, fileLimit, err := bundleArtifactLimits(root)
	if err != nil {
		return "", err
	}
	return computeContentRootWithLimits(root, m, totalLimit, fileLimit)
}

func computeContentRootWithLimits(root string, m Manifest, totalLimit, fileLimit int64) (string, error) {
	if err := validateBundleSize(root, totalLimit, fileLimit); err != nil {
		return "", err
	}
	files, err := bundleFiles(root)
	if err != nil {
		return "", err
	}
	paths := make([]string, 0, len(files))
	for path := range files {
		if derivedBundleFiles[path] {
			continue
		}
		paths = append(paths, path)
	}
	sort.Strings(paths)
	h := sha256.New()
	for _, path := range paths {
		var sum [32]byte
		if path == "manifest.json" {
			m.ApprovalPublicKey, m.ApprovalSignature, m.ContentRoot = "", "", ""
			sum = sha256.Sum256(mustJSON(m))
		} else {
			sum, err = hashFileBounded(filepath.Join(root, path), fileLimit)
			if err != nil {
				return "", err
			}
		}
		_, _ = fmt.Fprintf(h, "%s\x00%s\n", path, hex.EncodeToString(sum[:]))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func VerifyContentRoot(root string, m Manifest) error {
	if m.ContentRoot == "" {
		return errors.New("content root is missing")
	}
	got, err := computeContentRoot(root, m)
	if err != nil {
		return err
	}
	if !strings.EqualFold(got, m.ContentRoot) {
		return errors.New("content root mismatch")
	}
	return nil
}

// ApproveBundle is the local post-run approval operation. It requires an
// already integrity-checked bundle, signs the raw-evidence root with the
// out-of-band trusted key, then atomically updates indexes and checksums.
func ApproveBundle(root string) error {
	m, err := LoadManifest(root)
	if err != nil {
		return err
	}
	pub, err := trustedApprovalKey()
	if err != nil {
		return err
	}
	private, err := approvalPrivateKey()
	if err != nil {
		return err
	}
	if !bytes.Equal(private.Public().(ed25519.PublicKey), pub) {
		return errors.New("approval private key does not match trusted public key")
	}
	totalLimit, fileLimit := DefaultMaxArtifactBytes, DefaultMaxArtifactBytes
	if validateArtifactLimits(m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes) == nil {
		totalLimit, fileLimit = m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes
	}
	if err := verifyChecksumsWithLimits(root, totalLimit, fileLimit); err != nil {
		return err
	}
	contentRoot, err := computeContentRootWithLimits(root, m, totalLimit, fileLimit)
	if err != nil {
		return err
	}
	m.ContentRoot = contentRoot
	m.ApprovalPublicKey = hex.EncodeToString(pub)
	tuple, err := ApprovalTuple(m)
	if err != nil {
		return err
	}
	m.ApprovalSignature = hex.EncodeToString(ed25519.Sign(private, tuple))
	data := mustJSON(m)
	tmp, err := os.CreateTemp(root, ".manifest-approval-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err = tmp.Chmod(0o640); err == nil {
		_, err = tmp.Write(data)
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmpName, filepath.Join(root, "manifest.json")); err != nil {
		return err
	}
	if err = RebuildBundleIndexes(root); err != nil {
		return err
	}
	if err = VerifyChecksums(root); err != nil {
		return err
	}
	return VerifyContentRoot(root, m)
}

func approvalPrivateKey() (ed25519.PrivateKey, error) {
	b, err := decodeApproval(os.Getenv(ApprovalPrivateKeyEnv), ed25519.PrivateKeySize)
	if err != nil {
		return nil, errors.New("approval private key is missing or invalid")
	}
	return ed25519.PrivateKey(b), nil
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

func ValidateBenchmarkDSN(dsn string) error {
	lower := strings.ToLower(strings.TrimSpace(dsn))
	if lower == "" {
		return errors.New("benchmark database DSN is required")
	}
	if strings.Contains(lower, "production") || strings.Contains(lower, "prod_") || strings.Contains(lower, "public") || strings.Contains(lower, "shared") {
		return errors.New("production-looking or shared DSN is forbidden")
	}
	// The A safety helper intentionally rejects keyword overrides, but older
	// versions also misclassified the `://` in a URI as hostaddr. Validate URI
	// DSNs directly while preserving the same loopback/disposable invariants;
	// keyword DSNs continue through the shared safety helper.
	if u, err := url.Parse(dsn); err == nil && (u.Scheme == "postgres" || u.Scheme == "postgresql") {
		cfg, err := pgx.ParseConfig(dsn)
		if err != nil {
			return fmt.Errorf("unsafe benchmark database DSN: %w", err)
		}
		if !strings.HasPrefix(cfg.Database, "instant_bench_") {
			return fmt.Errorf("unsafe benchmark database DSN: database %q is not disposable", cfg.Database)
		}
		if u.Query().Get("host") != "" || u.Query().Get("hostaddr") != "" || u.Query().Get("service") != "" || u.Query().Get("fallbacks") != "" {
			return errors.New("unsafe benchmark database DSN: host override is not permitted")
		}
		if err := validateLoopbackDBHost(cfg.Host); err != nil {
			return fmt.Errorf("unsafe benchmark database DSN: %w", err)
		}
		for _, fallback := range cfg.Fallbacks {
			if fallback != nil {
				if err := validateLoopbackDBHost(fallback.Host); err != nil {
					return fmt.Errorf("unsafe benchmark database DSN: %w", err)
				}
			}
		}
		return nil
	}
	if err := benchharness.ValidateDatabaseURL(dsn); err != nil {
		return fmt.Errorf("unsafe benchmark database DSN: %w", err)
	}
	return nil
}

func validateLoopbackDBHost(host string) error {
	host = strings.TrimSpace(host)
	if host == "" {
		return errors.New("explicit loopback host is required")
	}
	if ip := net.ParseIP(host); ip != nil {
		if !ip.IsLoopback() {
			return errors.New("host is not loopback")
		}
		return nil
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve host: %w", err)
	}
	if len(ips) == 0 {
		return errors.New("host has no resolved addresses")
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return errors.New("host is not loopback")
		}
	}
	return nil
}

func validateArtifactLimits(total, perFile int64) error {
	if total <= 0 || perFile <= 0 || perFile > total || total > MaxAbsoluteArtifactBudget || perFile > MaxAbsoluteArtifactBudget {
		return errors.New("invalid artifact budget")
	}
	return nil
}

// bundleArtifactLimits reads the signed manifest's derived limits. Until a
// trusted detached approval is present, or if the small manifest/plan
// evidence is malformed or inconsistent, callers get conservative defaults so
// attacker-selected large limits cannot unlock hashing or allocation.
func bundleArtifactLimits(root string) (int64, int64, error) {
	defaults := func(err error) (int64, int64, error) {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, err
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, nil
	} else if err != nil {
		return defaults(err)
	}
	b, err := readBounded(manifestPath, SmallArtifactBytes)
	if err != nil {
		return defaults(err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return defaults(fmt.Errorf("invalid manifest budget evidence: %w", err))
	}
	if m.ArtifactMaxTotalBytes == 0 && m.ArtifactMaxFileBytes == 0 {
		return DefaultMaxArtifactBytes, DefaultMaxArtifactBytes, nil
	}
	if err := validateArtifactLimits(m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes); err != nil {
		return defaults(fmt.Errorf("invalid manifest budget evidence: %w", err))
	}
	planBytes, err := readBounded(filepath.Join(root, "plan.json"), SmallArtifactBytes)
	if err != nil {
		return defaults(err)
	}
	var plan Plan
	if err := json.Unmarshal(planBytes, &plan); err != nil {
		return defaults(err)
	}
	if plan.Seed != m.Seed || len(plan.Families) != 1 || canonicalBenchmarkFamily(plan.Families[0]) != canonicalBenchmarkFamily(m.Family) || len(plan.Scales) != 1 || plan.Scales[0] != m.SubscriberScale {
		return defaults(errors.New("manifest and plan do not match"))
	}
	targetCount, err := manifestTargetCount(m)
	if err != nil {
		return defaults(err)
	}
	derivedTotal, derivedFile, err := ContractArtifactBudgetForTargets(m.Family, m.SubscriberScale, plan, targetCount)
	if err != nil || derivedTotal != m.ArtifactMaxTotalBytes || derivedFile != m.ArtifactMaxFileBytes {
		return defaults(errors.New("manifest artifact budget does not match contract"))
	}
	if err := VerifyApproval(m); err != nil {
		return defaults(err)
	}
	return m.ArtifactMaxTotalBytes, m.ArtifactMaxFileBytes, nil
}

// manifestTargetCount derives the artifact cardinality from the signed
// manifest contract. It deliberately never inspects run rows: observed rows
// are untrusted and must not be able to unlock larger artifact limits.
func manifestTargetCount(m Manifest) (int, error) {
	if !isThreeTargetManifest(m) {
		return 2, nil
	}
	if err := validateThreeTargetManifestShape(m); err != nil {
		return 0, err
	}
	if len(m.TargetRevisions) != len(threeTargetIDs) {
		return 0, errors.New("three-target manifest target count is not canonical")
	}
	for _, id := range threeTargetIDs {
		if _, ok := m.TargetRevisions[id]; !ok {
			return 0, fmt.Errorf("three-target manifest missing target revision %s", id)
		}
	}
	return len(threeTargetIDs), nil
}

func openRegularArtifact(path string) (*os.File, int64, error) {
	lstat, err := os.Lstat(path)
	if err != nil {
		return nil, 0, err
	}
	if lstat.Mode()&os.ModeSymlink != 0 || !lstat.Mode().IsRegular() {
		return nil, 0, errors.New("artifact is not a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	opened, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, err
	}
	if !opened.Mode().IsRegular() || !os.SameFile(lstat, opened) {
		_ = f.Close()
		return nil, 0, errors.New("artifact changed while opening")
	}
	return f, opened.Size(), nil
}

func hashFileBoundedSize(path string, limit int64) ([32]byte, int64, error) {
	var zero [32]byte
	if limit <= 0 {
		return zero, 0, errors.New("invalid artifact read limit")
	}
	f, size, err := openRegularArtifact(path)
	if err != nil {
		return zero, 0, err
	}
	defer f.Close()
	if size > limit {
		return zero, size, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	h := sha256.New()
	n, err := io.CopyN(h, f, limit+1)
	if err != nil && !errors.Is(err, io.EOF) {
		return zero, size, err
	}
	if n > limit {
		return zero, n, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	if n != size {
		return zero, n, errors.New("artifact changed while hashing")
	}
	current, err := f.Stat()
	if err != nil {
		return zero, n, err
	}
	if current.Size() != size {
		return zero, current.Size(), errors.New("artifact changed while hashing")
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum, n, nil
}

func hashFileBounded(path string, limit int64) ([32]byte, error) {
	sum, _, err := hashFileBoundedSize(path, limit)
	return sum, err
}

func hashFileStringBounded(path string, limit int64) (string, error) {
	sum, err := hashFileBounded(path, limit)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(sum[:]), nil
}

func readBounded(path string, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, errors.New("invalid artifact read limit")
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("artifact is not a regular file")
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("artifact exceeds %d byte limit", limit)
	}
	return b, nil
}

func isSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
