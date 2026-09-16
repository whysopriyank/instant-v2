package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/instant-v2/instant-v2/internal/benchharness"
)

// EvidenceMode indicates how and where soak evidence is emitted.
type EvidenceMode string

const (
	EvidenceModeStdout EvidenceMode = "stdout"
	EvidenceModeFile   EvidenceMode = "file"
	EvidenceModeDir    EvidenceMode = "dir"
)

// ArtifactEntry records identity, size, and content digest for a published evidence artifact.
type ArtifactEntry struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	Required  bool   `json:"required"`
}

// WorkloadManifest records the executed workload parameters.
type WorkloadManifest struct {
	Sessions     int     `json:"sessions"`
	Duration     string  `json:"duration"`
	GlobalTxRate float64 `json:"global_tx_rate"`
	TxInterval   string  `json:"tx_interval"`
	RampUp       string  `json:"ramp_up"`
	Settle       string  `json:"settle"`
	Quiescence   string  `json:"quiescence"`
	MaxP99Lag    string  `json:"max_p99_lag,omitempty"`
}

// SummaryManifest records the observed outcomes and diagnostics of the soak run.
type SummaryManifest struct {
	Connects     int64  `json:"connects"`
	Transacts    int64  `json:"transacts"`
	Refreshes    int64  `json:"refreshes"`
	Dropped      int    `json:"dropped"`
	Unresolved   int    `json:"unresolved"`
	LagSamples   int    `json:"lag_samples"`
	LagP50       string `json:"lag_p50,omitempty"`
	LagP99       string `json:"lag_p99,omitempty"`
	LagMax       string `json:"lag_max,omitempty"`
	Success      bool   `json:"success"`
	FailureError string `json:"failure_error,omitempty"`
}

// CompletenessManifest provides tamper-evident proof that a soak run completed
// and all mandatory diagnostics and artifacts were captured and finalized.
type CompletenessManifest struct {
	SchemaVersion    int              `json:"schema_version"`
	Status           string           `json:"status"` // "complete" or "failed" / "partial"
	Completed        bool             `json:"completed"`
	RunID            string           `json:"run_id"`
	StartedAt        time.Time        `json:"started_at"`
	FinishedAt       time.Time        `json:"finished_at"`
	DurationSeconds  float64          `json:"duration_seconds"`
	TargetURL        string           `json:"target_url"`
	AppID            string           `json:"app_id"`
	Workload         WorkloadManifest `json:"workload"`
	Summary          SummaryManifest  `json:"summary"`
	Artifacts        []ArtifactEntry  `json:"artifacts"`
	PprofEndpoint    string           `json:"pprof_endpoint,omitempty"`
	CompletionMarker string           `json:"completion_marker,omitempty"`
}

// EvidenceDeps provides filesystem abstraction for deterministic failure injection.
type EvidenceDeps struct {
	LinkFile   func(oldname, newname string) error
	RenameFile func(oldname, newname string) error
	Mkdir      func(path string, perm os.FileMode) error
	RemoveFile func(name string) error
	RemoveAll  func(path string) error
	MkdirAll   func(path string, perm os.FileMode) error
	MkdirTemp  func(dir, pattern string) (string, error)
	OpenFile   func(name string, flag int, perm os.FileMode) (*os.File, error)
	Lstat      func(name string) (os.FileInfo, error)
	Stdout     io.Writer
}

func defaultEvidenceDeps() EvidenceDeps {
	return EvidenceDeps{
		LinkFile:   os.Link,
		RenameFile: os.Rename,
		Mkdir:      os.Mkdir,
		RemoveFile: os.Remove,
		RemoveAll:  os.RemoveAll,
		MkdirAll:   os.MkdirAll,
		MkdirTemp:  os.MkdirTemp,
		OpenFile:   os.OpenFile,
		Lstat:      os.Lstat,
		Stdout:     os.Stdout,
	}
}

// EvidenceConfig configures the evidence manager.
type EvidenceConfig struct {
	EventsPath string
	OutDir     string
	AppID      string
	TargetURL  string
}

// EvidenceManager coordinates mandatory soak evidence, atomic staging, and publication.
type EvidenceManager struct {
	mu             sync.Mutex
	cfg            EvidenceConfig
	deps           EvidenceDeps
	mode           EvidenceMode
	targetPath     string // canonical absolute target
	manifestPath   string // final manifest path for file mode
	completionPath string // final completion marker path for file mode
	parentDir      string
	stagingDir     string
	stagedEvents   string
	stagedFile     *os.File
	eventWriter    *benchharness.EventWriter
	writeErr       atomic.Pointer[error]
	closed         bool
	published      bool
	startedAt      time.Time
	customWriteFn  func([]byte) error // optional injected writer
}

// DeriveManifestPath returns the canonical manifest filename for a given file target.
func DeriveManifestPath(targetFile string) string {
	ext := filepath.Ext(targetFile)
	if ext != "" {
		return strings.TrimSuffix(targetFile, ext) + ".manifest.json"
	}
	return targetFile + ".manifest.json"
}

// DeriveCompletionPath returns the last-written completion marker for file-mode
// evidence. Its absence means the events/manifest pair is incomplete.
func DeriveCompletionPath(targetFile string) string { return targetFile + ".complete" }

// ValidateDestination verifies that the target path does not exist, is not a symlink,
// and does not traverse symlink ancestors.
func ValidateDestination(targetPath string, isDir bool, deps EvidenceDeps) error {
	if strings.TrimSpace(targetPath) == "" {
		return errors.New("empty evidence target path")
	}
	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return fmt.Errorf("resolve evidence target %q: %w", targetPath, err)
	}

	// 1. Target must not already exist (refuse to overwrite prior evidence).
	fi, err := deps.Lstat(absTarget)
	if err == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("evidence output target %q is a symlink: refusing symlink target", absTarget)
		}
		return fmt.Errorf("evidence output target %q already exists: refusing to overwrite prior evidence", absTarget)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect evidence target %q: %w", absTarget, err)
	}

	// 2. For file mode, manifest target must also not already exist.
	if !isDir {
		manifestPath := DeriveManifestPath(absTarget)
		if mfi, mErr := deps.Lstat(manifestPath); mErr == nil {
			if mfi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("evidence manifest target %q is a symlink: refusing symlink target", manifestPath)
			}
			return fmt.Errorf("evidence manifest target %q already exists: refusing to overwrite prior evidence", manifestPath)
		} else if !os.IsNotExist(mErr) {
			return fmt.Errorf("inspect evidence manifest target %q: %w", manifestPath, mErr)
		}
		completionPath := DeriveCompletionPath(absTarget)
		if cfi, cErr := deps.Lstat(completionPath); cErr == nil {
			if cfi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("evidence completion target %q is a symlink: refusing symlink target", completionPath)
			}
			return fmt.Errorf("evidence completion target %q already exists: refusing to overwrite prior evidence", completionPath)
		} else if !os.IsNotExist(cErr) {
			return fmt.Errorf("inspect evidence completion target %q: %w", completionPath, cErr)
		}
	}

	// 3. Ancestor directories must not cross symlinks.
	// We normalize standard OS alias prefixes (such as macOS /var -> /private/var or /tmp -> /private/tmp)
	// so platform alias mounts are not falsely flagged, while preserving strict checks for any
	// intermediate symlinks created within the filesystem.
	cleanTarget := absTarget
	for _, sysPrefix := range []string{"/var", "/tmp", "/etc"} {
		if cleanTarget == sysPrefix || strings.HasPrefix(cleanTarget, sysPrefix+"/") {
			if eval, err := filepath.EvalSymlinks(sysPrefix); err == nil {
				cleanTarget = eval + cleanTarget[len(sysPrefix):]
				break
			}
		}
	}

	curr := filepath.Dir(cleanTarget)
	for {
		parentFi, pErr := deps.Lstat(curr)
		if pErr == nil {
			if parentFi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("evidence output path crosses symlink ancestor %q", curr)
			}
		} else if !os.IsNotExist(pErr) {
			return fmt.Errorf("inspect evidence ancestor directory %q: %w", curr, pErr)
		}
		parent := filepath.Dir(curr)
		if parent == curr {
			break
		}
		curr = parent
	}

	return nil
}

// NewEvidenceManager initializes evidence staging and writer with fail-closed validation.
func NewEvidenceManager(cfg EvidenceConfig, deps EvidenceDeps) (*EvidenceManager, error) {
	defaults := defaultEvidenceDeps()
	if deps.LinkFile == nil {
		deps.LinkFile = defaults.LinkFile
	}
	if deps.RenameFile == nil {
		deps.RenameFile = defaults.RenameFile
	}
	if deps.Mkdir == nil {
		deps.Mkdir = defaults.Mkdir
	}
	if deps.RemoveFile == nil {
		deps.RemoveFile = defaults.RemoveFile
	}
	if deps.RemoveAll == nil {
		deps.RemoveAll = defaults.RemoveAll
	}
	if deps.MkdirAll == nil {
		deps.MkdirAll = defaults.MkdirAll
	}
	if deps.MkdirTemp == nil {
		deps.MkdirTemp = defaults.MkdirTemp
	}
	if deps.OpenFile == nil {
		deps.OpenFile = defaults.OpenFile
	}
	if deps.Lstat == nil {
		deps.Lstat = defaults.Lstat
	}
	if deps.Stdout == nil {
		deps.Stdout = defaults.Stdout
	}

	em := &EvidenceManager{
		cfg:       cfg,
		deps:      deps,
		startedAt: time.Now().UTC(),
	}

	// Mode selection
	rawEvents := strings.TrimSpace(cfg.EventsPath)
	rawOut := strings.TrimSpace(cfg.OutDir)

	if rawEvents == "-" {
		// Explicit stdout mode: non-publishing contract.
		em.mode = EvidenceModeStdout
		em.targetPath = "-"
		em.eventWriter = benchharness.NewEventWriter(func(b []byte) error {
			out := deps.Stdout
			if out == nil {
				out = os.Stdout
			}
			_, err := out.Write(b)
			if err != nil {
				em.recordError(fmt.Errorf("stdout event write: %w", err))
			}
			return err
		}, 0)
		return em, nil
	}

	var targetPath string
	var isDir bool

	if rawOut != "" {
		targetPath = rawOut
		isDir = true
		em.mode = EvidenceModeDir
	} else if rawEvents != "" {
		targetPath = rawEvents
		if strings.HasSuffix(rawEvents, ".jsonl") || strings.HasSuffix(rawEvents, ".json") {
			isDir = false
			em.mode = EvidenceModeFile
		} else {
			isDir = true
			em.mode = EvidenceModeDir
		}
	} else {
		// Fresh output directory when no path is specified.
		targetPath = fmt.Sprintf("soak-output-%s", em.startedAt.Format("20060102-150405"))
		isDir = true
		em.mode = EvidenceModeDir
	}

	absTarget, err := filepath.Abs(targetPath)
	if err != nil {
		return nil, fmt.Errorf("resolve target path: %w", err)
	}
	for _, sysPrefix := range []string{"/var", "/tmp", "/etc"} {
		if absTarget == sysPrefix || strings.HasPrefix(absTarget, sysPrefix+"/") {
			if eval, err := filepath.EvalSymlinks(sysPrefix); err == nil {
				absTarget = eval + absTarget[len(sysPrefix):]
				break
			}
		}
	}
	em.targetPath = absTarget

	if !isDir {
		em.manifestPath = DeriveManifestPath(absTarget)
		em.completionPath = DeriveCompletionPath(absTarget)
	} else {
		em.manifestPath = filepath.Join(absTarget, "manifest.json")
	}

	// Validate target against existing files and symlinks.
	if err := ValidateDestination(absTarget, isDir, deps); err != nil {
		return nil, err
	}

	// Ensure parent directory exists.
	parentDir := filepath.Dir(absTarget)
	em.parentDir = parentDir
	if err := deps.MkdirAll(parentDir, 0o750); err != nil {
		return nil, fmt.Errorf("create evidence parent directory %q: %w", parentDir, err)
	}

	// Create temporary staging directory next to target for atomic publication.
	stagingDir, err := deps.MkdirTemp(parentDir, ".soak-staging-")
	if err != nil {
		return nil, fmt.Errorf("create evidence staging directory: %w", err)
	}
	em.stagingDir = stagingDir

	// Create staged events file.
	stagedEvents := filepath.Join(stagingDir, "events.jsonl")
	em.stagedEvents = stagedEvents
	f, err := deps.OpenFile(stagedEvents, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		_ = deps.RemoveAll(stagingDir)
		return nil, fmt.Errorf("create staged events file: %w", err)
	}
	em.stagedFile = f

	em.eventWriter = benchharness.NewEventWriter(func(b []byte) error {
		if em.customWriteFn != nil {
			if err := em.customWriteFn(b); err != nil {
				em.recordError(err)
				return err
			}
		}
		_, writeErr := f.Write(b)
		if writeErr != nil {
			em.recordError(fmt.Errorf("staged event write: %w", writeErr))
		}
		return writeErr
	}, 0)

	return em, nil
}

func (em *EvidenceManager) recordError(err error) {
	if err == nil {
		return
	}
	em.writeErr.CompareAndSwap(nil, &err)
}

// FirstError returns the first recorded event writing, flush, or close error.
func (em *EvidenceManager) FirstError() error {
	p := em.writeErr.Load()
	if p == nil {
		return nil
	}
	return *p
}

// Mode returns the active evidence mode.
func (em *EvidenceManager) Mode() EvidenceMode {
	return em.mode
}

// TargetPath returns the target publication path.
func (em *EvidenceManager) TargetPath() string {
	return em.targetPath
}

// Emit writes a structured event and records any failure.
func (em *EvidenceManager) Emit(name string, fields map[string]any) error {
	if em == nil || em.eventWriter == nil {
		return nil
	}
	err := em.eventWriter.Emit(name, fields)
	if err != nil {
		em.recordError(err)
	}
	return err
}

// FlushAndClose syncs and closes the staged events file, recording any error.
func (em *EvidenceManager) FlushAndClose() error {
	em.mu.Lock()
	defer em.mu.Unlock()
	if em.closed {
		return em.FirstError()
	}
	em.closed = true

	if em.mode == EvidenceModeStdout || em.stagedFile == nil {
		return em.FirstError()
	}

	syncErr := em.stagedFile.Sync()
	if syncErr != nil {
		em.recordError(fmt.Errorf("sync staged events: %w", syncErr))
	}
	closeErr := em.stagedFile.Close()
	if closeErr != nil {
		em.recordError(fmt.Errorf("close staged events: %w", closeErr))
	}

	return em.FirstError()
}

// Publish writes the completeness manifest and atomically moves staged artifacts
// into the final target location. It fails closed if any write or flush errors occurred.
func (em *EvidenceManager) Publish(summary SummaryManifest, workload WorkloadManifest, pprofEndpoint string) (*CompletenessManifest, error) {
	em.mu.Lock()
	defer em.mu.Unlock()

	if em.published {
		return nil, errors.New("evidence already published")
	}

	// Non-publishing mode (stdout): return nil manifest without writing files.
	if em.mode == EvidenceModeStdout {
		em.published = true
		return nil, nil
	}

	// If any prior error occurred, refuse to publish a partial bundle as complete.
	if first := em.FirstError(); first != nil {
		return nil, fmt.Errorf("refusing to publish complete evidence after prior failure: %w", first)
	}
	if !summary.Success {
		return nil, fmt.Errorf("refusing to publish complete evidence for unsuccessful soak: %s", summary.FailureError)
	}

	// Ensure staged events file is closed.
	if !em.closed && em.stagedFile != nil {
		if syncErr := em.stagedFile.Sync(); syncErr != nil {
			em.recordError(syncErr)
			return nil, fmt.Errorf("sync staged events: %w", syncErr)
		}
		if closeErr := em.stagedFile.Close(); closeErr != nil {
			em.recordError(closeErr)
			return nil, fmt.Errorf("close staged events: %w", closeErr)
		}
		em.closed = true
	}

	// Digest staged events file.
	eventsInfo, err := em.deps.Lstat(em.stagedEvents)
	if err != nil {
		return nil, fmt.Errorf("stat staged events: %w", err)
	}
	eventsHash, err := hashFile(em.stagedEvents)
	if err != nil {
		return nil, fmt.Errorf("hash staged events: %w", err)
	}

	eventsArtifactName := "events.jsonl"
	if em.mode == EvidenceModeFile {
		eventsArtifactName = filepath.Base(em.targetPath)
	}

	manifest := &CompletenessManifest{
		SchemaVersion:   1,
		Status:          "complete",
		Completed:       true,
		RunID:           fmt.Sprintf("soak-%d", em.startedAt.UnixNano()),
		StartedAt:       em.startedAt,
		FinishedAt:      time.Now().UTC(),
		DurationSeconds: time.Since(em.startedAt).Seconds(),
		TargetURL:       em.cfg.TargetURL,
		AppID:           em.cfg.AppID,
		Workload:        workload,
		Summary:         summary,
		PprofEndpoint:   pprofEndpoint,
		Artifacts: []ArtifactEntry{
			{
				Name:      eventsArtifactName,
				Path:      eventsArtifactName,
				SizeBytes: eventsInfo.Size(),
				SHA256:    eventsHash,
				Required:  true,
			},
		},
	}
	if em.mode == EvidenceModeFile {
		manifest.CompletionMarker = filepath.Base(em.completionPath)
	}

	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("serialize completeness manifest: %w", err)
	}
	manifestBytes = append(manifestBytes, '\n')

	// Write staged manifest.
	stagedManifestPath := filepath.Join(em.stagingDir, "manifest.json")
	mf, err := em.deps.OpenFile(stagedManifestPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create staged manifest: %w", err)
	}
	if _, err := mf.Write(manifestBytes); err != nil {
		_ = mf.Close()
		return nil, fmt.Errorf("write staged manifest: %w", err)
	}
	if err := mf.Sync(); err != nil {
		_ = mf.Close()
		return nil, fmt.Errorf("sync staged manifest: %w", err)
	}
	if err := mf.Close(); err != nil {
		return nil, fmt.Errorf("close staged manifest: %w", err)
	}
	var stagedCompletionPath string
	if em.mode == EvidenceModeFile {
		stagedCompletionPath = filepath.Join(em.stagingDir, ".complete")
		cf, createErr := em.deps.OpenFile(stagedCompletionPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o600)
		if createErr != nil {
			return nil, fmt.Errorf("create staged completion marker: %w", createErr)
		}
		manifestHash := sha256.Sum256(manifestBytes)
		if _, writeErr := cf.Write([]byte(hex.EncodeToString(manifestHash[:]) + "\n")); writeErr != nil {
			_ = cf.Close()
			return nil, fmt.Errorf("write staged completion marker: %w", writeErr)
		}
		if syncErr := cf.Sync(); syncErr != nil {
			_ = cf.Close()
			return nil, fmt.Errorf("sync staged completion marker: %w", syncErr)
		}
		if closeErr := cf.Close(); closeErr != nil {
			return nil, fmt.Errorf("close staged completion marker: %w", closeErr)
		}
	}

	// Atomically publish from staging to final location.
	if em.mode == EvidenceModeDir {
		// Claim the final directory with no-replace mkdir. The manifest is linked
		// last and therefore acts as the directory-mode completion marker.
		if err := ValidateDestination(em.targetPath, true, em.deps); err != nil {
			return nil, fmt.Errorf("pre-publish validation failed: %w", err)
		}
		if err := em.deps.Mkdir(em.targetPath, 0o750); err != nil {
			return nil, fmt.Errorf("claim evidence directory %q without overwrite: %w", em.targetPath, err)
		}
		if err := em.deps.LinkFile(em.stagedEvents, filepath.Join(em.targetPath, "events.jsonl")); err != nil {
			return nil, fmt.Errorf("publish directory events file: %w", err)
		}
		if err := em.deps.LinkFile(stagedManifestPath, filepath.Join(em.targetPath, "manifest.json")); err != nil {
			return nil, fmt.Errorf("publish directory manifest file; incomplete evidence retained at %q: %w", em.targetPath, err)
		}
		_ = em.deps.RemoveAll(em.stagingDir)
	} else {
		// File mode: atomic publication of events and manifest file with rollback.
		if err := ValidateDestination(em.targetPath, false, em.deps); err != nil {
			return nil, fmt.Errorf("pre-publish validation failed: %w", err)
		}

		// Hard-link publication is atomic and refuses an existing target. The
		// staging directory is created beside the target, so this does not cross
		// filesystems and avoids check-then-rename overwrite races.
		if err := em.deps.LinkFile(em.stagedEvents, em.targetPath); err != nil {
			return nil, fmt.Errorf("publish events file %q: %w", em.targetPath, err)
		}

		// Publish the manifest through the same no-replace primitive. If this
		// second publication fails, retain the manifest-less events file as
		// explicitly incomplete evidence. Never unlink it: any rollback by path
		// would reintroduce a TOCTOU race against a concurrently replaced target.
		if err := em.deps.LinkFile(stagedManifestPath, em.manifestPath); err != nil {
			return nil, fmt.Errorf("publish manifest file %q; incomplete events evidence retained at %q: %w", em.manifestPath, em.targetPath, err)
		}
		if err := em.deps.LinkFile(stagedCompletionPath, em.completionPath); err != nil {
			return nil, fmt.Errorf("publish completion marker %q; incomplete evidence retained at %q: %w", em.completionPath, em.targetPath, err)
		}

		// Clean up temporary staging directory.
		_ = em.deps.RemoveAll(em.stagingDir)
	}

	em.published = true
	return manifest, nil
}

// Abort removes the staging directory if publication did not complete.
func (em *EvidenceManager) Abort() {
	em.mu.Lock()
	defer em.mu.Unlock()
	if em.published || em.stagingDir == "" {
		return
	}
	if em.stagedFile != nil && !em.closed {
		_ = em.stagedFile.Close()
		em.closed = true
	}
	_ = em.deps.RemoveAll(em.stagingDir)
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
