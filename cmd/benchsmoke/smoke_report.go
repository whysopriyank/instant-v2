package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"io"
	"os"
	"path/filepath"
	"time"
)

type smokeResult struct {
	TargetID           string                `json:"target_id"`
	TargetKind         string                `json:"target_kind"`
	TargetRevision     string                `json:"target_revision"`
	Passed             bool                  `json:"passed"`
	PrimaryClass       benchrun.FailureClass `json:"primary_class"`
	QualificationOK    bool                  `json:"qualification_passed"`
	ExpectedLedgerRows int                   `json:"expected_ledger_rows"`
	LedgerRows         int                   `json:"ledger_rows"`
	ProtocolErrors     int                   `json:"protocol_errors"`
	BehaviorErrors     int                   `json:"behavior_errors"`
	MeasuredWindowOK   bool                  `json:"measured_window_valid"`
	MeasuredMillis     int64                 `json:"measured_millis"`
	Failure            string                `json:"failure,omitempty"`
}

type smokeReport struct {
	SchemaVersion  string        `json:"schema_version"`
	DiagnosticOnly bool          `json:"diagnostic_only"`
	Family         string        `json:"family"`
	Scale          int           `json:"scale"`
	Seed           int64         `json:"seed"`
	StartedAt      time.Time     `json:"started_at"`
	FinishedAt     time.Time     `json:"finished_at"`
	Passed         bool          `json:"passed"`
	Results        []smokeResult `json:"results"`
}

func redactStreamTo(path string) error {
	if path == "" || path == "-" {
		return errors.New("redacted stream output path is required")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("redacted stream output is not a regular file")
		}
		return errors.New("redacted stream output already exists")
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	const maxBytes = 1 << 20
	reader := bufio.NewReaderSize(os.Stdin, 32<<10)
	written := 0
	truncated := false
	for {
		chunk, readErr := reader.ReadSlice('\n')
		if len(chunk) > 0 {
			if written < maxBytes {
				redacted := []byte(benchrun.Redact(string(chunk)))
				remaining := maxBytes - written
				if len(redacted) > remaining {
					redacted = redacted[:remaining]
					truncated = true
				}
				if n, writeErr := f.Write(redacted); writeErr != nil {
					return writeErr
				} else {
					written += n
				}
				if len(redacted) < len(chunk) {
					truncated = true
				}
			} else {
				truncated = true
			}
		}
		if readErr == io.EOF {
			break
		}
		if errors.Is(readErr, bufio.ErrBufferFull) {
			continue
		}
		if readErr != nil {
			return readErr
		}
	}
	if truncated {
		marker := []byte("\n[REDACTED OUTPUT TRUNCATED]\n")
		if len(marker) > maxBytes {
			marker = marker[:maxBytes]
		}
		// Keep the file bounded even when the final retained line filled the cap.
		if written+len(marker) > maxBytes {
			return nil
		}
		_, err = f.Write(marker)
	}
	return err
}

func writeSmokeReport(path string, report smokeReport) error {
	if report.SchemaVersion != benchrun.SchemaVersion || !report.DiagnosticOnly || report.Family != "H-append" || report.Scale != 300 || report.Seed == 0 || report.StartedAt.IsZero() || report.FinishedAt.IsZero() || !report.FinishedAt.After(report.StartedAt) {
		return errors.New("diagnostic smoke report identity or timestamps are invalid")
	}
	var file *os.File
	if path == "-" {
		file = os.Stdout
	} else {
		if err := noSymlinkComponents(path); err != nil {
			return err
		}
		if !filepath.IsAbs(path) {
			return errors.New("diagnostic output must be an absolute path")
		}
		opened, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return err
		}
		file = opened
		defer file.Close()
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}
