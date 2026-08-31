package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/benchrun"
	"path/filepath"
	"strings"
)

func validateProgressEvidence(dir string, run benchrun.Run, root ...*artifactRoot) error {
	readLines := validateJSONLines
	readBytes := readBounded
	if len(root) > 0 && root[0] != nil {
		relativeDir, err := filepath.Rel(root[0].path, dir)
		if err != nil || relativeDir == ".." || strings.HasPrefix(relativeDir, ".."+string(filepath.Separator)) {
			return errors.New("run artifact path escaped bundle root")
		}
		readLines = func(path string) error {
			relative := filepath.Join(relativeDir, filepath.Base(path))
			return validateJSONLinesAt(root[0], relative)
		}
		readBytes = func(path string, max int64) ([]byte, error) {
			return root[0].read(filepath.Join(relativeDir, filepath.Base(path)), max)
		}
	}
	for _, name := range []string{"ledger.jsonl", "frames.jsonl", "process.jsonl", "runtime-metrics.jsonl"} {
		if err := readLines(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	for _, name := range []string{"db-before.json", "db-after.json"} {
		b, err := readBytes(filepath.Join(dir, name), 8<<20)
		if err != nil {
			return err
		}
		if len(b) > 0 && !json.Valid(b) {
			return fmt.Errorf("malformed JSON sibling %s", name)
		}
	}
	if run.PrimaryClass != benchrun.Pass {
		return nil
	}
	if run.MeasuredStartedAt.IsZero() || run.MeasuredFinishedAt.IsZero() || !run.MeasuredFinishedAt.After(run.MeasuredStartedAt) || run.MeasuredStartedAt.Before(run.StartedAt) || run.MeasuredFinishedAt.After(run.EndedAt) {
		return errors.New("passing run measured timestamps are invalid")
	}
	for name, measurement := range run.Measurements {
		if err := benchrun.ValidateMeasurement(measurement); err != nil {
			return fmt.Errorf("invalid run measurement %s: %w", name, err)
		}
	}
	ledgerBytes, err := readBytes(filepath.Join(dir, "ledger.jsonl"), 8<<20)
	if err != nil {
		return err
	}
	rows := 0
	seen := make(map[string]bool)
	recipients := make(map[string]bool)
	scanner := bufio.NewScanner(bytes.NewReader(ledgerBytes))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var row benchrun.LedgerRow
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return err
		}
		if row.SchemaVersion != benchrun.SchemaVersion || row.RunID != run.ID || row.PairID != run.PairID || row.WriterID == "" || row.RecipientID == "" || row.ClientEventID == "" || len(row.ExpectedQuerySet) == 0 || len(row.ExpectedRecipientSet) == 0 || !uniqueNonEmpty(row.ExpectedQuerySet) || !uniqueNonEmpty(row.ExpectedRecipientSet) || row.SubmittedAt.IsZero() || row.AcknowledgementAt.IsZero() || row.ConvergedAt.IsZero() || (row.Coverage != "converged_without_intermediate" && row.CoverAt.IsZero()) || row.AcknowledgementAt.Before(row.SubmittedAt) || (!row.CoverAt.IsZero() && row.CoverAt.Before(row.AcknowledgementAt)) || row.ConvergedAt.Before(row.AcknowledgementAt) || (!row.CoverAt.IsZero() && row.ConvergedAt.Before(row.CoverAt)) || row.SubmittedAt.Before(run.StartedAt) || row.ConvergedAt.After(run.EndedAt) || !contains(row.ExpectedRecipientSet, row.RecipientID) || !benchrun.ValidateCoverage(row.Coverage) || row.Coverage == "none" || row.ExpectedMaterializedDigest == "" || row.ObservedMaterializedDigest == "" {
			return errors.New("passing run ledger evidence is incomplete")
		}
		key := row.ClientEventID + "\x00" + row.RecipientID
		if seen[key] {
			return errors.New("passing run ledger evidence contains duplicates")
		}
		seen[key] = true
		recipients[row.RecipientID] = true
		if row.Coverage == "coalesced" {
			if row.CoveredExpectedMaterializedDigest == "" || row.CoveredExpectedMaterializedDigest != row.ObservedMaterializedDigest || row.CoveredExpectedStateVersion <= row.ExpectedStateVersion || row.ObservedStateVersion < row.CoveredExpectedStateVersion {
				return errors.New("passing run coalesced ledger evidence is invalid")
			}
		} else if row.ExpectedMaterializedDigest != row.ObservedMaterializedDigest {
			return errors.New("passing run ledger semantic evidence mismatches")
		}
		rows++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(filepath.Join(dir, "ledger.jsonl")), err)
	}
	if rows != run.ExpectedLedgerRows || run.ExpectedLedgerRows <= 0 || run.ExpectedMutationRecipients != run.ExpectedLedgerRows {
		return errors.New("passing run ledger cardinality is not exact")
	}
	framesBytes, err := readBytes(filepath.Join(dir, "frames.jsonl"), 8<<20)
	if err != nil {
		return err
	}
	frameCount := 0
	scanner = bufio.NewScanner(bytes.NewReader(framesBytes))
	scanner.Buffer(make([]byte, 64<<10), 1<<20)
	for scanner.Scan() {
		var frame benchrun.Frame
		if err := json.Unmarshal(scanner.Bytes(), &frame); err != nil {
			return err
		}
		if frame.SchemaVersion != benchrun.SchemaVersion || frame.RunID != run.ID || frame.RecipientID == "" || !recipients[frame.RecipientID] || frame.ReceivedAt.IsZero() || frame.ReceivedAt.Before(run.StartedAt) || frame.ReceivedAt.After(run.EndedAt) {
			return errors.New("passing run frame evidence is incomplete")
		}
		if err := benchrun.ValidateMeasurement(frame.PayloadBytes); err != nil {
			return err
		}
		frameCount++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("malformed JSONL sibling %s: %w", filepath.Base(filepath.Join(dir, "frames.jsonl")), err)
	}
	if frameCount == 0 {
		return errors.New("passing run frame evidence is empty")
	}
	return nil
}
