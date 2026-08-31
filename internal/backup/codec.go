package backup

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"
)

// dumpHeader is the ordered header line (field order is part of the wire
// shape: "kind" leads every line).
type dumpHeader struct {
	Kind      string `json:"kind"`
	Format    string `json:"format"`
	Version   int    `json:"version"`
	AppID     string `json:"app_id"`
	Title     string `json:"title"`
	CreatorID string `json:"creator_id"`
}

// hashWriter tees every dumped byte into the checksum before the trailer.
type hashWriter struct {
	w io.Writer
	h interface {
		io.Writer
		Sum(b []byte) []byte
	}
}

func (hw hashWriter) writeLine(line string) error {
	if _, err := io.WriteString(hw.w, line); err != nil {
		return err
	}
	if _, err := io.WriteString(hw.w, "\n"); err != nil {
		return err
	}
	if _, err := io.WriteString(hw.h, line+"\n"); err != nil {
		return err
	}
	return nil
}

// recordWriter emits COPY/query output lines as logical records, honoring the
// resume offset and counting what it emits.
type recordWriter struct {
	hw    hashWriter
	skip  int
	count int64
}

func (r *recordWriter) emit(line string) error {
	if r.skip > 0 {
		r.skip--
		return nil
	}
	r.count++
	return r.hw.writeLine(line)
}

// lineScanner wraps bufio.Scanner with generous caps (values can be large).
type lineScanner struct {
	sc *bufio.Scanner
	l  string
}

func newLineScanner(r io.Reader) *lineScanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 64<<20)
	return &lineScanner{sc: sc}
}

func (l *lineScanner) Next() bool {
	if !l.sc.Scan() {
		return false
	}
	l.l = l.sc.Text()
	return true
}

func (l *lineScanner) line() string { return l.l }

func (l *lineScanner) err(def error) error {
	if e := l.sc.Err(); e != nil {
		return fmt.Errorf("backup: read dump: %v", e)
	}
	return def
}

// decodeKinded parses one record line into kind + raw body.
func decodeKinded(line string) (string, json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return "", nil, fmt.Errorf("backup: malformed record line: %v", err)
	}
	kb, ok := m["kind"]
	if !ok {
		return "", nil, errors.New(`backup: record missing "kind"`)
	}
	var kind string
	if err := json.Unmarshal(kb, &kind); err != nil {
		return "", nil, fmt.Errorf("backup: bad kind field: %v", err)
	}
	delete(m, "kind")
	body, err := json.Marshal(m)
	if err != nil {
		return "", nil, err
	}
	return kind, body, nil
}

func strictUnmarshal(body json.RawMessage, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("malformed record: %v", err)
	}
	return nil
}

// parsePGTime parses Postgres to_json(timestamptz) output (RFC3339-ish ISO).
func parsePGTime(s string) (time.Time, error) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.999999999Z07:00"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp")
}
