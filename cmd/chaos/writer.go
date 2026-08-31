package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

type journalEntry struct {
	Entity string
	TxID   int64
}

// writerLoop posts W sequential transacts over HTTP, returning the journal of
// acknowledged entities and how many were rejected.
func writerLoop(baseURL, appID, attrID string, writes int) ([]journalEntry, int, error) {
	var (
		journal  []journalEntry
		rejected int
	)
	for i := 0; i < writes; i++ {
		entity := writeEntityID(i)
		txID, code, body, err := postTransact(baseURL, appID, attrID, entity)
		if err == nil && code == 200 {
			journal = append(journal, journalEntry{Entity: entity, TxID: txID})
		} else {
			rejected++
			if len(journal) == 0 && rejected == 1 {
				return nil, rejected, fmt.Errorf("writer: first write rejected (status %d): %s", code, body)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(journal) == 0 {
		return nil, rejected, errors.New("writer: zero acknowledged transactions before chaos")
	}
	return journal, rejected, nil
}

// writeEntityID derives a stable UUID-shaped eid for write index i
// (transact parses eids as UUIDs).
func writeEntityID(i int) string {
	var u [16]byte
	u[6], u[8] = 0x40, 0x80 // v4-ish layout bits
	for pos, shift := 15, 0; pos >= 10 && shift < 48; pos, shift = pos-1, shift+8 {
		u[pos] = byte(i >> uint(shift))
	}
	return formatUUID(u)
}

// postTransact adds triple (entity, attrID, 1). Returns tx-id, HTTP status,
// the raw response body (for diagnostics), and transport error if any.
func postTransact(baseURL, appID, attrID, entity string) (int64, int, string, error) {
	body := fmt.Sprintf(
		`{"app-id":%q,"tx-steps":[["add-triple",%q,%q,1]]}`,
		appID, entity, attrID)
	resp, err := chaosHTTP.Post(baseURL+"/runtime/transact", "application/json", strings.NewReader(body))
	if err != nil {
		return 0, 0, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	var out struct {
		TxID int64 `json:"tx-id"`
	}
	_ = json.Unmarshal(raw, &out)
	if resp.StatusCode != 200 {
		return out.TxID, resp.StatusCode, strings.TrimSpace(string(raw)), fmt.Errorf("transact status %d", resp.StatusCode)
	}
	return out.TxID, resp.StatusCode, strings.TrimSpace(string(raw)), nil
}

func diffSets(want, got map[string]bool) (extra, missing []string) {
	for k := range got {
		if !want[k] {
			extra = append(extra, k)
		}
	}
	for k := range want {
		if !got[k] {
			missing = append(missing, k)
		}
	}
	return
}
