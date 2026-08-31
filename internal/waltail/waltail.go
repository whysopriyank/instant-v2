// Package waltail tails Postgres logical replication (pgoutput) and converts
// triple-table changes into invalidation records. Port surface of v1's
// jdbc/wal.clj + the Java pgoutput decoders, collapsed to a single consumer
// for single-node v2 (docs/reference/02-architecture.md §5: one process owns the slot).
//
// Checkpointing: the confirmed LSN is persisted in tail_state and replayed on
// boot; standby status updates are only sent after the consumer has handled
// the record (invariant 02 §5.2).
package waltail

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

const slotName = "instant_v2_tail"

// Record is one decoded triple-table change.
type Record struct {
	LSN      uint64
	Op       string // "insert" | "update" | "delete"
	AppID    string
	EntityID string
	AttrID   string
}

// Handler consumes records; when it returns nil the record is acknowledged.
type Handler func(ctx context.Context, rec Record) error

// Tailer owns the replication connection lifecycle.
type Tailer struct {
	Pool   *pgxpool.Pool
	Slot   string
	Logger *slog.Logger
}

// EnsurePublication creates the publication + slot if missing. Requires
// wal_level=logical; returns a descriptive error otherwise.
func (t *Tailer) EnsurePublication(ctx context.Context) error {
	var exists bool
	if err := t.Pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='instant_v2_publication')`).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if _, err := t.Pool.Exec(ctx,
			`CREATE PUBLICATION instant_v2_publication FOR TABLE triples`); err != nil && !isDuplicateObject(err) {
			return fmt.Errorf("waltail: publication: %w", err)
		}
	} else {
		// Schema resets drop and recreate the table, silently detaching it
		// from the publication; re-add when missing.
		var n int
		if err := t.Pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_publication_tables
			  WHERE pubname='instant_v2_publication' AND tablename='triples'`).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := t.Pool.Exec(ctx,
				`ALTER PUBLICATION instant_v2_publication ADD TABLE triples`); err != nil && !isDuplicateObject(err) {
				return fmt.Errorf("waltail: publication add table: %w", err)
			}
		}
	}
	conn, err := t.replicationConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, t.SlotName(), "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false})
	if err != nil {
		if isDuplicateObject(err) || isAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("waltail: slot: %w", err)
	}
	return nil
}

func (t *Tailer) replicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(t.config())
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, cfg)
}

var configHolder struct {
	dsn string
}

// SetDSN records the DSN used for replication connections (pgconn needs the raw form).
func SetDSN(dsn string) { configHolder.dsn = dsn }

func (t *Tailer) config() string { return configHolder.dsn }

// iswallevelLogical verifies wal_level; callers surface actionable guidance.
func (t *Tailer) CheckWalLevel(ctx context.Context) error {
	var level string
	if err := t.Pool.QueryRow(ctx, `SHOW wal_level`).Scan(&level); err != nil {
		return err
	}
	if level != "logical" {
		return fmt.Errorf("waltail: wal_level=%q; logical replication requires wal_level=logical "+
			"(single-instance deployments can rely on direct post-commit notification instead)", level)
	}
	return nil
}

// Run blocks until ctx is cancelled, streaming changes into h with reconnect +
// exponential backoff. Checkpoint resumes from the last confirmed LSN.
func (t *Tailer) Run(ctx context.Context, checkpoint *Checkpoint, h Handler) error {
	log := t.Logger
	if log == nil {
		log = slog.Default()
	}
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := t.streamOnce(ctx, checkpoint, h)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Error("waltail: stream error; retrying", "err", err, "backoff", backoff)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

func (t *Tailer) streamOnce(ctx context.Context, cp *Checkpoint, h Handler) error {
	conn, err := t.replicationConn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()

	startLSN := pglogrepl.LSN(cp.Last())
	if startLSN == 0 {
		// LSN 0 is invalid syntax for START_REPLICATION; begin from the
		// server's current flush position on first boot.
		ident, ierr := pglogrepl.IdentifySystem(ctx, conn)
		if ierr != nil {
			return ierr
		}
		startLSN = ident.XLogPos
	}
	err = pglogrepl.StartReplication(ctx, conn, t.SlotName(), startLSN,
		pglogrepl.StartReplicationOptions{
			// pglogrepl passes each element verbatim into START_REPLICATION's
			// option list; the whole set travels as ONE quoted string.
			PluginArgs: []string{`"proto_version" '1', "publication_names" 'instant_v2_publication'`},
		})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	log := t.Logger
	if log == nil {
		log = slog.Default()
	}
	log.Info("waltail: replication started", "slot", t.SlotName(), "from_lsn", startLSN)

	relations := map[uint32]*pglogrepl.RelationMessageV2{}
	nextStatus := time.Now().Add(10 * time.Second)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		rawMsg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				nextStatus = time.Now().Add(10 * time.Second)
				continue
			}
			return err
		}
		nextStatus = time.Now().Add(10 * time.Second)

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return fmt.Errorf("waltail: server error: %v", errMsg)
		}
		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, perr := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if perr != nil {
				return perr
			}
			if uint64(pkm.ServerWALEnd) > cp.Last() && pkm.ReplyRequested {
				if serr := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
					pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(cp.Last())}); serr != nil {
					return serr
				}
			}
		case pglogrepl.XLogDataByteID:
			xld, perr := pglogrepl.ParseXLogData(copyData.Data[1:])
			if perr != nil {
				return perr
			}
			logical, perr := pglogrepl.ParseV2(xld.WALData, false)
			if perr != nil {
				return perr
			}
			var (
				rec  Record
				have bool
			)
			switch m := logical.(type) {
			case *pglogrepl.RelationMessageV2:
				relations[m.RelationID] = m
			case *pglogrepl.InsertMessageV2:
				rec, have = decodeRow(relations[m.RelationID], m.Tuple, "insert")
				if have {
					rec.LSN = uint64(xld.WALStart)
				}
			case *pglogrepl.UpdateMessageV2:
				rec, have = decodeRow(relations[m.RelationID], m.NewTuple, "update")
				if have {
					rec.LSN = uint64(xld.WALStart)
				}
			case *pglogrepl.DeleteMessageV2:
				rec, have = decodeRow(relations[m.RelationID], m.OldTuple, "delete")
				if have {
					rec.LSN = uint64(xld.WALStart)
				}
			}
			if have {
				if herr := h(ctx, rec); herr != nil {
					return herr
				}
				cp.Confirm(rec.LSN + 1)
			}
		}
	}
}

// SlotName exposes the configured slot.
func (t *Tailer) SlotName() string {
	if t.Slot != "" {
		return t.Slot
	}
	return slotName
}

// decodeRow extracts (app_id, entity_id, attr_id) from a triples tuple.
func decodeRow(rel *pglogrepl.RelationMessageV2, tup *pglogrepl.TupleData, op string) (Record, bool) {
	if rel == nil || tup == nil {
		return Record{}, false
	}
	if rel.RelationName != "triples" {
		return Record{}, false
	}
	vals := map[string]string{}
	for i, col := range rel.Columns {
		if i >= len(tup.Columns) {
			break
		}
		tc := tup.Columns[i]
		if tc.DataType != pglogrepl.TupleDataTypeText && tc.DataType != pglogrepl.TupleDataTypeBinary {
			continue // null / toast-unchanged
		}
		vals[col.Name] = string(tc.Data)
	}
	rec := Record{
		Op:       op,
		AppID:    vals["app_id"],
		EntityID: vals["entity_id"],
		AttrID:   vals["attr_id"],
	}
	if rec.AppID == "" || rec.AttrID == "" {
		return Record{}, false
	}
	return rec, true
}

// Checkpoint is the durable LSN watermark.
type Checkpoint struct {
	db   *sql.DB
	mu   chan struct{} // serializes writers without a full mutex import dance
	last uint64

	dirty       int       // unpersisted advances
	sinceFlush  int       // confirms since last disk write
	lastPersist time.Time // zero until first flush
}

// OpenCheckpoint loads (or creates) the persisted LSN.
func OpenCheckpoint(db *sql.DB) (*Checkpoint, error) {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS tail_state(
		k text PRIMARY KEY, v text NOT NULL)`); err != nil {
		return nil, err
	}
	cp := &Checkpoint{db: db, mu: make(chan struct{}, 1), lastPersist: time.Now()}
	cp.mu <- struct{}{}
	var v string
	err := db.QueryRow(`SELECT v FROM tail_state WHERE k='lsn'`).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = db.Exec(`INSERT INTO tail_state(k,v) VALUES ('lsn','0')`)
		if err != nil {
			return nil, err
		}
	case err != nil:
		return nil, err
	default:
		_, _ = fmt.Sscanf(v, "%d", &cp.last)
	}
	return cp, nil
}

// Last returns the confirmed LSN.
func (c *Checkpoint) Last() uint64 {
	<-c.mu
	defer func() { c.mu <- struct{}{} }()
	return c.last
}

// Confirm advances the in-memory LSN. Disk persistence is coalesced: at
// most one UPDATE per checkpointInterval (2 s) or 500 records, whichever
// first — a per-record UPDATE made every WAL record pay a round-trip and let
// ungraceful restarts replay unbounded windows. Flush() forces persistence
// (graceful shutdown).
func (c *Checkpoint) Confirm(lsn uint64) {
	<-c.mu
	defer func() { c.mu <- struct{}{} }()
	if lsn <= c.last {
		return
	}
	c.last = lsn
	c.dirty++
	c.sinceFlush++
	now := time.Now()
	if c.sinceFlush >= 500 || now.Sub(c.lastPersist) >= checkpointInterval {
		persistLSN(c.db, lsn)
		c.dirty = 0
		c.sinceFlush = 0
		c.lastPersist = now
	}
}

const checkpointInterval = 2 * time.Second

// Flush persists the in-memory LSN if dirty. Safe to call multiple times.
func (c *Checkpoint) Flush() {
	<-c.mu
	defer func() { c.mu <- struct{}{} }()
	if c.dirty == 0 {
		return
	}
	persistLSN(c.db, c.last)
	c.dirty = 0
	c.sinceFlush = 0
	c.lastPersist = time.Now()
}

func persistLSN(db *sql.DB, lsn uint64) {
	_, _ = db.Exec(`UPDATE tail_state SET v=$1 WHERE k='lsn'`, json.Number(fmt.Sprint(lsn)).String())
}

func isDuplicateObject(err error) bool {
	return pgErrCode(err) == "42P07" || pgErrCode(err) == "42710"
}

func isAlreadyExists(err error) bool {
	return pgErrCode(err) == "42710" || pgErrCode(err) == "23505" ||
		errors.Is(err, errors.New("replication slot already exists"))
}

func pgErrCode(err error) string {
	var pgerr interface{ SQLState() string }
	if errors.As(err, &pgerr) {
		return pgerr.SQLState()
	}
	return ""
}
