package authn

// Transient auth artifacts ($magicCodes, $oauthRedirects, $oauthCodes) carry
// their own TTL, but that TTL is enforced lazily at consume time: an
// expired-but-never-consumed entity stayed in the triple store forever. Since
// POST /runtime/auth/send_magic_code is reachable anonymously per public
// app-id, this converted internet traffic into permanent shared-database
// growth (follow-up audit 2026-08-27, MED-1). SweepExpiredAuthEntities
// deletes such dead entities directly; cmd wires it next to PruneThrottle at
// boot and hourly so retention policy lives beside the lockout-state prune.
//
// Entity anchoring: every artifact kind owns its etype exclusively (all
// labels are same-generation metadata written in one insert), so grouping by
// entity and comparing max(triples.created_at) against the etype's TTL plus a
// grace window is both precise and catches any partial-write stragglers.
//
// Concurrency: consume paths claim entities via atomic DELETE ... RETURNING
// (consumeCode/burn flows). Racing this sweeper can only cost an attacker an
// expired-code rejection, never a live verification: the grace window keeps
// every TTL-fresh row out of the sweep's reach entirely.
//
// Journal bypass is deliberate: these are internal bookkeeping entities, not
// user-visible data (v1 likewise deletes them outside the tx pipeline), so
// skipping RecordTransaction avoids polluting WAL-tail invalidations for
// private namespaces clients cannot subscribe to through default rules.

import (
	"context"
	"time"
)

// sweepGrace extends every artifact TTL by this much before a row becomes
// sweep-eligible. It exists so a code/state consumed concurrently moments
// before its expiry decision can never race deletion mid-verification.
const sweepGrace = time.Hour

// SweepExpiredAuthEntities deletes entities of every transient artifact etype
// whose newest triple predates (TTL + sweepGrace). Returns the number of
// triple rows removed (entity members count individually), across all apps —
// the etypes are fixed system namespaces, so no per-app catalog walk is
// needed and multi-node deployments share correctness by construction.
func (s *Service) SweepExpiredAuthEntities(ctx context.Context) (int64, error) {
	if s.Pool == nil {
		return 0, nil
	}
	ttl := s.CodeTTL
	if ttl <= 0 {
		ttl = DefaultMagicCodeTTL
	}
	kinds := []struct {
		etype     string
		olderThan time.Duration
	}{
		{"$magicCodes", ttl},
		{"$oauthRedirects", oauthStateTTL},
		{"$oauthCodes", oauthCodeTTL},
	}
	var total int64
	for _, k := range kinds {
		n, err := s.sweepEntityType(ctx, k.etype, k.olderThan+sweepGrace)
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, nil
}

// sweepEntityType removes every entity whose triples under etype are all
// older than the cutoff. Server-side now() is used (not the service clock)
// so behavior stays identical across nodes regardless of local skew; tests
// backdate rows directly rather than faking the DB clock.
func (s *Service) sweepEntityType(ctx context.Context, etype string, olderThan time.Duration) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `
WITH candidate AS (
    SELECT t.app_id, t.entity_id, max(t.created_at) AS newest
      FROM triples t
      JOIN attrs a ON a.id = t.attr_id AND a.etype = $1::text
     GROUP BY t.app_id, t.entity_id
), dead AS (
    SELECT app_id, entity_id
      FROM candidate
     WHERE newest < now() - make_interval(secs => $2::int)
)
DELETE FROM triples tr
  USING dead d
 WHERE tr.app_id = d.app_id
   AND tr.entity_id = d.entity_id`,
		etype, int(olderThan.Seconds()))
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
