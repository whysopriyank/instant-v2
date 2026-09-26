package main

import (
	"testing"
	"time"
)

// passEvidence returns a fully-measured PASS baseline for one outcome id.
// Every field is a computed-style value (no verdict literal involved); each
// test below mutates exactly one measured input and expects FAIL.
func passEvidence(id string) outcomeEvidence {
	o := outcomeEvidence{
		ID:               id,
		Ran:              true,
		Precondition:     "test precondition observed",
		PreconditionMet:  true,
		ExpectedSubs:     2,
		ObservedSubs:     2,
		Converged:        true,
		ExitRecorded:     true,
		PortReleased:     true,
		InFlightResolved: true,
		PIDBefore:        100,
		PIDAfter:         100,
		CloseCodes:       []string{"sub0:1006"},
	}
	if id == "postgres-restart" {
		o.RTO = 90 * time.Second
	}
	if id == "drain-moderate" {
		o.ExitSeconds = 12
	}
	return o
}

func passAll() []outcomeEvidence {
	obs := make([]outcomeEvidence, 0, len(RecoveryOutcomeIDs))
	for _, id := range RecoveryOutcomeIDs {
		obs = append(obs, passEvidence(id))
	}
	return obs
}

func TestVerdictRecoveryAllPass(t *testing.T) {
	details, ok := verdictRecovery(passAll())
	if !ok {
		t.Fatalf("want all PASS: %+v", details)
	}
	mrto, _ := details["max_rto_seconds"].(int64)
	mdrain, _ := details["max_drain_seconds"].(int64)
	if mrto != 90 || mdrain != 12 {
		t.Fatalf("budgets must fold from measured RTO/exit: %+v", details)
	}
	outcomes, _ := details["outcomes"].([]map[string]any)
	if len(outcomes) != 7 {
		t.Fatalf("want 7 outcomes, got %d", len(outcomes))
	}
}

func failID(t *testing.T, obs []outcomeEvidence, id string) {
	t.Helper()
	details, ok := verdictRecovery(obs)
	if ok {
		t.Fatalf("%s must FAIL the record", id)
	}
	found := false
	for _, o := range details["outcomes"].([]map[string]any) {
		if o["id"] == id && o["result"] == "FAIL" {
			found = true
		}
	}
	if !found {
		t.Fatalf("%s must be the failing outcome: %+v", id, details)
	}
}

func TestVerdictPreconditionNotReachedFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "crash-before-commit" {
			obs[i].PreconditionMet = false
			obs[i].Precondition = "un-acked tx never outstanding within bound"
		}
	}
	failID(t, obs, "crash-before-commit")
}

func TestVerdictSubscriberDivergedFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "crash-after-commit" {
			obs[i].Converged = false
			obs[i].DivergedSubs = []string{"sub100 missing=[pg-x] extra=[]"}
		}
	}
	failID(t, obs, "crash-after-commit")
}

func TestVerdictPartialMultiTripleTxFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "crash-before-commit" {
			obs[i].PartialTxIDs = []string{"crash-before-commit-w0-3"}
		}
	}
	failID(t, obs, "crash-before-commit")
}

func TestVerdictAckedButMissingFromLedgerFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "crash-during-publication" {
			obs[i].AckedMissing = []string{"crash-during-publication-w1-9"}
		}
	}
	failID(t, obs, "crash-during-publication")
}

func TestVerdictDrain31sFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "drain-saturated" {
			obs[i].ExitSeconds = 31
		}
	}
	failID(t, obs, "drain-saturated")
}

func TestVerdictDrainUnresolvedInflightFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "drain-moderate" {
			obs[i].InFlightResolved = false
			obs[i].UnresolvedTxIDs = []string{"drain-moderate-w0-44"}
		}
	}
	failID(t, obs, "drain-moderate")
}

func TestVerdictPostgresPIDChangedFails(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "postgres-restart" {
			obs[i].PIDAfter = 999 // instantd restarted: not the same process
		}
	}
	failID(t, obs, "postgres-restart")
}

func TestVerdictMissingOutcomeFails(t *testing.T) {
	obs := passAll()[:6] // postgres-restart never ran
	failID(t, obs, "postgres-restart")
}

func TestVerdictRTOBudgetEnforced(t *testing.T) {
	obs := passAll()
	for i := range obs {
		if obs[i].ID == "postgres-restart" {
			obs[i].RTO = 3601 * time.Second
		}
	}
	failID(t, obs, "postgres-restart")
}

func TestVerdictUnknownIDIgnored(t *testing.T) {
	obs := append(passAll(), outcomeEvidence{ID: "bogus", Ran: true})
	details, ok := verdictRecovery(obs)
	if !ok {
		t.Fatal("unknown extra observation must not fail the seven required outcomes")
	}
	if len(details["outcomes"].([]map[string]any)) != 7 {
		t.Fatal("details must carry exactly the 7 required outcomes")
	}
}

// TestLedgerSettleAccountsEveryTx pins the ledger state machine with fakes:
// submit → ack settles once; double-settle is ignored; outstanding tracks
// the true in-flight count (the saturated precondition reads it).
func TestLedgerSettleAccountsEveryTx(t *testing.T) {
	l := newTxLedger()
	l.submit(&txRecord{ClientEventID: "a", Disposition: txUnknown})
	l.submit(&txRecord{ClientEventID: "b", Disposition: txUnknown})
	if got := l.pendingCount(); got != 2 {
		t.Fatalf("pending = %d, want 2", got)
	}
	l.settle("a", "101", "", time.Now())
	if got := l.pendingCount(); got != 1 {
		t.Fatalf("pending = %d, want 1", got)
	}
	l.settle("a", "101", "", time.Now()) // duplicate ack: ignored
	if got := l.pendingCount(); got != 1 {
		t.Fatalf("duplicate settle changed pending to %d", got)
	}
	l.settle("b", "", "socket-close: 1006", time.Now())
	if got := l.pendingCount(); got != 0 {
		t.Fatalf("pending = %d, want 0", got)
	}
	if got := l.maxOutstandingCount(); got != 2 {
		t.Fatalf("maxOutstanding = %d, want 2", got)
	}
	snap := l.snapshot()
	if snap[0].Disposition != txAcked || snap[0].ServerTxID != "101" {
		t.Fatalf("ack not recorded: %+v", snap[0])
	}
	if snap[1].Disposition != txError {
		t.Fatalf("error not recorded: %+v", snap[1])
	}
}

// TestCompareValueSetsConvergence pins the oracle comparison with fakes:
// equality passes; a missing or phantom value fails with the exact diff.
func TestCompareValueSetsConvergence(t *testing.T) {
	oracle := []string{"v1", "v2"}
	if missing, extra := compareValueSets([]string{"v1", "v2"}, oracle); len(missing) != 0 || len(extra) != 0 {
		t.Fatalf("equal sets must converge: %v %v", missing, extra)
	}
	if missing, _ := compareValueSets([]string{"v1"}, oracle); len(missing) != 1 || missing[0] != "v2" {
		t.Fatalf("diverged subscriber must name the missing value: %v", missing)
	}
	if _, extra := compareValueSets([]string{"v1", "v2", "ghost"}, oracle); len(extra) != 1 || extra[0] != "ghost" {
		t.Fatalf("phantom value must be reported: %v", extra)
	}
}

// TestSweepUniverse pins the universe sweep (must terminate and collect
// every ledger triple value; a self-recursive edit crashed the lane here).
func TestSweepUniverse(t *testing.T) {
	l := newTxLedger()
	l.submit(&txRecord{ClientEventID: "a", Disposition: txAcked, Triples: []txTriple{{Entity: "e1", Attr: "a", Value: "v1"}}})
	l.submit(&txRecord{ClientEventID: "b", Disposition: txUnknown, Triples: []txTriple{
		{Entity: "e2", Attr: "a", Value: "v2"},
		{Entity: "e3", Attr: "a", Value: "v3"},
		{Entity: "e4", Attr: "a", Value: "v4"},
	}})
	u := map[string]struct{}{}
	sweepUniverse(u, l)
	if len(u) != 4 {
		t.Fatalf("universe = %v, want 4 values", u)
	}
	for _, v := range []string{"v1", "v2", "v3", "v4"} {
		if _, ok := u[v]; !ok {
			t.Fatalf("universe missing %q", v)
		}
	}
}

// TestTxPresenceAtomicity pins the DB-side atomicity check with a fake
// oracle: full presence ok, absence ok, partial counted.
func TestTxPresenceAtomicity(t *testing.T) {
	mk := func(e, a, v string) txTriple { return txTriple{Entity: e, Attr: a, Value: v} }
	oracle := map[string]struct{}{oracleKey(mk("e1", "a", "v1")): {}}
	full := []txTriple{mk("e1", "a", "v1")}
	if got := txPresence(oracle, full); got != 1 {
		t.Fatalf("full presence = %d, want 1", got)
	}
	multi := []txTriple{mk("e1", "a", "v1"), mk("e2", "a", "v2"), mk("e3", "a", "v3")}
	if got := txPresence(oracle, multi); got != 1 {
		t.Fatalf("partial multi-triple presence = %d, want 1 (partial, must FAIL)", got)
	}
	if got := txPresence(oracle, []txTriple{mk("e9", "a", "v9")}); got != 0 {
		t.Fatalf("absent presence = %d, want 0", got)
	}
}
