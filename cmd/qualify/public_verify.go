package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"
)

// verify-public runs only from the committed candidate snapshot selected by
// the shell gate. It emits the exact policy selection after all evidence checks.
func runVerifyPublic(args []string) int {
	fs := flag.NewFlagSet("verify-public", flag.ContinueOnError)
	root := fs.String("evidence-root", "", "immutable evidence root")
	manifest := fs.String("manifest", "", "relative manifest path")
	policyPath := fs.String("policy", "", "candidate-owned policy path")
	candidate := fs.String("candidate", "", "candidate SHA")
	campaign := fs.String("campaign", "", "campaign ID")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	fail := func(err error) int { fmt.Fprintln(os.Stderr, "verify-public:", err); return 1 }
	p, err := readPublicPolicy(*policyPath)
	if err != nil {
		return fail(err)
	}
	if err = checkRelativePath(*manifest); err != nil {
		return fail(err)
	}
	b, err := os.ReadFile(filepath.Join(*root, filepath.FromSlash(*manifest)))
	if err != nil {
		return fail(err)
	}
	var m gateManifest
	if err = strictPublicJSON(b, &m); err != nil {
		return fail(err)
	}
	if m.Profile != PublicProfile || m.Lanes["performance"] != p.Performance || m.Lanes["external_v1"] != p.ExternalV1 {
		return fail(fmt.Errorf("manifest contradicts approved public policy"))
	}
	if err = validateManifest(*root, m, *candidate, *campaign); err != nil {
		return fail(err)
	}
	start, _ := time.Parse(time.RFC3339, m.CampaignStartedAt)
	now := time.Now()
	for _, name := range sortedRecordNames(m.ExternalRecords) {
		r, err := publicIdentityMatches(*root, m, name)
		if err != nil {
			return fail(err)
		}
		a, ea := time.Parse(time.RFC3339, r.StartedAt)
		z, ez := time.Parse(time.RFC3339, r.FinishedAt)
		if ea != nil || ez != nil || a.Before(start) || z.Before(a) || z.After(now) {
			return fail(fmt.Errorf("record %s timestamps invalid", name))
		}
	}
	for _, h := range m.Handoffs {
		b, err := os.ReadFile(filepath.Join(*root, filepath.FromSlash(h.Path)))
		if err != nil {
			return fail(err)
		}
		var content struct {
			Packet       string `json:"packet"`
			State        string `json:"state"`
			CandidateSHA string `json:"candidate_sha"`
			CampaignID   string `json:"campaign_id"`
			LedgerRef    string `json:"ledger_ref"`
			FinishedAt   string `json:"finished_at"`
			Record       string `json:"record"`
		}
		if err = strictPublicJSON(b, &content); err != nil {
			return fail(err)
		}
		if content.Packet != h.Packet || content.State != h.State || content.CandidateSHA != h.CandidateSHA || content.CampaignID != h.CampaignID || content.FinishedAt != h.FinishedAt || content.LedgerRef == "" || content.Record != recordPathForPublicPacket(h.Packet, m.ExternalRecords) {
			return fail(fmt.Errorf("handoff %s content/binding mismatch", h.Packet))
		}
	}
	for _, a := range []ArtifactRef{m.Candidate.Binary, m.Candidate.Configuration} {
		got, err := publicArtifact(*root, a.Path)
		if err != nil || !reflect.DeepEqual(got, a) {
			return fail(fmt.Errorf("candidate artifact hash/size mismatch"))
		}
	}
	packets, lanes, _, _ := selectPublicPerformance(p.Performance, p.ExternalV1)
	if err = json.NewEncoder(os.Stdout).Encode(map[string]any{"release_version": p.ReleaseVersion, "decision_id": p.DecisionID, "profile": p.Profile, "lanes": lanes, "packets": packets}); err != nil {
		return fail(err)
	}
	return 0
}
