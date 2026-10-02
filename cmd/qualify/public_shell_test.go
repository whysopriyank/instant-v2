package main

import (
	"encoding/json"
	"github.com/instant-v2/instant-v2/internal/corpus"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicShellGate(t *testing.T) {
	repoSource, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(t.TempDir(), "qualify")
	build := exec.Command("go", "build", "-o", helper, "./cmd/qualify")
	build.Dir = repoSource
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build verifier: %v %s", err, out)
	}
	original, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"", "no-parity", "no-parity-unapproved", "missing-restore", "wrong-image", "unapproved-performance", "changed-fixture"} {
		t.Run(mutation, func(t *testing.T) {
			requirePublicTestSuccess(t, os.Chdir(original))
			root, m, policy := publicManifestFixture(t)
			if strings.HasPrefix(mutation, "no-parity") {
				publicNoParityFixture(t, &m, policy)
				if mutation == "no-parity-unapproved" {
					var p map[string]any
					requirePublicTestSuccess(t, json.Unmarshal(readPublicTestFile(t, policy), &p))
					delete(p, "external_v1_approval")
					requirePublicTestSuccess(t, os.WriteFile(policy, marshalPublicTest(t, p), 0600))
				}
			}
			repo := t.TempDir()
			fakebin := t.TempDir()
			copyFile := func(source, dest string) {
				b, err := os.ReadFile(source)
				if err != nil {
					t.Fatal(err)
				}
				requirePublicTestSuccess(t, os.MkdirAll(filepath.Dir(dest), 0755))
				if err = os.WriteFile(dest, b, 0600); err != nil {
					t.Fatal(err)
				}
			}
			copyFile(filepath.Join(repoSource, "scripts/quality-release-gate.sh"), filepath.Join(repo, "scripts/quality-release-gate.sh"))
			copyFile(filepath.Join(repoSource, "scripts/qualify/build-candidate.sh"), filepath.Join(repo, "scripts/qualify/build-candidate.sh"))
			copyFile(policy, filepath.Join(repo, "docs/plans/next-release/qualification-policy.json"))
			copyFile(filepath.Join(repoSource, "corpus/manifest.json"), filepath.Join(repo, "corpus/manifest.json"))
			corpusManifest, err := corpus.LoadManifest(filepath.Join(repoSource, "corpus"))
			if err != nil {
				t.Fatal(err)
			}
			for _, scenario := range corpusManifest.Scenarios {
				copyFile(filepath.Join(repoSource, "corpus", scenario.Path), filepath.Join(repo, "corpus", scenario.Path))
			}
			requirePublicTestSuccess(t, os.WriteFile(filepath.Join(repo, ".gitignore"), []byte("bin/\n"), 0600))
			git := func(args ...string) string {
				cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
				cmd.Env = append(os.Environ(), "GIT_AUTHOR_DATE=2026-01-01T00:00:00Z", "GIT_COMMITTER_DATE=2026-01-01T00:00:00Z")
				b, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("git: %v %s", err, b)
				}
				return strings.TrimSpace(string(b))
			}
			git("init", "-q")
			git("config", "user.email", "test@example.invalid")
			git("config", "user.name", "test")
			git("add", ".")
			git("commit", "-qm", "candidate")
			sha := git("rev-parse", "HEAD")
			git("tag", "v0.1.0-alpha.1", sha)
			// Bind the test-only retained corpus frames and handoffs to this candidate.
			requirePublicTestSuccess(t, filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if info.Mode().IsRegular() {
					b := readPublicTestFile(t, path)
					b = []byte(strings.ReplaceAll(string(b), m.Candidate.SHA, sha))
					requirePublicTestSuccess(t, os.WriteFile(path, b, 0600))
				}
				return nil
			}))
			m.Candidate.SHA = sha
			for _, path := range m.ExternalRecords {
				b := readPublicTestFile(t, filepath.Join(root, path))
				var r gateRecord
				requirePublicTestSuccess(t, json.Unmarshal(b, &r))
				for i, a := range r.Artifacts {
					r.Artifacts[i], err = HashFileArtifact(root, a.Path)
					if err != nil {
						t.Fatal(err)
					}
				}
				b = marshalPublicTest(t, r)
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, path), b, 0600))
			}
			for i, h := range m.Handoffs {
				m.Handoffs[i].CandidateSHA = sha
				a := hashPublicTestArtifact(t, root, h.Path)
				m.Handoffs[i].SHA256 = a.SHA256
				m.Handoffs[i].SizeBytes = a.SizeBytes
			}
			switch mutation {
			case "missing-restore":
				delete(m.ExternalRecords, "restore")
			case "wrong-image":
				m.Candidate.ImageDigest = "sha256:" + strings.Repeat("0", 64)
			case "unapproved-performance":
				m.Lanes["performance"] = "artifact"
			case "changed-fixture":
				requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, "candidate/fixture.json"), []byte("changed"), 0600))
			}
			b := marshalPublicTest(t, m)
			requirePublicTestSuccess(t, os.WriteFile(filepath.Join(root, "manifest.json"), b, 0600))
			// Only command execution is stubbed; the production gate runs the actual
			// newly compiled verifier against immutable evidence and candidate policy.
			goStub := `#!/usr/bin/env bash
set -euo pipefail
if [[ "${@: -1}" == ./cmd/qualify ]]; then
 [[ -f docs/plans/next-release/qualification-policy.json ]] || exit 91
 args=("$@");for ((i=0;i<${#args[@]};i++));do if [[ ${args[i]} == -o ]];then cp "$PUBLIC_TEST_VERIFIER" "${args[i+1]}";exit 0;fi;done
 exit 93
else
 args=("$@");for ((i=0;i<${#args[@]};i++));do if [[ ${args[i]} == -o ]];then cp "$PUBLIC_TEST_BINARY" "${args[i+1]}";exit 0;fi;done
 exit 92
fi
`
			makeStub := `#!/usr/bin/env bash
set -euo pipefail
target=${@: -1}
case "$target" in test-unit|bench-acceptance|test-integration|test-contract)
 printf '{"Action":"pass","Package":"example/unit","Test":"TestSelected"}\n'
 if [[ $target == test-contract ]];then
 printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration"}\n'
 printf '{"Action":"pass","Package":"github.com/instant-v2/instant-v2/internal/corpus","Test":"TestCorpusReplayIntegration/00-smoke"}\n'
 fi;;esac
`
			requirePublicTestSuccess(t, os.WriteFile(filepath.Join(fakebin, "go"), []byte(goStub), 0755))
			requirePublicTestSuccess(t, os.WriteFile(filepath.Join(fakebin, "make"), []byte(makeStub), 0755))
			cmd := exec.Command("bash", filepath.Join(repo, "scripts/quality-release-gate.sh"))
			cmd.Env = append(os.Environ(), "PATH="+fakebin+":"+os.Getenv("PATH"), "RELEASE_CANDIDATE_SHA="+sha, "RELEASE_CAMPAIGN_ID="+m.CampaignID, "RELEASE_GATE_MANIFEST="+filepath.Join(root, "manifest.json"), "DATABASE_URL=postgres://qualification-test.invalid/owned", "PUBLIC_TEST_VERIFIER="+helper, "PUBLIC_TEST_BINARY="+filepath.Join(root, m.Candidate.Binary.Path))
			output, err := cmd.CombinedOutput()
			valid := mutation == "" || mutation == "no-parity"
			if valid != (err == nil) {
				t.Fatalf("public shell %q result %v\n%s", mutation, err, output)
			}
			if valid && !strings.Contains(string(output), "selected single-node-public-alpha checks passed") {
				t.Fatalf("public acceptance marker missing: %s", output)
			}
		})
	}
	requirePublicTestSuccess(t, os.Chdir(original))
}
