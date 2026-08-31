package benchrun

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// These snapshots protect the pre-extraction offline report, including every
// metric and claim reason. The manifest hash is checked separately because the
// runner records real execution timestamps in its manifest.
func TestOfflineReportCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		family     string
		triad      bool
		fail       int
		summarySHA string
		reportSHA  string
	}{
		{name: "pair_latency", family: "H-append", summarySHA: "5622f6c022a0979554eb4e57af2fe04f6aa2781b7d58020cf9d5f140550e94c5", reportSHA: "72fdb1d39fb4dfef04e92916b7eb2770cf7ba1da4835e11a97838c9d3955f334"},
		{name: "pair_throughput", family: "T", summarySHA: "4715e748ef61ab98c924632c46628e627ce5ab2ab468bc5c494d047457c6e563", reportSHA: "68bdce090b8e0348325a4a70485315c378060d9dd48f0d53b56d8f08baaf4c8b"},
		{name: "pair_failure", family: "H-append", fail: 3, summarySHA: "011ca729e1b7b1fb9a6fe06ae83021aeda694628f5aa66906d8e6b53c9800c59", reportSHA: "8d53853f0f01fb9c87dcbc647dde08796bad2d52671feeaffbcfcf779ab1baa4"},
		{name: "triad_latency", family: "H-append", triad: true, summarySHA: "8d42636f33209779ae268eb3f97e86a80a9f989b6eecbd1d3bbe02acc8af9ec6", reportSHA: "29875377a735cd76fe1846db9085b795f7c90b89ba2426bbcd33d22c50c6bf9b"},
		{name: "triad_throughput", family: "T", triad: true, summarySHA: "b787b7d349b7f52796b64b68aa04ac7e3d48dbafc64b2c5f3e095b386e8db9bf", reportSHA: "0d6df3bafab7766524c47ce34fea7d16df7833ed3870f100a9c09a1124cf9233"},
		{name: "triad_failure", family: "H-append", triad: true, fail: 3, summarySHA: "ac3b6bccc88c24a9f44e7a23c40c55c998f3009ec7720e9f4fd0d9bff81c82d3", reportSHA: "505ef443ef803c9f914fec84f561b7dd6c02182fd6b822ad1351245a4c81f4d8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := runCompatibilityBundle(t, tc.family, tc.triad, tc.fail)
			summary, err := ReportFromArtifacts(root)
			if err != nil {
				t.Fatal(err)
			}
			manifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			if got, want := summary.InputHashes["manifest.json"], compatibilitySHA(manifest); got != want {
				t.Fatalf("manifest input hash = %q, want %q", got, want)
			}
			summary.InputHashes = nil
			data, err := json.Marshal(summary)
			if err != nil {
				t.Fatal(err)
			}
			if got := compatibilitySHA(data); got != tc.summarySHA {
				t.Fatalf("summary bytes changed: digest=%s, want %s\n%s", got, tc.summarySHA, data)
			}
			if got := compatibilitySHA([]byte(RenderMarkdown(summary))); got != tc.reportSHA {
				t.Fatalf("report bytes changed: digest=%s, want %s", got, tc.reportSHA)
			}
		})
	}
}

func compatibilitySHA(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestArtifactFormatCompatibility(t *testing.T) {
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{
		"manifest.json": Manifest{SchemaVersion: SchemaVersion, BundleID: "compat", PairID: "p", Family: "H", SubscriberScale: 300, Seed: 17, RunOrder: []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}, StartedAt: time.Unix(1, 0).UTC()},
		"data.json":     map[string]any{"seed": int64(9007199254740993), "token": "private-value", "values": []int{2, 1}},
	} {
		if err := writer.WriteJSON(name, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.WriteJSONL("rows.jsonl", []map[string]any{{"password": "private-value", "count": 3}, {"count": 0}}); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finalize(); err != nil {
		t.Fatal(err)
	}
	if err := VerifyChecksums(root); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"manifest.json":    "b02b573beafe27a42bbe3fd90f762c4cce8b7a4d4b9d7e0cdb4207e778ac0ba6",
		"data.json":        "d8ad27567639a35964e12c8f7fa2eb0cc9ab5cf52275aa3607b3ae52c77b032c",
		"rows.jsonl":       "cb3362fe227cad8c11ab91d75264abc241957d042fddf19b1817c368f349dc4d",
		"raw-index.json":   "54be60808678deace81c3a1b4d44a0b6cabac2afd2bd1b7663ef9e312f39b642",
		"checksums.sha256": "05486b85c07d45065804d6d06e5b5f59b11bb892ccc1d0f3515cfb36e52d06fd",
	} {
		data, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		if got := compatibilitySHA(data); got != want {
			t.Fatalf("%s bytes changed: digest=%s, want %s\n%s", name, got, want, data)
		}
	}
	contentRoot, err := ComputeContentRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	if want := "39e39cd4f583f5799b98d0118da450dc49be878da53589bd4e8ac08c80aebfde"; contentRoot != want {
		t.Fatalf("content root changed: %s, want %s", contentRoot, want)
	}
}

func runCompatibilityBundle(t *testing.T, family string, triad bool, fail int) string {
	t.Helper()
	t.Setenv(TrustedApprovalPublicKeyEnv, "")
	root := t.TempDir()
	writer, err := NewArtifactWriter(root, 1<<28)
	if err != nil {
		t.Fatal(err)
	}
	targets := []Target{
		{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "baseline-sha"},
		{SchemaVersion: SchemaVersion, ID: "v2", Role: "v2_current", Revision: "candidate-sha"},
	}
	if triad {
		targets = []Target{
			{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1", Revision: "baseline-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_reference", Role: "v2_reference", Revision: "reference-sha"},
			{SchemaVersion: SchemaVersion, ID: "v2_current", Role: "v2_current", Revision: "candidate-sha"},
		}
	}
	runner := PairRunner{
		Writer: writer, Executor: SyntheticExecutor{FailAttempt: fail, FailClass: TargetTimeout},
		Manifest: Manifest{SchemaVersion: SchemaVersion, BundleID: "compatibility", PairID: "compat", Family: family, SubscriberScale: 300, Seed: 17, StartedAt: time.Unix(1, 0).UTC()},
		Plan:     Plan{SchemaVersion: SchemaVersion, Seed: 17, Pairs: 7}, Targets: targets,
	}
	if !triad {
		runner.Manifest.RunOrder = []string{"AB", "BA", "AB", "BA", "AB", "BA", "AB"}
		runner.Manifest.V1SHA = targets[0].Revision
		runner.Manifest.V2SHA = targets[1].Revision
	}
	if _, err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	return root
}

func TestReportValidationOrder(t *testing.T) {
	for _, triad := range []bool{false, true} {
		name := "pair"
		if triad {
			name = "triad"
		}
		t.Run(name, func(t *testing.T) {
			root := runCompatibilityBundle(t, "H-append", triad, 0)
			check := func(verify bool, want string) {
				t.Helper()
				_, err := reportFromArtifacts(root, verify)
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("report error=%v, want %q", err, want)
				}
			}
			write := func(name string, value any) {
				t.Helper()
				data, err := json.Marshal(value)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			// Both inputs are invalid: pair replay diagnoses runs first, while
			// triad replay needs qualified target revisions before loading runs.
			write("runs/compat-01-v1/run.json", Run{SchemaVersion: SchemaVersion, PrimaryClass: "invalid"})
			write("targets/v1.json", Target{SchemaVersion: SchemaVersion, ID: "v1", Role: "v1"})
			want := "invalid failure taxonomy"
			if triad {
				want = "incomplete target qualification: v1"
			}
			check(false, want)

			// Shared evidence is checked before either target/run branch.
			write("environment.json", Environment{SchemaVersion: "invalid"})
			check(false, "invalid environment provenance")

			// Public replay checks integrity before decoding shared evidence,
			// but still diagnoses an invalid manifest before checksum mismatch.
			check(true, "checksum mismatch")
			write("manifest.json", Manifest{SchemaVersion: "invalid"})
			check(true, "unsupported schema version")
		})
	}
}
