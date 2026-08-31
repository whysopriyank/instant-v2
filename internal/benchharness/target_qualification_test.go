package benchharness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestCompareMetadataRejectsWeakIdentityCatalog(t *testing.T) {
	want := IdentityAttributeMetadata{ID: "00000000-0000-0000-0000-000000000100", EntityType: "bench_items", Label: "id", ValueType: "blob", Cardinality: "one", Unique: true, Indexed: true, Required: true, Primary: true, Identity: true}
	cfg := TargetConfig{Kind: TargetV1, Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: want}
	for _, tc := range []struct {
		name   string
		mutate func(*IdentityAttributeMetadata)
		want   string
	}{
		{name: "missing", mutate: func(got *IdentityAttributeMetadata) { *got = IdentityAttributeMetadata{} }, want: "identity"},
		{name: "unindexed", mutate: func(got *IdentityAttributeMetadata) { got.Indexed = false }, want: "indexed"},
		{name: "optional", mutate: func(got *IdentityAttributeMetadata) { got.Required = false }, want: "required"},
		{name: "required without primary", mutate: func(got *IdentityAttributeMetadata) { got.Primary = false }, want: "primary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := want
			tc.mutate(&got)
			err := compareMetadata(cfg, TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: got}, func(string, bool, string) {})
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tc.want)) {
				t.Fatalf("weak identity catalog was accepted: %v", err)
			}
		})
	}
	if err := compareMetadata(cfg, TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_v1", PostgresVersion: "17", InvalidationMode: "logical", OutputPlugin: "wal2json", IdentityAttribute: want}, func(string, bool, string) {}); err != nil {
		t.Fatalf("exact identity catalog was rejected: %v", err)
	}
}

func TestTargetDriverQualifyRunsActualSessionProbe(t *testing.T) {
	health := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer health.Close()
	app := "00000000-0000-0000-0000-000000000001"
	dialer := &scriptedDialer{}
	driver, err := NewTargetDriver(TargetConfig{
		ID: "v2", Kind: TargetV2, Transport: TransportWebSocket,
		SessionURL: "ws://127.0.0.1:1/runtime/session", HealthURL: health.URL,
		AppID: app, Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit",
		MetadataProbe: func(context.Context) (TargetMetadata, error) {
			return TargetMetadata{Revision: "rev", DatabaseName: "instant_bench_probe", PostgresVersion: "17.11", InvalidationMode: "post-commit"}, nil
		},
		Provisioner: func(context.Context) error { return nil },
		Dialer:      dialer, QueryBuilder: func(Query) (any, error) { return map[string]any{"todos": map[string]any{}}, nil },
		TransactionBuilder: func(Mutation) ([]any, error) { return []any{[]any{"add-triple", "e", "a", "marker"}}, nil },
		Probe: LiveRefreshProbe{Query: Query{ID: "q"}, Mutation: Mutation{EventID: "e"}, Validate: func(initial, refreshed Refresh) error {
			if initial.Full == nil || refreshed.Full == nil {
				return errors.New("missing full refresh")
			}
			if mustDigest(*initial.Full) == mustDigest(*refreshed.Full) {
				return errors.New("refresh did not change semantic state")
			}
			return nil
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	qualification, err := driver.Qualify(context.Background())
	if err != nil || !qualification.Passed || !qualification.Checks["live_refresh"].Passed {
		t.Fatalf("qualification failed: %#v / %v", qualification, err)
	}
}

func TestCheckAdminRoutesUsesCanonicalAdminPrefix(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("admin probe used %s instead of POST", r.Method)
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	want := []string{"/admin/query", "/admin/transact", "/admin/subscribe-query"}
	for _, base := range []string{server.URL, server.URL + "/admin/"} {
		mu.Lock()
		paths = nil
		mu.Unlock()
		driver := &TargetDriver{cfg: TargetConfig{AdminBaseURL: base}, client: server.Client()}
		var check QualificationCheck
		if err := driver.checkAdminRoutes(context.Background(), func(name string, passed bool, details string) {
			if name == "admin_routes" {
				check = QualificationCheck{Passed: passed, Details: details}
			}
		}); err != nil || !check.Passed {
			t.Fatalf("admin route probe failed for base %q: %v / %#v", base, err, check)
		}
		mu.Lock()
		got := append([]string(nil), paths...)
		mu.Unlock()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("admin route probe for base %q used %v, want %v", base, got, want)
		}
	}
}

func TestFourClientSemanticProbeHasFrozenContractDefaults(t *testing.T) {
	probe := NewFourClientSemanticProbe(Query{ID: "q"}, Mutation{EventID: "e"}, func(Refresh, Refresh) error { return nil })
	if probe.Query.ID != "q" || probe.Mutation.EventID != "e" || probe.Timeout <= 0 || probe.Validate == nil {
		t.Fatalf("invalid standard probe: %#v", probe)
	}
}
