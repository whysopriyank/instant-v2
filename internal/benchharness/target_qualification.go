package benchharness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Prepare provisions/seeds the target through the caller-owned callback and
// then runs the independent qualification gate. It does not reset anything;
// destructive database setup remains in soaksetup's guarded owner.
func (d *TargetDriver) Prepare(ctx context.Context) (TargetQualification, error) {
	if err := d.Provision(ctx); err != nil {
		return TargetQualification{}, err
	}
	return d.Qualify(ctx)
}

func (d *TargetDriver) Qualify(ctx context.Context) (q TargetQualification, retErr error) {
	return d.qualify(ctx, nil)
}

// VerifyMetadata performs only the non-mutating metadata/identity check used
// after a qualification probe has been cleaned up. It deliberately does not
// open sessions or execute the live refresh probe.
func (d *TargetDriver) VerifyMetadata(ctx context.Context) error {
	if d == nil {
		return fmt.Errorf("nil target driver")
	}
	if d.cfg.MetadataProbe == nil {
		return &UnsupportedTargetError{Check: "metadata", Reason: "no independent metadata probe configured"}
	}
	metadata, err := d.cfg.MetadataProbe(ctx)
	if err != nil {
		return err
	}
	return compareMetadata(d.cfg, metadata, func(string, bool, string) {})
}

func (d *TargetDriver) qualify(ctx context.Context, evidence *evidenceCollector) (q TargetQualification, retErr error) {
	q = TargetQualification{TargetID: d.cfg.ID, Kind: d.cfg.Kind, Transport: d.cfg.Transport, Revision: d.cfg.Revision, DatabaseName: d.cfg.DatabaseName, PostgresVersion: d.cfg.PostgresVersion, InvalidationMode: d.cfg.InvalidationMode, OutputPlugin: d.cfg.OutputPlugin, Checks: map[string]QualificationCheck{}, StartedAt: time.Now()}
	defer func() { q.FinishedAt = time.Now() }()
	mark := func(name string, passed bool, detail string) {
		q.Checks[name] = QualificationCheck{Passed: passed, Details: detail}
		if !passed && q.Failure == "" {
			q.Failure = name + ": " + detail
		}
	}
	if err := validateTargetConfig(d.cfg); err != nil {
		mark("config", false, err.Error())
		return q, err
	}
	if err := d.checkHealth(ctx); err != nil {
		mark("health", false, err.Error())
		return q, err
	}
	mark("health", true, "target health endpoint returned ready")
	if d.cfg.MetadataProbe == nil {
		detail := "no independent metadata probe configured"
		mark("metadata", false, detail)
		q.Unsupported = true
		err := &UnsupportedTargetError{Check: "metadata", Reason: detail}
		return q, err
	}
	metadata, err := d.cfg.MetadataProbe(ctx)
	if err != nil {
		mark("metadata", false, err.Error())
		return q, err
	}
	if err := compareMetadata(d.cfg, metadata, mark); err != nil {
		return q, err
	}
	if d.cfg.AdminBaseURL != "" {
		if err := d.checkAdminRoutes(ctx, mark); err != nil {
			return q, err
		}
	} else {
		mark("admin_routes", true, "not applicable: no admin control base configured")
	}
	if d.cfg.Probe.Validate == nil || d.cfg.QueryBuilder == nil || d.cfg.TransactionBuilder == nil {
		detail := "four-subscriber live-refresh probe requires query, transaction, and semantic validation callbacks"
		mark("live_refresh", false, detail)
		q.Unsupported = true
		err := &UnsupportedTargetError{Check: "live_refresh", Reason: detail}
		return q, err
	}
	if err := d.liveRefreshProbe(ctx, d.cfg.Probe, evidence); err != nil {
		mark("live_refresh", false, err.Error())
		return q, err
	}
	mark("live_refresh", true, "four subscribers observed a validated post-transaction refresh")
	q.Passed = true
	return q, nil
}

func compareMetadata(cfg TargetConfig, got TargetMetadata, mark func(string, bool, string)) error {
	checks := []struct {
		name, want, actual string
	}{
		{"revision", cfg.Revision, got.Revision},
		{"database", cfg.DatabaseName, got.DatabaseName},
		{"postgres_version", cfg.PostgresVersion, got.PostgresVersion},
		{"invalidation_mode", cfg.InvalidationMode, got.InvalidationMode},
	}
	if cfg.Kind == TargetV1 {
		checks = append(checks, struct{ name, want, actual string }{"output_plugin", "wal2json", got.OutputPlugin})
	}
	for _, check := range checks {
		if check.actual == "" {
			mark(check.name, false, "metadata probe returned no value")
			return &UnsupportedTargetError{Check: check.name, Reason: "independent target evidence is unavailable"}
		}
		if check.actual != check.want {
			mark(check.name, false, fmt.Sprintf("configured %q, observed %q", check.want, check.actual))
			return fmt.Errorf("target %s mismatch: configured %q, observed %q", check.name, check.want, check.actual)
		}
		mark(check.name, true, check.actual)
	}
	if cfg.IdentityAttribute.ID != "" {
		if err := compareIdentityAttribute(cfg.IdentityAttribute, got.IdentityAttribute, mark); err != nil {
			return err
		}
	}
	if cfg.DirtyTreeHash != "" || got.DirtyTreeHash != "" {
		mark("clean_revision", false, "dirty product tree is not claim-eligible")
		return fmt.Errorf("target revision is dirty")
	}
	mark("clean_revision", true, "clean revision evidence")
	return nil
}

func compareIdentityAttribute(want, got IdentityAttributeMetadata, mark func(string, bool, string)) error {
	if got.ID == "" {
		mark("identity_attribute", false, "metadata probe returned no identity attribute")
		return &UnsupportedTargetError{Check: "identity_attribute", Reason: "independent identity catalog evidence is unavailable"}
	}
	stringsChecks := []struct {
		name, want, actual string
	}{
		{"identity_attribute_id", want.ID, got.ID},
		{"identity_attribute_entity_type", want.EntityType, got.EntityType},
		{"identity_attribute_label", want.Label, got.Label},
		{"identity_attribute_value_type", want.ValueType, got.ValueType},
		{"identity_attribute_cardinality", want.Cardinality, got.Cardinality},
	}
	for _, check := range stringsChecks {
		if check.actual != check.want {
			mark("identity_attribute", false, fmt.Sprintf("%s configured %q, observed %q", check.name, check.want, check.actual))
			return fmt.Errorf("identity attribute %s mismatch: configured %q, observed %q", check.name, check.want, check.actual)
		}
	}
	boolChecks := []struct {
		name         string
		want, actual bool
	}{
		{"unique", want.Unique, got.Unique},
		{"indexed", want.Indexed, got.Indexed},
		{"required", want.Required, got.Required},
		{"primary", want.Primary, got.Primary},
		{"identity", want.Identity, got.Identity},
	}
	for _, check := range boolChecks {
		if check.actual != check.want {
			mark("identity_attribute", false, fmt.Sprintf("identity attribute %s configured %t, observed %t", check.name, check.want, check.actual))
			return fmt.Errorf("identity attribute %s mismatch: configured %t, observed %t", check.name, check.want, check.actual)
		}
	}
	mark("identity_attribute", true, "catalog identity metadata matches signed fixture contract")
	return nil
}

func (d *TargetDriver) checkHealth(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.HealthURL, nil)
	if err != nil {
		return err
	}
	req.Header = d.cfg.Headers.Clone()
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }() // Health is determined by the response, not cleanup.
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("health status %s", resp.Status)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return err
	}
	var body struct {
		OK *bool `json:"ok"`
	}
	if len(bytes.TrimSpace(b)) > 0 && json.Unmarshal(b, &body) == nil && body.OK != nil && !*body.OK {
		return fmt.Errorf("health endpoint reported not ready")
	}
	return nil
}

func (d *TargetDriver) checkAdminRoutes(ctx context.Context, mark func(string, bool, string)) error {
	for _, route := range []string{"/query", "/transact", "/subscribe-query"} {
		routeURL, err := adminRouteURL(d.cfg.AdminBaseURL, route)
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, routeURL, strings.NewReader(`{}`))
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		req.Header = d.cfg.Headers.Clone()
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		req.Header.Set("Content-Type", "application/json")
		if d.cfg.AdminToken != "" {
			req.Header.Set("Authorization", "Bearer "+d.cfg.AdminToken)
		}
		resp, err := d.client.Do(req)
		if err != nil {
			mark("admin_routes", false, err.Error())
			return err
		}
		_ = resp.Body.Close() // This probe checks route existence from the status only.
		if resp.StatusCode == http.StatusNotFound {
			mark("admin_routes", false, route+" returned 404")
			return fmt.Errorf("admin route %s is unavailable", route)
		}
	}
	mark("admin_routes", true, "query/transact/subscribe-query routes responded")
	return nil
}

func adminRouteURL(base, route string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Hostname() == "" {
		return "", fmt.Errorf("invalid admin base URL %q", base)
	}
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		path = "/admin"
	} else if path != "/admin" && !strings.HasSuffix(path, "/admin") {
		path += "/admin"
	}
	u.Path = path + route
	u.RawPath = ""
	return u.String(), nil
}

func (d *TargetDriver) liveRefreshProbe(ctx context.Context, probe LiveRefreshProbe, evidence *evidenceCollector) error {
	timeout := probe.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	clients := make([]*TargetSession, 0, 4)
	defer func() { closeTargetSessions(clients) }()
	var initial Refresh
	for i := 0; i < 4; i++ {
		client, err := d.openSession(pctx, fmt.Sprintf("qualification-%d", i), evidence)
		if err != nil {
			return err
		}
		refresh, err := client.Subscribe(pctx, probe.Query)
		if err != nil {
			_ = client.Close()
			return err
		}
		if i == 0 {
			initial = refresh
		}
		clients = append(clients, client)
	}
	ack, err := clients[0].Transact(pctx, probe.Mutation)
	if err != nil {
		return err
	}
	if _, ok := saturationOrder(ack); !ok {
		return &UnsupportedTargetError{Check: "transaction_order", Reason: "target did not expose a numeric server tx-id or processed-tx-id"}
	}
	for _, client := range clients {
		for {
			ev, err := client.await(pctx, func(ev SessionEvent) bool { return ev.Op == "refresh-ok" || ev.Op == "refresh-ok-delta" })
			if err != nil {
				return err
			}
			ref, err := client.decodeRefresh(ev, probe.Query.ID)
			if err != nil {
				return err
			}
			if ref.Kind == RefreshNoop {
				continue
			}
			if err := probe.Validate(initial, ref); err != nil {
				return err
			}
			break
		}
	}
	return nil
}
