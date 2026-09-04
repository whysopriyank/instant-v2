package authn

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/instant-v2/instant-v2/internal/platform"
)

func completeSystemAttrs(seed byte) systemAttrs {
	a := systemAttrs{
		tokenHashedToken: [16]byte{seed},
		tokenUser:        [16]byte{seed + 1},
		magicCodeHash:    [16]byte{seed + 2},
		magicCodeEmail:   [16]byte{seed + 3},
		userEmail:        [16]byte{seed + 4},
		userType:         [16]byte{seed + 5},
		userID:           [16]byte{seed + 6},
	}
	a.labels = map[[16]byte]string{
		a.tokenHashedToken: "hashedToken",
		a.tokenUser:        "$user",
		a.magicCodeHash:    "codeHash",
		a.magicCodeEmail:   "email",
		a.userEmail:        "email",
		a.userType:         "type",
		a.userID:           "id",
	}
	return a
}

func TestInvalidationGenerationRejectsStaleAttrPublication(t *testing.T) {
	appID := [16]byte{1}
	svc := &Service{}

	loadVersion := uint64(0)
	svc.InvalidateAttrs(appID)
	if svc.cacheAttrsIfCurrent(appID, loadVersion, systemAttrs{}) {
		t.Fatal("an in-flight load from before invalidation republished stale attrs")
	}
	if svc.cacheAttrsIfCurrent(appID, 1, systemAttrs{}) == false {
		t.Fatal("current-generation attrs were not cached")
	}
}

func TestAttrsRetriesAfterInFlightInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	appID := [16]byte{1}
	svc := &Service{}
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	var calls atomic.Int32
	stale := completeSystemAttrs(2)
	fresh := completeSystemAttrs(12)
	loader := func(ctx context.Context, gotAppID [16]byte) (systemAttrs, error) {
		if gotAppID != appID {
			return systemAttrs{}, errors.New("loader received the wrong app")
		}
		if calls.Add(1) == 1 {
			close(oldStarted)
			select {
			case <-releaseOld:
				return stale, nil
			case <-ctx.Done():
				return systemAttrs{}, ctx.Err()
			}
		}
		return fresh, nil
	}

	type result struct {
		attrs systemAttrs
		err   error
	}
	done := make(chan result, 1)
	go func() {
		a, err := svc.attrsWithLoader(ctx, appID, loader)
		done <- result{attrs: a, err: err}
	}()
	select {
	case <-oldStarted:
	case <-ctx.Done():
		t.Fatal("old load did not reach the barrier")
	}
	svc.InvalidateAttrs(appID)
	close(releaseOld)

	var got result
	select {
	case got = <-done:
	case <-ctx.Done():
		t.Fatal("cache loader did not retry after invalidation")
	}
	if got.err != nil {
		t.Fatalf("attrs: %v", got.err)
	}
	if !reflect.DeepEqual(got.attrs, fresh) {
		t.Fatalf("in-flight load returned stale attrs: got %+v want %+v", got.attrs, fresh)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("loader calls = %d, want old load plus one retry", n)
	}

	// The retry's complete result is the cache hit; no third load is allowed.
	got.attrs, got.err = svc.attrsWithLoader(ctx, appID, func(context.Context, [16]byte) (systemAttrs, error) {
		t.Fatal("cache hit reloaded attrs")
		return systemAttrs{}, nil
	})
	if got.err != nil || !reflect.DeepEqual(got.attrs, fresh) {
		t.Fatalf("cached fresh attrs: got %+v err=%v", got.attrs, got.err)
	}
}

func TestAttrsRejectsIncompleteSnapshotsWithoutPoisoningCache(t *testing.T) {
	base := completeSystemAttrs(30)
	cases := []struct {
		name   string
		mutate func(*systemAttrs)
	}{
		{"missing required id", func(a *systemAttrs) { a.userID = [16]byte{} }},
		{"missing labels", func(a *systemAttrs) { a.labels = nil }},
		{"missing label entry", func(a *systemAttrs) { delete(a.labels, a.userID) }},
		{"mismatched label", func(a *systemAttrs) { a.labels[a.userID] = "wrong" }},
		{"duplicate required ids", func(a *systemAttrs) { a.userID = a.userType }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &Service{}
			bad := base
			bad.labels = cloneLabels(base.labels)
			tc.mutate(&bad)
			var calls atomic.Int32
			loader := func(context.Context, [16]byte) (systemAttrs, error) {
				calls.Add(1)
				return bad, nil
			}
			if _, err := svc.attrsWithLoader(context.Background(), [16]byte{31}, loader); err == nil {
				t.Fatal("invalid nil-error snapshot was accepted")
			}
			if _, err := svc.attrsWithLoader(context.Background(), [16]byte{31}, loader); err == nil {
				t.Fatal("invalid snapshot poisoned the cache")
			}
			if got := calls.Load(); got != 2 {
				t.Fatalf("loader calls = %d, want two uncached attempts", got)
			}
		})
	}
}

func TestValidateSystemAttrRejectsMismatchedExistingSchema(t *testing.T) {
	appID := [16]byte{77}
	et, label := "$users", "email"
	valid := platform.Attr{
		ID:          [16]byte{78},
		AppID:       appID,
		Etype:       &et,
		Label:       &label,
		ValueType:   "blob",
		Cardinality: "one",
		IsUnique:    true,
		IsIndexed:   true,
	}
	spec := systemAttrSpec{
		etype: "$users", label: "email", valueType: "blob", cardinality: "one",
		unique: true, indexed: true,
	}
	if err := validateSystemAttr(appID, valid, spec); err != nil {
		t.Fatalf("valid system attr rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*platform.Attr)
	}{
		{"wrong owner app", func(a *platform.Attr) { a.AppID = [16]byte{79} }},
		{"wrong etype", func(a *platform.Attr) { v := "$users2"; a.Etype = &v }},
		{"wrong label", func(a *platform.Attr) { v := "email2"; a.Label = &v }},
		{"wrong value type", func(a *platform.Attr) { a.ValueType = "ref" }},
		{"wrong cardinality", func(a *platform.Attr) { a.Cardinality = "many" }},
		{"missing uniqueness", func(a *platform.Attr) { a.IsUnique = false }},
		{"missing index", func(a *platform.Attr) { a.IsIndexed = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := valid
			tc.mutate(&got)
			if err := validateSystemAttr(appID, got, spec); err == nil {
				t.Fatal("schema-mismatched existing attr was accepted")
			}
		})
	}
}

func cloneLabels(in map[[16]byte]string) map[[16]byte]string {
	out := make(map[[16]byte]string, len(in))
	for id, label := range in {
		out[id] = label
	}
	return out
}

func TestAttrsCacheIsolatesAppsAndDoesNotPoisonErrors(t *testing.T) {
	ctx := context.Background()
	appA, appB, bad := [16]byte{1}, [16]byte{2}, [16]byte{3}
	svc := &Service{}
	var callsMu sync.Mutex
	calls := map[[16]byte]int{}
	good := map[[16]byte]systemAttrs{appA: completeSystemAttrs(40), appB: completeSystemAttrs(50)}
	loader := func(_ context.Context, appID [16]byte) (systemAttrs, error) {
		callsMu.Lock()
		calls[appID]++
		callsMu.Unlock()
		if appID == bad {
			return systemAttrs{}, errors.New("malformed auth attribute catalog")
		}
		return good[appID], nil
	}
	for _, appID := range [][16]byte{appA, appB} {
		got, err := svc.attrsWithLoader(ctx, appID, loader)
		if err != nil {
			t.Fatalf("app %v: %v", appID, err)
		}
		if !reflect.DeepEqual(got, good[appID]) {
			t.Fatalf("app %v returned wrong complete attrs: %+v", appID, got)
		}
	}
	for _, appID := range [][16]byte{appA, appB} {
		got, err := svc.attrsWithLoader(ctx, appID, func(context.Context, [16]byte) (systemAttrs, error) {
			t.Fatal("cache hit reloaded another app")
			return systemAttrs{}, nil
		})
		if err != nil || !reflect.DeepEqual(got, good[appID]) {
			t.Fatalf("app %v cache hit: %+v err=%v", appID, got, err)
		}
	}
	if _, err := svc.attrsWithLoader(ctx, bad, loader); err == nil {
		t.Fatal("malformed auth attributes unexpectedly succeeded")
	}
	if _, err := svc.attrsWithLoader(ctx, bad, loader); err == nil {
		t.Fatal("malformed auth attributes unexpectedly became cached")
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls[appA] != 1 || calls[appB] != 1 || calls[bad] != 2 {
		t.Fatalf("loader call counts = %#v, want appA=1 appB=1 bad=2", calls)
	}
}

func TestAttrsConcurrentLoadsPublishCompletePerAppSnapshots(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	appID := [16]byte{7}
	svc := &Service{}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	want := completeSystemAttrs(60)
	loader := func(ctx context.Context, gotAppID [16]byte) (systemAttrs, error) {
		if gotAppID != appID {
			return systemAttrs{}, errors.New("wrong app")
		}
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return want, nil
		case <-ctx.Done():
			return systemAttrs{}, ctx.Err()
		}
	}
	type result struct {
		attrs systemAttrs
		err   error
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			a, err := svc.attrsWithLoader(ctx, appID, loader)
			results <- result{attrs: a, err: err}
		}()
	}
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("concurrent load did not reach barrier")
	}
	close(release)
	for range 2 {
		select {
		case got := <-results:
			if got.err != nil || !reflect.DeepEqual(got.attrs, want) {
				t.Fatalf("concurrent load published partial/error result: %+v", got)
			}
		case <-ctx.Done():
			t.Fatal("concurrent load did not finish")
		}
	}
}
