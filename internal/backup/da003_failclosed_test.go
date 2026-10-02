package backup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/instant-v2/instant-v2/internal/backup"
)

// da003MemStore is an in-memory backup.ObjectStore that records calls so
// tests can prove unauthorized requests cause no object-store mutation.
type da003MemStore struct {
	mu                sync.Mutex
	objs              map[string][]byte
	puts              int
	gets              int
	putKeys           []string
	promotions        [][2]string
	deletes           []string
	putPrefixBytes    int64
	putErr            error
	promoteErr        error
	deleteErr         error
	cancelOnPut       context.CancelFunc
	deleteSawCanceled bool
	deleteHasDeadline bool
}

func newDA003MemStore() *da003MemStore {
	return &da003MemStore{objs: map[string][]byte{}}
}

func (m *da003MemStore) Put(_ context.Context, key string, r io.Reader, _ int64) error {
	reader := r
	if m.putPrefixBytes > 0 {
		reader = io.LimitReader(r, m.putPrefixBytes)
	}
	b, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.mu.Lock()
	m.puts++
	m.putKeys = append(m.putKeys, key)
	m.objs[key] = b
	cancel := m.cancelOnPut
	putErr := m.putErr
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return putErr
}

func (m *da003MemStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gets++
	b, ok := m.objs[key]
	if !ok {
		return nil, fmt.Errorf("%w: %s", backup.ErrObjectNotFound, key)
	}
	return io.NopCloser(bytes.NewReader(b)), nil
}

func (m *da003MemStore) Promote(_ context.Context, tempKey, finalKey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.promotions = append(m.promotions, [2]string{tempKey, finalKey})
	if m.promoteErr != nil {
		return m.promoteErr
	}
	b, ok := m.objs[tempKey]
	if !ok {
		return fmt.Errorf("missing staged object %q", tempKey)
	}
	m.objs[finalKey] = append([]byte{}, b...)
	return nil
}

func (m *da003MemStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletes = append(m.deletes, key)
	m.deleteSawCanceled = ctx.Err() != nil
	_, m.deleteHasDeadline = ctx.Deadline()
	if m.deleteErr != nil {
		return m.deleteErr
	}
	delete(m.objs, key)
	return nil
}

func (m *da003MemStore) snapshot(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.objs[key]
	return append([]byte{}, b...), ok
}

func (m *da003MemStore) calls() (puts []string, promotions [][2]string, deletes []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string{}, m.putKeys...), append([][2]string{}, m.promotions...), append([]string{}, m.deletes...)
}

// TestHandlerMissingAuthConfigObjectRoutes pins DA-003 row 1 on the object
// routes: a handler without an authorization callback refuses requests
// instead of panicking on nil func value.
func TestHandlerMissingAuthConfigObjectRoutes(t *testing.T) {
	store := newDA003MemStore()
	h := &backup.Handler{Pool: nil, AdminTokenCheck: nil, S3: store}
	appStr := "00000000-0000-4000-8000-000000000001"

	routes := []struct {
		method string
		path   string
	}{
		{http.MethodPut, "/backup/" + appStr + "/object?key=x"},
		{http.MethodGet, "/backup/" + appStr + "/object?key=x"},
		{http.MethodPost, "/backup/" + appStr + "/restore-object?key=x"},
	}
	for _, rt := range routes {
		t.Run(rt.method+" "+rt.path, func(t *testing.T) {
			req := httptest.NewRequest(rt.method, rt.path, nil)
			req.Header.Set("Authorization", "Bearer tok-123")
			rec := httptest.NewRecorder()
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("nil AdminTokenCheck panicked on %s %s: %v", rt.method, rt.path, recovered)
				}
			}()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusInternalServerError {
				t.Fatalf("expected 500 on missing auth config, got %d", rec.Code)
			}
		})
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.puts != 0 || store.gets != 0 {
		t.Fatalf("missing auth config requests touched store: puts=%d gets=%d", store.puts, store.gets)
	}
}

// TestPutObjectReturnsCounts pins the completion contract on the object
// path: PUT responds with the export counts (per the route doc) and the
// stored artifact ends with a checksum trailer, so truncation is
// detectable on restore.
func TestPutObjectReturnsCounts(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 3)

	store := newDA003MemStore()
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("put object: got %d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Counts backup.Counts `json:"counts"`
		Key    string        `json:"key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode put response: %v (body=%s)", err, rec.Body.String())
	}
	if resp.Counts.Triples != 15 || resp.Counts.Attrs != 4 || resp.Counts.Transactions != 1 || resp.Counts.Rules != 0 {
		t.Fatalf("put counts unverifiable: %+v", resp.Counts)
	}
	if resp.Key != appStr+"/dumps/app.ndjson" {
		t.Fatalf("put key not app-scoped: %q", resp.Key)
	}
	stored, ok := store.snapshot(resp.Key)
	if !ok || len(stored) == 0 {
		t.Fatal("object missing after put")
	}
	last := lastLine(stored)
	if !strings.HasPrefix(last, `{"kind":"checksum"`) {
		t.Fatalf("stored artifact lacks checksum trailer: %q", last)
	}
	var trailer struct {
		Kind    string `json:"kind"`
		SHA256  string `json:"sha256"`
		Records int64  `json:"records"`
	}
	if err := json.Unmarshal([]byte(last), &trailer); err != nil {
		t.Fatalf("decode checksum trailer: %v", err)
	}
	expectedRecords := resp.Counts.Attrs + resp.Counts.Triples + resp.Counts.Rules + resp.Counts.Transactions
	if trailer.Records != expectedRecords {
		t.Fatalf("trailer records %d != counts sum %d", trailer.Records, expectedRecords)
	}
	if trailer.SHA256 == "" {
		t.Fatal("stored artifact has empty checksum sha256")
	}
	puts, promotions, deletes := store.calls()
	if len(puts) != 1 || puts[0] == resp.Key || strings.HasPrefix(puts[0], appStr+"/") {
		t.Fatalf("put did not use private staging key: puts=%q final=%q", puts, resp.Key)
	}
	if len(promotions) != 1 || promotions[0] != [2]string{puts[0], resp.Key} {
		t.Fatalf("publish calls = %v, want one promotion from staging to final", promotions)
	}
	if len(deletes) != 1 || deletes[0] != puts[0] {
		t.Fatalf("cleanup calls = %q, want staged key %q", deletes, puts[0])
	}
	if _, ok := store.snapshot(puts[0]); ok {
		t.Fatal("staged object remains after successful publish")
	}
}

// TestPutObjectFailureFailsClosed pins failure handling on object PUT:
// when S3 store returns a write failure, PUT returns 502 Bad Gateway.
func TestPutObjectFailureFailsClosed(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.putErr = errors.New("simulated s3 network failure")
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/fail.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on store put failure, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestStagingObjectKeysArePrivate(t *testing.T) {
	store := newDA003MemStore()
	h := &backup.Handler{AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := "00000000-0000-4000-8000-000000000001"
	stagingKey := url.QueryEscape(".instant-backup-staging/" + appStr + "/random")
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"get", http.MethodGet, "/backup/" + appStr + "/object?key=" + stagingKey},
		{"put", http.MethodPut, "/backup/" + appStr + "/object?key=" + stagingKey},
		{"restore", http.MethodPost, "/backup/" + appStr + "/restore-object?key=" + stagingKey},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer tok-123")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("staging %s status = %d, want 400", tt.name, rec.Code)
			}
		})
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.puts != 0 || store.gets != 0 || len(store.promotions) != 0 || len(store.deletes) != 0 {
		t.Fatalf("private staging request reached object store: puts=%d gets=%d promotions=%d deletes=%d", store.puts, store.gets, len(store.promotions), len(store.deletes))
	}
}

func TestPutObjectCleanupFailurePreservesPublishedSuccess(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.deleteErr = errors.New("simulated staging delete failure")
	var logs bytes.Buffer
	h := &backup.Handler{
		Pool:            pool,
		AdminTokenCheck: fakeAuth{valid: "tok-123"}.check,
		S3:              store,
		Logger:          slog.New(slog.NewTextHandler(&logs, nil)),
	}
	appStr := uuidStr(appID)
	finalKey := appStr + "/dumps/app.ndjson"

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("cleanup failure changed published response: got %d body=%s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") != "" || strings.Contains(rec.Body.String(), `"error"`) {
		t.Fatalf("published response signals unsafe retry: headers=%v body=%s", rec.Header(), rec.Body.String())
	}
	if _, ok := store.snapshot(finalKey); !ok {
		t.Fatal("promoted object is missing after cleanup failure")
	}
	puts, promotions, deletes := store.calls()
	if len(puts) != 1 || len(promotions) != 1 || promotions[0] != [2]string{puts[0], finalKey} || len(deletes) != 1 || deletes[0] != puts[0] {
		t.Fatalf("unexpected publish calls: puts=%q promotions=%v deletes=%q", puts, promotions, deletes)
	}
	if !strings.Contains(logs.String(), "object published but staging cleanup failed") || !strings.Contains(logs.String(), "simulated staging delete failure") {
		t.Fatalf("cleanup failure was not logged: %s", logs.String())
	}
}

func TestPutObjectPrefixFailurePreservesFinalAndCleansStage(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.putPrefixBytes = 32
	store.putErr = errors.New("simulated prefix write failure")
	appStr := uuidStr(appID)
	finalKey := appStr + "/dumps/app.ndjson"
	old := []byte("previous complete object\n")
	store.objs[finalKey] = append([]byte{}, old...)
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got, _ := store.snapshot(finalKey); !bytes.Equal(got, old) {
		t.Fatalf("failed staged put replaced final: got %q want %q", got, old)
	}
	puts, promotions, deletes := store.calls()
	if len(puts) != 1 || puts[0] == finalKey || len(promotions) != 0 {
		t.Fatalf("unexpected publish calls: puts=%q promotions=%v", puts, promotions)
	}
	if len(deletes) != 1 || deletes[0] != puts[0] {
		t.Fatalf("staging cleanup = %q, want %q", deletes, puts[0])
	}
	if _, ok := store.snapshot(puts[0]); ok {
		t.Fatal("partial staged object remains after put failure")
	}
}

func TestPutObjectShortSuccessDoesNotPromote(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.putPrefixBytes = 32
	appStr := uuidStr(appID)
	finalKey := appStr + "/dumps/app.ndjson"
	old := []byte("previous complete object\n")
	store.objs[finalKey] = append([]byte{}, old...)
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("short-success must fail export, got %d body=%s", rec.Code, rec.Body.String())
	}
	if got, _ := store.snapshot(finalKey); !bytes.Equal(got, old) {
		t.Fatalf("short-success replaced final: got %q want %q", got, old)
	}
	puts, promotions, deletes := store.calls()
	if len(puts) != 1 || len(promotions) != 0 || len(deletes) != 1 || deletes[0] != puts[0] {
		t.Fatalf("unexpected calls: puts=%q promotions=%v deletes=%q", puts, promotions, deletes)
	}
}

func TestPutObjectCancellationDoesNotCancelCleanup(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.putPrefixBytes = 32
	store.putErr = context.Canceled
	reqCtx, cancel := context.WithCancel(context.Background())
	store.cancelOnPut = cancel
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil).WithContext(reqCtx)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("expected 502, got %d body=%s", rec.Code, rec.Body.String())
	}
	store.mu.Lock()
	deletes := append([]string{}, store.deletes...)
	sawCanceled := store.deleteSawCanceled
	hasDeadline := store.deleteHasDeadline
	store.mu.Unlock()
	if len(deletes) != 1 || sawCanceled || !hasDeadline {
		t.Fatalf("cleanup context: deletes=%q canceled=%v deadline=%v", deletes, sawCanceled, hasDeadline)
	}
}

func TestPutObjectPromotionFailureReportsUnknownStatus(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 1)

	store := newDA003MemStore()
	store.promoteErr = errors.New("simulated copy response loss")
	appStr := uuidStr(appID)
	finalKey := appStr + "/dumps/app.ndjson"
	old := []byte("previous complete object\n")
	store.objs[finalKey] = append([]byte{}, old...)
	h := &backup.Handler{Pool: pool, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}

	req := httptest.NewRequest(http.MethodPut, "/backup/"+appStr+"/object?key=dumps/app.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadGateway || !strings.Contains(rec.Body.String(), "status unknown") {
		t.Fatalf("promotion failure must report uncertain status: got %d body=%s", rec.Code, rec.Body.String())
	}
	if got, _ := store.snapshot(finalKey); !bytes.Equal(got, old) {
		t.Fatalf("known pre-commit promotion failure replaced final: got %q want %q", got, old)
	}
	puts, promotions, deletes := store.calls()
	if len(puts) != 1 || len(promotions) != 1 || promotions[0] != [2]string{puts[0], finalKey} {
		t.Fatalf("unexpected promotion calls: puts=%q promotions=%v", puts, promotions)
	}
	if len(deletes) != 1 || deletes[0] != puts[0] {
		t.Fatalf("promotion failure cleanup = %q, want %q", deletes, puts[0])
	}
}

// TestUnauthorizedObjectRoutesMutateNothing pins DA-003 row 2 on the
// object paths: bad-token and missing-auth PUT/GET/restore-object are
// rejected before any store call, emit no dump bytes, and record zero puts/gets.
func TestUnauthorizedObjectRoutesMutateNothing(t *testing.T) {
	store := newDA003MemStore()
	h := &backup.Handler{Pool: nil, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := "00000000-0000-4000-8000-000000000001"

	authCases := []struct {
		name    string
		headers map[string]string
	}{
		{"missing_token", nil},
		{"bad_bearer", map[string]string{"Authorization": "Bearer wrong-token"}},
		{"bad_x_admin_token", map[string]string{"X-admin-token": "wrong-token"}},
	}

	for _, ac := range authCases {
		t.Run(ac.name, func(t *testing.T) {
			do := func(method, path string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(method, path, nil)
				for k, v := range ac.headers {
					req.Header.Set(k, v)
				}
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				return rec
			}

			if rec := do(http.MethodPut, "/backup/"+appStr+"/object?key=x"); rec.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized put (%s): got %d", ac.name, rec.Code)
			}
			if rec := do(http.MethodGet, "/backup/"+appStr+"/object?key=x"); rec.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized get (%s): got %d", ac.name, rec.Code)
			} else if strings.Contains(rec.Body.String(), `"kind"`) {
				t.Fatalf("unauthorized get (%s) emitted dump bytes: %s", ac.name, rec.Body.String())
			}
			if rec := do(http.MethodPost, "/backup/"+appStr+"/restore-object?key=x"); rec.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized restore-object (%s): got %d", ac.name, rec.Code)
			}
		})
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.puts != 0 || store.gets != 0 {
		t.Fatalf("unauthorized requests touched the store: puts=%d gets=%d", store.puts, store.gets)
	}
}

// TestRestoreObjectCorruptPreservesSourceAndTarget pins DA-003 rows 4-5
// on the object path: a corrupted stored artifact is refused with 400,
// the source object is left intact, and the prior target rows are
// unchanged (single-transaction rollback).
func TestRestoreObjectCorruptPreservesSourceAndTarget(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 2)
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	target := emptyRestoreTarget(t, appID)
	before, _ := exportApp(t, ctx, target, appID, backup.ExportOptions{})

	corrupt := append([]byte{}, dump...)
	if i := bytes.Index(corrupt, []byte(`"value":4`)); i < 0 {
		t.Fatal("corruption target not found in dump")
	} else {
		copy(corrupt[i:], []byte(`"value":9`))
	}

	store := newDA003MemStore()
	h := &backup.Handler{Pool: target, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)
	scoped := appStr + "/dumps/corrupt.ndjson"
	store.mu.Lock()
	store.objs[scoped] = append([]byte{}, corrupt...)
	store.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/backup/"+appStr+"/restore-object?key=dumps/corrupt.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("corrupt restore-object: got %d body=%s", rec.Code, rec.Body.String())
	}

	after, ok := store.snapshot(scoped)
	if !ok || !bytes.Equal(after, corrupt) {
		t.Fatal("failed restore did not preserve the source object")
	}
	state, _ := exportApp(t, ctx, target, appID, backup.ExportOptions{})
	if !bytes.Equal(before, state) {
		t.Fatal("failed object restore changed exact target state")
	}
	var triples, attrs int
	if err := target.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM triples WHERE app_id=$1),
		        (SELECT count(*) FROM attrs   WHERE app_id=$1)`, appID).Scan(&triples, &attrs); err != nil {
		t.Fatal(err)
	}
	if triples != 0 || attrs != 0 {
		t.Fatalf("failed restore-object mutated target: triples=%d attrs=%d", triples, attrs)
	}
}

// TestRestoreObjectTruncatedPreservesSourceAndTarget pins DA-003 rows 4-5
// on the object path: a truncated stored artifact (missing checksum trailer)
// is refused with 400, the source object is left intact, and the prior target
// rows are unchanged (single-transaction rollback).
func TestRestoreObjectTruncatedPreservesSourceAndTarget(t *testing.T) {
	ctx := context.Background()
	pool, appID, cleanup := env(t)
	defer cleanup()
	seedTodoApp(t, ctx, pool, appID, 2)
	dump, _ := exportApp(t, ctx, pool, appID, backup.ExportOptions{})
	target := emptyRestoreTarget(t, appID)
	before, _ := exportApp(t, ctx, target, appID, backup.ExportOptions{})

	// Truncate before checksum trailer: drop the final checksum line.
	lines := bytes.Split(bytes.TrimSuffix(dump, []byte("\n")), []byte("\n"))
	truncated := append(bytes.Join(lines[:len(lines)-1], []byte("\n")), '\n')

	store := newDA003MemStore()
	h := &backup.Handler{Pool: target, AdminTokenCheck: fakeAuth{valid: "tok-123"}.check, S3: store}
	appStr := uuidStr(appID)
	scoped := appStr + "/dumps/truncated.ndjson"
	store.mu.Lock()
	store.objs[scoped] = append([]byte{}, truncated...)
	store.mu.Unlock()

	req := httptest.NewRequest(http.MethodPost, "/backup/"+appStr+"/restore-object?key=dumps/truncated.ndjson", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("truncated restore-object: got %d body=%s", rec.Code, rec.Body.String())
	}

	after, ok := store.snapshot(scoped)
	if !ok || !bytes.Equal(after, truncated) {
		t.Fatal("failed restore did not preserve the source object")
	}
	state, _ := exportApp(t, ctx, target, appID, backup.ExportOptions{})
	if !bytes.Equal(before, state) {
		t.Fatal("failed object restore changed exact target state")
	}
	var triples, attrs int
	if err := target.QueryRow(ctx,
		`SELECT (SELECT count(*) FROM triples WHERE app_id=$1),
		        (SELECT count(*) FROM attrs   WHERE app_id=$1)`, appID).Scan(&triples, &attrs); err != nil {
		t.Fatal(err)
	}
	if triples != 0 || attrs != 0 {
		t.Fatalf("failed restore-object mutated target: triples=%d attrs=%d", triples, attrs)
	}
}

func lastLine(b []byte) string {
	s := strings.TrimSuffix(string(b), "\n")
	if i := strings.LastIndex(s, "\n"); i >= 0 {
		return s[i+1:]
	}
	return s
}
