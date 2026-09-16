package backup_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/instant-v2/instant-v2/internal/backup"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 speaks just enough of the S3 REST API (path-style) for minio-go:
// bucket location probe, HEAD bucket, PUT/GET object. No auth enforcement.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string][]byte
	parts   map[string][][]byte // key -> uploaded parts (multipart)
	srv     *httptest.Server
}

func newFakeS3(t *testing.T, bucket string) *fakeS3 {
	t.Helper()
	f := &fakeS3{objects: map[string][]byte{}, parts: map[string][][]byte{}}
	// One catch-all handler: Go's method-pattern mux would route
	// HEAD /bkt/key to the GET location probe otherwise.
	mux := http.NewServeMux()
	mux.HandleFunc("/"+bucket+"/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/"+bucket+"/" || r.URL.Path == "/"+bucket {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>`)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/"+bucket+"/")
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
			source, err := url.PathUnescape(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/"))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			source = strings.TrimPrefix(source, bucket+"/")
			b, ok := f.objects[source]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			f.objects[key] = append([]byte{}, b...)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<?xml version="1.0"?><CopyObjectResult><LastModified>1970-01-01T00:00:00.000Z</LastModified><ETag>"fake-copy"</ETag></CopyObjectResult>`)
		case r.Method == http.MethodPost && r.URL.Query().Has("uploads"):
			// InitiateMultipartUpload
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><InitiateMultipartUploadResult><Bucket>%s</Bucket><Key>%s</Key><UploadId>u1</UploadId></InitiateMultipartUploadResult>`, bucket, key)
		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
			// CompleteMultipartUpload: concatenate parts in arrival order.
			var cat []byte
			for _, p := range f.parts[key] {
				cat = append(cat, p...)
			}
			f.objects[key] = cat
			delete(f.parts, key)
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprintf(w, `<?xml version="1.0"?><CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Location>http://%s/%s/%s</Location><Bucket>%s</Bucket><Key>%s</Key><ETag>"fake"</ETag></CompleteMultipartUploadResult>`, r.Host, bucket, key, bucket, key)
		case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "":
			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			f.parts[key] = append(f.parts[key], b)
			w.Header().Set("ETag", `"fake-part"`)
			w.WriteHeader(http.StatusOK)
		}
		if handled := r.Method == http.MethodPost || (r.Method == http.MethodPut && (r.URL.Query().Get("uploadId") != "" || r.Header.Get("X-Amz-Copy-Source") != "")); handled {
			return
		}
		switch r.Method {
		case http.MethodPut:
			b, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			f.objects[key] = b
			w.Header().Set("ETag", `"fake"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			delete(f.objects, key)
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet, http.MethodHead:
			b, ok := f.objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(len(b)))
			// minio-go parses these on both GET and its implicit Stat.
			w.Header().Set("Last-Modified", time.Unix(0, 0).UTC().Format(http.TimeFormat))
			w.Header().Set("ETag", `"fake"`)
			if r.Method == http.MethodHead {
				return
			}
			_, _ = w.Write(b)
		default:
			t.Logf("fake-s3 405: %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})
	logged := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Logf("fake-s3: %s %s", r.Method, r.URL.String())
		mux.ServeHTTP(w, r)
	})
	f.srv = httptest.NewServer(logged)
	t.Cleanup(f.srv.Close)
	return f
}

func TestS3StoreRoundTrip(t *testing.T) {
	f := newFakeS3(t, "bkt")
	store, err := backup.NewS3Store(context.Background(), backup.S3Config{
		Endpoint:  strings.TrimPrefix(f.srv.URL, "http://"),
		Bucket:    "bkt",
		PathStyle: true,
	})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	payload := "hello-ndjson\nlines\n"
	if err := store.Put(context.Background(), "staging/x.ndjson", strings.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := store.Promote(context.Background(), "staging/x.ndjson", "dumps/x.ndjson"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if err := store.Delete(context.Background(), "staging/x.ndjson"); err != nil {
		t.Fatalf("delete staging object: %v", err)
	}
	rc, err := store.Get(context.Background(), "dumps/x.ndjson")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer func() { _ = rc.Close() }()
	got, _ := io.ReadAll(rc)
	if string(got) != payload {
		t.Fatalf("round trip mismatch: %q", got)
	}
	f.mu.Lock()
	_, staged := f.objects["staging/x.ndjson"]
	f.mu.Unlock()
	if staged {
		t.Fatal("staging object remains after delete")
	}
}

func TestS3StoreNotFound(t *testing.T) {
	f := newFakeS3(t, "bkt")
	store, err := backup.NewS3Store(context.Background(), backup.S3Config{
		Endpoint: strings.TrimPrefix(f.srv.URL, "http://"), Bucket: "bkt", PathStyle: true,
	})
	if err != nil {
		t.Fatalf("new s3 store: %v", err)
	}
	_, err = store.Get(context.Background(), "missing")
	if !errors.Is(err, backup.ErrObjectNotFound) {
		t.Fatalf("want backup.ErrObjectNotFound, got %v", err)
	}
}
