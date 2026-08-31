package httpjson

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeObject(t *testing.T) {
	for _, input := range []string{"", "null", "[]", "true", "1", `"text"`, `{"ok":true`, `{"ok":true} {}`, `{"ok":true} junk`} {
		t.Run(input, func(t *testing.T) {
			if got, err := DecodeObject(strings.NewReader(input)); err == nil || got != nil {
				t.Fatalf("DecodeObject(%q) = %#v, %v; want nil object and error", input, got, err)
			}
		})
	}
	if got, err := DecodeObject(nil); err == nil || got != nil {
		t.Fatalf("nil input = %#v, %v", got, err)
	}
	got, err := DecodeObject(strings.NewReader("{\"n\": 1, \"app-id\": \"legacy\", \"unknown\": true} \n\t"))
	if err != nil || got["n"] != float64(1) || got["app-id"] != "legacy" || got["unknown"] != true {
		t.Fatalf("object = %#v, %v", got, err)
	}
}

func TestWritePreservesWire(t *testing.T) {
	for _, escape := range []bool{true, false} {
		recorder := httptest.NewRecorder()
		if err := Write(recorder, http.StatusCreated, map[string]string{"value": "<>&"}, escape); err != nil {
			t.Fatal(err)
		}
		want := "{\"value\":\"<>&\"}\n"
		if escape {
			want = "{\"value\":\"\\u003c\\u003e\\u0026\"}\n"
		}
		if recorder.Code != http.StatusCreated || recorder.Header().Get("Content-Type") != "application/json" || recorder.Body.String() != want {
			t.Fatalf("escape=%t: code=%d headers=%v body=%q", escape, recorder.Code, recorder.Header(), recorder.Body.String())
		}
	}
}

type failingWriter struct{ header http.Header }

var errWrite = errors.New("write failed")

func (w failingWriter) Header() http.Header       { return w.header }
func (w failingWriter) WriteHeader(int)           {}
func (w failingWriter) Write([]byte) (int, error) { return 0, errWrite }

func TestWriteReturnsErrors(t *testing.T) {
	if err := Write(failingWriter{header: make(http.Header)}, 200, map[string]bool{"ok": true}, true); !errors.Is(err, errWrite) {
		t.Fatalf("write error = %v", err)
	}
	if err := Write(httptest.NewRecorder(), 200, make(chan int), true); err == nil {
		t.Fatal("unsupported JSON value must return encoder error")
	}
}
