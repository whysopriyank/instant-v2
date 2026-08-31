package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

func TestInitDisabledPreservesProvider(t *testing.T) {
	before := otel.GetTracerProvider()
	shutdown, err := Init(context.Background(), "", "test-service", "test-node")
	if err != nil || shutdown != nil {
		t.Fatalf("disabled Init: shutdown present=%v, err=%v", shutdown != nil, err)
	}
	if otel.GetTracerProvider() != before {
		t.Fatal("disabled Init replaced the global provider")
	}
}

func TestConfiguredTracingFlushesResourceAndSpan(t *testing.T) {
	before := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(before) })
	requests := make(chan *collectortrace.ExportTraceServiceRequest, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read export: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request collectortrace.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &request); err != nil {
			t.Errorf("decode OTLP: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		select {
		case requests <- &request:
		default:
			t.Error("unexpected repeated export")
		}
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	shutdown, err := Init(ctx, server.URL, "quality-test", "node-test")
	if err != nil {
		t.Fatalf("Init: %v", err)
	}
	if shutdown == nil {
		t.Fatal("configured Init returned no shutdown")
	}
	_, span := otel.Tracer("quality-contract").Start(ctx, "verified-span")
	span.End()
	if err := shutdown(ctx); err != nil {
		t.Fatalf("flush on shutdown: %v", err)
	}
	select {
	case request := <-requests:
		if len(request.ResourceSpans) != 1 {
			t.Fatalf("resource spans: got %d, want 1", len(request.ResourceSpans))
		}
		rs := request.ResourceSpans[0]
		attributes := make(map[string]string)
		for _, attr := range rs.Resource.Attributes {
			attributes[attr.Key] = attr.Value.GetStringValue()
		}
		if attributes["service.name"] != "quality-test" || attributes["service.instance.id"] != "node-test" {
			t.Fatalf("resource identity mismatch: %v", attributes)
		}
		if len(rs.ScopeSpans) != 1 || len(rs.ScopeSpans[0].Spans) != 1 || rs.ScopeSpans[0].Spans[0].Name != "verified-span" {
			t.Fatalf("exported span mismatch: %v", rs.ScopeSpans)
		}
	case <-ctx.Done():
		t.Fatal("shutdown completed without exporting the span")
	}
}

func TestEndpointReadsConfiguration(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318")
	if got := Endpoint(); got != "http://127.0.0.1:4318" {
		t.Fatalf("Endpoint = %q", got)
	}
}
