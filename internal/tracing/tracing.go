// Package tracing wires OpenTelemetry spans across the transact→commit→
// fanout chain (docs/reference/09-tier2-architecture.md §T3 observability). Export is
// strictly opt-in: without OTEL_EXPORTER_OTLP_ENDPOINT the global tracer
// provider stays the SDK no-op, so span creation costs a few ns and nothing
// leaves the process. When configured, spans export over OTLP/HTTP — the
// standard collector protocol every self-host stack already speaks.
package tracing

import (
	"context"
	"errors"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	"go.opentelemetry.io/otel/trace"
)

// Init installs a global TracerProvider when OTLP export is configured.
// endpoint comes from OTEL_EXPORTER_OTLP_ENDPOINT (standard env); empty —
// the default — keeps the no-op provider and returns a nil shutdown.
// serviceName/node identify the series in the collector.
func Init(ctx context.Context, endpoint, serviceName, node string) (func(context.Context) error, error) {
	if endpoint == "" {
		return nil, nil
	}
	exp, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithTimeout(5*time.Second),
	)
	if err != nil {
		return nil, err
	}
	res, err := resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceInstanceID(node),
		))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sdktrace.NewBatchSpanProcessor(exp)),
		sdktrace.WithResource(res),
		// Parent-based sampling: respect upstream sampling decisions and
		// default to root-sampling everything that reaches us unparented.
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}

// Tracer is the process-wide tracer. Safe before Init: until a provider is
// installed it produces non-recording spans.
var Tracer trace.Tracer = otel.Tracer("instantd")

// Endpoint reads the standard OTLP endpoint variable.
func Endpoint() string { return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") }

// ErrDisabled is returned by helpers when tracing is not configured.
var ErrDisabled = errors.New("tracing: OTEL_EXPORTER_OTLP_ENDPOINT not set")
