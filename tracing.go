package gocbopentelemetry

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"log"
	"time"
)

// OpenTelemetryRequestTracer is an implementation of the gocb Tracer interface which wraps an OpenTelemetry tracer.
type OpenTelemetryRequestTracer struct {
	wrapped trace.Tracer
}

// NewOpenTelemetryRequestTracer creates a new OpenTelemetryRequestTracer.
func NewOpenTelemetryRequestTracer(provider trace.TracerProvider) *OpenTelemetryRequestTracer {
	return &OpenTelemetryRequestTracer{
		wrapped: provider.Tracer("com.couchbase.client/go"),
	}
}

// RequestSpan provides a wrapped OpenTelemetry Span.
func (tracer *OpenTelemetryRequestTracer) RequestSpan(parentContext gocb.RequestSpanContext, operationName string) gocb.RequestSpan {
	parentCtx := context.Background()
	if ctx, ok := parentContext.(context.Context); ok {
		parentCtx = ctx
	}

	return NewOpenTelemetryRequestSpan(tracer.wrapped.Start(parentCtx, operationName))
}

// OpenTelemetryRequestSpan is an implementation of the gocb Span interface which wraps an OpenTelemetry span.
type OpenTelemetryRequestSpan struct {
	ctx     context.Context
	wrapped trace.Span
}

// NewOpenTelemetryRequestSpan creates a new OpenTelemetryRequestSpan.
func NewOpenTelemetryRequestSpan(ctx context.Context, span trace.Span) *OpenTelemetryRequestSpan {
	return &OpenTelemetryRequestSpan{
		ctx:     ctx,
		wrapped: span,
	}
}

// End completes the span.
func (span *OpenTelemetryRequestSpan) End() {
	span.wrapped.End()
}

// Context returns the RequestSpanContext for this span.
func (span *OpenTelemetryRequestSpan) Context() gocb.RequestSpanContext {
	return span.ctx
}

// SetAttribute adds an attribute to this span.
func (span *OpenTelemetryRequestSpan) SetAttribute(key string, value interface{}) {
	switch v := value.(type) {
	case string:
		span.wrapped.SetAttributes(attribute.String(key, v))
	case *string:
		span.wrapped.SetAttributes(attribute.String(key, *v))
	case bool:
		span.wrapped.SetAttributes(attribute.Bool(key, v))
	case *bool:
		span.wrapped.SetAttributes(attribute.Bool(key, *v))
	case int:
		span.wrapped.SetAttributes(attribute.Int(key, v))
	case *int:
		span.wrapped.SetAttributes(attribute.Int(key, *v))
	case int64:
		span.wrapped.SetAttributes(attribute.Int64(key, v))
	case *int64:
		span.wrapped.SetAttributes(attribute.Int64(key, *v))
	case float64:
		span.wrapped.SetAttributes(attribute.Float64(key, v))
	case *float64:
		span.wrapped.SetAttributes(attribute.Float64(key, *v))
	case []string:
		span.wrapped.SetAttributes(attribute.StringSlice(key, v))
	case []bool:
		span.wrapped.SetAttributes(attribute.BoolSlice(key, v))
	case []int:
		span.wrapped.SetAttributes(attribute.IntSlice(key, v))
	case []int64:
		span.wrapped.SetAttributes(attribute.Int64Slice(key, v))
	case []float64:
		span.wrapped.SetAttributes(attribute.Float64Slice(key, v))
	case fmt.Stringer:
		span.wrapped.SetAttributes(attribute.String(key, v.String()))
	default:
		// This isn't great but we should make some effort to output some sort of warning.
		log.Println("Unable to determine value as a type that we can handle")
	}
}

// AddEvent adds an event to this span.
func (span *OpenTelemetryRequestSpan) AddEvent(key string, timestamp time.Time) {
	span.wrapped.AddEvent(key, trace.WithTimestamp(timestamp))
}
