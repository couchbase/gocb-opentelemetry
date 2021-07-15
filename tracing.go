package gocbopentelemetry

import (
	"context"
	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	span.wrapped.SetAttributes(attribute.Any(key, value))
}

// AddEvent adds an event to this span.
func (span *OpenTelemetryRequestSpan) AddEvent(key string, timestamp time.Time) {
	span.wrapped.AddEvent(key, trace.WithTimestamp(timestamp))
}
