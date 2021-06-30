package gocbopentelemetry

import (
	"context"
	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"time"
)

type OpenTelemetryRequestTracer struct {
	wrapped trace.Tracer
}

func NewOpenTelemetryRequestTracer(provider trace.TracerProvider) *OpenTelemetryRequestTracer {
	return &OpenTelemetryRequestTracer{
		wrapped: provider.Tracer("com.couchbase.client/go"),
	}
}

func (tracer *OpenTelemetryRequestTracer) RequestSpan(parentContext gocb.RequestSpanContext, operationName string) gocb.RequestSpan {
	parentCtx := context.Background()
	if ctx, ok := parentContext.(context.Context); ok {
		parentCtx = ctx
	}

	return NewOpenTelemetryRequestSpan(tracer.wrapped.Start(parentCtx, operationName))
}

type OpenTelemetryRequestSpan struct {
	ctx     context.Context
	wrapped trace.Span
}

func NewOpenTelemetryRequestSpan(ctx context.Context, span trace.Span) *OpenTelemetryRequestSpan {
	return &OpenTelemetryRequestSpan{
		ctx:     ctx,
		wrapped: span,
	}
}

func (span *OpenTelemetryRequestSpan) End() {
	span.wrapped.End()
}

func (span *OpenTelemetryRequestSpan) Context() gocb.RequestSpanContext {
	return span.ctx
}

func (span *OpenTelemetryRequestSpan) SetAttribute(key string, value interface{}) {
	span.wrapped.SetAttributes(attribute.Any(key, value))
}

func (span *OpenTelemetryRequestSpan) AddEvent(key string, timestamp time.Time) {
	span.wrapped.AddEvent(key, trace.WithTimestamp(timestamp))
}
