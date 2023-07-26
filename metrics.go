package gocbopentelemetry

import (
	"context"
	"fmt"
	"sync"

	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OpenTelemetryMeter is an implementation of the gocb Meter interface which wraps an OpenTelemetry meter.
type OpenTelemetryMeter struct {
	wrapped       metric.Meter
	counterCache  map[string]*openTelemetryCounter
	recorderCache map[string]*openTelemetryMeterValueRecorder
	lock          sync.Mutex
}

// NewOpenTelemetryMeter creates a new OpenTelemetryMeter.
func NewOpenTelemetryMeter(provider metric.MeterProvider) *OpenTelemetryMeter {
	return &OpenTelemetryMeter{
		wrapped:       provider.Meter("com.couchbase.client/go"),
		counterCache:  make(map[string]*openTelemetryCounter),
		recorderCache: make(map[string]*openTelemetryMeterValueRecorder),
	}
}

// Counter provides a wrapped OpenTelemetry Counter.
func (meter *OpenTelemetryMeter) Counter(name string, tags map[string]string) (gocb.Counter, error) {
	key := fmt.Sprintf("%s-%s", name, tags)
	meter.lock.Lock()
	counter := meter.counterCache[key]
	if counter == nil {
		otCounter, err := meter.wrapped.Int64Counter(name)
		if err != nil {
			meter.lock.Unlock()
			return nil, err
		}
		labels := []attribute.KeyValue{
			{Key: "system", Value: attribute.StringValue("couchbase")},
		}
		for k, v := range tags {
			labels = append(labels, attribute.String(k, v))
		}
		counter = newOpenTelemetryCounter(context.Background(), otCounter, labels)
		meter.counterCache[key] = counter
	}
	meter.lock.Unlock()

	return counter, nil
}

// ValueRecorder provides a wrapped OpenTelemetry ValueRecorder.
func (meter *OpenTelemetryMeter) ValueRecorder(name string, tags map[string]string) (gocb.ValueRecorder, error) {
	key := fmt.Sprintf("%s-%s", name, tags)
	meter.lock.Lock()
	recorder := meter.recorderCache[key]
	if recorder == nil {
		otRecorder, err := meter.wrapped.Int64Histogram(name)
		if err != nil {
			meter.lock.Unlock()
			return nil, err
		}
		var labels []attribute.KeyValue
		for k, v := range tags {
			labels = append(labels, attribute.String(k, v))
		}
		recorder = newOpenTelemetryValueRecorder(context.Background(), otRecorder, labels)
		meter.recorderCache[key] = recorder
	}
	meter.lock.Unlock()

	return recorder, nil
}

type openTelemetryCounter struct {
	ctx        context.Context
	wrapped    metric.Int64Counter
	attributes []attribute.KeyValue
}

func newOpenTelemetryCounter(ctx context.Context, counter metric.Int64Counter, attributes []attribute.KeyValue) *openTelemetryCounter {
	return &openTelemetryCounter{
		ctx:        ctx,
		wrapped:    counter,
		attributes: attributes,
	}
}

func (nm *openTelemetryCounter) IncrementBy(num uint64) {
	nm.wrapped.Add(nm.ctx, int64(num), metric.WithAttributes(nm.attributes...))
}

type openTelemetryMeterValueRecorder struct {
	ctx        context.Context
	wrapped    metric.Int64Histogram
	attributes []attribute.KeyValue
}

func newOpenTelemetryValueRecorder(ctx context.Context, valueRecorder metric.Int64Histogram, attributes []attribute.KeyValue) *openTelemetryMeterValueRecorder {
	return &openTelemetryMeterValueRecorder{
		ctx:        ctx,
		wrapped:    valueRecorder,
		attributes: attributes,
	}
}

func (nm *openTelemetryMeterValueRecorder) RecordValue(val uint64) {
	if val > 0 {
		nm.wrapped.Record(nm.ctx, int64(val), metric.WithAttributes(nm.attributes...))
	}
}
