package gocbopentelemetry

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"sync"
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
		otCounter, err := meter.wrapped.NewInt64Counter(name)
		if err != nil {
			meter.lock.Unlock()
			return nil, err
		}
		for k, v := range tags {
			otCounter.Bind(attribute.String(k, v))
		}
		counter = newOpenTelemetryCounter(context.Background(), otCounter)
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
		otRecorder, err := meter.wrapped.NewInt64ValueRecorder(name)
		if err != nil {
			meter.lock.Unlock()
			return nil, err
		}
		var labels []attribute.KeyValue
		for k, v := range tags {
			labels = append(labels, attribute.String(k, v))
		}
		otRecorder.Bind(labels...)
		recorder = newOpenTelemetryValueRecorder(context.Background(), otRecorder)
		meter.recorderCache[key] = recorder
	}
	meter.lock.Unlock()

	return recorder, nil
}

type openTelemetryCounter struct {
	ctx     context.Context
	wrapped metric.Int64Counter
}

func newOpenTelemetryCounter(ctx context.Context, counter metric.Int64Counter) *openTelemetryCounter {
	return &openTelemetryCounter{
		ctx:     ctx,
		wrapped: counter,
	}
}

func (nm *openTelemetryCounter) IncrementBy(num uint64) {
	nm.wrapped.Add(nm.ctx, int64(num), attribute.KeyValue{Key: "system", Value: attribute.StringValue("couchbase")})
}

type openTelemetryMeterValueRecorder struct {
	ctx     context.Context
	wrapped metric.Int64ValueRecorder
}

func newOpenTelemetryValueRecorder(ctx context.Context, valueRecorder metric.Int64ValueRecorder) *openTelemetryMeterValueRecorder {
	return &openTelemetryMeterValueRecorder{
		ctx:     ctx,
		wrapped: valueRecorder,
	}
}

func (nm *openTelemetryMeterValueRecorder) RecordValue(val uint64) {
	if val > 0 {
		nm.wrapped.Record(nm.ctx, int64(val))
	}
}
