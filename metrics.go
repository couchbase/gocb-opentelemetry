package gocbopentelemetry

import (
	"context"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"sync"
)

type OpenTelemetryMeter struct {
	wrapped       metric.Meter
	counterCache  map[string]*OpenTelemetryCounter
	recorderCache map[string]*OpenTelemetryMeterValueRecorder
	lock          sync.Mutex
}

func NewOpenTelemetryMeter(provider metric.MeterProvider) *OpenTelemetryMeter {
	return &OpenTelemetryMeter{
		wrapped:       provider.Meter("com.couchbase.client/go"),
		counterCache:  make(map[string]*OpenTelemetryCounter),
		recorderCache: make(map[string]*OpenTelemetryMeterValueRecorder),
	}
}

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
		counter = NewOpenTelemetryCounter(context.Background(), otCounter)
		meter.counterCache[key] = counter
	}
	meter.lock.Unlock()

	return counter, nil
}

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
		for k, v := range tags {
			otRecorder.Bind(attribute.String(k, v))
		}
		recorder = NewOpenTelemetryValueRecorder(context.Background(), otRecorder)
		meter.recorderCache[key] = recorder
	}
	meter.lock.Unlock()

	return recorder, nil
}

type OpenTelemetryCounter struct {
	ctx     context.Context
	wrapped metric.Int64Counter
}

func NewOpenTelemetryCounter(ctx context.Context, counter metric.Int64Counter) *OpenTelemetryCounter {
	return &OpenTelemetryCounter{
		ctx:     ctx,
		wrapped: counter,
	}
}

func (nm *OpenTelemetryCounter) IncrementBy(num uint64) {
	nm.wrapped.Add(nm.ctx, int64(num), attribute.KeyValue{Key: "system", Value: attribute.StringValue("couchbase")})
}

type OpenTelemetryMeterValueRecorder struct {
	ctx     context.Context
	wrapped metric.Int64ValueRecorder
}

func NewOpenTelemetryValueRecorder(ctx context.Context, valueRecorder metric.Int64ValueRecorder) *OpenTelemetryMeterValueRecorder {
	return &OpenTelemetryMeterValueRecorder{
		ctx:     ctx,
		wrapped: valueRecorder,
	}
}

func (nm *OpenTelemetryMeterValueRecorder) RecordValue(val uint64) {
	if val > 0 {
		nm.wrapped.Record(nm.ctx, int64(val))
	}
}
