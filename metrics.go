package gocbopentelemetry

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"

	"github.com/couchbase/gocb/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// OpenTelemetryMeter is an implementation of the gocb Meter interface which wraps an OpenTelemetry meter.
type OpenTelemetryMeter struct {
	wrapped       metric.Meter
	counterCache  map[string]gocb.Counter
	recorderCache map[string]gocb.ValueRecorder
	lock          sync.Mutex
	provider      metric.MeterProvider
}

// NewOpenTelemetryMeter creates a new OpenTelemetryMeter.
func NewOpenTelemetryMeter(provider metric.MeterProvider) *OpenTelemetryMeter {
	return &OpenTelemetryMeter{
		wrapped:       provider.Meter("com.couchbase.client/go"),
		counterCache:  make(map[string]gocb.Counter),
		recorderCache: make(map[string]gocb.ValueRecorder),
		provider:      provider,
	}
}

func (meter *OpenTelemetryMeter) Wrapped() metric.Meter {
	return meter.wrapped
}

func (meter *OpenTelemetryMeter) Provider() metric.MeterProvider {
	return meter.provider
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
	defer meter.lock.Unlock()

	recorder := meter.recorderCache[key]
	if recorder == nil {
		unit := tags["__unit"]

		var labels []attribute.KeyValue
		for k, v := range tags {
			if strings.HasPrefix(k, "__") {
				// Ignore any 'reserved' attributes, such as `__unit`
				continue
			}
			labels = append(labels, attribute.String(k, v))
		}

		switch unit {
		case "s":
			otelHistogram, err := meter.wrapped.Float64Histogram(name, metric.WithUnit("s"))
			if err != nil {
				return nil, err
			}
			recorder = newOpenTelemetryMeterSecondsValueRecorder(context.Background(), otelHistogram, labels)
		default:
			otelHistogram, err := meter.wrapped.Int64Histogram(name)
			if err != nil {
				return nil, err
			}
			recorder = newOpenTelemetryMeterDefaultValueRecorder(context.Background(), otelHistogram, labels)
		}

		meter.recorderCache[key] = recorder
	}
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
	capped := num
	if num > uint64(math.MaxInt64) {
		log.Printf("IncrementBy: value %d exceeds int64 max, capping to %d", num, int64(math.MaxInt64))
		capped = uint64(math.MaxInt64)
	}
	nm.wrapped.Add(nm.ctx, int64(capped), metric.WithAttributes(nm.attributes...)) //nolint:gosec
}

type openTelemetryMeterDefaultValueRecorder struct {
	ctx        context.Context
	wrapped    metric.Int64Histogram
	attributes []attribute.KeyValue
}

func newOpenTelemetryMeterDefaultValueRecorder(ctx context.Context, valueRecorder metric.Int64Histogram, attributes []attribute.KeyValue) *openTelemetryMeterDefaultValueRecorder {
	return &openTelemetryMeterDefaultValueRecorder{
		ctx:        ctx,
		wrapped:    valueRecorder,
		attributes: attributes,
	}
}

func (nm *openTelemetryMeterDefaultValueRecorder) RecordValue(val uint64) {
	if val == 0 {
		return
	}
	capped := val
	if val > uint64(math.MaxInt64) {
		log.Printf("RecordValue: value %d exceeds int64 max, capping to %d", val, int64(math.MaxInt64))
		capped = uint64(math.MaxInt64)
	}
	nm.wrapped.Record(nm.ctx, int64(capped), metric.WithAttributes(nm.attributes...)) //nolint:gosec
}

type openTelemetryMeterSecondsValueRecorder struct {
	ctx        context.Context
	wrapped    metric.Float64Histogram
	attributes []attribute.KeyValue
}

func newOpenTelemetryMeterSecondsValueRecorder(ctx context.Context, valueRecorder metric.Float64Histogram, attributes []attribute.KeyValue) *openTelemetryMeterSecondsValueRecorder {
	return &openTelemetryMeterSecondsValueRecorder{
		ctx:        ctx,
		wrapped:    valueRecorder,
		attributes: attributes,
	}
}

func (nm *openTelemetryMeterSecondsValueRecorder) RecordValue(val uint64) {
	if val == 0 {
		return
	}
	// Values are provided by the SDK in microseconds, we must convert them to seconds
	nm.wrapped.Record(nm.ctx, float64(val)/1_000_000, metric.WithAttributes(nm.attributes...)) //nolint:gosec
}
