package gocbopentelemetry

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/gocbcore/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
}

type clusterLabels struct {
	ClusterName string `json:"clusterName"`
	ClusterUuid string `json:"clusterUUID"`
}

func getClusterLabels(cluster *gocb.Cluster) (*clusterLabels, error) {
	agent, err := cluster.Bucket(bucket).Internal().IORouter()
	if err != nil {
		return nil, err
	}
	var res *clusterLabels
	var ch = make(chan struct{})
	var errOut error
	_, err = agent.DoHTTPRequest(&gocbcore.HTTPRequest{
		Service: gocbcore.MgmtService,
		Method:  "GET",
		Path:    "/pools/default/nodeServices",
	}, func(resp *gocbcore.HTTPResponse, httpErr error) {
		defer close(ch)
		if httpErr != nil {
			errOut = httpErr
			return
		}
		var body []byte
		body, errOut = io.ReadAll(resp.Body)
		resp.Body.Close()
		if errOut != nil {
			return
		}
		errOut = json.Unmarshal(body, &res)
	})
	if err != nil {
		close(ch)
		return nil, err
	}
	<-ch
	if errOut != nil {
		return nil, errOut
	}
	return res, nil
}

var server, user, password, bucket string

func TestMain(m *testing.M) {
	serverFlag := envFlagString("GOCBSERVER", "server", "localhost",
		"The connection string to connect to for a real server")
	userFlag := envFlagString("GOCBUSER", "user", "Administrator",
		"The username to use to authenticate when using a real server")
	passwordFlag := envFlagString("GOCBPASS", "pass", "password",
		"The password to use to authenticate when using a real server")
	bucketFlag := envFlagString("GOCBBUCKET", "bucket", "default",
		"The bucket to use to test against")
	flag.Parse()

	server = *serverFlag
	user = *userFlag
	password = *passwordFlag
	bucket = *bucketFlag

	result := m.Run()
	os.Exit(result)
}

func TestOpenTelemetryTracer(t *testing.T) {
	gocb.SetLogger(gocb.VerboseStdioLogger())
	ctx := context.Background()
	exporter := tracetest.NewInMemoryExporter()
	defer exporter.Shutdown(ctx)
	bsp := sdktrace.NewSimpleSpanProcessor(exporter)
	defer bsp.Shutdown(ctx)
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)

	tracer := tp.Tracer("test-demo")

	cluster, err := gocb.Connect(server, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: user,
			Password: password,
		},
		Tracer: NewOpenTelemetryRequestTracer(tp),
	})
	require.Nil(t, err)
	defer cluster.Close(nil)

	b := cluster.Bucket(bucket)
	err = b.WaitUntilReady(5*time.Second, nil)
	require.Nil(t, err, err)

	col := b.DefaultCollection()

	// First operation to ensure that cid fetches have already happened and that the connections are good to go.
	_, err = col.Upsert("someid", "someval", nil)
	require.Nil(t, err)

	// Force flush the processor and then reset the exporter so that we only get spans that we want.
	assert.NoError(t, bsp.ForceFlush(ctx))
	exporter.Reset()

	ctx, span := tracer.Start(ctx, "myparentoperation")
	_, err = col.Upsert("someid", "someval", &gocb.UpsertOptions{
		ParentSpan: NewOpenTelemetryRequestSpan(ctx, span),
	})
	require.Nil(t, err)
	span.End()

	assert.NoError(t, bsp.ForceFlush(ctx))
	spans := exporter.GetSpans()
	if len(spans) != 5 {
		t.Fatalf("Expected 5 spans but got %d", len(spans))
	} // myparentoperation, upsert, encoding, CMD_SET, dispatch

	sort.SliceStable(spans, func(i, j int) bool {
		return spans[i].StartTime.Before(spans[j].StartTime)
	})

	labels, err := getClusterLabels(cluster)
	require.Nil(t, err)

	assertOTSpan(t, spans[0], "myparentoperation", trace.SpanKindUnspecified, []attribute.KeyValue{})
	assertOTSpan(t, spans[1], "upsert", trace.SpanKindClient, []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
		{
			Key:   "db.couchbase.cluster_uuid",
			Value: attribute.StringValue(labels.ClusterUuid),
		},
		{
			Key:   "db.couchbase.cluster_name",
			Value: attribute.StringValue(labels.ClusterName),
		},
		{
			Key:   "db.couchbase.service",
			Value: attribute.StringValue("kv"),
		},
		{
			Key:   "db.name",
			Value: attribute.StringValue(b.Name()),
		},
		{
			Key:   "db.couchbase.scope",
			Value: attribute.StringValue("_default"),
		},
		{
			Key:   "db.couchbase.collection",
			Value: attribute.StringValue("_default"),
		},
		{
			Key:   "db.operation",
			Value: attribute.StringValue("upsert"),
		},
	})
	assertOTSpan(t, spans[2], "request_encoding", trace.SpanKindClient, []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
		{
			Key:   "db.couchbase.cluster_uuid",
			Value: attribute.StringValue(labels.ClusterUuid),
		},
		{
			Key:   "db.couchbase.cluster_name",
			Value: attribute.StringValue(labels.ClusterName),
		},
	})
	assertOTSpan(t, spans[3], "CMD_SET", trace.SpanKindClient, []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
		{
			Key:   "db.couchbase.retries",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "db.couchbase.cluster_uuid",
			Value: attribute.StringValue(labels.ClusterUuid),
		},
		{
			Key:   "db.couchbase.cluster_name",
			Value: attribute.StringValue(labels.ClusterName),
		},
	})
	assertOTSpan(t, spans[4], "dispatch_to_server", trace.SpanKindClient, []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
		{
			Key:   "db.couchbase.cluster_uuid",
			Value: attribute.StringValue(labels.ClusterUuid),
		},
		{
			Key:   "db.couchbase.cluster_name",
			Value: attribute.StringValue(labels.ClusterName),
		},
		{
			Key:   "net.transport",
			Value: attribute.StringValue("IP.TCP"),
		},
		{
			Key:   "db.couchbase.operation_id",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "db.couchbase.local_id",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "net.host.name",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "net.host.port",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "net.peer.name",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "net.peer.port",
			Value: attribute.StringValue(""),
		},
		{
			Key:   "db.couchbase.server_duration",
			Value: attribute.IntValue(0),
		},
	})
}

func TestOpenTelemetryMeter(t *testing.T) {
	gocb.SetLogger(gocb.VerboseStdioLogger())

	rdr := metric.NewManualReader()

	provider := metric.NewMeterProvider(
		metric.WithReader(rdr),
	)

	cluster, err := gocb.Connect(server, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: user,
			Password: password,
		},
		Meter: NewOpenTelemetryMeter(provider),
	})
	require.Nil(t, err)
	defer cluster.Close(nil)

	b := cluster.Bucket(bucket)
	err = b.WaitUntilReady(5*time.Second, nil)
	require.Nil(t, err, err)

	col := b.DefaultCollection()

	_, err = col.Upsert("someid", "someval", nil)
	require.Nil(t, err)

	_, err = col.Get("someid", nil)
	require.Nil(t, err)

	var data metricdata.ResourceMetrics
	err = rdr.Collect(context.Background(), &data)
	require.Nil(t, err)

	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)

	histogram, ok := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[int64])
	require.True(t, ok)

	labels, err := getClusterLabels(cluster)
	require.Nil(t, err)

	assertOTMetric(t, histogram.DataPoints[0], "upsert", labels)
	assertOTMetric(t, histogram.DataPoints[1], "get", labels)
}

func assertOTSpan(t *testing.T, span tracetest.SpanStub, name string, kind trace.SpanKind, attribs []attribute.KeyValue) {
	assert.NotZero(t, span.StartTime)
	assert.NotZero(t, span.EndTime)
	assert.Equal(t, name, span.Name)
	if kind != trace.SpanKindUnspecified {
		assert.Equal(t, kind, span.SpanKind)
	}

	require.Len(t, span.Attributes, len(attribs))
	for _, attrib := range attribs {
		var found bool
		for _, a := range span.Attributes {
			if attrib.Key == a.Key {
				// otel doesn't have a nil value type so we have to use empty string.
				if attrib.Value.AsString() == "" {
					assert.NotEmpty(t, a.Value)
				} else {
					assert.Equal(t, attrib.Value, a.Value)
				}
				found = true
				break
			}
		}
		assert.True(t, found, fmt.Sprintf("key not found: %s", attrib.Key))
	}
}

func assertOTMetric(t *testing.T, metric metricdata.HistogramDataPoint[int64], name string, labels *clusterLabels) {
	require.Equal(t, 8, metric.Attributes.Len())
	expectedKeys := []attribute.KeyValue{
		attribute.String("db.couchbase.service", "kv"),
		attribute.String("db.operation", name),
		attribute.String("db.name", bucket),
		attribute.String("db.couchbase.scope", "_default"),
		attribute.String("db.couchbase.collection", "_default"),
		attribute.String("db.couchbase.cluster_name", labels.ClusterName),
		attribute.String("db.couchbase.cluster_uuid", labels.ClusterUuid),
		attribute.String("outcome", "Success"),
	}

	for _, val := range expectedKeys {
		v, found := metric.Attributes.Value(val.Key)
		assert.True(t, found)
		assert.Equal(t, val.Value, v)
	}

	require.EqualValues(t, metric.Count, 1)
}

func TestOpenTelemetryMetricsInSeconds(t *testing.T) {
	rdr := metric.NewManualReader()

	provider := metric.NewMeterProvider(
		metric.WithReader(rdr),
	)

	meter := NewOpenTelemetryMeter(provider)
	recorder, err := meter.ValueRecorder("test_recorder", map[string]string{
		"foo":    "bar",
		"__unit": "s",
	})
	require.Nil(t, err)

	recorder.RecordValue(2_000_000)
	recorder.RecordValue(500_000)

	var data metricdata.ResourceMetrics
	err = rdr.Collect(context.Background(), &data)
	require.Nil(t, err)

	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "s", data.ScopeMetrics[0].Metrics[0].Unit)

	histogram, ok := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[float64])
	require.True(t, ok)

	require.Len(t, histogram.DataPoints, 1)

	dataPoint := histogram.DataPoints[0]
	assert.Equal(t, 2.5, dataPoint.Sum)
	assert.Equal(t, uint64(2), dataPoint.Count)

	assert.Equal(t, attribute.NewSet(attribute.String("foo", "bar")), dataPoint.Attributes)
}
