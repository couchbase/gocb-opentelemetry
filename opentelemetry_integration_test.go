package gocbopentelemetry

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var ( // nolint: gochecknoglobals
	user               = "couchbase"
	password           = "couchbase"
	integrationCleanup func() error
	integrationOnce    sync.Once
	bucketMetrics      = ""
	bucketTracer       = ""
	server             = ""
)

func TestMain(m *testing.M) {
	code := m.Run()
	if integrationCleanup != nil {
		if err := integrationCleanup(); err != nil {
			panic(err)
		}
	}

	os.Exit(code)
}

func requireCouchbase(tb testing.TB) {
	integrationOnce.Do(func() {
		pool, resource, err := setupCouchbase(tb)
		require.NoError(tb, err)

		port := resource.GetPort("11210/tcp")
		integrationCleanup = func() error {
			return pool.Purge(resource)
		}

		server = fmt.Sprintf("couchbase://localhost:%v", port)
		bucketMetrics = fmt.Sprintf("testing-couchbase-metrics-%d", time.Now().Unix())
		require.NoError(tb, createBucket(context.Background(), tb, bucketMetrics))
		tb.Cleanup(func() {
			require.NoError(tb, removeBucket(context.Background(), tb, bucketMetrics))
		})

		bucketTracer = fmt.Sprintf("testing-couchbase-tracer-%d", time.Now().Unix())
		require.NoError(tb, createBucket(context.Background(), tb, bucketTracer))
		tb.Cleanup(func() {
			require.NoError(tb, removeBucket(context.Background(), tb, bucketTracer))
		})
	})

}

func setupCouchbase(tb testing.TB) (*dockertest.Pool, *dockertest.Resource, error) {
	tb.Log("setup couchbase cluster")

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, nil, err
	}

	pwd, err := os.Getwd()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get working directory: %w", err)
	}

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "couchbase",
		Tag:        "latest",
		Cmd:        []string{"/opt/couchbase/configure-server.sh"},
		Env: []string{
			"CLUSTER_NAME=couchbase",
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_USERNAME=%s", user),
			fmt.Sprintf("COUCHBASE_ADMINISTRATOR_PASSWORD=%s", password),
		},
		Mounts: []string{
			fmt.Sprintf("%s/configure-server.sh:/opt/couchbase/configure-server.sh", pwd),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8091/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "8091",
				},
			},
			"11210/tcp": {
				{
					HostIP: "0.0.0.0", HostPort: "11210",
				},
			},
		},
	})
	if err != nil {
		return nil, nil, err
	}

	// Look for readyness
	var stderr bytes.Buffer
	time.Sleep(15 * time.Second)
	for range 5 {
		time.Sleep(time.Second)
		exitCode, err := resource.Exec([]string{"/usr/bin/cat", "/is-ready"}, dockertest.ExecOptions{
			StdErr: &stderr, // without stderr exit code is not reported
		})
		if exitCode == 0 && err == nil {
			break
		}
		tb.Log(err)
	}

	tb.Log("couchbase cluster ready")

	return pool, resource, nil
}

func createBucket(_ context.Context, _ testing.TB, bucket string) error {
	cluster, err := gocb.Connect(server, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: user,
			Password: password,
		},
	})
	if err != nil {
		return err
	}

	err = cluster.WaitUntilReady(time.Second*10, nil)
	if err != nil {
		return err
	}

	err = cluster.Buckets().CreateBucket(gocb.CreateBucketSettings{
		BucketSettings: gocb.BucketSettings{
			Name:       bucket,
			RAMQuotaMB: 100, // smallest value and allow max 10 running bucket with cluster-ramsize 1024 from setup script
			BucketType: gocb.CouchbaseBucketType,
		},
	}, nil)
	if err != nil {
		return err
	}

	for range 5 { // try five time
		time.Sleep(time.Second)
		err = cluster.Bucket(bucket).WaitUntilReady(time.Second*10, nil)
		if err == nil {
			break
		}
	}

	return err
}

func removeBucket(ctx context.Context, _ testing.TB, bucket string) error {
	cluster, err := gocb.Connect(server, gocb.ClusterOptions{
		Authenticator: gocb.PasswordAuthenticator{
			Username: user,
			Password: password,
		},
	})
	if err != nil {
		return err
	}

	return cluster.Buckets().DropBucket(bucket, &gocb.DropBucketOptions{
		Context: ctx,
	})
}

func TestOpenTelemetry(t *testing.T) {
	requireCouchbase(t)

	t.Run("tracer", testOpenTelemetryTracer)
	t.Run("meter", testOpenTelemetryMeter)
}

func testOpenTelemetryTracer(t *testing.T) {
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

	b := cluster.Bucket(bucketTracer)
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

	assertOTSpan(t, spans[0], "myparentoperation", []attribute.KeyValue{})
	assertOTSpan(t, spans[1], "upsert", []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
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
	assertOTSpan(t, spans[2], "request_encoding", []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
	})
	assertOTSpan(t, spans[3], "CMD_SET", []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
		},
		{
			Key:   "db.couchbase.retries",
			Value: attribute.StringValue(""),
		},
	})
	assertOTSpan(t, spans[4], "dispatch_to_server", []attribute.KeyValue{
		{
			Key:   "db.system",
			Value: attribute.StringValue("couchbase"),
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
			Value: attribute.StringValue(""),
		},
	})
}

func testOpenTelemetryMeter(t *testing.T) {
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

	b := cluster.Bucket(bucketMetrics)
	err = b.WaitUntilReady(5*time.Second, nil)
	require.Nil(t, err, err)

	col := b.DefaultCollection()

	_, err = col.Upsert("someid", "someval", nil)
	require.Nil(t, err)

	_, err = col.Get("someid", nil)
	require.Nil(t, err)

	var data metricdata.ResourceMetrics
	_ = rdr.Collect(context.Background(), &data)

	require.Len(t, data.ScopeMetrics, 1)
	require.Len(t, data.ScopeMetrics[0].Metrics, 1)

	histogram, ok := data.ScopeMetrics[0].Metrics[0].Data.(metricdata.Histogram[int64])
	require.True(t, ok)

	assertOTMetric(t, histogram.DataPoints[1], "get")
	assertOTMetric(t, histogram.DataPoints[0], "upsert")

}

func assertOTSpan(t *testing.T, span tracetest.SpanStub, name string, attribs []attribute.KeyValue) {
	assert.NotZero(t, span.StartTime)
	assert.NotZero(t, span.EndTime)
	assert.Equal(t, name, span.Name)

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

func assertOTMetric(t *testing.T, metric metricdata.HistogramDataPoint[int64], name string) {
	require.EqualValues(t, metric.Attributes.Len(), 2)
	expectedKeys := []attribute.KeyValue{
		attribute.String("db.couchbase.service", "kv"),
		attribute.String("db.operation", name),
	}

	for _, val := range expectedKeys {
		v, found := metric.Attributes.Value(val.Key)
		assert.True(t, found)
		assert.Equal(t, val.Value, v)
	}

	require.EqualValues(t, metric.Count, 1)
}
