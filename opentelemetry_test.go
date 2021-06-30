package gocbopentelemetry

import (
	"context"
	"flag"
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/export/trace"
	"go.opentelemetry.io/otel/sdk/export/trace/tracetest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"os"
	"sort"
	"testing"
	"time"
)

func envFlagString(envName, name, value, usage string) *string {
	envValue := os.Getenv(envName)
	if envValue != "" {
		value = envValue
	}
	return flag.String(name, value, usage)
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
	bsp.ForceFlush()
	exporter.Reset()

	ctx, span := tracer.Start(ctx, "myparentoperation")
	_, err = col.Upsert("someid", "someval", &gocb.UpsertOptions{
		ParentSpan: NewOpenTelemetryRequestSpan(ctx, span),
	})
	require.Nil(t, err)
	span.End()

	bsp.ForceFlush()
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
			Value: attribute.IntValue(0),
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

func assertOTSpan(t *testing.T, span *trace.SpanSnapshot, name string, attribs []attribute.KeyValue) {
	assert.NotZero(t, span.StartTime)
	assert.NotZero(t, span.EndTime)
	assert.Equal(t, name, span.Name)

	if assert.Len(t, span.Attributes, len(attribs)) {
		for _, attrib := range attribs {
			found := false
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
}
