# Couchbase Gocb Opentelemetry

This is the official Couchbase Go OpenTelemetry wrapper for use with the [http://github.com/couchbase/gocb] (Couchbase Go Client).

This repository provides wrappers for tracing and metrics and will remain v0.x until a time when Opentelemetry Go itself is stable.

## Installing

```bash
go get github.com/couchbase/gocb-opentelemetry
```

## Tracing

```go
tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(bsp))
defer tp.Shutdown(ctx)

cluster, err := gocb.Connect(server, gocb.ClusterOptions{
    Authenticator: gocb.PasswordAuthenticator{
        Username: user,
        Password: password,
    },
    Tracer: NewOpenTelemetryRequestTracer(tp),
})
if err != nil {
	panic(err)
}
defer cluster.Close(nil)

b := cluster.Bucket(bucket)
err = b.WaitUntilReady(5*time.Second, nil)
if err != nil {
    panic(err)
}

ctx, span := tracer.Start(ctx, "myparentoperation")
_, err = col.Upsert("someid", "someval", &gocb.UpsertOptions{
    ParentSpan: NewOpenTelemetryRequestSpan(ctx, span),
})
span.End()
if err != nil {
    panic(err)
}
```

Note that you do not need to call `End` on wrapped spans, the returned `RequestSpan` is just a wrapper around the span provided.

