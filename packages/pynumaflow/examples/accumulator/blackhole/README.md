# Blackhole Accumulator

An example User Defined Function that demonstrates a "blackhole" accumulator: it intentionally
discards every datum it receives without forwarding any data to the next vertex.

### Why emit drop messages instead of nothing?

An accumulator that simply reads its input and emits nothing leaves the framework unable to release
the per-datum tracked state (WAL), which leads to unbounded memory growth (see
[numaflow-python#356](https://github.com/numaproj/numaflow-python/issues/356)).

To get "blackhole" semantics without leaking memory, this example emits a *drop* message for every
datum using `Message.to_drop(datum)`. A drop message is not forwarded downstream, but it still lets
the framework advance the watermark and release the tracked state for that datum.

This pattern is useful for multiplexer-, cross-join-, or filter-style accumulators that legitimately
need to omit some (or all) of their inputs.

### Applying the Pipeline

To apply the pipeline, use the following command:

```shell
  kubectl apply -f pipeline.yaml
```

### Publish messages

Port-forward the HTTP endpoint, and make POST requests using curl. Remember to replace xxxx with the appropriate pod names.

```shell
  kubectl port-forward blackhole-http-one-0-xxxx 8444:8443

  # Post data to the HTTP endpoint
  curl -kq -X POST -d "101" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 60000"
  curl -kq -X POST -d "102" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 61000"
```

### Verify the output

```shell
  kubectl logs -f blackhole-py-sink-0-xxxx
```

The sink receives nothing - every datum is dropped by the accumulator - while the accumulator logs
each datum it drops.
