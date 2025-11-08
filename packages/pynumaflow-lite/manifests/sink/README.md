To create the `wheel` file, refer [root](../../README.md)

## HOWTO build Image

```bash
docker build . -t quay.io/numaio/numaflow/pynumaflow-lite-sink-log:v1 --load
```

Load it now to `k3d`

```bash
k3d image import quay.io/numaio/numaflow/pynumaflow-lite-sink-log:v1
```

## Run the pipeline

```bash
kubectl apply -f pipeline.yaml
```

## Example: Using different response types

The sink implementation supports all 5 response types:

```python
# Success - write was successful
responses.append(sinker.Response.as_success(msg.id))

# Failure - write failed with error message
responses.append(sinker.Response.as_failure(msg.id, "Database connection failed"))

# Fallback - forward to fallback sink
responses.append(sinker.Response.as_fallback(msg.id))

# Serve - write to serving store with payload
responses.append(sinker.Response.as_serve(msg.id, b"serving data"))

# OnSuccess - forward to onSuccess store with optional message
message = sinker.Message(
    value=b"processed data",
    keys=["key1", "key2"],
    user_metadata={"meta": sinker.KeyValueGroup.from_dict({"k": b"v"})}
)
responses.append(sinker.Response.as_on_success(msg.id, message))
```

