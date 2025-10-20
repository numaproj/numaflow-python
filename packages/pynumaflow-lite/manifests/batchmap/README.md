To create the `wheel` file, refer [root](../../README.md)

## HOWTO build Image

```bash
docker build . -t quay.io/numaio/numaflow/pynumaflow-lite-batchmap-cat:v2 --load
```

Load it now to `k3d`

```bash
k3d image import quay.io/numaio/numaflow/pynumaflow-lite-batchmap-cat:v2
```

## Run the pipeline

```bash
kubectl apply -f pipeline.yaml
```