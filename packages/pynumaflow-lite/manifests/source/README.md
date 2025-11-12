To create the `wheel` file, refer [root](../../README.md)

## HOWTO build Image

```bash
docker build . -t quay.io/numaio/numaflow/pynumaflow-lite-simple-source:v1 --load
```

### `k3d`

Load it now to `k3d`

```bash
k3d image import quay.io/numaio/numaflow/pynumaflow-lite-simple-source:v1
```

### Minikube

```bash
minikube image load quay.io/numaio/numaflow/pynumaflow-lite-simple-source:v1
```

## Run the pipeline

```bash
kubectl apply -f pipeline.yaml
```

