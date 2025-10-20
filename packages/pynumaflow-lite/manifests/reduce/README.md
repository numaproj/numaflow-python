To create the `wheel` file, refer [root](../../README.md)

## HOWTO build Image

```bash
docker build . -t quay.io/numaio/numaflow/pynumaflow-lite-reduce-counter:v1 --load
```

Load it now to `k3d`

### `k3d`

```bash
k3d image import quay.io/numaio/numaflow/pynumaflow-lite-reduce-counter:v1
```

### Minikube

```bash
minikube image load quay.io/numaio/numaflow/pynumaflow-lite-reduce-counter:v1
```

#### Delete image from minikube

`minikube` doesn't like pushing the same image over, delete and load if you are using
the same tag.

```bash
minikube image rm quay.io/numaio/numaflow/pynumaflow-lite-reduce-counter:v1
```

## Run the pipeline

```bash
kubectl apply -f pipeline.yaml
```

