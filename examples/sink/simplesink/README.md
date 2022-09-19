# Example User Defined Sink

1. Build the docker image and import into k3d
   ```shell
   docker build -t udsinkimg:v1 . && k3d image import docker.io/library/udsinkimg:v1
   ```

2. Apply the pipeline
   ```shell
   kubectl apply -f pipeline-numaflow.yaml
   ```
