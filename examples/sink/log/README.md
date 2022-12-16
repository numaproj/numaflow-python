# Example User Defined Sink

1. Build the docker image, and push
   ```shell
   make image
   # Privilege required
   docker push quay.io/numaio/numaflow-python/sink-log:latest
   ```

2. Apply the pipeline
   ```shell
   kubectl apply -f pipeline-numaflow.yaml
   ```
