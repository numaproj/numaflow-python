# Example User Defined Sink

1. Build the docker image, and push
   ```shell
   make image
   # Privilege required
   docker push quay.io/numaio/simplesink-example:python
   ```

2. Apply the pipeline
   ```shell
   kubectl apply -f pipeline-numaflow.yaml
   ```
