# Example User Defined Sink

1. Build the docker image, and push
   ```shell
   make image
   // Privilege requried
   docker push quay.io/numaio/python-simplesink-example
   ```

2. Apply the pipeline
   ```shell
   kubectl apply -f pipeline-numaflow.yaml
   ```
