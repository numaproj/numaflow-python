# Example Python User Defined Function using Kafka sink

1. Install Kafka in the Kubernetes cluster
   ```shell
    kubectl apply -f https://raw.githubusercontent.com/numaproj/numaflow/main/config/apps/kafka/kafka-minimal.yaml
   ```

2. Build the docker image and import into k3d
   ```shell
   docker build -t test-python-udf:v1 . && k3d image import docker.io/library/test-python-udf:v1
   ```

3. Apply the pipeline
   ```shell
    kubectl apply -f pipeline-numaflow.yaml
   ```