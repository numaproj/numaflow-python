# Stream Sorter

An example User Defined Function that sorts the incoming stream by event time.

### Applying the Pipeline

To apply the pipeline, use the following command:

```shell
  kubectl apply -f manifest/stream-sorter-pipeline.yaml
```

### Publish messages

Port-forward the HTTP endpoint, and make POST requests using curl. Remember to replace xxxx with the appropriate pod names.

```shell
  kubectl port-forward stream-sorter-http-one-0-xxxx 8444:8443

  # Post data to the HTTP endpoint
  curl -kq -X POST -d "101" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 60000"
  curl -kq -X POST -d "102" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 61000"
  curl -kq -X POST -d "103" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 62000"
  curl -kq -X POST -d "104" https://localhost:8444/vertices/http-one -H "X-Numaflow-Event-Time: 63000"
```

```shell
  kubectl port-forward stream-sorter-http-two-0-xxxx 8445:8443

  # Post data to the HTTP endpoint
  curl -kq -X POST -d "105" https://localhost:8445/vertices/http-two -H "X-Numaflow-Event-Time: 70000"
  curl -kq -X POST -d "106" https://localhost:8445/vertices/http-two -H "X-Numaflow-Event-Time: 71000"
  curl -kq -X POST -d "107" https://localhost:8445/vertices/http-two -H "X-Numaflow-Event-Time: 72000"
  curl -kq -X POST -d "108" https://localhost:8445/vertices/http-two -H "X-Numaflow-Event-Time: 73000"
```

### Verify the output

```shell
  kubectl logs -f stream-sorter-log-sink-0-xxxx
```

The output should be sorted by event time.