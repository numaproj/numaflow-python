To create the `wheel` file, refer [root](../../README.md)

## HOWTO build Image

```bash
docker build . -t quay.io/numaio/numaflow/pynumaflow-lite-sourcetransform-event-filter:v1 --load
```

Load it now to `k3d`

```bash
k3d image import quay.io/numaio/numaflow/pynumaflow-lite-sourcetransform-event-filter:v1
```

## Run the pipeline

```bash
kubectl apply -f pipeline.yaml
```

## About this example

This source transformer filters and routes messages based on their event time:

- **Messages before 2022**: Dropped
- **Messages within 2022**: Tagged with `within_year_2022` and event time set to Jan 1, 2022
- **Messages after 2022**: Tagged with `after_year_2022` and event time set to Jan 1, 2023

This demonstrates how source transformers can be used to:
1. Filter out old/stale data
2. Normalize event times
3. Route messages to different downstream vertices based on conditions

