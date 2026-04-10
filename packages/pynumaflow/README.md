# Python SDK for Numaflow

[![Build](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml/badge.svg)](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow-python?label=pynumaflow)](https://github.com/numaproj/numaflow-python/releases/latest)

`pynumaflow` is the Python SDK for [Numaflow](https://numaflow.numaproj.io/), a Kubernetes-native stream processing framework. Write a Python function, wire it to a server class, and Numaflow handles the gRPC transport, autoscaling, and deployment — no boilerplate required. The SDK supports synchronous and asynchronous execution models, and both function-based and class-based handler styles.

## Installation

```bash
pip install pynumaflow
```

<details>
<summary>Build &amp; develop locally</summary>

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and packaging.
To build the package locally, run the following command from the root of the project.

```bash
make setup
```

To run unit tests:
```bash
make test
```

To format code style using black and ruff:
```bash
make lint
```

Setup [pre-commit](https://pre-commit.com/) hooks:
```bash
pre-commit install
```

</details>

## Capabilities

The SDK covers the full range of Numaflow extension points. Each capability maps to a dedicated set of server classes and handler interfaces.

> [!TIP]
> Each capability below links to working examples in both function-based and class-based handler styles. See the full [examples directory](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples) for all implementations.

| | Description | API Reference |
|---|---|---|
| [**User-Defined Functions (UDFs)**](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) | Process and transform stream data — Map, Reduce, Reduce Stream, Map Stream, Batch Map, Accumulator | [Map](https://numaproj.io/numaflow-python/latest/api/mapper/) · [Reduce](https://numaproj.io/numaflow-python/latest/api/reducer/) · [Reduce Stream](https://numaproj.io/numaflow-python/latest/api/reducestreamer/) · [Map Stream](https://numaproj.io/numaflow-python/latest/api/mapstreamer/) · [Batch Map](https://numaproj.io/numaflow-python/latest/api/batchmapper/) · [Accumulator](https://numaproj.io/numaflow-python/latest/api/accumulator/) |
| [**User-Defined Sources (UDSource)**](https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/) | Ingest data from custom sources with read, ack, pending, and partition handlers | [Sourcer](https://numaproj.io/numaflow-python/latest/api/sourcer/) · [Source Transform](https://numaproj.io/numaflow-python/latest/api/sourcetransformer/) |
| [**User-Defined Sinks (UDSink)**](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) | Deliver data to custom destinations with per-message acknowledgment | [Sinker](https://numaproj.io/numaflow-python/latest/api/sinker/) |
| [**Side Inputs**](https://numaflow.numaproj.io/specifications/side-inputs/) | Broadcast slow-changing reference data to UDF vertices without passing it through the pipeline | [Side Input](https://numaproj.io/numaflow-python/latest/api/sideinput/) |

## Choosing Your Server Type

Each functionality is served by a dedicated server class. Choose the server type that matches your workload characteristics:

| | **Sync** | **Async** |
|---|---|---|
| **Concurrency Model** | Multithreaded | asyncio event loop |
| **Handler Signature** | `def handler(...)` | `async def handler(...)` |
| **GIL Behaviour** | Subject to GIL | Subject to GIL |
| **Typical Workloads** | Stateless transforms | I/O-bound operations |

## Server Class Reference

| Functionality | Server Class(es) |
|---|---|
| [**UDSource**](https://numaproj.io/numaflow-python/latest/api/sourcer/) | [SourceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/source/simple_source/example.py) |
| [**UDSink**](https://numaproj.io/numaflow-python/latest/api/sinker/) | [SinkServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/log/example.py), [SinkAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/async_log/example.py) |
| [**Side Input**](https://numaproj.io/numaflow-python/latest/api/sideinput/) | [SideInputServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sideinput/simple_sideinput/example.py) |
| [**Map**](https://numaproj.io/numaflow-python/latest/api/mapper/) | [MapServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/even_odd/example.py), [MapAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/async_forward_message/example.py) |
| [**Reduce**](https://numaproj.io/numaflow-python/latest/api/reducer/) | [ReduceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reduce/counter/example.py) |
| [**Reduce Stream**](https://numaproj.io/numaflow-python/latest/api/reducestreamer/) | [ReduceStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reducestream/counter/example.py) |
| [**Map Stream**](https://numaproj.io/numaflow-python/latest/api/mapstreamer/) | [MapStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/mapstream/flatmap_stream/example.py) |
| [**Batch Map**](https://numaproj.io/numaflow-python/latest/api/batchmapper/) | [BatchMapAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/batchmap/flatmap/example.py) |
| [**Accumulator**](https://numaproj.io/numaflow-python/latest/api/accumulator/) | [AccumulatorAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/accumulator/streamsorter/example.py) |
| [**Source Transform**](https://numaproj.io/numaflow-python/latest/api/sourcetransformer/) | [SourceTransformServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/event_time_filter/example.py), [SourceTransformAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/async_event_time_filter/example.py) |

All server types accept handlers in two styles:

- **Function-based** — pass a plain `def` or `async def` directly to the server. Best for simple, stateless logic.
- **Class-based** — inherit from the corresponding base class (e.g., `Mapper`, `Reducer`, `Sinker`) and implement the `handler` method. Useful when your handler needs initialization arguments, internal state, or helper methods.

The linked examples above demonstrate both styles for each functionality.

## Contributing

For SDK development workflow, testing against a live pipeline, and adding new examples, see the [Developer Guide](../../development.md).
For general contribution guidelines, see the [Numaproj Contributing Guide](https://github.com/numaproj/numaproj/blob/main/CONTRIBUTING.md).
