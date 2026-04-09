# Python SDK for Numaflow

[![Build](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml/badge.svg)](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow-python?label=pynumaflow)](https://github.com/numaproj/numaflow-python/releases/latest)

`pynumaflow` is the Python SDK for [Numaflow](https://numaflow.numaproj.io/), a Kubernetes-native stream processing framework. Write a Python function, wire it to a server class, and Numaflow handles the gRPC transport, autoscaling, and deployment — no boilerplate required. The SDK supports three execution models (synchronous, asynchronous, and multi-process) and both function-based and class-based handler styles.

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

| | Description | Examples |
|---|---|---|
| [**User-Defined Functions (UDFs)**](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/) | Process and transform stream data — Map, Reduce, Reduce Stream, Map Stream, Batch Map, Accumulator | [Map](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/map) · [Reduce](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reduce) · [Reduce Stream](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reducestream) · [Map Stream](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/mapstream) · [Batch Map](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/batchmap) · [Accumulator](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/accumulator) |
| [**User-Defined Sources (UDSource)**](https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/) | Ingest data from custom sources with read, ack, pending, and partition handlers | [Source](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source) · [Source Transform](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sourcetransform) |
| [**User-Defined Sinks (UDSink)**](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/) | Deliver data to custom destinations with per-message acknowledgment | [Sink](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink) |
| [**Side Inputs**](https://numaflow.numaproj.io/specifications/side-inputs/) | Broadcast slow-changing reference data to UDF vertices without passing it through the pipeline | [Side Input](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sideinput) |

## Choosing Your Server Type

Each functionality is served by a dedicated server class. Choose the server type that matches your workload characteristics:

| | **Sync** | **Async** | **MultiProcess** |
|---|---|---|---|
| **Concurrency Model** | Multithreaded | asyncio event loop | N forked server processes |
| **Handler Signature** | `def handler(...)` | `async def handler(...)` | `def handler(...)` |
| **GIL Behaviour** | Subject to GIL | Subject to GIL | Bypasses Python GIL |
| **Extra Config** | — | — | `server_count=N` required |
| **Typical Workloads** | Stateless transforms | I/O-bound operations | CPU-intensive operations |

> [!TIP]
> Both Sync and MultiProcess handlers use a plain `def` — only Async requires `async def`.

> [!IMPORTANT]
> MultiProcess forks N independent gRPC servers within the same container. The `server_count` parameter is required: `MapMultiprocServer(handler, server_count=2)`.

## Server Class Reference

| Functionality | Server Class(es) |
|---|---|
| [**UDSource**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source) | [SourceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/source/simple_source/example.py) |
| [**UDSink**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink) | [SinkServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/log/example.py), [SinkAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/async_log/example.py) |
| [**Side Input**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sideinput) | [SideInputServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sideinput/simple_sideinput/example.py) |
| [**Map**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/map) | [MapServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/even_odd/example.py), [MapAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/async_forward_message/example.py), [MapMultiprocServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/multiproc_map/example.py) |
| [**Reduce**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reduce) | [ReduceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reduce/counter/example.py) |
| [**Reduce Stream**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reducestream) | [ReduceStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reducestream/counter/example.py) |
| [**Map Stream**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/mapstream) | [MapStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/mapstream/flatmap_stream/example.py) |
| [**Batch Map**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/batchmap) | [BatchMapAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/batchmap/flatmap/example.py) |
| [**Accumulator**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/accumulator) | [AccumulatorAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/accumulator/streamsorter/example.py) |
| [**Source Transform**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sourcetransform) | [SourceTransformServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/event_time_filter/example.py), [SourceTransformAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/async_event_time_filter/example.py), SourceTransformMultiProcServer |

All server types accept handlers in two styles:

- **Function-based** — pass a plain `def` or `async def` directly to the server. Best for simple, stateless logic.
- **Class-based** — inherit from the corresponding base class (e.g., `Mapper`, `Reducer`, `Sinker`) and implement the `handler` method. Useful when your handler needs initialization arguments, internal state, or helper methods.

The linked examples above demonstrate both styles for each functionality.

## Contributing

For SDK development workflow, testing against a live pipeline, and adding new examples, see the [Developer Guide](../../development.md).
For general contribution guidelines, see the [Numaproj Contributing Guide](https://github.com/numaproj/numaproj/blob/main/CONTRIBUTING.md).
