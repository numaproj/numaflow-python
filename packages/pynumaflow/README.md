# Python SDK for Numaflow

[![Build](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml/badge.svg)](https://github.com/numaproj/numaflow-python/actions/workflows/run-tests.yml)
[![black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Release Version](https://img.shields.io/github/v/release/numaproj/numaflow-python?label=pynumaflow)](https://github.com/numaproj/numaflow-python/releases/latest)

This is the Python SDK for [Numaflow](https://numaflow.numaproj.io/).

This SDK provides the interface for writing different functionalities of Numaflow like [UDFs](https://numaflow.numaproj.io/user-guide/user-defined-functions/user-defined-functions/), [UDSinks](https://numaflow.numaproj.io/user-guide/sinks/user-defined-sinks/), [UDSources](https://numaflow.numaproj.io/user-guide/sources/user-defined-sources/) and [SideInput](https://numaflow.numaproj.io/specifications/side-inputs/) in Python.

## Installation

Install the package using pip.
```bash
pip install pynumaflow
```

### Build locally

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

## Implementing different functionalities

- [Implement User Defined Sources](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source)
- Implement User Defined Functions
    - [Map](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/map)
    - [Reduce](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reduce)
    - [Reduce Stream](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reducestream)
    - [Map Stream](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/mapstream)
    - [Batch Map](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/batchmap)
    - [Accumulator](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/accumulator)
    - [Source Transform](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sourcetransform)
- [Implement User Defined Sinks](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink)
- [Implement User Defined SideInputs](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sideinput)

## Server Types

There are different types of gRPC server mechanisms which can be used to serve the UDFs, UDSinks and UDSource.
These have different functionalities and are used for different use cases.

Currently we support the following server types:

- Sync Server
- Asynchronous Server
- MultiProcess Server

Not all of the above are supported for all UDFs, UDSource and UDSinks.

For each of the UDFs, UDSource and UDSinks, there are separate classes for each of the server types.
This helps in keeping the interface simple and easy to use, and the user can start the specific server type based on the use case.

### Sync Server

Synchronous Server is the simplest server type. It is a multithreaded server which can be used for simple UDFs and UDSinks.
Here the server will invoke the handler function for each message. The messaging is synchronous and the server will wait for the handler to return before processing the next message.

```py
grpc_server = MapServer(handler)
```

### Async Server

Asynchronous Server is a server which can be used for UDFs that are asynchronous. Here we utilize the asynchronous capabilities of Python to process multiple messages in parallel. The server will invoke the handler function for each message and will not wait for the handler to return before processing the next message.
The handler function for such a server should be an async function.

```py
grpc_server = MapAsyncServer(handler)
```

### MultiProcess Server

MultiProcess Server is a multi-process server which can be used for UDFs that are CPU intensive. Here we utilize the multi-processing capabilities of Python to process multiple messages in parallel by forking multiple servers in different processes.
The server will invoke the handler function for each message. Individually at the server level the messaging is synchronous, but since multiple servers run in parallel, the overall throughput also scales in parallel.

This is an alternative to creating multiple replicas of the same UDF container, as it uses multi-processing within the same container.

```py
grpc_server = MapMultiprocServer(mapper_instance=handler, server_count=2)
```

### Supported Server Types and Handler Classes

The table below lists the server classes and handler base class for each functionality.

| Functionality | Server Class(es) | Handler Base Class |
|---|---|---|
| [**UDSource**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/source) | [SourceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/source/simple_source/example.py) | Sourcer |
| [**UDSink**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sink) | [SinkServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/log/example.py), [SinkAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sink/async_log/example.py) | Sinker |
| [**SideInput**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sideinput) | [SideInputServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sideinput/simple_sideinput/example.py) | SideInput |
| [**Map**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/map) | [MapServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/even_odd/example.py), MapAsyncServer, [MapMultiprocServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/map/multiproc_map/example.py) | Mapper |
| [**Reduce**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reduce) | [ReduceAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reduce/counter/example.py) | Reducer |
| [**Reduce Stream**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/reducestream) | [ReduceStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/reducestream/counter/example.py) | ReduceStreamer |
| [**Map Stream**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/mapstream) | [MapStreamAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/mapstream/flatmap_stream/example.py) | MapStreamer |
| [**Batch Map**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/batchmap) | [BatchMapAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/batchmap/flatmap/example.py) | BatchMapper |
| [**Accumulator**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/accumulator) | [AccumulatorAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/accumulator/streamsorter/example.py) | Accumulator |
| [**Source Transform**](https://github.com/numaproj/numaflow-python/tree/main/packages/pynumaflow/examples/sourcetransform) | [SourceTransformServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/event_time_filter/example.py), [SourceTransformAsyncServer](https://github.com/numaproj/numaflow-python/blob/main/packages/pynumaflow/examples/sourcetransform/async_event_time_filter/example.py), SourceTransformMultiProcServer | SourceTransformer |

### Handler Functions and Classes

All server types take an instance of a handler class or a handler function as an argument.
The handler function or class implements the functionality of the UDF, UDSource or UDSink.
For ease of use the user can pass either of the two to the server and the server will handle the rest.

The handler for each server has a specific signature defined by the server type, and the implementation of the handlers should follow the same signature.

For class-based handlers, inherit from the base handler class for the respective functionality and implement the handler method.
More details about the handler signature for each server type is given in the documentation of the respective server type.
