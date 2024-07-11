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

This project uses [Poetry](https://python-poetry.org/) for dependency management and packaging.
To build the package locally, run the following command from the root of the project.

```bash
make setup
````

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
- [Implement User Defined Sources](https://github.com/numaproj/numaflow-python/tree/main/examples/source)
- [Implement User Defined Source Transformers](https://github.com/numaproj/numaflow-python/tree/main/examples/sourcetransform)
- Implement User Defined Functions
    - [Map](https://github.com/numaproj/numaflow-python/tree/main/examples/map)
    - [Reduce](https://github.com/numaproj/numaflow-python/tree/main/examples/reduce)
    - [Map Stream](https://github.com/numaproj/numaflow-python/tree/main/examples/mapstream)
    - [Batch Map](https://github.com/numaproj/numaflow-python/tree/main/examples/batchmap)
- [Implement User Defined Sinks](https://github.com/numaproj/numaflow-python/tree/main/examples/sink)
- [Implement User Defined SideInputs](https://github.com/numaproj/numaflow-python/tree/main/examples/sideinput)

## Server Types

There are different types of gRPC server mechanisms which can be used to serve the UDFs, UDSinks and UDSource.
These have different functionalities and are used for different use cases.

Currently we support the following server types:
- Sync Server
- Asyncronous Server
- MultiProcessing Server

Not all of the above are supported for all UDFs, UDSource and UDSinks.

For each of the UDFs, UDSource and UDSinks, there are seperate classes for each of the server types.
This helps in keeping the interface simple and easy to use, and the user can start the specific server type based
on the use case.


#### SyncServer

Syncronous Server is the simplest server type. It is a multithreaded threaded server which can be used for simple UDFs and UDSinks.
Here the server will invoke the handler function for each message. The messaging is synchronous and the server will wait
for the handler to return before processing the next message.

```
grpc_server = MapServer(handler)
```

#### AsyncServer

Asyncronous Server is a multi threaded server which can be used for UDFs which are asyncronous. Here we utilize the asyncronous capabilities of Python to process multiple messages in parallel. The server will invoke the handler function for each message. The messaging is asyncronous and the server will not wait for the handler to return before processing the next message. Thus this server type is useful for UDFs which are asyncronous.
The handler function for such a server should be an async function.

```
grpc_server = MapAsyncServer(handler)
```

#### MultiProcessServer

MultiProcess Server is a multi process server which can be used for UDFs which are CPU intensive. Here we utilize the multi process capabilities of Python to process multiple messages in parallel by forking multiple servers in different processes. 
The server will invoke the handler function for each message. Individually at the server level the messaging is synchronous and the server will wait for the handler to return before processing the next message. But since we have multiple servers running in parallel, the overall messaging also executes in parallel.

This could be an alternative to creating multiple replicas of the same UDF container as here we are using the multi processing capabilities of the system to process multiple messages in parallel but within the same container.

Thus this server type is useful for UDFs which are CPU intensive.
```
grpc_server = MapMultiProcServer(mapper_instance=handler, server_count=2)
```

#### Currently Supported Server Types for each functionality

These are the class names for the server types supported by each of the functionalities.

- UDFs
    - Map
        - MapServer
        - MapAsyncServer
        - MapMultiProcServer
    - Reduce
        - ReduceAsyncServer
    - MapStream
        - MapStreamAsyncServer
    - BatchMap
      - BatchMapAsyncServer
    - Source Transform
        - SourceTransformServer
        - SourceTransformMultiProcServer
- UDSource
    - SourceServer
    - SourceAsyncServer
- UDSink
    - SinkServer
    - SinkAsyncServer
- SideInput
    - SideInputServer




### Handler Function and Classes

All the server types take a instance of a handler class or a handler function as an argument.
The handler function or class is the function or class which implements the functionality of the UDF, UDSource or UDSink.
For ease of use the user can pass either of the two to the server and the server will handle the rest.

The handler for each of the servers has a specific signature which is defined by the server type and the implentation of the handlers
should follow the same signature.

For using the class based handlers the user can inherit from the base handler class for each of the functionalities and implement the handler function.
The base handler class for each of the functionalities has the same signature as the handler function for the respective server type.
The list of base handler classes for each of the functionalities is given below -
- UDFs
    - Map
        - Mapper
    - Reduce
        - Reducer
    - MapStream
        - MapStreamer
    - Source Transform
        - SourceTransformer
    - Batch Map
      - BatchMapper
- UDSource
    - Sourcer
- UDSink
    - Sinker
- SideInput
    - SideInput

More details about the signature of the handler function for each of the server types is given in the 
documentation of the respective server type.
