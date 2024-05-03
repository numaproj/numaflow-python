import contextlib
import multiprocessing
import os
import socket
from abc import ABCMeta, abstractmethod
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import grpc
import psutil

from pynumaflow._constants import (
    _LOGGER,
    MULTIPROC_MAP_SOCK_ADDR,
    UDFType,
)
from pynumaflow.exceptions import SocketError
from pynumaflow.info.server import get_sdk_version, write as info_server_write, get_metadata_env
from pynumaflow.info.types import (
    ServerInfo,
    Protocol,
    Language,
    METADATA_ENVS,
    MINIMUM_NUMAFLOW_VERSION,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.proto.sideinput import sideinput_pb2_grpc
from pynumaflow.proto.sinker import sink_pb2_grpc
from pynumaflow.proto.sourcer import source_pb2_grpc
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc
from pynumaflow.types import NumaflowServicerContext


class NumaflowServer(metaclass=ABCMeta):
    """
    Provides an interface to write a Numaflow Server
    which will be exposed over gRPC.
    """

    @abstractmethod
    def start(self):
        """
        Start the gRPC server
        """
        pass


def write_info_file(protocol: Protocol, info_file) -> None:
    """
    Write the server info file to the given path.
    """
    serv_info = ServerInfo(
        protocol=protocol,
        language=Language.PYTHON,
        minimum_numaflow_version=MINIMUM_NUMAFLOW_VERSION,
        version=get_sdk_version(),
    )
    info_server_write(server_info=serv_info, info_file=info_file)


def sync_server_start(
    servicer,
    bind_address: str,
    max_threads: int,
    server_info_file: str,
    server_options=None,
    udf_type: str = UDFType.Map,
):
    """
    Utility function to start a sync grpc server instance.
    """
    # Add the server information to the server info file
    server_info = ServerInfo(
        protocol=Protocol.UDS,
        language=Language.PYTHON,
        minimum_numaflow_version=MINIMUM_NUMAFLOW_VERSION,
        version=get_sdk_version(),
    )

    # Run a sync server instance
    _run_server(
        servicer=servicer,
        bind_address=bind_address,
        threads_per_proc=max_threads,
        server_options=server_options,
        udf_type=udf_type,
        server_info_file=server_info_file,
        server_info=server_info,
    )


def _run_server(
    servicer,
    bind_address: str,
    threads_per_proc,
    server_options,
    udf_type: str,
    server_info_file: Optional[str] = None,
    server_info: Optional[ServerInfo] = None,
) -> None:
    """
    Starts the Synchronous server instance on the given UNIX socket
    with given max threads. Wait for the server to terminate.
    """
    server = grpc.server(
        ThreadPoolExecutor(
            max_workers=threads_per_proc,
        ),
        options=server_options,
    )

    # add the correct servicer to the server based on the UDF type
    if udf_type == UDFType.Map:
        map_pb2_grpc.add_MapServicer_to_server(servicer, server)
    elif udf_type == UDFType.Sink:
        sink_pb2_grpc.add_SinkServicer_to_server(servicer, server)
    elif udf_type == UDFType.SourceTransformer:
        transform_pb2_grpc.add_SourceTransformServicer_to_server(servicer, server)
    elif udf_type == UDFType.Source:
        source_pb2_grpc.add_SourceServicer_to_server(servicer, server)
    elif udf_type == UDFType.SideInput:
        sideinput_pb2_grpc.add_SideInputServicer_to_server(servicer, server)

    # bind the server to the UDS/TCP socket
    server.add_insecure_port(bind_address)
    # start the gRPC server
    server.start()
    # Add the server information to the server info file if provided
    if server_info and server_info_file:
        info_server_write(server_info=server_info, info_file=server_info_file)

    _LOGGER.info("GRPC Server listening on: %s %d", bind_address, os.getpid())
    server.wait_for_termination()


def start_multiproc_server(
    max_threads: int,
    servicer,
    process_count: int,
    server_info_file: str,
    server_options=None,
    udf_type: str = UDFType.Map,
):
    """
    Start N grpc servers in different processes where N = The number of CPUs or the
    value of the env var NUM_CPU_MULTIPROC defined by the user. The max value
    is set to 2 * CPU count.
    Each server will be bound to a different UDS socket, and we will create equal number of
    workers to handle each server.
    On the client side there will be same number of connections as the number of servers.
    """

    _LOGGER.info(
        "Starting new Multiproc server with num_procs: %s, num_threads per proc: %s",
        process_count,
        max_threads,
    )
    workers = []
    for idx in range(process_count):
        # bind address is the UDS sock for each server to  bind to, it is in the format
        # unix:///var/run/numaflow/multiproc#serv_num.sock
        # -> unix:///var/run/numaflow/multiproc0.sock
        bind_address = f"unix://{MULTIPROC_MAP_SOCK_ADDR}{idx}.sock"
        _LOGGER.info("Starting server on: %s", bind_address)
        # NOTE: It is imperative that the worker subprocesses be forked before
        # any gRPC servers start up. See
        # https://github.com/grpc/grpc/issues/16001 for more details.
        worker = multiprocessing.Process(
            target=_run_server,
            args=(servicer, bind_address, max_threads, server_options, udf_type),
        )
        worker.start()
        workers.append(worker)

    serv_info = ServerInfo(
        protocol=Protocol.UDS,
        language=Language.PYTHON,
        minimum_numaflow_version=MINIMUM_NUMAFLOW_VERSION,
        version=get_sdk_version(),
        metadata=get_metadata_env(envs=METADATA_ENVS),
    )
    # Add the MULTIPROC metadata using the number of servers to use
    serv_info.metadata["MULTIPROC"] = str(process_count)
    info_server_write(server_info=serv_info, info_file=server_info_file)

    for worker in workers:
        worker.join()


async def start_async_server(
    server_async: grpc.aio.Server,
    sock_path: str,
    max_threads: int,
    cleanup_coroutines: list,
    server_info_file: str,
):
    """
    Starts the Async server instance on the given UNIX socket with given max threads.
    Add the server graceful shutdown coroutine to the cleanup_coroutines list.
    Wait for the server to terminate.
    """
    await server_async.start()

    # Add the server information to the server info file
    serv_info = ServerInfo(
        protocol=Protocol.UDS,
        language=Language.PYTHON,
        minimum_numaflow_version=MINIMUM_NUMAFLOW_VERSION,
        version=get_sdk_version(),
    )
    info_server_write(server_info=serv_info, info_file=server_info_file)

    # Log the server start
    _LOGGER.info(
        "Async GRPC Server listening on: %s with max threads: %s",
        sock_path,
        max_threads,
    )

    async def server_graceful_shutdown():
        """
        Shuts down the server with 5 seconds of grace period. During the
        grace period, the server won't accept new connections and allow
        existing RPCs to continue within the grace period.
        """
        _LOGGER.info("Starting graceful shutdown...")
        await server_async.stop(5)

    cleanup_coroutines.append(server_graceful_shutdown())
    await server_async.wait_for_termination()


@contextlib.contextmanager
def _reserve_port(port_num: int) -> Iterator[int]:
    """Find and reserve a port for all subprocesses to use."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 0:
        raise SocketError("Failed to set SO_REUSEADDR.")
    try:
        sock.bind(("", port_num))
        yield sock.getsockname()[1]
    finally:
        sock.close()


def checkInstance(instance, callable_type) -> bool:
    """
    Check if the given instance is of the given callable_type.
    """
    try:
        if not isinstance(instance, callable_type):
            return False
        else:
            return True
    except Exception as e:
        _LOGGER.error(e)
        return False


def exit_on_error(
    context: NumaflowServicerContext, err: str, parent: bool = False, update_context=True
):
    """
    Exit the current/parent process on an error.

    Args:
        context (NumaflowServicerContext): The gRPC context.
        err (str): The error message.
        parent (bool, optional): Whether this is the parent process.
            Defaults to False.
        update_context(bool, optional) : Is there a need to update
            the context with the error codes
    """
    if update_context:
        context.set_code(grpc.StatusCode.UNKNOWN)
        context.set_details(err)

    p = psutil.Process(os.getpid())
    # If the parent flag is true, we exit from the parent process
    # Use this for Multiproc right now to exit from the parent fork
    if parent:
        p = psutil.Process(os.getppid())
    _LOGGER.info("Killing process: Got exception %s", err)
    p.kill()
