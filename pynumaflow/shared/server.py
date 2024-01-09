import contextlib
import multiprocessing
import os
import socket
from abc import abstractmethod
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import grpc
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
    SERVER_INFO_FILE_PATH,
    METADATA_ENVS,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from pynumaflow.proto.sinker import sink_pb2_grpc
from pynumaflow.proto.sourcetransformer import transform_pb2_grpc


class NumaflowServer:
    """
    Provides an interface to write a Numaflow Server
    which will be exposed over gRPC.

    Members:
        sock_path: Path to the UNIX Domain Socket
        max_message_size: The max message size in bytes the server can receive and send
        max_threads: The max number of threads to be spawned;
                     defaults to number of processors x4
    """

    @abstractmethod
    def start(self):
        """
        Start the server
        """
        raise NotImplementedError


# def prepare_server(
#         sock_path: str,
#         server_type: ServerType,
#         max_threads=MAX_THREADS,
#         server_options=None,
#         process_count=1,
# ):
#     """
#     Create a new grpc Server instance.
#     A new servicer instance is created and attached to the server.
#     The server instance is returned.
#
#     """
#     if server_type == ServerType.Sync:
#         server = _get_sync_server(
#             bind_address=sock_path, threads_per_proc=max_threads, server_options=server_options
#         )
#         return server
#     # elif server_type == ServerType.Multiproc:
#     #     servers, server_ports = get_multiproc_servers(
#     #         max_threads=max_threads,
#     #         server_options=server_options,
#     #         process_count=process_count,
#     #     )
#     #     return servers, server_ports


def write_info_file(protocol: Protocol) -> None:
    """
    Write the server info file to the given path.
    """
    serv_info = ServerInfo(
        protocol=protocol,
        language=Language.PYTHON,
        version=get_sdk_version(),
    )
    info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)


def sync_server_start(
    servicer, bind_address: str, max_threads: int, server_options=None, udf_type: str = UDFType.Map
):
    """
    Starts the Synchronous server instance on the given UNIX socket with given max threads.
    Wait for the server to terminate.
    """
    # Add the server information to the server info file,
    # here we just write the protocol and language information
    server_info = ServerInfo(
        protocol=Protocol.UDS,
        language=Language.PYTHON,
        version=get_sdk_version(),
    )
    # Run a sync server instances
    _run_server(
        servicer=servicer,
        bind_address=bind_address,
        threads_per_proc=max_threads,
        server_options=server_options,
        udf_type=udf_type,
        server_info=server_info,
    )


def _run_server(
    servicer, bind_address: str, threads_per_proc, server_options, udf_type: str, server_info=None
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
    if udf_type == UDFType.Map:
        map_pb2_grpc.add_MapServicer_to_server(servicer, server)
    elif udf_type == UDFType.Sink:
        sink_pb2_grpc.add_SinkServicer_to_server(servicer, server)
    elif udf_type == UDFType.SourceTransformer:
        transform_pb2_grpc.add_SourceTransformServicer_to_server(servicer, server)

    server.add_insecure_port(bind_address)
    server.start()

    # Add the server information to the server info file if provided
    if server_info:
        info_server_write(server_info=server_info, info_file=SERVER_INFO_FILE_PATH)

    _LOGGER.info("GRPC Server listening on: %s %d", bind_address, os.getpid())
    server.wait_for_termination()


def start_multiproc_server(
    max_threads: int, servicer, process_count: int, server_options=None, udf_type: str = UDFType.Map
):
    """
    Start N grpc servers in different processes where N = The number of CPUs or the
    value of the env var NUM_CPU_MULTIPROC defined by the user. The max value
    is set to 2 * CPU count.
    Each server will be bound to a different port, and we will create equal number of
    workers to handle each server.
    On the client side there will be same number of connections as the number of servers.
    """

    _LOGGER.info(
        "Starting new Multiproc server with num_procs: %s, num_threads per proc: %s",
        process_count,
        max_threads,
    )
    workers = []
    server_ports = []
    for _ in range(process_count):
        # Find a port to bind to for each server, thus sending the port number = 0
        # to the _reserve_port function so that kernel can find and return a free port
        with _reserve_port(port_num=0) as port:
            bind_address = f"{MULTIPROC_MAP_SOCK_ADDR}:{port}"
            _LOGGER.info("Starting server on port: %s", port)
            # NOTE: It is imperative that the worker subprocesses be forked before
            # any gRPC servers start up. See
            # https://github.com/grpc/grpc/issues/16001 for more details.
            worker = multiprocessing.Process(
                target=_run_server,
                args=(servicer, bind_address, max_threads, server_options, udf_type),
            )
            worker.start()
            workers.append(worker)
            server_ports.append(port)

    # Convert the available ports to a comma separated string
    ports = ",".join(map(str, server_ports))

    serv_info = ServerInfo(
        protocol=Protocol.TCP,
        language=Language.PYTHON,
        version=get_sdk_version(),
        metadata=get_metadata_env(envs=METADATA_ENVS),
    )
    # Add the PORTS metadata using the available ports
    serv_info.metadata["SERV_PORTS"] = ports
    info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)

    for worker in workers:
        worker.join()


async def start_async_server(
    server_async: grpc.aio.Server, sock_path: str, max_threads: int, cleanup_coroutines: list
):
    """
    Starts the Async server instance on the given UNIX socket with given max threads.
    Add the server graceful shutdown coroutine to the cleanup_coroutines list.
    Wait for the server to terminate.
    """
    await server_async.start()

    # Add the server information to the server info file
    # Here we just write the protocol and language information
    write_info_file(Protocol.UDS)

    # Log the server start
    _LOGGER.info(
        "New Async GRPC Server listening on: %s with max threads: %s",
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


# async def __serve_async(self, server) -> None:
#     async def server_graceful_shutdown():
#         """
#         Shuts down the server with 5 seconds of grace period. During the
#         grace period, the server won't accept new connections and allow
#         existing RPCs to continue within the grace period.
#         """
#         _LOGGER.info("Starting graceful shutdown...")
#         await server.stop(5)
#
#     self.cleanup_coroutines.append(server_graceful_shutdown())
#     await server.wait_for_termination()
#
#
# async def start(self) -> None:
#     """Starts the Async gRPC mapper on the given UNIX socket."""
#     server = grpc.aio.server(options=self._server_options)
#     await self.__serve_async(server)


def _get_sync_server(bind_address: str, threads_per_proc: int, server_options: list):
    """Get a new sync grpc server instance."""
    try:
        server = grpc.server(
            ThreadPoolExecutor(
                max_workers=threads_per_proc,
            ),
            options=server_options,
        )
        server.add_insecure_port(bind_address)
        print("bind_address", bind_address)
        _LOGGER.info("Starting new server with bind_address: %s", bind_address)
    except Exception as err:
        _LOGGER.critical("Failed to start server: %s", err, exc_info=True)
        raise err
    return server


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
