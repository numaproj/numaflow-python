import contextlib
import logging
import multiprocessing
import os
import socket
from concurrent import futures

import grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow import setup_logging
from pynumaflow._constants import (
    MAX_MESSAGE_SIZE,
)
from pynumaflow._constants import MULTIPROC_MAP_SOCK_PORT, MULTIPROC_MAP_SOCK_ADDR
from pynumaflow.exceptions import SocketError
from pynumaflow.mapper import Datum
from pynumaflow.mapper._dtypes import MapCallable
from pynumaflow.mapper.proto import map_pb2
from pynumaflow.mapper.proto import map_pb2_grpc
from pynumaflow.types import NumaflowServicerContext
from pynumaflow.info.server import (
    get_sdk_version,
    write as info_server_write,
    get_metadata_env,
)
from pynumaflow.info.types import (
    ServerInfo,
    Protocol,
    Language,
    SERVER_INFO_FILE_PATH,
    METADATA_ENVS,
)

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class MultiProcMapper(map_pb2_grpc.MapServicer):
    """
    Provides an interface to write a Multi Proc Mapper
    which will be exposed over gRPC.

    Args:
        handler: Function callable following the type signature of MapCallable
        sock_path: Path to the TCP port to bind to
        max_message_size: The max message size in bytes the server can receive and send

    Example invocation:
    >>> from typing import Iterator
    >>> from pynumaflow.mapper import Messages, Message \
    ...     Datum, MultiProcMapper
    ...
    >>> def map_handler(keys: list[str], datum: Datum) -> Messages:
    ...   val = datum.value
    ...   _ = datum.event_time
    ...   _ = datum.watermark
    ...   messages = Messages(Message(val, keys=keys))
    ...   return messages
    ...
    >>> grpc_server = MultiProcMapper(handler=map_handler)
    >>> grpc_server.start()
    """

    __slots__ = (
        "__map_handler",
        "_max_message_size",
        "_server_options",
        "_sock_path",
        "_process_count",
        "_threads_per_proc",
    )

    def __init__(
        self,
        handler: MapCallable,
        sock_path=MULTIPROC_MAP_SOCK_PORT,
        max_message_size=MAX_MESSAGE_SIZE,
    ):
        self.__map_handler: MapCallable = handler
        self._max_message_size = max_message_size

        self._server_options = [
            ("grpc.max_send_message_length", self._max_message_size),
            ("grpc.max_receive_message_length", self._max_message_size),
            ("grpc.so_reuseport", 1),
            ("grpc.so_reuseaddr", 1),
        ]
        self._sock_path = sock_path
        self._process_count = int(os.getenv("NUM_CPU_MULTIPROC") or os.cpu_count())
        self._threads_per_proc = int(os.getenv("MAX_THREADS", "4"))

    def MapFn(
        self, request: map_pb2.MapRequest, context: NumaflowServicerContext
    ) -> map_pb2.MapResponse:
        """
        Applies a function to each datum element.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        # proto repeated field(keys) is of type google._upb._message.RepeatedScalarContainer
        # we need to explicitly convert it to list
        try:
            msgs = self.__map_handler(
                list(request.keys),
                Datum(
                    keys=list(request.keys),
                    value=request.value,
                    event_time=request.event_time.ToDatetime(),
                    watermark=request.watermark.ToDatetime(),
                ),
            )
        except Exception as err:
            _LOGGER.critical("UDFError, re-raising the error", exc_info=True)
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(str(err))
            return map_pb2.MapResponse(results=[])

        datums = []

        for msg in msgs:
            datums.append(map_pb2.MapResponse.Result(keys=msg.keys, value=msg.value, tags=msg.tags))

        return map_pb2.MapResponse(results=datums)

    def IsReady(
        self, request: _empty_pb2.Empty, context: NumaflowServicerContext
    ) -> map_pb2.ReadyResponse:
        """
        IsReady is the heartbeat endpoint for gRPC.
        The pascal case function name comes from the proto map_pb2_grpc.py file.
        """
        return map_pb2.ReadyResponse(ready=True)

    def _run_server(self, bind_address: str) -> None:
        """Start a server in a subprocess."""
        _LOGGER.info(
            "Starting new server with num_procs: %s, num_threads/proc: %s",
            self._process_count,
            self._threads_per_proc,
        )
        server = grpc.server(
            futures.ThreadPoolExecutor(
                max_workers=self._threads_per_proc,
            ),
            options=self._server_options,
        )
        map_pb2_grpc.add_MapServicer_to_server(self, server)
        server.add_insecure_port(bind_address)
        server.start()
        serv_info = ServerInfo(
            protocol=Protocol.TCP,
            language=Language.PYTHON,
            version=get_sdk_version(),
            metadata=get_metadata_env(envs=METADATA_ENVS),
        )
        # Overwrite the CPU_LIMIT metadata using user input
        serv_info.metadata["CPU_LIMIT"] = str(self._process_count)
        info_server_write(server_info=serv_info, info_file=SERVER_INFO_FILE_PATH)

        _LOGGER.info("GRPC Multi-Processor Server listening on: %s %d", bind_address, os.getpid())
        server.wait_for_termination()

    @contextlib.contextmanager
    def _reserve_port(self) -> int:
        """Find and reserve a port for all subprocesses to use."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 0:
            raise SocketError("Failed to set SO_REUSEADDR.")
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0:
            raise SocketError("Failed to set SO_REUSEPORT.")
        sock.bind(("", self._sock_path))
        try:
            yield sock.getsockname()[1]
        finally:
            sock.close()

    def start(self) -> None:
        """Start N grpc servers in different processes where N = CPU Count"""
        with self._reserve_port() as port:
            bind_address = f"{MULTIPROC_MAP_SOCK_ADDR}:{port}"
            workers = []
            for _ in range(self._process_count):
                # NOTE: It is imperative that the worker subprocesses be forked before
                # any gRPC servers start up. See
                # https://github.com/grpc/grpc/issues/16001 for more details.
                worker = multiprocessing.Process(target=self._run_server, args=(bind_address,))
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()
