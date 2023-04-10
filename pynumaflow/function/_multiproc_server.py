from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from concurrent import futures

import contextlib
import grpc
import logging
import multiprocessing
import os
import socket

from pynumaflow import setup_logging
from pynumaflow._constants import MULTIPROC_FUNCTION_SOCK_PORT, MULTIPROC_FUNCTION_SOCK_ADDR
from pynumaflow.exceptions import SocketError
from pynumaflow.function.proto import udfunction_pb2_grpc

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)


class MultiProcServer:
    """
    MultiProcServer Class a multiprocessing implementation of the grpc server.
    They use a given TCP socket with SOCK_REUSE to allow all servers,
    to listen on the same port. We start servers equal to the CPU count of the
    system.

    Arguments
    ----------
    udf_service :  UserDefinedFunctionServicer
      The interface for the User Defined Function (UDFunction)
        which will be exposed over gRPC.


    server_options :  list[tuple[str, int]]
        the custom options for configuring the grpc server

    """

    def __init__(self, udf_service, server_options):
        self.udf_service = udf_service
        self.sock_path = MULTIPROC_FUNCTION_SOCK_PORT
        self._PROCESS_COUNT = int(os.getenv("NUM_CPU_MULTIPROC", multiprocessing.cpu_count()))
        self._THREAD_CONCURRENCY = self._PROCESS_COUNT
        self.server_options = server_options

    def _run_server(self, bind_address):
        """Start a server in a subprocess."""
        _LOGGER.info("Starting new server.")
        options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]
        for x in options:
            self.server_options.append(x)
        server = grpc.server(
            futures.ThreadPoolExecutor(
                max_workers=self._THREAD_CONCURRENCY,
            ),
            options=self.server_options,
        )
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(self.udf_service, server)
        server.add_insecure_port(bind_address)
        server.start()
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
        sock.bind(("", self.sock_path))
        try:
            yield sock.getsockname()[1]
        finally:
            sock.close()

    def start(self) -> None:
        """Start N grpc servers in different processes where N = CPU Count"""
        with self._reserve_port() as port:
            bind_address = f"{MULTIPROC_FUNCTION_SOCK_ADDR}:{port}"
            workers = []
            for _ in range(self._PROCESS_COUNT):
                # NOTE: It is imperative that the worker subprocesses be forked before
                # any gRPC servers start up. See
                # https://github.com/grpc/grpc/issues/16001 for more details.
                worker = multiprocessing.Process(target=self._run_server, args=(bind_address,))
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()
