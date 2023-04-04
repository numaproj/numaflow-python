from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import contextlib
import os
from concurrent import futures
import datetime
import logging
import multiprocessing
import socket
import sys
import time
import grpc

from pynumaflow._constants import MULTI_FUNCTION_SOCK_PATH
from pynumaflow.function.proto import udfunction_pb2
from pynumaflow.function.proto import udfunction_pb2_grpc

from pynumaflow import setup_logging

_LOGGER = setup_logging(__name__)
if os.getenv("PYTHONDEBUG"):
    _LOGGER.setLevel(logging.DEBUG)

_ONE_DAY = datetime.timedelta(days=1)


class MultiProcServer:
    def __init__(self, udf_service, sock_path, server_options):
        # self.sock_path = sock_path
        self.udf_service = udf_service
        # self.sock_path = f"unix://{MULTI_FUNCTION_SOCK_PATH}"
        self.sock_path = 55551
        self._PROCESS_COUNT = multiprocessing.cpu_count()
        self._THREAD_CONCURRENCY = self._PROCESS_COUNT
        # self._PROCESS_COUNT = 1
        # self._THREAD_CONCURRENCY = 1
        self.server_options = server_options

    def _wait_forever(self, server):
        try:
            while True:
                time.sleep(_ONE_DAY.total_seconds())
        except KeyboardInterrupt:
            server.stop(None)

    def _run_server(self, bind_address):
        """Start a server in a subprocess."""
        _LOGGER.info('Starting new server.')
        options = [('grpc.so_reuseport', 1), ('grpc.so_reuseaddr', 1)]
        for x in options:
            self.server_options.append(x)
        # print("OPTS " +
        _LOGGER.info(f"{self.server_options}")
        server = grpc.server(futures.ThreadPoolExecutor(
            max_workers=self._THREAD_CONCURRENCY, ),
            options=self.server_options)
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(self.udf_service, server)
        # prime_pb2_grpc.add_PrimeCheckerServicer_to_server(PrimeChecker(), server)
        server.add_insecure_port(bind_address)
        server.start()
        _LOGGER.info(
            "GRPC Server MULTIPROC listening on: %s %d", bind_address, os.getpid()
        )
        server.wait_for_termination()
        # self._wait_forever(server)

    @contextlib.contextmanager
    def _reserve_port(self):
        """Find and reserve a port for all subprocesses to use."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR) == 0:
            raise RuntimeError("Failed to set SO_REUSEADDR.")
        if sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) == 0:
            raise RuntimeError("Failed to set SO_REUSEPORT.")
        _LOGGER.info(f"FUNC PATH {MULTI_FUNCTION_SOCK_PATH}")
        _LOGGER.info(f"SOCK PATH {self.sock_path}")
        sock.bind(('', self.sock_path))
        # try:
        #     os.remove(MULTI_FUNCTION_SOCK_PATH)
        # except OSError:
        #     pass
        # sock.bind(MULTI_FUNCTION_SOCK_PATH)
        try:
            yield sock.getsockname()[1]
        finally:
            sock.close()

    def start(self) -> None:
        with self._reserve_port() as port:
            # sock_path = port.getsockname()
            bind_address = f"0.0.0.0:{port}"
            # bind_address = self.sock_path
            # bind_address = 'localhost:{}'.format(port)
            # _LOGGER.info("Binding to '%s'", bind_address)
            # sys.stdout.flush()
            workers = []
            for _ in range(self._PROCESS_COUNT):
                # NOTE: It is imperative that the worker subprocesses be forked before
                # any gRPC servers start up. See
                # https://github.com/grpc/grpc/issues/16001 for more details.
                worker = multiprocessing.Process(target=self._run_server,
                                                 args=(bind_address,))
                worker.start()
                workers.append(worker)
            for worker in workers:
                worker.join()