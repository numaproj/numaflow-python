import os
import unittest
from unittest import mock

import grpc

from pynumaflow.function.multiproc_server import MultiProcServer
from pynumaflow.function.proto import udfunction_pb2_grpc
from pynumaflow.tests.function.server_utils import (
    mapt_handler,
    map_handler,
)
from pynumaflow.tests.function.test_async_server import async_reduce_handler


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestMultiProcMethods(unittest.TestCase):
    @mockenv(NUM_CPU_MULTIPROC="3")
    def test_multiproc_init(self) -> None:
        server = MultiProcServer(
            reduce_handler=async_reduce_handler, map_handler=map_handler, mapt_handler=mapt_handler
        )
        self.assertEqual(server._sock_path, 55551)
        self.assertEqual(server._PROCESS_COUNT, 3)
        self.assertEqual(server._THREAD_CONCURRENCY, 3)

    # To test the reuse property for the grpc servers which allow multiple
    # bindings to the same server
    def test_reuse_port(self):
        serv_options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]

        server = MultiProcServer(
            reduce_handler=async_reduce_handler, map_handler=map_handler, mapt_handler=mapt_handler
        )

        with server._reserve_port() as port:
            print(port)
            bind_address = f"localhost:{port}"
            server1 = grpc.server(thread_pool=None, options=serv_options)
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server1)
            server1.add_insecure_port(bind_address)

            # so_reuseport=0 -> the bind should raise an error
            server2 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 0),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server2)
            self.assertRaises(RuntimeError, server2.add_insecure_port, bind_address)

            # so_reuseport=1 -> should allow server to bind to port again
            server3 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 1),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(server, server3)
            server3.add_insecure_port(bind_address)


if __name__ == "__main__":
    unittest.main()
