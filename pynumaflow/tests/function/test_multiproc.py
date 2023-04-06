import os
import unittest
from unittest import mock

import grpc

from pynumaflow.function import UserDefinedFunctionServicer
from pynumaflow.function.multiproc_server import MultiProcServer
from pynumaflow.function.proto import udfunction_pb2_grpc
from pynumaflow.tests.function.test_server import (
    mapt_handler,
    map_handler,
)

from pynumaflow.tests.function.test_async_server import async_reduce_handler


def mockenv(**envvars):
    return mock.patch.dict(os.environ, envvars)


class TestMultiProcMethods(unittest.TestCase):
    @mockenv(NUM_CPU_MULTIPROC="3")
    def test_multiproc_init(self) -> None:
        serv_options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]
        udfs = UserDefinedFunctionServicer(
            reduce_handler=async_reduce_handler,
            map_handler=map_handler,
            mapt_handler=mapt_handler,
        )
        server = MultiProcServer(udf_service=udfs, server_options=serv_options)
        self.assertEqual(server.sock_path, 55551)
        self.assertEqual(server._PROCESS_COUNT, 3)
        self.assertEqual(server._THREAD_CONCURRENCY, 3)
        self.assertEqual(server.server_options, serv_options)

    # To test the reuse property for the grpc servers which allow multiple
    # bindings to the same server
    def test_reuse_port(self):
        serv_options = [("grpc.so_reuseport", 1), ("grpc.so_reuseaddr", 1)]
        udfs = UserDefinedFunctionServicer(
            reduce_handler=async_reduce_handler,
            map_handler=map_handler,
            mapt_handler=mapt_handler,
        )
        server = MultiProcServer(udf_service=udfs, server_options=serv_options)

        with server._reserve_port() as port:
            print(port)
            bind_address = f"localhost:{port}"
            server1 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 1),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server1)
            server1.add_insecure_port(bind_address)

            # so_reuseport=0 -> the bind should raise an error
            server2 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 0),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server2)
            self.assertRaises(RuntimeError, server2.add_insecure_port, bind_address)

            # so_reuseport=1 -> should allow server to bind to port again
            server3 = grpc.server(thread_pool=None, options=(("grpc.so_reuseport", 1),))
            udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server3)
            server3.add_insecure_port(bind_address)


if __name__ == "__main__":
    unittest.main()
