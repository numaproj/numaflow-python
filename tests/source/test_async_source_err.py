import asyncio
import logging
import threading
import unittest

import grpc

from grpc.aio._server import Server
from pynumaflow._constants import ServerType

from pynumaflow import setup_logging
from pynumaflow.sourcer import SourceServer
from pynumaflow.proto.sourcer import source_pb2_grpc, source_pb2
from google.protobuf import empty_pb2 as _empty_pb2
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    AsyncSourceError,
)

LOGGER = setup_logging(__name__)

_s: Server = None
server_port = "localhost:50062"
_channel = grpc.insecure_channel(server_port)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    class_instance = AsyncSourceError()
    server_instance = SourceServer(sourcer_instance=class_instance, server_type=ServerType.Async)
    udfs = server_instance.get_servicer(
        sourcer_instance=server_instance.sourcer_instance, server_type=ServerType.Async
    )
    # udfs = AsyncSourcer(
    #     read_handler=err_async_source_read_handler,
    #     ack_handler=err_async_source_ack_handler,
    #     pending_handler=err_async_source_pending_handler,
    #     partitions_handler=err_async_source_partition_handler,
    # )
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "[::]:50062"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncServerErrorScenario(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        asyncio.run_coroutine_threadsafe(start_server(), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("localhost:50062") as channel:
                    f = grpc.channel_ready_future(channel)
                    f.result(timeout=10)
                    if f.done():
                        break
            except grpc.FutureTimeoutError as e:
                LOGGER.error("error trying to connect to grpc server")
                LOGGER.error(e)

    @classmethod
    def tearDownClass(cls) -> None:
        try:
            _loop.stop()
            LOGGER.info("stopped the event loop")
        except Exception as e:
            LOGGER.error(e)

    def test_read_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = read_req_source_fn()
            generator_response = None
            try:
                generator_response = stub.ReadFn(request=source_pb2.ReadRequest(request=request))
                for _ in generator_response:
                    pass
            except Exception as e:
                self.assertTrue("Got a runtime error from read handler." in e.__str__())
                return
        self.fail("Expected an exception.")

    def test_ack_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            try:
                stub.AckFn(request=source_pb2.AckRequest(request=request))
            except Exception as e:
                self.assertTrue("Got a runtime error from ack handler." in e.__str__())
                return
        self.fail("Expected an exception.")

    def test_pending_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            try:
                stub.PendingFn(request=request)
            except Exception as e:
                self.assertTrue("Got a runtime error from pending handler." in e.__str__())
                return
        self.fail("Expected an exception.")

    def test_partition_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            try:
                stub.PartitionsFn(request=request)
            except Exception as e:
                self.assertTrue("Got a runtime error from partition handler." in e.__str__())
                return
        self.fail("Expected an exception.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
