import asyncio
import logging
import threading
import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server
from pynumaflow._constants import ServerType

from pynumaflow import setup_logging
from pynumaflow.sourcer import (
    SourceServer,
)
from pynumaflow.proto.sourcer import source_pb2_grpc, source_pb2
from tests.source.utils import (
    mock_offset,
    read_req_source_fn,
    ack_req_source_fn,
    mock_partitions,
    AsyncSource,
)

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False

server_port = "localhost:50058"

_s: Server = None
_channel = grpc.insecure_channel(server_port)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncSourcer():
    class_instance = AsyncSource()
    server = SourceServer(sourcer_instance=class_instance, server_type=ServerType.Async)

    udfs = server.get_servicer(
        sourcer_instance=server.sourcer_instance, server_type=server.server_type
    )
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "[::]:50058"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncSourcer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncSourcer()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel(server_port) as channel:
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

    def test_read_source(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)

            request = read_req_source_fn()
            generator_response = None
            try:
                generator_response = stub.ReadFn(request=source_pb2.ReadRequest(request=request))
            except grpc.RpcError as e:
                logging.error(e)

            counter = 0
            # capture the output from the ReadFn generator and assert.
            for r in generator_response:
                counter += 1
                self.assertEqual(
                    bytes("payload:test_mock_message", encoding="utf-8"),
                    r.result.payload,
                )
                self.assertEqual(
                    ["test_key"],
                    r.result.keys,
                )
                self.assertEqual(
                    mock_offset().offset,
                    r.result.offset.offset,
                )
                self.assertEqual(
                    mock_offset().partition_id,
                    r.result.offset.partition_id,
                )
            """Assert that the generator was called 10 times in the stream"""
            self.assertEqual(10, counter)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_ack(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            try:
                response = stub.AckFn(request=source_pb2.AckRequest(request=request))
            except grpc.RpcError as e:
                print(e)

            self.assertEqual(response, source_pb2.AckResponse())

    def test_pending(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.PendingFn(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(response.result.count, 10)

    def test_partitions(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.PartitionsFn(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(response.result.partitions, mock_partitions())

    def __stub(self):
        return source_pb2_grpc.SourceStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
