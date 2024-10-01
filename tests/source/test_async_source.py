import asyncio
import logging
import threading
import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.proto.sourcer import source_pb2_grpc, source_pb2
from pynumaflow.sourcer import (
    SourceAsyncServer,
)
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    mock_partitions,
    AsyncSource,
    mock_offset,
)

LOGGER = setup_logging(__name__)

server_port = "unix:///tmp/async_source.sock"

_s: Server = None
_channel = grpc.insecure_channel(server_port)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncSourcer():
    class_instance = AsyncSource()
    server = SourceAsyncServer(sourcer_instance=class_instance)
    udfs = server.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_source.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


def request_generator(count, request, req_type, resetkey: bool = False):
    for i in range(count):
        if req_type == "read":
            yield source_pb2.ReadRequest(handshake=source_pb2.Handshake(sot=True))
            yield source_pb2.ReadRequest(request=request)
        elif req_type == "ack":
            yield source_pb2.AckRequest(handshake=source_pb2.Handshake(sot=True))
            yield source_pb2.AckRequest(request=request)


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
                generator_response = stub.ReadFn(
                    request_iterator=request_generator(1, request, "read")
                )
            except grpc.RpcError as e:
                logging.error(e)

            counter = 0
            first = True
            # capture the output from the ReadFn generator and assert.
            for r in generator_response:
                counter += 1
                if first:
                    self.assertEqual(True, r.handshake.sot)
                    first = False
                    continue

                if r.status.eot:
                    last = True
                    continue

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

            self.assertFalse(first)
            self.assertTrue(last)

            # Assert that the generator was called 12
            # (10 data messages + handshake + eot) times in the stream
            self.assertEqual(12, counter)

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
                response = stub.AckFn(request_iterator=request_generator(1, request, "ack"))
            except grpc.RpcError as e:
                print(e)

            count = 0
            first = True
            for r in response:
                count += 1
                if first:
                    self.assertEqual(True, r.handshake.sot)
                    first = False
                    continue
                self.assertTrue(r.result.success)

            self.assertEqual(count, 2)
            self.assertFalse(first)

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

    def test_max_threads(self):
        class_instance = AsyncSource()
        # max cap at 16
        server = SourceAsyncServer(sourcer_instance=class_instance, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = SourceAsyncServer(sourcer_instance=class_instance, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = SourceAsyncServer(sourcer_instance=class_instance)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
