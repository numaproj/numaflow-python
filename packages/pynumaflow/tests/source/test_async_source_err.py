import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

import grpc

from grpc.aio import Server

from pynumaflow import setup_logging
from pynumaflow.proto.sourcer import source_pb2_grpc
from google.protobuf import empty_pb2 as _empty_pb2

from pynumaflow.sourcer import SourceAsyncServer
from tests.source.test_async_source import request_generator
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    AsyncSourceError,
    nack_req_source_fn,
)
from tests.testing_utils import mock_terminate_on_stop

LOGGER = setup_logging(__name__)

_s: Server = None
server_port = "unix:///tmp/async_err_source.sock"
_channel = grpc.insecure_channel(server_port)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    class_instance = AsyncSourceError()
    server_instance = SourceAsyncServer(sourcer_instance=class_instance)
    udfs = server_instance.servicer
    source_pb2_grpc.add_SourceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_err_source.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
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
                with grpc.insecure_channel("unix:///tmp/async_err_source.sock") as channel:
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
        grpc_exception = None
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = read_req_source_fn()
            generator_response = None
            try:
                generator_response = stub.ReadFn(
                    request_iterator=request_generator(1, request, "read")
                )
                for _ in generator_response:
                    pass
            except grpc.RpcError as e:
                grpc_exception = e
                self.assertEqual(grpc.StatusCode.INTERNAL, e.code())
                print(e.details())

        self.assertIsNotNone(grpc_exception)

    def test_read_handshake_error(self) -> None:
        grpc_exception = None
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = read_req_source_fn()
            generator_response = None
            try:
                generator_response = stub.ReadFn(
                    request_iterator=request_generator(1, request, "read", False)
                )
                for _ in generator_response:
                    pass
            except BaseException as e:
                self.assertTrue("ReadFn: expected handshake message" in e.__str__())
                return
            except grpc.RpcError as e:
                grpc_exception = e
                self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
                print(e.details())

        self.assertIsNotNone(grpc_exception)
        self.fail("Expected an exception.")

    def test_ack_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            try:
                resp = stub.AckFn(request_iterator=request_generator(1, request, "ack"))
                for _ in resp:
                    pass
            except BaseException as e:
                self.assertTrue("Got a runtime error from ack handler." in e.__str__())
                return
            except grpc.RpcError as e:
                self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
                print(e.details())
        self.fail("Expected an exception.")

    def test_nack_error(self):
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = nack_req_source_fn()
            with self.assertRaisesRegex(
                grpc.RpcError, "Got a runtime error from nack handler."
            ) as resp:
                stub.NackFn(request=request)

            self.assertEqual(grpc.StatusCode.INTERNAL, resp.exception.code())

    def test_ack_no_handshake_error(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            try:
                resp = stub.AckFn(request_iterator=request_generator(1, request, "ack", False))
                for _ in resp:
                    pass
            except BaseException as e:
                self.assertTrue("AckFn: expected handshake message" in e.__str__())
                return
            except grpc.RpcError as e:
                self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
                print(e.details())
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

    def test_invalid_server_type(self) -> None:
        with self.assertRaises(TypeError):
            SourceAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
