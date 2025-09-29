import asyncio
import logging
import unittest
from unittest.mock import patch

import grpc

from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.sourcer import SourceAsyncServer
from pynumaflow.proto.sourcer import source_pb2_grpc
from google.protobuf import empty_pb2 as _empty_pb2

from tests.source.test_async_source import request_generator
from tests.source.utils import (
    read_req_source_fn,
    ack_req_source_fn,
    AsyncSourceError,
)
from tests.testing_utils import mock_terminate_on_stop

LOGGER = setup_logging(__name__)

# Mock the handle_async_error function to prevent process SIGKILL
async def mock_handle_async_error(context, exception, exception_type):
    """Mock handle_async_error to prevent process termination during tests."""
    from pynumaflow.shared.server import update_context_err

    err_msg = f"{exception_type}: {repr(exception)}"
    update_context_err(context, exception, err_msg)


# We are mocking the error handler to not exit the program during testing
@patch("pynumaflow.sourcer.servicer.async_servicer.handle_async_error", mock_handle_async_error)
class TestAsyncServerErrorScenario(unittest.IsolatedAsyncioTestCase):
    listen_addr = "unix:///tmp/async_err_source.sock"

    async def asyncSetUp(self) -> None:
        # Create a fresh server for each test to avoid event loop issues
        self.server = grpc.aio.server()
        class_instance = AsyncSourceError()
        server_instance = SourceAsyncServer(sourcer_instance=class_instance)
        udfs = server_instance.servicer
        source_pb2_grpc.add_SourceServicer_to_server(udfs, self.server)
        self.server.add_insecure_port(self.listen_addr)
        await self.server.start()

        # Wait for server to be ready with timeout
        max_attempts = 50  # 5 seconds total (50 * 0.1)
        attempt = 0
        while attempt < max_attempts:
            try:
                async with grpc.aio.insecure_channel(self.listen_addr) as channel:
                    await channel.channel_ready()
                    break
            except Exception as e:
                LOGGER.debug("Waiting for server to be ready, attempt %d", attempt + 1)
                await asyncio.sleep(0.1)
            attempt += 1

        if attempt >= max_attempts:
            raise RuntimeError("Server failed to start within timeout period")

    async def asyncTearDown(self) -> None:
        # Stop the server after each test
        if hasattr(self, 'server') and self.server is not None:
            await self.server.stop(0)
            # Small delay to ensure socket cleanup
            await asyncio.sleep(0.1)

    async def test_read_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = read_req_source_fn()
            with self.assertRaises(grpc.RpcError) as resp_err:
                generator_response = stub.ReadFn(
                    request_iterator=request_generator(1, request, "read")
                )
                # await anext(aiter(generator_response)) should work for Python >=3.10
                [_ async for _ in generator_response]
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("Got a runtime error from read handler." in resp_err.exception.details())


    async def test_read_handshake_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = read_req_source_fn()
            with self.assertRaises(grpc.RpcError) as resp_err:
                generator_response = stub.ReadFn(
                    request_iterator=request_generator(1, request, "read", False)
                )
                # await anext(aiter(generator_response)) should work for Python >=3.10
                [_ async for _ in generator_response]
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("ReadFn: expected handshake message" in resp_err.exception.details())


    async def test_ack_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            with self.assertRaises(grpc.RpcError) as resp_err:
                resp = stub.AckFn(request_iterator=request_generator(1, request, "ack"))
                [_ async for _ in resp]
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("Got a runtime error from ack handler." in resp_err.exception.details())

    async def test_ack_no_handshake_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = ack_req_source_fn()
            with self.assertRaises(grpc.RpcError) as resp_err:
                resp = stub.AckFn(request_iterator=request_generator(1, request, "ack", False))
                [_ async for _ in resp]
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("AckFn: expected handshake message" in resp_err.exception.details())

    async def test_pending_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            with self.assertRaises(grpc.RpcError) as resp_err:
                await stub.PendingFn(request=request)
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("Got a runtime error from pending handler." in resp_err.exception.details())

    async def test_partition_error(self) -> None:
        async with grpc.aio.insecure_channel(self.listen_addr) as channel:
            stub = source_pb2_grpc.SourceStub(channel)
            request = _empty_pb2.Empty()
            with self.assertRaises(grpc.RpcError) as resp_err:
                response = await stub.PartitionsFn(request=request)
                # Force evaluation of the response
                _ = response.result
            self.assertEqual(grpc.StatusCode.INTERNAL, resp_err.exception.code())
            self.assertTrue("Got a runtime error from partition handler." in resp_err.exception.details())

    async def test_invalid_server_type(self) -> None:
        with self.assertRaises(TypeError):
            SourceAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
