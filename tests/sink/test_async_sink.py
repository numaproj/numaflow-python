import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.sinker import (
    Datum,
)
from pynumaflow.sinker import Responses, Response
from pynumaflow.proto.sinker import sink_pb2_grpc, sink_pb2
from pynumaflow.sinker.async_server import SinkAsyncServer
from tests.sink.test_server import (
    mock_message,
    mock_err_message,
)
from tests.testing_utils import get_time_args

LOGGER = setup_logging(__name__)


async def udsink_handler(datums: AsyncIterable[Datum]) -> Responses:
    responses = Responses()
    async for msg in datums:
        if msg.value.decode("utf-8") == "test_mock_err_message":
            raise ValueError("test_mock_err_message")
        print("User Defined Sink", msg.value.decode("utf-8"))
        responses.append(Response.as_success(msg.id))
    return responses


#
def request_generator(count, request):
    for i in range(count):
        request.id = str(i)
        yield request


#
#
def start_sink_streaming_request(err=False) -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    value = mock_message()
    if err:
        value = mock_err_message()

    request = sink_pb2.SinkRequest(
        value=value,
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    return request


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/async_sink.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    server_instance = SinkAsyncServer(sinker_instance=udsink_handler)
    uds = server_instance.servicer
    sink_pb2_grpc.add_SinkServicer_to_server(uds, server)
    listen_addr = "unix:///tmp/async_sink.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncSink(unittest.TestCase):
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
                with grpc.insecure_channel("unix:///tmp/async_sink.sock") as channel:
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

    #
    def test_run_server(self) -> None:
        with grpc.insecure_channel("unix:///tmp/async_sink.sock") as channel:
            stub = sink_pb2_grpc.SinkStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_sink(self) -> None:
        stub = self.__stub()
        request = start_sink_streaming_request()
        print(request)
        generator_response = None
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, request=request)
            )
        except grpc.RpcError as e:
            logging.error(e)

        # capture the output from the ReduceFn generator and assert.
        self.assertEqual(10, len(generator_response.results))
        for x in generator_response.results:
            self.assertTrue(x.success)

    def test_sink_err(self) -> None:
        stub = self.__stub()
        request = start_sink_streaming_request(err=True)
        # print(request)
        generator_response = None
        try:
            generator_response = stub.SinkFn(
                request_iterator=request_generator(count=10, request=request)
            )
        except grpc.RpcError as e:
            logging.error(e)

        # capture the output from the ReduceFn generator and assert.
        for x in generator_response.results:
            self.assertFalse(x.success)

        # since there is only one key, the output count is 1

    def __stub(self):
        return sink_pb2_grpc.SinkStub(_channel)

    def test_invalid_server_type(self) -> None:
        with self.assertRaises(TypeError):
            SinkAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
