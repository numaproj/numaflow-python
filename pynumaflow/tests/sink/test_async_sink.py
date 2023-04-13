import asyncio
import logging
import threading
import unittest
from typing import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.function import (
    Datum,
)
from pynumaflow.sink import Responses, Response, AsyncSink
from pynumaflow.sink.proto import udsink_pb2
from pynumaflow.sink.proto import udsink_pb2_grpc
from pynumaflow.tests.sink.test_server import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_err_message,
)

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
    event_time_timestamp = _timestamp_pb2.Timestamp()
    event_time_timestamp.FromDatetime(dt=mock_event_time())
    watermark_timestamp = _timestamp_pb2.Timestamp()
    watermark_timestamp.FromDatetime(dt=mock_watermark())
    value = mock_message()
    if err:
        value = mock_err_message()

    request = udsink_pb2.DatumRequest(
        value=value,
        event_time=udsink_pb2.EventTime(event_time=event_time_timestamp),
        watermark=udsink_pb2.Watermark(watermark=watermark_timestamp),
    )
    return request


_s: Server = None
_channel = grpc.insecure_channel("localhost:50055")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    uds = AsyncSink(sink_handler=udsink_handler)
    udsink_pb2_grpc.add_UserDefinedSinkServicer_to_server(uds, server)
    listen_addr = "[::]:50055"
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
                with grpc.insecure_channel("localhost:50055") as channel:
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
        with grpc.insecure_channel("localhost:50055") as channel:
            stub = udsink_pb2_grpc.UserDefinedSinkStub(channel)

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
        self.assertEqual(10, len(generator_response.responses))
        for x in generator_response.responses:
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
        for x in generator_response.responses:
            self.assertFalse(x.success)

        # since there is only one key, the output count is 1

    def __stub(self):
        return udsink_pb2_grpc.UserDefinedSinkStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
