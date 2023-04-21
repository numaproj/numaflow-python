import asyncio
import logging
import threading
import unittest
from typing import AsyncIterable

import grpc
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.function import Messages, Message, Datum, Metadata, AsyncServer
from pynumaflow.function.proto import udfunction_pb2, udfunction_pb2_grpc
from pynumaflow.tests.function.testing_utils import (
    mapt_handler,
    map_handler,
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
)

LOGGER = setup_logging(__name__)


# This handler mimics the scenario where reduce UDF throws a runtime error.
async def err_async_reduce_handler(
    key: str, datums: AsyncIterable[Datum], md: Metadata
) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
        raise ValueError("Got a runtime error from reduce handler.")

    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )

    return Messages(Message.to_vtx(key, str.encode(msg)))


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.key = f"key-{i}"
        yield request


def start_reduce_streaming_request() -> (Datum, tuple):
    event_time_timestamp = _timestamp_pb2.Timestamp()
    event_time_timestamp.FromDatetime(dt=mock_event_time())
    watermark_timestamp = _timestamp_pb2.Timestamp()
    watermark_timestamp.FromDatetime(dt=mock_watermark())

    request = udfunction_pb2.DatumRequest(
        value=mock_message(),
        event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
        watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
    )

    metadata = (
        (WIN_START_TIME, f"{mock_interval_window_start()}"),
        (WIN_END_TIME, f"{mock_interval_window_end()}"),
    )
    return request, metadata


_s: Server = None
_channel = grpc.insecure_channel("localhost:50052")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    udfs = AsyncServer(
        reduce_handler=err_async_reduce_handler,
        map_handler=map_handler,
        mapt_handler=mapt_handler,
    )
    udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server)
    listen_addr = "[::]:50052"
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
                with grpc.insecure_channel("localhost:50052") as channel:
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

    def test_reduce_error(self) -> None:
        stub = self.__stub()
        request, metadata = start_reduce_streaming_request()
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request), metadata=metadata
            )
            count = 0
            for _ in generator_response:
                count += 1
        except grpc.RpcError as e:
            self.assertEqual(e.code(), grpc.StatusCode.UNKNOWN)
            self.assertEqual(e.details(), "Got a runtime error from reduce handler.")
            return

        self.fail("Expected an exception.")

    def __stub(self):
        return udfunction_pb2_grpc.UserDefinedFunctionStub(_channel)

    def test_invalid_input(self):
        with self.assertRaises(ValueError):
            AsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
