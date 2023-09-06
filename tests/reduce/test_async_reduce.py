import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable
from collections.abc import Iterator

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducer import (
    Messages,
    Message,
    Datum,
    AsyncReducer,
    Metadata,
)
from pynumaflow.reducer.proto import reduce_pb2, reduce_pb2_grpc
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


async def async_reduce_handler(
    keys: list[str], datums: AsyncIterable[Datum], md: Metadata
) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )

    return Messages(Message(str.encode(msg), keys=keys))


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.keys.extend([f"key-{i}"])
        yield request


def start_request() -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()

    request = reduce_pb2.ReduceRequest(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    metadata = (
        (WIN_START_TIME, f"{mock_interval_window_start()}"),
        (WIN_END_TIME, f"{mock_interval_window_end()}"),
    )
    return request, metadata


_s: Server = None
_channel = grpc.insecure_channel("localhost:50057")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def reduce_handler(keys: list[str], datums: Iterator[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    return Messages(Message(str.encode(msg), keys=keys))


def NewAsyncReducer(
    reduce_handler=async_reduce_handler,
):
    udfs = AsyncReducer(handler=async_reduce_handler)

    return udfs


async def start_server(udfs: AsyncReducer):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    listen_addr = "[::]:50057"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncReducer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncReducer()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("localhost:50057") as channel:
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

    def test_reduce_invalid_metadata(self) -> None:
        stub = self.__stub()
        request, metadata = start_request()
        invalid_metadata = {}
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request),
                metadata=invalid_metadata,
            )
            count = 0
            for _ in generator_response:
                count += 1
        except grpc.RpcError as e:
            self.assertEqual(grpc.StatusCode.INVALID_ARGUMENT, e.code())
            self.assertEqual(
                "Expected to have all key/window_start_time/window_end_time;"
                " got start: None, end: None.",
                e.details(),
            )
        except Exception as err:
            self.fail("Expected an exception.")
            logging.error(err)

    def test_reduce(self) -> None:
        stub = self.__stub()
        request, metadata = start_request()
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request), metadata=metadata
            )
        except grpc.RpcError as e:
            logging.error(e)

        # capture the output from the ReduceFn generator and assert.
        count = 0
        for r in generator_response:
            count += 1
            self.assertEqual(
                bytes(
                    "counter:10 interval_window_start:2022-09-12 16:00:00+00:00 "
                    "interval_window_end:2022-09-12 16:01:00+00:00",
                    encoding="utf-8",
                ),
                r.results[0].value,
            )
        # since there is only one key, the output count is 1
        self.assertEqual(1, count)

    def test_reduce_with_multiple_keys(self) -> None:
        stub = self.__stub()
        request, metadata = start_request()
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=100, request=request, resetkey=True),
                metadata=metadata,
            )
        except grpc.RpcError as e:
            print(e)

        count = 0

        # capture the output from the ReduceFn generator and assert.
        for r in generator_response:
            count += 1
            self.assertEqual(
                bytes(
                    "counter:1 interval_window_start:2022-09-12 16:00:00+00:00 "
                    "interval_window_end:2022-09-12 16:01:00+00:00",
                    encoding="utf-8",
                ),
                r.results[0].value,
            )
        self.assertEqual(100, count)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("localhost:50057") as channel:
            stub = reduce_pb2_grpc.ReduceStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def __stub(self):
        return reduce_pb2_grpc.ReduceStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
