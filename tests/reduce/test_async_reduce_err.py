import asyncio
import logging
import unittest
from collections.abc import AsyncIterable

import grpc
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducer import (
    Messages,
    Message,
    Datum,
    Metadata,
    ReduceAsyncServer,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from tests.testing_utils import (
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    get_time_args,
)

LOGGER = setup_logging(__name__)


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.keys.extend([f"key-{i}"])
        yield request


def start_request(multiple_window: False) -> (Datum, tuple):
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = reduce_pb2.Window(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
    )
    payload = reduce_pb2.ReduceRequest.Payload(
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
    )
    operation = reduce_pb2.ReduceRequest.WindowOperation(
        event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
        windows=[window],
    )
    if multiple_window:
        operation = reduce_pb2.ReduceRequest.WindowOperation(
            event=reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND,
            windows=[window, window],
        )

    request = reduce_pb2.ReduceRequest(
        payload=payload,
        operation=operation,
    )
    metadata = (
        (WIN_START_TIME, f"{mock_interval_window_start()}"),
        (WIN_END_TIME, f"{mock_interval_window_end()}"),
    )
    return request, metadata


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/reduce_err.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def err_handler(keys: list[str], datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )
    raise RuntimeError("Got a runtime error from reduce handler.")
    return Messages(Message(str.encode(msg), keys=keys))


def NewAsyncReducer():
    server_instance = ReduceAsyncServer(err_handler)
    udfs = server_instance.servicer

    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/reduce_err.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# TODO: Check why terminating even after mocking
# We are mocking the terminate function from the psutil to not exit the program during testing
# @patch("psutil.Process.terminate", mock_terminate_on_stop)
# class TestAsyncReducerError(unittest.TestCase):
#     @classmethod
#     def setUpClass(cls) -> None:
#         global _loop
#         loop = asyncio.new_event_loop()
#         _loop = loop
#         _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
#         _thread.start()
#         udfs = NewAsyncReducer()
#         asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
#         while True:
#             try:
#                 with grpc.insecure_channel("unix:///tmp/reduce_err.sock") as channel:
#                     f = grpc.channel_ready_future(channel)
#                     f.result(timeout=10)
#                     if f.done():
#                         break
#             except grpc.FutureTimeoutError as e:
#                 LOGGER.error("error trying to connect to grpc server")
#                 LOGGER.error(e)
#
#     @classmethod
#     def tearDownClass(cls) -> None:
#         try:
#             _loop.stop()
#             LOGGER.info("stopped the event loop")
#         except Exception as e:
#             LOGGER.error(e)
#
#     def test_reduce(self) -> None:
#         stub = self.__stub()
#         request, metadata = start_request(multiple_window=False)
#         generator_response = None
#         try:
#             generator_response = stub.ReduceFn(
#                 request_iterator=request_generator(count=10, request=request)
#             )
#             counter = 0
#             for _ in generator_response:
#                 counter += 1
#         except Exception as err:
#             self.assertTrue("Got a runtime error from reduce handler." in err.__str__())
#             return
#         self.fail("Expected an exception.")
#
#     def test_reduce_window_len(self) -> None:
#         stub = self.__stub()
#         request, metadata = start_request(multiple_window=True)
#         generator_response = None
#         try:
#             generator_response = stub.ReduceFn(
#                 request_iterator=request_generator(count=10, request=request)
#             )
#             counter = 0
#             for _ in generator_response:
#                 counter += 1
#         except Exception as err:
#             self.assertTrue(
#                 "reduce create operation error: invalid number of windows" in err.__str__()
#             )
#             return
#         self.fail("Expected an exception.")
#
#     def __stub(self):
#         return reduce_pb2_grpc.ReduceStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
