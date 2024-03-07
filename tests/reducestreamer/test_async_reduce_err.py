import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.reducestreamer import (
    Message,
    Datum,
    ReduceStreamAsyncServer,
    ReduceStreamer,
    Metadata,
)
from pynumaflow.proto.reducer import reduce_pb2, reduce_pb2_grpc
from pynumaflow.shared.asynciter import NonBlockingIterator
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
            request.payload.keys.extend([f"key-{i}"])
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
_channel = grpc.insecure_channel("unix:///tmp/reduce_stream_err.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(ReduceStreamer):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self,
        keys: list[str],
        datums: AsyncIterable[Datum],
        output: NonBlockingIterator,
        md: Metadata,
    ):
        # print(md.start)
        async for _ in datums:
            print("HANDL-", self.counter)
            self.counter += 1
            if self.counter > 2:
                msg = f"counter:{self.counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
                raise RuntimeError("Got a runtime error from reduce handler.")
        raise RuntimeError("Got a runtime error from reduce handler.")
        print("HAND-2")
        msg = f"counter:{self.counter}"
        await output.put(Message(str.encode(msg), keys=keys))


async def reduce_handler_func(
    keys: list[str],
    datums: AsyncIterable[Datum],
    output: NonBlockingIterator,
    md: Metadata,
):
    counter = 0
    async for _ in datums:
        counter += 1
        if counter > 2:
            msg = f"counter:{counter}"
            await output.put(Message(str.encode(msg), keys=keys))
            counter = 0
    raise RuntimeError("Got a runtime error from reduce handler.")
    msg = f"counter:{counter}"
    await output.put(Message(str.encode(msg), keys=keys))


def NewAsyncReduceStreamer():
    server_instance = ReduceStreamAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer

    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/reduce_stream_err.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncReduceStreamerErr(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncReduceStreamer()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/reduce_stream_err.sock") as channel:
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

    def test_reduce(self) -> None:
        stub = self.__stub()
        request, metadata = start_request(multiple_window=False)
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request)
            )
            counter = 0
            for _ in generator_response:
                counter += 1
        except Exception as err:
            self.assertTrue("Got a runtime error from reduce handler." in err.__str__())
            return
        self.fail("Expected an exception.")

    def test_reduce_window_len(self) -> None:
        stub = self.__stub()
        request, metadata = start_request(multiple_window=True)
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request)
            )
            counter = 0
            for _ in generator_response:
                counter += 1
        except Exception as err:
            self.assertTrue(
                "reduce create operation error: invalid number of windows" in err.__str__()
            )
            return
        self.fail("Expected an exception.")

    def __stub(self):
        return reduce_pb2_grpc.ReduceStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
