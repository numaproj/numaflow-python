import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
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

        if i % 2:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.OPEN
        else:
            request.operation.event = reduce_pb2.ReduceRequest.WindowOperation.Event.APPEND
        yield request


def start_request() -> (Datum, tuple):
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
_channel = grpc.insecure_channel("unix:///tmp/reduce_stream.sock")
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
            self.counter += 1
            if self.counter > 2:
                msg = f"counter:{self.counter}"
                await output.put(Message(str.encode(msg), keys=keys))
                self.counter = 0
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
    msg = f"counter:{counter}"
    await output.put(Message(str.encode(msg), keys=keys))


def NewAsyncReduceStreamer():
    server_instance = ReduceStreamAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer

    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    reduce_pb2_grpc.add_ReduceServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/reduce_stream.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncReduceStreamer(unittest.TestCase):
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
                with grpc.insecure_channel("unix:///tmp/reduce_stream.sock") as channel:
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
        request, metadata = start_request()
        generator_response = None

        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=10, request=request)
            )
        except grpc.RpcError as e:
            logging.error(e)

        # capture the output from the ReduceFn generator and assert.
        count = 0
        eof_count = 0
        for r in generator_response:
            if r.result.value:
                count += 1
                if count <= 3:
                    self.assertEqual(
                        bytes(
                            "counter:3",
                            encoding="utf-8",
                        ),
                        r.result.value,
                    )
                else:
                    self.assertEqual(
                        bytes(
                            "counter:1",
                            encoding="utf-8",
                        ),
                        r.result.value,
                    )
                self.assertEqual(r.EOF, False)
            else:
                self.assertEqual(r.EOF, True)
                eof_count += 1
            self.assertEqual(r.window.start.ToSeconds(), 1662998400)
            self.assertEqual(r.window.end.ToSeconds(), 1662998460)
        # in our example we should be return 3 messages early with counter:3
        # and last message with counter:1
        self.assertEqual(4, count)
        self.assertEqual(1, eof_count)

    def test_reduce_with_multiple_keys(self) -> None:
        stub = self.__stub()
        request, metadata = start_request()
        generator_response = None
        try:
            generator_response = stub.ReduceFn(
                request_iterator=request_generator(count=100, request=request, resetkey=True),
            )
        except grpc.RpcError as e:
            print(e)

        count = 0
        eof_count = 0

        # capture the output from the ReduceFn generator and assert.
        for r in generator_response:
            # Check for responses with
            if r.result.value:
                count += 1
                self.assertEqual(
                    bytes(
                        "counter:1",
                        encoding="utf-8",
                    ),
                    r.result.value,
                )
                self.assertEqual(r.EOF, False)
            else:
                eof_count += 1
                self.assertEqual(r.EOF, True)
            self.assertEqual(r.window.start.ToSeconds(), 1662998400)
            self.assertEqual(r.window.end.ToSeconds(), 1662998460)
        self.assertEqual(100, count)
        self.assertEqual(1, eof_count)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("unix:///tmp/reduce_stream.sock") as channel:
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

    def test_error_init(self):
        # Check that reducer_handler in required
        with self.assertRaises(TypeError):
            ReduceStreamAsyncServer()
        # Check that the init_args and init_kwargs are passed
        # only with a Reducer class
        with self.assertRaises(TypeError):
            ReduceStreamAsyncServer(reduce_handler_func, init_args=(0, 1))
        # Check that an instance is not passed instead of the class
        # signature
        with self.assertRaises(TypeError):
            ReduceStreamAsyncServer(ExampleClass(0))

        # Check that an invalid class is passed
        class ExampleBadClass:
            pass

        with self.assertRaises(TypeError):
            ReduceStreamAsyncServer(reduce_stream_handler=ExampleBadClass)

    def test_max_threads(self):
        # max cap at 16
        server = ReduceStreamAsyncServer(reduce_stream_handler=ExampleClass, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = ReduceStreamAsyncServer(reduce_stream_handler=ExampleClass, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = ReduceStreamAsyncServer(reduce_stream_handler=ExampleClass)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
