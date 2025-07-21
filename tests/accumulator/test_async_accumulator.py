import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.accumulator import (
    Message,
    Datum,
    AccumulatorAsyncServer,
    Accumulator,
)
from pynumaflow.proto.accumulator import accumulator_pb2, accumulator_pb2_grpc
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
            # Clear previous keys and add new ones
            del request.payload.keys[:]
            request.payload.keys.extend([f"key-{i}"])

        # Set operation based on index - first is OPEN, rest are APPEND
        if i == 0:
            request.operation.event = accumulator_pb2.AccumulatorRequest.WindowOperation.Event.OPEN
        else:
            request.operation.event = (
                accumulator_pb2.AccumulatorRequest.WindowOperation.Event.APPEND
            )
        yield request


def start_request() -> accumulator_pb2.AccumulatorRequest:
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = accumulator_pb2.KeyedWindow(
        start=mock_interval_window_start(),
        end=mock_interval_window_end(),
        slot="slot-0",
        keys=["test_key"],
    )
    payload = accumulator_pb2.Payload(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
    )
    operation = accumulator_pb2.AccumulatorRequest.WindowOperation(
        event=accumulator_pb2.AccumulatorRequest.WindowOperation.Event.OPEN,
        keyedWindow=window,
    )
    request = accumulator_pb2.AccumulatorRequest(
        payload=payload,
        operation=operation,
    )
    return request


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/accumulator.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(Accumulator):
    def __init__(self, counter):
        self.counter = counter

    async def handler(self, datums: AsyncIterable[Datum], output: NonBlockingIterator):
        async for datum in datums:
            self.counter += 1
            msg = f"counter:{self.counter}"
            await output.put(Message(str.encode(msg), keys=datum.keys(), tags=[]))


async def accumulator_handler_func(datums: AsyncIterable[Datum], output: NonBlockingIterator):
    counter = 0
    async for datum in datums:
        counter += 1
        msg = f"counter:{counter}"
        await output.put(Message(str.encode(msg), keys=datum.keys(), tags=[]))


def NewAsyncAccumulator():
    server_instance = AccumulatorAsyncServer(ExampleClass, init_args=(0,))
    udfs = server_instance.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    accumulator_pb2_grpc.add_AccumulatorServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/accumulator.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncAccumulator(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncAccumulator()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/accumulator.sock") as channel:
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

    def test_accumulate(self) -> None:
        stub = self.__stub()
        request = start_request()
        generator_response = None
        try:
            generator_response = stub.AccumulateFn(
                request_iterator=request_generator(count=5, request=request)
            )
        except grpc.RpcError as e:
            logging.error(e)

        # capture the output from the AccumulateFn generator and assert.
        count = 0
        eof_count = 0
        for r in generator_response:
            if hasattr(r, "payload") and r.payload.value:
                count += 1
                # Each datum should increment the counter
                expected_msg = f"counter:{count}"
                self.assertEqual(
                    bytes(expected_msg, encoding="utf-8"),
                    r.payload.value,
                )
                self.assertEqual(r.EOF, False)
                # Check that keys are preserved
                self.assertEqual(list(r.payload.keys), ["test_key"])
            else:
                self.assertEqual(r.EOF, True)
                eof_count += 1

        # We should have received 5 messages (one for each datum)
        self.assertEqual(5, count)
        self.assertEqual(1, eof_count)

    def test_accumulate_with_multiple_keys(self) -> None:
        stub = self.__stub()
        request = start_request()
        generator_response = None
        try:
            generator_response = stub.AccumulateFn(
                request_iterator=request_generator(count=10, request=request, resetkey=True),
            )
        except grpc.RpcError as e:
            print(e)

        count = 0
        eof_count = 0
        key_counts = {}

        # capture the output from the AccumulateFn generator and assert.
        for r in generator_response:
            # Check for responses with values
            if r.payload.value:
                count += 1
                # Track count per key
                key = r.payload.keys[0] if r.payload.keys else "no_key"
                key_counts[key] = key_counts.get(key, 0) + 1

                # Each key should have its own counter starting from 1
                expected_msg = f"counter:{key_counts[key]}"
                self.assertEqual(
                    bytes(expected_msg, encoding="utf-8"),
                    r.payload.value,
                )
                self.assertEqual(r.EOF, False)
            else:
                eof_count += 1
                self.assertEqual(r.EOF, True)

        # We should have 10 messages (one for each key)
        self.assertEqual(10, count)
        self.assertEqual(10, eof_count)  # Each key/task sends its own EOF
        # Each key should appear once
        self.assertEqual(len(key_counts), 10)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("unix:///tmp/accumulator.sock") as channel:
            stub = accumulator_pb2_grpc.AccumulatorStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def __stub(self):
        return accumulator_pb2_grpc.AccumulatorStub(_channel)

    def test_error_init(self):
        # Check that accumulator_instance is required
        with self.assertRaises(TypeError):
            AccumulatorAsyncServer()
        # Check that the init_args and init_kwargs are passed
        # only with an Accumulator class
        with self.assertRaises(TypeError):
            AccumulatorAsyncServer(accumulator_handler_func, init_args=(0, 1))
        # Check that an instance is not passed instead of the class
        # signature
        with self.assertRaises(TypeError):
            AccumulatorAsyncServer(ExampleClass(0))

        # Check that an invalid class is passed
        class ExampleBadClass:
            pass

        with self.assertRaises(TypeError):
            AccumulatorAsyncServer(accumulator_instance=ExampleBadClass)

    def test_max_threads(self):
        # max cap at 16
        server = AccumulatorAsyncServer(accumulator_instance=ExampleClass, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = AccumulatorAsyncServer(accumulator_instance=ExampleClass, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = AccumulatorAsyncServer(accumulator_instance=ExampleClass)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
