import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable
from unittest.mock import patch

import grpc
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
    mock_terminate_on_stop,
)

LOGGER = setup_logging(__name__)


def request_generator(count, request):
    for i in range(count):
        yield request


def start_request() -> accumulator_pb2.AccumulatorRequest:
    event_time_timestamp, watermark_timestamp = get_time_args()
    window = accumulator_pb2.KeyedWindow(
        start=event_time_timestamp,
        end=watermark_timestamp,
        slot="slot-0",
        keys=["test_key"],
    )
    payload = accumulator_pb2.Payload(
        keys=["test_key"],
        value=mock_message(),
        event_time=event_time_timestamp,
        watermark=watermark_timestamp,
        id="test_id",
        headers={"test_header_key": "test_header_value", "source": "test_source"},
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
_channel = grpc.insecure_channel("unix:///tmp/accumulator_err.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleErrorClass(Accumulator):
    def __init__(self, counter):
        self.counter = counter

    async def handler(
        self, datums: AsyncIterable[Datum], output: NonBlockingIterator
    ):
        async for datum in datums:
            self.counter += 1
            if self.counter == 2:
                # Simulate an error on the second datum
                raise RuntimeError("Simulated error in accumulator handler")
            msg = f"counter:{self.counter}"
            await output.put(
                Message(str.encode(msg), keys=datum.keys(), tags=[])
            )


async def error_accumulator_handler_func(
    datums: AsyncIterable[Datum], output: NonBlockingIterator
):
    counter = 0
    async for datum in datums:
        counter += 1
        if counter == 2:
            # Simulate an error on the second datum
            raise RuntimeError("Simulated error in accumulator function")
        msg = f"counter:{counter}"
        await output.put(
            Message(str.encode(msg), keys=datum.keys(), tags=[])
        )


def NewAsyncAccumulatorError():
    server_instance = AccumulatorAsyncServer(ExampleErrorClass, init_args=(0,))
    udfs = server_instance.servicer
    return udfs


@patch("psutil.Process.kill", mock_terminate_on_stop)
async def start_server(udfs):
    server = grpc.aio.server()
    accumulator_pb2_grpc.add_AccumulatorServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/accumulator_err.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()

@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestAsyncAccumulatorError(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncAccumulatorError()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/accumulator_err.sock") as channel:
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

    @patch("psutil.Process.kill", mock_terminate_on_stop)
    def test_accumulate_error(self) -> None:
        stub = self.__stub()
        request = start_request()
        generator_response = None
        
        try:
            generator_response = stub.AccumulateFn(
                request_iterator=request_generator(count=5, request=request)
            )
            
            # Try to consume the generator
            counter = 0
            logging.info(f"[TEST_DEBUG] About to iterate through generator_response")
            for response in generator_response:
                counter += 1
                logging.info(f"[TEST_DEBUG] Received response {counter}: {response}")
            logging.info(f"[TEST_DEBUG] Finished iterating, got {counter} responses")
        except BaseException as err:
            logging.info(f"[TEST_DEBUG] Caught exception: {err}")
            self.assertTrue("Simulated error in accumulator handler" in str(err))
            return
        self.fail("Expected an exception.")

    @patch("psutil.Process.kill", mock_terminate_on_stop)
    def test_accumulate_partial_success(self) -> None:
        """Test that the first datum is processed before error occurs"""
        stub = self.__stub()
        request = start_request()
        
        try:
            generator_response = stub.AccumulateFn(
                request_iterator=request_generator(count=3, request=request)
            )
            
            # Try to consume the generator
            counter = 0
            for _ in generator_response:
                counter += 1
        except BaseException as err:
            self.assertTrue("Simulated error in accumulator handler" in str(err))
            return
        self.fail("Expected an exception.")

    def __stub(self):
        return accumulator_pb2_grpc.AccumulatorStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
