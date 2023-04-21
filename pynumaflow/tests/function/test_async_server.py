import asyncio
import logging
import threading
import unittest
from typing import AsyncIterable, List

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import WIN_START_TIME, WIN_END_TIME
from pynumaflow.function import (
    Messages,
    Message,
    Datum,
    AsyncServer,
    MessageTs,
    MessageT,
    Metadata,
)
from pynumaflow.function.proto import udfunction_pb2, udfunction_pb2_grpc
from pynumaflow.tests.function.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
    mock_interval_window_start,
    mock_interval_window_end,
    mock_new_event_time,
)

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


async def async_map_handler(keys: List[str], datum: Datum) -> Messages:
    if raise_error_from_map:
        raise ValueError("Exception thrown from map")
    val = datum.value
    msg = "payload:%s event_time:%s watermark:%s" % (
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(str.encode(msg), keys=keys))
    return messages


async def async_map_error_fn(keys: List[str], datum: Datum) -> Messages:
    raise ValueError("error invoking map")


async def async_mapt_handler(keys: List[str], datum: Datum) -> MessageTs:
    val = datum.value
    msg = "payload:%s event_time:%s watermark:%s" % (
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messagets = MessageTs()
    messagets.append(MessageT(value=val, keys=keys, event_time=mock_new_event_time()))
    return messagets


async def async_reduce_handler(
    keys: List[str], datums: AsyncIterable[Datum], md: Metadata
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
_channel = grpc.insecure_channel("localhost:50051")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncServer(
    map_handler=async_map_handler,
    mapt_handler=async_mapt_handler,
    reduce_handler=async_reduce_handler,
):
    udfs = AsyncServer(
        reduce_handler=reduce_handler,
        map_handler=map_handler,
        mapt_handler=mapt_handler,
    )

    return udfs


async def start_server(udfs: AsyncServer):
    server = grpc.aio.server()
    udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncServer()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("localhost:50051") as channel:
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

    def test_run_server(self) -> None:
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = udfunction_pb2_grpc.UserDefinedFunctionStub(channel)
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=mock_event_time())
            watermark_timestamp = _timestamp_pb2.Timestamp()
            watermark_timestamp.FromDatetime(dt=mock_watermark())

            request = udfunction_pb2.DatumRequest(
                keys=["test"],
                value=mock_message(),
                event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
                watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
            )

            metadata = (("this_metadata_will_be_skipped", "test_ignore"),)
            response = None
            try:
                response = stub.MapFn(request=request, metadata=metadata)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(1, len(response.elements))
            self.assertEqual(["test"], response.elements[0].keys)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message "
                    "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                    encoding="utf-8",
                ),
                response.elements[0].value,
            )
            LOGGER.info("Successfully validated the server")

    def test_map(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(_channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            keys=["test"],
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (("this_metadata_will_be_skipped", "test_ignore"),)

        response = None
        try:
            response = stub.MapFn(request=request, metadata=metadata)
        except grpc.RpcError as e:
            LOGGER.error(e)

        self.assertEqual(1, len(response.elements))
        self.assertEqual(["test"], response.elements[0].keys)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )

    def test_map_grpc_error(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(_channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            keys=["test"],
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (("this_metadata_will_be_skipped", "test_ignore"),)

        grpcException = None

        try:
            global raise_error_from_map
            raise_error_from_map = True
            _ = stub.MapFn(request=request, metadata=metadata)
        except grpc.RpcError as e:
            grpcException = e
            self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
            print(e.details())

        finally:
            raise_error_from_map = False

        self.assertIsNotNone(grpcException)

    def test_unimplemented_mapt(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(_channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.DatumRequest(
            keys=["test"],
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (("this_metadata_will_be_skipped", "test_ignore"),)
        grpcException = None
        try:
            _ = stub.MapTFn(request=request, metadata=metadata)
        except grpc.RpcError as e:
            grpcException = e
            self.assertEqual(grpc.StatusCode.UNIMPLEMENTED, e.code())
            self.assertEqual("Method not implemented!", e.details())

        self.assertIsNotNone(grpcException)

    def test_reduce_invalid_metadata(self) -> None:
        stub = self.__stub()
        request, metadata = start_reduce_streaming_request()
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
        request, metadata = start_reduce_streaming_request()
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
                r.elements[0].value,
            )
        # since there is only one key, the output count is 1
        self.assertEqual(1, count)

    def test_reduce_with_multiple_keys(self) -> None:
        stub = self.__stub()
        request, metadata = start_reduce_streaming_request()
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
                r.elements[0].value,
            )
        self.assertEqual(100, count)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = udfunction_pb2_grpc.UserDefinedFunctionStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def __stub(self):
        return udfunction_pb2_grpc.UserDefinedFunctionStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
