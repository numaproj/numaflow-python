import asyncio
import logging
import threading
import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.mapper import (
    AsyncMapper,
    Datum,
    Messages,
    Message,
)
from pynumaflow.mapper.proto import map_pb2_grpc, map_pb2
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_message,
)

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


async def async_map_handler(keys: list[str], datum: Datum) -> Messages:
    if raise_error_from_map:
        raise ValueError("Exception thrown from map")
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    val = bytes(msg, encoding="utf-8")
    messages = Messages()
    messages.append(Message(str.encode(msg), keys=keys))
    return messages


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.keys.extend([f"key-{i}"])
        yield request


_s: Server = None
_channel = grpc.insecure_channel("localhost:50056")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncMapper(
    map_handler=async_map_handler,
):
    udfs = AsyncMapper(
        map_handler=async_map_handler,
    )

    return udfs


async def start_server(udfs: AsyncMapper):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    listen_addr = "[::]:50056"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncMapper()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("localhost:50056") as channel:
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
        with grpc.insecure_channel("localhost:50056") as channel:
            stub = map_pb2_grpc.MapStub(channel)
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=mock_event_time())
            watermark_timestamp = _timestamp_pb2.Timestamp()
            watermark_timestamp.FromDatetime(dt=mock_watermark())

            request = map_pb2.MapRequest(
                keys=["test"],
                value=mock_message(),
                event_time=event_time_timestamp,
                watermark=watermark_timestamp,
            )

            response = None
            try:
                response = stub.MapFn(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(1, len(response.results))
            self.assertEqual(["test"], response.results[0].keys)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message "
                    "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                    encoding="utf-8",
                ),
                response.results[0].value,
            )
            LOGGER.info("Successfully validated the server")

    def test_map(self) -> None:
        stub = map_pb2_grpc.MapStub(_channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = map_pb2.MapRequest(
            keys=["test"],
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
        )
        response = None
        try:
            response = stub.MapFn(request=request)
        except grpc.RpcError as e:
            LOGGER.error(e)

        self.assertEqual(1, len(response.results))
        self.assertEqual(["test"], response.results[0].keys)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.results[0].value,
        )

    def test_map_grpc_error(self) -> None:
        stub = map_pb2_grpc.MapStub(_channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = map_pb2.MapRequest(
            keys=["test"],
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
        )

        grpcException = None

        try:
            global raise_error_from_map
            raise_error_from_map = True
            _ = stub.MapFn(request=request)
        except grpc.RpcError as e:
            grpcException = e
            self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
            print(e.details())

        finally:
            raise_error_from_map = False

        self.assertIsNotNone(grpcException)

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("localhost:50056") as channel:
            stub = map_pb2_grpc.MapStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def __stub(self):
        return map_pb2_grpc.MapStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
