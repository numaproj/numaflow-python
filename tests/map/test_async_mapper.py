import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.mapper import (
    Datum,
    Messages,
    Message,
)
from pynumaflow.mapper.async_server import MapAsyncServer
from pynumaflow.proto.mapper import map_pb2, map_pb2_grpc
from tests.testing_utils import (
    mock_event_time,
    mock_watermark,
    mock_headers,
    mock_message,
    get_time_args,
    mock_terminate_on_stop,
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


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/async_map.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def new_async_mapper():
    server = MapAsyncServer(mapper_instance=async_map_handler)
    udfs = server.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_map.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestAsyncMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = new_async_mapper()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/async_map.sock") as channel:
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
        with grpc.insecure_channel("unix:///tmp/async_map.sock") as channel:
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
                headers=mock_headers(),
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
            headers=mock_headers(),
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
        event_time_timestamp, watermark_timestamp = get_time_args()

        request = map_pb2.MapRequest(
            keys=["test"],
            value=mock_message(),
            event_time=event_time_timestamp,
            watermark=watermark_timestamp,
            headers=mock_headers(),
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
        with grpc.insecure_channel("unix:///tmp/async_map.sock") as channel:
            stub = map_pb2_grpc.MapStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            MapAsyncServer()

    def __stub(self):
        return map_pb2_grpc.MapStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
