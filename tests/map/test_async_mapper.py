import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow._constants import MAX_MESSAGE_SIZE
from pynumaflow.mapper import (
    Datum,
    Messages,
    Message,
)
from pynumaflow.mapper.async_server import MapAsyncServer
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.map.utils import get_test_datums
from tests.testing_utils import (
    mock_terminate_on_stop,
)

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


def request_generator(req):
    yield from req


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
    _server_options = [
        ("grpc.max_send_message_length", MAX_MESSAGE_SIZE),
        ("grpc.max_receive_message_length", MAX_MESSAGE_SIZE),
    ]
    server = grpc.aio.server(options=_server_options)
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
            request = get_test_datums()
            generator_response = None
            try:
                generator_response = stub.MapFn(request_iterator=request_generator(request))
            except grpc.RpcError as e:
                logging.error(e)

            responses = []
            # capture the output from the ReadFn generator and assert.
            for r in generator_response:
                responses.append(r)

            # 1 handshake + 3 data responses
            self.assertEqual(4, len(responses))

            self.assertTrue(responses[0].handshake.sot)

            idx = 1
            while idx < len(responses):
                _id = "test-id-" + str(idx)
                self.assertEqual(_id, responses[idx].id)
                self.assertEqual(
                    bytes(
                        "payload:test_mock_message "
                        "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                        encoding="utf-8",
                    ),
                    responses[idx].results[0].value,
                )
                self.assertEqual(1, len(responses[idx].results))
                idx += 1
            LOGGER.info("Successfully validated the server")

    def test_map(self) -> None:
        stub = map_pb2_grpc.MapStub(_channel)
        request = get_test_datums()
        generator_response = None
        try:
            generator_response = stub.MapFn(request_iterator=request_generator(request))
        except grpc.RpcError as e:
            logging.error(e)

        responses = []
        # capture the output from the ReadFn generator and assert.
        for r in generator_response:
            responses.append(r)

        # 1 handshake + 3 data responses
        self.assertEqual(4, len(responses))

        self.assertTrue(responses[0].handshake.sot)

        idx = 1
        while idx < len(responses):
            _id = "test-id-" + str(idx)
            self.assertEqual(_id, responses[idx].id)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message "
                    "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                    encoding="utf-8",
                ),
                responses[idx].results[0].value,
            )
            self.assertEqual(1, len(responses[idx].results))
            idx += 1

    def test_map_grpc_error_no_handshake(self) -> None:
        stub = map_pb2_grpc.MapStub(_channel)
        request = get_test_datums(handshake=False)
        grpc_exception = None

        responses = []
        try:
            generator_response = stub.MapFn(request_iterator=request_generator(request))
            # capture the output from the ReadFn generator and assert.
            for r in generator_response:
                responses.append(r)
        except grpc.RpcError as e:
            logging.error(e)
            grpc_exception = e
            self.assertTrue("MapFn: expected handshake message" in e.__str__())

        self.assertEqual(0, len(responses))
        self.assertIsNotNone(grpc_exception)

    def test_map_grpc_error(self) -> None:
        stub = map_pb2_grpc.MapStub(_channel)
        request = get_test_datums()
        grpc_exception = None

        responses = []
        try:
            global raise_error_from_map
            raise_error_from_map = True
            generator_response = stub.MapFn(request_iterator=request_generator(request))
            # capture the output from the ReadFn generator and assert.
            for r in generator_response:
                responses.append(r)
        except grpc.RpcError as e:
            logging.error(e)
            grpc_exception = e
            self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
            self.assertTrue("Exception thrown from map" in e.__str__())
        finally:
            raise_error_from_map = False
        # 1 handshake
        self.assertEqual(1, len(responses))
        self.assertIsNotNone(grpc_exception)

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

    def test_max_threads(self):
        # max cap at 16
        server = MapAsyncServer(mapper_instance=async_map_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = MapAsyncServer(mapper_instance=async_map_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = MapAsyncServer(mapper_instance=async_map_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
