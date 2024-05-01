import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable
from unittest.mock import patch

import grpc

from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.mapstreamer import Message, Datum, MapStreamAsyncServer
from pynumaflow.proto.mapstreamer import mapstream_pb2_grpc
from tests.mapstream.utils import start_request_map_stream
from tests.testing_utils import mock_terminate_on_stop

LOGGER = setup_logging(__name__)


# This handler mimics the scenario where map stream UDF throws a runtime error.
async def err_async_map_stream_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )

    for i in range(5):
        yield Message(str.encode(msg), keys=keys)

    raise RuntimeError("Got a runtime error from map stream handler.")


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/async_map_stream_err.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    server_instance = MapStreamAsyncServer(err_async_map_stream_handler)
    udfs = server_instance.servicer
    mapstream_pb2_grpc.add_MapStreamServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_map_stream_err.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestAsyncServerErrorScenario(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        asyncio.run_coroutine_threadsafe(start_server(), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/async_map_stream_err.sock") as channel:
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

    def test_map_stream_error(self) -> None:
        stub = self.__stub()
        request = start_request_map_stream()
        try:
            generator_response = stub.MapStreamFn(request=request)
            counter = 0
            for _ in generator_response:
                counter += 1
        except Exception as err:
            self.assertTrue("Got a runtime error from map stream handler." in err.__str__())
            return
        self.fail("Expected an exception.")

    def __stub(self):
        return mapstream_pb2_grpc.MapStreamStub(_channel)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            MapStreamAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
