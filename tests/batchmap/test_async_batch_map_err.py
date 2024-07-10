import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

import grpc

from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.batchmapper import BatchResponses
from pynumaflow.batchmapper import Datum, BatchMapAsyncServer
from pynumaflow.proto.batchmapper import batchmap_pb2_grpc
from tests.batchmap.utils import start_request
from tests.testing_utils import mock_terminate_on_stop

LOGGER = setup_logging(__name__)

raise_error = False


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        # add the id to the datum
        request.id = str(i)
        if resetkey:
            request.payload.keys.extend([f"key-{i}"])
        yield request


# This handler mimics the scenario where batch map UDF throws a runtime error.
async def err_handler(datums: list[Datum]) -> BatchResponses:
    if raise_error:
        raise RuntimeError("Got a runtime error from batch map handler.")
    batch_responses = BatchResponses()
    return batch_responses


listen_addr = "unix:///tmp/async_batch_map_err.sock"

_s: Server = None
_channel = grpc.insecure_channel(listen_addr)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
async def start_server():
    server = grpc.aio.server()
    server_instance = BatchMapAsyncServer(err_handler)
    udfs = server_instance.servicer
    batchmap_pb2_grpc.add_BatchMapServicer_to_server(udfs, server)
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
                with grpc.insecure_channel(listen_addr) as channel:
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

    def test_batch_map_error(self) -> None:
        global raise_error
        raise_error = True
        stub = self.__stub()
        try:
            generator_response = stub.BatchMapFn(
                request_iterator=request_generator(count=10, request=start_request())
            )
            counter = 0
            for _ in generator_response:
                counter += 1
        except Exception as err:
            self.assertTrue("Got a runtime error from batch map handler." in err.__str__())
            return
        self.fail("Expected an exception.")

    def test_batch_map_length_error(self) -> None:
        global raise_error
        raise_error = False
        stub = self.__stub()
        try:
            generator_response = stub.BatchMapFn(
                request_iterator=request_generator(count=10, request=start_request())
            )
            counter = 0
            for _ in generator_response:
                counter += 1
        except Exception as err:
            self.assertTrue(
                "batchMapFn: mismatch between length of batch requests and responses"
                in err.__str__()
            )
            return
        self.fail("Expected an exception.")

    def __stub(self):
        return batchmap_pb2_grpc.BatchMapStub(_channel)

    def test_invalid_input(self):
        with self.assertRaises(TypeError):
            BatchMapAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
