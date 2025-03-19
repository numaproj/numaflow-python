import asyncio
import logging
import threading
import unittest
from unittest.mock import patch

import grpc
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.proto.serving import store_pb2_grpc, store_pb2
from pynumaflow.servingstore import (
    ServingStorer,
    PutDatum,
    Payload,
    GetDatum,
    StoredResult,
    ServingStoreAsyncServer,
)
from tests.testing_utils import mock_terminate_on_stop

LOGGER = setup_logging(__name__)

_s: Server = None
server_port = "unix:///tmp/async_serving_store_err.sock"
_channel = grpc.insecure_channel(server_port)
_loop = None


class AsyncErrInMemoryStore(ServingStorer):
    def __init__(self):
        self.store = {}

    async def put(self, datum: PutDatum):
        req_id = datum.id
        print("Received Put request for ", req_id)
        if req_id not in self.store:
            self.store[req_id] = []

        cur_payloads = self.store[req_id]
        for x in datum.payloads:
            cur_payloads.append(Payload(x.origin, x.value))
        raise ValueError("something fishy")

    async def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        raise ValueError("get is fishy")


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def start_server():
    server = grpc.aio.server()
    class_instance = AsyncErrInMemoryStore()
    server_instance = ServingStoreAsyncServer(serving_store_instance=class_instance)
    udfs = server_instance.servicer
    store_pb2_grpc.add_ServingStoreServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_serving_store_err.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# We are mocking the terminate function from the psutil to not exit the program during testing
@patch("psutil.Process.kill", mock_terminate_on_stop)
class TestAsyncServingStoreErrorScenario(unittest.TestCase):
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
                with grpc.insecure_channel("unix:///tmp/async_serving_store_err.sock") as channel:
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

    def test_put_error(self) -> None:
        grpc_exception = None
        with grpc.insecure_channel(server_port) as channel:
            stub = store_pb2_grpc.ServingStoreStub(channel)
            val = bytes("test_put", encoding="utf-8")
            request = store_pb2.PutRequest(
                id="abc",
                payloads=[store_pb2.Payload(origin="abc1", value=val)],
            )
            try:
                _ = stub.Put(request=request)
            except BaseException as e:
                self.assertTrue("something fishy" in e.details())
                self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
                grpc_exception = e

        self.assertIsNotNone(grpc_exception)

    def test_get_error(self) -> None:
        grpc_exception = None
        with grpc.insecure_channel(server_port) as channel:
            stub = store_pb2_grpc.ServingStoreStub(channel)
            request = store_pb2.GetRequest(
                id="abc",
            )
            try:
                _ = stub.Get(request=request)
            except BaseException as e:
                self.assertTrue("get is fishy" in e.details())
                self.assertEqual(grpc.StatusCode.UNKNOWN, e.code())
                grpc_exception = e

        self.assertIsNotNone(grpc_exception)

    def test_invalid_server_type(self) -> None:
        with self.assertRaises(TypeError):
            ServingStoreAsyncServer()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
