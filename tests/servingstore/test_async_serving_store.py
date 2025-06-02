import asyncio
import logging
import threading
import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.proto.serving import store_pb2_grpc, store_pb2
from pynumaflow.servingstore import (
    ServingStoreAsyncServer,
    ServingStorer,
    PutDatum,
    Payload,
    GetDatum,
    StoredResult,
)


class AsyncInMemoryStore(ServingStorer):
    def __init__(self):
        self.store = {}

    async def put(self, datum: PutDatum):
        req_id = datum.id
        print("Received Put request for ", req_id)
        if req_id not in self.store:
            self.store[req_id] = []

        cur_payloads = self.store[req_id]
        for x in datum.payloads:
            print(x)
            cur_payloads.append(Payload(x.origin, x.value))
        self.store[req_id] = cur_payloads

    async def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        resp = []
        if req_id in self.store:
            resp = self.store[req_id]
        return StoredResult(id_=req_id, payloads=resp)


LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False

server_port = "unix:///tmp/async_serving_store.sock"

_s: Server = None
_channel = grpc.insecure_channel(server_port)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncStore():
    server = ServingStoreAsyncServer(serving_store_instance=AsyncInMemoryStore())
    udfs = server.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    store_pb2_grpc.add_ServingStoreServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_serving_store.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncServingStore(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncStore()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel(server_port) as channel:
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

    def test_is_ready(self) -> None:
        with grpc.insecure_channel(server_port) as channel:
            stub = store_pb2_grpc.ServingStoreStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_put_get(self) -> None:
        val = bytes("test_get", encoding="utf-8")
        with grpc.insecure_channel(server_port) as channel:
            stub = store_pb2_grpc.ServingStoreStub(channel)
            response = None
            request = store_pb2.PutRequest(
                id="abc",
                payloads=[store_pb2.Payload(origin="abc1", value=val)],
            )
            try:
                response = stub.Put(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(True, response.success)

            stub = store_pb2_grpc.ServingStoreStub(channel)
            response_get = None
            request = store_pb2.GetRequest(
                id="abc",
            )
            try:
                response_get = stub.Get(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(len(response_get.payloads), 1)
            self.assertEqual(response_get.payloads[0].value, val)
            self.assertEqual(response_get.payloads[0].origin, "abc1")

    def test_max_threads(self):
        class_instance = AsyncInMemoryStore()
        # max cap at 16
        server = ServingStoreAsyncServer(serving_store_instance=class_instance, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = ServingStoreAsyncServer(serving_store_instance=class_instance, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = ServingStoreAsyncServer(serving_store_instance=class_instance)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
