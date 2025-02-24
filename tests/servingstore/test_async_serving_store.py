import asyncio
import logging
import threading
import unittest

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.proto.serving import store_pb2_grpc
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
        self.store[req_id] = cur_payloads

    async def get(self, datum: GetDatum) -> StoredResult:
        req_id = datum.id
        print("Received Get request for ", req_id)
        raise ValueError("get is fishy")


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

    # def test_read_source(self) -> None:
    #     with grpc.insecure_channel(server_port) as channel:
    #         stub = store_pb2_grpc.ServingStoreStub(channel)
    #
    #         request = read_req_source_fn()
    #         generator_response = None
    #         try:
    #             generator_response = stub.Put(request=source_pb2.ReadRequest(request=request))
    #         except grpc.RpcError as e:
    #             logging.error(e)
    #
    #         counter = 0
    #         # capture the output from the ReadFn generator and assert.
    #         for r in generator_response:
    #             counter += 1
    #             self.assertEqual(
    #                 bytes("payload:test_mock_message", encoding="utf-8"),
    #                 r.result.payload,
    #             )
    #             self.assertEqual(
    #                 ["test_key"],
    #                 r.result.keys,
    #             )
    #             self.assertEqual(
    #                 mock_offset().offset,
    #                 r.result.offset.offset,
    #             )
    #             self.assertEqual(
    #                 mock_offset().partition_id,
    #                 r.result.offset.partition_id,
    #             )
    #         """Assert that the generator was called 10 times in the stream"""
    #         self.assertEqual(10, counter)

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

    # def test_ack(self) -> None:
    #     with grpc.insecure_channel(server_port) as channel:
    #         stub = source_pb2_grpc.SourceStub(channel)
    #         request = ack_req_source_fn()
    #         try:
    #             response = stub.AckFn(request=source_pb2.AckRequest(request=request))
    #         except grpc.RpcError as e:
    #             print(e)
    #
    #         self.assertEqual(response, source_pb2.AckResponse())
    #
    # def test_pending(self) -> None:
    #     with grpc.insecure_channel(server_port) as channel:
    #         stub = source_pb2_grpc.SourceStub(channel)
    #         request = _empty_pb2.Empty()
    #         response = None
    #         try:
    #             response = stub.PendingFn(request=request)
    #         except grpc.RpcError as e:
    #             logging.error(e)
    #
    #         self.assertEqual(response.result.count, 10)
    #
    # def test_partitions(self) -> None:
    #     with grpc.insecure_channel(server_port) as channel:
    #         stub = source_pb2_grpc.SourceStub(channel)
    #         request = _empty_pb2.Empty()
    #         response = None
    #         try:
    #             response = stub.PartitionsFn(request=request)
    #         except grpc.RpcError as e:
    #             logging.error(e)
    #
    #         self.assertEqual(response.result.partitions, mock_partitions())
    #
    # def __stub(self):
    #     return source_pb2_grpc.SourceStub(_channel)
    #
    # def test_max_threads(self):
    #     class_instance = AsyncSource()
    #     # max cap at 16
    #     server = SourceAsyncServer(sourcer_instance=class_instance, max_threads=32)
    #     self.assertEqual(server.max_threads, 16)
    #
    #     # use argument provided
    #     server = SourceAsyncServer(sourcer_instance=class_instance, max_threads=5)
    #     self.assertEqual(server.max_threads, 5)
    #
    #     # defaults to 4
    #     server = SourceAsyncServer(sourcer_instance=class_instance)
    #     self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
