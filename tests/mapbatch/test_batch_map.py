import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.batchmapper import (
    BatchMapServer,
    BatchMapUnaryServer,
    BatchMapGroupingServer,
    Datum,
    BatchResponses,
    Message,
    Messages,
)
from pynumaflow.proto.batchmapper import batchmap_pb2_grpc
from .utils import generate_request_items

from ..map.test_async_mapper import async_map_handler

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


async def async_batch_handler(datums: AsyncIterable[Datum]) -> AsyncIterable[BatchResponses]:
    idx = 0
    async for datum in datums:
        val = datum.value
        msg = "payload=={} event_time=={} watermark=={} id=={} idx=={}".format(
            val.decode("utf-8"),
            str(datum.event_time).replace(" ", "T"),
            str(datum.watermark).replace(" ", "T"),
            datum.id,
            idx,
        )
        msgs = Messages(Message(str.encode(msg), keys=[]))
        yield BatchResponses(datum.id, msgs)
        idx += 1


def _split_response_msg(data):
    kvp_parts = [d.split("==") for d in data.split()]
    return {k: v for k, v in kvp_parts}


_s: Server = None
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewBatchMapStreamer(
    handler=async_batch_handler,
):
    server = BatchMapServer(mapper_instance=handler)
    udfs = server.servicer
    return udfs


async def start_server(udfs, sock_addr):
    server = grpc.aio.server()
    batchmap_pb2_grpc.add_BatchMapServicer_to_server(udfs, server)
    listen_addr = sock_addr
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


# Attempt to avoid too much duplicate code between implementations with base clas for setup, but does
# require some implementation specific SOCK_NAME at class level to avoid socket connect conflicts
class TestBatchMapBase:
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = cls.class_under_test()
        sock_addr = f"unix:///tmp/{cls.SOCK_NAME}"
        asyncio.run_coroutine_threadsafe(start_server(udfs, sock_addr), loop=loop)
        while True:
            try:
                with grpc.insecure_channel(sock_addr) as channel:
                    cls._channel = grpc.insecure_channel(sock_addr)
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

    def _stub(self):
        return batchmap_pb2_grpc.BatchMapStub(self._channel)


class TestBatchMap(TestBatchMapBase, unittest.TestCase):
    SOCK_NAME = "batch_map_stream.sock"

    @classmethod
    def class_under_test(cls):
        return NewBatchMapStreamer()

    def test_map_stream(self) -> None:
        stub = self._stub()
        # request = get_request_item("a")
        try:
            # Two separate calls to show each as the 'idx' reset showing new invocations based on previous end-of-input-stream
            ids1 = ["a", "b", "c", "d", "e"]
            generator_response1 = stub.BatchMapFn(request_iterator=generate_request_items(ids1))

            ids2 = ["x", "y", "z"]
            generator_response2 = stub.BatchMapFn(request_iterator=generate_request_items(ids2))
        except grpc.RpcError as e:
            logging.error(e)

        results1 = [x for x in generator_response1]
        results2 = [x for x in generator_response2]

        assert len(results1) == len(ids1)
        assert len(results2) == len(ids2)

        for idx, response in enumerate(results1):
            assert response.id == ids1[idx]
            kvp = _split_response_msg(response.results[0].value.decode(encoding="utf-8"))
            assert int(kvp["idx"]) == idx

        for idx, response in enumerate(results2):
            assert response.id == ids2[idx]
            kvp = _split_response_msg(response.results[0].value.decode(encoding="utf-8"))
            assert int(kvp["idx"]) == idx


class TestBatchMapUnary(TestBatchMapBase, unittest.TestCase):
    SOCK_NAME = "batch_map_unary.sock"

    @classmethod
    def class_under_test(cls):
        # Explicitly use same handler from map/async-mapper test to show drop-in replacement ability
        server = BatchMapUnaryServer(mapper_instance=async_map_handler)
        udfs = server.servicer
        return udfs

    def test_map_unary(self) -> None:
        stub = self._stub()
        # request = get_request_item("a")
        try:
            ids1 = ["a", "b", "c", "d", "e"]
            generator_response1 = stub.BatchMapFn(request_iterator=generate_request_items(ids1))

            ids2 = ["x", "y", "z"]
            generator_response2 = stub.BatchMapFn(request_iterator=generate_request_items(ids2))
        except grpc.RpcError as e:
            logging.error(e)

        results1 = [x for x in generator_response1]
        results2 = [x for x in generator_response2]

        assert len(results1) == len(ids1)
        assert len(results2) == len(ids2)

        # Two separate calls for two "batches" of data, but each will be handled independently, repeating 'index=0'

        for idx, response in enumerate(results1):
            assert response.id == ids1[idx]

        for idx, response in enumerate(results2):
            assert response.id == ids2[idx]


class TestBatchMapGrouping(TestBatchMapBase, unittest.TestCase):
    SOCK_NAME = "batch_map_grouped.sock"

    @classmethod
    def class_under_test(cls):
        # Explicitly use same handler from map/async-mapper test to show drop-in replacement ability
        server = BatchMapGroupingServer(
            mapper_instance=async_batch_handler, max_batch_size=3, timeout_sec=2
        )
        udfs = server.servicer
        return udfs

    def test_map_unary(self) -> None:
        stub = self._stub()
        try:
            ids = ["a", "b", "c", "d", "e", "f", "g", "h"]
            generator_response = stub.BatchMapFn(request_iterator=generate_request_items(ids))
        except grpc.RpcError as e:
            logging.error(e)

        results = [x for x in generator_response]

        assert len(results) == len(ids)

        # We get all the responses back, but by inspecting the "idx" in the returned payload we can see that they
        # got provided an appropriate batch sizes
        expected_indices = [0, 1, 2, 0, 1, 2, 0, 1]
        for expected_idx, response in zip(expected_indices, results):
            kvp = _split_response_msg(response.results[0].value.decode(encoding="utf-8"))
            assert int(kvp["idx"]) == expected_idx
