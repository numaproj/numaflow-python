import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.batchmapper import (
    Message,
    Datum,
    BatchMapper,
    BatchResponses,
    BatchResponse,
    BatchMapAsyncServer,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.batchmap.utils import request_generator

LOGGER = setup_logging(__name__)

listen_addr = "unix:///tmp/batch_map.sock"

_s: Server = None
_channel = grpc.insecure_channel(listen_addr)
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


class ExampleClass(BatchMapper):
    async def handler(
        self,
        datums: AsyncIterable[Datum],
    ) -> BatchResponses:
        batch_responses = BatchResponses()
        async for datum in datums:
            val = datum.value
            _ = datum.event_time
            _ = datum.watermark
            strs = val.decode("utf-8").split(",")
            batch_response = BatchResponse.from_id(datum.id)
            if len(strs) == 0:
                batch_response.append(Message.to_drop())
            else:
                for s in strs:
                    batch_response.append(Message(str.encode(s)))
            batch_responses.append(batch_response)

        return batch_responses


async def handler(
    datums: AsyncIterable[Datum],
) -> BatchResponses:
    batch_responses = BatchResponses()
    async for datum in datums:
        val = datum.value
        _ = datum.event_time
        _ = datum.watermark
        strs = val.decode("utf-8").split(",")
        batch_response = BatchResponse.from_id(datum.id)
        if len(strs) == 0:
            batch_response.append(Message.to_drop())
        else:
            for s in strs:
                batch_response.append(Message(str.encode(s)))
        batch_responses.append(batch_response)

    return batch_responses


def NewAsyncBatchMapper():
    d = ExampleClass()
    server_instance = BatchMapAsyncServer(d)
    udfs = server_instance.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncBatchMapper(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncBatchMapper()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
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

    def test_batch_map(self) -> None:
        stub = self.__stub()
        generator_response = None

        try:
            generator_response = stub.MapFn(request_iterator=request_generator(count=10, session=1))
        except grpc.RpcError as e:
            logging.error(e)

        handshake = next(generator_response)
        # assert that handshake response is received.
        self.assertTrue(handshake.handshake.sot)
        data_resp = []
        for r in generator_response:
            data_resp.append(r)

        idx = 0
        while idx < len(data_resp) - 1:
            self.assertEqual(
                bytes(
                    "test_mock_message",
                    encoding="utf-8",
                ),
                data_resp[idx].results[0].value,
            )
            _id = data_resp[idx].id
            self.assertEqual(_id, "test-id-" + str(idx))
            # capture the output from the SinkFn generator and assert.
            # self.assertEqual(data_resp[idx].result.status, sink_pb2.Status.SUCCESS)
            idx += 1
        # EOT Response
        self.assertEqual(data_resp[len(data_resp) - 1].status.eot, True)
        # 10 sink responses + 1 EOT response
        self.assertEqual(11, len(data_resp))

    def test_is_ready(self) -> None:
        with grpc.insecure_channel(listen_addr) as channel:
            stub = map_pb2_grpc.MapStub(channel)

            request = _empty_pb2.Empty()
            response = None
            try:
                response = stub.IsReady(request=request)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertTrue(response.ready)

    def test_max_threads(self):
        # max cap at 16
        server = BatchMapAsyncServer(batch_mapper_instance=handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = BatchMapAsyncServer(batch_mapper_instance=handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = BatchMapAsyncServer(batch_mapper_instance=handler)
        self.assertEqual(server.max_threads, 4)

    def __stub(self):
        return map_pb2_grpc.MapStub(_channel)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
