import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable

import grpc
from google.protobuf import empty_pb2 as _empty_pb2
from grpc.aio._server import Server

from pynumaflow import setup_logging
from pynumaflow.mapstreamer import (
    Message,
    Datum,
    MapStreamAsyncServer,
)
from pynumaflow.proto.mapper import map_pb2_grpc
from tests.mapstream.utils import request_generator

LOGGER = setup_logging(__name__)

# if set to true, map handler will raise a `ValueError` exception.
raise_error_from_map = False


async def async_map_stream_handler(keys: list[str], datum: Datum) -> AsyncIterable[Message]:
    val = datum.value
    msg = "payload:{} event_time:{} watermark:{}".format(
        val.decode("utf-8"),
        datum.event_time,
        datum.watermark,
    )
    for i in range(10):
        yield Message(str.encode(msg), keys=keys)


_s: Server = None
_channel = grpc.insecure_channel("unix:///tmp/async_map_stream.sock")
_loop = None


def startup_callable(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


def NewAsyncMapStreamer(
    map_stream_handler=async_map_stream_handler,
):
    server = MapStreamAsyncServer(map_stream_instance=map_stream_handler)
    udfs = server.servicer
    return udfs


async def start_server(udfs):
    server = grpc.aio.server()
    map_pb2_grpc.add_MapServicer_to_server(udfs, server)
    listen_addr = "unix:///tmp/async_map_stream.sock"
    server.add_insecure_port(listen_addr)
    logging.info("Starting server on %s", listen_addr)
    global _s
    _s = server
    await server.start()
    await server.wait_for_termination()


class TestAsyncMapStreamer(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        global _loop
        loop = asyncio.new_event_loop()
        _loop = loop
        _thread = threading.Thread(target=startup_callable, args=(loop,), daemon=True)
        _thread.start()
        udfs = NewAsyncMapStreamer()
        asyncio.run_coroutine_threadsafe(start_server(udfs), loop=loop)
        while True:
            try:
                with grpc.insecure_channel("unix:///tmp/async_map_stream.sock") as channel:
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

    def test_map_stream(self) -> None:
        stub = self.__stub()

        # Send >1 requests
        req_count = 3
        try:
            generator_response = stub.MapFn(
                request_iterator=request_generator(count=req_count, session=1)
            )
        except grpc.RpcError as e:
            logging.error(e)
            self.fail(f"RPC failed: {e}")

        # First message must be the handshake
        handshake = next(generator_response)
        self.assertTrue(handshake.handshake.sot)

        # Expected: 10 results per request + 1 EOT per request
        expected_result_msgs = req_count * 10
        expected_eots = req_count

        # Prepare expected payload
        expected_payload = bytes(
            "payload:test_mock_message "
            "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
            encoding="utf-8",
        )

        from collections import Counter

        id_counter = Counter()
        result_msg_count = 0
        eot_count = 0

        for msg in generator_response:
            # Count EOTs wherever they show up
            if hasattr(msg, "status") and msg.status.eot:
                eot_count += 1
                continue

            # Otherwise, it's a data/result message; validate payload and tally by id
            self.assertTrue(msg.results, "Expected results in MapResponse.")
            self.assertEqual(expected_payload, msg.results[0].value)
            id_counter[msg.id] += 1
            result_msg_count += 1

        # Validate totals
        self.assertEqual(
            expected_result_msgs,
            result_msg_count,
            f"Expected {expected_result_msgs} result messages, got {result_msg_count}",
        )
        self.assertEqual(
            expected_eots, eot_count, f"Expected {expected_eots} EOT messages, got {eot_count}"
        )

        # Validate 10 messages per request id: test-id-0..test-id-(req_count-1)
        for i in range(req_count):
            self.assertEqual(
                10,
                id_counter[f"test-id-{i}"],
                f"Expected 10 results for test-id-{i}, got {id_counter[f'test-id-{i}']}",
            )

    def test_is_ready(self) -> None:
        with grpc.insecure_channel("unix:///tmp/async_map_stream.sock") as channel:
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

    def test_max_threads(self):
        # max cap at 16
        server = MapStreamAsyncServer(map_stream_instance=async_map_stream_handler, max_threads=32)
        self.assertEqual(server.max_threads, 16)

        # use argument provided
        server = MapStreamAsyncServer(map_stream_instance=async_map_stream_handler, max_threads=5)
        self.assertEqual(server.max_threads, 5)

        # defaults to 4
        server = MapStreamAsyncServer(map_stream_instance=async_map_stream_handler)
        self.assertEqual(server.max_threads, 4)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
