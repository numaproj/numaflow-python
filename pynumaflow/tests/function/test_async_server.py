import asyncio
import logging
import threading
import unittest
from collections.abc import AsyncIterable
from concurrent.futures import ThreadPoolExecutor

import grpc

from google.protobuf import timestamp_pb2 as _timestamp_pb2

from pynumaflow._constants import DATUM_KEY, WIN_START_TIME, WIN_END_TIME
from pynumaflow.function import Messages, Message, Datum, Metadata, UserDefinedFunctionServicer
from pynumaflow.function.generated import udfunction_pb2, udfunction_pb2_grpc
from pynumaflow.tests.function.test_server import map_handler, mock_event_time, mock_watermark, mock_message, \
    mock_interval_window_start, mock_interval_window_end, reduce_handler


async def async_reduce_handler(key: str, datums: AsyncIterable[Datum], md: Metadata) -> Messages:
    interval_window = md.interval_window
    counter = 0
    async for _ in datums:
        counter += 1
    msg = (
        f"counter:{counter} interval_window_start:{interval_window.start} "
        f"interval_window_end:{interval_window.end}"
    )

    return Messages(Message.to_vtx(key, str.encode(msg)))


def request_generator(count, request, resetkey: bool = False):
    for i in range(count):
        if resetkey:
            request.key = f'key-{i}'
        yield request


class TestAsyncServer(unittest.TestCase):

    def setUp(self) -> None:
        self._s = None
        self.loop = asyncio.new_event_loop()
        self.loop.set_debug(True)
        _thread = threading.Thread(target=self.between_callback)
        _thread.start()
        self.startThread = _thread
        try:
            with grpc.insecure_channel('localhost:50051') as channel:
                f = grpc.channel_ready_future(channel)
                f.result(timeout=10)
        except grpc.FutureTimeoutError:
            raise

        self._channel = grpc.insecure_channel('localhost:50051')

    def tearDown(self) -> None:
        logging.info("stopping the server")
        shutdownThread = threading.Thread(target=self.shutdown_callable)
        shutdownThread.start()
        self.startThread.join()
        shutdownThread.join()
        logging.info("shutdown Thread stopped")

    def shutdown_callable(self):
        logging.info("starting shutdown")
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self.stop_server())
        self._s.wait_for_termination()
        pending = asyncio.all_tasks(loop=loop)
        logging.info(len(pending), pending)
        asyncio.gather(*pending, loop=loop)
        loop.stop()
        loop.close()
        logging.info("loop should have closed")

    def between_callback(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.start_server())
        logging.info("start callable completed.")

    async def stop_server(self):
        await self._s.stop(grace=1)
        # pending = asyncio.all_tasks(loop=self.loop)
        # if pending:
        #     future = asyncio.gather(*pending)
        #     await future

    def test_run_server(self) -> None:
        with grpc.insecure_channel('localhost:50051') as channel:
            stub = udfunction_pb2_grpc.UserDefinedFunctionStub(channel)
            event_time_timestamp = _timestamp_pb2.Timestamp()
            event_time_timestamp.FromDatetime(dt=mock_event_time())
            watermark_timestamp = _timestamp_pb2.Timestamp()
            watermark_timestamp.FromDatetime(dt=mock_watermark())

            request = udfunction_pb2.Datum(
                value=mock_message(),
                event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
                watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
            )

            metadata = ((DATUM_KEY, "test"), ("this_metadata_will_be_skipped", "test_ignore"),)
            response = None
            try:
                response = stub.MapFn(request=request, metadata=metadata)
            except grpc.RpcError as e:
                logging.error(e)

            self.assertEqual(1, len(response.elements))
            self.assertEqual("test", response.elements[0].key)
            self.assertEqual(
                bytes(
                    "payload:test_mock_message "
                    "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                    encoding="utf-8",
                ),
                response.elements[0].value,
            )

    def test_map(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(self._channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.Datum(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = ((DATUM_KEY, "test"), ("this_metadata_will_be_skipped", "test_ignore"),)

        try:
            response = stub.MapFn(request=request, metadata=metadata)
        except grpc.RpcError as e:
            logging.error(e)

        self.assertEqual(1, len(response.elements))
        self.assertEqual("test", response.elements[0].key)
        self.assertEqual(
            bytes(
                "payload:test_mock_message "
                "event_time:2022-09-12 16:00:00 watermark:2022-09-12 16:01:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )

    def test_reduce(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(self._channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.Datum(
            key="test",
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (
            (DATUM_KEY, "test"),
            (WIN_START_TIME, f'{mock_interval_window_start()}'),
            (WIN_END_TIME, f'{mock_interval_window_end()}'),
        )

        response = None
        try:
            response = stub.ReduceFn(request_iterator=request_generator(count=10, request=request),
                                     metadata=metadata)
        except grpc.RpcError as e:
            logging.error(e)

        self.assertEqual(1, len(response.elements))
        self.assertEqual(
            bytes(
                "counter:10 interval_window_start:2022-09-12 16:00:00+00:00 "
                "interval_window_end:2022-09-12 16:01:00+00:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )

    def test_reduce_with_multiple_keys(self) -> None:
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(self._channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.Datum(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (
            (DATUM_KEY, "test"),
            (WIN_START_TIME, f'{mock_interval_window_start()}'),
            (WIN_END_TIME, f'{mock_interval_window_end()}'),
        )

        response = None
        try:
            response = stub.ReduceFn(request_iterator=request_generator(count=10, request=request, resetkey=True),
                                     metadata=metadata)
        except grpc.RpcError as e:
            print(e)

        self.assertEqual(10, len(response.elements))
        self.assertEqual(
            bytes(
                "counter:1 interval_window_start:2022-09-12 16:00:00+00:00 "
                "interval_window_end:2022-09-12 16:01:00+00:00",
                encoding="utf-8",
            ),
            response.elements[0].value,
        )

    def start_reduce_streaming_request(self) -> (Datum, dict):
        stub = udfunction_pb2_grpc.UserDefinedFunctionStub(self._channel)
        event_time_timestamp = _timestamp_pb2.Timestamp()
        event_time_timestamp.FromDatetime(dt=mock_event_time())
        watermark_timestamp = _timestamp_pb2.Timestamp()
        watermark_timestamp.FromDatetime(dt=mock_watermark())

        request = udfunction_pb2.Datum(
            value=mock_message(),
            event_time=udfunction_pb2.EventTime(event_time=event_time_timestamp),
            watermark=udfunction_pb2.Watermark(watermark=watermark_timestamp),
        )

        metadata = (
            (DATUM_KEY, "test"),
            (WIN_START_TIME, f'{mock_interval_window_start()}'),
            (WIN_END_TIME, f'{mock_interval_window_end()}'),
        )
        return request, metadata

    async def start_server(self):
        self._max_threads = 10
        server = grpc.aio.server()
        udfs = UserDefinedFunctionServicer(
            reduce_handler=async_reduce_handler,
            map_handler=map_handler,
        )
        udfunction_pb2_grpc.add_UserDefinedFunctionServicer_to_server(udfs, server)
        listen_addr = '[::]:50051'
        server.add_insecure_port(listen_addr)
        logging.info("Starting server on %s", listen_addr)
        self._s = server
        await server.start()
        await server.wait_for_termination()
        logging.info("after waiting for termination")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
